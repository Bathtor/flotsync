//! Group-membership and pending-group workflow scenarios.

use super::*;

#[test]
fn runtime_group_state_projection_rejects_duplicate_group_ids() {
    let local_member = alice_member();
    let group_id = GroupId(Uuid::from_u128(30));
    let record = inactive_group_record(
        group_id,
        vec![local_member.clone(), bob_member()],
        GroupSchema::default(),
    );

    let result = RuntimeGroupStateSnapshot::from_records(
        &local_member,
        &ApplicationSchemas::EMPTY,
        [record.clone(), record],
    );
    let Err(error) = result else {
        panic!("duplicate group identifiers should be rejected");
    };
    assert!(matches!(
        error,
        GroupInstallError::DuplicateStoredGroup {
            group_id: duplicate_group_id,
        } if duplicate_group_id == group_id
    ));
}

#[test]
fn runtime_group_state_reuses_matching_process_static_schema() {
    let local_member = alice_member();
    let group_id = GroupId(Uuid::from_u128(31));
    let dataset_id = docs_dataset_id();
    let loaded_schema = title_schema_shared();
    let record = inactive_group_record(
        group_id,
        vec![local_member.clone(), bob_member()],
        docs_group_schema_from_schema(loaded_schema),
    );
    let snapshot = RuntimeGroupStateSnapshot::from_records(
        &local_member,
        &TITLE_APPLICATION_SCHEMAS,
        [record],
    )
    .expect("matching stored schema should resolve");
    let schema = snapshot
        .group(&group_id)
        .expect("group should be projected")
        .group_schema()
        .schema(&dataset_id)
        .expect("dataset should be projected");

    assert!(matches!(schema, SchemaSource::Static(_)));
    assert!(std::ptr::eq(schema.as_schema(), title_schema_static()));
}

#[test]
fn runtime_group_state_keeps_non_matching_loaded_schema() {
    let local_member = alice_member();
    let group_id = GroupId(Uuid::from_u128(32));
    let dataset_id = docs_dataset_id();
    let loaded_schema = title_note_schema_shared();
    let record = inactive_group_record(
        group_id,
        vec![local_member.clone(), bob_member()],
        docs_group_schema_from_schema(loaded_schema.clone()),
    );
    let snapshot = RuntimeGroupStateSnapshot::from_records(
        &local_member,
        &TITLE_APPLICATION_SCHEMAS,
        [record],
    )
    .expect("non-matching stored schema should remain usable");
    let schema = snapshot
        .group(&group_id)
        .expect("group should be projected")
        .group_schema()
        .schema(&dataset_id)
        .expect("dataset should be projected");

    let SchemaSource::Shared(resolved) = schema else {
        panic!("non-matching schema should retain shared ownership");
    };
    assert!(Arc::ptr_eq(resolved, &loaded_schema));
}

#[test]
fn runtime_group_state_reuses_resolved_schema_arc_for_hosted_group() {
    let local_member = alice_member();
    let group_id = GroupId(Uuid::from_u128(33));
    let state = SharedGroupState::new(&ApplicationSchemas::EMPTY);
    let first_record = inactive_group_record(
        group_id,
        vec![local_member.clone(), bob_member()],
        docs_group_schema(),
    );
    let first_snapshot = state
        .build_runtime_state_from_active_records(&local_member, [first_record])
        .expect("first group projection should succeed");
    state.replace(first_snapshot);
    let first_schema = state
        .group_schema(&group_id)
        .expect("first projection should retain a schema");

    let second_record = inactive_group_record(
        group_id,
        vec![local_member.clone(), bob_member()],
        docs_group_schema(),
    );
    let second_snapshot = state
        .build_runtime_state_from_active_records(&local_member, [second_record])
        .expect("second group projection should succeed");
    state.replace(second_snapshot);
    let second_schema = state
        .group_schema(&group_id)
        .expect("second projection should retain a schema");

    assert!(Arc::ptr_eq(&first_schema, &second_schema));
}

#[test]
fn runtime_startup_hydrates_persisted_group_memberships_from_store() {
    let alice_member = alice_member();
    let dataset_id = docs_dataset_id();
    let store = sqlite_store(alice_member.clone());
    let group_id = GroupId(Uuid::from_u128(31));
    let members = GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member()])
        .expect("group should build");
    persist_group_in_store(
        store.as_ref(),
        ReplicationGroupRecord {
            group_id,
            member_keys: test_group_member_keys(members.ordered_members()),
            local_member_index: MemberIndex::new(0),
            group_schema: docs_group_schema(),
            version_vector: VersionVector::initial(
                NonZeroUsize::new(2).expect("group should have two members"),
            ),
            lifecycle: ReplicationGroupLifecycle::Open,
            security_material: current_slice_placeholder_group_security_material(group_id),
            ..Default::default()
        },
    );
    let listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts_and_application_schemas(
        app_alice_id(),
        &TITLE_APPLICATION_SCHEMAS,
        store.clone(),
        listener,
    );
    let row_id = test_row_id(group_id, dataset_id.clone(), 32);

    wait_for_group_install(&runtime, group_id);
    let group_state = runtime
        .group_state()
        .expect("startup group state should be available");
    let hydrated_group = group_state
        .group(&group_id)
        .expect("persisted group should be published at startup");
    assert_eq!(hydrated_group.group_id(), group_id);
    assert!(
        hydrated_group
            .group_schema()
            .has_same_schema_definitions(&docs_group_schema())
    );
    let hydrated_schema = hydrated_group
        .group_schema()
        .schema(&dataset_id)
        .expect("hydrated docs dataset should have a schema");
    assert!(matches!(hydrated_schema, SchemaSource::Static(_)));
    assert!(std::ptr::eq(
        hydrated_schema.as_schema(),
        title_schema_static()
    ));
    assert_eq!(hydrated_group.lifecycle(), &ReplicationGroupLifecycle::Open);
    assert!(hydrated_group.is_readable());
    assert!(hydrated_group.is_writable());
    assert_eq!(group_state.readable_groups().count(), 1);
    assert_eq!(
        hydrated_group.members().collect::<HashSet<_>>(),
        HashSet::from([alice_member.clone(), bob_member()])
    );
    let read_token = snapshot_read_token(runtime.as_ref(), group_id, docs_dataset_id());
    publish_changes(
        runtime.as_ref(),
        read_token,
        vec![RowMutation::Upsert {
            row_id,
            row: crate::row_values! {
                "title" => "hydrated on startup",
            },
        }],
    );
}

#[test]
fn group_state_retains_one_coherent_view_across_publications() {
    let alice_member = alice_member();
    let group_schema = docs_group_schema();
    let store = sqlite_store(alice_member.clone());
    let runtime = load_runtime_with_parts(
        app_alice_id(),
        store.clone(),
        Arc::new(ListenerStub::default()),
    );
    let before_creation = runtime
        .group_state()
        .expect("initial group state should be available");

    let group_id = wait_for_test_reply(runtime.create_group(CreateGroupRequest {
        group_name: Some("project notes".to_owned()),
        message: Some("discard after activation".to_owned()),
        members: vec![alice_member.clone()],
        group_schema: group_schema.clone(),
    }))
    .expect("group creation should succeed");

    let after_creation = runtime
        .group_state()
        .expect("updated group state should be available");
    assert!(before_creation.group(&group_id).is_none());
    assert_eq!(before_creation.groups().count(), 0);
    let created_group = after_creation
        .group(&group_id)
        .expect("created group should be published");
    assert_eq!(created_group.group_id(), group_id);
    assert_eq!(created_group.group_name(), Some("project notes"));
    assert_eq!(created_group.group_schema(), &group_schema);
    assert_eq!(created_group.lifecycle(), &ReplicationGroupLifecycle::Open);
    assert_eq!(
        created_group.members().collect::<Vec<_>>(),
        vec![alice_member]
    );
    assert_eq!(after_creation.groups().count(), 1);

    wait_for_test_reply(runtime.shutdown()).expect("runtime should shut down");
    assert!(matches!(
        runtime.group_state(),
        Err(ApiError::RuntimeUnavailable)
    ));
}

#[test]
fn create_group_persists_membership_across_runtime_restart() {
    let alice_member = alice_member();
    let dataset_id = docs_dataset_id();
    let store = sqlite_store(alice_member.clone());
    let first_listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts_and_application_schemas(
        app_alice_id(),
        &TITLE_APPLICATION_SCHEMAS,
        store.clone(),
        first_listener.clone(),
    );
    let group_id = wait_for_test_reply(runtime.create_group(CreateGroupRequest {
        group_name: Some("  shared docs  ".to_owned()),
        message: Some(String::new()),
        members: vec![alice_member.clone()],
        group_schema: docs_group_schema(),
    }))
    .expect("create_group should succeed");
    let creation_events = first_listener.captured_data_changes();
    assert_eq!(creation_events.len(), 1);
    assert!(creation_events[0].rows.is_empty());
    let creation_tokens = first_listener.captured_data_change_read_tokens();
    assert_eq!(creation_tokens.len(), 1);
    assert!(creation_tokens[0].group_version(&group_id).is_some());
    let created = load_persisted_group(store.as_ref(), group_id);
    assert_eq!(created.group_name.as_deref(), Some("shared docs"));
    let created_state = runtime
        .group_state()
        .expect("created group state should be available");
    let created_schema = created_state
        .group(&group_id)
        .expect("created group should be published")
        .group_schema()
        .schema(&dataset_id)
        .expect("created group should retain the docs schema");
    assert!(matches!(created_schema, SchemaSource::Static(_)));
    drop(runtime);

    let restarted_listener = Arc::new(ListenerStub::default());
    let restarted_runtime = load_runtime_with_parts_and_application_schemas(
        app_alice_id(),
        &TITLE_APPLICATION_SCHEMAS,
        store.clone(),
        restarted_listener,
    );
    let row_id = test_row_id(group_id, dataset_id, 33);

    wait_for_group_install(&restarted_runtime, group_id);
    let restarted = load_persisted_group(store.as_ref(), group_id);
    assert_eq!(restarted.group_name.as_deref(), Some("shared docs"));
    let read_token = snapshot_read_token(restarted_runtime.as_ref(), group_id, docs_dataset_id());
    publish_changes(
        restarted_runtime.as_ref(),
        read_token,
        vec![RowMutation::Upsert {
            row_id,
            row: crate::row_values! {
                "title" => "after restart",
            },
        }],
    );
}

#[test]
fn create_group_rejects_empty_name_after_trimming() {
    let alice_member = alice_member();
    let store = sqlite_store(alice_member.clone());
    let runtime = load_runtime_with_parts(
        app_alice_id(),
        store.clone(),
        Arc::new(ListenerStub::default()),
    );

    let error = wait_for_test_reply(runtime.create_group(CreateGroupRequest {
        group_name: Some(" \t ".to_owned()),
        message: None,
        members: vec![alice_member],
        group_schema: GroupSchema::default(),
    }))
    .expect_err("empty-after-trim name should fail");

    match error {
        ApiError::ApiExternal { source } => {
            assert!(matches!(
                source.downcast_ref::<CreateGroupError>(),
                Some(CreateGroupError::EmptyGroupName)
            ));
        }
        error => panic!("unexpected API error: {error:?}"),
    }
    assert!(load_persisted_groups(store.as_ref()).is_empty());
}

#[test]
fn create_group_default_is_rejected_as_incomplete() {
    let alice_member = alice_member();
    let store = sqlite_store(alice_member);
    let runtime = load_runtime_with_parts(
        app_alice_id(),
        store.clone(),
        Arc::new(ListenerStub::default()),
    );

    let error = wait_for_test_reply(runtime.create_group(CreateGroupRequest::default()))
        .expect_err("default request should be incomplete");
    match error {
        ApiError::ApiExternal { source } => assert!(matches!(
            source.downcast_ref::<CreateGroupError>(),
            Some(CreateGroupError::CreatorNotInMembers { .. })
        )),
        error => panic!("unexpected API error: {error:?}"),
    }
    assert!(load_persisted_groups(store.as_ref()).is_empty());
}

#[test]
fn runtime_replays_pending_group_decisions_and_persists_responses_on_startup() {
    let store = sqlite_store(alice_member());
    let invited_group_id = GroupId(Uuid::from_u128(60_101));
    store_pending_group_decision(
        store.as_ref(),
        runtime_test_invitation_decision(invited_group_id),
    );
    store_pending_group_decision(store.as_ref(), runtime_test_migration_proposal_decision());
    let listener = Arc::new(ListenerStub::default());

    let runtime = load_runtime_with_parts(app_alice_id(), store.clone(), listener.clone());

    listener.wait_for_pending_group_event_count(2);
    let events = listener.take_pending_group_events();
    assert_eq!(events.len(), 2);
    let mut accepted_invitation = false;
    let mut rejected_migration = false;
    for event in events {
        match event {
            CapturedPendingGroupEvent::GroupInvitation {
                invitation,
                respond,
            } => {
                assert_eq!(invitation.group_id, invited_group_id);
                assert_eq!(invitation.source, GroupInvitationSource::Creation);
                assert_eq!(
                    invitation.proposed_members,
                    vec![alice_member(), bob_member()]
                );
                wait_for_test_reply(respond.accept()).expect("invitation accept should persist");
                accepted_invitation = true;
            }
            CapturedPendingGroupEvent::MigrationProposal { proposal, respond } => {
                assert_eq!(proposal.migration_id, runtime_test_migration_id());
                assert_eq!(
                    proposal.proposed_members,
                    vec![alice_member(), bob_member(), carol_member()]
                );
                wait_for_test_reply(respond.reject(RejectionReason::UserDenied))
                    .expect("migration rejection should persist");
                rejected_migration = true;
            }
        }
    }
    assert!(accepted_invitation);
    assert!(rejected_migration);
    assert!(load_pending_group_decisions(store.as_ref()).is_empty());
    assert!(load_pending_group_activations(store.as_ref()).is_empty());
    assert_eq!(
        load_persisted_group(store.as_ref(), invited_group_id).group_id,
        invited_group_id
    );
    drop(runtime);

    let restarted_listener = Arc::new(ListenerStub::default());
    let restarted_runtime =
        load_runtime_with_parts(app_alice_id(), store.clone(), restarted_listener.clone());

    assert!(restarted_listener.take_pending_group_events().is_empty());
    assert!(load_pending_group_decisions(store.as_ref()).is_empty());
    assert!(load_pending_group_activations(store.as_ref()).is_empty());
    wait_for_test_reply(restarted_runtime.shutdown()).expect("restarted runtime should shut down");
}

#[test]
fn runtime_groups_competing_migration_proposals_and_activates_only_the_selected_target() {
    let store = sqlite_store(alice_member());
    let old_group_id = GroupId(Uuid::from_u128(60_120));
    let selected_group_id = GroupId(Uuid::from_u128(60_121));
    let competing_group_id = GroupId(Uuid::from_u128(60_122));
    persist_group_in_store(
        store.as_ref(),
        inactive_group_record(
            old_group_id,
            vec![alice_member(), bob_member()],
            GroupSchema::default(),
        ),
    );
    store_pending_group_decision(
        store.as_ref(),
        migration_proposal_decision(old_group_id, selected_group_id),
    );
    store_pending_group_decision(
        store.as_ref(),
        migration_proposal_decision(old_group_id, competing_group_id),
    );
    let listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts(app_alice_id(), store.clone(), listener.clone());

    listener.wait_for_pending_group_event_count(2);
    assert_eq!(listener.migration_proposal_event_sizes(), vec![2]);
    let events = listener.take_pending_group_events();
    let selected = events
        .into_iter()
        .find_map(|event| match event {
            CapturedPendingGroupEvent::MigrationProposal { proposal, respond }
                if proposal.migration_id.new_group_id == selected_group_id =>
            {
                Some(respond)
            }
            _ => None,
        })
        .expect("selected migration proposal should be exposed");
    wait_for_test_reply(selected.accept()).expect("selected migration should activate");

    assert!(load_pending_group_decisions(store.as_ref()).is_empty());
    assert!(load_pending_group_activations(store.as_ref()).is_empty());
    assert!(load_group_material(store.as_ref(), competing_group_id).is_none());
    assert_eq!(
        load_persisted_group(store.as_ref(), old_group_id).lifecycle,
        ReplicationGroupLifecycle::Closed {
            successor_group_id: selected_group_id,
            final_versions: VersionVector::initial(
                NonZeroUsize::new(2).expect("two old-group members"),
            ),
        }
    );
    assert_eq!(
        load_persisted_group(store.as_ref(), selected_group_id).lifecycle,
        ReplicationGroupLifecycle::Open
    );
    let group_state = runtime
        .group_state()
        .expect("activated group state should be available");
    assert!(matches!(
        group_state
            .group(&old_group_id)
            .expect("source group should remain visible")
            .lifecycle(),
        ReplicationGroupLifecycle::Closed {
            successor_group_id,
            ..
        } if *successor_group_id == selected_group_id
    ));
    assert!(
        !group_state
            .group(&old_group_id)
            .expect("source group should remain visible")
            .is_readable()
    );
    assert_eq!(
        group_state
            .group(&selected_group_id)
            .expect("selected target should be visible")
            .lifecycle(),
        &ReplicationGroupLifecycle::Open
    );
    assert_eq!(
        group_state
            .readable_groups()
            .map(ReplicationGroupView::group_id)
            .collect::<HashSet<_>>(),
        HashSet::from([selected_group_id])
    );
    assert!(group_state.group(&competing_group_id).is_none());
    wait_for_test_reply(runtime.shutdown()).expect("runtime should shut down");
}

#[test]
fn auto_accept_commit_failure_restarts_from_activation_instead_of_listener_decision() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let alice_store = sqlite_store(alice_member.clone());
    let bob_sqlite_store = sqlite_store(bob_member.clone());
    provision_test_security(alice_store.as_ref(), &alice_member, [bob_member.clone()]);
    provision_test_security(
        bob_sqlite_store.as_ref(),
        &bob_member,
        [alice_member.clone()],
    );
    let bob_store = Arc::new(FailingStore::new(bob_sqlite_store.clone()));
    let alice_listener = Arc::new(ListenerStub::default());
    let bob_listener = Arc::new(ListenerStub::default());
    let alice_runtime =
        load_runtime_with_parts(app_alice_id(), alice_store.clone(), alice_listener.clone());
    let auto_accept_config = ReplicationConfig {
        group_invitation_policy: GroupInvitationPolicy {
            creation: PolicyDecision::AutoAccept,
            ..GroupInvitationPolicy::default()
        },
        ..ReplicationConfig::default()
    };
    let bob_runtime = load_runtime_with_parts_and_config(
        app_bob_id(),
        bob_store.clone(),
        bob_listener,
        auto_accept_config.clone(),
    );
    publish_direct_peer_routes(&alice_runtime, &alice_member, &bob_runtime, &bob_member);
    bob_store.fail_after_next_pending_group_commit();

    let group_id = wait_for_test_reply(alice_runtime.create_group(CreateGroupRequest {
        members: vec![alice_member, bob_member],
        group_schema: GroupSchema::default(),
        ..Default::default()
    }))
    .expect("group creation should succeed locally");
    eventually(
        TEST_WAIT_TIMEOUT,
        || !load_pending_group_activations(bob_sqlite_store.as_ref()).is_empty(),
        "auto-accepted work should commit as activation before the injected failure",
    );
    assert!(load_pending_group_decisions(bob_sqlite_store.as_ref()).is_empty());
    assert!(
        load_persisted_groups(bob_sqlite_store.as_ref()).is_empty(),
        "the injected post-commit failure must prevent immediate activation"
    );

    wait_for_test_reply(bob_runtime.shutdown())
        .expect("runtime host should shut down after the induced component fault");
    let restarted_listener = Arc::new(ListenerStub::default());
    let restarted_runtime = load_runtime_with_parts_and_config(
        app_bob_id(),
        bob_sqlite_store.clone(),
        restarted_listener.clone(),
        auto_accept_config,
    );

    eventually(
        TEST_WAIT_TIMEOUT,
        || {
            restarted_runtime
                .membership_snapshot_for_test()
                .contains_group(&group_id)
        },
        "startup should resume the committed auto-accept activation",
    );
    assert!(restarted_listener.take_pending_group_events().is_empty());
    assert!(load_pending_group_activations(bob_sqlite_store.as_ref()).is_empty());
    wait_for_test_reply(restarted_runtime.shutdown()).expect("restarted runtime should shut down");
    wait_for_test_reply(alice_runtime.shutdown()).expect("alice runtime should shut down");
}

#[test]
fn failed_auto_accepted_migration_activation_keeps_published_source_read_only() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let old_group_id = GroupId(Uuid::from_u128(60_123));
    let new_group_id = GroupId(Uuid::from_u128(60_124));
    let members =
        GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member.clone()])
            .expect("group members should build");
    let group_setup = prepare_group_setup_for_members(new_group_id, &alice_member, &members);
    let sqlite_store = sqlite_store(bob_member.clone());
    provision_test_security(sqlite_store.as_ref(), &bob_member, [alice_member.clone()]);
    let store = Arc::new(FailingStore::new(sqlite_store.clone()));
    let runtime = load_runtime_with_parts(
        app_bob_id(),
        store.clone(),
        Arc::new(ListenerStub::default()),
    );
    runtime
        .install_group_for_test(old_group_id, members.clone())
        .expect("source group should install");
    let final_versions = load_persisted_group(sqlite_store.as_ref(), old_group_id).version_vector;
    let proposal = MigrationProposal {
        migration_id: MigrationId {
            old_group_id,
            new_group_id,
        },
        final_versions: final_versions.clone(),
        proposed_members: members.ordered_members(),
        group_schema: GroupSchema::default(),
        initial_snapshot: InitialSnapshot::Empty,
        group_name: Some("replacement".to_owned()),
        message: None,
    };
    store.fail_next_activate_replication_group();

    runtime
        .apply_pending_group_for_test(
            alice_member,
            PendingGroupDecisionRecord::MigrationProposal(proposal),
            group_setup,
        )
        .expect_err("target activation should fail after acceptance commits");

    let expected_lifecycle = ReplicationGroupLifecycle::ReadOnly {
        successor_group_id: new_group_id,
        final_versions,
    };
    assert_eq!(
        load_persisted_group(sqlite_store.as_ref(), old_group_id).lifecycle,
        expected_lifecycle
    );
    let group_state = runtime
        .group_state()
        .expect("accepted source lifecycle should remain available");
    assert_eq!(
        group_state
            .group(&old_group_id)
            .expect("source group should remain published")
            .lifecycle(),
        &expected_lifecycle
    );
    assert!(group_state.group(&new_group_id).is_none());
    wait_for_test_reply(runtime.shutdown()).expect("runtime should shut down");
}

#[test]
fn runtime_resumes_pending_group_activation_with_global_read_token() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let group_id = GroupId(Uuid::from_u128(60_105));
    let unrelated_group_id = GroupId(Uuid::from_u128(60_104));
    let row_key = RowKey(Uuid::from_u128(60_106));
    let store = sqlite_store(alice_member.clone());
    let members = vec![alice_member.clone(), bob_member.clone()];
    let member_count = NonZeroUsize::new(members.len()).expect("group should have members");
    let mut unrelated_versions = VersionVector::initial(member_count);
    unrelated_versions.increment_at(0);
    persist_group_in_store(
        store.as_ref(),
        ReplicationGroupRecord {
            group_id: unrelated_group_id,
            member_keys: test_group_member_keys(members.clone()),
            local_member_index: MemberIndex::new(0),
            group_schema: docs_group_schema(),
            version_vector: unrelated_versions.clone(),
            lifecycle: ReplicationGroupLifecycle::Open,
            security_material: current_slice_placeholder_group_security_material(
                unrelated_group_id,
            ),
            ..Default::default()
        },
    );
    store_inactive_group_material(
        store.as_ref(),
        ReplicationGroupRecord {
            group_id,
            member_keys: test_group_member_keys(members.clone()),
            local_member_index: MemberIndex::new(0),
            group_schema: docs_group_schema(),
            version_vector: VersionVector::initial(member_count),
            lifecycle: ReplicationGroupLifecycle::Open,
            security_material: current_slice_placeholder_group_security_material(group_id),
            ..Default::default()
        },
    );
    store_pending_group_activation(
        store.as_ref(),
        PendingGroupActivationRecord::GroupInvitation(GroupInvitation::new_creation(
            group_id,
            members,
            docs_group_schema(),
            one_title_row_snapshot(dataset_id.clone(), row_key, "activated on startup"),
            None,
            None,
        )),
    );
    let listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts(app_alice_id(), store.clone(), listener.clone());

    listener.wait_for_data_change_count(1);
    assert_eq!(
        listener.captured_data_changes(),
        vec![CapturedDataChange {
            rows: vec![CapturedRowChange::Upsert {
                row_id: RowId {
                    group_id,
                    dataset_id: dataset_id.clone(),
                    row_key,
                },
                title: "activated on startup".to_owned(),
            }],
        }]
    );
    let stored_row = load_persisted_row_slice(store.as_ref(), group_id, &dataset_id, [row_key])
        .rows
        .get(&row_key)
        .cloned()
        .flatten()
        .expect("activated row should persist");
    assert_eq!(stored_row.created_by, Some(UpdateId::INITIAL_STATE_ORIGIN));
    assert_eq!(
        listener.captured_data_change_lineages(),
        vec![DataChangeLineage::Update]
    );
    let read_tokens = listener.captured_data_change_read_tokens();
    let activation_read_token = read_tokens
        .last()
        .expect("activation event should carry a read token");
    assert_eq!(
        activation_read_token.group_version(&group_id),
        Some(&VersionVector::initial(member_count))
    );
    assert_eq!(
        activation_read_token.group_version(&unrelated_group_id),
        Some(&unrelated_versions)
    );
    assert!(load_pending_group_activations(store.as_ref()).is_empty());
    assert!(load_group_material(store.as_ref(), group_id).is_some());
    assert_eq!(
        load_persisted_group(store.as_ref(), group_id).group_id,
        group_id
    );
    wait_for_test_reply(runtime.shutdown()).expect("runtime should shut down");
}

#[test]
fn runtime_resumes_pending_migration_proposal_activation() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let migration_id = MigrationId {
        old_group_id: GroupId(Uuid::from_u128(60_111)),
        new_group_id: GroupId(Uuid::from_u128(60_112)),
    };
    let row_key = RowKey(Uuid::from_u128(60_113));
    let store = sqlite_store(alice_member.clone());
    let members = vec![alice_member, bob_member];
    let final_versions = VersionVector::Full(PureVersionVector::from([4, 0]));
    persist_group_in_store(
        store.as_ref(),
        ReplicationGroupRecord {
            group_id: migration_id.old_group_id,
            member_keys: test_group_member_keys(members.clone()),
            local_member_index: MemberIndex::new(0),
            group_schema: docs_group_schema(),
            version_vector: VersionVector::initial(NonZeroUsize::new(2).unwrap()),
            lifecycle: ReplicationGroupLifecycle::ReadOnly {
                successor_group_id: migration_id.new_group_id,
                final_versions: final_versions.clone(),
            },
            security_material: current_slice_placeholder_group_security_material(
                migration_id.old_group_id,
            ),
            ..Default::default()
        },
    );
    store_pending_group_activation(
        store.as_ref(),
        PendingGroupActivationRecord::MigrationProposal(MigrationProposal {
            migration_id,
            final_versions,
            proposed_members: members,
            group_schema: docs_group_schema(),
            initial_snapshot: one_title_row_snapshot(
                dataset_id.clone(),
                row_key,
                "migration resumed on startup",
            ),
            group_name: None,
            message: None,
        }),
    );
    let listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts(app_alice_id(), store.clone(), listener.clone());

    listener.wait_for_data_change_count(1);
    assert_eq!(
        listener.captured_data_changes(),
        vec![CapturedDataChange {
            rows: vec![CapturedRowChange::Upsert {
                row_id: RowId {
                    group_id: migration_id.new_group_id,
                    dataset_id,
                    row_key,
                },
                title: "migration resumed on startup".to_owned(),
            }],
        }]
    );
    assert_eq!(
        listener.captured_data_change_lineages(),
        vec![DataChangeLineage::GroupReplacement { migration_id }]
    );
    assert!(listener.take_pending_group_events().is_empty());
    assert!(load_pending_group_activations(store.as_ref()).is_empty());
    assert_eq!(
        load_persisted_group(store.as_ref(), migration_id.new_group_id).group_id,
        migration_id.new_group_id
    );
    assert!(matches!(
        load_persisted_group(store.as_ref(), migration_id.old_group_id).lifecycle,
        ReplicationGroupLifecycle::Closed {
            successor_group_id,
            ..
        } if successor_group_id == migration_id.new_group_id
    ));
    wait_for_test_reply(runtime.shutdown()).expect("runtime should shut down");
}

#[test]
fn runtime_keeps_inactive_group_material_hidden_without_accepted_work() {
    let alice_member_id = alice_member();
    let bob_member_id = bob_member();
    let group_id = GroupId(Uuid::from_u128(60_107));
    let store = sqlite_store(alice_member_id.clone());
    store_inactive_group_material(
        store.as_ref(),
        inactive_group_record(
            group_id,
            vec![alice_member_id, bob_member_id],
            GroupSchema::default(),
        ),
    );
    let listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts(app_alice_id(), store.clone(), listener.clone());

    assert!(load_persisted_groups(store.as_ref()).is_empty());
    assert!(load_group_material(store.as_ref(), group_id).is_some());
    assert!(listener.captured_data_changes().is_empty());
    wait_for_test_reply(runtime.shutdown()).expect("runtime should shut down");
}

#[test]
fn runtime_accepts_replayed_invitation_with_stored_group_material() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let group_id = GroupId(Uuid::from_u128(60_108));
    let row_key = RowKey(Uuid::from_u128(60_109));
    let store = sqlite_store(alice_member.clone());
    let members = vec![alice_member.clone(), bob_member.clone()];
    let mut inactive_group = inactive_group_record(group_id, members.clone(), docs_group_schema());
    inactive_group.group_name = Some("stored name".to_owned());
    store_inactive_group_material(store.as_ref(), inactive_group);
    store_pending_group_decision(
        store.as_ref(),
        PendingGroupDecisionRecord::GroupInvitation(GroupInvitation::new_creation(
            group_id,
            members,
            docs_group_schema(),
            InitialSnapshot::Inline(InitialGroupValueRows {
                datasets: vec![InitialDatasetValueRows {
                    dataset_id: dataset_id.clone(),
                    rows: vec![InitialValueRow {
                        row_key,
                        row: title_row_values("accepted from stored material"),
                    }],
                }],
            }),
            Some(String::new()),
            Some(String::new()),
        )),
    );
    let listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts(app_alice_id(), store.clone(), listener.clone());

    listener.wait_for_pending_group_event_count(1);
    let mut events = listener.take_pending_group_events();
    let CapturedPendingGroupEvent::GroupInvitation {
        invitation,
        respond,
    } = events.pop().expect("invitation should be replayed")
    else {
        panic!("expected invitation event");
    };
    assert_eq!(invitation.group_name.as_deref(), Some(""));
    assert_eq!(invitation.message.as_deref(), Some(""));
    wait_for_test_reply(respond.accept()).expect("accept should activate pending group work");

    listener.wait_for_data_change_count(1);
    assert_eq!(
        listener.captured_data_changes(),
        vec![CapturedDataChange {
            rows: vec![CapturedRowChange::Upsert {
                row_id: RowId {
                    group_id,
                    dataset_id,
                    row_key,
                },
                title: "accepted from stored material".to_owned(),
            }],
        }]
    );
    assert!(load_pending_group_decisions(store.as_ref()).is_empty());
    assert!(load_pending_group_activations(store.as_ref()).is_empty());
    assert!(load_group_material(store.as_ref(), group_id).is_some());
    let stored = load_persisted_group(store.as_ref(), group_id);
    assert_eq!(stored.group_id, group_id);
    assert_eq!(stored.group_name.as_deref(), Some(""));
    wait_for_test_reply(runtime.shutdown()).expect("runtime should shut down");
}

/// Prepare proposer-owned setup material for a test target group.
fn prepare_group_setup_for_members(
    group_id: GroupId,
    proposer: &MemberIdentity,
    members: &GroupMembers,
) -> Arc<GroupSetupMessage> {
    let proposer_store = sqlite_store(proposer.clone());
    let peers = members
        .iter()
        .filter(|member| *member != *proposer)
        .collect::<Vec<_>>();
    provision_test_security(proposer_store.as_ref(), proposer, peers);
    let proposer_security = load_test_runtime_security(proposer_store.clone(), proposer);
    let prepared = wait_for_test_reply(ReplicationRuntimeComponent::prepare_group_setup(
        &proposer_security,
        members.len(),
        group_id,
        members,
    ))
    .expect("group setup should prepare");
    Arc::new(prepared.group_setup().clone())
}

/// Build one inline title-row snapshot for activation scenarios.
fn one_title_row_snapshot(dataset_id: DatasetId, row_key: RowKey, title: &str) -> InitialSnapshot {
    InitialSnapshot::Inline(InitialGroupValueRows {
        datasets: vec![InitialDatasetValueRows {
            dataset_id,
            rows: vec![InitialValueRow {
                row_key,
                row: title_row_values(title),
            }],
        }],
    })
}

#[test]
fn active_group_invitation_replay_refreshes_metadata_without_reopening_decision() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let group_id = GroupId(Uuid::from_u128(60_116));
    let members =
        GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member.clone()])
            .expect("group members should build");
    let group_setup = prepare_group_setup_for_members(group_id, &alice_member, &members);

    let bob_store = sqlite_store(bob_member.clone());
    provision_test_security(bob_store.as_ref(), &bob_member, [alice_member.clone()]);
    let listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts(app_bob_id(), bob_store.clone(), listener.clone());
    let invitation = GroupInvitation::new_creation(
        group_id,
        members.ordered_members(),
        GroupSchema::default(),
        InitialSnapshot::Empty,
        Some("first name".to_owned()),
        Some("first message".to_owned()),
    );
    runtime
        .apply_pending_group_for_test(
            alice_member.clone(),
            PendingGroupDecisionRecord::GroupInvitation(invitation.clone()),
            group_setup.clone(),
        )
        .expect("initial invitation should install");
    listener.wait_for_pending_group_event_count(1);
    let mut events = listener.take_pending_group_events();
    let CapturedPendingGroupEvent::GroupInvitation { respond, .. } =
        events.pop().expect("invitation should reach the listener")
    else {
        panic!("expected invitation event");
    };
    wait_for_test_reply(respond.accept()).expect("invitation should activate");
    listener.wait_for_data_change_count(1);
    assert_eq!(
        listener.captured_data_changes(),
        vec![CapturedDataChange { rows: Vec::new() }]
    );
    let activation_tokens = listener.captured_data_change_read_tokens();
    assert_eq!(activation_tokens.len(), 1);
    assert!(activation_tokens[0].group_version(&group_id).is_some());

    let mut replay = invitation;
    replay.group_name = Some(String::new());
    replay.message = None;
    runtime
        .apply_pending_group_for_test(
            alice_member,
            PendingGroupDecisionRecord::GroupInvitation(replay),
            group_setup,
        )
        .expect("active metadata replay should succeed");

    assert!(listener.take_pending_group_events().is_empty());
    let stored = load_persisted_group(bob_store.as_ref(), group_id);
    assert_eq!(stored.group_name.as_deref(), Some(""));
    let group_state = runtime
        .group_state()
        .expect("active replay state should be available");
    assert_eq!(
        group_state
            .group(&group_id)
            .expect("active group should remain published")
            .group_name(),
        Some("")
    );
    wait_for_test_reply(runtime.shutdown()).expect("runtime should shut down");
}

#[test]
fn active_migration_replay_refreshes_metadata_and_ignores_consumed_snapshot() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let old_group_id = GroupId(Uuid::from_u128(60_117));
    let new_group_id = GroupId(Uuid::from_u128(60_118));
    let migration_id = MigrationId {
        old_group_id,
        new_group_id,
    };
    let members =
        GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member.clone()])
            .expect("group members should build");
    let group_setup = prepare_group_setup_for_members(new_group_id, &alice_member, &members);
    let bob_store = sqlite_store(bob_member.clone());
    provision_test_security(bob_store.as_ref(), &bob_member, [alice_member.clone()]);
    let listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts(app_bob_id(), bob_store.clone(), listener.clone());
    runtime
        .install_group_for_test(old_group_id, members.clone())
        .expect("old group should install");
    let final_versions = load_persisted_group(bob_store.as_ref(), old_group_id).version_vector;
    let proposal = MigrationProposal {
        migration_id,
        final_versions,
        proposed_members: members.ordered_members(),
        group_schema: GroupSchema::default(),
        initial_snapshot: InitialSnapshot::Empty,
        group_name: Some("first migration".to_owned()),
        message: Some("first message".to_owned()),
    };
    runtime
        .apply_pending_group_for_test(
            alice_member.clone(),
            PendingGroupDecisionRecord::MigrationProposal(proposal.clone()),
            group_setup.clone(),
        )
        .expect("initial migration proposal should install");
    // Unchanged membership is auto-accepted, so injection activates synchronously.
    assert!(listener.take_pending_group_events().is_empty());
    let old_group_after_activation = load_persisted_group(bob_store.as_ref(), old_group_id);
    let mut expected_new_group = load_persisted_group(bob_store.as_ref(), new_group_id);
    assert_eq!(
        expected_new_group.group_name.as_deref(),
        Some("first migration")
    );

    let mut replay = proposal;
    replay.initial_snapshot = InitialSnapshot::Inline(InitialGroupValueRows {
        datasets: Vec::new(),
    });
    replay.group_name = Some("replayed migration".to_owned());
    replay.message = None;
    runtime
        .apply_pending_group_for_test(
            alice_member,
            PendingGroupDecisionRecord::MigrationProposal(replay),
            group_setup,
        )
        .expect("active migration replay should ignore the consumed snapshot");

    expected_new_group.group_name = Some("replayed migration".to_owned());
    assert!(listener.take_pending_group_events().is_empty());
    assert_eq!(
        load_persisted_group(bob_store.as_ref(), old_group_id),
        old_group_after_activation
    );
    assert_eq!(
        load_persisted_group(bob_store.as_ref(), new_group_id),
        expected_new_group
    );
    let group_state = runtime
        .group_state()
        .expect("active migration replay state should be available");
    assert_eq!(
        group_state
            .group(&new_group_id)
            .expect("active target should remain published")
            .group_name(),
        Some("replayed migration")
    );
    wait_for_test_reply(runtime.shutdown()).expect("runtime should shut down");
}

#[test]
fn migration_proposal_with_changed_schema_is_rejected_before_pending_work_is_stored() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let old_group_id = GroupId(Uuid::from_u128(60_119));
    let new_group_id = GroupId(Uuid::from_u128(60_120));
    let members =
        GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member.clone()])
            .expect("group members should build");
    let group_setup = prepare_group_setup_for_members(new_group_id, &alice_member, &members);
    let store = sqlite_store(bob_member.clone());
    provision_test_security(store.as_ref(), &bob_member, [alice_member.clone()]);
    let listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts_and_application_schemas(
        app_bob_id(),
        &TITLE_APPLICATION_SCHEMAS,
        store.clone(),
        listener,
    );
    runtime
        .install_group_for_test(old_group_id, members.clone())
        .expect("source group should install");
    let final_versions = load_persisted_group(store.as_ref(), old_group_id).version_vector;
    let proposal = MigrationProposal {
        migration_id: MigrationId {
            old_group_id,
            new_group_id,
        },
        final_versions,
        proposed_members: members.ordered_members(),
        group_schema: docs_group_schema_from_schema(title_note_schema_shared()),
        initial_snapshot: InitialSnapshot::Empty,
        group_name: None,
        message: None,
    };

    let error = runtime
        .apply_pending_group_for_test(
            alice_member,
            PendingGroupDecisionRecord::MigrationProposal(proposal),
            group_setup,
        )
        .expect_err("schema-changing migration proposal must be rejected");

    assert!(matches!(
        error,
        InboundDeliveryError::MigrationSchemaMismatch {
            old_group_id: actual_old_group_id,
            new_group_id: actual_new_group_id,
        } if actual_old_group_id == old_group_id && actual_new_group_id == new_group_id
    ));
    assert!(load_pending_group_decisions(store.as_ref()).is_empty());
    assert!(load_pending_group_activations(store.as_ref()).is_empty());
    assert!(
        wait_for_test_future(async {
            let mut transaction = store
                .begin_read_transaction()
                .await
                .expect("read transaction should open");
            let target = transaction
                .load_replication_group_material(&new_group_id)
                .await
                .expect("target material lookup should succeed");
            transaction
                .release()
                .await
                .expect("read transaction should release");
            target
        })
        .is_none()
    );
    wait_for_test_reply(runtime.shutdown()).expect("runtime should shut down");
}

/// Assert manually restored Metadata work cannot transition into activation.
fn assert_metadata_work_accept_rejects_without_activation(record: PendingGroupDecisionRecord) {
    let group_id = record.group_id();
    let store = sqlite_store(alice_member());
    store_pending_group_decision(store.as_ref(), record);
    let listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts(app_alice_id(), store.clone(), listener.clone());

    listener.wait_for_pending_group_event_count(1);
    let mut events = listener.take_pending_group_events();
    let accept = match events.pop().expect("pending work should be replayed") {
        CapturedPendingGroupEvent::GroupInvitation { respond, .. } => respond.accept(),
        CapturedPendingGroupEvent::MigrationProposal { respond, .. } => respond.accept(),
    };
    let result = wait_for_test_reply(accept);

    assert!(
        matches!(result, Err(ApiError::ApiExternal { .. })),
        "metadata accept should fail before activation is persisted: {result:?}"
    );
    assert!(load_pending_group_decisions(store.as_ref()).is_empty());
    assert!(load_pending_group_activations(store.as_ref()).is_empty());
    assert!(load_group_material(store.as_ref(), group_id).is_none());
    wait_for_test_reply(runtime.shutdown()).expect("runtime should shut down");
}

#[test]
fn metadata_pending_group_work_accept_rejects_without_persisting_activation() {
    let alice_member_id = alice_member();
    let bob_member_id = bob_member();
    let member_count = NonZeroUsize::new(2).expect("two members");
    let invitation_group_id = GroupId(Uuid::from_u128(60_110));
    assert_metadata_work_accept_rejects_without_activation(
        PendingGroupDecisionRecord::GroupInvitation(GroupInvitation::new_creation(
            invitation_group_id,
            vec![alice_member_id.clone(), bob_member_id.clone()],
            GroupSchema::default(),
            metadata_initial_snapshot(invitation_group_id, member_count),
            None,
            None,
        )),
    );

    let migration_id = MigrationId {
        old_group_id: GroupId(Uuid::from_u128(60_114)),
        new_group_id: GroupId(Uuid::from_u128(60_115)),
    };
    assert_metadata_work_accept_rejects_without_activation(
        PendingGroupDecisionRecord::MigrationProposal(MigrationProposal {
            migration_id,
            final_versions: VersionVector::Full(PureVersionVector::from([4, 0])),
            proposed_members: vec![alice_member_id, bob_member_id],
            group_schema: GroupSchema::default(),
            initial_snapshot: metadata_initial_snapshot(migration_id.new_group_id, member_count),
            group_name: None,
            message: None,
        }),
    );
}

#[test]
fn stopped_runtime_stale_invitation_accept_reports_unavailable_after_reject() {
    let store = sqlite_store(alice_member());
    let group_id = GroupId(Uuid::from_u128(60_103));
    store_pending_group_decision(store.as_ref(), runtime_test_invitation_decision(group_id));
    let (first_runtime, stale_accept) = replay_one_pending_invitation(store.clone(), group_id);
    wait_for_test_reply(first_runtime.shutdown()).expect("first runtime should shut down");
    let (second_runtime, reject) = replay_one_pending_invitation(store.clone(), group_id);

    wait_for_test_reply(reject.reject(RejectionReason::UserDenied))
        .expect("reject should resolve the decision");
    let stale_result = wait_for_test_reply(stale_accept.accept());
    assert!(
        matches!(stale_result, Err(ApiError::RuntimeUnavailable)),
        "unexpected stale accept result: {stale_result:?}"
    );

    assert!(load_pending_group_decisions(store.as_ref()).is_empty());
    assert!(load_pending_group_activations(store.as_ref()).is_empty());
    wait_for_test_reply(second_runtime.shutdown()).expect("second runtime should shut down");
}

#[test]
fn stopped_runtime_stale_invitation_reject_reports_unavailable_after_accept() {
    let store = sqlite_store(alice_member());
    let group_id = GroupId(Uuid::from_u128(60_104));
    store_pending_group_decision(store.as_ref(), runtime_test_invitation_decision(group_id));
    let (first_runtime, stale_reject) = replay_one_pending_invitation(store.clone(), group_id);
    wait_for_test_reply(first_runtime.shutdown()).expect("first runtime should shut down");
    let (second_runtime, accept) = replay_one_pending_invitation(store.clone(), group_id);

    wait_for_test_reply(accept.accept()).expect("accept should resolve the decision");
    let stale_result = wait_for_test_reply(stale_reject.reject(RejectionReason::UserDenied));
    assert!(
        matches!(stale_result, Err(ApiError::RuntimeUnavailable)),
        "unexpected stale reject result: {stale_result:?}"
    );

    assert!(load_pending_group_decisions(store.as_ref()).is_empty());
    assert!(load_pending_group_activations(store.as_ref()).is_empty());
    assert_eq!(
        load_persisted_group(store.as_ref(), group_id).group_id,
        group_id
    );
    wait_for_test_reply(second_runtime.shutdown()).expect("second runtime should shut down");
}

#[test]
fn runtime_replay_listener_failure_keeps_pending_group_decision() {
    let alice_member = alice_member();
    let store = sqlite_store(alice_member.clone());
    provision_test_security(store.as_ref(), &alice_member, []);
    let group_id = GroupId(Uuid::from_u128(60_102));
    let decision = runtime_test_invitation_decision(group_id);
    let decision_key = decision.key();
    store_pending_group_decision(store.as_ref(), decision);
    let security = load_test_runtime_security(store.clone(), &alice_member);
    let listener = Arc::new(ListenerStub::default());
    listener.reject_pending_group_events();
    let start_result =
        kompact::prelude::block_on(DeliveryRuntimeHost::start_with_runtime_config_toml(
            &alice_member,
            &ApplicationSchemas::EMPTY,
            store.clone(),
            listener.clone(),
            ReplicationConfig::default(),
            security,
            None,
        ));

    match start_result {
        Ok(mut host) => {
            let startup_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                host.wait_for_runtime_startup();
            }));
            assert!(startup_result.is_err());
            let _ = wait_for_test_future(host.shutdown());
        }
        Err(error) => {
            assert!(
                matches!(
                    error,
                    RuntimeHostError::StartComponent {
                        component: "ReplicationRuntimeComponent",
                        ..
                    }
                ),
                "unexpected startup error while replay listener rejects: {error:?}"
            );
        }
    }

    assert_eq!(listener.rejected_pending_group_event_count(), 1);
    let decisions = load_pending_group_decisions(store.as_ref());
    assert_eq!(decisions.len(), 1);
    assert_eq!(decisions[0].key(), decision_key);
    assert!(load_pending_group_activations(store.as_ref()).is_empty());
}
