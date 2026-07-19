//! Group-membership and pending-group workflow scenarios.

use super::*;

#[test]
fn runtime_startup_hydrates_persisted_group_memberships_from_store() {
    let alice_member = alice_member();
    let dataset_id = docs_dataset_id();
    let store = sqlite_store_with_schemas(
        alice_member.clone(),
        [(dataset_id.clone(), title_schema_static())],
    );
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
        },
    );
    let listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts(app_alice_id(), store, listener);
    let row_id = test_row_id(group_id, dataset_id, 32);

    wait_for_group_install(&runtime, group_id);
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
fn create_group_persists_membership_across_runtime_restart() {
    let alice_member = alice_member();
    let dataset_id = docs_dataset_id();
    let store = sqlite_store_with_schemas(
        alice_member.clone(),
        [(dataset_id.clone(), title_schema_static())],
    );
    let first_listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts(app_alice_id(), store.clone(), first_listener);
    let group_id = wait_for_test_reply(runtime.create_group(CreateGroupRequest {
        members: vec![alice_member.clone()],
        group_schema: docs_group_schema(),
    }))
    .expect("create_group should succeed");
    drop(runtime);

    let restarted_listener = Arc::new(ListenerStub::default());
    let restarted_runtime = load_runtime_with_parts(app_alice_id(), store, restarted_listener);
    let row_id = test_row_id(group_id, dataset_id, 33);

    wait_for_group_install(&restarted_runtime, group_id);
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
fn runtime_replays_pending_group_decisions_and_persists_responses_on_startup() {
    let store = sqlite_store_with_schemas(alice_member(), Vec::<(DatasetId, SchemaSource)>::new());
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
    let store = sqlite_store_with_schemas(alice_member(), Vec::<(DatasetId, SchemaSource)>::new());
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
    wait_for_test_reply(runtime.shutdown()).expect("runtime should shut down");
}

#[test]
fn auto_accept_commit_failure_restarts_from_activation_instead_of_listener_decision() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let alice_store = sqlite_store_with_schemas(
        alice_member.clone(),
        Vec::<(DatasetId, SchemaSource)>::new(),
    );
    let bob_sqlite_store =
        sqlite_store_with_schemas(bob_member.clone(), Vec::<(DatasetId, SchemaSource)>::new());
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
        load_runtime_with_parts(app_alice_id(), alice_store, alice_listener.clone());
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
fn runtime_resumes_pending_group_activation_with_global_read_token() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let group_id = GroupId(Uuid::from_u128(60_105));
    let unrelated_group_id = GroupId(Uuid::from_u128(60_104));
    let row_key = RowKey(Uuid::from_u128(60_106));
    let store = sqlite_store_with_schemas(
        alice_member.clone(),
        [(dataset_id.clone(), title_schema_static())],
    );
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
        },
    );
    store_pending_group_activation(
        store.as_ref(),
        PendingGroupActivationRecord::GroupInvitation(GroupInvitation::new_creation(
            group_id,
            members,
            docs_group_schema(),
            InitialSnapshot::Inline(InitialGroupValueRows {
                datasets: vec![InitialDatasetValueRows {
                    dataset_id: dataset_id.clone(),
                    rows: vec![InitialValueRow {
                        row_key,
                        row: title_row_values("activated on startup"),
                    }],
                }],
            }),
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
                    dataset_id,
                    row_key,
                },
                title: "activated on startup".to_owned(),
            }],
        }]
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
    let store = sqlite_store_with_schemas(
        alice_member.clone(),
        [(dataset_id.clone(), title_schema_static())],
    );
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
        },
    );
    store_pending_group_activation(
        store.as_ref(),
        PendingGroupActivationRecord::MigrationProposal(MigrationProposal {
            migration_id,
            final_versions,
            proposed_members: members,
            group_schema: docs_group_schema(),
            initial_snapshot: InitialSnapshot::Inline(InitialGroupValueRows {
                datasets: vec![InitialDatasetValueRows {
                    dataset_id: dataset_id.clone(),
                    rows: vec![InitialValueRow {
                        row_key,
                        row: title_row_values("migration resumed on startup"),
                    }],
                }],
            }),
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
    let store = sqlite_store_with_schemas(
        alice_member_id.clone(),
        Vec::<(DatasetId, SchemaSource)>::new(),
    );
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
    let store = sqlite_store_with_schemas(
        alice_member.clone(),
        [(dataset_id.clone(), title_schema_static())],
    );
    let members = vec![alice_member.clone(), bob_member.clone()];
    store_inactive_group_material(
        store.as_ref(),
        inactive_group_record(group_id, members.clone(), docs_group_schema()),
    );
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
            None,
            None,
        )),
    );
    let listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts(app_alice_id(), store.clone(), listener.clone());

    listener.wait_for_pending_group_event_count(1);
    let mut events = listener.take_pending_group_events();
    let CapturedPendingGroupEvent::GroupInvitation { respond, .. } =
        events.pop().expect("invitation should be replayed")
    else {
        panic!("expected invitation event");
    };
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
    assert_eq!(
        load_persisted_group(store.as_ref(), group_id).group_id,
        group_id
    );
    wait_for_test_reply(runtime.shutdown()).expect("runtime should shut down");
}

/// Assert manually restored Metadata work cannot transition into activation.
fn assert_metadata_work_accept_rejects_without_activation(record: PendingGroupDecisionRecord) {
    let group_id = record.group_id();
    let store = sqlite_store_with_schemas(alice_member(), Vec::<(DatasetId, SchemaSource)>::new());
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
    let store = sqlite_store_with_schemas(alice_member(), Vec::<(DatasetId, SchemaSource)>::new());
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
    let store = sqlite_store_with_schemas(alice_member(), Vec::<(DatasetId, SchemaSource)>::new());
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
