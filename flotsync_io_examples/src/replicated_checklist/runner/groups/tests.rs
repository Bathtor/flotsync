//! Tests for checklist group registry and invitation behaviour.

use super::{test_support::*, *};
use crate::replicated_checklist::{
    ChecklistItem,
    runner::repl::{ChecklistListener, load_group_snapshot},
};
use flotsync_core::versions::VersionVector;
use flotsync_data_types::RowValues;
use flotsync_replication::{
    InitialSnapshot,
    ReplicationGroupLifecycle,
    RowId,
    RowKey,
    security::{KnownMemberKeyReport, KnownMemberReport, MemberKeyTrustReport},
    test_support::snapshot_read_token,
};
use std::{cell::Cell, sync::Mutex};

#[test]
fn creation_request_keeps_creator_first_and_uses_the_checklist_schema() {
    let creator = MemberIdentity::from_array(["alice"]);
    let bob = MemberIdentity::from_array(["bob"]);
    let carol = MemberIdentity::from_array(["carol"]);

    let request = checklist_group_creation_request(
        "shared errands".to_owned(),
        creator.clone(),
        vec![bob.clone(), carol.clone()],
    );

    assert_eq!(request.group_name.as_deref(), Some("shared errands"));
    assert_eq!(request.message, None);
    assert_eq!(request.members, vec![creator, bob, carol]);
    assert_eq!(request.group_schema, *CHECKLIST_GROUP_SCHEMA);
}

#[test]
fn known_member_reference_sorts_members_and_fingerprints_at_the_ui_boundary() {
    let alice = MemberIdentity::from_array(["alice"]);
    let bob = MemberIdentity::from_array(["bob"]);
    let low_fingerprint = KeyFingerprint::from_bytes([1; 32]);
    let high_fingerprint = KeyFingerprint::from_bytes([2; 32]);
    let mut report = KnownMemberKeysReport {
        members: vec![
            KnownMemberReport {
                member_id: bob.clone(),
                keys: vec![KnownMemberKeyReport {
                    fingerprint: low_fingerprint,
                    trust: MemberKeyTrustReport {
                        has_local_explicit_trust: false,
                    },
                }],
            },
            KnownMemberReport {
                member_id: alice.clone(),
                keys: vec![
                    KnownMemberKeyReport {
                        fingerprint: high_fingerprint,
                        trust: MemberKeyTrustReport {
                            has_local_explicit_trust: false,
                        },
                    },
                    KnownMemberKeyReport {
                        fingerprint: low_fingerprint,
                        trust: MemberKeyTrustReport {
                            has_local_explicit_trust: false,
                        },
                    },
                ],
            },
        ],
    };

    sort_known_members_for_display(&mut report);

    assert_eq!(
        report
            .members
            .iter()
            .map(|member| member.member_id.clone())
            .collect::<Vec<_>>(),
        vec![alice, bob]
    );
    assert_eq!(
        report.members[0]
            .keys
            .iter()
            .map(|key| key.fingerprint)
            .collect::<Vec<_>>(),
        vec![low_fingerprint, high_fingerprint]
    );
}

#[test]
fn additional_group_members_read_one_per_line_until_blank() {
    let creator = MemberIdentity::from_array(["alice"]);
    let bob = MemberIdentity::from_array(["bob"]);
    let mut member_inputs = ["bob", "carol", ""].into_iter();
    let mut read_member = || {
        Ok(member_inputs
            .next()
            .expect("member input should include a terminating blank")
            .to_owned())
    };
    assert_eq!(
        read_additional_group_members(&mut read_member, &creator)
            .expect("distinct valid members should parse"),
        vec![bob.clone(), MemberIdentity::from_array(["carol"])]
    );

    let mut member_inputs = ["alice"].into_iter();
    let mut read_member = || {
        Ok(member_inputs
            .next()
            .expect("creator input should be read")
            .to_owned())
    };
    assert!(matches!(
        read_additional_group_members(&mut read_member, &creator),
        Err(ReplicatedChecklistError::RepeatedGroupCreator { member_id })
            if member_id == creator
    ));

    let mut member_inputs = ["bob", "bob"].into_iter();
    let mut read_member = || {
        Ok(member_inputs
            .next()
            .expect("duplicate inputs should be read")
            .to_owned())
    };
    assert!(matches!(
        read_additional_group_members(&mut read_member, &creator),
        Err(ReplicatedChecklistError::DuplicateGroupMember { member_id })
            if member_id == bob
    ));

    let mut member_inputs = ["bad!"].into_iter();
    let mut read_member = || {
        Ok(member_inputs
            .next()
            .expect("invalid input should be read")
            .to_owned())
    };
    assert!(matches!(
        read_additional_group_members(&mut read_member, &creator),
        Err(ReplicatedChecklistError::InvalidGroupMemberIdentity {
            source: IdentifierParseError::InvalidSegment { .. }
        })
    ));
}

#[test]
fn listener_queues_group_invitations_without_deciding_them() {
    let (listener, receivers) = ChecklistListener::pair();
    let decisions = Arc::new(Mutex::new(Vec::new()));
    let group_id = GroupId::new_random();
    let invitation = GroupInvitation::new_creation(
        group_id,
        vec![MemberIdentity::from_array(["alice"])],
        CHECKLIST_GROUP_SCHEMA.clone(),
        InitialSnapshot::Empty,
        Some("shared".to_owned()),
        Some("join me".to_owned()),
    );

    block_on(listener.on_event(ReplicationEvent::GroupInvitation {
        invitation: invitation.clone(),
        respond: Box::new(RecordingInvitationResponder {
            decisions: decisions.clone(),
            accepted_event: None,
        }),
    }))
    .expect("listener should queue invitation");

    let pending = receivers
        .invitations
        .try_recv()
        .expect("invitation should be queued");
    assert_eq!(pending.invitation, invitation);
    assert!(
        decisions
            .lock()
            .expect("decision lock should be available")
            .is_empty()
    );
    block_on(pending.respond.reject(RejectionReason::UserDenied))
        .expect("queued responder should remain usable");
    assert_eq!(
        *decisions.lock().expect("decision lock should be available"),
        vec![RecordedInvitationDecision::Rejected(
            RejectionReason::UserDenied
        )]
    );
}

#[test]
fn creation_handler_uses_injected_prompts_refreshes_groups_and_keeps_default_clear() {
    let member = MemberIdentity::from_array(["alice"]);
    let (store, runtime, _listener, receivers) =
        load_test_runtime_with_groups(&member, std::iter::empty());
    let session = ChecklistSession::new([], ChecklistWorkingSet::new());
    let mut repl = ChecklistRepl::new(
        test_app_config(member),
        store,
        runtime.clone(),
        receivers,
        session,
    );
    let mut read_members = || Ok(String::new());
    let confirmation_count = Cell::new(0);
    let mut confirm_creation = |prompt: &str| {
        assert_eq!(prompt, "Create this group and send invitations?");
        confirmation_count.set(confirmation_count.get() + 1);
        Ok(true)
    };

    block_on(repl.create_group_with_prompts(
        vec!["shared".to_owned()],
        &mut read_members,
        &mut confirm_creation,
    ))
    .expect("creation handler should complete");

    assert_eq!(confirmation_count.get(), 1);
    assert_eq!(repl.session.default_group, None);
    assert_eq!(repl.session.groups.len(), 1);
    assert_eq!(
        repl.session
            .groups
            .values()
            .next()
            .and_then(|group| group.group_name.as_deref()),
        Some("shared")
    );
    assert!(repl.session.working_set.read_token().is_ok());

    block_on(runtime.shutdown()).expect("test runtime should shut down");
}

#[test]
fn invitation_accept_handler_applies_listener_rows_and_keeps_the_default() {
    let member = MemberIdentity::from_array(["alice"]);
    let default_group_id = GroupId::new_random();
    let invited_group_id = GroupId::new_random();
    let (store, runtime, listener, receivers) =
        load_test_runtime_with_groups(&member, [default_group_id, invited_group_id]);
    let groups = block_on(load_readable_groups(store.as_ref())).expect("test groups should load");
    let mut working_set = ChecklistWorkingSet::new();
    block_on(load_group_snapshot(
        runtime.as_ref(),
        &mut working_set,
        default_group_id,
    ))
    .expect("default group snapshot should load");
    let mut session = ChecklistSession::new(groups, working_set);
    session.default_group = Some(default_group_id);
    let mut repl = ChecklistRepl::new(
        test_app_config(member.clone()),
        store,
        runtime.clone(),
        receivers,
        session,
    );
    let decisions = Arc::new(Mutex::new(Vec::new()));
    let row_key = RowKey(Uuid::from_u128(72_001));
    let read_token =
        snapshot_read_token(runtime.as_ref(), invited_group_id, checklist_dataset_id());
    let row_patch = ChecklistItem::new("listener row").to_row_values_patch();
    let row_values = RowValues::try_from_fields(&CHECKLIST_SCHEMA, row_patch.fields)
        .expect("listener test row should match the checklist schema");
    let invitation = GroupInvitation::new_creation(
        invited_group_id,
        vec![member.clone()],
        CHECKLIST_GROUP_SCHEMA.clone(),
        InitialSnapshot::Empty,
        Some("invited".to_owned()),
        None,
    );
    block_on(listener.on_event(ReplicationEvent::GroupInvitation {
        invitation,
        respond: Box::new(RecordingInvitationResponder {
            decisions: decisions.clone(),
            accepted_event: Some(AcceptedListenerEvent {
                listener: listener.clone(),
                read_token,
                changes: vec![RowChange::Upsert {
                    row_id: RowId {
                        group_id: invited_group_id,
                        dataset_id: checklist_dataset_id(),
                        row_key,
                    },
                    row: Arc::new(row_values),
                }],
            }),
        }),
    }))
    .expect("invitation should reach the listener");

    assert!(
        block_on(repl.handle_command(ChecklistCommand::Group {
            command: ChecklistGroupCommand::Accept {
                invitation: NonZeroUsize::MIN,
            },
        }))
        .expect("accept command should succeed")
    );
    assert_eq!(repl.session.default_group, Some(default_group_id));
    assert_eq!(
        repl.session
            .working_set
            .item(ChecklistItemId::group(invited_group_id, row_key))
            .expect("listener-delivered row should be visible")
            .text,
        "listener row"
    );
    assert_eq!(
        *decisions.lock().expect("decision lock should be available"),
        vec![RecordedInvitationDecision::Accepted]
    );

    block_on(runtime.shutdown()).expect("test runtime should shut down");
}

#[test]
fn invitation_reject_handler_reports_user_denied_without_changing_the_default() {
    let member = MemberIdentity::from_array(["alice"]);
    let (store, runtime, listener, receivers) =
        load_test_runtime_with_groups(&member, std::iter::empty());
    let session = ChecklistSession::new([], ChecklistWorkingSet::new());
    let mut repl = ChecklistRepl::new(
        test_app_config(member.clone()),
        store,
        runtime.clone(),
        receivers,
        session,
    );
    let decisions = Arc::new(Mutex::new(Vec::new()));
    let invitation = GroupInvitation::new_creation(
        GroupId::new_random(),
        vec![member],
        CHECKLIST_GROUP_SCHEMA.clone(),
        InitialSnapshot::Empty,
        None,
        None,
    );
    block_on(listener.on_event(ReplicationEvent::GroupInvitation {
        invitation,
        respond: Box::new(RecordingInvitationResponder {
            decisions: decisions.clone(),
            accepted_event: None,
        }),
    }))
    .expect("rejected invitation should reach the listener");
    assert!(
        block_on(repl.handle_command(ChecklistCommand::Group {
            command: ChecklistGroupCommand::Reject {
                invitation: NonZeroUsize::MIN,
            },
        }))
        .expect("reject command should succeed")
    );
    assert_eq!(
        *decisions.lock().expect("decision lock should be available"),
        vec![RecordedInvitationDecision::Rejected(
            RejectionReason::UserDenied
        )]
    );
    assert_eq!(repl.session.default_group, None);

    block_on(runtime.shutdown()).expect("test runtime should shut down");
}

#[test]
fn created_group_is_visible_to_the_store_registry_and_listener() {
    let member = MemberIdentity::from_array(["alice"]);
    let (store, runtime, _listener, receivers) =
        load_test_runtime_with_groups(&member, std::iter::empty());
    let request = checklist_group_creation_request("shared".to_owned(), member, Vec::new());

    let group_id = block_on(runtime.create_group(request)).expect("group should be created");
    let groups =
        block_on(load_readable_groups(store.as_ref())).expect("temporary registry should reload");
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].group_id, group_id);
    assert_eq!(groups[0].group_name.as_deref(), Some("shared"));

    let listener_batch = receivers
        .batches
        .try_recv()
        .expect("created group should deliver its read position through the listener");
    assert!(listener_batch.changes.is_empty());
    let mut working_set = ChecklistWorkingSet::new();
    working_set.merge_read_token(listener_batch.read_token);
    assert!(working_set.listed_items().is_empty());
    assert!(working_set.read_token().is_ok());

    block_on(runtime.shutdown()).expect("test runtime should shut down");
}

#[test]
fn group_registry_resolves_uuid_before_exact_names_and_reports_ambiguity() {
    let member = MemberIdentity::from_array(["alice"]);
    let uuid_selected_id = GroupId(Uuid::from_u128(71_001));
    let uuid_named_id = GroupId(Uuid::from_u128(71_002));
    let first_shared_id = GroupId(Uuid::from_u128(71_003));
    let second_shared_id = GroupId(Uuid::from_u128(71_004));
    let mut session = ChecklistSession::new(
        [
            named_test_group(uuid_selected_id, &member, "ordinary"),
            named_test_group(uuid_named_id, &member, &uuid_selected_id.to_string()),
            named_test_group(first_shared_id, &member, "shared"),
            named_test_group(second_shared_id, &member, "shared"),
        ],
        ChecklistWorkingSet::new(),
    );

    assert_eq!(
        session
            .resolve_group(&uuid_selected_id.to_string())
            .expect("UUID should resolve")
            .group_id,
        uuid_selected_id
    );
    let error = session
        .resolve_group("shared")
        .expect_err("duplicate names should be ambiguous");
    assert!(matches!(
        error,
        ReplicatedChecklistError::AmbiguousGroupName { candidate_ids, .. }
            if candidate_ids == vec![first_shared_id, second_shared_id]
    ));

    assert_eq!(
        session
            .set_default("ordinary")
            .expect("writable named group should become default"),
        uuid_selected_id
    );
    assert_eq!(session.default_group, Some(uuid_selected_id));
    session.default_group = None;
    assert!(matches!(
        session.default_group(),
        Err(ReplicatedChecklistError::NoDefaultGroup)
    ));
}

#[test]
fn default_selection_rejects_read_only_groups() {
    let member = MemberIdentity::from_array(["alice"]);
    let group_id = GroupId(Uuid::from_u128(71_005));
    let mut group = named_test_group(group_id, &member, "archived");
    group.lifecycle = ReplicationGroupLifecycle::ReadOnly {
        successor_group_id: GroupId(Uuid::from_u128(71_006)),
        final_versions: VersionVector::initial(NonZeroUsize::MIN),
    };
    let mut session = ChecklistSession::new([group], ChecklistWorkingSet::new());

    assert!(matches!(
        session.set_default("archived"),
        Err(ReplicatedChecklistError::NonWritableDefaultGroup {
            group_id: actual
        }) if actual == group_id
    ));
    assert_eq!(session.default_group, None);
}

#[test]
fn listener_group_validation_rejects_rows_outside_the_registry() {
    let member = MemberIdentity::from_array(["alice"]);
    let known_group = GroupId(Uuid::from_u128(71_007));
    let unknown_group = GroupId(Uuid::from_u128(71_008));
    let session = ChecklistSession::new(
        [test_group(known_group, &member)],
        ChecklistWorkingSet::new(),
    );
    let known = RowChange::Delete {
        row_id: RowId {
            group_id: known_group,
            dataset_id: checklist_dataset_id(),
            row_key: RowKey(Uuid::from_u128(1)),
        },
    };
    let unknown = RowChange::Delete {
        row_id: RowId {
            group_id: unknown_group,
            dataset_id: checklist_dataset_id(),
            row_key: RowKey(Uuid::from_u128(2)),
        },
    };

    session
        .validate_listener_changes(&[known])
        .expect("known group should validate");
    assert!(matches!(
        session.validate_listener_changes(&[unknown]),
        Err(ReplicatedChecklistError::UnknownListenerGroup { group_id })
            if group_id == unknown_group
    ));
}

#[test]
fn registry_refresh_reassigns_a_non_writable_default_to_its_open_successor() {
    let member = MemberIdentity::from_array(["alice"]);
    let previous_group = GroupId(Uuid::from_u128(71_009));
    let intermediate_group = GroupId(Uuid::from_u128(71_010));
    let open_group = GroupId(Uuid::from_u128(71_011));
    let mut previous = test_group(previous_group, &member);
    previous.lifecycle = ReplicationGroupLifecycle::ReadOnly {
        successor_group_id: intermediate_group,
        final_versions: VersionVector::initial(NonZeroUsize::MIN),
    };
    let mut intermediate = test_group(intermediate_group, &member);
    intermediate.lifecycle = ReplicationGroupLifecycle::Closed {
        successor_group_id: open_group,
        final_versions: VersionVector::initial(NonZeroUsize::MIN),
    };
    let open = test_group(open_group, &member);
    let mut session = ChecklistSession::new([previous.clone()], ChecklistWorkingSet::new());
    session.default_group = Some(previous_group);

    let refresh = session.refresh_groups([previous, intermediate, open]);

    assert_eq!(
        refresh,
        DefaultGroupRefresh::Reassigned {
            previous_group_id: previous_group,
            successor_group_id: open_group,
        }
    );
    assert_eq!(session.default_group, Some(open_group));
    assert!(session.groups.contains_key(&previous_group));
    assert!(!session.groups.contains_key(&intermediate_group));
    assert!(session.groups.contains_key(&open_group));

    let mut closed_default_session = ChecklistSession::new([], ChecklistWorkingSet::new());
    closed_default_session.default_group = Some(intermediate_group);
    let mut closed = test_group(intermediate_group, &member);
    closed.lifecycle = ReplicationGroupLifecycle::Closed {
        successor_group_id: open_group,
        final_versions: VersionVector::initial(NonZeroUsize::MIN),
    };
    assert_eq!(
        closed_default_session.refresh_groups([
            test_group(previous_group, &member),
            closed,
            test_group(open_group, &member),
        ]),
        DefaultGroupRefresh::Reassigned {
            previous_group_id: intermediate_group,
            successor_group_id: open_group,
        }
    );
    assert_eq!(closed_default_session.default_group, Some(open_group));
}

#[test]
fn registry_refresh_clears_a_default_without_a_resolvable_open_successor() {
    let member = MemberIdentity::from_array(["alice"]);
    let first_group = GroupId(Uuid::from_u128(71_012));
    let second_group = GroupId(Uuid::from_u128(71_013));
    let mut first = test_group(first_group, &member);
    first.lifecycle = ReplicationGroupLifecycle::ReadOnly {
        successor_group_id: second_group,
        final_versions: VersionVector::initial(NonZeroUsize::MIN),
    };
    let mut second = test_group(second_group, &member);
    second.lifecycle = ReplicationGroupLifecycle::ReadOnly {
        successor_group_id: first_group,
        final_versions: VersionVector::initial(NonZeroUsize::MIN),
    };
    let mut session =
        ChecklistSession::new([first.clone(), second.clone()], ChecklistWorkingSet::new());
    session.default_group = Some(first_group);

    let refresh = session.refresh_groups([first, second]);

    assert_eq!(
        refresh,
        DefaultGroupRefresh::Cleared {
            previous_group_id: first_group,
        }
    );
    assert_eq!(session.default_group, None);
}

#[test]
fn registry_refresh_handles_open_missing_and_restart_defaults() {
    let member = MemberIdentity::from_array(["alice"]);
    let open_group = GroupId(Uuid::from_u128(71_014));
    let missing_successor = GroupId(Uuid::from_u128(71_015));
    let unavailable_group = GroupId(Uuid::from_u128(71_016));
    let open = test_group(open_group, &member);
    let mut unavailable = test_group(unavailable_group, &member);
    unavailable.lifecycle = ReplicationGroupLifecycle::ReadOnly {
        successor_group_id: missing_successor,
        final_versions: VersionVector::initial(NonZeroUsize::MIN),
    };
    let mut session = ChecklistSession::new([open.clone()], ChecklistWorkingSet::new());
    session.default_group = Some(open_group);

    assert_eq!(
        session.refresh_groups([open.clone(), unavailable.clone()]),
        DefaultGroupRefresh::Unchanged
    );
    assert_eq!(session.default_group, Some(open_group));

    session.default_group = Some(unavailable_group);
    assert_eq!(
        session.refresh_groups([open.clone(), unavailable]),
        DefaultGroupRefresh::Cleared {
            previous_group_id: unavailable_group,
        }
    );
    assert_eq!(session.default_group, None);

    let mut restarted = ChecklistSession::new([open.clone()], ChecklistWorkingSet::new());
    assert_eq!(restarted.default_group, None);
    assert_eq!(
        restarted.refresh_groups([open]),
        DefaultGroupRefresh::Unchanged
    );
    assert_eq!(restarted.default_group, None);
}

#[test]
fn readable_group_loader_supports_zero_and_several_groups() {
    let member = MemberIdentity::from_array(["alice"]);
    let store = block_on(SqliteReplicationStore::in_memory(member.clone()))
        .expect("test store should open");

    let empty = block_on(load_readable_groups(&store)).expect("zero-group store should load");
    assert!(empty.is_empty());

    let first_group_id = GroupId(Uuid::from_u128(70_001));
    block_on(insert_test_group(
        &store,
        test_group(first_group_id, &member),
    ));
    let second_group_id = GroupId(Uuid::from_u128(70_002));
    block_on(insert_test_group(
        &store,
        test_group(second_group_id, &member),
    ));
    let closed_group_id = GroupId(Uuid::from_u128(70_003));
    let mut closed_group = test_group(closed_group_id, &member);
    closed_group.lifecycle = ReplicationGroupLifecycle::Closed {
        successor_group_id: GroupId(Uuid::from_u128(70_004)),
        final_versions: VersionVector::initial(NonZeroUsize::MIN),
    };
    block_on(insert_test_group(&store, closed_group));

    let all_groups = block_on(load_group_records(&store)).expect("all groups should load");
    assert_eq!(
        all_groups
            .iter()
            .map(|group| group.group_id)
            .collect::<Vec<_>>(),
        vec![first_group_id, second_group_id, closed_group_id]
    );

    let loaded =
        block_on(load_readable_groups(&store)).expect("several readable groups should load");
    assert_eq!(
        loaded
            .into_iter()
            .map(|group| group.group_id)
            .collect::<Vec<_>>(),
        vec![first_group_id, second_group_id]
    );
}
