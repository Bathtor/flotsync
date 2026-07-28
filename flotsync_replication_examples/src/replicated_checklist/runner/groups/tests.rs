//! Tests for checklist group registry and invitation behaviour.

use super::{test_support::*, *};
use crate::replicated_checklist::{
    ChecklistItem,
    runner::repl::{ChecklistListener, load_group_snapshot},
};
use flotsync_core::versions::VersionVector;
use flotsync_data_types::RowValues;
use flotsync_replication::{
    InitialGroupValueRows,
    InitialSnapshot,
    InitialSnapshotMetadata,
    MigrationId,
    ReplicationGroupLifecycle,
    RowId,
    RowKey,
    SnapshotRef,
    security::{KnownMemberKeyReport, KnownMemberReport, MemberKeyTrustReport},
    test_support::{replication_group_snapshot, snapshot_read_token},
};
use indoc::indoc;
use std::{cell::Cell, sync::Mutex};

fn test_group_state(
    groups: impl IntoIterator<Item = flotsync_replication::ReplicationGroupRecord>,
) -> Arc<dyn ReplicationGroupSnapshot> {
    let groups = groups.into_iter().collect::<Vec<_>>();
    let local_member = groups
        .first()
        .expect("test group state should contain at least one group")
        .local_member()
        .clone();
    replication_group_snapshot(&local_member, groups)
}

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
fn group_creation_summary_uses_display_formatted_schema() {
    let request = checklist_group_creation_request(
        "shared errands".to_owned(),
        MemberIdentity::from_array(["alice"]),
        vec![MemberIdentity::from_array(["bob"])],
    );

    assert_eq!(
        format_group_creation_summary(&request),
        indoc! {"
            group creation summary:
              name: shared errands
              message: none
              members:
                0: alice
                1: bob
              schema:
                checklist_items:
                  SCHEMA (
                    edit_count UINT NOT NULL USING MONOTONIC_COUNTER,
                    note STRING NOT NULL USING LINEAR_STRING,
                    priority BYTE NOT NULL USING LATEST_VALUE_WINS,
                    status STRING NOT NULL USING TOTAL_ORDER_FSM(['open', 'in_progress', 'done']),
                    tags ARRAY<STRING> NOT NULL USING LINEAR_LIST,
                    text STRING NOT NULL USING LINEAR_STRING
                  )
        "}
    );
}

#[test]
fn group_invitation_uses_user_facing_creation_format() {
    let group_id = GroupId(Uuid::from_u128(1));
    let invitation = GroupInvitation::new_creation(
        group_id,
        vec![
            MemberIdentity::from_array(["alice"]),
            MemberIdentity::from_array(["bob"]),
        ],
        CHECKLIST_GROUP_SCHEMA.clone(),
        InitialSnapshot::Empty,
        Some("shared errands".to_owned()),
        Some("join me".to_owned()),
    );

    assert_eq!(
        format_group_invitation(NonZeroUsize::new(2).expect("two is non-zero"), &invitation),
        indoc! {"
            2. group 00000000-0000-0000-0000-000000000001
              source: creation
              name: shared errands
              message: join me
              members:
                0: alice
                1: bob
              schema:
                checklist_items:
                  SCHEMA (
                    edit_count UINT NOT NULL USING MONOTONIC_COUNTER,
                    note STRING NOT NULL USING LINEAR_STRING,
                    priority BYTE NOT NULL USING LATEST_VALUE_WINS,
                    status STRING NOT NULL USING TOTAL_ORDER_FSM(['open', 'in_progress', 'done']),
                    tags ARRAY<STRING> NOT NULL USING LINEAR_LIST,
                    text STRING NOT NULL USING LINEAR_STRING
                  )
              initial snapshot: empty
        "}
    );
}

#[test]
fn group_invitation_formats_migration_metadata_and_absent_optional_values() {
    let old_group_id = GroupId(Uuid::from_u128(2));
    let new_group_id = GroupId(Uuid::from_u128(3));
    let invitation = GroupInvitation::new_migration(
        MigrationId {
            old_group_id,
            new_group_id,
        },
        vec![MemberIdentity::from_array(["alice"])],
        GroupSchema::default(),
        InitialSnapshot::Metadata(InitialSnapshotMetadata {
            primary_ref: SnapshotRef {
                group_id: old_group_id,
                versions: VersionVector::initial(NonZeroUsize::MIN),
            },
            equivalent_refs: Vec::new().into(),
            record_count: None,
        }),
        None,
        None,
    );

    assert_eq!(
        format_group_invitation(NonZeroUsize::MIN, &invitation),
        indoc! {"
            1. group 00000000-0000-0000-0000-000000000003
              source: migration 00000000-0000-0000-0000-000000000002->00000000-0000-0000-0000-000000000003
              name: <unnamed>
              message: <none>
              members:
                0: alice
              schema:
                none
              initial snapshot: metadata (primary group: 00000000-0000-0000-0000-000000000002; versions: 〈0-0:0〉; equivalent references: 0; records: unknown)
        "}
    );
    assert_eq!(
        format_initial_snapshot(&InitialSnapshot::Inline(InitialGroupValueRows::default())),
        "inline (datasets: 0; rows: 0)"
    );
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
    let (_store, runtime, _listener, receivers) =
        load_test_runtime_with_groups(&member, std::iter::empty());
    let session = ChecklistSession::new(ChecklistWorkingSet::new());
    let mut repl = ChecklistRepl::new(test_app_config(member), runtime.clone(), receivers, session);
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
    let group_state = runtime
        .group_state()
        .expect("group state should be available");
    assert_eq!(group_state.groups().count(), 1);
    assert_eq!(
        group_state
            .groups()
            .next()
            .and_then(|group| group.group_name()),
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
    let (_store, runtime, listener, receivers) =
        load_test_runtime_with_groups(&member, [default_group_id, invited_group_id]);
    let mut working_set = ChecklistWorkingSet::new();
    block_on(load_group_snapshot(
        runtime.as_ref(),
        &mut working_set,
        default_group_id,
    ))
    .expect("default group snapshot should load");
    let mut session = ChecklistSession::new(working_set);
    session.default_group = Some(default_group_id);
    let mut repl = ChecklistRepl::new(
        test_app_config(member.clone()),
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
    let (_store, runtime, listener, receivers) =
        load_test_runtime_with_groups(&member, std::iter::empty());
    let session = ChecklistSession::new(ChecklistWorkingSet::new());
    let mut repl = ChecklistRepl::new(
        test_app_config(member.clone()),
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
fn created_group_is_visible_to_the_runtime_registry_and_listener() {
    let member = MemberIdentity::from_array(["alice"]);
    let (_store, runtime, _listener, receivers) =
        load_test_runtime_with_groups(&member, std::iter::empty());
    let request = checklist_group_creation_request("shared".to_owned(), member, Vec::new());

    let group_id = block_on(runtime.create_group(request)).expect("group should be created");
    let groups = runtime
        .group_state()
        .expect("group state should be available");
    assert_eq!(groups.groups().count(), 1);
    let group = groups
        .group(&group_id)
        .expect("created group should be visible");
    assert_eq!(group.group_id(), group_id);
    assert_eq!(group.group_name(), Some("shared"));

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
    let groups = test_group_state([
        named_test_group(uuid_selected_id, &member, "ordinary"),
        named_test_group(uuid_named_id, &member, &uuid_selected_id.to_string()),
        named_test_group(first_shared_id, &member, "shared"),
        named_test_group(second_shared_id, &member, "shared"),
    ]);
    let mut session = ChecklistSession::new(ChecklistWorkingSet::new());

    assert_eq!(
        ChecklistSession::resolve_group(groups.as_ref(), &uuid_selected_id.to_string())
            .expect("UUID should resolve")
            .group_id(),
        uuid_selected_id
    );
    let error = match ChecklistSession::resolve_group(groups.as_ref(), "shared") {
        Ok(group) => panic!(
            "duplicate names unexpectedly resolved to {}",
            group.group_id()
        ),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        ReplicatedChecklistError::AmbiguousGroupName { candidate_ids, .. }
            if candidate_ids == vec![first_shared_id, second_shared_id]
    ));

    assert_eq!(
        session
            .set_default(groups.as_ref(), "ordinary")
            .expect("writable named group should become default"),
        uuid_selected_id
    );
    assert_eq!(session.default_group, Some(uuid_selected_id));
    session.default_group = None;
    assert!(matches!(
        session.default_group(groups.as_ref()),
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
    let groups = test_group_state([group]);
    let mut session = ChecklistSession::new(ChecklistWorkingSet::new());

    assert!(matches!(
        session.set_default(groups.as_ref(), "archived"),
        Err(ReplicatedChecklistError::NonWritableDefaultGroup {
            group_id: actual
        }) if actual == group_id
    ));
    assert!(matches!(
        ChecklistSession::resolve_target_association(groups.as_ref(), "archived"),
        Err(ReplicatedChecklistError::NonWritableTargetGroup {
            group_id: actual
        }) if actual == group_id
    ));
    assert_eq!(session.default_group, None);
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "The assertions cover the complete item-selector precedence in one scenario."
)]
fn item_references_resolve_qualified_and_bare_workspace_identities() {
    let member = MemberIdentity::from_array(["alice"]);
    let shared_group = GroupId(Uuid::from_u128(71_051));
    let other_group = GroupId(Uuid::from_u128(71_052));
    let missing_group = GroupId(Uuid::from_u128(71_057));
    let unique_row = RowKey(Uuid::from_u128(72_051));
    let repeated_row = RowKey(Uuid::from_u128(72_052));
    let missing_row = RowKey(Uuid::from_u128(72_057));
    let local_id = ChecklistItemId::local(repeated_row);
    let shared_unique_id = ChecklistItemId::group(shared_group, unique_row);
    let shared_repeated_id = ChecklistItemId::group(shared_group, repeated_row);
    let other_repeated_id = ChecklistItemId::group(other_group, repeated_row);
    let missing_group_id = ChecklistItemId::group(missing_group, missing_row);
    let mut working_set = ChecklistWorkingSet::new();
    working_set.add_item_with_id(local_id, "local");
    working_set.add_item_with_id(shared_unique_id, "unique");
    working_set.add_item_with_id(shared_repeated_id, "shared");
    working_set.add_item_with_id(other_repeated_id, "other");
    working_set.add_item_with_id(missing_group_id, "missing registry metadata");
    let groups = test_group_state([
        named_test_group(shared_group, &member, "shared"),
        named_test_group(other_group, &member, "other"),
    ]);
    let session = ChecklistSession::new(working_set);

    assert_eq!(
        session
            .resolve_item(groups.as_ref(), &ItemSelector::RowKey(unique_row))
            .expect("unique bare UUID should resolve"),
        shared_unique_id
    );
    assert_eq!(
        session
            .resolve_item(
                groups.as_ref(),
                &ItemSelector::ListIndex(NonZeroUsize::new(2).expect("two is non-zero"),)
            )
            .expect("list position should resolve"),
        shared_unique_id
    );
    assert_eq!(
        session
            .resolve_item(
                groups.as_ref(),
                &ItemSelector::Qualified {
                    association: ItemAssociationSelector::Local,
                    row_key: repeated_row,
                }
            )
            .expect("local reference should resolve"),
        local_id
    );
    assert_eq!(
        session
            .resolve_item(
                groups.as_ref(),
                &ItemSelector::Qualified {
                    association: ItemAssociationSelector::Group("shared".to_owned()),
                    row_key: repeated_row,
                }
            )
            .expect("unique group name should resolve"),
        shared_repeated_id
    );
    assert_eq!(
        session
            .resolve_item(
                groups.as_ref(),
                &ItemSelector::Qualified {
                    association: ItemAssociationSelector::Group(other_group.to_string()),
                    row_key: repeated_row,
                }
            )
            .expect("group UUID should resolve"),
        other_repeated_id
    );
    assert_eq!(
        ChecklistSession::item_reference(groups.as_ref(), missing_group_id),
        format!("{missing_group}/{missing_row}")
    );
    assert_eq!(
        session
            .resolve_item(
                groups.as_ref(),
                &ItemSelector::Qualified {
                    association: ItemAssociationSelector::Group(missing_group.to_string()),
                    row_key: missing_row,
                }
            )
            .expect("group UUID should resolve without registry metadata"),
        missing_group_id
    );
    assert!(matches!(
        session.resolve_item(groups.as_ref(), &ItemSelector::RowKey(repeated_row)),
        Err(ReplicatedChecklistError::AmbiguousItemReference {
            row_key,
            candidates,
        }) if row_key == repeated_row
            && candidates == vec![
                format!("local/{repeated_row}"),
                format!("shared/{repeated_row}"),
                format!("other/{repeated_row}"),
            ]
    ));
}

#[test]
fn canonical_item_references_fall_back_to_group_uuids_for_unsafe_names() {
    let member = MemberIdentity::from_array(["alice"]);
    let whitespace_group = GroupId(Uuid::from_u128(71_053));
    let reserved_group = GroupId(Uuid::from_u128(71_054));
    let duplicate_first = GroupId(Uuid::from_u128(71_055));
    let duplicate_second = GroupId(Uuid::from_u128(71_056));
    let uuid_named_group = GroupId(Uuid::from_u128(71_058));
    let uuid_shaped_name = Uuid::from_u128(71_059).to_string();
    let row_key = RowKey(Uuid::from_u128(72_053));
    let groups = test_group_state([
        named_test_group(whitespace_group, &member, "shared errands"),
        named_test_group(reserved_group, &member, "local"),
        named_test_group(duplicate_first, &member, "duplicate"),
        named_test_group(duplicate_second, &member, "duplicate"),
        named_test_group(uuid_named_group, &member, &uuid_shaped_name),
    ]);

    for group_id in [
        whitespace_group,
        reserved_group,
        duplicate_first,
        duplicate_second,
        uuid_named_group,
    ] {
        assert_eq!(
            ChecklistSession::item_reference(
                groups.as_ref(),
                ChecklistItemId::group(group_id, row_key),
            ),
            format!("{group_id}/{row_key}")
        );
    }
}

#[test]
fn listener_group_validation_rejects_rows_outside_the_registry() {
    let member = MemberIdentity::from_array(["alice"]);
    let known_group = GroupId(Uuid::from_u128(71_007));
    let unknown_group = GroupId(Uuid::from_u128(71_008));
    let groups = test_group_state([test_group(known_group, &member)]);
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

    ChecklistSession::validate_listener_changes(groups.as_ref(), &[known])
        .expect("known group should validate");
    assert!(matches!(
        ChecklistSession::validate_listener_changes(groups.as_ref(), &[unknown]),
        Err(ReplicatedChecklistError::UnknownListenerGroup { group_id })
            if group_id == unknown_group
    ));
}

#[test]
fn default_repair_reassigns_a_non_writable_default_to_its_open_successor() {
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
    let groups = test_group_state([previous, intermediate, open]);
    let mut session = ChecklistSession::new(ChecklistWorkingSet::new());
    session.default_group = Some(previous_group);

    let repair = session.repair_default_group(groups.as_ref());

    assert_eq!(
        repair,
        DefaultGroupRepair::Reassigned {
            previous_group_id: previous_group,
            successor_group_id: open_group,
        }
    );
    assert_eq!(session.default_group, Some(open_group));

    let mut closed_default_session = ChecklistSession::new(ChecklistWorkingSet::new());
    closed_default_session.default_group = Some(intermediate_group);
    let mut closed = test_group(intermediate_group, &member);
    closed.lifecycle = ReplicationGroupLifecycle::Closed {
        successor_group_id: open_group,
        final_versions: VersionVector::initial(NonZeroUsize::MIN),
    };
    let groups = test_group_state([
        test_group(previous_group, &member),
        closed,
        test_group(open_group, &member),
    ]);
    assert_eq!(
        closed_default_session.repair_default_group(groups.as_ref()),
        DefaultGroupRepair::Reassigned {
            previous_group_id: intermediate_group,
            successor_group_id: open_group,
        }
    );
    assert_eq!(closed_default_session.default_group, Some(open_group));
}

#[test]
fn default_repair_clears_a_default_without_a_resolvable_open_successor() {
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
    let groups = test_group_state([first, second]);
    let mut session = ChecklistSession::new(ChecklistWorkingSet::new());
    session.default_group = Some(first_group);

    let repair = session.repair_default_group(groups.as_ref());

    assert_eq!(
        repair,
        DefaultGroupRepair::Cleared {
            previous_group_id: first_group,
        }
    );
    assert_eq!(session.default_group, None);
}

#[test]
fn default_repair_handles_open_missing_and_restart_defaults() {
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
    let mut session = ChecklistSession::new(ChecklistWorkingSet::new());
    session.default_group = Some(open_group);

    let groups = test_group_state([open.clone(), unavailable.clone()]);
    assert_eq!(
        session.repair_default_group(groups.as_ref()),
        DefaultGroupRepair::Unchanged
    );
    assert_eq!(session.default_group, Some(open_group));

    session.default_group = Some(unavailable_group);
    assert_eq!(
        session.repair_default_group(groups.as_ref()),
        DefaultGroupRepair::Cleared {
            previous_group_id: unavailable_group,
        }
    );
    assert_eq!(session.default_group, None);

    let mut restarted = ChecklistSession::new(ChecklistWorkingSet::new());
    assert_eq!(restarted.default_group, None);
    let groups = test_group_state([open]);
    assert_eq!(
        restarted.repair_default_group(groups.as_ref()),
        DefaultGroupRepair::Unchanged
    );
    assert_eq!(restarted.default_group, None);
}

#[test]
fn runtime_group_state_lists_several_groups_without_store_reads() {
    let member = MemberIdentity::from_array(["alice"]);
    let first_group_id = GroupId(Uuid::from_u128(70_001));
    let second_group_id = GroupId(Uuid::from_u128(70_002));
    let (_store, runtime, _listener, _receivers) =
        load_test_runtime_with_groups(&member, [first_group_id, second_group_id]);
    let groups = runtime
        .group_state()
        .expect("group state should be available");
    let group_ids = groups
        .groups()
        .map(ReplicationGroupView::group_id)
        .collect::<HashSet<_>>();
    assert_eq!(group_ids, HashSet::from([first_group_id, second_group_id]));
    block_on(runtime.shutdown()).expect("test runtime should shut down");
}
