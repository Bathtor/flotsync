//! Replication API tests.

use super::*;
use crate::test_support::docs_group_schema;

fn member_key_id<const N: usize>(segments: [&str; N], fingerprint_seed: u8) -> MemberKeyId {
    MemberKeyId {
        member_id: MemberIdentity::from_array(segments),
        fingerprint: KeyFingerprint::from_bytes([fingerprint_seed; 32]),
    }
}

fn member_public_keys_record() -> MemberPublicKeysRecord {
    MemberPublicKeysRecord {
        key_id: MemberKeyId {
            member_id: MemberIdentity::from_array(["debug", "alice"]),
            fingerprint: KeyFingerprint::from_bytes([9_u8; 32]),
        },
        signing_public_key: Box::from([1_u8, 2, 3]),
        encryption_public_key: Box::from([4_u8, 5, 6]),
    }
}

#[test]
fn policy_decision_order_matches_restrictiveness() {
    let decisions = [
        PolicyDecision::AutoAccept,
        PolicyDecision::AskListener,
        PolicyDecision::AutoReject,
    ];
    for expected_index in 0..decisions.len() {
        let expected = decisions[expected_index];
        for other in decisions.iter().copied().take(expected_index + 1) {
            assert_eq!(expected.most_restrictive(other), expected);
            assert_eq!(other.most_restrictive(expected), expected);
        }
    }
}

#[test]
fn group_policy_defaults_require_mediation_for_top_level_membership_changes() {
    assert_eq!(
        GroupInvitationPolicy::default(),
        GroupInvitationPolicy {
            creation: PolicyDecision::AskListener,
            migration_added_member: PolicyDecision::AskListener,
        }
    );
    assert_eq!(
        GroupMigrationPolicy::default(),
        GroupMigrationPolicy {
            epoch_change: PolicyDecision::AutoAccept,
            member_added: PolicyDecision::AskListener,
            member_device_added: PolicyDecision::AutoAccept,
            member_removed: PolicyDecision::AskListener,
            member_device_removed: PolicyDecision::AutoAccept,
            local_member_removed: PolicyDecision::AskListener,
        }
    );
}

#[test]
fn group_member_keys_preserve_order_indices_and_exact_keys() {
    let alice_key = member_key_id(["debug", "alice"], 1);
    let bob_key = member_key_id(["debug", "bob"], 2);

    let member_keys =
        GroupMemberKeys::from_ordered_member_keys([alice_key.clone(), bob_key.clone()])
            .expect("group member keys should build");

    assert_eq!(
        member_keys.member_ids().cloned().collect::<Vec<_>>(),
        vec![alice_key.member_id.clone(), bob_key.member_id.clone()]
    );
    assert_eq!(
        member_keys.member_index(&alice_key.member_id),
        Some(MemberIndex::new(0))
    );
    assert_eq!(
        member_keys.member_index(&bob_key.member_id),
        Some(MemberIndex::new(1))
    );
    assert_eq!(
        member_keys.member_key(&alice_key.member_id),
        Some(&alice_key)
    );
    assert_eq!(
        member_keys.member_key_at_index(MemberIndex::new(1)),
        Some(&bob_key)
    );
    assert_eq!(
        member_keys
            .to_group_members()
            .expect("identity group view should build")
            .ordered_members(),
        vec![alice_key.member_id, bob_key.member_id]
    );
}

#[test]
fn group_member_keys_reject_duplicate_member_identities() {
    let first_key = member_key_id(["debug", "alice"], 1);
    let second_key = member_key_id(["debug", "alice"], 2);

    let error = GroupMemberKeys::from_ordered_member_keys([first_key, second_key])
        .expect_err("duplicate member identity should be rejected");

    assert!(matches!(error, GroupMembersError::DuplicateMember { .. }));
}

#[test]
fn group_material_definition_matching_excludes_security_material() {
    let group_id = GroupId(uuid::Uuid::from_u128(91_000));
    let member_keys = GroupMemberKeys::from_ordered_member_keys([
        member_key_id(["debug", "alice"], 1),
        member_key_id(["debug", "bob"], 2),
    ])
    .expect("group member keys should build");
    let group_schema = docs_group_schema();
    let material = ReplicationGroupMaterialRecord {
        group_id,
        member_keys: member_keys.clone(),
        local_member_index: MemberIndex::new(0),
        group_schema: group_schema.clone(),
        security_material: current_slice_placeholder_group_security_material(group_id),
    };
    let member_count = NonZeroUsize::new(2).expect("test group has members");
    let active = material
        .clone()
        .activate(VersionVector::initial(member_count));
    let mut different_security = material.clone();
    different_security.security_material =
        current_slice_placeholder_group_security_material(GroupId(uuid::Uuid::from_u128(91_098)));
    let different_security_active = different_security
        .clone()
        .activate(VersionVector::initial(member_count));

    assert!(material.matches_definition(
        group_id,
        &member_keys,
        MemberIndex::new(0),
        &group_schema,
    ));
    assert!(active.matches_definition(&different_security_active));
    assert!(!active.matches_group_material(&different_security));
    assert!(!material.matches_definition(
        GroupId(uuid::Uuid::from_u128(91_099)),
        &member_keys,
        MemberIndex::new(0),
        &group_schema,
    ));
}

#[test]
fn active_group_decomposition_preserves_progress_and_lifecycle() {
    let group_id = GroupId(uuid::Uuid::from_u128(91_100));
    let successor_group_id = GroupId(uuid::Uuid::from_u128(91_101));
    let member_count = NonZeroUsize::new(1).expect("test group has one member");
    let mut versions = VersionVector::initial(member_count);
    versions.increment_at(0);
    let lifecycle = ReplicationGroupLifecycle::ReadOnly {
        successor_group_id,
        final_versions: versions.clone(),
    };
    let group = ReplicationGroupRecord {
        group_id,
        member_keys: GroupMemberKeys::from_ordered_member_keys([member_key_id(
            ["active-state", "alice"],
            1,
        )])
        .expect("test group member keys should build"),
        local_member_index: MemberIndex::new(0),
        group_schema: GroupSchema::default(),
        version_vector: versions.clone(),
        lifecycle: lifecycle.clone(),
        security_material: current_slice_placeholder_group_security_material(group_id),
    };

    let (_, active_state) = group.into_parts();

    assert_eq!(active_state.version_vector, versions);
    assert_eq!(active_state.lifecycle, lifecycle);
}

#[test]
fn group_invitation_rejects_mismatched_migration_group_id() {
    let old_group_id = GroupId(uuid::Uuid::from_u128(91_001));
    let new_group_id = GroupId(uuid::Uuid::from_u128(91_002));
    let wrong_group_id = GroupId(uuid::Uuid::from_u128(91_003));

    let error = GroupInvitation::try_new(
        wrong_group_id,
        GroupInvitationSource::Migration {
            migration_id: MigrationId {
                old_group_id,
                new_group_id,
            },
        },
        Vec::new(),
        GroupSchema::default(),
        InitialSnapshot::Empty,
        None,
        None,
    )
    .expect_err("mismatched migration invitation group id should be rejected");

    assert!(matches!(
        error,
        GroupInvitationError::GroupMismatch {
            group_id,
            new_group_id: actual_new_group_id,
        } if group_id == wrong_group_id && actual_new_group_id == new_group_id
    ));
}

#[test]
fn group_schema_alternate_debug_lists_datasets() {
    let group_schema = docs_group_schema();

    let default_output = format!("{group_schema:?}");
    let alternate_output = format!("{group_schema:#?}");

    assert!(default_output.contains("dataset_count"));
    assert!(!default_output.contains("DatasetSchema"));
    assert!(alternate_output.contains("DatasetSchema"));
    assert!(alternate_output.contains("docs"));
}

#[test]
fn member_public_keys_debug_prints_lengths_by_default() {
    let output = format!("{:?}", member_public_keys_record());

    assert_eq!(
        output,
        r#"MemberPublicKeysRecord { key_id: MemberKeyId { member_id: Identifier(i"debug", i"alice"), fingerprint: KeyFingerprint("CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQk") }, signing_public_key_len: 3, encryption_public_key_len: 3 }"#,
    );
}

#[test]
fn member_public_keys_alternate_debug_prints_base64url() {
    let output = format!("{:#?}", member_public_keys_record());

    assert_eq!(
        output,
        r#"MemberPublicKeysRecord {
    key_id: MemberKeyId {
        member_id: Identifier(i"debug", i"alice"),
        fingerprint: KeyFingerprint(
            "CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQk",
        ),
    },
    signing_public_key: "AQID",
    encryption_public_key: "BAUG",
}"#,
    );
}
