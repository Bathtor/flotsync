//! Replication API tests.

use super::{
    changes::{APPLICATION_READ_TOKEN_PROTOBUF_FORMAT_V1, GROUP_READ_TOKEN_PROTOBUF_FORMAT_V1},
    *,
};
use crate::test_support::docs_group_schema;
use base64::engine::general_purpose::STANDARD;
use flotsync_core::versions::{OverrideVersion, PureVersionVector};
use flotsync_data_types::Field;
use flotsync_messages::{
    buffa::{Message as _, MessageField},
    versions as versions_proto,
};
use std::sync::{Arc, LazyLock};
use uuid::Uuid;

static REPRESENTATION_TEST_SCHEMA: LazyLock<Schema> =
    LazyLock::new(|| Schema::from_fields([Field::linear_string("title")]));

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

fn read_token_group_id(value: u128) -> GroupId {
    GroupId(Uuid::from_u128(value))
}

fn decode_application_read_token_proto(bytes: &[u8]) -> versions_proto::ReadToken {
    let (&format, payload) = bytes
        .split_first()
        .expect("runtime-produced read token should contain a format discriminator");
    assert_eq!(format, APPLICATION_READ_TOKEN_PROTOBUF_FORMAT_V1);
    versions_proto::ReadToken::decode_from_slice(payload)
        .expect("runtime-produced read token should decode as its protobuf envelope")
}

fn encode_application_read_token_proto(proto: &versions_proto::ReadToken) -> Vec<u8> {
    let payload = proto.encode_to_vec();
    let mut bytes = Vec::with_capacity(payload.len() + 1);
    bytes.push(APPLICATION_READ_TOKEN_PROTOBUF_FORMAT_V1);
    bytes.extend(payload);
    bytes
}

fn decode_group_read_token_proto(bytes: &[u8]) -> versions_proto::ReadTokenGroup {
    let (&format, payload) = bytes
        .split_first()
        .expect("runtime-produced group token should contain a format discriminator");
    assert_eq!(format, GROUP_READ_TOKEN_PROTOBUF_FORMAT_V1);
    versions_proto::ReadTokenGroup::decode_from_slice(payload)
        .expect("runtime-produced group token should decode as one group entry")
}

#[test]
fn schema_source_equality_distinguishes_ownership_while_group_definitions_compare_structurally() {
    let dataset_id = DatasetId::try_from_static("representation_test")
        .expect("representation test dataset id should build");
    let static_source = SchemaSource::Static(&REPRESENTATION_TEST_SCHEMA);
    let shared_source = SchemaSource::Shared(Arc::new(REPRESENTATION_TEST_SCHEMA.clone()));
    assert_ne!(static_source, shared_source);
    assert_eq!(static_source.as_schema(), shared_source.as_schema());

    let static_group = GroupSchema::new(HashMap::from([(dataset_id.clone(), static_source)]));
    let shared_group = GroupSchema::new(HashMap::from([(dataset_id, shared_source)]));
    assert_ne!(static_group, shared_group);
    assert!(static_group.has_same_schema_definitions(&shared_group));
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
        group_name: Some("docs".to_owned()),
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
    let mut different_metadata = material.clone();
    different_metadata.group_name = Some("renamed".to_owned());

    assert!(material.matches_definition(
        group_id,
        &member_keys,
        MemberIndex::new(0),
        &group_schema,
    ));
    assert!(active.matches_definition(&different_security_active));
    assert!(!active.matches_group_material(&different_security));
    assert!(active.matches_group_material(&different_metadata));
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
        group_name: Some("active docs".to_owned()),
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

    let (material, active_state) = group.into_parts();

    assert_eq!(material.group_name.as_deref(), Some("active docs"));
    assert_eq!(active_state.version_vector, versions);
    assert_eq!(active_state.lifecycle, lifecycle);
}

#[test]
fn group_name_update_defaults_to_inherit() {
    assert_eq!(GroupNameUpdate::default(), GroupNameUpdate::Inherit);
}

#[test]
fn group_aggregate_defaults_are_explicitly_incomplete() {
    assert_eq!(
        CreateGroupRequest::default(),
        CreateGroupRequest {
            group_name: None,
            message: None,
            members: Vec::new(),
            group_schema: GroupSchema::default(),
        }
    );
    assert_eq!(
        ChangeGroupMembershipRequest::default(),
        ChangeGroupMembershipRequest {
            group_id: GroupId::NIL,
            add_members: HashSet::new(),
            remove_members: HashSet::new(),
            group_name: GroupNameUpdate::Inherit,
            message: None,
        }
    );

    let material = ReplicationGroupMaterialRecord::default();
    assert_eq!(material.group_id, GroupId::NIL);
    assert_eq!(material.group_name, None);
    assert!(material.member_keys.is_empty());
    assert_eq!(material.local_member_index, MemberIndex::new(u32::MAX));
    assert_eq!(material.group_schema, GroupSchema::default());
    assert_eq!(
        material.security_material,
        invalid_default_group_security_material()
    );

    let group = ReplicationGroupRecord::default();
    assert_eq!(group.group_id, GroupId::NIL);
    assert_eq!(group.group_name, None);
    assert!(group.member_keys.is_empty());
    assert_eq!(group.local_member_index, MemberIndex::new(u32::MAX));
    assert_eq!(group.group_schema, GroupSchema::default());
    assert_eq!(group.version_vector.num_members(), NonZeroUsize::MIN);
    assert_eq!(group.lifecycle, ReplicationGroupLifecycle::Open);
    assert_eq!(
        group.security_material,
        invalid_default_group_security_material()
    );
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
fn pending_group_debug_includes_metadata_values() {
    let old_group_id = GroupId(uuid::Uuid::from_u128(91_010));
    let new_group_id = GroupId(uuid::Uuid::from_u128(91_011));
    let invitation = GroupInvitation::new_creation(
        new_group_id,
        Vec::new(),
        GroupSchema::default(),
        InitialSnapshot::Empty,
        Some("invited docs".to_owned()),
        Some("invitation message".to_owned()),
    );
    let proposal = MigrationProposal {
        migration_id: MigrationId {
            old_group_id,
            new_group_id,
        },
        final_versions: VersionVector::initial(NonZeroUsize::new(1).unwrap()),
        proposed_members: Vec::new(),
        group_schema: GroupSchema::default(),
        initial_snapshot: InitialSnapshot::Empty,
        group_name: Some("migrated docs".to_owned()),
        message: Some("migration message".to_owned()),
    };

    assert_eq!(
        format!("{invitation:?}"),
        format!(
            "GroupInvitation {{ group_id: {:?}, source: {:?}, proposed_member_count: 0, group_schema: {:?}, initial_snapshot: {:?}, group_name: {:?}, message: {:?} }}",
            invitation.group_id,
            invitation.source,
            invitation.group_schema,
            invitation.initial_snapshot,
            invitation.group_name,
            invitation.message,
        )
    );
    assert_eq!(
        format!("{proposal:?}"),
        format!(
            "MigrationProposal {{ migration_id: {:?}, final_versions: {:?}, proposed_member_count: 0, group_schema: {:?}, initial_snapshot: {:?}, group_name: {:?}, message: {:?} }}",
            proposal.migration_id,
            proposal.final_versions,
            proposal.group_schema,
            proposal.initial_snapshot,
            proposal.group_name,
            proposal.message,
        )
    );
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
        r#"MemberPublicKeysRecord { key_id: MemberKeyId { member_id: MemberIdentity(Identifier(i"debug", i"alice")), fingerprint: KeyFingerprint("CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQk") }, signing_public_key_len: 3, encryption_public_key_len: 3 }"#,
    );
}

#[test]
fn member_public_keys_alternate_debug_prints_base64url() {
    let output = format!("{:#?}", member_public_keys_record());

    assert_eq!(
        output,
        r#"MemberPublicKeysRecord {
    key_id: MemberKeyId {
        member_id: MemberIdentity(
            Identifier(i"debug", i"alice"),
        ),
        fingerprint: KeyFingerprint(
            "CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQk",
        ),
    },
    signing_public_key: "AQID",
    encryption_public_key: "BAUG",
}"#,
    );
}

#[test]
fn application_read_token_bytes_and_text_are_canonical_across_vector_representations() {
    let member_count = NonZeroUsize::new(3).expect("three is non-zero");
    let synced_group = read_token_group_id(1);
    let override_group = read_token_group_id(2);
    let full_group = read_token_group_id(3);
    let entries = [
        (
            full_group,
            VersionVector::Full(PureVersionVector::from([4, 1, 3])),
        ),
        (
            synced_group,
            VersionVector::Synced {
                num_members: member_count,
                version: 7,
            },
        ),
        (
            override_group,
            VersionVector::Override {
                num_members: member_count,
                version: OverrideVersion::new(7, 1, 8),
            },
        ),
    ];
    let first = ApplicationReadToken::from_group_versions(HashMap::from(entries.clone()));
    let second = ApplicationReadToken::from_group_versions(HashMap::from([
        entries[1].clone(),
        entries[2].clone(),
        entries[0].clone(),
    ]));

    assert_eq!(first.to_bytes(), second.to_bytes());
    assert_eq!(
        ApplicationReadToken::from_bytes(&first.to_bytes()).expect("canonical bytes should decode"),
        first
    );

    let text = first.to_string();
    assert_eq!(text, STANDARD.encode(first.to_bytes()));
    assert_eq!(
        text.parse::<ApplicationReadToken>()
            .expect("canonical token text should parse"),
        first
    );

    let empty = ApplicationReadToken::default();
    assert_eq!(
        ApplicationReadToken::from_bytes(&empty.to_bytes())
            .expect("empty token bytes should decode"),
        empty
    );
    assert!(empty.is_empty());
}

#[test]
fn application_read_token_rebuilds_from_separate_group_tokens() {
    let first_group = read_token_group_id(4);
    let second_group = read_token_group_id(5);
    let first = GroupReadToken::from_group_version(
        first_group,
        VersionVector::Full(PureVersionVector::from([1, 4])),
    );
    let later_first = GroupReadToken::from_group_version(
        first_group,
        VersionVector::Full(PureVersionVector::from([3, 2])),
    );
    let second = GroupReadToken::from_group_version(
        second_group,
        VersionVector::Synced {
            num_members: NonZeroUsize::new(2).expect("two is non-zero"),
            version: 5,
        },
    );

    let empty = ApplicationReadToken::from_group_tokens([]);
    let singleton = ApplicationReadToken::from_group_tokens([first.clone()]);
    let rebuilt =
        ApplicationReadToken::from_group_tokens([first.clone(), second.clone(), later_first]);

    assert!(empty.is_empty());
    assert_eq!(singleton, ApplicationReadToken::from(first));
    assert_eq!(rebuilt.group_count(), 2);
    assert_eq!(
        rebuilt.group_version(&first_group),
        Some(&VersionVector::Override {
            num_members: NonZeroUsize::new(2).expect("two is non-zero"),
            version: OverrideVersion::new(3, 1, 4),
        })
    );
    assert_eq!(rebuilt.group_version(&second_group), Some(second.version()));
}

#[test]
fn application_read_token_merges_group_progress_and_applies_replacement() {
    let existing_group = read_token_group_id(10);
    let added_group = read_token_group_id(11);
    let replacement_group = read_token_group_id(12);
    let mut token = ApplicationReadToken::from_group_versions(HashMap::from([(
        existing_group,
        VersionVector::Full(PureVersionVector::from([1, 4])),
    )]));
    let applied = GroupReadToken::from_group_version(
        existing_group,
        VersionVector::Full(PureVersionVector::from([3, 2])),
    );
    let added = GroupReadToken::from_group_version(
        added_group,
        VersionVector::Synced {
            num_members: NonZeroUsize::new(2).expect("two is non-zero"),
            version: 5,
        },
    );

    token.merge_applied(&applied);
    token.merge_applied(&added);

    assert_eq!(token.group_count(), 2);
    assert_eq!(
        token.group_version(&existing_group),
        Some(&VersionVector::Override {
            num_members: NonZeroUsize::new(2).expect("two is non-zero"),
            version: OverrideVersion::new(3, 1, 4),
        })
    );
    assert_eq!(token.group_version(&added_group), Some(added.version()));

    let replacement = GroupReadToken::from_group_version(
        replacement_group,
        VersionVector::initial(NonZeroUsize::new(3).expect("three is non-zero")),
    );
    let replacement_position = DataChangeReadPosition::new(
        DataChangeLineage::GroupReplacement {
            migration_id: MigrationId {
                old_group_id: existing_group,
                new_group_id: replacement_group,
            },
        },
        replacement.clone(),
    );
    assert_eq!(
        replacement_position.lineage(),
        DataChangeLineage::GroupReplacement {
            migration_id: MigrationId {
                old_group_id: existing_group,
                new_group_id: replacement_group,
            },
        }
    );
    assert_eq!(replacement_position.group_read_token(), &replacement);

    token.apply_data_change(&replacement_position);

    assert_eq!(token.group_count(), 2);
    assert!(token.group_read_token(&existing_group).is_none());
    assert_eq!(token.group_version(&added_group), Some(added.version()));
    assert_eq!(
        token.group_read_token(&replacement_group),
        Some(replacement)
    );
}

#[test]
#[should_panic(expected = "replacement read position")]
fn data_change_read_position_rejects_a_mismatched_replacement_token() {
    let old_group_id = read_token_group_id(13);
    let expected_group_id = read_token_group_id(14);
    let actual_group_id = read_token_group_id(15);
    let read_token = GroupReadToken::from_group_version(
        actual_group_id,
        VersionVector::initial(NonZeroUsize::new(1).expect("one is non-zero")),
    );

    let _position = DataChangeReadPosition::new(
        DataChangeLineage::GroupReplacement {
            migration_id: MigrationId {
                old_group_id,
                new_group_id: expected_group_id,
            },
        },
        read_token,
    );
}

#[test]
fn read_token_debug_is_opaque_normally_and_diagnostic_when_alternate() {
    let group_id = read_token_group_id(20);
    let token = ApplicationReadToken::from_group_versions(HashMap::from([(
        group_id,
        VersionVector::initial(NonZeroUsize::new(1).expect("one is non-zero")),
    )]));

    let ordinary = format!("{token:?}");
    assert!(ordinary.contains("group_count"));
    assert!(!ordinary.contains(&group_id.to_string()));
    assert!(!ordinary.contains("Synced"));

    let alternate = format!("{token:#?}");
    assert!(alternate.contains("groups"));
    assert!(alternate.contains(&group_id.to_string()));
    assert!(alternate.contains("Synced"));

    let group_token = token
        .group_read_token(&group_id)
        .expect("application token should expose its opaque group position");
    let ordinary = format!("{group_token:?}");
    assert!(ordinary.contains(&group_id.to_string()));
    assert!(!ordinary.contains("Synced"));
    assert!(ordinary.contains(".."));
    let alternate = format!("{group_token:#?}");
    assert!(alternate.contains("Synced"));
    assert!(!alternate.contains(".."));
}

#[test]
fn application_read_token_decode_rejects_invalid_formats_and_structures() {
    let first_group = read_token_group_id(30);
    let second_group = read_token_group_id(31);
    let token = ApplicationReadToken::from_group_versions(HashMap::from([
        (
            first_group,
            VersionVector::initial(NonZeroUsize::new(1).expect("one is non-zero")),
        ),
        (
            second_group,
            VersionVector::initial(NonZeroUsize::new(1).expect("one is non-zero")),
        ),
    ]));
    assert!(ApplicationReadToken::from_bytes(&[]).is_err());
    assert!(ApplicationReadToken::from_bytes(&[2]).is_err());
    assert!(ApplicationReadToken::from_bytes(&[1, 0xff]).is_err());

    let canonical_proto = decode_application_read_token_proto(&token.to_bytes());

    let mut invalid_group = canonical_proto.clone();
    invalid_group.groups[0].group_id = vec![0];
    assert!(
        ApplicationReadToken::from_bytes(&encode_application_read_token_proto(&invalid_group))
            .is_err()
    );

    let mut missing_vector = canonical_proto.clone();
    missing_vector.groups[0].versions = MessageField::none();
    assert!(
        ApplicationReadToken::from_bytes(&encode_application_read_token_proto(&missing_vector))
            .is_err()
    );

    let mut invalid_vector = canonical_proto.clone();
    let mut vector = invalid_vector.groups[0]
        .versions
        .take()
        .expect("canonical token entry should contain a vector");
    vector.num_members = 0;
    invalid_vector.groups[0].versions = MessageField::some(vector);
    assert!(
        ApplicationReadToken::from_bytes(&encode_application_read_token_proto(&invalid_vector))
            .is_err()
    );

    let mut duplicate_group = canonical_proto.clone();
    duplicate_group
        .groups
        .push(duplicate_group.groups[0].clone());
    assert!(
        ApplicationReadToken::from_bytes(&encode_application_read_token_proto(&duplicate_group))
            .is_err()
    );

    assert!(matches!(
        "not base64!".parse::<ApplicationReadToken>(),
        Err(ParseReadTokenError::InvalidBase64 { .. })
    ));
    assert!(matches!(
        "AQ".parse::<ApplicationReadToken>(),
        Err(ParseReadTokenError::InvalidBase64 { .. })
    ));
}

#[test]
fn application_read_token_decode_accepts_compatible_protobuf_entry_order() {
    let first_group = read_token_group_id(40);
    let second_group = read_token_group_id(41);
    let token = ApplicationReadToken::from_group_versions(HashMap::from([
        (
            first_group,
            VersionVector::initial(NonZeroUsize::new(1).expect("one is non-zero")),
        ),
        (
            second_group,
            VersionVector::initial(NonZeroUsize::new(1).expect("one is non-zero")),
        ),
    ]));
    let mut reordered = decode_application_read_token_proto(&token.to_bytes());
    reordered.groups.reverse();
    let reordered_bytes = encode_application_read_token_proto(&reordered);
    assert_eq!(
        ApplicationReadToken::from_bytes(&reordered_bytes)
            .expect("compatible protobuf bytes may use a different entry order"),
        token
    );
    let reordered_text = STANDARD.encode(reordered_bytes);
    assert_eq!(
        reordered_text
            .parse::<ApplicationReadToken>()
            .expect("canonical Base64 may contain a compatible protobuf ordering"),
        token
    );
}

#[test]
fn group_and_application_read_token_encodings_are_distinct() {
    let first_group = read_token_group_id(50);
    let second_group = read_token_group_id(51);
    let token = GroupReadToken::from_group_version(
        first_group,
        VersionVector::initial(NonZeroUsize::new(2).expect("two is non-zero")),
    );

    assert_eq!(
        GroupReadToken::from_bytes(&token.to_bytes()).expect("group token bytes should decode"),
        token
    );
    assert_eq!(
        token
            .to_string()
            .parse::<GroupReadToken>()
            .expect("group token text should parse"),
        token
    );
    assert_eq!(token.group_id(), first_group);
    let group_proto = decode_group_read_token_proto(&token.to_bytes());
    assert_eq!(group_proto.group_id.len(), 16);
    assert!(group_proto.versions.is_set());

    let empty = ApplicationReadToken::default();
    assert!(GroupReadToken::from_bytes(&empty.to_bytes()).is_err());
    let singleton = ApplicationReadToken::from(token.clone());
    assert!(GroupReadToken::from_bytes(&singleton.to_bytes()).is_err());
    assert!(ApplicationReadToken::from_bytes(&token.to_bytes()).is_err());
    assert_ne!(token.to_bytes()[0], singleton.to_bytes()[0]);
    let aggregate = ApplicationReadToken::from_group_versions(HashMap::from([
        (
            first_group,
            VersionVector::initial(NonZeroUsize::new(1).expect("one is non-zero")),
        ),
        (
            second_group,
            VersionVector::initial(NonZeroUsize::new(1).expect("one is non-zero")),
        ),
    ]));
    assert!(GroupReadToken::from_bytes(&aggregate.to_bytes()).is_err());
}
