//! Message codec tests.

use super::{
    BootstrapMemberKeyMessage,
    CompactVersionVectorProtoCodec,
    DatasetUpdateMessage,
    DatasetUpdateMessageView,
    GroupInvitationMessage,
    GroupSetupKey,
    GroupSetupMessage,
    MemberCountContext,
    MigrationProposalMessage,
    NeedRangeMessage,
    RuntimeMessage,
    RuntimeMessageDecodeContext,
    RuntimeMessageError,
    SummaryMessage,
    SummaryRequestMessage,
    UpdateBatchMessage,
    UpdateMessage,
    UpdateMessageProtoSource,
    UpdateRangeMessage,
    VersionVectorCodecError,
    VersionVectorProtoCodec,
};
use crate::{
    api::{
        DatasetId,
        DatasetUpdateRecord,
        GroupInvitation,
        InitialDatasetValueRows,
        InitialGroupValueRows,
        InitialSnapshot,
        InitialSnapshotMetadata,
        InitialValueRow,
        MigrationId,
        MigrationProposal,
        ReplicationUpdateRecord,
        RowKey,
        RowValues,
        SnapshotRef,
    },
    test_support::{
        docs_dataset_id,
        docs_group_schema,
        docs_schema_source,
        test_public_member_keys,
    },
};
use flotsync_core::{
    GroupId,
    MemberIdentity,
    member::TrieMap,
    membership::{GroupMembers, GroupMemberships},
    versions::{OverrideVersion, PureVersionVector, UpdateId, VersionVector},
};
use flotsync_messages::{
    buffa::{Message as _, MessageView as _},
    datamodel as datamodel_proto,
    proto::{DecodeProto, DecodeProtoView, DecodeProtoViewWith, DecodeProtoWith, EncodeProto},
    replication as replication_proto,
    versions as versions_proto,
};
use flotsync_security::{GROUP_CIPHER_SUITE_CHACHA20_POLY1305, GROUP_KEY_LENGTH};
use std::{num::NonZeroUsize, sync::Arc};
use uuid::Uuid;

fn test_update_message(
    group_id: GroupId,
    update_id: UpdateId,
    read_versions: VersionVector,
) -> UpdateMessage {
    UpdateMessage {
        group_id,
        update_id,
        read_versions,
        dataset_updates: vec![DatasetUpdateMessage {
            dataset_id: DatasetId::try_new("docs").expect("dataset id should build"),
            operations: vec![datamodel_proto::SchemaOperation::default()],
        }],
    }
}

fn test_group_setup(members: &[MemberIdentity]) -> Arc<GroupSetupMessage> {
    let mut member_keys = TrieMap::new();
    for member in members {
        member_keys.insert(
            member.clone(),
            BootstrapMemberKeyMessage::from_public_keys(&test_public_member_keys(member)),
        );
    }
    Arc::new(
        GroupSetupMessage::new(
            members.to_vec(),
            member_keys,
            GROUP_CIPHER_SUITE_CHACHA20_POLY1305,
            GroupSetupKey::from_bytes([5; GROUP_KEY_LENGTH]),
        )
        .expect("test group setup should build"),
    )
}

fn inline_snapshot() -> InitialSnapshot {
    let schema = docs_schema_source();
    let row = RowValues::try_from_fields(
        schema.as_schema(),
        crate::row_values! {
            "title" => "borrowed payload",
        }
        .fields,
    )
    .expect("inline snapshot row should match docs schema");
    InitialSnapshot::Inline(InitialGroupValueRows {
        datasets: vec![InitialDatasetValueRows {
            dataset_id: docs_dataset_id(),
            rows: vec![InitialValueRow {
                row_key: RowKey(Uuid::from_u128(91_003)),
                row,
            }],
        }],
    })
}

fn metadata_snapshot(migration_id: MigrationId) -> InitialSnapshot {
    InitialSnapshot::Metadata(InitialSnapshotMetadata {
        primary_ref: SnapshotRef {
            group_id: migration_id.old_group_id,
            versions: VersionVector::Full(PureVersionVector::from([3, 4])),
        },
        equivalent_refs: smallvec::smallvec![SnapshotRef {
            group_id: migration_id.new_group_id,
            versions: VersionVector::Synced {
                num_members: NonZeroUsize::new(2).expect("two members"),
                version: 4,
            },
        }],
        record_count: Some(1),
    })
}

fn test_memberships(groups: &[(GroupId, usize)]) -> GroupMemberships {
    const MEMBER_NAMES: [&str; 4] = ["alice", "bob", "carol", "dave"];
    GroupMemberships::from_groups(groups.iter().map(|(group_id, member_count)| {
        let members: Vec<_> = MEMBER_NAMES[..*member_count]
            .iter()
            .map(|name| MemberIdentity::from_array(["runtime-message", *name]))
            .collect();
        let members = GroupMembers::from_ordered_members(members)
            .expect("test group members should be valid");
        (*group_id, members)
    }))
}

fn decode_runtime_message(
    payload: &[u8],
    memberships: &GroupMemberships,
) -> Result<RuntimeMessage, RuntimeMessageError> {
    RuntimeMessage::decode_proto_view_from_slice_with(
        payload,
        RuntimeMessageDecodeContext::new(memberships),
    )
}

fn assert_runtime_decode_paths(
    payload: &[u8],
    memberships: &GroupMemberships,
    expected: &RuntimeMessage,
) {
    let borrowed = decode_runtime_message(payload, memberships)
        .expect("runtime message borrowed view should decode");
    assert_eq!(&borrowed, expected);
    let owned = RuntimeMessage::decode_proto_from_slice_with(
        payload,
        RuntimeMessageDecodeContext::new(memberships),
    )
    .expect("runtime message owned protobuf should decode");
    assert_eq!(&owned, expected);
}

#[test]
fn compact_and_self_describing_version_vectors_round_trip_all_representations() {
    let full = VersionVector::Full(PureVersionVector::from([2, 3, 4]));
    let override_vector = VersionVector::Override {
        num_members: NonZeroUsize::new(3).expect("three members"),
        version: OverrideVersion::new(7, 1, 8),
    };
    let synced = VersionVector::Synced {
        num_members: NonZeroUsize::new(3).expect("three members"),
        version: 11,
    };

    for vector in [full, override_vector, synced] {
        let member_count = MemberCountContext::new(vector.num_members());
        let compact = CompactVersionVectorProtoCodec::from(&vector).encode_proto();
        let compact_payload = compact.encode_to_bytes();
        let compact_view = versions_proto::CompactVersionVectorView::decode_view(&compact_payload)
            .expect("compact view should decode");
        let decoded_view =
            CompactVersionVectorProtoCodec::decode_proto_view_with(&compact_view, member_count)
                .expect("compact view should decode");
        assert_eq!(decoded_view.into_version_vector(), vector);
        let decoded = CompactVersionVectorProtoCodec::decode_proto_with(compact, member_count)
            .expect("compact vector should decode");
        assert_eq!(decoded.into_version_vector(), vector);

        let self_describing = VersionVectorProtoCodec::from(&vector).encode_proto();
        let self_describing_payload = self_describing.encode_to_bytes();
        let self_describing_view =
            versions_proto::VersionVectorView::decode_view(&self_describing_payload)
                .expect("self-describing view should decode");
        let decoded_view = VersionVectorProtoCodec::decode_proto_view(&self_describing_view)
            .expect("self-describing view should convert");
        assert_eq!(decoded_view.into_version_vector(), vector);
        let decoded = VersionVectorProtoCodec::decode_proto(self_describing)
            .expect("self-describing vector should decode");
        assert_eq!(decoded.into_version_vector(), vector);
    }
}

#[test]
fn self_describing_version_vector_rejects_invalid_member_counts() {
    let vector = VersionVector::Full(PureVersionVector::from([2, 3]));
    let mut missing_count = VersionVectorProtoCodec::from(&vector).encode_proto();
    missing_count.num_members = 0;
    let missing_count_payload = missing_count.encode_to_bytes();
    let missing_count_view = versions_proto::VersionVectorView::decode_view(&missing_count_payload)
        .expect("invalid self-describing vector should remain valid protobuf");
    assert!(matches!(
        VersionVectorProtoCodec::decode_proto_view(&missing_count_view),
        Err(VersionVectorCodecError::InvalidMemberCount)
    ));
    assert!(matches!(
        VersionVectorProtoCodec::decode_proto(missing_count),
        Err(VersionVectorCodecError::InvalidMemberCount)
    ));

    let mut mismatched_count = VersionVectorProtoCodec::from(&vector).encode_proto();
    mismatched_count.num_members = 3;
    let mismatched_count_payload = mismatched_count.encode_to_bytes();
    let mismatched_count_view =
        versions_proto::VersionVectorView::decode_view(&mismatched_count_payload)
            .expect("mismatched self-describing vector should remain valid protobuf");
    assert!(matches!(
        VersionVectorProtoCodec::decode_proto_view(&mismatched_count_view),
        Err(VersionVectorCodecError::MemberCountMismatch {
            expected_members: 3,
            actual_members: 2,
        })
    ));
    assert!(matches!(
        VersionVectorProtoCodec::decode_proto(mismatched_count),
        Err(VersionVectorCodecError::MemberCountMismatch {
            expected_members: 3,
            actual_members: 2,
        })
    ));
}

#[test]
fn summary_messages_round_trip_through_runtime_envelope() {
    let group_id = GroupId(Uuid::from_u128(101));
    let correlation_id = Uuid::from_u128(202);
    let summary_request = RuntimeMessage::SummaryRequest(SummaryRequestMessage {
        group_id,
        correlation_id,
    });
    let request_payload = summary_request.encode_proto().encode_to_bytes();
    let memberships = test_memberships(&[(group_id, 2)]);

    assert_eq!(
        decode_runtime_message(&request_payload, &memberships)
            .expect("summary request should decode"),
        RuntimeMessage::SummaryRequest(SummaryRequestMessage {
            group_id,
            correlation_id,
        })
    );

    let has_versions = VersionVector::Full(PureVersionVector::from([2, 4]));
    let summary = RuntimeMessage::Summary(SummaryMessage::new(
        group_id,
        correlation_id,
        has_versions.clone(),
    ));
    let summary_payload = summary.encode_proto().encode_to_bytes();
    let decoded_summary =
        decode_runtime_message(&summary_payload, &memberships).expect("summary should decode");

    let RuntimeMessage::Summary(decoded_summary) = decoded_summary else {
        panic!("summary payload should decode as a summary");
    };
    assert_eq!(
        decoded_summary,
        SummaryMessage::new(group_id, correlation_id, has_versions)
    );
}

#[test]
fn updates_decode_with_member_count_context_from_owned_and_view() {
    let group_id = GroupId(Uuid::from_u128(211));
    let member_count = NonZeroUsize::new(2).expect("two members");
    let update = test_update_message(
        group_id,
        UpdateId {
            version: 3,
            node_index: 1,
        },
        VersionVector::Full(PureVersionVector::from([1, 2])),
    );

    assert_eq!(
        UpdateMessage::decode_proto_with(
            update.encode_proto(),
            MemberCountContext::new(member_count),
        )
        .expect("update should decode with member count"),
        update
    );
    let update_payload = update.encode_proto().encode_to_bytes();
    assert_eq!(
        UpdateMessage::decode_proto_from_slice_with(
            &update_payload,
            MemberCountContext::new(member_count),
        )
        .expect("update slice should decode with member count"),
        update
    );
    let mut update_payload_buf = update_payload.clone();
    assert_eq!(
        UpdateMessage::decode_proto_from_buf_with(
            &mut update_payload_buf,
            MemberCountContext::new(member_count),
        )
        .expect("update buffer should decode with member count"),
        update
    );
    assert_eq!(
        UpdateMessage::decode_proto_view_from_slice_with(
            &update_payload,
            MemberCountContext::new(member_count),
        )
        .expect("update view slice should decode with member count"),
        update
    );
    let update_view =
        replication_proto::UpdateView::decode_view(&update_payload).expect("view should decode");
    assert_eq!(
        UpdateMessage::decode_proto_view_with(&update_view, MemberCountContext::new(member_count),)
            .expect("update view should decode with member count"),
        update
    );
}

#[test]
fn update_batches_decode_with_member_count_context_from_owned_and_view() {
    let group_id = GroupId(Uuid::from_u128(211));
    let member_count = NonZeroUsize::new(2).expect("two members");
    let update = test_update_message(
        group_id,
        UpdateId {
            version: 3,
            node_index: 1,
        },
        VersionVector::Full(PureVersionVector::from([1, 2])),
    );
    let batch = UpdateBatchMessage {
        group_id,
        updates: vec![update],
    };

    assert_eq!(
        UpdateBatchMessage::decode_proto_with(
            batch.encode_proto(),
            MemberCountContext::new(member_count),
        )
        .expect("batch should decode with member count"),
        batch
    );
    let batch_payload = batch.encode_proto().encode_to_bytes();
    assert_eq!(
        UpdateBatchMessage::decode_proto_from_slice_with(
            &batch_payload,
            MemberCountContext::new(member_count),
        )
        .expect("batch slice should decode with member count"),
        batch
    );
    let mut batch_payload_buf = batch_payload.clone();
    assert_eq!(
        UpdateBatchMessage::decode_proto_from_buf_with(
            &mut batch_payload_buf,
            MemberCountContext::new(member_count),
        )
        .expect("batch buffer should decode with member count"),
        batch
    );
    assert_eq!(
        UpdateBatchMessage::decode_proto_view_from_slice_with(
            &batch_payload,
            MemberCountContext::new(member_count),
        )
        .expect("batch view slice should decode with member count"),
        batch
    );
    let batch_view = replication_proto::UpdateBatchView::decode_view(&batch_payload)
        .expect("view should decode");
    assert_eq!(
        UpdateBatchMessage::decode_proto_view_with(
            &batch_view,
            MemberCountContext::new(member_count),
        )
        .expect("batch view should decode with member count"),
        batch
    );
}

#[test]
fn borrowed_proto_sources_match_owned_runtime_message_encoding() {
    let group_id = GroupId(Uuid::from_u128(212));
    let update = test_update_message(
        group_id,
        UpdateId {
            version: 4,
            node_index: 1,
        },
        VersionVector::Full(PureVersionVector::from([1, 3])),
    );
    let summary = SummaryMessage::new(
        group_id,
        Uuid::from_u128(213),
        VersionVector::Override {
            num_members: NonZeroUsize::new(2).expect("two members"),
            version: OverrideVersion::new(6, 1, 7),
        },
    );
    let batch = UpdateBatchMessage {
        group_id,
        updates: vec![update.clone()],
    };

    let mut encoded_update = update.encode_proto();
    let encoded_read_versions = encoded_update
        .read_versions
        .take()
        .expect("update should include read versions");
    assert_eq!(
        CompactVersionVectorProtoCodec::from(&update.read_versions)
            .encode_proto()
            .encode_to_bytes(),
        encoded_read_versions.encode_to_bytes()
    );
    assert_eq!(
        update.dataset_updates[0]
            .view()
            .encode_proto()
            .encode_to_bytes(),
        update.dataset_updates[0].encode_proto().encode_to_bytes()
    );
    assert_eq!(
        update.view().encode_proto().encode_to_bytes(),
        update.encode_proto().encode_to_bytes()
    );
    assert_eq!(
        summary.view().encode_proto().encode_to_bytes(),
        summary.encode_proto().encode_to_bytes()
    );
    assert_eq!(
        batch.view().encode_proto().encode_to_bytes(),
        batch.encode_proto().encode_to_bytes()
    );
}

#[test]
fn stored_update_proto_source_matches_owned_update_message_encoding() {
    let group_id = GroupId(Uuid::from_u128(214));
    let update = ReplicationUpdateRecord {
        group_id,
        update_id: UpdateId {
            version: 5,
            node_index: 0,
        },
        sender: MemberIdentity::from_array(["runtime-message", "sender"]),
        read_versions: VersionVector::Synced {
            num_members: NonZeroUsize::new(2).expect("two members"),
            version: 3,
        },
        dataset_updates: vec![DatasetUpdateRecord {
            dataset_id: DatasetId::try_new("docs").expect("dataset id should build"),
            operations: vec![datamodel_proto::SchemaOperation::default()],
        }],
        applied_locally: true,
    };
    let owned_message = UpdateMessage::from(update.clone());

    assert_eq!(
        DatasetUpdateMessageView::from(&update.dataset_updates[0])
            .encode_proto()
            .encode_to_bytes(),
        owned_message.dataset_updates[0]
            .encode_proto()
            .encode_to_bytes()
    );
    assert_eq!(
        UpdateMessageProtoSource::from(&update)
            .encode_proto()
            .encode_to_bytes(),
        owned_message.encode_proto().encode_to_bytes()
    );
}

#[test]
fn pending_group_messages_round_trip_through_runtime_envelope() {
    let migration_id = MigrationId {
        old_group_id: GroupId(Uuid::from_u128(91_001)),
        new_group_id: GroupId(Uuid::from_u128(91_002)),
    };
    let members = vec![
        MemberIdentity::from_array(["runtime-message", "alice"]),
        MemberIdentity::from_array(["runtime-message", "bob"]),
    ];
    let group_schema = docs_group_schema();
    let group_setup = test_group_setup(&members);
    let memberships = GroupMemberships::new();
    let snapshots = [
        InitialSnapshot::Empty,
        inline_snapshot(),
        metadata_snapshot(migration_id),
    ];
    for snapshot in snapshots.iter().cloned() {
        let invitation = GroupInvitation::new_migration(
            migration_id,
            members.clone(),
            group_schema.clone(),
            snapshot,
            Some("docs".to_owned()),
            Some("join migration".to_owned()),
        );
        let invitation_message =
            GroupInvitationMessage::try_new(invitation, Arc::clone(&group_setup))
                .expect("invitation members should match setup");
        let runtime_message = RuntimeMessage::GroupInvitation(invitation_message);
        assert_eq!(runtime_message.group_id(), migration_id.new_group_id);
        let invitation_payload = runtime_message.encode_proto().encode_to_bytes();
        assert_runtime_decode_paths(&invitation_payload, &memberships, &runtime_message);
    }

    let final_versions = [
        VersionVector::Full(PureVersionVector::from([3, 4])),
        VersionVector::Synced {
            num_members: NonZeroUsize::new(2).expect("two members"),
            version: 3,
        },
        VersionVector::Override {
            num_members: NonZeroUsize::new(2).expect("two members"),
            version: OverrideVersion::new(3, 1, 4),
        },
    ];
    for final_versions in final_versions {
        for snapshot in snapshots.iter().cloned() {
            let proposal = MigrationProposal {
                migration_id,
                final_versions: final_versions.clone(),
                proposed_members: members.clone(),
                group_schema: group_schema.clone(),
                initial_snapshot: snapshot,
                group_name: Some("docs".to_owned()),
                message: Some("migrate".to_owned()),
            };
            let proposal_message =
                MigrationProposalMessage::try_new(proposal, Arc::clone(&group_setup))
                    .expect("proposal members should match setup");
            let runtime_message = RuntimeMessage::MigrationProposal(proposal_message);
            assert_eq!(runtime_message.group_id(), migration_id.old_group_id);
            let proposal_payload = runtime_message.encode_proto().encode_to_bytes();
            assert_runtime_decode_paths(&proposal_payload, &memberships, &runtime_message);
        }
    }
}

#[test]
fn pending_group_message_view_preserves_group_setup_validation() {
    let group_id = GroupId(Uuid::from_u128(92_001));
    let members = vec![
        MemberIdentity::from_array(["runtime-message", "alice"]),
        MemberIdentity::from_array(["runtime-message", "bob"]),
    ];
    let invitation = GroupInvitation::new_creation(
        group_id,
        members.clone(),
        docs_group_schema(),
        InitialSnapshot::Empty,
        None,
        None,
    );
    let memberships = GroupMemberships::new();

    let missing_setup = replication_proto::RuntimeMessage {
        body: Some(replication_proto::runtime_message::Body::GroupInvitation(
            Box::new(invitation.encode_proto()),
        )),
        ..replication_proto::RuntimeMessage::default()
    }
    .encode_to_bytes();
    let borrowed_error = decode_runtime_message(&missing_setup, &memberships)
        .expect_err("borrowed invitation without group setup should fail");
    assert!(matches!(
        borrowed_error,
        RuntimeMessageError::MissingGroupSetup
    ));
    let owned_error = RuntimeMessage::decode_proto_from_slice_with(
        &missing_setup,
        RuntimeMessageDecodeContext::new(&memberships),
    )
    .expect_err("owned invitation without group setup should fail");
    assert!(matches!(
        owned_error,
        RuntimeMessageError::MissingGroupSetup
    ));

    let group_setup = test_group_setup(&members);
    let mut mismatched_invitation = invitation.encode_proto();
    mismatched_invitation.proposed_members.pop();
    mismatched_invitation.group_setup =
        flotsync_messages::buffa::MessageField::some(group_setup.encode_proto());
    let mismatched_setup = replication_proto::RuntimeMessage {
        body: Some(replication_proto::runtime_message::Body::GroupInvitation(
            Box::new(mismatched_invitation),
        )),
        ..replication_proto::RuntimeMessage::default()
    }
    .encode_to_bytes();
    let borrowed_error = decode_runtime_message(&mismatched_setup, &memberships)
        .expect_err("borrowed invitation with mismatched group setup should fail");
    assert!(matches!(
        borrowed_error,
        RuntimeMessageError::GroupSetupMemberMismatch
    ));
    let owned_error = RuntimeMessage::decode_proto_from_slice_with(
        &mismatched_setup,
        RuntimeMessageDecodeContext::new(&memberships),
    )
    .expect_err("owned invitation with mismatched group setup should fail");
    assert!(matches!(
        owned_error,
        RuntimeMessageError::GroupSetupMemberMismatch
    ));
}

#[test]
fn update_range_omits_end_version_for_singletons() {
    let singleton = UpdateRangeMessage {
        producer_index: 1,
        start_version: 7,
        end_version: 7,
    };
    let singleton_proto = singleton.encode_proto();
    assert_eq!(singleton_proto.end_version, None);
    assert_eq!(
        UpdateRangeMessage::decode_proto(singleton_proto).expect("singleton should decode"),
        singleton
    );

    let range = UpdateRangeMessage {
        producer_index: 1,
        start_version: 7,
        end_version: 9,
    };
    let range_proto = range.encode_proto();
    assert_eq!(range_proto.end_version, Some(9));
    assert_eq!(
        UpdateRangeMessage::decode_proto(range_proto).expect("range should decode"),
        range
    );
}

#[test]
fn update_range_rejects_reserved_max_bound() {
    let group_id = GroupId(Uuid::from_u128(303));
    let payload = RuntimeMessage::NeedRange(NeedRangeMessage {
        group_id,
        ranges: vec![UpdateRangeMessage {
            producer_index: 1,
            start_version: u64::MAX - 1,
            end_version: u64::MAX,
        }],
    })
    .encode_proto()
    .encode_to_bytes();

    let error = decode_runtime_message(&payload, &GroupMemberships::new())
        .expect_err("reserved max range bound should be rejected");
    assert!(matches!(
        error,
        RuntimeMessageError::NeedRangeBoundTooLarge {
            producer_index: 1,
            version: u64::MAX,
        }
    ));
}

#[test]
fn update_rejects_reserved_update_id_version() {
    let group_id = GroupId(Uuid::from_u128(304));
    let update_id = UpdateId {
        version: u64::MAX,
        node_index: 0,
    };
    let payload = RuntimeMessage::Update(Box::new(test_update_message(
        group_id,
        update_id,
        VersionVector::initial(NonZeroUsize::new(2).expect("two members")),
    )))
    .encode_proto()
    .encode_to_bytes();

    let memberships = test_memberships(&[(group_id, 2)]);
    let error = decode_runtime_message(&payload, &memberships)
        .expect_err("reserved update id version should be rejected");
    assert!(matches!(
        error,
        RuntimeMessageError::UpdateVersionBoundTooLarge {
            update_id: actual_update_id,
            version: u64::MAX,
        } if actual_update_id == update_id
    ));
}

#[test]
fn update_rejects_reserved_read_version_bound() {
    let group_id = GroupId(Uuid::from_u128(305));
    let payload = RuntimeMessage::Update(Box::new(test_update_message(
        group_id,
        UpdateId {
            version: 1,
            node_index: 0,
        },
        VersionVector::Full(PureVersionVector::from([u64::MAX, 0])),
    )))
    .encode_proto()
    .encode_to_bytes();

    let memberships = test_memberships(&[(group_id, 2)]);
    let error = decode_runtime_message(&payload, &memberships)
        .expect_err("reserved read version should be rejected");
    assert!(matches!(
        error,
        RuntimeMessageError::InvalidReadVersions {
            field: "update.read_versions",
            source: VersionVectorCodecError::VersionBoundTooLarge {
                field: "full.entries",
                version: u64::MAX,
            },
        }
    ));
}

#[test]
fn summary_rejects_reserved_version_bound() {
    let group_id = GroupId(Uuid::from_u128(306));
    let correlation_id = Uuid::from_u128(307);
    let payload = RuntimeMessage::Summary(SummaryMessage::new(
        group_id,
        correlation_id,
        VersionVector::Synced {
            num_members: NonZeroUsize::new(2).expect("two members"),
            version: u64::MAX,
        },
    ))
    .encode_proto()
    .encode_to_bytes();

    let memberships = test_memberships(&[(group_id, 2)]);
    let error = decode_runtime_message(&payload, &memberships)
        .expect_err("reserved summary version should be rejected");
    assert!(matches!(
        error,
        RuntimeMessageError::InvalidReadVersions {
            field: "summary.has_versions",
            source: VersionVectorCodecError::VersionBoundTooLarge {
                field: "synced.group_version",
                version: u64::MAX,
            },
        }
    ));
}

#[test]
fn update_batch_rejects_mismatched_inner_group() {
    let batch_group_id = GroupId(Uuid::from_u128(401));
    let update_group_id = GroupId(Uuid::from_u128(402));
    let update_id = UpdateId {
        version: 1,
        node_index: 0,
    };
    let payload = RuntimeMessage::UpdateBatch(UpdateBatchMessage {
        group_id: batch_group_id,
        updates: vec![test_update_message(
            update_group_id,
            update_id,
            VersionVector::initial(NonZeroUsize::new(2).expect("two members")),
        )],
    })
    .encode_proto()
    .encode_to_bytes();

    let memberships = test_memberships(&[(batch_group_id, 2)]);
    let error = decode_runtime_message(&payload, &memberships)
        .expect_err("mismatched batch group should be rejected");
    assert!(matches!(
        error,
        RuntimeMessageError::UpdateBatchGroupMismatch {
            batch_group: actual_batch_group,
            update_group: actual_update_group,
            update: actual_update,
        } if actual_batch_group == batch_group_id
            && actual_update_group == update_group_id
            && actual_update == update_id
    ));
}
