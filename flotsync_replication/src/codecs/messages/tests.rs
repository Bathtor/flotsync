//! Message codec tests.

use super::{
    BootstrapMemberKeyMessage,
    DatasetUpdateMessage,
    DatasetUpdateMessageView,
    GroupInvitationMessage,
    GroupSetupKey,
    GroupSetupMessage,
    MemberCountContext,
    MigrationProposalMessage,
    NeedRangeMessage,
    RuntimeMessage,
    RuntimeMessageError,
    RuntimeVersionVectorProtoSource,
    SummaryMessage,
    SummaryRequestMessage,
    UpdateBatchMessage,
    UpdateMessage,
    UpdateMessageProtoSource,
    UpdateRangeMessage,
    WireRuntimeMessage,
    WireVersionVector,
    WireVersionVectorError,
};
use crate::{
    api::{
        DatasetId,
        DatasetUpdateRecord,
        GroupInvitation,
        InitialSnapshot,
        MigrationId,
        MigrationProposal,
        ReplicationUpdateRecord,
    },
    test_support::{docs_group_schema, test_public_member_keys},
};
use flotsync_core::{
    GroupId,
    MemberIdentity,
    member::TrieMap,
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

#[test]
fn wire_version_vector_round_trips_full_override_and_synced() {
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
        let wire = WireVersionVector::from_runtime(&vector);
        let proto = wire.encode_proto();
        let payload = proto.encode_to_bytes();
        let view =
            versions_proto::VersionVectorView::decode_view(&payload).expect("view should decode");
        assert_eq!(
            WireVersionVector::decode_proto_view(&view).expect("wire view decode should work"),
            wire
        );
        let decoded_wire = WireVersionVector::decode_proto(proto).expect("wire decode should work");
        let decoded_vector = decoded_wire
            .to_runtime(vector.num_members())
            .expect("runtime decode should work");
        assert_eq!(decoded_vector, vector);
    }
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

    assert_eq!(
        WireRuntimeMessage::decode_proto_view_from_slice(&request_payload)
            .expect("summary request should decode"),
        WireRuntimeMessage::SummaryRequest(SummaryRequestMessage {
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
    let decoded_summary = WireRuntimeMessage::decode_proto_view_from_slice(&summary_payload)
        .expect("summary should decode");

    let WireRuntimeMessage::Summary(decoded_summary) = decoded_summary else {
        panic!("summary payload should decode as a summary");
    };
    assert_eq!(
        decoded_summary
            .into_runtime(NonZeroUsize::new(2).expect("two members"))
            .expect("summary versions should normalise"),
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

    assert_eq!(
        RuntimeVersionVectorProtoSource::from(&update.read_versions)
            .encode_proto()
            .encode_to_bytes(),
        WireVersionVector::from_runtime(&update.read_versions)
            .encode_proto()
            .encode_to_bytes()
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
    let invitation = GroupInvitation::new_migration(
        migration_id,
        members.clone(),
        group_schema.clone(),
        InitialSnapshot::Empty,
        Some("docs".to_owned()),
        Some("join migration".to_owned()),
    );
    let invitation_message = GroupInvitationMessage::try_new(invitation, Arc::clone(&group_setup))
        .expect("invitation members should match setup");
    assert_eq!(
        RuntimeMessage::GroupInvitation(invitation_message.clone()).group_id(),
        migration_id.new_group_id
    );
    let invitation_payload = RuntimeMessage::GroupInvitation(invitation_message.clone())
        .encode_proto()
        .encode_to_bytes();
    let decoded_invitation = WireRuntimeMessage::decode_proto_view_from_slice(&invitation_payload)
        .expect("invitation should decode");
    assert_eq!(
        decoded_invitation,
        WireRuntimeMessage::GroupInvitation(invitation_message)
    );

    let proposal = MigrationProposal {
        migration_id,
        final_versions: VersionVector::Full(PureVersionVector::from([3, 4])),
        proposed_members: members,
        group_schema,
        initial_snapshot: InitialSnapshot::Empty,
        group_name: Some("docs".to_owned()),
        message: Some("migrate".to_owned()),
    };
    let proposal_message = MigrationProposalMessage::try_new(proposal, group_setup)
        .expect("proposal members should match setup");
    assert_eq!(
        RuntimeMessage::MigrationProposal(proposal_message.clone()).group_id(),
        migration_id.old_group_id
    );
    let proposal_payload = RuntimeMessage::MigrationProposal(proposal_message.clone())
        .encode_proto()
        .encode_to_bytes();
    let decoded_proposal = WireRuntimeMessage::decode_proto_view_from_slice(&proposal_payload)
        .expect("proposal should decode");
    assert_eq!(
        decoded_proposal,
        WireRuntimeMessage::MigrationProposal(proposal_message)
    );
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

    let error = WireRuntimeMessage::decode_proto_view_from_slice(&payload)
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

    let error = WireRuntimeMessage::decode_proto_view_from_slice(&payload)
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

    let error = WireRuntimeMessage::decode_proto_view_from_slice(&payload)
        .expect_err("reserved read version should be rejected");
    assert!(matches!(
        error,
        RuntimeMessageError::InvalidReadVersions {
            field: "update.read_versions",
            source: WireVersionVectorError::VersionBoundTooLarge {
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

    let error = WireRuntimeMessage::decode_proto_view_from_slice(&payload)
        .expect_err("reserved summary version should be rejected");
    assert!(matches!(
        error,
        RuntimeMessageError::InvalidReadVersions {
            field: "summary.has_versions",
            source: WireVersionVectorError::VersionBoundTooLarge {
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

    let error = WireRuntimeMessage::decode_proto_view_from_slice(&payload)
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
