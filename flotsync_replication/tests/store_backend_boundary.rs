//! Compile-time boundary coverage for downstream replication-store backends.

use flotsync_core::{GroupId, MemberIdentity, versions::UpdateId};
use flotsync_messages::{
    buffa::{Message as _, MessageField, MessageView as _},
    datamodel,
    replication,
    versions,
};
use flotsync_replication::api::ReplicationUpdateView;
use std::num::NonZeroUsize;
use uuid::Uuid;

#[test]
fn downstream_backend_can_invoke_validated_update_view_constructor() {
    let group_id = GroupId(Uuid::from_u128(42));
    let update_id = UpdateId {
        version: 1,
        node_index: 0,
    };
    let message = replication::Update {
        group_id: group_id.0.as_bytes().to_vec(),
        update_id: MessageField::some(datamodel::HistoryId {
            version: update_id.version,
            node_index: update_id.node_index,
            ..datamodel::HistoryId::default()
        }),
        read_versions: MessageField::some(versions::CompactVersionVector {
            versions: Some(versions::compact_version_vector::Versions::Full(Box::new(
                versions::FullVersionVector {
                    entries: vec![0],
                    ..versions::FullVersionVector::default()
                },
            ))),
            ..versions::CompactVersionVector::default()
        }),
        dataset_updates: vec![replication::DatasetUpdate {
            dataset_id: "docs".to_owned(),
            operations: vec![datamodel::SchemaOperation::default()],
            ..replication::DatasetUpdate::default()
        }],
        ..replication::Update::default()
    };
    let encoded = message.encode_to_bytes();
    let source = replication::UpdateView::decode_view(&encoded).unwrap();
    let sender = MemberIdentity::from_array(["app", "alice"]);
    let member_count = NonZeroUsize::new(1).unwrap();

    let view = ReplicationUpdateView::try_from_proto_view(&sender, &source, false, member_count)
        .expect("an external backend must be able to construct a valid page input");
    assert_eq!(view.group_id(), group_id);
    assert_eq!(view.update_id(), update_id);
    assert_eq!(view.dataset_updates().count(), 1);

    let incomplete = replication::Update::default().encode_to_bytes();
    let incomplete_source = replication::UpdateView::decode_view(&incomplete).unwrap();
    assert!(
        ReplicationUpdateView::try_from_proto_view(
            &sender,
            &incomplete_source,
            false,
            member_count
        )
        .is_err(),
        "an incomplete protobuf must fail validation"
    );
}
