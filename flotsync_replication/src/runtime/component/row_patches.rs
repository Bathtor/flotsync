//! Shared row-patch persistence for publication and inbound application transactions.

use crate::api::{
    DatasetRowStatePatch,
    GroupDatasetSchemaRef,
    GroupSchema,
    ReplicationStoreTransaction,
    StoreError,
};

/// Persist one set of explicit row patches back into the replication store.
///
/// Every patch must already have a schema in the hosted group. Writes follow
/// input order and stop at the first store error; the caller retains ownership
/// of the transaction's commit or rollback.
pub(super) async fn apply_dataset_row_patches(
    transaction: &mut dyn ReplicationStoreTransaction,
    group_schema: &GroupSchema,
    patches: Vec<DatasetRowStatePatch>,
) -> Result<(), StoreError> {
    for patch in patches {
        let schema = group_schema
            .schema(&patch.dataset_id)
            .expect("prepared row patches must belong to the hosted group");
        let dataset = GroupDatasetSchemaRef {
            group_id: &patch.group_id,
            dataset_id: &patch.dataset_id,
            schema: schema.as_schema(),
        };
        transaction.apply_dataset_row_patch(dataset, &patch).await?;
    }
    Ok(())
}
