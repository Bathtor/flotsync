//! Pending-group snapshot projection and embedding helpers.

use super::{
    errors::{ChangeGroupMembershipError, GroupActivationError, activation, change_membership},
    in_memory::LocalDataset,
};
use crate::api::{
    DatasetId,
    DatasetRowStatePatch,
    DatasetRowStateWrite,
    GroupDatasetSchemaRef,
    GroupSchema,
    InitialDatasetValueRows,
    InitialGroupValueRows,
    InitialSnapshot,
    InitialValueRow,
    ReplicationRowStateRecord,
    ReplicationStoreReadTransaction,
    ReplicationStoreTransaction,
    RowId,
    RowValues,
};
use flotsync_core::{
    GroupId,
    versions::{UpdateId, VersionVector},
};
use snafu::prelude::*;
use std::num::NonZeroUsize;

/// Convert one visible stored row record into new-group initial value state.
fn initial_row_state_from_record(
    old_group_id: GroupId,
    dataset_id: &DatasetId,
    schema: &flotsync_data_types::schema::datamodel::SchemaSource,
    record: &ReplicationRowStateRecord,
) -> Result<InitialValueRow, ChangeGroupMembershipError> {
    let row_key = record.row_id;
    let row_id = RowId {
        group_id: old_group_id,
        dataset_id: dataset_id.clone(),
        row_key,
    };
    let row = RowValues::from_row(schema.as_schema(), &record.snapshot)
        .context(change_membership::SnapshotRowValueSnafu { row_id })?;
    Ok(InitialValueRow { row_key, row })
}

/// Build an inline snapshot by scanning all visible rows in the old group.
pub(super) async fn build_inline_initial_snapshot(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    group_id: GroupId,
    group_schema: &GroupSchema,
) -> Result<InitialSnapshot, ChangeGroupMembershipError> {
    let mut datasets = Vec::new();
    let mut total_rows = 0usize;
    // TODO(flotsync-git-qsg): once the Metadata path is supported, use the
    // inline threshold as this scan limit so snapshot preparation still needs a
    // single storage roundtrip when it decides to embed inline state.
    let row_limit = NonZeroUsize::new(usize::MAX).expect("row scan limit must be non-zero");

    for dataset_schema in group_schema.datasets() {
        let dataset = GroupDatasetSchemaRef {
            group_id: &group_id,
            dataset_id: &dataset_schema.dataset_id,
            schema: dataset_schema.schema.as_schema(),
        };
        let batch = transaction
            .scan_dataset_row_batch(dataset, None, row_limit)
            .await
            .context(change_membership::StoreAccessSnafu)?;
        ensure!(
            batch.next_after.is_none(),
            change_membership::IncompleteInitialSnapshotScanSnafu {
                group_id,
                dataset_id: dataset_schema.dataset_id.clone(),
            }
        );

        let rows = batch
            .rows
            .into_iter()
            .filter(|row| !row.tombstoned)
            .map(|row| {
                initial_row_state_from_record(
                    group_id,
                    &dataset_schema.dataset_id,
                    &dataset_schema.schema,
                    &row,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        if !rows.is_empty() {
            total_rows += rows.len();
            datasets.push(InitialDatasetValueRows {
                dataset_id: dataset_schema.dataset_id,
                rows,
            });
        }
    }

    if total_rows == 0 {
        Ok(InitialSnapshot::Empty)
    } else {
        Ok(InitialSnapshot::Inline(InitialGroupValueRows { datasets }))
    }
}

/// Embed one dataset's initial value rows into a store patch.
fn embed_initial_dataset(
    group_id: GroupId,
    member_count: NonZeroUsize,
    group_schema: &GroupSchema,
    dataset_state: InitialDatasetValueRows,
) -> Result<DatasetRowStatePatch, GroupActivationError> {
    // TODO(flotsync-git-vy1): redesign snapshot activation so a dataset does
    // not simultaneously retain initial value rows, embedded CRDT state,
    // and store row snapshots. Large initial snapshots currently multiply row
    // memory until this dataset finishes embedding.
    let dataset_id = dataset_state.dataset_id;
    let schema = group_schema.schema(&dataset_id).with_context(|| {
        activation::MissingInitialDatasetSchemaSnafu {
            group_id,
            dataset_id: dataset_id.clone(),
        }
    })?;
    let row_keys = dataset_state
        .rows
        .iter()
        .map(|row_state| row_state.row_key)
        .collect::<Vec<_>>();
    let data = flotsync_messages::InMemoryStateData::from_initial_value_rows(
        schema.clone(),
        dataset_state
            .rows
            .into_iter()
            .map(|row_state| (row_state.row_key.0, row_state.row)),
        &UpdateId::INITIAL_STATE_ORIGIN,
    )
    .with_context(|_| activation::EmbedInitialRowsSnafu {
        group_id,
        dataset_id: dataset_id.clone(),
    })?;
    let dataset = LocalDataset { data };
    let mut actions = Vec::with_capacity(row_keys.len());

    for row_key in row_keys {
        let snapshot = dataset
            .snapshot_row(row_key)
            .expect("embedded activation row must be snapshotable");
        actions.push(DatasetRowStateWrite::UpsertActive { row_key, snapshot });
    }

    Ok(DatasetRowStatePatch {
        group_id,
        dataset_id,
        actions,
        change_id: UpdateId::INITIAL_STATE_ORIGIN,
        last_changed_versions: VersionVector::initial(member_count),
    })
}

/// Embed inline initial state into the active group within `transaction`.
///
/// Store patches are flushed one dataset at a time. The activation provider
/// subsequently streams listener rows from committed storage.
pub(super) async fn embed_inline_initial_snapshot(
    transaction: &mut dyn ReplicationStoreTransaction,
    group_id: GroupId,
    member_count: NonZeroUsize,
    group_schema: &GroupSchema,
    initial_state: InitialGroupValueRows,
) -> Result<(), GroupActivationError> {
    for dataset_state in initial_state.datasets {
        let row_patch = embed_initial_dataset(group_id, member_count, group_schema, dataset_state)?;
        if !row_patch.actions.is_empty() {
            let schema = group_schema
                .schema(&row_patch.dataset_id)
                .expect("embedded initial dataset must belong to the target group");
            let dataset = GroupDatasetSchemaRef {
                group_id: &row_patch.group_id,
                dataset_id: &row_patch.dataset_id,
                schema: schema.as_schema(),
            };
            transaction
                .apply_dataset_row_patch(dataset, &row_patch)
                .await
                .context(activation::StoreAccessSnafu)?;
        }
    }
    Ok(())
}

/// Embed one supported activation snapshot into the pending activation transaction.
pub(super) async fn embed_initial_snapshot(
    transaction: &mut dyn ReplicationStoreTransaction,
    group_id: GroupId,
    member_count: NonZeroUsize,
    group_schema: &GroupSchema,
    initial_snapshot: InitialSnapshot,
) -> Result<(), GroupActivationError> {
    match initial_snapshot {
        InitialSnapshot::Empty => Ok(()),
        InitialSnapshot::Inline(initial_state) => {
            embed_inline_initial_snapshot(
                transaction,
                group_id,
                member_count,
                group_schema,
                initial_state,
            )
            .await
        }
        InitialSnapshot::Metadata(_) => {
            activation::UnsupportedInitialSnapshotSnafu { group_id }.fail()
        }
    }
}
