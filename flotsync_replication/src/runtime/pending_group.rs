//! Pending-group snapshot projection and embedding helpers.

use super::{
    errors::{ChangeGroupMembershipError, GroupActivationError, activation, change_membership},
    in_memory::LocalDataset,
};
use crate::api::{
    DatasetId,
    DatasetRowStatePatch,
    DatasetRowStateWrite,
    GroupSchema,
    InitialDatasetValueRows,
    InitialGroupValueRows,
    InitialSnapshot,
    InitialValueRow,
    ReplicationRowStateRecord,
    ReplicationStoreReadTransaction,
    ReplicationStoreTransaction,
    RowChange,
    RowId,
    RowValues,
};
use flotsync_core::{
    GroupId,
    versions::{UpdateId, VersionVector},
};
use snafu::prelude::*;
use std::{num::NonZeroUsize, sync::Arc};

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
    // TODO(flotsync-git-i20): once the Metadata path is supported, use the
    // inline threshold as this scan limit so snapshot preparation still needs a
    // single storage roundtrip when it decides to embed inline state.
    let row_limit = NonZeroUsize::new(usize::MAX).expect("row scan limit must be non-zero");

    for dataset_schema in group_schema.datasets() {
        let batch = transaction
            .scan_dataset_row_batch(&group_id, &dataset_schema.dataset_id, None, row_limit)
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

/// Embed one dataset's initial value rows and convert them into store/listener output.
fn embed_initial_dataset(
    group_id: GroupId,
    member_count: NonZeroUsize,
    group_schema: &GroupSchema,
    dataset_state: InitialDatasetValueRows,
) -> Result<(DatasetRowStatePatch, Vec<RowChange>), GroupActivationError> {
    // TODO(flotsync-git-vy1): redesign snapshot activation so a dataset does
    // not simultaneously retain initial value rows, embedded CRDT state,
    // store row snapshots, and listener value rows. Large initial snapshots
    // currently multiply row memory until this dataset finishes embedding.
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
    let mut row_changes = Vec::with_capacity(row_keys.len());

    for row_key in row_keys {
        let row_id = RowId {
            group_id,
            dataset_id: dataset_id.clone(),
            row_key,
        };
        let snapshot = dataset
            .snapshot_row(row_key)
            .expect("embedded activation row must be snapshotable");
        let row = dataset
            .clone_value_row(row_key)
            .expect("embedded activation row must be readable");
        actions.push(DatasetRowStateWrite::UpsertActive { row_key, snapshot });
        row_changes.push(RowChange::Upsert {
            row_id,
            row: Arc::new(row),
        });
    }

    Ok((
        DatasetRowStatePatch {
            group_id,
            dataset_id,
            actions,
            last_changed_versions: VersionVector::initial(member_count),
        },
        row_changes,
    ))
}

/// Embed inline initial state into the active group within `transaction`.
///
/// Store patches are flushed one dataset at a time. Listener rows are still
/// returned as one activation event so callers only emit external state after
/// the activation transaction commits.
pub(super) async fn embed_inline_initial_snapshot(
    transaction: &mut dyn ReplicationStoreTransaction,
    group_id: GroupId,
    member_count: NonZeroUsize,
    group_schema: &GroupSchema,
    initial_state: InitialGroupValueRows,
) -> Result<Vec<RowChange>, GroupActivationError> {
    let mut row_changes = Vec::new();
    for dataset_state in initial_state.datasets {
        let (row_patch, dataset_row_changes) =
            embed_initial_dataset(group_id, member_count, group_schema, dataset_state)?;
        if !row_patch.actions.is_empty() {
            transaction
                .apply_dataset_row_patch(row_patch)
                .await
                .context(activation::StoreAccessSnafu)?;
        }
        row_changes.extend(dataset_row_changes);
    }
    Ok(row_changes)
}
