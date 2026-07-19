//! Store-backed snapshot row streaming for the runtime component.

use super::*;

/// Snapshot provider backed by one store read transaction.
///
/// The transaction pins a consistent store view while the provider batches rows
/// from requested datasets. Dropping the provider releases the transaction
/// through the store implementation's release-on-drop path.
pub(super) struct StoreSnapshotRowProvider {
    /// Store transaction that pins the snapshot view until the provider is drained or dropped.
    transaction: Option<Box<dyn ReplicationStoreReadTransaction>>,
    /// Group whose local row state is being streamed.
    group_id: GroupId,
    /// Dataset ids still waiting to be scanned.
    datasets: HashSet<DatasetId>,
    /// Schemas for all requested datasets, loaded up front so snapshot batches
    /// can project stored state into value rows without exposing CRDT state.
    schemas: HashMap<DatasetId, SchemaSource>,
    /// Dataset currently being scanned across batches.
    current_dataset: Option<DatasetId>,
    /// Exclusive lower bound for the current dataset scan.
    after_row_key: Option<RowKey>,
    max_rows_per_batch: NonZeroUsize,
    /// Whether retained tombstones should be included as tombstoned value rows.
    include_tombstones: bool,
}

impl StoreSnapshotRowProvider {
    /// Create a provider for one consistent group snapshot.
    pub(super) fn new(
        transaction: Box<dyn ReplicationStoreReadTransaction>,
        group_id: GroupId,
        datasets: HashSet<DatasetId>,
        schemas: HashMap<DatasetId, SchemaSource>,
        max_rows_per_batch: NonZeroUsize,
        include_tombstones: bool,
    ) -> Self {
        Self {
            transaction: Some(transaction),
            group_id,
            datasets,
            schemas,
            current_dataset: None,
            after_row_key: None,
            max_rows_per_batch,
            include_tombstones,
        }
    }

    async fn release_transaction(&mut self) -> Result<(), RowProviderError> {
        let Some(transaction) = self.transaction.take() else {
            return Ok(());
        };
        transaction
            .release()
            .await
            .boxed()
            .context(ProviderExternalSnafu)
    }

    fn select_dataset(&mut self) -> Option<DatasetId> {
        if self.current_dataset.is_none() {
            self.current_dataset = self.datasets.iter().next().cloned();
        }
        self.current_dataset.clone()
    }

    fn finish_current_dataset(&mut self) {
        if let Some(dataset_id) = self.current_dataset.take() {
            self.datasets.remove(&dataset_id);
        }
        self.after_row_key = None;
    }
}

impl BatchProvider for StoreSnapshotRowProvider {
    type Batch = SnapshotValueRowBatch;

    fn new_batch(&self) -> Self::Batch {
        SnapshotValueRowBatch::empty()
    }

    fn fill_batch(
        &mut self,
        mut reuse: Self::Batch,
    ) -> BoxFuture<'_, Result<Option<Self::Batch>, RowProviderError>> {
        async move {
            reuse.clear();
            while reuse.is_empty() {
                let Some(dataset_id) = self.select_dataset() else {
                    self.release_transaction().await?;
                    return Ok(None);
                };
                let group_id = self.group_id;
                let after = self.after_row_key;
                let max_rows_per_batch = self.max_rows_per_batch;
                let Some(transaction) = self.transaction.as_mut() else {
                    return Ok(None);
                };
                let batch = transaction
                    .scan_dataset_row_batch(&group_id, &dataset_id, after, max_rows_per_batch)
                    .await
                    .boxed()
                    .context(ProviderExternalSnafu)?;

                let schema = self
                    .schemas
                    .get(&dataset_id)
                    .expect("snapshot provider datasets must have loaded schemas")
                    .clone();
                let rows = reuse.prepare(schema, self.max_rows_per_batch.get());
                for record in batch.rows {
                    if record.tombstoned && !self.include_tombstones {
                        continue;
                    }
                    let row_id = RowId {
                        group_id: self.group_id,
                        dataset_id: dataset_id.clone(),
                        row_key: record.row_id,
                    };
                    rows.push_row_read(row_id, record.tombstoned, &record.snapshot)
                        .boxed()
                        .context(ProviderExternalSnafu)?;
                }

                if let Some(next_after) = batch.next_after {
                    self.after_row_key = Some(next_after);
                } else {
                    self.finish_current_dataset();
                }
            }

            Ok(Some(reuse))
        }
        .boxed()
    }
}
