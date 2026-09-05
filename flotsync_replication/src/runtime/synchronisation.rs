//! Store-cut preparation and bounded snapshot streaming for application startup.

use super::{
    errors::{InvalidGroupSnafu, RuntimeStartupError, StoreStartupSnafu},
    group_state::{RuntimeGroupStateSnapshot, SharedGroupState, resolve_group_schema},
};
use crate::api::{
    ApplicationReadToken,
    ApplicationSchemas,
    BatchProvider,
    DatasetSchema,
    GroupDatasetSchemaRef,
    GroupReadToken,
    ProviderExternalSnafu,
    ProviderFailedSnafu,
    ReplicationStateRowBatch,
    ReplicationStore,
    ReplicationStoreReadTransaction,
    RowId,
    RowKey,
    RowProviderError,
    SnapshotRowBatch,
    StoreError,
    StoreErrorResolution,
    StoreErrorScope,
};
use flotsync_core::{GroupId, MemberIdentity};
use flotsync_data_types::schema::Schema;
use flotsync_utils::{BoxError, BoxFuture};
use futures_util::FutureExt;
use snafu::ResultExt as _;
use std::{collections::VecDeque, num::NonZeroUsize, sync::Arc};

/// Prepare the runtime group view and application snapshots from one store cut.
pub(super) async fn prepare_application_state(
    local_member: &MemberIdentity,
    application_schemas: &'static ApplicationSchemas,
    store: &Arc<dyn ReplicationStore>,
    max_rows_per_batch: NonZeroUsize,
) -> Result<PreparedApplicationState, RuntimeStartupError> {
    let mut transaction = store
        .begin_read_transaction()
        .await
        .context(StoreStartupSnafu)?;
    let mut persisted_groups = transaction
        .load_replication_groups()
        .await
        .context(StoreStartupSnafu)?;
    persisted_groups.sort_by_key(|group| group.group_id);

    let group_capacity = persisted_groups.len();
    let group_state = Arc::new(SharedGroupState::new(application_schemas));
    let mut runtime_snapshot = RuntimeGroupStateSnapshot::new();
    let mut groups = VecDeque::with_capacity(group_capacity);
    let mut final_read_token = ApplicationReadToken::default();

    for persisted_group in persisted_groups {
        let group_id = persisted_group.group_id;
        let resolved_group_schema =
            resolve_group_schema(application_schemas, persisted_group.group_schema.clone());
        let readable_snapshot = if persisted_group.lifecycle.is_readable() {
            let read_token = GroupReadToken::from_group_version(
                group_id,
                persisted_group.version_vector.clone(),
            );
            Some((read_token, resolved_group_schema.datasets().into()))
        } else {
            None
        };
        runtime_snapshot
            .insert_record(local_member, resolved_group_schema, persisted_group)
            .context(InvalidGroupSnafu { group_id })?;
        if let Some((read_token, datasets)) = readable_snapshot {
            final_read_token.merge_applied(&read_token);
            groups.push_back(GroupSnapshotScan {
                group_id,
                read_token,
                datasets,
                after_row_key: None,
                claimed: false,
            });
        }
    }
    group_state.replace(runtime_snapshot);

    if groups.is_empty() {
        transaction.release().await.context(StoreStartupSnafu)?;
        Ok(PreparedApplicationState::Ready { group_state })
    } else {
        let snapshots = StoreSnapshotProvider::new(transaction, groups, max_rows_per_batch);
        Ok(PreparedApplicationState::Synchronising {
            group_state,
            final_read_token,
            snapshots: Box::new(snapshots),
        })
    }
}

/// Store-derived application state prepared before the Kompact system exists.
pub(super) enum PreparedApplicationState {
    /// No application-readable group state needs to be replayed.
    Ready {
        /// Complete runtime group view installed into every topology consumer.
        group_state: Arc<SharedGroupState>,
    },
    /// Application-readable state and the provider retaining its coherent store cut.
    Synchronising {
        /// Complete runtime group view installed into every topology consumer.
        group_state: Arc<SharedGroupState>,
        /// Optional aggregate convenience position for the complete replay.
        final_read_token: ApplicationReadToken,
        /// Per-group snapshots and the read transaction which fixes their store cut.
        snapshots: Box<StoreSnapshotProvider>,
    },
}

/// Single-transaction provider for the ordered readable-group snapshots.
pub(super) struct StoreSnapshotProvider {
    /// Transaction lifecycle, including sticky terminal failure.
    state: StoreSnapshotProviderState,
    /// Group metadata and corresponding dataset scans in application order.
    groups: VecDeque<GroupSnapshotScan>,
    /// Maximum stored rows inspected in one batch.
    max_rows_per_batch: NonZeroUsize,
    /// Reusable decoded store rows for the current batch.
    state_rows: ReplicationStateRowBatch,
}

impl StoreSnapshotProvider {
    /// Build one provider over an already prepared, non-empty group list.
    fn new(
        transaction: Box<dyn ReplicationStoreReadTransaction>,
        groups: VecDeque<GroupSnapshotScan>,
        max_rows_per_batch: NonZeroUsize,
    ) -> Self {
        Self {
            state: StoreSnapshotProviderState::Streaming(transaction),
            groups,
            max_rows_per_batch,
            state_rows: ReplicationStateRowBatch::new(&Schema::empty()),
        }
    }

    /// Claim the next group for sequential application processing.
    ///
    /// `None` means there is no unclaimed group. This includes a caller which
    /// dropped the current group before its snapshot was consumed; completion
    /// will still reject that incomplete provider.
    pub(super) fn claim_next_group(&mut self) -> Option<(GroupId, GroupReadToken)> {
        let group = self.groups.front_mut()?;
        if group.claimed {
            return None;
        }
        group.claimed = true;
        Some((group.group_id, group.read_token.clone()))
    }

    /// Borrow a batch provider restricted to the claimed group.
    pub(super) fn rows_for_group(&mut self, group_id: GroupId) -> StoreGroupSnapshotProvider<'_> {
        StoreGroupSnapshotProvider {
            group_id,
            snapshots: self,
        }
    }

    /// Return whether every group was scanned and the read transaction released.
    pub(super) const fn is_exhausted(&self) -> bool {
        matches!(self.state, StoreSnapshotProviderState::Exhausted)
    }

    /// Release this provider without representing its remaining rows as consumed.
    pub(super) async fn abort(self) -> Result<(), RowProviderError> {
        match self.state {
            StoreSnapshotProviderState::Streaming(transaction) => transaction
                .release()
                .await
                .boxed()
                .context(ProviderExternalSnafu),
            StoreSnapshotProviderState::Exhausted => Ok(()),
            StoreSnapshotProviderState::Failed => ProviderFailedSnafu.fail(),
        }
    }

    /// Release the read transaction after scan state proves natural exhaustion.
    async fn finish_exhausted(&mut self) -> Result<(), RowProviderError> {
        debug_assert!(self.groups.is_empty());
        let previous = std::mem::replace(&mut self.state, StoreSnapshotProviderState::Failed);
        match previous {
            StoreSnapshotProviderState::Streaming(transaction) => {
                transaction
                    .release()
                    .await
                    .boxed()
                    .context(ProviderExternalSnafu)?;
                self.state = StoreSnapshotProviderState::Exhausted;
                Ok(())
            }
            StoreSnapshotProviderState::Exhausted => {
                self.state = StoreSnapshotProviderState::Exhausted;
                Ok(())
            }
            StoreSnapshotProviderState::Failed => ProviderFailedSnafu.fail(),
        }
    }

    /// Fill one batch without advancing into a later group.
    async fn fill_group_batch(
        &mut self,
        group_id: GroupId,
        mut reuse: SnapshotRowBatch,
    ) -> Result<Option<SnapshotRowBatch>, RowProviderError> {
        reuse.clear();
        while reuse.is_empty() {
            let Some(group) = self.groups.front() else {
                self.finish_exhausted().await?;
                return Ok(None);
            };
            if group.group_id != group_id {
                return Ok(None);
            }
            if !group.claimed {
                self.state = StoreSnapshotProviderState::Failed;
                return ProviderFailedSnafu.fail();
            }
            let Some(dataset) = group.datasets.front() else {
                self.groups.pop_front();
                continue;
            };

            let dataset_id = dataset.dataset_id.clone();
            let schema = dataset.schema.clone();
            let after = group.after_row_key;
            let dataset_ref = GroupDatasetSchemaRef {
                group_id: &group_id,
                dataset_id: &dataset_id,
                schema: schema.as_schema(),
            };
            let StoreSnapshotProviderState::Streaming(transaction) = &mut self.state else {
                return ProviderFailedSnafu.fail();
            };
            let scan_result = transaction
                .scan_dataset_row_batch(
                    dataset_ref,
                    after,
                    self.max_rows_per_batch,
                    &mut self.state_rows,
                )
                .await;
            let batch = match scan_result {
                Ok(batch) => batch,
                Err(source) => {
                    let retryable = store_failure_is_retryable_in_transaction(&source);
                    let error = provider_external_error(source);
                    if !retryable {
                        self.state = StoreSnapshotProviderState::Failed;
                    }
                    return Err(error);
                }
            };

            let rows = reuse.prepare(schema, self.max_rows_per_batch.get());
            for record in self.state_rows.rows() {
                let metadata = record.metadata();
                if !metadata.tombstoned {
                    let row_id = RowId {
                        group_id,
                        dataset_id: dataset_id.clone(),
                        row_key: metadata.row_key,
                    };
                    if let Err(source) = rows.push_row_read(row_id, false, &record) {
                        let error = provider_external_error(source);
                        self.state = StoreSnapshotProviderState::Failed;
                        return Err(error);
                    }
                }
            }

            if let Some(next_after) = batch.next_after {
                self.groups
                    .front_mut()
                    .expect("the scanned group remains current")
                    .after_row_key = Some(next_after);
            } else {
                self.finish_current_dataset();
            }
        }
        Ok(Some(reuse))
    }

    /// Advance past one exhausted dataset and its group when no datasets remain.
    fn finish_current_dataset(&mut self) {
        if let Some(group) = self.groups.front_mut() {
            group.datasets.pop_front();
            group.after_row_key = None;
            if group.datasets.is_empty() {
                self.groups.pop_front();
            }
        }
    }
}

/// Batch-provider view limited to one claimed group snapshot.
pub(super) struct StoreGroupSnapshotProvider<'a> {
    /// Group whose rows this view may emit.
    group_id: GroupId,
    /// Shared transaction and ordered group scan state.
    snapshots: &'a mut StoreSnapshotProvider,
}

impl BatchProvider for StoreGroupSnapshotProvider<'_> {
    type Batch = SnapshotRowBatch;

    fn new_batch(&self) -> Self::Batch {
        SnapshotRowBatch::empty()
    }

    fn fill_batch(
        &mut self,
        reuse: Self::Batch,
    ) -> BoxFuture<'_, Result<Option<Self::Batch>, RowProviderError>> {
        async move { self.snapshots.fill_group_batch(self.group_id, reuse).await }.boxed()
    }
}

/// One readable group's application metadata and ordered dataset scan state.
struct GroupSnapshotScan {
    /// Replication group represented by this snapshot.
    group_id: GroupId,
    /// Group position reached after applying this complete snapshot.
    read_token: GroupReadToken,
    /// Dataset schemas remaining in deterministic identifier order.
    datasets: VecDeque<DatasetSchema>,
    /// Exclusive lower row-key bound within the first remaining dataset.
    after_row_key: Option<RowKey>,
    /// Whether the application has received this group synchronisation.
    claimed: bool,
}

/// Store transaction state retained across snapshot provider calls.
enum StoreSnapshotProviderState {
    /// Rows remain readable from the retained coherent store cut.
    Streaming(Box<dyn ReplicationStoreReadTransaction>),
    /// Every group was consumed and the read transaction released successfully.
    Exhausted,
    /// An earlier terminal operation left the provider invalid.
    Failed,
}

/// Return whether one failed scan can be repeated inside its existing transaction.
fn store_failure_is_retryable_in_transaction(source: &StoreError) -> bool {
    let classification = source.classification();
    classification.scope == StoreErrorScope::Operation
        && classification.resolution == StoreErrorResolution::Retry
}

/// Preserve an arbitrary provider source through the public provider error shape.
fn provider_external_error(source: impl Into<BoxError>) -> RowProviderError {
    let source = source.into();
    Result::<(), BoxError>::Err(source)
        .context(ProviderExternalSnafu)
        .expect_err("the constructed provider result is always an error")
}
