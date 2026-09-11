//! Provider lifecycle and batch streaming for planned startup work.

use self::incremental::{
    IncrementalEvidence,
    IncrementalPreparationError,
    prepare_incremental_group,
};
use super::planning::{
    IncrementalCandidate,
    PendingGroupSynchronisation,
    ReadableGroupSynchronisation,
};
use crate::api::{
    BatchProvider,
    DataChangeLineage,
    DataChangeReadPosition,
    DatasetSchema,
    GroupDatasetSchemaRef,
    GroupReadToken,
    ProviderExternalSnafu,
    ProviderFailedSnafu,
    ReplicationStateRowBatch,
    ReplicationStoreReadTransaction,
    RowChange,
    RowChangeBatch,
    RowId,
    RowKey,
    RowProviderError,
    SnapshotRowBatch,
    StoreError,
    StoreErrorResolution,
    StoreErrorScope,
};
use flotsync_core::GroupId;
use flotsync_data_types::schema::Schema;
use flotsync_utils::{BoxError, BoxFuture};
use futures_util::FutureExt;
use snafu::ResultExt as _;
use std::{collections::VecDeque, num::NonZeroUsize};

mod incremental;

#[cfg(test)]
mod tests;

/// One claimed application synchronisation entry and any borrowed row provider.
pub(in crate::runtime) enum ClaimedGroupSynchronisation<'a> {
    /// Complete current rows for one readable group.
    Snapshot {
        group_id: GroupId,
        read_token: GroupReadToken,
        rows: StoreGroupSnapshotProvider<'a>,
    },
    /// Coalesced current changes since one compatible supplied position.
    Changes {
        position: DataChangeReadPosition,
        rows: StoreGroupChangeProvider<'a>,
    },
    /// One supplied group which is absent from the current readable set.
    Retired { group_id: GroupId },
}

/// Single-transaction provider for ordered application synchronisation entries.
pub(in crate::runtime) struct StoreSynchronisationProvider {
    /// Transaction lifecycle, including sticky terminal failure.
    state: StoreSynchronisationProviderState,
    /// Unresolved group work in deterministic group-id order.
    pending_groups: VecDeque<PendingGroupSynchronisation>,
    /// Resolved work borrowed by the currently exposed row provider.
    current_group: Option<ResolvedGroupSynchronisation>,
    /// Maximum rows or update records loaded by one store operation.
    max_rows_per_batch: NonZeroUsize,
    /// Reusable decoded store rows for full snapshot batches.
    state_rows: ReplicationStateRowBatch,
}

impl StoreSynchronisationProvider {
    /// Build one provider over an already prepared, non-empty group list.
    pub(super) fn new(
        transaction: Box<dyn ReplicationStoreReadTransaction>,
        pending_groups: VecDeque<PendingGroupSynchronisation>,
        max_rows_per_batch: NonZeroUsize,
    ) -> Self {
        Self {
            state: StoreSynchronisationProviderState::Streaming(transaction),
            pending_groups,
            current_group: None,
            max_rows_per_batch,
            state_rows: ReplicationStateRowBatch::new(&Schema::empty()),
        }
    }

    /// Prepare and claim the next group without retaining another incremental group in memory.
    ///
    /// `Ok(Some(_))` exposes exactly one pending snapshot, change collection,
    /// or retirement entry. Snapshot and change entries remain installed until
    /// their borrowed row provider reports natural exhaustion.
    ///
    /// `Ok(None)` means either an earlier row provider was dropped before
    /// exhausting the installed current group, or all pending work was already
    /// consumed and the retained transaction has now been released.
    /// [`Self::is_exhausted`] distinguishes those cases for completion.
    ///
    /// An operation-scoped retry error restores the pending candidate, so
    /// another call retries inside the same read transaction. Other errors
    /// leave the provider failed and prevent further reconciliation.
    pub(in crate::runtime) async fn claim_next_group(
        &mut self,
    ) -> Result<Option<ClaimedGroupSynchronisation<'_>>, RowProviderError> {
        match self.state {
            StoreSynchronisationProviderState::Failed => ProviderFailedSnafu.fail(),
            StoreSynchronisationProviderState::Exhausted => Ok(None),
            StoreSynchronisationProviderState::Streaming(_) => self.claim_streaming_group().await,
        }
    }

    /// Claim one pending entry while the retained transaction is readable.
    ///
    /// `Ok(Some(_))` exposes the resolved front entry. `Ok(None)` means either
    /// the previously exposed entry remains installed or no work remains after
    /// releasing the transaction. A preparation error restores its incremental
    /// candidate before returning; retry classification determines whether the
    /// provider remains readable or enters the failed state.
    async fn claim_streaming_group(
        &mut self,
    ) -> Result<Option<ClaimedGroupSynchronisation<'_>>, RowProviderError> {
        if self.current_group.is_some() {
            Ok(None)
        } else if let Some(pending) = self.pending_groups.pop_front() {
            match pending {
                PendingGroupSynchronisation::Snapshot(group) => {
                    let current = group.into_resolved_snapshot();
                    Ok(Some(self.install_current_group(current)))
                }
                PendingGroupSynchronisation::Incremental(candidate) => {
                    match self.prepare_incremental_candidate(&candidate).await {
                        Ok(preparation) => {
                            let current = candidate.into_resolved(preparation);
                            Ok(Some(self.install_current_group(current)))
                        }
                        Err(source) => {
                            let pending = PendingGroupSynchronisation::Incremental(candidate);
                            self.pending_groups.push_front(pending);
                            Err(self.handle_preparation_error(source))
                        }
                    }
                }
                PendingGroupSynchronisation::Retired { group_id } => {
                    if self.pending_groups.is_empty() {
                        self.finish_exhausted().await?;
                    }
                    Ok(Some(ClaimedGroupSynchronisation::Retired { group_id }))
                }
            }
        } else {
            self.finish_exhausted().await?;
            Ok(None)
        }
    }

    /// Prepare one incremental candidate inside the proven streaming transaction.
    ///
    /// `Ok(Complete(_))` contains its coalesced current changes, while
    /// `Ok(SnapshotRequired)` requests conversion of the same candidate to a
    /// complete snapshot. Errors leave lifecycle handling and queue restoration
    /// to the caller.
    async fn prepare_incremental_candidate(
        &mut self,
        candidate: &IncrementalCandidate,
    ) -> Result<IncrementalEvidence<Vec<RowChange>>, IncrementalPreparationError> {
        let batch_size = self.max_rows_per_batch;
        let transaction = self.state.get_streaming_transaction();
        prepare_incremental_group(transaction, candidate, batch_size).await
    }

    /// Install resolved work and expose its matching borrowed provider.
    ///
    /// The returned provider borrows the newly installed current entry. The
    /// caller must not already have another current entry installed.
    fn install_current_group(
        &mut self,
        current: ResolvedGroupSynchronisation,
    ) -> ClaimedGroupSynchronisation<'_> {
        debug_assert!(self.current_group.is_none());
        self.current_group = Some(current);
        self.expose_current_group()
    }

    /// Expose the installed current entry through its matching borrowed provider.
    ///
    /// Returns snapshot metadata and a snapshot provider for a current snapshot,
    /// or an update-lineage position and change provider for current changes.
    /// A resolved current entry must already be installed.
    fn expose_current_group(&mut self) -> ClaimedGroupSynchronisation<'_> {
        let claim = match self
            .current_group
            .as_ref()
            .expect("a resolved current group was installed before exposure")
        {
            ResolvedGroupSynchronisation::Snapshot(group) => CurrentGroupClaim::Snapshot {
                group_id: group.group_id,
                read_token: group.read_token.clone(),
            },
            ResolvedGroupSynchronisation::Changes(group) => CurrentGroupClaim::Changes {
                position: DataChangeReadPosition::new(
                    DataChangeLineage::Update,
                    group.read_token.clone(),
                ),
            },
        };
        match claim {
            CurrentGroupClaim::Snapshot {
                group_id,
                read_token,
            } => ClaimedGroupSynchronisation::Snapshot {
                group_id,
                read_token,
                rows: StoreGroupSnapshotProvider {
                    synchronisation: self,
                },
            },
            CurrentGroupClaim::Changes { position } => ClaimedGroupSynchronisation::Changes {
                position,
                rows: StoreGroupChangeProvider {
                    synchronisation: self,
                },
            },
        }
    }

    /// Convert one preparation error and apply its provider lifecycle effect.
    ///
    /// A retryable operation-scoped store error leaves the retained transaction
    /// streaming. Every other error changes the provider to the terminal failed
    /// state. The returned public error preserves the store classification or
    /// wraps the contextual preparation failure as an external provider error.
    fn handle_preparation_error(
        &mut self,
        source: IncrementalPreparationError,
    ) -> RowProviderError {
        match source {
            IncrementalPreparationError::Store { source } => {
                if !store_failure_is_retryable_in_transaction(&source) {
                    self.state = StoreSynchronisationProviderState::Failed;
                }
                RowProviderError::from_store_error(source)
            }
            source => {
                self.state = StoreSynchronisationProviderState::Failed;
                provider_external_error(source)
            }
        }
    }

    /// Return whether every group was consumed and the read transaction released.
    ///
    /// `true` means reconciliation exhausted all work and released the retained
    /// transaction. `false` means work remains or an earlier error failed the
    /// provider.
    pub(in crate::runtime) const fn is_exhausted(&self) -> bool {
        matches!(self.state, StoreSynchronisationProviderState::Exhausted)
    }

    /// Release this provider without representing its remaining work as consumed.
    ///
    /// A streaming provider explicitly releases its transaction and returns any
    /// release failure. An exhausted provider needs no work and succeeds. An
    /// already failed provider returns the sticky provider-failed error.
    pub(in crate::runtime) async fn abort(self) -> Result<(), RowProviderError> {
        match self.state {
            StoreSynchronisationProviderState::Streaming(transaction) => transaction
                .release()
                .await
                .map_err(RowProviderError::from_store_error),
            StoreSynchronisationProviderState::Exhausted => Ok(()),
            StoreSynchronisationProviderState::Failed => ProviderFailedSnafu.fail(),
        }
    }

    /// Release the read transaction after group state proves natural exhaustion.
    ///
    /// Successful release changes a streaming provider to `Exhausted`; calling
    /// this again while exhausted is a successful no-op. A release failure or
    /// an already failed provider leaves the state as `Failed` and returns an
    /// error. Both group slots must be empty before this method is called.
    async fn finish_exhausted(&mut self) -> Result<(), RowProviderError> {
        debug_assert!(self.pending_groups.is_empty());
        debug_assert!(self.current_group.is_none());
        let previous =
            std::mem::replace(&mut self.state, StoreSynchronisationProviderState::Failed);
        match previous {
            StoreSynchronisationProviderState::Streaming(transaction) => {
                transaction
                    .release()
                    .await
                    .map_err(RowProviderError::from_store_error)?;
                self.state = StoreSynchronisationProviderState::Exhausted;
                Ok(())
            }
            StoreSynchronisationProviderState::Exhausted => {
                self.state = StoreSynchronisationProviderState::Exhausted;
                Ok(())
            }
            StoreSynchronisationProviderState::Failed => ProviderFailedSnafu.fail(),
        }
    }

    /// Fill one full snapshot batch without advancing into a later group.
    ///
    /// `Ok(Batch(_))` carries visible rows and restores the updated snapshot
    /// cursor for the provider's next call. `Ok(GroupExhausted)` means every
    /// dataset was consumed; the current entry remains absent and the retained
    /// transaction is released when no pending group remains.
    ///
    /// A retryable store error restores the current snapshot at its previous
    /// cursor. A terminal store or projection error leaves it absent and changes
    /// the provider to `Failed`.
    async fn fill_snapshot_batch(
        &mut self,
        mut reuse: SnapshotRowBatch,
    ) -> Result<CurrentBatchOutcome<SnapshotRowBatch>, RowProviderError> {
        reuse.clear();
        let current = self.current_group.take();
        let Some(ResolvedGroupSynchronisation::Snapshot(mut group)) = current else {
            self.state = StoreSynchronisationProviderState::Failed;
            return ProviderFailedSnafu.fail();
        };

        while reuse.is_empty() {
            let Some(dataset) = group.datasets.front().cloned() else {
                self.finish_current_group().await?;
                return Ok(CurrentBatchOutcome::GroupExhausted);
            };
            let group_id = group.group_id;
            let after = group.after_row_key;
            let dataset_id = dataset.dataset_id.clone();
            let schema = dataset.schema.clone();
            let dataset_ref = GroupDatasetSchemaRef {
                group_id: &group_id,
                dataset_id: &dataset_id,
                schema: schema.as_schema(),
            };
            let scan_result = {
                let transaction = self.state.get_transaction()?;
                transaction
                    .scan_dataset_row_batch(
                        dataset_ref,
                        after,
                        self.max_rows_per_batch,
                        &mut self.state_rows,
                    )
                    .await
            };
            let batch = match scan_result {
                Ok(batch) => batch,
                Err(source) => {
                    let retryable = store_failure_is_retryable_in_transaction(&source);
                    let error = RowProviderError::from_store_error(source);
                    if retryable {
                        self.current_group = Some(ResolvedGroupSynchronisation::Snapshot(group));
                    } else {
                        self.state = StoreSynchronisationProviderState::Failed;
                    }
                    return Err(error);
                }
            };

            let rows = reuse.prepare(schema, self.max_rows_per_batch.get());
            for record in self.state_rows.rows() {
                let metadata = record.metadata();
                if !metadata.tombstoned {
                    let row_id = RowId::new(group_id, dataset_id.clone(), metadata.row_key);
                    if let Err(source) = rows.push_row_read(row_id, false, &record) {
                        let error = provider_external_error(source);
                        self.state = StoreSynchronisationProviderState::Failed;
                        return Err(error);
                    }
                }
            }

            if let Some(next_after) = batch.next_after {
                group.after_row_key = Some(next_after);
            } else {
                group.datasets.pop_front();
                group.after_row_key = None;
            }
        }

        self.current_group = Some(ResolvedGroupSynchronisation::Snapshot(group));
        Ok(CurrentBatchOutcome::Batch(reuse))
    }

    /// Fill the one in-memory incremental collection.
    ///
    /// A non-empty collection is returned once as `Ok(Batch(_))`, with emitted
    /// state restored for the required exhaustion call. Its next call returns
    /// `Ok(GroupExhausted)`. An empty collection is exhausted immediately.
    /// Exhaustion leaves the current entry absent and releases the retained
    /// transaction when no pending group remains. A variant mismatch returns
    /// the sticky provider error and leaves the provider failed.
    async fn fill_change_batch(
        &mut self,
        mut reuse: RowChangeBatch,
    ) -> Result<CurrentBatchOutcome<RowChangeBatch>, RowProviderError> {
        reuse.clear();
        let current = self.current_group.take();
        let Some(ResolvedGroupSynchronisation::Changes(mut group)) = current else {
            self.state = StoreSynchronisationProviderState::Failed;
            return ProviderFailedSnafu.fail();
        };
        let has_batch = if group.emitted {
            false
        } else {
            group.emitted = true;
            reuse.extend(std::mem::take(&mut group.rows));
            !reuse.is_empty()
        };
        if has_batch {
            self.current_group = Some(ResolvedGroupSynchronisation::Changes(group));
            Ok(CurrentBatchOutcome::Batch(reuse))
        } else {
            self.finish_current_group().await?;
            Ok(CurrentBatchOutcome::GroupExhausted)
        }
    }

    /// Finish already-removed current work and release an otherwise empty provider.
    ///
    /// Returns success without touching the transaction while pending groups
    /// remain. With no pending work, successful release exhausts the provider;
    /// release failure leaves it failed and is returned to the caller.
    async fn finish_current_group(&mut self) -> Result<(), RowProviderError> {
        debug_assert!(self.current_group.is_none());
        if self.pending_groups.is_empty() {
            self.finish_exhausted().await
        } else {
            Ok(())
        }
    }
}

/// Batch-provider view limited to one claimed group snapshot.
pub(in crate::runtime) struct StoreGroupSnapshotProvider<'a> {
    /// Shared transaction and ordered group state.
    synchronisation: &'a mut StoreSynchronisationProvider,
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
        async move {
            let outcome = self.synchronisation.fill_snapshot_batch(reuse).await?;
            Ok(outcome.into_provider_option())
        }
        .boxed()
    }
}

/// Batch-provider view over one complete in-memory incremental group result.
pub(in crate::runtime) struct StoreGroupChangeProvider<'a> {
    /// Shared transaction and ordered group state.
    synchronisation: &'a mut StoreSynchronisationProvider,
}

impl BatchProvider for StoreGroupChangeProvider<'_> {
    type Batch = RowChangeBatch;

    fn new_batch(&self) -> Self::Batch {
        RowChangeBatch::new()
    }

    fn fill_batch(
        &mut self,
        reuse: Self::Batch,
    ) -> BoxFuture<'_, Result<Option<Self::Batch>, RowProviderError>> {
        async move {
            let outcome = self.synchronisation.fill_change_batch(reuse).await?;
            Ok(outcome.into_provider_option())
        }
        .boxed()
    }
}

impl ReadableGroupSynchronisation {
    /// Convert this input into resolved snapshot cursor state.
    fn into_resolved_snapshot(self) -> ResolvedGroupSynchronisation {
        ResolvedGroupSynchronisation::Snapshot(ResolvedGroupSnapshot {
            group_id: self.group_id,
            read_token: self.read_token,
            datasets: self.group_schema.datasets().into(),
            after_row_key: None,
        })
    }
}

/// Work fully resolved before it is exposed to the application.
enum ResolvedGroupSynchronisation {
    /// Complete snapshot cursor over ordered datasets.
    Snapshot(ResolvedGroupSnapshot),
    /// Complete in-memory coalesced changes.
    Changes(ResolvedGroupChanges),
}

/// Resolved complete-snapshot scan state for one group.
struct ResolvedGroupSnapshot {
    /// Group whose rows are scanned.
    group_id: GroupId,
    /// Position reached after applying the complete snapshot.
    read_token: GroupReadToken,
    /// Datasets remaining in deterministic identifier order.
    datasets: VecDeque<DatasetSchema>,
    /// Exclusive lower row-key bound within the current dataset.
    after_row_key: Option<RowKey>,
}

/// Resolved incremental change state for one group.
struct ResolvedGroupChanges {
    /// Position reached after applying the complete change collection.
    read_token: GroupReadToken,
    /// Deduplicated current changes retained for this group only.
    rows: Vec<RowChange>,
    /// Whether the one complete in-memory batch was already returned.
    emitted: bool,
}

/// Internal result of advancing one installed group row provider.
enum CurrentBatchOutcome<Batch> {
    /// One non-empty application batch was produced and current work remains installed.
    Batch(Batch),
    /// The installed group was naturally exhausted and removed.
    GroupExhausted,
}

impl<Batch> CurrentBatchOutcome<Batch> {
    /// Convert the internal lifecycle result to the public batch-provider convention.
    ///
    /// `Batch` becomes `Some(batch)`, while `GroupExhausted` becomes `None`.
    fn into_provider_option(self) -> Option<Batch> {
        match self {
            Self::Batch(batch) => Some(batch),
            Self::GroupExhausted => None,
        }
    }
}

impl IncrementalCandidate {
    /// Convert successful preparation into resolved current work.
    ///
    /// Complete evidence becomes an incremental change entry. Evidence requiring
    /// fallback converts the same readable-group input into a snapshot entry.
    fn into_resolved(
        self,
        preparation: IncrementalEvidence<Vec<RowChange>>,
    ) -> ResolvedGroupSynchronisation {
        match preparation {
            IncrementalEvidence::Complete(rows) => {
                ResolvedGroupSynchronisation::Changes(ResolvedGroupChanges {
                    read_token: self.group.read_token,
                    rows,
                    emitted: false,
                })
            }
            IncrementalEvidence::SnapshotRequired => self.group.into_resolved_snapshot(),
        }
    }
}

/// Lightweight description copied before borrowing the provider mutably.
enum CurrentGroupClaim {
    /// Snapshot metadata carried by the public claimed entry.
    Snapshot {
        group_id: GroupId,
        read_token: GroupReadToken,
    },
    /// Change position carried by the public claimed entry.
    Changes { position: DataChangeReadPosition },
}

/// Store transaction state retained across synchronisation calls.
enum StoreSynchronisationProviderState {
    /// Group work remains readable from the retained coherent store cut.
    Streaming(Box<dyn ReplicationStoreReadTransaction>),
    /// Every group was consumed and the read transaction released successfully.
    Exhausted,
    /// An earlier terminal operation left the provider invalid.
    Failed,
}

impl StoreSynchronisationProviderState {
    /// Return the retained transaction when this provider is still streaming.
    ///
    /// `Ok(_)` exposes the coherent read transaction. `Err(_)` means the
    /// provider was already exhausted or failed and therefore has no readable
    /// transaction.
    fn get_transaction(
        &mut self,
    ) -> Result<&mut dyn ReplicationStoreReadTransaction, RowProviderError> {
        match self {
            Self::Streaming(transaction) => Ok(transaction.as_mut()),
            Self::Exhausted | Self::Failed => ProviderFailedSnafu.fail(),
        }
    }

    /// Return the retained transaction after the caller proved streaming state.
    ///
    /// The caller must have selected the streaming provider branch before this
    /// method is called. Violating that precondition is an internal control-flow
    /// error rather than a recoverable provider failure.
    fn get_streaming_transaction(&mut self) -> &mut dyn ReplicationStoreReadTransaction {
        let Self::Streaming(transaction) = self else {
            unreachable!("the caller already proved that the provider is streaming")
        };
        transaction.as_mut()
    }
}

/// Return whether one failed operation can be repeated inside its existing transaction.
///
/// `true` means the error is operation-scoped with a retry resolution, so the
/// same retained transaction remains usable. `false` means retrying within that
/// transaction is not supported.
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
