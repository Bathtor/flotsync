//! Store-cut preparation and bounded application startup synchronisation.

use super::{
    errors::{InvalidGroupSnafu, RuntimeStartupError, StoreStartupSnafu},
    group_state::{RuntimeGroupStateSnapshot, SharedGroupState, resolve_group_schema},
    in_memory::{ProducerReadCausality, classify_producer_read_causality},
};
use crate::api::{
    ApplicationReadToken,
    ApplicationSchemas,
    BatchProvider,
    DataChangeLineage,
    DataChangeReadPosition,
    DatasetId,
    DatasetSchema,
    GroupDatasetSchemaRef,
    GroupReadToken,
    GroupSchema,
    ProviderExternalSnafu,
    ProviderFailedSnafu,
    ReplicationStateRowBatch,
    ReplicationStore,
    ReplicationStoreReadTransaction,
    ReplicationUpdateFilter,
    RowChange,
    RowChangeBatch,
    RowId,
    RowKey,
    RowProviderError,
    RowValues,
    SnapshotRowBatch,
    StoreError,
    StoreErrorResolution,
    StoreErrorScope,
};
use flotsync_core::{
    GroupId,
    MemberIdentity,
    MemberIndex,
    versions::{UpdateId, VersionVector, VersionVectorGap},
};
use flotsync_data_types::schema::Schema;
use flotsync_messages::codecs::datamodel::{OperationCodecError, decode_schema_operation_row_id};
use flotsync_utils::{BoxError, BoxFuture};
use futures_util::FutureExt;
use snafu::ResultExt as _;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashSet, VecDeque},
    num::NonZeroUsize,
    sync::Arc,
};

/// Prepare the runtime group view and application reconciliation from one store cut.
pub(super) async fn prepare_application_state(
    local_member: &MemberIdentity,
    application_schemas: &'static ApplicationSchemas,
    store: &Arc<dyn ReplicationStore>,
    application_read_token: Option<ApplicationReadToken>,
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
    let mut readable_groups = Vec::with_capacity(group_capacity);
    let mut readable_group_ids = HashSet::with_capacity(group_capacity);
    let mut final_read_token = ApplicationReadToken::default();

    for persisted_group in persisted_groups {
        let group_id = persisted_group.group_id;
        let resolved_group_schema =
            resolve_group_schema(application_schemas, persisted_group.group_schema.clone());
        let readable_group = if persisted_group.lifecycle.is_readable() {
            let read_token = GroupReadToken::from_group_version(
                group_id,
                persisted_group.version_vector.clone(),
            );
            Some((read_token, resolved_group_schema.clone()))
        } else {
            None
        };
        runtime_snapshot
            .insert_record(local_member, resolved_group_schema, persisted_group)
            .context(InvalidGroupSnafu { group_id })?;
        if let Some((read_token, group_schema)) = readable_group {
            readable_group_ids.insert(group_id);
            final_read_token.merge_applied(&read_token);
            readable_groups.push(ReadableGroupSynchronisation {
                group_id,
                read_token,
                group_schema,
            });
        }
    }
    group_state.replace(runtime_snapshot);

    let mut groups = plan_group_synchronisation(
        readable_groups,
        &readable_group_ids,
        application_read_token.as_ref(),
    );
    groups
        .make_contiguous()
        .sort_by_key(PendingGroupSynchronisation::group_id);

    if groups.is_empty() {
        transaction.release().await.context(StoreStartupSnafu)?;
        Ok(PreparedApplicationState::Ready { group_state })
    } else {
        let synchronisation =
            StoreSynchronisationProvider::new(transaction, groups, max_rows_per_batch);
        Ok(PreparedApplicationState::Synchronising {
            group_state,
            final_read_token,
            synchronisation: Box::new(synchronisation),
        })
    }
}

/// Store-derived application state prepared before the Kompact system exists.
pub(super) enum PreparedApplicationState {
    /// No application reconciliation is needed for the prepared store cut.
    Ready {
        /// Complete runtime group view installed into every topology consumer.
        group_state: Arc<SharedGroupState>,
    },
    /// Application state must be reconciled before runtime logic starts.
    Synchronising {
        /// Complete runtime group view installed into every topology consumer.
        group_state: Arc<SharedGroupState>,
        /// Aggregate position which applications may optionally persist after full reconciliation.
        final_read_token: ApplicationReadToken,
        /// Per-group work and the read transaction which fixes its store cut.
        synchronisation: Box<StoreSynchronisationProvider>,
    },
}

/// One claimed application synchronisation entry and any borrowed row provider.
pub(super) enum ClaimedGroupSynchronisation<'a> {
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
pub(super) struct StoreSynchronisationProvider {
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
    fn new(
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
    pub(super) async fn claim_next_group(
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
    pub(super) const fn is_exhausted(&self) -> bool {
        matches!(self.state, StoreSynchronisationProviderState::Exhausted)
    }

    /// Release this provider without representing its remaining work as consumed.
    ///
    /// A streaming provider explicitly releases its transaction and returns any
    /// release failure. An exhausted provider needs no work and succeeds. An
    /// already failed provider returns the sticky provider-failed error.
    pub(super) async fn abort(self) -> Result<(), RowProviderError> {
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
pub(super) struct StoreGroupSnapshotProvider<'a> {
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
pub(super) struct StoreGroupChangeProvider<'a> {
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

/// Ordered, deduplicated affected rows grouped by dataset.
type AffectedRows = BTreeMap<DatasetId, BTreeSet<RowKey>>;

/// Plan all group entries after selecting the application-token mode once.
fn plan_group_synchronisation(
    readable_groups: Vec<ReadableGroupSynchronisation>,
    readable_group_ids: &HashSet<GroupId>,
    application_read_token: Option<&ApplicationReadToken>,
) -> VecDeque<PendingGroupSynchronisation> {
    match application_read_token {
        None => readable_groups
            .into_iter()
            .map(ReadableGroupSynchronisation::synchronise_as_snapshot)
            .collect(),
        Some(application_read_token) => {
            let mut planned = VecDeque::with_capacity(readable_groups.len());
            for group in readable_groups {
                match plan_readable_group(group, application_read_token) {
                    ReadableGroupPlanning::Exact => {
                        // The supplied position already represents this group.
                    }
                    ReadableGroupPlanning::Synchronise(group) => planned.push_back(group),
                }
            }
            for (group_id, _) in application_read_token.group_versions() {
                if !readable_group_ids.contains(group_id) {
                    planned.push_back(PendingGroupSynchronisation::Retired {
                        group_id: *group_id,
                    });
                }
            }
            planned
        }
    }
}

/// Classify one readable group relative to a supplied application position.
fn plan_readable_group(
    group: ReadableGroupSynchronisation,
    application_read_token: &ApplicationReadToken,
) -> ReadableGroupPlanning {
    if let Some(supplied_versions) = application_read_token.group_version(&group.group_id) {
        match supplied_versions.partial_cmp(group.read_token.version()) {
            Some(Ordering::Equal) => ReadableGroupPlanning::Exact,
            Some(Ordering::Less) => ReadableGroupPlanning::Synchronise(
                group.synchronise_as_incremental(supplied_versions.clone()),
            ),
            Some(Ordering::Greater) | None => {
                ReadableGroupPlanning::Synchronise(group.synchronise_as_snapshot())
            }
        }
    } else {
        ReadableGroupPlanning::Synchronise(group.synchronise_as_snapshot())
    }
}

/// Prepare all coalesced current changes for one incremental candidate.
///
/// `Complete` carries the group-local changes. `SnapshotRequired` means
/// retained history or row-creation provenance is legitimately unavailable,
/// so the same group must instead be exposed as a complete snapshot.
async fn prepare_incremental_group(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    candidate: &IncrementalCandidate,
    batch_size: NonZeroUsize,
) -> Result<IncrementalEvidence<Vec<RowChange>>, IncrementalPreparationError> {
    match load_affected_rows(transaction, candidate).await? {
        IncrementalEvidence::Complete(affected_rows) => {
            load_current_changes(transaction, candidate, affected_rows, batch_size).await
        }
        IncrementalEvidence::SnapshotRequired => Ok(IncrementalEvidence::SnapshotRequired),
    }
}

/// Discover every row identity touched between the supplied and current positions.
///
/// `Complete` contains the ordered, deduplicated identities. `SnapshotRequired`
/// means at least one expected retained update is no longer available. Actual
/// store or metadata inconsistencies are returned as errors.
async fn load_affected_rows(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    candidate: &IncrementalCandidate,
) -> Result<IncrementalEvidence<AffectedRows>, IncrementalPreparationError> {
    let mut affected_rows = AffectedRows::new();
    let target_versions = candidate.group.read_token.version();
    let missing_ranges = candidate
        .from_versions
        .missing_version_ranges_to(target_versions);
    let mut snapshot_required = false;
    'ranges: for range in missing_ranges {
        match load_retained_update_range(transaction, candidate, range).await? {
            IncrementalEvidence::Complete(updates) => {
                for update in updates {
                    collect_affected_row_ids(candidate, &mut affected_rows, update)?;
                }
            }
            IncrementalEvidence::SnapshotRequired => {
                snapshot_required = true;
                break 'ranges;
            }
        }
    }
    if snapshot_required {
        Ok(IncrementalEvidence::SnapshotRequired)
    } else {
        Ok(IncrementalEvidence::Complete(affected_rows))
    }
}

/// Load and validate one complete missing producer range.
///
/// `Complete` contains every update in the inclusive range in version order.
/// `SnapshotRequired` means at least one expected update is no longer retained.
/// Malformed returned records are errors rather than snapshot fallback.
async fn load_retained_update_range(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    candidate: &IncrementalCandidate,
    range: VersionVectorGap,
) -> Result<
    IncrementalEvidence<Vec<crate::api::ReplicationUpdateRecord>>,
    IncrementalPreparationError,
> {
    let producer_index = MemberIndex::try_from(range.member_index)
        .expect("version-vector member positions fit in MemberIndex");
    // TODO(flotsync-h3l): Load this range through the store's reusable-batch
    // pagination API once that contract exists.
    let updates = transaction
        .load_replication_updates(
            &candidate.group.group_id,
            ReplicationUpdateFilter::ProducerRange {
                producer_index,
                start_version: range.start_version,
                end_version: range.end_version,
            },
            None,
        )
        .await
        .context(incremental_preparation_error::StoreSnafu)?;
    validate_retained_update_range(candidate, range, updates)
}

/// Validate the complete result returned for one missing producer range.
///
/// `Complete` preserves the validated updates. `SnapshotRequired` means the
/// result has a gap or ends before the inclusive requested range. Records
/// outside the requested filter or in an invalid order are errors.
fn validate_retained_update_range(
    candidate: &IncrementalCandidate,
    range: VersionVectorGap,
    updates: Vec<crate::api::ReplicationUpdateRecord>,
) -> Result<
    IncrementalEvidence<Vec<crate::api::ReplicationUpdateRecord>>,
    IncrementalPreparationError,
> {
    let producer_index = MemberIndex::try_from(range.member_index)
        .expect("version-vector member positions fit in MemberIndex");
    let mut next_version = Some(range.start_version);
    let mut snapshot_required = updates.is_empty();
    for update in &updates {
        if !snapshot_required {
            let expected_version = match next_version {
                Some(expected_version) => expected_version,
                None => incremental_preparation_error::UnexpectedUpdateSnafu {
                    expected_group: candidate.group.group_id,
                    expected_update: UpdateId {
                        node_index: producer_index.as_u32(),
                        version: range.end_version,
                    },
                    actual_group: update.group_id,
                    actual_update: update.update_id,
                }
                .fail()?,
            };
            let expected_id = UpdateId {
                node_index: producer_index.as_u32(),
                version: expected_version,
            };
            if update.group_id != candidate.group.group_id
                || update.update_id.node_index != producer_index.as_u32()
                || update.update_id.version < expected_version
                || update.update_id.version > range.end_version
            {
                incremental_preparation_error::UnexpectedUpdateSnafu {
                    expected_group: candidate.group.group_id,
                    expected_update: expected_id,
                    actual_group: update.group_id,
                    actual_update: update.update_id,
                }
                .fail()?;
            }
            if update.update_id.version > expected_version {
                snapshot_required = true;
            } else {
                validate_retained_update(candidate, update)?;
                next_version = if expected_version == range.end_version {
                    None
                } else {
                    Some(expected_version + 1)
                };
            }
        }
    }
    if snapshot_required || next_version.is_some() {
        Ok(IncrementalEvidence::SnapshotRequired)
    } else {
        Ok(IncrementalEvidence::Complete(updates))
    }
}

/// Collect the affected row identities named by one retained update.
///
/// Success adds every decoded identity to the ordered, deduplicating collection.
/// An unknown dataset or invalid operation row id returns a contextual error.
fn collect_affected_row_ids(
    candidate: &IncrementalCandidate,
    affected_rows: &mut AffectedRows,
    update: crate::api::ReplicationUpdateRecord,
) -> Result<(), IncrementalPreparationError> {
    for dataset_update in update.dataset_updates {
        if candidate
            .group
            .group_schema
            .schema(&dataset_update.dataset_id)
            .is_none()
        {
            return incremental_preparation_error::UnknownDatasetSnafu {
                group: candidate.group.group_id,
                dataset: dataset_update.dataset_id,
                update: update.update_id,
            }
            .fail();
        }
        let dataset_id = dataset_update.dataset_id;
        let dataset_rows = affected_rows.entry(dataset_id.clone()).or_default();
        for operation in dataset_update.operations {
            let row_id = decode_schema_operation_row_id(&operation).context(
                incremental_preparation_error::DecodeOperationSnafu {
                    group: candidate.group.group_id,
                    dataset: dataset_id.clone(),
                    update: update.update_id,
                },
            )?;
            dataset_rows.insert(RowKey(row_id));
        }
    }
    Ok(())
}

/// Materialise current values or deletions for one deduplicated affected-row set.
///
/// `Complete` carries every required current projection. `SnapshotRequired`
/// means a tombstone lacks optional creation provenance, so incremental delete
/// semantics cannot be determined safely. Store-contract violations are errors.
async fn load_current_changes(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    candidate: &IncrementalCandidate,
    mut affected_rows: AffectedRows,
    batch_size: NonZeroUsize,
) -> Result<IncrementalEvidence<Vec<RowChange>>, IncrementalPreparationError> {
    let affected_row_count = affected_rows.values().map(BTreeSet::len).sum();
    let mut changes = Vec::with_capacity(affected_row_count);
    let mut snapshot_required = false;
    'datasets: for dataset in candidate.group.group_schema.datasets() {
        if let Some(mut row_keys) = affected_rows.remove(&dataset.dataset_id) {
            while !row_keys.is_empty() {
                let remaining_row_keys = if row_keys.len() > batch_size.get() {
                    let split_at = *row_keys
                        .iter()
                        .nth(batch_size.get())
                        .expect("an oversized ordered row set has a split key");
                    row_keys.split_off(&split_at)
                } else {
                    BTreeSet::new()
                };
                let requested_row_count = row_keys.len();
                let slice = load_current_row_batch(
                    transaction,
                    candidate.group.group_id,
                    &dataset,
                    &row_keys,
                )
                .await?;
                validate_current_row_slice(
                    candidate.group.group_id,
                    &dataset.dataset_id,
                    requested_row_count,
                    &slice,
                )?;
                for row in slice.state_rows.rows() {
                    match project_current_row(candidate, &dataset, &row)? {
                        CurrentRowProjection::Emit(change) => changes.push(change),
                        CurrentRowProjection::Omit => {
                            // Creation and deletion both followed the supplied position.
                        }
                        CurrentRowProjection::SnapshotRequired => {
                            snapshot_required = true;
                            break 'datasets;
                        }
                    }
                }
                row_keys = remaining_row_keys;
            }
        }
    }
    if snapshot_required {
        Ok(IncrementalEvidence::SnapshotRequired)
    } else if affected_rows.is_empty() {
        changes.sort_by(|left, right| left.row_id().cmp(right.row_id()));
        Ok(IncrementalEvidence::Complete(changes))
    } else {
        let dataset_ids = affected_rows.into_keys().collect::<Vec<_>>();
        incremental_preparation_error::UnexpectedAffectedDatasetsSnafu {
            group: candidate.group.group_id,
            datasets: dataset_ids,
        }
        .fail()
    }
}

/// Load one bounded ordered set of current row identities.
///
/// Success returns the store slice for the exact requested identities. A store
/// failure is preserved as an incremental-preparation store error; validation of
/// the returned slice remains the caller's responsibility.
async fn load_current_row_batch(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    group_id: GroupId,
    dataset: &DatasetSchema,
    row_keys: &BTreeSet<RowKey>,
) -> Result<crate::api::DatasetRowStateSlice, IncrementalPreparationError> {
    let dataset_ref = GroupDatasetSchemaRef {
        group_id: &group_id,
        dataset_id: &dataset.dataset_id,
        schema: dataset.schema.as_schema(),
    };
    let mut row_key_iterator = row_keys.iter();
    transaction
        .load_dataset_rows(dataset_ref, &mut row_key_iterator)
        .await
        .context(incremental_preparation_error::StoreSnafu)
}

/// Validate the store row-slice contract required by current-row projection.
fn validate_current_row_slice(
    group_id: GroupId,
    dataset_id: &DatasetId,
    requested_row_count: usize,
    slice: &crate::api::DatasetRowStateSlice,
) -> Result<(), IncrementalPreparationError> {
    if slice.group_id != group_id || slice.dataset_id != *dataset_id {
        incremental_preparation_error::UnexpectedRowSliceSnafu {
            expected_group: group_id,
            expected_dataset: dataset_id.clone(),
            actual_group: slice.group_id,
            actual_dataset: slice.dataset_id.clone(),
        }
        .fail()
    } else if !slice.dataset_exists {
        incremental_preparation_error::MissingCurrentDatasetSnafu {
            group: group_id,
            dataset: dataset_id.clone(),
        }
        .fail()
    } else if !slice.missing_row_keys.is_empty() {
        incremental_preparation_error::MissingCurrentRowsSnafu {
            group: group_id,
            dataset: dataset_id.clone(),
            row_keys: slice.missing_row_keys.iter().copied().collect::<Vec<_>>(),
        }
        .fail()
    } else if slice.state_rows.len() != requested_row_count {
        incremental_preparation_error::UnexpectedCurrentRowCountSnafu {
            group: group_id,
            dataset: dataset_id.clone(),
            expected: requested_row_count,
            actual: slice.state_rows.len(),
        }
        .fail()
    } else {
        Ok(())
    }
}

/// Project one current stored row into its incremental application effect.
///
/// A visible row produces `Emit` with a complete upsert. A tombstone produces
/// `Emit` when its creation predates the supplied position, `Omit` when creation
/// and deletion both followed that position, or `SnapshotRequired` when optional
/// creation provenance is unavailable. Invalid provenance or row projection
/// returns a contextual error.
fn project_current_row(
    candidate: &IncrementalCandidate,
    dataset: &DatasetSchema,
    row: &flotsync_data_types::schema::datamodel::InMemoryStateRowView<
        '_,
        crate::api::ReplicationRowMetadata,
        UpdateId,
    >,
) -> Result<CurrentRowProjection, IncrementalPreparationError> {
    let metadata = row.metadata();
    let row_id = RowId::new(
        candidate.group.group_id,
        dataset.dataset_id.clone(),
        metadata.row_key,
    );
    if metadata.tombstoned {
        match creation_at_position(
            metadata.created_by,
            &candidate.from_versions,
            candidate.group.group_id,
            &dataset.dataset_id,
            metadata.row_key,
        )? {
            CreationAtPosition::Included => Ok(CurrentRowProjection::Emit(
                RowChange::ordinary_delete(row_id),
            )),
            CreationAtPosition::NotIncluded => Ok(CurrentRowProjection::Omit),
            CreationAtPosition::Unknown => Ok(CurrentRowProjection::SnapshotRequired),
        }
    } else {
        let values = RowValues::from_row(dataset.schema.as_schema(), row)
            .boxed()
            .context(incremental_preparation_error::ProjectRowSnafu {
                group: candidate.group.group_id,
                dataset: dataset.dataset_id.clone(),
                row: metadata.row_key,
            })?;
        Ok(CurrentRowProjection::Emit(RowChange::ordinary_upsert(
            row_id,
            Arc::new(values),
        )))
    }
}

/// Classify whether one stored row creation was represented by a supplied position.
///
/// `Included` means the row existed and a current tombstone must be emitted as
/// a delete. `NotIncluded` means creation and deletion both happened later, so
/// no row change is emitted. `Unknown` means optional creation provenance is
/// absent and the group requires snapshot fallback.
fn creation_at_position(
    created_by: Option<UpdateId>,
    versions: &VersionVector,
    group_id: GroupId,
    dataset_id: &DatasetId,
    row_key: RowKey,
) -> Result<CreationAtPosition, IncrementalPreparationError> {
    match created_by {
        None => Ok(CreationAtPosition::Unknown),
        Some(UpdateId::INITIAL_STATE_ORIGIN) => Ok(CreationAtPosition::Included),
        Some(created_by) => {
            let producer_position = usize::try_from(created_by.node_index)
                .expect("u32 member indices fit into usize on supported platforms");
            if producer_position < versions.num_members().get() {
                if created_by.version <= versions.version_at(producer_position) {
                    Ok(CreationAtPosition::Included)
                } else {
                    Ok(CreationAtPosition::NotIncluded)
                }
            } else {
                incremental_preparation_error::CreatorOutsideGroupSnafu {
                    group: group_id,
                    dataset: dataset_id.clone(),
                    row: row_key,
                    created_by,
                    member_count: versions.num_members().get(),
                }
                .fail()
            }
        }
    }
}

/// Complete readable-group input shared by snapshot and incremental planning.
struct ReadableGroupSynchronisation {
    /// Group affected by this entry.
    group_id: GroupId,
    /// Current position reached after applying the entry.
    read_token: GroupReadToken,
    /// Authoritative schemas used for snapshot or incremental projection.
    group_schema: Arc<GroupSchema>,
}

impl ReadableGroupSynchronisation {
    /// Plan this group as a complete snapshot.
    fn synchronise_as_snapshot(self) -> PendingGroupSynchronisation {
        PendingGroupSynchronisation::Snapshot(self)
    }

    /// Plan this group as a candidate incremental reconciliation.
    fn synchronise_as_incremental(
        self,
        from_versions: VersionVector,
    ) -> PendingGroupSynchronisation {
        PendingGroupSynchronisation::Incremental(IncrementalCandidate {
            group: self,
            from_versions,
        })
    }

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

/// Result of comparing one readable group with a supplied application token.
enum ReadableGroupPlanning {
    /// The supplied position exactly represents this readable group, so no entry is needed.
    Exact,
    /// This group needs the contained snapshot or incremental work.
    Synchronise(PendingGroupSynchronisation),
}

/// Unresolved work waiting in deterministic group order.
enum PendingGroupSynchronisation {
    /// Complete snapshot whose store rows have not yet been scanned.
    Snapshot(ReadableGroupSynchronisation),
    /// Incremental candidate whose retained history has not yet been inspected.
    Incremental(IncrementalCandidate),
    /// Supplied group absent from the readable store cut.
    Retired { group_id: GroupId },
}

impl PendingGroupSynchronisation {
    /// Return the deterministic group-order key for this pending entry.
    const fn group_id(&self) -> GroupId {
        match self {
            Self::Snapshot(group) => group.group_id,
            Self::Incremental(candidate) => candidate.group.group_id,
            Self::Retired { group_id } => *group_id,
        }
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

/// Owned inputs needed while asynchronously preparing one group.
struct IncrementalCandidate {
    /// Current readable group targeted by reconciliation.
    group: ReadableGroupSynchronisation,
    /// Application position from which changes would begin.
    from_versions: VersionVector,
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

/// Expected evidence result from incremental preparation.
enum IncrementalEvidence<T> {
    /// All required evidence was available and produced this value.
    Complete(T),
    /// Missing history or unknown optional provenance requires a complete snapshot.
    SnapshotRequired,
}

/// Validate causal metadata which must hold for one retained applied update.
fn validate_retained_update(
    candidate: &IncrementalCandidate,
    update: &crate::api::ReplicationUpdateRecord,
) -> Result<(), IncrementalPreparationError> {
    let target_versions = candidate.group.read_token.version();
    if !update.applied_locally {
        incremental_preparation_error::UpdateNotAppliedSnafu {
            group: update.group_id,
            update: update.update_id,
        }
        .fail()
    } else if update.read_versions.num_members() != target_versions.num_members()
        || !matches!(
            update.read_versions.partial_cmp(target_versions),
            Some(Ordering::Less | Ordering::Equal)
        )
    {
        incremental_preparation_error::InvalidUpdateReadVersionsSnafu {
            group: update.group_id,
            update: update.update_id,
        }
        .fail()
    } else {
        match classify_producer_read_causality(update) {
            ProducerReadCausality::PrecedesUpdate => Ok(()),
            ProducerReadCausality::IncludesUpdate {
                producer_read_version,
            } => incremental_preparation_error::SelfDependentReadVersionsSnafu {
                group: update.group_id,
                update: update.update_id,
                producer_read_version,
            }
            .fail(),
        }
    }
}

/// Classification of one row creation relative to the supplied application position.
enum CreationAtPosition {
    /// The supplied position already contained the row.
    Included,
    /// The row was created after the supplied position.
    NotIncluded,
    /// Optional store metadata did not retain the creating update.
    Unknown,
}

/// Application effect projected from one affected current stored row.
enum CurrentRowProjection {
    /// Emit this upsert or delete.
    Emit(RowChange),
    /// Emit nothing because the row was both created and deleted later.
    Omit,
    /// Fall back because safe delete projection needs unavailable provenance.
    SnapshotRequired,
}

/// Failure which either permits an in-transaction retry or poisons the provider.
#[derive(Debug, snafu::Snafu)]
#[snafu(module(incremental_preparation_error))]
enum IncrementalPreparationError {
    /// Store operation failed with an inspectable retry classification.
    #[snafu(display("Incremental synchronisation store operation failed: {source}"))]
    Store { source: StoreError },
    /// A producer-range query returned a record outside its requested identity.
    #[snafu(display(
        "Expected retained update {expected_update} in group {expected_group}, but the store returned {actual_update} in group {actual_group}."
    ))]
    UnexpectedUpdate {
        expected_group: GroupId,
        expected_update: UpdateId,
        actual_group: GroupId,
        actual_update: UpdateId,
    },
    /// An update inside the current applied frontier is not reflected in row state.
    #[snafu(display("Retained update {update} in group {group} is not applied locally."))]
    UpdateNotApplied { group: GroupId, update: UpdateId },
    /// A retained update's producer dependency includes its own or a later version.
    #[snafu(display(
        "Retained update {update} in group {group} carries producer read version {producer_read_version}, which does not precede the update."
    ))]
    SelfDependentReadVersions {
        group: GroupId,
        update: UpdateId,
        producer_read_version: u64,
    },
    /// Retained causal metadata is incompatible with the current store cut.
    #[snafu(display(
        "Retained update {update} in group {group} has read versions incompatible with the current group position."
    ))]
    InvalidUpdateReadVersions { group: GroupId, update: UpdateId },
    /// Retained history references a dataset absent from the authoritative schema.
    #[snafu(display(
        "Retained update {update} in group {group} references unknown dataset {dataset}."
    ))]
    UnknownDataset {
        group: GroupId,
        dataset: DatasetId,
        update: UpdateId,
    },
    /// One retained operation has no valid row identity.
    #[snafu(display(
        "Could not decode a row identity from update {update} in group {group}, dataset {dataset}: {source}"
    ))]
    DecodeOperation {
        group: GroupId,
        dataset: DatasetId,
        update: UpdateId,
        source: OperationCodecError,
    },
    /// A row lookup returned metadata for another group or dataset.
    #[snafu(display(
        "Expected current rows for {expected_group}/{expected_dataset}, but received {actual_group}/{actual_dataset}."
    ))]
    UnexpectedRowSlice {
        expected_group: GroupId,
        expected_dataset: DatasetId,
        actual_group: GroupId,
        actual_dataset: DatasetId,
    },
    /// An authoritative current dataset is absent from storage.
    #[snafu(display("Current dataset {dataset} is absent from group {group}."))]
    MissingCurrentDataset { group: GroupId, dataset: DatasetId },
    /// Affected rows are absent even though store tombstones retain row identities.
    #[snafu(display(
        "Current dataset {dataset} in group {group} is missing affected rows {row_keys:?}."
    ))]
    MissingCurrentRows {
        group: GroupId,
        dataset: DatasetId,
        row_keys: Vec<RowKey>,
    },
    /// A row slice violated its exact requested-row count contract.
    #[snafu(display(
        "Current dataset {dataset} in group {group} returned {actual} rows for {expected} requested identities."
    ))]
    UnexpectedCurrentRowCount {
        group: GroupId,
        dataset: DatasetId,
        expected: usize,
        actual: usize,
    },
    /// A current active row could not be projected into application values.
    #[snafu(display(
        "Could not project current row {row} in group {group}, dataset {dataset}: {source}"
    ))]
    ProjectRow {
        group: GroupId,
        dataset: DatasetId,
        row: RowKey,
        source: BoxError,
    },
    /// A row creator names a member outside the supplied version vector.
    #[snafu(display(
        "Row {row} in group {group}, dataset {dataset} was created by {created_by}, outside the {member_count}-member position."
    ))]
    CreatorOutsideGroup {
        group: GroupId,
        dataset: DatasetId,
        row: RowKey,
        created_by: UpdateId,
        member_count: usize,
    },
    /// Affected dataset identities remained after every authoritative schema was visited.
    #[snafu(display("Group {group} retained affected rows for unknown datasets {datasets:?}."))]
    UnexpectedAffectedDatasets {
        group: GroupId,
        datasets: Vec<DatasetId>,
    },
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroUsize;
    use uuid::Uuid;

    /// Build one incremental candidate with a single current producer update.
    fn incremental_candidate() -> IncrementalCandidate {
        let group_id = GroupId(Uuid::from_u128(10));
        let target_versions =
            VersionVector::initial(NonZeroUsize::MIN).with_update_applied(UpdateId {
                node_index: 0,
                version: 1,
            });
        IncrementalCandidate {
            group: ReadableGroupSynchronisation {
                group_id,
                read_token: GroupReadToken::from_group_version(group_id, target_versions),
                group_schema: Arc::new(crate::test_support::docs_group_schema()),
            },
            from_versions: VersionVector::initial(NonZeroUsize::MIN),
        }
    }

    /// Build one retained update for focused validation tests.
    fn retained_update(
        candidate: &IncrementalCandidate,
        dataset_updates: Vec<crate::api::DatasetUpdateRecord>,
    ) -> crate::api::ReplicationUpdateRecord {
        crate::api::ReplicationUpdateRecord {
            group_id: candidate.group.group_id,
            update_id: UpdateId {
                node_index: 0,
                version: 1,
            },
            sender: MemberIdentity::from_array(["test", "member"]),
            read_versions: candidate.from_versions.clone(),
            dataset_updates,
            applied_locally: true,
        }
    }

    #[test]
    fn row_creation_position_names_every_valid_outcome() {
        let versions = VersionVector::initial(NonZeroUsize::MIN).with_update_applied(UpdateId {
            node_index: 0,
            version: 1,
        });
        let group_id = GroupId(Uuid::from_u128(1));
        let dataset_id = DatasetId::try_from_static("docs").expect("dataset id should be valid");
        let row_key = RowKey(Uuid::from_u128(2));

        assert!(matches!(
            creation_at_position(None, &versions, group_id, &dataset_id, row_key),
            Ok(CreationAtPosition::Unknown)
        ));
        assert!(matches!(
            creation_at_position(
                Some(UpdateId::INITIAL_STATE_ORIGIN),
                &versions,
                group_id,
                &dataset_id,
                row_key,
            ),
            Ok(CreationAtPosition::Included)
        ));
        assert!(matches!(
            creation_at_position(
                Some(UpdateId {
                    node_index: 0,
                    version: 1,
                }),
                &versions,
                group_id,
                &dataset_id,
                row_key,
            ),
            Ok(CreationAtPosition::Included)
        ));
        assert!(matches!(
            creation_at_position(
                Some(UpdateId {
                    node_index: 0,
                    version: 2,
                }),
                &versions,
                group_id,
                &dataset_id,
                row_key,
            ),
            Ok(CreationAtPosition::NotIncluded)
        ));
    }

    #[test]
    fn row_creation_position_rejects_a_creator_outside_the_group() {
        let versions = VersionVector::initial(NonZeroUsize::MIN);
        let group_id = GroupId(Uuid::from_u128(3));
        let dataset_id = DatasetId::try_from_static("docs").expect("dataset id should be valid");
        let row_key = RowKey(Uuid::from_u128(4));

        let result = creation_at_position(
            Some(UpdateId {
                node_index: 1,
                version: 1,
            }),
            &versions,
            group_id,
            &dataset_id,
            row_key,
        );

        assert!(matches!(
            result,
            Err(IncrementalPreparationError::CreatorOutsideGroup {
                group: actual_group_id,
                dataset: actual_dataset_id,
                row: actual_row_key,
                created_by: UpdateId {
                    node_index: 1,
                    version: 1,
                },
                member_count: 1,
            }) if actual_group_id == group_id
                && actual_dataset_id == dataset_id
                && actual_row_key == row_key
        ));
    }

    #[test]
    fn retained_history_distinguishes_missing_evidence_from_malformed_records() {
        let candidate = incremental_candidate();
        let range = flotsync_core::versions::VersionVectorGap {
            member_index: 0,
            start_version: 1,
            end_version: 1,
        };
        assert!(matches!(
            validate_retained_update_range(&candidate, range, Vec::new()),
            Ok(IncrementalEvidence::SnapshotRequired)
        ));

        let mut malformed = retained_update(&candidate, Vec::new());
        malformed.group_id = GroupId(Uuid::from_u128(11));
        assert!(matches!(
            validate_retained_update_range(&candidate, range, vec![malformed]),
            Err(IncrementalPreparationError::UnexpectedUpdate { .. })
        ));

        let mut self_dependent = retained_update(&candidate, Vec::new());
        self_dependent.read_versions.increment_at(0);
        assert!(matches!(
            validate_retained_update_range(&candidate, range, vec![self_dependent]),
            Err(IncrementalPreparationError::SelfDependentReadVersions {
                producer_read_version: 1,
                ..
            })
        ));

        let first = retained_update(&candidate, Vec::new());
        let mut extra = first.clone();
        extra.update_id.version = 2;
        assert!(matches!(
            validate_retained_update_range(&candidate, range, vec![first, extra]),
            Err(IncrementalPreparationError::UnexpectedUpdate { .. })
        ));
    }

    #[test]
    fn affected_row_collection_rejects_unknown_datasets_and_malformed_operations() {
        let candidate = incremental_candidate();
        let unknown_dataset = DatasetId::try_from_static("unknown")
            .expect("unknown fixture dataset id should be valid");
        let unknown_update = retained_update(
            &candidate,
            vec![crate::api::DatasetUpdateRecord {
                dataset_id: unknown_dataset,
                operations: Vec::new(),
            }],
        );
        let mut affected_rows = AffectedRows::new();
        assert!(matches!(
            collect_affected_row_ids(&candidate, &mut affected_rows, unknown_update),
            Err(IncrementalPreparationError::UnknownDataset { .. })
        ));

        let malformed_update = retained_update(
            &candidate,
            vec![crate::api::DatasetUpdateRecord {
                dataset_id: crate::test_support::docs_dataset_id(),
                operations: vec![flotsync_messages::datamodel::SchemaOperation::default()],
            }],
        );
        assert!(matches!(
            collect_affected_row_ids(&candidate, &mut affected_rows, malformed_update),
            Err(IncrementalPreparationError::DecodeOperation { .. })
        ));
    }

    #[test]
    fn current_row_slice_contract_violations_are_errors() {
        let candidate = incremental_candidate();
        let group_id = candidate.group.group_id;
        let dataset_id = crate::test_support::docs_dataset_id();
        let empty_rows = || ReplicationStateRowBatch::new(&Schema::empty());
        let wrong_group = crate::api::DatasetRowStateSlice {
            group_id: GroupId(Uuid::from_u128(12)),
            dataset_id: dataset_id.clone(),
            dataset_exists: true,
            state_rows: empty_rows(),
            missing_row_keys: HashSet::new(),
        };
        assert!(matches!(
            validate_current_row_slice(group_id, &dataset_id, 0, &wrong_group),
            Err(IncrementalPreparationError::UnexpectedRowSlice { .. })
        ));

        let missing_dataset = crate::api::DatasetRowStateSlice {
            group_id,
            dataset_id: dataset_id.clone(),
            dataset_exists: false,
            state_rows: empty_rows(),
            missing_row_keys: HashSet::new(),
        };
        assert!(matches!(
            validate_current_row_slice(group_id, &dataset_id, 0, &missing_dataset),
            Err(IncrementalPreparationError::MissingCurrentDataset { .. })
        ));

        let missing_rows = crate::api::DatasetRowStateSlice {
            group_id,
            dataset_id: dataset_id.clone(),
            dataset_exists: true,
            state_rows: empty_rows(),
            missing_row_keys: HashSet::from([RowKey(Uuid::from_u128(13))]),
        };
        assert!(matches!(
            validate_current_row_slice(group_id, &dataset_id, 1, &missing_rows),
            Err(IncrementalPreparationError::MissingCurrentRows { .. })
        ));

        let wrong_count = crate::api::DatasetRowStateSlice {
            group_id,
            dataset_id: dataset_id.clone(),
            dataset_exists: true,
            state_rows: empty_rows(),
            missing_row_keys: HashSet::new(),
        };
        assert!(matches!(
            validate_current_row_slice(group_id, &dataset_id, 1, &wrong_count),
            Err(IncrementalPreparationError::UnexpectedCurrentRowCount { .. })
        ));
    }
}
