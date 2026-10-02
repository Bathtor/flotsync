//! Stored update validation and current-row projection for incremental startup work.

use super::super::{
    super::in_memory::{ProducerReadCausality, classify_producer_read_causality},
    planning::IncrementalCandidate,
};
use crate::api::{
    DatasetId,
    DatasetIdError,
    DatasetSchema,
    GroupDatasetSchemaRef,
    PageCursor,
    ReplicationStoreReadTransaction,
    ReplicationUpdateFilter,
    ReplicationUpdatePageInput,
    ReplicationUpdateView,
    ReplicationUpdatesQuery,
    RowChange,
    RowId,
    RowKey,
    RowValues,
    StoreError,
    VecPageBatch,
};
use flotsync_core::{
    GroupId,
    MemberIndex,
    versions::{UpdateId, VersionVector, VersionVectorGap},
};
use flotsync_messages::codecs::datamodel::{
    OperationCodecError,
    decode_schema_operation_view_row_id,
};
use flotsync_utils::BoxError;
use snafu::{OptionExt as _, ResultExt as _};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
    sync::Arc,
};

/// Ordered, deduplicated affected rows grouped by dataset.
pub(super) type AffectedRows = BTreeMap<DatasetId, BTreeSet<RowKey>>;

/// Prepare all coalesced current changes for one incremental candidate.
///
/// `Complete` carries the group-local changes. `SnapshotRequired` means
/// stored update history or row-creation provenance is legitimately unavailable,
/// so the same group must instead be exposed as a complete snapshot.
pub(super) async fn prepare_incremental_group(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    candidate: &IncrementalCandidate,
    batch_size: NonZeroUsize,
) -> Result<IncrementalEvidence<Vec<RowChange>>, IncrementalPreparationError> {
    match load_affected_rows(transaction, candidate, batch_size).await? {
        IncrementalEvidence::Complete(affected_rows) => {
            load_current_changes(transaction, candidate, affected_rows, batch_size).await
        }
        IncrementalEvidence::SnapshotRequired => Ok(IncrementalEvidence::SnapshotRequired),
    }
}

/// Position of one record within a requested producer range.
pub(super) enum UpdateRangePosition {
    /// This is the exact next producer version expected by reconciliation.
    Expected,
    /// At least one earlier producer version is missing from stored history.
    Gap,
}

/// Expected producer and version carried across every page of one range.
pub(super) struct UpdateRangeProgress {
    /// Group whose update log is being reconciled.
    group_id: GroupId,
    /// Producer whose inclusive version interval was requested.
    producer_index: MemberIndex,
    /// Final inclusive version requested from that producer.
    end_version: u64,
    /// Next required version; `None` means the final version was accepted.
    next_version: Option<u64>,
}

impl UpdateRangeProgress {
    /// Start validating one inclusive producer range.
    pub(super) fn new(group_id: GroupId, range: VersionVectorGap) -> Self {
        Self {
            group_id,
            producer_index: MemberIndex::try_from(range.member_index)
                .expect("version-vector member positions fit in MemberIndex"),
            end_version: range.end_version,
            next_version: Some(range.start_version),
        }
    }

    /// Accept an exact next record, report missing history, or reject a store inconsistency.
    pub(super) fn accept(
        &mut self,
        actual_group: GroupId,
        actual_update: UpdateId,
    ) -> Result<UpdateRangePosition, IncrementalPreparationError> {
        let expected_version =
            self.next_version
                .context(incremental_preparation_error::UnexpectedUpdateSnafu {
                    expected_group: self.group_id,
                    expected_update: UpdateId {
                        node_index: self.producer_index.as_u32(),
                        version: self.end_version,
                    },
                    actual_group,
                    actual_update,
                })?;
        let expected_update = UpdateId {
            node_index: self.producer_index.as_u32(),
            version: expected_version,
        };
        if actual_group != self.group_id
            || actual_update.node_index != self.producer_index.as_u32()
            || actual_update.version < expected_version
            || actual_update.version > self.end_version
        {
            incremental_preparation_error::UnexpectedUpdateSnafu {
                expected_group: self.group_id,
                expected_update,
                actual_group,
                actual_update,
            }
            .fail()
        } else if actual_update.version > expected_version {
            Ok(UpdateRangePosition::Gap)
        } else {
            self.next_version = if expected_version == self.end_version {
                None
            } else {
                Some(expected_version + 1)
            };
            Ok(UpdateRangePosition::Expected)
        }
    }

    /// Return `true` only after the inclusive final version was accepted.
    pub(super) fn is_complete(&self) -> bool {
        self.next_version.is_none()
    }
}

/// Check that a stored update references an authoritative group dataset.
pub(super) fn ensure_known_dataset(
    candidate: &IncrementalCandidate,
    dataset_id: &DatasetId,
    update_id: UpdateId,
) -> Result<(), IncrementalPreparationError> {
    if candidate.group.group_schema.schema(dataset_id).is_none() {
        incremental_preparation_error::UnknownDatasetSnafu {
            group: candidate.group.group_id,
            dataset: dataset_id.clone(),
            update: update_id,
        }
        .fail()
    } else {
        Ok(())
    }
}

/// Validate metadata for an update used to reconstruct current row changes.
///
/// The update must already be applied locally. Its read versions must have the
/// same member count as the target group version and be no later than that
/// target. The producer's own read version must precede this update's version.
pub(super) fn validate_update_metadata(
    candidate: &IncrementalCandidate,
    group_id: GroupId,
    update_id: UpdateId,
    read_versions: &VersionVector,
    applied_locally: bool,
) -> Result<(), IncrementalPreparationError> {
    let target_versions = candidate.group.read_token.version();
    if !applied_locally {
        incremental_preparation_error::UpdateNotAppliedSnafu {
            group: group_id,
            update: update_id,
        }
        .fail()
    } else if read_versions.num_members() != target_versions.num_members()
        || !matches!(
            read_versions.partial_cmp(target_versions),
            Some(Ordering::Less | Ordering::Equal)
        )
    {
        incremental_preparation_error::InvalidUpdateReadVersionsSnafu {
            group: group_id,
            update: update_id,
        }
        .fail()
    } else {
        match classify_producer_read_causality(update_id, read_versions) {
            ProducerReadCausality::PrecedesUpdate => Ok(()),
            ProducerReadCausality::IncludesUpdate {
                producer_read_version,
            } => incremental_preparation_error::SelfDependentReadVersionsSnafu {
                group: group_id,
                update: update_id,
                producer_read_version,
            }
            .fail(),
        }
    }
}

/// Validate the store row-slice contract required by current-row projection.
pub(super) fn validate_current_row_slice(
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

/// Classify whether one stored row creation was represented by a supplied position.
///
/// `Included` means the row existed and a current tombstone must be emitted as
/// a delete. `NotIncluded` means creation and deletion both happened later, so
/// no row change is emitted. `Unknown` means optional creation provenance is
/// absent and the group requires snapshot fallback.
pub(super) fn creation_at_position(
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

/// Expected evidence result from incremental preparation.
pub(super) enum IncrementalEvidence<T> {
    /// All required evidence was available and produced this value.
    Complete(T),
    /// Missing history or unknown optional provenance requires a complete snapshot.
    SnapshotRequired,
}

/// Classification of one row creation relative to the supplied application position.
pub(super) enum CreationAtPosition {
    /// The supplied position already contained the row.
    Included,
    /// The row was created after the supplied position.
    NotIncluded,
    /// Optional store metadata did not include the creating update.
    Unknown,
}

/// Failure which either permits an in-transaction retry or poisons the provider.
#[derive(Debug, snafu::Snafu)]
#[snafu(module(incremental_preparation_error))]
pub(super) enum IncrementalPreparationError {
    /// Store operation failed with an inspectable retry classification.
    #[snafu(display("Incremental synchronisation store operation failed: {source}"))]
    Store { source: StoreError },
    /// A producer-range query returned a record outside its requested identity.
    #[snafu(display(
        "Expected update {expected_update} in group {expected_group}, but the store returned {actual_update} in group {actual_group}."
    ))]
    UnexpectedUpdate {
        expected_group: GroupId,
        expected_update: UpdateId,
        actual_group: GroupId,
        actual_update: UpdateId,
    },
    /// An update inside the current applied frontier is not reflected in row state.
    #[snafu(display("Update {update} in group {group} is not applied locally."))]
    UpdateNotApplied { group: GroupId, update: UpdateId },
    /// An update's producer dependency includes its own or a later version.
    #[snafu(display(
        "Update {update} in group {group} carries producer read version {producer_read_version}, which does not precede the update."
    ))]
    SelfDependentReadVersions {
        group: GroupId,
        update: UpdateId,
        producer_read_version: u64,
    },
    /// Stored causal metadata is incompatible with the current group position.
    #[snafu(display(
        "Update {update} in group {group} has read versions incompatible with the current group position."
    ))]
    InvalidUpdateReadVersions { group: GroupId, update: UpdateId },
    /// A stored update references a dataset absent from the authoritative schema.
    #[snafu(display("Update {update} in group {group} references unknown dataset {dataset}."))]
    UnknownDataset {
        group: GroupId,
        dataset: DatasetId,
        update: UpdateId,
    },
    /// One stored operation has no valid row identity.
    #[snafu(display(
        "Could not decode a row identity from update {update} in group {group}, dataset {dataset}: {source}"
    ))]
    DecodeOperation {
        group: GroupId,
        dataset: DatasetId,
        update: UpdateId,
        source: OperationCodecError,
    },
    /// A dataset identifier from a borrowed update could not be converted.
    #[snafu(display(
        "Could not convert dataset id {dataset} from update {update} in group {group}: {source}"
    ))]
    InvalidProjectedDatasetId {
        group: GroupId,
        update: UpdateId,
        dataset: String,
        source: DatasetIdError,
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
    /// Affected rows are absent even though store tombstones preserve row identities.
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
    #[snafu(display("Group {group} has affected rows for unknown datasets {datasets:?}."))]
    UnexpectedAffectedDatasets {
        group: GroupId,
        datasets: Vec<DatasetId>,
    },
}

/// Discover every row identity touched between the supplied and current positions.
///
/// `Complete` contains the ordered, deduplicated identities. `SnapshotRequired`
/// means at least one expected update is no longer available. Actual
/// store or metadata inconsistencies are returned as errors.
async fn load_affected_rows(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    candidate: &IncrementalCandidate,
    batch_size: NonZeroUsize,
) -> Result<IncrementalEvidence<AffectedRows>, IncrementalPreparationError> {
    let mut affected_rows = AffectedRows::new();
    let target_versions = candidate.group.read_token.version();
    let missing_ranges = candidate
        .from_versions
        .missing_version_ranges_to(target_versions);
    let mut snapshot_required = false;
    'ranges: for range in missing_ranges {
        let range_evidence = load_update_range(
            transaction,
            candidate,
            range,
            batch_size,
            &mut affected_rows,
        )
        .await?;
        match range_evidence {
            IncrementalEvidence::Complete(()) => {
                // This range's rows were already merged into affected_rows.
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
/// `Complete` means every update in the inclusive range was folded into
/// `affected_rows` in version order.
/// `SnapshotRequired` means at least one expected update is absent from storage.
/// Malformed returned records are errors rather than snapshot fallback.
async fn load_update_range(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    candidate: &IncrementalCandidate,
    range: VersionVectorGap,
    batch_size: NonZeroUsize,
    affected_rows: &mut AffectedRows,
) -> Result<IncrementalEvidence<()>, IncrementalPreparationError> {
    let producer_index = MemberIndex::try_from(range.member_index)
        .expect("version-vector member positions fit in MemberIndex");
    let query = ReplicationUpdatesQuery::new(
        candidate.group.group_id,
        ReplicationUpdateFilter::ProducerRange {
            producer_index,
            start_version: range.start_version,
            end_version: range.end_version,
        },
    );
    let mut cursor = PageCursor::new(query);
    let project =
        |view: ReplicationUpdateView<'_>| Ok::<_, BoxError>(project_update_row_keys(&view));
    let mut batch =
        VecPageBatch::<ProjectedUpdate, (), ReplicationUpdatePageInput, _>::bounded_with(
            batch_size, project,
        );
    let mut progress = UpdateRangeProgress::new(candidate.group.group_id, range);
    while cursor.has_more() {
        transaction
            .load_replication_updates_into(&mut cursor, &mut batch)
            .await
            .map_err(StoreError::from)
            .context(incremental_preparation_error::StoreSnafu)?;
        for projected in batch.values_mut().drain(..) {
            let position = progress.accept(projected.group_id, projected.update_id)?;
            if matches!(position, UpdateRangePosition::Gap) {
                return Ok(IncrementalEvidence::SnapshotRequired);
            }
            validate_update_metadata(
                candidate,
                projected.group_id,
                projected.update_id,
                &projected.read_versions,
                projected.applied_locally,
            )?;
            let row_keys_by_dataset = projected.row_keys_result?;
            for (dataset_id, row_keys) in row_keys_by_dataset {
                ensure_known_dataset(candidate, &dataset_id, projected.update_id)?;
                affected_rows
                    .entry(dataset_id)
                    .or_default()
                    .extend(row_keys);
            }
        }
    }
    if progress.is_complete() {
        Ok(IncrementalEvidence::Complete(()))
    } else {
        Ok(IncrementalEvidence::SnapshotRequired)
    }
}

/// Owned scalar metadata and affected row identities from one borrowed update.
///
/// Decoding failures travel as values so the caller can attach the established
/// incremental error after the page fill rather than losing it in a batch error.
struct ProjectedUpdate {
    /// Group whose update log supplied this record.
    group_id: GroupId,
    /// Producer and version selected from the store.
    update_id: UpdateId,
    /// Causal position carried by the update.
    read_versions: VersionVector,
    /// Whether current row state incorporates this update.
    applied_locally: bool,
    /// Decoded dataset and row identities, or their contextual decoding error.
    row_keys_result: Result<Vec<(DatasetId, Vec<RowKey>)>, IncrementalPreparationError>,
}

/// Project update identity, causal metadata, and affected row keys from a temporary view.
fn project_update_row_keys(view: &ReplicationUpdateView<'_>) -> ProjectedUpdate {
    let row_keys_result = decode_update_row_keys(view);
    ProjectedUpdate {
        group_id: view.group_id(),
        update_id: view.update_id(),
        read_versions: view.read_versions().clone(),
        applied_locally: view.applied_locally(),
        row_keys_result,
    }
}

/// Decode affected row identities without owning stored operation payloads.
fn decode_update_row_keys(
    view: &ReplicationUpdateView<'_>,
) -> Result<Vec<(DatasetId, Vec<RowKey>)>, IncrementalPreparationError> {
    let mut datasets = Vec::new();
    for dataset_update in view.dataset_updates() {
        let raw_dataset_id = dataset_update.dataset_id();
        let dataset_id =
            DatasetId::try_from_owned(raw_dataset_id.to_owned()).with_context(|_| {
                incremental_preparation_error::InvalidProjectedDatasetIdSnafu {
                    group: view.group_id(),
                    update: view.update_id(),
                    dataset: raw_dataset_id.to_owned(),
                }
            })?;
        let mut row_keys = Vec::with_capacity(dataset_update.operations().len());
        for operation in dataset_update.operations() {
            let row_id = decode_schema_operation_view_row_id(operation).with_context(|_| {
                incremental_preparation_error::DecodeOperationSnafu {
                    group: view.group_id(),
                    dataset: dataset_id.clone(),
                    update: view.update_id(),
                }
            })?;
            row_keys.push(RowKey(row_id));
        }
        datasets.push((dataset_id, row_keys));
    }
    Ok(datasets)
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

/// Application effect projected from one affected current stored row.
enum CurrentRowProjection {
    /// Emit this upsert or delete.
    Emit(RowChange),
    /// Emit nothing because the row was both created and deleted later.
    Omit,
    /// Fall back because safe delete projection needs unavailable provenance.
    SnapshotRequired,
}
