//! Store-backed row streaming for one committed group activation.

use super::*;
use crate::api::{
    AcceptedCutRelation,
    DatasetRowPageBatch,
    DatasetRowTransitionPageBatch,
    DatasetRowTransitionQuery,
    DatasetRowsQuery,
    InMemoryStateRowView,
    PageCursor,
    PreviousRow,
    PreviousRowCreator,
    PreviousRowEvidence,
    ReplicationRowMetadata,
    ReplicationStateRowTransitionView,
    RowChangeBatch,
    RowChangeKind,
    RowFieldDifference,
    RowValues,
    SchemaSource,
};
use flotsync_data_types::schema::Schema;
use flotsync_utils::coerce_infallible;
use std::{borrow::Cow, cmp::Ordering, convert::Infallible};

/// Row provider for one committed activation event.
pub(super) struct StoreActivationRowProvider {
    /// Store view pinned after the activation transaction commits.
    transaction: Option<Box<dyn ReplicationStoreReadTransaction>>,
    /// Lineage-specific row projection represented by this provider.
    source: ActivationRowSource,
    /// Deterministically ordered schemas scanned from the activated group.
    datasets: Vec<crate::api::DatasetSchema>,
    /// Index of the dataset currently being scanned.
    dataset_index: usize,
    /// Cursor for the dataset currently being scanned.
    dataset_cursor: Option<ActivationDatasetCursor>,
    /// Reusable storage for ordinary row scans.
    state_rows: DatasetRowPageBatch,
    /// Reusable storage for old-to-new row transition scans.
    transition_rows: DatasetRowTransitionPageBatch,
}

impl StoreActivationRowProvider {
    /// Build a provider for ordinary rows introduced by a creation activation.
    pub(super) fn for_creation(
        transaction: Box<dyn ReplicationStoreReadTransaction>,
        group_id: GroupId,
        group_schema: &GroupSchema,
    ) -> Self {
        Self {
            transaction: Some(transaction),
            source: ActivationRowSource::Creation { group_id },
            datasets: group_schema.datasets(),
            dataset_index: 0,
            dataset_cursor: None,
            state_rows: DatasetRowPageBatch::bounded(&Schema::empty(), ACTIVATION_ROWS_PER_BATCH),
            transition_rows: DatasetRowTransitionPageBatch::bounded(
                &Schema::empty(),
                &Schema::empty(),
                ACTIVATION_ROWS_PER_BATCH,
            ),
        }
    }

    /// Build a provider which can compare a locally hosted predecessor.
    pub(super) fn hosted_replacement(
        transaction: Box<dyn ReplicationStoreReadTransaction>,
        migration_id: MigrationId,
        group_schema: &GroupSchema,
        local_member_index: MemberIndex,
        final_versions: VersionVector,
    ) -> Self {
        Self {
            transaction: Some(transaction),
            source: ActivationRowSource::GroupReplacement {
                migration_id,
                predecessor: ReplacementPredecessor::Hosted {
                    local_member_index,
                    final_versions,
                },
            },
            datasets: group_schema.datasets(),
            dataset_index: 0,
            dataset_cursor: None,
            state_rows: DatasetRowPageBatch::bounded(&Schema::empty(), ACTIVATION_ROWS_PER_BATCH),
            transition_rows: DatasetRowTransitionPageBatch::bounded(
                &Schema::empty(),
                &Schema::empty(),
                ACTIVATION_ROWS_PER_BATCH,
            ),
        }
    }

    /// Build a provider for a migration invitation whose predecessor is not hosted locally.
    pub(super) fn unavailable_replacement(
        transaction: Box<dyn ReplicationStoreReadTransaction>,
        migration_id: MigrationId,
        group_schema: &GroupSchema,
    ) -> Self {
        Self {
            transaction: Some(transaction),
            source: ActivationRowSource::GroupReplacement {
                migration_id,
                predecessor: ReplacementPredecessor::Unavailable,
            },
            datasets: group_schema.datasets(),
            dataset_index: 0,
            dataset_cursor: None,
            state_rows: DatasetRowPageBatch::bounded(&Schema::empty(), ACTIVATION_ROWS_PER_BATCH),
            transition_rows: DatasetRowTransitionPageBatch::bounded(
                &Schema::empty(),
                &Schema::empty(),
                ACTIVATION_ROWS_PER_BATCH,
            ),
        }
    }
}

impl StoreActivationRowProvider {
    /// Release the pinned read transaction after the provider is exhausted.
    async fn release_transaction(&mut self) -> Result<(), RowProviderError> {
        let Some(transaction) = self.transaction.take() else {
            return Ok(());
        };
        ReplicationStoreReadTransaction::release(transaction)
            .await
            .map_err(RowProviderError::from_store_error)
    }

    /// Return the current dataset schema, if another dataset remains.
    fn current_dataset(&self) -> Option<&crate::api::DatasetSchema> {
        self.datasets.get(self.dataset_index)
    }

    /// Advance to the next dataset and discard its exhausted cursor.
    fn finish_current_dataset(&mut self) {
        self.dataset_index += 1;
        self.dataset_cursor = None;
    }

    /// Load and translate one stored page for the current dataset.
    async fn fill_current_dataset(
        &mut self,
        output: &mut RowChangeBatch,
    ) -> Result<(), RowProviderError> {
        let dataset_index = self.dataset_index;
        if self.dataset_cursor.is_none() {
            let dataset_schema = self
                .datasets
                .get(dataset_index)
                .expect("activation provider must have a current dataset");
            self.dataset_cursor = Some(ActivationDatasetCursor::new(
                &self.source,
                dataset_schema.clone(),
            ));
        }
        let has_more = {
            let dataset_schema = self
                .datasets
                .get(dataset_index)
                .expect("activation provider must have a current dataset");
            let transaction = self
                .transaction
                .as_mut()
                .expect("activation provider must retain its transaction while datasets remain");

            match &self.source {
                ActivationRowSource::Creation { group_id } => {
                    let ActivationDatasetCursor::Rows(cursor) = self
                        .dataset_cursor
                        .as_mut()
                        .expect("activation provider must initialise its dataset cursor")
                    else {
                        panic!("creation activation requires an ordinary row cursor");
                    };
                    fill_creation_dataset(
                        transaction.as_mut(),
                        output,
                        *group_id,
                        dataset_schema,
                        cursor,
                        &mut self.state_rows,
                    )
                    .await?;
                    cursor.has_more()
                }
                ActivationRowSource::GroupReplacement {
                    migration_id,
                    predecessor:
                        ReplacementPredecessor::Hosted {
                            local_member_index,
                            final_versions,
                        },
                } => {
                    let ActivationDatasetCursor::Transitions(cursor) = self
                        .dataset_cursor
                        .as_mut()
                        .expect("activation provider must initialise its dataset cursor")
                    else {
                        panic!("hosted replacement requires a transition cursor");
                    };
                    fill_hosted_replacement_dataset(
                        transaction.as_mut(),
                        output,
                        HostedReplacementDatasetScan {
                            migration_id: *migration_id,
                            dataset_schema,
                            local_member_index: *local_member_index,
                            final_versions,
                        },
                        cursor,
                        &mut self.transition_rows,
                    )
                    .await?;
                    cursor.has_more()
                }
                ActivationRowSource::GroupReplacement {
                    migration_id,
                    predecessor: ReplacementPredecessor::Unavailable,
                } => {
                    let ActivationDatasetCursor::Rows(cursor) = self
                        .dataset_cursor
                        .as_mut()
                        .expect("activation provider must initialise its dataset cursor")
                    else {
                        panic!("unavailable replacement requires an ordinary row cursor");
                    };
                    fill_unavailable_replacement_dataset(
                        transaction.as_mut(),
                        output,
                        migration_id.new_group_id,
                        dataset_schema,
                        cursor,
                        &mut self.state_rows,
                    )
                    .await?;
                    cursor.has_more()
                }
            }
        };

        if !has_more {
            self.finish_current_dataset();
        }
        Ok(())
    }
}

impl BatchProvider for StoreActivationRowProvider {
    type Batch = RowChangeBatch;

    fn new_batch(&self) -> Self::Batch {
        RowChangeBatch::new()
    }

    fn fill_batch(
        &mut self,
        mut reuse: Self::Batch,
    ) -> BoxFuture<'_, Result<Option<Self::Batch>, RowProviderError>> {
        async move {
            reuse.clear();
            while reuse.is_empty() {
                if self.current_dataset().is_none() {
                    self.release_transaction().await?;
                    return Ok(None);
                }
                self.fill_current_dataset(&mut reuse).await?;
            }
            Ok(Some(reuse))
        }
        .boxed()
    }
}

/// Number of stored activation rows requested for one listener batch.
const ACTIVATION_ROWS_PER_BATCH: NonZeroUsize =
    NonZeroUsize::new(128).expect("activation batch size must be non-zero");

/// Load one ordinary committed activation page.
async fn fill_creation_dataset(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    output: &mut RowChangeBatch,
    group_id: GroupId,
    dataset_schema: &crate::api::DatasetSchema,
    cursor: &mut PageCursor<DatasetRowsQuery<'static>>,
    batch: &mut DatasetRowPageBatch,
) -> Result<(), RowProviderError> {
    transaction
        .scan_dataset_rows_into(cursor, batch)
        .await
        .map_err(|source| RowProviderError::from_store_error(source.into()))?;
    for current in batch.rows().rows() {
        append_creation_row(
            output,
            group_id,
            &dataset_schema.dataset_id,
            dataset_schema.schema.as_schema(),
            &current,
        )?;
    }
    Ok(())
}

/// Inputs which identify one page of a hosted old-to-new dataset comparison.
struct HostedReplacementDatasetScan<'a> {
    /// Explicit old-to-new group relationship.
    migration_id: MigrationId,
    /// Dataset and schema shared by the current activation projection.
    dataset_schema: &'a crate::api::DatasetSchema,
    /// Local member position in the previous group.
    local_member_index: MemberIndex,
    /// Accepted previous-group frontier.
    final_versions: &'a VersionVector,
}

/// Load one committed replacement page with a locally hosted predecessor.
async fn fill_hosted_replacement_dataset(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    output: &mut RowChangeBatch,
    scan: HostedReplacementDatasetScan<'_>,
    cursor: &mut PageCursor<DatasetRowTransitionQuery<'static>>,
    batch: &mut DatasetRowTransitionPageBatch,
) -> Result<(), RowProviderError> {
    let HostedReplacementDatasetScan {
        migration_id,
        dataset_schema,
        local_member_index,
        final_versions,
    } = scan;
    transaction
        .scan_dataset_row_transitions_into(cursor, batch)
        .await
        .map_err(|source| RowProviderError::from_store_error(source.into()))?;
    for transition in batch.transitions().rows() {
        append_hosted_transition(
            output,
            migration_id,
            &dataset_schema.dataset_id,
            &dataset_schema.schema,
            local_member_index,
            final_versions,
            transition,
        )?;
    }
    Ok(())
}

/// Load one committed replacement page whose predecessor is unavailable locally.
async fn fill_unavailable_replacement_dataset(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    output: &mut RowChangeBatch,
    group_id: GroupId,
    dataset_schema: &crate::api::DatasetSchema,
    cursor: &mut PageCursor<DatasetRowsQuery<'static>>,
    batch: &mut DatasetRowPageBatch,
) -> Result<(), RowProviderError> {
    let schema = dataset_schema.schema.as_schema();
    transaction
        .scan_dataset_rows_into(cursor, batch)
        .await
        .map_err(|source| RowProviderError::from_store_error(source.into()))?;
    for current in batch.rows().rows() {
        append_unavailable_current(
            output,
            group_id,
            &dataset_schema.dataset_id,
            schema,
            &current,
        )?;
    }
    Ok(())
}

/// Translate one raw hosted row transition into an application-visible operation.
fn append_hosted_transition(
    output: &mut RowChangeBatch,
    migration_id: MigrationId,
    dataset_id: &DatasetId,
    schema_source: &SchemaSource,
    local_member_index: MemberIndex,
    final_versions: &VersionVector,
    transition: ReplicationStateRowTransitionView<'_>,
) -> Result<(), RowProviderError> {
    let previous_visible = transition
        .previous()
        .is_some_and(|row| !row.metadata().tombstoned);
    let current_visible = transition
        .current()
        .is_some_and(|row| !row.metadata().tombstoned);

    match (previous_visible, current_visible) {
        (true, true) => append_changed_row(
            output,
            migration_id,
            dataset_id,
            schema_source,
            local_member_index,
            final_versions,
            transition,
        ),
        (true, false) => append_removed_row(
            output,
            migration_id.old_group_id,
            dataset_id,
            local_member_index,
            final_versions,
            transition,
        )
        .map_err(coerce_infallible),
        (false, true) => append_new_row(
            output,
            migration_id,
            dataset_id,
            schema_source.as_schema(),
            local_member_index,
            final_versions,
            transition,
        ),
        (false, false) => Ok(()),
    }
}

/// Emit one upsert whose old-group and successor values can be compared.
fn append_changed_row(
    output: &mut RowChangeBatch,
    migration_id: MigrationId,
    dataset_id: &DatasetId,
    schema_source: &SchemaSource,
    local_member_index: MemberIndex,
    final_versions: &VersionVector,
    transition: ReplicationStateRowTransitionView<'_>,
) -> Result<(), RowProviderError> {
    let previous = transition
        .previous()
        .expect("visible predecessor must contain a stored record");
    let current = transition
        .current()
        .expect("visible successor must contain a stored record");
    let previous_row_id = RowId::new(
        migration_id.old_group_id,
        dataset_id.clone(),
        transition.row_key(),
    );
    let current_row_id = RowId::new(
        migration_id.new_group_id,
        dataset_id.clone(),
        transition.row_key(),
    );
    let evidence = previous_row_evidence(previous.metadata(), local_member_index, final_versions);
    let schema = schema_source.as_schema();
    let previous_values = RowValues::from_row(schema, &previous)
        .boxed()
        .context(ProviderExternalSnafu)?;
    let current_values = RowValues::from_row(schema, &current)
        .boxed()
        .context(ProviderExternalSnafu)?;
    let differences = changed_value_fields(schema_source, &previous_values, &current_values);
    output.push(RowChange {
        previous: PreviousRow::Present {
            row_id: previous_row_id,
            evidence,
        },
        change: RowChangeKind::Upsert {
            row_id: current_row_id,
            row: Arc::new(current_values),
            previous_value_differences: Some(differences),
        },
    });
    Ok(())
}

/// Emit one delete for a visible old-group row missing from the successor.
#[allow(
    clippy::unnecessary_wraps,
    reason = "The result shape keeps all visibility branches uniform and uses the shared infallible error adapter."
)]
fn append_removed_row(
    output: &mut RowChangeBatch,
    previous_group_id: GroupId,
    dataset_id: &DatasetId,
    local_member_index: MemberIndex,
    final_versions: &VersionVector,
    transition: ReplicationStateRowTransitionView<'_>,
) -> Result<(), Infallible> {
    let previous = transition
        .previous()
        .expect("visible predecessor must contain a stored record");
    let previous_row_id = RowId::new(previous_group_id, dataset_id.clone(), transition.row_key());
    let evidence = previous_row_evidence(previous.metadata(), local_member_index, final_versions);
    output.push(RowChange {
        previous: PreviousRow::Present {
            row_id: previous_row_id.clone(),
            evidence,
        },
        change: RowChangeKind::Delete {
            row_id: previous_row_id,
        },
    });
    Ok(())
}

/// Emit one upsert for a successor row without a visible old-group row.
fn append_new_row(
    output: &mut RowChangeBatch,
    migration_id: MigrationId,
    dataset_id: &DatasetId,
    schema: &flotsync_data_types::schema::Schema,
    local_member_index: MemberIndex,
    final_versions: &VersionVector,
    transition: ReplicationStateRowTransitionView<'_>,
) -> Result<(), RowProviderError> {
    let previous = previous_absence(
        migration_id.old_group_id,
        dataset_id,
        transition.row_key(),
        transition.previous(),
        local_member_index,
        final_versions,
    );
    let current = transition
        .current()
        .expect("visible successor must contain a stored record");
    let current_row_id = RowId::new(
        migration_id.new_group_id,
        dataset_id.clone(),
        transition.row_key(),
    );
    let current_values = RowValues::from_row(schema, &current)
        .boxed()
        .context(ProviderExternalSnafu)?;
    output.push(RowChange {
        previous,
        change: RowChangeKind::Upsert {
            row_id: current_row_id,
            row: Arc::new(current_values),
            previous_value_differences: None,
        },
    });
    Ok(())
}

/// Emit one visible row from a creation-sourced activation.
fn append_creation_row(
    output: &mut RowChangeBatch,
    group_id: GroupId,
    dataset_id: &DatasetId,
    schema: &flotsync_data_types::schema::Schema,
    current: &InMemoryStateRowView<'_, ReplicationRowMetadata, UpdateId>,
) -> Result<(), RowProviderError> {
    let metadata = current.metadata();
    if metadata.tombstoned {
        return Ok(());
    }
    let row_id = RowId::new(group_id, dataset_id.clone(), metadata.row_key);
    let row = RowValues::from_row(schema, current)
        .boxed()
        .context(ProviderExternalSnafu)?;
    output.push(RowChange::ordinary_upsert(row_id, Arc::new(row)));
    Ok(())
}

/// Emit one visible successor row when the predecessor group is unavailable.
fn append_unavailable_current(
    output: &mut RowChangeBatch,
    current_group_id: GroupId,
    dataset_id: &DatasetId,
    schema: &flotsync_data_types::schema::Schema,
    current: &InMemoryStateRowView<'_, ReplicationRowMetadata, UpdateId>,
) -> Result<(), RowProviderError> {
    let metadata = current.metadata();
    if metadata.tombstoned {
        return Ok(());
    }
    let current_row_id = RowId::new(current_group_id, dataset_id.clone(), metadata.row_key);
    let current_values = RowValues::from_row(schema, current)
        .boxed()
        .context(ProviderExternalSnafu)?;
    output.push(RowChange {
        previous: PreviousRow::Unavailable,
        change: RowChangeKind::Upsert {
            row_id: current_row_id,
            row: Arc::new(current_values),
            previous_value_differences: None,
        },
    });
    Ok(())
}

/// Build previous-row absence metadata from an optional raw stored record.
fn previous_absence(
    previous_group_id: GroupId,
    dataset_id: &DatasetId,
    row_key: RowKey,
    previous: Option<InMemoryStateRowView<'_, ReplicationRowMetadata, UpdateId>>,
    local_member_index: MemberIndex,
    final_versions: &VersionVector,
) -> PreviousRow {
    previous.map_or(PreviousRow::NOT_STORED, |previous| {
        debug_assert!(previous.metadata().tombstoned);
        let row_id = RowId::new(previous_group_id, dataset_id.clone(), row_key);
        let evidence =
            previous_row_evidence(previous.metadata(), local_member_index, final_versions);
        PreviousRow::tombstoned(row_id, evidence)
    })
}

/// Derive the strongest conservative provenance retained for one predecessor record.
fn previous_row_evidence(
    previous: &ReplicationRowMetadata,
    local_member_index: MemberIndex,
    final_versions: &VersionVector,
) -> PreviousRowEvidence {
    let creator = previous
        .created_by
        .filter(|created_by| *created_by != UpdateId::INITIAL_STATE_ORIGIN)
        .map(|created_by| match created_by {
            UpdateId { node_index, .. } if node_index == local_member_index.as_u32() => {
                PreviousRowCreator::Local
            }
            UpdateId { .. } => PreviousRowCreator::Other,
        });
    let creation = previous
        .created_by
        .and_then(|created_by| update_cut_relation(created_by, final_versions));
    let last_state = version_cut_relation(&previous.last_changed_versions, final_versions);
    PreviousRowEvidence {
        creator,
        creation,
        last_state,
    }
}

/// Compare one concrete update id with the matching producer entry in the cut.
fn update_cut_relation(
    update_id: UpdateId,
    final_versions: &VersionVector,
) -> Option<AcceptedCutRelation> {
    let producer_index = update_id.node_index as usize;
    if producer_index >= final_versions.num_members().get() {
        return None;
    }
    if update_id.version <= final_versions.version_at(producer_index) {
        Some(AcceptedCutRelation::Included)
    } else {
        Some(AcceptedCutRelation::NotIncluded)
    }
}

/// Compare a retained row-state frontier with the accepted cut.
fn version_cut_relation(
    row_versions: &VersionVector,
    final_versions: &VersionVector,
) -> Option<AcceptedCutRelation> {
    row_versions
        .partial_cmp(final_versions)
        .map(|ordering| match ordering {
            Ordering::Less | Ordering::Equal => AcceptedCutRelation::Included,
            Ordering::Greater => AcceptedCutRelation::NotIncluded,
        })
}

/// Return changed field values in canonical field-name order.
fn changed_value_fields(
    schema_source: &SchemaSource,
    previous: &RowValues,
    current: &RowValues,
) -> Box<[RowFieldDifference]> {
    let mut differences = current
        .differing_field_names(previous)
        .map(|field_name| {
            let field_name = match schema_source {
                SchemaSource::Static(schema) => {
                    let (field_name, _field) = schema
                        .columns
                        .get_key_value(field_name)
                        .expect("row field must exist in its static schema");
                    Cow::Borrowed(field_name)
                }
                SchemaSource::Shared(_) => Cow::Owned(field_name.to_owned()),
            };
            RowFieldDifference::ValueChanged { field_name }
        })
        .collect::<Vec<_>>();
    differences.sort_by(|left, right| left.field_name().cmp(right.field_name()));
    differences.into_boxed_slice()
}

/// Retained page cursor selected by one activation lineage.
enum ActivationDatasetCursor {
    /// Ordinary rows from one dataset occurrence.
    Rows(PageCursor<DatasetRowsQuery<'static>>),
    /// Aligned old-to-new row transitions.
    Transitions(PageCursor<DatasetRowTransitionQuery<'static>>),
}

impl ActivationDatasetCursor {
    /// Build the cursor required by `source` for one owned dataset schema.
    fn new(source: &ActivationRowSource, dataset: crate::api::DatasetSchema) -> Self {
        match source {
            ActivationRowSource::Creation { group_id } => {
                Self::Rows(PageCursor::new(DatasetRowsQuery::owned(*group_id, dataset)))
            }
            ActivationRowSource::GroupReplacement {
                migration_id,
                predecessor: ReplacementPredecessor::Hosted { .. },
            } => Self::Transitions(PageCursor::new(DatasetRowTransitionQuery::new(
                DatasetRowsQuery::owned(migration_id.old_group_id, dataset.clone()),
                DatasetRowsQuery::owned(migration_id.new_group_id, dataset),
            ))),
            ActivationRowSource::GroupReplacement {
                migration_id,
                predecessor: ReplacementPredecessor::Unavailable,
            } => Self::Rows(PageCursor::new(DatasetRowsQuery::owned(
                migration_id.new_group_id,
                dataset,
            ))),
        }
    }
}

/// Store projection used to build one activation event.
enum ActivationRowSource {
    /// Ordinary rows introduced by a creation-sourced activation.
    Creation { group_id: GroupId },
    /// Old-to-new rows introduced by a group replacement.
    GroupReplacement {
        /// Explicit old-to-new group relationship.
        migration_id: MigrationId,
        /// Predecessor context available to the local runtime.
        predecessor: ReplacementPredecessor,
    },
}

/// Locally available evidence used to interpret predecessor rows.
enum ReplacementPredecessor {
    /// The old group and accepted final cut are available in local storage.
    Hosted {
        /// Old-group member index occupied by the local application member.
        local_member_index: MemberIndex,
        /// Accepted old-group version frontier used to build the successor.
        final_versions: VersionVector,
    },
    /// The local member joined only in the successor group.
    Unavailable,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        api::{
            DatasetRowPageMetadata,
            PreviousRowAbsence,
            ReplicationStateRowBatch,
            ReplicationStateRowTransitionBatch,
            SchemaSource,
            StoreErrorClassification,
            StoreErrorClassificationSource as _,
        },
        runtime::{
            in_memory::LocalDataset,
            tests::{
                DatasetRowStateTransitionFixture,
                ProviderTestRowScanResult,
                ProviderTestScanRequest,
                ReplicationRowStateFixture,
                provider_test_read_transaction,
            },
        },
        test_support::wait_for_test_future,
    };
    use flotsync_core::versions::PureVersionVector;
    use flotsync_data_types::Field;
    use std::sync::LazyLock;
    use uuid::Uuid;

    /// Fixed old-to-new migration id for provider translation tests.
    const TEST_MIGRATION_ID: MigrationId = MigrationId {
        old_group_id: GroupId(Uuid::from_u128(1)),
        new_group_id: GroupId(Uuid::from_u128(2)),
    };

    /// Single-field schema used by provider translation tests.
    static TITLE_SCHEMA: LazyLock<Schema> =
        LazyLock::new(|| Schema::from_fields([Field::linear_string("title")]));

    /// Build one stored row image with explicit provenance.
    fn stored_row(
        row_key: u128,
        title: &str,
        tombstoned: bool,
        created_by: Option<UpdateId>,
        last_changed_versions: VersionVector,
    ) -> ReplicationRowStateFixture {
        let row_values = RowValues::try_from_fields(
            &TITLE_SCHEMA,
            HashMap::from([("title".to_owned(), title.into())]),
        )
        .expect("test row values must match the title schema");
        let row_key = RowKey(Uuid::from_u128(row_key));
        let data = flotsync_messages::InMemoryStateData::from_initial_value_rows(
            Arc::new(TITLE_SCHEMA.clone()),
            [(row_key.0, row_values)],
            &UpdateId::INITIAL_STATE_ORIGIN,
        )
        .expect("test row state must embed");
        let snapshot = LocalDataset { data }
            .snapshot_row(row_key)
            .expect("embedded test row must be snapshotable");
        ReplicationRowStateFixture {
            row_id: row_key,
            snapshot,
            tombstoned,
            created_by,
            last_changed_versions,
        }
    }

    /// Copy the provenance columns from one owned row fixture.
    fn stored_row_metadata(row: &ReplicationRowStateFixture) -> ReplicationRowMetadata {
        ReplicationRowMetadata {
            row_key: row.row_id,
            tombstoned: row.tombstoned,
            created_by: row.created_by,
            last_changed_versions: row.last_changed_versions.clone(),
        }
    }

    /// Build a deterministic multi-dataset schema for provider traversal tests.
    fn group_schema(dataset_ids: impl IntoIterator<Item = &'static str>) -> GroupSchema {
        GroupSchema::new(
            dataset_ids
                .into_iter()
                .map(|dataset_id| {
                    let dataset_id = DatasetId::try_from_static(dataset_id)
                        .expect("test dataset id must be valid");
                    let schema = SchemaSource::Static(&TITLE_SCHEMA);
                    (dataset_id, schema)
                })
                .collect(),
        )
    }

    /// Build one unavailable-predecessor scan result.
    fn row_batch(
        dataset_id: &'static str,
        rows: Vec<ReplicationRowStateFixture>,
        next_after: Option<RowKey>,
    ) -> ProviderTestRowScanResult {
        ProviderTestRowScanResult {
            metadata: DatasetRowPageMetadata {
                group_id: TEST_MIGRATION_ID.new_group_id,
                dataset_id: DatasetId::try_from_static(dataset_id)
                    .expect("test dataset id must be valid"),
                dataset_exists: true,
            },
            rows,
            next_after,
        }
    }

    /// Decode stored-row fixtures into the same state batch used by ordinary scans.
    fn decoded_state_rows(rows: Vec<ReplicationRowStateFixture>) -> ReplicationStateRowBatch {
        let mut state_rows = ReplicationStateRowBatch::new(&TITLE_SCHEMA);
        state_rows.reserve_rows(rows.len());
        for row in rows {
            push_decoded_state_row(&mut state_rows, row);
        }
        state_rows
    }

    /// Decode one owned fixture and return its index in `state_rows`.
    fn push_decoded_state_row(
        state_rows: &mut ReplicationStateRowBatch,
        row: ReplicationRowStateFixture,
    ) -> usize {
        let encoded =
            flotsync_messages::codecs::datamodel::encode_row_snapshot(&row.snapshot, &TITLE_SCHEMA)
                .expect("test row must encode against the title schema");
        let mut decoder =
            flotsync_messages::snapshots::datamodel::ProtoSchemaSnapshotDecoder::new(encoded)
                .expect("test row must create a snapshot decoder");
        let row_index = state_rows.len();
        state_rows
            .push_decoded_row(
                ReplicationRowMetadata {
                    row_key: row.row_id,
                    tombstoned: row.tombstoned,
                    created_by: row.created_by,
                    last_changed_versions: row.last_changed_versions,
                },
                &mut decoder,
            )
            .expect("test row must decode into the state batch");
        row_index
    }

    /// Convert one owned transition fixture before exercising translation.
    fn append_test_hosted_transition(
        output: &mut RowChangeBatch,
        migration_id: MigrationId,
        dataset_id: &DatasetId,
        schema_source: &SchemaSource,
        local_member_index: MemberIndex,
        final_versions: &VersionVector,
        transition: DatasetRowStateTransitionFixture,
    ) -> Result<(), RowProviderError> {
        let mut transition_rows =
            ReplicationStateRowTransitionBatch::new(&TITLE_SCHEMA, &TITLE_SCHEMA);
        let previous_index = transition
            .previous
            .map(|row| push_decoded_state_row(transition_rows.previous_rows_mut(), row));
        let current_index = transition
            .current
            .map(|row| push_decoded_state_row(transition_rows.current_rows_mut(), row));
        transition_rows.push_alignment(previous_index, current_index);
        let transition = transition_rows
            .row(0)
            .expect("test transition must be present");
        append_hosted_transition(
            output,
            migration_id,
            dataset_id,
            schema_source,
            local_member_index,
            final_versions,
            transition,
        )
    }

    /// Build one test store error for provider propagation checks.
    fn test_store_error() -> StoreError {
        StoreError::new(
            StoreErrorClassification::UNKNOWN,
            std::io::Error::other("injected activation scan failure"),
        )
    }

    #[test]
    fn creation_provider_skips_invisible_pages_advances_datasets_and_releases_after_drain() {
        let versions =
            VersionVector::initial(NonZeroUsize::new(2).expect("test group must have members"));
        let first_key = RowKey(Uuid::from_u128(1));
        let (transaction, state) = provider_test_read_transaction(
            [
                Ok(row_batch(
                    "alpha",
                    vec![stored_row(
                        1,
                        "hidden",
                        true,
                        Some(UpdateId::INITIAL_STATE_ORIGIN),
                        versions.clone(),
                    )],
                    Some(first_key),
                )),
                Ok(row_batch(
                    "alpha",
                    vec![stored_row(
                        2,
                        "alpha visible",
                        false,
                        Some(UpdateId::INITIAL_STATE_ORIGIN),
                        versions.clone(),
                    )],
                    None,
                )),
                Ok(row_batch(
                    "beta",
                    vec![stored_row(
                        3,
                        "beta visible",
                        false,
                        Some(UpdateId::INITIAL_STATE_ORIGIN),
                        versions,
                    )],
                    None,
                )),
            ],
            [],
        );
        let mut provider = StoreActivationRowProvider {
            transaction: Some(transaction),
            source: ActivationRowSource::Creation {
                group_id: TEST_MIGRATION_ID.new_group_id,
            },
            datasets: group_schema(["beta", "alpha"]).datasets(),
            dataset_index: 0,
            dataset_cursor: None,
            state_rows: DatasetRowPageBatch::bounded(&Schema::empty(), ACTIVATION_ROWS_PER_BATCH),
            transition_rows: DatasetRowTransitionPageBatch::bounded(
                &Schema::empty(),
                &Schema::empty(),
                ACTIVATION_ROWS_PER_BATCH,
            ),
        };

        let first = wait_for_test_future(provider.fill_batch(RowChangeBatch::new()))
            .expect("first provider batch must load")
            .expect("alpha must emit one visible row");
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].row_id().dataset_id.as_str(), "alpha");
        assert_eq!(first[0].previous, PreviousRow::NotCompared);
        let second = wait_for_test_future(provider.fill_batch(first))
            .expect("second provider batch must load")
            .expect("beta must emit one visible row");
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].row_id().dataset_id.as_str(), "beta");
        assert!(
            wait_for_test_future(provider.fill_batch(second))
                .expect("provider exhaustion must release cleanly")
                .is_none()
        );

        let state = state
            .lock()
            .expect("test transaction state mutex must not be poisoned");
        assert_eq!(state.release_count, 1);
        assert_eq!(state.drop_count, 1);
        assert_eq!(
            state.row_requests,
            [
                ProviderTestScanRequest {
                    dataset_id: DatasetId::try_from_static("alpha")
                        .expect("test dataset id must be valid"),
                    after: None,
                    limit: ACTIVATION_ROWS_PER_BATCH,
                },
                ProviderTestScanRequest {
                    dataset_id: DatasetId::try_from_static("alpha")
                        .expect("test dataset id must be valid"),
                    after: Some(first_key),
                    limit: ACTIVATION_ROWS_PER_BATCH,
                },
                ProviderTestScanRequest {
                    dataset_id: DatasetId::try_from_static("beta")
                        .expect("test dataset id must be valid"),
                    after: None,
                    limit: ACTIVATION_ROWS_PER_BATCH,
                },
            ]
        );
    }

    #[test]
    fn creation_provider_propagates_later_page_failure_and_drops_transaction() {
        let versions =
            VersionVector::initial(NonZeroUsize::new(2).expect("test group must have members"));
        let first_key = RowKey(Uuid::from_u128(1));
        let (transaction, state) = provider_test_read_transaction(
            [
                Ok(row_batch(
                    "docs",
                    vec![stored_row(
                        1,
                        "hidden",
                        true,
                        Some(UpdateId::INITIAL_STATE_ORIGIN),
                        versions,
                    )],
                    Some(first_key),
                )),
                Err(test_store_error()),
            ],
            [],
        );
        let mut provider = StoreActivationRowProvider {
            transaction: Some(transaction),
            source: ActivationRowSource::Creation {
                group_id: TEST_MIGRATION_ID.new_group_id,
            },
            datasets: group_schema(["docs"]).datasets(),
            dataset_index: 0,
            dataset_cursor: None,
            state_rows: DatasetRowPageBatch::bounded(&Schema::empty(), ACTIVATION_ROWS_PER_BATCH),
            transition_rows: DatasetRowTransitionPageBatch::bounded(
                &Schema::empty(),
                &Schema::empty(),
                ACTIVATION_ROWS_PER_BATCH,
            ),
        };

        let result = wait_for_test_future(provider.fill_batch(RowChangeBatch::new()));
        let Err(error) = result else {
            panic!("row scan failure must reach the listener provider");
        };
        assert_eq!(
            error.store_error_classification(),
            Some(StoreErrorClassification::UNKNOWN)
        );
        assert!(matches!(error, RowProviderError::Store { .. }));
        drop(provider);

        let state = state
            .lock()
            .expect("test transaction state mutex must not be poisoned");
        assert_eq!(state.release_count, 0);
        assert_eq!(state.drop_count, 1);
        assert_eq!(state.row_requests.len(), 2);
        assert_eq!(state.row_requests[0].after, None);
        assert_eq!(state.row_requests[1].after, Some(first_key));
    }

    #[test]
    #[should_panic(
        expected = "activation provider must retain its transaction while datasets remain"
    )]
    fn provider_panics_if_its_transaction_is_missing_before_exhaustion() {
        let mut provider = StoreActivationRowProvider {
            transaction: None,
            source: ActivationRowSource::Creation {
                group_id: TEST_MIGRATION_ID.new_group_id,
            },
            datasets: group_schema(["docs"]).datasets(),
            dataset_index: 0,
            dataset_cursor: None,
            state_rows: DatasetRowPageBatch::bounded(&Schema::empty(), ACTIVATION_ROWS_PER_BATCH),
            transition_rows: DatasetRowTransitionPageBatch::bounded(
                &Schema::empty(),
                &Schema::empty(),
                ACTIVATION_ROWS_PER_BATCH,
            ),
        };

        let _batch = wait_for_test_future(provider.fill_batch(RowChangeBatch::new()));
    }

    #[test]
    fn provider_propagates_transition_scan_failure_and_drops_transaction() {
        let (transaction, state) = provider_test_read_transaction([], [Err(test_store_error())]);
        let mut provider = StoreActivationRowProvider {
            transaction: Some(transaction),
            source: ActivationRowSource::GroupReplacement {
                migration_id: TEST_MIGRATION_ID,
                predecessor: ReplacementPredecessor::Hosted {
                    local_member_index: MemberIndex::new(0),
                    final_versions: VersionVector::initial(
                        NonZeroUsize::new(2).expect("test group must have members"),
                    ),
                },
            },
            datasets: group_schema(["docs"]).datasets(),
            dataset_index: 0,
            dataset_cursor: None,
            state_rows: DatasetRowPageBatch::bounded(&Schema::empty(), ACTIVATION_ROWS_PER_BATCH),
            transition_rows: DatasetRowTransitionPageBatch::bounded(
                &Schema::empty(),
                &Schema::empty(),
                ACTIVATION_ROWS_PER_BATCH,
            ),
        };

        let result = wait_for_test_future(provider.fill_batch(RowChangeBatch::new()));
        let Err(error) = result else {
            panic!("transition scan failure must reach the listener provider");
        };
        assert_eq!(
            error.store_error_classification(),
            Some(StoreErrorClassification::UNKNOWN)
        );
        assert!(matches!(error, RowProviderError::Store { .. }));
        drop(provider);

        let state = state
            .lock()
            .expect("test transaction state mutex must not be poisoned");
        assert_eq!(state.release_count, 0);
        assert_eq!(state.drop_count, 1);
        assert_eq!(state.transition_requests.len(), 1);
    }

    #[test]
    fn empty_provider_releases_transaction_without_scanning() {
        let (transaction, state) = provider_test_read_transaction([], []);
        let mut provider = StoreActivationRowProvider {
            transaction: Some(transaction),
            source: ActivationRowSource::Creation {
                group_id: TEST_MIGRATION_ID.new_group_id,
            },
            datasets: Vec::new(),
            dataset_index: 0,
            dataset_cursor: None,
            state_rows: DatasetRowPageBatch::bounded(&Schema::empty(), ACTIVATION_ROWS_PER_BATCH),
            transition_rows: DatasetRowTransitionPageBatch::bounded(
                &Schema::empty(),
                &Schema::empty(),
                ACTIVATION_ROWS_PER_BATCH,
            ),
        };

        assert!(
            wait_for_test_future(provider.fill_batch(RowChangeBatch::new()))
                .expect("empty provider must release cleanly")
                .is_none()
        );

        let state = state
            .lock()
            .expect("test transaction state mutex must not be poisoned");
        assert_eq!(state.release_count, 1);
        assert_eq!(state.drop_count, 1);
        assert!(state.row_requests.is_empty());
        assert!(state.transition_requests.is_empty());
    }

    #[test]
    fn corresponding_visible_rows_emit_sorted_value_differences() {
        let dataset_id = DatasetId::try_from_static("docs").expect("test dataset id is valid");
        let previous = stored_row(
            3,
            "before",
            false,
            Some(UpdateId {
                node_index: 0,
                version: 1,
            }),
            VersionVector::Full(PureVersionVector::from([1, 0])),
        );
        let current = stored_row(
            3,
            "after",
            false,
            Some(UpdateId::INITIAL_STATE_ORIGIN),
            VersionVector::initial(NonZeroUsize::new(2).expect("test group has members")),
        );
        let mut output = RowChangeBatch::new();

        append_test_hosted_transition(
            &mut output,
            TEST_MIGRATION_ID,
            &dataset_id,
            &SchemaSource::Static(&TITLE_SCHEMA),
            MemberIndex::new(0),
            &VersionVector::Full(PureVersionVector::from([1, 0])),
            DatasetRowStateTransitionFixture {
                row_key: previous.row_id,
                previous: Some(previous),
                current: Some(current),
            },
        )
        .expect("visible transition must translate");

        assert_eq!(output.len(), 1);
        let change = output.pop().expect("one change must be emitted");
        assert!(matches!(
            change.previous,
            PreviousRow::Present {
                evidence: PreviousRowEvidence {
                    creator: Some(PreviousRowCreator::Local),
                    creation: Some(AcceptedCutRelation::Included),
                    last_state: Some(AcceptedCutRelation::Included),
                },
                ..
            }
        ));
        assert!(matches!(
            change.change,
            RowChangeKind::Upsert {
                previous_value_differences: Some(differences),
                ..
            } if differences.as_ref() == [RowFieldDifference::ValueChanged {
                field_name: Cow::Borrowed("title"),
            }]
        ));
    }

    #[test]
    fn corresponding_identical_rows_emit_an_empty_comparison() {
        let dataset_id = DatasetId::try_from_static("docs").expect("test dataset id is valid");
        let previous = stored_row(
            7,
            "same",
            false,
            Some(UpdateId {
                node_index: 0,
                version: 1,
            }),
            VersionVector::Full(PureVersionVector::from([1, 0])),
        );
        let current = previous.clone();
        let mut output = RowChangeBatch::new();

        append_test_hosted_transition(
            &mut output,
            TEST_MIGRATION_ID,
            &dataset_id,
            &SchemaSource::Static(&TITLE_SCHEMA),
            MemberIndex::new(0),
            &VersionVector::Full(PureVersionVector::from([1, 0])),
            DatasetRowStateTransitionFixture {
                row_key: previous.row_id,
                previous: Some(previous),
                current: Some(current),
            },
        )
        .expect("identical transition must translate");

        let change = output.pop().expect("one upsert must be emitted");
        assert!(matches!(
            change.change,
            RowChangeKind::Upsert {
                previous_value_differences: Some(differences),
                ..
            } if differences.is_empty()
        ));
    }

    #[test]
    fn visible_predecessor_without_visible_successor_emits_old_row_delete() {
        let dataset_id = DatasetId::try_from_static("docs").expect("test dataset id is valid");
        let previous = stored_row(
            4,
            "old only",
            false,
            Some(UpdateId {
                node_index: 0,
                version: 2,
            }),
            VersionVector::Full(PureVersionVector::from([2, 0])),
        );
        let row_key = previous.row_id;
        let mut output = RowChangeBatch::new();

        append_test_hosted_transition(
            &mut output,
            TEST_MIGRATION_ID,
            &dataset_id,
            &SchemaSource::Static(&TITLE_SCHEMA),
            MemberIndex::new(0),
            &VersionVector::Full(PureVersionVector::from([1, 0])),
            DatasetRowStateTransitionFixture {
                row_key,
                previous: Some(previous),
                current: None,
            },
        )
        .expect("old-only transition must translate");

        let change = output.pop().expect("one delete must be emitted");
        let expected_row_id =
            RowId::new(TEST_MIGRATION_ID.old_group_id, dataset_id.clone(), row_key);
        assert!(matches!(
            change.previous,
            PreviousRow::Present {
                row_id,
                evidence: PreviousRowEvidence {
                    creator: Some(PreviousRowCreator::Local),
                    creation: Some(AcceptedCutRelation::NotIncluded),
                    last_state: Some(AcceptedCutRelation::NotIncluded),
                },
            } if row_id == expected_row_id
        ));
        assert!(matches!(
            change.change,
            RowChangeKind::Delete { row_id } if row_id == expected_row_id
        ));
    }

    #[test]
    fn tombstoned_predecessor_is_absent_with_deletion_evidence() {
        let dataset_id = DatasetId::try_from_static("docs").expect("test dataset id is valid");
        let previous = stored_row(
            5,
            "deleted",
            true,
            Some(UpdateId {
                node_index: 1,
                version: 1,
            }),
            VersionVector::Full(PureVersionVector::from([1, 2])),
        );
        let current = stored_row(
            5,
            "restored",
            false,
            Some(UpdateId::INITIAL_STATE_ORIGIN),
            VersionVector::initial(NonZeroUsize::new(2).expect("test group has members")),
        );
        let mut output = RowChangeBatch::new();

        append_test_hosted_transition(
            &mut output,
            TEST_MIGRATION_ID,
            &dataset_id,
            &SchemaSource::Static(&TITLE_SCHEMA),
            MemberIndex::new(0),
            &VersionVector::Full(PureVersionVector::from([1, 1])),
            DatasetRowStateTransitionFixture {
                row_key: previous.row_id,
                previous: Some(previous),
                current: Some(current),
            },
        )
        .expect("restored transition must translate");

        let change = output.pop().expect("one upsert must be emitted");
        assert!(matches!(
            change.previous,
            PreviousRow::Absent(PreviousRowAbsence::Tombstoned {
                evidence: PreviousRowEvidence {
                    creator: Some(PreviousRowCreator::Other),
                    creation: Some(AcceptedCutRelation::Included),
                    last_state: Some(AcceptedCutRelation::NotIncluded),
                },
                ..
            })
        ));
        assert!(matches!(
            change.change,
            RowChangeKind::Upsert {
                previous_value_differences: None,
                ..
            }
        ));
    }

    #[test]
    fn missing_predecessor_is_reported_as_not_stored() {
        let dataset_id = DatasetId::try_from_static("docs").expect("test dataset id is valid");
        let current = stored_row(
            8,
            "new",
            false,
            Some(UpdateId::INITIAL_STATE_ORIGIN),
            VersionVector::initial(NonZeroUsize::new(2).expect("test group has members")),
        );
        let mut output = RowChangeBatch::new();

        append_test_hosted_transition(
            &mut output,
            TEST_MIGRATION_ID,
            &dataset_id,
            &SchemaSource::Static(&TITLE_SCHEMA),
            MemberIndex::new(0),
            &VersionVector::Full(PureVersionVector::from([1, 0])),
            DatasetRowStateTransitionFixture {
                row_key: current.row_id,
                previous: None,
                current: Some(current),
            },
        )
        .expect("new successor transition must translate");

        let change = output.pop().expect("one upsert must be emitted");
        assert_eq!(
            change.previous,
            PreviousRow::Absent(PreviousRowAbsence::NotStored)
        );
    }

    #[test]
    fn synthetic_creator_has_unknown_identity_but_included_creation() {
        let previous = stored_row(
            9,
            "initial",
            false,
            Some(UpdateId::INITIAL_STATE_ORIGIN),
            VersionVector::initial(NonZeroUsize::new(2).expect("test group has members")),
        );

        assert_eq!(
            previous_row_evidence(
                &stored_row_metadata(&previous),
                MemberIndex::new(0),
                &VersionVector::Full(PureVersionVector::from([1, 0])),
            ),
            PreviousRowEvidence {
                creator: None,
                creation: Some(AcceptedCutRelation::Included),
                last_state: Some(AcceptedCutRelation::Included),
            }
        );
    }

    #[test]
    fn missing_creator_provenance_is_reported_as_unknown() {
        let previous = stored_row(
            10,
            "unknown creator",
            false,
            None,
            VersionVector::initial(NonZeroUsize::new(2).expect("test group has members")),
        );

        assert_eq!(
            previous_row_evidence(
                &stored_row_metadata(&previous),
                MemberIndex::new(0),
                &VersionVector::Full(PureVersionVector::from([1, 0])),
            ),
            PreviousRowEvidence {
                creator: None,
                creation: None,
                last_state: Some(AcceptedCutRelation::Included),
            }
        );
    }

    #[test]
    fn concurrent_or_incompatible_state_frontiers_have_unknown_cut_relation() {
        let final_versions = VersionVector::Full(PureVersionVector::from([2, 1]));
        let concurrent = VersionVector::Full(PureVersionVector::from([1, 2]));
        let incompatible = VersionVector::initial(
            NonZeroUsize::new(3).expect("incompatible test group has members"),
        );

        assert_eq!(version_cut_relation(&concurrent, &final_versions), None);
        assert_eq!(version_cut_relation(&incompatible, &final_versions), None);
    }

    #[test]
    fn changed_value_fields_are_canonical_by_field_name() {
        let schema_source = SchemaSource::Shared(Arc::new(Schema::from_fields([
            Field::linear_string("zeta"),
            Field::linear_string("alpha"),
        ])));
        let previous = RowValues::from_fields_unchecked(HashMap::from([
            ("zeta".to_owned(), "before z".into()),
            ("alpha".to_owned(), "before a".into()),
        ]));
        let current = RowValues::from_fields_unchecked(HashMap::from([
            ("zeta".to_owned(), "after z".into()),
            ("alpha".to_owned(), "after a".into()),
        ]));

        assert_eq!(
            changed_value_fields(&schema_source, &previous, &current).as_ref(),
            [
                RowFieldDifference::ValueChanged {
                    field_name: Cow::Owned("alpha".to_owned()),
                },
                RowFieldDifference::ValueChanged {
                    field_name: Cow::Owned("zeta".to_owned()),
                },
            ]
        );
    }

    #[test]
    fn visible_successor_with_unavailable_predecessor_is_marked_unavailable() {
        let dataset_id = DatasetId::try_from_static("docs").expect("test dataset id is valid");
        let current = stored_row(
            6,
            "new member view",
            false,
            Some(UpdateId::INITIAL_STATE_ORIGIN),
            VersionVector::initial(NonZeroUsize::new(2).expect("test group has members")),
        );
        let state_rows = decoded_state_rows(vec![current]);
        let current = state_rows
            .row(0)
            .expect("test state batch must contain one row");
        let mut output = RowChangeBatch::new();

        append_unavailable_current(
            &mut output,
            TEST_MIGRATION_ID.new_group_id,
            &dataset_id,
            &TITLE_SCHEMA,
            &current,
        )
        .expect("unavailable predecessor row must translate");

        let change = output.pop().expect("one upsert must be emitted");
        assert_eq!(change.previous, PreviousRow::Unavailable);
        assert!(matches!(
            change.change,
            RowChangeKind::Upsert {
                previous_value_differences: None,
                ..
            }
        ));
    }
}
