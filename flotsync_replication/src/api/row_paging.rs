//! Schema-aware positional batches and query parameters for dataset-row paging.

use super::{
    DatasetId,
    DatasetRowStateSlice,
    DatasetSchema,
    GroupDatasetSchemaRef,
    GroupId,
    InMemoryStateRowView,
    PageBatch,
    PageBatchInput,
    PageError,
    PageLimit,
    ReplicationRowMetadata,
    ReplicationStateRowBatch,
    ReplicationStateRowTransitionBatch,
    RowKey,
    Schema,
    SchemaSource,
};
use flotsync_core::versions::UpdateId;
use snafu::Snafu;
use std::{collections::HashSet, num::NonZeroUsize, sync::Arc};

/// Query context for one dataset-row collection.
///
/// Borrowed contexts avoid cloning schema data for one-shot operations. Owned
/// contexts let providers retain a cursor without borrowing their own dataset
/// collections.
#[derive(Clone, Debug)]
pub enum DatasetRowsQuery<'a> {
    /// Context borrowed for the lifetime of the cursor.
    Borrowed(GroupDatasetSchemaRef<'a>),
    /// Cheaply owned context suitable for a retained cursor.
    Owned {
        /// Replication group containing the dataset.
        group_id: GroupId,
        /// Dataset identifier and shared schema source.
        dataset: DatasetSchema,
    },
}

impl<'a> DatasetRowsQuery<'a> {
    /// Build a query which borrows its group, dataset, and schema context.
    #[must_use]
    pub const fn borrowed(dataset: GroupDatasetSchemaRef<'a>) -> Self {
        Self::Borrowed(dataset)
    }

    /// Build a query which owns enough context to outlive its caller's borrows.
    #[must_use]
    pub const fn owned(group_id: GroupId, dataset: DatasetSchema) -> DatasetRowsQuery<'static> {
        DatasetRowsQuery::Owned { group_id, dataset }
    }

    /// Borrow the group, dataset, and schema context used by this query.
    #[must_use]
    pub fn context(&self) -> GroupDatasetSchemaRef<'_> {
        match self {
            Self::Borrowed(dataset) => *dataset,
            Self::Owned { group_id, dataset } => GroupDatasetSchemaRef {
                group_id,
                dataset_id: &dataset.dataset_id,
                schema: dataset.schema.as_schema(),
            },
        }
    }

    /// Convert borrowed context into cheaply cloneable owned cursor context.
    #[must_use]
    pub fn into_owned(self) -> DatasetRowsQuery<'static> {
        match self {
            Self::Borrowed(dataset) => DatasetRowsQuery::Owned {
                group_id: *dataset.group_id,
                dataset: DatasetSchema {
                    dataset_id: dataset.dataset_id.clone(),
                    schema: SchemaSource::Shared(Arc::new(dataset.schema.clone())),
                },
            },
            Self::Owned { group_id, dataset } => DatasetRowsQuery::Owned { group_id, dataset },
        }
    }
}

impl<'a> From<GroupDatasetSchemaRef<'a>> for DatasetRowsQuery<'a> {
    fn from(dataset: GroupDatasetSchemaRef<'a>) -> Self {
        Self::borrowed(dataset)
    }
}

/// Query context for the ordered union of one dataset in two replication groups.
#[derive(Clone, Debug)]
pub struct DatasetRowTransitionQuery<'a> {
    /// Dataset occurrence supplying previous row state.
    previous: DatasetRowsQuery<'a>,
    /// Dataset occurrence supplying current row state.
    current: DatasetRowsQuery<'a>,
}

impl<'a> DatasetRowTransitionQuery<'a> {
    /// Build a transition query from two occurrences of the same dataset.
    #[must_use]
    pub const fn new(previous: DatasetRowsQuery<'a>, current: DatasetRowsQuery<'a>) -> Self {
        Self { previous, current }
    }

    /// Return the previous dataset occurrence.
    #[must_use]
    pub const fn previous(&self) -> &DatasetRowsQuery<'a> {
        &self.previous
    }

    /// Return the current dataset occurrence.
    #[must_use]
    pub const fn current(&self) -> &DatasetRowsQuery<'a> {
        &self.current
    }

    /// Convert both sides into owned cursor context.
    #[must_use]
    pub fn into_owned(self) -> DatasetRowTransitionQuery<'static> {
        DatasetRowTransitionQuery {
            previous: self.previous.into_owned(),
            current: self.current.into_owned(),
        }
    }
}

/// Fixed ordered selection used by requested-row paging.
#[derive(Clone, Debug)]
pub struct RequestedDatasetRowsQuery<'a> {
    /// Dataset from which selected keys are loaded.
    dataset: DatasetRowsQuery<'a>,
    /// Sorted distinct keys fixed for the lifetime of the cursor.
    row_keys: Vec<RowKey>,
}

impl<'a> RequestedDatasetRowsQuery<'a> {
    /// Build a requested-row query, sorting and deduplicating its key selection.
    #[must_use]
    pub fn new(dataset: DatasetRowsQuery<'a>, row_keys: impl IntoIterator<Item = RowKey>) -> Self {
        let mut row_keys = row_keys.into_iter().collect::<Vec<_>>();
        row_keys.sort_unstable();
        row_keys.dedup();
        Self { dataset, row_keys }
    }

    /// Return the dataset from which rows are selected.
    #[must_use]
    pub const fn dataset(&self) -> &DatasetRowsQuery<'a> {
        &self.dataset
    }

    /// Return the sorted distinct requested keys.
    #[must_use]
    pub fn row_keys(&self) -> &[RowKey] {
        &self.row_keys
    }
}

/// Successful metadata for one ordinary or requested dataset-row page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatasetRowPageMetadata {
    /// Replication group containing the dataset occurrence.
    pub group_id: GroupId,
    /// Dataset whose rows were selected.
    pub dataset_id: DatasetId,
    /// Whether this dataset occurrence exists in storage.
    pub dataset_exists: bool,
}

/// Successful metadata for one dataset-row transition page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatasetRowTransitionPageMetadata {
    /// Replication group supplying previous row state.
    pub previous_group_id: GroupId,
    /// Replication group supplying current row state.
    pub current_group_id: GroupId,
    /// Dataset selected from both groups.
    pub dataset_id: DatasetId,
    /// Whether the previous dataset occurrence exists in storage.
    pub previous_dataset_exists: bool,
    /// Whether the current dataset occurrence exists in storage.
    pub current_dataset_exists: bool,
}

/// Backend source which appends one decoded row directly into positional storage.
///
/// Store implementations may keep backend-specific decoders in a stack-local
/// source. [`PageBatch::push`] consumes the borrow synchronously, so decoder
/// storage never escapes the store call.
pub trait ReplicationStateRowSource {
    /// Return the stable row key represented by this source.
    fn row_key(&self) -> RowKey;

    /// Decode and append this source to `output`.
    ///
    /// A successful call consumes this source's row. Callers must not append
    /// the same source again. A failed call must leave `output` internally
    /// consistent so the surrounding page attempt can clear it.
    ///
    /// # Errors
    ///
    /// Returns a contextual page or store failure when the source cannot be
    /// decoded against the batch schema.
    fn append_to(&mut self, output: &mut ReplicationStateRowBatch) -> Result<(), PageError>;
}

/// Input family accepting one temporary decoded-row source.
pub struct ReplicationStateRowPageInput;

impl PageBatchInput for ReplicationStateRowPageInput {
    type Value<'record> = &'record mut (dyn ReplicationStateRowSource + 'record);
}

/// One ordered transition input from the union of previous and current row keys.
pub struct ReplicationStateRowTransitionInput<'a> {
    /// Union key represented by this input.
    row_key: RowKey,
    /// Previous row source when that occurrence contains the key.
    previous: Option<&'a mut (dyn ReplicationStateRowSource + 'a)>,
    /// Current row source when that occurrence contains the key.
    current: Option<&'a mut (dyn ReplicationStateRowSource + 'a)>,
}

impl<'a> ReplicationStateRowTransitionInput<'a> {
    /// Build one union-key input with at least one present side.
    #[must_use]
    pub fn new(
        row_key: RowKey,
        previous: Option<&'a mut (dyn ReplicationStateRowSource + 'a)>,
        current: Option<&'a mut (dyn ReplicationStateRowSource + 'a)>,
    ) -> Self {
        Self {
            row_key,
            previous,
            current,
        }
    }
}

/// Input family accepting one temporary transition union entry.
pub struct ReplicationStateRowTransitionPageInput;

impl PageBatchInput for ReplicationStateRowTransitionPageInput {
    type Value<'record> = ReplicationStateRowTransitionInput<'record>;
}

/// One requested-key input, including explicit storage absence.
pub enum RequestedDatasetRowInput<'a> {
    /// The requested key has one stored row source.
    Present(&'a mut (dyn ReplicationStateRowSource + 'a)),
    /// The requested key is absent from storage.
    Missing(RowKey),
}

/// Input family accepting one explicit requested-key outcome.
pub struct RequestedDatasetRowPageInput;

impl PageBatchInput for RequestedDatasetRowPageInput {
    type Value<'record> = RequestedDatasetRowInput<'record>;
}

/// Reusable storage contract wrapped by [`PositionalPageBatch`].
pub trait PositionalPageStorage: Send {
    /// Temporary input family accepted by this storage.
    type Input: PageBatchInput;

    /// Clear retained page values while preserving reusable allocations.
    fn clear_page_values(&mut self);

    /// Reserve capacity for at least `additional` more logical page results.
    fn reserve_page_values(&mut self, additional: usize);

    /// Return the number of logical page results retained.
    fn page_value_len(&self) -> usize;

    /// Accept one logical page input.
    ///
    /// # Errors
    ///
    /// Returns the input's contextual projection or contract failure.
    fn push_page_value(
        &mut self,
        input: <Self::Input as PageBatchInput>::Value<'_>,
    ) -> Result<(), PageError>;
}

/// One reusable paging envelope shared by positional store outputs.
pub struct PositionalPageBatch<Storage, Metadata> {
    /// Positional values retained by the latest successful fill.
    storage: Storage,
    /// Metadata retained by the latest successful fill.
    metadata: Option<Metadata>,
    /// Record limit sampled by each new page attempt.
    limit: PageLimit,
}

impl<Storage, Metadata> PositionalPageBatch<Storage, Metadata> {
    /// Build a bounded positional page over `storage`.
    #[must_use]
    pub const fn with_maximum(storage: Storage, limit: NonZeroUsize) -> Self {
        Self {
            storage,
            metadata: None,
            limit: PageLimit::Max(limit),
        }
    }

    /// Build an unlimited positional page over `storage`.
    #[must_use]
    pub const fn with_unlimited(storage: Storage) -> Self {
        Self {
            storage,
            metadata: None,
            limit: PageLimit::Unlimited,
        }
    }

    /// Return the retained positional storage.
    #[must_use]
    pub const fn storage(&self) -> &Storage {
        &self.storage
    }

    /// Mutably borrow the positional storage.
    pub const fn storage_mut(&mut self) -> &mut Storage {
        &mut self.storage
    }

    /// Consume this page batch and return its positional storage.
    #[must_use]
    pub fn into_storage(self) -> Storage {
        self.storage
    }

    /// Return metadata from the latest successful fill.
    ///
    /// `Some` contains metadata installed when that fill completed. `None`
    /// means this batch is new, was prepared or cleared, or its latest fill
    /// failed.
    #[must_use]
    pub const fn metadata(&self) -> Option<&Metadata> {
        self.metadata.as_ref()
    }
}

impl<Storage, Metadata> PageBatch for PositionalPageBatch<Storage, Metadata>
where
    Storage: PositionalPageStorage,
    Metadata: Send,
{
    type Input = Storage::Input;
    type Metadata = Metadata;

    fn page_limit(&self) -> PageLimit {
        self.limit
    }

    fn clear(&mut self) {
        self.storage.clear_page_values();
        self.metadata = None;
    }

    fn reserve(&mut self, additional: usize) {
        self.storage.reserve_page_values(additional);
    }

    fn len(&self) -> usize {
        self.storage.page_value_len()
    }

    fn metadata(&self) -> Option<&Self::Metadata> {
        self.metadata.as_ref()
    }

    fn push(&mut self, input: <Self::Input as PageBatchInput>::Value<'_>) -> Result<(), PageError> {
        self.storage.push_page_value(input)
    }

    fn set_metadata(&mut self, metadata: Self::Metadata) {
        self.metadata = Some(metadata);
    }
}

impl PositionalPageStorage for ReplicationStateRowBatch {
    type Input = ReplicationStateRowPageInput;

    fn clear_page_values(&mut self) {
        self.reset_rows();
    }

    fn reserve_page_values(&mut self, additional: usize) {
        self.reserve_rows(additional);
    }

    fn page_value_len(&self) -> usize {
        self.len()
    }

    fn push_page_value(
        &mut self,
        input: <Self::Input as PageBatchInput>::Value<'_>,
    ) -> Result<(), PageError> {
        input.append_to(self)
    }
}

impl PositionalPageStorage for ReplicationStateRowTransitionBatch {
    type Input = ReplicationStateRowTransitionPageInput;

    fn clear_page_values(&mut self) {
        self.reset_rows();
    }

    fn reserve_page_values(&mut self, additional: usize) {
        self.reserve_rows(additional);
    }

    fn page_value_len(&self) -> usize {
        self.len()
    }

    fn push_page_value(
        &mut self,
        input: <Self::Input as PageBatchInput>::Value<'_>,
    ) -> Result<(), PageError> {
        let ReplicationStateRowTransitionInput {
            row_key,
            previous,
            current,
        } = input;
        if previous.is_none() && current.is_none() {
            return Err(PageError::from_batch_error(
                EmptyTransitionInputError { row_key }.into(),
            ));
        }
        let previous_index = append_transition_side(row_key, previous, self.previous_rows_mut())?;
        let current_index = append_transition_side(row_key, current, self.current_rows_mut())?;
        self.push_alignment(previous_index, current_index);
        Ok(())
    }
}

/// Page batch used by ordinary ordered dataset-row scans.
pub type DatasetRowPageBatch =
    PositionalPageBatch<ReplicationStateRowBatch, DatasetRowPageMetadata>;

impl DatasetRowPageBatch {
    /// Build a bounded row page prepared for `schema`.
    #[must_use]
    pub fn bounded(schema: &Schema, limit: NonZeroUsize) -> Self {
        Self::with_maximum(ReplicationStateRowBatch::new(schema), limit)
    }

    /// Build an unlimited row page prepared for `schema`.
    #[must_use]
    pub fn unlimited(schema: &Schema) -> Self {
        Self::with_unlimited(ReplicationStateRowBatch::new(schema))
    }

    /// Clear retained rows and prepare their positional layout for `schema`.
    pub fn prepare_for_schema(&mut self, schema: &Schema) {
        self.storage.reuse_for_schema(schema);
        self.metadata = None;
    }

    /// Return rows retained by the latest successful fill.
    #[must_use]
    pub const fn rows(&self) -> &ReplicationStateRowBatch {
        &self.storage
    }
}

/// Page batch used by ordered dataset-row transition scans.
pub type DatasetRowTransitionPageBatch =
    PositionalPageBatch<ReplicationStateRowTransitionBatch, DatasetRowTransitionPageMetadata>;

impl DatasetRowTransitionPageBatch {
    /// Build a bounded transition page prepared for both schemas.
    #[must_use]
    pub fn bounded(previous_schema: &Schema, current_schema: &Schema, limit: NonZeroUsize) -> Self {
        Self::with_maximum(
            ReplicationStateRowTransitionBatch::new(previous_schema, current_schema),
            limit,
        )
    }

    /// Build an unlimited transition page prepared for both schemas.
    #[must_use]
    pub fn unlimited(previous_schema: &Schema, current_schema: &Schema) -> Self {
        Self::with_unlimited(ReplicationStateRowTransitionBatch::new(
            previous_schema,
            current_schema,
        ))
    }

    /// Clear retained transitions and prepare both positional layouts.
    pub fn prepare_for_schemas(&mut self, previous_schema: &Schema, current_schema: &Schema) {
        self.storage
            .reuse_for_schemas(previous_schema, current_schema);
        self.metadata = None;
    }

    /// Return transitions retained by the latest successful fill.
    #[must_use]
    pub const fn transitions(&self) -> &ReplicationStateRowTransitionBatch {
        &self.storage
    }
}

/// Ordered requested-row result borrowed from a page batch.
pub enum RequestedDatasetRowView<'a> {
    /// One present row decoded into positional storage.
    Present(InMemoryStateRowView<'a, ReplicationRowMetadata, UpdateId>),
    /// One requested key absent from storage.
    Missing(RowKey),
}

/// Positional rows and outcome order retained for requested keys.
pub struct RequestedDatasetRowStorage {
    /// Present rows in requested-key order with missing entries omitted.
    rows: ReplicationStateRowBatch,
    /// One ordered result per distinct requested key.
    outcomes: Vec<RequestedDatasetRowOutcomeIndex>,
}

impl RequestedDatasetRowStorage {
    /// Build empty requested-row storage prepared for `schema`.
    #[must_use]
    pub fn new(schema: &Schema) -> Self {
        Self {
            rows: ReplicationStateRowBatch::new(schema),
            outcomes: Vec::new(),
        }
    }

    /// Return present rows retained by the latest fill.
    #[must_use]
    pub const fn rows(&self) -> &ReplicationStateRowBatch {
        &self.rows
    }

    /// Iterate over every distinct requested key outcome in query order.
    ///
    /// # Panics
    ///
    /// Panics only if this storage's private outcome index no longer refers to
    /// its corresponding retained row, which safe public APIs cannot cause.
    #[must_use]
    pub fn outcomes(
        &self,
    ) -> impl ExactSizeIterator<Item = RequestedDatasetRowView<'_>> + DoubleEndedIterator {
        self.outcomes.iter().map(|outcome| match outcome {
            RequestedDatasetRowOutcomeIndex::Present(row_index) => {
                RequestedDatasetRowView::Present(
                    self.rows
                        .row(*row_index)
                        .expect("requested-row outcome index must remain valid"),
                )
            }
            RequestedDatasetRowOutcomeIndex::Missing(row_key) => {
                RequestedDatasetRowView::Missing(*row_key)
            }
        })
    }

    /// Clear retained outcomes and prepare present-row storage for `schema`.
    fn prepare_for_schema(&mut self, schema: &Schema) {
        self.rows.reuse_for_schema(schema);
        self.outcomes.clear();
    }

    /// Consume this storage into the existing complete-result representation.
    fn into_rows_and_missing(self) -> (ReplicationStateRowBatch, HashSet<RowKey>) {
        let missing_row_keys = self
            .outcomes
            .into_iter()
            .filter_map(|outcome| match outcome {
                RequestedDatasetRowOutcomeIndex::Present(_) => None,
                RequestedDatasetRowOutcomeIndex::Missing(row_key) => Some(row_key),
            })
            .collect();
        (self.rows, missing_row_keys)
    }
}

impl PositionalPageStorage for RequestedDatasetRowStorage {
    type Input = RequestedDatasetRowPageInput;

    fn clear_page_values(&mut self) {
        self.rows.reset_rows();
        self.outcomes.clear();
    }

    fn reserve_page_values(&mut self, additional: usize) {
        self.rows.reserve_rows(additional);
        self.outcomes.reserve(additional);
    }

    fn page_value_len(&self) -> usize {
        self.outcomes.len()
    }

    fn push_page_value(
        &mut self,
        input: <Self::Input as PageBatchInput>::Value<'_>,
    ) -> Result<(), PageError> {
        match input {
            RequestedDatasetRowInput::Present(source) => {
                let row_index = self.rows.len();
                source.append_to(&mut self.rows)?;
                self.outcomes
                    .push(RequestedDatasetRowOutcomeIndex::Present(row_index));
            }
            RequestedDatasetRowInput::Missing(row_key) => {
                self.outcomes
                    .push(RequestedDatasetRowOutcomeIndex::Missing(row_key));
            }
        }
        Ok(())
    }
}

/// Page batch used by fixed requested-row selections.
pub type RequestedDatasetRowPageBatch =
    PositionalPageBatch<RequestedDatasetRowStorage, DatasetRowPageMetadata>;

impl RequestedDatasetRowPageBatch {
    /// Build a bounded requested-row page prepared for `schema`.
    #[must_use]
    pub fn bounded(schema: &Schema, limit: NonZeroUsize) -> Self {
        Self::with_maximum(RequestedDatasetRowStorage::new(schema), limit)
    }

    /// Build an unlimited requested-row page prepared for `schema`.
    #[must_use]
    pub fn unlimited(schema: &Schema) -> Self {
        Self::with_unlimited(RequestedDatasetRowStorage::new(schema))
    }

    /// Clear retained outcomes and prepare row storage for `schema`.
    pub fn prepare_for_schema(&mut self, schema: &Schema) {
        self.storage.prepare_for_schema(schema);
        self.metadata = None;
    }

    /// Return present rows retained by the latest successful fill.
    #[must_use]
    pub const fn rows(&self) -> &ReplicationStateRowBatch {
        self.storage.rows()
    }

    /// Iterate over every requested-key outcome in query order.
    #[must_use]
    pub fn outcomes(
        &self,
    ) -> impl ExactSizeIterator<Item = RequestedDatasetRowView<'_>> + DoubleEndedIterator {
        self.storage.outcomes()
    }

    /// Consume a complete requested-row batch into the legacy aggregate shape.
    ///
    /// # Panics
    ///
    /// Panics when called before a successful fill installed metadata.
    #[must_use]
    pub fn into_state_slice(self) -> DatasetRowStateSlice {
        let metadata = self
            .metadata
            .expect("a requested-row batch needs successful metadata before conversion");
        let (state_rows, missing_row_keys) = self.storage.into_rows_and_missing();
        DatasetRowStateSlice {
            group_id: metadata.group_id,
            dataset_id: metadata.dataset_id,
            dataset_exists: metadata.dataset_exists,
            state_rows,
            missing_row_keys,
        }
    }
}

/// Private requested-result representation referring into positional row storage.
enum RequestedDatasetRowOutcomeIndex {
    /// Present row stored at this positional index.
    Present(usize),
    /// Requested key absent from storage.
    Missing(RowKey),
}

/// Append one optional transition side after validating its union key.
fn append_transition_side(
    expected_row_key: RowKey,
    source: Option<&mut dyn ReplicationStateRowSource>,
    output: &mut ReplicationStateRowBatch,
) -> Result<Option<usize>, PageError> {
    let Some(source) = source else {
        return Ok(None);
    };
    let actual_row_key = source.row_key();
    if actual_row_key != expected_row_key {
        return Err(PageError::from_batch_error(
            TransitionRowKeyMismatchError {
                expected: expected_row_key,
                actual: actual_row_key,
            }
            .into(),
        ));
    }
    let row_index = output.len();
    source.append_to(output)?;
    Ok(Some(row_index))
}

/// A transition union entry contained neither previous nor current state.
#[derive(Debug, Snafu)]
#[snafu(display("Dataset row transition for '{row_key}' contained no row state."))]
struct EmptyTransitionInputError {
    /// Union key whose two sides were both absent.
    row_key: RowKey,
}

/// A transition side exposed a different key than its union entry.
#[derive(Debug, Snafu)]
#[snafu(display(
    "Dataset row transition expected key '{expected}', but one side contained '{actual}'."
))]
struct TransitionRowKeyMismatchError {
    /// Union key supplied by the backend.
    expected: RowKey,
    /// Key exposed by one row source.
    actual: RowKey,
}

/// Tests for positional page-storage invariants.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{PageCursor, ReplicationRowStateSnapshot, StoreTransactionId};
    use flotsync_core::versions::VersionVector;
    use flotsync_messages::{
        codecs::datamodel::encode_row_snapshot,
        snapshots::datamodel::ProtoSchemaSnapshotDecoder,
    };
    use uuid::Uuid;

    const TRANSACTION: StoreTransactionId = StoreTransactionId::from_uuid(Uuid::from_u128(31));

    /// Single-use row source backed by an empty-schema snapshot.
    struct TestRowSource {
        /// Metadata consumed by the first successful append.
        metadata: Option<ReplicationRowMetadata>,
    }

    impl TestRowSource {
        /// Build one active test row with the supplied key.
        fn new(row_key: RowKey) -> Self {
            Self {
                metadata: Some(ReplicationRowMetadata {
                    row_key,
                    tombstoned: false,
                    created_by: Some(UpdateId::INITIAL_STATE_ORIGIN),
                    last_changed_versions: VersionVector::initial(
                        NonZeroUsize::new(1).expect("test member count must be non-zero"),
                    ),
                }),
            }
        }
    }

    impl ReplicationStateRowSource for TestRowSource {
        fn row_key(&self) -> RowKey {
            self.metadata
                .as_ref()
                .expect("test row source must not be inspected after consumption")
                .row_key
        }

        fn append_to(&mut self, output: &mut ReplicationStateRowBatch) -> Result<(), PageError> {
            let schema = Schema::empty();
            let snapshot = ReplicationRowStateSnapshot::from_owned_fields(Vec::new());
            let encoded = encode_row_snapshot(&snapshot, &schema)
                .expect("empty test row must encode against the empty schema");
            let mut decoder = ProtoSchemaSnapshotDecoder::new(encoded)
                .expect("empty test row must create a snapshot decoder");
            let metadata = self
                .metadata
                .take()
                .expect("test row source must not be appended twice");
            output
                .push_decoded_row(metadata, &mut decoder)
                .expect("empty test row must decode into positional storage");
            Ok(())
        }
    }

    #[test]
    fn transition_page_rejects_an_input_without_either_side() {
        let schema = Schema::empty();
        let mut cursor = PageCursor::new(());
        let mut batch = DatasetRowTransitionPageBatch::bounded(
            &schema,
            &schema,
            NonZeroUsize::new(1).expect("test page limit must be non-zero"),
        );
        {
            let mut page = cursor
                .begin_page::<u8, _>(TRANSACTION, &mut batch)
                .expect("fresh test page must begin");
            let result = page.push(ReplicationStateRowTransitionInput::new(
                RowKey(Uuid::from_u128(1)),
                None,
                None,
            ));
            assert!(matches!(result, Err(PageError::BatchPush { .. })));
        }

        assert!(cursor.is_failed());
        assert!(batch.transitions().is_empty());
        assert!(batch.metadata().is_none());
    }

    #[test]
    fn transition_page_clears_a_previous_side_after_current_key_mismatch() {
        let schema = Schema::empty();
        let expected_key = RowKey(Uuid::from_u128(1));
        let mut previous = TestRowSource::new(expected_key);
        let mut current = TestRowSource::new(RowKey(Uuid::from_u128(2)));
        let mut cursor = PageCursor::new(());
        let mut batch = DatasetRowTransitionPageBatch::bounded(
            &schema,
            &schema,
            NonZeroUsize::new(1).expect("test page limit must be non-zero"),
        );
        {
            let mut page = cursor
                .begin_page::<u8, _>(TRANSACTION, &mut batch)
                .expect("fresh test page must begin");
            let input = ReplicationStateRowTransitionInput::new(
                expected_key,
                Some(&mut previous),
                Some(&mut current),
            );
            assert!(matches!(page.push(input), Err(PageError::BatchPush { .. })));
        }

        assert!(previous.metadata.is_none());
        assert!(current.metadata.is_some());
        assert!(cursor.is_failed());
        assert!(batch.transitions().is_empty());
        assert!(batch.metadata().is_none());
    }
}
