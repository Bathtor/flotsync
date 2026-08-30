//! Schema-bound row-major storage for decoded in-memory field state.

use super::{
    InMemoryFieldState,
    InMemoryStateData,
    InMemoryStateDataError,
    InMemoryStateDataSnapshotDecodeError,
    InMemoryStateRow,
    OrderedSchema,
    ProjectedFieldValue,
    RowStateRead,
    RowStateSnapshot,
    RowValueRead,
    Schema,
    SchemaSnapshotDecoder,
    SchemaSource,
};
use std::{fmt, hash::Hash};

/// Reusable positional state rows sharing one ordered schema layout.
#[derive(Clone, Debug, PartialEq)]
pub struct InMemoryStateRowBatch<Metadata, OperationId> {
    /// Owned positional schema governing every row in this batch.
    schema: OrderedSchema<'static>,
    /// Per-row metadata aligned with row-major chunks in `fields`.
    rows: Vec<Metadata>,
    /// Flat row-major field states without repeated field names.
    fields: Vec<InMemoryFieldState<OperationId>>,
}

impl<Metadata, OperationId> InMemoryStateRowBatch<Metadata, OperationId> {
    /// Create an empty batch for one positional layout.
    #[must_use]
    pub fn new(schema: &Schema) -> Self {
        Self {
            schema: OrderedSchema::from_schema(schema).into_owned(),
            rows: Vec::new(),
            fields: Vec::new(),
        }
    }

    /// Reserve enough storage for at least `additional_rows` more rows.
    pub fn reserve_rows(&mut self, additional_rows: usize) {
        self.rows.reserve(additional_rows);
        let additional_fields = additional_rows.saturating_mul(self.schema.len());
        self.fields.reserve(additional_fields);
    }

    /// Return the number of complete rows that fit without reallocating either vector.
    #[must_use]
    pub fn capacity(&self) -> usize {
        if self.schema.is_empty() {
            self.rows.capacity()
        } else {
            self.rows
                .capacity()
                .min(self.fields.capacity() / self.schema.len())
        }
    }

    /// Return the number of complete rows in the batch.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Return true iff the batch contains no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Return the ordered schema defining this batch's field positions.
    #[must_use]
    pub fn schema(&self) -> &OrderedSchema<'_> {
        &self.schema
    }

    /// Clear all rows while retaining row and field allocations.
    pub fn reset_rows(&mut self) {
        self.rows.clear();
        self.fields.clear();
    }

    /// Clear the batch and prepare it for `schema`.
    ///
    /// Existing allocations and the current ordered schema are retained when
    /// the supplied schema compares equal without considering field order.
    pub fn reuse_for_schema(&mut self, schema: &Schema) {
        self.reset_rows();
        if self.schema != *schema {
            self.schema = OrderedSchema::from_schema(schema).into_owned();
        }
    }

    /// Decode and append one complete row.
    ///
    /// Metadata becomes visible only after every field and the decoder end
    /// marker succeed. Any partially appended field states are removed on
    /// failure.
    ///
    /// # Errors
    ///
    /// Returns [`InMemoryStateDataSnapshotDecodeError`] when the decoder or one
    /// decoded field is invalid.
    pub fn push_decoded_row<D>(
        &mut self,
        metadata: Metadata,
        decoder: &mut D,
    ) -> Result<(), InMemoryStateDataSnapshotDecodeError<D::Error>>
    where
        OperationId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
        D: SchemaSnapshotDecoder<OperationId>,
    {
        let initial_fields_len = self.fields.len();
        let result = InMemoryStateRow::append_decoded_snapshot_fields(
            &self.schema,
            &mut self.fields,
            decoder,
        );
        if let Err(error) = result {
            self.fields.truncate(initial_fields_len);
            return Err(error);
        }
        self.rows.push(metadata);
        debug_assert_eq!(self.fields.len(), self.rows.len() * self.schema.len());
        Ok(())
    }

    /// Return one row by its positional index.
    #[must_use]
    pub fn row(&self, row_index: usize) -> Option<InMemoryStateRowView<'_, Metadata, OperationId>> {
        if row_index >= self.rows.len() {
            return None;
        }
        let field_offset = row_index.checked_mul(self.schema.len())?;
        Some(InMemoryStateRowView {
            batch: self,
            row_index,
            field_offset,
        })
    }

    /// Iterate over complete rows in insertion order.
    #[must_use]
    pub fn rows(
        &self,
    ) -> impl DoubleEndedIterator<Item = InMemoryStateRowView<'_, Metadata, OperationId>>
    + ExactSizeIterator {
        let field_count = self.schema.len();
        (0..self.rows.len()).map(move |row_index| InMemoryStateRowView {
            batch: self,
            row_index,
            field_offset: row_index.saturating_mul(field_count),
        })
    }
}

impl<RowId, OperationId> InMemoryStateData<RowId, OperationId>
where
    RowId: PartialEq + Eq + Hash,
{
    /// Consume a positional row batch into one in-memory dataset.
    ///
    /// Field states are moved into the dataset. Field names are borrowed only
    /// while mapping the batch's opaque order into the dataset's opaque order.
    ///
    /// # Errors
    ///
    /// See [`InMemoryStateDataError`] for failure conditions.
    pub fn from_row_batch<Metadata>(
        schema: impl Into<SchemaSource>,
        batch: InMemoryStateRowBatch<Metadata, OperationId>,
        map_metadata: impl Fn(Metadata) -> (RowId, bool),
    ) -> Result<Self, InMemoryStateDataError>
    where
        RowId: fmt::Display,
    {
        let InMemoryStateRowBatch {
            schema: batch_schema,
            rows: row_metadata,
            fields,
        } = batch;
        let mut data = Self::new(schema);
        data.row_id_map.reserve(row_metadata.len());
        data.rows.reserve(row_metadata.len());

        let fields_per_row = batch_schema.len();
        let mut fields = fields.into_iter();
        for metadata in row_metadata {
            let (row_id, tombstoned) = map_metadata(metadata);
            if data.row_id_map.contains_key(&row_id) {
                return Err(InMemoryStateDataError::DuplicateRowId {
                    row_id: row_id.to_string(),
                });
            }

            let named_fields = batch_schema
                .fields()
                .map(|field| field.name.as_str())
                .zip(fields.by_ref().take(fields_per_row));
            let mut row = data.row_from_named_fields(named_fields)?;
            row.deleted = tombstoned;

            let row_index = data.rows.len();
            data.rows.push(row);
            data.row_id_map.insert(row_id, row_index);
        }
        debug_assert_eq!(fields.len(), 0);
        Ok(data)
    }
}

/// Borrowed view over one row in an [`InMemoryStateRowBatch`].
#[derive(Debug)]
pub struct InMemoryStateRowView<'a, Metadata, OperationId> {
    /// Batch owning the schema, metadata, and field states.
    batch: &'a InMemoryStateRowBatch<Metadata, OperationId>,
    /// Index of this row's metadata.
    row_index: usize,
    /// Offset of this row's first state in the flat field vector.
    field_offset: usize,
}

impl<Metadata, OperationId> Clone for InMemoryStateRowView<'_, Metadata, OperationId> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Metadata, OperationId> Copy for InMemoryStateRowView<'_, Metadata, OperationId> {}

impl<'a, Metadata, OperationId> InMemoryStateRowView<'a, Metadata, OperationId> {
    /// Return this row's metadata.
    #[must_use]
    pub fn metadata(&self) -> &'a Metadata {
        &self.batch.rows[self.row_index]
    }

    /// Return the state at one positional field index.
    #[must_use]
    pub fn field_at(&self, field_index: usize) -> Option<&'a InMemoryFieldState<OperationId>> {
        if field_index >= self.batch.schema.len() {
            return None;
        }
        let value_index = self.field_offset.checked_add(field_index)?;
        self.batch.fields.get(value_index)
    }

    /// Return this row's complete field-state slice.
    #[must_use]
    pub fn fields(&self) -> &'a [InMemoryFieldState<OperationId>] {
        let end = self.field_offset + self.batch.schema.len();
        &self.batch.fields[self.field_offset..end]
    }

    /// Return a complete borrowed state snapshot for this row.
    #[must_use]
    pub fn snapshot(&self) -> RowStateSnapshot<'a, OperationId> {
        RowStateSnapshot::borrowed_fields(&self.batch.schema, self.fields())
    }
}

impl<Metadata, OperationId> RowStateRead<OperationId>
    for InMemoryStateRowView<'_, Metadata, OperationId>
{
    fn get_field(&self, field_name: &str) -> Option<&InMemoryFieldState<OperationId>> {
        let field_index = self.batch.schema.field_index(field_name)?;
        self.field_at(field_index)
    }
}

impl<Metadata, OperationId> RowValueRead for InMemoryStateRowView<'_, Metadata, OperationId>
where
    OperationId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
{
    fn get_value(&self, field_name: &str) -> Option<ProjectedFieldValue<'_>> {
        self.get_field(field_name)
            .map(InMemoryFieldState::project_value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        IdWithIndex,
        NullableBasicValue,
        schema::{
            Field,
            NullableBasicDataType,
            PrimitiveType,
            ReplicatedDataType,
            datamodel::{CounterValue, SnapshotNodeSource, StateSnapshotFieldValue},
            values::PrimitiveValueArray,
        },
        snapshot::SnapshotNode,
    };
    use std::{collections::HashMap, marker::PhantomData};

    /// State-only decoder used to exercise direct row-batch appends.
    struct TestDecoder {
        /// State values keyed by schema field name.
        fields: HashMap<String, StateSnapshotFieldValue>,
        /// Expected number of fields supplied to `begin`.
        expected_field_count: Option<usize>,
        /// Number of fields decoded so far.
        decoded_field_count: usize,
        /// Whether `end` should inject a failure after all fields were appended.
        fail_end: bool,
    }

    impl TestDecoder {
        /// Build one successful decoder from counter values.
        fn counters(values: impl IntoIterator<Item = (&'static str, u64)>) -> Self {
            Self {
                fields: values
                    .into_iter()
                    .map(|(name, value)| {
                        (
                            name.to_owned(),
                            StateSnapshotFieldValue::MonotonicCounter(CounterValue::UInt(value)),
                        )
                    })
                    .collect(),
                expected_field_count: None,
                decoded_field_count: 0,
                fail_end: false,
            }
        }
    }

    impl SchemaSnapshotDecoder<u32> for TestDecoder {
        type Error = TestDecoderError;

        type LatestValueWinsFieldSource<'a> = EmptyNodeSource<IdWithIndex<u32>, NullableBasicValue>;
        type LinearStringFieldSource<'a> = EmptyNodeSource<IdWithIndex<u32>, String>;
        type LinearListFieldSource<'a> = EmptyNodeSource<IdWithIndex<u32>, PrimitiveValueArray>;

        fn begin(&mut self, expected_field_count: usize) -> Result<(), Self::Error> {
            self.expected_field_count = Some(expected_field_count);
            self.decoded_field_count = 0;
            Ok(())
        }

        fn decode_state_field(
            &mut self,
            field_name: &str,
            _data_type: &ReplicatedDataType,
        ) -> Result<StateSnapshotFieldValue, Self::Error> {
            let value = self
                .fields
                .remove(field_name)
                .ok_or(TestDecoderError::MissingField {
                    field_name: field_name.to_owned(),
                })?;
            self.decoded_field_count += 1;
            Ok(value)
        }

        fn prepare_latest_value_wins_field<'a>(
            &'a mut self,
            _field_name: &str,
            _value_type: &NullableBasicDataType,
        ) -> Result<Self::LatestValueWinsFieldSource<'a>, Self::Error> {
            Ok(EmptyNodeSource::new())
        }

        fn prepare_linear_string_field<'a>(
            &'a mut self,
            _field_name: &str,
        ) -> Result<Self::LinearStringFieldSource<'a>, Self::Error> {
            Ok(EmptyNodeSource::new())
        }

        fn prepare_linear_list_field<'a>(
            &'a mut self,
            _field_name: &str,
            _value_type: PrimitiveType,
        ) -> Result<Self::LinearListFieldSource<'a>, Self::Error> {
            Ok(EmptyNodeSource::new())
        }

        fn end(&mut self) -> Result<(), Self::Error> {
            if self.fail_end {
                return Err(TestDecoderError::InjectedEnd);
            }
            let expected_field_count = self
                .expected_field_count
                .expect("batch decoder must call begin before end");
            if self.decoded_field_count != expected_field_count {
                return Err(TestDecoderError::FieldCount {
                    expected_field_count,
                    decoded_field_count: self.decoded_field_count,
                });
            }
            Ok(())
        }
    }

    /// Empty history source required by the decoder contract but unused by counter fields.
    struct EmptyNodeSource<Id, Value> {
        /// Associates the source with its node types without owning either.
        marker: PhantomData<fn() -> (Id, Value)>,
    }

    impl<Id, Value> EmptyNodeSource<Id, Value> {
        /// Build an exhausted source.
        fn new() -> Self {
            Self {
                marker: PhantomData,
            }
        }
    }

    impl<Id, Value> SnapshotNodeSource<Id, Value> for EmptyNodeSource<Id, Value> {
        type Error = TestDecoderError;

        fn next_node(&mut self) -> Result<Option<SnapshotNode<Id, Value>>, Self::Error> {
            Ok(None)
        }
    }

    /// Injected failures and contract errors from [`TestDecoder`].
    #[derive(Debug, snafu::Snafu)]
    enum TestDecoderError {
        /// A schema field had no supplied state value.
        #[snafu(display("missing field '{field_name}'"))]
        MissingField { field_name: String },
        /// The decoder completed with the wrong number of fields.
        #[snafu(display(
            "decoded {decoded_field_count} fields but expected {expected_field_count}"
        ))]
        FieldCount {
            expected_field_count: usize,
            decoded_field_count: usize,
        },
        /// Failure injected after all field values were decoded.
        #[snafu(display("injected decoder end failure"))]
        InjectedEnd,
    }

    /// Build the two-counter schema shared by row-batch tests.
    fn counter_schema() -> Schema {
        Schema::from_fields([
            Field::monotonic_counter("first"),
            Field::monotonic_counter("second"),
        ])
    }

    #[test]
    fn decoded_rows_support_positional_named_and_snapshot_access() {
        let schema = counter_schema();
        let mut batch = InMemoryStateRowBatch::<String, u32>::new(&schema);
        let mut decoder = TestDecoder::counters([("first", 11), ("second", 22)]);

        batch
            .push_decoded_row("row metadata".to_owned(), &mut decoder)
            .expect("valid state row must decode");

        assert_eq!(batch.len(), 1);
        assert!(!batch.is_empty());
        assert!(batch.row(1).is_none());
        let row = batch.row(0).expect("decoded row must be present");
        assert_eq!(row.metadata(), "row metadata");
        assert_eq!(row.fields().len(), schema.columns.len());
        for (index, field) in batch.schema().fields().enumerate() {
            assert!(std::ptr::eq(
                row.field_at(index).expect("positional field must exist"),
                row.get_field(&field.name)
                    .expect("named field lookup must succeed"),
            ));
        }
        assert!(row.field_at(schema.columns.len()).is_none());

        let snapshot_fields = row.snapshot().into_owned().into_owned_fields();
        for (field_name, field_state) in snapshot_fields {
            assert_eq!(Some(&field_state), row.get_field(&field_name));
        }
    }

    #[test]
    fn consuming_row_batch_moves_non_clone_field_states() {
        #[derive(Debug, PartialEq, Eq)]
        struct NonCloneOperationId;

        let schema = counter_schema();
        let batch_schema = OrderedSchema::from_schema(&schema).into_owned();
        let mut fields = Vec::new();
        for (first, second) in [(11, 22), (33, 44)] {
            for field in batch_schema.fields() {
                let value = match field.name.as_str() {
                    "first" => first,
                    "second" => second,
                    name => panic!("unexpected test field '{name}'"),
                };
                fields.push(InMemoryFieldState::MonotonicCounter(CounterValue::UInt(
                    value,
                )));
            }
        }
        let batch = InMemoryStateRowBatch::<_, NonCloneOperationId> {
            schema: batch_schema,
            rows: vec![(7_u8, false), (8_u8, true)],
            fields,
        };

        let data = InMemoryStateData::from_row_batch(schema, batch, |metadata| metadata)
            .expect("compatible positional rows must transfer");

        assert_eq!(data.len(), 2);
        assert_eq!(data.row_is_tombstoned(&7), Some(false));
        assert_eq!(data.row_is_tombstoned(&8), Some(true));
        let first_field_index = data
            .field_index("first")
            .expect("transferred schema must contain first");
        let second_field_index = data
            .field_index("second")
            .expect("transferred schema must contain second");
        let first_row_index = data.row_id_map[&7];
        let second_row_index = data.row_id_map[&8];
        assert_eq!(
            data.rows[first_row_index].fields[first_field_index],
            InMemoryFieldState::MonotonicCounter(CounterValue::UInt(11))
        );
        assert_eq!(
            data.rows[second_row_index].fields[second_field_index],
            InMemoryFieldState::MonotonicCounter(CounterValue::UInt(44))
        );
    }

    #[test]
    fn failed_decode_rolls_back_fields_and_metadata() {
        let schema = counter_schema();
        let mut batch = InMemoryStateRowBatch::<u8, u32>::new(&schema);
        let mut valid_decoder = TestDecoder::counters([("first", 1), ("second", 2)]);
        batch
            .push_decoded_row(1, &mut valid_decoder)
            .expect("first row must decode");
        let original_fields = batch
            .row(0)
            .expect("first row must remain present")
            .fields()
            .to_vec();
        let mut failing_decoder = TestDecoder::counters([("first", 3), ("second", 4)]);
        failing_decoder.fail_end = true;

        assert!(batch.push_decoded_row(2, &mut failing_decoder).is_err());

        assert_eq!(batch.len(), 1);
        let retained = batch.row(0).expect("successful row must be retained");
        assert_eq!(retained.metadata(), &1);
        assert_eq!(retained.fields(), original_fields);
    }

    #[test]
    fn reset_and_schema_reuse_retain_compatible_allocations() {
        let schema = counter_schema();
        let mut batch = InMemoryStateRowBatch::<u8, u32>::new(&schema);
        batch.reserve_rows(4);
        let reserved_capacity = batch.capacity();
        let original_layout = batch.schema().clone().into_owned();
        let original_first_field = std::ptr::from_ref(
            batch
                .schema()
                .field("first")
                .expect("original schema must contain first"),
        );
        let mut decoder = TestDecoder::counters([("first", 1), ("second", 2)]);
        batch
            .push_decoded_row(1, &mut decoder)
            .expect("test row must decode");

        batch.reset_rows();
        assert!(batch.is_empty());
        assert!(batch.capacity() >= reserved_capacity);
        batch.reuse_for_schema(&schema);
        assert_eq!(batch.schema(), &original_layout);
        assert!(batch.capacity() >= reserved_capacity);
        let retained_first_field = std::ptr::from_ref(
            batch
                .schema()
                .field("first")
                .expect("reused schema must contain first"),
        );
        assert!(std::ptr::eq(original_first_field, retained_first_field));

        let replacement = Schema::from_fields([Field::monotonic_counter("replacement")]);
        batch.reuse_for_schema(&replacement);
        assert_eq!(batch.schema().len(), 1);
        assert!(batch.schema().field("replacement").is_some());
        assert!(batch.schema().field("first").is_none());
    }
}
