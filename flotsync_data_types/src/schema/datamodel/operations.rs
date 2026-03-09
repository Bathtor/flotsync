use super::{
    in_memory::{InMemoryFieldValue, InMemoryRow, validate_in_memory_field_value},
    snapshots::{
        SchemaSnapshotDecoder,
        SchemaSnapshotEncoder,
        SchemaSnapshotEncodingWriter,
        prepare_schema_snapshot_encoder,
    },
    validation::{
        ensure_counter_type,
        ensure_finite_state_value,
        ensure_nullable_basic_type,
        ensure_primitive_array_type,
        ensure_primitive_type,
    },
    *,
};
use crate::{DataOperation, IdWithIndex, any_data::UpdateOperation};
use std::{borrow::Cow, collections::HashSet, fmt, hash::Hash};

/// Explicit operation payload shapes for all schema data types.
#[derive(Clone, Debug, PartialEq)]
pub enum OperationValue<Id> {
    LatestValueWins(UpdateOperation<IdWithIndex<Id>, NullableBasicValue>),
    /// One logical linear-string change may expand to multiple chunk-local operations.
    LinearString(Vec<DataOperation<IdWithIndex<Id>, String>>),
    /// One logical linear-list change may expand to multiple chunk-local operations.
    LinearList(Vec<DataOperation<IdWithIndex<Id>, PrimitiveValueArray>>),
    MonotonicCounterIncrement(CounterValue),
    TotalOrderRegisterSet(PrimitiveValue),
    TotalOrderFiniteStateRegisterSet(NullablePrimitiveValue),
}

/// One operation value bound to a concrete schema field.
#[derive(Clone, Debug, PartialEq)]
pub struct OperationFieldValue<'a, Id> {
    pub field_name: Cow<'a, str>,
    pub value: OperationValue<Id>,
}

/// A schema operation against one row.
#[derive(Clone, Debug, PartialEq)]
pub struct SchemaOperation<'a, RowId, ChangeId> {
    pub change_id: ChangeId,
    pub operation: RowOperation<'a, RowId, ChangeId>,
}
impl<RowId, ChangeId> SchemaOperation<'_, RowId, ChangeId> {
    pub fn validate_against_schema(&self, schema: &Schema) -> Result<(), SchemaValueError> {
        self.operation.validate_against_schema(schema)
    }
}
impl<RowId, ChangeId> SchemaOperation<'_, RowId, ChangeId> {
    /// Materialize this operation into an owned `'static` representation.
    pub fn into_owned(self) -> SchemaOperation<'static, RowId, ChangeId>
    where
        ChangeId: Clone,
    {
        let operation = match self.operation {
            RowOperation::Insert { row_id, snapshot } => RowOperation::Insert {
                row_id,
                snapshot: snapshot.into_owned(),
            },
            RowOperation::Update { row_id, fields } => RowOperation::Update {
                row_id,
                fields: fields
                    .into_iter()
                    .map(|field| OperationFieldValue {
                        field_name: Cow::Owned(field.field_name.into_owned()),
                        value: field.value,
                    })
                    .collect(),
            },
            RowOperation::Delete { row_id } => RowOperation::Delete { row_id },
        };
        SchemaOperation {
            change_id: self.change_id,
            operation,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum RowOperation<'a, RowId, ChangeId> {
    /// Insert a new row identified with `row_id` using the given `snapshot` as initial state.
    Insert {
        row_id: RowId,
        snapshot: RowSnapshot<'a, ChangeId>,
    },
    /// Update the `fields` in the existing row identified by `row_id`.
    Update {
        row_id: RowId,
        /// Fields to change. Unspecified fields remain unchanged.
        fields: Vec<OperationFieldValue<'a, ChangeId>>,
    },
    /// Tombstone the row identified by `row_id`.
    Delete { row_id: RowId },
}
impl<RowId, ChangeId> RowOperation<'_, RowId, ChangeId> {
    pub fn validate_against_schema(&self, schema: &Schema) -> Result<(), SchemaValueError> {
        match self {
            RowOperation::Insert { snapshot, .. } => validate_schema_snapshot(schema, snapshot),
            RowOperation::Update { fields, .. } => validate_schema_operation_fields(schema, fields),
            RowOperation::Delete { .. } => Ok(()),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RowSnapshot<'a, ChangeId> {
    repr: RowSnapshotRepr<'a, ChangeId>,
}

#[derive(Clone, Debug, PartialEq)]
enum RowSnapshotRepr<'a, ChangeId> {
    BorrowedInMemory {
        field_names: &'a [String],
        row: &'a InMemoryRow<ChangeId>,
    },
    Owned {
        fields: Vec<(String, InMemoryFieldValue<ChangeId>)>,
    },
}

impl<'a, ChangeId> RowSnapshot<'a, ChangeId> {
    pub(crate) fn borrowed_in_memory(
        field_names: &'a [String],
        row: &'a InMemoryRow<ChangeId>,
    ) -> Self {
        Self {
            repr: RowSnapshotRepr::BorrowedInMemory { field_names, row },
        }
    }

    pub fn from_owned_fields(fields: Vec<(String, InMemoryFieldValue<ChangeId>)>) -> Self {
        Self {
            repr: RowSnapshotRepr::Owned { fields },
        }
    }

    /// Materialize this snapshot into an owned `'static` representation.
    pub fn into_owned(self) -> RowSnapshot<'static, ChangeId>
    where
        ChangeId: Clone,
    {
        RowSnapshot::from_owned_fields(self.into_owned_fields())
    }

    /// Materialize this snapshot as owned `(field_name, value)` pairs.
    pub fn into_owned_fields(self) -> Vec<(String, InMemoryFieldValue<ChangeId>)>
    where
        ChangeId: Clone,
    {
        match self.repr {
            RowSnapshotRepr::BorrowedInMemory { field_names, row } => field_names
                .iter()
                .cloned()
                .zip(row.fields.iter().cloned())
                .collect(),
            RowSnapshotRepr::Owned { fields } => fields,
        }
    }

    pub fn encode_snapshot<V>(
        &self,
        schema: &Schema,
        encoder: &mut V,
    ) -> Result<(), RowSnapshotEncodeError<V::Error>>
    where
        ChangeId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
        V: SchemaSnapshotEncoder<ChangeId>,
    {
        let mut writer =
            prepare_schema_snapshot_encoder(encoder, schema).context(SchemaVisitSnafu)?;
        self.encode_snapshot_fields(&mut writer)
            .context(SchemaVisitSnafu)?;
        writer.end().context(SchemaVisitSnafu)
    }

    pub fn decode_snapshot<D>(
        schema: &Schema,
        decoder: &mut D,
    ) -> Result<Self, RowSnapshotDecodeError<D::Error>>
    where
        ChangeId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
        D: SchemaSnapshotDecoder<ChangeId>,
    {
        decoder.begin(schema.columns.len()).context(DecoderSnafu)?;

        let mut fields = Vec::with_capacity(schema.columns.len());
        for field_name in schema.columns.keys() {
            let schema_field = schema
                .columns
                .get(field_name.as_str())
                .expect("field names and schema are in sync");
            let field_value = InMemoryFieldValue::decode_snapshot_field(
                field_name.as_str(),
                schema_field,
                decoder,
            )
            .context(InMemoryFieldValueSnafu)?;
            fields.push((field_name.clone(), field_value));
        }

        decoder.end().context(DecoderSnafu)?;
        Ok(Self::from_owned_fields(fields))
    }

    fn encode_snapshot_fields<V>(
        &self,
        writer: &mut SchemaSnapshotEncodingWriter<'_, ChangeId, V>,
    ) -> Result<(), SchemaVisitError<V::Error>>
    where
        ChangeId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
        V: SchemaSnapshotEncoder<ChangeId>,
    {
        match &self.repr {
            RowSnapshotRepr::BorrowedInMemory { field_names, row } => {
                row.encode_snapshot_fields(field_names, writer)
            }
            RowSnapshotRepr::Owned { fields } => {
                for (field_name, field_value) in fields {
                    field_value.encode_snapshot_field(field_name, writer)?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Snafu)]
pub enum RowSnapshotEncodeError<E>
where
    E: snafu::Error + Send + Sync + 'static,
{
    #[snafu(display("Failed to encode row snapshot against the schema."))]
    SchemaVisit { source: SchemaVisitError<E> },
}

#[derive(Debug, Snafu)]
pub enum RowSnapshotDecodeError<E>
where
    E: snafu::Error + Send + Sync + 'static,
{
    #[snafu(display("Decoded row snapshot value does not match the schema data type."))]
    InMemoryFieldValue {
        source: InMemoryDataSnapshotDecodeError<E>,
    },
    #[snafu(display("Row snapshot decoder failed."))]
    Decoder { source: E },
}

fn validate_schema_snapshot<ChangeId>(
    schema: &Schema,
    snapshot: &RowSnapshot<'_, ChangeId>,
) -> Result<(), SchemaValueError> {
    let mut seen_fields = HashSet::<String>::new();
    let mut missing_fields = schema.columns.keys().cloned().collect::<HashSet<_>>();

    match &snapshot.repr {
        RowSnapshotRepr::BorrowedInMemory { field_names, row } => {
            for (field_name, field_value) in field_names
                .iter()
                .map(String::as_str)
                .zip(row.fields.iter())
            {
                validate_snapshot_field(
                    schema,
                    &mut seen_fields,
                    &mut missing_fields,
                    field_name,
                    field_value,
                )?;
            }
        }
        RowSnapshotRepr::Owned { fields } => {
            for (field_name, field_value) in fields {
                validate_snapshot_field(
                    schema,
                    &mut seen_fields,
                    &mut missing_fields,
                    field_name.as_str(),
                    field_value,
                )?;
            }
        }
    }

    let missing_required_field = missing_fields.into_iter().find(|field_name| {
        let field = schema.columns.get(field_name.as_str());
        field.is_none_or(|field| field.default_value.is_none())
    });
    if let Some(field_name) = missing_required_field {
        return MissingFieldSnafu { field_name }.fail();
    }

    Ok(())
}

fn validate_snapshot_field<ChangeId>(
    schema: &Schema,
    seen_fields: &mut HashSet<String>,
    missing_fields: &mut HashSet<String>,
    field_name: &str,
    field_value: &InMemoryFieldValue<ChangeId>,
) -> Result<(), SchemaValueError> {
    ensure!(
        seen_fields.insert(field_name.to_owned()),
        DuplicateFieldSnafu {
            field_name: field_name.to_owned(),
        }
    );

    let Some(schema_field) = schema.columns.get(field_name) else {
        return UnknownFieldSnafu {
            field_name: field_name.to_owned(),
        }
        .fail();
    };
    missing_fields.remove(field_name);

    validate_in_memory_field_value(&schema_field.data_type, field_value).with_context(|_| {
        InvalidSnapshotFieldValueSnafu {
            field_name: field_name.to_owned(),
        }
    })?;
    Ok(())
}

/// Validate partial operation field payloads against a schema.
///
/// Any subset is allowed, but field names must exist and each field may appear at most once.
pub fn validate_schema_operation_fields<Id>(
    schema: &Schema,
    fields: &[OperationFieldValue<'_, Id>],
) -> Result<(), SchemaValueError> {
    let mut seen_fields = HashSet::<&str>::new();

    for field in fields {
        let field_name = field.field_name.as_ref();
        ensure!(
            seen_fields.insert(field_name),
            DuplicateFieldSnafu {
                field_name: field_name.to_owned(),
            }
        );
        let Some(schema_field) = schema.columns.get(field_name) else {
            return UnknownFieldSnafu {
                field_name: field_name.to_owned(),
            }
            .fail();
        };
        validate_operation_value_for_type(&schema_field.data_type, &field.value).with_context(
            |_| InvalidOperationFieldValueSnafu {
                field_name: field_name.to_owned(),
            },
        )?;
    }

    Ok(())
}

fn validate_operation_value_for_type<Id>(
    data_type: &ReplicatedDataType,
    value: &OperationValue<Id>,
) -> Result<(), DataModelValueError> {
    match (data_type, value) {
        (
            ReplicatedDataType::LatestValueWins { value_type },
            OperationValue::LatestValueWins(value),
        ) => ensure_nullable_basic_type(value_type, &value.value.as_ref()),
        (ReplicatedDataType::LinearString, OperationValue::LinearString(values)) => {
            validate_linear_operation_batch(values, |_| Ok(()))
        }
        (ReplicatedDataType::LinearList { value_type }, OperationValue::LinearList(values)) => {
            validate_linear_operation_batch(values, |value| {
                ensure_primitive_array_type(*value_type, value.primitive_type())
            })
        }
        (
            ReplicatedDataType::MonotonicCounter { small_range },
            OperationValue::MonotonicCounterIncrement(value),
        ) => ensure_counter_type(*small_range, value.as_ref()),
        (
            ReplicatedDataType::TotalOrderRegister { value_type, .. },
            OperationValue::TotalOrderRegisterSet(value),
        ) => ensure_primitive_type(*value_type, value.value_type()),
        (
            ReplicatedDataType::TotalOrderFiniteStateRegister { value_type, states },
            OperationValue::TotalOrderFiniteStateRegisterSet(value),
        ) => ensure_finite_state_value(*value_type, states, &value.as_ref()),
        _ => InvalidOperationValueForTypeSnafu.fail(),
    }
}

fn validate_linear_operation_batch<Id, Value>(
    values: &[DataOperation<Id, Value>],
    validate_insert: impl Fn(&Value) -> Result<(), DataModelValueError>,
) -> Result<(), DataModelValueError> {
    ensure!(!values.is_empty(), EmptyLinearOperationBatchSnafu);

    for value in values {
        match value {
            DataOperation::Insert { value, .. } => validate_insert(value)?,
            DataOperation::Delete { .. } => {}
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Direction, Field, PrimitiveType, ReplicatedDataType, Schema};
    use chrono::NaiveDate;
    use std::{assert_matches, collections::HashMap};

    fn schema_with_field(field_name: &str, data_type: ReplicatedDataType) -> Schema {
        Schema {
            columns: HashMap::from([(
                field_name.to_owned(),
                Field {
                    name: field_name.to_owned(),
                    data_type,
                    default_value: None,
                    metadata: HashMap::new(),
                },
            )]),
            metadata: HashMap::new(),
        }
    }

    fn indexed(id: u32, index: u32) -> IdWithIndex<u32> {
        IdWithIndex { id, index }
    }

    #[test]
    fn linear_string_batches_must_not_be_empty() {
        let schema = schema_with_field("title", ReplicatedDataType::LinearString);
        let operation = SchemaOperation {
            change_id: 0,
            operation: RowOperation::Update {
                row_id: 10u32,
                fields: vec![OperationFieldValue {
                    field_name: Cow::Borrowed("title"),
                    value: OperationValue::<u32>::LinearString(Vec::new()),
                }],
            },
        };

        let err = operation.validate_against_schema(&schema).unwrap_err();
        assert_matches!(
            err,
            SchemaValueError::InvalidOperationFieldValue {
                field_name,
                source: DataModelValueError::EmptyLinearOperationBatch,
            } if field_name == "title"
        );
    }

    #[test]
    fn linear_list_batches_validate_every_action() {
        let schema = schema_with_field(
            "numbers",
            ReplicatedDataType::LinearList {
                value_type: PrimitiveType::UInt,
            },
        );
        let operation = SchemaOperation {
            change_id: 0,
            operation: RowOperation::Update {
                row_id: 10u32,
                fields: vec![OperationFieldValue {
                    field_name: Cow::Borrowed("numbers"),
                    value: OperationValue::LinearList(vec![
                        DataOperation::Insert {
                            id: indexed(10, 0),
                            pred: indexed(0, 0),
                            succ: indexed(1, 0),
                            value: PrimitiveValueArray::UInt(vec![1, 2]),
                        },
                        DataOperation::Delete {
                            start: indexed(10, 0),
                            end: Some(indexed(10, 1)),
                        },
                    ]),
                }],
            },
        };

        operation.validate_against_schema(&schema).unwrap();
    }

    #[test]
    fn insert_snapshot_requires_all_fields() {
        let schema = Schema {
            columns: HashMap::from([
                (
                    "name".to_owned(),
                    Field {
                        name: "name".to_owned(),
                        data_type: ReplicatedDataType::LinearString,
                        default_value: None,
                        metadata: HashMap::new(),
                    },
                ),
                (
                    "priority".to_owned(),
                    Field {
                        name: "priority".to_owned(),
                        data_type: ReplicatedDataType::TotalOrderRegister {
                            value_type: PrimitiveType::UInt,
                            direction: Direction::Ascending,
                        },
                        default_value: None,
                        metadata: HashMap::new(),
                    },
                ),
            ]),
            metadata: HashMap::new(),
        };

        let snapshot = RowSnapshot::from_owned_fields(vec![(
            "name".to_owned(),
            InMemoryFieldValue::LinearString(crate::text::LinearString::with_value(
                "hello".to_owned(),
                1u32,
            )),
        )]);

        let err = validate_schema_snapshot(&schema, &snapshot).unwrap_err();
        assert_matches!(
            err,
            SchemaValueError::MissingField { field_name } if field_name == "priority"
        );
    }

    #[test]
    fn insert_snapshot_allows_missing_fields_with_defaults() {
        let schema = Schema {
            columns: HashMap::from([
                (
                    "name".to_owned(),
                    Field {
                        name: "name".to_owned(),
                        data_type: ReplicatedDataType::LinearString,
                        default_value: None,
                        metadata: HashMap::new(),
                    },
                ),
                (
                    "priority".to_owned(),
                    Field::total_order_register(
                        "priority",
                        PrimitiveType::UInt,
                        Direction::Ascending,
                    )
                    .with_default(7u64)
                    .unwrap(),
                ),
            ]),
            metadata: HashMap::new(),
        };

        let snapshot = RowSnapshot::from_owned_fields(vec![(
            "name".to_owned(),
            InMemoryFieldValue::LinearString(crate::text::LinearString::with_value(
                "hello".to_owned(),
                1u32,
            )),
        )]);

        validate_schema_snapshot(&schema, &snapshot).unwrap();
    }

    #[test]
    fn insert_snapshot_rejects_incompatible_values() {
        let schema = schema_with_field(
            "created_on",
            ReplicatedDataType::TotalOrderRegister {
                value_type: PrimitiveType::Date,
                direction: Direction::Ascending,
            },
        );

        let snapshot = RowSnapshot::from_owned_fields(vec![(
            "created_on".to_owned(),
            InMemoryFieldValue::<u32>::TotalOrderRegister(PrimitiveValue::UInt(1)),
        )]);

        let err = validate_schema_snapshot(&schema, &snapshot).unwrap_err();
        assert_matches!(
            err,
            SchemaValueError::InvalidSnapshotFieldValue {
                field_name,
                source: DataModelValueError::PrimitiveTypeMismatch {
                    expected: PrimitiveType::Date,
                    actual: PrimitiveType::UInt,
                },
            } if field_name == "created_on"
        );
    }

    #[test]
    fn insert_snapshot_accepts_matching_owned_values() {
        let schema = schema_with_field(
            "created_on",
            ReplicatedDataType::TotalOrderRegister {
                value_type: PrimitiveType::Date,
                direction: Direction::Ascending,
            },
        );

        let snapshot = RowSnapshot::from_owned_fields(vec![(
            "created_on".to_owned(),
            InMemoryFieldValue::<u32>::TotalOrderRegister(PrimitiveValue::Date(
                NaiveDate::from_ymd_opt(2026, 3, 4).unwrap(),
            )),
        )]);

        validate_schema_snapshot(&schema, &snapshot).unwrap();
    }
}
