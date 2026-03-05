use super::{
    validation::{ensure_counter_type, ensure_finite_state_value, ensure_primitive_type},
    *,
};
use crate::{
    DataOperation,
    Decode,
    DecodeValueError,
    IdWithIndex,
    OperationOutcome,
    RowOperations,
    TableOperations,
    any_data::{
        LinearLatestValueWins,
        UpdateOperation,
        list::{LinearList, ListOperation, linear_diff as linear_list_diff},
    },
    linear_data::LinearData,
    snapshot::{SnapshotHeader, SnapshotNode, SnapshotNodeRef, SnapshotReadError, SnapshotSink},
    text::{LinearString, linear_diff as linear_string_diff},
};
use chrono::NaiveDate;
use ordered_float::OrderedFloat;
use std::{borrow::Cow, collections::HashMap, fmt, hash::Hash, marker::PhantomData};

mod decode;
#[allow(
    unused_imports,
    reason = "Decoder module primarily contributes trait impls."
)]
pub use decode::*;

/// Storage-level, in-memory dataset for one schema.
///
/// The schema is immutable while this value exists.
/// Field names are mapped once to positional indices and rows only store per-field values.
#[derive(Clone, Debug, PartialEq)]
pub struct InMemoryData<RowId, OperationId>
where
    RowId: PartialEq + Eq + Hash,
{
    schema: Cow<'static, Schema>,
    field_names: Vec<String>,
    field_index_by_name: HashMap<String, usize>,
    row_id_map: HashMap<RowId, usize>,
    rows: Vec<InMemoryRow<OperationId>>,
}
impl<RowId, OperationId> InMemoryData<RowId, OperationId>
where
    RowId: PartialEq + Eq + Hash,
{
    /// Create an empty in-memory dataset for `schema`.
    pub fn new(schema: Cow<'static, Schema>) -> Self {
        let mut field_names = Vec::with_capacity(schema.columns.len());
        let mut field_index_by_name = HashMap::with_capacity(schema.columns.len());

        for field_name in schema.columns.keys() {
            let index = field_names.len();
            let field_name = field_name.to_owned();
            field_index_by_name.insert(field_name.clone(), index);
            field_names.push(field_name);
        }

        Self {
            schema,
            field_names,
            field_index_by_name,
            row_id_map: HashMap::new(),
            rows: Vec::new(),
        }
    }

    /// Create an empty in-memory dataset borrowing a `'static` schema.
    pub fn with_static_schema(schema: &'static Schema) -> Self {
        Self::new(Cow::Borrowed(schema))
    }

    /// Create an empty in-memory dataset owning `schema`.
    pub fn with_owned_schema(schema: Schema) -> Self {
        Self::new(Cow::Owned(schema))
    }

    /// Get the immutable schema associated with this dataset.
    pub fn schema(&self) -> &Schema {
        self.schema.as_ref()
    }

    /// Return the number of fields expected in every row.
    pub fn num_fields(&self) -> usize {
        self.field_names.len()
    }

    /// Resolve a field name to its storage index in row vectors.
    pub(crate) fn field_index(&self, field_name: &str) -> Option<usize> {
        self.field_index_by_name.get(field_name).copied()
    }

    /// Resolve a storage index to its field name.
    pub fn field_name(&self, field_index: usize) -> Option<&str> {
        self.field_names.get(field_index).map(String::as_str)
    }

    /// Iterate all field names in the dataset's storage order.
    pub fn field_names(&self) -> impl Iterator<Item = &str> {
        self.field_names.iter().map(String::as_str)
    }

    /// Return the number of stored rows.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Return `true` when no rows are stored.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Return the number of rows that are not tombstoned.
    pub fn num_active_rows(&self) -> usize {
        self.rows.iter().filter(|row| !row.deleted).count()
    }

    /// Validate and append one row represented by positional field values.
    ///
    /// Returns the inserted row index on success.
    #[allow(dead_code, reason = "Maybe remove this if it remains unused")]
    pub(crate) fn push_row_from_field_values(
        &mut self,
        fields: Vec<InMemoryFieldValue<OperationId>>,
    ) -> Result<usize, InMemoryDataError> {
        let row = self.row_from_field_values(fields)?;
        self.rows.push(row);
        Ok(self.rows.len() - 1)
    }

    /// Validate and append one row represented by `(field_name, value)` pairs.
    ///
    /// Field names must be unique and complete for the schema.
    /// Returns the inserted row index on success.
    pub fn push_row_from_named_fields<I, Name>(
        &mut self,
        fields: I,
    ) -> Result<usize, InMemoryDataError>
    where
        I: IntoIterator<Item = (Name, InMemoryFieldValue<OperationId>)>,
        Name: AsRef<str>,
    {
        let row = self.row_from_named_fields(fields)?;
        self.rows.push(row);
        Ok(self.rows.len() - 1)
    }

    /// Validate an existing row by index against this dataset's schema.
    pub fn validate_row(&self, row_index: usize) -> Result<(), InMemoryDataError> {
        let row = self.row(row_index)?;
        self.validate_row_value(row)
    }

    /// Get one field value by row index and field name.
    pub fn field(
        &self,
        row_index: usize,
        field_name: &str,
    ) -> Result<&InMemoryFieldValue<OperationId>, InMemoryDataError> {
        let field_index =
            self.field_index(field_name)
                .ok_or_else(|| InMemoryDataError::UnknownField {
                    field_name: field_name.to_owned(),
                })?;
        let row = self.row(row_index)?;
        Ok(&row.fields[field_index])
    }

    /// Get a mutable reference to one field value by row index and field name.
    pub fn field_mut(
        &mut self,
        row_index: usize,
        field_name: &str,
    ) -> Result<&mut InMemoryFieldValue<OperationId>, InMemoryDataError> {
        let field_index =
            self.field_index(field_name)
                .ok_or_else(|| InMemoryDataError::UnknownField {
                    field_name: field_name.to_owned(),
                })?;
        let row = self.row_mut(row_index)?;
        Ok(&mut row.fields[field_index])
    }

    /// Iterate `(field_name, field_value)` pairs for one row.
    pub fn iter_row_fields(
        &self,
        row_index: usize,
    ) -> Result<impl Iterator<Item = (&str, &InMemoryFieldValue<OperationId>)>, InMemoryDataError>
    {
        let row = self.row(row_index)?;
        debug_assert_eq!(self.num_fields(), row.field_count());
        Ok(self
            .field_names
            .iter()
            .map(String::as_str)
            .zip(row.fields.iter()))
    }

    /// Encode all rows as schema snapshots via a dataset-level encoder.
    ///
    /// Flow:
    /// 1. `encoder.begin(row_count)`
    /// 2. for each row: `begin_row` -> row snapshot encoding -> `end_row`
    /// 3. `encoder.end()`
    pub fn encode_data_snapshots<E>(
        &self,
        encoder: &mut E,
    ) -> Result<(), InMemoryDataSnapshotEncodeError<E::Error>>
    where
        OperationId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
        E: DataSnapshotEncoder<OperationId>,
    {
        encoder.begin(self.rows.len()).context(EncoderSnafu)?;

        for (row_index, row) in self.rows.iter().enumerate() {
            let mut row_encoder = encoder.begin_row(row_index).context(EncoderSnafu)?;
            row.encode_snapshot(self.schema(), &self.field_names, &mut row_encoder)
                .context(SchemaVisitSnafu)?;
            drop(row_encoder);
            encoder.end_row(row_index).context(EncoderSnafu)?;
        }

        encoder.end().context(EncoderSnafu)
    }

    /// Decode all rows from a dataset-level decoder into a new in-memory dataset.
    ///
    /// Each row is decoded lazily field-by-field and history-backed fields are reconstructed via
    /// each CRDT's `from_snapshot_nodes` API.
    pub fn decode_data_snapshots<D>(
        schema: Cow<'static, Schema>,
        decoder: &mut D,
    ) -> Result<Self, InMemoryDataSnapshotDecodeError<D::Error>>
    where
        OperationId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
        D: DataSnapshotDecoder<OperationId>,
    {
        let row_count = decoder.begin().context(DecoderSnafu)?;
        let mut data = Self::new(schema);
        data.rows.reserve(row_count);

        for row_index in 0..row_count {
            let mut row_decoder = decoder.begin_row(row_index).context(DecoderSnafu)?;
            let row =
                InMemoryRow::decode_snapshot(data.schema(), &data.field_names, &mut row_decoder)?;
            drop(row_decoder);
            decoder.end_row(row_index).context(DecoderSnafu)?;

            data.validate_row_value(&row).context(InMemoryDataSnafu)?;
            data.rows.push(row);
        }

        decoder.end().context(DecoderSnafu)?;
        Ok(data)
    }

    fn row_from_field_values(
        &self,
        fields: Vec<InMemoryFieldValue<OperationId>>,
    ) -> Result<InMemoryRow<OperationId>, InMemoryDataError> {
        let row = InMemoryRow::new(fields);
        self.validate_row_value(&row)?;
        Ok(row)
    }

    fn validate_row_value(&self, row: &InMemoryRow<OperationId>) -> Result<(), InMemoryDataError> {
        if row.field_count() != self.num_fields() {
            return Err(InMemoryDataError::FieldCountMismatch {
                expected: self.num_fields(),
                actual: row.field_count(),
            });
        }

        for (field_index, value) in row.fields.iter().enumerate() {
            let field_name = self.field_names[field_index].as_str();
            let schema_field = self
                .schema
                .columns
                .get(field_name)
                .expect("field index map and schema are in sync");
            validate_in_memory_field_value(&schema_field.data_type, value).context(
                InvalidFieldValueSnafu {
                    field_name: field_name.to_owned(),
                },
            )?;
        }

        Ok(())
    }

    fn row_from_named_fields<I, Name>(
        &self,
        fields: I,
    ) -> Result<InMemoryRow<OperationId>, InMemoryDataError>
    where
        I: IntoIterator<Item = (Name, InMemoryFieldValue<OperationId>)>,
        Name: AsRef<str>,
    {
        let mut row_slots: Vec<Option<InMemoryFieldValue<OperationId>>> =
            std::iter::repeat_with(|| None)
                .take(self.field_names.len())
                .collect();

        for (field_name, value) in fields {
            let field_name = field_name.as_ref();
            let Some(field_index) = self.field_index(field_name) else {
                return Err(InMemoryDataError::UnknownField {
                    field_name: field_name.to_owned(),
                });
            };
            if row_slots[field_index].is_some() {
                return Err(InMemoryDataError::DuplicateField {
                    field_name: field_name.to_owned(),
                });
            }

            let schema_field = self
                .schema
                .columns
                .get(field_name)
                .expect("field index map and schema are in sync");
            validate_in_memory_field_value(&schema_field.data_type, &value).context(
                InvalidFieldValueSnafu {
                    field_name: field_name.to_owned(),
                },
            )?;

            row_slots[field_index] = Some(value);
        }

        if let Some((missing_index, _)) = row_slots
            .iter()
            .enumerate()
            .find(|(_, slot)| slot.is_none())
        {
            return Err(InMemoryDataError::MissingField {
                field_name: self.field_names[missing_index].clone(),
            });
        }

        let fields = row_slots
            .into_iter()
            .map(|slot| slot.expect("all slots were checked for missing values"))
            .collect();
        Ok(InMemoryRow::new(fields))
    }

    fn row(&self, row_index: usize) -> Result<&InMemoryRow<OperationId>, InMemoryDataError> {
        self.rows
            .get(row_index)
            .ok_or(InMemoryDataError::UnknownRow { row_index })
    }

    fn row_mut(
        &mut self,
        row_index: usize,
    ) -> Result<&mut InMemoryRow<OperationId>, InMemoryDataError> {
        self.rows
            .get_mut(row_index)
            .ok_or(InMemoryDataError::UnknownRow { row_index })
    }
}
impl<RowId, OperationId> TableOperations<RowId, OperationId> for InMemoryData<RowId, OperationId>
where
    OperationId:
        Clone + fmt::Debug + fmt::Display + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
    RowId: Clone + PartialEq + Eq + Hash + fmt::Debug + fmt::Display,
{
    type Row<'a>
        = InMemoryDataRow<'a, RowId, OperationId>
    where
        Self: 'a;

    fn schema(&self) -> &Schema {
        self.schema.as_ref()
    }

    fn get_row(&self, row_id: &RowId) -> Option<Self::Row<'_>> {
        let index = self.row_id_map.get(row_id)?;
        assert!(
            self.rows.len() > *index,
            "Illegal InMemoryData state: Row with id {row_id} has index={index}, which does not exist"
        );
        Some(InMemoryDataRow {
            data: self,
            row_index: *index,
        })
    }

    fn insert_row<'values, I>(
        &mut self,
        operation_id: OperationId,
        row_id: RowId,
        initial_values: I,
    ) -> crate::OperationResult<SchemaOperation<'_, RowId, OperationId>>
    where
        I: IntoIterator<Item = crate::schema::InitialFieldValue<'values>>,
    {
        if self.row_id_map.contains_key(&row_id) {
            return crate::DuplicateRowIdSnafu {
                row_id: row_id.to_string(),
            }
            .fail();
        }

        let initial_values = collect_initial_values(self.schema(), initial_values)?;
        let mut fields = Vec::with_capacity(self.field_names.len());
        for field_name in &self.field_names {
            let schema_field = self
                .schema
                .columns
                .get(field_name.as_str())
                .expect("field_names and schema are in sync");
            let Some(target_value) = initial_values.get(field_name.as_str()) else {
                return Err(crate::OperationError::SchemaValue {
                    source: SchemaValueError::MissingField {
                        field_name: field_name.clone(),
                    },
                });
            };
            let field_value = build_initial_field_value(
                field_name.as_str(),
                &schema_field.data_type,
                target_value,
                &operation_id,
            )?;
            fields.push(field_value);
        }

        let row = InMemoryRow::new(fields);
        self.validate_row_value(&row)
            .context(crate::InMemoryDataSnafu)?;

        self.rows.push(row);
        let index = self.rows.len() - 1;
        self.row_id_map.insert(row_id.clone(), index);
        Ok(SchemaOperation {
            change_id: operation_id.clone(),
            operation: RowOperation::Insert {
                row_id,
                snapshot: RowSnapshot::borrowed_in_memory(&self.field_names, &self.rows[index]),
            },
        })
    }

    fn modify_row<'values, I>(
        &mut self,
        operation_id: OperationId,
        row_id: RowId,
        changed_values: I,
    ) -> crate::OperationResult<OperationOutcome<SchemaOperation<'_, RowId, OperationId>>>
    where
        I: IntoIterator<Item = crate::schema::PendingFieldUpdate<'values>>,
    {
        let row_index =
            *self
                .row_id_map
                .get(&row_id)
                .ok_or_else(|| crate::OperationError::UnknownRowId {
                    row_id: row_id.to_string(),
                })?;
        if self.rows[row_index].deleted {
            return Err(crate::OperationError::ModifyDeletedRow {
                row_id: row_id.to_string(),
            });
        }
        let changed_values = collect_pending_updates(self.schema(), changed_values)?;
        let mut fields = Vec::with_capacity(changed_values.len());

        'changed_fields: for (field_name, target_value) in changed_values {
            let field_index = self
                .field_index(field_name.as_str())
                .expect("field exists in schema and index map");
            let current_value = self.rows[row_index].fields[field_index].clone();
            let Some(operation_value) = build_field_operation(
                field_name.as_str(),
                &current_value,
                target_value,
                operation_id.clone(),
            )?
            else {
                // `modify_row` accepts desired end-state values. If computing the concrete CRDT
                // delta for one field produces no operation, we intentionally skip that field and
                // keep collecting changes for the remaining inputs.
                continue 'changed_fields;
            };
            apply_operation_value(
                &mut self.rows[row_index].fields[field_index],
                &operation_value,
            )?;
            fields.push(OperationFieldValue {
                field_name: Cow::Owned(field_name),
                value: operation_value,
            });
        }

        if fields.is_empty() {
            return Ok(OperationOutcome::NoChanges);
        }

        Ok(OperationOutcome::Applied(SchemaOperation {
            change_id: operation_id,
            operation: RowOperation::Update { row_id, fields },
        }))
    }

    fn delete_row(
        &mut self,
        operation_id: OperationId,
        row_id: RowId,
    ) -> crate::OperationResult<SchemaOperation<'_, RowId, OperationId>> {
        let row_index =
            *self
                .row_id_map
                .get(&row_id)
                .ok_or_else(|| crate::OperationError::UnknownRowId {
                    row_id: row_id.to_string(),
                })?;
        if self.rows[row_index].deleted {
            return Err(crate::OperationError::UnknownRowId {
                row_id: row_id.to_string(),
            });
        }
        self.rows[row_index].deleted = true;

        Ok(SchemaOperation {
            change_id: operation_id,
            operation: RowOperation::Delete { row_id },
        })
    }
}

fn collect_initial_values<'a>(
    schema: &Schema,
    initial_values: impl IntoIterator<Item = crate::schema::InitialFieldValue<'a>>,
) -> crate::OperationResult<HashMap<String, super::super::public_api::FieldTargetValue>> {
    let mut collected = HashMap::new();
    for initial_value in initial_values {
        let field_name = initial_value.field_name();
        let Some(schema_field) = schema.columns.get(field_name) else {
            return Err(crate::OperationError::SchemaValue {
                source: SchemaValueError::UnknownField {
                    field_name: field_name.to_owned(),
                },
            });
        };
        if schema_field.data_type != *initial_value.expected_type() {
            return Err(crate::OperationError::SchemaValue {
                source: SchemaValueError::InvalidSnapshotFieldValue {
                    field_name: field_name.to_owned(),
                    source: DataModelValueError::InvalidSnapshotValueForType,
                },
            });
        }
        collected.insert(field_name.to_owned(), initial_value.value().clone());
    }
    Ok(collected)
}

fn collect_pending_updates<'a>(
    schema: &Schema,
    changed_values: impl IntoIterator<Item = crate::schema::PendingFieldUpdate<'a>>,
) -> crate::OperationResult<HashMap<String, super::super::public_api::FieldTargetValue>> {
    let mut collected = HashMap::new();
    for changed_value in changed_values {
        let field_name = changed_value.field_name();
        let Some(schema_field) = schema.columns.get(field_name) else {
            return Err(crate::OperationError::SchemaValue {
                source: SchemaValueError::UnknownField {
                    field_name: field_name.to_owned(),
                },
            });
        };
        if schema_field.data_type != *changed_value.expected_type() {
            return Err(crate::OperationError::SchemaValue {
                source: SchemaValueError::InvalidOperationFieldValue {
                    field_name: field_name.to_owned(),
                    source: DataModelValueError::InvalidOperationValueForType,
                },
            });
        }
        collected.insert(field_name.to_owned(), changed_value.value().clone());
    }
    Ok(collected)
}

fn build_initial_field_value<OperationId>(
    field_name: &str,
    data_type: &ReplicatedDataType,
    target_value: &super::super::public_api::FieldTargetValue,
    operation_id: &OperationId,
) -> crate::OperationResult<InMemoryFieldValue<OperationId>>
where
    OperationId:
        Clone + fmt::Debug + fmt::Display + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
{
    match (data_type, target_value) {
        (
            ReplicatedDataType::LatestValueWins { value_type },
            super::super::public_api::FieldTargetValue::NullableBasic(value),
        ) => build_initial_lww_value(field_name, value_type, value, operation_id),
        (
            ReplicatedDataType::LinearString,
            super::super::public_api::FieldTargetValue::String(value),
        ) => Ok(InMemoryFieldValue::LinearString(LinearString::with_value(
            value.clone(),
            operation_id.clone(),
        ))),
        (
            ReplicatedDataType::LinearList {
                value_type: PrimitiveType::String,
            },
            super::super::public_api::FieldTargetValue::PrimitiveArray(value),
        ) => Ok(InMemoryFieldValue::LinearList(LinearListValue::String(
            LinearList::with_values(
                decode_list_string(value.clone()).map_err(operation_invalid_value)?,
                operation_id.clone(),
            ),
        ))),
        (
            ReplicatedDataType::LinearList {
                value_type: PrimitiveType::UInt,
            },
            super::super::public_api::FieldTargetValue::PrimitiveArray(value),
        ) => Ok(InMemoryFieldValue::LinearList(LinearListValue::UInt(
            LinearList::with_values(
                decode_list_uint(value.clone()).map_err(operation_invalid_value)?,
                operation_id.clone(),
            ),
        ))),
        (
            ReplicatedDataType::LinearList {
                value_type: PrimitiveType::Int,
            },
            super::super::public_api::FieldTargetValue::PrimitiveArray(value),
        ) => Ok(InMemoryFieldValue::LinearList(LinearListValue::Int(
            LinearList::with_values(
                decode_list_int(value.clone()).map_err(operation_invalid_value)?,
                operation_id.clone(),
            ),
        ))),
        (
            ReplicatedDataType::LinearList {
                value_type: PrimitiveType::Byte,
            },
            super::super::public_api::FieldTargetValue::PrimitiveArray(value),
        ) => Ok(InMemoryFieldValue::LinearList(LinearListValue::Byte(
            LinearList::with_values(
                decode_list_byte(value.clone()).map_err(operation_invalid_value)?,
                operation_id.clone(),
            ),
        ))),
        (
            ReplicatedDataType::LinearList {
                value_type: PrimitiveType::Float,
            },
            super::super::public_api::FieldTargetValue::PrimitiveArray(value),
        ) => Ok(InMemoryFieldValue::LinearList(LinearListValue::Float(
            LinearList::with_values(
                decode_list_float(value.clone()).map_err(operation_invalid_value)?,
                operation_id.clone(),
            ),
        ))),
        (
            ReplicatedDataType::LinearList {
                value_type: PrimitiveType::Boolean,
            },
            super::super::public_api::FieldTargetValue::PrimitiveArray(value),
        ) => Ok(InMemoryFieldValue::LinearList(LinearListValue::Boolean(
            LinearList::with_values(
                decode_list_boolean(value.clone()).map_err(operation_invalid_value)?,
                operation_id.clone(),
            ),
        ))),
        (
            ReplicatedDataType::LinearList {
                value_type: PrimitiveType::Binary,
            },
            super::super::public_api::FieldTargetValue::PrimitiveArray(value),
        ) => Ok(InMemoryFieldValue::LinearList(LinearListValue::Binary(
            LinearList::with_values(
                decode_list_binary(value.clone()).map_err(operation_invalid_value)?,
                operation_id.clone(),
            ),
        ))),
        (
            ReplicatedDataType::LinearList {
                value_type: PrimitiveType::Date,
            },
            super::super::public_api::FieldTargetValue::PrimitiveArray(value),
        ) => Ok(InMemoryFieldValue::LinearList(LinearListValue::Date(
            LinearList::with_values(
                decode_list_date(value.clone()).map_err(operation_invalid_value)?,
                operation_id.clone(),
            ),
        ))),
        (
            ReplicatedDataType::LinearList {
                value_type: PrimitiveType::Timestamp,
            },
            super::super::public_api::FieldTargetValue::PrimitiveArray(value),
        ) => Ok(InMemoryFieldValue::LinearList(LinearListValue::Timestamp(
            LinearList::with_values(
                decode_list_timestamp(value.clone()).map_err(operation_invalid_value)?,
                operation_id.clone(),
            ),
        ))),
        (
            ReplicatedDataType::MonotonicCounter { .. },
            super::super::public_api::FieldTargetValue::Counter(value),
        ) => Ok(InMemoryFieldValue::MonotonicCounter(*value)),
        (
            ReplicatedDataType::TotalOrderRegister { .. },
            super::super::public_api::FieldTargetValue::Primitive(value),
        ) => Ok(InMemoryFieldValue::TotalOrderRegister(value.clone())),
        (
            ReplicatedDataType::TotalOrderFiniteStateRegister { .. },
            super::super::public_api::FieldTargetValue::NullablePrimitive(value),
        ) => Ok(InMemoryFieldValue::TotalOrderFiniteStateRegister(
            value.clone(),
        )),
        _ => crate::InternalOperationSnafu {
            context: format!(
                "Field '{field_name}' had a builder value incompatible with its schema type."
            ),
        }
        .fail(),
    }
}

fn build_initial_lww_value<OperationId>(
    field_name: &str,
    value_type: &NullableBasicDataType,
    value: &NullableBasicValue,
    operation_id: &OperationId,
) -> crate::OperationResult<InMemoryFieldValue<OperationId>>
where
    OperationId:
        Clone + fmt::Debug + fmt::Display + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
{
    let begin_id = IdWithIndex::zero(operation_id.clone());
    let value_id = begin_id.increment();
    let end_id = value_id.increment();
    let ids = [begin_id, value_id, end_id];

    macro_rules! build_lww_variant {
        ($variant:ident, $value:expr) => {{
            let register = LinearLatestValueWins::new($value, ids);
            Ok(InMemoryFieldValue::LatestValueWins(
                LinearLatestValueWinsValue::$variant(register),
            ))
        }};
    }

    match (value_type, value) {
        (
            NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::String)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::String(value))),
        ) => build_lww_variant!(String, value.clone()),
        (
            NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::UInt)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::UInt(value))),
        ) => build_lww_variant!(UInt, *value),
        (
            NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Int)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::Int(value))),
        ) => build_lww_variant!(Int, *value),
        (
            NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Byte)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::Byte(value))),
        ) => build_lww_variant!(Byte, *value),
        (
            NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Float)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::Float(value))),
        ) => build_lww_variant!(Float, *value),
        (
            NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Boolean)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::Boolean(value))),
        ) => build_lww_variant!(Boolean, *value),
        (
            NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Binary)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::Binary(value))),
        ) => build_lww_variant!(Binary, value.clone()),
        (
            NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Date)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::Date(value))),
        ) => build_lww_variant!(Date, *value),
        (
            NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Timestamp)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::Timestamp(value))),
        ) => build_lww_variant!(Timestamp, *value),
        (
            NullableBasicDataType::NonNull(BasicDataType::Array(array_type)),
            NullableBasicValue::Value(BasicValue::Array(value)),
        ) => match array_type.element_type {
            PrimitiveType::String => match value {
                PrimitiveValueArray::String(value) => build_lww_variant!(StringArray, value.clone()),
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
            PrimitiveType::UInt => match value {
                PrimitiveValueArray::UInt(value) => build_lww_variant!(UIntArray, value.clone()),
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
            PrimitiveType::Int => match value {
                PrimitiveValueArray::Int(value) => build_lww_variant!(IntArray, value.clone()),
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
            PrimitiveType::Byte => match value {
                PrimitiveValueArray::Byte(value) => build_lww_variant!(ByteArray, value.clone()),
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
            PrimitiveType::Float => match value {
                PrimitiveValueArray::Float(value) => build_lww_variant!(FloatArray, value.clone()),
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
            PrimitiveType::Boolean => match value {
                PrimitiveValueArray::Boolean(value) => {
                    build_lww_variant!(BooleanArray, value.clone())
                }
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
            PrimitiveType::Binary => match value {
                PrimitiveValueArray::Binary(value) => build_lww_variant!(BinaryArray, value.clone()),
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
            PrimitiveType::Date => match value {
                PrimitiveValueArray::Date(value) => build_lww_variant!(DateArray, value.clone()),
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
            PrimitiveType::Timestamp => match value {
                PrimitiveValueArray::Timestamp(value) => {
                    build_lww_variant!(TimestampArray, value.clone())
                }
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
        },
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::String)),
            NullableBasicValue::Null,
        ) => build_lww_variant!(NullableString, None::<String>),
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::UInt)),
            NullableBasicValue::Null,
        ) => build_lww_variant!(NullableUInt, None::<u64>),
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Int)),
            NullableBasicValue::Null,
        ) => build_lww_variant!(NullableInt, None::<i64>),
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Byte)),
            NullableBasicValue::Null,
        ) => build_lww_variant!(NullableByte, None::<u8>),
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Float)),
            NullableBasicValue::Null,
        ) => build_lww_variant!(NullableFloat, None::<OrderedFloat<f64>>),
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Boolean)),
            NullableBasicValue::Null,
        ) => build_lww_variant!(NullableBoolean, None::<bool>),
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Binary)),
            NullableBasicValue::Null,
        ) => build_lww_variant!(NullableBinary, None::<Vec<u8>>),
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Date)),
            NullableBasicValue::Null,
        ) => build_lww_variant!(NullableDate, None::<NaiveDate>),
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Timestamp)),
            NullableBasicValue::Null,
        ) => build_lww_variant!(NullableTimestamp, None::<UnixTimestamp>),
        (
            NullableBasicDataType::Nullable(BasicDataType::Array(array_type)),
            NullableBasicValue::Null,
        ) => match array_type.element_type {
            PrimitiveType::String => build_lww_variant!(NullableStringArray, None::<Vec<String>>),
            PrimitiveType::UInt => build_lww_variant!(NullableUIntArray, None::<Vec<u64>>),
            PrimitiveType::Int => build_lww_variant!(NullableIntArray, None::<Vec<i64>>),
            PrimitiveType::Byte => build_lww_variant!(NullableByteArray, None::<Vec<u8>>),
            PrimitiveType::Float => {
                build_lww_variant!(NullableFloatArray, None::<Vec<OrderedFloat<f64>>>)
            }
            PrimitiveType::Boolean => build_lww_variant!(NullableBooleanArray, None::<Vec<bool>>),
            PrimitiveType::Binary => build_lww_variant!(NullableBinaryArray, None::<Vec<Vec<u8>>>),
            PrimitiveType::Date => build_lww_variant!(NullableDateArray, None::<Vec<NaiveDate>>),
            PrimitiveType::Timestamp => {
                build_lww_variant!(NullableTimestampArray, None::<Vec<UnixTimestamp>>)
            }
        },
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::String)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::String(value))),
        ) => build_lww_variant!(NullableString, Some(value.clone())),
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::UInt)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::UInt(value))),
        ) => build_lww_variant!(NullableUInt, Some(*value)),
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Int)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::Int(value))),
        ) => build_lww_variant!(NullableInt, Some(*value)),
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Byte)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::Byte(value))),
        ) => build_lww_variant!(NullableByte, Some(*value)),
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Float)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::Float(value))),
        ) => build_lww_variant!(NullableFloat, Some(*value)),
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Boolean)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::Boolean(value))),
        ) => build_lww_variant!(NullableBoolean, Some(*value)),
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Binary)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::Binary(value))),
        ) => build_lww_variant!(NullableBinary, Some(value.clone())),
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Date)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::Date(value))),
        ) => build_lww_variant!(NullableDate, Some(*value)),
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Timestamp)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::Timestamp(value))),
        ) => build_lww_variant!(NullableTimestamp, Some(*value)),
        (
            NullableBasicDataType::Nullable(BasicDataType::Array(array_type)),
            NullableBasicValue::Value(BasicValue::Array(value)),
        ) => match array_type.element_type {
            PrimitiveType::String => match value {
                PrimitiveValueArray::String(value) => {
                    build_lww_variant!(NullableStringArray, Some(value.clone()))
                }
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
            PrimitiveType::UInt => match value {
                PrimitiveValueArray::UInt(value) => {
                    build_lww_variant!(NullableUIntArray, Some(value.clone()))
                }
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
            PrimitiveType::Int => match value {
                PrimitiveValueArray::Int(value) => {
                    build_lww_variant!(NullableIntArray, Some(value.clone()))
                }
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
            PrimitiveType::Byte => match value {
                PrimitiveValueArray::Byte(value) => {
                    build_lww_variant!(NullableByteArray, Some(value.clone()))
                }
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
            PrimitiveType::Float => match value {
                PrimitiveValueArray::Float(value) => {
                    build_lww_variant!(NullableFloatArray, Some(value.clone()))
                }
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
            PrimitiveType::Boolean => match value {
                PrimitiveValueArray::Boolean(value) => {
                    build_lww_variant!(NullableBooleanArray, Some(value.clone()))
                }
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
            PrimitiveType::Binary => match value {
                PrimitiveValueArray::Binary(value) => {
                    build_lww_variant!(NullableBinaryArray, Some(value.clone()))
                }
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
            PrimitiveType::Date => match value {
                PrimitiveValueArray::Date(value) => {
                    build_lww_variant!(NullableDateArray, Some(value.clone()))
                }
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
            PrimitiveType::Timestamp => match value {
                PrimitiveValueArray::Timestamp(value) => {
                    build_lww_variant!(NullableTimestampArray, Some(value.clone()))
                }
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
        },
        _ => crate::InternalOperationSnafu {
            context: format!(
                "Field '{field_name}' had a LatestValueWins payload incompatible with its schema type."
            ),
        }
        .fail(),
    }
}

fn build_field_operation<OperationId>(
    field_name: &str,
    current_value: &InMemoryFieldValue<OperationId>,
    target_value: super::super::public_api::FieldTargetValue,
    operation_id: OperationId,
) -> crate::OperationResult<Option<OperationValue<OperationId>>>
where
    OperationId:
        Clone + fmt::Debug + fmt::Display + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
{
    match (current_value, target_value) {
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::String(current)),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_string(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::UInt(current)),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_uint(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::Int(current)),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_int(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::Byte(current)),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_byte(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::Float(current)),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_float(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::Boolean(current)),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_boolean(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::Binary(current)),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_binary(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::Date(current)),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_date(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::Timestamp(current)),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_timestamp(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::StringArray(current)),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_string_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::UIntArray(current)),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_uint_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::IntArray(current)),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_int_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::ByteArray(current)),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_byte_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::FloatArray(current)),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_float_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::BooleanArray(current)),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_boolean_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::BinaryArray(current)),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_binary_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::DateArray(current)),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_date_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::TimestampArray(
                current,
            )),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_timestamp_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableString(
                current,
            )),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_string(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableUInt(current)),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_uint(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableInt(current)),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_int(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableByte(current)),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_byte(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableFloat(current)),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_float(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableBoolean(
                current,
            )),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_boolean(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableBinary(
                current,
            )),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_binary(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableDate(current)),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_date(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableTimestamp(
                current,
            )),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_timestamp(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableStringArray(
                current,
            )),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_string_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableUIntArray(
                current,
            )),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_uint_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableIntArray(
                current,
            )),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_int_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableByteArray(
                current,
            )),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_byte_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableFloatArray(
                current,
            )),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_float_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableBooleanArray(
                current,
            )),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_boolean_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableBinaryArray(
                current,
            )),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_binary_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableDateArray(
                current,
            )),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_date_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LatestValueWins(
                LinearLatestValueWinsValue::NullableTimestampArray(current),
            ),
            super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_timestamp_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldValue::LinearString(current),
            super::super::public_api::FieldTargetValue::String(target),
        ) => {
            if current.to_string() == target {
                return Ok(None);
            }
            let mut id_generator = std::iter::once(operation_id);
            let diff = linear_string_diff(current, &target, &mut id_generator)
                .context(crate::LinearStringDiffSnafu)?;
            let operations = diff.into_operations();
            if operations.is_empty() {
                Ok(None)
            } else {
                Ok(Some(OperationValue::LinearString(operations)))
            }
        }
        (
            InMemoryFieldValue::LinearList(current),
            super::super::public_api::FieldTargetValue::PrimitiveArray(target),
        ) => build_linear_list_operation(current, target, operation_id)
            .map(|value| value.map(OperationValue::LinearList)),
        (
            InMemoryFieldValue::MonotonicCounter(current),
            super::super::public_api::FieldTargetValue::Counter(target),
        ) => build_counter_operation(field_name, *current, target)
            .map(|value| value.map(OperationValue::MonotonicCounterIncrement)),
        (
            InMemoryFieldValue::TotalOrderRegister(current),
            super::super::public_api::FieldTargetValue::Primitive(target),
        ) => {
            if *current == target {
                Ok(None)
            } else {
                Ok(Some(OperationValue::TotalOrderRegisterSet(target)))
            }
        }
        (
            InMemoryFieldValue::TotalOrderFiniteStateRegister(current),
            super::super::public_api::FieldTargetValue::NullablePrimitive(target),
        ) => {
            if *current == target {
                Ok(None)
            } else {
                Ok(Some(OperationValue::TotalOrderFiniteStateRegisterSet(
                    target,
                )))
            }
        }
        _ => crate::InternalOperationSnafu {
            context: format!(
                "Field '{field_name}' had an unexpected target value for its in-memory type."
            ),
        }
        .fail(),
    }
}

fn build_lww_operation<OperationId, T>(
    current: &LinearLatestValueWins<IdWithIndex<OperationId>, T>,
    target: T,
    operation_id: OperationId,
) -> crate::OperationResult<Option<UpdateOperation<IdWithIndex<OperationId>, NullableBasicValue>>>
where
    OperationId:
        Clone + fmt::Debug + fmt::Display + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
    T: Clone + fmt::Debug + PartialEq,
    NullableBasicValue: From<T>,
{
    if current.content() == &target {
        Ok(None)
    } else {
        let indexed_operation_id = IdWithIndex::zero(operation_id);
        let operation = current.update_operation(indexed_operation_id, target);
        Ok(Some(UpdateOperation {
            id: operation.id,
            pred: operation.pred,
            succ: operation.succ,
            value: operation.value.into(),
        }))
    }
}

fn build_counter_operation(
    field_name: &str,
    current: CounterValue,
    target: CounterValue,
) -> crate::OperationResult<Option<CounterValue>> {
    match (current, target) {
        (CounterValue::Byte(current), CounterValue::Byte(target)) => {
            if target < current {
                return crate::UnsupportedOperationVariantSnafu {
                    explanation: Cow::Owned(format!(
                        "Field '{field_name}' is monotonic and cannot decrease from {current} to {target}."
                    )),
                }
                .fail();
            }
            if target == current {
                Ok(None)
            } else {
                Ok(Some(CounterValue::Byte(target - current)))
            }
        }
        (CounterValue::UInt(current), CounterValue::UInt(target)) => {
            if target < current {
                return crate::UnsupportedOperationVariantSnafu {
                    explanation: Cow::Owned(format!(
                        "Field '{field_name}' is monotonic and cannot decrease from {current} to {target}."
                    )),
                }
                .fail();
            }
            if target == current {
                Ok(None)
            } else {
                Ok(Some(CounterValue::UInt(target - current)))
            }
        }
        _ => crate::InternalOperationSnafu {
            context: format!("Field '{field_name}' had mismatched counter representations."),
        }
        .fail(),
    }
}

fn build_linear_list_operation<OperationId>(
    current: &LinearListValue<OperationId>,
    target: PrimitiveValueArray,
    operation_id: OperationId,
) -> crate::OperationResult<Option<Vec<DataOperation<IdWithIndex<OperationId>, PrimitiveValueArray>>>>
where
    OperationId:
        Clone + fmt::Debug + fmt::Display + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
{
    macro_rules! build_linear_list_op {
        ($current:expr, $target:expr, $decode:ident) => {{
            let target_values = $decode($target).map_err(operation_invalid_value)?;
            let diff = linear_list_diff($current, &target_values, operation_id.clone())
                .context(crate::LinearListDiffSnafu)?;
            let operations = diff.into_operations();
            if operations.is_empty() {
                Ok(None)
            } else {
                Ok(Some(
                    operations
                        .into_iter()
                        .map(|op| op.map_value(|values| values.into()))
                        .collect(),
                ))
            }
        }};
    }

    match current {
        LinearListValue::String(current) => {
            build_linear_list_op!(current, target, decode_list_string)
        }
        LinearListValue::UInt(current) => build_linear_list_op!(current, target, decode_list_uint),
        LinearListValue::Int(current) => build_linear_list_op!(current, target, decode_list_int),
        LinearListValue::Byte(current) => build_linear_list_op!(current, target, decode_list_byte),
        LinearListValue::Float(current) => {
            build_linear_list_op!(current, target, decode_list_float)
        }
        LinearListValue::Boolean(current) => {
            build_linear_list_op!(current, target, decode_list_boolean)
        }
        LinearListValue::Binary(current) => {
            build_linear_list_op!(current, target, decode_list_binary)
        }
        LinearListValue::Date(current) => build_linear_list_op!(current, target, decode_list_date),
        LinearListValue::Timestamp(current) => {
            build_linear_list_op!(current, target, decode_list_timestamp)
        }
    }
}

fn map_data_operation_value<Id, InputValue, OutputValue, E>(
    operation: DataOperation<Id, InputValue>,
    map_value: impl Fn(InputValue) -> Result<OutputValue, E>,
) -> Result<DataOperation<Id, OutputValue>, E> {
    match operation {
        DataOperation::Insert {
            id,
            pred,
            succ,
            value,
        } => Ok(DataOperation::Insert {
            id,
            pred,
            succ,
            value: map_value(value)?,
        }),
        DataOperation::Delete { start, end } => Ok(DataOperation::Delete { start, end }),
    }
}

fn apply_operation_value<OperationId>(
    field_value: &mut InMemoryFieldValue<OperationId>,
    operation_value: &OperationValue<OperationId>,
) -> crate::OperationResult<()>
where
    OperationId:
        Clone + fmt::Debug + fmt::Display + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
{
    macro_rules! apply_lww_operation {
        ($current:expr, $operation:expr, $decode:ident) => {{
            let operation = UpdateOperation {
                id: $operation.id.clone(),
                pred: $operation.pred.clone(),
                succ: $operation.succ.clone(),
                value: $decode($operation.value.clone()).map_err(operation_invalid_value)?,
            };
            $current.apply_operation(operation).map_err(|_| {
                crate::OperationError::InternalOperation {
                    context: "Applying a generated LatestValueWins operation failed.".to_owned(),
                    location: snafu::Location::default(),
                }
            })
        }};
    }

    macro_rules! apply_list_operations {
        ($current:expr, $operations:expr, $decode:ident) => {{
            for operation in $operations.iter().cloned() {
                let operation = map_data_operation_value(operation, $decode)
                    .map_err(operation_invalid_value)?;
                let operation = ListOperation::from_operation(operation);
                if $current.apply_operation(operation).is_err() {
                    return crate::InternalOperationSnafu {
                        context: "Applying a generated LinearList operation failed.".to_owned(),
                    }
                    .fail();
                }
            }
            Ok(())
        }};
    }

    match (field_value, operation_value) {
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::String(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_string),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::UInt(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_uint),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::Int(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_int),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::Byte(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_byte),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::Float(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_float),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::Boolean(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_boolean),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::Binary(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_binary),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::Date(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_date),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::Timestamp(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_timestamp),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::StringArray(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_string_array),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::UIntArray(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_uint_array),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::IntArray(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_int_array),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::ByteArray(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_byte_array),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::FloatArray(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_float_array),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::BooleanArray(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_boolean_array),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::BinaryArray(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_binary_array),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::DateArray(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_date_array),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::TimestampArray(
                current,
            )),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_timestamp_array),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableString(
                current,
            )),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_string),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableUInt(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_uint),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableInt(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_int),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableByte(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_byte),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableFloat(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_float),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableBoolean(
                current,
            )),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_boolean),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableBinary(
                current,
            )),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_binary),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableDate(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_date),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableTimestamp(
                current,
            )),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_timestamp),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableStringArray(
                current,
            )),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_string_array),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableUIntArray(
                current,
            )),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_uint_array),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableIntArray(
                current,
            )),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_int_array),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableByteArray(
                current,
            )),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_byte_array),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableFloatArray(
                current,
            )),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_float_array),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableBooleanArray(
                current,
            )),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_boolean_array),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableBinaryArray(
                current,
            )),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_binary_array),
        (
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableDateArray(
                current,
            )),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_date_array),
        (
            InMemoryFieldValue::LatestValueWins(
                LinearLatestValueWinsValue::NullableTimestampArray(current),
            ),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_timestamp_array),
        (InMemoryFieldValue::LinearString(current), OperationValue::LinearString(operations)) => {
            for operation in operations.iter().cloned() {
                if current.apply_operation(operation).is_err() {
                    return crate::InternalOperationSnafu {
                        context: "Applying a generated LinearString operation failed.".to_owned(),
                    }
                    .fail();
                }
            }
            Ok(())
        }
        (
            InMemoryFieldValue::LinearList(LinearListValue::String(current)),
            OperationValue::LinearList(operations),
        ) => apply_list_operations!(current, operations, decode_list_string),
        (
            InMemoryFieldValue::LinearList(LinearListValue::UInt(current)),
            OperationValue::LinearList(operations),
        ) => apply_list_operations!(current, operations, decode_list_uint),
        (
            InMemoryFieldValue::LinearList(LinearListValue::Int(current)),
            OperationValue::LinearList(operations),
        ) => apply_list_operations!(current, operations, decode_list_int),
        (
            InMemoryFieldValue::LinearList(LinearListValue::Byte(current)),
            OperationValue::LinearList(operations),
        ) => apply_list_operations!(current, operations, decode_list_byte),
        (
            InMemoryFieldValue::LinearList(LinearListValue::Float(current)),
            OperationValue::LinearList(operations),
        ) => apply_list_operations!(current, operations, decode_list_float),
        (
            InMemoryFieldValue::LinearList(LinearListValue::Boolean(current)),
            OperationValue::LinearList(operations),
        ) => apply_list_operations!(current, operations, decode_list_boolean),
        (
            InMemoryFieldValue::LinearList(LinearListValue::Binary(current)),
            OperationValue::LinearList(operations),
        ) => apply_list_operations!(current, operations, decode_list_binary),
        (
            InMemoryFieldValue::LinearList(LinearListValue::Date(current)),
            OperationValue::LinearList(operations),
        ) => apply_list_operations!(current, operations, decode_list_date),
        (
            InMemoryFieldValue::LinearList(LinearListValue::Timestamp(current)),
            OperationValue::LinearList(operations),
        ) => apply_list_operations!(current, operations, decode_list_timestamp),
        (
            InMemoryFieldValue::MonotonicCounter(current),
            OperationValue::MonotonicCounterIncrement(delta),
        ) => {
            match (current, delta) {
                (CounterValue::Byte(current), CounterValue::Byte(delta)) => {
                    *current = current.saturating_add(*delta)
                }
                (CounterValue::UInt(current), CounterValue::UInt(delta)) => {
                    *current = current.saturating_add(*delta)
                }
                _ => {
                    return crate::InternalOperationSnafu {
                        context: "Applying a generated monotonic-counter increment failed due to a type mismatch.".to_owned(),
                    }
                    .fail();
                }
            }
            Ok(())
        }
        (
            InMemoryFieldValue::TotalOrderRegister(current),
            OperationValue::TotalOrderRegisterSet(value),
        ) => {
            *current = value.clone();
            Ok(())
        }
        (
            InMemoryFieldValue::TotalOrderFiniteStateRegister(current),
            OperationValue::TotalOrderFiniteStateRegisterSet(value),
        ) => {
            *current = value.clone();
            Ok(())
        }
        _ => crate::InternalOperationSnafu {
            context: "Applying a generated field operation failed due to a type mismatch."
                .to_owned(),
        }
        .fail(),
    }
}

fn operation_invalid_value(source: DataModelValueError) -> crate::OperationError {
    crate::OperationError::SchemaValue {
        source: SchemaValueError::InvalidOperationFieldValue {
            field_name: "<derived>".to_owned(),
            source,
        },
    }
}

#[derive(Copy)]
pub struct InMemoryDataRow<'a, RowId, OperationId>
where
    RowId: PartialEq + Eq + Hash,
{
    data: &'a InMemoryData<RowId, OperationId>,
    row_index: usize,
}
impl<'a, RowId, OperationId> Clone for InMemoryDataRow<'a, RowId, OperationId>
where
    RowId: PartialEq + Eq + Hash,
{
    fn clone(&self) -> Self {
        Self {
            data: self.data,
            row_index: self.row_index,
        }
    }
}
impl<'a, RowId, OperationId> RowOperations<OperationId> for InMemoryDataRow<'a, RowId, OperationId>
where
    RowId: PartialEq + Eq + Hash,
{
    fn get_field(&self, field_name: &str) -> Option<&InMemoryFieldValue<OperationId>> {
        let field_index = self.data.field_index(field_name)?;
        let row = self.data.row(self.row_index).ok()?;
        row.fields.get(field_index)
    }

    fn get_field_value<T>(&self, field_name: &str) -> Result<Cow<'_, T>, DecodeValueError>
    where
        T: ?Sized + Decode<OperationId>,
    {
        let value =
            self.get_field(field_name)
                .ok_or_else(|| DecodeValueError::FieldDoesNotExist {
                    field_name: field_name.to_owned(),
                })?;
        T::decode(value)
    }

    fn get_nullable_field_value<T>(
        &self,
        field_name: &str,
    ) -> Result<Option<Cow<'_, T>>, DecodeValueError>
    where
        T: ?Sized + Decode<OperationId>,
    {
        let value =
            self.get_field(field_name)
                .ok_or_else(|| DecodeValueError::FieldDoesNotExist {
                    field_name: field_name.to_owned(),
                })?;
        match T::decode(value) {
            Ok(value) => Ok(Some(value)),
            Err(DecodeValueError::NullValue { .. }) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Snafu)]
pub enum InMemoryDataError {
    #[snafu(display("Unknown field '{field_name}'."))]
    UnknownField { field_name: String },
    #[snafu(display("Field '{field_name}' was provided more than once."))]
    DuplicateField { field_name: String },
    #[snafu(display("Missing required field '{field_name}'."))]
    MissingField { field_name: String },
    #[snafu(display(
        "Field '{field_name}' has a value that is incompatible with the schema data type."
    ))]
    InvalidFieldValue {
        field_name: String,
        source: DataModelValueError,
    },
    #[snafu(display("Row has wrong number of fields: expected {expected}, got {actual}."))]
    FieldCountMismatch { expected: usize, actual: usize },
    #[snafu(display("Unknown row index {row_index}."))]
    UnknownRow { row_index: usize },
}

#[derive(Debug, Snafu)]
pub enum InMemoryDataSnapshotEncodeError<E>
where
    E: snafu::Error + Send + Sync + 'static,
{
    #[snafu(display("Failed to encode row snapshot fields against the schema: {source}"))]
    SchemaVisit { source: SchemaVisitError<E> },
    #[snafu(display("Dataset snapshot encoder failed."))]
    Encoder { source: E },
}

#[derive(Debug, Snafu)]
pub enum InMemoryDataSnapshotDecodeError<E>
where
    E: snafu::Error + Send + Sync + 'static,
{
    #[snafu(display("Decoded row is invalid for the in-memory dataset."))]
    InMemoryData { source: InMemoryDataError },
    #[snafu(display("Decoded value does not match the schema data type."))]
    InvalidValue { source: DataModelValueError },
    #[snafu(display("Failed to read CRDT snapshot nodes while decoding a row."))]
    SnapshotRead {
        source: SnapshotReadError<InMemoryNodeDecodeError<E>>,
    },
    #[snafu(display("Dataset snapshot decoder failed."))]
    Decoder { source: E },
}

#[derive(Debug, Snafu)]
pub enum InMemoryNodeDecodeError<E>
where
    E: snafu::Error + Send + Sync + 'static,
{
    #[snafu(display("Snapshot node source failed while decoding history nodes."))]
    Source { source: E },
    #[snafu(display("Snapshot node value is incompatible with the schema data type."))]
    InvalidNodeValue { source: DataModelValueError },
}

/// Storage-level in-memory representation for one row over an associated schema.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct InMemoryRow<OperationId> {
    pub(crate) deleted: bool,
    pub(crate) fields: Vec<InMemoryFieldValue<OperationId>>,
}
impl<OperationId> InMemoryRow<OperationId> {
    fn new(fields: Vec<InMemoryFieldValue<OperationId>>) -> Self {
        Self {
            deleted: false,
            fields,
        }
    }

    fn field_count(&self) -> usize {
        self.fields.len()
    }

    fn encode_snapshot<V>(
        &self,
        schema: &Schema,
        field_names: &[String],
        encoder: &mut V,
    ) -> Result<(), SchemaVisitError<V::Error>>
    where
        OperationId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
        V: SchemaSnapshotEncoder<OperationId>,
    {
        let mut writer = prepare_schema_snapshot_encoder(encoder, schema)?;
        self.encode_snapshot_fields(field_names, &mut writer)?;
        writer.end()
    }

    pub(crate) fn encode_snapshot_fields<V>(
        &self,
        field_names: &[String],
        writer: &mut SchemaSnapshotEncodingWriter<'_, OperationId, V>,
    ) -> Result<(), SchemaVisitError<V::Error>>
    where
        OperationId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
        V: SchemaSnapshotEncoder<OperationId>,
    {
        debug_assert_eq!(field_names.len(), self.fields.len());
        for (field_name, field_value) in field_names
            .iter()
            .map(String::as_str)
            .zip(self.fields.iter())
        {
            field_value.encode_snapshot_field(field_name, writer)?;
        }
        Ok(())
    }

    fn decode_snapshot<D>(
        schema: &Schema,
        field_names: &[String],
        decoder: &mut D,
    ) -> Result<Self, InMemoryDataSnapshotDecodeError<D::Error>>
    where
        OperationId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
        D: SchemaSnapshotDecoder<OperationId>,
    {
        decoder.begin(field_names.len()).context(DecoderSnafu)?;

        let mut fields = Vec::with_capacity(field_names.len());
        for field_name in field_names {
            let schema_field = schema
                .columns
                .get(field_name.as_str())
                .expect("field names and schema are in sync");
            let field_value = InMemoryFieldValue::decode_snapshot_field(
                field_name.as_str(),
                schema_field,
                decoder,
            )?;
            fields.push(field_value);
        }

        decoder.end().context(DecoderSnafu)?;
        Ok(Self {
            deleted: false,
            fields,
        })
    }
}

/// In-memory CRDT state for one schema field.
#[derive(Clone, Debug, PartialEq)]
pub enum InMemoryFieldValue<OperationId> {
    LatestValueWins(LinearLatestValueWinsValue<OperationId>),
    LinearString(LinearString<OperationId>),
    LinearList(LinearListValue<OperationId>),
    MonotonicCounter(CounterValue),
    TotalOrderRegister(PrimitiveValue),
    TotalOrderFiniteStateRegister(NullablePrimitiveValue),
}

/// Specialized `LinearLatestValueWins` state variants by concrete value type.
#[derive(Clone, Debug, PartialEq)]
pub enum LinearLatestValueWinsValue<OperationId> {
    String(LinearLatestValueWins<IdWithIndex<OperationId>, String>),
    UInt(LinearLatestValueWins<IdWithIndex<OperationId>, u64>),
    Int(LinearLatestValueWins<IdWithIndex<OperationId>, i64>),
    Byte(LinearLatestValueWins<IdWithIndex<OperationId>, u8>),
    Float(LinearLatestValueWins<IdWithIndex<OperationId>, OrderedFloat<f64>>),
    Boolean(LinearLatestValueWins<IdWithIndex<OperationId>, bool>),
    Binary(LinearLatestValueWins<IdWithIndex<OperationId>, Vec<u8>>),
    Date(LinearLatestValueWins<IdWithIndex<OperationId>, NaiveDate>),
    Timestamp(LinearLatestValueWins<IdWithIndex<OperationId>, UnixTimestamp>),
    StringArray(LinearLatestValueWins<IdWithIndex<OperationId>, Vec<String>>),
    UIntArray(LinearLatestValueWins<IdWithIndex<OperationId>, Vec<u64>>),
    IntArray(LinearLatestValueWins<IdWithIndex<OperationId>, Vec<i64>>),
    ByteArray(LinearLatestValueWins<IdWithIndex<OperationId>, Vec<u8>>),
    FloatArray(LinearLatestValueWins<IdWithIndex<OperationId>, Vec<OrderedFloat<f64>>>),
    BooleanArray(LinearLatestValueWins<IdWithIndex<OperationId>, Vec<bool>>),
    BinaryArray(LinearLatestValueWins<IdWithIndex<OperationId>, Vec<Vec<u8>>>),
    DateArray(LinearLatestValueWins<IdWithIndex<OperationId>, Vec<NaiveDate>>),
    TimestampArray(LinearLatestValueWins<IdWithIndex<OperationId>, Vec<UnixTimestamp>>),
    NullableString(LinearLatestValueWins<IdWithIndex<OperationId>, Option<String>>),
    NullableUInt(LinearLatestValueWins<IdWithIndex<OperationId>, Option<u64>>),
    NullableInt(LinearLatestValueWins<IdWithIndex<OperationId>, Option<i64>>),
    NullableByte(LinearLatestValueWins<IdWithIndex<OperationId>, Option<u8>>),
    NullableFloat(LinearLatestValueWins<IdWithIndex<OperationId>, Option<OrderedFloat<f64>>>),
    NullableBoolean(LinearLatestValueWins<IdWithIndex<OperationId>, Option<bool>>),
    NullableBinary(LinearLatestValueWins<IdWithIndex<OperationId>, Option<Vec<u8>>>),
    NullableDate(LinearLatestValueWins<IdWithIndex<OperationId>, Option<NaiveDate>>),
    NullableTimestamp(LinearLatestValueWins<IdWithIndex<OperationId>, Option<UnixTimestamp>>),
    NullableStringArray(LinearLatestValueWins<IdWithIndex<OperationId>, Option<Vec<String>>>),
    NullableUIntArray(LinearLatestValueWins<IdWithIndex<OperationId>, Option<Vec<u64>>>),
    NullableIntArray(LinearLatestValueWins<IdWithIndex<OperationId>, Option<Vec<i64>>>),
    NullableByteArray(LinearLatestValueWins<IdWithIndex<OperationId>, Option<Vec<u8>>>),
    NullableFloatArray(
        LinearLatestValueWins<IdWithIndex<OperationId>, Option<Vec<OrderedFloat<f64>>>>,
    ),
    NullableBooleanArray(LinearLatestValueWins<IdWithIndex<OperationId>, Option<Vec<bool>>>),
    NullableBinaryArray(LinearLatestValueWins<IdWithIndex<OperationId>, Option<Vec<Vec<u8>>>>),
    NullableDateArray(LinearLatestValueWins<IdWithIndex<OperationId>, Option<Vec<NaiveDate>>>),
    NullableTimestampArray(
        LinearLatestValueWins<IdWithIndex<OperationId>, Option<Vec<UnixTimestamp>>>,
    ),
}
impl<OperationId> LinearLatestValueWinsValue<OperationId> {
    pub fn matches_type(&self, expected: &NullableBasicDataType) -> bool {
        match (self, expected) {
            (
                Self::NullableString(_),
                NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::String)),
            ) => true,
            (
                Self::NullableUInt(_),
                NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::UInt)),
            ) => true,
            (
                Self::NullableInt(_),
                NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Int)),
            ) => true,
            (
                Self::NullableByte(_),
                NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Byte)),
            ) => true,
            (
                Self::NullableFloat(_),
                NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Float)),
            ) => true,
            (
                Self::NullableBoolean(_),
                NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Boolean)),
            ) => true,
            (
                Self::NullableBinary(_),
                NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Binary)),
            ) => true,
            (
                Self::NullableDate(_),
                NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Date)),
            ) => true,
            (
                Self::NullableTimestamp(_),
                NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Timestamp)),
            ) => true,
            (
                Self::NullableStringArray(_),
                NullableBasicDataType::Nullable(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::String,
            (
                Self::NullableUIntArray(_),
                NullableBasicDataType::Nullable(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::UInt,
            (
                Self::NullableIntArray(_),
                NullableBasicDataType::Nullable(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::Int,
            (
                Self::NullableByteArray(_),
                NullableBasicDataType::Nullable(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::Byte,
            (
                Self::NullableFloatArray(_),
                NullableBasicDataType::Nullable(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::Float,
            (
                Self::NullableBooleanArray(_),
                NullableBasicDataType::Nullable(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::Boolean,
            (
                Self::NullableBinaryArray(_),
                NullableBasicDataType::Nullable(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::Binary,
            (
                Self::NullableDateArray(_),
                NullableBasicDataType::Nullable(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::Date,
            (
                Self::NullableTimestampArray(_),
                NullableBasicDataType::Nullable(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::Timestamp,
            (Self::NullableString(_), _) => false,
            (Self::NullableUInt(_), _) => false,
            (Self::NullableInt(_), _) => false,
            (Self::NullableByte(_), _) => false,
            (Self::NullableFloat(_), _) => false,
            (Self::NullableBoolean(_), _) => false,
            (Self::NullableBinary(_), _) => false,
            (Self::NullableDate(_), _) => false,
            (Self::NullableTimestamp(_), _) => false,
            (Self::NullableStringArray(_), _) => false,
            (Self::NullableUIntArray(_), _) => false,
            (Self::NullableIntArray(_), _) => false,
            (Self::NullableByteArray(_), _) => false,
            (Self::NullableFloatArray(_), _) => false,
            (Self::NullableBooleanArray(_), _) => false,
            (Self::NullableBinaryArray(_), _) => false,
            (Self::NullableDateArray(_), _) => false,
            (Self::NullableTimestampArray(_), _) => false,
            (_, NullableBasicDataType::Nullable(_)) => false,
            (
                Self::String(_),
                NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::String)),
            ) => true,
            (
                Self::UInt(_),
                NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::UInt)),
            ) => true,
            (
                Self::Int(_),
                NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Int)),
            ) => true,
            (
                Self::Byte(_),
                NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Byte)),
            ) => true,
            (
                Self::Float(_),
                NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Float)),
            ) => true,
            (
                Self::Boolean(_),
                NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Boolean)),
            ) => true,
            (
                Self::Binary(_),
                NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Binary)),
            ) => true,
            (
                Self::Date(_),
                NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Date)),
            ) => true,
            (
                Self::Timestamp(_),
                NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Timestamp)),
            ) => true,
            (
                Self::StringArray(_),
                NullableBasicDataType::NonNull(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::String,
            (
                Self::UIntArray(_),
                NullableBasicDataType::NonNull(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::UInt,
            (
                Self::IntArray(_),
                NullableBasicDataType::NonNull(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::Int,
            (
                Self::ByteArray(_),
                NullableBasicDataType::NonNull(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::Byte,
            (
                Self::FloatArray(_),
                NullableBasicDataType::NonNull(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::Float,
            (
                Self::BooleanArray(_),
                NullableBasicDataType::NonNull(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::Boolean,
            (
                Self::BinaryArray(_),
                NullableBasicDataType::NonNull(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::Binary,
            (
                Self::DateArray(_),
                NullableBasicDataType::NonNull(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::Date,
            (
                Self::TimestampArray(_),
                NullableBasicDataType::NonNull(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::Timestamp,
            _ => false,
        }
    }

    fn encode_snapshot<S, E>(&self, sink: &mut S) -> Result<(), E>
    where
        OperationId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
        S: for<'value> SnapshotSink<
                IdWithIndex<OperationId>,
                NullableBasicValueRef<'value>,
                Error = E,
            >,
    {
        match self {
            Self::String(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &String| {
                        NullableBasicValueRef::Value(BasicValueRef::Primitive(
                            PrimitiveValueRef::String(value.as_str()),
                        ))
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::UInt(value) => {
                let mut adapter = LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &u64| {
                    NullableBasicValueRef::Value(BasicValueRef::Primitive(PrimitiveValueRef::UInt(
                        *value,
                    )))
                });
                value.encode_snapshot(&mut adapter)
            }
            Self::Int(value) => {
                let mut adapter = LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &i64| {
                    NullableBasicValueRef::Value(BasicValueRef::Primitive(PrimitiveValueRef::Int(
                        *value,
                    )))
                });
                value.encode_snapshot(&mut adapter)
            }
            Self::Byte(value) => {
                let mut adapter = LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &u8| {
                    NullableBasicValueRef::Value(BasicValueRef::Primitive(PrimitiveValueRef::Byte(
                        *value,
                    )))
                });
                value.encode_snapshot(&mut adapter)
            }
            Self::Float(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &OrderedFloat<f64>| {
                        NullableBasicValueRef::Value(BasicValueRef::Primitive(
                            PrimitiveValueRef::Float(*value),
                        ))
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::Boolean(value) => {
                let mut adapter = LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &bool| {
                    NullableBasicValueRef::Value(BasicValueRef::Primitive(
                        PrimitiveValueRef::Boolean(*value),
                    ))
                });
                value.encode_snapshot(&mut adapter)
            }
            Self::Binary(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Vec<u8>| {
                        NullableBasicValueRef::Value(BasicValueRef::Primitive(
                            PrimitiveValueRef::Binary(value.as_slice()),
                        ))
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::Date(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &NaiveDate| {
                        NullableBasicValueRef::Value(BasicValueRef::Primitive(
                            PrimitiveValueRef::Date(*value),
                        ))
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::Timestamp(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &UnixTimestamp| {
                        NullableBasicValueRef::Value(BasicValueRef::Primitive(
                            PrimitiveValueRef::Timestamp(*value),
                        ))
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::StringArray(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Vec<String>| {
                        NullableBasicValueRef::Value(BasicValueRef::Array(
                            PrimitiveValueArrayRef::String(value.as_slice()),
                        ))
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::UIntArray(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Vec<u64>| {
                        NullableBasicValueRef::Value(BasicValueRef::Array(
                            PrimitiveValueArrayRef::UInt(value.as_slice()),
                        ))
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::IntArray(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Vec<i64>| {
                        NullableBasicValueRef::Value(BasicValueRef::Array(
                            PrimitiveValueArrayRef::Int(value.as_slice()),
                        ))
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::ByteArray(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Vec<u8>| {
                        NullableBasicValueRef::Value(BasicValueRef::Array(
                            PrimitiveValueArrayRef::Byte(value.as_slice()),
                        ))
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::FloatArray(value) => {
                let mut adapter = LatestValueWinsSnapshotSinkAdapter::new(
                    sink,
                    |value: &Vec<OrderedFloat<f64>>| {
                        NullableBasicValueRef::Value(BasicValueRef::Array(
                            PrimitiveValueArrayRef::Float(value.as_slice()),
                        ))
                    },
                );
                value.encode_snapshot(&mut adapter)
            }
            Self::BooleanArray(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Vec<bool>| {
                        NullableBasicValueRef::Value(BasicValueRef::Array(
                            PrimitiveValueArrayRef::Boolean(value.as_slice()),
                        ))
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::BinaryArray(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Vec<Vec<u8>>| {
                        NullableBasicValueRef::Value(BasicValueRef::Array(
                            PrimitiveValueArrayRef::Binary(value.as_slice()),
                        ))
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::DateArray(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Vec<NaiveDate>| {
                        NullableBasicValueRef::Value(BasicValueRef::Array(
                            PrimitiveValueArrayRef::Date(value.as_slice()),
                        ))
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::TimestampArray(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Vec<UnixTimestamp>| {
                        NullableBasicValueRef::Value(BasicValueRef::Array(
                            PrimitiveValueArrayRef::Timestamp(value.as_slice()),
                        ))
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableString(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Option<String>| {
                        match value {
                            Some(value) => NullableBasicValueRef::Value(BasicValueRef::Primitive(
                                PrimitiveValueRef::String(value.as_str()),
                            )),
                            None => NullableBasicValueRef::Null,
                        }
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableUInt(value) => {
                let mut adapter = LatestValueWinsSnapshotSinkAdapter::new(
                    sink,
                    |value: &Option<u64>| match value {
                        Some(value) => NullableBasicValueRef::Value(BasicValueRef::Primitive(
                            PrimitiveValueRef::UInt(*value),
                        )),
                        None => NullableBasicValueRef::Null,
                    },
                );
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableInt(value) => {
                let mut adapter = LatestValueWinsSnapshotSinkAdapter::new(
                    sink,
                    |value: &Option<i64>| match value {
                        Some(value) => NullableBasicValueRef::Value(BasicValueRef::Primitive(
                            PrimitiveValueRef::Int(*value),
                        )),
                        None => NullableBasicValueRef::Null,
                    },
                );
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableByte(value) => {
                let mut adapter = LatestValueWinsSnapshotSinkAdapter::new(
                    sink,
                    |value: &Option<u8>| match value {
                        Some(value) => NullableBasicValueRef::Value(BasicValueRef::Primitive(
                            PrimitiveValueRef::Byte(*value),
                        )),
                        None => NullableBasicValueRef::Null,
                    },
                );
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableFloat(value) => {
                let mut adapter = LatestValueWinsSnapshotSinkAdapter::new(
                    sink,
                    |value: &Option<OrderedFloat<f64>>| match value {
                        Some(value) => NullableBasicValueRef::Value(BasicValueRef::Primitive(
                            PrimitiveValueRef::Float(*value),
                        )),
                        None => NullableBasicValueRef::Null,
                    },
                );
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableBoolean(value) => {
                let mut adapter = LatestValueWinsSnapshotSinkAdapter::new(
                    sink,
                    |value: &Option<bool>| match value {
                        Some(value) => NullableBasicValueRef::Value(BasicValueRef::Primitive(
                            PrimitiveValueRef::Boolean(*value),
                        )),
                        None => NullableBasicValueRef::Null,
                    },
                );
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableBinary(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Option<Vec<u8>>| {
                        match value {
                            Some(value) => NullableBasicValueRef::Value(BasicValueRef::Primitive(
                                PrimitiveValueRef::Binary(value.as_slice()),
                            )),
                            None => NullableBasicValueRef::Null,
                        }
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableDate(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Option<NaiveDate>| {
                        match value {
                            Some(value) => NullableBasicValueRef::Value(BasicValueRef::Primitive(
                                PrimitiveValueRef::Date(*value),
                            )),
                            None => NullableBasicValueRef::Null,
                        }
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableTimestamp(value) => {
                let mut adapter = LatestValueWinsSnapshotSinkAdapter::new(
                    sink,
                    |value: &Option<UnixTimestamp>| match value {
                        Some(value) => NullableBasicValueRef::Value(BasicValueRef::Primitive(
                            PrimitiveValueRef::Timestamp(*value),
                        )),
                        None => NullableBasicValueRef::Null,
                    },
                );
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableStringArray(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Option<Vec<String>>| {
                        match value {
                            Some(value) => NullableBasicValueRef::Value(BasicValueRef::Array(
                                PrimitiveValueArrayRef::String(value.as_slice()),
                            )),
                            None => NullableBasicValueRef::Null,
                        }
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableUIntArray(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Option<Vec<u64>>| {
                        match value {
                            Some(value) => NullableBasicValueRef::Value(BasicValueRef::Array(
                                PrimitiveValueArrayRef::UInt(value.as_slice()),
                            )),
                            None => NullableBasicValueRef::Null,
                        }
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableIntArray(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Option<Vec<i64>>| {
                        match value {
                            Some(value) => NullableBasicValueRef::Value(BasicValueRef::Array(
                                PrimitiveValueArrayRef::Int(value.as_slice()),
                            )),
                            None => NullableBasicValueRef::Null,
                        }
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableByteArray(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Option<Vec<u8>>| {
                        match value {
                            Some(value) => NullableBasicValueRef::Value(BasicValueRef::Array(
                                PrimitiveValueArrayRef::Byte(value.as_slice()),
                            )),
                            None => NullableBasicValueRef::Null,
                        }
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableFloatArray(value) => {
                let mut adapter = LatestValueWinsSnapshotSinkAdapter::new(
                    sink,
                    |value: &Option<Vec<OrderedFloat<f64>>>| match value {
                        Some(value) => NullableBasicValueRef::Value(BasicValueRef::Array(
                            PrimitiveValueArrayRef::Float(value.as_slice()),
                        )),
                        None => NullableBasicValueRef::Null,
                    },
                );
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableBooleanArray(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Option<Vec<bool>>| {
                        match value {
                            Some(value) => NullableBasicValueRef::Value(BasicValueRef::Array(
                                PrimitiveValueArrayRef::Boolean(value.as_slice()),
                            )),
                            None => NullableBasicValueRef::Null,
                        }
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableBinaryArray(value) => {
                let mut adapter = LatestValueWinsSnapshotSinkAdapter::new(
                    sink,
                    |value: &Option<Vec<Vec<u8>>>| match value {
                        Some(value) => NullableBasicValueRef::Value(BasicValueRef::Array(
                            PrimitiveValueArrayRef::Binary(value.as_slice()),
                        )),
                        None => NullableBasicValueRef::Null,
                    },
                );
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableDateArray(value) => {
                let mut adapter = LatestValueWinsSnapshotSinkAdapter::new(
                    sink,
                    |value: &Option<Vec<NaiveDate>>| match value {
                        Some(value) => NullableBasicValueRef::Value(BasicValueRef::Array(
                            PrimitiveValueArrayRef::Date(value.as_slice()),
                        )),
                        None => NullableBasicValueRef::Null,
                    },
                );
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableTimestampArray(value) => {
                let mut adapter = LatestValueWinsSnapshotSinkAdapter::new(
                    sink,
                    |value: &Option<Vec<UnixTimestamp>>| match value {
                        Some(value) => NullableBasicValueRef::Value(BasicValueRef::Array(
                            PrimitiveValueArrayRef::Timestamp(value.as_slice()),
                        )),
                        None => NullableBasicValueRef::Null,
                    },
                );
                value.encode_snapshot(&mut adapter)
            }
        }
    }
}

/// Specialized `LinearList` state variants by concrete primitive element type.
#[derive(Clone, Debug, PartialEq)]
pub enum LinearListValue<OperationId> {
    String(LinearList<OperationId, String>),
    UInt(LinearList<OperationId, u64>),
    Int(LinearList<OperationId, i64>),
    Byte(LinearList<OperationId, u8>),
    Float(LinearList<OperationId, OrderedFloat<f64>>),
    Boolean(LinearList<OperationId, bool>),
    Binary(LinearList<OperationId, Vec<u8>>),
    Date(LinearList<OperationId, NaiveDate>),
    Timestamp(LinearList<OperationId, UnixTimestamp>),
}
impl<OperationId> LinearListValue<OperationId> {
    pub fn primitive_type(&self) -> PrimitiveType {
        match self {
            Self::String(_) => PrimitiveType::String,
            Self::UInt(_) => PrimitiveType::UInt,
            Self::Int(_) => PrimitiveType::Int,
            Self::Byte(_) => PrimitiveType::Byte,
            Self::Float(_) => PrimitiveType::Float,
            Self::Boolean(_) => PrimitiveType::Boolean,
            Self::Binary(_) => PrimitiveType::Binary,
            Self::Date(_) => PrimitiveType::Date,
            Self::Timestamp(_) => PrimitiveType::Timestamp,
        }
    }

    fn encode_snapshot<S, E>(&self, sink: &mut S) -> Result<(), E>
    where
        OperationId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
        S: for<'value> SnapshotSink<
                IdWithIndex<OperationId>,
                PrimitiveValueArrayRef<'value>,
                Error = E,
            >,
    {
        match self {
            Self::String(value) => {
                let mut adapter = LinearListSnapshotSinkAdapter::new(sink, |value: &[String]| {
                    PrimitiveValueArrayRef::String(value)
                });
                value.encode_snapshot(&mut adapter)
            }
            Self::UInt(value) => {
                let mut adapter = LinearListSnapshotSinkAdapter::new(sink, |value: &[u64]| {
                    PrimitiveValueArrayRef::UInt(value)
                });
                value.encode_snapshot(&mut adapter)
            }
            Self::Int(value) => {
                let mut adapter = LinearListSnapshotSinkAdapter::new(sink, |value: &[i64]| {
                    PrimitiveValueArrayRef::Int(value)
                });
                value.encode_snapshot(&mut adapter)
            }
            Self::Byte(value) => {
                let mut adapter = LinearListSnapshotSinkAdapter::new(sink, |value: &[u8]| {
                    PrimitiveValueArrayRef::Byte(value)
                });
                value.encode_snapshot(&mut adapter)
            }
            Self::Float(value) => {
                let mut adapter =
                    LinearListSnapshotSinkAdapter::new(sink, |value: &[OrderedFloat<f64>]| {
                        PrimitiveValueArrayRef::Float(value)
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::Boolean(value) => {
                let mut adapter = LinearListSnapshotSinkAdapter::new(sink, |value: &[bool]| {
                    PrimitiveValueArrayRef::Boolean(value)
                });
                value.encode_snapshot(&mut adapter)
            }
            Self::Binary(value) => {
                let mut adapter =
                    LinearListSnapshotSinkAdapter::new(sink, |value: &[Vec<u8>]| {
                        PrimitiveValueArrayRef::Binary(value)
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::Date(value) => {
                let mut adapter =
                    LinearListSnapshotSinkAdapter::new(sink, |value: &[NaiveDate]| {
                        PrimitiveValueArrayRef::Date(value)
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::Timestamp(value) => {
                let mut adapter =
                    LinearListSnapshotSinkAdapter::new(sink, |value: &[UnixTimestamp]| {
                        PrimitiveValueArrayRef::Timestamp(value)
                    });
                value.encode_snapshot(&mut adapter)
            }
        }
    }
}

impl<OperationId> InMemoryFieldValue<OperationId> {
    pub(crate) fn encode_snapshot_field<V>(
        &self,
        field_name: &str,
        writer: &mut SchemaSnapshotEncodingWriter<'_, OperationId, V>,
    ) -> Result<(), SchemaVisitError<V::Error>>
    where
        OperationId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
        V: SchemaSnapshotEncoder<OperationId>,
    {
        match self {
            Self::LatestValueWins(value) => {
                let mut sink = writer.prepare_latest_value_wins_field(field_name)?;
                value.encode_snapshot(&mut sink).context(VisitorSnafu)
            }
            Self::LinearString(value) => {
                let mut sink = writer.prepare_linear_string_field(field_name)?;
                value.encode_snapshot(&mut sink).context(VisitorSnafu)
            }
            Self::LinearList(value) => {
                let mut sink = writer.prepare_linear_list_field(field_name)?;
                value.encode_snapshot(&mut sink).context(VisitorSnafu)
            }
            Self::MonotonicCounter(value) => writer.state_field(
                field_name,
                SnapshotStateValueRef::MonotonicCounter(value.as_ref()),
            ),
            Self::TotalOrderRegister(value) => writer.state_field(
                field_name,
                SnapshotStateValueRef::TotalOrderRegister(value.as_ref()),
            ),
            Self::TotalOrderFiniteStateRegister(value) => writer.state_field(
                field_name,
                SnapshotStateValueRef::TotalOrderFiniteStateRegister(value.as_ref()),
            ),
        }
    }

    pub(crate) fn decode_snapshot_field<D>(
        field_name: &str,
        schema_field: &super::super::Field,
        decoder: &mut D,
    ) -> Result<Self, InMemoryDataSnapshotDecodeError<D::Error>>
    where
        OperationId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
        D: SchemaSnapshotDecoder<OperationId>,
    {
        match &schema_field.data_type {
            ReplicatedDataType::LatestValueWins { value_type } => {
                let mut source = decoder
                    .prepare_latest_value_wins_field(field_name, value_type)
                    .context(DecoderSnafu)?;
                let nodes =
                    SnapshotNodeSourceIter::<_, IdWithIndex<OperationId>, NullableBasicValue>::new(
                        &mut source,
                    )
                    .map(|result| result.context(SourceSnafu));
                let value = decode_latest_value_wins_snapshot(value_type, nodes)
                    .context(SnapshotReadSnafu)?;
                Ok(Self::LatestValueWins(value))
            }
            ReplicatedDataType::LinearString => {
                let mut source = decoder
                    .prepare_linear_string_field(field_name)
                    .context(DecoderSnafu)?;
                let nodes =
                    SnapshotNodeSourceIter::<_, IdWithIndex<OperationId>, String>::new(&mut source)
                        .map(|result| result.context(SourceSnafu));
                let value = LinearString::from_snapshot_nodes(nodes).context(SnapshotReadSnafu)?;
                Ok(Self::LinearString(value))
            }
            ReplicatedDataType::LinearList { value_type } => {
                let mut source = decoder
                    .prepare_linear_list_field(field_name, *value_type)
                    .context(DecoderSnafu)?;
                let nodes = SnapshotNodeSourceIter::<
                    _,
                    IdWithIndex<OperationId>,
                    PrimitiveValueArray,
                >::new(&mut source)
                .map(|result| result.context(SourceSnafu));
                let value =
                    decode_linear_list_snapshot(*value_type, nodes).context(SnapshotReadSnafu)?;
                Ok(Self::LinearList(value))
            }
            ReplicatedDataType::MonotonicCounter { .. }
            | ReplicatedDataType::TotalOrderRegister { .. }
            | ReplicatedDataType::TotalOrderFiniteStateRegister { .. } => {
                let state = decoder
                    .decode_state_field(field_name, &schema_field.data_type)
                    .context(DecoderSnafu)?;
                decode_state_snapshot_field(&schema_field.data_type, state)
                    .context(InvalidValueSnafu)
            }
        }
    }
}

struct LatestValueWinsSnapshotSinkAdapter<'a, Id, Value, Sink, Mapper>
where
    Mapper: for<'value> Fn(&'value Value) -> NullableBasicValueRef<'value>,
    for<'value> Sink: SnapshotSink<IdWithIndex<Id>, NullableBasicValueRef<'value>>,
{
    sink: &'a mut Sink,
    map_value: Mapper,
    _marker: PhantomData<fn(Id, Value)>,
}
impl<'a, Id, Value, Sink, Mapper> LatestValueWinsSnapshotSinkAdapter<'a, Id, Value, Sink, Mapper>
where
    Mapper: for<'value> Fn(&'value Value) -> NullableBasicValueRef<'value>,
    for<'value> Sink: SnapshotSink<IdWithIndex<Id>, NullableBasicValueRef<'value>>,
{
    fn new(sink: &'a mut Sink, map_value: Mapper) -> Self {
        Self {
            sink,
            map_value,
            _marker: PhantomData,
        }
    }
}
impl<Id, Value, Sink, Mapper, E> SnapshotSink<IdWithIndex<Id>, Value>
    for LatestValueWinsSnapshotSinkAdapter<'_, Id, Value, Sink, Mapper>
where
    Mapper: for<'value> Fn(&'value Value) -> NullableBasicValueRef<'value>,
    for<'value> Sink: SnapshotSink<IdWithIndex<Id>, NullableBasicValueRef<'value>, Error = E>,
{
    type Error = E;

    fn begin(&mut self, header: SnapshotHeader) -> Result<(), Self::Error> {
        self.sink.begin(header)
    }

    fn node(
        &mut self,
        index: usize,
        node: SnapshotNodeRef<'_, IdWithIndex<Id>, Value>,
    ) -> Result<(), Self::Error> {
        let mapped_value = node.value.map(|value| (self.map_value)(value));
        let mapped_node = SnapshotNodeRef {
            id: node.id,
            left: node.left,
            right: node.right,
            deleted: node.deleted,
            value: mapped_value.as_ref(),
        };
        self.sink.node(index, mapped_node)
    }

    fn end(&mut self) -> Result<(), Self::Error> {
        self.sink.end()
    }
}

struct LinearListSnapshotSinkAdapter<'a, Id, Value, Sink, Mapper>
where
    Mapper: for<'value> Fn(&'value [Value]) -> PrimitiveValueArrayRef<'value>,
    for<'value> Sink: SnapshotSink<IdWithIndex<Id>, PrimitiveValueArrayRef<'value>>,
{
    sink: &'a mut Sink,
    map_value: Mapper,
    _marker: PhantomData<fn(Id, Value)>,
}
impl<'a, Id, Value, Sink, Mapper> LinearListSnapshotSinkAdapter<'a, Id, Value, Sink, Mapper>
where
    Mapper: for<'value> Fn(&'value [Value]) -> PrimitiveValueArrayRef<'value>,
    for<'value> Sink: SnapshotSink<IdWithIndex<Id>, PrimitiveValueArrayRef<'value>>,
{
    fn new(sink: &'a mut Sink, map_value: Mapper) -> Self {
        Self {
            sink,
            map_value,
            _marker: PhantomData,
        }
    }
}
impl<Id, Value, Sink, Mapper, E> SnapshotSink<IdWithIndex<Id>, [Value]>
    for LinearListSnapshotSinkAdapter<'_, Id, Value, Sink, Mapper>
where
    Mapper: for<'value> Fn(&'value [Value]) -> PrimitiveValueArrayRef<'value>,
    for<'value> Sink: SnapshotSink<IdWithIndex<Id>, PrimitiveValueArrayRef<'value>, Error = E>,
{
    type Error = E;

    fn begin(&mut self, header: SnapshotHeader) -> Result<(), Self::Error> {
        self.sink.begin(header)
    }

    fn node(
        &mut self,
        index: usize,
        node: SnapshotNodeRef<'_, IdWithIndex<Id>, [Value]>,
    ) -> Result<(), Self::Error> {
        let mapped_value = node.value.map(|value| (self.map_value)(value));
        let mapped_node = SnapshotNodeRef {
            id: node.id,
            left: node.left,
            right: node.right,
            deleted: node.deleted,
            value: mapped_value.as_ref(),
        };
        self.sink.node(index, mapped_node)
    }

    fn end(&mut self) -> Result<(), Self::Error> {
        self.sink.end()
    }
}

fn decode_state_snapshot_field<OperationId>(
    data_type: &ReplicatedDataType,
    value: SnapshotStateValue,
) -> Result<InMemoryFieldValue<OperationId>, DataModelValueError> {
    match (data_type, value) {
        (
            ReplicatedDataType::MonotonicCounter { small_range },
            SnapshotStateValue::MonotonicCounter(value),
        ) => {
            ensure_counter_type(*small_range, value.as_ref())?;
            Ok(InMemoryFieldValue::MonotonicCounter(value))
        }
        (
            ReplicatedDataType::TotalOrderRegister { value_type, .. },
            SnapshotStateValue::TotalOrderRegister(value),
        ) => {
            ensure_primitive_type(*value_type, value.primitive_type())?;
            Ok(InMemoryFieldValue::TotalOrderRegister(value))
        }
        (
            ReplicatedDataType::TotalOrderFiniteStateRegister { value_type, states },
            SnapshotStateValue::TotalOrderFiniteStateRegister(value),
        ) => {
            ensure_finite_state_value(*value_type, states, &value.as_ref())?;
            Ok(InMemoryFieldValue::TotalOrderFiniteStateRegister(value))
        }
        _ => Err(DataModelValueError::InvalidSnapshotValueForType),
    }
}

fn decode_latest_value_wins_snapshot<OperationId, E, I>(
    value_type: &NullableBasicDataType,
    nodes: I,
) -> Result<LinearLatestValueWinsValue<OperationId>, SnapshotReadError<InMemoryNodeDecodeError<E>>>
where
    E: snafu::Error + Send + Sync + 'static,
    OperationId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
    I: IntoIterator<
        Item = Result<
            SnapshotNode<IdWithIndex<OperationId>, NullableBasicValue>,
            InMemoryNodeDecodeError<E>,
        >,
    >,
{
    match value_type {
        NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::String)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_required_string,
            ))
            .map(LinearLatestValueWinsValue::String)
        }
        NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::UInt)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_required_uint,
            ))
            .map(LinearLatestValueWinsValue::UInt)
        }
        NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Int)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_required_int,
            ))
            .map(LinearLatestValueWinsValue::Int)
        }
        NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Byte)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_required_byte,
            ))
            .map(LinearLatestValueWinsValue::Byte)
        }
        NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Float)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_required_float,
            ))
            .map(LinearLatestValueWinsValue::Float)
        }
        NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Boolean)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_required_boolean,
            ))
            .map(LinearLatestValueWinsValue::Boolean)
        }
        NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Binary)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_required_binary,
            ))
            .map(LinearLatestValueWinsValue::Binary)
        }
        NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Date)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_required_date,
            ))
            .map(LinearLatestValueWinsValue::Date)
        }
        NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Timestamp)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_required_timestamp,
            ))
            .map(LinearLatestValueWinsValue::Timestamp)
        }
        NullableBasicDataType::NonNull(BasicDataType::Array(array_type)) => {
            match array_type.element_type {
                PrimitiveType::String => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_required_string_array),
                )
                .map(LinearLatestValueWinsValue::StringArray),
                PrimitiveType::UInt => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_required_uint_array),
                )
                .map(LinearLatestValueWinsValue::UIntArray),
                PrimitiveType::Int => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_required_int_array),
                )
                .map(LinearLatestValueWinsValue::IntArray),
                PrimitiveType::Byte => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_required_byte_array),
                )
                .map(LinearLatestValueWinsValue::ByteArray),
                PrimitiveType::Float => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_required_float_array),
                )
                .map(LinearLatestValueWinsValue::FloatArray),
                PrimitiveType::Boolean => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_required_boolean_array),
                )
                .map(LinearLatestValueWinsValue::BooleanArray),
                PrimitiveType::Binary => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_required_binary_array),
                )
                .map(LinearLatestValueWinsValue::BinaryArray),
                PrimitiveType::Date => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_required_date_array),
                )
                .map(LinearLatestValueWinsValue::DateArray),
                PrimitiveType::Timestamp => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_required_timestamp_array),
                )
                .map(LinearLatestValueWinsValue::TimestampArray),
            }
        }
        NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::String)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_optional_string,
            ))
            .map(LinearLatestValueWinsValue::NullableString)
        }
        NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::UInt)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_optional_uint,
            ))
            .map(LinearLatestValueWinsValue::NullableUInt)
        }
        NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Int)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_optional_int,
            ))
            .map(LinearLatestValueWinsValue::NullableInt)
        }
        NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Byte)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_optional_byte,
            ))
            .map(LinearLatestValueWinsValue::NullableByte)
        }
        NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Float)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_optional_float,
            ))
            .map(LinearLatestValueWinsValue::NullableFloat)
        }
        NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Boolean)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_optional_boolean,
            ))
            .map(LinearLatestValueWinsValue::NullableBoolean)
        }
        NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Binary)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_optional_binary,
            ))
            .map(LinearLatestValueWinsValue::NullableBinary)
        }
        NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Date)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_optional_date,
            ))
            .map(LinearLatestValueWinsValue::NullableDate)
        }
        NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Timestamp)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_optional_timestamp,
            ))
            .map(LinearLatestValueWinsValue::NullableTimestamp)
        }
        NullableBasicDataType::Nullable(BasicDataType::Array(array_type)) => {
            match array_type.element_type {
                PrimitiveType::String => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_optional_string_array),
                )
                .map(LinearLatestValueWinsValue::NullableStringArray),
                PrimitiveType::UInt => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_optional_uint_array),
                )
                .map(LinearLatestValueWinsValue::NullableUIntArray),
                PrimitiveType::Int => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_optional_int_array),
                )
                .map(LinearLatestValueWinsValue::NullableIntArray),
                PrimitiveType::Byte => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_optional_byte_array),
                )
                .map(LinearLatestValueWinsValue::NullableByteArray),
                PrimitiveType::Float => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_optional_float_array),
                )
                .map(LinearLatestValueWinsValue::NullableFloatArray),
                PrimitiveType::Boolean => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_optional_boolean_array),
                )
                .map(LinearLatestValueWinsValue::NullableBooleanArray),
                PrimitiveType::Binary => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_optional_binary_array),
                )
                .map(LinearLatestValueWinsValue::NullableBinaryArray),
                PrimitiveType::Date => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_optional_date_array),
                )
                .map(LinearLatestValueWinsValue::NullableDateArray),
                PrimitiveType::Timestamp => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_optional_timestamp_array),
                )
                .map(LinearLatestValueWinsValue::NullableTimestampArray),
            }
        }
    }
}

fn decode_linear_list_snapshot<OperationId, E, I>(
    value_type: PrimitiveType,
    nodes: I,
) -> Result<LinearListValue<OperationId>, SnapshotReadError<InMemoryNodeDecodeError<E>>>
where
    E: snafu::Error + Send + Sync + 'static,
    OperationId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
    I: IntoIterator<
        Item = Result<
            SnapshotNode<IdWithIndex<OperationId>, PrimitiveValueArray>,
            InMemoryNodeDecodeError<E>,
        >,
    >,
{
    match value_type {
        PrimitiveType::String => {
            LinearList::from_snapshot_nodes(map_snapshot_nodes_value(nodes, decode_list_string))
                .map(LinearListValue::String)
        }
        PrimitiveType::UInt => {
            LinearList::from_snapshot_nodes(map_snapshot_nodes_value(nodes, decode_list_uint))
                .map(LinearListValue::UInt)
        }
        PrimitiveType::Int => {
            LinearList::from_snapshot_nodes(map_snapshot_nodes_value(nodes, decode_list_int))
                .map(LinearListValue::Int)
        }
        PrimitiveType::Byte => {
            LinearList::from_snapshot_nodes(map_snapshot_nodes_value(nodes, decode_list_byte))
                .map(LinearListValue::Byte)
        }
        PrimitiveType::Float => {
            LinearList::from_snapshot_nodes(map_snapshot_nodes_value(nodes, decode_list_float))
                .map(LinearListValue::Float)
        }
        PrimitiveType::Boolean => {
            LinearList::from_snapshot_nodes(map_snapshot_nodes_value(nodes, decode_list_boolean))
                .map(LinearListValue::Boolean)
        }
        PrimitiveType::Binary => {
            LinearList::from_snapshot_nodes(map_snapshot_nodes_value(nodes, decode_list_binary))
                .map(LinearListValue::Binary)
        }
        PrimitiveType::Date => {
            LinearList::from_snapshot_nodes(map_snapshot_nodes_value(nodes, decode_list_date))
                .map(LinearListValue::Date)
        }
        PrimitiveType::Timestamp => {
            LinearList::from_snapshot_nodes(map_snapshot_nodes_value(nodes, decode_list_timestamp))
                .map(LinearListValue::Timestamp)
        }
    }
}

fn map_snapshot_nodes_value<Id, InputValue, OutputValue, E, I, F>(
    nodes: I,
    map_value: F,
) -> impl Iterator<Item = Result<SnapshotNode<Id, OutputValue>, InMemoryNodeDecodeError<E>>>
where
    E: snafu::Error + Send + Sync + 'static,
    I: IntoIterator<Item = Result<SnapshotNode<Id, InputValue>, InMemoryNodeDecodeError<E>>>,
    F: Fn(InputValue) -> Result<OutputValue, DataModelValueError> + Copy,
{
    nodes.into_iter().map(move |entry| {
        let node = entry?;
        let value = match node.value {
            Some(value) => Some(map_value(value).context(InvalidNodeValueSnafu)?),
            None => None,
        };
        Ok(SnapshotNode {
            id: node.id,
            left: node.left,
            right: node.right,
            deleted: node.deleted,
            value,
        })
    })
}

macro_rules! define_nullable_basic_converters {
    ($required_fn:ident, $optional_fn:ident, $out_ty:ty, $pattern:pat => $mapped:expr) => {
        fn $required_fn(value: NullableBasicValue) -> Result<$out_ty, DataModelValueError> {
            match value {
                NullableBasicValue::Null => Err(DataModelValueError::NullabilityMismatch {
                    expected_nullable: false,
                    actual_nullable: true,
                }),
                NullableBasicValue::Value(value) => match value {
                    $pattern => Ok($mapped),
                    _ => Err(DataModelValueError::BasicTypeMismatch),
                },
            }
        }

        fn $optional_fn(value: NullableBasicValue) -> Result<Option<$out_ty>, DataModelValueError> {
            match value {
                NullableBasicValue::Null => Ok(None),
                NullableBasicValue::Value(value) => match value {
                    $pattern => Ok(Some($mapped)),
                    _ => Err(DataModelValueError::BasicTypeMismatch),
                },
            }
        }
    };
}

define_nullable_basic_converters!(
    decode_required_string,
    decode_optional_string,
    String,
    BasicValue::Primitive(PrimitiveValue::String(value)) => value
);
define_nullable_basic_converters!(
    decode_required_uint,
    decode_optional_uint,
    u64,
    BasicValue::Primitive(PrimitiveValue::UInt(value)) => value
);
define_nullable_basic_converters!(
    decode_required_int,
    decode_optional_int,
    i64,
    BasicValue::Primitive(PrimitiveValue::Int(value)) => value
);
define_nullable_basic_converters!(
    decode_required_byte,
    decode_optional_byte,
    u8,
    BasicValue::Primitive(PrimitiveValue::Byte(value)) => value
);
define_nullable_basic_converters!(
    decode_required_float,
    decode_optional_float,
    OrderedFloat<f64>,
    BasicValue::Primitive(PrimitiveValue::Float(value)) => value
);
define_nullable_basic_converters!(
    decode_required_boolean,
    decode_optional_boolean,
    bool,
    BasicValue::Primitive(PrimitiveValue::Boolean(value)) => value
);
define_nullable_basic_converters!(
    decode_required_binary,
    decode_optional_binary,
    Vec<u8>,
    BasicValue::Primitive(PrimitiveValue::Binary(value)) => value
);
define_nullable_basic_converters!(
    decode_required_date,
    decode_optional_date,
    NaiveDate,
    BasicValue::Primitive(PrimitiveValue::Date(value)) => value
);
define_nullable_basic_converters!(
    decode_required_timestamp,
    decode_optional_timestamp,
    UnixTimestamp,
    BasicValue::Primitive(PrimitiveValue::Timestamp(value)) => value
);
define_nullable_basic_converters!(
    decode_required_string_array,
    decode_optional_string_array,
    Vec<String>,
    BasicValue::Array(PrimitiveValueArray::String(value)) => value
);
define_nullable_basic_converters!(
    decode_required_uint_array,
    decode_optional_uint_array,
    Vec<u64>,
    BasicValue::Array(PrimitiveValueArray::UInt(value)) => value
);
define_nullable_basic_converters!(
    decode_required_int_array,
    decode_optional_int_array,
    Vec<i64>,
    BasicValue::Array(PrimitiveValueArray::Int(value)) => value
);
define_nullable_basic_converters!(
    decode_required_byte_array,
    decode_optional_byte_array,
    Vec<u8>,
    BasicValue::Array(PrimitiveValueArray::Byte(value)) => value
);
define_nullable_basic_converters!(
    decode_required_float_array,
    decode_optional_float_array,
    Vec<OrderedFloat<f64>>,
    BasicValue::Array(PrimitiveValueArray::Float(value)) => value
);
define_nullable_basic_converters!(
    decode_required_boolean_array,
    decode_optional_boolean_array,
    Vec<bool>,
    BasicValue::Array(PrimitiveValueArray::Boolean(value)) => value
);
define_nullable_basic_converters!(
    decode_required_binary_array,
    decode_optional_binary_array,
    Vec<Vec<u8>>,
    BasicValue::Array(PrimitiveValueArray::Binary(value)) => value
);
define_nullable_basic_converters!(
    decode_required_date_array,
    decode_optional_date_array,
    Vec<NaiveDate>,
    BasicValue::Array(PrimitiveValueArray::Date(value)) => value
);
define_nullable_basic_converters!(
    decode_required_timestamp_array,
    decode_optional_timestamp_array,
    Vec<UnixTimestamp>,
    BasicValue::Array(PrimitiveValueArray::Timestamp(value)) => value
);

fn decode_list_string(value: PrimitiveValueArray) -> Result<Vec<String>, DataModelValueError> {
    match value {
        PrimitiveValueArray::String(value) => Ok(value),
        actual => Err(DataModelValueError::PrimitiveTypeMismatch {
            expected: PrimitiveType::String,
            actual: actual.primitive_type(),
        }),
    }
}
fn decode_list_uint(value: PrimitiveValueArray) -> Result<Vec<u64>, DataModelValueError> {
    match value {
        PrimitiveValueArray::UInt(value) => Ok(value),
        actual => Err(DataModelValueError::PrimitiveTypeMismatch {
            expected: PrimitiveType::UInt,
            actual: actual.primitive_type(),
        }),
    }
}
fn decode_list_int(value: PrimitiveValueArray) -> Result<Vec<i64>, DataModelValueError> {
    match value {
        PrimitiveValueArray::Int(value) => Ok(value),
        actual => Err(DataModelValueError::PrimitiveTypeMismatch {
            expected: PrimitiveType::Int,
            actual: actual.primitive_type(),
        }),
    }
}
fn decode_list_byte(value: PrimitiveValueArray) -> Result<Vec<u8>, DataModelValueError> {
    match value {
        PrimitiveValueArray::Byte(value) => Ok(value),
        actual => Err(DataModelValueError::PrimitiveTypeMismatch {
            expected: PrimitiveType::Byte,
            actual: actual.primitive_type(),
        }),
    }
}
fn decode_list_float(
    value: PrimitiveValueArray,
) -> Result<Vec<OrderedFloat<f64>>, DataModelValueError> {
    match value {
        PrimitiveValueArray::Float(value) => Ok(value),
        actual => Err(DataModelValueError::PrimitiveTypeMismatch {
            expected: PrimitiveType::Float,
            actual: actual.primitive_type(),
        }),
    }
}
fn decode_list_boolean(value: PrimitiveValueArray) -> Result<Vec<bool>, DataModelValueError> {
    match value {
        PrimitiveValueArray::Boolean(value) => Ok(value),
        actual => Err(DataModelValueError::PrimitiveTypeMismatch {
            expected: PrimitiveType::Boolean,
            actual: actual.primitive_type(),
        }),
    }
}
fn decode_list_binary(value: PrimitiveValueArray) -> Result<Vec<Vec<u8>>, DataModelValueError> {
    match value {
        PrimitiveValueArray::Binary(value) => Ok(value),
        actual => Err(DataModelValueError::PrimitiveTypeMismatch {
            expected: PrimitiveType::Binary,
            actual: actual.primitive_type(),
        }),
    }
}
fn decode_list_date(value: PrimitiveValueArray) -> Result<Vec<NaiveDate>, DataModelValueError> {
    match value {
        PrimitiveValueArray::Date(value) => Ok(value),
        actual => Err(DataModelValueError::PrimitiveTypeMismatch {
            expected: PrimitiveType::Date,
            actual: actual.primitive_type(),
        }),
    }
}
fn decode_list_timestamp(
    value: PrimitiveValueArray,
) -> Result<Vec<UnixTimestamp>, DataModelValueError> {
    match value {
        PrimitiveValueArray::Timestamp(value) => Ok(value),
        actual => Err(DataModelValueError::PrimitiveTypeMismatch {
            expected: PrimitiveType::Timestamp,
            actual: actual.primitive_type(),
        }),
    }
}

pub(crate) fn validate_in_memory_field_value<OperationId>(
    data_type: &ReplicatedDataType,
    value: &InMemoryFieldValue<OperationId>,
) -> Result<(), DataModelValueError> {
    match (data_type, value) {
        (
            ReplicatedDataType::LatestValueWins { value_type },
            InMemoryFieldValue::LatestValueWins(v),
        ) => {
            if v.matches_type(value_type) {
                Ok(())
            } else {
                Err(DataModelValueError::BasicTypeMismatch)
            }
        }
        (ReplicatedDataType::LinearString, InMemoryFieldValue::LinearString(_)) => Ok(()),
        (ReplicatedDataType::LinearList { value_type }, InMemoryFieldValue::LinearList(v)) => {
            ensure_primitive_type(*value_type, v.primitive_type())
        }
        (
            ReplicatedDataType::MonotonicCounter { small_range },
            InMemoryFieldValue::MonotonicCounter(v),
        ) => ensure_counter_type(*small_range, v.as_ref()),
        (
            ReplicatedDataType::TotalOrderRegister { value_type, .. },
            InMemoryFieldValue::TotalOrderRegister(v),
        ) => ensure_primitive_type(*value_type, v.primitive_type()),
        (
            ReplicatedDataType::TotalOrderFiniteStateRegister { value_type, states },
            InMemoryFieldValue::TotalOrderFiniteStateRegister(v),
        ) => ensure_finite_state_value(*value_type, states, &v.as_ref()),
        _ => Err(DataModelValueError::InvalidSnapshotValueForType),
    }
}
