use super::{
    validation::{ensure_counter_type, ensure_finite_state_value, ensure_primitive_type},
    *,
};
use crate::{
    DataOperation,
    IdWithIndex,
    InMemoryValueDataError,
    OperationOutcome,
    ProjectedFieldValue,
    RowStateRead,
    RowValueRead,
    RowValues,
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
use std::{collections::HashMap, fmt, hash::Hash, marker::PhantomData};

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
pub struct InMemoryStateData<RowId, OperationId>
where
    RowId: PartialEq + Eq + Hash,
{
    schema: SchemaSource,
    field_names: Vec<String>,
    field_index_by_name: HashMap<String, usize>,
    row_id_map: HashMap<RowId, usize>,
    rows: Vec<InMemoryStateRow<OperationId>>,
}
impl<RowId, OperationId> InMemoryStateData<RowId, OperationId>
where
    RowId: PartialEq + Eq + Hash,
{
    /// Create an empty in-memory dataset for `schema`.
    pub fn new(schema: impl Into<SchemaSource>) -> Self {
        let schema = schema.into();
        let schema_ref = schema.as_schema();
        let mut field_names = Vec::with_capacity(schema_ref.columns.len());
        let mut field_index_by_name = HashMap::with_capacity(schema_ref.columns.len());

        field_names.extend(schema_ref.columns.keys().cloned());
        field_names.sort();
        for (index, field_name) in field_names.iter().enumerate() {
            field_index_by_name.insert(field_name.clone(), index);
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
    #[must_use]
    pub fn with_static_schema(schema: &'static Schema) -> Self {
        Self::new(schema)
    }

    /// Create an empty in-memory dataset owning `schema`.
    #[must_use]
    pub fn with_owned_schema(schema: Schema) -> Self {
        Self::new(schema)
    }

    /// Create one in-memory dataset from complete row snapshots and a `'static` schema.
    ///
    /// # Errors
    ///
    /// See `InMemoryStateDataError` for failure conditions.
    pub fn with_static_schema_and_row_snapshots<'snapshot, I>(
        schema: &'static Schema,
        rows: I,
    ) -> Result<Self, InMemoryStateDataError>
    where
        I: IntoIterator<Item = (RowId, RowStateSnapshot<'snapshot, OperationId>)>,
        RowId: fmt::Display,
        OperationId: Clone + 'snapshot,
    {
        Self::from_row_snapshots(schema, rows)
    }

    /// Create one in-memory dataset from complete row snapshots and an owned schema.
    ///
    /// # Errors
    ///
    /// See `InMemoryStateDataError` for failure conditions.
    pub fn with_owned_schema_and_row_snapshots<'snapshot, I>(
        schema: Schema,
        rows: I,
    ) -> Result<Self, InMemoryStateDataError>
    where
        I: IntoIterator<Item = (RowId, RowStateSnapshot<'snapshot, OperationId>)>,
        RowId: fmt::Display,
        OperationId: Clone + 'snapshot,
    {
        Self::from_row_snapshots(schema, rows)
    }

    /// Create one in-memory dataset from complete row snapshots.
    ///
    /// Each snapshot must contain the full row state for the associated schema.
    ///
    /// # Errors
    ///
    /// See `InMemoryStateDataError` for failure conditions.
    pub fn from_row_snapshots<'snapshot, I>(
        schema: impl Into<SchemaSource>,
        rows: I,
    ) -> Result<Self, InMemoryStateDataError>
    where
        I: IntoIterator<Item = (RowId, RowStateSnapshot<'snapshot, OperationId>)>,
        RowId: fmt::Display,
        OperationId: Clone + 'snapshot,
    {
        let mut data = Self::new(schema);
        for (row_id, snapshot) in rows {
            data.push_row_snapshot(row_id, snapshot)?;
        }
        Ok(data)
    }

    /// Create one in-memory dataset from complete row snapshots and retained
    /// row tombstone flags.
    ///
    /// # Errors
    ///
    /// See `InMemoryStateDataError` for failure conditions.
    pub fn from_row_snapshots_with_tombstones<'snapshot, I>(
        schema: impl Into<SchemaSource>,
        rows: I,
    ) -> Result<Self, InMemoryStateDataError>
    where
        I: IntoIterator<Item = RowRecord<'snapshot, RowId, OperationId>>,
        RowId: fmt::Display,
        OperationId: Clone + 'snapshot,
    {
        let mut data = Self::new(schema);
        for record in rows {
            data.push_row_record(record)?;
        }
        Ok(data)
    }

    /// Embed complete projected value rows into deterministic CRDT state.
    ///
    /// Callers should pass a stable synthetic origin when this is used for
    /// group initial state, such as `UpdateId::INITIAL_STATE_ORIGIN` in the
    /// replication crate.
    ///
    /// # Errors
    ///
    /// Returns `InitialValueRowsEmbeddingError` if a value row does not
    /// match the schema or if inserting the derived initial state fails.
    pub fn from_initial_value_rows<I>(
        schema: impl Into<SchemaSource>,
        rows: I,
        initial_origin: &OperationId,
    ) -> Result<Self, InitialValueRowsEmbeddingError<RowId>>
    where
        I: IntoIterator<Item = (RowId, RowValues)>,
        RowId: Clone + fmt::Debug + fmt::Display,
        OperationId:
            Clone + fmt::Debug + fmt::Display + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
    {
        let schema = schema.into();
        let mut data = Self::new(schema.clone());
        let field_names = data.field_names.clone();
        for (row_id, row) in rows {
            let initial_values = row
                .initial_values_in_field_order(schema.as_schema(), &field_names)
                .with_context(|_| initial_value_rows_embedding::SchemaMismatchSnafu {
                    row_id: row_id.clone(),
                })?;
            let error_row_id = row_id.clone();
            data.insert_row((*initial_origin).clone(), row_id, initial_values)
                .with_context(|_| initial_value_rows_embedding::StateInsertSnafu {
                    row_id: error_row_id,
                })?;
        }
        Ok(data)
    }

    /// Get the immutable schema associated with this dataset.
    #[must_use]
    pub fn schema(&self) -> &Schema {
        self.schema.as_schema()
    }

    /// Return the number of fields expected in every row.
    #[must_use]
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
    #[must_use]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Return `true` when no rows are stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Return the number of rows that are not tombstoned.
    #[must_use]
    pub fn num_active_rows(&self) -> usize {
        self.rows.iter().filter(|row| !row.deleted).count()
    }

    /// Iterate the ids of rows that are not currently tombstoned.
    ///
    /// # Panics
    ///
    /// Panics if the row-id index points outside the row storage.
    pub fn active_row_ids(&self) -> impl Iterator<Item = &RowId> {
        self.row_id_map.iter().filter_map(|(row_id, row_index)| {
            let row = self
                .rows
                .get(*row_index)
                .expect("row_id_map and rows must stay in sync");
            (!row.deleted).then_some(row_id)
        })
    }

    /// Return whether an addressable row is currently tombstoned.
    pub fn row_is_tombstoned(&self, row_id: &RowId) -> Option<bool> {
        let row_index = self.row_id_map.get(row_id)?;
        Some(self.rows[*row_index].deleted)
    }

    /// Validate and append one row represented by positional field values.
    ///
    /// Returns the inserted row index on success.
    #[allow(dead_code, reason = "Maybe remove this if it remains unused")]
    pub(crate) fn push_row_from_field_values(
        &mut self,
        fields: Vec<InMemoryFieldState<OperationId>>,
    ) -> Result<usize, InMemoryStateDataError> {
        let row = self.row_from_field_values(fields)?;
        self.rows.push(row);
        Ok(self.rows.len() - 1)
    }

    /// Validate and append one row represented by `(field_name, value)` pairs.
    ///
    /// Field names must be unique and complete for the schema.
    /// Returns the inserted row index on success.
    ///
    /// # Errors
    ///
    /// See `InMemoryStateDataError` for failure conditions.
    pub fn push_row_from_named_fields<I, Name>(
        &mut self,
        fields: I,
    ) -> Result<usize, InMemoryStateDataError>
    where
        I: IntoIterator<Item = (Name, InMemoryFieldState<OperationId>)>,
        Name: AsRef<str>,
    {
        let row = self.row_from_named_fields(fields)?;
        self.rows.push(row);
        Ok(self.rows.len() - 1)
    }

    /// Validate and append one row represented by a complete schema snapshot.
    ///
    /// # Errors
    ///
    /// See `InMemoryStateDataError` for failure conditions.
    pub fn push_row_snapshot<'snapshot>(
        &mut self,
        row_id: RowId,
        snapshot: RowStateSnapshot<'snapshot, OperationId>,
    ) -> Result<usize, InMemoryStateDataError>
    where
        RowId: fmt::Display,
        OperationId: Clone + 'snapshot,
    {
        self.push_row_record(RowRecord {
            row_id,
            snapshot,
            tombstoned: false,
        })
    }

    /// Validate and append one retained row record.
    ///
    /// # Errors
    ///
    /// See `InMemoryStateDataError` for failure conditions.
    pub fn push_row_record<'snapshot>(
        &mut self,
        record: RowRecord<'snapshot, RowId, OperationId>,
    ) -> Result<usize, InMemoryStateDataError>
    where
        RowId: fmt::Display,
        OperationId: Clone + 'snapshot,
    {
        let RowRecord {
            row_id,
            snapshot,
            tombstoned,
        } = record;
        if self.row_id_map.contains_key(&row_id) {
            return Err(InMemoryStateDataError::DuplicateRowId {
                row_id: row_id.to_string(),
            });
        }

        let mut row = self.row_from_named_fields(snapshot.into_owned_fields())?;
        row.deleted = tombstoned;
        self.rows.push(row);
        let index = self.rows.len() - 1;
        self.row_id_map.insert(row_id, index);
        Ok(index)
    }

    /// Validate an existing row by index against this dataset's schema.
    ///
    /// # Errors
    ///
    /// See `InMemoryStateDataError` for failure conditions.
    pub fn validate_row(&self, row_index: usize) -> Result<(), InMemoryStateDataError> {
        let row = self.row(row_index)?;
        self.validate_row_value(row)
    }

    /// Get one field value by row index and field name.
    ///
    /// # Errors
    ///
    /// See `InMemoryStateDataError` for failure conditions.
    pub fn field(
        &self,
        row_index: usize,
        field_name: &str,
    ) -> Result<&InMemoryFieldState<OperationId>, InMemoryStateDataError> {
        let field_index =
            self.field_index(field_name)
                .ok_or_else(|| InMemoryStateDataError::UnknownField {
                    field_name: field_name.to_owned(),
                })?;
        let row = self.row(row_index)?;
        Ok(&row.fields[field_index])
    }

    /// Get a mutable reference to one field value by row index and field name.
    ///
    /// # Errors
    ///
    /// See `InMemoryStateDataError` for failure conditions.
    pub fn field_mut(
        &mut self,
        row_index: usize,
        field_name: &str,
    ) -> Result<&mut InMemoryFieldState<OperationId>, InMemoryStateDataError> {
        let field_index =
            self.field_index(field_name)
                .ok_or_else(|| InMemoryStateDataError::UnknownField {
                    field_name: field_name.to_owned(),
                })?;
        let row = self.row_mut(row_index)?;
        Ok(&mut row.fields[field_index])
    }

    /// Iterate `(field_name, field_value)` pairs for one row.
    ///
    /// # Errors
    ///
    /// See `InMemoryStateDataError` for failure conditions.
    pub fn iter_row_fields(
        &self,
        row_index: usize,
    ) -> Result<
        impl Iterator<Item = (&str, &InMemoryFieldState<OperationId>)>,
        InMemoryStateDataError,
    > {
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
    ///
    /// # Errors
    ///
    /// See `InMemoryStateDataSnapshotEncodeError<E::Error>` for failure conditions.
    pub fn encode_data_snapshots<E>(
        &self,
        encoder: &mut E,
    ) -> Result<(), InMemoryStateDataSnapshotEncodeError<E::Error>>
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
    ///
    /// # Errors
    ///
    /// See `InMemoryStateDataSnapshotDecodeError<D::Error>` for failure conditions.
    pub fn decode_data_snapshots<D>(
        schema: impl Into<SchemaSource>,
        decoder: &mut D,
    ) -> Result<Self, InMemoryStateDataSnapshotDecodeError<D::Error>>
    where
        OperationId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
        D: DataSnapshotDecoder<OperationId>,
    {
        let row_count = decoder.begin().context(DecoderSnafu)?;
        let mut data = Self::new(schema);
        data.rows.reserve(row_count);

        for row_index in 0..row_count {
            let mut row_decoder = decoder.begin_row(row_index).context(DecoderSnafu)?;
            let row = InMemoryStateRow::decode_snapshot(
                data.schema(),
                &data.field_names,
                &mut row_decoder,
            )?;
            drop(row_decoder);
            decoder.end_row(row_index).context(DecoderSnafu)?;

            data.validate_row_value(&row)
                .context(InMemoryStateDataSnafu)?;
            data.rows.push(row);
        }

        decoder.end().context(DecoderSnafu)?;
        Ok(data)
    }

    fn row_from_field_values(
        &self,
        fields: Vec<InMemoryFieldState<OperationId>>,
    ) -> Result<InMemoryStateRow<OperationId>, InMemoryStateDataError> {
        let row = InMemoryStateRow::new(fields);
        self.validate_row_value(&row)?;
        Ok(row)
    }

    fn validate_row_value(
        &self,
        row: &InMemoryStateRow<OperationId>,
    ) -> Result<(), InMemoryStateDataError> {
        if row.field_count() != self.num_fields() {
            return Err(InMemoryStateDataError::FieldCountMismatch {
                expected: self.num_fields(),
                actual: row.field_count(),
            });
        }

        for (field_index, value) in row.fields.iter().enumerate() {
            let field_name = self.field_names[field_index].as_str();
            let schema_field = self
                .schema()
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
    ) -> Result<InMemoryStateRow<OperationId>, InMemoryStateDataError>
    where
        I: IntoIterator<Item = (Name, InMemoryFieldState<OperationId>)>,
        Name: AsRef<str>,
    {
        let mut row_slots: Vec<Option<InMemoryFieldState<OperationId>>> =
            std::iter::repeat_with(|| None)
                .take(self.field_names.len())
                .collect();

        for (field_name, value) in fields {
            let field_name = field_name.as_ref();
            let Some(field_index) = self.field_index(field_name) else {
                return Err(InMemoryStateDataError::UnknownField {
                    field_name: field_name.to_owned(),
                });
            };
            if row_slots[field_index].is_some() {
                return Err(InMemoryStateDataError::DuplicateField {
                    field_name: field_name.to_owned(),
                });
            }

            let schema_field = self
                .schema()
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
            return Err(InMemoryStateDataError::MissingField {
                field_name: self.field_names[missing_index].clone(),
            });
        }

        let fields = row_slots
            .into_iter()
            .map(|slot| slot.expect("all slots were checked for missing values"))
            .collect();
        Ok(InMemoryStateRow::new(fields))
    }

    fn row_from_snapshot_with_defaults(
        &self,
        fields: Vec<(String, InMemoryFieldState<OperationId>)>,
        change_id: &OperationId,
    ) -> crate::OperationResult<InMemoryStateRow<OperationId>>
    where
        OperationId:
            Clone + fmt::Debug + fmt::Display + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
    {
        let mut row_slots: Vec<Option<InMemoryFieldState<OperationId>>> =
            std::iter::repeat_with(|| None)
                .take(self.field_names.len())
                .collect();

        for (field_name, value) in fields {
            let Some(field_index) = self.field_index(field_name.as_str()) else {
                return Err(crate::OperationError::SchemaValue {
                    source: SchemaValueError::UnknownField { field_name },
                });
            };
            if row_slots[field_index].is_some() {
                return Err(crate::OperationError::SchemaValue {
                    source: SchemaValueError::DuplicateField { field_name },
                });
            }

            let schema_field = self
                .schema()
                .columns
                .get(field_name.as_str())
                .expect("field_names and schema are in sync");
            validate_in_memory_field_value(&schema_field.data_type, &value).map_err(|source| {
                crate::OperationError::SchemaValue {
                    source: SchemaValueError::InvalidSnapshotFieldValue {
                        field_name: field_name.clone(),
                        source,
                    },
                }
            })?;

            row_slots[field_index] = Some(value);
        }

        for (field_index, row_slot) in row_slots.iter_mut().enumerate() {
            if row_slot.is_some() {
                continue;
            }

            let field_name = self.field_names[field_index].as_str();
            let schema_field = self
                .schema()
                .columns
                .get(field_name)
                .expect("field_names and schema are in sync");
            let default_value = materialize_default_target_value(field_name, schema_field)?;
            let Some(default_value) = default_value else {
                return Err(crate::OperationError::SchemaValue {
                    source: SchemaValueError::MissingField {
                        field_name: field_name.to_owned(),
                    },
                });
            };
            let field_value = build_initial_field_value(
                field_name,
                &schema_field.data_type,
                &default_value,
                change_id,
            )?;
            *row_slot = Some(field_value);
        }

        let fields = row_slots
            .into_iter()
            .map(|slot| slot.expect("all row slots are resolved after default materialization"))
            .collect();
        Ok(InMemoryStateRow::new(fields))
    }

    fn row(
        &self,
        row_index: usize,
    ) -> Result<&InMemoryStateRow<OperationId>, InMemoryStateDataError> {
        self.rows
            .get(row_index)
            .ok_or(InMemoryStateDataError::UnknownRow { row_index })
    }

    fn row_mut(
        &mut self,
        row_index: usize,
    ) -> Result<&mut InMemoryStateRow<OperationId>, InMemoryStateDataError> {
        self.rows
            .get_mut(row_index)
            .ok_or(InMemoryStateDataError::UnknownRow { row_index })
    }

    /// Apply a replicated schema operation to this dataset.
    ///
    /// The method consumes `self` and only returns an updated instance if the full operation
    /// applies successfully.
    ///
    /// # Errors
    ///
    /// See `OperationError` for failure conditions.
    pub fn apply_schema_operation(
        mut self,
        operation: SchemaOperation<'_, RowId, OperationId>,
    ) -> crate::OperationResult<Self>
    where
        OperationId:
            Clone + fmt::Debug + fmt::Display + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
        RowId: Clone + fmt::Display,
    {
        operation
            .validate_against_schema(self.schema())
            .context(crate::SchemaValueSnafu)?;

        let SchemaOperation {
            change_id,
            operation: row_operation,
        } = operation;
        match row_operation {
            RowOperation::Insert { row_id, snapshot } => {
                if self.row_id_map.contains_key(&row_id) {
                    return crate::DuplicateRowIdSnafu {
                        row_id: row_id.to_string(),
                    }
                    .fail();
                }

                let row =
                    self.row_from_snapshot_with_defaults(snapshot.into_owned_fields(), &change_id)?;
                let index = self.rows.len();
                self.rows.push(row);
                self.row_id_map.insert(row_id, index);
            }
            RowOperation::Update { row_id, fields } => {
                let row_index = *self.row_id_map.get(&row_id).ok_or_else(|| {
                    crate::OperationError::UnknownRowId {
                        row_id: row_id.to_string(),
                    }
                })?;

                let mut indexed_fields = Vec::with_capacity(fields.len());
                for field in fields {
                    let field_name = field.field_name.into_owned();
                    let field_index = self.field_index(field_name.as_str()).ok_or_else(|| {
                        crate::OperationError::SchemaValue {
                            source: SchemaValueError::UnknownField {
                                field_name: field_name.clone(),
                            },
                        }
                    })?;
                    indexed_fields.push((field_index, field.value));
                }

                let row = self
                    .row_mut(row_index)
                    .context(crate::InMemoryStateDataSnafu)?;
                for (field_index, operation_value) in indexed_fields {
                    apply_operation_value(&mut row.fields[field_index], operation_value)?;
                }
            }
            RowOperation::Delete { row_id } => {
                let row_index = *self.row_id_map.get(&row_id).ok_or_else(|| {
                    crate::OperationError::UnknownRowId {
                        row_id: row_id.to_string(),
                    }
                })?;
                if !self.rows[row_index].deleted {
                    self.rows[row_index].deleted = true;
                }
            }
        }

        Ok(self)
    }
}

mod operations;

use operations::{
    apply_operation_value,
    build_initial_field_value,
    materialize_default_target_value,
};

#[derive(Copy)]
pub struct InMemoryStateDataRow<'a, RowId, OperationId>
where
    RowId: PartialEq + Eq + Hash,
{
    data: &'a InMemoryStateData<RowId, OperationId>,
    row_index: usize,
}
impl<RowId, OperationId> Clone for InMemoryStateDataRow<'_, RowId, OperationId>
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
impl<RowId, OperationId> InMemoryStateDataRow<'_, RowId, OperationId>
where
    RowId: PartialEq + Eq + Hash,
{
    /// Materialise this row into a complete owned snapshot.
    #[must_use]
    pub fn snapshot(&self) -> RowStateSnapshot<'static, OperationId>
    where
        OperationId: Clone,
    {
        RowStateSnapshot::borrowed_in_memory(
            &self.data.field_names,
            &self.data.rows[self.row_index],
        )
        .into_owned()
    }

    /// Return whether this retained row is currently tombstoned.
    #[must_use]
    pub fn is_tombstoned(&self) -> bool {
        self.data.rows[self.row_index].deleted
    }
}
impl<RowId, OperationId> RowStateRead<OperationId> for InMemoryStateDataRow<'_, RowId, OperationId>
where
    RowId: PartialEq + Eq + Hash,
{
    fn get_field(&self, field_name: &str) -> Option<&InMemoryFieldState<OperationId>> {
        let field_index = self.data.field_index(field_name)?;
        let row = self.data.row(self.row_index).ok()?;
        row.fields.get(field_index)
    }
}

impl<RowId, OperationId> RowValueRead for InMemoryStateDataRow<'_, RowId, OperationId>
where
    RowId: PartialEq + Eq + Hash,
    OperationId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
{
    fn get_value(&self, field_name: &str) -> Option<ProjectedFieldValue<'_>> {
        self.get_field(field_name)
            .map(InMemoryFieldState::project_value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum InMemoryStateDataError {
    #[snafu(display("Unknown field '{field_name}'."))]
    UnknownField { field_name: String },
    #[snafu(display("Field '{field_name}' was provided more than once."))]
    DuplicateField { field_name: String },
    #[snafu(display("Row '{row_id}' was provided more than once."))]
    DuplicateRowId { row_id: String },
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

/// Errors raised while embedding checked projected value rows as CRDT state.
#[derive(Debug, Snafu)]
#[snafu(module(initial_value_rows_embedding))]
pub enum InitialValueRowsEmbeddingError<RowId: fmt::Display> {
    /// One projected value row was incomplete or incompatible with its schema.
    #[snafu(display("Initial value row '{row_id}' does not match its schema: {source}"))]
    SchemaMismatch {
        row_id: RowId,
        #[snafu(source(from(InMemoryValueDataError, Box::new)))]
        source: Box<InMemoryValueDataError>,
    },
    /// The schema-compatible row could not be inserted as CRDT state.
    #[snafu(display("Initial value row '{row_id}' could not be inserted as state: {source}"))]
    StateInsert {
        row_id: RowId,
        source: crate::OperationError,
    },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum InMemoryStateDataSnapshotEncodeError<E>
where
    E: snafu::Error + Send + Sync + 'static,
{
    #[snafu(display("Failed to encode row snapshot fields against the schema: {source}"))]
    SchemaVisit { source: SchemaVisitError<E> },
    #[snafu(display("Dataset snapshot encoder failed."))]
    Encoder { source: E },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum InMemoryStateDataSnapshotDecodeError<E>
where
    E: snafu::Error + Send + Sync + 'static,
{
    #[snafu(display("Decoded row is invalid for the in-memory dataset."))]
    InMemoryStateData { source: InMemoryStateDataError },
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
#[snafu(visibility(pub(crate)))]
pub enum InMemoryNodeDecodeError<E>
where
    E: snafu::Error + Send + Sync + 'static,
{
    #[snafu(display("Snapshot node source failed while decoding history nodes."))]
    Source { source: E },
    #[snafu(display("Snapshot node value is incompatible with the schema data type."))]
    InvalidNodeValue { source: DataModelValueError },
}

mod field_state;
mod snapshots;

pub use field_state::{InMemoryFieldState, LinearLatestValueWinsState, LinearListState};
pub(crate) use field_state::{InMemoryStateRow, validate_in_memory_field_value};

#[allow(
    clippy::wildcard_imports,
    reason = "The parent module reuses the local snapshot implementation details across its state and operation code."
)]
use snapshots::*;
