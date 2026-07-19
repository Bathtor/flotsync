//! Projected row-value storage and read helpers.
//!
//! These types are the application-facing counterpart to the CRDT state rows in
//! `schema::datamodel::in_memory`. They carry only current projected values.

use crate::{
    DecodeValueError,
    schema::{
        FieldValueBuildError,
        InitialFieldValue,
        Schema,
        datamodel::{
            BasicValue,
            BasicValueRef,
            NullableBasicValue,
            NullableBasicValueRef,
            PrimitiveValueArrayRef,
            SchemaSource,
        },
        values::{PrimitiveValue, PrimitiveValueArray, PrimitiveValueRef},
    },
};
use snafu::prelude::*;
use std::{borrow::Cow, collections::HashMap, fmt, sync::Arc};

/// A projected field value that may either borrow existing value storage or own
/// a freshly projected value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProjectedFieldValue<'a> {
    /// Borrowed projected field value.
    Borrowed(NullableBasicValueRef<'a>),
    /// Owned projected field value.
    Owned(NullableBasicValue),
}

impl<'a> ProjectedFieldValue<'a> {
    /// Borrow this projected value regardless of its storage form.
    #[must_use]
    pub fn as_ref(&self) -> NullableBasicValueRef<'_> {
        match self {
            Self::Borrowed(value) => value.clone(),
            Self::Owned(value) => value.as_ref(),
        }
    }

    /// Convert the projected value into an owned value.
    #[must_use]
    pub fn into_owned(self) -> NullableBasicValue {
        match self {
            Self::Borrowed(value) => value.into_owned(),
            Self::Owned(value) => value,
        }
    }

    /// Decode this projected value into a Rust value.
    ///
    /// # Errors
    ///
    /// See `DecodeValueError` for failure conditions.
    pub fn decode<T>(self) -> Result<Cow<'a, T>, DecodeValueError>
    where
        T: ?Sized + Decode,
    {
        match self {
            Self::Borrowed(value) => T::decode(value),
            Self::Owned(value) => {
                let decoded = T::decode(value.as_ref())?;
                Ok(Cow::Owned(decoded.into_owned()))
            }
        }
    }
}

impl<'a> From<NullableBasicValueRef<'a>> for ProjectedFieldValue<'a> {
    fn from(value: NullableBasicValueRef<'a>) -> Self {
        Self::Borrowed(value)
    }
}

impl<'a> From<BasicValueRef<'a>> for ProjectedFieldValue<'a> {
    fn from(value: BasicValueRef<'a>) -> Self {
        Self::Borrowed(NullableBasicValueRef::Value(value))
    }
}

impl<'a> From<PrimitiveValueRef<'a>> for ProjectedFieldValue<'a> {
    fn from(value: PrimitiveValueRef<'a>) -> Self {
        Self::from(BasicValueRef::Primitive(value))
    }
}

impl<'a> From<PrimitiveValueArrayRef<'a>> for ProjectedFieldValue<'a> {
    fn from(value: PrimitiveValueArrayRef<'a>) -> Self {
        Self::from(BasicValueRef::Array(value))
    }
}

impl From<NullableBasicValue> for ProjectedFieldValue<'_> {
    fn from(value: NullableBasicValue) -> Self {
        Self::Owned(value)
    }
}

impl From<BasicValue> for ProjectedFieldValue<'_> {
    fn from(value: BasicValue) -> Self {
        Self::Owned(NullableBasicValue::Value(value))
    }
}

impl From<PrimitiveValue> for ProjectedFieldValue<'_> {
    fn from(value: PrimitiveValue) -> Self {
        Self::from(BasicValue::Primitive(value))
    }
}

impl From<PrimitiveValueArray> for ProjectedFieldValue<'_> {
    fn from(value: PrimitiveValueArray) -> Self {
        Self::from(BasicValue::Array(value))
    }
}

/// Marker trait for Rust types that can be decoded from a projected row value.
pub trait Decode: ToOwned {
    /// Decode one projected nullable field value.
    ///
    /// # Errors
    ///
    /// See `DecodeValueError` for failure conditions.
    fn decode(value: NullableBasicValueRef<'_>) -> Result<Cow<'_, Self>, DecodeValueError>;
}

/// Object-safe read-only view over projected row field values.
pub trait RowValueRead {
    /// Get the projected current value of a field.
    ///
    /// Returns `None` if the field does not exist.
    fn get_value(&self, field_name: &str) -> Option<ProjectedFieldValue<'_>>;
}

impl<T> RowValueRead for &T
where
    T: ?Sized + RowValueRead,
{
    fn get_value(&self, field_name: &str) -> Option<ProjectedFieldValue<'_>> {
        (*self).get_value(field_name)
    }
}

impl<T> RowValueRead for Arc<T>
where
    T: ?Sized + RowValueRead,
{
    fn get_value(&self, field_name: &str) -> Option<ProjectedFieldValue<'_>> {
        self.as_ref().get_value(field_name)
    }
}

impl<T> RowValueRead for Box<T>
where
    T: ?Sized + RowValueRead,
{
    fn get_value(&self, field_name: &str) -> Option<ProjectedFieldValue<'_>> {
        self.as_ref().get_value(field_name)
    }
}

/// Typed decode helpers layered on top of [`RowValueRead`].
///
/// The blanket implementation means these helpers are also available on
/// `&dyn RowValueRead` values whenever [`RowOperations`] is in scope.
pub trait RowOperations: RowValueRead {
    /// Get the current value of the field with `field_name` converted to `T`.
    ///
    /// # Errors
    ///
    /// Returns `Err(DecodeValueError::FieldDoesNotExist)` if the field does not exist.
    fn get_field_value<'a, T>(&'a self, field_name: &str) -> Result<Cow<'a, T>, DecodeValueError>
    where
        T: ?Sized + Decode;

    /// Get the current nullable value of the field with `field_name` converted to `T`.
    ///
    /// Returns `Ok(None)` if the field is `NULL`.
    ///
    /// # Errors
    ///
    /// Returns `Err(DecodeValueError::FieldDoesNotExist)` if the field does not exist.
    fn get_nullable_field_value<'a, T>(
        &'a self,
        field_name: &str,
    ) -> Result<Option<Cow<'a, T>>, DecodeValueError>
    where
        T: ?Sized + Decode;
}

impl<T> RowOperations for T
where
    T: ?Sized + RowValueRead,
{
    fn get_field_value<'a, Value>(
        &'a self,
        field_name: &str,
    ) -> Result<Cow<'a, Value>, DecodeValueError>
    where
        Value: ?Sized + Decode,
    {
        let field_value = self
            .get_value(field_name)
            .ok_or_else(|| field_does_not_exist(field_name))?;
        field_value.decode()
    }

    fn get_nullable_field_value<'a, Value>(
        &'a self,
        field_name: &str,
    ) -> Result<Option<Cow<'a, Value>>, DecodeValueError>
    where
        Value: ?Sized + Decode,
    {
        let field_value = self
            .get_value(field_name)
            .ok_or_else(|| field_does_not_exist(field_name))?;
        match field_value.decode() {
            Ok(value) => Ok(Some(value)),
            Err(DecodeValueError::NullValue { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }
}

/// Complete owned projected values for one row.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RowValues {
    /// Checked projected nullable field values keyed by field name.
    fields: HashMap<String, NullableBasicValue>,
}

impl RowValues {
    /// Create a complete projected row from field values after schema validation.
    ///
    /// # Errors
    ///
    /// Returns `InMemoryValueDataError` when the supplied fields are not exactly
    /// the schema fields or when at least one value cannot initialise its field.
    pub fn try_from_fields(
        schema: &Schema,
        fields: HashMap<String, NullableBasicValue>,
    ) -> Result<Self, InMemoryValueDataError> {
        validate_complete_value_row(schema, &fields)?;
        Ok(Self { fields })
    }

    /// Create a projected row without validating it against a schema.
    ///
    /// This is only for wire formats that must decode a row before the relevant
    /// schema is available. Callers must validate the row with
    /// [`Self::validate_against_schema`] before embedding state or exposing
    /// it as checked application data.
    #[must_use]
    pub fn from_fields_unchecked(fields: HashMap<String, NullableBasicValue>) -> Self {
        Self { fields }
    }

    /// Borrow the checked field values keyed by field name.
    #[must_use]
    pub fn fields(&self) -> &HashMap<String, NullableBasicValue> {
        &self.fields
    }

    /// Return the number of fields stored in this row.
    #[must_use]
    pub fn field_count(&self) -> usize {
        self.fields.len()
    }

    /// Return one projected value by field name.
    #[must_use]
    pub fn get(&self, field_name: &str) -> Option<&NullableBasicValue> {
        self.fields.get(field_name)
    }

    /// Build a complete projected row from any value-readable row.
    ///
    /// # Errors
    ///
    /// Returns `InMemoryValueDataError` if the source row does not expose every
    /// schema field or if any value is incompatible with the corresponding
    /// schema field.
    pub fn from_row(
        schema: &Schema,
        row: &impl RowValueRead,
    ) -> Result<Self, InMemoryValueDataError> {
        let mut fields = HashMap::with_capacity(schema.columns.len());
        for field_name in schema.columns.keys() {
            let value = row
                .get_value(field_name)
                .ok_or_else(|| InMemoryValueDataError::MissingField {
                    field_name: field_name.clone(),
                })?
                .into_owned();
            fields.insert(field_name.clone(), value);
        }
        Self::try_from_fields(schema, fields)
    }

    /// Convert this complete value row into schema initial field values in a
    /// caller-supplied field order.
    ///
    /// # Errors
    ///
    /// Returns `InMemoryValueDataError` if the row is not complete for `schema`,
    /// if `field_names` does not name schema fields, or if any value cannot
    /// initialise its field.
    pub(crate) fn initial_values_in_field_order<'schema>(
        &self,
        schema: &'schema Schema,
        field_names: &[String],
    ) -> Result<Vec<InitialFieldValue<'schema>>, InMemoryValueDataError> {
        validate_complete_value_row(schema, &self.fields)?;
        let mut initial_values = Vec::with_capacity(field_names.len());
        for field_name in field_names {
            let field = schema.columns.get(field_name.as_str()).ok_or_else(|| {
                InMemoryValueDataError::UnknownField {
                    field_name: field_name.clone(),
                }
            })?;
            let value = self.fields.get(field_name.as_str()).ok_or_else(|| {
                InMemoryValueDataError::MissingField {
                    field_name: field_name.clone(),
                }
            })?;
            let initial_value = field
                .initial(value.clone())
                .context(InvalidFieldValueSnafu {
                    field_name: field_name.clone(),
                })?;
            initial_values.push(initial_value);
        }
        Ok(initial_values)
    }

    /// Validate this row against `schema`.
    ///
    /// # Errors
    ///
    /// Returns `InMemoryValueDataError` if the row is not complete or any value
    /// does not match the corresponding schema field.
    pub fn validate_against_schema(&self, schema: &Schema) -> Result<(), InMemoryValueDataError> {
        validate_complete_value_row(schema, &self.fields)
    }
}

impl RowValueRead for RowValues {
    fn get_value(&self, field_name: &str) -> Option<ProjectedFieldValue<'_>> {
        self.fields
            .get(field_name)
            .map(|value| ProjectedFieldValue::Borrowed(value.as_ref()))
    }
}

/// Row metadata stored alongside one row-major value chunk in [`InMemoryValueData`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct InMemoryValueRowMeta<RowId> {
    /// Stable row identifier in the dataset.
    pub row_id: RowId,
    /// Whether this row is a retained tombstone.
    pub tombstoned: bool,
}

impl<RowId> fmt::Display for InMemoryValueRowMeta<RowId>
where
    RowId: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.tombstoned {
            write!(f, "{} ✝", self.row_id)
        } else {
            self.row_id.fmt(f)
        }
    }
}

/// Compact in-memory batch of projected row values for one schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InMemoryValueData<RowId> {
    /// Shared schema used to validate and decode every row in this batch.
    schema: SchemaSource,
    /// Stable sorted field order for the flat value vector.
    field_names: Vec<String>,
    /// Field-name lookup table into `field_names`.
    field_index_by_name: HashMap<String, usize>,
    /// Per-row metadata in the same order as the flat value chunks.
    rows: Vec<InMemoryValueRowMeta<RowId>>,
    /// Row-major projected field values laid out as `row_offset + field_index`.
    values: Vec<NullableBasicValue>,
}

impl<RowId> InMemoryValueData<RowId> {
    /// Create an empty projected-value batch for `schema`.
    #[must_use]
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
            rows: Vec::new(),
            values: Vec::new(),
        }
    }

    /// Create an empty projected-value batch and reserve enough allocation for
    /// `row_capacity` rows.
    #[must_use]
    pub fn with_row_capacity(schema: impl Into<SchemaSource>, row_capacity: usize) -> Self {
        let mut data = Self::new(schema);
        data.reserve_rows(row_capacity);
        data
    }

    /// Return the schema shared by every row in the batch.
    #[must_use]
    pub fn schema(&self) -> &Schema {
        self.schema.as_schema()
    }

    /// Reserve storage for at least `additional_rows` more rows.
    pub fn reserve_rows(&mut self, additional_rows: usize) {
        self.rows.reserve(additional_rows);
        let additional_values = additional_rows.saturating_mul(self.field_names.len());
        self.values.reserve(additional_values);
    }

    /// Return the number of rows in the batch.
    #[must_use]
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Return whether the batch contains no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Remove all rows while keeping row and value allocations for reuse.
    pub fn clear_rows(&mut self) {
        self.rows.clear();
        self.values.clear();
    }

    /// Append one complete projected row to the batch.
    ///
    /// # Errors
    ///
    /// Returns `InMemoryValueDataError` if the row is not complete or any value
    /// does not match the schema.
    pub fn push_row(
        &mut self,
        row_id: RowId,
        tombstoned: bool,
        row: &RowValues,
    ) -> Result<(), InMemoryValueDataError> {
        self.push_row_read(row_id, tombstoned, row)
    }

    /// Append one complete projected row from a read-only row view.
    ///
    /// # Errors
    ///
    /// Returns `InMemoryValueDataError` if the row does not expose every schema
    /// field or if any value does not match the schema.
    pub fn push_row_read(
        &mut self,
        row_id: RowId,
        tombstoned: bool,
        row: &impl RowValueRead,
    ) -> Result<(), InMemoryValueDataError> {
        let mut row_values = Vec::with_capacity(self.field_names.len());
        for field_name in &self.field_names {
            let value = row
                .get_value(field_name)
                .ok_or_else(|| InMemoryValueDataError::MissingField {
                    field_name: field_name.clone(),
                })?
                .into_owned();
            let field = self
                .schema()
                .columns
                .get(field_name.as_str())
                .ok_or_else(|| InMemoryValueDataError::UnknownField {
                    field_name: field_name.clone(),
                })?;
            field.can_initial(&value).context(InvalidFieldValueSnafu {
                field_name: field_name.clone(),
            })?;
            row_values.push(value);
        }
        let row_index = self.rows.len();
        self.rows.push(InMemoryValueRowMeta { row_id, tombstoned });
        self.values.extend(row_values);
        debug_assert_eq!(self.values.len(), (row_index + 1) * self.field_names.len());
        Ok(())
    }

    /// Return one row view by row index.
    #[must_use]
    pub fn row(&self, row_index: usize) -> Option<InMemoryValueDataRowRef<'_, RowId>> {
        if row_index >= self.rows.len() {
            return None;
        }
        let row_value_offset = row_index.checked_mul(self.field_names.len())?;
        Some(InMemoryValueDataRowRef {
            data: self,
            row_index,
            row_value_offset,
        })
    }

    /// Iterate over all row views in order.
    pub fn rows(&self) -> impl Iterator<Item = InMemoryValueDataRowRef<'_, RowId>> {
        (0..self.rows.len()).filter_map(|row_index| self.row(row_index))
    }
}

/// Borrowed row view into [`InMemoryValueData`].
#[derive(Clone, Copy, Debug)]
pub struct InMemoryValueDataRowRef<'a, RowId> {
    /// Batch that owns the schema, row metadata, and flat value vector.
    data: &'a InMemoryValueData<RowId>,
    /// Row index into `data.rows` and into the flat value vector.
    row_index: usize,
    /// Start index of this row's value chunk inside `data.values`.
    row_value_offset: usize,
}

impl<'a, RowId> InMemoryValueDataRowRef<'a, RowId> {
    /// Return row metadata.
    #[must_use]
    pub fn metadata(&self) -> &'a InMemoryValueRowMeta<RowId> {
        &self.data.rows[self.row_index]
    }

    /// Return this row's stable id.
    #[must_use]
    pub fn row_id(&self) -> &'a RowId {
        &self.metadata().row_id
    }

    /// Return whether this row is a retained tombstone.
    #[must_use]
    pub fn is_tombstoned(&self) -> bool {
        self.metadata().tombstoned
    }
}

impl<RowId> RowValueRead for InMemoryValueDataRowRef<'_, RowId> {
    fn get_value(&self, field_name: &str) -> Option<ProjectedFieldValue<'_>> {
        let field_index = self.data.field_index_by_name.get(field_name)?;
        let value_index = self.row_value_offset.checked_add(*field_index)?;
        self.data
            .values
            .get(value_index)
            .map(|value| ProjectedFieldValue::Borrowed(value.as_ref()))
    }
}

/// Errors raised while building compact projected-value rows.
#[derive(Clone, Debug, PartialEq, Snafu)]
pub enum InMemoryValueDataError {
    /// A supplied field does not exist in the schema.
    #[snafu(display("Unknown field '{field_name}'."))]
    UnknownField { field_name: String },
    /// A required schema field is missing from a complete row.
    #[snafu(display("Missing required field '{field_name}'."))]
    MissingField { field_name: String },
    /// The row had an unexpected field count.
    #[snafu(display("Row has wrong number of fields: expected {expected}, got {actual}."))]
    FieldCountMismatch { expected: usize, actual: usize },
    /// A field value is incompatible with the schema.
    #[snafu(display("Field '{field_name}' has a value incompatible with the schema."))]
    InvalidFieldValue {
        field_name: String,
        source: FieldValueBuildError,
    },
}

fn field_does_not_exist(field_name: &str) -> DecodeValueError {
    DecodeValueError::FieldDoesNotExist {
        field_name: field_name.to_owned(),
    }
}

/// Validate that `fields` is a complete row for `schema`.
///
/// This checks three invariants: the row has exactly the same number of fields
/// as the schema, every supplied field name belongs to the schema, and each
/// schema field has a value that can initialise that field.
fn validate_complete_value_row(
    schema: &Schema,
    fields: &HashMap<String, NullableBasicValue>,
) -> Result<(), InMemoryValueDataError> {
    if fields.len() != schema.columns.len() {
        return Err(InMemoryValueDataError::FieldCountMismatch {
            expected: schema.columns.len(),
            actual: fields.len(),
        });
    }

    for field_name in fields.keys() {
        if !schema.columns.contains_key(field_name.as_str()) {
            return Err(InMemoryValueDataError::UnknownField {
                field_name: field_name.clone(),
            });
        }
    }

    for (field_name, field) in &schema.columns {
        let Some(value) = fields.get(field_name.as_str()) else {
            return Err(InMemoryValueDataError::MissingField {
                field_name: field_name.clone(),
            });
        };
        field.can_initial(value).context(InvalidFieldValueSnafu {
            field_name: field_name.clone(),
        })?;
    }
    Ok(())
}
