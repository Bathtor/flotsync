use super::*;
use crate::{
    IdWithIndex,
    any_data::{LinearLatestValueWins, list::LinearList},
    snapshot::{SnapshotHeader, SnapshotNode, SnapshotNodeRef, SnapshotReadError, SnapshotSink},
    text::LinearString,
};
use chrono::NaiveDate;
use ordered_float::OrderedFloat;
use std::{borrow::Cow, collections::HashMap, fmt, hash::Hash, marker::PhantomData};

/// Storage-level, in-memory dataset for one schema.
///
/// The schema is immutable while this value exists.
/// Field names are mapped once to positional indices and rows only store per-field values.
#[derive(Clone, Debug, PartialEq)]
pub struct InMemoryData<RowId> {
    schema: Cow<'static, Schema>,
    field_names: Vec<String>,
    field_index_by_name: HashMap<String, usize>,
    rows: Vec<InMemoryRow<RowId>>,
}
impl<RowId> InMemoryData<RowId> {
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

    /// Validate and append one row represented by positional field values.
    ///
    /// Returns the inserted row index on success.
    #[allow(dead_code, reason = "Maybe remove this if it remains unused")]
    pub(crate) fn push_row_from_field_values(
        &mut self,
        fields: Vec<InMemoryFieldValue<RowId>>,
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
        I: IntoIterator<Item = (Name, InMemoryFieldValue<RowId>)>,
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
    ) -> Result<&InMemoryFieldValue<RowId>, InMemoryDataError> {
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
    ) -> Result<&mut InMemoryFieldValue<RowId>, InMemoryDataError> {
        let field_index =
            self.field_index(field_name)
                .ok_or_else(|| InMemoryDataError::UnknownField {
                    field_name: field_name.to_owned(),
                })?;
        let row = self.row_mut(row_index)?;
        Ok(&mut row.fields[field_index])
    }

    // /// Replace one field value in a row after validating against the schema field type.
    // pub fn set_field(
    //     &mut self,
    //     row_index: usize,
    //     field_name: &str,
    //     value: InMemoryFieldValue<RowId>,
    // ) -> Result<(), InMemoryDataError> {
    //     let field_index =
    //         self.field_index(field_name)
    //             .ok_or_else(|| InMemoryDataError::UnknownField {
    //                 field_name: field_name.to_owned(),
    //             })?;

    //     let schema_field = self
    //         .schema
    //         .columns
    //         .get(field_name)
    //         .expect("field index map and schema are in sync");
    //     validate_in_memory_field_value(&schema_field.data_type, &value).map_err(|source| {
    //         InMemoryDataError::InvalidFieldValue {
    //             field_name: field_name.to_owned(),
    //             source,
    //         }
    //     })?;

    //     let row = self.row_mut(row_index)?;
    //     row.fields[field_index] = value;
    //     Ok(())
    // }

    /// Iterate `(field_name, field_value)` pairs for one row.
    pub fn iter_row_fields(
        &self,
        row_index: usize,
    ) -> Result<impl Iterator<Item = (&str, &InMemoryFieldValue<RowId>)>, InMemoryDataError> {
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
        RowId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
        E: DataSnapshotEncoder<RowId>,
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
        RowId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
        D: DataSnapshotDecoder<RowId>,
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
        fields: Vec<InMemoryFieldValue<RowId>>,
    ) -> Result<InMemoryRow<RowId>, InMemoryDataError> {
        let row = InMemoryRow::new(fields);
        self.validate_row_value(&row)?;
        Ok(row)
    }

    fn validate_row_value(&self, row: &InMemoryRow<RowId>) -> Result<(), InMemoryDataError> {
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
    ) -> Result<InMemoryRow<RowId>, InMemoryDataError>
    where
        I: IntoIterator<Item = (Name, InMemoryFieldValue<RowId>)>,
        Name: AsRef<str>,
    {
        let mut row_slots: Vec<Option<InMemoryFieldValue<RowId>>> = std::iter::repeat_with(|| None)
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

    fn row(&self, row_index: usize) -> Result<&InMemoryRow<RowId>, InMemoryDataError> {
        self.rows
            .get(row_index)
            .ok_or(InMemoryDataError::UnknownRow { row_index })
    }

    fn row_mut(&mut self, row_index: usize) -> Result<&mut InMemoryRow<RowId>, InMemoryDataError> {
        self.rows
            .get_mut(row_index)
            .ok_or(InMemoryDataError::UnknownRow { row_index })
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
struct InMemoryRow<RowId> {
    fields: Vec<InMemoryFieldValue<RowId>>,
}
impl<RowId> InMemoryRow<RowId> {
    fn new(fields: Vec<InMemoryFieldValue<RowId>>) -> Self {
        Self { fields }
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
        RowId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
        V: SchemaSnapshotEncoder<RowId>,
    {
        let mut writer = prepare_schema_snapshot_encoder(encoder, schema)?;
        self.encode_snapshot_fields(field_names, &mut writer)?;
        writer.end()
    }

    fn encode_snapshot_fields<V>(
        &self,
        field_names: &[String],
        writer: &mut SchemaSnapshotEncodingWriter<'_, RowId, V>,
    ) -> Result<(), SchemaVisitError<V::Error>>
    where
        RowId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
        V: SchemaSnapshotEncoder<RowId>,
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
        RowId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
        D: SchemaSnapshotDecoder<RowId>,
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
        Ok(Self { fields })
    }
}

/// In-memory CRDT state for one schema field.
#[derive(Clone, Debug, PartialEq)]
pub enum InMemoryFieldValue<RowId> {
    LatestValueWins(LinearLatestValueWinsValue<RowId>),
    LinearString(LinearString<RowId>),
    LinearList(LinearListValue<RowId>),
    MonotonicCounter(CounterValue),
    TotalOrderRegister(PrimitiveValue),
    TotalOrderFiniteStateRegister(NullablePrimitiveValue),
}

/// Specialized `LinearLatestValueWins` state variants by concrete value type.
#[derive(Clone, Debug, PartialEq)]
pub enum LinearLatestValueWinsValue<RowId> {
    String(LinearLatestValueWins<RowId, String>),
    UInt(LinearLatestValueWins<RowId, u64>),
    Int(LinearLatestValueWins<RowId, i64>),
    Byte(LinearLatestValueWins<RowId, u8>),
    Float(LinearLatestValueWins<RowId, OrderedFloat<f64>>),
    Boolean(LinearLatestValueWins<RowId, bool>),
    Binary(LinearLatestValueWins<RowId, Vec<u8>>),
    Date(LinearLatestValueWins<RowId, NaiveDate>),
    Timestamp(LinearLatestValueWins<RowId, UnixTimestamp>),
    StringArray(LinearLatestValueWins<RowId, Vec<String>>),
    UIntArray(LinearLatestValueWins<RowId, Vec<u64>>),
    IntArray(LinearLatestValueWins<RowId, Vec<i64>>),
    ByteArray(LinearLatestValueWins<RowId, Vec<u8>>),
    FloatArray(LinearLatestValueWins<RowId, Vec<OrderedFloat<f64>>>),
    BooleanArray(LinearLatestValueWins<RowId, Vec<bool>>),
    BinaryArray(LinearLatestValueWins<RowId, Vec<Vec<u8>>>),
    DateArray(LinearLatestValueWins<RowId, Vec<NaiveDate>>),
    TimestampArray(LinearLatestValueWins<RowId, Vec<UnixTimestamp>>),
    NullableString(LinearLatestValueWins<RowId, Option<String>>),
    NullableUInt(LinearLatestValueWins<RowId, Option<u64>>),
    NullableInt(LinearLatestValueWins<RowId, Option<i64>>),
    NullableByte(LinearLatestValueWins<RowId, Option<u8>>),
    NullableFloat(LinearLatestValueWins<RowId, Option<OrderedFloat<f64>>>),
    NullableBoolean(LinearLatestValueWins<RowId, Option<bool>>),
    NullableBinary(LinearLatestValueWins<RowId, Option<Vec<u8>>>),
    NullableDate(LinearLatestValueWins<RowId, Option<NaiveDate>>),
    NullableTimestamp(LinearLatestValueWins<RowId, Option<UnixTimestamp>>),
    NullableStringArray(LinearLatestValueWins<RowId, Option<Vec<String>>>),
    NullableUIntArray(LinearLatestValueWins<RowId, Option<Vec<u64>>>),
    NullableIntArray(LinearLatestValueWins<RowId, Option<Vec<i64>>>),
    NullableByteArray(LinearLatestValueWins<RowId, Option<Vec<u8>>>),
    NullableFloatArray(LinearLatestValueWins<RowId, Option<Vec<OrderedFloat<f64>>>>),
    NullableBooleanArray(LinearLatestValueWins<RowId, Option<Vec<bool>>>),
    NullableBinaryArray(LinearLatestValueWins<RowId, Option<Vec<Vec<u8>>>>),
    NullableDateArray(LinearLatestValueWins<RowId, Option<Vec<NaiveDate>>>),
    NullableTimestampArray(LinearLatestValueWins<RowId, Option<Vec<UnixTimestamp>>>),
}
impl<RowId> LinearLatestValueWinsValue<RowId> {
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
        RowId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
        S: for<'value> SnapshotSink<RowId, NullableBasicValueRef<'value>, Error = E>,
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
pub enum LinearListValue<RowId> {
    String(LinearList<RowId, String>),
    UInt(LinearList<RowId, u64>),
    Int(LinearList<RowId, i64>),
    Byte(LinearList<RowId, u8>),
    Float(LinearList<RowId, OrderedFloat<f64>>),
    Boolean(LinearList<RowId, bool>),
    Binary(LinearList<RowId, Vec<u8>>),
    Date(LinearList<RowId, NaiveDate>),
    Timestamp(LinearList<RowId, UnixTimestamp>),
}
impl<RowId> LinearListValue<RowId> {
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
        RowId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
        S: for<'value> SnapshotSink<IdWithIndex<RowId>, PrimitiveValueArrayRef<'value>, Error = E>,
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

impl<RowId> InMemoryFieldValue<RowId> {
    fn encode_snapshot_field<V>(
        &self,
        field_name: &str,
        writer: &mut SchemaSnapshotEncodingWriter<'_, RowId, V>,
    ) -> Result<(), SchemaVisitError<V::Error>>
    where
        RowId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
        V: SchemaSnapshotEncoder<RowId>,
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

    fn decode_snapshot_field<D>(
        field_name: &str,
        schema_field: &super::super::Field,
        decoder: &mut D,
    ) -> Result<Self, InMemoryDataSnapshotDecodeError<D::Error>>
    where
        RowId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
        D: SchemaSnapshotDecoder<RowId>,
    {
        match &schema_field.data_type {
            ReplicatedDataType::LatestValueWins { value_type } => {
                let mut source = decoder
                    .prepare_latest_value_wins_field(field_name, value_type)
                    .context(DecoderSnafu)?;
                let nodes =
                    SnapshotNodeSourceIter::<_, RowId, NullableBasicValue>::new(&mut source)
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
                    SnapshotNodeSourceIter::<_, IdWithIndex<RowId>, String>::new(&mut source)
                        .map(|result| result.context(SourceSnafu));
                let value = LinearString::from_snapshot_nodes(nodes).context(SnapshotReadSnafu)?;
                Ok(Self::LinearString(value))
            }
            ReplicatedDataType::LinearList { value_type } => {
                let mut source = decoder
                    .prepare_linear_list_field(field_name, *value_type)
                    .context(DecoderSnafu)?;
                let nodes =
                    SnapshotNodeSourceIter::<_, IdWithIndex<RowId>, PrimitiveValueArray>::new(
                        &mut source,
                    )
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
    for<'value> Sink: SnapshotSink<Id, NullableBasicValueRef<'value>>,
{
    sink: &'a mut Sink,
    map_value: Mapper,
    _marker: PhantomData<fn(Id, Value)>,
}
impl<'a, Id, Value, Sink, Mapper> LatestValueWinsSnapshotSinkAdapter<'a, Id, Value, Sink, Mapper>
where
    Mapper: for<'value> Fn(&'value Value) -> NullableBasicValueRef<'value>,
    for<'value> Sink: SnapshotSink<Id, NullableBasicValueRef<'value>>,
{
    fn new(sink: &'a mut Sink, map_value: Mapper) -> Self {
        Self {
            sink,
            map_value,
            _marker: PhantomData,
        }
    }
}
impl<Id, Value, Sink, Mapper, E> SnapshotSink<Id, Value>
    for LatestValueWinsSnapshotSinkAdapter<'_, Id, Value, Sink, Mapper>
where
    Mapper: for<'value> Fn(&'value Value) -> NullableBasicValueRef<'value>,
    for<'value> Sink: SnapshotSink<Id, NullableBasicValueRef<'value>, Error = E>,
{
    type Error = E;

    fn begin(&mut self, header: SnapshotHeader) -> Result<(), Self::Error> {
        self.sink.begin(header)
    }

    fn node(
        &mut self,
        index: usize,
        node: SnapshotNodeRef<'_, Id, Value>,
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

fn decode_state_snapshot_field<RowId>(
    data_type: &ReplicatedDataType,
    value: SnapshotStateValue,
) -> Result<InMemoryFieldValue<RowId>, DataModelValueError> {
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

fn decode_latest_value_wins_snapshot<RowId, E, I>(
    value_type: &NullableBasicDataType,
    nodes: I,
) -> Result<LinearLatestValueWinsValue<RowId>, SnapshotReadError<InMemoryNodeDecodeError<E>>>
where
    E: snafu::Error + Send + Sync + 'static,
    RowId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
    I: IntoIterator<
        Item = Result<SnapshotNode<RowId, NullableBasicValue>, InMemoryNodeDecodeError<E>>,
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

fn decode_linear_list_snapshot<RowId, E, I>(
    value_type: PrimitiveType,
    nodes: I,
) -> Result<LinearListValue<RowId>, SnapshotReadError<InMemoryNodeDecodeError<E>>>
where
    E: snafu::Error + Send + Sync + 'static,
    RowId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
    I: IntoIterator<
        Item = Result<
            SnapshotNode<IdWithIndex<RowId>, PrimitiveValueArray>,
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

fn validate_in_memory_field_value<RowId>(
    data_type: &ReplicatedDataType,
    value: &InMemoryFieldValue<RowId>,
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
