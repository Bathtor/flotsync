use super::*;
use crate::{
    any_data::{LinearLatestValueWins, list::LinearList},
    text::LinearString,
};
use chrono::NaiveDate;
use ordered_float::OrderedFloat;
use snafu::prelude::*;
use std::{borrow::Cow, collections::HashMap};

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
            validate_in_memory_field_value(&schema_field.data_type, value).map_err(|source| {
                InMemoryDataError::InvalidFieldValue {
                    field_name: field_name.to_owned(),
                    source,
                }
            })?;
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
            validate_in_memory_field_value(&schema_field.data_type, &value).map_err(|source| {
                InMemoryDataError::InvalidFieldValue {
                    field_name: field_name.to_owned(),
                    source,
                }
            })?;

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
        #[snafu(source(false))]
        source: DataModelValueError,
    },
    #[snafu(display("Row has wrong number of fields: expected {expected}, got {actual}."))]
    FieldCountMismatch { expected: usize, actual: usize },
    #[snafu(display("Unknown row index {row_index}."))]
    UnknownRow { row_index: usize },
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
