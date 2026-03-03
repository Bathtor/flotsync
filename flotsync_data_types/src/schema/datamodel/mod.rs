//! Schema-aware value model for snapshots and operations.
//!
//! This module describes the concrete *value shapes* that a transport/library must
//! serialize and deserialize for each [[super::ReplicatedDataType]].
//!
//! For history-based CRDTs (`LatestValueWins`, `LinearString`, `LinearList`), value encoding
//! is intended to be used together with the visitor/lazy-node snapshot interfaces in
//! [[crate::snapshot]].

use super::{
    BasicDataType,
    NullableBasicDataType,
    NullablePrimitiveType,
    PrimitiveType,
    ReplicatedDataType,
    Schema,
    values::{
        NullablePrimitiveValue,
        NullablePrimitiveValueArray,
        NullablePrimitiveValueRef,
        PrimitiveValue,
        PrimitiveValueArray,
        PrimitiveValueRef,
        UnixTimestamp,
    },
};
use chrono::NaiveDate;
use ordered_float::OrderedFloat;
use snafu::prelude::*;

mod in_memory;
mod operations;
mod snapshots;
pub mod validation;

pub use in_memory::*;
pub use operations::*;
pub use snapshots::*;

/// A borrowed array of primitive values, flattened by primitive type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PrimitiveValueArrayRef<'a> {
    String(&'a [String]),
    UInt(&'a [u64]),
    Int(&'a [i64]),
    Byte(&'a [u8]),
    Float(&'a [OrderedFloat<f64>]),
    Boolean(&'a [bool]),
    Binary(&'a [Vec<u8>]),
    Date(&'a [NaiveDate]),
    Timestamp(&'a [UnixTimestamp]),
}
impl PrimitiveValueArrayRef<'_> {
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

    pub fn into_owned(self) -> PrimitiveValueArray {
        match self {
            Self::String(values) => PrimitiveValueArray::String(values.to_vec()),
            Self::UInt(values) => PrimitiveValueArray::UInt(values.to_vec()),
            Self::Int(values) => PrimitiveValueArray::Int(values.to_vec()),
            Self::Byte(values) => PrimitiveValueArray::Byte(values.to_vec()),
            Self::Float(values) => PrimitiveValueArray::Float(values.to_vec()),
            Self::Boolean(values) => PrimitiveValueArray::Boolean(values.to_vec()),
            Self::Binary(values) => PrimitiveValueArray::Binary(values.to_vec()),
            Self::Date(values) => PrimitiveValueArray::Date(values.to_vec()),
            Self::Timestamp(values) => PrimitiveValueArray::Timestamp(values.to_vec()),
        }
    }
}

impl PrimitiveValueArray {
    pub fn as_ref(&self) -> PrimitiveValueArrayRef<'_> {
        match self {
            Self::String(values) => PrimitiveValueArrayRef::String(values.as_slice()),
            Self::UInt(values) => PrimitiveValueArrayRef::UInt(values.as_slice()),
            Self::Int(values) => PrimitiveValueArrayRef::Int(values.as_slice()),
            Self::Byte(values) => PrimitiveValueArrayRef::Byte(values.as_slice()),
            Self::Float(values) => PrimitiveValueArrayRef::Float(values.as_slice()),
            Self::Boolean(values) => PrimitiveValueArrayRef::Boolean(values.as_slice()),
            Self::Binary(values) => PrimitiveValueArrayRef::Binary(values.as_slice()),
            Self::Date(values) => PrimitiveValueArrayRef::Date(values.as_slice()),
            Self::Timestamp(values) => PrimitiveValueArrayRef::Timestamp(values.as_slice()),
        }
    }

    pub fn primitive_type(&self) -> PrimitiveType {
        self.as_ref().primitive_type()
    }
}

/// An owned basic value for `LatestValueWins` payloads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BasicValue {
    Primitive(PrimitiveValue),
    Array(PrimitiveValueArray),
}
impl BasicValue {
    pub fn as_ref(&self) -> BasicValueRef<'_> {
        match self {
            Self::Primitive(value) => BasicValueRef::Primitive(value.as_ref()),
            Self::Array(values) => BasicValueRef::Array(values.as_ref()),
        }
    }

    pub fn matches_type(&self, expected: &BasicDataType) -> bool {
        self.as_ref().matches_type(expected)
    }
}

/// An owned nullable basic value for `LatestValueWins` payloads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NullableBasicValue {
    Null,
    Value(BasicValue),
}
impl NullableBasicValue {
    pub fn as_ref(&self) -> NullableBasicValueRef<'_> {
        match self {
            Self::Null => NullableBasicValueRef::Null,
            Self::Value(value) => NullableBasicValueRef::Value(value.as_ref()),
        }
    }

    pub fn matches_type(&self, expected: &NullableBasicDataType) -> bool {
        self.as_ref().matches_type(expected)
    }
}

/// A borrowed basic value for `LatestValueWins` payloads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BasicValueRef<'a> {
    Primitive(PrimitiveValueRef<'a>),
    Array(PrimitiveValueArrayRef<'a>),
}
impl BasicValueRef<'_> {
    pub fn into_owned(self) -> BasicValue {
        match self {
            Self::Primitive(value) => BasicValue::Primitive(value.to_owned()),
            Self::Array(values) => BasicValue::Array(values.into_owned()),
        }
    }

    pub fn matches_type(&self, expected: &BasicDataType) -> bool {
        match (self, expected) {
            (Self::Primitive(value), BasicDataType::Primitive(expected_primitive)) => {
                value.value_type() == *expected_primitive
            }
            (Self::Array(values), BasicDataType::Array(array_type)) => {
                values.primitive_type() == array_type.element_type
            }
            _ => false,
        }
    }
}

/// A borrowed nullable basic value for `LatestValueWins` payloads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NullableBasicValueRef<'a> {
    Null,
    Value(BasicValueRef<'a>),
}
impl NullableBasicValueRef<'_> {
    pub fn into_owned(self) -> NullableBasicValue {
        match self {
            Self::Null => NullableBasicValue::Null,
            Self::Value(value) => NullableBasicValue::Value(value.into_owned()),
        }
    }

    pub fn matches_type(&self, expected: &NullableBasicDataType) -> bool {
        match (self, expected) {
            (Self::Null, NullableBasicDataType::Nullable(_)) => true,
            (Self::Null, NullableBasicDataType::NonNull(_)) => false,
            (Self::Value(value), _) => value.matches_type(expected.value_type()),
        }
    }
}

/// A counter value for `MonotonicCounter`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CounterValue {
    Byte(u8),
    UInt(u64),
}
impl CounterValue {
    pub fn as_ref(&self) -> CounterValueRef {
        match self {
            Self::Byte(value) => CounterValueRef::Byte(*value),
            Self::UInt(value) => CounterValueRef::UInt(*value),
        }
    }

    pub fn matches_type(&self, small_range: bool) -> bool {
        self.as_ref().matches_type(small_range)
    }
}

/// A borrowed counter value for `MonotonicCounter`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CounterValueRef {
    Byte(u8),
    UInt(u64),
}
impl CounterValueRef {
    pub fn into_owned(self) -> CounterValue {
        match self {
            Self::Byte(value) => CounterValue::Byte(value),
            Self::UInt(value) => CounterValue::UInt(value),
        }
    }

    pub fn matches_type(self, small_range: bool) -> bool {
        if small_range {
            matches!(self, Self::Byte(_))
        } else {
            matches!(self, Self::UInt(_))
        }
    }
}
/// Schema-level validation errors for collections of field payloads.
#[derive(Clone, Debug, PartialEq, Eq, Snafu)]
pub enum SchemaValueError {
    #[snafu(display("Unknown field '{field_name}'."))]
    UnknownField { field_name: String },
    #[snafu(display("Field '{field_name}' was provided more than once."))]
    DuplicateField { field_name: String },
    #[snafu(display("Missing required field '{field_name}'."))]
    MissingField { field_name: String },
    #[snafu(display(
        "Field '{field_name}' has a snapshot value that is incompatible with the schema data type."
    ))]
    InvalidSnapshotFieldValue {
        field_name: String,
        source: DataModelValueError,
    },
    #[snafu(display(
        "Field '{field_name}' has an operation value that is incompatible with the schema data type."
    ))]
    InvalidOperationFieldValue {
        field_name: String,
        source: DataModelValueError,
    },
}
impl SchemaValueError {
    pub fn unknown_field_err<I>(field_name: I) -> Result<(), Self>
    where
        I: Into<String>,
    {
        UnknownFieldSnafu {
            field_name: field_name.into(),
        }
        .fail()
    }
}

#[derive(Debug, Snafu)]
pub enum SchemaVisitError<E>
where
    E: snafu::Error + Send + Sync + 'static,
{
    #[snafu(display("Schema value validation failed."))]
    InvalidSchemaValue { source: SchemaValueError },
    #[snafu(display("Schema visitor failed."))]
    Visitor { source: E },
}
/// Structural/schema validation errors for snapshot/operation payloads.
#[derive(Clone, Debug, PartialEq, Eq, Snafu)]
pub enum DataModelValueError {
    #[snafu(display("Snapshot value does not match the schema data type."))]
    InvalidSnapshotValueForType,
    #[snafu(display("Operation value does not match the schema data type."))]
    InvalidOperationValueForType,
    #[snafu(display("Linear operation batches must contain at least one action."))]
    EmptyLinearOperationBatch,
    #[snafu(display("Finite-state register schema is invalid."))]
    InvalidFiniteStateSchema,
    #[snafu(display("Basic value type does not match the schema data type."))]
    BasicTypeMismatch,
    #[snafu(display("Primitive value type mismatch: expected {expected:?}, got {actual:?}."))]
    PrimitiveTypeMismatch {
        expected: PrimitiveType,
        actual: PrimitiveType,
    },
    #[snafu(display(
        "Counter value type mismatch: expected small_range={expected_small_range}, got small_range={actual_small_range}."
    ))]
    CounterTypeMismatch {
        expected_small_range: bool,
        actual_small_range: bool,
    },
    #[snafu(display(
        "Nullability mismatch: expected nullable={expected_nullable}, got nullable={actual_nullable}."
    ))]
    NullabilityMismatch {
        expected_nullable: bool,
        actual_nullable: bool,
    },
    #[snafu(display("Finite-state value is not part of the schema-defined state set."))]
    FiniteStateValueNotInSchema,
}
#[derive(Debug, Snafu)]
pub enum VisitError<E>
where
    E: snafu::Error + Send + Sync + 'static,
{
    #[snafu(display("Value does not match the schema data type."))]
    InvalidVisitedValue { source: DataModelValueError },
    #[snafu(display("Value encoder failed."))]
    VisitorSource { source: E },
}

#[derive(Debug, Snafu)]
pub enum DecodeError<E>
where
    E: snafu::Error + Send + Sync + 'static,
{
    #[snafu(display("Decoded value does not match the schema data type."))]
    InvalidDecodedValue { source: DataModelValueError },
    #[snafu(display("Value decoder failed."))]
    DecoderSource { source: E },
}
