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

mod in_memory;
mod operations;
mod snapshots;

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
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SchemaValueError {
    UnknownField {
        field_name: String,
    },
    DuplicateField {
        field_name: String,
    },
    MissingField {
        field_name: String,
    },
    InvalidSnapshotFieldValue {
        field_name: String,
        source: DataModelValueError,
    },
    InvalidOperationFieldValue {
        field_name: String,
        source: DataModelValueError,
    },
}

#[derive(Debug)]
pub enum SchemaVisitError<E> {
    InvalidSchemaValue(SchemaValueError),
    Visitor(E),
}
/// Structural/schema validation errors for snapshot/operation payloads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DataModelValueError {
    InvalidSnapshotValueForType,
    InvalidOperationValueForType,
    InvalidFiniteStateSchema,
    BasicTypeMismatch,
    PrimitiveTypeMismatch {
        expected: PrimitiveType,
        actual: PrimitiveType,
    },
    CounterTypeMismatch {
        expected_small_range: bool,
        actual_small_range: bool,
    },
    NullabilityMismatch {
        expected_nullable: bool,
        actual_nullable: bool,
    },
    FiniteStateValueNotInSchema,
}
#[derive(Debug)]
pub enum VisitError<E> {
    InvalidValue(DataModelValueError),
    Visitor(E),
}

#[derive(Debug)]
pub enum DecodeError<E> {
    InvalidValue(DataModelValueError),
    Decoder(E),
}
fn ensure_basic_type(
    expected: &BasicDataType,
    value: &BasicValueRef<'_>,
) -> Result<(), DataModelValueError> {
    if value.matches_type(expected) {
        Ok(())
    } else {
        Err(DataModelValueError::BasicTypeMismatch)
    }
}

fn ensure_nullable_basic_type(
    expected: &NullableBasicDataType,
    value: &NullableBasicValueRef<'_>,
) -> Result<(), DataModelValueError> {
    match value {
        NullableBasicValueRef::Null => {
            if expected.is_nullable() {
                Ok(())
            } else {
                Err(DataModelValueError::NullabilityMismatch {
                    expected_nullable: false,
                    actual_nullable: true,
                })
            }
        }
        NullableBasicValueRef::Value(value) => ensure_basic_type(expected.value_type(), value),
    }
}

pub(super) fn ensure_primitive_type(
    expected: PrimitiveType,
    actual: PrimitiveType,
) -> Result<(), DataModelValueError> {
    if expected == actual {
        Ok(())
    } else {
        Err(DataModelValueError::PrimitiveTypeMismatch { expected, actual })
    }
}

fn ensure_primitive_array_type(
    expected: PrimitiveType,
    actual: PrimitiveType,
) -> Result<(), DataModelValueError> {
    ensure_primitive_type(expected, actual)
}

pub(super) fn ensure_counter_type(
    expected_small_range: bool,
    actual: CounterValueRef,
) -> Result<(), DataModelValueError> {
    if actual.matches_type(expected_small_range) {
        Ok(())
    } else {
        Err(DataModelValueError::CounterTypeMismatch {
            expected_small_range,
            actual_small_range: matches!(actual, CounterValueRef::Byte(_)),
        })
    }
}

pub(super) fn ensure_nullable_primitive_type(
    expected: NullablePrimitiveType,
    actual: NullablePrimitiveValueRef<'_>,
) -> Result<(), DataModelValueError> {
    match actual {
        NullablePrimitiveValueRef::Null => {
            if expected.is_nullable() {
                Ok(())
            } else {
                Err(DataModelValueError::NullabilityMismatch {
                    expected_nullable: false,
                    actual_nullable: true,
                })
            }
        }
        NullablePrimitiveValueRef::Value(value) => {
            ensure_primitive_type(expected.value_type(), value.value_type())
        }
    }
}

pub(super) fn ensure_finite_state_value(
    value_type: NullablePrimitiveType,
    states: &super::values::NullablePrimitiveValueArray,
    value: &NullablePrimitiveValueRef<'_>,
) -> Result<(), DataModelValueError> {
    ensure_nullable_primitive_type(value_type, value.clone())?;
    ensure_primitive_type(value_type.value_type(), states.primitive_type())
        .map_err(|_| DataModelValueError::InvalidFiniteStateSchema)?;
    if value_type.is_nullable() != states.is_nullable() || !states.is_valid() {
        return Err(DataModelValueError::InvalidFiniteStateSchema);
    }

    if finite_state_contains(states, value) {
        Ok(())
    } else {
        Err(DataModelValueError::FiniteStateValueNotInSchema)
    }
}

fn finite_state_contains(
    states: &super::values::NullablePrimitiveValueArray,
    value: &NullablePrimitiveValueRef<'_>,
) -> bool {
    match (states, value) {
        (
            super::values::NullablePrimitiveValueArray::NonNull(states),
            NullablePrimitiveValueRef::Value(value),
        ) => finite_state_contains_non_null(states, value),
        (
            super::values::NullablePrimitiveValueArray::Nullable { values, .. },
            NullablePrimitiveValueRef::Value(value),
        ) => finite_state_contains_non_null(values, value),
        (
            super::values::NullablePrimitiveValueArray::Nullable { .. },
            NullablePrimitiveValueRef::Null,
        ) => true,
        (
            super::values::NullablePrimitiveValueArray::NonNull(_),
            NullablePrimitiveValueRef::Null,
        ) => false,
    }
}

fn finite_state_contains_non_null(
    states: &PrimitiveValueArray,
    value: &PrimitiveValueRef<'_>,
) -> bool {
    match (states, value) {
        (PrimitiveValueArray::String(values), PrimitiveValueRef::String(value)) => {
            values.iter().any(|candidate| candidate == value)
        }
        (PrimitiveValueArray::UInt(values), PrimitiveValueRef::UInt(value)) => {
            values.contains(value)
        }
        (PrimitiveValueArray::Int(values), PrimitiveValueRef::Int(value)) => values.contains(value),
        (PrimitiveValueArray::Byte(values), PrimitiveValueRef::Byte(value)) => {
            values.contains(value)
        }
        (PrimitiveValueArray::Float(values), PrimitiveValueRef::Float(value)) => {
            values.contains(value)
        }
        (PrimitiveValueArray::Boolean(values), PrimitiveValueRef::Boolean(value)) => {
            values.contains(value)
        }
        (PrimitiveValueArray::Binary(values), PrimitiveValueRef::Binary(value)) => values
            .iter()
            .any(|candidate| candidate.as_slice() == *value),
        (PrimitiveValueArray::Date(values), PrimitiveValueRef::Date(value)) => {
            values.contains(value)
        }
        (PrimitiveValueArray::Timestamp(values), PrimitiveValueRef::Timestamp(value)) => {
            values.contains(value)
        }
        _ => false,
    }
}
