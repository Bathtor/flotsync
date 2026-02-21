use super::{NullablePrimitiveType, PrimitiveType};
use chrono::NaiveDate;
use ordered_float::OrderedFloat;

pub type UnixTimestamp = i64;

/// Arrays over primitive values.
///
/// Corresponds to the [[super::PrimitiveType]].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PrimitiveValueArray {
    String(Vec<String>),
    UInt(Vec<u64>),
    Int(Vec<i64>),
    Byte(Vec<u8>),
    Float(Vec<OrderedFloat<f64>>),
    Boolean(Vec<bool>),
    Binary(Vec<Vec<u8>>),
    Date(Vec<chrono::NaiveDate>),
    Timestamp(Vec<UnixTimestamp>),
}
impl PrimitiveValueArray {
    pub fn len(&self) -> usize {
        match self {
            Self::String(values) => values.len(),
            Self::UInt(values) => values.len(),
            Self::Int(values) => values.len(),
            Self::Byte(values) => values.len(),
            Self::Float(values) => values.len(),
            Self::Boolean(values) => values.len(),
            Self::Binary(values) => values.len(),
            Self::Date(values) => values.len(),
            Self::Timestamp(values) => values.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Primitive values.
///
/// Corresponds to the [[super::PrimitiveType]].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PrimitiveValue {
    String(String),
    UInt(u64),
    Int(i64),
    Byte(u8),
    Float(OrderedFloat<f64>),
    Boolean(bool),
    Binary(Vec<u8>),
    Date(chrono::NaiveDate),
    Timestamp(UnixTimestamp),
}
impl PrimitiveValue {
    pub fn as_ref(&self) -> PrimitiveValueRef<'_> {
        match self {
            Self::String(value) => PrimitiveValueRef::String(value.as_str()),
            Self::UInt(value) => PrimitiveValueRef::UInt(*value),
            Self::Int(value) => PrimitiveValueRef::Int(*value),
            Self::Byte(value) => PrimitiveValueRef::Byte(*value),
            Self::Float(value) => PrimitiveValueRef::Float(*value),
            Self::Boolean(value) => PrimitiveValueRef::Boolean(*value),
            Self::Binary(value) => PrimitiveValueRef::Binary(value.as_slice()),
            Self::Date(value) => PrimitiveValueRef::Date(*value),
            Self::Timestamp(value) => PrimitiveValueRef::Timestamp(*value),
        }
    }

    pub fn primitive_type(&self) -> PrimitiveType {
        self.as_ref().primitive_type()
    }

    pub fn value_type(&self) -> PrimitiveType {
        self.primitive_type()
    }
}

/// A borrowed primitive value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PrimitiveValueRef<'a> {
    String(&'a str),
    UInt(u64),
    Int(i64),
    Byte(u8),
    Float(OrderedFloat<f64>),
    Boolean(bool),
    Binary(&'a [u8]),
    Date(NaiveDate),
    Timestamp(UnixTimestamp),
}
impl PrimitiveValueRef<'_> {
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

    pub fn value_type(&self) -> PrimitiveType {
        self.primitive_type()
    }

    pub fn to_owned(&self) -> PrimitiveValue {
        match self {
            Self::String(value) => PrimitiveValue::String((*value).to_owned()),
            Self::UInt(value) => PrimitiveValue::UInt(*value),
            Self::Int(value) => PrimitiveValue::Int(*value),
            Self::Byte(value) => PrimitiveValue::Byte(*value),
            Self::Float(value) => PrimitiveValue::Float(*value),
            Self::Boolean(value) => PrimitiveValue::Boolean(*value),
            Self::Binary(value) => PrimitiveValue::Binary((*value).to_vec()),
            Self::Date(value) => PrimitiveValue::Date(*value),
            Self::Timestamp(value) => PrimitiveValue::Timestamp(*value),
        }
    }
}

/// Nullable wrapper for one primitive value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NullablePrimitiveValue {
    Null,
    Value(PrimitiveValue),
}
impl NullablePrimitiveValue {
    pub fn as_ref(&self) -> NullablePrimitiveValueRef<'_> {
        match self {
            Self::Null => NullablePrimitiveValueRef::Null,
            Self::Value(value) => NullablePrimitiveValueRef::Value(value.as_ref()),
        }
    }

    pub fn value_type(&self, value_type: NullablePrimitiveType) -> NullablePrimitiveType {
        match self {
            Self::Null => value_type,
            Self::Value(value) => match value_type {
                NullablePrimitiveType::Nullable(_) => {
                    NullablePrimitiveType::Nullable(value.primitive_type())
                }
                NullablePrimitiveType::NonNull(_) => {
                    NullablePrimitiveType::NonNull(value.primitive_type())
                }
            },
        }
    }
}

/// Borrowed nullable primitive value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NullablePrimitiveValueRef<'a> {
    Null,
    Value(PrimitiveValueRef<'a>),
}
impl NullablePrimitiveValueRef<'_> {
    pub fn to_owned(&self) -> NullablePrimitiveValue {
        match self {
            Self::Null => NullablePrimitiveValue::Null,
            Self::Value(value) => NullablePrimitiveValue::Value(value.to_owned()),
        }
    }
}

/// Nullable wrapper for an ordered primitive state space.
///
/// `null_index` defines where `NULL` appears in the total order among `values`.
/// It may be `values.len()` to place `NULL` last.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NullablePrimitiveValueArray {
    NonNull(PrimitiveValueArray),
    Nullable {
        values: PrimitiveValueArray,
        null_index: usize,
    },
}
impl NullablePrimitiveValueArray {
    pub fn value_type(&self) -> NullablePrimitiveType {
        match self {
            Self::NonNull(values) => NullablePrimitiveType::NonNull(values.primitive_type()),
            Self::Nullable { values, .. } => {
                NullablePrimitiveType::Nullable(values.primitive_type())
            }
        }
    }

    pub fn primitive_type(&self) -> PrimitiveType {
        match self {
            Self::NonNull(values) => values.primitive_type(),
            Self::Nullable { values, .. } => values.primitive_type(),
        }
    }

    pub fn is_nullable(&self) -> bool {
        matches!(self, Self::Nullable { .. })
    }

    pub fn value_count(&self) -> usize {
        match self {
            Self::NonNull(values) => values.len(),
            Self::Nullable { values, .. } => values.len(),
        }
    }

    pub fn state_count(&self) -> usize {
        self.value_count() + usize::from(self.is_nullable())
    }

    pub fn null_index(&self) -> Option<usize> {
        match self {
            Self::NonNull(_) => None,
            Self::Nullable { null_index, .. } => Some(*null_index),
        }
    }

    pub fn is_valid(&self) -> bool {
        match self {
            Self::NonNull(_) => true,
            Self::Nullable { values, null_index } => *null_index <= values.len(),
        }
    }
}
