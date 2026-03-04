use super::{NullablePrimitiveType, PrimitiveType};
use chrono::NaiveDate;
use ordered_float::OrderedFloat;
use snafu::prelude::*;
use std::fmt;

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

macro_rules! impl_primitive_value_array_from_vec {
    ($ty:ty, $variant:ident, $map:expr) => {
        impl From<Vec<$ty>> for PrimitiveValueArray {
            fn from(values: Vec<$ty>) -> Self {
                Self::$variant(($map)(values))
            }
        }

        impl From<Box<[$ty]>> for PrimitiveValueArray {
            fn from(values: Box<[$ty]>) -> Self {
                Self::$variant(($map)(Vec::from(values)))
            }
        }
    };
}

impl_primitive_value_array_from_vec!(String, String, |values: Vec<String>| values);
impl_primitive_value_array_from_vec!(u64, UInt, |values: Vec<u64>| values);
impl_primitive_value_array_from_vec!(i64, Int, |values: Vec<i64>| values);
impl_primitive_value_array_from_vec!(u8, Byte, |values: Vec<u8>| values);
impl_primitive_value_array_from_vec!(OrderedFloat<f64>, Float, |values: Vec<
    OrderedFloat<f64>,
>| values);
impl_primitive_value_array_from_vec!(f64, Float, |values: Vec<f64>| values
    .into_iter()
    .map(OrderedFloat)
    .collect());
impl_primitive_value_array_from_vec!(f32, Float, |values: Vec<f32>| values
    .into_iter()
    .map(|value| OrderedFloat(value as f64))
    .collect());
impl_primitive_value_array_from_vec!(bool, Boolean, |values: Vec<bool>| values);
impl_primitive_value_array_from_vec!(Vec<u8>, Binary, |values: Vec<Vec<u8>>| values);
impl_primitive_value_array_from_vec!(NaiveDate, Date, |values: Vec<NaiveDate>| values);

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

macro_rules! impl_primitive_value_from_signed {
    ($($ty:ty),* $(,)?) => {
        $(
            impl From<$ty> for PrimitiveValue {
                fn from(value: $ty) -> Self {
                    Self::Int(value as i64)
                }
            }
        )*
    };
}

macro_rules! impl_primitive_value_from_unsigned {
    ($($ty:ty),* $(,)?) => {
        $(
            impl From<$ty> for PrimitiveValue {
                fn from(value: $ty) -> Self {
                    Self::UInt(value as u64)
                }
            }
        )*
    };
}

impl From<String> for PrimitiveValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for PrimitiveValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<char> for PrimitiveValue {
    fn from(value: char) -> Self {
        Self::String(value.to_string())
    }
}

impl From<u8> for PrimitiveValue {
    fn from(value: u8) -> Self {
        Self::Byte(value)
    }
}

impl_primitive_value_from_unsigned!(u16, u32, u64, usize);
impl_primitive_value_from_signed!(i8, i16, i32, i64, isize);

impl From<f64> for PrimitiveValue {
    fn from(value: f64) -> Self {
        Self::Float(OrderedFloat(value))
    }
}

impl From<f32> for PrimitiveValue {
    fn from(value: f32) -> Self {
        Self::Float(OrderedFloat(value as f64))
    }
}

impl From<bool> for PrimitiveValue {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}

impl From<NaiveDate> for PrimitiveValue {
    fn from(value: NaiveDate) -> Self {
        Self::Date(value)
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

/// One explicitly ordered finite-state register value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OrderedValue {
    Null,
    Value(PrimitiveValue),
}

/// Convenience alias for placing `NULL` explicitly in an ordered finite-state state list.
pub const NULL: OrderedValue = OrderedValue::Null;

impl From<PrimitiveValue> for OrderedValue {
    fn from(value: PrimitiveValue) -> Self {
        Self::Value(value)
    }
}

impl From<NullablePrimitiveValue> for OrderedValue {
    fn from(value: NullablePrimitiveValue) -> Self {
        match value {
            NullablePrimitiveValue::Null => Self::Null,
            NullablePrimitiveValue::Value(value) => Self::Value(value),
        }
    }
}

impl From<String> for OrderedValue {
    fn from(value: String) -> Self {
        Self::Value(PrimitiveValue::String(value))
    }
}

impl From<&str> for OrderedValue {
    fn from(value: &str) -> Self {
        Self::Value(PrimitiveValue::String(value.to_owned()))
    }
}

impl From<char> for OrderedValue {
    fn from(value: char) -> Self {
        Self::Value(PrimitiveValue::String(value.to_string()))
    }
}

impl From<u8> for OrderedValue {
    fn from(value: u8) -> Self {
        Self::Value(PrimitiveValue::Byte(value))
    }
}

macro_rules! impl_ordered_value_from_signed {
    ($($ty:ty),* $(,)?) => {
        $(
            impl From<$ty> for OrderedValue {
                fn from(value: $ty) -> Self {
                    Self::Value(PrimitiveValue::Int(value as i64))
                }
            }
        )*
    };
}

macro_rules! impl_ordered_value_from_unsigned {
    ($($ty:ty),* $(,)?) => {
        $(
            impl From<$ty> for OrderedValue {
                fn from(value: $ty) -> Self {
                    Self::Value(PrimitiveValue::UInt(value as u64))
                }
            }
        )*
    };
}

impl_ordered_value_from_unsigned!(u16, u32, u64, usize);
impl_ordered_value_from_signed!(i8, i16, i32, i64, isize);

impl From<f64> for OrderedValue {
    fn from(value: f64) -> Self {
        Self::Value(PrimitiveValue::Float(OrderedFloat(value)))
    }
}

impl From<f32> for OrderedValue {
    fn from(value: f32) -> Self {
        Self::Value(PrimitiveValue::Float(OrderedFloat(value as f64)))
    }
}

impl From<bool> for OrderedValue {
    fn from(value: bool) -> Self {
        Self::Value(PrimitiveValue::Boolean(value))
    }
}

impl From<Vec<u8>> for OrderedValue {
    fn from(value: Vec<u8>) -> Self {
        Self::Value(PrimitiveValue::Binary(value))
    }
}

impl From<Box<[u8]>> for OrderedValue {
    fn from(value: Box<[u8]>) -> Self {
        Self::Value(PrimitiveValue::Binary(Vec::from(value)))
    }
}

impl From<NaiveDate> for OrderedValue {
    fn from(value: NaiveDate) -> Self {
        Self::Value(PrimitiveValue::Date(value))
    }
}

#[derive(Debug, Snafu)]
pub enum OrderedValueError {
    #[snafu(display("Ordered finite-state registers must declare at least one state."))]
    EmptyStateList,
    #[snafu(display(
        "Ordered finite-state registers must contain at least one non-NULL state to infer the primitive type."
    ))]
    MissingPrimitiveType,
    #[snafu(display(
        "Ordered finite-state registers may only contain one NULL entry, but found another at position {duplicate_index} after the first at {first_index}."
    ))]
    DuplicateNull {
        first_index: usize,
        duplicate_index: usize,
    },
    #[snafu(display(
        "Ordered finite-state register states must all have the same primitive type; expected {expected:?}, got {actual:?} at position {index}."
    ))]
    MixedPrimitiveTypes {
        expected: PrimitiveType,
        actual: PrimitiveType,
        index: usize,
    },
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
    pub fn ordered<I, V>(states: I) -> Result<Self, OrderedValueError>
    where
        I: IntoIterator<Item = V>,
        V: Into<OrderedValue>,
    {
        let mut primitive_type = None;
        let mut null_index = None;
        let mut saw_any = false;
        let mut values = Vec::new();

        for (index, state) in states.into_iter().enumerate() {
            saw_any = true;
            match state.into() {
                OrderedValue::Null => {
                    if let Some(first_index) = null_index {
                        return DuplicateNullSnafu {
                            first_index,
                            duplicate_index: index,
                        }
                        .fail();
                    }
                    null_index = Some(index);
                }
                OrderedValue::Value(value) => {
                    let value_type = value.primitive_type();
                    if let Some(expected) = primitive_type {
                        ensure!(
                            expected == value_type,
                            MixedPrimitiveTypesSnafu {
                                expected,
                                actual: value_type,
                                index,
                            }
                        );
                    } else {
                        primitive_type = Some(value_type);
                    }
                    values.push(value);
                }
            }
        }

        ensure!(saw_any, EmptyStateListSnafu);
        let primitive_type = primitive_type.context(MissingPrimitiveTypeSnafu)?;
        let values = primitive_values_into_array(values, primitive_type);
        if let Some(null_index) = null_index {
            Ok(Self::Nullable { values, null_index })
        } else {
            Ok(Self::NonNull(values))
        }
    }

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

impl fmt::Display for OrderedValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => f.write_str("NULL"),
            Self::Value(value) => fmt_primitive_value(value, f),
        }
    }
}

impl fmt::Display for NullablePrimitiveValueArray {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[")?;
        let mut first = true;
        let value_count = self.value_count();
        for index in 0..=value_count {
            if self.null_index() == Some(index) {
                if !first {
                    f.write_str(", ")?;
                }
                f.write_str("NULL")?;
                first = false;
            }
            if index == value_count {
                continue;
            }
            if !first {
                f.write_str(", ")?;
            }
            fmt_primitive_value_from_array(self, index, f)?;
            first = false;
        }
        f.write_str("]")
    }
}

fn primitive_values_into_array(
    values: Vec<PrimitiveValue>,
    primitive_type: PrimitiveType,
) -> PrimitiveValueArray {
    match primitive_type {
        PrimitiveType::String => PrimitiveValueArray::String(
            values
                .into_iter()
                .map(|value| match value {
                    PrimitiveValue::String(value) => value,
                    _ => unreachable!("primitive type already validated"),
                })
                .collect(),
        ),
        PrimitiveType::UInt => PrimitiveValueArray::UInt(
            values
                .into_iter()
                .map(|value| match value {
                    PrimitiveValue::UInt(value) => value,
                    _ => unreachable!("primitive type already validated"),
                })
                .collect(),
        ),
        PrimitiveType::Int => PrimitiveValueArray::Int(
            values
                .into_iter()
                .map(|value| match value {
                    PrimitiveValue::Int(value) => value,
                    _ => unreachable!("primitive type already validated"),
                })
                .collect(),
        ),
        PrimitiveType::Byte => PrimitiveValueArray::Byte(
            values
                .into_iter()
                .map(|value| match value {
                    PrimitiveValue::Byte(value) => value,
                    _ => unreachable!("primitive type already validated"),
                })
                .collect(),
        ),
        PrimitiveType::Float => PrimitiveValueArray::Float(
            values
                .into_iter()
                .map(|value| match value {
                    PrimitiveValue::Float(value) => value,
                    _ => unreachable!("primitive type already validated"),
                })
                .collect(),
        ),
        PrimitiveType::Boolean => PrimitiveValueArray::Boolean(
            values
                .into_iter()
                .map(|value| match value {
                    PrimitiveValue::Boolean(value) => value,
                    _ => unreachable!("primitive type already validated"),
                })
                .collect(),
        ),
        PrimitiveType::Binary => PrimitiveValueArray::Binary(
            values
                .into_iter()
                .map(|value| match value {
                    PrimitiveValue::Binary(value) => value,
                    _ => unreachable!("primitive type already validated"),
                })
                .collect(),
        ),
        PrimitiveType::Date => PrimitiveValueArray::Date(
            values
                .into_iter()
                .map(|value| match value {
                    PrimitiveValue::Date(value) => value,
                    _ => unreachable!("primitive type already validated"),
                })
                .collect(),
        ),
        PrimitiveType::Timestamp => PrimitiveValueArray::Timestamp(
            values
                .into_iter()
                .map(|value| match value {
                    PrimitiveValue::Timestamp(value) => value,
                    _ => unreachable!("primitive type already validated"),
                })
                .collect(),
        ),
    }
}

fn fmt_primitive_value_from_array(
    values: &NullablePrimitiveValueArray,
    index: usize,
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    match values {
        NullablePrimitiveValueArray::NonNull(values)
        | NullablePrimitiveValueArray::Nullable { values, .. } => match values {
            PrimitiveValueArray::String(values) => {
                fmt_primitive_value(&PrimitiveValue::String(values[index].clone()), f)
            }
            PrimitiveValueArray::UInt(values) => {
                fmt_primitive_value(&PrimitiveValue::UInt(values[index]), f)
            }
            PrimitiveValueArray::Int(values) => {
                fmt_primitive_value(&PrimitiveValue::Int(values[index]), f)
            }
            PrimitiveValueArray::Byte(values) => {
                fmt_primitive_value(&PrimitiveValue::Byte(values[index]), f)
            }
            PrimitiveValueArray::Float(values) => {
                fmt_primitive_value(&PrimitiveValue::Float(values[index]), f)
            }
            PrimitiveValueArray::Boolean(values) => {
                fmt_primitive_value(&PrimitiveValue::Boolean(values[index]), f)
            }
            PrimitiveValueArray::Binary(values) => {
                fmt_primitive_value(&PrimitiveValue::Binary(values[index].clone()), f)
            }
            PrimitiveValueArray::Date(values) => {
                fmt_primitive_value(&PrimitiveValue::Date(values[index]), f)
            }
            PrimitiveValueArray::Timestamp(values) => {
                fmt_primitive_value(&PrimitiveValue::Timestamp(values[index]), f)
            }
        },
    }
}

fn fmt_primitive_value(value: &PrimitiveValue, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match value {
        PrimitiveValue::String(value) => write!(f, "{value:?}"),
        PrimitiveValue::UInt(value) => write!(f, "{value}"),
        PrimitiveValue::Int(value) => write!(f, "{value}"),
        PrimitiveValue::Byte(value) => write!(f, "{value}"),
        PrimitiveValue::Float(value) => write!(f, "{value}"),
        PrimitiveValue::Boolean(value) => write!(f, "{value}"),
        PrimitiveValue::Binary(value) => write!(f, "{value:?}"),
        PrimitiveValue::Date(value) => write!(f, "{value}"),
        PrimitiveValue::Timestamp(value) => write!(f, "{value}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::assert_matches;

    #[test]
    fn ordered_states_preserve_null_position_and_display() {
        let states =
            NullablePrimitiveValueArray::ordered(["draft".into(), NULL, "published".into()])
                .unwrap();

        assert_eq!(
            states,
            NullablePrimitiveValueArray::Nullable {
                values: PrimitiveValueArray::String(vec![
                    "draft".to_owned(),
                    "published".to_owned(),
                ]),
                null_index: 1,
            }
        );
        assert_eq!(states.to_string(), "[\"draft\", NULL, \"published\"]");
    }

    #[test]
    fn ordered_states_reject_duplicate_nulls() {
        let err = NullablePrimitiveValueArray::ordered([NULL, NULL]).unwrap_err();
        assert_matches!(
            err,
            OrderedValueError::DuplicateNull {
                first_index: 0,
                duplicate_index: 1,
            }
        );
    }

    #[test]
    fn ordered_states_reject_mixed_primitive_types() {
        let err = NullablePrimitiveValueArray::ordered([
            OrderedValue::from("draft"),
            OrderedValue::from(7u64),
        ])
        .unwrap_err();
        assert_matches!(
            err,
            OrderedValueError::MixedPrimitiveTypes {
                expected: PrimitiveType::String,
                actual: PrimitiveType::UInt,
                index: 1,
            }
        );
    }
}
