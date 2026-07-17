use super::{
    BasicValueRef,
    NullableBasicValueRef,
    PrimitiveValueArrayRef,
    PrimitiveValueRef,
    UnixTimestamp,
};
use crate::{Decode, DecodeValueError};
use chrono::NaiveDate;
use ordered_float::OrderedFloat;
use std::{any::type_name, borrow::Cow, fmt};

/// Integer-like projected primitives that can be range-checked into Rust integers.
#[derive(Clone, Copy)]
enum IntegerValue {
    Byte(u8),
    UInt(u64),
    Int(i64),
    Timestamp(UnixTimestamp),
}

/// Borrowed integer-like projected arrays that can be range-checked into Rust integer vectors.
#[derive(Clone, Copy)]
enum IntegerArrayRef<'a> {
    Byte(&'a [u8]),
    UInt(&'a [u64]),
    Int(&'a [i64]),
    Timestamp(&'a [UnixTimestamp]),
}

fn null_decode_error<T: ?Sized>() -> DecodeValueError {
    DecodeValueError::NullValue {
        requested_type: type_name::<T>(),
    }
}

fn type_mismatch<T: ?Sized>(actual_type: impl fmt::Display) -> DecodeValueError {
    DecodeValueError::TypeMismatch {
        requested_type: type_name::<T>(),
        actual_type: Cow::Owned(actual_type.to_string()),
    }
}

fn conversion_failed<T: ?Sized>(
    source_type: &'static str,
    value: impl fmt::Display,
) -> DecodeValueError {
    DecodeValueError::ConversionFailed {
        requested_type: type_name::<T>(),
        source_type,
        value: value.to_string(),
    }
}

fn invalid_value<T: ?Sized>(explanation: impl Into<Cow<'static, str>>) -> DecodeValueError {
    DecodeValueError::InvalidValue {
        requested_type: type_name::<T>(),
        explanation: explanation.into(),
    }
}

fn basic_type_mismatch<T: ?Sized>(actual: &BasicValueRef<'_>) -> DecodeValueError {
    type_mismatch::<T>(actual.value_type())
}

fn require_not_null<T: ?Sized>(
    value: NullableBasicValueRef<'_>,
) -> Result<BasicValueRef<'_>, DecodeValueError> {
    match value {
        NullableBasicValueRef::Null => Err(null_decode_error::<T>()),
        NullableBasicValueRef::Value(v) => Ok(v),
    }
}

fn require_primitive<T: ?Sized>(
    value: NullableBasicValueRef<'_>,
) -> Result<PrimitiveValueRef<'_>, DecodeValueError> {
    match require_not_null::<T>(value)? {
        BasicValueRef::Primitive(v) => Ok(v),
        actual @ BasicValueRef::Array(_) => Err(basic_type_mismatch::<T>(&actual)),
    }
}

fn require_array<T: ?Sized>(
    value: NullableBasicValueRef<'_>,
) -> Result<PrimitiveValueArrayRef<'_>, DecodeValueError> {
    match require_not_null::<T>(value)? {
        BasicValueRef::Array(v) => Ok(v),
        actual @ BasicValueRef::Primitive(_) => Err(basic_type_mismatch::<T>(&actual)),
    }
}

fn try_convert<T, U>(value: U, source_type: &'static str) -> Result<T, DecodeValueError>
where
    T: TryFrom<U>,
    U: Copy + fmt::Display,
{
    T::try_from(value).map_err(|_| conversion_failed::<T>(source_type, value))
}

fn collect_try_convert<T, U, I>(
    values: I,
    source_type: &'static str,
) -> Result<Vec<T>, DecodeValueError>
where
    T: TryFrom<U>,
    U: Copy + fmt::Display,
    I: IntoIterator<Item = U>,
{
    values
        .into_iter()
        .map(|value| try_convert::<T, U>(value, source_type))
        .collect()
}

fn extract_integer_value<T: ?Sized>(
    value: NullableBasicValueRef<'_>,
) -> Result<IntegerValue, DecodeValueError> {
    match require_primitive::<T>(value)? {
        PrimitiveValueRef::Byte(v) => Ok(IntegerValue::Byte(v)),
        PrimitiveValueRef::UInt(v) => Ok(IntegerValue::UInt(v)),
        PrimitiveValueRef::Int(v) => Ok(IntegerValue::Int(v)),
        PrimitiveValueRef::Timestamp(v) => Ok(IntegerValue::Timestamp(v)),
        actual => Err(basic_type_mismatch::<T>(&BasicValueRef::Primitive(actual))),
    }
}

fn extract_integer_array<T: ?Sized>(
    value: NullableBasicValueRef<'_>,
) -> Result<IntegerArrayRef<'_>, DecodeValueError> {
    match require_array::<T>(value)? {
        PrimitiveValueArrayRef::Byte(values) => Ok(IntegerArrayRef::Byte(values)),
        PrimitiveValueArrayRef::UInt(values) => Ok(IntegerArrayRef::UInt(values)),
        PrimitiveValueArrayRef::Int(values) => Ok(IntegerArrayRef::Int(values)),
        PrimitiveValueArrayRef::Timestamp(values) => Ok(IntegerArrayRef::Timestamp(values)),
        actual => Err(basic_type_mismatch::<T>(&BasicValueRef::Array(actual))),
    }
}

fn convert_integer_value<T>(value: IntegerValue) -> Result<T, DecodeValueError>
where
    T: TryFrom<u8> + TryFrom<u64> + TryFrom<i64>,
{
    match value {
        IntegerValue::Byte(v) => try_convert::<T, _>(v, "u8"),
        IntegerValue::UInt(v) => try_convert::<T, _>(v, "u64"),
        IntegerValue::Int(v) => try_convert::<T, _>(v, "i64"),
        IntegerValue::Timestamp(v) => try_convert::<T, _>(v, "UnixTimestamp"),
    }
}

fn convert_integer_array<T>(values: IntegerArrayRef<'_>) -> Result<Vec<T>, DecodeValueError>
where
    T: TryFrom<u8> + TryFrom<u64> + TryFrom<i64>,
{
    match values {
        IntegerArrayRef::Byte(values) => {
            collect_try_convert::<T, _, _>(values.iter().copied(), "u8")
        }
        IntegerArrayRef::UInt(values) => {
            collect_try_convert::<T, _, _>(values.iter().copied(), "u64")
        }
        IntegerArrayRef::Int(values) => {
            collect_try_convert::<T, _, _>(values.iter().copied(), "i64")
        }
        IntegerArrayRef::Timestamp(values) => {
            collect_try_convert::<T, _, _>(values.iter().copied(), "UnixTimestamp")
        }
    }
}

fn decode_single_char<T: ?Sized>(value: &str) -> Result<char, DecodeValueError> {
    let mut chars = value.chars();
    let Some(ch) = chars.next() else {
        return Err(invalid_value::<T>("expected exactly one character"));
    };
    if chars.next().is_some() {
        return Err(invalid_value::<T>("expected exactly one character"));
    }
    Ok(ch)
}

fn try_decode_char_code<T: ?Sized>(code_point: u32) -> Result<char, DecodeValueError> {
    char::try_from(code_point).map_err(|_| {
        invalid_value::<T>(format!(
            "value {code_point} is not a valid Unicode scalar value"
        ))
    })
}

#[allow(
    clippy::cast_possible_truncation,
    reason = "Decoding f32 from the f64 datamodel float type intentionally narrows precision."
)]
fn narrow_f64_to_f32(value: OrderedFloat<f64>) -> f32 {
    value.into_inner() as f32
}

macro_rules! impl_decode_integer_scalar {
    ($target:ty) => {
        impl Decode for $target {
            fn decode(value: NullableBasicValueRef<'_>) -> Result<Cow<'_, Self>, DecodeValueError> {
                Ok(Cow::Owned(convert_integer_value::<Self>(
                    extract_integer_value::<Self>(value)?,
                )?))
            }
        }
    };
}

macro_rules! impl_decode_integer_vec {
    ($target:ty) => {
        impl Decode for Vec<$target> {
            fn decode(value: NullableBasicValueRef<'_>) -> Result<Cow<'_, Self>, DecodeValueError> {
                Ok(Cow::Owned(convert_integer_array::<$target>(
                    extract_integer_array::<Self>(value)?,
                )?))
            }
        }
    };
}

macro_rules! impl_decode_exact_vec {
    ($target:ty, $variant:ident) => {
        impl Decode for Vec<$target> {
            fn decode(value: NullableBasicValueRef<'_>) -> Result<Cow<'_, Self>, DecodeValueError> {
                match require_array::<Self>(value)? {
                    PrimitiveValueArrayRef::$variant(values) => Ok(Cow::Owned(values.to_vec())),
                    actual => Err(basic_type_mismatch::<Self>(&BasicValueRef::Array(actual))),
                }
            }
        }
    };
}

macro_rules! impl_decode_box_slice_via_vec {
    ($element:ty) => {
        impl Decode for Box<[$element]> {
            fn decode(value: NullableBasicValueRef<'_>) -> Result<Cow<'_, Self>, DecodeValueError> {
                let values = <Vec<$element> as Decode>::decode(value)?;
                Ok(Cow::Owned(values.into_owned().into_boxed_slice()))
            }
        }
    };
}

macro_rules! impl_decode_integer_slice {
    ($element:ty) => {
        impl Decode for [$element] {
            fn decode(value: NullableBasicValueRef<'_>) -> Result<Cow<'_, Self>, DecodeValueError> {
                let values = <Vec<$element> as Decode>::decode(value)?;
                Ok(Cow::Owned(values.into_owned()))
            }
        }
    };
}

macro_rules! impl_decode_exact_slice {
    ($element:ty, $variant:ident) => {
        impl Decode for [$element] {
            fn decode(value: NullableBasicValueRef<'_>) -> Result<Cow<'_, Self>, DecodeValueError> {
                match require_array::<Self>(value)? {
                    PrimitiveValueArrayRef::$variant(values) => Ok(Cow::Borrowed(values)),
                    actual => Err(basic_type_mismatch::<Self>(&BasicValueRef::Array(actual))),
                }
            }
        }
    };
}

impl Decode for str {
    fn decode(value: NullableBasicValueRef<'_>) -> Result<Cow<'_, Self>, DecodeValueError> {
        match require_primitive::<Self>(value)? {
            PrimitiveValueRef::String(v) => Ok(Cow::Borrowed(v)),
            actual => Err(basic_type_mismatch::<Self>(&BasicValueRef::Primitive(
                actual,
            ))),
        }
    }
}

impl Decode for String {
    fn decode(value: NullableBasicValueRef<'_>) -> Result<Cow<'_, Self>, DecodeValueError> {
        let value = <str as Decode>::decode(value)?;
        Ok(Cow::Owned(value.into_owned()))
    }
}

impl Decode for char {
    fn decode(value: NullableBasicValueRef<'_>) -> Result<Cow<'_, Self>, DecodeValueError> {
        match require_primitive::<Self>(value)? {
            PrimitiveValueRef::String(v) => Ok(Cow::Owned(decode_single_char::<Self>(v)?)),
            PrimitiveValueRef::Byte(v) => {
                let code_point = convert_integer_value::<u32>(IntegerValue::Byte(v))?;
                Ok(Cow::Owned(try_decode_char_code::<Self>(code_point)?))
            }
            PrimitiveValueRef::UInt(v) => {
                let code_point = convert_integer_value::<u32>(IntegerValue::UInt(v))?;
                Ok(Cow::Owned(try_decode_char_code::<Self>(code_point)?))
            }
            PrimitiveValueRef::Int(v) => {
                let code_point = convert_integer_value::<u32>(IntegerValue::Int(v))?;
                Ok(Cow::Owned(try_decode_char_code::<Self>(code_point)?))
            }
            PrimitiveValueRef::Timestamp(v) => {
                let code_point = convert_integer_value::<u32>(IntegerValue::Timestamp(v))?;
                Ok(Cow::Owned(try_decode_char_code::<Self>(code_point)?))
            }
            actual => Err(basic_type_mismatch::<Self>(&BasicValueRef::Primitive(
                actual,
            ))),
        }
    }
}

impl Decode for OrderedFloat<f64> {
    fn decode(value: NullableBasicValueRef<'_>) -> Result<Cow<'_, Self>, DecodeValueError> {
        match require_primitive::<Self>(value)? {
            PrimitiveValueRef::Float(v) => Ok(Cow::Owned(v)),
            actual => Err(basic_type_mismatch::<Self>(&BasicValueRef::Primitive(
                actual,
            ))),
        }
    }
}

impl Decode for f64 {
    fn decode(value: NullableBasicValueRef<'_>) -> Result<Cow<'_, Self>, DecodeValueError> {
        let value = <OrderedFloat<f64> as Decode>::decode(value)?;
        Ok(Cow::Owned(value.into_inner()))
    }
}

impl Decode for f32 {
    fn decode(value: NullableBasicValueRef<'_>) -> Result<Cow<'_, Self>, DecodeValueError> {
        let value = <OrderedFloat<f64> as Decode>::decode(value)?;
        Ok(Cow::Owned(narrow_f64_to_f32(*value)))
    }
}

impl Decode for bool {
    fn decode(value: NullableBasicValueRef<'_>) -> Result<Cow<'_, Self>, DecodeValueError> {
        match require_primitive::<Self>(value)? {
            PrimitiveValueRef::Boolean(v) => Ok(Cow::Owned(v)),
            actual => Err(basic_type_mismatch::<Self>(&BasicValueRef::Primitive(
                actual,
            ))),
        }
    }
}

impl Decode for Vec<u8> {
    fn decode(value: NullableBasicValueRef<'_>) -> Result<Cow<'_, Self>, DecodeValueError> {
        match value {
            NullableBasicValueRef::Null => Err(null_decode_error::<Self>()),
            NullableBasicValueRef::Value(BasicValueRef::Primitive(PrimitiveValueRef::Binary(
                v,
            ))) => Ok(Cow::Owned(v.to_vec())),
            NullableBasicValueRef::Value(BasicValueRef::Primitive(actual)) => Err(
                basic_type_mismatch::<Self>(&BasicValueRef::Primitive(actual)),
            ),
            NullableBasicValueRef::Value(BasicValueRef::Array(PrimitiveValueArrayRef::Byte(v))) => {
                Ok(Cow::Owned(v.to_vec()))
            }
            NullableBasicValueRef::Value(BasicValueRef::Array(PrimitiveValueArrayRef::UInt(v))) => {
                Ok(Cow::Owned(collect_try_convert::<u8, _, _>(
                    v.iter().copied(),
                    "u64",
                )?))
            }
            NullableBasicValueRef::Value(BasicValueRef::Array(PrimitiveValueArrayRef::Int(v))) => {
                Ok(Cow::Owned(collect_try_convert::<u8, _, _>(
                    v.iter().copied(),
                    "i64",
                )?))
            }
            NullableBasicValueRef::Value(BasicValueRef::Array(
                PrimitiveValueArrayRef::Timestamp(v),
            )) => Ok(Cow::Owned(collect_try_convert::<u8, _, _>(
                v.iter().copied(),
                "UnixTimestamp",
            )?)),
            NullableBasicValueRef::Value(BasicValueRef::Array(actual)) => {
                Err(basic_type_mismatch::<Self>(&BasicValueRef::Array(actual)))
            }
        }
    }
}

impl Decode for NaiveDate {
    fn decode(value: NullableBasicValueRef<'_>) -> Result<Cow<'_, Self>, DecodeValueError> {
        match require_primitive::<Self>(value)? {
            PrimitiveValueRef::Date(v) => Ok(Cow::Owned(v)),
            actual => Err(basic_type_mismatch::<Self>(&BasicValueRef::Primitive(
                actual,
            ))),
        }
    }
}

impl_decode_integer_scalar!(u8);
impl_decode_integer_scalar!(u16);
impl_decode_integer_scalar!(u32);
impl_decode_integer_scalar!(u64);
impl_decode_integer_scalar!(u128);
impl_decode_integer_scalar!(usize);
impl_decode_integer_scalar!(i8);
impl_decode_integer_scalar!(i16);
impl_decode_integer_scalar!(i32);
impl_decode_integer_scalar!(i64);
impl_decode_integer_scalar!(i128);
impl_decode_integer_scalar!(isize);

impl_decode_exact_vec!(String, String);
impl_decode_integer_vec!(u16);
impl_decode_integer_vec!(u32);
impl_decode_integer_vec!(u64);
impl_decode_integer_vec!(u128);
impl_decode_integer_vec!(usize);
impl_decode_integer_vec!(i8);
impl_decode_integer_vec!(i16);
impl_decode_integer_vec!(i32);
impl_decode_integer_vec!(i64);
impl_decode_integer_vec!(i128);
impl_decode_integer_vec!(isize);
impl_decode_exact_vec!(OrderedFloat<f64>, Float);
impl_decode_exact_vec!(bool, Boolean);
impl_decode_exact_vec!(Vec<u8>, Binary);
impl_decode_exact_vec!(NaiveDate, Date);

impl Decode for Vec<f64> {
    fn decode(value: NullableBasicValueRef<'_>) -> Result<Cow<'_, Self>, DecodeValueError> {
        match require_array::<Self>(value)? {
            PrimitiveValueArrayRef::Float(values) => {
                Ok(Cow::Owned(values.iter().map(|v| v.into_inner()).collect()))
            }
            actual => Err(basic_type_mismatch::<Self>(&BasicValueRef::Array(actual))),
        }
    }
}

impl Decode for Vec<f32> {
    fn decode(value: NullableBasicValueRef<'_>) -> Result<Cow<'_, Self>, DecodeValueError> {
        match require_array::<Self>(value)? {
            PrimitiveValueArrayRef::Float(values) => Ok(Cow::Owned(
                values.iter().map(|v| narrow_f64_to_f32(*v)).collect(),
            )),
            actual => Err(basic_type_mismatch::<Self>(&BasicValueRef::Array(actual))),
        }
    }
}

impl Decode for Vec<char> {
    fn decode(value: NullableBasicValueRef<'_>) -> Result<Cow<'_, Self>, DecodeValueError> {
        match value {
            NullableBasicValueRef::Null => Err(null_decode_error::<Self>()),
            NullableBasicValueRef::Value(BasicValueRef::Primitive(PrimitiveValueRef::String(
                v,
            ))) => Ok(Cow::Owned(v.chars().collect())),
            NullableBasicValueRef::Value(BasicValueRef::Primitive(actual)) => Err(
                basic_type_mismatch::<Self>(&BasicValueRef::Primitive(actual)),
            ),
            NullableBasicValueRef::Value(BasicValueRef::Array(PrimitiveValueArrayRef::String(
                values,
            ))) => values
                .iter()
                .map(|v| decode_single_char::<Self>(v.as_str()))
                .collect::<Result<Vec<_>, _>>()
                .map(Cow::Owned),
            NullableBasicValueRef::Value(BasicValueRef::Array(actual)) => {
                Err(basic_type_mismatch::<Self>(&BasicValueRef::Array(actual)))
            }
        }
    }
}

impl_decode_box_slice_via_vec!(u8);
impl_decode_box_slice_via_vec!(u16);
impl_decode_box_slice_via_vec!(u32);
impl_decode_box_slice_via_vec!(u64);
impl_decode_box_slice_via_vec!(u128);
impl_decode_box_slice_via_vec!(usize);
impl_decode_box_slice_via_vec!(i8);
impl_decode_box_slice_via_vec!(i16);
impl_decode_box_slice_via_vec!(i32);
impl_decode_box_slice_via_vec!(i64);
impl_decode_box_slice_via_vec!(i128);
impl_decode_box_slice_via_vec!(isize);
impl_decode_box_slice_via_vec!(f32);
impl_decode_box_slice_via_vec!(f64);
impl_decode_box_slice_via_vec!(char);
impl_decode_box_slice_via_vec!(String);
impl_decode_box_slice_via_vec!(OrderedFloat<f64>);
impl_decode_box_slice_via_vec!(bool);
impl_decode_box_slice_via_vec!(Vec<u8>);
impl_decode_box_slice_via_vec!(NaiveDate);

impl_decode_integer_slice!(u8);
impl_decode_integer_slice!(u16);
impl_decode_integer_slice!(u32);
impl_decode_integer_slice!(u64);
impl_decode_integer_slice!(u128);
impl_decode_integer_slice!(usize);
impl_decode_integer_slice!(i8);
impl_decode_integer_slice!(i16);
impl_decode_integer_slice!(i32);
impl_decode_integer_slice!(i64);
impl_decode_integer_slice!(i128);
impl_decode_integer_slice!(isize);
impl_decode_exact_slice!(String, String);
impl_decode_exact_slice!(OrderedFloat<f64>, Float);
impl_decode_exact_slice!(bool, Boolean);
impl_decode_exact_slice!(Vec<u8>, Binary);
impl_decode_exact_slice!(NaiveDate, Date);
