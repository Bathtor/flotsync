use super::*;
use crate::{Decode, DecodeValueError, linear_data::LinearData};
use chrono::NaiveDate;
use ordered_float::OrderedFloat;
use std::{any::type_name, borrow::Cow, fmt, hash::Hash};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExtractFieldError {
    Null,
    TypeMismatch,
}

enum IntegerValueRef<'a> {
    Byte(&'a u8),
    UInt(&'a u64),
    Int(&'a i64),
    Timestamp(&'a UnixTimestamp),
}

enum IntegerVecRef<'a> {
    Byte(&'a Vec<u8>),
    UInt(&'a Vec<u64>),
    Int(&'a Vec<i64>),
    Timestamp(&'a Vec<UnixTimestamp>),
}

trait DecodeOperationIdBounds:
    Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static
{
}

impl<T> DecodeOperationIdBounds for T where
    T: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static
{
}

fn field_value_kind<OperationId>(value: &InMemoryFieldValue<OperationId>) -> &'static str {
    match value {
        InMemoryFieldValue::LatestValueWins(_) => "LatestValueWins",
        InMemoryFieldValue::LinearString(_) => "LinearString",
        InMemoryFieldValue::LinearList(_) => "LinearList",
        InMemoryFieldValue::MonotonicCounter(_) => "MonotonicCounter",
        InMemoryFieldValue::TotalOrderRegister(_) => "TotalOrderRegister",
        InMemoryFieldValue::TotalOrderFiniteStateRegister(_) => "TotalOrderFiniteStateRegister",
    }
}

fn null_decode_error<T>() -> DecodeValueError {
    DecodeValueError::NullValue {
        requested_type: type_name::<T>(),
    }
}

fn type_mismatch<T, OperationId>(value: &InMemoryFieldValue<OperationId>) -> DecodeValueError {
    DecodeValueError::TypeMismatch {
        requested_type: type_name::<T>(),
        actual_type: Cow::Borrowed(field_value_kind(value)),
    }
}

fn conversion_failed<T>(source_type: &'static str, value: impl fmt::Display) -> DecodeValueError {
    DecodeValueError::ConversionFailed {
        requested_type: type_name::<T>(),
        source_type,
        value: value.to_string(),
    }
}

fn invalid_value<T>(explanation: impl Into<Cow<'static, str>>) -> DecodeValueError {
    DecodeValueError::InvalidValue {
        requested_type: type_name::<T>(),
        explanation: explanation.into(),
    }
}

fn map_extract_error<T, OperationId>(
    value: &InMemoryFieldValue<OperationId>,
    error: ExtractFieldError,
) -> DecodeValueError {
    match error {
        ExtractFieldError::Null => null_decode_error::<T>(),
        ExtractFieldError::TypeMismatch => type_mismatch::<T, _>(value),
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

fn decode_single_char<T>(value: &str) -> Result<char, DecodeValueError> {
    let mut chars = value.chars();
    let Some(ch) = chars.next() else {
        return Err(invalid_value::<T>("expected exactly one character"));
    };
    if chars.next().is_some() {
        return Err(invalid_value::<T>("expected exactly one character"));
    }
    Ok(ch)
}

fn try_decode_char_code<T>(code_point: u32) -> Result<char, DecodeValueError> {
    char::try_from(code_point).map_err(|_| {
        invalid_value::<T>(format!(
            "value {code_point} is not a valid Unicode scalar value"
        ))
    })
}

fn extract_integer_value_ref<'a, OperationId>(
    value: &'a InMemoryFieldValue<OperationId>,
) -> Result<IntegerValueRef<'a>, ExtractFieldError>
where
    OperationId: DecodeOperationIdBounds,
{
    match value {
        InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::Byte(value)) => {
            Ok(IntegerValueRef::Byte(value.content()))
        }
        InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::UInt(value)) => {
            Ok(IntegerValueRef::UInt(value.content()))
        }
        InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::Int(value)) => {
            Ok(IntegerValueRef::Int(value.content()))
        }
        InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::Timestamp(value)) => {
            Ok(IntegerValueRef::Timestamp(value.content()))
        }
        InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableByte(value)) => {
            value
                .content()
                .as_ref()
                .map(IntegerValueRef::Byte)
                .ok_or(ExtractFieldError::Null)
        }
        InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableUInt(value)) => {
            value
                .content()
                .as_ref()
                .map(IntegerValueRef::UInt)
                .ok_or(ExtractFieldError::Null)
        }
        InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableInt(value)) => {
            value
                .content()
                .as_ref()
                .map(IntegerValueRef::Int)
                .ok_or(ExtractFieldError::Null)
        }
        InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableTimestamp(
            value,
        )) => value
            .content()
            .as_ref()
            .map(IntegerValueRef::Timestamp)
            .ok_or(ExtractFieldError::Null),
        InMemoryFieldValue::MonotonicCounter(CounterValue::Byte(value)) => {
            Ok(IntegerValueRef::Byte(value))
        }
        InMemoryFieldValue::MonotonicCounter(CounterValue::UInt(value)) => {
            Ok(IntegerValueRef::UInt(value))
        }
        InMemoryFieldValue::TotalOrderRegister(PrimitiveValue::Byte(value)) => {
            Ok(IntegerValueRef::Byte(value))
        }
        InMemoryFieldValue::TotalOrderRegister(PrimitiveValue::UInt(value)) => {
            Ok(IntegerValueRef::UInt(value))
        }
        InMemoryFieldValue::TotalOrderRegister(PrimitiveValue::Int(value)) => {
            Ok(IntegerValueRef::Int(value))
        }
        InMemoryFieldValue::TotalOrderRegister(PrimitiveValue::Timestamp(value)) => {
            Ok(IntegerValueRef::Timestamp(value))
        }
        InMemoryFieldValue::TotalOrderFiniteStateRegister(NullablePrimitiveValue::Value(
            PrimitiveValue::Byte(value),
        )) => Ok(IntegerValueRef::Byte(value)),
        InMemoryFieldValue::TotalOrderFiniteStateRegister(NullablePrimitiveValue::Value(
            PrimitiveValue::UInt(value),
        )) => Ok(IntegerValueRef::UInt(value)),
        InMemoryFieldValue::TotalOrderFiniteStateRegister(NullablePrimitiveValue::Value(
            PrimitiveValue::Int(value),
        )) => Ok(IntegerValueRef::Int(value)),
        InMemoryFieldValue::TotalOrderFiniteStateRegister(NullablePrimitiveValue::Value(
            PrimitiveValue::Timestamp(value),
        )) => Ok(IntegerValueRef::Timestamp(value)),
        InMemoryFieldValue::TotalOrderFiniteStateRegister(NullablePrimitiveValue::Null) => {
            Err(ExtractFieldError::Null)
        }
        _ => Err(ExtractFieldError::TypeMismatch),
    }
}

fn extract_integer_vec_ref<'a, OperationId>(
    value: &'a InMemoryFieldValue<OperationId>,
) -> Result<IntegerVecRef<'a>, ExtractFieldError>
where
    OperationId: DecodeOperationIdBounds,
{
    match value {
        InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::ByteArray(value)) => {
            Ok(IntegerVecRef::Byte(value.content()))
        }
        InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::UIntArray(value)) => {
            Ok(IntegerVecRef::UInt(value.content()))
        }
        InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::IntArray(value)) => {
            Ok(IntegerVecRef::Int(value.content()))
        }
        InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::TimestampArray(value)) => {
            Ok(IntegerVecRef::Timestamp(value.content()))
        }
        InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableByteArray(
            value,
        )) => value
            .content()
            .as_ref()
            .map(IntegerVecRef::Byte)
            .ok_or(ExtractFieldError::Null),
        InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableUIntArray(
            value,
        )) => value
            .content()
            .as_ref()
            .map(IntegerVecRef::UInt)
            .ok_or(ExtractFieldError::Null),
        InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableIntArray(
            value,
        )) => value
            .content()
            .as_ref()
            .map(IntegerVecRef::Int)
            .ok_or(ExtractFieldError::Null),
        InMemoryFieldValue::LatestValueWins(
            LinearLatestValueWinsValue::NullableTimestampArray(value),
        ) => value
            .content()
            .as_ref()
            .map(IntegerVecRef::Timestamp)
            .ok_or(ExtractFieldError::Null),
        _ => Err(ExtractFieldError::TypeMismatch),
    }
}

fn convert_integer_value<T>(value: IntegerValueRef<'_>) -> Result<T, DecodeValueError>
where
    T: TryFrom<u8> + TryFrom<u64> + TryFrom<i64>,
{
    match value {
        IntegerValueRef::Byte(value) => try_convert::<T, _>(*value, "u8"),
        IntegerValueRef::UInt(value) => try_convert::<T, _>(*value, "u64"),
        IntegerValueRef::Int(value) => try_convert::<T, _>(*value, "i64"),
        IntegerValueRef::Timestamp(value) => try_convert::<T, _>(*value, "UnixTimestamp"),
    }
}

fn convert_integer_vec<T>(value: IntegerVecRef<'_>) -> Result<Vec<T>, DecodeValueError>
where
    T: TryFrom<u8> + TryFrom<u64> + TryFrom<i64>,
{
    match value {
        IntegerVecRef::Byte(value) => collect_try_convert::<T, _, _>(value.iter().copied(), "u8"),
        IntegerVecRef::UInt(value) => collect_try_convert::<T, _, _>(value.iter().copied(), "u64"),
        IntegerVecRef::Int(value) => collect_try_convert::<T, _, _>(value.iter().copied(), "i64"),
        IntegerVecRef::Timestamp(value) => {
            collect_try_convert::<T, _, _>(value.iter().copied(), "UnixTimestamp")
        }
    }
}

macro_rules! define_exact_scalar_extractor {
    ($fn_name:ident, $ty:ty, $nonnull_variant:ident, $nullable_variant:ident, $primitive_variant:ident) => {
        fn $fn_name<OperationId>(
            value: &InMemoryFieldValue<OperationId>,
        ) -> Result<&$ty, ExtractFieldError>
        where
            OperationId: DecodeOperationIdBounds,
        {
            match value {
                InMemoryFieldValue::LatestValueWins(
                    LinearLatestValueWinsValue::$nonnull_variant(value),
                ) => Ok(value.content()),
                InMemoryFieldValue::LatestValueWins(
                    LinearLatestValueWinsValue::$nullable_variant(value),
                ) => value.content().as_ref().ok_or(ExtractFieldError::Null),
                InMemoryFieldValue::TotalOrderRegister(PrimitiveValue::$primitive_variant(
                    value,
                )) => Ok(value),
                InMemoryFieldValue::TotalOrderFiniteStateRegister(
                    NullablePrimitiveValue::Value(PrimitiveValue::$primitive_variant(value)),
                ) => Ok(value),
                InMemoryFieldValue::TotalOrderFiniteStateRegister(NullablePrimitiveValue::Null) => {
                    Err(ExtractFieldError::Null)
                }
                _ => Err(ExtractFieldError::TypeMismatch),
            }
        }
    };
}

macro_rules! define_exact_array_extractor {
    ($fn_name:ident, $ty:ty, $nonnull_variant:ident, $nullable_variant:ident) => {
        fn $fn_name<OperationId>(
            value: &InMemoryFieldValue<OperationId>,
        ) -> Result<&Vec<$ty>, ExtractFieldError>
        where
            OperationId: DecodeOperationIdBounds,
        {
            match value {
                InMemoryFieldValue::LatestValueWins(
                    LinearLatestValueWinsValue::$nonnull_variant(value),
                ) => Ok(value.content()),
                InMemoryFieldValue::LatestValueWins(
                    LinearLatestValueWinsValue::$nullable_variant(value),
                ) => value.content().as_ref().ok_or(ExtractFieldError::Null),
                _ => Err(ExtractFieldError::TypeMismatch),
            }
        }
    };
}

define_exact_scalar_extractor!(
    extract_string_value_ref,
    String,
    String,
    NullableString,
    String
);
define_exact_scalar_extractor!(
    extract_float_value_ref,
    OrderedFloat<f64>,
    Float,
    NullableFloat,
    Float
);
define_exact_scalar_extractor!(
    extract_boolean_value_ref,
    bool,
    Boolean,
    NullableBoolean,
    Boolean
);
define_exact_scalar_extractor!(
    extract_binary_value_ref,
    Vec<u8>,
    Binary,
    NullableBinary,
    Binary
);
define_exact_scalar_extractor!(extract_date_value_ref, NaiveDate, Date, NullableDate, Date);

define_exact_array_extractor!(
    extract_string_vec_ref,
    String,
    StringArray,
    NullableStringArray
);
define_exact_array_extractor!(
    extract_float_vec_ref,
    OrderedFloat<f64>,
    FloatArray,
    NullableFloatArray
);
define_exact_array_extractor!(
    extract_boolean_vec_ref,
    bool,
    BooleanArray,
    NullableBooleanArray
);
define_exact_array_extractor!(
    extract_binary_vec_ref,
    Vec<u8>,
    BinaryArray,
    NullableBinaryArray
);
define_exact_array_extractor!(
    extract_date_vec_ref,
    NaiveDate,
    DateArray,
    NullableDateArray
);

macro_rules! impl_decode_exact_scalar {
    ($target:ty, $extractor:ident) => {
        impl<OperationId> Decode<OperationId> for $target
        where
            OperationId: DecodeOperationIdBounds,
        {
            fn decode<'a>(
                value: &'a InMemoryFieldValue<OperationId>,
            ) -> Result<Cow<'a, Self>, DecodeValueError> {
                $extractor(value)
                    .map(Cow::Borrowed)
                    .map_err(|err| map_extract_error::<Self, _>(value, err))
            }
        }
    };
}

macro_rules! impl_decode_integer_scalar_borrowed {
    ($target:ty, [$($exact_variant:ident),+ $(,)?]) => {
        impl<OperationId> Decode<OperationId> for $target
        where
            OperationId: DecodeOperationIdBounds,
        {
            fn decode<'a>(
                value: &'a InMemoryFieldValue<OperationId>,
            ) -> Result<Cow<'a, Self>, DecodeValueError> {
                match extract_integer_value_ref(value)
                    .map_err(|err| map_extract_error::<Self, _>(value, err))?
                {
                    $(IntegerValueRef::$exact_variant(value) => Ok(Cow::Borrowed(value)),)+
                    value => Ok(Cow::Owned(convert_integer_value::<Self>(value)?)),
                }
            }
        }
    };
}

macro_rules! impl_decode_integer_scalar_owned {
    ($target:ty) => {
        impl<OperationId> Decode<OperationId> for $target
        where
            OperationId: DecodeOperationIdBounds,
        {
            fn decode<'a>(
                value: &'a InMemoryFieldValue<OperationId>,
            ) -> Result<Cow<'a, Self>, DecodeValueError> {
                let value = extract_integer_value_ref(value)
                    .map_err(|err| map_extract_error::<Self, _>(value, err))?;
                Ok(Cow::Owned(convert_integer_value::<Self>(value)?))
            }
        }
    };
}

macro_rules! impl_decode_exact_vec_or_list {
    ($element:ty, $extractor:ident, $list_variant:ident) => {
        impl<OperationId> Decode<OperationId> for Vec<$element>
        where
            OperationId: DecodeOperationIdBounds,
        {
            fn decode<'a>(
                value: &'a InMemoryFieldValue<OperationId>,
            ) -> Result<Cow<'a, Self>, DecodeValueError> {
                match $extractor(value) {
                    Ok(values) => Ok(Cow::Borrowed(values)),
                    Err(ExtractFieldError::Null) => Err(null_decode_error::<Self>()),
                    Err(ExtractFieldError::TypeMismatch) => match value {
                        InMemoryFieldValue::LinearList(LinearListValue::$list_variant(values)) => {
                            Ok(Cow::Owned(values.iter().cloned().collect()))
                        }
                        _ => Err(type_mismatch::<Self, _>(value)),
                    },
                }
            }
        }
    };
}

macro_rules! impl_decode_integer_vec_borrowed {
    ($element:ty, [$($exact_variant:ident),+ $(,)?]) => {
        impl<OperationId> Decode<OperationId> for Vec<$element>
        where
            OperationId: DecodeOperationIdBounds,
        {
            fn decode<'a>(
                value: &'a InMemoryFieldValue<OperationId>,
            ) -> Result<Cow<'a, Self>, DecodeValueError> {
                match extract_integer_vec_ref(value) {
                    Ok(value) => match value {
                        $(IntegerVecRef::$exact_variant(values) => Ok(Cow::Borrowed(values)),)+
                        values => Ok(Cow::Owned(convert_integer_vec::<$element>(values)?)),
                    },
                    Err(ExtractFieldError::Null) => Err(null_decode_error::<Self>()),
                    Err(ExtractFieldError::TypeMismatch) => match value {
                        InMemoryFieldValue::LinearList(LinearListValue::Byte(values)) => Ok(
                            Cow::Owned(collect_try_convert::<$element, _, _>(
                                values.iter().copied(),
                                "u8",
                            )?),
                        ),
                        InMemoryFieldValue::LinearList(LinearListValue::UInt(values)) => Ok(
                            Cow::Owned(collect_try_convert::<$element, _, _>(
                                values.iter().copied(),
                                "u64",
                            )?),
                        ),
                        InMemoryFieldValue::LinearList(LinearListValue::Int(values)) => Ok(
                            Cow::Owned(collect_try_convert::<$element, _, _>(
                                values.iter().copied(),
                                "i64",
                            )?),
                        ),
                        InMemoryFieldValue::LinearList(LinearListValue::Timestamp(values)) => Ok(
                            Cow::Owned(collect_try_convert::<$element, _, _>(
                                values.iter().copied(),
                                "UnixTimestamp",
                            )?),
                        ),
                        _ => Err(type_mismatch::<Self, _>(value)),
                    },
                }
            }
        }
    };
}

macro_rules! impl_decode_integer_vec_owned {
    ($element:ty) => {
        impl<OperationId> Decode<OperationId> for Vec<$element>
        where
            OperationId: DecodeOperationIdBounds,
        {
            fn decode<'a>(
                value: &'a InMemoryFieldValue<OperationId>,
            ) -> Result<Cow<'a, Self>, DecodeValueError> {
                match extract_integer_vec_ref(value) {
                    Ok(value) => Ok(Cow::Owned(convert_integer_vec::<$element>(value)?)),
                    Err(ExtractFieldError::Null) => Err(null_decode_error::<Self>()),
                    Err(ExtractFieldError::TypeMismatch) => match value {
                        InMemoryFieldValue::LinearList(LinearListValue::Byte(values)) => {
                            Ok(Cow::Owned(collect_try_convert::<$element, _, _>(
                                values.iter().copied(),
                                "u8",
                            )?))
                        }
                        InMemoryFieldValue::LinearList(LinearListValue::UInt(values)) => {
                            Ok(Cow::Owned(collect_try_convert::<$element, _, _>(
                                values.iter().copied(),
                                "u64",
                            )?))
                        }
                        InMemoryFieldValue::LinearList(LinearListValue::Int(values)) => {
                            Ok(Cow::Owned(collect_try_convert::<$element, _, _>(
                                values.iter().copied(),
                                "i64",
                            )?))
                        }
                        InMemoryFieldValue::LinearList(LinearListValue::Timestamp(values)) => {
                            Ok(Cow::Owned(collect_try_convert::<$element, _, _>(
                                values.iter().copied(),
                                "UnixTimestamp",
                            )?))
                        }
                        _ => Err(type_mismatch::<Self, _>(value)),
                    },
                }
            }
        }
    };
}

macro_rules! impl_decode_box_slice_via_vec {
    ($element:ty) => {
        impl<OperationId> Decode<OperationId> for Box<[$element]>
        where
            OperationId: DecodeOperationIdBounds,
        {
            fn decode<'a>(
                value: &'a InMemoryFieldValue<OperationId>,
            ) -> Result<Cow<'a, Self>, DecodeValueError> {
                let values = <Vec<$element> as Decode<OperationId>>::decode(value)?;
                Ok(Cow::Owned(values.into_owned().into_boxed_slice()))
            }
        }
    };
}

macro_rules! impl_decode_slice_via_vec {
    ($element:ty) => {
        impl<OperationId> Decode<OperationId> for [$element]
        where
            OperationId: DecodeOperationIdBounds,
        {
            fn decode<'a>(
                value: &'a InMemoryFieldValue<OperationId>,
            ) -> Result<Cow<'a, Self>, DecodeValueError> {
                match <Vec<$element> as Decode<OperationId>>::decode(value)? {
                    Cow::Borrowed(values) => Ok(Cow::Borrowed(values.as_slice())),
                    Cow::Owned(values) => Ok(Cow::Owned(values)),
                }
            }
        }
    };
}

impl_decode_exact_scalar!(OrderedFloat<f64>, extract_float_value_ref);
impl_decode_exact_scalar!(bool, extract_boolean_value_ref);
impl_decode_exact_scalar!(NaiveDate, extract_date_value_ref);

impl_decode_integer_scalar_borrowed!(u8, [Byte]);
impl_decode_integer_scalar_owned!(u16);
impl_decode_integer_scalar_owned!(u32);
impl_decode_integer_scalar_borrowed!(u64, [UInt]);
impl_decode_integer_scalar_owned!(u128);
impl_decode_integer_scalar_owned!(usize);
impl_decode_integer_scalar_owned!(i8);
impl_decode_integer_scalar_owned!(i16);
impl_decode_integer_scalar_owned!(i32);
impl_decode_integer_scalar_borrowed!(i64, [Int, Timestamp]);
impl_decode_integer_scalar_owned!(i128);
impl_decode_integer_scalar_owned!(isize);

impl<OperationId> Decode<OperationId> for String
where
    OperationId: DecodeOperationIdBounds,
{
    fn decode<'a>(
        value: &'a InMemoryFieldValue<OperationId>,
    ) -> Result<Cow<'a, Self>, DecodeValueError> {
        match extract_string_value_ref(value) {
            Ok(value) => Ok(Cow::Borrowed(value)),
            Err(ExtractFieldError::Null) => Err(null_decode_error::<Self>()),
            Err(ExtractFieldError::TypeMismatch) => match value {
                InMemoryFieldValue::LinearString(value) => {
                    Ok(Cow::Owned(value.iter_values().collect()))
                }
                _ => Err(type_mismatch::<Self, _>(value)),
            },
        }
    }
}

impl<OperationId> Decode<OperationId> for str
where
    OperationId: DecodeOperationIdBounds,
{
    fn decode<'a>(
        value: &'a InMemoryFieldValue<OperationId>,
    ) -> Result<Cow<'a, Self>, DecodeValueError> {
        match <String as Decode<OperationId>>::decode(value)? {
            Cow::Borrowed(value) => Ok(Cow::Borrowed(value.as_str())),
            Cow::Owned(value) => Ok(Cow::Owned(value)),
        }
    }
}

impl<OperationId> Decode<OperationId> for f64
where
    OperationId: DecodeOperationIdBounds,
{
    fn decode<'a>(
        value: &'a InMemoryFieldValue<OperationId>,
    ) -> Result<Cow<'a, Self>, DecodeValueError> {
        let value = extract_float_value_ref(value)
            .map_err(|err| map_extract_error::<Self, _>(value, err))?;
        Ok(Cow::Owned(value.into_inner()))
    }
}

impl<OperationId> Decode<OperationId> for f32
where
    OperationId: DecodeOperationIdBounds,
{
    fn decode<'a>(
        value: &'a InMemoryFieldValue<OperationId>,
    ) -> Result<Cow<'a, Self>, DecodeValueError> {
        let value = extract_float_value_ref(value)
            .map_err(|err| map_extract_error::<Self, _>(value, err))?;
        Ok(Cow::Owned(value.into_inner() as f32))
    }
}

impl<OperationId> Decode<OperationId> for char
where
    OperationId: DecodeOperationIdBounds,
{
    fn decode<'a>(
        value: &'a InMemoryFieldValue<OperationId>,
    ) -> Result<Cow<'a, Self>, DecodeValueError> {
        match extract_string_value_ref(value) {
            Ok(value) => Ok(Cow::Owned(decode_single_char::<Self>(value.as_str())?)),
            Err(ExtractFieldError::Null) => Err(null_decode_error::<Self>()),
            Err(ExtractFieldError::TypeMismatch) => match value {
                InMemoryFieldValue::LinearString(value) => {
                    let rendered: String = value.iter_values().collect();
                    Ok(Cow::Owned(decode_single_char::<Self>(&rendered)?))
                }
                _ => {
                    let code_point = convert_integer_value::<u32>(
                        extract_integer_value_ref(value)
                            .map_err(|err| map_extract_error::<Self, _>(value, err))?,
                    )?;
                    Ok(Cow::Owned(try_decode_char_code::<Self>(code_point)?))
                }
            },
        }
    }
}

impl<OperationId> Decode<OperationId> for Vec<u8>
where
    OperationId: DecodeOperationIdBounds,
{
    fn decode<'a>(
        value: &'a InMemoryFieldValue<OperationId>,
    ) -> Result<Cow<'a, Self>, DecodeValueError> {
        match extract_binary_value_ref(value) {
            Ok(value) => Ok(Cow::Borrowed(value)),
            Err(ExtractFieldError::Null) => Err(null_decode_error::<Self>()),
            Err(ExtractFieldError::TypeMismatch) => match extract_integer_vec_ref(value) {
                Ok(IntegerVecRef::Byte(values)) => Ok(Cow::Borrowed(values)),
                Ok(values) => Ok(Cow::Owned(convert_integer_vec::<u8>(values)?)),
                Err(ExtractFieldError::Null) => Err(null_decode_error::<Self>()),
                Err(ExtractFieldError::TypeMismatch) => match value {
                    InMemoryFieldValue::LinearList(LinearListValue::Byte(values)) => {
                        Ok(Cow::Owned(values.iter().copied().collect()))
                    }
                    InMemoryFieldValue::LinearList(LinearListValue::UInt(values)) => {
                        Ok(Cow::Owned(collect_try_convert::<u8, _, _>(
                            values.iter().copied(),
                            "u64",
                        )?))
                    }
                    InMemoryFieldValue::LinearList(LinearListValue::Int(values)) => Ok(Cow::Owned(
                        collect_try_convert::<u8, _, _>(values.iter().copied(), "i64")?,
                    )),
                    InMemoryFieldValue::LinearList(LinearListValue::Timestamp(values)) => {
                        Ok(Cow::Owned(collect_try_convert::<u8, _, _>(
                            values.iter().copied(),
                            "UnixTimestamp",
                        )?))
                    }
                    _ => Err(type_mismatch::<Self, _>(value)),
                },
            },
        }
    }
}

impl_decode_exact_vec_or_list!(String, extract_string_vec_ref, String);
impl_decode_exact_vec_or_list!(OrderedFloat<f64>, extract_float_vec_ref, Float);
impl_decode_exact_vec_or_list!(bool, extract_boolean_vec_ref, Boolean);
impl_decode_exact_vec_or_list!(Vec<u8>, extract_binary_vec_ref, Binary);
impl_decode_exact_vec_or_list!(NaiveDate, extract_date_vec_ref, Date);

impl_decode_integer_vec_owned!(u16);
impl_decode_integer_vec_owned!(u32);
impl_decode_integer_vec_borrowed!(u64, [UInt]);
impl_decode_integer_vec_owned!(u128);
impl_decode_integer_vec_owned!(usize);
impl_decode_integer_vec_owned!(i8);
impl_decode_integer_vec_owned!(i16);
impl_decode_integer_vec_owned!(i32);
impl_decode_integer_vec_borrowed!(i64, [Int, Timestamp]);
impl_decode_integer_vec_owned!(i128);
impl_decode_integer_vec_owned!(isize);

impl<OperationId> Decode<OperationId> for Vec<f64>
where
    OperationId: DecodeOperationIdBounds,
{
    fn decode<'a>(
        value: &'a InMemoryFieldValue<OperationId>,
    ) -> Result<Cow<'a, Self>, DecodeValueError> {
        match extract_float_vec_ref(value) {
            Ok(values) => Ok(Cow::Owned(
                values.iter().map(|value| value.into_inner()).collect(),
            )),
            Err(ExtractFieldError::Null) => Err(null_decode_error::<Self>()),
            Err(ExtractFieldError::TypeMismatch) => match value {
                InMemoryFieldValue::LinearList(LinearListValue::Float(values)) => Ok(Cow::Owned(
                    values.iter().map(|value| value.into_inner()).collect(),
                )),
                _ => Err(type_mismatch::<Self, _>(value)),
            },
        }
    }
}

impl<OperationId> Decode<OperationId> for Vec<f32>
where
    OperationId: DecodeOperationIdBounds,
{
    fn decode<'a>(
        value: &'a InMemoryFieldValue<OperationId>,
    ) -> Result<Cow<'a, Self>, DecodeValueError> {
        match extract_float_vec_ref(value) {
            Ok(values) => Ok(Cow::Owned(
                values
                    .iter()
                    .map(|value| value.into_inner() as f32)
                    .collect(),
            )),
            Err(ExtractFieldError::Null) => Err(null_decode_error::<Self>()),
            Err(ExtractFieldError::TypeMismatch) => match value {
                InMemoryFieldValue::LinearList(LinearListValue::Float(values)) => Ok(Cow::Owned(
                    values
                        .iter()
                        .map(|value| value.into_inner() as f32)
                        .collect(),
                )),
                _ => Err(type_mismatch::<Self, _>(value)),
            },
        }
    }
}

impl<OperationId> Decode<OperationId> for Vec<char>
where
    OperationId: DecodeOperationIdBounds,
{
    fn decode<'a>(
        value: &'a InMemoryFieldValue<OperationId>,
    ) -> Result<Cow<'a, Self>, DecodeValueError> {
        match extract_string_value_ref(value) {
            Ok(value) => Ok(Cow::Owned(value.chars().collect())),
            Err(ExtractFieldError::Null) => Err(null_decode_error::<Self>()),
            Err(ExtractFieldError::TypeMismatch) => match value {
                InMemoryFieldValue::LinearString(value) => Ok(Cow::Owned(
                    value.iter_values().collect::<String>().chars().collect(),
                )),
                _ => Err(type_mismatch::<Self, _>(value)),
            },
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
impl_decode_slice_via_vec!(u8);
impl_decode_slice_via_vec!(u16);
impl_decode_slice_via_vec!(u32);
impl_decode_slice_via_vec!(u64);
impl_decode_slice_via_vec!(u128);
impl_decode_slice_via_vec!(usize);
impl_decode_slice_via_vec!(i8);
impl_decode_slice_via_vec!(i16);
impl_decode_slice_via_vec!(i32);
impl_decode_slice_via_vec!(i64);
impl_decode_slice_via_vec!(i128);
impl_decode_slice_via_vec!(isize);
impl_decode_slice_via_vec!(f32);
impl_decode_slice_via_vec!(f64);
impl_decode_slice_via_vec!(char);
impl_decode_slice_via_vec!(String);
impl_decode_slice_via_vec!(OrderedFloat<f64>);
impl_decode_slice_via_vec!(bool);
impl_decode_slice_via_vec!(Vec<u8>);
impl_decode_slice_via_vec!(NaiveDate);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        RowOperations,
        schema::{Direction, Field},
    };
    use std::{assert_matches, collections::HashMap};

    type OperationId = u32;

    fn make_field(name: &str, data_type: ReplicatedDataType) -> (String, Field) {
        (
            name.to_owned(),
            Field {
                name: name.to_owned(),
                data_type,
                default_value: None,
                metadata: HashMap::new(),
            },
        )
    }

    fn make_schema(fields: impl IntoIterator<Item = (String, Field)>) -> Schema {
        Schema {
            columns: fields.into_iter().collect(),
            metadata: HashMap::new(),
        }
    }

    fn indexed_id(index: u32) -> IdWithIndex<u32> {
        IdWithIndex { id: 1, index }
    }

    fn make_row<'a>(
        data: &'a InMemoryData<(), OperationId>,
    ) -> InMemoryDataRow<'a, (), OperationId> {
        InMemoryDataRow { data, row_index: 0 }
    }

    #[test]
    fn get_field_value_borrows_exact_latest_value_wins_strings() {
        let schema = make_schema([make_field(
            "title",
            ReplicatedDataType::LatestValueWins {
                value_type: NullableBasicDataType::NonNull(BasicDataType::Primitive(
                    PrimitiveType::String,
                )),
            },
        )]);
        let mut data = InMemoryData::with_owned_schema(schema);
        data.push_row_from_named_fields([(
            "title",
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::String(
                LinearLatestValueWins::new(
                    "hello".to_owned(),
                    [indexed_id(0), indexed_id(1), indexed_id(2)],
                ),
            )),
        )])
        .unwrap();

        let row = make_row(&data);
        let value = row.get_field_value::<String>("title").unwrap();
        assert_matches!(value, Cow::Borrowed(value) if value == "hello");

        let value = row.get_field_value::<str>("title").unwrap();
        assert_matches!(value, Cow::Borrowed(value) if value == "hello");
    }

    #[test]
    fn get_field_value_projects_linear_string_to_owned_string() {
        let schema = make_schema([make_field("title", ReplicatedDataType::LinearString)]);
        let mut data = InMemoryData::with_owned_schema(schema);
        data.push_row_from_named_fields([(
            "title",
            InMemoryFieldValue::LinearString(LinearString::with_value("hej".to_owned(), 1)),
        )])
        .unwrap();

        let row = make_row(&data);
        let value = row.get_field_value::<String>("title").unwrap();
        assert_matches!(value, Cow::Owned(value) if value == "hej");
    }

    #[test]
    fn get_field_value_converts_integer_collections_and_boxed_slices() {
        let schema = make_schema([
            make_field(
                "count",
                ReplicatedDataType::TotalOrderRegister {
                    value_type: PrimitiveType::UInt,
                    direction: Direction::Ascending,
                },
            ),
            make_field(
                "numbers",
                ReplicatedDataType::LinearList {
                    value_type: PrimitiveType::UInt,
                },
            ),
        ]);
        let mut data = InMemoryData::with_owned_schema(schema);
        data.push_row_from_named_fields([
            (
                "count",
                InMemoryFieldValue::TotalOrderRegister(PrimitiveValue::UInt(42)),
            ),
            (
                "numbers",
                InMemoryFieldValue::LinearList(LinearListValue::UInt(LinearList::with_values(
                    [1_u64, 2, 3],
                    4,
                ))),
            ),
        ])
        .unwrap();

        let row = make_row(&data);
        assert_eq!(*row.get_field_value::<u16>("count").unwrap(), 42);

        let values = row.get_field_value::<Vec<u32>>("numbers").unwrap();
        assert_eq!(values.as_ref(), &[1, 2, 3]);

        let values = row.get_field_value::<[u32]>("numbers").unwrap();
        assert_eq!(values.as_ref(), &[1, 2, 3]);

        let boxed = row.get_field_value::<Box<[u32]>>("numbers").unwrap();
        assert_eq!(boxed.as_ref().as_ref(), &[1, 2, 3]);
    }

    #[test]
    fn nullable_field_value_distinguishes_null_from_missing_and_present() {
        let schema = make_schema([make_field(
            "maybe_count",
            ReplicatedDataType::LatestValueWins {
                value_type: NullableBasicDataType::Nullable(BasicDataType::Primitive(
                    PrimitiveType::UInt,
                )),
            },
        )]);
        let mut data = InMemoryData::with_owned_schema(schema);
        data.push_row_from_named_fields([(
            "maybe_count",
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableUInt(
                LinearLatestValueWins::new(None, [indexed_id(0), indexed_id(1), indexed_id(2)]),
            )),
        )])
        .unwrap();

        let row = make_row(&data);
        assert_matches!(
            row.get_field_value::<u64>("maybe_count"),
            Err(DecodeValueError::NullValue { .. })
        );
        assert_eq!(
            row.get_nullable_field_value::<u64>("maybe_count").unwrap(),
            None
        );
        assert_matches!(
            row.get_field_value::<u64>("missing"),
            Err(DecodeValueError::FieldDoesNotExist { .. })
        );
    }
}
