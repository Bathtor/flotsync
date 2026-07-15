use super::{
    CounterValue,
    InMemoryFieldState,
    LinearLatestValueWinsState,
    LinearListState,
    NullablePrimitiveValue,
    PrimitiveValue,
    UnixTimestamp,
};
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

fn field_value_kind<OperationId>(value: &InMemoryFieldState<OperationId>) -> &'static str {
    match value {
        InMemoryFieldState::LatestValueWins(_) => "LatestValueWins",
        InMemoryFieldState::LinearString(_) => "LinearString",
        InMemoryFieldState::LinearList(_) => "LinearList",
        InMemoryFieldState::MonotonicCounter(_) => "MonotonicCounter",
        InMemoryFieldState::TotalOrderRegister(_) => "TotalOrderRegister",
        InMemoryFieldState::TotalOrderFiniteStateRegister(_) => "TotalOrderFiniteStateRegister",
    }
}

fn null_decode_error<T>() -> DecodeValueError {
    DecodeValueError::NullValue {
        requested_type: type_name::<T>(),
    }
}

fn type_mismatch<T, OperationId>(value: &InMemoryFieldState<OperationId>) -> DecodeValueError {
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
    value: &InMemoryFieldState<OperationId>,
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

#[allow(
    clippy::cast_possible_truncation,
    reason = "Decoding `f32` from the in-memory f64 representation intentionally narrows precision."
)]
fn narrow_f64_to_f32(value: OrderedFloat<f64>) -> f32 {
    value.into_inner() as f32
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

fn extract_integer_value_ref<OperationId>(
    value: &InMemoryFieldState<OperationId>,
) -> Result<IntegerValueRef<'_>, ExtractFieldError>
where
    OperationId: DecodeOperationIdBounds,
{
    match value {
        InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::Byte(value)) => {
            Ok(IntegerValueRef::Byte(value.content()))
        }
        InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::UInt(value)) => {
            Ok(IntegerValueRef::UInt(value.content()))
        }
        InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::Int(value)) => {
            Ok(IntegerValueRef::Int(value.content()))
        }
        InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::Timestamp(value)) => {
            Ok(IntegerValueRef::Timestamp(value.content()))
        }
        InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableByte(value)) => {
            value
                .content()
                .as_ref()
                .map(IntegerValueRef::Byte)
                .ok_or(ExtractFieldError::Null)
        }
        InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableUInt(value)) => {
            value
                .content()
                .as_ref()
                .map(IntegerValueRef::UInt)
                .ok_or(ExtractFieldError::Null)
        }
        InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableInt(value)) => {
            value
                .content()
                .as_ref()
                .map(IntegerValueRef::Int)
                .ok_or(ExtractFieldError::Null)
        }
        InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableTimestamp(
            value,
        )) => value
            .content()
            .as_ref()
            .map(IntegerValueRef::Timestamp)
            .ok_or(ExtractFieldError::Null),
        InMemoryFieldState::MonotonicCounter(CounterValue::Byte(value))
        | InMemoryFieldState::TotalOrderRegister(PrimitiveValue::Byte(value)) => {
            Ok(IntegerValueRef::Byte(value))
        }
        InMemoryFieldState::MonotonicCounter(CounterValue::UInt(value))
        | InMemoryFieldState::TotalOrderRegister(PrimitiveValue::UInt(value)) => {
            Ok(IntegerValueRef::UInt(value))
        }
        InMemoryFieldState::TotalOrderRegister(PrimitiveValue::Int(value)) => {
            Ok(IntegerValueRef::Int(value))
        }
        InMemoryFieldState::TotalOrderRegister(PrimitiveValue::Timestamp(value)) => {
            Ok(IntegerValueRef::Timestamp(value))
        }
        InMemoryFieldState::TotalOrderFiniteStateRegister(NullablePrimitiveValue::Value(
            PrimitiveValue::Byte(value),
        )) => Ok(IntegerValueRef::Byte(value)),
        InMemoryFieldState::TotalOrderFiniteStateRegister(NullablePrimitiveValue::Value(
            PrimitiveValue::UInt(value),
        )) => Ok(IntegerValueRef::UInt(value)),
        InMemoryFieldState::TotalOrderFiniteStateRegister(NullablePrimitiveValue::Value(
            PrimitiveValue::Int(value),
        )) => Ok(IntegerValueRef::Int(value)),
        InMemoryFieldState::TotalOrderFiniteStateRegister(NullablePrimitiveValue::Value(
            PrimitiveValue::Timestamp(value),
        )) => Ok(IntegerValueRef::Timestamp(value)),
        InMemoryFieldState::TotalOrderFiniteStateRegister(NullablePrimitiveValue::Null) => {
            Err(ExtractFieldError::Null)
        }
        _ => Err(ExtractFieldError::TypeMismatch),
    }
}

fn extract_integer_vec_ref<OperationId>(
    value: &InMemoryFieldState<OperationId>,
) -> Result<IntegerVecRef<'_>, ExtractFieldError>
where
    OperationId: DecodeOperationIdBounds,
{
    match value {
        InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::ByteArray(value)) => {
            Ok(IntegerVecRef::Byte(value.content()))
        }
        InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::UIntArray(value)) => {
            Ok(IntegerVecRef::UInt(value.content()))
        }
        InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::IntArray(value)) => {
            Ok(IntegerVecRef::Int(value.content()))
        }
        InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::TimestampArray(value)) => {
            Ok(IntegerVecRef::Timestamp(value.content()))
        }
        InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableByteArray(
            value,
        )) => value
            .content()
            .as_ref()
            .map(IntegerVecRef::Byte)
            .ok_or(ExtractFieldError::Null),
        InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableUIntArray(
            value,
        )) => value
            .content()
            .as_ref()
            .map(IntegerVecRef::UInt)
            .ok_or(ExtractFieldError::Null),
        InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableIntArray(
            value,
        )) => value
            .content()
            .as_ref()
            .map(IntegerVecRef::Int)
            .ok_or(ExtractFieldError::Null),
        InMemoryFieldState::LatestValueWins(
            LinearLatestValueWinsState::NullableTimestampArray(value),
        ) => value
            .content()
            .as_ref()
            .map(IntegerVecRef::Timestamp)
            .ok_or(ExtractFieldError::Null),
        _ => Err(ExtractFieldError::TypeMismatch),
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "The small reference enum is a local extraction result and is intentionally consumed by conversion helpers."
)]
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

#[allow(
    clippy::needless_pass_by_value,
    reason = "The small reference enum is a local extraction result and is intentionally consumed by conversion helpers."
)]
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
            value: &InMemoryFieldState<OperationId>,
        ) -> Result<&$ty, ExtractFieldError>
        where
            OperationId: DecodeOperationIdBounds,
        {
            match value {
                InMemoryFieldState::LatestValueWins(
                    LinearLatestValueWinsState::$nonnull_variant(value),
                ) => Ok(value.content()),
                InMemoryFieldState::LatestValueWins(
                    LinearLatestValueWinsState::$nullable_variant(value),
                ) => value.content().as_ref().ok_or(ExtractFieldError::Null),
                InMemoryFieldState::TotalOrderRegister(PrimitiveValue::$primitive_variant(
                    value,
                )) => Ok(value),
                InMemoryFieldState::TotalOrderFiniteStateRegister(
                    NullablePrimitiveValue::Value(PrimitiveValue::$primitive_variant(value)),
                ) => Ok(value),
                InMemoryFieldState::TotalOrderFiniteStateRegister(NullablePrimitiveValue::Null) => {
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
            value: &InMemoryFieldState<OperationId>,
        ) -> Result<&Vec<$ty>, ExtractFieldError>
        where
            OperationId: DecodeOperationIdBounds,
        {
            match value {
                InMemoryFieldState::LatestValueWins(
                    LinearLatestValueWinsState::$nonnull_variant(value),
                ) => Ok(value.content()),
                InMemoryFieldState::LatestValueWins(
                    LinearLatestValueWinsState::$nullable_variant(value),
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
            fn decode(
                value: &InMemoryFieldState<OperationId>,
            ) -> Result<Cow<'_, Self>, DecodeValueError> {
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
            fn decode(
                value: &InMemoryFieldState<OperationId>,
            ) -> Result<Cow<'_, Self>, DecodeValueError> {
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
            fn decode(
                value: &InMemoryFieldState<OperationId>,
            ) -> Result<Cow<'_, Self>, DecodeValueError> {
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
            fn decode(
                value: &InMemoryFieldState<OperationId>,
            ) -> Result<Cow<'_, Self>, DecodeValueError> {
                match $extractor(value) {
                    Ok(values) => Ok(Cow::Borrowed(values)),
                    Err(ExtractFieldError::Null) => Err(null_decode_error::<Self>()),
                    Err(ExtractFieldError::TypeMismatch) => match value {
                        InMemoryFieldState::LinearList(LinearListState::$list_variant(values)) => {
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
            fn decode(
                value: &InMemoryFieldState<OperationId>,
            ) -> Result<Cow<'_, Self>, DecodeValueError> {
                match extract_integer_vec_ref(value) {
                    Ok(value) => match value {
                        $(IntegerVecRef::$exact_variant(values) => Ok(Cow::Borrowed(values)),)+
                        values => Ok(Cow::Owned(convert_integer_vec::<$element>(values)?)),
                    },
                    Err(ExtractFieldError::Null) => Err(null_decode_error::<Self>()),
                    Err(ExtractFieldError::TypeMismatch) => match value {
                        InMemoryFieldState::LinearList(LinearListState::Byte(values)) => Ok(
                            Cow::Owned(collect_try_convert::<$element, _, _>(
                                values.iter().copied(),
                                "u8",
                            )?),
                        ),
                        InMemoryFieldState::LinearList(LinearListState::UInt(values)) => Ok(
                            Cow::Owned(collect_try_convert::<$element, _, _>(
                                values.iter().copied(),
                                "u64",
                            )?),
                        ),
                        InMemoryFieldState::LinearList(LinearListState::Int(values)) => Ok(
                            Cow::Owned(collect_try_convert::<$element, _, _>(
                                values.iter().copied(),
                                "i64",
                            )?),
                        ),
                        InMemoryFieldState::LinearList(LinearListState::Timestamp(values)) => Ok(
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
            fn decode(
                value: &InMemoryFieldState<OperationId>,
            ) -> Result<Cow<'_, Self>, DecodeValueError> {
                match extract_integer_vec_ref(value) {
                    Ok(value) => Ok(Cow::Owned(convert_integer_vec::<$element>(value)?)),
                    Err(ExtractFieldError::Null) => Err(null_decode_error::<Self>()),
                    Err(ExtractFieldError::TypeMismatch) => match value {
                        InMemoryFieldState::LinearList(LinearListState::Byte(values)) => {
                            Ok(Cow::Owned(collect_try_convert::<$element, _, _>(
                                values.iter().copied(),
                                "u8",
                            )?))
                        }
                        InMemoryFieldState::LinearList(LinearListState::UInt(values)) => {
                            Ok(Cow::Owned(collect_try_convert::<$element, _, _>(
                                values.iter().copied(),
                                "u64",
                            )?))
                        }
                        InMemoryFieldState::LinearList(LinearListState::Int(values)) => {
                            Ok(Cow::Owned(collect_try_convert::<$element, _, _>(
                                values.iter().copied(),
                                "i64",
                            )?))
                        }
                        InMemoryFieldState::LinearList(LinearListState::Timestamp(values)) => {
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
            fn decode(
                value: &InMemoryFieldState<OperationId>,
            ) -> Result<Cow<'_, Self>, DecodeValueError> {
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
            fn decode(
                value: &InMemoryFieldState<OperationId>,
            ) -> Result<Cow<'_, Self>, DecodeValueError> {
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
    fn decode(value: &InMemoryFieldState<OperationId>) -> Result<Cow<'_, Self>, DecodeValueError> {
        match extract_string_value_ref(value) {
            Ok(value) => Ok(Cow::Borrowed(value)),
            Err(ExtractFieldError::Null) => Err(null_decode_error::<Self>()),
            Err(ExtractFieldError::TypeMismatch) => match value {
                InMemoryFieldState::LinearString(value) => {
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
    fn decode(value: &InMemoryFieldState<OperationId>) -> Result<Cow<'_, Self>, DecodeValueError> {
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
    fn decode(value: &InMemoryFieldState<OperationId>) -> Result<Cow<'_, Self>, DecodeValueError> {
        let value = extract_float_value_ref(value)
            .map_err(|err| map_extract_error::<Self, _>(value, err))?;
        Ok(Cow::Owned(value.into_inner()))
    }
}

impl<OperationId> Decode<OperationId> for f32
where
    OperationId: DecodeOperationIdBounds,
{
    fn decode(value: &InMemoryFieldState<OperationId>) -> Result<Cow<'_, Self>, DecodeValueError> {
        let value = extract_float_value_ref(value)
            .map_err(|err| map_extract_error::<Self, _>(value, err))?;
        Ok(Cow::Owned(narrow_f64_to_f32(*value)))
    }
}

impl<OperationId> Decode<OperationId> for char
where
    OperationId: DecodeOperationIdBounds,
{
    fn decode(value: &InMemoryFieldState<OperationId>) -> Result<Cow<'_, Self>, DecodeValueError> {
        match extract_string_value_ref(value) {
            Ok(value) => Ok(Cow::Owned(decode_single_char::<Self>(value.as_str())?)),
            Err(ExtractFieldError::Null) => Err(null_decode_error::<Self>()),
            Err(ExtractFieldError::TypeMismatch) => {
                if let InMemoryFieldState::LinearString(value) = value {
                    let rendered: String = value.iter_values().collect();
                    Ok(Cow::Owned(decode_single_char::<Self>(&rendered)?))
                } else {
                    let code_point = convert_integer_value::<u32>(
                        extract_integer_value_ref(value)
                            .map_err(|err| map_extract_error::<Self, _>(value, err))?,
                    )?;
                    Ok(Cow::Owned(try_decode_char_code::<Self>(code_point)?))
                }
            }
        }
    }
}

impl<OperationId> Decode<OperationId> for Vec<u8>
where
    OperationId: DecodeOperationIdBounds,
{
    fn decode(value: &InMemoryFieldState<OperationId>) -> Result<Cow<'_, Self>, DecodeValueError> {
        match extract_binary_value_ref(value) {
            Ok(value) => Ok(Cow::Borrowed(value)),
            Err(ExtractFieldError::Null) => Err(null_decode_error::<Self>()),
            Err(ExtractFieldError::TypeMismatch) => match extract_integer_vec_ref(value) {
                Ok(IntegerVecRef::Byte(values)) => Ok(Cow::Borrowed(values)),
                Ok(values) => Ok(Cow::Owned(convert_integer_vec::<u8>(values)?)),
                Err(ExtractFieldError::Null) => Err(null_decode_error::<Self>()),
                Err(ExtractFieldError::TypeMismatch) => match value {
                    InMemoryFieldState::LinearList(LinearListState::Byte(values)) => {
                        Ok(Cow::Owned(values.iter().copied().collect()))
                    }
                    InMemoryFieldState::LinearList(LinearListState::UInt(values)) => {
                        Ok(Cow::Owned(collect_try_convert::<u8, _, _>(
                            values.iter().copied(),
                            "u64",
                        )?))
                    }
                    InMemoryFieldState::LinearList(LinearListState::Int(values)) => Ok(Cow::Owned(
                        collect_try_convert::<u8, _, _>(values.iter().copied(), "i64")?,
                    )),
                    InMemoryFieldState::LinearList(LinearListState::Timestamp(values)) => {
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
    fn decode(value: &InMemoryFieldState<OperationId>) -> Result<Cow<'_, Self>, DecodeValueError> {
        match extract_float_vec_ref(value) {
            Ok(values) => Ok(Cow::Owned(
                values.iter().map(|value| value.into_inner()).collect(),
            )),
            Err(ExtractFieldError::Null) => Err(null_decode_error::<Self>()),
            Err(ExtractFieldError::TypeMismatch) => match value {
                InMemoryFieldState::LinearList(LinearListState::Float(values)) => Ok(Cow::Owned(
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
    fn decode(value: &InMemoryFieldState<OperationId>) -> Result<Cow<'_, Self>, DecodeValueError> {
        match extract_float_vec_ref(value) {
            Ok(values) => Ok(Cow::Owned(
                values
                    .iter()
                    .map(|value| narrow_f64_to_f32(*value))
                    .collect(),
            )),
            Err(ExtractFieldError::Null) => Err(null_decode_error::<Self>()),
            Err(ExtractFieldError::TypeMismatch) => match value {
                InMemoryFieldState::LinearList(LinearListState::Float(values)) => Ok(Cow::Owned(
                    values
                        .iter()
                        .map(|value| narrow_f64_to_f32(*value))
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
    fn decode(value: &InMemoryFieldState<OperationId>) -> Result<Cow<'_, Self>, DecodeValueError> {
        match extract_string_value_ref(value) {
            Ok(value) => Ok(Cow::Owned(value.chars().collect())),
            Err(ExtractFieldError::Null) => Err(null_decode_error::<Self>()),
            Err(ExtractFieldError::TypeMismatch) => match value {
                InMemoryFieldState::LinearString(value) => Ok(Cow::Owned(
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
        IdWithIndex,
        RowOperations,
        any_data::{LinearLatestValueWins, list::LinearList},
        schema::{
            BasicDataType,
            Direction,
            Field,
            NullableBasicDataType,
            PrimitiveType,
            ReplicatedDataType,
            Schema,
            datamodel::{InMemoryStateData, InMemoryStateDataRow},
        },
        text::LinearString,
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

    fn make_row(
        data: &InMemoryStateData<(), OperationId>,
    ) -> InMemoryStateDataRow<'_, (), OperationId> {
        InMemoryStateDataRow { data, row_index: 0 }
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
        let mut data = InMemoryStateData::with_owned_schema(schema);
        data.push_row_from_named_fields([(
            "title",
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::String(
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
        let mut data = InMemoryStateData::with_owned_schema(schema);
        data.push_row_from_named_fields([(
            "title",
            InMemoryFieldState::LinearString(LinearString::with_value("hej".to_owned(), 1)),
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
        let mut data = InMemoryStateData::with_owned_schema(schema);
        data.push_row_from_named_fields([
            (
                "count",
                InMemoryFieldState::TotalOrderRegister(PrimitiveValue::UInt(42)),
            ),
            (
                "numbers",
                InMemoryFieldState::LinearList(LinearListState::UInt(LinearList::with_values(
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
        let mut data = InMemoryStateData::with_owned_schema(schema);
        data.push_row_from_named_fields([(
            "maybe_count",
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableUInt(
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
