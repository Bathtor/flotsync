use super::{
    ArrayType,
    Field,
    NullableBasicDataType,
    NullablePrimitiveType,
    PrimitiveType,
    ReplicatedDataType,
    datamodel::{
        BasicValue,
        BasicValueRef,
        CounterValue,
        NullableBasicValue,
        NullableBasicValueRef,
        PrimitiveValueArrayRef,
    },
    values::{NullablePrimitiveValue, PrimitiveValue, PrimitiveValueArray, PrimitiveValueRef},
};
use ordered_float::OrderedFloat;
use snafu::prelude::*;
use std::{borrow::Cow, convert::TryInto};

#[derive(Clone, Debug, PartialEq)]
pub struct InitialFieldValue<'a> {
    field_name: Cow<'a, str>,
    expected_type: &'a ReplicatedDataType,
    value: FieldTargetValue,
}

impl InitialFieldValue<'_> {
    pub(crate) fn field_name(&self) -> &str {
        self.field_name.as_ref()
    }

    pub(crate) fn expected_type(&self) -> &ReplicatedDataType {
        self.expected_type
    }

    pub(crate) fn value(&self) -> &FieldTargetValue {
        &self.value
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PendingFieldUpdate<'a> {
    field_name: Cow<'a, str>,
    expected_type: &'a ReplicatedDataType,
    value: FieldTargetValue,
}

impl PendingFieldUpdate<'_> {
    pub(crate) fn field_name(&self) -> &str {
        self.field_name.as_ref()
    }

    pub(crate) fn expected_type(&self) -> &ReplicatedDataType {
        self.expected_type
    }

    pub(crate) fn value(&self) -> &FieldTargetValue {
        &self.value
    }
}

#[derive(Clone, Debug, PartialEq, Snafu)]
pub enum FieldValueBuildError {
    #[snafu(display("Field '{field_name}' does not accept NULL values."))]
    NullNotAllowed { field_name: String },
    #[snafu(display("Field '{field_name}' expected {expected} but received {actual}."))]
    TypeMismatch {
        field_name: String,
        expected: Cow<'static, str>,
        actual: Cow<'static, str>,
    },
    #[snafu(display("Field '{field_name}' could not convert value {value} to {expected}."))]
    ConversionFailed {
        field_name: String,
        expected: Cow<'static, str>,
        value: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum FieldTargetValue {
    NullableBasic(NullableBasicValue),
    String(String),
    PrimitiveArray(PrimitiveValueArray),
    Counter(CounterValue),
    Primitive(PrimitiveValue),
    NullablePrimitive(NullablePrimitiveValue),
}

impl Field {
    /// # Errors
    ///
    /// See `FieldValueBuildError` for failure conditions.
    pub fn with_default<V>(mut self, value: V) -> Result<Self, FieldValueBuildError>
    where
        V: Into<NullableBasicValue>,
    {
        let value = value.into();
        self.normalize_field_value(value.clone())?;
        self.default_value = Some(value);
        Ok(self)
    }

    #[must_use]
    pub fn without_default(mut self) -> Self {
        self.default_value = None;
        self
    }

    pub(crate) fn default_target_value(
        &self,
    ) -> Result<Option<FieldTargetValue>, FieldValueBuildError> {
        let Some(value) = self.default_value.as_ref() else {
            return Ok(None);
        };
        let target_value = self.normalize_field_value(value.clone())?;
        Ok(Some(target_value))
    }

    /// # Errors
    ///
    /// See `FieldValueBuildError` for failure conditions.
    pub fn initial<V>(&self, value: V) -> Result<InitialFieldValue<'_>, FieldValueBuildError>
    where
        V: Into<NullableBasicValue>,
    {
        Ok(InitialFieldValue {
            field_name: Cow::Borrowed(self.name.as_str()),
            expected_type: &self.data_type,
            value: self.normalize_field_value(value.into())?,
        })
    }

    /// Check whether `value` can initialise this field without building the
    /// owned initial field value.
    ///
    /// # Errors
    ///
    /// See `FieldValueBuildError` for failure conditions.
    pub fn can_initial(&self, value: &NullableBasicValue) -> Result<(), FieldValueBuildError> {
        self.validate_field_value(value.as_ref())
    }

    /// # Errors
    ///
    /// See `FieldValueBuildError` for failure conditions.
    pub fn set<V>(&self, value: V) -> Result<PendingFieldUpdate<'_>, FieldValueBuildError>
    where
        V: Into<NullableBasicValue>,
    {
        Ok(PendingFieldUpdate {
            field_name: Cow::Borrowed(self.name.as_str()),
            expected_type: &self.data_type,
            value: self.normalize_field_value(value.into())?,
        })
    }

    fn normalize_field_value(
        &self,
        value: NullableBasicValue,
    ) -> Result<FieldTargetValue, FieldValueBuildError> {
        match &self.data_type {
            ReplicatedDataType::LatestValueWins { value_type } => {
                validate_nullable_basic_value(&self.name, value_type, &value)?;
                Ok(FieldTargetValue::NullableBasic(value))
            }
            ReplicatedDataType::LinearString => Ok(FieldTargetValue::String(normalize_string(
                &self.name, value,
            )?)),
            ReplicatedDataType::LinearList { value_type } => Ok(FieldTargetValue::PrimitiveArray(
                normalize_primitive_array(&self.name, *value_type, value)?,
            )),
            ReplicatedDataType::MonotonicCounter { small_range } => Ok(FieldTargetValue::Counter(
                normalize_counter(&self.name, *small_range, value)?,
            )),
            ReplicatedDataType::TotalOrderRegister { value_type, .. } => Ok(
                FieldTargetValue::Primitive(normalize_primitive(&self.name, *value_type, value)?),
            ),
            ReplicatedDataType::TotalOrderFiniteStateRegister { value_type, .. } => {
                Ok(FieldTargetValue::NullablePrimitive(
                    normalize_nullable_primitive(&self.name, *value_type, value)?,
                ))
            }
        }
    }

    /// Validate that a projected value can initialise the field without
    /// normalising the value or building CRDT state.
    ///
    /// This checks nullability, replicated data-type shape, primitive or array
    /// element type compatibility, integer range compatibility, and
    /// finite-state register value compatibility.
    fn validate_field_value(
        &self,
        value: NullableBasicValueRef<'_>,
    ) -> Result<(), FieldValueBuildError> {
        match &self.data_type {
            ReplicatedDataType::LatestValueWins { value_type } => {
                validate_nullable_basic_value_ref(&self.name, value_type, &value)
            }
            ReplicatedDataType::LinearString => {
                let primitive = validate_non_null_primitive(&self.name, value, "a string value")?;
                match primitive {
                    PrimitiveValueRef::String(_) => Ok(()),
                    actual => {
                        primitive_ref_mismatch(&self.name, Cow::Borrowed("a string value"), &actual)
                    }
                }
            }
            ReplicatedDataType::LinearList { value_type } => {
                validate_primitive_array(&self.name, *value_type, value)
            }
            ReplicatedDataType::MonotonicCounter { small_range } => {
                let primitive = validate_non_null_primitive(&self.name, value, "a counter value")?;
                if *small_range {
                    validate_primitive_integer::<u8>(&self.name, &primitive, "u8 counter value")
                } else {
                    validate_primitive_integer::<u64>(&self.name, &primitive, "u64 counter value")
                }
            }
            ReplicatedDataType::TotalOrderRegister { value_type, .. } => {
                let primitive = validate_non_null_primitive(
                    &self.name,
                    value,
                    primitive_type_name(*value_type).as_str(),
                )?;
                validate_primitive_value(&self.name, *value_type, &primitive)
            }
            ReplicatedDataType::TotalOrderFiniteStateRegister { value_type, .. } => {
                validate_nullable_primitive(&self.name, *value_type, value)
            }
        }
    }
}

fn validate_nullable_basic_value(
    field_name: &str,
    value_type: &NullableBasicDataType,
    value: &NullableBasicValue,
) -> Result<(), FieldValueBuildError> {
    validate_nullable_basic_value_ref(field_name, value_type, &value.as_ref())
}

fn validate_nullable_basic_value_ref(
    field_name: &str,
    value_type: &NullableBasicDataType,
    value: &NullableBasicValueRef<'_>,
) -> Result<(), FieldValueBuildError> {
    if value.matches_type(value_type) {
        return Ok(());
    }

    if matches!(value, NullableBasicValueRef::Null) {
        return NullNotAllowedSnafu {
            field_name: field_name.to_owned(),
        }
        .fail();
    }

    TypeMismatchSnafu {
        field_name: field_name.to_owned(),
        expected: Cow::Owned(nullable_basic_data_type_name(value_type)),
        actual: Cow::Owned(value.value_type().to_string()),
    }
    .fail()
}

fn validate_non_null_primitive<'a>(
    field_name: &str,
    value: NullableBasicValueRef<'a>,
    expected: &str,
) -> Result<PrimitiveValueRef<'a>, FieldValueBuildError> {
    match value {
        NullableBasicValueRef::Null => NullNotAllowedSnafu {
            field_name: field_name.to_owned(),
        }
        .fail(),
        NullableBasicValueRef::Value(BasicValueRef::Primitive(value)) => Ok(value),
        NullableBasicValueRef::Value(actual) => TypeMismatchSnafu {
            field_name: field_name.to_owned(),
            expected: Cow::Owned(expected.to_owned()),
            actual: Cow::Owned(actual.value_type().to_string()),
        }
        .fail(),
    }
}

fn validate_nullable_primitive(
    field_name: &str,
    value_type: NullablePrimitiveType,
    value: NullableBasicValueRef<'_>,
) -> Result<(), FieldValueBuildError> {
    match value {
        NullableBasicValueRef::Null => {
            ensure!(
                value_type.is_nullable(),
                NullNotAllowedSnafu {
                    field_name: field_name.to_owned(),
                }
            );
            Ok(())
        }
        NullableBasicValueRef::Value(BasicValueRef::Primitive(primitive)) => {
            validate_primitive_value(field_name, value_type.value_type(), &primitive)
        }
        NullableBasicValueRef::Value(actual) => TypeMismatchSnafu {
            field_name: field_name.to_owned(),
            expected: Cow::Owned(nullable_primitive_type_name(value_type)),
            actual: Cow::Owned(actual.value_type().to_string()),
        }
        .fail(),
    }
}

fn validate_primitive_array(
    field_name: &str,
    value_type: PrimitiveType,
    value: NullableBasicValueRef<'_>,
) -> Result<(), FieldValueBuildError> {
    match value {
        NullableBasicValueRef::Null => NullNotAllowedSnafu {
            field_name: field_name.to_owned(),
        }
        .fail(),
        NullableBasicValueRef::Value(BasicValueRef::Array(value)) => {
            validate_primitive_array_value(field_name, value_type, &value)
        }
        NullableBasicValueRef::Value(BasicValueRef::Primitive(PrimitiveValueRef::Binary(_)))
            if value_type == PrimitiveType::Byte =>
        {
            Ok(())
        }
        NullableBasicValueRef::Value(actual) => TypeMismatchSnafu {
            field_name: field_name.to_owned(),
            expected: Cow::Owned(array_type_name(value_type)),
            actual: Cow::Owned(actual.value_type().to_string()),
        }
        .fail(),
    }
}

fn validate_primitive_value(
    field_name: &str,
    value_type: PrimitiveType,
    value: &PrimitiveValueRef<'_>,
) -> Result<(), FieldValueBuildError> {
    match value_type {
        PrimitiveType::String => match value {
            PrimitiveValueRef::String(_) => Ok(()),
            actual => primitive_ref_mismatch(field_name, primitive_type_name(value_type), actual),
        },
        PrimitiveType::UInt => validate_primitive_integer::<u64>(
            field_name,
            value,
            primitive_type_name(value_type).as_str(),
        ),
        PrimitiveType::Int | PrimitiveType::Timestamp => validate_primitive_integer::<i64>(
            field_name,
            value,
            primitive_type_name(value_type).as_str(),
        ),
        PrimitiveType::Byte => validate_primitive_integer::<u8>(
            field_name,
            value,
            primitive_type_name(value_type).as_str(),
        ),
        PrimitiveType::Float => match value {
            PrimitiveValueRef::Float(_) => Ok(()),
            actual => primitive_ref_mismatch(field_name, primitive_type_name(value_type), actual),
        },
        PrimitiveType::Boolean => match value {
            PrimitiveValueRef::Boolean(_) => Ok(()),
            actual => primitive_ref_mismatch(field_name, primitive_type_name(value_type), actual),
        },
        PrimitiveType::Binary => match value {
            PrimitiveValueRef::Binary(_) => Ok(()),
            actual => primitive_ref_mismatch(field_name, primitive_type_name(value_type), actual),
        },
        PrimitiveType::Date => match value {
            PrimitiveValueRef::Date(_) => Ok(()),
            actual => primitive_ref_mismatch(field_name, primitive_type_name(value_type), actual),
        },
    }
}

fn validate_primitive_array_value(
    field_name: &str,
    value_type: PrimitiveType,
    value: &PrimitiveValueArrayRef<'_>,
) -> Result<(), FieldValueBuildError> {
    match value_type {
        PrimitiveType::String => match value {
            PrimitiveValueArrayRef::String(_) => Ok(()),
            actual => array_ref_mismatch(field_name, value_type, actual),
        },
        PrimitiveType::UInt => validate_integer_array::<u64>(field_name, value_type, value),
        PrimitiveType::Int | PrimitiveType::Timestamp => {
            validate_integer_array::<i64>(field_name, value_type, value)
        }
        PrimitiveType::Byte => validate_integer_array::<u8>(field_name, value_type, value),
        PrimitiveType::Float => match value {
            PrimitiveValueArrayRef::Float(_) => Ok(()),
            actual => array_ref_mismatch(field_name, value_type, actual),
        },
        PrimitiveType::Boolean => match value {
            PrimitiveValueArrayRef::Boolean(_) => Ok(()),
            actual => array_ref_mismatch(field_name, value_type, actual),
        },
        PrimitiveType::Binary => match value {
            PrimitiveValueArrayRef::Binary(_) => Ok(()),
            actual => array_ref_mismatch(field_name, value_type, actual),
        },
        PrimitiveType::Date => match value {
            PrimitiveValueArrayRef::Date(_) => Ok(()),
            actual => array_ref_mismatch(field_name, value_type, actual),
        },
    }
}

fn normalize_string(
    field_name: &str,
    value: NullableBasicValue,
) -> Result<String, FieldValueBuildError> {
    let primitive = normalize_non_null_primitive(field_name, value, "a string value")?;
    match primitive {
        PrimitiveValue::String(value) => Ok(value),
        actual => TypeMismatchSnafu {
            field_name: field_name.to_owned(),
            expected: Cow::Borrowed("a string value"),
            actual: Cow::Owned(actual.value_type().to_string()),
        }
        .fail(),
    }
}

fn normalize_counter(
    field_name: &str,
    small_range: bool,
    value: NullableBasicValue,
) -> Result<CounterValue, FieldValueBuildError> {
    let primitive = normalize_non_null_primitive(field_name, value, "a counter value")?;
    if small_range {
        let value = convert_primitive_integer::<u8>(field_name, &primitive, "u8 counter value")?;
        Ok(CounterValue::Byte(value))
    } else {
        let value = convert_primitive_integer::<u64>(field_name, &primitive, "u64 counter value")?;
        Ok(CounterValue::UInt(value))
    }
}

fn normalize_primitive(
    field_name: &str,
    value_type: PrimitiveType,
    value: NullableBasicValue,
) -> Result<PrimitiveValue, FieldValueBuildError> {
    let primitive =
        normalize_non_null_primitive(field_name, value, primitive_type_name(value_type).as_str())?;
    convert_primitive_value(field_name, value_type, primitive)
}

fn normalize_nullable_primitive(
    field_name: &str,
    value_type: NullablePrimitiveType,
    value: NullableBasicValue,
) -> Result<NullablePrimitiveValue, FieldValueBuildError> {
    match value {
        NullableBasicValue::Null => {
            ensure!(
                value_type.is_nullable(),
                NullNotAllowedSnafu {
                    field_name: field_name.to_owned(),
                }
            );
            Ok(NullablePrimitiveValue::Null)
        }
        NullableBasicValue::Value(value) => {
            let BasicValue::Primitive(primitive) = value else {
                return TypeMismatchSnafu {
                    field_name: field_name.to_owned(),
                    expected: Cow::Owned(nullable_primitive_type_name(value_type)),
                    actual: Cow::Owned(value.as_ref().value_type().to_string()),
                }
                .fail();
            };
            Ok(NullablePrimitiveValue::Value(convert_primitive_value(
                field_name,
                value_type.value_type(),
                primitive,
            )?))
        }
    }
}

fn normalize_primitive_array(
    field_name: &str,
    value_type: PrimitiveType,
    value: NullableBasicValue,
) -> Result<PrimitiveValueArray, FieldValueBuildError> {
    match value {
        NullableBasicValue::Null => NullNotAllowedSnafu {
            field_name: field_name.to_owned(),
        }
        .fail(),
        NullableBasicValue::Value(BasicValue::Array(value)) => {
            convert_primitive_array_value(field_name, value_type, value)
        }
        NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::Binary(value)))
            if value_type == PrimitiveType::Byte =>
        {
            Ok(PrimitiveValueArray::Byte(value))
        }
        NullableBasicValue::Value(actual) => TypeMismatchSnafu {
            field_name: field_name.to_owned(),
            expected: Cow::Owned(array_type_name(value_type)),
            actual: Cow::Owned(actual.as_ref().value_type().to_string()),
        }
        .fail(),
    }
}

fn normalize_non_null_primitive(
    field_name: &str,
    value: NullableBasicValue,
    expected: &str,
) -> Result<PrimitiveValue, FieldValueBuildError> {
    match value {
        NullableBasicValue::Null => NullNotAllowedSnafu {
            field_name: field_name.to_owned(),
        }
        .fail(),
        NullableBasicValue::Value(BasicValue::Primitive(value)) => Ok(value),
        NullableBasicValue::Value(actual) => TypeMismatchSnafu {
            field_name: field_name.to_owned(),
            expected: Cow::Owned(expected.to_owned()),
            actual: Cow::Owned(actual.as_ref().value_type().to_string()),
        }
        .fail(),
    }
}

fn convert_primitive_value(
    field_name: &str,
    value_type: PrimitiveType,
    value: PrimitiveValue,
) -> Result<PrimitiveValue, FieldValueBuildError> {
    match value_type {
        PrimitiveType::String => match value {
            PrimitiveValue::String(value) => Ok(PrimitiveValue::String(value)),
            actual => mismatch(field_name, primitive_type_name(value_type), &actual),
        },
        PrimitiveType::UInt => Ok(PrimitiveValue::UInt(convert_primitive_integer::<u64>(
            field_name,
            &value,
            primitive_type_name(value_type).as_str(),
        )?)),
        PrimitiveType::Int => Ok(PrimitiveValue::Int(convert_primitive_integer::<i64>(
            field_name,
            &value,
            primitive_type_name(value_type).as_str(),
        )?)),
        PrimitiveType::Byte => Ok(PrimitiveValue::Byte(convert_primitive_integer::<u8>(
            field_name,
            &value,
            primitive_type_name(value_type).as_str(),
        )?)),
        PrimitiveType::Float => match value {
            PrimitiveValue::Float(value) => Ok(PrimitiveValue::Float(value)),
            actual => mismatch(field_name, primitive_type_name(value_type), &actual),
        },
        PrimitiveType::Boolean => match value {
            PrimitiveValue::Boolean(value) => Ok(PrimitiveValue::Boolean(value)),
            actual => mismatch(field_name, primitive_type_name(value_type), &actual),
        },
        PrimitiveType::Binary => match value {
            PrimitiveValue::Binary(value) => Ok(PrimitiveValue::Binary(value)),
            actual => mismatch(field_name, primitive_type_name(value_type), &actual),
        },
        PrimitiveType::Date => match value {
            PrimitiveValue::Date(value) => Ok(PrimitiveValue::Date(value)),
            actual => mismatch(field_name, primitive_type_name(value_type), &actual),
        },
        PrimitiveType::Timestamp => {
            Ok(PrimitiveValue::Timestamp(convert_primitive_integer::<i64>(
                field_name,
                &value,
                primitive_type_name(value_type).as_str(),
            )?))
        }
    }
}

fn convert_primitive_array_value(
    field_name: &str,
    value_type: PrimitiveType,
    value: PrimitiveValueArray,
) -> Result<PrimitiveValueArray, FieldValueBuildError> {
    match value_type {
        PrimitiveType::String => match value {
            PrimitiveValueArray::String(value) => Ok(PrimitiveValueArray::String(value)),
            actual => array_mismatch(field_name, value_type, &actual),
        },
        PrimitiveType::UInt => convert_integer_array::<u64, _>(
            field_name,
            value_type,
            value,
            PrimitiveValueArray::UInt,
        ),
        PrimitiveType::Int => {
            convert_integer_array::<i64, _>(field_name, value_type, value, PrimitiveValueArray::Int)
        }
        PrimitiveType::Byte => {
            convert_integer_array::<u8, _>(field_name, value_type, value, PrimitiveValueArray::Byte)
        }
        PrimitiveType::Float => match value {
            PrimitiveValueArray::Float(value) => Ok(PrimitiveValueArray::Float(value)),
            actual => array_mismatch(field_name, value_type, &actual),
        },
        PrimitiveType::Boolean => match value {
            PrimitiveValueArray::Boolean(value) => Ok(PrimitiveValueArray::Boolean(value)),
            actual => array_mismatch(field_name, value_type, &actual),
        },
        PrimitiveType::Binary => match value {
            PrimitiveValueArray::Binary(value) => Ok(PrimitiveValueArray::Binary(value)),
            actual => array_mismatch(field_name, value_type, &actual),
        },
        PrimitiveType::Date => match value {
            PrimitiveValueArray::Date(value) => Ok(PrimitiveValueArray::Date(value)),
            actual => array_mismatch(field_name, value_type, &actual),
        },
        PrimitiveType::Timestamp => convert_integer_array::<i64, _>(
            field_name,
            value_type,
            value,
            PrimitiveValueArray::Timestamp,
        ),
    }
}

fn convert_integer_array<T, F>(
    field_name: &str,
    value_type: PrimitiveType,
    value: PrimitiveValueArray,
    build: F,
) -> Result<PrimitiveValueArray, FieldValueBuildError>
where
    T: TryFrom<i64> + TryFrom<u64> + TryFrom<u8>,
    <T as TryFrom<i64>>::Error: std::fmt::Debug,
    <T as TryFrom<u64>>::Error: std::fmt::Debug,
    <T as TryFrom<u8>>::Error: std::fmt::Debug,
    F: Fn(Vec<T>) -> PrimitiveValueArray,
{
    let converted = match value {
        PrimitiveValueArray::Int(values) => values
            .into_iter()
            .map(|value| convert_integer(field_name, primitive_type_name(value_type), value))
            .collect::<Result<Vec<_>, _>>()?,
        PrimitiveValueArray::UInt(values) => values
            .into_iter()
            .map(|value| convert_integer(field_name, primitive_type_name(value_type), value))
            .collect::<Result<Vec<_>, _>>()?,
        PrimitiveValueArray::Byte(values) => values
            .into_iter()
            .map(|value| convert_integer(field_name, primitive_type_name(value_type), value))
            .collect::<Result<Vec<_>, _>>()?,
        actual => return array_mismatch(field_name, value_type, &actual),
    };
    Ok(build(converted))
}

fn validate_integer_array<T>(
    field_name: &str,
    value_type: PrimitiveType,
    value: &PrimitiveValueArrayRef<'_>,
) -> Result<(), FieldValueBuildError>
where
    T: TryFrom<i64> + TryFrom<u64> + TryFrom<u8>,
    <T as TryFrom<i64>>::Error: std::fmt::Debug,
    <T as TryFrom<u64>>::Error: std::fmt::Debug,
    <T as TryFrom<u8>>::Error: std::fmt::Debug,
{
    let expected = primitive_type_name(value_type);
    match value {
        PrimitiveValueArrayRef::Int(values) => values
            .iter()
            .try_for_each(|value| validate_integer::<T, _>(field_name, expected.clone(), *value)),
        PrimitiveValueArrayRef::UInt(values) => values
            .iter()
            .try_for_each(|value| validate_integer::<T, _>(field_name, expected.clone(), *value)),
        PrimitiveValueArrayRef::Byte(values) => values
            .iter()
            .try_for_each(|value| validate_integer::<T, _>(field_name, expected.clone(), *value)),
        actual => array_ref_mismatch(field_name, value_type, actual),
    }
}

fn convert_primitive_integer<T>(
    field_name: &str,
    value: &PrimitiveValue,
    expected: &str,
) -> Result<T, FieldValueBuildError>
where
    T: TryFrom<i64> + TryFrom<u64> + TryFrom<u8>,
    <T as TryFrom<i64>>::Error: std::fmt::Debug,
    <T as TryFrom<u64>>::Error: std::fmt::Debug,
    <T as TryFrom<u8>>::Error: std::fmt::Debug,
{
    #[allow(
        clippy::match_same_arms,
        reason = "Accepted integer variants have different payload types and share the same conversion path."
    )]
    match value {
        PrimitiveValue::Int(value) => {
            convert_integer(field_name, Cow::Owned(expected.to_owned()), *value)
        }
        PrimitiveValue::UInt(value) => {
            convert_integer(field_name, Cow::Owned(expected.to_owned()), *value)
        }
        PrimitiveValue::Byte(value) => {
            convert_integer(field_name, Cow::Owned(expected.to_owned()), *value)
        }
        PrimitiveValue::Timestamp(value) => {
            convert_integer(field_name, Cow::Owned(expected.to_owned()), *value)
        }
        actual => mismatch(field_name, Cow::Owned(expected.to_owned()), actual),
    }
}

fn validate_primitive_integer<T>(
    field_name: &str,
    value: &PrimitiveValueRef<'_>,
    expected: &str,
) -> Result<(), FieldValueBuildError>
where
    T: TryFrom<i64> + TryFrom<u64> + TryFrom<u8>,
    <T as TryFrom<i64>>::Error: std::fmt::Debug,
    <T as TryFrom<u64>>::Error: std::fmt::Debug,
    <T as TryFrom<u8>>::Error: std::fmt::Debug,
{
    #[allow(
        clippy::match_same_arms,
        reason = "Accepted integer variants have different payload types and share the same validation path."
    )]
    match value {
        PrimitiveValueRef::Int(value) => {
            validate_integer::<T, _>(field_name, Cow::Owned(expected.to_owned()), *value)
        }
        PrimitiveValueRef::UInt(value) => {
            validate_integer::<T, _>(field_name, Cow::Owned(expected.to_owned()), *value)
        }
        PrimitiveValueRef::Byte(value) => {
            validate_integer::<T, _>(field_name, Cow::Owned(expected.to_owned()), *value)
        }
        PrimitiveValueRef::Timestamp(value) => {
            validate_integer::<T, _>(field_name, Cow::Owned(expected.to_owned()), *value)
        }
        actual => primitive_ref_mismatch(field_name, Cow::Owned(expected.to_owned()), actual),
    }
}

fn convert_integer<T, Input>(
    field_name: &str,
    expected: impl Into<Cow<'static, str>>,
    value: Input,
) -> Result<T, FieldValueBuildError>
where
    T: TryFrom<Input>,
    Input: Copy + std::fmt::Display,
    <T as TryFrom<Input>>::Error: std::fmt::Debug,
{
    value
        .try_into()
        .map_err(|_| FieldValueBuildError::ConversionFailed {
            field_name: field_name.to_owned(),
            expected: expected.into(),
            value: value.to_string(),
        })
}

fn validate_integer<T, Input>(
    field_name: &str,
    expected: impl Into<Cow<'static, str>>,
    value: Input,
) -> Result<(), FieldValueBuildError>
where
    T: TryFrom<Input>,
    Input: Copy + std::fmt::Display,
    <T as TryFrom<Input>>::Error: std::fmt::Debug,
{
    T::try_from(value)
        .map(|_| ())
        .map_err(|_| FieldValueBuildError::ConversionFailed {
            field_name: field_name.to_owned(),
            expected: expected.into(),
            value: value.to_string(),
        })
}

fn mismatch<T>(
    field_name: &str,
    expected: impl Into<Cow<'static, str>>,
    actual: &PrimitiveValue,
) -> Result<T, FieldValueBuildError> {
    primitive_ref_mismatch(field_name, expected, &actual.as_ref())
}

fn primitive_ref_mismatch<T>(
    field_name: &str,
    expected: impl Into<Cow<'static, str>>,
    actual: &PrimitiveValueRef<'_>,
) -> Result<T, FieldValueBuildError> {
    TypeMismatchSnafu {
        field_name: field_name.to_owned(),
        expected: expected.into(),
        actual: Cow::Owned(actual.value_type().to_string()),
    }
    .fail()
}

fn array_mismatch<T>(
    field_name: &str,
    value_type: PrimitiveType,
    actual: &PrimitiveValueArray,
) -> Result<T, FieldValueBuildError> {
    array_ref_mismatch(field_name, value_type, &actual.as_ref())
}

fn array_ref_mismatch<T>(
    field_name: &str,
    value_type: PrimitiveType,
    actual: &PrimitiveValueArrayRef<'_>,
) -> Result<T, FieldValueBuildError> {
    TypeMismatchSnafu {
        field_name: field_name.to_owned(),
        expected: Cow::Owned(array_type_name(value_type)),
        actual: Cow::Owned(array_type_name(actual.primitive_type())),
    }
    .fail()
}

fn primitive_type_name(value_type: PrimitiveType) -> String {
    value_type.to_string()
}

fn nullable_basic_data_type_name(value_type: &NullableBasicDataType) -> String {
    value_type.to_string()
}

fn nullable_primitive_type_name(value_type: NullablePrimitiveType) -> String {
    value_type.to_string()
}

fn array_type_name(value_type: PrimitiveType) -> String {
    ArrayType {
        element_type: value_type,
    }
    .to_string()
}

impl From<OrderedFloat<f64>> for PrimitiveValue {
    fn from(value: OrderedFloat<f64>) -> Self {
        Self::Float(value)
    }
}
