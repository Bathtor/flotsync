use super::{
    Field,
    NullableBasicDataType,
    NullablePrimitiveType,
    PrimitiveType,
    ReplicatedDataType,
    datamodel::{BasicValue, CounterValue, NullableBasicValue},
    values::{NullablePrimitiveValue, PrimitiveValue, PrimitiveValueArray},
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
    pub fn with_default<V>(mut self, value: V) -> Result<Self, FieldValueBuildError>
    where
        V: Into<NullableBasicValue>,
    {
        let value = value.into();
        self.normalize_field_value(value.clone())?;
        self.default_value = Some(value);
        Ok(self)
    }

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

    pub fn initial<'a, V>(&'a self, value: V) -> Result<InitialFieldValue<'a>, FieldValueBuildError>
    where
        V: Into<NullableBasicValue>,
    {
        Ok(InitialFieldValue {
            field_name: Cow::Borrowed(self.name.as_str()),
            expected_type: &self.data_type,
            value: self.normalize_field_value(value.into())?,
        })
    }

    pub fn set<'a, V>(&'a self, value: V) -> Result<PendingFieldUpdate<'a>, FieldValueBuildError>
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
}

fn validate_nullable_basic_value(
    field_name: &str,
    value_type: &NullableBasicDataType,
    value: &NullableBasicValue,
) -> Result<(), FieldValueBuildError> {
    if value.matches_type(value_type) {
        return Ok(());
    }

    if matches!(value, NullableBasicValue::Null) {
        return NullNotAllowedSnafu {
            field_name: field_name.to_owned(),
        }
        .fail();
    }

    TypeMismatchSnafu {
        field_name: field_name.to_owned(),
        expected: Cow::Owned(nullable_basic_data_type_name(value_type)),
        actual: Cow::Owned(nullable_basic_value_name(value)),
    }
    .fail()
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
            actual: Cow::Owned(primitive_value_name(&actual)),
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
                    actual: Cow::Owned(basic_value_name(&value)),
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
            expected: Cow::Owned(format!("a {} array", primitive_type_name(value_type))),
            actual: Cow::Owned(basic_value_name(&actual)),
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
            actual: Cow::Owned(basic_value_name(&actual)),
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

fn mismatch<T>(
    field_name: &str,
    expected: impl Into<Cow<'static, str>>,
    actual: &PrimitiveValue,
) -> Result<T, FieldValueBuildError> {
    TypeMismatchSnafu {
        field_name: field_name.to_owned(),
        expected: expected.into(),
        actual: Cow::Owned(primitive_value_name(actual)),
    }
    .fail()
}

fn array_mismatch<T>(
    field_name: &str,
    value_type: PrimitiveType,
    actual: &PrimitiveValueArray,
) -> Result<T, FieldValueBuildError> {
    TypeMismatchSnafu {
        field_name: field_name.to_owned(),
        expected: Cow::Owned(format!("a {} array", primitive_type_name(value_type))),
        actual: Cow::Owned(primitive_array_value_name(actual)),
    }
    .fail()
}

fn primitive_type_name(value_type: PrimitiveType) -> String {
    match value_type {
        PrimitiveType::String => "string".to_owned(),
        PrimitiveType::UInt => "u64".to_owned(),
        PrimitiveType::Int => "i64".to_owned(),
        PrimitiveType::Byte => "u8".to_owned(),
        PrimitiveType::Float => "f64".to_owned(),
        PrimitiveType::Boolean => "bool".to_owned(),
        PrimitiveType::Binary => "binary".to_owned(),
        PrimitiveType::Date => "date".to_owned(),
        PrimitiveType::Timestamp => "timestamp".to_owned(),
    }
}

fn nullable_basic_data_type_name(value_type: &NullableBasicDataType) -> String {
    if value_type.is_nullable() {
        format!("nullable {}", basic_data_type_name(value_type.value_type()))
    } else {
        basic_data_type_name(value_type.value_type())
    }
}

fn basic_data_type_name(value_type: &super::BasicDataType) -> String {
    match value_type {
        super::BasicDataType::Primitive(value_type) => primitive_type_name(*value_type),
        super::BasicDataType::Array(value_type) => {
            format!("{} array", primitive_type_name(value_type.element_type))
        }
    }
}

fn nullable_primitive_type_name(value_type: NullablePrimitiveType) -> String {
    if value_type.is_nullable() {
        format!("nullable {}", primitive_type_name(value_type.value_type()))
    } else {
        primitive_type_name(value_type.value_type())
    }
}

fn nullable_basic_value_name(value: &NullableBasicValue) -> String {
    match value {
        NullableBasicValue::Null => "null".to_owned(),
        NullableBasicValue::Value(value) => basic_value_name(value),
    }
}

fn basic_value_name(value: &BasicValue) -> String {
    match value {
        BasicValue::Primitive(value) => primitive_value_name(value),
        BasicValue::Array(value) => primitive_array_value_name(value),
    }
}

fn primitive_value_name(value: &PrimitiveValue) -> String {
    match value {
        PrimitiveValue::String(_) => "string".to_owned(),
        PrimitiveValue::UInt(_) => "u64".to_owned(),
        PrimitiveValue::Int(_) => "i64".to_owned(),
        PrimitiveValue::Byte(_) => "u8".to_owned(),
        PrimitiveValue::Float(_) => "f64".to_owned(),
        PrimitiveValue::Boolean(_) => "bool".to_owned(),
        PrimitiveValue::Binary(_) => "binary".to_owned(),
        PrimitiveValue::Date(_) => "date".to_owned(),
        PrimitiveValue::Timestamp(_) => "timestamp".to_owned(),
    }
}

fn primitive_array_value_name(value: &PrimitiveValueArray) -> String {
    match value {
        PrimitiveValueArray::String(_) => "string array".to_owned(),
        PrimitiveValueArray::UInt(_) => "u64 array".to_owned(),
        PrimitiveValueArray::Int(_) => "i64 array".to_owned(),
        PrimitiveValueArray::Byte(_) => "u8 array".to_owned(),
        PrimitiveValueArray::Float(_) => "f64 array".to_owned(),
        PrimitiveValueArray::Boolean(_) => "bool array".to_owned(),
        PrimitiveValueArray::Binary(_) => "binary array".to_owned(),
        PrimitiveValueArray::Date(_) => "date array".to_owned(),
        PrimitiveValueArray::Timestamp(_) => "timestamp array".to_owned(),
    }
}

impl From<OrderedFloat<f64>> for PrimitiveValue {
    fn from(value: OrderedFloat<f64>) -> Self {
        Self::Float(value)
    }
}
