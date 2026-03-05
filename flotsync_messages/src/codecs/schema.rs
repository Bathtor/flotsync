use crate::datamodel as proto;
use flotsync_data_types::{
    chrono::{Datelike, NaiveDate},
    schema::{
        ArrayType,
        BasicDataType,
        Direction,
        Field,
        NullableBasicDataType,
        NullablePrimitiveType,
        PrimitiveType,
        ReplicatedDataType,
        Schema,
        values::{NullablePrimitiveValueArray, PrimitiveValueArray},
    },
};
use ordered_float::OrderedFloat;
use protobuf::{EnumOrUnknown, MessageField};
use snafu::prelude::*;
use std::collections::HashMap;

type SchemaResult<T> = Result<T, SchemaCodecError>;

trait RequiredMessageFieldExt<T> {
    fn take_required(&mut self, message: &'static str, field: &'static str) -> SchemaResult<T>;
}
impl<T> RequiredMessageFieldExt<T> for MessageField<T> {
    fn take_required(&mut self, message: &'static str, field: &'static str) -> SchemaResult<T> {
        self.take().context(MissingFieldSnafu { message, field })
    }
}

trait RequiredOneofExt<T> {
    fn take_required_oneof(self, name: &'static str) -> SchemaResult<T>;
}
impl<T> RequiredOneofExt<T> for Option<T> {
    fn take_required_oneof(self, name: &'static str) -> SchemaResult<T> {
        self.context(MissingOneofSnafu { name })
    }
}

#[derive(Debug, Snafu)]
pub enum SchemaCodecError {
    #[snafu(display("Protobuf message '{message}' is missing required field '{field}'."))]
    MissingField {
        message: &'static str,
        field: &'static str,
    },
    #[snafu(display("Protobuf oneof '{name}' has no selected value."))]
    MissingOneof { name: &'static str },
    #[snafu(display("Unknown PrimitiveType enum value {value}."))]
    UnknownPrimitiveType { value: i32 },
    #[snafu(display("PrimitiveType must not be PRIMITIVE_TYPE_UNSPECIFIED."))]
    UnspecifiedPrimitiveType,
    #[snafu(display("Unknown Direction enum value {value}."))]
    UnknownDirection { value: i32 },
    #[snafu(display("Direction must not be DIRECTION_UNSPECIFIED."))]
    UnspecifiedDirection,
    #[snafu(display(
        "Unknown ReplicatedDataTypeKind enum value {value} in ReplicatedDataType.kind."
    ))]
    UnknownReplicatedDataTypeKind { value: i32 },
    #[snafu(display("ReplicatedDataType.kind must not be REPLICATED_DATA_TYPE_KIND_UNSPECIFIED."))]
    UnspecifiedReplicatedDataTypeKind,
    #[snafu(display("Schema definition contains duplicate field name '{field_name}'."))]
    DuplicateFieldName { field_name: String },
    #[snafu(display("ReplicatedDataType.kind={kind} is missing its required detail payload."))]
    MissingReplicatedDataTypeDetail { kind: &'static str },
    #[snafu(display("ReplicatedDataType.kind={kind} has unexpected detail payload '{detail}'."))]
    UnexpectedReplicatedDataTypeDetail {
        kind: &'static str,
        detail: &'static str,
    },
    #[snafu(display("ReplicatedDataType.kind={kind} requires total_order_direction."))]
    MissingTotalOrderDirection { kind: &'static str },
    #[snafu(display("ReplicatedDataType.kind={kind} must not set total_order_direction."))]
    UnexpectedTotalOrderDirection { kind: &'static str },
    #[snafu(display("ReplicatedDataType.kind={kind} requires finite_state_value_type."))]
    MissingFiniteStateValueType { kind: &'static str },
    #[snafu(display("ReplicatedDataType.kind={kind} must not set finite_state_value_type."))]
    UnexpectedFiniteStateValueType { kind: &'static str },
    #[snafu(display(
        "Finite-state register definitions must contain at least one non-null state."
    ))]
    EmptyFiniteStateStates,
    #[snafu(display(
        "Finite-state register null_index {null_index} must be <= value_count {value_count}."
    ))]
    InvalidFiniteStateNullIndex {
        null_index: usize,
        value_count: usize,
    },
    #[snafu(display(
        "Finite-state register null_index {null_index} does not fit into u32 for wire encoding."
    ))]
    NullIndexTooLargeForWire { null_index: usize },
    #[snafu(display(
        "Finite-state register value_type mismatch. Expected {expected}, got {actual}."
    ))]
    FiniteStateValueTypeMismatch {
        expected: NullablePrimitiveType,
        actual: NullablePrimitiveType,
    },
    #[snafu(display("Date {year:04}-{month:02}-{day:02} is not a valid calendar date."))]
    InvalidDate { year: i32, month: u32, day: u32 },
}

/// Encode a `Schema` into its protobuf schema transport form.
pub fn encode_schema_definition(schema: &Schema) -> SchemaResult<proto::SchemaDefinition> {
    let mut encoded = proto::SchemaDefinition::new();
    let mut fields = schema.columns.values().collect::<Vec<_>>();
    fields.sort_unstable_by(|left, right| left.name.cmp(&right.name));
    encoded.fields = fields
        .into_iter()
        .map(encode_field_definition)
        .try_collect()?;
    encoded.metadata = schema.metadata.clone();
    Ok(encoded)
}

/// Decode a `Schema` from protobuf schema transport form.
pub fn decode_schema_definition(mut schema: proto::SchemaDefinition) -> SchemaResult<Schema> {
    let mut columns = HashMap::with_capacity(schema.fields.len());
    for field in schema.fields.drain(..) {
        let field = decode_field_definition(field)?;
        let field_name = field.name.clone();
        if columns.insert(field_name.clone(), field).is_some() {
            return DuplicateFieldNameSnafu { field_name }.fail();
        }
    }
    Ok(Schema {
        columns,
        metadata: schema.metadata,
    })
}

fn encode_field_definition(field: &Field) -> SchemaResult<proto::FieldDefinition> {
    let mut encoded = proto::FieldDefinition::new();
    encoded.name = field.name.clone();
    encoded.data_type = MessageField::some(encode_replicated_data_type(&field.data_type)?);
    encoded.metadata = field.metadata.clone();
    Ok(encoded)
}

fn decode_field_definition(mut field: proto::FieldDefinition) -> SchemaResult<Field> {
    let data_type = field
        .data_type
        .take_required("FieldDefinition", "data_type")
        .and_then(decode_replicated_data_type)?;
    Ok(Field {
        name: field.name,
        data_type,
        metadata: field.metadata,
    })
}

fn encode_replicated_data_type(
    data_type: &ReplicatedDataType,
) -> SchemaResult<proto::ReplicatedDataType> {
    let mut encoded = proto::ReplicatedDataType::new();
    match data_type {
        ReplicatedDataType::LatestValueWins { value_type } => {
            encoded.kind = EnumOrUnknown::new(
                proto::ReplicatedDataTypeKind::REPLICATED_DATA_TYPE_KIND_LATEST_VALUE_WINS,
            );
            encoded.detail = Some(proto::replicated_data_type::Detail::NullableBasicValueType(
                encode_nullable_basic_data_type(value_type),
            ));
        }
        ReplicatedDataType::LinearString => {
            encoded.kind = EnumOrUnknown::new(
                proto::ReplicatedDataTypeKind::REPLICATED_DATA_TYPE_KIND_LINEAR_STRING,
            );
        }
        ReplicatedDataType::LinearList { value_type } => {
            encoded.kind = EnumOrUnknown::new(
                proto::ReplicatedDataTypeKind::REPLICATED_DATA_TYPE_KIND_LINEAR_LIST,
            );
            encoded.detail = Some(proto::replicated_data_type::Detail::PrimitiveValueType(
                encode_primitive_type(*value_type),
            ));
        }
        ReplicatedDataType::MonotonicCounter { small_range } => {
            encoded.kind = EnumOrUnknown::new(
                proto::ReplicatedDataTypeKind::REPLICATED_DATA_TYPE_KIND_MONOTONIC_COUNTER,
            );
            encoded.detail =
                Some(proto::replicated_data_type::Detail::MonotonicCounterSmallRange(*small_range));
        }
        ReplicatedDataType::TotalOrderRegister {
            value_type,
            direction,
        } => {
            encoded.kind = EnumOrUnknown::new(
                proto::ReplicatedDataTypeKind::REPLICATED_DATA_TYPE_KIND_TOTAL_ORDER_REGISTER,
            );
            encoded.detail = Some(proto::replicated_data_type::Detail::PrimitiveValueType(
                encode_primitive_type(*value_type),
            ));
            encoded.total_order_direction = Some(encode_direction(direction.clone()));
        }
        ReplicatedDataType::TotalOrderFiniteStateRegister { value_type, states } => {
            validate_finite_state_definition(*value_type, states)?;
            encoded.kind = EnumOrUnknown::new(
                proto::ReplicatedDataTypeKind::REPLICATED_DATA_TYPE_KIND_TOTAL_ORDER_FINITE_STATE_REGISTER,
            );
            encoded.detail = Some(proto::replicated_data_type::Detail::FiniteStateStates(
                encode_nullable_primitive_value_array(states)?,
            ));
            encoded.finite_state_value_type =
                MessageField::some(encode_nullable_primitive_type(*value_type));
        }
    }
    Ok(encoded)
}

fn decode_replicated_data_type(
    mut data_type: proto::ReplicatedDataType,
) -> SchemaResult<ReplicatedDataType> {
    let kind = decode_replicated_data_type_kind(data_type.kind)?;
    let kind_name = replicated_data_type_kind_name(kind);
    let detail = data_type.detail.take();
    let total_order_direction = data_type
        .total_order_direction
        .take()
        .map(decode_direction)
        .transpose()?;
    let finite_state_value_type = data_type
        .finite_state_value_type
        .take()
        .map(decode_nullable_primitive_type)
        .transpose()?;

    match kind {
        proto::ReplicatedDataTypeKind::REPLICATED_DATA_TYPE_KIND_LATEST_VALUE_WINS => {
            ensure!(
                total_order_direction.is_none(),
                UnexpectedTotalOrderDirectionSnafu { kind: kind_name }
            );
            ensure!(
                finite_state_value_type.is_none(),
                UnexpectedFiniteStateValueTypeSnafu { kind: kind_name }
            );
            let detail = detail.context(MissingReplicatedDataTypeDetailSnafu { kind: kind_name })?;
            let proto::replicated_data_type::Detail::NullableBasicValueType(value_type) = detail
            else {
                return UnexpectedReplicatedDataTypeDetailSnafu {
                    kind: kind_name,
                    detail: replicated_data_type_detail_name(&detail),
                }
                .fail();
            };
            let value_type = decode_nullable_basic_data_type(value_type)?;
            Ok(ReplicatedDataType::LatestValueWins { value_type })
        }
        proto::ReplicatedDataTypeKind::REPLICATED_DATA_TYPE_KIND_LINEAR_STRING => {
            ensure!(
                detail.is_none(),
                UnexpectedReplicatedDataTypeDetailSnafu {
                    kind: kind_name,
                    detail: replicated_data_type_optional_detail_name(detail.as_ref()),
                }
            );
            ensure!(
                total_order_direction.is_none(),
                UnexpectedTotalOrderDirectionSnafu { kind: kind_name }
            );
            ensure!(
                finite_state_value_type.is_none(),
                UnexpectedFiniteStateValueTypeSnafu { kind: kind_name }
            );
            Ok(ReplicatedDataType::LinearString)
        }
        proto::ReplicatedDataTypeKind::REPLICATED_DATA_TYPE_KIND_LINEAR_LIST => {
            ensure!(
                total_order_direction.is_none(),
                UnexpectedTotalOrderDirectionSnafu { kind: kind_name }
            );
            ensure!(
                finite_state_value_type.is_none(),
                UnexpectedFiniteStateValueTypeSnafu { kind: kind_name }
            );
            let detail = detail.context(MissingReplicatedDataTypeDetailSnafu { kind: kind_name })?;
            let proto::replicated_data_type::Detail::PrimitiveValueType(value_type) = detail else {
                return UnexpectedReplicatedDataTypeDetailSnafu {
                    kind: kind_name,
                    detail: replicated_data_type_detail_name(&detail),
                }
                .fail();
            };
            let value_type = decode_primitive_type(value_type)?;
            Ok(ReplicatedDataType::LinearList { value_type })
        }
        proto::ReplicatedDataTypeKind::REPLICATED_DATA_TYPE_KIND_MONOTONIC_COUNTER => {
            ensure!(
                total_order_direction.is_none(),
                UnexpectedTotalOrderDirectionSnafu { kind: kind_name }
            );
            ensure!(
                finite_state_value_type.is_none(),
                UnexpectedFiniteStateValueTypeSnafu { kind: kind_name }
            );
            let detail = detail.context(MissingReplicatedDataTypeDetailSnafu { kind: kind_name })?;
            let proto::replicated_data_type::Detail::MonotonicCounterSmallRange(small_range) =
                detail
            else {
                return UnexpectedReplicatedDataTypeDetailSnafu {
                    kind: kind_name,
                    detail: replicated_data_type_detail_name(&detail),
                }
                .fail();
            };
            Ok(ReplicatedDataType::MonotonicCounter { small_range })
        }
        proto::ReplicatedDataTypeKind::REPLICATED_DATA_TYPE_KIND_TOTAL_ORDER_REGISTER => {
            ensure!(
                finite_state_value_type.is_none(),
                UnexpectedFiniteStateValueTypeSnafu { kind: kind_name }
            );
            let detail = detail.context(MissingReplicatedDataTypeDetailSnafu { kind: kind_name })?;
            let proto::replicated_data_type::Detail::PrimitiveValueType(value_type) = detail else {
                return UnexpectedReplicatedDataTypeDetailSnafu {
                    kind: kind_name,
                    detail: replicated_data_type_detail_name(&detail),
                }
                .fail();
            };
            let value_type = decode_primitive_type(value_type)?;
            let direction = total_order_direction.context(MissingTotalOrderDirectionSnafu {
                kind: kind_name,
            })?;
            Ok(ReplicatedDataType::TotalOrderRegister {
                value_type,
                direction,
            })
        }
        proto::ReplicatedDataTypeKind::REPLICATED_DATA_TYPE_KIND_TOTAL_ORDER_FINITE_STATE_REGISTER => {
            ensure!(
                total_order_direction.is_none(),
                UnexpectedTotalOrderDirectionSnafu { kind: kind_name }
            );
            let detail = detail.context(MissingReplicatedDataTypeDetailSnafu { kind: kind_name })?;
            let proto::replicated_data_type::Detail::FiniteStateStates(states) = detail else {
                return UnexpectedReplicatedDataTypeDetailSnafu {
                    kind: kind_name,
                    detail: replicated_data_type_detail_name(&detail),
                }
                .fail();
            };
            let states = decode_nullable_primitive_value_array(states)?;
            let value_type = finite_state_value_type.context(MissingFiniteStateValueTypeSnafu {
                kind: kind_name,
            })?;
            validate_finite_state_definition(value_type, &states)?;
            Ok(ReplicatedDataType::TotalOrderFiniteStateRegister { value_type, states })
        }
        proto::ReplicatedDataTypeKind::REPLICATED_DATA_TYPE_KIND_UNSPECIFIED => {
            UnspecifiedReplicatedDataTypeKindSnafu.fail()
        }
    }
}

fn encode_nullable_basic_data_type(
    value_type: &NullableBasicDataType,
) -> proto::NullableBasicDataType {
    let mut encoded = proto::NullableBasicDataType::new();
    match value_type {
        NullableBasicDataType::NonNull(value_type) => {
            encoded.value = Some(proto::nullable_basic_data_type::Value::NonNull(
                encode_basic_data_type(value_type),
            ));
        }
        NullableBasicDataType::Nullable(value_type) => {
            encoded.value = Some(proto::nullable_basic_data_type::Value::Nullable(
                encode_basic_data_type(value_type),
            ));
        }
    }
    encoded
}

fn decode_nullable_basic_data_type(
    mut value_type: proto::NullableBasicDataType,
) -> SchemaResult<NullableBasicDataType> {
    let value = value_type
        .value
        .take()
        .take_required_oneof("NullableBasicDataType.value")?;
    match value {
        proto::nullable_basic_data_type::Value::NonNull(value_type) => {
            decode_basic_data_type(value_type).map(NullableBasicDataType::NonNull)
        }
        proto::nullable_basic_data_type::Value::Nullable(value_type) => {
            decode_basic_data_type(value_type).map(NullableBasicDataType::Nullable)
        }
    }
}

fn encode_basic_data_type(value_type: &BasicDataType) -> proto::BasicDataType {
    let mut encoded = proto::BasicDataType::new();
    match value_type {
        BasicDataType::Primitive(value_type) => {
            encoded.value = Some(proto::basic_data_type::Value::Primitive(
                encode_primitive_type(*value_type),
            ));
        }
        BasicDataType::Array(array_type) => {
            encoded.value = Some(proto::basic_data_type::Value::PrimitiveArray(
                encode_primitive_type(array_type.element_type),
            ));
        }
    }
    encoded
}

fn decode_basic_data_type(mut value_type: proto::BasicDataType) -> SchemaResult<BasicDataType> {
    let value = value_type
        .value
        .take()
        .take_required_oneof("BasicDataType.value")?;
    match value {
        proto::basic_data_type::Value::Primitive(value_type) => {
            decode_primitive_type(value_type).map(BasicDataType::Primitive)
        }
        proto::basic_data_type::Value::PrimitiveArray(element_type) => {
            decode_primitive_type(element_type)
                .map(|element_type| BasicDataType::Array(Box::new(ArrayType { element_type })))
        }
    }
}

fn encode_nullable_primitive_type(
    value_type: NullablePrimitiveType,
) -> proto::NullablePrimitiveType {
    let mut encoded = proto::NullablePrimitiveType::new();
    match value_type {
        NullablePrimitiveType::NonNull(value_type) => {
            encoded.value = Some(proto::nullable_primitive_type::Value::NonNull(
                encode_primitive_type(value_type),
            ));
        }
        NullablePrimitiveType::Nullable(value_type) => {
            encoded.value = Some(proto::nullable_primitive_type::Value::Nullable(
                encode_primitive_type(value_type),
            ));
        }
    }
    encoded
}

fn decode_nullable_primitive_type(
    mut value_type: proto::NullablePrimitiveType,
) -> SchemaResult<NullablePrimitiveType> {
    let value = value_type
        .value
        .take()
        .take_required_oneof("NullablePrimitiveType.value")?;
    match value {
        proto::nullable_primitive_type::Value::NonNull(value_type) => {
            decode_primitive_type(value_type).map(NullablePrimitiveType::NonNull)
        }
        proto::nullable_primitive_type::Value::Nullable(value_type) => {
            decode_primitive_type(value_type).map(NullablePrimitiveType::Nullable)
        }
    }
}

fn encode_nullable_primitive_value_array(
    states: &NullablePrimitiveValueArray,
) -> SchemaResult<proto::NullablePrimitiveValueArray> {
    let mut encoded = proto::NullablePrimitiveValueArray::new();
    match states {
        NullablePrimitiveValueArray::NonNull(values) => {
            encoded.values = MessageField::some(encode_primitive_array(values));
        }
        NullablePrimitiveValueArray::Nullable { values, null_index } => {
            ensure!(
                *null_index <= values.len(),
                InvalidFiniteStateNullIndexSnafu {
                    null_index: *null_index,
                    value_count: values.len(),
                }
            );
            let null_index = u32::try_from(*null_index).map_err(|_| {
                SchemaCodecError::NullIndexTooLargeForWire {
                    null_index: *null_index,
                }
            })?;
            encoded.values = MessageField::some(encode_primitive_array(values));
            encoded.null_index = Some(null_index);
        }
    }
    Ok(encoded)
}

fn decode_nullable_primitive_value_array(
    mut states: proto::NullablePrimitiveValueArray,
) -> SchemaResult<NullablePrimitiveValueArray> {
    let values = states
        .values
        .take_required("NullablePrimitiveValueArray", "values")
        .and_then(decode_primitive_array)?;
    let null_index = states.null_index.map(|value| value as usize);
    if let Some(null_index) = null_index {
        ensure!(
            null_index <= values.len(),
            InvalidFiniteStateNullIndexSnafu {
                null_index,
                value_count: values.len(),
            }
        );
        return Ok(NullablePrimitiveValueArray::Nullable { values, null_index });
    }
    Ok(NullablePrimitiveValueArray::NonNull(values))
}

fn validate_finite_state_definition(
    value_type: NullablePrimitiveType,
    states: &NullablePrimitiveValueArray,
) -> SchemaResult<()> {
    ensure!(states.value_count() > 0, EmptyFiniteStateStatesSnafu);
    if let NullablePrimitiveValueArray::Nullable { values, null_index } = states {
        ensure!(
            *null_index <= values.len(),
            InvalidFiniteStateNullIndexSnafu {
                null_index: *null_index,
                value_count: values.len(),
            }
        );
    }
    let actual = states.value_type();
    ensure!(
        value_type == actual,
        FiniteStateValueTypeMismatchSnafu {
            expected: value_type,
            actual,
        }
    );
    Ok(())
}

fn encode_primitive_array(value: &PrimitiveValueArray) -> proto::PrimitiveArrayValue {
    let mut encoded = proto::PrimitiveArrayValue::new();
    match value {
        PrimitiveValueArray::String(values) => {
            let mut message = proto::StringArrayValue::new();
            message.values = values.to_vec();
            encoded.value = Some(proto::primitive_array_value::Value::String(message));
        }
        PrimitiveValueArray::UInt(values) => {
            let mut message = proto::UIntArrayValue::new();
            message.values = values.to_vec();
            encoded.value = Some(proto::primitive_array_value::Value::Uint(message));
        }
        PrimitiveValueArray::Int(values) => {
            let mut message = proto::IntArrayValue::new();
            message.values = values.to_vec();
            encoded.value = Some(proto::primitive_array_value::Value::Int(message));
        }
        PrimitiveValueArray::Byte(values) => {
            let mut message = proto::ByteArrayValue::new();
            message.values = values.to_vec();
            encoded.value = Some(proto::primitive_array_value::Value::Byte(message));
        }
        PrimitiveValueArray::Float(values) => {
            let mut message = proto::FloatArrayValue::new();
            message.values = values.iter().map(|value| value.0).collect();
            encoded.value = Some(proto::primitive_array_value::Value::Float(message));
        }
        PrimitiveValueArray::Boolean(values) => {
            let mut message = proto::BooleanArrayValue::new();
            message.values = values.to_vec();
            encoded.value = Some(proto::primitive_array_value::Value::Boolean(message));
        }
        PrimitiveValueArray::Binary(values) => {
            let mut message = proto::BinaryArrayValue::new();
            message.values = values.to_vec();
            encoded.value = Some(proto::primitive_array_value::Value::Binary(message));
        }
        PrimitiveValueArray::Date(values) => {
            let mut message = proto::DateArrayValue::new();
            message.values = values.iter().copied().map(encode_date).collect();
            encoded.value = Some(proto::primitive_array_value::Value::Date(message));
        }
        PrimitiveValueArray::Timestamp(values) => {
            let mut message = proto::TimestampArrayValue::new();
            message.values = values.to_vec();
            encoded.value = Some(proto::primitive_array_value::Value::Timestamp(message));
        }
    }
    encoded
}

fn decode_primitive_array(
    mut value: proto::PrimitiveArrayValue,
) -> SchemaResult<PrimitiveValueArray> {
    let value = value
        .value
        .take()
        .take_required_oneof("PrimitiveArrayValue.value")?;
    match value {
        proto::primitive_array_value::Value::String(values) => {
            Ok(PrimitiveValueArray::String(values.values))
        }
        proto::primitive_array_value::Value::Uint(values) => {
            Ok(PrimitiveValueArray::UInt(values.values))
        }
        proto::primitive_array_value::Value::Int(values) => {
            Ok(PrimitiveValueArray::Int(values.values))
        }
        proto::primitive_array_value::Value::Byte(values) => {
            Ok(PrimitiveValueArray::Byte(values.values))
        }
        proto::primitive_array_value::Value::Float(values) => Ok(PrimitiveValueArray::Float(
            values.values.into_iter().map(OrderedFloat).collect(),
        )),
        proto::primitive_array_value::Value::Boolean(values) => {
            Ok(PrimitiveValueArray::Boolean(values.values))
        }
        proto::primitive_array_value::Value::Binary(values) => {
            Ok(PrimitiveValueArray::Binary(values.values))
        }
        proto::primitive_array_value::Value::Date(values) => {
            let values = values.values.into_iter().map(decode_date).try_collect()?;
            Ok(PrimitiveValueArray::Date(values))
        }
        proto::primitive_array_value::Value::Timestamp(values) => {
            Ok(PrimitiveValueArray::Timestamp(values.values))
        }
    }
}

fn encode_date(value: NaiveDate) -> proto::Date {
    proto::Date {
        year: value.year(),
        month: value.month(),
        day: value.day(),
        ..proto::Date::new()
    }
}

fn decode_date(value: proto::Date) -> SchemaResult<NaiveDate> {
    NaiveDate::from_ymd_opt(value.year, value.month, value.day).context(InvalidDateSnafu {
        year: value.year,
        month: value.month,
        day: value.day,
    })
}

fn encode_primitive_type(value: PrimitiveType) -> EnumOrUnknown<proto::PrimitiveType> {
    let value = match value {
        PrimitiveType::String => proto::PrimitiveType::PRIMITIVE_TYPE_STRING,
        PrimitiveType::UInt => proto::PrimitiveType::PRIMITIVE_TYPE_UINT,
        PrimitiveType::Int => proto::PrimitiveType::PRIMITIVE_TYPE_INT,
        PrimitiveType::Byte => proto::PrimitiveType::PRIMITIVE_TYPE_BYTE,
        PrimitiveType::Float => proto::PrimitiveType::PRIMITIVE_TYPE_FLOAT,
        PrimitiveType::Boolean => proto::PrimitiveType::PRIMITIVE_TYPE_BOOLEAN,
        PrimitiveType::Binary => proto::PrimitiveType::PRIMITIVE_TYPE_BINARY,
        PrimitiveType::Date => proto::PrimitiveType::PRIMITIVE_TYPE_DATE,
        PrimitiveType::Timestamp => proto::PrimitiveType::PRIMITIVE_TYPE_TIMESTAMP,
    };
    EnumOrUnknown::new(value)
}

fn decode_primitive_type(
    value: EnumOrUnknown<proto::PrimitiveType>,
) -> SchemaResult<PrimitiveType> {
    let value = value
        .enum_value()
        .map_err(|value| SchemaCodecError::UnknownPrimitiveType { value })?;
    match value {
        proto::PrimitiveType::PRIMITIVE_TYPE_UNSPECIFIED => UnspecifiedPrimitiveTypeSnafu.fail(),
        proto::PrimitiveType::PRIMITIVE_TYPE_STRING => Ok(PrimitiveType::String),
        proto::PrimitiveType::PRIMITIVE_TYPE_UINT => Ok(PrimitiveType::UInt),
        proto::PrimitiveType::PRIMITIVE_TYPE_INT => Ok(PrimitiveType::Int),
        proto::PrimitiveType::PRIMITIVE_TYPE_BYTE => Ok(PrimitiveType::Byte),
        proto::PrimitiveType::PRIMITIVE_TYPE_FLOAT => Ok(PrimitiveType::Float),
        proto::PrimitiveType::PRIMITIVE_TYPE_BOOLEAN => Ok(PrimitiveType::Boolean),
        proto::PrimitiveType::PRIMITIVE_TYPE_BINARY => Ok(PrimitiveType::Binary),
        proto::PrimitiveType::PRIMITIVE_TYPE_DATE => Ok(PrimitiveType::Date),
        proto::PrimitiveType::PRIMITIVE_TYPE_TIMESTAMP => Ok(PrimitiveType::Timestamp),
    }
}

fn encode_direction(value: Direction) -> EnumOrUnknown<proto::Direction> {
    let value = match value {
        Direction::Ascending => proto::Direction::DIRECTION_ASCENDING,
        Direction::Descending => proto::Direction::DIRECTION_DESCENDING,
    };
    EnumOrUnknown::new(value)
}

fn decode_direction(value: EnumOrUnknown<proto::Direction>) -> SchemaResult<Direction> {
    let value = value
        .enum_value()
        .map_err(|value| SchemaCodecError::UnknownDirection { value })?;
    match value {
        proto::Direction::DIRECTION_UNSPECIFIED => UnspecifiedDirectionSnafu.fail(),
        proto::Direction::DIRECTION_ASCENDING => Ok(Direction::Ascending),
        proto::Direction::DIRECTION_DESCENDING => Ok(Direction::Descending),
    }
}

fn decode_replicated_data_type_kind(
    value: EnumOrUnknown<proto::ReplicatedDataTypeKind>,
) -> SchemaResult<proto::ReplicatedDataTypeKind> {
    let value = value
        .enum_value()
        .map_err(|value| SchemaCodecError::UnknownReplicatedDataTypeKind { value })?;
    if value == proto::ReplicatedDataTypeKind::REPLICATED_DATA_TYPE_KIND_UNSPECIFIED {
        return UnspecifiedReplicatedDataTypeKindSnafu.fail();
    }
    Ok(value)
}

fn replicated_data_type_kind_name(kind: proto::ReplicatedDataTypeKind) -> &'static str {
    match kind {
        proto::ReplicatedDataTypeKind::REPLICATED_DATA_TYPE_KIND_UNSPECIFIED => "UNSPECIFIED",
        proto::ReplicatedDataTypeKind::REPLICATED_DATA_TYPE_KIND_LATEST_VALUE_WINS => {
            "LATEST_VALUE_WINS"
        }
        proto::ReplicatedDataTypeKind::REPLICATED_DATA_TYPE_KIND_LINEAR_STRING => "LINEAR_STRING",
        proto::ReplicatedDataTypeKind::REPLICATED_DATA_TYPE_KIND_LINEAR_LIST => "LINEAR_LIST",
        proto::ReplicatedDataTypeKind::REPLICATED_DATA_TYPE_KIND_MONOTONIC_COUNTER => {
            "MONOTONIC_COUNTER"
        }
        proto::ReplicatedDataTypeKind::REPLICATED_DATA_TYPE_KIND_TOTAL_ORDER_REGISTER => {
            "TOTAL_ORDER_REGISTER"
        }
        proto::ReplicatedDataTypeKind::REPLICATED_DATA_TYPE_KIND_TOTAL_ORDER_FINITE_STATE_REGISTER => {
            "TOTAL_ORDER_FINITE_STATE_REGISTER"
        }
    }
}

fn replicated_data_type_optional_detail_name(
    detail: Option<&proto::replicated_data_type::Detail>,
) -> &'static str {
    detail.map_or("none", replicated_data_type_detail_name)
}

fn replicated_data_type_detail_name(detail: &proto::replicated_data_type::Detail) -> &'static str {
    match detail {
        proto::replicated_data_type::Detail::NullableBasicValueType(_) => {
            "nullable_basic_value_type"
        }
        proto::replicated_data_type::Detail::PrimitiveValueType(_) => "primitive_value_type",
        proto::replicated_data_type::Detail::MonotonicCounterSmallRange(_) => {
            "monotonic_counter_small_range"
        }
        proto::replicated_data_type::Detail::FiniteStateStates(_) => "finite_state_states",
    }
}
