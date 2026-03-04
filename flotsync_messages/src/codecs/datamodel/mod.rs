pub mod columnar_history;
pub mod operations;
pub use columnar_history::*;
pub use operations::*;

use crate::datamodel as proto;
use flotsync_core::versions::UpdateId;
use flotsync_data_types::{
    IdWithIndex,
    IntegrityError,
    chrono::{Datelike, NaiveDate},
    schema::{
        ReplicatedDataType,
        datamodel::{
            BasicValue as ModelBasicValue,
            BasicValueRef as ModelBasicValueRef,
            CounterValue as ModelCounterValue,
            CounterValueRef as ModelCounterValueRef,
            DataModelValueError,
            NullableBasicValue as ModelNullableBasicValue,
            NullableBasicValueRef as ModelNullableBasicValueRef,
            PrimitiveValueArrayRef as ModelPrimitiveValueArrayRef,
            SnapshotStateValue,
            SnapshotStateValueRef,
            validation::ensure_snapshot_state_value_type,
        },
        values::{
            NullablePrimitiveValue as ModelNullablePrimitiveValue,
            NullablePrimitiveValueRef as ModelNullablePrimitiveValueRef,
            PrimitiveValue as ModelPrimitiveValue,
            PrimitiveValueArray as ModelPrimitiveValueArray,
            PrimitiveValueRef as ModelPrimitiveValueRef,
        },
    },
};
use ordered_float::OrderedFloat;
use snafu::prelude::*;

pub(crate) type UpdateIdWithIndex = IdWithIndex<UpdateId>;

#[derive(Debug, Snafu)]
pub enum CodecError {
    #[snafu(display("Protobuf message '{message}' is missing a required field '{field}'."))]
    MissingField {
        message: &'static str,
        field: &'static str,
    },
    #[snafu(display("Protobuf oneof '{name}' has no selected value."))]
    MissingOneof { name: &'static str },
    #[snafu(display("Byte value {value} does not fit into u8."))]
    ByteOutOfRange { value: u32 },
    #[snafu(display("Date {year:04}-{month:02}-{day:02} is not a valid calendar date."))]
    InvalidDate { year: i32, month: u32, day: u32 },
    #[snafu(display("Snapshot payload does not match the schema data type."))]
    InvalidSnapshotValue { source: DataModelValueError },
    #[snafu(display("Reconstructed CRDT failed integrity validation."))]
    Integrity { source: IntegrityError },
}

#[derive(Clone, Debug, PartialEq)]
pub enum StateSnapshotWireValue {
    MonotonicCounter(proto::CounterValue),
    TotalOrderRegister(proto::PrimitiveValue),
    TotalOrderFiniteStateRegister(proto::NullablePrimitiveValue),
}

pub fn encode_update_id(id: UpdateId) -> proto::HistoryId {
    proto::HistoryId {
        version: id.version,
        node_index: id.node_index,
        chunk_index: 0,
        ..proto::HistoryId::new()
    }
}

pub fn decode_update_id(id: proto::HistoryId) -> Result<UpdateId, CodecError> {
    Ok(UpdateId {
        version: id.version,
        node_index: id.node_index,
    })
}

pub fn encode_indexed_update_id(id: &UpdateIdWithIndex) -> proto::HistoryId {
    proto::HistoryId {
        version: id.id.version,
        node_index: id.id.node_index,
        chunk_index: id.index,
        ..proto::HistoryId::new()
    }
}

pub fn decode_indexed_update_id(id: proto::HistoryId) -> Result<UpdateIdWithIndex, CodecError> {
    Ok(IdWithIndex {
        id: UpdateId {
            version: id.version,
            node_index: id.node_index,
        },
        index: id.chunk_index,
    })
}

pub fn encode_primitive_value(value: ModelPrimitiveValueRef<'_>) -> proto::PrimitiveValue {
    let mut encoded = proto::PrimitiveValue::new();
    match value {
        ModelPrimitiveValueRef::String(value) => encoded.set_string(value.to_owned()),
        ModelPrimitiveValueRef::UInt(value) => encoded.set_uint(value),
        ModelPrimitiveValueRef::Int(value) => encoded.set_int(value),
        ModelPrimitiveValueRef::Byte(value) => encoded.set_byte(u32::from(value)),
        ModelPrimitiveValueRef::Float(value) => encoded.set_float(value.0),
        ModelPrimitiveValueRef::Boolean(value) => encoded.set_boolean(value),
        ModelPrimitiveValueRef::Binary(value) => encoded.set_binary(value.to_vec()),
        ModelPrimitiveValueRef::Date(value) => encoded.set_date(encode_date(value)),
        ModelPrimitiveValueRef::Timestamp(value) => encoded.set_timestamp(value),
    }
    encoded
}

pub fn decode_primitive_value(
    mut value: proto::PrimitiveValue,
) -> Result<ModelPrimitiveValue, CodecError> {
    let value = value.value.take().context(MissingOneofSnafu {
        name: "PrimitiveValue.value",
    })?;
    match value {
        proto::primitive_value::Value::String(value) => Ok(ModelPrimitiveValue::String(value)),
        proto::primitive_value::Value::Uint(value) => Ok(ModelPrimitiveValue::UInt(value)),
        proto::primitive_value::Value::Int(value) => Ok(ModelPrimitiveValue::Int(value)),
        proto::primitive_value::Value::Byte(value) => {
            let byte = u8::try_from(value).map_err(|_| CodecError::ByteOutOfRange { value })?;
            Ok(ModelPrimitiveValue::Byte(byte))
        }
        proto::primitive_value::Value::Float(value) => {
            Ok(ModelPrimitiveValue::Float(OrderedFloat(value)))
        }
        proto::primitive_value::Value::Boolean(value) => Ok(ModelPrimitiveValue::Boolean(value)),
        proto::primitive_value::Value::Binary(value) => Ok(ModelPrimitiveValue::Binary(value)),
        proto::primitive_value::Value::Date(value) => {
            let date = decode_date(value)?;
            Ok(ModelPrimitiveValue::Date(date))
        }
        proto::primitive_value::Value::Timestamp(value) => {
            Ok(ModelPrimitiveValue::Timestamp(value))
        }
    }
}

pub fn encode_primitive_array(
    value: ModelPrimitiveValueArrayRef<'_>,
) -> proto::PrimitiveArrayValue {
    let mut encoded = proto::PrimitiveArrayValue::new();
    match value {
        ModelPrimitiveValueArrayRef::String(values) => {
            let mut message = proto::StringArrayValue::new();
            message.values = values.to_vec();
            encoded.set_string(message);
        }
        ModelPrimitiveValueArrayRef::UInt(values) => {
            let mut message = proto::UIntArrayValue::new();
            message.values = values.to_vec();
            encoded.set_uint(message);
        }
        ModelPrimitiveValueArrayRef::Int(values) => {
            let mut message = proto::IntArrayValue::new();
            message.values = values.to_vec();
            encoded.set_int(message);
        }
        ModelPrimitiveValueArrayRef::Byte(values) => {
            let mut message = proto::ByteArrayValue::new();
            message.values = values.to_vec();
            encoded.set_byte(message);
        }
        ModelPrimitiveValueArrayRef::Float(values) => {
            let mut message = proto::FloatArrayValue::new();
            message.values = values.iter().map(|value| value.0).collect();
            encoded.set_float(message);
        }
        ModelPrimitiveValueArrayRef::Boolean(values) => {
            let mut message = proto::BooleanArrayValue::new();
            message.values = values.to_vec();
            encoded.set_boolean(message);
        }
        ModelPrimitiveValueArrayRef::Binary(values) => {
            let mut message = proto::BinaryArrayValue::new();
            message.values = values.to_vec();
            encoded.set_binary(message);
        }
        ModelPrimitiveValueArrayRef::Date(values) => {
            let mut message = proto::DateArrayValue::new();
            message.values = values.iter().copied().map(encode_date).collect();
            encoded.set_date(message);
        }
        ModelPrimitiveValueArrayRef::Timestamp(values) => {
            let mut message = proto::TimestampArrayValue::new();
            message.values = values.to_vec();
            encoded.set_timestamp(message);
        }
    }
    encoded
}

pub fn decode_primitive_array(
    mut value: proto::PrimitiveArrayValue,
) -> Result<ModelPrimitiveValueArray, CodecError> {
    match value.value.take().context(MissingOneofSnafu {
        name: "PrimitiveArrayValue.value",
    })? {
        proto::primitive_array_value::Value::String(values) => {
            Ok(ModelPrimitiveValueArray::String(values.values))
        }
        proto::primitive_array_value::Value::Uint(values) => {
            Ok(ModelPrimitiveValueArray::UInt(values.values))
        }
        proto::primitive_array_value::Value::Int(values) => {
            Ok(ModelPrimitiveValueArray::Int(values.values))
        }
        proto::primitive_array_value::Value::Byte(values) => {
            Ok(ModelPrimitiveValueArray::Byte(values.values))
        }
        proto::primitive_array_value::Value::Float(values) => {
            let floats = values.values.into_iter().map(OrderedFloat).collect();
            Ok(ModelPrimitiveValueArray::Float(floats))
        }
        proto::primitive_array_value::Value::Boolean(values) => {
            Ok(ModelPrimitiveValueArray::Boolean(values.values))
        }
        proto::primitive_array_value::Value::Binary(values) => {
            Ok(ModelPrimitiveValueArray::Binary(values.values))
        }
        proto::primitive_array_value::Value::Date(values) => {
            let dates: Vec<NaiveDate> = values.values.into_iter().map(decode_date).try_collect()?;
            Ok(ModelPrimitiveValueArray::Date(dates))
        }
        proto::primitive_array_value::Value::Timestamp(values) => {
            Ok(ModelPrimitiveValueArray::Timestamp(values.values))
        }
    }
}

pub fn encode_basic_value(value: ModelBasicValueRef<'_>) -> proto::BasicValue {
    let mut encoded = proto::BasicValue::new();
    match value {
        ModelBasicValueRef::Primitive(value) => {
            encoded.set_primitive(encode_primitive_value(value))
        }
        ModelBasicValueRef::Array(value) => encoded.set_array(encode_primitive_array(value)),
    }
    encoded
}

pub fn decode_basic_value(mut value: proto::BasicValue) -> Result<ModelBasicValue, CodecError> {
    let value = value.value.take().context(MissingOneofSnafu {
        name: "BasicValue.value",
    })?;
    match value {
        proto::basic_value::Value::Primitive(value) => {
            decode_primitive_value(value).map(ModelBasicValue::Primitive)
        }
        proto::basic_value::Value::Array(value) => {
            decode_primitive_array(value).map(ModelBasicValue::Array)
        }
    }
}

pub fn encode_nullable_basic_value(
    value: ModelNullableBasicValueRef<'_>,
) -> proto::NullableBasicValue {
    let mut encoded = proto::NullableBasicValue::new();
    match value {
        ModelNullableBasicValueRef::Null => encoded.set_null(proto::NullValue::new()),
        ModelNullableBasicValueRef::Value(ModelBasicValueRef::Primitive(value)) => {
            encoded.set_primitive(encode_primitive_value(value))
        }
        ModelNullableBasicValueRef::Value(ModelBasicValueRef::Array(value)) => {
            encoded.set_array(encode_primitive_array(value))
        }
    }
    encoded
}

pub fn decode_nullable_basic_value(
    mut value: proto::NullableBasicValue,
) -> Result<ModelNullableBasicValue, CodecError> {
    let value = value.value.take().context(MissingOneofSnafu {
        name: "NullableBasicValue.value",
    })?;
    match value {
        proto::nullable_basic_value::Value::Null(_) => Ok(ModelNullableBasicValue::Null),
        proto::nullable_basic_value::Value::Primitive(value) => decode_primitive_value(value)
            .map(ModelBasicValue::Primitive)
            .map(ModelNullableBasicValue::Value),
        proto::nullable_basic_value::Value::Array(value) => decode_primitive_array(value)
            .map(ModelBasicValue::Array)
            .map(ModelNullableBasicValue::Value),
    }
}

pub fn encode_nullable_primitive_value(
    value: ModelNullablePrimitiveValueRef<'_>,
) -> proto::NullablePrimitiveValue {
    let mut encoded = proto::NullablePrimitiveValue::new();
    match value {
        ModelNullablePrimitiveValueRef::Null => encoded.set_null(proto::NullValue::new()),
        ModelNullablePrimitiveValueRef::Value(value) => {
            encoded.set_primitive(encode_primitive_value(value))
        }
    }
    encoded
}

pub fn decode_nullable_primitive_value(
    mut value: proto::NullablePrimitiveValue,
) -> Result<ModelNullablePrimitiveValue, CodecError> {
    let value = value.value.take().context(MissingOneofSnafu {
        name: "NullablePrimitiveValue.value",
    })?;
    match value {
        proto::nullable_primitive_value::Value::Null(_) => Ok(ModelNullablePrimitiveValue::Null),
        proto::nullable_primitive_value::Value::Primitive(value) => {
            decode_primitive_value(value).map(ModelNullablePrimitiveValue::Value)
        }
    }
}

pub fn encode_counter_value(value: ModelCounterValueRef) -> proto::CounterValue {
    let mut encoded = proto::CounterValue::new();
    match value {
        ModelCounterValueRef::Byte(value) => encoded.set_byte(value as u32),
        ModelCounterValueRef::UInt(value) => encoded.set_uint(value),
    }
    encoded
}

pub fn decode_counter_value(
    mut value: proto::CounterValue,
) -> Result<ModelCounterValue, CodecError> {
    let value = value.value.take().context(MissingOneofSnafu {
        name: "CounterValue.value",
    })?;
    match value {
        proto::counter_value::Value::Byte(value) => {
            let byte = u8::try_from(value).map_err(|_| CodecError::ByteOutOfRange { value })?;
            Ok(ModelCounterValue::Byte(byte))
        }
        proto::counter_value::Value::Uint(value) => Ok(ModelCounterValue::UInt(value)),
    }
}

pub fn encode_state_snapshot_value(
    data_type: &ReplicatedDataType,
    value: SnapshotStateValueRef<'_>,
) -> Result<StateSnapshotWireValue, CodecError> {
    ensure_snapshot_state_value_type(data_type, value.clone())
        .context(InvalidSnapshotValueSnafu)?;
    match (data_type, value) {
        (
            ReplicatedDataType::MonotonicCounter { .. },
            SnapshotStateValueRef::MonotonicCounter(value),
        ) => Ok(StateSnapshotWireValue::MonotonicCounter(
            encode_counter_value(value),
        )),
        (
            ReplicatedDataType::TotalOrderRegister { .. },
            SnapshotStateValueRef::TotalOrderRegister(value),
        ) => Ok(StateSnapshotWireValue::TotalOrderRegister(
            encode_primitive_value(value),
        )),
        (
            ReplicatedDataType::TotalOrderFiniteStateRegister { .. },
            SnapshotStateValueRef::TotalOrderFiniteStateRegister(value),
        ) => Ok(StateSnapshotWireValue::TotalOrderFiniteStateRegister(
            encode_nullable_primitive_value(value),
        )),
        _ => Err(CodecError::InvalidSnapshotValue {
            source: DataModelValueError::InvalidSnapshotValueForType,
        }),
    }
}

pub fn decode_state_snapshot_value(
    data_type: &ReplicatedDataType,
    value: StateSnapshotWireValue,
) -> Result<SnapshotStateValue, CodecError> {
    let decoded = match value {
        StateSnapshotWireValue::MonotonicCounter(value) => {
            let counter_value = decode_counter_value(value)?;
            SnapshotStateValue::MonotonicCounter(counter_value)
        }
        StateSnapshotWireValue::TotalOrderRegister(value) => {
            let register_value = decode_primitive_value(value)?;
            SnapshotStateValue::TotalOrderRegister(register_value)
        }
        StateSnapshotWireValue::TotalOrderFiniteStateRegister(value) => {
            let register_value = decode_nullable_primitive_value(value)?;
            SnapshotStateValue::TotalOrderFiniteStateRegister(register_value)
        }
    };
    ensure_snapshot_state_value_type(data_type, decoded.as_ref())
        .context(InvalidSnapshotValueSnafu)?;
    Ok(decoded)
}

fn encode_date(value: NaiveDate) -> proto::Date {
    proto::Date {
        year: value.year(),
        month: value.month(),
        day: value.day(),
        ..proto::Date::new()
    }
}

fn decode_date(value: proto::Date) -> Result<NaiveDate, CodecError> {
    NaiveDate::from_ymd_opt(value.year, value.month, value.day).context(InvalidDateSnafu {
        year: value.year,
        month: value.month,
        day: value.day,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use flotsync_data_types::schema::{
        NullablePrimitiveType,
        PrimitiveType,
        ReplicatedDataType,
        datamodel::{SnapshotStateValue, SnapshotStateValueRef},
        values::NullablePrimitiveValueArray,
    };
    use std::assert_matches;

    #[test]
    fn shared_value_codecs_roundtrip() {
        let date = NaiveDate::from_ymd_opt(2026, 3, 3).unwrap();

        let primitive = ModelPrimitiveValue::Date(date);
        let primitive_roundtrip =
            decode_primitive_value(encode_primitive_value(primitive.as_ref())).unwrap();
        assert_eq!(primitive_roundtrip, primitive);

        let array = ModelPrimitiveValueArray::Int(vec![-1, 0, 42]);
        let array_roundtrip =
            decode_primitive_array(encode_primitive_array(array.as_ref())).unwrap();
        assert_eq!(array_roundtrip, array);

        let basic = ModelBasicValue::Array(ModelPrimitiveValueArray::Boolean(vec![true, false]));
        let basic_roundtrip = decode_basic_value(encode_basic_value(basic.as_ref())).unwrap();
        assert_eq!(basic_roundtrip, basic);

        let nullable = ModelNullableBasicValue::Null;
        let nullable_roundtrip =
            decode_nullable_basic_value(encode_nullable_basic_value(nullable.as_ref())).unwrap();
        assert_eq!(nullable_roundtrip, nullable);

        let primitive_nullable =
            ModelNullablePrimitiveValue::Value(ModelPrimitiveValue::String("draft".to_owned()));
        let primitive_nullable_roundtrip = decode_nullable_primitive_value(
            encode_nullable_primitive_value(primitive_nullable.as_ref()),
        )
        .unwrap();
        assert_eq!(primitive_nullable_roundtrip, primitive_nullable);

        let counter = ModelCounterValue::Byte(7);
        let counter_roundtrip =
            decode_counter_value(encode_counter_value(counter.as_ref())).unwrap();
        assert_eq!(counter_roundtrip, counter);
    }

    #[test]
    fn state_snapshot_values_roundtrip_with_schema_validation() {
        let counter_type = ReplicatedDataType::MonotonicCounter { small_range: false };
        let register_type = ReplicatedDataType::TotalOrderRegister {
            value_type: PrimitiveType::UInt,
            direction: flotsync_data_types::schema::Direction::Ascending,
        };
        let finite_type = ReplicatedDataType::TotalOrderFiniteStateRegister {
            value_type: NullablePrimitiveType::Nullable(PrimitiveType::String),
            states: NullablePrimitiveValueArray::Nullable {
                values: ModelPrimitiveValueArray::String(vec![
                    "draft".to_owned(),
                    "published".to_owned(),
                ]),
                null_index: 1,
            },
        };

        let counter = SnapshotStateValue::MonotonicCounter(ModelCounterValue::UInt(11));
        let register = SnapshotStateValue::TotalOrderRegister(ModelPrimitiveValue::UInt(9));
        let finite = SnapshotStateValue::TotalOrderFiniteStateRegister(
            ModelNullablePrimitiveValue::Value(ModelPrimitiveValue::String("published".to_owned())),
        );

        let counter_roundtrip = decode_state_snapshot_value(
            &counter_type,
            encode_state_snapshot_value(&counter_type, counter.as_ref()).unwrap(),
        )
        .unwrap();
        let register_roundtrip = decode_state_snapshot_value(
            &register_type,
            encode_state_snapshot_value(&register_type, register.as_ref()).unwrap(),
        )
        .unwrap();
        let finite_roundtrip = decode_state_snapshot_value(
            &finite_type,
            encode_state_snapshot_value(&finite_type, finite.as_ref()).unwrap(),
        )
        .unwrap();

        assert_eq!(counter_roundtrip, counter);
        assert_eq!(register_roundtrip, register);
        assert_eq!(finite_roundtrip, finite);
    }

    #[test]
    fn invalid_state_value_is_rejected() {
        let err = encode_state_snapshot_value(
            &ReplicatedDataType::MonotonicCounter { small_range: true },
            SnapshotStateValueRef::MonotonicCounter(ModelCounterValueRef::UInt(9)),
        )
        .unwrap_err();

        assert_matches!(
            err,
            CodecError::InvalidSnapshotValue {
                source: DataModelValueError::CounterTypeMismatch { .. },
            }
        );
    }
}
