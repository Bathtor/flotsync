#![allow(
    clippy::needless_pass_by_value,
    reason = "Codec helpers consistently accept owned protobuf messages and small value-ref enums."
)]

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

#[must_use]
pub fn encode_update_id(id: UpdateId) -> proto::HistoryId {
    proto::HistoryId {
        version: id.version,
        node_index: id.node_index,
        chunk_index: 0,
        ..proto::HistoryId::default()
    }
}

/// # Errors
///
/// See `CodecError` for failure conditions.
pub fn decode_update_id(id: proto::HistoryId) -> Result<UpdateId, CodecError> {
    Ok(UpdateId {
        version: id.version,
        node_index: id.node_index,
    })
}

#[must_use]
pub fn encode_indexed_update_id(id: &UpdateIdWithIndex) -> proto::HistoryId {
    proto::HistoryId {
        version: id.id.version,
        node_index: id.id.node_index,
        chunk_index: id.index,
        ..proto::HistoryId::default()
    }
}

/// # Errors
///
/// See `CodecError` for failure conditions.
pub fn decode_indexed_update_id(id: proto::HistoryId) -> Result<UpdateIdWithIndex, CodecError> {
    Ok(IdWithIndex {
        id: UpdateId {
            version: id.version,
            node_index: id.node_index,
        },
        index: id.chunk_index,
    })
}

#[must_use]
pub fn encode_primitive_value(value: ModelPrimitiveValueRef<'_>) -> proto::PrimitiveValue {
    let mut encoded = proto::PrimitiveValue::default();
    let value_enum = match value {
        ModelPrimitiveValueRef::String(value) => {
            proto::primitive_value::Value::String(value.to_owned())
        }
        ModelPrimitiveValueRef::UInt(value) => proto::primitive_value::Value::Uint(value),
        ModelPrimitiveValueRef::Int(value) => proto::primitive_value::Value::Int(value),
        ModelPrimitiveValueRef::Byte(value) => {
            proto::primitive_value::Value::Byte(u32::from(value))
        }
        ModelPrimitiveValueRef::Float(value) => proto::primitive_value::Value::Float(value.0),
        ModelPrimitiveValueRef::Boolean(value) => proto::primitive_value::Value::Boolean(value),
        ModelPrimitiveValueRef::Binary(value) => {
            proto::primitive_value::Value::Binary(value.to_vec())
        }
        ModelPrimitiveValueRef::Date(value) => {
            proto::primitive_value::Value::Date(Box::new(encode_date(value)))
        }
        ModelPrimitiveValueRef::Timestamp(value) => proto::primitive_value::Value::Timestamp(value),
    };
    encoded.value = Some(value_enum);
    encoded
}

/// # Errors
///
/// See `CodecError` for failure conditions.
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
            let date = decode_date(&value)?;
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
    let value_enum = match value {
        ModelPrimitiveValueArrayRef::String(values) => {
            let message = proto::StringArrayValue {
                values: values.to_vec(),
                ..proto::StringArrayValue::default()
            };
            proto::primitive_array_value::Value::String(Box::new(message))
        }
        ModelPrimitiveValueArrayRef::UInt(values) => {
            let message = proto::UIntArrayValue {
                values: values.to_vec(),
                ..proto::UIntArrayValue::default()
            };
            proto::primitive_array_value::Value::Uint(Box::new(message))
        }
        ModelPrimitiveValueArrayRef::Int(values) => {
            let message = proto::IntArrayValue {
                values: values.to_vec(),
                ..proto::IntArrayValue::default()
            };
            proto::primitive_array_value::Value::Int(Box::new(message))
        }
        ModelPrimitiveValueArrayRef::Byte(values) => {
            let message = proto::ByteArrayValue {
                values: values.to_vec(),
                ..proto::ByteArrayValue::default()
            };
            proto::primitive_array_value::Value::Byte(Box::new(message))
        }
        ModelPrimitiveValueArrayRef::Float(values) => {
            let message = proto::FloatArrayValue {
                values: values.iter().map(|value| value.0).collect(),
                ..proto::FloatArrayValue::default()
            };
            proto::primitive_array_value::Value::Float(Box::new(message))
        }
        ModelPrimitiveValueArrayRef::Boolean(values) => {
            let message = proto::BooleanArrayValue {
                values: values.to_vec(),
                ..proto::BooleanArrayValue::default()
            };
            proto::primitive_array_value::Value::Boolean(Box::new(message))
        }
        ModelPrimitiveValueArrayRef::Binary(values) => {
            let message = proto::BinaryArrayValue {
                values: values.to_vec(),
                ..proto::BinaryArrayValue::default()
            };
            proto::primitive_array_value::Value::Binary(Box::new(message))
        }
        ModelPrimitiveValueArrayRef::Date(values) => {
            let message = proto::DateArrayValue {
                values: values.iter().copied().map(encode_date).collect(),
                ..proto::DateArrayValue::default()
            };
            proto::primitive_array_value::Value::Date(Box::new(message))
        }
        ModelPrimitiveValueArrayRef::Timestamp(values) => {
            let message = proto::TimestampArrayValue {
                values: values.to_vec(),
                ..proto::TimestampArrayValue::default()
            };
            proto::primitive_array_value::Value::Timestamp(Box::new(message))
        }
    };
    proto::PrimitiveArrayValue {
        value: Some(value_enum),
        ..proto::PrimitiveArrayValue::default()
    }
}

/// # Errors
///
/// See `CodecError` for failure conditions.
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
            let dates: Vec<NaiveDate> = values.values.iter().map(decode_date).try_collect()?;
            Ok(ModelPrimitiveValueArray::Date(dates))
        }
        proto::primitive_array_value::Value::Timestamp(values) => {
            Ok(ModelPrimitiveValueArray::Timestamp(values.values))
        }
    }
}

#[must_use]
pub fn encode_basic_value(value: ModelBasicValueRef<'_>) -> proto::BasicValue {
    let mut encoded = proto::BasicValue::default();
    match value {
        ModelBasicValueRef::Primitive(value) => {
            encoded.value = Some(proto::basic_value::Value::Primitive(Box::new(
                encode_primitive_value(value),
            )));
        }
        ModelBasicValueRef::Array(value) => {
            encoded.value = Some(proto::basic_value::Value::Array(Box::new(
                encode_primitive_array(value),
            )));
        }
    }
    encoded
}

/// # Errors
///
/// See `CodecError` for failure conditions.
pub fn decode_basic_value(mut value: proto::BasicValue) -> Result<ModelBasicValue, CodecError> {
    let value = value.value.take().context(MissingOneofSnafu {
        name: "BasicValue.value",
    })?;
    match value {
        proto::basic_value::Value::Primitive(value) => {
            decode_primitive_value(*value).map(ModelBasicValue::Primitive)
        }
        proto::basic_value::Value::Array(value) => {
            decode_primitive_array(*value).map(ModelBasicValue::Array)
        }
    }
}

#[must_use]
pub fn encode_nullable_basic_value(
    value: ModelNullableBasicValueRef<'_>,
) -> proto::NullableBasicValue {
    let mut encoded = proto::NullableBasicValue::default();
    let value_enum = match value {
        ModelNullableBasicValueRef::Null => {
            proto::nullable_basic_value::Value::Null(Box::default())
        }
        ModelNullableBasicValueRef::Value(ModelBasicValueRef::Primitive(value)) => {
            proto::nullable_basic_value::Value::Primitive(Box::new(encode_primitive_value(value)))
        }
        ModelNullableBasicValueRef::Value(ModelBasicValueRef::Array(value)) => {
            proto::nullable_basic_value::Value::Array(Box::new(encode_primitive_array(value)))
        }
    };
    encoded.value = Some(value_enum);
    encoded
}

/// # Errors
///
/// See `CodecError` for failure conditions.
pub fn decode_nullable_basic_value(
    mut value: proto::NullableBasicValue,
) -> Result<ModelNullableBasicValue, CodecError> {
    let value = value.value.take().context(MissingOneofSnafu {
        name: "NullableBasicValue.value",
    })?;
    match value {
        proto::nullable_basic_value::Value::Null(_) => Ok(ModelNullableBasicValue::Null),
        proto::nullable_basic_value::Value::Primitive(value) => decode_primitive_value(*value)
            .map(ModelBasicValue::Primitive)
            .map(ModelNullableBasicValue::Value),
        proto::nullable_basic_value::Value::Array(value) => decode_primitive_array(*value)
            .map(ModelBasicValue::Array)
            .map(ModelNullableBasicValue::Value),
    }
}

#[must_use]
pub fn encode_nullable_primitive_value(
    value: ModelNullablePrimitiveValueRef<'_>,
) -> proto::NullablePrimitiveValue {
    let mut encoded = proto::NullablePrimitiveValue::default();
    let value_enum = match value {
        ModelNullablePrimitiveValueRef::Null => {
            proto::nullable_primitive_value::Value::Null(Box::default())
        }
        ModelNullablePrimitiveValueRef::Value(value) => {
            proto::nullable_primitive_value::Value::Primitive(Box::new(encode_primitive_value(
                value,
            )))
        }
    };
    encoded.value = Some(value_enum);
    encoded
}

/// # Errors
///
/// See `CodecError` for failure conditions.
pub fn decode_nullable_primitive_value(
    mut value: proto::NullablePrimitiveValue,
) -> Result<ModelNullablePrimitiveValue, CodecError> {
    let value = value.value.take().context(MissingOneofSnafu {
        name: "NullablePrimitiveValue.value",
    })?;
    match value {
        proto::nullable_primitive_value::Value::Null(_) => Ok(ModelNullablePrimitiveValue::Null),
        proto::nullable_primitive_value::Value::Primitive(value) => {
            decode_primitive_value(*value).map(ModelNullablePrimitiveValue::Value)
        }
    }
}

#[must_use]
pub fn encode_counter_value(value: ModelCounterValueRef) -> proto::CounterValue {
    let mut encoded = proto::CounterValue::default();
    let value_enum = match value {
        ModelCounterValueRef::Byte(value) => proto::counter_value::Value::Byte(u32::from(value)),
        ModelCounterValueRef::UInt(value) => proto::counter_value::Value::Uint(value),
    };
    encoded.value = Some(value_enum);
    encoded
}

/// # Errors
///
/// See `CodecError` for failure conditions.
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

/// # Errors
///
/// See `CodecError` for failure conditions.
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

/// # Errors
///
/// See `CodecError` for failure conditions.
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
        ..proto::Date::default()
    }
}

pub(crate) fn decode_date(value: &proto::Date) -> Result<NaiveDate, CodecError> {
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
