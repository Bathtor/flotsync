use super::*;
use crate::schema::values::NullablePrimitiveValueArray;

/// Ensure that a basic value matches the expected schema type.
pub fn ensure_basic_type(
    expected: &BasicDataType,
    value: &BasicValueRef<'_>,
) -> Result<(), DataModelValueError> {
    ensure!(value.matches_type(expected), BasicTypeMismatchSnafu);
    Ok(())
}

/// Ensure that a nullable basic value matches the expected schema type.
pub fn ensure_nullable_basic_type(
    expected: &NullableBasicDataType,
    value: &NullableBasicValueRef<'_>,
) -> Result<(), DataModelValueError> {
    match value {
        NullableBasicValueRef::Null => {
            ensure!(
                expected.is_nullable(),
                NullabilityMismatchSnafu {
                    expected_nullable: false,
                    actual_nullable: true,
                }
            );
            Ok(())
        }
        NullableBasicValueRef::Value(value) => ensure_basic_type(expected.value_type(), value),
    }
}

/// Ensure that a primitive value kind matches the expected schema type.
pub fn ensure_primitive_type(
    expected: PrimitiveType,
    actual: PrimitiveType,
) -> Result<(), DataModelValueError> {
    ensure!(
        expected == actual,
        PrimitiveTypeMismatchSnafu { expected, actual }
    );
    Ok(())
}

/// Ensure that a primitive array element type matches the expected schema type.
pub fn ensure_primitive_array_type(
    expected: PrimitiveType,
    actual: PrimitiveType,
) -> Result<(), DataModelValueError> {
    ensure_primitive_type(expected, actual)
}

/// Ensure that a counter value matches the schema's range configuration.
pub fn ensure_counter_type(
    expected_small_range: bool,
    actual: CounterValueRef,
) -> Result<(), DataModelValueError> {
    ensure!(
        actual.matches_type(expected_small_range),
        CounterTypeMismatchSnafu {
            expected_small_range,
            actual_small_range: matches!(actual, CounterValueRef::Byte(_)),
        }
    );
    Ok(())
}

/// Ensure that a nullable primitive value matches the expected schema type.
pub fn ensure_nullable_primitive_type(
    expected: NullablePrimitiveType,
    actual: NullablePrimitiveValueRef<'_>,
) -> Result<(), DataModelValueError> {
    match actual {
        NullablePrimitiveValueRef::Null => {
            ensure!(
                expected.is_nullable(),
                NullabilityMismatchSnafu {
                    expected_nullable: false,
                    actual_nullable: true,
                }
            );
            Ok(())
        }
        NullablePrimitiveValueRef::Value(value) => {
            ensure_primitive_type(expected.value_type(), value.value_type())
        }
    }
}

/// Ensure that a finite-state register value matches the schema and allowed state set.
pub fn ensure_finite_state_value(
    value_type: NullablePrimitiveType,
    states: &NullablePrimitiveValueArray,
    value: &NullablePrimitiveValueRef<'_>,
) -> Result<(), DataModelValueError> {
    ensure_nullable_primitive_type(value_type, value.clone())?;
    ensure_primitive_type(value_type.value_type(), states.primitive_type())
        .map_err(|_| DataModelValueError::InvalidFiniteStateSchema)?;
    if value_type.is_nullable() != states.is_nullable() || !states.is_valid() {
        return Err(DataModelValueError::InvalidFiniteStateSchema);
    }

    ensure!(
        finite_state_contains(states, value),
        FiniteStateValueNotInSchemaSnafu
    );
    Ok(())
}

/// Ensure that a snapshot state value matches the schema data type.
pub fn ensure_snapshot_state_value_type(
    data_type: &ReplicatedDataType,
    value: SnapshotStateValueRef<'_>,
) -> Result<(), DataModelValueError> {
    match (data_type, value) {
        (
            ReplicatedDataType::MonotonicCounter { small_range },
            SnapshotStateValueRef::MonotonicCounter(value),
        ) => ensure_counter_type(*small_range, value),
        (
            ReplicatedDataType::TotalOrderRegister { value_type, .. },
            SnapshotStateValueRef::TotalOrderRegister(value),
        ) => ensure_primitive_type(*value_type, value.value_type()),
        (
            ReplicatedDataType::TotalOrderFiniteStateRegister { value_type, states },
            SnapshotStateValueRef::TotalOrderFiniteStateRegister(value),
        ) => ensure_finite_state_value(*value_type, states, &value),
        _ => InvalidSnapshotValueForTypeSnafu.fail(),
    }
}

/// Ensure that a snapshot history node payload matches the schema data type.
pub fn ensure_snapshot_history_value_type(
    data_type: &ReplicatedDataType,
    value: &NullableBasicValueRef<'_>,
) -> Result<(), DataModelValueError> {
    match data_type {
        ReplicatedDataType::LatestValueWins { value_type } => {
            ensure_nullable_basic_type(value_type, value)
        }
        ReplicatedDataType::LinearString => match value {
            NullableBasicValueRef::Value(BasicValueRef::Primitive(PrimitiveValueRef::String(
                _,
            ))) => Ok(()),
            _ => InvalidSnapshotValueForTypeSnafu.fail(),
        },
        ReplicatedDataType::LinearList { value_type } => match value {
            NullableBasicValueRef::Value(BasicValueRef::Array(value)) => {
                ensure_primitive_type(*value_type, value.primitive_type())
            }
            _ => InvalidSnapshotValueForTypeSnafu.fail(),
        },
        _ => InvalidSnapshotValueForTypeSnafu.fail(),
    }
}

fn finite_state_contains(
    states: &NullablePrimitiveValueArray,
    value: &NullablePrimitiveValueRef<'_>,
) -> bool {
    match (states, value) {
        (NullablePrimitiveValueArray::NonNull(states), NullablePrimitiveValueRef::Value(value)) => {
            finite_state_contains_non_null(states, value)
        }
        (
            NullablePrimitiveValueArray::Nullable { values, .. },
            NullablePrimitiveValueRef::Value(value),
        ) => finite_state_contains_non_null(values, value),
        (NullablePrimitiveValueArray::Nullable { .. }, NullablePrimitiveValueRef::Null) => true,
        (NullablePrimitiveValueArray::NonNull(_), NullablePrimitiveValueRef::Null) => false,
    }
}

fn finite_state_contains_non_null(
    states: &PrimitiveValueArray,
    value: &PrimitiveValueRef<'_>,
) -> bool {
    match (states, value) {
        (PrimitiveValueArray::String(values), PrimitiveValueRef::String(value)) => {
            values.iter().any(|candidate| candidate == value)
        }
        (PrimitiveValueArray::UInt(values), PrimitiveValueRef::UInt(value)) => {
            values.contains(value)
        }
        (PrimitiveValueArray::Int(values), PrimitiveValueRef::Int(value)) => values.contains(value),
        (PrimitiveValueArray::Byte(values), PrimitiveValueRef::Byte(value)) => {
            values.contains(value)
        }
        (PrimitiveValueArray::Float(values), PrimitiveValueRef::Float(value)) => {
            values.contains(value)
        }
        (PrimitiveValueArray::Boolean(values), PrimitiveValueRef::Boolean(value)) => {
            values.contains(value)
        }
        (PrimitiveValueArray::Binary(values), PrimitiveValueRef::Binary(value)) => values
            .iter()
            .any(|candidate| candidate.as_slice() == *value),
        (PrimitiveValueArray::Date(values), PrimitiveValueRef::Date(value)) => {
            values.contains(value)
        }
        (PrimitiveValueArray::Timestamp(values), PrimitiveValueRef::Timestamp(value)) => {
            values.contains(value)
        }
        _ => false,
    }
}
