//! Test-only codecs for snapshot payload validation.

#[cfg(test)]
use super::contracts::{HistorySnapshotNodeValue, HistorySnapshotNodeValueRef};
use super::*;

/// Visitor used by serializers to encode one history node payload at a time.
#[cfg(test)]
pub(super) trait HistorySnapshotNodeValueEncoder {
    type Error;

    fn visit_latest_value_wins_node(
        &mut self,
        value_type: &NullableBasicDataType,
        value: NullableBasicValueRef<'_>,
    ) -> Result<(), Self::Error>;

    fn visit_linear_string_node(&mut self, value: &str) -> Result<(), Self::Error>;

    fn visit_linear_list_node(
        &mut self,
        value_type: PrimitiveType,
        values: PrimitiveValueArrayRef<'_>,
    ) -> Result<(), Self::Error>;
}

/// Decoder used to lazily decode one history node payload at a time.
#[cfg(test)]
pub(super) trait HistorySnapshotNodeValueDecoder {
    type Error;

    fn decode_latest_value_wins_node(
        &mut self,
        value_type: &NullableBasicDataType,
    ) -> Result<NullableBasicValue, Self::Error>;

    fn decode_linear_string_node(&mut self) -> Result<String, Self::Error>;

    fn decode_linear_list_node(
        &mut self,
        value_type: PrimitiveType,
    ) -> Result<PrimitiveValueArray, Self::Error>;
}

/// Visitor used by serializers to encode one state snapshot value at a time.
pub(super) trait StateSnapshotFieldValueEncoder {
    type Error;

    fn visit_monotonic_counter(
        &mut self,
        small_range: bool,
        value: CounterValueRef,
    ) -> Result<(), Self::Error>;

    fn visit_total_order_register(
        &mut self,
        value_type: PrimitiveType,
        value: PrimitiveValueRef<'_>,
    ) -> Result<(), Self::Error>;

    fn visit_total_order_finite_state_register(
        &mut self,
        value_type: NullablePrimitiveType,
        states: &NullablePrimitiveValueArray,
        value: NullablePrimitiveValueRef<'_>,
    ) -> Result<(), Self::Error>;
}

/// Decoder used to lazily decode one state snapshot value at a time.
#[cfg(test)]
pub(super) trait StateSnapshotFieldValueDecoder {
    type Error;

    fn decode_monotonic_counter(&mut self, small_range: bool) -> Result<CounterValue, Self::Error>;

    fn decode_total_order_register(
        &mut self,
        value_type: PrimitiveType,
    ) -> Result<PrimitiveValue, Self::Error>;

    fn decode_total_order_finite_state_register(
        &mut self,
        value_type: NullablePrimitiveType,
        states: &NullablePrimitiveValueArray,
    ) -> Result<NullablePrimitiveValue, Self::Error>;
}
/// Dispatch + validate one history node payload against its schema data type before visiting.
#[cfg(test)]
pub(super) fn encode_snapshot_node_value<V>(
    visitor: &mut V,
    data_type: &ReplicatedDataType,
    value: HistorySnapshotNodeValueRef<'_>,
) -> Result<(), VisitError<V::Error>>
where
    V: HistorySnapshotNodeValueEncoder,
    V::Error: snafu::Error + Send + Sync + 'static,
{
    match (data_type, value) {
        (
            ReplicatedDataType::LatestValueWins { value_type },
            HistorySnapshotNodeValueRef::LatestValueWins(v),
        ) => {
            ensure_nullable_basic_type(value_type, &v)
                .map_err(|source| VisitError::InvalidVisitedValue { source })?;
            visitor
                .visit_latest_value_wins_node(value_type, v)
                .map_err(|source| VisitError::VisitorSource { source })
        }
        (ReplicatedDataType::LinearString, HistorySnapshotNodeValueRef::LinearString(v)) => visitor
            .visit_linear_string_node(v)
            .map_err(|source| VisitError::VisitorSource { source }),
        (
            ReplicatedDataType::LinearList { value_type },
            HistorySnapshotNodeValueRef::LinearList(v),
        ) => {
            ensure_primitive_array_type(*value_type, v.primitive_type())
                .map_err(|source| VisitError::InvalidVisitedValue { source })?;
            visitor
                .visit_linear_list_node(*value_type, v)
                .map_err(|source| VisitError::VisitorSource { source })
        }
        _ => Err(VisitError::InvalidVisitedValue {
            source: DataModelValueError::InvalidSnapshotValueForType,
        }),
    }
}

/// Decode + validate one history node payload lazily for the provided schema data type.
#[cfg(test)]
pub(super) fn decode_snapshot_node_value<D>(
    decoder: &mut D,
    data_type: &ReplicatedDataType,
) -> Result<HistorySnapshotNodeValue, DecodeError<D::Error>>
where
    D: HistorySnapshotNodeValueDecoder,
    D::Error: snafu::Error + Send + Sync + 'static,
{
    match data_type {
        ReplicatedDataType::LatestValueWins { value_type } => {
            let value = decoder
                .decode_latest_value_wins_node(value_type)
                .map_err(|source| DecodeError::DecoderSource { source })?;
            ensure_nullable_basic_type(value_type, &value.as_ref())
                .map_err(|source| DecodeError::InvalidDecodedValue { source })?;
            Ok(HistorySnapshotNodeValue::LatestValueWins(value))
        }
        ReplicatedDataType::LinearString => decoder
            .decode_linear_string_node()
            .map(HistorySnapshotNodeValue::LinearString)
            .map_err(|source| DecodeError::DecoderSource { source }),
        ReplicatedDataType::LinearList { value_type } => {
            let values = decoder
                .decode_linear_list_node(*value_type)
                .map_err(|source| DecodeError::DecoderSource { source })?;
            ensure_primitive_array_type(*value_type, values.primitive_type())
                .map_err(|source| DecodeError::InvalidDecodedValue { source })?;
            Ok(HistorySnapshotNodeValue::LinearList(values))
        }
        ReplicatedDataType::MonotonicCounter { .. }
        | ReplicatedDataType::TotalOrderRegister { .. }
        | ReplicatedDataType::TotalOrderFiniteStateRegister { .. } => {
            Err(DecodeError::InvalidDecodedValue {
                source: DataModelValueError::InvalidSnapshotValueForType,
            })
        }
    }
}

/// Dispatch + validate one state snapshot value against its schema data type before visiting.
pub(super) fn encode_snapshot_state_value<V>(
    visitor: &mut V,
    data_type: &ReplicatedDataType,
    value: StateSnapshotFieldValueRef<'_>,
) -> Result<(), VisitError<V::Error>>
where
    V: StateSnapshotFieldValueEncoder,
    V::Error: snafu::Error + Send + Sync + 'static,
{
    match (data_type, value) {
        (
            ReplicatedDataType::MonotonicCounter { small_range },
            StateSnapshotFieldValueRef::MonotonicCounter(v),
        ) => {
            ensure_counter_type(*small_range, v)
                .map_err(|source| VisitError::InvalidVisitedValue { source })?;
            visitor
                .visit_monotonic_counter(*small_range, v)
                .map_err(|source| VisitError::VisitorSource { source })
        }
        (
            ReplicatedDataType::TotalOrderRegister { value_type, .. },
            StateSnapshotFieldValueRef::TotalOrderRegister(v),
        ) => {
            ensure_primitive_type(*value_type, v.value_type())
                .map_err(|source| VisitError::InvalidVisitedValue { source })?;
            visitor
                .visit_total_order_register(*value_type, v)
                .map_err(|source| VisitError::VisitorSource { source })
        }
        (
            ReplicatedDataType::TotalOrderFiniteStateRegister { value_type, states },
            StateSnapshotFieldValueRef::TotalOrderFiniteStateRegister(v),
        ) => {
            ensure_finite_state_value(*value_type, states, &v)
                .map_err(|source| VisitError::InvalidVisitedValue { source })?;
            visitor
                .visit_total_order_finite_state_register(*value_type, states, v)
                .map_err(|source| VisitError::VisitorSource { source })
        }
        _ => Err(VisitError::InvalidVisitedValue {
            source: DataModelValueError::InvalidSnapshotValueForType,
        }),
    }
}

/// Decode + validate one state snapshot value lazily for the provided schema data type.
#[cfg(test)]
pub(super) fn decode_snapshot_state_value<D>(
    decoder: &mut D,
    data_type: &ReplicatedDataType,
) -> Result<StateSnapshotFieldValue, DecodeError<D::Error>>
where
    D: StateSnapshotFieldValueDecoder,
    D::Error: snafu::Error + Send + Sync + 'static,
{
    match data_type {
        ReplicatedDataType::MonotonicCounter { small_range } => {
            let value = decoder
                .decode_monotonic_counter(*small_range)
                .map_err(|source| DecodeError::DecoderSource { source })?;
            ensure_counter_type(*small_range, value.as_ref())
                .map_err(|source| DecodeError::InvalidDecodedValue { source })?;
            Ok(StateSnapshotFieldValue::MonotonicCounter(value))
        }
        ReplicatedDataType::TotalOrderRegister { value_type, .. } => {
            let value = decoder
                .decode_total_order_register(*value_type)
                .map_err(|source| DecodeError::DecoderSource { source })?;
            ensure_primitive_type(*value_type, value.primitive_type())
                .map_err(|source| DecodeError::InvalidDecodedValue { source })?;
            Ok(StateSnapshotFieldValue::TotalOrderRegister(value))
        }
        ReplicatedDataType::TotalOrderFiniteStateRegister { value_type, states } => {
            let value = decoder
                .decode_total_order_finite_state_register(*value_type, states)
                .map_err(|source| DecodeError::DecoderSource { source })?;
            ensure_finite_state_value(*value_type, states, &value.as_ref())
                .map_err(|source| DecodeError::InvalidDecodedValue { source })?;
            Ok(StateSnapshotFieldValue::TotalOrderFiniteStateRegister(
                value,
            ))
        }
        ReplicatedDataType::LatestValueWins { .. }
        | ReplicatedDataType::LinearString
        | ReplicatedDataType::LinearList { .. } => Err(DecodeError::InvalidDecodedValue {
            source: DataModelValueError::InvalidSnapshotValueForType,
        }),
    }
}
pub(super) fn validate_snapshot_state_value_for_type(
    data_type: &ReplicatedDataType,
    value: StateSnapshotFieldValueRef<'_>,
) -> Result<(), DataModelValueError> {
    let mut visitor = NoopStateSnapshotFieldValueEncoder;
    match encode_snapshot_state_value(&mut visitor, data_type, value) {
        Ok(()) => Ok(()),
        Err(VisitError::InvalidVisitedValue { source }) => Err(source),
        Err(VisitError::VisitorSource { source: never }) => match never {},
    }
}
struct NoopStateSnapshotFieldValueEncoder;
impl StateSnapshotFieldValueEncoder for NoopStateSnapshotFieldValueEncoder {
    type Error = Infallible;

    fn visit_monotonic_counter(
        &mut self,
        _small_range: bool,
        _value: CounterValueRef,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn visit_total_order_register(
        &mut self,
        _value_type: PrimitiveType,
        _value: PrimitiveValueRef<'_>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn visit_total_order_finite_state_register(
        &mut self,
        _value_type: NullablePrimitiveType,
        _states: &NullablePrimitiveValueArray,
        _value: NullablePrimitiveValueRef<'_>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}
pub(super) fn is_history_data_type(data_type: &ReplicatedDataType) -> bool {
    matches!(
        data_type,
        ReplicatedDataType::LatestValueWins { .. }
            | ReplicatedDataType::LinearString
            | ReplicatedDataType::LinearList { .. }
    )
}
