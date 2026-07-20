//! Snapshot adapters and codecs for in-memory field state.

use super::*;

pub(super) struct LatestValueWinsSnapshotSinkAdapter<'a, Id, Value, Sink, Mapper>
where
    Mapper: for<'value> Fn(&'value Value) -> NullableBasicValueRef<'value>,
    for<'value> Sink: SnapshotSink<IdWithIndex<Id>, NullableBasicValueRef<'value>>,
{
    sink: &'a mut Sink,
    map_value: Mapper,
    _marker: PhantomData<fn(Id, Value)>,
}
impl<'a, Id, Value, Sink, Mapper> LatestValueWinsSnapshotSinkAdapter<'a, Id, Value, Sink, Mapper>
where
    Mapper: for<'value> Fn(&'value Value) -> NullableBasicValueRef<'value>,
    for<'value> Sink: SnapshotSink<IdWithIndex<Id>, NullableBasicValueRef<'value>>,
{
    pub(super) fn new(sink: &'a mut Sink, map_value: Mapper) -> Self {
        Self {
            sink,
            map_value,
            _marker: PhantomData,
        }
    }
}
impl<Id, Value, Sink, Mapper, E> SnapshotSink<IdWithIndex<Id>, Value>
    for LatestValueWinsSnapshotSinkAdapter<'_, Id, Value, Sink, Mapper>
where
    Mapper: for<'value> Fn(&'value Value) -> NullableBasicValueRef<'value>,
    for<'value> Sink: SnapshotSink<IdWithIndex<Id>, NullableBasicValueRef<'value>, Error = E>,
{
    type Error = E;

    fn begin(&mut self, header: SnapshotHeader) -> Result<(), Self::Error> {
        self.sink.begin(header)
    }

    fn node(
        &mut self,
        index: usize,
        node: SnapshotNodeRef<'_, IdWithIndex<Id>, Value>,
    ) -> Result<(), Self::Error> {
        let mapped_value = node.value.map(|value| (self.map_value)(value));
        let mapped_node = SnapshotNodeRef {
            id: node.id,
            left: node.left,
            right: node.right,
            deleted: node.deleted,
            value: mapped_value.as_ref(),
        };
        self.sink.node(index, mapped_node)
    }

    fn end(&mut self) -> Result<(), Self::Error> {
        self.sink.end()
    }
}

pub(super) struct LinearListSnapshotSinkAdapter<'a, Id, Value, Sink, Mapper>
where
    Mapper: for<'value> Fn(&'value [Value]) -> PrimitiveValueArrayRef<'value>,
    for<'value> Sink: SnapshotSink<IdWithIndex<Id>, PrimitiveValueArrayRef<'value>>,
{
    sink: &'a mut Sink,
    map_value: Mapper,
    _marker: PhantomData<fn(Id, Value)>,
}
impl<'a, Id, Value, Sink, Mapper> LinearListSnapshotSinkAdapter<'a, Id, Value, Sink, Mapper>
where
    Mapper: for<'value> Fn(&'value [Value]) -> PrimitiveValueArrayRef<'value>,
    for<'value> Sink: SnapshotSink<IdWithIndex<Id>, PrimitiveValueArrayRef<'value>>,
{
    pub(super) fn new(sink: &'a mut Sink, map_value: Mapper) -> Self {
        Self {
            sink,
            map_value,
            _marker: PhantomData,
        }
    }
}
impl<Id, Value, Sink, Mapper, E> SnapshotSink<IdWithIndex<Id>, [Value]>
    for LinearListSnapshotSinkAdapter<'_, Id, Value, Sink, Mapper>
where
    Mapper: for<'value> Fn(&'value [Value]) -> PrimitiveValueArrayRef<'value>,
    for<'value> Sink: SnapshotSink<IdWithIndex<Id>, PrimitiveValueArrayRef<'value>, Error = E>,
{
    type Error = E;

    fn begin(&mut self, header: SnapshotHeader) -> Result<(), Self::Error> {
        self.sink.begin(header)
    }

    fn node(
        &mut self,
        index: usize,
        node: SnapshotNodeRef<'_, IdWithIndex<Id>, [Value]>,
    ) -> Result<(), Self::Error> {
        let mapped_value = node.value.map(|value| (self.map_value)(value));
        let mapped_node = SnapshotNodeRef {
            id: node.id,
            left: node.left,
            right: node.right,
            deleted: node.deleted,
            value: mapped_value.as_ref(),
        };
        self.sink.node(index, mapped_node)
    }

    fn end(&mut self) -> Result<(), Self::Error> {
        self.sink.end()
    }
}

pub(super) fn decode_state_snapshot_field<OperationId>(
    data_type: &ReplicatedDataType,
    value: StateSnapshotFieldValue,
) -> Result<InMemoryFieldState<OperationId>, DataModelValueError> {
    match (data_type, value) {
        (
            ReplicatedDataType::MonotonicCounter { small_range },
            StateSnapshotFieldValue::MonotonicCounter(value),
        ) => {
            ensure_counter_type(*small_range, value.as_ref())?;
            Ok(InMemoryFieldState::MonotonicCounter(value))
        }
        (
            ReplicatedDataType::TotalOrderRegister { value_type, .. },
            StateSnapshotFieldValue::TotalOrderRegister(value),
        ) => {
            ensure_primitive_type(*value_type, value.primitive_type())?;
            Ok(InMemoryFieldState::TotalOrderRegister(value))
        }
        (
            ReplicatedDataType::TotalOrderFiniteStateRegister { value_type, states },
            StateSnapshotFieldValue::TotalOrderFiniteStateRegister(value),
        ) => {
            ensure_finite_state_value(*value_type, states, &value.as_ref())?;
            Ok(InMemoryFieldState::TotalOrderFiniteStateRegister(value))
        }
        _ => Err(DataModelValueError::InvalidSnapshotValueForType),
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "Snapshot decoding mirrors each latest-value-wins variant in the wire format."
)]
pub(super) fn decode_latest_value_wins_snapshot<OperationId, E, I>(
    value_type: &NullableBasicDataType,
    nodes: I,
) -> Result<LinearLatestValueWinsState<OperationId>, SnapshotReadError<InMemoryNodeDecodeError<E>>>
where
    E: snafu::Error + Send + Sync + 'static,
    OperationId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
    I: IntoIterator<
        Item = Result<
            SnapshotNode<IdWithIndex<OperationId>, NullableBasicValue>,
            InMemoryNodeDecodeError<E>,
        >,
    >,
{
    match value_type {
        NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::String)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_required_string,
            ))
            .map(LinearLatestValueWinsState::String)
        }
        NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::UInt)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_required_uint,
            ))
            .map(LinearLatestValueWinsState::UInt)
        }
        NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Int)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_required_int,
            ))
            .map(LinearLatestValueWinsState::Int)
        }
        NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Byte)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_required_byte,
            ))
            .map(LinearLatestValueWinsState::Byte)
        }
        NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Float)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_required_float,
            ))
            .map(LinearLatestValueWinsState::Float)
        }
        NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Boolean)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_required_boolean,
            ))
            .map(LinearLatestValueWinsState::Boolean)
        }
        NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Binary)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_required_binary,
            ))
            .map(LinearLatestValueWinsState::Binary)
        }
        NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Date)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_required_date,
            ))
            .map(LinearLatestValueWinsState::Date)
        }
        NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Timestamp)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_required_timestamp,
            ))
            .map(LinearLatestValueWinsState::Timestamp)
        }
        NullableBasicDataType::NonNull(BasicDataType::Array(array_type)) => {
            match array_type.element_type {
                PrimitiveType::String => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_required_string_array),
                )
                .map(LinearLatestValueWinsState::StringArray),
                PrimitiveType::UInt => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_required_uint_array),
                )
                .map(LinearLatestValueWinsState::UIntArray),
                PrimitiveType::Int => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_required_int_array),
                )
                .map(LinearLatestValueWinsState::IntArray),
                PrimitiveType::Byte => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_required_byte_array),
                )
                .map(LinearLatestValueWinsState::ByteArray),
                PrimitiveType::Float => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_required_float_array),
                )
                .map(LinearLatestValueWinsState::FloatArray),
                PrimitiveType::Boolean => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_required_boolean_array),
                )
                .map(LinearLatestValueWinsState::BooleanArray),
                PrimitiveType::Binary => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_required_binary_array),
                )
                .map(LinearLatestValueWinsState::BinaryArray),
                PrimitiveType::Date => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_required_date_array),
                )
                .map(LinearLatestValueWinsState::DateArray),
                PrimitiveType::Timestamp => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_required_timestamp_array),
                )
                .map(LinearLatestValueWinsState::TimestampArray),
            }
        }
        NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::String)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_optional_string,
            ))
            .map(LinearLatestValueWinsState::NullableString)
        }
        NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::UInt)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_optional_uint,
            ))
            .map(LinearLatestValueWinsState::NullableUInt)
        }
        NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Int)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_optional_int,
            ))
            .map(LinearLatestValueWinsState::NullableInt)
        }
        NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Byte)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_optional_byte,
            ))
            .map(LinearLatestValueWinsState::NullableByte)
        }
        NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Float)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_optional_float,
            ))
            .map(LinearLatestValueWinsState::NullableFloat)
        }
        NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Boolean)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_optional_boolean,
            ))
            .map(LinearLatestValueWinsState::NullableBoolean)
        }
        NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Binary)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_optional_binary,
            ))
            .map(LinearLatestValueWinsState::NullableBinary)
        }
        NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Date)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_optional_date,
            ))
            .map(LinearLatestValueWinsState::NullableDate)
        }
        NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Timestamp)) => {
            LinearLatestValueWins::from_snapshot_nodes(map_snapshot_nodes_value(
                nodes,
                decode_optional_timestamp,
            ))
            .map(LinearLatestValueWinsState::NullableTimestamp)
        }
        NullableBasicDataType::Nullable(BasicDataType::Array(array_type)) => {
            match array_type.element_type {
                PrimitiveType::String => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_optional_string_array),
                )
                .map(LinearLatestValueWinsState::NullableStringArray),
                PrimitiveType::UInt => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_optional_uint_array),
                )
                .map(LinearLatestValueWinsState::NullableUIntArray),
                PrimitiveType::Int => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_optional_int_array),
                )
                .map(LinearLatestValueWinsState::NullableIntArray),
                PrimitiveType::Byte => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_optional_byte_array),
                )
                .map(LinearLatestValueWinsState::NullableByteArray),
                PrimitiveType::Float => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_optional_float_array),
                )
                .map(LinearLatestValueWinsState::NullableFloatArray),
                PrimitiveType::Boolean => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_optional_boolean_array),
                )
                .map(LinearLatestValueWinsState::NullableBooleanArray),
                PrimitiveType::Binary => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_optional_binary_array),
                )
                .map(LinearLatestValueWinsState::NullableBinaryArray),
                PrimitiveType::Date => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_optional_date_array),
                )
                .map(LinearLatestValueWinsState::NullableDateArray),
                PrimitiveType::Timestamp => LinearLatestValueWins::from_snapshot_nodes(
                    map_snapshot_nodes_value(nodes, decode_optional_timestamp_array),
                )
                .map(LinearLatestValueWinsState::NullableTimestampArray),
            }
        }
    }
}

pub(super) fn decode_linear_list_snapshot<OperationId, E, I>(
    value_type: PrimitiveType,
    nodes: I,
) -> Result<LinearListState<OperationId>, SnapshotReadError<InMemoryNodeDecodeError<E>>>
where
    E: snafu::Error + Send + Sync + 'static,
    OperationId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
    I: IntoIterator<
        Item = Result<
            SnapshotNode<IdWithIndex<OperationId>, PrimitiveValueArray>,
            InMemoryNodeDecodeError<E>,
        >,
    >,
{
    match value_type {
        PrimitiveType::String => {
            LinearList::from_snapshot_nodes(map_snapshot_nodes_value(nodes, decode_list_string))
                .map(LinearListState::String)
        }
        PrimitiveType::UInt => {
            LinearList::from_snapshot_nodes(map_snapshot_nodes_value(nodes, decode_list_uint))
                .map(LinearListState::UInt)
        }
        PrimitiveType::Int => {
            LinearList::from_snapshot_nodes(map_snapshot_nodes_value(nodes, decode_list_int))
                .map(LinearListState::Int)
        }
        PrimitiveType::Byte => {
            LinearList::from_snapshot_nodes(map_snapshot_nodes_value(nodes, decode_list_byte))
                .map(LinearListState::Byte)
        }
        PrimitiveType::Float => {
            LinearList::from_snapshot_nodes(map_snapshot_nodes_value(nodes, decode_list_float))
                .map(LinearListState::Float)
        }
        PrimitiveType::Boolean => {
            LinearList::from_snapshot_nodes(map_snapshot_nodes_value(nodes, decode_list_boolean))
                .map(LinearListState::Boolean)
        }
        PrimitiveType::Binary => {
            LinearList::from_snapshot_nodes(map_snapshot_nodes_value(nodes, decode_list_binary))
                .map(LinearListState::Binary)
        }
        PrimitiveType::Date => {
            LinearList::from_snapshot_nodes(map_snapshot_nodes_value(nodes, decode_list_date))
                .map(LinearListState::Date)
        }
        PrimitiveType::Timestamp => {
            LinearList::from_snapshot_nodes(map_snapshot_nodes_value(nodes, decode_list_timestamp))
                .map(LinearListState::Timestamp)
        }
    }
}

pub(super) fn map_snapshot_nodes_value<Id, InputValue, OutputValue, E, I, F>(
    nodes: I,
    map_value: F,
) -> impl Iterator<Item = Result<SnapshotNode<Id, OutputValue>, InMemoryNodeDecodeError<E>>>
where
    E: snafu::Error + Send + Sync + 'static,
    I: IntoIterator<Item = Result<SnapshotNode<Id, InputValue>, InMemoryNodeDecodeError<E>>>,
    F: Fn(InputValue) -> Result<OutputValue, DataModelValueError> + Copy,
{
    nodes.into_iter().map(move |entry| {
        let node = entry?;
        let value = match node.value {
            Some(value) => Some(map_value(value).context(InvalidNodeValueSnafu)?),
            None => None,
        };
        Ok(SnapshotNode {
            id: node.id,
            left: node.left,
            right: node.right,
            deleted: node.deleted,
            value,
        })
    })
}

macro_rules! define_nullable_basic_converters {
    ($required_fn:ident, $optional_fn:ident, $out_ty:ty, $pattern:pat => $mapped:expr) => {
        pub(super) fn $required_fn(
            value: NullableBasicValue,
        ) -> Result<$out_ty, DataModelValueError> {
            match value {
                NullableBasicValue::Null => Err(DataModelValueError::NullabilityMismatch {
                    expected_nullable: false,
                    actual_nullable: true,
                }),
                NullableBasicValue::Value(value) => match value {
                    $pattern => Ok($mapped),
                    _ => Err(DataModelValueError::BasicTypeMismatch),
                },
            }
        }

        pub(super) fn $optional_fn(
            value: NullableBasicValue,
        ) -> Result<Option<$out_ty>, DataModelValueError> {
            match value {
                NullableBasicValue::Null => Ok(None),
                NullableBasicValue::Value(value) => match value {
                    $pattern => Ok(Some($mapped)),
                    _ => Err(DataModelValueError::BasicTypeMismatch),
                },
            }
        }
    };
}

define_nullable_basic_converters!(
    decode_required_string,
    decode_optional_string,
    String,
    BasicValue::Primitive(PrimitiveValue::String(value)) => value
);
define_nullable_basic_converters!(
    decode_required_uint,
    decode_optional_uint,
    u64,
    BasicValue::Primitive(PrimitiveValue::UInt(value)) => value
);
define_nullable_basic_converters!(
    decode_required_int,
    decode_optional_int,
    i64,
    BasicValue::Primitive(PrimitiveValue::Int(value)) => value
);
define_nullable_basic_converters!(
    decode_required_byte,
    decode_optional_byte,
    u8,
    BasicValue::Primitive(PrimitiveValue::Byte(value)) => value
);
define_nullable_basic_converters!(
    decode_required_float,
    decode_optional_float,
    OrderedFloat<f64>,
    BasicValue::Primitive(PrimitiveValue::Float(value)) => value
);
define_nullable_basic_converters!(
    decode_required_boolean,
    decode_optional_boolean,
    bool,
    BasicValue::Primitive(PrimitiveValue::Boolean(value)) => value
);
define_nullable_basic_converters!(
    decode_required_binary,
    decode_optional_binary,
    Vec<u8>,
    BasicValue::Primitive(PrimitiveValue::Binary(value)) => value
);
define_nullable_basic_converters!(
    decode_required_date,
    decode_optional_date,
    NaiveDate,
    BasicValue::Primitive(PrimitiveValue::Date(value)) => value
);
define_nullable_basic_converters!(
    decode_required_timestamp,
    decode_optional_timestamp,
    UnixTimestamp,
    BasicValue::Primitive(PrimitiveValue::Timestamp(value)) => value
);
define_nullable_basic_converters!(
    decode_required_string_array,
    decode_optional_string_array,
    Vec<String>,
    BasicValue::Array(PrimitiveValueArray::String(value)) => value
);
define_nullable_basic_converters!(
    decode_required_uint_array,
    decode_optional_uint_array,
    Vec<u64>,
    BasicValue::Array(PrimitiveValueArray::UInt(value)) => value
);
define_nullable_basic_converters!(
    decode_required_int_array,
    decode_optional_int_array,
    Vec<i64>,
    BasicValue::Array(PrimitiveValueArray::Int(value)) => value
);
define_nullable_basic_converters!(
    decode_required_byte_array,
    decode_optional_byte_array,
    Vec<u8>,
    BasicValue::Array(PrimitiveValueArray::Byte(value)) => value
);
define_nullable_basic_converters!(
    decode_required_float_array,
    decode_optional_float_array,
    Vec<OrderedFloat<f64>>,
    BasicValue::Array(PrimitiveValueArray::Float(value)) => value
);
define_nullable_basic_converters!(
    decode_required_boolean_array,
    decode_optional_boolean_array,
    Vec<bool>,
    BasicValue::Array(PrimitiveValueArray::Boolean(value)) => value
);
define_nullable_basic_converters!(
    decode_required_binary_array,
    decode_optional_binary_array,
    Vec<Vec<u8>>,
    BasicValue::Array(PrimitiveValueArray::Binary(value)) => value
);
define_nullable_basic_converters!(
    decode_required_date_array,
    decode_optional_date_array,
    Vec<NaiveDate>,
    BasicValue::Array(PrimitiveValueArray::Date(value)) => value
);
define_nullable_basic_converters!(
    decode_required_timestamp_array,
    decode_optional_timestamp_array,
    Vec<UnixTimestamp>,
    BasicValue::Array(PrimitiveValueArray::Timestamp(value)) => value
);

pub(super) fn decode_list_string(
    value: PrimitiveValueArray,
) -> Result<Vec<String>, DataModelValueError> {
    match value {
        PrimitiveValueArray::String(value) => Ok(value),
        actual => Err(DataModelValueError::PrimitiveTypeMismatch {
            expected: PrimitiveType::String,
            actual: actual.primitive_type(),
        }),
    }
}
pub(super) fn decode_list_uint(
    value: PrimitiveValueArray,
) -> Result<Vec<u64>, DataModelValueError> {
    match value {
        PrimitiveValueArray::UInt(value) => Ok(value),
        actual => Err(DataModelValueError::PrimitiveTypeMismatch {
            expected: PrimitiveType::UInt,
            actual: actual.primitive_type(),
        }),
    }
}
pub(super) fn decode_list_int(value: PrimitiveValueArray) -> Result<Vec<i64>, DataModelValueError> {
    match value {
        PrimitiveValueArray::Int(value) => Ok(value),
        actual => Err(DataModelValueError::PrimitiveTypeMismatch {
            expected: PrimitiveType::Int,
            actual: actual.primitive_type(),
        }),
    }
}
pub(super) fn decode_list_byte(value: PrimitiveValueArray) -> Result<Vec<u8>, DataModelValueError> {
    match value {
        PrimitiveValueArray::Byte(value) => Ok(value),
        actual => Err(DataModelValueError::PrimitiveTypeMismatch {
            expected: PrimitiveType::Byte,
            actual: actual.primitive_type(),
        }),
    }
}
pub(super) fn decode_list_float(
    value: PrimitiveValueArray,
) -> Result<Vec<OrderedFloat<f64>>, DataModelValueError> {
    match value {
        PrimitiveValueArray::Float(value) => Ok(value),
        actual => Err(DataModelValueError::PrimitiveTypeMismatch {
            expected: PrimitiveType::Float,
            actual: actual.primitive_type(),
        }),
    }
}
pub(super) fn decode_list_boolean(
    value: PrimitiveValueArray,
) -> Result<Vec<bool>, DataModelValueError> {
    match value {
        PrimitiveValueArray::Boolean(value) => Ok(value),
        actual => Err(DataModelValueError::PrimitiveTypeMismatch {
            expected: PrimitiveType::Boolean,
            actual: actual.primitive_type(),
        }),
    }
}
pub(super) fn decode_list_binary(
    value: PrimitiveValueArray,
) -> Result<Vec<Vec<u8>>, DataModelValueError> {
    match value {
        PrimitiveValueArray::Binary(value) => Ok(value),
        actual => Err(DataModelValueError::PrimitiveTypeMismatch {
            expected: PrimitiveType::Binary,
            actual: actual.primitive_type(),
        }),
    }
}
pub(super) fn decode_list_date(
    value: PrimitiveValueArray,
) -> Result<Vec<NaiveDate>, DataModelValueError> {
    match value {
        PrimitiveValueArray::Date(value) => Ok(value),
        actual => Err(DataModelValueError::PrimitiveTypeMismatch {
            expected: PrimitiveType::Date,
            actual: actual.primitive_type(),
        }),
    }
}
pub(super) fn decode_list_timestamp(
    value: PrimitiveValueArray,
) -> Result<Vec<UnixTimestamp>, DataModelValueError> {
    match value {
        PrimitiveValueArray::Timestamp(value) => Ok(value),
        actual => Err(DataModelValueError::PrimitiveTypeMismatch {
            expected: PrimitiveType::Timestamp,
            actual: actual.primitive_type(),
        }),
    }
}
