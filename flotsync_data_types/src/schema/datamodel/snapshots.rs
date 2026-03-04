#[cfg(test)]
use super::validation::{ensure_nullable_basic_type, ensure_primitive_array_type};
use super::{
    validation::{ensure_counter_type, ensure_finite_state_value, ensure_primitive_type},
    *,
};
use crate::{
    IdWithIndex,
    snapshot::{SnapshotNode, SnapshotSink},
};
use std::{collections::HashSet, convert::Infallible, marker::PhantomData};

/// Snapshot node payload shapes for history-based CRDT snapshots.
#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
enum SnapshotNodeValue {
    LatestValueWins(NullableBasicValue),
    LinearString(String),
    LinearList(PrimitiveValueArray),
}
#[cfg(test)]
impl SnapshotNodeValue {
    pub fn as_ref(&self) -> SnapshotNodeValueRef<'_> {
        match self {
            Self::LatestValueWins(value) => SnapshotNodeValueRef::LatestValueWins(value.as_ref()),
            Self::LinearString(value) => SnapshotNodeValueRef::LinearString(value.as_str()),
            Self::LinearList(values) => SnapshotNodeValueRef::LinearList(values.as_ref()),
        }
    }
}

/// Borrowed snapshot node payload shapes for history-based CRDT snapshots.
#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
enum SnapshotNodeValueRef<'a> {
    LatestValueWins(NullableBasicValueRef<'a>),
    LinearString(&'a str),
    LinearList(PrimitiveValueArrayRef<'a>),
}

/// Snapshot value shapes for state-based CRDT snapshots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotStateValue {
    MonotonicCounter(CounterValue),
    TotalOrderRegister(PrimitiveValue),
    TotalOrderFiniteStateRegister(NullablePrimitiveValue),
}
impl SnapshotStateValue {
    pub fn as_ref(&self) -> SnapshotStateValueRef<'_> {
        match self {
            Self::MonotonicCounter(value) => {
                SnapshotStateValueRef::MonotonicCounter(value.as_ref())
            }
            Self::TotalOrderRegister(value) => {
                SnapshotStateValueRef::TotalOrderRegister(value.as_ref())
            }
            Self::TotalOrderFiniteStateRegister(value) => {
                SnapshotStateValueRef::TotalOrderFiniteStateRegister(value.as_ref())
            }
        }
    }
}

/// Borrowed snapshot value shapes for state-based CRDT snapshots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotStateValueRef<'a> {
    MonotonicCounter(CounterValueRef),
    TotalOrderRegister(PrimitiveValueRef<'a>),
    TotalOrderFiniteStateRegister(NullablePrimitiveValueRef<'a>),
}
impl SnapshotStateValueRef<'_> {
    pub fn into_owned(self) -> SnapshotStateValue {
        match self {
            Self::MonotonicCounter(value) => {
                SnapshotStateValue::MonotonicCounter(value.into_owned())
            }
            Self::TotalOrderRegister(value) => {
                SnapshotStateValue::TotalOrderRegister(value.to_owned())
            }
            Self::TotalOrderFiniteStateRegister(value) => {
                SnapshotStateValue::TotalOrderFiniteStateRegister(value.to_owned())
            }
        }
    }
}

/// Visitor for complete-schema snapshots.
///
/// History-backed fields are prepared once and then receive node streams through a
/// `SnapshotSink` implementation that is specialized for that field type.
///
/// Intended flow:
/// 1. `prepare_schema_snapshot_encoder` (or manual `begin`)
/// 2. `state_field` and `prepare_*_field` as needed
/// 3. stream history nodes via `SnapshotSink::begin/node/end` (typically by calling CRDT
///    `encode_snapshot` directly on the prepared field sink)
/// 4. `end`
pub trait SchemaSnapshotEncoder<Id> {
    type Error: snafu::Error + Send + Sync + 'static;

    type LatestValueWinsFieldSink<'a>: for<'value> SnapshotSink<
            IdWithIndex<Id>,
            NullableBasicValueRef<'value>,
            Error = Self::Error,
        >
    where
        Self: 'a,
        Id: 'a;

    type LinearStringFieldSink<'a>: SnapshotSink<IdWithIndex<Id>, str, Error = Self::Error>
    where
        Self: 'a,
        Id: 'a;

    type LinearListFieldSink<'a>: for<'value> SnapshotSink<
            IdWithIndex<Id>,
            PrimitiveValueArrayRef<'value>,
            Error = Self::Error,
        >
    where
        Self: 'a,
        Id: 'a;

    fn begin(&mut self, field_count: usize) -> Result<(), Self::Error>;

    fn state_field(
        &mut self,
        field_name: &str,
        value: SnapshotStateValueRef<'_>,
    ) -> Result<(), Self::Error>;

    /// Prepare a sink for one `LatestValueWins` field.
    ///
    /// The returned sink is fed using `SnapshotSink::begin/node/end`.
    fn prepare_latest_value_wins_field<'a>(
        &'a mut self,
        field_name: &str,
        value_type: &NullableBasicDataType,
    ) -> Result<Self::LatestValueWinsFieldSink<'a>, Self::Error>;

    /// Prepare a sink for one `LinearString` field.
    ///
    /// The returned sink is fed using `SnapshotSink::begin/node/end`.
    fn prepare_linear_string_field<'a>(
        &'a mut self,
        field_name: &str,
    ) -> Result<Self::LinearStringFieldSink<'a>, Self::Error>;

    /// Prepare a sink for one `LinearList` field.
    ///
    /// The returned sink is fed using `SnapshotSink::begin/node/end`.
    fn prepare_linear_list_field<'a>(
        &'a mut self,
        field_name: &str,
        value_type: PrimitiveType,
    ) -> Result<Self::LinearListFieldSink<'a>, Self::Error>;

    fn end(&mut self) -> Result<(), Self::Error>;
}

/// Streaming snapshot writer that validates against a schema while fields are emitted.
///
/// Use `prepare_*_field` to obtain a field-local sink and pass it to CRDT `encode_snapshot`.
///
/// Typical flow:
/// 1. `prepare_schema_snapshot_encoder`
/// 2. call `state_field` and `prepare_*_field` for all fields
/// 3. for each prepared history field sink, call CRDT `encode_snapshot`
/// 4. `end`
///
/// Example:
/// ```ignore
/// let mut snapshot = prepare_schema_snapshot_encoder(&mut visitor, schema)?;
///
/// snapshot.state_field("counter", SnapshotStateValueRef::MonotonicCounter(...))?;
///
/// // LinearString can stream directly.
/// let mut title_sink = snapshot.prepare_linear_string_field("title")?;
/// title.encode_snapshot(&mut title_sink)?;
///
/// // For LinearList / LatestValueWins, wrap/adapt the sink so CRDT node values are
/// // converted into PrimitiveValueArrayRef / BasicValueRef respectively, then call
/// // CRDT encode_snapshot on that adapter.
/// snapshot.end()?;
/// ```
pub(crate) struct SchemaSnapshotEncodingWriter<'a, Id, V>
where
    V: SchemaSnapshotEncoder<Id>,
{
    schema: &'a Schema,
    visitor: &'a mut V,
    seen_fields: HashSet<String>,
    _id_marker: PhantomData<Id>,
}
impl<'a, Id, V> SchemaSnapshotEncodingWriter<'a, Id, V>
where
    V: SchemaSnapshotEncoder<Id>,
{
    fn register_field(&mut self, field_name: &str) -> Result<(), SchemaValueError> {
        if !self.seen_fields.insert(field_name.to_owned()) {
            return Err(SchemaValueError::DuplicateField {
                field_name: field_name.to_owned(),
            });
        }

        if !self.schema.columns.contains_key(field_name) {
            return Err(SchemaValueError::UnknownField {
                field_name: field_name.to_owned(),
            });
        }

        Ok(())
    }

    pub(super) fn state_field(
        &mut self,
        field_name: &str,
        value: SnapshotStateValueRef<'_>,
    ) -> Result<(), SchemaVisitError<V::Error>> {
        self.register_field(field_name)
            .map_err(|source| SchemaVisitError::InvalidSchemaValue { source })?;
        let schema_field = self
            .schema
            .columns
            .get(field_name)
            .expect("field existence was validated");

        if is_history_data_type(&schema_field.data_type) {
            return Err(SchemaVisitError::InvalidSchemaValue {
                source: SchemaValueError::InvalidSnapshotFieldValue {
                    field_name: field_name.to_owned(),
                    source: DataModelValueError::InvalidSnapshotValueForType,
                },
            });
        }

        validate_snapshot_state_value_for_type(&schema_field.data_type, value.clone()).map_err(
            |source| SchemaVisitError::InvalidSchemaValue {
                source: SchemaValueError::InvalidSnapshotFieldValue {
                    field_name: field_name.to_owned(),
                    source,
                },
            },
        )?;

        self.visitor
            .state_field(field_name, value)
            .map_err(|source| SchemaVisitError::Visitor { source })
    }

    pub(super) fn prepare_latest_value_wins_field<'s>(
        &'s mut self,
        field_name: &str,
    ) -> Result<V::LatestValueWinsFieldSink<'s>, SchemaVisitError<V::Error>> {
        self.register_field(field_name)
            .map_err(|source| SchemaVisitError::InvalidSchemaValue { source })?;
        let schema_field = self
            .schema
            .columns
            .get(field_name)
            .expect("field existence was validated");
        let ReplicatedDataType::LatestValueWins { value_type } = &schema_field.data_type else {
            return Err(SchemaVisitError::InvalidSchemaValue {
                source: SchemaValueError::InvalidSnapshotFieldValue {
                    field_name: field_name.to_owned(),
                    source: DataModelValueError::InvalidSnapshotValueForType,
                },
            });
        };

        self.visitor
            .prepare_latest_value_wins_field(field_name, value_type)
            .map_err(|source| SchemaVisitError::Visitor { source })
    }

    pub(super) fn prepare_linear_string_field<'s>(
        &'s mut self,
        field_name: &str,
    ) -> Result<V::LinearStringFieldSink<'s>, SchemaVisitError<V::Error>> {
        self.register_field(field_name)
            .map_err(|source| SchemaVisitError::InvalidSchemaValue { source })?;
        let schema_field = self
            .schema
            .columns
            .get(field_name)
            .expect("field existence was validated");
        if !matches!(schema_field.data_type, ReplicatedDataType::LinearString) {
            return Err(SchemaVisitError::InvalidSchemaValue {
                source: SchemaValueError::InvalidSnapshotFieldValue {
                    field_name: field_name.to_owned(),
                    source: DataModelValueError::InvalidSnapshotValueForType,
                },
            });
        }

        self.visitor
            .prepare_linear_string_field(field_name)
            .map_err(|source| SchemaVisitError::Visitor { source })
    }

    pub(super) fn prepare_linear_list_field<'s>(
        &'s mut self,
        field_name: &str,
    ) -> Result<V::LinearListFieldSink<'s>, SchemaVisitError<V::Error>> {
        self.register_field(field_name)
            .map_err(|source| SchemaVisitError::InvalidSchemaValue { source })?;
        let schema_field = self
            .schema
            .columns
            .get(field_name)
            .expect("field existence was validated");
        let ReplicatedDataType::LinearList { value_type } = &schema_field.data_type else {
            return Err(SchemaVisitError::InvalidSchemaValue {
                source: SchemaValueError::InvalidSnapshotFieldValue {
                    field_name: field_name.to_owned(),
                    source: DataModelValueError::InvalidSnapshotValueForType,
                },
            });
        };

        self.visitor
            .prepare_linear_list_field(field_name, *value_type)
            .map_err(|source| SchemaVisitError::Visitor { source })
    }

    pub(super) fn end(self) -> Result<(), SchemaVisitError<V::Error>> {
        if self.seen_fields.len() != self.schema.columns.len() {
            let mut missing_fields: Vec<&str> = self
                .schema
                .columns
                .keys()
                .map(String::as_str)
                .filter(|name| !self.seen_fields.contains(*name))
                .collect();
            missing_fields.sort_unstable();

            if let Some(missing_field) = missing_fields.first() {
                return Err(SchemaVisitError::InvalidSchemaValue {
                    source: SchemaValueError::MissingField {
                        field_name: (*missing_field).to_owned(),
                    },
                });
            }
        }

        self.visitor
            .end()
            .map_err(|source| SchemaVisitError::Visitor { source })
    }
}

/// Prepare a streaming schema snapshot writer.
///
/// This calls `SchemaSnapshotEncoder::begin` immediately with `schema.columns.len()`.
/// The resulting writer enforces:
/// - only known schema fields can be emitted
/// - no duplicate fields
/// - complete snapshots (`end` fails if any field is missing)
pub(crate) fn prepare_schema_snapshot_encoder<'a, Id, V>(
    visitor: &'a mut V,
    schema: &'a Schema,
) -> Result<SchemaSnapshotEncodingWriter<'a, Id, V>, SchemaVisitError<V::Error>>
where
    V: SchemaSnapshotEncoder<Id>,
{
    visitor
        .begin(schema.columns.len())
        .map_err(|source| SchemaVisitError::Visitor { source })?;

    Ok(SchemaSnapshotEncodingWriter {
        schema,
        visitor,
        seen_fields: HashSet::new(),
        _id_marker: PhantomData,
    })
}

/// Streaming node source used by snapshot decoders.
///
/// This is the decode counterpart to `SnapshotSink`, yielding one owned node at a time.
pub trait SnapshotNodeSource<Id, Value> {
    type Error;

    fn next_node(&mut self) -> Result<Option<SnapshotNode<Id, Value>>, Self::Error>;
}

/// Iterator adapter over a `SnapshotNodeSource`.
pub(crate) struct SnapshotNodeSourceIter<'a, Source, Id, Value>
where
    Source: SnapshotNodeSource<Id, Value>,
{
    source: &'a mut Source,
    _marker: PhantomData<fn(Id, Value)>,
}
impl<'a, Source, Id, Value> SnapshotNodeSourceIter<'a, Source, Id, Value>
where
    Source: SnapshotNodeSource<Id, Value>,
{
    pub(crate) fn new(source: &'a mut Source) -> Self {
        Self {
            source,
            _marker: PhantomData,
        }
    }
}
impl<Source, Id, Value> Iterator for SnapshotNodeSourceIter<'_, Source, Id, Value>
where
    Source: SnapshotNodeSource<Id, Value>,
{
    type Item = Result<SnapshotNode<Id, Value>, Source::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.source.next_node() {
            Ok(Some(node)) => Some(Ok(node)),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        }
    }
}

/// Decoder for one complete row snapshot across schema fields.
///
/// Intended flow:
/// 1. `begin(expected_field_count)`
/// 2. decode each schema field via `decode_state_field` or `prepare_*_field`
/// 3. for history fields, consume node streams lazily via `SnapshotNodeSourceIter`
/// 4. `end`
pub trait SchemaSnapshotDecoder<Id> {
    type Error: snafu::Error + Send + Sync + 'static;

    type LatestValueWinsFieldSource<'a>: SnapshotNodeSource<IdWithIndex<Id>, NullableBasicValue, Error = Self::Error>
    where
        Self: 'a,
        Id: 'a;

    type LinearStringFieldSource<'a>: SnapshotNodeSource<IdWithIndex<Id>, String, Error = Self::Error>
    where
        Self: 'a,
        Id: 'a;

    type LinearListFieldSource<'a>: SnapshotNodeSource<IdWithIndex<Id>, PrimitiveValueArray, Error = Self::Error>
    where
        Self: 'a,
        Id: 'a;

    fn begin(&mut self, expected_field_count: usize) -> Result<(), Self::Error>;

    fn decode_state_field(
        &mut self,
        field_name: &str,
        data_type: &ReplicatedDataType,
    ) -> Result<SnapshotStateValue, Self::Error>;

    fn prepare_latest_value_wins_field<'a>(
        &'a mut self,
        field_name: &str,
        value_type: &NullableBasicDataType,
    ) -> Result<Self::LatestValueWinsFieldSource<'a>, Self::Error>;

    fn prepare_linear_string_field<'a>(
        &'a mut self,
        field_name: &str,
    ) -> Result<Self::LinearStringFieldSource<'a>, Self::Error>;

    fn prepare_linear_list_field<'a>(
        &'a mut self,
        field_name: &str,
        value_type: PrimitiveType,
    ) -> Result<Self::LinearListFieldSource<'a>, Self::Error>;

    fn end(&mut self) -> Result<(), Self::Error>;
}

/// Dataset-level encoder that streams complete row snapshots.
///
/// Intended flow:
/// 1. `begin(row_count)`
/// 2. repeat for each row index:
///    `begin_row` -> row `SchemaSnapshotEncoder` -> row `end` -> `end_row`
/// 3. `end`
pub trait DataSnapshotEncoder<Id> {
    type Error: snafu::Error + Send + Sync + 'static;

    type RowEncoder<'a>: SchemaSnapshotEncoder<Id, Error = Self::Error>
    where
        Self: 'a,
        Id: 'a;

    fn begin(&mut self, row_count: usize) -> Result<(), Self::Error>;

    fn begin_row<'a>(&'a mut self, row_index: usize) -> Result<Self::RowEncoder<'a>, Self::Error>;

    fn end_row(&mut self, row_index: usize) -> Result<(), Self::Error>;

    fn end(&mut self) -> Result<(), Self::Error>;
}

/// Dataset-level decoder that streams complete row snapshots lazily.
///
/// Intended flow:
/// 1. `begin()` -> row count
/// 2. repeat for each row index:
///    `begin_row` -> row `SchemaSnapshotDecoder` -> row `end` -> `end_row`
/// 3. `end`
pub trait DataSnapshotDecoder<Id> {
    type Error: snafu::Error + Send + Sync + 'static;

    type RowDecoder<'a>: SchemaSnapshotDecoder<Id, Error = Self::Error>
    where
        Self: 'a,
        Id: 'a;

    fn begin(&mut self) -> Result<usize, Self::Error>;

    fn begin_row<'a>(&'a mut self, row_index: usize) -> Result<Self::RowDecoder<'a>, Self::Error>;

    fn end_row(&mut self, row_index: usize) -> Result<(), Self::Error>;

    fn end(&mut self) -> Result<(), Self::Error>;
}

/// Visitor used by serializers to encode one history node payload at a time.
#[cfg(test)]
trait SnapshotNodeValueEncoder {
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
trait SnapshotNodeValueDecoder {
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
trait SnapshotStateValueEncoder {
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
trait SnapshotStateValueDecoder {
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
fn encode_snapshot_node_value<V>(
    visitor: &mut V,
    data_type: &ReplicatedDataType,
    value: SnapshotNodeValueRef<'_>,
) -> Result<(), VisitError<V::Error>>
where
    V: SnapshotNodeValueEncoder,
    V::Error: snafu::Error + Send + Sync + 'static,
{
    match (data_type, value) {
        (
            ReplicatedDataType::LatestValueWins { value_type },
            SnapshotNodeValueRef::LatestValueWins(v),
        ) => {
            ensure_nullable_basic_type(value_type, &v)
                .map_err(|source| VisitError::InvalidVisitedValue { source })?;
            visitor
                .visit_latest_value_wins_node(value_type, v)
                .map_err(|source| VisitError::VisitorSource { source })
        }
        (ReplicatedDataType::LinearString, SnapshotNodeValueRef::LinearString(v)) => visitor
            .visit_linear_string_node(v)
            .map_err(|source| VisitError::VisitorSource { source }),
        (ReplicatedDataType::LinearList { value_type }, SnapshotNodeValueRef::LinearList(v)) => {
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
fn decode_snapshot_node_value<D>(
    decoder: &mut D,
    data_type: &ReplicatedDataType,
) -> Result<SnapshotNodeValue, DecodeError<D::Error>>
where
    D: SnapshotNodeValueDecoder,
    D::Error: snafu::Error + Send + Sync + 'static,
{
    match data_type {
        ReplicatedDataType::LatestValueWins { value_type } => {
            let value = decoder
                .decode_latest_value_wins_node(value_type)
                .map_err(|source| DecodeError::DecoderSource { source })?;
            ensure_nullable_basic_type(value_type, &value.as_ref())
                .map_err(|source| DecodeError::InvalidDecodedValue { source })?;
            Ok(SnapshotNodeValue::LatestValueWins(value))
        }
        ReplicatedDataType::LinearString => decoder
            .decode_linear_string_node()
            .map(SnapshotNodeValue::LinearString)
            .map_err(|source| DecodeError::DecoderSource { source }),
        ReplicatedDataType::LinearList { value_type } => {
            let values = decoder
                .decode_linear_list_node(*value_type)
                .map_err(|source| DecodeError::DecoderSource { source })?;
            ensure_primitive_array_type(*value_type, values.primitive_type())
                .map_err(|source| DecodeError::InvalidDecodedValue { source })?;
            Ok(SnapshotNodeValue::LinearList(values))
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
fn encode_snapshot_state_value<V>(
    visitor: &mut V,
    data_type: &ReplicatedDataType,
    value: SnapshotStateValueRef<'_>,
) -> Result<(), VisitError<V::Error>>
where
    V: SnapshotStateValueEncoder,
    V::Error: snafu::Error + Send + Sync + 'static,
{
    match (data_type, value) {
        (
            ReplicatedDataType::MonotonicCounter { small_range },
            SnapshotStateValueRef::MonotonicCounter(v),
        ) => {
            ensure_counter_type(*small_range, v)
                .map_err(|source| VisitError::InvalidVisitedValue { source })?;
            visitor
                .visit_monotonic_counter(*small_range, v)
                .map_err(|source| VisitError::VisitorSource { source })
        }
        (
            ReplicatedDataType::TotalOrderRegister { value_type, .. },
            SnapshotStateValueRef::TotalOrderRegister(v),
        ) => {
            ensure_primitive_type(*value_type, v.value_type())
                .map_err(|source| VisitError::InvalidVisitedValue { source })?;
            visitor
                .visit_total_order_register(*value_type, v)
                .map_err(|source| VisitError::VisitorSource { source })
        }
        (
            ReplicatedDataType::TotalOrderFiniteStateRegister { value_type, states },
            SnapshotStateValueRef::TotalOrderFiniteStateRegister(v),
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
fn decode_snapshot_state_value<D>(
    decoder: &mut D,
    data_type: &ReplicatedDataType,
) -> Result<SnapshotStateValue, DecodeError<D::Error>>
where
    D: SnapshotStateValueDecoder,
    D::Error: snafu::Error + Send + Sync + 'static,
{
    match data_type {
        ReplicatedDataType::MonotonicCounter { small_range } => {
            let value = decoder
                .decode_monotonic_counter(*small_range)
                .map_err(|source| DecodeError::DecoderSource { source })?;
            ensure_counter_type(*small_range, value.as_ref())
                .map_err(|source| DecodeError::InvalidDecodedValue { source })?;
            Ok(SnapshotStateValue::MonotonicCounter(value))
        }
        ReplicatedDataType::TotalOrderRegister { value_type, .. } => {
            let value = decoder
                .decode_total_order_register(*value_type)
                .map_err(|source| DecodeError::DecoderSource { source })?;
            ensure_primitive_type(*value_type, value.primitive_type())
                .map_err(|source| DecodeError::InvalidDecodedValue { source })?;
            Ok(SnapshotStateValue::TotalOrderRegister(value))
        }
        ReplicatedDataType::TotalOrderFiniteStateRegister { value_type, states } => {
            let value = decoder
                .decode_total_order_finite_state_register(*value_type, states)
                .map_err(|source| DecodeError::DecoderSource { source })?;
            ensure_finite_state_value(*value_type, states, &value.as_ref())
                .map_err(|source| DecodeError::InvalidDecodedValue { source })?;
            Ok(SnapshotStateValue::TotalOrderFiniteStateRegister(value))
        }
        ReplicatedDataType::LatestValueWins { .. }
        | ReplicatedDataType::LinearString
        | ReplicatedDataType::LinearList { .. } => Err(DecodeError::InvalidDecodedValue {
            source: DataModelValueError::InvalidSnapshotValueForType,
        }),
    }
}
fn validate_snapshot_state_value_for_type(
    data_type: &ReplicatedDataType,
    value: SnapshotStateValueRef<'_>,
) -> Result<(), DataModelValueError> {
    let mut visitor = NoopSnapshotStateValueEncoder;
    match encode_snapshot_state_value(&mut visitor, data_type, value) {
        Ok(()) => Ok(()),
        Err(VisitError::InvalidVisitedValue { source }) => Err(source),
        Err(VisitError::VisitorSource { source: never }) => match never {},
    }
}
struct NoopSnapshotStateValueEncoder;
impl SnapshotStateValueEncoder for NoopSnapshotStateValueEncoder {
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
fn is_history_data_type(data_type: &ReplicatedDataType) -> bool {
    matches!(
        data_type,
        ReplicatedDataType::LatestValueWins { .. }
            | ReplicatedDataType::LinearString
            | ReplicatedDataType::LinearList { .. }
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        IdWithIndex,
        any_data::{LinearLatestValueWins, list::LinearList},
        linear_data::snapshot::bytes_testkit as snapshot_bytes,
        schema::{Direction, Field},
        snapshot::{SnapshotHeader, SnapshotNodeRef, SnapshotSink},
        text::LinearString,
    };
    use bytes::{Buf, BufMut, Bytes, BytesMut};
    use chrono::Datelike;
    use std::{borrow::Cow, collections::HashMap, sync::LazyLock};

    const TAG_NULL: u8 = 0;
    const TAG_BASIC_PRIMITIVE: u8 = 1;
    const TAG_BASIC_ARRAY: u8 = 2;
    const DATASET_MAGIC: [u8; 4] = *b"DSNP";
    const DATASET_END_MARKER: u8 = 0xEF;

    #[derive(Clone, Debug, Snafu)]
    #[snafu(display("{message}"))]
    struct TestError {
        message: String,
    }
    impl From<String> for TestError {
        fn from(message: String) -> Self {
            Self { message }
        }
    }

    fn write_row_field_map(
        target: &mut BytesMut,
        fields: &HashMap<String, Bytes>,
    ) -> Result<(), String> {
        target.put_u32_le(u32::try_from(fields.len()).map_err(|_| "too many fields".to_owned())?);

        let mut entries: Vec<(&String, &Bytes)> = fields.iter().collect();
        entries.sort_by_key(|(l, _)| *l);
        for (field_name, payload) in entries {
            snapshot_bytes::write_bytes(target, field_name.as_bytes())?;
            snapshot_bytes::write_bytes(target, payload.as_ref())?;
        }

        Ok(())
    }

    fn read_row_field_map(input: &mut Bytes) -> Result<HashMap<String, Bytes>, String> {
        let field_count: usize = snapshot_bytes::read_u32(input)?.try_into().unwrap();
        let mut fields = HashMap::with_capacity(field_count);
        for _ in 0..field_count {
            let field_name_bytes = snapshot_bytes::read_len_prefixed(input)?;
            let field_name = String::from_utf8(field_name_bytes.to_vec())
                .map_err(|_| "invalid utf8 field name".to_owned())?;
            let payload = snapshot_bytes::read_len_prefixed(input)?;
            if fields.insert(field_name.clone(), payload).is_some() {
                return Err(format!("duplicate field '{field_name}' in dataset payload"));
            }
        }
        Ok(fields)
    }

    fn primitive_type_tag(value_type: PrimitiveType) -> u8 {
        match value_type {
            PrimitiveType::String => 0,
            PrimitiveType::UInt => 1,
            PrimitiveType::Int => 2,
            PrimitiveType::Byte => 3,
            PrimitiveType::Float => 4,
            PrimitiveType::Boolean => 5,
            PrimitiveType::Binary => 6,
            PrimitiveType::Date => 7,
            PrimitiveType::Timestamp => 8,
        }
    }

    fn primitive_type_from_tag(tag: u8) -> Result<PrimitiveType, String> {
        match tag {
            0 => Ok(PrimitiveType::String),
            1 => Ok(PrimitiveType::UInt),
            2 => Ok(PrimitiveType::Int),
            3 => Ok(PrimitiveType::Byte),
            4 => Ok(PrimitiveType::Float),
            5 => Ok(PrimitiveType::Boolean),
            6 => Ok(PrimitiveType::Binary),
            7 => Ok(PrimitiveType::Date),
            8 => Ok(PrimitiveType::Timestamp),
            _ => Err(format!("unknown primitive type tag {tag}")),
        }
    }

    fn write_primitive_value(
        target: &mut BytesMut,
        value: PrimitiveValueRef<'_>,
    ) -> Result<(), String> {
        target.put_u8(primitive_type_tag(value.primitive_type()));
        write_primitive_payload(target, value)
    }

    fn write_primitive_payload(
        target: &mut BytesMut,
        value: PrimitiveValueRef<'_>,
    ) -> Result<(), String> {
        match value {
            PrimitiveValueRef::String(value) => {
                snapshot_bytes::write_bytes(target, value.as_bytes())
            }
            PrimitiveValueRef::UInt(value) => {
                target.put_u64_le(value);
                Ok(())
            }
            PrimitiveValueRef::Int(value) => {
                target.put_i64_le(value);
                Ok(())
            }
            PrimitiveValueRef::Byte(value) => {
                target.put_u8(value);
                Ok(())
            }
            PrimitiveValueRef::Float(value) => {
                target.put_f64_le(value.0);
                Ok(())
            }
            PrimitiveValueRef::Boolean(value) => {
                target.put_u8(u8::from(value));
                Ok(())
            }
            PrimitiveValueRef::Binary(value) => snapshot_bytes::write_bytes(target, value),
            PrimitiveValueRef::Date(value) => {
                target.put_i32_le(value.year());
                target.put_u8(value.month().try_into().unwrap());
                target.put_u8(value.day().try_into().unwrap());
                Ok(())
            }
            PrimitiveValueRef::Timestamp(value) => {
                target.put_i64_le(value);
                Ok(())
            }
        }
    }

    fn read_i32(input: &mut Bytes) -> Result<i32, String> {
        if input.remaining() < 4 {
            return Err("unexpected end of payload".to_owned());
        }
        Ok(input.get_i32_le())
    }

    fn read_i64(input: &mut Bytes) -> Result<i64, String> {
        if input.remaining() < 8 {
            return Err("unexpected end of payload".to_owned());
        }
        Ok(input.get_i64_le())
    }

    fn read_u64(input: &mut Bytes) -> Result<u64, String> {
        if input.remaining() < 8 {
            return Err("unexpected end of payload".to_owned());
        }
        Ok(input.get_u64_le())
    }

    fn read_f64(input: &mut Bytes) -> Result<f64, String> {
        if input.remaining() < 8 {
            return Err("unexpected end of payload".to_owned());
        }
        Ok(input.get_f64_le())
    }

    fn read_primitive_value(input: &mut Bytes) -> Result<PrimitiveValue, String> {
        let value_type = primitive_type_from_tag(snapshot_bytes::read_u8(input)?)?;
        read_primitive_payload(input, value_type)
    }

    fn read_primitive_payload(
        input: &mut Bytes,
        value_type: PrimitiveType,
    ) -> Result<PrimitiveValue, String> {
        match value_type {
            PrimitiveType::String => {
                let value = snapshot_bytes::read_len_prefixed(input)?;
                String::from_utf8(value.to_vec())
                    .map(PrimitiveValue::String)
                    .map_err(|_| "invalid utf8 string".to_owned())
            }
            PrimitiveType::UInt => read_u64(input).map(PrimitiveValue::UInt),
            PrimitiveType::Int => read_i64(input).map(PrimitiveValue::Int),
            PrimitiveType::Byte => snapshot_bytes::read_u8(input).map(PrimitiveValue::Byte),
            PrimitiveType::Float => read_f64(input)
                .map(ordered_float::OrderedFloat)
                .map(PrimitiveValue::Float),
            PrimitiveType::Boolean => {
                let value = snapshot_bytes::read_u8(input)?;
                match value {
                    0 => Ok(PrimitiveValue::Boolean(false)),
                    1 => Ok(PrimitiveValue::Boolean(true)),
                    _ => Err("invalid boolean encoding".to_owned()),
                }
            }
            PrimitiveType::Binary => snapshot_bytes::read_len_prefixed(input)
                .map(|value| PrimitiveValue::Binary(value.to_vec())),
            PrimitiveType::Date => {
                let year = read_i32(input)?;
                let month: u32 = snapshot_bytes::read_u8(input)?.into();
                let day: u32 = snapshot_bytes::read_u8(input)?.into();
                chrono::NaiveDate::from_ymd_opt(year, month, day)
                    .map(PrimitiveValue::Date)
                    .ok_or_else(|| "invalid date encoding".to_owned())
            }
            PrimitiveType::Timestamp => read_i64(input).map(PrimitiveValue::Timestamp),
        }
    }

    fn write_primitive_array(
        target: &mut BytesMut,
        values: PrimitiveValueArrayRef<'_>,
    ) -> Result<(), String> {
        target.put_u8(primitive_type_tag(values.primitive_type()));
        match values {
            PrimitiveValueArrayRef::String(values) => {
                target.put_u32_le(
                    u32::try_from(values.len()).map_err(|_| "array too large".to_owned())?,
                );
                for value in values {
                    snapshot_bytes::write_bytes(target, value.as_bytes())?;
                }
            }
            PrimitiveValueArrayRef::UInt(values) => {
                target.put_u32_le(
                    u32::try_from(values.len()).map_err(|_| "array too large".to_owned())?,
                );
                for value in values {
                    target.put_u64_le(*value);
                }
            }
            PrimitiveValueArrayRef::Int(values) => {
                target.put_u32_le(
                    u32::try_from(values.len()).map_err(|_| "array too large".to_owned())?,
                );
                for value in values {
                    target.put_i64_le(*value);
                }
            }
            PrimitiveValueArrayRef::Byte(values) => {
                target.put_u32_le(
                    u32::try_from(values.len()).map_err(|_| "array too large".to_owned())?,
                );
                target.put_slice(values);
            }
            PrimitiveValueArrayRef::Float(values) => {
                target.put_u32_le(
                    u32::try_from(values.len()).map_err(|_| "array too large".to_owned())?,
                );
                for value in values {
                    target.put_f64_le(value.0);
                }
            }
            PrimitiveValueArrayRef::Boolean(values) => {
                target.put_u32_le(
                    u32::try_from(values.len()).map_err(|_| "array too large".to_owned())?,
                );
                for value in values {
                    target.put_u8(u8::from(*value));
                }
            }
            PrimitiveValueArrayRef::Binary(values) => {
                target.put_u32_le(
                    u32::try_from(values.len()).map_err(|_| "array too large".to_owned())?,
                );
                for value in values {
                    snapshot_bytes::write_bytes(target, value)?;
                }
            }
            PrimitiveValueArrayRef::Date(values) => {
                target.put_u32_le(
                    u32::try_from(values.len()).map_err(|_| "array too large".to_owned())?,
                );
                for value in values {
                    target.put_i32_le(value.year());
                    target.put_u8(value.month().try_into().unwrap());
                    target.put_u8(value.day().try_into().unwrap());
                }
            }
            PrimitiveValueArrayRef::Timestamp(values) => {
                target.put_u32_le(
                    u32::try_from(values.len()).map_err(|_| "array too large".to_owned())?,
                );
                for value in values {
                    target.put_i64_le(*value);
                }
            }
        }
        Ok(())
    }

    fn read_primitive_array(input: &mut Bytes) -> Result<PrimitiveValueArray, String> {
        let value_type = primitive_type_from_tag(snapshot_bytes::read_u8(input)?)?;
        let len: usize = snapshot_bytes::read_u32(input)?.try_into().unwrap();
        match value_type {
            PrimitiveType::String => {
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    let value = snapshot_bytes::read_len_prefixed(input)?;
                    values.push(
                        String::from_utf8(value.to_vec())
                            .map_err(|_| "invalid utf8 string".to_owned())?,
                    );
                }
                Ok(PrimitiveValueArray::String(values))
            }
            PrimitiveType::UInt => {
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    values.push(read_u64(input)?);
                }
                Ok(PrimitiveValueArray::UInt(values))
            }
            PrimitiveType::Int => {
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    values.push(read_i64(input)?);
                }
                Ok(PrimitiveValueArray::Int(values))
            }
            PrimitiveType::Byte => {
                if input.remaining() < len {
                    return Err("unexpected end of payload".to_owned());
                }
                Ok(PrimitiveValueArray::Byte(input.copy_to_bytes(len).to_vec()))
            }
            PrimitiveType::Float => {
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    values.push(ordered_float::OrderedFloat(read_f64(input)?));
                }
                Ok(PrimitiveValueArray::Float(values))
            }
            PrimitiveType::Boolean => {
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    let value = snapshot_bytes::read_u8(input)?;
                    match value {
                        0 => values.push(false),
                        1 => values.push(true),
                        _ => return Err("invalid boolean encoding".to_owned()),
                    }
                }
                Ok(PrimitiveValueArray::Boolean(values))
            }
            PrimitiveType::Binary => {
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    values.push(snapshot_bytes::read_len_prefixed(input)?.to_vec());
                }
                Ok(PrimitiveValueArray::Binary(values))
            }
            PrimitiveType::Date => {
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    let year = read_i32(input)?;
                    let month: u32 = snapshot_bytes::read_u8(input)?.into();
                    let day: u32 = snapshot_bytes::read_u8(input)?.into();
                    let date = chrono::NaiveDate::from_ymd_opt(year, month, day)
                        .ok_or_else(|| "invalid date encoding".to_owned())?;
                    values.push(date);
                }
                Ok(PrimitiveValueArray::Date(values))
            }
            PrimitiveType::Timestamp => {
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    values.push(read_i64(input)?);
                }
                Ok(PrimitiveValueArray::Timestamp(values))
            }
        }
    }

    fn write_nullable_basic_value(
        target: &mut BytesMut,
        value: NullableBasicValueRef<'_>,
    ) -> Result<(), String> {
        match value {
            NullableBasicValueRef::Null => {
                target.put_u8(TAG_NULL);
                Ok(())
            }
            NullableBasicValueRef::Value(BasicValueRef::Primitive(value)) => {
                target.put_u8(TAG_BASIC_PRIMITIVE);
                write_primitive_value(target, value)
            }
            NullableBasicValueRef::Value(BasicValueRef::Array(values)) => {
                target.put_u8(TAG_BASIC_ARRAY);
                write_primitive_array(target, values)
            }
        }
    }

    fn read_nullable_basic_value(input: &mut Bytes) -> Result<NullableBasicValue, String> {
        match snapshot_bytes::read_u8(input)? {
            TAG_NULL => Ok(NullableBasicValue::Null),
            TAG_BASIC_PRIMITIVE => read_primitive_value(input)
                .map(BasicValue::Primitive)
                .map(NullableBasicValue::Value),
            TAG_BASIC_ARRAY => read_primitive_array(input)
                .map(BasicValue::Array)
                .map(NullableBasicValue::Value),
            tag => Err(format!("unknown nullable basic tag {tag}")),
        }
    }

    fn write_nullable_primitive_value(
        target: &mut BytesMut,
        value: NullablePrimitiveValueRef<'_>,
    ) -> Result<(), String> {
        match value {
            NullablePrimitiveValueRef::Null => {
                target.put_u8(TAG_NULL);
                Ok(())
            }
            NullablePrimitiveValueRef::Value(value) => {
                target.put_u8(TAG_BASIC_PRIMITIVE);
                write_primitive_value(target, value)
            }
        }
    }

    fn read_nullable_primitive_value(input: &mut Bytes) -> Result<NullablePrimitiveValue, String> {
        match snapshot_bytes::read_u8(input)? {
            TAG_NULL => Ok(NullablePrimitiveValue::Null),
            TAG_BASIC_PRIMITIVE => read_primitive_value(input).map(NullablePrimitiveValue::Value),
            tag => Err(format!("unknown nullable primitive tag {tag}")),
        }
    }

    fn write_counter_value(
        target: &mut BytesMut,
        small_range: bool,
        value: CounterValueRef,
    ) -> Result<(), String> {
        match (small_range, value) {
            (true, CounterValueRef::Byte(value)) => {
                target.put_u8(value);
                Ok(())
            }
            (false, CounterValueRef::UInt(value)) => {
                target.put_u64_le(value);
                Ok(())
            }
            _ => Err("counter value does not match schema".to_owned()),
        }
    }

    fn read_counter_value(input: &mut Bytes, small_range: bool) -> Result<CounterValue, String> {
        if small_range {
            snapshot_bytes::read_u8(input).map(CounterValue::Byte)
        } else {
            read_u64(input).map(CounterValue::UInt)
        }
    }

    #[derive(Default)]
    struct BytesNodeValueEncoder {
        bytes: BytesMut,
    }
    impl BytesNodeValueEncoder {
        fn finish(self) -> Bytes {
            self.bytes.freeze()
        }
    }
    impl SnapshotNodeValueEncoder for BytesNodeValueEncoder {
        type Error = TestError;

        fn visit_latest_value_wins_node(
            &mut self,
            _value_type: &NullableBasicDataType,
            value: NullableBasicValueRef<'_>,
        ) -> Result<(), Self::Error> {
            write_nullable_basic_value(&mut self.bytes, value).map_err(Into::into)
        }

        fn visit_linear_string_node(&mut self, value: &str) -> Result<(), Self::Error> {
            snapshot_bytes::write_bytes(&mut self.bytes, value.as_bytes()).map_err(Into::into)
        }

        fn visit_linear_list_node(
            &mut self,
            _value_type: PrimitiveType,
            values: PrimitiveValueArrayRef<'_>,
        ) -> Result<(), Self::Error> {
            write_primitive_array(&mut self.bytes, values).map_err(Into::into)
        }
    }

    struct BytesNodeValueDecoder {
        bytes: Bytes,
    }
    impl BytesNodeValueDecoder {
        fn new(bytes: Bytes) -> Self {
            Self { bytes }
        }

        fn ensure_consumed(&self) -> Result<(), String> {
            if self.bytes.has_remaining() {
                Err("trailing bytes in payload".to_owned())
            } else {
                Ok(())
            }
        }
    }
    impl SnapshotNodeValueDecoder for BytesNodeValueDecoder {
        type Error = TestError;

        fn decode_latest_value_wins_node(
            &mut self,
            _value_type: &NullableBasicDataType,
        ) -> Result<NullableBasicValue, Self::Error> {
            read_nullable_basic_value(&mut self.bytes).map_err(Into::into)
        }

        fn decode_linear_string_node(&mut self) -> Result<String, Self::Error> {
            let bytes = snapshot_bytes::read_len_prefixed(&mut self.bytes)?;
            String::from_utf8(bytes.to_vec())
                .map_err(|_| "invalid utf8 string".to_owned())
                .map_err(Into::into)
        }

        fn decode_linear_list_node(
            &mut self,
            _value_type: PrimitiveType,
        ) -> Result<PrimitiveValueArray, Self::Error> {
            read_primitive_array(&mut self.bytes).map_err(Into::into)
        }
    }

    #[derive(Default)]
    struct BytesStateValueEncoder {
        bytes: BytesMut,
    }
    impl BytesStateValueEncoder {
        fn finish(self) -> Bytes {
            self.bytes.freeze()
        }
    }
    impl SnapshotStateValueEncoder for BytesStateValueEncoder {
        type Error = TestError;

        fn visit_monotonic_counter(
            &mut self,
            small_range: bool,
            value: CounterValueRef,
        ) -> Result<(), Self::Error> {
            write_counter_value(&mut self.bytes, small_range, value).map_err(Into::into)
        }

        fn visit_total_order_register(
            &mut self,
            _value_type: PrimitiveType,
            value: PrimitiveValueRef<'_>,
        ) -> Result<(), Self::Error> {
            write_primitive_value(&mut self.bytes, value).map_err(Into::into)
        }

        fn visit_total_order_finite_state_register(
            &mut self,
            _value_type: NullablePrimitiveType,
            _states: &NullablePrimitiveValueArray,
            value: NullablePrimitiveValueRef<'_>,
        ) -> Result<(), Self::Error> {
            write_nullable_primitive_value(&mut self.bytes, value).map_err(Into::into)
        }
    }

    struct BytesStateValueDecoder {
        bytes: Bytes,
    }
    impl BytesStateValueDecoder {
        fn new(bytes: Bytes) -> Self {
            Self { bytes }
        }

        fn ensure_consumed(&self) -> Result<(), String> {
            if self.bytes.has_remaining() {
                Err("trailing bytes in payload".to_owned())
            } else {
                Ok(())
            }
        }
    }
    impl SnapshotStateValueDecoder for BytesStateValueDecoder {
        type Error = TestError;

        fn decode_monotonic_counter(
            &mut self,
            small_range: bool,
        ) -> Result<CounterValue, Self::Error> {
            read_counter_value(&mut self.bytes, small_range).map_err(Into::into)
        }

        fn decode_total_order_register(
            &mut self,
            _value_type: PrimitiveType,
        ) -> Result<PrimitiveValue, Self::Error> {
            read_primitive_value(&mut self.bytes).map_err(Into::into)
        }

        fn decode_total_order_finite_state_register(
            &mut self,
            _value_type: NullablePrimitiveType,
            _states: &NullablePrimitiveValueArray,
        ) -> Result<NullablePrimitiveValue, Self::Error> {
            read_nullable_primitive_value(&mut self.bytes).map_err(Into::into)
        }
    }

    fn visit_error_to_string(error: VisitError<TestError>) -> String {
        match error {
            VisitError::InvalidVisitedValue { source } => format!("invalid value: {source:?}"),
            VisitError::VisitorSource { source } => source.to_string(),
        }
    }

    fn decode_error_to_string(error: DecodeError<TestError>) -> String {
        match error {
            DecodeError::InvalidDecodedValue { source } => format!("invalid value: {source:?}"),
            DecodeError::DecoderSource { source } => source.to_string(),
        }
    }

    fn encode_node_payload(
        data_type: &ReplicatedDataType,
        value: SnapshotNodeValueRef<'_>,
    ) -> Result<Vec<u8>, String> {
        let mut encoder = BytesNodeValueEncoder::default();
        encode_snapshot_node_value(&mut encoder, data_type, value)
            .map_err(visit_error_to_string)?;
        Ok(encoder.finish().to_vec())
    }

    fn decode_node_payload(
        data_type: &ReplicatedDataType,
        payload: &[u8],
    ) -> Result<SnapshotNodeValue, String> {
        let mut decoder = BytesNodeValueDecoder::new(Bytes::copy_from_slice(payload));
        let value =
            decode_snapshot_node_value(&mut decoder, data_type).map_err(decode_error_to_string)?;
        decoder.ensure_consumed()?;
        Ok(value)
    }

    fn encode_state_payload(
        data_type: &ReplicatedDataType,
        value: SnapshotStateValueRef<'_>,
    ) -> Result<Vec<u8>, String> {
        let mut encoder = BytesStateValueEncoder::default();
        encode_snapshot_state_value(&mut encoder, data_type, value)
            .map_err(visit_error_to_string)?;
        Ok(encoder.finish().to_vec())
    }

    fn decode_state_payload(
        data_type: &ReplicatedDataType,
        payload: &[u8],
    ) -> Result<SnapshotStateValue, String> {
        let mut decoder = BytesStateValueDecoder::new(Bytes::copy_from_slice(payload));
        let value =
            decode_snapshot_state_value(&mut decoder, data_type).map_err(decode_error_to_string)?;
        decoder.ensure_consumed()?;
        Ok(value)
    }

    type RawLinearHistorySink = snapshot_bytes::ByteBufSink<
        IdWithIndex<u32>,
        [u8],
        fn(&IdWithIndex<u32>) -> Vec<u8>,
        fn(&[u8]) -> Vec<u8>,
    >;
    type RawLvwHistorySink = snapshot_bytes::ByteBufSink<
        IdWithIndex<u32>,
        [u8],
        fn(&IdWithIndex<u32>) -> Vec<u8>,
        fn(&[u8]) -> Vec<u8>,
    >;

    fn encode_raw_bytes(value: &[u8]) -> Vec<u8> {
        value.to_vec()
    }

    fn new_raw_linear_history_sink() -> RawLinearHistorySink {
        snapshot_bytes::ByteBufSink::new(
            snapshot_bytes::encode_id_with_index_u32,
            encode_raw_bytes as fn(&[u8]) -> Vec<u8>,
        )
    }
    fn new_raw_lvw_history_sink() -> RawLvwHistorySink {
        snapshot_bytes::ByteBufSink::new(
            snapshot_bytes::encode_id_with_index_u32,
            encode_raw_bytes as fn(&[u8]) -> Vec<u8>,
        )
    }

    struct LatestValueWinsHistoryBytesSink<'a> {
        field_name: String,
        value_type: NullableBasicDataType,
        output: &'a mut HashMap<String, Bytes>,
        sink: Option<RawLvwHistorySink>,
    }
    impl<'a> LatestValueWinsHistoryBytesSink<'a> {
        fn new(
            field_name: &str,
            value_type: NullableBasicDataType,
            output: &'a mut HashMap<String, Bytes>,
        ) -> Self {
            Self {
                field_name: field_name.to_owned(),
                value_type,
                output,
                sink: Some(new_raw_lvw_history_sink()),
            }
        }

        fn sink_mut(&mut self) -> Result<&mut RawLvwHistorySink, String> {
            self.sink
                .as_mut()
                .ok_or_else(|| "field sink already closed".to_owned())
        }
    }
    impl<'a, 'value> SnapshotSink<IdWithIndex<u32>, NullableBasicValueRef<'value>>
        for LatestValueWinsHistoryBytesSink<'a>
    {
        type Error = TestError;

        fn begin(&mut self, header: SnapshotHeader) -> Result<(), Self::Error> {
            self.sink_mut()?.begin(header).map_err(Into::into)
        }

        fn node(
            &mut self,
            index: usize,
            node: SnapshotNodeRef<'_, IdWithIndex<u32>, NullableBasicValueRef<'value>>,
        ) -> Result<(), Self::Error> {
            let data_type = ReplicatedDataType::LatestValueWins {
                value_type: self.value_type.clone(),
            };
            let encoded_value = if let Some(value) = node.value {
                Some(encode_node_payload(
                    &data_type,
                    SnapshotNodeValueRef::LatestValueWins(value.clone()),
                )?)
            } else {
                None
            };

            let raw_node = SnapshotNodeRef {
                id: node.id,
                left: node.left,
                right: node.right,
                deleted: node.deleted,
                value: encoded_value.as_deref(),
            };
            self.sink_mut()?.node(index, raw_node).map_err(Into::into)
        }

        fn end(&mut self) -> Result<(), Self::Error> {
            let mut sink = self
                .sink
                .take()
                .ok_or_else(|| "field sink already closed".to_owned())?;
            sink.end()?;
            self.output
                .insert(self.field_name.clone(), sink.into_bytes());
            Ok(())
        }
    }

    struct LinearStringHistoryBytesSink<'a> {
        field_name: String,
        output: &'a mut HashMap<String, Bytes>,
        sink: Option<RawLinearHistorySink>,
    }
    impl<'a> LinearStringHistoryBytesSink<'a> {
        fn new(field_name: &str, output: &'a mut HashMap<String, Bytes>) -> Self {
            Self {
                field_name: field_name.to_owned(),
                output,
                sink: Some(new_raw_linear_history_sink()),
            }
        }

        fn sink_mut(&mut self) -> Result<&mut RawLinearHistorySink, String> {
            self.sink
                .as_mut()
                .ok_or_else(|| "field sink already closed".to_owned())
        }
    }
    impl<'a> SnapshotSink<IdWithIndex<u32>, str> for LinearStringHistoryBytesSink<'a> {
        type Error = TestError;

        fn begin(&mut self, header: SnapshotHeader) -> Result<(), Self::Error> {
            self.sink_mut()?.begin(header).map_err(Into::into)
        }

        fn node(
            &mut self,
            index: usize,
            node: SnapshotNodeRef<'_, IdWithIndex<u32>, str>,
        ) -> Result<(), Self::Error> {
            let data_type = ReplicatedDataType::LinearString;
            let encoded_value = if let Some(value) = node.value {
                Some(encode_node_payload(
                    &data_type,
                    SnapshotNodeValueRef::LinearString(value),
                )?)
            } else {
                None
            };

            let raw_node = SnapshotNodeRef {
                id: node.id,
                left: node.left,
                right: node.right,
                deleted: node.deleted,
                value: encoded_value.as_deref(),
            };
            self.sink_mut()?.node(index, raw_node).map_err(Into::into)
        }

        fn end(&mut self) -> Result<(), Self::Error> {
            let mut sink = self
                .sink
                .take()
                .ok_or_else(|| "field sink already closed".to_owned())?;
            sink.end()?;
            self.output
                .insert(self.field_name.clone(), sink.into_bytes());
            Ok(())
        }
    }

    struct LinearListHistoryBytesSink<'a> {
        field_name: String,
        value_type: PrimitiveType,
        output: &'a mut HashMap<String, Bytes>,
        sink: Option<RawLinearHistorySink>,
    }
    impl<'a> LinearListHistoryBytesSink<'a> {
        fn new(
            field_name: &str,
            value_type: PrimitiveType,
            output: &'a mut HashMap<String, Bytes>,
        ) -> Self {
            Self {
                field_name: field_name.to_owned(),
                value_type,
                output,
                sink: Some(new_raw_linear_history_sink()),
            }
        }

        fn sink_mut(&mut self) -> Result<&mut RawLinearHistorySink, String> {
            self.sink
                .as_mut()
                .ok_or_else(|| "field sink already closed".to_owned())
        }
    }
    impl<'a, 'value> SnapshotSink<IdWithIndex<u32>, PrimitiveValueArrayRef<'value>>
        for LinearListHistoryBytesSink<'a>
    {
        type Error = TestError;

        fn begin(&mut self, header: SnapshotHeader) -> Result<(), Self::Error> {
            self.sink_mut()?.begin(header).map_err(Into::into)
        }

        fn node(
            &mut self,
            index: usize,
            node: SnapshotNodeRef<'_, IdWithIndex<u32>, PrimitiveValueArrayRef<'value>>,
        ) -> Result<(), Self::Error> {
            let data_type = ReplicatedDataType::LinearList {
                value_type: self.value_type,
            };
            let encoded_value = if let Some(value) = node.value {
                Some(encode_node_payload(
                    &data_type,
                    SnapshotNodeValueRef::LinearList(value.clone()),
                )?)
            } else {
                None
            };

            let raw_node = SnapshotNodeRef {
                id: node.id,
                left: node.left,
                right: node.right,
                deleted: node.deleted,
                value: encoded_value.as_deref(),
            };
            self.sink_mut()?.node(index, raw_node).map_err(Into::into)
        }

        fn end(&mut self) -> Result<(), Self::Error> {
            let mut sink = self
                .sink
                .take()
                .ok_or_else(|| "field sink already closed".to_owned())?;
            sink.end()?;
            self.output
                .insert(self.field_name.clone(), sink.into_bytes());
            Ok(())
        }
    }

    struct BytesSchemaSnapshotEncoder<'a> {
        schema: &'a Schema,
        begin_field_count: Option<usize>,
        state_fields: HashMap<String, Bytes>,
        history_fields: HashMap<String, Bytes>,
    }
    impl<'a> BytesSchemaSnapshotEncoder<'a> {
        fn new(schema: &'a Schema) -> Self {
            Self {
                schema,
                begin_field_count: None,
                state_fields: HashMap::new(),
                history_fields: HashMap::new(),
            }
        }
    }
    impl SchemaSnapshotEncoder<u32> for BytesSchemaSnapshotEncoder<'_> {
        type Error = TestError;

        type LatestValueWinsFieldSink<'a>
            = LatestValueWinsHistoryBytesSink<'a>
        where
            Self: 'a;

        type LinearStringFieldSink<'a>
            = LinearStringHistoryBytesSink<'a>
        where
            Self: 'a;

        type LinearListFieldSink<'a>
            = LinearListHistoryBytesSink<'a>
        where
            Self: 'a;

        fn begin(&mut self, field_count: usize) -> Result<(), Self::Error> {
            self.begin_field_count = Some(field_count);
            Ok(())
        }

        fn state_field(
            &mut self,
            field_name: &str,
            value: SnapshotStateValueRef<'_>,
        ) -> Result<(), Self::Error> {
            let data_type = &self
                .schema
                .columns
                .get(field_name)
                .ok_or_else(|| format!("unknown field '{field_name}'"))?
                .data_type;
            let bytes = encode_state_payload(data_type, value)?;
            self.state_fields
                .insert(field_name.to_owned(), Bytes::from(bytes));
            Ok(())
        }

        fn prepare_latest_value_wins_field<'a>(
            &'a mut self,
            field_name: &str,
            value_type: &NullableBasicDataType,
        ) -> Result<Self::LatestValueWinsFieldSink<'a>, Self::Error> {
            Ok(LatestValueWinsHistoryBytesSink::new(
                field_name,
                value_type.clone(),
                &mut self.history_fields,
            ))
        }

        fn prepare_linear_string_field<'a>(
            &'a mut self,
            field_name: &str,
        ) -> Result<Self::LinearStringFieldSink<'a>, Self::Error> {
            Ok(LinearStringHistoryBytesSink::new(
                field_name,
                &mut self.history_fields,
            ))
        }

        fn prepare_linear_list_field<'a>(
            &'a mut self,
            field_name: &str,
            value_type: PrimitiveType,
        ) -> Result<Self::LinearListFieldSink<'a>, Self::Error> {
            Ok(LinearListHistoryBytesSink::new(
                field_name,
                value_type,
                &mut self.history_fields,
            ))
        }

        fn end(&mut self) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    struct BytesDataSnapshotRowEncoder<'schema, 'row> {
        inner: &'row mut BytesSchemaSnapshotEncoder<'schema>,
    }
    impl<'schema, 'row> SchemaSnapshotEncoder<u32> for BytesDataSnapshotRowEncoder<'schema, 'row> {
        type Error = TestError;

        type LatestValueWinsFieldSink<'a>
            = <BytesSchemaSnapshotEncoder<'schema> as SchemaSnapshotEncoder<u32>>::LatestValueWinsFieldSink<'a>
        where
            Self: 'a;

        type LinearStringFieldSink<'a>
            = <BytesSchemaSnapshotEncoder<'schema> as SchemaSnapshotEncoder<u32>>::LinearStringFieldSink<'a>
        where
            Self: 'a;

        type LinearListFieldSink<'a>
            =
            <BytesSchemaSnapshotEncoder<'schema> as SchemaSnapshotEncoder<u32>>::LinearListFieldSink<
                'a,
            >
        where
            Self: 'a;

        fn begin(&mut self, field_count: usize) -> Result<(), Self::Error> {
            self.inner.begin(field_count)
        }

        fn state_field(
            &mut self,
            field_name: &str,
            value: SnapshotStateValueRef<'_>,
        ) -> Result<(), Self::Error> {
            self.inner.state_field(field_name, value)
        }

        fn prepare_latest_value_wins_field<'a>(
            &'a mut self,
            field_name: &str,
            value_type: &NullableBasicDataType,
        ) -> Result<Self::LatestValueWinsFieldSink<'a>, Self::Error> {
            self.inner
                .prepare_latest_value_wins_field(field_name, value_type)
        }

        fn prepare_linear_string_field<'a>(
            &'a mut self,
            field_name: &str,
        ) -> Result<Self::LinearStringFieldSink<'a>, Self::Error> {
            self.inner.prepare_linear_string_field(field_name)
        }

        fn prepare_linear_list_field<'a>(
            &'a mut self,
            field_name: &str,
            value_type: PrimitiveType,
        ) -> Result<Self::LinearListFieldSink<'a>, Self::Error> {
            self.inner.prepare_linear_list_field(field_name, value_type)
        }

        fn end(&mut self) -> Result<(), Self::Error> {
            self.inner.end()
        }
    }

    struct BytesDataSnapshotEncoder<'a> {
        schema: &'a Schema,
        begin_row_count: Option<usize>,
        current_row: Option<BytesSchemaSnapshotEncoder<'a>>,
        row_state_fields: Vec<HashMap<String, Bytes>>,
        row_history_fields: Vec<HashMap<String, Bytes>>,
    }
    impl<'a> BytesDataSnapshotEncoder<'a> {
        fn new(schema: &'a Schema) -> Self {
            Self {
                schema,
                begin_row_count: None,
                current_row: None,
                row_state_fields: Vec::new(),
                row_history_fields: Vec::new(),
            }
        }

        fn result(self) -> Bytes {
            assert!(self.current_row.is_none(), "row encoder still open");
            assert_eq!(
                self.row_state_fields.len(),
                self.row_history_fields.len(),
                "state/history row count mismatch",
            );

            let mut bytes = BytesMut::new();
            bytes.put_slice(&DATASET_MAGIC);
            bytes.put_u32_le(u32::try_from(self.row_state_fields.len()).expect("too many rows"));

            for row_index in 0..self.row_state_fields.len() {
                write_row_field_map(&mut bytes, &self.row_state_fields[row_index])
                    .expect("failed to encode state fields");
                write_row_field_map(&mut bytes, &self.row_history_fields[row_index])
                    .expect("failed to encode history fields");
            }

            bytes.put_u8(DATASET_END_MARKER);
            bytes.freeze()
        }
    }
    impl<'schema> DataSnapshotEncoder<u32> for BytesDataSnapshotEncoder<'schema> {
        type Error = TestError;

        type RowEncoder<'a>
            = BytesDataSnapshotRowEncoder<'schema, 'a>
        where
            Self: 'a;

        fn begin(&mut self, row_count: usize) -> Result<(), Self::Error> {
            self.begin_row_count = Some(row_count);
            self.row_state_fields = Vec::with_capacity(row_count);
            self.row_history_fields = Vec::with_capacity(row_count);
            Ok(())
        }

        fn begin_row<'a>(
            &'a mut self,
            row_index: usize,
        ) -> Result<Self::RowEncoder<'a>, Self::Error> {
            if self.current_row.is_some() {
                return Err("previous row encoder was not closed".to_owned().into());
            }
            let expected = self
                .begin_row_count
                .ok_or_else(|| "begin must be called first".to_owned())?;
            if row_index >= expected {
                return Err(
                    format!("row index {row_index} out of bounds for {expected} rows").into(),
                );
            }

            self.current_row = Some(BytesSchemaSnapshotEncoder::new(self.schema));
            Ok(BytesDataSnapshotRowEncoder {
                inner: self.current_row.as_mut().expect("row was inserted"),
            })
        }

        fn end_row(&mut self, _row_index: usize) -> Result<(), Self::Error> {
            let row = self
                .current_row
                .take()
                .ok_or_else(|| "row encoder was not opened".to_owned())?;
            self.row_state_fields.push(row.state_fields);
            self.row_history_fields.push(row.history_fields);
            Ok(())
        }

        fn end(&mut self) -> Result<(), Self::Error> {
            if self.current_row.is_some() {
                return Err("row encoder still open at end".to_owned().into());
            }
            let expected = self
                .begin_row_count
                .ok_or_else(|| "begin must be called first".to_owned())?;
            if self.row_state_fields.len() != expected || self.row_history_fields.len() != expected
            {
                return Err(format!(
                    "encoded {} rows but expected {expected}",
                    self.row_state_fields.len()
                )
                .into());
            }
            Ok(())
        }
    }

    struct VecSnapshotNodeSource<Id, Value> {
        nodes: std::vec::IntoIter<SnapshotNode<Id, Value>>,
    }
    impl<Id, Value> VecSnapshotNodeSource<Id, Value> {
        fn new(nodes: Vec<SnapshotNode<Id, Value>>) -> Self {
            Self {
                nodes: nodes.into_iter(),
            }
        }
    }
    impl<Id, Value> SnapshotNodeSource<Id, Value> for VecSnapshotNodeSource<Id, Value> {
        type Error = TestError;

        fn next_node(&mut self) -> Result<Option<SnapshotNode<Id, Value>>, Self::Error> {
            Ok(self.nodes.next())
        }
    }

    struct BytesSchemaSnapshotDecoder<'row> {
        state_fields: &'row HashMap<String, Bytes>,
        history_fields: &'row HashMap<String, Bytes>,
        expected_field_count: Option<usize>,
        decoded_field_count: usize,
    }
    impl<'row> BytesSchemaSnapshotDecoder<'row> {
        fn new(
            state_fields: &'row HashMap<String, Bytes>,
            history_fields: &'row HashMap<String, Bytes>,
        ) -> Self {
            Self {
                state_fields,
                history_fields,
                expected_field_count: None,
                decoded_field_count: 0,
            }
        }
    }
    impl SchemaSnapshotDecoder<u32> for BytesSchemaSnapshotDecoder<'_> {
        type Error = TestError;

        type LatestValueWinsFieldSource<'a>
            = VecSnapshotNodeSource<IdWithIndex<u32>, NullableBasicValue>
        where
            Self: 'a;

        type LinearStringFieldSource<'a>
            = VecSnapshotNodeSource<IdWithIndex<u32>, String>
        where
            Self: 'a;

        type LinearListFieldSource<'a>
            = VecSnapshotNodeSource<IdWithIndex<u32>, PrimitiveValueArray>
        where
            Self: 'a;

        fn begin(&mut self, expected_field_count: usize) -> Result<(), Self::Error> {
            self.expected_field_count = Some(expected_field_count);
            self.decoded_field_count = 0;
            Ok(())
        }

        fn decode_state_field(
            &mut self,
            field_name: &str,
            data_type: &ReplicatedDataType,
        ) -> Result<SnapshotStateValue, Self::Error> {
            let bytes = self
                .state_fields
                .get(field_name)
                .ok_or_else(|| format!("missing state field '{field_name}'"))?;
            let value = decode_state_payload(data_type, bytes.as_ref())?;
            self.decoded_field_count += 1;
            Ok(value)
        }

        fn prepare_latest_value_wins_field<'a>(
            &'a mut self,
            field_name: &str,
            value_type: &NullableBasicDataType,
        ) -> Result<Self::LatestValueWinsFieldSource<'a>, Self::Error> {
            let bytes = self
                .history_fields
                .get(field_name)
                .ok_or_else(|| format!("missing history field '{field_name}'"))?
                .clone();
            let data_type = ReplicatedDataType::LatestValueWins {
                value_type: value_type.clone(),
            };
            let nodes = snapshot_bytes::parse_snapshot_nodes(
                bytes,
                snapshot_bytes::decode_id_with_index_u32,
                |payload| match decode_node_payload(&data_type, payload)? {
                    SnapshotNodeValue::LatestValueWins(value) => Ok(value),
                    _ => Err("expected latest value wins node value".to_owned()),
                },
            )?;
            self.decoded_field_count += 1;
            Ok(VecSnapshotNodeSource::new(nodes))
        }

        fn prepare_linear_string_field<'a>(
            &'a mut self,
            field_name: &str,
        ) -> Result<Self::LinearStringFieldSource<'a>, Self::Error> {
            let bytes = self
                .history_fields
                .get(field_name)
                .ok_or_else(|| format!("missing history field '{field_name}'"))?
                .clone();
            let data_type = ReplicatedDataType::LinearString;
            let nodes = snapshot_bytes::parse_snapshot_nodes(
                bytes,
                snapshot_bytes::decode_id_with_index_u32,
                |payload| match decode_node_payload(&data_type, payload)? {
                    SnapshotNodeValue::LinearString(value) => Ok(value),
                    _ => Err("expected linear string node value".to_owned()),
                },
            )?;
            self.decoded_field_count += 1;
            Ok(VecSnapshotNodeSource::new(nodes))
        }

        fn prepare_linear_list_field<'a>(
            &'a mut self,
            field_name: &str,
            value_type: PrimitiveType,
        ) -> Result<Self::LinearListFieldSource<'a>, Self::Error> {
            let bytes = self
                .history_fields
                .get(field_name)
                .ok_or_else(|| format!("missing history field '{field_name}'"))?
                .clone();
            let data_type = ReplicatedDataType::LinearList { value_type };
            let nodes = snapshot_bytes::parse_snapshot_nodes(
                bytes,
                snapshot_bytes::decode_id_with_index_u32,
                |payload| match decode_node_payload(&data_type, payload)? {
                    SnapshotNodeValue::LinearList(value) => Ok(value),
                    _ => Err("expected linear list node value".to_owned()),
                },
            )?;
            self.decoded_field_count += 1;
            Ok(VecSnapshotNodeSource::new(nodes))
        }

        fn end(&mut self) -> Result<(), Self::Error> {
            let expected = self
                .expected_field_count
                .ok_or_else(|| "begin must be called first".to_owned())?;
            if self.decoded_field_count != expected {
                return Err(format!(
                    "decoded {} fields but expected {expected}",
                    self.decoded_field_count
                )
                .into());
            }
            Ok(())
        }
    }

    struct BytesDataSnapshotDecoder {
        row_state_fields: Vec<HashMap<String, Bytes>>,
        row_history_fields: Vec<HashMap<String, Bytes>>,
        began: bool,
        open_row: Option<usize>,
    }
    impl BytesDataSnapshotDecoder {
        fn try_from_bytes(bytes: Bytes) -> Result<Self, TestError> {
            let mut input = bytes;
            if input.remaining() < DATASET_MAGIC.len() {
                return Err("dataset payload missing magic".to_owned().into());
            }
            let magic = input.copy_to_bytes(DATASET_MAGIC.len());
            if magic.as_ref() != DATASET_MAGIC.as_slice() {
                return Err("invalid dataset payload magic".to_owned().into());
            }

            let row_count: usize = snapshot_bytes::read_u32(&mut input)
                .map_err(TestError::from)?
                .try_into()
                .unwrap();
            let mut row_state_fields = Vec::with_capacity(row_count);
            let mut row_history_fields = Vec::with_capacity(row_count);
            for _ in 0..row_count {
                row_state_fields.push(read_row_field_map(&mut input).map_err(TestError::from)?);
                row_history_fields.push(read_row_field_map(&mut input).map_err(TestError::from)?);
            }

            let end = snapshot_bytes::read_u8(&mut input).map_err(TestError::from)?;
            if end != DATASET_END_MARKER {
                return Err("invalid dataset payload end marker".to_owned().into());
            }
            if input.has_remaining() {
                return Err("trailing bytes in dataset payload".to_owned().into());
            }

            Ok(Self {
                row_state_fields,
                row_history_fields,
                began: false,
                open_row: None,
            })
        }
    }
    impl DataSnapshotDecoder<u32> for BytesDataSnapshotDecoder {
        type Error = TestError;

        type RowDecoder<'a>
            = BytesSchemaSnapshotDecoder<'a>
        where
            Self: 'a;

        fn begin(&mut self) -> Result<usize, Self::Error> {
            if self.row_state_fields.len() != self.row_history_fields.len() {
                return Err("state and history row counts mismatch".to_owned().into());
            }
            self.began = true;
            Ok(self.row_state_fields.len())
        }

        fn begin_row<'a>(
            &'a mut self,
            row_index: usize,
        ) -> Result<Self::RowDecoder<'a>, Self::Error> {
            if !self.began {
                return Err("begin must be called first".to_owned().into());
            }
            if self.open_row.is_some() {
                return Err("previous row decoder was not closed".to_owned().into());
            }
            if row_index >= self.row_state_fields.len() {
                return Err(format!("row index {row_index} out of bounds").into());
            }

            self.open_row = Some(row_index);
            Ok(BytesSchemaSnapshotDecoder::new(
                &self.row_state_fields[row_index],
                &self.row_history_fields[row_index],
            ))
        }

        fn end_row(&mut self, row_index: usize) -> Result<(), Self::Error> {
            match self.open_row {
                Some(open_row) if open_row == row_index => {
                    self.open_row = None;
                    Ok(())
                }
                Some(open_row) => {
                    Err(format!("ending row {row_index} but row {open_row} is open").into())
                }
                None => Err("no row decoder is open".to_owned().into()),
            }
        }

        fn end(&mut self) -> Result<(), Self::Error> {
            if !self.began {
                return Err("begin must be called first".to_owned().into());
            }
            if self.open_row.is_some() {
                return Err("row decoder still open at end".to_owned().into());
            }
            Ok(())
        }
    }

    static TEST_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
        let mut columns = HashMap::new();
        columns.insert(
            "latest".to_owned(),
            Field {
                name: "latest".to_owned(),
                data_type: ReplicatedDataType::LatestValueWins {
                    value_type: NullableBasicDataType::Nullable(BasicDataType::Primitive(
                        PrimitiveType::UInt,
                    )),
                },
                metadata: HashMap::new(),
            },
        );
        columns.insert(
            "title".to_owned(),
            Field {
                name: "title".to_owned(),
                data_type: ReplicatedDataType::LinearString,
                metadata: HashMap::new(),
            },
        );
        columns.insert(
            "numbers".to_owned(),
            Field {
                name: "numbers".to_owned(),
                data_type: ReplicatedDataType::LinearList {
                    value_type: PrimitiveType::Int,
                },
                metadata: HashMap::new(),
            },
        );
        columns.insert(
            "counter".to_owned(),
            Field {
                name: "counter".to_owned(),
                data_type: ReplicatedDataType::MonotonicCounter { small_range: false },
                metadata: HashMap::new(),
            },
        );
        columns.insert(
            "priority".to_owned(),
            Field {
                name: "priority".to_owned(),
                data_type: ReplicatedDataType::TotalOrderRegister {
                    value_type: PrimitiveType::UInt,
                    direction: Direction::Ascending,
                },
                metadata: HashMap::new(),
            },
        );
        columns.insert(
            "status".to_owned(),
            Field {
                name: "status".to_owned(),
                data_type: ReplicatedDataType::TotalOrderFiniteStateRegister {
                    value_type: NullablePrimitiveType::Nullable(PrimitiveType::String),
                    states: NullablePrimitiveValueArray::Nullable {
                        values: PrimitiveValueArray::String(vec![
                            "draft".to_owned(),
                            "published".to_owned(),
                        ]),
                        null_index: 1,
                    },
                },
                metadata: HashMap::new(),
            },
        );

        Schema {
            columns,
            metadata: HashMap::new(),
        }
    });

    #[test]
    fn snapshot_node_value_roundtrips_via_bytes_codec() {
        let lvw_type = ReplicatedDataType::LatestValueWins {
            value_type: NullableBasicDataType::Nullable(BasicDataType::Primitive(
                PrimitiveType::UInt,
            )),
        };
        let string_type = ReplicatedDataType::LinearString;
        let list_type = ReplicatedDataType::LinearList {
            value_type: PrimitiveType::Int,
        };

        let cases = vec![
            (
                &lvw_type,
                SnapshotNodeValue::LatestValueWins(NullableBasicValue::Null),
            ),
            (
                &lvw_type,
                SnapshotNodeValue::LatestValueWins(NullableBasicValue::Value(
                    BasicValue::Primitive(PrimitiveValue::UInt(42)),
                )),
            ),
            (
                &string_type,
                SnapshotNodeValue::LinearString("hello world".to_owned()),
            ),
            (
                &list_type,
                SnapshotNodeValue::LinearList(PrimitiveValueArray::Int(vec![-1, 0, 9])),
            ),
        ];

        for (data_type, value) in cases {
            let encoded = encode_node_payload(data_type, value.as_ref()).unwrap();
            let decoded = decode_node_payload(data_type, &encoded).unwrap();
            assert_eq!(decoded, value);
        }
    }

    #[test]
    fn snapshot_state_value_roundtrips_via_bytes_codec() {
        let counter_type = ReplicatedDataType::MonotonicCounter { small_range: false };
        let register_type = ReplicatedDataType::TotalOrderRegister {
            value_type: PrimitiveType::UInt,
            direction: Direction::Ascending,
        };
        let finite_type = ReplicatedDataType::TotalOrderFiniteStateRegister {
            value_type: NullablePrimitiveType::Nullable(PrimitiveType::String),
            states: NullablePrimitiveValueArray::Nullable {
                values: PrimitiveValueArray::String(vec![
                    "draft".to_owned(),
                    "published".to_owned(),
                ]),
                null_index: 1,
            },
        };

        let cases = vec![
            (
                &counter_type,
                SnapshotStateValue::MonotonicCounter(CounterValue::UInt(9)),
            ),
            (
                &register_type,
                SnapshotStateValue::TotalOrderRegister(PrimitiveValue::UInt(33)),
            ),
            (
                &finite_type,
                SnapshotStateValue::TotalOrderFiniteStateRegister(NullablePrimitiveValue::Null),
            ),
            (
                &finite_type,
                SnapshotStateValue::TotalOrderFiniteStateRegister(NullablePrimitiveValue::Value(
                    PrimitiveValue::String("published".to_owned()),
                )),
            ),
        ];

        for (data_type, value) in cases {
            let encoded = encode_state_payload(data_type, value.as_ref()).unwrap();
            let decoded = decode_state_payload(data_type, &encoded).unwrap();
            assert_eq!(decoded, value);
        }
    }

    #[test]
    fn schema_snapshot_roundtrips_history_and_state_via_bytes() {
        let schema = &*TEST_SCHEMA;

        let mut lvw_id_generator = 0u32..;
        let latest_initial_ids = [
            IdWithIndex::zero(lvw_id_generator.next().unwrap()),
            IdWithIndex::zero(lvw_id_generator.next().unwrap()),
            IdWithIndex::zero(lvw_id_generator.next().unwrap()),
        ];
        let mut latest = LinearLatestValueWins::new(Some(1u64), latest_initial_ids);
        latest.update(IdWithIndex::zero(100), None);
        latest.update(IdWithIndex::zero(101), Some(99));

        let mut title_id_generator = 1000u32..;
        let mut title =
            LinearString::with_value("alpha".to_owned(), title_id_generator.next().unwrap());
        title.append(IdWithIndex::zero(55), " beta".to_owned());
        let title_range = title.ids_in_range(1..=2).unwrap();
        title_range.delete(&mut title).unwrap();

        let mut numbers_id_generator = 2000u32..;
        let mut numbers =
            LinearList::with_values([10i64, 20, 30], numbers_id_generator.next().unwrap());
        numbers.append(IdWithIndex::zero(77), [40, 50]);
        let _ = numbers.delete_at(1);

        let counter = CounterValue::UInt(12);
        let priority = PrimitiveValue::UInt(7);
        let status = NullablePrimitiveValue::Value(PrimitiveValue::String("published".to_owned()));

        let mut data: InMemoryData<(), u32> = InMemoryData::with_owned_schema(schema.clone());
        data.push_row_from_named_fields([
            (
                "latest",
                InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableUInt(
                    latest.clone(),
                )),
            ),
            ("title", InMemoryFieldValue::LinearString(title.clone())),
            (
                "numbers",
                InMemoryFieldValue::LinearList(LinearListValue::Int(numbers.clone())),
            ),
            ("counter", InMemoryFieldValue::MonotonicCounter(counter)),
            (
                "priority",
                InMemoryFieldValue::TotalOrderRegister(priority.clone()),
            ),
            (
                "status",
                InMemoryFieldValue::TotalOrderFiniteStateRegister(status.clone()),
            ),
        ])
        .unwrap();

        let mut encoder = BytesDataSnapshotEncoder::new(schema);
        data.encode_data_snapshots(&mut encoder).unwrap();
        let bytes = encoder.result();

        let mut decoder = BytesDataSnapshotDecoder::try_from_bytes(bytes).unwrap();
        let roundtrip =
            InMemoryData::decode_data_snapshots(Cow::Borrowed(schema), &mut decoder).unwrap();
        assert_eq!(roundtrip, data);
    }
}
