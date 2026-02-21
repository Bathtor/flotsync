use super::*;
use crate::snapshot::SnapshotSink;
use std::{collections::HashSet, convert::Infallible, marker::PhantomData};

/// Snapshot node payload shapes for history-based CRDT snapshots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotNodeValue {
    LatestValueWins(NullableBasicValue),
    LinearString(String),
    LinearList(PrimitiveValueArray),
}
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotNodeValueRef<'a> {
    LatestValueWins(NullableBasicValueRef<'a>),
    LinearString(&'a str),
    LinearList(PrimitiveValueArrayRef<'a>),
}
impl SnapshotNodeValueRef<'_> {
    pub fn into_owned(self) -> SnapshotNodeValue {
        match self {
            Self::LatestValueWins(value) => SnapshotNodeValue::LatestValueWins(value.into_owned()),
            Self::LinearString(value) => SnapshotNodeValue::LinearString(value.to_owned()),
            Self::LinearList(values) => SnapshotNodeValue::LinearList(values.into_owned()),
        }
    }
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
/// 1. `prepare_schema_snapshot` (or manual `begin`)
/// 2. `state_field` and `prepare_*_field` as needed
/// 3. stream history nodes via `SnapshotSink::begin/node/end` (typically by calling CRDT
///    `visit_snapshot` directly on the prepared field sink)
/// 4. `end`
pub trait SchemaSnapshotVisitor<Id> {
    type Error;

    type LatestValueWinsFieldSink<'a>: for<'value> SnapshotSink<Id, NullableBasicValueRef<'value>, Error = Self::Error>
    where
        Self: 'a,
        Id: 'a;

    type LinearStringFieldSink<'a>: SnapshotSink<Id, str, Error = Self::Error>
    where
        Self: 'a,
        Id: 'a;

    type LinearListFieldSink<'a>: for<'value> SnapshotSink<Id, PrimitiveValueArrayRef<'value>, Error = Self::Error>
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
/// Use `prepare_*_field` to obtain a field-local sink and pass it to CRDT `visit_snapshot`.
///
/// Typical flow:
/// 1. `prepare_schema_snapshot`
/// 2. call `state_field` and `prepare_*_field` for all fields
/// 3. for each prepared history field sink, call CRDT `visit_snapshot`
/// 4. `end`
///
/// Example:
/// ```ignore
/// let mut snapshot = prepare_schema_snapshot(&mut visitor, schema)?;
///
/// snapshot.state_field("counter", SnapshotStateValueRef::MonotonicCounter(...))?;
///
/// // LinearString can stream directly.
/// let mut title_sink = snapshot.prepare_linear_string_field("title")?;
/// title.visit_snapshot(&mut title_sink)?;
///
/// // For LinearList / LatestValueWins, wrap/adapt the sink so CRDT node values are
/// // converted into PrimitiveValueArrayRef / BasicValueRef respectively, then call
/// // CRDT visit_snapshot on that adapter.
/// snapshot.end()?;
/// ```
pub struct SchemaSnapshotWriter<'a, Id, V>
where
    V: SchemaSnapshotVisitor<Id>,
{
    schema: &'a Schema,
    visitor: &'a mut V,
    seen_fields: HashSet<String>,
    _id_marker: PhantomData<Id>,
}
impl<'a, Id, V> SchemaSnapshotWriter<'a, Id, V>
where
    V: SchemaSnapshotVisitor<Id>,
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

    pub fn state_field(
        &mut self,
        field_name: &str,
        value: SnapshotStateValueRef<'_>,
    ) -> Result<(), SchemaVisitError<V::Error>> {
        self.register_field(field_name)
            .map_err(SchemaVisitError::InvalidSchemaValue)?;
        let schema_field = self
            .schema
            .columns
            .get(field_name)
            .expect("field existence was validated");

        if is_history_data_type(&schema_field.data_type) {
            return Err(SchemaVisitError::InvalidSchemaValue(
                SchemaValueError::InvalidSnapshotFieldValue {
                    field_name: field_name.to_owned(),
                    source: DataModelValueError::InvalidSnapshotValueForType,
                },
            ));
        }

        validate_snapshot_state_value_for_type(&schema_field.data_type, value.clone()).map_err(
            |source| {
                SchemaVisitError::InvalidSchemaValue(SchemaValueError::InvalidSnapshotFieldValue {
                    field_name: field_name.to_owned(),
                    source,
                })
            },
        )?;

        self.visitor
            .state_field(field_name, value)
            .map_err(SchemaVisitError::Visitor)
    }

    pub fn prepare_latest_value_wins_field<'s>(
        &'s mut self,
        field_name: &str,
    ) -> Result<V::LatestValueWinsFieldSink<'s>, SchemaVisitError<V::Error>> {
        self.register_field(field_name)
            .map_err(SchemaVisitError::InvalidSchemaValue)?;
        let schema_field = self
            .schema
            .columns
            .get(field_name)
            .expect("field existence was validated");
        let ReplicatedDataType::LatestValueWins { value_type } = &schema_field.data_type else {
            return Err(SchemaVisitError::InvalidSchemaValue(
                SchemaValueError::InvalidSnapshotFieldValue {
                    field_name: field_name.to_owned(),
                    source: DataModelValueError::InvalidSnapshotValueForType,
                },
            ));
        };

        self.visitor
            .prepare_latest_value_wins_field(field_name, value_type)
            .map_err(SchemaVisitError::Visitor)
    }

    pub fn prepare_linear_string_field<'s>(
        &'s mut self,
        field_name: &str,
    ) -> Result<V::LinearStringFieldSink<'s>, SchemaVisitError<V::Error>> {
        self.register_field(field_name)
            .map_err(SchemaVisitError::InvalidSchemaValue)?;
        let schema_field = self
            .schema
            .columns
            .get(field_name)
            .expect("field existence was validated");
        if !matches!(schema_field.data_type, ReplicatedDataType::LinearString) {
            return Err(SchemaVisitError::InvalidSchemaValue(
                SchemaValueError::InvalidSnapshotFieldValue {
                    field_name: field_name.to_owned(),
                    source: DataModelValueError::InvalidSnapshotValueForType,
                },
            ));
        }

        self.visitor
            .prepare_linear_string_field(field_name)
            .map_err(SchemaVisitError::Visitor)
    }

    pub fn prepare_linear_list_field<'s>(
        &'s mut self,
        field_name: &str,
    ) -> Result<V::LinearListFieldSink<'s>, SchemaVisitError<V::Error>> {
        self.register_field(field_name)
            .map_err(SchemaVisitError::InvalidSchemaValue)?;
        let schema_field = self
            .schema
            .columns
            .get(field_name)
            .expect("field existence was validated");
        let ReplicatedDataType::LinearList { value_type } = &schema_field.data_type else {
            return Err(SchemaVisitError::InvalidSchemaValue(
                SchemaValueError::InvalidSnapshotFieldValue {
                    field_name: field_name.to_owned(),
                    source: DataModelValueError::InvalidSnapshotValueForType,
                },
            ));
        };

        self.visitor
            .prepare_linear_list_field(field_name, *value_type)
            .map_err(SchemaVisitError::Visitor)
    }

    pub fn end(self) -> Result<(), SchemaVisitError<V::Error>> {
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
                return Err(SchemaVisitError::InvalidSchemaValue(
                    SchemaValueError::MissingField {
                        field_name: (*missing_field).to_owned(),
                    },
                ));
            }
        }

        self.visitor.end().map_err(SchemaVisitError::Visitor)
    }
}

/// Prepare a streaming schema snapshot writer.
///
/// This calls `SchemaSnapshotVisitor::begin` immediately with `schema.columns.len()`.
/// The resulting writer enforces:
/// - only known schema fields can be emitted
/// - no duplicate fields
/// - complete snapshots (`end` fails if any field is missing)
pub fn prepare_schema_snapshot<'a, Id, V>(
    visitor: &'a mut V,
    schema: &'a Schema,
) -> Result<SchemaSnapshotWriter<'a, Id, V>, SchemaVisitError<V::Error>>
where
    V: SchemaSnapshotVisitor<Id>,
{
    visitor
        .begin(schema.columns.len())
        .map_err(SchemaVisitError::Visitor)?;

    Ok(SchemaSnapshotWriter {
        schema,
        visitor,
        seen_fields: HashSet::new(),
        _id_marker: PhantomData,
    })
}

/// Visitor used by serializers to encode one history node payload at a time.
pub trait SnapshotNodeValueVisitor {
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
pub trait SnapshotNodeValueDecoder {
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
pub trait SnapshotStateValueVisitor {
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
pub trait SnapshotStateValueDecoder {
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
pub fn visit_snapshot_node_value<V>(
    visitor: &mut V,
    data_type: &ReplicatedDataType,
    value: SnapshotNodeValueRef<'_>,
) -> Result<(), VisitError<V::Error>>
where
    V: SnapshotNodeValueVisitor,
{
    match (data_type, value) {
        (
            ReplicatedDataType::LatestValueWins { value_type },
            SnapshotNodeValueRef::LatestValueWins(v),
        ) => {
            ensure_nullable_basic_type(value_type, &v).map_err(VisitError::InvalidValue)?;
            visitor
                .visit_latest_value_wins_node(value_type, v)
                .map_err(VisitError::Visitor)
        }
        (ReplicatedDataType::LinearString, SnapshotNodeValueRef::LinearString(v)) => visitor
            .visit_linear_string_node(v)
            .map_err(VisitError::Visitor),
        (ReplicatedDataType::LinearList { value_type }, SnapshotNodeValueRef::LinearList(v)) => {
            ensure_primitive_array_type(*value_type, v.primitive_type())
                .map_err(VisitError::InvalidValue)?;
            visitor
                .visit_linear_list_node(*value_type, v)
                .map_err(VisitError::Visitor)
        }
        _ => Err(VisitError::InvalidValue(
            DataModelValueError::InvalidSnapshotValueForType,
        )),
    }
}

/// Decode + validate one history node payload lazily for the provided schema data type.
pub fn decode_snapshot_node_value<D>(
    decoder: &mut D,
    data_type: &ReplicatedDataType,
) -> Result<SnapshotNodeValue, DecodeError<D::Error>>
where
    D: SnapshotNodeValueDecoder,
{
    match data_type {
        ReplicatedDataType::LatestValueWins { value_type } => {
            let value = decoder
                .decode_latest_value_wins_node(value_type)
                .map_err(DecodeError::Decoder)?;
            ensure_nullable_basic_type(value_type, &value.as_ref())
                .map_err(DecodeError::InvalidValue)?;
            Ok(SnapshotNodeValue::LatestValueWins(value))
        }
        ReplicatedDataType::LinearString => decoder
            .decode_linear_string_node()
            .map(SnapshotNodeValue::LinearString)
            .map_err(DecodeError::Decoder),
        ReplicatedDataType::LinearList { value_type } => {
            let values = decoder
                .decode_linear_list_node(*value_type)
                .map_err(DecodeError::Decoder)?;
            ensure_primitive_array_type(*value_type, values.primitive_type())
                .map_err(DecodeError::InvalidValue)?;
            Ok(SnapshotNodeValue::LinearList(values))
        }
        ReplicatedDataType::MonotonicCounter { .. }
        | ReplicatedDataType::TotalOrderRegister { .. }
        | ReplicatedDataType::TotalOrderFiniteStateRegister { .. } => Err(
            DecodeError::InvalidValue(DataModelValueError::InvalidSnapshotValueForType),
        ),
    }
}

/// Dispatch + validate one state snapshot value against its schema data type before visiting.
pub fn visit_snapshot_state_value<V>(
    visitor: &mut V,
    data_type: &ReplicatedDataType,
    value: SnapshotStateValueRef<'_>,
) -> Result<(), VisitError<V::Error>>
where
    V: SnapshotStateValueVisitor,
{
    match (data_type, value) {
        (
            ReplicatedDataType::MonotonicCounter { small_range },
            SnapshotStateValueRef::MonotonicCounter(v),
        ) => {
            ensure_counter_type(*small_range, v).map_err(VisitError::InvalidValue)?;
            visitor
                .visit_monotonic_counter(*small_range, v)
                .map_err(VisitError::Visitor)
        }
        (
            ReplicatedDataType::TotalOrderRegister { value_type, .. },
            SnapshotStateValueRef::TotalOrderRegister(v),
        ) => {
            ensure_primitive_type(*value_type, v.value_type()).map_err(VisitError::InvalidValue)?;
            visitor
                .visit_total_order_register(*value_type, v)
                .map_err(VisitError::Visitor)
        }
        (
            ReplicatedDataType::TotalOrderFiniteStateRegister { value_type, states },
            SnapshotStateValueRef::TotalOrderFiniteStateRegister(v),
        ) => {
            ensure_finite_state_value(*value_type, states, &v).map_err(VisitError::InvalidValue)?;
            visitor
                .visit_total_order_finite_state_register(*value_type, states, v)
                .map_err(VisitError::Visitor)
        }
        _ => Err(VisitError::InvalidValue(
            DataModelValueError::InvalidSnapshotValueForType,
        )),
    }
}

/// Decode + validate one state snapshot value lazily for the provided schema data type.
pub fn decode_snapshot_state_value<D>(
    decoder: &mut D,
    data_type: &ReplicatedDataType,
) -> Result<SnapshotStateValue, DecodeError<D::Error>>
where
    D: SnapshotStateValueDecoder,
{
    match data_type {
        ReplicatedDataType::MonotonicCounter { small_range } => {
            let value = decoder
                .decode_monotonic_counter(*small_range)
                .map_err(DecodeError::Decoder)?;
            ensure_counter_type(*small_range, value.as_ref()).map_err(DecodeError::InvalidValue)?;
            Ok(SnapshotStateValue::MonotonicCounter(value))
        }
        ReplicatedDataType::TotalOrderRegister { value_type, .. } => {
            let value = decoder
                .decode_total_order_register(*value_type)
                .map_err(DecodeError::Decoder)?;
            ensure_primitive_type(*value_type, value.primitive_type())
                .map_err(DecodeError::InvalidValue)?;
            Ok(SnapshotStateValue::TotalOrderRegister(value))
        }
        ReplicatedDataType::TotalOrderFiniteStateRegister { value_type, states } => {
            let value = decoder
                .decode_total_order_finite_state_register(*value_type, states)
                .map_err(DecodeError::Decoder)?;
            ensure_finite_state_value(*value_type, states, &value.as_ref())
                .map_err(DecodeError::InvalidValue)?;
            Ok(SnapshotStateValue::TotalOrderFiniteStateRegister(value))
        }
        ReplicatedDataType::LatestValueWins { .. }
        | ReplicatedDataType::LinearString
        | ReplicatedDataType::LinearList { .. } => Err(DecodeError::InvalidValue(
            DataModelValueError::InvalidSnapshotValueForType,
        )),
    }
}
fn validate_snapshot_state_value_for_type(
    data_type: &ReplicatedDataType,
    value: SnapshotStateValueRef<'_>,
) -> Result<(), DataModelValueError> {
    let mut visitor = NoopSnapshotStateValueVisitor;
    match visit_snapshot_state_value(&mut visitor, data_type, value) {
        Ok(()) => Ok(()),
        Err(VisitError::InvalidValue(error)) => Err(error),
        Err(VisitError::Visitor(never)) => match never {},
    }
}
struct NoopSnapshotStateValueVisitor;
impl SnapshotStateValueVisitor for NoopSnapshotStateValueVisitor {
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
