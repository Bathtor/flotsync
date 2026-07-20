//! In-memory row storage and CRDT field-state representations.

use super::*;

/// Storage-level in-memory representation for one row over an associated schema.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct InMemoryStateRow<OperationId> {
    pub(crate) deleted: bool,
    pub(crate) fields: Vec<InMemoryFieldState<OperationId>>,
}
impl<OperationId> InMemoryStateRow<OperationId> {
    pub(super) fn new(fields: Vec<InMemoryFieldState<OperationId>>) -> Self {
        Self {
            deleted: false,
            fields,
        }
    }

    pub(super) fn field_count(&self) -> usize {
        self.fields.len()
    }

    pub(super) fn encode_snapshot<V>(
        &self,
        schema: &Schema,
        field_names: &[String],
        encoder: &mut V,
    ) -> Result<(), SchemaVisitError<V::Error>>
    where
        OperationId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
        V: SchemaSnapshotEncoder<OperationId>,
    {
        let mut writer = prepare_schema_snapshot_encoder(encoder, schema)?;
        self.encode_snapshot_fields(field_names, &mut writer)?;
        writer.end()
    }

    pub(crate) fn encode_snapshot_fields<V>(
        &self,
        field_names: &[String],
        writer: &mut SchemaSnapshotEncodingWriter<'_, OperationId, V>,
    ) -> Result<(), SchemaVisitError<V::Error>>
    where
        OperationId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
        V: SchemaSnapshotEncoder<OperationId>,
    {
        debug_assert_eq!(field_names.len(), self.fields.len());
        for (field_name, field_value) in field_names
            .iter()
            .map(String::as_str)
            .zip(self.fields.iter())
        {
            field_value.encode_snapshot_field(field_name, writer)?;
        }
        Ok(())
    }

    pub(super) fn decode_snapshot<D>(
        schema: &Schema,
        field_names: &[String],
        decoder: &mut D,
    ) -> Result<Self, InMemoryStateDataSnapshotDecodeError<D::Error>>
    where
        OperationId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
        D: SchemaSnapshotDecoder<OperationId>,
    {
        decoder.begin(field_names.len()).context(DecoderSnafu)?;

        let mut fields = Vec::with_capacity(field_names.len());
        for field_name in field_names {
            let schema_field = schema
                .columns
                .get(field_name.as_str())
                .expect("field names and schema are in sync");
            let field_value = InMemoryFieldState::decode_snapshot_field(
                field_name.as_str(),
                schema_field,
                decoder,
            )?;
            fields.push(field_value);
        }

        decoder.end().context(DecoderSnafu)?;
        Ok(Self {
            deleted: false,
            fields,
        })
    }
}

/// In-memory CRDT state for one schema field.
#[derive(Clone, Debug, PartialEq)]
pub enum InMemoryFieldState<OperationId> {
    LatestValueWins(LinearLatestValueWinsState<OperationId>),
    LinearString(LinearString<OperationId>),
    LinearList(LinearListState<OperationId>),
    MonotonicCounter(CounterValue),
    TotalOrderRegister(PrimitiveValue),
    TotalOrderFiniteStateRegister(NullablePrimitiveValue),
}

impl<OperationId> InMemoryFieldState<OperationId>
where
    OperationId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
{
    /// Project this CRDT state to its current application-visible value.
    #[must_use]
    pub fn project_value(&self) -> ProjectedFieldValue<'_> {
        match self {
            Self::LatestValueWins(value) => ProjectedFieldValue::from(value.project_value()),
            Self::LinearString(value) => {
                ProjectedFieldValue::from(PrimitiveValue::String(value.to_string()))
            }
            Self::LinearList(value) => ProjectedFieldValue::from(value.project_value()),
            Self::MonotonicCounter(value) => ProjectedFieldValue::from(value.project_value()),
            Self::TotalOrderRegister(value) => ProjectedFieldValue::from(value.as_ref()),
            Self::TotalOrderFiniteStateRegister(NullablePrimitiveValue::Null) => {
                ProjectedFieldValue::from(NullableBasicValueRef::Null)
            }
            Self::TotalOrderFiniteStateRegister(NullablePrimitiveValue::Value(value)) => {
                ProjectedFieldValue::from(value.as_ref())
            }
        }
    }
}

fn projected_primitive(value: PrimitiveValueRef<'_>) -> NullableBasicValueRef<'_> {
    NullableBasicValueRef::Value(BasicValueRef::Primitive(value))
}

fn projected_array(value: PrimitiveValueArrayRef<'_>) -> NullableBasicValueRef<'_> {
    NullableBasicValueRef::Value(BasicValueRef::Array(value))
}

fn projected_nullable_primitive(value: Option<PrimitiveValueRef<'_>>) -> NullableBasicValueRef<'_> {
    match value {
        Some(value) => projected_primitive(value),
        None => NullableBasicValueRef::Null,
    }
}

fn projected_nullable_array(
    value: Option<PrimitiveValueArrayRef<'_>>,
) -> NullableBasicValueRef<'_> {
    match value {
        Some(value) => projected_array(value),
        None => NullableBasicValueRef::Null,
    }
}

/// Specialized `LinearLatestValueWins` state variants by concrete value type.
#[derive(Clone, Debug, PartialEq)]
pub enum LinearLatestValueWinsState<OperationId> {
    String(LinearLatestValueWins<IdWithIndex<OperationId>, String>),
    UInt(LinearLatestValueWins<IdWithIndex<OperationId>, u64>),
    Int(LinearLatestValueWins<IdWithIndex<OperationId>, i64>),
    Byte(LinearLatestValueWins<IdWithIndex<OperationId>, u8>),
    Float(LinearLatestValueWins<IdWithIndex<OperationId>, OrderedFloat<f64>>),
    Boolean(LinearLatestValueWins<IdWithIndex<OperationId>, bool>),
    Binary(LinearLatestValueWins<IdWithIndex<OperationId>, Vec<u8>>),
    Date(LinearLatestValueWins<IdWithIndex<OperationId>, NaiveDate>),
    Timestamp(LinearLatestValueWins<IdWithIndex<OperationId>, UnixTimestamp>),
    StringArray(LinearLatestValueWins<IdWithIndex<OperationId>, Vec<String>>),
    UIntArray(LinearLatestValueWins<IdWithIndex<OperationId>, Vec<u64>>),
    IntArray(LinearLatestValueWins<IdWithIndex<OperationId>, Vec<i64>>),
    ByteArray(LinearLatestValueWins<IdWithIndex<OperationId>, Vec<u8>>),
    FloatArray(LinearLatestValueWins<IdWithIndex<OperationId>, Vec<OrderedFloat<f64>>>),
    BooleanArray(LinearLatestValueWins<IdWithIndex<OperationId>, Vec<bool>>),
    BinaryArray(LinearLatestValueWins<IdWithIndex<OperationId>, Vec<Vec<u8>>>),
    DateArray(LinearLatestValueWins<IdWithIndex<OperationId>, Vec<NaiveDate>>),
    TimestampArray(LinearLatestValueWins<IdWithIndex<OperationId>, Vec<UnixTimestamp>>),
    NullableString(LinearLatestValueWins<IdWithIndex<OperationId>, Option<String>>),
    NullableUInt(LinearLatestValueWins<IdWithIndex<OperationId>, Option<u64>>),
    NullableInt(LinearLatestValueWins<IdWithIndex<OperationId>, Option<i64>>),
    NullableByte(LinearLatestValueWins<IdWithIndex<OperationId>, Option<u8>>),
    NullableFloat(LinearLatestValueWins<IdWithIndex<OperationId>, Option<OrderedFloat<f64>>>),
    NullableBoolean(LinearLatestValueWins<IdWithIndex<OperationId>, Option<bool>>),
    NullableBinary(LinearLatestValueWins<IdWithIndex<OperationId>, Option<Vec<u8>>>),
    NullableDate(LinearLatestValueWins<IdWithIndex<OperationId>, Option<NaiveDate>>),
    NullableTimestamp(LinearLatestValueWins<IdWithIndex<OperationId>, Option<UnixTimestamp>>),
    NullableStringArray(LinearLatestValueWins<IdWithIndex<OperationId>, Option<Vec<String>>>),
    NullableUIntArray(LinearLatestValueWins<IdWithIndex<OperationId>, Option<Vec<u64>>>),
    NullableIntArray(LinearLatestValueWins<IdWithIndex<OperationId>, Option<Vec<i64>>>),
    NullableByteArray(LinearLatestValueWins<IdWithIndex<OperationId>, Option<Vec<u8>>>),
    NullableFloatArray(
        LinearLatestValueWins<IdWithIndex<OperationId>, Option<Vec<OrderedFloat<f64>>>>,
    ),
    NullableBooleanArray(LinearLatestValueWins<IdWithIndex<OperationId>, Option<Vec<bool>>>),
    NullableBinaryArray(LinearLatestValueWins<IdWithIndex<OperationId>, Option<Vec<Vec<u8>>>>),
    NullableDateArray(LinearLatestValueWins<IdWithIndex<OperationId>, Option<Vec<NaiveDate>>>),
    NullableTimestampArray(
        LinearLatestValueWins<IdWithIndex<OperationId>, Option<Vec<UnixTimestamp>>>,
    ),
}
impl<OperationId> LinearLatestValueWinsState<OperationId> {
    /// Project this CRDT register to its current application-visible value.
    #[must_use]
    #[allow(
        clippy::too_many_lines,
        reason = "The method is an explicit matrix between typed register variants and projected value shapes."
    )]
    pub fn project_value(&self) -> NullableBasicValueRef<'_>
    where
        OperationId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
    {
        match self {
            Self::String(value) => {
                projected_primitive(PrimitiveValueRef::String(value.content().as_str()))
            }
            Self::UInt(value) => projected_primitive(PrimitiveValueRef::UInt(*value.content())),
            Self::Int(value) => projected_primitive(PrimitiveValueRef::Int(*value.content())),
            Self::Byte(value) => projected_primitive(PrimitiveValueRef::Byte(*value.content())),
            Self::Float(value) => projected_primitive(PrimitiveValueRef::Float(*value.content())),
            Self::Boolean(value) => {
                projected_primitive(PrimitiveValueRef::Boolean(*value.content()))
            }
            Self::Binary(value) => {
                projected_primitive(PrimitiveValueRef::Binary(value.content().as_slice()))
            }
            Self::Date(value) => projected_primitive(PrimitiveValueRef::Date(*value.content())),
            Self::Timestamp(value) => {
                projected_primitive(PrimitiveValueRef::Timestamp(*value.content()))
            }
            Self::StringArray(value) => {
                projected_array(PrimitiveValueArrayRef::String(value.content().as_slice()))
            }
            Self::UIntArray(value) => {
                projected_array(PrimitiveValueArrayRef::UInt(value.content().as_slice()))
            }
            Self::IntArray(value) => {
                projected_array(PrimitiveValueArrayRef::Int(value.content().as_slice()))
            }
            Self::ByteArray(value) => {
                projected_array(PrimitiveValueArrayRef::Byte(value.content().as_slice()))
            }
            Self::FloatArray(value) => {
                projected_array(PrimitiveValueArrayRef::Float(value.content().as_slice()))
            }
            Self::BooleanArray(value) => {
                projected_array(PrimitiveValueArrayRef::Boolean(value.content().as_slice()))
            }
            Self::BinaryArray(value) => {
                projected_array(PrimitiveValueArrayRef::Binary(value.content().as_slice()))
            }
            Self::DateArray(value) => {
                projected_array(PrimitiveValueArrayRef::Date(value.content().as_slice()))
            }
            Self::TimestampArray(value) => projected_array(PrimitiveValueArrayRef::Timestamp(
                value.content().as_slice(),
            )),
            Self::NullableString(value) => projected_nullable_primitive(
                value
                    .content()
                    .as_ref()
                    .map(|value| PrimitiveValueRef::String(value.as_str())),
            ),
            Self::NullableUInt(value) => projected_nullable_primitive(
                value
                    .content()
                    .as_ref()
                    .map(|value| PrimitiveValueRef::UInt(*value)),
            ),
            Self::NullableInt(value) => projected_nullable_primitive(
                value
                    .content()
                    .as_ref()
                    .map(|value| PrimitiveValueRef::Int(*value)),
            ),
            Self::NullableByte(value) => projected_nullable_primitive(
                value
                    .content()
                    .as_ref()
                    .map(|value| PrimitiveValueRef::Byte(*value)),
            ),
            Self::NullableFloat(value) => projected_nullable_primitive(
                value
                    .content()
                    .as_ref()
                    .map(|value| PrimitiveValueRef::Float(*value)),
            ),
            Self::NullableBoolean(value) => projected_nullable_primitive(
                value
                    .content()
                    .as_ref()
                    .map(|value| PrimitiveValueRef::Boolean(*value)),
            ),
            Self::NullableBinary(value) => projected_nullable_primitive(
                value
                    .content()
                    .as_ref()
                    .map(|value| PrimitiveValueRef::Binary(value.as_slice())),
            ),
            Self::NullableDate(value) => projected_nullable_primitive(
                value
                    .content()
                    .as_ref()
                    .map(|value| PrimitiveValueRef::Date(*value)),
            ),
            Self::NullableTimestamp(value) => projected_nullable_primitive(
                value
                    .content()
                    .as_ref()
                    .map(|value| PrimitiveValueRef::Timestamp(*value)),
            ),
            Self::NullableStringArray(value) => projected_nullable_array(
                value
                    .content()
                    .as_ref()
                    .map(|value| PrimitiveValueArrayRef::String(value.as_slice())),
            ),
            Self::NullableUIntArray(value) => projected_nullable_array(
                value
                    .content()
                    .as_ref()
                    .map(|value| PrimitiveValueArrayRef::UInt(value.as_slice())),
            ),
            Self::NullableIntArray(value) => projected_nullable_array(
                value
                    .content()
                    .as_ref()
                    .map(|value| PrimitiveValueArrayRef::Int(value.as_slice())),
            ),
            Self::NullableByteArray(value) => projected_nullable_array(
                value
                    .content()
                    .as_ref()
                    .map(|value| PrimitiveValueArrayRef::Byte(value.as_slice())),
            ),
            Self::NullableFloatArray(value) => projected_nullable_array(
                value
                    .content()
                    .as_ref()
                    .map(|value| PrimitiveValueArrayRef::Float(value.as_slice())),
            ),
            Self::NullableBooleanArray(value) => projected_nullable_array(
                value
                    .content()
                    .as_ref()
                    .map(|value| PrimitiveValueArrayRef::Boolean(value.as_slice())),
            ),
            Self::NullableBinaryArray(value) => projected_nullable_array(
                value
                    .content()
                    .as_ref()
                    .map(|value| PrimitiveValueArrayRef::Binary(value.as_slice())),
            ),
            Self::NullableDateArray(value) => projected_nullable_array(
                value
                    .content()
                    .as_ref()
                    .map(|value| PrimitiveValueArrayRef::Date(value.as_slice())),
            ),
            Self::NullableTimestampArray(value) => projected_nullable_array(
                value
                    .content()
                    .as_ref()
                    .map(|value| PrimitiveValueArrayRef::Timestamp(value.as_slice())),
            ),
        }
    }

    #[must_use]
    #[allow(
        clippy::too_many_lines,
        clippy::match_same_arms,
        reason = "The method is an explicit matrix between runtime value variants and schema-level nullable basic types."
    )]
    pub fn matches_type(&self, expected: &NullableBasicDataType) -> bool {
        match (self, expected) {
            (
                Self::NullableString(_),
                NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::String)),
            ) => true,
            (
                Self::NullableUInt(_),
                NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::UInt)),
            ) => true,
            (
                Self::NullableInt(_),
                NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Int)),
            ) => true,
            (
                Self::NullableByte(_),
                NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Byte)),
            ) => true,
            (
                Self::NullableFloat(_),
                NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Float)),
            ) => true,
            (
                Self::NullableBoolean(_),
                NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Boolean)),
            ) => true,
            (
                Self::NullableBinary(_),
                NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Binary)),
            ) => true,
            (
                Self::NullableDate(_),
                NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Date)),
            ) => true,
            (
                Self::NullableTimestamp(_),
                NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Timestamp)),
            ) => true,
            (
                Self::NullableStringArray(_),
                NullableBasicDataType::Nullable(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::String,
            (
                Self::NullableUIntArray(_),
                NullableBasicDataType::Nullable(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::UInt,
            (
                Self::NullableIntArray(_),
                NullableBasicDataType::Nullable(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::Int,
            (
                Self::NullableByteArray(_),
                NullableBasicDataType::Nullable(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::Byte,
            (
                Self::NullableFloatArray(_),
                NullableBasicDataType::Nullable(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::Float,
            (
                Self::NullableBooleanArray(_),
                NullableBasicDataType::Nullable(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::Boolean,
            (
                Self::NullableBinaryArray(_),
                NullableBasicDataType::Nullable(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::Binary,
            (
                Self::NullableDateArray(_),
                NullableBasicDataType::Nullable(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::Date,
            (
                Self::NullableTimestampArray(_),
                NullableBasicDataType::Nullable(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::Timestamp,
            (Self::NullableString(_), _) => false,
            (Self::NullableUInt(_), _) => false,
            (Self::NullableInt(_), _) => false,
            (Self::NullableByte(_), _) => false,
            (Self::NullableFloat(_), _) => false,
            (Self::NullableBoolean(_), _) => false,
            (Self::NullableBinary(_), _) => false,
            (Self::NullableDate(_), _) => false,
            (Self::NullableTimestamp(_), _) => false,
            (Self::NullableStringArray(_), _) => false,
            (Self::NullableUIntArray(_), _) => false,
            (Self::NullableIntArray(_), _) => false,
            (Self::NullableByteArray(_), _) => false,
            (Self::NullableFloatArray(_), _) => false,
            (Self::NullableBooleanArray(_), _) => false,
            (Self::NullableBinaryArray(_), _) => false,
            (Self::NullableDateArray(_), _) => false,
            (Self::NullableTimestampArray(_), _) => false,
            (_, NullableBasicDataType::Nullable(_)) => false,
            (
                Self::String(_),
                NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::String)),
            ) => true,
            (
                Self::UInt(_),
                NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::UInt)),
            ) => true,
            (
                Self::Int(_),
                NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Int)),
            ) => true,
            (
                Self::Byte(_),
                NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Byte)),
            ) => true,
            (
                Self::Float(_),
                NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Float)),
            ) => true,
            (
                Self::Boolean(_),
                NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Boolean)),
            ) => true,
            (
                Self::Binary(_),
                NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Binary)),
            ) => true,
            (
                Self::Date(_),
                NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Date)),
            ) => true,
            (
                Self::Timestamp(_),
                NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Timestamp)),
            ) => true,
            (
                Self::StringArray(_),
                NullableBasicDataType::NonNull(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::String,
            (
                Self::UIntArray(_),
                NullableBasicDataType::NonNull(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::UInt,
            (
                Self::IntArray(_),
                NullableBasicDataType::NonNull(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::Int,
            (
                Self::ByteArray(_),
                NullableBasicDataType::NonNull(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::Byte,
            (
                Self::FloatArray(_),
                NullableBasicDataType::NonNull(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::Float,
            (
                Self::BooleanArray(_),
                NullableBasicDataType::NonNull(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::Boolean,
            (
                Self::BinaryArray(_),
                NullableBasicDataType::NonNull(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::Binary,
            (
                Self::DateArray(_),
                NullableBasicDataType::NonNull(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::Date,
            (
                Self::TimestampArray(_),
                NullableBasicDataType::NonNull(BasicDataType::Array(array_type)),
            ) => array_type.element_type == PrimitiveType::Timestamp,
            _ => false,
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "Snapshot encoding writes every primitive array shape explicitly to preserve wire compatibility."
    )]
    fn encode_snapshot<S, E>(&self, sink: &mut S) -> Result<(), E>
    where
        OperationId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
        S: for<'value> SnapshotSink<
                IdWithIndex<OperationId>,
                NullableBasicValueRef<'value>,
                Error = E,
            >,
    {
        match self {
            Self::String(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &String| {
                        NullableBasicValueRef::Value(BasicValueRef::Primitive(
                            PrimitiveValueRef::String(value.as_str()),
                        ))
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::UInt(value) => {
                let mut adapter = LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &u64| {
                    NullableBasicValueRef::Value(BasicValueRef::Primitive(PrimitiveValueRef::UInt(
                        *value,
                    )))
                });
                value.encode_snapshot(&mut adapter)
            }
            Self::Int(value) => {
                let mut adapter = LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &i64| {
                    NullableBasicValueRef::Value(BasicValueRef::Primitive(PrimitiveValueRef::Int(
                        *value,
                    )))
                });
                value.encode_snapshot(&mut adapter)
            }
            Self::Byte(value) => {
                let mut adapter = LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &u8| {
                    NullableBasicValueRef::Value(BasicValueRef::Primitive(PrimitiveValueRef::Byte(
                        *value,
                    )))
                });
                value.encode_snapshot(&mut adapter)
            }
            Self::Float(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &OrderedFloat<f64>| {
                        NullableBasicValueRef::Value(BasicValueRef::Primitive(
                            PrimitiveValueRef::Float(*value),
                        ))
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::Boolean(value) => {
                let mut adapter = LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &bool| {
                    NullableBasicValueRef::Value(BasicValueRef::Primitive(
                        PrimitiveValueRef::Boolean(*value),
                    ))
                });
                value.encode_snapshot(&mut adapter)
            }
            Self::Binary(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Vec<u8>| {
                        NullableBasicValueRef::Value(BasicValueRef::Primitive(
                            PrimitiveValueRef::Binary(value.as_slice()),
                        ))
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::Date(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &NaiveDate| {
                        NullableBasicValueRef::Value(BasicValueRef::Primitive(
                            PrimitiveValueRef::Date(*value),
                        ))
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::Timestamp(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &UnixTimestamp| {
                        NullableBasicValueRef::Value(BasicValueRef::Primitive(
                            PrimitiveValueRef::Timestamp(*value),
                        ))
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::StringArray(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Vec<String>| {
                        NullableBasicValueRef::Value(BasicValueRef::Array(
                            PrimitiveValueArrayRef::String(value.as_slice()),
                        ))
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::UIntArray(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Vec<u64>| {
                        NullableBasicValueRef::Value(BasicValueRef::Array(
                            PrimitiveValueArrayRef::UInt(value.as_slice()),
                        ))
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::IntArray(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Vec<i64>| {
                        NullableBasicValueRef::Value(BasicValueRef::Array(
                            PrimitiveValueArrayRef::Int(value.as_slice()),
                        ))
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::ByteArray(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Vec<u8>| {
                        NullableBasicValueRef::Value(BasicValueRef::Array(
                            PrimitiveValueArrayRef::Byte(value.as_slice()),
                        ))
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::FloatArray(value) => {
                let mut adapter = LatestValueWinsSnapshotSinkAdapter::new(
                    sink,
                    |value: &Vec<OrderedFloat<f64>>| {
                        NullableBasicValueRef::Value(BasicValueRef::Array(
                            PrimitiveValueArrayRef::Float(value.as_slice()),
                        ))
                    },
                );
                value.encode_snapshot(&mut adapter)
            }
            Self::BooleanArray(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Vec<bool>| {
                        NullableBasicValueRef::Value(BasicValueRef::Array(
                            PrimitiveValueArrayRef::Boolean(value.as_slice()),
                        ))
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::BinaryArray(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Vec<Vec<u8>>| {
                        NullableBasicValueRef::Value(BasicValueRef::Array(
                            PrimitiveValueArrayRef::Binary(value.as_slice()),
                        ))
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::DateArray(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Vec<NaiveDate>| {
                        NullableBasicValueRef::Value(BasicValueRef::Array(
                            PrimitiveValueArrayRef::Date(value.as_slice()),
                        ))
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::TimestampArray(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Vec<UnixTimestamp>| {
                        NullableBasicValueRef::Value(BasicValueRef::Array(
                            PrimitiveValueArrayRef::Timestamp(value.as_slice()),
                        ))
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableString(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Option<String>| {
                        match value {
                            Some(value) => NullableBasicValueRef::Value(BasicValueRef::Primitive(
                                PrimitiveValueRef::String(value.as_str()),
                            )),
                            None => NullableBasicValueRef::Null,
                        }
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableUInt(value) => {
                let mut adapter = LatestValueWinsSnapshotSinkAdapter::new(
                    sink,
                    |value: &Option<u64>| match value {
                        Some(value) => NullableBasicValueRef::Value(BasicValueRef::Primitive(
                            PrimitiveValueRef::UInt(*value),
                        )),
                        None => NullableBasicValueRef::Null,
                    },
                );
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableInt(value) => {
                let mut adapter = LatestValueWinsSnapshotSinkAdapter::new(
                    sink,
                    |value: &Option<i64>| match value {
                        Some(value) => NullableBasicValueRef::Value(BasicValueRef::Primitive(
                            PrimitiveValueRef::Int(*value),
                        )),
                        None => NullableBasicValueRef::Null,
                    },
                );
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableByte(value) => {
                let mut adapter = LatestValueWinsSnapshotSinkAdapter::new(
                    sink,
                    |value: &Option<u8>| match value {
                        Some(value) => NullableBasicValueRef::Value(BasicValueRef::Primitive(
                            PrimitiveValueRef::Byte(*value),
                        )),
                        None => NullableBasicValueRef::Null,
                    },
                );
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableFloat(value) => {
                let mut adapter = LatestValueWinsSnapshotSinkAdapter::new(
                    sink,
                    |value: &Option<OrderedFloat<f64>>| match value {
                        Some(value) => NullableBasicValueRef::Value(BasicValueRef::Primitive(
                            PrimitiveValueRef::Float(*value),
                        )),
                        None => NullableBasicValueRef::Null,
                    },
                );
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableBoolean(value) => {
                let mut adapter = LatestValueWinsSnapshotSinkAdapter::new(
                    sink,
                    |value: &Option<bool>| match value {
                        Some(value) => NullableBasicValueRef::Value(BasicValueRef::Primitive(
                            PrimitiveValueRef::Boolean(*value),
                        )),
                        None => NullableBasicValueRef::Null,
                    },
                );
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableBinary(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Option<Vec<u8>>| {
                        match value {
                            Some(value) => NullableBasicValueRef::Value(BasicValueRef::Primitive(
                                PrimitiveValueRef::Binary(value.as_slice()),
                            )),
                            None => NullableBasicValueRef::Null,
                        }
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableDate(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Option<NaiveDate>| {
                        match value {
                            Some(value) => NullableBasicValueRef::Value(BasicValueRef::Primitive(
                                PrimitiveValueRef::Date(*value),
                            )),
                            None => NullableBasicValueRef::Null,
                        }
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableTimestamp(value) => {
                let mut adapter = LatestValueWinsSnapshotSinkAdapter::new(
                    sink,
                    |value: &Option<UnixTimestamp>| match value {
                        Some(value) => NullableBasicValueRef::Value(BasicValueRef::Primitive(
                            PrimitiveValueRef::Timestamp(*value),
                        )),
                        None => NullableBasicValueRef::Null,
                    },
                );
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableStringArray(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Option<Vec<String>>| {
                        match value {
                            Some(value) => NullableBasicValueRef::Value(BasicValueRef::Array(
                                PrimitiveValueArrayRef::String(value.as_slice()),
                            )),
                            None => NullableBasicValueRef::Null,
                        }
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableUIntArray(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Option<Vec<u64>>| {
                        match value {
                            Some(value) => NullableBasicValueRef::Value(BasicValueRef::Array(
                                PrimitiveValueArrayRef::UInt(value.as_slice()),
                            )),
                            None => NullableBasicValueRef::Null,
                        }
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableIntArray(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Option<Vec<i64>>| {
                        match value {
                            Some(value) => NullableBasicValueRef::Value(BasicValueRef::Array(
                                PrimitiveValueArrayRef::Int(value.as_slice()),
                            )),
                            None => NullableBasicValueRef::Null,
                        }
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableByteArray(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Option<Vec<u8>>| {
                        match value {
                            Some(value) => NullableBasicValueRef::Value(BasicValueRef::Array(
                                PrimitiveValueArrayRef::Byte(value.as_slice()),
                            )),
                            None => NullableBasicValueRef::Null,
                        }
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableFloatArray(value) => {
                let mut adapter = LatestValueWinsSnapshotSinkAdapter::new(
                    sink,
                    |value: &Option<Vec<OrderedFloat<f64>>>| match value {
                        Some(value) => NullableBasicValueRef::Value(BasicValueRef::Array(
                            PrimitiveValueArrayRef::Float(value.as_slice()),
                        )),
                        None => NullableBasicValueRef::Null,
                    },
                );
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableBooleanArray(value) => {
                let mut adapter =
                    LatestValueWinsSnapshotSinkAdapter::new(sink, |value: &Option<Vec<bool>>| {
                        match value {
                            Some(value) => NullableBasicValueRef::Value(BasicValueRef::Array(
                                PrimitiveValueArrayRef::Boolean(value.as_slice()),
                            )),
                            None => NullableBasicValueRef::Null,
                        }
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableBinaryArray(value) => {
                let mut adapter = LatestValueWinsSnapshotSinkAdapter::new(
                    sink,
                    |value: &Option<Vec<Vec<u8>>>| match value {
                        Some(value) => NullableBasicValueRef::Value(BasicValueRef::Array(
                            PrimitiveValueArrayRef::Binary(value.as_slice()),
                        )),
                        None => NullableBasicValueRef::Null,
                    },
                );
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableDateArray(value) => {
                let mut adapter = LatestValueWinsSnapshotSinkAdapter::new(
                    sink,
                    |value: &Option<Vec<NaiveDate>>| match value {
                        Some(value) => NullableBasicValueRef::Value(BasicValueRef::Array(
                            PrimitiveValueArrayRef::Date(value.as_slice()),
                        )),
                        None => NullableBasicValueRef::Null,
                    },
                );
                value.encode_snapshot(&mut adapter)
            }
            Self::NullableTimestampArray(value) => {
                let mut adapter = LatestValueWinsSnapshotSinkAdapter::new(
                    sink,
                    |value: &Option<Vec<UnixTimestamp>>| match value {
                        Some(value) => NullableBasicValueRef::Value(BasicValueRef::Array(
                            PrimitiveValueArrayRef::Timestamp(value.as_slice()),
                        )),
                        None => NullableBasicValueRef::Null,
                    },
                );
                value.encode_snapshot(&mut adapter)
            }
        }
    }
}

/// Specialized `LinearList` state variants by concrete primitive element type.
#[derive(Clone, Debug, PartialEq)]
pub enum LinearListState<OperationId> {
    String(LinearList<OperationId, String>),
    UInt(LinearList<OperationId, u64>),
    Int(LinearList<OperationId, i64>),
    Byte(LinearList<OperationId, u8>),
    Float(LinearList<OperationId, OrderedFloat<f64>>),
    Boolean(LinearList<OperationId, bool>),
    Binary(LinearList<OperationId, Vec<u8>>),
    Date(LinearList<OperationId, NaiveDate>),
    Timestamp(LinearList<OperationId, UnixTimestamp>),
}
impl<OperationId> LinearListState<OperationId> {
    /// Project this list CRDT to its current application-visible array value.
    #[must_use]
    pub fn project_value(&self) -> PrimitiveValueArray
    where
        OperationId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
    {
        match self {
            Self::String(value) => PrimitiveValueArray::String(value.iter().cloned().collect()),
            Self::UInt(value) => PrimitiveValueArray::UInt(value.iter().copied().collect()),
            Self::Int(value) => PrimitiveValueArray::Int(value.iter().copied().collect()),
            Self::Byte(value) => PrimitiveValueArray::Byte(value.iter().copied().collect()),
            Self::Float(value) => PrimitiveValueArray::Float(value.iter().copied().collect()),
            Self::Boolean(value) => PrimitiveValueArray::Boolean(value.iter().copied().collect()),
            Self::Binary(value) => PrimitiveValueArray::Binary(value.iter().cloned().collect()),
            Self::Date(value) => PrimitiveValueArray::Date(value.iter().copied().collect()),
            Self::Timestamp(value) => {
                PrimitiveValueArray::Timestamp(value.iter().copied().collect())
            }
        }
    }

    #[must_use]
    pub fn primitive_type(&self) -> PrimitiveType {
        match self {
            Self::String(_) => PrimitiveType::String,
            Self::UInt(_) => PrimitiveType::UInt,
            Self::Int(_) => PrimitiveType::Int,
            Self::Byte(_) => PrimitiveType::Byte,
            Self::Float(_) => PrimitiveType::Float,
            Self::Boolean(_) => PrimitiveType::Boolean,
            Self::Binary(_) => PrimitiveType::Binary,
            Self::Date(_) => PrimitiveType::Date,
            Self::Timestamp(_) => PrimitiveType::Timestamp,
        }
    }

    fn encode_snapshot<S, E>(&self, sink: &mut S) -> Result<(), E>
    where
        OperationId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
        S: for<'value> SnapshotSink<
                IdWithIndex<OperationId>,
                PrimitiveValueArrayRef<'value>,
                Error = E,
            >,
    {
        match self {
            Self::String(value) => {
                let mut adapter = LinearListSnapshotSinkAdapter::new(sink, |value: &[String]| {
                    PrimitiveValueArrayRef::String(value)
                });
                value.encode_snapshot(&mut adapter)
            }
            Self::UInt(value) => {
                let mut adapter = LinearListSnapshotSinkAdapter::new(sink, |value: &[u64]| {
                    PrimitiveValueArrayRef::UInt(value)
                });
                value.encode_snapshot(&mut adapter)
            }
            Self::Int(value) => {
                let mut adapter = LinearListSnapshotSinkAdapter::new(sink, |value: &[i64]| {
                    PrimitiveValueArrayRef::Int(value)
                });
                value.encode_snapshot(&mut adapter)
            }
            Self::Byte(value) => {
                let mut adapter = LinearListSnapshotSinkAdapter::new(sink, |value: &[u8]| {
                    PrimitiveValueArrayRef::Byte(value)
                });
                value.encode_snapshot(&mut adapter)
            }
            Self::Float(value) => {
                let mut adapter =
                    LinearListSnapshotSinkAdapter::new(sink, |value: &[OrderedFloat<f64>]| {
                        PrimitiveValueArrayRef::Float(value)
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::Boolean(value) => {
                let mut adapter = LinearListSnapshotSinkAdapter::new(sink, |value: &[bool]| {
                    PrimitiveValueArrayRef::Boolean(value)
                });
                value.encode_snapshot(&mut adapter)
            }
            Self::Binary(value) => {
                let mut adapter =
                    LinearListSnapshotSinkAdapter::new(sink, |value: &[Vec<u8>]| {
                        PrimitiveValueArrayRef::Binary(value)
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::Date(value) => {
                let mut adapter =
                    LinearListSnapshotSinkAdapter::new(sink, |value: &[NaiveDate]| {
                        PrimitiveValueArrayRef::Date(value)
                    });
                value.encode_snapshot(&mut adapter)
            }
            Self::Timestamp(value) => {
                let mut adapter =
                    LinearListSnapshotSinkAdapter::new(sink, |value: &[UnixTimestamp]| {
                        PrimitiveValueArrayRef::Timestamp(value)
                    });
                value.encode_snapshot(&mut adapter)
            }
        }
    }
}

impl<OperationId> InMemoryFieldState<OperationId> {
    pub(crate) fn encode_snapshot_field<V>(
        &self,
        field_name: &str,
        writer: &mut SchemaSnapshotEncodingWriter<'_, OperationId, V>,
    ) -> Result<(), SchemaVisitError<V::Error>>
    where
        OperationId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
        V: SchemaSnapshotEncoder<OperationId>,
    {
        match self {
            Self::LatestValueWins(value) => {
                let mut sink = writer.prepare_latest_value_wins_field(field_name)?;
                value.encode_snapshot(&mut sink).context(VisitorSnafu)
            }
            Self::LinearString(value) => {
                let mut sink = writer.prepare_linear_string_field(field_name)?;
                value.encode_snapshot(&mut sink).context(VisitorSnafu)
            }
            Self::LinearList(value) => {
                let mut sink = writer.prepare_linear_list_field(field_name)?;
                value.encode_snapshot(&mut sink).context(VisitorSnafu)
            }
            Self::MonotonicCounter(value) => writer.state_field(
                field_name,
                StateSnapshotFieldValueRef::MonotonicCounter(value.as_ref()),
            ),
            Self::TotalOrderRegister(value) => writer.state_field(
                field_name,
                StateSnapshotFieldValueRef::TotalOrderRegister(value.as_ref()),
            ),
            Self::TotalOrderFiniteStateRegister(value) => writer.state_field(
                field_name,
                StateSnapshotFieldValueRef::TotalOrderFiniteStateRegister(value.as_ref()),
            ),
        }
    }

    pub(crate) fn decode_snapshot_field<D>(
        field_name: &str,
        schema_field: &super::super::super::Field,
        decoder: &mut D,
    ) -> Result<Self, InMemoryStateDataSnapshotDecodeError<D::Error>>
    where
        OperationId: Clone + fmt::Debug + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
        D: SchemaSnapshotDecoder<OperationId>,
    {
        match &schema_field.data_type {
            ReplicatedDataType::LatestValueWins { value_type } => {
                let mut source = decoder
                    .prepare_latest_value_wins_field(field_name, value_type)
                    .context(DecoderSnafu)?;
                let nodes =
                    SnapshotNodeSourceIter::<_, IdWithIndex<OperationId>, NullableBasicValue>::new(
                        &mut source,
                    )
                    .map(|result| result.context(SourceSnafu));
                let value = decode_latest_value_wins_snapshot(value_type, nodes)
                    .context(SnapshotReadSnafu)?;
                Ok(Self::LatestValueWins(value))
            }
            ReplicatedDataType::LinearString => {
                let mut source = decoder
                    .prepare_linear_string_field(field_name)
                    .context(DecoderSnafu)?;
                let nodes =
                    SnapshotNodeSourceIter::<_, IdWithIndex<OperationId>, String>::new(&mut source)
                        .map(|result| result.context(SourceSnafu));
                let value = LinearString::from_snapshot_nodes(nodes).context(SnapshotReadSnafu)?;
                Ok(Self::LinearString(value))
            }
            ReplicatedDataType::LinearList { value_type } => {
                let mut source = decoder
                    .prepare_linear_list_field(field_name, *value_type)
                    .context(DecoderSnafu)?;
                let nodes = SnapshotNodeSourceIter::<
                    _,
                    IdWithIndex<OperationId>,
                    PrimitiveValueArray,
                >::new(&mut source)
                .map(|result| result.context(SourceSnafu));
                let value =
                    decode_linear_list_snapshot(*value_type, nodes).context(SnapshotReadSnafu)?;
                Ok(Self::LinearList(value))
            }
            ReplicatedDataType::MonotonicCounter { .. }
            | ReplicatedDataType::TotalOrderRegister { .. }
            | ReplicatedDataType::TotalOrderFiniteStateRegister { .. } => {
                let state = decoder
                    .decode_state_field(field_name, &schema_field.data_type)
                    .context(DecoderSnafu)?;
                decode_state_snapshot_field(&schema_field.data_type, state)
                    .context(InvalidValueSnafu)
            }
        }
    }
}

pub(crate) fn validate_in_memory_field_value<OperationId>(
    data_type: &ReplicatedDataType,
    value: &InMemoryFieldState<OperationId>,
) -> Result<(), DataModelValueError> {
    match (data_type, value) {
        (
            ReplicatedDataType::LatestValueWins { value_type },
            InMemoryFieldState::LatestValueWins(v),
        ) => {
            if v.matches_type(value_type) {
                Ok(())
            } else {
                Err(DataModelValueError::BasicTypeMismatch)
            }
        }
        (ReplicatedDataType::LinearString, InMemoryFieldState::LinearString(_)) => Ok(()),
        (ReplicatedDataType::LinearList { value_type }, InMemoryFieldState::LinearList(v)) => {
            ensure_primitive_type(*value_type, v.primitive_type())
        }
        (
            ReplicatedDataType::MonotonicCounter { small_range },
            InMemoryFieldState::MonotonicCounter(v),
        ) => ensure_counter_type(*small_range, v.as_ref()),
        (
            ReplicatedDataType::TotalOrderRegister { value_type, .. },
            InMemoryFieldState::TotalOrderRegister(v),
        ) => ensure_primitive_type(*value_type, v.primitive_type()),
        (
            ReplicatedDataType::TotalOrderFiniteStateRegister { value_type, states },
            InMemoryFieldState::TotalOrderFiniteStateRegister(v),
        ) => ensure_finite_state_value(*value_type, states, &v.as_ref()),
        _ => Err(DataModelValueError::InvalidSnapshotValueForType),
    }
}
