use super::*;
use std::{collections::HashSet, convert::Infallible};

/// Explicit operation payload shapes for all schema data types.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OperationValue {
    LatestValueWinsUpdate(NullableBasicValue),
    LinearStringInsert(String),
    LinearStringDelete,
    LinearListInsert(PrimitiveValueArray),
    LinearListDelete,
    MonotonicCounterIncrement(CounterValue),
    TotalOrderRegisterSet(PrimitiveValue),
    TotalOrderFiniteStateRegisterSet(NullablePrimitiveValue),
}
impl OperationValue {
    pub fn as_ref(&self) -> OperationValueRef<'_> {
        match self {
            Self::LatestValueWinsUpdate(value) => {
                OperationValueRef::LatestValueWinsUpdate(value.as_ref())
            }
            Self::LinearStringInsert(value) => {
                OperationValueRef::LinearStringInsert(value.as_str())
            }
            Self::LinearStringDelete => OperationValueRef::LinearStringDelete,
            Self::LinearListInsert(values) => OperationValueRef::LinearListInsert(values.as_ref()),
            Self::LinearListDelete => OperationValueRef::LinearListDelete,
            Self::MonotonicCounterIncrement(value) => {
                OperationValueRef::MonotonicCounterIncrement(value.as_ref())
            }
            Self::TotalOrderRegisterSet(value) => {
                OperationValueRef::TotalOrderRegisterSet(value.as_ref())
            }
            Self::TotalOrderFiniteStateRegisterSet(value) => {
                OperationValueRef::TotalOrderFiniteStateRegisterSet(value.as_ref())
            }
        }
    }
}

/// Borrowed operation payload shapes for all schema data types.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OperationValueRef<'a> {
    LatestValueWinsUpdate(NullableBasicValueRef<'a>),
    LinearStringInsert(&'a str),
    LinearStringDelete,
    LinearListInsert(PrimitiveValueArrayRef<'a>),
    LinearListDelete,
    MonotonicCounterIncrement(CounterValueRef),
    TotalOrderRegisterSet(PrimitiveValueRef<'a>),
    TotalOrderFiniteStateRegisterSet(NullablePrimitiveValueRef<'a>),
}
impl OperationValueRef<'_> {
    pub fn into_owned(self) -> OperationValue {
        match self {
            Self::LatestValueWinsUpdate(value) => {
                OperationValue::LatestValueWinsUpdate(value.into_owned())
            }
            Self::LinearStringInsert(value) => OperationValue::LinearStringInsert(value.to_owned()),
            Self::LinearStringDelete => OperationValue::LinearStringDelete,
            Self::LinearListInsert(values) => OperationValue::LinearListInsert(values.into_owned()),
            Self::LinearListDelete => OperationValue::LinearListDelete,
            Self::MonotonicCounterIncrement(value) => {
                OperationValue::MonotonicCounterIncrement(value.into_owned())
            }
            Self::TotalOrderRegisterSet(value) => {
                OperationValue::TotalOrderRegisterSet(value.to_owned())
            }
            Self::TotalOrderFiniteStateRegisterSet(value) => {
                OperationValue::TotalOrderFiniteStateRegisterSet(value.to_owned())
            }
        }
    }
}

/// Decoded operation payload variant for `LinearString`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LinearStringOperationValue {
    Insert(String),
    Delete,
}
impl LinearStringOperationValue {
    pub fn as_ref(&self) -> LinearStringOperationValueRef<'_> {
        match self {
            Self::Insert(value) => LinearStringOperationValueRef::Insert(value.as_str()),
            Self::Delete => LinearStringOperationValueRef::Delete,
        }
    }
}

/// Borrowed decoded operation payload variant for `LinearString`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LinearStringOperationValueRef<'a> {
    Insert(&'a str),
    Delete,
}
impl LinearStringOperationValueRef<'_> {
    pub fn into_owned(self) -> LinearStringOperationValue {
        match self {
            Self::Insert(value) => LinearStringOperationValue::Insert(value.to_owned()),
            Self::Delete => LinearStringOperationValue::Delete,
        }
    }
}

/// Decoded operation payload variant for `LinearList`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LinearListOperationValue {
    Insert(PrimitiveValueArray),
    Delete,
}
impl LinearListOperationValue {
    pub fn as_ref(&self) -> LinearListOperationValueRef<'_> {
        match self {
            Self::Insert(values) => LinearListOperationValueRef::Insert(values.as_ref()),
            Self::Delete => LinearListOperationValueRef::Delete,
        }
    }
}

/// Borrowed decoded operation payload variant for `LinearList`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LinearListOperationValueRef<'a> {
    Insert(PrimitiveValueArrayRef<'a>),
    Delete,
}
impl LinearListOperationValueRef<'_> {
    pub fn into_owned(self) -> LinearListOperationValue {
        match self {
            Self::Insert(values) => LinearListOperationValue::Insert(values.into_owned()),
            Self::Delete => LinearListOperationValue::Delete,
        }
    }
}
/// One operation value bound to a concrete schema field.
///
/// For full-schema operations this list can be a strict subset of schema fields:
/// unspecified fields are unchanged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperationFieldValue {
    pub field_name: String,
    pub value: OperationValue,
}
impl OperationFieldValue {
    pub fn as_ref(&self) -> OperationFieldValueRef<'_> {
        OperationFieldValueRef {
            field_name: self.field_name.as_str(),
            value: self.value.as_ref(),
        }
    }
}

/// Borrowed form of [[OperationFieldValue]].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperationFieldValueRef<'a> {
    pub field_name: &'a str,
    pub value: OperationValueRef<'a>,
}
impl OperationFieldValueRef<'_> {
    pub fn into_owned(self) -> OperationFieldValue {
        OperationFieldValue {
            field_name: self.field_name.to_owned(),
            value: self.value.into_owned(),
        }
    }
}

/// A partial operation across fields in a schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaOperation {
    /// Fields to change. Unspecified fields remain unchanged.
    pub fields: Vec<OperationFieldValue>,
}
impl SchemaOperation {
    pub fn validate_against_schema(&self, schema: &Schema) -> Result<(), SchemaValueError> {
        validate_schema_operation_fields(
            schema,
            self.fields.iter().map(OperationFieldValue::as_ref),
        )
    }
}
/// Visitor for partial-schema operations.
pub trait SchemaOperationVisitor {
    type Error;

    fn begin(&mut self, changed_field_count: usize) -> Result<(), Self::Error>;

    fn field(&mut self, field_name: &str, value: OperationValueRef<'_>) -> Result<(), Self::Error>;

    fn end(&mut self) -> Result<(), Self::Error>;
}
/// Validate and visit a partial operation across schema fields.
pub fn visit_schema_operation<'a, V, I>(
    visitor: &mut V,
    schema: &Schema,
    fields: I,
) -> Result<(), SchemaVisitError<V::Error>>
where
    V: SchemaOperationVisitor,
    I: IntoIterator<Item = OperationFieldValueRef<'a>>,
{
    let fields: Vec<OperationFieldValueRef<'a>> = fields.into_iter().collect();
    validate_schema_operation_fields(schema, fields.iter().cloned())
        .map_err(SchemaVisitError::InvalidSchemaValue)?;

    visitor
        .begin(fields.len())
        .map_err(SchemaVisitError::Visitor)?;
    for field in fields {
        visitor
            .field(field.field_name, field.value)
            .map_err(SchemaVisitError::Visitor)?;
    }
    visitor.end().map_err(SchemaVisitError::Visitor)
}
/// Validate partial operation field payloads against a schema.
///
/// Any subset is allowed, but field names must exist and each field may appear at most once.
pub fn validate_schema_operation_fields<'a, I>(
    schema: &Schema,
    fields: I,
) -> Result<(), SchemaValueError>
where
    I: IntoIterator<Item = OperationFieldValueRef<'a>>,
{
    let mut seen_fields = HashSet::<String>::new();

    for field in fields {
        if !seen_fields.insert(field.field_name.to_owned()) {
            return Err(SchemaValueError::DuplicateField {
                field_name: field.field_name.to_owned(),
            });
        }
        let Some(schema_field) = schema.columns.get(field.field_name) else {
            return Err(SchemaValueError::UnknownField {
                field_name: field.field_name.to_owned(),
            });
        };
        validate_operation_value_for_type(&schema_field.data_type, field.value).map_err(
            |source| SchemaValueError::InvalidOperationFieldValue {
                field_name: field.field_name.to_owned(),
                source,
            },
        )?;
    }

    Ok(())
}
/// Visitor used by serializers to encode one operation value at a time.
pub trait OperationValueVisitor {
    type Error;

    fn visit_latest_value_wins_update(
        &mut self,
        value_type: &NullableBasicDataType,
        value: NullableBasicValueRef<'_>,
    ) -> Result<(), Self::Error>;

    fn visit_linear_string_insert(&mut self, value: &str) -> Result<(), Self::Error>;

    fn visit_linear_string_delete(&mut self) -> Result<(), Self::Error>;

    fn visit_linear_list_insert(
        &mut self,
        value_type: PrimitiveType,
        values: PrimitiveValueArrayRef<'_>,
    ) -> Result<(), Self::Error>;

    fn visit_linear_list_delete(&mut self) -> Result<(), Self::Error>;

    fn visit_monotonic_counter_increment(
        &mut self,
        small_range: bool,
        value: CounterValueRef,
    ) -> Result<(), Self::Error>;

    fn visit_total_order_register_set(
        &mut self,
        value_type: PrimitiveType,
        value: PrimitiveValueRef<'_>,
    ) -> Result<(), Self::Error>;

    fn visit_total_order_finite_state_register_set(
        &mut self,
        value_type: NullablePrimitiveType,
        states: &NullablePrimitiveValueArray,
        value: NullablePrimitiveValueRef<'_>,
    ) -> Result<(), Self::Error>;
}

/// Decoder used to lazily decode one operation value at a time.
pub trait OperationValueDecoder {
    type Error;

    fn decode_latest_value_wins_update(
        &mut self,
        value_type: &NullableBasicDataType,
    ) -> Result<NullableBasicValue, Self::Error>;

    fn decode_linear_string_operation(&mut self)
    -> Result<LinearStringOperationValue, Self::Error>;

    fn decode_linear_list_operation(
        &mut self,
        value_type: PrimitiveType,
    ) -> Result<LinearListOperationValue, Self::Error>;

    fn decode_monotonic_counter_increment(
        &mut self,
        small_range: bool,
    ) -> Result<CounterValue, Self::Error>;

    fn decode_total_order_register_set(
        &mut self,
        value_type: PrimitiveType,
    ) -> Result<PrimitiveValue, Self::Error>;

    fn decode_total_order_finite_state_register_set(
        &mut self,
        value_type: NullablePrimitiveType,
        states: &NullablePrimitiveValueArray,
    ) -> Result<NullablePrimitiveValue, Self::Error>;
}
/// Dispatch + validate one operation value against its schema data type before visiting.
pub fn visit_operation_value<V>(
    visitor: &mut V,
    data_type: &ReplicatedDataType,
    value: OperationValueRef<'_>,
) -> Result<(), VisitError<V::Error>>
where
    V: OperationValueVisitor,
{
    match (data_type, value) {
        (
            ReplicatedDataType::LatestValueWins { value_type },
            OperationValueRef::LatestValueWinsUpdate(v),
        ) => {
            ensure_nullable_basic_type(value_type, &v).map_err(VisitError::InvalidValue)?;
            visitor
                .visit_latest_value_wins_update(value_type, v)
                .map_err(VisitError::Visitor)
        }
        (ReplicatedDataType::LinearString, OperationValueRef::LinearStringInsert(v)) => visitor
            .visit_linear_string_insert(v)
            .map_err(VisitError::Visitor),
        (ReplicatedDataType::LinearString, OperationValueRef::LinearStringDelete) => visitor
            .visit_linear_string_delete()
            .map_err(VisitError::Visitor),
        (ReplicatedDataType::LinearList { value_type }, OperationValueRef::LinearListInsert(v)) => {
            ensure_primitive_array_type(*value_type, v.primitive_type())
                .map_err(VisitError::InvalidValue)?;
            visitor
                .visit_linear_list_insert(*value_type, v)
                .map_err(VisitError::Visitor)
        }
        (ReplicatedDataType::LinearList { .. }, OperationValueRef::LinearListDelete) => visitor
            .visit_linear_list_delete()
            .map_err(VisitError::Visitor),
        (
            ReplicatedDataType::MonotonicCounter { small_range },
            OperationValueRef::MonotonicCounterIncrement(v),
        ) => {
            ensure_counter_type(*small_range, v).map_err(VisitError::InvalidValue)?;
            visitor
                .visit_monotonic_counter_increment(*small_range, v)
                .map_err(VisitError::Visitor)
        }
        (
            ReplicatedDataType::TotalOrderRegister { value_type, .. },
            OperationValueRef::TotalOrderRegisterSet(v),
        ) => {
            ensure_primitive_type(*value_type, v.value_type()).map_err(VisitError::InvalidValue)?;
            visitor
                .visit_total_order_register_set(*value_type, v)
                .map_err(VisitError::Visitor)
        }
        (
            ReplicatedDataType::TotalOrderFiniteStateRegister { value_type, states },
            OperationValueRef::TotalOrderFiniteStateRegisterSet(v),
        ) => {
            ensure_finite_state_value(*value_type, states, &v).map_err(VisitError::InvalidValue)?;
            visitor
                .visit_total_order_finite_state_register_set(*value_type, states, v)
                .map_err(VisitError::Visitor)
        }
        _ => Err(VisitError::InvalidValue(
            DataModelValueError::InvalidOperationValueForType,
        )),
    }
}

/// Decode + validate one operation value lazily for the provided schema data type.
pub fn decode_operation_value<D>(
    decoder: &mut D,
    data_type: &ReplicatedDataType,
) -> Result<OperationValue, DecodeError<D::Error>>
where
    D: OperationValueDecoder,
{
    match data_type {
        ReplicatedDataType::LatestValueWins { value_type } => {
            let value = decoder
                .decode_latest_value_wins_update(value_type)
                .map_err(DecodeError::Decoder)?;
            ensure_nullable_basic_type(value_type, &value.as_ref())
                .map_err(DecodeError::InvalidValue)?;
            Ok(OperationValue::LatestValueWinsUpdate(value))
        }
        ReplicatedDataType::LinearString => match decoder
            .decode_linear_string_operation()
            .map_err(DecodeError::Decoder)?
        {
            LinearStringOperationValue::Insert(value) => {
                Ok(OperationValue::LinearStringInsert(value))
            }
            LinearStringOperationValue::Delete => Ok(OperationValue::LinearStringDelete),
        },
        ReplicatedDataType::LinearList { value_type } => {
            match decoder
                .decode_linear_list_operation(*value_type)
                .map_err(DecodeError::Decoder)?
            {
                LinearListOperationValue::Insert(values) => {
                    ensure_primitive_array_type(*value_type, values.primitive_type())
                        .map_err(DecodeError::InvalidValue)?;
                    Ok(OperationValue::LinearListInsert(values))
                }
                LinearListOperationValue::Delete => Ok(OperationValue::LinearListDelete),
            }
        }
        ReplicatedDataType::MonotonicCounter { small_range } => {
            let value = decoder
                .decode_monotonic_counter_increment(*small_range)
                .map_err(DecodeError::Decoder)?;
            ensure_counter_type(*small_range, value.as_ref()).map_err(DecodeError::InvalidValue)?;
            Ok(OperationValue::MonotonicCounterIncrement(value))
        }
        ReplicatedDataType::TotalOrderRegister { value_type, .. } => {
            let value = decoder
                .decode_total_order_register_set(*value_type)
                .map_err(DecodeError::Decoder)?;
            ensure_primitive_type(*value_type, value.primitive_type())
                .map_err(DecodeError::InvalidValue)?;
            Ok(OperationValue::TotalOrderRegisterSet(value))
        }
        ReplicatedDataType::TotalOrderFiniteStateRegister { value_type, states } => {
            let value = decoder
                .decode_total_order_finite_state_register_set(*value_type, states)
                .map_err(DecodeError::Decoder)?;
            ensure_finite_state_value(*value_type, states, &value.as_ref())
                .map_err(DecodeError::InvalidValue)?;
            Ok(OperationValue::TotalOrderFiniteStateRegisterSet(value))
        }
    }
}
fn validate_operation_value_for_type(
    data_type: &ReplicatedDataType,
    value: OperationValueRef<'_>,
) -> Result<(), DataModelValueError> {
    let mut visitor = NoopOperationValueVisitor;
    match visit_operation_value(&mut visitor, data_type, value) {
        Ok(()) => Ok(()),
        Err(VisitError::InvalidValue(error)) => Err(error),
        Err(VisitError::Visitor(never)) => match never {},
    }
}
struct NoopOperationValueVisitor;
impl OperationValueVisitor for NoopOperationValueVisitor {
    type Error = Infallible;

    fn visit_latest_value_wins_update(
        &mut self,
        _value_type: &NullableBasicDataType,
        _value: NullableBasicValueRef<'_>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn visit_linear_string_insert(&mut self, _value: &str) -> Result<(), Self::Error> {
        Ok(())
    }

    fn visit_linear_string_delete(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn visit_linear_list_insert(
        &mut self,
        _value_type: PrimitiveType,
        _values: PrimitiveValueArrayRef<'_>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn visit_linear_list_delete(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn visit_monotonic_counter_increment(
        &mut self,
        _small_range: bool,
        _value: CounterValueRef,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn visit_total_order_register_set(
        &mut self,
        _value_type: PrimitiveType,
        _value: PrimitiveValueRef<'_>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn visit_total_order_finite_state_register_set(
        &mut self,
        _value_type: NullablePrimitiveType,
        _states: &NullablePrimitiveValueArray,
        _value: NullablePrimitiveValueRef<'_>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}
