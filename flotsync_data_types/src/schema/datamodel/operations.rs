use super::*;
use crate::{DataOperation, IdWithIndex, any_data::UpdateOperation};
use std::{borrow::Cow, collections::HashSet};

/// Explicit operation payload shapes for all schema data types.
#[derive(Clone, Debug, PartialEq)]
pub enum OperationValue<Id> {
    LatestValueWins(UpdateOperation<Id, NullableBasicValue>),
    LinearString(DataOperation<IdWithIndex<Id>, String>),
    LinearList(DataOperation<IdWithIndex<Id>, PrimitiveValueArray>),
    MonotonicCounterIncrement(CounterValue),
    TotalOrderRegisterSet(PrimitiveValue),
    TotalOrderFiniteStateRegisterSet(NullablePrimitiveValue),
}

/// One operation value bound to a concrete schema field.
#[derive(Clone, Debug, PartialEq)]
pub struct OperationFieldValue<'a, Id> {
    pub field_name: Cow<'a, str>,
    pub value: OperationValue<Id>,
}

/// A partial operation across fields in a schema.
#[derive(Clone, Debug, PartialEq)]
pub struct SchemaOperation<'a, Id> {
    /// Fields to change. Unspecified fields remain unchanged.
    pub fields: Vec<OperationFieldValue<'a, Id>>,
}
impl<Id> SchemaOperation<'_, Id> {
    pub fn validate_against_schema(&self, schema: &Schema) -> Result<(), SchemaValueError> {
        validate_schema_operation_fields(schema, &self.fields)
    }
}

/// Validate partial operation field payloads against a schema.
///
/// Any subset is allowed, but field names must exist and each field may appear at most once.
pub fn validate_schema_operation_fields<Id>(
    schema: &Schema,
    fields: &[OperationFieldValue<'_, Id>],
) -> Result<(), SchemaValueError> {
    let mut seen_fields = HashSet::<&str>::new();

    for field in fields {
        let field_name = field.field_name.as_ref();
        ensure!(
            seen_fields.insert(field_name),
            DuplicateFieldSnafu {
                field_name: field_name.to_owned(),
            }
        );
        let Some(schema_field) = schema.columns.get(field_name) else {
            return UnknownFieldSnafu {
                field_name: field_name.to_owned(),
            }
            .fail();
        };
        validate_operation_value_for_type(&schema_field.data_type, &field.value).with_context(
            |_| InvalidOperationFieldValueSnafu {
                field_name: field_name.to_owned(),
            },
        )?;
    }

    Ok(())
}

fn validate_operation_value_for_type<Id>(
    data_type: &ReplicatedDataType,
    value: &OperationValue<Id>,
) -> Result<(), DataModelValueError> {
    match (data_type, value) {
        (
            ReplicatedDataType::LatestValueWins { value_type },
            OperationValue::LatestValueWins(value),
        ) => ensure_nullable_basic_type(value_type, &value.value.as_ref()),
        (ReplicatedDataType::LinearString, OperationValue::LinearString(_)) => Ok(()),
        (ReplicatedDataType::LinearList { value_type }, OperationValue::LinearList(value)) => {
            match value {
                DataOperation::Insert { value, .. } => {
                    ensure_primitive_array_type(*value_type, value.primitive_type())
                }
                DataOperation::Delete { .. } => Ok(()),
            }
        }
        (
            ReplicatedDataType::MonotonicCounter { small_range },
            OperationValue::MonotonicCounterIncrement(value),
        ) => ensure_counter_type(*small_range, value.as_ref()),
        (
            ReplicatedDataType::TotalOrderRegister { value_type, .. },
            OperationValue::TotalOrderRegisterSet(value),
        ) => ensure_primitive_type(*value_type, value.value_type()),
        (
            ReplicatedDataType::TotalOrderFiniteStateRegister { value_type, states },
            OperationValue::TotalOrderFiniteStateRegisterSet(value),
        ) => ensure_finite_state_value(*value_type, states, &value.as_ref()),
        _ => InvalidOperationValueForTypeSnafu.fail(),
    }
}
