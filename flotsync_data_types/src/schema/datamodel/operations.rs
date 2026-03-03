use super::{
    validation::{
        ensure_counter_type,
        ensure_finite_state_value,
        ensure_nullable_basic_type,
        ensure_primitive_array_type,
        ensure_primitive_type,
    },
    *,
};
use crate::{DataOperation, IdWithIndex, any_data::UpdateOperation};
use std::{borrow::Cow, collections::HashSet};

/// Explicit operation payload shapes for all schema data types.
#[derive(Clone, Debug, PartialEq)]
pub enum OperationValue<Id> {
    LatestValueWins(UpdateOperation<Id, NullableBasicValue>),
    /// One logical linear-string change may expand to multiple chunk-local operations.
    LinearString(Vec<DataOperation<IdWithIndex<Id>, String>>),
    /// One logical linear-list change may expand to multiple chunk-local operations.
    LinearList(Vec<DataOperation<IdWithIndex<Id>, PrimitiveValueArray>>),
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
    pub change_id: Id,
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
        (ReplicatedDataType::LinearString, OperationValue::LinearString(values)) => {
            validate_linear_operation_batch(values, |_| Ok(()))
        }
        (ReplicatedDataType::LinearList { value_type }, OperationValue::LinearList(values)) => {
            validate_linear_operation_batch(values, |value| {
                ensure_primitive_array_type(*value_type, value.primitive_type())
            })
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

fn validate_linear_operation_batch<Id, Value>(
    values: &[DataOperation<Id, Value>],
    validate_insert: impl Fn(&Value) -> Result<(), DataModelValueError>,
) -> Result<(), DataModelValueError> {
    ensure!(!values.is_empty(), EmptyLinearOperationBatchSnafu);

    for value in values {
        match value {
            DataOperation::Insert { value, .. } => validate_insert(value)?,
            DataOperation::Delete { .. } => {}
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Field, PrimitiveType, ReplicatedDataType, Schema};
    use std::collections::HashMap;

    fn schema_with_field(field_name: &str, data_type: ReplicatedDataType) -> Schema {
        Schema {
            columns: HashMap::from([(
                field_name.to_owned(),
                Field {
                    name: field_name.to_owned(),
                    data_type,
                    metadata: HashMap::new(),
                },
            )]),
            metadata: HashMap::new(),
        }
    }

    fn indexed(id: u32, index: u32) -> IdWithIndex<u32> {
        IdWithIndex { id, index }
    }

    #[test]
    fn linear_string_batches_must_not_be_empty() {
        let schema = schema_with_field("title", ReplicatedDataType::LinearString);
        let operation = SchemaOperation {
            change_id: 0,
            fields: vec![OperationFieldValue {
                field_name: Cow::Borrowed("title"),
                value: OperationValue::<u32>::LinearString(Vec::new()),
            }],
        };

        let err = operation.validate_against_schema(&schema).unwrap_err();
        assert!(matches!(
            err,
            SchemaValueError::InvalidOperationFieldValue {
                field_name,
                source: DataModelValueError::EmptyLinearOperationBatch,
            } if field_name == "title"
        ));
    }

    #[test]
    fn linear_list_batches_validate_every_action() {
        let schema = schema_with_field(
            "numbers",
            ReplicatedDataType::LinearList {
                value_type: PrimitiveType::UInt,
            },
        );
        let operation = SchemaOperation {
            change_id: 0,
            fields: vec![OperationFieldValue {
                field_name: Cow::Borrowed("numbers"),
                value: OperationValue::LinearList(vec![
                    DataOperation::Insert {
                        id: indexed(10, 0),
                        pred: indexed(0, 0),
                        succ: indexed(1, 0),
                        value: PrimitiveValueArray::UInt(vec![1, 2]),
                    },
                    DataOperation::Delete {
                        start: indexed(10, 0),
                        end: Some(indexed(10, 1)),
                    },
                ]),
            }],
        };

        operation.validate_against_schema(&schema).unwrap();
    }
}
