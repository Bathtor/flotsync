use crate::{
    DataOperation,
    IdWithIndex,
    any_data::UpdateOperation,
    schema::{
        ArrayType,
        BasicDataType,
        Direction,
        Field,
        NullableBasicDataType,
        NullablePrimitiveType,
        PrimitiveType,
        ReplicatedDataType,
        Schema,
        datamodel::{
            CounterValue,
            NullableBasicValue,
            OperationFieldValue,
            OperationValue,
            RowOperation,
            SchemaOperation,
        },
        values::{
            NullablePrimitiveValue,
            NullablePrimitiveValueArray,
            PrimitiveValue,
            PrimitiveValueArray,
        },
    },
};
use chrono::NaiveDate;
use ordered_float::OrderedFloat;
use snafu::prelude::*;
use std::{borrow::Cow, collections::HashMap};

mod operations;
mod schema;

pub use operations::{exhaustive_schema_operation, exhaustive_schema_operations};
pub use schema::exhaustive_schema;

/// The number of fields in [`exhaustive_schema`].
pub const EXHAUSTIVE_SCHEMA_FIELD_COUNT: usize = 84;

/// The number of single-field operations returned by [`exhaustive_schema_operations`].
pub const EXHAUSTIVE_SCHEMA_OPERATION_COUNT: usize = 88;

/// The minimum number of IDs required by [`exhaustive_schema_operation`].
pub const EXHAUSTIVE_SCHEMA_OPERATION_MIN_ID_COUNT: usize = 139;

/// The minimum number of IDs required by [`exhaustive_schema_operations`].
pub const EXHAUSTIVE_SCHEMA_OPERATIONS_MIN_ID_COUNT: usize = 231;

/// Errors constructing exhaustive example operations.
#[derive(Debug, Snafu)]
pub enum ExampleBuildError {
    #[snafu(display(
        "{builder} requires at least {required_minimum} IDs, but the iterator ran out after yielding {consumed}."
    ))]
    InsufficientIds {
        builder: &'static str,
        consumed: usize,
        required_minimum: usize,
    },
}

fn insert_field(columns: &mut HashMap<String, Field>, name: String, data_type: ReplicatedDataType) {
    columns.insert(
        name.clone(),
        Field {
            name,
            data_type,
            default_value: None,
            metadata: HashMap::new(),
        },
    );
}

fn set_field_default<V>(columns: &mut HashMap<String, Field>, field_name: String, value: V)
where
    V: Into<NullableBasicValue>,
{
    let field = columns
        .remove(field_name.as_str())
        .expect("field must exist when assigning exhaustive defaults");
    let field = field
        .with_default(value)
        .expect("exhaustive defaults must be type-compatible");
    columns.insert(field_name, field);
}

fn field<Id>(field_name: String, value: OperationValue<Id>) -> OperationFieldValue<'static, Id> {
    OperationFieldValue {
        field_name: Cow::Owned(field_name),
        value,
    }
}

fn single_field_operation<RowId, ChangeId>(
    row_id: RowId,
    change_id: ChangeId,
    field_name: String,
    value: OperationValue<ChangeId>,
) -> SchemaOperation<'static, RowId, ChangeId>
where
    ChangeId: Clone,
{
    SchemaOperation {
        change_id,
        operation: RowOperation::Update {
            row_id,
            fields: vec![field(field_name, value)],
        },
    }
}

fn latest_value_wins_operation<Id, Ids>(
    ids: &mut IdSource<Ids>,
    value: NullableBasicValue,
) -> Result<OperationValue<Id>, ExampleBuildError>
where
    Ids: Iterator<Item = Id>,
{
    Ok(OperationValue::LatestValueWins(UpdateOperation {
        id: indexed_id(ids.next_id()?),
        pred: indexed_id(ids.next_id()?),
        succ: indexed_id(ids.next_id()?),
        value,
    }))
}

fn linear_string_insert<Id, Ids>(
    ids: &mut IdSource<Ids>,
    value: String,
) -> Result<OperationValue<Id>, ExampleBuildError>
where
    Ids: Iterator<Item = Id>,
{
    Ok(OperationValue::LinearString(vec![DataOperation::Insert {
        id: indexed_id(ids.next_id()?),
        pred: indexed_id(ids.next_id()?),
        succ: indexed_id(ids.next_id()?),
        value,
    }]))
}

fn linear_string_delete<Id, Ids>(
    ids: &mut IdSource<Ids>,
) -> Result<OperationValue<Id>, ExampleBuildError>
where
    Ids: Iterator<Item = Id>,
{
    Ok(OperationValue::LinearString(vec![DataOperation::Delete {
        start: indexed_id(ids.next_id()?),
        end: None,
    }]))
}

fn linear_list_insert<Id, Ids>(
    ids: &mut IdSource<Ids>,
    value: PrimitiveValueArray,
) -> Result<OperationValue<Id>, ExampleBuildError>
where
    Ids: Iterator<Item = Id>,
{
    Ok(OperationValue::LinearList(vec![DataOperation::Insert {
        id: indexed_id(ids.next_id()?),
        pred: indexed_id(ids.next_id()?),
        succ: indexed_id(ids.next_id()?),
        value,
    }]))
}

fn linear_list_delete<Id, Ids>(
    ids: &mut IdSource<Ids>,
) -> Result<OperationValue<Id>, ExampleBuildError>
where
    Ids: Iterator<Item = Id>,
{
    Ok(OperationValue::LinearList(vec![DataOperation::Delete {
        start: indexed_id(ids.next_id()?),
        end: None,
    }]))
}

fn indexed_id<Id>(id: Id) -> IdWithIndex<Id> {
    IdWithIndex { id, index: 0 }
}

fn nullable_basic_primitive(
    nullable: bool,
    primitive_type: PrimitiveType,
) -> NullableBasicDataType {
    wrap_basic(nullable, BasicDataType::Primitive(primitive_type))
}

fn nullable_basic_array(nullable: bool, primitive_type: PrimitiveType) -> NullableBasicDataType {
    wrap_basic(
        nullable,
        BasicDataType::Array(Box::new(ArrayType {
            element_type: primitive_type,
        })),
    )
}

fn wrap_basic(nullable: bool, basic_type: BasicDataType) -> NullableBasicDataType {
    if nullable {
        NullableBasicDataType::Nullable(basic_type)
    } else {
        NullableBasicDataType::NonNull(basic_type)
    }
}

fn nullable_primitive_type(nullable: bool, primitive_type: PrimitiveType) -> NullablePrimitiveType {
    if nullable {
        NullablePrimitiveType::Nullable(primitive_type)
    } else {
        NullablePrimitiveType::NonNull(primitive_type)
    }
}

fn nullable_basic_value(
    _nullable: bool,
    is_array: bool,
    primitive_type: PrimitiveType,
) -> NullableBasicValue {
    let value = if is_array {
        crate::schema::datamodel::BasicValue::Array(primitive_value_array(primitive_type))
    } else {
        crate::schema::datamodel::BasicValue::Primitive(primitive_value(primitive_type))
    };
    NullableBasicValue::Value(value)
}

fn primitive_value(primitive_type: PrimitiveType) -> PrimitiveValue {
    match primitive_type {
        PrimitiveType::String => PrimitiveValue::String("example-string".to_owned()),
        PrimitiveType::UInt => PrimitiveValue::UInt(42),
        PrimitiveType::Int => PrimitiveValue::Int(-42),
        PrimitiveType::Byte => PrimitiveValue::Byte(7),
        PrimitiveType::Float => PrimitiveValue::Float(OrderedFloat(3.5)),
        PrimitiveType::Boolean => PrimitiveValue::Boolean(true),
        PrimitiveType::Binary => PrimitiveValue::Binary(vec![0xAA, 0xBB]),
        PrimitiveType::Date => PrimitiveValue::Date(example_date(2025, 1, 15)),
        PrimitiveType::Timestamp => PrimitiveValue::Timestamp(1_736_944_200_000),
    }
}

fn primitive_value_array(primitive_type: PrimitiveType) -> PrimitiveValueArray {
    match primitive_type {
        PrimitiveType::String => {
            PrimitiveValueArray::String(vec!["alpha".to_owned(), "beta".to_owned()])
        }
        PrimitiveType::UInt => PrimitiveValueArray::UInt(vec![7, 42, 99]),
        PrimitiveType::Int => PrimitiveValueArray::Int(vec![-7, 0, 9]),
        PrimitiveType::Byte => PrimitiveValueArray::Byte(vec![1, 2, 3]),
        PrimitiveType::Float => {
            PrimitiveValueArray::Float(vec![OrderedFloat(1.25), OrderedFloat(2.5)])
        }
        PrimitiveType::Boolean => PrimitiveValueArray::Boolean(vec![true, false, true]),
        PrimitiveType::Binary => {
            PrimitiveValueArray::Binary(vec![vec![0x00], vec![0x01], vec![0x02]])
        }
        PrimitiveType::Date => PrimitiveValueArray::Date(vec![
            example_date(2025, 1, 1),
            example_date(2025, 2, 1),
            example_date(2025, 3, 1),
        ]),
        PrimitiveType::Timestamp => {
            PrimitiveValueArray::Timestamp(vec![1_700_000_000_000, 1_710_000_000_000])
        }
    }
}

fn finite_state_values(
    nullable: bool,
    primitive_type: PrimitiveType,
) -> NullablePrimitiveValueArray {
    let values = match primitive_type {
        PrimitiveType::String => PrimitiveValueArray::String(vec![
            "draft".to_owned(),
            "review".to_owned(),
            "published".to_owned(),
        ]),
        PrimitiveType::UInt => PrimitiveValueArray::UInt(vec![0, 1, 2]),
        PrimitiveType::Int => PrimitiveValueArray::Int(vec![-2, 0, 4]),
        PrimitiveType::Byte => PrimitiveValueArray::Byte(vec![1, 5, 9]),
        PrimitiveType::Float => PrimitiveValueArray::Float(vec![
            OrderedFloat(1.0),
            OrderedFloat(2.5),
            OrderedFloat(4.0),
        ]),
        PrimitiveType::Boolean => PrimitiveValueArray::Boolean(vec![false, true]),
        PrimitiveType::Binary => {
            PrimitiveValueArray::Binary(vec![vec![0x10], vec![0x20], vec![0x30]])
        }
        PrimitiveType::Date => PrimitiveValueArray::Date(vec![
            example_date(2025, 1, 1),
            example_date(2025, 1, 15),
            example_date(2025, 2, 1),
        ]),
        PrimitiveType::Timestamp => PrimitiveValueArray::Timestamp(vec![
            1_700_000_000_000,
            1_710_000_000_000,
            1_720_000_000_000,
        ]),
    };

    if nullable {
        NullablePrimitiveValueArray::Nullable {
            values,
            null_index: 1,
        }
    } else {
        NullablePrimitiveValueArray::NonNull(values)
    }
}

fn finite_state_value(primitive_type: PrimitiveType) -> PrimitiveValue {
    match primitive_type {
        PrimitiveType::String => PrimitiveValue::String("review".to_owned()),
        PrimitiveType::UInt => PrimitiveValue::UInt(1),
        PrimitiveType::Int => PrimitiveValue::Int(0),
        PrimitiveType::Byte => PrimitiveValue::Byte(5),
        PrimitiveType::Float => PrimitiveValue::Float(OrderedFloat(2.5)),
        PrimitiveType::Boolean => PrimitiveValue::Boolean(true),
        PrimitiveType::Binary => PrimitiveValue::Binary(vec![0x20]),
        PrimitiveType::Date => PrimitiveValue::Date(example_date(2025, 1, 15)),
        PrimitiveType::Timestamp => PrimitiveValue::Timestamp(1_710_000_000_000),
    }
}

fn lvw_field_name(nullable: bool, is_array: bool, primitive_type: PrimitiveType) -> String {
    let shape = if is_array { "array" } else { "primitive" };
    format!(
        "lvw_{}_{}_{}",
        nullable_label(nullable),
        primitive_label(primitive_type),
        shape
    )
}

fn linear_list_field_name(primitive_type: PrimitiveType) -> String {
    format!("linear_list_{}", primitive_label(primitive_type))
}

fn total_order_register_field_name(direction: &Direction, primitive_type: PrimitiveType) -> String {
    format!(
        "total_order_register_{}_{}",
        direction_label(direction),
        primitive_label(primitive_type)
    )
}

fn finite_state_register_field_name(nullable: bool, primitive_type: PrimitiveType) -> String {
    format!(
        "finite_state_register_{}_{}",
        nullable_label(nullable),
        primitive_label(primitive_type)
    )
}

fn nullable_label(nullable: bool) -> &'static str {
    if nullable { "nullable" } else { "non_null" }
}

fn direction_label(direction: &Direction) -> &'static str {
    match direction {
        Direction::Ascending => "ascending",
        Direction::Descending => "descending",
    }
}

fn primitive_label(primitive_type: PrimitiveType) -> &'static str {
    match primitive_type {
        PrimitiveType::String => "string",
        PrimitiveType::UInt => "uint",
        PrimitiveType::Int => "int",
        PrimitiveType::Byte => "byte",
        PrimitiveType::Float => "float",
        PrimitiveType::Boolean => "boolean",
        PrimitiveType::Binary => "binary",
        PrimitiveType::Date => "date",
        PrimitiveType::Timestamp => "timestamp",
    }
}

fn primitive_types() -> [PrimitiveType; 9] {
    [
        PrimitiveType::String,
        PrimitiveType::UInt,
        PrimitiveType::Int,
        PrimitiveType::Byte,
        PrimitiveType::Float,
        PrimitiveType::Boolean,
        PrimitiveType::Binary,
        PrimitiveType::Date,
        PrimitiveType::Timestamp,
    ]
}

fn example_date(year: i32, month: u32, day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(year, month, day).expect("example dates are valid")
}

struct IdSource<Ids> {
    ids: Ids,
    builder: &'static str,
    consumed: usize,
    required_minimum: usize,
}
impl<Ids> IdSource<Ids> {
    fn new(ids: Ids, builder: &'static str, required_minimum: usize) -> Self {
        Self {
            ids,
            builder,
            consumed: 0,
            required_minimum,
        }
    }
}
impl<Id, Ids> IdSource<Ids>
where
    Ids: Iterator<Item = Id>,
{
    fn next_id(&mut self) -> Result<Id, ExampleBuildError> {
        let Some(id) = self.ids.next() else {
            return InsufficientIdsSnafu {
                builder: self.builder,
                consumed: self.consumed,
                required_minimum: self.required_minimum,
            }
            .fail();
        };
        self.consumed += 1;
        Ok(id)
    }
}

#[cfg(test)]
mod tests;
