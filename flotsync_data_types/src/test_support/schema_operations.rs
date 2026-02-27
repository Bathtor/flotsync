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
            SchemaOperation,
            validate_schema_operation_fields,
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

/// The number of fields in [`exhaustive_schema`].
pub const EXHAUSTIVE_SCHEMA_FIELD_COUNT: usize = 84;

/// The number of single-field operations returned by [`exhaustive_schema_operations`].
pub const EXHAUSTIVE_SCHEMA_OPERATION_COUNT: usize = 88;

/// The minimum number of IDs required by [`exhaustive_schema_operation`].
pub const EXHAUSTIVE_SCHEMA_OPERATION_MIN_ID_COUNT: usize = 138;

/// The minimum number of IDs required by [`exhaustive_schema_operations`].
pub const EXHAUSTIVE_SCHEMA_OPERATIONS_MIN_ID_COUNT: usize = 143;

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

/// Build a schema that covers every supported CRDT/data-type combination in the operations model.
pub fn exhaustive_schema() -> Schema {
    let mut columns = HashMap::with_capacity(EXHAUSTIVE_SCHEMA_FIELD_COUNT);

    for primitive_type in primitive_types() {
        insert_field(
            &mut columns,
            lvw_field_name(false, false, primitive_type),
            ReplicatedDataType::LatestValueWins {
                value_type: nullable_basic_primitive(false, primitive_type),
            },
        );
        insert_field(
            &mut columns,
            lvw_field_name(true, false, primitive_type),
            ReplicatedDataType::LatestValueWins {
                value_type: nullable_basic_primitive(true, primitive_type),
            },
        );
        insert_field(
            &mut columns,
            lvw_field_name(false, true, primitive_type),
            ReplicatedDataType::LatestValueWins {
                value_type: nullable_basic_array(false, primitive_type),
            },
        );
        insert_field(
            &mut columns,
            lvw_field_name(true, true, primitive_type),
            ReplicatedDataType::LatestValueWins {
                value_type: nullable_basic_array(true, primitive_type),
            },
        );
    }

    insert_field(
        &mut columns,
        "linear_string".to_owned(),
        ReplicatedDataType::LinearString,
    );

    for primitive_type in primitive_types() {
        insert_field(
            &mut columns,
            linear_list_field_name(primitive_type),
            ReplicatedDataType::LinearList {
                value_type: primitive_type,
            },
        );
    }

    insert_field(
        &mut columns,
        "monotonic_counter_byte".to_owned(),
        ReplicatedDataType::MonotonicCounter { small_range: true },
    );
    insert_field(
        &mut columns,
        "monotonic_counter_uint".to_owned(),
        ReplicatedDataType::MonotonicCounter { small_range: false },
    );

    for primitive_type in primitive_types() {
        insert_field(
            &mut columns,
            total_order_register_field_name(Direction::Ascending, primitive_type),
            ReplicatedDataType::TotalOrderRegister {
                value_type: primitive_type,
                direction: Direction::Ascending,
            },
        );
        insert_field(
            &mut columns,
            total_order_register_field_name(Direction::Descending, primitive_type),
            ReplicatedDataType::TotalOrderRegister {
                value_type: primitive_type,
                direction: Direction::Descending,
            },
        );
    }

    for primitive_type in primitive_types() {
        insert_field(
            &mut columns,
            finite_state_register_field_name(false, primitive_type),
            ReplicatedDataType::TotalOrderFiniteStateRegister {
                value_type: nullable_primitive_type(false, primitive_type),
                states: finite_state_values(false, primitive_type),
            },
        );
        insert_field(
            &mut columns,
            finite_state_register_field_name(true, primitive_type),
            ReplicatedDataType::TotalOrderFiniteStateRegister {
                value_type: nullable_primitive_type(true, primitive_type),
                states: finite_state_values(true, primitive_type),
            },
        );
    }

    Schema {
        columns,
        metadata: HashMap::new(),
    }
}

/// Build one partial schema operation per example payload shape.
///
/// The iterator must yield at least [`EXHAUSTIVE_SCHEMA_OPERATIONS_MIN_ID_COUNT`] IDs.
pub fn exhaustive_schema_operations<Id>(
    ids: impl Iterator<Item = Id>,
) -> Result<Vec<SchemaOperation<'static, Id>>, ExampleBuildError> {
    let mut ids = IdSource::new(
        ids,
        "exhaustive_schema_operations",
        EXHAUSTIVE_SCHEMA_OPERATIONS_MIN_ID_COUNT,
    );
    let mut operations = Vec::with_capacity(EXHAUSTIVE_SCHEMA_OPERATION_COUNT);

    for primitive_type in primitive_types() {
        operations.push(single_field_operation(
            lvw_field_name(false, false, primitive_type),
            latest_value_wins_operation(
                &mut ids,
                nullable_basic_value(false, false, primitive_type),
            )?,
        ));
        operations.push(single_field_operation(
            lvw_field_name(true, false, primitive_type),
            latest_value_wins_operation(
                &mut ids,
                nullable_basic_value(true, false, primitive_type),
            )?,
        ));
        operations.push(single_field_operation(
            lvw_field_name(false, true, primitive_type),
            latest_value_wins_operation(
                &mut ids,
                nullable_basic_value(false, true, primitive_type),
            )?,
        ));
        operations.push(single_field_operation(
            lvw_field_name(true, true, primitive_type),
            latest_value_wins_operation(
                &mut ids,
                nullable_basic_value(true, true, primitive_type),
            )?,
        ));
    }

    operations.push(single_field_operation(
        "linear_string".to_owned(),
        linear_string_insert(&mut ids, "example linear string".to_owned())?,
    ));

    for primitive_type in primitive_types() {
        operations.push(single_field_operation(
            linear_list_field_name(primitive_type),
            linear_list_insert(&mut ids, primitive_value_array(primitive_type))?,
        ));
    }

    operations.push(single_field_operation(
        "monotonic_counter_byte".to_owned(),
        OperationValue::MonotonicCounterIncrement(CounterValue::Byte(7)),
    ));
    operations.push(single_field_operation(
        "monotonic_counter_uint".to_owned(),
        OperationValue::MonotonicCounterIncrement(CounterValue::UInt(42)),
    ));

    for primitive_type in primitive_types() {
        operations.push(single_field_operation(
            total_order_register_field_name(Direction::Ascending, primitive_type),
            OperationValue::TotalOrderRegisterSet(primitive_value(primitive_type)),
        ));
        operations.push(single_field_operation(
            total_order_register_field_name(Direction::Descending, primitive_type),
            OperationValue::TotalOrderRegisterSet(primitive_value(primitive_type)),
        ));
    }

    for primitive_type in primitive_types() {
        operations.push(single_field_operation(
            finite_state_register_field_name(false, primitive_type),
            OperationValue::TotalOrderFiniteStateRegisterSet(NullablePrimitiveValue::Value(
                finite_state_value(primitive_type),
            )),
        ));
        operations.push(single_field_operation(
            finite_state_register_field_name(true, primitive_type),
            OperationValue::TotalOrderFiniteStateRegisterSet(NullablePrimitiveValue::Value(
                finite_state_value(primitive_type),
            )),
        ));
    }

    operations.push(single_field_operation(
        lvw_field_name(true, false, PrimitiveType::String),
        latest_value_wins_operation(&mut ids, NullableBasicValue::Null)?,
    ));
    operations.push(single_field_operation(
        "linear_string".to_owned(),
        linear_string_delete(&mut ids)?,
    ));
    operations.push(single_field_operation(
        linear_list_field_name(PrimitiveType::String),
        linear_list_delete(&mut ids)?,
    ));
    operations.push(single_field_operation(
        finite_state_register_field_name(true, PrimitiveType::String),
        OperationValue::TotalOrderFiniteStateRegisterSet(NullablePrimitiveValue::Null),
    ));

    debug_assert_eq!(operations.len(), EXHAUSTIVE_SCHEMA_OPERATION_COUNT);
    Ok(operations)
}

/// Build one combined schema operation containing one field update for every field in [`exhaustive_schema`].
///
/// The iterator must yield at least [`EXHAUSTIVE_SCHEMA_OPERATION_MIN_ID_COUNT`] IDs.
pub fn exhaustive_schema_operation<Id>(
    ids: impl Iterator<Item = Id>,
) -> Result<SchemaOperation<'static, Id>, ExampleBuildError> {
    let mut ids = IdSource::new(
        ids,
        "exhaustive_schema_operation",
        EXHAUSTIVE_SCHEMA_OPERATION_MIN_ID_COUNT,
    );
    let mut fields = Vec::with_capacity(EXHAUSTIVE_SCHEMA_FIELD_COUNT);

    for primitive_type in primitive_types() {
        fields.push(field(
            lvw_field_name(false, false, primitive_type),
            latest_value_wins_operation(
                &mut ids,
                nullable_basic_value(false, false, primitive_type),
            )?,
        ));
        fields.push(field(
            lvw_field_name(true, false, primitive_type),
            latest_value_wins_operation(
                &mut ids,
                nullable_basic_value(true, false, primitive_type),
            )?,
        ));
        fields.push(field(
            lvw_field_name(false, true, primitive_type),
            latest_value_wins_operation(
                &mut ids,
                nullable_basic_value(false, true, primitive_type),
            )?,
        ));
        fields.push(field(
            lvw_field_name(true, true, primitive_type),
            latest_value_wins_operation(
                &mut ids,
                nullable_basic_value(true, true, primitive_type),
            )?,
        ));
    }

    fields.push(field(
        "linear_string".to_owned(),
        linear_string_insert(&mut ids, "example linear string".to_owned())?,
    ));

    for primitive_type in primitive_types() {
        fields.push(field(
            linear_list_field_name(primitive_type),
            linear_list_insert(&mut ids, primitive_value_array(primitive_type))?,
        ));
    }

    fields.push(field(
        "monotonic_counter_byte".to_owned(),
        OperationValue::MonotonicCounterIncrement(CounterValue::Byte(7)),
    ));
    fields.push(field(
        "monotonic_counter_uint".to_owned(),
        OperationValue::MonotonicCounterIncrement(CounterValue::UInt(42)),
    ));

    for primitive_type in primitive_types() {
        fields.push(field(
            total_order_register_field_name(Direction::Ascending, primitive_type),
            OperationValue::TotalOrderRegisterSet(primitive_value(primitive_type)),
        ));
        fields.push(field(
            total_order_register_field_name(Direction::Descending, primitive_type),
            OperationValue::TotalOrderRegisterSet(primitive_value(primitive_type)),
        ));
    }

    for primitive_type in primitive_types() {
        fields.push(field(
            finite_state_register_field_name(false, primitive_type),
            OperationValue::TotalOrderFiniteStateRegisterSet(NullablePrimitiveValue::Value(
                finite_state_value(primitive_type),
            )),
        ));
        fields.push(field(
            finite_state_register_field_name(true, primitive_type),
            OperationValue::TotalOrderFiniteStateRegisterSet(NullablePrimitiveValue::Value(
                finite_state_value(primitive_type),
            )),
        ));
    }

    debug_assert_eq!(fields.len(), EXHAUSTIVE_SCHEMA_FIELD_COUNT);
    Ok(SchemaOperation { fields })
}

fn insert_field(columns: &mut HashMap<String, Field>, name: String, data_type: ReplicatedDataType) {
    columns.insert(
        name.clone(),
        Field {
            name,
            data_type,
            metadata: HashMap::new(),
        },
    );
}

fn field<Id>(field_name: String, value: OperationValue<Id>) -> OperationFieldValue<'static, Id> {
    OperationFieldValue {
        field_name: Cow::Owned(field_name),
        value,
    }
}

fn single_field_operation<Id>(
    field_name: String,
    value: OperationValue<Id>,
) -> SchemaOperation<'static, Id> {
    SchemaOperation {
        fields: vec![field(field_name, value)],
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
        id: ids.next_id()?,
        pred: ids.next_id()?,
        succ: ids.next_id()?,
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
    Ok(OperationValue::LinearString(DataOperation::Insert {
        id: indexed_id(ids.next_id()?),
        pred: indexed_id(ids.next_id()?),
        succ: indexed_id(ids.next_id()?),
        value,
    }))
}

fn linear_string_delete<Id, Ids>(
    ids: &mut IdSource<Ids>,
) -> Result<OperationValue<Id>, ExampleBuildError>
where
    Ids: Iterator<Item = Id>,
{
    Ok(OperationValue::LinearString(DataOperation::Delete {
        start: indexed_id(ids.next_id()?),
        end: None,
    }))
}

fn linear_list_insert<Id, Ids>(
    ids: &mut IdSource<Ids>,
    value: PrimitiveValueArray,
) -> Result<OperationValue<Id>, ExampleBuildError>
where
    Ids: Iterator<Item = Id>,
{
    Ok(OperationValue::LinearList(DataOperation::Insert {
        id: indexed_id(ids.next_id()?),
        pred: indexed_id(ids.next_id()?),
        succ: indexed_id(ids.next_id()?),
        value,
    }))
}

fn linear_list_delete<Id, Ids>(
    ids: &mut IdSource<Ids>,
) -> Result<OperationValue<Id>, ExampleBuildError>
where
    Ids: Iterator<Item = Id>,
{
    Ok(OperationValue::LinearList(DataOperation::Delete {
        start: indexed_id(ids.next_id()?),
        end: None,
    }))
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

fn total_order_register_field_name(direction: Direction, primitive_type: PrimitiveType) -> String {
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

fn direction_label(direction: Direction) -> &'static str {
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
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    enum PrimitiveCoverageKey {
        String,
        UInt,
        Int,
        Byte,
        Float,
        Boolean,
        Binary,
        Date,
        Timestamp,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    enum NullableCoverageKey {
        NonNull,
        Nullable,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    enum DirectionCoverageKey {
        Ascending,
        Descending,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    enum BasicCoverageKey {
        Primitive(PrimitiveCoverageKey),
        Array(PrimitiveCoverageKey),
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    enum SchemaCoverageKey {
        LatestValueWins {
            nullable: NullableCoverageKey,
            value: BasicCoverageKey,
        },
        LinearString,
        LinearList {
            value_type: PrimitiveCoverageKey,
        },
        MonotonicCounter {
            small_range: bool,
        },
        TotalOrderRegister {
            direction: DirectionCoverageKey,
            value_type: PrimitiveCoverageKey,
        },
        TotalOrderFiniteStateRegister {
            nullable: NullableCoverageKey,
            value_type: PrimitiveCoverageKey,
        },
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    enum OperationCoverageKey {
        LatestValueWinsValue {
            nullable: NullableCoverageKey,
            value: BasicCoverageKey,
        },
        LatestValueWinsNull,
        LinearStringInsert,
        LinearStringDelete,
        LinearListInsert {
            value_type: PrimitiveCoverageKey,
        },
        LinearListDelete,
        MonotonicCounterIncrement {
            small_range: bool,
        },
        TotalOrderRegisterSet {
            direction: DirectionCoverageKey,
            value_type: PrimitiveCoverageKey,
        },
        TotalOrderFiniteStateRegisterValue {
            nullable: NullableCoverageKey,
            value_type: PrimitiveCoverageKey,
        },
        TotalOrderFiniteStateRegisterNull,
    }

    fn primitive_coverage_key(primitive_type: PrimitiveType) -> PrimitiveCoverageKey {
        match primitive_type {
            PrimitiveType::String => PrimitiveCoverageKey::String,
            PrimitiveType::UInt => PrimitiveCoverageKey::UInt,
            PrimitiveType::Int => PrimitiveCoverageKey::Int,
            PrimitiveType::Byte => PrimitiveCoverageKey::Byte,
            PrimitiveType::Float => PrimitiveCoverageKey::Float,
            PrimitiveType::Boolean => PrimitiveCoverageKey::Boolean,
            PrimitiveType::Binary => PrimitiveCoverageKey::Binary,
            PrimitiveType::Date => PrimitiveCoverageKey::Date,
            PrimitiveType::Timestamp => PrimitiveCoverageKey::Timestamp,
        }
    }

    fn nullable_coverage_key(nullable: bool) -> NullableCoverageKey {
        if nullable {
            NullableCoverageKey::Nullable
        } else {
            NullableCoverageKey::NonNull
        }
    }

    fn direction_coverage_key(direction: Direction) -> DirectionCoverageKey {
        match direction {
            Direction::Ascending => DirectionCoverageKey::Ascending,
            Direction::Descending => DirectionCoverageKey::Descending,
        }
    }

    fn basic_coverage_key(value_type: &BasicDataType) -> BasicCoverageKey {
        match value_type {
            BasicDataType::Primitive(primitive_type) => {
                BasicCoverageKey::Primitive(primitive_coverage_key(*primitive_type))
            }
            BasicDataType::Array(array_type) => {
                BasicCoverageKey::Array(primitive_coverage_key(array_type.element_type))
            }
        }
    }

    fn nullable_basic_coverage_key(value_type: &NullableBasicDataType) -> SchemaCoverageKey {
        match value_type {
            NullableBasicDataType::NonNull(value_type) => SchemaCoverageKey::LatestValueWins {
                nullable: NullableCoverageKey::NonNull,
                value: basic_coverage_key(value_type),
            },
            NullableBasicDataType::Nullable(value_type) => SchemaCoverageKey::LatestValueWins {
                nullable: NullableCoverageKey::Nullable,
                value: basic_coverage_key(value_type),
            },
        }
    }

    fn nullable_primitive_coverage_parts(
        value_type: NullablePrimitiveType,
    ) -> (NullableCoverageKey, PrimitiveCoverageKey) {
        match value_type {
            NullablePrimitiveType::NonNull(value_type) => (
                NullableCoverageKey::NonNull,
                primitive_coverage_key(value_type),
            ),
            NullablePrimitiveType::Nullable(value_type) => (
                NullableCoverageKey::Nullable,
                primitive_coverage_key(value_type),
            ),
        }
    }

    fn schema_coverage_key(data_type: &ReplicatedDataType) -> SchemaCoverageKey {
        match data_type {
            ReplicatedDataType::LatestValueWins { value_type } => {
                nullable_basic_coverage_key(value_type)
            }
            ReplicatedDataType::LinearString => SchemaCoverageKey::LinearString,
            ReplicatedDataType::LinearList { value_type } => SchemaCoverageKey::LinearList {
                value_type: primitive_coverage_key(*value_type),
            },
            ReplicatedDataType::MonotonicCounter { small_range } => {
                SchemaCoverageKey::MonotonicCounter {
                    small_range: *small_range,
                }
            }
            ReplicatedDataType::TotalOrderRegister {
                value_type,
                direction,
            } => SchemaCoverageKey::TotalOrderRegister {
                direction: direction_coverage_key(direction.clone()),
                value_type: primitive_coverage_key(*value_type),
            },
            ReplicatedDataType::TotalOrderFiniteStateRegister { value_type, .. } => {
                let (nullable, value_type) = nullable_primitive_coverage_parts(*value_type);
                SchemaCoverageKey::TotalOrderFiniteStateRegister {
                    nullable,
                    value_type,
                }
            }
        }
    }

    fn operation_coverage_key<Id>(
        data_type: &ReplicatedDataType,
        value: &OperationValue<Id>,
    ) -> OperationCoverageKey {
        match (data_type, value) {
            (
                ReplicatedDataType::LatestValueWins { value_type },
                OperationValue::LatestValueWins(UpdateOperation { value, .. }),
            ) => {
                let (nullable, basic_type) = match value_type {
                    NullableBasicDataType::NonNull(value_type) => {
                        (NullableCoverageKey::NonNull, basic_coverage_key(value_type))
                    }
                    NullableBasicDataType::Nullable(value_type) => (
                        NullableCoverageKey::Nullable,
                        basic_coverage_key(value_type),
                    ),
                };
                match value {
                    NullableBasicValue::Null => OperationCoverageKey::LatestValueWinsNull,
                    NullableBasicValue::Value(_) => OperationCoverageKey::LatestValueWinsValue {
                        nullable,
                        value: basic_type,
                    },
                }
            }
            (ReplicatedDataType::LinearString, OperationValue::LinearString(value)) => {
                match value {
                    DataOperation::Insert { .. } => OperationCoverageKey::LinearStringInsert,
                    DataOperation::Delete { .. } => OperationCoverageKey::LinearStringDelete,
                }
            }
            (ReplicatedDataType::LinearList { value_type }, OperationValue::LinearList(value)) => {
                match value {
                    DataOperation::Insert { .. } => OperationCoverageKey::LinearListInsert {
                        value_type: primitive_coverage_key(*value_type),
                    },
                    DataOperation::Delete { .. } => OperationCoverageKey::LinearListDelete,
                }
            }
            (
                ReplicatedDataType::MonotonicCounter { small_range },
                OperationValue::MonotonicCounterIncrement(_),
            ) => OperationCoverageKey::MonotonicCounterIncrement {
                small_range: *small_range,
            },
            (
                ReplicatedDataType::TotalOrderRegister {
                    value_type,
                    direction,
                },
                OperationValue::TotalOrderRegisterSet(_),
            ) => OperationCoverageKey::TotalOrderRegisterSet {
                direction: direction_coverage_key(direction.clone()),
                value_type: primitive_coverage_key(*value_type),
            },
            (
                ReplicatedDataType::TotalOrderFiniteStateRegister { value_type, .. },
                OperationValue::TotalOrderFiniteStateRegisterSet(value),
            ) => {
                let (nullable, value_type) = nullable_primitive_coverage_parts(*value_type);
                match value {
                    NullablePrimitiveValue::Null => {
                        OperationCoverageKey::TotalOrderFiniteStateRegisterNull
                    }
                    NullablePrimitiveValue::Value(_) => {
                        OperationCoverageKey::TotalOrderFiniteStateRegisterValue {
                            nullable,
                            value_type,
                        }
                    }
                }
            }
            (ReplicatedDataType::LatestValueWins { .. }, _)
            | (ReplicatedDataType::LinearString, _)
            | (ReplicatedDataType::LinearList { .. }, _)
            | (ReplicatedDataType::MonotonicCounter { .. }, _)
            | (ReplicatedDataType::TotalOrderRegister { .. }, _)
            | (ReplicatedDataType::TotalOrderFiniteStateRegister { .. }, _) => {
                panic!("operation does not match schema data type")
            }
        }
    }

    fn expected_schema_coverage() -> BTreeSet<SchemaCoverageKey> {
        let mut coverage = BTreeSet::new();

        for primitive_type in primitive_types() {
            let primitive_type = primitive_coverage_key(primitive_type);
            for nullable in [false, true] {
                coverage.insert(SchemaCoverageKey::LatestValueWins {
                    nullable: nullable_coverage_key(nullable),
                    value: BasicCoverageKey::Primitive(primitive_type),
                });
                coverage.insert(SchemaCoverageKey::LatestValueWins {
                    nullable: nullable_coverage_key(nullable),
                    value: BasicCoverageKey::Array(primitive_type),
                });
            }

            coverage.insert(SchemaCoverageKey::LinearList {
                value_type: primitive_type,
            });

            for direction in [Direction::Ascending, Direction::Descending] {
                coverage.insert(SchemaCoverageKey::TotalOrderRegister {
                    direction: direction_coverage_key(direction),
                    value_type: primitive_type,
                });
            }

            for nullable in [false, true] {
                coverage.insert(SchemaCoverageKey::TotalOrderFiniteStateRegister {
                    nullable: nullable_coverage_key(nullable),
                    value_type: primitive_type,
                });
            }
        }

        coverage.insert(SchemaCoverageKey::LinearString);
        coverage.insert(SchemaCoverageKey::MonotonicCounter { small_range: true });
        coverage.insert(SchemaCoverageKey::MonotonicCounter { small_range: false });
        coverage
    }

    fn expected_operation_coverage() -> BTreeSet<OperationCoverageKey> {
        let mut coverage = BTreeSet::new();

        for primitive_type in primitive_types() {
            let primitive_type = primitive_coverage_key(primitive_type);
            for nullable in [false, true] {
                coverage.insert(OperationCoverageKey::LatestValueWinsValue {
                    nullable: nullable_coverage_key(nullable),
                    value: BasicCoverageKey::Primitive(primitive_type),
                });
                coverage.insert(OperationCoverageKey::LatestValueWinsValue {
                    nullable: nullable_coverage_key(nullable),
                    value: BasicCoverageKey::Array(primitive_type),
                });
                coverage.insert(OperationCoverageKey::TotalOrderFiniteStateRegisterValue {
                    nullable: nullable_coverage_key(nullable),
                    value_type: primitive_type,
                });
            }

            coverage.insert(OperationCoverageKey::LinearListInsert {
                value_type: primitive_type,
            });

            for direction in [Direction::Ascending, Direction::Descending] {
                coverage.insert(OperationCoverageKey::TotalOrderRegisterSet {
                    direction: direction_coverage_key(direction),
                    value_type: primitive_type,
                });
            }
        }

        coverage.insert(OperationCoverageKey::LatestValueWinsNull);
        coverage.insert(OperationCoverageKey::LinearStringInsert);
        coverage.insert(OperationCoverageKey::LinearStringDelete);
        coverage.insert(OperationCoverageKey::LinearListDelete);
        coverage.insert(OperationCoverageKey::MonotonicCounterIncrement { small_range: true });
        coverage.insert(OperationCoverageKey::MonotonicCounterIncrement { small_range: false });
        coverage.insert(OperationCoverageKey::TotalOrderFiniteStateRegisterNull);
        coverage
    }

    #[test]
    fn exhaustive_schema_has_expected_number_of_fields() {
        let schema = exhaustive_schema();
        assert_eq!(schema.columns.len(), EXHAUSTIVE_SCHEMA_FIELD_COUNT);
    }

    #[test]
    fn exhaustive_schema_covers_all_supported_schema_shapes() {
        let schema = exhaustive_schema();
        let actual = schema
            .columns
            .values()
            .map(|field| schema_coverage_key(&field.data_type))
            .collect::<BTreeSet<_>>();

        assert_eq!(actual, expected_schema_coverage());
    }

    #[test]
    fn exhaustive_schema_operations_validate_against_exhaustive_schema() {
        let schema = exhaustive_schema();
        let operations = exhaustive_schema_operations(0u32..).unwrap();

        assert_eq!(operations.len(), EXHAUSTIVE_SCHEMA_OPERATION_COUNT);
        for (index, operation) in operations.iter().enumerate() {
            validate_schema_operation_fields(&schema, &operation.fields).unwrap_or_else(|error| {
                panic!(
                    "single-field operation {index} failed validation: {error:?}; fields={:?}",
                    operation.fields
                )
            });
        }
    }

    #[test]
    fn exhaustive_schema_operations_cover_all_supported_operation_shapes() {
        let schema = exhaustive_schema();
        let operations = exhaustive_schema_operations(0u32..).unwrap();
        let actual = operations
            .iter()
            .map(|operation| {
                let [field] = operation.fields.as_slice() else {
                    panic!("expected single-field operation");
                };
                let data_type = &schema
                    .columns
                    .get(field.field_name.as_ref())
                    .expect("example field exists in exhaustive schema")
                    .data_type;
                operation_coverage_key(data_type, &field.value)
            })
            .collect::<BTreeSet<_>>();

        assert_eq!(actual, expected_operation_coverage());
    }

    #[test]
    fn exhaustive_schema_operation_validates_against_exhaustive_schema() {
        let schema = exhaustive_schema();
        let operation = exhaustive_schema_operation(0u32..).unwrap();

        assert_eq!(operation.fields.len(), EXHAUSTIVE_SCHEMA_FIELD_COUNT);
        operation
            .validate_against_schema(&schema)
            .unwrap_or_else(|error| {
                panic!(
                    "combined operation failed validation: {error:?}; fields={:?}",
                    operation.fields
                )
            });
    }

    #[test]
    fn exhaustive_schema_operation_accepts_exact_minimum_id_count() {
        let operation =
            exhaustive_schema_operation(0u32..EXHAUSTIVE_SCHEMA_OPERATION_MIN_ID_COUNT as u32)
                .unwrap();
        assert_eq!(operation.fields.len(), EXHAUSTIVE_SCHEMA_FIELD_COUNT);
    }

    #[test]
    fn exhaustive_schema_operations_accept_exact_minimum_id_count() {
        let operations =
            exhaustive_schema_operations(0u32..EXHAUSTIVE_SCHEMA_OPERATIONS_MIN_ID_COUNT as u32)
                .unwrap();
        assert_eq!(operations.len(), EXHAUSTIVE_SCHEMA_OPERATION_COUNT);
    }

    #[test]
    fn exhaustive_schema_operation_reports_insufficient_ids() {
        let err = exhaustive_schema_operation(
            0u32..(EXHAUSTIVE_SCHEMA_OPERATION_MIN_ID_COUNT as u32 - 1),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            ExampleBuildError::InsufficientIds {
                builder: "exhaustive_schema_operation",
                required_minimum: EXHAUSTIVE_SCHEMA_OPERATION_MIN_ID_COUNT,
                ..
            }
        ));
    }

    #[test]
    fn exhaustive_schema_operations_report_insufficient_ids() {
        let err = exhaustive_schema_operations(
            0u32..(EXHAUSTIVE_SCHEMA_OPERATIONS_MIN_ID_COUNT as u32 - 1),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            ExampleBuildError::InsufficientIds {
                builder: "exhaustive_schema_operations",
                required_minimum: EXHAUSTIVE_SCHEMA_OPERATIONS_MIN_ID_COUNT,
                ..
            }
        ));
    }
}
