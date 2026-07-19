//! Construction of exhaustive example schema operations.

use super::*;

/// Build one partial schema operation per example payload shape.
///
/// The `change_ids` iterator must yield at least [`EXHAUSTIVE_SCHEMA_OPERATIONS_MIN_ID_COUNT`]
/// ids and `row_ids` must yield at least [`EXHAUSTIVE_SCHEMA_OPERATION_COUNT`] ids.
///
/// # Errors
///
/// See `ExampleBuildError` for failure conditions.
#[allow(
    clippy::too_many_lines,
    reason = "The exhaustive operation builder mirrors the exhaustive schema field set."
)]
pub fn exhaustive_schema_operations<RowId, ChangeId>(
    row_ids: impl Iterator<Item = RowId>,
    change_ids: impl Iterator<Item = ChangeId>,
) -> Result<Vec<SchemaOperation<'static, RowId, ChangeId>>, ExampleBuildError>
where
    RowId: Clone,
    ChangeId: Clone,
{
    let mut row_ids = IdSource::new(
        row_ids,
        "exhaustive_schema_operations(row_ids)",
        EXHAUSTIVE_SCHEMA_OPERATION_COUNT,
    );
    let mut ids = IdSource::new(
        change_ids,
        "exhaustive_schema_operations",
        EXHAUSTIVE_SCHEMA_OPERATIONS_MIN_ID_COUNT,
    );
    let mut operations = Vec::with_capacity(EXHAUSTIVE_SCHEMA_OPERATION_COUNT);

    for primitive_type in primitive_types() {
        operations.push(single_field_operation(
            row_ids.next_id()?,
            ids.next_id()?,
            lvw_field_name(false, false, primitive_type),
            latest_value_wins_operation(
                &mut ids,
                nullable_basic_value(false, false, primitive_type),
            )?,
        ));
        operations.push(single_field_operation(
            row_ids.next_id()?,
            ids.next_id()?,
            lvw_field_name(true, false, primitive_type),
            latest_value_wins_operation(
                &mut ids,
                nullable_basic_value(true, false, primitive_type),
            )?,
        ));
        operations.push(single_field_operation(
            row_ids.next_id()?,
            ids.next_id()?,
            lvw_field_name(false, true, primitive_type),
            latest_value_wins_operation(
                &mut ids,
                nullable_basic_value(false, true, primitive_type),
            )?,
        ));
        operations.push(single_field_operation(
            row_ids.next_id()?,
            ids.next_id()?,
            lvw_field_name(true, true, primitive_type),
            latest_value_wins_operation(
                &mut ids,
                nullable_basic_value(true, true, primitive_type),
            )?,
        ));
    }

    operations.push(single_field_operation(
        row_ids.next_id()?,
        ids.next_id()?,
        "linear_string".to_owned(),
        linear_string_insert(&mut ids, "example linear string".to_owned())?,
    ));

    for primitive_type in primitive_types() {
        operations.push(single_field_operation(
            row_ids.next_id()?,
            ids.next_id()?,
            linear_list_field_name(primitive_type),
            linear_list_insert(&mut ids, primitive_value_array(primitive_type))?,
        ));
    }

    operations.push(single_field_operation(
        row_ids.next_id()?,
        ids.next_id()?,
        "monotonic_counter_byte".to_owned(),
        OperationValue::MonotonicCounterIncrement(CounterValue::Byte(7)),
    ));
    operations.push(single_field_operation(
        row_ids.next_id()?,
        ids.next_id()?,
        "monotonic_counter_uint".to_owned(),
        OperationValue::MonotonicCounterIncrement(CounterValue::UInt(42)),
    ));

    for primitive_type in primitive_types() {
        operations.push(single_field_operation(
            row_ids.next_id()?,
            ids.next_id()?,
            total_order_register_field_name(&Direction::Ascending, primitive_type),
            OperationValue::TotalOrderRegisterSet(primitive_value(primitive_type)),
        ));
        operations.push(single_field_operation(
            row_ids.next_id()?,
            ids.next_id()?,
            total_order_register_field_name(&Direction::Descending, primitive_type),
            OperationValue::TotalOrderRegisterSet(primitive_value(primitive_type)),
        ));
    }

    for primitive_type in primitive_types() {
        operations.push(single_field_operation(
            row_ids.next_id()?,
            ids.next_id()?,
            finite_state_register_field_name(false, primitive_type),
            OperationValue::TotalOrderFiniteStateRegisterSet(NullablePrimitiveValue::Value(
                finite_state_value(primitive_type),
            )),
        ));
        operations.push(single_field_operation(
            row_ids.next_id()?,
            ids.next_id()?,
            finite_state_register_field_name(true, primitive_type),
            OperationValue::TotalOrderFiniteStateRegisterSet(NullablePrimitiveValue::Value(
                finite_state_value(primitive_type),
            )),
        ));
    }

    operations.push(single_field_operation(
        row_ids.next_id()?,
        ids.next_id()?,
        lvw_field_name(true, false, PrimitiveType::String),
        latest_value_wins_operation(&mut ids, NullableBasicValue::Null)?,
    ));
    operations.push(single_field_operation(
        row_ids.next_id()?,
        ids.next_id()?,
        "linear_string".to_owned(),
        linear_string_delete(&mut ids)?,
    ));
    operations.push(single_field_operation(
        row_ids.next_id()?,
        ids.next_id()?,
        linear_list_field_name(PrimitiveType::String),
        linear_list_delete(&mut ids)?,
    ));
    operations.push(single_field_operation(
        row_ids.next_id()?,
        ids.next_id()?,
        finite_state_register_field_name(true, PrimitiveType::String),
        OperationValue::TotalOrderFiniteStateRegisterSet(NullablePrimitiveValue::Null),
    ));

    debug_assert_eq!(operations.len(), EXHAUSTIVE_SCHEMA_OPERATION_COUNT);
    Ok(operations)
}

/// Build one combined schema operation containing one field update for every field in [`exhaustive_schema`].
///
/// The iterator must yield at least [`EXHAUSTIVE_SCHEMA_OPERATION_MIN_ID_COUNT`] IDs.
///
/// # Errors
///
/// See `ExampleBuildError` for failure conditions.
pub fn exhaustive_schema_operation<RowId, ChangeId>(
    row_id: RowId,
    ids: impl Iterator<Item = ChangeId>,
) -> Result<SchemaOperation<'static, RowId, ChangeId>, ExampleBuildError>
where
    ChangeId: Clone,
{
    let mut ids = IdSource::new(
        ids,
        "exhaustive_schema_operation",
        EXHAUSTIVE_SCHEMA_OPERATION_MIN_ID_COUNT,
    );
    let change_id = ids.next_id()?;
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
            total_order_register_field_name(&Direction::Ascending, primitive_type),
            OperationValue::TotalOrderRegisterSet(primitive_value(primitive_type)),
        ));
        fields.push(field(
            total_order_register_field_name(&Direction::Descending, primitive_type),
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
    Ok(SchemaOperation {
        change_id,
        operation: RowOperation::Update { row_id, fields },
    })
}
