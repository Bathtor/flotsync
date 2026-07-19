//! Construction of the exhaustive example schema.

use super::*;

/// Build a schema that covers every supported CRDT/data-type combination in the operations model.
#[must_use]
#[allow(
    clippy::too_many_lines,
    reason = "The exhaustive schema intentionally lists every supported CRDT/data-type combination."
)]
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
            total_order_register_field_name(&Direction::Ascending, primitive_type),
            ReplicatedDataType::TotalOrderRegister {
                value_type: primitive_type,
                direction: Direction::Ascending,
            },
        );
        insert_field(
            &mut columns,
            total_order_register_field_name(&Direction::Descending, primitive_type),
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

    set_field_default(
        &mut columns,
        lvw_field_name(false, false, PrimitiveType::String),
        nullable_basic_value(false, false, PrimitiveType::String),
    );
    set_field_default(
        &mut columns,
        "linear_string".to_owned(),
        "default-linear-string",
    );
    set_field_default(
        &mut columns,
        linear_list_field_name(PrimitiveType::Int),
        vec![-7i64, 0, 9],
    );
    set_field_default(&mut columns, "monotonic_counter_uint".to_owned(), 42u64);
    set_field_default(
        &mut columns,
        total_order_register_field_name(&Direction::Ascending, PrimitiveType::UInt),
        42u64,
    );
    set_field_default(
        &mut columns,
        finite_state_register_field_name(true, PrimitiveType::String),
        "review",
    );

    Schema {
        columns,
        metadata: HashMap::new(),
    }
}
