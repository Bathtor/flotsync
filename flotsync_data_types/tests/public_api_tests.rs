use flotsync_data_types::{
    DataOperation,
    FieldOperations,
    OperationError,
    OperationOutcome,
    Schema,
    TableOperations,
    initial_values,
    schema::{
        Direction,
        Field,
        PrimitiveType,
        datamodel::{InMemoryData, OperationValue, RowOperation, SchemaValueError},
        values::PrimitiveValueArray,
    },
    update_values,
};
use std::{assert_matches, borrow::Cow, sync::LazyLock};

static TEST_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    Schema::from_fields([
        Field::linear_string("title"),
        Field::linear_list("numbers", PrimitiveType::Int),
        Field::monotonic_counter("counter"),
        Field::total_order_register("priority", PrimitiveType::UInt, Direction::Ascending),
    ])
});

type RowId = u64;
type OperationId = u32;

fn expect_applied<T>(outcome: OperationOutcome<T>) -> T {
    match outcome {
        OperationOutcome::Applied(operation) => operation,
        OperationOutcome::NoChanges => panic!("expected an applied operation"),
    }
}

#[test]
fn table_operations_insert_modify_and_delete_rows() {
    let schema = &*TEST_SCHEMA;

    let mut data: InMemoryData<RowId, OperationId> = InMemoryData::with_static_schema(schema);

    {
        let insert = data
            .insert_row(
                10,
                1,
                initial_values! {
                    schema["title"] => "hello",
                    schema["numbers"] => vec![1i64, 2, 3],
                    schema["counter"] => 4u64,
                    schema["priority"] => 7u64,
                },
            )
            .unwrap();
        assert_matches!(insert.operation, RowOperation::Insert { row_id, .. } if row_id == 1);
    }
    assert_eq!(data.len(), 1);
    assert!(!data.is_empty());
    assert_eq!(data.num_active_rows(), 1);

    {
        let row = data.get_row(&1).unwrap();

        let title_value: Cow<'_, str> = schema["title"].get_value(&row).unwrap();
        let number_values: Cow<'_, [i64]> = schema["numbers"].get_value(&row).unwrap();
        let counter_value: Cow<'_, u64> = schema["counter"].get_value(&row).unwrap();
        let priority_value: Cow<'_, u64> = schema["priority"].get_value(&row).unwrap();

        assert_eq!(title_value.as_ref(), "hello");
        assert_eq!(number_values.as_ref(), &[1, 2, 3]);
        assert_eq!(*counter_value, 4);
        assert_eq!(*priority_value, 7);
    }

    let no_changes = data
        .modify_row(
            11,
            1,
            update_values! {
                schema["title"] => "hello",
                schema["numbers"] => vec![1i64, 2, 3],
                schema["counter"] => 4u64,
                schema["priority"] => 7u64,
            },
        )
        .unwrap();
    assert_matches!(no_changes, OperationOutcome::NoChanges);

    {
        let update = data
            .modify_row(
                12,
                1,
                update_values! {
                    schema["title"] => "hello world",
                    schema["numbers"] => vec![3i64, 5],
                    schema["counter"] => 9u64,
                    schema["priority"] => 2u64,
                },
            )
            .unwrap();

        let update = match update {
            OperationOutcome::Applied(operation) => operation,
            OperationOutcome::NoChanges => panic!("expected an update operation"),
        };
        assert_matches!(
            update.operation,
            RowOperation::Update { row_id, ref fields } if row_id == 1 && fields.len() == 4
        );
    }

    {
        let row = data.get_row(&1).unwrap();

        let title_value: Cow<'_, str> = schema["title"].get_value(&row).unwrap();
        let number_values: Cow<'_, [i64]> = schema["numbers"].get_value(&row).unwrap();
        let counter_value: Cow<'_, u64> = schema["counter"].get_value(&row).unwrap();
        let priority_value: Cow<'_, u64> = schema["priority"].get_value(&row).unwrap();

        assert_eq!(title_value.as_ref(), "hello world");
        assert_eq!(number_values.as_ref(), &[3, 5]);
        assert_eq!(*counter_value, 9);
        assert_eq!(*priority_value, 2);
    }

    let delete = data.delete_row(13, 1).unwrap();
    assert_matches!(delete.operation, RowOperation::Delete { row_id } if row_id == 1);
    assert_eq!(data.len(), 1);
    assert!(!data.is_empty());
    assert_eq!(data.num_active_rows(), 0);
    assert!(data.get_row(&1).is_some());

    let deleted_update = data.modify_row(
        14,
        1,
        update_values! {
            schema["title"] => "ignored",
        },
    );
    assert_matches!(deleted_update, Err(OperationError::ModifyDeletedRow { .. }));
}

#[test]
fn modify_row_linear_list_emits_targeted_diff_operations() {
    let schema = &*TEST_SCHEMA;
    let mut data: InMemoryData<RowId, OperationId> = InMemoryData::with_static_schema(schema);

    data.insert_row(
        100,
        7,
        initial_values! {
            schema["title"] => "t",
            schema["numbers"] => vec![1i64, 2, 3, 4],
            schema["counter"] => 0u64,
            schema["priority"] => 0u64,
        },
    )
    .unwrap();

    let update = data
        .modify_row(
            101,
            7,
            update_values! {
                schema["numbers"] => vec![1i64, 9, 3, 4],
            },
        )
        .unwrap();
    let update = match update {
        OperationOutcome::Applied(operation) => operation,
        OperationOutcome::NoChanges => panic!("expected an update operation"),
    };

    let fields = match update.operation {
        RowOperation::Update { fields, .. } => fields,
        _ => panic!("expected row update operation"),
    };
    assert_eq!(fields.len(), 1);

    let numbers_field = fields
        .iter()
        .find(|field| field.field_name.as_ref() == "numbers")
        .expect("numbers field should be present");
    let operations = match &numbers_field.value {
        OperationValue::LinearList(operations) => operations,
        _ => panic!("numbers field should contain a linear list operation"),
    };

    assert_eq!(operations.len(), 2);
    let delete_count = operations
        .iter()
        .filter(|operation| matches!(operation, DataOperation::Delete { .. }))
        .count();
    assert_eq!(delete_count, 1);

    let inserted_chunks: Vec<Vec<i64>> = operations
        .iter()
        .filter_map(|operation| match operation {
            DataOperation::Insert { value, .. } => match value {
                PrimitiveValueArray::Int(values) => Some(values.clone()),
                _ => panic!("numbers field should encode int list insert payloads"),
            },
            DataOperation::Delete { .. } => None,
        })
        .collect();
    assert_eq!(inserted_chunks, vec![vec![9]]);
}

#[test]
fn insert_row_requires_explicit_values_for_all_fields() {
    let schema = &*TEST_SCHEMA;

    let mut data: InMemoryData<RowId, OperationId> = InMemoryData::with_static_schema(schema);
    let error = data
        .insert_row(
            20,
            2,
            initial_values! {
                schema["title"] => "draft",
                schema["numbers"] => vec![1i64],
                schema["counter"] => 0u64,
            },
        )
        .unwrap_err();

    assert_matches!(
        error,
        OperationError::SchemaValue {
            source: SchemaValueError::MissingField { ref field_name }
        } if field_name == "priority"
    );
}

#[test]
fn insert_row_uses_schema_defaults_for_omitted_fields() {
    let schema = Schema::from_fields([
        Field::linear_string("title")
            .with_default("untitled")
            .unwrap(),
        Field::linear_list("numbers", PrimitiveType::Int)
            .with_default(vec![7i64, 9])
            .unwrap(),
        Field::monotonic_counter("counter")
            .with_default(5u64)
            .unwrap(),
        Field::total_order_register("priority", PrimitiveType::UInt, Direction::Ascending)
            .with_default(11u64)
            .unwrap(),
    ]);
    let mut data: InMemoryData<RowId, OperationId> =
        InMemoryData::with_owned_schema(schema.clone());

    let operation = data
        .insert_row(
            30,
            4,
            initial_values! {
                schema["title"] => "provided",
            },
        )
        .unwrap()
        .into_owned();

    let row = data.get_row(&4).expect("row should exist");
    let title: Cow<'_, str> = schema["title"].get_value(&row).unwrap();
    let numbers: Cow<'_, [i64]> = schema["numbers"].get_value(&row).unwrap();
    let counter: Cow<'_, u64> = schema["counter"].get_value(&row).unwrap();
    let priority: Cow<'_, u64> = schema["priority"].get_value(&row).unwrap();
    assert_eq!(title.as_ref(), "provided");
    assert_eq!(numbers.as_ref(), &[7, 9]);
    assert_eq!(*counter, 5);
    assert_eq!(*priority, 11);

    let RowOperation::Insert { snapshot, .. } = operation.operation else {
        panic!("insert operation must carry a row snapshot");
    };
    assert_eq!(snapshot.into_owned_fields().len(), schema.columns.len());
}

#[test]
fn apply_insert_materializes_defaults_for_missing_fields() {
    let old_schema = Schema::from_fields([Field::linear_string("title")]);
    let new_schema = Schema::from_fields([
        Field::linear_string("title"),
        Field::linear_string("subtitle")
            .with_default("default subtitle")
            .unwrap(),
        Field::linear_list("numbers", PrimitiveType::Int)
            .with_default(vec![3i64, 4])
            .unwrap(),
    ]);

    let mut source: InMemoryData<RowId, OperationId> =
        InMemoryData::with_owned_schema(old_schema.clone());
    let insert = source
        .insert_row(
            300,
            12,
            initial_values! {
                old_schema["title"] => "legacy row",
            },
        )
        .unwrap()
        .into_owned();

    let target = InMemoryData::with_owned_schema(new_schema.clone())
        .apply_schema_operation(insert)
        .unwrap();

    let row = target.get_row(&12).expect("row should exist");
    let title: Cow<'_, str> = new_schema["title"].get_value(&row).unwrap();
    let subtitle: Cow<'_, str> = new_schema["subtitle"].get_value(&row).unwrap();
    let numbers: Cow<'_, [i64]> = new_schema["numbers"].get_value(&row).unwrap();
    assert_eq!(title.as_ref(), "legacy row");
    assert_eq!(subtitle.as_ref(), "default subtitle");
    assert_eq!(numbers.as_ref(), &[3, 4]);
}

#[test]
fn apply_schema_operation_roundtrips_insert_update_delete() {
    let schema = &*TEST_SCHEMA;
    let mut source: InMemoryData<RowId, OperationId> = InMemoryData::with_static_schema(schema);

    let insert = source
        .insert_row(
            200,
            9,
            initial_values! {
                schema["title"] => "hello",
                schema["numbers"] => vec![1i64, 2],
                schema["counter"] => 1u64,
                schema["priority"] => 3u64,
            },
        )
        .unwrap()
        .into_owned();
    let update = expect_applied(
        source
            .modify_row(
                201,
                9,
                update_values! {
                    schema["title"] => "hello world",
                    schema["numbers"] => vec![5i64, 8],
                    schema["counter"] => 9u64,
                    schema["priority"] => 10u64,
                },
            )
            .unwrap(),
    )
    .into_owned();
    let delete = source.delete_row(202, 9).unwrap().into_owned();

    let target = InMemoryData::with_static_schema(schema)
        .apply_schema_operation(insert)
        .unwrap()
        .apply_schema_operation(update)
        .unwrap()
        .apply_schema_operation(delete)
        .unwrap();

    assert_eq!(target, source);
}

#[test]
fn apply_schema_operation_rejects_duplicate_insert_and_unknown_rows() {
    let schema = &*TEST_SCHEMA;
    let mut source: InMemoryData<RowId, OperationId> = InMemoryData::with_static_schema(schema);
    let insert = source
        .insert_row(
            210,
            5,
            initial_values! {
                schema["title"] => "value",
                schema["numbers"] => vec![1i64],
                schema["counter"] => 0u64,
                schema["priority"] => 0u64,
            },
        )
        .unwrap()
        .into_owned();
    let update = expect_applied(
        source
            .modify_row(
                211,
                5,
                update_values! {
                    schema["priority"] => 7u64,
                },
            )
            .unwrap(),
    )
    .into_owned();
    let delete = source.delete_row(212, 5).unwrap().into_owned();

    let empty_target: InMemoryData<RowId, OperationId> = InMemoryData::with_static_schema(schema);
    let update_err = empty_target.clone().apply_schema_operation(update.clone());
    assert_matches!(update_err, Err(OperationError::UnknownRowId { .. }));

    let delete_err = empty_target.apply_schema_operation(delete.clone());
    assert_matches!(delete_err, Err(OperationError::UnknownRowId { .. }));

    let applied_once = InMemoryData::with_static_schema(schema)
        .apply_schema_operation(insert.clone())
        .unwrap();
    let duplicate_insert = applied_once.apply_schema_operation(insert);
    assert_matches!(duplicate_insert, Err(OperationError::DuplicateRowId { .. }));
}

#[test]
fn apply_schema_operation_update_on_deleted_row_applies_and_repeated_delete_is_ok() {
    let schema = &*TEST_SCHEMA;
    let mut source: InMemoryData<RowId, OperationId> = InMemoryData::with_static_schema(schema);
    let insert = source
        .insert_row(
            220,
            8,
            initial_values! {
                schema["title"] => "draft",
                schema["numbers"] => vec![1i64, 2],
                schema["counter"] => 0u64,
                schema["priority"] => 1u64,
            },
        )
        .unwrap()
        .into_owned();
    let update = expect_applied(
        source
            .modify_row(
                221,
                8,
                update_values! {
                    schema["priority"] => 42u64,
                },
            )
            .unwrap(),
    )
    .into_owned();
    let delete = source.delete_row(222, 8).unwrap().into_owned();

    let target = InMemoryData::with_static_schema(schema)
        .apply_schema_operation(insert)
        .unwrap()
        .apply_schema_operation(delete.clone())
        .unwrap()
        .apply_schema_operation(update)
        .unwrap();

    assert_eq!(target.num_active_rows(), 0);
    let row = target.get_row(&8).expect("row should still be addressable");
    let priority_value: Cow<'_, u64> = schema["priority"].get_value(&row).unwrap();
    assert_eq!(*priority_value, 42);

    let before_repeat_delete = target.clone();
    let after_repeat_delete = target.apply_schema_operation(delete).unwrap();
    assert_eq!(after_repeat_delete, before_repeat_delete);
}

#[test]
fn apply_schema_operation_schema_validation_rejects_unknown_fields() {
    let schema = &*TEST_SCHEMA;
    let mut source: InMemoryData<RowId, OperationId> = InMemoryData::with_static_schema(schema);
    let insert = source
        .insert_row(
            230,
            3,
            initial_values! {
                schema["title"] => "v",
                schema["numbers"] => vec![9i64],
                schema["counter"] => 0u64,
                schema["priority"] => 1u64,
            },
        )
        .unwrap()
        .into_owned();
    let mut update = expect_applied(
        source
            .modify_row(
                231,
                3,
                update_values! {
                    schema["priority"] => 4u64,
                },
            )
            .unwrap(),
    )
    .into_owned();
    let RowOperation::Update { fields, .. } = &mut update.operation else {
        panic!("expected update operation");
    };
    fields[0].field_name = Cow::Borrowed("unknown_field");

    let target = InMemoryData::with_static_schema(schema)
        .apply_schema_operation(insert)
        .unwrap();
    let err = target.apply_schema_operation(update);
    assert_matches!(
        err,
        Err(OperationError::SchemaValue {
            source: SchemaValueError::UnknownField { .. }
        })
    );
}

#[test]
fn apply_schema_operation_enforces_causal_order_for_linear_string_updates() {
    let schema = &*TEST_SCHEMA;
    let mut source: InMemoryData<RowId, OperationId> = InMemoryData::with_static_schema(schema);
    let insert = source
        .insert_row(
            240,
            6,
            initial_values! {
                schema["title"] => "a",
                schema["numbers"] => vec![0i64],
                schema["counter"] => 0u64,
                schema["priority"] => 0u64,
            },
        )
        .unwrap()
        .into_owned();
    let update_1 = expect_applied(
        source
            .modify_row(
                241,
                6,
                update_values! {
                    schema["title"] => "ab",
                },
            )
            .unwrap(),
    )
    .into_owned();
    let update_2 = expect_applied(
        source
            .modify_row(
                242,
                6,
                update_values! {
                    schema["title"] => "abc",
                },
            )
            .unwrap(),
    )
    .into_owned();

    let target = InMemoryData::with_static_schema(schema)
        .apply_schema_operation(insert)
        .unwrap();

    let before_out_of_order = target.clone();
    let out_of_order = target.clone().apply_schema_operation(update_2.clone());
    assert_matches!(out_of_order, Err(OperationError::InternalOperation { .. }));
    assert_eq!(target, before_out_of_order);

    let converged = target
        .apply_schema_operation(update_1)
        .unwrap()
        .apply_schema_operation(update_2)
        .unwrap();
    let row = converged.get_row(&6).unwrap();
    let title_value: Cow<'_, str> = schema["title"].get_value(&row).unwrap();
    assert_eq!(title_value.as_ref(), "abc");
}

#[test]
fn apply_schema_operation_duplicate_updates_can_succeed_or_fail_by_field_type() {
    let schema = &*TEST_SCHEMA;

    let mut register_source: InMemoryData<RowId, OperationId> =
        InMemoryData::with_static_schema(schema);
    let register_insert = register_source
        .insert_row(
            250,
            11,
            initial_values! {
                schema["title"] => "x",
                schema["numbers"] => vec![1i64],
                schema["counter"] => 0u64,
                schema["priority"] => 1u64,
            },
        )
        .unwrap()
        .into_owned();
    let register_update = expect_applied(
        register_source
            .modify_row(
                251,
                11,
                update_values! {
                    schema["priority"] => 9u64,
                },
            )
            .unwrap(),
    )
    .into_owned();

    let register_target = InMemoryData::with_static_schema(schema)
        .apply_schema_operation(register_insert)
        .unwrap()
        .apply_schema_operation(register_update.clone())
        .unwrap()
        .apply_schema_operation(register_update)
        .unwrap();
    let register_row = register_target.get_row(&11).unwrap();
    let register_value: Cow<'_, u64> = schema["priority"].get_value(&register_row).unwrap();
    assert_eq!(*register_value, 9);

    let mut string_source: InMemoryData<RowId, OperationId> =
        InMemoryData::with_static_schema(schema);
    let string_insert = string_source
        .insert_row(
            260,
            12,
            initial_values! {
                schema["title"] => "a",
                schema["numbers"] => vec![1i64],
                schema["counter"] => 0u64,
                schema["priority"] => 0u64,
            },
        )
        .unwrap()
        .into_owned();
    let string_update = expect_applied(
        string_source
            .modify_row(
                261,
                12,
                update_values! {
                    schema["title"] => "ab",
                },
            )
            .unwrap(),
    )
    .into_owned();

    let string_target = InMemoryData::with_static_schema(schema)
        .apply_schema_operation(string_insert)
        .unwrap()
        .apply_schema_operation(string_update.clone())
        .unwrap();
    let retry = string_target.clone().apply_schema_operation(string_update);
    assert_matches!(retry, Err(OperationError::InternalOperation { .. }));
    assert_eq!(string_target.num_active_rows(), 1);
}
