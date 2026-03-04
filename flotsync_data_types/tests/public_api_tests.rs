use flotsync_data_types::{
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
        datamodel::{InMemoryData, RowOperation, SchemaValueError},
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
