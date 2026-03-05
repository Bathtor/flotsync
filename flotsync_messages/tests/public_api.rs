use flotsync_core::versions::UpdateId;
use flotsync_data_types::{
    FieldOperations,
    IdWithIndex,
    NULL,
    OperationOutcome,
    TableOperations,
    any_data::{LinearLatestValueWins, list::LinearList},
    initial_values,
    schema::{
        BasicDataType,
        Direction,
        Field,
        NullableBasicDataType,
        PrimitiveType,
        Schema,
        datamodel::{InMemoryFieldValue, LinearLatestValueWinsValue, LinearListValue},
        values::{NullablePrimitiveValue, PrimitiveValue},
    },
    test_support::schema_operations::{
        exhaustive_schema,
        exhaustive_schema_operation,
        exhaustive_schema_operations,
    },
    text::LinearString,
    update_values,
};
use flotsync_messages::{
    InMemoryData,
    Uuid,
    codecs::{
        datamodel::{decode_schema_operation, encode_schema_operation},
        schema::{decode_schema_definition, encode_schema_definition},
    },
    datamodel as proto,
    protobuf::Message,
    snapshots::datamodel::{ProtoDataSnapshotDecoder, ProtoDataSnapshotEncoder},
};
use std::{borrow::Cow, sync::LazyLock};

fn update_id(version: u64, node_index: u32) -> UpdateId {
    UpdateId {
        version,
        node_index,
    }
}

fn indexed(version: u64, node_index: u32, chunk_index: u32) -> IdWithIndex<UpdateId> {
    IdWithIndex {
        id: update_id(version, node_index),
        index: chunk_index,
    }
}

fn row_id(value: u128) -> Uuid {
    Uuid::from_u128(value)
}

fn row_ids() -> impl Iterator<Item = Uuid> {
    (1u128..).map(row_id)
}

fn operation_ids() -> impl Iterator<Item = UpdateId> {
    (1u64..).map(|version| UpdateId {
        version,
        node_index: (version % 7) as u32,
    })
}

static PUBLIC_TEST_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    Schema::from_fields([
        Field::latest_value_wins(
            "latest",
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::UInt)),
        ),
        Field::linear_string("title"),
        Field::linear_list("numbers", PrimitiveType::Int),
        Field::monotonic_counter("counter"),
        Field::total_order_register("priority", PrimitiveType::UInt, Direction::Ascending),
        Field::finite_state_register("status", ["draft".into(), NULL, "published".into()]).unwrap(),
    ])
});

#[test]
fn public_snapshot_transport_roundtrips_dataset() {
    let schema = &*PUBLIC_TEST_SCHEMA;

    let mut latest = LinearLatestValueWins::new(
        Some(1u64),
        [indexed(1, 0, 0), indexed(1, 1, 0), indexed(1, 2, 0)],
    );
    latest.update(indexed(2, 0, 0), None);
    latest.update(indexed(3, 0, 0), Some(99));

    let mut title = LinearString::with_value("alpha".to_owned(), update_id(10, 0));
    title.append(indexed(11, 0, 0), " beta".to_owned());
    let title_range = title.ids_in_range(1..=2).unwrap();
    title_range.delete(&mut title).unwrap();

    let mut numbers = LinearList::with_values([10i64, 20, 30], update_id(20, 0));
    numbers.append(indexed(21, 0, 0), [40, 50]);
    let _ = numbers.delete_at(1);

    let counter = flotsync_data_types::schema::datamodel::CounterValue::UInt(12);
    let priority = PrimitiveValue::UInt(7);
    let status = NullablePrimitiveValue::Null;

    let mut data = InMemoryData::with_owned_schema(schema.clone());
    data.push_row_from_named_fields([
        (
            "latest",
            InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableUInt(latest)),
        ),
        ("title", InMemoryFieldValue::LinearString(title)),
        (
            "numbers",
            InMemoryFieldValue::LinearList(LinearListValue::Int(numbers)),
        ),
        ("counter", InMemoryFieldValue::MonotonicCounter(counter)),
        (
            "priority",
            InMemoryFieldValue::TotalOrderRegister(priority.clone()),
        ),
        (
            "status",
            InMemoryFieldValue::TotalOrderFiniteStateRegister(status.clone()),
        ),
    ])
    .unwrap();

    let mut encoder = ProtoDataSnapshotEncoder::new(schema);
    data.encode_data_snapshots(&mut encoder).unwrap();
    let snapshot = encoder.into_snapshot().unwrap();
    let bytes = snapshot.write_to_bytes().unwrap();
    let snapshot = proto::DataSnapshot::parse_from_bytes(&bytes).unwrap();

    let mut decoder = ProtoDataSnapshotDecoder::new(snapshot);
    let roundtrip =
        InMemoryData::decode_data_snapshots(Cow::Borrowed(schema), &mut decoder).unwrap();

    assert_eq!(roundtrip, data);
}

#[test]
fn public_schema_transport_roundtrips_exhaustive_schema() {
    let mut schema = exhaustive_schema();
    schema
        .metadata
        .insert("schema-meta".to_owned(), "present".to_owned());
    schema
        .columns
        .get_mut("linear_string")
        .expect("exhaustive schema must contain linear_string")
        .metadata
        .insert("field-meta".to_owned(), "present".to_owned());

    let encoded = encode_schema_definition(&schema).unwrap();
    let bytes = encoded.write_to_bytes().unwrap();
    let encoded = proto::SchemaDefinition::parse_from_bytes(&bytes).unwrap();
    let decoded = decode_schema_definition(encoded).unwrap();

    assert_eq!(decoded, schema);
}

#[test]
fn public_operation_transport_roundtrips_exhaustive_examples() {
    let schema = exhaustive_schema();
    let operations = exhaustive_schema_operations(row_ids(), operation_ids()).unwrap();

    for operation in operations {
        let encoded = encode_schema_operation(&operation, &schema).unwrap();
        let bytes = encoded.write_to_bytes().unwrap();
        let encoded = proto::SchemaOperation::parse_from_bytes(&bytes).unwrap();
        let decoded = decode_schema_operation(encoded, &schema).unwrap();
        assert_eq!(decoded, operation);
    }
}

#[test]
fn public_operation_transport_roundtrips_exhaustive_multi_field_example() {
    let schema = exhaustive_schema();
    let operation = exhaustive_schema_operation(row_id(999), operation_ids()).unwrap();

    let encoded = encode_schema_operation(&operation, &schema).unwrap();
    let bytes = encoded.write_to_bytes().unwrap();
    let encoded = proto::SchemaOperation::parse_from_bytes(&bytes).unwrap();
    let decoded = decode_schema_operation(encoded, &schema).unwrap();

    assert_eq!(decoded, operation);
}

#[test]
fn public_operation_transport_decodes_and_applies_to_in_memory_data() {
    let schema = &*PUBLIC_TEST_SCHEMA;
    let row_id = row_id(4242);

    let mut source = InMemoryData::with_owned_schema(schema.clone());
    let decoded_insert = {
        let insert = source
            .insert_row(
                update_id(100, 0),
                row_id,
                initial_values! {
                    schema["latest"] => 1u64,
                    schema["title"] => "draft",
                    schema["numbers"] => vec![1i64, 2, 3],
                    schema["counter"] => 0u64,
                    schema["priority"] => 1u64,
                    schema["status"] => "draft",
                },
            )
            .unwrap();
        let encoded = encode_schema_operation(&insert, schema).unwrap();
        let bytes = encoded.write_to_bytes().unwrap();
        let encoded = proto::SchemaOperation::parse_from_bytes(&bytes).unwrap();
        decode_schema_operation(encoded, schema).unwrap()
    };
    let decoded_update = match source
        .modify_row(
            update_id(101, 0),
            row_id,
            update_values! {
                schema["priority"] => 9u64,
            },
        )
        .unwrap()
    {
        OperationOutcome::Applied(operation) => {
            let encoded = encode_schema_operation(&operation, schema).unwrap();
            let bytes = encoded.write_to_bytes().unwrap();
            let encoded = proto::SchemaOperation::parse_from_bytes(&bytes).unwrap();
            decode_schema_operation(encoded, schema).unwrap()
        }
        OperationOutcome::NoChanges => panic!("expected an update operation"),
    };
    let decoded_delete = {
        let delete = source.delete_row(update_id(102, 0), row_id).unwrap();
        let encoded = encode_schema_operation(&delete, schema).unwrap();
        let bytes = encoded.write_to_bytes().unwrap();
        let encoded = proto::SchemaOperation::parse_from_bytes(&bytes).unwrap();
        decode_schema_operation(encoded, schema).unwrap()
    };

    let target = InMemoryData::with_owned_schema(schema.clone())
        .apply_schema_operation(decoded_insert)
        .unwrap()
        .apply_schema_operation(decoded_delete)
        .unwrap()
        .apply_schema_operation(decoded_update)
        .unwrap();

    assert_eq!(target.num_active_rows(), 0);
    let row = target
        .get_row(&row_id)
        .expect("row should remain addressable");
    let priority: Cow<'_, u64> = schema["priority"].get_value(&row).unwrap();
    assert_eq!(*priority, 9);
    assert_eq!(target, source);
}
