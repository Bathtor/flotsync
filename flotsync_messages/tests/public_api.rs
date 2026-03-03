use flotsync_core::versions::UpdateId;
use flotsync_data_types::{
    IdWithIndex,
    any_data::{LinearLatestValueWins, list::LinearList},
    schema::{
        BasicDataType,
        Direction,
        Field,
        NullableBasicDataType,
        NullablePrimitiveType,
        PrimitiveType,
        ReplicatedDataType,
        Schema,
        datamodel::{
            InMemoryData,
            InMemoryFieldValue,
            LinearLatestValueWinsValue,
            LinearListValue,
        },
        values::{NullablePrimitiveValue, NullablePrimitiveValueArray, PrimitiveValue},
    },
    test_support::schema_operations::{
        exhaustive_schema,
        exhaustive_schema_operation,
        exhaustive_schema_operations,
    },
    text::LinearString,
};
use flotsync_messages::{
    codecs::datamodel::{decode_schema_operation, encode_schema_operation},
    datamodel as proto,
    protobuf::Message,
    snapshots::datamodel::{ProtoDataSnapshotDecoder, ProtoDataSnapshotEncoder},
};
use std::{borrow::Cow, collections::HashMap, sync::LazyLock};

fn update_id(version: u64, node_index: u32) -> UpdateId {
    UpdateId {
        version,
        node_index,
    }
}

type TestUnused = ();

fn indexed(version: u64, node_index: u32, chunk_index: u32) -> IdWithIndex<UpdateId> {
    IdWithIndex {
        id: update_id(version, node_index),
        index: chunk_index,
    }
}

fn operation_ids() -> impl Iterator<Item = UpdateId> {
    (1u64..).map(|version| UpdateId {
        version,
        node_index: (version % 7) as u32,
    })
}

static PUBLIC_TEST_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    let mut columns = HashMap::new();
    columns.insert(
        "latest".to_owned(),
        Field {
            name: "latest".to_owned(),
            data_type: ReplicatedDataType::LatestValueWins {
                value_type: NullableBasicDataType::Nullable(BasicDataType::Primitive(
                    PrimitiveType::UInt,
                )),
            },
            metadata: HashMap::new(),
        },
    );
    columns.insert(
        "title".to_owned(),
        Field {
            name: "title".to_owned(),
            data_type: ReplicatedDataType::LinearString,
            metadata: HashMap::new(),
        },
    );
    columns.insert(
        "numbers".to_owned(),
        Field {
            name: "numbers".to_owned(),
            data_type: ReplicatedDataType::LinearList {
                value_type: PrimitiveType::Int,
            },
            metadata: HashMap::new(),
        },
    );
    columns.insert(
        "counter".to_owned(),
        Field {
            name: "counter".to_owned(),
            data_type: ReplicatedDataType::MonotonicCounter { small_range: false },
            metadata: HashMap::new(),
        },
    );
    columns.insert(
        "priority".to_owned(),
        Field {
            name: "priority".to_owned(),
            data_type: ReplicatedDataType::TotalOrderRegister {
                value_type: PrimitiveType::UInt,
                direction: Direction::Ascending,
            },
            metadata: HashMap::new(),
        },
    );
    columns.insert(
        "status".to_owned(),
        Field {
            name: "status".to_owned(),
            data_type: ReplicatedDataType::TotalOrderFiniteStateRegister {
                value_type: NullablePrimitiveType::Nullable(PrimitiveType::String),
                states: NullablePrimitiveValueArray::Nullable {
                    values: flotsync_data_types::schema::values::PrimitiveValueArray::String(vec![
                        "draft".to_owned(),
                        "published".to_owned(),
                    ]),
                    null_index: 1,
                },
            },
            metadata: HashMap::new(),
        },
    );
    Schema {
        columns,
        metadata: HashMap::new(),
    }
});

#[test]
fn public_snapshot_transport_roundtrips_dataset() {
    let schema = &*PUBLIC_TEST_SCHEMA;

    let mut latest = LinearLatestValueWins::new(
        Some(1u64),
        [update_id(1, 0), update_id(1, 1), update_id(1, 2)],
    );
    latest.update(update_id(2, 0), None);
    latest.update(update_id(3, 0), Some(99));

    let mut title = LinearString::with_value("alpha".to_owned(), update_id(10, 0));
    title.append(indexed(11, 0, 0), " beta".to_owned());
    let title_range = title.ids_in_range(1..=2).unwrap();
    title_range.delete(&mut title).unwrap();

    let mut numbers = LinearList::with_values([10i64, 20, 30], update_id(20, 0));
    numbers.append(indexed(21, 0, 0), [40, 50]);
    let _ = numbers.delete_at(1);

    let counter = flotsync_data_types::schema::datamodel::CounterValue::UInt(12);
    let priority = PrimitiveValue::UInt(7);
    let status = NullablePrimitiveValue::Value(PrimitiveValue::String("published".to_owned()));

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
fn public_operation_transport_roundtrips_exhaustive_examples() {
    let schema = exhaustive_schema();
    let operations = exhaustive_schema_operations(operation_ids()).unwrap();

    for operation in operations {
        let encoded = encode_schema_operation(&operation).unwrap();
        let bytes = encoded.write_to_bytes().unwrap();
        let encoded = proto::SchemaOperation::parse_from_bytes(&bytes).unwrap();
        let decoded = decode_schema_operation(encoded, &schema).unwrap();
        assert_eq!(decoded, operation);
    }
}

#[test]
fn public_operation_transport_roundtrips_exhaustive_multi_field_example() {
    let schema = exhaustive_schema();
    let operation = exhaustive_schema_operation(operation_ids()).unwrap();

    let encoded = encode_schema_operation(&operation).unwrap();
    let bytes = encoded.write_to_bytes().unwrap();
    let encoded = proto::SchemaOperation::parse_from_bytes(&bytes).unwrap();
    let decoded = decode_schema_operation(encoded, &schema).unwrap();

    assert_eq!(decoded, operation);
}
