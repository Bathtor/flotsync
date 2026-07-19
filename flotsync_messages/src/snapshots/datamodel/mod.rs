use crate::{
    codecs::datamodel::{
        CodecError,
        ColumnarHistoryCodecError,
        StateSnapshotWireValue,
        UpdateIdWithIndex,
        decode_columnar_latest_value_wins_history_snapshot,
        decode_columnar_linear_list_history_snapshot,
        decode_columnar_linear_string_history_snapshot,
        decode_state_snapshot_value,
        encode_columnar_latest_value_wins_history_snapshot,
        encode_columnar_linear_list_history_snapshot,
        encode_columnar_linear_string_history_snapshot,
        encode_state_snapshot_value,
    },
    datamodel as proto,
};
use flotsync_core::versions::UpdateId;
use flotsync_data_types::{
    schema::{
        NullableBasicDataType,
        PrimitiveType,
        ReplicatedDataType,
        Schema,
        datamodel::{
            NullableBasicValue,
            NullableBasicValueRef,
            PrimitiveValueArrayRef,
            SchemaSnapshotDecoder,
            SchemaSnapshotEncoder,
            SnapshotNodeSource,
            StateSnapshotFieldValue,
            StateSnapshotFieldValueRef,
        },
        values::PrimitiveValueArray,
    },
    snapshot::{SnapshotHeader, SnapshotNode, SnapshotNodeRef, SnapshotSink},
};
use snafu::prelude::*;
use std::collections::{HashMap, HashSet, hash_map::Entry};

mod data;
mod schema;

pub use data::*;
pub use schema::*;

/// Errors raised while adapting protobuf snapshot messages to the schema snapshot APIs.
#[derive(Debug, Snafu)]
pub enum SnapshotAdapterError {
    #[snafu(display("Snapshot payload codec failed."))]
    Codec { source: CodecError },
    #[snafu(display("History snapshot field codec failed."))]
    HistoryCodec { source: ColumnarHistoryCodecError },
    #[snafu(display("Field '{field_name}' does not exist in the schema."))]
    UnknownSchemaField { field_name: String },
    #[snafu(display(
        "Field '{field_name}' has an unexpected schema data type; expected {expected}."
    ))]
    UnexpectedSchemaDataType {
        field_name: String,
        expected: &'static str,
    },
    #[snafu(display("Snapshot row contains duplicate field '{field_name}'."))]
    DuplicateField { field_name: String },
    #[snafu(display("Snapshot row is missing field '{field_name}'."))]
    MissingField { field_name: String },
    #[snafu(display("Snapshot field '{field_name}' is missing its value oneof."))]
    MissingFieldValue { field_name: String },
    #[snafu(display(
        "Snapshot field '{field_name}' has wrong value kind; expected {expected}, got {actual}."
    ))]
    UnexpectedFieldKind {
        field_name: String,
        expected: &'static str,
        actual: &'static str,
    },
    #[snafu(display("Snapshot row expected {expected} fields but observed {actual}."))]
    FieldCountMismatch { expected: usize, actual: usize },
    #[snafu(display("Snapshot row still has unconsumed fields at end: {field_names:?}."))]
    RemainingFields { field_names: Vec<String> },
    #[snafu(display("Snapshot stream expected node index {expected} but got {actual}."))]
    UnexpectedNodeIndex { expected: usize, actual: usize },
    #[snafu(display("Snapshot stream declared {expected} nodes but emitted {actual}."))]
    NodeCountMismatch { expected: usize, actual: usize },
    #[snafu(display("History field sink was already ended."))]
    HistoryFieldClosed,
    #[snafu(display("{target} must be begun before fields, rows, or nodes can be processed."))]
    BeginRequired { target: &'static str },
    #[snafu(display("{target} was begun more than once."))]
    AlreadyBegun { target: &'static str },
    #[snafu(display("A dataset row is already open."))]
    RowAlreadyOpen,
    #[snafu(display("No dataset row is currently open."))]
    NoOpenRow,
    #[snafu(display("Row {row_index} is out of bounds for a dataset with {row_count} rows."))]
    RowOutOfBounds { row_index: usize, row_count: usize },
    #[snafu(display("Ending row {row_index}, but row {open_row} is currently open."))]
    WrongOpenRow { row_index: usize, open_row: usize },
    #[snafu(display("Row {row_index} has already been taken for decoding."))]
    RowAlreadyConsumed { row_index: usize },
    #[snafu(display("Dataset decoding ended with rows that were never decoded: {row_indices:?}."))]
    RemainingRows { row_indices: Vec<usize> },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffa::Message;
    use flotsync_data_types::{
        NULL,
        any_data::{LinearLatestValueWins, list::LinearList},
        schema::{
            BasicDataType,
            Direction,
            Field,
            NullableBasicDataType,
            datamodel::{
                DataSnapshotDecoder,
                InMemoryFieldState,
                InMemoryStateData,
                LinearLatestValueWinsState,
                LinearListState,
            },
            values::{NullablePrimitiveValue, PrimitiveValue},
        },
        text::LinearString,
    };
    use std::{assert_matches, sync::LazyLock};

    fn update_id(version: u64, node_index: u32) -> UpdateId {
        UpdateId {
            version,
            node_index,
        }
    }

    fn indexed(version: u64, node_index: u32, chunk_index: u32) -> UpdateIdWithIndex {
        UpdateIdWithIndex {
            id: update_id(version, node_index),
            index: chunk_index,
        }
    }

    fn state_field(name: &str, value: proto::snapshot_field::Value) -> proto::SnapshotField {
        proto::SnapshotField {
            field_name: name.to_owned(),
            value: Some(value),
            ..proto::SnapshotField::default()
        }
    }

    static TEST_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
        Schema::from_fields([
            Field::latest_value_wins(
                "latest",
                NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::UInt)),
            ),
            Field::linear_string("title"),
            Field::linear_list("numbers", PrimitiveType::Int),
            Field::monotonic_counter("counter"),
            Field::total_order_register("priority", PrimitiveType::UInt, Direction::Ascending),
            Field::finite_state_register("status", ["draft".into(), NULL, "published".into()])
                .unwrap(),
        ])
    });

    #[test]
    fn dataset_roundtrips_via_protobuf_snapshot_messages() {
        let schema = &*TEST_SCHEMA;

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
        let status = NullablePrimitiveValue::Value(PrimitiveValue::String("published".to_owned()));

        let mut data: InMemoryStateData<(), UpdateId> =
            InMemoryStateData::with_static_schema(schema);
        data.push_row_from_named_fields([
            (
                "latest",
                InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableUInt(
                    latest.clone(),
                )),
            ),
            ("title", InMemoryFieldState::LinearString(title.clone())),
            (
                "numbers",
                InMemoryFieldState::LinearList(LinearListState::Int(numbers.clone())),
            ),
            ("counter", InMemoryFieldState::MonotonicCounter(counter)),
            (
                "priority",
                InMemoryFieldState::TotalOrderRegister(priority.clone()),
            ),
            (
                "status",
                InMemoryFieldState::TotalOrderFiniteStateRegister(status.clone()),
            ),
        ])
        .unwrap();

        let mut encoder = ProtoDataSnapshotEncoder::new(schema);
        data.encode_data_snapshots(&mut encoder).unwrap();
        let snapshot = encoder.into_snapshot().unwrap();
        let bytes = snapshot.encode_to_vec();
        let snapshot = proto::DataSnapshot::decode_from_slice(&bytes).unwrap();

        let mut decoder = ProtoDataSnapshotDecoder::new(snapshot);
        let roundtrip = InMemoryStateData::decode_data_snapshots(schema, &mut decoder).unwrap();
        assert_eq!(roundtrip, data);
    }

    #[test]
    fn schema_decoder_rejects_duplicate_fields() {
        let row = proto::RowSnapshot {
            fields: vec![
                state_field(
                    "counter",
                    proto::snapshot_field::Value::MonotonicCounter(Box::new(
                        crate::codecs::datamodel::encode_counter_value(
                            flotsync_data_types::schema::datamodel::CounterValueRef::UInt(1),
                        ),
                    )),
                ),
                state_field(
                    "counter",
                    proto::snapshot_field::Value::MonotonicCounter(Box::new(
                        crate::codecs::datamodel::encode_counter_value(
                            flotsync_data_types::schema::datamodel::CounterValueRef::UInt(2),
                        ),
                    )),
                ),
            ],
            ..proto::RowSnapshot::default()
        };

        let err = ProtoSchemaSnapshotDecoder::new(row).unwrap_err();
        assert_matches!(
            err,
            SnapshotAdapterError::DuplicateField { field_name } if field_name == "counter"
        );
    }

    #[test]
    fn schema_decoder_rejects_wrong_field_variant() {
        let row = proto::RowSnapshot {
            fields: vec![state_field(
                "counter",
                proto::snapshot_field::Value::LinearString(Box::default()),
            )],
            ..proto::RowSnapshot::default()
        };
        let mut decoder = ProtoSchemaSnapshotDecoder::new(row).unwrap();
        decoder.begin(1).unwrap();

        let err = decoder
            .decode_state_field(
                "counter",
                &ReplicatedDataType::MonotonicCounter { small_range: false },
            )
            .unwrap_err();
        assert_matches!(
            err,
            SnapshotAdapterError::UnexpectedFieldKind {
                field_name,
                expected,
                actual,
            } if field_name == "counter"
                && expected == "monotonic_counter"
                && actual == "linear_string"
        );
    }

    #[test]
    fn schema_decoder_reports_missing_field() {
        let row = proto::RowSnapshot {
            fields: vec![
                state_field(
                    "counter",
                    proto::snapshot_field::Value::MonotonicCounter(Box::new(
                        crate::codecs::datamodel::encode_counter_value(
                            flotsync_data_types::schema::datamodel::CounterValueRef::UInt(1),
                        ),
                    )),
                ),
                state_field(
                    "extra",
                    proto::snapshot_field::Value::TotalOrderRegister(Box::new(
                        crate::codecs::datamodel::encode_primitive_value(
                            flotsync_data_types::schema::values::PrimitiveValueRef::UInt(7),
                        ),
                    )),
                ),
            ],
            ..proto::RowSnapshot::default()
        };
        let mut decoder = ProtoSchemaSnapshotDecoder::new(row).unwrap();
        decoder.begin(2).unwrap();
        let _ = decoder
            .decode_state_field(
                "counter",
                &ReplicatedDataType::MonotonicCounter { small_range: false },
            )
            .unwrap();

        let err = decoder
            .decode_state_field(
                "priority",
                &ReplicatedDataType::TotalOrderRegister {
                    value_type: PrimitiveType::UInt,
                    direction: Direction::Ascending,
                },
            )
            .unwrap_err();
        assert_matches!(
            err,
            SnapshotAdapterError::MissingField { field_name } if field_name == "priority"
        );
    }

    #[test]
    fn schema_decoder_rejects_unconsumed_extra_fields_on_end() {
        let row = proto::RowSnapshot {
            fields: vec![
                state_field(
                    "counter",
                    proto::snapshot_field::Value::MonotonicCounter(Box::new(
                        crate::codecs::datamodel::encode_counter_value(
                            flotsync_data_types::schema::datamodel::CounterValueRef::UInt(1),
                        ),
                    )),
                ),
                state_field(
                    "extra",
                    proto::snapshot_field::Value::TotalOrderRegister(Box::new(
                        crate::codecs::datamodel::encode_primitive_value(
                            flotsync_data_types::schema::values::PrimitiveValueRef::UInt(7),
                        ),
                    )),
                ),
            ],
            ..proto::RowSnapshot::default()
        };
        let mut decoder = ProtoSchemaSnapshotDecoder::new(row).unwrap();
        decoder.begin(2).unwrap();
        let _ = decoder
            .decode_state_field(
                "counter",
                &ReplicatedDataType::MonotonicCounter { small_range: false },
            )
            .unwrap();

        let err = decoder.end().unwrap_err();
        assert_matches!(
            err,
            SnapshotAdapterError::RemainingFields { field_names }
                if field_names == vec!["extra".to_owned()]
        );
    }

    #[test]
    fn dataset_decoder_rejects_remaining_rows() {
        let snapshot = proto::DataSnapshot {
            rows: vec![proto::RowSnapshot::default()],
            ..proto::DataSnapshot::default()
        };
        let mut decoder = ProtoDataSnapshotDecoder::new(snapshot);
        let row_count = decoder.begin().unwrap();
        assert_eq!(row_count, 1);
        let err = decoder.end().unwrap_err();
        assert_matches!(
            err,
            SnapshotAdapterError::RemainingRows { row_indices } if row_indices == vec![0]
        );
    }
}
