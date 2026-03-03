use crate::{
    codecs::datamodel::{
        CodecError,
        ColumnarHistoryCodecError,
        StateSnapshotWireValue,
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
    IdWithIndex,
    schema::{
        NullableBasicDataType,
        PrimitiveType,
        ReplicatedDataType,
        Schema,
        datamodel::{
            SchemaSnapshotDecoder,
            SchemaSnapshotEncoder,
            SnapshotNodeSource,
            SnapshotStateValue,
            SnapshotStateValueRef,
        },
        values::PrimitiveValueArray,
    },
    snapshot::{SnapshotHeader, SnapshotNode, SnapshotNodeRef, SnapshotSink},
};
use snafu::prelude::*;
use std::collections::{HashMap, HashSet, hash_map::Entry};

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

/// Protobuf-backed implementation of `SchemaSnapshotEncoder` for one row snapshot.
pub struct ProtoSchemaSnapshotEncoder<'schema> {
    schema: &'schema Schema,
    row: proto::RowSnapshot,
    expected_field_count: Option<usize>,
    seen_fields: HashSet<&'schema str>,
}

impl<'schema> ProtoSchemaSnapshotEncoder<'schema> {
    /// Create an empty row encoder bound to `schema`.
    pub fn new(schema: &'schema Schema) -> Self {
        Self {
            schema,
            row: proto::RowSnapshot::new(),
            expected_field_count: None,
            seen_fields: HashSet::new(),
        }
    }

    /// Finish encoding and return the owned protobuf row snapshot.
    pub fn into_row_snapshot(self) -> Result<proto::RowSnapshot, SnapshotAdapterError> {
        self.validate_finished()?;
        Ok(self.row)
    }

    fn ensure_begun(&self) -> Result<(), SnapshotAdapterError> {
        ensure!(
            self.expected_field_count.is_some(),
            BeginRequiredSnafu {
                target: "schema snapshot encoder",
            }
        );
        Ok(())
    }

    fn validate_finished(&self) -> Result<(), SnapshotAdapterError> {
        let expected = self.expected_field_count.context(BeginRequiredSnafu {
            target: "schema snapshot encoder",
        })?;
        ensure!(
            self.row.fields.len() == expected,
            FieldCountMismatchSnafu {
                expected,
                actual: self.row.fields.len(),
            }
        );
        Ok(())
    }

    fn register_field(
        &mut self,
        field_name: &str,
    ) -> Result<(&'schema str, &'schema ReplicatedDataType), SnapshotAdapterError> {
        self.ensure_begun()?;
        let Some((schema_field_name, schema_field)) = self.schema.columns.get_key_value(field_name)
        else {
            return UnknownSchemaFieldSnafu {
                field_name: field_name.to_owned(),
            }
            .fail();
        };
        ensure!(
            self.seen_fields.insert(schema_field_name.as_str()),
            DuplicateFieldSnafu {
                field_name: field_name.to_owned(),
            }
        );
        Ok((schema_field_name.as_str(), &schema_field.data_type))
    }

    fn push_state_field(
        &mut self,
        field_name: &'schema str,
        wire_value: StateSnapshotWireValue,
    ) -> Result<(), SnapshotAdapterError> {
        let mut field = proto::SnapshotField {
            field_name: field_name.to_string(),
            ..proto::SnapshotField::new()
        };
        match wire_value {
            StateSnapshotWireValue::MonotonicCounter(value) => field.set_monotonic_counter(value),
            StateSnapshotWireValue::TotalOrderRegister(value) => {
                field.set_total_order_register(value)
            }
            StateSnapshotWireValue::TotalOrderFiniteStateRegister(value) => {
                field.set_total_order_finite_state_register(value)
            }
        }
        self.row.fields.push(field);
        Ok(())
    }

    fn begin_history_field<'row, Node>(
        &'row mut self,
        field_name: &'schema str,
        field_kind: HistoryFieldKind,
    ) -> Result<HistoryFieldEncoderState<'schema, 'row, Node>, SnapshotAdapterError> {
        self.ensure_begun()?;
        Ok(HistoryFieldEncoderState {
            owner: self,
            field_name,
            field_kind,
            expected_node_count: None,
            next_index: 0,
            nodes: Vec::new(),
        })
    }
}

impl<'schema> SchemaSnapshotEncoder<UpdateId> for ProtoSchemaSnapshotEncoder<'schema> {
    type Error = SnapshotAdapterError;

    type LatestValueWinsFieldSink<'a>
        = LatestValueWinsHistoryFieldSink<'schema, 'a>
    where
        Self: 'a;

    type LinearStringFieldSink<'a>
        = LinearStringHistoryFieldSink<'schema, 'a>
    where
        Self: 'a;

    type LinearListFieldSink<'a>
        = LinearListHistoryFieldSink<'schema, 'a>
    where
        Self: 'a;

    fn begin(&mut self, field_count: usize) -> Result<(), Self::Error> {
        ensure!(
            self.expected_field_count.is_none(),
            AlreadyBegunSnafu {
                target: "schema snapshot encoder",
            }
        );
        self.expected_field_count = Some(field_count);
        self.row.fields = Vec::with_capacity(field_count);
        self.seen_fields.clear();
        Ok(())
    }

    fn state_field(
        &mut self,
        field_name: &str,
        value: SnapshotStateValueRef<'_>,
    ) -> Result<(), Self::Error> {
        let (field_name, data_type) = self.register_field(field_name)?;
        ensure!(
            !matches!(
                data_type,
                ReplicatedDataType::LatestValueWins { .. }
                    | ReplicatedDataType::LinearString
                    | ReplicatedDataType::LinearList { .. }
            ),
            UnexpectedSchemaDataTypeSnafu {
                field_name: field_name.to_owned(),
                expected: "state-backed field",
            }
        );
        let wire_value = encode_state_snapshot_value(data_type, value).context(CodecSnafu)?;
        self.push_state_field(field_name, wire_value)
    }

    fn prepare_latest_value_wins_field<'a>(
        &'a mut self,
        field_name: &str,
        _value_type: &NullableBasicDataType,
    ) -> Result<Self::LatestValueWinsFieldSink<'a>, Self::Error> {
        let (field_name, schema_field) = self.register_field(field_name)?;
        match schema_field {
            ReplicatedDataType::LatestValueWins { value_type } => {
                Ok(LatestValueWinsHistoryFieldSink {
                    state: Some(
                        self.begin_history_field(field_name, HistoryFieldKind::LatestValueWins)?,
                    ),
                    value_type,
                })
            }
            _ => UnexpectedSchemaDataTypeSnafu {
                field_name: field_name.to_owned(),
                expected: "LatestValueWins field",
            }
            .fail(),
        }
    }

    fn prepare_linear_string_field<'a>(
        &'a mut self,
        field_name: &str,
    ) -> Result<Self::LinearStringFieldSink<'a>, Self::Error> {
        let (field_name, schema_field) = self.register_field(field_name)?;
        match schema_field {
            ReplicatedDataType::LinearString => Ok(LinearStringHistoryFieldSink {
                state: Some(self.begin_history_field(field_name, HistoryFieldKind::LinearString)?),
            }),
            _ => UnexpectedSchemaDataTypeSnafu {
                field_name: field_name.to_owned(),
                expected: "LinearString field",
            }
            .fail(),
        }
    }

    fn prepare_linear_list_field<'a>(
        &'a mut self,
        field_name: &str,
        _value_type: PrimitiveType,
    ) -> Result<Self::LinearListFieldSink<'a>, Self::Error> {
        let (field_name, schema_field) = self.register_field(field_name)?;
        match schema_field {
            ReplicatedDataType::LinearList { value_type } => Ok(LinearListHistoryFieldSink {
                state: Some(self.begin_history_field(field_name, HistoryFieldKind::LinearList)?),
                value_type: *value_type,
            }),
            _ => UnexpectedSchemaDataTypeSnafu {
                field_name: field_name.to_owned(),
                expected: "LinearList field",
            }
            .fail(),
        }
    }

    fn end(&mut self) -> Result<(), Self::Error> {
        self.validate_finished()
    }
}

#[derive(Clone, Copy)]
enum HistoryFieldKind {
    LatestValueWins,
    LinearString,
    LinearList,
}

impl HistoryFieldKind {
    fn into_snapshot_field(
        self,
        field_name: &str,
        history: proto::HistorySnapshot,
    ) -> proto::SnapshotField {
        let mut field = proto::SnapshotField {
            field_name: field_name.to_string(),
            ..proto::SnapshotField::new()
        };
        match self {
            Self::LatestValueWins => field.set_latest_value_wins(history),
            Self::LinearString => field.set_linear_string(history),
            Self::LinearList => field.set_linear_list(history),
        }
        field
    }
}

struct HistoryFieldEncoderState<'schema, 'row, Node> {
    owner: &'row mut ProtoSchemaSnapshotEncoder<'schema>,
    field_name: &'schema str,
    field_kind: HistoryFieldKind,
    expected_node_count: Option<usize>,
    next_index: usize,
    nodes: Vec<Node>,
}

impl<Node> HistoryFieldEncoderState<'_, '_, Node> {
    fn begin(&mut self, header: SnapshotHeader) -> Result<(), SnapshotAdapterError> {
        ensure!(
            self.expected_node_count.is_none(),
            AlreadyBegunSnafu {
                target: "history field sink",
            }
        );
        self.expected_node_count = Some(header.node_count);
        self.nodes = Vec::with_capacity(header.node_count);
        self.next_index = 0;
        Ok(())
    }

    fn push_node(&mut self, index: usize, node: Node) -> Result<(), SnapshotAdapterError> {
        self.expected_node_count.context(BeginRequiredSnafu {
            target: "history field sink",
        })?;
        ensure!(
            index == self.next_index,
            UnexpectedNodeIndexSnafu {
                expected: self.next_index,
                actual: index,
            }
        );
        self.nodes.push(node);
        self.next_index += 1;
        Ok(())
    }

    fn finish(self, history: proto::HistorySnapshot) -> Result<(), SnapshotAdapterError> {
        let expected = self.expected_node_count.context(BeginRequiredSnafu {
            target: "history field sink",
        })?;
        ensure!(
            self.nodes.len() == expected,
            NodeCountMismatchSnafu {
                expected,
                actual: self.nodes.len(),
            }
        );
        self.owner.row.fields.push(
            self.field_kind
                .into_snapshot_field(self.field_name, history),
        );
        Ok(())
    }
}

/// Streaming sink for one `LatestValueWins` history field.
pub struct LatestValueWinsHistoryFieldSink<'schema, 'row> {
    state: Option<
        HistoryFieldEncoderState<
            'schema,
            'row,
            SnapshotNode<UpdateId, flotsync_data_types::schema::datamodel::NullableBasicValue>,
        >,
    >,
    value_type: &'schema NullableBasicDataType,
}

impl<'schema, 'row, 'value>
    SnapshotSink<UpdateId, flotsync_data_types::schema::datamodel::NullableBasicValueRef<'value>>
    for LatestValueWinsHistoryFieldSink<'schema, 'row>
{
    type Error = SnapshotAdapterError;

    fn begin(&mut self, header: SnapshotHeader) -> Result<(), Self::Error> {
        self.state
            .as_mut()
            .context(HistoryFieldClosedSnafu)?
            .begin(header)
    }

    fn node(
        &mut self,
        index: usize,
        node: SnapshotNodeRef<
            '_,
            UpdateId,
            flotsync_data_types::schema::datamodel::NullableBasicValueRef<'value>,
        >,
    ) -> Result<(), Self::Error> {
        self.state
            .as_mut()
            .context(HistoryFieldClosedSnafu)?
            .push_node(index, owned_latest_value_wins_node(node))
    }

    fn end(&mut self) -> Result<(), Self::Error> {
        let state = self.state.take().context(HistoryFieldClosedSnafu)?;
        let history =
            encode_columnar_latest_value_wins_history_snapshot(&state.nodes, self.value_type)
                .context(HistoryCodecSnafu)?;
        state.finish(history)
    }
}

/// Streaming sink for one `LinearString` history field.
pub struct LinearStringHistoryFieldSink<'schema, 'row> {
    state: Option<
        HistoryFieldEncoderState<'schema, 'row, SnapshotNode<IdWithIndex<UpdateId>, String>>,
    >,
}

impl<'schema, 'row> SnapshotSink<IdWithIndex<UpdateId>, str>
    for LinearStringHistoryFieldSink<'schema, 'row>
{
    type Error = SnapshotAdapterError;

    fn begin(&mut self, header: SnapshotHeader) -> Result<(), Self::Error> {
        self.state
            .as_mut()
            .context(HistoryFieldClosedSnafu)?
            .begin(header)
    }

    fn node(
        &mut self,
        index: usize,
        node: SnapshotNodeRef<'_, IdWithIndex<UpdateId>, str>,
    ) -> Result<(), Self::Error> {
        self.state
            .as_mut()
            .context(HistoryFieldClosedSnafu)?
            .push_node(index, owned_linear_string_node(node))
    }

    fn end(&mut self) -> Result<(), Self::Error> {
        let state = self.state.take().context(HistoryFieldClosedSnafu)?;
        let history = encode_columnar_linear_string_history_snapshot(&state.nodes)
            .context(HistoryCodecSnafu)?;
        state.finish(history)
    }
}

/// Streaming sink for one `LinearList` history field.
pub struct LinearListHistoryFieldSink<'schema, 'row> {
    state: Option<
        HistoryFieldEncoderState<
            'schema,
            'row,
            SnapshotNode<IdWithIndex<UpdateId>, PrimitiveValueArray>,
        >,
    >,
    value_type: PrimitiveType,
}

impl<'schema, 'row, 'value>
    SnapshotSink<
        IdWithIndex<UpdateId>,
        flotsync_data_types::schema::datamodel::PrimitiveValueArrayRef<'value>,
    > for LinearListHistoryFieldSink<'schema, 'row>
{
    type Error = SnapshotAdapterError;

    fn begin(&mut self, header: SnapshotHeader) -> Result<(), Self::Error> {
        self.state
            .as_mut()
            .context(HistoryFieldClosedSnafu)?
            .begin(header)
    }

    fn node(
        &mut self,
        index: usize,
        node: SnapshotNodeRef<
            '_,
            IdWithIndex<UpdateId>,
            flotsync_data_types::schema::datamodel::PrimitiveValueArrayRef<'_>,
        >,
    ) -> Result<(), Self::Error> {
        self.state
            .as_mut()
            .context(HistoryFieldClosedSnafu)?
            .push_node(index, owned_linear_list_node(node))
    }

    fn end(&mut self) -> Result<(), Self::Error> {
        let state = self.state.take().context(HistoryFieldClosedSnafu)?;
        let history = encode_columnar_linear_list_history_snapshot(&state.nodes, self.value_type)
            .context(HistoryCodecSnafu)?;
        state.finish(history)
    }
}

/// Protobuf-backed implementation of `SchemaSnapshotDecoder` for one row snapshot.
#[derive(Debug)]
pub struct ProtoSchemaSnapshotDecoder {
    raw_field_count: usize,
    fields: HashMap<String, Option<proto::snapshot_field::Value>>,
    expected_field_count: Option<usize>,
    decoded_field_count: usize,
}

impl ProtoSchemaSnapshotDecoder {
    /// Create a row decoder that consumes `row` destructively.
    pub fn new(row: proto::RowSnapshot) -> Result<Self, SnapshotAdapterError> {
        let raw_field_count = row.fields.len();
        let mut fields = HashMap::with_capacity(raw_field_count);
        for mut field in row.fields {
            let field_name = std::mem::take(&mut field.field_name);
            match fields.entry(field_name) {
                Entry::Vacant(entry) => {
                    entry.insert(field.value.take());
                }
                Entry::Occupied(entry) => {
                    return DuplicateFieldSnafu {
                        field_name: entry.key().clone(),
                    }
                    .fail();
                }
            }
        }
        Ok(Self {
            raw_field_count,
            fields,
            expected_field_count: None,
            decoded_field_count: 0,
        })
    }

    fn ensure_begun(&self) -> Result<usize, SnapshotAdapterError> {
        self.expected_field_count.context(BeginRequiredSnafu {
            target: "schema snapshot decoder",
        })
    }

    fn take_field_value(
        &mut self,
        field_name: &str,
    ) -> Result<proto::snapshot_field::Value, SnapshotAdapterError> {
        let _ = self.ensure_begun()?;
        let value = self.fields.remove(field_name).context(MissingFieldSnafu {
            field_name: field_name.to_owned(),
        })?;
        let value = value.context(MissingFieldValueSnafu {
            field_name: field_name.to_owned(),
        })?;
        self.decoded_field_count += 1;
        Ok(value)
    }

    fn validate_finished(&self) -> Result<(), SnapshotAdapterError> {
        let expected = self.ensure_begun()?;
        ensure!(
            self.fields.is_empty(),
            RemainingFieldsSnafu {
                field_names: self.fields.keys().cloned().collect::<Vec<_>>(),
            }
        );
        ensure!(
            self.decoded_field_count == expected,
            FieldCountMismatchSnafu {
                expected,
                actual: self.decoded_field_count,
            }
        );
        Ok(())
    }
}

impl SchemaSnapshotDecoder<UpdateId> for ProtoSchemaSnapshotDecoder {
    type Error = SnapshotAdapterError;

    type LatestValueWinsFieldSource<'a>
        = LatestValueWinsHistoryFieldSource
    where
        Self: 'a;

    type LinearStringFieldSource<'a>
        = LinearStringHistoryFieldSource
    where
        Self: 'a;

    type LinearListFieldSource<'a>
        = LinearListHistoryFieldSource
    where
        Self: 'a;

    fn begin(&mut self, expected_field_count: usize) -> Result<(), Self::Error> {
        ensure!(
            self.expected_field_count.is_none(),
            AlreadyBegunSnafu {
                target: "schema snapshot decoder",
            }
        );
        ensure!(
            self.raw_field_count == expected_field_count,
            FieldCountMismatchSnafu {
                expected: expected_field_count,
                actual: self.raw_field_count,
            }
        );
        self.expected_field_count = Some(expected_field_count);
        self.decoded_field_count = 0;
        Ok(())
    }

    fn decode_state_field(
        &mut self,
        field_name: &str,
        data_type: &ReplicatedDataType,
    ) -> Result<SnapshotStateValue, Self::Error> {
        let value = self.take_field_value(field_name)?;
        let wire = match value {
            proto::snapshot_field::Value::MonotonicCounter(value) => {
                StateSnapshotWireValue::MonotonicCounter(value)
            }
            proto::snapshot_field::Value::TotalOrderRegister(value) => {
                StateSnapshotWireValue::TotalOrderRegister(value)
            }
            proto::snapshot_field::Value::TotalOrderFiniteStateRegister(value) => {
                StateSnapshotWireValue::TotalOrderFiniteStateRegister(value)
            }
            other => {
                return UnexpectedFieldKindSnafu {
                    field_name: field_name.to_owned(),
                    expected: expected_state_field_kind(data_type),
                    actual: snapshot_field_kind_name(&other),
                }
                .fail();
            }
        };
        decode_state_snapshot_value(data_type, wire).context(CodecSnafu)
    }

    fn prepare_latest_value_wins_field<'a>(
        &'a mut self,
        field_name: &str,
        value_type: &NullableBasicDataType,
    ) -> Result<Self::LatestValueWinsFieldSource<'a>, Self::Error> {
        let value = self.take_field_value(field_name)?;
        let history = match value {
            proto::snapshot_field::Value::LatestValueWins(history) => history,
            other => {
                return UnexpectedFieldKindSnafu {
                    field_name: field_name.to_owned(),
                    expected: "latest_value_wins",
                    actual: snapshot_field_kind_name(&other),
                }
                .fail();
            }
        };
        let nodes = decode_columnar_latest_value_wins_history_snapshot(history, value_type.clone())
            .context(HistoryCodecSnafu)?;
        Ok(LatestValueWinsHistoryFieldSource {
            nodes: nodes.into_iter(),
        })
    }

    fn prepare_linear_string_field<'a>(
        &'a mut self,
        field_name: &str,
    ) -> Result<Self::LinearStringFieldSource<'a>, Self::Error> {
        let value = self.take_field_value(field_name)?;
        let history = match value {
            proto::snapshot_field::Value::LinearString(history) => history,
            other => {
                return UnexpectedFieldKindSnafu {
                    field_name: field_name.to_owned(),
                    expected: "linear_string",
                    actual: snapshot_field_kind_name(&other),
                }
                .fail();
            }
        };
        Ok(LinearStringHistoryFieldSource {
            nodes: decode_columnar_linear_string_history_snapshot(history)
                .context(HistoryCodecSnafu)?
                .into_iter(),
        })
    }

    fn prepare_linear_list_field<'a>(
        &'a mut self,
        field_name: &str,
        value_type: PrimitiveType,
    ) -> Result<Self::LinearListFieldSource<'a>, Self::Error> {
        let value = self.take_field_value(field_name)?;
        let history = match value {
            proto::snapshot_field::Value::LinearList(history) => history,
            other => {
                return UnexpectedFieldKindSnafu {
                    field_name: field_name.to_owned(),
                    expected: "linear_list",
                    actual: snapshot_field_kind_name(&other),
                }
                .fail();
            }
        };
        Ok(LinearListHistoryFieldSource {
            nodes: decode_columnar_linear_list_history_snapshot(history, value_type)
                .context(HistoryCodecSnafu)?
                .into_iter(),
        })
    }

    fn end(&mut self) -> Result<(), Self::Error> {
        self.validate_finished()
    }
}

/// Lazy node source for a `LatestValueWins` history field decoded from protobuf.
pub struct LatestValueWinsHistoryFieldSource {
    nodes: std::vec::IntoIter<
        SnapshotNode<UpdateId, flotsync_data_types::schema::datamodel::NullableBasicValue>,
    >,
}

impl SnapshotNodeSource<UpdateId, flotsync_data_types::schema::datamodel::NullableBasicValue>
    for LatestValueWinsHistoryFieldSource
{
    type Error = SnapshotAdapterError;

    fn next_node(
        &mut self,
    ) -> Result<
        Option<SnapshotNode<UpdateId, flotsync_data_types::schema::datamodel::NullableBasicValue>>,
        Self::Error,
    > {
        Ok(self.nodes.next())
    }
}

/// Lazy node source for a `LinearString` history field decoded from protobuf.
pub struct LinearStringHistoryFieldSource {
    nodes: std::vec::IntoIter<SnapshotNode<IdWithIndex<UpdateId>, String>>,
}

impl SnapshotNodeSource<IdWithIndex<UpdateId>, String> for LinearStringHistoryFieldSource {
    type Error = SnapshotAdapterError;

    fn next_node(
        &mut self,
    ) -> Result<Option<SnapshotNode<IdWithIndex<UpdateId>, String>>, Self::Error> {
        Ok(self.nodes.next())
    }
}

/// Lazy node source for a `LinearList` history field decoded from protobuf.
pub struct LinearListHistoryFieldSource {
    nodes: std::vec::IntoIter<PrimitiveValueArrayNode>,
}

type PrimitiveValueArrayNode = SnapshotNode<IdWithIndex<UpdateId>, PrimitiveValueArray>;

impl SnapshotNodeSource<IdWithIndex<UpdateId>, PrimitiveValueArray>
    for LinearListHistoryFieldSource
{
    type Error = SnapshotAdapterError;

    fn next_node(&mut self) -> Result<Option<PrimitiveValueArrayNode>, Self::Error> {
        Ok(self.nodes.next())
    }
}

fn owned_latest_value_wins_node(
    node: SnapshotNodeRef<
        '_,
        UpdateId,
        flotsync_data_types::schema::datamodel::NullableBasicValueRef<'_>,
    >,
) -> SnapshotNode<UpdateId, flotsync_data_types::schema::datamodel::NullableBasicValue> {
    SnapshotNode {
        id: *node.id,
        left: node.left.copied(),
        right: node.right.copied(),
        deleted: node.deleted,
        value: node.value.map(|value| value.clone().into_owned()),
    }
}

fn owned_linear_string_node(
    node: SnapshotNodeRef<'_, IdWithIndex<UpdateId>, str>,
) -> SnapshotNode<IdWithIndex<UpdateId>, String> {
    SnapshotNode {
        id: node.id.clone(),
        left: node.left.cloned(),
        right: node.right.cloned(),
        deleted: node.deleted,
        value: node.value.map(str::to_owned),
    }
}

fn owned_linear_list_node(
    node: SnapshotNodeRef<
        '_,
        IdWithIndex<UpdateId>,
        flotsync_data_types::schema::datamodel::PrimitiveValueArrayRef<'_>,
    >,
) -> PrimitiveValueArrayNode {
    SnapshotNode {
        id: node.id.clone(),
        left: node.left.cloned(),
        right: node.right.cloned(),
        deleted: node.deleted,
        value: node.value.map(|value| value.clone().into_owned()),
    }
}

/// Row-scoped proxy around `ProtoSchemaSnapshotEncoder`.
///
/// This exists to bridge trait-associated lifetimes cleanly for the history field sink types.
pub struct ProtoDataSnapshotRowEncoder<'schema, 'row> {
    inner: &'row mut ProtoSchemaSnapshotEncoder<'schema>,
}

impl<'schema, 'row> SchemaSnapshotEncoder<UpdateId> for ProtoDataSnapshotRowEncoder<'schema, 'row> {
    type Error = SnapshotAdapterError;

    type LatestValueWinsFieldSink<'a>
        = <ProtoSchemaSnapshotEncoder<'schema> as SchemaSnapshotEncoder<UpdateId>>::LatestValueWinsFieldSink<'a>
    where
        Self: 'a;

    type LinearStringFieldSink<'a>
        = <ProtoSchemaSnapshotEncoder<'schema> as SchemaSnapshotEncoder<UpdateId>>::LinearStringFieldSink<
        'a,
    >
    where
        Self: 'a;

    type LinearListFieldSink<'a>
        =
        <ProtoSchemaSnapshotEncoder<'schema> as SchemaSnapshotEncoder<UpdateId>>::LinearListFieldSink<'a>
    where
        Self: 'a;

    fn begin(&mut self, field_count: usize) -> Result<(), Self::Error> {
        self.inner.begin(field_count)
    }

    fn state_field(
        &mut self,
        field_name: &str,
        value: SnapshotStateValueRef<'_>,
    ) -> Result<(), Self::Error> {
        self.inner.state_field(field_name, value)
    }

    fn prepare_latest_value_wins_field<'a>(
        &'a mut self,
        field_name: &str,
        value_type: &NullableBasicDataType,
    ) -> Result<Self::LatestValueWinsFieldSink<'a>, Self::Error> {
        self.inner
            .prepare_latest_value_wins_field(field_name, value_type)
    }

    fn prepare_linear_string_field<'a>(
        &'a mut self,
        field_name: &str,
    ) -> Result<Self::LinearStringFieldSink<'a>, Self::Error> {
        self.inner.prepare_linear_string_field(field_name)
    }

    fn prepare_linear_list_field<'a>(
        &'a mut self,
        field_name: &str,
        value_type: PrimitiveType,
    ) -> Result<Self::LinearListFieldSink<'a>, Self::Error> {
        self.inner.prepare_linear_list_field(field_name, value_type)
    }

    fn end(&mut self) -> Result<(), Self::Error> {
        self.inner.end()
    }
}

/// Protobuf-backed implementation of `DataSnapshotEncoder` for dataset snapshots.
pub struct ProtoDataSnapshotEncoder<'schema> {
    schema: &'schema Schema,
    snapshot: proto::DataSnapshot,
    expected_row_count: Option<usize>,
    current_row_index: Option<usize>,
    current_row: Option<ProtoSchemaSnapshotEncoder<'schema>>,
}

impl<'schema> ProtoDataSnapshotEncoder<'schema> {
    /// Create an empty dataset encoder bound to `schema`.
    pub fn new(schema: &'schema Schema) -> Self {
        Self {
            schema,
            snapshot: proto::DataSnapshot::new(),
            expected_row_count: None,
            current_row_index: None,
            current_row: None,
        }
    }

    pub fn into_snapshot(self) -> Result<proto::DataSnapshot, SnapshotAdapterError> {
        self.validate_finished()?;
        Ok(self.snapshot)
    }

    fn validate_finished(&self) -> Result<(), SnapshotAdapterError> {
        let expected = self.expected_row_count.context(BeginRequiredSnafu {
            target: "data snapshot encoder",
        })?;
        ensure!(self.current_row.is_none(), RowAlreadyOpenSnafu);
        ensure!(
            self.snapshot.rows.len() == expected,
            FieldCountMismatchSnafu {
                expected,
                actual: self.snapshot.rows.len(),
            }
        );
        Ok(())
    }
}

impl<'schema> flotsync_data_types::schema::datamodel::DataSnapshotEncoder<UpdateId>
    for ProtoDataSnapshotEncoder<'schema>
{
    type Error = SnapshotAdapterError;

    type RowEncoder<'a>
        = ProtoDataSnapshotRowEncoder<'schema, 'a>
    where
        Self: 'a;

    fn begin(&mut self, row_count: usize) -> Result<(), Self::Error> {
        ensure!(
            self.expected_row_count.is_none(),
            AlreadyBegunSnafu {
                target: "data snapshot encoder",
            }
        );
        self.expected_row_count = Some(row_count);
        self.snapshot.rows = Vec::with_capacity(row_count);
        Ok(())
    }

    fn begin_row<'a>(&'a mut self, row_index: usize) -> Result<Self::RowEncoder<'a>, Self::Error> {
        let row_count = self.expected_row_count.context(BeginRequiredSnafu {
            target: "data snapshot encoder",
        })?;
        ensure!(self.current_row.is_none(), RowAlreadyOpenSnafu);
        ensure!(
            row_index < row_count,
            RowOutOfBoundsSnafu {
                row_index,
                row_count,
            }
        );
        self.current_row_index = Some(row_index);
        self.current_row = Some(ProtoSchemaSnapshotEncoder::new(self.schema));
        Ok(ProtoDataSnapshotRowEncoder {
            inner: self.current_row.as_mut().expect("row was inserted"),
        })
    }

    fn end_row(&mut self, row_index: usize) -> Result<(), Self::Error> {
        let open_row = self.current_row_index.context(NoOpenRowSnafu)?;
        ensure!(
            open_row == row_index,
            WrongOpenRowSnafu {
                row_index,
                open_row
            }
        );
        let row = self.current_row.take().context(NoOpenRowSnafu)?;
        self.current_row_index = None;
        self.snapshot.rows.push(row.into_row_snapshot()?);
        Ok(())
    }

    fn end(&mut self) -> Result<(), Self::Error> {
        self.validate_finished()
    }
}

/// Protobuf-backed implementation of `DataSnapshotDecoder` for dataset snapshots.
pub struct ProtoDataSnapshotDecoder {
    rows: Vec<Option<proto::RowSnapshot>>,
    began: bool,
    open_row: Option<usize>,
}

impl ProtoDataSnapshotDecoder {
    /// Create a dataset decoder that consumes `snapshot` destructively.
    pub fn new(snapshot: proto::DataSnapshot) -> Self {
        Self {
            rows: snapshot.rows.into_iter().map(Some).collect(),
            began: false,
            open_row: None,
        }
    }
}

impl flotsync_data_types::schema::datamodel::DataSnapshotDecoder<UpdateId>
    for ProtoDataSnapshotDecoder
{
    type Error = SnapshotAdapterError;

    type RowDecoder<'a>
        = ProtoSchemaSnapshotDecoder
    where
        Self: 'a;

    fn begin(&mut self) -> Result<usize, Self::Error> {
        ensure!(
            !self.began,
            AlreadyBegunSnafu {
                target: "data snapshot decoder"
            }
        );
        self.began = true;
        Ok(self.rows.len())
    }

    fn begin_row<'a>(&'a mut self, row_index: usize) -> Result<Self::RowDecoder<'a>, Self::Error> {
        ensure!(
            self.began,
            BeginRequiredSnafu {
                target: "data snapshot decoder"
            }
        );
        ensure!(self.open_row.is_none(), RowAlreadyOpenSnafu);
        ensure!(
            row_index < self.rows.len(),
            RowOutOfBoundsSnafu {
                row_index,
                row_count: self.rows.len(),
            }
        );
        let row = self.rows[row_index]
            .take()
            .context(RowAlreadyConsumedSnafu { row_index })?;
        self.open_row = Some(row_index);
        ProtoSchemaSnapshotDecoder::new(row)
    }

    fn end_row(&mut self, row_index: usize) -> Result<(), Self::Error> {
        let open_row = self.open_row.context(NoOpenRowSnafu)?;
        ensure!(
            open_row == row_index,
            WrongOpenRowSnafu {
                row_index,
                open_row
            }
        );
        self.open_row = None;
        Ok(())
    }

    fn end(&mut self) -> Result<(), Self::Error> {
        ensure!(
            self.began,
            BeginRequiredSnafu {
                target: "data snapshot decoder"
            }
        );
        ensure!(self.open_row.is_none(), RowAlreadyOpenSnafu);
        let remaining_rows: Vec<usize> = self
            .rows
            .iter()
            .enumerate()
            .filter_map(|(index, row)| row.as_ref().map(|_| index))
            .collect();
        ensure!(
            remaining_rows.is_empty(),
            RemainingRowsSnafu {
                row_indices: remaining_rows,
            }
        );
        Ok(())
    }
}

fn snapshot_field_kind_name(value: &proto::snapshot_field::Value) -> &'static str {
    match value {
        proto::snapshot_field::Value::LatestValueWins(_) => "latest_value_wins",
        proto::snapshot_field::Value::LinearString(_) => "linear_string",
        proto::snapshot_field::Value::LinearList(_) => "linear_list",
        proto::snapshot_field::Value::MonotonicCounter(_) => "monotonic_counter",
        proto::snapshot_field::Value::TotalOrderRegister(_) => "total_order_register",
        proto::snapshot_field::Value::TotalOrderFiniteStateRegister(_) => {
            "total_order_finite_state_register"
        }
    }
}

fn expected_state_field_kind(data_type: &ReplicatedDataType) -> &'static str {
    match data_type {
        ReplicatedDataType::MonotonicCounter { .. } => "monotonic_counter",
        ReplicatedDataType::TotalOrderRegister { .. } => "total_order_register",
        ReplicatedDataType::TotalOrderFiniteStateRegister { .. } => {
            "total_order_finite_state_register"
        }
        ReplicatedDataType::LatestValueWins { .. } => "latest_value_wins",
        ReplicatedDataType::LinearString => "linear_string",
        ReplicatedDataType::LinearList { .. } => "linear_list",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protobuf::Message;
    use flotsync_data_types::{
        any_data::{LinearLatestValueWins, list::LinearList},
        schema::{
            BasicDataType,
            Direction,
            Field,
            NullableBasicDataType,
            NullablePrimitiveType,
            datamodel::{
                DataSnapshotDecoder,
                InMemoryData,
                InMemoryFieldValue,
                LinearLatestValueWinsValue,
                LinearListValue,
            },
            values::{NullablePrimitiveValue, NullablePrimitiveValueArray, PrimitiveValue},
        },
        text::LinearString,
    };
    use std::{borrow::Cow, collections::HashMap, sync::LazyLock};

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

    fn state_field(name: &str, value: proto::snapshot_field::Value) -> proto::SnapshotField {
        proto::SnapshotField {
            field_name: name.to_owned(),
            value: Some(value),
            ..proto::SnapshotField::new()
        }
    }

    static TEST_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
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
                        values: flotsync_data_types::schema::values::PrimitiveValueArray::String(
                            vec!["draft".to_owned(), "published".to_owned()],
                        ),
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
    fn dataset_roundtrips_via_protobuf_snapshot_messages() {
        let schema = &*TEST_SCHEMA;

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
                InMemoryFieldValue::LatestValueWins(LinearLatestValueWinsValue::NullableUInt(
                    latest.clone(),
                )),
            ),
            ("title", InMemoryFieldValue::LinearString(title.clone())),
            (
                "numbers",
                InMemoryFieldValue::LinearList(LinearListValue::Int(numbers.clone())),
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
    fn schema_decoder_rejects_duplicate_fields() {
        let row = proto::RowSnapshot {
            fields: vec![
                state_field(
                    "counter",
                    proto::snapshot_field::Value::MonotonicCounter(
                        crate::codecs::datamodel::encode_counter_value(
                            flotsync_data_types::schema::datamodel::CounterValueRef::UInt(1),
                        ),
                    ),
                ),
                state_field(
                    "counter",
                    proto::snapshot_field::Value::MonotonicCounter(
                        crate::codecs::datamodel::encode_counter_value(
                            flotsync_data_types::schema::datamodel::CounterValueRef::UInt(2),
                        ),
                    ),
                ),
            ],
            ..proto::RowSnapshot::new()
        };

        let err = ProtoSchemaSnapshotDecoder::new(row).unwrap_err();
        assert!(matches!(
            err,
            SnapshotAdapterError::DuplicateField { field_name } if field_name == "counter"
        ));
    }

    #[test]
    fn schema_decoder_rejects_wrong_field_variant() {
        let row = proto::RowSnapshot {
            fields: vec![state_field(
                "counter",
                proto::snapshot_field::Value::LinearString(proto::HistorySnapshot::new()),
            )],
            ..proto::RowSnapshot::new()
        };
        let mut decoder = ProtoSchemaSnapshotDecoder::new(row).unwrap();
        decoder.begin(1).unwrap();

        let err = decoder
            .decode_state_field(
                "counter",
                &ReplicatedDataType::MonotonicCounter { small_range: false },
            )
            .unwrap_err();
        assert!(matches!(
            err,
            SnapshotAdapterError::UnexpectedFieldKind {
                field_name,
                expected,
                actual,
            } if field_name == "counter"
                && expected == "monotonic_counter"
                && actual == "linear_string"
        ));
    }

    #[test]
    fn schema_decoder_reports_missing_field() {
        let row = proto::RowSnapshot {
            fields: vec![
                state_field(
                    "counter",
                    proto::snapshot_field::Value::MonotonicCounter(
                        crate::codecs::datamodel::encode_counter_value(
                            flotsync_data_types::schema::datamodel::CounterValueRef::UInt(1),
                        ),
                    ),
                ),
                state_field(
                    "extra",
                    proto::snapshot_field::Value::TotalOrderRegister(
                        crate::codecs::datamodel::encode_primitive_value(
                            flotsync_data_types::schema::values::PrimitiveValueRef::UInt(7),
                        ),
                    ),
                ),
            ],
            ..proto::RowSnapshot::new()
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
        assert!(matches!(
            err,
            SnapshotAdapterError::MissingField { field_name } if field_name == "priority"
        ));
    }

    #[test]
    fn schema_decoder_rejects_unconsumed_extra_fields_on_end() {
        let row = proto::RowSnapshot {
            fields: vec![
                state_field(
                    "counter",
                    proto::snapshot_field::Value::MonotonicCounter(
                        crate::codecs::datamodel::encode_counter_value(
                            flotsync_data_types::schema::datamodel::CounterValueRef::UInt(1),
                        ),
                    ),
                ),
                state_field(
                    "extra",
                    proto::snapshot_field::Value::TotalOrderRegister(
                        crate::codecs::datamodel::encode_primitive_value(
                            flotsync_data_types::schema::values::PrimitiveValueRef::UInt(7),
                        ),
                    ),
                ),
            ],
            ..proto::RowSnapshot::new()
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
        assert!(matches!(
            err,
            SnapshotAdapterError::RemainingFields { field_names }
                if field_names == vec!["extra".to_owned()]
        ));
    }

    #[test]
    fn dataset_decoder_rejects_remaining_rows() {
        let snapshot = proto::DataSnapshot {
            rows: vec![proto::RowSnapshot::new()],
            ..proto::DataSnapshot::new()
        };
        let mut decoder = ProtoDataSnapshotDecoder::new(snapshot);
        let row_count = decoder.begin().unwrap();
        assert_eq!(row_count, 1);
        let err = decoder.end().unwrap_err();
        assert!(matches!(
            err,
            SnapshotAdapterError::RemainingRows { row_indices } if row_indices == vec![0]
        ));
    }
}
