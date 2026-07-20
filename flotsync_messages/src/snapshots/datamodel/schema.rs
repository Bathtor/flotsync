//! Protobuf adapters for individual schema snapshots and their history fields.

use super::*;

pub struct ProtoSchemaSnapshotEncoder<'schema> {
    schema: &'schema Schema,
    row: proto::RowSnapshot,
    expected_field_count: Option<usize>,
    seen_fields: HashSet<&'schema str>,
}

impl<'schema> ProtoSchemaSnapshotEncoder<'schema> {
    /// Create an empty row encoder bound to `schema`.
    #[must_use]
    pub fn new(schema: &'schema Schema) -> Self {
        Self {
            schema,
            row: proto::RowSnapshot::default(),
            expected_field_count: None,
            seen_fields: HashSet::new(),
        }
    }

    /// Finish encoding and return the owned protobuf row snapshot.
    ///
    /// # Errors
    ///
    /// See `SnapshotAdapterError` for failure conditions.
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

    fn push_state_field(&mut self, field_name: &'schema str, wire_value: StateSnapshotWireValue) {
        let mut field = proto::SnapshotField {
            field_name: field_name.to_string(),
            ..proto::SnapshotField::default()
        };
        match wire_value {
            StateSnapshotWireValue::MonotonicCounter(value) => {
                field.value = Some(proto::snapshot_field::Value::MonotonicCounter(Box::new(
                    value,
                )));
            }
            StateSnapshotWireValue::TotalOrderRegister(value) => {
                field.value = Some(proto::snapshot_field::Value::TotalOrderRegister(Box::new(
                    value,
                )));
            }
            StateSnapshotWireValue::TotalOrderFiniteStateRegister(value) => {
                field.value = Some(proto::snapshot_field::Value::TotalOrderFiniteStateRegister(
                    Box::new(value),
                ));
            }
        }
        self.row.fields.push(field);
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
        value: StateSnapshotFieldValueRef<'_>,
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
        self.push_state_field(field_name, wire_value);
        Ok(())
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
            ..proto::SnapshotField::default()
        };
        let boxed_history = Box::new(history);
        match self {
            Self::LatestValueWins => {
                field.value = Some(proto::snapshot_field::Value::LatestValueWins(boxed_history));
            }
            Self::LinearString => {
                field.value = Some(proto::snapshot_field::Value::LinearString(boxed_history));
            }
            Self::LinearList => {
                field.value = Some(proto::snapshot_field::Value::LinearList(boxed_history));
            }
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
            SnapshotNode<UpdateIdWithIndex, NullableBasicValue>,
        >,
    >,
    value_type: &'schema NullableBasicDataType,
}

impl<'value> SnapshotSink<UpdateIdWithIndex, NullableBasicValueRef<'value>>
    for LatestValueWinsHistoryFieldSink<'_, '_>
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
        node: SnapshotNodeRef<'_, UpdateIdWithIndex, NullableBasicValueRef<'value>>,
    ) -> Result<(), Self::Error> {
        self.state
            .as_mut()
            .context(HistoryFieldClosedSnafu)?
            .push_node(index, owned_latest_value_wins_node(&node))
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
    state: Option<HistoryFieldEncoderState<'schema, 'row, SnapshotNode<UpdateIdWithIndex, String>>>,
}

impl SnapshotSink<UpdateIdWithIndex, str> for LinearStringHistoryFieldSink<'_, '_> {
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
        node: SnapshotNodeRef<'_, UpdateIdWithIndex, str>,
    ) -> Result<(), Self::Error> {
        self.state
            .as_mut()
            .context(HistoryFieldClosedSnafu)?
            .push_node(index, owned_linear_string_node(&node))
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
            SnapshotNode<UpdateIdWithIndex, PrimitiveValueArray>,
        >,
    >,
    value_type: PrimitiveType,
}

impl SnapshotSink<UpdateIdWithIndex, PrimitiveValueArrayRef<'_>>
    for LinearListHistoryFieldSink<'_, '_>
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
        node: SnapshotNodeRef<'_, UpdateIdWithIndex, PrimitiveValueArrayRef<'_>>,
    ) -> Result<(), Self::Error> {
        self.state
            .as_mut()
            .context(HistoryFieldClosedSnafu)?
            .push_node(index, owned_linear_list_node(&node))
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
    ///
    /// # Errors
    ///
    /// See `SnapshotAdapterError` for failure conditions.
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
    ) -> Result<StateSnapshotFieldValue, Self::Error> {
        let value = self.take_field_value(field_name)?;
        let wire = match value {
            proto::snapshot_field::Value::MonotonicCounter(value) => {
                StateSnapshotWireValue::MonotonicCounter(*value)
            }
            proto::snapshot_field::Value::TotalOrderRegister(value) => {
                StateSnapshotWireValue::TotalOrderRegister(*value)
            }
            proto::snapshot_field::Value::TotalOrderFiniteStateRegister(value) => {
                StateSnapshotWireValue::TotalOrderFiniteStateRegister(*value)
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
        let nodes =
            decode_columnar_latest_value_wins_history_snapshot(*history, value_type.clone())
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
            nodes: decode_columnar_linear_string_history_snapshot(*history)
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
            nodes: decode_columnar_linear_list_history_snapshot(*history, value_type)
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
    nodes: std::vec::IntoIter<SnapshotNode<UpdateIdWithIndex, NullableBasicValue>>,
}

impl SnapshotNodeSource<UpdateIdWithIndex, NullableBasicValue>
    for LatestValueWinsHistoryFieldSource
{
    type Error = SnapshotAdapterError;

    fn next_node(
        &mut self,
    ) -> Result<Option<SnapshotNode<UpdateIdWithIndex, NullableBasicValue>>, Self::Error> {
        Ok(self.nodes.next())
    }
}

/// Lazy node source for a `LinearString` history field decoded from protobuf.
pub struct LinearStringHistoryFieldSource {
    nodes: std::vec::IntoIter<SnapshotNode<UpdateIdWithIndex, String>>,
}

impl SnapshotNodeSource<UpdateIdWithIndex, String> for LinearStringHistoryFieldSource {
    type Error = SnapshotAdapterError;

    fn next_node(
        &mut self,
    ) -> Result<Option<SnapshotNode<UpdateIdWithIndex, String>>, Self::Error> {
        Ok(self.nodes.next())
    }
}

/// Lazy node source for a `LinearList` history field decoded from protobuf.
pub struct LinearListHistoryFieldSource {
    nodes: std::vec::IntoIter<PrimitiveValueArrayNode>,
}

type PrimitiveValueArrayNode = SnapshotNode<UpdateIdWithIndex, PrimitiveValueArray>;

impl SnapshotNodeSource<UpdateIdWithIndex, PrimitiveValueArray> for LinearListHistoryFieldSource {
    type Error = SnapshotAdapterError;

    fn next_node(&mut self) -> Result<Option<PrimitiveValueArrayNode>, Self::Error> {
        Ok(self.nodes.next())
    }
}

fn owned_latest_value_wins_node(
    node: &SnapshotNodeRef<'_, UpdateIdWithIndex, NullableBasicValueRef<'_>>,
) -> SnapshotNode<UpdateIdWithIndex, NullableBasicValue> {
    SnapshotNode {
        id: node.id.clone(),
        left: node.left.cloned(),
        right: node.right.cloned(),
        deleted: node.deleted,
        value: node.value.map(|value| value.clone().into_owned()),
    }
}

fn owned_linear_string_node(
    node: &SnapshotNodeRef<'_, UpdateIdWithIndex, str>,
) -> SnapshotNode<UpdateIdWithIndex, String> {
    SnapshotNode {
        id: node.id.clone(),
        left: node.left.cloned(),
        right: node.right.cloned(),
        deleted: node.deleted,
        value: node.value.map(str::to_owned),
    }
}

fn owned_linear_list_node(
    node: &SnapshotNodeRef<'_, UpdateIdWithIndex, PrimitiveValueArrayRef<'_>>,
) -> PrimitiveValueArrayNode {
    SnapshotNode {
        id: node.id.clone(),
        left: node.left.cloned(),
        right: node.right.cloned(),
        deleted: node.deleted,
        value: node.value.map(|value| value.clone().into_owned()),
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
