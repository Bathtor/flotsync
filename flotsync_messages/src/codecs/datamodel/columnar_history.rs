use super::*;
use crate::datamodel as proto;
use flotsync_data_types::{
    schema::{
        BasicDataType,
        NullableBasicDataType,
        PrimitiveType,
        ReplicatedDataType,
        datamodel::{
            BasicValue as ModelBasicValue,
            BasicValueRef as ModelBasicValueRef,
            DataModelValueError,
            NullableBasicValue as ModelNullableBasicValue,
            NullableBasicValueRef as ModelNullableBasicValueRef,
            PrimitiveValueArrayRef as ModelPrimitiveValueArrayRef,
            validation::ensure_snapshot_history_value_type,
        },
        values::{
            PrimitiveValue as ModelPrimitiveValue,
            PrimitiveValueArray as ModelPrimitiveValueArray,
            PrimitiveValueRef as ModelPrimitiveValueRef,
        },
    },
    snapshot::SnapshotNode,
};
use snafu::prelude::*;

/// Errors encoding and decoding the columnar history snapshot format.
#[derive(Debug, Snafu)]
pub enum ColumnarHistoryCodecError {
    #[snafu(display("Shared datamodel protobuf codec failed."))]
    Codec { source: CodecError },
    #[snafu(display("Snapshot payload does not match the schema data type."))]
    InvalidSnapshotValue { source: DataModelValueError },
    #[snafu(display("History snapshot node metadata is invalid: {reason}"))]
    InvalidNodeMeta { reason: String },
    #[snafu(display(
        "History snapshot value buffer has primitive type {actual:?}, expected {expected:?}."
    ))]
    InvalidValueBufferType {
        expected: PrimitiveType,
        actual: PrimitiveType,
    },
    #[snafu(display("History snapshot expected {expected} but found {actual}."))]
    InvalidValueBufferKind {
        expected: &'static str,
        actual: &'static str,
    },
    #[snafu(display(
        "History snapshot value span offset={offset}, len={value_len} exceeds buffer length {buffer_len}."
    ))]
    InvalidValueSpan {
        offset: usize,
        value_len: usize,
        buffer_len: usize,
    },
    #[snafu(display(
        "History snapshot string span offset={offset}, len={value_len} is invalid for buffer length {buffer_len}."
    ))]
    InvalidStringSpan {
        offset: usize,
        value_len: usize,
        buffer_len: usize,
    },
    #[snafu(display(
        "History snapshot value buffer length {len} exceeds the maximum u32-addressable size."
    ))]
    ValueBufferTooLarge { len: usize },
}

type ColumnarResult<T> = Result<T, ColumnarHistoryCodecError>;

#[derive(Clone, Debug, PartialEq, Eq)]
enum ColumnarHistoryNodeValue {
    LatestValueWins(ModelNullableBasicValue),
    LinearString(String),
    LinearList(ModelPrimitiveValueArray),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ColumnarHistoryNodeValueRef<'a> {
    LatestValueWins(ModelNullableBasicValueRef<'a>),
    LinearString(&'a str),
    LinearList(ModelPrimitiveValueArrayRef<'a>),
}

struct ColumnarHistoryNodeRecord<'a> {
    id: proto::HistoryId,
    left: Option<proto::HistoryId>,
    right: Option<proto::HistoryId>,
    deleted: bool,
    value: Option<ColumnarHistoryNodeValueRef<'a>>,
}

struct ColumnarValueBuffer {
    values: ColumnarValueBufferKind,
}

enum ColumnarValueBufferKind {
    Primitive(ModelPrimitiveValueArray),
    String(String),
}

impl ColumnarValueBuffer {
    fn new(data_type: &ReplicatedDataType) -> ColumnarResult<Self> {
        let values = match data_type {
            ReplicatedDataType::LinearString => ColumnarValueBufferKind::String(String::new()),
            _ => {
                let primitive_type = history_primitive_type(data_type)?;
                let primitive_empty_value = match primitive_type {
                    PrimitiveType::String => ModelPrimitiveValueArray::String(Vec::new()),
                    PrimitiveType::UInt => ModelPrimitiveValueArray::UInt(Vec::new()),
                    PrimitiveType::Int => ModelPrimitiveValueArray::Int(Vec::new()),
                    PrimitiveType::Byte => ModelPrimitiveValueArray::Byte(Vec::new()),
                    PrimitiveType::Float => ModelPrimitiveValueArray::Float(Vec::new()),
                    PrimitiveType::Boolean => ModelPrimitiveValueArray::Boolean(Vec::new()),
                    PrimitiveType::Binary => ModelPrimitiveValueArray::Binary(Vec::new()),
                    PrimitiveType::Date => ModelPrimitiveValueArray::Date(Vec::new()),
                    PrimitiveType::Timestamp => ModelPrimitiveValueArray::Timestamp(Vec::new()),
                };
                ColumnarValueBufferKind::Primitive(primitive_empty_value)
            }
        };
        Ok(Self { values })
    }

    fn primitive(values: ModelPrimitiveValueArray) -> Self {
        Self {
            values: ColumnarValueBufferKind::Primitive(values),
        }
    }

    fn string(values: String) -> Self {
        Self {
            values: ColumnarValueBufferKind::String(values),
        }
    }

    fn len(&self) -> usize {
        match &self.values {
            ColumnarValueBufferKind::Primitive(values) => values.len(),
            ColumnarValueBufferKind::String(values) => values.len(),
        }
    }

    fn append_primitive(&mut self, value: ModelPrimitiveValueRef<'_>) -> ColumnarResult<usize> {
        let ColumnarValueBufferKind::Primitive(values) = &mut self.values else {
            return InvalidValueBufferKindSnafu {
                expected: "primitive_values",
                actual: "string_values",
            }
            .fail();
        };
        match (values, value) {
            (ModelPrimitiveValueArray::String(values), ModelPrimitiveValueRef::String(value)) => {
                values.push(value.to_owned())
            }
            (ModelPrimitiveValueArray::UInt(values), ModelPrimitiveValueRef::UInt(value)) => {
                values.push(value)
            }
            (ModelPrimitiveValueArray::Int(values), ModelPrimitiveValueRef::Int(value)) => {
                values.push(value)
            }
            (ModelPrimitiveValueArray::Byte(values), ModelPrimitiveValueRef::Byte(value)) => {
                values.push(value)
            }
            (ModelPrimitiveValueArray::Float(values), ModelPrimitiveValueRef::Float(value)) => {
                values.push(value)
            }
            (ModelPrimitiveValueArray::Boolean(values), ModelPrimitiveValueRef::Boolean(value)) => {
                values.push(value)
            }
            (ModelPrimitiveValueArray::Binary(values), ModelPrimitiveValueRef::Binary(value)) => {
                values.push(value.to_vec())
            }
            (ModelPrimitiveValueArray::Date(values), ModelPrimitiveValueRef::Date(value)) => {
                values.push(value)
            }
            (
                ModelPrimitiveValueArray::Timestamp(values),
                ModelPrimitiveValueRef::Timestamp(value),
            ) => values.push(value),
            (values, value) => {
                return InvalidValueBufferTypeSnafu {
                    expected: values.primitive_type(),
                    actual: value.primitive_type(),
                }
                .fail();
            }
        }
        u32::try_from(self.len())
            .map_err(|_| ColumnarHistoryCodecError::ValueBufferTooLarge { len: self.len() })?;
        Ok(1)
    }

    fn append_array(&mut self, value: ModelPrimitiveValueArrayRef<'_>) -> ColumnarResult<usize> {
        let value_len = array_ref_len(&value);
        u32::try_from(value_len)
            .map_err(|_| ColumnarHistoryCodecError::ValueBufferTooLarge { len: value_len })?;

        let ColumnarValueBufferKind::Primitive(values) = &mut self.values else {
            return InvalidValueBufferKindSnafu {
                expected: "primitive_values",
                actual: "string_values",
            }
            .fail();
        };
        match (values, value) {
            (
                ModelPrimitiveValueArray::String(values),
                ModelPrimitiveValueArrayRef::String(value),
            ) => values.extend_from_slice(value),
            (ModelPrimitiveValueArray::UInt(values), ModelPrimitiveValueArrayRef::UInt(value)) => {
                values.extend_from_slice(value)
            }
            (ModelPrimitiveValueArray::Int(values), ModelPrimitiveValueArrayRef::Int(value)) => {
                values.extend_from_slice(value)
            }
            (ModelPrimitiveValueArray::Byte(values), ModelPrimitiveValueArrayRef::Byte(value)) => {
                values.extend_from_slice(value)
            }
            (
                ModelPrimitiveValueArray::Float(values),
                ModelPrimitiveValueArrayRef::Float(value),
            ) => values.extend_from_slice(value),
            (
                ModelPrimitiveValueArray::Boolean(values),
                ModelPrimitiveValueArrayRef::Boolean(value),
            ) => values.extend_from_slice(value),
            (
                ModelPrimitiveValueArray::Binary(values),
                ModelPrimitiveValueArrayRef::Binary(value),
            ) => values.extend_from_slice(value),
            (ModelPrimitiveValueArray::Date(values), ModelPrimitiveValueArrayRef::Date(value)) => {
                values.extend_from_slice(value)
            }
            (
                ModelPrimitiveValueArray::Timestamp(values),
                ModelPrimitiveValueArrayRef::Timestamp(value),
            ) => values.extend_from_slice(value),
            (values, value) => {
                return InvalidValueBufferTypeSnafu {
                    expected: values.primitive_type(),
                    actual: value.primitive_type(),
                }
                .fail();
            }
        }

        u32::try_from(self.len())
            .map_err(|_| ColumnarHistoryCodecError::ValueBufferTooLarge { len: self.len() })?;
        Ok(value_len)
    }

    fn append_string(&mut self, value: &str) -> ColumnarResult<usize> {
        let value_len = value.len();
        u32::try_from(value_len)
            .map_err(|_| ColumnarHistoryCodecError::ValueBufferTooLarge { len: value_len })?;
        let ColumnarValueBufferKind::String(values) = &mut self.values else {
            return InvalidValueBufferKindSnafu {
                expected: "string_values",
                actual: "primitive_values",
            }
            .fail();
        };
        values.push_str(value);
        u32::try_from(self.len())
            .map_err(|_| ColumnarHistoryCodecError::ValueBufferTooLarge { len: self.len() })?;
        Ok(value_len)
    }

    fn into_proto_values(self) -> proto::history_snapshot::Values {
        match self.values {
            ColumnarValueBufferKind::Primitive(values) => {
                proto::history_snapshot::Values::PrimitiveValues(encode_primitive_array(
                    values.as_ref(),
                ))
            }
            ColumnarValueBufferKind::String(values) => {
                proto::history_snapshot::Values::StringValues(values)
            }
        }
    }

    fn as_primitive(&self) -> ColumnarResult<&ModelPrimitiveValueArray> {
        let ColumnarValueBufferKind::Primitive(values) = &self.values else {
            return InvalidValueBufferKindSnafu {
                expected: "primitive_values",
                actual: "string_values",
            }
            .fail();
        };
        Ok(values)
    }

    fn as_string(&self) -> ColumnarResult<&str> {
        let ColumnarValueBufferKind::String(values) = &self.values else {
            return InvalidValueBufferKindSnafu {
                expected: "string_values",
                actual: "primitive_values",
            }
            .fail();
        };
        Ok(values)
    }
}

/// Encode a columnar history snapshot for a LatestValueWins field.
pub fn encode_columnar_latest_value_wins_history_snapshot(
    nodes: &[SnapshotNode<UpdateIdWithIndex, ModelNullableBasicValue>],
    value_type: &NullableBasicDataType,
) -> ColumnarResult<proto::HistorySnapshot> {
    let data_type = ReplicatedDataType::LatestValueWins {
        value_type: value_type.clone(),
    };
    let records = nodes.iter().map(|node| ColumnarHistoryNodeRecord {
        id: encode_indexed_update_id(&node.id),
        left: node.left.as_ref().map(encode_indexed_update_id),
        right: node.right.as_ref().map(encode_indexed_update_id),
        deleted: node.deleted,
        value: node
            .value
            .as_ref()
            .map(|value| ColumnarHistoryNodeValueRef::LatestValueWins(value.as_ref())),
    });
    encode_columnar_history_snapshot(&data_type, records)
}

/// Decode a columnar history snapshot for a LatestValueWins field.
pub fn decode_columnar_latest_value_wins_history_snapshot(
    snapshot: proto::HistorySnapshot,
    value_type: NullableBasicDataType,
) -> ColumnarResult<Vec<SnapshotNode<UpdateIdWithIndex, ModelNullableBasicValue>>> {
    let data_type = ReplicatedDataType::LatestValueWins { value_type };
    let decoded = decode_columnar_history_snapshot(&data_type, snapshot, decode_indexed_update_id)?;
    decoded
        .into_iter()
        .map(|node| match node.value {
            Some(ColumnarHistoryNodeValue::LatestValueWins(value)) => Ok(SnapshotNode {
                id: node.id,
                left: node.left,
                right: node.right,
                deleted: node.deleted,
                value: Some(value),
            }),
            None => Ok(SnapshotNode {
                id: node.id,
                left: node.left,
                right: node.right,
                deleted: node.deleted,
                value: None,
            }),
            _ => Err(ColumnarHistoryCodecError::InvalidSnapshotValue {
                source: DataModelValueError::InvalidSnapshotValueForType,
            }),
        })
        .collect()
}

/// Encode a columnar history snapshot for a LinearString field.
pub fn encode_columnar_linear_string_history_snapshot(
    nodes: &[SnapshotNode<UpdateIdWithIndex, String>],
) -> ColumnarResult<proto::HistorySnapshot> {
    let data_type = ReplicatedDataType::LinearString;
    let records = nodes.iter().map(|node| ColumnarHistoryNodeRecord {
        id: encode_indexed_update_id(&node.id),
        left: node.left.as_ref().map(encode_indexed_update_id),
        right: node.right.as_ref().map(encode_indexed_update_id),
        deleted: node.deleted,
        value: node
            .value
            .as_deref()
            .map(ColumnarHistoryNodeValueRef::LinearString),
    });
    encode_columnar_history_snapshot(&data_type, records)
}

/// Decode a columnar history snapshot for a LinearString field.
pub fn decode_columnar_linear_string_history_snapshot(
    snapshot: proto::HistorySnapshot,
) -> ColumnarResult<Vec<SnapshotNode<UpdateIdWithIndex, String>>> {
    let data_type = ReplicatedDataType::LinearString;
    let decoded = decode_columnar_history_snapshot(&data_type, snapshot, decode_indexed_update_id)?;
    decoded
        .into_iter()
        .map(|node| match node.value {
            Some(ColumnarHistoryNodeValue::LinearString(value)) => Ok(SnapshotNode {
                id: node.id,
                left: node.left,
                right: node.right,
                deleted: node.deleted,
                value: Some(value),
            }),
            None => Ok(SnapshotNode {
                id: node.id,
                left: node.left,
                right: node.right,
                deleted: node.deleted,
                value: None,
            }),
            _ => Err(ColumnarHistoryCodecError::InvalidSnapshotValue {
                source: DataModelValueError::InvalidSnapshotValueForType,
            }),
        })
        .collect()
}

/// Encode a columnar history snapshot for a LinearList field.
pub fn encode_columnar_linear_list_history_snapshot(
    nodes: &[SnapshotNode<UpdateIdWithIndex, ModelPrimitiveValueArray>],
    value_type: PrimitiveType,
) -> ColumnarResult<proto::HistorySnapshot> {
    let data_type = ReplicatedDataType::LinearList { value_type };
    let records = nodes.iter().map(|node| ColumnarHistoryNodeRecord {
        id: encode_indexed_update_id(&node.id),
        left: node.left.as_ref().map(encode_indexed_update_id),
        right: node.right.as_ref().map(encode_indexed_update_id),
        deleted: node.deleted,
        value: node
            .value
            .as_ref()
            .map(|value| ColumnarHistoryNodeValueRef::LinearList(value.as_ref())),
    });
    encode_columnar_history_snapshot(&data_type, records)
}

/// Decode a columnar history snapshot for a LinearList field.
pub fn decode_columnar_linear_list_history_snapshot(
    snapshot: proto::HistorySnapshot,
    value_type: PrimitiveType,
) -> ColumnarResult<Vec<SnapshotNode<UpdateIdWithIndex, ModelPrimitiveValueArray>>> {
    let data_type = ReplicatedDataType::LinearList { value_type };
    let decoded = decode_columnar_history_snapshot(&data_type, snapshot, decode_indexed_update_id)?;
    decoded
        .into_iter()
        .map(|node| match node.value {
            Some(ColumnarHistoryNodeValue::LinearList(value)) => Ok(SnapshotNode {
                id: node.id,
                left: node.left,
                right: node.right,
                deleted: node.deleted,
                value: Some(value),
            }),
            None => Ok(SnapshotNode {
                id: node.id,
                left: node.left,
                right: node.right,
                deleted: node.deleted,
                value: None,
            }),
            _ => Err(ColumnarHistoryCodecError::InvalidSnapshotValue {
                source: DataModelValueError::InvalidSnapshotValueForType,
            }),
        })
        .collect()
}

fn encode_columnar_history_snapshot<'a>(
    data_type: &ReplicatedDataType,
    nodes: impl IntoIterator<Item = ColumnarHistoryNodeRecord<'a>>,
) -> ColumnarResult<proto::HistorySnapshot> {
    let mut values = ColumnarValueBuffer::new(data_type)?;
    let mut metas: Vec<proto::HistoryNodeMeta> = Vec::new();

    for node in nodes {
        let (value_len, value_is_null) = match node.value {
            None => (0usize, false),
            Some(value) => encode_value_span(data_type, value, &mut values)?,
        };
        let mut meta = proto::HistoryNodeMeta::new();
        set_self_id_fields(&mut meta, node.id);
        set_origin_left_fields(&mut meta, node.left);
        set_origin_right_fields(&mut meta, node.right);
        meta.deleted = node.deleted;
        meta.value_len = u32::try_from(value_len)
            .map_err(|_| ColumnarHistoryCodecError::ValueBufferTooLarge { len: value_len })?;
        meta.value_is_null = value_is_null;
        metas.push(meta);
    }

    Ok(proto::HistorySnapshot {
        nodes: metas,
        values: Some(values.into_proto_values()),
        ..proto::HistorySnapshot::new()
    })
}

fn decode_columnar_history_snapshot<Id>(
    data_type: &ReplicatedDataType,
    mut snapshot: proto::HistorySnapshot,
    decode_id: impl Fn(proto::HistoryId) -> Result<Id, CodecError>,
) -> ColumnarResult<Vec<SnapshotNode<Id, ColumnarHistoryNodeValue>>> {
    let values = decode_columnar_value_buffer(data_type, snapshot.values.take())?;
    let mut next_value_offset = 0usize;
    let mut decoded = Vec::with_capacity(snapshot.nodes.len());

    for meta in snapshot.nodes {
        let node = decode_columnar_history_node(
            data_type,
            meta,
            &values,
            &mut next_value_offset,
            &decode_id,
        )?;
        decoded.push(node);
    }

    let value_buffer_len = values.len();
    ensure!(
        next_value_offset == value_buffer_len,
        InvalidNodeMetaSnafu {
            reason: format!(
                "history snapshot consumed {next_value_offset} value buffer entries, expected {value_buffer_len}"
            ),
        }
    );

    Ok(decoded)
}

fn decode_columnar_value_buffer(
    data_type: &ReplicatedDataType,
    values: Option<proto::history_snapshot::Values>,
) -> ColumnarResult<ColumnarValueBuffer> {
    let values = values
        .context(MissingOneofSnafu {
            name: "HistorySnapshot.values",
        })
        .context(CodecSnafu)?;

    match values {
        proto::history_snapshot::Values::PrimitiveValues(values) => {
            ensure!(
                !matches!(data_type, ReplicatedDataType::LinearString),
                InvalidValueBufferKindSnafu {
                    expected: "string_values",
                    actual: "primitive_values",
                }
            );
            let values = decode_primitive_array(values).context(CodecSnafu)?;
            let expected_primitive_type = history_primitive_type(data_type)?;
            ensure!(
                values.primitive_type() == expected_primitive_type,
                InvalidValueBufferTypeSnafu {
                    expected: expected_primitive_type,
                    actual: values.primitive_type(),
                }
            );
            Ok(ColumnarValueBuffer::primitive(values))
        }
        proto::history_snapshot::Values::StringValues(values) => {
            ensure!(
                matches!(data_type, ReplicatedDataType::LinearString),
                InvalidValueBufferKindSnafu {
                    expected: "primitive_values",
                    actual: "string_values",
                }
            );
            Ok(ColumnarValueBuffer::string(values))
        }
    }
}

fn decode_columnar_history_node<Id>(
    data_type: &ReplicatedDataType,
    meta: proto::HistoryNodeMeta,
    values: &ColumnarValueBuffer,
    next_value_offset: &mut usize,
    decode_id: impl Fn(proto::HistoryId) -> Result<Id, CodecError>,
) -> ColumnarResult<SnapshotNode<Id, ColumnarHistoryNodeValue>> {
    let id = decode_id(proto::HistoryId {
        version: meta.version,
        node_index: meta.node_index,
        chunk_index: meta.chunk_index,
        ..proto::HistoryId::new()
    })
    .context(CodecSnafu)?;
    let left = decode_optional_origin_id(
        "origin_left",
        meta.origin_left_version,
        meta.origin_left_node_index,
        meta.origin_left_chunk_index,
        &decode_id,
    )?;
    let right = decode_optional_origin_id(
        "origin_right",
        meta.origin_right_version,
        meta.origin_right_node_index,
        meta.origin_right_chunk_index,
        &decode_id,
    )?;
    let is_boundary = left.is_none() || right.is_none();
    let value_len = usize::try_from(meta.value_len).map_err(|_| {
        ColumnarHistoryCodecError::InvalidNodeMeta {
            reason: "value_len does not fit into usize".to_owned(),
        }
    })?;
    let value = decode_value_span(
        data_type,
        values,
        next_value_offset,
        is_boundary,
        value_len,
        meta.value_is_null,
    )?;

    Ok(SnapshotNode {
        id,
        left,
        right,
        deleted: meta.deleted,
        value,
    })
}

fn encode_value_span(
    data_type: &ReplicatedDataType,
    value: ColumnarHistoryNodeValueRef<'_>,
    values: &mut ColumnarValueBuffer,
) -> ColumnarResult<(usize, bool)> {
    let normalized = normalize_history_value_ref(value.clone());
    ensure_snapshot_history_value_type(data_type, &normalized)
        .context(InvalidSnapshotValueSnafu)?;

    let encoded = match value {
        ColumnarHistoryNodeValueRef::LatestValueWins(ModelNullableBasicValueRef::Null) => {
            (0usize, true)
        }
        ColumnarHistoryNodeValueRef::LatestValueWins(ModelNullableBasicValueRef::Value(
            ModelBasicValueRef::Primitive(value),
        )) => (values.append_primitive(value)?, false),
        ColumnarHistoryNodeValueRef::LatestValueWins(ModelNullableBasicValueRef::Value(
            ModelBasicValueRef::Array(value),
        )) => (values.append_array(value)?, false),
        ColumnarHistoryNodeValueRef::LinearString(value) => (values.append_string(value)?, false),
        ColumnarHistoryNodeValueRef::LinearList(value) => (values.append_array(value)?, false),
    };

    Ok(encoded)
}

fn decode_value_span(
    data_type: &ReplicatedDataType,
    values: &ColumnarValueBuffer,
    next_value_offset: &mut usize,
    is_boundary: bool,
    value_len: usize,
    value_is_null: bool,
) -> ColumnarResult<Option<ColumnarHistoryNodeValue>> {
    if is_boundary {
        ensure!(
            !value_is_null,
            InvalidNodeMetaSnafu {
                reason: "boundary nodes must not mark their payload as null".to_owned(),
            }
        );
        ensure!(
            value_len == 0,
            InvalidNodeMetaSnafu {
                reason: "boundary nodes must have value_len = 0".to_owned(),
            }
        );
        return Ok(None);
    }

    if value_is_null {
        ensure!(
            value_len == 0,
            InvalidNodeMetaSnafu {
                reason: "null payloads must have value_len = 0".to_owned(),
            }
        );
        let value = ModelNullableBasicValue::Null;
        ensure_snapshot_history_value_type(data_type, &value.as_ref())
            .context(InvalidSnapshotValueSnafu)?;
        return Ok(Some(ColumnarHistoryNodeValue::LatestValueWins(value)));
    }

    let value_offset = *next_value_offset;
    let consumed_value_offset =
        value_offset
            .checked_add(value_len)
            .context(InvalidNodeMetaSnafu {
                reason: "value_len overflowed the running value cursor".to_owned(),
            })?;

    let decoded = match data_type {
        ReplicatedDataType::LatestValueWins { value_type } => {
            let values = values.as_primitive()?;
            let value = match value_type.value_type() {
                BasicDataType::Primitive(_) => {
                    ensure!(
                        value_len == 1,
                        InvalidNodeMetaSnafu {
                            reason: "primitive LatestValueWins payloads must have value_len = 1"
                                .to_owned(),
                        }
                    );
                    let primitive = slice_primitive_value(values, value_offset, value_len)?;
                    ModelNullableBasicValue::Value(ModelBasicValue::Primitive(primitive))
                }
                BasicDataType::Array(_) => {
                    let array = slice_primitive_array(values, value_offset, value_len)?;
                    ModelNullableBasicValue::Value(ModelBasicValue::Array(array))
                }
            };
            ensure_snapshot_history_value_type(data_type, &value.as_ref())
                .context(InvalidSnapshotValueSnafu)?;
            Ok(Some(ColumnarHistoryNodeValue::LatestValueWins(value)))
        }
        ReplicatedDataType::LinearString => {
            let value = slice_string_value(values.as_string()?, value_offset, value_len)?;
            let normalized = ModelNullableBasicValueRef::Value(ModelBasicValueRef::Primitive(
                ModelPrimitiveValueRef::String(value.as_str()),
            ));
            ensure_snapshot_history_value_type(data_type, &normalized)
                .context(InvalidSnapshotValueSnafu)?;
            Ok(Some(ColumnarHistoryNodeValue::LinearString(value)))
        }
        ReplicatedDataType::LinearList { .. } => {
            let values = values.as_primitive()?;
            let array = slice_primitive_array(values, value_offset, value_len)?;
            let normalized =
                ModelNullableBasicValueRef::Value(ModelBasicValueRef::Array(array.as_ref()));
            ensure_snapshot_history_value_type(data_type, &normalized)
                .context(InvalidSnapshotValueSnafu)?;
            Ok(Some(ColumnarHistoryNodeValue::LinearList(array)))
        }
        _ => Err(ColumnarHistoryCodecError::InvalidSnapshotValue {
            source: DataModelValueError::InvalidSnapshotValueForType,
        }),
    }?;

    *next_value_offset = consumed_value_offset;
    Ok(decoded)
}

fn normalize_history_value_ref(
    value: ColumnarHistoryNodeValueRef<'_>,
) -> ModelNullableBasicValueRef<'_> {
    match value {
        ColumnarHistoryNodeValueRef::LatestValueWins(value) => value,
        ColumnarHistoryNodeValueRef::LinearString(value) => ModelNullableBasicValueRef::Value(
            ModelBasicValueRef::Primitive(ModelPrimitiveValueRef::String(value)),
        ),
        ColumnarHistoryNodeValueRef::LinearList(value) => {
            ModelNullableBasicValueRef::Value(ModelBasicValueRef::Array(value))
        }
    }
}

fn history_primitive_type(data_type: &ReplicatedDataType) -> ColumnarResult<PrimitiveType> {
    match data_type {
        ReplicatedDataType::LatestValueWins { value_type } => match value_type.value_type() {
            BasicDataType::Primitive(value_type) => Ok(*value_type),
            BasicDataType::Array(array_type) => Ok(array_type.element_type),
        },
        ReplicatedDataType::LinearString => Ok(PrimitiveType::String),
        ReplicatedDataType::LinearList { value_type } => Ok(*value_type),
        _ => Err(ColumnarHistoryCodecError::InvalidSnapshotValue {
            source: DataModelValueError::InvalidSnapshotValueForType,
        }),
    }
}

fn set_self_id_fields(meta: &mut proto::HistoryNodeMeta, id: proto::HistoryId) {
    meta.version = id.version;
    meta.node_index = id.node_index;
    meta.chunk_index = id.chunk_index;
}

fn set_origin_left_fields(meta: &mut proto::HistoryNodeMeta, id: Option<proto::HistoryId>) {
    if let Some(id) = id {
        meta.origin_left_version = Some(id.version);
        meta.origin_left_node_index = Some(id.node_index);
        meta.origin_left_chunk_index = Some(id.chunk_index);
    }
}

fn set_origin_right_fields(meta: &mut proto::HistoryNodeMeta, id: Option<proto::HistoryId>) {
    if let Some(id) = id {
        meta.origin_right_version = Some(id.version);
        meta.origin_right_node_index = Some(id.node_index);
        meta.origin_right_chunk_index = Some(id.chunk_index);
    }
}

fn decode_optional_origin_id<Id>(
    side: &'static str,
    version: Option<u64>,
    node_index: Option<u32>,
    chunk_index: Option<u32>,
    decode_id: impl Fn(proto::HistoryId) -> Result<Id, CodecError>,
) -> ColumnarResult<Option<Id>> {
    match (version, node_index) {
        (None, None) => {
            ensure!(
                chunk_index.is_none(),
                InvalidNodeMetaSnafu {
                    reason: format!("{side} chunk_index was set without a matching id"),
                }
            );
            Ok(None)
        }
        (Some(version), Some(node_index)) => decode_id(proto::HistoryId {
            version,
            node_index,
            chunk_index: chunk_index.unwrap_or(0),
            ..proto::HistoryId::new()
        })
        .context(CodecSnafu)
        .map(Some),
        _ => InvalidNodeMetaSnafu {
            reason: format!("{side} version/node_index presence did not match"),
        }
        .fail(),
    }
}

fn slice_primitive_array(
    values: &ModelPrimitiveValueArray,
    value_offset: usize,
    value_len: usize,
) -> ColumnarResult<ModelPrimitiveValueArray> {
    let start = value_offset;
    let end = start
        .checked_add(value_len)
        .context(InvalidValueSpanSnafu {
            offset: value_offset,
            value_len,
            buffer_len: values.len(),
        })?;
    ensure!(
        end <= values.len(),
        InvalidValueSpanSnafu {
            offset: value_offset,
            value_len,
            buffer_len: values.len(),
        }
    );
    let slice = match values {
        ModelPrimitiveValueArray::String(values) => {
            ModelPrimitiveValueArray::String(values[start..end].to_vec())
        }
        ModelPrimitiveValueArray::UInt(values) => {
            ModelPrimitiveValueArray::UInt(values[start..end].to_vec())
        }
        ModelPrimitiveValueArray::Int(values) => {
            ModelPrimitiveValueArray::Int(values[start..end].to_vec())
        }
        ModelPrimitiveValueArray::Byte(values) => {
            ModelPrimitiveValueArray::Byte(values[start..end].to_vec())
        }
        ModelPrimitiveValueArray::Float(values) => {
            ModelPrimitiveValueArray::Float(values[start..end].to_vec())
        }
        ModelPrimitiveValueArray::Boolean(values) => {
            ModelPrimitiveValueArray::Boolean(values[start..end].to_vec())
        }
        ModelPrimitiveValueArray::Binary(values) => {
            ModelPrimitiveValueArray::Binary(values[start..end].to_vec())
        }
        ModelPrimitiveValueArray::Date(values) => {
            ModelPrimitiveValueArray::Date(values[start..end].to_vec())
        }
        ModelPrimitiveValueArray::Timestamp(values) => {
            ModelPrimitiveValueArray::Timestamp(values[start..end].to_vec())
        }
    };
    Ok(slice)
}

fn slice_string_value(
    values: &str,
    value_offset: usize,
    value_len: usize,
) -> ColumnarResult<String> {
    let start = value_offset;
    let end = start
        .checked_add(value_len)
        .context(InvalidStringSpanSnafu {
            offset: value_offset,
            value_len,
            buffer_len: values.len(),
        })?;
    ensure!(
        end <= values.len(),
        InvalidStringSpanSnafu {
            offset: value_offset,
            value_len,
            buffer_len: values.len(),
        }
    );
    let slice = values.get(start..end).context(InvalidStringSpanSnafu {
        offset: value_offset,
        value_len,
        buffer_len: values.len(),
    })?;
    Ok(slice.to_owned())
}

fn slice_primitive_value(
    values: &ModelPrimitiveValueArray,
    value_offset: usize,
    value_len: usize,
) -> ColumnarResult<ModelPrimitiveValue> {
    ensure!(
        value_len == 1,
        InvalidNodeMetaSnafu {
            reason: "scalar payloads must have value_len = 1".to_owned(),
        }
    );
    let index = value_offset;
    let value = match values {
        ModelPrimitiveValueArray::String(values) => {
            values.get(index).cloned().map(ModelPrimitiveValue::String)
        }
        ModelPrimitiveValueArray::UInt(values) => {
            values.get(index).copied().map(ModelPrimitiveValue::UInt)
        }
        ModelPrimitiveValueArray::Int(values) => {
            values.get(index).copied().map(ModelPrimitiveValue::Int)
        }
        ModelPrimitiveValueArray::Byte(values) => {
            values.get(index).copied().map(ModelPrimitiveValue::Byte)
        }
        ModelPrimitiveValueArray::Float(values) => {
            values.get(index).copied().map(ModelPrimitiveValue::Float)
        }
        ModelPrimitiveValueArray::Boolean(values) => {
            values.get(index).copied().map(ModelPrimitiveValue::Boolean)
        }
        ModelPrimitiveValueArray::Binary(values) => {
            values.get(index).cloned().map(ModelPrimitiveValue::Binary)
        }
        ModelPrimitiveValueArray::Date(values) => {
            values.get(index).copied().map(ModelPrimitiveValue::Date)
        }
        ModelPrimitiveValueArray::Timestamp(values) => values
            .get(index)
            .copied()
            .map(ModelPrimitiveValue::Timestamp),
    };
    value.context(InvalidValueSpanSnafu {
        offset: value_offset,
        value_len,
        buffer_len: values.len(),
    })
}

fn array_ref_len(values: &ModelPrimitiveValueArrayRef<'_>) -> usize {
    match values {
        ModelPrimitiveValueArrayRef::String(values) => values.len(),
        ModelPrimitiveValueArrayRef::UInt(values) => values.len(),
        ModelPrimitiveValueArrayRef::Int(values) => values.len(),
        ModelPrimitiveValueArrayRef::Byte(values) => values.len(),
        ModelPrimitiveValueArrayRef::Float(values) => values.len(),
        ModelPrimitiveValueArrayRef::Boolean(values) => values.len(),
        ModelPrimitiveValueArrayRef::Binary(values) => values.len(),
        ModelPrimitiveValueArrayRef::Date(values) => values.len(),
        ModelPrimitiveValueArrayRef::Timestamp(values) => values.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flotsync_core::versions::UpdateId;
    use flotsync_data_types::{
        any_data::{LinearLatestValueWins, list::LinearList},
        schema::{ArrayType, BasicDataType, NullableBasicDataType},
        snapshot::{SnapshotHeader, SnapshotNodeRef, SnapshotSink},
        text::LinearString,
    };
    use std::assert_matches;

    fn latest_value_wins_array_fixture() -> (
        NullableBasicDataType,
        Vec<SnapshotNode<UpdateIdWithIndex, ModelNullableBasicValue>>,
    ) {
        let value_type =
            NullableBasicDataType::Nullable(BasicDataType::Array(Box::new(ArrayType {
                element_type: PrimitiveType::Int,
            })));
        let mut latest = LinearLatestValueWins::new(
            ModelNullableBasicValue::Value(ModelBasicValue::Array(ModelPrimitiveValueArray::Int(
                vec![1, 2, 3],
            ))),
            [
                IdWithIndex::zero(UpdateId {
                    version: 1,
                    node_index: 0,
                }),
                IdWithIndex::zero(UpdateId {
                    version: 1,
                    node_index: 1,
                }),
                IdWithIndex::zero(UpdateId {
                    version: 1,
                    node_index: 2,
                }),
            ],
        );
        latest.update(
            IdWithIndex::zero(UpdateId {
                version: 2,
                node_index: 0,
            }),
            ModelNullableBasicValue::Value(ModelBasicValue::Array(ModelPrimitiveValueArray::Int(
                Vec::new(),
            ))),
        );
        latest.update(
            IdWithIndex::zero(UpdateId {
                version: 3,
                node_index: 0,
            }),
            ModelNullableBasicValue::Null,
        );

        let mut sink = CollectSink::new(Clone::clone);
        latest.encode_snapshot(&mut sink).unwrap();
        (value_type, sink.into_nodes())
    }

    fn linear_string_tombstone_fixture() -> Vec<SnapshotNode<UpdateIdWithIndex, String>> {
        let mut value = LinearString::with_value(
            "alpha beta gamma".to_owned(),
            UpdateId {
                version: 10,
                node_index: 0,
            },
        );
        let range = value.ids_in_range(6..10).unwrap();
        range.delete(&mut value).unwrap();
        value.append(
            IdWithIndex::zero(UpdateId {
                version: 11,
                node_index: 0,
            }),
            " delta".to_owned(),
        );

        let mut sink = CollectSink::new(str::to_owned);
        value.encode_snapshot(&mut sink).unwrap();
        sink.into_nodes()
    }

    fn linear_string_utf8_fixture() -> Vec<SnapshotNode<UpdateIdWithIndex, String>> {
        let mut value = LinearString::with_value(
            "a\u{00E9}\u{1F642}".to_owned(),
            UpdateId {
                version: 12,
                node_index: 0,
            },
        );
        value.append(
            IdWithIndex::zero(UpdateId {
                version: 13,
                node_index: 0,
            }),
            "\u{6F22}z".to_owned(),
        );

        let mut sink = CollectSink::new(str::to_owned);
        value.encode_snapshot(&mut sink).unwrap();
        sink.into_nodes()
    }

    fn linear_list_tombstone_fixture()
    -> Vec<SnapshotNode<UpdateIdWithIndex, ModelPrimitiveValueArray>> {
        let mut value = LinearList::with_values(
            [10i64, 20, 30, 40, 50],
            UpdateId {
                version: 20,
                node_index: 0,
            },
        );
        let range = value.ids_in_range(1..4).unwrap();
        range.delete(&mut value).unwrap();
        value.append(
            IdWithIndex::zero(UpdateId {
                version: 21,
                node_index: 0,
            }),
            [60, 70],
        );

        let mut sink =
            CollectSink::new(|value: &[i64]| ModelPrimitiveValueArray::Int(value.to_vec()));
        value.encode_snapshot(&mut sink).unwrap();
        sink.into_nodes()
    }

    fn assert_boundary_nodes<Id, Value>(nodes: &[SnapshotNode<Id, Value>]) {
        let first = nodes.first().unwrap();
        let last = nodes.last().unwrap();

        assert!(first.left.is_none());
        assert!(first.right.is_some());
        assert!(!first.deleted);
        assert!(first.value.is_none());

        assert!(last.left.is_some());
        assert!(last.right.is_none());
        assert!(!last.deleted);
        assert!(last.value.is_none());
    }

    struct CollectSink<Id, Borrowed: ?Sized, Owned, Mapper> {
        nodes: Vec<SnapshotNode<Id, Owned>>,
        map_value: Mapper,
        _marker: std::marker::PhantomData<fn(&Borrowed)>,
    }

    impl<Id, Borrowed: ?Sized, Owned, Mapper> CollectSink<Id, Borrowed, Owned, Mapper> {
        fn new(map_value: Mapper) -> Self {
            Self {
                nodes: Vec::new(),
                map_value,
                _marker: std::marker::PhantomData,
            }
        }

        fn into_nodes(self) -> Vec<SnapshotNode<Id, Owned>> {
            self.nodes
        }
    }

    impl<Id, Borrowed: ?Sized, Owned, Mapper> SnapshotSink<Id, Borrowed>
        for CollectSink<Id, Borrowed, Owned, Mapper>
    where
        Id: Clone,
        Mapper: FnMut(&Borrowed) -> Owned,
    {
        type Error = std::convert::Infallible;

        fn begin(&mut self, _header: SnapshotHeader) -> Result<(), Self::Error> {
            Ok(())
        }

        fn node(
            &mut self,
            _index: usize,
            node: SnapshotNodeRef<'_, Id, Borrowed>,
        ) -> Result<(), Self::Error> {
            self.nodes.push(SnapshotNode {
                id: node.id.clone(),
                left: node.left.cloned(),
                right: node.right.cloned(),
                deleted: node.deleted,
                value: node.value.map(|value| (self.map_value)(value)),
            });
            Ok(())
        }

        fn end(&mut self) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[test]
    fn latest_value_wins_array_history_roundtrips_and_reconstructs() {
        let (value_type, nodes) = latest_value_wins_array_fixture();
        assert_boundary_nodes(&nodes);

        let encoded =
            encode_columnar_latest_value_wins_history_snapshot(&nodes, &value_type).unwrap();
        let decoded =
            decode_columnar_latest_value_wins_history_snapshot(encoded, value_type.clone())
                .unwrap();

        assert_eq!(decoded, nodes);

        let reconstructed = LinearLatestValueWins::from_snapshot_nodes(
            decoded
                .into_iter()
                .map(Result::<_, ColumnarHistoryCodecError>::Ok),
        )
        .unwrap();
        reconstructed.validate_integrity().unwrap();
    }

    #[test]
    fn linear_string_history_roundtrips_and_reconstructs() {
        let nodes = linear_string_tombstone_fixture();
        assert_boundary_nodes(&nodes);
        assert!(nodes.iter().any(|node| node.deleted));

        let encoded = encode_columnar_linear_string_history_snapshot(&nodes).unwrap();
        let decoded = decode_columnar_linear_string_history_snapshot(encoded).unwrap();

        assert_eq!(decoded, nodes);

        let reconstructed = LinearString::from_snapshot_nodes(
            decoded
                .into_iter()
                .map(Result::<_, ColumnarHistoryCodecError>::Ok),
        )
        .unwrap();
        reconstructed.validate_integrity().unwrap();
    }

    #[test]
    fn linear_string_utf8_history_roundtrips_and_reconstructs() {
        let nodes = linear_string_utf8_fixture();
        assert_boundary_nodes(&nodes);

        let encoded = encode_columnar_linear_string_history_snapshot(&nodes).unwrap();
        let decoded = decode_columnar_linear_string_history_snapshot(encoded).unwrap();

        assert_eq!(decoded, nodes);

        let reconstructed = LinearString::from_snapshot_nodes(
            decoded
                .into_iter()
                .map(Result::<_, ColumnarHistoryCodecError>::Ok),
        )
        .unwrap();
        reconstructed.validate_integrity().unwrap();
    }

    #[test]
    fn linear_list_history_roundtrips_and_reconstructs() {
        let nodes = linear_list_tombstone_fixture();
        assert_boundary_nodes(&nodes);
        assert!(nodes.iter().any(|node| node.deleted));

        let encoded =
            encode_columnar_linear_list_history_snapshot(&nodes, PrimitiveType::Int).unwrap();
        let decoded =
            decode_columnar_linear_list_history_snapshot(encoded, PrimitiveType::Int).unwrap();

        assert_eq!(decoded, nodes);

        let reconstructed = LinearList::from_snapshot_nodes(decoded.into_iter().map(|node| {
            let value = node
                .value
                .map(|value| match value {
                    ModelPrimitiveValueArray::Int(values) => Ok(values),
                    _ => Err(ColumnarHistoryCodecError::InvalidSnapshotValue {
                        source: DataModelValueError::InvalidSnapshotValueForType,
                    }),
                })
                .transpose()?;
            Ok::<SnapshotNode<UpdateIdWithIndex, Vec<i64>>, ColumnarHistoryCodecError>(
                SnapshotNode {
                    id: node.id,
                    left: node.left,
                    right: node.right,
                    deleted: node.deleted,
                    value,
                },
            )
        }))
        .unwrap();
        reconstructed.validate_integrity().unwrap();
    }

    #[test]
    fn decode_rejects_malformed_null_metadata() {
        let value_type =
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::String));
        let mut snapshot = proto::HistorySnapshot::new();
        snapshot.values = Some(proto::history_snapshot::Values::PrimitiveValues(
            encode_primitive_array(ModelPrimitiveValueArray::String(Vec::new()).as_ref()),
        ));
        snapshot.nodes.push(proto::HistoryNodeMeta {
            version: 1,
            node_index: 0,
            chunk_index: 0,
            origin_left_version: Some(0),
            origin_left_node_index: Some(0),
            origin_left_chunk_index: Some(0),
            origin_right_version: Some(2),
            origin_right_node_index: Some(0),
            origin_right_chunk_index: Some(0),
            deleted: false,
            value_len: 1,
            value_is_null: true,
            ..proto::HistoryNodeMeta::new()
        });

        let err =
            decode_columnar_latest_value_wins_history_snapshot(snapshot, value_type).unwrap_err();
        assert_matches!(err, ColumnarHistoryCodecError::InvalidNodeMeta { .. });
    }

    #[test]
    fn latest_value_wins_metadata_marks_nulls_empty_arrays_and_boundaries() {
        let (value_type, nodes) = latest_value_wins_array_fixture();
        let snapshot =
            encode_columnar_latest_value_wins_history_snapshot(&nodes, &value_type).unwrap();

        assert_matches!(
            snapshot.values,
            Some(proto::history_snapshot::Values::PrimitiveValues(_))
        );

        assert_eq!(
            snapshot
                .nodes
                .iter()
                .filter(|node| node.value_is_null)
                .count(),
            1
        );
        assert_eq!(
            snapshot
                .nodes
                .iter()
                .filter(|node| {
                    node.origin_left_version.is_some()
                        && node.origin_right_version.is_some()
                        && !node.value_is_null
                        && node.value_len == 0
                })
                .count(),
            1
        );

        let first = snapshot.nodes.first().unwrap();
        let last = snapshot.nodes.last().unwrap();
        assert!(first.origin_left_version.is_none() || first.origin_right_version.is_none());
        assert_eq!(first.value_len, 0);
        assert!(!first.value_is_null);
        assert!(last.origin_left_version.is_none() || last.origin_right_version.is_none());
        assert_eq!(last.value_len, 0);
        assert!(!last.value_is_null);
    }

    #[test]
    fn linear_string_uses_concatenated_string_buffer() {
        let nodes = linear_string_utf8_fixture();
        let snapshot = encode_columnar_linear_string_history_snapshot(&nodes).unwrap();

        match snapshot.values {
            Some(proto::history_snapshot::Values::StringValues(values)) => {
                assert_eq!(values, "a\u{00E9}\u{1F642}\u{6F22}z");
            }
            _ => panic!("expected string_values buffer"),
        }
    }

    #[test]
    fn decode_rejects_invalid_utf8_byte_slice_boundaries() {
        let mut snapshot = proto::HistorySnapshot::new();
        snapshot.values = Some(proto::history_snapshot::Values::StringValues(
            "a\u{00E9}".to_owned(),
        ));
        snapshot.nodes.push(proto::HistoryNodeMeta {
            version: 1,
            node_index: 0,
            chunk_index: 0,
            deleted: false,
            origin_right_version: Some(2),
            origin_right_node_index: Some(0),
            origin_right_chunk_index: Some(0),
            value_len: 0,
            value_is_null: false,
            ..proto::HistoryNodeMeta::new()
        });
        snapshot.nodes.push(proto::HistoryNodeMeta {
            version: 2,
            node_index: 0,
            chunk_index: 0,
            origin_left_version: Some(1),
            origin_left_node_index: Some(0),
            origin_left_chunk_index: Some(0),
            origin_right_version: Some(3),
            origin_right_node_index: Some(0),
            origin_right_chunk_index: Some(1),
            deleted: false,
            value_len: 1,
            value_is_null: false,
            ..proto::HistoryNodeMeta::new()
        });
        snapshot.nodes.push(proto::HistoryNodeMeta {
            version: 3,
            node_index: 0,
            chunk_index: 1,
            origin_left_version: Some(2),
            origin_left_node_index: Some(0),
            origin_left_chunk_index: Some(0),
            origin_right_version: Some(4),
            origin_right_node_index: Some(0),
            origin_right_chunk_index: Some(0),
            deleted: false,
            value_len: 1,
            value_is_null: false,
            ..proto::HistoryNodeMeta::new()
        });
        snapshot.nodes.push(proto::HistoryNodeMeta {
            version: 4,
            node_index: 0,
            chunk_index: 0,
            deleted: false,
            origin_left_version: Some(3),
            origin_left_node_index: Some(0),
            origin_left_chunk_index: Some(1),
            value_len: 0,
            value_is_null: false,
            ..proto::HistoryNodeMeta::new()
        });

        let err = decode_columnar_linear_string_history_snapshot(snapshot).unwrap_err();
        assert_matches!(err, ColumnarHistoryCodecError::InvalidStringSpan { .. });
    }

    #[test]
    fn tombstone_fixtures_preserve_deleted_node_metadata() {
        let string_snapshot =
            encode_columnar_linear_string_history_snapshot(&linear_string_tombstone_fixture())
                .unwrap();
        assert!(string_snapshot.nodes.iter().any(|node| node.deleted));

        let list_snapshot = encode_columnar_linear_list_history_snapshot(
            &linear_list_tombstone_fixture(),
            PrimitiveType::Int,
        )
        .unwrap();
        assert!(list_snapshot.nodes.iter().any(|node| node.deleted));
    }
}
