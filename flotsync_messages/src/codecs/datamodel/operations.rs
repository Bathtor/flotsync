use super::*;
use crate::{
    datamodel as proto,
    snapshots::datamodel::{
        ProtoSchemaSnapshotDecoder,
        ProtoSchemaSnapshotEncoder,
        SnapshotAdapterError,
    },
};
use flotsync_core::versions::UpdateId;
use flotsync_data_types::{
    DataOperation,
    IdWithIndex,
    any_data::UpdateOperation,
    schema::{
        Schema,
        datamodel::{self as model, RowOperation},
        values,
    },
};
use protobuf::MessageField;
use snafu::prelude::*;
use std::{borrow::Cow, collections::HashSet};
use uuid::Uuid;

type OperationResult<T> = Result<T, OperationCodecError>;

/// Errors converting schema operations to and from protobuf messages.
#[derive(Debug, Snafu)]
pub enum OperationCodecError {
    #[snafu(display("Shared datamodel protobuf codec failed."))]
    Codec { source: CodecError },
    #[snafu(display("Schema operation snapshot adapter failed."))]
    SnapshotAdapter { source: SnapshotAdapterError },
    #[snafu(display("Schema operation snapshot encoding failed."))]
    SnapshotEncode {
        source: model::RowSnapshotEncodeError<SnapshotAdapterError>,
    },
    #[snafu(display("Schema operation snapshot decoding failed."))]
    SnapshotDecode {
        source: model::RowSnapshotDecodeError<SnapshotAdapterError>,
    },
    #[snafu(display("Schema operation payload does not satisfy datamodel invariants."))]
    InvalidOperationValue { source: model::DataModelValueError },
    #[snafu(display("Schema operation payload does not match the schema."))]
    InvalidSchemaOperation { source: model::SchemaValueError },
    #[snafu(display(
        "Linear delete ranges must stay within one logical update id; start and end differed."
    ))]
    DeleteRangeCrossesUpdateBoundary,
    #[snafu(display("Row id bytes must contain one UUID, but received {len} bytes."))]
    InvalidRowIdBytes { len: usize, source: uuid::Error },
}

trait RequiredMessageFieldExt<T> {
    fn take_required(&mut self, message: &'static str, field: &'static str) -> OperationResult<T>;
}
impl<T> RequiredMessageFieldExt<T> for MessageField<T> {
    fn take_required(&mut self, message: &'static str, field: &'static str) -> OperationResult<T> {
        self.take()
            .context(MissingFieldSnafu { message, field })
            .context(CodecSnafu)
    }
}

trait RequiredOneofExt<T> {
    fn take_required_oneof(self, name: &'static str) -> OperationResult<T>;
}
impl<T> RequiredOneofExt<T> for Option<T> {
    fn take_required_oneof(self, name: &'static str) -> OperationResult<T> {
        self.context(MissingOneofSnafu { name }).context(CodecSnafu)
    }
}

/// Encode a schema operation into its protobuf transport form.
pub fn encode_schema_operation(
    operation: &model::SchemaOperation<'_, Uuid, UpdateId>,
    schema: &Schema,
) -> OperationResult<proto::SchemaOperation> {
    let mut encoded = proto::SchemaOperation::new();
    encoded.change_id = MessageField::some(encode_update_id(operation.change_id));
    match &operation.operation {
        RowOperation::Insert { row_id, snapshot } => {
            let mut insert = proto::InsertRowOperation::new();
            insert.row_id = encode_row_id(*row_id);
            insert.snapshot = MessageField::some(encode_row_snapshot(snapshot, schema)?);
            encoded.set_insert(insert);
        }
        RowOperation::Update { row_id, fields } => {
            let mut update = proto::UpdateRowOperation::new();
            update.row_id = encode_row_id(*row_id);
            update.fields = fields.iter().map(encode_operation_field).try_collect()?;
            encoded.set_update(update);
        }
        RowOperation::Delete { row_id } => {
            let mut delete = proto::DeleteRowOperation::new();
            delete.row_id = encode_row_id(*row_id);
            encoded.set_delete(delete);
        }
    }
    Ok(encoded)
}

/// Decode a schema operation from protobuf, validating it against the provided schema.
pub fn decode_schema_operation<'schema>(
    mut operation: proto::SchemaOperation,
    schema: &'schema Schema,
) -> OperationResult<model::SchemaOperation<'schema, Uuid, UpdateId>> {
    let change_id = operation
        .change_id
        .take_required("SchemaOperation", "change_id")
        .and_then(|id| decode_update_id(id).context(CodecSnafu))?;

    let operation = operation
        .operation
        .take()
        .take_required_oneof("SchemaOperation.operation")?;

    let operation = match operation {
        proto::schema_operation::Operation::Insert(insert) => {
            let operation = decode_insert_row_operation(insert, schema)?;
            model::SchemaOperation {
                change_id,
                operation,
            }
        }
        proto::schema_operation::Operation::Update(update) => {
            let operation = decode_update_row_operation(update, schema)?;
            model::SchemaOperation {
                change_id,
                operation,
            }
        }
        proto::schema_operation::Operation::Delete(delete) => {
            let operation = decode_delete_row_operation(delete)?;
            model::SchemaOperation {
                change_id,
                operation,
            }
        }
    };
    operation
        .validate_against_schema(schema)
        .context(InvalidSchemaOperationSnafu)?;
    Ok(operation)
}

fn encode_row_snapshot(
    snapshot: &model::RowSnapshot<'_, UpdateId>,
    schema: &Schema,
) -> OperationResult<proto::RowSnapshot> {
    let mut encoder = ProtoSchemaSnapshotEncoder::new(schema);
    snapshot
        .encode_snapshot(schema, &mut encoder)
        .context(SnapshotEncodeSnafu)?;
    encoder.into_row_snapshot().context(SnapshotAdapterSnafu)
}

fn decode_row_snapshot(
    snapshot: proto::RowSnapshot,
    schema: &Schema,
) -> OperationResult<model::RowSnapshot<'static, UpdateId>> {
    let mut seen_fields = HashSet::<String>::new();
    let mut decoded_fields = Vec::with_capacity(snapshot.fields.len());
    for field in snapshot.fields {
        let field_name = field.field_name.clone();
        if !seen_fields.insert(field_name.clone()) {
            return Err(OperationCodecError::InvalidSchemaOperation {
                source: model::SchemaValueError::DuplicateField { field_name },
            });
        }
        let Some(schema_field) = schema.columns.get(field_name.as_str()) else {
            return Err(OperationCodecError::InvalidSchemaOperation {
                source: model::SchemaValueError::UnknownField { field_name },
            });
        };
        let field_value = decode_single_row_snapshot_field(field, schema_field)?;
        decoded_fields.push((schema_field.name.clone(), field_value));
    }

    Ok(model::RowSnapshot::from_owned_fields(decoded_fields))
}

fn decode_single_row_snapshot_field(
    field: proto::SnapshotField,
    schema_field: &flotsync_data_types::schema::Field,
) -> OperationResult<model::InMemoryFieldValue<UpdateId>> {
    let single_field_schema = Schema::from_fields([schema_field.clone()]);
    let mut snapshot = proto::RowSnapshot::new();
    snapshot.fields.push(field);

    let mut decoder = ProtoSchemaSnapshotDecoder::new(snapshot).context(SnapshotAdapterSnafu)?;
    let decoded_snapshot = model::RowSnapshot::decode_snapshot(&single_field_schema, &mut decoder)
        .context(SnapshotDecodeSnafu)?;
    let mut fields = decoded_snapshot.into_owned_fields();
    let (_, field_value) = fields
        .pop()
        .expect("single-field snapshot decoding must produce exactly one field");
    Ok(field_value)
}

fn decode_insert_row_operation(
    mut operation: proto::InsertRowOperation,
    schema: &Schema,
) -> OperationResult<RowOperation<'static, Uuid, UpdateId>> {
    let row_id = decode_row_id(operation.row_id)?;
    let snapshot = operation
        .snapshot
        .take_required("InsertRowOperation", "snapshot")
        .and_then(|snapshot| decode_row_snapshot(snapshot, schema))?;
    Ok(RowOperation::Insert { row_id, snapshot })
}

fn decode_update_row_operation<'schema>(
    operation: proto::UpdateRowOperation,
    schema: &'schema Schema,
) -> OperationResult<RowOperation<'schema, Uuid, UpdateId>> {
    let row_id = decode_row_id(operation.row_id)?;
    let fields = operation
        .fields
        .into_iter()
        .map(|field| decode_operation_field(field, schema))
        .try_collect()?;
    Ok(RowOperation::Update { row_id, fields })
}

fn decode_delete_row_operation(
    operation: proto::DeleteRowOperation,
) -> OperationResult<RowOperation<'static, Uuid, UpdateId>> {
    let row_id = decode_row_id(operation.row_id)?;
    Ok(RowOperation::Delete { row_id })
}

/// Encode one schema-bound field operation.
pub fn encode_operation_field(
    field: &model::OperationFieldValue<'_, UpdateId>,
) -> OperationResult<proto::OperationField> {
    let mut encoded = proto::OperationField::new();
    encoded.field_name = field.field_name.to_string();
    match &field.value {
        model::OperationValue::LatestValueWins(value) => {
            encoded.set_latest_value_wins(encode_latest_value_wins_operation(value))
        }
        model::OperationValue::LinearString(values) => {
            let encoded_op = encode_linear_string_operation(values)?;
            encoded.set_linear_string(encoded_op)
        }
        model::OperationValue::LinearList(values) => {
            let encoded_op = encode_linear_list_operation(values)?;
            encoded.set_linear_list(encoded_op)
        }
        model::OperationValue::MonotonicCounterIncrement(value) => encoded
            .set_monotonic_counter_increment(encode_monotonic_counter_increment_operation(*value)),
        model::OperationValue::TotalOrderRegisterSet(value) => {
            encoded.set_total_order_register_set(encode_total_order_register_set_operation(value))
        }
        model::OperationValue::TotalOrderFiniteStateRegisterSet(value) => encoded
            .set_total_order_finite_state_register_set(
                encode_total_order_finite_state_register_set_operation(value),
            ),
    }
    Ok(encoded)
}

fn decode_operation_field<'schema>(
    mut field: proto::OperationField,
    schema: &'schema Schema,
) -> OperationResult<model::OperationFieldValue<'schema, UpdateId>> {
    let field_name = field.field_name.as_str();
    let Some((field_name, _)) = schema.columns.get_key_value(field_name) else {
        return Err(OperationCodecError::InvalidSchemaOperation {
            source: model::SchemaValueError::UnknownField {
                field_name: field_name.to_owned(),
            },
        });
    };

    let value = field
        .value
        .take()
        .take_required_oneof("OperationField.value")?;

    let value = match value {
        proto::operation_field::Value::LatestValueWins(value) => {
            let decoded_op = decode_latest_value_wins_operation(value)?;
            model::OperationValue::LatestValueWins(decoded_op)
        }
        proto::operation_field::Value::LinearString(value) => {
            let decoded_op = decode_linear_string_operation(value)?;
            model::OperationValue::LinearString(decoded_op)
        }
        proto::operation_field::Value::LinearList(value) => {
            let decoded_op = decode_linear_list_operation(value)?;
            model::OperationValue::LinearList(decoded_op)
        }
        proto::operation_field::Value::MonotonicCounterIncrement(value) => {
            let decoded_op = decode_monotonic_counter_increment_operation(value)?;
            model::OperationValue::MonotonicCounterIncrement(decoded_op)
        }
        proto::operation_field::Value::TotalOrderRegisterSet(value) => {
            let decoded_op = decode_total_order_register_set_operation(value)?;
            model::OperationValue::TotalOrderRegisterSet(decoded_op)
        }
        proto::operation_field::Value::TotalOrderFiniteStateRegisterSet(value) => {
            let decoded_op = decode_total_order_finite_state_register_set_operation(value)?;
            model::OperationValue::TotalOrderFiniteStateRegisterSet(decoded_op)
        }
    };

    Ok(model::OperationFieldValue {
        field_name: Cow::Borrowed(field_name),
        value,
    })
}

fn encode_latest_value_wins_operation(
    operation: &UpdateOperation<IdWithIndex<UpdateId>, model::NullableBasicValue>,
) -> proto::LatestValueWinsOperation {
    let mut encoded = proto::LatestValueWinsOperation::new();
    encoded.id = MessageField::some(encode_indexed_update_id(&operation.id));
    encoded.pred = MessageField::some(encode_indexed_update_id(&operation.pred));
    encoded.succ = MessageField::some(encode_indexed_update_id(&operation.succ));
    encoded.value = MessageField::some(encode_nullable_basic_value(operation.value.as_ref()));
    encoded
}

fn decode_latest_value_wins_operation(
    mut operation: proto::LatestValueWinsOperation,
) -> OperationResult<UpdateOperation<IdWithIndex<UpdateId>, model::NullableBasicValue>> {
    let id = operation
        .id
        .take_required("LatestValueWinsOperation", "id")
        .and_then(|id| decode_indexed_update_id(id).context(CodecSnafu))?;
    let pred = operation
        .pred
        .take_required("LatestValueWinsOperation", "pred")
        .and_then(|id| decode_indexed_update_id(id).context(CodecSnafu))?;
    let succ = operation
        .succ
        .take_required("LatestValueWinsOperation", "succ")
        .and_then(|id| decode_indexed_update_id(id).context(CodecSnafu))?;
    let value = operation
        .value
        .take_required("LatestValueWinsOperation", "value")
        .and_then(|value| decode_nullable_basic_value(value).context(CodecSnafu))?;

    Ok(UpdateOperation {
        id,
        pred,
        succ,
        value,
    })
}

fn encode_row_id(row_id: Uuid) -> Vec<u8> {
    row_id.as_bytes().to_vec()
}

fn decode_row_id(bytes: Vec<u8>) -> OperationResult<Uuid> {
    Uuid::from_slice(&bytes).map_err(|source| OperationCodecError::InvalidRowIdBytes {
        len: bytes.len(),
        source,
    })
}

fn encode_linear_string_operation(
    actions: &[DataOperation<UpdateIdWithIndex, String>],
) -> OperationResult<proto::LinearStringOperation> {
    ensure_non_empty_batch(actions)?;

    let mut encoded = proto::LinearStringOperation::new();
    encoded.actions = actions
        .iter()
        .map(encode_linear_string_action)
        .try_collect()?;
    Ok(encoded)
}

fn decode_linear_string_operation(
    operation: proto::LinearStringOperation,
) -> OperationResult<Vec<DataOperation<UpdateIdWithIndex, String>>> {
    ensure_non_empty_batch(&operation.actions)?;
    operation
        .actions
        .into_iter()
        .map(decode_linear_string_action)
        .collect()
}

fn encode_linear_string_action(
    action: &DataOperation<UpdateIdWithIndex, String>,
) -> OperationResult<proto::LinearStringAction> {
    let mut encoded = proto::LinearStringAction::new();
    match action {
        DataOperation::Insert {
            id,
            pred,
            succ,
            value,
        } => encoded.set_insert(encode_linear_string_insert_operation(id, pred, succ, value)),
        DataOperation::Delete { start, end } => {
            let encoded_op = encode_linear_delete_operation(start, end.as_ref())?;
            encoded.set_delete(encoded_op)
        }
    }
    Ok(encoded)
}

fn decode_linear_string_action(
    mut action: proto::LinearStringAction,
) -> OperationResult<DataOperation<UpdateIdWithIndex, String>> {
    let value = action
        .value
        .take()
        .take_required_oneof("LinearStringAction.value")?;
    match value {
        proto::linear_string_action::Value::Insert(value) => {
            decode_linear_string_insert_operation(value)
        }
        proto::linear_string_action::Value::Delete(value) => decode_linear_delete_operation(value),
    }
}

#[inline(always)]
fn encode_linear_string_insert_operation(
    id: &UpdateIdWithIndex,
    pred: &UpdateIdWithIndex,
    succ: &UpdateIdWithIndex,
    value: &str,
) -> proto::LinearStringInsertOperation {
    let mut encoded = proto::LinearStringInsertOperation::new();
    encoded.id = MessageField::some(encode_indexed_update_id(id));
    encoded.pred = MessageField::some(encode_indexed_update_id(pred));
    encoded.succ = MessageField::some(encode_indexed_update_id(succ));
    encoded.value = value.to_owned();
    encoded
}

fn decode_linear_string_insert_operation(
    mut operation: proto::LinearStringInsertOperation,
) -> OperationResult<DataOperation<UpdateIdWithIndex, String>> {
    let id = operation
        .id
        .take_required("LinearStringInsertOperation", "id")
        .and_then(|id| decode_indexed_update_id(id).context(CodecSnafu))?;
    let pred = operation
        .pred
        .take_required("LinearStringInsertOperation", "pred")
        .and_then(|id| decode_indexed_update_id(id).context(CodecSnafu))?;
    let succ = operation
        .succ
        .take_required("LinearStringInsertOperation", "succ")
        .and_then(|id| decode_indexed_update_id(id).context(CodecSnafu))?;

    Ok(DataOperation::Insert {
        id,
        pred,
        succ,
        value: operation.value,
    })
}

fn encode_linear_list_operation(
    actions: &[DataOperation<UpdateIdWithIndex, values::PrimitiveValueArray>],
) -> OperationResult<proto::LinearListOperation> {
    ensure_non_empty_batch(actions)?;

    let mut encoded = proto::LinearListOperation::new();
    encoded.actions = actions
        .iter()
        .map(encode_linear_list_action)
        .try_collect()?;
    Ok(encoded)
}

fn decode_linear_list_operation(
    operation: proto::LinearListOperation,
) -> OperationResult<Vec<DataOperation<UpdateIdWithIndex, values::PrimitiveValueArray>>> {
    ensure_non_empty_batch(&operation.actions)?;
    operation
        .actions
        .into_iter()
        .map(decode_linear_list_action)
        .collect()
}

fn encode_linear_list_action(
    action: &DataOperation<UpdateIdWithIndex, values::PrimitiveValueArray>,
) -> OperationResult<proto::LinearListAction> {
    let mut encoded = proto::LinearListAction::new();
    match action {
        DataOperation::Insert {
            id,
            pred,
            succ,
            value,
        } => encoded.set_insert(encode_linear_list_insert_operation(id, pred, succ, value)),
        DataOperation::Delete { start, end } => {
            encoded.set_delete(encode_linear_delete_operation(start, end.as_ref())?)
        }
    }
    Ok(encoded)
}

fn decode_linear_list_action(
    mut action: proto::LinearListAction,
) -> OperationResult<DataOperation<UpdateIdWithIndex, values::PrimitiveValueArray>> {
    let value = action
        .value
        .take()
        .take_required_oneof("LinearListAction.value")?;
    match value {
        proto::linear_list_action::Value::Insert(value) => {
            decode_linear_list_insert_operation(value)
        }
        proto::linear_list_action::Value::Delete(value) => decode_linear_delete_operation(value),
    }
}

fn encode_linear_list_insert_operation(
    id: &UpdateIdWithIndex,
    pred: &UpdateIdWithIndex,
    succ: &UpdateIdWithIndex,
    value: &values::PrimitiveValueArray,
) -> proto::LinearListInsertOperation {
    let mut encoded = proto::LinearListInsertOperation::new();
    encoded.id = MessageField::some(encode_indexed_update_id(id));
    encoded.pred = MessageField::some(encode_indexed_update_id(pred));
    encoded.succ = MessageField::some(encode_indexed_update_id(succ));
    encoded.value = MessageField::some(encode_primitive_array(value.as_ref()));
    encoded
}

fn decode_linear_list_insert_operation(
    mut operation: proto::LinearListInsertOperation,
) -> OperationResult<DataOperation<UpdateIdWithIndex, values::PrimitiveValueArray>> {
    let id = operation
        .id
        .take_required("LinearListInsertOperation", "id")
        .and_then(|id| decode_indexed_update_id(id).context(CodecSnafu))?;
    let pred = operation
        .pred
        .take_required("LinearListInsertOperation", "pred")
        .and_then(|id| decode_indexed_update_id(id).context(CodecSnafu))?;
    let succ = operation
        .succ
        .take_required("LinearListInsertOperation", "succ")
        .and_then(|id| decode_indexed_update_id(id).context(CodecSnafu))?;
    let value = operation
        .value
        .take_required("LinearListInsertOperation", "value")
        .and_then(|value| decode_primitive_array(value).context(CodecSnafu))?;

    Ok(DataOperation::Insert {
        id,
        pred,
        succ,
        value,
    })
}

fn encode_linear_delete_operation(
    start: &UpdateIdWithIndex,
    end: Option<&UpdateIdWithIndex>,
) -> OperationResult<proto::LinearDeleteOperation> {
    if let Some(end) = end {
        ensure!(start.id == end.id, DeleteRangeCrossesUpdateBoundarySnafu);
    }

    let mut encoded = proto::LinearDeleteOperation::new();
    encoded.start = MessageField::some(encode_indexed_update_id(start));
    encoded.end_chunk_index = end.map(|end| end.index);
    Ok(encoded)
}

fn decode_linear_delete_operation<Value>(
    mut operation: proto::LinearDeleteOperation,
) -> OperationResult<DataOperation<UpdateIdWithIndex, Value>> {
    let start = operation
        .start
        .take_required("LinearDeleteOperation", "start")
        .and_then(|id| decode_indexed_update_id(id).context(CodecSnafu))?;
    let end = operation.end_chunk_index.map(|index| IdWithIndex {
        id: start.id,
        index,
    });

    Ok(DataOperation::Delete { start, end })
}

fn encode_monotonic_counter_increment_operation(
    value: model::CounterValue,
) -> proto::MonotonicCounterIncrementOperation {
    let mut encoded = proto::MonotonicCounterIncrementOperation::new();
    encoded.value = MessageField::some(encode_counter_value(value.as_ref()));
    encoded
}

fn decode_monotonic_counter_increment_operation(
    mut operation: proto::MonotonicCounterIncrementOperation,
) -> OperationResult<model::CounterValue> {
    operation
        .value
        .take_required("MonotonicCounterIncrementOperation", "value")
        .and_then(|value| decode_counter_value(value).context(CodecSnafu))
}

fn encode_total_order_register_set_operation(
    value: &values::PrimitiveValue,
) -> proto::TotalOrderRegisterSetOperation {
    let mut encoded = proto::TotalOrderRegisterSetOperation::new();
    encoded.value = MessageField::some(encode_primitive_value(value.as_ref()));
    encoded
}

fn decode_total_order_register_set_operation(
    mut operation: proto::TotalOrderRegisterSetOperation,
) -> OperationResult<values::PrimitiveValue> {
    operation
        .value
        .take_required("TotalOrderRegisterSetOperation", "value")
        .and_then(|value| decode_primitive_value(value).context(CodecSnafu))
}

fn encode_total_order_finite_state_register_set_operation(
    value: &values::NullablePrimitiveValue,
) -> proto::TotalOrderFiniteStateRegisterSetOperation {
    let mut encoded = proto::TotalOrderFiniteStateRegisterSetOperation::new();
    encoded.value = MessageField::some(encode_nullable_primitive_value(value.as_ref()));
    encoded
}

fn decode_total_order_finite_state_register_set_operation(
    mut operation: proto::TotalOrderFiniteStateRegisterSetOperation,
) -> OperationResult<values::NullablePrimitiveValue> {
    operation
        .value
        .take_required("TotalOrderFiniteStateRegisterSetOperation", "value")
        .and_then(|value| decode_nullable_primitive_value(value).context(CodecSnafu))
}

fn ensure_non_empty_batch<T>(values: &[T]) -> OperationResult<()> {
    if values.is_empty() {
        return Err(OperationCodecError::InvalidOperationValue {
            source: model::DataModelValueError::EmptyLinearOperationBatch,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Uuid;
    use flotsync_data_types::{
        schema::{
            Direction,
            Field,
            PrimitiveType,
            Schema,
            datamodel::{InMemoryFieldValue, NullableBasicValue, RowSnapshot},
            values::{NullablePrimitiveValue, PrimitiveValue, PrimitiveValueArray},
        },
        test_support::schema_operations::{
            exhaustive_schema,
            exhaustive_schema_operation,
            exhaustive_schema_operations,
        },
    };
    use ordered_float::OrderedFloat;
    use std::assert_matches;

    fn update_ids() -> impl Iterator<Item = UpdateId> {
        (1u64..).map(|version| UpdateId {
            version,
            node_index: (version % 7) as u32,
        })
    }

    fn row_id(value: u128) -> Uuid {
        Uuid::from_u128(value)
    }

    fn row_ids() -> impl Iterator<Item = Uuid> {
        (1u128..).map(row_id)
    }

    fn indexed(version: u64, node_index: u32, index: u32) -> IdWithIndex<UpdateId> {
        IdWithIndex {
            id: UpdateId {
                version,
                node_index,
            },
            index,
        }
    }

    fn single_register_schema() -> Schema {
        Schema::from_fields([Field::total_order_register(
            "priority",
            PrimitiveType::UInt,
            Direction::Ascending,
        )])
    }

    #[test]
    fn exhaustive_single_field_operations_roundtrip_via_protobuf() {
        let schema = exhaustive_schema();
        let operations = exhaustive_schema_operations(row_ids(), update_ids()).unwrap();

        for operation in operations {
            let encoded = encode_schema_operation(&operation, &schema).unwrap();
            let decoded = decode_schema_operation(encoded, &schema).unwrap();
            assert_eq!(decoded, operation);
        }
    }

    #[test]
    fn exhaustive_multi_field_operation_roundtrips_via_protobuf() {
        let schema = exhaustive_schema();
        let operation = exhaustive_schema_operation(row_id(999), update_ids()).unwrap();

        let encoded = encode_schema_operation(&operation, &schema).unwrap();
        let decoded = decode_schema_operation(encoded, &schema).unwrap();

        assert_eq!(decoded, operation);
    }

    #[test]
    fn linear_batches_preserve_order() {
        let schema = exhaustive_schema();
        let operation = model::SchemaOperation {
            change_id: UpdateId {
                version: 100,
                node_index: 1,
            },
            operation: model::RowOperation::Update {
                row_id: row_id(101),
                fields: vec![
                    model::OperationFieldValue {
                        field_name: Cow::Borrowed("linear_string"),
                        value: model::OperationValue::LinearString(vec![
                            DataOperation::Insert {
                                id: indexed(1, 1, 0),
                                pred: indexed(2, 2, 0),
                                succ: indexed(3, 3, 0),
                                value: "alpha".to_owned(),
                            },
                            DataOperation::Delete {
                                start: indexed(1, 1, 0),
                                end: Some(indexed(1, 1, 1)),
                            },
                        ]),
                    },
                    model::OperationFieldValue {
                        field_name: Cow::Borrowed("linear_list_int"),
                        value: model::OperationValue::LinearList(vec![
                            DataOperation::Insert {
                                id: indexed(4, 4, 0),
                                pred: indexed(5, 5, 0),
                                succ: indexed(6, 6, 0),
                                value: PrimitiveValueArray::Int(vec![-7, 0, 9]),
                            },
                            DataOperation::Delete {
                                start: indexed(4, 4, 0),
                                end: Some(indexed(4, 4, 2)),
                            },
                        ]),
                    },
                ],
            },
        };

        let encoded = encode_schema_operation(&operation, &schema).unwrap();
        let decoded = decode_schema_operation(encoded, &schema).unwrap();

        assert_eq!(decoded, operation);
    }

    #[test]
    fn encode_rejects_delete_ranges_crossing_update_boundaries() {
        let operation = model::SchemaOperation {
            change_id: UpdateId {
                version: 200,
                node_index: 0,
            },
            operation: model::RowOperation::Update {
                row_id: row_id(201),
                fields: vec![model::OperationFieldValue {
                    field_name: Cow::Borrowed("linear_string"),
                    value: model::OperationValue::LinearString(vec![DataOperation::Delete {
                        start: indexed(8, 0, 0),
                        end: Some(indexed(9, 0, 1)),
                    }]),
                }],
            },
        };

        let err = encode_schema_operation(&operation, &exhaustive_schema()).unwrap_err();
        assert_matches!(err, OperationCodecError::DeleteRangeCrossesUpdateBoundary);
    }

    #[test]
    fn decode_rejects_empty_linear_batches() {
        let schema = exhaustive_schema();
        let mut field = proto::OperationField::new();
        field.field_name = "linear_string".to_owned();
        field.set_linear_string(proto::LinearStringOperation::new());

        let mut operation = proto::SchemaOperation::new();
        operation.change_id = MessageField::some(encode_update_id(UpdateId {
            version: 300,
            node_index: 0,
        }));
        let mut update = proto::UpdateRowOperation::new();
        update.row_id = encode_row_id(row_id(301));
        update.fields.push(field);
        operation.set_update(update);

        let err = decode_schema_operation(operation, &schema).unwrap_err();
        assert_matches!(
            err,
            OperationCodecError::InvalidOperationValue {
                source: model::DataModelValueError::EmptyLinearOperationBatch,
            }
        );
    }

    #[test]
    fn decode_rejects_wrong_field_variant_for_schema() {
        let schema = exhaustive_schema();
        let mut field = proto::OperationField::new();
        field.field_name = "linear_string".to_owned();
        field.set_total_order_register_set({
            let mut operation = proto::TotalOrderRegisterSetOperation::new();
            operation.value = MessageField::some(encode_primitive_value(
                PrimitiveValue::String("wrong".to_owned()).as_ref(),
            ));
            operation
        });

        let mut operation = proto::SchemaOperation::new();
        operation.change_id = MessageField::some(encode_update_id(UpdateId {
            version: 302,
            node_index: 0,
        }));
        let mut update = proto::UpdateRowOperation::new();
        update.row_id = encode_row_id(row_id(303));
        update.fields.push(field);
        operation.set_update(update);

        let err = decode_schema_operation(operation, &schema).unwrap_err();
        assert_matches!(
            err,
            OperationCodecError::InvalidSchemaOperation {
                source: model::SchemaValueError::InvalidOperationFieldValue { field_name, .. },
            } if field_name == "linear_string"
        );
    }

    #[test]
    fn decode_preserves_change_id_distinct_from_nested_ids() {
        let schema = exhaustive_schema();
        let operation = model::SchemaOperation {
            change_id: UpdateId {
                version: 999,
                node_index: 9,
            },
            operation: model::RowOperation::Update {
                row_id: row_id(998),
                fields: vec![
                    model::OperationFieldValue {
                        field_name: Cow::Borrowed("lvw_nullable_string_primitive"),
                        value: model::OperationValue::LatestValueWins(UpdateOperation {
                            id: indexed(1, 1, 0),
                            pred: indexed(2, 2, 0),
                            succ: indexed(3, 3, 0),
                            value: NullableBasicValue::Null,
                        }),
                    },
                    model::OperationFieldValue {
                        field_name: Cow::Borrowed("finite_state_register_nullable_string"),
                        value: model::OperationValue::TotalOrderFiniteStateRegisterSet(
                            NullablePrimitiveValue::Value(PrimitiveValue::String(
                                "review".to_owned(),
                            )),
                        ),
                    },
                    model::OperationFieldValue {
                        field_name: Cow::Borrowed("total_order_register_ascending_float"),
                        value: model::OperationValue::TotalOrderRegisterSet(PrimitiveValue::Float(
                            OrderedFloat(2.5),
                        )),
                    },
                ],
            },
        };

        let encoded = encode_schema_operation(&operation, &schema).unwrap();
        let decoded = decode_schema_operation(encoded, &schema).unwrap();

        assert_eq!(decoded.change_id, operation.change_id);
        assert_eq!(decoded, operation);
    }

    #[test]
    fn insert_and_delete_row_operations_roundtrip_via_protobuf() {
        let schema = single_register_schema();

        let insert = model::SchemaOperation {
            change_id: UpdateId {
                version: 400,
                node_index: 4,
            },
            operation: model::RowOperation::Insert {
                row_id: row_id(40),
                snapshot: RowSnapshot::from_owned_fields(vec![(
                    "priority".to_owned(),
                    InMemoryFieldValue::<UpdateId>::TotalOrderRegister(PrimitiveValue::UInt(7)),
                )]),
            },
        };
        let encoded_insert = encode_schema_operation(&insert, &schema).unwrap();
        let decoded_insert = decode_schema_operation(encoded_insert, &schema).unwrap();
        assert_eq!(decoded_insert, insert);

        let delete = model::SchemaOperation {
            change_id: UpdateId {
                version: 401,
                node_index: 4,
            },
            operation: model::RowOperation::Delete { row_id: row_id(40) },
        };
        let encoded_delete = encode_schema_operation(&delete, &schema).unwrap();
        let decoded_delete = decode_schema_operation(encoded_delete, &schema).unwrap();
        assert_eq!(decoded_delete, delete);
    }
}
