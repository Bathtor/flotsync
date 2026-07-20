//! Table-operation application for the in-memory data model.

use super::*;

impl<RowId, OperationId> TableOperations<RowId, OperationId>
    for InMemoryStateData<RowId, OperationId>
where
    OperationId:
        Clone + fmt::Debug + fmt::Display + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
    RowId: Clone + PartialEq + Eq + Hash + fmt::Debug + fmt::Display,
{
    type Row<'a>
        = InMemoryStateDataRow<'a, RowId, OperationId>
    where
        Self: 'a;

    fn schema(&self) -> &Schema {
        self.schema.as_schema()
    }

    fn get_row(&self, row_id: &RowId) -> Option<Self::Row<'_>> {
        let index = self.row_id_map.get(row_id)?;
        assert!(
            self.rows.len() > *index,
            "Illegal InMemoryStateData state: Row with id {row_id} has index={index}, which does not exist"
        );
        Some(InMemoryStateDataRow {
            data: self,
            row_index: *index,
        })
    }

    fn insert_row<'values, I>(
        &mut self,
        operation_id: OperationId,
        row_id: RowId,
        initial_values: I,
    ) -> crate::OperationResult<SchemaOperation<'_, RowId, OperationId>>
    where
        I: IntoIterator<Item = crate::schema::InitialFieldValue<'values>>,
    {
        if self.row_id_map.contains_key(&row_id) {
            return crate::DuplicateRowIdSnafu {
                row_id: row_id.to_string(),
            }
            .fail();
        }

        let initial_values = collect_initial_values(self.schema(), initial_values)?;
        let mut fields = Vec::with_capacity(self.field_names.len());
        for field_name in &self.field_names {
            let schema_field = self
                .schema()
                .columns
                .get(field_name.as_str())
                .expect("field_names and schema are in sync");
            let Some(target_value) = initial_values.get(field_name.as_str()) else {
                return Err(crate::OperationError::SchemaValue {
                    source: SchemaValueError::MissingField {
                        field_name: field_name.clone(),
                    },
                });
            };
            let field_value = build_initial_field_value(
                field_name.as_str(),
                &schema_field.data_type,
                target_value,
                &operation_id,
            )?;
            fields.push(field_value);
        }

        let row = InMemoryStateRow::new(fields);
        self.validate_row_value(&row)
            .context(crate::InMemoryStateDataSnafu)?;

        self.rows.push(row);
        let index = self.rows.len() - 1;
        self.row_id_map.insert(row_id.clone(), index);
        Ok(SchemaOperation {
            change_id: operation_id.clone(),
            operation: RowOperation::Insert {
                row_id,
                snapshot: RowStateSnapshot::borrowed_in_memory(
                    &self.field_names,
                    &self.rows[index],
                ),
            },
        })
    }

    fn modify_row<'values, I>(
        &mut self,
        operation_id: OperationId,
        row_id: RowId,
        changed_values: I,
    ) -> crate::OperationResult<OperationOutcome<SchemaOperation<'_, RowId, OperationId>>>
    where
        I: IntoIterator<Item = crate::schema::PendingFieldUpdate<'values>>,
    {
        let row_index =
            *self
                .row_id_map
                .get(&row_id)
                .ok_or_else(|| crate::OperationError::UnknownRowId {
                    row_id: row_id.to_string(),
                })?;
        if self.rows[row_index].deleted {
            return Err(crate::OperationError::ModifyDeletedRow {
                row_id: row_id.to_string(),
            });
        }
        let changed_values = collect_pending_updates(self.schema(), changed_values)?;
        let mut fields = Vec::with_capacity(changed_values.len());

        'changed_fields: for (field_name, target_value) in changed_values {
            let field_index = self
                .field_index(field_name.as_str())
                .expect("field exists in schema and index map");
            let current_value = self.rows[row_index].fields[field_index].clone();
            let Some(operation_value) = build_field_operation(
                field_name.as_str(),
                &current_value,
                target_value,
                operation_id.clone(),
            )?
            else {
                // `modify_row` accepts desired end-state values. If computing the concrete CRDT
                // delta for one field produces no operation, we intentionally skip that field and
                // keep collecting changes for the remaining inputs.
                continue 'changed_fields;
            };
            apply_operation_value(
                &mut self.rows[row_index].fields[field_index],
                operation_value.clone(),
            )?;
            fields.push(OperationFieldValue {
                field_name: Cow::Owned(field_name),
                value: operation_value,
            });
        }

        if fields.is_empty() {
            return Ok(OperationOutcome::NoChanges);
        }

        Ok(OperationOutcome::Applied(SchemaOperation {
            change_id: operation_id,
            operation: RowOperation::Update { row_id, fields },
        }))
    }

    fn delete_row(
        &mut self,
        operation_id: OperationId,
        row_id: RowId,
    ) -> crate::OperationResult<SchemaOperation<'_, RowId, OperationId>> {
        let row_index =
            *self
                .row_id_map
                .get(&row_id)
                .ok_or_else(|| crate::OperationError::UnknownRowId {
                    row_id: row_id.to_string(),
                })?;
        if self.rows[row_index].deleted {
            return Err(crate::OperationError::UnknownRowId {
                row_id: row_id.to_string(),
            });
        }
        self.rows[row_index].deleted = true;

        Ok(SchemaOperation {
            change_id: operation_id,
            operation: RowOperation::Delete { row_id },
        })
    }
}

fn collect_initial_values<'a>(
    schema: &Schema,
    initial_values: impl IntoIterator<Item = crate::schema::InitialFieldValue<'a>>,
) -> crate::OperationResult<HashMap<String, super::super::super::public_api::FieldTargetValue>> {
    let mut collected = HashMap::new();
    for initial_value in initial_values {
        let field_name = initial_value.field_name();
        let Some(schema_field) = schema.columns.get(field_name) else {
            return Err(crate::OperationError::SchemaValue {
                source: SchemaValueError::UnknownField {
                    field_name: field_name.to_owned(),
                },
            });
        };
        if schema_field.data_type != *initial_value.expected_type() {
            return Err(crate::OperationError::SchemaValue {
                source: SchemaValueError::InvalidSnapshotFieldValue {
                    field_name: field_name.to_owned(),
                    source: DataModelValueError::InvalidSnapshotValueForType,
                },
            });
        }
        collected.insert(field_name.to_owned(), initial_value.value().clone());
    }
    for (field_name, schema_field) in &schema.columns {
        if collected.contains_key(field_name.as_str()) {
            continue;
        }
        let default_value = materialize_default_target_value(field_name.as_str(), schema_field)?;
        if let Some(default_value) = default_value {
            collected.insert(field_name.clone(), default_value);
        }
    }
    Ok(collected)
}

pub(super) fn materialize_default_target_value(
    field_name: &str,
    schema_field: &crate::schema::Field,
) -> crate::OperationResult<Option<super::super::super::public_api::FieldTargetValue>> {
    schema_field
        .default_target_value()
        .map_err(|_| crate::OperationError::SchemaValue {
            source: SchemaValueError::InvalidSnapshotFieldValue {
                field_name: field_name.to_owned(),
                source: DataModelValueError::InvalidSnapshotValueForType,
            },
        })
}

fn collect_pending_updates<'a>(
    schema: &Schema,
    changed_values: impl IntoIterator<Item = crate::schema::PendingFieldUpdate<'a>>,
) -> crate::OperationResult<HashMap<String, super::super::super::public_api::FieldTargetValue>> {
    let mut collected = HashMap::new();
    for changed_value in changed_values {
        let field_name = changed_value.field_name();
        let Some(schema_field) = schema.columns.get(field_name) else {
            return Err(crate::OperationError::SchemaValue {
                source: SchemaValueError::UnknownField {
                    field_name: field_name.to_owned(),
                },
            });
        };
        if schema_field.data_type != *changed_value.expected_type() {
            return Err(crate::OperationError::SchemaValue {
                source: SchemaValueError::InvalidOperationFieldValue {
                    field_name: field_name.to_owned(),
                    source: DataModelValueError::InvalidOperationValueForType,
                },
            });
        }
        collected.insert(field_name.to_owned(), changed_value.value().clone());
    }
    Ok(collected)
}

#[allow(
    clippy::too_many_lines,
    reason = "This constructor is the central schema-to-in-memory type mapping table."
)]
pub(super) fn build_initial_field_value<OperationId>(
    field_name: &str,
    data_type: &ReplicatedDataType,
    target_value: &super::super::super::public_api::FieldTargetValue,
    operation_id: &OperationId,
) -> crate::OperationResult<InMemoryFieldState<OperationId>>
where
    OperationId:
        Clone + fmt::Debug + fmt::Display + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
{
    match (data_type, target_value) {
        (
            ReplicatedDataType::LatestValueWins { value_type },
            super::super::super::public_api::FieldTargetValue::NullableBasic(value),
        ) => build_initial_lww_value(field_name, value_type, value, operation_id),
        (
            ReplicatedDataType::LinearString,
            super::super::super::public_api::FieldTargetValue::String(value),
        ) => Ok(InMemoryFieldState::LinearString(LinearString::with_value(
            value.clone(),
            operation_id.clone(),
        ))),
        (
            ReplicatedDataType::LinearList {
                value_type: PrimitiveType::String,
            },
            super::super::super::public_api::FieldTargetValue::PrimitiveArray(value),
        ) => Ok(InMemoryFieldState::LinearList(LinearListState::String(
            LinearList::with_values(
                decode_list_string(value.clone()).map_err(operation_invalid_value)?,
                operation_id.clone(),
            ),
        ))),
        (
            ReplicatedDataType::LinearList {
                value_type: PrimitiveType::UInt,
            },
            super::super::super::public_api::FieldTargetValue::PrimitiveArray(value),
        ) => Ok(InMemoryFieldState::LinearList(LinearListState::UInt(
            LinearList::with_values(
                decode_list_uint(value.clone()).map_err(operation_invalid_value)?,
                operation_id.clone(),
            ),
        ))),
        (
            ReplicatedDataType::LinearList {
                value_type: PrimitiveType::Int,
            },
            super::super::super::public_api::FieldTargetValue::PrimitiveArray(value),
        ) => Ok(InMemoryFieldState::LinearList(LinearListState::Int(
            LinearList::with_values(
                decode_list_int(value.clone()).map_err(operation_invalid_value)?,
                operation_id.clone(),
            ),
        ))),
        (
            ReplicatedDataType::LinearList {
                value_type: PrimitiveType::Byte,
            },
            super::super::super::public_api::FieldTargetValue::PrimitiveArray(value),
        ) => Ok(InMemoryFieldState::LinearList(LinearListState::Byte(
            LinearList::with_values(
                decode_list_byte(value.clone()).map_err(operation_invalid_value)?,
                operation_id.clone(),
            ),
        ))),
        (
            ReplicatedDataType::LinearList {
                value_type: PrimitiveType::Float,
            },
            super::super::super::public_api::FieldTargetValue::PrimitiveArray(value),
        ) => Ok(InMemoryFieldState::LinearList(LinearListState::Float(
            LinearList::with_values(
                decode_list_float(value.clone()).map_err(operation_invalid_value)?,
                operation_id.clone(),
            ),
        ))),
        (
            ReplicatedDataType::LinearList {
                value_type: PrimitiveType::Boolean,
            },
            super::super::super::public_api::FieldTargetValue::PrimitiveArray(value),
        ) => Ok(InMemoryFieldState::LinearList(LinearListState::Boolean(
            LinearList::with_values(
                decode_list_boolean(value.clone()).map_err(operation_invalid_value)?,
                operation_id.clone(),
            ),
        ))),
        (
            ReplicatedDataType::LinearList {
                value_type: PrimitiveType::Binary,
            },
            super::super::super::public_api::FieldTargetValue::PrimitiveArray(value),
        ) => Ok(InMemoryFieldState::LinearList(LinearListState::Binary(
            LinearList::with_values(
                decode_list_binary(value.clone()).map_err(operation_invalid_value)?,
                operation_id.clone(),
            ),
        ))),
        (
            ReplicatedDataType::LinearList {
                value_type: PrimitiveType::Date,
            },
            super::super::super::public_api::FieldTargetValue::PrimitiveArray(value),
        ) => Ok(InMemoryFieldState::LinearList(LinearListState::Date(
            LinearList::with_values(
                decode_list_date(value.clone()).map_err(operation_invalid_value)?,
                operation_id.clone(),
            ),
        ))),
        (
            ReplicatedDataType::LinearList {
                value_type: PrimitiveType::Timestamp,
            },
            super::super::super::public_api::FieldTargetValue::PrimitiveArray(value),
        ) => Ok(InMemoryFieldState::LinearList(LinearListState::Timestamp(
            LinearList::with_values(
                decode_list_timestamp(value.clone()).map_err(operation_invalid_value)?,
                operation_id.clone(),
            ),
        ))),
        (
            ReplicatedDataType::MonotonicCounter { .. },
            super::super::super::public_api::FieldTargetValue::Counter(value),
        ) => Ok(InMemoryFieldState::MonotonicCounter(*value)),
        (
            ReplicatedDataType::TotalOrderRegister { .. },
            super::super::super::public_api::FieldTargetValue::Primitive(value),
        ) => Ok(InMemoryFieldState::TotalOrderRegister(value.clone())),
        (
            ReplicatedDataType::TotalOrderFiniteStateRegister { .. },
            super::super::super::public_api::FieldTargetValue::NullablePrimitive(value),
        ) => Ok(InMemoryFieldState::TotalOrderFiniteStateRegister(
            value.clone(),
        )),
        _ => crate::InternalOperationSnafu {
            context: format!(
                "Field '{field_name}' had a builder value incompatible with its schema type."
            ),
        }
        .fail(),
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "This decision table enumerates every initial latest-value-wins representation."
)]
fn build_initial_lww_value<OperationId>(
    field_name: &str,
    value_type: &NullableBasicDataType,
    value: &NullableBasicValue,
    operation_id: &OperationId,
) -> crate::OperationResult<InMemoryFieldState<OperationId>>
where
    OperationId:
        Clone + fmt::Debug + fmt::Display + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
{
    let begin_id = IdWithIndex::zero(operation_id.clone());
    let value_id = begin_id.increment();
    let end_id = value_id.increment();
    let ids = [begin_id, value_id, end_id];

    macro_rules! build_lww_variant {
        ($variant:ident, $value:expr) => {{
            let register = LinearLatestValueWins::new($value, ids);
            Ok(InMemoryFieldState::LatestValueWins(
                LinearLatestValueWinsState::$variant(register),
            ))
        }};
    }

    match (value_type, value) {
        (
            NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::String)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::String(value))),
        ) => build_lww_variant!(String, value.clone()),
        (
            NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::UInt)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::UInt(value))),
        ) => build_lww_variant!(UInt, *value),
        (
            NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Int)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::Int(value))),
        ) => build_lww_variant!(Int, *value),
        (
            NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Byte)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::Byte(value))),
        ) => build_lww_variant!(Byte, *value),
        (
            NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Float)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::Float(value))),
        ) => build_lww_variant!(Float, *value),
        (
            NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Boolean)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::Boolean(value))),
        ) => build_lww_variant!(Boolean, *value),
        (
            NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Binary)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::Binary(value))),
        ) => build_lww_variant!(Binary, value.clone()),
        (
            NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Date)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::Date(value))),
        ) => build_lww_variant!(Date, *value),
        (
            NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Timestamp)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::Timestamp(value))),
        ) => build_lww_variant!(Timestamp, *value),
        (
            NullableBasicDataType::NonNull(BasicDataType::Array(array_type)),
            NullableBasicValue::Value(BasicValue::Array(value)),
        ) => match array_type.element_type {
            PrimitiveType::String => match value {
                PrimitiveValueArray::String(value) => build_lww_variant!(StringArray, value.clone()),
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
            PrimitiveType::UInt => match value {
                PrimitiveValueArray::UInt(value) => build_lww_variant!(UIntArray, value.clone()),
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
            PrimitiveType::Int => match value {
                PrimitiveValueArray::Int(value) => build_lww_variant!(IntArray, value.clone()),
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
            PrimitiveType::Byte => match value {
                PrimitiveValueArray::Byte(value) => build_lww_variant!(ByteArray, value.clone()),
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
            PrimitiveType::Float => match value {
                PrimitiveValueArray::Float(value) => build_lww_variant!(FloatArray, value.clone()),
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
            PrimitiveType::Boolean => match value {
                PrimitiveValueArray::Boolean(value) => {
                    build_lww_variant!(BooleanArray, value.clone())
                }
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
            PrimitiveType::Binary => match value {
                PrimitiveValueArray::Binary(value) => build_lww_variant!(BinaryArray, value.clone()),
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
            PrimitiveType::Date => match value {
                PrimitiveValueArray::Date(value) => build_lww_variant!(DateArray, value.clone()),
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
            PrimitiveType::Timestamp => match value {
                PrimitiveValueArray::Timestamp(value) => {
                    build_lww_variant!(TimestampArray, value.clone())
                }
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
        },
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::String)),
            NullableBasicValue::Null,
        ) => build_lww_variant!(NullableString, None::<String>),
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::UInt)),
            NullableBasicValue::Null,
        ) => build_lww_variant!(NullableUInt, None::<u64>),
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Int)),
            NullableBasicValue::Null,
        ) => build_lww_variant!(NullableInt, None::<i64>),
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Byte)),
            NullableBasicValue::Null,
        ) => build_lww_variant!(NullableByte, None::<u8>),
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Float)),
            NullableBasicValue::Null,
        ) => build_lww_variant!(NullableFloat, None::<OrderedFloat<f64>>),
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Boolean)),
            NullableBasicValue::Null,
        ) => build_lww_variant!(NullableBoolean, None::<bool>),
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Binary)),
            NullableBasicValue::Null,
        ) => build_lww_variant!(NullableBinary, None::<Vec<u8>>),
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Date)),
            NullableBasicValue::Null,
        ) => build_lww_variant!(NullableDate, None::<NaiveDate>),
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Timestamp)),
            NullableBasicValue::Null,
        ) => build_lww_variant!(NullableTimestamp, None::<UnixTimestamp>),
        (
            NullableBasicDataType::Nullable(BasicDataType::Array(array_type)),
            NullableBasicValue::Null,
        ) => match array_type.element_type {
            PrimitiveType::String => build_lww_variant!(NullableStringArray, None::<Vec<String>>),
            PrimitiveType::UInt => build_lww_variant!(NullableUIntArray, None::<Vec<u64>>),
            PrimitiveType::Int => build_lww_variant!(NullableIntArray, None::<Vec<i64>>),
            PrimitiveType::Byte => build_lww_variant!(NullableByteArray, None::<Vec<u8>>),
            PrimitiveType::Float => {
                build_lww_variant!(NullableFloatArray, None::<Vec<OrderedFloat<f64>>>)
            }
            PrimitiveType::Boolean => build_lww_variant!(NullableBooleanArray, None::<Vec<bool>>),
            PrimitiveType::Binary => build_lww_variant!(NullableBinaryArray, None::<Vec<Vec<u8>>>),
            PrimitiveType::Date => build_lww_variant!(NullableDateArray, None::<Vec<NaiveDate>>),
            PrimitiveType::Timestamp => {
                build_lww_variant!(NullableTimestampArray, None::<Vec<UnixTimestamp>>)
            }
        },
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::String)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::String(value))),
        ) => build_lww_variant!(NullableString, Some(value.clone())),
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::UInt)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::UInt(value))),
        ) => build_lww_variant!(NullableUInt, Some(*value)),
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Int)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::Int(value))),
        ) => build_lww_variant!(NullableInt, Some(*value)),
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Byte)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::Byte(value))),
        ) => build_lww_variant!(NullableByte, Some(*value)),
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Float)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::Float(value))),
        ) => build_lww_variant!(NullableFloat, Some(*value)),
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Boolean)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::Boolean(value))),
        ) => build_lww_variant!(NullableBoolean, Some(*value)),
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Binary)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::Binary(value))),
        ) => build_lww_variant!(NullableBinary, Some(value.clone())),
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Date)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::Date(value))),
        ) => build_lww_variant!(NullableDate, Some(*value)),
        (
            NullableBasicDataType::Nullable(BasicDataType::Primitive(PrimitiveType::Timestamp)),
            NullableBasicValue::Value(BasicValue::Primitive(PrimitiveValue::Timestamp(value))),
        ) => build_lww_variant!(NullableTimestamp, Some(*value)),
        (
            NullableBasicDataType::Nullable(BasicDataType::Array(array_type)),
            NullableBasicValue::Value(BasicValue::Array(value)),
        ) => match array_type.element_type {
            PrimitiveType::String => match value {
                PrimitiveValueArray::String(value) => {
                    build_lww_variant!(NullableStringArray, Some(value.clone()))
                }
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
            PrimitiveType::UInt => match value {
                PrimitiveValueArray::UInt(value) => {
                    build_lww_variant!(NullableUIntArray, Some(value.clone()))
                }
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
            PrimitiveType::Int => match value {
                PrimitiveValueArray::Int(value) => {
                    build_lww_variant!(NullableIntArray, Some(value.clone()))
                }
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
            PrimitiveType::Byte => match value {
                PrimitiveValueArray::Byte(value) => {
                    build_lww_variant!(NullableByteArray, Some(value.clone()))
                }
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
            PrimitiveType::Float => match value {
                PrimitiveValueArray::Float(value) => {
                    build_lww_variant!(NullableFloatArray, Some(value.clone()))
                }
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
            PrimitiveType::Boolean => match value {
                PrimitiveValueArray::Boolean(value) => {
                    build_lww_variant!(NullableBooleanArray, Some(value.clone()))
                }
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
            PrimitiveType::Binary => match value {
                PrimitiveValueArray::Binary(value) => {
                    build_lww_variant!(NullableBinaryArray, Some(value.clone()))
                }
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
            PrimitiveType::Date => match value {
                PrimitiveValueArray::Date(value) => {
                    build_lww_variant!(NullableDateArray, Some(value.clone()))
                }
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
            PrimitiveType::Timestamp => match value {
                PrimitiveValueArray::Timestamp(value) => {
                    build_lww_variant!(NullableTimestampArray, Some(value.clone()))
                }
                _ => crate::InternalOperationSnafu {
                    context: format!(
                        "Field '{field_name}' had an unexpected LatestValueWins array payload."
                    ),
                }
                .fail(),
            },
        },
        _ => crate::InternalOperationSnafu {
            context: format!(
                "Field '{field_name}' had a LatestValueWins payload incompatible with its schema type."
            ),
        }
        .fail(),
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "This operation builder deliberately keeps the public target-value to concrete CRDT operation mapping in one exhaustive match."
)]
fn build_field_operation<OperationId>(
    field_name: &str,
    current_value: &InMemoryFieldState<OperationId>,
    target_value: super::super::super::public_api::FieldTargetValue,
    operation_id: OperationId,
) -> crate::OperationResult<Option<OperationValue<OperationId>>>
where
    OperationId:
        Clone + fmt::Debug + fmt::Display + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
{
    match (current_value, target_value) {
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::String(current)),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_string(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::UInt(current)),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_uint(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::Int(current)),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_int(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::Byte(current)),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_byte(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::Float(current)),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_float(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::Boolean(current)),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_boolean(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::Binary(current)),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_binary(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::Date(current)),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_date(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::Timestamp(current)),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_timestamp(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::StringArray(current)),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_string_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::UIntArray(current)),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_uint_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::IntArray(current)),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_int_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::ByteArray(current)),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_byte_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::FloatArray(current)),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_float_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::BooleanArray(current)),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_boolean_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::BinaryArray(current)),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_binary_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::DateArray(current)),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_date_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::TimestampArray(
                current,
            )),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_required_timestamp_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableString(
                current,
            )),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_string(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableUInt(current)),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_uint(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableInt(current)),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_int(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableByte(current)),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_byte(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableFloat(current)),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_float(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableBoolean(
                current,
            )),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_boolean(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableBinary(
                current,
            )),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_binary(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableDate(current)),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_date(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableTimestamp(
                current,
            )),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_timestamp(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableStringArray(
                current,
            )),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_string_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableUIntArray(
                current,
            )),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_uint_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableIntArray(
                current,
            )),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_int_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableByteArray(
                current,
            )),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_byte_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableFloatArray(
                current,
            )),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_float_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableBooleanArray(
                current,
            )),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_boolean_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableBinaryArray(
                current,
            )),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_binary_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableDateArray(
                current,
            )),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_date_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LatestValueWins(
                LinearLatestValueWinsState::NullableTimestampArray(current),
            ),
            super::super::super::public_api::FieldTargetValue::NullableBasic(target),
        ) => build_lww_operation(
            current,
            decode_optional_timestamp_array(target).map_err(operation_invalid_value)?,
            operation_id,
        )
        .map(|value| value.map(OperationValue::LatestValueWins)),
        (
            InMemoryFieldState::LinearString(current),
            super::super::super::public_api::FieldTargetValue::String(target),
        ) => {
            if current.to_string() == target {
                return Ok(None);
            }
            let mut id_generator = std::iter::once(operation_id);
            let diff = linear_string_diff(current, &target, &mut id_generator)
                .context(crate::LinearStringDiffSnafu)?;
            let operations = diff.into_operations();
            if operations.is_empty() {
                Ok(None)
            } else {
                Ok(Some(OperationValue::LinearString(operations)))
            }
        }
        (
            InMemoryFieldState::LinearList(current),
            super::super::super::public_api::FieldTargetValue::PrimitiveArray(target),
        ) => build_linear_list_operation(current, target, &operation_id)
            .map(|value| value.map(OperationValue::LinearList)),
        (
            InMemoryFieldState::MonotonicCounter(current),
            super::super::super::public_api::FieldTargetValue::Counter(target),
        ) => build_counter_operation(field_name, *current, target)
            .map(|value| value.map(OperationValue::MonotonicCounterIncrement)),
        (
            InMemoryFieldState::TotalOrderRegister(current),
            super::super::super::public_api::FieldTargetValue::Primitive(target),
        ) => {
            if *current == target {
                Ok(None)
            } else {
                Ok(Some(OperationValue::TotalOrderRegisterSet(target)))
            }
        }
        (
            InMemoryFieldState::TotalOrderFiniteStateRegister(current),
            super::super::super::public_api::FieldTargetValue::NullablePrimitive(target),
        ) => {
            if *current == target {
                Ok(None)
            } else {
                Ok(Some(OperationValue::TotalOrderFiniteStateRegisterSet(
                    target,
                )))
            }
        }
        _ => crate::InternalOperationSnafu {
            context: format!(
                "Field '{field_name}' had an unexpected target value for its in-memory type."
            ),
        }
        .fail(),
    }
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "The helper shares the operation-result shape used by neighbouring builders and call sites."
)]
fn build_lww_operation<OperationId, T>(
    current: &LinearLatestValueWins<IdWithIndex<OperationId>, T>,
    target: T,
    operation_id: OperationId,
) -> crate::OperationResult<Option<UpdateOperation<IdWithIndex<OperationId>, NullableBasicValue>>>
where
    OperationId:
        Clone + fmt::Debug + fmt::Display + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
    T: Clone + fmt::Debug + PartialEq,
    NullableBasicValue: From<T>,
{
    if current.content() == &target {
        Ok(None)
    } else {
        let indexed_operation_id = IdWithIndex::zero(operation_id);
        let operation = current.update_operation(indexed_operation_id, target);
        Ok(Some(UpdateOperation {
            id: operation.id,
            pred: operation.pred,
            succ: operation.succ,
            value: operation.value.into(),
        }))
    }
}

fn build_counter_operation(
    field_name: &str,
    current: CounterValue,
    target: CounterValue,
) -> crate::OperationResult<Option<CounterValue>> {
    match (current, target) {
        (CounterValue::Byte(current), CounterValue::Byte(target)) => {
            if target < current {
                return crate::UnsupportedOperationVariantSnafu {
                    explanation: Cow::Owned(format!(
                        "Field '{field_name}' is monotonic and cannot decrease from {current} to {target}."
                    )),
                }
                .fail();
            }
            if target == current {
                Ok(None)
            } else {
                Ok(Some(CounterValue::Byte(target - current)))
            }
        }
        (CounterValue::UInt(current), CounterValue::UInt(target)) => {
            if target < current {
                return crate::UnsupportedOperationVariantSnafu {
                    explanation: Cow::Owned(format!(
                        "Field '{field_name}' is monotonic and cannot decrease from {current} to {target}."
                    )),
                }
                .fail();
            }
            if target == current {
                Ok(None)
            } else {
                Ok(Some(CounterValue::UInt(target - current)))
            }
        }
        _ => crate::InternalOperationSnafu {
            context: format!("Field '{field_name}' had mismatched counter representations."),
        }
        .fail(),
    }
}

type LinearListOperations<OperationId> =
    Vec<DataOperation<IdWithIndex<OperationId>, PrimitiveValueArray>>;

fn build_linear_list_operation<OperationId>(
    current: &LinearListState<OperationId>,
    target: PrimitiveValueArray,
    operation_id: &OperationId,
) -> crate::OperationResult<Option<LinearListOperations<OperationId>>>
where
    OperationId:
        Clone + fmt::Debug + fmt::Display + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
{
    macro_rules! build_linear_list_op {
        ($current:expr, $target:expr, $decode:ident) => {{
            let target_values = $decode($target).map_err(operation_invalid_value)?;
            let diff = linear_list_diff($current, &target_values, &operation_id)
                .context(crate::LinearListDiffSnafu)?;
            let operations = diff.into_operations();
            if operations.is_empty() {
                Ok(None)
            } else {
                Ok(Some(
                    operations
                        .into_iter()
                        .map(|op| op.map_value(|values| values.into()))
                        .collect(),
                ))
            }
        }};
    }

    match current {
        LinearListState::String(current) => {
            build_linear_list_op!(current, target, decode_list_string)
        }
        LinearListState::UInt(current) => build_linear_list_op!(current, target, decode_list_uint),
        LinearListState::Int(current) => build_linear_list_op!(current, target, decode_list_int),
        LinearListState::Byte(current) => build_linear_list_op!(current, target, decode_list_byte),
        LinearListState::Float(current) => {
            build_linear_list_op!(current, target, decode_list_float)
        }
        LinearListState::Boolean(current) => {
            build_linear_list_op!(current, target, decode_list_boolean)
        }
        LinearListState::Binary(current) => {
            build_linear_list_op!(current, target, decode_list_binary)
        }
        LinearListState::Date(current) => build_linear_list_op!(current, target, decode_list_date),
        LinearListState::Timestamp(current) => {
            build_linear_list_op!(current, target, decode_list_timestamp)
        }
    }
}

fn map_data_operation_value<Id, InputValue, OutputValue, E>(
    operation: DataOperation<Id, InputValue>,
    map_value: impl Fn(InputValue) -> Result<OutputValue, E>,
) -> Result<DataOperation<Id, OutputValue>, E> {
    match operation {
        DataOperation::Insert {
            id,
            pred,
            succ,
            value,
        } => Ok(DataOperation::Insert {
            id,
            pred,
            succ,
            value: map_value(value)?,
        }),
        DataOperation::Delete { start, end } => Ok(DataOperation::Delete { start, end }),
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "Applying operation values is an exhaustive CRDT variant dispatch table."
)]
pub(super) fn apply_operation_value<OperationId>(
    field_value: &mut InMemoryFieldState<OperationId>,
    operation_value: OperationValue<OperationId>,
) -> crate::OperationResult<()>
where
    OperationId:
        Clone + fmt::Debug + fmt::Display + PartialEq + Eq + Hash + PartialOrd + Ord + 'static,
{
    macro_rules! apply_lww_operation {
        ($current:expr, $operation:expr, $decode:ident) => {{
            let operation = UpdateOperation {
                id: $operation.id,
                pred: $operation.pred,
                succ: $operation.succ,
                value: $decode($operation.value).map_err(operation_invalid_value)?,
            };
            $current.apply_operation(operation).map_err(|_| {
                crate::InternalOperationSnafu {
                    context: "Applying a generated LatestValueWins operation failed.".to_owned(),
                }
                .build()
            })
        }};
    }

    macro_rules! apply_list_operations {
        ($current:expr, $operations:expr, $decode:ident) => {{
            for operation in $operations {
                let operation = map_data_operation_value(operation, $decode)
                    .map_err(operation_invalid_value)?;
                let operation = ListOperation::from_operation(operation);
                if $current.apply_operation(operation).is_err() {
                    return crate::InternalOperationSnafu {
                        context: "Applying a generated LinearList operation failed.".to_owned(),
                    }
                    .fail();
                }
            }
            Ok(())
        }};
    }

    match (field_value, operation_value) {
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::String(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_string),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::UInt(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_uint),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::Int(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_int),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::Byte(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_byte),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::Float(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_float),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::Boolean(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_boolean),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::Binary(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_binary),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::Date(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_date),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::Timestamp(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_timestamp),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::StringArray(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_string_array),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::UIntArray(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_uint_array),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::IntArray(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_int_array),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::ByteArray(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_byte_array),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::FloatArray(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_float_array),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::BooleanArray(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_boolean_array),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::BinaryArray(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_binary_array),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::DateArray(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_date_array),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::TimestampArray(
                current,
            )),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_required_timestamp_array),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableString(
                current,
            )),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_string),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableUInt(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_uint),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableInt(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_int),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableByte(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_byte),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableFloat(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_float),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableBoolean(
                current,
            )),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_boolean),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableBinary(
                current,
            )),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_binary),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableDate(current)),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_date),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableTimestamp(
                current,
            )),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_timestamp),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableStringArray(
                current,
            )),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_string_array),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableUIntArray(
                current,
            )),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_uint_array),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableIntArray(
                current,
            )),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_int_array),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableByteArray(
                current,
            )),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_byte_array),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableFloatArray(
                current,
            )),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_float_array),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableBooleanArray(
                current,
            )),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_boolean_array),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableBinaryArray(
                current,
            )),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_binary_array),
        (
            InMemoryFieldState::LatestValueWins(LinearLatestValueWinsState::NullableDateArray(
                current,
            )),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_date_array),
        (
            InMemoryFieldState::LatestValueWins(
                LinearLatestValueWinsState::NullableTimestampArray(current),
            ),
            OperationValue::LatestValueWins(operation),
        ) => apply_lww_operation!(current, operation, decode_optional_timestamp_array),
        (InMemoryFieldState::LinearString(current), OperationValue::LinearString(operations)) => {
            for operation in operations {
                if current.apply_operation(operation).is_err() {
                    return crate::InternalOperationSnafu {
                        context: "Applying a generated LinearString operation failed.".to_owned(),
                    }
                    .fail();
                }
            }
            Ok(())
        }
        (
            InMemoryFieldState::LinearList(LinearListState::String(current)),
            OperationValue::LinearList(operations),
        ) => apply_list_operations!(current, operations, decode_list_string),
        (
            InMemoryFieldState::LinearList(LinearListState::UInt(current)),
            OperationValue::LinearList(operations),
        ) => apply_list_operations!(current, operations, decode_list_uint),
        (
            InMemoryFieldState::LinearList(LinearListState::Int(current)),
            OperationValue::LinearList(operations),
        ) => apply_list_operations!(current, operations, decode_list_int),
        (
            InMemoryFieldState::LinearList(LinearListState::Byte(current)),
            OperationValue::LinearList(operations),
        ) => apply_list_operations!(current, operations, decode_list_byte),
        (
            InMemoryFieldState::LinearList(LinearListState::Float(current)),
            OperationValue::LinearList(operations),
        ) => apply_list_operations!(current, operations, decode_list_float),
        (
            InMemoryFieldState::LinearList(LinearListState::Boolean(current)),
            OperationValue::LinearList(operations),
        ) => apply_list_operations!(current, operations, decode_list_boolean),
        (
            InMemoryFieldState::LinearList(LinearListState::Binary(current)),
            OperationValue::LinearList(operations),
        ) => apply_list_operations!(current, operations, decode_list_binary),
        (
            InMemoryFieldState::LinearList(LinearListState::Date(current)),
            OperationValue::LinearList(operations),
        ) => apply_list_operations!(current, operations, decode_list_date),
        (
            InMemoryFieldState::LinearList(LinearListState::Timestamp(current)),
            OperationValue::LinearList(operations),
        ) => apply_list_operations!(current, operations, decode_list_timestamp),
        (
            InMemoryFieldState::MonotonicCounter(current),
            OperationValue::MonotonicCounterIncrement(delta),
        ) => {
            match (current, delta) {
                (CounterValue::Byte(current), CounterValue::Byte(delta)) => {
                    *current = current.saturating_add(delta);
                }
                (CounterValue::UInt(current), CounterValue::UInt(delta)) => {
                    *current = current.saturating_add(delta);
                }
                _ => {
                    return crate::InternalOperationSnafu {
                        context: "Applying a generated monotonic-counter increment failed due to a type mismatch.".to_owned(),
                    }
                    .fail();
                }
            }
            Ok(())
        }
        (
            InMemoryFieldState::TotalOrderRegister(current),
            OperationValue::TotalOrderRegisterSet(value),
        ) => {
            *current = value;
            Ok(())
        }
        (
            InMemoryFieldState::TotalOrderFiniteStateRegister(current),
            OperationValue::TotalOrderFiniteStateRegisterSet(value),
        ) => {
            *current = value;
            Ok(())
        }
        _ => crate::InternalOperationSnafu {
            context: "Applying a generated field operation failed due to a type mismatch."
                .to_owned(),
        }
        .fail(),
    }
}

fn operation_invalid_value(source: DataModelValueError) -> crate::OperationError {
    crate::OperationError::SchemaValue {
        source: SchemaValueError::InvalidOperationFieldValue {
            field_name: "<derived>".to_owned(),
            source,
        },
    }
}
