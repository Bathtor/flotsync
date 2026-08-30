//! Schema snapshot writer and field sinks.

use super::codec::{is_history_data_type, validate_snapshot_state_value_for_type};
#[allow(
    clippy::wildcard_imports,
    reason = "The private helper module intentionally shares its parent's local implementation vocabulary."
)]
use super::*;
use crate::schema::{Field, OrderedSchema};

/// Field lookup shared by unordered and position-bearing schema views during snapshot encoding.
pub(crate) trait SchemaSnapshotFieldView {
    /// Return the number of fields expected in the snapshot.
    fn len(&self) -> usize;

    /// Look up a field by name.
    fn field(&self, field_name: &str) -> Option<&Field>;

    /// Return the lexicographically first field not present in `seen_fields`.
    fn first_missing_field<'schema>(
        &'schema self,
        seen_fields: &HashSet<String>,
    ) -> Option<&'schema str>;
}

impl SchemaSnapshotFieldView for Schema {
    fn len(&self) -> usize {
        self.columns.len()
    }

    fn field(&self, field_name: &str) -> Option<&Field> {
        self.field(field_name)
    }

    fn first_missing_field<'schema>(
        &'schema self,
        seen_fields: &HashSet<String>,
    ) -> Option<&'schema str> {
        self.columns
            .keys()
            .filter(|field_name| !seen_fields.contains(*field_name))
            .min()
    }
}

impl SchemaSnapshotFieldView for OrderedSchema<'_> {
    fn len(&self) -> usize {
        self.len()
    }

    fn field(&self, field_name: &str) -> Option<&Field> {
        self.field(field_name)
    }

    fn first_missing_field<'schema>(
        &'schema self,
        seen_fields: &HashSet<String>,
    ) -> Option<&'schema str> {
        self.fields()
            .map(|field| field.name.as_str())
            .filter(|field_name| !seen_fields.contains(*field_name))
            .min()
    }
}

/// Streaming snapshot writer that validates against a schema while fields are emitted.
///
/// Use `prepare_*_field` to obtain a field-local sink and pass it to CRDT `encode_snapshot`.
///
/// Typical flow:
/// 1. `prepare_schema_snapshot_encoder`
/// 2. call `state_field` and `prepare_*_field` for all fields
/// 3. for each prepared history field sink, call CRDT `encode_snapshot`
/// 4. `end`
///
/// Example:
/// ```ignore
/// let mut snapshot = prepare_schema_snapshot_encoder(&mut visitor, schema)?;
///
/// snapshot.state_field("counter", StateSnapshotFieldValueRef::MonotonicCounter(...))?;
///
/// // LinearString can stream directly.
/// let mut title_sink = snapshot.prepare_linear_string_field("title")?;
/// title.encode_snapshot(&mut title_sink)?;
///
/// // For LinearList / LatestValueWins, wrap/adapt the sink so CRDT node values are
/// // converted into PrimitiveValueArrayRef / BasicValueRef respectively, then call
/// // CRDT encode_snapshot on that adapter.
/// snapshot.end()?;
/// ```
pub(crate) struct SchemaSnapshotEncodingWriter<'a, Id, V>
where
    V: SchemaSnapshotEncoder<Id>,
{
    schema: &'a dyn SchemaSnapshotFieldView,
    visitor: &'a mut V,
    seen_fields: HashSet<String>,
    _id_marker: PhantomData<Id>,
}
impl<Id, V> SchemaSnapshotEncodingWriter<'_, Id, V>
where
    V: SchemaSnapshotEncoder<Id>,
{
    fn register_field(&mut self, field_name: &str) -> Result<(), SchemaValueError> {
        if !self.seen_fields.insert(field_name.to_owned()) {
            return Err(SchemaValueError::DuplicateField {
                field_name: field_name.to_owned(),
            });
        }

        if self.schema.field(field_name).is_none() {
            return Err(SchemaValueError::UnknownField {
                field_name: field_name.to_owned(),
            });
        }

        Ok(())
    }

    pub(in crate::schema::datamodel) fn state_field(
        &mut self,
        field_name: &str,
        value: StateSnapshotFieldValueRef<'_>,
    ) -> Result<(), SchemaVisitError<V::Error>> {
        self.register_field(field_name)
            .map_err(|source| SchemaVisitError::InvalidSchemaValue { source })?;
        let schema_field = self
            .schema
            .field(field_name)
            .expect("field existence was validated");

        if is_history_data_type(&schema_field.data_type) {
            return Err(SchemaVisitError::InvalidSchemaValue {
                source: SchemaValueError::InvalidSnapshotFieldValue {
                    field_name: field_name.to_owned(),
                    source: DataModelValueError::InvalidSnapshotValueForType,
                },
            });
        }

        validate_snapshot_state_value_for_type(&schema_field.data_type, value.clone()).map_err(
            |source| SchemaVisitError::InvalidSchemaValue {
                source: SchemaValueError::InvalidSnapshotFieldValue {
                    field_name: field_name.to_owned(),
                    source,
                },
            },
        )?;

        self.visitor
            .state_field(field_name, value)
            .map_err(|source| SchemaVisitError::Visitor { source })
    }

    pub(in crate::schema::datamodel) fn prepare_latest_value_wins_field<'s>(
        &'s mut self,
        field_name: &str,
    ) -> Result<V::LatestValueWinsFieldSink<'s>, SchemaVisitError<V::Error>> {
        self.register_field(field_name)
            .map_err(|source| SchemaVisitError::InvalidSchemaValue { source })?;
        let schema_field = self
            .schema
            .field(field_name)
            .expect("field existence was validated");
        let ReplicatedDataType::LatestValueWins { value_type } = &schema_field.data_type else {
            return Err(SchemaVisitError::InvalidSchemaValue {
                source: SchemaValueError::InvalidSnapshotFieldValue {
                    field_name: field_name.to_owned(),
                    source: DataModelValueError::InvalidSnapshotValueForType,
                },
            });
        };

        self.visitor
            .prepare_latest_value_wins_field(field_name, value_type)
            .map_err(|source| SchemaVisitError::Visitor { source })
    }

    pub(in crate::schema::datamodel) fn prepare_linear_string_field<'s>(
        &'s mut self,
        field_name: &str,
    ) -> Result<V::LinearStringFieldSink<'s>, SchemaVisitError<V::Error>> {
        self.register_field(field_name)
            .map_err(|source| SchemaVisitError::InvalidSchemaValue { source })?;
        let schema_field = self
            .schema
            .field(field_name)
            .expect("field existence was validated");
        if !matches!(schema_field.data_type, ReplicatedDataType::LinearString) {
            return Err(SchemaVisitError::InvalidSchemaValue {
                source: SchemaValueError::InvalidSnapshotFieldValue {
                    field_name: field_name.to_owned(),
                    source: DataModelValueError::InvalidSnapshotValueForType,
                },
            });
        }

        self.visitor
            .prepare_linear_string_field(field_name)
            .map_err(|source| SchemaVisitError::Visitor { source })
    }

    pub(in crate::schema::datamodel) fn prepare_linear_list_field<'s>(
        &'s mut self,
        field_name: &str,
    ) -> Result<V::LinearListFieldSink<'s>, SchemaVisitError<V::Error>> {
        self.register_field(field_name)
            .map_err(|source| SchemaVisitError::InvalidSchemaValue { source })?;
        let schema_field = self
            .schema
            .field(field_name)
            .expect("field existence was validated");
        let ReplicatedDataType::LinearList { value_type } = &schema_field.data_type else {
            return Err(SchemaVisitError::InvalidSchemaValue {
                source: SchemaValueError::InvalidSnapshotFieldValue {
                    field_name: field_name.to_owned(),
                    source: DataModelValueError::InvalidSnapshotValueForType,
                },
            });
        };

        self.visitor
            .prepare_linear_list_field(field_name, *value_type)
            .map_err(|source| SchemaVisitError::Visitor { source })
    }

    pub(in crate::schema::datamodel) fn end(self) -> Result<(), SchemaVisitError<V::Error>> {
        if self.seen_fields.len() != self.schema.len()
            && let Some(missing_field) = self.schema.first_missing_field(&self.seen_fields)
        {
            return Err(SchemaVisitError::InvalidSchemaValue {
                source: SchemaValueError::MissingField {
                    field_name: missing_field.to_owned(),
                },
            });
        }

        self.visitor
            .end()
            .map_err(|source| SchemaVisitError::Visitor { source })
    }
}

/// Prepare a streaming schema snapshot writer.
///
/// This calls `SchemaSnapshotEncoder::begin` immediately with the schema's field count.
/// The resulting writer enforces:
/// - only known schema fields can be emitted
/// - no duplicate fields
/// - complete snapshots (`end` fails if any field is missing)
pub(crate) fn prepare_schema_snapshot_encoder<'a, Id, V>(
    visitor: &'a mut V,
    schema: &'a dyn SchemaSnapshotFieldView,
) -> Result<SchemaSnapshotEncodingWriter<'a, Id, V>, SchemaVisitError<V::Error>>
where
    V: SchemaSnapshotEncoder<Id>,
{
    visitor
        .begin(schema.len())
        .map_err(|source| SchemaVisitError::Visitor { source })?;

    Ok(SchemaSnapshotEncodingWriter {
        schema,
        visitor,
        seen_fields: HashSet::new(),
        _id_marker: PhantomData,
    })
}
