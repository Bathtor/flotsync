//! Positional schema layouts derived from unordered [`Schema`] values.

use ahash::RandomState;
use flotsync_utils::ProjectedIndexHashMap;
use std::{collections::HashMap, fmt, iter::FusedIterator, slice};

use super::{Field, FieldNameProjection, Schema};

/// A schema with some field order suitable for positional processing.
///
/// Construction uses the current iteration order of the source [`Schema`]
/// fields. Callers cannot select that order, and separately constructed
/// instances are not guaranteed to use the same order. Once constructed, the
/// order remains fixed and can safely define a positional row layout.
pub struct OrderedSchema<'schema> {
    /// Borrowed or owned field and metadata storage.
    repr: OrderedSchemaRepr<'schema>,
}

impl<'schema> OrderedSchema<'schema> {
    /// Build a positional layout borrowing all fields and metadata from `schema`.
    ///
    /// This allocates the internal structure but does not clone fields or field
    /// names.
    #[must_use]
    pub fn from_schema(schema: &'schema Schema) -> Self {
        let fields = ProjectedIndexHashMap::from_values_unchecked_with_hasher(
            schema.columns.iter(),
            RandomState::new(),
        );
        Self {
            repr: OrderedSchemaRepr::Borrowed {
                fields,
                metadata: &schema.metadata,
            },
        }
    }

    /// Return true iff the schema contains no fields.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the number of fields in this positional layout.
    #[must_use]
    pub fn len(&self) -> usize {
        match &self.repr {
            OrderedSchemaRepr::Borrowed { fields, .. } => fields.len(),
            OrderedSchemaRepr::Owned { fields, .. } => fields.len(),
        }
    }

    /// Look up a field by name.
    #[must_use]
    pub fn field(&self, field_name: &str) -> Option<&Field> {
        match &self.repr {
            OrderedSchemaRepr::Borrowed { fields, .. } => fields.get(field_name).copied(),
            OrderedSchemaRepr::Owned { fields, .. } => fields.get(field_name),
        }
    }

    /// Return the field's index in this ordered schema.
    #[must_use]
    pub fn field_index(&self, field_name: &str) -> Option<usize> {
        match &self.repr {
            OrderedSchemaRepr::Borrowed { fields, .. } => fields.get_index_of(field_name),
            OrderedSchemaRepr::Owned { fields, .. } => fields.get_index_of(field_name),
        }
    }

    /// Return the field at `index`.
    #[must_use]
    pub fn field_at(&self, index: usize) -> Option<&Field> {
        match &self.repr {
            OrderedSchemaRepr::Borrowed { fields, .. } => fields.get_index(index).copied(),
            OrderedSchemaRepr::Owned { fields, .. } => fields.get_index(index),
        }
    }

    /// Iterate over fields in this ordered schema's field order.
    #[must_use]
    pub fn fields(&self) -> OrderedSchemaFields<'_, 'schema> {
        let repr = match &self.repr {
            OrderedSchemaRepr::Borrowed { fields, .. } => {
                OrderedSchemaFieldsRepr::Borrowed(fields.iter())
            }
            OrderedSchemaRepr::Owned { fields, .. } => {
                OrderedSchemaFieldsRepr::Owned(fields.iter())
            }
        };
        OrderedSchemaFields { repr }
    }

    /// Return the schema metadata.
    #[must_use]
    pub fn metadata(&self) -> &HashMap<String, String> {
        match &self.repr {
            OrderedSchemaRepr::Borrowed { metadata, .. } => metadata,
            OrderedSchemaRepr::Owned { metadata, .. } => metadata,
        }
    }

    /// Compare two ordered schemas without considering their positional order.
    ///
    /// Metadata is compared normally. Fields are matched by name and then
    /// compared in full.
    #[must_use]
    pub fn eq_unordered(&self, other: &OrderedSchema<'_>) -> bool {
        self.metadata() == other.metadata()
            && self.len() == other.len()
            && self
                .fields()
                .all(|field| other.field(&field.name) == Some(field))
    }

    /// Convert this wrapper into a fully owned ordered schema.
    ///
    /// An already-owned representation retains its field storage and index.
    /// A borrowed representation clones every field and the metadata once while
    /// preserving the existing positional order.
    #[must_use]
    pub fn into_owned(self) -> OrderedSchema<'static> {
        match self.repr {
            OrderedSchemaRepr::Borrowed { fields, metadata } => {
                let owned_fields = fields.into_values().into_vec().into_iter().cloned();
                let owned_fields = ProjectedIndexHashMap::from_values_unchecked_with_hasher(
                    owned_fields,
                    RandomState::new(),
                );
                OrderedSchema {
                    repr: OrderedSchemaRepr::Owned {
                        fields: owned_fields,
                        metadata: metadata.clone(),
                    },
                }
            }
            OrderedSchemaRepr::Owned { fields, metadata } => OrderedSchema {
                repr: OrderedSchemaRepr::Owned { fields, metadata },
            },
        }
    }
}

impl OrderedSchema<'static> {
    /// Build a positional layout by consuming `schema` without cloning fields.
    ///
    /// The resulting order is the field iteration order of the consumed schema.
    #[must_use]
    pub fn from_owned_schema(schema: Schema) -> Self {
        let Schema { columns, metadata } = schema;
        let fields =
            ProjectedIndexHashMap::from_values_unchecked_with_hasher(columns, RandomState::new());
        Self {
            repr: OrderedSchemaRepr::Owned { fields, metadata },
        }
    }
}

impl Clone for OrderedSchema<'_> {
    fn clone(&self) -> Self {
        Self {
            repr: self.repr.clone(),
        }
    }
}

impl fmt::Debug for OrderedSchema<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OrderedSchema")
            .field("fields", &self.fields().collect::<Vec<_>>())
            .field("metadata", self.metadata())
            .finish()
    }
}

impl<'right> PartialEq<OrderedSchema<'right>> for OrderedSchema<'_> {
    /// Compare metadata and fields in positional order.
    ///
    /// Use [`Self::eq_unordered`] when position is irrelevant.
    fn eq(&self, other: &OrderedSchema<'right>) -> bool {
        self.metadata() == other.metadata() && self.fields().eq(other.fields())
    }
}

impl Eq for OrderedSchema<'_> {}

impl PartialEq<Schema> for OrderedSchema<'_> {
    /// Compare metadata and fields without considering field position.
    fn eq(&self, other: &Schema) -> bool {
        self.metadata() == &other.metadata
            && self.len() == other.columns.len()
            && self
                .fields()
                .all(|field| other.columns.get(&field.name) == Some(field))
    }
}

impl PartialEq<OrderedSchema<'_>> for Schema {
    /// Compare metadata and fields without considering field position.
    fn eq(&self, other: &OrderedSchema<'_>) -> bool {
        other == self
    }
}

/// Iterator over fields in one [`OrderedSchema`]'s field order.
pub struct OrderedSchemaFields<'ordered, 'schema>
where
    'schema: 'ordered,
{
    /// Iterator for the active storage representation.
    repr: OrderedSchemaFieldsRepr<'ordered, 'schema>,
}

impl<'ordered, 'schema> Iterator for OrderedSchemaFields<'ordered, 'schema>
where
    'schema: 'ordered,
{
    type Item = &'ordered Field;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.repr {
            OrderedSchemaFieldsRepr::Borrowed(fields) => fields.next().copied(),
            OrderedSchemaFieldsRepr::Owned(fields) => fields.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.repr {
            OrderedSchemaFieldsRepr::Borrowed(fields) => fields.size_hint(),
            OrderedSchemaFieldsRepr::Owned(fields) => fields.size_hint(),
        }
    }
}

impl<'ordered, 'schema> DoubleEndedIterator for OrderedSchemaFields<'ordered, 'schema>
where
    'schema: 'ordered,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        match &mut self.repr {
            OrderedSchemaFieldsRepr::Borrowed(fields) => fields.next_back().copied(),
            OrderedSchemaFieldsRepr::Owned(fields) => fields.next_back(),
        }
    }
}

impl ExactSizeIterator for OrderedSchemaFields<'_, '_> {
    fn len(&self) -> usize {
        match &self.repr {
            OrderedSchemaFieldsRepr::Borrowed(fields) => fields.len(),
            OrderedSchemaFieldsRepr::Owned(fields) => fields.len(),
        }
    }
}

impl FusedIterator for OrderedSchemaFields<'_, '_> {}

/// Positional field storage using the cheap hasher selected for trusted schemas.
type OrderedFieldIndex<Value> = ProjectedIndexHashMap<Value, FieldNameProjection, RandomState>;

/// Borrowed or owned projected field storage.
#[derive(Clone)]
enum OrderedSchemaRepr<'schema> {
    /// References into an existing unordered schema.
    Borrowed {
        /// Fields indexed by their names.
        fields: OrderedFieldIndex<&'schema Field>,
        /// Metadata borrowed from the same schema.
        metadata: &'schema HashMap<String, String>,
    },
    /// Self-contained field and metadata storage.
    Owned {
        /// Fields indexed by their names.
        fields: OrderedFieldIndex<Field>,
        /// Owned schema metadata.
        metadata: HashMap<String, String>,
    },
}

/// Iterator representation matching [`OrderedSchemaRepr`].
enum OrderedSchemaFieldsRepr<'ordered, 'schema> {
    /// Iterates through stored references.
    Borrowed(slice::Iter<'ordered, &'schema Field>),
    /// Iterates through stored owned fields.
    Owned(slice::Iter<'ordered, Field>),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema_with_fields(fields: [Field; 3]) -> Schema {
        let mut schema = Schema::from_fields(fields);
        schema
            .metadata
            .insert("owner".to_owned(), "sync".to_owned());
        schema
    }

    #[test]
    fn borrowed_ordered_schema_reuses_original_fields() {
        let schema = schema_with_fields([
            Field::linear_string("title"),
            Field::monotonic_counter("count"),
            Field::linear_string("description"),
        ]);
        let ordered = OrderedSchema::from_schema(&schema);
        let source_order = schema.columns.iter().collect::<Vec<_>>();
        let ordered_fields = ordered.fields().collect::<Vec<_>>();

        assert_eq!(ordered_fields, source_order);
        for field in ordered.fields() {
            assert!(std::ptr::eq(field, schema.field(&field.name).unwrap()));
            assert_eq!(
                ordered
                    .field_index(&field.name)
                    .map(|index| ordered.field_at(index)),
                Some(Some(field))
            );
        }
        assert!(std::ptr::eq(ordered.metadata(), &raw const schema.metadata));
    }

    #[test]
    fn owned_ordered_schema_moves_fields_and_supports_lookup() {
        let schema = schema_with_fields([
            Field::linear_string("title"),
            Field::monotonic_counter("count"),
            Field::linear_string("description"),
        ]);
        let title_name_pointer = schema.field("title").unwrap().name.as_ptr();
        let ordered = OrderedSchema::from_owned_schema(schema);

        assert_eq!(
            ordered.field("title").map(|field| &field.name),
            Some(&"title".to_owned())
        );
        assert_eq!(
            ordered.field("title").unwrap().name.as_ptr(),
            title_name_pointer
        );
        assert_eq!(
            ordered.metadata().get("owner").map(String::as_str),
            Some("sync")
        );
    }

    #[test]
    fn converting_borrowed_schema_to_owned_preserves_order() {
        let schema = schema_with_fields([
            Field::linear_string("title"),
            Field::monotonic_counter("count"),
            Field::linear_string("description"),
        ]);
        let borrowed = OrderedSchema::from_schema(&schema);
        let names = borrowed
            .fields()
            .map(|field| field.name.clone())
            .collect::<Vec<_>>();
        let owned = borrowed.into_owned();

        assert_eq!(
            owned.fields().map(|field| &field.name).collect::<Vec<_>>(),
            names.iter().collect::<Vec<_>>()
        );
        assert!(!std::ptr::eq(
            owned.field("title").unwrap(),
            schema.field("title").unwrap()
        ));
    }

    #[test]
    fn ordered_equality_compares_positions() {
        let schema = schema_with_fields([
            Field::linear_string("title"),
            Field::monotonic_counter("count"),
            Field::linear_string("description"),
        ]);
        let first = OrderedSchema::from_schema(&schema);
        let reversed_fields = first.fields().rev().cloned().collect::<Vec<_>>();
        let reversed = OrderedSchema {
            repr: OrderedSchemaRepr::Owned {
                fields: ProjectedIndexHashMap::from_values_unchecked_with_hasher(
                    reversed_fields,
                    RandomState::new(),
                ),
                metadata: schema.metadata.clone(),
            },
        };

        assert_ne!(first, reversed);
        assert!(first.eq_unordered(&reversed));
        assert_eq!(first, schema);
        assert_eq!(schema, reversed);
    }

    #[test]
    fn base_schema_equality_remains_unordered() {
        let first = Schema::from_fields([
            Field::linear_string("title"),
            Field::monotonic_counter("count"),
            Field::linear_string("description"),
        ]);
        let second = Schema::from_fields([
            Field::linear_string("description"),
            Field::linear_string("title"),
            Field::monotonic_counter("count"),
        ]);

        assert_eq!(first, second);
    }

    #[test]
    fn ordered_equality_compares_metadata() {
        let schema = schema_with_fields([
            Field::linear_string("title"),
            Field::monotonic_counter("count"),
            Field::linear_string("description"),
        ]);
        let first = OrderedSchema::from_schema(&schema).into_owned();
        let mut changed_schema = schema.clone();
        changed_schema
            .metadata
            .insert("owner".to_owned(), "different".to_owned());
        let changed = OrderedSchema::from_owned_schema(changed_schema);

        assert_ne!(first, changed);
        assert!(!first.eq_unordered(&changed));
    }
}
