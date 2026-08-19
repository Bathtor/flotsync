//! Process-static application schema registration.

use super::{DatasetId, DatasetIdError};
use flotsync_data_types::schema::Schema;
use snafu::prelude::*;
use std::{cmp::Ordering, sync::LazyLock};

/// Schema definitions compiled into an application and available to replication runtimes.
///
/// Applications normally construct one instance in [`std::sync::LazyLock`] and pass a
/// `&'static ApplicationSchemas` to every runtime they load. The registry intentionally hides its
/// backing collection so its read-optimised representation can change without affecting callers.
#[derive(Debug)]
pub struct ApplicationSchemas {
    /// Process-static schemas ordered by identifier byte length and then contents.
    schemas: Option<Box<[(&'static str, &'static Schema)]>>,
}

impl ApplicationSchemas {
    /// Empty application schema registry requiring no allocation or lazy initialisation.
    pub const EMPTY: Self = Self { schemas: None };

    /// Build an application schema registry from process-static definitions.
    ///
    /// The registry takes no ownership of identifiers or schemas. Construction validates the
    /// entries and compacts them into a read-only representation optimised for lookup.
    ///
    /// # Errors
    ///
    /// Returns [`ApplicationSchemasError::InvalidDatasetId`] when an identifier is not a valid
    /// [`DatasetId`], or [`ApplicationSchemasError::DuplicateDataset`] when an identifier occurs
    /// more than once.
    pub fn try_from_entries(
        entries: impl IntoIterator<Item = (&'static str, &'static Schema)>,
    ) -> Result<Self, ApplicationSchemasError> {
        let mut schemas = entries.into_iter().collect::<Vec<_>>();
        for &(dataset_id, _) in &schemas {
            DatasetId::validate(dataset_id).context(InvalidDatasetIdSnafu { dataset_id })?;
        }

        schemas.sort_unstable_by(|(left, _), (right, _)| compare_dataset_ids(left, right));
        for adjacent in schemas.windows(2) {
            let [(left, _), (right, _)] = adjacent else {
                unreachable!("two-entry windows always contain two entries")
            };
            if left == right {
                return DuplicateDatasetSnafu { dataset_id: *right }.fail();
            }
        }

        let schemas = if schemas.is_empty() {
            None
        } else {
            Some(schemas.into_boxed_slice())
        };
        Ok(Self { schemas })
    }

    /// Build an application schema registry containing exactly one process-static definition.
    ///
    /// # Errors
    ///
    /// Returns [`ApplicationSchemasError::InvalidDatasetId`] when `dataset_id` is not a valid
    /// [`DatasetId`].
    pub fn try_from_entry(
        dataset_id: &'static str,
        schema: &'static Schema,
    ) -> Result<Self, ApplicationSchemasError> {
        DatasetId::validate(dataset_id).context(InvalidDatasetIdSnafu { dataset_id })?;
        Ok(Self {
            schemas: Some(Box::new([(dataset_id, schema)])),
        })
    }

    /// Build an application schema registry from process-static lazy schema definitions.
    ///
    /// Every supplied [`LazyLock`] is initialised while constructing the registry.
    ///
    /// # Errors
    ///
    /// See [`Self::try_from_entries`] for failure conditions.
    pub fn try_from_lazy_entries(
        entries: impl IntoIterator<Item = (&'static str, &'static LazyLock<Schema>)>,
    ) -> Result<Self, ApplicationSchemasError> {
        let entries = entries
            .into_iter()
            .map(|(dataset_id, schema)| (dataset_id, LazyLock::force(schema)));
        Self::try_from_entries(entries)
    }

    /// Build a one-entry registry from a process-static lazy schema definition.
    ///
    /// The supplied [`LazyLock`] is initialised while constructing the registry.
    ///
    /// # Errors
    ///
    /// Returns [`ApplicationSchemasError::InvalidDatasetId`] when `dataset_id` is not a valid
    /// [`DatasetId`].
    pub fn try_from_lazy_entry(
        dataset_id: &'static str,
        schema: &'static LazyLock<Schema>,
    ) -> Result<Self, ApplicationSchemasError> {
        Self::try_from_entry(dataset_id, LazyLock::force(schema))
    }

    /// Return the registered schema for `dataset_id`, if the application supplies one.
    #[must_use]
    pub fn get(&self, dataset_id: &DatasetId) -> Option<&'static Schema> {
        let schemas = self.as_slice();
        let index = schemas
            .binary_search_by(|(candidate, _)| compare_dataset_ids(candidate, dataset_id.as_str()))
            .ok()?;
        Some(schemas[index].1)
    }

    /// Return whether the registry contains no application schemas.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.schemas.is_none()
    }

    /// Return the number of registered application schemas.
    #[must_use]
    pub fn len(&self) -> usize {
        self.as_slice().len()
    }

    /// Iterate over every registered dataset identifier and schema.
    ///
    /// The iteration order is unspecified and may change with the private registry
    /// representation. Returned identifiers borrow the registry's validated process-static keys,
    /// so constructing them neither allocates nor repeats validation.
    #[must_use]
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (DatasetId, &'static Schema)> + '_ {
        self.as_slice()
            .iter()
            .map(|&(dataset_id, schema)| (DatasetId::from_static_unchecked(dataset_id), schema))
    }

    /// Return the registry entries as an empty or populated slice.
    fn as_slice(&self) -> &[(&'static str, &'static Schema)] {
        self.schemas.as_deref().unwrap_or_default()
    }
}

/// Invalid process-static application schema registration.
#[derive(Debug, Snafu)]
pub enum ApplicationSchemasError {
    /// One registry key is not a valid replication dataset identifier.
    #[snafu(display("Invalid application dataset identifier '{dataset_id}': {source}"))]
    InvalidDatasetId {
        /// Invalid process-static identifier supplied by the application.
        dataset_id: &'static str,
        /// Dataset identifier validation failure.
        source: DatasetIdError,
    },
    /// The registry supplied the same dataset identifier more than once.
    #[snafu(display(
        "Application dataset identifier '{dataset_id}' is registered more than once."
    ))]
    DuplicateDataset {
        /// Repeated process-static identifier supplied by the application.
        dataset_id: &'static str,
    },
}

/// Compare validated dataset identifier strings in registry storage order.
fn compare_dataset_ids(left: &str, right: &str) -> Ordering {
    left.len().cmp(&right.len()).then_with(|| left.cmp(right))
}

#[cfg(test)]
mod tests {
    use super::*;
    use flotsync_data_types::{Field, Schema};
    use std::sync::LazyLock;

    static TITLE_SCHEMA: LazyLock<Schema> =
        LazyLock::new(|| Schema::from_fields([Field::linear_string("title")]));
    static NOTE_SCHEMA: LazyLock<Schema> =
        LazyLock::new(|| Schema::from_fields([Field::linear_string("note")]));

    #[test]
    fn registry_finds_entries_across_length_and_lexical_partitions() {
        let schemas = ApplicationSchemas::try_from_lazy_entries([
            ("long_notes", &NOTE_SCHEMA),
            ("bb", &NOTE_SCHEMA),
            ("a", &TITLE_SCHEMA),
            ("aa", &TITLE_SCHEMA),
        ])
        .expect("application schemas should build");

        assert_eq!(schemas.len(), 4);
        assert!(!schemas.is_empty());
        let title_schema: &Schema = &TITLE_SCHEMA;
        let note_schema: &Schema = &NOTE_SCHEMA;
        for (dataset_id, expected_schema) in [
            ("a", title_schema),
            ("aa", title_schema),
            ("bb", note_schema),
            ("long_notes", note_schema),
        ] {
            let dataset_id =
                DatasetId::try_from_static(dataset_id).expect("dataset id should build");
            let registered_schema = schemas
                .get(&dataset_id)
                .expect("schema should be registered");
            assert!(std::ptr::eq(registered_schema, expected_schema));
        }
        assert!(
            schemas
                .get(
                    &DatasetId::try_from_static("missing_overlong")
                        .expect("dataset id should build"),
                )
                .is_none()
        );
    }

    #[test]
    fn empty_single_and_lazy_constructors_preserve_static_schemas() {
        assert!(ApplicationSchemas::EMPTY.is_empty());
        assert_eq!(ApplicationSchemas::EMPTY.len(), 0);
        assert_eq!(ApplicationSchemas::EMPTY.iter().len(), 0);

        let direct = ApplicationSchemas::try_from_entry("docs", &TITLE_SCHEMA)
            .expect("single application schema should build");
        let lazy = ApplicationSchemas::try_from_lazy_entry("docs", &TITLE_SCHEMA)
            .expect("single lazy application schema should build");
        let dataset_id = DatasetId::try_from_static("docs").expect("dataset id should build");
        assert!(std::ptr::eq(
            direct.get(&dataset_id).expect("direct schema should exist"),
            lazy.get(&dataset_id).expect("lazy schema should exist"),
        ));
    }

    #[test]
    fn iteration_returns_complete_identifier_to_schema_mapping() {
        let schemas = ApplicationSchemas::try_from_lazy_entries([
            ("notes", &NOTE_SCHEMA),
            ("docs", &TITLE_SCHEMA),
        ])
        .expect("application schemas should build");

        let iterated = schemas.iter().collect::<std::collections::HashMap<_, _>>();
        assert_eq!(iterated.len(), 2);
        let docs = DatasetId::try_from_static("docs").expect("dataset id should build");
        let notes = DatasetId::try_from_static("notes").expect("dataset id should build");
        let title_schema: &Schema = &TITLE_SCHEMA;
        let note_schema: &Schema = &NOTE_SCHEMA;
        assert!(std::ptr::eq(
            iterated
                .get(&docs)
                .copied()
                .expect("docs schema should be iterated"),
            title_schema,
        ));
        assert!(std::ptr::eq(
            iterated
                .get(&notes)
                .copied()
                .expect("notes schema should be iterated"),
            note_schema,
        ));
    }

    #[test]
    fn registry_rejects_invalid_and_duplicate_identifiers() {
        let invalid = ApplicationSchemas::try_from_lazy_entry("not-valid", &TITLE_SCHEMA)
            .expect_err("invalid dataset identifier should fail");
        assert!(matches!(
            invalid,
            ApplicationSchemasError::InvalidDatasetId {
                dataset_id: "not-valid",
                ..
            }
        ));

        let duplicate = ApplicationSchemas::try_from_lazy_entries([
            ("docs", &TITLE_SCHEMA),
            ("a", &TITLE_SCHEMA),
            ("docs", &NOTE_SCHEMA),
        ])
        .expect_err("duplicate dataset identifier should fail");
        assert!(matches!(
            duplicate,
            ApplicationSchemasError::DuplicateDataset { dataset_id: "docs" }
        ));
    }
}
