//! Summary exchange, initial-state, and schema metadata.

use super::*;

/// Request one peer's current version vector for a group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SummaryRequest {
    /// Replication group whose progress should be reported.
    pub group_id: GroupId,
    /// Group member that should answer the request.
    pub target: MemberIdentity,
}

/// One peer's reported replication progress for a group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Summary {
    /// Replication group described by this summary.
    pub group_id: GroupId,
    /// Member that produced this summary.
    pub responder: MemberIdentity,
    /// Group version vector currently known to `responder`.
    pub has_versions: VersionVector,
}

/// One row entry in an initial dataset's value rows.
#[derive(Clone, PartialEq, Eq)]
pub struct InitialValueRow {
    pub row_key: RowKey,
    pub row: RowValues,
}

impl fmt::Debug for InitialValueRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InitialValueRow")
            .field("row_key", &self.row_key)
            .field("field_count", &self.row.field_count())
            .finish()
    }
}

/// Initial value rows for one dataset.
#[derive(Clone, PartialEq, Eq)]
pub struct InitialDatasetValueRows {
    pub dataset_id: DatasetId,
    pub rows: Vec<InitialValueRow>,
}

impl fmt::Debug for InitialDatasetValueRows {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InitialDatasetValueRows")
            .field("dataset_id", &self.dataset_id)
            .field("row_count", &self.rows.len())
            .finish()
    }
}

/// Initial group value rows grouped by dataset.
//
// TODO(flotsync-git-d3w): Ensure all nodes deterministically derive equivalent
// initial CRDT state from these values during group creation/migration. If data
// types such as LinearString would diverge on generated ids, redesign this to
// carry state instead of values.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct InitialGroupValueRows {
    pub datasets: Vec<InitialDatasetValueRows>,
}

impl InitialGroupValueRows {
    fn row_count(&self) -> usize {
        self.datasets.iter().map(|dataset| dataset.rows.len()).sum()
    }
}

impl fmt::Debug for InitialGroupValueRows {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InitialGroupValueRows")
            .field("dataset_count", &self.datasets.len())
            .field("row_count", &self.row_count())
            .finish()
    }
}

/// Fixed schema for one dataset in a replication group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatasetSchema {
    /// Dataset governed by this schema.
    pub dataset_id: DatasetId,
    /// Schema fixed for `dataset_id` for the lifetime of its group.
    pub schema: SchemaSource,
}

/// Dataset schemas fixed for the lifetime of a replication group.
///
/// Schema changes require migrating to a new group.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct GroupSchema {
    /// Schemas keyed by dataset.
    datasets: HashMap<DatasetId, SchemaSource>,
}

impl GroupSchema {
    /// Build a group schema from per-dataset schemas keyed by dataset id.
    #[must_use]
    pub fn new(datasets: HashMap<DatasetId, SchemaSource>) -> Self {
        Self { datasets }
    }

    /// Insert a repeated schema entry while rejecting duplicate dataset ids.
    ///
    /// This is intended for codecs that decode repeated protobuf schema entries.
    /// Ordinary callers should construct [`GroupSchema`] from a map instead.
    ///
    /// # Errors
    ///
    /// Returns [`GroupSchemaError::DuplicateDataset`] when this schema already
    /// contains an entry for `dataset_schema.dataset_id`.
    pub fn insert_checked(
        &mut self,
        dataset_schema: DatasetSchema,
    ) -> Result<(), GroupSchemaError> {
        match self.datasets.entry(dataset_schema.dataset_id) {
            Entry::Vacant(entry) => {
                entry.insert(dataset_schema.schema);
                Ok(())
            }
            Entry::Occupied(entry) => {
                let dataset_id = entry.key().clone();
                group_schema::DuplicateDatasetSnafu { dataset_id }.fail()
            }
        }
    }

    /// Return the per-dataset schema entries in deterministic dataset-id order.
    #[must_use]
    pub fn datasets(&self) -> Vec<DatasetSchema> {
        let mut datasets = self
            .datasets
            .iter()
            .map(|(dataset_id, schema)| DatasetSchema {
                dataset_id: dataset_id.clone(),
                schema: schema.clone(),
            })
            .collect::<Vec<_>>();
        datasets.sort_by(|left, right| left.dataset_id.cmp(&right.dataset_id));
        datasets
    }

    /// Return the schema for `dataset_id`, if this group declares it.
    #[must_use]
    pub fn schema(&self, dataset_id: &DatasetId) -> Option<&SchemaSource> {
        self.datasets.get(dataset_id)
    }

    /// Return whether this group declares a schema for `dataset_id`.
    #[must_use]
    pub fn contains_dataset(&self, dataset_id: &DatasetId) -> bool {
        self.datasets.contains_key(dataset_id)
    }

    /// Return whether this value and `other` contain the same dataset schema definitions.
    ///
    /// Unlike [`PartialEq`], this comparison deliberately ignores each
    /// [`SchemaSource`]'s ownership representation and compares the underlying
    /// [`Schema`] values structurally.
    #[must_use]
    pub fn has_same_schema_definitions(&self, other: &Self) -> bool {
        self.datasets.len() == other.datasets.len()
            && self.datasets.iter().all(|(dataset_id, schema)| {
                other
                    .datasets
                    .get(dataset_id)
                    .is_some_and(|other_schema| schema.as_schema() == other_schema.as_schema())
            })
    }

    /// Consume this group schema and return its dataset schema map.
    pub(crate) fn into_datasets(self) -> HashMap<DatasetId, SchemaSource> {
        self.datasets
    }

    /// Return the number of dataset schemas in this group schema.
    #[must_use]
    pub fn len(&self) -> usize {
        self.datasets.len()
    }

    /// Return whether this group schema has no dataset schemas.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.datasets.is_empty()
    }
}

impl fmt::Debug for GroupSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let alternate = f.alternate();
        let mut debug = f.debug_struct("GroupSchema");
        if alternate {
            debug.field("datasets", &self.datasets());
        } else {
            debug.field("dataset_count", &self.datasets.len());
        }
        debug.finish()
    }
}

/// Failed construction of a group schema from repeated entries.
#[derive(Debug, Snafu)]
#[snafu(module(group_schema))]
pub enum GroupSchemaError {
    /// The repeated schema entries listed the same dataset more than once.
    #[snafu(display("Group schema contained duplicate dataset id '{dataset_id}'."))]
    DuplicateDataset { dataset_id: DatasetId },
}

/// Group/version reference for one materialised snapshot.
///
/// Multiple references may identify the same logical snapshot. During
/// migration, the old group's `final_versions` and the new group's zero vector
/// are expected to be equivalent references to the initial target-group state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotRef {
    /// Group whose version vector names the snapshot state.
    pub group_id: GroupId,
    /// Version vector that identifies the materialised state in `group_id`.
    pub versions: VersionVector,
}

/// Initial dataset contents required before a joined or migrated group becomes
/// externally active.
#[derive(Clone, Default, PartialEq, Eq)]
pub enum InitialSnapshot {
    /// The initial dataset contents are empty.
    ///
    /// This does not imply that the group was newly created.
    #[default]
    Empty,
    /// The initial value rows are carried directly in the invitation or proposal.
    Inline(InitialGroupValueRows),
    /// The initial contents must be resolved from metadata before activation.
    Metadata(InitialSnapshotMetadata),
}

impl fmt::Debug for InitialSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("InitialSnapshot::Empty"),
            Self::Inline(state) => f
                .debug_struct("InitialSnapshot::Inline")
                .field("dataset_count", &state.datasets.len())
                .field("row_count", &state.row_count())
                .finish(),
            Self::Metadata(metadata) => f
                .debug_tuple("InitialSnapshot::Metadata")
                .field(metadata)
                .finish(),
        }
    }
}

impl InitialSnapshot {
    /// Return whether activation needs snapshot data that is not carried inline.
    #[must_use]
    pub fn requires_snapshot_fetch(&self) -> bool {
        matches!(self, Self::Metadata(_))
    }
}

/// Metadata for an initial snapshot that is not carried inline.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitialSnapshotMetadata {
    /// Primary reference the recipient should use when reasoning about this snapshot.
    pub primary_ref: SnapshotRef,
    /// Other group/version references that identify the same logical snapshot.
    ///
    /// Not required to be exhaustive.
    pub equivalent_refs: SmallVec<[SnapshotRef; 1]>,
    /// Optional number of row records expected in the snapshot.
    pub record_count: Option<u64>,
}
