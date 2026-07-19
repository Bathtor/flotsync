//! Change publication requests and provider contracts.

use super::*;

/// Write-only row payload submitted by applications.
///
/// For a new row this must contain the initial values required by the dataset
/// schema. For an existing row this is a sparse field patch: fields omitted from
/// `fields` are intentionally left unchanged by `publish_changes`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RowValuesPatch {
    /// Desired nullable field values keyed by field name.
    pub fields: HashMap<String, NullableBasicValue>,
}

impl RowValuesPatch {
    #[must_use]
    pub fn new(fields: HashMap<String, NullableBasicValue>) -> Self {
        Self { fields }
    }
}

/// A row-level mutation submitted by an application.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RowMutation {
    /// Insert a new row or update the provided fields of an existing row.
    Upsert { row_id: RowId, row: RowValuesPatch },
    /// Tombstone an existing row.
    Delete { row_id: RowId },
}

impl RowMutation {
    #[must_use]
    pub fn row_id(&self) -> &RowId {
        match self {
            RowMutation::Upsert { row_id, .. } | RowMutation::Delete { row_id } => row_id,
        }
    }
}

/// Opaque read-position token returned by the replication runtime.
///
/// Applications should store this value alongside their in-memory application
/// state and pass it back to [`ReplicationApi::publish_changes`].
#[derive(Clone, PartialEq, Eq)]
pub struct ReadToken {
    // Developer note: the token intentionally hides its group-scoped version
    // vectors from applications. Runtime internals remain responsible for
    // validating group compatibility and advancing the right producer position.
    versions: Arc<ReadTokenVersions>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReadTokenVersions {
    pub(crate) groups: HashMap<GroupId, VersionVector>,
}

impl ReadToken {
    pub(crate) fn from_group_versions(groups: HashMap<GroupId, VersionVector>) -> Self {
        Self {
            versions: Arc::new(ReadTokenVersions { groups }),
        }
    }

    pub(crate) fn group_version(&self, group_id: &GroupId) -> Option<&VersionVector> {
        self.versions.groups.get(group_id)
    }

    pub(crate) fn group_count(&self) -> usize {
        self.versions.groups.len()
    }

    pub(crate) fn with_group_version(
        &self,
        group_id: GroupId,
        version_vector: VersionVector,
    ) -> Self {
        let mut groups = self.versions.groups.clone();
        groups.insert(group_id, version_vector);
        Self::from_group_versions(groups)
    }

    pub(crate) fn with_update_applied(&self, group_id: GroupId, update_id: UpdateId) -> Self {
        let Some(group_versions) = self.group_version(&group_id) else {
            return self.clone();
        };
        self.with_group_version(group_id, group_versions.with_update_applied(update_id))
    }

    /// Merge an event or snapshot token into this application read position.
    ///
    /// This is safe for applications that publish locally while listener
    /// events are still queued: the merge keeps the furthest-known version for
    /// every group instead of replacing newer local progress with an older
    /// event token.
    ///
    /// # Panics
    ///
    /// Panics if both tokens contain the same group with incompatible member
    /// counts.
    pub fn merge_applied(&mut self, applied: &ReadToken) {
        let versions = Arc::make_mut(&mut self.versions);
        for (group_id, applied_versions) in &applied.versions.groups {
            let merged_versions = match versions.groups.get(group_id) {
                Some(existing_versions) => existing_versions.least_upper_bound(applied_versions),
                None => applied_versions.clone(),
            };
            versions.groups.insert(*group_id, merged_versions);
        }
    }
}

impl std::fmt::Debug for ReadToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            return f
                .debug_struct("ReadToken")
                .field("groups", &self.versions.groups)
                .finish();
        }
        f.debug_struct("ReadToken")
            .field("group_count", &self.group_count())
            .finish_non_exhaustive()
    }
}

/// Request to publish one local set of row mutations from a known read token.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishChangesRequest {
    /// Opaque read position for the application state this change was based on.
    pub read_token: ReadToken,
    /// Row mutations to publish.
    pub changes: Vec<RowMutation>,
}

/// Row-level change emitted by the framework to an application listener.
pub enum RowChange {
    Upsert {
        row_id: RowId,
        row: Arc<dyn RowValueRead + Send + Sync>,
    },
    Delete {
        row_id: RowId,
    },
}

impl RowChange {
    #[must_use]
    pub fn row_id(&self) -> &RowId {
        match self {
            RowChange::Upsert { row_id, .. } | RowChange::Delete { row_id } => row_id,
        }
    }

    #[must_use]
    pub fn row(&self) -> Option<&(dyn RowValueRead + Send + Sync)> {
        match self {
            RowChange::Upsert { row, .. } => Some(row.as_ref()),
            RowChange::Delete { .. } => None,
        }
    }
}

/// One owned batch emitted by a [`BatchProvider`].
pub trait ProviderBatch: Send + 'static {
    /// Remove previously emitted rows before reusing this allocation.
    fn clear(&mut self);
    /// Return whether the batch currently contains no rows.
    fn is_empty(&self) -> bool;
}

impl<T> ProviderBatch for Vec<T>
where
    T: Send + 'static,
{
    fn clear(&mut self) {
        Vec::clear(self);
    }

    fn is_empty(&self) -> bool {
        Vec::is_empty(self)
    }
}

impl<A> ProviderBatch for SmallVec<A>
where
    A: Array + Send + 'static,
    A::Item: Send + 'static,
{
    fn clear(&mut self) {
        SmallVec::clear(self);
    }

    fn is_empty(&self) -> bool {
        SmallVec::is_empty(self)
    }
}

/// Source for owned batches with explicit end-of-stream signalling.
///
/// `Some(batch)` means at least one row was emitted. `None` means the provider
/// is exhausted. `fill_batch` accepts the previous batch by value so
/// implementations may reuse, move, or hand the allocation to another thread
/// while preparing the next batch.
pub trait BatchProvider: Send {
    type Batch: ProviderBatch;

    /// Allocate a fresh empty batch using this provider's batching policy.
    fn new_batch(&self) -> Self::Batch;

    fn fill_batch(
        &mut self,
        reuse: Self::Batch,
    ) -> BoxFuture<'_, Result<Option<Self::Batch>, RowProviderError>>;

    fn next_batch(&mut self) -> BoxFuture<'_, Result<Option<Self::Batch>, RowProviderError>> {
        let reuse = self.new_batch();
        self.fill_batch(reuse)
    }
}

/// Process all batches from `provider`, reusing each emitted batch allocation.
///
/// `process` receives the batch mutably so it can use concrete batch APIs such
/// as `drain` before the allocation is handed back to the provider.
///
/// # Errors
///
/// Returns provider errors from fetching a batch or processing errors returned
/// by `process`.
pub async fn process_batches<B>(
    provider: &mut dyn BatchProvider<Batch = B>,
    mut process: impl FnMut(&mut B) -> Result<(), RowProviderError>,
) -> Result<(), RowProviderError>
where
    B: ProviderBatch,
{
    let mut batch = provider.new_batch();
    while let Some(mut filled_batch) = provider.fill_batch(batch).await? {
        process(&mut filled_batch)?;
        batch = filled_batch;
    }
    Ok(())
}

/// Batch of row changes emitted by a [`RowProvider`].
pub type RowChangeBatch = SmallVec<[RowChange; 4]>;

/// Source for batched row-level changes carried in `ReplicationEvent::DataChanged`.
pub type RowProvider = dyn BatchProvider<Batch = RowChangeBatch>;
