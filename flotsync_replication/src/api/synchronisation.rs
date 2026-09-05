//! Application startup synchronisation snapshot rows and bounded batches.

use super::{
    BatchProvider,
    InMemoryValueData,
    InMemoryValueDataRowRef,
    ProviderBatch,
    RowId,
    Schema,
    SchemaSource,
};

/// One borrowed visible row in an application startup snapshot.
pub type SnapshotRow<'a> = InMemoryValueDataRowRef<'a, RowId>;

/// Source for bounded snapshot-row batches within one group synchronisation.
pub type SnapshotRowProvider<'a> = dyn BatchProvider<Batch = SnapshotRowBatch> + 'a;

/// Compact batch of visible snapshot rows from one dataset.
pub struct SnapshotRowBatch {
    /// Compact value storage for one dataset batch.
    rows: InMemoryValueData<RowId>,
}

impl SnapshotRowBatch {
    /// Create an empty reusable snapshot-row batch.
    pub(crate) fn empty() -> Self {
        Self {
            rows: InMemoryValueData::new(Schema::empty()),
        }
    }

    /// Prepare this allocation for one dataset schema.
    pub(crate) fn prepare(
        &mut self,
        schema: impl Into<SchemaSource>,
        row_capacity: usize,
    ) -> &mut InMemoryValueData<RowId> {
        let schema = schema.into();
        if self.rows.schema() == schema.as_schema() {
            self.rows.clear_rows();
            self.rows.reserve_rows(row_capacity);
        } else {
            self.rows = InMemoryValueData::with_row_capacity(schema, row_capacity);
        }
        &mut self.rows
    }

    /// Return the number of visible rows in this batch.
    #[must_use]
    pub fn row_count(&self) -> usize {
        self.rows.row_count()
    }

    /// Return whether this batch contains no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Iterate the visible snapshot rows in this batch.
    pub fn rows(&self) -> impl Iterator<Item = SnapshotRow<'_>> {
        self.rows.rows()
    }

    /// Remove every row while retaining reusable allocation.
    pub(crate) fn clear(&mut self) {
        self.rows.clear_rows();
    }
}

impl ProviderBatch for SnapshotRowBatch {
    fn clear(&mut self) {
        SnapshotRowBatch::clear(self);
    }

    fn is_empty(&self) -> bool {
        SnapshotRowBatch::is_empty(self)
    }
}

impl std::fmt::Debug for SnapshotRowBatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SnapshotRowBatch")
            .field("row_count", &self.row_count())
            .finish_non_exhaustive()
    }
}
