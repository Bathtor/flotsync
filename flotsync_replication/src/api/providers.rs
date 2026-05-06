use crate::api::{
    BatchProvider,
    BoxFuture,
    InitialRowBatch,
    InitialRowState,
    RowChange,
    RowChangeBatch,
    RowProviderError,
};
use futures_util::FutureExt;
use std::{collections::VecDeque, num::NonZeroUsize};

const DEFAULT_INITIAL_ROW_BATCH_SIZE: NonZeroUsize = NonZeroUsize::new(128).unwrap();

/// Convenience row-change provider backed by an in-memory `Vec`.
#[derive(Default)]
pub struct VecRowProvider {
    rows: Vec<RowChange>,
}

impl VecRowProvider {
    #[must_use]
    pub fn new(rows: Vec<RowChange>) -> Self {
        Self { rows }
    }
}

impl BatchProvider for VecRowProvider {
    type Batch = RowChangeBatch;

    fn new_batch(&self) -> Self::Batch {
        RowChangeBatch::new()
    }

    fn fill_batch(
        &mut self,
        mut reuse: Self::Batch,
    ) -> BoxFuture<'_, Result<Option<Self::Batch>, RowProviderError>> {
        async move {
            reuse.clear();
            reuse.extend(std::mem::take(&mut self.rows));
            if reuse.is_empty() {
                return Ok(None);
            }
            Ok(Some(reuse))
        }
        .boxed()
    }
}

/// Convenience initial-row provider backed by an in-memory `VecDeque`.
#[derive(Debug)]
pub struct VecInitialRowProvider {
    rows: VecDeque<InitialRowState>,
    max_rows_per_batch: NonZeroUsize,
}

impl VecInitialRowProvider {
    #[must_use]
    pub fn new(rows: Vec<InitialRowState>) -> Self {
        Self::with_max_rows_per_batch(rows, DEFAULT_INITIAL_ROW_BATCH_SIZE)
    }

    #[must_use]
    pub fn with_max_rows_per_batch(
        rows: Vec<InitialRowState>,
        max_rows_per_batch: NonZeroUsize,
    ) -> Self {
        Self {
            rows: rows.into(),
            max_rows_per_batch,
        }
    }
}

impl Default for VecInitialRowProvider {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

impl BatchProvider for VecInitialRowProvider {
    type Batch = InitialRowBatch;

    fn new_batch(&self) -> Self::Batch {
        Vec::with_capacity(self.max_rows_per_batch.get())
    }

    fn fill_batch(
        &mut self,
        mut reuse: Self::Batch,
    ) -> BoxFuture<'_, Result<Option<Self::Batch>, RowProviderError>> {
        async move {
            reuse.clear();
            while reuse.len() < self.max_rows_per_batch.get() {
                let Some(row) = self.rows.pop_front() else {
                    break;
                };
                reuse.push(row);
            }
            if reuse.is_empty() {
                return Ok(None);
            }
            Ok(Some(reuse))
        }
        .boxed()
    }
}
