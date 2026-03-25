use crate::api::{
    BoxFuture,
    InitialRowBatch,
    InitialRowProvider,
    InitialRowState,
    RowChange,
    RowChangeBatch,
    RowProvider,
    RowProviderError,
};
use std::{collections::VecDeque, num::NonZeroUsize};

/// Convenience `RowProvider` backed by an in-memory `Vec`.
#[derive(Default)]
pub struct VecRowProvider {
    rows: Vec<RowChange>,
}

impl VecRowProvider {
    pub fn new(rows: Vec<RowChange>) -> Self {
        Self { rows }
    }
}

impl RowProvider for VecRowProvider {
    fn next_batch<'a>(&'a mut self) -> BoxFuture<'a, Result<RowChangeBatch, RowProviderError>> {
        Box::pin(async move {
            let rows = std::mem::take(&mut self.rows);
            Ok(RowChangeBatch::from_vec(rows))
        })
    }
}

/// Convenience `InitialRowProvider` backed by an in-memory `VecDeque`.
#[derive(Debug, Default)]
pub struct VecInitialRowProvider {
    rows: VecDeque<InitialRowState>,
}

impl VecInitialRowProvider {
    pub fn new(rows: Vec<InitialRowState>) -> Self {
        Self { rows: rows.into() }
    }
}

impl InitialRowProvider for VecInitialRowProvider {
    fn next_batch<'a>(
        &'a mut self,
        mut reuse: InitialRowBatch,
        max_rows: NonZeroUsize,
    ) -> BoxFuture<'a, Result<InitialRowBatch, RowProviderError>> {
        Box::pin(async move {
            reuse.clear();
            while reuse.len() < max_rows.get() {
                let Some(row) = self.rows.pop_front() else {
                    break;
                };
                reuse.push(row);
            }
            Ok(reuse)
        })
    }
}
