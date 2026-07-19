use crate::api::{BatchProvider, BoxFuture, RowChange, RowChangeBatch, RowProviderError};
use futures_util::FutureExt;

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
