//! SmallVec-backed reusable page batch.

use super::{OwnedPageBatchInput, PageBatch, PageBatchInput, PageError, PageLimit, accept_owned};
use flotsync_utils::{BoxError, NonOwningPhantomData};
use smallvec::{Array, SmallVec};
use std::{marker::PhantomData, num::NonZeroUsize};

/// SmallVec-backed page batch with a compile-time maximum size.
///
/// The default input family accepts owned `Value`s and retains each value
/// unchanged. Use [`Self::new_with`] when the batch accepts another input family
/// or transforms inputs before retaining them.
///
/// Constructing a batch with no capacity fails during compile-time evaluation:
///
/// ```compile_fail
/// use flotsync_replication::InlinePageBatch;
///
/// let _batch = InlinePageBatch::<u32, (), 0>::new();
/// ```
pub struct InlinePageBatch<
    Value,
    Metadata,
    const N: usize,
    Input = OwnedPageBatchInput<Value>,
    Accept = fn(Value) -> Result<Value, BoxError>,
> where
    Input: PageBatchInput,
    [Value; N]: Array<Item = Value>,
{
    /// Values retained by the latest successful fill.
    values: SmallVec<[Value; N]>,
    /// Metadata retained by the latest successful fill.
    metadata: Option<Metadata>,
    /// Fallible operation which accepts one input and produces one value.
    accept: Accept,
    /// Associated input family represented without owning an input value.
    input: NonOwningPhantomData<Input>,
}

impl<Value, Metadata, const N: usize> InlinePageBatch<Value, Metadata, N>
where
    [Value; N]: Array<Item = Value>,
{
    /// Create a batch which retains up to `N` owned values inline.
    #[must_use]
    pub fn new() -> Self {
        Self::new_with(accept_owned::<Value> as fn(Value) -> Result<Value, BoxError>)
    }
}

impl<Value, Metadata, const N: usize> Default for InlinePageBatch<Value, Metadata, N>
where
    [Value; N]: Array<Item = Value>,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<Value, Metadata, const N: usize, Input, Accept>
    InlinePageBatch<Value, Metadata, N, Input, Accept>
where
    Input: PageBatchInput,
    [Value; N]: Array<Item = Value>,
{
    /// Non-zero capacity whose construction is evaluated with each instantiation.
    const CAPACITY: NonZeroUsize =
        NonZeroUsize::new(N).expect("inline page capacity must be non-zero");

    /// Create a batch which accepts up to `N` inputs through `accept`.
    #[must_use]
    pub fn new_with(accept: Accept) -> Self {
        Self {
            values: SmallVec::with_capacity(Self::CAPACITY.get()),
            metadata: None,
            accept,
            input: PhantomData,
        }
    }

    /// Return values retained by the latest successful fill.
    #[must_use]
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// Return metadata from the latest successful fill.
    ///
    /// `Some` contains successful-fill metadata. `None` means this batch is new,
    /// was cleared, or its latest fill failed.
    #[must_use]
    pub fn metadata(&self) -> Option<&Metadata> {
        self.metadata.as_ref()
    }
}

impl<Value, Metadata, const N: usize, Input, Accept> PageBatch
    for InlinePageBatch<Value, Metadata, N, Input, Accept>
where
    Input: PageBatchInput,
    [Value; N]: Array<Item = Value>,
    Value: Send,
    Metadata: Send,
    Accept: for<'record> FnMut(Input::Value<'record>) -> Result<Value, BoxError> + Send,
{
    type Input = Input;
    type Metadata = Metadata;

    fn page_limit(&self) -> PageLimit {
        PageLimit::Max(Self::CAPACITY)
    }

    fn clear(&mut self) {
        self.values.clear();
        self.metadata = None;
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn metadata(&self) -> Option<&Self::Metadata> {
        self.metadata.as_ref()
    }

    fn push(&mut self, input: Input::Value<'_>) -> Result<(), PageError> {
        if self.values.len() == N {
            Err(PageError::PageLimitExceeded)
        } else {
            let value = (self.accept)(input).map_err(PageError::from_batch_error)?;
            self.values.push(value);
            Ok(())
        }
    }

    fn set_metadata(&mut self, metadata: Self::Metadata) {
        self.metadata = Some(metadata);
    }
}

/// Tests for inline capacity and storage behaviour.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_up_to_the_maximum_remain_inline() {
        let mut batch = InlinePageBatch::<u32, (), 2>::new();
        PageBatch::push(&mut batch, 1).unwrap();
        PageBatch::push(&mut batch, 2).unwrap();

        assert_eq!(batch.values(), &[1, 2]);
        assert!(!batch.values.spilled());
    }

    #[test]
    fn value_beyond_the_maximum_uses_the_page_limit_error() {
        let mut batch = InlinePageBatch::<u32, (), 1>::new();
        PageBatch::push(&mut batch, 1).unwrap();

        assert!(matches!(
            PageBatch::push(&mut batch, 2),
            Err(PageError::PageLimitExceeded)
        ));
        assert!(!batch.values.spilled());
    }
}
