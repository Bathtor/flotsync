//! Vec-backed reusable page batch.

use super::{OwnedPageBatchInput, PageBatch, PageBatchInput, PageError, PageLimit, accept_owned};
use flotsync_utils::{BoxError, NonOwningPhantomData};
use std::{marker::PhantomData, num::NonZeroUsize};

/// Vec-backed page batch with a runtime sizing policy.
///
/// The default input family accepts owned `Value`s and retains each value
/// unchanged. Use [`Self::bounded_with`] or [`Self::unlimited_with`] when the
/// batch accepts another input family or transforms inputs before retaining
/// them.
pub struct VecPageBatch<
    Value,
    Metadata,
    Input = OwnedPageBatchInput<Value>,
    Accept = fn(Value) -> Result<Value, BoxError>,
> where
    Input: PageBatchInput,
{
    /// Values retained by the latest successful fill.
    values: Vec<Value>,
    /// Metadata retained by the latest successful fill.
    metadata: Option<Metadata>,
    /// Record limit supplied to each new page attempt.
    limit: PageLimit,
    /// Fallible operation which accepts one input and produces one value.
    accept: Accept,
    /// Associated input family represented without owning an input value.
    input: NonOwningPhantomData<Input>,
}

impl<Value, Metadata> VecPageBatch<Value, Metadata> {
    /// Create a batch which retains at most `limit` owned values per fill.
    #[must_use]
    pub fn bounded(limit: NonZeroUsize) -> Self {
        Self::bounded_with(
            limit,
            accept_owned::<Value> as fn(Value) -> Result<Value, BoxError>,
        )
    }

    /// Create a growing batch which retains all remaining owned values.
    #[must_use]
    pub fn unlimited() -> Self {
        Self::unlimited_with(accept_owned::<Value> as fn(Value) -> Result<Value, BoxError>)
    }
}

impl<Value, Metadata, Input, Accept> VecPageBatch<Value, Metadata, Input, Accept>
where
    Input: PageBatchInput,
{
    /// Create a batch accepting at most `limit` inputs per fill through `accept`.
    #[must_use]
    pub fn bounded_with(limit: NonZeroUsize, accept: Accept) -> Self {
        Self {
            values: Vec::with_capacity(limit.get()),
            metadata: None,
            limit: PageLimit::Max(limit),
            accept,
            input: PhantomData,
        }
    }

    /// Create a growing batch which accepts all remaining inputs through `accept`.
    #[must_use]
    pub fn unlimited_with(accept: Accept) -> Self {
        Self {
            values: Vec::new(),
            metadata: None,
            limit: PageLimit::Unlimited,
            accept,
            input: PhantomData,
        }
    }

    /// Return values retained by the latest successful fill.
    #[must_use]
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// Mutably borrow values retained by the latest successful fill.
    pub fn values_mut(&mut self) -> &mut Vec<Value> {
        &mut self.values
    }

    /// Consume the batch and return its retained values.
    #[must_use]
    pub fn into_values(self) -> Vec<Value> {
        self.values
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

impl<Value, Metadata, Input, Accept> PageBatch for VecPageBatch<Value, Metadata, Input, Accept>
where
    Input: PageBatchInput,
    Value: Send,
    Metadata: Send,
    Accept: for<'record> FnMut(Input::Value<'record>) -> Result<Value, BoxError> + Send,
{
    type Input = Input;
    type Metadata = Metadata;

    fn page_limit(&self) -> PageLimit {
        self.limit
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
        let value = (self.accept)(input).map_err(PageError::from_batch_error)?;
        self.values.push(value);
        Ok(())
    }

    fn set_metadata(&mut self, metadata: Self::Metadata) {
        self.metadata = Some(metadata);
    }
}
