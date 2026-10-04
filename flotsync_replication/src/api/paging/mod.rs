//! Reusable keyset pagination contracts for replication stores.
//!
//! A [`PageCursor`] owns immutable query parameters and an erased backend
//! continuation. It binds to a store transaction on first use. A [`PageBatch`]
//! accepts temporary records during one fill and retains caller output and query
//! metadata for inspection after the fill completes.
//!
//! Store implementations use the cursor to begin a [`PageAttempt`], select
//! records after its typed opaque continuation, offer them to the attempt, and
//! report whether another page may remain. The backend defines all ordering,
//! comparison, collation, and tie-breaker semantics. Finishing commits one
//! continuation for the whole page; dropping an unfinished attempt clears
//! partial output and fails the cursor.
//!
//! ```
//! use flotsync_replication::{
//!     PageCursor, PageEnd, PageError, PageLimit, StoreTransactionId, VecPageBatch,
//! };
//! use std::num::NonZeroUsize;
//!
//! struct SliceContinuation(usize);
//!
//! fn fill_page(
//!     cursor: &mut PageCursor<&str>,
//!     transaction_id: StoreTransactionId,
//!     batch: &mut VecPageBatch<u32, usize>,
//!     stored: &[u32],
//! ) -> Result<(), PageError> {
//!     let mut page = cursor.begin_page::<SliceContinuation, _>(transaction_id, batch)?;
//!     let after = page.after().map_or(0, |continuation| continuation.0);
//!     let maximum = match page.limit() {
//!         PageLimit::Max(limit) => limit.get(),
//!         PageLimit::Unlimited => usize::MAX,
//!     };
//!     let records = stored
//!         .iter()
//!         .copied()
//!         .skip(after)
//!         .take(maximum);
//!     let mut next = None;
//!     for record in records {
//!         page.push(record)?;
//!         next = Some(SliceContinuation(after + page.len()));
//!     }
//!     let end = if page.has_filled_limit() {
//!         PageEnd::MayHaveMore(next.expect("a non-zero limit was filled"))
//!     } else {
//!         PageEnd::Exhausted
//!     };
//!     page.finish(stored.len(), end)
//! }
//!
//! let transaction_id = StoreTransactionId::new_random();
//! let mut cursor = PageCursor::new("all records");
//! let limit = NonZeroUsize::new(2).unwrap();
//! let mut batch = VecPageBatch::<u32, usize>::bounded(limit);
//!
//! fill_page(&mut cursor, transaction_id, &mut batch, &[1, 2, 3])?;
//! assert_eq!(batch.values(), &[1, 2]);
//! assert_eq!(batch.metadata(), Some(&3));
//! assert!(cursor.has_more());
//! # Ok::<(), PageError>(())
//! ```

use flotsync_utils::{BoxError, NonOwningPhantomData};
use std::{any::Any, fmt, num::NonZeroUsize};
use uuid::Uuid;

mod error;
mod inline_batch;
mod vec_batch;

pub use error::PageError;
pub use inline_batch::InlinePageBatch;
pub use vec_batch::VecPageBatch;

/// Type-level family of records accepted by a [`PageBatch`].
///
/// A family lets a batch accept values which borrow backend-owned storage for
/// the duration of one call while keeping [`PageBatch`] usable as a trait
/// object.
pub trait PageBatchInput {
    /// Record type accepted for one particular borrow lifetime.
    type Value<'record>;
}

/// Reusable output storage and sizing policy for one pageable query.
///
/// Store methods offer records through [`Self::push`]. A page fill clears prior
/// values and metadata before it starts, then installs metadata only when it
/// completes successfully. Implementations decide how accepted inputs map to
/// retained output values.
pub trait PageBatch: Send {
    /// Family of temporary records accepted by this batch.
    type Input: PageBatchInput;
    /// Query-specific metadata retained for a successful fill.
    type Metadata;

    /// Return the record limit to sample at the start of the next fill.
    fn page_limit(&self) -> PageLimit;

    /// Remove retained values and metadata while preserving reusable storage
    /// and configuration.
    fn clear(&mut self);

    /// Reserve capacity for at least `additional` more retained results.
    ///
    /// Implementations without reservable storage may leave this as a no-op.
    fn reserve(&mut self, _additional: usize) {}

    /// Return the number of values currently retained.
    fn len(&self) -> usize;

    /// Return metadata from the latest successful fill.
    ///
    /// `Some` contains metadata installed when that fill completed. `None`
    /// means this batch is new, was cleared, or its latest fill failed.
    fn metadata(&self) -> Option<&Self::Metadata>;

    /// Accept one input from the current page fill.
    ///
    /// The input is valid only for this call. Implementations may retain any
    /// output derived from it.
    ///
    /// # Errors
    ///
    /// Returns the batch-specific failure translated to [`PageError`].
    fn push(&mut self, input: <Self::Input as PageBatchInput>::Value<'_>) -> Result<(), PageError>;

    /// Store metadata for the completed fill.
    fn set_metadata(&mut self, metadata: Self::Metadata);

    /// Return `true` iff this batch currently retains no values.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Input family used when a page batch accepts owned values of type `Value`.
///
/// The supplied Vec and inline batches use this family by default, so callers
/// only need a custom [`PageBatchInput`] implementation for temporary borrowed
/// inputs.
pub struct OwnedPageBatchInput<Value> {
    /// Owned value type represented without storing a value.
    value: NonOwningPhantomData<Value>,
}

impl<Value> PageBatchInput for OwnedPageBatchInput<Value> {
    type Value<'record> = Value;
}

/// Opaque identity of one concrete store transaction instance.
///
/// Backends control how the UUID is generated. They must not reuse an identity
/// while a cursor created for the earlier transaction could still be supplied
/// to another transaction, including across pools or store instances.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct StoreTransactionId(Uuid);

impl StoreTransactionId {
    /// Generate a random identity for one transaction instance.
    #[must_use]
    pub fn new_random() -> Self {
        Self(Uuid::new_v4())
    }

    /// Wrap a backend-generated UUID identifying one transaction instance.
    #[must_use]
    pub const fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }
}

impl fmt::Display for StoreTransactionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Maximum number of records accepted during one page fill.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PageLimit {
    /// Accept no more than this many records.
    Max(NonZeroUsize),
    /// Accept every remaining record and exhaust the cursor.
    Unlimited,
}

impl PageLimit {
    /// Convert this limit into its optional finite maximum.
    ///
    /// `Some` contains the maximum number of records. `None` means the fill is
    /// unlimited.
    #[must_use]
    pub const fn into_option(self) -> Option<NonZeroUsize> {
        match self {
            Self::Max(limit) => Some(limit),
            Self::Unlimited => None,
        }
    }
}

/// Backend-reported completion state for one successful page fill.
pub enum PageEnd<Continuation> {
    /// Another call may be required, using this backend-owned continuation.
    MayHaveMore(Continuation),
    /// The backend established that no selected records remain.
    Exhausted,
}

/// Query parameters and private continuation for one pageable store operation.
pub struct PageCursor<Params> {
    /// Immutable arguments which define the query across every page call.
    params: Params,
    /// Transaction association established by the first page call.
    transaction_id: Option<StoreTransactionId>,
    /// Exclusive continuation and terminal lifecycle state.
    position: PagePosition,
}

impl<Params> PageCursor<Params> {
    /// Create a cursor positioned before the first result for `params`.
    #[must_use]
    pub const fn new(params: Params) -> Self {
        Self {
            params,
            transaction_id: None,
            position: PagePosition::Ready { continuation: None },
        }
    }

    /// Return the immutable query parameters carried by this cursor.
    #[must_use]
    pub const fn params(&self) -> &Params {
        &self.params
    }

    /// Return `true` while another page call may produce results.
    #[must_use]
    pub const fn has_more(&self) -> bool {
        matches!(self.position, PagePosition::Ready { .. })
    }

    /// Return `true` when successful paging established end-of-query.
    #[must_use]
    pub const fn is_exhausted(&self) -> bool {
        matches!(self.position, PagePosition::Exhausted)
    }

    /// Return `true` when an unfinished or failed page invalidated the cursor.
    #[must_use]
    pub const fn is_failed(&self) -> bool {
        matches!(self.position, PagePosition::Failed)
    }

    /// Begin one page fill for `transaction_id` and sample the batch policy.
    ///
    /// The batch is cleared before the transaction association is checked. A
    /// transaction mismatch leaves the cursor usable with its original
    /// transaction association. An exhausted cursor remains exhausted when the
    /// rejected attempt returns an error.
    ///
    /// # Errors
    ///
    /// Returns a contract error when the cursor is exhausted, failed, belongs to
    /// another transaction, or already carries a continuation of another
    /// backend format. A format mismatch leaves the cursor usable with its
    /// original transaction and continuation.
    pub fn begin_page<'a, Continuation, Batch>(
        &'a mut self,
        transaction_id: StoreTransactionId,
        batch: &'a mut Batch,
    ) -> Result<PageAttempt<'a, Params, Continuation, Batch>, PageError>
    where
        Continuation: Send + 'static,
        Batch: PageBatch + ?Sized,
    {
        batch.clear();
        self.bind(transaction_id)?;
        let continuation_validation = match &self.position {
            PagePosition::Ready {
                continuation: Some(continuation),
            } if !continuation.is::<Continuation>() => Err(PageError::ContinuationTypeMismatch),
            PagePosition::Ready { .. } => Ok(()),
            PagePosition::Exhausted => Err(PageError::CursorExhausted),
            PagePosition::Failed => Err(PageError::CursorFailed),
        };
        continuation_validation?;
        let limit = batch.page_limit();
        Ok(PageAttempt {
            cursor: self,
            batch,
            limit,
            continuation: NonOwningPhantomData::default(),
            state: PageAttemptState::Filling { records: 0 },
        })
    }

    /// Bind an unused cursor or validate its existing transaction association.
    fn bind(&mut self, requested: StoreTransactionId) -> Result<(), PageError> {
        match self.transaction_id {
            Some(expected) if expected != requested => Err(PageError::TransactionMismatch {
                expected,
                actual: requested,
            }),
            Some(_) => Ok(()),
            None => {
                self.transaction_id = Some(requested);
                Ok(())
            }
        }
    }
}

/// Shared state machine used by backend page implementations.
///
/// Begin an attempt through [`PageCursor::begin_page`] immediately before
/// reading backend records, offer selected records through [`Self::push`], and
/// consume the attempt with [`Self::finish`]. The backend interprets the typed
/// continuation and supplies the next one after filling the page. Dropping an
/// unfinished attempt clears its batch and invalidates its cursor, including
/// when asynchronous work is cancelled.
pub struct PageAttempt<'a, Params, Continuation, Batch>
where
    Batch: PageBatch + ?Sized,
{
    /// Cursor whose continuation is committed by successful completion.
    cursor: &'a mut PageCursor<Params>,
    /// Reusable output cleared on entry and after every unsuccessful fill.
    batch: &'a mut Batch,
    /// Batch policy sampled once for this attempt.
    limit: PageLimit,
    /// Backend continuation type represented without owning a value.
    continuation: NonOwningPhantomData<Continuation>,
    /// Current lifecycle and the data valid in that lifecycle.
    state: PageAttemptState,
}

impl<Params, Continuation, Batch> PageAttempt<'_, Params, Continuation, Batch>
where
    Continuation: Send + 'static,
    Batch: PageBatch + ?Sized,
{
    /// Return the immutable query parameters for the backend selection.
    #[must_use]
    pub fn params(&self) -> &Params {
        &self.cursor.params
    }

    /// Return the backend continuation for this page, if paging has begun.
    ///
    /// `Some` is the opaque value committed by the previous successful fill.
    /// `None` means this is the first fill.
    ///
    /// # Panics
    ///
    /// Panics only if the cursor's erased continuation changes type after
    /// [`PageCursor::begin_page`] validated it, which safe code cannot do.
    #[must_use]
    pub fn after(&self) -> Option<&Continuation> {
        match &self.cursor.position {
            PagePosition::Ready { continuation } => continuation.as_ref().map(|continuation| {
                continuation
                    .downcast_ref()
                    .expect("page continuation type was validated when the attempt began")
            }),
            PagePosition::Exhausted | PagePosition::Failed => None,
        }
    }

    /// Return the record limit sampled for this fill.
    #[must_use]
    pub const fn limit(&self) -> PageLimit {
        self.limit
    }

    /// Reserve batch storage for at least `additional` more source results.
    ///
    /// Backends should call this when a query result exposes its size before
    /// individual records are decoded and pushed.
    pub fn reserve(&mut self, additional: usize) {
        self.batch.reserve(additional);
    }

    /// Return `true` when this fill accepted its sampled finite maximum.
    ///
    /// `false` means the fill is unlimited, has accepted fewer source records
    /// than its finite maximum, or has failed.
    #[must_use]
    pub fn has_filled_limit(&self) -> bool {
        match (&self.state, self.limit) {
            (PageAttemptState::Filling { records }, PageLimit::Max(limit)) => {
                *records == limit.get()
            }
            (PageAttemptState::Filling { .. }, PageLimit::Unlimited)
            | (PageAttemptState::Failed, _) => false,
            (PageAttemptState::Completed, _) => {
                unreachable!("a completed page attempt cannot be queried")
            }
        }
    }

    /// Return the number of source records accepted during this active fill.
    ///
    /// A failed attempt reports zero. Successful completion consumes the
    /// attempt, so completed attempts cannot be queried.
    #[must_use]
    pub fn len(&self) -> usize {
        match &self.state {
            PageAttemptState::Filling { records } => *records,
            PageAttemptState::Failed => 0,
            PageAttemptState::Completed => {
                unreachable!("a completed page attempt cannot be queried")
            }
        }
    }

    /// Return `true` when this fill has accepted no source records.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Offer one source record selected for this fill.
    ///
    /// The cursor continuation remains unchanged until [`Self::finish`] commits
    /// the backend's completion state. Any failure clears values and metadata
    /// and invalidates the cursor.
    ///
    /// # Errors
    ///
    /// Returns a batch or contract error when the batch cannot accept the input
    /// or the source-record limit is exceeded.
    pub fn push(
        &mut self,
        input: <Batch::Input as PageBatchInput>::Value<'_>,
    ) -> Result<(), PageError> {
        let validation = match &self.state {
            PageAttemptState::Filling { records } => {
                let limit_exceeded = self
                    .limit
                    .into_option()
                    .is_some_and(|limit| *records == limit.get());
                if limit_exceeded {
                    Err(PageError::PageLimitExceeded)
                } else {
                    Ok(*records + 1)
                }
            }
            PageAttemptState::Failed => Err(PageError::CursorFailed),
            PageAttemptState::Completed => {
                unreachable!("a completed page attempt cannot accept records")
            }
        };

        let result = match validation {
            Ok(next_records) => match self.batch.push(input) {
                Ok(()) => {
                    match &mut self.state {
                        PageAttemptState::Filling { records } => *records = next_records,
                        PageAttemptState::Failed | PageAttemptState::Completed => {
                            unreachable!("validated page attempt must still be filling")
                        }
                    }
                    Ok(())
                }
                Err(error) => Err(error),
            },
            Err(error) => Err(error),
        };

        if result.is_err() {
            self.invalidate();
        }
        result
    }

    /// Complete the page and retain its query-specific metadata in the batch.
    ///
    /// `end` is supplied by the backend after it has accepted all source
    /// records. [`PageEnd::MayHaveMore`] commits an opaque continuation for a
    /// bounded fill; [`PageEnd::Exhausted`] makes the cursor terminal.
    ///
    /// # Errors
    ///
    /// Returns [`PageError::CursorFailed`] if an earlier batch or contract error
    /// invalidated this attempt. Returns
    /// [`PageError::UnlimitedPageContinuation`] if an unlimited fill reports
    /// that another page may remain, or [`PageError::EmptyPageContinuation`] if
    /// a fill without accepted source records reports a continuation.
    pub fn finish(
        mut self,
        metadata: Batch::Metadata,
        end: PageEnd<Continuation>,
    ) -> Result<(), PageError> {
        let completion_error = match &self.state {
            PageAttemptState::Failed => Some(PageError::CursorFailed),
            PageAttemptState::Completed => {
                unreachable!("a completed page attempt cannot be finished again")
            }
            PageAttemptState::Filling { .. }
                if matches!(self.limit, PageLimit::Unlimited)
                    && matches!(&end, PageEnd::MayHaveMore(_)) =>
            {
                Some(PageError::UnlimitedPageContinuation)
            }
            PageAttemptState::Filling { records: 0 } if matches!(&end, PageEnd::MayHaveMore(_)) => {
                Some(PageError::EmptyPageContinuation)
            }
            PageAttemptState::Filling { .. } => None,
        };
        match completion_error {
            Some(PageError::CursorFailed) => Err(PageError::CursorFailed),
            Some(error) => {
                self.invalidate();
                Err(error)
            }
            None => {
                let position = match end {
                    PageEnd::MayHaveMore(continuation) => PagePosition::Ready {
                        continuation: Some(Box::new(continuation)),
                    },
                    PageEnd::Exhausted => PagePosition::Exhausted,
                };
                self.batch.set_metadata(metadata);
                self.cursor.position = position;
                self.state = PageAttemptState::Completed;
                Ok(())
            }
        }
    }

    /// Clear partial output and mark this attempt and cursor as failed.
    fn invalidate(&mut self) {
        self.batch.clear();
        self.cursor.position = PagePosition::Failed;
        self.state = PageAttemptState::Failed;
    }
}

impl<Params, Continuation, Batch> Drop for PageAttempt<'_, Params, Continuation, Batch>
where
    Batch: PageBatch + ?Sized,
{
    fn drop(&mut self) {
        if matches!(self.state, PageAttemptState::Filling { .. }) {
            self.batch.clear();
            self.cursor.position = PagePosition::Failed;
            self.state = PageAttemptState::Failed;
        }
    }
}

/// Accept one owned value unchanged.
#[allow(
    clippy::unnecessary_wraps,
    reason = "identity acceptance shares the fallible callback signature"
)]
pub(super) fn accept_owned<Value>(value: Value) -> Result<Value, BoxError> {
    Ok(value)
}

/// Lifecycle of one page attempt and data valid in each state.
enum PageAttemptState {
    /// Records may be accepted; the count describes this fill only.
    Filling {
        /// Number of source records accepted during this fill.
        records: usize,
    },
    /// A batch or paging contract failure invalidated the cursor.
    Failed,
    /// `finish` committed the attempt and suppresses drop cleanup.
    Completed,
}

/// Lifecycle and exclusive lower bound for one cursor.
enum PagePosition {
    /// The optional erased value is the backend continuation for the next page.
    Ready {
        /// Backend continuation, or `None` before the first completed page.
        continuation: Option<Box<dyn Any + Send>>,
    },
    /// The query is known to contain no remaining records.
    Exhausted,
    /// A prior fill did not complete successfully.
    Failed,
}

/// Shared paging contract tests.
#[cfg(test)]
mod tests;
