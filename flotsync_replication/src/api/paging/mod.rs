//! Reusable keyset pagination contracts for replication stores.
//!
//! A [`PageCursor`] owns the immutable parameters and exclusive continuation
//! for one query. It binds to a store transaction on first use. A [`PageBatch`]
//! accepts temporary records during one fill and retains the caller's output and
//! query metadata for inspection after the fill completes.
//!
//! Store implementations use the cursor to begin a [`PageAttempt`], select
//! records after its exclusive lower bound, offer them in increasing key order,
//! and finish the attempt with query metadata. Finishing commits the next
//! continuation; dropping an unfinished attempt clears partial output and fails
//! the cursor.
//!
//! ```
//! use flotsync_replication::{
//!     PageCursor, PageError, PageLimit, StoreTransactionId, VecPageBatch,
//! };
//! use std::num::NonZeroUsize;
//!
//! fn fill_page(
//!     cursor: &mut PageCursor<&str, u32>,
//!     transaction_id: StoreTransactionId,
//!     batch: &mut VecPageBatch<u32, usize>,
//!     stored: &[u32],
//! ) -> Result<(), PageError> {
//!     let mut page = cursor.begin_page(transaction_id, batch)?;
//!     if !page.is_exhausted() {
//!         let after = page.after().copied();
//!         let maximum = match page.limit() {
//!             PageLimit::Max(limit) => limit.get(),
//!             PageLimit::Unlimited => usize::MAX,
//!         };
//!         let records = stored
//!             .iter()
//!             .copied()
//!             .filter(|key| after.is_none_or(|after| *key > after))
//!             .take(maximum);
//!         for record in records {
//!             page.push(record, record)?;
//!         }
//!     }
//!     page.finish(stored.len())
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
use std::{fmt, mem, num::NonZeroUsize};
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

/// Query parameters and private continuation for one pageable store operation.
pub struct PageCursor<Params, Key> {
    /// Immutable arguments which define the query across every page call.
    params: Params,
    /// Transaction association established by the first page call.
    transaction_id: Option<StoreTransactionId>,
    /// Exclusive continuation and terminal lifecycle state.
    position: PagePosition<Key>,
}

impl<Params, Key> PageCursor<Params, Key> {
    /// Create a cursor positioned before the first result for `params`.
    #[must_use]
    pub const fn new(params: Params) -> Self {
        Self {
            params,
            transaction_id: None,
            position: PagePosition::Ready(None),
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
        matches!(self.position, PagePosition::Ready(_))
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
    /// transaction association.
    ///
    /// # Errors
    ///
    /// Returns a contract error when the cursor is failed or belongs to another
    /// transaction.
    pub fn begin_page<'a, Batch>(
        &'a mut self,
        transaction_id: StoreTransactionId,
        batch: &'a mut Batch,
    ) -> Result<PageAttempt<'a, Params, Key, Batch>, PageError>
    where
        Key: Clone + Ord,
        Batch: PageBatch + ?Sized,
    {
        PageAttempt::begin(self, transaction_id, batch)
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
/// reading backend records. Return early when [`Self::is_exhausted`] is true,
/// offer successive records whose keys form a strictly increasing sequence
/// through [`Self::push`], and consume the attempt with [`Self::finish`].
/// Dropping an unfinished filling attempt clears its batch and invalidates its
/// cursor, including when asynchronous work is cancelled.
pub struct PageAttempt<'a, Params, Key, Batch>
where
    Batch: PageBatch + ?Sized,
{
    /// Cursor whose continuation is committed by successful completion.
    cursor: &'a mut PageCursor<Params, Key>,
    /// Reusable output cleared on entry and after every unsuccessful fill.
    batch: &'a mut Batch,
    /// Transaction identity validated when the attempt began.
    transaction_id: StoreTransactionId,
    /// Batch policy sampled once for this attempt.
    limit: PageLimit,
    /// Current lifecycle and the data valid in that lifecycle.
    state: PageAttemptState<Key>,
}

impl<'a, Params, Key, Batch> PageAttempt<'a, Params, Key, Batch>
where
    Key: Clone + Ord,
    Batch: PageBatch + ?Sized,
{
    /// Start one page fill on behalf of [`PageCursor::begin_page`].
    ///
    /// A transaction mismatch leaves the cursor usable with its original
    /// transaction association.
    ///
    /// # Errors
    ///
    /// Returns a contract error when the cursor is failed or belongs to another
    /// transaction.
    fn begin(
        cursor: &'a mut PageCursor<Params, Key>,
        transaction_id: StoreTransactionId,
        batch: &'a mut Batch,
    ) -> Result<Self, PageError> {
        batch.clear();
        cursor.bind(transaction_id)?;
        if matches!(cursor.position, PagePosition::Failed) {
            Err(PageError::CursorFailed)
        } else {
            let state = if matches!(cursor.position, PagePosition::Exhausted) {
                PageAttemptState::Exhausted
            } else {
                PageAttemptState::Filling {
                    last_key: None,
                    records: 0,
                }
            };
            let limit = batch.page_limit();
            Ok(Self {
                cursor,
                batch,
                transaction_id,
                limit,
                state,
            })
        }
    }

    /// Return the immutable query parameters for the backend selection.
    #[must_use]
    pub fn params(&self) -> &Params {
        &self.cursor.params
    }

    /// Return the exclusive lower-bound key for this page, if paging has begun.
    #[must_use]
    pub fn after(&self) -> Option<&Key> {
        match &self.cursor.position {
            PagePosition::Ready(after) => after.as_ref(),
            PagePosition::Exhausted | PagePosition::Failed => None,
        }
    }

    /// Return the record limit sampled for this fill.
    #[must_use]
    pub const fn limit(&self) -> PageLimit {
        self.limit
    }

    /// Return `true` when the cursor was already exhausted before this fill.
    #[must_use]
    pub const fn is_exhausted(&self) -> bool {
        matches!(self.state, PageAttemptState::Exhausted)
    }

    /// Offer the next source record in a strictly increasing key sequence.
    ///
    /// This method maintains the page contract: keys advance strictly from the
    /// cursor's exclusive continuation and from one successful push to the next;
    /// the number of accepted source records does not exceed the sampled
    /// maximum; and the cursor continuation remains unchanged until
    /// [`Self::finish`] commits the complete fill. Any failure clears values and
    /// metadata and invalidates the cursor.
    ///
    /// # Errors
    ///
    /// Returns a batch or contract error when the batch cannot accept the input,
    /// keys do not advance, or the limit is exceeded.
    pub fn push(
        &mut self,
        key: Key,
        input: <Batch::Input as PageBatchInput>::Value<'_>,
    ) -> Result<(), PageError> {
        let validation = match &self.state {
            PageAttemptState::Filling { last_key, records } => {
                let limit_exceeded = self
                    .limit
                    .into_option()
                    .is_some_and(|limit| *records == limit.get());
                let previous_key = last_key.as_ref().or_else(|| self.after());
                let key_did_not_advance = previous_key.is_some_and(|previous| key <= *previous);
                if limit_exceeded {
                    Err(PageError::PageLimitExceeded)
                } else if key_did_not_advance {
                    Err(PageError::NonIncreasingKey {
                        transaction_id: self.transaction_id,
                    })
                } else {
                    Ok(*records + 1)
                }
            }
            PageAttemptState::Exhausted => Err(PageError::RecordAfterExhaustion {
                transaction_id: self.transaction_id,
            }),
            PageAttemptState::Failed | PageAttemptState::Completed => Err(PageError::CursorFailed),
        };

        let result = match validation {
            Ok(next_records) => match self.batch.push(input) {
                Ok(()) => {
                    match &mut self.state {
                        PageAttemptState::Filling { last_key, records } => {
                            *last_key = Some(key);
                            *records = next_records;
                        }
                        PageAttemptState::Exhausted
                        | PageAttemptState::Failed
                        | PageAttemptState::Completed => {
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
            self.batch.clear();
            self.cursor.position = PagePosition::Failed;
            self.state = PageAttemptState::Failed;
        }
        result
    }

    /// Complete the page and retain its query-specific metadata in the batch.
    ///
    /// A bounded page filled exactly to its maximum leaves the cursor ready for
    /// another call. The following call may be empty before exhaustion becomes
    /// known.
    ///
    /// # Errors
    ///
    /// Returns [`PageError::CursorFailed`] if an earlier batch or contract error
    /// invalidated this attempt.
    pub fn finish(mut self, metadata: Batch::Metadata) -> Result<(), PageError> {
        let state = mem::replace(&mut self.state, PageAttemptState::Completed);
        match state {
            PageAttemptState::Filling { last_key, records } => {
                let filled_bounded_page = self
                    .limit
                    .into_option()
                    .is_some_and(|limit| records == limit.get());
                self.cursor.position = if filled_bounded_page {
                    PagePosition::Ready(last_key)
                } else {
                    PagePosition::Exhausted
                };
                self.batch.set_metadata(metadata);
                Ok(())
            }
            PageAttemptState::Exhausted => {
                self.batch.set_metadata(metadata);
                Ok(())
            }
            PageAttemptState::Failed | PageAttemptState::Completed => Err(PageError::CursorFailed),
        }
    }
}

impl<Params, Key, Batch> Drop for PageAttempt<'_, Params, Key, Batch>
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
enum PageAttemptState<Key> {
    /// Records may be accepted; the key and count describe this fill only.
    Filling {
        /// Last key accepted during this fill.
        last_key: Option<Key>,
        /// Number of source records accepted during this fill.
        records: usize,
    },
    /// The cursor was already exhausted when the attempt began.
    Exhausted,
    /// A batch or paging contract failure invalidated the cursor.
    Failed,
    /// `finish` committed the attempt and suppresses drop cleanup.
    Completed,
}

/// Lifecycle and exclusive lower bound for one cursor.
enum PagePosition<Key> {
    /// The optional key is the exclusive lower bound for the next page.
    Ready(Option<Key>),
    /// The query is known to contain no remaining records.
    Exhausted,
    /// A prior fill did not complete successfully.
    Failed,
}

/// Shared paging contract tests.
#[cfg(test)]
mod tests;
