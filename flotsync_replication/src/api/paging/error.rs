//! Errors produced by paging contracts and reusable batch implementations.

use super::StoreTransactionId;
use crate::api::{
    StoreError,
    StoreErrorClass,
    StoreErrorClassification,
    StoreErrorClassificationSource,
    StoreErrorResolution,
    StoreErrorScope,
};
use flotsync_utils::BoxError;
use snafu::Snafu;

/// Failure while validating, reading, or filling one store page.
#[derive(Debug, Snafu)]
#[non_exhaustive]
pub enum PageError {
    /// The backend failed while reading the page.
    #[snafu(display("Store page access failed: {source}"))]
    Store { source: StoreError },
    /// The supplied batch failed while accepting one record.
    #[snafu(display("Page batch failed while accepting a record: {source}"))]
    BatchPush { source: BoxError },
    /// This cursor belongs to a different transaction instance.
    #[snafu(display("Page cursor belongs to transaction {expected}, not transaction {actual}."))]
    TransactionMismatch {
        expected: StoreTransactionId,
        actual: StoreTransactionId,
    },
    /// This cursor has already reached the end of its query.
    #[snafu(display("Page cursor is already exhausted."))]
    CursorExhausted,
    /// An earlier unfinished or failed fill invalidated this cursor.
    #[snafu(display("Page cursor is invalid after an earlier unsuccessful fill."))]
    CursorFailed,
    /// The backend attempted to resume a cursor with another continuation format.
    #[snafu(display("Page cursor continuation has an incompatible backend format."))]
    ContinuationTypeMismatch,
    /// An unlimited fill attempted to retain a continuation for another call.
    #[snafu(display("An unlimited page must exhaust its cursor."))]
    UnlimitedPageContinuation,
    /// A fill without accepted source records attempted to retain a continuation.
    #[snafu(display("An empty page cannot retain a continuation."))]
    EmptyPageContinuation,
    /// A page attempt or fixed-capacity batch received an input beyond its limit.
    #[snafu(display("Page record limit exceeded."))]
    PageLimitExceeded,
    /// A full bounded page did not supply the continuation required to resume it.
    #[snafu(display("A full bounded page must provide a continuation."))]
    MissingContinuation,
}

impl PageError {
    /// Wrap a classified backend failure for return from a page method.
    #[must_use]
    pub fn from_store_error(source: StoreError) -> Self {
        Self::Store { source }
    }

    /// Wrap a batch callback failure for return from a page method.
    #[must_use]
    pub fn from_batch_error(source: BoxError) -> Self {
        Self::BatchPush { source }
    }
}

impl StoreErrorClassificationSource for PageError {
    fn store_error_classification(&self) -> Option<StoreErrorClassification> {
        match self {
            Self::Store { source } => Some(source.classification()),
            Self::BatchPush { .. } => None,
            Self::TransactionMismatch { .. }
            | Self::CursorExhausted
            | Self::CursorFailed
            | Self::ContinuationTypeMismatch
            | Self::UnlimitedPageContinuation
            | Self::EmptyPageContinuation
            | Self::PageLimitExceeded
            | Self::MissingContinuation => Some(
                StoreErrorClassification::UNKNOWN
                    .with_scope(StoreErrorScope::Operation)
                    .with_class(StoreErrorClass::Contract)
                    .with_resolution(StoreErrorResolution::FixBug),
            ),
        }
    }
}

impl From<PageError> for StoreError {
    fn from(error: PageError) -> Self {
        match error {
            PageError::Store { source } => source,
            error => Self::from_classification_source(error),
        }
    }
}

impl From<StoreError> for PageError {
    fn from(source: StoreError) -> Self {
        Self::from_store_error(source)
    }
}
