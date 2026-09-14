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
    /// An earlier unfinished or failed fill invalidated this cursor.
    #[snafu(display("Page cursor is invalid after an earlier unsuccessful fill."))]
    CursorFailed,
    /// The backend supplied a key at or before the exclusive continuation.
    #[snafu(display(
        "Page backend returned a non-increasing continuation key in transaction {transaction_id}."
    ))]
    NonIncreasingKey {
        /// Transaction whose result violated the ordering contract.
        transaction_id: StoreTransactionId,
    },
    /// A page attempt or fixed-capacity batch received an input beyond its limit.
    #[snafu(display("Page record limit exceeded."))]
    PageLimitExceeded,
    /// The backend attempted to append to an exhausted cursor.
    #[snafu(display(
        "Page backend returned a record after the cursor was exhausted in transaction {transaction_id}."
    ))]
    RecordAfterExhaustion {
        /// Transaction in which the cursor was already exhausted.
        transaction_id: StoreTransactionId,
    },
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
            | Self::CursorFailed
            | Self::NonIncreasingKey { .. }
            | Self::PageLimitExceeded
            | Self::RecordAfterExhaustion { .. } => Some(
                StoreErrorClassification::UNKNOWN
                    .with_scope(StoreErrorScope::Operation)
                    .with_class(StoreErrorClass::Contract)
                    .with_resolution(StoreErrorResolution::FixBug),
            ),
        }
    }
}
