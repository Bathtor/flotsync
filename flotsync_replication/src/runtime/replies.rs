//! Completes a promise reply and reports whether its receiver is still present.

use kompact::prelude::{Fulfillable as _, KPromise};

/// Attempt one reply; receiver disposal is reported without cancelling the caller's work.
pub fn complete_reply<T: Send + 'static>(promise: KPromise<T>, reply: T) -> ReplyOutcome {
    if promise.fulfil(reply).is_ok() {
        ReplyOutcome::Delivered
    } else {
        ReplyOutcome::ReceiverDropped
    }
}

/// Whether the promise's receiver accepted the reply.
#[derive(Debug, PartialEq, Eq)]
pub enum ReplyOutcome {
    /// The receiver accepted the reply.
    Delivered,
    /// The receiver was dropped before the reply was attempted.
    ReceiverDropped,
}
