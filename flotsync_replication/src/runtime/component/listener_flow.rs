//! Owns ordered data-change delivery and the caller's continuation.
//!
//! `ListenerFlow` is ready to deliver an event. Starting it moves the event into
//! an effect and records a `DeliveringListener` containing the remaining events
//! and continuation. Only that waiting value can accept a callback result. A
//! successful callback starts the next event; rejection abandons the remainder.
//! The child acquires no separate lane: its caller retains the ordering claim.
//!
//! A provider belongs to the ready event until delivery transfers it to the
//! callback. Exhaustion releases any retained read transaction through the
//! provider's existing implementation; dropping it abandons unread rows. The
//! callback may retain a provider, so acceptance is not an exhaustion assertion.

use super::listeners::{ListenerDataChangeBatches, ListenerDataChanges};
use crate::api::{
    DataChangeReadPosition,
    ListenerError,
    ReplicationEvent,
    ReplicationEventListener,
    RowProvider,
};
use std::sync::Arc;

/// Execute an owned listener child, returning its continuation after acceptance or rejection.
pub async fn drive_listener<C>(
    listener: &Arc<dyn ReplicationEventListener>,
    flow: ListenerFlow<C>,
) -> (C, Result<(), ListenerError>) {
    let mut step = flow.start();
    while let ListenerStep::Deliver { event, waiting } = step {
        let result = listener.on_event(event.into_event()).await;
        step = waiting.complete(result);
    }
    match step {
        ListenerStep::Accepted(continuation) => (continuation, Ok(())),
        ListenerStep::Failed {
            continuation,
            error,
        } => (continuation, Err(error)),
        ListenerStep::Deliver { .. } => unreachable!("the delivery loop handles every ready event"),
    }
}

/// Data-change notification containing its application position and row provider.
pub struct ListenerDataChange {
    /// Application-view position reached by the complete provider.
    pub position: DataChangeReadPosition,
    /// Callback-owned row stream, which may retain a read transaction until exhaustion/drop.
    pub rows: Box<RowProvider>,
}

impl ListenerDataChange {
    /// Transfer the provider into the established public callback event.
    pub fn into_event(self) -> ReplicationEvent {
        ReplicationEvent::DataChanged {
            position: self.position,
            rows: self.rows,
        }
    }
}

impl std::fmt::Debug for ListenerDataChange {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ListenerDataChange")
            .field("position", &self.position)
            .field("rows", &"<row provider>")
            .finish()
    }
}

/// Ready listener work, with ordered payloads and the caller's suspended continuation.
pub struct ListenerFlow<C> {
    /// Remaining ordinary update batches or the single required notification.
    events: ListenerEvents,
    /// Caller state returned only after all events succeed or one callback fails.
    continuation: C,
}

impl<C> ListenerFlow<C> {
    /// Create a listener flow for ordinary update batches, skipping batches without row changes.
    ///
    /// Each provider is constructed when its batch is selected for delivery.
    pub fn updates(batches: ListenerDataChangeBatches, continuation: C) -> Self {
        Self {
            events: ListenerEvents::Updates(batches.into_iter()),
            continuation,
        }
    }

    /// Prepare one required event, including an empty creation or activation snapshot.
    pub fn required(notification: ListenerDataChange, continuation: C) -> Self {
        Self {
            events: ListenerEvents::Required(Some(notification)),
            continuation,
        }
    }

    /// Deliver the next event, or return the continuation if no events remain.
    pub fn start(mut self) -> ListenerStep<C> {
        match self.events.next_notification() {
            Some(event) => ListenerStep::Deliver {
                event,
                waiting: DeliveringListener { remaining: self },
            },
            None => ListenerStep::Accepted(self.continuation),
        }
    }
}

/// Listener flow suspended until the current callback returns its result.
///
/// The delivery effect owns that callback's event and row provider.
pub struct DeliveringListener<C> {
    /// Remaining events and continuation, inaccessible until callback completion.
    remaining: ListenerFlow<C>,
}

impl<C> DeliveringListener<C> {
    /// Accept this callback result exactly once by consuming its waiting state.
    pub fn complete(self, result: Result<(), ListenerError>) -> ListenerStep<C> {
        match result {
            Ok(()) => self.remaining.start(),
            Err(error) => ListenerStep::Failed {
                continuation: self.remaining.continuation,
                error,
            },
        }
    }
}

/// Delivery effect or terminal outcome of the listener child.
pub enum ListenerStep<C> {
    /// Next notification and listener state awaiting the callback's result.
    Deliver {
        /// Notification and row provider to transfer to the callback.
        event: ListenerDataChange,
        /// Remaining listener work to resume after the callback completes.
        waiting: DeliveringListener<C>,
    },
    /// Caller continuation after every deliverable notification has been accepted.
    Accepted(C),
    /// Callback rejection and caller continuation after abandoning the remaining notifications.
    Failed {
        /// Caller state returned after abandoning the remaining notifications.
        continuation: C,
        /// Rejection returned by the callback for the current event.
        error: ListenerError,
    },
}

/// Remaining data-change notifications awaiting listener delivery.
///
/// Ordinary updates retain their original batch iterator; a required creation or
/// activation notification is retained directly, including when its provider is empty.
enum ListenerEvents {
    /// Remaining ordinary update batches, converted into notifications as they are consumed.
    ///
    /// Batches with no row changes are discarded without invoking the listener.
    Updates(smallvec::IntoIter<[ListenerDataChanges; 1]>),
    /// A creation or activation notification that must be delivered even without rows.
    ///
    /// `Some` holds the pending notification; `None` means it has already been transferred.
    Required(Option<ListenerDataChange>),
}

impl ListenerEvents {
    /// Consume the next deliverable notification, discarding empty ordinary update batches.
    ///
    /// `Some` transfers a notification and its provider; `None` means the source is exhausted.
    fn next_notification(&mut self) -> Option<ListenerDataChange> {
        match self {
            Self::Updates(batches) => {
                // find consumes the owning iterator, dropping each skipped empty batch.
                let next = batches.find(|batch| !batch.row_changes.is_empty());
                next.map(ListenerDataChanges::into_notification)
            }
            Self::Required(event) => event.take(),
        }
    }
}

/// Ordered delivery, lazy notification construction and provider ownership boundary tests.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{
        BatchProvider,
        DataChangeLineage,
        DatasetId,
        GroupReadToken,
        RowChange,
        RowChangeBatch,
        RowId,
        RowKey,
        RowProviderError,
        providers::VecRowProvider,
    };
    use flotsync_core::{
        GroupId,
        versions::{UpdateId, VersionVector},
    };
    use flotsync_utils::BoxFuture;
    use futures_util::FutureExt;
    use kompact::prelude::block_on;
    use std::{
        num::NonZeroUsize,
        sync::atomic::{AtomicBool, Ordering},
    };
    use uuid::Uuid;

    /// Construct the read token reached by one synthetic update.
    fn token(version: u32) -> GroupReadToken {
        let versions = VersionVector::initial(NonZeroUsize::new(1).expect("one member"))
            .with_update_applied(UpdateId {
                node_index: 0,
                version: u64::from(version),
            });
        GroupReadToken::from_group_version(GroupId(Uuid::nil()), versions)
    }

    /// Construct a batch with either visible changes or the ordinary empty-update case.
    fn batch(version: u32, changes: Vec<RowChange>) -> ListenerDataChanges {
        ListenerDataChanges {
            read_token: token(version),
            row_changes: changes,
        }
    }

    #[test]
    fn filters_empty_updates_orders_callbacks_and_returns_continuation_on_rejection() {
        let row_id = RowId::new(
            GroupId(Uuid::nil()),
            DatasetId::try_from_static("docs").expect("dataset"),
            RowKey(Uuid::nil()),
        );
        let mut batches = ListenerDataChangeBatches::new();
        batches.push(batch(1, Vec::new()));
        for version in 2..=4 {
            batches.push(batch(
                version,
                vec![RowChange::ordinary_delete(row_id.clone())],
            ));
        }
        let flow = ListenerFlow::updates(batches, "caller continuation");
        let ListenerStep::Deliver { event, waiting } = flow.start() else {
            panic!("first non-empty event")
        };
        assert_eq!(event.position.group_read_token(), &token(2));
        assert!(
            matches!(&waiting.remaining.events, ListenerEvents::Updates(batches) if batches.len() == 2)
        );
        let ListenerStep::Deliver { event, waiting } = waiting.complete(Ok(())) else {
            panic!("next event after callback completion")
        };
        assert_eq!(event.position.group_read_token(), &token(3));
        assert!(
            matches!(&waiting.remaining.events, ListenerEvents::Updates(batches) if batches.len() == 1)
        );
        let error = ListenerError::Rejected {
            message: "callback rejected".to_owned(),
        };
        assert!(matches!(
            waiting.complete(Err(error)),
            ListenerStep::Failed {
                continuation: "caller continuation",
                error: ListenerError::Rejected { .. }
            }
        ));
    }

    #[test]
    fn consumes_interspersed_and_trailing_empty_batches_without_rescanning() {
        let row_id = RowId::new(
            GroupId(Uuid::nil()),
            DatasetId::try_from_static("docs").expect("dataset"),
            RowKey(Uuid::nil()),
        );
        let mut batches = ListenerDataChangeBatches::new();
        for version in 1..=6 {
            let changes = match version {
                2 | 5 => vec![RowChange::ordinary_delete(row_id.clone())],
                _ => Vec::new(),
            };
            batches.push(batch(version, changes));
        }
        let mut events = ListenerEvents::Updates(batches.into_iter());
        for (version, remaining) in [(2, 4), (5, 1)] {
            let event = events.next_notification().expect("next non-empty batch");
            assert_eq!(event.position.group_read_token(), &token(version));
            assert!(
                matches!(&events, ListenerEvents::Updates(batches) if batches.len() == remaining)
            );
        }
        assert!(events.next_notification().is_none());
        assert!(matches!(&events, ListenerEvents::Updates(batches) if batches.len() == 0));
        assert!(events.next_notification().is_none());
    }

    #[test]
    fn single_update_retains_unconverted_inline_batch_until_delivery() {
        let row_id = RowId::new(
            GroupId(Uuid::nil()),
            DatasetId::try_from_static("docs").expect("dataset"),
            RowKey(Uuid::nil()),
        );
        let mut batches = ListenerDataChangeBatches::new();
        batches.push(batch(1, vec![RowChange::ordinary_delete(row_id)]));
        assert!(!batches.spilled(), "common case starts inline");
        let flow = ListenerFlow::updates(batches, ());
        assert!(matches!(&flow.events, ListenerEvents::Updates(batches) if batches.len() == 1));
        let ListenerStep::Deliver { event, waiting } = flow.start() else {
            panic!("single update must deliver")
        };
        assert_eq!(event.position.group_read_token(), &token(1));
        assert!(
            matches!(&waiting.remaining.events, ListenerEvents::Updates(batches) if batches.len() == 0)
        );
        assert!(matches!(
            waiting.complete(Ok(())),
            ListenerStep::Accepted(())
        ));
    }

    #[test]
    fn required_empty_event_can_be_dropped_without_exhaustion_before_acceptance() {
        let dropped = Arc::new(AtomicBool::new(false));
        let exhausted = Arc::new(AtomicBool::new(false));
        let flow = tracked_flow(dropped.clone(), exhausted.clone());
        assert!(matches!(&flow.events, ListenerEvents::Required(Some(_))));
        let ListenerStep::Deliver { event, waiting } = flow.start() else {
            panic!("empty activation still invokes callback")
        };
        assert!(!dropped.load(Ordering::SeqCst));
        assert!(matches!(
            &waiting.remaining.events,
            ListenerEvents::Required(None)
        ));
        drop(event);
        assert!(dropped.load(Ordering::SeqCst));
        assert!(
            !exhausted.load(Ordering::SeqCst),
            "handoff must not silently drain the provider"
        );
        assert!(matches!(
            waiting.complete(Ok(())),
            ListenerStep::Accepted("continuation")
        ));
    }

    #[test]
    fn exhaustion_and_callback_completion_are_separate_boundaries() {
        let dropped = Arc::new(AtomicBool::new(false));
        let exhausted = Arc::new(AtomicBool::new(false));
        let flow = tracked_flow(dropped.clone(), exhausted.clone());
        let ListenerStep::Deliver { event, waiting } = flow.start() else {
            panic!("required callback")
        };
        let ListenerDataChange { mut rows, .. } = event;
        let batch = rows.new_batch();
        assert!(
            block_on(rows.fill_batch(batch))
                .expect("provider exhaustion")
                .is_none()
        );
        assert!(exhausted.load(Ordering::SeqCst));
        assert!(!dropped.load(Ordering::SeqCst));
        drop(rows);
        assert!(dropped.load(Ordering::SeqCst));
        assert!(matches!(
            waiting.complete(Ok(())),
            ListenerStep::Accepted("continuation")
        ));
    }

    /// Track an empty provider transferred by a required creation/activation notification.
    fn tracked_flow(
        dropped: Arc<AtomicBool>,
        exhausted: Arc<AtomicBool>,
    ) -> ListenerFlow<&'static str> {
        let rows = Box::new(TrackedProvider {
            inner: VecRowProvider::default(),
            dropped,
            exhausted,
        });
        let position = DataChangeReadPosition::new(DataChangeLineage::Update, token(0));
        ListenerFlow::required(ListenerDataChange { position, rows }, "continuation")
    }

    /// Row provider fixture recording exhaustion and drop as separate events.
    struct TrackedProvider {
        /// Empty provider implementing the fixture's batch operations.
        inner: VecRowProvider,
        /// Set when the provider is dropped, regardless of whether it was exhausted.
        dropped: Arc<AtomicBool>,
        /// Set only when a fill call returns no remaining rows.
        exhausted: Arc<AtomicBool>,
    }

    impl BatchProvider for TrackedProvider {
        type Batch = RowChangeBatch;

        fn new_batch(&self) -> Self::Batch {
            self.inner.new_batch()
        }

        fn fill_batch(
            &mut self,
            batch: Self::Batch,
        ) -> BoxFuture<'_, Result<Option<Self::Batch>, RowProviderError>> {
            async move {
                let next = self.inner.fill_batch(batch).await?;
                if next.is_none() {
                    self.exhausted.store(true, Ordering::SeqCst);
                }
                Ok(next)
            }
            .boxed()
        }
    }

    impl Drop for TrackedProvider {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }
}
