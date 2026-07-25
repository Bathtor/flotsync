//! Listener notification support local to the runtime component.

use super::*;

/// Listener notification batches with inline storage for the common single batch.
pub(super) type ListenerDataChangeBatches = SmallVec<[ListenerDataChanges; 1]>;

/// One listener notification batch paired with the read token reached by that batch.
pub(super) struct ListenerDataChanges {
    pub(super) read_token: ReadToken,
    pub(super) row_changes: Vec<RowChange>,
}

/// Notify a listener about data changes while translating its error for inbound delivery.
pub(super) async fn notify_listener_batches(
    listener: Arc<dyn ReplicationEventListener>,
    event_batches: ListenerDataChangeBatches,
) -> Result<(), InboundDeliveryError> {
    notify_listener_data_changes(listener, event_batches)
        .await
        .context(inbound::NotifyListenerSnafu)
}

/// Emit non-empty listener data-change batches in their prepared order.
pub(super) async fn notify_listener_data_changes(
    listener: Arc<dyn ReplicationEventListener>,
    event_batches: ListenerDataChangeBatches,
) -> Result<(), ListenerError> {
    for event_batch in event_batches {
        if event_batch.row_changes.is_empty() {
            continue;
        }
        notify_listener_data_change(listener.clone(), event_batch).await?;
    }
    Ok(())
}

/// Notify the listener about one batch, including an empty batch that advances its read position.
pub(super) async fn notify_listener_data_change(
    listener: Arc<dyn ReplicationEventListener>,
    event_batch: ListenerDataChanges,
) -> Result<(), ListenerError> {
    listener
        .on_event(ReplicationEvent::DataChanged {
            read_token: event_batch.read_token,
            rows: Box::new(VecRowProvider::new(event_batch.row_changes)),
        })
        .await
}

/// Notify the listener after accepted activation, including an empty initial snapshot.
pub(super) async fn notify_pending_activation_data_changes(
    listener: Arc<dyn ReplicationEventListener>,
    outcome: PendingGroupActivationOutcome,
) -> Result<(), ListenerError> {
    notify_listener_data_change(
        listener,
        ListenerDataChanges {
            read_token: outcome.read_token,
            row_changes: outcome.row_changes,
        },
    )
    .await
}
