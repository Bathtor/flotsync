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
        listener
            .on_event(ReplicationEvent::DataChanged {
                read_token: event_batch.read_token,
                rows: Box::new(VecRowProvider::new(event_batch.row_changes)),
            })
            .await?;
    }
    Ok(())
}

/// Notify listeners when accepted pending activation produced externally visible rows.
pub(super) async fn notify_pending_activation_data_changes(
    listener: Arc<dyn ReplicationEventListener>,
    outcome: PendingGroupActivationOutcome,
) -> Result<(), ListenerError> {
    notify_listener_data_changes(
        listener,
        smallvec![ListenerDataChanges {
            read_token: outcome.read_token,
            row_changes: outcome.row_changes,
        }],
    )
    .await
}
