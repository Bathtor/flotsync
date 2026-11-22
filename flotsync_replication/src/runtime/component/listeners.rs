//! Listener notification support local to the runtime component.

use super::{
    listener_flow::{ListenerDataChange, ListenerFlow, drive_listener},
    *,
};

/// Listener notification batches with inline storage for the common single batch.
pub type ListenerDataChangeBatches = SmallVec<[ListenerDataChanges; 1]>;

/// One listener notification batch paired with the read token reached by that batch.
pub struct ListenerDataChanges {
    /// Group-local position reached after every row change in this batch is consumed.
    pub read_token: GroupReadToken,
    /// Prepared ordinary changes; empty update batches are filtered by the listener child.
    pub row_changes: Vec<RowChange>,
}

impl ListenerDataChanges {
    /// Move prepared ordinary changes into the callback-owned provider.
    pub fn into_notification(self) -> ListenerDataChange {
        ListenerDataChange {
            position: DataChangeReadPosition::new(DataChangeLineage::Update, self.read_token),
            rows: Box::new(VecRowProvider::new(self.row_changes)),
        }
    }
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
    let flow = ListenerFlow::updates(event_batches, ());
    let ((), result) = drive_listener(&listener, flow).await;
    result
}

/// Notify the listener about one batch, including an empty batch that advances its read position.
pub(super) async fn notify_listener_data_change(
    listener: Arc<dyn ReplicationEventListener>,
    event_batch: ListenerDataChanges,
) -> Result<(), ListenerError> {
    let flow = ListenerFlow::required(event_batch.into_notification(), ());
    let ((), result) = drive_listener(&listener, flow).await;
    result
}

/// Notify the listener after accepted activation, including an empty initial snapshot.
pub(super) async fn notify_pending_activation_data_changes(
    listener: Arc<dyn ReplicationEventListener>,
    outcome: PendingGroupActivationOutcome,
) -> Result<(), ListenerError> {
    let flow = ListenerFlow::required(
        ListenerDataChange {
            position: outcome.read_position,
            rows: outcome.rows,
        },
        (),
    );
    let ((), result) = drive_listener(&listener, flow).await;
    result
}
