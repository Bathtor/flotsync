//! Inbound-delivery support local to the runtime component.

use super::*;

/// Source-specific sender validation for an inbound update.
pub(super) enum InboundUpdateOrigin {
    /// Direct `Update` delivery where the envelope sender must be the producer.
    Producer { sender: MemberIdentity },
    /// Catch-up delivery where the envelope sender forwards another producer's update.
    Forwarder { sender: MemberIdentity },
}

/// Result of processing one inbound update.
pub(super) struct InboundUpdateOutcome {
    /// Listener batches produced by updates that became newly applicable.
    pub(super) event_batches: ListenerDataChangeBatches,
    /// Producer-version ranges still needed before blocked updates can apply.
    pub(super) needed_ranges: Vec<UpdateRangeMessage>,
    /// Producer versions now known to be present in the local update log.
    pub(super) observed_available: Vec<UpdateRangeMessage>,
}

/// Catch-up notifications derived from one observed summary message.
pub(super) struct SummaryCatchUpObservation {
    /// Group whose local progress was compared with the observed summary.
    pub(super) group_id: GroupId,
    /// Local pending update-log versions that can be advertised as available.
    pub(super) observed_available: Vec<UpdateRangeMessage>,
    /// Versions the summary says exist elsewhere and are not already pending locally.
    pub(super) needed_ranges: Vec<UpdateRangeMessage>,
}

/// Envelope metadata included in inbound delivery fault logs.
#[derive(Clone, Debug)]
pub(super) enum InboundDeliveryContext {
    /// Recipient-addressed envelope handed over by reliable delivery.
    Reliable {
        sender: MemberIdentity,
        recipient: MemberIdentity,
        message_id: MessageId,
    },
    /// Group-scoped envelope handed over by group broadcast.
    Group {
        group_id: GroupId,
        sender: MemberIdentity,
        message_id: MessageId,
    },
}

impl InboundDeliveryContext {
    pub(super) fn group(header: &GroupMessageHeader) -> Self {
        Self::Group {
            group_id: header.group_id,
            sender: header.sender.clone(),
            message_id: header.message_id,
        }
    }

    pub(super) fn reliable(header: &ReliableMessageHeader) -> Self {
        Self::Reliable {
            sender: header.sender.clone(),
            recipient: header.recipient.clone(),
            message_id: header.message_id,
        }
    }
}

impl std::fmt::Display for InboundDeliveryContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Reliable {
                sender,
                recipient,
                message_id,
            } => write!(
                f,
                "reliable delivery message {message_id} from {sender} to {recipient}"
            ),
            Self::Group {
                group_id,
                sender,
                message_id,
            } => write!(
                f,
                "group broadcast message {message_id} for group {group_id} from {sender}"
            ),
        }
    }
}

/// Inbound delivery failure paired with the envelope that caused it.
pub(super) struct InboundDeliveryFailure {
    pub(super) context: InboundDeliveryContext,
    pub(super) error: Box<InboundDeliveryError>,
}

impl InboundDeliveryFailure {
    pub(super) fn new(context: InboundDeliveryContext, error: InboundDeliveryError) -> Self {
        Self {
            context,
            error: Box::new(error),
        }
    }
}

/// Convert a decoded summary message into the runtime summary representation.
pub(super) fn summary_from_message(sender: MemberIdentity, message: SummaryMessage) -> Summary {
    Summary {
        group_id: message.group_id,
        responder: sender,
        has_versions: message.has_versions,
    }
}

/// Finish inbound handling after escalating fatal failures.
pub(super) fn handled_after_inbound_failure(
    action: InboundFailureAction,
    failure: &InboundDeliveryFailure,
) -> HandlerResult {
    panic_if_fatal_inbound_failure(action, failure);
    Handled::OK
}

/// Escalate inbound delivery failures that the runtime classifies as fatal.
pub(super) fn panic_if_fatal_inbound_failure(
    action: InboundFailureAction,
    failure: &InboundDeliveryFailure,
) {
    if matches!(action, InboundFailureAction::Fatal) {
        panic!(
            "fatal inbound {} failure: {}",
            failure.context, failure.error
        );
    }
}
