use super::errors::{InboundDeliveryError, InboundFailureAction, SummaryError, inbound, summary};
use crate::{
    api::{ApiError, ApiExternalSnafu, Summary, SummaryRequest},
    codecs::messages::{
        RuntimeMessage,
        SummaryRequestMessage,
        WireRuntimeMessage,
        WireSummaryMessage,
    },
    delivery::{
        contracts::{
            ReliableDeliveryPort,
            ReliableDeliveryPortIndication,
            ReliableDeliveryPortRequest,
        },
        reliable_delivery::{
            ReliableDeliveryDeliver,
            ReliableDeliverySubmit,
            ReliableMessageEnvelope,
            ReliableMessageHeader,
        },
        shared::{MessageId, PlaintextPayload, ReliableMessageScope},
    },
};
use flotsync_core::{GroupId, MemberIdentity, membership::SharedGroupMemberships};
use flotsync_messages::proto::{DecodeProtoView, EncodeProto};
use flotsync_utils::{KClaimablePromise, OptionExt as _};
use kompact::prelude::*;
use snafu::prelude::*;
use std::{collections::HashMap, num::NonZeroUsize, time::Duration};
use uuid::Uuid;

/// Local-actor messages understood by [`SummaryRequestManagerComponent`].
#[derive(Debug)]
pub(super) enum SummaryRequestManagerMessage {
    /// Ask one group member for its current group version vector.
    RequestSummary(Ask<SummaryRequest, Result<Summary, ApiError>>),
}

/// Outstanding application summary request waiting for a matching runtime reply.
struct PendingSummaryRequest {
    group_id: GroupId,
    target: MemberIdentity,
    promise: KPromise<Result<Summary, ApiError>>,
    timeout_timer: ScheduledTimer,
}

/// Envelope metadata included in inbound summary fault logs.
#[derive(Clone, Debug)]
enum SummaryInboundContext {
    /// Recipient-addressed envelope handed over by reliable delivery.
    Reliable {
        sender: MemberIdentity,
        recipient: MemberIdentity,
        message_id: MessageId,
    },
}

impl SummaryInboundContext {
    fn reliable(header: &ReliableMessageHeader) -> Self {
        Self::Reliable {
            sender: header.sender.clone(),
            recipient: header.recipient.clone(),
            message_id: header.message_id,
        }
    }
}

impl std::fmt::Display for SummaryInboundContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Reliable {
                sender,
                recipient,
                message_id,
            } => write!(
                f,
                "reliable delivery summary message {message_id} from {sender} to {recipient}"
            ),
        }
    }
}

/// Inbound summary failure paired with the envelope that caused it.
struct SummaryInboundFailure {
    context: SummaryInboundContext,
    error: Box<InboundDeliveryError>,
}

impl SummaryInboundFailure {
    fn new(context: SummaryInboundContext, error: InboundDeliveryError) -> Self {
        Self {
            context,
            error: Box::new(error),
        }
    }
}

/// Kompact component that owns summary request/response protocol state.
#[derive(ComponentDefinition)]
pub(super) struct SummaryRequestManagerComponent {
    ctx: ComponentContext<Self>,
    reliable_delivery: RequiredPort<ReliableDeliveryPort>,
    local_member: MemberIdentity,
    group_memberships: SharedGroupMemberships,
    request_timeout: Duration,
    pending_summaries: HashMap<Uuid, PendingSummaryRequest>,
}

impl SummaryRequestManagerComponent {
    pub(super) fn new(
        local_member: MemberIdentity,
        group_memberships: SharedGroupMemberships,
        request_timeout: Duration,
    ) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            reliable_delivery: RequiredPort::uninitialised(),
            local_member,
            group_memberships,
            request_timeout,
            pending_summaries: HashMap::new(),
        }
    }

    fn reply_api<T>(
        &self,
        promise: KPromise<Result<T, ApiError>>,
        operation: &'static str,
        reply: Result<T, ApiError>,
    ) where
        T: Send + 'static,
    {
        if promise.fulfil(reply).is_err() {
            warn!(self.log(), "dropping {operation} reply");
        }
    }

    fn validate_summary_request(&self, request: &SummaryRequest) -> Result<(), SummaryError> {
        let memberships = self.group_memberships.snapshot();
        let members =
            memberships
                .members(&request.group_id)
                .context(summary::UnknownGroupSnafu {
                    group_id: request.group_id,
                })?;
        ensure!(
            members.contains(&request.target),
            summary::TargetNotInGroupSnafu {
                group_id: request.group_id,
                target: request.target.clone(),
            }
        );
        Ok(())
    }

    fn submit_reliable_runtime_message(
        &mut self,
        recipient: MemberIdentity,
        message: &RuntimeMessage,
    ) {
        let payload = message.encode_proto_to_bytes();
        self.reliable_delivery
            .trigger(ReliableDeliveryPortRequest::Submit(
                ReliableDeliverySubmit {
                    envelope: ReliableMessageEnvelope::<PlaintextPayload> {
                        header: ReliableMessageHeader {
                            sender: self.local_member.clone(),
                            recipient,
                            message_id: MessageId(Uuid::new_v4()),
                            scope: ReliableMessageScope::Group {
                                group_id: message.group_id(),
                            },
                        },
                        payload: PlaintextPayload { bytes: payload },
                    },
                },
            ));
    }

    fn fulfil_pending_summary(&mut self, correlation_id: Uuid, summary: Summary) {
        let Some(pending) = self.pending_summaries.remove(&correlation_id) else {
            return;
        };
        if summary.group_id != pending.group_id || summary.responder != pending.target {
            warn!(
                self.log(),
                "ignoring mismatched summary response for group {} from {}",
                summary.group_id,
                summary.responder
            );
            self.pending_summaries.insert(correlation_id, pending);
            return;
        }
        self.cancel_timer(pending.timeout_timer);
        if pending.promise.fulfil(Ok(summary)).is_err() {
            warn!(self.log(), "dropping request_summary reply");
        }
    }

    fn handle_summary_timeout(
        &mut self,
        correlation_id: Uuid,
        expected_timer: &ScheduledTimer,
    ) -> HandlerResult {
        let Some(pending) = self.pending_summaries.get(&correlation_id) else {
            return Handled::OK;
        };
        if &pending.timeout_timer != expected_timer {
            return Handled::OK;
        }
        let pending = self
            .pending_summaries
            .remove(&correlation_id)
            .whatever_unrecoverable("checked pending summary must still exist")?;
        if pending
            .promise
            .fulfil(Err(ApiError::SummaryTimedOut {
                group_id: pending.group_id,
                target: pending.target,
            }))
            .is_err()
        {
            warn!(self.log(), "dropping request_summary timeout reply");
        }
        Handled::OK
    }

    fn record_inbound_failure(&self, failure: &SummaryInboundFailure) -> InboundFailureAction {
        let action = failure.error.failure_action();
        match action {
            InboundFailureAction::Drop => {
                warn!(
                    self.log(),
                    "dropping inbound {} after recoverable error: {}",
                    failure.context,
                    failure.error
                );
            }
            InboundFailureAction::Fatal => {
                error!(
                    self.log(),
                    "fatal inbound {} failure: {}", failure.context, failure.error
                );
            }
        }
        action
    }

    fn member_count_for_group(
        &self,
        group_id: GroupId,
    ) -> Result<NonZeroUsize, InboundDeliveryError> {
        let memberships = self.group_memberships.snapshot();
        let members = memberships
            .members(&group_id)
            .context(inbound::UnknownHostedGroupSnafu { group_id })?;
        Ok(NonZeroUsize::new(members.len()).expect("group members must not be empty"))
    }

    fn handle_summary(
        &mut self,
        context: SummaryInboundContext,
        sender: MemberIdentity,
        processed: KClaimablePromise<()>,
        message: WireSummaryMessage,
    ) -> HandlerResult {
        let reply = self.handle_summary_payload(sender, processed, message);
        if let Err(error) = reply {
            let failure = SummaryInboundFailure::new(context, error);
            let action = self.record_inbound_failure(&failure);
            return handled_after_inbound_failure(action, &failure);
        }
        Handled::OK
    }

    fn handle_summary_payload(
        &mut self,
        sender: MemberIdentity,
        processed: KClaimablePromise<()>,
        message: WireSummaryMessage,
    ) -> Result<(), InboundDeliveryError> {
        let group_id = message.group_id;
        let member_count = self.member_count_for_group(group_id)?;
        let summary_message = message
            .into_runtime(member_count)
            .context(inbound::DecodeReadVersionsSnafu { group_id })?;
        let summary = Summary {
            group_id: summary_message.group_id,
            responder: sender,
            has_versions: summary_message.has_versions,
        };
        self.fulfil_pending_summary(summary_message.correlation_id, summary);
        processed
            .complete()
            .context(inbound::CompleteProcessedPromiseSnafu {
                group_id: summary_message.group_id,
            })?;
        Ok(())
    }

    fn handle_reliable_delivery(&mut self, deliver: ReliableDeliveryDeliver) -> HandlerResult {
        let context = SummaryInboundContext::reliable(&deliver.envelope.header);
        let message =
            match WireRuntimeMessage::decode_proto_view_from_slice(&deliver.envelope.payload.bytes)
                .context(inbound::DecodeMessageSnafu)
            {
                Ok(message) => message,
                Err(error) => {
                    let failure = SummaryInboundFailure::new(context, error);
                    let action = self.record_inbound_failure(&failure);
                    return handled_after_inbound_failure(action, &failure);
                }
            };
        match message {
            WireRuntimeMessage::Summary(message) => {
                let sender = deliver.envelope.header.sender.clone();
                self.handle_summary(context, sender, deliver.processed, message)
            }
            WireRuntimeMessage::Update(_)
            | WireRuntimeMessage::SummaryRequest(_)
            | WireRuntimeMessage::NeedRange(_)
            | WireRuntimeMessage::UpdateBatch(_)
            | WireRuntimeMessage::GroupInvitation(_)
            | WireRuntimeMessage::MigrationProposal(_) => Handled::OK,
        }
    }

    fn handle_request_summary(
        &mut self,
        ask: Ask<SummaryRequest, Result<Summary, ApiError>>,
    ) -> HandlerResult {
        let (promise, request) = ask.take();
        if let Err(error) = self.validate_summary_request(&request) {
            let reply = Err(error).boxed().context(ApiExternalSnafu);
            self.reply_api(promise, "request_summary", reply);
            return Handled::OK;
        }

        let correlation_id = Uuid::new_v4();
        let timeout_group_id = request.group_id;
        let timeout_target = request.target.clone();
        let timeout_timer =
            self.schedule_once(self.request_timeout, move |component, expected_timer| {
                component.handle_summary_timeout(correlation_id, &expected_timer)
            });
        self.pending_summaries.insert(
            correlation_id,
            PendingSummaryRequest {
                group_id: timeout_group_id,
                target: timeout_target,
                promise,
                timeout_timer,
            },
        );
        let message = RuntimeMessage::SummaryRequest(SummaryRequestMessage {
            group_id: request.group_id,
            correlation_id,
        });
        self.submit_reliable_runtime_message(request.target, &message);
        Handled::OK
    }
}

ignore_lifecycle!(SummaryRequestManagerComponent);

impl Require<ReliableDeliveryPort> for SummaryRequestManagerComponent {
    fn handle(&mut self, indication: ReliableDeliveryPortIndication) -> HandlerResult {
        let ReliableDeliveryPortIndication::Deliver(deliver) = indication;
        self.handle_reliable_delivery(deliver)
    }
}

impl Actor for SummaryRequestManagerComponent {
    type Message = SummaryRequestManagerMessage;

    fn receive_local(&mut self, msg: Self::Message) -> HandlerResult {
        match msg {
            SummaryRequestManagerMessage::RequestSummary(ask) => self.handle_request_summary(ask),
        }
    }
}

fn panic_if_fatal_inbound_failure(action: InboundFailureAction, failure: &SummaryInboundFailure) {
    if matches!(action, InboundFailureAction::Fatal) {
        panic!(
            "fatal inbound {} failure: {}",
            failure.context, failure.error
        );
    }
}

fn handled_after_inbound_failure(
    action: InboundFailureAction,
    failure: &SummaryInboundFailure,
) -> HandlerResult {
    panic_if_fatal_inbound_failure(action, failure);
    Handled::OK
}
