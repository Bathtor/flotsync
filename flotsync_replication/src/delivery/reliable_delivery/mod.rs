//! Recipient-addressed durable delivery types and the minimal direct-runtime slice.

mod wire;

use super::{
    contracts::{
        ReliableDeliveryPort,
        ReliableDeliveryPortIndication,
        ReliableDeliveryPortRequest,
    },
    ingress::InboundDeliveryMeta,
    route_transport::{
        FlotsyncSerializable,
        RouteDiscoveryPort,
        RouteSharingKind,
        RouteTransportActorMessage,
        RouteTransportNackReason,
        RouteTransportSend,
        RouteTransportSubmitResult,
        SendRouteCandidate,
        TransportRouteKey,
    },
    shared::{
        ActiveRouteRecord,
        DetachedSignature,
        EncryptedPayload,
        LogicalRouteId,
        MailboxItemId,
        MessageId,
        PendingRouteReason,
        RelayIdentity,
        RouteActiveState,
        RouteSendId,
        SignatureScheme,
        SignedEnvelopeFooter,
        StableRouteKey,
        WorkScopeKey,
    },
};
use crate::api::MemberIdentity;
use bytes::Bytes;
use flotsync_core::member::TrieMap;
use flotsync_messages::delivery as delivery_proto;
use flotsync_utils::{KClaimablePromise, NonOwningPhantomData, OptionExt as _, ResultExt as _};
use kompact::{kompact_config, prelude::*};
use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    sync::Arc,
    time::{Duration, Instant},
};
use uuid::Uuid;
use wire::{
    recipient_ack_from_wire,
    recipient_ack_to_wire_format,
    reliable_envelope_from_wire,
    reliable_envelope_to_wire_format,
};

/// Plaintext recipient-addressed envelope header.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReliableMessageHeader {
    pub sender: MemberIdentity,
    pub recipient: MemberIdentity,
    pub message_id: MessageId,
}

/// Immutable sender-signed envelope used for both direct recipient delivery and
/// relay mailbox storage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReliableMessageEnvelope {
    pub header: ReliableMessageHeader,
    pub payload: EncryptedPayload,
    pub footer: SignedEnvelopeFooter,
}

impl ReliableMessageEnvelope {
    fn to_wire_format(&self) -> delivery_proto::DeliveryBoundaryFrame {
        reliable_envelope_to_wire_format(self)
    }
}

/// Replication-to-delivery request for one recipient-addressed message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReliableDeliverySubmit {
    pub envelope: ReliableMessageEnvelope,
}

/// Inbound reliable-delivery message delivered by the network-facing service.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReliableDeliveryDeliver {
    pub envelope: ReliableMessageEnvelope,
    /// Completing this handle confirms that the recipient processed the
    /// delivered item and may now emit a semantic recipient ack.
    pub processed: KClaimablePromise<()>,
}

/// Inbound reliable-delivery payload handed to the semantic owner from
/// delivery ingress.
#[derive(Clone, Debug, PartialEq)]
pub struct ReliableDeliveryInboundDeliver<R> {
    /// Shared ingress metadata derived before the semantic handoff.
    pub meta: InboundDeliveryMeta<R>,
    /// Fully decoded reliable-delivery boundary frame owned by the generated
    /// protobuf types.
    pub frame: delivery_proto::ReliableDeliveryFrame,
}

/// Internal ingress port that feeds decoded reliable-delivery boundary frames
/// into the reliable-delivery service.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReliableDeliveryInboundPort<R>(NonOwningPhantomData<R>);

impl<R> Port for ReliableDeliveryInboundPort<R>
where
    R: Clone + std::fmt::Debug + Send + Sync + 'static,
{
    /// Delivery ingress is the sole producer for this internal semantic
    /// stream.
    type Request = Never;
    type Indication = ReliableDeliveryInboundDeliver<R>;
}

/// Queue-owned in-memory state for one accepted reliable-delivery message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReliableDeliveryWorkItem {
    pub submit: ReliableDeliverySubmit,
    pub recipient_route: ActiveRouteRecord,
    pub relay_routes: Vec<ActiveRouteRecord>,
    pub recipient_ack: RecipientAckStatus,
}

/// Sender-side completion tracking for recipient-addressed durable delivery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecipientAckStatus {
    Pending,
    Observed { ack: RecipientAck },
}

/// Plaintext recipient-ack header.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecipientAckHeader {
    pub message_id: MessageId,
    pub original_sender: MemberIdentity,
    pub recipient: MemberIdentity,
}

/// Recipient-signed completion signal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecipientAck {
    pub header: RecipientAckHeader,
    pub footer: SignedEnvelopeFooter,
}

impl RecipientAck {
    fn to_wire_format(&self) -> delivery_proto::DeliveryBoundaryFrame {
        recipient_ack_to_wire_format(self)
    }
}

/// Signed proof used by the recipient when checking in with a relay mailbox.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdentityProof {
    pub signer: MemberIdentity,
    pub challenge: Uuid,
    pub footer: SignedEnvelopeFooter,
}

/// Recipient-driven mailbox retrieval request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MailboxFetch {
    pub recipient: MemberIdentity,
    pub freshness_token: Uuid,
    pub proof: IdentityProof,
}

/// One mailbox item returned by a relay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MailboxItem {
    pub item_id: MailboxItemId,
    pub envelope: ReliableMessageEnvelope,
}

/// One full fetch response from a relay mailbox.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MailboxBatch {
    pub relay: RelayIdentity,
    pub recipient: MemberIdentity,
    pub items: Vec<MailboxItem>,
}

/// Handle used when acknowledging mailbox items back to the relay.
///
/// The protocol draft explicitly leaves room for either stable message ids,
/// relay-issued handles, or both.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum MailboxAckHandle {
    Message(MessageId),
    Item(MailboxItemId),
}

/// Relay cleanup acknowledgement sent only after the recipient durably accepted
/// the mailbox item locally.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MailboxAck {
    pub relay: RelayIdentity,
    pub recipient: MemberIdentity,
    pub acknowledgements: Vec<MailboxAckHandle>,
}

/// Minimal semantic owner for recipient-addressed reliable delivery.
///
/// This first slice supports only the direct envelope path plus semantic
/// recipient acks after explicit processed confirmation. Relay mailbox and
/// retry policy remain follow-up work.
#[derive(ComponentDefinition)]
pub struct ReliableDeliveryComponent {
    ctx: ComponentContext<Self>,
    delivery_port: ProvidedPort<ReliableDeliveryPort>,
    ingress_inbound_port: RequiredPort<TransportReliableDeliveryInboundPort>,
    discovery_port: RequiredPort<TransportRouteDiscoveryPort>,
    route_transport: ActorRefStrong<RouteTransportActorMessage<TransportRouteKey>>,
    direct_peer_routes: TrieMap<SendRouteCandidate<TransportRouteKey>>,
    sender_work_items: HashMap<MessageId, ReliableDeliveryWorkItem>,
    inbound_deliveries: HashMap<MessageId, PendingInboundDelivery>,
    retry_queue: RetryQueue,
    retry_timer: Option<ScheduledTimer>,
    retry_delay: Duration,
    recipient_ack_timeout: Duration,
}

impl ReliableDeliveryComponent {
    /// Create one new reliable-delivery component around the shared
    /// route-transport actor.
    #[must_use]
    pub fn new(
        route_transport: ActorRefStrong<RouteTransportActorMessage<TransportRouteKey>>,
    ) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            delivery_port: ProvidedPort::uninitialised(),
            ingress_inbound_port: RequiredPort::uninitialised(),
            discovery_port: RequiredPort::uninitialised(),
            route_transport,
            direct_peer_routes: TrieMap::new(),
            sender_work_items: HashMap::new(),
            inbound_deliveries: HashMap::new(),
            retry_queue: RetryQueue::new(),
            retry_timer: None,
            retry_delay: DEFAULT_RETRY_DELAY,
            recipient_ack_timeout: DEFAULT_RECIPIENT_ACK_TIMEOUT,
        }
    }

    fn now(&self) -> Instant {
        self.ctx.system().now()
    }

    fn new_direct_route_record(
        recipient: &MemberIdentity,
        message_id: MessageId,
        state: RouteActiveState,
    ) -> ActiveRouteRecord {
        ActiveRouteRecord {
            key: StableRouteKey {
                scope: WorkScopeKey::Reliable {
                    recipient: recipient.clone(),
                    message_id,
                },
                route_id: LogicalRouteId::peer(recipient.clone()),
            },
            state,
        }
    }

    fn new_sender_work_item(submit: ReliableDeliverySubmit) -> ReliableDeliveryWorkItem {
        let recipient = submit.envelope.header.recipient.clone();
        let message_id = submit.envelope.header.message_id;
        ReliableDeliveryWorkItem {
            submit,
            recipient_route: Self::new_direct_route_record(
                &recipient,
                message_id,
                RouteActiveState::PendingRoute {
                    retry_after: None,
                    reason: PendingRouteReason::ReachabilityUnknown,
                },
            ),
            relay_routes: Vec::new(),
            recipient_ack: RecipientAckStatus::Pending,
        }
    }

    fn load_retry_delay(&self) -> Duration {
        match self.ctx.config().read_or_default(&config_keys::RETRY_DELAY) {
            Ok(delay) => delay,
            Err(error) => {
                warn!(
                    self.log(),
                    "Failed to load reliable-delivery retry delay from {}: {}. Falling back to {:?}",
                    config_keys::RETRY_DELAY.key,
                    error,
                    DEFAULT_RETRY_DELAY
                );
                DEFAULT_RETRY_DELAY
            }
        }
    }

    fn load_recipient_ack_timeout(&self) -> Duration {
        match self
            .ctx
            .config()
            .read_or_default(&config_keys::RECIPIENT_ACK_TIMEOUT)
        {
            Ok(timeout) => timeout,
            Err(error) => {
                warn!(
                    self.log(),
                    "Failed to load reliable-delivery recipient ack timeout from {}: {}. Falling back to {:?}",
                    config_keys::RECIPIENT_ACK_TIMEOUT.key,
                    error,
                    DEFAULT_RECIPIENT_ACK_TIMEOUT
                );
                DEFAULT_RECIPIENT_ACK_TIMEOUT
            }
        }
    }

    fn handle_submit_request(&mut self, submit: ReliableDeliverySubmit) -> HandlerResult {
        let message_id = submit.envelope.header.message_id;
        if self.sender_work_items.contains_key(&message_id) {
            warn!(
                self.log(),
                "Reliable delivery rejected duplicate submit for existing {message_id}"
            );
            return Handled::OK;
        }
        self.sender_work_items
            .insert(message_id, Self::new_sender_work_item(submit));
        self.try_dispatch_sender_message(message_id);
        Handled::OK
    }

    fn handle_discovery_update(&mut self, update: TransportDiscoveryRouteUpdate) -> HandlerResult {
        match update {
            TransportDiscoveryRouteUpdate::PeerRoutes { peer, routes, .. } => {
                if let Some(route) = select_best_direct_route(routes) {
                    // TODO(flotsync-irn): Store multiple direct routes per peer
                    // so reliable delivery can retry on an alternate route after
                    // one direct send fails.
                    self.direct_peer_routes.insert(peer.clone(), route);
                } else {
                    self.direct_peer_routes.remove(&peer);
                }
                self.retry_pending_sender_work_for_peer(&peer);
                self.retry_pending_inbound_acks_for_peer(&peer);
            }
            TransportDiscoveryRouteUpdate::RelayRoutes { .. } => {
                // TODO(flotsync-fx5): Consume relay-route discovery updates once
                // the reliable-delivery relay path is implemented.
            }
        }
        Handled::OK
    }

    fn handle_ingress_indication(
        &mut self,
        indication: ReliableDeliveryInboundDeliver<TransportRouteKey>,
    ) -> HandlerResult {
        let body = indication.frame.body.with_whatever_benign(|| {
            format!(
                "Reliable delivery dropped inbound frame with empty body target={:?}",
                indication.meta.target
            )
        })?;

        match body {
            delivery_proto::reliable_delivery_frame::Body::Envelope(envelope) => {
                self.handle_inbound_envelope(*envelope)
            }
            delivery_proto::reliable_delivery_frame::Body::RecipientAck(ack) => {
                self.handle_inbound_recipient_ack(*ack)
            }
            other => {
                debug!(
                    self.log(),
                    "Reliable delivery ignored unsupported inbound frame variant={other:?}"
                );
                Handled::OK
            }
        }
    }

    fn handle_inbound_envelope(
        &mut self,
        envelope: delivery_proto::ReliableEnvelopeWire,
    ) -> HandlerResult {
        let envelope = reliable_envelope_from_wire(envelope)
            .whatever_benign("Reliable delivery dropped inbound envelope that failed to decode")?;
        let message_id = envelope.header.message_id;
        if self.handle_inbound_envelope_if_already_tracked(message_id) {
            return Handled::OK;
        }

        let (processed, processed_future) = KClaimablePromise::create_pair();
        self.inbound_deliveries.insert(
            message_id,
            PendingInboundDelivery {
                envelope: envelope.clone(),
                state: PendingInboundDeliveryState::AwaitingProcessed,
                ack: None,
            },
        );
        self.delivery_port
            .trigger(ReliableDeliveryPortIndication::Deliver(
                ReliableDeliveryDeliver {
                    envelope,
                    processed,
                },
            ));
        debug!(
            self.log(),
            "Reliable delivery waiting for processed completion for {message_id}"
        );
        self.spawn_local(async move |mut async_self| {
            debug!(
                async_self.log(),
                "Reliable delivery spawned processed wait task for {message_id}"
            );
            async_self
                .await_processed_delivery(message_id, processed_future)
                .await;
            Handled::OK
        });
        Handled::OK
    }

    /// Return whether an inbound envelope matched existing receiver-side state
    /// and was handled without re-delivering it to the semantic owner.
    fn handle_inbound_envelope_if_already_tracked(&mut self, message_id: MessageId) -> bool {
        let existing_state = self
            .inbound_deliveries
            .get(&message_id)
            .map(|pending| pending.state);
        let Some(state) = existing_state else {
            return false;
        };

        match state {
            PendingInboundDeliveryState::AwaitingProcessed => {
                debug!(
                    self.log(),
                    "Reliable delivery dropped duplicate inbound envelope message_id={message_id} while awaiting processed completion"
                );
            }
            PendingInboundDeliveryState::AckPending => {
                debug!(
                    self.log(),
                    "Reliable delivery observed duplicate inbound envelope message_id={message_id} while recipient ack is pending; retrying ack dispatch"
                );
                self.try_dispatch_inbound_ack(message_id);
            }
            PendingInboundDeliveryState::AckInFlight => {
                debug!(
                    self.log(),
                    "Reliable delivery dropped duplicate inbound envelope message_id={message_id} while recipient ack is already in flight"
                );
            }
        }
        true
    }

    fn handle_inbound_recipient_ack(
        &mut self,
        ack: delivery_proto::RecipientAckWire,
    ) -> HandlerResult {
        let ack = recipient_ack_from_wire(ack).whatever_benign(
            "Reliable delivery dropped inbound recipient ack that failed to decode",
        )?;
        let message_id = ack.header.message_id;
        if !self.sender_work_items.contains_key(&message_id) {
            debug!(
                self.log(),
                "Reliable delivery ignored recipient ack for unknown message_id={}", message_id
            );
            return Handled::OK;
        }
        self.cancel_retry(RetryKey::Sender(message_id));
        debug!(
            self.log(),
            "Reliable delivery observed recipient ack for message_id={} from recipient={} original_sender={}",
            message_id,
            ack.header.recipient,
            ack.header.original_sender
        );
        let work_item = self
            .sender_work_items
            .get_mut(&message_id)
            .expect("sender work item was checked above");
        work_item.recipient_ack = RecipientAckStatus::Observed { ack };
        Handled::OK
    }

    #[cfg(test)]
    pub(crate) fn knows_direct_route(&self, peer: &MemberIdentity) -> bool {
        self.direct_peer_routes.get(peer).is_some()
    }

    async fn await_processed_delivery(
        &mut self,
        message_id: MessageId,
        processed_future: KFuture<()>,
    ) {
        match processed_future.await {
            Ok(()) => {
                debug!(
                    self.log(),
                    "Reliable delivery observed processed completion for {message_id}"
                );
                self.finish_processed_delivery(message_id).await;
            }
            Err(_error) => {
                // If every clone of the processed handle disappears before one
                // of them completes it, this receiver instance forgets the
                // accepted inbound copy and withholds the semantic recipient
                // ack. That is intentional: the original sender
                // still observes no ack, so it retains the outbound work item
                // and will eventually retry delivery of the same message id.
                // A later redelivery can therefore re-surface the item to the
                // application instead of falsely acknowledging unprocessed
                // work.
                warn!(
                    self.log(),
                    "Reliable delivery dropped processed wait for message_id={message_id} because the completion handle was dropped"
                );
                self.inbound_deliveries.remove(&message_id);
            }
        }
    }

    async fn finish_processed_delivery(&mut self, message_id: MessageId) {
        let Some((recipient, original_sender)) =
            self.inbound_deliveries.get(&message_id).map(|pending| {
                (
                    pending.envelope.header.recipient.clone(),
                    pending.envelope.header.sender.clone(),
                )
            })
        else {
            warn!(
                self.log(),
                "Reliable delivery lost inbound state before it could finish processed handling for {message_id}"
            );
            return;
        };

        let ack = RecipientAck {
            header: RecipientAckHeader {
                message_id,
                original_sender: original_sender.clone(),
                recipient,
            },
            footer: placeholder_signed_footer(),
        };

        let pending = self
            .inbound_deliveries
            .get_mut(&message_id)
            .expect("must be able to access the same inbound again");
        pending.ack = Some(ack.clone());
        pending.state = PendingInboundDeliveryState::AckPending;

        if let Some(route) = self.direct_peer_routes.get(&original_sender).cloned() {
            self.dispatch_recipient_ack(message_id, ack, route).await;
        } else {
            warn!(
                self.log(),
                "Reliable delivery processed message_id={message_id} but has no direct route back to original sender={original_sender} for recipient ack"
            );
            self.schedule_retry(RetryKey::InboundAck(message_id), self.retry_delay);
        }
    }

    async fn dispatch_recipient_ack(
        &mut self,
        message_id: MessageId,
        ack: RecipientAck,
        route: SendRouteCandidate<TransportRouteKey>,
    ) {
        let boundary = ack.to_wire_format();

        self.cancel_retry(RetryKey::InboundAck(message_id));
        if let Some(pending) = self.inbound_deliveries.get_mut(&message_id) {
            pending.state = PendingInboundDeliveryState::AckInFlight;
        }

        let payload: Arc<dyn FlotsyncSerializable> = Arc::new(boundary);
        let send = RouteTransportSend {
            send_id: RouteSendId(Uuid::new_v4()),
            route,
            payload,
        };
        let future = self
            .route_transport
            .ask_with(|promise| RouteTransportActorMessage::Submit(Ask::new(promise, send)));
        self.finish_recipient_ack_dispatch(message_id, future).await;
    }

    async fn finish_recipient_ack_dispatch(
        &mut self,
        message_id: MessageId,
        future: KFuture<RouteTransportSubmitResult<TransportRouteKey>>,
    ) {
        match future.await {
            Ok(RouteTransportSubmitResult::Sent { .. }) => {
                self.inbound_deliveries.remove(&message_id);
            }
            Ok(RouteTransportSubmitResult::SendFailed { reason, .. }) => {
                if let Some(pending) = self.inbound_deliveries.get_mut(&message_id) {
                    pending.state = PendingInboundDeliveryState::AckPending;
                }
                self.schedule_retry(RetryKey::InboundAck(message_id), self.retry_delay);
                warn!(
                    self.log(),
                    "Reliable delivery recipient ack transport send failed for {message_id}: {reason:?}"
                );
            }
            Err(_error) => {
                if let Some(pending) = self.inbound_deliveries.get_mut(&message_id) {
                    pending.state = PendingInboundDeliveryState::AckPending;
                }
                self.schedule_retry(RetryKey::InboundAck(message_id), self.retry_delay);
                warn!(
                    self.log(),
                    "Reliable delivery recipient ack transport promise dropped for message_id={message_id}"
                );
            }
        }
    }

    async fn finish_outbound_envelope_submit(
        &mut self,
        message_id: MessageId,
        future: KFuture<RouteTransportSubmitResult<TransportRouteKey>>,
    ) {
        match future.await {
            Ok(RouteTransportSubmitResult::Sent { .. }) => {
                self.cancel_retry(RetryKey::Sender(message_id));
                let mut should_schedule_ack_timeout = false;
                if let Some(work_item) = self.sender_work_items.get_mut(&message_id) {
                    work_item.recipient_route.state = RouteActiveState::AwaitingRecipientAck;
                    if matches!(work_item.recipient_ack, RecipientAckStatus::Pending) {
                        should_schedule_ack_timeout = true;
                    } else {
                        debug!(
                            self.log(),
                            "Reliable delivery skipped recipient ack timeout for message_id={message_id} because ack was already observed"
                        );
                    }
                }
                if should_schedule_ack_timeout {
                    self.schedule_retry(RetryKey::Sender(message_id), self.recipient_ack_timeout);
                }
            }
            Ok(RouteTransportSubmitResult::SendFailed { reason, .. }) => {
                self.mark_sender_work_pending_retry(message_id, sender_retry_reason(&reason));
                self.schedule_retry(RetryKey::Sender(message_id), self.retry_delay);
                warn!(
                    self.log(),
                    "Reliable delivery outbound envelope send failed for {message_id}: {reason:?}"
                );
            }
            Err(_error) => {
                self.mark_sender_work_pending_retry(
                    message_id,
                    PendingRouteReason::LocalResourcePressure,
                );
                self.schedule_retry(RetryKey::Sender(message_id), self.retry_delay);
                warn!(
                    self.log(),
                    "Reliable delivery outbound envelope promise dropped for {message_id}"
                );
            }
        }
    }

    fn try_dispatch_sender_message(&mut self, message_id: MessageId) {
        let Some(recipient) = self
            .sender_work_items
            .get(&message_id)
            .and_then(|work_item| {
                if matches!(
                    work_item.recipient_route.state,
                    RouteActiveState::AttemptingDirect { .. }
                        | RouteActiveState::AwaitingRecipientAck
                ) {
                    return None;
                }
                Some(work_item.submit.envelope.header.recipient.clone())
            })
        else {
            return;
        };
        let Some(route) = self.direct_peer_routes.get(&recipient).cloned() else {
            self.mark_sender_work_pending_retry(
                message_id,
                PendingRouteReason::PeerCurrentlyUnreachable,
            );
            return;
        };

        let boundary = match self.sender_work_items.get(&message_id) {
            Some(work_item) => work_item.submit.envelope.to_wire_format(),
            None => {
                return;
            }
        };

        self.cancel_retry(RetryKey::Sender(message_id));
        let send_id = RouteSendId(Uuid::new_v4());
        if let Some(work_item) = self.sender_work_items.get_mut(&message_id) {
            work_item.recipient_route.state = RouteActiveState::AttemptingDirect { send_id };
        }

        let route_transport = self.route_transport.clone();
        self.spawn_local(async move |mut async_self| {
            let payload: Arc<dyn FlotsyncSerializable> = Arc::new(boundary);
            let send = RouteTransportSend {
                send_id,
                route,
                payload,
            };
            let future = route_transport
                .ask_with(|promise| RouteTransportActorMessage::Submit(Ask::new(promise, send)));
            async_self
                .finish_outbound_envelope_submit(message_id, future)
                .await;
            Handled::OK
        });
    }

    fn retry_pending_sender_work_for_peer(&mut self, peer: &MemberIdentity) {
        let message_ids: Vec<_> = self
            .sender_work_items
            .iter()
            .filter(|(_, work_item)| {
                work_item.submit.envelope.header.recipient == *peer
                    && matches!(work_item.recipient_ack, RecipientAckStatus::Pending)
                    && matches!(
                        work_item.recipient_route.state,
                        RouteActiveState::PendingRoute { .. }
                    )
            })
            .map(|(message_id, _)| *message_id)
            .collect();
        for message_id in message_ids {
            self.try_dispatch_sender_message(message_id);
        }
    }

    fn retry_pending_inbound_acks_for_peer(&mut self, peer: &MemberIdentity) {
        let message_ids: Vec<_> = self
            .inbound_deliveries
            .iter()
            .filter(|(_, pending)| {
                pending.envelope.header.sender == *peer
                    && matches!(pending.state, PendingInboundDeliveryState::AckPending)
                    && pending.ack.is_some()
            })
            .map(|(message_id, _)| *message_id)
            .collect();
        for message_id in message_ids {
            self.try_dispatch_inbound_ack(message_id);
        }
    }

    fn try_dispatch_inbound_ack(&mut self, message_id: MessageId) {
        let Some((ack, original_sender)) =
            self.inbound_deliveries
                .get(&message_id)
                .and_then(|pending| {
                    if pending.state == PendingInboundDeliveryState::AckInFlight {
                        return None;
                    }
                    pending
                        .ack
                        .clone()
                        .map(|ack| (ack, pending.envelope.header.sender.clone()))
                })
        else {
            return;
        };
        let Some(route) = self.direct_peer_routes.get(&original_sender).cloned() else {
            self.schedule_retry(RetryKey::InboundAck(message_id), self.retry_delay);
            return;
        };
        self.spawn_local(move |mut async_self| async move {
            async_self
                .dispatch_recipient_ack(message_id, ack, route)
                .await;
            Handled::OK
        });
    }

    fn mark_sender_work_pending_retry(
        &mut self,
        message_id: MessageId,
        reason: PendingRouteReason,
    ) {
        if let Some(work_item) = self.sender_work_items.get_mut(&message_id) {
            work_item.recipient_route.state = RouteActiveState::PendingRoute {
                retry_after: None,
                reason,
            };
        }
    }

    fn schedule_retry(&mut self, key: RetryKey, delay: Duration) {
        let now = self.now();
        self.retry_queue.schedule(key, now + delay);
        self.set_retry_timer(now);
    }

    fn cancel_retry(&mut self, key: RetryKey) {
        self.retry_queue.cancel(key);
        self.set_retry_timer(self.now());
    }

    fn set_retry_timer(&mut self, now: Instant) {
        self.retry_queue.remove_stale_entries();
        if let Some(timer) = self.retry_timer.take() {
            self.cancel_timer(timer);
        }
        let Some(next_due_at) = self.retry_queue.next_due_at() else {
            return;
        };
        let delay = next_due_at.saturating_duration_since(now);
        let timer = self.schedule_once(delay, move |component, expected_timer| {
            component.handle_retry_timeout(&expected_timer)
        });
        self.retry_timer = Some(timer);
    }

    fn handle_retry_timeout(&mut self, expected_timer: &ScheduledTimer) -> HandlerResult {
        let Some(active_timer) = self.retry_timer.take() else {
            return Handled::OK;
        };
        if &active_timer != expected_timer {
            self.retry_timer = Some(active_timer);
            return Handled::OK;
        }
        let now = self.now();
        let ready = self.retry_queue.take_ready(now);
        for key in ready {
            match key {
                RetryKey::Sender(message_id) => self.handle_sender_retry_timeout(message_id),
                RetryKey::InboundAck(message_id) => self.try_dispatch_inbound_ack(message_id),
            }
        }
        self.set_retry_timer(now);
        Handled::OK
    }

    fn handle_sender_retry_timeout(&mut self, message_id: MessageId) {
        let Some(work_item) = self.sender_work_items.get(&message_id) else {
            debug!(
                self.log(),
                "Reliable delivery ignored stale sender retry for unknown message_id={message_id}"
            );
            return;
        };
        if matches!(work_item.recipient_ack, RecipientAckStatus::Observed { .. }) {
            debug!(
                self.log(),
                "Reliable delivery ignored sender retry for message_id={message_id} because recipient ack was already observed"
            );
            return;
        }

        match &work_item.recipient_route.state {
            RouteActiveState::AwaitingRecipientAck => {
                let sender = work_item.submit.envelope.header.sender.clone();
                let recipient = work_item.submit.envelope.header.recipient.clone();
                warn!(
                    self.log(),
                    "Reliable delivery recipient ack timed out for message_id={} sender={} recipient={} after {:?}; retrying envelope delivery",
                    message_id,
                    sender,
                    recipient,
                    self.recipient_ack_timeout
                );
                self.mark_sender_work_pending_retry(
                    message_id,
                    PendingRouteReason::BackoffInEffect,
                );
                self.try_dispatch_sender_message(message_id);
            }
            RouteActiveState::PendingRoute { .. } | RouteActiveState::Queued => {
                self.try_dispatch_sender_message(message_id);
            }
            RouteActiveState::AttemptingDirect { send_id } => {
                debug!(
                    self.log(),
                    "Reliable delivery ignored sender retry for message_id={} because direct send {} is still in flight",
                    message_id,
                    format_args!("{send_id:?}")
                );
            }
            RouteActiveState::AwaitingRelayStore { send_id } => {
                debug!(
                    self.log(),
                    "Reliable delivery ignored sender retry for message_id={} because relay store {} is still in flight",
                    message_id,
                    format_args!("{send_id:?}")
                );
            }
        }
    }

    #[cfg(test)]
    fn sender_work_item(&self, message_id: MessageId) -> Option<&ReliableDeliveryWorkItem> {
        self.sender_work_items.get(&message_id)
    }

    #[cfg(test)]
    fn inbound_delivery_state(&self, message_id: MessageId) -> Option<PendingInboundDeliveryState> {
        self.inbound_deliveries
            .get(&message_id)
            .map(|pending| pending.state)
    }
}

impl ComponentLifecycle for ReliableDeliveryComponent {
    fn on_start(&mut self) -> HandlerResult {
        self.retry_delay = self.load_retry_delay();
        self.recipient_ack_timeout = self.load_recipient_ack_timeout();
        Handled::OK
    }

    fn on_stop(&mut self) -> HandlerResult {
        if let Some(timer) = self.retry_timer.take() {
            self.cancel_timer(timer);
        }
        Handled::OK
    }

    fn on_kill(&mut self) -> HandlerResult {
        self.on_stop()
    }
}

impl Provide<ReliableDeliveryPort> for ReliableDeliveryComponent {
    fn handle(&mut self, request: ReliableDeliveryPortRequest) -> HandlerResult {
        match request {
            ReliableDeliveryPortRequest::Submit(submit) => self.handle_submit_request(submit),
        }
    }
}

impl Require<TransportReliableDeliveryInboundPort> for ReliableDeliveryComponent {
    fn handle(
        &mut self,
        indication: ReliableDeliveryInboundDeliver<TransportRouteKey>,
    ) -> HandlerResult {
        self.handle_ingress_indication(indication)
    }
}

impl Require<TransportRouteDiscoveryPort> for ReliableDeliveryComponent {
    fn handle(&mut self, indication: TransportDiscoveryRouteUpdate) -> HandlerResult {
        self.handle_discovery_update(indication)
    }
}

impl Actor for ReliableDeliveryComponent {
    type Message = Never;

    fn receive_local(&mut self, _msg: Self::Message) -> HandlerResult {
        unreachable!("Message type cannot be instantiated");
    }
}

type TransportReliableDeliveryInboundPort = ReliableDeliveryInboundPort<TransportRouteKey>;
type TransportRouteDiscoveryPort = RouteDiscoveryPort<TransportRouteKey>;
type TransportDiscoveryRouteUpdate = super::contracts::DiscoveryRouteUpdate<TransportRouteKey>;

mod config_keys {
    use super::{DEFAULT_RECIPIENT_ACK_TIMEOUT, Duration, kompact_config};
    use kompact::config::DurationValue;

    kompact_config! {
        RETRY_DELAY,
        key = "flotsync.reliable-delivery.retry-delay",
        type = DurationValue,
        default = Duration::from_secs(30),
        doc = "Base retry delay for direct reliable-delivery sends and semantic recipient acknowledgements.",
        version = "0.1.0"
    }

    kompact_config! {
        RECIPIENT_ACK_TIMEOUT,
        key = "flotsync.reliable-delivery.recipient-ack-timeout",
        type = DurationValue,
        default = DEFAULT_RECIPIENT_ACK_TIMEOUT,
        doc = "Maximum wait for a semantic recipient acknowledgement after a reliable-delivery envelope send succeeds.",
        version = "0.1.0"
    }
}

const DEFAULT_RETRY_DELAY: Duration = Duration::from_secs(30);
const DEFAULT_RECIPIENT_ACK_TIMEOUT: Duration = Duration::from_mins(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Receiver-side state for one inbound reliable-delivery envelope after it was
/// accepted locally by the component.
enum PendingInboundDeliveryState {
    /// The message was delivered upward and reliable delivery is waiting for
    /// the application to confirm semantic processing.
    AwaitingProcessed,
    /// Semantic processing completed, the recipient ack exists, and the
    /// component may retry dispatch once a route or transport attempt succeeds.
    AckPending,
    /// One recipient ack transport send is currently in flight.
    AckInFlight,
}

struct PendingInboundDelivery {
    envelope: ReliableMessageEnvelope,
    state: PendingInboundDeliveryState,
    /// Cached semantic recipient ack reused for later retries after transient
    /// route-transport or discovery failures.
    ack: Option<RecipientAck>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum RetryKey {
    Sender(MessageId),
    InboundAck(MessageId),
}

/// Monotonic retry scheduler shared across all sender and recipient-ack retries.
///
/// This keeps per-message due times but only arms one Kompact timer for the
/// earliest known retry, which scales better than holding one timer per
/// outstanding reliable-delivery work item.
#[derive(Debug)]
struct RetryQueue {
    due_by_key: HashMap<RetryKey, Instant>,
    due_heap: BinaryHeap<Reverse<(Instant, RetryKey)>>,
}

impl RetryQueue {
    fn new() -> Self {
        Self {
            due_by_key: HashMap::new(),
            due_heap: BinaryHeap::new(),
        }
    }

    fn schedule(&mut self, key: RetryKey, due_at: Instant) {
        self.due_by_key.insert(key, due_at);
        self.due_heap.push(Reverse((due_at, key)));
    }

    fn cancel(&mut self, key: RetryKey) {
        self.due_by_key.remove(&key);
    }

    fn remove_stale_entries(&mut self) {
        while let Some(Reverse((due_at, key))) = self.due_heap.peek().copied() {
            let Some(current_due_at) = self.due_by_key.get(&key).copied() else {
                self.due_heap.pop();
                continue;
            };
            if current_due_at != due_at {
                self.due_heap.pop();
                continue;
            }
            break;
        }
    }

    fn next_due_at(&mut self) -> Option<Instant> {
        while let Some(Reverse((due_at, key))) = self.due_heap.peek().copied() {
            match self.due_by_key.get(&key).copied() {
                Some(current_due_at) if current_due_at == due_at => return Some(due_at),
                _ => {
                    self.due_heap.pop();
                }
            }
        }
        None
    }

    fn take_ready(&mut self, now: Instant) -> Vec<RetryKey> {
        let mut ready = Vec::new();
        while let Some(Reverse((due_at, key))) = self.due_heap.peek().copied() {
            let Some(current_due_at) = self.due_by_key.get(&key).copied() else {
                self.due_heap.pop();
                continue;
            };
            if current_due_at != due_at {
                self.due_heap.pop();
                continue;
            }
            if due_at > now {
                break;
            }
            self.due_heap.pop();
            self.due_by_key.remove(&key);
            ready.push(key);
        }
        ready
    }
}

fn select_best_direct_route(
    routes: Vec<SendRouteCandidate<TransportRouteKey>>,
) -> Option<SendRouteCandidate<TransportRouteKey>> {
    routes
        .into_iter()
        .filter(|route| route.sharing == RouteSharingKind::Exclusive)
        .max_by_key(|route| route.preference_rank)
}

fn placeholder_signed_footer() -> SignedEnvelopeFooter {
    SignedEnvelopeFooter {
        // TODO(flotsync-d8d): Replace this placeholder once the real
        // reliable-delivery signing boundary is available.
        signature: DetachedSignature {
            scheme: SignatureScheme::Ed25519,
            bytes: Bytes::from_static(b"placeholder-signature"),
        },
    }
}

fn sender_retry_reason(reason: &RouteTransportNackReason) -> PendingRouteReason {
    match reason {
        RouteTransportNackReason::RouteUnknown | RouteTransportNackReason::RouteUnavailable => {
            PendingRouteReason::PeerCurrentlyUnreachable
        }
        RouteTransportNackReason::Backpressure
        | RouteTransportNackReason::LocalResourcePressure => {
            PendingRouteReason::LocalResourcePressure
        }
        RouteTransportNackReason::InvalidPayload | RouteTransportNackReason::Other(_) => {
            PendingRouteReason::BackoffInEffect
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        GroupMemberships,
        SharedGroupMemberships,
        delivery::{
            ingress::{DeliveryIngressComponent, DeliveryInterestConfig},
            route_transport::{
                DatagramRouteScope,
                RoutePreferenceRank,
                RouteTransportPort,
                UdpRouteKey,
            },
            test_support::{
                DiscoveryRouteSource,
                FULL_STACK_WAIT_TIMEOUT,
                TransportHarnessCore,
                build_delivery_test_system,
                build_delivery_test_system_with,
                default_udpour_config,
                member_identity,
            },
        },
    };
    use flotsync_io::{
        prelude::UdpLocalBind,
        test_support::{WAIT_TIMEOUT, eventually_component_state, localhost, start_component},
    };
    use std::{collections::HashSet, net::SocketAddr, sync::mpsc, time::Duration};

    const TEST_RECIPIENT_ACK_TIMEOUT: Duration = Duration::from_millis(50);

    #[derive(ComponentDefinition)]
    struct ReliableDeliveryClientProbe {
        ctx: ComponentContext<Self>,
        delivery: RequiredPort<ReliableDeliveryPort>,
        indications: mpsc::Sender<ReliableDeliveryPortIndication>,
    }

    impl ReliableDeliveryClientProbe {
        fn new(indications: mpsc::Sender<ReliableDeliveryPortIndication>) -> Self {
            Self {
                ctx: ComponentContext::uninitialised(),
                delivery: RequiredPort::uninitialised(),
                indications,
            }
        }
    }

    ignore_lifecycle!(ReliableDeliveryClientProbe);

    impl Require<ReliableDeliveryPort> for ReliableDeliveryClientProbe {
        fn handle(&mut self, indication: ReliableDeliveryPortIndication) -> HandlerResult {
            self.indications
                .send(indication)
                .expect("reliable delivery indication receiver must stay live");
            Handled::OK
        }
    }

    impl Actor for ReliableDeliveryClientProbe {
        type Message = Never;

        fn receive_local(&mut self, _msg: Self::Message) -> HandlerResult {
            unreachable!("Never type is empty")
        }
    }

    struct FullStackHarness {
        core: TransportHarnessCore,
        ingress: Arc<Component<DeliveryIngressComponent>>,
        reliable: Arc<Component<ReliableDeliveryComponent>>,
        discovery_source: Arc<Component<DiscoveryRouteSource>>,
        client: Arc<Component<ReliableDeliveryClientProbe>>,
        client_rx: mpsc::Receiver<ReliableDeliveryPortIndication>,
        local_addr: SocketAddr,
    }

    impl FullStackHarness {
        fn new(local_member: MemberIdentity) -> Self {
            Self::with_system(local_member, build_delivery_test_system())
        }

        fn with_recipient_ack_timeout(
            local_member: MemberIdentity,
            recipient_ack_timeout: Duration,
        ) -> Self {
            let system = build_delivery_test_system_with(|config| {
                config.set_config_value(&config_keys::RECIPIENT_ACK_TIMEOUT, recipient_ack_timeout);
            });
            Self::with_system(local_member, system)
        }

        fn with_system(local_member: MemberIdentity, system: KompactSystem) -> Self {
            let core = TransportHarnessCore::with_socket_budgets(
                system,
                default_udpour_config(),
                true,
                &[],
                0,
            );
            let manager_ref = core.manager_ref();
            let local_members: Arc<HashSet<MemberIdentity>> =
                Arc::new([local_member].into_iter().collect());
            let ingress = core.system().create(move || {
                DeliveryIngressComponent::new(DeliveryInterestConfig {
                    group_memberships: SharedGroupMemberships::new(GroupMemberships::new()),
                    local_members,
                    hosted_mailboxes: Arc::new(HashSet::new()),
                })
            });
            let reliable = core
                .system()
                .create(move || ReliableDeliveryComponent::new(manager_ref.clone()));
            let discovery_source = core.system().create(DiscoveryRouteSource::new);
            let (client_tx, client_rx) = mpsc::channel();
            let client = core
                .system()
                .create(move || ReliableDeliveryClientProbe::new(client_tx));

            biconnect_components::<RouteTransportPort<TransportRouteKey>, _, _>(
                core.manager(),
                &ingress,
            )
            .expect("route transport manager must connect to delivery ingress");
            biconnect_components::<TransportReliableDeliveryInboundPort, _, _>(&ingress, &reliable)
                .expect("delivery ingress must connect to reliable delivery");
            biconnect_components::<TransportRouteDiscoveryPort, _, _>(&discovery_source, &reliable)
                .expect("discovery source must connect to reliable delivery");
            biconnect_components::<ReliableDeliveryPort, _, _>(&reliable, &client)
                .expect("reliable delivery must connect to the external client probe");

            core.start();
            start_component(core.system(), &ingress);
            start_component(core.system(), &reliable);
            start_component(core.system(), &discovery_source);
            start_component(core.system(), &client);

            let (socket_id, local_addr) = core
                .bind_external_socket(UdpLocalBind::Exact(localhost(0)), FULL_STACK_WAIT_TIMEOUT);
            core.wait_for_manager_external_socket_binding(
                socket_id,
                local_addr,
                FULL_STACK_WAIT_TIMEOUT,
            );

            Self {
                core,
                ingress,
                reliable,
                discovery_source,
                client,
                client_rx,
                local_addr,
            }
        }

        fn publish_direct_route(&self, peer: MemberIdentity, remote_addr: SocketAddr) {
            let route = SendRouteCandidate {
                coverage_key: TransportRouteKey::Udp(UdpRouteKey {
                    remote_addr,
                    scope: DatagramRouteScope::Unicast,
                    local_bind: Some(self.local_addr),
                }),
                sharing: RouteSharingKind::Exclusive,
                preference_rank: RoutePreferenceRank::new(1),
            };
            let expected_peer = peer.clone();
            self.discovery_source.on_definition(|component| {
                component
                    .discovery
                    .trigger(TransportDiscoveryRouteUpdate::PeerRoutes {
                        peer,
                        classification: super::super::shared::ReachabilityClass::Reachable,
                        routes: vec![route],
                    });
            });
            eventually_component_state(
                WAIT_TIMEOUT,
                &self.reliable,
                |component| component.knows_direct_route(&expected_peer),
                "timed out waiting for reliable-delivery route publication",
            );
        }

        fn submit(&self, submit: ReliableDeliverySubmit) {
            self.client.on_definition(|component| {
                component
                    .delivery
                    .trigger(ReliableDeliveryPortRequest::Submit(submit));
            });
        }

        fn wait_for_delivery(&self) -> ReliableDeliveryDeliver {
            match self
                .client_rx
                .recv_timeout(WAIT_TIMEOUT)
                .expect("timed out waiting for reliable delivery indication")
            {
                ReliableDeliveryPortIndication::Deliver(deliver) => deliver,
            }
        }

        fn expect_no_delivery(&self, timeout: Duration) {
            match self.client_rx.recv_timeout(timeout) {
                Ok(ReliableDeliveryPortIndication::Deliver(deliver)) => panic!(
                    "unexpected reliable delivery indication for message_id={}",
                    deliver.envelope.header.message_id
                ),
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    panic!("reliable delivery indication sender disconnected")
                }
            }
        }

        fn inject_recipient_ack(&self, ack: &RecipientAck) {
            self.reliable.on_definition(|component| {
                let frame = ack.to_wire_format();
                let Some(delivery_proto::delivery_boundary_frame::Boundary::ReliableDelivery(
                    frame,
                )) = frame.boundary
                else {
                    panic!("recipient ack must encode as reliable delivery boundary");
                };
                let Some(delivery_proto::reliable_delivery_frame::Body::RecipientAck(ack)) =
                    frame.body
                else {
                    panic!("recipient ack must encode as recipient ack body");
                };
                let _ = component.handle_inbound_recipient_ack(*ack);
            });
        }

        fn wait_for_sender_ack_observed(&self, message_id: MessageId) {
            eventually_component_state(
                WAIT_TIMEOUT,
                &self.reliable,
                |component| {
                    component
                        .sender_work_item(message_id)
                        .is_some_and(|work_item| {
                            matches!(work_item.recipient_ack, RecipientAckStatus::Observed { .. })
                        })
                },
                format_args!(
                    "timed out waiting for sender-side recipient ack observation for {message_id:?}"
                ),
            );
        }

        fn wait_for_sender_route_state(&self, message_id: MessageId, expected: &RouteActiveState) {
            eventually_component_state(
                WAIT_TIMEOUT,
                &self.reliable,
                |component| {
                    component
                        .sender_work_item(message_id)
                        .is_some_and(|work_item| &work_item.recipient_route.state == expected)
                },
                format_args!(
                    "timed out waiting for sender-side route state {expected:?} for {message_id:?}"
                ),
            );
        }

        fn wait_for_inbound_state(
            &self,
            message_id: MessageId,
            expected: PendingInboundDeliveryState,
        ) {
            eventually_component_state(
                WAIT_TIMEOUT,
                &self.reliable,
                |component| component.inbound_delivery_state(message_id) == Some(expected),
                format_args!("timed out waiting for inbound state {expected:?} for {message_id:?}"),
            );
        }

        fn wait_for_inbound_clear(&self, message_id: MessageId) {
            eventually_component_state(
                WAIT_TIMEOUT,
                &self.reliable,
                |component| component.inbound_delivery_state(message_id).is_none(),
                format_args!("timed out waiting for inbound delivery cleanup for {message_id:?}"),
            );
        }

        fn wait_for_sender_ciphertext(&self, message_id: MessageId, expected: &Bytes) {
            eventually_component_state(
                WAIT_TIMEOUT,
                &self.reliable,
                |component| {
                    component
                        .sender_work_item(message_id)
                        .is_some_and(|work_item| {
                            work_item.submit.envelope.payload.ciphertext.as_ref()
                                == expected.as_ref()
                        })
                },
                format_args!(
                    "timed out waiting for sender-side ciphertext {expected:?} for {message_id}"
                ),
            );
        }
    }

    impl Drop for FullStackHarness {
        fn drop(&mut self) {
            let _ = self
                .core
                .system()
                .kill_notify(self.client.clone())
                .wait_timeout(WAIT_TIMEOUT);
            let _ = self
                .core
                .system()
                .kill_notify(self.discovery_source.clone())
                .wait_timeout(WAIT_TIMEOUT);
            let _ = self
                .core
                .system()
                .kill_notify(self.reliable.clone())
                .wait_timeout(WAIT_TIMEOUT);
            let _ = self
                .core
                .system()
                .kill_notify(self.ingress.clone())
                .wait_timeout(WAIT_TIMEOUT);
        }
    }

    fn reliable_submit(
        sender: MemberIdentity,
        recipient: MemberIdentity,
        message_id: MessageId,
        payload: &'static [u8],
    ) -> ReliableDeliverySubmit {
        ReliableDeliverySubmit {
            envelope: ReliableMessageEnvelope {
                header: ReliableMessageHeader {
                    sender,
                    recipient,
                    message_id,
                },
                payload: EncryptedPayload {
                    ciphertext: Bytes::from_static(payload),
                },
                footer: placeholder_signed_footer(),
            },
        }
    }

    fn recipient_ack(
        original_sender: MemberIdentity,
        recipient: MemberIdentity,
        message_id: MessageId,
    ) -> RecipientAck {
        RecipientAck {
            header: RecipientAckHeader {
                message_id,
                original_sender,
                recipient,
            },
            footer: placeholder_signed_footer(),
        }
    }

    #[test]
    fn reliable_delivery_round_trips_direct_envelope_and_processed_ack() {
        let alice = member_identity(&["alice"]);
        let bob = member_identity(&["bob"]);
        let sender = FullStackHarness::new(alice.clone());
        let receiver = FullStackHarness::new(bob.clone());

        sender.publish_direct_route(bob.clone(), receiver.local_addr);
        receiver.publish_direct_route(alice.clone(), sender.local_addr);

        let message_id = MessageId(Uuid::from_u128(1));
        sender.submit(ReliableDeliverySubmit {
            envelope: ReliableMessageEnvelope {
                header: ReliableMessageHeader {
                    sender: alice,
                    recipient: bob,
                    message_id,
                },
                payload: EncryptedPayload {
                    ciphertext: Bytes::from_static(b"bootstrap payload"),
                },
                footer: placeholder_signed_footer(),
            },
        });

        let deliver = receiver.wait_for_delivery();
        assert_eq!(deliver.envelope.header.message_id, message_id);
        sender.wait_for_sender_route_state(message_id, &RouteActiveState::AwaitingRecipientAck);
        receiver.wait_for_inbound_state(message_id, PendingInboundDeliveryState::AwaitingProcessed);

        deliver
            .processed
            .complete()
            .expect("processed completion should succeed exactly once");

        sender.wait_for_sender_ack_observed(message_id);
        receiver.wait_for_inbound_clear(message_id);
    }

    #[test]
    fn recipient_ack_timeout_redelivers_unprocessed_envelope() {
        let alice = member_identity(&["alice"]);
        let bob = member_identity(&["bob"]);
        let sender =
            FullStackHarness::with_recipient_ack_timeout(alice.clone(), TEST_RECIPIENT_ACK_TIMEOUT);
        let receiver = FullStackHarness::new(bob.clone());

        sender.publish_direct_route(bob.clone(), receiver.local_addr);

        let message_id = MessageId(Uuid::from_u128(41));
        sender.submit(reliable_submit(
            alice,
            bob,
            message_id,
            b"retry bootstrap payload",
        ));

        let deliver = receiver.wait_for_delivery();
        assert_eq!(deliver.envelope.header.message_id, message_id);
        sender.wait_for_sender_route_state(message_id, &RouteActiveState::AwaitingRecipientAck);
        drop(deliver);
        receiver.wait_for_inbound_clear(message_id);

        let redelivered = receiver.wait_for_delivery();
        assert_eq!(redelivered.envelope.header.message_id, message_id);
        assert_eq!(
            redelivered.envelope.payload.ciphertext,
            Bytes::from_static(b"retry bootstrap payload")
        );
    }

    #[test]
    fn recipient_ack_cancels_timeout_redelivery() {
        let alice = member_identity(&["alice"]);
        let bob = member_identity(&["bob"]);
        let sender =
            FullStackHarness::with_recipient_ack_timeout(alice.clone(), TEST_RECIPIENT_ACK_TIMEOUT);
        let receiver = FullStackHarness::new(bob.clone());

        sender.publish_direct_route(bob.clone(), receiver.local_addr);
        receiver.publish_direct_route(alice.clone(), sender.local_addr);

        let message_id = MessageId(Uuid::from_u128(42));
        sender.submit(reliable_submit(
            alice,
            bob,
            message_id,
            b"ack cancels timeout",
        ));

        let deliver = receiver.wait_for_delivery();
        deliver
            .processed
            .complete()
            .expect("processed completion should succeed exactly once");
        sender.wait_for_sender_route_state(message_id, &RouteActiveState::AwaitingRecipientAck);
        sender.wait_for_sender_ack_observed(message_id);

        receiver.expect_no_delivery(TEST_RECIPIENT_ACK_TIMEOUT * 2);
    }

    #[test]
    fn duplicate_inbound_envelope_is_dropped_while_awaiting_processed() {
        let alice = member_identity(&["alice"]);
        let bob = member_identity(&["bob"]);
        let sender =
            FullStackHarness::with_recipient_ack_timeout(alice.clone(), TEST_RECIPIENT_ACK_TIMEOUT);
        let receiver = FullStackHarness::new(bob.clone());

        sender.publish_direct_route(bob.clone(), receiver.local_addr);

        let message_id = MessageId(Uuid::from_u128(43));
        sender.submit(reliable_submit(
            alice,
            bob,
            message_id,
            b"duplicate while processing",
        ));

        let deliver = receiver.wait_for_delivery();
        assert_eq!(deliver.envelope.header.message_id, message_id);
        sender.wait_for_sender_route_state(message_id, &RouteActiveState::AwaitingRecipientAck);
        receiver.wait_for_inbound_state(message_id, PendingInboundDeliveryState::AwaitingProcessed);

        receiver.expect_no_delivery(TEST_RECIPIENT_ACK_TIMEOUT * 2);
        deliver
            .processed
            .complete()
            .expect("processed completion should succeed exactly once");
    }

    #[test]
    fn duplicate_inbound_envelope_retries_pending_recipient_ack_without_redelivery() {
        let alice = member_identity(&["alice"]);
        let bob = member_identity(&["bob"]);
        let sender =
            FullStackHarness::with_recipient_ack_timeout(alice.clone(), TEST_RECIPIENT_ACK_TIMEOUT);
        let receiver = FullStackHarness::new(bob.clone());

        sender.publish_direct_route(bob.clone(), receiver.local_addr);

        let message_id = MessageId(Uuid::from_u128(44));
        sender.submit(reliable_submit(
            alice.clone(),
            bob.clone(),
            message_id,
            b"duplicate while ack pending",
        ));

        let deliver = receiver.wait_for_delivery();
        sender.wait_for_sender_route_state(message_id, &RouteActiveState::AwaitingRecipientAck);
        deliver
            .processed
            .complete()
            .expect("processed completion should succeed exactly once");
        receiver.wait_for_inbound_state(message_id, PendingInboundDeliveryState::AckPending);

        receiver.expect_no_delivery(TEST_RECIPIENT_ACK_TIMEOUT * 2);
        receiver.publish_direct_route(alice, sender.local_addr);
        sender.wait_for_sender_ack_observed(message_id);
        receiver.wait_for_inbound_clear(message_id);
    }

    #[test]
    fn late_recipient_ack_is_accepted_for_pending_sender_work() {
        let alice = member_identity(&["alice"]);
        let bob = member_identity(&["bob"]);
        let sender = FullStackHarness::new(alice.clone());

        let message_id = MessageId(Uuid::from_u128(45));
        sender.submit(reliable_submit(
            alice.clone(),
            bob.clone(),
            message_id,
            b"pending route late ack",
        ));
        sender.wait_for_sender_route_state(
            message_id,
            &RouteActiveState::PendingRoute {
                retry_after: None,
                reason: PendingRouteReason::PeerCurrentlyUnreachable,
            },
        );

        sender.inject_recipient_ack(&recipient_ack(alice, bob, message_id));
        sender.wait_for_sender_ack_observed(message_id);
    }

    #[test]
    fn retry_queue_keeps_overdue_entries_ready_after_timer_reset() {
        let mut queue = RetryQueue::new();
        let first = RetryKey::Sender(MessageId(Uuid::from_u128(1)));
        let second = RetryKey::InboundAck(MessageId(Uuid::from_u128(2)));
        let base = Instant::now();

        queue.schedule(first, base);
        queue.schedule(second, base + Duration::from_secs(30));
        queue.remove_stale_entries();

        let ready = queue.take_ready(base + Duration::from_millis(1));
        assert_eq!(ready, vec![first]);
        assert_eq!(queue.next_due_at(), Some(base + Duration::from_secs(30)));
    }

    #[test]
    fn duplicate_submit_keeps_the_original_work_item() {
        let alice = member_identity(&["alice"]);
        let bob = member_identity(&["bob"]);
        let sender = FullStackHarness::new(alice.clone());
        let message_id = MessageId(Uuid::from_u128(7));

        sender.submit(ReliableDeliverySubmit {
            envelope: ReliableMessageEnvelope {
                header: ReliableMessageHeader {
                    sender: alice.clone(),
                    recipient: bob.clone(),
                    message_id,
                },
                payload: EncryptedPayload {
                    ciphertext: Bytes::from_static(b"first payload"),
                },
                footer: placeholder_signed_footer(),
            },
        });
        sender.submit(ReliableDeliverySubmit {
            envelope: ReliableMessageEnvelope {
                header: ReliableMessageHeader {
                    sender: alice,
                    recipient: bob,
                    message_id,
                },
                payload: EncryptedPayload {
                    ciphertext: Bytes::from_static(b"second payload"),
                },
                footer: placeholder_signed_footer(),
            },
        });

        sender.wait_for_sender_ciphertext(message_id, &Bytes::from_static(b"first payload"));
    }
}
