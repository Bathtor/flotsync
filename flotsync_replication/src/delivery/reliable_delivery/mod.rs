//! Recipient-addressed reliable delivery types and the minimal direct-runtime slice.

mod wire;

use super::{
    contracts::{
        ReliableDeliveryPort,
        ReliableDeliveryPortIndication,
        ReliableDeliveryPortRequest,
    },
    ingress::InboundDeliveryMeta,
    security::{DeliverySecurity, DeliverySecurityError},
    shared::{
        ActiveRouteRecord,
        DetachedSignature,
        LogicalRouteId,
        MailboxItemId,
        MessageId,
        PendingRouteReason,
        PlaintextPayload,
        ReliableMessageScope,
        RouteActiveState,
        SignedEnvelopeFooter,
        StableRouteKey,
        WorkScopeKey,
    },
};
use bytes::Bytes;
use flotsync_core::{MemberIdentity, member::TrieMap};
use flotsync_messages::{
    delivery as delivery_proto,
    endpoint as endpoint_proto,
    serialisation::FlotsyncSerializable,
};
use flotsync_routes::{
    RelayIdentity,
    RouteDiscoveryPort,
    RouteSendId,
    RouteSharingKind,
    RouteTransportActorMessage,
    RouteTransportNackReason,
    RouteTransportSend,
    RouteTransportSubmitResult,
    SendRouteCandidate,
    TransportRouteKey,
};
use flotsync_security::SealedHPKEPayload;
use flotsync_utils::{
    KClaimablePromise,
    NonOwningPhantomData,
    OptionExt as _,
    ResultExt as _,
    kompact_config::ConfigReadExt as _,
};
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
    recipient_ack_public_header_bytes,
    recipient_ack_to_wire_format,
    reliable_envelope_from_wire,
    reliable_envelope_to_wire_format,
};

mod retry;
#[cfg(test)]
mod tests;
mod types;

pub use types::{
    EncryptedPayload,
    IdentityProof,
    MailboxAck,
    MailboxAckHandle,
    MailboxBatch,
    MailboxFetch,
    MailboxItem,
    RecipientAck,
    RecipientAckHeader,
    RecipientAckStatus,
    ReliableDeliveryDeliver,
    ReliableDeliveryInboundDeliver,
    ReliableDeliveryInboundPort,
    ReliableDeliverySubmit,
    ReliableDeliveryWorkItem,
    ReliableMessageEnvelope,
    ReliableMessageHeader,
};

use retry::{RetryKey, RetryQueue};

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
    security: DeliverySecurity,
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
    pub(crate) fn new(
        route_transport: ActorRefStrong<RouteTransportActorMessage<TransportRouteKey>>,
        security: DeliverySecurity,
    ) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            delivery_port: ProvidedPort::uninitialised(),
            ingress_inbound_port: RequiredPort::uninitialised(),
            discovery_port: RequiredPort::uninitialised(),
            route_transport,
            security,
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
        message_scope: ReliableMessageScope,
        state: RouteActiveState,
    ) -> ActiveRouteRecord {
        ActiveRouteRecord {
            key: StableRouteKey {
                scope: WorkScopeKey::Reliable {
                    recipient: recipient.clone(),
                    message_id,
                    message_scope,
                },
                route_id: LogicalRouteId::peer(recipient.clone()),
            },
            state,
        }
    }

    fn new_sender_work_item(submit: ReliableDeliverySubmit) -> ReliableDeliveryWorkItem {
        let recipient = submit.envelope.header.recipient.clone();
        let message_id = submit.envelope.header.message_id;
        let message_scope = submit.envelope.header.scope;
        ReliableDeliveryWorkItem {
            submit,
            recipient_route: Self::new_direct_route_record(
                &recipient,
                message_id,
                message_scope,
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
        self.ctx
            .config()
            .read_or_default_warn(self.log(), &config_keys::RETRY_DELAY)
    }

    fn load_recipient_ack_timeout(&self) -> Duration {
        self.ctx
            .config()
            .read_or_default_warn(self.log(), &config_keys::RECIPIENT_ACK_TIMEOUT)
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
        let encrypted_envelope = reliable_envelope_from_wire(envelope)
            .whatever_benign("Reliable delivery dropped inbound envelope that failed to decode")?;
        let message_id = encrypted_envelope.header.message_id;
        if self.handle_inbound_envelope_if_already_tracked(message_id) {
            return Handled::OK;
        }

        Handled::block_on(self, async move |mut async_self| {
            let plaintext = async_self
                .security
                .open_reliable_payload(
                    &encrypted_envelope.header,
                    &encrypted_envelope.payload.sealed,
                )
                .await
                .whatever_benign(
                    "Reliable delivery dropped inbound envelope that failed to open",
                )?;
            let envelope = ReliableMessageEnvelope::<PlaintextPayload> {
                header: encrypted_envelope.header,
                payload: PlaintextPayload {
                    bytes: Bytes::from(plaintext),
                },
            };
            let (processed, processed_future) = KClaimablePromise::create_pair();
            async_self.inbound_deliveries.insert(
                message_id,
                PendingInboundDelivery {
                    envelope: envelope.clone(),
                    state: PendingInboundDeliveryState::AwaitingProcessed,
                    ack: None,
                },
            );
            async_self
                .delivery_port
                .trigger(ReliableDeliveryPortIndication::Deliver(
                    ReliableDeliveryDeliver {
                        envelope,
                        processed,
                    },
                ));
            debug!(
                async_self.log(),
                "Reliable delivery waiting for processed completion for {message_id}"
            );
            debug!(
                async_self.log(),
                "Reliable delivery spawned processed wait task for {message_id}"
            );
            async_self.spawn_local(async move |mut async_self| {
                async_self
                    .await_processed_delivery(message_id, processed_future)
                    .await;
                Handled::OK
            });
            Handled::OK
        })
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
        let Some((expected_original_sender, expected_recipient)) =
            self.sender_work_items.get(&message_id).map(|work_item| {
                (
                    work_item.submit.envelope.header.sender.clone(),
                    work_item.submit.envelope.header.recipient.clone(),
                )
            })
        else {
            debug!(
                self.log(),
                "Reliable delivery ignored recipient ack for unknown message_id={}", message_id
            );
            return Handled::OK;
        };
        if ack.header.original_sender != expected_original_sender {
            warn!(
                self.log(),
                "Reliable delivery dropped recipient ack for message_id={} with wrong original_sender={} expected={}",
                message_id,
                ack.header.original_sender,
                expected_original_sender
            );
            return Handled::OK;
        }
        if ack.header.recipient != expected_recipient {
            warn!(
                self.log(),
                "Reliable delivery dropped recipient ack for message_id={} with wrong recipient={} expected={}",
                message_id,
                ack.header.recipient,
                expected_recipient
            );
            return Handled::OK;
        }
        let public_header = recipient_ack_public_header_bytes(&ack.header);
        self.spawn_local(async move |mut async_self| {
            async_self
                .security
                .verify_recipient_ack(&ack.header, public_header.as_ref(), &ack.signature)
                .await
                .whatever_benign(
                    "Reliable delivery dropped recipient ack that failed verification",
                )?;
            async_self.cancel_retry(RetryKey::Sender(message_id));
            debug!(
                async_self.log(),
                "Reliable delivery observed recipient ack for message_id={} from recipient={} original_sender={}",
                message_id,
                ack.header.recipient,
                ack.header.original_sender
            );
            if let Some(work_item) = async_self.sender_work_items.get_mut(&message_id) {
                work_item.recipient_ack = RecipientAckStatus::Observed { ack };
            }
            Handled::OK
        });
        Handled::OK
    }

    #[cfg(any(test, feature = "test-support"))]
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

        let header = RecipientAckHeader {
            message_id,
            original_sender: original_sender.clone(),
            recipient,
        };
        let public_header = recipient_ack_public_header_bytes(&header);
        let signature = match self
            .security
            .sign_recipient_ack(&header, public_header.as_ref())
        {
            Ok(signature) => signature,
            Err(error) => {
                warn!(
                    self.log(),
                    "Reliable delivery failed to sign recipient ack for {message_id}: {error}"
                );
                self.inbound_deliveries.remove(&message_id);
                return;
            }
        };
        let ack = RecipientAck { header, signature };

        let pending = self
            .inbound_deliveries
            .get_mut(&message_id)
            .expect("must be able to access the same inbound again");
        pending.ack = Some(ack.clone());
        pending.state = PendingInboundDeliveryState::AckPending;

        if let Some(route) = self.direct_peer_routes.get(&original_sender).cloned() {
            self.dispatch_recipient_ack(message_id, ack, route).await;
        } else {
            debug!(
                self.log(),
                "Reliable delivery processed message_id={message_id} before observing a direct route back to original sender={original_sender}; recipient ack will retry"
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

        let Some(work_item) = self.sender_work_items.get(&message_id) else {
            return;
        };
        let envelope = work_item.submit.envelope.clone();

        self.cancel_retry(RetryKey::Sender(message_id));
        let send_id = RouteSendId(Uuid::new_v4());
        self.spawn_local(async move |mut async_self| {
            let boundary = match async_self
                .security
                .seal_reliable_payload(&envelope.header, envelope.payload.bytes.as_ref())
                .await
            {
                Ok(sealed) => {
                    let envelope = ReliableMessageEnvelope::<EncryptedPayload> {
                        header: envelope.header,
                        payload: EncryptedPayload { sealed },
                    };
                    envelope.to_wire_format()
                }
                Err(error) => {
                    async_self.handle_outbound_security_error(message_id, &error);
                    return Handled::OK;
                }
            };
            if let Some(work_item) = async_self.sender_work_items.get_mut(&message_id) {
                work_item.recipient_route.state = RouteActiveState::AttemptingDirect { send_id };
            }
            let payload: Arc<dyn FlotsyncSerializable> = Arc::new(boundary);
            let send = RouteTransportSend {
                send_id,
                route,
                payload,
            };
            let future = async_self
                .route_transport
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

    fn handle_outbound_security_error(
        &mut self,
        message_id: MessageId,
        error: &DeliverySecurityError,
    ) {
        if error.is_retryable() {
            warn!(
                self.log(),
                "Reliable delivery failed to seal outbound envelope for {message_id}; scheduling retry after {:?}: {error}",
                self.retry_delay
            );
            self.mark_sender_work_pending_retry(
                message_id,
                PendingRouteReason::LocalResourcePressure,
            );
            self.schedule_retry(RetryKey::Sender(message_id), self.retry_delay);
        } else {
            warn!(
                self.log(),
                "Reliable delivery failed to seal outbound envelope for {message_id}; dropping sender work because the error is permanent: {error}"
            );
            self.cancel_retry(RetryKey::Sender(message_id));
            self.sender_work_items.remove(&message_id);
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
    envelope: ReliableMessageEnvelope<PlaintextPayload>,
    state: PendingInboundDeliveryState,
    /// Cached semantic recipient ack reused for later retries after transient
    /// route-transport or discovery failures.
    ack: Option<RecipientAck>,
}

fn select_best_direct_route(
    routes: Vec<SendRouteCandidate<TransportRouteKey>>,
) -> Option<SendRouteCandidate<TransportRouteKey>> {
    routes
        .into_iter()
        .filter(|route| route.sharing == RouteSharingKind::Exclusive)
        .max_by_key(|route| route.preference_rank)
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
