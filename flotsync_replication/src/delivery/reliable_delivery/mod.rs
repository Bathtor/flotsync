//! Recipient-addressed reliable delivery types and the minimal direct-runtime slice.

mod outbound;
mod wire;

use super::{
    contracts::{
        ReliableDeliveryPort,
        ReliableDeliveryPortIndication,
        ReliableDeliveryPortRequest,
        ReliableDeliveryStore,
        StoredReliableDeliveryWork,
        StoredReliableDeliveryWorkMetadata,
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
use crate::api::{
    StoreErrorClass,
    StoreErrorClassification,
    StoreErrorClassificationSource,
    StoreErrorResolution,
    StoreErrorScope,
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
    RouteTransportSend,
    RouteTransportSubmitResult,
    SendRouteCandidate,
    TransportRouteKey,
};
use flotsync_security::SealedHPKEPayload;
use flotsync_utils::{
    BoxFuture,
    KClaimablePromise,
    NonOwningPhantomData,
    OptionExt as _,
    ResultExt as _,
    kompact_config::ConfigReadExt as _,
};
use futures_util::FutureExt as _;
use kompact::{KompactLogger, kompact_config, prelude::*};
use smallvec::SmallVec;
use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};
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
    ReliableDeliveryDeliver,
    ReliableDeliveryInboundDeliver,
    ReliableDeliveryInboundPort,
    ReliableDeliverySubmit,
    ReliableDeliveryWorkItem,
    ReliableMessageEnvelope,
    ReliableMessageHeader,
};

#[cfg(test)]
use outbound::ReadyKey;
use outbound::{
    AttemptCompletion,
    EncodedReliableEnvelope,
    OutboundAttempt,
    OutboundAttemptResult,
    OutboundState,
    StoredRemovalAttempt,
    UnpersistedSubmission,
};
use retry::{RetryKey, RetryQueue};
use types::{PendingRemoval, PendingRemovalReason};
use uuid::Uuid;

/// Minimal semantic owner for recipient-addressed reliable delivery.
///
/// This slice supports the direct envelope path, persisted sender retries, and
/// semantic recipient acks after explicit processed confirmation. Relay and
/// mailbox delivery remain follow-up work.
#[derive(ComponentDefinition)]
pub struct ReliableDeliveryComponent {
    ctx: ComponentContext<Self>,
    delivery_port: ProvidedPort<ReliableDeliveryPort>,
    ingress_inbound_port: RequiredPort<TransportReliableDeliveryInboundPort>,
    discovery_port: RequiredPort<TransportRouteDiscoveryPort>,
    /// Runtime configuration loaded when the component starts.
    config: Config,
    /// Receiver-side semantic-delivery and acknowledgement state.
    inbound: InboundState,
    /// Sender-side admission, persistence, queueing, and cleanup state.
    outbound: OutboundState,
    /// Dependencies and scheduler state used by both delivery directions.
    shared: SharedState,
}

impl ReliableDeliveryComponent {
    /// Create one new reliable-delivery component around the shared
    /// route-transport actor.
    #[must_use]
    pub(crate) fn new(
        route_transport: ActorRefStrong<RouteTransportActorMessage<TransportRouteKey>>,
        security: DeliverySecurity,
        store: Arc<dyn ReliableDeliveryStore>,
    ) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            delivery_port: ProvidedPort::uninitialised(),
            ingress_inbound_port: RequiredPort::uninitialised(),
            discovery_port: RequiredPort::uninitialised(),
            config: Config::default(),
            inbound: InboundState::default(),
            outbound: OutboundState::new(store),
            shared: SharedState::new(route_transport, security),
        }
    }

    fn now(&self) -> Instant {
        self.ctx.system().now()
    }

    /// Admit one plaintext submission unless the message id is already live.
    fn handle_submit_request(&mut self, submit: ReliableDeliverySubmit) -> HandlerResult {
        let message_id = submit.envelope.header.message_id;
        if self.outbound.contains_message(message_id) {
            warn!(
                self.log(),
                "Reliable delivery rejected duplicate submit for existing {message_id}"
            );
            return Handled::OK;
        }
        self.outbound.unpersisted_submissions.insert(
            message_id,
            UnpersistedSubmission::Plaintext {
                submit,
                first_submitted_at: SystemTime::now(),
            },
        );

        Handled::block_on(self, async move |mut async_self| {
            let _handled = async_self
                .persist_one_unpersisted_submission(message_id)
                .await?;
            let attempts = async_self.select_ready_outbound_attempts();
            async_self
                .load_and_submit_selected_outbound_attempts(attempts)
                .await
        })
    }

    /// Seal plaintext once, then retry only storage for an encoded admission item.
    async fn persist_one_unpersisted_submission(&mut self, message_id: MessageId) -> HandlerResult {
        let Some(unpersisted) = self
            .outbound
            .unpersisted_submissions
            .get(&message_id)
            .cloned()
        else {
            debug!(
                self.log(),
                "Reliable delivery ignored stale unpersisted retry for unknown message_id={message_id}"
            );
            return Handled::OK;
        };
        let stored = match unpersisted {
            UnpersistedSubmission::Plaintext {
                submit,
                first_submitted_at,
            } => {
                let recipient = submit.envelope.header.recipient.clone();
                let stored = match OutboundState::seal_submission(
                    &self.shared.security,
                    submit,
                    first_submitted_at,
                )
                .await
                {
                    Ok(stored) => stored,
                    Err(error) => {
                        return self
                            .handle_unpersisted_security_error(message_id, &recipient, error);
                    }
                };
                self.outbound.unpersisted_submissions.insert(
                    message_id,
                    UnpersistedSubmission::Encoded {
                        work: stored.clone(),
                    },
                );
                stored
            }
            UnpersistedSubmission::Encoded { work } => work,
        };
        let recipient = stored.metadata.recipient.clone();
        if let Err(error) = self
            .outbound
            .store
            .store_reliable_delivery_work(stored.clone())
            .await
        {
            if !store_failure_can_retry_later(error.classification()) {
                return Err(error).whatever_unrecoverable(format!(
                    "Reliable delivery failed to persist outbound message_id={message_id}"
                ));
            }
            self.schedule_retry(RetryKey::Unpersisted(message_id), self.config.retry_delay);
            warn!(
                self.log(),
                "Reliable delivery retained encoded outbound message_id={} recipient={} in memory after persistence failed; it is not persisted and can be lost if this component stops; retrying after {:?}: {}",
                message_id,
                recipient,
                self.config.retry_delay,
                error
            );
            return Handled::OK;
        }
        self.cancel_retry(RetryKey::Unpersisted(message_id));
        self.outbound.unpersisted_submissions.remove(&message_id);
        self.outbound.work_items.insert(
            message_id,
            OutboundState::new_work_item(stored.metadata, PendingRouteReason::ReachabilityUnknown),
        );
        let logger = self.log().clone();
        if self
            .outbound
            .place_by_route(message_id, &self.shared.direct_peer_routes, &logger)
            .is_err()
        {
            self.cancel_outbound_retries(message_id);
        }
        Handled::OK
    }

    /// Retain retryable plaintext work, reject permanent non-store failures, or fail on permanent
    /// store failures.
    fn handle_unpersisted_security_error(
        &mut self,
        message_id: MessageId,
        recipient: &MemberIdentity,
        error: DeliverySecurityError,
    ) -> HandlerResult {
        let store_classification = error.store_error_classification();
        let retryable = match store_classification {
            Some(classification) => store_failure_can_retry_later(classification),
            None => error.is_retryable(),
        };
        if retryable {
            self.schedule_retry(RetryKey::Unpersisted(message_id), self.config.retry_delay);
            warn!(
                self.log(),
                "Reliable delivery retained outbound message_id={} recipient={} in memory after sealing failed; it is not persisted and can be lost if this component stops; retrying after {:?}: {}",
                message_id,
                recipient,
                self.config.retry_delay,
                error
            );
            Handled::OK
        } else if store_classification.is_some() {
            Err(error).whatever_unrecoverable(format!(
                "Reliable delivery failed to prepare outbound message_id={message_id} recipient={recipient} because store access cannot be retried"
            ))
        } else {
            self.cancel_retry(RetryKey::Unpersisted(message_id));
            self.outbound.unpersisted_submissions.remove(&message_id);
            error!(
                self.log(),
                "Reliable delivery permanently rejected outbound message_id={} recipient={} before persistence: {}",
                message_id,
                recipient,
                error
            );
            Handled::OK
        }
    }

    /// Reserve all currently available outbound slots before entering blocking mode.
    ///
    /// The returned stack has its oldest attempt at the end.
    fn select_ready_outbound_attempts(&mut self) -> SmallVec<[OutboundAttempt; 1]> {
        let available_slots = self
            .config
            .max_concurrent_outbound_attempts
            .saturating_sub(self.outbound.attempts_in_flight.len());
        let logger = self.log().clone();
        let selected = self.outbound.select_ready_attempts(
            available_slots,
            &self.shared.direct_peer_routes,
            &logger,
        );
        self.cancel_retries_for_purged_messages(selected.purged);
        selected.attempts
    }

    /// Load an already reserved attempt stack and refill slots freed by local failures.
    ///
    /// `attempts` must have its oldest item at the end so `pop` preserves scheduling order.
    /// Passing the initial stack keeps slot reservation before the caller's blocking boundary.
    fn load_and_submit_selected_outbound_attempts(
        &mut self,
        mut attempts: SmallVec<[OutboundAttempt; 1]>,
    ) -> BoxFuture<'_, HandlerResult> {
        async move {
            while let Some(attempt) = attempts.pop() {
                let load_result = self
                    .outbound
                    .store
                    .load_reliable_delivery_work(attempt.message_id)
                    .await;
                match load_result {
                    Ok(Some(stored)) => {
                        let payload: Arc<dyn FlotsyncSerializable> =
                            Arc::new(EncodedReliableEnvelope(stored.encoded_envelope));
                        let send = RouteTransportSend {
                            send_id: attempt.send_id,
                            route: attempt.route,
                            payload,
                        };
                        let future = self.shared.route_transport.ask_with(|promise| {
                            RouteTransportActorMessage::Submit(Ask::new(promise, send))
                        });
                        self.spawn_local(async move |mut async_self| {
                            async_self
                                .finish_outbound_envelope_submit(
                                    attempt.message_id,
                                    attempt.send_id,
                                    future,
                                )
                                .await
                        });
                    }
                    Ok(None) => {
                        error!(
                            self.log(),
                            "Reliable delivery selected outbound message_id={} but its stored row was missing; purging all in-memory state for this message and continuing",
                            attempt.message_id
                        );
                        self.outbound.purge_message(attempt.message_id);
                        self.cancel_outbound_retries(attempt.message_id);
                    }
                    Err(error) => {
                        match selected_load_failure_policy(error.classification()) {
                            SelectedLoadFailurePolicy::RetryLater => {
                                let logger = self.log().clone();
                                let completion = self.outbound.finish_outbound_attempt(
                                    attempt.message_id,
                                    attempt.send_id,
                                    OutboundAttemptResult::LoadFailed,
                                    &logger,
                                );
                                self.apply_attempt_completion(attempt.message_id, completion);
                                error!(
                                    self.log(),
                                    "Reliable delivery failed to load outbound message_id={} from storage; scheduling sender retry: {}",
                                    attempt.message_id,
                                    error
                                );
                            }
                            SelectedLoadFailurePolicy::RemoveInvalidRecord => {
                                error!(
                                    self.log(),
                                    "Reliable delivery isolated invalid stored outbound message_id={} and will remove only that record: {}",
                                    attempt.message_id,
                                    error
                                );
                                let logger = self.log().clone();
                                let completion = self.outbound.finish_outbound_attempt(
                                    attempt.message_id,
                                    attempt.send_id,
                                    OutboundAttemptResult::InvalidStoredRecord,
                                    &logger,
                                );
                                self.apply_attempt_completion(attempt.message_id, completion);
                                let _handled = self
                                    .attempt_stored_sender_removal(attempt.message_id)
                                    .await?;
                            }
                            SelectedLoadFailurePolicy::FailComponent => {
                                return Err(error).whatever_unrecoverable(format!(
                                    "Reliable delivery failed to load outbound message_id={}",
                                    attempt.message_id
                                ));
                            }
                        }
                    }
                }
                if attempts.is_empty() {
                    attempts = self.select_ready_outbound_attempts();
                }
            }
            Handled::OK
        }
        .boxed()
    }

    /// Finish one route-transport attempt and release its configured slot.
    async fn finish_outbound_envelope_submit(
        &mut self,
        message_id: MessageId,
        send_id: RouteSendId,
        future: KFuture<RouteTransportSubmitResult<TransportRouteKey>>,
    ) -> HandlerResult {
        let result = match future.await {
            Ok(result) => OutboundAttemptResult::Transport(result),
            Err(_error) => OutboundAttemptResult::PromiseDropped,
        };
        let logger = self.log().clone();
        let completion = self
            .outbound
            .finish_outbound_attempt(message_id, send_id, result, &logger);
        if completion == AttemptCompletion::PermanentFailure {
            error!(
                self.log(),
                "Reliable delivery permanently failed outbound message_id={message_id}; removing persisted work because route transport rejected the stored envelope as an invalid payload"
            );
        }
        self.apply_attempt_completion(message_id, completion);

        if completion == AttemptCompletion::PermanentFailure {
            let _handled = self.attempt_stored_sender_removal(message_id).await?;
        }

        let attempts = self.select_ready_outbound_attempts();
        if attempts.is_empty() {
            Handled::OK
        } else {
            Handled::block_on(self, async move |mut async_self| {
                async_self
                    .load_and_submit_selected_outbound_attempts(attempts)
                    .await
            })
        }
    }

    /// Apply scheduler effects requested by the outbound attempt state transition.
    fn apply_attempt_completion(&mut self, message_id: MessageId, completion: AttemptCompletion) {
        match completion {
            AttemptCompletion::AwaitingRecipientAck => {
                self.cancel_retry(RetryKey::Sender(message_id));
                self.schedule_retry(
                    RetryKey::Sender(message_id),
                    self.config.recipient_ack_timeout,
                );
            }
            AttemptCompletion::RetryScheduled => {
                self.schedule_retry(RetryKey::Sender(message_id), self.config.retry_delay);
            }
            AttemptCompletion::PermanentFailure => {
                self.cancel_retry(RetryKey::Sender(message_id));
            }
            AttemptCompletion::CleanupOnly => {
                debug!(
                    self.log(),
                    "Reliable delivery ignored completed outbound attempt for cleanup-pending message_id={message_id}"
                );
            }
            AttemptCompletion::Purged => self.cancel_outbound_retries(message_id),
        }
    }

    /// Attempt one pending stored-row removal without making the message sendable again.
    async fn attempt_stored_sender_removal(&mut self, message_id: MessageId) -> HandlerResult {
        let logger = self.log().clone();
        let attempt = self
            .outbound
            .attempt_stored_removal(message_id, &logger)
            .await
            .whatever_unrecoverable(format!(
                "Reliable delivery failed to remove stored outbound message_id={message_id}"
            ))?;
        match attempt {
            StoredRemovalAttempt::Completed => {
                self.cancel_retry(RetryKey::SenderRemoval(message_id));
            }
            StoredRemovalAttempt::Retry => {
                self.schedule_retry(RetryKey::SenderRemoval(message_id), self.config.retry_delay);
            }
            StoredRemovalAttempt::Stale => {
                debug!(
                    self.log(),
                    "Reliable delivery ignored stale sender-removal retry for message_id={message_id}"
                );
            }
        }
        Handled::OK
    }

    /// Promote one sender retry after its due time was removed from the scheduler.
    fn handle_sender_retry_timeout(&mut self, message_id: MessageId) {
        let Some((state, recipient, reported_ack_timeout, cleanup_pending)) =
            self.outbound.work_items.get(&message_id).map(|work_item| {
                (
                    work_item.recipient_route.state.clone(),
                    work_item.metadata.recipient.clone(),
                    work_item.reported_ack_timeout,
                    work_item.pending_removal.is_some(),
                )
            })
        else {
            debug!(
                self.log(),
                "Reliable delivery ignored stale sender retry for unknown message_id={message_id}"
            );
            return;
        };
        if cleanup_pending {
            debug!(
                self.log(),
                "Reliable delivery ignored stale sender retry for cleanup-pending message_id={message_id}"
            );
            return;
        }

        match state {
            RouteActiveState::AwaitingRecipientAck => {
                let sender = self.shared.security.local_member().clone();
                if reported_ack_timeout {
                    debug!(
                        self.log(),
                        "Reliable delivery recipient ack remains absent for message_id={} sender={} recipient={} after another {:?}; retrying envelope delivery",
                        message_id,
                        sender,
                        recipient,
                        self.config.recipient_ack_timeout
                    );
                } else {
                    self.outbound
                        .work_items
                        .get_mut(&message_id)
                        .expect("known sender work must remain present")
                        .reported_ack_timeout = true;
                    warn!(
                        self.log(),
                        "Reliable delivery recipient ack timed out for message_id={} sender={} recipient={} after {:?}; retrying envelope delivery",
                        message_id,
                        sender,
                        recipient,
                        self.config.recipient_ack_timeout
                    );
                }
                self.outbound
                    .mark_retry_scheduled(message_id, PendingRouteReason::BackoffInEffect);
                self.place_outbound_by_route(message_id);
            }
            RouteActiveState::RetryScheduled { .. } => self.place_outbound_by_route(message_id),
            RouteActiveState::WaitingForRoute { .. } => {
                debug!(
                    self.log(),
                    "Reliable delivery ignored stale sender retry for route-waiting message_id={message_id}"
                );
            }
            RouteActiveState::Queued => {
                debug!(
                    self.log(),
                    "Reliable delivery ignored stale sender retry for already ready message_id={message_id}"
                );
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

    /// Put work with an available route into the ready queue, or otherwise into waiting.
    fn place_outbound_by_route(&mut self, message_id: MessageId) {
        let logger = self.log().clone();
        if self
            .outbound
            .place_by_route(message_id, &self.shared.direct_peer_routes, &logger)
            .is_err()
        {
            self.cancel_outbound_retries(message_id);
        }
    }

    /// Cancel scheduler entries after purging inconsistent outbound memory.
    fn cancel_retries_for_purged_messages(
        &mut self,
        message_ids: impl IntoIterator<Item = MessageId>,
    ) {
        let mut changed = false;
        for message_id in message_ids {
            self.shared
                .retry_queue
                .cancel(RetryKey::Unpersisted(message_id));
            self.shared.retry_queue.cancel(RetryKey::Sender(message_id));
            self.shared
                .retry_queue
                .cancel(RetryKey::SenderRemoval(message_id));
            changed = true;
        }
        if changed {
            self.set_retry_timer(self.now());
        }
    }

    /// Cancel every outbound retry kind for one message id.
    fn cancel_outbound_retries(&mut self, message_id: MessageId) {
        self.cancel_retries_for_purged_messages([message_id]);
    }

    fn handle_discovery_update(&mut self, update: TransportDiscoveryRouteUpdate) -> HandlerResult {
        match update {
            TransportDiscoveryRouteUpdate::PeerRoutes { peer, routes, .. } => {
                if let Some(route) = select_best_direct_route(routes) {
                    // TODO(flotsync-irn): Store multiple direct routes per peer
                    // so reliable delivery can retry on an alternate route after
                    // one direct send fails.
                    self.shared.direct_peer_routes.insert(peer.clone(), route);
                    let logger = self.log().clone();
                    let purged = self.outbound.ready_waiting_items_for_route(&peer, &logger);
                    self.cancel_retries_for_purged_messages(purged);
                } else {
                    self.shared.direct_peer_routes.remove(&peer);
                    let logger = self.log().clone();
                    let purged = self.outbound.withdraw_ready(&peer, &logger);
                    self.cancel_retries_for_purged_messages(purged);
                }
                self.retry_pending_inbound_acks_for_peer(&peer);
            }
            TransportDiscoveryRouteUpdate::RelayRoutes { .. } => {
                // TODO(flotsync-fx5): Consume relay-route discovery updates once
                // the reliable-delivery relay path is implemented.
            }
        }
        let attempts = self.select_ready_outbound_attempts();
        if attempts.is_empty() {
            Handled::OK
        } else {
            Handled::block_on(self, async move |mut async_self| {
                async_self
                    .load_and_submit_selected_outbound_attempts(attempts)
                    .await
            })
        }
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
                .shared
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
            async_self.inbound.deliveries.insert(
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
            .inbound
            .deliveries
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
        let Some((expected_original_sender, expected_recipient)) = self
            .outbound
            .work_items
            .get(&message_id)
            .filter(|work_item| work_item.pending_removal.is_none())
            .map(|work_item| {
                (
                    self.shared.security.local_member().clone(),
                    work_item.metadata.recipient.clone(),
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
        // Stored-envelope loads and acknowledgement cleanup both run through
        // explicit blocking component futures. The transport result may arrive
        // later, but the envelope bytes were already loaded before submission.
        Handled::block_on(self, async move |mut async_self| {
            async_self
                .shared
                .security
                .verify_recipient_ack(&ack.header, public_header.as_ref(), &ack.signature)
                .await
                .whatever_benign(
                    "Reliable delivery dropped recipient ack that failed verification",
                )?;
            async_self.cancel_retry(RetryKey::Sender(message_id));
            let _began_removal = async_self
                .outbound
                .begin_removal(message_id, PendingRemovalReason::RecipientAcknowledged);
            let _handled = async_self.attempt_stored_sender_removal(message_id).await?;
            debug!(
                async_self.log(),
                "Reliable delivery observed recipient ack for message_id={} from recipient={} original_sender={}",
                message_id,
                ack.header.recipient,
                ack.header.original_sender
            );
            let attempts = async_self.select_ready_outbound_attempts();
            async_self
                .load_and_submit_selected_outbound_attempts(attempts)
                .await
        })
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn knows_direct_route(&self, peer: &MemberIdentity) -> bool {
        self.shared.direct_peer_routes.get(peer).is_some()
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
                self.inbound.deliveries.remove(&message_id);
            }
        }
    }

    async fn finish_processed_delivery(&mut self, message_id: MessageId) {
        let Some((recipient, original_sender)) =
            self.inbound.deliveries.get(&message_id).map(|pending| {
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
            .shared
            .security
            .sign_recipient_ack(&header, public_header.as_ref())
        {
            Ok(signature) => signature,
            Err(error) => {
                warn!(
                    self.log(),
                    "Reliable delivery failed to sign recipient ack for {message_id}: {error}"
                );
                self.inbound.deliveries.remove(&message_id);
                return;
            }
        };
        let ack = RecipientAck { header, signature };

        let pending = self
            .inbound
            .deliveries
            .get_mut(&message_id)
            .expect("must be able to access the same inbound again");
        pending.ack = Some(ack.clone());
        pending.state = PendingInboundDeliveryState::AckPending;

        if let Some(route) = self
            .shared
            .direct_peer_routes
            .get(&original_sender)
            .cloned()
        {
            self.dispatch_recipient_ack(message_id, ack, route).await;
        } else {
            debug!(
                self.log(),
                "Reliable delivery processed message_id={message_id} before observing a direct route back to original sender={original_sender}; recipient ack will retry"
            );
            self.schedule_retry(RetryKey::InboundAck(message_id), self.config.retry_delay);
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
        if let Some(pending) = self.inbound.deliveries.get_mut(&message_id) {
            pending.state = PendingInboundDeliveryState::AckInFlight;
        }

        let payload: Arc<dyn FlotsyncSerializable> = Arc::new(boundary);
        let send = RouteTransportSend {
            send_id: RouteSendId::new_random(),
            route,
            payload,
        };
        let future = self
            .shared
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
                self.inbound.deliveries.remove(&message_id);
            }
            Ok(RouteTransportSubmitResult::SendFailed { reason, .. }) => {
                if let Some(pending) = self.inbound.deliveries.get_mut(&message_id) {
                    pending.state = PendingInboundDeliveryState::AckPending;
                }
                self.schedule_retry(RetryKey::InboundAck(message_id), self.config.retry_delay);
                warn!(
                    self.log(),
                    "Reliable delivery recipient ack transport send failed for {message_id}: {reason:?}"
                );
            }
            Err(_error) => {
                if let Some(pending) = self.inbound.deliveries.get_mut(&message_id) {
                    pending.state = PendingInboundDeliveryState::AckPending;
                }
                self.schedule_retry(RetryKey::InboundAck(message_id), self.config.retry_delay);
                warn!(
                    self.log(),
                    "Reliable delivery recipient ack transport promise dropped for message_id={message_id}"
                );
            }
        }
    }

    fn retry_pending_inbound_acks_for_peer(&mut self, peer: &MemberIdentity) {
        let message_ids: Vec<_> = self
            .inbound
            .deliveries
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
            self.inbound
                .deliveries
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
        let Some(route) = self
            .shared
            .direct_peer_routes
            .get(&original_sender)
            .cloned()
        else {
            self.schedule_retry(RetryKey::InboundAck(message_id), self.config.retry_delay);
            return;
        };
        self.spawn_local(move |mut async_self| async move {
            async_self
                .dispatch_recipient_ack(message_id, ack, route)
                .await;
            Handled::OK
        });
    }

    fn schedule_retry(&mut self, key: RetryKey, delay: Duration) {
        let now = self.now();
        self.shared.retry_queue.schedule(key, now + delay);
        self.set_retry_timer(now);
    }

    fn cancel_retry(&mut self, key: RetryKey) {
        self.shared.retry_queue.cancel(key);
        self.set_retry_timer(self.now());
    }

    fn set_retry_timer(&mut self, now: Instant) {
        self.shared.retry_queue.remove_stale_entries();
        if let Some(timer) = self.shared.retry_timer.take() {
            self.cancel_timer(timer);
        }
        let Some(next_due_at) = self.shared.retry_queue.next_due_at() else {
            return;
        };
        let delay = next_due_at.saturating_duration_since(now);
        let timer = self.schedule_once(delay, move |component, expected_timer| {
            component.handle_retry_timeout(&expected_timer)
        });
        self.shared.retry_timer = Some(timer);
    }

    fn handle_retry_timeout(&mut self, expected_timer: &ScheduledTimer) -> HandlerResult {
        let Some(active_timer) = self.shared.retry_timer.take() else {
            return Handled::OK;
        };
        if &active_timer != expected_timer {
            self.shared.retry_timer = Some(active_timer);
            return Handled::OK;
        }
        let now = self.now();
        let ready = self.shared.retry_queue.take_ready(now);
        let mut unpersisted_message_ids = Vec::new();
        let mut removal_message_ids = Vec::new();
        for key in ready {
            match key {
                RetryKey::Unpersisted(message_id) => unpersisted_message_ids.push(message_id),
                RetryKey::Sender(message_id) => self.handle_sender_retry_timeout(message_id),
                RetryKey::SenderRemoval(message_id) => removal_message_ids.push(message_id),
                RetryKey::InboundAck(message_id) => self.try_dispatch_inbound_ack(message_id),
            }
        }
        self.set_retry_timer(now);
        if unpersisted_message_ids.is_empty() && removal_message_ids.is_empty() {
            let attempts = self.select_ready_outbound_attempts();
            if attempts.is_empty() {
                Handled::OK
            } else {
                Handled::block_on(self, async move |mut async_self| {
                    async_self
                        .load_and_submit_selected_outbound_attempts(attempts)
                        .await
                })
            }
        } else {
            Handled::block_on(self, async move |mut async_self| {
                for message_id in unpersisted_message_ids {
                    let _handled = async_self
                        .persist_one_unpersisted_submission(message_id)
                        .await?;
                }
                for message_id in removal_message_ids {
                    let _handled = async_self.attempt_stored_sender_removal(message_id).await?;
                }
                let attempts = async_self.select_ready_outbound_attempts();
                async_self
                    .load_and_submit_selected_outbound_attempts(attempts)
                    .await
            })
        }
    }

    #[cfg(test)]
    fn sender_work_item(&self, message_id: MessageId) -> Option<&ReliableDeliveryWorkItem> {
        self.outbound.work_items.get(&message_id)
    }

    /// Return whether this message has already emitted its one visible ack-timeout warning.
    #[cfg(test)]
    fn reported_recipient_ack_timeout(&self, message_id: MessageId) -> bool {
        self.outbound
            .work_items
            .get(&message_id)
            .is_some_and(|work_item| work_item.reported_ack_timeout)
    }

    #[cfg(test)]
    fn inbound_delivery_state(&self, message_id: MessageId) -> Option<PendingInboundDeliveryState> {
        self.inbound
            .deliveries
            .get(&message_id)
            .map(|pending| pending.state)
    }
}

impl ComponentLifecycle for ReliableDeliveryComponent {
    fn on_start(&mut self) -> HandlerResult {
        self.config = Config::load(self.ctx.config(), self.log());
        Handled::block_on(self, async move |mut async_self| {
            let stored_metadata = async_self
                .outbound
                .store
                .load_reliable_delivery_work_metadata()
                .await
                .whatever_unrecoverable(
                    "Reliable delivery failed to restore outbound sender work",
                )?;
            for metadata in stored_metadata {
                let message_id = metadata.message_id;
                async_self.outbound.work_items.insert(
                    message_id,
                    OutboundState::new_work_item(
                        metadata,
                        PendingRouteReason::RecoveredAfterRestart,
                    ),
                );
                async_self.place_outbound_by_route(message_id);
            }
            let attempts = async_self.select_ready_outbound_attempts();
            async_self
                .load_and_submit_selected_outbound_attempts(attempts)
                .await
        })
    }

    fn on_stop(&mut self) -> HandlerResult {
        if let Some(timer) = self.shared.retry_timer.take() {
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

/// Return whether reliable delivery may repeat already-retained idempotent store work later.
const fn store_failure_can_retry_later(classification: StoreErrorClassification) -> bool {
    matches!(
        classification.resolution,
        StoreErrorResolution::Retry | StoreErrorResolution::WaitForResume
    )
}

/// Choose how reliable delivery handles one failed selected-row load.
const fn selected_load_failure_policy(
    classification: StoreErrorClassification,
) -> SelectedLoadFailurePolicy {
    if store_failure_can_retry_later(classification) {
        SelectedLoadFailurePolicy::RetryLater
    } else if matches!(
        (
            classification.scope,
            classification.class,
            classification.resolution
        ),
        (
            StoreErrorScope::Record,
            StoreErrorClass::InvalidData,
            StoreErrorResolution::Repair
        )
    ) {
        SelectedLoadFailurePolicy::RemoveInvalidRecord
    } else {
        SelectedLoadFailurePolicy::FailComponent
    }
}

/// Component action for one selected outbound-row load failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SelectedLoadFailurePolicy {
    /// Retain the work and use the existing sender retry schedule.
    RetryLater,
    /// Exclude and remove only the invalid stored record.
    RemoveInvalidRecord,
    /// Fail the component because continuing would violate reliable-delivery guarantees.
    FailComponent,
}

/// Runtime values loaded from the Kompact configuration when the component starts.
struct Config {
    /// Delay before retrying admission, failed sends, cleanup, or recipient acknowledgements.
    retry_delay: Duration,
    /// Wait after local transport success before an envelope is sent again.
    recipient_ack_timeout: Duration,
    /// Upper bound for simultaneous outbound route attempts.
    max_concurrent_outbound_attempts: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            retry_delay: DEFAULT_RETRY_DELAY,
            recipient_ack_timeout: DEFAULT_RECIPIENT_ACK_TIMEOUT,
            max_concurrent_outbound_attempts: DEFAULT_MAX_CONCURRENT_OUTBOUND_ATTEMPTS,
        }
    }
}

impl Config {
    /// Load the complete reliable-delivery configuration from Kompact.
    fn load(config: &kompact::config::Config, logger: &KompactLogger) -> Self {
        Self {
            retry_delay: config.read_or_default_warn(logger, &config_keys::RETRY_DELAY),
            recipient_ack_timeout: config
                .read_or_default_warn(logger, &config_keys::RECIPIENT_ACK_TIMEOUT),
            max_concurrent_outbound_attempts: config
                .read_or_default_warn(logger, &config_keys::MAX_CONCURRENT_OUTBOUND_ATTEMPTS),
        }
    }
}

/// Receiver-side messages awaiting semantic processing or acknowledgement transport.
#[derive(Default)]
struct InboundState {
    /// Messages retained until application processing and acknowledgement transport finish.
    deliveries: HashMap<MessageId, PendingInboundDelivery>,
}

/// Dependencies and scheduler state shared by inbound and outbound delivery.
struct SharedState {
    /// Route-transport actor used for envelopes and recipient acknowledgements.
    route_transport: ActorRefStrong<RouteTransportActorMessage<TransportRouteKey>>,
    /// Cryptographic boundary shared by sender and recipient processing.
    security: DeliverySecurity,
    /// Latest direct route advertised for each peer.
    direct_peer_routes: TrieMap<SendRouteCandidate<TransportRouteKey>>,
    /// Due times for admission, sender, cleanup, and inbound-ack retries.
    retry_queue: RetryQueue,
    /// Sole Kompact timer currently scheduled for the earliest retry.
    retry_timer: Option<ScheduledTimer>,
}

impl SharedState {
    /// Build empty shared state around the component dependencies.
    fn new(
        route_transport: ActorRefStrong<RouteTransportActorMessage<TransportRouteKey>>,
        security: DeliverySecurity,
    ) -> Self {
        Self {
            route_transport,
            security,
            direct_peer_routes: TrieMap::new(),
            retry_queue: RetryQueue::new(),
            retry_timer: None,
        }
    }
}

mod config_keys {
    use super::{
        DEFAULT_MAX_CONCURRENT_OUTBOUND_ATTEMPTS,
        DEFAULT_RECIPIENT_ACK_TIMEOUT,
        Duration,
        kompact_config,
    };
    use kompact::config::{DurationValue, UsizeValue};

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
        doc = "Maximum wait for a semantic recipient acknowledgement after local transport submission succeeds.",
        version = "0.1.0"
    }

    kompact_config! {
        MAX_CONCURRENT_OUTBOUND_ATTEMPTS,
        key = "flotsync.reliable-delivery.max-concurrent-outbound-attempts",
        type = UsizeValue,
        default = DEFAULT_MAX_CONCURRENT_OUTBOUND_ATTEMPTS,
        validate = |value| *value > 0,
        doc = "Maximum number of persisted reliable envelopes concurrently submitted through route transport. Values must be greater than zero.",
        version = "0.1.0"
    }
}

const DEFAULT_RETRY_DELAY: Duration = Duration::from_secs(30);
const DEFAULT_MAX_CONCURRENT_OUTBOUND_ATTEMPTS: usize = 10;
/// Interactive recovery interval for ambiguous loss after successful local transport submission.
///
/// Normal reliable messages are small, and listener-mediated group work is acknowledged after
/// persistence plus successful listener notification rather than after a manual decision.
const DEFAULT_RECIPIENT_ACK_TIMEOUT: Duration = Duration::from_secs(10);

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
