//! The Kompact component implementation for UDPour runtime state.

use super::{queue::*, *};

#[doc(hidden)]
#[derive(Debug)]
pub enum UDPourComponentMessage {
    /// Directed outbound submission resolved once the initial multipart payload
    /// handoff either succeeded or failed.
    Submit(Ask<UDPourSend, UDPourSubmitResult>),
    /// Internal feedback from `flotsync_io` about one physical UDP send.
    SendResult(UdpSendResult),
}

/// Kompact component that runs the multipart UDP protocol over one shared UDP socket.
///
/// The socket must already exist in `flotsync_io`. This component does not own UDP bind/connect
/// policy; it only owns multipart sender/receiver state, pool-backed frame encoding, and the
/// mapping between logical messages and `flotsync_io` send requests.
///
/// TODO(flotsync-nzz): Integrate this one-socket runtime boundary with discovery-published route
/// candidates and broader route-handle ownership.
#[derive(ComponentDefinition)]
pub struct UDPourComponent {
    ctx: ComponentContext<Self>,
    udp_port: RequiredPort<UdpPort>,
    udpour_port: ProvidedPort<UDPourPort>,
    socket_id: SocketId,
    egress_pool: EgressPool,
    config: UDPourConfig,
    sender: SenderMachine,
    receiver: ReceiverMachine,
    next_transmission_id: TransmissionId,
    /// Logical outbound submissions keyed by sender `MessageId`.
    ///
    /// This is the per-logical-message layer: one entry tracks the target address, submit
    /// promise, and how many initial payload datagrams must still be transport-acked before the
    /// caller can see `UDPourSubmitResult::Sent`.
    outbound: HashMap<MessageId, OutboundTransfer>,
    /// Physical UDP datagrams already handed to `flotsync_io` and still awaiting `Ack`/`Nack`.
    ///
    /// This is the per-datagram layer: one transmission id maps to exactly one queued datagram
    /// that is currently in flight on the socket.
    pending_transmissions: HashMap<TransmissionId, QueuedDatagram>,
    /// Datagrams not yet handed to `flotsync_io`, plus pacing state for when the next handoff may
    /// begin.
    dispatcher: OutboundDispatcherState,
    /// Once the underlying UDP socket closes, this runtime never dispatches another datagram.
    ///
    /// A close can race with the async frame-encoding task that was already spawned for the next
    /// queue head. This flag makes that task drop its work instead of reintroducing pending sends
    /// after the runtime has already failed every outstanding submit.
    socket_closed: bool,
    poll_timer: Option<ScheduledTimer>,
    #[cfg(test)]
    /// Logical deadline of the active poll timer, mirrored only for the
    /// manual-time test harness.
    next_poll_at: Option<Instant>,
    #[cfg(test)]
    /// Last `NeedParts` request that this runtime already handled locally.
    ///
    /// Runtime tests use this to distinguish "the bridge observed a
    /// `NeedParts` frame on the sender socket" from "the sender runtime
    /// consumed that frame and queued the corresponding repair response".
    last_processed_need_parts: Option<ProcessedNeedPartsSnapshot>,
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub(crate) struct ActiveTimerSnapshot {
    pub(crate) handle: ScheduledTimer,
    pub(crate) due_at: Instant,
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub(crate) struct RuntimeTimerSnapshot {
    pub(crate) now: Instant,
    pub(crate) dispatch_timer: Option<ActiveTimerSnapshot>,
    pub(crate) poll_timer: Option<ActiveTimerSnapshot>,
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
struct ProcessedNeedPartsSnapshot {
    source: SocketAddr,
    frame: NeedPartsFrame,
}

impl UDPourComponent {
    /// Creates one runtime component bound to one already-open UDP socket.
    #[must_use]
    pub fn new(socket_id: SocketId, egress_pool: EgressPool, config: UDPourConfig) -> Self {
        let sender = SenderMachine::new(config.sender.clone());
        let receiver = ReceiverMachine::new(config.receiver.clone());
        Self {
            ctx: ComponentContext::uninitialised(),
            udp_port: RequiredPort::uninitialised(),
            udpour_port: ProvidedPort::uninitialised(),
            socket_id,
            egress_pool,
            config,
            sender,
            receiver,
            next_transmission_id: TransmissionId::ONE,
            outbound: HashMap::new(),
            pending_transmissions: HashMap::new(),
            dispatcher: OutboundDispatcherState::new(),
            socket_closed: false,
            poll_timer: None,
            #[cfg(test)]
            next_poll_at: None,
            #[cfg(test)]
            last_processed_need_parts: None,
        }
    }

    fn now(&self) -> Instant {
        self.ctx.system().now()
    }

    fn next_send_allowed_at(&self) -> Instant {
        self.dispatcher.next_send_allowed_at.expect(
            "dispatch pacing must be initialised in on_start before the UDPour runtime handles traffic",
        )
    }

    #[cfg(test)]
    pub(crate) fn is_waiting_for_dispatch_timer(&self) -> bool {
        !self.dispatcher.dispatch_in_progress && self.dispatcher.dispatch_timer.is_some()
    }

    #[cfg(test)]
    /// Returns the currently armed runtime timers together with the logical
    /// `now()` seen by this component.
    ///
    /// The manual-time test harness uses this to wait for timer callbacks that
    /// were already armed before one logical-time advance.
    pub(crate) fn timer_snapshot(&self) -> RuntimeTimerSnapshot {
        RuntimeTimerSnapshot {
            now: self.now(),
            dispatch_timer: self.dispatcher.dispatch_timer.clone().map(|handle| {
                ActiveTimerSnapshot {
                    handle,
                    due_at: self.next_send_allowed_at(),
                }
            }),
            poll_timer: self
                .poll_timer
                .clone()
                .zip(self.next_poll_at)
                .map(|(handle, due_at)| ActiveTimerSnapshot { handle, due_at }),
        }
    }

    #[cfg(test)]
    /// Returns whether receiver state has already reflected one observed
    /// payload header from `source`.
    pub(crate) fn receiver_has_reflected_payload(
        &self,
        source: SocketAddr,
        header: UDPourHeader,
    ) -> bool {
        self.receiver.has_reflected_payload(source, header)
    }

    #[cfg(test)]
    /// Returns whether the sender runtime already handled one concrete
    /// `NeedParts` request from `source`.
    pub(crate) fn sender_has_processed_need_parts(
        &self,
        source: SocketAddr,
        frame: &NeedPartsFrame,
    ) -> bool {
        self.last_processed_need_parts
            .as_ref()
            .is_some_and(|snapshot| snapshot.source == source && snapshot.frame.eq(frame))
    }

    fn load_send_rate_control(&self) -> UDPourSendRateControl {
        let send_delay = self
            .ctx
            .config()
            .read_or_default_warn(self.log(), &config_keys::SEND_DELAY);
        let backpressure_retry_delay = self
            .ctx
            .config()
            .read_or_default_warn(self.log(), &config_keys::BACKPRESSURE_RETRY_DELAY);
        let max_in_flight_datagrams = self
            .ctx
            .config()
            .read_or_default_warn(self.log(), &config_keys::MAX_IN_FLIGHT_DATAGRAMS);
        UDPourSendRateControl {
            send_delay,
            backpressure_retry_delay,
            max_in_flight_datagrams,
        }
    }

    fn enqueue_back_all<I>(&mut self, datagrams: I)
    where
        I: IntoIterator<Item = QueuedDatagram>,
    {
        self.dispatcher.queue.extend(datagrams);
        self.try_dispatch_outbound();
    }

    fn enqueue_front(&mut self, datagram: QueuedDatagram) {
        self.dispatcher.queue.push_front(datagram);
        self.try_dispatch_outbound();
    }

    fn enqueue_front_all<I>(&mut self, datagrams: I)
    where
        I: DoubleEndedIterator<Item = QueuedDatagram>,
    {
        self.dispatcher.queue.prepend(datagrams);
        self.try_dispatch_outbound();
    }

    fn try_dispatch_outbound(&mut self) {
        if self.socket_closed {
            return;
        }
        if self.dispatcher.dispatch_in_progress {
            // One spawned encode/send task is already working on the next queue head, so there is
            // nothing for this caller to do until that task finishes and re-enters dispatch.
            return;
        }
        if self.pending_transmissions.len() >= self.dispatcher.send_rate.max_in_flight_datagrams {
            return;
        }
        if self.dispatcher.queue.is_empty() {
            self.clear_dispatch_timer();
            return;
        }
        let now = self.now();
        let next_send_allowed_at = self.next_send_allowed_at();
        if now < next_send_allowed_at {
            self.set_dispatch_timer(next_send_allowed_at - now);
            return;
        }

        self.clear_dispatch_timer();
        let datagram = self
            .dispatcher
            .queue
            .pop_front()
            .expect("queue emptiness was checked before dispatch");
        self.dispatcher.dispatch_in_progress = true;
        let socket_id = self.socket_id;
        let transmission_id = self.next_transmission_id.take_next();
        let egress_pool = self.egress_pool.clone();
        let reply_to = self
            .actor_ref()
            .recipient_with(UDPourComponentMessage::SendResult);
        self.spawn_local(move |mut async_self| async move {
            let frame_type = datagram.frame_type();
            let target = datagram.target;
            let encoded = match encode_frame_with_pool(&egress_pool, &datagram.frame).await {
                Ok(encoded) => encoded,
                Err(error) => {
                    async_self.dispatcher.dispatch_in_progress = false;
                    if datagram.is_outbound_payload_datagram() {
                        error!(
                            async_self.log(),
                            "Failed to encode {:?} outbound UDPour payload datagram for message_id={:?} to {}: {}",
                            frame_type,
                            datagram.message_id(),
                            datagram.target,
                            error
                        );
                        async_self.report_send_failure(
                            datagram.message_id(),
                            UDPourSendFailureReason::Encode(classify_encode_error(&error)),
                        );
                    } else {
                        error!(
                            async_self.log(),
                            "Failed to encode {frame_type:?} control frame for UDPour send to {}: {}",
                            datagram.target,
                            error
                        );
                    }
                    async_self.try_dispatch_outbound();
                    return Handled::OK;
                }
            };

            if async_self.socket_closed {
                async_self.dispatcher.dispatch_in_progress = false;
                if datagram.is_outbound_payload_datagram() {
                    async_self.abort_outbound_transfer(
                        datagram.message_id(),
                        Some(UDPourSendFailureReason::Transport(
                            SendFailureReason::Closed,
                        )),
                    );
                }
                return Handled::OK;
            }

            async_self.pending_transmissions.insert(transmission_id, datagram);
            async_self.dispatcher.dispatch_in_progress = false;
            async_self.dispatcher.next_send_allowed_at =
                Some(async_self.now() + async_self.dispatcher.send_rate.send_delay);
            async_self.udp_port.trigger(UdpRequest::Send {
                socket_id,
                transmission_id,
                payload: encoded,
                target: Some(target),
                reply_to,
            });
            async_self.try_dispatch_outbound();
            Handled::OK
        });
    }

    fn set_dispatch_timer(&mut self, delay: Duration) {
        self.clear_dispatch_timer();
        let timer = self.schedule_once(delay, Self::handle_dispatch_timeout);
        self.dispatcher.dispatch_timer = Some(timer);
    }

    fn clear_dispatch_timer(&mut self) {
        if let Some(timer) = self.dispatcher.dispatch_timer.take() {
            self.cancel_timer(timer);
        }
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "Kompact scheduled callbacks deliver timer handles by value"
    )]
    fn handle_dispatch_timeout(&mut self, expected_timer: ScheduledTimer) -> HandlerResult {
        let Some(active_timer) = self.dispatcher.dispatch_timer.take() else {
            return Handled::OK;
        };
        if active_timer != expected_timer {
            self.dispatcher.dispatch_timer = Some(active_timer);
            return Handled::OK;
        }
        self.try_dispatch_outbound();
        Handled::OK
    }

    fn handle_submit(&mut self, ask: Ask<UDPourSend, UDPourSubmitResult>) -> HandlerResult {
        let (promise, send) = ask.take();
        if self.socket_closed {
            let _ = promise.fulfil(UDPourSubmitResult::SendFailed {
                reason: UDPourSendFailureReason::Transport(SendFailureReason::Closed),
            });
            return Handled::OK;
        }
        let now = self.now();
        match self.sender.start_transfer(send.payload, now) {
            Ok(SenderAction::SendPayloads { message_id, frames }) => {
                self.outbound.insert(
                    message_id,
                    OutboundTransfer {
                        target: send.target,
                        submit_promise: Some(promise),
                        failure_reported: false,
                        sent_reported: false,
                        pending_initial_transmissions: frames.len(),
                    },
                );
                self.dispatch_payload_frames(message_id, frames, true);
            }
            Ok(other) => {
                debug_assert!(
                    false,
                    "start_transfer returned unexpected action: {other:?}"
                );
            }
            Err(error) => {
                let _ = promise.fulfil(UDPourSubmitResult::SendFailed {
                    reason: UDPourSendFailureReason::State(classify_sender_error(&error)),
                });
                Err(error).whatever_benign("Sender state machine rejected UDPour submit")?;
            }
        }
        Handled::OK
    }

    fn handle_udp_indication(&mut self, indication: UdpIndication) -> HandlerResult {
        if indication.socket_id() != Some(self.socket_id) {
            return Handled::OK;
        }
        match indication {
            UdpIndication::Received {
                source, payload, ..
            } => self.handle_received(source, payload),
            UdpIndication::Closed { .. } => {
                self.handle_socket_closed();
                Handled::OK
            }
            _ => Handled::OK,
        }
    }

    fn handle_received(&mut self, source: SocketAddr, payload: IoPayload) -> HandlerResult {
        let frame = decode_frame(payload)
            .with_whatever_benign(|_| format!("Dropping invalid UDPour frame from {source}"))?;
        let now = self.now();

        match frame {
            UDPourFrame::Payload(frame) => match self.receiver.accept_payload(source, frame, now) {
                Ok(actions) => {
                    for action in actions {
                        self.handle_receiver_action(action);
                    }
                }
                Err(error) => {
                    return Err(error).with_whatever_benign(|_| {
                        format!(
                            "Receiver state machine failed while handling payload from {source}"
                        )
                    });
                }
            },
            UDPourFrame::Ack(frame) => {
                if let Some(action) = self.sender.handle_ack(&frame, now) {
                    self.handle_sender_action(action, Some(source));
                }
            }
            UDPourFrame::NeedParts(frame) => match self.sender.handle_need_parts(&frame) {
                Ok(action) => {
                    self.handle_sender_action(action, Some(source));
                    #[cfg(test)]
                    {
                        self.last_processed_need_parts =
                            Some(ProcessedNeedPartsSnapshot { source, frame });
                    }
                }
                Err(error) => {
                    return Err(error).with_whatever_benign(|_| {
                        format!(
                            "Sender state machine failed while handling NeedParts from {source}"
                        )
                    });
                }
            },
            UDPourFrame::NoLongerAvailable(frame) => {
                if let Some(action) = self.receiver.accept_no_longer_available(source, frame) {
                    self.handle_receiver_action(action);
                }
            }
        }

        Handled::OK
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "component messages are consumed at the actor boundary"
    )]
    fn handle_send_result(&mut self, result: UdpSendResult) -> HandlerResult {
        match result {
            UdpSendResult::Ack {
                socket_id,
                transmission_id,
            } if socket_id == self.socket_id => {
                let Some(pending) = self.pending_transmissions.remove(&transmission_id) else {
                    return Handled::OK;
                };

                if pending.counts_towards_sent() {
                    let message_id = pending.message_id();
                    let mut promise = None;
                    let mut report_sent = false;
                    if let Some(outbound) = self.outbound.get_mut(&message_id)
                        && !outbound.failure_reported
                        && !outbound.sent_reported
                    {
                        outbound.pending_initial_transmissions =
                            outbound.pending_initial_transmissions.saturating_sub(1);
                        if outbound.pending_initial_transmissions == 0 {
                            outbound.sent_reported = true;
                            promise = outbound.submit_promise.take();
                            report_sent = true;
                        }
                    }
                    if report_sent {
                        self.sender
                            .mark_initial_send_complete(message_id, self.now());
                    }
                    if let Some(promise) = promise {
                        let _ = promise.fulfil(UDPourSubmitResult::Sent);
                    }
                }
                self.try_dispatch_outbound();
            }
            UdpSendResult::Nack {
                socket_id,
                transmission_id,
                reason,
            } if socket_id == self.socket_id => {
                let Some(pending) = self.pending_transmissions.remove(&transmission_id) else {
                    return Handled::OK;
                };
                if reason == SendFailureReason::Backpressure {
                    self.dispatcher.next_send_allowed_at = Some(
                        self.next_send_allowed_at()
                            .max(self.now() + self.dispatcher.send_rate.backpressure_retry_delay),
                    );
                    self.enqueue_front(pending);
                    return Handled::OK;
                }
                if pending.is_outbound_payload_datagram() {
                    self.report_send_failure(
                        pending.message_id(),
                        UDPourSendFailureReason::Transport(reason),
                    );
                } else {
                    error!(
                        self.log(),
                        "Dropping UDPour control frame to {} after terminal transport failure: {:?}",
                        pending.target,
                        reason
                    );
                }
                self.try_dispatch_outbound();
            }
            _ => {}
        }
        Handled::OK
    }

    fn handle_sender_action(&mut self, action: SenderAction, source: Option<SocketAddr>) {
        match action {
            SenderAction::SendPayloads { message_id, frames } => {
                self.dispatch_payload_frames(message_id, frames, false);
            }
            SenderAction::SendNoLongerAvailable { frame } => {
                if let Some(target) = source {
                    self.dispatch_control_frame(target, UDPourFrame::NoLongerAvailable(frame));
                }
            }
            SenderAction::ObservedAck { message_id } => {
                // Logical send completion is reported when all initial datagrams were transport
                // acknowledged. A later receiver `Ack` only informs sender-side retention state,
                // so the runtime has no additional control-plane work to perform here.
                debug!(
                    self.log(),
                    "Observed receiver Ack for UDPour message_id={:?}; eager_ack_cleanup={} so runtime submit state stays unchanged",
                    message_id,
                    self.config.sender.eager_ack_cleanup
                );
            }
            SenderAction::Purged { message_id, .. } => {
                self.pending_transmissions
                    .retain(|_, pending| !pending.is_outbound_payload_for(message_id));
                self.dispatcher
                    .queue
                    .retain(|datagram| !datagram.is_outbound_payload_for(message_id));
                self.outbound.remove(&message_id);
                self.try_dispatch_outbound();
            }
        }
    }

    fn handle_receiver_action(&mut self, action: ReceiverAction) {
        match action {
            ReceiverAction::Deliver {
                source, payload, ..
            } => {
                self.udpour_port.trigger(UDPourDeliver {
                    socket_id: self.socket_id,
                    source,
                    payload,
                });
            }
            ReceiverAction::SendAck { source, frame } => {
                self.dispatch_control_frame(source, UDPourFrame::Ack(frame));
            }
            ReceiverAction::SendNeedParts { source, frame } => {
                self.dispatch_control_frame(source, UDPourFrame::NeedParts(frame));
            }
            ReceiverAction::Purged { key, reason } => {
                debug!(
                    self.log(),
                    "Purged inbound UDPour state for source={} message_id={:?}: {:?}",
                    key.source,
                    key.message_id,
                    reason
                );
            }
        }
    }

    fn dispatch_payload_frames(
        &mut self,
        message_id: MessageId,
        frames: Vec<PayloadFrame>,
        counts_towards_sent: bool,
    ) {
        let Some(outbound) = self.outbound.get(&message_id) else {
            return;
        };
        let target = outbound.target;
        let datagrams = frames
            .into_iter()
            .map(|frame| QueuedDatagram::payload(target, frame, counts_towards_sent));
        if counts_towards_sent {
            self.enqueue_back_all(datagrams);
        } else {
            self.enqueue_front_all(datagrams);
        }
    }

    fn dispatch_control_frame(&mut self, target: SocketAddr, frame: UDPourFrame) {
        self.enqueue_front(QueuedDatagram::control(target, frame));
    }

    fn report_send_failure(&mut self, message_id: MessageId, reason: UDPourSendFailureReason) {
        self.abort_outbound_transfer(message_id, Some(reason));
    }

    fn abort_outbound_transfer(
        &mut self,
        message_id: MessageId,
        reason: Option<UDPourSendFailureReason>,
    ) {
        let promise = {
            let Some(outbound) = self.outbound.get_mut(&message_id) else {
                return;
            };
            if reason.is_some() {
                outbound.failure_reported = true;
                outbound.submit_promise.take()
            } else {
                None
            }
        };
        if let Some(promise) = promise {
            let _ = promise.fulfil(UDPourSubmitResult::SendFailed {
                reason: reason.expect("submit promise can only be present when reporting failure"),
            });
        }
        let Some(action) = self.sender.abort_transfer(message_id, self.now()) else {
            return;
        };
        self.handle_sender_action(action, None);
    }

    fn handle_socket_closed(&mut self) {
        if self.socket_closed {
            return;
        }
        self.socket_closed = true;
        self.clear_poll_timer();
        self.clear_dispatch_timer();
        self.dispatcher.dispatch_in_progress = false;
        self.dispatcher.queue.clear();
        self.pending_transmissions.clear();

        let outstanding_message_ids: Vec<_> = self.outbound.keys().copied().collect();
        for message_id in outstanding_message_ids {
            self.abort_outbound_transfer(
                message_id,
                Some(UDPourSendFailureReason::Transport(
                    SendFailureReason::Closed,
                )),
            );
        }
    }

    fn poll_runtime(&mut self) {
        let now = self.now();
        for action in self.sender.poll_timeouts(now) {
            self.handle_sender_action(action, None);
        }
        match self.receiver.poll_timeouts(now) {
            Ok(actions) => {
                for action in actions {
                    self.handle_receiver_action(action);
                }
            }
            Err(error) => {
                error!(
                    self.log(),
                    "Receiver timeout processing failed in UDPour runtime: {error}"
                );
            }
        }
    }

    fn set_poll_timer(&mut self) {
        self.clear_poll_timer();
        #[cfg(test)]
        {
            self.next_poll_at = Some(self.now() + self.config.poll_interval);
        }
        let timer = self.schedule_once(self.config.poll_interval, Self::handle_poll_timeout);
        self.poll_timer = Some(timer);
    }

    fn clear_poll_timer(&mut self) {
        if let Some(timer) = self.poll_timer.take() {
            self.cancel_timer(timer);
        }
        #[cfg(test)]
        {
            self.next_poll_at = None;
        }
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "Kompact scheduled callbacks deliver timer handles by value"
    )]
    fn handle_poll_timeout(&mut self, expected_timer: ScheduledTimer) -> HandlerResult {
        let Some(active_timer) = self.poll_timer.take() else {
            return Handled::OK;
        };
        if active_timer != expected_timer {
            self.poll_timer = Some(active_timer);
            return Handled::OK;
        }
        self.poll_runtime();
        self.set_poll_timer();
        Handled::OK
    }
}

impl ComponentLifecycle for UDPourComponent {
    fn on_start(&mut self) -> HandlerResult {
        self.dispatcher.send_rate = self.load_send_rate_control();
        self.dispatcher.next_send_allowed_at = Some(self.now());
        self.set_poll_timer();
        Handled::OK
    }

    fn on_stop(&mut self) -> HandlerResult {
        self.clear_poll_timer();
        self.clear_dispatch_timer();
        Handled::OK
    }

    fn on_kill(&mut self) -> HandlerResult {
        self.clear_poll_timer();
        self.clear_dispatch_timer();
        Handled::OK
    }
}

ignore_requests!(UDPourPort, UDPourComponent);

impl Require<UdpPort> for UDPourComponent {
    fn handle(&mut self, indication: UdpIndication) -> HandlerResult {
        self.handle_udp_indication(indication)
    }
}

impl Actor for UDPourComponent {
    type Message = UDPourComponentMessage;

    fn receive_local(&mut self, msg: Self::Message) -> HandlerResult {
        match msg {
            UDPourComponentMessage::Submit(ask) => self.handle_submit(ask),
            UDPourComponentMessage::SendResult(result) => self.handle_send_result(result),
        }
    }
}
