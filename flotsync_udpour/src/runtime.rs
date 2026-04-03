//! Kompact runtime adapter for the UDPour protocol.
//!
//! This module binds the pure sender/receiver state machines to one shared UDP
//! socket from `flotsync_io`, owns the mapping from logical multipart messages
//! to concrete UDP sends, and surfaces route-transport failures at the
//! component boundary.

use crate::{
    codec::{FRAME_HEADER_LEN, decode_frame, encoded_frame_len},
    receiver::{ReceiverAction, ReceiverConfig, ReceiverMachine},
    roaring_helpers::RoaringBitmapError,
    sender::{SenderAction, SenderConfig, SenderError, SenderMachine},
    types::{
        AckFrame,
        Checksum,
        MessageId,
        NeedPartsFrame,
        NoLongerAvailableFrame,
        PartCount,
        PayloadFrame,
        UDPourFrame,
    },
    wire::EncodeToBufMut,
};
use flotsync_io::prelude::{
    EgressPool,
    Error as IoError,
    IoPayload,
    PayloadWriter,
    SendFailureReason,
    SocketId,
    TransmissionId,
    UdpIndication,
    UdpPort,
    UdpRequest,
    UdpSendResult,
};
use flotsync_utils::{LocalActor, impl_local_actor};
use kompact::{
    config::{DurationValue, HoconExt, UsizeValue},
    kompact_config,
    prelude::*,
};
use snafu::prelude::*;
use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    time::{Duration, Instant},
};

/// One logical outbound transfer request for the UDPour runtime.
#[derive(Clone, Debug)]
pub struct UDPourSend {
    /// Route-level UDP target that should receive every `Payload` retransmission.
    pub target: SocketAddr,
    /// Full logical payload to split into multipart `Payload` frames.
    pub payload: IoPayload,
}

/// One fully reassembled inbound logical message.
#[derive(Clone, Debug)]
pub struct UDPourDeliver {
    /// Forwarded UDP source address that identified the sender at this route.
    pub source: SocketAddr,
    /// Sender-local logical message id recovered from the multipart header.
    pub message_id: MessageId,
    /// Fully reassembled logical payload.
    pub payload: IoPayload,
    /// Total number of payload parts that made up this logical message.
    pub part_count: PartCount,
    /// Whole-message CRC32C recovered from the multipart header.
    pub checksum: Checksum,
}

/// Why a logical outbound send could not make progress through the runtime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UDPourSendFailureReason {
    /// The pure sender state machine rejected the request locally.
    State(UDPourStateFailure),
    /// The runtime could not serialize one frame into egress-managed memory.
    Encode(UDPourEncodeFailure),
    /// `flotsync_io` rejected one UDP datagram belonging to this logical send.
    ///
    /// This runtime intentionally treats the first transport `Nack` as a terminal logical-send
    /// failure. Semantic-delivery acks live above this layer, so a later multipart `Ack` does not
    /// retract the already reported transport failure.
    Transport(SendFailureReason),
}

/// Directed outcome of one outbound UDPour submission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UDPourSubmitResult {
    /// All initial `Payload` datagrams were accepted by the UDP transport.
    Sent { message_id: MessageId },
    /// The logical send failed before or during its initial UDP handoff.
    SendFailed {
        message_id: Option<MessageId>,
        reason: UDPourSendFailureReason,
    },
}

/// Cloneable summary of sender-state-machine failures at the runtime boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UDPourStateFailure {
    ZeroMaxPartPayloadLen,
    TooManyParts {
        payload_len: usize,
        max_part_payload_len: usize,
    },
    InvalidPartCount,
    MessageIdExhausted,
}

/// Cloneable summary of runtime frame-encoding failures at the runtime boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UDPourEncodeFailure {
    Io,
    Bitmap,
    EmptyEncodedFrame,
}

/// Kompact port exported by the UDPour runtime component.
///
/// This port is indication-only: the runtime triggers fully reassembled
/// deliveries upward here, while directed outbound submission uses actor ask on
/// [`UDPourComponentMessage::Submit`].
#[derive(Clone, Copy, Debug)]
pub struct UDPourPort;

impl Port for UDPourPort {
    type Request = Never;
    type Indication = UDPourPortIndication;
}

/// Indications emitted by the UDPour runtime component.
#[derive(Clone, Debug)]
pub enum UDPourPortIndication {
    Deliver(UDPourDeliver),
}

/// Kompact configuration keys that control UDPour runtime pacing.
pub mod config_keys {
    use super::*;

    kompact_config! {
        SEND_DELAY,
        key = "flotsync.udpour.send-delay",
        type = DurationValue,
        default = Duration::ZERO,
        doc = "Minimum spacing between all outbound UDPour datagram attempts.",
        version = "0.1.0"
    }

    kompact_config! {
        BACKPRESSURE_RETRY_DELAY,
        key = "flotsync.udpour.backpressure-retry-delay",
        type = DurationValue,
        default = Duration::from_millis(10),
        doc = "Extra cooldown applied after a UDPour datagram send attempt is Nacked with Backpressure.",
        version = "0.1.0"
    }

    kompact_config! {
        MAX_IN_FLIGHT_DATAGRAMS,
        key = "flotsync.udpour.max-in-flight-datagrams",
        type = UsizeValue,
        default = 1024,
        validate = |value| *value > 0,
        doc = "Maximum number of UDPour datagrams that may be in flight on one socket at once.",
        version = "0.1.0"
    }
}

/// Runtime configuration for the UDPour component.
#[derive(Clone, Debug)]
pub struct UDPourConfig {
    /// Sender-side multipart retention and message-id reuse policy.
    pub sender: SenderConfig,
    /// Receiver-side repair, give-up, and duplicate-suppression policy.
    ///
    /// `delivered_tombstone_timeout` is derived in [`UDPourConfig::new`] from sender retention
    /// and message-id reuse cooldown because the receiver must remember successful deliveries long
    /// enough to suppress late shared-route repairs for the old logical message.
    pub receiver: ReceiverConfig,
    /// Timer cadence used to drive sender retention expiry and receiver repair.
    pub poll_interval: Duration,
}

impl UDPourConfig {
    /// Builds runtime configuration from the sender/receiver state-machine configs.
    pub fn new(
        sender: SenderConfig,
        mut receiver: ReceiverConfig,
    ) -> Result<Self, UDPourConfigError> {
        ensure!(
            sender.max_part_payload_len > 0,
            ZeroMaxPartPayloadLenConfigSnafu
        );
        ensure!(
            receiver.max_need_parts_frame_len > FRAME_HEADER_LEN,
            NeedPartsFrameLenTooSmallSnafu {
                max_need_parts_frame_len: receiver.max_need_parts_frame_len,
            }
        );
        receiver.delivered_tombstone_timeout = sender
            .retention_timeout
            .saturating_add(sender.reuse_cooldown);
        let poll_interval = sender
            .retention_timeout
            .min(receiver.repair_interval)
            .min(receiver.give_up_timeout)
            .min(receiver.delivered_tombstone_timeout);
        ensure!(poll_interval > Duration::ZERO, ZeroPollIntervalSnafu);
        Ok(Self {
            sender,
            receiver,
            poll_interval,
        })
    }
}

/// Configuration errors for the UDPour runtime.
#[derive(Debug, Snafu)]
pub enum UDPourConfigError {
    #[snafu(display("sender max_part_payload_len must be greater than zero"))]
    ZeroMaxPartPayloadLenConfig,
    #[snafu(display("runtime poll interval must be greater than zero"))]
    ZeroPollInterval,
    #[snafu(display(
        "receiver max_need_parts_frame_len {max_need_parts_frame_len} must be larger than the {FRAME_HEADER_LEN}-byte fixed header"
    ))]
    NeedPartsFrameLenTooSmall { max_need_parts_frame_len: usize },
}

/// Runtime pacing policy for outbound UDPour datagrams.
#[derive(Clone, Debug)]
struct UDPourSendRateControl {
    send_delay: Duration,
    backpressure_retry_delay: Duration,
    max_in_flight_datagrams: usize,
}

impl Default for UDPourSendRateControl {
    fn default() -> Self {
        Self {
            send_delay: config_keys::SEND_DELAY
                .default()
                .expect("UDPour send-delay key must define a default"),
            backpressure_retry_delay: config_keys::BACKPRESSURE_RETRY_DELAY
                .default()
                .expect("UDPour backpressure-retry-delay key must define a default"),
            max_in_flight_datagrams: config_keys::MAX_IN_FLIGHT_DATAGRAMS
                .default()
                .expect("UDPour max-in-flight-datagrams key must define a default"),
        }
    }
}

/// Owned outbound scheduling state for one UDPour socket runtime.
#[derive(Debug)]
struct OutboundDispatcherState {
    send_rate: UDPourSendRateControl,
    /// Datagrams still waiting for one physical UDP send attempt.
    queue: VecDeque<QueuedDatagram>,
    /// Datagrams already handed to `flotsync_io` and still awaiting `Ack`/`Nack`.
    in_flight_datagrams: usize,
    /// True while one spawned async task is encoding and dispatching the next queue head.
    dispatch_in_progress: bool,
    /// Earliest time at which the next UDP send attempt may start.
    next_send_allowed_at: Instant,
    /// Timer used to resume dispatch once `next_send_allowed_at` is reached.
    dispatch_timer: Option<DispatchTimerState>,
    /// Monotonic generation used to ignore stale dispatch timer firings.
    next_dispatch_generation: usize,
}

impl OutboundDispatcherState {
    fn new() -> Self {
        Self {
            send_rate: UDPourSendRateControl::default(),
            queue: VecDeque::new(),
            in_flight_datagrams: 0,
            dispatch_in_progress: false,
            next_send_allowed_at: Instant::now(),
            dispatch_timer: None,
            next_dispatch_generation: 1,
        }
    }
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
    udp: RequiredPort<UdpPort>,
    transfer: ProvidedPort<UDPourPort>,
    socket_id: SocketId,
    egress_pool: EgressPool,
    config: UDPourConfig,
    sender: SenderMachine,
    receiver: ReceiverMachine,
    next_transmission_id: TransmissionId,
    outbound: HashMap<MessageId, OutboundTransfer>,
    pending_transmissions: HashMap<TransmissionId, PendingTransmission>,
    dispatcher: OutboundDispatcherState,
    /// Once the underlying UDP socket closes, this runtime never dispatches another datagram.
    ///
    /// A close can race with the async frame-encoding task that was already spawned for the next
    /// queue head. This flag makes that task drop its work instead of reintroducing pending sends
    /// after the runtime has already failed every outstanding submit.
    socket_closed: bool,
    poll_timer: Option<PollTimerState>,
    next_poll_generation: usize,
}

impl UDPourComponent {
    /// Creates one runtime component bound to one already-open UDP socket.
    pub fn new(socket_id: SocketId, egress_pool: EgressPool, config: UDPourConfig) -> Self {
        let sender = SenderMachine::new(config.sender.clone());
        let receiver = ReceiverMachine::new(config.receiver.clone());
        Self {
            ctx: ComponentContext::uninitialised(),
            udp: RequiredPort::uninitialised(),
            transfer: ProvidedPort::uninitialised(),
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
            next_poll_generation: 1,
        }
    }

    fn load_send_rate_control(&self) -> UDPourSendRateControl {
        let defaults = UDPourSendRateControl::default();
        let send_delay = match self.ctx.config().get_or_default(&config_keys::SEND_DELAY) {
            Ok(value) => value,
            Err(error) => {
                warn!(
                    self.log(),
                    "Failed to load UDPour send-delay config from {}: {}. Falling back to {:?}",
                    config_keys::SEND_DELAY.key,
                    error,
                    defaults.send_delay
                );
                defaults.send_delay
            }
        };
        let backpressure_retry_delay = match self
            .ctx
            .config()
            .get_or_default(&config_keys::BACKPRESSURE_RETRY_DELAY)
        {
            Ok(value) => value,
            Err(error) => {
                warn!(
                    self.log(),
                    "Failed to load UDPour backpressure-retry-delay config from {}: {}. Falling back to {:?}",
                    config_keys::BACKPRESSURE_RETRY_DELAY.key,
                    error,
                    defaults.backpressure_retry_delay
                );
                defaults.backpressure_retry_delay
            }
        };
        let max_in_flight_datagrams = match self
            .ctx
            .config()
            .get_or_default(&config_keys::MAX_IN_FLIGHT_DATAGRAMS)
        {
            Ok(value) => value,
            Err(error) => {
                warn!(
                    self.log(),
                    "Failed to load UDPour max-in-flight-datagrams config from {}: {}. Falling back to {}",
                    config_keys::MAX_IN_FLIGHT_DATAGRAMS.key,
                    error,
                    defaults.max_in_flight_datagrams
                );
                defaults.max_in_flight_datagrams
            }
        };
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
        I: IntoIterator<Item = QueuedDatagram>,
    {
        let datagrams: Vec<_> = datagrams.into_iter().collect();
        for datagram in datagrams.into_iter().rev() {
            self.dispatcher.queue.push_front(datagram);
        }
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
        if self.dispatcher.in_flight_datagrams >= self.dispatcher.send_rate.max_in_flight_datagrams
        {
            return;
        }
        if self.dispatcher.queue.is_empty() {
            self.clear_dispatch_timer();
            return;
        }
        let now = Instant::now();
        if now < self.dispatcher.next_send_allowed_at {
            self.set_dispatch_timer(self.dispatcher.next_send_allowed_at - now);
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
                    if let Some(message_id) = datagram.message_id() {
                        async_self.report_send_failure(
                            message_id,
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
                    return Handled::Ok;
                }
            };

            if async_self.socket_closed {
                async_self.dispatcher.dispatch_in_progress = false;
                if let Some(message_id) = datagram.message_id() {
                    async_self.abort_outbound_transfer(
                        message_id,
                        Some(UDPourSendFailureReason::Transport(
                            SendFailureReason::Closed,
                        )),
                    );
                }
                async_self.try_dispatch_outbound();
                return Handled::Ok;
            }

            async_self.pending_transmissions.insert(
                transmission_id,
                PendingTransmission { datagram },
            );
            async_self.dispatcher.in_flight_datagrams += 1;
            async_self.dispatcher.dispatch_in_progress = false;
            async_self.dispatcher.next_send_allowed_at =
                Instant::now() + async_self.dispatcher.send_rate.send_delay;
            async_self.udp.trigger(UdpRequest::Send {
                socket_id,
                transmission_id,
                payload: encoded,
                target: Some(target),
                reply_to,
            });
            async_self.try_dispatch_outbound();
            Handled::Ok
        });
    }

    fn set_dispatch_timer(&mut self, delay: Duration) {
        self.clear_dispatch_timer();
        let generation = self.dispatcher.next_dispatch_generation;
        self.dispatcher.next_dispatch_generation =
            self.dispatcher.next_dispatch_generation.wrapping_add(1);
        let timer = self.schedule_once(delay, move |component, _| {
            component.handle_dispatch_timeout(generation)
        });
        self.dispatcher.dispatch_timer = Some(DispatchTimerState { generation, timer });
    }

    fn clear_dispatch_timer(&mut self) {
        if let Some(timer) = self.dispatcher.dispatch_timer.take() {
            self.cancel_timer(timer.timer);
        }
    }

    fn handle_dispatch_timeout(&mut self, generation: usize) -> Handled {
        let Some(timer) = self.dispatcher.dispatch_timer.take() else {
            return Handled::Ok;
        };
        if timer.generation != generation {
            self.dispatcher.dispatch_timer = Some(timer);
            return Handled::Ok;
        }
        self.try_dispatch_outbound();
        Handled::Ok
    }

    fn handle_submit(&mut self, ask: Ask<UDPourSend, UDPourSubmitResult>) -> Handled {
        let (promise, send) = ask.take();
        if self.socket_closed {
            let _ = promise.fulfil(UDPourSubmitResult::SendFailed {
                message_id: None,
                reason: UDPourSendFailureReason::Transport(SendFailureReason::Closed),
            });
            return Handled::Ok;
        }
        let now = Instant::now();
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
                    message_id: None,
                    reason: UDPourSendFailureReason::State(classify_sender_error(&error)),
                });
            }
        }
        Handled::Ok
    }

    fn handle_udp_indication(&mut self, indication: UdpIndication) -> Handled {
        match indication {
            UdpIndication::Received {
                socket_id,
                source,
                payload,
            } if socket_id == self.socket_id => self.handle_received(source, payload),
            UdpIndication::Closed { socket_id, .. } if socket_id == self.socket_id => {
                self.handle_socket_closed();
                Handled::Ok
            }
            _ => Handled::Ok,
        }
    }

    fn handle_received(&mut self, source: SocketAddr, payload: IoPayload) -> Handled {
        let frame = match decode_frame(payload) {
            Ok(frame) => frame,
            Err(error) => {
                error!(
                    self.log(),
                    "Dropping invalid UDPour frame from {source}: {error}"
                );
                return Handled::Ok;
            }
        };
        let now = Instant::now();

        match frame {
            UDPourFrame::Payload(frame) => match self.receiver.accept_payload(source, frame, now) {
                Ok(actions) => {
                    for action in actions {
                        self.handle_receiver_action(action);
                    }
                }
                Err(error) => {
                    error!(
                        self.log(),
                        "Receiver state machine failed while handling payload from {source}: {error}"
                    );
                }
            },
            UDPourFrame::Ack(frame) => {
                if let Some(action) = self.sender.handle_ack(&frame, now) {
                    self.handle_sender_action(action, Some(source));
                }
            }
            UDPourFrame::NeedParts(frame) => {
                if let Some(action) = self.sender.handle_need_parts(&frame, now) {
                    self.handle_sender_action(action, Some(source));
                }
            }
            UDPourFrame::NoLongerAvailable(frame) => {
                if let Some(action) = self.receiver.accept_no_longer_available(source, frame) {
                    self.handle_receiver_action(action);
                }
            }
        }

        Handled::Ok
    }

    fn handle_send_result(&mut self, result: UdpSendResult) -> Handled {
        match result {
            UdpSendResult::Ack {
                socket_id,
                transmission_id,
            } if socket_id == self.socket_id => {
                let Some(pending) = self.pending_transmissions.remove(&transmission_id) else {
                    return Handled::Ok;
                };
                self.dispatcher.in_flight_datagrams =
                    self.dispatcher.in_flight_datagrams.saturating_sub(1);

                if let Some(message_id) = pending.datagram.message_id() {
                    let mut promise = None;
                    let mut report_sent = false;
                    if let Some(outbound) = self.outbound.get_mut(&message_id)
                        && pending.datagram.counts_towards_sent()
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
                            .mark_initial_send_complete(message_id, Instant::now());
                    }
                    if let Some(promise) = promise {
                        let _ = promise.fulfil(UDPourSubmitResult::Sent { message_id });
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
                    return Handled::Ok;
                };
                self.dispatcher.in_flight_datagrams =
                    self.dispatcher.in_flight_datagrams.saturating_sub(1);
                if reason == SendFailureReason::Backpressure {
                    self.dispatcher.next_send_allowed_at = self
                        .dispatcher
                        .next_send_allowed_at
                        .max(Instant::now() + self.dispatcher.send_rate.backpressure_retry_delay);
                    self.enqueue_front(pending.datagram);
                    return Handled::Ok;
                }
                if let Some(message_id) = pending.datagram.message_id() {
                    self.report_send_failure(
                        message_id,
                        UDPourSendFailureReason::Transport(reason),
                    );
                } else {
                    error!(
                        self.log(),
                        "Dropping UDPour control frame to {} after terminal transport failure: {:?}",
                        pending.datagram.target,
                        reason
                    );
                }
                self.try_dispatch_outbound();
            }
            _ => {}
        }
        Handled::Ok
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
                let mut removed_in_flight = 0usize;
                self.pending_transmissions.retain(|_, pending| {
                    let keep = pending.datagram.message_id() != Some(message_id);
                    if !keep {
                        removed_in_flight += 1;
                    }
                    keep
                });
                self.dispatcher.in_flight_datagrams = self
                    .dispatcher
                    .in_flight_datagrams
                    .saturating_sub(removed_in_flight);
                self.dispatcher
                    .queue
                    .retain(|datagram| datagram.message_id() != Some(message_id));
                self.outbound.remove(&message_id);
                self.try_dispatch_outbound();
            }
        }
    }

    fn handle_receiver_action(&mut self, action: ReceiverAction) {
        match action {
            ReceiverAction::Deliver {
                source,
                message_id,
                payload,
                part_count,
                checksum,
            } => {
                self.transfer
                    .trigger(UDPourPortIndication::Deliver(UDPourDeliver {
                        source,
                        message_id,
                        payload,
                        part_count,
                        checksum,
                    }));
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
            .map(|frame| QueuedDatagram::payload(message_id, target, frame, counts_towards_sent));
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
                message_id: Some(message_id),
                reason: reason.expect("submit promise can only be present when reporting failure"),
            });
        }
        let Some(action) = self.sender.abort_transfer(message_id, Instant::now()) else {
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
        self.dispatcher.in_flight_datagrams = 0;
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
        let now = Instant::now();
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
        let generation = self.next_poll_generation;
        self.next_poll_generation = self.next_poll_generation.wrapping_add(1);
        let timer = self.schedule_once(self.config.poll_interval, move |component, _| {
            component.handle_poll_timeout(generation)
        });
        self.poll_timer = Some(PollTimerState { generation, timer });
    }

    fn clear_poll_timer(&mut self) {
        if let Some(timer) = self.poll_timer.take() {
            self.cancel_timer(timer.timer);
        }
    }

    fn handle_poll_timeout(&mut self, generation: usize) -> Handled {
        let Some(timer) = self.poll_timer.take() else {
            return Handled::Ok;
        };
        if timer.generation != generation {
            self.poll_timer = Some(timer);
            return Handled::Ok;
        }
        self.poll_runtime();
        self.set_poll_timer();
        Handled::Ok
    }
}

impl ComponentLifecycle for UDPourComponent {
    fn on_start(&mut self) -> Handled {
        self.dispatcher.send_rate = self.load_send_rate_control();
        self.dispatcher.next_send_allowed_at = Instant::now();
        self.set_poll_timer();
        Handled::Ok
    }

    fn on_stop(&mut self) -> Handled {
        self.clear_poll_timer();
        self.clear_dispatch_timer();
        Handled::Ok
    }

    fn on_kill(&mut self) -> Handled {
        self.clear_poll_timer();
        self.clear_dispatch_timer();
        Handled::Ok
    }
}

impl Provide<UDPourPort> for UDPourComponent {
    fn handle(&mut self, request: Never) -> Handled {
        match request {}
    }
}

impl Require<UdpPort> for UDPourComponent {
    fn handle(&mut self, indication: UdpIndication) -> Handled {
        self.handle_udp_indication(indication)
    }
}

impl LocalActor for UDPourComponent {
    type Message = UDPourComponentMessage;

    fn receive(&mut self, msg: Self::Message) -> Handled {
        match msg {
            UDPourComponentMessage::Submit(ask) => self.handle_submit(ask),
            UDPourComponentMessage::SendResult(result) => self.handle_send_result(result),
        }
    }
}

impl_local_actor!(UDPourComponent);

#[doc(hidden)]
#[derive(Debug)]
pub enum UDPourComponentMessage {
    /// Directed outbound submission resolved once the initial multipart payload
    /// handoff either succeeded or failed.
    Submit(Ask<UDPourSend, UDPourSubmitResult>),
    /// Internal feedback from `flotsync_io` about one physical UDP send.
    SendResult(UdpSendResult),
}

#[derive(Clone, Debug)]
struct PollTimerState {
    generation: usize,
    timer: ScheduledTimer,
}

#[derive(Clone, Debug)]
struct DispatchTimerState {
    generation: usize,
    timer: ScheduledTimer,
}

#[derive(Debug)]
struct OutboundTransfer {
    target: SocketAddr,
    submit_promise: Option<KPromise<UDPourSubmitResult>>,
    failure_reported: bool,
    sent_reported: bool,
    pending_initial_transmissions: usize,
}

#[derive(Clone, Debug)]
struct QueuedDatagram {
    target: SocketAddr,
    frame: UDPourFrame,
    message_id: Option<MessageId>,
    counts_towards_sent: bool,
}

impl QueuedDatagram {
    fn payload(
        message_id: MessageId,
        target: SocketAddr,
        frame: PayloadFrame,
        counts_towards_sent: bool,
    ) -> Self {
        Self {
            target,
            frame: UDPourFrame::Payload(frame),
            message_id: Some(message_id),
            counts_towards_sent,
        }
    }

    fn control(target: SocketAddr, frame: UDPourFrame) -> Self {
        Self {
            target,
            frame,
            message_id: None,
            counts_towards_sent: false,
        }
    }

    fn frame_type(&self) -> crate::types::FrameType {
        self.frame.header().frame_type
    }

    fn message_id(&self) -> Option<MessageId> {
        self.message_id
    }

    fn counts_towards_sent(&self) -> bool {
        self.counts_towards_sent
    }
}

#[derive(Clone, Debug)]
struct PendingTransmission {
    datagram: QueuedDatagram,
}

/// Pool-backed frame-encoding errors for the UDP runtime.
#[derive(Debug, Snafu)]
pub(crate) enum RuntimeEncodeError {
    #[snafu(display("pool-backed frame encoding failed"))]
    Io { source: flotsync_io::prelude::Error },
    #[snafu(display("failed to encode NeedParts bitmap"))]
    Bitmap { source: RoaringBitmapError },
    #[snafu(display("frame encoding produced no payload"))]
    EmptyEncodedFrame,
}

async fn encode_frame_with_pool(
    egress_pool: &EgressPool,
    frame: &UDPourFrame,
) -> Result<IoPayload, RuntimeEncodeError> {
    let hint = encoded_frame_len(frame);
    let mut writer = egress_pool.writer(Some(hint));
    match frame {
        UDPourFrame::Payload(PayloadFrame { payload, .. }) => {
            {
                let mut reserved = writer
                    .write_with_reserved(FRAME_HEADER_LEN)
                    .await
                    .context(IoSnafu)?;
                frame
                    .header()
                    .encode_into_buf(&mut reserved)
                    .expect("UDPourHeader::encode_into_buf is infallible");
            }
            let adopt_result = writer.adopt_payload(payload.clone()).await;
            match adopt_result {
                Ok(()) => {}
                Err(IoError::SharedIoPayloadOwnership) => {
                    writer.copy_payload(payload).await.context(IoSnafu)?;
                }
                Err(source) => return Err(RuntimeEncodeError::Io { source }),
            }
        }
        UDPourFrame::Ack(AckFrame { .. })
        | UDPourFrame::NoLongerAvailable(NoLongerAvailableFrame { .. }) => {
            let mut reserved = writer
                .write_with_reserved(FRAME_HEADER_LEN)
                .await
                .context(IoSnafu)?;
            frame
                .header()
                .encode_into_buf(&mut reserved)
                .expect("UDPourHeader::encode_into_buf is infallible");
        }
        UDPourFrame::NeedParts(NeedPartsFrame { missing_parts, .. }) => {
            let mut reserved = writer.write_with_reserved(hint).await.context(IoSnafu)?;
            frame
                .header()
                .encode_into_buf(&mut reserved)
                .expect("UDPourHeader::encode_into_buf is infallible");
            missing_parts
                .encode_into_buf(&mut reserved)
                .context(BitmapSnafu)?;
        }
    }
    let payload = writer.finish().context(IoSnafu)?;
    payload.context(EmptyEncodedFrameSnafu)
}

fn classify_sender_error(error: &SenderError) -> UDPourStateFailure {
    match error {
        SenderError::ZeroMaxPartPayloadLen => UDPourStateFailure::ZeroMaxPartPayloadLen,
        SenderError::TooManyParts {
            payload_len,
            max_part_payload_len,
            ..
        } => UDPourStateFailure::TooManyParts {
            payload_len: *payload_len,
            max_part_payload_len: *max_part_payload_len,
        },
        SenderError::InvalidPartCount { .. } => UDPourStateFailure::InvalidPartCount,
        SenderError::MessageIdExhausted => UDPourStateFailure::MessageIdExhausted,
    }
}

fn classify_encode_error(error: &RuntimeEncodeError) -> UDPourEncodeFailure {
    match error {
        RuntimeEncodeError::Io { .. } => UDPourEncodeFailure::Io,
        RuntimeEncodeError::Bitmap { .. } => UDPourEncodeFailure::Bitmap,
        RuntimeEncodeError::EmptyEncodedFrame => UDPourEncodeFailure::EmptyEncodedFrame,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_need_parts_frame_budget_that_cannot_fit_a_header() {
        let sender =
            SenderConfig::new(256, Duration::from_secs(1), Duration::from_secs(1)).unwrap();
        let receiver = ReceiverConfig {
            repair_interval: Duration::from_millis(10),
            give_up_timeout: Duration::from_secs(1),
            max_need_parts_frame_len: FRAME_HEADER_LEN,
            delivered_tombstone_timeout: Duration::ZERO,
        };

        let error = UDPourConfig::new(sender, receiver).unwrap_err();
        assert!(matches!(
            error,
            UDPourConfigError::NeedPartsFrameLenTooSmall {
                max_need_parts_frame_len: FRAME_HEADER_LEN
            }
        ));
    }

    #[test]
    fn rejects_zero_sender_part_payload_len() {
        let sender = SenderConfig {
            max_part_payload_len: 0,
            retention_timeout: Duration::from_secs(1),
            reuse_cooldown: Duration::from_secs(1),
            eager_ack_cleanup: false,
        };
        let receiver = ReceiverConfig {
            repair_interval: Duration::from_millis(10),
            give_up_timeout: Duration::from_secs(1),
            max_need_parts_frame_len: FRAME_HEADER_LEN + 1,
            delivered_tombstone_timeout: Duration::ZERO,
        };

        let error = UDPourConfig::new(sender, receiver).unwrap_err();
        assert!(matches!(
            error,
            UDPourConfigError::ZeroMaxPartPayloadLenConfig
        ));
    }
}
