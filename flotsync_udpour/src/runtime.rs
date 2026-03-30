//! Kompact runtime adapter for the UDPour protocol.
//!
//! This module binds the pure sender/receiver state machines to one shared UDP
//! socket from `flotsync_io`, owns the mapping from logical multipart messages
//! to concrete UDP sends, and surfaces route-transport failures at the
//! component boundary.

use crate::{
    codec::{
        FRAME_HEADER_LEN,
        decode_frame,
        encode_header_into,
        encode_need_parts_body_into,
        encoded_frame_len,
    },
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
use kompact::prelude::*;
use snafu::prelude::*;
use std::{
    collections::HashMap,
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

/// Runtime configuration for the UDPour component.
#[derive(Clone, Debug)]
pub struct UDPourConfig {
    /// Sender-side multipart retention and message-id reuse policy.
    pub sender: SenderConfig,
    /// Receiver-side repair and give-up policy.
    pub receiver: ReceiverConfig,
    /// Timer cadence used to drive sender retention expiry and receiver repair.
    pub poll_interval: Duration,
}

impl UDPourConfig {
    /// Builds runtime configuration from the sender/receiver state-machine configs.
    pub fn new(sender: SenderConfig, receiver: ReceiverConfig) -> Result<Self, UDPourConfigError> {
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
        let poll_interval = sender
            .retention_timeout
            .min(receiver.repair_interval)
            .min(receiver.give_up_timeout);
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
            poll_timer: None,
            next_poll_generation: 1,
        }
    }

    fn handle_submit(&mut self, ask: Ask<UDPourSend, UDPourSubmitResult>) -> Handled {
        let (promise, send) = ask.take();
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
                self.clear_poll_timer();
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
                let Some(outbound) = self.outbound.get_mut(&pending.message_id) else {
                    return Handled::Ok;
                };
                if !pending.counts_towards_sent
                    || outbound.failure_reported
                    || outbound.sent_reported
                {
                    return Handled::Ok;
                }
                outbound.pending_initial_transmissions =
                    outbound.pending_initial_transmissions.saturating_sub(1);
                if outbound.pending_initial_transmissions == 0 {
                    outbound.sent_reported = true;
                    if let Some(promise) = outbound.submit_promise.take() {
                        let _ = promise.fulfil(UDPourSubmitResult::Sent {
                            message_id: pending.message_id,
                        });
                    }
                }
            }
            UdpSendResult::Nack {
                socket_id,
                transmission_id,
                reason,
            } if socket_id == self.socket_id => {
                let Some(pending) = self.pending_transmissions.remove(&transmission_id) else {
                    return Handled::Ok;
                };
                let message_id = pending.message_id;
                let Some(outbound) = self.outbound.get_mut(&message_id) else {
                    return Handled::Ok;
                };
                if outbound.failure_reported {
                    return Handled::Ok;
                }
                // Transport failure is terminal at the route-transport layer. Higher-level
                // semantic delivery is responsible for deciding whether later multipart acks or
                // retries change the overall message outcome.
                if reason == SendFailureReason::Backpressure {
                    // TODO(flotsync-ml0): Backpressure should feed a bounded send-rate controller
                    // rather than surfacing as an immediate logical-send failure.
                }
                outbound.failure_reported = true;
                if let Some(promise) = outbound.submit_promise.take() {
                    let _ = promise.fulfil(UDPourSubmitResult::SendFailed {
                        message_id: Some(message_id),
                        reason: UDPourSendFailureReason::Transport(reason),
                    });
                }
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
            SenderAction::ObservedAck { .. } => {}
            SenderAction::Purged { message_id, .. } => {
                self.pending_transmissions
                    .retain(|_, pending| pending.message_id != message_id);
                self.outbound.remove(&message_id);
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
            ReceiverAction::SendNeedParts { source, frames } => {
                for frame in frames {
                    self.dispatch_control_frame(source, UDPourFrame::NeedParts(frame));
                }
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
        for frame in frames {
            self.dispatch_outbound_frame(
                message_id,
                target,
                UDPourFrame::Payload(frame),
                counts_towards_sent,
            );
        }
    }

    fn dispatch_control_frame(&mut self, target: SocketAddr, frame: UDPourFrame) {
        self.spawn_encoded_send(target, frame, None);
    }

    fn dispatch_outbound_frame(
        &mut self,
        message_id: MessageId,
        target: SocketAddr,
        frame: UDPourFrame,
        counts_towards_sent: bool,
    ) {
        self.spawn_encoded_send(target, frame, Some((message_id, counts_towards_sent)));
    }

    fn spawn_encoded_send(
        &mut self,
        target: SocketAddr,
        frame: UDPourFrame,
        message_id: Option<(MessageId, bool)>,
    ) {
        // TODO(flotsync-szl): Replace the spawn_local-per-frame send path with one owned
        // outbound queue/dispatcher so large multipart sends do not translate into one task per
        // datagram.
        let socket_id = self.socket_id;
        let transmission_id = self.next_transmission_id.take_next();
        let egress_pool = self.egress_pool.clone();
        let reply_to = self
            .actor_ref()
            .recipient_with(UDPourComponentMessage::SendResult);
        self.spawn_local(move |mut async_self| async move {
            let frame_type = frame.header().frame_type;
            let encoded = match encode_frame_with_pool(&egress_pool, &frame).await {
                Ok(encoded) => encoded,
                Err(error) => {
                    if let Some((message_id, _)) = message_id {
                        async_self.report_send_failure(
                            message_id,
                            UDPourSendFailureReason::Encode(classify_encode_error(
                                &error,
                            )),
                        );
                    } else {
                        error!(
                            async_self.log(),
                            "Failed to encode {frame_type:?} control frame for UDPour send to {target}: {error}"
                        );
                    }
                    return Handled::Ok;
                }
            };
            if let Some((message_id, counts_towards_sent)) = message_id {
                async_self.pending_transmissions.insert(
                    transmission_id,
                    PendingTransmission {
                        message_id,
                        counts_towards_sent,
                    },
                );
            }
            async_self.udp.trigger(UdpRequest::Send {
                socket_id,
                transmission_id,
                payload: encoded,
                target: Some(target),
                reply_to,
            });
            Handled::Ok
        });
    }

    fn report_send_failure(&mut self, message_id: MessageId, reason: UDPourSendFailureReason) {
        let Some(outbound) = self.outbound.get_mut(&message_id) else {
            return;
        };
        if outbound.failure_reported {
            return;
        }
        outbound.failure_reported = true;
        if let Some(promise) = outbound.submit_promise.take() {
            let _ = promise.fulfil(UDPourSubmitResult::SendFailed {
                message_id: Some(message_id),
                reason,
            });
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

    fn arm_poll_timer(&mut self) {
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
        self.arm_poll_timer();
        Handled::Ok
    }
}

impl ComponentLifecycle for UDPourComponent {
    fn on_start(&mut self) -> Handled {
        self.arm_poll_timer();
        Handled::Ok
    }

    fn on_stop(&mut self) -> Handled {
        self.clear_poll_timer();
        Handled::Ok
    }

    fn on_kill(&mut self) -> Handled {
        self.clear_poll_timer();
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

#[derive(Debug)]
struct OutboundTransfer {
    target: SocketAddr,
    submit_promise: Option<KPromise<UDPourSubmitResult>>,
    failure_reported: bool,
    sent_reported: bool,
    pending_initial_transmissions: usize,
}

#[derive(Clone, Copy, Debug)]
struct PendingTransmission {
    message_id: MessageId,
    counts_towards_sent: bool,
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
                encode_header_into(frame.header(), &mut reserved);
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
            encode_header_into(frame.header(), &mut reserved);
        }
        UDPourFrame::NeedParts(NeedPartsFrame { missing_parts, .. }) => {
            let mut reserved = writer.write_with_reserved(hint).await.context(IoSnafu)?;
            encode_header_into(frame.header(), &mut reserved);
            encode_need_parts_body_into(missing_parts, &mut reserved).context(BitmapSnafu)?;
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
        };

        let error = UDPourConfig::new(sender, receiver).unwrap_err();
        assert!(matches!(
            error,
            UDPourConfigError::ZeroMaxPartPayloadLenConfig
        ));
    }
}
