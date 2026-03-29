//! Kompact runtime adapter for the datagram multipart protocol.
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
        DatagramFrame,
        MessageId,
        NeedPartsFrame,
        NoLongerAvailableFrame,
        PartCount,
        PayloadFrame,
    },
};
use flotsync_io::prelude::{
    EgressPool,
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

/// One logical outbound transfer request for the UDP multipart runtime.
#[derive(Clone, Debug)]
pub struct DatagramTransferSend {
    /// Caller-owned correlation id for this logical send.
    pub send_ref: DatagramTransferSendRef,
    /// Route-level UDP target that should receive every `Payload` retransmission.
    pub target: SocketAddr,
    /// Full logical payload to split into multipart `Payload` frames.
    pub payload: IoPayload,
}

/// Caller-owned correlation id for one logical send.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DatagramTransferSendRef(pub u64);

/// One fully reassembled inbound logical message.
#[derive(Clone, Debug)]
pub struct DatagramTransferDeliver {
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

/// One logical outbound send failure surfaced back to the runtime user.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatagramTransferSendFailed {
    /// Caller-owned correlation id from the original [`DatagramTransferSend`].
    pub send_ref: DatagramTransferSendRef,
    /// Multipart `message_id` once the sender state machine had allocated one.
    ///
    /// State-machine failures raised before allocation leave this as `None`.
    pub message_id: Option<MessageId>,
    /// Route-transport failure classification for this logical send.
    pub reason: DatagramTransferSendFailureReason,
}

/// Why a logical outbound send could not make progress through the runtime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DatagramTransferSendFailureReason {
    /// The pure sender state machine rejected the request locally.
    State(DatagramTransferStateFailure),
    /// The runtime could not serialize one frame into egress-managed memory.
    Encode(DatagramTransferEncodeFailure),
    /// `flotsync_io` rejected one UDP datagram belonging to this logical send.
    ///
    /// This runtime intentionally treats the first transport `Nack` as a terminal logical-send
    /// failure. Semantic-delivery acks live above this layer, so a later multipart `Ack` does not
    /// retract the already reported transport failure.
    Transport(SendFailureReason),
}

/// Cloneable summary of sender-state-machine failures at the runtime boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DatagramTransferStateFailure {
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
pub enum DatagramTransferEncodeFailure {
    Io,
    Bitmap,
    EmptyEncodedFrame,
}

/// Kompact port exported by the UDP multipart runtime component.
#[derive(Clone, Copy, Debug)]
pub struct DatagramTransferPort;

impl Port for DatagramTransferPort {
    type Request = DatagramTransferPortRequest;
    type Indication = DatagramTransferPortIndication;
}

/// Requests accepted by the UDP multipart runtime component.
#[derive(Clone, Debug)]
pub enum DatagramTransferPortRequest {
    Send(DatagramTransferSend),
}

/// Indications emitted by the UDP multipart runtime component.
#[derive(Clone, Debug)]
pub enum DatagramTransferPortIndication {
    Deliver(DatagramTransferDeliver),
    SendFailed(DatagramTransferSendFailed),
}

/// Runtime configuration for the UDP multipart component.
#[derive(Clone, Debug)]
pub struct DatagramTransferConfig {
    /// Sender-side multipart retention and message-id reuse policy.
    pub sender: SenderConfig,
    /// Receiver-side repair and give-up policy.
    pub receiver: ReceiverConfig,
    /// Timer cadence used to drive sender retention expiry and receiver repair.
    pub poll_interval: Duration,
}

impl DatagramTransferConfig {
    /// Builds runtime configuration from the sender/receiver state-machine configs.
    pub fn new(
        sender: SenderConfig,
        receiver: ReceiverConfig,
    ) -> Result<Self, DatagramTransferConfigError> {
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

/// Configuration errors for the UDP multipart runtime.
#[derive(Debug, Snafu)]
pub enum DatagramTransferConfigError {
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
pub struct DatagramTransferComponent {
    ctx: ComponentContext<Self>,
    udp: RequiredPort<UdpPort>,
    transfer: ProvidedPort<DatagramTransferPort>,
    socket_id: SocketId,
    egress_pool: EgressPool,
    config: DatagramTransferConfig,
    sender: SenderMachine,
    receiver: ReceiverMachine,
    next_transmission_id: TransmissionId,
    outbound: HashMap<MessageId, OutboundTransfer>,
    pending_transmissions: HashMap<TransmissionId, MessageId>,
    poll_timer: Option<PollTimerState>,
    next_poll_generation: usize,
}

impl DatagramTransferComponent {
    /// Creates one runtime component bound to one already-open UDP socket.
    pub fn new(
        socket_id: SocketId,
        egress_pool: EgressPool,
        config: DatagramTransferConfig,
    ) -> Self {
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

    fn handle_request(&mut self, request: DatagramTransferPortRequest) -> Handled {
        match request {
            DatagramTransferPortRequest::Send(send) => {
                let now = Instant::now();
                match self.sender.start_transfer(send.payload, now) {
                    Ok(SenderAction::SendPayloads { message_id, frames }) => {
                        self.outbound.insert(
                            message_id,
                            OutboundTransfer {
                                send_ref: send.send_ref,
                                target: send.target,
                                failure_reported: false,
                            },
                        );
                        self.dispatch_payload_frames(message_id, frames);
                    }
                    Ok(other) => {
                        debug_assert!(
                            false,
                            "start_transfer returned unexpected action: {other:?}"
                        );
                    }
                    Err(error) => {
                        self.transfer
                            .trigger(DatagramTransferPortIndication::SendFailed(
                                DatagramTransferSendFailed {
                                    send_ref: send.send_ref,
                                    message_id: None,
                                    reason: DatagramTransferSendFailureReason::State(
                                        classify_sender_error(&error),
                                    ),
                                },
                            ));
                    }
                }
                Handled::Ok
            }
        }
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
                    "Dropping invalid datagram-transfer frame from {source}: {error}"
                );
                return Handled::Ok;
            }
        };
        let now = Instant::now();

        match frame {
            DatagramFrame::Payload(frame) => match self.receiver.accept_payload(source, frame, now)
            {
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
            DatagramFrame::Ack(frame) => {
                if let Some(action) = self.sender.handle_ack(&frame, now) {
                    self.handle_sender_action(action, Some(source));
                }
            }
            DatagramFrame::NeedParts(frame) => {
                if let Some(action) = self.sender.handle_need_parts(&frame, now) {
                    self.handle_sender_action(action, Some(source));
                }
            }
            DatagramFrame::NoLongerAvailable(frame) => {
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
                self.pending_transmissions.remove(&transmission_id);
            }
            UdpSendResult::Nack {
                socket_id,
                transmission_id,
                reason,
            } if socket_id == self.socket_id => {
                let Some(message_id) = self.pending_transmissions.remove(&transmission_id) else {
                    return Handled::Ok;
                };
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
                self.transfer
                    .trigger(DatagramTransferPortIndication::SendFailed(
                        DatagramTransferSendFailed {
                            send_ref: outbound.send_ref,
                            message_id: Some(message_id),
                            reason: DatagramTransferSendFailureReason::Transport(reason),
                        },
                    ));
            }
            _ => {}
        }
        Handled::Ok
    }

    fn handle_sender_action(&mut self, action: SenderAction, source: Option<SocketAddr>) {
        match action {
            SenderAction::SendPayloads { message_id, frames } => {
                self.dispatch_payload_frames(message_id, frames);
            }
            SenderAction::SendNoLongerAvailable { frame } => {
                if let Some(target) = source {
                    self.dispatch_control_frame(target, DatagramFrame::NoLongerAvailable(frame));
                }
            }
            SenderAction::ObservedAck { .. } => {}
            SenderAction::Purged { message_id, .. } => {
                self.pending_transmissions
                    .retain(|_, pending_message_id| *pending_message_id != message_id);
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
                    .trigger(DatagramTransferPortIndication::Deliver(
                        DatagramTransferDeliver {
                            source,
                            message_id,
                            payload,
                            part_count,
                            checksum,
                        },
                    ));
            }
            ReceiverAction::SendAck { source, frame } => {
                self.dispatch_control_frame(source, DatagramFrame::Ack(frame));
            }
            ReceiverAction::SendNeedParts { source, frames } => {
                for frame in frames {
                    self.dispatch_control_frame(source, DatagramFrame::NeedParts(frame));
                }
            }
            ReceiverAction::Purged { key, reason } => {
                debug!(
                    self.log(),
                    "Purged inbound datagram-transfer state for source={} message_id={:?}: {:?}",
                    key.source,
                    key.message_id,
                    reason
                );
            }
        }
    }

    fn dispatch_payload_frames(&mut self, message_id: MessageId, frames: Vec<PayloadFrame>) {
        let Some(outbound) = self.outbound.get(&message_id) else {
            return;
        };
        let target = outbound.target;
        for frame in frames {
            self.dispatch_outbound_frame(message_id, target, DatagramFrame::Payload(frame));
        }
    }

    fn dispatch_control_frame(&mut self, target: SocketAddr, frame: DatagramFrame) {
        self.spawn_encoded_send(target, frame, None);
    }

    fn dispatch_outbound_frame(
        &mut self,
        message_id: MessageId,
        target: SocketAddr,
        frame: DatagramFrame,
    ) {
        self.spawn_encoded_send(target, frame, Some(message_id));
    }

    fn spawn_encoded_send(
        &mut self,
        target: SocketAddr,
        frame: DatagramFrame,
        message_id: Option<MessageId>,
    ) {
        // TODO(flotsync-szl): Replace the spawn_local-per-frame send path with one owned
        // outbound queue/dispatcher so large multipart sends do not translate into one task per
        // datagram.
        let socket_id = self.socket_id;
        let transmission_id = self.next_transmission_id.take_next();
        let egress_pool = self.egress_pool.clone();
        let reply_to = self
            .actor_ref()
            .recipient_with(DatagramTransferComponentMessage::SendResult);
        self.spawn_local(move |mut async_self| async move {
            let frame_type = frame.header().frame_type;
            let encoded = match encode_frame_with_pool(&egress_pool, &frame).await {
                Ok(encoded) => encoded,
                Err(error) => {
                    if let Some(message_id) = message_id {
                        async_self.report_send_failure(
                            message_id,
                            DatagramTransferSendFailureReason::Encode(classify_encode_error(
                                &error,
                            )),
                        );
                    } else {
                        error!(
                            async_self.log(),
                            "Failed to encode {frame_type:?} control frame for datagram-transfer send to {target}: {error}"
                        );
                    }
                    return Handled::Ok;
                }
            };
            if let Some(message_id) = message_id {
                async_self
                    .pending_transmissions
                    .insert(transmission_id, message_id);
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

    fn report_send_failure(
        &mut self,
        message_id: MessageId,
        reason: DatagramTransferSendFailureReason,
    ) {
        let Some(outbound) = self.outbound.get_mut(&message_id) else {
            return;
        };
        if outbound.failure_reported {
            return;
        }
        outbound.failure_reported = true;
        self.transfer
            .trigger(DatagramTransferPortIndication::SendFailed(
                DatagramTransferSendFailed {
                    send_ref: outbound.send_ref,
                    message_id: Some(message_id),
                    reason,
                },
            ));
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
                    "Receiver timeout processing failed in datagram-transfer runtime: {error}"
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

impl ComponentLifecycle for DatagramTransferComponent {
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

impl Provide<DatagramTransferPort> for DatagramTransferComponent {
    fn handle(&mut self, request: DatagramTransferPortRequest) -> Handled {
        self.handle_request(request)
    }
}

impl Require<UdpPort> for DatagramTransferComponent {
    fn handle(&mut self, indication: UdpIndication) -> Handled {
        self.handle_udp_indication(indication)
    }
}

impl LocalActor for DatagramTransferComponent {
    type Message = DatagramTransferComponentMessage;

    fn receive(&mut self, msg: Self::Message) -> Handled {
        match msg {
            DatagramTransferComponentMessage::SendResult(result) => self.handle_send_result(result),
        }
    }
}

impl_local_actor!(DatagramTransferComponent);

#[doc(hidden)]
#[derive(Clone, Debug)]
pub enum DatagramTransferComponentMessage {
    SendResult(UdpSendResult),
}

#[derive(Clone, Debug)]
struct PollTimerState {
    generation: usize,
    timer: ScheduledTimer,
}

#[derive(Clone, Debug)]
struct OutboundTransfer {
    send_ref: DatagramTransferSendRef,
    target: SocketAddr,
    failure_reported: bool,
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
    frame: &DatagramFrame,
) -> Result<IoPayload, RuntimeEncodeError> {
    let hint = encoded_frame_len(frame);
    let mut writer = egress_pool.writer(Some(hint));
    match frame {
        DatagramFrame::Payload(PayloadFrame { payload, .. }) => {
            {
                let mut reserved = writer
                    .write_with_reserved(FRAME_HEADER_LEN)
                    .await
                    .context(IoSnafu)?;
                encode_header_into(frame.header(), &mut reserved);
            }
            writer
                .adopt_payload(payload.clone())
                .await
                .context(IoSnafu)?;
        }
        DatagramFrame::Ack(AckFrame { .. })
        | DatagramFrame::NoLongerAvailable(NoLongerAvailableFrame { .. }) => {
            let mut reserved = writer
                .write_with_reserved(FRAME_HEADER_LEN)
                .await
                .context(IoSnafu)?;
            encode_header_into(frame.header(), &mut reserved);
        }
        DatagramFrame::NeedParts(NeedPartsFrame { missing_parts, .. }) => {
            let mut reserved = writer.write_with_reserved(hint).await.context(IoSnafu)?;
            encode_header_into(frame.header(), &mut reserved);
            encode_need_parts_body_into(missing_parts, &mut reserved).context(BitmapSnafu)?;
        }
    }
    let payload = writer.finish().context(IoSnafu)?;
    payload.context(EmptyEncodedFrameSnafu)
}

fn classify_sender_error(error: &SenderError) -> DatagramTransferStateFailure {
    match error {
        SenderError::ZeroMaxPartPayloadLen => DatagramTransferStateFailure::ZeroMaxPartPayloadLen,
        SenderError::TooManyParts {
            payload_len,
            max_part_payload_len,
            ..
        } => DatagramTransferStateFailure::TooManyParts {
            payload_len: *payload_len,
            max_part_payload_len: *max_part_payload_len,
        },
        SenderError::InvalidPartCount { .. } => DatagramTransferStateFailure::InvalidPartCount,
        SenderError::MessageIdExhausted => DatagramTransferStateFailure::MessageIdExhausted,
    }
}

fn classify_encode_error(error: &RuntimeEncodeError) -> DatagramTransferEncodeFailure {
    match error {
        RuntimeEncodeError::Io { .. } => DatagramTransferEncodeFailure::Io,
        RuntimeEncodeError::Bitmap { .. } => DatagramTransferEncodeFailure::Bitmap,
        RuntimeEncodeError::EmptyEncodedFrame => DatagramTransferEncodeFailure::EmptyEncodedFrame,
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

        let error = DatagramTransferConfig::new(sender, receiver).unwrap_err();
        assert!(matches!(
            error,
            DatagramTransferConfigError::NeedPartsFrameLenTooSmall {
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

        let error = DatagramTransferConfig::new(sender, receiver).unwrap_err();
        assert!(matches!(
            error,
            DatagramTransferConfigError::ZeroMaxPartPayloadLenConfig
        ));
    }
}
