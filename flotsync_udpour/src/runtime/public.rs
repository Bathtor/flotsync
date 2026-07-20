//! Public `UDPour` runtime messages, failures, port, and configuration.

use super::*;

/// One logical outbound transfer request for the `UDPour` runtime.
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
    /// Local UDP socket that received the final multipart payload part.
    pub socket_id: SocketId,
    /// Forwarded UDP source address that identified the sender at this route.
    pub source: SocketAddr,
    /// Fully reassembled logical payload.
    pub payload: IoPayload,
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

impl fmt::Display for UDPourSendFailureReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::State(reason) => write!(f, "sender state failure: {reason}"),
            Self::Encode(reason) => write!(f, "frame encoding failure: {reason}"),
            Self::Transport(reason) => write!(f, "transport failure: {reason:?}"),
        }
    }
}

/// Directed outcome of one outbound `UDPour` submission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UDPourSubmitResult {
    /// All initial `Payload` datagrams were accepted by the UDP transport.
    Sent,
    /// The logical send failed before or during its initial UDP handoff.
    SendFailed { reason: UDPourSendFailureReason },
}

impl fmt::Display for UDPourSubmitResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sent => write!(f, "initial UDPour payload datagrams were accepted"),
            Self::SendFailed { reason } => write!(f, "UDPour send failed: {reason}"),
        }
    }
}

/// Cloneable summary of sender-state-machine failures at the runtime boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UDPourStateFailure {
    TooManyParts {
        payload_len: usize,
        max_part_payload_len: usize,
    },
    InvalidPartCount,
    MessageIdExhausted,
}

impl fmt::Display for UDPourStateFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyParts {
                payload_len,
                max_part_payload_len,
            } => write!(
                f,
                "payload of {payload_len} bytes exceeds supported multipart split for part size {max_part_payload_len}"
            ),
            Self::InvalidPartCount => write!(f, "sender produced an invalid part count"),
            Self::MessageIdExhausted => {
                write!(f, "all sender message ids are live or cooling down")
            }
        }
    }
}

/// Cloneable summary of runtime frame-encoding failures at the runtime boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UDPourEncodeFailure {
    Io,
    Bitmap,
    EmptyEncodedFrame,
}

impl fmt::Display for UDPourEncodeFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io => write!(f, "egress writer failed while encoding a frame"),
            Self::Bitmap => write!(f, "NeedParts bitmap encoding failed"),
            Self::EmptyEncodedFrame => write!(f, "frame encoding produced an empty payload"),
        }
    }
}

/// Kompact port exported by the `UDPour` runtime component.
///
/// This port is indication-only: the runtime triggers fully reassembled
/// deliveries upward here, while directed outbound submission uses actor ask on
/// [`UDPourComponentMessage::Submit`].
#[derive(Clone, Copy, Debug)]
pub struct UDPourPort;

impl Port for UDPourPort {
    type Request = Never;
    type Indication = UDPourDeliver;
}

/// Kompact configuration keys that control `UDPour` runtime pacing.
pub mod config_keys {
    use super::{Duration, DurationValue, Result, UsizeValue, kompact_config};

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

/// Runtime configuration for the `UDPour` component.
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
    ///
    /// # Errors
    ///
    /// See `UDPourConfigError` for failure conditions.
    pub fn new(
        sender: SenderConfig,
        mut receiver: ReceiverConfig,
    ) -> Result<Self, UDPourConfigError> {
        ensure!(
            receiver.max_need_parts_frame_len > FRAME_HEADER_LEN + MIN_ENCODED_NON_EMPTY_BITMAP_LEN,
            NeedPartsFrameLenTooSmallSnafu {
                max_need_parts_frame_len: receiver.max_need_parts_frame_len,
            }
        );
        receiver.delivered_tombstone_timeout = sender
            .retention_timeout
            .saturating_add(sender.id_reuse_cooldown);
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

impl Default for UDPourConfig {
    fn default() -> Self {
        Self::new(SenderConfig::default(), ReceiverConfig::default())
            .expect("default UDPour runtime config must be valid")
    }
}

/// Configuration errors for the `UDPour` runtime.
#[derive(Debug, Snafu)]
pub enum UDPourConfigError {
    #[snafu(display("runtime poll interval must be greater than zero"))]
    ZeroPollInterval,
    #[snafu(display(
        "receiver max_need_parts_frame_len {max_need_parts_frame_len} must be larger than the {FRAME_HEADER_LEN}-byte fixed header"
    ))]
    NeedPartsFrameLenTooSmall { max_need_parts_frame_len: usize },
}
