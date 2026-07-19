//! Outbound UDPour datagram queue state and error classification.

use super::*;

/// Pool-backed frame-encoding errors for the UDP runtime.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(in crate::runtime)))]
pub(in crate::runtime) enum RuntimeEncodeError {
    #[snafu(display("pool-backed frame encoding failed"))]
    Io { source: flotsync_io::prelude::Error },
    #[snafu(display("failed to encode NeedParts bitmap"))]
    Bitmap { source: RoaringBitmapError },
    #[snafu(display("frame encoding produced no payload"))]
    EmptyEncodedFrame,
}

/// Runtime pacing policy for outbound `UDPour` datagrams.
#[derive(Clone, Debug)]
pub(in crate::runtime) struct UDPourSendRateControl {
    pub(in crate::runtime) send_delay: Duration,
    pub(in crate::runtime) backpressure_retry_delay: Duration,
    pub(in crate::runtime) max_in_flight_datagrams: usize,
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

/// Owned outbound scheduling state for one `UDPour` socket runtime.
#[derive(Debug)]
pub(in crate::runtime) struct OutboundDispatcherState {
    pub(in crate::runtime) send_rate: UDPourSendRateControl,
    /// Datagrams still waiting for one physical UDP send attempt.
    pub(in crate::runtime) queue: VecDeque<QueuedDatagram>,
    /// True while one spawned async task is encoding and dispatching the next queue head.
    pub(in crate::runtime) dispatch_in_progress: bool,
    /// Earliest time at which the next UDP send attempt may start once the component is live.
    ///
    /// This stays unset until `on_start`, because timer-aligned time is only
    /// available once the component has a live Kompact context.
    pub(in crate::runtime) next_send_allowed_at: Option<Instant>,
    /// Timer used to resume dispatch once `next_send_allowed_at` is reached.
    pub(in crate::runtime) dispatch_timer: Option<ScheduledTimer>,
}

impl OutboundDispatcherState {
    pub(in crate::runtime) fn new() -> Self {
        Self {
            send_rate: UDPourSendRateControl::default(),
            queue: VecDeque::new(),
            dispatch_in_progress: false,
            next_send_allowed_at: None,
            dispatch_timer: None,
        }
    }
}

#[doc(hidden)]
#[derive(Debug)]
pub(in crate::runtime) struct OutboundTransfer {
    pub(in crate::runtime) target: SocketAddr,
    pub(in crate::runtime) submit_promise: Option<KPromise<UDPourSubmitResult>>,
    pub(in crate::runtime) failure_reported: bool,
    pub(in crate::runtime) sent_reported: bool,
    pub(in crate::runtime) pending_initial_transmissions: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::runtime) enum QueuedDatagramKind {
    Payload { counts_towards_sent: bool },
    Control,
}

#[derive(Clone, Debug)]
pub(in crate::runtime) struct QueuedDatagram {
    pub(in crate::runtime) target: SocketAddr,
    pub(in crate::runtime) frame: UDPourFrame,
    pub(in crate::runtime) kind: QueuedDatagramKind,
}

impl QueuedDatagram {
    pub(in crate::runtime) fn payload(
        target: SocketAddr,
        frame: PayloadFrame,
        counts_towards_sent: bool,
    ) -> Self {
        Self {
            target,
            frame: UDPourFrame::Payload(frame),
            kind: QueuedDatagramKind::Payload {
                counts_towards_sent,
            },
        }
    }

    pub(in crate::runtime) fn control(target: SocketAddr, frame: UDPourFrame) -> Self {
        Self {
            target,
            frame,
            kind: QueuedDatagramKind::Control,
        }
    }

    pub(in crate::runtime) fn frame_type(&self) -> crate::types::FrameType {
        self.frame.header().frame_type
    }

    pub(in crate::runtime) fn message_id(&self) -> MessageId {
        self.frame.header().message_id
    }

    pub(in crate::runtime) fn counts_towards_sent(&self) -> bool {
        matches!(
            self.kind,
            QueuedDatagramKind::Payload {
                counts_towards_sent: true
            }
        )
    }

    /// Returns whether this queued datagram is one sender-originated payload frame that belongs
    /// to a logical outbound submit tracked in `outbound`.
    pub(in crate::runtime) fn is_outbound_payload_datagram(&self) -> bool {
        matches!(self.kind, QueuedDatagramKind::Payload { .. })
    }

    /// Returns whether this queued datagram is one outbound payload frame for the given logical
    /// sender `message_id`.
    ///
    /// Control traffic also carries a `message_id`, but that only correlates protocol state. It
    /// does not mean the control datagram should be purged together with the sender-side lifetime
    /// of that logical outbound submit.
    pub(in crate::runtime) fn is_outbound_payload_for(&self, message_id: MessageId) -> bool {
        self.is_outbound_payload_datagram() && self.message_id() == message_id
    }
}

pub(in crate::runtime) async fn encode_frame_with_pool(
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
            // The queued payload is usually already owned by this same egress pool, so adoption
            // still succeeds here despite the clone. When that is not true, the writer cannot take
            // ownership of the shared payload and we must copy the readable bytes instead.
            let adopt_result = writer.adopt_payload(payload.clone()).await;
            match adopt_result {
                Ok(()) => {}
                Err(Error::SharedIoPayloadOwnership) => {
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

pub(in crate::runtime) fn classify_sender_error(error: &SenderError) -> UDPourStateFailure {
    match error {
        SenderError::TooManyParts {
            payload_len,
            max_part_payload_len,
            ..
        } => UDPourStateFailure::TooManyParts {
            payload_len: *payload_len,
            max_part_payload_len: *max_part_payload_len,
        },
        SenderError::InvalidPartCount { .. } | SenderError::RequestedPartOutOfRange { .. } => {
            UDPourStateFailure::InvalidPartCount
        }
        SenderError::MessageIdExhausted => UDPourStateFailure::MessageIdExhausted,
    }
}

pub(in crate::runtime) fn classify_encode_error(error: &RuntimeEncodeError) -> UDPourEncodeFailure {
    match error {
        RuntimeEncodeError::Io { .. } => UDPourEncodeFailure::Io,
        RuntimeEncodeError::Bitmap { .. } => UDPourEncodeFailure::Bitmap,
        RuntimeEncodeError::EmptyEncodedFrame => UDPourEncodeFailure::EmptyEncodedFrame,
    }
}
