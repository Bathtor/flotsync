//! Sender-side multipart state machine.
//!
//! This module owns sender-local `message_id` allocation, bounded retention of
//! original payload parts, and late repair behavior.

use crate::types::{
    AckFrame,
    Checksum,
    FrameFlags,
    FrameType,
    MessageId,
    NeedPartsFrame,
    NoLongerAvailableFrame,
    PartCount,
    PartNumber,
    PayloadFrame,
    UDPourHeader,
};
use bytes::Buf;
use flotsync_io::prelude::IoPayload;
use smallvec::SmallVec;
use snafu::prelude::*;
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

/// Sender-side runtime configuration.
#[derive(Clone, Debug)]
pub struct SenderConfig {
    /// Maximum payload bytes carried by one non-final `Payload` frame.
    pub max_part_payload_len: usize,
    /// How long the sender retains parts for repair after the initial send.
    pub retention_timeout: Duration,
    /// How long a purged `message_id` must stay out of circulation before reuse.
    pub reuse_cooldown: Duration,
    /// Whether one observed `Ack` should immediately purge the retained message.
    pub eager_ack_cleanup: bool,
}

impl SenderConfig {
    /// Basic sender configuration with shared-route-safe defaults.
    #[cfg(test)]
    pub(crate) fn new(
        max_part_payload_len: usize,
        retention_timeout: Duration,
        reuse_cooldown: Duration,
    ) -> Result<Self, SenderError> {
        ensure!(max_part_payload_len > 0, ZeroMaxPartPayloadLenSnafu);
        Ok(Self {
            max_part_payload_len,
            retention_timeout,
            reuse_cooldown,
            eager_ack_cleanup: false,
        })
    }
}

/// One sender-owned multiplexed multipart state machine.
#[derive(Debug)]
pub(crate) struct SenderMachine {
    config: SenderConfig,
    next_message_id: u32,
    live: HashMap<MessageId, LiveMessage>,
    cooldown: HashMap<MessageId, Instant>,
}

/// Sender timeout polling only emits purge actions for expired retained messages.
///
/// The per-tick action list is therefore usually tiny even though the individual
/// `SendPayloads` action can still carry a large payload-frame `Vec`.
type SenderActions = SmallVec<[SenderAction; 2]>;

impl SenderMachine {
    /// Creates one new sender state machine.
    pub fn new(config: SenderConfig) -> Self {
        Self::with_initial_message_id(config, MessageId(0))
    }

    /// Creates one new sender state machine with an explicit initial id.
    pub fn with_initial_message_id(config: SenderConfig, next_message_id: MessageId) -> Self {
        Self {
            config,
            next_message_id: next_message_id.0,
            live: HashMap::new(),
            cooldown: HashMap::new(),
        }
    }

    /// Starts one new multipart transfer and returns the initial payload frames.
    pub fn start_transfer(
        &mut self,
        payload: IoPayload,
        now: Instant,
    ) -> Result<SenderAction, SenderError> {
        self.collect_expired_cooldown(now);
        let message_id = self.allocate_message_id(now)?;
        let checksum = checksum_payload(&payload);
        let part_count = part_count_for_payload(payload.len(), self.config.max_part_payload_len)?;
        let live_message = LiveMessage {
            payload,
            checksum,
            part_count,
            expires_at: None,
        };
        // TODO(flotsync-2hd): Stream payload frames directly into the runtime send path instead
        // of materializing one Vec<PayloadFrame> per logical send.
        let frames = live_message.all_payload_frames(message_id, self.config.max_part_payload_len);
        self.live.insert(message_id, live_message);
        Ok(SenderAction::SendPayloads { message_id, frames })
    }

    /// Starts the retained-state timeout once the initial multipart send has fully completed.
    pub fn mark_initial_send_complete(&mut self, message_id: MessageId, now: Instant) {
        let Some(live) = self.live.get_mut(&message_id) else {
            return;
        };
        live.expires_at = Some(now + self.config.retention_timeout);
    }

    /// Handles one receiver acknowledgment.
    pub fn handle_ack(&mut self, ack: &AckFrame, now: Instant) -> Option<SenderAction> {
        self.collect_expired_cooldown(now);
        let live = self.live.get(&ack.header.message_id)?;
        if !live.matches(ack.header.part_count, ack.header.checksum) {
            return None;
        }
        if self.config.eager_ack_cleanup {
            return self.purge_message(ack.header.message_id, now, SenderPurgeReason::Acked);
        }
        Some(SenderAction::ObservedAck {
            message_id: ack.header.message_id,
        })
    }

    /// Handles one receiver repair request.
    pub fn handle_need_parts(
        &mut self,
        need_parts: &NeedPartsFrame,
        now: Instant,
    ) -> Option<SenderAction> {
        self.collect_expired_cooldown(now);
        let Some(live) = self.live.get(&need_parts.header.message_id) else {
            return Some(SenderAction::SendNoLongerAvailable {
                frame: no_longer_available_frame(need_parts.header),
            });
        };
        if !live.matches(need_parts.header.part_count, need_parts.header.checksum) {
            return Some(SenderAction::SendNoLongerAvailable {
                frame: no_longer_available_frame(need_parts.header),
            });
        }

        let frames = live.payload_frames_for_missing_parts(
            need_parts.header.message_id,
            self.config.max_part_payload_len,
            &need_parts.missing_parts,
        );
        Some(SenderAction::SendPayloads {
            message_id: need_parts.header.message_id,
            frames,
        })
    }

    /// Purges expired retained messages and moves their ids into cooldown.
    pub fn poll_timeouts(&mut self, now: Instant) -> SenderActions {
        self.collect_expired_cooldown(now);
        let expired_ids: Vec<_> = self
            .live
            .iter()
            .filter_map(|(message_id, live)| {
                live.expires_at
                    .filter(|expires_at| *expires_at <= now)
                    .map(|_| *message_id)
            })
            .collect();
        let mut actions = SenderActions::new();
        for message_id in expired_ids {
            if let Some(action) =
                self.purge_message(message_id, now, SenderPurgeReason::RetentionExpired)
            {
                actions.push(action);
            }
        }
        actions
    }

    /// Aborts one live logical message and moves its `message_id` into cooldown immediately.
    pub fn abort_transfer(&mut self, message_id: MessageId, now: Instant) -> Option<SenderAction> {
        self.collect_expired_cooldown(now);
        self.purge_message(message_id, now, SenderPurgeReason::Aborted)
    }

    fn allocate_message_id(&mut self, now: Instant) -> Result<MessageId, SenderError> {
        for _ in 0..=u32::MAX {
            let candidate = MessageId(self.next_message_id);
            self.next_message_id = self.next_message_id.wrapping_add(1);
            if self.live.contains_key(&candidate) {
                continue;
            }
            if let Some(cooldown_expires_at) = self.cooldown.get(&candidate)
                && *cooldown_expires_at > now
            {
                continue;
            }
            return Ok(candidate);
        }
        MessageIdExhaustedSnafu.fail()
    }

    fn collect_expired_cooldown(&mut self, now: Instant) {
        self.cooldown.retain(|_, expires_at| *expires_at > now);
    }

    fn purge_message(
        &mut self,
        message_id: MessageId,
        now: Instant,
        reason: SenderPurgeReason,
    ) -> Option<SenderAction> {
        self.live.remove(&message_id)?;
        self.cooldown
            .insert(message_id, now + self.config.reuse_cooldown);
        Some(SenderAction::Purged { message_id, reason })
    }
}

/// Sender-facing outcomes from one state-machine interaction.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SenderAction {
    /// Emit these payload frames on the selected UDP route.
    SendPayloads {
        message_id: MessageId,
        frames: Vec<PayloadFrame>,
    },
    /// Reject a late repair request because the sender has already purged the
    /// retained multipart state for that logical message.
    SendNoLongerAvailable { frame: NoLongerAvailableFrame },
    /// Observe one valid receiver `Ack` without purging local retained state.
    ObservedAck { message_id: MessageId },
    /// Report that the retained sender-side state for one logical message was purged.
    Purged {
        message_id: MessageId,
        reason: SenderPurgeReason,
    },
}

/// Why one sender-side live transfer was purged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SenderPurgeReason {
    Acked,
    Aborted,
    RetentionExpired,
}

/// Sender-side errors.
#[derive(Debug, Snafu)]
pub(crate) enum SenderError {
    #[snafu(display("max_part_payload_len must be greater than zero"))]
    ZeroMaxPartPayloadLen,
    #[snafu(display(
        "Payload of {payload_len} bytes would exceed the supported part count for max_part_payload_len={max_part_payload_len}"
    ))]
    TooManyParts {
        payload_len: usize,
        max_part_payload_len: usize,
        source: std::num::TryFromIntError,
    },
    #[snafu(display("Generated an invalid part count"))]
    InvalidPartCount {
        source: crate::types::UDPourTypeError,
    },
    #[snafu(display("All message ids are currently live or cooling down"))]
    MessageIdExhausted,
}

fn part_count_for_payload(
    payload_len: usize,
    max_part_payload_len: usize,
) -> Result<PartCount, SenderError> {
    let part_count = payload_len.max(1).div_ceil(max_part_payload_len);
    let part_count = u32::try_from(part_count).context(TooManyPartsSnafu {
        payload_len,
        max_part_payload_len,
    })?;
    PartCount::new(part_count).context(InvalidPartCountSnafu)
}

fn slice_part(
    payload: IoPayload,
    max_part_payload_len: usize,
    part_number: PartNumber,
    part_count: PartCount,
) -> IoPayload {
    let start = part_number.0 as usize * max_part_payload_len;
    let end = if part_number == part_count.last_part_number() {
        payload.len()
    } else {
        (start + max_part_payload_len).min(payload.len())
    };
    payload
        .try_slice(start, end - start)
        .expect("payload part range must stay inside the retained payload")
}

fn no_longer_available_frame(header: UDPourHeader) -> NoLongerAvailableFrame {
    let header = UDPourHeader::control(
        FrameType::NoLongerAvailable,
        header.message_id,
        header.part_count,
        header.checksum,
    )
    .expect("NeedParts metadata must always produce a valid NoLongerAvailable header");
    NoLongerAvailableFrame { header }
}

fn checksum_payload(payload: &IoPayload) -> Checksum {
    let mut cursor = payload.cursor();
    let mut checksum = 0u32;
    while cursor.has_remaining() {
        let chunk = cursor.chunk();
        let chunk_len = chunk.len();
        checksum = crc32c::crc32c_append(checksum, chunk);
        cursor.advance(chunk_len);
    }
    Checksum(checksum)
}

/// One retained logical message together with the metadata needed for repair.
#[derive(Debug)]
struct LiveMessage {
    payload: IoPayload,
    checksum: Checksum,
    part_count: PartCount,
    /// `None` until the initial multipart send has fully completed.
    expires_at: Option<Instant>,
}

impl LiveMessage {
    fn matches(&self, part_count: PartCount, checksum: Checksum) -> bool {
        self.part_count == part_count && self.checksum == checksum
    }

    fn all_payload_frames(
        &self,
        message_id: MessageId,
        max_part_payload_len: usize,
    ) -> Vec<PayloadFrame> {
        (0..self.part_count.get())
            .map(|part_number| {
                self.payload_frame(
                    message_id,
                    max_part_payload_len,
                    PartNumber(part_number),
                    FrameFlags::DEFAULT,
                )
            })
            .collect()
    }

    fn payload_frames_for_missing_parts(
        &self,
        message_id: MessageId,
        max_part_payload_len: usize,
        missing_parts: &roaring::RoaringBitmap,
    ) -> Vec<PayloadFrame> {
        missing_parts
            .iter()
            .filter(|part_number| *part_number < self.part_count.get())
            .map(|part_number| {
                self.payload_frame(
                    message_id,
                    max_part_payload_len,
                    PartNumber(part_number),
                    FrameFlags::DEFAULT.with_retransmit(),
                )
            })
            .collect()
    }

    fn payload_frame(
        &self,
        message_id: MessageId,
        max_part_payload_len: usize,
        part_number: PartNumber,
        flags: FrameFlags,
    ) -> PayloadFrame {
        let payload = slice_part(
            self.payload.clone(),
            max_part_payload_len,
            part_number,
            self.part_count,
        );
        let header = UDPourHeader::payload_with_flags(
            message_id,
            part_number,
            self.part_count,
            self.checksum,
            flags,
        );
        PayloadFrame { header, payload }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{NeedPartsFrame, PartNumber};
    use roaring::RoaringBitmap;

    #[test]
    fn splits_payload_and_retransmits_missing_parts() {
        let config =
            SenderConfig::new(4, Duration::from_secs(10), Duration::from_secs(10)).unwrap();
        let mut sender = SenderMachine::new(config);
        let now = Instant::now();

        let SenderAction::SendPayloads { message_id, frames } = sender
            .start_transfer(IoPayload::from_static(b"abcdefghijkl"), now)
            .unwrap()
        else {
            panic!("expected payload send");
        };
        assert_eq!(frames.len(), 3);
        assert_eq!(frames[0].payload.to_vec().as_slice(), b"abcd");
        assert_eq!(frames[2].payload.to_vec().as_slice(), b"ijkl");

        let missing_parts = RoaringBitmap::from([1]);
        let need_parts = NeedPartsFrame {
            header: UDPourHeader::control(
                FrameType::NeedParts,
                message_id,
                PartCount::new(3).unwrap(),
                Checksum(crc32c::crc32c(b"abcdefghijkl")),
            )
            .unwrap(),
            missing_parts,
        };
        let Some(SenderAction::SendPayloads { frames, .. }) =
            sender.handle_need_parts(&need_parts, now)
        else {
            panic!("expected resend");
        };
        assert_eq!(frames.len(), 1);
        assert!(frames[0].header.is_retransmit());
        assert_eq!(frames[0].payload.to_vec().as_slice(), b"efgh");
    }

    #[test]
    fn sends_no_longer_available_for_unknown_message() {
        let config =
            SenderConfig::new(4, Duration::from_secs(10), Duration::from_secs(10)).unwrap();
        let mut sender = SenderMachine::new(config);
        let now = Instant::now();
        let missing_parts = RoaringBitmap::from([0]);
        let need_parts = NeedPartsFrame {
            header: UDPourHeader::control(
                FrameType::NeedParts,
                MessageId(22),
                PartCount::new(1).unwrap(),
                Checksum(7),
            )
            .unwrap(),
            missing_parts,
        };

        let Some(SenderAction::SendNoLongerAvailable { frame }) =
            sender.handle_need_parts(&need_parts, now)
        else {
            panic!("expected NoLongerAvailable");
        };
        assert_eq!(frame.header.message_id, MessageId(22));
    }

    #[test]
    fn cooled_down_message_ids_are_skipped_on_wraparound() {
        let config =
            SenderConfig::new(4, Duration::from_millis(5), Duration::from_millis(50)).unwrap();
        let mut sender = SenderMachine::with_initial_message_id(config, MessageId(0));
        let now = Instant::now();

        let SenderAction::SendPayloads { message_id, .. } = sender
            .start_transfer(IoPayload::from_static(b"abcd"), now)
            .unwrap()
        else {
            panic!("expected payload send");
        };
        assert_eq!(message_id, MessageId(0));
        sender.mark_initial_send_complete(message_id, now);

        let purges = sender.poll_timeouts(now + Duration::from_millis(6));
        assert!(purges.iter().any(|action| matches!(
            action,
            SenderAction::Purged {
                message_id: MessageId(0),
                reason: SenderPurgeReason::RetentionExpired,
            }
        )));

        sender.next_message_id = 0;
        let SenderAction::SendPayloads { message_id, .. } = sender
            .start_transfer(
                IoPayload::from_static(b"wxyz"),
                now + Duration::from_millis(6),
            )
            .unwrap()
        else {
            panic!("expected payload send");
        };
        assert_eq!(message_id, MessageId(1));
    }

    #[test]
    fn handle_ack_observes_matching_ack() {
        let config =
            SenderConfig::new(4, Duration::from_secs(10), Duration::from_secs(10)).unwrap();
        let mut sender = SenderMachine::new(config);
        let now = Instant::now();

        let SenderAction::SendPayloads { message_id, .. } = sender
            .start_transfer(IoPayload::from_static(b"abcd"), now)
            .unwrap()
        else {
            panic!("expected payload send");
        };

        let ack = AckFrame {
            header: UDPourHeader::control(
                FrameType::Ack,
                message_id,
                PartCount::new(1).unwrap(),
                Checksum(crc32c::crc32c(b"abcd")),
            )
            .unwrap(),
        };

        assert_eq!(
            sender.handle_ack(&ack, now + Duration::from_millis(1)),
            Some(SenderAction::ObservedAck { message_id })
        );
    }

    #[test]
    fn handle_ack_eager_cleanup_purges_matching_message() {
        let mut config =
            SenderConfig::new(4, Duration::from_secs(10), Duration::from_secs(10)).unwrap();
        config.eager_ack_cleanup = true;
        let mut sender = SenderMachine::new(config);
        let now = Instant::now();

        let SenderAction::SendPayloads { message_id, .. } = sender
            .start_transfer(IoPayload::from_static(b"abcd"), now)
            .unwrap()
        else {
            panic!("expected payload send");
        };

        let ack = AckFrame {
            header: UDPourHeader::control(
                FrameType::Ack,
                message_id,
                PartCount::new(1).unwrap(),
                Checksum(crc32c::crc32c(b"abcd")),
            )
            .unwrap(),
        };

        assert_eq!(
            sender.handle_ack(&ack, now + Duration::from_millis(1)),
            Some(SenderAction::Purged {
                message_id,
                reason: SenderPurgeReason::Acked,
            })
        );
        assert!(
            sender
                .handle_ack(&ack, now + Duration::from_millis(2))
                .is_none()
        );
    }

    #[test]
    fn handle_ack_ignores_stale_ack() {
        let config =
            SenderConfig::new(4, Duration::from_secs(10), Duration::from_secs(10)).unwrap();
        let mut sender = SenderMachine::new(config);
        let now = Instant::now();

        let SenderAction::SendPayloads { message_id, .. } = sender
            .start_transfer(IoPayload::from_static(b"abcd"), now)
            .unwrap()
        else {
            panic!("expected payload send");
        };

        let stale_ack = AckFrame {
            header: UDPourHeader::control(
                FrameType::Ack,
                message_id,
                PartCount::new(1).unwrap(),
                Checksum(999),
            )
            .unwrap(),
        };

        assert!(
            sender
                .handle_ack(&stale_ack, now + Duration::from_millis(1))
                .is_none()
        );
    }

    #[test]
    fn zero_length_payload_produces_one_empty_part() {
        let config =
            SenderConfig::new(4, Duration::from_secs(10), Duration::from_secs(10)).unwrap();
        let mut sender = SenderMachine::new(config);
        let now = Instant::now();

        let SenderAction::SendPayloads { frames, .. } = sender
            .start_transfer(IoPayload::from_static(b""), now)
            .unwrap()
        else {
            panic!("expected payload send");
        };

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].header.part_number, PartNumber(0));
        assert_eq!(frames[0].header.part_count, PartCount::new(1).unwrap());
        assert_eq!(frames[0].payload.len(), 0);
    }
}
