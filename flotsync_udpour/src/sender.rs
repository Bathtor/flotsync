//! Sender-side multipart state machine.
//!
//! This module owns sender-local `message_id` allocation, bounded retention of
//! original payload parts, and late repair behavior.

use crate::types::*;
use bytes::Buf;
use flotsync_io::prelude::IoPayload;
use roaring::RoaringBitmap;
use smallvec::SmallVec;
use snafu::prelude::*;
use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    num::NonZeroUsize,
    time::{Duration, Instant},
};

/// Sender-side runtime configuration.
#[derive(Clone, Debug)]
pub struct SenderConfig {
    /// Maximum payload bytes carried by one non-final `Payload` frame.
    pub max_part_payload_len: NonZeroUsize,
    /// How long the sender retains parts for repair after the initial send.
    pub retention_timeout: Duration,
    /// How long a purged `message_id` must stay out of circulation before reuse.
    pub id_reuse_cooldown: Duration,
    /// Whether one observed `Ack` should immediately purge the retained message.
    pub eager_ack_cleanup: bool,
}

impl SenderConfig {
    /// Basic sender configuration with shared-route-safe defaults.
    #[cfg(test)]
    pub(crate) fn new(
        max_part_payload_len: NonZeroUsize,
        retention_timeout: Duration,
        id_reuse_cooldown: Duration,
    ) -> Self {
        Self {
            max_part_payload_len,
            retention_timeout,
            id_reuse_cooldown,
            eager_ack_cleanup: false,
        }
    }
}

/// One sender-owned multiplexed multipart state machine.
#[derive(Debug)]
pub(crate) struct SenderMachine {
    config: SenderConfig,
    next_message_id: MessageId,
    live: HashMap<MessageId, LiveMessage>,
    cooldown: CooldownIds,
}

impl SenderMachine {
    /// Creates one new sender state machine.
    pub fn new(config: SenderConfig) -> Self {
        Self::with_initial_message_id(config, MessageId(0))
    }

    /// Creates one new sender state machine with an explicit initial id.
    pub fn with_initial_message_id(config: SenderConfig, next_message_id: MessageId) -> Self {
        Self {
            config,
            next_message_id,
            live: HashMap::new(),
            cooldown: CooldownIds::new(),
        }
    }

    /// Starts one new multipart transfer and returns the initial payload frames.
    pub fn start_transfer(
        &mut self,
        payload: IoPayload,
        now: Instant,
    ) -> Result<SenderAction, SenderError> {
        let message_id = self.allocate_message_id(now)?;
        let checksum = calculate_payload_checksum(&payload);
        let part_count = part_count_for_payload(payload.len(), self.config.max_part_payload_len)?;
        let live_message = LiveMessage {
            payload,
            checksum,
            part_count,
            expires_at: None,
        };
        // TODO(flotsync-2hd): Stream initial payload frames directly into the runtime send path
        // instead of materializing one Vec<PayloadFrame> per logical send.
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
        let live = self.live.get(&ack.header.message_id)?;
        if !live.matches(ack.header.part_count, ack.header.checksum) {
            return None;
        }
        if self.config.eager_ack_cleanup {
            self.purge_message(ack.header.message_id, now, SenderPurgeReason::Acked)
        } else {
            Some(SenderAction::ObservedAck {
                message_id: ack.header.message_id,
            })
        }
    }

    /// Handles one receiver repair request.
    pub fn handle_need_parts(
        &mut self,
        need_parts: &NeedPartsFrame,
    ) -> Result<SenderAction, SenderError> {
        let Some(live) = self.live.get(&need_parts.header.message_id) else {
            return Ok(SenderAction::SendNoLongerAvailable {
                frame: need_parts.no_longer_available_frame(),
            });
        };
        if !live.matches(need_parts.header.part_count, need_parts.header.checksum) {
            return Ok(SenderAction::SendNoLongerAvailable {
                frame: need_parts.no_longer_available_frame(),
            });
        }

        // TODO(flotsync-2hd): Stream NeedParts responses directly into the runtime send path
        // instead of materializing one Vec<PayloadFrame> per repair response.
        let frames = live.payload_frames_for_missing_parts(
            need_parts.header.message_id,
            self.config.max_part_payload_len,
            &need_parts.missing_parts,
        )?;
        Ok(SenderAction::SendPayloads {
            message_id: need_parts.header.message_id,
            frames,
        })
    }

    /// Purges expired retained messages and moves their ids into cooldown.
    pub fn poll_timeouts(&mut self, now: Instant) -> SenderActions {
        self.cooldown.gc_expired(now);
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
        self.purge_message(message_id, now, SenderPurgeReason::Aborted)
    }

    fn allocate_message_id(&mut self, now: Instant) -> Result<MessageId, SenderError> {
        self.cooldown.gc_expired(now);
        'search_loop: for _ in 0..=u32::MAX {
            let candidate = self.next_message_id;
            self.next_message_id.wrapping_increment();
            if self.live.contains_key(&candidate) {
                // Id is still in use.
                continue 'search_loop;
            }
            if self.cooldown.contains(candidate) {
                // Id is still in cooldown.
                continue 'search_loop;
            }
            return Ok(candidate);
        }
        MessageIdExhaustedSnafu.fail()
    }

    fn purge_message(
        &mut self,
        message_id: MessageId,
        now: Instant,
        reason: SenderPurgeReason,
    ) -> Option<SenderAction> {
        self.live.remove(&message_id)?;
        self.cooldown
            .insert(message_id, now + self.config.id_reuse_cooldown);
        Some(SenderAction::Purged { message_id, reason })
    }
}

/// Most sender methods only return < 2 actions even though the individual
/// `SendPayloads` action can still carry a large payload-frame `Vec`.
pub(crate) type SenderActions = SmallVec<[SenderAction; 2]>;

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
    /// The receiver acknowledged the message and sender policy allowed eager cleanup.
    Acked,
    /// The runtime aborted the logical send after a terminal lower-layer failure.
    Aborted,
    /// The sender-side retention window expired naturally.
    RetentionExpired,
}

/// Sender-side errors.
#[derive(Debug, Snafu)]
pub(crate) enum SenderError {
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
    #[snafu(display(
        "NeedParts requested part {part_number} but the retained message only has {part_count} parts"
    ))]
    RequestedPartOutOfRange { part_number: u32, part_count: u32 },
}

/// Calculates how many multipart payload frames are needed to carry `payload_len` bytes.
///
/// Zero-length logical payloads still occupy one empty payload frame so the receiver can observe
/// one complete message with checksum and part metadata.
fn part_count_for_payload(
    payload_len: usize,
    max_part_payload_len: NonZeroUsize,
) -> Result<PartCount, SenderError> {
    let max_part_payload_len = max_part_payload_len.get();
    let part_count = payload_len.max(1).div_ceil(max_part_payload_len);
    let part_count = u32::try_from(part_count).context(TooManyPartsSnafu {
        payload_len,
        max_part_payload_len,
    })?;
    PartCount::new(part_count).context(InvalidPartCountSnafu)
}

/// Returns the retained payload slice for one concrete multipart payload part.
///
/// All non-final parts have exactly `max_part_payload_len` bytes. The final part may be shorter
/// so the returned range is trimmed to the logical payload length.
fn get_slice_for_part(
    payload: IoPayload,
    max_part_payload_len: NonZeroUsize,
    part_number: PartNumber,
    part_count: PartCount,
) -> IoPayload {
    let max_part_payload_len = max_part_payload_len.get();
    let start = part_number.0 as usize * max_part_payload_len;
    let len = if part_number == part_count.last_part_number() {
        payload.len().saturating_sub(start)
    } else {
        max_part_payload_len
    };
    payload
        .try_slice(start..start + len)
        .expect("payload part range must stay inside the retained payload")
}

fn calculate_payload_checksum(payload: &IoPayload) -> Checksum {
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

/// Membership plus expiry tracking for sender ids that are temporarily cooling down.
///
/// Cooldown ids are garbage-collected lazily right before id allocation and during regular sender
/// timeout polling. That keeps the hot acknowledgment/repair paths free of unrelated cleanup work.
///
/// Each `MessageId` may enter cooldown at most once before expiry. Re-inserting an id that is
/// still present indicates a sender-state bug, so `insert` asserts that this never happens.
#[derive(Debug)]
struct CooldownIds {
    ids: RoaringBitmap,
    expirations: BinaryHeap<Reverse<(Instant, MessageId)>>,
}

impl CooldownIds {
    fn new() -> Self {
        Self {
            ids: RoaringBitmap::new(),
            expirations: BinaryHeap::new(),
        }
    }

    fn contains(&self, message_id: MessageId) -> bool {
        self.ids.contains(message_id.0)
    }

    /// Inserts one newly cooled-down id.
    ///
    /// This asserts that the id was not already cooling down, because sender logic must not purge
    /// the same live transfer twice without first waiting for cooldown expiry and reallocation.
    fn insert(&mut self, message_id: MessageId, expires_at: Instant) {
        assert!(
            !self.ids.contains(message_id.0),
            "message_id {} entered cooldown twice before expiry",
            message_id.0
        );
        self.ids.insert(message_id.0);
        self.expirations.push(Reverse((expires_at, message_id)));
    }

    fn gc_expired(&mut self, now: Instant) {
        while let Some(Reverse((expires_at, message_id))) = self.expirations.peek().copied() {
            if expires_at > now {
                break;
            }
            self.expirations.pop();
            self.ids.remove(message_id.0);
        }
    }
}

impl LiveMessage {
    fn matches(&self, part_count: PartCount, checksum: Checksum) -> bool {
        self.part_count == part_count && self.checksum == checksum
    }

    fn all_payload_frames(
        &self,
        message_id: MessageId,
        max_part_payload_len: NonZeroUsize,
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
        max_part_payload_len: NonZeroUsize,
        missing_parts: &RoaringBitmap,
    ) -> Result<Vec<PayloadFrame>, SenderError> {
        if let Some(max_requested_part) = missing_parts.max()
            && max_requested_part >= self.part_count.get()
        {
            return RequestedPartOutOfRangeSnafu {
                part_number: max_requested_part,
                part_count: self.part_count.get(),
            }
            .fail();
        }

        let frames = missing_parts
            .iter()
            .map(|part_number| {
                self.payload_frame(
                    message_id,
                    max_part_payload_len,
                    PartNumber(part_number),
                    FrameFlags::DEFAULT.with_retransmit(),
                )
            })
            .collect::<Vec<_>>();
        Ok(frames)
    }

    fn payload_frame(
        &self,
        message_id: MessageId,
        max_part_payload_len: NonZeroUsize,
        part_number: PartNumber,
        flags: FrameFlags,
    ) -> PayloadFrame {
        let payload = get_slice_for_part(
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
    fn cooldown_ids_track_membership_until_gc_runs() {
        let now = Instant::now();
        let mut cooldown = CooldownIds::new();
        let first = MessageId(7);
        let second = MessageId(9);

        cooldown.insert(first, now + Duration::from_millis(10));
        cooldown.insert(second, now + Duration::from_millis(20));

        assert!(cooldown.contains(first));
        assert!(cooldown.contains(second));

        cooldown.gc_expired(now + Duration::from_millis(15));

        assert!(!cooldown.contains(first));
        assert!(cooldown.contains(second));
    }

    #[test]
    #[should_panic(expected = "entered cooldown twice before expiry")]
    fn cooldown_ids_panics_on_duplicate_insert() {
        let now = Instant::now();
        let mut cooldown = CooldownIds::new();
        let message_id = MessageId(11);

        cooldown.insert(message_id, now + Duration::from_millis(10));
        cooldown.insert(message_id, now + Duration::from_millis(30));
    }

    #[test]
    fn splits_payload_and_retransmits_missing_parts() {
        let config = SenderConfig::new(
            NonZeroUsize::new(4).unwrap(),
            Duration::from_secs(10),
            Duration::from_secs(10),
        );
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
        let SenderAction::SendPayloads { frames, .. } =
            sender.handle_need_parts(&need_parts).unwrap()
        else {
            panic!("expected resend");
        };
        assert_eq!(frames.len(), 1);
        assert!(frames[0].header.is_retransmit());
        assert_eq!(frames[0].payload.to_vec().as_slice(), b"efgh");
    }

    #[test]
    fn sends_no_longer_available_for_unknown_message() {
        let config = SenderConfig::new(
            NonZeroUsize::new(4).unwrap(),
            Duration::from_secs(10),
            Duration::from_secs(10),
        );
        let mut sender = SenderMachine::new(config);
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

        let SenderAction::SendNoLongerAvailable { frame } =
            sender.handle_need_parts(&need_parts).unwrap()
        else {
            panic!("expected NoLongerAvailable");
        };
        assert_eq!(frame.header.message_id, MessageId(22));
    }

    #[test]
    fn cooled_down_message_ids_are_skipped_on_wraparound() {
        let config = SenderConfig::new(
            NonZeroUsize::new(4).unwrap(),
            Duration::from_millis(5),
            Duration::from_millis(50),
        );
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

        sender.next_message_id = MessageId(0);
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
        let config = SenderConfig::new(
            NonZeroUsize::new(4).unwrap(),
            Duration::from_secs(10),
            Duration::from_secs(10),
        );
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
        let mut config = SenderConfig::new(
            NonZeroUsize::new(4).unwrap(),
            Duration::from_secs(10),
            Duration::from_secs(10),
        );
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
        let config = SenderConfig::new(
            NonZeroUsize::new(4).unwrap(),
            Duration::from_secs(10),
            Duration::from_secs(10),
        );
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
        let config = SenderConfig::new(
            NonZeroUsize::new(4).unwrap(),
            Duration::from_secs(10),
            Duration::from_secs(10),
        );
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
