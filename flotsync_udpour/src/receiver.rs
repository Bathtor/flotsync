//! Receiver-side multipart reassembly state machine.
//!
//! This module owns `(source, message_id)` reassembly state, repair polling,
//! whole-message checksum validation, and purge behavior.

use crate::{codec::fit_one_need_parts_frame, types::*};
use bytes::Buf;
use flotsync_io::prelude::IoPayload;
use roaring::RoaringBitmap;
use smallvec::{SmallVec, smallvec};
use snafu::prelude::*;
use std::{
    collections::{HashMap, hash_map::Entry},
    net::SocketAddr,
    time::{Duration, Instant},
};

/// Receiver-side runtime configuration.
#[derive(Clone, Debug)]
pub struct ReceiverConfig {
    /// How often the receiver should ask for still-missing parts.
    pub repair_interval: Duration,
    /// How long one stalled transfer may sit idle before it is purged.
    ///
    /// This deadline resets whenever a fresh payload part is accepted.
    pub give_up_timeout: Duration,
    /// Maximum encoded wire size for one `NeedParts` frame.
    pub max_need_parts_frame_len: usize,
    /// How long one successful delivery stays around as a duplicate-suppression tombstone.
    ///
    /// `UDPourConfig::new` derives this from sender retention and message-id reuse cooldown so
    /// late shared-route repair traffic is still recognized as belonging to the old logical
    /// message. Callers that construct `ReceiverConfig` directly in tests must set it explicitly.
    pub delivered_tombstone_timeout: Duration,
}

impl Default for ReceiverConfig {
    fn default() -> Self {
        Self {
            repair_interval: Duration::from_millis(100),
            give_up_timeout: Duration::from_secs(30),
            max_need_parts_frame_len: 1024,
            // `UDPourConfig::new` overwrites this with the sender-derived
            // duplicate-suppression window for normal runtime construction.
            delivered_tombstone_timeout: Duration::ZERO,
        }
    }
}

/// Sender identity plus logical message id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ReceiverTransferKey {
    pub source: SocketAddr,
    pub message_id: MessageId,
}

/// One receiver-owned multiplexed multipart state machine.
#[derive(Debug)]
pub(crate) struct ReceiverMachine {
    config: ReceiverConfig,
    transfers: HashMap<ReceiverTransferKey, InboundState>,
}

impl ReceiverMachine {
    /// Creates one new receiver state machine.
    pub fn new(config: ReceiverConfig) -> Self {
        Self {
            config,
            transfers: HashMap::new(),
        }
    }

    /// Accepts one inbound payload frame from a UDP source.
    pub fn accept_payload(
        &mut self,
        source: SocketAddr,
        frame: PayloadFrame,
        now: Instant,
    ) -> Result<ReceiverActions, ReceiverError> {
        let key = ReceiverTransferKey {
            source,
            message_id: frame.header.message_id,
        };
        match self.transfers.entry(key) {
            Entry::Occupied(mut entry) => {
                // Copy the delivered tombstone out before moving the occupied entry into the
                // helper. Otherwise the temporary borrow from `entry.get()` overlaps with moving
                // the entry itself, and Rust is right to reject that.
                let delivered_tombstone = match entry.get() {
                    InboundState::Delivered(tombstone) => Some(*tombstone),
                    InboundState::Pending(_) => None,
                };
                if let Some(tombstone) = delivered_tombstone {
                    Ok(Self::apply_delivered_tombstone_update(
                        &self.config,
                        key,
                        entry,
                        tombstone,
                        frame,
                        now,
                    ))
                } else {
                    match entry.get_mut() {
                        InboundState::Delivered(_) => unreachable!("handled above"),
                        InboundState::Pending(transfer) => {
                            let update = transfer.accept_payload(frame, now, &self.config);
                            Ok(Self::apply_pending_update(
                                &self.config,
                                source,
                                key,
                                entry,
                                update,
                                now,
                            ))
                        }
                    }
                }
            }
            Entry::Vacant(entry) => {
                let mut transfer = InboundTransfer::new(
                    frame.header.part_count,
                    frame.header.checksum,
                    now,
                    &self.config,
                );
                let update = transfer.accept_payload(frame, now, &self.config);
                Ok(Self::apply_new_transfer_update(
                    &self.config,
                    source,
                    key,
                    entry,
                    transfer,
                    update,
                    now,
                ))
            }
        }
    }

    fn accept_delivered_payload(
        config: &ReceiverConfig,
        key: ReceiverTransferKey,
        tombstone: DeliveredTombstone,
        frame: PayloadFrame,
        now: Instant,
    ) -> (Option<InboundState>, ReceiverActions) {
        if tombstone.matches(frame.header.part_count, frame.header.checksum) {
            let header = UDPourHeader::control(
                FrameType::Ack,
                key.message_id,
                frame.header.part_count,
                frame.header.checksum,
            )
            .expect("delivered tombstone metadata must always produce a valid Ack header");
            return (
                Some(InboundState::Delivered(tombstone)),
                smallvec![ReceiverAction::SendAck {
                    source: key.source,
                    frame: AckFrame { header },
                }],
            );
        }
        if frame.header.is_retransmit() {
            // A retransmitted part that disagrees with the delivered tombstone belongs to some
            // earlier repair attempt and must not resurrect inbound state after successful
            // delivery.
            return (Some(InboundState::Delivered(tombstone)), SmallVec::new());
        }

        let mut transfer =
            InboundTransfer::new(frame.header.part_count, frame.header.checksum, now, config);
        match transfer.accept_payload(frame, now, config) {
            InboundUpdate::Pending => (Some(InboundState::Pending(transfer)), SmallVec::new()),
            InboundUpdate::Deliver {
                payload,
                message_id,
                part_count,
                checksum,
            } => {
                let next_state = InboundState::Delivered(DeliveredTombstone::new(
                    part_count,
                    checksum,
                    now + config.delivered_tombstone_timeout,
                ));
                let header =
                    UDPourHeader::control(FrameType::Ack, message_id, part_count, checksum)
                        .expect("Ack metadata gathered from a valid transfer must stay valid");
                (
                    Some(next_state),
                    smallvec![
                        ReceiverAction::Deliver {
                            source: key.source,
                            payload,
                        },
                        ReceiverAction::SendAck {
                            source: key.source,
                            frame: AckFrame { header },
                        },
                    ],
                )
            }
            InboundUpdate::Purged(reason) => {
                (None, smallvec![ReceiverAction::Purged { key, reason }])
            }
        }
    }

    /// Accepts one sender `NoLongerAvailable` control frame.
    pub fn accept_no_longer_available(
        &mut self,
        source: SocketAddr,
        frame: NoLongerAvailableFrame,
    ) -> Option<ReceiverAction> {
        let key = ReceiverTransferKey {
            source,
            message_id: frame.header.message_id,
        };
        let InboundState::Pending(transfer) = self.transfers.get(&key)? else {
            return None;
        };
        if transfer.part_count != frame.header.part_count
            || transfer.checksum != frame.header.checksum
        {
            return None;
        }
        self.transfers.remove(&key);
        Some(ReceiverAction::Purged {
            key,
            reason: ReceiverPurgeReason::NoLongerAvailable,
        })
    }

    #[cfg(test)]
    /// Returns whether receiver state already reflects one observed payload
    /// header from `source`.
    ///
    /// This lets runtime tests distinguish "the bridge observer saw the frame"
    /// from "the receiving UDPour state machine already consumed the frame and
    /// reset any dependent repair deadlines".
    pub(crate) fn has_reflected_payload(&self, source: SocketAddr, header: UDPourHeader) -> bool {
        let key = ReceiverTransferKey {
            source,
            message_id: header.message_id,
        };
        match self.transfers.get(&key) {
            Some(InboundState::Pending(transfer)) => {
                transfer.part_count == header.part_count
                    && transfer.checksum == header.checksum
                    && !transfer.missing_parts.contains(header.part_number.0)
            }
            Some(InboundState::Delivered(tombstone)) => {
                tombstone.matches(header.part_count, header.checksum)
            }
            None => false,
        }
    }

    /// Advances timeout-driven repair and purge behavior.
    pub fn poll_timeouts(&mut self, now: Instant) -> Result<Vec<ReceiverAction>, ReceiverError> {
        let keys: Vec<_> = self.transfers.keys().copied().collect();
        let mut actions = Vec::new();
        'key_loop: for key in keys {
            let Entry::Occupied(mut entry) = self.transfers.entry(key) else {
                continue 'key_loop;
            };
            match entry.get_mut() {
                InboundState::Pending(transfer) => {
                    if transfer.give_up_deadline <= now {
                        entry.remove();
                        actions.push(ReceiverAction::Purged {
                            key,
                            reason: ReceiverPurgeReason::TimedOut,
                        });
                        continue 'key_loop;
                    }
                    if transfer.repair_deadline <= now {
                        let frame = transfer.next_need_parts_frame(
                            key.message_id,
                            self.config.max_need_parts_frame_len,
                        )?;
                        if let Some(frame) = frame {
                            actions.push(ReceiverAction::SendNeedParts {
                                source: key.source,
                                frame,
                            });
                            transfer.repair_deadline = now + self.config.repair_interval;
                        } else {
                            unreachable!(
                                "We must be missing parts if haven't delivered this transfer yet"
                            );
                        }
                    }
                }
                InboundState::Delivered(tombstone) => {
                    if tombstone.expires_at <= now {
                        entry.remove();
                        actions.push(ReceiverAction::Purged {
                            key,
                            reason: ReceiverPurgeReason::DeliveredTombstoneExpired,
                        });
                    }
                }
            }
        }
        Ok(actions)
    }

    fn apply_pending_update(
        config: &ReceiverConfig,
        source: SocketAddr,
        key: ReceiverTransferKey,
        mut entry: std::collections::hash_map::OccupiedEntry<'_, ReceiverTransferKey, InboundState>,
        update: InboundUpdate,
        now: Instant,
    ) -> ReceiverActions {
        match update {
            InboundUpdate::Pending => SmallVec::new(),
            InboundUpdate::Deliver {
                payload,
                message_id,
                part_count,
                checksum,
            } => {
                entry.insert(InboundState::Delivered(DeliveredTombstone::new(
                    part_count,
                    checksum,
                    now + config.delivered_tombstone_timeout,
                )));
                let header =
                    UDPourHeader::control(FrameType::Ack, message_id, part_count, checksum)
                        .expect("Ack metadata gathered from a valid transfer must stay valid");
                smallvec![
                    ReceiverAction::Deliver { source, payload },
                    ReceiverAction::SendAck {
                        source,
                        frame: AckFrame { header },
                    },
                ]
            }
            InboundUpdate::Purged(reason) => {
                entry.remove();
                smallvec![ReceiverAction::Purged { key, reason }]
            }
        }
    }

    fn apply_new_transfer_update(
        config: &ReceiverConfig,
        source: SocketAddr,
        key: ReceiverTransferKey,
        entry: std::collections::hash_map::VacantEntry<'_, ReceiverTransferKey, InboundState>,
        transfer: InboundTransfer,
        update: InboundUpdate,
        now: Instant,
    ) -> ReceiverActions {
        match update {
            InboundUpdate::Pending => {
                entry.insert(InboundState::Pending(transfer));
                SmallVec::new()
            }
            InboundUpdate::Deliver {
                payload,
                message_id,
                part_count,
                checksum,
            } => {
                entry.insert(InboundState::Delivered(DeliveredTombstone::new(
                    part_count,
                    checksum,
                    now + config.delivered_tombstone_timeout,
                )));
                let header =
                    UDPourHeader::control(FrameType::Ack, message_id, part_count, checksum)
                        .expect("Ack metadata gathered from a valid transfer must stay valid");
                smallvec![
                    ReceiverAction::Deliver { source, payload },
                    ReceiverAction::SendAck {
                        source,
                        frame: AckFrame { header },
                    },
                ]
            }
            InboundUpdate::Purged(reason) => smallvec![ReceiverAction::Purged { key, reason }],
        }
    }

    fn apply_delivered_tombstone_update(
        config: &ReceiverConfig,
        key: ReceiverTransferKey,
        mut entry: std::collections::hash_map::OccupiedEntry<'_, ReceiverTransferKey, InboundState>,
        tombstone: DeliveredTombstone,
        frame: PayloadFrame,
        now: Instant,
    ) -> ReceiverActions {
        let (next_state, actions) =
            Self::accept_delivered_payload(config, key, tombstone, frame, now);
        match next_state {
            Some(state) => {
                entry.insert(state);
            }
            None => {
                entry.remove();
            }
        }
        actions
    }
}

/// Most receiver interactions produce zero, one, or two actions:
/// one optional `Deliver`, one optional immediate follow-up control frame.
type ReceiverActions = SmallVec<[ReceiverAction; 2]>;

/// Receiver-facing outcomes from one state-machine interaction.
#[derive(Clone, Debug)]
pub(crate) enum ReceiverAction {
    /// Deliver one fully reassembled logical payload upward.
    Deliver {
        source: SocketAddr,
        payload: IoPayload,
    },
    /// Send one successful receiver acknowledgment back to the sender.
    SendAck { source: SocketAddr, frame: AckFrame },
    /// Ask the sender to repeat one fitted chunk of still-missing parts.
    ///
    /// The receiver emits at most one such request per repair poll and advances its
    /// internal repair watermark after doing so.
    SendNeedParts {
        source: SocketAddr,
        frame: NeedPartsFrame,
    },
    /// Report that the receiver-side state for one inbound transfer was purged.
    ///
    /// The runtime uses this only for local observability and cleanup decisions; it
    /// does not go on the wire.
    Purged {
        key: ReceiverTransferKey,
        reason: ReceiverPurgeReason,
    },
}

/// Why one inbound multipart transfer was purged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReceiverPurgeReason {
    /// Later packets for the same `(source, message_id)` disagreed on metadata or payload bytes.
    Confused,
    /// The reassembled payload bytes did not match the advertised whole-message checksum.
    ChecksumMismatch,
    /// The duplicate-suppression tombstone for one successfully delivered transfer expired.
    DeliveredTombstoneExpired,
    /// The sender explicitly reported that retained repair state for this message is gone.
    NoLongerAvailable,
    /// The receiver gave up waiting for more payload parts.
    TimedOut,
}

/// Receiver-side errors.
#[derive(Debug, Snafu)]
pub(crate) enum ReceiverError {
    /// One `NeedParts` frame could not be fitted into the configured frame budget.
    #[snafu(display("Failed to split NeedParts frames"))]
    SplitNeedParts { source: crate::codec::CodecError },
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

/// One active receiver-side entry for one `(source, message_id)` pair.
#[derive(Debug)]
enum InboundState {
    /// One incomplete transfer that is still collecting payload parts or asking for repairs.
    Pending(InboundTransfer),
    /// One recently delivered transfer kept only as a duplicate-suppression tombstone.
    Delivered(DeliveredTombstone),
}

/// One receiver-owned in-progress transfer keyed by `(source, message_id)`.
#[derive(Debug)]
struct InboundTransfer {
    part_count: PartCount,
    checksum: Checksum,
    /// One preallocated slot per expected payload part.
    ///
    /// The receiver knows `part_count` from the first accepted frame, so indexed storage is
    /// simpler than a sparse map and matches the eventual reassembly shape directly.
    parts: Vec<Option<IoPayload>>,
    missing_parts: RoaringBitmap,
    /// Largest missing part number included in the last emitted repair request.
    ///
    /// The next repair poll first prefers missing parts strictly greater than this watermark and
    /// only wraps back to the beginning once every later missing part has been requested once.
    repair_watermark: Option<u32>,
    regular_part_size: Option<usize>,
    give_up_deadline: Instant,
    repair_deadline: Instant,
}

impl InboundTransfer {
    fn new(
        part_count: PartCount,
        checksum: Checksum,
        now: Instant,
        config: &ReceiverConfig,
    ) -> Self {
        Self {
            part_count,
            checksum,
            parts: vec![None; part_count.get() as usize],
            missing_parts: RoaringBitmap::from_iter(0..part_count.get()),
            repair_watermark: None,
            regular_part_size: None,
            give_up_deadline: now + config.give_up_timeout,
            repair_deadline: now + config.repair_interval,
        }
    }

    fn accept_payload(
        &mut self,
        frame: PayloadFrame,
        now: Instant,
        config: &ReceiverConfig,
    ) -> InboundUpdate {
        if frame.header.part_count != self.part_count
            || frame.header.checksum != self.checksum
            || frame.header.part_number.0 >= self.part_count.get()
        {
            return InboundUpdate::Purged(ReceiverPurgeReason::Confused);
        }

        let is_final_part = frame.header.part_number == self.part_count.last_part_number();
        if !is_final_part {
            match self.regular_part_size {
                Some(existing) if existing != frame.payload.len() => {
                    return InboundUpdate::Purged(ReceiverPurgeReason::Confused);
                }
                None => {
                    self.regular_part_size = Some(frame.payload.len());
                }
                Some(_) => {
                    // Do nothing, already matches.
                }
            }
        }

        let part_index = frame.header.part_number.0 as usize;
        if let Some(existing) = self.parts[part_index].as_ref() {
            if !io_payload_eq(existing, &frame.payload) {
                return InboundUpdate::Purged(ReceiverPurgeReason::Confused);
            }
        } else {
            self.parts[part_index] = Some(frame.payload);
            self.missing_parts.remove(frame.header.part_number.0);
        }

        self.give_up_deadline = now + config.give_up_timeout;
        self.repair_deadline = now + config.repair_interval;

        if self.missing_parts.is_empty() {
            let payload = self.assemble_payload();
            let checksum = checksum_payload(&payload);
            if checksum != self.checksum {
                return InboundUpdate::Purged(ReceiverPurgeReason::ChecksumMismatch);
            }

            InboundUpdate::Deliver {
                payload,
                message_id: frame.header.message_id,
                part_count: self.part_count,
                checksum: self.checksum,
            }
        } else {
            InboundUpdate::Pending
        }
    }

    fn next_need_parts_frame(
        &mut self,
        message_id: MessageId,
        max_need_parts_frame_len: usize,
    ) -> Result<Option<NeedPartsFrame>, ReceiverError> {
        if self.missing_parts.is_empty() {
            return Ok(None);
        }

        // Repair polling walks forward through the missing-part space. We only wrap back to
        // earlier missing parts once every later missing part has been requested at least once.
        let frame = fit_one_need_parts_frame(
            message_id,
            self.part_count,
            self.checksum,
            &self.missing_parts,
            self.repair_watermark,
            max_need_parts_frame_len,
        )
        .context(SplitNeedPartsSnafu)?;
        let frame = match frame {
            Some(frame) => frame,
            None => fit_one_need_parts_frame(
                message_id,
                self.part_count,
                self.checksum,
                &self.missing_parts,
                None,
                max_need_parts_frame_len,
            )
            .context(SplitNeedPartsSnafu)?
            .expect("non-empty missing_parts must yield one fitted frame after wrap"),
        };
        self.repair_watermark = frame.missing_parts.max();
        Ok(Some(frame))
    }

    fn assemble_payload(&self) -> IoPayload {
        let parts = self.parts.iter().map(|part| {
            part.as_ref()
                .expect("complete transfer has all parts")
                .clone()
        });
        IoPayload::chain(parts)
    }
}

/// One successfully delivered transfer remembered just long enough to suppress late repair
/// traffic from shared routes.
#[derive(Clone, Copy, Debug)]
struct DeliveredTombstone {
    part_count: PartCount,
    checksum: Checksum,
    expires_at: Instant,
}

impl DeliveredTombstone {
    fn new(part_count: PartCount, checksum: Checksum, expires_at: Instant) -> Self {
        Self {
            part_count,
            checksum,
            expires_at,
        }
    }

    fn matches(self, part_count: PartCount, checksum: Checksum) -> bool {
        self.part_count == part_count && self.checksum == checksum
    }
}

#[derive(Debug)]
enum InboundUpdate {
    /// The transfer is still incomplete and remains pending.
    Pending,
    /// The transfer became complete, passed checksum validation, and can now be delivered.
    Deliver {
        payload: IoPayload,
        message_id: MessageId,
        part_count: PartCount,
        checksum: Checksum,
    },
    /// The transfer must be dropped immediately for the given purge reason.
    Purged(ReceiverPurgeReason),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PartNumber;
    use std::str::FromStr;

    fn receiver_config() -> ReceiverConfig {
        ReceiverConfig {
            repair_interval: Duration::from_millis(10),
            give_up_timeout: Duration::from_millis(50),
            max_need_parts_frame_len: 256,
            delivered_tombstone_timeout: Duration::from_millis(100),
        }
    }

    fn source() -> SocketAddr {
        SocketAddr::from_str("127.0.0.1:8000").unwrap()
    }

    #[test]
    fn reassembles_and_acks() {
        let mut receiver = ReceiverMachine::new(receiver_config());
        let now = Instant::now();
        let message_id = MessageId(5);
        let checksum = Checksum(crc32c::crc32c(b"abcdefghijkl"));
        let part_count = PartCount::new(3).unwrap();

        let mut actions = Vec::new();
        actions.extend(
            receiver
                .accept_payload(
                    source(),
                    PayloadFrame {
                        header: UDPourHeader::payload(
                            message_id,
                            PartNumber(0),
                            part_count,
                            checksum,
                        ),
                        payload: IoPayload::from_static(b"abcd"),
                    },
                    now,
                )
                .unwrap(),
        );
        actions.extend(
            receiver
                .accept_payload(
                    source(),
                    PayloadFrame {
                        header: UDPourHeader::payload(
                            message_id,
                            PartNumber(1),
                            part_count,
                            checksum,
                        ),
                        payload: IoPayload::from_static(b"efgh"),
                    },
                    now + Duration::from_millis(1),
                )
                .unwrap(),
        );
        actions.extend(
            receiver
                .accept_payload(
                    source(),
                    PayloadFrame {
                        header: UDPourHeader::payload(
                            message_id,
                            PartNumber(2),
                            part_count,
                            checksum,
                        ),
                        payload: IoPayload::from_static(b"ijkl"),
                    },
                    now + Duration::from_millis(2),
                )
                .unwrap(),
        );

        assert!(actions.iter().any(|action| {
            let ReceiverAction::Deliver { payload, .. } = action else {
                return false;
            };
            payload.to_vec().as_slice() == b"abcdefghijkl"
        }));
        assert!(
            actions
                .iter()
                .any(|action| matches!(action, ReceiverAction::SendAck { .. }))
        );
    }

    #[test]
    fn emits_need_parts_and_resets_timeout_on_new_payload() {
        let mut receiver = ReceiverMachine::new(receiver_config());
        let now = Instant::now();
        let message_id = MessageId(8);
        let checksum = Checksum(crc32c::crc32c(b"abcdefghijkl"));
        let part_count = PartCount::new(3).unwrap();
        receiver
            .accept_payload(
                source(),
                PayloadFrame {
                    header: UDPourHeader::payload(message_id, PartNumber(0), part_count, checksum),
                    payload: IoPayload::from_static(b"abcd"),
                },
                now,
            )
            .unwrap();

        let actions = receiver
            .poll_timeouts(now + Duration::from_millis(11))
            .unwrap();
        assert!(
            actions
                .iter()
                .any(|action| matches!(action, ReceiverAction::SendNeedParts { .. }))
        );

        receiver
            .accept_payload(
                source(),
                PayloadFrame {
                    header: UDPourHeader::payload(message_id, PartNumber(2), part_count, checksum),
                    payload: IoPayload::from_static(b"ijkl"),
                },
                now + Duration::from_millis(12),
            )
            .unwrap();
        let later_actions = receiver
            .poll_timeouts(now + Duration::from_millis(55))
            .unwrap();
        assert!(!later_actions.iter().any(|action| matches!(
            action,
            ReceiverAction::Purged {
                reason: ReceiverPurgeReason::TimedOut,
                ..
            }
        )));
    }

    #[test]
    fn need_parts_repairs_progress_across_missing_parts_before_wrapping() {
        let mut receiver = ReceiverMachine::new(ReceiverConfig {
            repair_interval: Duration::from_millis(10),
            give_up_timeout: Duration::from_millis(100),
            max_need_parts_frame_len: 40,
            delivered_tombstone_timeout: Duration::from_millis(100),
        });
        let now = Instant::now();
        let message_id = MessageId(44);
        let checksum = Checksum(1234);
        let part_count = PartCount::new(1003).unwrap();
        let key = ReceiverTransferKey {
            source: source(),
            message_id,
        };
        receiver.transfers.insert(
            key,
            InboundState::Pending(pending_transfer_with_missing_parts(
                part_count,
                checksum,
                RoaringBitmap::from([1, 2, 3, 1000, 1001, 1002]),
                now,
            )),
        );

        let first_actions = receiver
            .poll_timeouts(now + Duration::from_millis(11))
            .unwrap();
        let first_requested = first_actions
            .iter()
            .find_map(|action| match action {
                ReceiverAction::SendNeedParts { frame, .. } => Some(frame.missing_parts.clone()),
                _ => None,
            })
            .expect("first repair poll should emit one NeedParts frame");
        let mut saw_wrap = false;
        let mut seen_chunks = vec![first_requested.clone()];
        for step in 0..4u64 {
            let actions = receiver
                .poll_timeouts(now + Duration::from_millis(22 + (step * 11)))
                .unwrap();
            let requested = actions
                .iter()
                .find_map(|action| match action {
                    ReceiverAction::SendNeedParts { frame, .. } => {
                        Some(frame.missing_parts.clone())
                    }
                    _ => None,
                })
                .expect("repair poll should emit one NeedParts frame");

            if requested == first_requested {
                saw_wrap = true;
                break;
            }
            assert!(
                seen_chunks.iter().all(|prior| prior != &requested),
                "repair chunks should not repeat before the watermark wraps"
            );
            seen_chunks.push(requested);
        }

        assert!(
            saw_wrap,
            "repair polling should eventually wrap back to the first chunk"
        );
    }

    fn pending_transfer_with_missing_parts(
        part_count: PartCount,
        checksum: Checksum,
        missing_parts: RoaringBitmap,
        now: Instant,
    ) -> InboundTransfer {
        let config = ReceiverConfig {
            repair_interval: Duration::from_millis(10),
            give_up_timeout: Duration::from_millis(100),
            max_need_parts_frame_len: 256,
            delivered_tombstone_timeout: Duration::from_millis(100),
        };
        let mut transfer = InboundTransfer::new(part_count, checksum, now, &config);
        transfer.missing_parts = missing_parts;
        transfer
    }

    #[test]
    fn purges_on_conflicting_metadata() {
        let mut receiver = ReceiverMachine::new(receiver_config());
        let now = Instant::now();
        let message_id = MessageId(9);
        let checksum = Checksum(crc32c::crc32c(b"abcdwxyz"));
        let part_count = PartCount::new(2).unwrap();
        receiver
            .accept_payload(
                source(),
                PayloadFrame {
                    header: UDPourHeader::payload(message_id, PartNumber(0), part_count, checksum),
                    payload: IoPayload::from_static(b"abcd"),
                },
                now,
            )
            .unwrap();

        let actions = receiver
            .accept_payload(
                source(),
                PayloadFrame {
                    header: UDPourHeader::payload(
                        message_id,
                        PartNumber(1),
                        PartCount::new(3).unwrap(),
                        checksum,
                    ),
                    payload: IoPayload::from_static(b"wxyz"),
                },
                now + Duration::from_millis(1),
            )
            .unwrap();
        assert!(actions.iter().any(|action| matches!(
            action,
            ReceiverAction::Purged {
                reason: ReceiverPurgeReason::Confused,
                ..
            }
        )));
    }

    #[test]
    fn purges_on_checksum_mismatch() {
        let mut receiver = ReceiverMachine::new(receiver_config());
        let now = Instant::now();
        let message_id = MessageId(10);
        let checksum = Checksum(0);
        let part_count = PartCount::new(2).unwrap();
        receiver
            .accept_payload(
                source(),
                PayloadFrame {
                    header: UDPourHeader::payload(message_id, PartNumber(0), part_count, checksum),
                    payload: IoPayload::from_static(b"abcd"),
                },
                now,
            )
            .unwrap();

        let actions = receiver
            .accept_payload(
                source(),
                PayloadFrame {
                    header: UDPourHeader::payload(message_id, PartNumber(1), part_count, checksum),
                    payload: IoPayload::from_static(b"efgh"),
                },
                now + Duration::from_millis(1),
            )
            .unwrap();
        assert!(actions.iter().any(|action| matches!(
            action,
            ReceiverAction::Purged {
                reason: ReceiverPurgeReason::ChecksumMismatch,
                ..
            }
        )));
    }

    #[test]
    fn accept_no_longer_available_purges_matching_transfer() {
        let mut receiver = ReceiverMachine::new(receiver_config());
        let now = Instant::now();
        let message_id = MessageId(11);
        let checksum = Checksum(crc32c::crc32c(b"abcdefgh"));
        let part_count = PartCount::new(2).unwrap();
        receiver
            .accept_payload(
                source(),
                PayloadFrame {
                    header: UDPourHeader::payload(message_id, PartNumber(0), part_count, checksum),
                    payload: IoPayload::from_static(b"abcd"),
                },
                now,
            )
            .unwrap();

        let action = receiver.accept_no_longer_available(
            source(),
            NoLongerAvailableFrame {
                header: UDPourHeader::control(
                    FrameType::NoLongerAvailable,
                    message_id,
                    part_count,
                    checksum,
                )
                .unwrap(),
            },
        );

        assert!(matches!(
            action,
            Some(ReceiverAction::Purged {
                key: ReceiverTransferKey {
                    source: purged_source,
                    message_id: MessageId(11),
                },
                reason: ReceiverPurgeReason::NoLongerAvailable,
            }) if purged_source == source()
        ));
    }

    #[test]
    fn accept_no_longer_available_ignores_mismatched_metadata() {
        let mut receiver = ReceiverMachine::new(receiver_config());
        let now = Instant::now();
        let message_id = MessageId(12);
        let checksum = Checksum(crc32c::crc32c(b"abcdefgh"));
        let part_count = PartCount::new(2).unwrap();
        receiver
            .accept_payload(
                source(),
                PayloadFrame {
                    header: UDPourHeader::payload(message_id, PartNumber(0), part_count, checksum),
                    payload: IoPayload::from_static(b"abcd"),
                },
                now,
            )
            .unwrap();

        let wrong_source = SocketAddr::from_str("127.0.0.1:9000").unwrap();
        assert!(
            receiver
                .accept_no_longer_available(
                    wrong_source,
                    NoLongerAvailableFrame {
                        header: UDPourHeader::control(
                            FrameType::NoLongerAvailable,
                            message_id,
                            part_count,
                            checksum,
                        )
                        .unwrap(),
                    },
                )
                .is_none()
        );
        assert!(
            receiver
                .accept_no_longer_available(
                    source(),
                    NoLongerAvailableFrame {
                        header: UDPourHeader::control(
                            FrameType::NoLongerAvailable,
                            message_id,
                            part_count,
                            Checksum(checksum.0 ^ 0xFFFF),
                        )
                        .unwrap(),
                    },
                )
                .is_none()
        );
        assert!(
            receiver
                .accept_no_longer_available(
                    source(),
                    NoLongerAvailableFrame {
                        header: UDPourHeader::control(
                            FrameType::NoLongerAvailable,
                            message_id,
                            PartCount::new(3).unwrap(),
                            checksum,
                        )
                        .unwrap(),
                    },
                )
                .is_none()
        );
    }

    #[test]
    fn zero_length_payload_delivers_and_acks() {
        let mut receiver = ReceiverMachine::new(receiver_config());
        let now = Instant::now();
        let message_id = MessageId(13);
        let checksum = Checksum(crc32c::crc32c(b""));
        let part_count = PartCount::new(1).unwrap();

        let actions = receiver
            .accept_payload(
                source(),
                PayloadFrame {
                    header: UDPourHeader::payload(message_id, PartNumber(0), part_count, checksum),
                    payload: IoPayload::from_static(b""),
                },
                now,
            )
            .unwrap();

        assert!(actions.iter().any(|action| matches!(
            action,
            ReceiverAction::Deliver { payload, .. } if payload.is_empty()
        )));
        assert!(
            actions
                .iter()
                .any(|action| matches!(action, ReceiverAction::SendAck { .. }))
        );
    }

    #[test]
    fn delivered_tombstone_suppresses_duplicate_delivery_and_reacks() {
        let mut receiver = ReceiverMachine::new(receiver_config());
        let now = Instant::now();
        let message_id = MessageId(14);
        let checksum = Checksum(crc32c::crc32c(b"abcdefghijkl"));
        let part_count = PartCount::new(3).unwrap();

        for (offset, part_number, payload) in [
            (0u64, PartNumber(0), IoPayload::from_static(b"abcd")),
            (1u64, PartNumber(1), IoPayload::from_static(b"efgh")),
            (2u64, PartNumber(2), IoPayload::from_static(b"ijkl")),
        ] {
            let actions = receiver
                .accept_payload(
                    source(),
                    PayloadFrame {
                        header: UDPourHeader::payload(
                            message_id,
                            part_number,
                            part_count,
                            checksum,
                        ),
                        payload,
                    },
                    now + Duration::from_millis(offset),
                )
                .unwrap();
            if part_number == PartNumber(2) {
                assert!(actions.iter().any(|action| matches!(
                    action,
                    ReceiverAction::Deliver { payload, .. }
                        if payload.to_vec().as_slice() == b"abcdefghijkl"
                )));
            }
        }

        let mut duplicate_actions = Vec::new();
        for (offset, part_number, payload) in [
            (10u64, PartNumber(0), IoPayload::from_static(b"abcd")),
            (11u64, PartNumber(1), IoPayload::from_static(b"efgh")),
            (12u64, PartNumber(2), IoPayload::from_static(b"ijkl")),
        ] {
            duplicate_actions.extend(
                receiver
                    .accept_payload(
                        source(),
                        PayloadFrame {
                            header: UDPourHeader::payload(
                                message_id,
                                part_number,
                                part_count,
                                checksum,
                            ),
                            payload,
                        },
                        now + Duration::from_millis(offset),
                    )
                    .unwrap(),
            );
        }

        assert!(
            !duplicate_actions
                .iter()
                .any(|action| matches!(action, ReceiverAction::Deliver { .. }))
        );
        assert!(
            duplicate_actions
                .iter()
                .any(|action| matches!(action, ReceiverAction::SendAck { .. }))
        );
    }
}
