//! Receiver-side multipart reassembly state machine.
//!
//! This module owns `(source, message_id)` reassembly state, repair polling,
//! whole-message checksum validation, and purge behavior.

use crate::{
    codec::split_need_parts_frames,
    types::{
        AckFrame,
        Checksum,
        DatagramHeader,
        FrameType,
        MessageId,
        NeedPartsFrame,
        NoLongerAvailableFrame,
        PartCount,
        PayloadFrame,
        io_payload_eq,
    },
};
use bytes::Buf;
use flotsync_io::prelude::IoPayload;
use roaring::RoaringBitmap;
use snafu::prelude::*;
use std::{
    collections::{BTreeMap, HashMap},
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
    transfers: HashMap<ReceiverTransferKey, InboundTransfer>,
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
    ) -> Result<Vec<ReceiverAction>, ReceiverError> {
        let key = ReceiverTransferKey {
            source,
            message_id: frame.header.message_id,
        };
        let mut actions = Vec::new();
        let transfer = match self.transfers.remove(&key) {
            Some(transfer) => transfer,
            None => InboundTransfer::new(
                frame.header.part_count,
                frame.header.checksum,
                now,
                &self.config,
            ),
        };

        match transfer.accept_payload(frame, now, &self.config) {
            InboundUpdate::Pending(next) => {
                self.transfers.insert(key, next);
            }
            InboundUpdate::Deliver {
                payload,
                message_id,
                part_count,
                checksum,
            } => {
                actions.push(ReceiverAction::Deliver {
                    source,
                    message_id,
                    payload,
                    part_count,
                    checksum,
                });
                let header =
                    DatagramHeader::control(FrameType::Ack, message_id, part_count, checksum)
                        .expect("Ack metadata gathered from a valid transfer must stay valid");
                actions.push(ReceiverAction::SendAck {
                    source,
                    frame: AckFrame { header },
                });
                actions.push(ReceiverAction::Purged {
                    key,
                    reason: ReceiverPurgeReason::Delivered,
                });
            }
            InboundUpdate::Purged(reason) => {
                actions.push(ReceiverAction::Purged { key, reason });
            }
        }
        Ok(actions)
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
        let transfer = self.transfers.get(&key)?;
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

    /// Advances timeout-driven repair and purge behavior.
    pub fn poll_timeouts(&mut self, now: Instant) -> Result<Vec<ReceiverAction>, ReceiverError> {
        let keys: Vec<_> = self.transfers.keys().copied().collect();
        let mut actions = Vec::new();
        for key in keys {
            let Some(mut transfer) = self.transfers.remove(&key) else {
                continue;
            };
            if transfer.give_up_deadline <= now {
                actions.push(ReceiverAction::Purged {
                    key,
                    reason: ReceiverPurgeReason::TimedOut,
                });
                continue;
            }
            if transfer.repair_deadline <= now {
                // TODO(flotsync-1n9): Keep repair state incremental so we do not rebuild the full
                // missing-part set and re-split roaring bitmaps from scratch on every poll.
                let missing_parts = transfer.missing_parts();
                let frames = split_need_parts_frames(
                    key.message_id,
                    transfer.part_count,
                    transfer.checksum,
                    &missing_parts,
                    self.config.max_need_parts_frame_len,
                )
                .context(SplitNeedPartsSnafu)?;
                if !frames.is_empty() {
                    actions.push(ReceiverAction::SendNeedParts {
                        source: key.source,
                        frames,
                    });
                    transfer.repair_deadline = now + self.config.repair_interval;
                }
            }
            self.transfers.insert(key, transfer);
        }
        Ok(actions)
    }
}

/// Receiver-facing outcomes from one state-machine interaction.
#[derive(Clone, Debug)]
pub(crate) enum ReceiverAction {
    /// Deliver one fully reassembled logical payload upward.
    Deliver {
        source: SocketAddr,
        message_id: MessageId,
        payload: IoPayload,
        part_count: PartCount,
        checksum: Checksum,
    },
    /// Send one successful receiver acknowledgment back to the sender.
    SendAck { source: SocketAddr, frame: AckFrame },
    /// Ask the sender to repeat one or more missing parts.
    SendNeedParts {
        source: SocketAddr,
        frames: Vec<NeedPartsFrame>,
    },
    /// Report that the receiver-side state for one inbound transfer was purged.
    Purged {
        key: ReceiverTransferKey,
        reason: ReceiverPurgeReason,
    },
}

/// Why one inbound multipart transfer was purged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReceiverPurgeReason {
    Confused,
    ChecksumMismatch,
    Delivered,
    NoLongerAvailable,
    TimedOut,
}

/// One receiver-owned in-progress transfer keyed by `(source, message_id)`.
#[derive(Debug)]
struct InboundTransfer {
    part_count: PartCount,
    checksum: Checksum,
    parts: BTreeMap<u32, IoPayload>,
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
            parts: BTreeMap::new(),
            regular_part_size: None,
            give_up_deadline: now + config.give_up_timeout,
            repair_deadline: now + config.repair_interval,
        }
    }

    fn accept_payload(
        mut self,
        frame: PayloadFrame,
        now: Instant,
        config: &ReceiverConfig,
    ) -> InboundUpdate {
        if frame.header.part_count != self.part_count || frame.header.checksum != self.checksum {
            return InboundUpdate::Purged(ReceiverPurgeReason::Confused);
        }
        if frame.header.part_number.0 >= self.part_count.get() {
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
                Some(_) => {}
            }
        }

        if let Some(existing) = self.parts.get(&frame.header.part_number.0) {
            if !io_payload_eq(existing, &frame.payload) {
                return InboundUpdate::Purged(ReceiverPurgeReason::Confused);
            }
        } else {
            self.parts.insert(frame.header.part_number.0, frame.payload);
        }

        self.give_up_deadline = now + config.give_up_timeout;
        self.repair_deadline = now + config.repair_interval;

        if self.parts.len() as u32 != self.part_count.get() {
            return InboundUpdate::Pending(self);
        }

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
    }

    fn missing_parts(&self) -> RoaringBitmap {
        let mut missing = RoaringBitmap::new();
        for part_number in 0..self.part_count.get() {
            if !self.parts.contains_key(&part_number) {
                missing.insert(part_number);
            }
        }
        missing
    }

    fn assemble_payload(&self) -> IoPayload {
        let parts = (0..self.part_count.get())
            .map(|part_number| {
                self.parts
                    .get(&part_number)
                    .expect("complete transfer has all parts")
                    .clone()
            })
            .collect::<Vec<_>>();
        IoPayload::chain(parts)
    }
}

#[derive(Debug)]
enum InboundUpdate {
    Pending(InboundTransfer),
    Deliver {
        payload: IoPayload,
        message_id: MessageId,
        part_count: PartCount,
        checksum: Checksum,
    },
    Purged(ReceiverPurgeReason),
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

/// Receiver-side errors.
#[derive(Debug, Snafu)]
pub(crate) enum ReceiverError {
    #[snafu(display("Failed to split NeedParts frames"))]
    SplitNeedParts { source: crate::codec::CodecError },
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
                        header: DatagramHeader::payload(
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
                        header: DatagramHeader::payload(
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
                        header: DatagramHeader::payload(
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
                    header: DatagramHeader::payload(
                        message_id,
                        PartNumber(0),
                        part_count,
                        checksum,
                    ),
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
                    header: DatagramHeader::payload(
                        message_id,
                        PartNumber(2),
                        part_count,
                        checksum,
                    ),
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
                    header: DatagramHeader::payload(
                        message_id,
                        PartNumber(0),
                        part_count,
                        checksum,
                    ),
                    payload: IoPayload::from_static(b"abcd"),
                },
                now,
            )
            .unwrap();

        let actions = receiver
            .accept_payload(
                source(),
                PayloadFrame {
                    header: DatagramHeader::payload(
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
                    header: DatagramHeader::payload(
                        message_id,
                        PartNumber(0),
                        part_count,
                        checksum,
                    ),
                    payload: IoPayload::from_static(b"abcd"),
                },
                now,
            )
            .unwrap();

        let actions = receiver
            .accept_payload(
                source(),
                PayloadFrame {
                    header: DatagramHeader::payload(
                        message_id,
                        PartNumber(1),
                        part_count,
                        checksum,
                    ),
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
                    header: DatagramHeader::payload(
                        message_id,
                        PartNumber(0),
                        part_count,
                        checksum,
                    ),
                    payload: IoPayload::from_static(b"abcd"),
                },
                now,
            )
            .unwrap();

        let action = receiver.accept_no_longer_available(
            source(),
            NoLongerAvailableFrame {
                header: DatagramHeader::control(
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
                    header: DatagramHeader::payload(
                        message_id,
                        PartNumber(0),
                        part_count,
                        checksum,
                    ),
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
                        header: DatagramHeader::control(
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
                        header: DatagramHeader::control(
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
                        header: DatagramHeader::control(
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
                    header: DatagramHeader::payload(
                        message_id,
                        PartNumber(0),
                        part_count,
                        checksum,
                    ),
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
}
