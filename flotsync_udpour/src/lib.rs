//! UDPour protocol and state machines.
//!
//! This crate implements one small multipart protocol for carrying one logical
//! payload over one UDP route when that payload does not fit into a single
//! datagram.
//!
//! The crate is split into three layers:
//!
//! - fixed wire types and codec logic
//! - pure Rust sender/receiver state machines that multiplex many logical
//!   transfers without spawning one runtime object per message
//! - a Kompact/`flotsync_io` UDP runtime adapter
//!
//! This protocol belongs to route transport, not semantic delivery.
//!
//! # Scope
//!
//! The protocol defines:
//!
//! - a fixed 20-byte header shared by data and control frames
//! - sender-side message-id allocation, retention, and repair behavior
//! - receiver-side reassembly, retry, and give-up behavior
//! - whole-message checksum validation and confusion handling
//!
//! It deliberately does not define:
//!
//! - discovery or route-candidate ownership
//! - inbound demux ownership above the UDP socket boundary
//! - stream framing
//! - cryptographic protection
//!
//! # Wire Format
//!
//! Every frame starts with the same fixed 20-byte header:
//!
//! ```text
//! +--------+---------+-------+----------+
//! | type   | version | flags | reserved |
//! +--------+---------+-------+----------+
//! | message_id                          |
//! +-------------------------------------+
//! | part_number                         |
//! +-------------------------------------+
//! | part_count                          |
//! +-------------------------------------+
//! | checksum                            |
//! +-------------------------------------+
//! ```
//!
//! Multi-byte integers are encoded in network byte order.
//!
//! `message_id`, `part_count`, and `checksum` are present on all frames,
//! including control traffic, so receivers can cheaply sanity-check that a
//! frame still belongs to the intended logical message before doing more work.
//!
//! `Payload` frames use zero-based `part_number`. Control frames do not refer
//! to a concrete payload slice and therefore always carry `part_number = 0`.
//! The low bit in `flags` marks a sender-side payload retransmission that was
//! emitted in response to `NeedParts`.
//!
//! The current frame family is:
//!
//! - `0x01`: `Payload`
//! - `0x02`: `NoLongerAvailable`
//! - `0x81`: `Ack`
//! - `0x82`: `NeedParts`
//!
//! The top bit separates sender-originated traffic from receiver-originated
//! traffic. `0x00` and `0xFF` are intentionally left unassigned.
//!
//! # Logical Message Identity
//!
//! A logical message is identified by:
//!
//! - the forwarded UDP source address
//! - `message_id`
//!
//! The protocol intentionally does not add a sender-incarnation nonce. Instead
//! it uses `part_count`, `checksum`, and conservative `message_id` reuse policy
//! to turn collisions into lossy confusion rather than silent corruption.
//!
//! # Payload Layout
//!
//! For multi-part messages, every non-final `Payload` frame must carry the same
//! body length. The final part may be shorter. The header does not carry total
//! length in bytes; once the receiver has seen any non-final part, it can infer
//! a close upper bound as:
//!
//! ```text
//! regular_part_size * part_count
//! ```
//!
//! The exact length is the sum of all received part lengths and is therefore
//! known once reassembly completes.
//!
//! # Checksums
//!
//! `checksum` is a whole-message CRC32C over the fully reassembled payload. It
//! is used to detect corruption and confusion after reassembly; it is not meant
//! to provide cryptographic protection.
//!
//! # Sender Behavior
//!
//! The sender:
//!
//! - allocates one sender-local `message_id`
//! - splits one logical payload into `Payload` frames
//! - retains the original parts for a bounded retention window
//! - answers `NeedParts` with retransmitted `Payload` frames
//! - answers late repair requests with `NoLongerAvailable` once retained state
//!   has been purged
//!
//! `message_id` values must not be reused while live and must not be reused
//! immediately after purge. The implementation keeps purged ids in cooldown to
//! reduce the chance that late packets collide with a new logical message that
//! reused the same id.
//!
//! This protocol is intentionally shared-route friendly: the sender does not
//! track missing parts per receiver. Any `NeedParts` is treated as a route-level
//! request, and retransmitted parts may be received redundantly by other
//! listeners on the same route.
//!
//! # Receiver Behaviour
//!
//! The receiver creates reassembly state when it first observes a `Payload`
//! frame for one `(source, message_id)` pair. That state records:
//!
//! - `part_count`
//! - `checksum`
//! - the set of received parts
//! - enough payload storage to reassemble the message
//!
//! If later `Payload` frames for the same `(source, message_id)` disagree on
//! `part_count` or `checksum`, the receiver treats that as confusion and purges
//! the current reassembly state immediately.
//!
//! Once all parts are present, the receiver reassembles the payload and checks
//! the whole-message checksum:
//!
//! - on success, it delivers the payload upward, emits `Ack`, and keeps a short
//!   delivered tombstone so late shared-route repair traffic does not trigger a
//!   second delivery
//! - on checksum mismatch, it purges local state immediately without delivery
//!
//! Incomplete reassembly state is retained only until the receiver give-up
//! timeout expires. That timeout resets whenever a fresh `Payload` part is
//! accepted. Until then the receiver may emit one or more `NeedParts` frames.
//! Delivered tombstones are retained only for a bounded timeout derived from
//! sender retention and `message_id` reuse cooldown.
//!
//! # Runtime Boundary
//!
//! The Kompact runtime in this crate is intentionally narrow:
//!
//! - it runs over one already-open `flotsync_io` UDP socket
//! - it accepts logical outbound sends via actor ask
//! - it emits fully reassembled inbound deliveries
//! - it surfaces route-transport send failures separately from higher-level
//!   semantic-delivery acknowledgments
#![feature(deque_extend_front)]

mod codec;
mod receiver;
mod roaring_helpers;
mod runtime;
#[cfg(test)]
mod runtime_tests;
mod sender;
mod types;
mod wire;

pub use crate::{
    receiver::ReceiverConfig,
    runtime::{
        UDPourComponent,
        UDPourComponentMessage,
        UDPourConfig,
        UDPourConfigError,
        UDPourDeliver,
        UDPourEncodeFailure,
        UDPourPort,
        UDPourSend,
        UDPourSendFailureReason,
        UDPourStateFailure,
        UDPourSubmitResult,
        config_keys,
    },
    sender::SenderConfig,
    types::{Checksum, MessageId, PartCount},
};
