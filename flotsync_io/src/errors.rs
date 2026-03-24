//! Error surface for flotsync_io.

use crate::api::{ConnectionId, ListenerId, SocketId};
use snafu::Snafu;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[non_exhaustive]
pub enum Error {
    #[snafu(display("failed to create mio poll instance for flotsync_io"))]
    CreateDriverPoll { source: std::io::Error },

    #[snafu(display("failed to create mio waker for flotsync_io"))]
    CreateDriverWaker { source: std::io::Error },

    #[snafu(display("failed to spawn flotsync_io driver thread"))]
    SpawnDriverThread { source: std::io::Error },

    #[snafu(display("mio poll loop failed in flotsync_io driver thread"))]
    DriverPoll { source: std::io::Error },

    #[snafu(display("failed to wake flotsync_io driver thread"))]
    DriverWake { source: std::io::Error },

    #[snafu(display("failed to update mio interest for TCP connection {connection_id}"))]
    UpdateTcpInterest {
        connection_id: ConnectionId,
        source: std::io::Error,
    },

    #[snafu(display("flotsync_io driver command channel is closed"))]
    DriverCommandChannelClosed,

    #[snafu(display("flotsync_io driver response channel is closed"))]
    DriverResponseChannelClosed,

    #[snafu(display("flotsync_io driver event channel is closed"))]
    DriverEventChannelClosed,

    #[snafu(display("flotsync_io driver is unavailable"))]
    DriverUnavailable,

    #[snafu(display("flotsync_io driver event receiver is unavailable for this driver instance"))]
    DriverEventReceiverUnavailable,

    #[snafu(display("flotsync_io buffer-pool request channel is closed"))]
    IoBufferRequestChannelClosed,

    #[snafu(display("flotsync_io {pool_kind} buffer-pool state is poisoned"))]
    IoBufferStatePoisoned { pool_kind: &'static str },

    #[snafu(display("flotsync_io driver thread panicked"))]
    DriverThreadPanicked,

    #[snafu(display("invalid flotsync_io pool configuration: {details}"))]
    InvalidIoPoolConfig { details: String },

    #[snafu(display("invalid egress reservation size {requested_bytes} bytes"))]
    InvalidEgressReservationSize { requested_bytes: usize },

    #[snafu(display(
        "requested egress reservation of {requested_bytes} bytes exceeds pool capacity {max_bytes} bytes"
    ))]
    EgressReservationTooLarge {
        requested_bytes: usize,
        max_bytes: usize,
    },

    #[snafu(display(
        "invalid ingress commit length {written_bytes} for chunk capacity {chunk_capacity}"
    ))]
    InvalidIngressCommitLength {
        written_bytes: usize,
        chunk_capacity: usize,
    },

    #[snafu(display("cannot create an empty lease-backed payload"))]
    EmptyIoLease,

    #[snafu(display(
        "invalid IoPayload slice range offset {offset} len {len} for payload length {payload_len}"
    ))]
    InvalidIoPayloadSliceRange {
        offset: usize,
        len: usize,
        payload_len: usize,
    },

    #[snafu(display(
        "egress serialization exceeded the reserved budget: attempted {attempted_bytes} bytes with only {reserved_bytes} bytes reserved"
    ))]
    EgressReservationOverflow {
        reserved_bytes: usize,
        attempted_bytes: usize,
    },

    #[snafu(display(
        "cannot transfer pooled payload ownership because the payload is still shared"
    ))]
    SharedIoPayloadOwnership,

    #[snafu(display(
        "cannot adopt {chunk_count} live chunks into the egress pool because that would exceed the configured max chunk count {max_chunk_count}"
    ))]
    EgressLiveChunkAdoptionExhausted {
        chunk_count: usize,
        max_chunk_count: usize,
    },

    #[snafu(display("unknown TCP listener id {listener_id}"))]
    UnknownListener { listener_id: ListenerId },

    #[snafu(display("unknown TCP connection id {connection_id}"))]
    UnknownConnection { connection_id: ConnectionId },

    #[snafu(display("unknown UDP socket id {socket_id}"))]
    UnknownSocket { socket_id: SocketId },
}
