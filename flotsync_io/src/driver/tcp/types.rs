//! Listener and connection records used by the TCP driver.

use super::*;

/// Driver-owned listener state for one reserved TCP listener handle.
///
/// Listener handles are reserved before any OS socket exists. Once `Listen` succeeds, `listener`
/// and `local_addr` become live and the entry owns one registered `mio::net::TcpListener`.
#[derive(Debug)]
pub(super) struct TcpListenerEntry {
    pub(super) record: ResourceRecord,
    pub(super) listener: Option<MioTcpListener>,
    pub(super) local_addr: Option<SocketAddr>,
    pub(super) registered: bool,
}

impl TcpListenerEntry {
    pub(super) fn new(record: ResourceRecord) -> Self {
        Self {
            record,
            listener: None,
            local_addr: None,
            registered: false,
        }
    }

    pub(super) fn is_open(&self) -> bool {
        self.listener.is_some()
    }
}

/// Driver-owned connection state for one reserved TCP connection handle.
///
/// In addition to normal outbound/open connections, the same entry type also represents accepted
/// inbound connections that are waiting for their Kompact owner to decide whether to accept or
/// reject them. While `pending_adoption` is set the connection has a live socket but deliberately
/// exposes no readiness interest, so no inbound bytes can outrun session routing.
#[derive(Debug)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "TCP connection lifecycle flags track independent mio/runtime states."
)]
pub(super) struct TcpConnectionEntry {
    pub(super) record: ResourceRecord,
    pub(super) stream: Option<MioTcpStream>,
    pub(super) remote_addr: Option<SocketAddr>,
    pub(super) accepted_from: Option<ListenerId>,
    pub(super) connect_pending: bool,
    pub(super) pending_adoption: bool,
    pub(super) read_suspended: bool,
    pub(super) write_suspended: bool,
    pub(super) registered: bool,
    pub(super) pending_send: Option<PendingTcpSend>,
    pub(super) close_after_flush: bool,
}

impl TcpConnectionEntry {
    pub(super) fn new(record: ResourceRecord) -> Self {
        Self {
            record,
            stream: None,
            remote_addr: None,
            accepted_from: None,
            connect_pending: false,
            pending_adoption: false,
            read_suspended: false,
            write_suspended: false,
            registered: false,
            pending_send: None,
            close_after_flush: false,
        }
    }

    pub(super) fn new_pending_accepted(
        record: ResourceRecord,
        listener_id: ListenerId,
        stream: MioTcpStream,
        peer_addr: SocketAddr,
    ) -> Self {
        Self {
            record,
            stream: Some(stream),
            remote_addr: Some(peer_addr),
            accepted_from: Some(listener_id),
            connect_pending: false,
            pending_adoption: true,
            read_suspended: false,
            write_suspended: false,
            registered: false,
            pending_send: None,
            close_after_flush: false,
        }
    }

    pub(super) fn desired_interest(&self) -> Option<Interest> {
        self.stream.as_ref()?;
        if self.connect_pending {
            return Some(Interest::WRITABLE);
        }
        if self.pending_adoption {
            return None;
        }

        match (self.read_suspended, self.pending_send.is_some()) {
            (false, false) => Some(Interest::READABLE),
            (false, true) => Some(Interest::READABLE.add(Interest::WRITABLE)),
            (true, false) => None,
            (true, true) => Some(Interest::WRITABLE),
        }
    }

    pub(super) fn reset_to_reserved(&mut self) {
        self.stream = None;
        self.remote_addr = None;
        self.accepted_from = None;
        self.connect_pending = false;
        self.pending_adoption = false;
        self.read_suspended = false;
        self.write_suspended = false;
        self.registered = false;
        self.pending_send = None;
        self.close_after_flush = false;
    }

    pub(super) fn is_open(&self) -> bool {
        self.stream.is_some()
    }

    pub(super) fn is_pending_accepted(&self) -> bool {
        self.pending_adoption && self.accepted_from.is_some()
    }

    /// Returns `true` when the connection transitioned into write-suspended state.
    pub(super) fn suspend_write(&mut self) -> bool {
        if self.write_suspended {
            return false;
        }
        self.write_suspended = true;
        true
    }

    /// Returns `true` when the connection transitioned back to accepting new sends.
    pub(super) fn resume_write(&mut self) -> bool {
        if !self.write_suspended {
            return false;
        }
        self.write_suspended = false;
        true
    }
}

/// Newly accepted inbound TCP stream that is not yet associated with a connection slot.
#[derive(Debug)]
pub(in crate::driver) struct AcceptedTcpConnection {
    pub(in crate::driver) stream: MioTcpStream,
    pub(in crate::driver) peer_addr: SocketAddr,
}

/// Listener cleanup result used by the shared runtime to release readiness tokens.
#[derive(Debug)]
pub(in crate::driver) struct ReleasedTcpListener {
    pub(in crate::driver) listener_record: ResourceRecord,
    pub(in crate::driver) pending_connection_records: Vec<ResourceRecord>,
}

#[derive(Debug)]
pub(super) struct PendingTcpSend {
    pub(super) transmission_id: TransmissionId,
    pub(super) buffer: IoPayloadCursor,
}

impl PendingTcpSend {
    pub(super) fn new(transmission_id: TransmissionId, payload: &IoPayload) -> Self {
        Self {
            transmission_id,
            buffer: payload.cursor(),
        }
    }

    pub(super) fn remaining(&self) -> usize {
        self.buffer.remaining()
    }

    pub(super) fn chunk(&self) -> &[u8] {
        self.buffer.chunk()
    }

    pub(super) fn advance(&mut self, cnt: usize) {
        self.buffer.advance(cnt);
    }
}
