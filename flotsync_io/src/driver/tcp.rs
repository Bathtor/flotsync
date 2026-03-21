#[cfg(test)]
use super::runtime::ResourceSnapshot;
use super::{DriverEventSink, DriverToken, registry::SlotRegistry, runtime::ResourceRecord};
use crate::{
    api::{
        CloseReason,
        ConnectionId,
        IoPayload,
        ListenerId,
        SendFailureReason,
        TcpEvent,
        TransmissionId,
    },
    errors::{Error, Result, UpdateTcpInterestSnafu},
    logging::RuntimeLogger,
    pool::IngressPool,
};
use bytes::{Buf, Bytes};
use flotsync_utils::option_when;
use mio::{
    Interest,
    Registry,
    net::{TcpListener as MioTcpListener, TcpStream as MioTcpStream},
};
use slog::{debug, error, warn};
use snafu::ResultExt;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::{
    io,
    io::{ErrorKind, Read, Write},
    net::{SocketAddr, TcpStream as StdTcpStream},
};

/// TCP-side runtime state owned by the shared driver.
#[derive(Debug)]
pub(super) struct TcpRuntimeState {
    logger: RuntimeLogger,
    listeners: SlotRegistry<TcpListenerEntry>,
    connections: SlotRegistry<TcpConnectionEntry>,
}

/// Driver-owned listener state for one reserved TCP listener handle.
///
/// Listener handles are reserved before any OS socket exists. Once `Listen` succeeds, `listener`
/// and `local_addr` become live and the entry owns one registered `mio::net::TcpListener`.
#[derive(Debug)]
struct TcpListenerEntry {
    record: ResourceRecord,
    listener: Option<MioTcpListener>,
    local_addr: Option<SocketAddr>,
    registered: bool,
}

impl TcpListenerEntry {
    fn new(record: ResourceRecord) -> Self {
        Self {
            record,
            listener: None,
            local_addr: None,
            registered: false,
        }
    }

    fn is_open(&self) -> bool {
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
struct TcpConnectionEntry {
    record: ResourceRecord,
    stream: Option<MioTcpStream>,
    remote_addr: Option<SocketAddr>,
    accepted_from: Option<ListenerId>,
    connect_pending: bool,
    pending_adoption: bool,
    read_suspended: bool,
    write_suspended: bool,
    registered: bool,
    pending_send: Option<PendingTcpSend>,
    close_after_flush: bool,
}

impl TcpConnectionEntry {
    fn new(record: ResourceRecord) -> Self {
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

    fn new_pending_accepted(
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

    fn desired_interest(&self) -> Option<Interest> {
        if self.stream.is_none() {
            return None;
        }
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

    fn reset_to_reserved(&mut self) {
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

    fn is_open(&self) -> bool {
        self.stream.is_some()
    }

    fn is_pending_accepted(&self) -> bool {
        self.pending_adoption && self.accepted_from.is_some()
    }

    /// Returns `true` when the connection transitioned into write-suspended state.
    fn suspend_write(&mut self) -> bool {
        if self.write_suspended {
            return false;
        }
        self.write_suspended = true;
        true
    }

    /// Returns `true` when the connection transitioned back to accepting new sends.
    fn resume_write(&mut self) -> bool {
        if !self.write_suspended {
            return false;
        }
        self.write_suspended = false;
        true
    }
}

/// Newly accepted inbound TCP stream that is not yet associated with a connection slot.
#[derive(Debug)]
pub(super) struct AcceptedTcpConnection {
    pub(super) stream: MioTcpStream,
    pub(super) peer_addr: SocketAddr,
}

/// Listener cleanup result used by the shared runtime to release readiness tokens.
#[derive(Debug)]
pub(super) struct ReleasedTcpListener {
    pub(super) listener_record: ResourceRecord,
    pub(super) pending_connection_records: Vec<ResourceRecord>,
}

#[derive(Debug)]
struct PendingTcpSend {
    transmission_id: TransmissionId,
    buffer: PendingSendBuffer,
}

impl PendingTcpSend {
    fn new(transmission_id: TransmissionId, payload: IoPayload) -> Self {
        let buffer = match payload {
            IoPayload::Lease(lease) => PendingSendBuffer::Lease(lease.cursor()),
            IoPayload::Bytes(bytes) => PendingSendBuffer::Bytes(bytes),
        };
        Self {
            transmission_id,
            buffer,
        }
    }

    fn remaining(&self) -> usize {
        self.buffer.remaining()
    }

    fn chunk(&self) -> &[u8] {
        self.buffer.chunk()
    }

    fn advance(&mut self, cnt: usize) {
        self.buffer.advance(cnt);
    }
}

#[derive(Debug)]
enum PendingSendBuffer {
    Lease(crate::pool::IoCursor),
    Bytes(Bytes),
}

impl PendingSendBuffer {
    fn remaining(&self) -> usize {
        match self {
            Self::Lease(cursor) => cursor.remaining(),
            Self::Bytes(bytes) => bytes.remaining(),
        }
    }

    fn chunk(&self) -> &[u8] {
        match self {
            Self::Lease(cursor) => cursor.chunk(),
            Self::Bytes(bytes) => bytes.chunk(),
        }
    }

    fn advance(&mut self, cnt: usize) {
        match self {
            Self::Lease(cursor) => cursor.advance(cnt),
            Self::Bytes(bytes) => bytes.advance(cnt),
        }
    }
}

impl TcpRuntimeState {
    pub(super) fn new(logger: RuntimeLogger) -> Self {
        Self {
            logger,
            listeners: SlotRegistry::default(),
            connections: SlotRegistry::default(),
        }
    }

    pub(super) fn next_listener_slot(&self) -> usize {
        self.listeners.next_slot()
    }

    pub(super) fn next_connection_slot(&self) -> usize {
        self.connections.next_slot()
    }

    pub(super) fn reserve_listener(&mut self, listener_id: ListenerId, token: DriverToken) {
        let slot = self
            .listeners
            .reserve(TcpListenerEntry::new(ResourceRecord::new(token)));
        debug_assert_eq!(slot, listener_id.0);
    }

    pub(super) fn reserve_connection(&mut self, connection_id: ConnectionId, token: DriverToken) {
        let slot = self
            .connections
            .reserve(TcpConnectionEntry::new(ResourceRecord::new(token)));
        debug_assert_eq!(slot, connection_id.0);
    }

    pub(super) fn release_listener(
        &mut self,
        listener_id: ListenerId,
        registry: &Registry,
    ) -> Result<ReleasedTcpListener> {
        let entry = self
            .listeners
            .remove(listener_id.0)
            .ok_or(Error::UnknownListener { listener_id })?;
        let mut listener = entry.listener;
        if let Some(listener) = listener.as_mut() {
            if entry.registered {
                let deregister_result = registry.deregister(listener);
                if let Err(error) = deregister_result {
                    warn!(
                        self.logger,
                        "failed to deregister TCP listener {} during release: {}",
                        listener_id,
                        error
                    );
                }
            }
        }

        let pending_connection_ids = self.pending_connections_for_listener(listener_id);
        let mut pending_connection_records = Vec::with_capacity(pending_connection_ids.len());
        for connection_id in pending_connection_ids {
            let record = self.release_pending_connection(connection_id, registry)?;
            pending_connection_records.push(record);
        }

        Ok(ReleasedTcpListener {
            listener_record: entry.record,
            pending_connection_records,
        })
    }

    pub(super) fn release_connection(
        &mut self,
        connection_id: ConnectionId,
        registry: &Registry,
    ) -> Result<ResourceRecord> {
        let entry = self
            .connections
            .remove(connection_id.0)
            .ok_or(Error::UnknownConnection { connection_id })?;
        let mut stream = entry.stream;
        if let Some(stream) = stream.as_mut() {
            if entry.registered {
                let deregister_result = registry.deregister(stream);
                if let Err(error) = deregister_result {
                    warn!(
                        self.logger,
                        "failed to deregister TCP connection {} during release: {}",
                        connection_id,
                        error
                    );
                }
            }
        }
        Ok(entry.record)
    }

    pub(super) fn record_listener_readiness_hit(&mut self, listener_id: ListenerId) {
        if let Some(entry) = self.listeners.get_mut(listener_id.0) {
            entry.record.readiness_hits += 1;
        }
    }

    pub(super) fn record_connection_readiness_hit(&mut self, connection_id: ConnectionId) {
        if let Some(entry) = self.connections.get_mut(connection_id.0) {
            entry.record.readiness_hits += 1;
        }
    }

    pub(super) fn handle_connect(
        &mut self,
        connection_id: ConnectionId,
        local_addr: Option<SocketAddr>,
        remote_addr: SocketAddr,
        registry: &Registry,
        event_sink: &dyn DriverEventSink,
    ) -> Result<()> {
        let Some(entry) = self.connections.get_mut(connection_id.0) else {
            event_sink.publish(super::DriverEvent::Tcp(TcpEvent::ConnectFailed {
                connection_id,
                remote_addr,
                error_kind: ErrorKind::NotFound,
            }))?;
            return Ok(());
        };
        if entry.is_open() {
            event_sink.publish(super::DriverEvent::Tcp(TcpEvent::ConnectFailed {
                connection_id,
                remote_addr,
                error_kind: ErrorKind::AlreadyExists,
            }))?;
            return Ok(());
        }

        let stream_result = connect_tcp_stream(local_addr, remote_addr);
        let stream = match stream_result {
            Ok(stream) => stream,
            Err(error) => {
                event_sink.publish(super::DriverEvent::Tcp(TcpEvent::ConnectFailed {
                    connection_id,
                    remote_addr,
                    error_kind: error.kind(),
                }))?;
                return Ok(());
            }
        };

        let connect_pending = match check_tcp_connect_result(&stream) {
            Ok(true) => false,
            Ok(false) => true,
            Err(error_kind) => {
                event_sink.publish(super::DriverEvent::Tcp(TcpEvent::ConnectFailed {
                    connection_id,
                    remote_addr,
                    error_kind,
                }))?;
                return Ok(());
            }
        };

        entry.stream = Some(stream);
        entry.remote_addr = Some(remote_addr);
        entry.connect_pending = connect_pending;
        entry.read_suspended = false;
        entry.pending_send = None;
        entry.close_after_flush = false;
        let apply_result = apply_connection_interest(entry, registry);
        if let Err(error) = apply_result {
            entry.reset_to_reserved();
            event_sink.publish(super::DriverEvent::Tcp(TcpEvent::ConnectFailed {
                connection_id,
                remote_addr,
                error_kind: error.kind(),
            }))?;
            return Ok(());
        }

        if !connect_pending {
            event_sink.publish(super::DriverEvent::Tcp(TcpEvent::Connected {
                connection_id,
                peer_addr: remote_addr,
            }))?;
        }

        Ok(())
    }

    pub(super) fn handle_listen(
        &mut self,
        listener_id: ListenerId,
        local_addr: SocketAddr,
        registry: &Registry,
        event_sink: &dyn DriverEventSink,
    ) -> Result<()> {
        let Some(entry) = self.listeners.get_mut(listener_id.0) else {
            event_sink.publish(super::DriverEvent::Tcp(TcpEvent::ListenFailed {
                listener_id,
                local_addr,
                error_kind: ErrorKind::NotFound,
            }))?;
            return Ok(());
        };
        if entry.is_open() {
            event_sink.publish(super::DriverEvent::Tcp(TcpEvent::ListenFailed {
                listener_id,
                local_addr,
                error_kind: ErrorKind::AlreadyExists,
            }))?;
            return Ok(());
        }

        let listener_result = MioTcpListener::bind(local_addr);
        let mut listener = match listener_result {
            Ok(listener) => listener,
            Err(error) => {
                event_sink.publish(super::DriverEvent::Tcp(TcpEvent::ListenFailed {
                    listener_id,
                    local_addr,
                    error_kind: error.kind(),
                }))?;
                return Ok(());
            }
        };
        let bound_addr = match listener.local_addr() {
            Ok(bound_addr) => bound_addr,
            Err(error) => {
                event_sink.publish(super::DriverEvent::Tcp(TcpEvent::ListenFailed {
                    listener_id,
                    local_addr,
                    error_kind: error.kind(),
                }))?;
                return Ok(());
            }
        };
        let register_result =
            registry.register(&mut listener, entry.record.token, Interest::READABLE);
        if let Err(error) = register_result {
            event_sink.publish(super::DriverEvent::Tcp(TcpEvent::ListenFailed {
                listener_id,
                local_addr,
                error_kind: error.kind(),
            }))?;
            return Ok(());
        }

        entry.listener = Some(listener);
        entry.local_addr = Some(bound_addr);
        entry.registered = true;
        event_sink.publish(super::DriverEvent::Tcp(TcpEvent::Listening {
            listener_id,
            local_addr: bound_addr,
        }))?;
        Ok(())
    }

    pub(super) fn close_listener(
        &mut self,
        listener_id: ListenerId,
        registry: &Registry,
        event_sink: &dyn DriverEventSink,
    ) -> Result<Option<ReleasedTcpListener>> {
        let release_result = self.release_listener(listener_id, registry);
        let released = match release_result {
            Ok(released) => released,
            Err(Error::UnknownListener { .. }) => {
                warn!(
                    self.logger,
                    "ignored TCP listener close for unknown listener {}", listener_id
                );
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        event_sink.publish(super::DriverEvent::Tcp(TcpEvent::ListenerClosed {
            listener_id,
        }))?;
        Ok(Some(released))
    }

    pub(super) fn accept_next(
        &mut self,
        listener_id: ListenerId,
    ) -> Result<Option<AcceptedTcpConnection>> {
        let Some(entry) = self.listeners.get_mut(listener_id.0) else {
            debug!(
                self.logger,
                "ignoring stale TCP listener readiness for unknown listener {}", listener_id
            );
            return Ok(None);
        };
        let Some(listener) = entry.listener.as_mut() else {
            debug!(
                self.logger,
                "ignoring stale TCP listener readiness for listener {} without OS handle",
                listener_id
            );
            return Ok(None);
        };

        let accept_result = listener.accept();
        match accept_result {
            Ok((stream, peer_addr)) => Ok(Some(AcceptedTcpConnection { stream, peer_addr })),
            Err(error) if error.kind() == ErrorKind::WouldBlock => Ok(None),
            Err(error) => {
                error!(
                    self.logger,
                    "TCP listener {} accept failed: {}", listener_id, error
                );
                Ok(None)
            }
        }
    }

    pub(super) fn install_accepted_connection(
        &mut self,
        connection_id: ConnectionId,
        listener_id: ListenerId,
        accepted: AcceptedTcpConnection,
    ) -> Result<()> {
        let Some(entry) = self.connections.get_mut(connection_id.0) else {
            return Err(Error::UnknownConnection { connection_id });
        };

        let record = entry.record;
        *entry = TcpConnectionEntry::new_pending_accepted(
            record,
            listener_id,
            accepted.stream,
            accepted.peer_addr,
        );
        Ok(())
    }

    pub(super) fn adopt_accepted_connection(
        &mut self,
        connection_id: ConnectionId,
        registry: &Registry,
    ) -> Result<()> {
        let Some(entry) = self.connections.get_mut(connection_id.0) else {
            return Err(Error::UnknownConnection { connection_id });
        };
        if !entry.is_pending_accepted() {
            warn!(
                self.logger,
                "ignored TCP adopt for connection {} that is not pending adoption", connection_id
            );
            return Ok(());
        }

        entry.pending_adoption = false;
        entry.accepted_from = None;
        let update_interest_result = apply_connection_interest(entry, registry);
        update_interest_result.context(UpdateTcpInterestSnafu { connection_id })?;
        Ok(())
    }

    pub(super) fn reject_accepted_connection(
        &mut self,
        connection_id: ConnectionId,
        registry: &Registry,
    ) -> Result<Option<ResourceRecord>> {
        let Some(entry) = self.connections.get(connection_id.0) else {
            warn!(
                self.logger,
                "ignored TCP reject for unknown pending connection {}", connection_id
            );
            return Ok(None);
        };
        if !entry.is_pending_accepted() {
            warn!(
                self.logger,
                "ignored TCP reject for connection {} that is not pending adoption", connection_id
            );
            return Ok(None);
        }

        let record = self.release_connection(connection_id, registry)?;
        Ok(Some(record))
    }

    pub(super) fn handle_send(
        &mut self,
        connection_id: ConnectionId,
        transmission_id: TransmissionId,
        payload: IoPayload,
        registry: &Registry,
        event_sink: &dyn DriverEventSink,
    ) -> Result<Option<ResourceRecord>> {
        let Some(entry) = self.connections.get_mut(connection_id.0) else {
            event_sink.publish(super::DriverEvent::Tcp(TcpEvent::SendNack {
                connection_id,
                transmission_id,
                reason: SendFailureReason::Closed,
            }))?;
            return Ok(None);
        };
        if !entry.is_open() {
            event_sink.publish(super::DriverEvent::Tcp(TcpEvent::SendNack {
                connection_id,
                transmission_id,
                reason: SendFailureReason::InvalidState,
            }))?;
            return Ok(None);
        }
        if entry.connect_pending || entry.pending_adoption {
            event_sink.publish(super::DriverEvent::Tcp(TcpEvent::SendNack {
                connection_id,
                transmission_id,
                reason: SendFailureReason::InvalidState,
            }))?;
            return Ok(None);
        }
        if entry.pending_send.is_some() {
            if entry.suspend_write() {
                event_sink.publish(super::DriverEvent::Tcp(TcpEvent::WriteSuspended {
                    connection_id,
                }))?;
            }
            event_sink.publish(super::DriverEvent::Tcp(TcpEvent::SendNack {
                connection_id,
                transmission_id,
                reason: SendFailureReason::Backpressure,
            }))?;
            return Ok(None);
        }
        if payload.is_empty() {
            event_sink.publish(super::DriverEvent::Tcp(TcpEvent::SendAck {
                connection_id,
                transmission_id,
            }))?;
            return Ok(None);
        }

        entry.pending_send = Some(PendingTcpSend::new(transmission_id, payload));
        self.flush_pending_send(connection_id, registry, event_sink)
    }

    pub(super) fn handle_close(
        &mut self,
        connection_id: ConnectionId,
        abort: bool,
        registry: &Registry,
        event_sink: &dyn DriverEventSink,
    ) -> Result<Option<ResourceRecord>> {
        if abort {
            let record =
                self.close_connection(connection_id, registry, CloseReason::Aborted, event_sink)?;
            return Ok(Some(record));
        }

        let Some(entry) = self.connections.get_mut(connection_id.0) else {
            warn!(
                self.logger,
                "ignored TCP close for unknown connection {}", connection_id
            );
            return Ok(None);
        };
        if entry.pending_adoption {
            let record = self.release_connection(connection_id, registry)?;
            return Ok(Some(record));
        }

        if entry.pending_send.is_some() {
            entry.close_after_flush = true;
            let update_interest_result = apply_connection_interest(entry, registry);
            update_interest_result.context(UpdateTcpInterestSnafu { connection_id })?;
            return Ok(None);
        }

        let record =
            self.close_connection(connection_id, registry, CloseReason::Graceful, event_sink)?;
        Ok(Some(record))
    }

    pub(super) fn handle_ready(
        &mut self,
        connection_id: ConnectionId,
        registry: &Registry,
        ingress_pool: &IngressPool,
        event_sink: &dyn DriverEventSink,
        is_readable: bool,
        is_writable: bool,
    ) -> Result<Option<ResourceRecord>> {
        let should_complete_connect = match self.connections.get(connection_id.0) {
            Some(entry) => entry.connect_pending && (is_readable || is_writable),
            None => false,
        };
        if should_complete_connect {
            let closed_record = self.finish_connect(connection_id, registry, event_sink)?;
            if closed_record.is_some() {
                return Ok(closed_record);
            }
        }

        if is_writable {
            let closed_record = self.flush_pending_send(connection_id, registry, event_sink)?;
            if closed_record.is_some() {
                return Ok(closed_record);
            }
        }
        if is_readable {
            return self.handle_readable(connection_id, registry, ingress_pool, event_sink);
        }

        Ok(None)
    }

    pub(super) fn resume_suspended_reads(
        &mut self,
        registry: &Registry,
        ingress_pool: &IngressPool,
        event_sink: &dyn DriverEventSink,
    ) -> Result<()> {
        if !ingress_pool.has_available_capacity()? {
            return Ok(());
        }

        let suspended_connections: Vec<ConnectionId> = self
            .connections
            .entries()
            .iter()
            .enumerate()
            .filter_map(|(slot, entry_opt)| {
                entry_opt.as_ref().and_then(|entry| {
                    option_when!(
                        entry.read_suspended
                            && entry.is_open()
                            && !entry.connect_pending
                            && !entry.pending_adoption,
                        ConnectionId(slot)
                    )
                })
            })
            .collect();

        for connection_id in suspended_connections {
            let resumed = self.resume_read(connection_id, registry)?;
            if resumed {
                event_sink.publish(super::DriverEvent::Tcp(TcpEvent::ReadResumed {
                    connection_id,
                }))?;
            }
        }

        Ok(())
    }

    #[cfg(test)]
    pub(super) fn listener_snapshots(&self) -> Vec<ResourceSnapshot> {
        self.listeners
            .iter()
            .map(|(slot, entry)| ResourceSnapshot {
                slot,
                token: entry.record.token,
                readiness_hits: entry.record.readiness_hits,
            })
            .collect()
    }

    #[cfg(test)]
    pub(super) fn connection_snapshots(&self) -> Vec<ResourceSnapshot> {
        self.connections
            .iter()
            .map(|(slot, entry)| ResourceSnapshot {
                slot,
                token: entry.record.token,
                readiness_hits: entry.record.readiness_hits,
            })
            .collect()
    }

    fn finish_connect(
        &mut self,
        connection_id: ConnectionId,
        registry: &Registry,
        event_sink: &dyn DriverEventSink,
    ) -> Result<Option<ResourceRecord>> {
        let Some(entry) = self.connections.get_mut(connection_id.0) else {
            debug!(
                self.logger,
                "ignoring stale TCP readiness for unknown connection {}", connection_id
            );
            return Ok(None);
        };
        if !entry.connect_pending {
            return Ok(None);
        }

        let remote_addr = entry
            .remote_addr
            .expect("connecting TCP entry missing remote address");
        let connect_result = {
            let stream = entry
                .stream
                .as_ref()
                .expect("connecting TCP entry missing stream handle");
            check_tcp_connect_result(stream)
        };
        match connect_result {
            Ok(false) => Ok(None),
            Ok(true) => {
                entry.connect_pending = false;
                let update_interest_result = apply_connection_interest(entry, registry);
                update_interest_result.context(UpdateTcpInterestSnafu { connection_id })?;
                event_sink.publish(super::DriverEvent::Tcp(TcpEvent::Connected {
                    connection_id,
                    peer_addr: remote_addr,
                }))?;
                Ok(None)
            }
            Err(error_kind) => {
                let reset_error = reset_connection_after_failure(entry, registry);
                if let Err(error) = reset_error {
                    warn!(
                        self.logger,
                        "failed to reset TCP connection {} after connect failure: {}",
                        connection_id,
                        error
                    );
                }
                event_sink.publish(super::DriverEvent::Tcp(TcpEvent::ConnectFailed {
                    connection_id,
                    remote_addr,
                    error_kind,
                }))?;
                Ok(None)
            }
        }
    }

    fn flush_pending_send(
        &mut self,
        connection_id: ConnectionId,
        registry: &Registry,
        event_sink: &dyn DriverEventSink,
    ) -> Result<Option<ResourceRecord>> {
        let Some(entry) = self.connections.get_mut(connection_id.0) else {
            debug!(
                self.logger,
                "ignoring stale TCP writability for unknown connection {}", connection_id
            );
            return Ok(None);
        };
        if entry.connect_pending || entry.pending_send.is_none() {
            return Ok(None);
        }

        loop {
            let write_result = {
                let stream = entry
                    .stream
                    .as_mut()
                    .expect("open TCP entry missing stream handle");
                let pending_send = entry
                    .pending_send
                    .as_mut()
                    .expect("TCP entry lost pending send during flush");
                stream.write(pending_send.chunk())
            };

            match write_result {
                Ok(0) => {
                    let transmission_id = entry
                        .pending_send
                        .as_ref()
                        .expect("TCP pending send missing for zero-length write")
                        .transmission_id;
                    event_sink.publish(super::DriverEvent::Tcp(TcpEvent::SendNack {
                        connection_id,
                        transmission_id,
                        reason: SendFailureReason::Closed,
                    }))?;
                    let record = self.close_connection(
                        connection_id,
                        registry,
                        CloseReason::Aborted,
                        event_sink,
                    )?;
                    return Ok(Some(record));
                }
                Ok(bytes_written) => {
                    let pending_send = entry
                        .pending_send
                        .as_mut()
                        .expect("TCP pending send missing after successful write");
                    pending_send.advance(bytes_written);
                    if pending_send.remaining() == 0 {
                        let transmission_id = pending_send.transmission_id;
                        entry.pending_send = None;
                        event_sink.publish(super::DriverEvent::Tcp(TcpEvent::SendAck {
                            connection_id,
                            transmission_id,
                        }))?;
                        let write_resumed = entry.resume_write();
                        if entry.close_after_flush {
                            let record = self.close_connection(
                                connection_id,
                                registry,
                                CloseReason::Graceful,
                                event_sink,
                            )?;
                            return Ok(Some(record));
                        }
                        if write_resumed {
                            event_sink.publish(super::DriverEvent::Tcp(
                                TcpEvent::WriteResumed { connection_id },
                            ))?;
                        }
                        apply_connection_interest(entry, registry)
                            .context(UpdateTcpInterestSnafu { connection_id })?;
                        return Ok(None);
                    }
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    if entry.suspend_write() {
                        event_sink.publish(super::DriverEvent::Tcp(TcpEvent::WriteSuspended {
                            connection_id,
                        }))?;
                    }
                    apply_connection_interest(entry, registry)
                        .context(UpdateTcpInterestSnafu { connection_id })?;
                    return Ok(None);
                }
                Err(error) => {
                    let transmission_id = entry
                        .pending_send
                        .as_ref()
                        .expect("TCP pending send missing after write error")
                        .transmission_id;
                    event_sink.publish(super::DriverEvent::Tcp(TcpEvent::SendNack {
                        connection_id,
                        transmission_id,
                        reason: SendFailureReason::from(&error),
                    }))?;
                    let record = self.close_connection(
                        connection_id,
                        registry,
                        CloseReason::Aborted,
                        event_sink,
                    )?;
                    return Ok(Some(record));
                }
            }
        }
    }

    fn handle_readable(
        &mut self,
        connection_id: ConnectionId,
        registry: &Registry,
        ingress_pool: &IngressPool,
        event_sink: &dyn DriverEventSink,
    ) -> Result<Option<ResourceRecord>> {
        loop {
            let mut ingress_buffer = match ingress_pool.try_acquire()? {
                Some(buffer) => buffer,
                None => {
                    let suspended = self.suspend_read(connection_id, registry)?;
                    if suspended {
                        event_sink.publish(super::DriverEvent::Tcp(TcpEvent::ReadSuspended {
                            connection_id,
                        }))?;
                    }
                    return Ok(None);
                }
            };

            let read_result = {
                let Some(entry) = self.connections.get_mut(connection_id.0) else {
                    debug!(
                        self.logger,
                        "ignoring stale TCP readability for unknown connection {}", connection_id
                    );
                    return Ok(None);
                };
                let Some(stream) = entry.stream.as_mut() else {
                    debug!(
                        self.logger,
                        "ignoring stale TCP readability for connection {} without OS handle",
                        connection_id
                    );
                    return Ok(None);
                };
                stream.read(ingress_buffer.writable())
            };

            match read_result {
                Ok(0) => {
                    drop(ingress_buffer);
                    let record = self.close_connection(
                        connection_id,
                        registry,
                        CloseReason::Graceful,
                        event_sink,
                    )?;
                    return Ok(Some(record));
                }
                Ok(bytes_read) => {
                    let lease = ingress_buffer.commit(bytes_read)?;
                    event_sink.publish(super::DriverEvent::Tcp(TcpEvent::Received {
                        connection_id,
                        payload: IoPayload::Lease(lease),
                    }))?;
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    return Ok(None);
                }
                Err(error) => {
                    error!(
                        self.logger,
                        "TCP connection {} recv failed: {}", connection_id, error
                    );
                    let record = self.close_connection(
                        connection_id,
                        registry,
                        CloseReason::Aborted,
                        event_sink,
                    )?;
                    return Ok(Some(record));
                }
            }
        }
    }

    /// Returns `true` when the connection transitioned from readable to driver-suspended.
    fn suspend_read(&mut self, connection_id: ConnectionId, registry: &Registry) -> Result<bool> {
        let Some(entry) = self.connections.get_mut(connection_id.0) else {
            return Ok(false);
        };
        if entry.read_suspended || entry.connect_pending {
            return Ok(false);
        }
        entry.read_suspended = true;
        let update_interest_result = apply_connection_interest(entry, registry);
        update_interest_result.context(UpdateTcpInterestSnafu { connection_id })?;
        Ok(true)
    }

    /// Returns `true` when the connection transitioned from driver-suspended back to readable.
    fn resume_read(&mut self, connection_id: ConnectionId, registry: &Registry) -> Result<bool> {
        let Some(entry) = self.connections.get_mut(connection_id.0) else {
            return Ok(false);
        };
        if !entry.read_suspended {
            return Ok(false);
        }
        entry.read_suspended = false;
        let update_interest_result = apply_connection_interest(entry, registry);
        update_interest_result.context(UpdateTcpInterestSnafu { connection_id })?;
        Ok(true)
    }

    fn close_connection(
        &mut self,
        connection_id: ConnectionId,
        registry: &Registry,
        reason: CloseReason,
        event_sink: &dyn DriverEventSink,
    ) -> Result<ResourceRecord> {
        let record = self.release_connection(connection_id, registry)?;
        event_sink.publish(super::DriverEvent::Tcp(TcpEvent::Closed {
            connection_id,
            reason,
        }))?;
        Ok(record)
    }

    fn pending_connections_for_listener(&self, listener_id: ListenerId) -> Vec<ConnectionId> {
        self.connections
            .entries()
            .iter()
            .enumerate()
            .filter_map(|(slot, entry_opt)| {
                let entry = entry_opt.as_ref()?;
                option_when!(
                    entry.pending_adoption && entry.accepted_from == Some(listener_id),
                    ConnectionId(slot)
                )
            })
            .collect()
    }

    fn release_pending_connection(
        &mut self,
        connection_id: ConnectionId,
        registry: &Registry,
    ) -> Result<ResourceRecord> {
        let entry = self
            .connections
            .remove(connection_id.0)
            .ok_or(Error::UnknownConnection { connection_id })?;
        let mut stream = entry.stream;
        if let Some(stream) = stream.as_mut() {
            if entry.registered {
                let deregister_result = registry.deregister(stream);
                if let Err(error) = deregister_result {
                    warn!(
                        self.logger,
                        "failed to deregister pending TCP connection {} during release: {}",
                        connection_id,
                        error
                    );
                }
            }
        }
        Ok(entry.record)
    }
}

fn apply_connection_interest(
    entry: &mut TcpConnectionEntry,
    registry: &Registry,
) -> io::Result<()> {
    let desired_interest = entry.desired_interest();
    let Some(stream) = entry.stream.as_mut() else {
        entry.registered = false;
        return Ok(());
    };

    match desired_interest {
        Some(interest) => {
            if entry.registered {
                registry.reregister(stream, entry.record.token, interest)?;
            } else {
                registry.register(stream, entry.record.token, interest)?;
                entry.registered = true;
            }
        }
        None => {
            if entry.registered {
                registry.deregister(stream)?;
                entry.registered = false;
            }
        }
    }

    Ok(())
}

fn reset_connection_after_failure(
    entry: &mut TcpConnectionEntry,
    registry: &Registry,
) -> io::Result<()> {
    if let Some(stream) = entry.stream.as_mut() {
        if entry.registered {
            registry.deregister(stream)?;
        }
    }
    entry.reset_to_reserved();
    Ok(())
}

/// Checks non-blocking TCP connect completion according to `mio::TcpStream::connect` semantics.
///
/// `mio` requires callers to:
/// 1. inspect `take_error()` first
/// 2. then inspect `peer_addr()`
/// 3. treat `ErrorKind::NotConnected` from `peer_addr()` as \"still pending\"
///
/// The helper returns `Ok(true)` when the stream is connected, `Ok(false)` when it is still
/// pending, and `Err(kind)` when the connect attempt has failed.
fn check_tcp_connect_result(stream: &MioTcpStream) -> std::result::Result<bool, ErrorKind> {
    let take_error_result = stream.take_error();
    let pending_error = match take_error_result {
        Ok(error) => error,
        Err(error) => return Err(error.kind()),
    };
    if let Some(error) = pending_error {
        return Err(error.kind());
    }

    match stream.peer_addr() {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == ErrorKind::NotConnected => Ok(false),
        Err(error) => Err(error.kind()),
    }
}

fn connect_tcp_stream(
    local_addr: Option<SocketAddr>,
    remote_addr: SocketAddr,
) -> io::Result<MioTcpStream> {
    if let Some(local_addr) = local_addr {
        return connect_tcp_stream_with_local_addr(local_addr, remote_addr);
    }
    MioTcpStream::connect(remote_addr)
}

/// Connects one TCP stream after first binding it to a caller-selected local address.
///
/// `mio` does not currently expose this path directly, so the implementation uses `socket2` and
/// converts the resulting non-blocking `std::net::TcpStream` back into `mio`.
fn connect_tcp_stream_with_local_addr(
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
) -> io::Result<MioTcpStream> {
    let domain = match remote_addr {
        SocketAddr::V4(_) => Domain::IPV4,
        SocketAddr::V6(_) => Domain::IPV6,
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_nonblocking(true)?;
    socket.bind(&SockAddr::from(local_addr))?;

    match socket.connect(&SockAddr::from(remote_addr)) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::WouldBlock => {}
        Err(error) => return Err(error),
    }

    let stream: StdTcpStream = socket.into();
    Ok(MioTcpStream::from_std(stream))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        driver::{
            DriverCommand,
            DriverConfig,
            DriverEvent,
            DriverRequest,
            IoDriver,
            wait_for_request,
        },
        prelude::Result,
        test_support::init_test_logger,
    };
    use std::{
        net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener, TcpStream},
        sync::mpsc,
        time::{Duration, Instant},
    };

    fn resolve_request<T>(request: Result<DriverRequest<T>>) -> T {
        let request = request.expect("enqueue driver request");
        wait_for_request(request).expect("resolve driver request")
    }

    fn localhost(port: u16) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
    }

    fn wait_for_event(driver: &IoDriver) -> DriverEvent {
        let deadline = Instant::now() + Duration::from_secs(2);

        loop {
            let event = driver.try_next_event().expect("read driver event");
            if let Some(event) = event {
                return event;
            }

            if Instant::now() >= deadline {
                panic!("timed out waiting for flotsync_io driver event");
            }

            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn assert_no_event(driver: &IoDriver, timeout: Duration) {
        let deadline = Instant::now() + timeout;

        loop {
            let event = driver.try_next_event().expect("read driver event");
            if let Some(event) = event {
                panic!("unexpected flotsync_io driver event: {event:?}");
            }

            if Instant::now() >= deadline {
                return;
            }

            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn payload_bytes(payload: IoPayload) -> Bytes {
        match payload {
            IoPayload::Lease(lease) => lease.create_byte_clone(),
            IoPayload::Bytes(bytes) => bytes,
        }
    }

    #[test]
    fn tcp_listen_failure_is_reported() {
        init_test_logger();

        let occupied = TcpListener::bind(("127.0.0.1", 0)).expect("bind occupied TCP listener");
        let local_addr = occupied.local_addr().expect("occupied listener addr");

        let driver = IoDriver::start(DriverConfig::default()).expect("driver starts");
        let listener_id = resolve_request(driver.reserve_listener());
        driver
            .dispatch(DriverCommand::Tcp(crate::api::TcpCommand::Listen {
                listener_id,
                local_addr,
            }))
            .expect("dispatch TCP listen");

        loop {
            match wait_for_event(&driver) {
                DriverEvent::Tcp(TcpEvent::ListenFailed {
                    listener_id: failed_id,
                    local_addr: failed_addr,
                    ..
                }) if failed_id == listener_id => {
                    assert_eq!(failed_addr, local_addr);
                    break;
                }
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for TCP listen failure: {:?}",
                        other
                    );
                }
            }
        }

        driver.shutdown().expect("driver shuts down");
    }

    #[test]
    fn tcp_listener_accept_requires_adoption_before_reads() {
        init_test_logger();

        let driver = IoDriver::start(DriverConfig::default()).expect("driver starts");
        let listener_id = resolve_request(driver.reserve_listener());
        driver
            .dispatch(DriverCommand::Tcp(crate::api::TcpCommand::Listen {
                listener_id,
                local_addr: localhost(0),
            }))
            .expect("dispatch TCP listen");

        let listener_addr = loop {
            match wait_for_event(&driver) {
                DriverEvent::Tcp(TcpEvent::Listening {
                    listener_id: listening_id,
                    local_addr,
                }) if listening_id == listener_id => {
                    break local_addr;
                }
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for TCP listening event: {:?}",
                        other
                    );
                }
            }
        };

        let mut client = TcpStream::connect(listener_addr).expect("connect TCP client");
        let accepted_connection_id = loop {
            match wait_for_event(&driver) {
                DriverEvent::Tcp(TcpEvent::Accepted {
                    listener_id: accepted_listener_id,
                    connection_id,
                    ..
                }) if accepted_listener_id == listener_id => {
                    break connection_id;
                }
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for TCP accepted event: {:?}",
                        other
                    );
                }
            }
        };

        client
            .write_all(b"pending")
            .expect("write pending TCP payload");
        assert_no_event(&driver, Duration::from_millis(50));

        driver
            .dispatch(DriverCommand::Tcp(crate::api::TcpCommand::AdoptAccepted {
                connection_id: accepted_connection_id,
            }))
            .expect("dispatch TCP adopt accepted");

        loop {
            match wait_for_event(&driver) {
                DriverEvent::Tcp(TcpEvent::Received {
                    connection_id,
                    payload,
                }) if connection_id == accepted_connection_id => {
                    assert_eq!(payload_bytes(payload), b"pending"[..]);
                    break;
                }
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for adopted TCP payload: {:?}",
                        other
                    );
                }
            }
        }

        drop(client);
        driver.shutdown().expect("driver shuts down");
    }

    #[test]
    fn tcp_listener_reject_closes_pending_connection() {
        init_test_logger();

        let driver = IoDriver::start(DriverConfig::default()).expect("driver starts");
        let listener_id = resolve_request(driver.reserve_listener());
        driver
            .dispatch(DriverCommand::Tcp(crate::api::TcpCommand::Listen {
                listener_id,
                local_addr: localhost(0),
            }))
            .expect("dispatch TCP listen");

        let listener_addr = loop {
            match wait_for_event(&driver) {
                DriverEvent::Tcp(TcpEvent::Listening {
                    listener_id: listening_id,
                    local_addr,
                }) if listening_id == listener_id => {
                    break local_addr;
                }
                _ => {}
            }
        };

        let mut client = TcpStream::connect(listener_addr).expect("connect TCP client");
        let accepted_connection_id = loop {
            match wait_for_event(&driver) {
                DriverEvent::Tcp(TcpEvent::Accepted {
                    listener_id: accepted_listener_id,
                    connection_id,
                    ..
                }) if accepted_listener_id == listener_id => {
                    break connection_id;
                }
                _ => {}
            }
        };

        driver
            .dispatch(DriverCommand::Tcp(crate::api::TcpCommand::RejectAccepted {
                connection_id: accepted_connection_id,
            }))
            .expect("dispatch TCP reject accepted");

        std::thread::sleep(Duration::from_millis(50));
        let mut buf = [0_u8; 1];
        let read_result = client.read(&mut buf);
        match read_result {
            Ok(0) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::UnexpectedEof
                ) => {}
            other => panic!("unexpected client read result after reject: {other:?}"),
        }
        assert_no_event(&driver, Duration::from_millis(50));

        driver.shutdown().expect("driver shuts down");
    }

    #[test]
    fn tcp_connect_send_and_receive_work() {
        init_test_logger();

        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind TCP listener");
        let remote_addr = listener.local_addr().expect("listener addr");
        let (server_tx, server_rx) = mpsc::sync_channel(1);
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept TCP stream");
            let mut buf = [0_u8; 5];
            stream.read_exact(&mut buf).expect("server read exact");
            server_tx.send(buf).expect("send server payload");
            stream.write_all(b"world").expect("server write response");
        });

        let driver = IoDriver::start(DriverConfig::default()).expect("driver starts");
        let connection_id = resolve_request(driver.reserve_connection());

        driver
            .dispatch(DriverCommand::Tcp(crate::api::TcpCommand::Connect {
                connection_id,
                local_addr: None,
                remote_addr,
            }))
            .expect("dispatch TCP connect");

        loop {
            match wait_for_event(&driver) {
                DriverEvent::Tcp(TcpEvent::Connected {
                    connection_id: connected_id,
                    peer_addr,
                }) if connected_id == connection_id => {
                    assert_eq!(peer_addr, remote_addr);
                    break;
                }
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for TCP connect: {:?}",
                        other
                    );
                }
            }
        }

        let transmission_id = TransmissionId(1);
        driver
            .dispatch(DriverCommand::Tcp(crate::api::TcpCommand::Send {
                connection_id,
                transmission_id,
                payload: IoPayload::Bytes(Bytes::from_static(b"hello")),
            }))
            .expect("dispatch TCP send");

        let mut saw_ack = false;
        let mut received_payload = None;
        while !saw_ack || received_payload.is_none() {
            match wait_for_event(&driver) {
                DriverEvent::Tcp(TcpEvent::SendAck {
                    connection_id: _,
                    transmission_id: ack_id,
                }) if ack_id == transmission_id => {
                    saw_ack = true;
                }
                DriverEvent::Tcp(TcpEvent::Received {
                    connection_id: received_id,
                    payload,
                }) if received_id == connection_id => {
                    received_payload = Some(payload_bytes(payload));
                }
                other => {
                    log::debug!("ignoring unrelated TCP event: {:?}", other);
                }
            }
        }

        assert_eq!(server_rx.recv().expect("server payload"), *b"hello");
        assert_eq!(received_payload.expect("received payload"), b"world"[..]);

        server.join().expect("join server thread");
        driver.shutdown().expect("driver shuts down");
    }

    #[test]
    fn tcp_connect_failure_is_reported() {
        init_test_logger();

        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind probe listener");
        let remote_addr = listener.local_addr().expect("probe addr");
        drop(listener);

        let driver = IoDriver::start(DriverConfig::default()).expect("driver starts");
        let connection_id = resolve_request(driver.reserve_connection());

        driver
            .dispatch(DriverCommand::Tcp(crate::api::TcpCommand::Connect {
                connection_id,
                local_addr: None,
                remote_addr,
            }))
            .expect("dispatch failing TCP connect");

        loop {
            match wait_for_event(&driver) {
                DriverEvent::Tcp(TcpEvent::ConnectFailed {
                    connection_id: failed_id,
                    remote_addr: failed_addr,
                    ..
                }) if failed_id == connection_id => {
                    assert_eq!(failed_addr, remote_addr);
                    break;
                }
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for TCP connect failure: {:?}",
                        other
                    );
                }
            }
        }

        driver.shutdown().expect("driver shuts down");
    }

    #[test]
    fn tcp_send_while_another_send_is_pending_is_nacked() {
        init_test_logger();

        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind TCP listener");
        let remote_addr = listener.local_addr().expect("listener addr");
        let server = std::thread::spawn(move || {
            let (_stream, _) = listener.accept().expect("accept TCP stream");
            std::thread::sleep(Duration::from_millis(200));
        });

        let driver = IoDriver::start(DriverConfig::default()).expect("driver starts");
        let connection_id = resolve_request(driver.reserve_connection());
        driver
            .dispatch(DriverCommand::Tcp(crate::api::TcpCommand::Connect {
                connection_id,
                local_addr: None,
                remote_addr,
            }))
            .expect("dispatch TCP connect");

        loop {
            match wait_for_event(&driver) {
                DriverEvent::Tcp(TcpEvent::Connected {
                    connection_id: connected_id,
                    ..
                }) if connected_id == connection_id => {
                    break;
                }
                _ => {}
            }
        }

        let large_payload = Bytes::from(vec![b'x'; 4 * 1024 * 1024]);
        driver
            .dispatch(DriverCommand::Tcp(crate::api::TcpCommand::Send {
                connection_id,
                transmission_id: TransmissionId(10),
                payload: IoPayload::Bytes(large_payload),
            }))
            .expect("dispatch large TCP send");
        driver
            .dispatch(DriverCommand::Tcp(crate::api::TcpCommand::Send {
                connection_id,
                transmission_id: TransmissionId(11),
                payload: IoPayload::Bytes(Bytes::from_static(b"second")),
            }))
            .expect("dispatch second TCP send");

        let mut saw_write_suspended = false;
        let mut saw_backpressure_nack = false;
        // This send sequence is only complete once we have seen both the
        // connection-level suspension signal and the rejected second send.
        while !saw_write_suspended || !saw_backpressure_nack {
            match wait_for_event(&driver) {
                DriverEvent::Tcp(TcpEvent::WriteSuspended {
                    connection_id: suspended_id,
                }) if suspended_id == connection_id => {
                    saw_write_suspended = true;
                }
                DriverEvent::Tcp(TcpEvent::SendNack {
                    connection_id: _,
                    transmission_id,
                    reason,
                }) if transmission_id == TransmissionId(11) => {
                    assert_eq!(reason, SendFailureReason::Backpressure);
                    saw_backpressure_nack = true;
                }
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for TCP write suspension/backpressure nack: {:?}",
                        other
                    );
                }
            }
        }

        server.join().expect("join server thread");
        driver.shutdown().expect("driver shuts down");
    }

    #[test]
    fn tcp_write_resumes_after_pending_send_drains() {
        init_test_logger();

        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind TCP listener");
        let remote_addr = listener.local_addr().expect("listener addr");
        let (start_read_tx, start_read_rx) = mpsc::sync_channel(1);
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept TCP stream");
            start_read_rx.recv().expect("wait for read signal");
            let mut remaining = 4 * 1024 * 1024;
            let mut buf = vec![0_u8; 64 * 1024];
            while remaining > 0 {
                let next_len = remaining.min(buf.len());
                stream
                    .read_exact(&mut buf[..next_len])
                    .expect("read exact TCP payload");
                remaining -= next_len;
            }
        });

        let driver = IoDriver::start(DriverConfig::default()).expect("driver starts");
        let connection_id = resolve_request(driver.reserve_connection());
        driver
            .dispatch(DriverCommand::Tcp(crate::api::TcpCommand::Connect {
                connection_id,
                local_addr: None,
                remote_addr,
            }))
            .expect("dispatch TCP connect");

        loop {
            match wait_for_event(&driver) {
                DriverEvent::Tcp(TcpEvent::Connected {
                    connection_id: connected_id,
                    ..
                }) if connected_id == connection_id => {
                    break;
                }
                _ => {}
            }
        }

        driver
            .dispatch(DriverCommand::Tcp(crate::api::TcpCommand::Send {
                connection_id,
                transmission_id: TransmissionId(12),
                payload: IoPayload::Bytes(Bytes::from(vec![b'y'; 4 * 1024 * 1024])),
            }))
            .expect("dispatch large TCP send");

        loop {
            match wait_for_event(&driver) {
                DriverEvent::Tcp(TcpEvent::WriteSuspended {
                    connection_id: suspended_id,
                }) if suspended_id == connection_id => {
                    break;
                }
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for TCP WriteSuspended: {:?}",
                        other
                    );
                }
            }
        }

        start_read_tx.send(()).expect("signal server to read");

        let mut saw_ack = false;
        let mut saw_resume = false;
        while !saw_ack || !saw_resume {
            match wait_for_event(&driver) {
                DriverEvent::Tcp(TcpEvent::SendAck {
                    connection_id: ack_id,
                    transmission_id,
                }) if ack_id == connection_id && transmission_id == TransmissionId(12) => {
                    saw_ack = true;
                }
                DriverEvent::Tcp(TcpEvent::WriteResumed {
                    connection_id: resumed_id,
                }) if resumed_id == connection_id => {
                    saw_resume = true;
                }
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for TCP SendAck/WriteResumed: {:?}",
                        other
                    );
                }
            }
        }

        server.join().expect("join server thread");
        driver.shutdown().expect("driver shuts down");
    }

    #[test]
    fn tcp_graceful_close_waits_for_pending_send() {
        init_test_logger();

        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind TCP listener");
        let remote_addr = listener.local_addr().expect("listener addr");
        let (server_tx, server_rx) = mpsc::sync_channel(1);
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept TCP stream");
            let mut buf = [0_u8; 5];
            stream.read_exact(&mut buf).expect("server read exact");
            server_tx.send(buf).expect("send server payload");
        });

        let driver = IoDriver::start(DriverConfig::default()).expect("driver starts");
        let connection_id = resolve_request(driver.reserve_connection());
        driver
            .dispatch(DriverCommand::Tcp(crate::api::TcpCommand::Connect {
                connection_id,
                local_addr: None,
                remote_addr,
            }))
            .expect("dispatch TCP connect");

        loop {
            match wait_for_event(&driver) {
                DriverEvent::Tcp(TcpEvent::Connected {
                    connection_id: connected_id,
                    ..
                }) if connected_id == connection_id => {
                    break;
                }
                _ => {}
            }
        }

        let transmission_id = TransmissionId(20);
        driver
            .dispatch(DriverCommand::Tcp(crate::api::TcpCommand::Send {
                connection_id,
                transmission_id,
                payload: IoPayload::Bytes(Bytes::from_static(b"hello")),
            }))
            .expect("dispatch TCP send");
        driver
            .dispatch(DriverCommand::Tcp(crate::api::TcpCommand::Close {
                connection_id,
                abort: false,
            }))
            .expect("dispatch graceful TCP close");

        let mut saw_ack = false;
        let mut saw_closed = false;
        while !saw_ack || !saw_closed {
            match wait_for_event(&driver) {
                DriverEvent::Tcp(TcpEvent::SendAck {
                    connection_id: _,
                    transmission_id: ack_id,
                }) if ack_id == transmission_id => {
                    saw_ack = true;
                }
                DriverEvent::Tcp(TcpEvent::Closed {
                    connection_id: closed_id,
                    reason,
                }) if closed_id == connection_id => {
                    assert_eq!(reason, CloseReason::Graceful);
                    saw_closed = true;
                }
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for TCP graceful close: {:?}",
                        other
                    );
                }
            }
        }

        assert_eq!(server_rx.recv().expect("server payload"), *b"hello");

        server.join().expect("join server thread");
        driver.shutdown().expect("driver shuts down");
    }

    #[test]
    fn tcp_read_suspends_and_resumes_when_ingress_capacity_returns() {
        init_test_logger();

        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind TCP listener");
        let remote_addr = listener.local_addr().expect("listener addr");
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept TCP stream");
            stream
                .write_all(&vec![b'a'; 300])
                .expect("server write payload");
            std::thread::sleep(Duration::from_millis(50));
        });

        let driver_config = crate::driver::DriverConfig {
            buffer_config: crate::pool::IoBufferConfig {
                ingress: crate::pool::IoPoolConfig {
                    chunk_size: 128,
                    initial_chunk_count: 1,
                    max_chunk_count: 2,
                    encode_buf_min_free_space: 64,
                },
                egress: crate::pool::IoPoolConfig::default(),
            },
            ..crate::driver::DriverConfig::default()
        };
        let driver = IoDriver::start(driver_config).expect("driver starts");
        let connection_id = resolve_request(driver.reserve_connection());
        driver
            .dispatch(DriverCommand::Tcp(crate::api::TcpCommand::Connect {
                connection_id,
                local_addr: None,
                remote_addr,
            }))
            .expect("dispatch TCP connect");

        loop {
            match wait_for_event(&driver) {
                DriverEvent::Tcp(TcpEvent::Connected {
                    connection_id: connected_id,
                    ..
                }) if connected_id == connection_id => {
                    break;
                }
                _ => {}
            }
        }

        let first_lease = loop {
            match wait_for_event(&driver) {
                DriverEvent::Tcp(TcpEvent::Received {
                    connection_id: received_id,
                    payload: IoPayload::Lease(lease),
                }) if received_id == connection_id => break lease,
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for first TCP receive: {:?}",
                        other
                    );
                }
            }
        };
        let second_lease = loop {
            match wait_for_event(&driver) {
                DriverEvent::Tcp(TcpEvent::Received {
                    connection_id: received_id,
                    payload: IoPayload::Lease(lease),
                }) if received_id == connection_id => break lease,
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for second TCP receive: {:?}",
                        other
                    );
                }
            }
        };

        loop {
            match wait_for_event(&driver) {
                DriverEvent::Tcp(TcpEvent::ReadSuspended {
                    connection_id: suspended_id,
                }) if suspended_id == connection_id => {
                    break;
                }
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for TCP ReadSuspended: {:?}",
                        other
                    );
                }
            }
        }

        drop(first_lease);
        drop(second_lease);

        let mut saw_resume = false;
        let mut third_payload = None;
        while !saw_resume || third_payload.is_none() {
            match wait_for_event(&driver) {
                DriverEvent::Tcp(TcpEvent::ReadResumed {
                    connection_id: resumed_id,
                }) if resumed_id == connection_id => {
                    saw_resume = true;
                }
                DriverEvent::Tcp(TcpEvent::Received {
                    connection_id: received_id,
                    payload,
                }) if received_id == connection_id => {
                    third_payload = Some(payload_bytes(payload));
                }
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for TCP ReadResumed/Received: {:?}",
                        other
                    );
                }
            }
        }

        assert_eq!(third_payload.expect("third payload").len(), 44);

        server.join().expect("join server thread");
        driver.shutdown().expect("driver shuts down");
    }
}
