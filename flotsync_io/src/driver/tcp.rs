#[cfg(test)]
use super::runtime::ResourceSnapshot;
use super::{DriverEvent, DriverToken, registry::SlotRegistry, runtime::ResourceRecord};
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
    pool::IngressPool,
};
use bytes::{Buf, Bytes};
use flotsync_utils::option_when;
use mio::{Interest, Registry, net::TcpStream as MioTcpStream};
use snafu::ResultExt;
use std::{
    io,
    io::{ErrorKind, Read, Write},
    net::SocketAddr,
};

/// TCP-side runtime state owned by the shared driver.
#[derive(Debug, Default)]
pub(super) struct TcpRuntimeState {
    listeners: SlotRegistry<ResourceRecord>,
    connections: SlotRegistry<TcpConnectionEntry>,
}

#[derive(Debug)]
struct TcpConnectionEntry {
    record: ResourceRecord,
    stream: Option<MioTcpStream>,
    remote_addr: Option<SocketAddr>,
    connect_pending: bool,
    read_suspended: bool,
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
            connect_pending: false,
            read_suspended: false,
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
        self.connect_pending = false;
        self.read_suspended = false;
        self.registered = false;
        self.pending_send = None;
        self.close_after_flush = false;
    }

    fn is_open(&self) -> bool {
        self.stream.is_some()
    }
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
    pub(super) fn next_listener_slot(&self) -> usize {
        self.listeners.next_slot()
    }

    pub(super) fn next_connection_slot(&self) -> usize {
        self.connections.next_slot()
    }

    pub(super) fn reserve_listener(&mut self, listener_id: ListenerId, token: DriverToken) {
        let slot = self.listeners.reserve(ResourceRecord::new(token));
        debug_assert_eq!(slot, listener_id.0);
    }

    pub(super) fn reserve_connection(&mut self, connection_id: ConnectionId, token: DriverToken) {
        let slot = self
            .connections
            .reserve(TcpConnectionEntry::new(ResourceRecord::new(token)));
        debug_assert_eq!(slot, connection_id.0);
    }

    pub(super) fn release_listener(&mut self, listener_id: ListenerId) -> Result<ResourceRecord> {
        self.listeners
            .remove(listener_id.0)
            .ok_or(Error::UnknownListener { listener_id })
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
                    log::warn!(
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
            entry.readiness_hits += 1;
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
        remote_addr: SocketAddr,
        registry: &Registry,
        event_tx: &crossbeam_channel::Sender<DriverEvent>,
    ) -> Result<()> {
        let Some(entry) = self.connections.get_mut(connection_id.0) else {
            return emit_tcp_event(
                event_tx,
                TcpEvent::ConnectFailed {
                    connection_id,
                    remote_addr,
                    error_kind: ErrorKind::NotFound,
                },
            );
        };
        if entry.is_open() {
            return emit_tcp_event(
                event_tx,
                TcpEvent::ConnectFailed {
                    connection_id,
                    remote_addr,
                    error_kind: ErrorKind::AlreadyExists,
                },
            );
        }

        let stream_result = MioTcpStream::connect(remote_addr);
        let stream = match stream_result {
            Ok(stream) => stream,
            Err(error) => {
                return emit_tcp_event(
                    event_tx,
                    TcpEvent::ConnectFailed {
                        connection_id,
                        remote_addr,
                        error_kind: error.kind(),
                    },
                );
            }
        };

        let connect_pending = match check_tcp_connect_result(&stream) {
            Ok(true) => false,
            Ok(false) => true,
            Err(error_kind) => {
                return emit_tcp_event(
                    event_tx,
                    TcpEvent::ConnectFailed {
                        connection_id,
                        remote_addr,
                        error_kind,
                    },
                );
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
            return emit_tcp_event(
                event_tx,
                TcpEvent::ConnectFailed {
                    connection_id,
                    remote_addr,
                    error_kind: error.kind(),
                },
            );
        }

        if !connect_pending {
            emit_tcp_event(
                event_tx,
                TcpEvent::Connected {
                    connection_id,
                    peer_addr: remote_addr,
                },
            )?;
        }

        Ok(())
    }

    pub(super) fn handle_send(
        &mut self,
        connection_id: ConnectionId,
        transmission_id: TransmissionId,
        payload: IoPayload,
        registry: &Registry,
        event_tx: &crossbeam_channel::Sender<DriverEvent>,
    ) -> Result<Option<ResourceRecord>> {
        let Some(entry) = self.connections.get_mut(connection_id.0) else {
            emit_tcp_event(
                event_tx,
                TcpEvent::SendNack {
                    transmission_id,
                    reason: SendFailureReason::Closed,
                },
            )?;
            return Ok(None);
        };
        if !entry.is_open() {
            emit_tcp_event(
                event_tx,
                TcpEvent::SendNack {
                    transmission_id,
                    reason: SendFailureReason::InvalidState,
                },
            )?;
            return Ok(None);
        }
        if entry.connect_pending {
            emit_tcp_event(
                event_tx,
                TcpEvent::SendNack {
                    transmission_id,
                    reason: SendFailureReason::InvalidState,
                },
            )?;
            return Ok(None);
        }
        if entry.pending_send.is_some() {
            emit_tcp_event(
                event_tx,
                TcpEvent::SendNack {
                    transmission_id,
                    reason: SendFailureReason::Backpressure,
                },
            )?;
            return Ok(None);
        }
        if payload.is_empty() {
            emit_tcp_event(event_tx, TcpEvent::SendAck { transmission_id })?;
            return Ok(None);
        }

        entry.pending_send = Some(PendingTcpSend::new(transmission_id, payload));
        self.flush_pending_send(connection_id, registry, event_tx)
    }

    pub(super) fn handle_close(
        &mut self,
        connection_id: ConnectionId,
        abort: bool,
        registry: &Registry,
        event_tx: &crossbeam_channel::Sender<DriverEvent>,
    ) -> Result<Option<ResourceRecord>> {
        if abort {
            let record =
                self.close_connection(connection_id, registry, CloseReason::Aborted, event_tx)?;
            return Ok(Some(record));
        }

        let Some(entry) = self.connections.get_mut(connection_id.0) else {
            log::warn!("ignored TCP close for unknown connection {}", connection_id);
            return Ok(None);
        };

        if entry.pending_send.is_some() {
            entry.close_after_flush = true;
            let update_interest_result = apply_connection_interest(entry, registry);
            update_interest_result.context(UpdateTcpInterestSnafu { connection_id })?;
            return Ok(None);
        }

        let record =
            self.close_connection(connection_id, registry, CloseReason::Graceful, event_tx)?;
        Ok(Some(record))
    }

    pub(super) fn handle_ready(
        &mut self,
        connection_id: ConnectionId,
        registry: &Registry,
        ingress_pool: &IngressPool,
        event_tx: &crossbeam_channel::Sender<DriverEvent>,
        is_readable: bool,
        is_writable: bool,
    ) -> Result<Option<ResourceRecord>> {
        let should_complete_connect = match self.connections.get(connection_id.0) {
            Some(entry) => entry.connect_pending && (is_readable || is_writable),
            None => false,
        };
        if should_complete_connect {
            let closed_record = self.finish_connect(connection_id, registry, event_tx)?;
            if closed_record.is_some() {
                return Ok(closed_record);
            }
        }

        if is_writable {
            let closed_record = self.flush_pending_send(connection_id, registry, event_tx)?;
            if closed_record.is_some() {
                return Ok(closed_record);
            }
        }
        if is_readable {
            return self.handle_readable(connection_id, registry, ingress_pool, event_tx);
        }

        Ok(None)
    }

    pub(super) fn resume_suspended_reads(
        &mut self,
        registry: &Registry,
        ingress_pool: &IngressPool,
        event_tx: &crossbeam_channel::Sender<DriverEvent>,
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
                        entry.read_suspended && entry.is_open() && !entry.connect_pending,
                        ConnectionId(slot)
                    )
                })
            })
            .collect();

        for connection_id in suspended_connections {
            let resumed = self.resume_read(connection_id, registry)?;
            if resumed {
                emit_tcp_event(event_tx, TcpEvent::ReadResumed { connection_id })?;
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
                token: entry.token,
                readiness_hits: entry.readiness_hits,
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
        event_tx: &crossbeam_channel::Sender<DriverEvent>,
    ) -> Result<Option<ResourceRecord>> {
        let Some(entry) = self.connections.get_mut(connection_id.0) else {
            log::debug!(
                "ignoring stale TCP readiness for unknown connection {}",
                connection_id
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
                emit_tcp_event(
                    event_tx,
                    TcpEvent::Connected {
                        connection_id,
                        peer_addr: remote_addr,
                    },
                )?;
                Ok(None)
            }
            Err(error_kind) => {
                let reset_error = reset_connection_after_failure(entry, registry);
                if let Err(error) = reset_error {
                    log::warn!(
                        "failed to reset TCP connection {} after connect failure: {}",
                        connection_id,
                        error
                    );
                }
                emit_tcp_event(
                    event_tx,
                    TcpEvent::ConnectFailed {
                        connection_id,
                        remote_addr,
                        error_kind,
                    },
                )?;
                Ok(None)
            }
        }
    }

    fn flush_pending_send(
        &mut self,
        connection_id: ConnectionId,
        registry: &Registry,
        event_tx: &crossbeam_channel::Sender<DriverEvent>,
    ) -> Result<Option<ResourceRecord>> {
        let Some(entry) = self.connections.get_mut(connection_id.0) else {
            log::debug!(
                "ignoring stale TCP writability for unknown connection {}",
                connection_id
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
                    emit_tcp_event(
                        event_tx,
                        TcpEvent::SendNack {
                            transmission_id,
                            reason: SendFailureReason::Closed,
                        },
                    )?;
                    let record = self.close_connection(
                        connection_id,
                        registry,
                        CloseReason::Aborted,
                        event_tx,
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
                        emit_tcp_event(event_tx, TcpEvent::SendAck { transmission_id })?;
                        if entry.close_after_flush {
                            let record = self.close_connection(
                                connection_id,
                                registry,
                                CloseReason::Graceful,
                                event_tx,
                            )?;
                            return Ok(Some(record));
                        }
                        apply_connection_interest(entry, registry)
                            .context(UpdateTcpInterestSnafu { connection_id })?;
                        return Ok(None);
                    }
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
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
                    emit_tcp_event(
                        event_tx,
                        TcpEvent::SendNack {
                            transmission_id,
                            reason: SendFailureReason::from(&error),
                        },
                    )?;
                    let record = self.close_connection(
                        connection_id,
                        registry,
                        CloseReason::Aborted,
                        event_tx,
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
        event_tx: &crossbeam_channel::Sender<DriverEvent>,
    ) -> Result<Option<ResourceRecord>> {
        loop {
            let mut ingress_buffer = match ingress_pool.try_acquire()? {
                Some(buffer) => buffer,
                None => {
                    let suspended = self.suspend_read(connection_id, registry)?;
                    if suspended {
                        emit_tcp_event(event_tx, TcpEvent::ReadSuspended { connection_id })?;
                    }
                    return Ok(None);
                }
            };

            let read_result = {
                let Some(entry) = self.connections.get_mut(connection_id.0) else {
                    log::debug!(
                        "ignoring stale TCP readability for unknown connection {}",
                        connection_id
                    );
                    return Ok(None);
                };
                let Some(stream) = entry.stream.as_mut() else {
                    log::debug!(
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
                        event_tx,
                    )?;
                    return Ok(Some(record));
                }
                Ok(bytes_read) => {
                    let lease = ingress_buffer.commit(bytes_read)?;
                    emit_tcp_event(
                        event_tx,
                        TcpEvent::Received {
                            connection_id,
                            payload: IoPayload::Lease(lease),
                        },
                    )?;
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    return Ok(None);
                }
                Err(error) => {
                    log::error!("TCP connection {} recv failed: {}", connection_id, error);
                    let record = self.close_connection(
                        connection_id,
                        registry,
                        CloseReason::Aborted,
                        event_tx,
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
        event_tx: &crossbeam_channel::Sender<DriverEvent>,
    ) -> Result<ResourceRecord> {
        let record = self.release_connection(connection_id, registry)?;
        emit_tcp_event(
            event_tx,
            TcpEvent::Closed {
                connection_id,
                reason,
            },
        )?;
        Ok(record)
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

fn emit_tcp_event(
    event_tx: &crossbeam_channel::Sender<DriverEvent>,
    event: TcpEvent,
) -> Result<()> {
    let send_result = event_tx.send(DriverEvent::Tcp(event));
    if send_result.is_err() {
        return Err(Error::DriverEventChannelClosed);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        driver::{DriverCommand, DriverConfig, DriverRequest, IoDriver, wait_for_request},
        prelude::Result,
        test_support::init_test_logger,
    };
    use std::{
        net::TcpListener,
        sync::mpsc,
        time::{Duration, Instant},
    };

    fn resolve_request<T>(request: Result<DriverRequest<T>>) -> T {
        let request = request.expect("enqueue driver request");
        wait_for_request(request).expect("resolve driver request")
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

    fn payload_bytes(payload: IoPayload) -> Bytes {
        match payload {
            IoPayload::Lease(lease) => lease.create_byte_clone(),
            IoPayload::Bytes(bytes) => bytes,
        }
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

        loop {
            match wait_for_event(&driver) {
                DriverEvent::Tcp(TcpEvent::SendNack {
                    transmission_id,
                    reason,
                }) if transmission_id == TransmissionId(11) => {
                    assert_eq!(reason, SendFailureReason::Backpressure);
                    break;
                }
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for TCP backpressure nack: {:?}",
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
