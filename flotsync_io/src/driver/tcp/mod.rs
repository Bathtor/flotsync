#[cfg(test)]
use super::runtime::ResourceSnapshot;
use super::{DriverEventSink, DriverToken, registry::SlotRegistry, runtime::ResourceRecord};
use crate::{
    api::{
        CloseReason,
        ConnectionId,
        IoPayload,
        IoPayloadCursor,
        ListenerId,
        SendFailureReason,
        TcpEvent,
        TransmissionId,
    },
    errors::{Error, Result, UpdateTcpInterestSnafu},
    logging::RuntimeLogger,
    pool::IngressPool,
    socket_support::{configure_bind_reuse, socket_domain},
};
use bytes::Buf;
#[cfg(test)]
use bytes::Bytes;
use flotsync_utils::option_when;
use mio::{
    Interest,
    Registry,
    net::{TcpListener as MioTcpListener, TcpStream as MioTcpStream},
};
use slog::{debug, error, warn};
use snafu::ResultExt;
use socket2::{Protocol, SockAddr, Socket, Type};
use std::{
    io,
    io::{ErrorKind, Read, Write},
    net::{SocketAddr, TcpListener as StdTcpListener, TcpStream as StdTcpStream},
};

/// Backlog used when the reuse-enabled test bind path has to construct listeners via `socket2`.
const TCP_LISTEN_BACKLOG: i32 = 1024;

#[cfg(test)]
mod tests;
mod types;

pub(in crate::driver) use types::{AcceptedTcpConnection, ReleasedTcpListener};
use types::{PendingTcpSend, TcpConnectionEntry, TcpListenerEntry};

/// TCP-side runtime state owned by the shared driver.
#[derive(Debug)]
pub(super) struct TcpRuntimeState {
    logger: RuntimeLogger,
    /// Mirrors the driver-wide bind policy so tests can opt listener binds into platform socket
    /// re-use options.
    bind_reuse_address: bool,
    listeners: SlotRegistry<TcpListenerEntry>,
    connections: SlotRegistry<TcpConnectionEntry>,
}

impl TcpRuntimeState {
    pub(super) fn new(logger: RuntimeLogger, bind_reuse_address: bool) -> Self {
        Self {
            logger,
            bind_reuse_address,
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
        if let Some(listener) = listener.as_mut()
            && entry.registered
        {
            let deregister_result = registry.deregister(listener);
            if let Err(error) = deregister_result {
                warn!(
                    self.logger,
                    "failed to deregister TCP listener {} during release: {}", listener_id, error
                );
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
        if let Some(stream) = stream.as_mut()
            && entry.registered
        {
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

        let listener_result = bind_tcp_listener(local_addr, self.bind_reuse_address);
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

    #[allow(
        clippy::unnecessary_wraps,
        reason = "The TCP API returns Result consistently with other readiness handlers and leaves room for accept-path errors."
    )]
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
        payload: &IoPayload,
        registry: &Registry,
        event_sink: &dyn DriverEventSink,
    ) -> Result<Option<ResourceRecord>> {
        self.handle_send_inner(
            connection_id,
            transmission_id,
            payload,
            false,
            registry,
            event_sink,
        )
    }

    pub(super) fn handle_send_and_close(
        &mut self,
        connection_id: ConnectionId,
        transmission_id: TransmissionId,
        payload: &IoPayload,
        registry: &Registry,
        event_sink: &dyn DriverEventSink,
    ) -> Result<Option<ResourceRecord>> {
        self.handle_send_inner(
            connection_id,
            transmission_id,
            payload,
            true,
            registry,
            event_sink,
        )
    }

    fn handle_send_inner(
        &mut self,
        connection_id: ConnectionId,
        transmission_id: TransmissionId,
        payload: &IoPayload,
        close_after_send: bool,
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
            if close_after_send {
                let record = self.close_connection(
                    connection_id,
                    registry,
                    CloseReason::Graceful,
                    event_sink,
                )?;
                return Ok(Some(record));
            }
            return Ok(None);
        }

        entry.pending_send = Some(PendingTcpSend::new(transmission_id, payload));
        entry.close_after_flush = close_after_send;
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

    #[allow(
        clippy::too_many_lines,
        reason = "TCP send flushing handles partial writes, close-after-send, and event publication in one state transition."
    )]
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
            let Some(mut ingress_buffer) = ingress_pool.try_acquire()? else {
                let suspended = self.suspend_read(connection_id, registry)?;
                if suspended {
                    event_sink.publish(super::DriverEvent::Tcp(TcpEvent::ReadSuspended {
                        connection_id,
                    }))?;
                }
                return Ok(None);
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
        if let Some(stream) = stream.as_mut()
            && entry.registered
        {
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
    if let Some(stream) = entry.stream.as_mut()
        && entry.registered
    {
        registry.deregister(stream)?;
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

/// Binds one TCP listener, optionally opting into platform socket re-use for test-only port
/// reservation.
fn bind_tcp_listener(
    local_addr: SocketAddr,
    bind_reuse_address: bool,
) -> io::Result<MioTcpListener> {
    if !bind_reuse_address {
        return MioTcpListener::bind(local_addr);
    }

    let socket = Socket::new(socket_domain(local_addr), Type::STREAM, Some(Protocol::TCP))?;
    configure_bind_reuse(&socket)?;
    socket.set_nonblocking(true)?;
    socket.bind(&SockAddr::from(local_addr))?;
    socket.listen(TCP_LISTEN_BACKLOG)?;

    let listener: StdTcpListener = socket.into();
    Ok(MioTcpListener::from_std(listener))
}

/// Connects one TCP stream after first binding it to a caller-selected local address.
///
/// `mio` does not currently expose this path directly, so the implementation uses `socket2` and
/// converts the resulting non-blocking `std::net::TcpStream` back into `mio`.
fn connect_tcp_stream_with_local_addr(
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
) -> io::Result<MioTcpStream> {
    let socket = Socket::new(
        socket_domain(remote_addr),
        Type::STREAM,
        Some(Protocol::TCP),
    )?;
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
