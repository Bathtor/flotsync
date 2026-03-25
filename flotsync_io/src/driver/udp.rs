#[cfg(test)]
use super::runtime::ResourceSnapshot;
use super::{DriverEventSink, DriverToken, registry::SlotRegistry, runtime::ResourceRecord};
use crate::{
    api::{
        IoPayload,
        MAX_UDP_PAYLOAD_BYTES,
        SendFailureReason,
        SocketId,
        TransmissionId,
        UdpCloseReason,
        UdpEvent,
        UdpLocalBind,
        UdpSocketOption,
    },
    errors::{Error, Result},
    logging::RuntimeLogger,
    pool::IngressPool,
};
use bytes::Buf;
use flotsync_utils::option_when;
use mio::{Interest, Registry, net::UdpSocket as MioUdpSocket};
use slog::{debug, error, warn};
use socket2::SockRef;
use std::{io, io::ErrorKind, net::SocketAddr};

/// UDP-side runtime state owned by the shared driver.
#[derive(Debug)]
pub(super) struct UdpRuntimeState {
    logger: RuntimeLogger,
    sockets: SlotRegistry<UdpSocketEntry>,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct ReleasedUdpSocket {
    pub(super) record: ResourceRecord,
    pub(super) remote_addr: Option<SocketAddr>,
}

#[derive(Debug)]
struct UdpSocketEntry {
    record: ResourceRecord,
    socket: Option<MioUdpSocket>,
    local_addr: Option<SocketAddr>,
    remote_addr: Option<SocketAddr>,
    read_suspended: bool,
    write_suspended: bool,
    registered: bool,
}

impl UdpSocketEntry {
    fn new(record: ResourceRecord) -> Self {
        Self {
            record,
            socket: None,
            local_addr: None,
            remote_addr: None,
            read_suspended: false,
            write_suspended: false,
            registered: false,
        }
    }

    /// Returns whether this entry currently owns a live OS UDP socket handle.
    fn is_open(&self) -> bool {
        self.socket.is_some()
    }

    /// Returns the readiness interests required for the socket's current suspension state.
    fn desired_interest(&self) -> Option<Interest> {
        self.socket.as_ref()?;

        match (self.read_suspended, self.write_suspended) {
            (false, false) => Some(Interest::READABLE),
            (false, true) => Some(Interest::READABLE.add(Interest::WRITABLE)),
            (true, false) => None,
            (true, true) => Some(Interest::WRITABLE),
        }
    }

    /// Returns `true` when the socket transitioned into write-suspended state.
    fn suspend_write(&mut self) -> bool {
        if self.write_suspended {
            return false;
        }
        self.write_suspended = true;
        true
    }

    /// Returns `true` when the socket transitioned back to accepting new sends.
    fn resume_write(&mut self) -> bool {
        if !self.write_suspended {
            return false;
        }
        self.write_suspended = false;
        true
    }
}

#[derive(Clone, Copy, Debug)]
enum UdpSendTarget {
    Connected,
    Unconnected(SocketAddr),
}

impl UdpRuntimeState {
    pub(super) fn new(logger: RuntimeLogger) -> Self {
        Self {
            logger,
            sockets: SlotRegistry::default(),
        }
    }

    pub(super) fn next_socket_slot(&self) -> usize {
        self.sockets.next_slot()
    }

    pub(super) fn reserve_socket(&mut self, socket_id: SocketId, token: DriverToken) {
        let slot = self
            .sockets
            .reserve(UdpSocketEntry::new(ResourceRecord::new(token)));
        debug_assert_eq!(slot, socket_id.0);
    }

    pub(super) fn release_socket(
        &mut self,
        socket_id: SocketId,
        registry: &Registry,
    ) -> Result<ReleasedUdpSocket> {
        let entry = self
            .sockets
            .remove(socket_id.0)
            .ok_or(Error::UnknownSocket { socket_id })?;
        let remote_addr = entry.remote_addr;
        if let Some(mut socket) = entry.socket
            && entry.registered
        {
            let deregister_result = registry.deregister(&mut socket);
            if let Err(error) = deregister_result {
                warn!(
                    self.logger,
                    "failed to deregister UDP socket {} during release: {}", socket_id, error
                );
            }
        }
        Ok(ReleasedUdpSocket {
            record: entry.record,
            remote_addr,
        })
    }

    pub(super) fn record_socket_readiness_hit(&mut self, socket_id: SocketId) {
        if let Some(entry) = self.sockets.get_mut(socket_id.0) {
            entry.record.readiness_hits += 1;
        }
    }

    pub(super) fn handle_bind(
        &mut self,
        socket_id: SocketId,
        bind: UdpLocalBind,
        registry: &Registry,
        event_sink: &dyn DriverEventSink,
    ) -> Result<()> {
        let local_addr = bind.resolve_local_addr();
        let Some(entry) = self.sockets.get_mut(socket_id.0) else {
            event_sink.publish(super::DriverEvent::Udp(UdpEvent::BindFailed {
                socket_id,
                local_addr,
                error_kind: ErrorKind::NotFound,
            }))?;
            return Ok(());
        };

        if entry.is_open() {
            event_sink.publish(super::DriverEvent::Udp(UdpEvent::BindFailed {
                socket_id,
                local_addr,
                error_kind: ErrorKind::AlreadyExists,
            }))?;
            return Ok(());
        }

        let socket = match bind_udp_socket(local_addr) {
            Ok(socket) => socket,
            Err(error) => {
                event_sink.publish(super::DriverEvent::Udp(UdpEvent::BindFailed {
                    socket_id,
                    local_addr,
                    error_kind: error.kind(),
                }))?;
                return Ok(());
            }
        };

        let actual_local_addr = match socket.local_addr() {
            Ok(addr) => addr,
            Err(error) => {
                warn!(
                    self.logger,
                    "failed to query local address for UDP socket {} after bind: {}",
                    socket_id,
                    error
                );
                local_addr
            }
        };

        entry.socket = Some(socket);
        entry.local_addr = Some(actual_local_addr);
        entry.remote_addr = None;
        entry.read_suspended = false;
        entry.write_suspended = false;
        entry.registered = false;

        let register_result = apply_socket_interest(entry, registry);
        if let Err(error) = register_result {
            reset_socket_after_failure(&self.logger, entry, registry);
            event_sink.publish(super::DriverEvent::Udp(UdpEvent::BindFailed {
                socket_id,
                local_addr,
                error_kind: error.kind(),
            }))?;
            return Ok(());
        }

        event_sink.publish(super::DriverEvent::Udp(UdpEvent::Bound {
            socket_id,
            local_addr: actual_local_addr,
        }))
    }

    pub(super) fn handle_connect(
        &mut self,
        socket_id: SocketId,
        remote_addr: SocketAddr,
        bind: UdpLocalBind,
        registry: &Registry,
        event_sink: &dyn DriverEventSink,
    ) -> Result<()> {
        let local_addr = bind.resolve_local_addr();
        let Some(entry) = self.sockets.get_mut(socket_id.0) else {
            event_sink.publish(super::DriverEvent::Udp(UdpEvent::ConnectFailed {
                socket_id,
                local_addr,
                remote_addr,
                error_kind: ErrorKind::NotFound,
            }))?;
            return Ok(());
        };

        if entry.is_open() {
            event_sink.publish(super::DriverEvent::Udp(UdpEvent::ConnectFailed {
                socket_id,
                local_addr,
                remote_addr,
                error_kind: ErrorKind::AlreadyExists,
            }))?;
            return Ok(());
        }

        let socket = match bind_udp_socket(local_addr) {
            Ok(socket) => socket,
            Err(error) => {
                event_sink.publish(super::DriverEvent::Udp(UdpEvent::ConnectFailed {
                    socket_id,
                    local_addr,
                    remote_addr,
                    error_kind: error.kind(),
                }))?;
                return Ok(());
            }
        };

        let connect_result = socket.connect(remote_addr);
        if let Err(error) = connect_result {
            event_sink.publish(super::DriverEvent::Udp(UdpEvent::ConnectFailed {
                socket_id,
                local_addr,
                remote_addr,
                error_kind: error.kind(),
            }))?;
            return Ok(());
        }

        let actual_local_addr = match socket.local_addr() {
            Ok(addr) => addr,
            Err(error) => {
                warn!(
                    self.logger,
                    "failed to query local address for connected UDP socket {}: {}",
                    socket_id,
                    error
                );
                local_addr
            }
        };

        entry.socket = Some(socket);
        entry.local_addr = Some(actual_local_addr);
        entry.remote_addr = Some(remote_addr);
        entry.read_suspended = false;
        entry.write_suspended = false;
        entry.registered = false;

        let register_result = apply_socket_interest(entry, registry);
        if let Err(error) = register_result {
            reset_socket_after_failure(&self.logger, entry, registry);
            event_sink.publish(super::DriverEvent::Udp(UdpEvent::ConnectFailed {
                socket_id,
                local_addr,
                remote_addr,
                error_kind: error.kind(),
            }))?;
            return Ok(());
        }

        event_sink.publish(super::DriverEvent::Udp(UdpEvent::Connected {
            socket_id,
            local_addr: actual_local_addr,
            remote_addr,
        }))
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "driver send handlers take runtime resources explicitly to stay testable"
    )]
    pub(super) fn handle_send(
        &mut self,
        socket_id: SocketId,
        transmission_id: TransmissionId,
        payload: IoPayload,
        target: Option<SocketAddr>,
        registry: &Registry,
        event_sink: &dyn DriverEventSink,
        udp_send_scratch: &mut [u8],
    ) -> Result<()> {
        let Some(entry) = self.sockets.get_mut(socket_id.0) else {
            event_sink.publish(super::DriverEvent::Udp(UdpEvent::SendNack {
                socket_id,
                transmission_id,
                reason: SendFailureReason::Closed,
            }))?;
            return Ok(());
        };

        if !entry.is_open() {
            event_sink.publish(super::DriverEvent::Udp(UdpEvent::SendNack {
                socket_id,
                transmission_id,
                reason: SendFailureReason::InvalidState,
            }))?;
            return Ok(());
        }
        if entry.write_suspended {
            event_sink.publish(super::DriverEvent::Udp(UdpEvent::SendNack {
                socket_id,
                transmission_id,
                reason: SendFailureReason::Backpressure,
            }))?;
            return Ok(());
        }

        let send_target = match (entry.remote_addr, target) {
            (Some(_), Some(_)) => {
                event_sink.publish(super::DriverEvent::Udp(UdpEvent::SendNack {
                    socket_id,
                    transmission_id,
                    reason: SendFailureReason::UnexpectedTargetForConnectedSocket,
                }))?;
                return Ok(());
            }
            (Some(_), None) => UdpSendTarget::Connected,
            (None, Some(target)) => UdpSendTarget::Unconnected(target),
            (None, None) => {
                event_sink.publish(super::DriverEvent::Udp(UdpEvent::SendNack {
                    socket_id,
                    transmission_id,
                    reason: SendFailureReason::MissingTargetForUnconnectedSocket,
                }))?;
                return Ok(());
            }
        };

        let payload_len = payload.len();
        if payload_len > MAX_UDP_PAYLOAD_BYTES {
            event_sink.publish(super::DriverEvent::Udp(UdpEvent::SendNack {
                socket_id,
                transmission_id,
                reason: SendFailureReason::MessageTooLarge,
            }))?;
            return Ok(());
        }

        let send_result = {
            let socket = entry
                .socket
                .as_mut()
                .expect("open UDP socket missing handle");
            send_udp_payload(socket, payload, send_target, udp_send_scratch)
        };
        match send_result {
            Ok(bytes_sent) if bytes_sent == payload_len => {
                event_sink.publish(super::DriverEvent::Udp(UdpEvent::SendAck {
                    socket_id,
                    transmission_id,
                }))
            }
            Ok(bytes_sent) => {
                error!(
                    self.logger,
                    "UDP socket {} sent {} of {} requested bytes for transmission {}",
                    socket_id,
                    bytes_sent,
                    payload_len,
                    transmission_id
                );
                event_sink.publish(super::DriverEvent::Udp(UdpEvent::SendNack {
                    socket_id,
                    transmission_id,
                    reason: SendFailureReason::IoError,
                }))
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                event_sink.publish(super::DriverEvent::Udp(UdpEvent::SendNack {
                    socket_id,
                    transmission_id,
                    reason: SendFailureReason::Backpressure,
                }))?;
                if suspend_write(&self.logger, entry, socket_id, registry) {
                    event_sink.publish(super::DriverEvent::Udp(UdpEvent::WriteSuspended {
                        socket_id,
                    }))?;
                }
                Ok(())
            }
            Err(error) => event_sink.publish(super::DriverEvent::Udp(UdpEvent::SendNack {
                socket_id,
                transmission_id,
                reason: SendFailureReason::from(&error),
            })),
        }
    }

    pub(super) fn handle_configure(
        &mut self,
        socket_id: SocketId,
        option: UdpSocketOption,
        event_sink: &dyn DriverEventSink,
    ) -> Result<()> {
        let Some(entry) = self.sockets.get_mut(socket_id.0) else {
            event_sink.publish(super::DriverEvent::Udp(UdpEvent::ConfigureFailed {
                socket_id,
                option,
                error_kind: ErrorKind::NotFound,
            }))?;
            return Ok(());
        };

        let Some(socket) = entry.socket.as_ref() else {
            event_sink.publish(super::DriverEvent::Udp(UdpEvent::ConfigureFailed {
                socket_id,
                option,
                error_kind: ErrorKind::InvalidInput,
            }))?;
            return Ok(());
        };

        let configure_result = apply_udp_socket_option(socket, option);
        if let Err(error) = configure_result {
            event_sink.publish(super::DriverEvent::Udp(UdpEvent::ConfigureFailed {
                socket_id,
                option,
                error_kind: error.kind(),
            }))?;
            return Ok(());
        }

        event_sink.publish(super::DriverEvent::Udp(UdpEvent::Configured {
            socket_id,
            option,
        }))
    }

    pub(super) fn handle_readable(
        &mut self,
        socket_id: SocketId,
        registry: &Registry,
        ingress_pool: &IngressPool,
        event_sink: &dyn DriverEventSink,
    ) -> Result<Option<ReleasedUdpSocket>> {
        loop {
            let mut ingress_buffer = match ingress_pool.try_acquire()? {
                Some(buffer) => buffer,
                None => {
                    let suspended = self.suspend_read(socket_id, registry);
                    if suspended {
                        event_sink.publish(super::DriverEvent::Udp(UdpEvent::ReadSuspended {
                            socket_id,
                        }))?;
                    }
                    return Ok(None);
                }
            };

            let recv_result = {
                let Some(entry) = self.sockets.get_mut(socket_id.0) else {
                    debug!(
                        self.logger,
                        "ignoring stale UDP readability for unknown socket {}", socket_id
                    );
                    return Ok(None);
                };
                let Some(socket) = entry.socket.as_mut() else {
                    debug!(
                        self.logger,
                        "ignoring stale UDP readability for socket {} without OS handle", socket_id
                    );
                    return Ok(None);
                };
                if let Some(remote_addr) = entry.remote_addr {
                    let recv_result = socket.recv(ingress_buffer.writable());
                    recv_result.map(|bytes_read| (bytes_read, remote_addr))
                } else {
                    socket.recv_from(ingress_buffer.writable())
                }
            };

            match recv_result {
                Ok((0, source)) => {
                    drop(ingress_buffer);
                    event_sink.publish(super::DriverEvent::Udp(UdpEvent::Received {
                        socket_id,
                        source,
                        payload: IoPayload::Bytes(bytes::Bytes::new()),
                    }))?;
                }
                Ok((bytes_read, source)) => {
                    let lease = ingress_buffer.commit(bytes_read)?;
                    event_sink.publish(super::DriverEvent::Udp(UdpEvent::Received {
                        socket_id,
                        source,
                        payload: IoPayload::Lease(lease),
                    }))?;
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    return Ok(None);
                }
                Err(error) if error.kind() == ErrorKind::ConnectionRefused => {
                    let remote_addr = self
                        .sockets
                        .get(socket_id.0)
                        .and_then(|entry| entry.remote_addr);
                    if let Some(remote_addr) = remote_addr {
                        drop(ingress_buffer);
                        let released = self.release_socket(socket_id, registry)?;
                        event_sink.publish(super::DriverEvent::Udp(UdpEvent::Closed {
                            socket_id,
                            remote_addr: Some(remote_addr),
                            reason: UdpCloseReason::Disconnected,
                        }))?;
                        return Ok(Some(released));
                    }
                    error!(
                        self.logger,
                        "UDP socket {} recv failed: {}", socket_id, error
                    );
                    return Ok(None);
                }
                Err(error) => {
                    error!(
                        self.logger,
                        "UDP socket {} recv failed: {}", socket_id, error
                    );
                    return Ok(None);
                }
            }
        }
    }

    pub(super) fn handle_writable(
        &mut self,
        socket_id: SocketId,
        registry: &Registry,
        event_sink: &dyn DriverEventSink,
    ) -> Result<()> {
        let resumed = resume_write(self, socket_id, registry);
        if resumed {
            event_sink.publish(super::DriverEvent::Udp(UdpEvent::WriteResumed {
                socket_id,
            }))?;
        }
        Ok(())
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

        let suspended_sockets: Vec<SocketId> = self
            .sockets
            .entries()
            .iter()
            .enumerate()
            .filter_map(|(slot, entry_opt)| {
                entry_opt.as_ref().and_then(|entry| {
                    option_when!(entry.read_suspended && entry.is_open(), SocketId(slot))
                })
            })
            .collect();

        for socket_id in suspended_sockets {
            let resumed = self.resume_read(socket_id, registry);
            if resumed {
                event_sink.publish(super::DriverEvent::Udp(UdpEvent::ReadResumed { socket_id }))?;
            }
        }

        Ok(())
    }

    /// Returns `true` when the socket transitioned from readable to driver-suspended.
    fn suspend_read(&mut self, socket_id: SocketId, registry: &Registry) -> bool {
        let Some(entry) = self.sockets.get_mut(socket_id.0) else {
            return false;
        };
        if entry.read_suspended {
            return false;
        }
        if entry.socket.is_none() {
            return false;
        }

        entry.read_suspended = true;
        let update_result = apply_socket_interest(entry, registry);
        if let Err(error) = update_result {
            entry.read_suspended = false;
            error!(
                self.logger,
                "failed to suspend UDP socket {} by updating readiness interest: {}",
                socket_id,
                error
            );
            return false;
        }
        true
    }

    /// Returns `true` when the socket transitioned from driver-suspended back to readable.
    fn resume_read(&mut self, socket_id: SocketId, registry: &Registry) -> bool {
        let Some(entry) = self.sockets.get_mut(socket_id.0) else {
            return false;
        };
        if !entry.read_suspended {
            return false;
        }
        if entry.socket.is_none() {
            return false;
        }

        entry.read_suspended = false;
        let update_result = apply_socket_interest(entry, registry);
        if let Err(error) = update_result {
            entry.read_suspended = true;
            error!(
                self.logger,
                "failed to resume UDP socket {} by updating readiness interest: {}",
                socket_id,
                error
            );
            return false;
        }
        true
    }

    #[cfg(test)]
    pub(super) fn socket_snapshots(&self) -> Vec<ResourceSnapshot> {
        self.sockets
            .iter()
            .map(|(slot, entry)| ResourceSnapshot {
                slot,
                token: entry.record.token,
                readiness_hits: entry.record.readiness_hits,
            })
            .collect()
    }
}

fn bind_udp_socket(local_addr: SocketAddr) -> io::Result<MioUdpSocket> {
    MioUdpSocket::bind(local_addr)
}

fn apply_socket_interest(entry: &mut UdpSocketEntry, registry: &Registry) -> io::Result<()> {
    let desired_interest = entry.desired_interest();
    let Some(socket) = entry.socket.as_mut() else {
        entry.registered = false;
        return Ok(());
    };

    match desired_interest {
        Some(interest) => {
            if entry.registered {
                registry.reregister(socket, entry.record.token, interest)?;
            } else {
                registry.register(socket, entry.record.token, interest)?;
                entry.registered = true;
            }
        }
        None => {
            if entry.registered {
                registry.deregister(socket)?;
                entry.registered = false;
            }
        }
    }

    Ok(())
}

fn reset_socket_after_failure(
    logger: &RuntimeLogger,
    entry: &mut UdpSocketEntry,
    registry: &Registry,
) {
    if let Some(socket) = entry.socket.as_mut()
        && entry.registered
    {
        let deregister_result = registry.deregister(socket);
        if let Err(error) = deregister_result {
            warn!(
                logger,
                "failed to deregister UDP socket {} while resetting failed bind/connect state: {}",
                entry.record.token.0,
                error
            );
        }
    }
    entry.socket = None;
    entry.local_addr = None;
    entry.remote_addr = None;
    entry.read_suspended = false;
    entry.write_suspended = false;
    entry.registered = false;
}

fn suspend_write(
    logger: &RuntimeLogger,
    entry: &mut UdpSocketEntry,
    socket_id: SocketId,
    registry: &Registry,
) -> bool {
    if !entry.suspend_write() {
        return false;
    }

    let update_result = apply_socket_interest(entry, registry);
    if let Err(error) = update_result {
        entry.write_suspended = false;
        error!(
            logger,
            "failed to suspend UDP socket {} by updating readiness interest: {}", socket_id, error
        );
        return false;
    }

    true
}

fn resume_write(state: &mut UdpRuntimeState, socket_id: SocketId, registry: &Registry) -> bool {
    let Some(entry) = state.sockets.get_mut(socket_id.0) else {
        return false;
    };
    if !entry.resume_write() {
        return false;
    }

    let update_result = apply_socket_interest(entry, registry);
    if let Err(error) = update_result {
        entry.write_suspended = true;
        error!(
            state.logger,
            "failed to resume UDP socket {} by updating readiness interest: {}", socket_id, error
        );
        return false;
    }

    true
}

/// Applies one shared socket option directly to a live UDP socket.
///
/// `mio` exposes most UDP broadcast and multicast operations itself. A few socket options,
/// notably explicit multicast interface selection and IPv6 multicast hop configuration, still
/// require dropping to `socket2::SockRef` for access to the underlying OS socket API.
fn apply_udp_socket_option(socket: &MioUdpSocket, option: UdpSocketOption) -> io::Result<()> {
    match option {
        UdpSocketOption::Broadcast(enabled) => socket.set_broadcast(enabled),
        UdpSocketOption::MulticastLoopV4(enabled) => socket.set_multicast_loop_v4(enabled),
        UdpSocketOption::MulticastLoopV6(enabled) => socket.set_multicast_loop_v6(enabled),
        UdpSocketOption::MulticastTtlV4(ttl) => socket.set_multicast_ttl_v4(ttl),
        UdpSocketOption::MulticastHopsV6(hops) => {
            let socket_ref = SockRef::from(socket);
            socket_ref.set_multicast_hops_v6(hops)
        }
        UdpSocketOption::MulticastInterfaceV4(interface) => {
            let socket_ref = SockRef::from(socket);
            socket_ref.set_multicast_if_v4(&interface)
        }
        UdpSocketOption::MulticastInterfaceV6(interface) => {
            let socket_ref = SockRef::from(socket);
            socket_ref.set_multicast_if_v6(interface)
        }
        UdpSocketOption::JoinMulticastV4 { group, interface } => {
            socket.join_multicast_v4(&group, &interface)
        }
        UdpSocketOption::LeaveMulticastV4 { group, interface } => {
            socket.leave_multicast_v4(&group, &interface)
        }
        UdpSocketOption::JoinMulticastV6 { group, interface } => {
            socket.join_multicast_v6(&group, interface)
        }
        UdpSocketOption::LeaveMulticastV6 { group, interface } => {
            socket.leave_multicast_v6(&group, interface)
        }
    }
}

fn send_udp_payload(
    socket: &mut MioUdpSocket,
    payload: IoPayload,
    target: UdpSendTarget,
    udp_send_scratch: &mut [u8],
) -> io::Result<usize> {
    let mut cursor = payload.cursor();
    if cursor.chunk().len() == cursor.remaining() {
        return send_udp_slice(socket, cursor.chunk(), target);
    }

    // mio UDP sends only accept a single contiguous slice, so multi-segment payloads are
    // linearised into a fixed driver-owned scratch buffer before the syscall.
    let mut written = 0;
    while cursor.has_remaining() {
        let chunk = cursor.chunk();
        let chunk_len = chunk.len();
        let next_written = written + chunk_len;
        debug_assert!(next_written <= udp_send_scratch.len());
        udp_send_scratch[written..next_written].copy_from_slice(chunk);
        cursor.advance(chunk_len);
        written = next_written;
    }
    send_udp_slice(socket, &udp_send_scratch[..written], target)
}

fn send_udp_slice(
    socket: &mut MioUdpSocket,
    bytes: &[u8],
    target: UdpSendTarget,
) -> io::Result<usize> {
    match target {
        UdpSendTarget::Connected => socket.send(bytes),
        UdpSendTarget::Unconnected(target) => socket.send_to(bytes, target),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        api::{IoPayload, MAX_UDP_PAYLOAD_BYTES, SendFailureReason, TransmissionId, UdpCommand},
        driver::{
            DriverCommand,
            DriverConfig,
            DriverEvent,
            DriverRequest,
            IoDriver,
            wait_for_request,
        },
        logging::default_runtime_logger,
        pool::{IoBufferConfig, IoPoolConfig},
        prelude::Result,
        test_support::init_test_logger,
    };
    use bytes::Bytes;
    use mio::{Poll, Token};
    use std::{
        net::{Ipv4Addr, SocketAddr, SocketAddrV4},
        sync::Mutex,
        time::{Duration, Instant},
    };

    fn localhost(port: u16) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
    }

    fn resolve_request<T>(request: Result<DriverRequest<T>>) -> T {
        let request = request.expect("enqueue driver request");
        wait_for_request(request).expect("resolve driver request")
    }

    #[derive(Default)]
    struct RecordingEventSink {
        events: Mutex<Vec<DriverEvent>>,
    }

    impl RecordingEventSink {
        fn take_events(&self) -> Vec<DriverEvent> {
            std::mem::take(&mut self.events.lock().expect("recording sink lock"))
        }
    }

    impl DriverEventSink for RecordingEventSink {
        fn publish(&self, event: DriverEvent) -> Result<()> {
            self.events.lock().expect("recording sink lock").push(event);
            Ok(())
        }
    }

    fn wait_for_event(driver: &IoDriver) -> DriverEvent {
        let deadline = Instant::now() + Duration::from_secs(1);

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
        payload.create_byte_clone()
    }

    fn bind_udp_socket_at(
        driver: &IoDriver,
        socket_id: SocketId,
        local_addr: SocketAddr,
    ) -> SocketAddr {
        driver
            .dispatch(DriverCommand::Udp(UdpCommand::Bind {
                socket_id,
                bind: UdpLocalBind::Exact(local_addr),
            }))
            .expect("dispatch UDP bind");

        loop {
            let event = wait_for_event(driver);
            match event {
                DriverEvent::Udp(UdpEvent::Bound {
                    socket_id: bound_socket_id,
                    local_addr,
                }) if bound_socket_id == socket_id => return local_addr,
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for bind: {:?}",
                        other
                    );
                }
            }
        }
    }

    fn bind_udp_socket(driver: &IoDriver, socket_id: SocketId) -> SocketAddr {
        bind_udp_socket_at(driver, socket_id, localhost(0))
    }

    #[test]
    fn udp_unconnected_bind_send_and_receive_work() {
        init_test_logger();

        let driver = IoDriver::start(DriverConfig::default()).expect("driver starts");
        let receiver_id = resolve_request(driver.reserve_socket());
        let sender_id = resolve_request(driver.reserve_socket());

        let receiver_addr = bind_udp_socket(&driver, receiver_id);
        let sender_addr = bind_udp_socket(&driver, sender_id);
        let transmission_id = TransmissionId(1);

        driver
            .dispatch(DriverCommand::Udp(UdpCommand::Send {
                socket_id: sender_id,
                transmission_id,
                payload: IoPayload::Bytes(Bytes::from_static(b"hello")),
                target: Some(receiver_addr),
            }))
            .expect("dispatch UDP send");

        let mut saw_ack = false;
        let mut received_payload = None;
        while !saw_ack || received_payload.is_none() {
            match wait_for_event(&driver) {
                DriverEvent::Udp(UdpEvent::SendAck {
                    socket_id: _,
                    transmission_id: ack_id,
                }) if ack_id == transmission_id => {
                    saw_ack = true;
                }
                DriverEvent::Udp(UdpEvent::Received {
                    socket_id,
                    source,
                    payload,
                }) if socket_id == receiver_id => {
                    assert_eq!(source, sender_addr);
                    received_payload = Some(payload_bytes(payload));
                }
                other => {
                    log::debug!("ignoring unrelated UDP event: {:?}", other);
                }
            }
        }

        assert_eq!(received_payload.expect("received payload"), b"hello"[..]);

        driver.shutdown().expect("driver shuts down");
    }

    #[test]
    fn udp_connected_send_and_receive_work() {
        init_test_logger();

        let driver = IoDriver::start(DriverConfig::default()).expect("driver starts");
        let receiver_id = resolve_request(driver.reserve_socket());
        let sender_id = resolve_request(driver.reserve_socket());

        let receiver_addr = bind_udp_socket(&driver, receiver_id);

        driver
            .dispatch(DriverCommand::Udp(UdpCommand::Connect {
                socket_id: sender_id,
                remote_addr: receiver_addr,
                bind: UdpLocalBind::ForPeer(receiver_addr),
            }))
            .expect("dispatch UDP connect");

        let sender_addr = loop {
            match wait_for_event(&driver) {
                DriverEvent::Udp(UdpEvent::Connected {
                    socket_id,
                    local_addr,
                    remote_addr,
                }) if socket_id == sender_id => {
                    assert_eq!(remote_addr, receiver_addr);
                    break local_addr;
                }
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for UDP connect: {:?}",
                        other
                    );
                }
            }
        };

        let transmission_id = TransmissionId(2);
        driver
            .dispatch(DriverCommand::Udp(UdpCommand::Send {
                socket_id: sender_id,
                transmission_id,
                payload: IoPayload::Bytes(Bytes::from_static(b"world")),
                target: None,
            }))
            .expect("dispatch connected UDP send");

        let mut saw_ack = false;
        let mut received_payload = None;
        while !saw_ack || received_payload.is_none() {
            match wait_for_event(&driver) {
                DriverEvent::Udp(UdpEvent::SendAck {
                    socket_id: _,
                    transmission_id: ack_id,
                }) if ack_id == transmission_id => {
                    saw_ack = true;
                }
                DriverEvent::Udp(UdpEvent::Received {
                    socket_id,
                    source,
                    payload,
                }) if socket_id == receiver_id => {
                    assert_eq!(source, sender_addr);
                    received_payload = Some(payload_bytes(payload));
                }
                other => {
                    log::debug!("ignoring unrelated UDP event: {:?}", other);
                }
            }
        }

        assert_eq!(received_payload.expect("received payload"), b"world"[..]);

        driver.shutdown().expect("driver shuts down");
    }

    #[test]
    fn udp_missing_target_for_unconnected_socket_is_nacked() {
        init_test_logger();

        let driver = IoDriver::start(DriverConfig::default()).expect("driver starts");
        let socket_id = resolve_request(driver.reserve_socket());
        bind_udp_socket(&driver, socket_id);

        let transmission_id = TransmissionId(3);
        driver
            .dispatch(DriverCommand::Udp(UdpCommand::Send {
                socket_id,
                transmission_id,
                payload: IoPayload::Bytes(Bytes::from_static(b"oops")),
                target: None,
            }))
            .expect("dispatch invalid UDP send");

        loop {
            match wait_for_event(&driver) {
                DriverEvent::Udp(UdpEvent::SendNack {
                    socket_id: _,
                    transmission_id: nack_id,
                    reason,
                }) if nack_id == transmission_id => {
                    assert_eq!(reason, SendFailureReason::MissingTargetForUnconnectedSocket);
                    break;
                }
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for UDP SendNack: {:?}",
                        other
                    );
                }
            }
        }

        driver.shutdown().expect("driver shuts down");
    }

    #[test]
    fn udp_unexpected_target_for_connected_socket_is_nacked() {
        init_test_logger();

        let driver = IoDriver::start(DriverConfig::default()).expect("driver starts");
        let receiver_id = resolve_request(driver.reserve_socket());
        let sender_id = resolve_request(driver.reserve_socket());

        let receiver_addr = bind_udp_socket(&driver, receiver_id);

        driver
            .dispatch(DriverCommand::Udp(UdpCommand::Connect {
                socket_id: sender_id,
                remote_addr: receiver_addr,
                bind: UdpLocalBind::ForPeer(receiver_addr),
            }))
            .expect("dispatch UDP connect");

        loop {
            match wait_for_event(&driver) {
                DriverEvent::Udp(UdpEvent::Connected { socket_id, .. })
                    if socket_id == sender_id =>
                {
                    break;
                }
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for UDP connect: {:?}",
                        other
                    );
                }
            }
        }

        let transmission_id = TransmissionId(4);
        driver
            .dispatch(DriverCommand::Udp(UdpCommand::Send {
                socket_id: sender_id,
                transmission_id,
                payload: IoPayload::Bytes(Bytes::from_static(b"oops")),
                target: Some(receiver_addr),
            }))
            .expect("dispatch invalid connected UDP send");

        loop {
            match wait_for_event(&driver) {
                DriverEvent::Udp(UdpEvent::SendNack {
                    socket_id: _,
                    transmission_id: nack_id,
                    reason,
                }) if nack_id == transmission_id => {
                    assert_eq!(
                        reason,
                        SendFailureReason::UnexpectedTargetForConnectedSocket
                    );
                    break;
                }
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for UDP SendNack: {:?}",
                        other
                    );
                }
            }
        }

        driver.shutdown().expect("driver shuts down");
    }

    #[test]
    fn udp_broadcast_configuration_is_reported() {
        init_test_logger();

        let driver = IoDriver::start(DriverConfig::default()).expect("driver starts");
        let socket_id = resolve_request(driver.reserve_socket());
        bind_udp_socket(&driver, socket_id);
        let option = UdpSocketOption::Broadcast(true);

        driver
            .dispatch(DriverCommand::Udp(UdpCommand::Configure {
                socket_id,
                option,
            }))
            .expect("dispatch UDP configure");

        loop {
            match wait_for_event(&driver) {
                DriverEvent::Udp(UdpEvent::Configured {
                    socket_id: configured_socket_id,
                    option: configured_option,
                }) if configured_socket_id == socket_id => {
                    assert_eq!(configured_option, option);
                    break;
                }
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for UDP configure: {:?}",
                        other
                    );
                }
            }
        }

        driver.shutdown().expect("driver shuts down");
    }

    #[test]
    fn udp_ipv4_multicast_membership_configuration_is_reported() {
        init_test_logger();

        let driver = IoDriver::start(DriverConfig::default()).expect("driver starts");
        let socket_id = resolve_request(driver.reserve_socket());
        bind_udp_socket_at(
            &driver,
            socket_id,
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)),
        );
        let group = Ipv4Addr::new(224, 0, 0, 251);
        let join_option = UdpSocketOption::JoinMulticastV4 {
            group,
            interface: Ipv4Addr::UNSPECIFIED,
        };

        driver
            .dispatch(DriverCommand::Udp(UdpCommand::Configure {
                socket_id,
                option: join_option,
            }))
            .expect("dispatch UDP multicast join");

        loop {
            match wait_for_event(&driver) {
                DriverEvent::Udp(UdpEvent::Configured {
                    socket_id: configured_socket_id,
                    option,
                }) if configured_socket_id == socket_id => {
                    assert_eq!(option, join_option);
                    break;
                }
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for UDP multicast join: {:?}",
                        other
                    );
                }
            }
        }

        let leave_option = UdpSocketOption::LeaveMulticastV4 {
            group,
            interface: Ipv4Addr::UNSPECIFIED,
        };
        driver
            .dispatch(DriverCommand::Udp(UdpCommand::Configure {
                socket_id,
                option: leave_option,
            }))
            .expect("dispatch UDP multicast leave");

        loop {
            match wait_for_event(&driver) {
                DriverEvent::Udp(UdpEvent::Configured {
                    socket_id: configured_socket_id,
                    option,
                }) if configured_socket_id == socket_id => {
                    assert_eq!(option, leave_option);
                    break;
                }
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for UDP multicast leave: {:?}",
                        other
                    );
                }
            }
        }

        driver.shutdown().expect("driver shuts down");
    }

    #[test]
    fn udp_read_suspends_and_resumes_when_ingress_capacity_returns() {
        init_test_logger();

        let pool_config = IoPoolConfig {
            chunk_size: MAX_UDP_PAYLOAD_BYTES,
            initial_chunk_count: 1,
            max_chunk_count: 2,
            encode_buf_min_free_space: 64,
        };
        let driver_config = DriverConfig {
            buffer_config: IoBufferConfig {
                ingress: pool_config.clone(),
                egress: pool_config,
            },
            ..DriverConfig::default()
        };
        let driver = IoDriver::start(driver_config).expect("driver starts");
        let receiver_id = resolve_request(driver.reserve_socket());
        let sender_id = resolve_request(driver.reserve_socket());

        let receiver_addr = bind_udp_socket(&driver, receiver_id);
        bind_udp_socket(&driver, sender_id);

        driver
            .dispatch(DriverCommand::Udp(UdpCommand::Send {
                socket_id: sender_id,
                transmission_id: TransmissionId(10),
                payload: IoPayload::Bytes(Bytes::from_static(b"first")),
                target: Some(receiver_addr),
            }))
            .expect("dispatch first UDP send");

        let first_lease = loop {
            match wait_for_event(&driver) {
                DriverEvent::Udp(UdpEvent::Received {
                    socket_id,
                    payload: IoPayload::Lease(lease),
                    ..
                }) if socket_id == receiver_id => break lease,
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for first UDP receive: {:?}",
                        other
                    );
                }
            }
        };

        driver
            .dispatch(DriverCommand::Udp(UdpCommand::Send {
                socket_id: sender_id,
                transmission_id: TransmissionId(11),
                payload: IoPayload::Bytes(Bytes::from_static(b"second")),
                target: Some(receiver_addr),
            }))
            .expect("dispatch second UDP send");

        let second_lease = loop {
            match wait_for_event(&driver) {
                DriverEvent::Udp(UdpEvent::Received {
                    socket_id,
                    payload: IoPayload::Lease(lease),
                    ..
                }) if socket_id == receiver_id => break lease,
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for second UDP receive: {:?}",
                        other
                    );
                }
            }
        };

        driver
            .dispatch(DriverCommand::Udp(UdpCommand::Send {
                socket_id: sender_id,
                transmission_id: TransmissionId(12),
                payload: IoPayload::Bytes(Bytes::from_static(b"third")),
                target: Some(receiver_addr),
            }))
            .expect("dispatch third UDP send");

        loop {
            match wait_for_event(&driver) {
                DriverEvent::Udp(UdpEvent::ReadSuspended { socket_id })
                    if socket_id == receiver_id =>
                {
                    break;
                }
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for ReadSuspended: {:?}",
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
                DriverEvent::Udp(UdpEvent::ReadResumed { socket_id })
                    if socket_id == receiver_id =>
                {
                    saw_resume = true;
                }
                DriverEvent::Udp(UdpEvent::Received {
                    socket_id, payload, ..
                }) if socket_id == receiver_id => {
                    third_payload = Some(payload_bytes(payload));
                }
                other => {
                    log::debug!(
                        "ignoring unrelated event while waiting for ReadResumed/Received: {:?}",
                        other
                    );
                }
            }
        }

        assert_eq!(third_payload.expect("third payload"), b"third"[..]);

        driver.shutdown().expect("driver shuts down");
    }

    #[test]
    fn udp_write_resumes_after_suspended_socket_becomes_writable() {
        init_test_logger();

        let poll = Poll::new().expect("create mio poll");
        let mut state = UdpRuntimeState::new(default_runtime_logger());
        let event_sink = RecordingEventSink::default();
        let socket_id = SocketId(0);
        let transmission_id = TransmissionId(20);
        let resumed_transmission_id = TransmissionId(21);
        let receiver = std::net::UdpSocket::bind(localhost(0)).expect("bind UDP receiver");
        let receiver_addr = receiver.local_addr().expect("receiver addr");

        state.reserve_socket(socket_id, Token(1));
        state
            .handle_bind(
                socket_id,
                UdpLocalBind::Exact(localhost(0)),
                poll.registry(),
                &event_sink,
            )
            .expect("bind UDP socket");
        let _ = event_sink.take_events();

        {
            let entry = state
                .sockets
                .get_mut(socket_id.0)
                .expect("reserved UDP socket entry");
            assert!(suspend_write(
                &state.logger,
                entry,
                socket_id,
                poll.registry()
            ));
        }

        let mut udp_send_scratch = [0_u8; MAX_UDP_PAYLOAD_BYTES];
        state
            .handle_send(
                socket_id,
                transmission_id,
                IoPayload::Bytes(Bytes::from_static(b"blocked")),
                Some(receiver_addr),
                poll.registry(),
                &event_sink,
                &mut udp_send_scratch,
            )
            .expect("send while suspended");

        let events = event_sink.take_events();
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            DriverEvent::Udp(UdpEvent::SendNack {
                socket_id: observed_socket_id,
                transmission_id: observed_transmission_id,
                reason: SendFailureReason::Backpressure,
            }) if *observed_socket_id == socket_id
                && *observed_transmission_id == transmission_id
        ));

        state
            .handle_writable(socket_id, poll.registry(), &event_sink)
            .expect("resume suspended UDP writes");
        let events = event_sink.take_events();
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            DriverEvent::Udp(UdpEvent::WriteResumed {
                socket_id: observed_socket_id,
            }) if *observed_socket_id == socket_id
        ));

        state
            .handle_send(
                socket_id,
                resumed_transmission_id,
                IoPayload::Bytes(Bytes::from_static(b"resumed")),
                Some(receiver_addr),
                poll.registry(),
                &event_sink,
                &mut udp_send_scratch,
            )
            .expect("send after write resume");
        let events = event_sink.take_events();
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            DriverEvent::Udp(UdpEvent::SendAck {
                socket_id: observed_socket_id,
                transmission_id: observed_transmission_id,
            }) if *observed_socket_id == socket_id
                && *observed_transmission_id == resumed_transmission_id
        ));
    }
}
