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
        UdpEvent,
    },
    errors::{Error, Result},
    pool::IngressPool,
};
use bytes::Buf;
use flotsync_utils::option_when;
use mio::{Interest, Registry, net::UdpSocket as MioUdpSocket};
use std::{
    io,
    io::ErrorKind,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
};

/// UDP-side runtime state owned by the shared driver.
#[derive(Debug, Default)]
pub(super) struct UdpRuntimeState {
    sockets: SlotRegistry<UdpSocketEntry>,
}

#[derive(Debug)]
struct UdpSocketEntry {
    record: ResourceRecord,
    socket: Option<MioUdpSocket>,
    local_addr: Option<SocketAddr>,
    remote_addr: Option<SocketAddr>,
    read_suspended: bool,
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
            registered: false,
        }
    }

    /// Returns whether this entry currently owns a live OS UDP socket handle.
    fn is_open(&self) -> bool {
        self.socket.is_some()
    }
}

#[derive(Clone, Copy, Debug)]
enum UdpSendTarget {
    Connected,
    Unconnected(SocketAddr),
}

impl UdpRuntimeState {
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
    ) -> Result<ResourceRecord> {
        let entry = self
            .sockets
            .remove(socket_id.0)
            .ok_or(Error::UnknownSocket { socket_id })?;
        if let Some(mut socket) = entry.socket {
            if entry.registered {
                let deregister_result = registry.deregister(&mut socket);
                if let Err(error) = deregister_result {
                    log::warn!(
                        "failed to deregister UDP socket {} during release: {}",
                        socket_id,
                        error
                    );
                }
            }
        }
        Ok(entry.record)
    }

    pub(super) fn record_socket_readiness_hit(&mut self, socket_id: SocketId) {
        if let Some(entry) = self.sockets.get_mut(socket_id.0) {
            entry.record.readiness_hits += 1;
        }
    }

    pub(super) fn handle_bind(
        &mut self,
        socket_id: SocketId,
        local_addr: SocketAddr,
        registry: &Registry,
        event_sink: &dyn DriverEventSink,
    ) -> Result<()> {
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

        let mut socket = match bind_udp_socket(local_addr) {
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

        let register_result =
            registry.register(&mut socket, entry.record.token, Interest::READABLE);
        if let Err(error) = register_result {
            event_sink.publish(super::DriverEvent::Udp(UdpEvent::BindFailed {
                socket_id,
                local_addr,
                error_kind: error.kind(),
            }))?;
            return Ok(());
        }

        let actual_local_addr = match socket.local_addr() {
            Ok(addr) => addr,
            Err(error) => {
                log::warn!(
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
        entry.registered = true;

        event_sink.publish(super::DriverEvent::Udp(UdpEvent::Bound {
            socket_id,
            local_addr: actual_local_addr,
        }))
    }

    pub(super) fn handle_connect(
        &mut self,
        socket_id: SocketId,
        remote_addr: SocketAddr,
        local_addr: Option<SocketAddr>,
        registry: &Registry,
        event_sink: &dyn DriverEventSink,
    ) -> Result<()> {
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

        let bind_addr = local_addr.unwrap_or_else(|| default_udp_bind_addr(remote_addr));
        let mut socket = match bind_udp_socket(bind_addr) {
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

        let register_result =
            registry.register(&mut socket, entry.record.token, Interest::READABLE);
        if let Err(error) = register_result {
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
                log::warn!(
                    "failed to query local address for connected UDP socket {}: {}",
                    socket_id,
                    error
                );
                bind_addr
            }
        };

        entry.socket = Some(socket);
        entry.local_addr = Some(actual_local_addr);
        entry.remote_addr = Some(remote_addr);
        entry.read_suspended = false;
        entry.registered = true;

        event_sink.publish(super::DriverEvent::Udp(UdpEvent::Connected {
            socket_id,
            local_addr: actual_local_addr,
            remote_addr,
        }))
    }

    pub(super) fn handle_send(
        &mut self,
        socket_id: SocketId,
        transmission_id: TransmissionId,
        payload: IoPayload,
        target: Option<SocketAddr>,
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
                log::error!(
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
            Err(error) => event_sink.publish(super::DriverEvent::Udp(UdpEvent::SendNack {
                socket_id,
                transmission_id,
                reason: SendFailureReason::from(&error),
            })),
        }
    }

    pub(super) fn handle_readable(
        &mut self,
        socket_id: SocketId,
        registry: &Registry,
        ingress_pool: &IngressPool,
        event_sink: &dyn DriverEventSink,
    ) -> Result<()> {
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
                    return Ok(());
                }
            };

            let recv_result = {
                let Some(entry) = self.sockets.get_mut(socket_id.0) else {
                    log::debug!(
                        "ignoring stale UDP readability for unknown socket {}",
                        socket_id
                    );
                    return Ok(());
                };
                let Some(socket) = entry.socket.as_mut() else {
                    log::debug!(
                        "ignoring stale UDP readability for socket {} without OS handle",
                        socket_id
                    );
                    return Ok(());
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
                    return Ok(());
                }
                Err(error) => {
                    log::error!("UDP socket {} recv failed: {}", socket_id, error);
                    return Ok(());
                }
            }
        }
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
        if entry.read_suspended || !entry.registered {
            return false;
        }

        let Some(socket) = entry.socket.as_mut() else {
            return false;
        };

        let deregister_result = registry.deregister(socket);
        if let Err(error) = deregister_result {
            log::error!(
                "failed to suspend UDP socket {} by deregistering it: {}",
                socket_id,
                error
            );
            return false;
        }

        entry.read_suspended = true;
        entry.registered = false;
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

        let Some(socket) = entry.socket.as_mut() else {
            return false;
        };

        let register_result = registry.register(socket, entry.record.token, Interest::READABLE);
        if let Err(error) = register_result {
            log::error!(
                "failed to resume UDP socket {} by re-registering it: {}",
                socket_id,
                error
            );
            return false;
        }

        entry.read_suspended = false;
        entry.registered = true;
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

fn default_udp_bind_addr(remote_addr: SocketAddr) -> SocketAddr {
    match remote_addr {
        SocketAddr::V4(_) => SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)),
        SocketAddr::V6(_) => SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0)),
    }
}

fn bind_udp_socket(local_addr: SocketAddr) -> io::Result<MioUdpSocket> {
    MioUdpSocket::bind(local_addr)
}

fn send_udp_payload(
    socket: &mut MioUdpSocket,
    payload: IoPayload,
    target: UdpSendTarget,
    udp_send_scratch: &mut [u8],
) -> io::Result<usize> {
    match payload {
        IoPayload::Bytes(bytes) => send_udp_slice(socket, bytes.as_ref(), target),
        IoPayload::Lease(lease) => {
            let mut cursor = lease.cursor();
            if cursor.chunk().len() == cursor.remaining() {
                return send_udp_slice(socket, cursor.chunk(), target);
            }

            // mio UDP sends only accept a single contiguous slice, so multi-segment payloads are
            // linearised into a fixed driver-owned scratch buffer before the syscall.
            let mut written = 0;
            while cursor.remaining() > 0 {
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
    }
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
        pool::{IoBufferConfig, IoPoolConfig},
        prelude::Result,
        test_support::init_test_logger,
    };
    use bytes::Bytes;
    use std::{
        net::{Ipv4Addr, SocketAddr, SocketAddrV4},
        time::{Duration, Instant},
    };

    fn localhost(port: u16) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
    }

    fn resolve_request<T>(request: Result<DriverRequest<T>>) -> T {
        let request = request.expect("enqueue driver request");
        wait_for_request(request).expect("resolve driver request")
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
        match payload {
            IoPayload::Lease(lease) => lease.create_byte_clone(),
            IoPayload::Bytes(bytes) => bytes,
        }
    }

    fn bind_udp_socket(driver: &IoDriver, socket_id: SocketId) -> SocketAddr {
        driver
            .dispatch(DriverCommand::Udp(UdpCommand::Bind {
                socket_id,
                local_addr: localhost(0),
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
                local_addr: None,
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
                local_addr: None,
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
}
