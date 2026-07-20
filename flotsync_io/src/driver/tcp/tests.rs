//! TCP driver tests.

use super::*;
use crate::{
    driver::{DriverCommand, DriverConfig, DriverEvent, DriverRequest, IoDriver, wait_for_request},
    prelude::Result,
    test_support::{
        ReservedSocketKind,
        bind_reserved_tcp_listener,
        init_test_logger,
        reserve_sockets,
    },
};
use std::{
    net::{Ipv4Addr, Shutdown, SocketAddr, SocketAddrV4, TcpListener, TcpStream},
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

        assert!(
            Instant::now() < deadline,
            "timed out waiting for flotsync_io driver event"
        );

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

fn start_bind_reuse_driver() -> IoDriver {
    IoDriver::start(DriverConfig {
        bind_reuse_address: true,
        ..DriverConfig::default()
    })
    .expect("driver starts")
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
                    "ignoring unrelated event while waiting for TCP listen failure: {other:?}"
                );
            }
        }
    }

    driver.shutdown().expect("driver shuts down");
}

#[test]
fn socket2_tcp_reuse_address_allows_rebinding_on_the_same_thread() {
    let reservation = Socket::new(
        crate::socket_support::socket_domain(localhost(0)),
        Type::STREAM,
        Some(Protocol::TCP),
    )
    .expect("create first TCP socket");
    configure_bind_reuse(&reservation).expect("enable TCP re-use on first TCP socket");
    reservation
        .bind(&SockAddr::from(localhost(0)))
        .expect("bind first TCP socket");
    let reserved_addr = reservation
        .local_addr()
        .expect("first TCP socket local addr")
        .as_socket()
        .expect("TCP reservation must use an IP socket address");

    let rebound = Socket::new(
        crate::socket_support::socket_domain(localhost(0)),
        Type::STREAM,
        Some(Protocol::TCP),
    )
    .expect("create second TCP socket");
    configure_bind_reuse(&rebound).expect("enable TCP re-use on second TCP socket");
    rebound
        .bind(&SockAddr::from(reserved_addr))
        .expect("rebind TCP socket with SO_REUSEADDR");
}

#[test]
fn tcp_listener_accept_requires_adoption_before_reads() {
    init_test_logger();

    let mut listener_lease = reserve_sockets(&[ReservedSocketKind::TcpListener]);
    let driver = start_bind_reuse_driver();
    let listener_id = resolve_request(driver.reserve_listener());
    driver
        .dispatch(DriverCommand::Tcp(crate::api::TcpCommand::Listen {
            listener_id,
            local_addr: listener_lease.addr(0),
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
                    "ignoring unrelated event while waiting for TCP listening event: {other:?}"
                );
            }
        }
    };
    listener_lease.release_binding(0);

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
                    "ignoring unrelated event while waiting for TCP accepted event: {other:?}"
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
                assert_eq!(payload.to_vec().as_slice(), b"pending");
                break;
            }
            other => {
                log::debug!(
                    "ignoring unrelated event while waiting for adopted TCP payload: {other:?}"
                );
            }
        }
    }

    drop(client);
    listener_lease
        .rebind_binding(0)
        .expect("rebind reserved TCP listener");
    driver.shutdown().expect("driver shuts down");
}

#[test]
fn tcp_listener_reject_closes_pending_connection() {
    init_test_logger();

    let mut listener_lease = reserve_sockets(&[ReservedSocketKind::TcpListener]);
    let driver = start_bind_reuse_driver();
    let listener_id = resolve_request(driver.reserve_listener());
    driver
        .dispatch(DriverCommand::Tcp(crate::api::TcpCommand::Listen {
            listener_id,
            local_addr: listener_lease.addr(0),
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
    listener_lease.release_binding(0);

    let mut client = TcpStream::connect(listener_addr).expect("connect TCP client");
    client
        .set_read_timeout(Some(Duration::from_millis(500)))
        .expect("set TCP client read timeout");
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
    listener_lease
        .rebind_binding(0)
        .expect("rebind reserved TCP listener");
    driver.shutdown().expect("driver shuts down");
}

#[test]
fn tcp_connect_send_and_receive_work() {
    init_test_logger();

    let mut listener_lease = reserve_sockets(&[ReservedSocketKind::TcpListener]);
    let listener =
        bind_reserved_tcp_listener(&listener_lease, 0).expect("bind reserved TCP listener");
    listener_lease.release_binding(0);
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
                log::debug!("ignoring unrelated event while waiting for TCP connect: {other:?}");
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
                received_payload = Some(payload.to_vec());
            }
            other => {
                log::debug!("ignoring unrelated TCP event: {other:?}");
            }
        }
    }

    assert_eq!(server_rx.recv().expect("server payload"), *b"hello");
    assert_eq!(received_payload.expect("received payload"), b"world"[..]);

    server.join().expect("join server thread");
    listener_lease
        .rebind_binding(0)
        .expect("rebind reserved TCP listener");
    driver.shutdown().expect("driver shuts down");
}

#[test]
#[cfg_attr(
    windows,
    ignore = "Windows can self-connect after probing a freed loopback port."
)]
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
                    "ignoring unrelated event while waiting for TCP connect failure: {other:?}"
                );
            }
        }
    }

    driver.shutdown().expect("driver shuts down");
}

#[test]
#[cfg_attr(
    windows,
    ignore = "Windows loopback can buffer 4 MiB without write backpressure."
)]
fn tcp_send_while_another_send_is_pending_is_nacked() {
    init_test_logger();

    let mut listener_lease = reserve_sockets(&[ReservedSocketKind::TcpListener]);
    let listener =
        bind_reserved_tcp_listener(&listener_lease, 0).expect("bind reserved TCP listener");
    listener_lease.release_binding(0);
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
                transmission_id: TransmissionId(11),
                reason,
            }) => {
                assert_eq!(reason, SendFailureReason::Backpressure);
                saw_backpressure_nack = true;
            }
            other => {
                log::debug!(
                    "ignoring unrelated event while waiting for TCP write suspension/backpressure nack: {other:?}"
                );
            }
        }
    }

    server.join().expect("join server thread");
    listener_lease
        .rebind_binding(0)
        .expect("rebind reserved TCP listener");
    driver.shutdown().expect("driver shuts down");
}

#[test]
#[cfg_attr(
    windows,
    ignore = "Windows loopback can buffer 4 MiB without write backpressure."
)]
fn tcp_write_resumes_after_pending_send_drains() {
    init_test_logger();

    let mut listener_lease = reserve_sockets(&[ReservedSocketKind::TcpListener]);
    let listener =
        bind_reserved_tcp_listener(&listener_lease, 0).expect("bind reserved TCP listener");
    listener_lease.release_binding(0);
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
                    "ignoring unrelated event while waiting for TCP WriteSuspended: {other:?}"
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
                    "ignoring unrelated event while waiting for TCP SendAck/WriteResumed: {other:?}"
                );
            }
        }
    }

    server.join().expect("join server thread");
    listener_lease
        .rebind_binding(0)
        .expect("rebind reserved TCP listener");
    driver.shutdown().expect("driver shuts down");
}

#[test]
fn tcp_graceful_close_waits_for_pending_send() {
    init_test_logger();

    let mut listener_lease = reserve_sockets(&[ReservedSocketKind::TcpListener]);
    let listener =
        bind_reserved_tcp_listener(&listener_lease, 0).expect("bind reserved TCP listener");
    listener_lease.release_binding(0);
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
                    "ignoring unrelated event while waiting for TCP graceful close: {other:?}"
                );
            }
        }
    }

    assert_eq!(server_rx.recv().expect("server payload"), *b"hello");

    server.join().expect("join server thread");
    listener_lease
        .rebind_binding(0)
        .expect("rebind reserved TCP listener");
    driver.shutdown().expect("driver shuts down");
}

#[test]
fn tcp_send_and_close_supports_chained_payloads() {
    init_test_logger();

    let mut listener_lease = reserve_sockets(&[ReservedSocketKind::TcpListener]);
    let listener =
        bind_reserved_tcp_listener(&listener_lease, 0).expect("bind reserved TCP listener");
    listener_lease.release_binding(0);
    let remote_addr = listener.local_addr().expect("listener addr");
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept TCP stream");
        let mut received = Vec::new();
        stream
            .read_to_end(&mut received)
            .expect("server read to end");
        received
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
            }) if connected_id == connection_id => break,
            _ => {}
        }
    }

    let transmission_id = TransmissionId(21);
    driver
        .dispatch(DriverCommand::Tcp(crate::api::TcpCommand::SendAndClose {
            connection_id,
            transmission_id,
            payload: IoPayload::chain([
                IoPayload::from_static(b"he"),
                IoPayload::from_static(b"llo"),
            ]),
        }))
        .expect("dispatch chained send-and-close");

    let mut saw_ack = false;
    let mut saw_closed = false;
    while !saw_ack || !saw_closed {
        match wait_for_event(&driver) {
            DriverEvent::Tcp(TcpEvent::SendAck {
                connection_id: ack_connection_id,
                transmission_id: ack_id,
            }) if ack_connection_id == connection_id && ack_id == transmission_id => {
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
                    "ignoring unrelated event while waiting for TCP send-and-close: {other:?}"
                );
            }
        }
    }

    let received = server.join().expect("join server thread");
    assert_eq!(received, b"hello");
    listener_lease
        .rebind_binding(0)
        .expect("rebind reserved TCP listener");
    driver.shutdown().expect("driver shuts down");
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "This integration-style test exercises the full suspend/resume TCP read flow."
)]
fn tcp_read_suspends_and_resumes_when_ingress_capacity_returns() {
    init_test_logger();

    let mut listener_lease = reserve_sockets(&[ReservedSocketKind::TcpListener]);
    let listener =
        bind_reserved_tcp_listener(&listener_lease, 0).expect("bind reserved TCP listener");
    listener_lease.release_binding(0);
    let remote_addr = listener.local_addr().expect("listener addr");
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept TCP stream");
        stream
            .write_all(&vec![b'a'; 300])
            .expect("server write payload");
        stream
            .shutdown(Shutdown::Write)
            .expect("shutdown TCP write half after payload");
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
                    "ignoring unrelated event while waiting for first TCP receive: {other:?}"
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
                    "ignoring unrelated event while waiting for second TCP receive: {other:?}"
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
                    "ignoring unrelated event while waiting for TCP ReadSuspended: {other:?}"
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
                third_payload = Some(payload.to_vec());
            }
            other => {
                log::debug!(
                    "ignoring unrelated event while waiting for TCP ReadResumed/Received: {other:?}"
                );
            }
        }
    }

    assert_eq!(third_payload.expect("third payload").len(), 44);

    server.join().expect("join server thread");
    listener_lease
        .rebind_binding(0)
        .expect("rebind reserved TCP listener");
    driver.shutdown().expect("driver shuts down");
}
