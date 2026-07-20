//! Tests for TCP bridge sessions and pending-session behaviour.

use super::{fixtures::*, *};

#[test]
fn tcp_bridge_opens_sessions_and_routes_events_to_the_session_recipient() {
    let mut listener_lease = reserve_sockets(&[ReservedSocketKind::TcpListener]);
    let listener =
        bind_reserved_tcp_listener(&listener_lease, 0).expect("bind reserved TCP listener");
    listener_lease.release_binding(0);
    let remote_addr = listener.local_addr().expect("listener address");
    let (server_tx, server_rx) = mpsc::sync_channel(1);
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept TCP stream");
        let mut buf = [0_u8; 5];
        stream.read_exact(&mut buf).expect("read exact request");
        server_tx.send(buf).expect("send request bytes");
        stream.write_all(b"world").expect("write TCP response");
    });

    let system = build_test_kompact_system();
    let driver_component = system.create(|| IoDriverComponent::new(DriverConfig::default()));
    let driver_for_bridge = driver_component.clone();
    let bridge = system.create(move || IoBridge::new(&driver_for_bridge));
    let (events_tx, events_rx) = mpsc::channel();
    let event_probe = system.create(move || TcpSessionEventProbe::new(events_tx));

    start_component(&system, &driver_component);
    start_component(&system, &bridge);
    start_component(&system, &event_probe);

    let bridge_handle = IoBridgeHandle::from_component(&bridge);
    let opened_session = bridge_handle
        .open_tcp_session(OpenTcpSession {
            remote_addr,
            local_addr: None,
            events_to: event_probe.actor_ref().recipient(),
        })
        .wait_timeout(WAIT_TIMEOUT)
        .expect("TCP open future")
        .expect("TCP session open");
    assert_eq!(opened_session.peer_addr, remote_addr);

    opened_session.session.tell(TcpSessionRequest::Send {
        transmission_id: TransmissionId(7),
        payload: IoPayload::Bytes(Bytes::from_static(b"hello")),
    });

    let mut saw_ack = false;
    let mut saw_received = false;
    while !saw_ack || !saw_received {
        match recv_until(&events_rx, |_| true) {
            TcpSessionEvent::SendAck { transmission_id } => {
                assert_eq!(transmission_id, TransmissionId(7));
                saw_ack = true;
            }
            TcpSessionEvent::Received { payload } => {
                assert_eq!(payload.to_vec().as_slice(), b"world");
                saw_received = true;
            }
            TcpSessionEvent::Closed { reason } => {
                assert!(matches!(
                    reason,
                    CloseReason::Graceful | CloseReason::Aborted
                ));
            }
            other => {
                log::debug!("ignoring unrelated TCP session event in test: {other:?}");
            }
        }
    }

    opened_session
        .session
        .tell(TcpSessionRequest::Close { abort: false });
    match recv_until(&events_rx, |event| {
        matches!(event, TcpSessionEvent::Closed { .. })
    }) {
        TcpSessionEvent::Closed { reason } => {
            assert!(matches!(
                reason,
                CloseReason::Graceful | CloseReason::Aborted
            ));
        }
        other => unreachable!("filtered to TCP Closed, got {other:?}"),
    }

    assert_eq!(
        server_rx
            .recv_timeout(WAIT_TIMEOUT)
            .expect("server payload"),
        *b"hello"
    );

    server.join().expect("join TCP server thread");
    listener_lease
        .rebind_binding(0)
        .expect("rebind reserved TCP listener");
    drop(opened_session);
    drop(bridge_handle);
    kill_component(&system, event_probe);
    kill_component(&system, bridge);
    kill_component(&system, driver_component);
    system.shutdown().wait().expect("Kompact shutdown");
}

#[test]
#[allow(
    clippy::match_wildcard_for_single_variants,
    reason = "The receive helper filters to one event variant and the fallback keeps assertion diagnostics precise."
)]
fn tcp_listener_exposes_pending_sessions_before_session_io_begins() {
    let mut listener_lease = reserve_sockets(&[ReservedSocketKind::TcpListener]);
    let system = build_test_kompact_system_with(enable_bind_reuse_address);
    let driver_component = system.create(|| IoDriverComponent::new(DriverConfig::default()));
    let driver_for_bridge = driver_component.clone();
    let bridge = system.create(move || IoBridge::new(&driver_for_bridge));
    let (listener_events_tx, listener_events_rx) = mpsc::channel();
    let listener_probe = system.create(move || TcpListenerEventProbe::new(listener_events_tx));
    let (session_events_tx, session_events_rx) = mpsc::channel();
    let session_probe = system.create(move || TcpSessionEventProbe::new(session_events_tx));

    start_component(&system, &driver_component);
    start_component(&system, &bridge);
    start_component(&system, &listener_probe);
    start_component(&system, &session_probe);

    let bridge_handle = IoBridgeHandle::from_component(&bridge);
    let opened_listener = bridge_handle
        .open_tcp_listener(OpenTcpListener {
            local_addr: listener_lease.addr(0),
            incoming_to: listener_probe.actor_ref().recipient(),
        })
        .wait_timeout(WAIT_TIMEOUT)
        .expect("TCP listener open future")
        .expect("TCP listener open");
    let listener_addr = opened_listener.local_addr;
    listener_lease.release_binding(0);

    let mut client = std::net::TcpStream::connect(listener_addr).expect("connect TCP client");
    let pending = match recv_until(&listener_events_rx, |event| {
        matches!(event, TcpListenerEvent::Incoming { .. })
    }) {
        TcpListenerEvent::Incoming { pending, .. } => pending,
        other => unreachable!("filtered to TCP listener Incoming, got {other:?}"),
    };

    client
        .write_all(b"hello")
        .expect("write pending TCP payload");
    assert!(
        session_events_rx
            .recv_timeout(Duration::from_millis(50))
            .is_err()
    );

    let session_ref = pending
        .accept(session_probe.actor_ref().recipient())
        .wait_timeout(WAIT_TIMEOUT)
        .expect("pending TCP session accept future")
        .expect("pending TCP session accept");

    match recv_until(&session_events_rx, |_| true) {
        TcpSessionEvent::Received { payload } => {
            assert_eq!(payload.to_vec().as_slice(), b"hello");
        }
        other => {
            panic!("expected accepted TCP session to start with Received, got {other:?}");
        }
    }

    session_ref.tell(TcpSessionRequest::Close { abort: false });
    match recv_until(&session_events_rx, |event| {
        matches!(event, TcpSessionEvent::Closed { .. })
    }) {
        TcpSessionEvent::Closed { reason } => {
            assert!(matches!(
                reason,
                CloseReason::Graceful | CloseReason::Aborted
            ));
        }
        other => unreachable!("filtered to TCP session Closed, got {other:?}"),
    }

    opened_listener
        .listener
        .tell(super::TcpListenerRequest::Close);
    match recv_until(&listener_events_rx, |event| {
        matches!(event, TcpListenerEvent::Closed)
    }) {
        TcpListenerEvent::Closed => {}
        other => unreachable!("filtered to TCP listener Closed, got {other:?}"),
    }

    listener_lease
        .rebind_binding(0)
        .expect("rebind reserved TCP listener");
    drop(client);
    drop(session_ref);
    drop(opened_listener);
    drop(bridge_handle);
    kill_component(&system, session_probe);
    kill_component(&system, listener_probe);
    kill_component(&system, bridge);
    kill_component(&system, driver_component);
    system.shutdown().wait().expect("Kompact shutdown");
}

#[test]
#[allow(
    clippy::match_wildcard_for_single_variants,
    reason = "The receive helper filters to one event variant and the fallback keeps assertion diagnostics precise."
)]
fn tcp_pending_session_accept_tagged_forwards_runtime_tagged_events() {
    let mut listener_lease = reserve_sockets(&[ReservedSocketKind::TcpListener]);
    let system = build_test_kompact_system_with(enable_bind_reuse_address);
    let driver_component = system.create(|| IoDriverComponent::new(DriverConfig::default()));
    let driver_for_bridge = driver_component.clone();
    let bridge = system.create(move || IoBridge::new(&driver_for_bridge));
    let (listener_events_tx, listener_events_rx) = mpsc::channel();
    let listener_probe = system.create(move || TcpListenerEventProbe::new(listener_events_tx));
    let (tagged_events_tx, tagged_events_rx) = mpsc::channel();
    let tagged_probe = system.create(move || TaggedSessionEventProbe::new(tagged_events_tx));

    start_component(&system, &driver_component);
    start_component(&system, &bridge);
    start_component(&system, &listener_probe);
    start_component(&system, &tagged_probe);

    let bridge_handle = IoBridgeHandle::from_component(&bridge);
    let opened_listener = bridge_handle
        .open_tcp_listener(OpenTcpListener {
            local_addr: listener_lease.addr(0),
            incoming_to: listener_probe.actor_ref().recipient(),
        })
        .wait_timeout(WAIT_TIMEOUT)
        .expect("TCP listener open future")
        .expect("TCP listener open");
    let listener_addr = opened_listener.local_addr;
    listener_lease.release_binding(0);

    let mut client = std::net::TcpStream::connect(listener_addr).expect("connect TCP client");
    let pending = match recv_until(&listener_events_rx, |event| {
        matches!(event, TcpListenerEvent::Incoming { .. })
    }) {
        TcpListenerEvent::Incoming { pending, .. } => pending,
        other => unreachable!("filtered to TCP listener Incoming, got {other:?}"),
    };

    let session_ref = pending
        .accept_tagged(tagged_probe.actor_ref(), 7, wrap_tagged_session_event)
        .wait_timeout(WAIT_TIMEOUT)
        .expect("pending tagged TCP session accept future")
        .expect("pending tagged TCP session accept");

    client
        .write_all(b"hello")
        .expect("write tagged TCP session payload");

    let TaggedSessionEvent { tag, event } = recv_until(&tagged_events_rx, |event| {
        matches!(event.event, TcpSessionEvent::Received { .. })
    });
    assert_eq!(tag, 7);
    match event {
        TcpSessionEvent::Received { payload } => {
            assert_eq!(payload.to_vec().as_slice(), b"hello");
        }
        other => unreachable!("filtered to tagged TCP session Received, got {other:?}"),
    }

    session_ref.close(false);
    let TaggedSessionEvent { tag, event } = recv_until(&tagged_events_rx, |event| {
        matches!(event.event, TcpSessionEvent::Closed { .. })
    });
    assert_eq!(tag, 7);
    match event {
        TcpSessionEvent::Closed { reason } => {
            assert!(matches!(
                reason,
                CloseReason::Graceful | CloseReason::Aborted
            ));
        }
        other => unreachable!("filtered to tagged TCP session Closed, got {other:?}"),
    }

    opened_listener
        .listener
        .tell(super::TcpListenerRequest::Close);
    match recv_until(&listener_events_rx, |event| {
        matches!(event, TcpListenerEvent::Closed)
    }) {
        TcpListenerEvent::Closed => {}
        other => unreachable!("filtered to TCP listener Closed, got {other:?}"),
    }

    listener_lease
        .rebind_binding(0)
        .expect("rebind reserved TCP listener");
    drop(client);
    drop(session_ref);
    drop(opened_listener);
    drop(bridge_handle);
    kill_component(&system, tagged_probe);
    kill_component(&system, listener_probe);
    kill_component(&system, bridge);
    kill_component(&system, driver_component);
    system.shutdown().wait().expect("Kompact shutdown");
}

#[test]
#[allow(
    clippy::match_wildcard_for_single_variants,
    reason = "The receive helper filters to one event variant and the fallback keeps assertion diagnostics precise."
)]
fn dropping_pending_tcp_session_rejects_the_connection() {
    let mut listener_lease = reserve_sockets(&[ReservedSocketKind::TcpListener]);
    let system = build_test_kompact_system_with(enable_bind_reuse_address);
    let driver_component = system.create(|| IoDriverComponent::new(DriverConfig::default()));
    let driver_for_bridge = driver_component.clone();
    let bridge = system.create(move || IoBridge::new(&driver_for_bridge));
    let (listener_events_tx, listener_events_rx) = mpsc::channel();
    let listener_probe = system.create(move || TcpListenerEventProbe::new(listener_events_tx));

    start_component(&system, &driver_component);
    start_component(&system, &bridge);
    start_component(&system, &listener_probe);

    let bridge_handle = IoBridgeHandle::from_component(&bridge);
    let opened_listener = bridge_handle
        .open_tcp_listener(OpenTcpListener {
            local_addr: listener_lease.addr(0),
            incoming_to: listener_probe.actor_ref().recipient(),
        })
        .wait_timeout(WAIT_TIMEOUT)
        .expect("TCP listener open future")
        .expect("TCP listener open");
    let listener_addr = opened_listener.local_addr;
    listener_lease.release_binding(0);

    let mut client = std::net::TcpStream::connect(listener_addr).expect("connect TCP client");
    client
        .set_read_timeout(Some(Duration::from_millis(200)))
        .expect("set client read timeout");
    let pending = match recv_until(&listener_events_rx, |event| {
        matches!(event, TcpListenerEvent::Incoming { .. })
    }) {
        TcpListenerEvent::Incoming { pending, .. } => pending,
        other => unreachable!("filtered to TCP listener Incoming, got {other:?}"),
    };

    drop(pending);

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
        other => panic!("unexpected client read result after dropping pending session: {other:?}"),
    }

    opened_listener
        .listener
        .tell(super::TcpListenerRequest::Close);
    match recv_until(&listener_events_rx, |event| {
        matches!(event, TcpListenerEvent::Closed)
    }) {
        TcpListenerEvent::Closed => {}
        other => unreachable!("filtered to TCP listener Closed, got {other:?}"),
    }

    listener_lease
        .rebind_binding(0)
        .expect("rebind reserved TCP listener");
    drop(client);
    drop(opened_listener);
    drop(bridge_handle);
    kill_component(&system, listener_probe);
    kill_component(&system, bridge);
    kill_component(&system, driver_component);
    system.shutdown().wait().expect("Kompact shutdown");
}
