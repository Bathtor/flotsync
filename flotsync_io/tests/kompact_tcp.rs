use bytes::Bytes;
use flotsync_io::{
    prelude::*,
    test_support::{
        TcpListenerEventProbe,
        TcpSessionEventProbe,
        build_test_kompact_system,
        init_test_logger,
        kill_component,
        localhost,
        payload_bytes,
        recv_until,
        start_component,
    },
};
use kompact::prelude::*;
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    thread,
    time::Duration,
};

#[test]
fn tcp_bridge_routes_outbound_session_lifecycle_and_flow_control_events_to_the_owner_only() {
    init_test_logger();

    let listener = TcpListener::bind(localhost(0)).expect("bind TCP listener");
    let remote_addr = listener.local_addr().expect("listener addr");
    let (start_read_tx, start_read_rx) = mpsc::sync_channel(1);
    let server = thread::spawn(move || {
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
        stream.write_all(b"world").expect("write TCP response");
    });

    let system = build_test_kompact_system();
    let driver_component = system.create(|| IoDriverComponent::new(DriverConfig::default()));
    let driver_for_bridge = driver_component.clone();
    let bridge = system.create(move || IoBridge::new(&driver_for_bridge));
    let (session1_tx, session1_rx) = mpsc::channel();
    let session_probe1 = system.create(move || TcpSessionEventProbe::new(session1_tx));
    let (session2_tx, session2_rx) = mpsc::channel();
    let session_probe2 = system.create(move || TcpSessionEventProbe::new(session2_tx));

    start_component(&system, &driver_component);
    start_component(&system, &bridge);
    start_component(&system, &session_probe1);
    start_component(&system, &session_probe2);

    let bridge_handle = IoBridgeHandle::from_component(&bridge);
    let opened_session = bridge_handle
        .open_tcp_session(OpenTcpSession {
            remote_addr,
            local_addr: None,
            events_to: session_probe1.actor_ref().recipient(),
        })
        .wait_timeout(Duration::from_secs(2))
        .expect("open TCP session future")
        .expect("open TCP session");
    assert_eq!(opened_session.peer_addr, remote_addr);
    assert!(
        session2_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err(),
        "unrelated TCP session event probe must stay silent"
    );

    opened_session.session.tell(TcpSessionRequest::Send {
        transmission_id: TransmissionId(10),
        payload: IoPayload::Bytes(Bytes::from(vec![b'x'; 4 * 1024 * 1024])),
    });
    opened_session.session.tell(TcpSessionRequest::Send {
        transmission_id: TransmissionId(11),
        payload: IoPayload::Bytes(Bytes::from_static(b"second")),
    });

    let mut saw_write_suspended = false;
    let mut saw_backpressure_nack = false;
    while !saw_write_suspended || !saw_backpressure_nack {
        match recv_until(&session1_rx, |_| true) {
            TcpSessionEvent::WriteSuspended => {
                saw_write_suspended = true;
            }
            TcpSessionEvent::SendNack {
                transmission_id: TransmissionId(11),
                reason,
            } => {
                assert_eq!(reason, SendFailureReason::Backpressure);
                saw_backpressure_nack = true;
            }
            other => {
                log::debug!("ignoring unrelated outbound TCP session event: {:?}", other);
            }
        }
    }

    start_read_tx.send(()).expect("signal server to read");

    let mut saw_send_ack = false;
    let mut saw_write_resumed = false;
    let mut saw_received = false;
    while !saw_send_ack || !saw_write_resumed || !saw_received {
        match recv_until(&session1_rx, |_| true) {
            TcpSessionEvent::SendAck {
                transmission_id: TransmissionId(10),
            } => {
                saw_send_ack = true;
            }
            TcpSessionEvent::WriteResumed => {
                saw_write_resumed = true;
            }
            TcpSessionEvent::Received { payload } => {
                assert_eq!(payload_bytes(payload), Bytes::from_static(b"world"));
                saw_received = true;
            }
            other => {
                log::debug!("ignoring unrelated outbound TCP session event: {:?}", other);
            }
        }
    }

    opened_session
        .session
        .tell(TcpSessionRequest::Close { abort: false });
    let _ = recv_until(&session1_rx, |event| {
        matches!(event, TcpSessionEvent::Closed { .. })
    });
    assert!(
        session2_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err(),
        "unrelated TCP session probe must stay silent after close as well"
    );

    server.join().expect("join server thread");
    drop(opened_session);
    drop(bridge_handle);
    kill_component(&system, session_probe1);
    kill_component(&system, session_probe2);
    kill_component(&system, bridge);
    kill_component(&system, driver_component);
    system.shutdown().expect("Kompact shutdown");
}

#[test]
fn tcp_listener_accepts_and_rejects_pending_sessions_and_listener_close_keeps_accepted_session_alive()
 {
    init_test_logger();

    let system = build_test_kompact_system();
    let driver_component = system.create(|| IoDriverComponent::new(DriverConfig::default()));
    let driver_for_bridge = driver_component.clone();
    let bridge = system.create(move || IoBridge::new(&driver_for_bridge));
    let (listener_tx, listener_rx) = mpsc::channel();
    let listener_probe = system.create(move || TcpListenerEventProbe::new(listener_tx));
    let (session_tx, session_rx) = mpsc::channel();
    let session_probe = system.create(move || TcpSessionEventProbe::new(session_tx));

    start_component(&system, &driver_component);
    start_component(&system, &bridge);
    start_component(&system, &listener_probe);
    start_component(&system, &session_probe);

    let bridge_handle = IoBridgeHandle::from_component(&bridge);
    let opened_listener = bridge_handle
        .open_tcp_listener(OpenTcpListener {
            local_addr: localhost(0),
            incoming_to: listener_probe.actor_ref().recipient(),
        })
        .wait_timeout(Duration::from_secs(2))
        .expect("open TCP listener future")
        .expect("open TCP listener");
    let listener_addr = opened_listener.local_addr;

    let mut accepted_client = TcpStream::connect(listener_addr).expect("connect accepted client");
    accepted_client
        .write_all(b"hello")
        .expect("write pending accepted payload");
    let pending = match recv_until(&listener_rx, |event| {
        matches!(event, TcpListenerEvent::Incoming { .. })
    }) {
        TcpListenerEvent::Incoming { pending, .. } => pending,
        other => unreachable!("filtered to first TCP listener Incoming event, got {other:?}"),
    };

    let session_ref = pending
        .accept(session_probe.actor_ref().recipient())
        .wait_timeout(Duration::from_secs(2))
        .expect("accept pending TCP session future")
        .expect("accept pending TCP session");
    match recv_until(&session_rx, |event| {
        matches!(event, TcpSessionEvent::Received { .. })
    }) {
        TcpSessionEvent::Received { payload } => {
            assert_eq!(payload_bytes(payload), Bytes::from_static(b"hello"));
        }
        other => unreachable!("filtered to first accepted TCP session receive, got {other:?}"),
    }

    let mut rejected_client = TcpStream::connect(listener_addr).expect("connect rejected client");
    rejected_client
        .set_read_timeout(Some(Duration::from_millis(200)))
        .expect("set rejected client read timeout");
    let rejected_pending = match recv_until(&listener_rx, |event| {
        matches!(event, TcpListenerEvent::Incoming { .. })
    }) {
        TcpListenerEvent::Incoming { pending, .. } => pending,
        other => unreachable!("filtered to second TCP listener Incoming event, got {other:?}"),
    };
    rejected_pending
        .reject()
        .wait_timeout(Duration::from_secs(2))
        .expect("reject pending TCP session future")
        .expect("reject pending TCP session");

    let mut rejected_buf = [0_u8; 1];
    let rejected_read = rejected_client.read(&mut rejected_buf);
    match rejected_read {
        Ok(0) => {}
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::WouldBlock
                    | std::io::ErrorKind::TimedOut
            ) => {}
        other => panic!("unexpected rejected-client read result: {other:?}"),
    }

    opened_listener.listener.tell(TcpListenerRequest::Close);
    match recv_until(&listener_rx, |event| {
        matches!(event, TcpListenerEvent::Closed)
    }) {
        TcpListenerEvent::Closed => {}
        other => unreachable!("filtered to TCP listener Closed event, got {other:?}"),
    }

    accepted_client
        .write_all(b"again")
        .expect("write to accepted session after listener close");
    match recv_until(&session_rx, |event| {
        matches!(event, TcpSessionEvent::Received { .. })
    }) {
        TcpSessionEvent::Received { payload } => {
            assert_eq!(payload_bytes(payload), Bytes::from_static(b"again"));
        }
        other => unreachable!("filtered to post-close accepted TCP session receive, got {other:?}"),
    }

    session_ref.tell(TcpSessionRequest::Close { abort: false });
    let _ = recv_until(&session_rx, |event| {
        matches!(event, TcpSessionEvent::Closed { .. })
    });

    drop(session_ref);
    drop(opened_listener);
    drop(bridge_handle);
    kill_component(&system, session_probe);
    kill_component(&system, listener_probe);
    kill_component(&system, bridge);
    kill_component(&system, driver_component);
    system.shutdown().expect("Kompact shutdown");
}

#[test]
fn dropping_pending_tcp_session_auto_rejects_the_connection() {
    init_test_logger();

    let system = build_test_kompact_system();
    let driver_component = system.create(|| IoDriverComponent::new(DriverConfig::default()));
    let driver_for_bridge = driver_component.clone();
    let bridge = system.create(move || IoBridge::new(&driver_for_bridge));
    let (listener_tx, listener_rx) = mpsc::channel();
    let listener_probe = system.create(move || TcpListenerEventProbe::new(listener_tx));

    start_component(&system, &driver_component);
    start_component(&system, &bridge);
    start_component(&system, &listener_probe);

    let bridge_handle = IoBridgeHandle::from_component(&bridge);
    let opened_listener = bridge_handle
        .open_tcp_listener(OpenTcpListener {
            local_addr: localhost(0),
            incoming_to: listener_probe.actor_ref().recipient(),
        })
        .wait_timeout(Duration::from_secs(2))
        .expect("open TCP listener future")
        .expect("open TCP listener");
    let listener_addr = opened_listener.local_addr;

    let mut client = TcpStream::connect(listener_addr).expect("connect TCP client");
    client
        .set_read_timeout(Some(Duration::from_millis(200)))
        .expect("set TCP client read timeout");
    let pending = match recv_until(&listener_rx, |event| {
        matches!(event, TcpListenerEvent::Incoming { .. })
    }) {
        TcpListenerEvent::Incoming { pending, .. } => pending,
        other => unreachable!("filtered to TCP listener Incoming event, got {other:?}"),
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
                    | std::io::ErrorKind::WouldBlock
                    | std::io::ErrorKind::TimedOut
            ) => {}
        other => panic!("unexpected client read result after dropping pending session: {other:?}"),
    }

    opened_listener.listener.tell(TcpListenerRequest::Close);
    let _ = recv_until(&listener_rx, |event| {
        matches!(event, TcpListenerEvent::Closed)
    });

    drop(opened_listener);
    drop(bridge_handle);
    kill_component(&system, listener_probe);
    kill_component(&system, bridge);
    kill_component(&system, driver_component);
    system.shutdown().expect("Kompact shutdown");
}
