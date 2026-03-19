mod support;

use bytes::Bytes;
use flotsync_io::prelude::*;
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    thread,
    time::Duration,
};
use support::{
    assert_no_driver_event,
    init_test_logger,
    localhost,
    payload_bytes,
    wait_for_driver_event,
    wait_for_driver_request,
};

fn reserve_connection(driver: &IoDriver) -> ConnectionId {
    let request = driver
        .reserve_connection()
        .expect("enqueue connection reservation");
    wait_for_driver_request(request)
}

fn reserve_listener(driver: &IoDriver) -> ListenerId {
    let request = driver
        .reserve_listener()
        .expect("enqueue listener reservation");
    wait_for_driver_request(request)
}

fn listen_at(driver: &IoDriver, listener_id: ListenerId) -> std::net::SocketAddr {
    driver
        .dispatch(DriverCommand::Tcp(TcpCommand::Listen {
            listener_id,
            local_addr: localhost(0),
        }))
        .expect("dispatch TCP listen");

    match wait_for_driver_event(driver, |event| {
        matches!(
            event,
            DriverEvent::Tcp(TcpEvent::Listening {
                listener_id: observed_listener_id,
                ..
            }) if *observed_listener_id == listener_id
        )
    }) {
        DriverEvent::Tcp(TcpEvent::Listening {
            listener_id: observed_listener_id,
            local_addr,
        }) => {
            assert_eq!(observed_listener_id, listener_id);
            local_addr
        }
        other => unreachable!("filtered to TCP Listening event, got {other:?}"),
    }
}

#[test]
fn tcp_driver_reports_connect_failure() {
    init_test_logger();

    let probe_listener = TcpListener::bind(localhost(0)).expect("bind probe TCP listener");
    let remote_addr = probe_listener.local_addr().expect("probe listener addr");
    drop(probe_listener);

    let driver = IoDriver::start(DriverConfig::default()).expect("driver starts");
    let connection_id = reserve_connection(&driver);
    driver
        .dispatch(DriverCommand::Tcp(TcpCommand::Connect {
            connection_id,
            local_addr: None,
            remote_addr,
        }))
        .expect("dispatch failing TCP connect");

    match wait_for_driver_event(&driver, |event| {
        matches!(
            event,
            DriverEvent::Tcp(TcpEvent::ConnectFailed {
                connection_id: observed_connection_id,
                ..
            }) if *observed_connection_id == connection_id
        )
    }) {
        DriverEvent::Tcp(TcpEvent::ConnectFailed {
            connection_id: observed_connection_id,
            remote_addr: observed_remote_addr,
            ..
        }) => {
            assert_eq!(observed_connection_id, connection_id);
            assert_eq!(observed_remote_addr, remote_addr);
        }
        other => unreachable!("filtered to TCP ConnectFailed event, got {other:?}"),
    }

    driver.shutdown().expect("driver shuts down");
}

#[test]
fn tcp_driver_outbound_session_lifecycle_handles_remote_close_and_abortive_close() {
    init_test_logger();

    let listener = TcpListener::bind(localhost(0)).expect("bind TCP listener");
    let remote_addr = listener.local_addr().expect("listener addr");
    let (server_payload_tx, server_payload_rx) = mpsc::sync_channel(1);
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept TCP stream");
        let mut request = [0_u8; 5];
        stream.read_exact(&mut request).expect("read TCP request");
        server_payload_tx
            .send(request)
            .expect("send request payload");
        stream.write_all(b"world").expect("write TCP response");
    });

    let driver = IoDriver::start(DriverConfig::default()).expect("driver starts");
    let connection_id = reserve_connection(&driver);
    driver
        .dispatch(DriverCommand::Tcp(TcpCommand::Connect {
            connection_id,
            local_addr: None,
            remote_addr,
        }))
        .expect("dispatch TCP connect");

    match wait_for_driver_event(&driver, |event| {
        matches!(
            event,
            DriverEvent::Tcp(TcpEvent::Connected {
                connection_id: observed_connection_id,
                ..
            }) if *observed_connection_id == connection_id
        )
    }) {
        DriverEvent::Tcp(TcpEvent::Connected {
            connection_id: observed_connection_id,
            peer_addr,
        }) => {
            assert_eq!(observed_connection_id, connection_id);
            assert_eq!(peer_addr, remote_addr);
        }
        other => unreachable!("filtered to TCP Connected event, got {other:?}"),
    }

    driver
        .dispatch(DriverCommand::Tcp(TcpCommand::Send {
            connection_id,
            transmission_id: TransmissionId(1),
            payload: IoPayload::Bytes(Bytes::from_static(b"hello")),
        }))
        .expect("dispatch TCP send");

    let mut saw_ack = false;
    let mut saw_receive = false;
    let mut saw_closed = false;
    while !saw_ack || !saw_receive || !saw_closed {
        match wait_for_driver_event(&driver, |_| true) {
            DriverEvent::Tcp(TcpEvent::SendAck {
                connection_id: observed_connection_id,
                transmission_id,
            }) if observed_connection_id == connection_id
                && transmission_id == TransmissionId(1) =>
            {
                saw_ack = true;
            }
            DriverEvent::Tcp(TcpEvent::Received {
                connection_id: observed_connection_id,
                payload,
            }) if observed_connection_id == connection_id => {
                assert_eq!(payload_bytes(payload), Bytes::from_static(b"world"));
                saw_receive = true;
            }
            DriverEvent::Tcp(TcpEvent::Closed {
                connection_id: observed_connection_id,
                reason,
            }) if observed_connection_id == connection_id => {
                assert_eq!(reason, CloseReason::Graceful);
                saw_closed = true;
            }
            other => {
                log::debug!("ignoring unrelated raw TCP event: {:?}", other);
            }
        }
    }

    assert_eq!(server_payload_rx.recv().expect("server payload"), *b"hello");
    server.join().expect("join server thread");

    let listener = TcpListener::bind(localhost(0)).expect("bind second TCP listener");
    let remote_addr = listener.local_addr().expect("second listener addr");
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept second TCP stream");
        stream
            .set_read_timeout(Some(Duration::from_millis(500)))
            .expect("set read timeout");
        let mut buf = [0_u8; 1];
        let _ = stream.read(&mut buf);
    });

    let abort_connection_id = reserve_connection(&driver);
    driver
        .dispatch(DriverCommand::Tcp(TcpCommand::Connect {
            connection_id: abort_connection_id,
            local_addr: None,
            remote_addr,
        }))
        .expect("dispatch TCP connect for abort close");
    let _ = wait_for_driver_event(&driver, |event| {
        matches!(
            event,
            DriverEvent::Tcp(TcpEvent::Connected {
                connection_id: observed_connection_id,
                ..
            }) if *observed_connection_id == abort_connection_id
        )
    });

    driver
        .dispatch(DriverCommand::Tcp(TcpCommand::Close {
            connection_id: abort_connection_id,
            abort: true,
        }))
        .expect("dispatch abortive TCP close");
    match wait_for_driver_event(&driver, |event| {
        matches!(
            event,
            DriverEvent::Tcp(TcpEvent::Closed {
                connection_id: observed_connection_id,
                ..
            }) if *observed_connection_id == abort_connection_id
        )
    }) {
        DriverEvent::Tcp(TcpEvent::Closed {
            connection_id: observed_connection_id,
            reason,
        }) => {
            assert_eq!(observed_connection_id, abort_connection_id);
            assert_eq!(reason, CloseReason::Aborted);
        }
        other => unreachable!("filtered to abortive TCP Closed event, got {other:?}"),
    }

    server.join().expect("join abort-close server thread");
    driver.shutdown().expect("driver shuts down");
}

#[test]
fn tcp_driver_listener_requires_adoption_rejects_pending_connections_and_keeps_adopted_sessions_alive()
 {
    init_test_logger();

    let driver = IoDriver::start(DriverConfig::default()).expect("driver starts");
    let listener_id = reserve_listener(&driver);
    let listener_addr = listen_at(&driver, listener_id);

    let mut accepted_client = TcpStream::connect(listener_addr).expect("connect accepted client");
    let accepted_connection_id = match wait_for_driver_event(&driver, |event| {
        matches!(
            event,
            DriverEvent::Tcp(TcpEvent::Accepted {
                listener_id: observed_listener_id,
                ..
            }) if *observed_listener_id == listener_id
        )
    }) {
        DriverEvent::Tcp(TcpEvent::Accepted {
            listener_id: observed_listener_id,
            connection_id,
            ..
        }) => {
            assert_eq!(observed_listener_id, listener_id);
            connection_id
        }
        other => unreachable!("filtered to first TCP Accepted event, got {other:?}"),
    };

    accepted_client
        .write_all(b"pending")
        .expect("write pending TCP payload");
    assert_no_driver_event(&driver, Duration::from_millis(50));

    driver
        .dispatch(DriverCommand::Tcp(TcpCommand::AdoptAccepted {
            connection_id: accepted_connection_id,
        }))
        .expect("dispatch adopt accepted connection");
    match wait_for_driver_event(&driver, |event| {
        matches!(
            event,
            DriverEvent::Tcp(TcpEvent::Received {
                connection_id: observed_connection_id,
                ..
            }) if *observed_connection_id == accepted_connection_id
        )
    }) {
        DriverEvent::Tcp(TcpEvent::Received {
            connection_id: observed_connection_id,
            payload,
        }) => {
            assert_eq!(observed_connection_id, accepted_connection_id);
            assert_eq!(payload_bytes(payload), Bytes::from_static(b"pending"));
        }
        other => unreachable!("filtered to adopted TCP receive event, got {other:?}"),
    }

    let mut rejected_client = TcpStream::connect(listener_addr).expect("connect rejected client");
    rejected_client
        .set_read_timeout(Some(Duration::from_millis(200)))
        .expect("set rejected client read timeout");
    let rejected_connection_id = match wait_for_driver_event(&driver, |event| {
        matches!(
            event,
            DriverEvent::Tcp(TcpEvent::Accepted {
                listener_id: observed_listener_id,
                connection_id,
                ..
            }) if *observed_listener_id == listener_id && *connection_id != accepted_connection_id
        )
    }) {
        DriverEvent::Tcp(TcpEvent::Accepted { connection_id, .. }) => connection_id,
        other => unreachable!("filtered to second TCP Accepted event, got {other:?}"),
    };

    driver
        .dispatch(DriverCommand::Tcp(TcpCommand::RejectAccepted {
            connection_id: rejected_connection_id,
        }))
        .expect("dispatch reject accepted connection");

    thread::sleep(Duration::from_millis(50));
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

    driver
        .dispatch(DriverCommand::Tcp(TcpCommand::CloseListener {
            listener_id,
        }))
        .expect("dispatch TCP listener close");
    match wait_for_driver_event(&driver, |event| {
        matches!(
            event,
            DriverEvent::Tcp(TcpEvent::ListenerClosed {
                listener_id: observed_listener_id,
            }) if *observed_listener_id == listener_id
        )
    }) {
        DriverEvent::Tcp(TcpEvent::ListenerClosed {
            listener_id: observed_listener_id,
        }) => {
            assert_eq!(observed_listener_id, listener_id);
        }
        other => unreachable!("filtered to TCP ListenerClosed event, got {other:?}"),
    }

    accepted_client
        .write_all(b"again")
        .expect("write on adopted connection after listener close");
    match wait_for_driver_event(&driver, |event| {
        matches!(
            event,
            DriverEvent::Tcp(TcpEvent::Received {
                connection_id: observed_connection_id,
                ..
            }) if *observed_connection_id == accepted_connection_id
        )
    }) {
        DriverEvent::Tcp(TcpEvent::Received {
            connection_id: observed_connection_id,
            payload,
        }) => {
            assert_eq!(observed_connection_id, accepted_connection_id);
            assert_eq!(payload_bytes(payload), Bytes::from_static(b"again"));
        }
        other => unreachable!("filtered to post-listener-close TCP receive, got {other:?}"),
    }

    drop(accepted_client);
    match wait_for_driver_event(&driver, |event| {
        matches!(
            event,
            DriverEvent::Tcp(TcpEvent::Closed {
                connection_id: observed_connection_id,
                ..
            }) if *observed_connection_id == accepted_connection_id
        )
    }) {
        DriverEvent::Tcp(TcpEvent::Closed {
            connection_id: observed_connection_id,
            reason,
        }) => {
            assert_eq!(observed_connection_id, accepted_connection_id);
            assert_eq!(reason, CloseReason::Graceful);
        }
        other => unreachable!("filtered to accepted-connection Closed event, got {other:?}"),
    }

    driver.shutdown().expect("driver shuts down");
}

#[test]
fn tcp_driver_reports_read_and_write_flow_control_transitions() {
    init_test_logger();

    let listener = TcpListener::bind(localhost(0)).expect("bind flow-control TCP listener");
    let remote_addr = listener.local_addr().expect("flow-control listener addr");
    let (start_read_tx, start_read_rx) = mpsc::sync_channel(1);
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept flow-control TCP stream");
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
        stream
            .write_all(&vec![b'a'; 300])
            .expect("write TCP ingress payload");
        thread::sleep(Duration::from_millis(50));
    });

    let driver = IoDriver::start(DriverConfig {
        buffer_config: IoBufferConfig {
            ingress: IoPoolConfig {
                chunk_size: 128,
                initial_chunk_count: 1,
                max_chunk_count: 2,
                encode_buf_min_free_space: 64,
            },
            egress: IoPoolConfig::default(),
        },
        ..DriverConfig::default()
    })
    .expect("driver starts");
    let connection_id = reserve_connection(&driver);
    driver
        .dispatch(DriverCommand::Tcp(TcpCommand::Connect {
            connection_id,
            local_addr: None,
            remote_addr,
        }))
        .expect("dispatch TCP connect for flow-control test");
    let _ = wait_for_driver_event(&driver, |event| {
        matches!(
            event,
            DriverEvent::Tcp(TcpEvent::Connected {
                connection_id: observed_connection_id,
                ..
            }) if *observed_connection_id == connection_id
        )
    });

    driver
        .dispatch(DriverCommand::Tcp(TcpCommand::Send {
            connection_id,
            transmission_id: TransmissionId(10),
            payload: IoPayload::Bytes(Bytes::from(vec![b'x'; 4 * 1024 * 1024])),
        }))
        .expect("dispatch large TCP send");
    driver
        .dispatch(DriverCommand::Tcp(TcpCommand::Send {
            connection_id,
            transmission_id: TransmissionId(11),
            payload: IoPayload::Bytes(Bytes::from_static(b"second")),
        }))
        .expect("dispatch second TCP send");

    let mut saw_write_suspended = false;
    let mut saw_backpressure_nack = false;
    while !saw_write_suspended || !saw_backpressure_nack {
        match wait_for_driver_event(&driver, |_| true) {
            DriverEvent::Tcp(TcpEvent::WriteSuspended {
                connection_id: observed_connection_id,
            }) if observed_connection_id == connection_id => {
                saw_write_suspended = true;
            }
            DriverEvent::Tcp(TcpEvent::SendNack {
                connection_id: observed_connection_id,
                transmission_id,
                reason,
            }) if observed_connection_id == connection_id
                && transmission_id == TransmissionId(11) =>
            {
                assert_eq!(reason, SendFailureReason::Backpressure);
                saw_backpressure_nack = true;
            }
            other => {
                log::debug!("ignoring unrelated TCP flow-control event: {:?}", other);
            }
        }
    }

    start_read_tx.send(()).expect("signal server to read");

    let mut saw_send_ack = false;
    let mut saw_write_resumed = false;
    while !saw_send_ack || !saw_write_resumed {
        match wait_for_driver_event(&driver, |_| true) {
            DriverEvent::Tcp(TcpEvent::SendAck {
                connection_id: observed_connection_id,
                transmission_id,
            }) if observed_connection_id == connection_id
                && transmission_id == TransmissionId(10) =>
            {
                saw_send_ack = true;
            }
            DriverEvent::Tcp(TcpEvent::WriteResumed {
                connection_id: observed_connection_id,
            }) if observed_connection_id == connection_id => {
                saw_write_resumed = true;
            }
            other => {
                log::debug!("ignoring unrelated TCP flow-control event: {:?}", other);
            }
        }
    }

    let first_lease = match wait_for_driver_event(&driver, |event| {
        matches!(
            event,
            DriverEvent::Tcp(TcpEvent::Received {
                connection_id: observed_connection_id,
                payload: IoPayload::Lease(_),
            }) if *observed_connection_id == connection_id
        )
    }) {
        DriverEvent::Tcp(TcpEvent::Received {
            payload: IoPayload::Lease(lease),
            ..
        }) => lease,
        other => unreachable!("filtered to first TCP lease-backed receive, got {other:?}"),
    };
    let second_lease = match wait_for_driver_event(&driver, |event| {
        matches!(
            event,
            DriverEvent::Tcp(TcpEvent::Received {
                connection_id: observed_connection_id,
                payload: IoPayload::Lease(_),
            }) if *observed_connection_id == connection_id
        )
    }) {
        DriverEvent::Tcp(TcpEvent::Received {
            payload: IoPayload::Lease(lease),
            ..
        }) => lease,
        other => unreachable!("filtered to second TCP lease-backed receive, got {other:?}"),
    };

    let _ = wait_for_driver_event(&driver, |event| {
        matches!(
            event,
            DriverEvent::Tcp(TcpEvent::ReadSuspended {
                connection_id: observed_connection_id,
            }) if *observed_connection_id == connection_id
        )
    });

    drop(first_lease);
    drop(second_lease);

    let mut saw_read_resumed = false;
    let mut saw_third_payload = false;
    while !saw_read_resumed || !saw_third_payload {
        match wait_for_driver_event(&driver, |_| true) {
            DriverEvent::Tcp(TcpEvent::ReadResumed {
                connection_id: observed_connection_id,
            }) if observed_connection_id == connection_id => {
                saw_read_resumed = true;
            }
            DriverEvent::Tcp(TcpEvent::Received {
                connection_id: observed_connection_id,
                payload,
            }) if observed_connection_id == connection_id => {
                assert_eq!(payload_bytes(payload).len(), 44);
                saw_third_payload = true;
            }
            other => {
                log::debug!("ignoring unrelated TCP flow-control event: {:?}", other);
            }
        }
    }

    server.join().expect("join flow-control server thread");
    driver.shutdown().expect("driver shuts down");
}
