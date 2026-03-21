use super::{
    IoBridge,
    IoBridgeHandle,
    IoDriverComponent,
    OpenTcpListener,
    OpenTcpSession,
    TcpListenerEvent,
    TcpSessionEvent,
    TcpSessionRequest,
    UdpIndication,
    UdpPort,
    UdpRequest,
    UdpSendResult,
};
use crate::{
    api::{CloseReason, IoPayload, TransmissionId, UdpLocalBind, UdpSocketOption},
    driver::DriverConfig,
    test_support::{
        TcpListenerEventProbe,
        TcpSessionEventProbe,
        UdpObserver,
        UdpSendResultProbe,
        WAIT_TIMEOUT,
        build_test_kompact_system,
        init_test_logger,
        kill_component,
        localhost,
        payload_bytes,
        recv_until,
        start_component,
    },
};
use ::kompact::prelude::*;
use bytes::Bytes;
use std::{
    io::{Read, Write},
    net::{Ipv4Addr, TcpListener},
    sync::mpsc,
    thread,
    time::Duration,
};

#[test]
fn udp_bridge_broadcasts_socket_activity_but_send_results_stay_private() {
    init_test_logger();

    let system = build_test_kompact_system();
    let driver_component = system.create(|| IoDriverComponent::new(DriverConfig::default()));
    let driver_for_bridge = driver_component.clone();
    let bridge = system.create(move || IoBridge::new(&driver_for_bridge));

    let (observer1_tx, observer1_rx) = mpsc::channel();
    let observer1 = system.create(move || UdpObserver::new(observer1_tx));
    let (observer2_tx, observer2_rx) = mpsc::channel();
    let observer2 = system.create(move || UdpObserver::new(observer2_tx));
    let (reply1_tx, reply1_rx) = mpsc::channel();
    let reply1 = system.create(move || UdpSendResultProbe::new(reply1_tx));
    let (reply2_tx, reply2_rx) = mpsc::channel();
    let reply2 = system.create(move || UdpSendResultProbe::new(reply2_tx));

    let _bridge_to_observer1 = biconnect_components::<UdpPort, _, _>(&bridge, &observer1)
        .expect("bridge/observer1 connection");
    let _bridge_to_observer2 = biconnect_components::<UdpPort, _, _>(&bridge, &observer2)
        .expect("bridge/observer2 connection");

    start_component(&system, &driver_component);
    start_component(&system, &bridge);
    start_component(&system, &observer1);
    start_component(&system, &observer2);
    start_component(&system, &reply1);
    start_component(&system, &reply2);

    let bridge_handle = IoBridgeHandle::from_component(&bridge);
    let receiver_id = bridge_handle
        .reserve_udp_socket()
        .wait_timeout(WAIT_TIMEOUT)
        .expect("receiver reservation future")
        .expect("receiver reservation");
    let sender_id = bridge_handle
        .reserve_udp_socket()
        .wait_timeout(WAIT_TIMEOUT)
        .expect("sender reservation future")
        .expect("sender reservation");

    observer1.on_definition(|component| {
        component.udp.trigger(UdpRequest::Bind {
            socket_id: receiver_id,
            bind: UdpLocalBind::Exact(localhost(0)),
        });
    });
    let receiver_addr = match recv_until(&observer1_rx, |event| {
        matches!(
            event,
            UdpIndication::Bound {
                socket_id,
                ..
            } if *socket_id == receiver_id
        )
    }) {
        UdpIndication::Bound {
            socket_id,
            local_addr,
        } => {
            assert_eq!(socket_id, receiver_id);
            local_addr
        }
        other => unreachable!("filtered to Bound for receiver socket, got {other:?}"),
    };
    match recv_until(&observer2_rx, |event| {
        matches!(
            event,
            UdpIndication::Bound {
                socket_id,
                ..
            } if *socket_id == receiver_id
        )
    }) {
        UdpIndication::Bound {
            socket_id,
            local_addr,
        } => {
            assert_eq!(socket_id, receiver_id);
            assert_eq!(local_addr, receiver_addr);
        }
        other => unreachable!("filtered to Bound for receiver socket, got {other:?}"),
    }

    observer2.on_definition(|component| {
        component.udp.trigger(UdpRequest::Bind {
            socket_id: sender_id,
            bind: UdpLocalBind::Exact(localhost(0)),
        });
    });
    recv_until(&observer1_rx, |event| {
        matches!(
            event,
            UdpIndication::Bound {
                socket_id,
                ..
            } if *socket_id == sender_id
        )
    });
    recv_until(&observer2_rx, |event| {
        matches!(
            event,
            UdpIndication::Bound {
                socket_id,
                ..
            } if *socket_id == sender_id
        )
    });

    observer1.on_definition(|component| {
        component.udp.trigger(UdpRequest::Send {
            socket_id: sender_id,
            transmission_id: TransmissionId(1),
            payload: IoPayload::Bytes(Bytes::from_static(b"hello")),
            target: Some(receiver_addr),
            reply_to: reply1.actor_ref().recipient(),
        });
    });

    match recv_until(&reply1_rx, |result| {
        matches!(
            result,
            UdpSendResult::Ack {
                socket_id,
                transmission_id,
            } if *socket_id == sender_id && *transmission_id == TransmissionId(1)
        )
    }) {
        UdpSendResult::Ack {
            socket_id,
            transmission_id,
        } => {
            assert_eq!(socket_id, sender_id);
            assert_eq!(transmission_id, TransmissionId(1));
        }
        other => unreachable!("filtered to UDP send ack, got {other:?}"),
    }
    assert!(
        reply2_rx.recv_timeout(Duration::from_millis(200)).is_err(),
        "unused UDP reply recipient must stay silent"
    );

    match recv_until(&observer1_rx, |event| {
        matches!(
            event,
            UdpIndication::Received {
                socket_id,
                ..
            } if *socket_id == receiver_id
        )
    }) {
        UdpIndication::Received {
            socket_id, payload, ..
        } => {
            assert_eq!(socket_id, receiver_id);
            assert_eq!(payload_bytes(payload), Bytes::from_static(b"hello"));
        }
        other => unreachable!("filtered to UDP receive, got {other:?}"),
    }
    match recv_until(&observer2_rx, |event| {
        matches!(
            event,
            UdpIndication::Received {
                socket_id,
                ..
            } if *socket_id == receiver_id
        )
    }) {
        UdpIndication::Received {
            socket_id, payload, ..
        } => {
            assert_eq!(socket_id, receiver_id);
            assert_eq!(payload_bytes(payload), Bytes::from_static(b"hello"));
        }
        other => unreachable!("filtered to UDP receive, got {other:?}"),
    }

    drop(bridge_handle);
    drop(_bridge_to_observer1);
    drop(_bridge_to_observer2);
    kill_component(&system, reply1);
    kill_component(&system, reply2);
    kill_component(&system, observer1);
    kill_component(&system, observer2);
    kill_component(&system, bridge);
    kill_component(&system, driver_component);

    system.shutdown().expect("Kompact shutdown");
}

#[test]
fn udp_bridge_broadcasts_socket_configuration_indications() {
    init_test_logger();

    let system = build_test_kompact_system();
    let driver_component = system.create(|| IoDriverComponent::new(DriverConfig::default()));
    let driver_for_bridge = driver_component.clone();
    let bridge = system.create(move || IoBridge::new(&driver_for_bridge));

    let (observer1_tx, observer1_rx) = mpsc::channel();
    let observer1 = system.create(move || UdpObserver::new(observer1_tx));
    let (observer2_tx, observer2_rx) = mpsc::channel();
    let observer2 = system.create(move || UdpObserver::new(observer2_tx));

    let _bridge_to_observer1 = biconnect_components::<UdpPort, _, _>(&bridge, &observer1)
        .expect("bridge/observer1 connection");
    let _bridge_to_observer2 = biconnect_components::<UdpPort, _, _>(&bridge, &observer2)
        .expect("bridge/observer2 connection");

    start_component(&system, &driver_component);
    start_component(&system, &bridge);
    start_component(&system, &observer1);
    start_component(&system, &observer2);

    let bridge_handle = IoBridgeHandle::from_component(&bridge);
    let socket_id = bridge_handle
        .reserve_udp_socket()
        .wait_timeout(WAIT_TIMEOUT)
        .expect("socket reservation future")
        .expect("socket reservation");

    observer1.on_definition(|component| {
        component.udp.trigger(UdpRequest::Bind {
            socket_id,
            bind: UdpLocalBind::Exact(localhost(0)),
        });
    });
    recv_until(&observer1_rx, |event| {
        matches!(
            event,
            UdpIndication::Bound {
                socket_id: observed_socket_id,
                ..
            } if *observed_socket_id == socket_id
        )
    });
    recv_until(&observer2_rx, |event| {
        matches!(
            event,
            UdpIndication::Bound {
                socket_id: observed_socket_id,
                ..
            } if *observed_socket_id == socket_id
        )
    });

    let option = UdpSocketOption::Broadcast(true);
    observer1.on_definition(|component| {
        component
            .udp
            .trigger(UdpRequest::Configure { socket_id, option });
    });

    match recv_until(&observer1_rx, |event| {
        matches!(
            event,
            UdpIndication::Configured {
                socket_id: observed_socket_id,
                option: observed_option,
            } if *observed_socket_id == socket_id && *observed_option == option
        )
    }) {
        UdpIndication::Configured {
            socket_id: observed_socket_id,
            option: observed_option,
        } => {
            assert_eq!(observed_socket_id, socket_id);
            assert_eq!(observed_option, option);
        }
        other => unreachable!("filtered to UDP Configured, got {other:?}"),
    }
    match recv_until(&observer2_rx, |event| {
        matches!(
            event,
            UdpIndication::Configured {
                socket_id: observed_socket_id,
                option: observed_option,
            } if *observed_socket_id == socket_id && *observed_option == option
        )
    }) {
        UdpIndication::Configured {
            socket_id: observed_socket_id,
            option: observed_option,
        } => {
            assert_eq!(observed_socket_id, socket_id);
            assert_eq!(observed_option, option);
        }
        other => unreachable!("filtered to UDP Configured, got {other:?}"),
    }

    drop(bridge_handle);
    drop(_bridge_to_observer1);
    drop(_bridge_to_observer2);
    kill_component(&system, observer1);
    kill_component(&system, observer2);
    kill_component(&system, bridge);
    kill_component(&system, driver_component);

    system.shutdown().expect("Kompact shutdown");
}

#[test]
fn tcp_bridge_opens_sessions_and_routes_events_to_the_session_recipient() {
    init_test_logger();

    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind TCP listener");
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
    let session_ref = bridge_handle
        .open_tcp_session(OpenTcpSession {
            remote_addr,
            local_addr: None,
            events_to: event_probe.actor_ref().recipient(),
        })
        .wait_timeout(WAIT_TIMEOUT)
        .expect("TCP open future")
        .expect("TCP session open");

    match recv_until(&events_rx, |event| {
        matches!(event, TcpSessionEvent::Connected { .. })
    }) {
        TcpSessionEvent::Connected { peer_addr } => {
            assert_eq!(peer_addr, remote_addr);
        }
        other => unreachable!("filtered to TCP Connected, got {other:?}"),
    }

    session_ref.tell(TcpSessionRequest::Send {
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
                assert_eq!(payload_bytes(payload), Bytes::from_static(b"world"));
                saw_received = true;
            }
            TcpSessionEvent::Closed { reason } => {
                assert!(matches!(
                    reason,
                    CloseReason::Graceful | CloseReason::Aborted
                ));
            }
            other => {
                log::debug!("ignoring unrelated TCP session event in test: {:?}", other);
            }
        }
    }

    session_ref.tell(TcpSessionRequest::Close { abort: false });
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
    drop(session_ref);
    drop(bridge_handle);
    kill_component(&system, event_probe);
    kill_component(&system, bridge);
    kill_component(&system, driver_component);
    system.shutdown().expect("Kompact shutdown");
}

#[test]
fn tcp_listener_exposes_pending_sessions_before_session_io_begins() {
    init_test_logger();

    let system = build_test_kompact_system();
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
    let listener_ref = bridge_handle
        .open_tcp_listener(OpenTcpListener {
            local_addr: localhost(0),
            incoming_to: listener_probe.actor_ref().recipient(),
        })
        .wait_timeout(WAIT_TIMEOUT)
        .expect("TCP listener open future")
        .expect("TCP listener open");

    let listener_addr = match recv_until(&listener_events_rx, |event| {
        matches!(event, TcpListenerEvent::Listening { .. })
    }) {
        TcpListenerEvent::Listening { local_addr } => local_addr,
        other => unreachable!("filtered to TCP listener Listening, got {other:?}"),
    };

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
            assert_eq!(payload_bytes(payload), Bytes::from_static(b"hello"));
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

    listener_ref.tell(super::TcpListenerRequest::Close);
    match recv_until(&listener_events_rx, |event| {
        matches!(event, TcpListenerEvent::Closed)
    }) {
        TcpListenerEvent::Closed => {}
        other => unreachable!("filtered to TCP listener Closed, got {other:?}"),
    }

    drop(client);
    drop(session_ref);
    drop(listener_ref);
    drop(bridge_handle);
    kill_component(&system, session_probe);
    kill_component(&system, listener_probe);
    kill_component(&system, bridge);
    kill_component(&system, driver_component);
    system.shutdown().expect("Kompact shutdown");
}

#[test]
fn dropping_pending_tcp_session_rejects_the_connection() {
    init_test_logger();

    let system = build_test_kompact_system();
    let driver_component = system.create(|| IoDriverComponent::new(DriverConfig::default()));
    let driver_for_bridge = driver_component.clone();
    let bridge = system.create(move || IoBridge::new(&driver_for_bridge));
    let (listener_events_tx, listener_events_rx) = mpsc::channel();
    let listener_probe = system.create(move || TcpListenerEventProbe::new(listener_events_tx));

    start_component(&system, &driver_component);
    start_component(&system, &bridge);
    start_component(&system, &listener_probe);

    let bridge_handle = IoBridgeHandle::from_component(&bridge);
    let listener_ref = bridge_handle
        .open_tcp_listener(OpenTcpListener {
            local_addr: localhost(0),
            incoming_to: listener_probe.actor_ref().recipient(),
        })
        .wait_timeout(WAIT_TIMEOUT)
        .expect("TCP listener open future")
        .expect("TCP listener open");

    let listener_addr = match recv_until(&listener_events_rx, |event| {
        matches!(event, TcpListenerEvent::Listening { .. })
    }) {
        TcpListenerEvent::Listening { local_addr } => local_addr,
        other => unreachable!("filtered to TCP listener Listening, got {other:?}"),
    };

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

    listener_ref.tell(super::TcpListenerRequest::Close);
    match recv_until(&listener_events_rx, |event| {
        matches!(event, TcpListenerEvent::Closed)
    }) {
        TcpListenerEvent::Closed => {}
        other => unreachable!("filtered to TCP listener Closed, got {other:?}"),
    }

    drop(client);
    drop(listener_ref);
    drop(bridge_handle);
    kill_component(&system, listener_probe);
    kill_component(&system, bridge);
    kill_component(&system, driver_component);
    system.shutdown().expect("Kompact shutdown");
}
