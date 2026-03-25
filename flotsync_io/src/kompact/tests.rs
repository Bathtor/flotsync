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
    UdpOpenRequestId,
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

#[derive(Clone, Debug)]
struct TaggedSessionEvent {
    tag: usize,
    event: TcpSessionEvent,
}

fn wrap_tagged_session_event(tag: usize, event: TcpSessionEvent) -> TaggedSessionEvent {
    TaggedSessionEvent { tag, event }
}

#[derive(ComponentDefinition)]
struct TaggedSessionEventProbe {
    ctx: ComponentContext<Self>,
    events: mpsc::Sender<TaggedSessionEvent>,
}

impl TaggedSessionEventProbe {
    fn new(events: mpsc::Sender<TaggedSessionEvent>) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            events,
        }
    }
}

ignore_lifecycle!(TaggedSessionEventProbe);

impl Actor for TaggedSessionEventProbe {
    type Message = TaggedSessionEvent;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        self.events
            .send(msg)
            .expect("tagged TCP session event receiver must stay live during integration tests");
        Handled::Ok
    }

    fn receive_network(&mut self, _msg: NetMessage) -> Handled {
        unimplemented!("tagged TCP session probe does not use network actor messages")
    }
}

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
    let receiver_request_id = UdpOpenRequestId::new();
    let sender_request_id = UdpOpenRequestId::new();

    observer1.on_definition(|component| {
        component.udp.trigger(UdpRequest::Bind {
            request_id: receiver_request_id,
            bind: UdpLocalBind::Exact(localhost(0)),
        });
    });
    let (receiver_id, receiver_addr) = match recv_until(&observer1_rx, |event| {
        matches!(
            event,
            UdpIndication::Bound {
                request_id,
                socket_id,
                ..
            } if *request_id == receiver_request_id
        )
    }) {
        UdpIndication::Bound {
            request_id,
            socket_id,
            local_addr,
        } => {
            assert_eq!(request_id, receiver_request_id);
            (socket_id, local_addr)
        }
        other => unreachable!("filtered to Bound for receiver socket, got {other:?}"),
    };
    match recv_until(&observer2_rx, |event| {
        matches!(
            event,
            UdpIndication::Bound {
                request_id,
                ..
            } if *request_id == receiver_request_id
        )
    }) {
        UdpIndication::Bound {
            request_id,
            socket_id,
            local_addr,
        } => {
            assert_eq!(request_id, receiver_request_id);
            assert_eq!(socket_id, receiver_id);
            assert_eq!(local_addr, receiver_addr);
        }
        other => unreachable!("filtered to Bound for receiver socket, got {other:?}"),
    }

    observer2.on_definition(|component| {
        component.udp.trigger(UdpRequest::Bind {
            request_id: sender_request_id,
            bind: UdpLocalBind::Exact(localhost(0)),
        });
    });
    let sender_id = match recv_until(&observer1_rx, |event| {
        matches!(
            event,
            UdpIndication::Bound {
                request_id,
                ..
            } if *request_id == sender_request_id
        )
    }) {
        UdpIndication::Bound {
            request_id,
            socket_id,
            ..
        } => {
            assert_eq!(request_id, sender_request_id);
            socket_id
        }
        other => unreachable!("filtered to Bound for sender socket, got {other:?}"),
    };
    recv_until(&observer2_rx, |event| {
        matches!(
            event,
            UdpIndication::Bound {
                request_id,
                ..
            } if *request_id == sender_request_id
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
    let request_id = UdpOpenRequestId::new();

    observer1.on_definition(|component| {
        component.udp.trigger(UdpRequest::Bind {
            request_id,
            bind: UdpLocalBind::Exact(localhost(0)),
        });
    });
    let socket_id = match recv_until(&observer1_rx, |event| {
        matches!(
            event,
            UdpIndication::Bound {
                request_id: observed_request_id,
                ..
            } if *observed_request_id == request_id
        )
    }) {
        UdpIndication::Bound {
            request_id: observed_request_id,
            socket_id,
            ..
        } => {
            assert_eq!(observed_request_id, request_id);
            socket_id
        }
        other => unreachable!("filtered to UDP Bound, got {other:?}"),
    };
    recv_until(&observer2_rx, |event| {
        matches!(
            event,
            UdpIndication::Bound {
                request_id: observed_request_id,
                ..
            } if *observed_request_id == request_id
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
    drop(opened_session);
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
    let opened_listener = bridge_handle
        .open_tcp_listener(OpenTcpListener {
            local_addr: localhost(0),
            incoming_to: listener_probe.actor_ref().recipient(),
        })
        .wait_timeout(WAIT_TIMEOUT)
        .expect("TCP listener open future")
        .expect("TCP listener open");
    let listener_addr = opened_listener.local_addr;

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

    opened_listener
        .listener
        .tell(super::TcpListenerRequest::Close);
    match recv_until(&listener_events_rx, |event| {
        matches!(event, TcpListenerEvent::Closed)
    }) {
        TcpListenerEvent::Closed => {}
        other => unreachable!("filtered to TCP listener Closed, got {other:?}"),
    }

    drop(client);
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
fn tcp_pending_session_accept_tagged_forwards_runtime_tagged_events() {
    init_test_logger();

    let system = build_test_kompact_system();
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
            local_addr: localhost(0),
            incoming_to: listener_probe.actor_ref().recipient(),
        })
        .wait_timeout(WAIT_TIMEOUT)
        .expect("TCP listener open future")
        .expect("TCP listener open");
    let listener_addr = opened_listener.local_addr;

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
            assert_eq!(payload_bytes(payload), Bytes::from_static(b"hello"));
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

    drop(client);
    drop(session_ref);
    drop(opened_listener);
    drop(bridge_handle);
    kill_component(&system, tagged_probe);
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
    let opened_listener = bridge_handle
        .open_tcp_listener(OpenTcpListener {
            local_addr: localhost(0),
            incoming_to: listener_probe.actor_ref().recipient(),
        })
        .wait_timeout(WAIT_TIMEOUT)
        .expect("TCP listener open future")
        .expect("TCP listener open");
    let listener_addr = opened_listener.local_addr;

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

    drop(client);
    drop(opened_listener);
    drop(bridge_handle);
    kill_component(&system, listener_probe);
    kill_component(&system, bridge);
    kill_component(&system, driver_component);
    system.shutdown().expect("Kompact shutdown");
}
