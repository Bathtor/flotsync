use super::{
    IoBridge,
    IoBridgeHandle,
    IoDriverComponent,
    OpenTcpSession,
    TcpSessionEvent,
    TcpSessionRequest,
    UdpIndication,
    UdpPort,
    UdpRequest,
    UdpSendResult,
};
use crate::{
    api::{CloseReason, IoPayload, TransmissionId},
    driver::DriverConfig,
    test_support::init_test_logger,
};
use ::kompact::prelude::*;
use bytes::Bytes;
use std::{
    io::{Read, Write},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener},
    sync::mpsc,
    thread,
    time::Duration,
};

const WAIT_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(ComponentDefinition)]
struct UdpObserver {
    ctx: ComponentContext<Self>,
    udp: RequiredPort<UdpPort>,
    indications: mpsc::Sender<UdpIndication>,
}

impl UdpObserver {
    fn new(indications: mpsc::Sender<UdpIndication>) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            udp: RequiredPort::uninitialised(),
            indications,
        }
    }
}

ignore_lifecycle!(UdpObserver);

impl Require<UdpPort> for UdpObserver {
    fn handle(&mut self, indication: UdpIndication) -> Handled {
        self.indications
            .send(indication)
            .expect("UDP indication receiver must be live during test");
        Handled::Ok
    }
}

impl Actor for UdpObserver {
    type Message = Never;

    fn receive_local(&mut self, _msg: Self::Message) -> Handled {
        unreachable!("Never type is empty")
    }

    fn receive_network(&mut self, _msg: NetMessage) -> Handled {
        unimplemented!("UDP observer test component does not use network actor messages")
    }
}

#[derive(ComponentDefinition)]
struct UdpSendResultProbe {
    ctx: ComponentContext<Self>,
    results: mpsc::Sender<UdpSendResult>,
}

impl UdpSendResultProbe {
    fn new(results: mpsc::Sender<UdpSendResult>) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            results,
        }
    }
}

ignore_lifecycle!(UdpSendResultProbe);

impl Actor for UdpSendResultProbe {
    type Message = UdpSendResult;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        self.results
            .send(msg)
            .expect("UDP send result receiver must be live during test");
        Handled::Ok
    }

    fn receive_network(&mut self, _msg: NetMessage) -> Handled {
        unimplemented!("UDP send result probe does not use network actor messages")
    }
}

#[derive(ComponentDefinition)]
struct TcpSessionEventProbe {
    ctx: ComponentContext<Self>,
    events: mpsc::Sender<TcpSessionEvent>,
}

impl TcpSessionEventProbe {
    fn new(events: mpsc::Sender<TcpSessionEvent>) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            events,
        }
    }
}

ignore_lifecycle!(TcpSessionEventProbe);

impl Actor for TcpSessionEventProbe {
    type Message = TcpSessionEvent;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        self.events
            .send(msg)
            .expect("TCP session event receiver must be live during test");
        Handled::Ok
    }

    fn receive_network(&mut self, _msg: NetMessage) -> Handled {
        unimplemented!("TCP session probe does not use network actor messages")
    }
}

fn localhost(port: u16) -> SocketAddr {
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
}

fn payload_bytes(payload: IoPayload) -> Bytes {
    match payload {
        IoPayload::Lease(lease) => lease.create_byte_clone(),
        IoPayload::Bytes(bytes) => bytes,
    }
}

fn recv_until<T>(rx: &mpsc::Receiver<T>, mut predicate: impl FnMut(&T) -> bool) -> T {
    loop {
        let value = rx
            .recv_timeout(WAIT_TIMEOUT)
            .expect("timed out waiting for Kompact test event");
        if predicate(&value) {
            return value;
        }
    }
}

fn start_component<C>(system: &KompactSystem, component: &std::sync::Arc<Component<C>>)
where
    C: ComponentDefinition + ComponentLifecycle + Sized + 'static,
{
    system
        .start_notify(component)
        .wait_timeout(WAIT_TIMEOUT)
        .expect("component start");
}

fn kill_component<C>(system: &KompactSystem, component: std::sync::Arc<Component<C>>)
where
    C: ComponentDefinition + ComponentLifecycle + Sized + 'static,
{
    system
        .kill_notify(component)
        .wait_timeout(WAIT_TIMEOUT)
        .expect("component kill");
}

#[test]
fn udp_bridge_broadcasts_socket_activity_but_send_results_stay_private() {
    init_test_logger();

    let system = KompactConfig::default().build().expect("KompactSystem");
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
            local_addr: localhost(0),
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
            local_addr: localhost(0),
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

    let system = KompactConfig::default().build().expect("KompactSystem");
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
