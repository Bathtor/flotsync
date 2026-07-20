//! Tests for UDP bridge delivery and socket indications.

use super::*;

#[test]
#[allow(
    clippy::too_many_lines,
    clippy::match_wildcard_for_single_variants,
    reason = "This Kompact integration test verifies multi-recipient UDP bridge routing end to end."
)]
fn udp_bridge_broadcasts_socket_activity_but_send_results_stay_private() {
    let mut socket_lease =
        reserve_sockets(&[ReservedSocketKind::UdpSocket, ReservedSocketKind::UdpSocket]);
    let system = build_test_kompact_system_with(enable_bind_reuse_address);
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

    biconnect_components::<UdpPort, _, _>(&bridge, &observer1)
        .expect("bridge/observer1 connection");
    biconnect_components::<UdpPort, _, _>(&bridge, &observer2)
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
            bind: UdpLocalBind::Exact(socket_lease.addr(0)),
            options: UdpBindOptions::default(),
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
            bind: UdpLocalBind::Exact(socket_lease.addr(1)),
            options: UdpBindOptions::default(),
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
    socket_lease.release_binding(0);
    socket_lease.release_binding(1);

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
            assert_eq!(payload.to_vec().as_slice(), b"hello");
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
            assert_eq!(payload.to_vec().as_slice(), b"hello");
        }
        other => unreachable!("filtered to UDP receive, got {other:?}"),
    }

    socket_lease
        .rebind_binding(0)
        .expect("rebind reserved UDP receiver");
    socket_lease
        .rebind_binding(1)
        .expect("rebind reserved UDP sender");
    drop(bridge_handle);
    kill_component(&system, reply1);
    kill_component(&system, reply2);
    kill_component(&system, observer1);
    kill_component(&system, observer2);
    kill_component(&system, bridge);
    kill_component(&system, driver_component);

    system.shutdown().wait().expect("Kompact shutdown");
}

#[test]
#[allow(
    clippy::too_many_lines,
    clippy::match_wildcard_for_single_variants,
    reason = "This Kompact integration test verifies UDP configuration fan-out end to end."
)]
fn udp_bridge_broadcasts_socket_configuration_indications() {
    let mut socket_lease = reserve_sockets(&[ReservedSocketKind::UdpSocket]);
    let system = build_test_kompact_system_with(enable_bind_reuse_address);
    let driver_component = system.create(|| IoDriverComponent::new(DriverConfig::default()));
    let driver_for_bridge = driver_component.clone();
    let bridge = system.create(move || IoBridge::new(&driver_for_bridge));

    let (observer1_tx, observer1_rx) = mpsc::channel();
    let observer1 = system.create(move || UdpObserver::new(observer1_tx));
    let (observer2_tx, observer2_rx) = mpsc::channel();
    let observer2 = system.create(move || UdpObserver::new(observer2_tx));

    let bridge_to_observer1 = biconnect_components::<UdpPort, _, _>(&bridge, &observer1)
        .expect("bridge/observer1 connection");
    let bridge_to_observer2 = biconnect_components::<UdpPort, _, _>(&bridge, &observer2)
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
            bind: UdpLocalBind::Exact(socket_lease.addr(0)),
            options: UdpBindOptions::default(),
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
    socket_lease.release_binding(0);

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

    socket_lease
        .rebind_binding(0)
        .expect("rebind reserved UDP socket");
    drop(bridge_handle);
    drop(bridge_to_observer1);
    drop(bridge_to_observer2);
    kill_component(&system, observer1);
    kill_component(&system, observer2);
    kill_component(&system, bridge);
    kill_component(&system, driver_component);

    system.shutdown().wait().expect("Kompact shutdown");
}
