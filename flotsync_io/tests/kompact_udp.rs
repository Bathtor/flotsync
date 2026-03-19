mod support;

use bytes::Bytes;
use flotsync_io::prelude::*;
use kompact::prelude::*;
use std::{sync::mpsc, time::Duration};
use support::{
    UdpObserver,
    UdpSendResultProbe,
    init_test_logger,
    kill_component,
    localhost,
    payload_bytes,
    recv_until,
    start_component,
};

#[test]
fn udp_bridge_broadcasts_shared_indications_and_keeps_send_results_private() {
    init_test_logger();

    let system = KompactConfig::default()
        .build()
        .expect("build KompactSystem");
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

    let bridge_to_observer1 = biconnect_components::<UdpPort, _, _>(&bridge, &observer1)
        .expect("connect bridge/observer1");
    let bridge_to_observer2 = biconnect_components::<UdpPort, _, _>(&bridge, &observer2)
        .expect("connect bridge/observer2");

    start_component(&system, &driver_component);
    start_component(&system, &bridge);
    start_component(&system, &observer1);
    start_component(&system, &observer2);
    start_component(&system, &reply1);
    start_component(&system, &reply2);

    let bridge_handle = IoBridgeHandle::from_component(&bridge);
    let receiver_id = bridge_handle
        .reserve_udp_socket()
        .wait_timeout(Duration::from_secs(2))
        .expect("receiver reservation future")
        .expect("receiver reservation");
    let sender_id = bridge_handle
        .reserve_udp_socket()
        .wait_timeout(Duration::from_secs(2))
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
        other => unreachable!("filtered to receiver Bound indication, got {other:?}"),
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
        other => unreachable!("filtered to mirrored receiver Bound indication, got {other:?}"),
    }

    observer2.on_definition(|component| {
        component.udp.trigger(UdpRequest::Bind {
            socket_id: sender_id,
            local_addr: localhost(0),
        });
    });
    let _ = recv_until(&observer1_rx, |event| {
        matches!(
            event,
            UdpIndication::Bound {
                socket_id,
                ..
            } if *socket_id == sender_id
        )
    });
    let _ = recv_until(&observer2_rx, |event| {
        matches!(
            event,
            UdpIndication::Bound {
                socket_id,
                ..
            } if *socket_id == sender_id
        )
    });

    let option = UdpSocketOption::Broadcast(true);
    observer1.on_definition(|component| {
        component.udp.trigger(UdpRequest::Configure {
            socket_id: sender_id,
            option,
        });
    });
    let _ = recv_until(&observer1_rx, |event| {
        matches!(
            event,
            UdpIndication::Configured {
                socket_id,
                option: observed_option,
            } if *socket_id == sender_id && *observed_option == option
        )
    });
    let _ = recv_until(&observer2_rx, |event| {
        matches!(
            event,
            UdpIndication::Configured {
                socket_id,
                option: observed_option,
            } if *socket_id == sender_id && *observed_option == option
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
        other => unreachable!("filtered to UDP private Ack result, got {other:?}"),
    }
    assert!(
        reply2_rx.recv_timeout(Duration::from_millis(200)).is_err(),
        "unused UDP send-result recipient must stay silent"
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
        other => unreachable!("filtered to first UDP Received indication, got {other:?}"),
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
        other => unreachable!("filtered to mirrored UDP Received indication, got {other:?}"),
    }

    drop(bridge_handle);
    drop(bridge_to_observer1);
    drop(bridge_to_observer2);
    kill_component(&system, reply1);
    kill_component(&system, reply2);
    kill_component(&system, observer1);
    kill_component(&system, observer2);
    kill_component(&system, bridge);
    kill_component(&system, driver_component);
    system.shutdown().expect("Kompact shutdown");
}

#[test]
fn udp_bridge_shutdown_releases_owned_socket_bindings() {
    init_test_logger();

    let system = KompactConfig::default()
        .build()
        .expect("build KompactSystem");
    let driver_component = system.create(|| IoDriverComponent::new(DriverConfig::default()));
    start_component(&system, &driver_component);

    let driver_for_bridge1 = driver_component.clone();
    let bridge1 = system.create(move || IoBridge::new(&driver_for_bridge1));
    let (observer1_tx, observer1_rx) = mpsc::channel();
    let observer1 = system.create(move || UdpObserver::new(observer1_tx));
    let bridge1_to_observer1 = biconnect_components::<UdpPort, _, _>(&bridge1, &observer1)
        .expect("connect first bridge/observer");
    start_component(&system, &bridge1);
    start_component(&system, &observer1);

    let bridge1_handle = IoBridgeHandle::from_component(&bridge1);
    let socket_id = bridge1_handle
        .reserve_udp_socket()
        .wait_timeout(Duration::from_secs(2))
        .expect("first socket reservation future")
        .expect("first socket reservation");

    observer1.on_definition(|component| {
        component.udp.trigger(UdpRequest::Bind {
            socket_id,
            local_addr: localhost(0),
        });
    });
    let bound_addr = match recv_until(&observer1_rx, |event| {
        matches!(
            event,
            UdpIndication::Bound {
                socket_id: observed_socket_id,
                ..
            } if *observed_socket_id == socket_id
        )
    }) {
        UdpIndication::Bound { local_addr, .. } => local_addr,
        other => unreachable!("filtered to first bridge Bound indication, got {other:?}"),
    };

    drop(bridge1_handle);
    drop(bridge1_to_observer1);
    kill_component(&system, observer1);
    kill_component(&system, bridge1);

    let driver_for_bridge2 = driver_component.clone();
    let bridge2 = system.create(move || IoBridge::new(&driver_for_bridge2));
    let (observer2_tx, observer2_rx) = mpsc::channel();
    let observer2 = system.create(move || UdpObserver::new(observer2_tx));
    let bridge2_to_observer2 = biconnect_components::<UdpPort, _, _>(&bridge2, &observer2)
        .expect("connect second bridge/observer");
    start_component(&system, &bridge2);
    start_component(&system, &observer2);

    let bridge2_handle = IoBridgeHandle::from_component(&bridge2);
    let rebound_socket_id = bridge2_handle
        .reserve_udp_socket()
        .wait_timeout(Duration::from_secs(2))
        .expect("second socket reservation future")
        .expect("second socket reservation");

    observer2.on_definition(|component| {
        component.udp.trigger(UdpRequest::Bind {
            socket_id: rebound_socket_id,
            local_addr: bound_addr,
        });
    });
    match recv_until(&observer2_rx, |event| {
        matches!(
            event,
            UdpIndication::Bound {
                socket_id: observed_socket_id,
                ..
            } if *observed_socket_id == rebound_socket_id
        )
    }) {
        UdpIndication::Bound {
            socket_id: observed_socket_id,
            local_addr,
        } => {
            assert_eq!(observed_socket_id, rebound_socket_id);
            assert_eq!(local_addr, bound_addr);
        }
        other => unreachable!("filtered to rebound UDP Bound indication, got {other:?}"),
    }

    drop(bridge2_handle);
    drop(bridge2_to_observer2);
    kill_component(&system, observer2);
    kill_component(&system, bridge2);
    kill_component(&system, driver_component);
    system.shutdown().expect("Kompact shutdown");
}
