//! Tests for TCP and UDP bind-reuse configuration.

use super::{fixtures::*, *};

#[test]
fn udp_bind_reuse_config_allows_binding_to_a_reserved_port() {
    let (_reservation, reserved_addr) = hold_reusable_udp_reservation();

    let system = build_test_kompact_system();
    let driver_component = system.create(|| IoDriverComponent::new(DriverConfig::default()));
    let driver_for_bridge = driver_component.clone();
    let bridge = system.create(move || IoBridge::new(&driver_for_bridge));
    let (observer_tx, observer_rx) = mpsc::channel();
    let observer = system.create(move || UdpObserver::new(observer_tx));
    let _bridge_to_observer =
        biconnect_components::<UdpPort, _, _>(&bridge, &observer).expect("bridge/observer");

    start_component(&system, &driver_component);
    start_component(&system, &bridge);
    start_component(&system, &observer);

    let request_id = UdpOpenRequestId::new();
    observer.on_definition(|component| {
        component.udp.trigger(UdpRequest::Bind {
            request_id,
            bind: UdpLocalBind::Exact(reserved_addr),
            options: UdpBindOptions::default(),
        });
    });
    match recv_until(&observer_rx, |event| {
        matches!(
            event,
            UdpIndication::Bound { request_id: event_request_id, .. }
                | UdpIndication::BindFailed { request_id: event_request_id, .. }
                if *event_request_id == request_id
        )
    }) {
        UdpIndication::BindFailed {
            request_id: failed_request_id,
            local_addr,
            ..
        } => {
            assert_eq!(failed_request_id, request_id);
            assert_eq!(local_addr, reserved_addr);
        }
        other => panic!("reserved UDP bind without reuse config unexpectedly produced {other:?}"),
    }

    kill_component(&system, observer);
    kill_component(&system, bridge);
    kill_component(&system, driver_component);
    system.shutdown().wait().expect("Kompact shutdown");

    let system = build_test_kompact_system_with(enable_bind_reuse_address);
    let driver_component = system.create(|| IoDriverComponent::new(DriverConfig::default()));
    let driver_for_bridge = driver_component.clone();
    let bridge = system.create(move || IoBridge::new(&driver_for_bridge));
    let (observer_tx, observer_rx) = mpsc::channel();
    let observer = system.create(move || UdpObserver::new(observer_tx));
    let _bridge_to_observer =
        biconnect_components::<UdpPort, _, _>(&bridge, &observer).expect("bridge/observer");

    start_component(&system, &driver_component);
    start_component(&system, &bridge);
    start_component(&system, &observer);

    let request_id = UdpOpenRequestId::new();
    observer.on_definition(|component| {
        component.udp.trigger(UdpRequest::Bind {
            request_id,
            bind: UdpLocalBind::Exact(reserved_addr),
            options: UdpBindOptions::default(),
        });
    });
    match recv_until(&observer_rx, |event| {
        matches!(
            event,
            UdpIndication::Bound { request_id: event_request_id, .. }
                | UdpIndication::BindFailed { request_id: event_request_id, .. }
                if *event_request_id == request_id
        )
    }) {
        UdpIndication::Bound {
            request_id: bound_request_id,
            local_addr,
            ..
        } => {
            assert_eq!(bound_request_id, request_id);
            assert_eq!(local_addr, reserved_addr);
        }
        other => panic!("reserved UDP bind with reuse config unexpectedly produced {other:?}"),
    }

    kill_component(&system, observer);
    kill_component(&system, bridge);
    kill_component(&system, driver_component);
    system.shutdown().wait().expect("Kompact shutdown");
}

#[test]
fn udp_bind_request_options_allow_binding_to_a_reserved_port() {
    let (_reservation, reserved_addr) = hold_reusable_udp_reservation();

    let system = build_test_kompact_system();
    let driver_component = system.create(|| IoDriverComponent::new(DriverConfig::default()));
    let driver_for_bridge = driver_component.clone();
    let bridge = system.create(move || IoBridge::new(&driver_for_bridge));
    let (observer_tx, observer_rx) = mpsc::channel();
    let observer = system.create(move || UdpObserver::new(observer_tx));
    biconnect_components::<UdpPort, _, _>(&bridge, &observer).expect("bridge/observer");

    start_component(&system, &driver_component);
    start_component(&system, &bridge);
    start_component(&system, &observer);

    let request_id = UdpOpenRequestId::new();
    observer.on_definition(|component| {
        component.udp.trigger(UdpRequest::Bind {
            request_id,
            bind: UdpLocalBind::Exact(reserved_addr),
            options: UdpBindOptions::default().with_socket_reuse(true),
        });
    });
    match recv_until(&observer_rx, |event| {
        matches!(
            event,
            UdpIndication::Bound { request_id: event_request_id, .. }
                | UdpIndication::BindFailed { request_id: event_request_id, .. }
                if *event_request_id == request_id
        )
    }) {
        UdpIndication::Bound {
            request_id: bound_request_id,
            local_addr,
            ..
        } => {
            assert_eq!(bound_request_id, request_id);
            assert_eq!(local_addr, reserved_addr);
        }
        other => panic!("reserved UDP bind with request reuse unexpectedly produced {other:?}"),
    }

    kill_component(&system, observer);
    kill_component(&system, bridge);
    kill_component(&system, driver_component);
    system.shutdown().wait().expect("Kompact shutdown");
}

#[test]
#[allow(
    clippy::match_wildcard_for_single_variants,
    reason = "The receive helper filters to one event variant and the fallback keeps assertion diagnostics precise."
)]
fn tcp_listener_reuse_config_allows_binding_to_a_reserved_port() {
    let (_reservation, reserved_addr) = hold_reusable_tcp_reservation();

    let system = build_test_kompact_system_with(enable_bind_reuse_address);
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
            local_addr: reserved_addr,
            incoming_to: listener_probe.actor_ref().recipient(),
        })
        .wait_timeout(WAIT_TIMEOUT)
        .expect("open TCP listener future")
        .expect("reserved TCP port should open with reuse config");
    assert_eq!(opened_listener.local_addr, reserved_addr);

    opened_listener
        .listener
        .tell(super::TcpListenerRequest::Close);
    match recv_until(&listener_rx, |event| {
        matches!(event, TcpListenerEvent::Closed)
    }) {
        TcpListenerEvent::Closed => {}
        other => unreachable!("filtered to TCP listener Closed, got {other:?}"),
    }

    drop(opened_listener);
    drop(bridge_handle);
    kill_component(&system, listener_probe);
    kill_component(&system, bridge);
    kill_component(&system, driver_component);
    system.shutdown().wait().expect("Kompact shutdown");
}
