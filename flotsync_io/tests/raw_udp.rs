use bytes::Bytes;
use flotsync_io::{
    api::UdpCloseReason,
    prelude::*,
    test_support::{
        assert_no_driver_event,
        init_test_logger,
        localhost,
        payload_bytes,
        wait_for_driver_event,
        wait_for_driver_request,
    },
};
use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket},
    time::Duration,
};

fn reserve_socket(driver: &IoDriver) -> SocketId {
    let request = driver.reserve_socket().expect("enqueue socket reservation");
    wait_for_driver_request(request)
}

fn bind_socket_at(driver: &IoDriver, socket_id: SocketId, local_addr: SocketAddr) -> SocketAddr {
    driver
        .dispatch(DriverCommand::Udp(UdpCommand::Bind {
            socket_id,
            bind: UdpLocalBind::Exact(local_addr),
        }))
        .expect("dispatch UDP bind");

    match wait_for_driver_event(driver, |event| {
        matches!(
            event,
            DriverEvent::Udp(UdpEvent::Bound {
                socket_id: observed_socket_id,
                ..
            }) if *observed_socket_id == socket_id
        )
    }) {
        DriverEvent::Udp(UdpEvent::Bound {
            socket_id: observed_socket_id,
            local_addr,
        }) => {
            assert_eq!(observed_socket_id, socket_id);
            local_addr
        }
        other => unreachable!("filtered to UDP Bound event, got {other:?}"),
    }
}

#[test]
fn udp_driver_supports_unconnected_and_connected_send_paths_and_closed_nacks() {
    init_test_logger();

    let driver = IoDriver::start(DriverConfig::default()).expect("driver starts");
    let receiver_id = reserve_socket(&driver);
    let sender_id = reserve_socket(&driver);
    let connected_sender_id = reserve_socket(&driver);

    let receiver_addr = bind_socket_at(&driver, receiver_id, localhost(0));
    let sender_addr = bind_socket_at(&driver, sender_id, localhost(0));

    driver
        .dispatch(DriverCommand::Udp(UdpCommand::Send {
            socket_id: sender_id,
            transmission_id: TransmissionId(1),
            payload: IoPayload::Bytes(Bytes::from_static(b"hello")),
            target: Some(receiver_addr),
        }))
        .expect("dispatch unconnected UDP send");

    let mut saw_unconnected_ack = false;
    let mut saw_unconnected_receive = false;
    while !saw_unconnected_ack || !saw_unconnected_receive {
        match wait_for_driver_event(&driver, |_| true) {
            DriverEvent::Udp(UdpEvent::SendAck {
                socket_id,
                transmission_id,
            }) if socket_id == sender_id && transmission_id == TransmissionId(1) => {
                saw_unconnected_ack = true;
            }
            DriverEvent::Udp(UdpEvent::Received {
                socket_id,
                source,
                payload,
            }) if socket_id == receiver_id => {
                assert_eq!(source, sender_addr);
                assert_eq!(payload_bytes(payload), Bytes::from_static(b"hello"));
                saw_unconnected_receive = true;
            }
            other => {
                log::debug!("ignoring unrelated raw UDP event: {:?}", other);
            }
        }
    }

    driver
        .dispatch(DriverCommand::Udp(UdpCommand::Connect {
            socket_id: connected_sender_id,
            remote_addr: receiver_addr,
            local_addr: None,
        }))
        .expect("dispatch connected UDP open");

    let connected_sender_addr = match wait_for_driver_event(&driver, |event| {
        matches!(
            event,
            DriverEvent::Udp(UdpEvent::Connected {
                socket_id: observed_socket_id,
                ..
            }) if *observed_socket_id == connected_sender_id
        )
    }) {
        DriverEvent::Udp(UdpEvent::Connected {
            socket_id,
            local_addr,
            remote_addr,
        }) => {
            assert_eq!(socket_id, connected_sender_id);
            assert_eq!(remote_addr, receiver_addr);
            local_addr
        }
        other => unreachable!("filtered to UDP Connected event, got {other:?}"),
    };

    driver
        .dispatch(DriverCommand::Udp(UdpCommand::Send {
            socket_id: connected_sender_id,
            transmission_id: TransmissionId(2),
            payload: IoPayload::Bytes(Bytes::from_static(b"world")),
            target: None,
        }))
        .expect("dispatch connected UDP send");

    let mut saw_connected_ack = false;
    let mut saw_connected_receive = false;
    while !saw_connected_ack || !saw_connected_receive {
        match wait_for_driver_event(&driver, |_| true) {
            DriverEvent::Udp(UdpEvent::SendAck {
                socket_id,
                transmission_id,
            }) if socket_id == connected_sender_id && transmission_id == TransmissionId(2) => {
                saw_connected_ack = true;
            }
            DriverEvent::Udp(UdpEvent::Received {
                socket_id,
                source,
                payload,
            }) if socket_id == receiver_id => {
                assert_eq!(source, connected_sender_addr);
                assert_eq!(payload_bytes(payload), Bytes::from_static(b"world"));
                saw_connected_receive = true;
            }
            other => {
                log::debug!("ignoring unrelated raw UDP event: {:?}", other);
            }
        }
    }

    driver
        .dispatch(DriverCommand::Udp(UdpCommand::Close {
            socket_id: connected_sender_id,
        }))
        .expect("dispatch UDP close");

    match wait_for_driver_event(&driver, |event| {
        matches!(
            event,
            DriverEvent::Udp(UdpEvent::Closed {
                socket_id: observed_socket_id,
                ..
            }) if *observed_socket_id == connected_sender_id
        )
    }) {
        DriverEvent::Udp(UdpEvent::Closed {
            socket_id,
            remote_addr,
            reason,
        }) => {
            assert_eq!(socket_id, connected_sender_id);
            assert_eq!(remote_addr, Some(receiver_addr));
            assert_eq!(reason, UdpCloseReason::Requested);
        }
        other => unreachable!("filtered to UDP Closed event, got {other:?}"),
    }

    driver
        .dispatch(DriverCommand::Udp(UdpCommand::Send {
            socket_id: connected_sender_id,
            transmission_id: TransmissionId(3),
            payload: IoPayload::Bytes(Bytes::from_static(b"closed")),
            target: None,
        }))
        .expect("dispatch UDP send on closed socket");

    match wait_for_driver_event(&driver, |event| {
        matches!(
            event,
            DriverEvent::Udp(UdpEvent::SendNack {
                socket_id: observed_socket_id,
                transmission_id,
                ..
            }) if *observed_socket_id == connected_sender_id && *transmission_id == TransmissionId(3)
        )
    }) {
        DriverEvent::Udp(UdpEvent::SendNack {
            socket_id,
            transmission_id,
            reason,
        }) => {
            assert_eq!(socket_id, connected_sender_id);
            assert_eq!(transmission_id, TransmissionId(3));
            assert_eq!(reason, SendFailureReason::Closed);
        }
        other => unreachable!("filtered to UDP SendNack event, got {other:?}"),
    }

    driver.shutdown().expect("driver shuts down");
}

#[test]
fn udp_driver_reports_bind_failures_and_socket_configuration_changes() {
    init_test_logger();

    let occupied_socket = UdpSocket::bind(localhost(0)).expect("bind occupied UDP socket");
    let occupied_addr = occupied_socket.local_addr().expect("occupied UDP addr");

    let driver = IoDriver::start(DriverConfig::default()).expect("driver starts");
    let failed_socket_id = reserve_socket(&driver);
    driver
        .dispatch(DriverCommand::Udp(UdpCommand::Bind {
            socket_id: failed_socket_id,
            bind: UdpLocalBind::Exact(occupied_addr),
        }))
        .expect("dispatch UDP bind that should fail");

    match wait_for_driver_event(&driver, |event| {
        matches!(
            event,
            DriverEvent::Udp(UdpEvent::BindFailed {
                socket_id: observed_socket_id,
                ..
            }) if *observed_socket_id == failed_socket_id
        )
    }) {
        DriverEvent::Udp(UdpEvent::BindFailed {
            socket_id,
            local_addr,
            ..
        }) => {
            assert_eq!(socket_id, failed_socket_id);
            assert_eq!(local_addr, occupied_addr);
        }
        other => unreachable!("filtered to UDP BindFailed event, got {other:?}"),
    }

    let config_socket_id = reserve_socket(&driver);
    let unspecified_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0));
    let _bound_addr = bind_socket_at(&driver, config_socket_id, unspecified_addr);
    let multicast_group = Ipv4Addr::new(224, 0, 0, 251);
    let options = [
        UdpSocketOption::Broadcast(true),
        UdpSocketOption::MulticastLoopV4(true),
        UdpSocketOption::MulticastTtlV4(7),
        UdpSocketOption::MulticastInterfaceV4(Ipv4Addr::UNSPECIFIED),
        UdpSocketOption::JoinMulticastV4 {
            group: multicast_group,
            interface: Ipv4Addr::UNSPECIFIED,
        },
        UdpSocketOption::LeaveMulticastV4 {
            group: multicast_group,
            interface: Ipv4Addr::UNSPECIFIED,
        },
    ];

    for option in options {
        driver
            .dispatch(DriverCommand::Udp(UdpCommand::Configure {
                socket_id: config_socket_id,
                option,
            }))
            .expect("dispatch UDP configure");

        match wait_for_driver_event(&driver, |event| {
            matches!(
                event,
                DriverEvent::Udp(UdpEvent::Configured {
                    socket_id: observed_socket_id,
                    option: observed_option,
                }) if *observed_socket_id == config_socket_id && *observed_option == option
            )
        }) {
            DriverEvent::Udp(UdpEvent::Configured {
                socket_id,
                option: observed_option,
            }) => {
                assert_eq!(socket_id, config_socket_id);
                assert_eq!(observed_option, option);
            }
            other => unreachable!("filtered to UDP Configured event, got {other:?}"),
        }
    }

    drop(occupied_socket);
    driver.shutdown().expect("driver shuts down");
}

#[test]
fn udp_driver_read_suspends_and_resumes_when_ingress_capacity_returns() {
    init_test_logger();

    let pool_config = IoPoolConfig {
        chunk_size: MAX_UDP_PAYLOAD_BYTES,
        initial_chunk_count: 1,
        max_chunk_count: 2,
        encode_buf_min_free_space: 64,
    };
    let driver = IoDriver::start(DriverConfig {
        buffer_config: IoBufferConfig {
            ingress: pool_config.clone(),
            egress: pool_config,
        },
        ..DriverConfig::default()
    })
    .expect("driver starts");

    let receiver_id = reserve_socket(&driver);
    let sender_id = reserve_socket(&driver);
    let receiver_addr = bind_socket_at(&driver, receiver_id, localhost(0));
    bind_socket_at(&driver, sender_id, localhost(0));

    for (transmission_id, payload) in [
        (TransmissionId(10), Bytes::from_static(b"first")),
        (TransmissionId(11), Bytes::from_static(b"second")),
        (TransmissionId(12), Bytes::from_static(b"third")),
    ] {
        driver
            .dispatch(DriverCommand::Udp(UdpCommand::Send {
                socket_id: sender_id,
                transmission_id,
                payload: IoPayload::Bytes(payload),
                target: Some(receiver_addr),
            }))
            .expect("dispatch UDP send for ingress-starvation scenario");
    }

    let first_lease = match wait_for_driver_event(&driver, |event| {
        matches!(
            event,
            DriverEvent::Udp(UdpEvent::Received {
                socket_id,
                payload: IoPayload::Lease(_),
                ..
            }) if *socket_id == receiver_id
        )
    }) {
        DriverEvent::Udp(UdpEvent::Received {
            payload: IoPayload::Lease(lease),
            ..
        }) => lease,
        other => unreachable!("filtered to first lease-backed UDP receive, got {other:?}"),
    };
    let second_lease = match wait_for_driver_event(&driver, |event| {
        matches!(
            event,
            DriverEvent::Udp(UdpEvent::Received {
                socket_id,
                payload: IoPayload::Lease(_),
                ..
            }) if *socket_id == receiver_id
        )
    }) {
        DriverEvent::Udp(UdpEvent::Received {
            payload: IoPayload::Lease(lease),
            ..
        }) => lease,
        other => unreachable!("filtered to second lease-backed UDP receive, got {other:?}"),
    };

    match wait_for_driver_event(&driver, |event| {
        matches!(
            event,
            DriverEvent::Udp(UdpEvent::ReadSuspended {
                socket_id: observed_socket_id,
            }) if *observed_socket_id == receiver_id
        )
    }) {
        DriverEvent::Udp(UdpEvent::ReadSuspended { socket_id }) => {
            assert_eq!(socket_id, receiver_id);
        }
        other => unreachable!("filtered to UDP ReadSuspended event, got {other:?}"),
    }

    drop(first_lease);
    drop(second_lease);

    let mut saw_resume = false;
    let mut saw_third_payload = false;
    while !saw_resume || !saw_third_payload {
        match wait_for_driver_event(&driver, |_| true) {
            DriverEvent::Udp(UdpEvent::ReadResumed { socket_id }) if socket_id == receiver_id => {
                saw_resume = true;
            }
            DriverEvent::Udp(UdpEvent::Received {
                socket_id, payload, ..
            }) if socket_id == receiver_id => {
                assert_eq!(payload_bytes(payload), Bytes::from_static(b"third"));
                saw_third_payload = true;
            }
            other => {
                log::debug!(
                    "ignoring unrelated raw UDP event while waiting for resume/receive: {:?}",
                    other
                );
            }
        }
    }

    driver
        .dispatch(DriverCommand::Udp(UdpCommand::Close {
            socket_id: receiver_id,
        }))
        .expect("dispatch UDP close for receiver");
    match wait_for_driver_event(&driver, |event| {
        matches!(
            event,
            DriverEvent::Udp(UdpEvent::Closed {
                socket_id: observed_socket_id,
                ..
            }) if *observed_socket_id == receiver_id
        )
    }) {
        DriverEvent::Udp(UdpEvent::Closed {
            socket_id,
            remote_addr,
            reason,
        }) => {
            assert_eq!(socket_id, receiver_id);
            assert_eq!(remote_addr, None);
            assert_eq!(reason, UdpCloseReason::Requested);
        }
        other => unreachable!("filtered to UDP Closed event, got {other:?}"),
    }

    driver
        .dispatch(DriverCommand::Udp(UdpCommand::Send {
            socket_id: receiver_id,
            transmission_id: TransmissionId(13),
            payload: IoPayload::Bytes(Bytes::from_static(b"after-close")),
            target: Some(receiver_addr),
        }))
        .expect("dispatch send on closed UDP socket");
    match wait_for_driver_event(&driver, |event| {
        matches!(
            event,
            DriverEvent::Udp(UdpEvent::SendNack {
                socket_id: observed_socket_id,
                transmission_id,
                ..
            }) if *observed_socket_id == receiver_id && *transmission_id == TransmissionId(13)
        )
    }) {
        DriverEvent::Udp(UdpEvent::SendNack {
            reason,
            transmission_id,
            ..
        }) => {
            assert_eq!(transmission_id, TransmissionId(13));
            assert_eq!(reason, SendFailureReason::Closed);
        }
        other => unreachable!("filtered to UDP SendNack event, got {other:?}"),
    }

    assert_no_driver_event(&driver, Duration::from_millis(25));
    driver.shutdown().expect("driver shuts down");
}
