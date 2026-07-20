//! Encoded `UDPour` frame builders and timing assertions for tests.

use super::{harness::RuntimeHarness, support::TestSendRateControl, *};

pub(in crate::runtime_tests) fn build_runtime_test_kompact_system(
    send_rate_control: TestSendRateControl,
    manual_time: bool,
) -> (KompactSystem, Option<KompactManualTimer>) {
    let configure = |config: &mut KompactConfig| {
        enable_bind_reuse_address(config);
        config.set_config_value(&config_keys::SEND_DELAY, send_rate_control.send_delay);
        config.set_config_value(
            &config_keys::BACKPRESSURE_RETRY_DELAY,
            send_rate_control.backpressure_retry_delay,
        );
        config.set_config_value(
            &config_keys::MAX_IN_FLIGHT_DATAGRAMS,
            send_rate_control.max_in_flight_datagrams,
        );
    };

    if manual_time {
        let (system, timer) = build_test_kompact_system_with_manual_timer(configure);
        (system, Some(timer))
    } else {
        (build_test_kompact_system_with(configure), None)
    }
}

pub(in crate::runtime_tests) fn bind_socket(
    observer: &Arc<Component<UdpObserver>>,
    observer_rx: &BufferedReceiver<UdpIndication>,
    local_addr: SocketAddr,
) -> (SocketId, SocketAddr) {
    let request_id = UdpOpenRequestId::new();
    observer.on_definition(|component| {
        component.udp.trigger(UdpRequest::Bind {
            request_id,
            bind: UdpLocalBind::Exact(local_addr),
            options: UdpBindOptions::default(),
        });
    });
    match observer_rx.recv_matching_or_fail(
        WAIT_TIMEOUT,
        |event| {
            matches!(
                event,
                UdpIndication::Bound {
                    request_id: indicated_request_id,
                    ..
                } if *indicated_request_id == request_id
            )
        },
        |event| match event {
            UdpIndication::BindFailed {
                request_id: indicated_request_id,
                local_addr,
                reason,
            } if *indicated_request_id == request_id => Some(format!(
                "UDPour runtime test socket bind failed for request {request_id:?} at {local_addr}: {reason:?}"
            )),
            _ => None,
        },
    ) {
        UdpIndication::Bound {
            request_id: indicated_request_id,
            socket_id,
            local_addr,
        } => {
            assert_eq!(indicated_request_id, request_id);
            (socket_id, local_addr)
        }
        other => unreachable!("filtered to Bound, got {other:?}"),
    }
}

pub(in crate::runtime_tests) const DEFAULT_SENDER_CONFIG: SenderConfig = SenderConfig {
    max_part_payload_len: NonZeroUsize::new(4).expect("part size is non-zero"),
    retention_timeout: Duration::from_millis(200),
    id_reuse_cooldown: Duration::from_millis(100),
    eager_ack_cleanup: false,
};

pub(in crate::runtime_tests) const DEFAULT_RECEIVER_CONFIG: ReceiverConfig = ReceiverConfig {
    repair_interval: Duration::from_millis(40),
    give_up_timeout: Duration::from_millis(200),
    max_need_parts_frame_len: 256,
    delivered_tombstone_timeout: Duration::from_millis(300),
};

pub(in crate::runtime_tests) fn wait_for_retransmitted_parts(
    harness: &RuntimeHarness,
    message_id: MessageId,
    expected_part_count: PartCount,
) -> Vec<u32> {
    let mut part_numbers = Vec::new();
    for _ in 0..expected_part_count.get() {
        let frame = harness.wait_for_bridge_frame(harness.receiver_socket_id, |frame| {
            matches!(
                frame,
                UDPourFrame::Payload(frame)
                    if frame.header.message_id == message_id && frame.header.is_retransmit()
            )
        });
        let UDPourFrame::Payload(frame) = frame else {
            unreachable!("filtered to retransmitted payload");
        };
        part_numbers.push(frame.header.part_number.0);
    }
    part_numbers.sort_unstable();
    part_numbers
}

pub(in crate::runtime_tests) fn payload_frame_for_socket(
    socket_id: SocketId,
    indication: &UdpIndication,
) -> Option<PayloadFrame> {
    let UdpIndication::Received {
        socket_id: indicated_socket_id,
        payload,
        ..
    } = indication
    else {
        return None;
    };
    if *indicated_socket_id != socket_id {
        return None;
    }
    match decode_frame(payload.clone()).ok()? {
        UDPourFrame::Payload(frame) => Some(frame),
        _ => None,
    }
}

pub(in crate::runtime_tests) fn conflicting_duplicate_indication(
    indication: &UdpIndication,
    frame: &PayloadFrame,
) -> UdpIndication {
    let UdpIndication::Received {
        socket_id, source, ..
    } = indication
    else {
        unreachable!("payload indication must be Received");
    };
    let mut bytes = frame.payload.to_vec();
    if bytes.is_empty() {
        bytes.push(0xFF);
    } else {
        bytes[0] ^= 0xFF;
    }
    let duplicate = UDPourFrame::Payload(PayloadFrame {
        header: frame.header,
        payload: IoPayload::from(Bytes::from(bytes)),
    });
    UdpIndication::Received {
        socket_id: *socket_id,
        source: *source,
        payload: encode_frame(&duplicate).expect("duplicate payload frame must encode"),
    }
}

pub(in crate::runtime_tests) fn malformed_payload_indication(
    socket_id: SocketId,
    source: SocketAddr,
) -> UdpIndication {
    let valid = UDPourFrame::Payload(PayloadFrame {
        header: UDPourHeader::payload(
            MessageId(901),
            PartNumber(0),
            PartCount::new(1).unwrap(),
            Checksum(77),
        ),
        payload: IoPayload::from_static(b"x"),
    });
    let mut bytes = encode_frame(&valid)
        .expect("valid payload frame must encode")
        .to_vec();
    bytes[8..12].copy_from_slice(&1u32.to_be_bytes());
    UdpIndication::Received {
        socket_id,
        source,
        payload: IoPayload::from(Bytes::from(bytes)),
    }
}

pub(in crate::runtime_tests) fn malformed_control_indication(
    socket_id: SocketId,
    source: SocketAddr,
) -> UdpIndication {
    let valid = UDPourFrame::Ack(AckFrame {
        header: UDPourHeader::control(
            FrameType::Ack,
            MessageId(902),
            PartCount::new(1).unwrap(),
            Checksum(88),
        )
        .unwrap(),
    });
    let mut bytes = encode_frame(&valid)
        .expect("valid ack frame must encode")
        .to_vec();
    bytes[1] = PROTOCOL_VERSION + 1;
    UdpIndication::Received {
        socket_id,
        source,
        payload: IoPayload::from(Bytes::from(bytes)),
    }
}
