//! Basic delivery, acknowledgement, backpressure, and repair scenarios.

use super::{frames::*, harness::*, support::*, *};

#[test]
fn basic_component_smoke_send_deliver_ack_without_repair() {
    let harness = RuntimeHarness::new(
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::Pass,
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::Pass,
        DEFAULT_SENDER_CONFIG,
        DEFAULT_RECEIVER_CONFIG,
    );

    assert!(matches!(
        harness.send(IoPayload::from_static(b"hello world")),
        UDPourSubmitResult::Sent
    ));

    let deliver = harness.wait_for_receiver_deliver();
    assert_eq!(deliver.payload.to_vec().as_slice(), b"hello world");

    let ack = harness.wait_for_bridge_frame(harness.sender_socket_id, |frame| {
        matches!(frame, UDPourFrame::Ack(_))
    });
    assert!(matches!(ack, UDPourFrame::Ack(_)));
    // Once the receiver has delivered and the sender has observed the ACK,
    // the repair path must stay dormant instead of emitting a spurious
    // `NeedParts` request later on.
    harness.assert_no_bridge_frame(
        harness.sender_socket_id,
        Duration::from_millis(500),
        |frame| matches!(frame, UDPourFrame::NeedParts(_)),
    );
    harness.shutdown();
}

#[test]
fn backpressure_is_retried_without_failing_logical_send() {
    let harness = RuntimeHarness::new_manual_time(
        ProxyRequestBehavior::NackFirstSend {
            reason: SendFailureReason::Backpressure,
            fired: false,
        },
        ProxyIndicationBehavior::Pass,
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::Pass,
        DEFAULT_SENDER_CONFIG,
        DEFAULT_RECEIVER_CONFIG,
    );

    let submit = harness.submit_async(IoPayload::from_static(b"hello world"));
    harness.wait_for_sender_dispatch_timer();
    harness.advance_time(TestSendRateControl::default().backpressure_retry_delay);
    assert!(matches!(
        submit
            .wait_timeout(WAIT_TIMEOUT)
            .expect("timed out waiting for backpressure retry submit result"),
        UDPourSubmitResult::Sent
    ));

    let deliver = harness.wait_for_receiver_deliver();
    assert_eq!(deliver.payload.to_vec().as_slice(), b"hello world");
    harness.wait_for_bridge_frame(harness.sender_socket_id, |frame| {
        matches!(frame, UDPourFrame::Ack(_))
    });
    harness.shutdown();
}

#[test]
fn repair_path_emits_need_parts_and_retransmits_only_missing_part() {
    let harness = RuntimeHarness::new_manual_time(
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::Pass,
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::DropFirstPayloadPart {
            part_number: PartNumber(1),
            dropped: false,
        },
        DEFAULT_SENDER_CONFIG,
        ReceiverConfig {
            repair_interval: Duration::from_millis(20),
            ..DEFAULT_RECEIVER_CONFIG
        },
    );

    assert!(matches!(
        harness.send(IoPayload::from_static(b"abcdefghijkl")),
        UDPourSubmitResult::Sent
    ));
    for _ in 0..3 {
        let frame = harness.wait_for_bridge_frame(harness.receiver_socket_id, |frame| {
            matches!(frame, UDPourFrame::Payload(_))
        });
        let UDPourFrame::Payload(payload) = frame else {
            unreachable!("filtered to Payload");
        };
        if payload.header.part_number != PartNumber(1) {
            harness.wait_for_receiver_runtime_payload(harness.sender_addr, payload.header);
        }
    }

    harness.advance_time_and_process_due_timers(Duration::from_millis(20));

    let need_parts = harness.wait_for_bridge_frame(harness.sender_socket_id, |frame| {
        matches!(frame, UDPourFrame::NeedParts(_))
    });
    let UDPourFrame::NeedParts(need_parts) = need_parts else {
        unreachable!("filtered to NeedParts");
    };
    let missing: Vec<_> = need_parts.missing_parts.iter().collect();
    assert_eq!(missing, vec![1]);
    harness.wait_for_sender_runtime_need_parts(harness.receiver_addr, &need_parts);

    let retransmit = harness.wait_for_bridge_frame(harness.receiver_socket_id, |frame| {
        let UDPourFrame::Payload(frame) = frame else {
            return false;
        };
        if frame.header.part_number != PartNumber(1) {
            return false;
        }
        frame.payload.to_vec().as_slice() == b"efgh"
    });
    assert!(matches!(retransmit, UDPourFrame::Payload(_)));
    // This transfer is missing exactly one part, so the repair path must stop
    // after retransmitting that part instead of leaking extra payload frames.
    harness.assert_no_bridge_frame(
        harness.receiver_socket_id,
        Duration::from_millis(500),
        |frame| matches!(frame, UDPourFrame::Payload(_)),
    );

    let deliver = harness.wait_for_receiver_deliver();
    assert_eq!(deliver.payload.to_vec().as_slice(), b"abcdefghijkl");
    harness.shutdown();
}

#[test]
fn shared_route_retransmissions_do_not_redeliver_before_tombstone_expiry() {
    let sender_config = SenderConfig {
        retention_timeout: Duration::from_millis(80),
        id_reuse_cooldown: Duration::from_millis(40),
        ..DEFAULT_SENDER_CONFIG
    };
    let harness = RuntimeHarness::new_manual_time(
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::Pass,
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::Pass,
        sender_config,
        ReceiverConfig {
            repair_interval: Duration::from_millis(20),
            ..DEFAULT_RECEIVER_CONFIG
        },
    );

    assert!(matches!(
        harness.send(IoPayload::from_static(b"abcdefghijkl")),
        UDPourSubmitResult::Sent
    ));
    let UDPourFrame::Payload(first_payload) = harness
        .wait_for_bridge_frame(harness.receiver_socket_id, |frame| {
            matches!(frame, UDPourFrame::Payload(_))
        })
    else {
        unreachable!("filtered to Payload");
    };
    let message_id = first_payload.header.message_id;
    let part_count = first_payload.header.part_count;
    let checksum = first_payload.header.checksum;
    let first_deliver = harness.wait_for_receiver_deliver();
    assert_eq!(first_deliver.payload.to_vec().as_slice(), b"abcdefghijkl");
    harness.wait_for_bridge_frame(harness.sender_socket_id, |frame| {
        matches!(frame, UDPourFrame::Ack(_))
    });

    let missing_parts = RoaringBitmap::from([0, 1, 2]);
    harness.inject_sender_indication(
        localhost(9001),
        &UDPourFrame::NeedParts(NeedPartsFrame {
            header: UDPourHeader::control(FrameType::NeedParts, message_id, part_count, checksum)
                .unwrap(),
            missing_parts,
        }),
    );

    let retransmitted_parts = wait_for_retransmitted_parts(&harness, message_id, part_count);
    assert_eq!(retransmitted_parts, vec![0, 1, 2]);

    // This wait has to stay inside the current tombstone lifetime. It proves
    // that late shared-route repairs do not redeliver before duplicate
    // suppression expires.
    harness.advance_time_and_process_due_timers(Duration::from_millis(60));
    harness.assert_no_receiver_deliver(Duration::ZERO);
    harness.wait_for_bridge_frame(harness.sender_socket_id, |frame| {
        matches!(frame, UDPourFrame::Ack(_))
    });

    harness.advance_time_and_process_due_timers(Duration::from_millis(140));

    for (part_number, payload) in [
        (PartNumber(0), IoPayload::from_static(b"abcd")),
        (PartNumber(1), IoPayload::from_static(b"efgh")),
        (PartNumber(2), IoPayload::from_static(b"ijkl")),
    ] {
        harness.inject_receiver_indication(
            harness.sender_addr,
            &UDPourFrame::Payload(PayloadFrame {
                header: UDPourHeader::payload(message_id, part_number, part_count, checksum),
                payload,
            }),
        );
    }

    let second_deliver = harness.wait_for_receiver_deliver();
    assert_eq!(second_deliver.payload.to_vec().as_slice(), b"abcdefghijkl");
    harness.shutdown();
}
