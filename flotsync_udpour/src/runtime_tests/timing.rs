//! Timing, pacing, retention, close, and ordering scenarios.

use super::{frames::*, harness::*, support::*, *};

#[test]
fn send_delay_and_window_pace_multipart_transmission() {
    let harness = RuntimeHarness::with_send_rate_control_manual_time(
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::Pass,
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::Pass,
        DEFAULT_SENDER_CONFIG,
        // This test isolates sender-side pacing. Receiver repair must stay out
        // of the way, otherwise `NeedParts` retransmissions can overlap with
        // the paced sends and stop testing the behaviour we actually care about.
        ReceiverConfig {
            repair_interval: Duration::from_mins(1),
            ..DEFAULT_RECEIVER_CONFIG
        },
        TestSendRateControl {
            send_delay: Duration::from_millis(40),
            backpressure_retry_delay: Duration::from_millis(10),
            max_in_flight_datagrams: 1,
        },
    );

    let submit = harness.submit_async(IoPayload::from_static(b"abcdefghijkl"));

    let first = harness.wait_for_bridge_frame(harness.receiver_socket_id, |frame| {
        matches!(
            frame,
            UDPourFrame::Payload(frame) if frame.header.part_number == PartNumber(0)
        )
    });
    let UDPourFrame::Payload(first_payload) = first else {
        unreachable!("filtered to Payload");
    };
    harness.wait_for_receiver_runtime_payload(harness.sender_addr, first_payload.header);
    // This short window is the pacing assertion itself: with the current
    // `send_delay`, the next payload part must not already be on the bridge
    // yet.
    harness.wait_for_sender_dispatch_timer();
    harness.advance_time_and_process_due_timers(Duration::from_millis(15));
    harness.assert_no_bridge_frame(harness.receiver_socket_id, Duration::ZERO, |frame| {
        matches!(frame, UDPourFrame::Payload(_))
    });
    harness.advance_time_and_process_due_timers(Duration::from_millis(25));

    let second = harness.wait_for_bridge_frame(harness.receiver_socket_id, |frame| {
        matches!(
            frame,
            UDPourFrame::Payload(frame) if frame.header.part_number == PartNumber(1)
        )
    });
    let UDPourFrame::Payload(second_payload) = second else {
        unreachable!("filtered to Payload");
    };
    harness.wait_for_receiver_runtime_payload(harness.sender_addr, second_payload.header);
    // The second gap proves the sender keeps pacing subsequent parts rather
    // than only delaying the initial burst.
    harness.wait_for_sender_dispatch_timer();
    harness.advance_time_and_process_due_timers(Duration::from_millis(15));
    harness.assert_no_bridge_frame(harness.receiver_socket_id, Duration::ZERO, |frame| {
        matches!(frame, UDPourFrame::Payload(_))
    });
    harness.advance_time_and_process_due_timers(Duration::from_millis(25));

    let third = harness.wait_for_bridge_frame(harness.receiver_socket_id, |frame| {
        matches!(
            frame,
            UDPourFrame::Payload(frame) if frame.header.part_number == PartNumber(2)
        )
    });
    assert!(matches!(third, UDPourFrame::Payload(_)));

    assert!(matches!(
        submit
            .wait_timeout(WAIT_TIMEOUT)
            .expect("timed out waiting for paced sender submit result"),
        UDPourSubmitResult::Sent
    ));

    let deliver = harness.wait_for_receiver_deliver();
    assert_eq!(deliver.payload.to_vec().as_slice(), b"abcdefghijkl");
    harness.shutdown();
}

#[test]
fn no_longer_available_after_sender_retention_expiry() {
    let harness = RuntimeHarness::new_manual_time(
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::Pass,
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::DropFirstPayloadPart {
            part_number: PartNumber(1),
            dropped: false,
        },
        SenderConfig {
            retention_timeout: Duration::from_millis(10),
            ..DEFAULT_SENDER_CONFIG
        },
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
    harness.advance_time_and_process_due_timers(Duration::from_millis(11));
    harness.advance_time_and_process_due_timers(Duration::from_millis(10));
    let need_parts = harness.wait_for_bridge_frame(harness.sender_socket_id, |frame| {
        matches!(frame, UDPourFrame::NeedParts(_))
    });
    let UDPourFrame::NeedParts(need_parts) = need_parts else {
        unreachable!("filtered to NeedParts");
    };
    harness.wait_for_sender_runtime_need_parts(harness.receiver_addr, &need_parts);
    let nla = harness.wait_for_bridge_frame(harness.receiver_socket_id, |frame| {
        matches!(frame, UDPourFrame::NoLongerAvailable(_))
    });
    assert!(matches!(nla, UDPourFrame::NoLongerAvailable(_)));
    // After `NoLongerAvailable`, the receiver must never assemble and deliver
    // the transfer from the partial state it still holds.
    harness.advance_time_and_process_due_timers(Duration::from_millis(500));
    harness.assert_no_receiver_deliver(Duration::from_millis(100));
    harness.shutdown();
}

#[test]
fn runtime_nack_reports_one_send_failed_with_correct_identity() {
    let harness = RuntimeHarness::new(
        ProxyRequestBehavior::NackFirstSend {
            reason: SendFailureReason::Closed,
            fired: false,
        },
        ProxyIndicationBehavior::Pass,
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::Pass,
        DEFAULT_SENDER_CONFIG,
        DEFAULT_RECEIVER_CONFIG,
    );

    let failed = harness.send(IoPayload::from_static(b"x"));
    assert_eq!(
        failed,
        UDPourSubmitResult::SendFailed {
            reason: UDPourSendFailureReason::Transport(SendFailureReason::Closed),
        }
    );
    harness.shutdown();
}

#[test]
fn socket_close_fails_submit_while_datagrams_are_still_queued() {
    let harness = RuntimeHarness::with_send_rate_control(
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::Pass,
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::Pass,
        DEFAULT_SENDER_CONFIG,
        DEFAULT_RECEIVER_CONFIG,
        TestSendRateControl {
            send_delay: Duration::from_millis(50),
            backpressure_retry_delay: Duration::from_millis(10),
            max_in_flight_datagrams: 1,
        },
    );

    let submit = harness.submit_async(IoPayload::from_static(
        b"abcdefghijklmnopqrstuvwxyz0123456789ABCD",
    ));
    harness.wait_for_bridge_frame(harness.receiver_socket_id, |frame| {
        matches!(
            frame,
            UDPourFrame::Payload(frame) if frame.header.part_number == PartNumber(0)
        )
    });
    harness.close_sender_socket();

    assert_eq!(
        submit
            .wait_timeout(WAIT_TIMEOUT)
            .expect("timed out waiting for closed-socket submit result"),
        UDPourSubmitResult::SendFailed {
            reason: UDPourSendFailureReason::Transport(SendFailureReason::Closed),
        }
    );
    harness.shutdown();
}
