//! Duplicate, malformed-frame, and empty-payload resilience scenarios.

use super::{frames::*, harness::*, support::*, *};

#[test]
fn runtime_out_of_order_payloads_still_reassemble_and_ack() {
    let harness = RuntimeHarness::new(
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::Pass,
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::ReorderFirstTransfer {
            buffered: Vec::new(),
            expected_parts: None,
            flushed: false,
        },
        DEFAULT_SENDER_CONFIG,
        DEFAULT_RECEIVER_CONFIG,
    );

    assert!(matches!(
        harness.send(IoPayload::from_static(b"abcdefghijkl")),
        UDPourSubmitResult::Sent
    ));

    let deliver = harness.wait_for_receiver_deliver();
    assert_eq!(deliver.payload.to_vec().as_slice(), b"abcdefghijkl");
    harness.wait_for_bridge_frame(harness.sender_socket_id, |frame| {
        matches!(frame, UDPourFrame::Ack(_))
    });
    harness.shutdown();
}
#[test]
fn runtime_duplicate_payload_datagrams_do_not_break_delivery() {
    let harness = RuntimeHarness::new(
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::Pass,
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::DuplicatePayloadPart {
            part_number: PartNumber(0),
            conflicting: false,
            duplicated: false,
            drop_later_payloads: false,
            duplicated_message_id: None,
        },
        DEFAULT_SENDER_CONFIG,
        DEFAULT_RECEIVER_CONFIG,
    );

    assert!(matches!(
        harness.send(IoPayload::from_static(b"abcdefghijkl")),
        UDPourSubmitResult::Sent
    ));

    let deliver = harness.wait_for_receiver_deliver();
    assert_eq!(deliver.payload.to_vec().as_slice(), b"abcdefghijkl");
    // Duplicate payload datagrams should be absorbed by the completed
    // transfer state instead of provoking a repair request.
    harness.assert_no_bridge_frame(
        harness.sender_socket_id,
        Duration::from_millis(500),
        |frame| matches!(frame, UDPourFrame::NeedParts(_)),
    );
    harness.shutdown();
}

#[test]
fn runtime_conflicting_duplicates_purge_without_requesting_repair() {
    let harness = RuntimeHarness::new(
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::Pass,
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::DuplicatePayloadPart {
            part_number: PartNumber(0),
            conflicting: true,
            duplicated: false,
            drop_later_payloads: true,
            duplicated_message_id: None,
        },
        DEFAULT_SENDER_CONFIG,
        DEFAULT_RECEIVER_CONFIG,
    );

    assert!(matches!(
        harness.send(IoPayload::from_static(b"abcdefgh")),
        UDPourSubmitResult::Sent
    ));
    // Conflicting duplicates must be purged locally. The receiver should
    // neither redeliver the corrupted transfer nor ask the sender for repair.
    harness.assert_no_receiver_deliver(Duration::from_millis(500));
    harness.assert_no_bridge_frame(
        harness.sender_socket_id,
        Duration::from_millis(500),
        |frame| matches!(frame, UDPourFrame::NeedParts(_)),
    );

    assert!(matches!(
        harness.send(IoPayload::from_static(b"qrst")),
        UDPourSubmitResult::Sent
    ));
    let deliver = harness.wait_for_receiver_deliver();
    assert_eq!(deliver.payload.to_vec().as_slice(), b"qrst");
    harness.shutdown();
}

#[test]
fn malformed_frames_are_dropped_without_poisoning_valid_traffic() {
    let harness = RuntimeHarness::new(
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::Pass,
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::InjectMalformedFramesOnce { injected: false },
        DEFAULT_SENDER_CONFIG,
        DEFAULT_RECEIVER_CONFIG,
    );

    assert!(matches!(
        harness.send(IoPayload::from_static(b"abcdefghijkl")),
        UDPourSubmitResult::Sent
    ));
    let first = harness.wait_for_receiver_deliver();
    assert_eq!(first.payload.to_vec().as_slice(), b"abcdefghijkl");

    assert!(matches!(
        harness.send(IoPayload::from_static(b"mnop")),
        UDPourSubmitResult::Sent
    ));
    let second = harness.wait_for_receiver_deliver();
    assert_eq!(second.payload.to_vec().as_slice(), b"mnop");
    harness.shutdown();
}

#[test]
fn zero_length_payload_round_trips_through_runtime() {
    let harness = RuntimeHarness::new(
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::Pass,
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::Pass,
        DEFAULT_SENDER_CONFIG,
        DEFAULT_RECEIVER_CONFIG,
    );

    assert!(matches!(
        harness.send(IoPayload::from_static(b"")),
        UDPourSubmitResult::Sent
    ));
    let deliver = harness.wait_for_receiver_deliver();
    assert_eq!(deliver.source, harness.sender_addr);
    assert_eq!(deliver.payload.len(), 0);
    harness.wait_for_bridge_frame(harness.sender_socket_id, |frame| {
        matches!(frame, UDPourFrame::Ack(_))
    });
    harness.shutdown();
}
