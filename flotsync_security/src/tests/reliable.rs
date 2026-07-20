//! Tests for reliable-delivery payload encryption.

use super::{fixtures::*, *};

#[test]
fn reliable_payload_round_trips_to_recipient() {
    let alice = local_member("alice", ALICE_SEED);
    let bob = local_member("bob", BOB_SEED);
    let context = ReliablePayloadContext {
        frame_kind: "runtime-message",
        sender: alice.member_id(),
        recipient: bob.member_id(),
        scope: HpkeEnvelopeScope::DirectMessage,
        message_id: Uuid::from_u128(77),
    };
    let mut rng = rng_from_seed([9_u8; 32]);

    let sealed =
        seal_reliable_payload(&alice, bob.public_keys(), context, b"group key", &mut rng).unwrap();
    let opened = open_reliable_payload(alice.public_keys(), &bob, context, &sealed).unwrap();

    assert_eq!(opened, b"group key");
}

#[test]
fn reliable_payload_seal_rejects_context_sender_key_mismatch() {
    let alice = local_member("alice", ALICE_SEED);
    let bob = local_member("bob", BOB_SEED);
    let context = ReliablePayloadContext {
        frame_kind: "runtime-message",
        sender: bob.member_id(),
        recipient: bob.member_id(),
        scope: HpkeEnvelopeScope::DirectMessage,
        message_id: Uuid::from_u128(78),
    };
    let mut rng = rng_from_seed([9_u8; 32]);

    let err = seal_reliable_payload(&alice, bob.public_keys(), context, b"group key", &mut rng)
        .unwrap_err();

    assert!(matches!(
        err,
        SecurityError::ContextMemberMismatch {
            member_role: ContextMemberRole::Sender,
            ..
        }
    ));
}

#[test]
fn reliable_payload_seal_rejects_context_recipient_key_mismatch() {
    let alice = local_member("alice", ALICE_SEED);
    let bob = local_member("bob", BOB_SEED);
    let context = ReliablePayloadContext {
        frame_kind: "runtime-message",
        sender: alice.member_id(),
        recipient: alice.member_id(),
        scope: HpkeEnvelopeScope::DirectMessage,
        message_id: Uuid::from_u128(81),
    };
    let mut rng = rng_from_seed([9_u8; 32]);

    let err = seal_reliable_payload(&alice, bob.public_keys(), context, b"group key", &mut rng)
        .unwrap_err();

    assert!(matches!(
        err,
        SecurityError::ContextMemberMismatch {
            member_role: ContextMemberRole::Recipient,
            ..
        }
    ));
}

#[test]
fn reliable_payload_open_rejects_context_sender_key_mismatch() {
    let alice = local_member("alice", ALICE_SEED);
    let bob = local_member("bob", BOB_SEED);
    let context = ReliablePayloadContext {
        frame_kind: "runtime-message",
        sender: alice.member_id(),
        recipient: bob.member_id(),
        scope: HpkeEnvelopeScope::DirectMessage,
        message_id: Uuid::from_u128(79),
    };
    let mut rng = rng_from_seed([9_u8; 32]);
    let sealed =
        seal_reliable_payload(&alice, bob.public_keys(), context, b"group key", &mut rng).unwrap();
    let mismatched_context = ReliablePayloadContext {
        sender: bob.member_id(),
        ..context
    };

    let err =
        open_reliable_payload(alice.public_keys(), &bob, mismatched_context, &sealed).unwrap_err();

    assert!(matches!(
        err,
        SecurityError::ContextMemberMismatch {
            member_role: ContextMemberRole::Sender,
            ..
        }
    ));
}

#[test]
fn reliable_payload_open_rejects_context_recipient_key_mismatch() {
    let alice = local_member("alice", ALICE_SEED);
    let bob = local_member("bob", BOB_SEED);
    let context = ReliablePayloadContext {
        frame_kind: "runtime-message",
        sender: alice.member_id(),
        recipient: bob.member_id(),
        scope: HpkeEnvelopeScope::DirectMessage,
        message_id: Uuid::from_u128(80),
    };
    let mut rng = rng_from_seed([9_u8; 32]);
    let sealed =
        seal_reliable_payload(&alice, bob.public_keys(), context, b"group key", &mut rng).unwrap();
    let mismatched_context = ReliablePayloadContext {
        recipient: alice.member_id(),
        ..context
    };

    let err =
        open_reliable_payload(alice.public_keys(), &bob, mismatched_context, &sealed).unwrap_err();

    assert!(matches!(
        err,
        SecurityError::ContextMemberMismatch {
            member_role: ContextMemberRole::Recipient,
            ..
        }
    ));
}

#[test]
fn reliable_payload_open_fails_when_signature_changes() {
    let alice = local_member("alice", ALICE_SEED);
    let bob = local_member("bob", BOB_SEED);
    let context = ReliablePayloadContext {
        frame_kind: "runtime-message",
        sender: alice.member_id(),
        recipient: bob.member_id(),
        scope: HpkeEnvelopeScope::DirectMessage,
        message_id: Uuid::from_u128(77),
    };
    let mut rng = rng_from_seed([9_u8; 32]);
    let mut sealed =
        seal_reliable_payload(&alice, bob.public_keys(), context, b"group key", &mut rng).unwrap();
    sealed.signature[0] ^= 0x01;

    let err = open_reliable_payload(alice.public_keys(), &bob, context, &sealed).unwrap_err();

    assert!(matches!(
        err,
        SecurityError::InvalidSignatureBytes { .. } | SecurityError::VerifySignature { .. }
    ));
}

#[test]
fn reliable_payload_open_fails_when_ciphertext_changes() {
    let alice = local_member("alice", ALICE_SEED);
    let bob = local_member("bob", BOB_SEED);
    let context = ReliablePayloadContext {
        frame_kind: "runtime-message",
        sender: alice.member_id(),
        recipient: bob.member_id(),
        scope: HpkeEnvelopeScope::DirectMessage,
        message_id: Uuid::from_u128(77),
    };
    let mut rng = rng_from_seed([9_u8; 32]);
    let mut sealed =
        seal_reliable_payload(&alice, bob.public_keys(), context, b"group key", &mut rng).unwrap();
    sealed.ciphertext[0] ^= 0x01;

    let err = open_reliable_payload(alice.public_keys(), &bob, context, &sealed).unwrap_err();

    assert!(matches!(err, SecurityError::VerifySignature { .. }));
}
