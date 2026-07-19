//! Tests for signed frames and group payload encryption.

use super::{fixtures::*, *};

#[test]
fn signature_verification_fails_when_ciphertext_changes() {
    let (alice, signature) = signed_frame_fixture();

    let err = verify_frame_signature(
        alice.public_keys(),
        SignedFrameParts {
            frame_kind: FRAME_KIND,
            public_header: PUBLIC_HEADER,
            ciphertext: b"tampered",
        },
        &signature,
    )
    .unwrap_err();

    assert!(matches!(err, SecurityError::VerifySignature { .. }));
}

#[test]
fn signature_verification_fails_when_frame_kind_changes() {
    let (alice, signature) = signed_frame_fixture();

    let err = verify_frame_signature(
        alice.public_keys(),
        SignedFrameParts {
            frame_kind: "bootstrap-message",
            public_header: PUBLIC_HEADER,
            ciphertext: CIPHERTEXT,
        },
        &signature,
    )
    .unwrap_err();

    assert!(matches!(err, SecurityError::VerifySignature { .. }));
}

#[test]
fn signature_verification_fails_when_public_header_changes() {
    let (alice, signature) = signed_frame_fixture();

    let err = verify_frame_signature(
        alice.public_keys(),
        SignedFrameParts {
            frame_kind: FRAME_KIND,
            public_header: b"{\"sender\":\"mallory\"}",
            ciphertext: CIPHERTEXT,
        },
        &signature,
    )
    .unwrap_err();

    assert!(matches!(err, SecurityError::VerifySignature { .. }));
}

#[test]
fn frame_signature_decodes_from_proto_view() {
    let (_alice, signature) = signed_frame_fixture();
    let payload = signature.encode_proto().encode_to_bytes();
    let view = DiscoverySignatureView::decode_view(&payload).expect("signature view should decode");

    let decoded = FrameSignature::decode_proto_view(&view).expect("signature view should convert");

    assert_eq!(decoded, signature);
}

#[test]
fn group_message_round_trips() {
    let alice = member("alice");
    let key = GroupKey::from_bytes([7u8; 32]);
    let group_id = Uuid::from_u128(10);
    let message_id = Uuid::from_u128(20);
    let context = GroupMessageContext {
        group_id,
        frame_kind: "group-message",
        sender: &alice,
        message_id,
    };
    let public_header = b"{\"group\":\"10\"}";

    let ciphertext = seal_group_message(&key, context, public_header, b"hello group").unwrap();
    let plaintext = open_group_message(&key, context, public_header, &ciphertext).unwrap();

    assert_eq!(plaintext.as_ref(), b"hello group");
}

#[test]
fn signed_group_payload_round_trips() {
    let alice = local_member("alice", ALICE_SEED);
    let key = GroupKey::from_bytes([7u8; 32]);
    let context = GroupMessageContext {
        group_id: Uuid::from_u128(10),
        frame_kind: "group-message",
        sender: alice.member_id(),
        message_id: Uuid::from_u128(20),
    };
    let public_header = b"{\"group\":\"10\"}";

    let sealed = seal_group_payload(&alice, &key, context, public_header, b"hello group").unwrap();
    let plaintext =
        open_group_payload(alice.public_keys(), &key, context, public_header, &sealed).unwrap();

    assert_eq!(plaintext.as_ref(), b"hello group");
}

#[test]
fn signed_group_payload_rejects_signature_tampering() {
    let alice = local_member("alice", ALICE_SEED);
    let key = GroupKey::from_bytes([7u8; 32]);
    let context = GroupMessageContext {
        group_id: Uuid::from_u128(10),
        frame_kind: "group-message",
        sender: alice.member_id(),
        message_id: Uuid::from_u128(20),
    };
    let public_header = b"{\"group\":\"10\"}";
    let mut sealed =
        seal_group_payload(&alice, &key, context, public_header, b"hello group").unwrap();
    sealed.signature[0] ^= 0x01;

    let err =
        open_group_payload(alice.public_keys(), &key, context, public_header, &sealed).unwrap_err();

    assert!(matches!(err, SecurityError::VerifySignature { .. }));
}

#[test]
fn signed_group_payload_rejects_ciphertext_tampering() {
    let alice = local_member("alice", ALICE_SEED);
    let key = GroupKey::from_bytes([7u8; 32]);
    let context = GroupMessageContext {
        group_id: Uuid::from_u128(10),
        frame_kind: "group-message",
        sender: alice.member_id(),
        message_id: Uuid::from_u128(20),
    };
    let public_header = b"{\"group\":\"10\"}";
    let mut sealed =
        seal_group_payload(&alice, &key, context, public_header, b"hello group").unwrap();
    let mut ciphertext = sealed.ciphertext.to_vec();
    ciphertext[0] ^= 0x01;
    sealed.ciphertext = Bytes::from(ciphertext);

    let err =
        open_group_payload(alice.public_keys(), &key, context, public_header, &sealed).unwrap_err();

    assert!(matches!(err, SecurityError::VerifySignature { .. }));
}

#[test]
fn signed_group_payload_rejects_public_header_tampering() {
    let alice = local_member("alice", ALICE_SEED);
    let key = GroupKey::from_bytes([7u8; 32]);
    let context = GroupMessageContext {
        group_id: Uuid::from_u128(10),
        frame_kind: "group-message",
        sender: alice.member_id(),
        message_id: Uuid::from_u128(20),
    };
    let sealed =
        seal_group_payload(&alice, &key, context, b"{\"group\":\"10\"}", b"hello group").unwrap();

    let err = open_group_payload(
        alice.public_keys(),
        &key,
        context,
        b"{\"group\":\"11\"}",
        &sealed,
    )
    .unwrap_err();

    assert!(matches!(err, SecurityError::VerifySignature { .. }));
}

#[test]
fn signed_group_payload_rejects_group_context_tampering() {
    let alice = local_member("alice", ALICE_SEED);
    let key = GroupKey::from_bytes([7u8; 32]);
    let context = GroupMessageContext {
        group_id: Uuid::from_u128(10),
        frame_kind: "group-message",
        sender: alice.member_id(),
        message_id: Uuid::from_u128(20),
    };
    let public_header = b"{\"group\":\"10\"}";
    let sealed = seal_group_payload(&alice, &key, context, public_header, b"hello group").unwrap();
    let tampered_context = GroupMessageContext {
        group_id: Uuid::from_u128(11),
        frame_kind: "group-message",
        sender: alice.member_id(),
        message_id: Uuid::from_u128(20),
    };

    let err = open_group_payload(
        alice.public_keys(),
        &key,
        tampered_context,
        public_header,
        &sealed,
    )
    .unwrap_err();

    assert!(matches!(err, SecurityError::GroupOpen));
}

#[test]
fn stored_group_secret_plaintext_decodes_current_suite() {
    let key = GroupKey::from_bytes([9u8; GROUP_KEY_LENGTH]);
    let plaintext = key.stored_secret_plaintext();

    let decoded = group_key_from_stored_secret_plaintext(plaintext.as_slice()).unwrap();

    assert_eq!(decoded, key);
}

#[test]
fn stored_group_secret_plaintext_rejects_unsupported_suite() {
    let mut plaintext = Vec::with_capacity(2 + GROUP_KEY_LENGTH);
    let unsupported_suite = GROUP_CIPHER_SUITE_CHACHA20_POLY1305
        .as_u16()
        .wrapping_add(1);
    plaintext.extend_from_slice(&unsupported_suite.to_be_bytes());
    plaintext.extend_from_slice(&[9u8; GROUP_KEY_LENGTH]);

    let err = group_key_from_stored_secret_plaintext(&plaintext).unwrap_err();

    assert!(matches!(
        err,
        SecurityError::UnsupportedGroupCipherSuite { .. }
    ));
}

#[test]
fn stored_group_secret_plaintext_rejects_malformed_length() {
    let plaintext = [9u8; GROUP_KEY_LENGTH];

    let err = group_key_from_stored_secret_plaintext(&plaintext).unwrap_err();

    assert!(matches!(err, SecurityError::StoredGroupSecretLength { .. }));
}

#[test]
fn group_context_tampering_fails_to_open() {
    let alice = member("alice");
    let key = GroupKey::from_bytes([7u8; 32]);
    let group_id = Uuid::from_u128(10);
    let context = GroupMessageContext {
        group_id,
        frame_kind: "group-message",
        sender: &alice,
        message_id: Uuid::from_u128(20),
    };
    let public_header = b"{\"group\":\"10\"}";
    let ciphertext = seal_group_message(&key, context, public_header, b"hello group").unwrap();
    let tampered_context = GroupMessageContext {
        group_id,
        frame_kind: "group-message",
        sender: &alice,
        message_id: Uuid::from_u128(21),
    };

    let err = open_group_message(&key, tampered_context, public_header, &ciphertext).unwrap_err();

    assert!(matches!(err, SecurityError::GroupOpen));
}

#[test]
fn group_ciphertext_tampering_fails_to_open() {
    let alice = member("alice");
    let key = GroupKey::from_bytes([7u8; 32]);
    let context = GroupMessageContext {
        group_id: Uuid::from_u128(10),
        frame_kind: "group-message",
        sender: &alice,
        message_id: Uuid::from_u128(20),
    };
    let public_header = b"{\"group\":\"10\"}";
    let ciphertext = seal_group_message(&key, context, public_header, b"hello group").unwrap();
    let mut ciphertext = ciphertext.to_vec();
    ciphertext[0] ^= 0x01;

    let err = open_group_message(&key, context, public_header, &ciphertext).unwrap_err();

    assert!(matches!(err, SecurityError::GroupOpen));
}

#[test]
fn wrong_public_header_fails_to_open() {
    let alice = member("alice");
    let key = GroupKey::from_bytes([7u8; 32]);
    let context = GroupMessageContext {
        group_id: Uuid::from_u128(10),
        frame_kind: "group-message",
        sender: &alice,
        message_id: Uuid::from_u128(20),
    };
    let ciphertext =
        seal_group_message(&key, context, b"{\"group\":\"10\"}", b"hello group").unwrap();

    let err = open_group_message(&key, context, b"{\"group\":\"11\"}", &ciphertext).unwrap_err();

    assert!(matches!(err, SecurityError::GroupOpen));
}
