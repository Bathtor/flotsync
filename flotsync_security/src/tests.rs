use crate::{
    FrameSignature,
    GroupKey,
    GroupMessageContext,
    HpkeCiphertext,
    LocalMemberKeys,
    MemberIdentity,
    ReliablePayloadContext,
    SecurityError,
    SignedFrameParts,
    StoreSecretContext,
    StoreSecretCryptoVersion,
    StoreSecretKey,
    hpke_open,
    hpke_seal,
    identity::{MEMBER_KEY_SEED_LENGTH, generate_member_key_files_from_seed},
    local_member_keys_from_jwks,
    open_group_message,
    open_reliable_payload,
    open_store_secret,
    public_member_keys_from_jwks,
    seal_group_message,
    seal_reliable_payload,
    seal_store_secret_for_test,
    sign_frame,
    test_support::rng_from_seed,
    verify_frame_signature,
};
use flotsync_core::member::Identifier;
use jose_jwk::{JwkSet, Operations};
use std::collections::BTreeSet;
use uuid::Uuid;

const ALICE_SEED: [u8; MEMBER_KEY_SEED_LENGTH] = [1u8; MEMBER_KEY_SEED_LENGTH];
const BOB_SEED: [u8; MEMBER_KEY_SEED_LENGTH] = [2u8; MEMBER_KEY_SEED_LENGTH];
const FRAME_KIND: &str = "group-message";
const PUBLIC_HEADER: &[u8] = b"{\"sender\":\"alice\"}";
const CIPHERTEXT: &[u8] = b"ciphertext-with-tag";

fn local_member(name: &str, seed: [u8; MEMBER_KEY_SEED_LENGTH]) -> LocalMemberKeys {
    let member = member(name);
    let generated = generate_member_key_files_from_seed(member.clone(), &seed).unwrap();
    local_member_keys_from_jwks(generated.local_private_jwks.as_str(), Some(&member)).unwrap()
}

fn member(name: &str) -> MemberIdentity {
    Identifier::from_array([name, "laptop"])
}

fn key_ops(jwks: &JwkSet, kid: &str) -> BTreeSet<Operations> {
    jwks.keys
        .iter()
        .find(|jwk| jwk.prm.kid.as_deref() == Some(kid))
        .unwrap()
        .prm
        .ops
        .clone()
        .unwrap()
}

fn signed_frame_fixture() -> (LocalMemberKeys, FrameSignature) {
    let alice = local_member("alice", ALICE_SEED);
    let signature = sign_frame(
        &alice,
        SignedFrameParts {
            frame_kind: FRAME_KIND,
            public_header: PUBLIC_HEADER,
            ciphertext: CIPHERTEXT,
        },
    )
    .unwrap();
    (alice, signature)
}

#[test]
fn generated_key_files_parse_back_to_member_keys() {
    let alice = member("alice");
    let generated = generate_member_key_files_from_seed(alice.clone(), &ALICE_SEED).unwrap();

    let local =
        local_member_keys_from_jwks(generated.local_private_jwks.as_str(), Some(&alice)).unwrap();
    let public = public_member_keys_from_jwks(&generated.public_jwks, Some(&alice)).unwrap();

    assert_eq!(local.member_id(), &alice);
    assert_eq!(public.member_id(), &alice);
    assert_eq!(local.public_keys(), &public);
}

#[test]
fn rejects_key_with_wrong_role_suffix() {
    let alice = member("alice");
    let generated = generate_member_key_files_from_seed(alice, &ALICE_SEED).unwrap();
    let invalid = generated.public_jwks.replace("#sign", "#enc");

    let err = public_member_keys_from_jwks(&invalid, None).unwrap_err();

    assert!(matches!(err, SecurityError::KeyRoleMismatch { .. }));
}

#[test]
fn generated_key_files_use_scope_appropriate_key_ops() {
    let alice = member("alice");
    let generated = generate_member_key_files_from_seed(alice, &ALICE_SEED).unwrap();
    let public_jwks = serde_json::from_str::<JwkSet>(&generated.public_jwks).unwrap();
    let private_jwks =
        serde_json::from_str::<JwkSet>(generated.local_private_jwks.as_str()).unwrap();

    assert_eq!(
        key_ops(&public_jwks, "alice.laptop#sign"),
        BTreeSet::from([Operations::Verify])
    );
    assert_eq!(
        key_ops(&public_jwks, "alice.laptop#enc"),
        BTreeSet::from([Operations::Encrypt])
    );
    assert_eq!(
        key_ops(&private_jwks, "alice.laptop#sign"),
        BTreeSet::from([Operations::Sign])
    );
    assert_eq!(
        key_ops(&private_jwks, "alice.laptop#enc"),
        BTreeSet::from([Operations::Decrypt])
    );
}

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

    assert_eq!(plaintext, b"hello group");
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
    let mut ciphertext = seal_group_message(&key, context, public_header, b"hello group").unwrap();
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

#[test]
fn hpke_round_trips_to_recipient() {
    let alice = local_member("alice", ALICE_SEED);
    let mut rng = rng_from_seed([9u8; 32]);
    let sealed = hpke_seal(
        alice.public_keys(),
        b"group-bootstrap",
        b"metadata",
        b"group secret",
        &mut rng,
    )
    .unwrap();

    let opened = hpke_open(&alice, b"group-bootstrap", b"metadata", &sealed).unwrap();

    assert_eq!(opened, b"group secret");
}

#[test]
fn hpke_decrypt_fails_for_wrong_recipient() {
    let alice = local_member("alice", ALICE_SEED);
    let bob = local_member("bob", BOB_SEED);
    let mut rng = rng_from_seed([9u8; 32]);
    let sealed = hpke_seal(
        alice.public_keys(),
        b"group-bootstrap",
        b"metadata",
        b"group secret",
        &mut rng,
    )
    .unwrap();

    let err = hpke_open(&bob, b"group-bootstrap", b"metadata", &sealed).unwrap_err();

    assert!(matches!(err, SecurityError::HpkeOpen { .. }));
}

#[test]
fn hpke_ciphertext_parts_reconstruct_wire_material() {
    let alice = local_member("alice", ALICE_SEED);
    let mut rng = rng_from_seed([9u8; 32]);
    let sealed = hpke_seal(
        alice.public_keys(),
        b"group-bootstrap",
        b"metadata",
        b"group secret",
        &mut rng,
    )
    .unwrap();
    let (encapsulated_key, ciphertext) = sealed.into_parts();
    let reconstructed = HpkeCiphertext::from_parts(encapsulated_key, ciphertext);

    let opened = hpke_open(&alice, b"group-bootstrap", b"metadata", &reconstructed).unwrap();

    assert_eq!(opened, b"group secret");
    assert_eq!(reconstructed.encapsulated_key(), &encapsulated_key);
    assert!(!reconstructed.ciphertext().is_empty());
}

#[test]
fn hpke_decrypt_fails_when_encapsulated_key_changes() {
    let alice = local_member("alice", ALICE_SEED);
    let mut rng = rng_from_seed([9u8; 32]);
    let sealed = hpke_seal(
        alice.public_keys(),
        b"group-bootstrap",
        b"metadata",
        b"group secret",
        &mut rng,
    )
    .unwrap();
    let (mut encapsulated_key, ciphertext) = sealed.into_parts();
    encapsulated_key[0] ^= 0x01;
    let tampered = HpkeCiphertext::from_parts(encapsulated_key, ciphertext);

    let err = hpke_open(&alice, b"group-bootstrap", b"metadata", &tampered).unwrap_err();

    assert!(matches!(
        err,
        SecurityError::HpkeKeyDecode { .. } | SecurityError::HpkeOpen { .. }
    ));
}

#[test]
fn store_secret_round_trips_with_logical_context() {
    let key = StoreSecretKey::from_bytes([3_u8; 32]);
    let context = StoreSecretContext {
        table: "replication_group",
        column: "group_secret",
        row_id: b"group-1",
        key_id: "local-test-key",
        crypto_version: StoreSecretCryptoVersion::V1,
    };
    let sealed = seal_store_secret_for_test(&key, context, b"group key", [4_u8; 24]).unwrap();

    let opened = open_store_secret(&key, context, &sealed).unwrap();

    assert_eq!(opened, b"group key");
}

#[test]
fn store_secret_open_fails_when_context_changes() {
    let key = StoreSecretKey::from_bytes([3_u8; 32]);
    let context = StoreSecretContext {
        table: "replication_group",
        column: "group_secret",
        row_id: b"group-1",
        key_id: "local-test-key",
        crypto_version: StoreSecretCryptoVersion::V1,
    };
    let sealed = seal_store_secret_for_test(&key, context, b"group key", [4_u8; 24]).unwrap();
    let tampered_context = StoreSecretContext {
        row_id: b"group-2",
        ..context
    };

    let err = open_store_secret(&key, tampered_context, &sealed).unwrap_err();

    assert!(matches!(err, SecurityError::StoreSecretOpen));
}

#[test]
fn reliable_payload_round_trips_to_recipient() {
    let alice = local_member("alice", ALICE_SEED);
    let bob = local_member("bob", BOB_SEED);
    let context = ReliablePayloadContext {
        frame_kind: "runtime-message",
        sender: alice.member_id(),
        recipient: bob.member_id(),
        message_id: Uuid::from_u128(77),
    };
    let mut rng = rng_from_seed([9_u8; 32]);

    let sealed =
        seal_reliable_payload(&alice, bob.public_keys(), context, b"group key", &mut rng).unwrap();
    let opened = open_reliable_payload(alice.public_keys(), &bob, context, &sealed).unwrap();

    assert_eq!(opened, b"group key");
}

#[test]
fn reliable_payload_open_fails_when_signature_changes() {
    let alice = local_member("alice", ALICE_SEED);
    let bob = local_member("bob", BOB_SEED);
    let context = ReliablePayloadContext {
        frame_kind: "runtime-message",
        sender: alice.member_id(),
        recipient: bob.member_id(),
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
        message_id: Uuid::from_u128(77),
    };
    let mut rng = rng_from_seed([9_u8; 32]);
    let mut sealed =
        seal_reliable_payload(&alice, bob.public_keys(), context, b"group key", &mut rng).unwrap();
    sealed.ciphertext[0] ^= 0x01;

    let err = open_reliable_payload(alice.public_keys(), &bob, context, &sealed).unwrap_err();

    assert!(matches!(err, SecurityError::VerifySignature { .. }));
}
