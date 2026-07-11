use crate::{
    ContextMemberRole,
    FrameSignature,
    GROUP_CIPHER_SUITE_CHACHA20_POLY1305,
    GROUP_KEY_LENGTH,
    GroupKey,
    GroupMessageContext,
    HpkeCiphertext,
    HpkeContext,
    HpkeEnvelopePurpose,
    HpkeEnvelopeScope,
    KEY_FINGERPRINT_LENGTH,
    KeyFingerprint,
    KeyFingerprintParseError,
    LocalMemberKeys,
    LocalStoreSecretError,
    LocalStoreSecretProfile,
    MemberIdentity,
    PublicKeyBundle,
    ReliablePayloadContext,
    SecurityError,
    SignedFrameParts,
    StoreSecretContext,
    StoreSecretCryptoVersion,
    StoreSecretKey,
    encode_public_key_bundle,
    group_key_from_stored_secret_plaintext,
    hpke_open,
    hpke_seal,
    identity::{MEMBER_KEY_SEED_LENGTH, generate_member_key_bundles_from_seed},
    install_local_store_secret_test_store,
    load_local_store_secret,
    load_or_create_local_store_secret,
    local_member_keys_from_private_bundle,
    open_group_message,
    open_group_payload,
    open_reliable_payload,
    open_store_secret,
    public_member_keys_from_public_bundle,
    seal_group_message,
    seal_group_payload,
    seal_reliable_payload,
    seal_store_secret_for_test,
    sign_frame,
    test_support::rng_from_seed,
    verify_frame_signature,
};
use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, URL_SAFE},
};
use bytes::Bytes;
use flotsync_core::{GroupId, member::Identifier};
use flotsync_messages::{
    buffa::{EnumValue, Message as _, MessageField, MessageView as _},
    discovery::DiscoverySignatureView,
    proto::{DecodeProtoView, EncodeProto},
    security as security_proto,
};
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

const ALICE_SEED: [u8; MEMBER_KEY_SEED_LENGTH] = [1u8; MEMBER_KEY_SEED_LENGTH];
const BOB_SEED: [u8; MEMBER_KEY_SEED_LENGTH] = [2u8; MEMBER_KEY_SEED_LENGTH];
const FRAME_KIND: &str = "group-message";
const PUBLIC_HEADER: &[u8] = b"{\"sender\":\"alice\"}";
const CIPHERTEXT: &[u8] = b"ciphertext-with-tag";
const STORE_SECRET_TEST_KEY_ID: Uuid = Uuid::from_u128(0x100);

fn local_member(name: &str, seed: [u8; MEMBER_KEY_SEED_LENGTH]) -> LocalMemberKeys {
    let member = member(name);
    let generated = generate_member_key_bundles_from_seed(member.clone(), &seed);
    local_member_keys_from_private_bundle(generated.local_private_bundle.as_bytes(), member)
        .unwrap()
}

fn member(name: &str) -> MemberIdentity {
    Identifier::from_array([name, "laptop"])
}

fn unique_local_store_secret_profile(label: &str) -> LocalStoreSecretProfile {
    static NEXT_PROFILE: AtomicU64 = AtomicU64::new(1);
    let index = NEXT_PROFILE.fetch_add(1, Ordering::Relaxed);
    LocalStoreSecretProfile::new(format!("{label}-{index}")).unwrap()
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

fn direct_hpke_context<'a>(
    sender: &'a MemberIdentity,
    recipient: &'a MemberIdentity,
    delivery_message_id: Uuid,
    authenticated_public_metadata: &'a [u8],
) -> HpkeContext<'a> {
    HpkeContext {
        purpose: HpkeEnvelopePurpose::ReliablePayload,
        sender,
        recipient,
        scope: HpkeEnvelopeScope::DirectMessage,
        delivery_message_id,
        authenticated_public_metadata,
    }
}

fn group_hpke_context<'a>(
    sender: &'a MemberIdentity,
    recipient: &'a MemberIdentity,
    group_id: GroupId,
    delivery_message_id: Uuid,
    authenticated_public_metadata: &'a [u8],
) -> HpkeContext<'a> {
    HpkeContext {
        purpose: HpkeEnvelopePurpose::ReliablePayload,
        sender,
        recipient,
        scope: HpkeEnvelopeScope::Group { group_id },
        delivery_message_id,
        authenticated_public_metadata,
    }
}

#[test]
fn generated_key_bundles_decode_back_to_member_keys() {
    let alice = member("alice");
    let generated = generate_member_key_bundles_from_seed(alice.clone(), &ALICE_SEED);

    let local = local_member_keys_from_private_bundle(
        generated.local_private_bundle.as_bytes(),
        alice.clone(),
    )
    .unwrap();
    let public =
        public_member_keys_from_public_bundle(&generated.public_bundle, alice.clone()).unwrap();

    assert_eq!(local.member_id(), &alice);
    assert_eq!(public.member_id(), &alice);
    assert_eq!(local.public_keys(), &public);
}

#[test]
fn generated_public_key_bundle_is_identity_free() {
    let alice = member("alice");
    let bob = member("bob");
    let generated = generate_member_key_bundles_from_seed(alice, &ALICE_SEED);

    let decoded =
        public_member_keys_from_public_bundle(&generated.public_bundle, bob.clone()).unwrap();

    assert_eq!(decoded.member_id(), &bob);
}

#[test]
fn rejects_malformed_key_bundle_bytes() {
    let err = public_member_keys_from_public_bundle(b"not protobuf", member("alice")).unwrap_err();

    assert!(matches!(err, SecurityError::DecodeKeyBundle { .. }));
}

#[test]
fn rejects_unsupported_key_bundle_version() {
    let alice = member("alice");
    let generated = generate_member_key_bundles_from_seed(alice.clone(), &ALICE_SEED);
    let public = public_member_keys_from_public_bundle(&generated.public_bundle, alice).unwrap();
    let mut proto = public.encode_proto();
    proto.format_version = 999;

    let err =
        public_member_keys_from_public_bundle(&proto.encode_to_vec(), member("alice")).unwrap_err();

    assert!(matches!(
        err,
        SecurityError::UnsupportedKeyBundleVersion { actual: 999, .. }
    ));
}

#[test]
fn rejects_unknown_public_key_scheme() {
    let alice = member("alice");
    let generated = generate_member_key_bundles_from_seed(alice.clone(), &ALICE_SEED);
    let public = public_member_keys_from_public_bundle(&generated.public_bundle, alice).unwrap();
    let mut proto = public.encode_proto();
    let mut signing_key = proto.signing_key.take().expect("signing key should exist");
    signing_key.scheme = EnumValue::from(404);
    proto.signing_key = MessageField::some(signing_key);

    let err =
        public_member_keys_from_public_bundle(&proto.encode_to_vec(), member("alice")).unwrap_err();

    assert!(matches!(
        err,
        SecurityError::UnknownKeyScheme {
            role: crate::KeyRole::Signing,
            value: 404,
        }
    ));
}

#[test]
fn rejects_public_private_key_pair_mismatch() {
    let alice = member("alice");
    let generated = generate_member_key_bundles_from_seed(alice.clone(), &ALICE_SEED);
    let mut proto = security_proto::LocalPrivateKeyBundle::decode_from_slice(
        generated.local_private_bundle.as_bytes(),
    )
    .unwrap();
    let mut signing_key = proto.signing_key.take().expect("signing key should exist");
    signing_key.public_key = [9u8; 32].to_vec();
    proto.signing_key = MessageField::some(signing_key);

    let err = local_member_keys_from_private_bundle(&proto.encode_to_vec(), alice).unwrap_err();

    assert!(matches!(
        err,
        SecurityError::KeyPairMismatch {
            role: crate::KeyRole::Signing,
        }
    ));
}

#[test]
fn public_key_bundle_encoding_roundtrips_public_keys() {
    let alice = member("alice");
    let local = local_member("alice", ALICE_SEED);
    let bundle = encode_public_key_bundle(local.public_keys());

    let public = public_member_keys_from_public_bundle(&bundle, alice.clone()).unwrap();

    assert_eq!(public.member_id(), &alice);
    assert_eq!(&public, local.public_keys());
}

#[test]
fn identity_free_public_key_bundle_roundtrips_bytes() {
    let local = local_member("alice", ALICE_SEED);
    let bundle = PublicKeyBundle::from_public_member_keys(local.public_keys());
    let encoded = bundle.to_bytes();

    let decoded = PublicKeyBundle::from_bytes(&encoded).unwrap();

    assert_eq!(decoded, bundle);
    assert_eq!(decoded.fingerprint(), local.public_keys().fingerprint());
}

#[test]
fn identity_free_public_key_bundle_binds_to_explicit_member() {
    let local = local_member("alice", ALICE_SEED);
    let bundle = PublicKeyBundle::from_public_member_keys(local.public_keys());
    let bob = member("bob");

    let public_keys = bundle.bind_member(bob.clone());

    assert_eq!(public_keys.member_id(), &bob);
    assert_eq!(public_keys.fingerprint(), local.public_keys().fingerprint());
}

#[test]
fn pasteable_public_key_bundle_roundtrips() {
    let local = local_member("alice", ALICE_SEED);
    let bundle = local.public_keys().public_key_bundle();

    let pasteable = bundle.to_pasteable_string();
    let decoded = PublicKeyBundle::from_pasteable_string(&pasteable).unwrap();

    assert_eq!(pasteable, STANDARD.encode(bundle.to_bytes()));
    assert!(!pasteable.starts_with("flotsync-key-v1"));
    assert_eq!(decoded, bundle);
}

#[test]
fn pasteable_public_key_bundle_rejects_malformed_base64() {
    let err = PublicKeyBundle::from_pasteable_string("not base64!").unwrap_err();

    assert!(matches!(
        err,
        SecurityError::DecodePasteablePublicKeyBundle { .. }
    ));
}

#[test]
fn public_key_fingerprint_survives_public_bundle_roundtrip() {
    let alice = member("alice");
    let local = local_member("alice", ALICE_SEED);
    let bundle = encode_public_key_bundle(local.public_keys());

    let public = public_member_keys_from_public_bundle(&bundle, alice).unwrap();

    assert_eq!(public.fingerprint(), local.public_keys().fingerprint());
}

#[test]
fn public_key_fingerprint_is_identity_free() {
    let alice = local_member("alice", ALICE_SEED);
    let rebound = crate::PublicMemberKeys::from_key_bytes(
        member("bob"),
        alice.public_keys().signing_key_bytes(),
        alice.public_keys().encryption_key_bytes(),
    )
    .unwrap();

    assert_eq!(rebound.fingerprint(), alice.public_keys().fingerprint());
}

#[test]
fn public_key_fingerprint_changes_with_signing_key_material() {
    let alice = local_member("alice", ALICE_SEED);
    let bob = local_member("bob", BOB_SEED);
    let mixed = crate::PublicMemberKeys::from_key_bytes(
        member("alice"),
        bob.public_keys().signing_key_bytes(),
        alice.public_keys().encryption_key_bytes(),
    )
    .unwrap();

    assert_ne!(mixed.fingerprint(), alice.public_keys().fingerprint());
}

#[test]
fn public_key_fingerprint_changes_with_encryption_key_material() {
    let alice = local_member("alice", ALICE_SEED);
    let bob = local_member("bob", BOB_SEED);
    let mixed = crate::PublicMemberKeys::from_key_bytes(
        member("alice"),
        alice.public_keys().signing_key_bytes(),
        bob.public_keys().encryption_key_bytes(),
    )
    .unwrap();

    assert_ne!(mixed.fingerprint(), alice.public_keys().fingerprint());
}

#[test]
fn key_fingerprint_canonical_base64url_roundtrips() {
    let fingerprint = local_member("alice", ALICE_SEED)
        .public_keys()
        .fingerprint();

    let encoded = fingerprint.to_canonical_base64url();
    let parsed = KeyFingerprint::from_canonical_base64url(&encoded).unwrap();
    let parsed_from_str = encoded.parse::<KeyFingerprint>().unwrap();

    assert_eq!(encoded.len(), 44);
    assert!(encoded.ends_with('='));
    assert_eq!(parsed, fingerprint);
    assert_eq!(parsed_from_str, fingerprint);
    assert_eq!(fingerprint.to_canonical_base64url(), encoded);
}

#[test]
fn key_fingerprint_rejects_malformed_canonical_input() {
    let fingerprint = local_member("alice", ALICE_SEED)
        .public_keys()
        .fingerprint();
    let unpadded = fingerprint
        .to_canonical_base64url()
        .trim_end_matches('=')
        .to_owned();
    let mut invalid_base64 = fingerprint.to_canonical_base64url();
    invalid_base64.replace_range(0..1, "!");
    let short_bytes = URL_SAFE.encode([7u8; KEY_FINGERPRINT_LENGTH - 1]);

    assert!(matches!(
        KeyFingerprint::from_canonical_base64url(&unpadded),
        Err(KeyFingerprintParseError::InvalidTextLength { .. })
    ));
    assert!(matches!(
        KeyFingerprint::from_canonical_base64url(&invalid_base64),
        Err(KeyFingerprintParseError::InvalidCanonicalBase64Url { .. })
    ));
    assert!(matches!(
        KeyFingerprint::from_canonical_base64url(&short_bytes),
        Err(KeyFingerprintParseError::InvalidByteLength { .. })
    ));
}

#[test]
fn key_fingerprint_display_string_groups_unpadded_base64url() {
    let fingerprint = local_member("alice", ALICE_SEED)
        .public_keys()
        .fingerprint();

    let display = fingerprint.to_display_string();

    assert_eq!(
        display,
        "q6MZ-UqAf-CCRA-Mifp-LKvD-ohTf-JZ6a-Qi62-egmX-D_Sx-xX0"
    );
    assert!(display.parse::<KeyFingerprint>().is_err());
    assert_eq!(fingerprint.to_string(), display);
    assert!(format!("{fingerprint:?}").contains(&display));
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

#[test]
fn hpke_round_trips_to_recipient() {
    let alice = local_member("alice", ALICE_SEED);
    let sender = member("sender");
    let context = direct_hpke_context(
        &sender,
        alice.member_id(),
        Uuid::from_u128(501),
        b"metadata",
    );
    let mut rng = rng_from_seed([9u8; 32]);
    let sealed = hpke_seal(alice.public_keys(), context, b"group secret", &mut rng).unwrap();

    let opened = hpke_open(&alice, context, &sealed).unwrap();

    assert_eq!(opened, b"group secret");
}

#[test]
fn hpke_decrypt_fails_for_wrong_recipient() {
    let alice = local_member("alice", ALICE_SEED);
    let bob = local_member("bob", BOB_SEED);
    let sender = member("sender");
    let context = direct_hpke_context(
        &sender,
        alice.member_id(),
        Uuid::from_u128(502),
        b"metadata",
    );
    let mut rng = rng_from_seed([9u8; 32]);
    let sealed = hpke_seal(alice.public_keys(), context, b"group secret", &mut rng).unwrap();

    let err = hpke_open(&bob, context, &sealed).unwrap_err();

    assert!(matches!(
        err,
        SecurityError::ContextMemberMismatch {
            member_role: ContextMemberRole::Recipient,
            ..
        }
    ));
}

#[test]
fn hpke_seal_rejects_context_recipient_key_mismatch() {
    let alice = local_member("alice", ALICE_SEED);
    let bob = local_member("bob", BOB_SEED);
    let sender = member("sender");
    let context = direct_hpke_context(&sender, bob.member_id(), Uuid::from_u128(508), b"metadata");
    let mut rng = rng_from_seed([9u8; 32]);

    let err = hpke_seal(alice.public_keys(), context, b"group secret", &mut rng).unwrap_err();

    assert!(matches!(
        err,
        SecurityError::ContextMemberMismatch {
            member_role: ContextMemberRole::Recipient,
            ..
        }
    ));
}

#[test]
fn hpke_ciphertext_parts_reconstruct_wire_material() {
    let alice = local_member("alice", ALICE_SEED);
    let sender = member("sender");
    let context = direct_hpke_context(
        &sender,
        alice.member_id(),
        Uuid::from_u128(503),
        b"metadata",
    );
    let mut rng = rng_from_seed([9u8; 32]);
    let sealed = hpke_seal(alice.public_keys(), context, b"group secret", &mut rng).unwrap();
    let (encapsulated_key, ciphertext) = sealed.into_parts();
    let reconstructed = HpkeCiphertext::from_parts(encapsulated_key, ciphertext);

    let opened = hpke_open(&alice, context, &reconstructed).unwrap();

    assert_eq!(opened, b"group secret");
    assert_eq!(reconstructed.encapsulated_key(), &encapsulated_key);
    assert!(!reconstructed.ciphertext().is_empty());
}

#[test]
fn hpke_decrypt_fails_when_encapsulated_key_changes() {
    let alice = local_member("alice", ALICE_SEED);
    let sender = member("sender");
    let context = direct_hpke_context(
        &sender,
        alice.member_id(),
        Uuid::from_u128(504),
        b"metadata",
    );
    let mut rng = rng_from_seed([9u8; 32]);
    let sealed = hpke_seal(alice.public_keys(), context, b"group secret", &mut rng).unwrap();
    let (mut encapsulated_key, ciphertext) = sealed.into_parts();
    encapsulated_key[0] ^= 0x01;
    let tampered = HpkeCiphertext::from_parts(encapsulated_key, ciphertext);

    let err = hpke_open(&alice, context, &tampered).unwrap_err();

    assert!(matches!(
        err,
        SecurityError::HpkeKeyDecode { .. } | SecurityError::HpkeOpen { .. }
    ));
}

#[test]
fn hpke_decrypt_fails_when_context_changes() {
    let alice = local_member("alice", ALICE_SEED);
    let sender = member("sender");
    let other_sender = member("other-sender");
    let context = direct_hpke_context(
        &sender,
        alice.member_id(),
        Uuid::from_u128(505),
        b"metadata",
    );
    let mut rng = rng_from_seed([9u8; 32]);
    let sealed = hpke_seal(alice.public_keys(), context, b"group secret", &mut rng).unwrap();

    let mismatched_contexts = [
        HpkeContext {
            sender: &other_sender,
            ..context
        },
        HpkeContext {
            scope: HpkeEnvelopeScope::Group {
                group_id: GroupId(Uuid::from_u128(88)),
            },
            ..context
        },
        HpkeContext {
            delivery_message_id: Uuid::from_u128(506),
            ..context
        },
        HpkeContext {
            authenticated_public_metadata: b"other metadata",
            ..context
        },
    ];

    for mismatched_context in mismatched_contexts {
        let err = hpke_open(&alice, mismatched_context, &sealed).unwrap_err();
        assert!(matches!(err, SecurityError::HpkeOpen { .. }));
    }
}

#[test]
fn hpke_decrypt_fails_when_group_scope_changes() {
    let alice = local_member("alice", ALICE_SEED);
    let sender = member("sender");
    let context = group_hpke_context(
        &sender,
        alice.member_id(),
        GroupId(Uuid::from_u128(90)),
        Uuid::from_u128(507),
        b"metadata",
    );
    let mut rng = rng_from_seed([9u8; 32]);
    let sealed = hpke_seal(alice.public_keys(), context, b"group secret", &mut rng).unwrap();
    let mismatched_context = HpkeContext {
        scope: HpkeEnvelopeScope::Group {
            group_id: GroupId(Uuid::from_u128(91)),
        },
        ..context
    };

    let err = hpke_open(&alice, mismatched_context, &sealed).unwrap_err();

    assert!(matches!(err, SecurityError::HpkeOpen { .. }));
}

#[test]
fn store_secret_round_trips_with_logical_context() {
    let key = StoreSecretKey::from_bytes([3_u8; 32]);
    let context = StoreSecretContext {
        table: "replication_group",
        column: "group_secret",
        row_id: b"group-1",
        key_id: STORE_SECRET_TEST_KEY_ID.as_bytes(),
        crypto_version: StoreSecretCryptoVersion::V1,
    };
    let sealed = seal_store_secret_for_test(&key, context, b"group key", [4_u8; 24]).unwrap();

    let opened = open_store_secret(&key, context, &sealed).unwrap();

    assert_eq!(opened.as_slice(), b"group key");
}

#[test]
fn store_secret_open_fails_when_context_changes() {
    let key = StoreSecretKey::from_bytes([3_u8; 32]);
    let context = StoreSecretContext {
        table: "replication_group",
        column: "group_secret",
        row_id: b"group-1",
        key_id: STORE_SECRET_TEST_KEY_ID.as_bytes(),
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
fn local_store_secret_create_then_load_round_trips_key() {
    install_local_store_secret_test_store().unwrap();
    let application_id = Identifier::from_array(["flotsync", "security", "tests"]);
    let profile = unique_local_store_secret_profile("round-trip");

    let created = load_or_create_local_store_secret(&application_id, &profile).unwrap();
    let (key_id, created_key) = created.into_parts();
    assert_ne!(key_id.as_bytes(), Uuid::nil().as_bytes());

    let context = StoreSecretContext {
        table: "replication_group",
        column: "group_secret",
        row_id: b"group-1",
        key_id: key_id.as_bytes(),
        crypto_version: StoreSecretCryptoVersion::V1,
    };
    let sealed =
        seal_store_secret_for_test(&created_key, context, b"group key", [5_u8; 24]).unwrap();

    let loaded = load_local_store_secret(&application_id, &profile).unwrap();
    let (loaded_key_id, loaded_key) = loaded.into_parts();
    let opened = open_store_secret(&loaded_key, context, &sealed).unwrap();

    assert_eq!(loaded_key_id, key_id);
    assert_eq!(opened.as_slice(), b"group key");
}

#[test]
fn local_store_secret_profiles_are_isolated() {
    install_local_store_secret_test_store().unwrap();
    let application_id = Identifier::from_array(["flotsync", "security", "tests"]);
    let first_profile = unique_local_store_secret_profile("first");
    let second_profile = unique_local_store_secret_profile("second");

    let first = load_or_create_local_store_secret(&application_id, &first_profile).unwrap();
    let second = load_or_create_local_store_secret(&application_id, &second_profile).unwrap();

    assert_ne!(first.key_id(), second.key_id());
}

#[test]
fn local_store_secret_application_ids_are_isolated() {
    install_local_store_secret_test_store().unwrap();
    let first_application = Identifier::from_array(["flotsync", "security", "first"]);
    let second_application = Identifier::from_array(["flotsync", "security", "second"]);
    let profile = unique_local_store_secret_profile("same-profile");

    let first = load_or_create_local_store_secret(&first_application, &profile).unwrap();
    let second = load_or_create_local_store_secret(&second_application, &profile).unwrap();

    assert_ne!(first.key_id(), second.key_id());
}

#[test]
fn local_store_secret_load_reports_missing_profile() {
    install_local_store_secret_test_store().unwrap();
    let application_id = Identifier::from_array(["flotsync", "security", "tests"]);
    let profile = unique_local_store_secret_profile("missing");

    let err = load_local_store_secret(&application_id, &profile).unwrap_err();

    assert!(matches!(err, LocalStoreSecretError::Missing { .. }));
}

#[test]
fn local_store_secret_profile_rejects_empty_selector() {
    let err = LocalStoreSecretProfile::new("  ").unwrap_err();

    assert!(matches!(err, LocalStoreSecretError::EmptyProfile));
}

#[test]
fn local_store_secret_profile_rejects_surrounding_whitespace() {
    let err = LocalStoreSecretProfile::new(" dev").unwrap_err();

    assert!(matches!(
        err,
        LocalStoreSecretError::ProfileWhitespace { .. }
    ));
}

#[test]
fn local_store_secret_profile_rejects_non_portable_selector() {
    let err = LocalStoreSecretProfile::new("Dev").unwrap_err();

    assert!(matches!(
        err,
        LocalStoreSecretError::ProfileNotPortable { .. }
    ));
}

#[test]
fn local_store_secret_profile_rejects_account_separator() {
    let err = LocalStoreSecretProfile::new("dev/local").unwrap_err();

    assert!(matches!(
        err,
        LocalStoreSecretError::ProfileNotPortable { .. }
    ));
}

#[test]
fn local_store_secret_rejects_non_portable_application_id() {
    install_local_store_secret_test_store().unwrap();
    let application_id = Identifier::from_array(["Flotsync", "security", "tests"]);
    let profile = unique_local_store_secret_profile("application");

    let err = load_or_create_local_store_secret(&application_id, &profile).unwrap_err();

    assert!(matches!(
        err,
        LocalStoreSecretError::ApplicationIdNotPortable { .. }
    ));
}

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
