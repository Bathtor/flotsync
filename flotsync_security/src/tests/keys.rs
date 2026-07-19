//! Tests for member key bundles, public bundles, and fingerprints.

use super::{fixtures::*, *};

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
