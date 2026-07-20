//! Tests for HPKE envelope construction and authenticated contexts.

use super::{fixtures::*, *};

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
