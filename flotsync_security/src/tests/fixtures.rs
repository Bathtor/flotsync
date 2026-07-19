//! Shared fixtures, contexts, and constants for security tests.

use super::*;

pub(super) const ALICE_SEED: [u8; MEMBER_KEY_SEED_LENGTH] = [1u8; MEMBER_KEY_SEED_LENGTH];
pub(super) const BOB_SEED: [u8; MEMBER_KEY_SEED_LENGTH] = [2u8; MEMBER_KEY_SEED_LENGTH];
pub(super) const FRAME_KIND: &str = "group-message";
pub(super) const PUBLIC_HEADER: &[u8] = b"{\"sender\":\"alice\"}";
pub(super) const CIPHERTEXT: &[u8] = b"ciphertext-with-tag";
pub(super) const STORE_SECRET_TEST_KEY_ID: Uuid = Uuid::from_u128(0x100);

pub(super) fn local_member(name: &str, seed: [u8; MEMBER_KEY_SEED_LENGTH]) -> LocalMemberKeys {
    let member = member(name);
    let generated = generate_member_key_bundles_from_seed(member.clone(), &seed);
    local_member_keys_from_private_bundle(generated.local_private_bundle.as_bytes(), member)
        .unwrap()
}

pub(super) fn member(name: &str) -> MemberIdentity {
    Identifier::from_array([name, "laptop"])
}

pub(super) fn unique_local_store_secret_profile(label: &str) -> LocalStoreSecretProfile {
    static NEXT_PROFILE: AtomicU64 = AtomicU64::new(1);
    let index = NEXT_PROFILE.fetch_add(1, Ordering::Relaxed);
    LocalStoreSecretProfile::new(format!("{label}-{index}")).unwrap()
}

pub(super) fn signed_frame_fixture() -> (LocalMemberKeys, FrameSignature) {
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

pub(super) fn direct_hpke_context<'a>(
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
pub(super) fn group_hpke_context<'a>(
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
