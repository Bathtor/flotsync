use crate::{
    error::{RandomnessSnafu, Result, SecurityError},
    identity::MemberIdentity,
    util::{append_len_prefixed, fixed_array, hash_len_prefixed, len_u64},
};
use bytes::BufMut;
use chacha20poly1305::{
    ChaCha20Poly1305,
    Key,
    KeyInit,
    Nonce,
    aead::{Aead, Payload},
};
use rand_core::{OsRng, TryRngCore};
use sha2::{Digest, Sha256};
use snafu::prelude::*;
use std::fmt;
use uuid::Uuid;
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

/// Byte length of a high-entropy symmetric group key for the current AEAD.
pub const GROUP_KEY_LENGTH: usize = 32;

/// Byte length of a ChaCha20-Poly1305 nonce.
pub const GROUP_NONCE_LENGTH: usize = 12;

/// Encrypt one group message with the group's symmetric key.
///
/// The nonce is derived from the immutable group/message context. The public
/// header is included as authenticated data and remains unencrypted.
///
/// # Errors
///
/// Returns [`SecurityError::GroupSeal`] if the AEAD implementation rejects the
/// encryption request.
pub fn seal_group_message(
    group_key: &GroupKey,
    context: GroupMessageContext<'_>,
    public_header: &[u8],
    plaintext: &[u8],
) -> Result<Vec<u8>> {
    let cipher = ChaCha20Poly1305::new(Key::from_slice(&group_key.bytes));
    let nonce = group_nonce(context);
    let aad = group_aad(context, public_header);
    cipher
        .encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: plaintext,
                aad: &aad,
            },
        )
        .map_err(|_| SecurityError::GroupSeal)
}

/// Decrypt and authenticate one group message with the group's symmetric key.
///
/// # Errors
///
/// Returns [`SecurityError::GroupOpen`] if the context, public header,
/// ciphertext, tag, or group key do not authenticate together.
pub fn open_group_message(
    group_key: &GroupKey,
    context: GroupMessageContext<'_>,
    public_header: &[u8],
    ciphertext: &[u8],
) -> Result<Vec<u8>> {
    let cipher = ChaCha20Poly1305::new(Key::from_slice(&group_key.bytes));
    let nonce = group_nonce(context);
    let aad = group_aad(context, public_header);
    cipher
        .decrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: ciphertext,
                aad: &aad,
            },
        )
        .map_err(|_| SecurityError::GroupOpen)
}

/// Symmetric key used to encrypt replication messages within one group.
///
/// The key bytes are zeroised on drop. This type deliberately does not
/// implement `Clone`; share it via [`std::rc::Rc`] or [`std::sync::Arc`] when
/// multiple owners need access.
#[derive(Zeroize, ZeroizeOnDrop)]
pub struct GroupKey {
    bytes: [u8; GROUP_KEY_LENGTH],
}

impl GroupKey {
    /// Build a group key from already generated high-entropy key bytes.
    #[must_use]
    pub fn from_bytes(bytes: [u8; GROUP_KEY_LENGTH]) -> Self {
        Self { bytes }
    }

    /// Generate a fresh symmetric group key from operating system randomness.
    ///
    /// # Errors
    ///
    /// Returns [`SecurityError::Randomness`] if the operating system random
    /// source fails.
    pub fn generate() -> Result<Self> {
        let mut bytes = Zeroizing::new([0u8; GROUP_KEY_LENGTH]);
        OsRng
            .try_fill_bytes(bytes.as_mut())
            .context(RandomnessSnafu)?;
        Ok(Self { bytes: *bytes })
    }
}

impl fmt::Debug for GroupKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("GroupKey").field(&"<redacted>").finish()
    }
}

/// Public context that binds a group ciphertext to its replication identity.
#[derive(Clone, Copy, Debug)]
pub struct GroupMessageContext<'a> {
    /// Immutable replication group id; changing this changes nonce and AAD.
    pub group_id: Uuid,
    /// Stable protocol frame kind, not a display label.
    pub frame_kind: &'static str,
    /// Member identity of the sender that produced this group message.
    pub sender: &'a MemberIdentity,
    /// Message id unique within the group context; changing this changes nonce and AAD.
    pub message_id: Uuid,
}

const DOMAIN_GROUP_AAD: &[u8] = b"flotsync/security/group-aad/v1";
const DOMAIN_GROUP_NONCE: &[u8] = b"flotsync/security/group-nonce/v1";

/// Derive the deterministic AEAD nonce from immutable group/message identity.
///
/// Message ids are required to be unique under one group key. The nonce uses the
/// leftmost 12 bytes of the domain-separated SHA-256 digest; fixed-position
/// digest bytes are not meaningfully weaker than other fixed positions for this
/// construction, and the remaining risk is digest-prefix collision probability.
fn group_nonce(context: GroupMessageContext<'_>) -> [u8; GROUP_NONCE_LENGTH] {
    let mut hasher = Sha256::new();
    hasher.update(DOMAIN_GROUP_NONCE);
    hasher.update(context.group_id.as_bytes());
    hash_len_prefixed(&mut hasher, context.frame_kind.as_bytes());
    hash_member_identity(&mut hasher, context.sender);
    hasher.update(context.message_id.as_bytes());
    let digest = hasher.finalize();
    fixed_array(&digest[..GROUP_NONCE_LENGTH])
}

/// Build the authenticated data bound to one group ciphertext.
fn group_aad(context: GroupMessageContext<'_>, public_header: &[u8]) -> Vec<u8> {
    let mut aad = Vec::new();
    append_len_prefixed(&mut aad, DOMAIN_GROUP_AAD);
    aad.put_slice(context.group_id.as_bytes());
    append_len_prefixed(&mut aad, context.frame_kind.as_bytes());
    append_member_identity(&mut aad, context.sender);
    aad.put_slice(context.message_id.as_bytes());
    append_len_prefixed(&mut aad, public_header);
    aad
}

/// Hash a member identity as a segment count followed by length-prefixed segments.
fn hash_member_identity(hasher: &mut Sha256, member: &MemberIdentity) {
    hasher.update(len_u64(member.len()).to_be_bytes());
    for segment in member.segments_iter() {
        hash_len_prefixed(hasher, segment.as_ref().as_bytes());
    }
}

/// Append a member identity as a segment count followed by length-prefixed segments.
fn append_member_identity(output: &mut Vec<u8>, member: &MemberIdentity) {
    output.put_u64(len_u64(member.len()));
    for segment in member.segments_iter() {
        append_len_prefixed(output, segment.as_ref().as_bytes());
    }
}
