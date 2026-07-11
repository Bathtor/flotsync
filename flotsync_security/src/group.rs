use crate::{
    error::{
        RandomnessSnafu,
        Result,
        SecurityError,
        StoredGroupSecretLengthSnafu,
        UnsupportedGroupCipherSuiteSnafu,
    },
    identity::{LocalMemberKeys, MemberIdentity, PublicMemberKeys},
    sealed_psk_payload::SealedPSKPayload,
    signature::{FrameSignature, SignedFrameParts, sign_frame, verify_frame_signature},
    store_secret::{StoreSecretCiphertext, StoreSecretContext, StoreSecretKey, open_store_secret},
    util::{append_len_prefixed, append_member_identity, fixed_array, hash_len_prefixed, len_u64},
};
use bytes::{BufMut, Bytes};
use chacha20poly1305::{
    ChaCha20Poly1305,
    Key,
    KeyInit,
    Nonce,
    aead::{Aead, Payload},
};
use flotsync_core::member::IdentifierLike;
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

/// Current symmetric group-message cipher suite identifier.
pub const GROUP_CIPHER_SUITE_CHACHA20_POLY1305: GroupCipherSuite =
    GroupCipherSuite::CHACHA20_POLY1305;

/// Encrypt and sign one group message with the group's symmetric key.
///
/// # Errors
///
/// Returns [`SecurityError`] if group AEAD sealing or sender signing fails.
pub fn seal_group_payload(
    sender_keys: &LocalMemberKeys,
    group_key: &GroupKey,
    context: GroupMessageContext<'_>,
    public_header: &[u8],
    plaintext: &[u8],
) -> Result<SealedPSKPayload> {
    let ciphertext = seal_group_message(group_key, context, public_header, plaintext)?;
    let signature = sign_frame(
        sender_keys,
        SignedFrameParts {
            frame_kind: context.frame_kind,
            public_header,
            ciphertext: ciphertext.as_ref(),
        },
    )?;
    Ok(SealedPSKPayload {
        ciphertext,
        signature: *signature.as_bytes(),
    })
}

/// Verify and decrypt one signed group message with the group's symmetric key.
///
/// # Errors
///
/// Returns [`SecurityError`] if sender signature verification or group AEAD
/// opening fails.
pub fn open_group_payload(
    sender_keys: &PublicMemberKeys,
    group_key: &GroupKey,
    context: GroupMessageContext<'_>,
    public_header: &[u8],
    sealed: &SealedPSKPayload,
) -> Result<Bytes> {
    verify_frame_signature(
        sender_keys,
        SignedFrameParts {
            frame_kind: context.frame_kind,
            public_header,
            ciphertext: sealed.ciphertext.as_ref(),
        },
        &FrameSignature::from_bytes(sealed.signature),
    )?;
    open_group_message(
        group_key,
        context,
        public_header,
        sealed.ciphertext.as_ref(),
    )
}

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
) -> Result<Bytes> {
    let cipher = ChaCha20Poly1305::new(Key::from_slice(&group_key.bytes));
    let nonce = group_nonce(context);
    let aad = group_aad(context, public_header);
    let ciphertext = cipher
        .encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: plaintext,
                aad: &aad,
            },
        )
        .map_err(|_| SecurityError::GroupSeal)?;
    Ok(Bytes::from(ciphertext))
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
) -> Result<Bytes> {
    let cipher = ChaCha20Poly1305::new(Key::from_slice(&group_key.bytes));
    let nonce = group_nonce(context);
    let aad = group_aad(context, public_header);
    let plaintext = cipher
        .decrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: ciphertext,
                aad: &aad,
            },
        )
        .map_err(|_| SecurityError::GroupOpen)?;
    Ok(Bytes::from(plaintext))
}

/// Open and decode one encrypted group key from sensitive store-cell material.
///
/// The decrypted plaintext is kept inside this crate and zeroised before the
/// function returns.
///
/// # Errors
///
/// Returns [`SecurityError::StoreSecretOpen`] when the encrypted store cell
/// does not authenticate, or stored group-secret parsing errors when the
/// decrypted plaintext is malformed.
pub fn open_stored_group_key(
    key: &StoreSecretKey,
    context: StoreSecretContext<'_>,
    sealed: &StoreSecretCiphertext,
) -> Result<GroupKey> {
    let plaintext = open_store_secret(key, context, sealed)?;
    group_key_from_stored_secret_plaintext(plaintext.as_slice())
}

/// Decode stored group-secret plaintext into the active group cipher key.
///
/// # Errors
///
/// Returns [`SecurityError::StoredGroupSecretLength`] for malformed plaintext
/// length and [`SecurityError::UnsupportedGroupCipherSuite`] for any cipher
/// suite other than the currently supported ChaCha20-Poly1305 suite.
pub fn group_key_from_stored_secret_plaintext(plaintext: &[u8]) -> Result<GroupKey> {
    let expected = 2 + GROUP_KEY_LENGTH;
    ensure!(
        plaintext.len() == expected,
        StoredGroupSecretLengthSnafu {
            expected,
            actual: plaintext.len(),
        }
    );
    let (cipher_suite_bytes, key_bytes) = plaintext.split_at(2);
    let cipher_suite = GroupCipherSuite::new(u16::from_be_bytes(fixed_array(cipher_suite_bytes)));
    let expected_suite = GROUP_CIPHER_SUITE_CHACHA20_POLY1305;
    ensure!(
        cipher_suite == expected_suite,
        UnsupportedGroupCipherSuiteSnafu {
            expected: expected_suite,
            actual: cipher_suite,
        }
    );
    Ok(GroupKey::from_bytes(fixed_array(key_bytes)))
}

/// Symmetric key used to encrypt replication messages within one group.
///
/// The key bytes are zeroised on drop. This type deliberately does not
/// implement `Clone`; share it via [`std::rc::Rc`] or [`std::sync::Arc`] when
/// multiple owners need access.
#[derive(PartialEq, Eq, Zeroize, ZeroizeOnDrop)]
pub struct GroupKey {
    bytes: [u8; GROUP_KEY_LENGTH],
}

impl GroupKey {
    /// Build a group key from already generated high-entropy key bytes.
    #[must_use]
    pub fn from_bytes(bytes: [u8; GROUP_KEY_LENGTH]) -> Self {
        Self { bytes }
    }

    /// Return a copy of the raw group key bytes for internal protocol wrapping.
    #[must_use]
    pub const fn to_bytes(&self) -> [u8; GROUP_KEY_LENGTH] {
        self.bytes
    }

    /// Encode this key as the sensitive plaintext protected by store-secret AEAD.
    ///
    /// The returned buffer contains the current group cipher-suite id followed
    /// by raw key bytes, and zeroises itself on drop.
    #[must_use]
    pub fn stored_secret_plaintext(&self) -> Zeroizing<Vec<u8>> {
        let mut output = Zeroizing::new(Vec::with_capacity(2 + GROUP_KEY_LENGTH));
        output.extend_from_slice(&GROUP_CIPHER_SUITE_CHACHA20_POLY1305.as_u16().to_be_bytes());
        output.extend_from_slice(&self.bytes);
        output
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

/// Symmetric group-message cipher suite identifier stored in protocol payloads.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct GroupCipherSuite(u16);

impl GroupCipherSuite {
    /// ChaCha20-Poly1305 with deterministic nonces derived from group context.
    pub const CHACHA20_POLY1305: Self = Self(1);

    /// Build a group cipher-suite id from its wire value.
    #[must_use]
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    /// Return the integer value carried in protocol payloads.
    #[must_use]
    pub const fn as_u16(self) -> u16 {
        self.0
    }

    /// Return the stable human-readable name for a known cipher suite.
    #[must_use]
    pub const fn name(self) -> Option<&'static str> {
        match self {
            Self::CHACHA20_POLY1305 => Some("ChaCha20-Poly1305"),
            Self(_) => None,
        }
    }
}

impl fmt::Display for GroupCipherSuite {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.name() {
            Some(name) => write!(f, "{name} ({})", self.0),
            None => write!(f, "unknown suite {}", self.0),
        }
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
    for segment in member.segments() {
        hash_len_prefixed(hasher, segment.as_ref().as_bytes());
    }
}

/// Build a deterministic group key for tests from a domain and group id.
#[cfg(any(test, feature = "test-support"))]
#[must_use]
pub fn test_group_key_from_id(group_id: Uuid) -> GroupKey {
    let mut hasher = Sha256::new();
    hasher.update(b"flotsync/security/test-group-key/v1");
    hasher.update(group_id.as_bytes());
    let digest = hasher.finalize();
    GroupKey::from_bytes(fixed_array(&digest[..GROUP_KEY_LENGTH]))
}
