use crate::{
    error::{
        ContextMemberMismatchSnafu,
        ContextMemberRole,
        HpkeKeyDecodeSnafu,
        HpkeOpenSnafu,
        HpkeSealSnafu,
        Result,
    },
    identity::{LocalMemberKeys, PublicMemberKeys},
    util::{append_len_prefixed, append_member_identity, fixed_array},
};
use bytes::BufMut;
use flotsync_core::{GroupId, MemberIdentity};
use hpke::{
    Deserializable,
    Kem,
    OpModeR,
    OpModeS,
    Serializable,
    aead::ChaCha20Poly1305 as HpkeAead,
    kdf::HkdfSha256,
    kem::X25519HkdfSha256,
};
use snafu::prelude::*;
use uuid::Uuid;

/// Byte length of an HPKE DHKEM(X25519, HKDF-SHA256) encapsulated key.
pub const HPKE_ENCAPSULATED_KEY_LENGTH: usize = 32;

const DOMAIN_HPKE_INFO: &[u8] = b"flotsync/security/hpke-info/v1";
const DOMAIN_HPKE_AAD: &[u8] = b"flotsync/security/hpke-aad/v1";
const PURPOSE_RELIABLE_PAYLOAD: &[u8] = b"reliable-payload";
const SCOPE_DIRECT_MESSAGE: &[u8] = b"direct-message";
const SCOPE_GROUP: &[u8] = b"group";

/// Encrypt one payload for a member's HPKE public key.
///
/// This provides recipient confidentiality only. Sender authentication remains
/// the responsibility of the signed outer replication frame.
///
/// The typed `context` is canonicalised into HPKE setup `info` and
/// authenticated but unencrypted AEAD data inside this crate. The RNG is
/// supplied by the caller because HPKE sealing needs fresh encapsulation
/// randomness; tests may inject a deterministic CSPRNG, while production callers
/// must provide a cryptographic RNG.
///
/// # Errors
///
/// Returns [`crate::SecurityError::InvalidHpkeKey`] if the recipient public key
/// bytes cannot be converted to the HPKE library type, or
/// [`crate::SecurityError::HpkeSeal`] if HPKE setup or encryption fails.
pub fn hpke_seal<R>(
    recipient: &PublicMemberKeys,
    context: HpkeContext<'_>,
    plaintext: &[u8],
    rng: &mut R,
) -> Result<HpkeCiphertext>
where
    R: hpke::rand_core::CryptoRng + hpke::rand_core::RngCore,
{
    context.ensure_recipient_matches_key(recipient.member_id())?;
    let recipient_public_key = recipient.hpke_public_key.to_hpke_public_key()?;
    let info = context.info_bytes();
    let aad = context.aad_bytes();
    let (encapsulated_key, ciphertext) = hpke::single_shot_seal::<HpkeAead, HpkeKdf, HpkeKem, R>(
        &OpModeS::Base,
        &recipient_public_key,
        &info,
        plaintext,
        &aad,
        rng,
    )
    .context(HpkeSealSnafu)?;

    Ok(HpkeCiphertext {
        encapsulated_key: fixed_array(encapsulated_key.to_bytes().as_slice()),
        ciphertext,
    })
}

/// Decrypt one HPKE payload for the local member.
///
/// The typed `context` must be semantically identical to the one used while
/// sealing the payload. It is canonicalised into HPKE setup `info` and AEAD
/// authenticated data inside this crate.
///
/// # Errors
///
/// Returns [`crate::SecurityError::HpkeKeyDecode`] if the encapsulated key is
/// invalid, or [`crate::SecurityError::HpkeOpen`] if HPKE setup or
/// authentication fails.
pub fn hpke_open(
    recipient: &LocalMemberKeys,
    context: HpkeContext<'_>,
    ciphertext: &HpkeCiphertext,
) -> Result<Vec<u8>> {
    context.ensure_recipient_matches_key(recipient.member_id())?;
    let encapsulated_key =
        HpkeEncappedKey::from_bytes(&ciphertext.encapsulated_key).context(HpkeKeyDecodeSnafu)?;
    let info = context.info_bytes();
    let aad = context.aad_bytes();
    hpke::single_shot_open::<HpkeAead, HpkeKdf, HpkeKem>(
        &OpModeR::Base,
        &recipient.hpke_private_key,
        &encapsulated_key,
        &info,
        &ciphertext.ciphertext,
        &aad,
    )
    .context(HpkeOpenSnafu)
}

/// Public purpose label for one HPKE envelope family.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum HpkeEnvelopePurpose {
    /// Recipient-specific reliable-delivery payload.
    ReliablePayload,
}

impl HpkeEnvelopePurpose {
    /// Return the private transcript label for this public purpose.
    const fn as_bytes(self) -> &'static [u8] {
        match self {
            Self::ReliablePayload => PURPOSE_RELIABLE_PAYLOAD,
        }
    }
}

/// Public scope that disambiguates otherwise similar HPKE envelopes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HpkeEnvelopeScope {
    /// Recipient-specific payload without a replication-group context.
    DirectMessage,
    /// Recipient-specific payload whose plaintext meaning is scoped to a replication group.
    Group {
        /// Replication group that scopes the envelope's plaintext meaning.
        group_id: GroupId,
    },
}

impl HpkeEnvelopeScope {
    /// Append this scope to a protocol transcript.
    pub(crate) fn append_to<B>(self, output: &mut B)
    where
        B: BufMut,
    {
        match self {
            Self::DirectMessage => append_len_prefixed(output, SCOPE_DIRECT_MESSAGE),
            Self::Group { group_id } => {
                append_len_prefixed(output, SCOPE_GROUP);
                output.put_slice(group_id.0.as_bytes());
            }
        }
    }
}

/// Typed public context bound into one HPKE seal/open operation.
#[derive(Clone, Copy, Debug)]
pub struct HpkeContext<'a> {
    /// Protocol envelope family being protected.
    pub purpose: HpkeEnvelopePurpose,
    /// Member identity of the sender responsible for the surrounding frame.
    pub sender: &'a MemberIdentity,
    /// Member identity of the recipient that can open the HPKE payload.
    pub recipient: &'a MemberIdentity,
    /// Scope that separates direct and group-bound recipient payloads.
    pub scope: HpkeEnvelopeScope,
    /// Stable delivery message id for this recipient-specific envelope.
    pub delivery_message_id: Uuid,
    /// Public metadata authenticated by HPKE AEAD but not encrypted.
    pub authenticated_public_metadata: &'a [u8],
}

impl HpkeContext<'_> {
    /// Ensure the context recipient identity matches the supplied HPKE key owner.
    fn ensure_recipient_matches_key(self, key_member: &MemberIdentity) -> Result<()> {
        ensure!(
            self.recipient == key_member,
            ContextMemberMismatchSnafu {
                member_role: ContextMemberRole::Recipient,
                context_member: self.recipient.clone(),
                key_member: key_member.clone(),
            }
        );
        Ok(())
    }

    /// Build HPKE setup info for this protocol context.
    fn info_bytes(self) -> Vec<u8> {
        self.transcript_bytes(DOMAIN_HPKE_INFO, None)
    }

    /// Build HPKE AEAD authenticated data for this protocol context.
    fn aad_bytes(self) -> Vec<u8> {
        self.transcript_bytes(DOMAIN_HPKE_AAD, Some(self.authenticated_public_metadata))
    }

    /// Build a canonical context transcript under a specific HPKE domain.
    fn transcript_bytes(
        self,
        domain: &[u8],
        authenticated_public_metadata: Option<&[u8]>,
    ) -> Vec<u8> {
        let mut output = Vec::new();
        append_len_prefixed(&mut output, domain);
        append_len_prefixed(&mut output, self.purpose.as_bytes());
        self.scope.append_to(&mut output);
        append_member_identity(&mut output, self.sender);
        append_member_identity(&mut output, self.recipient);
        output.put_slice(self.delivery_message_id.as_bytes());
        if let Some(authenticated_public_metadata) = authenticated_public_metadata {
            append_len_prefixed(&mut output, authenticated_public_metadata);
        }
        output
    }
}

/// HPKE single-shot ciphertext plus the encapsulated key required to open it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HpkeCiphertext {
    /// HPKE encapsulated ephemeral public key bytes sent alongside the ciphertext.
    encapsulated_key: [u8; HPKE_ENCAPSULATED_KEY_LENGTH],
    /// HPKE ciphertext including its AEAD authentication tag.
    ciphertext: Vec<u8>,
}

impl HpkeCiphertext {
    /// Build HPKE wire material from fixed-width encapsulated key bytes and ciphertext.
    #[must_use]
    pub fn from_parts(
        encapsulated_key: [u8; HPKE_ENCAPSULATED_KEY_LENGTH],
        ciphertext: Vec<u8>,
    ) -> Self {
        Self {
            encapsulated_key,
            ciphertext,
        }
    }

    /// Return the fixed-width HPKE encapsulated key bytes.
    #[must_use]
    pub const fn encapsulated_key(&self) -> &[u8; HPKE_ENCAPSULATED_KEY_LENGTH] {
        &self.encapsulated_key
    }

    /// Return the HPKE ciphertext including its AEAD authentication tag.
    #[must_use]
    pub fn ciphertext(&self) -> &[u8] {
        &self.ciphertext
    }

    /// Split this value into the wire fields that must be transmitted together.
    #[must_use]
    pub fn into_parts(self) -> ([u8; HPKE_ENCAPSULATED_KEY_LENGTH], Vec<u8>) {
        (self.encapsulated_key, self.ciphertext)
    }
}

/// HPKE KEM selected for member-targeted encryption.
pub(crate) type HpkeKem = X25519HkdfSha256;
/// HPKE KDF selected for member-targeted encryption.
pub(crate) type HpkeKdf = HkdfSha256;
/// Public key type for the selected HPKE KEM.
pub(crate) type HpkePublicKey = <HpkeKem as Kem>::PublicKey;
/// Private key type for the selected HPKE KEM.
pub(crate) type HpkePrivateKey = <HpkeKem as Kem>::PrivateKey;
/// Encapsulated key type sent with each HPKE ciphertext.
pub(crate) type HpkeEncappedKey = <HpkeKem as Kem>::EncappedKey;
