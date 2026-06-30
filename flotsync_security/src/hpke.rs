use crate::{
    error::{HpkeKeyDecodeSnafu, HpkeOpenSnafu, HpkeSealSnafu, Result},
    identity::{LocalMemberKeys, PublicMemberKeys},
    util::fixed_array,
};
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

/// Byte length of an HPKE DHKEM(X25519, HKDF-SHA256) encapsulated key.
pub const HPKE_ENCAPSULATED_KEY_LENGTH: usize = 32;

/// Encrypt one payload for a member's HPKE public key.
///
/// This provides recipient confidentiality only. Sender authentication remains
/// the responsibility of the signed outer replication frame.
///
/// `info` is public protocol context bound into HPKE setup. `aad` is
/// authenticated but unencrypted AEAD data. The RNG is supplied by the caller
/// because HPKE sealing needs fresh encapsulation randomness; tests may inject a
/// deterministic CSPRNG, while production callers must provide a cryptographic
/// RNG.
///
/// # Errors
///
/// Returns [`crate::SecurityError::InvalidHpkeKey`] if the recipient public key
/// bytes cannot be converted to the HPKE library type, or
/// [`crate::SecurityError::HpkeSeal`] if HPKE setup or encryption fails.
pub fn hpke_seal<R>(
    recipient: &PublicMemberKeys,
    info: &[u8],
    aad: &[u8],
    plaintext: &[u8],
    rng: &mut R,
) -> Result<HpkeCiphertext>
where
    R: hpke::rand_core::CryptoRng + hpke::rand_core::RngCore,
{
    // Follow-up flotsync-icw: replace raw info/aad slices with a typed HPKE
    // context that canonicalises purpose, member identities, group/message ids,
    // and authenticated public metadata inside this crate.
    let recipient_public_key = recipient.hpke_public_key.to_hpke_public_key()?;
    let (encapsulated_key, ciphertext) = hpke::single_shot_seal::<HpkeAead, HpkeKdf, HpkeKem, R>(
        &OpModeS::Base,
        &recipient_public_key,
        info,
        plaintext,
        aad,
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
/// `info` and `aad` must be byte-for-byte identical to the values used while
/// sealing the payload.
///
/// # Errors
///
/// Returns [`crate::SecurityError::HpkeKeyDecode`] if the encapsulated key is
/// invalid, or [`crate::SecurityError::HpkeOpen`] if HPKE setup or
/// authentication fails.
pub fn hpke_open(
    recipient: &LocalMemberKeys,
    info: &[u8],
    aad: &[u8],
    ciphertext: &HpkeCiphertext,
) -> Result<Vec<u8>> {
    let encapsulated_key =
        HpkeEncappedKey::from_bytes(&ciphertext.encapsulated_key).context(HpkeKeyDecodeSnafu)?;
    hpke::single_shot_open::<HpkeAead, HpkeKdf, HpkeKem>(
        &OpModeR::Base,
        &recipient.hpke_private_key,
        &encapsulated_key,
        info,
        &ciphertext.ciphertext,
        aad,
    )
    .context(HpkeOpenSnafu)
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
