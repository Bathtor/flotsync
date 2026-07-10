//! Stable fingerprints for member public key material.

use crate::util::{fixed_array, hash_len_prefixed};
use base64::{
    Engine as _,
    engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD},
};
use sha2::{Digest, Sha256};
use snafu::prelude::*;
use std::{fmt, str::FromStr};

/// Byte length of one key-material fingerprint.
pub const KEY_FINGERPRINT_LENGTH: usize = 32;

const CANONICAL_BASE64URL_LENGTH: usize = 44;
const KEY_FINGERPRINT_TRANSCRIPT_DOMAIN: &[u8] = b"flotsync.security.key-fingerprint.v1";
const SIGNING_ROLE_LABEL: &[u8] = b"signing";
const SIGNING_SCHEME_ED25519_LABEL: &[u8] = b"ed25519";
const ENCRYPTION_ROLE_LABEL: &[u8] = b"encryption";
const ENCRYPTION_SCHEME_X25519_LABEL: &[u8] = b"x25519-hpke";

/// Stable, non-secret fingerprint of one public member key bundle.
///
/// The canonical padded base64url representation is the string transfer format
/// for fields, commands, and APIs that Flotsync may parse again. [`Display`] and
/// [`Debug`] use the grouped human-verification form instead; [`FromStr`] accepts
/// only the canonical transfer representation.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeyFingerprint([u8; KEY_FINGERPRINT_LENGTH]);

impl KeyFingerprint {
    /// Build a fingerprint from its raw bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; KEY_FINGERPRINT_LENGTH]) -> Self {
        Self(bytes)
    }

    /// Build a fingerprint from a fixed-width byte slice.
    ///
    /// # Errors
    ///
    /// Returns [`KeyFingerprintParseError::InvalidByteLength`] when `bytes`
    /// is not exactly [`KEY_FINGERPRINT_LENGTH`] bytes.
    pub fn try_from_slice(bytes: &[u8]) -> Result<Self, KeyFingerprintParseError> {
        ensure!(
            bytes.len() == KEY_FINGERPRINT_LENGTH,
            InvalidByteLengthSnafu {
                expected: KEY_FINGERPRINT_LENGTH,
                actual: bytes.len(),
            }
        );
        Ok(Self(fixed_array(bytes)))
    }

    /// Return this fingerprint's raw bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; KEY_FINGERPRINT_LENGTH] {
        &self.0
    }

    /// Return the canonical padded base64url fingerprint string.
    ///
    /// Use this representation whenever the fingerprint is transferred as text
    /// and may be parsed by Flotsync again.
    #[must_use]
    pub fn to_canonical_base64url(&self) -> String {
        URL_SAFE.encode(self.0)
    }

    /// Parse a canonical padded base64url fingerprint string.
    ///
    /// This accepts the text transfer representation produced by
    /// [`Self::to_canonical_base64url`], not the grouped human-verification
    /// representation produced by [`Self::to_display_string`].
    ///
    /// # Errors
    ///
    /// Returns [`KeyFingerprintParseError`] when `input` is not padded
    /// base64url, not exactly one fingerprint, or not the canonical encoding
    /// of its decoded bytes.
    pub fn from_canonical_base64url(input: &str) -> Result<Self, KeyFingerprintParseError> {
        ensure!(
            input.len() == CANONICAL_BASE64URL_LENGTH,
            InvalidTextLengthSnafu {
                expected: CANONICAL_BASE64URL_LENGTH,
                actual: input.len(),
            }
        );
        let bytes = URL_SAFE
            .decode(input)
            .context(InvalidCanonicalBase64UrlSnafu)?;
        let fingerprint = Self::try_from_slice(&bytes)?;
        let canonical = fingerprint.to_canonical_base64url();
        ensure!(
            canonical == input,
            NonCanonicalBase64UrlSnafu {
                canonical,
                actual: input.to_owned(),
            }
        );
        Ok(fingerprint)
    }

    /// Return grouped unpadded base64url text for human verification.
    ///
    /// This form is display-only and intentionally has no parser.
    #[must_use]
    pub fn to_display_string(&self) -> String {
        let unpadded = URL_SAFE_NO_PAD.encode(self.0);
        let mut output = String::with_capacity(unpadded.len() + 10);
        for (index, chunk) in unpadded.as_bytes().chunks(4).enumerate() {
            if index > 0 {
                output.push('-');
            }
            for byte in chunk {
                output.push(char::from(*byte));
            }
        }
        output
    }
}

impl AsRef<[u8]> for KeyFingerprint {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl fmt::Debug for KeyFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("KeyFingerprint")
            .field(&self.to_display_string())
            .finish()
    }
}

impl fmt::Display for KeyFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_display_string())
    }
}

impl FromStr for KeyFingerprint {
    type Err = KeyFingerprintParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Self::from_canonical_base64url(input)
    }
}

/// Errors raised while parsing a key fingerprint from external text or bytes.
#[derive(Debug, Snafu)]
pub enum KeyFingerprintParseError {
    /// Raw fingerprint bytes had the wrong length.
    #[snafu(display("key fingerprint is {actual} byte(s), expected {expected}"))]
    InvalidByteLength { expected: usize, actual: usize },
    /// Canonical base64url text had the wrong length.
    #[snafu(display(
        "canonical key fingerprint text is {actual} character(s), expected {expected}"
    ))]
    InvalidTextLength { expected: usize, actual: usize },
    /// Canonical fingerprint text was not padded base64url.
    #[snafu(display("key fingerprint text is not valid padded base64url: {source}"))]
    InvalidCanonicalBase64Url { source: base64::DecodeError },
    /// Fingerprint text decoded successfully but was not the exact transfer spelling.
    #[snafu(display("key fingerprint text '{actual}' is not canonical; expected '{canonical}'"))]
    NonCanonicalBase64Url { canonical: String, actual: String },
}

/// Derive the v1 fingerprint for one typed public key pair.
pub(crate) fn derive_public_key_fingerprint(
    signing_public_key: &[u8],
    encryption_public_key: &[u8],
) -> KeyFingerprint {
    let mut hasher = Sha256::new();
    hash_len_prefixed(&mut hasher, KEY_FINGERPRINT_TRANSCRIPT_DOMAIN);
    hash_len_prefixed(&mut hasher, SIGNING_ROLE_LABEL);
    hash_len_prefixed(&mut hasher, SIGNING_SCHEME_ED25519_LABEL);
    hash_len_prefixed(&mut hasher, signing_public_key);
    hash_len_prefixed(&mut hasher, ENCRYPTION_ROLE_LABEL);
    hash_len_prefixed(&mut hasher, ENCRYPTION_SCHEME_X25519_LABEL);
    hash_len_prefixed(&mut hasher, encryption_public_key);
    let digest = hasher.finalize();
    KeyFingerprint::from_bytes(fixed_array(digest.as_slice()))
}
