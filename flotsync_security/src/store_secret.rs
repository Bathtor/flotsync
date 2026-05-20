use crate::{
    error::{RandomnessSnafu, Result, SecurityError},
    util::append_len_prefixed,
};
use chacha20poly1305::{
    Key,
    KeyInit,
    XChaCha20Poly1305,
    XNonce,
    aead::{Aead, Payload},
};
use rand_core::{OsRng, TryRngCore};
use snafu::prelude::*;
use std::fmt;
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

/// Cryptographic setup version for encrypted store-secret cells.
pub const STORE_SECRET_CRYPTO_VERSION_V1: StoreSecretCryptoVersion = StoreSecretCryptoVersion::V1;
/// Byte length of the XChaCha20-Poly1305 store-secret key.
pub const STORE_SECRET_KEY_LENGTH: usize = 32;
/// Byte length of random XChaCha20-Poly1305 store-secret nonces.
pub const STORE_SECRET_NONCE_LENGTH: usize = 24;

/// Device-local key used to encrypt sensitive store cells before persistence.
#[derive(Zeroize, ZeroizeOnDrop)]
pub struct StoreSecretKey {
    bytes: [u8; STORE_SECRET_KEY_LENGTH],
}

impl StoreSecretKey {
    /// Build a store-secret key from already generated high-entropy bytes.
    #[must_use]
    pub fn from_bytes(bytes: [u8; STORE_SECRET_KEY_LENGTH]) -> Self {
        Self { bytes }
    }

    /// Validate and build a store-secret key from already generated high-entropy bytes.
    ///
    /// # Errors
    ///
    /// Returns [`SecurityError::StoreSecretKeyLength`] if `bytes` does not
    /// contain exactly [`STORE_SECRET_KEY_LENGTH`] bytes.
    pub fn try_from_slice(bytes: &[u8]) -> Result<Self> {
        let bytes = <[u8; STORE_SECRET_KEY_LENGTH]>::try_from(bytes).map_err(|_| {
            SecurityError::StoreSecretKeyLength {
                expected: STORE_SECRET_KEY_LENGTH,
                actual: bytes.len(),
            }
        })?;
        Ok(Self::from_bytes(bytes))
    }

    /// Generate a fresh store-secret key from operating system randomness.
    ///
    /// # Errors
    ///
    /// Returns [`SecurityError::Randomness`] if the operating system random
    /// source fails.
    pub fn generate() -> Result<Self> {
        let mut bytes = Zeroizing::new([0u8; STORE_SECRET_KEY_LENGTH]);
        OsRng
            .try_fill_bytes(bytes.as_mut())
            .context(RandomnessSnafu)?;
        Ok(Self { bytes: *bytes })
    }
}

impl fmt::Debug for StoreSecretKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("StoreSecretKey")
            .field(&"<redacted>")
            .finish()
    }
}

/// Logical identity of one sensitive cell encrypted before it enters storage.
#[derive(Clone, Copy, Debug)]
pub struct StoreSecretContext<'a> {
    /// Logical table identifier, independent of a concrete backend table name.
    pub table: &'static str,
    /// Logical column identifier, independent of a concrete backend column name.
    pub column: &'static str,
    /// Logical row identifier for the encrypted value.
    pub row_id: &'a [u8],
    /// Caller-defined id of the device-local key used for this cell.
    pub key_id: &'a str,
    /// Cryptographic setup version stored next to this encrypted cell.
    pub crypto_version: StoreSecretCryptoVersion,
}

/// One encrypted sensitive store cell produced by [`seal_store_secret`].
#[derive(Clone, PartialEq, Eq)]
pub struct StoreSecretCiphertext {
    /// Random nonce used for this encrypted cell.
    pub nonce: [u8; STORE_SECRET_NONCE_LENGTH],
    /// Ciphertext including the AEAD authentication tag.
    pub ciphertext: Vec<u8>,
}

impl fmt::Debug for StoreSecretCiphertext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoreSecretCiphertext")
            .field("nonce_len", &self.nonce.len())
            .field("ciphertext_len", &self.ciphertext.len())
            .finish()
    }
}

/// Encrypt one sensitive store cell with authenticated logical storage context.
///
/// # Errors
///
/// Returns [`SecurityError::Randomness`] if nonce generation fails, or
/// [`SecurityError::StoreSecretSeal`] if the AEAD rejects the request.
pub fn seal_store_secret(
    key: &StoreSecretKey,
    context: StoreSecretContext<'_>,
    plaintext: &[u8],
) -> Result<StoreSecretCiphertext> {
    let mut nonce = [0u8; STORE_SECRET_NONCE_LENGTH];
    OsRng.try_fill_bytes(&mut nonce).context(RandomnessSnafu)?;
    let ciphertext = seal_store_secret_with_nonce(key, context, plaintext, nonce)?;
    Ok(StoreSecretCiphertext { nonce, ciphertext })
}

/// Decrypt and authenticate one sensitive store cell.
///
/// # Errors
///
/// Returns [`SecurityError::StoreSecretOpen`] if the key, nonce, context, or
/// ciphertext do not authenticate together.
pub fn open_store_secret(
    key: &StoreSecretKey,
    context: StoreSecretContext<'_>,
    sealed: &StoreSecretCiphertext,
) -> Result<Zeroizing<Vec<u8>>> {
    let cipher = XChaCha20Poly1305::new(Key::from_slice(&key.bytes));
    let aad = store_secret_aad(context);
    cipher
        .decrypt(
            XNonce::from_slice(&sealed.nonce),
            Payload {
                msg: &sealed.ciphertext,
                aad: &aad,
            },
        )
        .map(Zeroizing::new)
        .map_err(|_| SecurityError::StoreSecretOpen)
}

/// Encrypt one sensitive store cell with a caller-supplied nonce.
///
/// This is restricted to crate tests and deterministic fixtures; production
/// callers should use [`seal_store_secret`] so nonce uniqueness comes from OS
/// randomness.
///
/// # Errors
///
/// Returns [`SecurityError::StoreSecretSeal`] if the AEAD implementation
/// rejects the encryption request.
#[cfg(any(test, feature = "test-support"))]
pub fn seal_store_secret_for_test(
    key: &StoreSecretKey,
    context: StoreSecretContext<'_>,
    plaintext: &[u8],
    nonce: [u8; STORE_SECRET_NONCE_LENGTH],
) -> Result<StoreSecretCiphertext> {
    let ciphertext = seal_store_secret_with_nonce(key, context, plaintext, nonce)?;
    Ok(StoreSecretCiphertext { nonce, ciphertext })
}

/// Shared AEAD implementation for production random nonces and deterministic test fixtures.
fn seal_store_secret_with_nonce(
    key: &StoreSecretKey,
    context: StoreSecretContext<'_>,
    plaintext: &[u8],
    nonce: [u8; STORE_SECRET_NONCE_LENGTH],
) -> Result<Vec<u8>> {
    let cipher = XChaCha20Poly1305::new(Key::from_slice(&key.bytes));
    let aad = store_secret_aad(context);
    cipher
        .encrypt(
            XNonce::from_slice(&nonce),
            Payload {
                msg: plaintext,
                aad: &aad,
            },
        )
        .map_err(|_| SecurityError::StoreSecretSeal)
}

/// Build authenticated data from logical storage identifiers rather than backend column names.
fn store_secret_aad(context: StoreSecretContext<'_>) -> Vec<u8> {
    let mut aad = Vec::new();
    append_len_prefixed(&mut aad, b"flotsync/security/store-secret/v1");
    append_len_prefixed(&mut aad, context.table.as_bytes());
    append_len_prefixed(&mut aad, context.column.as_bytes());
    append_len_prefixed(&mut aad, context.row_id);
    append_len_prefixed(&mut aad, context.key_id.as_bytes());
    aad.extend_from_slice(&context.crypto_version.as_u16().to_be_bytes());
    aad
}

/// Store-secret cryptographic setup identifier stored with encrypted cells.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StoreSecretCryptoVersion(u16);

impl StoreSecretCryptoVersion {
    /// XChaCha20-Poly1305 with random 24-byte nonces.
    pub const V1: Self = Self(1);

    /// Build a store-secret crypto version from its wire/storage value.
    #[must_use]
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    /// Return the integer value stored in backend metadata columns.
    #[must_use]
    pub const fn as_u16(self) -> u16 {
        self.0
    }
}
