//! Application-local store-secret profiles and backend selection.

#[cfg(feature = "local-secret-manager")]
use crate::SecurityError;
use crate::{StoreSecretKey, StoreSecretKeyId};
use flotsync_core::member::Identifier;
#[cfg(feature = "local-secret-manager")]
use keyring_core::Error as KeyringError;
use snafu::prelude::*;
use std::fmt;

#[cfg(feature = "local-secret-manager")]
mod manager;
#[cfg(not(feature = "local-secret-manager"))]
mod unmanaged;

#[cfg(all(any(test, feature = "test-support"), feature = "local-secret-manager"))]
pub use manager::install_local_store_secret_test_store;
#[cfg(feature = "local-secret-manager")]
pub use manager::{load_local_store_secret, load_or_create_local_store_secret};
#[cfg(not(feature = "local-secret-manager"))]
pub use unmanaged::{load_local_store_secret, load_or_create_local_store_secret};

/// Result type for loading or creating the device-local store secret.
pub type LocalStoreSecretResult<T> = std::result::Result<T, LocalStoreSecretError>;

/// Application-local profile that selects one device-local store secret.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LocalStoreSecretProfile(String);

impl LocalStoreSecretProfile {
    /// Build a profile from a caller-provided local selector.
    ///
    /// # Errors
    ///
    /// Returns [`LocalStoreSecretError::EmptyProfile`] if the selector is empty
    /// after trimming whitespace, or [`LocalStoreSecretError::ProfileWhitespace`]
    /// if it has leading or trailing whitespace. Returns
    /// [`LocalStoreSecretError::ProfileNotPortable`] if the selector cannot be
    /// used consistently by all supported keyring backends.
    pub fn new(value: impl Into<String>) -> LocalStoreSecretResult<Self> {
        let value = value.into();
        let trimmed = value.trim();
        ensure!(!trimmed.is_empty(), EmptyProfileSnafu);
        ensure!(
            trimmed == value,
            ProfileWhitespaceSnafu {
                value: value.clone(),
            }
        );
        ensure_portable_selector(trimmed).map_err(|message| {
            LocalStoreSecretError::ProfileNotPortable {
                value: value.clone(),
                message,
            }
        })?;
        Ok(Self(value))
    }

    /// Return the profile selector as provided by the caller.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for LocalStoreSecretProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Store-secret key loaded from local application profile storage.
#[derive(Debug)]
pub struct LoadedLocalStoreSecret {
    key_id: StoreSecretKeyId,
    store_secret_key: StoreSecretKey,
}

impl LoadedLocalStoreSecret {
    /// Return the generated key id stored next to encrypted replication cells.
    #[must_use]
    pub fn key_id(&self) -> StoreSecretKeyId {
        self.key_id
    }

    /// Consume this value into its generated key id and secret key.
    #[must_use]
    pub fn into_parts(self) -> (StoreSecretKeyId, StoreSecretKey) {
        (self.key_id, self.store_secret_key)
    }
}

/// Errors from application-local store-secret loading and first-run creation.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum LocalStoreSecretError {
    /// The profile selector is empty and cannot identify a local secret slot.
    #[snafu(display("Local store-secret profile must not be empty."))]
    EmptyProfile,
    /// The profile selector would be ambiguous after ordinary config whitespace cleanup.
    #[snafu(display("Local store-secret profile {value:?} must not have surrounding whitespace."))]
    ProfileWhitespace { value: String },
    /// The profile selector cannot be represented consistently by supported keyring backends.
    #[snafu(display("Local store-secret profile {value:?} is not portable: {message}"))]
    ProfileNotPortable {
        value: String,
        message: &'static str,
    },
    /// The application id cannot be represented consistently by supported keyring backends.
    #[snafu(display(
        "Application id '{application_id}' cannot select a local store secret: {message}"
    ))]
    ApplicationIdNotPortable {
        application_id: String,
        message: &'static str,
    },
    /// The readable account name would exceed the smallest supported backend limit.
    #[snafu(display(
        "Local store-secret account is {actual} bytes, but supported keyring backends allow at most {maximum} bytes."
    ))]
    AccountTooLong { maximum: usize, actual: usize },
    /// This binary was built without local secret-manager support.
    #[snafu(display(
        "This binary was built without local secret manager support. Rebuild with the 'local-secret-manager' feature or configure caller-managed store-secret material."
    ))]
    LocalSecretManagerUnavailable,
    /// The process could not initialise the configured local secret storage.
    #[snafu(display("Failed to initialise local store-secret storage: {source}"))]
    #[cfg(feature = "local-secret-manager")]
    InitialiseStorage { source: KeyringError },
    /// No persistent keyring store has been selected for the current target.
    #[snafu(display(
        "No persistent local store-secret keyring is configured for target '{target_os}'. Add a target-specific persistent keyring backend before enabling local store-secret loading on this platform."
    ))]
    UnsupportedPersistentStore { target_os: &'static str },
    /// The selected local secret entry could not be built.
    #[snafu(display(
        "Failed to build local store-secret entry for application '{application_id}' and profile '{profile}': {source}"
    ))]
    #[cfg(feature = "local-secret-manager")]
    BuildEntry {
        application_id: Identifier,
        profile: LocalStoreSecretProfile,
        source: KeyringError,
    },
    /// The selected application/profile slot has not yet been initialised.
    #[snafu(display(
        "No local store secret exists for application '{application_id}' and profile '{profile}'."
    ))]
    Missing {
        application_id: Identifier,
        profile: LocalStoreSecretProfile,
    },
    /// Reading the selected local secret failed.
    #[snafu(display(
        "Failed to read local store secret for application '{application_id}' and profile '{profile}': {source}"
    ))]
    #[cfg(feature = "local-secret-manager")]
    Read {
        application_id: Identifier,
        profile: LocalStoreSecretProfile,
        source: KeyringError,
    },
    /// First-run generation of a store-secret key failed.
    #[snafu(display("Failed to generate local store-secret key: {source}"))]
    #[cfg(feature = "local-secret-manager")]
    GenerateKey { source: SecurityError },
    /// Writing the newly generated local secret failed.
    #[snafu(display(
        "Failed to write local store secret for application '{application_id}' and profile '{profile}': {source}"
    ))]
    #[cfg(feature = "local-secret-manager")]
    Write {
        application_id: Identifier,
        profile: LocalStoreSecretProfile,
        source: KeyringError,
    },
    /// The selected local secret entry exists but does not match the expected record format.
    #[snafu(display(
        "Stored local secret for application '{application_id}' and profile '{profile}' is invalid: {message}"
    ))]
    InvalidRecord {
        application_id: Identifier,
        profile: LocalStoreSecretProfile,
        message: String,
    },
}

/// Separator chosen for our readable account string: `flotsync/<application>/<profile>`.
const LOCAL_STORE_SECRET_ACCOUNT_SEPARATOR: char = '/';
/// Android's native keyring backend internally joins service and account with this divider.
///
/// It is not our account separator. We reject it inside caller-controlled
/// account components so Android cannot reinterpret two distinct selectors as
/// the same native key.
const ANDROID_NATIVE_STORE_DIVIDER: &str = "\u{FEFF}@\u{FEFF}";
const PORTABLE_SELECTOR_ANDROID_DIVIDER_MESSAGE: &str =
    "must not contain the Android keyring divider";
const PORTABLE_SELECTOR_ACCOUNT_SEPARATOR_MESSAGE: &str =
    "must not contain the local account separator '/'";
const PORTABLE_SELECTOR_CHARACTER_SET_MESSAGE: &str =
    "must contain only lower-case ASCII letters, digits, '.', ':', '_', or '-'";

/// Validate one selector component against the portable keyring account subset.
fn ensure_portable_selector(value: &str) -> Result<(), &'static str> {
    fn is_portable_account_char(character: char) -> bool {
        character.is_ascii_lowercase()
            || character.is_ascii_digit()
            || matches!(character, '.' | ':' | '_' | '-')
    }

    if value.contains(ANDROID_NATIVE_STORE_DIVIDER) {
        return Err(PORTABLE_SELECTOR_ANDROID_DIVIDER_MESSAGE);
    }
    if value.contains(LOCAL_STORE_SECRET_ACCOUNT_SEPARATOR) {
        return Err(PORTABLE_SELECTOR_ACCOUNT_SEPARATOR_MESSAGE);
    }
    if !value.chars().all(is_portable_account_char) {
        return Err(PORTABLE_SELECTOR_CHARACTER_SET_MESSAGE);
    }
    Ok(())
}
