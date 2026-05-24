use crate::{STORE_SECRET_KEY_LENGTH, SecurityError, StoreSecretKey, StoreSecretKeyId};
use flotsync_core::member::Identifier;
use keyring_core::{Entry, Error as KeyringError};
use snafu::prelude::*;
use std::fmt;
use zeroize::Zeroizing;

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

    /// Encode the keyring record format for this generated local secret.
    fn encode_record(&self) -> Zeroizing<Vec<u8>> {
        let mut record = Zeroizing::new(Vec::with_capacity(
            LOCAL_STORE_SECRET_RECORD_MAGIC.len()
                + 1
                + LOCAL_STORE_SECRET_KEY_ID_LENGTH
                + STORE_SECRET_KEY_LENGTH,
        ));
        record.extend_from_slice(LOCAL_STORE_SECRET_RECORD_MAGIC);
        record.push(LOCAL_STORE_SECRET_RECORD_VERSION);
        record.extend_from_slice(self.key_id.as_bytes());
        record.extend_from_slice(self.store_secret_key.as_bytes());
        record
    }

    /// Decode one keyring record into the runtime store-secret input.
    fn decode_record(
        application_id: &Identifier,
        profile: &LocalStoreSecretProfile,
        record: &[u8],
    ) -> LocalStoreSecretResult<Self> {
        let Some(remainder) = record.strip_prefix(LOCAL_STORE_SECRET_RECORD_MAGIC) else {
            return InvalidRecordSnafu {
                application_id: application_id.clone(),
                profile: profile.clone(),
                message: "record magic did not match".to_string(),
            }
            .fail();
        };
        let Some((&version, remainder)) = remainder.split_first() else {
            return InvalidRecordSnafu {
                application_id: application_id.clone(),
                profile: profile.clone(),
                message: "record ended before fixed fields".to_string(),
            }
            .fail();
        };
        ensure!(
            version == LOCAL_STORE_SECRET_RECORD_VERSION,
            InvalidRecordSnafu {
                application_id: application_id.clone(),
                profile: profile.clone(),
                message: format!(
                    "record version {version} is unsupported; expected {LOCAL_STORE_SECRET_RECORD_VERSION}"
                ),
            }
        );
        let expected_remainder = LOCAL_STORE_SECRET_KEY_ID_LENGTH + STORE_SECRET_KEY_LENGTH;
        ensure!(
            remainder.len() == expected_remainder,
            InvalidRecordSnafu {
                application_id: application_id.clone(),
                profile: profile.clone(),
                message: format!(
                    "record body was {} bytes; expected {expected_remainder}",
                    remainder.len()
                ),
            }
        );
        let (key_id, key) = remainder.split_at(LOCAL_STORE_SECRET_KEY_ID_LENGTH);
        let key_id = StoreSecretKeyId::from_bytes(
            key_id
                .try_into()
                .expect("validated local store-secret key id length"),
        );
        let store_secret_key = StoreSecretKey::try_from_slice(key).map_err(|source| {
            LocalStoreSecretError::InvalidRecord {
                application_id: application_id.clone(),
                profile: profile.clone(),
                message: source.to_string(),
            }
        })?;
        Ok(Self {
            key_id,
            store_secret_key,
        })
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
    /// The process could not initialise the configured local secret storage.
    #[snafu(display("Failed to initialise local store-secret storage: {source}"))]
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
    Read {
        application_id: Identifier,
        profile: LocalStoreSecretProfile,
        source: KeyringError,
    },
    /// First-run generation of a store-secret key failed.
    #[snafu(display("Failed to generate local store-secret key: {source}"))]
    GenerateKey { source: SecurityError },
    /// Writing the newly generated local secret failed.
    #[snafu(display(
        "Failed to write local store secret for application '{application_id}' and profile '{profile}': {source}"
    ))]
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

/// Load an existing store secret for one application/profile slot.
///
/// # Errors
///
/// Returns [`LocalStoreSecretError::Missing`] if the slot has not been
/// initialised, or another [`LocalStoreSecretError`] when local secret storage
/// cannot be accessed or the stored record is malformed.
pub fn load_local_store_secret(
    application_id: &Identifier,
    profile: &LocalStoreSecretProfile,
) -> LocalStoreSecretResult<LoadedLocalStoreSecret> {
    let entry = local_store_secret_entry(application_id, profile)?;
    let record = read_local_store_secret_record(&entry, application_id, profile)?;
    LoadedLocalStoreSecret::decode_record(application_id, profile, record.as_slice())
}

/// Load an existing store secret, or create it on first run.
///
/// Contract: first-run creation for one application/profile is expected to be
/// driven by one process at a time. Concurrent loads are fine once the slot has
/// been initialised, but concurrent creators may overwrite each other because
/// keyring does not provide a portable create-if-absent operation.
///
/// # Errors
///
/// Returns [`LocalStoreSecretError`] if local secret storage cannot be accessed,
/// key generation fails, or the stored/generated record cannot be read back.
pub fn load_or_create_local_store_secret(
    application_id: &Identifier,
    profile: &LocalStoreSecretProfile,
) -> LocalStoreSecretResult<LoadedLocalStoreSecret> {
    match load_local_store_secret(application_id, profile) {
        Ok(secret) => Ok(secret),
        Err(LocalStoreSecretError::Missing { .. }) => {
            let entry = local_store_secret_entry(application_id, profile)?;
            let created = generate_local_store_secret()?;
            let record = created.encode_record();
            entry
                .set_secret(record.as_slice())
                .with_context(|_| WriteSnafu {
                    application_id: application_id.clone(),
                    profile: profile.clone(),
                })?;
            // Read the value back to increase the probability that we get a consistent value when
            // racing with another process trying to install the same profile.
            load_local_store_secret(application_id, profile)
        }
        Err(error) => Err(error),
    }
}

/// Keyring service name that scopes all flotsync local store-secret entries.
const LOCAL_STORE_SECRET_SERVICE: &str = "flotsync.local-store-secret.v1";
/// Prefix used to make flotsync-owned account names recognisable in keyring tools.
const LOCAL_STORE_SECRET_ACCOUNT_PREFIX: &str = "flotsync";
/// Short marker that distinguishes this binary record from unrelated keyring bytes.
const LOCAL_STORE_SECRET_RECORD_MAGIC: &[u8] = b"fs-lss";
/// Binary record version for the current magic-prefixed local secret payload.
const LOCAL_STORE_SECRET_RECORD_VERSION: u8 = 1;
/// Length of the generated key id stored before the secret key bytes.
const LOCAL_STORE_SECRET_KEY_ID_LENGTH: usize = StoreSecretKeyId::BYTE_LENGTH;
/// Separator chosen for our readable account string: `flotsync/<application>/<profile>`.
const LOCAL_STORE_SECRET_ACCOUNT_SEPARATOR: char = '/';
/// Conservative account-name limit shared by the currently supported keyring backends.
const LOCAL_STORE_SECRET_ACCOUNT_MAX_BYTES: usize = 512;
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

/// Build the keyring entry address for one application/profile slot.
fn local_store_secret_entry(
    application_id: &Identifier,
    profile: &LocalStoreSecretProfile,
) -> LocalStoreSecretResult<Entry> {
    ensure_default_local_secret_store()?;
    let account = local_store_secret_account(application_id, profile)?;
    Entry::new(LOCAL_STORE_SECRET_SERVICE, &account).with_context(|_| BuildEntrySnafu {
        application_id: application_id.clone(),
        profile: profile.clone(),
    })
}

/// Ensure callers get a usable keyring backend before entry construction.
fn ensure_default_local_secret_store() -> LocalStoreSecretResult<()> {
    if keyring_core::get_default_store().is_some() {
        return Ok(());
    }
    install_default_local_secret_store()
}

/*
 * We intentionally initialise the backing stores directly instead of depending on
 * the `keyring` convenience crate. Upstream treats that crate as sample/glue
 * code, and its dependency graph includes stores we do not use. Local
 * store-secret loading should either use a persistent backend we chose
 * explicitly or fail with an actionable setup error.
 */

/// Install the persistent OS-backed keyring store selected for this target.
#[cfg(target_os = "macos")]
fn install_default_local_secret_store() -> LocalStoreSecretResult<()> {
    let store =
        apple_native_keyring_store::keychain::Store::new().context(InitialiseStorageSnafu)?;
    keyring_core::set_default_store(store);
    Ok(())
}

/// Install the persistent OS-backed keyring store selected for this target.
#[cfg(target_os = "ios")]
fn install_default_local_secret_store() -> LocalStoreSecretResult<()> {
    let store =
        apple_native_keyring_store::protected::Store::new().context(InitialiseStorageSnafu)?;
    keyring_core::set_default_store(store);
    Ok(())
}

/// Install the persistent OS-backed keyring store selected for this target.
#[cfg(target_os = "android")]
fn install_default_local_secret_store() -> LocalStoreSecretResult<()> {
    let store = android_native_keyring_store::Store::new().context(InitialiseStorageSnafu)?;
    keyring_core::set_default_store(store);
    Ok(())
}

/// Install the persistent OS-backed keyring store selected for this target.
#[cfg(target_os = "windows")]
fn install_default_local_secret_store() -> LocalStoreSecretResult<()> {
    let store = windows_native_keyring_store::Store::new().context(InitialiseStorageSnafu)?;
    keyring_core::set_default_store(store);
    Ok(())
}

/// Install the persistent OS-backed keyring store selected for this target.
#[cfg(target_os = "linux")]
fn install_default_local_secret_store() -> LocalStoreSecretResult<()> {
    let store = dbus_secret_service_keyring_store::Store::new().context(InitialiseStorageSnafu)?;
    keyring_core::set_default_store(store);
    Ok(())
}

/// Fail fast on targets without a chosen persistent OS-backed store.
#[cfg(not(any(
    target_os = "android",
    target_os = "ios",
    target_os = "linux",
    target_os = "macos",
    target_os = "windows",
)))]
fn install_default_local_secret_store() -> LocalStoreSecretResult<()> {
    UnsupportedPersistentStoreSnafu {
        target_os: std::env::consts::OS,
    }
    .fail()
}

/// Read the opaque stored keyring bytes for one local secret entry.
fn read_local_store_secret_record(
    entry: &Entry,
    application_id: &Identifier,
    profile: &LocalStoreSecretProfile,
) -> LocalStoreSecretResult<Zeroizing<Vec<u8>>> {
    match entry.get_secret() {
        Ok(record) => Ok(Zeroizing::new(record)),
        Err(KeyringError::NoEntry) => Err(LocalStoreSecretError::Missing {
            application_id: application_id.clone(),
            profile: profile.clone(),
        }),
        Err(source) => Err(LocalStoreSecretError::Read {
            application_id: application_id.clone(),
            profile: profile.clone(),
            source,
        }),
    }
}

/// Generate a new store-secret key and matching persistent key id.
fn generate_local_store_secret() -> LocalStoreSecretResult<LoadedLocalStoreSecret> {
    let store_secret_key = StoreSecretKey::generate().context(GenerateKeySnafu)?;
    let key_id = StoreSecretKeyId::generate();
    Ok(LoadedLocalStoreSecret {
        key_id,
        store_secret_key,
    })
}

/// Derive the keyring account name from non-secret application/profile context.
fn local_store_secret_account(
    application_id: &Identifier,
    profile: &LocalStoreSecretProfile,
) -> LocalStoreSecretResult<String> {
    let application_id = application_id.to_string();
    ensure_portable_selector(&application_id).map_err(|message| {
        LocalStoreSecretError::ApplicationIdNotPortable {
            application_id: application_id.clone(),
            message,
        }
    })?;
    let account = format!(
        "{LOCAL_STORE_SECRET_ACCOUNT_PREFIX}{LOCAL_STORE_SECRET_ACCOUNT_SEPARATOR}{application_id}{LOCAL_STORE_SECRET_ACCOUNT_SEPARATOR}{profile}"
    );
    ensure!(
        account.len() <= LOCAL_STORE_SECRET_ACCOUNT_MAX_BYTES,
        AccountTooLongSnafu {
            maximum: LOCAL_STORE_SECRET_ACCOUNT_MAX_BYTES,
            actual: account.len(),
        }
    );
    Ok(account)
}

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

/// Install the keyring sample store for tests before creating local entries.
///
/// Keyring's default store is process-global. Test code must agree on this
/// single sample store; installing a different store later is a test isolation
/// bug and this helper asserts rather than silently replacing it.
///
/// # Errors
///
/// This helper currently returns [`Ok`] after the one-time installation has
/// succeeded. The controlled sample-store configuration is expected to be
/// infallible for tests.
///
/// # Panics
///
/// Panics if the sample store cannot be installed or if another default store
/// replaced it after installation.
#[cfg(any(test, feature = "test-support"))]
pub fn install_local_store_secret_test_store() -> LocalStoreSecretResult<()> {
    use std::{collections::HashMap, sync::LazyLock};

    static INSTALL: LazyLock<()> = LazyLock::new(|| {
        let store = keyring_core::sample::Store::new_with_configuration(&HashMap::from([(
            "persist", "false",
        )]))
        .expect("local store-secret sample store should install for tests");
        keyring_core::set_default_store(store);
    });
    LazyLock::force(&INSTALL);
    let store = keyring_core::get_default_store()
        .expect("local store-secret sample store should be installed for tests");
    assert!(
        store.vendor().starts_with("Sample store,"),
        "local store-secret tests require the process-global sample keyring store"
    );
    Ok(())
}
