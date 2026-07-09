//! Unmanaged local store-secret fallback for feature-minimal builds.

use super::{
    LoadedLocalStoreSecret,
    LocalSecretManagerUnavailableSnafu,
    LocalStoreSecretProfile,
    LocalStoreSecretResult,
};
use flotsync_core::member::Identifier;

/// Report unavailable local secret-manager support in feature-minimal builds.
///
/// # Errors
///
/// Always returns [`LocalStoreSecretError::LocalSecretManagerUnavailable`](super::LocalStoreSecretError::LocalSecretManagerUnavailable).
pub fn load_local_store_secret(
    _application_id: &Identifier,
    _profile: &LocalStoreSecretProfile,
) -> LocalStoreSecretResult<LoadedLocalStoreSecret> {
    LocalSecretManagerUnavailableSnafu.fail()
}

/// Report unavailable local secret-manager support in feature-minimal builds.
///
/// # Errors
///
/// Always returns [`LocalStoreSecretError::LocalSecretManagerUnavailable`](super::LocalStoreSecretError::LocalSecretManagerUnavailable).
pub fn load_or_create_local_store_secret(
    _application_id: &Identifier,
    _profile: &LocalStoreSecretProfile,
) -> LocalStoreSecretResult<LoadedLocalStoreSecret> {
    LocalSecretManagerUnavailableSnafu.fail()
}
