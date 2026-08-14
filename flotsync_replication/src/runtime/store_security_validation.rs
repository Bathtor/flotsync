use crate::{
    api::{
        BoxError,
        EncryptedGroupSecurityMaterial,
        LoadError,
        LoadSecurityError,
        ReplicationGroupRecord,
        ReplicationStore,
        STORE_SECRET_CRYPTO_V1,
        StoreError,
        StoreErrorClassification,
        StoreErrorClassificationSource,
        StoreSecretKeyId,
        load_error,
        load_security_error,
    },
    delivery::security::DeliverySecurityError,
};
use flotsync_core::{ApplicationId, GroupId, MemberIdentity, membership::GroupMembersError};
use flotsync_security::STORE_SECRET_NONCE_LENGTH;
use snafu::{IntoError, prelude::*};
use std::sync::Arc;

/// Validate persisted group security metadata required during runtime startup.
pub(super) async fn validate_loaded_group_security(
    application_id: ApplicationId,
    store: Arc<dyn ReplicationStore>,
    expected_store_secret_key_id: &StoreSecretKeyId,
) -> Result<(), RuntimeSecurityLoadError> {
    let mut transaction = store
        .begin_read_transaction()
        .await
        .context(StoreAccessSnafu {
            application_id: application_id.clone(),
        })?;
    let groups = transaction
        .load_replication_groups()
        .await
        .context(StoreAccessSnafu {
            application_id: application_id.clone(),
        })?;
    transaction
        .release()
        .await
        .context(StoreAccessSnafu { application_id })?;

    for group in &groups {
        validate_replication_group_security(expected_store_secret_key_id, group)?;
    }
    Ok(())
}

/// Build the public load error variant while keeping the concrete security type boxed.
#[track_caller]
pub(super) fn security_load_error(
    application_id: ApplicationId,
    source: LoadSecurityError,
) -> LoadError {
    let source: Box<LoadSecurityError> = source.into();
    load_error::SecuritySnafu { application_id }.into_error(source)
}

/// Translate local-member security setup failures into caller-actionable public errors.
#[track_caller]
pub(super) fn load_security_error_from_local_member(
    local_member: &MemberIdentity,
    source: DeliverySecurityError,
) -> LoadSecurityError {
    match source {
        DeliverySecurityError::InvalidLocalPrivateKeys { .. }
        | DeliverySecurityError::OpenLocalPrivateKeys { .. }
        | DeliverySecurityError::UnsupportedStoreSecretVersion { .. }
        | DeliverySecurityError::InvalidStoreSecretNonce { .. } => {
            invalid_local_private_keys(local_member.clone(), source)
        }
        other => classified_or_other_load_security_error(other),
    }
}

/// Translate stored-group security-readiness failures into public load errors.
#[track_caller]
pub(super) fn load_security_error_from_runtime(
    source: RuntimeSecurityLoadError,
) -> LoadSecurityError {
    match source {
        RuntimeSecurityLoadError::InvalidGroupMembers { group_id, source } => {
            LoadSecurityError::StoredGroupInvalidMembers { group_id, source }
        }
        RuntimeSecurityLoadError::KeyIdMismatch {
            group_id,
            expected,
            actual,
        } => LoadSecurityError::StoredGroupKeyIdMismatch {
            group_id,
            expected,
            actual,
        },
        RuntimeSecurityLoadError::UnsupportedStoreSecretVersion {
            group_id,
            version,
            supported,
        } => LoadSecurityError::StoredGroupUnsupportedStoreSecretVersion {
            group_id,
            version,
            supported,
        },
        RuntimeSecurityLoadError::InvalidNonceLength {
            group_id,
            expected,
            actual,
        } => LoadSecurityError::StoredGroupInvalidGroupSecretNonceLength {
            group_id,
            expected,
            actual,
        },
        other @ RuntimeSecurityLoadError::StoreAccess { .. } => {
            classified_or_other_load_security_error(other)
        }
    }
}

/// Validate one stored group's security metadata and member identity shape.
fn validate_replication_group_security(
    expected_store_secret_key_id: &StoreSecretKeyId,
    group: &ReplicationGroupRecord,
) -> Result<(), RuntimeSecurityLoadError> {
    let group_id = group.group_id;
    validate_stored_group_security_material(
        group_id,
        expected_store_secret_key_id,
        &group.security_material,
    )?;
    group
        .member_keys
        .to_group_members()
        .context(InvalidGroupMembersSnafu { group_id })?;
    Ok(())
}

/// Validate metadata that can be checked without decrypting the group secret.
fn validate_stored_group_security_material(
    group_id: GroupId,
    expected_store_secret_key_id: &StoreSecretKeyId,
    security_material: &EncryptedGroupSecurityMaterial,
) -> Result<(), RuntimeSecurityLoadError> {
    let secret = &security_material.encrypted_group_secret;
    ensure!(
        &secret.key_id == expected_store_secret_key_id,
        KeyIdMismatchSnafu {
            group_id,
            expected: *expected_store_secret_key_id,
            actual: secret.key_id,
        }
    );
    let version = secret.crypto_version.as_u16();
    let supported = STORE_SECRET_CRYPTO_V1.as_u16();
    ensure!(
        version == supported,
        UnsupportedStoreSecretVersionSnafu {
            group_id,
            version,
            supported,
        }
    );
    let actual = secret.nonce.len();
    ensure!(
        actual == STORE_SECRET_NONCE_LENGTH,
        InvalidNonceLengthSnafu {
            group_id,
            expected: STORE_SECRET_NONCE_LENGTH,
            actual,
        }
    );
    Ok(())
}

/// Attach the original local-key failure as non-public source context.
#[track_caller]
fn invalid_local_private_keys(
    member_id: MemberIdentity,
    source: DeliverySecurityError,
) -> LoadSecurityError {
    let source: BoxError = source.into();
    load_security_error::InvalidLocalPrivateKeysSnafu { member_id }.into_error(source)
}

/// Preserve non-actionable internals as source context without making them public API.
#[track_caller]
fn other_load_security_error(
    source: impl std::error::Error + Send + Sync + 'static,
) -> LoadSecurityError {
    let source: BoxError = source.into();
    load_security_error::OtherSnafu.into_error(source)
}

/// Preserve a store classification when `source` exposes one, or retain it as an ordinary internal
/// security-loading error otherwise.
#[track_caller]
fn classified_or_other_load_security_error<E>(source: E) -> LoadSecurityError
where
    E: StoreErrorClassificationSource + Into<BoxError> + 'static,
{
    if let Some(classification) = source.store_error_classification() {
        // Snapshot the small classification at this type-erasure boundary so this variant is
        // always classified while `BoxError` retains ordinary Snafu sourcing and downcasting.
        LoadSecurityError::StoreAccess {
            classification,
            source: source.into(),
        }
    } else {
        other_load_security_error(source)
    }
}

/// Internal security-readiness failure wrapped by the public [`LoadError::Security`] variant.
#[derive(Debug, Snafu)]
pub(super) enum RuntimeSecurityLoadError {
    /// Store access failed while reading security readiness state.
    #[snafu(display(
        "Failed to load security records for application '{application_id}': {source}"
    ))]
    StoreAccess {
        application_id: ApplicationId,
        source: StoreError,
    },
    /// A persisted group record no longer satisfies group-member invariants.
    #[snafu(display("Stored replication group {group_id} has invalid members: {source}"))]
    InvalidGroupMembers {
        group_id: GroupId,
        source: GroupMembersError,
    },
    /// A persisted group's encrypted group secret uses a different key id.
    #[snafu(display(
        "Stored replication group {group_id} uses store-secret key id {actual}; expected {expected}."
    ))]
    KeyIdMismatch {
        group_id: GroupId,
        expected: StoreSecretKeyId,
        actual: StoreSecretKeyId,
    },
    /// A persisted group's encrypted group secret uses an unsupported crypto version.
    #[snafu(display(
        "Stored replication group {group_id} uses unsupported store-secret crypto version {version}; supported version is {supported}."
    ))]
    UnsupportedStoreSecretVersion {
        group_id: GroupId,
        version: u16,
        supported: u16,
    },
    /// A persisted group's encrypted group secret has an invalid nonce width.
    #[snafu(display(
        "Stored replication group {group_id} has encrypted group-secret nonce length {actual}; expected {expected}."
    ))]
    InvalidNonceLength {
        group_id: GroupId,
        expected: usize,
        actual: usize,
    },
}

impl StoreErrorClassificationSource for RuntimeSecurityLoadError {
    fn store_error_classification(&self) -> Option<StoreErrorClassification> {
        match self {
            Self::StoreAccess { source, .. } => source.store_error_classification(),
            Self::InvalidGroupMembers { .. }
            | Self::KeyIdMismatch { .. }
            | Self::UnsupportedStoreSecretVersion { .. }
            | Self::InvalidNonceLength { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{StoreErrorClass, StoreErrorResolution, StoreErrorScope};

    #[test]
    fn runtime_security_store_failure_reaches_public_load_error() {
        let classification = StoreErrorClassification::UNKNOWN
            .with_scope(StoreErrorScope::Store)
            .with_class(StoreErrorClass::InvalidData)
            .with_resolution(StoreErrorResolution::Repair);
        let store_error = StoreError::new(
            classification,
            std::io::Error::other("injected security store failure"),
        );
        let runtime_error = RuntimeSecurityLoadError::StoreAccess {
            application_id: ApplicationId::from_array(["test-application"]),
            source: store_error,
        };

        let security_error = load_security_error_from_runtime(runtime_error);
        let error = security_load_error(
            ApplicationId::from_array(["test-application"]),
            security_error,
        );

        assert_eq!(error.store_error_classification(), Some(classification));
        let LoadError::Security { source, .. } = error else {
            panic!("unexpected load error")
        };
        let LoadSecurityError::StoreAccess { source, .. } = source.as_ref() else {
            panic!("unexpected security load error")
        };
        assert!(source.downcast_ref::<RuntimeSecurityLoadError>().is_some());
    }
}
