use crate::{
    api::{
        BoxError,
        EncryptedGroupSecurityMaterial,
        InvalidLocalPrivateKeysSnafu,
        LoadError,
        LoadSecurityError,
        OtherSnafu,
        ReplicationGroupRecord,
        ReplicationStore,
        STORE_SECRET_CRYPTO_V1,
        SecuritySnafu,
        StoreError,
        StoreSecretKeyId,
        StoredGroupInvalidTrustedPublicKeysSnafu,
    },
    delivery::security::{DeliverySecurity, DeliverySecurityError},
};
use flotsync_core::{
    GroupId,
    MemberIdentity,
    member::Identifier,
    membership::{GroupMembers, GroupMembersError},
};
use flotsync_security::STORE_SECRET_NONCE_LENGTH;
use snafu::{IntoError, prelude::*};
use std::sync::Arc;

/// Validate that every persisted group can load the security records required at runtime.
pub(super) async fn validate_loaded_group_security(
    application_id: Identifier,
    store: Arc<dyn ReplicationStore>,
    expected_store_secret_key_id: &StoreSecretKeyId,
    security: &DeliverySecurity,
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

    for group in groups {
        validate_replication_group_security(security, expected_store_secret_key_id, group).await?;
    }
    Ok(())
}

/// Build the public load error variant while keeping the concrete security type boxed.
#[track_caller]
pub(super) fn security_load_error(
    application_id: Identifier,
    source: LoadSecurityError,
) -> LoadError {
    let source: Box<LoadSecurityError> = source.into();
    SecuritySnafu { application_id }.into_error(source)
}

/// Translate local-member security setup failures into caller-actionable public errors.
#[track_caller]
pub(super) fn load_security_error_from_local_member(
    local_member: &MemberIdentity,
    source: DeliverySecurityError,
) -> LoadSecurityError {
    match source {
        DeliverySecurityError::MissingLocalPrivateKeys { member_id } => {
            LoadSecurityError::MissingLocalPrivateKeys { member_id }
        }
        DeliverySecurityError::InvalidLocalPrivateKeys { .. }
        | DeliverySecurityError::OpenLocalPrivateKeys { .. }
        | DeliverySecurityError::UnsupportedStoreSecretVersion { .. }
        | DeliverySecurityError::InvalidStoreSecretNonce { .. } => {
            invalid_local_private_keys(local_member.clone(), source)
        }
        other => other_load_security_error(other),
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
        RuntimeSecurityLoadError::GroupSecurity { group_id, source } => {
            load_security_error_from_group(group_id, source)
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
        other @ RuntimeSecurityLoadError::StoreAccess { .. } => other_load_security_error(other),
    }
}

/// Validate one stored group's security metadata and member trust records.
async fn validate_replication_group_security(
    security: &DeliverySecurity,
    expected_store_secret_key_id: &StoreSecretKeyId,
    group: ReplicationGroupRecord,
) -> Result<(), RuntimeSecurityLoadError> {
    let group_id = group.group_id;
    validate_stored_group_security_material(
        group_id,
        expected_store_secret_key_id,
        &group.security_material,
    )?;
    let members = GroupMembers::from_ordered_members(group.members)
        .context(InvalidGroupMembersSnafu { group_id })?;
    security
        .public_keys_for_members(&members)
        .await
        .context(GroupSecuritySnafu { group_id })?;
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

/// Translate group-member trust failures into public load errors.
#[track_caller]
fn load_security_error_from_group(
    group_id: GroupId,
    source: DeliverySecurityError,
) -> LoadSecurityError {
    match source {
        DeliverySecurityError::MissingTrustedPublicKeys { member_id } => {
            LoadSecurityError::StoredGroupMissingTrustedPublicKeys {
                group_id,
                member_id,
            }
        }
        DeliverySecurityError::InvalidTrustedPublicKeyLength {
            member_id,
            expected,
            actual,
        } => LoadSecurityError::StoredGroupInvalidTrustedPublicKeyLength {
            group_id,
            member_id,
            expected,
            actual,
        },
        DeliverySecurityError::InvalidTrustedPublicKeys { member_id, source } => {
            invalid_stored_group_trusted_public_keys(group_id, member_id, source)
        }
        other => other_load_security_error(other),
    }
}

/// Attach the original local-key failure as non-public source context.
#[track_caller]
fn invalid_local_private_keys(
    member_id: MemberIdentity,
    source: DeliverySecurityError,
) -> LoadSecurityError {
    let source: BoxError = source.into();
    InvalidLocalPrivateKeysSnafu { member_id }.into_error(source)
}

/// Attach the original trusted-key decode failure as non-public source context.
#[track_caller]
fn invalid_stored_group_trusted_public_keys(
    group_id: GroupId,
    member_id: MemberIdentity,
    source: BoxError,
) -> LoadSecurityError {
    StoredGroupInvalidTrustedPublicKeysSnafu {
        group_id,
        member_id,
    }
    .into_error(source)
}

/// Preserve non-actionable internals as source context without making them public API.
#[track_caller]
fn other_load_security_error(
    source: impl std::error::Error + Send + Sync + 'static,
) -> LoadSecurityError {
    let source: BoxError = source.into();
    OtherSnafu.into_error(source)
}

/// Internal security-readiness failure wrapped by the public [`LoadError::Security`] variant.
#[derive(Debug, Snafu)]
pub(super) enum RuntimeSecurityLoadError {
    /// Store access failed while reading security readiness state.
    #[snafu(display(
        "Failed to load security records for application '{application_id}': {source}"
    ))]
    StoreAccess {
        application_id: Identifier,
        source: StoreError,
    },
    /// A persisted group record no longer satisfies group-member invariants.
    #[snafu(display("Stored replication group {group_id} has invalid members: {source}"))]
    InvalidGroupMembers {
        group_id: GroupId,
        source: GroupMembersError,
    },
    /// A persisted group references members whose key material is unavailable or invalid.
    #[snafu(display(
        "Stored replication group {group_id} cannot load security material: {source}"
    ))]
    GroupSecurity {
        group_id: GroupId,
        source: DeliverySecurityError,
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
