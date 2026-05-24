//! Temporary application-side security provisioning helpers.
//!
//! These functions exist for the current replicated-checklist security slice.
//! Long-term, application code should not manually provision these store
//! records before runtime start; `flotsync-uohh` tracks replacing this setup
//! path with the next security-configuration shape.

use crate::{
    api::{
        BoxError,
        EncryptedGroupSecurityMaterial,
        EncryptedLocalMemberPrivateKeys,
        EncryptedStoreSecret,
        GroupId,
        LocalMemberPrivateKeysRecord,
        MemberIdentity,
        ReplicationSecuritySecrets,
        ReplicationStore,
        ReplicationStoreTransaction,
        StoreError,
        TrustedMemberPublicKeysRecord,
    },
    delivery::security::{
        LOGICAL_GROUP_SECRET_COLUMN,
        LOGICAL_GROUP_TABLE,
        LOGICAL_LOCAL_MEMBER_TABLE,
        LOGICAL_LOCAL_PRIVATE_KEYS_COLUMN,
    },
};
use flotsync_security::{
    GroupKey,
    PublicMemberKeys,
    STORE_SECRET_CRYPTO_VERSION_V1,
    StoreSecretContext,
    local_member_keys_from_jwks,
    open_store_secret,
    open_stored_group_key,
    public_member_keys_from_jwks,
    seal_store_secret,
};
use snafu::prelude::*;
use std::collections::HashSet;

/// Result of temporarily provisioning member key records into a replication store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProvisionedReplicationSecurity {
    /// Local member whose private key bundle was provisioned or confirmed.
    pub local_member: MemberIdentity,
    /// Trusted remote/public members parsed from the supplied public JWKS inputs.
    pub trusted_members: Vec<MemberIdentity>,
}

/// Errors raised by temporary replicated-checklist security provisioning.
#[derive(Debug, Snafu)]
#[snafu(module(provision_security_error))]
pub enum ProvisionSecurityError {
    /// Store access failed while writing or checking provisioned security records.
    #[snafu(display("Failed to access replication store while provisioning security: {source}"))]
    StoreAccess { source: StoreError },
    /// The configured local private JWKS did not parse or did not match the configured member.
    #[snafu(display("Local private JWKS for member {member_id} is invalid: {source}"))]
    InvalidLocalPrivateJwks {
        member_id: MemberIdentity,
        source: BoxError,
    },
    /// One configured trusted public JWKS did not parse into usable public member keys.
    #[snafu(display("Trusted public JWKS is invalid: {source}"))]
    InvalidTrustedPublicJwks { source: BoxError },
    /// The same trusted public member identity was supplied more than once.
    #[snafu(display("Trusted public JWKS for member {member_id} was supplied more than once."))]
    DuplicateTrustedPublicJwks { member_id: MemberIdentity },
    /// The store already contains different local private-key material.
    #[snafu(display(
        "Stored local private keys for member {member_id} differ from configured JWKS."
    ))]
    LocalPrivateKeysMismatch { member_id: MemberIdentity },
    /// The stored local private-key record uses a different store-secret key id.
    #[snafu(display(
        "Stored local private keys for member {member_id} use store-secret key id {actual}; expected {expected}."
    ))]
    LocalPrivateKeysKeyIdMismatch {
        member_id: MemberIdentity,
        expected: crate::api::StoreSecretKeyId,
        actual: crate::api::StoreSecretKeyId,
    },
    /// Local private-key material could not be encrypted for storage.
    #[snafu(display("Failed to encrypt local private keys for member {member_id}: {source}"))]
    SealLocalPrivateKeys {
        member_id: MemberIdentity,
        source: BoxError,
    },
    /// Stored local private-key material could not be opened for comparison.
    #[snafu(display("Failed to open stored local private keys for member {member_id}: {source}"))]
    OpenStoredLocalPrivateKeys {
        member_id: MemberIdentity,
        source: BoxError,
    },
    /// Initial group security material could not be encrypted for storage.
    #[snafu(display("Failed to encrypt initial group secret: {source}"))]
    SealGroupSecret { source: BoxError },
    /// The stored initial group secret uses a different store-secret key id.
    #[snafu(display(
        "Stored initial group secret for group {group_id} uses store-secret key id {actual}; expected {expected}."
    ))]
    GroupSecretKeyIdMismatch {
        group_id: GroupId,
        expected: crate::api::StoreSecretKeyId,
        actual: crate::api::StoreSecretKeyId,
    },
    /// Stored initial group security material could not be opened for comparison.
    #[snafu(display("Failed to open stored initial group secret for group {group_id}: {source}"))]
    OpenStoredGroupSecret { group_id: GroupId, source: BoxError },
    /// The stored initial group secret does not match the configured group secret.
    #[snafu(display(
        "Stored initial group secret for group {group_id} differs from configured group-secret-password."
    ))]
    GroupSecretMismatch { group_id: GroupId },
}

/// Temporarily provision local private and trusted public member keys into a store.
///
/// This is application setup support for the replicated-checklist MVP only. It
/// deliberately keeps TOML/file handling outside `flotsync_replication`, while
/// centralising the encrypted store-record details that callers should not
/// duplicate.
///
/// # Errors
///
/// Returns [`ProvisionSecurityError`] when key parsing fails, store access
/// fails, configured trusted members are duplicated, or existing local key
/// material conflicts with the supplied local private JWKS.
pub async fn provision_replication_security<'a>(
    store: &dyn ReplicationStore,
    local_member: &MemberIdentity,
    security: &ReplicationSecuritySecrets,
    local_private_jwks: &str,
    trusted_public_jwks: impl IntoIterator<Item = &'a str>,
) -> Result<ProvisionedReplicationSecurity, ProvisionSecurityError> {
    local_member_keys_from_jwks(local_private_jwks, Some(local_member))
        .boxed()
        .context(provision_security_error::InvalidLocalPrivateJwksSnafu {
            member_id: local_member.clone(),
        })?;
    let trusted_public_keys = parse_trusted_public_keys(trusted_public_jwks)?;

    let mut transaction = store
        .begin_transaction()
        .await
        .context(provision_security_error::StoreAccessSnafu)?;
    provision_local_private_keys(
        transaction.as_mut(),
        local_member,
        security,
        local_private_jwks,
    )
    .await?;
    for public_keys in &trusted_public_keys {
        transaction
            .ensure_trusted_member_public_keys(trusted_public_keys_record(public_keys))
            .await
            .context(provision_security_error::StoreAccessSnafu)?;
    }
    transaction
        .commit()
        .await
        .context(provision_security_error::StoreAccessSnafu)?;

    Ok(ProvisionedReplicationSecurity {
        local_member: local_member.clone(),
        trusted_members: trusted_public_keys
            .iter()
            .map(|keys| keys.member_id().clone())
            .collect(),
    })
}

/// Temporarily prepare encrypted group-security material for an initial static group.
///
/// This helper is intentionally narrow and temporary: it exists so the
/// replicated-checklist example does not duplicate encrypted-cell contexts
/// while `flotsync-uohh` and the next setup shape are still pending.
///
/// # Errors
///
/// Returns [`ProvisionSecurityError::SealGroupSecret`] if encrypting the group
/// secret fails.
pub fn prepare_initial_group_security_material(
    group_id: crate::api::GroupId,
    security: &ReplicationSecuritySecrets,
    group_key: &GroupKey,
) -> Result<EncryptedGroupSecurityMaterial, ProvisionSecurityError> {
    let context = StoreSecretContext {
        table: LOGICAL_GROUP_TABLE,
        column: LOGICAL_GROUP_SECRET_COLUMN,
        row_id: group_id.0.as_bytes(),
        key_id: security.store_secret_key_id().as_bytes(),
        crypto_version: STORE_SECRET_CRYPTO_VERSION_V1,
    };
    let plaintext = group_key.stored_secret_plaintext();
    let sealed = seal_store_secret(security.store_secret_key(), context, plaintext.as_slice())
        .boxed()
        .context(provision_security_error::SealGroupSecretSnafu)?;
    Ok(EncryptedGroupSecurityMaterial {
        encrypted_group_secret: EncryptedStoreSecret::from_store_secret_ciphertext(
            *security.store_secret_key_id(),
            sealed,
        ),
    })
}

/// Confirm stored initial group-security material matches configured setup.
///
/// This helper is intentionally temporary and scoped to the replicated-checklist
/// file-config setup. It lets the example fail early when a local store already
/// contains a static group created under a different shared group password.
///
/// # Errors
///
/// Returns [`ProvisionSecurityError`] if the stored encrypted group secret
/// cannot be opened with the configured store secret or does not match the
/// configured group key.
pub fn validate_initial_group_security_material(
    group_id: crate::api::GroupId,
    security: &ReplicationSecuritySecrets,
    group_key: &GroupKey,
    security_material: &EncryptedGroupSecurityMaterial,
) -> Result<(), ProvisionSecurityError> {
    let secret = &security_material.encrypted_group_secret;
    ensure!(
        &secret.key_id == security.store_secret_key_id(),
        provision_security_error::GroupSecretKeyIdMismatchSnafu {
            group_id,
            expected: *security.store_secret_key_id(),
            actual: secret.key_id,
        }
    );
    let sealed = secret
        .to_store_secret_ciphertext()
        .boxed()
        .context(provision_security_error::OpenStoredGroupSecretSnafu { group_id })?;
    let context = StoreSecretContext {
        table: LOGICAL_GROUP_TABLE,
        column: LOGICAL_GROUP_SECRET_COLUMN,
        row_id: group_id.0.as_bytes(),
        key_id: security.store_secret_key_id().as_bytes(),
        crypto_version: STORE_SECRET_CRYPTO_VERSION_V1,
    };
    let stored_group_key = open_stored_group_key(security.store_secret_key(), context, &sealed)
        .boxed()
        .context(provision_security_error::OpenStoredGroupSecretSnafu { group_id })?;
    ensure!(
        &stored_group_key == group_key,
        provision_security_error::GroupSecretMismatchSnafu { group_id }
    );
    Ok(())
}

/// Parse all trusted public JWKS inputs and reject duplicate member identities.
fn parse_trusted_public_keys<'a>(
    trusted_public_jwks: impl IntoIterator<Item = &'a str>,
) -> Result<Vec<PublicMemberKeys>, ProvisionSecurityError> {
    let mut seen_members = HashSet::new();
    let mut parsed = Vec::new();
    for jwks in trusted_public_jwks {
        // Temporary MVP limitation: file/path context lives in the example layer,
        // while this helper only sees JWKS text. Keep this intentionally simple
        // until `flotsync-uohh` removes the application-side provisioning path.
        let public_keys = public_member_keys_from_jwks(jwks, None)
            .boxed()
            .context(provision_security_error::InvalidTrustedPublicJwksSnafu)?;
        ensure!(
            seen_members.insert(public_keys.member_id().clone()),
            provision_security_error::DuplicateTrustedPublicJwksSnafu {
                member_id: public_keys.member_id().clone(),
            }
        );
        parsed.push(public_keys);
    }
    Ok(parsed)
}

/// Write the local private-key record or validate a previously provisioned record.
async fn provision_local_private_keys(
    transaction: &mut dyn ReplicationStoreTransaction,
    local_member: &MemberIdentity,
    security: &ReplicationSecuritySecrets,
    local_private_jwks: &str,
) -> Result<(), ProvisionSecurityError> {
    let existing = transaction
        .load_local_member_private_keys(local_member)
        .await
        .context(provision_security_error::StoreAccessSnafu)?;
    if let Some(existing) = existing {
        confirm_existing_local_private_keys(local_member, security, local_private_jwks, &existing)?;
        return Ok(());
    }

    let record = local_private_keys_record(local_member, security, local_private_jwks)?;
    transaction
        .ensure_local_member_private_keys(record)
        .await
        .context(provision_security_error::StoreAccessSnafu)
}

/// Confirm that an existing encrypted local-key record matches the configured JWKS.
fn confirm_existing_local_private_keys(
    local_member: &MemberIdentity,
    security: &ReplicationSecuritySecrets,
    expected_jwks: &str,
    existing: &LocalMemberPrivateKeysRecord,
) -> Result<(), ProvisionSecurityError> {
    let secret = &existing.private_keys.secret;
    ensure!(
        &secret.key_id == security.store_secret_key_id(),
        provision_security_error::LocalPrivateKeysKeyIdMismatchSnafu {
            member_id: local_member.clone(),
            expected: *security.store_secret_key_id(),
            actual: secret.key_id,
        }
    );
    let sealed = secret.to_store_secret_ciphertext().boxed().context(
        provision_security_error::OpenStoredLocalPrivateKeysSnafu {
            member_id: local_member.clone(),
        },
    )?;
    let row_id = local_member.to_string();
    let context = StoreSecretContext {
        table: LOGICAL_LOCAL_MEMBER_TABLE,
        column: LOGICAL_LOCAL_PRIVATE_KEYS_COLUMN,
        row_id: row_id.as_bytes(),
        key_id: security.store_secret_key_id().as_bytes(),
        crypto_version: STORE_SECRET_CRYPTO_VERSION_V1,
    };
    let plaintext = open_store_secret(security.store_secret_key(), context, &sealed)
        .boxed()
        .context(provision_security_error::OpenStoredLocalPrivateKeysSnafu {
            member_id: local_member.clone(),
        })?;
    ensure!(
        plaintext.as_slice() == expected_jwks.as_bytes(),
        provision_security_error::LocalPrivateKeysMismatchSnafu {
            member_id: local_member.clone(),
        }
    );
    Ok(())
}

/// Build the encrypted local-private key record for first-time setup.
fn local_private_keys_record(
    local_member: &MemberIdentity,
    security: &ReplicationSecuritySecrets,
    local_private_jwks: &str,
) -> Result<LocalMemberPrivateKeysRecord, ProvisionSecurityError> {
    let row_id = local_member.to_string();
    let context = StoreSecretContext {
        table: LOGICAL_LOCAL_MEMBER_TABLE,
        column: LOGICAL_LOCAL_PRIVATE_KEYS_COLUMN,
        row_id: row_id.as_bytes(),
        key_id: security.store_secret_key_id().as_bytes(),
        crypto_version: STORE_SECRET_CRYPTO_VERSION_V1,
    };
    let sealed = seal_store_secret(
        security.store_secret_key(),
        context,
        local_private_jwks.as_bytes(),
    )
    .boxed()
    .context(provision_security_error::SealLocalPrivateKeysSnafu {
        member_id: local_member.clone(),
    })?;
    Ok(LocalMemberPrivateKeysRecord {
        member_id: local_member.clone(),
        private_keys: EncryptedLocalMemberPrivateKeys {
            secret: EncryptedStoreSecret::from_store_secret_ciphertext(
                *security.store_secret_key_id(),
                sealed,
            ),
        },
    })
}

/// Convert typed public member keys into the store's opaque trusted-key record.
fn trusted_public_keys_record(public_keys: &PublicMemberKeys) -> TrustedMemberPublicKeysRecord {
    TrustedMemberPublicKeysRecord {
        member_id: public_keys.member_id().clone(),
        signing_public_key: public_keys.signing_key_bytes().into(),
        encryption_public_key: public_keys.encryption_key_bytes().into(),
    }
}
