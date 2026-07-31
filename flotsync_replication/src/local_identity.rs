//! Pre-runtime local identity provisioning.

use crate::{
    api::{
        BoxError,
        EncryptedLocalMemberPrivateKeys,
        EncryptedStoreSecret,
        LocalIdentityProvisioningStore,
        LocalMemberPrivateKeysRecord,
        MemberPublicKeysRecord,
        ReplicationSecuritySecrets,
        StoreError,
    },
    delivery::security::{LOGICAL_LOCAL_MEMBER_TABLE, LOGICAL_LOCAL_PRIVATE_KEYS_COLUMN},
};
use flotsync_core::MemberIdentity;
use flotsync_security::{
    LocalMemberKeys,
    PublicKeyBundle,
    STORE_SECRET_CRYPTO_VERSION_V1,
    StoreSecretContext,
    generate_member_key_bundles,
    local_member_keys_from_private_bundle,
    seal_store_secret,
};
use snafu::prelude::*;

/// Provision one local identity and its key material before runtime loading.
///
/// The store must not already contain local-private material for any member. This operation
/// generates fresh keys and commits the encrypted private record together with its matching public
/// binding in one transaction. On success, the store can be activated as a replication store.
///
/// # Errors
///
/// Returns [`ProvisionLocalIdentityError`] when the store is already provisioned or when store
/// access, key generation, private-key validation, or encrypted local storage fails.
pub async fn provision_local_identity(
    store: &dyn LocalIdentityProvisioningStore,
    local_member: MemberIdentity,
    security: &ReplicationSecuritySecrets,
) -> Result<ProvisionedLocalIdentity, ProvisionLocalIdentityError> {
    let existing_member = store
        .local_member_identity()
        .await
        .context(provision_local_identity_error::StoreAccessSnafu)?;
    if let Some(member_id) = existing_member {
        provision_local_identity_error::AlreadyProvisionedSnafu { member_id }.fail()?;
    }

    let generated = generate_member_key_bundles(local_member.clone())
        .boxed()
        .with_context(|_| provision_local_identity_error::GenerateSnafu {
            member_id: local_member.clone(),
        })?;
    let private_bundle = generated.local_private_bundle.as_bytes();
    let local_keys = decode_local_member_keys(&local_member, private_bundle)?;
    let private_record = local_private_keys_record(&local_member, security, private_bundle)?;
    let public_keys = local_keys.public_keys();
    let public_bundle = public_keys.public_key_bundle();

    let mut transaction = store
        .begin_transaction()
        .await
        .context(provision_local_identity_error::StoreAccessSnafu)?;
    let existing_member = transaction
        .load_local_member_identity()
        .await
        .context(provision_local_identity_error::StoreAccessSnafu)?;
    if let Some(member_id) = existing_member {
        provision_local_identity_error::AlreadyProvisionedSnafu { member_id }.fail()?;
    }
    transaction
        .ensure_local_member_private_keys(private_record)
        .await
        .context(provision_local_identity_error::StoreAccessSnafu)?;
    transaction
        .ensure_member_public_keys(MemberPublicKeysRecord::from_public_keys(public_keys))
        .await
        .context(provision_local_identity_error::StoreAccessSnafu)?;
    transaction
        .commit()
        .await
        .context(provision_local_identity_error::StoreAccessSnafu)?;

    Ok(ProvisionedLocalIdentity {
        member_id: local_member,
        public_bundle,
    })
}

/// Result of atomically provisioning one store's local member identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProvisionedLocalIdentity {
    /// Newly provisioned local member identity.
    member_id: MemberIdentity,
    /// Shareable public bundle derived from the stored local-private material.
    public_bundle: PublicKeyBundle,
}

impl ProvisionedLocalIdentity {
    /// Return the newly provisioned local member identity.
    #[must_use]
    pub fn member_id(&self) -> &MemberIdentity {
        &self.member_id
    }

    /// Return the shareable identity-free public key bundle.
    #[must_use]
    pub fn public_bundle(&self) -> &PublicKeyBundle {
        &self.public_bundle
    }

    /// Consume this result and return its shareable identity-free public key bundle.
    #[must_use]
    pub fn into_public_bundle(self) -> PublicKeyBundle {
        self.public_bundle
    }
}

/// Failures while provisioning local identity material before runtime startup.
#[derive(Debug, Snafu)]
#[snafu(module(provision_local_identity_error))]
pub enum ProvisionLocalIdentityError {
    /// Replication store access failed while provisioning identity material.
    #[snafu(display(
        "Failed to access replication store while provisioning local identity: {source}"
    ))]
    StoreAccess { source: StoreError },
    /// A local identity was already provisioned before this operation began.
    #[snafu(display("The replication store is already provisioned for local member {member_id}."))]
    AlreadyProvisioned { member_id: MemberIdentity },
    /// Fresh member key generation failed.
    #[snafu(display("Failed to generate local identity keys for member {member_id}: {source}"))]
    Generate {
        member_id: MemberIdentity,
        source: BoxError,
    },
    /// A generated local-private key bundle could not be decoded for the selected member.
    #[snafu(display("Local identity keys for member {member_id} are invalid: {source}"))]
    InvalidPrivateBundle {
        member_id: MemberIdentity,
        source: BoxError,
    },
    /// Fresh local-private key material could not be encrypted for storage.
    #[snafu(display("Failed to encrypt local identity keys for member {member_id}: {source}"))]
    Seal {
        member_id: MemberIdentity,
        source: BoxError,
    },
}

/// Decode one private bundle and bind it to the authoritative local member.
fn decode_local_member_keys(
    local_member: &MemberIdentity,
    private_bundle: &[u8],
) -> Result<LocalMemberKeys, ProvisionLocalIdentityError> {
    local_member_keys_from_private_bundle(private_bundle, local_member.clone())
        .boxed()
        .with_context(
            |_| provision_local_identity_error::InvalidPrivateBundleSnafu {
                member_id: local_member.clone(),
            },
        )
}

/// Build the encrypted local-private key record for first-time setup.
fn local_private_keys_record(
    local_member: &MemberIdentity,
    security: &ReplicationSecuritySecrets,
    private_bundle: &[u8],
) -> Result<LocalMemberPrivateKeysRecord, ProvisionLocalIdentityError> {
    let row_id = local_member.to_string();
    let context = StoreSecretContext {
        table: LOGICAL_LOCAL_MEMBER_TABLE,
        column: LOGICAL_LOCAL_PRIVATE_KEYS_COLUMN,
        row_id: row_id.as_bytes(),
        key_id: security.store_secret_key_id().as_bytes(),
        crypto_version: STORE_SECRET_CRYPTO_VERSION_V1,
    };
    let sealed = seal_store_secret(security.store_secret_key(), context, private_bundle)
        .boxed()
        .with_context(|_| provision_local_identity_error::SealSnafu {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        SqliteReplicationStoreProvisioner,
        api::{ReplicationStore, TrustPolicy},
        delivery::security::DeliverySecurity,
        security_store::SecurityStore,
        test_support::{test_replication_security_secrets, wait_for_test_future},
    };
    use std::sync::Arc;
    #[test]
    fn provisioning_atomically_stores_private_and_public_identity_material() {
        let member = MemberIdentity::from_array(["provision", "alice"]);
        let provisioner = wait_for_test_future(SqliteReplicationStoreProvisioner::in_memory())
            .expect("provisioner should build");
        let security = test_replication_security_secrets();

        let identity_setup = wait_for_test_future(provision_local_identity(
            &provisioner,
            member.clone(),
            &security,
        ))
        .expect("identity should provision");
        assert_eq!(identity_setup.member_id(), &member);

        let store = wait_for_test_future(provisioner.into_replication_store())
            .expect("provisioned store should activate");
        let stored_member = wait_for_test_future(store.local_member_identity())
            .expect("ready store should expose its identity");
        assert_eq!(stored_member, member);
        let mut transaction = wait_for_test_future(store.begin_read_transaction())
            .expect("read transaction should start");
        let private_record = wait_for_test_future(
            transaction.load_local_member_private_keys(identity_setup.member_id()),
        )
        .expect("private keys should load")
        .expect("private keys should exist");
        assert_eq!(private_record.member_id, member);
        let public_records =
            wait_for_test_future(transaction.load_member_public_keys_for_member(&member))
                .expect("public keys should load");
        let [public_record] = public_records.as_slice() else {
            panic!("exactly one public-key binding should be stored: {public_records:?}");
        };
        assert_eq!(
            public_record.key_id,
            crate::api::MemberKeyId {
                member_id: member,
                fingerprint: identity_setup.public_bundle().fingerprint(),
            }
        );
        wait_for_test_future(transaction.release()).expect("transaction should release");
    }

    #[test]
    fn provisioning_rejects_an_existing_local_identity() {
        let member = MemberIdentity::from_array(["provision", "existing"]);
        let provisioner = wait_for_test_future(SqliteReplicationStoreProvisioner::in_memory())
            .expect("provisioner should build");
        let security = test_replication_security_secrets();
        wait_for_test_future(provision_local_identity(
            &provisioner,
            member.clone(),
            &security,
        ))
        .expect("first provisioning should succeed");

        let other_member = MemberIdentity::from_array(["provision", "other"]);
        let error = wait_for_test_future(provision_local_identity(
            &provisioner,
            other_member,
            &security,
        ))
        .expect_err("second provisioning should fail");
        assert!(matches!(
            error,
            ProvisionLocalIdentityError::AlreadyProvisioned { member_id }
                if member_id == member
        ));
    }

    #[test]
    fn runtime_security_restores_public_keys_from_authoritative_private_keys() {
        let member = MemberIdentity::from_array(["restore", "alice"]);
        let provisioner = wait_for_test_future(SqliteReplicationStoreProvisioner::in_memory())
            .expect("provisioner should build");
        let security = test_replication_security_secrets();
        let generated =
            generate_member_key_bundles(member.clone()).expect("member keys should generate");
        let private_record = local_private_keys_record(
            &member,
            &security,
            generated.local_private_bundle.as_bytes(),
        )
        .expect("private keys should encrypt");
        let mut transaction = wait_for_test_future(provisioner.begin_transaction())
            .expect("provisioning transaction should start");
        wait_for_test_future(transaction.ensure_local_member_private_keys(private_record))
            .expect("private keys should store");
        wait_for_test_future(transaction.commit()).expect("private keys should commit");
        let store = Arc::new(
            wait_for_test_future(provisioner.into_replication_store())
                .expect("private-key-backed store should activate"),
        );

        let mut before = wait_for_test_future(store.begin_read_transaction())
            .expect("read transaction should start");
        let public_records =
            wait_for_test_future(before.load_member_public_keys_for_member(&member))
                .expect("public keys should load");
        assert_eq!(public_records, []);
        wait_for_test_future(before.release()).expect("read transaction should release");

        let delivery_security = wait_for_test_future(DeliverySecurity::load(
            SecurityStore::new(store.clone(), TrustPolicy::default()),
            &member,
            security.store_secret_key().clone(),
            *security.store_secret_key_id(),
        ))
        .expect("runtime security should load from private keys");

        let mut after = wait_for_test_future(store.begin_read_transaction())
            .expect("read transaction should start");
        let public_records =
            wait_for_test_future(after.load_member_public_keys_for_member(&member))
                .expect("restored public keys should load");
        let [public_record] = public_records.as_slice() else {
            panic!("exactly one public-key binding should be restored: {public_records:?}");
        };
        assert_eq!(
            public_record,
            &MemberPublicKeysRecord::from_public_keys(delivery_security.local_keys().public_keys())
        );
        wait_for_test_future(after.release()).expect("read transaction should release");
    }
}
