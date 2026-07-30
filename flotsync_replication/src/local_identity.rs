//! Pre-runtime local identity initialisation.

use crate::{
    api::{
        BoxError,
        EncryptedLocalMemberPrivateKeys,
        EncryptedStoreSecret,
        LocalMemberPrivateKeysRecord,
        MemberPublicKeysRecord,
        ReplicationSecuritySecrets,
        ReplicationStore,
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
    open_store_secret,
    seal_store_secret,
};
use snafu::prelude::*;

/// Initialise or repair the local identity material required before runtime loading.
///
/// The store supplies the authoritative local member identity. This operation generates private
/// identity keys only when the store has no local-private record, and always ensures the matching
/// public-key binding in the same transaction.
///
/// # Errors
///
/// Returns [`InitialiseLocalIdentityError`] when store access, key generation, private-key
/// validation, or encrypted local storage fails.
pub async fn initialise_local_identity(
    store: &dyn ReplicationStore,
    security: &ReplicationSecuritySecrets,
) -> Result<LocalIdentityInitialisation, InitialiseLocalIdentityError> {
    let local_member = store
        .local_member_identity()
        .await
        .context(initialise_local_identity_error::StoreAccessSnafu)?;
    let mut transaction = store
        .begin_transaction()
        .await
        .context(initialise_local_identity_error::StoreAccessSnafu)?;
    let existing = transaction
        .load_local_member_private_keys(&local_member)
        .await
        .context(initialise_local_identity_error::StoreAccessSnafu)?;

    let (local_keys, created) = if let Some(existing) = existing {
        let plaintext = open_local_private_key_bundle(&local_member, security, &existing)?;
        let local_keys = decode_local_member_keys(&local_member, plaintext.as_ref())?;
        (local_keys, false)
    } else {
        let generated = generate_member_key_bundles(local_member.clone())
            .boxed()
            .with_context(|_| initialise_local_identity_error::GenerateSnafu {
                member_id: local_member.clone(),
            })?;
        let private_bundle = generated.local_private_bundle.as_bytes();
        let local_keys = decode_local_member_keys(&local_member, private_bundle)?;
        let record = local_private_keys_record(&local_member, security, private_bundle)?;
        transaction
            .ensure_local_member_private_keys(record)
            .await
            .context(initialise_local_identity_error::StoreAccessSnafu)?;
        (local_keys, true)
    };

    let public_keys = local_keys.public_keys();
    let public_bundle = public_keys.public_key_bundle();
    transaction
        .ensure_member_public_keys(MemberPublicKeysRecord::from_public_keys(public_keys))
        .await
        .context(initialise_local_identity_error::StoreAccessSnafu)?;
    transaction
        .commit()
        .await
        .context(initialise_local_identity_error::StoreAccessSnafu)?;

    Ok(LocalIdentityInitialisation {
        member_id: local_member,
        public_bundle,
        created,
    })
}

/// Result of idempotently initialising one store's local member identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalIdentityInitialisation {
    /// Authoritative local member identity supplied by the store.
    member_id: MemberIdentity,
    /// Shareable public bundle derived from the stored local-private material.
    public_bundle: PublicKeyBundle,
    /// Whether this operation generated and stored fresh private key material.
    created: bool,
}

impl LocalIdentityInitialisation {
    /// Return the authoritative local member identity.
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

    /// Return whether this operation generated and stored fresh private key material.
    ///
    /// `false` means compatible private material already existed and was reused. The operation may
    /// still have repaired a missing public-key binding for that material.
    #[must_use]
    pub fn was_created(&self) -> bool {
        self.created
    }
}

/// Failures while initialising local identity material before runtime startup.
#[derive(Debug, Snafu)]
#[snafu(module(initialise_local_identity_error))]
pub enum InitialiseLocalIdentityError {
    /// Replication store access failed while loading or establishing identity material.
    #[snafu(display(
        "Failed to access replication store while initialising local identity: {source}"
    ))]
    StoreAccess { source: StoreError },
    /// Fresh member key generation failed.
    #[snafu(display("Failed to generate local identity keys for member {member_id}: {source}"))]
    Generate {
        member_id: MemberIdentity,
        source: BoxError,
    },
    /// A local-private key bundle could not be decoded for the store's local member.
    #[snafu(display("Local identity keys for member {member_id} are invalid: {source}"))]
    InvalidPrivateBundle {
        member_id: MemberIdentity,
        source: BoxError,
    },
    /// The stored local-private record uses a different store-secret key id.
    #[snafu(display(
        "Local identity keys for member {member_id} use store-secret key id {actual}; expected {expected}."
    ))]
    StoreSecretKeyIdMismatch {
        member_id: MemberIdentity,
        expected: crate::api::StoreSecretKeyId,
        actual: crate::api::StoreSecretKeyId,
    },
    /// Fresh local-private key material could not be encrypted for storage.
    #[snafu(display("Failed to encrypt local identity keys for member {member_id}: {source}"))]
    Seal {
        member_id: MemberIdentity,
        source: BoxError,
    },
    /// Existing local-private key material could not be decrypted from storage.
    #[snafu(display("Failed to open local identity keys for member {member_id}: {source}"))]
    Open {
        member_id: MemberIdentity,
        source: BoxError,
    },
}

/// Decode one private bundle and bind it to the authoritative local member.
fn decode_local_member_keys(
    local_member: &MemberIdentity,
    private_bundle: &[u8],
) -> Result<LocalMemberKeys, InitialiseLocalIdentityError> {
    local_member_keys_from_private_bundle(private_bundle, local_member.clone())
        .boxed()
        .with_context(
            |_| initialise_local_identity_error::InvalidPrivateBundleSnafu {
                member_id: local_member.clone(),
            },
        )
}

/// Open one encrypted local-private key bundle from a store record.
fn open_local_private_key_bundle(
    local_member: &MemberIdentity,
    security: &ReplicationSecuritySecrets,
    existing: &LocalMemberPrivateKeysRecord,
) -> Result<impl AsRef<[u8]>, InitialiseLocalIdentityError> {
    let secret = &existing.private_keys.secret;
    ensure!(
        &secret.key_id == security.store_secret_key_id(),
        initialise_local_identity_error::StoreSecretKeyIdMismatchSnafu {
            member_id: local_member.clone(),
            expected: *security.store_secret_key_id(),
            actual: secret.key_id,
        }
    );
    let sealed = secret
        .to_store_secret_ciphertext()
        .boxed()
        .with_context(|_| initialise_local_identity_error::OpenSnafu {
            member_id: local_member.clone(),
        })?;
    let row_id = local_member.to_string();
    let context = StoreSecretContext {
        table: LOGICAL_LOCAL_MEMBER_TABLE,
        column: LOGICAL_LOCAL_PRIVATE_KEYS_COLUMN,
        row_id: row_id.as_bytes(),
        key_id: security.store_secret_key_id().as_bytes(),
        crypto_version: STORE_SECRET_CRYPTO_VERSION_V1,
    };
    open_store_secret(security.store_secret_key(), context, &sealed)
        .boxed()
        .with_context(|_| initialise_local_identity_error::OpenSnafu {
            member_id: local_member.clone(),
        })
}

/// Build the encrypted local-private key record for first-time setup.
fn local_private_keys_record(
    local_member: &MemberIdentity,
    security: &ReplicationSecuritySecrets,
    private_bundle: &[u8],
) -> Result<LocalMemberPrivateKeysRecord, InitialiseLocalIdentityError> {
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
        .with_context(|_| initialise_local_identity_error::SealSnafu {
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
    use std::sync::Arc;

    use super::*;
    use crate::{
        SqliteReplicationStore,
        api::StoreSecretKeyId,
        test_support::{test_replication_security_secrets, wait_for_test_future},
    };
    use flotsync_core::member::Identifier;
    use flotsync_security::StoreSecretKey;

    #[test]
    fn initialisation_generates_once_and_stores_the_public_binding() {
        let member = Identifier::from_array(["initialise", "alice"]);
        let store = wait_for_test_future(SqliteReplicationStore::in_memory(member.clone()))
            .expect("store should build");
        let security = test_replication_security_secrets();

        let first = wait_for_test_future(initialise_local_identity(&store, &security))
            .expect("first initialisation should succeed");
        assert_eq!(first.member_id(), &member);
        assert!(first.was_created());

        let second = wait_for_test_future(initialise_local_identity(&store, &security))
            .expect("second initialisation should succeed");
        assert!(!second.was_created());
        assert_eq!(second.public_bundle(), first.public_bundle());

        let mut transaction = wait_for_test_future(store.begin_read_transaction())
            .expect("read transaction should start");
        let private_record =
            wait_for_test_future(transaction.load_local_member_private_keys(&member))
                .expect("private keys should load");
        assert!(private_record.is_some());
        let public_records =
            wait_for_test_future(transaction.load_member_public_keys_for_member(&member))
                .expect("public keys should load");
        assert_eq!(public_records.len(), 1);
        assert_eq!(
            public_records[0].key_id.fingerprint,
            first.public_bundle().fingerprint()
        );
        wait_for_test_future(transaction.release()).expect("transaction should release");
    }

    #[test]
    fn initialisation_repairs_a_missing_public_binding() {
        let member = Identifier::from_array(["initialise", "repair"]);
        let store = wait_for_test_future(SqliteReplicationStore::in_memory(member.clone()))
            .expect("store should build");
        let security = test_replication_security_secrets();
        let generated = generate_member_key_bundles(member.clone())
            .expect("test identity keys should generate");
        let private_bundle = generated.local_private_bundle.as_bytes();
        let expected_local_keys = decode_local_member_keys(&member, private_bundle)
            .expect("generated private keys should decode");
        let expected_public_bundle = expected_local_keys.public_keys().public_key_bundle();
        let private_record = local_private_keys_record(&member, &security, private_bundle)
            .expect("private keys should seal");
        let mut transaction =
            wait_for_test_future(store.begin_transaction()).expect("transaction should start");
        wait_for_test_future(transaction.ensure_local_member_private_keys(private_record))
            .expect("private keys should store");
        wait_for_test_future(transaction.commit()).expect("transaction should commit");

        let initialised = wait_for_test_future(initialise_local_identity(&store, &security))
            .expect("existing private keys should initialise");
        assert!(!initialised.was_created());
        assert_eq!(initialised.public_bundle(), &expected_public_bundle);

        let mut transaction = wait_for_test_future(store.begin_read_transaction())
            .expect("read transaction should start");
        let public_records =
            wait_for_test_future(transaction.load_member_public_keys_for_member(&member))
                .expect("public keys should load");
        assert_eq!(public_records.len(), 1);
        assert_eq!(
            public_records[0].key_id.fingerprint,
            expected_public_bundle.fingerprint()
        );
        wait_for_test_future(transaction.release()).expect("transaction should release");
    }

    #[test]
    fn initialisation_rejects_existing_keys_opened_with_the_wrong_secret() {
        let member = Identifier::from_array(["initialise", "wrong-secret"]);
        let store = wait_for_test_future(SqliteReplicationStore::in_memory(member.clone()))
            .expect("store should build");
        let security = test_replication_security_secrets();
        wait_for_test_future(initialise_local_identity(&store, &security))
            .expect("initialisation should succeed");
        let wrong_security = ReplicationSecuritySecrets::new(
            *security.store_secret_key_id(),
            Arc::new(StoreSecretKey::from_bytes([41; 32])),
        );

        let error = wait_for_test_future(initialise_local_identity(&store, &wrong_security))
            .expect_err("wrong store secret should fail");
        assert!(matches!(
            error,
            InitialiseLocalIdentityError::Open { member_id, .. } if member_id == member
        ));
    }

    #[test]
    fn initialisation_rejects_existing_keys_with_a_different_store_secret_key_id() {
        let member = Identifier::from_array(["initialise", "wrong-key-id"]);
        let store = wait_for_test_future(SqliteReplicationStore::in_memory(member.clone()))
            .expect("store should build");
        let security = test_replication_security_secrets();
        wait_for_test_future(initialise_local_identity(&store, &security))
            .expect("initialisation should succeed");

        let mismatched_security = ReplicationSecuritySecrets::new(
            StoreSecretKeyId::from_bytes(*b"different-key-id"),
            Arc::new(StoreSecretKey::from_bytes([41; 32])),
        );
        let error = wait_for_test_future(initialise_local_identity(&store, &mismatched_security))
            .expect_err("different key id should fail");
        assert!(matches!(
            error,
            InitialiseLocalIdentityError::StoreSecretKeyIdMismatch {
                member_id,
                expected,
                actual,
            } if member_id == member
                && expected == *mismatched_security.store_secret_key_id()
                && actual == *security.store_secret_key_id()
        ));
    }

    #[test]
    fn initialisation_rejects_correctly_sealed_malformed_private_keys() {
        let member = Identifier::from_array(["initialise", "malformed-private-keys"]);
        let store = wait_for_test_future(SqliteReplicationStore::in_memory(member.clone()))
            .expect("store should build");
        let security = test_replication_security_secrets();
        let private_record =
            local_private_keys_record(&member, &security, b"not a local private key bundle")
                .expect("malformed private keys should still seal");
        let mut transaction =
            wait_for_test_future(store.begin_transaction()).expect("transaction should start");
        wait_for_test_future(transaction.ensure_local_member_private_keys(private_record))
            .expect("malformed private keys should store");
        wait_for_test_future(transaction.commit()).expect("transaction should commit");

        let error = wait_for_test_future(initialise_local_identity(&store, &security))
            .expect_err("malformed private keys should fail validation");
        assert!(matches!(
            error,
            InitialiseLocalIdentityError::InvalidPrivateBundle { member_id, .. }
                if member_id == member
        ));
    }
}
