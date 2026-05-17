#[cfg(test)]
use crate::delivery::security::DeliverySecurity;
use crate::{
    api::{
        EncryptedLocalMemberPrivateKeys,
        EncryptedStoreSecret,
        LoadError,
        LocalMemberPrivateKeysRecord,
        MemberIdentity,
        ReplicationApi,
        ReplicationConfig,
        ReplicationEventListener,
        ReplicationSecuritySecrets,
        ReplicationStore,
        RuntimeSnafu,
        StoreSecretKeyId,
        TrustedMemberPublicKeysRecord,
    },
    runtime::handle::load_replication_runtime_with_runtime_config_toml,
};
use flotsync_core::member::Identifier;
use flotsync_security::{
    PublicMemberKeys,
    STORE_SECRET_CRYPTO_VERSION_V1,
    STORE_SECRET_NONCE_LENGTH,
    StoreSecretContext,
    StoreSecretKey,
    public_member_keys_from_jwks,
    seal_store_secret_for_test,
    test_support::{TEST_MEMBER_KEY_SEED_LENGTH, member_key_files_from_seed},
};
use snafu::prelude::*;
use std::sync::Arc;

const TEST_STORE_SECRET_KEY_ID: &str = "replication-test-store-secret";
const TEST_STORE_SECRET_KEY_BYTES: [u8; 32] = [149; 32];

/// Load a replication runtime for tests that have not yet gained real security setup.
///
/// This helper provisions deterministic local-private member keys into the
/// supplied store, loads runtime security from those records, and then starts
/// the same concrete runtime host used by normal runtime tests. It is temporary
/// test scaffolding for slices before replicated-checklist security setup
/// lands.
///
/// # Errors
///
/// Returns [`LoadError`] when store access, deterministic key generation,
/// local-private-key sealing, or runtime startup fails.
pub async fn load_replication_runtime_with_test_security_toml(
    application_id: Identifier,
    store: Arc<dyn ReplicationStore>,
    listener: Arc<dyn ReplicationEventListener>,
    config: ReplicationConfig,
    runtime_config_toml: &str,
) -> Result<Arc<dyn ReplicationApi>, LoadError> {
    let local_member = store
        .local_member_identity()
        .await
        .boxed()
        .context(RuntimeSnafu {
            application_id: application_id.clone(),
        })?;
    provision_test_security(
        application_id.clone(),
        store.as_ref(),
        &local_member,
        std::iter::empty::<MemberIdentity>(),
    )
    .await?;
    let runtime = load_replication_runtime_with_runtime_config_toml(
        application_id,
        store,
        listener,
        config,
        test_replication_security_secrets(),
        runtime_config_toml,
    )
    .await?;
    Ok(runtime)
}

/// Provision deterministic local-private keys and trusted peer keys into one store.
///
/// # Errors
///
/// Returns [`LoadError`] when deterministic test key generation, store-secret
/// sealing, or any store transaction operation fails.
pub async fn provision_test_security(
    application_id: Identifier,
    store: &dyn ReplicationStore,
    local_member: &Identifier,
    trusted_members: impl IntoIterator<Item = MemberIdentity>,
) -> Result<(), LoadError> {
    let local_seed = test_member_seed(local_member);
    let generated = member_key_files_from_seed(local_member.clone(), &local_seed)
        .boxed()
        .context(RuntimeSnafu {
            application_id: application_id.clone(),
        })?;
    let key_id = StoreSecretKeyId::new(TEST_STORE_SECRET_KEY_ID);
    let row_id = local_member.to_string();
    let context = StoreSecretContext {
        table: "local_member",
        column: "private_keys",
        row_id: row_id.as_bytes(),
        key_id: key_id.as_str(),
        crypto_version: STORE_SECRET_CRYPTO_VERSION_V1,
    };
    let sealed = seal_store_secret_for_test(
        &test_store_secret_key(),
        context,
        generated.local_private_jwks.as_str().as_bytes(),
        test_store_secret_nonce(local_member),
    )
    .boxed()
    .context(RuntimeSnafu {
        application_id: application_id.clone(),
    })?;
    let record = LocalMemberPrivateKeysRecord {
        member_id: local_member.clone(),
        private_keys: EncryptedLocalMemberPrivateKeys {
            secret: EncryptedStoreSecret::from_store_secret_ciphertext(key_id, sealed),
        },
    };
    let mut transaction = store
        .begin_transaction()
        .await
        .boxed()
        .context(RuntimeSnafu {
            application_id: application_id.clone(),
        })?;
    transaction
        .ensure_local_member_private_keys(record)
        .await
        .boxed()
        .context(RuntimeSnafu {
            application_id: application_id.clone(),
        })?;
    for trusted_member in trusted_members {
        let trusted_keys = test_public_member_keys(&trusted_member);
        let record = TrustedMemberPublicKeysRecord {
            member_id: trusted_member,
            signing_public_key: trusted_keys.signing_key_bytes().into(),
            encryption_public_key: trusted_keys.encryption_key_bytes().into(),
        };
        transaction
            .ensure_trusted_member_public_keys(record)
            .await
            .boxed()
            .context(RuntimeSnafu {
                application_id: application_id.clone(),
            })?;
    }
    transaction
        .commit()
        .await
        .boxed()
        .context(RuntimeSnafu { application_id })
}

/// Load delivery security from deterministic test records already provisioned in a store.
#[cfg(test)]
pub(crate) async fn load_test_delivery_security(
    application_id: Identifier,
    store: Arc<dyn ReplicationStore>,
    local_member: &MemberIdentity,
) -> Result<DeliverySecurity, LoadError> {
    DeliverySecurity::load(
        store,
        local_member,
        Arc::new(test_store_secret_key()),
        StoreSecretKeyId::new(TEST_STORE_SECRET_KEY_ID),
    )
    .await
    .boxed()
    .context(RuntimeSnafu { application_id })
}

/// Build the deterministic runtime security input used by test runtime support.
#[must_use]
pub fn test_replication_security_secrets() -> ReplicationSecuritySecrets {
    ReplicationSecuritySecrets::new(
        StoreSecretKeyId::new(TEST_STORE_SECRET_KEY_ID),
        Arc::new(test_store_secret_key()),
    )
}

/// Build deterministic seed material for one test member identity.
fn test_member_seed(member: &Identifier) -> [u8; TEST_MEMBER_KEY_SEED_LENGTH] {
    let mut seed = [0u8; TEST_MEMBER_KEY_SEED_LENGTH];
    for (index, byte) in member.to_string().bytes().enumerate() {
        let slot = index % seed.len();
        seed[slot] = seed[slot]
            .wrapping_add(byte)
            .wrapping_add(u8::try_from(index % 251).expect("modulo bounds the test seed offset"));
    }
    seed
}

/// Build deterministic public keys for one test member identity.
#[must_use]
pub(crate) fn test_public_member_keys(member: &MemberIdentity) -> PublicMemberKeys {
    let seed = test_member_seed(member);
    let generated = member_key_files_from_seed(member.clone(), &seed)
        .expect("test member keys should generate");
    public_member_keys_from_jwks(&generated.public_jwks, Some(member))
        .expect("test public keys should parse")
}

/// Build the deterministic store-secret key used by test runtime support.
fn test_store_secret_key() -> StoreSecretKey {
    StoreSecretKey::from_bytes(TEST_STORE_SECRET_KEY_BYTES)
}

/// Build the deterministic nonce used for one member's encrypted local keys.
fn test_store_secret_nonce(member: &Identifier) -> [u8; STORE_SECRET_NONCE_LENGTH] {
    [test_member_seed(member)[0]; STORE_SECRET_NONCE_LENGTH]
}
