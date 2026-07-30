//! Store and local security setup for the replicated checklist runner.

use super::*;

/// Load and validate checklist configuration without creating setup state.
pub(super) fn load_checklist_config(
    config_path: &Path,
) -> Result<ChecklistAppConfig, ReplicatedChecklistError> {
    ChecklistAppConfig::load(config_path).context(repl_error::ConfigSnafu)
}

/// Create setup resources after the user accepts first-run identity initialisation.
pub(super) async fn create_checklist_store_setup(
    config: ChecklistAppConfig,
) -> Result<ChecklistStoreSetup, ReplicatedChecklistError> {
    let replication_security = load_or_create_checklist_replication_security(&config)
        .context(repl_error::LocalStoreSecretSnafu)?;
    ensure_store_parent_exists(&config.store_path)?;
    build_checklist_store_setup(config, replication_security).await
}

/// Open setup resources for an existing store without creating a replacement store secret.
pub(super) async fn open_checklist_store_setup(
    config: ChecklistAppConfig,
) -> Result<ChecklistStoreSetup, ReplicatedChecklistError> {
    let replication_security = load_existing_checklist_replication_security(&config)
        .context(repl_error::LocalStoreSecretSnafu)?;
    build_checklist_store_setup(config, replication_security).await
}

/// Store and security resources used while loading one configured runtime.
pub(super) struct ChecklistStoreSetup {
    pub(super) config: ChecklistAppConfig,
    pub(super) replication_security: ReplicationSecuritySecrets,
    pub(super) store: Arc<SqliteReplicationStore>,
}

/// Load the existing local store secret required by an existing checklist store.
fn load_existing_checklist_replication_security(
    config: &ChecklistAppConfig,
) -> Result<ReplicationSecuritySecrets, LoadSecurityError> {
    if config
        .store_secret_profile
        .as_str()
        .starts_with(UNSAFE_STORE_SECRET_PROFILE_PREFIX)
    {
        Ok(derive_unsafe_checklist_replication_security(config))
    } else {
        ReplicationSecuritySecrets::load_local(
            &checklist_application_id(),
            &config.store_secret_profile,
        )
    }
}

/// Load or create the local store secret for an accepted first-run setup.
fn load_or_create_checklist_replication_security(
    config: &ChecklistAppConfig,
) -> Result<ReplicationSecuritySecrets, LoadSecurityError> {
    if config
        .store_secret_profile
        .as_str()
        .starts_with(UNSAFE_STORE_SECRET_PROFILE_PREFIX)
    {
        Ok(derive_unsafe_checklist_replication_security(config))
    } else {
        ReplicationSecuritySecrets::load_or_create_local(
            &checklist_application_id(),
            &config.store_secret_profile,
        )
    }
}

/// Open the checklist SQLite store after its secret-loading policy has been resolved.
async fn build_checklist_store_setup(
    config: ChecklistAppConfig,
    replication_security: ReplicationSecuritySecrets,
) -> Result<ChecklistStoreSetup, ReplicatedChecklistError> {
    let store = SqliteReplicationStore::file_with_schema_sources(
        config.local_member.clone(),
        &config.store_path,
        [(checklist_dataset_id(), &*CHECKLIST_SCHEMA)],
    )
    .await
    .context(repl_error::StoreSnafu)?;
    Ok(ChecklistStoreSetup {
        config,
        replication_security,
        store: Arc::new(store),
    })
}

/// Derive temporary headless example secrets from plaintext config.
fn derive_unsafe_checklist_replication_security(
    config: &ChecklistAppConfig,
) -> ReplicationSecuritySecrets {
    let store_secret_key_id = derive_unsafe_checklist_store_secret_key_id(config);
    let store_secret_key =
        StoreSecretKey::from_bytes(derive_unsafe_checklist_store_secret_key(config));
    ReplicationSecuritySecrets::from_unmanaged_store_secret(store_secret_key_id, store_secret_key)
}

/// Build the synthetic key id attached to example-local encrypted cells.
fn derive_unsafe_checklist_store_secret_key_id(config: &ChecklistAppConfig) -> StoreSecretKeyId {
    let digest =
        derive_unsafe_checklist_store_secret_digest(UNSAFE_STORE_SECRET_KEY_ID_DOMAIN, config);
    let mut key_id = [0_u8; StoreSecretKeyId::BYTE_LENGTH];
    key_id.copy_from_slice(&digest[..StoreSecretKeyId::BYTE_LENGTH]);
    StoreSecretKeyId::from_bytes(key_id)
}

/// Build the synthetic store-secret key used by the unsafe example path.
fn derive_unsafe_checklist_store_secret_key(
    config: &ChecklistAppConfig,
) -> [u8; STORE_SECRET_KEY_LENGTH] {
    derive_unsafe_checklist_store_secret_digest(UNSAFE_STORE_SECRET_KEY_DOMAIN, config)
}

/// Bind unsafe derivation to this example application and full profile string.
fn derive_unsafe_checklist_store_secret_digest(
    domain: &[u8],
    config: &ChecklistAppConfig,
) -> [u8; 32] {
    let application_id = checklist_application_id().to_string();
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update([0]);
    hasher.update(application_id.as_bytes());
    hasher.update([0]);
    hasher.update(config.store_secret_profile.as_str().as_bytes());
    hasher.finalize().into()
}

fn ensure_store_parent_exists(path: &Path) -> Result<(), ReplicatedChecklistError> {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => {
            fs::create_dir_all(parent).context(repl_error::CreateStoreDirectorySnafu {
                path: parent.to_path_buf(),
            })
        }
        // A bare filename has `Some("")` as parent, meaning the store lives in the current directory.
        Some(_) | None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(profile: &str) -> ChecklistAppConfig {
        ChecklistAppConfig {
            source_path: PathBuf::from("test.toml"),
            runtime_config_toml: String::new(),
            local_member: MemberIdentity::from_array(["alice"]),
            store_path: PathBuf::from("unused.sqlite"),
            store_secret_profile: flotsync_replication::LocalStoreSecretProfile::new(profile)
                .expect("test profile should build"),
        }
    }

    #[test]
    fn unsafe_store_secret_profile_derives_stable_security_without_keyring() {
        let config = test_config("unsafe:test-profile");

        let first = load_existing_checklist_replication_security(&config)
            .expect("unsafe security should derive");
        let second = load_or_create_checklist_replication_security(&config)
            .expect("unsafe security should derive");

        assert_eq!(first.store_secret_key_id(), second.store_secret_key_id());
    }

    #[test]
    fn unsafe_store_secret_profile_changes_derived_key_id() {
        let first_config = test_config("unsafe:first-profile");
        let second_config = test_config("unsafe:second-profile");

        let first = load_existing_checklist_replication_security(&first_config)
            .expect("first unsafe security should derive");
        let second = load_existing_checklist_replication_security(&second_config)
            .expect("second unsafe security should derive");

        assert_ne!(first.store_secret_key_id(), second.store_secret_key_id());
    }
}
