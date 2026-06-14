use flotsync_core::{GroupId, MemberIdentity, member::Identifier};
use flotsync_replication::LocalStoreSecretProfile;
use flotsync_security::GroupKey;
use kompact::config::{Config, parse_config_str};
use sha2::{Digest, Sha256};
use snafu::prelude::*;
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};
use uuid::Uuid;

const LOCAL_MEMBER_KEY: &str = "flotsync.examples.replicated-checklist.local-member";
const STORE_PATH_KEY: &str = "flotsync.examples.replicated-checklist.store-path";
const STORE_SECRET_PROFILE_KEY: &str =
    "flotsync.examples.replicated-checklist.store-secret-profile";
const GROUP_SECRET_PASSWORD_KEY: &str =
    "flotsync.examples.replicated-checklist.group-secret-password";
const LOCAL_PRIVATE_JWKS_PATH_KEY: &str =
    "flotsync.examples.replicated-checklist.local-private-jwks-path";
const TRUSTED_PUBLIC_JWKS_PATHS_KEY: &str =
    "flotsync.examples.replicated-checklist.trusted-public-jwks-paths";
const GROUP_ID_KEY: &str = "flotsync.examples.replicated-checklist.group-id";
const ORDERED_MEMBERS_KEY: &str = "flotsync.examples.replicated-checklist.ordered-members";
const LOCAL_ENDPOINT_KEY: &str = "flotsync.replication.runtime.local-endpoint-bind-addr";

const GROUP_SECRET_PASSWORD_DOMAIN: &[u8] =
    b"flotsync/examples/replicated-checklist/group-secret-password/v1";

#[derive(Clone, Debug)]
pub struct ChecklistAppConfig {
    pub source_path: PathBuf,
    /// Raw source TOML forwarded to the runtime config loader.
    ///
    /// Contract until the remaining static-group setup work lands: this string
    /// may still contain the temporary example group-secret password config, so
    /// treat this whole config value as secret-bearing and do not log it in
    /// production paths.
    pub runtime_config_toml: String,
    pub local_member: MemberIdentity,
    pub store_path: PathBuf,
    /// Device-local profile used to load or create the replication store-secret key.
    pub store_secret_profile: LocalStoreSecretProfile,
    /// Shared static-group secret derived from temporary plaintext config.
    pub group_key: Arc<GroupKey>,
    /// Local-private member JWKS used by temporary setup provisioning.
    pub local_private_jwks_path: PathBuf,
    /// Trusted public member JWKS files used by temporary setup provisioning.
    pub trusted_public_jwks_paths: Vec<PathBuf>,
    pub group_id: GroupId,
    pub ordered_members: Vec<MemberIdentity>,
    pub local_endpoint_bind_addr: SocketAddr,
}

impl ChecklistAppConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ChecklistConfigError> {
        let source_path = path.as_ref().to_path_buf();
        let runtime_config_toml =
            std::fs::read_to_string(&source_path).with_context(|_| ReadFileSnafu {
                path: source_path.clone(),
            })?;
        let config = parse_config_str(&runtime_config_toml).map_err(|source| {
            ChecklistConfigError::ParseFile {
                path: source_path.clone(),
                message: source.to_string(),
            }
        })?;

        let local_member = read_member(&config, LOCAL_MEMBER_KEY)?;
        let store_path = read_store_path(&config, &source_path)?;
        let local_private_jwks_path =
            read_path(&config, LOCAL_PRIVATE_JWKS_PATH_KEY, &source_path)?;
        let trusted_public_jwks_paths =
            read_path_list(&config, TRUSTED_PUBLIC_JWKS_PATHS_KEY, &source_path)?;
        let group_id = read_group_id(&config)?;
        let ordered_members = read_ordered_members(&config)?;
        let store_secret_profile = read_store_secret_profile(&config)?;
        let group_key = read_group_key(&config, group_id, &ordered_members)?;
        let local_endpoint_bind_addr = read_socket_addr(&config, LOCAL_ENDPOINT_KEY)?;

        ensure!(
            ordered_members.iter().any(|member| member == &local_member),
            InvalidConfigSnafu {
                key: ORDERED_MEMBERS_KEY,
                message: format!(
                    "ordered members must include configured local member {local_member}"
                ),
            }
        );

        Ok(Self {
            source_path,
            runtime_config_toml,
            local_member,
            store_path,
            store_secret_profile,
            group_key: Arc::new(group_key),
            local_private_jwks_path,
            trusted_public_jwks_paths,
            group_id,
            ordered_members,
            local_endpoint_bind_addr,
        })
    }
}

#[derive(Debug, Snafu)]
pub enum ChecklistConfigError {
    #[snafu(display("Failed to read checklist config {}: {source}", path.display()))]
    ReadFile {
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("Failed to parse checklist config {}: {message}", path.display()))]
    ParseFile { path: PathBuf, message: String },
    #[snafu(display("Checklist config key {key} is invalid: {message}"))]
    InvalidConfig { key: &'static str, message: String },
}

/// Application id used to scope replicated-checklist local store-secret profiles.
pub fn checklist_application_id() -> Identifier {
    Identifier::from_array(["flotsync", "examples", "replicated-checklist"])
}

fn read_member(config: &Config, key: &'static str) -> Result<MemberIdentity, ChecklistConfigError> {
    let value = read_string(config, key)?;
    MemberIdentity::from_str(&value).map_err(|source| ChecklistConfigError::InvalidConfig {
        key,
        message: source.to_string(),
    })
}

fn read_store_path(config: &Config, source_path: &Path) -> Result<PathBuf, ChecklistConfigError> {
    read_path(config, STORE_PATH_KEY, source_path)
}

fn read_path(
    config: &Config,
    key: &'static str,
    source_path: &Path,
) -> Result<PathBuf, ChecklistConfigError> {
    let value = read_string(config, key)?;
    Ok(resolve_config_path(PathBuf::from(value), source_path))
}

fn read_path_list(
    config: &Config,
    key: &'static str,
    source_path: &Path,
) -> Result<Vec<PathBuf>, ChecklistConfigError> {
    let lookup = config.select(key);
    let entries = lookup
        .array_entries()
        .map_err(|source| ChecklistConfigError::InvalidConfig {
            key,
            message: source.to_string(),
        })?;
    let mut paths = Vec::new();
    for (index, entry) in entries {
        let value = entry
            .as_string()
            .map_err(|source| ChecklistConfigError::InvalidConfig {
                key,
                message: format!("path[{index}]: {source}"),
            })?;
        paths.push(resolve_config_path(PathBuf::from(value), source_path));
    }
    Ok(paths)
}

fn resolve_config_path(path: PathBuf, source_path: &Path) -> PathBuf {
    if path.is_absolute() {
        return path;
    }
    let Some(base_dir) = source_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    else {
        return path;
    };
    base_dir.join(path)
}

fn read_group_id(config: &Config) -> Result<GroupId, ChecklistConfigError> {
    let value = config.select(GROUP_ID_KEY).as_i64().map_err(|source| {
        ChecklistConfigError::InvalidConfig {
            key: GROUP_ID_KEY,
            message: source.to_string(),
        }
    })?;
    ensure!(
        value >= 0,
        InvalidConfigSnafu {
            key: GROUP_ID_KEY,
            message: format!("must be a non-negative integer, got {value}"),
        }
    );
    let value = u128::try_from(value).map_err(|_| ChecklistConfigError::InvalidConfig {
        key: GROUP_ID_KEY,
        message: format!("must be a non-negative integer, got {value}"),
    })?;
    Ok(GroupId(Uuid::from_u128(value)))
}

/// Read the local profile used to look up this device's store-secret key.
fn read_store_secret_profile(
    config: &Config,
) -> Result<LocalStoreSecretProfile, ChecklistConfigError> {
    let value = read_string(config, STORE_SECRET_PROFILE_KEY)?;
    LocalStoreSecretProfile::new(value).map_err(|source| ChecklistConfigError::InvalidConfig {
        key: STORE_SECRET_PROFILE_KEY,
        message: source.to_string(),
    })
}

/// Build the temporary static-group key from shared checklist config.
///
/// This plaintext password is an MVP-only setup bridge. The derivation binds
/// the password to the configured group id and ordered members so accidental
/// reuse across distinct static groups does not produce the same key.
fn read_group_key(
    config: &Config,
    group_id: GroupId,
    ordered_members: &[MemberIdentity],
) -> Result<GroupKey, ChecklistConfigError> {
    let password = read_string(config, GROUP_SECRET_PASSWORD_KEY)?;
    ensure!(
        !password.is_empty(),
        InvalidConfigSnafu {
            key: GROUP_SECRET_PASSWORD_KEY,
            message: "must not be empty".to_owned(),
        }
    );
    Ok(derive_group_key_from_password(
        password.as_bytes(),
        group_id,
        ordered_members,
    ))
}

/// Hash shared static-group config into the current group-key width.
fn derive_group_key_from_password(
    password: &[u8],
    group_id: GroupId,
    ordered_members: &[MemberIdentity],
) -> GroupKey {
    let mut hasher = Sha256::new();
    hasher.update(GROUP_SECRET_PASSWORD_DOMAIN);
    hash_len_prefixed(&mut hasher, password);
    hasher.update(group_id.0.as_bytes());
    for member in ordered_members {
        hash_len_prefixed(&mut hasher, member.to_string().as_bytes());
    }
    GroupKey::from_bytes(hasher.finalize().into())
}

/// Add one unambiguous variable-width field to a password derivation transcript.
fn hash_len_prefixed(hasher: &mut Sha256, bytes: &[u8]) {
    let len = u64::try_from(bytes.len()).expect("usize length must fit into u64");
    hasher.update(len.to_be_bytes());
    hasher.update(bytes);
}

fn read_ordered_members(config: &Config) -> Result<Vec<MemberIdentity>, ChecklistConfigError> {
    let members_lookup = config.select(ORDERED_MEMBERS_KEY);
    let member_entries =
        members_lookup
            .array_entries()
            .map_err(|source| ChecklistConfigError::InvalidConfig {
                key: ORDERED_MEMBERS_KEY,
                message: source.to_string(),
            })?;

    let mut members = Vec::new();
    for (index, member_lookup) in member_entries {
        match member_lookup.as_string() {
            Ok(value) => {
                let member = MemberIdentity::from_str(&value).map_err(|source| {
                    ChecklistConfigError::InvalidConfig {
                        key: ORDERED_MEMBERS_KEY,
                        message: format!("member[{index}] {value:?} is invalid: {source}"),
                    }
                })?;
                ensure!(
                    !members.iter().any(|existing| existing == &member),
                    InvalidConfigSnafu {
                        key: ORDERED_MEMBERS_KEY,
                        message: format!("member[{index}] duplicates {member}"),
                    }
                );
                members.push(member);
            }
            Err(error) => {
                return Err(ChecklistConfigError::InvalidConfig {
                    key: ORDERED_MEMBERS_KEY,
                    message: format!("member[{index}]: {error}"),
                });
            }
        }
    }

    ensure!(
        !members.is_empty(),
        InvalidConfigSnafu {
            key: ORDERED_MEMBERS_KEY,
            message: "must contain at least one member".to_owned(),
        }
    );
    Ok(members)
}

fn read_socket_addr(
    config: &Config,
    key: &'static str,
) -> Result<SocketAddr, ChecklistConfigError> {
    let value = read_string(config, key)?;
    value.parse().map_err(
        |source: std::net::AddrParseError| ChecklistConfigError::InvalidConfig {
            key,
            message: source.to_string(),
        },
    )
}

fn read_string(config: &Config, key: &'static str) -> Result<String, ChecklistConfigError> {
    config
        .select(key)
        .as_string()
        .map_err(|source| ChecklistConfigError::InvalidConfig {
            key,
            message: source.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_checklist_app_config_from_single_toml() {
        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join("alice-checklist.toml");
        let config = parse_config_str(
            r#"
            [flotsync.examples.replicated-checklist]
            local-member = "alice"
            store-path = "alice.sqlite"
            store-secret-profile = "config-parse-profile"
            group-secret-password = "temporary-group-password"
            local-private-jwks-path = "alice-private.jwks"
            trusted-public-jwks-paths = ["bob-public.jwks", "/tmp/carol-public.jwks"]
            group-id = 123
            ordered-members = ["alice", "bob"]

            [flotsync.replication.runtime]
            local-endpoint-bind-addr = "127.0.0.1:45100"
            "#,
        )
        .expect("config should parse");

        let local_member = read_member(&config, LOCAL_MEMBER_KEY).expect("member should parse");
        let store_path = read_store_path(&config, &path).expect("store path should parse");
        let store_secret_profile =
            read_store_secret_profile(&config).expect("profile should parse");
        let local_private_jwks_path = read_path(&config, LOCAL_PRIVATE_JWKS_PATH_KEY, &path)
            .expect("local private key path should parse");
        let trusted_public_jwks_paths =
            read_path_list(&config, TRUSTED_PUBLIC_JWKS_PATHS_KEY, &path)
                .expect("trusted public key paths should parse");
        let group_id = read_group_id(&config).expect("group id should parse");
        let ordered_members = read_ordered_members(&config).expect("members should parse");
        let group_key =
            read_group_key(&config, group_id, &ordered_members).expect("group key should derive");
        let endpoint =
            read_socket_addr(&config, LOCAL_ENDPOINT_KEY).expect("endpoint should parse");

        assert_eq!(local_member, MemberIdentity::from_array(["alice"]));
        assert_eq!(store_path, temp_dir.join("alice.sqlite"));
        assert_eq!(store_secret_profile.as_str(), "config-parse-profile");
        assert_eq!(local_private_jwks_path, temp_dir.join("alice-private.jwks"));
        assert_eq!(
            trusted_public_jwks_paths,
            vec![
                temp_dir.join("bob-public.jwks"),
                PathBuf::from("/tmp/carol-public.jwks")
            ]
        );
        assert_eq!(group_id, GroupId(Uuid::from_u128(123)));
        assert_eq!(
            ordered_members,
            vec![
                MemberIdentity::from_array(["alice"]),
                MemberIdentity::from_array(["bob"])
            ]
        );
        assert_eq!(
            group_key,
            derive_group_key_from_password(b"temporary-group-password", group_id, &ordered_members,)
        );
        assert_eq!(endpoint, "127.0.0.1:45100".parse().unwrap());
    }

    #[test]
    fn rejects_config_when_local_member_is_not_ordered_member() {
        let config = parse_config_str(
            r#"
            [flotsync.examples.replicated-checklist]
            local-member = "carol"
            store-path = "alice.sqlite"
            store-secret-profile = "local-dev"
            group-secret-password = "temporary-group-password"
            local-private-jwks-path = "carol-private.jwks"
            trusted-public-jwks-paths = ["alice-public.jwks", "bob-public.jwks"]
            group-id = 123
            ordered-members = ["alice", "bob"]

            [flotsync.replication.runtime]
            local-endpoint-bind-addr = "127.0.0.1:45100"
            "#,
        )
        .expect("config should parse");

        let result = read_ordered_members(&config).and_then(|ordered_members| {
            let local_member = read_member(&config, LOCAL_MEMBER_KEY)?;
            ensure!(
                ordered_members.iter().any(|member| member == &local_member),
                InvalidConfigSnafu {
                    key: ORDERED_MEMBERS_KEY,
                    message: "ordered members must include configured local member".to_owned(),
                }
            );
            Ok(())
        });

        assert!(matches!(
            result,
            Err(ChecklistConfigError::InvalidConfig { .. })
        ));
    }

    #[test]
    fn rejects_empty_store_secret_profile() {
        let config = parse_config_str(
            r#"
            [flotsync.examples.replicated-checklist]
            store-secret-profile = ""
            "#,
        )
        .expect("config should parse");

        let result = read_store_secret_profile(&config);

        assert!(matches!(
            result,
            Err(ChecklistConfigError::InvalidConfig { key, .. })
                if key == STORE_SECRET_PROFILE_KEY
        ));
    }

    #[test]
    fn rejects_empty_group_secret_password() {
        let config = parse_config_str(
            r#"
            [flotsync.examples.replicated-checklist]
            group-secret-password = ""
            "#,
        )
        .expect("config should parse");

        let result = read_group_key(
            &config,
            GroupId(Uuid::from_u128(123)),
            &[MemberIdentity::from_array(["alice"])],
        );

        assert!(matches!(
            result,
            Err(ChecklistConfigError::InvalidConfig { key, .. })
                if key == GROUP_SECRET_PASSWORD_KEY
        ));
    }

    #[test]
    fn rejects_non_array_trusted_public_jwks_paths() {
        let config = parse_config_str(
            r#"
            [flotsync.examples.replicated-checklist]
            trusted-public-jwks-paths = "bob-public.jwks"
            "#,
        )
        .expect("config should parse");

        let result = read_path_list(
            &config,
            TRUSTED_PUBLIC_JWKS_PATHS_KEY,
            Path::new("alice.toml"),
        );

        assert!(matches!(
            result,
            Err(ChecklistConfigError::InvalidConfig { key, .. })
                if key == TRUSTED_PUBLIC_JWKS_PATHS_KEY
        ));
    }
}
