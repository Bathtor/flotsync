use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use flotsync_replication::{GroupId, MemberIdentity, ReplicationSecuritySecrets, StoreSecretKeyId};
use flotsync_security::StoreSecretKey;
use kompact::config::{Config, parse_config_str};
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
const STORE_SECRET_KEY_ID_KEY: &str = "flotsync.examples.replicated-checklist.store-secret-key-id";
const STORE_SECRET_KEY_BASE64_KEY: &str =
    "flotsync.examples.replicated-checklist.store-secret-key-base64";
const GROUP_ID_KEY: &str = "flotsync.examples.replicated-checklist.group-id";
const ORDERED_MEMBERS_KEY: &str = "flotsync.examples.replicated-checklist.ordered-members";
const LOCAL_ENDPOINT_KEY: &str = "flotsync.replication.runtime.local-endpoint-bind-addr";

#[derive(Clone, Debug)]
pub struct ChecklistAppConfig {
    pub source_path: PathBuf,
    /// Raw source TOML forwarded to the runtime config loader.
    ///
    /// Contract until `flotsync-sec.9`: this string may still contain the
    /// temporary example store-secret key config, so treat this whole config
    /// value as secret-bearing and do not log it in production paths.
    pub runtime_config_toml: String,
    pub local_member: MemberIdentity,
    pub store_path: PathBuf,
    /// Device-local security input passed to the replication runtime loader.
    pub replication_security: ReplicationSecuritySecrets,
    pub group_id: GroupId,
    pub ordered_members: Vec<MemberIdentity>,
    pub local_endpoint_bind_addr: SocketAddr,
}

impl ChecklistAppConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ChecklistConfigError> {
        let source_path = path.as_ref().to_path_buf();
        let runtime_config_toml = std::fs::read_to_string(&source_path).map_err(|source| {
            ChecklistConfigError::ReadFile {
                path: source_path.clone(),
                source,
            }
        })?;
        let config = parse_config_str(&runtime_config_toml).map_err(|source| {
            ChecklistConfigError::ParseFile {
                path: source_path.clone(),
                message: source.to_string(),
            }
        })?;

        let local_member = read_member(&config, LOCAL_MEMBER_KEY)?;
        let store_path = read_store_path(&config, &source_path)?;
        let replication_security = read_replication_security(&config)?;
        let group_id = read_group_id(&config)?;
        let ordered_members = read_ordered_members(&config)?;
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
            replication_security,
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

fn read_member(config: &Config, key: &'static str) -> Result<MemberIdentity, ChecklistConfigError> {
    let value = read_string(config, key)?;
    MemberIdentity::from_str(&value).map_err(|source| ChecklistConfigError::InvalidConfig {
        key,
        message: source.to_string(),
    })
}

fn read_store_path(config: &Config, source_path: &Path) -> Result<PathBuf, ChecklistConfigError> {
    let store_path_value = read_string(config, STORE_PATH_KEY)?;
    let store_path = PathBuf::from(store_path_value);
    if store_path.is_absolute() {
        return Ok(store_path);
    }
    let Some(base_dir) = source_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    else {
        return Ok(store_path);
    };
    Ok(base_dir.join(store_path))
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

/// Build replication loader security input from the checklist application config.
fn read_replication_security(
    config: &Config,
) -> Result<ReplicationSecuritySecrets, ChecklistConfigError> {
    let key_id_value = read_string(config, STORE_SECRET_KEY_ID_KEY)?;
    let key_id = StoreSecretKeyId::new(key_id_value);
    let key = read_store_secret_key_base64(config)?;
    Ok(ReplicationSecuritySecrets::new(key_id, Arc::new(key)))
}

/// Read the configured 32-byte store-secret key from URL-safe base64 without padding.
fn read_store_secret_key_base64(config: &Config) -> Result<StoreSecretKey, ChecklistConfigError> {
    let value = read_string(config, STORE_SECRET_KEY_BASE64_KEY)?;
    let decoded = URL_SAFE_NO_PAD.decode(value.as_bytes()).map_err(|source| {
        ChecklistConfigError::InvalidConfig {
            key: STORE_SECRET_KEY_BASE64_KEY,
            message: format!("must be URL-safe base64 without padding: {source}"),
        }
    })?;
    StoreSecretKey::try_from_slice(decoded.as_slice()).map_err(|source| {
        ChecklistConfigError::InvalidConfig {
            key: STORE_SECRET_KEY_BASE64_KEY,
            message: source.to_string(),
        }
    })
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

    fn test_store_secret_key_base64() -> String {
        URL_SAFE_NO_PAD.encode([17_u8; 32])
    }

    #[test]
    fn parses_checklist_app_config_from_single_toml() {
        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join("alice-checklist.toml");
        let store_secret_key = test_store_secret_key_base64();
        let config = parse_config_str(&format!(
            r#"
            [flotsync.examples.replicated-checklist]
            local-member = "alice"
            store-path = "alice.sqlite"
            store-secret-key-id = "alice-test-key"
            store-secret-key-base64 = "{store_secret_key}"
            group-id = 123
            ordered-members = ["alice", "bob"]

            [flotsync.replication.runtime]
            local-endpoint-bind-addr = "127.0.0.1:45100"

            [[flotsync.replication.runtime.static-peer-routes]]
            name = "bob"
            protocol = "udp"
            ip = "127.0.0.1"
            port = 45101
            "#,
        ))
        .expect("config should parse");

        let local_member = read_member(&config, LOCAL_MEMBER_KEY).expect("member should parse");
        let store_path = read_store_path(&config, &path).expect("store path should parse");
        let replication_security =
            read_replication_security(&config).expect("security should parse");
        let group_id = read_group_id(&config).expect("group id should parse");
        let ordered_members = read_ordered_members(&config).expect("members should parse");
        let endpoint =
            read_socket_addr(&config, LOCAL_ENDPOINT_KEY).expect("endpoint should parse");

        assert_eq!(local_member, MemberIdentity::from_array(["alice"]));
        assert_eq!(store_path, temp_dir.join("alice.sqlite"));
        assert_eq!(
            replication_security.store_secret_key_id().as_str(),
            "alice-test-key"
        );
        assert_eq!(group_id, GroupId(Uuid::from_u128(123)));
        assert_eq!(
            ordered_members,
            vec![
                MemberIdentity::from_array(["alice"]),
                MemberIdentity::from_array(["bob"])
            ]
        );
        assert_eq!(endpoint, "127.0.0.1:45100".parse().unwrap());
    }

    #[test]
    fn rejects_config_when_local_member_is_not_ordered_member() {
        let store_secret_key = test_store_secret_key_base64();
        let config = parse_config_str(&format!(
            r#"
            [flotsync.examples.replicated-checklist]
            local-member = "carol"
            store-path = "alice.sqlite"
            store-secret-key-id = "alice-test-key"
            store-secret-key-base64 = "{store_secret_key}"
            group-id = 123
            ordered-members = ["alice", "bob"]

            [flotsync.replication.runtime]
            local-endpoint-bind-addr = "127.0.0.1:45100"
            "#,
        ))
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
    fn rejects_store_secret_key_base64_with_wrong_decoded_length() {
        let config = parse_config_str(
            r#"
            [flotsync.examples.replicated-checklist]
            store-secret-key-id = "alice-test-key"
            store-secret-key-base64 = "AQID"
            "#,
        )
        .expect("config should parse");

        let result = read_store_secret_key_base64(&config);

        assert!(matches!(
            result,
            Err(ChecklistConfigError::InvalidConfig { key, .. })
                if key == STORE_SECRET_KEY_BASE64_KEY
        ));
    }

    #[test]
    fn rejects_invalid_store_secret_key_base64() {
        let config = parse_config_str(
            r#"
            [flotsync.examples.replicated-checklist]
            store-secret-key-id = "alice-test-key"
            store-secret-key-base64 = "not valid base64?!"
            "#,
        )
        .expect("config should parse");

        let result = read_store_secret_key_base64(&config);

        assert!(matches!(
            result,
            Err(ChecklistConfigError::InvalidConfig { key, .. })
                if key == STORE_SECRET_KEY_BASE64_KEY
        ));
    }
}
