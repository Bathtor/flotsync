use flotsync_replication::{GroupId, MemberIdentity};
use kompact::config::{Config, ConfigError, parse_config_str};
use snafu::prelude::*;
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    str::FromStr,
};
use uuid::Uuid;

const LOCAL_MEMBER_KEY: &str = "flotsync.examples.replicated-checklist.local-member";
const STORE_PATH_KEY: &str = "flotsync.examples.replicated-checklist.store-path";
const GROUP_ID_KEY: &str = "flotsync.examples.replicated-checklist.group-id";
const ORDERED_MEMBERS_KEY: &str = "flotsync.examples.replicated-checklist.ordered-members";
const LOCAL_ENDPOINT_KEY: &str = "flotsync.replication.runtime.local-endpoint-bind-addr";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChecklistAppConfig {
    pub source_path: PathBuf,
    pub runtime_config_toml: String,
    pub local_member: MemberIdentity,
    pub store_path: PathBuf,
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
    let store_path = PathBuf::from(read_string(config, STORE_PATH_KEY)?);
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

fn read_ordered_members(config: &Config) -> Result<Vec<MemberIdentity>, ChecklistConfigError> {
    let members_lookup = config.select(ORDERED_MEMBERS_KEY);
    members_lookup
        .value()
        .map_err(|source| ChecklistConfigError::InvalidConfig {
            key: ORDERED_MEMBERS_KEY,
            message: source.to_string(),
        })?;

    let mut members = Vec::new();
    // TODO(flotsync-6in): Replace this index probing once Kompact exposes
    // selected config arrays as a slice or iterator.
    for index in 0.. {
        match members_lookup.get_index(index).as_string() {
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
            Err(error) if is_missing_path(&error) => break,
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

fn is_missing_path(error: &ConfigError) -> bool {
    matches!(error, ConfigError::PathError(path) if path.is_missing())
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
        )
        .expect("config should parse");

        let local_member = read_member(&config, LOCAL_MEMBER_KEY).expect("member should parse");
        let store_path = read_store_path(&config, &path).expect("store path should parse");
        let group_id = read_group_id(&config).expect("group id should parse");
        let ordered_members = read_ordered_members(&config).expect("members should parse");
        let endpoint =
            read_socket_addr(&config, LOCAL_ENDPOINT_KEY).expect("endpoint should parse");

        assert_eq!(local_member, MemberIdentity::from_array(["alice"]));
        assert_eq!(store_path, temp_dir.join("alice.sqlite"));
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
        let config = parse_config_str(
            r#"
            [flotsync.examples.replicated-checklist]
            local-member = "carol"
            store-path = "alice.sqlite"
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
}
