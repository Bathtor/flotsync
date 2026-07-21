use flotsync_core::{MemberIdentity, member::Identifier};
use flotsync_replication::LocalStoreSecretProfile;
use kompact::config::{Config, parse_config_str};
use snafu::prelude::*;
use std::{
    path::{Path, PathBuf},
    str::FromStr,
};

const LOCAL_MEMBER_KEY: &str = "flotsync.examples.replicated-checklist.local-member";
const STORE_PATH_KEY: &str = "flotsync.examples.replicated-checklist.store-path";
const STORE_SECRET_PROFILE_KEY: &str =
    "flotsync.examples.replicated-checklist.store-secret-profile";

#[derive(Clone, Debug)]
pub struct ChecklistAppConfig {
    pub source_path: PathBuf,
    /// Raw source TOML forwarded to the runtime config loader.
    pub runtime_config_toml: String,
    pub local_member: MemberIdentity,
    pub store_path: PathBuf,
    /// Device-local profile used to load or create the replication store-secret key.
    pub store_secret_profile: LocalStoreSecretProfile,
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
        let store_secret_profile = read_store_secret_profile(&config)?;

        Ok(Self {
            source_path,
            runtime_config_toml,
            local_member,
            store_path,
            store_secret_profile,
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
    use flotsync_core::member::MAX_IDENTIFIER_SEGMENTS;
    use itertools::Itertools;
    use uuid::Uuid;

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
            "#,
        )
        .expect("config should parse");

        let local_member = read_member(&config, LOCAL_MEMBER_KEY).expect("member should parse");
        let store_path = read_store_path(&config, &path).expect("store path should parse");
        let store_secret_profile =
            read_store_secret_profile(&config).expect("profile should parse");

        assert_eq!(local_member, MemberIdentity::from_array(["alice"]));
        assert_eq!(store_path, temp_dir.join("alice.sqlite"));
        assert_eq!(store_secret_profile.as_str(), "config-parse-profile");
    }

    #[test]
    fn loads_checklist_app_config_without_runtime_local_endpoint_bind() {
        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join(format!(
            "alice-checklist-{}.toml",
            Uuid::new_v4().as_hyphenated()
        ));
        std::fs::write(
            &path,
            r#"
            [flotsync.examples.replicated-checklist]
            local-member = "alice"
            store-path = "alice.sqlite"
            store-secret-profile = "config-load-profile"
            "#,
        )
        .expect("test config file should be written");

        let loaded = ChecklistAppConfig::load(&path).expect("checklist config should load");

        assert_eq!(loaded.local_member, MemberIdentity::from_array(["alice"]));
        assert_eq!(loaded.store_path, temp_dir.join("alice.sqlite"));
        assert_eq!(
            loaded.runtime_config_toml,
            std::fs::read_to_string(&path).unwrap()
        );
        std::fs::remove_file(path).expect("test config file should be removed");
    }

    #[test]
    fn rejects_overlong_member_identifier_in_config() {
        let member = std::iter::repeat_n("s", MAX_IDENTIFIER_SEGMENTS + 1).join(".");
        let config = parse_config_str(&format!(
            r#"
            [flotsync.examples.replicated-checklist]
            local-member = "{member}"
            "#
        ))
        .expect("config should parse");

        let result = read_member(&config, LOCAL_MEMBER_KEY);

        assert!(matches!(
            result,
            Err(ChecklistConfigError::InvalidConfig { key, .. }) if key == LOCAL_MEMBER_KEY
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
}
