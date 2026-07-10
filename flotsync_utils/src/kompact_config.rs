//! Shared Kompact configuration read helpers.

use kompact::{
    KompactLogger,
    config::{Config, ConfigEntry, ConfigValueType},
    prelude::warn,
};
use std::fmt;

/// Extension helpers for Kompact config reads with Flotsync fallback logging.
pub trait ConfigReadExt {
    /// Read a config entry, warning and falling back to the declared default when parsing fails.
    ///
    /// Missing keys still use Kompact's normal `read_or_default` behaviour and do not log.
    ///
    /// # Panics
    ///
    /// Panics if `key` has no declared default and the configured value cannot be read.
    fn read_or_default_warn<T>(&self, logger: &KompactLogger, key: &ConfigEntry<T>) -> T::Value
    where
        T: ConfigValueType,
        T::Value: fmt::Debug;
}

impl ConfigReadExt for Config {
    fn read_or_default_warn<T>(&self, logger: &KompactLogger, key: &ConfigEntry<T>) -> T::Value
    where
        T: ConfigValueType,
        T::Value: fmt::Debug,
    {
        match self.read_or_default(key) {
            Ok(value) => value,
            Err(error) => {
                let fallback = key.default().unwrap_or_else(|| {
                    panic!(
                        "read_or_default_warn requires config entry {} to declare a default",
                        key.key
                    )
                });
                warn!(
                    logger,
                    "Failed to read Kompact config key {}: {}. Falling back to {:?}",
                    key.key,
                    error,
                    fallback
                );
                fallback
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kompact::{
        config::{Config, UsizeValue, parse_config_str},
        kompact_config,
        test_support::captured_kompact_logger,
    };

    kompact_config! {
        TEST_LIMIT,
        key = "flotsync.test.limit",
        type = UsizeValue,
        default = 7,
        validate = |value| *value > 0,
        doc = "Test config value with a non-zero limit.",
        version = "0.1.0"
    }

    #[test]
    fn reads_configured_value() {
        let config = parse_config_str("flotsync.test.limit = 11").expect("valid config");
        let logger = captured_kompact_logger();

        let value = config.read_or_default_warn(&logger, &TEST_LIMIT);

        assert_eq!(value, 11);
    }

    #[test]
    fn reads_default_for_missing_value_without_warning_path() {
        let config = Config::new();
        let logger = captured_kompact_logger();

        let value = config.read_or_default_warn(&logger, &TEST_LIMIT);

        assert_eq!(value, 7);
    }

    #[test]
    fn falls_back_to_default_for_invalid_value() {
        let config = parse_config_str("flotsync.test.limit = 0").expect("valid config shape");
        let logger = captured_kompact_logger();

        let value = config.read_or_default_warn(&logger, &TEST_LIMIT);

        assert_eq!(value, 7);
    }
}
