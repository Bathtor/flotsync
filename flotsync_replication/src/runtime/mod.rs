//! Replication runtime component, host, and runtime protocol support.

use kompact::{config::UsizeValue, kompact_config};

/// Default maximum group size for inlining public key bundles in bootstrap messages.
pub const DEFAULT_MAX_INLINE_BOOTSTRAP_PUBLIC_KEY_BUNDLES: usize = 10;

/// Kompact configuration keys consumed by the replication runtime.
pub mod config_keys {
    use super::{DEFAULT_MAX_INLINE_BOOTSTRAP_PUBLIC_KEY_BUNDLES, UsizeValue, kompact_config};

    kompact_config! {
        BOOTSTRAP_MAX_INLINE_PUBLIC_KEY_BUNDLES,
        key = "flotsync.replication.runtime.bootstrap.max-inline-public-key-bundles",
        type = UsizeValue,
        default = DEFAULT_MAX_INLINE_BOOTSTRAP_PUBLIC_KEY_BUNDLES,
        doc = "Maximum group size for including inline public key bundles in bootstrap messages. Larger groups carry fingerprints only. Set to 0 to always elide inline bundles.",
        version = "0.1.0"
    }
}

mod catch_up_manager;
mod component;
mod errors;
pub mod handle;
pub(crate) mod host;
mod in_memory;
mod pending_group;
mod replay;
mod store_security_validation;
mod summary_request_manager;

pub use component::{ReplicationRuntimeComponent, ReplicationRuntimeMessage};
pub(crate) use errors::BoxedError;
pub use handle::{load_replication_runtime, load_replication_runtime_with_runtime_config_toml};

#[cfg(test)]
mod tests;
