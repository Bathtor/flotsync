//! Application-facing replication runtime and API contracts.

/// Highest producer/group version accepted by the replication runtime.
///
/// The protocol reserves `u64::MAX` as an exhaustion sentinel. Decode paths
/// reject it before runtime code can derive catch-up intervals from the value.
pub const MAX_VERSION_VALUE: u64 = u64::MAX - 1;

pub mod api;
pub mod delivery;
pub mod runtime;
pub mod security_provisioning;
pub(crate) mod security_store;
pub mod store;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

pub use api::*;
pub use runtime::{load_replication_runtime, load_replication_runtime_with_runtime_config_toml};
pub use security_provisioning::{
    ProvisionSecurityError,
    ProvisionedReplicationSecurity,
    prepare_initial_group_security_material,
    provision_replication_security,
    validate_initial_group_security_material,
};
pub use store::SqliteReplicationStore;
