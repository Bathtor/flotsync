//! Application-facing replication runtime and API contracts.

/// Highest producer/group version accepted by the replication runtime.
///
/// The protocol reserves `u64::MAX` as an exhaustion sentinel. Decode paths
/// reject it before runtime code can derive catch-up intervals from the value.
pub const MAX_VERSION_VALUE: u64 = u64::MAX - 1;

pub mod api;
pub(crate) mod codecs;
pub mod delivery;
mod local_identity;
pub mod runtime;
pub(crate) mod security_store;
pub mod store;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

pub use api::*;
pub use local_identity::{
    ProvisionLocalIdentityError,
    ProvisionedLocalIdentity,
    provision_local_identity,
};
pub use runtime::{
    ApplicationSynchronisation,
    GroupChangesSynchronisation,
    GroupSnapshotSynchronisation,
    ReplicationRuntime,
    ReplicationRuntimeBuilder,
    ReplicationRuntimeLoad,
    RetiredGroupSynchronisation,
    SingleGroupSynchronisation,
};
pub use store::{SqliteReplicationStore, SqliteReplicationStoreProvisioner};
