//! Replication-store implementations and store-specific test support.

pub use sqlite::{SqliteReplicationStore, SqliteReplicationStoreProvisioner};

#[cfg(test)]
pub(crate) mod test_support;

mod sqlite;
