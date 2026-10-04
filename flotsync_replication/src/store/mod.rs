//! Replication-store implementations and store-specific test support.

#[cfg(test)]
pub(crate) use sqlite::SqliteTextPageContinuation;
#[cfg(test)]
pub(crate) use sqlite::SqliteUpdatePageContinuation;
pub use sqlite::{SqliteReplicationStore, SqliteReplicationStoreProvisioner};

#[cfg(test)]
pub(crate) mod test_support;

mod sqlite;
