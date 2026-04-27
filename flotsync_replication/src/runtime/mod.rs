mod component;
mod errors;
pub mod handle;
mod host;
mod in_memory;
pub(crate) mod messages;

pub use component::{ReplicationRuntimeComponent, ReplicationRuntimeMessage};
pub use handle::{load_replication_runtime, load_replication_runtime_with_runtime_config_toml};

#[cfg(test)]
mod tests;
