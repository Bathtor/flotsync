mod component;
mod errors;
pub mod handle;
mod host;
mod in_memory;
mod messages;

pub use component::{ReplicationRuntimeComponent, ReplicationRuntimeMessage};
pub use handle::load_replication_runtime;

type ApiResult<T> = Result<T, crate::api::ApiError>;

#[cfg(test)]
mod tests;
