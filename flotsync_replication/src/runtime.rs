use crate::api::{
    LoadError,
    ReplicationApi,
    ReplicationConfig,
    ReplicationEventListener,
    ReplicationStore,
};
use flotsync_core::member::Identifier;
use std::sync::Arc;

/// Entry point for loading replication state for one application id.
///
/// The concrete runtime implementation will be wired in this module in follow-up tasks.
pub async fn load_replication_runtime(
    application_id: Identifier,
    store: Arc<dyn ReplicationStore>,
    listener: Arc<dyn ReplicationEventListener>,
    config: ReplicationConfig,
) -> Result<Arc<dyn ReplicationApi>, LoadError> {
    let _local_member = store
        .local_member_identity()
        .await
        .map_err(|source| LoadError::runtime(application_id.clone(), source))?;
    let _ = listener;
    let _ = config;
    Err(LoadError::Unavailable { application_id })
}
