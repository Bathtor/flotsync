//! Compile-time coverage for the normal downstream application runtime boundary.

use flotsync_core::ApplicationId;
use flotsync_replication::{
    ApplicationSchemas,
    ReplicationApi,
    ReplicationConfig,
    ReplicationEventListener,
    ReplicationSecuritySecrets,
    ReplicationStore,
    load_replication_runtime,
};
use std::sync::Arc;

/// Require one value's type to satisfy the executor hand-off bound at compile time.
fn assert_send<T: Send>(_: &T) {}

/// Require one application-facing trait object to be shareable at compile time.
fn assert_send_sync<T: Send + Sync + ?Sized>() {}

#[allow(dead_code)]
fn assert_normal_build_load_future_is_send(
    application_id: ApplicationId,
    application_schemas: &'static ApplicationSchemas,
    store: Arc<dyn ReplicationStore>,
    listener: Arc<dyn ReplicationEventListener>,
    config: ReplicationConfig,
    security_secrets: ReplicationSecuritySecrets,
) {
    let load_future = load_replication_runtime(
        application_id,
        application_schemas,
        store,
        listener,
        config,
        security_secrets,
    );
    assert_send(&load_future);
}

#[allow(dead_code)]
fn assert_api_operation_future_is_send(api: &dyn ReplicationApi) {
    let api_future = api.local_public_key_bundle();
    assert_send(&api_future);
}

#[test]
fn application_runtime_boundary_traits_are_send_and_sync() {
    assert_send_sync::<dyn ReplicationApi>();
    assert_send_sync::<dyn ReplicationEventListener>();
}
