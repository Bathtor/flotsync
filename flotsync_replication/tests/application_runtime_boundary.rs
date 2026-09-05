//! Compile-time coverage for the normal downstream application runtime boundary.

use flotsync_core::ApplicationId;
use flotsync_replication::{
    ApplicationSchemas,
    ApplicationSynchronisation,
    ReplicationApi,
    ReplicationConfig,
    ReplicationEventListener,
    ReplicationRuntimeLoad,
    ReplicationSecuritySecrets,
    ReplicationStore,
    load_replication_runtime,
};
use std::sync::Arc;

/// Require one named application-facing type to support executor hand-off.
fn assert_send<T: Send + ?Sized>() {}

/// Require one named application-facing type to support shared access.
fn assert_sync<T: Sync + ?Sized>() {}

/// Infer and require `Send` for an opaque return type which cannot be named at the type level.
fn assert_inferred_send<T: Send + ?Sized>(_: &T) {}

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
        None,
        listener,
        config,
        security_secrets,
    );
    assert_inferred_send(&load_future);
}

#[allow(dead_code)]
fn assert_api_operation_future_is_send(api: &dyn ReplicationApi) {
    let api_future = api.local_public_key_bundle();
    assert_inferred_send(&api_future);
}

#[test]
fn application_runtime_boundary_traits_are_send_and_sync() {
    assert_send::<ApplicationSynchronisation>();
    assert_send::<ReplicationRuntimeLoad>();
    assert_send::<dyn ReplicationApi>();
    assert_sync::<dyn ReplicationApi>();
    assert_send::<dyn ReplicationEventListener>();
    assert_sync::<dyn ReplicationEventListener>();
}
