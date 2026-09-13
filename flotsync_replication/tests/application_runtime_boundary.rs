//! Compile-time coverage for the normal downstream application runtime boundary.

use flotsync_replication::{
    ApplicationSynchronisation,
    ReplicationApi,
    ReplicationEventListener,
    ReplicationRuntimeLoad,
};
use flotsync_utils::testing::{assert_send, assert_sync};

#[test]
fn application_runtime_boundary_traits_are_send_and_sync() {
    assert_send::<ApplicationSynchronisation>();
    assert_send::<ReplicationRuntimeLoad>();
    assert_send::<dyn ReplicationApi>();
    assert_sync::<dyn ReplicationApi>();
    assert_send::<dyn ReplicationEventListener>();
    assert_sync::<dyn ReplicationEventListener>();
}

#[test]
fn application_runtime_boundary_futures_are_send() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/trybuild/runtime_futures_are_send.rs");
}
