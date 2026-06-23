//! Shared test-only helpers for replication delivery integration tests.
//!
//! Route-transport harnesses live with `flotsync_routes`; this module
//! keeps the semantic delivery tests on their existing local import path.

// TODO(flotsync-lobi): Update the remaining delivery-test use sites to import
// directly from `flotsync_routes::test_support` before opening the PR.
pub(crate) use flotsync_routes::test_support::{
    FULL_STACK_WAIT_TIMEOUT,
    TransportHarnessCore,
    build_delivery_test_system,
    build_delivery_test_system_with,
    default_udpour_config,
    member_identity,
};
