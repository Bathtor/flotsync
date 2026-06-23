//! Shared test-only helpers for replication delivery integration tests.
//!
//! Route-transport harnesses live with `flotsync_route_transport`; this module
//! keeps the semantic delivery tests on their existing local import path.

pub(crate) use flotsync_route_transport::test_support::{
    FULL_STACK_WAIT_TIMEOUT,
    TransportHarnessCore,
    build_delivery_test_system,
    build_delivery_test_system_with,
    default_udpour_config,
    member_identity,
};
