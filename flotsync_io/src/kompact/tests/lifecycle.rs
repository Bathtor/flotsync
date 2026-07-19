//! Tests for I/O runtime lifecycle coordination.

use super::*;

#[test]
fn io_runtime_kill_notify_issues_bridge_shutdown_before_future_is_polled() {
    let system = build_test_kompact_system();
    let runtime = IoRuntime::build(&system, DriverConfig::default());

    runtime
        .start_notify(&system)
        .wait_timeout(WAIT_TIMEOUT)
        .expect("runtime start timeout")
        .expect("runtime start");
    let bridge = runtime.bridge_component().clone();

    let shutdown = runtime.kill_notify(&system);
    eventually(
        WAIT_TIMEOUT,
        || bridge.is_destroyed(),
        "runtime kill_notify should enqueue bridge shutdown before its future is polled",
    );
    shutdown
        .wait_timeout(WAIT_TIMEOUT)
        .expect("runtime kill timeout")
        .expect("runtime kill");

    system.shutdown().wait().expect("Kompact shutdown");
}

#[test]
fn io_runtime_starts_and_kills_shared_driver_bridge_pair() {
    let system = build_test_kompact_system();
    let runtime = IoRuntime::build(&system, DriverConfig::default());

    runtime
        .start_notify(&system)
        .wait_timeout(WAIT_TIMEOUT)
        .expect("runtime start timeout")
        .expect("runtime start");
    runtime
        .kill_notify(&system)
        .wait_timeout(WAIT_TIMEOUT)
        .expect("runtime kill timeout")
        .expect("runtime kill");

    system.shutdown().wait().expect("Kompact shutdown");
}
