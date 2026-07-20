//! Polling helpers for asynchronous Kompact tests.

use super::EVENTUALLY_POLL_INTERVAL;
use kompact::prelude::{Component, ComponentDefinition};
use std::{
    fmt,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

/// Poll until `predicate` succeeds or `timeout` elapses.
///
/// # Panics
///
/// Panics with `failure_message` if `predicate` does not succeed before the timeout.
pub fn eventually(
    timeout: Duration,
    mut predicate: impl FnMut() -> bool,
    failure_message: impl fmt::Display,
) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if predicate() {
            return;
        }
        thread::sleep(EVENTUALLY_POLL_INTERVAL);
    }
    panic!("{failure_message}");
}

/// Wait until one component-state predicate becomes true or `timeout` elapses.
///
/// # Panics
///
/// Panics with `failure_message` if the predicate does not become true before the timeout.
pub fn eventually_component_state<C>(
    timeout: Duration,
    component: &Arc<Component<C>>,
    mut predicate: impl FnMut(&mut C) -> bool,
    failure_message: impl fmt::Display,
) where
    C: ComponentDefinition + Sized + 'static,
{
    eventually(
        timeout,
        || component.on_definition(|definition| predicate(definition)),
        failure_message,
    );
}
