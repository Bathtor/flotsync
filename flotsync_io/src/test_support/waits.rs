//! Synchronous wait helpers for tests.

#[allow(
    clippy::wildcard_imports,
    reason = "The private helper module intentionally shares its parent's local implementation vocabulary."
)]
use super::*;

/// Returns the loopback address for the supplied port.
#[must_use]
pub fn localhost(port: u16) -> SocketAddr {
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
}

/// Waits until `probe` returns one value or `timeout` elapses.
pub fn eventually_value<T, M>(
    timeout: Duration,
    probe: impl FnMut() -> Option<T>,
    failure_message: M,
) -> T
where
    M: Display,
{
    eventually_value_with_poll(timeout, EVENTUALLY_POLL_INTERVAL, probe, failure_message)
}

/// Waits until `probe` returns one value using the supplied poll cadence.
///
/// # Panics
///
/// Panics with `failure_message` if `probe` does not return a value before `timeout`.
pub fn eventually_value_with_poll<T, M>(
    timeout: Duration,
    poll_interval: Duration,
    mut probe: impl FnMut() -> Option<T>,
    failure_message: M,
) -> T
where
    M: Display,
{
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(value) = probe() {
            return value;
        }

        let now = Instant::now();
        assert!(now < deadline, "{failure_message}");

        thread::sleep(deadline.saturating_duration_since(now).min(poll_interval));
    }
}

/// Waits until `predicate` becomes true or `timeout` elapses.
pub fn eventually<M>(timeout: Duration, mut predicate: impl FnMut() -> bool, failure_message: M)
where
    M: Display,
{
    eventually_value(timeout, || option_when!(predicate(), ()), failure_message);
}

/// Waits for one future to resolve within `timeout`.
pub fn wait_for_future<F, M>(timeout: Duration, future: F, failure_message: M) -> F::Output
where
    F: Future,
    M: Display,
{
    let mut future = pin!(future);
    eventually_value(timeout, || future.as_mut().now_or_never(), failure_message)
}

/// Waits until one component-state predicate becomes true or `timeout`
/// elapses.
pub fn eventually_component_state<C, M>(
    timeout: Duration,
    component: &Arc<Component<C>>,
    mut predicate: impl FnMut(&mut C) -> bool,
    failure_message: M,
) where
    C: ComponentDefinition + Sized + 'static,
    M: Display,
{
    eventually(
        timeout,
        || component.on_definition(|definition| predicate(definition)),
        failure_message,
    );
}

/// Asserts that `predicate` never becomes true during `duration`.
///
/// # Panics
///
/// Panics with `failure_message` if `predicate` becomes true before `duration` elapses.
pub fn assert_never<M>(duration: Duration, mut predicate: impl FnMut() -> bool, failure_message: M)
where
    M: Display,
{
    let deadline = Instant::now() + duration;
    loop {
        assert!(!predicate(), "{failure_message}");

        let now = Instant::now();
        if now >= deadline {
            return;
        }

        thread::sleep(
            deadline
                .saturating_duration_since(now)
                .min(EVENTUALLY_POLL_INTERVAL),
        );
    }
}
