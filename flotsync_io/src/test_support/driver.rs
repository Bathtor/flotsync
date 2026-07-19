//! Helpers for driving IO requests and components in tests.

use super::*;

/// Waits for a driver request to complete within [`WAIT_TIMEOUT`].
///
/// # Panics
///
/// Panics if the request fails or does not produce a reply before [`WAIT_TIMEOUT`].
#[must_use]
pub fn wait_for_driver_request<T>(mut request: DriverRequest<T>) -> T {
    eventually_value(
        WAIT_TIMEOUT,
        || match request.try_receive() {
            Ok(Some(reply)) => Some(reply),
            Ok(None) => None,
            Err(error) => panic!("driver request failed: {error}"),
        },
        "timed out waiting for driver request reply",
    )
}

/// Waits for the next driver event matching `predicate`.
///
/// # Panics
///
/// Panics if driver event retrieval fails or no matching event arrives before [`WAIT_TIMEOUT`].
pub fn wait_for_driver_event(
    driver: &IoDriver,
    mut predicate: impl FnMut(&DriverEvent) -> bool,
) -> DriverEvent {
    eventually_value(
        WAIT_TIMEOUT,
        || match driver.try_next_event() {
            Ok(Some(event)) if predicate(&event) => Some(event),
            Ok(Some(other)) => {
                log::debug!(
                    "ignoring unrelated driver event while waiting in integration test: {other:?}"
                );
                None
            }
            Ok(None) => None,
            Err(error) => panic!("driver event retrieval failed: {error}"),
        },
        "timed out waiting for driver event",
    )
}

/// Asserts that no driver event arrives for `duration`.
///
/// # Panics
///
/// Panics if a driver event arrives, or if driver event retrieval fails, during `duration`.
pub fn assert_no_driver_event(driver: &IoDriver, duration: Duration) {
    assert_never(
        duration,
        || match driver.try_next_event() {
            Ok(Some(event)) => panic!("unexpected driver event while expecting silence: {event:?}"),
            Ok(None) => false,
            Err(error) => panic!("driver event retrieval failed: {error}"),
        },
        "unexpected driver event while expecting silence",
    );
}

/// Receives until `predicate` selects a value.
///
/// # Panics
///
/// Panics if no event is received before [`WAIT_TIMEOUT`].
pub fn recv_until<T>(rx: &mpsc::Receiver<T>, mut predicate: impl FnMut(&T) -> bool) -> T {
    loop {
        let value = rx
            .recv_timeout(WAIT_TIMEOUT)
            .expect("timed out waiting for integration-test event");
        if predicate(&value) {
            return value;
        }
    }
}

/// Starts a component and waits for the lifecycle future to complete.
///
/// # Panics
///
/// Panics if the component does not start successfully before [`WAIT_TIMEOUT`].
pub fn start_component<C>(system: &KompactSystem, component: &Arc<Component<C>>)
where
    C: ComponentDefinition + ComponentLifecycle + Sized + 'static,
{
    system
        .start_notify(component)
        .wait_timeout(WAIT_TIMEOUT)
        .expect("component start");
}

/// Kills a component and waits for the lifecycle future to complete.
///
/// # Panics
///
/// Panics if the component does not stop successfully before [`WAIT_TIMEOUT`].
pub fn kill_component<C>(system: &KompactSystem, component: Arc<Component<C>>)
where
    C: ComponentDefinition + ComponentLifecycle + Sized + 'static,
{
    system
        .kill_notify(component)
        .wait_timeout(WAIT_TIMEOUT)
        .expect("component kill");
}
