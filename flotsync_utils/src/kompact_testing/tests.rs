//! Unit tests for Kompact testing support.

use std::{sync::Arc, time::Duration};

use kompact::test_support::test_kompact_config;

use super::*;

struct TestPort;
impl Port for TestPort {
    type Indication = String;
    type Request = String;
}

const TEST_TIMEOUT: Duration = Duration::from_secs(1);
const ABSENCE_WINDOW: Duration = Duration::from_millis(25);

fn start_component<C>(system: &KompactSystem, component: &Arc<Component<C>>)
where
    C: ComponentDefinition + ComponentLifecycle + Sized + 'static,
{
    system
        .start_notify(component)
        .wait_timeout(TEST_TIMEOUT)
        .expect("component start");
}

fn wait_observed<T>(future: ObservedFuture<T>) -> T {
    future
        .wait_timeout(TEST_TIMEOUT)
        .expect("timed out waiting for observed event")
        .expect("tester promise must stay live")
}

#[test]
fn sidecar_observes_logged_and_future_indications() {
    let system = test_kompact_config().build().wait().expect("system");
    let sender = system.create(TestPort::tester_component_sidecar);
    let observer = system.create(TestPort::tester_component_sidecar);

    biconnect_components(&sender, &observer).expect("connection");
    let sender_ref = sender.actor_ref();
    let observer_ref = observer.actor_ref();

    start_component(&system, &sender);
    start_component(&system, &observer);

    sender_ref.inject_indication("First message".to_owned());

    let matching_event = observer_ref
        .observe(|(index, event)| {
            index == 0
                && matches!(
                    event,
                    TestEvent::Indication(indication) if indication == "First message"
                )
        })
        .wait_timeout(TEST_TIMEOUT)
        .unwrap();
    assert_eq!(matching_event.index(), 0);
    assert_eq!(matching_event.unwrap_indication(), "First message");

    let matching_event_f = observer_ref.observe_indication(|event| event.contains("Second"));

    sender_ref.inject_indication("Second message".to_owned());

    let matching_event = wait_observed(matching_event_f);
    assert_eq!(matching_event.index(), 1);
    assert_eq!(matching_event.indication(), "Second message");

    system.shutdown().wait().expect("shutdown");
}

#[test]
fn sidecar_observes_requests_after_index() {
    let system = test_kompact_config().build().wait().expect("system");
    let provider = system.create(TestPort::tester_component_sidecar);
    let requirer = system.create(TestPort::tester_component_sidecar);

    biconnect_components(&provider, &requirer).expect("connection");
    let provider_ref = provider.actor_ref();
    let requirer_ref = requirer.actor_ref();

    start_component(&system, &provider);
    start_component(&system, &requirer);

    requirer_ref.inject_request("Repeated request".to_owned());

    let first = provider_ref
        .observe_request(|event| event == "Repeated request")
        .wait_timeout(TEST_TIMEOUT)
        .expect("timed out waiting for first event")
        .expect("tester promise must stay live");
    assert_eq!(first.index(), 0);
    assert_eq!(first.request(), "Repeated request");

    let second_f =
        provider_ref.observe_request_from(first.index() + 1, |event| event == "Repeated request");

    requirer_ref.inject_request("Noise request".to_owned());
    requirer_ref.inject_request("Repeated request".to_owned());

    let second = wait_observed(second_f);
    assert_eq!(second.index(), 2);
    assert_eq!(second.request(), "Repeated request");

    system.shutdown().wait().expect("shutdown");
}

#[test]
fn sidecar_fails_if_matching_event_is_observed_in_window() {
    let system = test_kompact_config().build().wait().expect("system");
    let sender = system.create(TestPort::tester_component_sidecar);
    let observer = system.create(TestPort::tester_component_sidecar);

    biconnect_components(&sender, &observer).expect("connection");
    let sender_ref = sender.actor_ref();
    let observer_ref = observer.actor_ref();

    start_component(&system, &sender);
    start_component(&system, &observer);

    sender_ref.inject_indication("Forbidden".to_owned());
    let old_event = wait_observed(observer_ref.observe_indication(|event| event == "Forbidden"));

    let ignores_old_event =
        observer_ref.fail_if_indication_observed(ABSENCE_WINDOW, |event| event == "Forbidden");
    assert!(wait_observed(ignores_old_event).is_ok());

    let fails_from_cursor =
        observer_ref.fail_if_indication_observed_from(old_event.index(), ABSENCE_WINDOW, |event| {
            event == "Forbidden"
        });
    let failure = wait_observed(fails_from_cursor).expect_err("old event must fail cursor check");
    assert_eq!(failure.index(), old_event.index());
    assert_eq!(failure.indication(), "Forbidden");

    let future_failure = observer_ref.fail_if_indication_observed_from(
        old_event.index() + 1,
        TEST_TIMEOUT,
        |event| event == "Forbidden",
    );
    sender_ref.inject_indication("Forbidden".to_owned());

    let failure = wait_observed(future_failure).expect_err("future event must fail check");
    assert_eq!(failure.indication(), "Forbidden");
    assert!(failure.index() > old_event.index());

    system.shutdown().wait().expect("shutdown");
}

#[test]
fn forwarding_tester_relays_indications_and_requests() {
    let system = test_kompact_config().build().wait().expect("system");
    let source = system.create(TestPort::tester_component_sidecar);
    let proxy = system.create(TestPort::tester_component_forwarding);
    let sink = system.create(TestPort::tester_component_sidecar);

    biconnect_components(&source, &proxy).expect("source/proxy connection");
    biconnect_components(&proxy, &sink).expect("proxy/sink connection");

    let source_ref = source.actor_ref();
    let proxy_ref = proxy.actor_ref();
    let sink_ref = sink.actor_ref();

    start_component(&system, &source);
    start_component(&system, &proxy);
    start_component(&system, &sink);

    let proxy_f = proxy_ref.observe_indication(|event| event == "Forwarded indication");
    let sink_f = sink_ref.observe_indication(|event| event == "Forwarded indication");

    source_ref.inject_indication("Forwarded indication".to_owned());

    let proxy_event = wait_observed(proxy_f);
    let sink_event = wait_observed(sink_f);
    assert_eq!(proxy_event.indication(), "Forwarded indication");
    assert_eq!(sink_event.indication(), "Forwarded indication");

    let proxy_f = proxy_ref.observe_request(|event| event == "Forwarded request");
    let source_f = source_ref.observe_request(|event| event == "Forwarded request");

    sink_ref.inject_request("Forwarded request".to_owned());

    let proxy_event = wait_observed(proxy_f);
    let source_event = wait_observed(source_f);
    assert_eq!(proxy_event.request(), "Forwarded request");
    assert_eq!(source_event.request(), "Forwarded request");

    system.shutdown().wait().expect("shutdown");
}
