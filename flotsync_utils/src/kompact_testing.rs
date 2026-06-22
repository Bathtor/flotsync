//! Test helpers for observing and injecting Kompact port traffic.
//!
//! [`PortTesterComponent`] owns one provided port and one required port of the
//! same Kompact [`Port`] type. Tests can connect it to components under test
//! and then use its actor reference to inject requests or indications, or to
//! wait until matching traffic has been observed.
//!
//! In sidecar mode, the tester only records traffic. This relies on Kompact
//! port fan-out: a connected observer receives the same events as the
//! production recipient but does not forward them further.
//!
//! In forwarding mode, the tester acts as an inline proxy. Each event observed
//! on one side is recorded and then triggered on the opposite side.

use std::{fmt, sync::Arc};

use kompact::prelude::*;

/// One event observed on a Kompact port pair.
///
/// Requests are traffic entering a provided port. Indications are traffic
/// entering a required port.
pub enum TestEvent<PortType>
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: Send + Sync + 'static,
    PortType::Request: Send + Sync + 'static,
{
    /// A request observed on the tester's provided port.
    Request(PortType::Request),
    /// An indication observed on the tester's required port.
    Indication(PortType::Indication),
}

impl<PortType> TestEvent<PortType>
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: fmt::Debug + Send + Sync + 'static,
    PortType::Request: fmt::Debug + Send + Sync + 'static,
{
    /// Returns the indication carried by this event.
    ///
    /// # Panics
    ///
    /// Panics when this event is a request.
    #[must_use]
    pub fn unwrap_indication(&self) -> &PortType::Indication {
        if let Self::Indication(event) = self {
            event
        } else {
            panic!("Cannot unwrap indication from {self:?}");
        }
    }

    /// Returns the request carried by this event.
    ///
    /// # Panics
    ///
    /// Panics when this event is an indication.
    #[must_use]
    pub fn unwrap_request(&self) -> &PortType::Request {
        if let Self::Request(event) = self {
            event
        } else {
            panic!("Cannot unwrap request from {self:?}");
        }
    }
}

impl<PortType> fmt::Debug for TestEvent<PortType>
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: fmt::Debug + Send + Sync + 'static,
    PortType::Request: fmt::Debug + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Request(r) => f.debug_tuple("Request").field(r).finish(),
            Self::Indication(i) => f.debug_tuple("Indication").field(i).finish(),
        }
    }
}

/// Predicate used to select a matching event from a tester's event log.
///
/// The predicate receives both the zero-based event-log index and the event.
/// It must be `Send` because it is carried as an actor message to the tester
/// component.
pub struct EventPredicate<PortType>(BoxedEventPredicate<PortType>)
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: Send + Sync + 'static,
    PortType::Request: Send + Sync + 'static;

impl<PortType> EventPredicate<PortType>
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: Send + Sync + 'static,
    PortType::Request: Send + Sync + 'static,
{
    /// Evaluate the predicate against one indexed event.
    #[must_use]
    pub fn evaluate(&self, index: usize, event: &TestEvent<PortType>) -> bool {
        (self.0)((index, event))
    }
}

impl<F, PortType> From<F> for EventPredicate<PortType>
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: Send + Sync + 'static,
    PortType::Request: Send + Sync + 'static,
    F: Fn((usize, &TestEvent<PortType>)) -> bool + Send + 'static,
{
    fn from(value: F) -> Self {
        Self(Box::new(value))
    }
}

impl<PortType> fmt::Debug for EventPredicate<PortType>
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: Send + Sync + 'static,
    PortType::Request: Send + Sync + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<event predicate>")
    }
}

/// A request to observe matching events at or after one event-log index.
///
/// `start_index` is inclusive. Events with lower indexes are skipped before
/// the predicate is evaluated, so callers can use an observation result as a
/// cursor without baking index checks into the predicate itself.
pub struct EventObservationPredicate<PortType>
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: Send + Sync + 'static,
    PortType::Request: Send + Sync + 'static,
{
    /// First event-log index considered by this observation.
    start_index: usize,
    /// Predicate used to select a matching event once the index is in range.
    predicate: EventPredicate<PortType>,
}

impl<PortType> EventObservationPredicate<PortType>
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: Send + Sync + 'static,
    PortType::Request: Send + Sync + 'static,
{
    /// Create an observation that evaluates `predicate` from `start_index`.
    #[must_use]
    pub fn new(start_index: usize, predicate: EventPredicate<PortType>) -> Self {
        Self {
            start_index,
            predicate,
        }
    }

    /// Return the first event-log index this observation considers.
    #[must_use]
    pub fn start_index(&self) -> usize {
        self.start_index
    }

    /// Evaluate this observation against one indexed event.
    ///
    /// The predicate is only evaluated when `index` is at or after
    /// [`Self::start_index`].
    #[must_use]
    pub fn evaluate(&self, index: usize, event: &TestEvent<PortType>) -> bool {
        index >= self.start_index && self.predicate.evaluate(index, event)
    }
}

impl<PortType> fmt::Debug for EventObservationPredicate<PortType>
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: Send + Sync + 'static,
    PortType::Request: Send + Sync + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventObservation")
            .field("start_index", &self.start_index)
            .field("predicate", &self.predicate)
            .finish()
    }
}

/// One event selected from a [`PortTesterComponent`] event log.
///
/// The event is stored behind an [`Arc`] so several pending observations can
/// resolve to the same logged event without cloning the port payload.
pub struct ObservedEvent<PortType>
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: Send + Sync + 'static,
    PortType::Request: Send + Sync + 'static,
{
    /// The position of the observed event in the tester's event log.
    index: usize,
    /// The observed event itself.
    event: Arc<TestEvent<PortType>>,
}

impl<PortType> ObservedEvent<PortType>
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: Send + Sync + 'static,
    PortType::Request: Send + Sync + 'static,
{
    /// Return the zero-based position of this event in the tester's log.
    #[must_use]
    pub fn index(&self) -> usize {
        self.index
    }

    /// Borrow the observed event payload.
    #[must_use]
    pub fn event(&self) -> &TestEvent<PortType> {
        self.event.as_ref()
    }

    /// Borrow the shared event handle.
    #[must_use]
    pub fn shared_event(&self) -> &Arc<TestEvent<PortType>> {
        &self.event
    }

    /// Consume this observation and return the shared event handle.
    #[must_use]
    pub fn into_shared_event(self) -> Arc<TestEvent<PortType>> {
        self.event
    }
}

impl<PortType> ObservedEvent<PortType>
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: fmt::Debug + Send + Sync + 'static,
    PortType::Request: fmt::Debug + Send + Sync + 'static,
{
    /// Returns the indication carried by the observed event.
    ///
    /// # Panics
    ///
    /// Panics when the observed event is a request.
    #[must_use]
    pub fn unwrap_indication(&self) -> &PortType::Indication {
        self.event().unwrap_indication()
    }

    /// Returns the request carried by the observed event.
    ///
    /// # Panics
    ///
    /// Panics when the observed event is an indication.
    #[must_use]
    pub fn unwrap_request(&self) -> &PortType::Request {
        self.event().unwrap_request()
    }
}

impl<PortType> fmt::Debug for ObservedEvent<PortType>
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: fmt::Debug + Send + Sync + 'static,
    PortType::Request: fmt::Debug + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObservedEvent")
            .field("index", &self.index)
            .field("event", &self.event)
            .finish()
    }
}

/// Actor messages understood by [`PortTesterComponent`].
pub enum PortTestMsg<PortType>
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: Send + Sync + 'static,
    PortType::Request: Send + Sync + 'static,
{
    /// Resolve the ask with the first event, in observed order, matching the
    /// supplied observation request.
    Observe(Ask<EventObservationPredicate<PortType>, ObservedEvent<PortType>>),
    /// Trigger the given event through the tester's connected ports.
    Inject(TestEvent<PortType>),
}

impl<PortType> PortTestMsg<PortType>
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: Send + Sync + 'static,
    PortType::Request: Send + Sync + 'static,
{
    /// Build a message that injects `request` through the tester's required port.
    ///
    /// When the tester is connected to a component under test, this makes the
    /// component observe the request on its provided port.
    #[must_use]
    pub fn inject_request(request: PortType::Request) -> Self {
        Self::Inject(TestEvent::Request(request))
    }

    /// Build a message that injects `indication` through the tester's provided port.
    ///
    /// When the tester is connected to a component under test, this makes the
    /// component observe the indication on its required port.
    #[must_use]
    pub fn inject_indication(indication: PortType::Indication) -> Self {
        Self::Inject(TestEvent::Indication(indication))
    }

    /// Build a message and future for observing the first matching event.
    ///
    /// The tester first searches its existing event log. If nothing matches, it
    /// stores the observation and resolves the future when a future event
    /// matches `predicate`.
    pub fn observe<F>(predicate: F) -> (Self, KFuture<ObservedEvent<PortType>>)
    where
        F: Fn((usize, &TestEvent<PortType>)) -> bool + Send + 'static,
    {
        Self::observe_from(0, predicate)
    }

    /// Build an observation message and future starting at `start_index`.
    pub fn observe_from<F>(
        start_index: usize,
        predicate: F,
    ) -> (Self, KFuture<ObservedEvent<PortType>>)
    where
        F: Fn((usize, &TestEvent<PortType>)) -> bool + Send + 'static,
    {
        let boxed_predicate = EventPredicate::from(predicate);
        let observation = EventObservationPredicate::new(start_index, boxed_predicate);
        let (promise, future) = promise();
        let ask = Ask::new(promise, observation);
        (Self::Observe(ask), future)
    }

    /// Build a message and future for observing the first matching request.
    pub fn observe_request<F>(predicate: F) -> (Self, KFuture<ObservedEvent<PortType>>)
    where
        F: Fn(&PortType::Request) -> bool + Send + 'static,
    {
        Self::observe(move |(_index, event)| match event {
            TestEvent::Request(request) => predicate(request),
            TestEvent::Indication(_) => false,
        })
    }

    /// Build a message and future for observing the first matching request
    /// at or after `start_index`.
    ///
    /// `start_index` is inclusive. Pass `0` to search from the beginning of the
    /// log, or pass one greater than [`ObservedEvent::index`] from a previous
    /// result to wait for the next matching event.
    pub fn observe_request_from<F>(
        start_index: usize,
        predicate: F,
    ) -> (Self, KFuture<ObservedEvent<PortType>>)
    where
        F: Fn(&PortType::Request) -> bool + Send + 'static,
    {
        Self::observe_from(start_index, move |(_index, event)| match event {
            TestEvent::Request(request) => predicate(request),
            TestEvent::Indication(_) => false,
        })
    }

    /// Build a message and future for observing the first matching indication.
    pub fn observe_indication<F>(predicate: F) -> (Self, KFuture<ObservedEvent<PortType>>)
    where
        F: Fn(&PortType::Indication) -> bool + Send + 'static,
    {
        Self::observe(move |(_index, event)| match event {
            TestEvent::Request(_) => false,
            TestEvent::Indication(indication) => predicate(indication),
        })
    }

    /// Build a message and future for observing the first matching indication
    /// at or after `start_index`.
    ///
    /// `start_index` is inclusive. Pass `0` to search from the beginning of the
    /// log, or pass one greater than [`ObservedEvent::index`] from a previous
    /// result to wait for the next matching event.
    pub fn observe_indication_from<F>(
        start_index: usize,
        predicate: F,
    ) -> (Self, KFuture<ObservedEvent<PortType>>)
    where
        F: Fn(&PortType::Indication) -> bool + Send + 'static,
    {
        Self::observe_from(start_index, move |(_index, event)| match event {
            TestEvent::Request(_) => false,
            TestEvent::Indication(indication) => predicate(indication),
        })
    }
}

impl<PortType> fmt::Debug for PortTestMsg<PortType>
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: fmt::Debug + Send + Sync + 'static,
    PortType::Request: fmt::Debug + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Observe(arg0) => f.debug_tuple("Observe").field(arg0).finish(),
            Self::Inject(arg0) => f.debug_tuple("Inject").field(arg0).finish(),
        }
    }
}

/// A component for testing the event flow along a channel of type `PortType`.
///
/// In sidecar mode, the tester passively observes events delivered through its
/// connected ports and records them in an event log. It does not trigger the
/// opposite port, so it can be attached to an existing connection without
/// changing the normal event path.
///
/// In forwarding mode, the tester records each event and then triggers the
/// opposite port with a clone of the payload. This lets tests place the tester
/// inline between two components without directly connecting those components
/// to each other.
#[derive(ComponentDefinition)]
pub struct PortTesterComponent<PortType>
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: Send + Sync + 'static,
    PortType::Request: Send + Sync + 'static,
{
    ctx: ComponentContext<Self>,
    port_provided: ProvidedPort<PortType>,
    port_required: RequiredPort<PortType>,
    event_log: Vec<Arc<TestEvent<PortType>>>,
    pending_observations: Vec<Ask<EventObservationPredicate<PortType>, ObservedEvent<PortType>>>,
    forward_events: bool,
}

impl<PortType> PortTesterComponent<PortType>
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: Send + Sync + 'static,
    PortType::Request: Send + Sync + 'static,
{
    /// Create a new tester component.
    ///
    /// Set `forward_events` to `false` for sidecar observation and `true` for
    /// inline forwarding.
    #[must_use]
    pub fn new(forward_events: bool) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            port_provided: ProvidedPort::uninitialised(),
            port_required: RequiredPort::uninitialised(),
            event_log: Vec::new(),
            pending_observations: Vec::new(),
            forward_events,
        }
    }

    fn check_pending_observations(
        &mut self,
        index: usize,
        observed_event: &Arc<TestEvent<PortType>>,
    ) {
        let mut remaining = Vec::with_capacity(self.pending_observations.len());
        let mut had_error = false;
        for ask in self.pending_observations.drain(..) {
            let observation = ask.request();
            if observation.evaluate(index, observed_event.as_ref()) {
                let res = ask.reply(ObservedEvent {
                    index,
                    event: Arc::clone(observed_event),
                });
                had_error |= res.is_err();
            } else {
                remaining.push(ask);
            }
        }
        self.pending_observations = remaining;
        if had_error {
            warn!(
                self.log(),
                "Some listener dropped a pending ask that matched {observed_event:?}. Listeners should wait for their events to be observed!"
            );
        }
    }
}

impl<PortType> Provide<PortType> for PortTesterComponent<PortType>
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: Send + Sync + 'static,
    PortType::Request: Send + Sync + 'static,
{
    fn handle(&mut self, event: PortType::Request) -> HandlerResult {
        let last_event = if self.forward_events {
            let arc_event = Arc::new(TestEvent::Request(event.clone()));
            self.port_required.trigger(event);
            arc_event
        } else {
            // Avoid cloning the underlying event again if forwarding isn't necessary.
            Arc::new(TestEvent::Request(event))
        };

        // Do the checks before pushing, so we don't need to clone the Arc.
        let last_index = self.event_log.len();
        self.check_pending_observations(last_index, &last_event);

        self.event_log.push(last_event);
        Handled::OK
    }
}

impl<PortType> Require<PortType> for PortTesterComponent<PortType>
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: Send + Sync + 'static,
    PortType::Request: Send + Sync + 'static,
{
    fn handle(&mut self, event: PortType::Indication) -> HandlerResult {
        let last_event = if self.forward_events {
            let arc_event = Arc::new(TestEvent::Indication(event.clone()));
            self.port_provided.trigger(event);
            arc_event
        } else {
            // Avoid cloning the underlying event again if forwarding isn't necessary.
            Arc::new(TestEvent::Indication(event))
        };

        // Do the checks before pushing, so we don't need to clone the Arc.
        let last_index = self.event_log.len();
        self.check_pending_observations(last_index, &last_event);

        self.event_log.push(last_event);
        Handled::OK
    }
}

impl<PortType> Actor for PortTesterComponent<PortType>
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: Send + Sync + 'static,
    PortType::Request: Send + Sync + 'static,
{
    type Message = PortTestMsg<PortType>;

    fn receive_local(&mut self, msg: Self::Message) -> HandlerResult {
        match msg {
            PortTestMsg::Observe(ask) => {
                let observation = ask.request();
                for (index, observed_event) in self
                    .event_log
                    .iter()
                    .enumerate()
                    .skip(observation.start_index())
                {
                    if observation.evaluate(index, observed_event.as_ref()) {
                        ask.reply(ObservedEvent {
                            index,
                            event: Arc::clone(observed_event),
                        })
                        .benign_err()?;
                        return Handled::OK;
                    }
                }
                self.pending_observations.push(ask);
                Handled::OK
            }
            PortTestMsg::Inject(test_event) => match test_event {
                TestEvent::Request(r) => {
                    self.port_required.trigger(r);
                    Handled::OK
                }
                TestEvent::Indication(i) => {
                    self.port_provided.trigger(i);
                    Handled::OK
                }
            },
        }
    }
}

// Cannot use the macro due to the type parameter.
impl<PortType> ComponentLifecycle for PortTesterComponent<PortType>
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: Send + Sync + 'static,
    PortType::Request: Send + Sync + 'static,
{
}

/// Extension trait for creating tester components from a port type.
pub trait PortTestingExt: Port + Sized + Send + Sync + 'static
where
    Self::Indication: Send + Sync + 'static,
    Self::Request: Send + Sync + 'static,
{
    /// Create a passive sidecar tester for this port type.
    #[must_use]
    fn tester_component_sidecar() -> PortTesterComponent<Self>;

    /// Create an inline forwarding tester for this port type.
    #[must_use]
    fn tester_component_forwarding() -> PortTesterComponent<Self>;
}

impl<P> PortTestingExt for P
where
    P: Port + Sized + Send + Sync + 'static,
    P::Indication: Send + Sync + 'static,
    P::Request: Send + Sync + 'static,
{
    fn tester_component_sidecar() -> PortTesterComponent<Self> {
        PortTesterComponent::new(false)
    }

    fn tester_component_forwarding() -> PortTesterComponent<Self> {
        PortTesterComponent::new(true)
    }
}

/// Extension trait for controlling a [`PortTesterComponent`] through its actor reference.
pub trait PortTestingRefExt<PortType>
where
    PortType: Port + Sized + Send + Sync + 'static,
    PortType::Indication: Send + Sync + 'static,
    PortType::Request: Send + Sync + 'static,
{
    /// Inject `request` through the tester's required port.
    ///
    /// When connected to a component under test, this makes the component
    /// observe the request on its provided port.
    fn inject_request(&self, request: PortType::Request);

    /// Inject `indication` through the tester's provided port.
    ///
    /// When connected to a component under test, this makes the component
    /// observe the indication on its required port.
    fn inject_indication(&self, indication: PortType::Indication);

    /// Wait until the tester has observed an event matching `predicate`.
    ///
    /// Existing logged events are checked before future events. The returned
    /// future resolves to the first match in observed order.
    fn observe<F>(&self, predicate: F) -> KFuture<ObservedEvent<PortType>>
    where
        F: Fn((usize, &TestEvent<PortType>)) -> bool + Send + 'static;

    /// Wait until the tester has observed a matching request.
    fn observe_request<F>(&self, predicate: F) -> KFuture<ObservedEvent<PortType>>
    where
        F: Fn(&PortType::Request) -> bool + Send + 'static;

    /// Wait until the tester has observed a matching request at or after `start_index`.
    fn observe_request_from<F>(
        &self,
        start_index: usize,
        predicate: F,
    ) -> KFuture<ObservedEvent<PortType>>
    where
        F: Fn(&PortType::Request) -> bool + Send + 'static;

    /// Wait until the tester has observed a matching indication.
    fn observe_indication<F>(&self, predicate: F) -> KFuture<ObservedEvent<PortType>>
    where
        F: Fn(&PortType::Indication) -> bool + Send + 'static;

    /// Wait until the tester has observed a matching indication at or after `start_index`.
    fn observe_indication_from<F>(
        &self,
        start_index: usize,
        predicate: F,
    ) -> KFuture<ObservedEvent<PortType>>
    where
        F: Fn(&PortType::Indication) -> bool + Send + 'static;
}

impl<P> PortTestingRefExt<P> for ActorRef<PortTestMsg<P>>
where
    P: Port + Sized + Send + Sync + 'static,
    P::Indication: Send + Sync + 'static,
    P::Request: Send + Sync + 'static,
{
    fn inject_request(&self, request: <P as Port>::Request) {
        self.tell(PortTestMsg::inject_request(request));
    }

    fn inject_indication(&self, indication: <P as Port>::Indication) {
        self.tell(PortTestMsg::inject_indication(indication));
    }

    fn observe<F>(&self, predicate: F) -> KFuture<ObservedEvent<P>>
    where
        F: Fn((usize, &TestEvent<P>)) -> bool + Send + 'static,
    {
        let (msg, future) = PortTestMsg::observe(predicate);
        self.tell(msg);
        future
    }

    fn observe_request<F>(&self, predicate: F) -> KFuture<ObservedEvent<P>>
    where
        F: Fn(&<P as Port>::Request) -> bool + Send + 'static,
    {
        let (msg, future) = PortTestMsg::observe_request(predicate);
        self.tell(msg);
        future
    }

    fn observe_request_from<F>(&self, start_index: usize, predicate: F) -> KFuture<ObservedEvent<P>>
    where
        F: Fn(&<P as Port>::Request) -> bool + Send + 'static,
    {
        let (msg, future) = PortTestMsg::observe_request_from(start_index, predicate);
        self.tell(msg);
        future
    }

    fn observe_indication<F>(&self, predicate: F) -> KFuture<ObservedEvent<P>>
    where
        F: Fn(&<P as Port>::Indication) -> bool + Send + 'static,
    {
        let (msg, future) = PortTestMsg::observe_indication(predicate);
        self.tell(msg);
        future
    }

    fn observe_indication_from<F>(
        &self,
        start_index: usize,
        predicate: F,
    ) -> KFuture<ObservedEvent<P>>
    where
        F: Fn(&<P as Port>::Indication) -> bool + Send + 'static,
    {
        let (msg, future) = PortTestMsg::observe_indication_from(start_index, predicate);
        self.tell(msg);
        future
    }
}

type BoxedEventPredicate<PortType> =
    Box<dyn Fn((usize, &TestEvent<PortType>)) -> bool + Send + 'static>;

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use kompact::test_support::test_kompact_config;

    use super::*;

    struct TestPort;
    impl Port for TestPort {
        type Indication = String;
        type Request = String;
    }

    const TEST_TIMEOUT: Duration = Duration::from_secs(1);

    fn start_component<C>(system: &KompactSystem, component: &Arc<Component<C>>)
    where
        C: ComponentDefinition + ComponentLifecycle + Sized + 'static,
    {
        system
            .start_notify(component)
            .wait_timeout(TEST_TIMEOUT)
            .expect("component start");
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

        let matching_event = matching_event_f.wait_timeout(TEST_TIMEOUT).unwrap();
        assert_eq!(matching_event.index(), 1);
        assert_eq!(matching_event.unwrap_indication(), "Second message");

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
            .unwrap();
        assert_eq!(first.index(), 0);
        assert_eq!(first.unwrap_request(), "Repeated request");

        let second_f = provider_ref
            .observe_request_from(first.index() + 1, |event| event == "Repeated request");

        requirer_ref.inject_request("Noise request".to_owned());
        requirer_ref.inject_request("Repeated request".to_owned());

        let second = second_f.wait_timeout(TEST_TIMEOUT).unwrap();
        assert_eq!(second.index(), 2);
        assert_eq!(second.unwrap_request(), "Repeated request");

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

        let proxy_event = proxy_f.wait_timeout(TEST_TIMEOUT).unwrap();
        let sink_event = sink_f.wait_timeout(TEST_TIMEOUT).unwrap();
        assert_eq!(proxy_event.unwrap_indication(), "Forwarded indication");
        assert_eq!(sink_event.unwrap_indication(), "Forwarded indication");

        let proxy_f = proxy_ref.observe_request(|event| event == "Forwarded request");
        let source_f = source_ref.observe_request(|event| event == "Forwarded request");

        sink_ref.inject_request("Forwarded request".to_owned());

        let proxy_event = proxy_f.wait_timeout(TEST_TIMEOUT).unwrap();
        let source_event = source_f.wait_timeout(TEST_TIMEOUT).unwrap();
        assert_eq!(proxy_event.unwrap_request(), "Forwarded request");
        assert_eq!(source_event.unwrap_request(), "Forwarded request");

        system.shutdown().wait().expect("shutdown");
    }
}
