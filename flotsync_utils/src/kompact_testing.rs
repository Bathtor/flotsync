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
//!
//! Observation results carry their zero-based event-log index. Use a positive
//! observation as the synchronisation point before extracting the complete log
//! from the tester at the end of a test.
//!
//! The tester intentionally exposes cursor-based observation before bulk log
//! export through actor messages. If a downstream test needs the complete event
//! sequence at the end of the test, extract it from the component definition
//! with [`PortTesterComponent::event_log_snapshot`] before shutting the system
//! down.
//!
//! # Example
//!
//! ```rust
//! # use std::time::Duration;
//! # use kompact::prelude::*;
//! # use flotsync_utils::kompact_testing::{PortTestingExt, PortTestingRefExt, TestEvent};
//! # struct TestPort;
//! # impl Port for TestPort {
//! #     type Indication = String;
//! #     type Request = String;
//! # }
//! # let system = KompactConfig::default().build().wait().expect("system");
//! # let source = system.create(TestPort::tester_component_sidecar);
//! # let observer = system.create(TestPort::tester_component_sidecar);
//! # biconnect_components(&source, &observer).expect("connection");
//! # system
//! #     .start_notify(&source)
//! #     .wait_timeout(Duration::from_secs(1))
//! #     .expect("source start");
//! # system
//! #     .start_notify(&observer)
//! #     .wait_timeout(Duration::from_secs(1))
//! #     .expect("observer start");
//! let source_ref = source.actor_ref();
//! let observer_ref = observer.actor_ref();
//! let final_event = observer_ref.observe_indication(|event| event == "second");
//!
//! source_ref.inject_indication("first".to_owned());
//! source_ref.inject_indication("second".to_owned());
//!
//! final_event
//!     .wait_timeout(Duration::from_secs(1))
//!     .expect("final event before timeout")
//!     .expect("tester promise still live");
//!
//! let log = observer.on_definition(|component| component.event_log_snapshot());
//! assert_eq!(log.len(), 2);
//! assert!(matches!(log[0].as_ref(), TestEvent::Indication(value) if value == "first"));
//! assert!(matches!(log[1].as_ref(), TestEvent::Indication(value) if value == "second"));
//! # system.shutdown().wait().expect("shutdown");
//! ```

use crate::NonOwningPhantomData;
use futures_util::FutureExt as _;
use kompact::prelude::*;
use snafu::{ResultExt as _, Whatever};
use std::{
    fmt,
    marker::PhantomData,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

const EVENTUALLY_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Future returned by typed observation helpers.
///
/// The wrapped future resolves to `Err` only when the tester-side promise is
/// dropped before completion. Synchronous tests can use Kompact's
/// [`BlockingFutureExt::wait_timeout`] on this type.
pub type ObservedFuture<T> = crate::BoxFuture<'static, Result<T, Whatever>>;

/// One request selected from a [`PortTesterComponent`] event log.
pub type ObservedRequest<PortType> = ObservedEvent<PortType, RequestEvent>;

/// One indication selected from a [`PortTesterComponent`] event log.
pub type ObservedIndication<PortType> = ObservedEvent<PortType, IndicationEvent>;

/// Wait until `predicate` becomes true or `timeout` elapses.
///
/// # Panics
///
/// Panics with `failure_message` if the predicate does not become true before the timeout.
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

/// A request to fail if a matching event is observed during a time window.
///
/// When `start_index` is [`None`], the window starts from the end of the event
/// log at the point where the tester component processes the request. Existing
/// earlier events are ignored. When `start_index` is [`Some`], the value is
/// inclusive and matching existing log entries at or after that index fail the
/// request immediately.
pub struct EventAbsencePredicate<PortType>
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: Send + Sync + 'static,
    PortType::Request: Send + Sync + 'static,
{
    /// First event-log index considered by this absence check, if fixed by the caller.
    start_index: Option<usize>,
    /// Length of the no-match window.
    duration: Duration,
    /// Predicate that selects events which should fail the check.
    predicate: EventPredicate<PortType>,
}

impl<PortType> EventAbsencePredicate<PortType>
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: Send + Sync + 'static,
    PortType::Request: Send + Sync + 'static,
{
    /// Create an absence check over a future window.
    ///
    /// Existing log entries are ignored. The effective start index is the
    /// current log length when the tester processes the request.
    #[must_use]
    pub fn new(duration: Duration, predicate: EventPredicate<PortType>) -> Self {
        Self {
            start_index: None,
            duration,
            predicate,
        }
    }

    /// Create an absence check that starts at an inclusive event-log index.
    ///
    /// Existing log entries at or after `start_index` are checked immediately
    /// before the future window is scheduled.
    #[must_use]
    pub fn from_index(
        start_index: usize,
        duration: Duration,
        predicate: EventPredicate<PortType>,
    ) -> Self {
        Self {
            start_index: Some(start_index),
            duration,
            predicate,
        }
    }

    /// Return the fixed start index, if the caller supplied one.
    #[must_use]
    pub fn start_index(&self) -> Option<usize> {
        self.start_index
    }

    /// Return the length of the no-match window.
    #[must_use]
    pub fn duration(&self) -> Duration {
        self.duration
    }

    /// Resolve the effective start index for this request.
    #[must_use]
    pub fn effective_start_index(&self, current_log_len: usize) -> usize {
        self.start_index.unwrap_or(current_log_len)
    }

    /// Evaluate this absence check against one indexed event.
    #[must_use]
    pub fn evaluate_from(
        &self,
        start_index: usize,
        index: usize,
        event: &TestEvent<PortType>,
    ) -> bool {
        index >= start_index && self.predicate.evaluate(index, event)
    }
}

impl<PortType> fmt::Debug for EventAbsencePredicate<PortType>
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: Send + Sync + 'static,
    PortType::Request: Send + Sync + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventAbsence")
            .field("start_index", &self.start_index)
            .field("duration", &self.duration)
            .field("predicate", &self.predicate)
            .finish()
    }
}

/// View marker for an observation that may contain either port event direction.
pub struct AnyEvent;

/// View marker for an observation known to contain a request.
pub struct RequestEvent;

/// View marker for an observation known to contain an indication.
pub struct IndicationEvent;

/// One event selected from a [`PortTesterComponent`] event log.
///
/// The event is stored behind an [`Arc`] so several pending observations can
/// resolve to the same logged event without cloning the port payload. The
/// `EventView` parameter narrows what [`Self::event`] returns for helper APIs
/// that already know which side of the port matched.
pub struct ObservedEvent<PortType, EventView = AnyEvent>
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: Send + Sync + 'static,
    PortType::Request: Send + Sync + 'static,
{
    /// The position of the observed event in the tester's event log.
    index: usize,
    /// The observed event itself.
    event: Arc<TestEvent<PortType>>,
    /// Marker selecting the type returned by [`Self::event`].
    event_view: NonOwningPhantomData<EventView>,
}

impl<PortType, EventView> ObservedEvent<PortType, EventView>
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: Send + Sync + 'static,
    PortType::Request: Send + Sync + 'static,
{
    /// Build an observation for one event-log entry.
    fn new(index: usize, event: Arc<TestEvent<PortType>>) -> Self {
        Self {
            index,
            event,
            event_view: PhantomData,
        }
    }

    /// Reinterpret an already type-checked observation through a narrower event view.
    fn into_view<NewView>(self) -> ObservedEvent<PortType, NewView> {
        ObservedEvent {
            index: self.index,
            event: self.event,
            event_view: PhantomData,
        }
    }

    /// Return the zero-based position of this event in the tester's log.
    #[must_use]
    pub fn index(&self) -> usize {
        self.index
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

impl<PortType> ObservedEvent<PortType, AnyEvent>
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: Send + Sync + 'static,
    PortType::Request: Send + Sync + 'static,
{
    /// Borrow the observed event payload.
    #[must_use]
    pub fn event(&self) -> &TestEvent<PortType> {
        self.event.as_ref()
    }

    /// Narrow this observation to the request view.
    ///
    /// # Panics
    ///
    /// Panics in debug builds when the observed event is not a request.
    #[must_use]
    pub fn into_request(self) -> ObservedRequest<PortType> {
        debug_assert!(matches!(self.event(), TestEvent::Request(_)));
        self.into_view()
    }

    /// Narrow this observation to the indication view.
    ///
    /// # Panics
    ///
    /// Panics in debug builds when the observed event is not an indication.
    #[must_use]
    pub fn into_indication(self) -> ObservedIndication<PortType> {
        debug_assert!(matches!(self.event(), TestEvent::Indication(_)));
        self.into_view()
    }
}

impl<PortType> ObservedEvent<PortType, AnyEvent>
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

impl<PortType> ObservedEvent<PortType, RequestEvent>
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: Send + Sync + 'static,
    PortType::Request: Send + Sync + 'static,
{
    /// Borrow the observed request payload.
    #[must_use]
    pub fn event(&self) -> &PortType::Request {
        match self.event.as_ref() {
            TestEvent::Request(request) => request,
            TestEvent::Indication(_) => {
                unreachable!("ObservedRequest must contain a request event");
            }
        }
    }

    /// Borrow the observed request payload.
    #[must_use]
    pub fn request(&self) -> &PortType::Request {
        self.event()
    }
}

impl<PortType> ObservedEvent<PortType, IndicationEvent>
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: Send + Sync + 'static,
    PortType::Request: Send + Sync + 'static,
{
    /// Borrow the observed indication payload.
    #[must_use]
    pub fn event(&self) -> &PortType::Indication {
        match self.event.as_ref() {
            TestEvent::Request(_) => {
                unreachable!("ObservedIndication must contain an indication event");
            }
            TestEvent::Indication(indication) => indication,
        }
    }

    /// Borrow the observed indication payload.
    #[must_use]
    pub fn indication(&self) -> &PortType::Indication {
        self.event()
    }
}

impl<PortType, EventView> fmt::Debug for ObservedEvent<PortType, EventView>
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: fmt::Debug + Send + Sync + 'static,
    PortType::Request: fmt::Debug + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObservedEvent")
            .field("index", &self.index)
            .field("event", self.event.as_ref())
            .finish_non_exhaustive()
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
    /// Resolve the ask with `Err` if a matching event appears before the
    /// absence window completes, or `Ok(())` if the window expires without a
    /// match.
    FailIfObserved(Ask<EventAbsencePredicate<PortType>, Result<(), ObservedEvent<PortType>>>),
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

    /// Build a message and typed future for observing the first matching request.
    pub fn observe_request<F>(predicate: F) -> (Self, ObservedFuture<ObservedRequest<PortType>>)
    where
        F: Fn(&PortType::Request) -> bool + Send + 'static,
    {
        let (msg, future) = Self::observe(move |(_index, event)| match event {
            TestEvent::Request(request) => predicate(request),
            TestEvent::Indication(_) => false,
        });
        let typed_future = Self::map_observed_future(future, ObservedEvent::into_request);
        (msg, typed_future)
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
    ) -> (Self, ObservedFuture<ObservedRequest<PortType>>)
    where
        F: Fn(&PortType::Request) -> bool + Send + 'static,
    {
        let (msg, future) = Self::observe_from(start_index, move |(_index, event)| match event {
            TestEvent::Request(request) => predicate(request),
            TestEvent::Indication(_) => false,
        });
        let typed_future = Self::map_observed_future(future, ObservedEvent::into_request);
        (msg, typed_future)
    }

    /// Build a message and typed future for observing the first matching indication.
    pub fn observe_indication<F>(
        predicate: F,
    ) -> (Self, ObservedFuture<ObservedIndication<PortType>>)
    where
        F: Fn(&PortType::Indication) -> bool + Send + 'static,
    {
        let (msg, future) = Self::observe(move |(_index, event)| match event {
            TestEvent::Request(_) => false,
            TestEvent::Indication(indication) => predicate(indication),
        });
        let typed_future = Self::map_observed_future(future, ObservedEvent::into_indication);
        (msg, typed_future)
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
    ) -> (Self, ObservedFuture<ObservedIndication<PortType>>)
    where
        F: Fn(&PortType::Indication) -> bool + Send + 'static,
    {
        let (msg, future) = Self::observe_from(start_index, move |(_index, event)| match event {
            TestEvent::Request(_) => false,
            TestEvent::Indication(indication) => predicate(indication),
        });
        let typed_future = Self::map_observed_future(future, ObservedEvent::into_indication);
        (msg, typed_future)
    }

    /// Build a message and future that fails if a matching event is observed
    /// during `duration`.
    ///
    /// Existing log entries are ignored. Use
    /// [`Self::fail_if_observed_from`] to include previously logged
    /// events from a known cursor.
    pub fn fail_if_observed<F>(
        duration: Duration,
        predicate: F,
    ) -> (Self, KFuture<Result<(), ObservedEvent<PortType>>>)
    where
        F: Fn((usize, &TestEvent<PortType>)) -> bool + Send + 'static,
    {
        let boxed_predicate = EventPredicate::from(predicate);
        let absence = EventAbsencePredicate::new(duration, boxed_predicate);
        let (promise, future) = promise();
        let ask = Ask::new(promise, absence);
        (Self::FailIfObserved(ask), future)
    }

    /// Build a message and future that fails if a matching event is observed
    /// at or after `start_index` during `duration`.
    ///
    /// `start_index` is inclusive. Existing matching log entries at or after
    /// this index fail the returned future immediately when the tester
    /// processes the message.
    pub fn fail_if_observed_from<F>(
        start_index: usize,
        duration: Duration,
        predicate: F,
    ) -> (Self, KFuture<Result<(), ObservedEvent<PortType>>>)
    where
        F: Fn((usize, &TestEvent<PortType>)) -> bool + Send + 'static,
    {
        let boxed_predicate = EventPredicate::from(predicate);
        let absence = EventAbsencePredicate::from_index(start_index, duration, boxed_predicate);
        let (promise, future) = promise();
        let ask = Ask::new(promise, absence);
        (Self::FailIfObserved(ask), future)
    }

    /// Build a message and typed future that fails if a matching request is
    /// observed during `duration`.
    pub fn fail_if_request_observed<F>(
        duration: Duration,
        predicate: F,
    ) -> (Self, ObservedFuture<Result<(), ObservedRequest<PortType>>>)
    where
        F: Fn(&PortType::Request) -> bool + Send + 'static,
    {
        let (msg, future) = Self::fail_if_observed(duration, move |(_index, event)| match event {
            TestEvent::Request(request) => predicate(request),
            TestEvent::Indication(_) => false,
        });
        let typed_future = Self::map_absence_future(future, ObservedEvent::into_request);
        (msg, typed_future)
    }

    /// Build a message and typed future that fails if a matching request is
    /// observed at or after `start_index` during `duration`.
    pub fn fail_if_request_observed_from<F>(
        start_index: usize,
        duration: Duration,
        predicate: F,
    ) -> (Self, ObservedFuture<Result<(), ObservedRequest<PortType>>>)
    where
        F: Fn(&PortType::Request) -> bool + Send + 'static,
    {
        let (msg, future) = Self::fail_if_observed_from(
            start_index,
            duration,
            move |(_index, event)| match event {
                TestEvent::Request(request) => predicate(request),
                TestEvent::Indication(_) => false,
            },
        );
        let typed_future = Self::map_absence_future(future, ObservedEvent::into_request);
        (msg, typed_future)
    }

    /// Build a message and typed future that fails if a matching indication is
    /// observed during `duration`.
    pub fn fail_if_indication_observed<F>(
        duration: Duration,
        predicate: F,
    ) -> (
        Self,
        ObservedFuture<Result<(), ObservedIndication<PortType>>>,
    )
    where
        F: Fn(&PortType::Indication) -> bool + Send + 'static,
    {
        let (msg, future) = Self::fail_if_observed(duration, move |(_index, event)| match event {
            TestEvent::Request(_) => false,
            TestEvent::Indication(indication) => predicate(indication),
        });
        let typed_future = Self::map_absence_future(future, ObservedEvent::into_indication);
        (msg, typed_future)
    }

    /// Build a message and typed future that fails if a matching indication is
    /// observed at or after `start_index` during `duration`.
    pub fn fail_if_indication_observed_from<F>(
        start_index: usize,
        duration: Duration,
        predicate: F,
    ) -> (
        Self,
        ObservedFuture<Result<(), ObservedIndication<PortType>>>,
    )
    where
        F: Fn(&PortType::Indication) -> bool + Send + 'static,
    {
        let (msg, future) = Self::fail_if_observed_from(
            start_index,
            duration,
            move |(_index, event)| match event {
                TestEvent::Request(_) => false,
                TestEvent::Indication(indication) => predicate(indication),
            },
        );
        let typed_future = Self::map_absence_future(future, ObservedEvent::into_indication);
        (msg, typed_future)
    }

    /// Convert a raw `KFuture` observation into a typed boxed future.
    fn map_observed_future<T, F>(
        future: KFuture<ObservedEvent<PortType>>,
        mapper: F,
    ) -> ObservedFuture<T>
    where
        T: Send + 'static,
        F: FnOnce(ObservedEvent<PortType>) -> T + Send + 'static,
    {
        async move {
            let observed_event = future
                .await
                .whatever_context("tester promise dropped before observation completed")?;
            Ok(mapper(observed_event))
        }
        .boxed()
    }

    /// Convert a raw absence-check future into a typed boxed future.
    fn map_absence_future<T, F>(
        future: KFuture<Result<(), ObservedEvent<PortType>>>,
        mapper: F,
    ) -> ObservedFuture<Result<(), T>>
    where
        T: Send + 'static,
        F: FnOnce(ObservedEvent<PortType>) -> T + Send + 'static,
    {
        async move {
            let result = future
                .await
                .whatever_context("tester promise dropped before absence check completed")?;
            Ok(result.map_err(mapper))
        }
        .boxed()
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
            Self::FailIfObserved(arg0) => f.debug_tuple("FailIfObserved").field(arg0).finish(),
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
    // TODO(kompact#231): Index these by the scheduled timer once
    // `ScheduledTimer` exposes map-friendly key semantics.
    pending_absence_observations: Vec<PendingAbsenceObservation<PortType>>,
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
            pending_absence_observations: Vec::new(),
            forward_events,
        }
    }

    /// Return a snapshot of the currently observed event log.
    ///
    /// The returned vector preserves observed order and clones only the
    /// [`Arc`] handles, not the underlying port payloads.
    #[must_use]
    pub fn event_log_snapshot(&self) -> Vec<Arc<TestEvent<PortType>>> {
        self.event_log.clone()
    }

    /// Resolve pending positive observations against the newest event.
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
                let res = ask.reply(ObservedEvent::new(index, Arc::clone(observed_event)));
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

    /// Fail pending absence checks that match the newest event.
    fn check_pending_absence_observations(
        &mut self,
        index: usize,
        observed_event: &Arc<TestEvent<PortType>>,
    ) {
        let pending = std::mem::take(&mut self.pending_absence_observations);
        let mut remaining = Vec::with_capacity(pending.len());
        let mut had_error = false;
        for pending_absence in pending {
            let PendingAbsenceObservation {
                ask,
                start_index,
                timer,
            } = pending_absence;
            if ask
                .request()
                .evaluate_from(start_index, index, observed_event.as_ref())
            {
                let res = ask.reply(Err(ObservedEvent::new(index, Arc::clone(observed_event))));
                had_error |= res.is_err();
                self.cancel_timer(timer);
            } else {
                remaining.push(PendingAbsenceObservation {
                    ask,
                    start_index,
                    timer,
                });
            }
        }
        self.pending_absence_observations = remaining;
        if had_error {
            warn!(
                self.log(),
                "Some listener dropped a pending absence check that failed on {observed_event:?}. Listeners should wait for their events to be observed!"
            );
        }
    }

    /// Start an absence check by scanning eligible history and scheduling its expiry.
    fn begin_absence_observation(
        &mut self,
        ask: Ask<EventAbsencePredicate<PortType>, Result<(), ObservedEvent<PortType>>>,
    ) -> HandlerResult {
        let start_index = ask.request().effective_start_index(self.event_log.len());
        for (index, observed_event) in self.event_log.iter().enumerate().skip(start_index) {
            if ask
                .request()
                .evaluate_from(start_index, index, observed_event.as_ref())
            {
                ask.reply(Err(ObservedEvent::new(index, Arc::clone(observed_event))))
                    .benign_err()?;
                return Handled::OK;
            }
        }

        let duration = ask.request().duration();
        let timer = self.schedule_once(duration, |component, timer| {
            component.complete_absence_observation(&timer)
        });
        self.pending_absence_observations
            .push(PendingAbsenceObservation {
                ask,
                start_index,
                timer,
            });
        Handled::OK
    }

    /// Complete the absence check associated with an expired timer.
    fn complete_absence_observation(&mut self, timer: &ScheduledTimer) -> HandlerResult {
        let pending = std::mem::take(&mut self.pending_absence_observations);
        let mut remaining = Vec::with_capacity(pending.len());
        let mut completed = false;
        let mut had_error = false;
        for pending_absence in pending {
            let PendingAbsenceObservation {
                ask,
                start_index,
                timer: pending_timer,
            } = pending_absence;
            if &pending_timer == timer {
                completed = true;
                had_error |= ask.reply(Ok(())).is_err();
            } else {
                remaining.push(PendingAbsenceObservation {
                    ask,
                    start_index,
                    timer: pending_timer,
                });
            }
        }
        self.pending_absence_observations = remaining;
        if had_error {
            warn!(
                self.log(),
                "Some listener dropped a pending absence check before it completed"
            );
        }
        if !completed {
            debug!(
                self.log(),
                "Ignoring completed absence-check timer without pending observation: {timer:?}"
            );
        }
        Handled::OK
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
        self.check_pending_absence_observations(last_index, &last_event);

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
        self.check_pending_absence_observations(last_index, &last_event);

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
                        ask.reply(ObservedEvent::new(index, Arc::clone(observed_event)))
                            .benign_err()?;
                        return Handled::OK;
                    }
                }
                self.pending_observations.push(ask);
                Handled::OK
            }
            PortTestMsg::FailIfObserved(ask) => self.begin_absence_observation(ask),
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
    fn observe_request<F>(&self, predicate: F) -> ObservedFuture<ObservedRequest<PortType>>
    where
        F: Fn(&PortType::Request) -> bool + Send + 'static;

    /// Wait until the tester has observed a matching request at or after `start_index`.
    fn observe_request_from<F>(
        &self,
        start_index: usize,
        predicate: F,
    ) -> ObservedFuture<ObservedRequest<PortType>>
    where
        F: Fn(&PortType::Request) -> bool + Send + 'static;

    /// Wait until the tester has observed a matching indication.
    fn observe_indication<F>(&self, predicate: F) -> ObservedFuture<ObservedIndication<PortType>>
    where
        F: Fn(&PortType::Indication) -> bool + Send + 'static;

    /// Wait until the tester has observed a matching indication at or after `start_index`.
    fn observe_indication_from<F>(
        &self,
        start_index: usize,
        predicate: F,
    ) -> ObservedFuture<ObservedIndication<PortType>>
    where
        F: Fn(&PortType::Indication) -> bool + Send + 'static;

    /// Fail if the tester observes a matching event during `duration`.
    ///
    /// Existing log entries are ignored. Use
    /// [`Self::fail_if_observed_from`] when a check should include
    /// events from a known cursor.
    fn fail_if_observed<F>(
        &self,
        duration: Duration,
        predicate: F,
    ) -> KFuture<Result<(), ObservedEvent<PortType>>>
    where
        F: Fn((usize, &TestEvent<PortType>)) -> bool + Send + 'static;

    /// Fail if the tester observes a matching event at or after `start_index`
    /// during `duration`.
    fn fail_if_observed_from<F>(
        &self,
        start_index: usize,
        duration: Duration,
        predicate: F,
    ) -> KFuture<Result<(), ObservedEvent<PortType>>>
    where
        F: Fn((usize, &TestEvent<PortType>)) -> bool + Send + 'static;

    /// Fail if the tester observes a matching request during `duration`.
    fn fail_if_request_observed<F>(
        &self,
        duration: Duration,
        predicate: F,
    ) -> ObservedFuture<Result<(), ObservedRequest<PortType>>>
    where
        F: Fn(&PortType::Request) -> bool + Send + 'static;

    /// Fail if the tester observes a matching request at or after `start_index`
    /// during `duration`.
    fn fail_if_request_observed_from<F>(
        &self,
        start_index: usize,
        duration: Duration,
        predicate: F,
    ) -> ObservedFuture<Result<(), ObservedRequest<PortType>>>
    where
        F: Fn(&PortType::Request) -> bool + Send + 'static;

    /// Fail if the tester observes a matching indication during `duration`.
    fn fail_if_indication_observed<F>(
        &self,
        duration: Duration,
        predicate: F,
    ) -> ObservedFuture<Result<(), ObservedIndication<PortType>>>
    where
        F: Fn(&PortType::Indication) -> bool + Send + 'static;

    /// Fail if the tester observes a matching indication at or after
    /// `start_index` during `duration`.
    fn fail_if_indication_observed_from<F>(
        &self,
        start_index: usize,
        duration: Duration,
        predicate: F,
    ) -> ObservedFuture<Result<(), ObservedIndication<PortType>>>
    where
        F: Fn(&PortType::Indication) -> bool + Send + 'static;

    // TODO(flotsync-e0wg): Add a combined observe-or-fail helper that removes
    // the losing observation branch when the other branch resolves.
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

    fn observe_request<F>(&self, predicate: F) -> ObservedFuture<ObservedRequest<P>>
    where
        F: Fn(&<P as Port>::Request) -> bool + Send + 'static,
    {
        let (msg, future) = PortTestMsg::observe_request(predicate);
        self.tell(msg);
        future
    }

    fn observe_request_from<F>(
        &self,
        start_index: usize,
        predicate: F,
    ) -> ObservedFuture<ObservedRequest<P>>
    where
        F: Fn(&<P as Port>::Request) -> bool + Send + 'static,
    {
        let (msg, future) = PortTestMsg::observe_request_from(start_index, predicate);
        self.tell(msg);
        future
    }

    fn observe_indication<F>(&self, predicate: F) -> ObservedFuture<ObservedIndication<P>>
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
    ) -> ObservedFuture<ObservedIndication<P>>
    where
        F: Fn(&<P as Port>::Indication) -> bool + Send + 'static,
    {
        let (msg, future) = PortTestMsg::observe_indication_from(start_index, predicate);
        self.tell(msg);
        future
    }

    fn fail_if_observed<F>(
        &self,
        duration: Duration,
        predicate: F,
    ) -> KFuture<Result<(), ObservedEvent<P>>>
    where
        F: Fn((usize, &TestEvent<P>)) -> bool + Send + 'static,
    {
        let (msg, future) = PortTestMsg::fail_if_observed(duration, predicate);
        self.tell(msg);
        future
    }

    fn fail_if_observed_from<F>(
        &self,
        start_index: usize,
        duration: Duration,
        predicate: F,
    ) -> KFuture<Result<(), ObservedEvent<P>>>
    where
        F: Fn((usize, &TestEvent<P>)) -> bool + Send + 'static,
    {
        let (msg, future) = PortTestMsg::fail_if_observed_from(start_index, duration, predicate);
        self.tell(msg);
        future
    }

    fn fail_if_request_observed<F>(
        &self,
        duration: Duration,
        predicate: F,
    ) -> ObservedFuture<Result<(), ObservedRequest<P>>>
    where
        F: Fn(&<P as Port>::Request) -> bool + Send + 'static,
    {
        let (msg, future) = PortTestMsg::fail_if_request_observed(duration, predicate);
        self.tell(msg);
        future
    }

    fn fail_if_request_observed_from<F>(
        &self,
        start_index: usize,
        duration: Duration,
        predicate: F,
    ) -> ObservedFuture<Result<(), ObservedRequest<P>>>
    where
        F: Fn(&<P as Port>::Request) -> bool + Send + 'static,
    {
        let (msg, future) =
            PortTestMsg::fail_if_request_observed_from(start_index, duration, predicate);
        self.tell(msg);
        future
    }

    fn fail_if_indication_observed<F>(
        &self,
        duration: Duration,
        predicate: F,
    ) -> ObservedFuture<Result<(), ObservedIndication<P>>>
    where
        F: Fn(&<P as Port>::Indication) -> bool + Send + 'static,
    {
        let (msg, future) = PortTestMsg::fail_if_indication_observed(duration, predicate);
        self.tell(msg);
        future
    }

    fn fail_if_indication_observed_from<F>(
        &self,
        start_index: usize,
        duration: Duration,
        predicate: F,
    ) -> ObservedFuture<Result<(), ObservedIndication<P>>>
    where
        F: Fn(&<P as Port>::Indication) -> bool + Send + 'static,
    {
        let (msg, future) =
            PortTestMsg::fail_if_indication_observed_from(start_index, duration, predicate);
        self.tell(msg);
        future
    }
}

/// Heap-allocated predicate used to move event selection logic through actor messages.
type BoxedEventPredicate<PortType> =
    Box<dyn Fn((usize, &TestEvent<PortType>)) -> bool + Send + 'static>;

/// One active no-match window waiting for either a forbidden event or its timer.
struct PendingAbsenceObservation<PortType>
where
    PortType: Port + Send + Sync + 'static,
    PortType::Indication: Send + Sync + 'static,
    PortType::Request: Send + Sync + 'static,
{
    /// Ask resolved when the window completes or fails.
    ask: Ask<EventAbsencePredicate<PortType>, Result<(), ObservedEvent<PortType>>>,
    /// Effective inclusive start index for this observation.
    start_index: usize,
    /// Timer that resolves this observation successfully if no event matched.
    timer: ScheduledTimer,
}

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

        let second_f = provider_ref
            .observe_request_from(first.index() + 1, |event| event == "Repeated request");

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
        let old_event =
            wait_observed(observer_ref.observe_indication(|event| event == "Forbidden"));

        let ignores_old_event =
            observer_ref.fail_if_indication_observed(ABSENCE_WINDOW, |event| event == "Forbidden");
        assert!(wait_observed(ignores_old_event).is_ok());

        let fails_from_cursor = observer_ref.fail_if_indication_observed_from(
            old_event.index(),
            ABSENCE_WINDOW,
            |event| event == "Forbidden",
        );
        let failure =
            wait_observed(fails_from_cursor).expect_err("old event must fail cursor check");
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
}
