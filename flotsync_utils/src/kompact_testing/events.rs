//! Event records and matching predicates for observed port traffic.

use super::*;

/// Heap-allocated predicate used to move event selection logic through actor messages.
type BoxedEventPredicate<PortType> =
    Box<dyn Fn((usize, &TestEvent<PortType>)) -> bool + Send + 'static>;

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
    pub(super) fn new(index: usize, event: Arc<TestEvent<PortType>>) -> Self {
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
