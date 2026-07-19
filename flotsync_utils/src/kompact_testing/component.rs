//! Port-testing messages and the tester component implementation.

use super::*;

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
