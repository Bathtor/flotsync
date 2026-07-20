//! Kompact probe components and buffered receivers for tests.

#[allow(
    clippy::wildcard_imports,
    reason = "The private helper module intentionally shares its parent's local implementation vocabulary."
)]
use super::*;

/// Buffered synchronous receiver for test probes.
///
/// Some harnesses need to observe only one subset of events while preserving
/// the rest for later assertions. This helper keeps unmatched events in a local
/// deferred queue instead of dropping them.
#[derive(Debug)]
pub struct BufferedReceiver<T> {
    receiver: mpsc::Receiver<T>,
    deferred: RefCell<VecDeque<T>>,
}

impl<T> BufferedReceiver<T> {
    /// Wraps one plain channel receiver with deferred-match support.
    #[must_use]
    pub fn new(receiver: mpsc::Receiver<T>) -> Self {
        Self {
            receiver,
            deferred: RefCell::new(VecDeque::new()),
        }
    }

    fn take_deferred_match(&self, predicate: &mut impl FnMut(&T) -> bool) -> Option<T> {
        let mut deferred = self.deferred.borrow_mut();
        let deferred_len = deferred.len();
        for _ in 0..deferred_len {
            let event = deferred
                .pop_front()
                .expect("deferred length was just measured");
            if predicate(&event) {
                return Some(event);
            }
            deferred.push_back(event);
        }
        None
    }

    fn take_deferred_match_or_failure(
        &self,
        predicate: &mut impl FnMut(&T) -> bool,
        fail_fast: &mut impl FnMut(&T) -> Option<String>,
    ) -> Result<Option<T>, String> {
        let mut deferred = self.deferred.borrow_mut();
        let deferred_len = deferred.len();
        for _ in 0..deferred_len {
            let event = deferred
                .pop_front()
                .expect("deferred length was just measured");
            if let Some(message) = fail_fast(&event) {
                return Err(message);
            }
            if predicate(&event) {
                return Ok(Some(event));
            }
            deferred.push_back(event);
        }
        Ok(None)
    }

    /// Waits for the next event that satisfies `predicate`, preserving
    /// unrelated events for later checks.
    ///
    /// # Panics
    ///
    /// Panics if no matching event is received before `timeout`.
    pub fn recv_matching(&self, timeout: Duration, mut predicate: impl FnMut(&T) -> bool) -> T {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(event) = self.take_deferred_match(&mut predicate) {
                return event;
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            let event = self
                .receiver
                .recv_timeout(remaining)
                .expect("timed out waiting for buffered test event");
            if predicate(&event) {
                return event;
            }
            self.deferred.borrow_mut().push_back(event);
        }
    }

    /// Waits for the next event that satisfies `predicate`, but aborts early
    /// if `fail_fast` identifies one event that proves the expected state
    /// transition cannot happen anymore.
    ///
    /// # Panics
    ///
    /// Panics if `fail_fast` rejects an observed event or if no matching event is received before
    /// `timeout`.
    pub fn recv_matching_or_fail(
        &self,
        timeout: Duration,
        mut predicate: impl FnMut(&T) -> bool,
        mut fail_fast: impl FnMut(&T) -> Option<String>,
    ) -> T {
        let deadline = Instant::now() + timeout;
        loop {
            match self.take_deferred_match_or_failure(&mut predicate, &mut fail_fast) {
                Ok(Some(event)) => return event,
                Ok(None) => {}
                Err(message) => panic!("{message}"),
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            let event = self
                .receiver
                .recv_timeout(remaining)
                .expect("timed out waiting for buffered test event");
            if let Some(message) = fail_fast(&event) {
                panic!("{message}");
            }
            if predicate(&event) {
                return event;
            }
            self.deferred.borrow_mut().push_back(event);
        }
    }

    /// Asserts that no buffered or newly received event satisfies
    /// `predicate` for the whole `duration`.
    ///
    /// A disconnected sender is treated as a harness failure rather than as
    /// silence because negative assertions rely on the probe staying live for
    /// the entire observation window.
    ///
    /// # Panics
    ///
    /// Panics if a buffered or newly received event matches `predicate`, or if the sender
    /// disconnects before `duration` elapses.
    pub fn assert_no_match(&self, duration: Duration, mut predicate: impl FnMut(&T) -> bool)
    where
        T: Debug,
    {
        if let Some(matching_event) = self
            .deferred
            .borrow()
            .iter()
            .find(|event| predicate(*event))
        {
            panic!("unexpected buffered test event matched negative assertion: {matching_event:?}");
        }

        let deadline = Instant::now() + duration;
        loop {
            let now = Instant::now();
            if now >= deadline {
                return;
            }

            let timeout = deadline
                .saturating_duration_since(now)
                .min(Duration::from_millis(10));
            let event = match self.receiver.recv_timeout(timeout) {
                Ok(event) => event,
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    panic!("buffered test event sender disconnected during negative assertion")
                }
            };
            assert!(
                !predicate(&event),
                "unexpected test event matched negative assertion: {event:?}"
            );
            self.deferred.borrow_mut().push_back(event);
        }
    }
}

/// Test observer that forwards UDP indications to an `mpsc` channel.
// TODO(flotsync-h1z0): Replace this observer/barrier fixture once generic
// actor-message testing support covers barrier-style test commands.
#[derive(ComponentDefinition)]
pub struct UdpObserver {
    ctx: ComponentContext<Self>,
    /// Required UDP port used by the bridge integration tests.
    pub udp: RequiredPort<UdpPort>,
    indications: mpsc::Sender<UdpIndication>,
}

/// Local actor messages understood by [`UdpObserver`].
#[derive(Debug)]
pub enum UdpObserverMessage {
    /// Completes once the observer has processed every mailbox item that was
    /// queued before this barrier.
    Barrier(KPromise<()>),
}

impl UdpObserver {
    /// Creates a new UDP observer.
    #[must_use]
    pub fn new(indications: mpsc::Sender<UdpIndication>) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            udp: RequiredPort::uninitialised(),
            indications,
        }
    }
}

ignore_lifecycle!(UdpObserver);

impl Require<UdpPort> for UdpObserver {
    fn handle(&mut self, indication: UdpIndication) -> HandlerResult {
        self.indications
            .send(indication)
            .expect("UDP indication receiver must stay live during integration tests");
        Handled::OK
    }
}

impl Actor for UdpObserver {
    type Message = UdpObserverMessage;

    fn receive_local(&mut self, msg: Self::Message) -> HandlerResult {
        match msg {
            UdpObserverMessage::Barrier(promise) => {
                let _ = promise.fulfil(());
                Handled::OK
            }
        }
    }
}

/// Test actor that forwards UDP send results to an `mpsc` channel.
// TODO(flotsync-h1z0): Replace with a generic actor-message probe.
#[derive(ComponentDefinition)]
pub struct UdpSendResultProbe {
    ctx: ComponentContext<Self>,
    results: mpsc::Sender<UdpSendResult>,
}

impl UdpSendResultProbe {
    /// Creates a new UDP send-result probe.
    #[must_use]
    pub fn new(results: mpsc::Sender<UdpSendResult>) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            results,
        }
    }
}

ignore_lifecycle!(UdpSendResultProbe);

impl Actor for UdpSendResultProbe {
    type Message = UdpSendResult;

    fn receive_local(&mut self, msg: Self::Message) -> HandlerResult {
        self.results
            .send(msg)
            .expect("UDP send result receiver must stay live during integration tests");
        Handled::OK
    }
}

/// Test actor that forwards TCP session events to an `mpsc` channel.
// TODO(flotsync-h1z0): Replace with a generic actor-message probe.
#[derive(ComponentDefinition)]
pub struct TcpSessionEventProbe {
    ctx: ComponentContext<Self>,
    events: mpsc::Sender<TcpSessionEvent>,
}

impl TcpSessionEventProbe {
    /// Creates a new TCP session-event probe.
    #[must_use]
    pub fn new(events: mpsc::Sender<TcpSessionEvent>) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            events,
        }
    }
}

ignore_lifecycle!(TcpSessionEventProbe);

impl Actor for TcpSessionEventProbe {
    type Message = TcpSessionEvent;

    fn receive_local(&mut self, msg: Self::Message) -> HandlerResult {
        self.events
            .send(msg)
            .expect("TCP session event receiver must stay live during integration tests");
        Handled::OK
    }
}

/// Test actor that forwards TCP listener events to an `mpsc` channel.
// TODO(flotsync-h1z0): Replace with a generic actor-message probe.
#[derive(ComponentDefinition)]
pub struct TcpListenerEventProbe {
    ctx: ComponentContext<Self>,
    events: mpsc::Sender<TcpListenerEvent>,
}

impl TcpListenerEventProbe {
    /// Creates a new TCP listener-event probe.
    #[must_use]
    pub fn new(events: mpsc::Sender<TcpListenerEvent>) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            events,
        }
    }
}

ignore_lifecycle!(TcpListenerEventProbe);

impl Actor for TcpListenerEventProbe {
    type Message = TcpListenerEvent;

    fn receive_local(&mut self, msg: Self::Message) -> HandlerResult {
        self.events
            .send(msg)
            .expect("TCP listener event receiver must stay live during integration tests");
        Handled::OK
    }
}
