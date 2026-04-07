//! Shared test helpers for `flotsync_io`.
//!
//! This module is public behind the `test-support` feature so other crates can reuse the same
//! probes, wait helpers, and captured logging setup that the crate's own tests use.

use crate::prelude::*;
use kompact::{KompactLogger, prelude::*};
use slog::{Drain, Logger, PushFnValue, o};
use std::{
    cell::RefCell,
    collections::VecDeque,
    fmt::{Debug, Display},
    io::{self, Write},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::{Arc, OnceLock, mpsc},
    thread,
    time::{Duration, Instant},
};

/// Shared timeout used by the crate's synchronous test wait helpers.
pub const WAIT_TIMEOUT: Duration = Duration::from_secs(2);

/// Default poll cadence for eventually-style synchronous test waits.
pub const EVENTUALLY_POLL_INTERVAL: Duration = Duration::from_millis(1);

/// Installs the captured test logger once for both the `log` facade and Kompact's `slog` logger.
pub fn init_test_logger() {
    static LOGGER: OnceLock<()> = OnceLock::new();
    static LOGGER_GUARD: OnceLock<slog_scope::GlobalLoggerGuard> = OnceLock::new();

    LOGGER.get_or_init(|| {
        let guard = slog_scope::set_global_logger(captured_log_logger());
        let _ = LOGGER_GUARD.set(guard);
        slog_stdlog::init().expect("install slog/log bridge");
        log::set_max_level(log::LevelFilter::Trace);
    });
}

/// Builds a Kompact system whose logs follow libtest output capture.
pub fn build_test_kompact_system() -> KompactSystem {
    build_test_kompact_system_with(|_| {})
}

/// Builds a Kompact system whose logs follow libtest output capture after applying extra config.
pub fn build_test_kompact_system_with(configure: impl FnOnce(&mut KompactConfig)) -> KompactSystem {
    init_test_logger();

    let mut config = KompactConfig::default();
    configure(&mut config);
    config.logger(captured_kompact_logger());
    config.build().expect("build KompactSystem")
}

fn captured_log_logger() -> Logger {
    let decorator = slog_term::PlainSyncDecorator::new(CapturedOutput::default());
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    Logger::root(
        drain,
        o!(
            "location" => PushFnValue(|record: &slog::Record<'_>, serializer| {
                serializer.emit(format_args!("{}:{}", record.file(), record.line()))
            })
        ),
    )
}

fn captured_kompact_logger() -> KompactLogger {
    let decorator = slog_term::PlainSyncDecorator::new(CapturedOutput::default());
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).chan_size(1024).build().fuse();
    Logger::root_typed(
        Arc::new(drain),
        o!(
            "location" => PushFnValue(|record: &slog::Record<'_>, serializer| {
                serializer.emit(format_args!("{}:{}", record.file(), record.line()))
            })
        ),
    )
}

/// Returns the loopback address for the supplied port.
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
        if now >= deadline {
            panic!("{failure_message}");
        }

        thread::sleep(deadline.saturating_duration_since(now).min(poll_interval));
    }
}

/// Waits until `predicate` becomes true or `timeout` elapses.
pub fn eventually<M>(timeout: Duration, mut predicate: impl FnMut() -> bool, failure_message: M)
where
    M: Display,
{
    eventually_value(timeout, || predicate().then_some(()), failure_message);
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
pub fn assert_never<M>(duration: Duration, mut predicate: impl FnMut() -> bool, failure_message: M)
where
    M: Display,
{
    let deadline = Instant::now() + duration;
    loop {
        if predicate() {
            panic!("{failure_message}");
        }

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

/// Waits for a driver request to complete within [`WAIT_TIMEOUT`].
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
                    "ignoring unrelated driver event while waiting in integration test: {:?}",
                    other
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
pub fn kill_component<C>(system: &KompactSystem, component: Arc<Component<C>>)
where
    C: ComponentDefinition + ComponentLifecycle + Sized + 'static,
{
    system
        .kill_notify(component)
        .wait_timeout(WAIT_TIMEOUT)
        .expect("component kill");
}

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

    /// Waits for the next event that satisfies `predicate`, preserving
    /// unrelated events for later checks.
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

    /// Asserts that no buffered or newly received event satisfies
    /// `predicate` for the whole `duration`.
    ///
    /// A disconnected sender is treated as a harness failure rather than as
    /// silence because negative assertions rely on the probe staying live for
    /// the entire observation window.
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
            if predicate(&event) {
                panic!("unexpected test event matched negative assertion: {event:?}");
            }
            self.deferred.borrow_mut().push_back(event);
        }
    }
}

/// Test observer that forwards UDP indications to an `mpsc` channel.
#[derive(ComponentDefinition)]
pub struct UdpObserver {
    ctx: ComponentContext<Self>,
    /// Required UDP port used by the bridge integration tests.
    pub udp: RequiredPort<UdpPort>,
    indications: mpsc::Sender<UdpIndication>,
}

impl UdpObserver {
    /// Creates a new UDP observer.
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
    fn handle(&mut self, indication: UdpIndication) -> Handled {
        self.indications
            .send(indication)
            .expect("UDP indication receiver must stay live during integration tests");
        Handled::Ok
    }
}

impl Actor for UdpObserver {
    type Message = Never;

    fn receive_local(&mut self, _msg: Self::Message) -> Handled {
        unreachable!("Never type is empty")
    }

    fn receive_network(&mut self, _msg: NetMessage) -> Handled {
        unimplemented!("UDP observer test component does not use network actor messages")
    }
}

/// Test actor that forwards UDP send results to an `mpsc` channel.
#[derive(ComponentDefinition)]
pub struct UdpSendResultProbe {
    ctx: ComponentContext<Self>,
    results: mpsc::Sender<UdpSendResult>,
}

impl UdpSendResultProbe {
    /// Creates a new UDP send-result probe.
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

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        self.results
            .send(msg)
            .expect("UDP send result receiver must stay live during integration tests");
        Handled::Ok
    }

    fn receive_network(&mut self, _msg: NetMessage) -> Handled {
        unimplemented!("UDP send result probe does not use network actor messages")
    }
}

/// Test actor that forwards TCP session events to an `mpsc` channel.
#[derive(ComponentDefinition)]
pub struct TcpSessionEventProbe {
    ctx: ComponentContext<Self>,
    events: mpsc::Sender<TcpSessionEvent>,
}

impl TcpSessionEventProbe {
    /// Creates a new TCP session-event probe.
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

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        self.events
            .send(msg)
            .expect("TCP session event receiver must stay live during integration tests");
        Handled::Ok
    }

    fn receive_network(&mut self, _msg: NetMessage) -> Handled {
        unimplemented!("TCP session probe does not use network actor messages")
    }
}

/// Test actor that forwards TCP listener events to an `mpsc` channel.
#[derive(ComponentDefinition)]
pub struct TcpListenerEventProbe {
    ctx: ComponentContext<Self>,
    events: mpsc::Sender<TcpListenerEvent>,
}

impl TcpListenerEventProbe {
    /// Creates a new TCP listener-event probe.
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

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        self.events
            .send(msg)
            .expect("TCP listener event receiver must stay live during integration tests");
        Handled::Ok
    }

    fn receive_network(&mut self, _msg: NetMessage) -> Handled {
        unimplemented!("TCP listener probe does not use network actor messages")
    }
}

/// Writer that routes test log output through libtest's captured stdout path.
#[derive(Default)]
struct CapturedOutput {
    pending: Vec<u8>,
}

impl CapturedOutput {
    fn emit_complete_lines(&mut self) {
        while let Some(newline_index) = self.pending.iter().position(|byte| *byte == b'\n') {
            let line: Vec<u8> = self.pending.drain(..=newline_index).collect();
            self.emit_bytes(&line);
        }
    }

    fn emit_bytes(&self, bytes: &[u8]) {
        let text = String::from_utf8_lossy(bytes);
        print!("{text}");
        let _ = io::stdout().flush();
    }
}

impl Write for CapturedOutput {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.pending.extend_from_slice(buf);
        self.emit_complete_lines();
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if !self.pending.is_empty() {
            let remaining = std::mem::take(&mut self.pending);
            self.emit_bytes(&remaining);
        }
        Ok(())
    }
}

impl Drop for CapturedOutput {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}
