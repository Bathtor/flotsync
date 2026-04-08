//! Shared test helpers for `flotsync_io`.
//!
//! This module is public behind the `test-support` feature so other crates can reuse the same
//! probes, wait helpers, and captured logging setup that the crate's own tests use.

use crate::{
    config_keys,
    prelude::*,
    socket_support::{configure_bind_reuse, socket_domain},
};
use kompact::{KompactLogger, default_components::install_manual_timer, prelude::*};
use slog::{Drain, Logger, PushFnValue, o};
use socket2::{Protocol, SockAddr, Socket, Type};
use std::{
    cell::RefCell,
    collections::VecDeque,
    fmt::{Debug, Display},
    io::{self, Write},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::{Arc, Condvar, LazyLock, Mutex, OnceLock, mpsc},
    thread,
    time::{Duration, Instant},
};

/// Shared timeout used by the crate's synchronous test wait helpers.
pub const WAIT_TIMEOUT: Duration = Duration::from_secs(2);

/// Default poll cadence for eventually-style synchronous test waits.
pub const EVENTUALLY_POLL_INTERVAL: Duration = Duration::from_millis(1);

/// Retry cadence for the reserved-socket broker while waiting for the OS to
/// make more loopback sockets available.
pub const RESERVED_SOCKET_RETRY_INTERVAL: Duration = Duration::from_millis(10);

/// Maximum time one test will wait for its full reserved-socket request before
/// failing.
pub const RESERVED_SOCKET_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);

/// One socket kind that the reserved-socket broker can hold on behalf of one
/// test.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReservedSocketKind {
    TcpListener,
    UdpSocket,
}

/// Lease over one reserved-socket request.
///
/// Each entry keeps one live reservation socket open for the whole test so the
/// OS continues to account for that port while the actual driver, bridge, or
/// child process binds against the same address with the test-only re-use
/// option enabled.
#[derive(Debug)]
pub struct ReservedSocketLease {
    reserved: Vec<ReservedSocket>,
}

impl ReservedSocketLease {
    /// Returns the reserved socket address at `index`.
    pub fn addr(&self, index: usize) -> SocketAddr {
        self.reserved[index].addr
    }

    /// Returns the reserved socket kind at `index`.
    pub fn kind(&self, index: usize) -> ReservedSocketKind {
        self.reserved[index].kind
    }

    /// Returns how many reserved sockets this lease holds.
    pub fn len(&self) -> usize {
        self.reserved.len()
    }

    /// Returns whether this lease holds no reserved sockets.
    pub fn is_empty(&self) -> bool {
        self.reserved.is_empty()
    }
}

impl Drop for ReservedSocketLease {
    fn drop(&mut self) {
        RESERVED_SOCKET_BROKER.release(std::mem::take(&mut self.reserved));
    }
}

/// Reserves one ordered set of socket addresses for the duration of one test.
///
/// This broker is currently process-local. It can still deadlock across test
/// executables if multiple processes each hold partial reservations while
/// waiting for more sockets. We intentionally keep the first cut simple and
/// rely on the OS as the only global coordinator for now.
pub fn reserve_sockets(kinds: &[ReservedSocketKind]) -> ReservedSocketLease {
    RESERVED_SOCKET_BROKER.reserve(kinds)
}

/// Enables the `flotsync_io` test-only bind re-use mode on one Kompact config.
pub fn enable_bind_reuse_address(config: &mut KompactConfig) {
    config.set_config_value(&config_keys::BIND_REUSE_ADDRESS, true);
}

/// Binds one blocking `TcpListener` at the reserved address held by `lease`.
pub fn bind_reserved_tcp_listener(
    lease: &ReservedSocketLease,
    index: usize,
) -> io::Result<std::net::TcpListener> {
    let reserved = &lease.reserved[index];
    assert_eq!(
        reserved.kind,
        ReservedSocketKind::TcpListener,
        "reserved socket {index} is not a TCP listener reservation"
    );

    let socket = Socket::new(
        socket_domain(reserved.addr),
        Type::STREAM,
        Some(Protocol::TCP),
    )?;
    configure_bind_reuse(&socket)?;
    socket.bind(&SockAddr::from(reserved.addr))?;
    socket.listen(128)?;
    Ok(socket.into())
}

/// Binds one blocking `UdpSocket` at the reserved address held by `lease`.
pub fn bind_reserved_udp_socket(
    lease: &ReservedSocketLease,
    index: usize,
) -> io::Result<std::net::UdpSocket> {
    let reserved = &lease.reserved[index];
    assert_eq!(
        reserved.kind,
        ReservedSocketKind::UdpSocket,
        "reserved socket {index} is not a UDP socket reservation"
    );

    let socket = Socket::new(
        socket_domain(reserved.addr),
        Type::DGRAM,
        Some(Protocol::UDP),
    )?;
    configure_bind_reuse(&socket)?;
    socket.bind(&SockAddr::from(reserved.addr))?;
    Ok(socket.into())
}

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

/// Builds a Kompact system with a manually-driven timer after applying extra config.
pub fn build_test_kompact_system_with_manual_timer(
    configure: impl FnOnce(&mut KompactConfig),
) -> (KompactSystem, kompact::timer::ManualTimer) {
    init_test_logger();

    let mut config = KompactConfig::default();
    configure(&mut config);
    let timer = install_manual_timer(&mut config);
    config.logger(captured_kompact_logger());
    let system = config.build().expect("build KompactSystem");
    (system, timer)
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

/// Process-local broker for reservation sockets used by one test binary.
static RESERVED_SOCKET_BROKER: LazyLock<ReservedSocketBroker> =
    LazyLock::new(ReservedSocketBroker::new);

/// Coordinates process-local socket reservations across concurrently running
/// tests.
///
/// The broker keeps live reservation sockets open until the corresponding
/// lease is dropped. That means the OS continues to account for those ports
/// while the real driver, bridge, or child process binds the same address with
/// the test-only re-use option enabled.
#[derive(Debug)]
struct ReservedSocketBroker {
    state: Mutex<ReservedSocketBrokerState>,
    changed: Condvar,
}

impl ReservedSocketBroker {
    fn new() -> Self {
        Self {
            state: Mutex::new(ReservedSocketBrokerState::default()),
            changed: Condvar::new(),
        }
    }

    fn reserve(&self, kinds: &[ReservedSocketKind]) -> ReservedSocketLease {
        let requested_tcp = kinds
            .iter()
            .filter(|kind| matches!(kind, ReservedSocketKind::TcpListener))
            .count();
        let requested_udp = kinds
            .iter()
            .filter(|kind| matches!(kind, ReservedSocketKind::UdpSocket))
            .count();
        let deadline = Instant::now() + RESERVED_SOCKET_ACQUIRE_TIMEOUT;
        let mut state = self
            .state
            .lock()
            .expect("reserved-socket broker state lock");
        state.waiting_tcp += requested_tcp;
        state.waiting_udp += requested_udp;

        // Each caller declares its full socket requirement up front. That keeps
        // the process-local broker free of hold-and-wait deadlocks.
        'wait_for_request: loop {
            let mut acquired = Vec::with_capacity(kinds.len());
            let mut failure = None;

            'acquire_requested_kinds: for kind in kinds.iter().copied() {
                if let Some(reserved) = state.take_idle(kind) {
                    acquired.push(reserved);
                    continue;
                }

                match ReservedSocket::open(kind) {
                    Ok(reserved) => acquired.push(reserved),
                    Err(error) => {
                        failure = Some(error);
                        break 'acquire_requested_kinds;
                    }
                }
            }

            if let Some(error) = failure {
                // If one requested kind cannot be acquired yet, hand back any
                // partial progress before waiting again. Otherwise one blocked
                // test could hoard enough sockets to starve the next request.
                for reserved in acquired {
                    state.reclaim_idle(reserved);
                }
                let now = Instant::now();
                if now >= deadline {
                    state.waiting_tcp -= requested_tcp;
                    state.waiting_udp -= requested_udp;
                    state.trim_idle_to_waiting();
                    panic!(
                        "timed out waiting to reserve {requested_tcp} TCP and {requested_udp} UDP sockets for one test: {error}"
                    );
                }
                let wait = deadline
                    .saturating_duration_since(now)
                    .min(RESERVED_SOCKET_RETRY_INTERVAL);
                let (next_state, _) = self
                    .changed
                    .wait_timeout(state, wait)
                    .expect("reserved-socket broker condvar wait");
                state = next_state;
                continue 'wait_for_request;
            }

            state.waiting_tcp -= requested_tcp;
            state.waiting_udp -= requested_udp;
            state.trim_idle_to_waiting();
            return ReservedSocketLease { reserved: acquired };
        }
    }

    fn release(&self, reserved: Vec<ReservedSocket>) {
        if reserved.is_empty() {
            return;
        }

        let mut state = self
            .state
            .lock()
            .expect("reserved-socket broker state lock");
        for reserved in reserved {
            state.reclaim_idle(reserved);
        }
        state.trim_idle_to_waiting();
        self.changed.notify_all();
    }
}

/// Mutable broker state guarded by [`ReservedSocketBroker::state`].
///
/// `waiting_*` tracks how much unsatisfied demand currently exists across
/// blocked callers. Idle pools are trimmed back to those counts so one process
/// does not keep more ports reserved than any current waiter still needs.
#[derive(Debug, Default)]
struct ReservedSocketBrokerState {
    idle_tcp: Vec<ReservedSocket>,
    idle_udp: Vec<ReservedSocket>,
    waiting_tcp: usize,
    waiting_udp: usize,
}

impl ReservedSocketBrokerState {
    fn take_idle(&mut self, kind: ReservedSocketKind) -> Option<ReservedSocket> {
        match kind {
            ReservedSocketKind::TcpListener => self.idle_tcp.pop(),
            ReservedSocketKind::UdpSocket => self.idle_udp.pop(),
        }
    }

    fn reclaim_idle(&mut self, reserved: ReservedSocket) {
        let idle = match reserved.kind {
            ReservedSocketKind::TcpListener => &mut self.idle_tcp,
            ReservedSocketKind::UdpSocket => &mut self.idle_udp,
        };
        let waiting = match reserved.kind {
            ReservedSocketKind::TcpListener => self.waiting_tcp,
            ReservedSocketKind::UdpSocket => self.waiting_udp,
        };
        if idle.len() < waiting {
            idle.push(reserved);
        }
    }

    fn trim_idle_to_waiting(&mut self) {
        self.idle_tcp.truncate(self.waiting_tcp);
        self.idle_udp.truncate(self.waiting_udp);
    }
}

/// One live reservation socket held on behalf of one test.
///
/// `_socket` is intentionally never used directly after construction. Its only
/// job is to keep the reserved port allocated until the owning lease is
/// dropped.
#[derive(Debug)]
struct ReservedSocket {
    kind: ReservedSocketKind,
    addr: SocketAddr,
    _socket: Socket,
}

impl ReservedSocket {
    fn open(kind: ReservedSocketKind) -> io::Result<Self> {
        let socket = match kind {
            ReservedSocketKind::TcpListener => Socket::new(
                socket_domain(localhost(0)),
                Type::STREAM,
                Some(Protocol::TCP),
            )?,
            ReservedSocketKind::UdpSocket => Socket::new(
                socket_domain(localhost(0)),
                Type::DGRAM,
                Some(Protocol::UDP),
            )?,
        };
        configure_bind_reuse(&socket)?;
        socket.bind(&SockAddr::from(localhost(0)))?;
        let addr = socket
            .local_addr()?
            .as_socket()
            .expect("reserved test socket must use an IP socket address");
        Ok(Self {
            kind,
            addr,
            _socket: socket,
        })
    }
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

/// Local actor messages understood by [`UdpObserver`].
#[derive(Debug)]
pub enum UdpObserverMessage {
    /// Completes once the observer has processed every mailbox item that was
    /// queued before this barrier.
    Barrier(KPromise<()>),
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
    type Message = UdpObserverMessage;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        match msg {
            UdpObserverMessage::Barrier(promise) => {
                let _ = promise.fulfil(());
                Handled::Ok
            }
        }
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
