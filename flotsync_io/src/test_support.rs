//! Shared test helpers for `flotsync_io`.
//!
//! This module is public behind the `test-support` feature so other crates can reuse the same
//! probes, wait helpers, and captured logging setup that the crate's own tests use.

use crate::{
    config_keys,
    prelude::*,
    socket_support::{configure_bind_reuse, socket_domain},
};
use futures_util::FutureExt;
use kompact::{
    KompactLogger,
    config_keys::system,
    default_components::install_manual_timer,
    prelude::*,
};
use slog::{Drain, Logger, PushFnValue, o};
use socket2::{Protocol, SockAddr, Socket, Type};
use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    fmt::{Debug, Display},
    future::Future,
    io::{self, Write},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    pin::pin,
    process::Command,
    sync::{
        Arc,
        Condvar,
        LazyLock,
        Mutex,
        MutexGuard,
        OnceLock,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread::{self, ThreadId},
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
pub const RESERVED_SOCKET_ACQUIRE_TIMEOUT: Duration = Duration::from_mins(3);

/// Maximum attempts to acquire one hidden reservation socket directly from the
/// OS while still validating that the broker never tracks the same
/// `(kind, addr)` twice.
const RESERVED_SOCKET_BIND_ATTEMPTS: usize = 16;

/// One socket kind that the reserved-socket broker can hold on behalf of one
/// test.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReservedSocketKind {
    TcpListener,
    UdpSocket,
}

/// Lease over one reserved-socket request.
///
/// Each entry starts with one live reservation socket open so the OS continues
/// to account for that port while the actual driver, bridge, or child process
/// binds against the same address with the test-only re-use option enabled.
/// Tests may temporarily release and later re-bind the hidden reservation
/// socket while still keeping ownership of the lease slot.
#[derive(Debug)]
pub struct ReservedSocketLease {
    reserved: Vec<ReservedSocket>,
}

impl ReservedSocketLease {
    /// Returns the reserved socket address at `index`.
    #[must_use]
    pub fn addr(&self, index: usize) -> SocketAddr {
        self.reserved[index].addr
    }

    /// Returns the reserved socket kind at `index`.
    #[must_use]
    pub fn kind(&self, index: usize) -> ReservedSocketKind {
        self.reserved[index].kind
    }

    /// Returns how many reserved sockets this lease holds.
    #[must_use]
    pub fn len(&self) -> usize {
        self.reserved.len()
    }

    /// Returns whether this lease holds no reserved sockets.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.reserved.is_empty()
    }

    /// Hand one lease slot over to a live real socket that is already bound.
    ///
    /// Tests use this after the real system socket has successfully bound the
    /// reserved port, so the hidden reservation socket can no longer race to
    /// receive inbound UDP traffic on Linux.
    ///
    /// Prefer this over [`Self::release_binding`] in new code because it also
    /// updates the broker-visible slot state to reflect that the real socket is
    /// now live.
    pub fn activate_live_binding(&mut self, index: usize) {
        self.reserved[index].activate_live_binding();
    }

    /// Start handing one lease slot over to a real socket that has not yet
    /// completed its bind.
    ///
    /// This temporarily reduces the broker-visible live socket count for the
    /// slot until the caller either confirms the real bind or restores the
    /// hidden reservation socket.
    pub fn begin_live_binding(&mut self, index: usize) {
        self.reserved[index].begin_live_binding();
    }

    /// Confirm that a previously started live-bind handoff succeeded.
    pub fn complete_live_binding(&mut self, index: usize) {
        self.reserved[index].complete_live_binding();
    }

    /// Closes the hidden reservation socket at `index` without changing the
    /// broker-visible slot state.
    ///
    /// Older tests still use this simpler handoff shape. New code should
    /// prefer [`Self::activate_live_binding`] or the
    /// [`Self::begin_live_binding`] / [`Self::complete_live_binding`] pair so
    /// the broker can account for the live socket transition precisely.
    pub fn release_binding(&mut self, index: usize) {
        self.reserved[index].release_binding();
    }

    /// Re-binds the reservation socket at `index` to its original address.
    ///
    /// Tests use this before teardown so the lease once again owns a live
    /// reservation socket before the driver or bridge releases the port.
    ///
    /// # Errors
    ///
    /// See `io::Error` for failure conditions.
    pub fn rebind_binding(&mut self, index: usize) -> io::Result<()> {
        self.reserved[index].ensure_bound()
    }
}

impl Drop for ReservedSocketLease {
    fn drop(&mut self) {
        let mut rebound = Vec::with_capacity(self.reserved.len());
        let mut failed_rebinds = Vec::new();
        for mut reserved in std::mem::take(&mut self.reserved) {
            match reserved.ensure_bound() {
                Ok(()) => rebound.push(reserved),
                Err(error) => {
                    failed_rebinds.push(format!(
                        "{:?} reservation at {}: {error}",
                        reserved.kind, reserved.addr
                    ));
                }
            }
        }
        RESERVED_SOCKET_BROKER.release(rebound);
        assert!(
            failed_rebinds.is_empty() || std::thread::panicking(),
            "failed to rebind reserved test sockets before returning them to the broker: {}",
            failed_rebinds.join("; ")
        );
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

/// Sets the Kompact runtime label used by the system logger and shutdown
/// diagnostics for one test system.
pub fn set_test_system_label(config: &mut KompactConfig, label_prefix: &str) {
    static TEST_SYSTEM_LABEL_COUNTER: AtomicUsize = AtomicUsize::new(1);
    let label_index = TEST_SYSTEM_LABEL_COUNTER.fetch_add(1, Ordering::Relaxed);
    let label = format!("{label_prefix}-{label_index}");
    config.set_config_value(&system::LABEL, label);
}

/// Binds one blocking `TcpListener` at the reserved address held by `lease`.
///
/// # Errors
///
/// See `io::Error` for failure conditions.
///
/// # Panics
///
/// Panics if `index` is out of bounds or does not refer to a TCP listener reservation.
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
///
/// # Errors
///
/// See `io::Error` for failure conditions.
///
/// # Panics
///
/// Panics if `index` is out of bounds or does not refer to a UDP socket reservation.
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
///
/// # Panics
///
/// Panics if the `slog` to `log` bridge cannot be installed.
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
#[must_use]
pub fn build_test_kompact_system() -> KompactSystem {
    build_test_kompact_system_with(|_| {})
}

/// Builds a Kompact system whose logs follow libtest output capture after applying extra config.
///
/// # Panics
///
/// Panics if the test logger cannot be installed or the Kompact system cannot be built.
pub fn build_test_kompact_system_with(configure: impl FnOnce(&mut KompactConfig)) -> KompactSystem {
    init_test_logger();

    let mut config = KompactConfig::default();
    configure(&mut config);
    config.logger(captured_kompact_logger());
    config.build().expect("build KompactSystem")
}

/// Builds a Kompact system with a manually-driven timer after applying extra config.
///
/// # Panics
///
/// Panics if the test logger cannot be installed or the Kompact system cannot be built.
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
static RESERVED_SOCKET_SLOT_ID_COUNTER: AtomicUsize = AtomicUsize::new(1);
static RESERVED_SOCKET_REQUEST_ID_COUNTER: AtomicUsize = AtomicUsize::new(1);
const RESERVED_SOCKET_BROKER_LOG_TARGET: &str = "flotsync_io::reserved_socket_broker";

/// Coordinates process-local socket reservations across concurrently running
/// tests.
///
/// The broker hands out leases whose reservation sockets start out live. Leases
/// may temporarily release those hidden sockets, but they keep ownership of the
/// reservation slots until drop, at which point any successfully rebound
/// sockets are returned to the broker's idle pool.
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

    fn lock_state(
        &self,
        action: &'static str,
    ) -> std::sync::MutexGuard<'_, ReservedSocketBrokerState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let state = poisoned.get_ref();
                let expected_live = state.expected_live_socket_counts();
                panic!(
                    "reserved-socket broker state lock poisoned during {action}: waiting_tcp={}, waiting_udp={}, idle_tcp={}, idle_udp={}, expected_live_tcp={}, expected_live_udp={}, slot_summary={}",
                    state.waiting_tcp,
                    state.waiting_udp,
                    state.idle_tcp.len(),
                    state.idle_udp.len(),
                    expected_live.tcp,
                    expected_live.udp,
                    state.slot_summary(),
                );
            }
        }
    }

    fn transition_slot_state(
        &self,
        slot_id: usize,
        new_state: ReservedSocketSlotState,
        action: &'static str,
    ) {
        let mut state = self.lock_state(action);
        state.transition_slot_state(slot_id, new_state);
    }

    /// Only returns once we are the ones who may reserve slots.
    fn wait_to_be_allowed_to_reserve<'a>(
        &self,
        mut state: MutexGuard<'a, ReservedSocketBrokerState>,
        trace: &ReservationTrace,
        acquired: &[ReservedSocket],
        kinds: &[ReservedSocketKind],
    ) -> MutexGuard<'a, ReservedSocketBrokerState> {
        let my_id_opt = Some(trace.owner_thread_id);
        while state.acquisition_in_progress_by != my_id_opt {
            if state.acquisition_in_progress_by.is_none() {
                state.acquisition_in_progress_by = my_id_opt;
                let snapshot = state.build_reservation_counters(trace, acquired, kinds);
                trace.became_active(&snapshot);
            } else {
                state = self.wait_for_changes(state);
            }
        }
        state
    }

    fn wait_for_changes<'a>(
        &self,
        state: MutexGuard<'a, ReservedSocketBrokerState>,
    ) -> MutexGuard<'a, ReservedSocketBrokerState> {
        let wait_result = self
            .changed
            .wait_timeout(state, RESERVED_SOCKET_RETRY_INTERVAL);
        match wait_result {
            Ok((next_state, _timeout_result)) => next_state,
            Err(poisoned) => {
                let (state, _) = poisoned.get_ref();
                let expected_live = state.expected_live_socket_counts();
                panic!(
                    "reserved-socket broker condvar wait poisoned while reserving sockets: waiting_tcp={}, waiting_udp={}, idle_tcp={}, idle_udp={}, expected_live_tcp={}, expected_live_udp={}, slot_summary={}",
                    state.waiting_tcp,
                    state.waiting_udp,
                    state.idle_tcp.len(),
                    state.idle_udp.len(),
                    expected_live.tcp,
                    expected_live.udp,
                    state.slot_summary(),
                );
            }
        }
    }

    fn complete_reservation_phase(
        &self,
        mut state: MutexGuard<'_, ReservedSocketBrokerState>,
        requested_tcp: usize,
        requested_udp: usize,
    ) {
        state.waiting_tcp -= requested_tcp;
        state.waiting_udp -= requested_udp;
        state.trim_idle_to_waiting();

        state.acquisition_in_progress_by = None;
        drop(state);
        self.changed.notify_all();
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
        let trace = ReservationTrace::new(requested_tcp, requested_udp);
        let mut state = self.lock_state("socket reservation");
        state.waiting_tcp += requested_tcp;
        state.waiting_udp += requested_udp;
        let mut acquired = Vec::with_capacity(kinds.len());
        trace.request_started(&state.build_reservation_counters(&trace, &acquired, kinds));

        state = self.wait_to_be_allowed_to_reserve(state, &trace, &acquired, kinds);

        // Each caller declares its full socket requirement up front. That keeps
        // the process-local broker free of hold-and-wait deadlocks.
        while Instant::now() < deadline {
            if acquired.len() == kinds.len() {
                trace.completed(&state.build_reservation_counters(&trace, &acquired, kinds));
                self.complete_reservation_phase(state, requested_tcp, requested_udp);
                return ReservedSocketLease { reserved: acquired };
            }

            // Try to acquire from idle.
            let acquired_before_idle = acquired.len();
            while let Some(kind) = kinds.get(acquired.len()).copied() {
                let Some(reserved) = state.take_idle(kind) else {
                    break;
                };
                acquired.push(reserved);
            }
            let took_idle_count = acquired.len() - acquired_before_idle;
            if took_idle_count > 0 {
                trace.took_idle_batch(
                    &state.build_reservation_counters(&trace, &acquired, kinds),
                    took_idle_count,
                );
            }

            if acquired.len() == kinds.len() {
                trace.completed(&state.build_reservation_counters(&trace, &acquired, kinds));
                self.complete_reservation_phase(state, requested_tcp, requested_udp);
                return ReservedSocketLease { reserved: acquired };
            }

            // Not enough idle sockets, try to acquire a socket.
            let next_kind = kinds[acquired.len()];

            // Drop state while we acquire so other threads can get the mutex to return leases.
            drop(state);

            let open_result = ReservedSocket::open(next_kind);

            // Re-acquire state before completing the loop.
            state = self.lock_state("socket reservation");

            match open_result {
                Ok(reserved) => {
                    state.register_new_leased_slot(&reserved);
                    acquired.push(reserved);
                    trace.acquired_new_socket(
                        &state.build_reservation_counters(&trace, &acquired, kinds),
                        next_kind,
                    );
                }
                Err(error) => {
                    trace.acquire_failed(
                        &state.build_reservation_failure_context(&trace, &acquired, kinds),
                        next_kind,
                        &error,
                    );
                    // Wait for leases to be returned before retrying to acquire another socket.
                    state = self.wait_for_changes(state);
                }
            }
        }
        self.handle_reservation_timeout(state, requested_tcp, requested_udp, acquired);
    }

    fn handle_reservation_timeout(
        &self,
        mut state: MutexGuard<'_, ReservedSocketBrokerState>,
        requested_tcp: usize,
        requested_udp: usize,
        acquired: Vec<ReservedSocket>,
    ) -> Never {
        let diagnostics = state.build_diagnostics();

        for reserved in acquired {
            state.reclaim_idle(reserved);
        }
        self.complete_reservation_phase(state, requested_tcp, requested_udp);

        let diagnostics_rendered = diagnostics.render();
        let has_socket_accounting_mismatch = diagnostics.has_socket_accounting_mismatch();
        assert!(
            !has_socket_accounting_mismatch,
            "reserved-socket broker detected a socket-accounting mismatch while reserving {requested_tcp} TCP and {requested_udp} UDP sockets for one test: {diagnostics_rendered}",
        );
        panic!(
            "timed out waiting to reserve {requested_tcp} TCP and {requested_udp} UDP sockets for one test: {diagnostics_rendered}",
        );
    }

    fn release(&self, reserved: Vec<ReservedSocket>) {
        if reserved.is_empty() {
            return;
        }

        let mut state = self.lock_state("socket release");
        for reserved in reserved {
            state.reclaim_idle(reserved);
        }
        state.trim_idle_to_waiting();
        drop(state);
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
    acquisition_in_progress_by: Option<ThreadId>,
    slots: HashMap<usize, ReservedSocketSlotRecord>,
}

impl ReservedSocketBrokerState {
    fn take_idle(&mut self, kind: ReservedSocketKind) -> Option<ReservedSocket> {
        let reserved = match kind {
            ReservedSocketKind::TcpListener => self.idle_tcp.pop(),
            ReservedSocketKind::UdpSocket => self.idle_udp.pop(),
        };
        if let Some(reserved) = reserved {
            self.transition_slot_state(reserved.slot_id, ReservedSocketSlotState::LeaseHiddenBound);
            return Some(reserved);
        }
        None
    }

    fn reclaim_idle(&mut self, reserved: ReservedSocket) {
        self.transition_slot_state(reserved.slot_id, ReservedSocketSlotState::IdleHiddenBound);
        match reserved.kind {
            ReservedSocketKind::TcpListener => {
                if self.idle_tcp.len() < self.waiting_tcp {
                    self.idle_tcp.push(reserved);
                    return;
                }
            }
            ReservedSocketKind::UdpSocket => {
                if self.idle_udp.len() < self.waiting_udp {
                    self.idle_udp.push(reserved);
                    return;
                }
            }
        }
        self.remove_slot(reserved.slot_id);
    }

    fn trim_idle_to_waiting(&mut self) {
        self.trim_idle_kind(ReservedSocketKind::TcpListener);
        self.trim_idle_kind(ReservedSocketKind::UdpSocket);
    }

    fn trim_idle_kind(&mut self, kind: ReservedSocketKind) {
        while self.idle_len(kind) > self.waiting_len(kind) {
            let reserved = self
                .take_idle_for_trim(kind)
                .expect("idle pool length must stay consistent while trimming");
            self.remove_slot(reserved.slot_id);
        }
    }

    fn idle_len(&self, kind: ReservedSocketKind) -> usize {
        match kind {
            ReservedSocketKind::TcpListener => self.idle_tcp.len(),
            ReservedSocketKind::UdpSocket => self.idle_udp.len(),
        }
    }

    fn waiting_len(&self, kind: ReservedSocketKind) -> usize {
        match kind {
            ReservedSocketKind::TcpListener => self.waiting_tcp,
            ReservedSocketKind::UdpSocket => self.waiting_udp,
        }
    }

    fn take_idle_for_trim(&mut self, kind: ReservedSocketKind) -> Option<ReservedSocket> {
        match kind {
            ReservedSocketKind::TcpListener => self.idle_tcp.pop(),
            ReservedSocketKind::UdpSocket => self.idle_udp.pop(),
        }
    }

    fn register_new_leased_slot(&mut self, reserved: &ReservedSocket) {
        self.assert_unique_slot_addr(reserved);
        let previous = self.slots.insert(
            reserved.slot_id,
            ReservedSocketSlotRecord {
                kind: reserved.kind,
                addr: reserved.addr,
                state: ReservedSocketSlotState::LeaseHiddenBound,
            },
        );
        debug_assert!(
            previous.is_none(),
            "reserved-socket broker slot ids must be unique"
        );
    }

    fn assert_unique_slot_addr(&self, reserved: &ReservedSocket) {
        let duplicate_slot = self
            .slots
            .iter()
            .find(|(_, slot)| slot.kind == reserved.kind && slot.addr == reserved.addr);
        let Some((duplicate_slot_id, duplicate_slot)) = duplicate_slot else {
            return;
        };
        panic!(
            "reserved-socket broker allocated duplicate {:?} reservation address {} for slot_id={} while slot_id={} is still in state {:?}; slot_summary={}",
            reserved.kind,
            reserved.addr,
            reserved.slot_id,
            duplicate_slot_id,
            duplicate_slot.state,
            self.slot_summary(),
        );
    }

    fn transition_slot_state(&mut self, slot_id: usize, new_state: ReservedSocketSlotState) {
        let slot = self.slots.get_mut(&slot_id).unwrap_or_else(|| {
            panic!("reserved-socket broker lost slot state for slot_id={slot_id}")
        });
        slot.state = new_state;
    }

    fn remove_slot(&mut self, slot_id: usize) {
        let removed = self.slots.remove(&slot_id);
        debug_assert!(
            removed.is_some(),
            "reserved-socket broker must only remove known slot ids"
        );
    }

    fn expected_live_socket_counts(&self) -> SocketProtocolCounts {
        let mut counts = SocketProtocolCounts::default();
        for slot in self.slots.values() {
            if !slot.state.counts_as_live_socket() {
                continue;
            }
            counts.increment(slot.kind);
        }
        counts
    }

    fn leased_socket_counts(&self) -> SocketProtocolCounts {
        let mut counts = SocketProtocolCounts::default();
        for slot in self.slots.values() {
            if !slot.state.counts_as_leased_socket() {
                continue;
            }
            counts.increment(slot.kind);
        }
        counts
    }

    fn slot_summary(&self) -> String {
        let mut entries = self
            .slots
            .iter()
            .map(|(slot_id, slot)| {
                format!("#{slot_id}:{:?}@{}={:?}", slot.kind, slot.addr, slot.state)
            })
            .collect::<Vec<_>>();
        entries.sort();
        if entries.is_empty() {
            return String::from("<no-slots>");
        }
        entries.join(", ")
    }

    fn build_reservation_counters(
        &self,
        trace: &ReservationTrace,
        acquired: &[ReservedSocket],
        kinds: &[ReservedSocketKind],
    ) -> ReservationCounters {
        let next_kind = kinds.get(acquired.len()).copied();
        ReservationCounters {
            requested: trace.requested,
            acquired: SocketProtocolCounts::from_reserved_sockets(acquired),
            next_kind,
            waiting: SocketProtocolCounts {
                tcp: self.waiting_tcp,
                udp: self.waiting_udp,
            },
            idle: SocketProtocolCounts {
                tcp: self.idle_tcp.len(),
                udp: self.idle_udp.len(),
            },
        }
    }

    fn build_reservation_failure_context(
        &self,
        trace: &ReservationTrace,
        acquired: &[ReservedSocket],
        kinds: &[ReservedSocketKind],
    ) -> ReservationFailureContext {
        let counters = self.build_reservation_counters(trace, acquired, kinds);
        ReservationFailureContext {
            next_kind_idle_available: counters.next_kind.map(|kind| self.idle_len(kind) > 0),
            leased: self.leased_socket_counts(),
            expected_live: self.expected_live_socket_counts(),
            counters,
        }
    }

    fn build_diagnostics(&self) -> ReservedSocketBrokerDiagnostics {
        ReservedSocketBrokerDiagnostics {
            waiting: SocketProtocolCounts {
                tcp: self.waiting_tcp,
                udp: self.waiting_udp,
            },
            idle: SocketProtocolCounts {
                tcp: self.idle_tcp.len(),
                udp: self.idle_udp.len(),
            },
            expected_live: self.expected_live_socket_counts(),
            actual_live: capture_process_socket_census(),
            slot_summary: self.slot_summary(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct SocketProtocolCounts {
    tcp: usize,
    udp: usize,
}

impl SocketProtocolCounts {
    fn increment(&mut self, kind: ReservedSocketKind) {
        match kind {
            ReservedSocketKind::TcpListener => self.tcp += 1,
            ReservedSocketKind::UdpSocket => self.udp += 1,
        }
    }

    fn from_reserved_sockets(reserved: &[ReservedSocket]) -> Self {
        let mut counts = Self::default();
        for reserved in reserved {
            counts.increment(reserved.kind);
        }
        counts
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReservedSocketSlotState {
    IdleHiddenBound,
    LeaseHiddenBound,
    LeasePendingRealBind,
    LeaseRealBound,
}

impl ReservedSocketSlotState {
    fn counts_as_live_socket(self) -> bool {
        !matches!(self, Self::LeasePendingRealBind)
    }

    fn counts_as_leased_socket(self) -> bool {
        !matches!(self, Self::IdleHiddenBound)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReservedSocketSlotRecord {
    kind: ReservedSocketKind,
    addr: SocketAddr,
    state: ReservedSocketSlotState,
}

#[derive(Debug)]
struct ReservedSocketBrokerDiagnostics {
    waiting: SocketProtocolCounts,
    idle: SocketProtocolCounts,
    expected_live: SocketProtocolCounts,
    actual_live: Result<SocketProtocolCounts, String>,
    slot_summary: String,
}

/// One compact broker snapshot for logging one reservation request event.
#[derive(Debug)]
struct ReservationCounters {
    requested: SocketProtocolCounts,
    acquired: SocketProtocolCounts,
    next_kind: Option<ReservedSocketKind>,
    waiting: SocketProtocolCounts,
    idle: SocketProtocolCounts,
}

impl ReservationCounters {
    fn render(&self, trace: &ReservationTrace) -> String {
        let next_kind = self
            .next_kind
            .map_or_else(|| String::from("<complete>"), |kind| format!("{kind:?}"));
        format!(
            "request_id={}, owner={}, requested_tcp={}, requested_udp={}, acquired_tcp={}, acquired_udp={}, next_kind={}, waiting_tcp={}, waiting_udp={}, idle_tcp={}, idle_udp={}",
            trace.request_id,
            trace.owner_label,
            self.requested.tcp,
            self.requested.udp,
            self.acquired.tcp,
            self.acquired.udp,
            next_kind,
            self.waiting.tcp,
            self.waiting.udp,
            self.idle.tcp,
            self.idle.udp,
        )
    }
}

#[derive(Debug)]
struct ReservationFailureContext {
    counters: ReservationCounters,
    next_kind_idle_available: Option<bool>,
    leased: SocketProtocolCounts,
    expected_live: SocketProtocolCounts,
}

impl ReservationFailureContext {
    fn render(&self, trace: &ReservationTrace) -> String {
        let next_kind_idle_available = self
            .next_kind_idle_available
            .map_or_else(|| String::from("<n/a>"), |available| available.to_string());
        format!(
            "{}, next_kind_idle_available={}, leased_tcp={}, leased_udp={}, expected_live_tcp={}, expected_live_udp={}",
            self.counters.render(trace),
            next_kind_idle_available,
            self.leased.tcp,
            self.leased.udp,
            self.expected_live.tcp,
            self.expected_live.udp,
        )
    }
}

/// One trace context for a single reservation request.
#[derive(Debug)]
struct ReservationTrace {
    request_id: usize,
    owner_thread_id: ThreadId,
    owner_label: String,
    requested: SocketProtocolCounts,
}

impl ReservationTrace {
    fn new(requested_tcp: usize, requested_udp: usize) -> Self {
        let current_thread = thread::current();
        let owner_thread_id = current_thread.id();
        let owner_name = current_thread.name().unwrap_or("<unnamed>");
        let owner_label = format!("{owner_name} ({owner_thread_id:?})");
        let request_id = RESERVED_SOCKET_REQUEST_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        Self {
            request_id,
            owner_thread_id,
            owner_label,
            requested: SocketProtocolCounts {
                tcp: requested_tcp,
                udp: requested_udp,
            },
        }
    }

    fn request_started(&self, counters: &ReservationCounters) {
        self.log_debug("request_started", counters);
    }

    fn became_active(&self, counters: &ReservationCounters) {
        self.log_debug("became_active", counters);
    }

    fn took_idle_batch(&self, counters: &ReservationCounters, count: usize) {
        self.log_debug_with_message("took_idle_batch", counters, format_args!("count={count}"));
    }

    fn acquired_new_socket(&self, counters: &ReservationCounters, kind: ReservedSocketKind) {
        self.log_debug_with_message(
            "acquired_new_socket",
            counters,
            format_args!("kind={kind:?}"),
        );
    }

    fn acquire_failed(
        &self,
        context: &ReservationFailureContext,
        kind: ReservedSocketKind,
        error: &io::Error,
    ) {
        if log::log_enabled!(target: RESERVED_SOCKET_BROKER_LOG_TARGET, log::Level::Warn) {
            log::warn!(
                target: RESERVED_SOCKET_BROKER_LOG_TARGET,
                "reserved-socket broker acquire_failed: {}, kind={kind:?}, error={error}",
                context.render(self),
            );
        }
    }

    fn completed(&self, counters: &ReservationCounters) {
        self.log_debug("completed", counters);
    }

    fn log_debug(&self, event: &str, counters: &ReservationCounters) {
        if log::log_enabled!(target: RESERVED_SOCKET_BROKER_LOG_TARGET, log::Level::Debug) {
            log::debug!(
                target: RESERVED_SOCKET_BROKER_LOG_TARGET,
                "reserved-socket broker {event}: {}",
                counters.render(self),
            );
        }
    }

    fn log_debug_with_message(
        &self,
        event: &str,
        counters: &ReservationCounters,
        message: impl Display,
    ) {
        if log::log_enabled!(target: RESERVED_SOCKET_BROKER_LOG_TARGET, log::Level::Debug) {
            log::debug!(
                target: RESERVED_SOCKET_BROKER_LOG_TARGET,
                "reserved-socket broker {event}: {}, {}",
                counters.render(self),
                message,
            );
        }
    }
}

impl ReservedSocketBrokerDiagnostics {
    fn has_socket_accounting_mismatch(&self) -> bool {
        let Ok(actual_live) = &self.actual_live else {
            return false;
        };
        *actual_live != self.expected_live
    }

    fn render(&self) -> String {
        let actual_live = match &self.actual_live {
            Ok(actual_live) => {
                format!(
                    "actual_live_tcp={}, actual_live_udp={}",
                    actual_live.tcp, actual_live.udp
                )
            }
            Err(error) => format!("actual_live_socket_census_failed={error}"),
        };
        format!(
            "waiting_tcp={}, waiting_udp={}, idle_tcp={}, idle_udp={}, expected_live_tcp={}, expected_live_udp={}, {}, slot_summary={}",
            self.waiting.tcp,
            self.waiting.udp,
            self.idle.tcp,
            self.idle.udp,
            self.expected_live.tcp,
            self.expected_live.udp,
            actual_live,
            self.slot_summary,
        )
    }
}

fn capture_process_socket_census() -> Result<SocketProtocolCounts, String> {
    let pid = std::process::id().to_string();
    let mut last_error = None;
    let mut output = None;
    for program in ["/usr/sbin/lsof", "lsof"] {
        match Command::new(program)
            .args(["-nP", "-a", "-p", &pid, "-iTCP", "-iUDP", "-F", "P"])
            .output()
        {
            Ok(candidate) => {
                output = Some(candidate);
                break;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                last_error = Some(format!("{program}: {error}"));
            }
            Err(error) => {
                return Err(format!("run {program} for process socket census: {error}"));
            }
        }
    }
    let output = output.ok_or_else(|| {
        format!(
            "run lsof for process socket census: {}",
            last_error.unwrap_or_else(|| String::from("no lsof binary found"))
        )
    })?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let mut counts = SocketProtocolCounts::default();
    for line in stdout.lines() {
        let Some(protocol) = line.strip_prefix('P') else {
            continue;
        };
        match protocol {
            "TCP" => counts.tcp += 1,
            "UDP" => counts.udp += 1,
            _ => {}
        }
    }
    if !output.status.success()
        && counts == SocketProtocolCounts::default()
        && !stderr.trim().is_empty()
    {
        return Err(format!(
            "lsof exited with status {} and stderr '{}'",
            output
                .status
                .code()
                .map_or_else(|| String::from("<signal>"), |code| code.to_string()),
            stderr.trim(),
        ));
    }
    Ok(counts)
}

/// One live reservation socket held on behalf of one test.
///
/// `socket` is intentionally never used directly after construction. Its only
/// job is to keep the reserved port allocated whenever the owning lease wants
/// the hidden reservation socket to stay bound.
#[derive(Debug)]
struct ReservedSocket {
    slot_id: usize,
    kind: ReservedSocketKind,
    addr: SocketAddr,
    socket: Option<Socket>,
}

impl ReservedSocket {
    fn open(kind: ReservedSocketKind) -> io::Result<Self> {
        let slot_id = RESERVED_SOCKET_SLOT_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut last_error = None;
        for _attempt in 0..RESERVED_SOCKET_BIND_ATTEMPTS {
            match Self::bind_socket(kind, localhost(0)) {
                Ok(socket) => {
                    let addr = socket.local_addr().map_err(|error| {
                        io::Error::new(
                            error.kind(),
                            format!(
                                "read assigned local address for reserved {kind:?} socket: {error}"
                            ),
                        )
                    })?;
                    let addr = addr.as_socket().ok_or_else(|| {
                        io::Error::other(format!(
                            "reserved {kind:?} socket was assigned a non-IP local address"
                        ))
                    })?;
                    return Ok(Self {
                        slot_id,
                        kind,
                        addr,
                        socket: Some(socket),
                    });
                }
                Err(error) => {
                    last_error = Some(error);
                }
            }
        }

        Err(last_error.expect("reserved socket bind attempts must record a failure"))
    }

    fn bind_socket(kind: ReservedSocketKind, addr: SocketAddr) -> io::Result<Socket> {
        let socket = match kind {
            ReservedSocketKind::TcpListener => {
                Socket::new(socket_domain(addr), Type::STREAM, Some(Protocol::TCP)).map_err(
                    |error| {
                        io::Error::new(
                            error.kind(),
                            format!("create reserved TCP listener socket at {addr}: {error}"),
                        )
                    },
                )?
            }
            ReservedSocketKind::UdpSocket => {
                Socket::new(socket_domain(addr), Type::DGRAM, Some(Protocol::UDP)).map_err(
                    |error| {
                        io::Error::new(
                            error.kind(),
                            format!("create reserved UDP socket at {addr}: {error}"),
                        )
                    },
                )?
            }
        };
        configure_bind_reuse(&socket).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("configure bind reuse for reserved {kind:?} socket at {addr}: {error}"),
            )
        })?;
        socket.bind(&SockAddr::from(addr)).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("bind reserved {kind:?} socket at {addr}: {error}"),
            )
        })?;
        if matches!(kind, ReservedSocketKind::TcpListener) {
            socket.listen(128).map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!("listen on reserved TCP listener socket at {addr}: {error}"),
                )
            })?;
        }
        Ok(socket)
    }

    fn release_binding(&mut self) {
        self.socket = None;
    }

    fn ensure_bound(&mut self) -> io::Result<()> {
        if self.socket.is_some() {
            return Ok(());
        }
        self.socket = Some(Self::bind_socket(self.kind, self.addr)?);
        RESERVED_SOCKET_BROKER.transition_slot_state(
            self.slot_id,
            ReservedSocketSlotState::LeaseHiddenBound,
            "reserved socket rebind",
        );
        Ok(())
    }

    fn activate_live_binding(&mut self) {
        self.socket = None;
        RESERVED_SOCKET_BROKER.transition_slot_state(
            self.slot_id,
            ReservedSocketSlotState::LeaseRealBound,
            "reserved socket activate live binding",
        );
    }

    fn begin_live_binding(&mut self) {
        self.release_binding();
        RESERVED_SOCKET_BROKER.transition_slot_state(
            self.slot_id,
            ReservedSocketSlotState::LeasePendingRealBind,
            "reserved socket begin live binding",
        );
    }

    fn complete_live_binding(&mut self) {
        RESERVED_SOCKET_BROKER.transition_slot_state(
            self.slot_id,
            ReservedSocketSlotState::LeaseRealBound,
            "reserved socket complete live binding",
        );
    }
}

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
    eventually_value(timeout, || predicate().then_some(()), failure_message);
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

/// Writer that routes test log output through libtest's captured stdout path.
#[derive(Default)]
struct CapturedOutput {
    pending: Vec<u8>,
}

impl CapturedOutput {
    fn emit_complete_lines(&mut self) {
        while let Some(newline_index) = self.pending.iter().position(|byte| *byte == b'\n') {
            let line: Vec<u8> = self.pending.drain(..=newline_index).collect();
            Self::emit_bytes(&line);
        }
    }

    fn emit_bytes(bytes: &[u8]) {
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
            Self::emit_bytes(&remaining);
        }
        Ok(())
    }
}

impl Drop for CapturedOutput {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}
