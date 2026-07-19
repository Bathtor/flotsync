//! Process-local socket reservations for network tests.

use super::*;

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

/// Builds a Kompact system whose logs follow libtest output capture after applying extra config.
///
/// # Panics
///
/// Panics if the test logger cannot be installed or the Kompact system cannot be built.
pub fn build_test_kompact_system_with(configure: impl FnOnce(&mut KompactConfig)) -> KompactSystem {
    init_test_logger();

    let mut config = KompactConfig::default();
    configure(&mut config);
    kompact::test_support::configure_test_logger(&mut config);
    config.build().wait().expect("build KompactSystem")
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
    kompact::test_support::configure_test_logger(&mut config);
    let system = config.build().wait().expect("build KompactSystem");
    (system, timer)
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
                state.acquisition_in_progress_owner = Some(trace.owner_label.clone());
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
        state.acquisition_in_progress_owner = None;
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
        let absolute_deadline = Instant::now() + RESERVED_SOCKET_ACQUIRE_TIMEOUT;
        let trace = ReservationTrace::new(requested_tcp, requested_udp);
        let mut state = self.lock_state("socket reservation");
        state.waiting_tcp += requested_tcp;
        state.waiting_udp += requested_udp;
        let mut acquired = Vec::with_capacity(kinds.len());
        trace.request_started(&state.build_reservation_counters(&trace, &acquired, kinds));

        state = self.wait_to_be_allowed_to_reserve(state, &trace, &acquired, kinds);
        let mut progress_watch = ReservationProgressWatch::new(&state, Instant::now());

        // Each caller declares its full socket requirement up front. That keeps
        // the process-local broker free of hold-and-wait deadlocks.
        loop {
            let now = Instant::now();
            if now >= absolute_deadline {
                self.handle_reservation_failure(
                    state,
                    FailedReservation {
                        reason: ReservationFailureReason::AbsoluteTimeout {
                            timeout: RESERVED_SOCKET_ACQUIRE_TIMEOUT,
                        },
                        trace: &trace,
                        kinds,
                    },
                    acquired,
                );
            }
            if progress_watch.is_stalled(&state, now, RESERVED_SOCKET_NO_PROGRESS_TIMEOUT) {
                self.handle_reservation_failure(
                    state,
                    FailedReservation {
                        reason: ReservationFailureReason::NoProgress {
                            stall_timeout: RESERVED_SOCKET_NO_PROGRESS_TIMEOUT,
                        },
                        trace: &trace,
                        kinds,
                    },
                    acquired,
                );
            }

            if acquired.len() == kinds.len() {
                return self.finish_successful_reservation(state, acquired, &trace, kinds);
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
                return self.finish_successful_reservation(state, acquired, &trace, kinds);
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
                    state.record_bind_error(next_kind, &error);
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
    }

    fn finish_successful_reservation(
        &self,
        mut state: MutexGuard<'_, ReservedSocketBrokerState>,
        acquired: Vec<ReservedSocket>,
        trace: &ReservationTrace,
        kinds: &[ReservedSocketKind],
    ) -> ReservedSocketLease {
        state.record_progress(
            ReservedSocketProgressKind::ReservationFulfilled,
            format!("request_id={}", trace.request_id),
        );
        trace.completed(&state.build_reservation_counters(trace, &acquired, kinds));
        self.complete_reservation_phase(state, trace.requested.tcp, trace.requested.udp);
        ReservedSocketLease { reserved: acquired }
    }

    fn handle_reservation_failure(
        &self,
        mut state: MutexGuard<'_, ReservedSocketBrokerState>,
        failure: FailedReservation<'_>,
        acquired: Vec<ReservedSocket>,
    ) -> Never {
        let diagnostics =
            state.build_reservation_diagnostics(failure.trace, &acquired, failure.kinds);

        for reserved in acquired {
            state.reclaim_idle(
                reserved,
                ReservedSocketProgressKind::PartialAcquisitionReturned,
            );
        }
        self.complete_reservation_phase(
            state,
            failure.trace.requested.tcp,
            failure.trace.requested.udp,
        );

        let diagnostics_rendered = diagnostics.render();
        let has_socket_accounting_mismatch = diagnostics.has_socket_accounting_mismatch();
        assert!(
            !has_socket_accounting_mismatch,
            "reserved-socket broker detected a socket-accounting mismatch while reserving {} TCP and {} UDP sockets for one test: {diagnostics_rendered}",
            failure.trace.requested.tcp, failure.trace.requested.udp,
        );
        panic!(
            "reserved-socket broker failed to reserve {} TCP and {} UDP sockets for one test after {}: {diagnostics_rendered}",
            failure.trace.requested.tcp, failure.trace.requested.udp, failure.reason,
        );
    }

    fn release(&self, reserved: Vec<ReservedSocket>) {
        if reserved.is_empty() {
            return;
        }

        let mut state = self.lock_state("socket release");
        for reserved in reserved {
            state.reclaim_idle(reserved, ReservedSocketProgressKind::LeaseReleased);
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
    /// Hidden TCP sockets currently held for future test reservations.
    idle_tcp: Vec<ReservedSocket>,
    /// Hidden UDP sockets currently held for future test reservations.
    idle_udp: Vec<ReservedSocket>,
    /// Total TCP demand from waiters that has not yet received a lease.
    waiting_tcp: usize,
    /// Total UDP demand from waiters that has not yet received a lease.
    waiting_udp: usize,
    /// Thread currently allowed to acquire sockets from the OS.
    acquisition_in_progress_by: Option<ThreadId>,
    /// Human-readable owner label for the thread currently acquiring sockets.
    acquisition_in_progress_owner: Option<String>,
    /// Monotonic broker-local progress counter used by reservation stall watches.
    progress_epoch: u64,
    /// Last event that advanced [`Self::progress_epoch`].
    last_progress: Option<ReservedSocketProgress>,
    /// Last OS bind error observed while acquiring a reservation socket.
    last_bind_error: Option<String>,
    /// Known broker-owned sockets keyed by slot id for diagnostics and invariants.
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
            self.record_progress(
                ReservedSocketProgressKind::IdleSocketConsumed,
                format!(
                    "slot_id={}, kind={kind:?}, addr={}",
                    reserved.slot_id, reserved.addr
                ),
            );
            return Some(reserved);
        }
        None
    }

    fn reclaim_idle(
        &mut self,
        reserved: ReservedSocket,
        progress_kind: ReservedSocketProgressKind,
    ) {
        let progress_detail = format!(
            "slot_id={}, kind={:?}, addr={}",
            reserved.slot_id, reserved.kind, reserved.addr
        );
        self.transition_slot_state(reserved.slot_id, ReservedSocketSlotState::IdleHiddenBound);
        match reserved.kind {
            ReservedSocketKind::TcpListener => {
                if self.idle_tcp.len() < self.waiting_tcp {
                    self.idle_tcp.push(reserved);
                    self.record_progress(progress_kind, progress_detail);
                    return;
                }
            }
            ReservedSocketKind::UdpSocket => {
                if self.idle_udp.len() < self.waiting_udp {
                    self.idle_udp.push(reserved);
                    self.record_progress(progress_kind, progress_detail);
                    return;
                }
            }
        }
        self.remove_slot(reserved.slot_id);
        self.record_progress(progress_kind, progress_detail);
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
            let detail = format!(
                "slot_id={}, kind={kind:?}, addr={}",
                reserved.slot_id, reserved.addr
            );
            self.remove_slot(reserved.slot_id);
            self.record_progress(ReservedSocketProgressKind::IdleTrimmed, detail);
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
        self.record_progress(
            ReservedSocketProgressKind::OsSocketAcquired,
            format!(
                "slot_id={}, kind={:?}, addr={}",
                reserved.slot_id, reserved.kind, reserved.addr
            ),
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

    fn record_bind_error(&mut self, kind: ReservedSocketKind, error: &io::Error) {
        self.last_bind_error = Some(format!("kind={kind:?}, error={error}"));
    }

    fn record_progress(&mut self, kind: ReservedSocketProgressKind, detail: impl Into<String>) {
        self.progress_epoch = self.progress_epoch.saturating_add(1);
        self.last_progress = Some(ReservedSocketProgress {
            epoch: self.progress_epoch,
            kind,
            detail: detail.into(),
        });
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

    fn build_reservation_diagnostics(
        &self,
        trace: &ReservationTrace,
        acquired: &[ReservedSocket],
        kinds: &[ReservedSocketKind],
    ) -> ReservedSocketBrokerDiagnostics {
        let counters = self.build_reservation_counters(trace, acquired, kinds);
        ReservedSocketBrokerDiagnostics {
            request_id: trace.request_id,
            owner_label: trace.owner_label.clone(),
            requested: counters.requested,
            acquired: counters.acquired,
            next_kind: counters.next_kind,
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
            acquisition_in_progress_owner: self.acquisition_in_progress_owner.clone(),
            progress_epoch: self.progress_epoch,
            last_progress: self.last_progress.clone(),
            last_bind_error: self.last_bind_error.clone(),
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

/// Broker-local event category that proves a waiting reservation is not stuck.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReservedSocketProgressKind {
    /// A reservation request obtained all sockets it asked for.
    ReservationFulfilled,
    /// A test dropped a lease and returned at least one socket to the broker.
    LeaseReleased,
    /// The broker successfully acquired a hidden socket from the OS.
    OsSocketAcquired,
    /// A waiting reservation consumed one socket from an idle pool.
    IdleSocketConsumed,
    /// A failed reservation returned a partially acquired socket to the broker.
    PartialAcquisitionReturned,
    /// The broker closed an idle socket that no current local waiter needs.
    IdleTrimmed,
}

/// Last broker-local progress event visible to reservation stall diagnostics.
#[derive(Clone, Debug, Eq, PartialEq)]
struct ReservedSocketProgress {
    /// Value of the broker progress counter after this event.
    epoch: u64,
    /// Category of progress that occurred.
    kind: ReservedSocketProgressKind,
    /// Human-readable event payload, usually including slot id, kind, and address.
    detail: String,
}

/// Tracks whether one reservation request has observed broker-local progress.
#[derive(Debug)]
struct ReservationProgressWatch {
    /// Most recent broker progress epoch observed by this request.
    observed_epoch: u64,
    /// Instant at which the request last observed a new progress epoch.
    no_progress_since: Instant,
}

impl ReservationProgressWatch {
    fn new(state: &ReservedSocketBrokerState, now: Instant) -> Self {
        Self {
            observed_epoch: state.progress_epoch,
            no_progress_since: now,
        }
    }

    fn is_stalled(
        &mut self,
        state: &ReservedSocketBrokerState,
        now: Instant,
        stall_timeout: Duration,
    ) -> bool {
        if state.progress_epoch != self.observed_epoch {
            self.observed_epoch = state.progress_epoch;
            self.no_progress_since = now;
            return false;
        }
        now.duration_since(self.no_progress_since) >= stall_timeout
    }
}

/// Reason a reservation request stopped waiting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReservationFailureReason {
    /// The broker made no local progress for the configured stall timeout.
    NoProgress { stall_timeout: Duration },
    /// The last-resort absolute timeout fired before the reservation completed.
    AbsoluteTimeout { timeout: Duration },
}

impl Display for ReservationFailureReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoProgress { stall_timeout } => {
                write!(f, "no broker-local progress for {stall_timeout:?}")
            }
            Self::AbsoluteTimeout { timeout } => write!(f, "absolute timeout {timeout:?}"),
        }
    }
}

/// Failure context kept separate from partially acquired sockets during cleanup.
#[derive(Clone, Copy, Debug)]
struct FailedReservation<'a> {
    /// Why the reservation request gave up.
    reason: ReservationFailureReason,
    /// Request trace used for diagnostics and panic messages.
    trace: &'a ReservationTrace,
    /// Full requested socket sequence.
    kinds: &'a [ReservedSocketKind],
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
    /// Unique reservation request id.
    request_id: usize,
    /// Human-readable owner thread label for the failed request.
    owner_label: String,
    /// Total socket counts requested by the failed reservation.
    requested: SocketProtocolCounts,
    /// Socket counts acquired by the failed reservation before it gave up.
    acquired: SocketProtocolCounts,
    /// Next socket kind the failed reservation still needed.
    next_kind: Option<ReservedSocketKind>,
    /// Current unsatisfied demand across all broker waiters.
    waiting: SocketProtocolCounts,
    /// Current hidden sockets available in broker idle pools.
    idle: SocketProtocolCounts,
    /// Live sockets expected from the broker's internal slot records.
    expected_live: SocketProtocolCounts,
    /// Live TCP/UDP socket census reported by the OS for this process.
    actual_live: Result<SocketProtocolCounts, String>,
    /// Owner currently allowed to acquire sockets from the OS, if any.
    acquisition_in_progress_owner: Option<String>,
    /// Broker-local progress epoch captured at failure time.
    progress_epoch: u64,
    /// Last broker-local progress event captured at failure time.
    last_progress: Option<ReservedSocketProgress>,
    /// Last OS bind error captured at failure time.
    last_bind_error: Option<String>,
    /// Per-slot diagnostic summary of broker-owned sockets.
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
        let next_kind = self
            .next_kind
            .map_or_else(|| String::from("<complete>"), |kind| format!("{kind:?}"));
        let actual_live = match &self.actual_live {
            Ok(actual_live) => {
                format!(
                    "actual_live_tcp={}, actual_live_udp={}",
                    actual_live.tcp, actual_live.udp
                )
            }
            Err(error) => format!("actual_live_socket_census_failed={error}"),
        };
        let active_owner = self
            .acquisition_in_progress_owner
            .as_deref()
            .unwrap_or("<none>");
        let last_progress = self.last_progress.as_ref().map_or_else(
            || String::from("<none>"),
            |progress| {
                format!(
                    "epoch={}, kind={:?}, detail={}",
                    progress.epoch, progress.kind, progress.detail
                )
            },
        );
        let last_bind_error = self.last_bind_error.as_deref().unwrap_or("<none>");
        let local_holder_hint = if self.expected_live == SocketProtocolCounts::default()
            && self.last_bind_error.is_some()
        {
            "no local broker-owned sockets are live, so full cross-binary holder diagnosis is deferred to flotsync-7hp0"
        } else {
            "<none>"
        };
        format!(
            "\
request_id={}
owner={}
requested_tcp={}
requested_udp={}
acquired_tcp={}
acquired_udp={}
next_kind={}
waiting_tcp={}
waiting_udp={}
idle_tcp={}
idle_udp={}
active_acquisition_owner={}
progress_epoch={}
last_progress={}
last_bind_error={}
expected_live_tcp={}
expected_live_udp={}
{}
slot_summary={}
local_holder_hint={}",
            self.request_id,
            self.owner_label,
            self.requested.tcp,
            self.requested.udp,
            self.acquired.tcp,
            self.acquired.udp,
            next_kind,
            self.waiting.tcp,
            self.waiting.udp,
            self.idle.tcp,
            self.idle.udp,
            active_owner,
            self.progress_epoch,
            last_progress,
            last_bind_error,
            self.expected_live.tcp,
            self.expected_live.udp,
            actual_live,
            self.slot_summary,
            local_holder_hint,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn reserved_socket_for_test(
        slot_id: usize,
        kind: ReservedSocketKind,
        port: u16,
    ) -> ReservedSocket {
        ReservedSocket {
            slot_id,
            kind,
            addr: localhost(port),
            socket: None,
        }
    }

    fn last_progress_kind(state: &ReservedSocketBrokerState) -> ReservedSocketProgressKind {
        state
            .last_progress
            .as_ref()
            .expect("state should have recorded progress")
            .kind
    }

    #[test]
    fn reservation_progress_watch_resets_when_progress_advances() {
        let mut state = ReservedSocketBrokerState::default();
        let start = Instant::now();
        let mut watch = ReservationProgressWatch::new(&state, start);
        let stall_timeout = Duration::from_secs(10);

        assert!(!watch.is_stalled(&state, start + Duration::from_secs(9), stall_timeout));

        state.record_progress(
            ReservedSocketProgressKind::OsSocketAcquired,
            "test progress",
        );
        assert!(!watch.is_stalled(&state, start + Duration::from_secs(11), stall_timeout));
        assert!(!watch.is_stalled(&state, start + Duration::from_secs(20), stall_timeout));
        assert!(watch.is_stalled(&state, start + Duration::from_secs(21), stall_timeout));
    }

    #[test]
    fn reservation_progress_watch_reports_no_progress_stall() {
        let state = ReservedSocketBrokerState::default();
        let start = Instant::now();
        let mut watch = ReservationProgressWatch::new(&state, start);

        assert!(watch.is_stalled(
            &state,
            start + Duration::from_secs(10),
            Duration::from_secs(10)
        ));
    }

    #[test]
    fn bind_failures_do_not_count_as_progress() {
        let mut state = ReservedSocketBrokerState::default();
        let start = Instant::now();
        let mut watch = ReservationProgressWatch::new(&state, start);

        state.record_bind_error(
            ReservedSocketKind::UdpSocket,
            &io::Error::new(io::ErrorKind::AddrNotAvailable, "no UDP slots"),
        );

        assert_eq!(state.progress_epoch, 0);
        assert!(state.last_bind_error.is_some());
        assert!(watch.is_stalled(
            &state,
            start + Duration::from_secs(10),
            Duration::from_secs(10)
        ));
    }

    #[test]
    fn lease_release_counts_as_progress() {
        let mut state = ReservedSocketBrokerState::default();
        let reserved = reserved_socket_for_test(1, ReservedSocketKind::UdpSocket, 41_001);
        state.register_new_leased_slot(&reserved);
        let epoch_after_acquire = state.progress_epoch;

        state.reclaim_idle(reserved, ReservedSocketProgressKind::LeaseReleased);

        assert!(state.progress_epoch > epoch_after_acquire);
        assert_eq!(
            last_progress_kind(&state),
            ReservedSocketProgressKind::LeaseReleased
        );
    }

    #[test]
    fn idle_socket_consumption_counts_as_progress() {
        let mut state = ReservedSocketBrokerState::default();
        let reserved = reserved_socket_for_test(2, ReservedSocketKind::UdpSocket, 41_002);
        state.register_new_leased_slot(&reserved);
        state.waiting_udp = 1;
        state.reclaim_idle(reserved, ReservedSocketProgressKind::LeaseReleased);
        let epoch_before_take = state.progress_epoch;

        let consumed = state
            .take_idle(ReservedSocketKind::UdpSocket)
            .expect("idle UDP reservation should be available");

        assert_eq!(consumed.slot_id, 2);
        assert!(state.progress_epoch > epoch_before_take);
        assert_eq!(
            last_progress_kind(&state),
            ReservedSocketProgressKind::IdleSocketConsumed
        );
    }
}
