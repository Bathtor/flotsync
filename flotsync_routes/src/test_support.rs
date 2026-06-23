//! Shared test-only helpers for replication delivery integration tests.
//!
//! This module centralises the common transport bring-up and synchronous wait
//! helpers used by the delivery-domain test harnesses. It intentionally stays
//! small: semantic-owner probes still live next to the tests that use them.

use crate::{
    ExternalUdpSocketRegistration,
    RouteTransportActorMessage,
    TransportRouteKey,
    manager::{RouteTransportManager, configure_replication_runtime},
};
use flotsync_core::{MemberIdentity, member::IdentifierBuf};
use flotsync_io::{
    kompact::shutdown_system_bounded,
    prelude::{
        DriverConfig,
        IoBridge,
        IoBridgeHandle,
        IoDriverComponent,
        SocketId,
        UdpBindOptions,
        UdpIndication,
        UdpLocalBind,
        UdpOpenRequestId,
        UdpRequest,
    },
    test_support::{
        BufferedReceiver,
        ReservedSocketKind,
        ReservedSocketLease,
        UdpObserver,
        WAIT_TIMEOUT,
        bind_reserved_udp_socket,
        build_test_kompact_system_with,
        enable_bind_reuse_address,
        eventually_component_state,
        reserve_sockets,
        set_test_system_label,
        start_component,
    },
};
use flotsync_udpour::UDPourConfig;
use kompact::prelude::*;
use std::{
    collections::HashMap,
    net::SocketAddr,
    ops::{Deref, DerefMut},
    sync::{
        Arc,
        Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    time::Duration,
};

/// Longer timeout used by the semantic full-stack delivery tests.
pub const FULL_STACK_WAIT_TIMEOUT: Duration = Duration::from_secs(20);

/// Declared test budget for UDP sockets opened lazily by the manager.
///
/// The budget turns manager-owned ephemeral binds into pre-reserved test
/// socket slots so parallel tests keep full socket ownership up front.
pub struct ManagerOwnedUdpBindBudget {
    lease: ReservedSocketLease,
    available_slots: Vec<usize>,
    pending_slots: HashMap<UdpOpenRequestId, ClaimedManagerOwnedUdpBind>,
    live_slots: HashMap<SocketId, ClaimedManagerOwnedUdpBind>,
}

impl ManagerOwnedUdpBindBudget {
    /// Reserve one fixed amount of manager-owned UDP bind capacity for one
    /// test harness.
    pub fn new(slot_count: usize) -> Option<Arc<Mutex<Self>>> {
        if slot_count == 0 {
            return None;
        }

        let lease = reserve_sockets(&vec![ReservedSocketKind::UdpSocket; slot_count]);
        let available_slots = (0..slot_count).rev().collect();
        Some(Arc::new(Mutex::new(Self {
            lease,
            available_slots,
            pending_slots: HashMap::new(),
            live_slots: HashMap::new(),
        })))
    }

    /// Claim one reservation slot for one manager-owned bind request before
    /// the real bridge bind is issued.
    pub fn begin_bind(
        &mut self,
        request_id: UdpOpenRequestId,
        requested_local_addr: SocketAddr,
    ) -> Result<SocketAddr, String> {
        let Some(slot_index) = self.available_slots.pop() else {
            return Err(format!(
                "no declared manager-owned UDP bind budget remained for local_addr={requested_local_addr}; declared_slots={}, pending_binds={}, live_binds={}",
                self.lease.len(),
                self.pending_slots.len(),
                self.live_slots.len(),
            ));
        };
        let reserved_bind_addr = self.lease.addr(slot_index);
        self.lease.begin_live_binding(slot_index);
        let previous = self.pending_slots.insert(
            request_id,
            ClaimedManagerOwnedUdpBind {
                slot_index,
                requested_local_addr,
                reserved_bind_addr,
            },
        );
        debug_assert!(
            previous.is_none(),
            "manager-owned UDP bind request ids must be unique in tests"
        );
        Ok(reserved_bind_addr)
    }

    /// Promote one pending bind reservation into one live manager-owned UDP
    /// socket.
    pub fn complete_bind(
        &mut self,
        request_id: UdpOpenRequestId,
        socket_id: SocketId,
        actual_local_addr: SocketAddr,
    ) -> Result<(), String> {
        let claimed = self.pending_slots.remove(&request_id).ok_or_else(|| {
            format!("manager-owned UDP bind budget lost pending request_id={request_id:?}")
        })?;
        if actual_local_addr != claimed.reserved_bind_addr {
            let message = format!(
                "manager-owned UDP bind request_id={request_id:?} bound at {actual_local_addr}, but reserved slot {} expected {}; requested_local_addr={}",
                claimed.slot_index, claimed.reserved_bind_addr, claimed.requested_local_addr,
            );
            let restore_result = self.restore_slot(&claimed);
            return match restore_result {
                Ok(()) => Err(message),
                Err(error) => Err(format!(
                    "{message}; additionally failed to restore slot: {error}"
                )),
            };
        }
        self.lease.complete_live_binding(claimed.slot_index);
        let previous = self.live_slots.insert(socket_id, claimed);
        debug_assert!(
            previous.is_none(),
            "manager-owned UDP bind socket ids must be unique in tests"
        );
        Ok(())
    }

    /// Restore one failed pending manager-owned bind request back into the
    /// available budget.
    pub fn fail_bind(&mut self, request_id: UdpOpenRequestId) -> Result<(), String> {
        let claimed = self.pending_slots.remove(&request_id).ok_or_else(|| {
            format!("manager-owned UDP bind budget lost failed request_id={request_id:?}")
        })?;
        self.restore_slot(&claimed)
    }

    /// Restore one closed live manager-owned UDP socket back into the
    /// available budget.
    pub fn release_live(&mut self, socket_id: SocketId) -> Result<bool, String> {
        let Some(claimed) = self.live_slots.remove(&socket_id) else {
            return Ok(false);
        };
        self.restore_slot(&claimed)?;
        Ok(true)
    }

    /// Return one claimed reservation slot to the available manager-owned
    /// bind budget.
    fn restore_slot(&mut self, claimed: &ClaimedManagerOwnedUdpBind) -> Result<(), String> {
        self.lease
            .rebind_binding(claimed.slot_index)
            .map_err(|error| {
                format!(
                    "rebind manager-owned UDP bind budget slot {} for local_addr={}: {error}",
                    claimed.slot_index, claimed.requested_local_addr
                )
            })?;
        self.available_slots.push(claimed.slot_index);
        Ok(())
    }
}

/// Reservation slot currently claimed by one manager-owned UDP bind.
struct ClaimedManagerOwnedUdpBind {
    slot_index: usize,
    requested_local_addr: SocketAddr,
    reserved_bind_addr: SocketAddr,
}

/// Shared transport-only harness core for delivery-domain tests.
///
/// This owns the driver, bridge, manager, and observer stack. Semantic-owner
/// harnesses layer ingress, probes, and route-discovery fixtures on top.
pub struct TransportHarnessCore {
    system: KompactSystem,
    reserved_socket_lease: Option<Arc<Mutex<ReservedSocketLease>>>,
    _manager_owned_udp_bind_budget: Option<Arc<Mutex<ManagerOwnedUdpBindBudget>>>,
    extra_reserved_socket_slots: usize,
    extra_reserved_socket_slot_offset: usize,
    external_socket_binding_released: AtomicBool,
    driver: Arc<Component<IoDriverComponent>>,
    bridge: Arc<Component<IoBridge>>,
    manager: Arc<Component<RouteTransportManager>>,
    manager_ref: ActorRefStrong<RouteTransportActorMessage<TransportRouteKey>>,
    observer: Arc<Component<UdpObserver>>,
    observer_rx: BufferedReceiver<UdpIndication>,
}

impl TransportHarnessCore {
    /// Create one transport-only harness core with explicit budgets for both
    /// pre-bound raw test sockets and lazy manager-owned UDP sockets.
    pub fn with_socket_budgets(
        system: KompactSystem,
        udpour_config: UDPourConfig,
        reserve_external_udp_socket: bool,
        extra_socket_kinds: &[ReservedSocketKind],
        manager_owned_udp_sockets: usize,
    ) -> Self {
        let mut reserved_socket_kinds =
            Vec::with_capacity(extra_socket_kinds.len() + usize::from(reserve_external_udp_socket));
        if reserve_external_udp_socket {
            reserved_socket_kinds.push(ReservedSocketKind::UdpSocket);
        }
        reserved_socket_kinds.extend_from_slice(extra_socket_kinds);
        let reserved_socket_lease = if reserved_socket_kinds.is_empty() {
            None
        } else {
            Some(Arc::new(Mutex::new(reserve_sockets(
                &reserved_socket_kinds,
            ))))
        };
        let manager_owned_udp_bind_budget =
            ManagerOwnedUdpBindBudget::new(manager_owned_udp_sockets);
        let driver = system.create(|| IoDriverComponent::new(DriverConfig::default()));
        let driver_for_bridge = driver.clone();
        let bridge = system.create(move || IoBridge::new(&driver_for_bridge));
        let bridge_handle = IoBridgeHandle::from_component(&bridge);
        let manager_system = system.clone();
        let manager_owned_udp_bind_budget_for_manager = manager_owned_udp_bind_budget.clone();
        let manager = system.create(move || {
            RouteTransportManager::new_with_test_manager_owned_udp_bind_budget(
                manager_system,
                bridge_handle,
                udpour_config,
                manager_owned_udp_bind_budget_for_manager,
            )
        });
        let manager_ref = manager
            .actor_ref()
            .hold()
            .expect("route transport manager must expose a strong actor ref in tests");
        let (observer_tx, observer_rx) = mpsc::channel();
        let observer = system.create(move || UdpObserver::new(observer_tx));

        Self {
            system,
            reserved_socket_lease,
            _manager_owned_udp_bind_budget: manager_owned_udp_bind_budget,
            extra_reserved_socket_slots: extra_socket_kinds.len(),
            extra_reserved_socket_slot_offset: usize::from(reserve_external_udp_socket),
            external_socket_binding_released: AtomicBool::new(false),
            driver,
            bridge,
            manager,
            manager_ref,
            observer,
            observer_rx: BufferedReceiver::new(observer_rx),
        }
    }

    /// Start the transport core and connect bridge UDP delivery into the
    /// manager and observer.
    pub fn start(&self) {
        let udp_connect_handle = IoBridgeHandle::from_component(&self.bridge);
        start_component(&self.system, &self.driver);
        start_component(&self.system, &self.bridge);
        block_on(udp_connect_handle.connect_udp(&self.manager))
            .expect("bridge must connect to route transport manager");
        block_on(udp_connect_handle.connect_udp(&self.observer))
            .expect("bridge must connect to UDP observer");
        start_component(&self.system, &self.manager);
        start_component(&self.system, &self.observer);
    }

    /// Access the underlying Kompact system for creating extra components.
    pub fn system(&self) -> &KompactSystem {
        &self.system
    }

    /// Access the route-transport manager component for port wiring or direct
    /// state inspection in tests.
    pub fn manager(&self) -> &Arc<Component<RouteTransportManager>> {
        &self.manager
    }

    /// Access the manager actor ref for submit asks.
    pub fn manager_ref(&self) -> ActorRefStrong<RouteTransportActorMessage<TransportRouteKey>> {
        self.manager_ref.clone()
    }

    /// Wait for one externally bound UDP socket owned by the observer.
    pub fn bind_external_socket(
        &self,
        bind: UdpLocalBind,
        timeout: Duration,
    ) -> (SocketId, SocketAddr) {
        let reserved_socket_lease = self
            .reserved_socket_lease
            .as_ref()
            .expect("external socket bind requires one declared reserved socket lease");
        let bind = match bind {
            UdpLocalBind::Exact(addr) if addr.port() == 0 => UdpLocalBind::Exact(
                reserved_socket_lease
                    .lock()
                    .expect("external socket lease lock")
                    .addr(0),
            ),
            other => other,
        };
        let request_id = UdpOpenRequestId::new();
        self.observer.on_definition(|component| {
            component.udp.trigger(UdpRequest::Bind {
                request_id,
                bind,
                options: UdpBindOptions::default(),
            });
        });
        match self.observer_rx.recv_matching_or_fail(
            timeout,
            |event| {
                matches!(
                    event,
                    UdpIndication::Bound {
                        request_id: indicated_request_id,
                        ..
                    } if *indicated_request_id == request_id
                )
            },
            |event| match event {
                UdpIndication::BindFailed {
                    request_id: indicated_request_id,
                    local_addr,
                    reason,
                } if *indicated_request_id == request_id => Some(format!(
                    "external UDP socket bind failed for request {request_id:?} at {local_addr}: {reason:?}"
                )),
                _ => None,
            },
        ) {
            UdpIndication::Bound {
                request_id: indicated_request_id,
                socket_id,
                local_addr,
            } => {
                assert_eq!(indicated_request_id, request_id);
                self.release_external_socket_binding(local_addr);
                (socket_id, local_addr)
            }
            other => unreachable!("filtered to Bound, got {other:?}"),
        }
    }

    /// Bind one pre-reserved raw UDP socket owned directly by the test.
    ///
    /// `reservation_index` addresses the extra reservation slots that were
    /// requested via [`with_reserved_socket_kinds`](Self::with_reserved_socket_kinds).
    /// The externally managed route-transport socket always occupies slot `0`
    /// internally and is therefore not visible through this helper.
    pub fn bind_reserved_udp_socket(&self, reservation_index: usize) -> BoundReservedUdpSocket {
        assert!(
            reservation_index < self.extra_reserved_socket_slots,
            "reserved UDP socket index {reservation_index} is out of range for {} extra slots",
            self.extra_reserved_socket_slots,
        );
        let lease_index = reservation_index + self.extra_reserved_socket_slot_offset;
        let reserved_socket_lease = self
            .reserved_socket_lease
            .as_ref()
            .expect("reserved raw UDP socket bind requires one declared reserved socket lease");
        let socket = {
            let mut reserved_socket_lease = reserved_socket_lease
                .lock()
                .expect("reserved socket lease lock");
            let socket = bind_reserved_udp_socket(&reserved_socket_lease, lease_index)
                .expect("bind reserved raw UDP socket");
            reserved_socket_lease.activate_live_binding(lease_index);
            socket
        };

        BoundReservedUdpSocket {
            socket: Some(socket),
            reserved_socket_lease: Arc::clone(reserved_socket_lease),
            lease_index,
        }
    }

    /// Mark the external route-transport socket reservation as live once the
    /// bridge has bound it.
    fn release_external_socket_binding(&self, local_addr: SocketAddr) {
        if self
            .external_socket_binding_released
            .load(Ordering::Relaxed)
        {
            return;
        }
        let Some(reserved_socket_lease) = self.reserved_socket_lease.as_ref() else {
            panic!("external socket binding release requires one declared reserved socket lease");
        };
        let mut reserved_socket_lease = reserved_socket_lease
            .lock()
            .expect("external socket lease lock");
        if reserved_socket_lease.addr(0) != local_addr {
            return;
        }
        reserved_socket_lease.activate_live_binding(0);
        self.external_socket_binding_released
            .store(true, Ordering::Relaxed);
    }

    /// Rebind the hidden external-socket reservation during harness teardown.
    fn rebind_external_socket_binding(&self) {
        if !self
            .external_socket_binding_released
            .load(Ordering::Relaxed)
        {
            return;
        }
        self.reserved_socket_lease
            .as_ref()
            .expect("external socket binding rebind requires one declared reserved socket lease")
            .lock()
            .expect("external socket lease lock")
            .rebind_binding(0)
            .expect("rebind reserved external UDP socket");
        self.external_socket_binding_released
            .store(false, Ordering::Relaxed);
    }

    /// Register one externally bound socket with the route-transport manager
    /// before tests publish routes that rely on reusing it.
    pub fn wait_for_manager_external_socket_binding(
        &self,
        socket_id: SocketId,
        local_addr: SocketAddr,
        timeout: Duration,
    ) {
        let registration = ExternalUdpSocketRegistration {
            socket_id,
            local_addr,
        };
        let registration_result = self
            .manager_ref
            .ask_with(|promise| {
                RouteTransportActorMessage::RegisterExternalUdpSocket(Ask::new(
                    promise,
                    registration,
                ))
            })
            .wait_timeout(timeout)
            .expect("timed out registering external UDP socket with route transport manager");
        if let Err(error) = registration_result {
            panic!("external UDP socket registration failed: {error}");
        }
        eventually_component_state(
            timeout,
            &self.manager,
            |component| component.knows_external_udp_socket_binding(socket_id, local_addr),
            format_args!(
                "timed out waiting for route transport manager to observe external UDP socket {socket_id:?} at {local_addr}"
            ),
        );
    }

    /// Wait for the next UDP bind indication observed by the bridge observer.
    pub fn wait_for_new_bound_socket(&self, timeout: Duration) -> (SocketId, SocketAddr) {
        match self.observer_rx.recv_matching_or_fail(
            timeout,
            |event| matches!(event, UdpIndication::Bound { .. }),
            |event| match event {
                UdpIndication::BindFailed {
                    request_id,
                    local_addr,
                    reason,
                } => Some(format!(
                    "route-transport UDP bind request {request_id:?} failed at {local_addr}: {reason:?}"
                )),
                _ => None,
            },
        ) {
            UdpIndication::Bound {
                socket_id,
                local_addr,
                ..
            } => (socket_id, local_addr),
            other => unreachable!("filtered to Bound, got {other:?}"),
        }
    }

    /// Access the buffered observer event stream for specialised assertions in
    /// the manager tests.
    pub fn observer_rx(&self) -> &BufferedReceiver<UdpIndication> {
        &self.observer_rx
    }
}

/// One raw UDP socket bound against one reserved test-harness lease slot.
///
/// Dropping this wrapper closes the live socket and re-binds the hidden
/// reservation so later teardown steps keep ownership of the port.
pub struct BoundReservedUdpSocket {
    socket: Option<std::net::UdpSocket>,
    reserved_socket_lease: Arc<Mutex<ReservedSocketLease>>,
    lease_index: usize,
}

impl Deref for BoundReservedUdpSocket {
    type Target = std::net::UdpSocket;

    fn deref(&self) -> &Self::Target {
        self.socket
            .as_ref()
            .expect("reserved UDP socket must remain available while borrowed")
    }
}

impl DerefMut for BoundReservedUdpSocket {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.socket
            .as_mut()
            .expect("reserved UDP socket must remain available while borrowed")
    }
}

impl Drop for BoundReservedUdpSocket {
    fn drop(&mut self) {
        drop(self.socket.take());
        let rebind_result = self
            .reserved_socket_lease
            .lock()
            .expect("reserved socket lease lock")
            .rebind_binding(self.lease_index);
        if let Err(error) = rebind_result
            && !std::thread::panicking()
        {
            panic!(
                "rebind reserved raw UDP socket at slot {}: {error}",
                self.lease_index
            );
        }
    }
}

impl Drop for TransportHarnessCore {
    fn drop(&mut self) {
        let _ = self
            .system
            .kill_notify(self.observer.clone())
            .wait_timeout(WAIT_TIMEOUT);
        let _ = self
            .system
            .kill_notify(self.manager.clone())
            .wait_timeout(WAIT_TIMEOUT);
        let _ = self
            .system
            .kill_notify(self.bridge.clone())
            .wait_timeout(WAIT_TIMEOUT);
        let _ = self
            .system
            .kill_notify(self.driver.clone())
            .wait_timeout(WAIT_TIMEOUT);
        shutdown_system_bounded(self.system.clone(), WAIT_TIMEOUT, true);
        self.rebind_external_socket_binding();
    }
}

/// Build a Kompact system for the semantic delivery full-stack tests.
pub fn build_delivery_test_system() -> KompactSystem {
    build_delivery_test_system_with(|_| {})
}

/// Build a Kompact system for semantic delivery tests with extra config.
pub fn build_delivery_test_system_with(
    configure: impl FnOnce(&mut KompactConfig),
) -> KompactSystem {
    build_test_kompact_system_with(|config| {
        set_test_system_label(config, "replication-delivery-test-system");
        enable_bind_reuse_address(config);
        configure_replication_runtime(config);
        configure(config);
    })
}

/// Build the shared `UDPour` config used by the semantic full-stack tests.
pub fn default_udpour_config() -> UDPourConfig {
    UDPourConfig::default()
}

/// Build one loopback member identity for tests from the supplied path
/// segments.
pub fn member_identity(segments: &[&str]) -> MemberIdentity {
    let mut identifier = IdentifierBuf::new();
    for segment in segments {
        identifier
            .push_checked((*segment).to_owned())
            .expect("test member identifier segment must be valid");
    }
    identifier.into_identifier()
}
