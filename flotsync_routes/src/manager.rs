//! UDP-first route-transport manager.
//!
//! This component now owns the concrete UDP route path end to end:
//!
//! - opening UDP sockets on demand through the shared bridge port
//! - creating one child `UDPour` runtime per live UDP socket
//! - serializing `FlotsyncSerializable` payloads into pooled `IoPayload`
//! - resolving directed route-transport submits via actor `Ask`
//!
//! TCP route handling is intentionally deferred via `TODO(flotsync-638)`.

use super::{
    ConnectionFailureReason,
    ConnectionInfoIndication,
    ConnectionInfoPort,
    DatagramRouteScope,
    Debug,
    ExternalUdpSocketRegistration,
    ExternalUdpSocketRegistrationError,
    Hash,
    IString,
    InboundTransportMeta,
    RouteEndpointBinding,
    RouteEndpointLifecycle,
    RouteEndpointLifecyclePort,
    RouteEndpointUnavailableReason,
    RouteSendId,
    RouteTransportActorMessage,
    RouteTransportInboundDeliver,
    RouteTransportNackReason,
    RouteTransportPort,
    RouteTransportSend,
    RouteTransportSubmitResult,
    TcpRouteKey,
    TransportRouteKey,
    UdpRouteKey,
};
#[cfg(any(test, feature = "test-support"))]
use crate::test_support::ManagerOwnedUdpBindBudget;
use flotsync_io::prelude::{
    IoBridgeHandle,
    IoPayload,
    OpenFailureReason,
    SendFailureReason,
    SocketId,
    UdpBindOptions,
    UdpCloseReason,
    UdpIndication,
    UdpLocalBind,
    UdpOpenRequestId,
    UdpPort,
    UdpRequest,
    UdpSocketOption,
};
use flotsync_messages::serialisation::{FlotsyncSerializeError, encode_message_payload};
use flotsync_udpour::{
    UDPourComponent,
    UDPourComponentMessage,
    UDPourConfig,
    UDPourDeliver,
    UDPourPort,
    UDPourSend,
    UDPourSendFailureReason,
    UDPourSubmitResult,
};
use flotsync_utils::OptionExt as _;
use kompact::{config::UsizeValue, kompact_config, prelude::*};
#[cfg(any(test, feature = "test-support"))]
use std::sync::Mutex;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
};

/// Concrete route-transport manager for the current UDP/TCP key type.
///
/// UDP is fully owned here via child `UDPourComponent`s. TCP is left
/// as a follow-up.
#[derive(ComponentDefinition)]
pub struct RouteTransportManager {
    ctx: ComponentContext<Self>,
    inbound_port: ProvidedPort<TransportRouteTransportPort>,
    connection_info_port: ProvidedPort<TransportConnectionInfoPort>,
    /// Accepted endpoint lifecycle after route transport has applied it locally.
    ///
    /// Republishing from here preserves registration-before-use ordering for
    /// downstream route users such as route establishment.
    route_endpoint_lifecycle_provided: ProvidedPort<RouteEndpointLifecyclePort>,
    /// Raw endpoint-owner lifecycle that drives route-transport registration.
    route_endpoint_lifecycle_required: RequiredPort<RouteEndpointLifecyclePort>,
    udp_bridge_port: RequiredPort<UdpPort>,
    /// Inbound `UDPour` deliveries from child runtimes.
    udpour_port: RequiredPort<UDPourPort>,
    /// Concrete system handle needed for direct `trigger_i` startup queueing.
    system: KompactSystem,
    /// Shared bridge handle used to open transport resources on demand.
    bridge: IoBridgeHandle,
    /// Runtime configuration shared by every live UDP `UDPour` child.
    udpour_config: UDPourConfig,
    /// Policy that decides when a bound UDP socket gains its `UDPour` child.
    udp_activation_policy: UdpActivationPolicy,
    /// UDP local sockets currently waiting for a bind result.
    ///
    /// This is keyed by the reusable local-socket shape rather than the full
    /// route key so multiple targets can converge onto one child runtime.
    udp_open_sockets: HashMap<UdpSocketKey, PendingUdpSocketOpen>,
    /// Correlates bridge-local UDP open ids back to the socket key being opened.
    udp_open_requests: HashMap<UdpOpenRequestId, UdpSocketKey>,
    /// Sockets that are already bound but whose `UDPour` child is still starting.
    udp_starting_sockets: HashMap<UdpSocketKey, StartingUdpSocketHandle>,
    /// Bound UDP sockets that exist on the bridge but do not yet have a `UDPour` child.
    udp_dormant_sockets: HashMap<UdpSocketKey, DormantUdpSocketHandle>,
    /// Live sockets currently waiting for `Broadcast(true)` to apply.
    udp_broadcast_configurations: HashMap<SocketId, UdpSocketKey>,
    /// Reverse lookup from driver socket id back to the live local-socket key.
    udp_socket_ids: HashMap<SocketId, UdpSocketKey>,
    /// Current live UDP socket table keyed by the reusable local-socket key.
    udp_sockets: HashMap<UdpSocketKey, LiveUdpSocketHandle>,
    /// Current live TCP route table keyed by the concrete TCP route key.
    ///
    /// TODO(flotsync-638): Populate this once the TCP backend is implemented.
    #[allow(dead_code)]
    tcp_routes: HashMap<TcpRouteKey, LiveTcpRouteHandle>,
    /// Logical sends that are still waiting for one route-transport outcome.
    pending_sends: HashMap<RouteSendId, PendingRouteSend>,
    /// Test-only declared budget for manager-owned lazy UDP binds.
    #[cfg(any(test, feature = "test-support"))]
    test_manager_owned_udp_bind_budget: Option<Arc<Mutex<ManagerOwnedUdpBindBudget>>>,
}

impl RouteTransportManager {
    /// Creates one manager around the shared bridge handle and UDP child config.
    #[must_use]
    pub fn new(system: KompactSystem, bridge: IoBridgeHandle, udpour_config: UDPourConfig) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            inbound_port: ProvidedPort::uninitialised(),
            connection_info_port: ProvidedPort::uninitialised(),
            route_endpoint_lifecycle_provided: ProvidedPort::uninitialised(),
            route_endpoint_lifecycle_required: RequiredPort::uninitialised(),
            udp_bridge_port: RequiredPort::uninitialised(),
            udpour_port: RequiredPort::uninitialised(),
            system,
            bridge,
            udpour_config,
            udp_activation_policy: UdpActivationPolicy::default(),
            udp_open_sockets: HashMap::new(),
            udp_open_requests: HashMap::new(),
            udp_starting_sockets: HashMap::new(),
            udp_dormant_sockets: HashMap::new(),
            udp_broadcast_configurations: HashMap::new(),
            udp_socket_ids: HashMap::new(),
            udp_sockets: HashMap::new(),
            tcp_routes: HashMap::new(),
            pending_sends: HashMap::new(),
            #[cfg(any(test, feature = "test-support"))]
            test_manager_owned_udp_bind_budget: None,
        }
    }

    /// Return the route-endpoint lifecycle port reference used by tests to inject endpoint state.
    #[cfg(test)]
    fn route_endpoint_lifecycle_port(&mut self) -> RequiredRef<RouteEndpointLifecyclePort> {
        self.route_endpoint_lifecycle_required.share()
    }

    /// Creates one manager with one explicit test-only budget for lazy
    /// manager-owned UDP binds.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn new_with_test_manager_owned_udp_bind_budget(
        system: KompactSystem,
        bridge: IoBridgeHandle,
        udpour_config: UDPourConfig,
        test_manager_owned_udp_bind_budget: Option<Arc<Mutex<ManagerOwnedUdpBindBudget>>>,
    ) -> Self {
        let mut manager = Self::new(system, bridge, udpour_config);
        manager.test_manager_owned_udp_bind_budget = test_manager_owned_udp_bind_budget;
        manager
    }

    fn load_udp_activation_policy(&self) -> UdpActivationPolicy {
        let raw = match self
            .ctx
            .config()
            .read_or_default(&config_keys::UDP_ACTIVATION_POLICY)
        {
            Ok(value) => value,
            Err(error) => {
                warn!(
                    self.log(),
                    "Failed to load route-transport UDP activation policy from {}: {}. Falling back to OnBind",
                    config_keys::UDP_ACTIVATION_POLICY.key,
                    error
                );
                return UdpActivationPolicy::OnBind;
            }
        };
        match raw {
            0 => UdpActivationPolicy::OnBind,
            1 => UdpActivationPolicy::OnFirstUse,
            other => {
                warn!(
                    self.log(),
                    "Unknown route-transport UDP activation policy value {} in {}. Falling back to OnBind",
                    other,
                    config_keys::UDP_ACTIVATION_POLICY.key
                );
                UdpActivationPolicy::OnBind
            }
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    fn test_manager_owned_udp_bind_policy(
        &mut self,
        request_id: UdpOpenRequestId,
        socket_key: UdpSocketKey,
        requested_bind: UdpLocalBind,
    ) -> UdpLocalBind {
        let requested_local_addr = requested_bind.resolve_local_addr();
        let system_label = self
            .ctx
            .config()
            .read_or_default(&kompact::config_keys::system::LABEL)
            .unwrap_or_else(|_| String::from("<unlabelled-route-transport-test-system>"));
        assert!(
            requested_local_addr == socket_key.local_addr,
            "route-transport test system '{system_label}' saw inconsistent manager-owned UDP bind state for {socket_key:?}; requested_bind resolved to {requested_local_addr}"
        );
        assert!(
            requested_local_addr.port() == 0,
            "route-transport test system '{system_label}' attempted manager-owned exact UDP bind for {socket_key:?}; the test bind budget only supports port-zero manager-owned binds"
        );
        let Some(test_manager_owned_udp_bind_budget) = &self.test_manager_owned_udp_bind_budget
        else {
            panic!(
                "route-transport test system '{system_label}' attempted undeclared manager-owned UDP bind for {socket_key:?}; declare manager_owned_udp_sockets up front in the harness"
            );
        };
        let reserved_bind_addr = test_manager_owned_udp_bind_budget
            .lock()
            .expect("manager-owned UDP bind budget lock")
            .begin_bind(request_id, requested_local_addr);
        match reserved_bind_addr {
            Ok(reserved_bind_addr) => UdpLocalBind::Exact(reserved_bind_addr),
            Err(error) => panic!(
                "route-transport test system '{system_label}' exhausted declared manager-owned UDP bind budget for {socket_key:?}: {error}"
            ),
        }
    }

    #[cfg(not(any(test, feature = "test-support")))]
    #[allow(
        clippy::unused_self,
        reason = "Test builds inspect component state here; non-test builds keep the same call shape."
    )]
    fn test_manager_owned_udp_bind_policy(
        &mut self,
        _request_id: UdpOpenRequestId,
        _socket_key: UdpSocketKey,
        requested_bind: UdpLocalBind,
    ) -> UdpLocalBind {
        requested_bind
    }

    #[cfg(any(test, feature = "test-support"))]
    fn complete_test_manager_owned_udp_bind(
        &mut self,
        request_id: UdpOpenRequestId,
        socket_id: SocketId,
        local_addr: SocketAddr,
    ) {
        let Some(test_manager_owned_udp_bind_budget) = &self.test_manager_owned_udp_bind_budget
        else {
            return;
        };
        let complete_result = test_manager_owned_udp_bind_budget
            .lock()
            .expect("manager-owned UDP bind budget lock")
            .complete_bind(request_id, socket_id, local_addr);
        if let Err(error) = complete_result {
            panic!("complete manager-owned UDP bind budget accounting: {error}");
        }
    }

    #[cfg(not(any(test, feature = "test-support")))]
    #[allow(
        clippy::unused_self,
        reason = "Test builds inspect component state here; non-test builds keep the same call shape."
    )]
    fn complete_test_manager_owned_udp_bind(
        &mut self,
        _request_id: UdpOpenRequestId,
        _socket_id: SocketId,
        _local_addr: SocketAddr,
    ) {
    }

    #[cfg(any(test, feature = "test-support"))]
    fn fail_test_manager_owned_udp_bind(&mut self, request_id: UdpOpenRequestId) {
        let Some(test_manager_owned_udp_bind_budget) = &self.test_manager_owned_udp_bind_budget
        else {
            return;
        };
        let fail_result = test_manager_owned_udp_bind_budget
            .lock()
            .expect("manager-owned UDP bind budget lock")
            .fail_bind(request_id);
        if let Err(error) = fail_result {
            panic!("restore failed manager-owned UDP bind budget slot: {error}");
        }
    }

    #[cfg(not(any(test, feature = "test-support")))]
    #[allow(
        clippy::unused_self,
        reason = "Test builds inspect component state here; non-test builds keep the same call shape."
    )]
    fn fail_test_manager_owned_udp_bind(&mut self, _request_id: UdpOpenRequestId) {}

    #[cfg(any(test, feature = "test-support"))]
    fn release_test_manager_owned_udp_bind(&mut self, socket_id: SocketId) {
        let Some(test_manager_owned_udp_bind_budget) = &self.test_manager_owned_udp_bind_budget
        else {
            return;
        };
        let release_result = test_manager_owned_udp_bind_budget
            .lock()
            .expect("manager-owned UDP bind budget lock")
            .release_live(socket_id);
        if let Err(error) = release_result {
            panic!("restore closed manager-owned UDP bind budget slot: {error}");
        }
    }

    #[cfg(not(any(test, feature = "test-support")))]
    #[allow(
        clippy::unused_self,
        reason = "Test builds inspect component state here; non-test builds keep the same call shape."
    )]
    fn release_test_manager_owned_udp_bind(&mut self, _socket_id: SocketId) {}

    fn handle_submit(
        &mut self,
        ask: Ask<TransportRouteTransportSend, TransportRouteTransportSubmitResult>,
    ) -> HandlerResult {
        let (promise, send) = ask.take();
        let send_id = send.send_id;
        match send.route.coverage_key {
            TransportRouteKey::Udp(route) => {
                let previous = self.pending_sends.insert(
                    send_id,
                    PendingRouteSend {
                        send,
                        submit_promise: Some(promise),
                    },
                );
                debug_assert!(
                    previous.is_none(),
                    "route transport submit replaced pending send_id={send_id:?}"
                );
                self.handle_udp_route_send(send_id, route);
            }
            TransportRouteKey::Tcp(_) => {
                todo!("TODO(flotsync-638): implement TCP route transport backend")
            }
        }
        Handled::OK
    }

    fn handle_udp_route_send(&mut self, send_id: RouteSendId, route: UdpRouteKey) {
        let socket_key = UdpSocketKey::for_route(route);

        if self.udp_sockets.contains_key(&socket_key) {
            self.handle_live_udp_send(send_id, route, socket_key);
            return;
        }

        if let Some(starting) = self.udp_starting_sockets.get_mut(&socket_key) {
            starting.queued_sends.push(QueuedUdpSend { send_id, route });
            return;
        }

        if let Some(open) = self.udp_open_sockets.get_mut(&socket_key) {
            open.queued_sends.push(QueuedUdpSend { send_id, route });
            return;
        }

        if let Some(dormant) = self.udp_dormant_sockets.remove(&socket_key) {
            self.begin_starting_udp_socket(
                socket_key,
                dormant.socket_id,
                UdpSocketStartOrigin::ExternalDormant,
                vec![QueuedUdpSend { send_id, route }],
            );
            return;
        }

        let request_id = UdpOpenRequestId::default();
        let bind = self.test_manager_owned_udp_bind_policy(
            request_id,
            socket_key,
            socket_key.bind_policy(),
        );
        self.udp_open_sockets.insert(
            socket_key,
            PendingUdpSocketOpen {
                queued_sends: vec![QueuedUdpSend { send_id, route }],
            },
        );
        self.udp_open_requests.insert(request_id, socket_key);
        self.udp_bridge_port.trigger(UdpRequest::Bind {
            request_id,
            bind,
            options: UdpBindOptions::default(),
        });
    }

    fn handle_udp_bound(
        &mut self,
        request_id: UdpOpenRequestId,
        socket_id: SocketId,
        local_addr: SocketAddr,
    ) {
        let Some(socket_key) = self.udp_open_requests.remove(&request_id) else {
            trace!(
                self.log(),
                "route transport ignored externally bound UDP socket {socket_id:?} at {local_addr}"
            );
            return;
        };
        self.complete_test_manager_owned_udp_bind(request_id, socket_id, local_addr);
        let Some(open) = self.udp_open_sockets.remove(&socket_key) else {
            return;
        };
        self.begin_starting_udp_socket(
            socket_key,
            socket_id,
            UdpSocketStartOrigin::ManagerOwned,
            open.queued_sends,
        );
    }

    fn handle_register_external_udp_socket(
        &mut self,
        ask: Ask<ExternalUdpSocketRegistration, Result<(), ExternalUdpSocketRegistrationError>>,
    ) -> HandlerResult {
        let (promise, registration) = ask.take();
        let result = self.register_external_udp_socket(registration);
        let _ = promise.fulfil(result);
        Handled::OK
    }

    /// Register one socket that another runtime component owns but route transport may use.
    ///
    /// Unknown UDP bind events are ignored, so this is the only path that may
    /// attach transport state to externally owned sockets.
    fn register_external_udp_socket(
        &mut self,
        registration: ExternalUdpSocketRegistration,
    ) -> Result<(), ExternalUdpSocketRegistrationError> {
        let socket_key = UdpSocketKey {
            local_addr: registration.local_addr,
        };
        if let Some(registered_socket_key) = self.udp_socket_ids.get(&registration.socket_id) {
            if *registered_socket_key == socket_key {
                return Ok(());
            }
            return Err(
                ExternalUdpSocketRegistrationError::SocketIdAlreadyRegistered {
                    socket_id: registration.socket_id,
                },
            );
        }
        if self.udp_sockets.contains_key(&socket_key)
            || self.udp_starting_sockets.contains_key(&socket_key)
            || self.udp_open_sockets.contains_key(&socket_key)
            || self.udp_dormant_sockets.contains_key(&socket_key)
        {
            return Err(
                ExternalUdpSocketRegistrationError::LocalAddrAlreadyRegistered {
                    local_addr: registration.local_addr,
                },
            );
        }

        self.udp_socket_ids
            .insert(registration.socket_id, socket_key);
        match self.udp_activation_policy {
            UdpActivationPolicy::OnBind => {
                self.begin_starting_udp_socket(
                    socket_key,
                    registration.socket_id,
                    UdpSocketStartOrigin::ExternalDormant,
                    Vec::new(),
                );
            }
            UdpActivationPolicy::OnFirstUse => {
                self.udp_dormant_sockets.insert(
                    socket_key,
                    DormantUdpSocketHandle {
                        socket_id: registration.socket_id,
                    },
                );
            }
        }
        Ok(())
    }

    /// Apply one route-endpoint lifecycle event from the endpoint owner.
    fn handle_route_endpoint_lifecycle(&mut self, lifecycle: RouteEndpointLifecycle) {
        match lifecycle {
            RouteEndpointLifecycle::Available(binding) => {
                if let Err(error) = self.register_route_endpoint(binding) {
                    error!(
                        self.log(),
                        "route transport rejected route endpoint lifecycle registration: {}", error
                    );
                    return;
                }
                self.route_endpoint_lifecycle_provided
                    .trigger(RouteEndpointLifecycle::Available(binding));
            }
            RouteEndpointLifecycle::Unavailable { binding, reason } => {
                match reason {
                    RouteEndpointUnavailableReason::Closed { reason } => {
                        self.handle_udp_closed(binding.socket_id, reason);
                    }
                }
                self.route_endpoint_lifecycle_provided
                    .trigger(RouteEndpointLifecycle::Unavailable { binding, reason });
            }
        }
    }

    /// Register one endpoint-owner socket as a route-transport endpoint.
    fn register_route_endpoint(
        &mut self,
        binding: RouteEndpointBinding,
    ) -> Result<(), ExternalUdpSocketRegistrationError> {
        self.register_external_udp_socket(ExternalUdpSocketRegistration {
            socket_id: binding.socket_id,
            local_addr: binding.socket_bound_addr,
        })
    }

    fn begin_starting_udp_socket(
        &mut self,
        socket_key: UdpSocketKey,
        socket_id: SocketId,
        origin: UdpSocketStartOrigin,
        queued_sends: Vec<QueuedUdpSend>,
    ) {
        let udpour_config = self.udpour_config.clone();
        let egress_pool = self.bridge.egress_pool().clone();
        let runtime = self
            .ctx
            .system()
            .create(move || UDPourComponent::new(socket_id, egress_pool, udpour_config));
        let udp_port = runtime.required_ref();
        let transfer_channel = runtime.connect_to_required(self.required_ref());
        let runtime_ref = runtime
            .actor_ref()
            .hold()
            .expect("child UDPour runtime must still be live after creation");
        self.udp_socket_ids.insert(socket_id, socket_key);
        self.udp_starting_sockets.insert(
            socket_key,
            StartingUdpSocketHandle {
                socket_id,
                runtime,
                runtime_ref,
                udp_port,
                transfer_channel,
                origin,
                queued_sends,
                buffered_datagram_count: 0,
            },
        );
        self.spawn_local(move |mut async_self| async move {
            async_self.finish_udp_socket_activation(socket_key).await;
            Handled::OK
        });
    }

    fn handle_udp_bind_failed(&mut self, request_id: UdpOpenRequestId, reason: OpenFailureReason) {
        let Some(socket_key) = self.udp_open_requests.remove(&request_id) else {
            return;
        };
        self.fail_test_manager_owned_udp_bind(request_id);
        let Some(open) = self.udp_open_sockets.remove(&socket_key) else {
            return;
        };

        let discovery_reason = classify_open_failure_for_discovery(reason);
        let nack_reason = classify_open_failure_for_nack(reason);
        let mut failed_routes = HashSet::new();
        for queued in &open.queued_sends {
            failed_routes.insert(queued.route);
        }
        for route in failed_routes {
            self.report_route_failed(TransportRouteKey::Udp(route), discovery_reason.clone());
        }
        for queued in open.queued_sends {
            self.fail_pending_send(
                queued.send_id,
                TransportRouteKey::Udp(queued.route),
                nack_reason.clone(),
            );
        }
    }

    fn handle_udp_broadcast_configured(&mut self, socket_id: SocketId) {
        let Some(socket_key) = self.udp_broadcast_configurations.remove(&socket_id) else {
            return;
        };
        let queued_sends = {
            let Some(handle) = self.udp_sockets.get_mut(&socket_key) else {
                return;
            };
            match std::mem::replace(&mut handle.broadcast_state, UdpBroadcastState::Enabled) {
                UdpBroadcastState::Enabling { queued_sends } => queued_sends,
                other => {
                    handle.broadcast_state = other;
                    Vec::new()
                }
            }
        };
        self.flush_udp_open_queue(socket_key, queued_sends);
    }

    fn handle_udp_broadcast_config_failed(&mut self, socket_id: SocketId) {
        let Some(socket_key) = self.udp_broadcast_configurations.remove(&socket_id) else {
            return;
        };
        let queued_sends = {
            let Some(handle) = self.udp_sockets.get_mut(&socket_key) else {
                return;
            };
            match std::mem::replace(&mut handle.broadcast_state, UdpBroadcastState::Failed) {
                UdpBroadcastState::Enabling { queued_sends } => queued_sends,
                other => {
                    handle.broadcast_state = other;
                    Vec::new()
                }
            }
        };

        let mut failed_routes = HashSet::new();
        for queued in &queued_sends {
            failed_routes.insert(queued.route);
        }
        for route in failed_routes {
            self.report_route_failed(
                TransportRouteKey::Udp(route),
                ConnectionFailureReason::Other(IString::from("udp broadcast configuration failed")),
            );
        }
        for queued in queued_sends {
            self.fail_pending_send(
                queued.send_id,
                TransportRouteKey::Udp(queued.route),
                RouteTransportNackReason::RouteUnavailable,
            );
        }
    }

    async fn finish_udp_socket_activation(&mut self, socket_key: UdpSocketKey) {
        let Some((socket_id, runtime)) = self
            .udp_starting_sockets
            .get(&socket_key)
            .map(|handle| (handle.socket_id, handle.runtime.clone()))
        else {
            return;
        };
        let bridge = self.bridge.clone();

        let connect_result = bridge.connect_udp(&runtime).await;
        if let Err(error) = connect_result {
            let Some(starting) = self.udp_starting_sockets.remove(&socket_key) else {
                return;
            };
            self.handle_udp_socket_activation_failed(
                socket_key,
                starting.socket_id,
                starting.origin,
                starting.queued_sends,
                &classify_udp_connect_failure_for_discovery(&error),
            );
            self.ctx.system().kill(starting.runtime);
            return;
        }

        let Some(starting) = self.udp_starting_sockets.remove(&socket_key) else {
            self.ctx.system().kill(runtime);
            return;
        };
        self.ctx.system().start(&starting.runtime);
        self.udp_sockets.insert(
            socket_key,
            LiveUdpSocketHandle {
                socket_id,
                runtime: starting.runtime,
                runtime_ref: starting.runtime_ref,
                _transfer_channel: starting.transfer_channel,
                startup_buffered_datagram_count: starting.buffered_datagram_count,
                known_routes: HashSet::new(),
                broadcast_state: UdpBroadcastState::Disabled,
            },
        );
        self.flush_udp_open_queue(socket_key, starting.queued_sends);
    }

    fn flush_udp_open_queue(&mut self, socket_key: UdpSocketKey, queued_sends: Vec<QueuedUdpSend>) {
        for queued in queued_sends {
            self.handle_live_udp_send(queued.send_id, queued.route, socket_key);
        }
    }

    fn handle_live_udp_send(
        &mut self,
        send_id: RouteSendId,
        route: UdpRouteKey,
        socket_key: UdpSocketKey,
    ) {
        let action = {
            let Some(handle) = self.udp_sockets.get_mut(&socket_key) else {
                self.fail_pending_send(
                    send_id,
                    TransportRouteKey::Udp(route),
                    RouteTransportNackReason::RouteUnavailable,
                );
                return;
            };
            handle.known_routes.insert(route);
            if route.scope == DatagramRouteScope::Broadcast {
                match &mut handle.broadcast_state {
                    UdpBroadcastState::Disabled => {
                        let socket_id = handle.socket_id;
                        handle.broadcast_state = UdpBroadcastState::Enabling {
                            queued_sends: vec![QueuedUdpSend { send_id, route }],
                        };
                        LiveUdpSendAction::ConfigureBroadcast { socket_id }
                    }
                    UdpBroadcastState::Enabling { queued_sends } => {
                        queued_sends.push(QueuedUdpSend { send_id, route });
                        LiveUdpSendAction::Queued
                    }
                    UdpBroadcastState::Enabled => LiveUdpSendAction::Dispatch,
                    UdpBroadcastState::Failed => LiveUdpSendAction::FailBroadcast,
                }
            } else {
                LiveUdpSendAction::Dispatch
            }
        };

        match action {
            LiveUdpSendAction::Dispatch => self.spawn_udp_dispatch(send_id, route, socket_key),
            LiveUdpSendAction::Queued => {}
            LiveUdpSendAction::ConfigureBroadcast { socket_id } => {
                self.udp_broadcast_configurations
                    .insert(socket_id, socket_key);
                self.udp_bridge_port.trigger(UdpRequest::Configure {
                    socket_id,
                    option: UdpSocketOption::Broadcast(true),
                });
            }
            LiveUdpSendAction::FailBroadcast => {
                self.fail_pending_send(
                    send_id,
                    TransportRouteKey::Udp(route),
                    RouteTransportNackReason::RouteUnavailable,
                );
            }
        }
    }

    fn spawn_udp_dispatch(
        &mut self,
        send_id: RouteSendId,
        route: UdpRouteKey,
        socket_key: UdpSocketKey,
    ) {
        self.spawn_local(move |mut async_self| async move {
            async_self
                .dispatch_udp_send(send_id, route, socket_key)
                .await;
            Handled::OK
        });
    }

    async fn dispatch_udp_send(
        &mut self,
        send_id: RouteSendId,
        route: UdpRouteKey,
        socket_key: UdpSocketKey,
    ) {
        let Some((payload_source, coverage_key)) =
            self.pending_sends.get(&send_id).map(|pending| {
                (
                    Arc::clone(&pending.send.payload),
                    pending.send.route.coverage_key,
                )
            })
        else {
            return;
        };
        let Some(handle) = self.udp_sockets.get(&socket_key) else {
            self.fail_pending_send(
                send_id,
                coverage_key,
                RouteTransportNackReason::RouteUnavailable,
            );
            return;
        };
        let runtime_ref = handle.runtime_ref.clone();
        let payload = match encode_message_payload(
            self.bridge.egress_pool(),
            payload_source.as_ref(),
        )
        .await
        {
            Ok(payload) => payload,
            Err(error) => {
                self.fail_pending_send(
                    send_id,
                    coverage_key,
                    classify_serialization_failure(&error),
                );
                return;
            }
        };
        let submit = runtime_ref.ask_with(|promise| {
            UDPourComponentMessage::Submit(Ask::new(
                promise,
                UDPourSend {
                    target: route.remote_addr,
                    payload,
                },
            ))
        });
        match submit.await {
            Ok(UDPourSubmitResult::Sent) => {
                self.complete_pending_send_success(send_id, coverage_key);
            }
            Ok(UDPourSubmitResult::SendFailed { reason }) => {
                self.fail_pending_send(
                    send_id,
                    coverage_key,
                    classify_udpour_send_failure(&reason),
                );
            }
            Err(_) => {
                self.fail_pending_send(
                    send_id,
                    coverage_key,
                    RouteTransportNackReason::RouteUnavailable,
                );
            }
        }
    }

    fn fail_pending_send(
        &mut self,
        send_id: RouteSendId,
        coverage_key: TransportRouteKey,
        reason: RouteTransportNackReason,
    ) {
        let Some(pending) = self.pending_sends.remove(&send_id) else {
            return;
        };
        let Some(promise) = pending.submit_promise else {
            return;
        };
        let _ = promise.fulfil(RouteTransportSubmitResult::SendFailed {
            coverage_key,
            reason,
        });
    }

    fn complete_pending_send_success(
        &mut self,
        send_id: RouteSendId,
        coverage_key: TransportRouteKey,
    ) {
        let Some(pending) = self.pending_sends.remove(&send_id) else {
            return;
        };
        let Some(promise) = pending.submit_promise else {
            return;
        };
        let _ = promise.fulfil(RouteTransportSubmitResult::Sent { coverage_key });
    }

    fn report_route_failed(&mut self, route: TransportRouteKey, reason: ConnectionFailureReason) {
        self.connection_info_port
            .trigger(ConnectionInfoIndication::ReportRouteFailed { route, reason });
    }

    fn handle_udp_runtime_indication(&mut self, deliver: UDPourDeliver) -> HandlerResult {
        let socket_key = self
            .udp_socket_ids
            .get(&deliver.socket_id)
            .copied()
            .with_whatever_benign(|| {
                format!(
                    "Dropping UDPour delivery from unknown socket_id={} and source={}",
                    deliver.socket_id, deliver.source
                )
            })?;
        let route = TransportRouteKey::Udp(UdpRouteKey {
            remote_addr: deliver.source,
            scope: DatagramRouteScope::Unicast,
            local_bind: Some(socket_key.local_addr),
        });
        self.inbound_port.trigger(RouteTransportInboundDeliver {
            payload: deliver.payload,
            transport: InboundTransportMeta {
                route,
                remote_addr: Some(deliver.source),
            },
        });
        Handled::OK
    }

    fn handle_udp_received(&mut self, socket_id: SocketId, source: SocketAddr, payload: IoPayload) {
        let Some(socket_key) = self.udp_socket_ids.get(&socket_id).copied() else {
            return;
        };
        if self.udp_sockets.contains_key(&socket_key) {
            return;
        }
        if let Some(dormant) = self.udp_dormant_sockets.remove(&socket_key) {
            self.begin_starting_udp_socket(
                socket_key,
                dormant.socket_id,
                UdpSocketStartOrigin::ExternalDormant,
                Vec::new(),
            );
        }
        let Some(starting) = self.udp_starting_sockets.get_mut(&socket_key) else {
            return;
        };
        if starting.buffered_datagram_count >= MAX_BUFFERED_STARTUP_DATAGRAMS {
            warn!(
                self.log(),
                "Dropping inbound UDP datagram for socket_id={socket_id:?} while UDPour child is starting because the startup buffer is full"
            );
            return;
        }
        self.system.trigger_i(
            UdpIndication::Received {
                socket_id,
                source,
                payload,
            },
            &starting.udp_port,
        );
        starting.buffered_datagram_count += 1;
    }

    fn handle_udp_closed(&mut self, socket_id: SocketId, reason: UdpCloseReason) {
        self.udp_broadcast_configurations.remove(&socket_id);
        self.release_test_manager_owned_udp_bind(socket_id);
        let Some(socket_key) = self.udp_socket_ids.remove(&socket_id) else {
            return;
        };
        if self.udp_dormant_sockets.remove(&socket_key).is_some() {
            return;
        }
        if let Some(starting) = self.udp_starting_sockets.remove(&socket_key) {
            let discovery_reason = classify_udp_close_for_discovery(reason);
            for queued in starting.queued_sends {
                self.report_route_failed(
                    TransportRouteKey::Udp(queued.route),
                    discovery_reason.clone(),
                );
                self.fail_pending_send(
                    queued.send_id,
                    TransportRouteKey::Udp(queued.route),
                    RouteTransportNackReason::RouteUnavailable,
                );
            }
            self.ctx.system().kill(starting.runtime);
            return;
        }
        let Some(handle) = self.udp_sockets.remove(&socket_key) else {
            return;
        };
        self.ctx.system().kill(handle.runtime);
        let discovery_reason = classify_udp_close_for_discovery(reason);
        for route in &handle.known_routes {
            self.report_route_failed(TransportRouteKey::Udp(*route), discovery_reason.clone());
        }

        if let UdpBroadcastState::Enabling { queued_sends } = handle.broadcast_state {
            for queued in queued_sends {
                self.fail_pending_send(
                    queued.send_id,
                    TransportRouteKey::Udp(queued.route),
                    RouteTransportNackReason::RouteUnavailable,
                );
            }
        }
    }

    fn shutdown_children(&mut self) {
        for (_, handle) in self.udp_sockets.drain() {
            self.ctx.system().kill(handle.runtime);
        }
        self.udp_open_sockets.clear();
        self.udp_open_requests.clear();
        for (_, handle) in self.udp_starting_sockets.drain() {
            self.ctx.system().kill(handle.runtime);
        }
        self.udp_dormant_sockets.clear();
        self.udp_broadcast_configurations.clear();
        self.udp_socket_ids.clear();
        self.pending_sends.clear();
    }

    /// Restores or forgets one starting UDP socket after its child could not be connected.
    ///
    /// Manager-owned sockets are removed completely because the manager itself requested the bind
    /// and cannot recover without re-opening a fresh socket. Externally bound dormant sockets stay
    /// around so a later inbound datagram can retry child activation.
    fn handle_udp_socket_activation_failed(
        &mut self,
        socket_key: UdpSocketKey,
        socket_id: SocketId,
        origin: UdpSocketStartOrigin,
        queued_sends: Vec<QueuedUdpSend>,
        discovery_reason: &ConnectionFailureReason,
    ) {
        match origin {
            UdpSocketStartOrigin::ManagerOwned => {
                self.udp_bridge_port
                    .trigger(UdpRequest::Close { socket_id });
            }
            UdpSocketStartOrigin::ExternalDormant => {
                self.udp_socket_ids.insert(socket_id, socket_key);
                self.udp_dormant_sockets
                    .insert(socket_key, DormantUdpSocketHandle { socket_id });
            }
        }

        let mut failed_routes = HashSet::new();
        for queued in &queued_sends {
            failed_routes.insert(queued.route);
        }
        for route in failed_routes {
            self.report_route_failed(TransportRouteKey::Udp(route), (*discovery_reason).clone());
        }
        for queued in queued_sends {
            self.fail_pending_send(
                queued.send_id,
                TransportRouteKey::Udp(queued.route),
                RouteTransportNackReason::RouteUnavailable,
            );
        }
    }

    /// Test-only probe for whether one externally bound socket has been
    /// incorporated into the manager's current UDP socket state yet.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn knows_external_udp_socket_binding(
        &self,
        socket_id: SocketId,
        local_addr: SocketAddr,
    ) -> bool {
        let socket_key = UdpSocketKey { local_addr };
        self.udp_dormant_sockets
            .get(&socket_key)
            .is_some_and(|handle| handle.socket_id == socket_id)
            || self
                .udp_starting_sockets
                .get(&socket_key)
                .is_some_and(|handle| handle.socket_id == socket_id)
            || self
                .udp_sockets
                .get(&socket_key)
                .is_some_and(|handle| handle.socket_id == socket_id)
    }
}

impl ComponentLifecycle for RouteTransportManager {
    fn on_start(&mut self) -> HandlerResult {
        self.udp_activation_policy = self.load_udp_activation_policy();
        Handled::OK
    }

    fn on_stop(&mut self) -> HandlerResult {
        self.shutdown_children();
        Handled::OK
    }

    fn on_kill(&mut self) -> HandlerResult {
        self.shutdown_children();
        Handled::OK
    }
}

ignore_requests!(TransportRouteTransportPort, RouteTransportManager);
ignore_requests!(TransportConnectionInfoPort, RouteTransportManager);
ignore_requests!(RouteEndpointLifecyclePort, RouteTransportManager);

impl Require<UDPourPort> for RouteTransportManager {
    fn handle(&mut self, indication: UDPourDeliver) -> HandlerResult {
        self.handle_udp_runtime_indication(indication)
    }
}

impl Require<RouteEndpointLifecyclePort> for RouteTransportManager {
    fn handle(&mut self, indication: RouteEndpointLifecycle) -> HandlerResult {
        self.handle_route_endpoint_lifecycle(indication);
        Handled::OK
    }
}

impl Require<UdpPort> for RouteTransportManager {
    fn handle(&mut self, indication: UdpIndication) -> HandlerResult {
        match indication {
            UdpIndication::Bound {
                request_id,
                socket_id,
                local_addr,
            } => self.handle_udp_bound(request_id, socket_id, local_addr),
            UdpIndication::BindFailed {
                request_id, reason, ..
            } => self.handle_udp_bind_failed(request_id, reason),
            UdpIndication::Configured {
                socket_id,
                option: UdpSocketOption::Broadcast(true),
            } => self.handle_udp_broadcast_configured(socket_id),
            UdpIndication::ConfigureFailed {
                socket_id,
                option: UdpSocketOption::Broadcast(true),
                ..
            } => self.handle_udp_broadcast_config_failed(socket_id),
            UdpIndication::Closed {
                socket_id, reason, ..
            } => self.handle_udp_closed(socket_id, reason),
            UdpIndication::Received {
                socket_id,
                source,
                payload,
            } => self.handle_udp_received(socket_id, source, payload),
            _ => {}
        }
        Handled::OK
    }
}

impl Actor for RouteTransportManager {
    type Message = TransportRouteTransportMessage;

    fn receive_local(&mut self, msg: Self::Message) -> HandlerResult {
        match msg {
            RouteTransportActorMessage::Submit(ask) => self.handle_submit(ask),
            RouteTransportActorMessage::RegisterExternalUdpSocket(ask) => {
                self.handle_register_external_udp_socket(ask)
            }
        }
    }
}

type TransportConnectionInfoPort = ConnectionInfoPort<TransportRouteKey>;
type TransportRouteTransportMessage = RouteTransportActorMessage<TransportRouteKey>;
type TransportRouteTransportSend = RouteTransportSend<TransportRouteKey>;
type TransportRouteTransportPort = RouteTransportPort<TransportRouteKey>;
type TransportRouteTransportSubmitResult = RouteTransportSubmitResult<TransportRouteKey>;

/// Kompact configuration keys that control when the manager creates per-socket
/// `UDPour` children.
mod config_keys {
    use super::{UsizeValue, kompact_config};

    kompact_config! {
        UDP_ACTIVATION_POLICY,
        key = "flotsync.route-transport.udp-activation-policy",
        type = UsizeValue,
        default = 0,
        doc = "When to create one UDPour child for a bound UDP socket: 0 = OnBind, 1 = OnFirstUse.",
        version = "0.1.0"
    }
}

/// Upper bound on inbound UDP datagrams forwarded into one per-socket `UDPour` child while it is
/// still starting.
///
/// During this short bring-up window the manager does not keep its own copy of incoming datagrams.
/// Instead it enqueues exact `UdpIndication::Received` events onto the starting child's required
/// `UdpPort` via `KompactSystem::trigger_i`. This counter only exists to bound that queueing.
const MAX_BUFFERED_STARTUP_DATAGRAMS: usize = 64;

/// When one bound UDP socket should gain its per-socket `UDPour` child.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
enum UdpActivationPolicy {
    /// Create and connect the child immediately once the socket bind completes.
    #[default]
    OnBind,
    /// Keep the socket dormant until the first inbound or outbound message actually needs it.
    OnFirstUse,
}

impl UdpActivationPolicy {
    const fn config_value(self) -> usize {
        match self {
            Self::OnBind => 0,
            Self::OnFirstUse => 1,
        }
    }
}

/// Configure the route-transport runtime for the replication full-stack host.
///
/// The current replication runtime keeps per-socket `UDPour` children dormant
/// until a concrete route is first used. That avoids unnecessary startup churn
/// for sockets that may never carry delivery traffic.
pub fn configure_replication_runtime(config: &mut KompactConfig) {
    config.set_config_value(
        &config_keys::UDP_ACTIVATION_POLICY,
        UdpActivationPolicy::OnFirstUse.config_value(),
    );
}

/// Live UDP route handle owned by the manager.
struct LiveUdpSocketHandle {
    socket_id: SocketId,
    runtime: Arc<Component<UDPourComponent>>,
    runtime_ref: ActorRefStrong<UDPourComponentMessage>,
    _transfer_channel: ProviderChannel<UDPourPort, UDPourComponent>,
    /// Number of raw UDP datagrams that were queued onto this child's required
    /// `UdpPort` before the child was started.
    ///
    /// This survives the `Starting -> Live` transition so tests and diagnostics
    /// can still tell whether the lazy startup path buffered anything.
    #[cfg_attr(not(test), allow(dead_code))]
    startup_buffered_datagram_count: usize,
    /// Every concrete UDP route that currently relies on this socket.
    known_routes: HashSet<UdpRouteKey>,
    /// Broadcast enablement state for this shared socket.
    broadcast_state: UdpBroadcastState,
}

/// Live TCP route handle owned by the manager.
#[derive(Clone, Debug)]
#[allow(dead_code)]
struct LiveTcpRouteHandle {
    route: TcpRouteKey,
    session: Option<()>,
    open_in_flight: bool,
}

/// Logical send remembered while one concrete route attempt is still in flight.
struct PendingRouteSend {
    send: TransportRouteTransportSend,
    submit_promise: Option<KPromise<TransportRouteTransportSubmitResult>>,
}

/// UDP route that has been requested but is not yet ready for sends.
struct PendingUdpSocketOpen {
    queued_sends: Vec<QueuedUdpSend>,
}

/// One bound UDP socket that exists on the shared bridge but has not yet
/// needed a `UDPour` child.
struct DormantUdpSocketHandle {
    socket_id: SocketId,
}

/// One bound UDP socket whose child runtime is still being connected and started.
struct StartingUdpSocketHandle {
    socket_id: SocketId,
    runtime: Arc<Component<UDPourComponent>>,
    runtime_ref: ActorRefStrong<UDPourComponentMessage>,
    udp_port: RequiredRef<UdpPort>,
    transfer_channel: ProviderChannel<UDPourPort, UDPourComponent>,
    /// Whether this starting socket came from a manager-owned bind or from an externally bound
    /// dormant socket that may need to be restored on activation failure.
    origin: UdpSocketStartOrigin,
    /// Logical sends queued while the child runtime is not yet ready.
    queued_sends: Vec<QueuedUdpSend>,
    /// Number of raw UDP datagrams queued onto the child before it was started.
    buffered_datagram_count: usize,
}

/// Provenance of one per-socket `UDPour` child while it is still starting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UdpSocketStartOrigin {
    /// The manager opened this socket itself and owns the whole resource lifecycle.
    ManagerOwned,
    /// The bridge reported an external bind and the manager is only attaching a child on demand.
    ExternalDormant,
}

/// One logical UDP send queued while the shared local socket is not fully ready.
#[derive(Clone, Copy)]
struct QueuedUdpSend {
    send_id: RouteSendId,
    route: UdpRouteKey,
}

/// Current broadcast enablement state for one live shared UDP socket.
enum UdpBroadcastState {
    Disabled,
    Enabling { queued_sends: Vec<QueuedUdpSend> },
    Enabled,
    Failed,
}

/// Action selected while deciding how a live shared UDP socket should handle one send.
enum LiveUdpSendAction {
    Dispatch,
    Queued,
    ConfigureBroadcast { socket_id: SocketId },
    FailBroadcast,
}

/// Key for one reusable local UDP socket.
///
/// This intentionally collapses multiple full UDP route keys onto one local
/// socket whenever they would bind the same concrete local address. In
/// particular, `ForPeer(...)` policies are normalized to the concrete local
/// bind shape they resolve to so one child `UDPour` runtime can multiplex many
/// targets on the same socket.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct UdpSocketKey {
    local_addr: SocketAddr,
}

impl UdpSocketKey {
    fn for_route(route: UdpRouteKey) -> Self {
        let bind = match route.local_bind {
            Some(local_addr) => UdpLocalBind::Exact(local_addr),
            None => UdpLocalBind::ForPeer(route.remote_addr),
        };
        Self {
            local_addr: bind.resolve_local_addr(),
        }
    }

    fn bind_policy(self) -> UdpLocalBind {
        UdpLocalBind::Exact(self.local_addr)
    }
}

fn classify_serialization_failure(error: &FlotsyncSerializeError) -> RouteTransportNackReason {
    match error {
        FlotsyncSerializeError::Io { .. } => RouteTransportNackReason::LocalResourcePressure,
        FlotsyncSerializeError::Encoding { .. } => RouteTransportNackReason::InvalidPayload,
    }
}

fn classify_udpour_send_failure(reason: &UDPourSendFailureReason) -> RouteTransportNackReason {
    match reason {
        UDPourSendFailureReason::State(_) => RouteTransportNackReason::InvalidPayload,
        UDPourSendFailureReason::Encode(_) => RouteTransportNackReason::LocalResourcePressure,
        UDPourSendFailureReason::Transport(reason) => classify_transport_send_failure(*reason),
    }
}

fn classify_transport_send_failure(reason: SendFailureReason) -> RouteTransportNackReason {
    match reason {
        SendFailureReason::Backpressure => RouteTransportNackReason::Backpressure,
        SendFailureReason::MessageTooLarge => RouteTransportNackReason::InvalidPayload,
        SendFailureReason::MissingTargetForUnconnectedSocket
        | SendFailureReason::UnexpectedTargetForConnectedSocket => {
            RouteTransportNackReason::RouteUnknown
        }
        SendFailureReason::Closed
        | SendFailureReason::InvalidState
        | SendFailureReason::DriverUnavailable => RouteTransportNackReason::RouteUnavailable,
        SendFailureReason::IoError => {
            RouteTransportNackReason::Other(IString::from("udp io error"))
        }
        _ => RouteTransportNackReason::Other(IString::from("unknown udp send failure")),
    }
}

fn classify_open_failure_for_nack(reason: OpenFailureReason) -> RouteTransportNackReason {
    match reason {
        OpenFailureReason::InvalidHandle => RouteTransportNackReason::RouteUnknown,
        OpenFailureReason::DriverUnavailable | OpenFailureReason::Io(_) => {
            RouteTransportNackReason::RouteUnavailable
        }
    }
}

fn classify_open_failure_for_discovery(reason: OpenFailureReason) -> ConnectionFailureReason {
    match reason {
        OpenFailureReason::InvalidHandle => {
            ConnectionFailureReason::Other(IString::from("bridge rejected udp route handle"))
        }
        OpenFailureReason::DriverUnavailable => ConnectionFailureReason::Closed,
        OpenFailureReason::Io(_) => ConnectionFailureReason::Unreachable,
    }
}

fn classify_udp_close_for_discovery(reason: UdpCloseReason) -> ConnectionFailureReason {
    match reason {
        UdpCloseReason::Requested => ConnectionFailureReason::Closed,
        UdpCloseReason::Disconnected => ConnectionFailureReason::Unreachable,
        _ => ConnectionFailureReason::Other(IString::from("unknown udp close reason")),
    }
}

fn classify_udp_connect_failure_for_discovery(
    error: &flotsync_io::prelude::Error,
) -> ConnectionFailureReason {
    match error {
        flotsync_io::prelude::Error::DriverUnavailable => ConnectionFailureReason::Closed,
        _ => ConnectionFailureReason::Other(IString::from(
            "failed to connect udp child runtime to bridge",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        RoutePreferenceRank,
        RouteSharingKind,
        SendRouteCandidate,
        test_support::{BoundReservedUdpSocket, TransportHarnessCore},
    };
    use bytes::Bytes;
    use flotsync_io::{
        pool::PayloadWriter,
        prelude::{SocketId, UdpIndication, UdpLocalBind},
        test_support::{
            ReservedSocketKind,
            WAIT_TIMEOUT,
            build_test_kompact_system_with,
            enable_bind_reuse_address,
            eventually_component_state,
            eventually_value,
            localhost,
            set_test_system_label,
            start_component,
        },
    };
    use flotsync_messages::serialisation::{
        FlotsyncSerializable,
        FlotsyncSerializeError,
        SizeHint,
    };
    use flotsync_udpour::{
        MessageId,
        ReceiverConfig,
        SenderConfig,
        config_keys as udpour_config_keys,
    };
    use flotsync_utils::{
        BoxFuture,
        kompact_testing::{PortTestingExt, PortTestingRefExt},
    };
    use futures_util::FutureExt;
    use std::{
        cell::Cell,
        fmt::{self, Display},
        net::SocketAddr,
        num::NonZeroUsize,
        time::Duration,
    };
    use uuid::Uuid;

    #[derive(Clone, Copy, Debug)]
    struct TestSendRateControl {
        send_delay: Duration,
        backpressure_retry_delay: Duration,
        max_in_flight_datagrams: usize,
    }

    impl Default for TestSendRateControl {
        fn default() -> Self {
            Self {
                send_delay: udpour_config_keys::SEND_DELAY
                    .default()
                    .expect("UDPour send-delay default must exist"),
                backpressure_retry_delay: udpour_config_keys::BACKPRESSURE_RETRY_DELAY
                    .default()
                    .expect("UDPour backpressure-retry-delay default must exist"),
                max_in_flight_datagrams: udpour_config_keys::MAX_IN_FLIGHT_DATAGRAMS
                    .default()
                    .expect("UDPour max-in-flight-datagrams default must exist"),
            }
        }
    }

    struct BytesPayload(Vec<u8>);

    impl FlotsyncSerializable for BytesPayload {
        fn serialized_size_hint(&self) -> SizeHint {
            SizeHint::Exact(self.0.len())
        }

        fn serialize_into<'a>(
            &'a self,
            writer: &'a mut flotsync_io::prelude::EgressAsyncWriter,
        ) -> BoxFuture<'a, Result<(), FlotsyncSerializeError>> {
            async move {
                writer
                    .write_slice(&self.0)
                    .await
                    .map_err(|source| FlotsyncSerializeError::Io { source })?;
                Ok(())
            }
            .boxed()
        }
    }

    struct StartupBufferedDatagramsTimeout<'a> {
        socket_key: UdpSocketKey,
        min_count: usize,
        max_observed: &'a Cell<usize>,
    }

    impl Display for StartupBufferedDatagramsTimeout<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "timed out waiting for the manager to buffer {} startup datagrams for {:?}; max observed was {}",
                self.min_count,
                self.socket_key,
                self.max_observed.get()
            )
        }
    }

    struct UdpManagerHarness {
        core: TransportHarnessCore,
        manager: Arc<Component<RouteTransportManager>>,
        manager_ref: ActorRefStrong<TransportRouteTransportMessage>,
    }

    impl UdpManagerHarness {
        fn new() -> Self {
            Self::with_socket_budgets(
                0,
                0,
                UdpActivationPolicy::OnBind,
                TestSendRateControl::default(),
                udpour_config(),
            )
        }

        fn with_socket_budgets(
            extra_reserved_udp_sockets: usize,
            manager_owned_udp_sockets: usize,
            activation_policy: UdpActivationPolicy,
            send_rate_control: TestSendRateControl,
            udpour_config: UDPourConfig,
        ) -> Self {
            let extra_socket_kinds =
                vec![ReservedSocketKind::UdpSocket; extra_reserved_udp_sockets];
            Self::with_reserved_socket_kinds_and_manager_budget(
                false,
                &extra_socket_kinds,
                manager_owned_udp_sockets,
                activation_policy,
                send_rate_control,
                udpour_config,
            )
        }

        fn with_reserved_socket_kinds_and_manager_budget(
            reserve_external_udp_socket: bool,
            extra_socket_kinds: &[ReservedSocketKind],
            manager_owned_udp_sockets: usize,
            activation_policy: UdpActivationPolicy,
            send_rate_control: TestSendRateControl,
            udpour_config: UDPourConfig,
        ) -> Self {
            let system = build_manager_test_kompact_system(activation_policy, send_rate_control);
            let core = TransportHarnessCore::with_socket_budgets(
                system,
                udpour_config,
                reserve_external_udp_socket,
                extra_socket_kinds,
                manager_owned_udp_sockets,
            );
            let manager = Arc::clone(core.manager());
            let manager_ref = core.manager_ref();
            core.start();

            Self {
                core,
                manager,
                manager_ref,
            }
        }

        fn with_config(
            activation_policy: UdpActivationPolicy,
            send_rate_control: TestSendRateControl,
            udpour_config: UDPourConfig,
        ) -> Self {
            Self::with_socket_budgets(0, 0, activation_policy, send_rate_control, udpour_config)
        }

        fn with_external_socket(
            activation_policy: UdpActivationPolicy,
            send_rate_control: TestSendRateControl,
            udpour_config: UDPourConfig,
        ) -> Self {
            Self::with_reserved_socket_kinds_and_manager_budget(
                true,
                &[],
                0,
                activation_policy,
                send_rate_control,
                udpour_config,
            )
        }

        fn send(&self, send: TransportRouteTransportSend) -> TransportRouteTransportSubmitResult {
            self.send_async(send)
                .wait_timeout(WAIT_TIMEOUT)
                .expect("timed out waiting for route transport submit result")
        }

        fn send_async(
            &self,
            send: TransportRouteTransportSend,
        ) -> KFuture<TransportRouteTransportSubmitResult> {
            self.manager_ref
                .ask_with(|promise| RouteTransportActorMessage::Submit(Ask::new(promise, send)))
        }

        fn register_external_socket(&self, socket_id: SocketId, local_addr: SocketAddr) {
            let registration = ExternalUdpSocketRegistration {
                socket_id,
                local_addr,
            };
            let result = self
                .manager_ref
                .ask_with(|promise| {
                    RouteTransportActorMessage::RegisterExternalUdpSocket(Ask::new(
                        promise,
                        registration,
                    ))
                })
                .wait_timeout(WAIT_TIMEOUT)
                .expect("timed out waiting for external UDP socket registration");
            if let Err(error) = result {
                panic!("external UDP socket registration failed: {error}");
            }
        }

        fn publish_route_endpoint_available(
            &self,
            socket_id: SocketId,
            socket_bound_addr: SocketAddr,
        ) {
            let route_endpoint_lifecycle_port = self
                .manager
                .on_definition(RouteTransportManager::route_endpoint_lifecycle_port);
            self.core.system().trigger_i(
                RouteEndpointLifecycle::Available(RouteEndpointBinding {
                    socket_id,
                    socket_bound_addr,
                }),
                &route_endpoint_lifecycle_port,
            );
        }

        fn publish_route_endpoint_unavailable(
            &self,
            socket_id: SocketId,
            socket_bound_addr: SocketAddr,
        ) {
            let route_endpoint_lifecycle_port = self
                .manager
                .on_definition(RouteTransportManager::route_endpoint_lifecycle_port);
            self.core.system().trigger_i(
                RouteEndpointLifecycle::Unavailable {
                    binding: RouteEndpointBinding {
                        socket_id,
                        socket_bound_addr,
                    },
                    reason: RouteEndpointUnavailableReason::Closed {
                        reason: UdpCloseReason::Requested,
                    },
                },
                &route_endpoint_lifecycle_port,
            );
        }

        fn wait_for_send_ack(&self, send: TransportRouteTransportSend) -> TransportRouteKey {
            match self.send(send) {
                RouteTransportSubmitResult::Sent { coverage_key } => coverage_key,
                RouteTransportSubmitResult::SendFailed {
                    coverage_key,
                    reason,
                } => {
                    panic!("unexpected SendFailed for {coverage_key:?}: {reason:?}")
                }
            }
        }

        fn wait_for_send_ack_future(
            future: KFuture<TransportRouteTransportSubmitResult>,
        ) -> TransportRouteKey {
            match future
                .wait_timeout(WAIT_TIMEOUT)
                .expect("timed out waiting for route transport send success")
            {
                RouteTransportSubmitResult::Sent { coverage_key } => coverage_key,
                RouteTransportSubmitResult::SendFailed {
                    coverage_key,
                    reason,
                } => {
                    panic!("unexpected SendFailed for {coverage_key:?}: {reason:?}")
                }
            }
        }

        fn wait_for_send_nack(
            future: KFuture<TransportRouteTransportSubmitResult>,
        ) -> (TransportRouteKey, RouteTransportNackReason) {
            match future
                .wait_timeout(WAIT_TIMEOUT)
                .expect("timed out waiting for route transport send failure")
            {
                RouteTransportSubmitResult::SendFailed {
                    coverage_key,
                    reason,
                } => (coverage_key, reason),
                RouteTransportSubmitResult::Sent { coverage_key } => {
                    panic!("unexpected Sent result for {coverage_key:?}")
                }
            }
        }

        fn live_udp_socket_count(&self) -> usize {
            self.manager
                .on_definition(|component| component.udp_sockets.len())
        }

        fn dormant_udp_socket_count(&self) -> usize {
            self.manager
                .on_definition(|component| component.udp_dormant_sockets.len())
        }

        fn wait_for_dormant_socket(&self, socket_key: UdpSocketKey) {
            eventually_component_state(
                WAIT_TIMEOUT,
                &self.manager,
                |component| component.udp_dormant_sockets.contains_key(&socket_key),
                format_args!("timed out waiting for dormant UDP socket {socket_key:?}"),
            );
        }

        fn wait_for_startup_buffered_datagrams(
            &self,
            socket_key: UdpSocketKey,
            min_count: usize,
        ) -> usize {
            // `eventually_value` owns both the polling closure and the deferred
            // failure message. `Cell` lets the closure update the best
            // diagnostic sample while the display helper still reads the final
            // value if the wait times out.
            let max_observed = Cell::new(0usize);
            eventually_value(
                WAIT_TIMEOUT,
                || {
                    let (buffered_count, live) = self.manager.on_definition(|component| {
                        (
                            component
                                .udp_starting_sockets
                                .get(&socket_key)
                                .map(|handle| handle.buffered_datagram_count)
                                .or_else(|| {
                                    component
                                        .udp_sockets
                                        .get(&socket_key)
                                        .map(|handle| handle.startup_buffered_datagram_count)
                                }),
                            component.udp_sockets.contains_key(&socket_key),
                        )
                    });
                    if let Some(buffered_count) = buffered_count {
                        max_observed.set(max_observed.get().max(buffered_count));
                        if buffered_count >= min_count {
                            return Some(buffered_count);
                        }
                    }
                    assert!(
                        !(live && max_observed.get() == 0),
                        "UDPour child became live before any startup datagram was buffered for {socket_key:?}"
                    );
                    None
                },
                StartupBufferedDatagramsTimeout {
                    socket_key,
                    min_count,
                    max_observed: &max_observed,
                },
            )
        }

        fn wait_for_live_udp_socket(&self, socket_key: UdpSocketKey) {
            eventually_component_state(
                WAIT_TIMEOUT,
                &self.manager,
                |component| component.udp_sockets.contains_key(&socket_key),
                format_args!("timed out waiting for live UDP socket {socket_key:?}"),
            );
        }

        fn bind_external_socket(&self, bind: UdpLocalBind) -> (SocketId, SocketAddr) {
            self.core.bind_external_socket(bind, WAIT_TIMEOUT)
        }

        fn bind_reserved_udp_socket(&self, reservation_index: usize) -> BoundReservedUdpSocket {
            self.core.bind_reserved_udp_socket(reservation_index)
        }

        fn wait_for_new_bound_socket(&self) -> (SocketId, SocketAddr) {
            self.core.wait_for_new_bound_socket(WAIT_TIMEOUT)
        }

        fn wait_for_bridge_frame_type(&self, socket_id: SocketId, frame_type: u8) {
            let _ = self
                .core
                .observer_rx()
                .recv_matching_or_fail(
                    WAIT_TIMEOUT,
                    |indication| {
                        let UdpIndication::Received {
                            socket_id: indicated_socket_id,
                            payload,
                            ..
                        } = indication
                        else {
                            return false;
                        };
                        *indicated_socket_id == socket_id
                            && payload.to_vec().first().copied() == Some(frame_type)
                    },
                    |indication| match indication {
                        UdpIndication::BindFailed {
                            request_id,
                            local_addr,
                            reason,
                        } => Some(format!(
                            "route-transport UDP bind request {request_id:?} failed at {local_addr} while waiting for frame type 0x{frame_type:02x}: {reason:?}"
                        )),
                        UdpIndication::Closed {
                            socket_id: indicated_socket_id,
                            remote_addr,
                            reason,
                        } if *indicated_socket_id == socket_id => Some(format!(
                            "UDP socket {socket_id:?} closed while waiting for frame type 0x{frame_type:02x}: remote={remote_addr:?}, reason={reason:?}"
                        )),
                        _ => None,
                    },
                );
        }

        fn wait_for_bridge_payload_frames(&self, socket_id: SocketId, min_count: usize) {
            for _ in 0..min_count {
                let _ = self
                    .core
                    .observer_rx()
                    .recv_matching_or_fail(
                        WAIT_TIMEOUT,
                        |indication| {
                            let UdpIndication::Received {
                                socket_id: indicated_socket_id,
                                payload,
                                ..
                            } = indication
                            else {
                                return false;
                            };
                            *indicated_socket_id == socket_id
                                && payload.to_vec().first().copied() == Some(0x01)
                        },
                        |indication| match indication {
                            UdpIndication::BindFailed {
                                request_id,
                                local_addr,
                                reason,
                            } => Some(format!(
                                "route-transport UDP bind request {request_id:?} failed at {local_addr} while waiting for payload frames on {socket_id:?}: {reason:?}"
                            )),
                            UdpIndication::Closed {
                                socket_id: indicated_socket_id,
                                remote_addr,
                                reason,
                            } if *indicated_socket_id == socket_id => Some(format!(
                                "UDP socket {socket_id:?} closed while waiting for payload frames: remote={remote_addr:?}, reason={reason:?}"
                            )),
                            _ => None,
                        },
                    );
            }
        }
    }

    #[test]
    fn udp_manager_sends_payload_over_real_socket() {
        let harness = UdpManagerHarness::with_socket_budgets(
            1,
            1,
            UdpActivationPolicy::OnBind,
            TestSendRateControl::default(),
            udpour_config(),
        );
        let receiver = harness.bind_reserved_udp_socket(0);
        receiver
            .set_read_timeout(Some(WAIT_TIMEOUT))
            .expect("set UDP read timeout");
        let receiver_addr = receiver.local_addr().expect("receiver local addr");

        let route = UdpRouteKey {
            remote_addr: receiver_addr,
            scope: DatagramRouteScope::Unicast,
            local_bind: None,
        };
        let send_id = RouteSendId(Uuid::new_v4());
        let coverage_key = harness.wait_for_send_ack(route_send(
            send_id,
            route,
            b"hello route transport".to_vec(),
        ));
        assert_eq!(coverage_key, TransportRouteKey::Udp(route));
        assert_eq!(harness.live_udp_socket_count(), 1);

        let mut buffer = [0u8; 2048];
        let (len, _source) = receiver
            .recv_from(&mut buffer)
            .expect("receive UDP datagram from managed sender");
        assert!(buffer[..len].ends_with(b"hello route transport"));
    }

    #[test]
    fn udp_manager_reuses_one_socket_for_two_loopback_targets() {
        let harness = UdpManagerHarness::with_socket_budgets(
            2,
            1,
            UdpActivationPolicy::OnBind,
            TestSendRateControl::default(),
            udpour_config(),
        );
        let receiver1 = harness.bind_reserved_udp_socket(0);
        let receiver2 = harness.bind_reserved_udp_socket(1);
        receiver1
            .set_read_timeout(Some(WAIT_TIMEOUT))
            .expect("set first UDP read timeout");
        receiver2
            .set_read_timeout(Some(WAIT_TIMEOUT))
            .expect("set second UDP read timeout");

        let route1 = UdpRouteKey {
            remote_addr: receiver1.local_addr().expect("first receiver addr"),
            scope: DatagramRouteScope::Unicast,
            local_bind: None,
        };
        let route2 = UdpRouteKey {
            remote_addr: receiver2.local_addr().expect("second receiver addr"),
            scope: DatagramRouteScope::Unicast,
            local_bind: None,
        };
        let send_id1 = RouteSendId(Uuid::new_v4());
        let send_id2 = RouteSendId(Uuid::new_v4());

        let submit1 = harness.send_async(route_send(send_id1, route1, b"first target".to_vec()));
        let submit2 = harness.send_async(route_send(send_id2, route2, b"second target".to_vec()));

        assert_eq!(
            UdpManagerHarness::wait_for_send_ack_future(submit1),
            TransportRouteKey::Udp(route1)
        );
        assert_eq!(
            UdpManagerHarness::wait_for_send_ack_future(submit2),
            TransportRouteKey::Udp(route2)
        );
        assert_eq!(harness.live_udp_socket_count(), 1);

        let mut buffer1 = [0u8; 2048];
        let mut buffer2 = [0u8; 2048];
        let (len1, source1) = receiver1
            .recv_from(&mut buffer1)
            .expect("receive first UDP datagram from managed sender");
        let (len2, source2) = receiver2
            .recv_from(&mut buffer2)
            .expect("receive second UDP datagram from managed sender");

        assert!(buffer1[..len1].ends_with(b"first target"));
        assert!(buffer2[..len2].ends_with(b"second target"));
        assert_eq!(source1, source2);
    }

    #[test]
    fn udp_manager_buffers_inbound_datagrams_while_socket_is_starting() {
        let harness = UdpManagerHarness::new();
        let socket_id = SocketId(7);
        let socket_key = UdpSocketKey {
            local_addr: "127.0.0.1:34567".parse().expect("socket key addr"),
        };
        let source: SocketAddr = "127.0.0.1:45678".parse().expect("source addr");

        harness.manager.on_definition(|component| {
            let runtime = component.ctx.system().create({
                let egress_pool = component.bridge.egress_pool().clone();
                let config = component.udpour_config.clone();
                move || UDPourComponent::new(socket_id, egress_pool, config)
            });
            let runtime_ref = runtime
                .actor_ref()
                .hold()
                .expect("child UDPour runtime must still be live after creation");
            let udp_port = runtime.required_ref();
            let transfer_channel = runtime.connect_to_required(component.required_ref());
            component.udp_socket_ids.insert(socket_id, socket_key);
            component.udp_starting_sockets.insert(
                socket_key,
                StartingUdpSocketHandle {
                    socket_id,
                    runtime,
                    runtime_ref,
                    udp_port,
                    transfer_channel,
                    origin: UdpSocketStartOrigin::ManagerOwned,
                    queued_sends: Vec::new(),
                    buffered_datagram_count: 0,
                },
            );
            component.handle_udp_received(
                socket_id,
                source,
                zero_length_udpour_payload(MessageId(77)),
            );

            let buffered_datagram_count = component
                .udp_starting_sockets
                .get(&socket_key)
                .expect("socket should still be starting")
                .buffered_datagram_count;
            assert_eq!(buffered_datagram_count, 1);
        });
    }

    #[test]
    fn udp_manager_starts_dormant_socket_on_first_inbound_udpour_message() {
        let receiver_harness = UdpManagerHarness::with_external_socket(
            UdpActivationPolicy::OnFirstUse,
            TestSendRateControl::default(),
            udpour_config_with_part_size(64),
        );
        let (receiver_socket_id, receiver_addr) =
            receiver_harness.bind_external_socket(UdpLocalBind::Exact(localhost(0)));
        let receiver_socket_key = UdpSocketKey {
            local_addr: receiver_addr,
        };
        receiver_harness.register_external_socket(receiver_socket_id, receiver_addr);
        receiver_harness.wait_for_dormant_socket(receiver_socket_key);
        assert_eq!(receiver_harness.dormant_udp_socket_count(), 1);
        assert_eq!(receiver_harness.live_udp_socket_count(), 0);

        let sender_harness = UdpManagerHarness::with_socket_budgets(
            0,
            1,
            UdpActivationPolicy::OnBind,
            TestSendRateControl {
                send_delay: Duration::from_millis(10),
                backpressure_retry_delay: Duration::from_millis(10),
                max_in_flight_datagrams: 1,
            },
            udpour_config_with_part_size(64),
        );
        let route = UdpRouteKey {
            remote_addr: receiver_addr,
            scope: DatagramRouteScope::Unicast,
            local_bind: None,
        };
        let send_id = RouteSendId(Uuid::new_v4());
        let multipart_payload = vec![0x5a; 64 * 16];

        let submit = sender_harness.send_async(route_send(send_id, route, multipart_payload));

        let (sender_socket_id, _sender_addr) = sender_harness.wait_for_new_bound_socket();
        let buffered_count =
            receiver_harness.wait_for_startup_buffered_datagrams(receiver_socket_key, 1);
        receiver_harness.wait_for_live_udp_socket(receiver_socket_key);
        receiver_harness.wait_for_bridge_payload_frames(receiver_socket_id, buffered_count + 3);

        assert_eq!(
            UdpManagerHarness::wait_for_send_ack_future(submit),
            TransportRouteKey::Udp(route)
        );
        sender_harness.wait_for_bridge_frame_type(sender_socket_id, 0x81);
    }

    #[test]
    fn udp_manager_ignores_unregistered_external_udp_bind() {
        let harness = UdpManagerHarness::with_config(
            UdpActivationPolicy::OnFirstUse,
            TestSendRateControl::default(),
            udpour_config(),
        );
        let socket_id = SocketId(77);
        let local_addr = localhost(34568);
        let socket_key = UdpSocketKey { local_addr };

        harness.manager.on_definition(|component| {
            component.handle_udp_bound(UdpOpenRequestId::new(), socket_id, local_addr);

            assert!(!component.udp_socket_ids.contains_key(&socket_id));
            assert!(!component.udp_dormant_sockets.contains_key(&socket_key));
            assert!(!component.udp_starting_sockets.contains_key(&socket_key));
            assert!(!component.udp_sockets.contains_key(&socket_key));
        });
    }

    #[test]
    fn udp_manager_registers_lifecycle_authorised_route_endpoint() {
        let harness = UdpManagerHarness::with_config(
            UdpActivationPolicy::OnFirstUse,
            TestSendRateControl::default(),
            udpour_config(),
        );
        let lifecycle_probe = harness
            .core
            .system()
            .create(RouteEndpointLifecyclePort::tester_component_sidecar);
        let lifecycle_probe_ref = lifecycle_probe.actor_ref();
        biconnect_components::<RouteEndpointLifecyclePort, _, _>(
            &harness.manager,
            &lifecycle_probe,
        )
        .expect("connect accepted route endpoint lifecycle probe");
        start_component(harness.core.system(), &lifecycle_probe);

        let socket_id = SocketId(78);
        let local_addr = localhost(34569);
        let socket_key = UdpSocketKey { local_addr };
        let available_future = lifecycle_probe_ref.observe_indication(move |indication| {
            *indication
                == RouteEndpointLifecycle::Available(RouteEndpointBinding {
                    socket_id,
                    socket_bound_addr: local_addr,
                })
        });

        harness.publish_route_endpoint_available(socket_id, local_addr);

        eventually_component_state(
            WAIT_TIMEOUT,
            &harness.manager,
            |component| {
                component.udp_socket_ids.get(&socket_id) == Some(&socket_key)
                    && component.udp_dormant_sockets.contains_key(&socket_key)
            },
            "route endpoint lifecycle should register a dormant UDP socket",
        );
        let available = available_future
            .wait_timeout(WAIT_TIMEOUT)
            .expect("accepted route endpoint lifecycle should publish availability")
            .expect("accepted lifecycle probe should stay live");

        let unavailable_future =
            lifecycle_probe_ref.observe_indication_from(available.index() + 1, move |indication| {
                *indication
                    == RouteEndpointLifecycle::Unavailable {
                        binding: RouteEndpointBinding {
                            socket_id,
                            socket_bound_addr: local_addr,
                        },
                        reason: RouteEndpointUnavailableReason::Closed {
                            reason: UdpCloseReason::Requested,
                        },
                    }
            });

        harness.publish_route_endpoint_unavailable(socket_id, local_addr);

        eventually_component_state(
            WAIT_TIMEOUT,
            &harness.manager,
            |component| {
                !component.udp_socket_ids.contains_key(&socket_id)
                    && !component.udp_dormant_sockets.contains_key(&socket_key)
                    && !component.udp_starting_sockets.contains_key(&socket_key)
                    && !component.udp_sockets.contains_key(&socket_key)
            },
            "route endpoint lifecycle should remove the dormant UDP socket",
        );
        unavailable_future
            .wait_timeout(WAIT_TIMEOUT)
            .expect("accepted route endpoint lifecycle should publish unavailability")
            .expect("accepted lifecycle probe should stay live");
    }

    #[test]
    fn udp_manager_restores_dormant_external_socket_after_activation_failure() {
        let harness = UdpManagerHarness::with_config(
            UdpActivationPolicy::OnFirstUse,
            TestSendRateControl::default(),
            udpour_config(),
        );
        let local_addr = localhost(34567);
        let socket_key = UdpSocketKey { local_addr };
        let socket_id = SocketId(77);
        let route = UdpRouteKey {
            remote_addr: localhost(45678),
            scope: DatagramRouteScope::Unicast,
            local_bind: Some(local_addr),
        };
        let send_id = RouteSendId(Uuid::new_v4());
        let (promise, submit) = promise::<TransportRouteTransportSubmitResult>();

        harness.manager.on_definition(|component| {
            component.pending_sends.insert(
                send_id,
                PendingRouteSend {
                    send: route_send(send_id, route, b"retry me".to_vec()),
                    submit_promise: Some(promise),
                },
            );
            component.udp_socket_ids.insert(socket_id, socket_key);
            component.handle_udp_socket_activation_failed(
                socket_key,
                socket_id,
                UdpSocketStartOrigin::ExternalDormant,
                vec![QueuedUdpSend { send_id, route }],
                &ConnectionFailureReason::TimedOut,
            );
        });

        harness.wait_for_dormant_socket(socket_key);
        assert_eq!(harness.dormant_udp_socket_count(), 1);
        let (coverage_key, reason) = UdpManagerHarness::wait_for_send_nack(submit);
        assert_eq!(coverage_key, TransportRouteKey::Udp(route));
        assert_eq!(reason, RouteTransportNackReason::RouteUnavailable);
    }

    fn route_send(
        send_id: RouteSendId,
        route: UdpRouteKey,
        bytes: Vec<u8>,
    ) -> TransportRouteTransportSend {
        RouteTransportSend {
            send_id,
            route: SendRouteCandidate {
                coverage_key: TransportRouteKey::Udp(route),
                sharing: RouteSharingKind::Exclusive,
                preference_rank: RoutePreferenceRank::UNRANKED,
            },
            payload: Arc::new(BytesPayload(bytes)),
        }
    }

    fn udpour_config() -> UDPourConfig {
        udpour_config_with_part_size(1024)
    }

    fn udpour_config_with_part_size(max_part_payload_len: usize) -> UDPourConfig {
        let sender = SenderConfig {
            max_part_payload_len: NonZeroUsize::new(max_part_payload_len)
                .expect("test UDPour config must use a non-zero part payload length"),
            retention_timeout: Duration::from_secs(1),
            id_reuse_cooldown: Duration::from_millis(100),
            eager_ack_cleanup: false,
        };
        let receiver = ReceiverConfig {
            max_need_parts_frame_len: 1024,
            repair_interval: Duration::from_millis(100),
            give_up_timeout: Duration::from_secs(1),
            delivered_tombstone_timeout: Duration::ZERO,
        };
        UDPourConfig::new(sender, receiver).expect("valid datagram config")
    }

    fn build_manager_test_kompact_system(
        activation_policy: UdpActivationPolicy,
        send_rate_control: TestSendRateControl,
    ) -> KompactSystem {
        build_test_kompact_system_with(|config| {
            set_test_system_label(config, "route-transport-manager-test-system");
            enable_bind_reuse_address(config);
            let activation_policy_value = match activation_policy {
                UdpActivationPolicy::OnBind => 0usize,
                UdpActivationPolicy::OnFirstUse => 1usize,
            };
            config.set_config_value(&config_keys::UDP_ACTIVATION_POLICY, activation_policy_value);
            config.set_config_value(
                &udpour_config_keys::SEND_DELAY,
                send_rate_control.send_delay,
            );
            config.set_config_value(
                &udpour_config_keys::BACKPRESSURE_RETRY_DELAY,
                send_rate_control.backpressure_retry_delay,
            );
            config.set_config_value(
                &udpour_config_keys::MAX_IN_FLIGHT_DATAGRAMS,
                send_rate_control.max_in_flight_datagrams,
            );
        })
    }

    fn zero_length_udpour_payload(message_id: MessageId) -> IoPayload {
        let mut bytes = Vec::with_capacity(20);
        bytes.push(0x01);
        bytes.push(1);
        bytes.push(0);
        bytes.push(0);
        bytes.extend_from_slice(&message_id.0.to_be_bytes());
        bytes.extend_from_slice(&0_u32.to_be_bytes());
        bytes.extend_from_slice(&1_u32.to_be_bytes());
        bytes.extend_from_slice(&0_u32.to_be_bytes());
        IoPayload::from(Bytes::from(bytes))
    }
}
