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
use flotsync_utils::{OptionExt as _, kompact_config::ConfigReadExt as _};
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
        let raw = self
            .ctx
            .config()
            .read_or_default_warn(self.log(), &config_keys::UDP_ACTIVATION_POLICY);
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

mod state;
#[cfg(test)]
mod tests;

use state::{
    DormantUdpSocketHandle,
    LiveTcpRouteHandle,
    LiveUdpSendAction,
    LiveUdpSocketHandle,
    PendingRouteSend,
    PendingUdpSocketOpen,
    QueuedUdpSend,
    StartingUdpSocketHandle,
    UdpBroadcastState,
    UdpSocketKey,
    UdpSocketStartOrigin,
};

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
