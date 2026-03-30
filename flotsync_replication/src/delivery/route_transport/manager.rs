//! UDP-first route-transport manager.
//!
//! This component now owns the concrete UDP route path end to end:
//!
//! - opening UDP sockets on demand through the shared bridge port
//! - creating one child UDPour runtime per live UDP socket
//! - serializing `FlotsyncSerializable` payloads into pooled `IoPayload`
//! - mapping directed UDPour submit results onto `RouteTransportPort`
//!
//! TCP route handling is intentionally deferred via `TODO(flotsync-638)`.

use super::*;
use crate::delivery::shared::RouteSendId;
use bytes::Bytes;
use flotsync_io::prelude::{
    IoBridgeHandle,
    IoPayload,
    OpenFailureReason,
    SendFailureReason,
    SocketId,
    UdpCloseReason,
    UdpIndication,
    UdpLocalBind,
    UdpOpenRequestId,
    UdpPort,
    UdpRequest,
    UdpSocketOption,
};
use flotsync_udpour::{
    UDPourComponent,
    UDPourComponentMessage,
    UDPourConfig,
    UDPourPort,
    UDPourPortIndication,
    UDPourSend,
    UDPourSendFailureReason,
    UDPourSubmitResult,
};
use kompact::{Never, prelude::*};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
};

type TransportConnectionInfoPort = ConnectionInfoPort<TransportRouteKey>;
type TransportRouteTransportSend = RouteTransportSend<TransportRouteKey>;
type TransportRouteTransportPort = RouteTransportPort<TransportRouteKey>;

/// Upper bound on inbound UDP datagrams forwarded into one per-socket UDPour child while it is
/// still starting.
///
/// During this short bring-up window the manager does not keep its own copy of incoming datagrams.
/// Instead it enqueues exact `UdpIndication::Received` events onto the starting child's required
/// `UdpPort` via `KompactSystem::trigger_i`. This counter only exists to bound that queueing.
const MAX_BUFFERED_STARTUP_DATAGRAMS: usize = 64;

/// Concrete route-transport manager for the current UDP/TCP key type.
///
/// UDP is fully owned here via child `UDPourComponent`s. TCP is left
/// as a follow-up.
#[derive(ComponentDefinition)]
pub struct RouteTransportManager {
    ctx: ComponentContext<Self>,
    transport_port: ProvidedPort<TransportRouteTransportPort>,
    connection_info_port: ProvidedPort<TransportConnectionInfoPort>,
    udp_bridge_port: RequiredPort<UdpPort>,
    /// Inbound UDPour deliveries from child runtimes.
    udpour_port: RequiredPort<UDPourPort>,
    /// Concrete system handle needed for direct `trigger_i` startup queueing.
    system: KompactSystem,
    /// Shared bridge handle used to open transport resources on demand.
    bridge: IoBridgeHandle,
    /// Runtime configuration shared by every live UDP UDPour child.
    udpour_config: UDPourConfig,
    /// UDP local sockets currently waiting for a bind result.
    ///
    /// This is keyed by the reusable local-socket shape rather than the full
    /// route key so multiple targets can converge onto one child runtime.
    udp_open_sockets: HashMap<UdpSocketKey, PendingUdpSocketOpen>,
    /// Correlates bridge-local UDP open ids back to the socket key being opened.
    udp_open_requests: HashMap<UdpOpenRequestId, UdpSocketKey>,
    /// Sockets that are already bound but whose UDPour child is still starting.
    udp_starting_sockets: HashMap<UdpSocketKey, StartingUdpSocketHandle>,
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
}

impl RouteTransportManager {
    /// Creates one manager around the shared bridge handle and UDP child config.
    pub fn new(system: KompactSystem, bridge: IoBridgeHandle, udpour_config: UDPourConfig) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            transport_port: ProvidedPort::uninitialised(),
            connection_info_port: ProvidedPort::uninitialised(),
            udp_bridge_port: RequiredPort::uninitialised(),
            udpour_port: RequiredPort::uninitialised(),
            system,
            bridge,
            udpour_config,
            udp_open_sockets: HashMap::new(),
            udp_open_requests: HashMap::new(),
            udp_starting_sockets: HashMap::new(),
            udp_broadcast_configurations: HashMap::new(),
            udp_socket_ids: HashMap::new(),
            udp_sockets: HashMap::new(),
            tcp_routes: HashMap::new(),
            pending_sends: HashMap::new(),
        }
    }

    fn handle_udp_route_send(&mut self, send: TransportRouteTransportSend, route: UdpRouteKey) {
        let send_id = send.send_id;
        let socket_key = UdpSocketKey::for_route(route);
        self.pending_sends
            .insert(send_id, PendingRouteSend { send });

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

        let request_id = UdpOpenRequestId::default();
        let bind = socket_key.bind_policy();
        self.udp_open_sockets.insert(
            socket_key,
            PendingUdpSocketOpen {
                queued_sends: vec![QueuedUdpSend { send_id, route }],
            },
        );
        self.udp_open_requests.insert(request_id, socket_key);
        self.udp_bridge_port
            .trigger(UdpRequest::Bind { request_id, bind });
    }

    fn handle_udp_bound(&mut self, request_id: UdpOpenRequestId, socket_id: SocketId) {
        let Some(socket_key) = self.udp_open_requests.remove(&request_id) else {
            return;
        };
        let Some(open) = self.udp_open_sockets.remove(&socket_key) else {
            return;
        };
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
                queued_sends: open.queued_sends,
                buffered_datagram_count: 0,
            },
        );
        self.spawn_local(move |mut async_self| async move {
            async_self.finish_udp_socket_activation(socket_key).await;
            Handled::Ok
        });
    }

    fn handle_udp_bind_failed(&mut self, request_id: UdpOpenRequestId, reason: OpenFailureReason) {
        let Some(socket_key) = self.udp_open_requests.remove(&request_id) else {
            return;
        };
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
            self.udp_socket_ids.remove(&starting.socket_id);
            let mut failed_routes = HashSet::new();
            for queued in &starting.queued_sends {
                failed_routes.insert(queued.route);
            }
            let discovery_reason = classify_udp_connect_failure_for_discovery(&error);
            for route in failed_routes {
                self.report_route_failed(TransportRouteKey::Udp(route), discovery_reason.clone());
            }
            for queued in starting.queued_sends {
                self.fail_pending_send(
                    queued.send_id,
                    TransportRouteKey::Udp(queued.route),
                    RouteTransportNackReason::RouteUnavailable,
                );
            }
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
            if route.scope != DatagramRouteScope::Broadcast {
                LiveUdpSendAction::Dispatch
            } else {
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
            Handled::Ok
        });
    }

    async fn dispatch_udp_send(
        &mut self,
        send_id: RouteSendId,
        route: UdpRouteKey,
        socket_key: UdpSocketKey,
    ) {
        let Some(pending) = self.pending_sends.get(&send_id).cloned() else {
            return;
        };
        let Some(handle) = self.udp_sockets.get(&socket_key) else {
            self.fail_pending_send(
                send_id,
                pending.send.route.coverage_key,
                RouteTransportNackReason::RouteUnavailable,
            );
            return;
        };
        let runtime_ref = handle.runtime_ref.clone();
        let coverage_key = pending.send.route.coverage_key;
        let payload = match serialize_payload(self.bridge.egress_pool(), pending.send.payload).await
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
            Ok(UDPourSubmitResult::Sent { .. }) => {
                let Some(_) = self.pending_sends.remove(&send_id) else {
                    return;
                };
                self.transport_port
                    .trigger(RouteTransportPortIndication::SendAck {
                        send_id,
                        coverage_key,
                    });
            }
            Ok(UDPourSubmitResult::SendFailed { reason, .. }) => {
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
        let Some(_) = self.pending_sends.remove(&send_id) else {
            return;
        };
        self.transport_port
            .trigger(RouteTransportPortIndication::SendNack {
                send_id,
                coverage_key,
                reason,
            });
    }

    fn report_route_failed(&mut self, route: TransportRouteKey, reason: ConnectionFailureReason) {
        self.connection_info_port
            .trigger(ConnectionInfoIndication::ReportRouteFailed { route, reason });
    }

    fn handle_udp_runtime_indication(&mut self, indication: UDPourPortIndication) -> Handled {
        match indication {
            UDPourPortIndication::Deliver(deliver) => {
                let _ = deliver;
                // TODO(flotsync-zup): Route inbound UDPour deliveries into the
                // semantic-delivery demux once inbound ownership is specified.
            }
        }
        Handled::Ok
    }

    fn handle_udp_received(&mut self, socket_id: SocketId, source: SocketAddr, payload: IoPayload) {
        let Some(socket_key) = self.udp_socket_ids.get(&socket_id).copied() else {
            return;
        };
        if self.udp_sockets.contains_key(&socket_key) {
            return;
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
        let Some(socket_key) = self.udp_socket_ids.remove(&socket_id) else {
            return;
        };
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
        self.udp_broadcast_configurations.clear();
        self.udp_socket_ids.clear();
        self.pending_sends.clear();
    }
}

impl ComponentLifecycle for RouteTransportManager {
    fn on_start(&mut self) -> Handled {
        Handled::Ok
    }

    fn on_stop(&mut self) -> Handled {
        self.shutdown_children();
        Handled::Ok
    }

    fn on_kill(&mut self) -> Handled {
        self.shutdown_children();
        Handled::Ok
    }
}

impl Provide<TransportRouteTransportPort> for RouteTransportManager {
    fn handle(&mut self, request: TransportRouteTransportSend) -> Handled {
        match request.route.coverage_key {
            TransportRouteKey::Udp(route) => {
                self.handle_udp_route_send(request, route);
                Handled::Ok
            }
            TransportRouteKey::Tcp(_) => {
                todo!("TODO(flotsync-638): implement TCP route transport backend")
            }
        }
    }
}

impl Provide<TransportConnectionInfoPort> for RouteTransportManager {
    fn handle(&mut self, request: Never) -> Handled {
        match request {}
    }
}

impl Require<UDPourPort> for RouteTransportManager {
    fn handle(&mut self, indication: UDPourPortIndication) -> Handled {
        self.handle_udp_runtime_indication(indication)
    }
}

impl Require<UdpPort> for RouteTransportManager {
    fn handle(&mut self, indication: UdpIndication) -> Handled {
        match indication {
            UdpIndication::Bound {
                request_id,
                socket_id,
                ..
            } => self.handle_udp_bound(request_id, socket_id),
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
        Handled::Ok
    }
}

impl LocalActor for RouteTransportManager {
    type Message = Never;

    fn receive(&mut self, msg: Self::Message) -> Handled {
        match msg {}
    }
}

impl_local_actor!(RouteTransportManager);

/// Live UDP route handle owned by the manager.
struct LiveUdpSocketHandle {
    socket_id: SocketId,
    runtime: Arc<Component<UDPourComponent>>,
    runtime_ref: ActorRefStrong<UDPourComponentMessage>,
    _transfer_channel: ProviderChannel<UDPourPort, UDPourComponent>,
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
#[derive(Clone)]
struct PendingRouteSend {
    send: TransportRouteTransportSend,
}

/// UDP route that has been requested but is not yet ready for sends.
struct PendingUdpSocketOpen {
    queued_sends: Vec<QueuedUdpSend>,
}

/// One bound UDP socket whose child runtime is still being connected and started.
struct StartingUdpSocketHandle {
    socket_id: SocketId,
    runtime: Arc<Component<UDPourComponent>>,
    runtime_ref: ActorRefStrong<UDPourComponentMessage>,
    udp_port: RequiredRef<UdpPort>,
    transfer_channel: ProviderChannel<UDPourPort, UDPourComponent>,
    /// Logical sends queued while the child runtime is not yet ready.
    queued_sends: Vec<QueuedUdpSend>,
    /// Number of raw UDP datagrams queued onto the child before it was started.
    buffered_datagram_count: usize,
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
/// bind shape they resolve to so one child UDPour runtime can multiplex many
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

async fn serialize_payload(
    egress_pool: &flotsync_io::prelude::EgressPool,
    payload: Arc<dyn FlotsyncSerializable>,
) -> Result<IoPayload, FlotsyncSerializeError> {
    let hint = match payload.serialized_size_hint() {
        SizeHint::Unknown => None,
        SizeHint::Exact(bytes) | SizeHint::UpperBound(bytes) | SizeHint::Estimate(bytes) => {
            Some(bytes)
        }
    };
    let mut writer = egress_pool.writer(hint);
    payload.serialize_into(&mut writer).await?;
    let payload = writer
        .finish()
        .map_err(|source| FlotsyncSerializeError::Io { source })?;
    Ok(payload.unwrap_or_else(|| IoPayload::from(Bytes::new())))
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
        OpenFailureReason::DriverUnavailable => RouteTransportNackReason::RouteUnavailable,
        OpenFailureReason::Io(_) => RouteTransportNackReason::RouteUnavailable,
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
    use bytes::Bytes;
    use flotsync_io::{
        pool::PayloadWriter,
        prelude::{DriverConfig, IoBridge, IoDriverComponent},
        test_support::{WAIT_TIMEOUT, build_test_kompact_system, start_component},
    };
    use flotsync_udpour::{MessageId, ReceiverConfig, SenderConfig};
    use flotsync_utils::BoxFuture;
    use std::{
        net::{SocketAddr, UdpSocket},
        sync::mpsc,
        time::Duration,
    };
    use uuid::Uuid;

    #[derive(ComponentDefinition)]
    struct RouteTransportProbe {
        ctx: ComponentContext<Self>,
        transport: RequiredPort<TransportRouteTransportPort>,
        indications: mpsc::Sender<RouteTransportPortIndication<TransportRouteKey>>,
    }

    impl RouteTransportProbe {
        fn new(indications: mpsc::Sender<RouteTransportPortIndication<TransportRouteKey>>) -> Self {
            Self {
                ctx: ComponentContext::uninitialised(),
                transport: RequiredPort::uninitialised(),
                indications,
            }
        }
    }

    ignore_lifecycle!(RouteTransportProbe);

    impl Require<TransportRouteTransportPort> for RouteTransportProbe {
        fn handle(
            &mut self,
            indication: RouteTransportPortIndication<TransportRouteKey>,
        ) -> Handled {
            self.indications
                .send(indication)
                .expect("route transport probe receiver must stay live during tests");
            Handled::Ok
        }
    }

    impl Actor for RouteTransportProbe {
        type Message = Never;

        fn receive_local(&mut self, _msg: Self::Message) -> Handled {
            unreachable!("Never type is empty")
        }

        fn receive_network(&mut self, _msg: NetMessage) -> Handled {
            unreachable!("workspace tests do not use Kompact network actor messages")
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
            Box::pin(async move {
                writer
                    .write_slice(&self.0)
                    .await
                    .map_err(|source| FlotsyncSerializeError::Io { source })?;
                Ok(())
            })
        }
    }

    struct UdpManagerHarness {
        system: KompactSystem,
        driver: Arc<Component<IoDriverComponent>>,
        bridge: Arc<Component<IoBridge>>,
        manager: Arc<Component<RouteTransportManager>>,
        probe: Arc<Component<RouteTransportProbe>>,
        _bridge_to_manager: TwoWayChannel<UdpPort, IoBridge, RouteTransportManager>,
        _manager_to_probe:
            TwoWayChannel<TransportRouteTransportPort, RouteTransportManager, RouteTransportProbe>,
        indications: mpsc::Receiver<RouteTransportPortIndication<TransportRouteKey>>,
    }

    impl UdpManagerHarness {
        fn new() -> Self {
            let system = build_test_kompact_system();
            let driver = system.create(|| IoDriverComponent::new(DriverConfig::default()));
            let driver_for_bridge = driver.clone();
            let bridge = system.create(move || IoBridge::new(&driver_for_bridge));
            let bridge_handle = IoBridgeHandle::from_component(&bridge);
            let config = udpour_config();
            let manager_system = system.clone();
            let manager = system
                .create(move || RouteTransportManager::new(manager_system, bridge_handle, config));
            let (indications_tx, indications_rx) = mpsc::channel();
            let probe = system.create(move || RouteTransportProbe::new(indications_tx));

            let bridge_to_manager = biconnect_components::<UdpPort, _, _>(&bridge, &manager)
                .expect("bridge must connect to route transport manager");
            let manager_to_probe =
                biconnect_components::<TransportRouteTransportPort, _, _>(&manager, &probe)
                    .expect("manager must connect to transport probe");

            start_component(&system, &driver);
            start_component(&system, &bridge);
            start_component(&system, &manager);
            start_component(&system, &probe);

            Self {
                system,
                driver,
                bridge,
                manager,
                probe,
                _bridge_to_manager: bridge_to_manager,
                _manager_to_probe: manager_to_probe,
                indications: indications_rx,
            }
        }

        fn send(&self, send: TransportRouteTransportSend) {
            self.probe.on_definition(|component| {
                component.transport.trigger(send);
            });
        }

        fn wait_for_send_ack(&self, send_id: RouteSendId) -> TransportRouteKey {
            loop {
                let indication = self
                    .indications
                    .recv_timeout(WAIT_TIMEOUT)
                    .expect("timed out waiting for route transport indication");
                match indication {
                    RouteTransportPortIndication::SendAck {
                        send_id: observed_send_id,
                        coverage_key,
                    } if observed_send_id == send_id => return coverage_key,
                    RouteTransportPortIndication::SendNack {
                        send_id: observed_send_id,
                        reason,
                        ..
                    } if observed_send_id == send_id => {
                        panic!("unexpected SendNack for {send_id:?}: {reason:?}")
                    }
                    _ => {}
                }
            }
        }

        fn live_udp_socket_count(&self) -> usize {
            self.manager
                .on_definition(|component| component.udp_sockets.len())
        }
    }

    impl Drop for UdpManagerHarness {
        fn drop(&mut self) {
            let _ = self
                .system
                .kill_notify(self.probe.clone())
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
        }
    }

    #[test]
    fn udp_manager_sends_payload_over_real_socket() {
        let harness = UdpManagerHarness::new();
        let receiver = UdpSocket::bind("127.0.0.1:0").expect("bind raw UDP receiver");
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
        harness.send(route_send(
            send_id,
            route,
            b"hello route transport".to_vec(),
        ));

        let coverage_key = harness.wait_for_send_ack(send_id);
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
        let harness = UdpManagerHarness::new();
        let receiver1 = UdpSocket::bind("127.0.0.1:0").expect("bind first raw UDP receiver");
        let receiver2 = UdpSocket::bind("127.0.0.1:0").expect("bind second raw UDP receiver");
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

        harness.send(route_send(send_id1, route1, b"first target".to_vec()));
        harness.send(route_send(send_id2, route2, b"second target".to_vec()));

        assert_eq!(
            harness.wait_for_send_ack(send_id1),
            TransportRouteKey::Udp(route1)
        );
        assert_eq!(
            harness.wait_for_send_ack(send_id2),
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

    // TODO(flotsync-ml0): Add a real end-to-end startup-buffering smoke test here once UDPour
    // send-rate control exists. That should pace a multi-part sender slowly enough that several
    // parts are queued through the manager before the per-socket child starts and several more
    // arrive afterward on the normal live path.

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
        let sender = SenderConfig {
            max_part_payload_len: 1024,
            retention_timeout: Duration::from_secs(1),
            reuse_cooldown: Duration::from_millis(100),
            eager_ack_cleanup: false,
        };
        let receiver = ReceiverConfig {
            max_need_parts_frame_len: 1024,
            repair_interval: Duration::from_millis(100),
            give_up_timeout: Duration::from_secs(1),
        };
        UDPourConfig::new(sender, receiver).expect("valid datagram config")
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
