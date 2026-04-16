use super::ReplicationRuntimeComponent;
use crate::{
    GroupMemberships,
    SharedGroupMemberships,
    api::{MemberIdentity, ReplicationEventListener, ReplicationStore},
    delivery::{
        contracts::{GroupBroadcastPort, ReliableDeliveryPort},
        group_broadcast::GroupBroadcastComponent,
        ingress::{DeliveryIngressComponent, DeliveryInterestConfig},
        reliable_delivery::ReliableDeliveryComponent,
        route_transport::{
            DiscoveryRouteUpdate,
            RouteDiscoveryPort,
            RouteTransportPort,
            TransportRouteKey,
            manager::{RouteTransportManager, configure_replication_runtime},
        },
    },
};
use flotsync_io::prelude::{
    DriverConfig,
    IoBridge,
    IoBridgeHandle,
    IoDriverComponent,
    SocketId,
    UdpIndication,
    UdpLocalBind,
    UdpOpenRequestId,
    UdpPort,
    UdpRequest,
};
use flotsync_udpour::UDPourConfig;
use flotsync_utils::{LocalActor, impl_local_actor};
use kompact::{
    config::{DurationValue, HoconExt, StringValue},
    kompact_config,
    prelude::*,
};
use snafu::prelude::*;
use std::{collections::HashSet, net::SocketAddr, sync::Arc, time::Duration};

type TransportRoutePort = RouteTransportPort<TransportRouteKey>;
type GroupBroadcastInboundRoutePort =
    crate::delivery::group_broadcast::GroupBroadcastInboundPort<TransportRouteKey>;
type ReliableDeliveryInboundRoutePort =
    crate::delivery::reliable_delivery::ReliableDeliveryInboundPort<TransportRouteKey>;

mod config_keys {
    use super::*;

    fn default_local_endpoint_bind_addr() -> String {
        if cfg!(test) {
            String::from("127.0.0.1:0")
        } else {
            String::from("0.0.0.0:0")
        }
    }

    kompact_config! {
        CONTROL_TIMEOUT,
        key = "flotsync.replication.runtime-host.control-timeout",
        type = DurationValue,
        default = Duration::from_secs(5),
        doc = "Maximum wait for startup, bind, and shutdown control operations in the replication runtime host.",
        version = "0.1.0"
    }

    kompact_config! {
        LOCAL_ENDPOINT_BIND_ADDR,
        key = "flotsync.replication.runtime-host.local-endpoint-bind-addr",
        type = StringValue,
        default = default_local_endpoint_bind_addr(),
        doc = "Configured local UDP bind address owned by the temporary replication runtime endpoint binder.",
        version = "0.1.0"
    }
}

/// Startup and shutdown failures for the internal delivery runtime host.
#[derive(Debug, Snafu)]
pub(crate) enum RuntimeHostError {
    #[snafu(display("Failed to build Kompact system for the replication runtime: {message}"))]
    BuildSystem { message: String },
    #[snafu(display("Invalid replication runtime host config at {key}: {message}"))]
    InvalidConfig { key: &'static str, message: String },
    #[snafu(display("Failed to start runtime component '{component}': {message}"))]
    StartComponent {
        component: &'static str,
        message: String,
    },
    #[snafu(display("Failed to connect runtime bridge UDP to route transport: {message}"))]
    ConnectUdp { message: String },
    #[snafu(display("Failed to bind the runtime local delivery endpoint: {message}"))]
    BindLocalEndpoint { message: String },
    #[snafu(display("Failed to connect runtime components for {link}: {message}"))]
    ConnectComponents { link: &'static str, message: String },
}

#[derive(Clone, Copy, Debug)]
struct DeliveryRuntimeHostConfig {
    control_timeout: Duration,
    local_endpoint_bind_addr: SocketAddr,
}

impl DeliveryRuntimeHostConfig {
    fn from_system_config(system: &KompactSystem) -> Result<Self, RuntimeHostError> {
        let control_timeout = system
            .config()
            .get_or_default(&config_keys::CONTROL_TIMEOUT)
            .map_err(|error| RuntimeHostError::InvalidConfig {
                key: config_keys::CONTROL_TIMEOUT.key,
                message: error.to_string(),
            })?;
        let local_endpoint_bind_addr = system
            .config()
            .get_or_default(&config_keys::LOCAL_ENDPOINT_BIND_ADDR)
            .map_err(|error| RuntimeHostError::InvalidConfig {
                key: config_keys::LOCAL_ENDPOINT_BIND_ADDR.key,
                message: error.to_string(),
            })?;
        let local_endpoint_bind_addr =
            local_endpoint_bind_addr
                .parse()
                .map_err(
                    |error: std::net::AddrParseError| RuntimeHostError::InvalidConfig {
                        key: config_keys::LOCAL_ENDPOINT_BIND_ADDR.key,
                        message: error.to_string(),
                    },
                )?;
        Ok(Self {
            control_timeout,
            local_endpoint_bind_addr,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct LocalEndpointBinding {
    socket_id: SocketId,
    local_addr: SocketAddr,
}

/// Live internal host for the delivery-layer components used by replication.
///
/// This owns the concrete Kompact/io graph and exposes a small imperative
/// surface that later replication logic can build on without knowing transport
/// internals.
///
/// TODO(flotsync-3ht): Revisit whether this imperative host should remain the
/// top-level orchestrator at all, or whether ReplicationRuntimeComponent
/// should own more of the topology startup and wiring directly.
pub(crate) struct DeliveryRuntimeHost {
    system: KompactSystem,
    graph: Option<DeliveryRuntimeGraph>,
    group_memberships: SharedGroupMemberships,
    control_timeout: Duration,
    #[cfg_attr(not(test), allow(dead_code))]
    external_udp_addr: SocketAddr,
}

struct DeliveryRuntimeGraph {
    driver: Arc<Component<IoDriverComponent>>,
    bridge: Arc<Component<IoBridge>>,
    manager: Arc<Component<RouteTransportManager>>,
    ingress: Arc<Component<DeliveryIngressComponent>>,
    group_broadcast: Arc<Component<GroupBroadcastComponent>>,
    reliable_delivery: Arc<Component<ReliableDeliveryComponent>>,
    discovery_source: Arc<Component<RuntimeDiscoverySource>>,
    discovery_source_ref: ActorRefStrong<RuntimeDiscoverySourceMessage>,
    local_endpoint_manager: Arc<Component<LocalEndpointManager>>,
    runtime_component: Option<Arc<Component<ReplicationRuntimeComponent>>>,
}

impl DeliveryRuntimeHost {
    fn graph(&self) -> &DeliveryRuntimeGraph {
        self.graph
            .as_ref()
            .expect("delivery runtime host graph must still be live")
    }

    /// Start one new delivery runtime host for a single local member.
    pub(crate) fn new(local_member: MemberIdentity) -> Result<Self, RuntimeHostError> {
        let system = build_runtime_system()?;
        let host_config = DeliveryRuntimeHostConfig::from_system_config(&system)?;
        let group_memberships = SharedGroupMemberships::default();
        let graph = Self::build_graph(&system, &group_memberships, &local_member);
        Self::connect_graph(&graph)?;
        Self::start_graph(&system, &graph, host_config.control_timeout)?;
        let local_endpoint = bind_local_endpoint(
            &graph.local_endpoint_manager,
            host_config.local_endpoint_bind_addr,
            host_config.control_timeout,
        )?;

        Ok(Self {
            system,
            graph: Some(graph),
            group_memberships,
            control_timeout: host_config.control_timeout,
            external_udp_addr: local_endpoint.local_addr,
        })
    }

    pub(crate) fn start_runtime_component(
        &mut self,
        local_member: MemberIdentity,
        store: Arc<dyn ReplicationStore>,
        listener: Arc<dyn ReplicationEventListener>,
    ) -> Result<Arc<Component<ReplicationRuntimeComponent>>, RuntimeHostError> {
        let runtime_memberships = self.group_memberships.clone();
        let runtime_component = self.system.create(move || {
            ReplicationRuntimeComponent::new(
                local_member.clone(),
                store.clone(),
                listener.clone(),
                runtime_memberships.clone(),
            )
        });
        let graph = self.graph();

        connect_components_with_context::<GroupBroadcastPort, _, _>(
            &graph.group_broadcast,
            &runtime_component,
            "group broadcast -> replication runtime",
        )?;
        connect_components_with_context::<ReliableDeliveryPort, _, _>(
            &graph.reliable_delivery,
            &runtime_component,
            "reliable delivery -> replication runtime",
        )?;
        start_component(
            &self.system,
            &runtime_component,
            "replication_runtime",
            self.control_timeout,
        )?;
        self.graph
            .as_mut()
            .expect("delivery runtime host graph must still be live")
            .runtime_component = Some(runtime_component.clone());

        Ok(runtime_component)
    }

    fn build_graph(
        system: &KompactSystem,
        group_memberships: &SharedGroupMemberships,
        local_member: &MemberIdentity,
    ) -> DeliveryRuntimeGraph {
        let driver = system.create(|| IoDriverComponent::new(DriverConfig::default()));
        let driver_for_bridge = driver.clone();
        let bridge = system.create(move || IoBridge::new(&driver_for_bridge));
        let bridge_handle = IoBridgeHandle::from_component(&bridge);
        let manager_system = system.clone();
        let manager = system.create(move || {
            RouteTransportManager::new(manager_system, bridge_handle, UDPourConfig::default())
        });
        let manager_ref = manager
            .actor_ref()
            .hold()
            .expect("route transport manager must expose a strong actor ref");
        let ingress_memberships = group_memberships.clone();
        let ingress_local_member = local_member.clone();
        let ingress = system.create(move || {
            DeliveryIngressComponent::new(DeliveryInterestConfig {
                group_memberships: ingress_memberships,
                local_members: Arc::new([ingress_local_member.clone()].into_iter().collect()),
                hosted_mailboxes: Arc::new(HashSet::new()),
            })
        });
        let broadcast_memberships = group_memberships.clone();
        let broadcast_manager_ref = manager_ref.clone();
        let reliable_manager_ref = manager_ref;
        let group_broadcast = system.create(move || {
            GroupBroadcastComponent::new(broadcast_memberships, broadcast_manager_ref)
        });
        let reliable_delivery =
            system.create(move || ReliableDeliveryComponent::new(reliable_manager_ref));
        let discovery_source = system.create(RuntimeDiscoverySource::new);
        let discovery_source_ref = discovery_source
            .actor_ref()
            .hold()
            .expect("runtime discovery source must expose a strong actor ref");
        let local_endpoint_manager = system.create(LocalEndpointManager::new);

        DeliveryRuntimeGraph {
            driver,
            bridge,
            manager,
            ingress,
            group_broadcast,
            reliable_delivery,
            discovery_source,
            discovery_source_ref,
            local_endpoint_manager,
            runtime_component: None,
        }
    }

    fn connect_graph(graph: &DeliveryRuntimeGraph) -> Result<(), RuntimeHostError> {
        connect_components_with_context::<TransportRoutePort, _, _>(
            &graph.manager,
            &graph.ingress,
            "route transport -> ingress",
        )?;
        connect_components_with_context::<GroupBroadcastInboundRoutePort, _, _>(
            &graph.ingress,
            &graph.group_broadcast,
            "ingress -> group broadcast",
        )?;
        connect_components_with_context::<ReliableDeliveryInboundRoutePort, _, _>(
            &graph.ingress,
            &graph.reliable_delivery,
            "ingress -> reliable delivery",
        )?;
        connect_components_with_context::<RouteDiscoveryPort<TransportRouteKey>, _, _>(
            &graph.discovery_source,
            &graph.group_broadcast,
            "discovery -> group broadcast",
        )?;
        connect_components_with_context::<RouteDiscoveryPort<TransportRouteKey>, _, _>(
            &graph.discovery_source,
            &graph.reliable_delivery,
            "discovery -> reliable delivery",
        )?;
        Ok(())
    }

    fn start_graph(
        system: &KompactSystem,
        graph: &DeliveryRuntimeGraph,
        control_timeout: Duration,
    ) -> Result<(), RuntimeHostError> {
        start_component(system, &graph.driver, "io_driver", control_timeout)?;
        start_component(system, &graph.bridge, "io_bridge", control_timeout)?;
        let udp_connect_handle = IoBridgeHandle::from_component(&graph.bridge);
        block_on(udp_connect_handle.connect_udp(&graph.manager)).map_err(|error| {
            RuntimeHostError::ConnectUdp {
                message: format_component_error(&error),
            }
        })?;
        block_on(udp_connect_handle.connect_udp(&graph.local_endpoint_manager)).map_err(
            |error| RuntimeHostError::ConnectUdp {
                message: format_component_error(&error),
            },
        )?;
        start_component(system, &graph.manager, "route_transport", control_timeout)?;
        start_component(system, &graph.ingress, "delivery_ingress", control_timeout)?;
        start_component(
            system,
            &graph.group_broadcast,
            "group_broadcast",
            control_timeout,
        )?;
        start_component(
            system,
            &graph.reliable_delivery,
            "reliable_delivery",
            control_timeout,
        )?;
        start_component(
            system,
            &graph.discovery_source,
            "discovery_source",
            control_timeout,
        )?;
        start_component(
            system,
            &graph.local_endpoint_manager,
            "local_endpoint_manager",
            control_timeout,
        )?;
        Ok(())
    }

    pub(crate) fn shutdown(&mut self) {
        let Some(mut graph) = self.graph.take() else {
            return;
        };

        if let Some(runtime_component) = graph.runtime_component.take() {
            stop_component(&self.system, &runtime_component, self.control_timeout);
        }
        stop_component(
            &self.system,
            &graph.local_endpoint_manager,
            self.control_timeout,
        );
        stop_component(&self.system, &graph.discovery_source, self.control_timeout);
        stop_component(&self.system, &graph.reliable_delivery, self.control_timeout);
        stop_component(&self.system, &graph.group_broadcast, self.control_timeout);
        stop_component(&self.system, &graph.ingress, self.control_timeout);
        stop_component(&self.system, &graph.manager, self.control_timeout);
        stop_component(&self.system, &graph.bridge, self.control_timeout);
        stop_component(&self.system, &graph.driver, self.control_timeout);
        let _ = self.system.clone().shutdown();
    }

    /// Replace the authoritative shared group-membership snapshot.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn replace_group_memberships(&self, memberships: GroupMemberships) {
        self.group_memberships.replace(memberships);
    }

    /// Publish one route-discovery update into both semantic delivery owners.
    ///
    /// This is temporary until the replication runtime is wired to the real
    /// discovery mechanism. Today it exists so tests and bring-up tooling can
    /// inject direct routes explicitly.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn publish_route_update(&self, update: DiscoveryRouteUpdate<TransportRouteKey>) {
        self.graph()
            .discovery_source_ref
            .tell(RuntimeDiscoverySourceMessage::Publish(update));
    }

    /// Return the concrete local UDP socket address currently bound by this
    /// host for externally reachable delivery traffic.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn external_udp_bind_addr(&self) -> SocketAddr {
        self.external_udp_addr
    }

    /// Read the current authoritative membership snapshot.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn membership_snapshot(&self) -> Arc<GroupMemberships> {
        self.group_memberships.snapshot()
    }

    #[cfg(test)]
    pub(crate) fn advertised_loopback_udp_addr(&self) -> SocketAddr {
        loopback_advertise_addr(self.external_udp_addr)
    }

    #[cfg(test)]
    pub(crate) fn publish_direct_peer_route(&self, peer: MemberIdentity, remote_addr: SocketAddr) {
        use crate::delivery::route_transport::{
            DatagramRouteScope,
            RoutePreferenceRank,
            RouteSharingKind,
            SendRouteCandidate,
            UdpRouteKey,
        };

        let route = SendRouteCandidate {
            coverage_key: TransportRouteKey::Udp(UdpRouteKey {
                remote_addr,
                scope: DatagramRouteScope::Unicast,
                local_bind: Some(self.external_udp_addr),
            }),
            sharing: RouteSharingKind::Exclusive,
            preference_rank: RoutePreferenceRank::new(1),
        };
        let route_peer = peer.clone();
        self.publish_route_update(DiscoveryRouteUpdate::PeerRoutes {
            peer: route_peer,
            classification: crate::delivery::shared::ReachabilityClass::Reachable,
            routes: vec![route],
        });
        self.wait_for_direct_peer_route(&peer);
    }

    #[cfg(test)]
    fn wait_for_direct_peer_route(&self, peer: &MemberIdentity) {
        use crate::delivery::test_support::FULL_STACK_WAIT_TIMEOUT;
        use flotsync_io::test_support::eventually_component_state;

        let graph = self.graph();
        let broadcast_peer = peer.clone();
        eventually_component_state(
            FULL_STACK_WAIT_TIMEOUT,
            &graph.group_broadcast,
            |component| component.knows_direct_route(&broadcast_peer),
            format_args!("timed out waiting for group-broadcast route publication for peer={peer}"),
        );

        let reliable_peer = peer.clone();
        eventually_component_state(
            FULL_STACK_WAIT_TIMEOUT,
            &graph.reliable_delivery,
            |component| component.knows_direct_route(&reliable_peer),
            format_args!(
                "timed out waiting for reliable-delivery route publication for peer={peer}"
            ),
        );
    }
}

impl Drop for DeliveryRuntimeHost {
    fn drop(&mut self) {
        if self.graph.take().is_some() {
            self.system.shutdown_async();
        }
    }
}

fn bind_local_endpoint(
    local_endpoint_manager: &Arc<Component<LocalEndpointManager>>,
    local_endpoint_bind_addr: SocketAddr,
    control_timeout: Duration,
) -> Result<LocalEndpointBinding, RuntimeHostError> {
    let local_endpoint_ref = local_endpoint_manager
        .actor_ref()
        .hold()
        .expect("local endpoint manager must expose a strong actor ref");
    let (promise, future) = promise::<Result<LocalEndpointBinding, RuntimeHostError>>();
    local_endpoint_ref.tell(LocalEndpointManagerMessage::Bind(Ask::new(
        promise,
        local_endpoint_bind_addr,
    )));
    future
        .wait_timeout(control_timeout)
        .map_err(|error| RuntimeHostError::BindLocalEndpoint {
            message: format_component_error(&error),
        })?
}

#[cfg(test)]
fn loopback_advertise_addr(bind_addr: SocketAddr) -> SocketAddr {
    if !bind_addr.ip().is_unspecified() {
        return bind_addr;
    }

    match bind_addr {
        SocketAddr::V4(addr) => SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, addr.port())),
        SocketAddr::V6(addr) => SocketAddr::from((std::net::Ipv6Addr::LOCALHOST, addr.port())),
    }
}

#[derive(Debug)]
enum RuntimeDiscoverySourceMessage {
    Publish(DiscoveryRouteUpdate<TransportRouteKey>),
}

/// Temporary route-discovery source used until the replication runtime is
/// wired to the real discovery component.
#[derive(ComponentDefinition)]
struct RuntimeDiscoverySource {
    ctx: ComponentContext<Self>,
    discovery: ProvidedPort<RouteDiscoveryPort<TransportRouteKey>>,
}

impl RuntimeDiscoverySource {
    fn new() -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            discovery: ProvidedPort::uninitialised(),
        }
    }
}

ignore_lifecycle!(RuntimeDiscoverySource);

impl Provide<RouteDiscoveryPort<TransportRouteKey>> for RuntimeDiscoverySource {
    fn handle(&mut self, _request: Never) -> Handled {
        unreachable!("runtime discovery source is indication-only")
    }
}

impl LocalActor for RuntimeDiscoverySource {
    type Message = RuntimeDiscoverySourceMessage;

    fn receive(&mut self, msg: Self::Message) -> Handled {
        match msg {
            RuntimeDiscoverySourceMessage::Publish(update) => {
                self.discovery.trigger(update);
                Handled::Ok
            }
        }
    }
}

impl_local_actor!(RuntimeDiscoverySource);

#[derive(Debug)]
enum LocalEndpointManagerMessage {
    Bind(Ask<SocketAddr, Result<LocalEndpointBinding, RuntimeHostError>>),
}

/// Single local delivery-endpoint owner for the current runtime slice.
///
/// This is a deliberate stub until `flotsync-665` lands. It owns one
/// configured UDP bind, records the concrete local address assigned by the
/// system, and is the place where later rebinding/network-change logic should
/// grow rather than scattering bind handling across the host.
#[derive(ComponentDefinition)]
struct LocalEndpointManager {
    ctx: ComponentContext<Self>,
    udp: RequiredPort<UdpPort>,
    state: LocalEndpointManagerState,
}

impl LocalEndpointManager {
    fn new() -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            udp: RequiredPort::uninitialised(),
            state: LocalEndpointManagerState::Unbound,
        }
    }
}

ignore_lifecycle!(LocalEndpointManager);

impl Require<UdpPort> for LocalEndpointManager {
    fn handle(&mut self, indication: UdpIndication) -> Handled {
        let current_state = std::mem::replace(&mut self.state, LocalEndpointManagerState::Unbound);
        self.state = match (current_state, indication) {
            (
                LocalEndpointManagerState::Binding {
                    request_id,
                    promise,
                },
                UdpIndication::Bound {
                    request_id: indicated_request_id,
                    socket_id,
                    local_addr,
                },
            ) if request_id == indicated_request_id => {
                let binding = LocalEndpointBinding {
                    socket_id,
                    local_addr,
                };
                if promise.fulfil(Ok(binding)).is_ok() {
                    LocalEndpointManagerState::Bound(binding)
                } else {
                    LocalEndpointManagerState::Unbound
                }
            }
            (
                LocalEndpointManagerState::Binding {
                    request_id,
                    promise,
                },
                UdpIndication::BindFailed {
                    request_id: indicated_request_id,
                    local_addr,
                    reason,
                },
            ) if request_id == indicated_request_id => {
                let _ = promise.fulfil(Err(RuntimeHostError::BindLocalEndpoint {
                    message: format!("bind at {local_addr} failed: {reason:?}"),
                }));
                LocalEndpointManagerState::Unbound
            }
            (state, _) => state,
        };
        Handled::Ok
    }
}

impl LocalActor for LocalEndpointManager {
    type Message = LocalEndpointManagerMessage;

    fn receive(&mut self, msg: Self::Message) -> Handled {
        match msg {
            LocalEndpointManagerMessage::Bind(ask) => {
                let (promise, bind_addr) = ask.take();
                match &self.state {
                    LocalEndpointManagerState::Unbound => {
                        let request_id = UdpOpenRequestId::new();
                        self.state = LocalEndpointManagerState::Binding {
                            request_id,
                            promise,
                        };
                        self.udp.trigger(UdpRequest::Bind {
                            request_id,
                            bind: UdpLocalBind::Exact(bind_addr),
                        });
                    }
                    LocalEndpointManagerState::Binding { .. } => {
                        let _ = promise.fulfil(Err(RuntimeHostError::BindLocalEndpoint {
                            message: "local endpoint bind already in progress".to_owned(),
                        }));
                    }
                    LocalEndpointManagerState::Bound(binding) => {
                        let _ = promise.fulfil(Ok(*binding));
                    }
                }
                Handled::Ok
            }
        }
    }
}

impl_local_actor!(LocalEndpointManager);

enum LocalEndpointManagerState {
    Unbound,
    Binding {
        request_id: UdpOpenRequestId,
        promise: KPromise<Result<LocalEndpointBinding, RuntimeHostError>>,
    },
    Bound(LocalEndpointBinding),
}

fn build_runtime_system() -> Result<KompactSystem, RuntimeHostError> {
    let mut config = KompactConfig::default();
    configure_replication_runtime(&mut config);
    config
        .build()
        .map_err(|error| RuntimeHostError::BuildSystem {
            message: format!("{error}"),
        })
}

fn connect_components_with_context<P, C1, C2>(
    source: &Arc<Component<C1>>,
    target: &Arc<Component<C2>>,
    link: &'static str,
) -> Result<(), RuntimeHostError>
where
    P: Port + 'static,
    C1: ComponentDefinition + Sized + 'static + Provide<P> + ProvideRef<P>,
    C2: ComponentDefinition + Sized + 'static + Require<P> + RequireRef<P>,
{
    biconnect_components::<P, _, _>(source, target).map_err(|error| {
        RuntimeHostError::ConnectComponents {
            link,
            message: format_component_error(&error),
        }
    })?;
    Ok(())
}

fn format_component_error(error: &impl std::fmt::Debug) -> String {
    format!("{error:?}")
}

fn start_component<C>(
    system: &KompactSystem,
    component: &Arc<Component<C>>,
    name: &'static str,
    control_timeout: Duration,
) -> Result<(), RuntimeHostError>
where
    C: ComponentDefinition + ComponentLifecycle + Sized + 'static,
{
    system
        .start_notify(component)
        .wait_timeout(control_timeout)
        .map_err(|error| RuntimeHostError::StartComponent {
            component: name,
            message: format_component_error(&error),
        })
}

fn stop_component<C>(
    system: &KompactSystem,
    component: &Arc<Component<C>>,
    control_timeout: Duration,
) where
    C: ComponentDefinition + ComponentLifecycle + Sized + 'static,
{
    let _ = system
        .kill_notify(component.clone())
        .wait_timeout(control_timeout);
}
