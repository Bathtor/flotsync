use super::{
    ReplicationRuntimeComponent,
    catch_up_manager::CatchUpManagerComponent,
    summary_request_manager::SummaryRequestManagerComponent,
};
#[cfg(test)]
use super::{ReplicationRuntimeMessage, handle::wait_for_test_reply};
use crate::{
    GroupMemberships,
    SharedGroupMemberships,
    api::{BoxError, MemberIdentity, ReplicationEventListener, ReplicationStore},
    delivery::{
        contracts::{GroupBroadcastPort, ReliableDeliveryPort},
        group_broadcast::{GroupBroadcastComponent, GroupBroadcastInboundPort},
        ingress::{DeliveryIngressComponent, DeliveryInterestConfig},
        reliable_delivery::{ReliableDeliveryComponent, ReliableDeliveryInboundPort},
        route_transport::{
            RouteDiscoveryPort,
            RouteTransportActorMessage,
            RouteTransportPort,
            TransportRouteKey,
            manager::{RouteTransportManager, configure_replication_runtime},
        },
        security::DeliverySecurity,
    },
};
#[cfg(test)]
use flotsync_io::test_support::{
    ReservedSocketKind,
    ReservedSocketLease,
    enable_bind_reuse_address,
    reserve_sockets,
    set_test_system_label,
};
use flotsync_io::{
    kompact::shutdown_system_bounded,
    prelude::{DriverConfig, IoBridge, IoBridgeHandle, IoDriverComponent, UdpPort},
};
use flotsync_udpour::UDPourConfig;
use flotsync_utils::{FutureTimeoutExt as _, TimeoutError};
use futures_util::{FutureExt, future::BoxFuture};
use kompact::{
    KompactLogger,
    config::{ConfigError, ConfigLoadingError},
    prelude::*,
    runtime::KompactError,
};
use snafu::prelude::*;
use std::{collections::HashSet, net::SocketAddr, sync::Arc, time::Duration};

mod discovery;
mod local_endpoint;

#[cfg(not(test))]
use discovery::PreconfiguredPeerRoutesPublishMode;
#[cfg(test)]
pub(super) use discovery::PreconfiguredPeerRoutesPublishMode;
use discovery::{
    PreconfiguredPeerRoutesComponent,
    PreconfiguredPeerRoutesConfig,
    PreconfiguredPeerRoutesMessage,
};
use local_endpoint::LocalEndpointManager;

type TransportRoutePort = RouteTransportPort<TransportRouteKey>;
type GroupBroadcastInboundRoutePort = GroupBroadcastInboundPort<TransportRouteKey>;
type ReliableDeliveryInboundRoutePort = ReliableDeliveryInboundPort<TransportRouteKey>;

mod config_keys {
    use kompact::{
        config::{DurationValue, StringValue},
        kompact_config,
    };
    use std::time::Duration;

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
        key = "flotsync.replication.runtime.local-endpoint-bind-addr",
        type = StringValue,
        default = default_local_endpoint_bind_addr(),
        doc = "Configured local UDP bind address owned by the replication runtime endpoint binder.",
        version = "0.1.0"
    }

    kompact_config! {
        SUMMARY_REQUEST_TIMEOUT,
        key = "flotsync.replication.runtime.summary-request-timeout",
        type = DurationValue,
        default = Duration::from_secs(2),
        doc = "Maximum wait for a summary response from a peer.",
        version = "0.1.0"
    }
}

/// Startup and shutdown failures for the internal delivery runtime host.
#[derive(Debug, Snafu)]
pub(crate) enum RuntimeHostError {
    #[snafu(display("Failed to build Kompact system for the replication runtime: {source}"))]
    BuildSystem {
        #[snafu(source(from(KompactError, RuntimeBuildSystemError::from)))]
        source: RuntimeBuildSystemError,
    },
    #[snafu(display("Invalid replication runtime host config at {key}: {message}"))]
    InvalidConfig { key: &'static str, message: String },
    #[snafu(display("Failed to start runtime component '{component}': {source}"))]
    StartComponent {
        component: &'static str,
        source: RuntimeControlError,
    },
    #[snafu(display("Failed to stop runtime component '{component}': {source}"))]
    StopComponent {
        component: &'static str,
        source: RuntimeControlError,
    },
    #[snafu(display("Failed to connect runtime bridge UDP to '{component}': {message}"))]
    ConnectUdp {
        component: &'static str,
        message: String,
    },
    #[snafu(display("Failed to bind the runtime local delivery endpoint: {source}"))]
    BindLocalEndpoint { source: RuntimeControlError },
    #[snafu(display(
        "Failed to bind the runtime local delivery endpoint: {source}; configured_bind_addr={configured_bind_addr}; system_label={system_label}"
    ))]
    BindLocalEndpointInSystem {
        source: RuntimeControlError,
        configured_bind_addr: SocketAddr,
        system_label: String,
    },
    #[snafu(display("Failed to connect runtime components for {link}: {message}"))]
    ConnectComponents { link: &'static str, message: String },
}

/// Send-safe wrapper around Kompact build failures.
///
/// This exists because `KompactError::Other` stores `Box<dyn Error>` without
/// `Send + Sync`, while replication load errors eventually box runtime-host
/// failures into the public send-safe `BoxError` shape.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub(crate) enum RuntimeBuildSystemError {
    #[snafu(display("Kompact system state was poisoned"))]
    Poisoned,
    #[snafu(display("Kompact config loading failed: {source}"))]
    ConfigLoading { source: ConfigLoadingError },
    #[snafu(display("Kompact config lookup failed: {source}"))]
    Config { source: ConfigError },
    #[snafu(display("Kompact reported an opaque build error: {message}"))]
    Other { message: String },
}

impl From<KompactError> for RuntimeBuildSystemError {
    fn from(error: KompactError) -> Self {
        match error {
            KompactError::Poisoned => Self::Poisoned,
            KompactError::ConfigLoadingError(source) => Self::ConfigLoading { source },
            KompactError::ConfigError(source) => Self::Config { source },
            KompactError::Other(source) => Self::Other {
                message: source.to_string(),
            },
        }
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub(crate) enum RuntimeControlError {
    #[snafu(display("timed out: {source}"))]
    Timeout { source: TimeoutError },
    #[snafu(display("Kompact control future failed: {source}"))]
    ControlFuture { source: BoxError },
    #[snafu(display("{message}"))]
    Failed { message: String },
}

impl RuntimeControlError {
    fn failed(message: impl Into<String>) -> Self {
        Self::Failed {
            message: message.into(),
        }
    }
}

impl From<TimeoutError> for RuntimeControlError {
    fn from(source: TimeoutError) -> Self {
        Self::Timeout { source }
    }
}

#[derive(Clone, Copy, Debug)]
struct DeliveryRuntimeHostConfig {
    control_timeout: Duration,
    summary_request_timeout: Duration,
    local_endpoint_bind_addr: SocketAddr,
}

impl DeliveryRuntimeHostConfig {
    fn from_system_config(system: &KompactSystem) -> Result<Self, RuntimeHostError> {
        let control_timeout = system
            .config()
            .read_or_default(&config_keys::CONTROL_TIMEOUT)
            .map_err(|error| RuntimeHostError::InvalidConfig {
                key: config_keys::CONTROL_TIMEOUT.key,
                message: error.to_string(),
            })?;
        let summary_request_timeout = system
            .config()
            .read_or_default(&config_keys::SUMMARY_REQUEST_TIMEOUT)
            .map_err(|error| RuntimeHostError::InvalidConfig {
                key: config_keys::SUMMARY_REQUEST_TIMEOUT.key,
                message: error.to_string(),
            })?;
        let local_endpoint_bind_addr = system
            .config()
            .read_or_default(&config_keys::LOCAL_ENDPOINT_BIND_ADDR)
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
            summary_request_timeout,
            local_endpoint_bind_addr,
        })
    }
}

/// Live internal host for the delivery-layer components used by replication.
///
/// This owns the concrete Kompact/io topology and exposes a small imperative
/// surface that later replication logic can build on without knowing transport
/// internals.
///
/// TODO(flotsync-3ht): Revisit whether this external host should keep owning
/// topology startup and shutdown at all, or whether that lifecycle should move
/// into `ReplicationRuntimeComponent` itself. This does not defer treating the
/// runtime component as a normal topology node in the current runtime.
pub(crate) struct DeliveryRuntimeHost {
    system: Option<KompactSystem>,
    topology: Option<RuntimeTopology>,
    group_memberships: SharedGroupMemberships,
    control_timeout: Duration,
    #[cfg_attr(not(test), allow(dead_code))]
    external_udp_addr: SocketAddr,
    #[cfg(test)]
    local_endpoint_lease: ReservedSocketLease,
}

/// Type-erased lifecycle operations for one concrete Kompact component.
trait RuntimeLifecycleComponent: Send + Sync {
    fn start<'a>(
        &'a self,
        system: &'a KompactSystem,
        control_timeout: Duration,
    ) -> BoxFuture<'a, Result<(), RuntimeHostError>>;

    fn stop(
        &self,
        system: &KompactSystem,
        control_timeout: Duration,
    ) -> Result<(), RuntimeHostError>;
}

/// A component topology that can expose its concrete lifecycle nodes in start order.
///
/// Topologies with fixed component fields can return an iterator over those fields
/// directly. Larger or dynamic topologies can choose their own storage shape as long
/// as the exposed node order is the desired startup order.
trait ComponentTopology {
    /// Expose this topology's lifecycle components in the order they should start.
    fn nodes(&self) -> impl DoubleEndedIterator<Item = &dyn RuntimeLifecycleComponent>;

    /// Start every lifecycle node in the order exposed by `nodes`.
    async fn start_all(
        &self,
        system: &KompactSystem,
        control_timeout: Duration,
    ) -> Result<(), RuntimeHostError> {
        for component in self.nodes() {
            component.start(system, control_timeout).await?;
        }
        Ok(())
    }

    /// Stop every lifecycle node in the reverse order exposed by `nodes`.
    fn stop_all(
        &self,
        system: &KompactSystem,
        control_timeout: Duration,
    ) -> Result<(), RuntimeHostError> {
        for component in self.nodes().rev() {
            component.stop(system, control_timeout)?;
        }
        Ok(())
    }
}

fn connect_components<P, C1, C2>(
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
            message: describe_component_connect_error(&error).to_owned(),
        }
    })?;
    Ok(())
}

fn describe_component_connect_error(error: &TryDualLockError) -> &'static str {
    match error {
        TryDualLockError::LeftWouldBlock => "provider component lock would block",
        TryDualLockError::RightWouldBlock => "requirer component lock would block",
        TryDualLockError::LeftPoisoned => "provider component lock was poisoned",
        TryDualLockError::RightPoisoned => "requirer component lock was poisoned",
    }
}

async fn connect_udp_component<C>(
    udp_connect_handle: &IoBridgeHandle,
    component: &Arc<Component<C>>,
) -> Result<(), RuntimeHostError>
where
    C: ComponentDefinition
        + ComponentLifecycle
        + Require<UdpPort>
        + RequireRef<UdpPort>
        + Sized
        + 'static,
{
    udp_connect_handle
        .connect_udp(component)
        .await
        .map_err(|error| RuntimeHostError::ConnectUdp {
            component: C::type_name(),
            message: error.to_string(),
        })
}

impl<C> RuntimeLifecycleComponent for Arc<Component<C>>
where
    C: ComponentDefinition + ComponentLifecycle + Sized + 'static,
{
    fn start<'a>(
        &'a self,
        system: &'a KompactSystem,
        control_timeout: Duration,
    ) -> BoxFuture<'a, Result<(), RuntimeHostError>> {
        async move {
            system
                .start_notify(self)
                .map(|result| result.boxed().context(ControlFutureSnafu))
                .timeout_fold_err(control_timeout)
                .await
                .context(StartComponentSnafu {
                    component: C::type_name(),
                })
        }
        .boxed()
    }

    fn stop(
        &self,
        system: &KompactSystem,
        control_timeout: Duration,
    ) -> Result<(), RuntimeHostError> {
        block_on(
            system
                .stop_notify(self)
                .map(|result| result.boxed().context(ControlFutureSnafu))
                .timeout_fold_err(control_timeout),
        )
        .context(StopComponentSnafu {
            component: C::type_name(),
        })
    }
}

struct BuiltRuntimeSystem {
    system: KompactSystem,
    #[cfg(test)]
    local_endpoint_lease: ReservedSocketLease,
}

struct IoTopology {
    driver: Arc<Component<IoDriverComponent>>,
    bridge: Arc<Component<IoBridge>>,
}

impl IoTopology {
    fn build(system: &KompactSystem) -> Self {
        let driver = IoDriverComponent::new(DriverConfig::default());
        let driver = system.create(move || driver);
        let bridge = IoBridge::new(&driver);
        let bridge = system.create(move || bridge);
        Self { driver, bridge }
    }

    async fn connect_udp_ports(
        &self,
        transport: &TransportTopology,
        discovery: &DiscoveryTopology,
    ) -> Result<(), RuntimeHostError> {
        let udp_connect_handle = IoBridgeHandle::from_component(&self.bridge);
        connect_udp_component(&udp_connect_handle, transport.route_transport_manager()).await?;
        connect_udp_component(&udp_connect_handle, discovery.local_endpoint_manager()).await?;
        connect_udp_component(&udp_connect_handle, discovery.route_discovery_provider()).await
    }
}

impl ComponentTopology for IoTopology {
    fn nodes(&self) -> impl DoubleEndedIterator<Item = &dyn RuntimeLifecycleComponent> {
        std::iter::once(&self.driver as &dyn RuntimeLifecycleComponent).chain(std::iter::once(
            &self.bridge as &dyn RuntimeLifecycleComponent,
        ))
    }
}

struct TransportTopology {
    manager: Arc<Component<RouteTransportManager>>,
}

impl TransportTopology {
    fn build(system: &KompactSystem, bridge: &Arc<Component<IoBridge>>) -> Self {
        let manager = RouteTransportManager::new(
            system.clone(),
            IoBridgeHandle::from_component(bridge),
            UDPourConfig::default(),
        );
        let manager = system.create(move || manager);
        Self { manager }
    }

    fn manager_ref(&self) -> ActorRefStrong<RouteTransportActorMessage<TransportRouteKey>> {
        self.manager
            .actor_ref()
            .hold()
            .expect("route transport manager must expose a strong actor ref")
    }

    fn route_transport_manager(&self) -> &Arc<Component<RouteTransportManager>> {
        &self.manager
    }
}

impl ComponentTopology for TransportTopology {
    fn nodes(&self) -> impl DoubleEndedIterator<Item = &dyn RuntimeLifecycleComponent> {
        std::iter::once(&self.manager as &dyn RuntimeLifecycleComponent)
    }
}

struct DeliveryTopology {
    ingress: Arc<Component<DeliveryIngressComponent>>,
    group_broadcast: Arc<Component<GroupBroadcastComponent>>,
    reliable_delivery: Arc<Component<ReliableDeliveryComponent>>,
}

impl DeliveryTopology {
    fn build(
        system: &KompactSystem,
        group_memberships: SharedGroupMemberships,
        local_member: MemberIdentity,
        manager_ref: ActorRefStrong<RouteTransportActorMessage<TransportRouteKey>>,
        security: DeliverySecurity,
    ) -> Self {
        let ingress = DeliveryIngressComponent::new(DeliveryInterestConfig {
            group_memberships: group_memberships.clone(),
            local_members: Arc::new([local_member].into_iter().collect()),
            hosted_mailboxes: Arc::new(HashSet::new()),
        });
        let ingress = system.create(move || ingress);
        let group_broadcast =
            GroupBroadcastComponent::new(group_memberships, manager_ref.clone(), security.clone());
        let group_broadcast = system.create(move || group_broadcast);
        let reliable_delivery = ReliableDeliveryComponent::new(manager_ref, security);
        let reliable_delivery = system.create(move || reliable_delivery);
        Self {
            ingress,
            group_broadcast,
            reliable_delivery,
        }
    }

    fn connect_transport(&self, transport: &TransportTopology) -> Result<(), RuntimeHostError> {
        connect_components::<TransportRoutePort, _, _>(
            transport.route_transport_manager(),
            &self.ingress,
            "route transport -> ingress",
        )
    }

    fn connect_internal_routes(&self) -> Result<(), RuntimeHostError> {
        connect_components::<GroupBroadcastInboundRoutePort, _, _>(
            &self.ingress,
            &self.group_broadcast,
            "ingress -> group broadcast",
        )?;
        connect_components::<ReliableDeliveryInboundRoutePort, _, _>(
            &self.ingress,
            &self.reliable_delivery,
            "ingress -> reliable delivery",
        )
    }

    fn connect_discovery(&self, discovery: &DiscoveryTopology) -> Result<(), RuntimeHostError> {
        connect_components::<RouteDiscoveryPort<TransportRouteKey>, _, _>(
            discovery.route_discovery_provider(),
            &self.group_broadcast,
            "preconfigured peer routes -> group broadcast",
        )?;
        connect_components::<RouteDiscoveryPort<TransportRouteKey>, _, _>(
            discovery.route_discovery_provider(),
            &self.reliable_delivery,
            "preconfigured peer routes -> reliable delivery",
        )
    }

    fn group_broadcast_provider(&self) -> &Arc<Component<GroupBroadcastComponent>> {
        &self.group_broadcast
    }

    fn reliable_delivery_provider(&self) -> &Arc<Component<ReliableDeliveryComponent>> {
        &self.reliable_delivery
    }
}

impl ComponentTopology for DeliveryTopology {
    fn nodes(&self) -> impl DoubleEndedIterator<Item = &dyn RuntimeLifecycleComponent> {
        std::iter::once(&self.ingress as &dyn RuntimeLifecycleComponent)
            .chain(std::iter::once(
                &self.group_broadcast as &dyn RuntimeLifecycleComponent,
            ))
            .chain(std::iter::once(
                &self.reliable_delivery as &dyn RuntimeLifecycleComponent,
            ))
    }
}

struct DiscoveryTopology {
    preconfigured_peer_routes: Arc<Component<PreconfiguredPeerRoutesComponent>>,
    #[cfg_attr(not(test), allow(dead_code))]
    preconfigured_peer_routes_ref: ActorRefStrong<PreconfiguredPeerRoutesMessage>,
    local_endpoint_manager: Arc<Component<LocalEndpointManager>>,
}

impl DiscoveryTopology {
    fn build(
        system: &KompactSystem,
        host_config: DeliveryRuntimeHostConfig,
        route_publish_mode: PreconfiguredPeerRoutesPublishMode,
    ) -> Self {
        let preconfigured_peer_routes = PreconfiguredPeerRoutesComponent::new(
            host_config.local_endpoint_bind_addr,
            route_publish_mode,
        );
        let preconfigured_peer_routes = system.create(move || preconfigured_peer_routes);
        let preconfigured_peer_routes_ref = preconfigured_peer_routes
            .actor_ref()
            .hold()
            .expect("preconfigured peer routes must expose a strong actor ref");
        let local_endpoint_manager =
            LocalEndpointManager::new(host_config.local_endpoint_bind_addr);
        let local_endpoint_manager = system.create(move || local_endpoint_manager);
        Self {
            preconfigured_peer_routes,
            preconfigured_peer_routes_ref,
            local_endpoint_manager,
        }
    }

    fn route_discovery_provider(&self) -> &Arc<Component<PreconfiguredPeerRoutesComponent>> {
        &self.preconfigured_peer_routes
    }

    fn local_endpoint_manager(&self) -> &Arc<Component<LocalEndpointManager>> {
        &self.local_endpoint_manager
    }
}

impl ComponentTopology for DiscoveryTopology {
    fn nodes(&self) -> impl DoubleEndedIterator<Item = &dyn RuntimeLifecycleComponent> {
        std::iter::once(&self.preconfigured_peer_routes as &dyn RuntimeLifecycleComponent).chain(
            std::iter::once(&self.local_endpoint_manager as &dyn RuntimeLifecycleComponent),
        )
    }
}

struct RuntimeLogicTopology {
    catch_up_manager: Arc<Component<CatchUpManagerComponent>>,
    summary_request_manager: Arc<Component<SummaryRequestManagerComponent>>,
    runtime_component: Arc<Component<ReplicationRuntimeComponent>>,
}

impl RuntimeLogicTopology {
    fn build(
        system: &KompactSystem,
        group_memberships: SharedGroupMemberships,
        local_member: MemberIdentity,
        store: Arc<dyn ReplicationStore>,
        listener: Arc<dyn ReplicationEventListener>,
        security: DeliverySecurity,
        host_config: DeliveryRuntimeHostConfig,
    ) -> Self {
        let catch_up_manager = CatchUpManagerComponent::new(
            local_member.clone(),
            group_memberships.clone(),
            store.clone(),
        );
        let catch_up_manager = system.create(move || catch_up_manager);
        let catch_up_manager_ref = catch_up_manager
            .actor_ref()
            .hold()
            .expect("catch-up manager must expose a strong actor ref");
        let summary_request_manager = SummaryRequestManagerComponent::new(
            local_member.clone(),
            group_memberships.clone(),
            host_config.summary_request_timeout,
        );
        let summary_request_manager = system.create(move || summary_request_manager);
        let summary_request_manager_ref = summary_request_manager
            .actor_ref()
            .hold()
            .expect("summary request manager must expose a strong actor ref");
        let runtime_component = ReplicationRuntimeComponent::new(
            local_member,
            store,
            listener,
            security,
            group_memberships,
            summary_request_manager_ref,
            catch_up_manager_ref,
        );
        let runtime_component = system.create(move || runtime_component);
        Self {
            catch_up_manager,
            summary_request_manager,
            runtime_component,
        }
    }

    fn connect_delivery(&self, delivery: &DeliveryTopology) -> Result<(), RuntimeHostError> {
        connect_components::<GroupBroadcastPort, _, _>(
            delivery.group_broadcast_provider(),
            &self.runtime_component,
            "group broadcast -> replication runtime",
        )?;
        connect_components::<GroupBroadcastPort, _, _>(
            delivery.group_broadcast_provider(),
            &self.catch_up_manager,
            "group broadcast -> catch-up manager",
        )?;
        connect_components::<ReliableDeliveryPort, _, _>(
            delivery.reliable_delivery_provider(),
            &self.runtime_component,
            "reliable delivery -> replication runtime",
        )?;
        connect_components::<ReliableDeliveryPort, _, _>(
            delivery.reliable_delivery_provider(),
            &self.summary_request_manager,
            "reliable delivery -> summary request manager",
        )
    }
}

impl ComponentTopology for RuntimeLogicTopology {
    fn nodes(&self) -> impl DoubleEndedIterator<Item = &dyn RuntimeLifecycleComponent> {
        std::iter::once(&self.catch_up_manager as &dyn RuntimeLifecycleComponent)
            .chain(std::iter::once(
                &self.summary_request_manager as &dyn RuntimeLifecycleComponent,
            ))
            .chain(std::iter::once(
                &self.runtime_component as &dyn RuntimeLifecycleComponent,
            ))
    }
}

struct RuntimeTopology {
    io: IoTopology,
    transport: TransportTopology,
    delivery: DeliveryTopology,
    discovery: DiscoveryTopology,
    runtime: RuntimeLogicTopology,
}

/// Inputs needed to assemble a full runtime topology.
struct RuntimeTopologyBuildInput {
    group_memberships: SharedGroupMemberships,
    local_member: MemberIdentity,
    store: Arc<dyn ReplicationStore>,
    listener: Arc<dyn ReplicationEventListener>,
    security: DeliverySecurity,
    host_config: DeliveryRuntimeHostConfig,
    route_publish_mode: PreconfiguredPeerRoutesPublishMode,
}

impl RuntimeTopology {
    fn build(system: &KompactSystem, input: RuntimeTopologyBuildInput) -> Self {
        let io = IoTopology::build(system);
        let transport = TransportTopology::build(system, &io.bridge);
        let manager_ref = transport.manager_ref();
        let delivery = DeliveryTopology::build(
            system,
            input.group_memberships.clone(),
            input.local_member.clone(),
            manager_ref,
            input.security.clone(),
        );
        let discovery =
            DiscoveryTopology::build(system, input.host_config, input.route_publish_mode);
        let runtime = RuntimeLogicTopology::build(
            system,
            input.group_memberships,
            input.local_member,
            input.store,
            input.listener,
            input.security,
            input.host_config,
        );

        Self {
            io,
            transport,
            delivery,
            discovery,
            runtime,
        }
    }

    fn connect_all(&self) -> Result<(), RuntimeHostError> {
        self.delivery.connect_transport(&self.transport)?;
        self.delivery.connect_internal_routes()?;
        self.delivery.connect_discovery(&self.discovery)?;
        self.runtime.connect_delivery(&self.delivery)
    }

    async fn start_all(
        &self,
        system: &KompactSystem,
        control_timeout: Duration,
    ) -> Result<(), RuntimeHostError> {
        self.io.start_all(system, control_timeout).await?;
        self.io
            .connect_udp_ports(&self.transport, &self.discovery)
            .await?;
        self.transport.start_all(system, control_timeout).await?;
        self.delivery.start_all(system, control_timeout).await?;
        self.discovery.start_all(system, control_timeout).await?;
        self.runtime.start_all(system, control_timeout).await?;
        Ok(())
    }

    fn stop_all(
        &self,
        system: &KompactSystem,
        control_timeout: Duration,
    ) -> Result<(), RuntimeHostError> {
        self.runtime.stop_all(system, control_timeout)?;
        self.discovery.stop_all(system, control_timeout)?;
        self.delivery.stop_all(system, control_timeout)?;
        self.transport.stop_all(system, control_timeout)?;
        self.io.stop_all(system, control_timeout)?;
        Ok(())
    }
}

impl ComponentTopology for RuntimeTopology {
    fn nodes(&self) -> impl DoubleEndedIterator<Item = &dyn RuntimeLifecycleComponent> {
        self.io
            .nodes()
            .chain(self.transport.nodes())
            .chain(self.delivery.nodes())
            .chain(self.discovery.nodes())
            .chain(self.runtime.nodes())
    }
}

impl DeliveryRuntimeHost {
    fn new(
        system: KompactSystem,
        topology: RuntimeTopology,
        group_memberships: SharedGroupMemberships,
        control_timeout: Duration,
        external_udp_addr: SocketAddr,
        #[cfg(test)] local_endpoint_lease: ReservedSocketLease,
    ) -> Self {
        Self {
            system: Some(system),
            topology: Some(topology),
            group_memberships,
            control_timeout,
            external_udp_addr,
            #[cfg(test)]
            local_endpoint_lease,
        }
    }

    fn topology(&self) -> &RuntimeTopology {
        self.topology
            .as_ref()
            .expect("delivery runtime host topology must still be live")
    }

    pub(crate) fn logger(&self) -> &KompactLogger {
        self.system
            .as_ref()
            .expect("delivery runtime host system must still be live")
            .logger()
    }

    pub(crate) fn runtime_component(&self) -> &Arc<Component<ReplicationRuntimeComponent>> {
        &self.topology().runtime.runtime_component
    }

    /// Start one new delivery runtime host with an additional in-memory TOML
    /// config fragment merged into the Kompact runtime config.
    pub(crate) async fn start_with_runtime_config_toml(
        local_member: &MemberIdentity,
        store: Arc<dyn ReplicationStore>,
        listener: Arc<dyn ReplicationEventListener>,
        security: DeliverySecurity,
        runtime_config_toml: Option<&str>,
    ) -> Result<Self, RuntimeHostError> {
        Self::start_with_options(
            local_member,
            store,
            listener,
            security,
            runtime_config_toml,
            PreconfiguredPeerRoutesPublishMode::OnLocalEndpointBound,
        )
        .await
    }

    async fn start_with_options(
        local_member: &MemberIdentity,
        store: Arc<dyn ReplicationStore>,
        listener: Arc<dyn ReplicationEventListener>,
        security: DeliverySecurity,
        runtime_config_toml: Option<&str>,
        route_publish_mode: PreconfiguredPeerRoutesPublishMode,
    ) -> Result<Self, RuntimeHostError> {
        let built_system = build_runtime_system(runtime_config_toml).await?;
        let system = built_system.system.clone();
        let host_config = DeliveryRuntimeHostConfig::from_system_config(&system)?;
        let routes_config = PreconfiguredPeerRoutesConfig::from_config(system.config())?;
        routes_config.validate_local_endpoint_bind_addr(host_config.local_endpoint_bind_addr)?;
        let group_memberships = SharedGroupMemberships::new(GroupMemberships::new());
        let topology = RuntimeTopology::build(
            &system,
            RuntimeTopologyBuildInput {
                group_memberships: group_memberships.clone(),
                local_member: local_member.clone(),
                store,
                listener,
                security,
                host_config,
                route_publish_mode,
            },
        );
        topology.connect_all()?;
        topology
            .start_all(&system, host_config.control_timeout)
            .await?;
        let local_endpoint = local_endpoint::ensure_local_endpoint_bound(
            &topology.discovery.local_endpoint_manager,
            host_config.control_timeout,
        )
        .await
        .map_err(|error| {
            annotate_local_endpoint_bind_error(&system, host_config.local_endpoint_bind_addr, error)
        })?;
        #[cfg(test)]
        let mut local_endpoint_lease = built_system.local_endpoint_lease;
        #[cfg(test)]
        release_reserved_runtime_local_endpoint_binding(
            &mut local_endpoint_lease,
            local_endpoint.local_addr,
        )?;

        Ok(Self::new(
            system,
            topology,
            group_memberships,
            host_config.control_timeout,
            local_endpoint.local_addr,
            #[cfg(test)]
            local_endpoint_lease,
        ))
    }

    #[cfg(test)]
    pub(super) async fn start_with_route_publish_mode_for_test(
        local_member: &MemberIdentity,
        store: Arc<dyn ReplicationStore>,
        listener: Arc<dyn ReplicationEventListener>,
        security: DeliverySecurity,
        runtime_config_toml: Option<&str>,
        route_publish_mode: PreconfiguredPeerRoutesPublishMode,
    ) -> Result<Self, RuntimeHostError> {
        Self::start_with_options(
            local_member,
            store,
            listener,
            security,
            runtime_config_toml,
            route_publish_mode,
        )
        .await
    }

    pub(crate) fn shutdown(&mut self) {
        let Some(topology) = self.topology.take() else {
            return;
        };
        let Some(system) = self.system.take() else {
            return;
        };
        let stop_result = topology.stop_all(&system, self.control_timeout);
        drop(topology);
        if let Err(error) = stop_result {
            shutdown_system_bounded(system, self.control_timeout, true);
            #[cfg(test)]
            rebind_reserved_runtime_local_endpoint_binding(&mut self.local_endpoint_lease);
            panic!("failed to stop delivery runtime host cleanly: {error}");
        }
        shutdown_system_bounded(system, self.control_timeout, false);
        #[cfg(test)]
        rebind_reserved_runtime_local_endpoint_binding(&mut self.local_endpoint_lease);
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
    #[cfg(test)]
    pub(crate) fn publish_route_update(
        &self,
        update: crate::delivery::route_transport::DiscoveryRouteUpdate<TransportRouteKey>,
    ) {
        self.topology()
            .discovery
            .preconfigured_peer_routes_ref
            .tell(PreconfiguredPeerRoutesMessage::Publish(update));
    }

    #[cfg(test)]
    pub(crate) fn publish_preconfigured_peer_routes_for_test(&self) {
        self.topology()
            .discovery
            .preconfigured_peer_routes_ref
            .tell(PreconfiguredPeerRoutesMessage::PublishPreconfiguredRoutes);
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
}

impl Drop for DeliveryRuntimeHost {
    fn drop(&mut self) {
        let Some(topology) = self.topology.take() else {
            return;
        };
        let Some(system) = self.system.take() else {
            return;
        };
        let stop_result = topology.stop_all(&system, self.control_timeout);
        drop(topology);
        if let Err(error) = stop_result {
            log::error!("delivery runtime host drop stop_all failed before forced kill: {error}");
        }
        shutdown_system_bounded(system, self.control_timeout, true);
        #[cfg(test)]
        rebind_reserved_runtime_local_endpoint_binding(&mut self.local_endpoint_lease);
    }
}

#[cfg(test)]
async fn build_runtime_system(
    runtime_config_toml: Option<&str>,
) -> Result<BuiltRuntimeSystem, RuntimeHostError> {
    let local_endpoint_lease = reserve_sockets(&[ReservedSocketKind::UdpSocket]);
    let local_endpoint_bind_addr = local_endpoint_lease.addr(0).to_string();
    let mut config = kompact::test_support::test_kompact_config();
    set_test_system_label(&mut config, "replication-runtime-host-test-system");
    enable_bind_reuse_address(&mut config);
    configure_replication_runtime(&mut config);
    if let Some(runtime_config_toml) = runtime_config_toml {
        config.load_config_str(runtime_config_toml);
    }
    config.set_config_value(
        &config_keys::LOCAL_ENDPOINT_BIND_ADDR,
        local_endpoint_bind_addr,
    );
    let system = config.build().await.context(BuildSystemSnafu)?;
    Ok(BuiltRuntimeSystem {
        system,
        local_endpoint_lease,
    })
}

#[cfg(not(test))]
async fn build_runtime_system(
    runtime_config_toml: Option<&str>,
) -> Result<BuiltRuntimeSystem, RuntimeHostError> {
    let mut config = KompactConfig::default();
    configure_replication_runtime(&mut config);
    if let Some(runtime_config_toml) = runtime_config_toml {
        config.load_config_str(runtime_config_toml);
    }
    let system = config.build().await.context(BuildSystemSnafu)?;
    Ok(BuiltRuntimeSystem { system })
}

#[cfg(test)]
fn release_reserved_runtime_local_endpoint_binding(
    local_endpoint_lease: &mut ReservedSocketLease,
    local_addr: SocketAddr,
) -> Result<(), RuntimeHostError> {
    let reserved_addr = local_endpoint_lease.addr(0);
    if reserved_addr != local_addr {
        return Err(RuntimeHostError::BindLocalEndpoint {
            source: RuntimeControlError::failed(format!(
                "runtime host bound local endpoint at {local_addr}, but the reserved test socket expected {reserved_addr}"
            )),
        });
    }
    local_endpoint_lease.activate_live_binding(0);
    Ok(())
}

#[cfg(test)]
fn rebind_reserved_runtime_local_endpoint_binding(local_endpoint_lease: &mut ReservedSocketLease) {
    let rebind_result = local_endpoint_lease.rebind_binding(0);
    if let Err(error) = rebind_result
        && !std::thread::panicking()
    {
        panic!("rebind reserved runtime-host local endpoint socket: {error}");
    }
}

fn annotate_local_endpoint_bind_error(
    system: &KompactSystem,
    configured_bind_addr: SocketAddr,
    error: RuntimeHostError,
) -> RuntimeHostError {
    let RuntimeHostError::BindLocalEndpoint { source } = error else {
        return error;
    };
    let system_label = system
        .config()
        .read_or_default(&kompact::config_keys::system::LABEL)
        .unwrap_or_else(|_| String::from("<unlabelled-runtime-host-system>"));
    RuntimeHostError::BindLocalEndpointInSystem {
        source,
        configured_bind_addr,
        system_label,
    }
}

#[cfg(test)]
pub(crate) trait DeliveryRuntimeHostTestExt {
    fn advertised_loopback_udp_addr(&self) -> SocketAddr;
    fn publish_direct_peer_route(&self, peer: MemberIdentity, remote_addr: SocketAddr);
    fn publish_preconfigured_peer_routes(&self);
    fn wait_for_direct_peer_route(&self, peer: &MemberIdentity);
    fn wait_for_runtime_startup(&self);
}

#[cfg(test)]
impl DeliveryRuntimeHostTestExt for DeliveryRuntimeHost {
    fn advertised_loopback_udp_addr(&self) -> SocketAddr {
        loopback_advertise_addr(self.external_udp_addr)
    }

    fn publish_direct_peer_route(&self, peer: MemberIdentity, remote_addr: SocketAddr) {
        use crate::delivery::{
            route_transport::{
                DatagramRouteScope,
                RoutePreferenceRank,
                RouteSharingKind,
                SendRouteCandidate,
                UdpRouteKey,
            },
            shared::ReachabilityClass,
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
        self.publish_route_update(
            crate::delivery::route_transport::DiscoveryRouteUpdate::PeerRoutes {
                peer: peer.clone(),
                classification: ReachabilityClass::Reachable,
                routes: vec![route],
            },
        );
        wait_for_direct_peer_route(self.topology(), &peer);
    }

    fn publish_preconfigured_peer_routes(&self) {
        self.publish_preconfigured_peer_routes_for_test();
    }

    fn wait_for_direct_peer_route(&self, peer: &MemberIdentity) {
        wait_for_direct_peer_route(self.topology(), peer);
    }

    fn wait_for_runtime_startup(&self) {
        let future = self
            .runtime_component()
            .actor_ref()
            .ask_with(ReplicationRuntimeMessage::test_ping);
        match wait_for_test_reply(future) {
            Ok(()) => {}
            Err(error) => panic!(
                "replication runtime component became unavailable during test startup barrier: {error:?}"
            ),
        }
    }
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

#[cfg(test)]
fn wait_for_direct_peer_route(topology: &RuntimeTopology, peer: &MemberIdentity) {
    use crate::delivery::test_support::FULL_STACK_WAIT_TIMEOUT;
    use flotsync_io::test_support::eventually_component_state;

    let broadcast_peer = peer.clone();
    eventually_component_state(
        FULL_STACK_WAIT_TIMEOUT,
        &topology.delivery.group_broadcast,
        |component| component.knows_direct_route(&broadcast_peer),
        format_args!("timed out waiting for group-broadcast route publication for peer={peer}"),
    );

    let reliable_peer = peer.clone();
    eventually_component_state(
        FULL_STACK_WAIT_TIMEOUT,
        &topology.delivery.reliable_delivery,
        |component| component.knows_direct_route(&reliable_peer),
        format_args!("timed out waiting for reliable-delivery route publication for peer={peer}"),
    );
}
