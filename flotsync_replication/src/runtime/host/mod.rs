//! Kompact component topology for a replication runtime host.

use super::{
    ReplicationRuntimeComponent,
    catch_up_manager::CatchUpManagerComponent,
    summary_request_manager::SummaryRequestManagerComponent,
};
#[cfg(test)]
use super::{ReplicationRuntimeMessage, handle::wait_for_test_reply};
use crate::{
    api::{BoxError, ReplicationEventListener, ReplicationStore},
    delivery::{
        contracts::{GroupBroadcastPort, ReliableDeliveryPort},
        group_broadcast::{GroupBroadcastComponent, GroupBroadcastInboundPort},
        ingress::{DeliveryIngressComponent, DeliveryInterestConfig},
        reliable_delivery::{ReliableDeliveryComponent, ReliableDeliveryInboundPort},
        security::DeliverySecurity,
    },
};
use flotsync_core::{
    MemberIdentity,
    membership::{GroupMemberships, SharedGroupMemberships},
};
use flotsync_discovery::{
    config_keys as discovery_config_keys,
    endpoint_selection::EndpointSelectionPort,
    services::{
        PeerAnnouncementComponent,
        PeerAnnouncementObservationComponent,
        PeerAnnouncementObservationPort,
        PeerAnnouncementOptions,
        PeerAnnouncementSocketMaintenance,
    },
};
#[cfg(any(test, feature = "test-support"))]
use flotsync_io::test_support::{
    ReservedSocketKind,
    ReservedSocketLease,
    enable_bind_reuse_address,
    reserve_sockets,
    set_test_system_label,
};
use flotsync_io::{
    kompact::shutdown_system_bounded,
    prelude::{DriverConfig, EgressPool, IoBridge, IoBridgeHandle, IoDriverComponent, UdpPort},
};
use flotsync_routes::{
    RouteDiscoveryPort,
    RouteEndpointLifecyclePort,
    RouteTransportActorMessage,
    RouteTransportPort,
    TransportRouteKey,
    UDPourConfig,
    key_material_discovery::{KeyMaterialDiscoveryComponent, KeyMaterialDiscoveryPort},
    manager::{RouteTransportManager, configure_replication_runtime},
    route_establishment::{
        ManualRouteWatchError,
        RouteEstablishmentComponent,
        RouteEstablishmentConfig,
        RouteEstablishmentMessage,
    },
};
#[cfg(any(test, feature = "test-support"))]
use flotsync_utils::kompact_testing::{
    PortTestMsg,
    PortTesterComponent,
    PortTestingExt as _,
    PortTestingRefExt as _,
};
use flotsync_utils::{FutureTimeoutExt as _, TimeoutError};
use futures_util::{FutureExt, future::BoxFuture};
use kompact::{
    KompactLogger,
    config::{ConfigError, ConfigLoadingError},
    prelude::*,
    runtime::KompactError,
};
use snafu::prelude::*;
use std::{
    collections::HashSet,
    error::Error as StdError,
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

mod discovery;
mod local_endpoint;

use discovery::PreconfiguredPeerRoutesConfig;
#[cfg(test)]
pub(super) use discovery::PreconfiguredPeerRoutesPublishMode;
use local_endpoint::LocalEndpointManager;

type TransportRoutePort = RouteTransportPort<TransportRouteKey>;
type GroupBroadcastInboundRoutePort = GroupBroadcastInboundPort<TransportRouteKey>;
type ReliableDeliveryInboundRoutePort = ReliableDeliveryInboundPort<TransportRouteKey>;
#[cfg(any(test, feature = "test-support"))]
type ManualRouteDiscoveryPort = RouteDiscoveryPort<TransportRouteKey>;

#[cfg(any(test, feature = "test-support"))]
const TEST_DIRECT_PEER_ROUTE_TIMEOUT: Duration = Duration::from_secs(5);

mod config_keys {
    use kompact::{
        config::{DurationValue, StringValue},
        kompact_config,
    };
    use std::time::Duration;

    /// Default poll cadence for refreshing selected discovery endpoints from wildcard binds.
    const DEFAULT_LOCAL_ENDPOINT_SELECTION_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

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

    kompact_config! {
        LOCAL_ENDPOINT_SELECTION_REFRESH_INTERVAL,
        key = "flotsync.replication.runtime.local-endpoint-selection-refresh-interval",
        type = DurationValue,
        default = DEFAULT_LOCAL_ENDPOINT_SELECTION_REFRESH_INTERVAL,
        doc = "Poll cadence for refreshing selected discovery endpoints while the local runtime endpoint is bound to a wildcard address.",
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
    #[snafu(display("Failed to configure route-establishment manual watches: {source}"))]
    ConfigureRouteEstablishmentWatches { source: RuntimeControlError },
    #[snafu(display("Invalid route-establishment manual watches: {source}"))]
    InvalidRouteEstablishmentManualWatches { source: ManualRouteWatchError },
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
    peer_announcement_bind_addr: SocketAddr,
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
        let peer_announcement_bind_addr = system
            .config()
            .read_or_default(&discovery_config_keys::PEER_ANNOUNCEMENT_BIND_ADDR)
            .map_err(|error| RuntimeHostError::InvalidConfig {
                key: discovery_config_keys::PEER_ANNOUNCEMENT_BIND_ADDR.key,
                message: error.to_string(),
            })?;
        let peer_announcement_bind_addr =
            peer_announcement_bind_addr
                .parse()
                .map_err(
                    |error: std::net::AddrParseError| RuntimeHostError::InvalidConfig {
                        key: discovery_config_keys::PEER_ANNOUNCEMENT_BIND_ADDR.key,
                        message: error.to_string(),
                    },
                )?;
        Ok(Self {
            control_timeout,
            summary_request_timeout,
            local_endpoint_bind_addr,
            peer_announcement_bind_addr,
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
    #[cfg_attr(not(any(test, feature = "test-support")), allow(dead_code))]
    external_udp_addr: SocketAddr,
    #[cfg(any(test, feature = "test-support"))]
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
    #[cfg(any(test, feature = "test-support"))]
    local_endpoint_lease: ReservedSocketLease,
}

/// IO driver and bridge topology.
///
/// ```text
/// IoDriverComponent
///        ^
///        |
///    IoBridge --UdpPort--+--> RouteTransportManager
///                         +--> LocalEndpointManager
///                         +--> KeyMaterialDiscoveryComponent
///                         +--> PeerAnnouncementComponent
///                         +--> PeerAnnouncementObservationComponent
/// ```
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
        connect_udp_component(&udp_connect_handle, &discovery.key_material_discovery).await?;
        connect_udp_component(&udp_connect_handle, &discovery.peer_announcement).await?;
        connect_udp_component(
            &udp_connect_handle,
            &discovery.peer_announcement_observation,
        )
        .await
    }
}

impl ComponentTopology for IoTopology {
    fn nodes(&self) -> impl DoubleEndedIterator<Item = &dyn RuntimeLifecycleComponent> {
        std::iter::once(&self.driver as &dyn RuntimeLifecycleComponent).chain(std::iter::once(
            &self.bridge as &dyn RuntimeLifecycleComponent,
        ))
    }
}

/// Transport backend topology.
///
/// ```text
/// IoBridge ----------------UdpPort----------------+
///                                                 v
/// LocalEndpointManager --RouteEndpointLifecyclePort--> RouteTransportManager --RouteTransportPort--+--> DeliveryIngress
///                                                                  |                              |
///                                                                  +--RouteEndpointLifecyclePort--+
///                                                                                                 v
///                                                                                  RouteEstablishmentComponent
/// ```
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

/// Semantic delivery topology.
///
/// ```text
/// RouteTransportManager --RouteTransportPort--> DeliveryIngress
///                                                |--GroupBroadcastInboundPort----> GroupBroadcastComponent <---+
///                                                |                                                             |
///                                                +--ReliableDeliveryInboundPort--> ReliableDeliveryComponent <--+
///                                                                                                              |
/// RouteDiscoveryPort ------------------------------------------------------------------------------------------+
/// ```
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
            "route establishment -> group broadcast",
        )?;
        connect_components::<RouteDiscoveryPort<TransportRouteKey>, _, _>(
            discovery.route_discovery_provider(),
            &self.reliable_delivery,
            "route establishment -> reliable delivery",
        )?;
        #[cfg(any(test, feature = "test-support"))]
        {
            connect_components::<RouteDiscoveryPort<TransportRouteKey>, _, _>(
                discovery.manual_route_discovery_provider(),
                &self.group_broadcast,
                "manual route discovery -> group broadcast",
            )?;
            connect_components::<RouteDiscoveryPort<TransportRouteKey>, _, _>(
                discovery.manual_route_discovery_provider(),
                &self.reliable_delivery,
                "manual route discovery -> reliable delivery",
            )?;
        }
        Ok(())
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

/// Discovery and route-establishment topology.
///
/// ```text
/// LocalEndpointManager
///   |--EndpointSelectionPort------+--> PeerAnnouncementComponent
///   |                              |
///   |                              +--------------------------+--------------------+
///   |                                                         v                    |
///   +--RouteEndpointLifecyclePort--> RouteTransportManager    |
///   |                              |--RouteEndpointLifecyclePort-------------------+
///   |                                                                             |
///   +--RouteEndpointLifecyclePort--------------------------------------------+    |
/// PeerAnnouncementObservationComponent --------------------------+
///                                                                 v
///                                               RouteEstablishmentComponent
///                                                    |--RouteDiscoveryPort--> semantic delivery
///                                                    |
///                              KeyMaterialDiscoveryComponent <--KeyMaterialDiscoveryPort--+
/// ```
struct DiscoveryTopology {
    peer_announcement: Arc<Component<PeerAnnouncementComponent>>,
    peer_announcement_observation: Arc<Component<PeerAnnouncementObservationComponent>>,
    route_establishment: Arc<Component<RouteEstablishmentComponent>>,
    key_material_discovery: Arc<Component<KeyMaterialDiscoveryComponent>>,
    #[cfg(any(test, feature = "test-support"))]
    manual_route_discovery: Arc<Component<PortTesterComponent<ManualRouteDiscoveryPort>>>,
    #[cfg(any(test, feature = "test-support"))]
    manual_route_discovery_ref: ActorRef<PortTestMsg<ManualRouteDiscoveryPort>>,
    local_endpoint_manager: Arc<Component<LocalEndpointManager>>,
    static_route_hints: PreconfiguredPeerRoutesConfig,
}

/// Shared transport handles needed by discovery-side runtime components.
struct DiscoveryTransportHandles {
    /// Actor interface used by route establishment for UDPour-capable discovery frames.
    route_transport: ActorRefStrong<RouteTransportActorMessage<TransportRouteKey>>,
    /// Egress pool used by direct UDP discovery components for payload encoding.
    egress_pool: EgressPool,
}

impl DiscoveryTopology {
    fn build(
        system: &KompactSystem,
        host_config: DeliveryRuntimeHostConfig,
        group_memberships: SharedGroupMemberships,
        local_member: MemberIdentity,
        security: DeliverySecurity,
        transport_handles: DiscoveryTransportHandles,
        static_route_hints: PreconfiguredPeerRoutesConfig,
    ) -> Self {
        let DiscoveryTransportHandles {
            route_transport,
            egress_pool,
        } = transport_handles;
        let route_config = route_establishment_config(host_config.peer_announcement_bind_addr);
        let peer_options = PeerAnnouncementOptions::DEFAULT
            .with_socket_bind_addr(route_config.peer_announcement_bind_addr)
            .with_instance_id(route_config.instance_id)
            .with_socket_maintenance(PeerAnnouncementSocketMaintenance::Maintain);
        let peer_announcement =
            system.create(move || PeerAnnouncementComponent::with_options(peer_options));
        let peer_observation_bind_addr = route_config.peer_announcement_bind_addr;
        let peer_announcement_observation = system.create(move || {
            PeerAnnouncementObservationComponent::with_socket_maintenance(
                peer_observation_bind_addr,
                PeerAnnouncementSocketMaintenance::Observe,
            )
        });
        let key_material_member = local_member.clone();
        let key_material_security = security.clone();
        let key_material_discovery = system.create(move || {
            KeyMaterialDiscoveryComponent::new(
                key_material_member,
                Arc::new(key_material_security),
                egress_pool,
            )
        });
        let route_establishment = system.create(move || {
            RouteEstablishmentComponent::new(
                route_config,
                route_transport,
                local_member,
                Arc::new(security),
                group_memberships,
            )
        });
        #[cfg(any(test, feature = "test-support"))]
        let manual_route_discovery =
            system.create(ManualRouteDiscoveryPort::tester_component_sidecar);
        #[cfg(any(test, feature = "test-support"))]
        let manual_route_discovery_ref = manual_route_discovery.actor_ref();
        let local_endpoint_manager =
            LocalEndpointManager::new(host_config.local_endpoint_bind_addr);
        let local_endpoint_manager = system.create(move || local_endpoint_manager);
        Self {
            peer_announcement,
            peer_announcement_observation,
            route_establishment,
            key_material_discovery,
            #[cfg(any(test, feature = "test-support"))]
            manual_route_discovery,
            #[cfg(any(test, feature = "test-support"))]
            manual_route_discovery_ref,
            local_endpoint_manager,
            static_route_hints,
        }
    }

    fn route_discovery_provider(&self) -> &Arc<Component<RouteEstablishmentComponent>> {
        &self.route_establishment
    }

    #[cfg(any(test, feature = "test-support"))]
    fn manual_route_discovery_provider(
        &self,
    ) -> &Arc<Component<PortTesterComponent<ManualRouteDiscoveryPort>>> {
        &self.manual_route_discovery
    }

    fn local_endpoint_manager(&self) -> &Arc<Component<LocalEndpointManager>> {
        &self.local_endpoint_manager
    }

    /// Connect route-transport ingress carrying discovery endpoint frames into route establishment.
    fn connect_transport(&self, transport: &TransportTopology) -> Result<(), RuntimeHostError> {
        connect_components::<TransportRoutePort, _, _>(
            transport.route_transport_manager(),
            &self.route_establishment,
            "route transport -> route establishment",
        )?;
        connect_components::<RouteEndpointLifecyclePort, _, _>(
            &self.local_endpoint_manager,
            transport.route_transport_manager(),
            "local endpoint lifecycle -> route transport",
        )?;
        connect_components::<RouteEndpointLifecyclePort, _, _>(
            transport.route_transport_manager(),
            &self.route_establishment,
            "route transport endpoint lifecycle -> route establishment",
        )?;
        Ok(())
    }

    fn connect_internal_routes(&self) -> Result<(), RuntimeHostError> {
        connect_components::<PeerAnnouncementObservationPort, _, _>(
            &self.peer_announcement_observation,
            &self.route_establishment,
            "peer announcement observation -> route establishment",
        )?;
        connect_components::<EndpointSelectionPort, _, _>(
            &self.local_endpoint_manager,
            &self.peer_announcement,
            "local endpoint selection -> peer announcement",
        )?;
        connect_components::<EndpointSelectionPort, _, _>(
            &self.local_endpoint_manager,
            &self.route_establishment,
            "local endpoint selection -> route establishment",
        )?;
        connect_components::<RouteEndpointLifecyclePort, _, _>(
            &self.local_endpoint_manager,
            &self.key_material_discovery,
            "local endpoint lifecycle -> key material discovery",
        )?;
        connect_components::<KeyMaterialDiscoveryPort, _, _>(
            &self.key_material_discovery,
            &self.route_establishment,
            "route establishment -> key material discovery",
        )
    }

    async fn configure_route_establishment_watches(
        &self,
        control_timeout: Duration,
    ) -> Result<(), RuntimeHostError> {
        self.replace_manual_route_watches(control_timeout).await
    }

    async fn replace_manual_route_watches(
        &self,
        control_timeout: Duration,
    ) -> Result<(), RuntimeHostError> {
        let watches = self.static_route_hints.watched_routes();
        let route_establishment_ref = self
            .route_establishment
            .actor_ref()
            .hold()
            .expect("route establishment must expose a strong actor ref");
        let future = route_establishment_ref.ask_with(|promise| {
            RouteEstablishmentMessage::ReplaceManualRouteWatches(Ask::new(promise, watches))
        });
        let result = future
            .map(flatten_manual_route_watch_ask_result)
            .timeout_fold_err(control_timeout)
            .await
            .context(ConfigureRouteEstablishmentWatchesSnafu)?;
        result.context(InvalidRouteEstablishmentManualWatchesSnafu)
    }

    #[cfg(any(test, feature = "test-support"))]
    fn publish_route_update(
        &self,
        update: flotsync_routes::DiscoveryRouteUpdate<TransportRouteKey>,
    ) {
        self.manual_route_discovery_ref.inject_indication(update);
    }

    #[cfg(test)]
    fn publish_preconfigured_peer_routes(&self, local_endpoint: SocketAddr) {
        for update in self.static_route_hints.direct_route_updates(local_endpoint) {
            self.publish_route_update(update);
        }
    }
}

fn route_establishment_config(peer_announcement_bind_addr: SocketAddr) -> RouteEstablishmentConfig {
    let mut config = RouteEstablishmentConfig::new();
    config.peer_announcement_bind_addr = peer_announcement_bind_addr;
    config
}

fn flatten_manual_route_watch_ask_result<E>(
    result: Result<Result<(), ManualRouteWatchError>, E>,
) -> Result<Result<(), ManualRouteWatchError>, RuntimeControlError>
where
    E: StdError + Send + Sync + 'static,
{
    result.boxed().context(ControlFutureSnafu)
}

impl ComponentTopology for DiscoveryTopology {
    #[cfg(not(any(test, feature = "test-support")))]
    fn nodes(&self) -> impl DoubleEndedIterator<Item = &dyn RuntimeLifecycleComponent> {
        [
            &self.peer_announcement as &dyn RuntimeLifecycleComponent,
            &self.peer_announcement_observation as &dyn RuntimeLifecycleComponent,
            &self.route_establishment as &dyn RuntimeLifecycleComponent,
            &self.key_material_discovery as &dyn RuntimeLifecycleComponent,
            &self.local_endpoint_manager as &dyn RuntimeLifecycleComponent,
        ]
        .into_iter()
    }

    #[cfg(any(test, feature = "test-support"))]
    fn nodes(&self) -> impl DoubleEndedIterator<Item = &dyn RuntimeLifecycleComponent> {
        [
            &self.peer_announcement as &dyn RuntimeLifecycleComponent,
            &self.peer_announcement_observation as &dyn RuntimeLifecycleComponent,
            &self.route_establishment as &dyn RuntimeLifecycleComponent,
            &self.key_material_discovery as &dyn RuntimeLifecycleComponent,
            &self.manual_route_discovery as &dyn RuntimeLifecycleComponent,
            &self.local_endpoint_manager as &dyn RuntimeLifecycleComponent,
        ]
        .into_iter()
    }
}

/// Replication runtime logic topology.
///
/// ```text
/// GroupBroadcastComponent ----+--> CatchUpManagerComponent
///                             |
///                             v
///                  ReplicationRuntimeComponent
///                             ^
///                             |
/// ReliableDeliveryComponent --+--> SummaryRequestManagerComponent
/// ```
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

/// Full runtime host topology.
///
/// ```text
/// IoTopology --sockets--+--> TransportTopology --inbound payloads--+
///                       |                         |                 |
///                       v                         |                 v
///                DiscoveryTopology --routes-------+----------> DeliveryTopology --semantic events--> RuntimeLogicTopology
///                       ^                         |
///                       +----inbound payloads-----+
/// ```
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
    static_route_hints: PreconfiguredPeerRoutesConfig,
}

impl RuntimeTopology {
    fn build(system: &KompactSystem, input: RuntimeTopologyBuildInput) -> Self {
        let io = IoTopology::build(system);
        let transport = TransportTopology::build(system, &io.bridge);
        let egress_pool = IoBridgeHandle::from_component(&io.bridge)
            .egress_pool()
            .clone();
        let manager_ref = transport.manager_ref();
        let discovery_transport_handles = DiscoveryTransportHandles {
            route_transport: manager_ref.clone(),
            egress_pool,
        };
        let delivery = DeliveryTopology::build(
            system,
            input.group_memberships.clone(),
            input.local_member.clone(),
            manager_ref.clone(),
            input.security.clone(),
        );
        let discovery = DiscoveryTopology::build(
            system,
            input.host_config,
            input.group_memberships.clone(),
            input.local_member.clone(),
            input.security.clone(),
            discovery_transport_handles,
            input.static_route_hints,
        );
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
        self.discovery.connect_transport(&self.transport)?;
        self.delivery.connect_internal_routes()?;
        self.discovery.connect_internal_routes()?;
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
        #[cfg(any(test, feature = "test-support"))] local_endpoint_lease: ReservedSocketLease,
    ) -> Self {
        Self {
            system: Some(system),
            topology: Some(topology),
            group_memberships,
            control_timeout,
            external_udp_addr,
            #[cfg(any(test, feature = "test-support"))]
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
            #[cfg(test)]
            PreconfiguredPeerRoutesPublishMode::ManualForTest,
        )
        .await
    }

    async fn start_with_options(
        local_member: &MemberIdentity,
        store: Arc<dyn ReplicationStore>,
        listener: Arc<dyn ReplicationEventListener>,
        security: DeliverySecurity,
        runtime_config_toml: Option<&str>,
        #[cfg(test)] route_publish_mode: PreconfiguredPeerRoutesPublishMode,
    ) -> Result<Self, RuntimeHostError> {
        let built_system = build_runtime_system(runtime_config_toml).await?;
        let system = built_system.system.clone();
        let host_config = DeliveryRuntimeHostConfig::from_system_config(&system)?;
        let routes_config = PreconfiguredPeerRoutesConfig::from_config(system.config())?;
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
                static_route_hints: routes_config,
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
        #[cfg(any(test, feature = "test-support"))]
        let mut local_endpoint_lease = built_system.local_endpoint_lease;
        #[cfg(any(test, feature = "test-support"))]
        release_reserved_runtime_bindings(
            &mut local_endpoint_lease,
            local_endpoint.local_addr,
            host_config.peer_announcement_bind_addr,
        )?;
        topology
            .discovery
            .configure_route_establishment_watches(host_config.control_timeout)
            .await?;
        #[cfg(test)]
        if route_publish_mode == PreconfiguredPeerRoutesPublishMode::OnLocalEndpointBound {
            topology
                .discovery
                .publish_preconfigured_peer_routes(local_endpoint.local_addr);
        }

        Ok(Self::new(
            system,
            topology,
            group_memberships,
            host_config.control_timeout,
            local_endpoint.local_addr,
            #[cfg(any(test, feature = "test-support"))]
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
            #[cfg(any(test, feature = "test-support"))]
            rebind_reserved_runtime_local_endpoint_binding(&mut self.local_endpoint_lease);
            panic!("failed to stop delivery runtime host cleanly: {error}");
        }
        shutdown_system_bounded(system, self.control_timeout, false);
        #[cfg(any(test, feature = "test-support"))]
        rebind_reserved_runtime_local_endpoint_binding(&mut self.local_endpoint_lease);
    }

    /// Replace the authoritative shared group-membership snapshot.
    #[cfg(test)]
    pub(crate) fn replace_group_memberships(&self, memberships: GroupMemberships) {
        self.group_memberships.replace(memberships);
    }

    /// Publish one route-discovery update into both semantic delivery owners.
    ///
    /// This is temporary until the replication runtime is wired to the real
    /// discovery mechanism. Today it exists so tests and bring-up tooling can
    /// inject direct routes explicitly.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn publish_route_update(
        &self,
        update: flotsync_routes::DiscoveryRouteUpdate<TransportRouteKey>,
    ) {
        self.topology().discovery.publish_route_update(update);
    }

    #[cfg(test)]
    pub(crate) fn publish_preconfigured_peer_routes_for_test(&self) {
        self.topology()
            .discovery
            .publish_preconfigured_peer_routes(self.external_udp_addr);
    }

    /// Return the concrete local UDP socket address currently bound by this
    /// host for externally reachable delivery traffic.
    #[cfg(test)]
    pub(crate) fn external_udp_bind_addr(&self) -> SocketAddr {
        self.external_udp_addr
    }

    /// Read the current authoritative membership snapshot.
    #[cfg_attr(not(any(test, feature = "test-support")), allow(dead_code))]
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
        #[cfg(any(test, feature = "test-support"))]
        rebind_reserved_runtime_local_endpoint_binding(&mut self.local_endpoint_lease);
    }
}

#[cfg(any(test, feature = "test-support"))]
async fn build_runtime_system(
    runtime_config_toml: Option<&str>,
) -> Result<BuiltRuntimeSystem, RuntimeHostError> {
    let local_endpoint_lease =
        reserve_sockets(&[ReservedSocketKind::UdpSocket, ReservedSocketKind::UdpSocket]);
    let local_endpoint_bind_addr = local_endpoint_lease.addr(0).to_string();
    let peer_announcement_bind_addr = local_endpoint_lease.addr(1).to_string();
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
    config.set_config_value(
        &discovery_config_keys::PEER_ANNOUNCEMENT_BIND_ADDR,
        peer_announcement_bind_addr,
    );
    let system = config.build().await.context(BuildSystemSnafu)?;
    Ok(BuiltRuntimeSystem {
        system,
        local_endpoint_lease,
    })
}

#[cfg(not(any(test, feature = "test-support")))]
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

#[cfg(any(test, feature = "test-support"))]
fn release_reserved_runtime_bindings(
    local_endpoint_lease: &mut ReservedSocketLease,
    local_addr: SocketAddr,
    peer_announcement_addr: SocketAddr,
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
    let reserved_peer_announcement_addr = local_endpoint_lease.addr(1);
    if reserved_peer_announcement_addr != peer_announcement_addr {
        return Err(RuntimeHostError::BindLocalEndpoint {
            source: RuntimeControlError::failed(format!(
                "runtime host peer announcement configured at {peer_announcement_addr}, but the reserved test socket expected {reserved_peer_announcement_addr}"
            )),
        });
    }
    local_endpoint_lease.activate_live_binding(1);
    Ok(())
}

#[cfg(any(test, feature = "test-support"))]
fn rebind_reserved_runtime_local_endpoint_binding(local_endpoint_lease: &mut ReservedSocketLease) {
    for index in 0..local_endpoint_lease.len() {
        let rebind_result = local_endpoint_lease.rebind_binding(index);
        if let Err(error) = rebind_result
            && !std::thread::panicking()
        {
            panic!("rebind reserved runtime-host socket {index}: {error}");
        }
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

#[cfg(any(test, feature = "test-support"))]
/// Test-support accessors and route injection helpers for runtime fixtures.
pub(crate) trait DeliveryRuntimeHostTestExt {
    /// Return the address peers should use when this host bound an unspecified interface.
    fn advertised_loopback_udp_addr(&self) -> SocketAddr;
    /// Publish a direct unicast peer route and wait until delivery components observe it.
    fn publish_direct_peer_route(&self, peer: MemberIdentity, remote_addr: SocketAddr);
    /// Publish configured static routes after a test has explicitly requested them.
    #[cfg(test)]
    fn publish_preconfigured_peer_routes(&self);
    /// Return whether both direct-delivery components currently know a peer route.
    #[cfg(test)]
    fn knows_direct_peer_route(&self, peer: &MemberIdentity) -> bool;
    /// Wait until both direct-delivery components have observed a peer route.
    #[cfg(test)]
    fn wait_for_direct_peer_route(&self, peer: &MemberIdentity);
    /// Wait until the runtime component accepts one mailbox turn.
    #[cfg(test)]
    fn wait_for_runtime_startup(&self);
}

#[cfg(any(test, feature = "test-support"))]
impl DeliveryRuntimeHostTestExt for DeliveryRuntimeHost {
    fn advertised_loopback_udp_addr(&self) -> SocketAddr {
        loopback_advertise_addr(self.external_udp_addr)
    }

    fn publish_direct_peer_route(&self, peer: MemberIdentity, remote_addr: SocketAddr) {
        use flotsync_routes::{
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
        self.publish_route_update(flotsync_routes::DiscoveryRouteUpdate::PeerRoutes {
            peer: peer.clone(),
            routes: vec![route],
        });
        wait_for_direct_peer_route(self.topology(), &peer);
    }

    #[cfg(test)]
    fn publish_preconfigured_peer_routes(&self) {
        self.publish_preconfigured_peer_routes_for_test();
    }

    #[cfg(test)]
    fn knows_direct_peer_route(&self, peer: &MemberIdentity) -> bool {
        let broadcast_peer = peer.clone();
        let broadcast_knows = self
            .topology()
            .delivery
            .group_broadcast
            .on_definition(|component| component.knows_direct_route(&broadcast_peer));
        let reliable_peer = peer.clone();
        let reliable_knows = self
            .topology()
            .delivery
            .reliable_delivery
            .on_definition(|component| component.knows_direct_route(&reliable_peer));
        broadcast_knows && reliable_knows
    }

    #[cfg(test)]
    fn wait_for_direct_peer_route(&self, peer: &MemberIdentity) {
        wait_for_direct_peer_route(self.topology(), peer);
    }

    #[cfg(test)]
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

#[cfg(any(test, feature = "test-support"))]
fn loopback_advertise_addr(bind_addr: SocketAddr) -> SocketAddr {
    if !bind_addr.ip().is_unspecified() {
        return bind_addr;
    }

    match bind_addr {
        SocketAddr::V4(addr) => SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, addr.port())),
        SocketAddr::V6(addr) => SocketAddr::from((std::net::Ipv6Addr::LOCALHOST, addr.port())),
    }
}

#[cfg(any(test, feature = "test-support"))]
fn wait_for_direct_peer_route(topology: &RuntimeTopology, peer: &MemberIdentity) {
    use flotsync_io::test_support::eventually_component_state;

    let broadcast_peer = peer.clone();
    eventually_component_state(
        TEST_DIRECT_PEER_ROUTE_TIMEOUT,
        &topology.delivery.group_broadcast,
        |component| component.knows_direct_route(&broadcast_peer),
        format_args!("timed out waiting for group-broadcast route publication for peer={peer}"),
    );

    let reliable_peer = peer.clone();
    eventually_component_state(
        TEST_DIRECT_PEER_ROUTE_TIMEOUT,
        &topology.delivery.reliable_delivery,
        |component| component.knows_direct_route(&reliable_peer),
        format_args!("timed out waiting for reliable-delivery route publication for peer={peer}"),
    );
}
