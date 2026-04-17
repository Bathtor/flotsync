use super::ReplicationRuntimeComponent;
use crate::{
    GroupMemberships,
    SharedGroupMemberships,
    api::{MemberIdentity, ReplicationEventListener, ReplicationStore},
    delivery::{
        contracts::{GroupBroadcastPort, ReliableDeliveryPort},
        group_broadcast::{GroupBroadcastComponent, GroupBroadcastInboundPort},
        ingress::{DeliveryIngressComponent, DeliveryInterestConfig},
        reliable_delivery::{ReliableDeliveryComponent, ReliableDeliveryInboundPort},
        route_transport::{
            DiscoveryRouteUpdate,
            RouteDiscoveryPort,
            RouteTransportPort,
            TransportRouteKey,
            manager::{RouteTransportManager, configure_replication_runtime},
        },
    },
};
use flotsync_io::prelude::{DriverConfig, IoBridge, IoBridgeHandle, IoDriverComponent};
use flotsync_udpour::UDPourConfig;
use kompact::{KompactLogger, config::HoconExt, prelude::*};
use snafu::prelude::*;
use std::{collections::HashSet, net::SocketAddr, sync::Arc, time::Duration};

mod discovery;
mod local_endpoint;

use discovery::{RuntimeDiscoverySource, RuntimeDiscoverySourceMessage};
use local_endpoint::{LocalEndpointManager, ensure_local_endpoint_bound};

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
}

struct RuntimeTopology {
    driver: Arc<Component<IoDriverComponent>>,
    bridge: Arc<Component<IoBridge>>,
    manager: Arc<Component<RouteTransportManager>>,
    ingress: Arc<Component<DeliveryIngressComponent>>,
    group_broadcast: Arc<Component<GroupBroadcastComponent>>,
    reliable_delivery: Arc<Component<ReliableDeliveryComponent>>,
    discovery_source: Arc<Component<RuntimeDiscoverySource>>,
    discovery_source_ref: ActorRefStrong<RuntimeDiscoverySourceMessage>,
    local_endpoint_manager: Arc<Component<LocalEndpointManager>>,
    runtime_component: Arc<Component<ReplicationRuntimeComponent>>,
}

impl RuntimeTopology {
    fn build(
        system: &KompactSystem,
        group_memberships: &SharedGroupMemberships,
        local_member: &MemberIdentity,
        store: Arc<dyn ReplicationStore>,
        listener: Arc<dyn ReplicationEventListener>,
        local_endpoint_bind_addr: SocketAddr,
    ) -> Self {
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
        let local_endpoint_manager =
            system.create(move || LocalEndpointManager::new(local_endpoint_bind_addr));
        let runtime_memberships = group_memberships.clone();
        let runtime_local_member = local_member.clone();
        let runtime_component = system.create(move || {
            ReplicationRuntimeComponent::new(
                runtime_local_member.clone(),
                store.clone(),
                listener.clone(),
                runtime_memberships.clone(),
            )
        });

        Self {
            driver,
            bridge,
            manager,
            ingress,
            group_broadcast,
            reliable_delivery,
            discovery_source,
            discovery_source_ref,
            local_endpoint_manager,
            runtime_component,
        }
    }

    fn connect_all(&self) -> Result<(), RuntimeHostError> {
        self.connect_components::<TransportRoutePort, _, _>(
            &self.manager,
            &self.ingress,
            "route transport -> ingress",
        )?;
        self.connect_components::<GroupBroadcastInboundRoutePort, _, _>(
            &self.ingress,
            &self.group_broadcast,
            "ingress -> group broadcast",
        )?;
        self.connect_components::<ReliableDeliveryInboundRoutePort, _, _>(
            &self.ingress,
            &self.reliable_delivery,
            "ingress -> reliable delivery",
        )?;
        self.connect_components::<RouteDiscoveryPort<TransportRouteKey>, _, _>(
            &self.discovery_source,
            &self.group_broadcast,
            "discovery -> group broadcast",
        )?;
        self.connect_components::<RouteDiscoveryPort<TransportRouteKey>, _, _>(
            &self.discovery_source,
            &self.reliable_delivery,
            "discovery -> reliable delivery",
        )?;
        self.connect_components::<GroupBroadcastPort, _, _>(
            &self.group_broadcast,
            &self.runtime_component,
            "group broadcast -> replication runtime",
        )?;
        self.connect_components::<ReliableDeliveryPort, _, _>(
            &self.reliable_delivery,
            &self.runtime_component,
            "reliable delivery -> replication runtime",
        )?;
        Ok(())
    }

    fn start_all(
        &self,
        system: &KompactSystem,
        control_timeout: Duration,
    ) -> Result<(), RuntimeHostError> {
        self.start_component(system, &self.driver, "io_driver", control_timeout)?;
        self.start_component(system, &self.bridge, "io_bridge", control_timeout)?;
        let udp_connect_handle = IoBridgeHandle::from_component(&self.bridge);
        block_on(udp_connect_handle.connect_udp(&self.manager)).map_err(|error| {
            RuntimeHostError::ConnectUdp {
                message: format!("{error:?}"),
            }
        })?;
        block_on(udp_connect_handle.connect_udp(&self.local_endpoint_manager)).map_err(
            |error| RuntimeHostError::ConnectUdp {
                message: format!("{error:?}"),
            },
        )?;
        self.start_component(system, &self.manager, "route_transport", control_timeout)?;
        self.start_component(system, &self.ingress, "delivery_ingress", control_timeout)?;
        self.start_component(
            system,
            &self.group_broadcast,
            "group_broadcast",
            control_timeout,
        )?;
        self.start_component(
            system,
            &self.reliable_delivery,
            "reliable_delivery",
            control_timeout,
        )?;
        self.start_component(
            system,
            &self.discovery_source,
            "discovery_source",
            control_timeout,
        )?;
        self.start_component(
            system,
            &self.local_endpoint_manager,
            "local_endpoint_manager",
            control_timeout,
        )?;
        self.start_component(
            system,
            &self.runtime_component,
            "replication_runtime",
            control_timeout,
        )?;
        Ok(())
    }

    fn stop_all(&self, system: &KompactSystem, control_timeout: Duration) {
        self.stop_component(system, &self.runtime_component, control_timeout);
        self.stop_component(system, &self.reliable_delivery, control_timeout);
        self.stop_component(system, &self.group_broadcast, control_timeout);
        self.stop_component(system, &self.ingress, control_timeout);
        self.stop_component(system, &self.discovery_source, control_timeout);
        self.stop_component(system, &self.local_endpoint_manager, control_timeout);
        self.stop_component(system, &self.manager, control_timeout);
        self.stop_component(system, &self.bridge, control_timeout);
        self.stop_component(system, &self.driver, control_timeout);
    }

    fn connect_components<P, C1, C2>(
        &self,
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
                message: format!("{error:?}"),
            }
        })?;
        Ok(())
    }

    fn start_component<C>(
        &self,
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
                message: format!("{error:?}"),
            })
    }

    fn stop_component<C>(
        &self,
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
}

impl DeliveryRuntimeHost {
    fn new(
        system: KompactSystem,
        topology: RuntimeTopology,
        group_memberships: SharedGroupMemberships,
        control_timeout: Duration,
        external_udp_addr: SocketAddr,
    ) -> Self {
        Self {
            system: Some(system),
            topology: Some(topology),
            group_memberships,
            control_timeout,
            external_udp_addr,
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
        &self.topology().runtime_component
    }

    /// Start one new delivery runtime host for a single local member.
    pub(crate) fn start(
        local_member: MemberIdentity,
        store: Arc<dyn ReplicationStore>,
        listener: Arc<dyn ReplicationEventListener>,
    ) -> Result<Self, RuntimeHostError> {
        let system = build_runtime_system()?;
        let host_config = DeliveryRuntimeHostConfig::from_system_config(&system)?;
        let group_memberships = SharedGroupMemberships::default();
        let topology = RuntimeTopology::build(
            &system,
            &group_memberships,
            &local_member,
            store,
            listener,
            host_config.local_endpoint_bind_addr,
        );
        topology.connect_all()?;
        topology.start_all(&system, host_config.control_timeout)?;
        let local_endpoint = ensure_local_endpoint_bound(
            &topology.local_endpoint_manager,
            host_config.control_timeout,
        )?;

        Ok(Self::new(
            system,
            topology,
            group_memberships,
            host_config.control_timeout,
            local_endpoint.local_addr,
        ))
    }

    pub(crate) fn shutdown(&mut self) {
        let Some(topology) = self.topology.take() else {
            return;
        };
        let Some(system) = self.system.take() else {
            return;
        };
        topology.stop_all(&system, self.control_timeout);
        drop(topology);
        let _ = system.kill_system();
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
        self.topology()
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
}

impl Drop for DeliveryRuntimeHost {
    fn drop(&mut self) {
        let Some(topology) = self.topology.take() else {
            return;
        };
        let Some(system) = self.system.take() else {
            return;
        };
        drop(topology);
        let _ = system.kill_system();
    }
}

fn build_runtime_system() -> Result<KompactSystem, RuntimeHostError> {
    let mut config = KompactConfig::default();
    configure_replication_runtime(&mut config);
    config
        .build()
        .map_err(|error| RuntimeHostError::BuildSystem {
            message: error.to_string(),
        })
}

#[cfg(test)]
pub(crate) trait DeliveryRuntimeHostTestExt {
    fn advertised_loopback_udp_addr(&self) -> SocketAddr;
    fn publish_direct_peer_route(&self, peer: MemberIdentity, remote_addr: SocketAddr);
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
        self.publish_route_update(DiscoveryRouteUpdate::PeerRoutes {
            peer: peer.clone(),
            classification: ReachabilityClass::Reachable,
            routes: vec![route],
        });
        wait_for_direct_peer_route(self.topology(), &peer);
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
        &topology.group_broadcast,
        |component| component.knows_direct_route(&broadcast_peer),
        format_args!("timed out waiting for group-broadcast route publication for peer={peer}"),
    );

    let reliable_peer = peer.clone();
    eventually_component_state(
        FULL_STACK_WAIT_TIMEOUT,
        &topology.reliable_delivery,
        |component| component.knows_direct_route(&reliable_peer),
        format_args!("timed out waiting for reliable-delivery route publication for peer={peer}"),
    );
}
