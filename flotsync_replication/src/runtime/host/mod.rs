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
    api::{MemberIdentity, ReplicationEventListener, ReplicationStore},
    delivery::{
        contracts::{GroupBroadcastPort, ReliableDeliveryPort},
        group_broadcast::{GroupBroadcastComponent, GroupBroadcastInboundPort},
        ingress::{DeliveryIngressComponent, DeliveryInterestConfig},
        reliable_delivery::{ReliableDeliveryComponent, ReliableDeliveryInboundPort},
        route_transport::{
            RouteDiscoveryPort,
            RouteTransportPort,
            TransportRouteKey,
            manager::{RouteTransportManager, configure_replication_runtime},
        },
    },
};
#[cfg(test)]
use flotsync_io::test_support::{
    ReservedSocketKind,
    ReservedSocketLease,
    build_test_kompact_system_with,
    enable_bind_reuse_address,
    reserve_sockets,
    set_test_system_label,
};
use flotsync_io::{
    kompact::shutdown_system_bounded,
    prelude::{DriverConfig, IoBridge, IoBridgeHandle, IoDriverComponent},
};
use flotsync_udpour::UDPourConfig;
use kompact::{KompactLogger, prelude::*};
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
    #[snafu(display("Failed to build Kompact system for the replication runtime: {message}"))]
    BuildSystem { message: String },
    #[snafu(display("Invalid replication runtime host config at {key}: {message}"))]
    InvalidConfig { key: &'static str, message: String },
    #[snafu(display("Failed to start runtime component '{component}': {message}"))]
    StartComponent {
        component: &'static str,
        message: String,
    },
    #[snafu(display("Failed to stop runtime component '{component}': {message}"))]
    StopComponent {
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

struct RuntimeTopology {
    driver: Arc<Component<IoDriverComponent>>,
    bridge: Arc<Component<IoBridge>>,
    manager: Arc<Component<RouteTransportManager>>,
    ingress: Arc<Component<DeliveryIngressComponent>>,
    group_broadcast: Arc<Component<GroupBroadcastComponent>>,
    reliable_delivery: Arc<Component<ReliableDeliveryComponent>>,
    preconfigured_peer_routes: Arc<Component<PreconfiguredPeerRoutesComponent>>,
    #[cfg_attr(not(test), allow(dead_code))]
    preconfigured_peer_routes_ref: ActorRefStrong<PreconfiguredPeerRoutesMessage>,
    local_endpoint_manager: Arc<Component<LocalEndpointManager>>,
    catch_up_manager: Arc<Component<CatchUpManagerComponent>>,
    summary_request_manager: Arc<Component<SummaryRequestManagerComponent>>,
    runtime_component: Arc<Component<ReplicationRuntimeComponent>>,
}

struct BuiltRuntimeSystem {
    system: KompactSystem,
    #[cfg(test)]
    local_endpoint_lease: ReservedSocketLease,
}

impl RuntimeTopology {
    #[allow(clippy::too_many_lines)]
    fn build(
        system: &KompactSystem,
        group_memberships: &SharedGroupMemberships,
        local_member: &MemberIdentity,
        store: Arc<dyn ReplicationStore>,
        listener: Arc<dyn ReplicationEventListener>,
        host_config: DeliveryRuntimeHostConfig,
        route_publish_mode: PreconfiguredPeerRoutesPublishMode,
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
        let preconfigured_peer_routes = system.create(move || {
            PreconfiguredPeerRoutesComponent::new(
                host_config.local_endpoint_bind_addr,
                route_publish_mode,
            )
        });
        let preconfigured_peer_routes_ref = preconfigured_peer_routes
            .actor_ref()
            .hold()
            .expect("preconfigured peer routes must expose a strong actor ref");
        let local_endpoint_manager =
            system.create(move || LocalEndpointManager::new(host_config.local_endpoint_bind_addr));
        let catch_up_memberships = group_memberships.clone();
        let catch_up_local_member = local_member.clone();
        let catch_up_store = store.clone();
        let catch_up_manager = system.create(move || {
            CatchUpManagerComponent::new(
                catch_up_local_member,
                catch_up_memberships,
                catch_up_store,
            )
        });
        let catch_up_manager_ref = catch_up_manager
            .actor_ref()
            .hold()
            .expect("catch-up manager must expose a strong actor ref");
        let summary_manager_memberships = group_memberships.clone();
        let summary_manager_local_member = local_member.clone();
        let summary_request_timeout = host_config.summary_request_timeout;
        let summary_request_manager = system.create(move || {
            SummaryRequestManagerComponent::new(
                summary_manager_local_member,
                summary_manager_memberships,
                summary_request_timeout,
            )
        });
        let summary_request_manager_ref = summary_request_manager
            .actor_ref()
            .hold()
            .expect("summary request manager must expose a strong actor ref");
        let runtime_memberships = group_memberships.clone();
        let runtime_local_member = local_member.clone();
        let runtime_component = system.create(move || {
            ReplicationRuntimeComponent::new(
                runtime_local_member,
                store,
                listener,
                runtime_memberships,
                summary_request_manager_ref,
                catch_up_manager_ref,
            )
        });

        Self {
            driver,
            bridge,
            manager,
            ingress,
            group_broadcast,
            reliable_delivery,
            preconfigured_peer_routes,
            preconfigured_peer_routes_ref,
            local_endpoint_manager,
            catch_up_manager,
            summary_request_manager,
            runtime_component,
        }
    }

    fn connect_all(&self) -> Result<(), RuntimeHostError> {
        Self::connect_components::<TransportRoutePort, _, _>(
            &self.manager,
            &self.ingress,
            "route transport -> ingress",
        )?;
        Self::connect_components::<GroupBroadcastInboundRoutePort, _, _>(
            &self.ingress,
            &self.group_broadcast,
            "ingress -> group broadcast",
        )?;
        Self::connect_components::<ReliableDeliveryInboundRoutePort, _, _>(
            &self.ingress,
            &self.reliable_delivery,
            "ingress -> reliable delivery",
        )?;
        Self::connect_components::<RouteDiscoveryPort<TransportRouteKey>, _, _>(
            &self.preconfigured_peer_routes,
            &self.group_broadcast,
            "preconfigured peer routes -> group broadcast",
        )?;
        Self::connect_components::<RouteDiscoveryPort<TransportRouteKey>, _, _>(
            &self.preconfigured_peer_routes,
            &self.reliable_delivery,
            "preconfigured peer routes -> reliable delivery",
        )?;
        Self::connect_components::<GroupBroadcastPort, _, _>(
            &self.group_broadcast,
            &self.runtime_component,
            "group broadcast -> replication runtime",
        )?;
        Self::connect_components::<GroupBroadcastPort, _, _>(
            &self.group_broadcast,
            &self.catch_up_manager,
            "group broadcast -> catch-up manager",
        )?;
        Self::connect_components::<ReliableDeliveryPort, _, _>(
            &self.reliable_delivery,
            &self.runtime_component,
            "reliable delivery -> replication runtime",
        )?;
        Self::connect_components::<ReliableDeliveryPort, _, _>(
            &self.reliable_delivery,
            &self.summary_request_manager,
            "reliable delivery -> summary request manager",
        )?;
        Ok(())
    }

    fn start_all(
        &self,
        system: &KompactSystem,
        control_timeout: Duration,
    ) -> Result<(), RuntimeHostError> {
        Self::start_component(system, &self.driver, "io_driver", control_timeout)?;
        Self::start_component(system, &self.bridge, "io_bridge", control_timeout)?;
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
        block_on(udp_connect_handle.connect_udp(&self.preconfigured_peer_routes)).map_err(
            |error| RuntimeHostError::ConnectUdp {
                message: format!("{error:?}"),
            },
        )?;
        Self::start_component(system, &self.manager, "route_transport", control_timeout)?;
        Self::start_component(system, &self.ingress, "delivery_ingress", control_timeout)?;
        Self::start_component(
            system,
            &self.group_broadcast,
            "group_broadcast",
            control_timeout,
        )?;
        Self::start_component(
            system,
            &self.reliable_delivery,
            "reliable_delivery",
            control_timeout,
        )?;
        Self::start_component(
            system,
            &self.preconfigured_peer_routes,
            "preconfigured_peer_routes",
            control_timeout,
        )?;
        Self::start_component(
            system,
            &self.local_endpoint_manager,
            "local_endpoint_manager",
            control_timeout,
        )?;
        Self::start_component(
            system,
            &self.catch_up_manager,
            "catch_up_manager",
            control_timeout,
        )?;
        Self::start_component(
            system,
            &self.summary_request_manager,
            "summary_request_manager",
            control_timeout,
        )?;
        Self::start_component(
            system,
            &self.runtime_component,
            "replication_runtime",
            control_timeout,
        )?;
        Ok(())
    }

    fn stop_all(
        &self,
        system: &KompactSystem,
        control_timeout: Duration,
    ) -> Result<(), RuntimeHostError> {
        Self::stop_component(
            system,
            &self.runtime_component,
            "replication_runtime",
            control_timeout,
        )?;
        Self::stop_component(
            system,
            &self.summary_request_manager,
            "summary_request_manager",
            control_timeout,
        )?;
        Self::stop_component(
            system,
            &self.catch_up_manager,
            "catch_up_manager",
            control_timeout,
        )?;
        Self::stop_component(
            system,
            &self.reliable_delivery,
            "reliable_delivery",
            control_timeout,
        )?;
        Self::stop_component(
            system,
            &self.group_broadcast,
            "group_broadcast",
            control_timeout,
        )?;
        Self::stop_component(system, &self.ingress, "delivery_ingress", control_timeout)?;
        Self::stop_component(
            system,
            &self.preconfigured_peer_routes,
            "preconfigured_peer_routes",
            control_timeout,
        )?;
        Self::stop_component(
            system,
            &self.local_endpoint_manager,
            "local_endpoint_manager",
            control_timeout,
        )?;
        Self::stop_component(system, &self.manager, "route_transport", control_timeout)?;
        Self::stop_component(system, &self.bridge, "io_bridge", control_timeout)?;
        Self::stop_component(system, &self.driver, "io_driver", control_timeout)?;
        Ok(())
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
                message: format!("{error:?}"),
            }
        })?;
        Ok(())
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
                message: format!("{error:?}"),
            })
    }

    fn stop_component<C>(
        system: &KompactSystem,
        component: &Arc<Component<C>>,
        name: &'static str,
        control_timeout: Duration,
    ) -> Result<(), RuntimeHostError>
    where
        C: ComponentDefinition + ComponentLifecycle + Sized + 'static,
    {
        system
            .stop_notify(component)
            .wait_timeout(control_timeout)
            .map_err(|error| RuntimeHostError::StopComponent {
                component: name,
                message: format!("{error:?}"),
            })
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
        &self.topology().runtime_component
    }

    /// Start one new delivery runtime host for a single local member.
    #[cfg(test)]
    pub(crate) fn start(
        local_member: &MemberIdentity,
        store: Arc<dyn ReplicationStore>,
        listener: Arc<dyn ReplicationEventListener>,
    ) -> Result<Self, RuntimeHostError> {
        Self::start_with_runtime_config_toml(local_member, store, listener, None)
    }

    /// Start one new delivery runtime host with an additional in-memory TOML
    /// config fragment merged into the Kompact runtime config.
    pub(crate) fn start_with_runtime_config_toml(
        local_member: &MemberIdentity,
        store: Arc<dyn ReplicationStore>,
        listener: Arc<dyn ReplicationEventListener>,
        runtime_config_toml: Option<&str>,
    ) -> Result<Self, RuntimeHostError> {
        Self::start_with_options(
            local_member,
            store,
            listener,
            runtime_config_toml,
            PreconfiguredPeerRoutesPublishMode::OnLocalEndpointBound,
        )
    }

    fn start_with_options(
        local_member: &MemberIdentity,
        store: Arc<dyn ReplicationStore>,
        listener: Arc<dyn ReplicationEventListener>,
        runtime_config_toml: Option<&str>,
        route_publish_mode: PreconfiguredPeerRoutesPublishMode,
    ) -> Result<Self, RuntimeHostError> {
        let built_system = build_runtime_system(runtime_config_toml)?;
        let system = built_system.system.clone();
        let host_config = DeliveryRuntimeHostConfig::from_system_config(&system)?;
        let routes_config = PreconfiguredPeerRoutesConfig::from_config(system.config())?;
        routes_config.validate_local_endpoint_bind_addr(host_config.local_endpoint_bind_addr)?;
        let group_memberships = SharedGroupMemberships::new(GroupMemberships::new());
        let topology = RuntimeTopology::build(
            &system,
            &group_memberships,
            local_member,
            store,
            listener,
            host_config,
            route_publish_mode,
        );
        topology.connect_all()?;
        topology.start_all(&system, host_config.control_timeout)?;
        let local_endpoint = ensure_local_endpoint_bound(
            &topology.local_endpoint_manager,
            host_config.control_timeout,
        )
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
    pub(super) fn start_with_route_publish_mode_for_test(
        local_member: &MemberIdentity,
        store: Arc<dyn ReplicationStore>,
        listener: Arc<dyn ReplicationEventListener>,
        runtime_config_toml: Option<&str>,
        route_publish_mode: PreconfiguredPeerRoutesPublishMode,
    ) -> Result<Self, RuntimeHostError> {
        Self::start_with_options(
            local_member,
            store,
            listener,
            runtime_config_toml,
            route_publish_mode,
        )
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
            .preconfigured_peer_routes_ref
            .tell(PreconfiguredPeerRoutesMessage::Publish(update));
    }

    #[cfg(test)]
    pub(crate) fn publish_preconfigured_peer_routes_for_test(&self) {
        self.topology()
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
#[allow(
    clippy::unnecessary_wraps,
    reason = "The test and production cfg variants share a Result-returning interface."
)]
fn build_runtime_system(
    runtime_config_toml: Option<&str>,
) -> Result<BuiltRuntimeSystem, RuntimeHostError> {
    let local_endpoint_lease = reserve_sockets(&[ReservedSocketKind::UdpSocket]);
    let local_endpoint_bind_addr = local_endpoint_lease.addr(0).to_string();
    let system = build_test_kompact_system_with(|config| {
        set_test_system_label(config, "replication-runtime-host-test-system");
        enable_bind_reuse_address(config);
        configure_replication_runtime(config);
        if let Some(runtime_config_toml) = runtime_config_toml {
            config.load_config_str(runtime_config_toml);
        }
        config.set_config_value(
            &config_keys::LOCAL_ENDPOINT_BIND_ADDR,
            local_endpoint_bind_addr,
        );
    });
    Ok(BuiltRuntimeSystem {
        system,
        local_endpoint_lease,
    })
}

#[cfg(not(test))]
fn build_runtime_system(
    runtime_config_toml: Option<&str>,
) -> Result<BuiltRuntimeSystem, RuntimeHostError> {
    let mut config = KompactConfig::default();
    configure_replication_runtime(&mut config);
    if let Some(runtime_config_toml) = runtime_config_toml {
        config.load_config_str(runtime_config_toml);
    }
    let system = config
        .build()
        .map_err(|error| RuntimeHostError::BuildSystem {
            message: error.to_string(),
        })?;
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
            message: format!(
                "runtime host bound local endpoint at {local_addr}, but the reserved test socket expected {reserved_addr}"
            ),
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
    let RuntimeHostError::BindLocalEndpoint { message } = error else {
        return error;
    };
    let system_label = system
        .config()
        .read_or_default(&kompact::config_keys::system::LABEL)
        .unwrap_or_else(|_| String::from("<unlabelled-runtime-host-system>"));
    RuntimeHostError::BindLocalEndpoint {
        message: format!(
            "{message}; configured_bind_addr={configured_bind_addr}; system_label={system_label}"
        ),
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
