//! Kompact component topology for a replication runtime host.

use super::{
    ReplicationRuntimeComponent,
    catch_up_manager::CatchUpManagerComponent,
    component::{
        RuntimeApplicationServices,
        RuntimeComponentActors,
        RuntimeIdentityContext,
        RuntimeSecurityContext,
    },
    summary_request_manager::SummaryRequestManagerComponent,
};
#[cfg(test)]
use super::{ReplicationRuntimeMessage, handle::wait_for_test_reply};
use crate::{
    api::{BoxError, ReplicationConfig, ReplicationEventListener, ReplicationStore},
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
use flotsync_io::prelude::{
    DriverConfig,
    EgressPool,
    IoBridge,
    IoBridgeHandle,
    IoDriverComponent,
    UdpPort,
};
#[cfg(any(test, feature = "test-support"))]
use flotsync_io::test_support::{
    ReservedSocketKind,
    ReservedSocketLease,
    enable_bind_reuse_address,
    reserve_sockets,
    set_test_system_label,
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

mod config;
mod discovery;
mod local_endpoint;
mod topology;

#[cfg(any(test, feature = "test-support"))]
mod test_ext;

use config::*;
pub(crate) use config::{RuntimeControlError, RuntimeHostError};
use discovery::PreconfiguredPeerRoutesConfig;
#[cfg(test)]
pub(super) use discovery::PreconfiguredPeerRoutesPublishMode;
use local_endpoint::LocalEndpointManager;
use topology::*;

#[cfg(any(test, feature = "test-support"))]
pub(crate) use test_ext::DeliveryRuntimeHostTestExt;

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

/// runtime component as a normal topology node in the current runtime.
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
        config: ReplicationConfig,
        security: DeliverySecurity,
        runtime_config_toml: Option<&str>,
    ) -> Result<Self, RuntimeHostError> {
        Self::start_with_options(
            local_member,
            store,
            listener,
            config,
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
        config: ReplicationConfig,
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
                config,
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
        config: ReplicationConfig,
        security: DeliverySecurity,
        runtime_config_toml: Option<&str>,
        route_publish_mode: PreconfiguredPeerRoutesPublishMode,
    ) -> Result<Self, RuntimeHostError> {
        Self::start_with_options(
            local_member,
            store,
            listener,
            config,
            security,
            runtime_config_toml,
            route_publish_mode,
        )
        .await
    }

    pub(crate) async fn shutdown(&mut self) -> Result<(), RuntimeHostError> {
        let Some(topology) = self.topology.take() else {
            return Ok(());
        };
        let Some(system) = self.system.take() else {
            return Ok(());
        };
        let stop_result = topology.stop_all(&system, self.control_timeout).await;
        drop(topology);
        if let Err(error) = stop_result {
            system.shutdown_async();
            #[cfg(any(test, feature = "test-support"))]
            rebind_reserved_runtime_local_endpoint_binding(&mut self.local_endpoint_lease);
            return Err(error);
        }
        system
            .shutdown()
            .map(|result| result.boxed().context(ControlFutureSnafu))
            .timeout_fold_err(self.control_timeout)
            .await
            .context(ShutdownSystemSnafu)?;
        #[cfg(any(test, feature = "test-support"))]
        rebind_reserved_runtime_local_endpoint_binding(&mut self.local_endpoint_lease);
        Ok(())
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
        drop(self.topology.take());
        let Some(system) = self.system.take() else {
            return;
        };
        log::warn!(
            "replication runtime host dropped without graceful shutdown; call ReplicationApi::shutdown().await before dropping the runtime handle"
        );
        system.shutdown_async();
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
