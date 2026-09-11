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
    group_state::SharedGroupState,
    summary_request_manager::SummaryRequestManagerComponent,
};
#[cfg(test)]
use super::{ReplicationRuntimeMessage, handle::wait_for_test_reply};
#[cfg(test)]
use crate::delivery::contracts::GroupBroadcastPortIndication;
use crate::{
    api::{
        BoxError,
        ListenerError,
        ReplicationConfig,
        ReplicationEvent,
        ReplicationEventListener,
        ReplicationGroupSnapshot,
        ReplicationStore,
    },
    delivery::{
        contracts::{GroupBroadcastPort, ReliableDeliveryPort, ReliableDeliveryStore},
        group_broadcast::{GroupBroadcastComponent, GroupBroadcastInboundPort},
        ingress::{DeliveryIngressComponent, DeliveryInterestConfig},
        reliable_delivery::{ReliableDeliveryComponent, ReliableDeliveryInboundPort},
        security::DeliverySecurity,
    },
};
use flotsync_core::{MemberIdentity, membership::SharedGroupMemberships};
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

#[allow(
    clippy::wildcard_imports,
    reason = "The host facade owns and reuses the local configuration implementation vocabulary."
)]
use config::*;
pub(crate) use config::{RuntimeControlError, RuntimeHostError};
use discovery::PreconfiguredPeerRoutesConfig;
#[cfg(test)]
pub(super) use discovery::PreconfiguredPeerRoutesPublishMode;
use local_endpoint::LocalEndpointManager;
#[cfg(test)]
pub(in crate::runtime) use topology::RuntimeHostTestSupport;
#[allow(
    clippy::wildcard_imports,
    reason = "The host facade owns and reuses the local topology implementation vocabulary."
)]
use topology::*;

#[cfg(test)]
pub(crate) use test_ext::DeliveryRuntimeHostTestExt;
#[cfg(any(test, feature = "test-support"))]
pub(crate) use test_ext::DeliveryRuntimeHostTestSupportExt;

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

/// Behaviour if the inactive runtime component unexpectedly invokes its placeholder listener.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum StartupEventPolicy {
    /// Report and discard the unexpected event so production failure remains observable.
    Log,
    /// Panic immediately so lifecycle tests detect an invalid startup ordering.
    #[allow(
        dead_code,
        reason = "the panic policy is retained for focused staged-startup lifecycle tests"
    )]
    Panic,
}

impl StartupEventPolicy {
    /// Build a staged-startup listener with this unexpected-event policy.
    pub(super) fn create_listener(self) -> Arc<dyn ReplicationEventListener> {
        Arc::new(StartupEventListener::new(self))
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
    group_memberships: Arc<SharedGroupState>,
    control_timeout: Duration,
    #[cfg_attr(not(any(test, feature = "test-support")), allow(dead_code))]
    external_udp_addr: SocketAddr,
    #[cfg(any(test, feature = "test-support"))]
    local_endpoint_lease: ReservedSocketLease,
    /// Unit-test-only behaviour and components kept outside the production topology.
    #[cfg(test)]
    test_support: RuntimeHostTestSupport,
}

impl DeliveryRuntimeHost {
    fn new(
        system: KompactSystem,
        topology: RuntimeTopology,
        group_memberships: Arc<SharedGroupState>,
        control_timeout: Duration,
        external_udp_addr: SocketAddr,
        #[cfg(any(test, feature = "test-support"))] local_endpoint_lease: ReservedSocketLease,
        #[cfg(test)] test_support: RuntimeHostTestSupport,
    ) -> Self {
        Self {
            system: Some(system),
            topology: Some(topology),
            group_memberships,
            control_timeout,
            external_udp_addr,
            #[cfg(any(test, feature = "test-support"))]
            local_endpoint_lease,
            #[cfg(test)]
            test_support,
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

    /// Return the route-establishment component that owns peer-route diagnostic state.
    pub(crate) fn route_establishment_component(
        &self,
    ) -> &Arc<Component<RouteEstablishmentComponent>> {
        self.topology().discovery.route_discovery_provider()
    }

    /// Start one new active delivery runtime host with an additional in-memory TOML
    /// config fragment merged into the Kompact runtime config.
    ///
    /// This compatibility path drains no application state. New application
    /// loading uses [`Self::prepare_with_runtime_config_toml`] and activates the
    /// runtime only after synchronisation.
    #[cfg(test)]
    pub(super) async fn start_with_runtime_config_toml(
        local_member: &MemberIdentity,
        group_memberships: Arc<SharedGroupState>,
        store: Arc<dyn ReplicationStore>,
        listener: Arc<dyn ReplicationEventListener>,
        config: ReplicationConfig,
        security: DeliverySecurity,
        runtime_config_toml: Option<&str>,
    ) -> Result<Self, RuntimeHostError> {
        let host = Self::prepare_with_options(
            local_member,
            group_memberships,
            store,
            config,
            security,
            runtime_config_toml,
            StartupEventPolicy::Log.create_listener(),
            #[cfg(test)]
            RuntimeHostTestSupport::direct(),
        )
        .await?;
        host.activate_runtime(listener).await?;
        Ok(host)
    }

    /// Prepare networking and delivery with an explicit inactive listener.
    pub(super) async fn prepare_with_runtime_config_toml(
        local_member: &MemberIdentity,
        group_memberships: Arc<SharedGroupState>,
        store: Arc<dyn ReplicationStore>,
        config: ReplicationConfig,
        security: DeliverySecurity,
        runtime_config_toml: Option<&str>,
        startup_listener: Arc<dyn ReplicationEventListener>,
    ) -> Result<Self, RuntimeHostError> {
        Self::prepare_with_options(
            local_member,
            group_memberships,
            store,
            config,
            security,
            runtime_config_toml,
            startup_listener,
            #[cfg(test)]
            RuntimeHostTestSupport::direct(),
        )
        .await
    }

    /// Prepare a staged host with a caller-selected unit-test support payload.
    ///
    /// Ordinary preparation constructs direct production-equivalent support internally. This
    /// special case exists so focused lifecycle tests can opt into additional host seams without
    /// adding test concerns to the production preparation signature.
    #[cfg(test)]
    #[allow(
        clippy::too_many_arguments,
        reason = "This focused test boundary mirrors the production preparation inputs and adds one isolated test-support payload."
    )]
    pub(super) async fn prepare_with_test_support(
        local_member: &MemberIdentity,
        group_memberships: Arc<SharedGroupState>,
        store: Arc<dyn ReplicationStore>,
        config: ReplicationConfig,
        security: DeliverySecurity,
        runtime_config_toml: Option<&str>,
        startup_listener: Arc<dyn ReplicationEventListener>,
        test_support: RuntimeHostTestSupport,
    ) -> Result<Self, RuntimeHostError> {
        Self::prepare_with_options(
            local_member,
            group_memberships,
            store,
            config,
            security,
            runtime_config_toml,
            startup_listener,
            test_support,
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "This internal startup boundary keeps independently owned runtime services explicit; the eighth argument is a test-only support payload."
    )]
    async fn prepare_with_options(
        local_member: &MemberIdentity,
        group_memberships: Arc<SharedGroupState>,
        store: Arc<dyn ReplicationStore>,
        config: ReplicationConfig,
        security: DeliverySecurity,
        runtime_config_toml: Option<&str>,
        startup_listener: Arc<dyn ReplicationEventListener>,
        #[cfg(test)] test_support: RuntimeHostTestSupport,
    ) -> Result<Self, RuntimeHostError> {
        let built_system = build_runtime_system(runtime_config_toml).await?;
        let system = built_system.system.clone();
        #[cfg(test)]
        let test_support = test_support.materialise(&system);
        let host_config = DeliveryRuntimeHostConfig::from_system_config(&system)?;
        let routes_config = PreconfiguredPeerRoutesConfig::from_config(system.config())?;
        let topology = RuntimeTopology::build(
            &system,
            RuntimeTopologyBuildInput {
                group_memberships: group_memberships.clone(),
                local_member: local_member.clone(),
                store,
                listener: startup_listener,
                config,
                security,
                host_config,
                static_route_hints: routes_config,
            },
        );
        topology.connect_all(
            #[cfg(test)]
            &test_support,
        )?;
        topology
            .start_network(
                &system,
                host_config.control_timeout,
                #[cfg(test)]
                &test_support,
            )
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
        if test_support.route_publish_mode()
            == PreconfiguredPeerRoutesPublishMode::OnLocalEndpointBound
        {
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
            #[cfg(test)]
            test_support,
        ))
    }

    #[cfg(test)]
    #[allow(
        clippy::too_many_arguments,
        reason = "This test-only startup boundary adds explicit route publication control to the production runtime inputs."
    )]
    pub(super) async fn start_with_route_publish_mode_for_test(
        local_member: &MemberIdentity,
        group_memberships: Arc<SharedGroupState>,
        store: Arc<dyn ReplicationStore>,
        listener: Arc<dyn ReplicationEventListener>,
        config: ReplicationConfig,
        security: DeliverySecurity,
        runtime_config_toml: Option<&str>,
        route_publish_mode: PreconfiguredPeerRoutesPublishMode,
    ) -> Result<Self, RuntimeHostError> {
        let host = Self::prepare_with_options(
            local_member,
            group_memberships,
            store,
            config,
            security,
            runtime_config_toml,
            StartupEventPolicy::Log.create_listener(),
            RuntimeHostTestSupport::with_route_publish_mode(route_publish_mode),
        )
        .await?;
        host.activate_runtime(listener).await?;
        Ok(host)
    }

    /// Install the real listener and start every runtime-logic component.
    pub(crate) async fn activate_runtime(
        &self,
        listener: Arc<dyn ReplicationEventListener>,
    ) -> Result<(), RuntimeHostError> {
        let system = self
            .system
            .as_ref()
            .expect("delivery runtime host system must still be live");
        let runtime_component = self.runtime_component();
        assert!(
            !runtime_component.is_active(),
            "replication runtime listener must be installed before component start"
        );
        runtime_component.on_definition(|component| {
            component.replace_listener_before_start(listener);
        });
        self.topology()
            .start_runtime_logic(system, self.control_timeout)
            .await
    }

    pub(crate) async fn shutdown(&mut self) -> Result<(), RuntimeHostError> {
        let Some(topology) = self.topology.take() else {
            return Ok(());
        };
        let Some(system) = self.system.take() else {
            return Ok(());
        };
        let stop_result = topology
            .stop_all(
                &system,
                self.control_timeout,
                #[cfg(test)]
                &self.test_support,
            )
            .await;
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

    /// Publish one route-discovery update into every runtime route consumer.
    ///
    /// The manual provider lets tests and bring-up tooling inject direct routes
    /// explicitly alongside the production route-establishment provider.
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
    pub(crate) fn membership_snapshot(
        &self,
    ) -> Arc<dyn flotsync_core::membership::GroupMemberships> {
        self.group_memberships.snapshot()
    }

    /// Read the current restricted application group-state snapshot.
    pub(crate) fn group_state_snapshot(&self) -> Arc<dyn ReplicationGroupSnapshot> {
        self.group_memberships.application_snapshot()
    }
}

impl Drop for DeliveryRuntimeHost {
    fn drop(&mut self) {
        drop(self.topology.take());
        let Some(system) = self.system.take() else {
            return;
        };
        log::warn!(
            "replication runtime host dropped without graceful shutdown; call ReplicationApi::shutdown().await or ApplicationSynchronisation::shutdown().await before dropping its owning handle"
        );
        system.shutdown_async();
        #[cfg(any(test, feature = "test-support"))]
        rebind_reserved_runtime_local_endpoint_binding(&mut self.local_endpoint_lease);
    }
}

/// Non-optional listener installed until application startup synchronisation completes.
struct StartupEventListener {
    /// Observable response to an event emitted before real-listener installation.
    policy: StartupEventPolicy,
}

impl StartupEventListener {
    /// Build a placeholder with the selected event policy.
    fn new(policy: StartupEventPolicy) -> Self {
        Self { policy }
    }
}

impl ReplicationEventListener for StartupEventListener {
    fn on_event(&self, event: ReplicationEvent) -> BoxFuture<'_, Result<(), ListenerError>> {
        log::error!(
            "replication runtime emitted an event before startup synchronisation completed: {event:?}"
        );
        match self.policy {
            StartupEventPolicy::Log => futures_util::future::ready(Ok(())).boxed(),
            StartupEventPolicy::Panic => {
                panic!(
                    "replication runtime emitted an event before startup synchronisation completed"
                )
            }
        }
    }
}

/// Create the Kompact system without retaining its thread-local config state in this future.
async fn build_runtime_system(
    runtime_config_toml: Option<&str>,
) -> Result<BuiltRuntimeSystem, RuntimeHostError> {
    let runtime_config_toml = runtime_config_toml.map(str::to_owned);
    // KompactConfig contains Rc-backed builders and build() returns a local future. Keep both
    // entirely on the reusable blocking pool until Kompact provides a Send-compatible path:
    // https://github.com/kompics/kompact/issues/232
    blocking::unblock(move || build_runtime_system_blocking(runtime_config_toml.as_deref())).await
}

/// Build the test-support system synchronously on a blocking-pool worker.
#[cfg(any(test, feature = "test-support"))]
fn build_runtime_system_blocking(
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
    let system = config.build().wait().context(BuildSystemSnafu)?;
    Ok(BuiltRuntimeSystem {
        system,
        local_endpoint_lease,
    })
}

/// Build the production system synchronously on a blocking-pool worker.
#[cfg(not(any(test, feature = "test-support")))]
fn build_runtime_system_blocking(
    runtime_config_toml: Option<&str>,
) -> Result<BuiltRuntimeSystem, RuntimeHostError> {
    let mut config = KompactConfig::default();
    configure_replication_runtime(&mut config);
    if let Some(runtime_config_toml) = runtime_config_toml {
        config.load_config_str(runtime_config_toml);
    }
    let system = config.build().wait().context(BuildSystemSnafu)?;
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
