//! Component topology construction for the delivery runtime host.

use super::*;

trait RuntimeLifecycleComponent: Send + Sync {
    fn start<'a>(
        &'a self,
        system: &'a KompactSystem,
        control_timeout: Duration,
    ) -> BoxFuture<'a, Result<(), RuntimeHostError>>;

    fn stop<'a>(
        &'a self,
        system: &'a KompactSystem,
        control_timeout: Duration,
    ) -> BoxFuture<'a, Result<(), RuntimeHostError>>;
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

    /// Stop every lifecycle node in reverse dependency order.
    async fn stop_all(
        &self,
        system: &KompactSystem,
        control_timeout: Duration,
    ) -> Result<(), RuntimeHostError> {
        // Stop sequentially because later-started components depend on
        // earlier-started providers during shutdown.
        for component in self.nodes().rev() {
            component.stop(system, control_timeout).await?;
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

    fn stop<'a>(
        &'a self,
        system: &'a KompactSystem,
        control_timeout: Duration,
    ) -> BoxFuture<'a, Result<(), RuntimeHostError>> {
        async move {
            // Faulty and destroyed components cannot process lifecycle messages.
            // The enclosing system shutdown remains responsible for final cleanup.
            if self.is_faulty() || self.is_destroyed() {
                return Ok(());
            }
            system
                .stop_notify(self)
                .map(|result| result.boxed().context(ControlFutureSnafu))
                .timeout_fold_err(control_timeout)
                .await
                .context(StopComponentSnafu {
                    component: C::type_name(),
                })
        }
        .boxed()
    }
}

pub(in crate::runtime::host) struct BuiltRuntimeSystem {
    pub(in crate::runtime::host) system: KompactSystem,
    #[cfg(any(test, feature = "test-support"))]
    pub(in crate::runtime::host) local_endpoint_lease: ReservedSocketLease,
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
pub(in crate::runtime::host) struct IoTopology {
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
pub(in crate::runtime::host) struct TransportTopology {
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
pub(in crate::runtime::host) struct DeliveryTopology {
    pub(in crate::runtime::host) ingress: Arc<Component<DeliveryIngressComponent>>,
    pub(in crate::runtime::host) group_broadcast: Arc<Component<GroupBroadcastComponent>>,
    pub(in crate::runtime::host) reliable_delivery: Arc<Component<ReliableDeliveryComponent>>,
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
pub(in crate::runtime::host) struct DiscoveryTopology {
    peer_announcement: Arc<Component<PeerAnnouncementComponent>>,
    peer_announcement_observation: Arc<Component<PeerAnnouncementObservationComponent>>,
    route_establishment: Arc<Component<RouteEstablishmentComponent>>,
    key_material_discovery: Arc<Component<KeyMaterialDiscoveryComponent>>,
    #[cfg(any(test, feature = "test-support"))]
    manual_route_discovery: Arc<Component<PortTesterComponent<ManualRouteDiscoveryPort>>>,
    #[cfg(any(test, feature = "test-support"))]
    manual_route_discovery_ref: ActorRef<PortTestMsg<ManualRouteDiscoveryPort>>,
    pub(in crate::runtime::host) local_endpoint_manager: Arc<Component<LocalEndpointManager>>,
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

    pub(in crate::runtime::host) async fn configure_route_establishment_watches(
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
    pub(in crate::runtime::host) fn publish_route_update(
        &self,
        update: flotsync_routes::DiscoveryRouteUpdate<TransportRouteKey>,
    ) {
        self.manual_route_discovery_ref.inject_indication(update);
    }

    #[cfg(test)]
    pub(in crate::runtime::host) fn publish_preconfigured_peer_routes(
        &self,
        local_endpoint: SocketAddr,
    ) {
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
pub(in crate::runtime::host) struct RuntimeLogicTopology {
    catch_up_manager: Arc<Component<CatchUpManagerComponent>>,
    summary_request_manager: Arc<Component<SummaryRequestManagerComponent>>,
    pub(in crate::runtime::host) runtime_component: Arc<Component<ReplicationRuntimeComponent>>,
}

/// Runtime-logic knobs that are not application or identity dependencies.
struct RuntimeLogicSettings {
    summary_request_timeout: Duration,
}

/// Grouped inputs for the runtime logic topology.
struct RuntimeLogicTopologyInput {
    identity: RuntimeIdentityContext,
    services: RuntimeApplicationServices,
    security: RuntimeSecurityContext,
    settings: RuntimeLogicSettings,
}

impl RuntimeLogicTopology {
    fn build(system: &KompactSystem, input: RuntimeLogicTopologyInput) -> Self {
        let catch_up_manager = CatchUpManagerComponent::new(
            input.identity.local_member.clone(),
            input.identity.group_memberships.clone(),
            input.services.store.clone(),
        );
        let catch_up_manager = system.create(move || catch_up_manager);
        let catch_up_manager_ref = catch_up_manager
            .actor_ref()
            .hold()
            .expect("catch-up manager must expose a strong actor ref");
        let summary_request_manager = SummaryRequestManagerComponent::new(
            input.identity.local_member.clone(),
            input.identity.group_memberships.clone(),
            input.settings.summary_request_timeout,
        );
        let summary_request_manager = system.create(move || summary_request_manager);
        let summary_request_manager_ref = summary_request_manager
            .actor_ref()
            .hold()
            .expect("summary request manager must expose a strong actor ref");
        let runtime_component = ReplicationRuntimeComponent::new(
            input.identity,
            input.services,
            input.security,
            RuntimeComponentActors {
                summary_request_manager: summary_request_manager_ref,
                catch_up_manager: catch_up_manager_ref,
            },
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
pub(in crate::runtime::host) struct RuntimeTopology {
    pub(in crate::runtime::host) io: IoTopology,
    pub(in crate::runtime::host) transport: TransportTopology,
    pub(in crate::runtime::host) delivery: DeliveryTopology,
    pub(in crate::runtime::host) discovery: DiscoveryTopology,
    pub(in crate::runtime::host) runtime: RuntimeLogicTopology,
}

/// Inputs needed to assemble a full runtime topology.
pub(in crate::runtime::host) struct RuntimeTopologyBuildInput {
    pub(in crate::runtime::host) group_memberships: SharedGroupMemberships,
    pub(in crate::runtime::host) local_member: MemberIdentity,
    pub(in crate::runtime::host) store: Arc<dyn ReplicationStore>,
    pub(in crate::runtime::host) listener: Arc<dyn ReplicationEventListener>,
    pub(in crate::runtime::host) config: ReplicationConfig,
    pub(in crate::runtime::host) security: DeliverySecurity,
    pub(in crate::runtime::host) host_config: DeliveryRuntimeHostConfig,
    pub(in crate::runtime::host) static_route_hints: PreconfiguredPeerRoutesConfig,
}

impl RuntimeTopology {
    pub(in crate::runtime::host) fn build(
        system: &KompactSystem,
        input: RuntimeTopologyBuildInput,
    ) -> Self {
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
        let identity = RuntimeIdentityContext {
            local_member: input.local_member.clone(),
            group_memberships: input.group_memberships.clone(),
        };
        let services = RuntimeApplicationServices {
            store: input.store,
            listener: input.listener,
            config: input.config,
        };
        let security = RuntimeSecurityContext {
            security: input.security.clone(),
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
            RuntimeLogicTopologyInput {
                identity,
                services,
                security,
                settings: RuntimeLogicSettings {
                    summary_request_timeout: input.host_config.summary_request_timeout,
                },
            },
        );

        Self {
            io,
            transport,
            delivery,
            discovery,
            runtime,
        }
    }

    pub(in crate::runtime::host) fn connect_all(&self) -> Result<(), RuntimeHostError> {
        self.delivery.connect_transport(&self.transport)?;
        self.discovery.connect_transport(&self.transport)?;
        self.delivery.connect_internal_routes()?;
        self.discovery.connect_internal_routes()?;
        self.delivery.connect_discovery(&self.discovery)?;
        self.runtime.connect_delivery(&self.delivery)
    }

    pub(in crate::runtime::host) async fn start_all(
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

    pub(in crate::runtime::host) async fn stop_all(
        &self,
        system: &KompactSystem,
        control_timeout: Duration,
    ) -> Result<(), RuntimeHostError> {
        self.runtime.stop_all(system, control_timeout).await?;
        self.discovery.stop_all(system, control_timeout).await?;
        self.delivery.stop_all(system, control_timeout).await?;
        self.transport.stop_all(system, control_timeout).await?;
        self.io.stop_all(system, control_timeout).await?;
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
