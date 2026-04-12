#[cfg(test)]
use crate::delivery::test_support::FULL_STACK_WAIT_TIMEOUT;
use crate::{
    GroupMemberships,
    SharedGroupMemberships,
    api::MemberIdentity,
    delivery::{
        contracts::{
            GroupBroadcastPort,
            GroupBroadcastPortIndication,
            GroupBroadcastPortRequest,
            ReliableDeliveryPort,
            ReliableDeliveryPortIndication,
            ReliableDeliveryPortRequest,
        },
        group_broadcast::{GroupBroadcastComponent, GroupBroadcastSubmit},
        ingress::{DeliveryIngressComponent, DeliveryInterestConfig},
        reliable_delivery::{ReliableDeliveryComponent, ReliableDeliverySubmit},
        route_transport::{
            DiscoveryRouteUpdate,
            RouteDiscoveryPort,
            RouteTransportPort,
            TransportRouteKey,
            manager::RouteTransportManager,
        },
    },
};
#[cfg(test)]
use flotsync_io::test_support::eventually_component_state;
use flotsync_io::{
    config_keys::BIND_REUSE_ADDRESS,
    prelude::{
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
    },
};
use flotsync_udpour::{ReceiverConfig, SenderConfig, UDPourConfig};
use flotsync_utils::{LocalActor, impl_local_actor};
use kompact::prelude::*;
use snafu::prelude::*;
#[cfg(test)]
use std::net::{Ipv4Addr, Ipv6Addr};
use std::{
    collections::{HashSet, VecDeque},
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

/// Startup and shutdown failures for the internal delivery runtime host.
#[derive(Debug, Snafu)]
pub(crate) enum RuntimeHostError {
    #[snafu(display("Failed to build Kompact system for the replication runtime: {message}"))]
    BuildSystem { message: String },
    #[snafu(display("Failed to start runtime component '{component}': {message}"))]
    StartComponent {
        component: &'static str,
        message: String,
    },
    #[snafu(display("Failed to connect runtime bridge UDP to route transport: {message}"))]
    ConnectUdp { message: String },
    #[snafu(display("Failed to bind the runtime external UDP socket: {message}"))]
    BindExternalUdp { message: String },
    #[snafu(display("Failed to connect runtime components for {link}: {message}"))]
    ConnectComponents { link: &'static str, message: String },
}

/// Live internal host for the delivery-layer components used by replication.
///
/// This owns the concrete Kompact/io graph and exposes a small imperative
/// surface that later replication logic can build on without knowing transport
/// internals.
pub(crate) struct DeliveryRuntimeHost {
    system: KompactSystem,
    driver: Arc<Component<IoDriverComponent>>,
    bridge: Arc<Component<IoBridge>>,
    manager: Arc<Component<RouteTransportManager>>,
    ingress: Arc<Component<DeliveryIngressComponent>>,
    group_broadcast: Arc<Component<GroupBroadcastComponent>>,
    reliable_delivery: Arc<Component<ReliableDeliveryComponent>>,
    discovery_source: Arc<Component<RuntimeDiscoverySource>>,
    group_client: Arc<Component<GroupBroadcastClientAdapter>>,
    reliable_client: Arc<Component<ReliableDeliveryClientAdapter>>,
    udp_observer: Arc<Component<RuntimeUdpObserver>>,
    group_indications: IndicationQueue<GroupBroadcastPortIndication>,
    reliable_indications: IndicationQueue<ReliableDeliveryPortIndication>,
    group_memberships: SharedGroupMemberships,
    #[cfg_attr(not(test), allow(dead_code))]
    external_udp_addr: SocketAddr,
}

impl DeliveryRuntimeHost {
    /// Start one new delivery runtime host for a single local member.
    pub(crate) fn new(
        local_member: MemberIdentity,
        external_udp_bind_addr: SocketAddr,
    ) -> Result<Self, RuntimeHostError> {
        let system = build_runtime_system()?;
        let group_memberships = SharedGroupMemberships::default();
        let driver = system.create(|| IoDriverComponent::new(DriverConfig::default()));
        let driver_for_bridge = driver.clone();
        let bridge = system.create(move || IoBridge::new(&driver_for_bridge));
        let bridge_handle = IoBridgeHandle::from_component(&bridge);
        let manager_system = system.clone();
        let manager = system.create(move || {
            RouteTransportManager::new(manager_system, bridge_handle, default_udpour_config())
        });
        let manager_ref = manager
            .actor_ref()
            .hold()
            .expect("route transport manager must expose a strong actor ref");
        let ingress_memberships = group_memberships.clone();
        let ingress = system.create(move || {
            DeliveryIngressComponent::new(DeliveryInterestConfig {
                group_memberships: ingress_memberships,
                local_members: Arc::new([local_member.clone()].into_iter().collect()),
                hosted_mailboxes: Arc::new(HashSet::new()),
            })
        });
        let broadcast_memberships = group_memberships.clone();
        let broadcast_manager_ref = manager_ref.clone();
        let reliable_manager_ref = manager_ref.clone();
        let group_broadcast = system.create(move || {
            GroupBroadcastComponent::new(broadcast_memberships, broadcast_manager_ref)
        });
        let reliable_delivery =
            system.create(move || ReliableDeliveryComponent::new(reliable_manager_ref));
        let discovery_source = system.create(RuntimeDiscoverySource::new);
        let group_indications = IndicationQueue::default();
        let reliable_indications = IndicationQueue::default();
        let udp_indications = IndicationQueue::default();
        let group_client_queue = group_indications.clone();
        let reliable_client_queue = reliable_indications.clone();
        let udp_observer_queue = udp_indications.clone();
        let group_client =
            system.create(move || GroupBroadcastClientAdapter::new(group_client_queue));
        let reliable_client =
            system.create(move || ReliableDeliveryClientAdapter::new(reliable_client_queue));
        let udp_observer = system.create(move || RuntimeUdpObserver::new(udp_observer_queue));

        biconnect_components::<RouteTransportPort<TransportRouteKey>, _, _>(&manager, &ingress)
            .map_err(|error| RuntimeHostError::ConnectComponents {
                link: "route transport -> ingress",
                message: format!("{error:?}"),
            })?;
        biconnect_components::<
            crate::delivery::group_broadcast::GroupBroadcastInboundPort<TransportRouteKey>,
            _,
            _,
        >(&ingress, &group_broadcast)
        .map_err(|error| RuntimeHostError::ConnectComponents {
            link: "ingress -> group broadcast",
            message: format!("{error:?}"),
        })?;
        biconnect_components::<
            crate::delivery::reliable_delivery::ReliableDeliveryInboundPort<TransportRouteKey>,
            _,
            _,
        >(&ingress, &reliable_delivery)
        .map_err(|error| RuntimeHostError::ConnectComponents {
            link: "ingress -> reliable delivery",
            message: format!("{error:?}"),
        })?;
        biconnect_components::<RouteDiscoveryPort<TransportRouteKey>, _, _>(
            &discovery_source,
            &group_broadcast,
        )
        .map_err(|error| RuntimeHostError::ConnectComponents {
            link: "discovery -> group broadcast",
            message: format!("{error:?}"),
        })?;
        biconnect_components::<RouteDiscoveryPort<TransportRouteKey>, _, _>(
            &discovery_source,
            &reliable_delivery,
        )
        .map_err(|error| RuntimeHostError::ConnectComponents {
            link: "discovery -> reliable delivery",
            message: format!("{error:?}"),
        })?;
        biconnect_components::<GroupBroadcastPort, _, _>(&group_broadcast, &group_client).map_err(
            |error| RuntimeHostError::ConnectComponents {
                link: "group broadcast -> runtime client",
                message: format!("{error:?}"),
            },
        )?;
        biconnect_components::<ReliableDeliveryPort, _, _>(&reliable_delivery, &reliable_client)
            .map_err(|error| RuntimeHostError::ConnectComponents {
                link: "reliable delivery -> runtime client",
                message: format!("{error:?}"),
            })?;

        start_component(&system, &driver, "io_driver")?;
        start_component(&system, &bridge, "io_bridge")?;
        let udp_connect_handle = IoBridgeHandle::from_component(&bridge);
        block_on(udp_connect_handle.connect_udp(&manager)).map_err(|error| {
            RuntimeHostError::ConnectUdp {
                message: format!("{error:?}"),
            }
        })?;
        block_on(udp_connect_handle.connect_udp(&udp_observer)).map_err(|error| {
            RuntimeHostError::ConnectUdp {
                message: format!("{error:?}"),
            }
        })?;
        start_component(&system, &manager, "route_transport")?;
        start_component(&system, &ingress, "delivery_ingress")?;
        start_component(&system, &group_broadcast, "group_broadcast")?;
        start_component(&system, &reliable_delivery, "reliable_delivery")?;
        start_component(&system, &discovery_source, "discovery_source")?;
        start_component(&system, &group_client, "group_client")?;
        start_component(&system, &reliable_client, "reliable_client")?;
        start_component(&system, &udp_observer, "udp_observer")?;
        let (_external_udp_socket_id, external_udp_addr) =
            bind_external_udp_socket(&udp_observer, &udp_indications, external_udp_bind_addr)?;

        Ok(Self {
            system,
            driver,
            bridge,
            manager,
            ingress,
            group_broadcast,
            reliable_delivery,
            discovery_source,
            group_client,
            reliable_client,
            udp_observer,
            group_indications,
            reliable_indications,
            group_memberships,
            external_udp_addr,
        })
    }

    /// Replace the authoritative shared group-membership snapshot.
    pub(crate) fn replace_group_memberships(&self, memberships: GroupMemberships) {
        self.group_memberships.replace(memberships);
    }

    /// Publish one route-discovery update into both semantic delivery owners.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn publish_route_update(&self, update: DiscoveryRouteUpdate<TransportRouteKey>) {
        self.discovery_source.on_definition(|component| {
            component.discovery.trigger(update);
        });
    }

    /// Return the concrete local UDP socket address currently bound by this
    /// host for externally reachable delivery traffic.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn external_udp_bind_addr(&self) -> SocketAddr {
        self.external_udp_addr
    }

    /// Submit one group-scoped delivery request into `GroupBroadcast`.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn submit_group_broadcast(&self, submit: GroupBroadcastSubmit) {
        self.group_client.on_definition(|component| {
            component
                .delivery
                .trigger(GroupBroadcastPortRequest::Submit(submit));
        });
    }

    /// Submit one recipient-scoped delivery request into `ReliableDelivery`.
    pub(crate) fn submit_reliable_delivery(&self, submit: ReliableDeliverySubmit) {
        self.reliable_client.on_definition(|component| {
            component
                .delivery
                .trigger(ReliableDeliveryPortRequest::Submit(submit));
        });
    }

    /// Pop the next observed group-broadcast indication when one is queued.
    pub(crate) fn pop_group_indication(&self) -> Option<GroupBroadcastPortIndication> {
        self.group_indications.pop()
    }

    /// Pop the next observed reliable-delivery indication when one is queued.
    pub(crate) fn pop_reliable_indication(&self) -> Option<ReliableDeliveryPortIndication> {
        self.reliable_indications.pop()
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
        let route = crate::delivery::route_transport::SendRouteCandidate {
            coverage_key: TransportRouteKey::Udp(crate::delivery::route_transport::UdpRouteKey {
                remote_addr,
                scope: crate::delivery::route_transport::DatagramRouteScope::Unicast,
                local_bind: Some(self.external_udp_addr),
            }),
            sharing: crate::delivery::route_transport::RouteSharingKind::Exclusive,
            preference_rank: crate::delivery::route_transport::RoutePreferenceRank::new(1),
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
        let broadcast_peer = peer.clone();
        eventually_component_state(
            FULL_STACK_WAIT_TIMEOUT,
            &self.group_broadcast,
            |component| component.knows_direct_route(&broadcast_peer),
            format_args!("timed out waiting for group-broadcast route publication for peer={peer}"),
        );

        let reliable_peer = peer.clone();
        eventually_component_state(
            FULL_STACK_WAIT_TIMEOUT,
            &self.reliable_delivery,
            |component| component.knows_direct_route(&reliable_peer),
            format_args!(
                "timed out waiting for reliable-delivery route publication for peer={peer}"
            ),
        );
    }
}

impl Drop for DeliveryRuntimeHost {
    fn drop(&mut self) {
        const CONTROL_TIMEOUT: Duration = Duration::from_secs(5);

        let _ = self
            .system
            .kill_notify(self.udp_observer.clone())
            .wait_timeout(CONTROL_TIMEOUT);
        let _ = self
            .system
            .kill_notify(self.reliable_client.clone())
            .wait_timeout(CONTROL_TIMEOUT);
        let _ = self
            .system
            .kill_notify(self.group_client.clone())
            .wait_timeout(CONTROL_TIMEOUT);
        let _ = self
            .system
            .kill_notify(self.discovery_source.clone())
            .wait_timeout(CONTROL_TIMEOUT);
        let _ = self
            .system
            .kill_notify(self.reliable_delivery.clone())
            .wait_timeout(CONTROL_TIMEOUT);
        let _ = self
            .system
            .kill_notify(self.group_broadcast.clone())
            .wait_timeout(CONTROL_TIMEOUT);
        let _ = self
            .system
            .kill_notify(self.ingress.clone())
            .wait_timeout(CONTROL_TIMEOUT);
        let _ = self
            .system
            .kill_notify(self.manager.clone())
            .wait_timeout(CONTROL_TIMEOUT);
        let _ = self
            .system
            .kill_notify(self.bridge.clone())
            .wait_timeout(CONTROL_TIMEOUT);
        let _ = self
            .system
            .kill_notify(self.driver.clone())
            .wait_timeout(CONTROL_TIMEOUT);
        let _ = self.system.clone().shutdown();
    }
}

/// Shared queue used by the runtime host's semantic delivery adapters.
#[derive(Clone)]
struct IndicationQueue<T> {
    inner: Arc<Mutex<VecDeque<T>>>,
}

impl<T> Default for IndicationQueue<T> {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}

impl<T> IndicationQueue<T> {
    fn push(&self, value: T) {
        self.inner
            .lock()
            .expect("runtime indication queue mutex must not be poisoned")
            .push_back(value);
    }

    fn pop(&self) -> Option<T> {
        self.inner
            .lock()
            .expect("runtime indication queue mutex must not be poisoned")
            .pop_front()
    }
}

fn bind_external_udp_socket(
    udp_observer: &Arc<Component<RuntimeUdpObserver>>,
    udp_indications: &IndicationQueue<UdpIndication>,
    external_udp_bind_addr: SocketAddr,
) -> Result<(SocketId, SocketAddr), RuntimeHostError> {
    let request_id = UdpOpenRequestId::new();
    udp_observer.on_definition(|component| {
        component.udp.trigger(UdpRequest::Bind {
            request_id,
            bind: UdpLocalBind::Exact(external_udp_bind_addr),
        });
    });

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let Some(indication) = udp_indications.pop() else {
            if Instant::now() >= deadline {
                return BindExternalUdpSnafu {
                    message: "timed out waiting for UDP bind confirmation".to_owned(),
                }
                .fail();
            }
            std::thread::sleep(Duration::from_millis(10));
            continue;
        };

        match indication {
            UdpIndication::Bound {
                request_id: indicated_request_id,
                socket_id,
                local_addr,
            } if indicated_request_id == request_id => return Ok((socket_id, local_addr)),
            UdpIndication::BindFailed {
                request_id: indicated_request_id,
                local_addr,
                reason,
            } if indicated_request_id == request_id => {
                return BindExternalUdpSnafu {
                    message: format!("bind at {local_addr} failed: {reason:?}"),
                }
                .fail();
            }
            _ => {}
        }
    }
}

#[cfg(test)]
fn loopback_advertise_addr(bind_addr: SocketAddr) -> SocketAddr {
    if !bind_addr.ip().is_unspecified() {
        return bind_addr;
    }

    match bind_addr {
        SocketAddr::V4(addr) => SocketAddr::from((Ipv4Addr::LOCALHOST, addr.port())),
        SocketAddr::V6(addr) => SocketAddr::from((Ipv6Addr::LOCALHOST, addr.port())),
    }
}

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

impl Actor for RuntimeDiscoverySource {
    type Message = Never;

    fn receive_local(&mut self, _msg: Self::Message) -> Handled {
        unreachable!("Never type is empty")
    }

    fn receive_network(&mut self, _msg: NetMessage) -> Handled {
        unreachable!("runtime discovery source does not use network actor messages")
    }
}

/// Temporary side-channel for the externally owned UDP bind path.
///
/// This remains connected after startup in the first replication slice, so
/// inbound `UdpIndication`s can accumulate in the shared queue. See
/// `flotsync-36j` for removing or draining this observer after bind setup.
#[derive(ComponentDefinition)]
struct RuntimeUdpObserver {
    ctx: ComponentContext<Self>,
    udp: RequiredPort<UdpPort>,
    queue: IndicationQueue<UdpIndication>,
}

impl RuntimeUdpObserver {
    fn new(queue: IndicationQueue<UdpIndication>) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            udp: RequiredPort::uninitialised(),
            queue,
        }
    }
}

ignore_lifecycle!(RuntimeUdpObserver);

impl Require<UdpPort> for RuntimeUdpObserver {
    fn handle(&mut self, indication: UdpIndication) -> Handled {
        self.queue.push(indication);
        Handled::Ok
    }
}

impl LocalActor for RuntimeUdpObserver {
    type Message = Never;

    fn receive(&mut self, _msg: Self::Message) -> Handled {
        unreachable!("runtime UDP observer does not use local actor messages")
    }
}

impl_local_actor!(RuntimeUdpObserver);

#[derive(ComponentDefinition)]
struct GroupBroadcastClientAdapter {
    ctx: ComponentContext<Self>,
    delivery: RequiredPort<GroupBroadcastPort>,
    queue: IndicationQueue<GroupBroadcastPortIndication>,
}

impl GroupBroadcastClientAdapter {
    fn new(queue: IndicationQueue<GroupBroadcastPortIndication>) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            delivery: RequiredPort::uninitialised(),
            queue,
        }
    }
}

ignore_lifecycle!(GroupBroadcastClientAdapter);

impl Require<GroupBroadcastPort> for GroupBroadcastClientAdapter {
    fn handle(&mut self, indication: GroupBroadcastPortIndication) -> Handled {
        self.queue.push(indication);
        Handled::Ok
    }
}

impl LocalActor for GroupBroadcastClientAdapter {
    type Message = Never;

    fn receive(&mut self, _msg: Self::Message) -> Handled {
        unreachable!("runtime group client adapter does not use local actor messages")
    }
}

impl_local_actor!(GroupBroadcastClientAdapter);

#[derive(ComponentDefinition)]
struct ReliableDeliveryClientAdapter {
    ctx: ComponentContext<Self>,
    delivery: RequiredPort<ReliableDeliveryPort>,
    queue: IndicationQueue<ReliableDeliveryPortIndication>,
}

impl ReliableDeliveryClientAdapter {
    fn new(queue: IndicationQueue<ReliableDeliveryPortIndication>) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            delivery: RequiredPort::uninitialised(),
            queue,
        }
    }
}

ignore_lifecycle!(ReliableDeliveryClientAdapter);

impl Require<ReliableDeliveryPort> for ReliableDeliveryClientAdapter {
    fn handle(&mut self, indication: ReliableDeliveryPortIndication) -> Handled {
        self.queue.push(indication);
        Handled::Ok
    }
}

impl LocalActor for ReliableDeliveryClientAdapter {
    type Message = Never;

    fn receive(&mut self, _msg: Self::Message) -> Handled {
        unreachable!("runtime reliable client adapter does not use local actor messages")
    }
}

impl_local_actor!(ReliableDeliveryClientAdapter);

fn build_runtime_system() -> Result<KompactSystem, RuntimeHostError> {
    let mut config = KompactConfig::default();
    config.set_config_value(&BIND_REUSE_ADDRESS, true);
    config.load_config_str("flotsync.route-transport.udp-activation-policy = 1");
    config
        .build()
        .map_err(|error| RuntimeHostError::BuildSystem {
            message: format!("{error}"),
        })
}

fn start_component<C>(
    system: &KompactSystem,
    component: &Arc<Component<C>>,
    name: &'static str,
) -> Result<(), RuntimeHostError>
where
    C: ComponentDefinition + ComponentLifecycle + Sized + 'static,
{
    system
        .start_notify(component)
        .wait_timeout(Duration::from_secs(5))
        .map_err(|error| RuntimeHostError::StartComponent {
            component: name,
            message: format!("{error:?}"),
        })
}

fn default_udpour_config() -> UDPourConfig {
    let sender = SenderConfig {
        max_part_payload_len: std::num::NonZeroUsize::new(1024)
            .expect("runtime UDPour config must use a non-zero part payload length"),
        retention_timeout: Duration::from_secs(1),
        id_reuse_cooldown: Duration::from_millis(100),
        eager_ack_cleanup: false,
    };
    let receiver = ReceiverConfig {
        max_need_parts_frame_len: 1024,
        repair_interval: Duration::from_millis(100),
        give_up_timeout: Duration::from_secs(1),
        delivered_tombstone_timeout: Duration::ZERO,
    };
    UDPourConfig::new(sender, receiver).expect("runtime host must use a valid UDPour config")
}
