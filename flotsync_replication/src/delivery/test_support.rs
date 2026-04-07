//! Shared test-only helpers for replication delivery integration tests.
//!
//! This module centralises the common transport bring-up and synchronous wait
//! helpers used by the delivery-domain test harnesses. It intentionally stays
//! small: semantic-owner probes still live next to the tests that use them.

use super::route_transport::{
    RouteDiscoveryPort,
    RouteTransportActorMessage,
    TransportRouteKey,
    manager::RouteTransportManager,
};
use crate::api::MemberIdentity;
use flotsync_core::member::IdentifierBuf;
use flotsync_io::{
    prelude::{
        DriverConfig,
        IoBridge,
        IoBridgeHandle,
        IoDriverComponent,
        SocketId,
        UdpIndication,
        UdpLocalBind,
        UdpOpenRequestId,
        UdpRequest,
    },
    test_support::{
        BufferedReceiver,
        UdpObserver,
        WAIT_TIMEOUT,
        build_test_kompact_system_with,
        localhost,
        start_component,
    },
};
use flotsync_udpour::{ReceiverConfig, SenderConfig, UDPourConfig};
use kompact::prelude::*;
use std::{
    net::SocketAddr,
    num::NonZeroUsize,
    sync::{Arc, mpsc},
    time::Duration,
};

/// Longer timeout used by the semantic full-stack delivery tests.
pub(crate) const FULL_STACK_WAIT_TIMEOUT: Duration = Duration::from_secs(20);

/// Minimal test-only discovery source that publishes route updates into one
/// semantic owner.
#[derive(ComponentDefinition)]
pub(crate) struct DiscoveryRouteSource {
    ctx: ComponentContext<Self>,
    /// Provided route-discovery stream owned directly by the test harness.
    pub(crate) discovery: ProvidedPort<RouteDiscoveryPort<TransportRouteKey>>,
}

impl DiscoveryRouteSource {
    /// Create one new route-discovery source with an initially disconnected
    /// provided port.
    pub(crate) fn new() -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            discovery: ProvidedPort::uninitialised(),
        }
    }
}

ignore_lifecycle!(DiscoveryRouteSource);

impl Provide<RouteDiscoveryPort<TransportRouteKey>> for DiscoveryRouteSource {
    fn handle(&mut self, _request: Never) -> Handled {
        unreachable!("route discovery test source is indication-only")
    }
}

impl Actor for DiscoveryRouteSource {
    type Message = Never;

    fn receive_local(&mut self, _msg: Self::Message) -> Handled {
        unreachable!("Never type is empty")
    }

    fn receive_network(&mut self, _msg: NetMessage) -> Handled {
        unreachable!("route discovery test source does not use network actor messages")
    }
}

/// Shared transport-only harness core for delivery-domain tests.
///
/// This owns the driver, bridge, manager, and observer stack. Semantic-owner
/// harnesses layer ingress, probes, and route-discovery fixtures on top.
pub(crate) struct TransportHarnessCore {
    system: KompactSystem,
    driver: Arc<Component<IoDriverComponent>>,
    bridge: Arc<Component<IoBridge>>,
    manager: Arc<Component<RouteTransportManager>>,
    manager_ref: ActorRefStrong<RouteTransportActorMessage<TransportRouteKey>>,
    observer: Arc<Component<UdpObserver>>,
    observer_rx: BufferedReceiver<UdpIndication>,
}

impl TransportHarnessCore {
    /// Create one transport-only harness core inside the provided test system.
    ///
    /// The caller is responsible for connecting any semantic-owner ports before
    /// calling [`start`](Self::start).
    pub(crate) fn new(system: KompactSystem, udpour_config: UDPourConfig) -> Self {
        let driver = system.create(|| IoDriverComponent::new(DriverConfig::default()));
        let driver_for_bridge = driver.clone();
        let bridge = system.create(move || IoBridge::new(&driver_for_bridge));
        let bridge_handle = IoBridgeHandle::from_component(&bridge);
        let manager_system = system.clone();
        let manager = system.create(move || {
            RouteTransportManager::new(manager_system, bridge_handle, udpour_config)
        });
        let manager_ref = manager
            .actor_ref()
            .hold()
            .expect("route transport manager must expose a strong actor ref in tests");
        let (observer_tx, observer_rx) = mpsc::channel();
        let observer = system.create(move || UdpObserver::new(observer_tx));

        Self {
            system,
            driver,
            bridge,
            manager,
            manager_ref,
            observer,
            observer_rx: BufferedReceiver::new(observer_rx),
        }
    }

    /// Start the transport core and connect bridge UDP delivery into the
    /// manager and observer.
    pub(crate) fn start(&self) {
        let udp_connect_handle = IoBridgeHandle::from_component(&self.bridge);
        start_component(&self.system, &self.driver);
        start_component(&self.system, &self.bridge);
        block_on(udp_connect_handle.connect_udp(&self.manager))
            .expect("bridge must connect to route transport manager");
        block_on(udp_connect_handle.connect_udp(&self.observer))
            .expect("bridge must connect to UDP observer");
        start_component(&self.system, &self.manager);
        start_component(&self.system, &self.observer);
    }

    /// Access the underlying Kompact system for creating extra components.
    pub(crate) fn system(&self) -> &KompactSystem {
        &self.system
    }

    /// Access the route-transport manager component for port wiring or direct
    /// state inspection in tests.
    pub(crate) fn manager(&self) -> &Arc<Component<RouteTransportManager>> {
        &self.manager
    }

    /// Access the manager actor ref for submit asks.
    pub(crate) fn manager_ref(
        &self,
    ) -> ActorRefStrong<RouteTransportActorMessage<TransportRouteKey>> {
        self.manager_ref.clone()
    }

    /// Wait for one externally bound UDP socket owned by the observer.
    pub(crate) fn bind_external_socket(
        &self,
        bind: UdpLocalBind,
        timeout: Duration,
    ) -> (SocketId, SocketAddr) {
        let request_id = UdpOpenRequestId::new();
        self.observer.on_definition(|component| {
            component.udp.trigger(UdpRequest::Bind { request_id, bind });
        });
        match self.observer_rx.recv_matching(timeout, |event| {
            matches!(
                event,
                UdpIndication::Bound {
                    request_id: indicated_request_id,
                    ..
                } if *indicated_request_id == request_id
            )
        }) {
            UdpIndication::Bound {
                request_id: indicated_request_id,
                socket_id,
                local_addr,
            } => {
                assert_eq!(indicated_request_id, request_id);
                (socket_id, local_addr)
            }
            other => unreachable!("filtered to Bound, got {other:?}"),
        }
    }

    /// Wait for the next UDP bind indication observed by the bridge observer.
    pub(crate) fn wait_for_new_bound_socket(&self, timeout: Duration) -> (SocketId, SocketAddr) {
        match self.observer_rx.recv_matching(timeout, |event| {
            matches!(event, UdpIndication::Bound { .. })
        }) {
            UdpIndication::Bound {
                socket_id,
                local_addr,
                ..
            } => (socket_id, local_addr),
            other => unreachable!("filtered to Bound, got {other:?}"),
        }
    }

    /// Access the buffered observer event stream for specialised assertions in
    /// the manager tests.
    pub(crate) fn observer_rx(&self) -> &BufferedReceiver<UdpIndication> {
        &self.observer_rx
    }
}

impl Drop for TransportHarnessCore {
    fn drop(&mut self) {
        let _ = self
            .system
            .kill_notify(self.observer.clone())
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

/// Build a Kompact system for the semantic delivery full-stack tests.
pub(crate) fn build_delivery_test_system() -> KompactSystem {
    build_test_kompact_system_with(|config| {
        config.load_config_str("flotsync.route-transport.udp-activation-policy = 1");
    })
}

/// Build the shared UDPour config used by the semantic full-stack tests.
pub(crate) fn default_udpour_config() -> UDPourConfig {
    let sender = SenderConfig {
        max_part_payload_len: NonZeroUsize::new(1024)
            .expect("test UDPour config must use a non-zero part payload length"),
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
    UDPourConfig::new(sender, receiver).expect("valid datagram config")
}

/// Build one loopback member identity for tests from the supplied path
/// segments.
pub(crate) fn member_identity(segments: &[&str]) -> MemberIdentity {
    let mut identifier = IdentifierBuf::new();
    for segment in segments {
        identifier
            .push_checked((*segment).to_owned())
            .expect("test member identifier segment must be valid");
    }
    identifier.into_identifier()
}

/// Convenience helper for binding an ephemeral loopback UDP socket in the
/// shared observer harness.
pub(crate) fn bind_ephemeral_local_socket(
    core: &TransportHarnessCore,
    timeout: Duration,
) -> SocketAddr {
    core.bind_external_socket(UdpLocalBind::Exact(localhost(0)), timeout)
        .1
}
