//! Test-only host controls and route-observation helpers.

#[allow(
    clippy::wildcard_imports,
    reason = "The private host helper shares its parent's local implementation vocabulary."
)]
use super::*;

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
