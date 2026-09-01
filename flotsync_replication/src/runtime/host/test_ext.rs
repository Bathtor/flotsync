//! Test-only host controls and route-observation helpers.

#[allow(
    clippy::wildcard_imports,
    reason = "The private host helper shares its parent's local implementation vocabulary."
)]
use super::*;
#[cfg(test)]
use crate::runtime::summary_request_manager::SummaryRequestManagerMessage;
#[cfg(test)]
use flotsync_core::GroupId;

pub(crate) trait DeliveryRuntimeHostTestExt {
    /// Return the address peers should use when this host bound an unspecified interface.
    fn advertised_loopback_udp_addr(&self) -> SocketAddr;
    /// Publish a direct unicast peer route and wait until every route consumer observes it.
    fn publish_direct_peer_route(&self, peer: MemberIdentity, remote_addr: SocketAddr);
    /// Withdraw every direct route for one peer and wait until every route consumer observes it.
    #[cfg(test)]
    fn withdraw_direct_peer_routes(&self, peer: MemberIdentity);
    /// Publish a direct route through the production route-establishment provider.
    #[cfg(test)]
    fn publish_route_establishment_peer_route(&self, peer: MemberIdentity, remote_addr: SocketAddr);
    /// Replace route-establishment watches with test-selected routes.
    #[cfg(test)]
    fn replace_route_establishment_watches(
        &self,
        watches: Vec<flotsync_routes::route_establishment::WatchedRoute>,
    );
    /// Publish configured static routes after a test has explicitly requested them.
    #[cfg(test)]
    fn publish_preconfigured_peer_routes(&self);
    /// Return whether every direct-route consumer currently knows a peer route.
    #[cfg(test)]
    fn knows_direct_peer_route(&self, peer: &MemberIdentity) -> bool;
    /// Wait until every direct-route consumer has observed a peer route.
    #[cfg(test)]
    fn wait_for_direct_peer_route(&self, peer: &MemberIdentity);
    /// Wait until the runtime component accepts one mailbox turn.
    #[cfg(test)]
    fn wait_for_runtime_startup(&self);
    /// Wait until one application summary request is owned by the current manager.
    #[cfg(test)]
    fn wait_for_pending_summary_request(&self, group_id: GroupId, target: &MemberIdentity);
    /// Trigger a recoverable summary-manager invariant failure and wait for replacement.
    #[cfg(test)]
    fn recover_summary_request_manager(&self);
}

#[cfg(any(test, feature = "test-support"))]
impl DeliveryRuntimeHostTestExt for DeliveryRuntimeHost {
    fn advertised_loopback_udp_addr(&self) -> SocketAddr {
        loopback_advertise_addr(self.external_udp_addr)
    }

    fn publish_direct_peer_route(&self, peer: MemberIdentity, remote_addr: SocketAddr) {
        let update = direct_peer_route_update(self.external_udp_addr, peer.clone(), remote_addr);
        self.publish_route_update(update);
        wait_for_direct_peer_route(self.topology(), &peer);
    }

    #[cfg(test)]
    fn withdraw_direct_peer_routes(&self, peer: MemberIdentity) {
        self.publish_route_update(flotsync_routes::DiscoveryRouteUpdate::PeerRoutes {
            peer: peer.clone(),
            routes: Vec::new(),
        });
        wait_for_no_direct_peer_route(self.topology(), &peer);
    }

    #[cfg(test)]
    /// Publish a direct route through the production route-establishment provider.
    fn publish_route_establishment_peer_route(
        &self,
        peer: MemberIdentity,
        remote_addr: SocketAddr,
    ) {
        let update = direct_peer_route_update(self.external_udp_addr, peer.clone(), remote_addr);
        self.route_establishment_component()
            .on_definition(|component| component.publish_route_update_for_test(update));
        wait_for_direct_peer_route(self.topology(), &peer);
    }

    #[cfg(test)]
    fn replace_route_establishment_watches(
        &self,
        watches: Vec<flotsync_routes::route_establishment::WatchedRoute>,
    ) {
        let future = self
            .topology()
            .discovery
            .replace_route_establishment_watches_for_test(watches, self.control_timeout);
        wait_for_test_reply(future).expect("test route-establishment watches should be replaced");
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
        let summary_peer = peer.clone();
        let summary_knows = self
            .topology()
            .runtime
            .summary_request_manager()
            .on_definition(|component| component.knows_direct_route(&summary_peer));
        broadcast_knows && reliable_knows && summary_knows
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
            Ok(()) => {
                // Receiving the barrier reply is the complete success condition.
            }
            Err(error) => panic!(
                "replication runtime component became unavailable during test startup barrier: {error:?}"
            ),
        }
    }

    #[cfg(test)]
    fn wait_for_pending_summary_request(&self, group_id: GroupId, target: &MemberIdentity) {
        use flotsync_io::test_support::eventually_component_state;

        let manager = self.topology().runtime.summary_request_manager();
        let target = target.clone();
        eventually_component_state(
            TEST_DIRECT_PEER_ROUTE_TIMEOUT,
            &manager,
            |component| component.has_pending_summary(group_id, &target),
            format_args!(
                "timed out waiting for pending summary request for group={group_id}, target={target}"
            ),
        );
    }

    #[cfg(test)]
    fn recover_summary_request_manager(&self) {
        use flotsync_io::test_support::eventually;

        let original = self.topology().runtime.summary_request_manager();
        let original_id = original.id();
        original.actor_ref().tell(
            SummaryRequestManagerMessage::TriggerMissingInternalRetryForTest {
                group_id: flotsync_core::GroupId(uuid::Uuid::from_u128(50_901)),
                peer: MemberIdentity::from_array(["recovery", "probe"]),
                operation_id: uuid::Uuid::from_u128(50_902),
            },
        );
        eventually(
            TEST_DIRECT_PEER_ROUTE_TIMEOUT,
            || original.is_faulty(),
            "original summary request manager should become faulty",
        );
        eventually(
            TEST_DIRECT_PEER_ROUTE_TIMEOUT,
            || {
                let replacement = self.topology().runtime.summary_request_manager();
                replacement.id() != original_id && replacement.is_active()
            },
            "summary request manager should recover into an active replacement",
        );
    }
}

/// Build the direct-route indication shared by manual and production-provider tests.
#[cfg(any(test, feature = "test-support"))]
fn direct_peer_route_update(
    local_addr: SocketAddr,
    peer: MemberIdentity,
    remote_addr: SocketAddr,
) -> flotsync_routes::DiscoveryRouteUpdate<TransportRouteKey> {
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
            local_bind: Some(local_addr),
        }),
        sharing: RouteSharingKind::Exclusive,
        preference_rank: RoutePreferenceRank::new(1),
    };
    flotsync_routes::DiscoveryRouteUpdate::PeerRoutes {
        peer,
        routes: vec![route],
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

    let summary_peer = peer.clone();
    let summary_request_manager = topology.runtime.summary_request_manager();
    eventually_component_state(
        TEST_DIRECT_PEER_ROUTE_TIMEOUT,
        &summary_request_manager,
        |component| component.knows_direct_route(&summary_peer),
        format_args!("timed out waiting for summary-manager route publication for peer={peer}"),
    );
}

#[cfg(test)]
fn wait_for_no_direct_peer_route(topology: &RuntimeTopology, peer: &MemberIdentity) {
    use flotsync_io::test_support::eventually_component_state;

    let broadcast_peer = peer.clone();
    eventually_component_state(
        TEST_DIRECT_PEER_ROUTE_TIMEOUT,
        &topology.delivery.group_broadcast,
        |component| !component.knows_direct_route(&broadcast_peer),
        format_args!("timed out waiting for group-broadcast route withdrawal for peer={peer}"),
    );

    let reliable_peer = peer.clone();
    eventually_component_state(
        TEST_DIRECT_PEER_ROUTE_TIMEOUT,
        &topology.delivery.reliable_delivery,
        |component| !component.knows_direct_route(&reliable_peer),
        format_args!("timed out waiting for reliable-delivery route withdrawal for peer={peer}"),
    );

    let summary_peer = peer.clone();
    let summary_request_manager = topology.runtime.summary_request_manager();
    eventually_component_state(
        TEST_DIRECT_PEER_ROUTE_TIMEOUT,
        &summary_request_manager,
        |component| !component.knows_direct_route(&summary_peer),
        format_args!("timed out waiting for summary-manager route withdrawal for peer={peer}"),
    );
}
