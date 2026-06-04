#![allow(
    dead_code,
    reason = "route establishment adapter is wired into the host in flotsync-ol4e"
)]

use crate::delivery::{
    route_transport::{
        DatagramRouteScope,
        DiscoveryRouteUpdate as TransportDiscoveryRouteUpdate,
        RouteDiscoveryPort,
        RoutePreferenceRank,
        RouteSharingKind,
        SendRouteCandidate,
        TransportRouteKey,
        UdpRouteKey,
    },
    security::DeliverySecurity,
};
use flotsync_core::MemberIdentity;
use flotsync_discovery::{
    protocol::DiscoveryRoute,
    route_establishment::{DiscoveryCredentialFuture, DiscoveryCredentials},
    route_publication::{DiscoveryRoutePort, DiscoveryRouteUpdate},
};
use flotsync_utils::BoxError;
use futures_util::FutureExt as _;
use kompact::prelude::*;

impl DiscoveryCredentials for DeliverySecurity {
    fn sign_discovery_claim_payload(
        &self,
        payload: &[u8],
    ) -> Result<flotsync_security::FrameSignature, BoxError> {
        DeliverySecurity::sign_discovery_claim_payload(self, payload)
            .map_err(|error| Box::new(error) as BoxError)
    }

    fn verify_discovery_claim_payload<'a>(
        &'a self,
        member: &'a MemberIdentity,
        payload: &'a [u8],
        signature: &'a flotsync_security::FrameSignature,
    ) -> DiscoveryCredentialFuture<'a> {
        async move {
            DeliverySecurity::verify_discovery_claim_payload(self, member, payload, signature)
                .await
                .map_err(|error| Box::new(error) as BoxError)
        }
        .boxed()
    }
}

/// Adapter from discovery-level route updates to replication transport route updates.
#[derive(ComponentDefinition)]
pub struct DiscoveryRouteAdapterComponent {
    ctx: ComponentContext<Self>,
    discovery_provided: ProvidedPort<RouteDiscoveryPort<TransportRouteKey>>,
    discovery_required: RequiredPort<DiscoveryRoutePort>,
}

impl DiscoveryRouteAdapterComponent {
    /// Build one route update adapter.
    #[must_use]
    pub fn new() -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            discovery_provided: ProvidedPort::uninitialised(),
            discovery_required: RequiredPort::uninitialised(),
        }
    }
}

ignore_lifecycle!(DiscoveryRouteAdapterComponent);

ignore_requests!(
    RouteDiscoveryPort<TransportRouteKey>,
    DiscoveryRouteAdapterComponent
);

impl Require<DiscoveryRoutePort> for DiscoveryRouteAdapterComponent {
    fn handle(&mut self, indication: DiscoveryRouteUpdate) -> HandlerResult {
        match indication {
            DiscoveryRouteUpdate::PeerRoutes { peer, routes } => {
                let routes = routes
                    .into_iter()
                    .map(|route| {
                        let coverage_key = match route.route {
                            DiscoveryRoute::Udp(remote_addr) => {
                                TransportRouteKey::Udp(UdpRouteKey {
                                    remote_addr,
                                    scope: DatagramRouteScope::Unicast,
                                    local_bind: route.local_bind,
                                })
                            }
                        };
                        SendRouteCandidate {
                            coverage_key,
                            sharing: RouteSharingKind::Exclusive,
                            preference_rank: RoutePreferenceRank::new(1),
                        }
                    })
                    .collect();
                self.discovery_provided
                    .trigger(TransportDiscoveryRouteUpdate::PeerRoutes { peer, routes });
            }
        }
        Handled::OK
    }
}

impl Actor for DiscoveryRouteAdapterComponent {
    type Message = Never;

    fn receive_local(&mut self, _msg: Self::Message) -> HandlerResult {
        unreachable!("Never type is empty")
    }
}
