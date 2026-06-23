//! Runtime-host adapters between route establishment and delivery transport.

use crate::delivery::security::DeliverySecurity;
use flotsync_core::MemberIdentity;
use flotsync_discovery::protocol::DiscoveryRoute;
use flotsync_routes::{
    DatagramRouteScope,
    DiscoveryRouteUpdate as TransportDiscoveryRouteUpdate,
    RouteDiscoveryPort,
    RoutePreferenceRank,
    RouteSharingKind,
    SendRouteCandidate,
    TransportRouteKey,
    UdpRouteKey,
    route_establishment::{DiscoveryCredentialFuture, DiscoveryCredentials},
    route_publication::{DiscoveryRoutePort, DiscoveryRouteUpdate},
};
use flotsync_utils::BoxError;
use futures_util::FutureExt as _;
use kompact::prelude::*;

/// Actor messages accepted by [`DiscoveryRouteAdapterComponent`].
#[derive(Debug)]
pub enum DiscoveryRouteAdapterMessage {
    /// Test-support route update injected directly into delivery owners.
    #[cfg(any(test, feature = "test-support"))]
    Publish(TransportDiscoveryRouteUpdate<TransportRouteKey>),
}

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
    type Message = DiscoveryRouteAdapterMessage;

    fn receive_local(&mut self, msg: Self::Message) -> HandlerResult {
        match msg {
            #[cfg(any(test, feature = "test-support"))]
            DiscoveryRouteAdapterMessage::Publish(update) => {
                self.discovery_provided.trigger(update);
                Handled::OK
            }
        }
    }
}
