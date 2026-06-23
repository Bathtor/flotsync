use crate::protocol::DiscoveryRoute;
use flotsync_core::MemberIdentity;
use kompact::prelude::{Never, Port};
use smallvec::SmallVec;
use std::net::SocketAddr;

/// Route candidates are usually one direct LAN path and occasionally one extra address.
pub type DiscoveryRouteCandidates = SmallVec<[DiscoveryRouteCandidate; 2]>;

/// One route candidate discovered for a peer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DiscoveryRouteCandidate {
    /// Verified route endpoint.
    pub route: DiscoveryRoute,
    /// Local endpoint bind used to reach this route, if known.
    pub local_bind: Option<SocketAddr>,
}

/// Published discovery route update for one peer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiscoveryRouteUpdate {
    /// Fully replace the route set for `peer`; an empty list withdraws all routes.
    PeerRoutes {
        peer: MemberIdentity,
        routes: DiscoveryRouteCandidates,
    },
}

/// Discovery port used to publish verified route updates.
#[derive(Clone, Copy, Debug, Default)]
pub struct DiscoveryRoutePort;

impl Port for DiscoveryRoutePort {
    type Request = Never;
    type Indication = DiscoveryRouteUpdate;
}
