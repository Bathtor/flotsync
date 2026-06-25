//! Route-establishment configuration and advertised-route validation.

use flotsync_discovery::DEFAULT_DISCOVERY_PORT;
use snafu::Snafu;
use std::{
    collections::BTreeSet,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};
use uuid::Uuid;

/// Configuration failures for route establishment components.
#[derive(Clone, Debug, PartialEq, Eq, Snafu)]
pub enum RouteEstablishmentConfigError {
    /// A route configured for signed advertisement is not externally reachable.
    #[snafu(display(
        "Route establishment advertised route {route} is not concrete; wildcard addresses and port 0 cannot be signed."
    ))]
    InvalidAdvertisedRoute { route: SocketAddr },
}

/// Runtime configuration for route establishment components.
#[derive(Clone, Debug)]
pub struct RouteEstablishmentConfig {
    /// Local UDP socket address used to receive plaintext `Peer` announcement messages.
    ///
    /// This is the peer-announcement discovery socket. It does not control the delivery endpoint
    /// address, the advertised delivery routes signed in introductions, or the peer-announcement
    /// broadcast destination port.
    pub peer_announcement_bind_addr: SocketAddr,
    /// Local process instance id used in outgoing peer announcements and introductions.
    pub instance_id: Uuid,
    /// Initial concrete routes this endpoint is allowed to claim in signed introductions.
    advertised_routes: ConcreteRoutes,
}

impl RouteEstablishmentConfig {
    /// Build a default route-establishment config.
    #[must_use]
    pub fn new() -> Self {
        Self {
            peer_announcement_bind_addr: SocketAddr::new(
                IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                *DEFAULT_DISCOVERY_PORT,
            ),
            instance_id: Uuid::new_v4(),
            advertised_routes: ConcreteRoutes::default(),
        }
    }

    /// Replace the advertised routes used in signed introduction claims.
    ///
    /// # Errors
    ///
    /// Returns [`RouteEstablishmentConfigError`] when any route uses a wildcard address or port
    /// `0`, because those are bind instructions rather than remotely reachable routes.
    pub fn with_advertised_routes(
        mut self,
        routes: impl IntoIterator<Item = SocketAddr>,
    ) -> Result<Self, RouteEstablishmentConfigError> {
        self.advertised_routes = ConcreteRoutes::from_routes(routes)?;
        Ok(self)
    }

    /// Return concrete routes this endpoint may claim in signed introductions.
    #[must_use]
    pub fn advertised_routes(&self) -> &ConcreteRoutes {
        &self.advertised_routes
    }

    /// Replace the local process instance id.
    #[cfg(test)]
    #[must_use]
    pub fn with_instance_id(mut self, instance_id: Uuid) -> Self {
        self.instance_id = instance_id;
        self
    }
}

impl Default for RouteEstablishmentConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Concrete UDP routes that can be claimed in signed route-establishment introductions.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConcreteRoutes {
    /// Validated concrete UDP socket addresses.
    routes: BTreeSet<SocketAddr>,
}

impl ConcreteRoutes {
    /// Build a validated concrete route set.
    ///
    /// # Errors
    ///
    /// Returns [`RouteEstablishmentConfigError`] when any route uses a wildcard address or port
    /// `0`, because those are bind instructions rather than remotely reachable routes.
    pub fn from_routes(
        routes: impl IntoIterator<Item = SocketAddr>,
    ) -> Result<Self, RouteEstablishmentConfigError> {
        let mut concrete_routes = BTreeSet::new();
        for route in routes {
            if !is_concrete_advertised_route(route) {
                return Err(RouteEstablishmentConfigError::InvalidAdvertisedRoute { route });
            }
            concrete_routes.insert(route);
        }
        Ok(Self {
            routes: concrete_routes,
        })
    }

    /// Return whether the set contains no concrete routes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }

    /// Return how many concrete routes are currently in the set.
    #[must_use]
    pub fn len(&self) -> usize {
        self.routes.len()
    }

    /// Iterate over the concrete routes in stable address order.
    pub fn iter(&self) -> impl Iterator<Item = &SocketAddr> {
        self.routes.iter()
    }

    /// Return the underlying concrete route set.
    #[must_use]
    pub fn routes(&self) -> &BTreeSet<SocketAddr> {
        &self.routes
    }
}

/// Returns whether `route` can be advertised as a remotely reachable endpoint.
fn is_concrete_advertised_route(route: SocketAddr) -> bool {
    route.port() != 0 && !route.ip().is_unspecified()
}
