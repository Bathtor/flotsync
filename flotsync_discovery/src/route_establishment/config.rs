use crate::DEFAULT_DISCOVERY_PORT;
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
    /// Local socket address used to receive plaintext `Peer` announcement messages.
    pub peer_announcement_bind_addr: SocketAddr,
    /// Local runtime endpoint bind configured for follow-up discovery and replication traffic.
    pub configured_local_endpoint: SocketAddr,
    /// Local process instance id used in outgoing peer announcements and introductions.
    pub instance_id: Uuid,
    /// Concrete routes this endpoint is allowed to claim in signed introductions.
    advertised_routes: BTreeSet<SocketAddr>,
}

impl RouteEstablishmentConfig {
    /// Build a default config for the given runtime endpoint.
    #[must_use]
    pub fn new(configured_local_endpoint: SocketAddr) -> Self {
        Self {
            peer_announcement_bind_addr: SocketAddr::new(
                IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                *DEFAULT_DISCOVERY_PORT,
            ),
            configured_local_endpoint,
            instance_id: Uuid::new_v4(),
            advertised_routes: BTreeSet::new(),
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
        let mut advertised_routes = BTreeSet::new();
        for route in routes {
            if !is_concrete_advertised_route(route) {
                return Err(RouteEstablishmentConfigError::InvalidAdvertisedRoute { route });
            }
            advertised_routes.insert(route);
        }
        self.advertised_routes = advertised_routes;
        Ok(self)
    }

    /// Return concrete routes this endpoint may claim in signed introductions.
    #[must_use]
    pub fn advertised_routes(&self) -> &BTreeSet<SocketAddr> {
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

/// Returns whether `route` can be advertised as a remotely reachable endpoint.
fn is_concrete_advertised_route(route: SocketAddr) -> bool {
    route.port() != 0 && !route.ip().is_unspecified()
}
