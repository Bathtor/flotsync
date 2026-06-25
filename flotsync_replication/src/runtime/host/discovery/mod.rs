//! Runtime-host discovery configuration and static route parsing.

use super::RuntimeHostError;
use flotsync_core::MemberIdentity;
use flotsync_discovery::protocol::DiscoveryRoute;
use flotsync_routes::route_establishment::WatchedRoute;
#[cfg(test)]
use flotsync_routes::{
    DatagramRouteScope,
    DiscoveryRouteUpdate,
    RoutePreferenceRank,
    RouteSharingKind,
    SendRouteCandidate,
    TransportRouteKey,
    UdpRouteKey,
};
use kompact::config::{Config, ConfigError, ConfigLookup};
use std::{
    fmt,
    net::{IpAddr, SocketAddr},
    str::FromStr,
};

pub(in crate::runtime::host) mod route_establishment;

pub(super) const STATIC_PEER_ROUTES_KEY: &str = "flotsync.replication.runtime.static-peer-routes";
const SUPPORTED_STATIC_ROUTE_PROTOCOLS: &[StaticPeerRouteProtocol] =
    &[StaticPeerRouteProtocol::Udp];

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::runtime) enum PreconfiguredPeerRoutesPublishMode {
    OnLocalEndpointBound,
    ManualForTest,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct PreconfiguredPeerRoutesConfig {
    routes: Vec<PreconfiguredPeerRoute>,
}

impl PreconfiguredPeerRoutesConfig {
    pub(super) fn from_config(config: &Config) -> Result<Self, RuntimeHostError> {
        let routes_lookup = config.select(STATIC_PEER_ROUTES_KEY);
        let mut routes = Vec::new();
        let route_entries = match routes_lookup.array_entries() {
            Ok(route_entries) => route_entries,
            Err(error) if is_missing_path(&error) => return Ok(Self { routes }),
            Err(error) => return Err(invalid_static_routes_config(&error)),
        };

        for (index, route_entry_lookup) in route_entries {
            let route = parse_peer_route(route_entry_lookup, index)?;
            routes.push(route);
        }

        Ok(Self { routes })
    }

    pub(super) fn watched_routes(&self) -> Vec<WatchedRoute> {
        self.routes
            .iter()
            .map(PreconfiguredPeerRoute::to_watched_route)
            .collect()
    }

    #[cfg(test)]
    pub(super) fn direct_route_updates(
        &self,
        local_endpoint: SocketAddr,
    ) -> Vec<DiscoveryRouteUpdate<TransportRouteKey>> {
        self.routes
            .iter()
            .map(|route| route.to_discovery_update(local_endpoint))
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PreconfiguredPeerRoute {
    peer: MemberIdentity,
    remote_addr: SocketAddr,
}

impl PreconfiguredPeerRoute {
    fn to_watched_route(&self) -> WatchedRoute {
        WatchedRoute {
            route: DiscoveryRoute::Udp(self.remote_addr),
            expected_member: Some(self.peer.clone()),
        }
    }

    #[cfg(test)]
    fn to_discovery_update(
        &self,
        local_endpoint: SocketAddr,
    ) -> DiscoveryRouteUpdate<TransportRouteKey> {
        DiscoveryRouteUpdate::PeerRoutes {
            peer: self.peer.clone(),
            routes: vec![SendRouteCandidate {
                coverage_key: TransportRouteKey::Udp(UdpRouteKey {
                    remote_addr: self.remote_addr,
                    scope: DatagramRouteScope::Unicast,
                    local_bind: Some(local_endpoint),
                }),
                sharing: RouteSharingKind::Exclusive,
                preference_rank: RoutePreferenceRank::new(1),
            }],
        }
    }
}

fn parse_peer_route(
    route_lookup: ConfigLookup<'_, '_>,
    index: usize,
) -> Result<PreconfiguredPeerRoute, RuntimeHostError> {
    let name = read_route_string(route_lookup, index, "name")?;
    let protocol = read_route_string(route_lookup, index, "protocol")?;
    let protocol = StaticPeerRouteProtocol::parse(&protocol, index)?;

    let ip = read_route_string(route_lookup, index, "ip")?
        .parse::<IpAddr>()
        .map_err(|error| {
            invalid_static_routes_config(&format!("route[{index}].ip is invalid: {error}"))
        })?;
    let port = read_route_port(route_lookup, index)?;
    let peer = MemberIdentity::from_str(&name).map_err(|error| {
        invalid_static_routes_config(&format!("route[{index}].name is invalid: {error}"))
    })?;

    match protocol {
        StaticPeerRouteProtocol::Udp => Ok(PreconfiguredPeerRoute {
            peer,
            remote_addr: SocketAddr::new(ip, port),
        }),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StaticPeerRouteProtocol {
    Udp,
}

impl StaticPeerRouteProtocol {
    fn parse(value: &str, index: usize) -> Result<Self, RuntimeHostError> {
        SUPPORTED_STATIC_ROUTE_PROTOCOLS
            .iter()
            .copied()
            .find(|protocol| protocol.as_str() == value)
            .ok_or_else(|| {
                invalid_static_routes_config(&format!(
                    "route[{index}].protocol {value:?} is unsupported; supported protocols are: {}",
                    supported_static_route_protocols()
                ))
            })
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Udp => "udp",
        }
    }
}

fn read_route_string(
    route_lookup: ConfigLookup<'_, '_>,
    index: usize,
    field: &'static str,
) -> Result<String, RuntimeHostError> {
    route_lookup
        .get(field)
        .as_string()
        .map_err(|error| invalid_static_routes_config(&format!("route[{index}].{field}: {error}")))
}

fn supported_static_route_protocols() -> String {
    SUPPORTED_STATIC_ROUTE_PROTOCOLS
        .iter()
        .map(|protocol| protocol.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

fn read_route_port(
    route_lookup: ConfigLookup<'_, '_>,
    index: usize,
) -> Result<u16, RuntimeHostError> {
    let port = route_lookup
        .get("port")
        .as_i64()
        .map_err(|error| invalid_static_routes_config(&format!("route[{index}].port: {error}")))?;
    if !(1..=i64::from(u16::MAX)).contains(&port) {
        return Err(invalid_static_routes_config(&format!(
            "route[{index}].port must be between 1 and {}, got {port}",
            u16::MAX
        )));
    }
    u16::try_from(port).map_err(|error| {
        invalid_static_routes_config(&format!(
            "route[{index}].port passed range validation but could not be converted: {error}"
        ))
    })
}

fn is_missing_path(error: &ConfigError) -> bool {
    matches!(error, ConfigError::PathError(path) if path.is_missing())
}

fn invalid_static_routes_config(message: &dyn fmt::Display) -> RuntimeHostError {
    RuntimeHostError::InvalidConfig {
        key: STATIC_PEER_ROUTES_KEY,
        message: message.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kompact::config::parse_config_str;

    #[test]
    fn parses_static_peer_routes_from_toml_array_tables() {
        let config = parse_config_str(
            r#"
            [[flotsync.replication.runtime.static-peer-routes]]
            name = "bob"
            protocol = "udp"
            ip = "127.0.0.1"
            port = 45100

            [[flotsync.replication.runtime.static-peer-routes]]
            name = "carol"
            protocol = "udp"
            ip = "::1"
            port = 45101
            "#,
        )
        .expect("config should parse");

        let routes =
            PreconfiguredPeerRoutesConfig::from_config(&config).expect("routes should parse");

        assert_eq!(routes.routes.len(), 2);
        assert_eq!(routes.routes[0].peer, MemberIdentity::from_array(["bob"]));
        assert_eq!(
            routes.routes[0].remote_addr,
            SocketAddr::from(([127, 0, 0, 1], 45100))
        );
        assert_eq!(routes.routes[1].peer, MemberIdentity::from_array(["carol"]));
        assert_eq!(
            routes.routes[1].remote_addr,
            SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], 45101))
        );
    }

    #[test]
    fn static_peer_routes_convert_to_expected_member_watches() {
        let config = parse_config_str(
            r#"
            [[flotsync.replication.runtime.static-peer-routes]]
            name = "bob"
            protocol = "udp"
            ip = "127.0.0.1"
            port = 45100
            "#,
        )
        .expect("config should parse");
        let routes =
            PreconfiguredPeerRoutesConfig::from_config(&config).expect("routes should parse");

        let watches = routes.watched_routes();

        assert_eq!(watches.len(), 1);
        assert_eq!(
            watches[0].route,
            DiscoveryRoute::Udp(SocketAddr::from(([127, 0, 0, 1], 45100)))
        );
        assert_eq!(
            watches[0].expected_member,
            Some(MemberIdentity::from_array(["bob"]))
        );
    }

    #[test]
    fn rejects_unsupported_static_peer_route_protocols() {
        let config = parse_config_str(
            r#"
            [[flotsync.replication.runtime.static-peer-routes]]
            name = "bob"
            protocol = "tcp"
            ip = "127.0.0.1"
            port = 45100
            "#,
        )
        .expect("config should parse");

        let result = PreconfiguredPeerRoutesConfig::from_config(&config);

        match result {
            Err(RuntimeHostError::InvalidConfig { message, .. }) => {
                assert!(message.contains("supported protocols are: udp"));
            }
            other => panic!("expected invalid config for unsupported protocol, got {other:?}"),
        }
    }
}
