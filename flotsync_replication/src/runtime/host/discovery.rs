use super::{RuntimeHostError, config_keys};
use crate::{
    api::MemberIdentity,
    delivery::{
        route_transport::{
            DatagramRouteScope,
            DiscoveryRouteUpdate,
            RouteDiscoveryPort,
            RoutePreferenceRank,
            RouteSharingKind,
            SendRouteCandidate,
            TransportRouteKey,
            UdpRouteKey,
        },
        shared::ReachabilityClass,
    },
};
use flotsync_io::prelude::{UdpIndication, UdpPort};
use kompact::{
    config::{Config, ConfigError, ConfigLookup},
    prelude::*,
};
use std::{
    net::{IpAddr, SocketAddr},
    str::FromStr,
};

pub(super) const STATIC_PEER_ROUTES_KEY: &str = "flotsync.replication.runtime.static-peer-routes";
const SUPPORTED_STATIC_ROUTE_PROTOCOLS: &[StaticPeerRouteProtocol] =
    &[StaticPeerRouteProtocol::Udp];

#[derive(Debug)]
pub(super) enum PreconfiguredPeerRoutesMessage {
    #[cfg(test)]
    Publish(DiscoveryRouteUpdate<TransportRouteKey>),
    #[cfg(test)]
    PublishPreconfiguredRoutes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::runtime) enum PreconfiguredPeerRoutesPublishMode {
    OnLocalEndpointBound,
    #[cfg(test)]
    ManualForTest,
}

/// Route-discovery source backed only by explicitly configured peer endpoints.
#[derive(ComponentDefinition)]
pub(super) struct PreconfiguredPeerRoutesComponent {
    ctx: ComponentContext<Self>,
    discovery: ProvidedPort<RouteDiscoveryPort<TransportRouteKey>>,
    udp: RequiredPort<UdpPort>,
    configured_local_endpoint: SocketAddr,
    routes: Vec<PreconfiguredPeerRoute>,
    local_endpoint: Option<SocketAddr>,
    published: bool,
    publish_mode: PreconfiguredPeerRoutesPublishMode,
}

impl PreconfiguredPeerRoutesComponent {
    pub(super) fn new(
        configured_local_endpoint: SocketAddr,
        publish_mode: PreconfiguredPeerRoutesPublishMode,
    ) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            discovery: ProvidedPort::uninitialised(),
            udp: RequiredPort::uninitialised(),
            configured_local_endpoint,
            routes: Vec::new(),
            local_endpoint: None,
            published: false,
            publish_mode,
        }
    }

    fn record_local_endpoint_bound(&mut self, local_addr: SocketAddr) {
        if self.configured_local_endpoint.port() != 0
            && local_addr != self.configured_local_endpoint
        {
            debug!(
                self.log(),
                "ignored UDP bind announcement for local_addr={} while waiting for configured local endpoint {}",
                local_addr,
                self.configured_local_endpoint
            );
            return;
        }
        if self.configured_local_endpoint.port() == 0
            && local_addr.ip() != self.configured_local_endpoint.ip()
        {
            debug!(
                self.log(),
                "ignored UDP bind announcement for local_addr={} while waiting for configured local endpoint {}",
                local_addr,
                self.configured_local_endpoint
            );
            return;
        }

        info!(
            self.log(),
            "observed runtime local endpoint bind local_addr={} configured_local_endpoint={} static_peer_route_count={} publish_mode={:?}",
            local_addr,
            self.configured_local_endpoint,
            self.routes.len(),
            self.publish_mode
        );
        self.local_endpoint = Some(local_addr);
        match self.publish_mode {
            PreconfiguredPeerRoutesPublishMode::OnLocalEndpointBound => {
                self.publish_preconfigured_routes();
            }
            #[cfg(test)]
            PreconfiguredPeerRoutesPublishMode::ManualForTest => {}
        }
    }

    fn publish_preconfigured_routes(&mut self) {
        if self.published {
            debug!(
                self.log(),
                "ignored duplicate preconfigured peer route publication request"
            );
            return;
        }
        let Some(local_endpoint) = self.local_endpoint else {
            warn!(
                self.log(),
                "cannot publish preconfigured peer routes before the local endpoint is bound"
            );
            return;
        };

        info!(
            self.log(),
            "publishing {} preconfigured peer route(s) from local_endpoint={}",
            self.routes.len(),
            local_endpoint
        );
        for route in &self.routes {
            debug!(
                self.log(),
                "publishing preconfigured peer route peer={} remote_addr={} local_endpoint={}",
                route.peer,
                route.remote_addr,
                local_endpoint
            );
            self.discovery
                .trigger(route.to_discovery_update(local_endpoint));
        }
        self.published = true;
    }
}

impl ComponentLifecycle for PreconfiguredPeerRoutesComponent {
    fn on_start(&mut self) -> Handled {
        match PreconfiguredPeerRoutesConfig::from_config(self.ctx.config()) {
            Ok(config) => {
                info!(
                    self.log(),
                    "loaded {} preconfigured peer route(s) for configured_local_endpoint={} publish_mode={:?}",
                    config.routes.len(),
                    self.configured_local_endpoint,
                    self.publish_mode
                );
                self.routes = config.routes;
                Handled::Ok
            }
            Err(error) => {
                error!(
                    self.log(),
                    "preconfigured peer route startup failed: {error}"
                );
                panic!("preconfigured peer route startup failed: {error}");
            }
        }
    }
}

impl Require<UdpPort> for PreconfiguredPeerRoutesComponent {
    fn handle(&mut self, indication: UdpIndication) -> Handled {
        if let UdpIndication::Bound { local_addr, .. } = indication {
            self.record_local_endpoint_bound(local_addr);
        }
        Handled::Ok
    }
}

ignore_requests!(
    RouteDiscoveryPort<TransportRouteKey>,
    PreconfiguredPeerRoutesComponent
);

impl Actor for PreconfiguredPeerRoutesComponent {
    type Message = PreconfiguredPeerRoutesMessage;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        match msg {
            #[cfg(test)]
            PreconfiguredPeerRoutesMessage::Publish(update) => {
                self.discovery.trigger(update);
                Handled::Ok
            }
            #[cfg(test)]
            PreconfiguredPeerRoutesMessage::PublishPreconfiguredRoutes => {
                self.publish_preconfigured_routes();
                Handled::Ok
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct PreconfiguredPeerRoutesConfig {
    routes: Vec<PreconfiguredPeerRoute>,
}

impl PreconfiguredPeerRoutesConfig {
    pub(super) fn from_config(config: &Config) -> Result<Self, RuntimeHostError> {
        let routes_lookup = config.select(STATIC_PEER_ROUTES_KEY);
        if let Err(error) = routes_lookup.value() {
            if is_missing_path(&error) {
                return Ok(Self { routes: Vec::new() });
            }
            return Err(invalid_static_routes_config(error));
        }

        let mut routes = Vec::new();
        // TODO(flotsync-6in): Replace this index probing once Kompact exposes
        // selected config arrays as a slice or iterator.
        for index in 0.. {
            let route_lookup = routes_lookup.get_index(index);
            match route_lookup.value() {
                Ok(_) => {
                    routes.push(parse_peer_route(route_lookup, index)?);
                }
                Err(error) if is_missing_path(&error) => break,
                Err(error) => return Err(invalid_static_routes_config(error)),
            }
        }

        Ok(Self { routes })
    }

    pub(super) fn validate_local_endpoint_bind_addr(
        &self,
        bind_addr: SocketAddr,
    ) -> Result<(), RuntimeHostError> {
        if !self.routes.is_empty() && bind_addr.port() == 0 {
            return Err(RuntimeHostError::InvalidConfig {
                key: config_keys::LOCAL_ENDPOINT_BIND_ADDR.key,
                message: format!(
                    "static peer routes require a fixed local endpoint port, but configured bind address is {bind_addr}"
                ),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PreconfiguredPeerRoute {
    peer: MemberIdentity,
    remote_addr: SocketAddr,
}

impl PreconfiguredPeerRoute {
    fn to_discovery_update(
        &self,
        local_endpoint: SocketAddr,
    ) -> DiscoveryRouteUpdate<TransportRouteKey> {
        DiscoveryRouteUpdate::PeerRoutes {
            peer: self.peer.clone(),
            classification: ReachabilityClass::Reachable,
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
            invalid_static_routes_config(format!("route[{index}].ip is invalid: {error}"))
        })?;
    let port = read_route_port(route_lookup, index)?;
    let peer = MemberIdentity::from_str(&name).map_err(|error| {
        invalid_static_routes_config(format!("route[{index}].name is invalid: {error}"))
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
                invalid_static_routes_config(format!(
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
        .map_err(|error| invalid_static_routes_config(format!("route[{index}].{field}: {error}")))
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
        .map_err(|error| invalid_static_routes_config(format!("route[{index}].port: {error}")))?;
    if !(1..=u16::MAX as i64).contains(&port) {
        return Err(invalid_static_routes_config(format!(
            "route[{index}].port must be between 1 and {}, got {port}",
            u16::MAX
        )));
    }
    Ok(port as u16)
}

fn is_missing_path(error: &ConfigError) -> bool {
    matches!(error, ConfigError::PathError(path) if path.is_missing())
}

fn invalid_static_routes_config(message: impl ToString) -> RuntimeHostError {
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
    fn rejects_static_peer_routes_with_ephemeral_local_endpoint() {
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

        let result = routes.validate_local_endpoint_bind_addr(
            "127.0.0.1:0".parse().expect("address should parse"),
        );

        assert!(matches!(
            result,
            Err(RuntimeHostError::InvalidConfig { .. })
        ));
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
