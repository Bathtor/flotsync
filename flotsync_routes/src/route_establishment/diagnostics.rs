//! Read-only diagnostic snapshots of route-establishment state.

use flotsync_core::MemberIdentity;
use flotsync_discovery::protocol::DiscoveryRoute;
use std::{fmt, net::SocketAddr};

/// One point-in-time diagnostic snapshot of local and remote route establishment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteEstablishmentDiagnostics {
    /// Local route endpoint currently available for introduction traffic.
    pub local_endpoint: Option<SocketAddr>,
    /// Concrete local endpoints currently advertised in signed introduction claims.
    pub advertised_endpoints: Vec<SocketAddr>,
    /// Known remote routes.
    pub routes: Vec<RouteDiagnostic>,
}

impl fmt::Display for RouteEstablishmentDiagnostics {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "Peer route diagnostics")?;
        match self.local_endpoint {
            Some(endpoint) => writeln!(formatter, "local endpoint: udp://{endpoint}")?,
            None => writeln!(formatter, "local endpoint: unavailable")?,
        }
        if self.advertised_endpoints.is_empty() {
            writeln!(formatter, "advertised endpoints: none")?;
        } else {
            writeln!(formatter, "advertised endpoints:")?;
            for endpoint in &self.advertised_endpoints {
                writeln!(formatter, "  udp://{endpoint}")?;
            }
        }

        let mut phase_counts = [0_usize; 4];
        if self.routes.is_empty() {
            writeln!(formatter, "routes: none")?;
        } else {
            writeln!(formatter, "routes:")?;
            for route in &self.routes {
                phase_counts[route_diagnostic_phase_index(route.phase)] += 1;
                writeln!(formatter, "  {route}")?;
            }
        }
        write!(
            formatter,
            "summary: known={}, probing={}, reachable={}, stale={}",
            phase_counts[0], phase_counts[1], phase_counts[2], phase_counts[3]
        )
    }
}

/// Diagnostic state for one known remote route.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteDiagnostic {
    /// Concrete remote route observed or configured for establishment.
    pub route: DiscoveryRoute,
    /// Whether a plaintext peer announcement named this route.
    pub peer_announced: bool,
    /// Optional member constraint supplied by local route configuration.
    pub configured_members: Option<ConfiguredRouteMembers>,
    /// Current route-verification lifecycle phase.
    pub phase: RouteDiagnosticPhase,
    /// Members authenticated by the latest qualifying response, retained across later staleness.
    pub identified_members: Vec<MemberIdentity>,
    /// Members for whom this route is currently published to delivery.
    pub reachable_members: Vec<MemberIdentity>,
}

impl fmt::Display for RouteDiagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}: {}; sources={}; expected=",
            self.route,
            self.phase,
            route_interest_sources(self)
        )?;
        match &self.configured_members {
            Some(configured_members) => write!(formatter, "{configured_members}")?,
            None => formatter.write_str("none")?,
        }
        write!(
            formatter,
            "; identified={}; reachable={}",
            MemberList(&self.identified_members),
            MemberList(&self.reachable_members)
        )
    }
}

/// Member constraint attached to one locally configured route.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConfiguredRouteMembers {
    /// The configured route accepts any member permitted by local discovery policy.
    Any,
    /// The configured route accepts exactly these expected members.
    Members(Vec<MemberIdentity>),
}

impl fmt::Display for ConfiguredRouteMembers {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Any => formatter.write_str("any"),
            Self::Members(members) => write!(formatter, "{}", MemberList(members)),
        }
    }
}

/// Current route-establishment lifecycle exposed through diagnostics.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RouteDiagnosticPhase {
    /// The route is known but no introduction probe is currently active.
    Known,
    /// An introduction probe is waiting for a matching response.
    Probing,
    /// At least one authenticated member is currently published through the route.
    Reachable,
    /// The route was withdrawn, expired, or failed its latest establishment attempt.
    Stale,
}

impl RouteDiagnosticPhase {
    /// Stable lower-case diagnostic label for this lifecycle phase.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Known => "known",
            Self::Probing => "probing",
            Self::Reachable => "reachable",
            Self::Stale => "stale",
        }
    }
}

impl fmt::Display for RouteDiagnosticPhase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Return the summary-array index for one route phase.
const fn route_diagnostic_phase_index(phase: RouteDiagnosticPhase) -> usize {
    match phase {
        RouteDiagnosticPhase::Known => 0,
        RouteDiagnosticPhase::Probing => 1,
        RouteDiagnosticPhase::Reachable => 2,
        RouteDiagnosticPhase::Stale => 3,
    }
}

/// Format active interest sources for one route.
fn route_interest_sources(route: &RouteDiagnostic) -> &'static str {
    match (route.peer_announced, route.configured_members.is_some()) {
        (true, true) => "announced,configured",
        (true, false) => "announced",
        (false, true) => "configured",
        (false, false) => "none",
    }
}

/// Display adapter for one member slice in its supplied order.
struct MemberList<'a>(&'a [MemberIdentity]);

impl fmt::Display for MemberList<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            formatter.write_str("none")
        } else {
            for (index, member) in self.0.iter().enumerate() {
                if index > 0 {
                    formatter.write_str(",")?;
                }
                write!(formatter, "{member}")?;
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indoc::indoc;

    /// Build one otherwise-empty route diagnostic for display aggregation tests.
    fn route_with_phase(last_octet: u8, phase: RouteDiagnosticPhase) -> RouteDiagnostic {
        RouteDiagnostic {
            route: DiscoveryRoute::Udp(SocketAddr::from(([192, 0, 2, last_octet], 45_101))),
            peer_announced: true,
            configured_members: None,
            phase,
            identified_members: Vec::new(),
            reachable_members: Vec::new(),
        }
    }

    #[test]
    fn diagnostic_leaf_types_have_readable_display() {
        let alice = MemberIdentity::from_array(["alice"]);
        let bob = MemberIdentity::from_array(["bob"]);

        assert_eq!(RouteDiagnosticPhase::Known.to_string(), "known");
        assert_eq!(RouteDiagnosticPhase::Probing.to_string(), "probing");
        assert_eq!(RouteDiagnosticPhase::Reachable.to_string(), "reachable");
        assert_eq!(RouteDiagnosticPhase::Stale.to_string(), "stale");
        assert_eq!(
            DiscoveryRoute::Udp(SocketAddr::from(([192, 0, 2, 20], 45_101))).to_string(),
            "udp://192.0.2.20:45101"
        );
        assert_eq!(ConfiguredRouteMembers::Any.to_string(), "any");
        assert_eq!(
            ConfiguredRouteMembers::Members(vec![bob, alice]).to_string(),
            "bob,alice"
        );
    }

    #[test]
    fn route_diagnostic_display_includes_all_observability_fields() {
        let alice = MemberIdentity::from_array(["alice"]);
        let bob = MemberIdentity::from_array(["bob"]);
        let route = RouteDiagnostic {
            route: DiscoveryRoute::Udp(SocketAddr::from(([192, 0, 2, 20], 45_101))),
            peer_announced: true,
            configured_members: Some(ConfiguredRouteMembers::Members(vec![alice.clone()])),
            phase: RouteDiagnosticPhase::Reachable,
            identified_members: vec![alice],
            reachable_members: vec![bob],
        };

        assert_eq!(
            route.to_string(),
            "udp://192.0.2.20:45101: reachable; sources=announced,configured; expected=alice; identified=alice; reachable=bob"
        );
    }

    #[test]
    fn route_establishment_diagnostics_display_empty_snapshot() {
        let snapshot = RouteEstablishmentDiagnostics {
            local_endpoint: None,
            advertised_endpoints: Vec::new(),
            routes: Vec::new(),
        };

        assert_eq!(
            snapshot.to_string(),
            indoc! {"
                Peer route diagnostics
                local endpoint: unavailable
                advertised endpoints: none
                routes: none
                summary: known=0, probing=0, reachable=0, stale=0"}
        );
    }

    #[test]
    fn route_establishment_diagnostics_display_every_phase_in_supplied_order() {
        let snapshot = RouteEstablishmentDiagnostics {
            local_endpoint: Some(SocketAddr::from(([0, 0, 0, 0], 45_100))),
            advertised_endpoints: vec![SocketAddr::from(([192, 0, 2, 10], 45_100))],
            routes: vec![
                route_with_phase(20, RouteDiagnosticPhase::Known),
                route_with_phase(21, RouteDiagnosticPhase::Probing),
                route_with_phase(22, RouteDiagnosticPhase::Reachable),
                route_with_phase(23, RouteDiagnosticPhase::Stale),
            ],
        };

        assert_eq!(
            snapshot.to_string(),
            indoc! {"
                Peer route diagnostics
                local endpoint: udp://0.0.0.0:45100
                advertised endpoints:
                  udp://192.0.2.10:45100
                routes:
                  udp://192.0.2.20:45101: known; sources=announced; expected=none; identified=none; reachable=none
                  udp://192.0.2.21:45101: probing; sources=announced; expected=none; identified=none; reachable=none
                  udp://192.0.2.22:45101: reachable; sources=announced; expected=none; identified=none; reachable=none
                  udp://192.0.2.23:45101: stale; sources=announced; expected=none; identified=none; reachable=none
                summary: known=1, probing=1, reachable=1, stale=1"}
        );
    }
}
