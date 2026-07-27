//! Tests for checklist-specific peer-route diagnostic presentation.

use super::*;
use crate::replicated_checklist::runner::groups::test_support::named_test_group;
use flotsync_replication::{DiscoveryRoute, RouteDiagnostic, RouteDiagnosticPhase};
use indoc::indoc;
use std::net::SocketAddr;
use uuid::Uuid;

#[test]
fn peer_diagnostics_display_empty_state() {
    let groups = HashMap::new();
    let report = ChecklistPeerDiagnostics::new(
        RouteEstablishmentDiagnostics {
            local_endpoint: None,
            advertised_endpoints: Vec::new(),
            routes: Vec::new(),
        },
        &groups,
    );

    assert_eq!(
        report.to_string(),
        indoc! {"
            Peer route diagnostics
            local endpoint: unavailable
            advertised endpoints: none
            routes: none
            summary: known=0, probing=0, reachable=0, stale=0"}
    );
}

#[test]
fn peer_diagnostics_display_sorts_at_presentation_and_appends_shared_groups() {
    let bob = MemberIdentity::from_array(["bob"]);
    let carol = MemberIdentity::from_array(["carol"]);
    let group_id = GroupId(Uuid::from_u128(72_013));
    let groups = HashMap::from([(
        group_id,
        named_test_group(group_id, &bob, "shared with bob"),
    )]);
    let snapshot = RouteEstablishmentDiagnostics {
        local_endpoint: Some(SocketAddr::from(([0, 0, 0, 0], 45_100))),
        advertised_endpoints: vec![
            SocketAddr::from(([192, 0, 2, 11], 45_100)),
            SocketAddr::from(([192, 0, 2, 10], 45_100)),
        ],
        routes: vec![
            RouteDiagnostic {
                route: DiscoveryRoute::Udp(SocketAddr::from(([192, 0, 2, 23], 45_104))),
                peer_announced: true,
                configured_members: None,
                phase: RouteDiagnosticPhase::Stale,
                identified_members: vec![carol.clone()],
                reachable_members: Vec::new(),
            },
            RouteDiagnostic {
                route: DiscoveryRoute::Udp(SocketAddr::from(([192, 0, 2, 22], 45_103))),
                peer_announced: true,
                configured_members: Some(ConfiguredRouteMembers::Any),
                phase: RouteDiagnosticPhase::Reachable,
                identified_members: vec![carol.clone(), bob.clone()],
                reachable_members: vec![carol, bob.clone()],
            },
            RouteDiagnostic {
                route: DiscoveryRoute::Udp(SocketAddr::from(([192, 0, 2, 21], 45_102))),
                peer_announced: true,
                configured_members: None,
                phase: RouteDiagnosticPhase::Probing,
                identified_members: Vec::new(),
                reachable_members: Vec::new(),
            },
            RouteDiagnostic {
                route: DiscoveryRoute::Udp(SocketAddr::from(([192, 0, 2, 20], 45_101))),
                peer_announced: false,
                configured_members: Some(ConfiguredRouteMembers::Members(vec![bob.clone()])),
                phase: RouteDiagnosticPhase::Known,
                identified_members: Vec::new(),
                reachable_members: Vec::new(),
            },
        ],
    };

    assert_eq!(
        ChecklistPeerDiagnostics::new(snapshot, &groups).to_string(),
        indoc! {"
            Peer route diagnostics
            local endpoint: udp://0.0.0.0:45100
            advertised endpoints:
              udp://192.0.2.10:45100
              udp://192.0.2.11:45100
            routes:
              udp://192.0.2.20:45101: known; sources=configured; expected=bob; identified=none; reachable=none
              udp://192.0.2.21:45102: probing; sources=announced; expected=none; identified=none; reachable=none
              udp://192.0.2.22:45103: reachable; sources=announced,configured; expected=any; identified=bob,carol; reachable=bob,carol
              udp://192.0.2.23:45104: stale; sources=announced; expected=none; identified=carol; reachable=none
            summary: known=1, probing=1, reachable=1, stale=1
            identified member groups:
              bob: shared groups=1
              carol: shared groups=0"}
    );
}
