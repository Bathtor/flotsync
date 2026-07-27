//! Tests for read-only route-establishment diagnostics.

use super::{fixtures::*, *};

#[test]
fn diagnostics_report_empty_route_state() {
    let local_member = member(["alice"]);
    let harness = RouteEstablishmentHarness::new(
        local_member.clone(),
        shared_memberships(&local_member, &member(["bob"])),
    );

    assert_eq!(
        harness.diagnostics(),
        RouteEstablishmentDiagnostics {
            local_endpoint: None,
            advertised_endpoints: Vec::new(),
            routes: Vec::new(),
        }
    );
    harness.shutdown();
}

#[test]
fn diagnostics_report_local_endpoints_and_unidentified_announced_route() {
    let local_member = member(["alice"]);
    let harness = RouteEstablishmentHarness::new(
        local_member.clone(),
        shared_memberships(&local_member, &member(["bob"])),
    );
    let local_endpoint = SocketAddr::from(([127, 0, 0, 1], 49_130));
    let advertised_endpoint = SocketAddr::from(([192, 0, 2, 10], 49_130));
    let remote_route = SocketAddr::from(([192, 0, 2, 20], 62_190));

    harness.observe_peer_route(Uuid::from_u128(190), remote_route);
    harness.publish_endpoint_selection_and_wait_until_applied([advertised_endpoint]);
    harness.bind_endpoint(SocketId(110), local_endpoint);
    harness.expect_transport_probe(local_endpoint, remote_route);

    assert_eq!(
        harness.diagnostics(),
        RouteEstablishmentDiagnostics {
            local_endpoint: Some(local_endpoint),
            advertised_endpoints: vec![advertised_endpoint],
            routes: vec![RouteDiagnostic {
                route: DiscoveryRoute::Udp(remote_route),
                peer_announced: true,
                configured_members: None,
                phase: RouteDiagnosticPhase::Probing,
                identified_members: Vec::new(),
                reachable_members: Vec::new(),
            }],
        }
    );
    harness.shutdown();
}

#[test]
fn diagnostics_report_configured_members_for_each_route() {
    let local_member = member(["alice"]);
    let bob = member(["bob"]);
    let carol = member(["carol"]);
    let harness = RouteEstablishmentHarness::new(
        local_member.clone(),
        shared_memberships(&local_member, &bob),
    );
    let first_route = SocketAddr::from(([192, 0, 2, 30], 62_191));
    let second_route = SocketAddr::from(([192, 0, 2, 31], 62_192));

    harness
        .replace_manual_route_watches([
            watched_udp_route(second_route, Some(carol.clone())),
            watched_udp_route(first_route, Some(carol.clone())),
            watched_udp_route(first_route, Some(bob.clone())),
        ])
        .expect("configured routes should be accepted");

    let diagnostics = harness.diagnostics();
    assert_eq!(diagnostics.routes.len(), 2);
    let first = diagnostics
        .routes
        .iter()
        .find(|route| route.route == DiscoveryRoute::Udp(first_route))
        .expect("first configured route should be present");
    let Some(ConfiguredRouteMembers::Members(first_members)) = &first.configured_members else {
        panic!("first route should retain its configured member constraint");
    };
    assert_eq!(
        member_set(first_members.clone()),
        member_set([bob, carol.clone()])
    );
    let second = diagnostics
        .routes
        .iter()
        .find(|route| route.route == DiscoveryRoute::Udp(second_route))
        .expect("second configured route should be present");
    assert_eq!(
        second.configured_members,
        Some(ConfiguredRouteMembers::Members(vec![carol]))
    );
    assert!(
        diagnostics
            .routes
            .iter()
            .all(|route| route.phase == RouteDiagnosticPhase::Known)
    );
    harness.shutdown();
}

#[test]
fn diagnostics_replace_identified_members_and_retain_latest_after_stale() {
    let local_member = member(["alice"]);
    let bob = member(["bob"]);
    let carol = member(["carol"]);
    let dave = member(["dave"]);
    let harness = RouteEstablishmentHarness::new(
        local_member.clone(),
        shared_memberships(&local_member, &bob),
    );
    let remote_route = SocketAddr::from(([192, 0, 2, 40], 62_193));
    let first_response_members = member_set([bob.clone(), carol.clone()]);

    harness.observe_peer_route(Uuid::from_u128(193), remote_route);
    harness
        .mark_route_reachable_with_identified_members(remote_route, [carol.clone(), bob.clone()]);

    let route = &harness.diagnostics().routes[0];
    assert_eq!(route.phase, RouteDiagnosticPhase::Reachable);
    assert_eq!(
        member_set(route.identified_members.clone()),
        first_response_members
    );
    assert_eq!(
        member_set(route.reachable_members.clone()),
        first_response_members
    );

    harness.mark_route_stale(remote_route);

    let route = &harness.diagnostics().routes[0];
    assert_eq!(route.phase, RouteDiagnosticPhase::Stale);
    assert_eq!(
        member_set(route.identified_members.clone()),
        first_response_members
    );
    assert!(route.reachable_members.is_empty());

    harness.mark_route_reachable_with_identified_members(remote_route, [dave.clone()]);

    let route = &harness.diagnostics().routes[0];
    assert_eq!(
        member_set(route.identified_members.clone()),
        member_set([dave.clone()])
    );
    assert_eq!(
        member_set(route.reachable_members.clone()),
        member_set([dave])
    );
    harness.shutdown();
}

#[test]
fn authenticated_response_replaces_previous_identified_members() {
    let local_member = member(["alice"]);
    let previous_member = member(["bob"]);
    let authenticated_member = member(["carol"]);
    let memberships = shared_memberships(&local_member, &authenticated_member);
    let local_endpoint = SocketAddr::from(([127, 0, 0, 1], 49_131));
    let remote_route = SocketAddr::from(([127, 0, 0, 1], 62_194));
    let remote_instance = Uuid::from_u128(194);
    let harness = RouteEstablishmentHarness::new(local_member, memberships);
    let nonce = harness.probe_manual_route(
        SocketId(111),
        local_endpoint,
        [watched_udp_route(
            remote_route,
            Some(authenticated_member.clone()),
        )],
        remote_route,
    );
    harness.replace_identified_members(remote_route, [previous_member]);
    let response = IntroductionSpec::new(
        &authenticated_member,
        remote_instance,
        remote_route,
        [group_id(1)],
    )
    .encode(nonce);

    harness.receive_transport(remote_route, response);

    harness.expect_peer_route_update(&authenticated_member, &[remote_route], Some(local_endpoint));
    let route = &harness.diagnostics().routes[0];
    assert_eq!(route.identified_members, vec![authenticated_member]);
    harness.shutdown();
}

#[test]
fn response_without_authenticated_claims_retains_previous_identified_members() {
    let local_member = member(["alice"]);
    let previous_member = member(["bob"]);
    let rejected_member = member(["carol"]);
    let memberships = shared_memberships(&local_member, &rejected_member);
    let local_endpoint = SocketAddr::from(([127, 0, 0, 1], 49_132));
    let remote_route = SocketAddr::from(([127, 0, 0, 1], 62_195));
    let remote_instance = Uuid::from_u128(195);
    let harness = RouteEstablishmentHarness::with_credentials(
        local_member,
        memberships,
        Arc::new(RouteEstablishmentTestCredentials::reject_claim_verification()),
    );
    let nonce = harness.probe_manual_route(
        SocketId(112),
        local_endpoint,
        [watched_udp_route(
            remote_route,
            Some(rejected_member.clone()),
        )],
        remote_route,
    );
    harness.replace_identified_members(remote_route, [previous_member.clone()]);
    let response = IntroductionSpec::new(
        &rejected_member,
        remote_instance,
        remote_route,
        [group_id(1)],
    )
    .encode(nonce);

    harness.receive_transport(remote_route, response);

    harness.expect_no_route_update("unverifiable response should not publish a route");
    let route = &harness.diagnostics().routes[0];
    assert_eq!(route.phase, RouteDiagnosticPhase::Stale);
    assert_eq!(route.identified_members, vec![previous_member]);
    harness.shutdown();
}
