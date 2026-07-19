//! Tests for manual route watches and their verification outcomes.

use super::{fixtures::*, *};

#[test]
fn endpoint_binding_report_probes_inactive_routes() {
    let local_member = member(["alice"]);
    let remote_member = member(["bob"]);
    let memberships = shared_memberships(&local_member, &remote_member);
    let remote_route = SocketAddr::from(([127, 0, 0, 1], 62157));
    let local_endpoint = SocketAddr::from(([127, 0, 0, 1], 49101));
    let instance_id = Uuid::from_u128(41);
    let harness = RouteEstablishmentHarness::new(local_member, memberships);

    harness.observe_peer_route(instance_id, remote_route);
    harness.bind_endpoint(SocketId(82), local_endpoint);

    harness.expect_transport_probe(local_endpoint, remote_route);
    harness.shutdown();
}

#[test]
fn endpoint_binding_does_not_probe_local_peer_announcement() {
    let local_member = member(["alice"]);
    let remote_member = member(["bob"]);
    let memberships = shared_memberships(&local_member, &remote_member);
    let remote_route = SocketAddr::from(([127, 0, 0, 1], 62158));
    let local_instance_id = Uuid::from_u128(11);
    let harness = RouteEstablishmentHarness::new(local_member, memberships);

    harness.observe_peer_route(local_instance_id, remote_route);
    harness.bind_endpoint(SocketId(83), SocketAddr::from(([127, 0, 0, 1], 49102)));

    harness.expect_no_transport_submit("local peer announcements must not produce probes");
    harness.shutdown();
}

#[test]
fn manual_route_watch_verifies_and_publishes_expected_member() {
    let local_member = member(["alice"]);
    let remote_member = member(["bob"]);
    let memberships = shared_memberships(&local_member, &remote_member);
    let local_endpoint = SocketAddr::from(([127, 0, 0, 1], 49110));
    let remote_route = SocketAddr::from(([127, 0, 0, 1], 62170));
    let remote_instance = Uuid::from_u128(71);
    let harness = RouteEstablishmentHarness::new(local_member, memberships);
    let nonce = harness.probe_manual_route(
        SocketId(91),
        local_endpoint,
        [watched_udp_route(remote_route, Some(remote_member.clone()))],
        remote_route,
    );
    let payload =
        IntroductionSpec::new(&remote_member, remote_instance, remote_route, [group_id(1)])
            .encode(nonce);

    harness.receive_transport(remote_route, payload);

    harness.expect_peer_route_update(&remote_member, &[remote_route], Some(local_endpoint));
    harness.shutdown();
}

#[test]
fn manual_route_watch_without_expected_member_publishes_verified_group_member() {
    let local_member = member(["alice"]);
    let remote_member = member(["bob"]);
    let memberships = shared_memberships(&local_member, &remote_member);
    let local_endpoint = SocketAddr::from(([127, 0, 0, 1], 49116));
    let remote_route = SocketAddr::from(([127, 0, 0, 1], 62177));
    let remote_instance = Uuid::from_u128(77);
    let harness = RouteEstablishmentHarness::new(local_member, memberships);
    let nonce = harness.probe_manual_route(
        SocketId(97),
        local_endpoint,
        [watched_udp_route(remote_route, None)],
        remote_route,
    );
    let payload =
        IntroductionSpec::new(&remote_member, remote_instance, remote_route, [group_id(1)])
            .encode(nonce);

    harness.receive_transport(remote_route, payload);

    harness.expect_peer_route_update(&remote_member, &[remote_route], Some(local_endpoint));
    harness.shutdown();
}

#[test]
fn manual_route_watch_unions_constrained_duplicate_members() {
    let local_member = member(["alice"]);
    let first_expected_member = member(["bob"]);
    let other_member = member(["charlie"]);
    let memberships = single_group_memberships([
        local_member.clone(),
        first_expected_member.clone(),
        other_member.clone(),
    ]);
    let local_endpoint = SocketAddr::from(([127, 0, 0, 1], 49117));
    let remote_route = SocketAddr::from(([127, 0, 0, 1], 62178));
    let remote_instance = Uuid::from_u128(78);
    let harness = RouteEstablishmentHarness::new(local_member, memberships);
    let nonce = harness.probe_manual_route(
        SocketId(98),
        local_endpoint,
        [
            watched_udp_route(remote_route, Some(first_expected_member)),
            watched_udp_route(remote_route, Some(other_member.clone())),
        ],
        remote_route,
    );
    let payload =
        IntroductionSpec::new(&other_member, remote_instance, remote_route, [group_id(1)])
            .encode(nonce);

    harness.receive_transport(remote_route, payload);

    harness.expect_peer_route_update(&other_member, &[remote_route], Some(local_endpoint));
    harness.shutdown();
}

#[test]
fn manual_route_watch_rejects_conflicting_member_filters_without_changing_existing_watch() {
    let local_member = member(["alice"]);
    let expected_member = member(["bob"]);
    let other_member = member(["charlie"]);
    let memberships = single_group_memberships([
        local_member.clone(),
        expected_member.clone(),
        other_member.clone(),
    ]);
    let local_endpoint = SocketAddr::from(([127, 0, 0, 1], 49120));
    let remote_route = SocketAddr::from(([127, 0, 0, 1], 62181));
    let remote_instance = Uuid::from_u128(81);
    let harness = RouteEstablishmentHarness::new(local_member, memberships);
    harness
        .replace_manual_route_watches([watched_udp_route(
            remote_route,
            Some(expected_member.clone()),
        )])
        .expect("initial constrained manual watch should succeed");

    let result = harness.replace_manual_route_watches([
        watched_udp_route(remote_route, Some(expected_member)),
        watched_udp_route(remote_route, None),
    ]);

    assert_eq!(
        result,
        Err(ManualRouteWatchError::ConflictingMemberFilters {
            route: DiscoveryRoute::Udp(remote_route),
        })
    );
    harness.bind_endpoint(SocketId(101), local_endpoint);
    let nonce = harness.expect_transport_probe_with_nonce(local_endpoint, remote_route);
    let payload =
        IntroductionSpec::new(&other_member, remote_instance, remote_route, [group_id(1)])
            .encode(nonce);
    harness.receive_transport(remote_route, payload);
    harness.expect_no_route_update("failed replacement must not widen the existing watch");
    harness.shutdown();
}

#[test]
fn manual_route_watch_constraint_overrides_peer_announcement() {
    let local_member = member(["alice"]);
    let expected_member = member(["bob"]);
    let other_member = member(["charlie"]);
    let memberships = single_group_memberships([
        local_member.clone(),
        expected_member.clone(),
        other_member.clone(),
    ]);
    let local_endpoint = SocketAddr::from(([127, 0, 0, 1], 49121));
    let remote_route = SocketAddr::from(([127, 0, 0, 1], 62182));
    let remote_instance = Uuid::from_u128(82);
    let harness = RouteEstablishmentHarness::new(local_member, memberships);

    harness.observe_peer_route(Uuid::from_u128(8200), remote_route);
    let nonce = harness.probe_manual_route(
        SocketId(102),
        local_endpoint,
        [watched_udp_route(remote_route, Some(expected_member))],
        remote_route,
    );
    let payload =
        IntroductionSpec::new(&other_member, remote_instance, remote_route, [group_id(1)])
            .encode(nonce);

    harness.receive_transport(remote_route, payload);

    harness.expect_no_route_update("peer announcement must not bypass a manual member constraint");
    harness.shutdown();
}

#[test]
fn manual_route_watch_rejects_unexpected_member() {
    let local_member = member(["alice"]);
    let expected_member = member(["bob"]);
    let unexpected_member = member(["charlie"]);
    let memberships = single_group_memberships([
        local_member.clone(),
        expected_member.clone(),
        unexpected_member.clone(),
    ]);
    let local_endpoint = SocketAddr::from(([127, 0, 0, 1], 49111));
    let remote_route = SocketAddr::from(([127, 0, 0, 1], 62171));
    let remote_instance = Uuid::from_u128(72);
    let harness = RouteEstablishmentHarness::new(local_member, memberships);
    let nonce = harness.probe_manual_route(
        SocketId(92),
        local_endpoint,
        [watched_udp_route(remote_route, Some(expected_member))],
        remote_route,
    );
    let payload = IntroductionSpec::new(
        &unexpected_member,
        remote_instance,
        remote_route,
        [group_id(1)],
    )
    .encode(nonce);

    harness.receive_transport(remote_route, payload);

    harness.expect_no_route_update("unexpected member should not publish a watched route");
    harness.shutdown();
}

#[test]
fn manual_route_watch_rejects_unverifiable_claim() {
    let local_member = member(["alice"]);
    let remote_member = member(["bob"]);
    let memberships = shared_memberships(&local_member, &remote_member);
    let local_endpoint = SocketAddr::from(([127, 0, 0, 1], 49112));
    let remote_route = SocketAddr::from(([127, 0, 0, 1], 62172));
    let remote_instance = Uuid::from_u128(73);
    let harness = RouteEstablishmentHarness::with_credentials(
        local_member,
        memberships,
        Arc::new(RouteEstablishmentTestCredentials::reject_claim_verification()),
    );
    let nonce = harness.probe_manual_route(
        SocketId(93),
        local_endpoint,
        [watched_udp_route(remote_route, Some(remote_member.clone()))],
        remote_route,
    );
    let payload =
        IntroductionSpec::new(&remote_member, remote_instance, remote_route, [group_id(1)])
            .encode(nonce);

    harness.receive_transport(remote_route, payload);

    harness.expect_no_route_update("unverifiable claim should not publish a watched route");
    harness.shutdown();
}

#[test]
fn manual_route_watch_reports_missing_key_material_without_publishing_route() {
    let local_member = member(["alice"]);
    let remote_member = member(["bob"]);
    let memberships = shared_memberships(&local_member, &remote_member);
    let local_endpoint = SocketAddr::from(([127, 0, 0, 1], 49123));
    let remote_route = SocketAddr::from(([127, 0, 0, 1], 62184));
    let remote_instance = Uuid::from_u128(84);
    let harness = RouteEstablishmentHarness::with_credentials(
        local_member,
        memberships,
        Arc::new(RouteEstablishmentTestCredentials::missing_key_material()),
    );
    let nonce = harness.probe_manual_route(
        SocketId(104),
        local_endpoint,
        [watched_udp_route(remote_route, Some(remote_member.clone()))],
        remote_route,
    );
    let payload =
        IntroductionSpec::new(&remote_member, remote_instance, remote_route, [group_id(1)])
            .encode(nonce);

    harness.receive_transport(remote_route, payload);

    harness.expect_fetch_key_material_request(
        remote_route,
        &remote_member,
        TEST_DISCOVERY_KEY_FINGERPRINT,
    );
    harness.expect_no_route_update("missing key material should not publish a watched route");
    harness.shutdown();
}

#[test]
fn manual_route_watch_rejects_claim_without_route_publication_permission() {
    let local_member = member(["alice"]);
    let remote_member = member(["bob"]);
    let memberships = shared_memberships(&local_member, &remote_member);
    let local_endpoint = SocketAddr::from(([127, 0, 0, 1], 49122));
    let remote_route = SocketAddr::from(([127, 0, 0, 1], 62183));
    let remote_instance = Uuid::from_u128(83);
    let harness = RouteEstablishmentHarness::with_credentials(
        local_member,
        memberships,
        Arc::new(RouteEstablishmentTestCredentials::reject_route_publication()),
    );
    let nonce = harness.probe_manual_route(
        SocketId(103),
        local_endpoint,
        [watched_udp_route(remote_route, Some(remote_member.clone()))],
        remote_route,
    );
    let payload =
        IntroductionSpec::new(&remote_member, remote_instance, remote_route, [group_id(1)])
            .encode(nonce);

    harness.receive_transport(remote_route, payload);

    harness.expect_no_route_update(
        "claim without route-publication permission should not publish a watched route",
    );
    harness.shutdown();
}

#[test]
fn manual_route_watch_rejects_nonce_mismatch() {
    let local_member = member(["alice"]);
    let remote_member = member(["bob"]);
    let memberships = shared_memberships(&local_member, &remote_member);
    let local_endpoint = SocketAddr::from(([127, 0, 0, 1], 49113));
    let remote_route = SocketAddr::from(([127, 0, 0, 1], 62173));
    let remote_instance = Uuid::from_u128(74);
    let harness = RouteEstablishmentHarness::new(local_member, memberships);
    let nonce = harness.probe_manual_route(
        SocketId(94),
        local_endpoint,
        [watched_udp_route(remote_route, Some(remote_member.clone()))],
        remote_route,
    );
    let payload =
        IntroductionSpec::new(&remote_member, remote_instance, remote_route, [group_id(1)])
            .with_top_level_nonce(Uuid::from_u128(7400))
            .encode(nonce);

    harness.receive_transport(remote_route, payload);

    harness.expect_no_route_update("nonce mismatch should not publish a watched route");
    harness.shutdown();
}

#[test]
fn manual_route_watch_rejects_claim_payload_nonce_mismatch() {
    let local_member = member(["alice"]);
    let remote_member = member(["bob"]);
    let memberships = shared_memberships(&local_member, &remote_member);
    let local_endpoint = SocketAddr::from(([127, 0, 0, 1], 49118));
    let remote_route = SocketAddr::from(([127, 0, 0, 1], 62179));
    let remote_instance = Uuid::from_u128(79);
    let harness = RouteEstablishmentHarness::new(local_member, memberships);
    let nonce = harness.probe_manual_route(
        SocketId(99),
        local_endpoint,
        [watched_udp_route(remote_route, Some(remote_member.clone()))],
        remote_route,
    );
    let payload =
        IntroductionSpec::new(&remote_member, remote_instance, remote_route, [group_id(1)])
            .with_claim_nonce(Uuid::from_u128(7900))
            .encode(nonce);

    harness.receive_transport(remote_route, payload);

    harness
        .expect_no_route_update("claim payload nonce mismatch should not publish a watched route");
    harness.shutdown();
}

#[test]
fn manual_route_watch_rejects_claim_payload_instance_mismatch() {
    let local_member = member(["alice"]);
    let remote_member = member(["bob"]);
    let memberships = shared_memberships(&local_member, &remote_member);
    let local_endpoint = SocketAddr::from(([127, 0, 0, 1], 49119));
    let remote_route = SocketAddr::from(([127, 0, 0, 1], 62180));
    let remote_instance = Uuid::from_u128(80);
    let mismatched_claim_instance = Uuid::from_u128(8000);
    let harness = RouteEstablishmentHarness::new(local_member, memberships);
    let nonce = harness.probe_manual_route(
        SocketId(100),
        local_endpoint,
        [watched_udp_route(remote_route, Some(remote_member.clone()))],
        remote_route,
    );
    let payload =
        IntroductionSpec::new(&remote_member, remote_instance, remote_route, [group_id(1)])
            .with_claim_instance(mismatched_claim_instance)
            .encode(nonce);

    harness.receive_transport(remote_route, payload);

    harness.expect_no_route_update(
        "claim payload instance mismatch should not publish a watched route",
    );
    harness.shutdown();
}

#[test]
fn manual_route_watch_rejects_claimed_route_mismatch() {
    let local_member = member(["alice"]);
    let remote_member = member(["bob"]);
    let memberships = shared_memberships(&local_member, &remote_member);
    let local_endpoint = SocketAddr::from(([127, 0, 0, 1], 49114));
    let remote_route = SocketAddr::from(([127, 0, 0, 1], 62174));
    let claimed_route = SocketAddr::from(([127, 0, 0, 1], 62175));
    let remote_instance = Uuid::from_u128(75);
    let harness = RouteEstablishmentHarness::new(local_member, memberships);
    let nonce = harness.probe_manual_route(
        SocketId(95),
        local_endpoint,
        [watched_udp_route(remote_route, Some(remote_member.clone()))],
        remote_route,
    );
    let payload =
        IntroductionSpec::new(&remote_member, remote_instance, remote_route, [group_id(1)])
            .with_claimed_route(claimed_route)
            .encode(nonce);

    harness.receive_transport(remote_route, payload);

    harness.expect_no_route_update("route mismatch should not publish a watched route");
    harness.shutdown();
}
