//! Tests for route configuration and introduction-claim handling.

use super::{fixtures::*, *};

#[test]
fn config_rejects_wildcard_advertised_route() {
    let result = RouteEstablishmentConfig::new()
        .with_advertised_routes([SocketAddr::from(([0, 0, 0, 0], 52156))]);

    assert!(matches!(
        result,
        Err(RouteEstablishmentConfigError::InvalidAdvertisedRoute { .. })
    ));
}

#[test]
fn config_rejects_port_zero_advertised_route() {
    let result = RouteEstablishmentConfig::new()
        .with_advertised_routes([SocketAddr::from(([127, 0, 0, 1], 0))]);

    assert!(matches!(
        result,
        Err(RouteEstablishmentConfigError::InvalidAdvertisedRoute { .. })
    ));
}

#[test]
fn config_accepts_concrete_advertised_route() {
    let route = SocketAddr::from(([127, 0, 0, 1], 52156));
    let config = RouteEstablishmentConfig::new()
        .with_advertised_routes([route])
        .expect("concrete route should be accepted");

    assert_eq!(
        config
            .advertised_routes()
            .routes()
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![route]
    );
}

#[test]
fn local_claim_groups_only_include_groups_hosted_by_local_member() {
    let local_member = member(["alice"]);
    let remote_member = member(["bob"]);
    let local_group = group_id(1);
    let remote_only_group = group_id(2);
    let memberships = GroupMemberships::from_groups([
        (
            local_group,
            group_members([local_member.clone(), remote_member.clone()]),
        ),
        (remote_only_group, group_members([remote_member])),
    ]);
    let memberships = SharedGroupMemberships::new(memberships);

    let advertised_groups = local_claim_group_ids(&memberships, &local_member);

    assert_eq!(advertised_groups, vec![local_group]);
}

#[test]
fn endpoint_selection_port_updates_introduction_claim_routes() {
    let local_member = member(["alice"]);
    let remote_member = member(["bob"]);
    let memberships = shared_memberships(&local_member, &remote_member);
    let local_endpoint = SocketAddr::from(([0, 0, 0, 0], 45_100));
    let selected_endpoint = SocketAddr::from(([192, 168, 1, 20], 45_100));
    let remote_route = SocketAddr::from(([127, 0, 0, 1], 62_100));
    let request_nonce = Uuid::from_u128(42_100);
    let harness = RouteEstablishmentHarness::new(local_member.clone(), memberships);

    harness.publish_endpoint_selection_and_wait_until_applied([selected_endpoint]);
    harness.bind_endpoint(SocketId(42), local_endpoint);
    let frame = DiscoveryEndpointFrameView::IntroductionRequest { request_nonce }.encode_proto();
    let payload = endpoint_payload(&frame);
    harness.receive_transport(remote_route, payload);
    let response = harness.recv_transport_submit();

    assert_introduction_claims_route(
        &response,
        local_endpoint,
        remote_route,
        &local_member,
        TEST_DISCOVERY_KEY_FINGERPRINT,
        request_nonce,
        selected_endpoint,
    );
    harness.shutdown();
}

#[test]
fn oversized_introduction_response_is_submitted_through_route_transport() {
    let local_member = member(["alice"]);
    let remote_member = member(["bob"]);
    let memberships = many_shared_group_memberships(&local_member, &remote_member, 128);
    let local_endpoint = SocketAddr::from(([0, 0, 0, 0], 45_101));
    let selected_endpoint = SocketAddr::from(([192, 168, 1, 21], 45_101));
    let remote_route = SocketAddr::from(([127, 0, 0, 1], 62_101));
    let request_nonce = Uuid::from_u128(42_101);
    let harness = RouteEstablishmentHarness::new(local_member.clone(), memberships);

    harness.publish_endpoint_selection_and_wait_until_applied([selected_endpoint]);
    harness.bind_endpoint(SocketId(43), local_endpoint);
    let frame = DiscoveryEndpointFrameView::IntroductionRequest { request_nonce }.encode_proto();
    let payload = endpoint_payload(&frame);
    harness.receive_transport(remote_route, payload);
    let response = harness.recv_transport_submit();

    assert_introduction_claims_route(
        &response,
        local_endpoint,
        remote_route,
        &local_member,
        TEST_DISCOVERY_KEY_FINGERPRINT,
        request_nonce,
        selected_endpoint,
    );
    let response_payload = encode_transport_payload(&response.payload);
    assert!(
        response_payload.len() > MAX_UDP_PAYLOAD_BYTES,
        "expected introduction response to exceed one UDP datagram; response_len={}, max_udp_payload={MAX_UDP_PAYLOAD_BYTES}",
        response_payload.len(),
    );
    harness.shutdown();
}

#[test]
fn verified_claim_acceptance_uses_group_membership_snapshot() {
    let local_member = member(["alice"]);
    let remote_member = member(["bob"]);
    let unknown_member = member(["charlie"]);
    let shared_group = group_id(1);
    let unrelated_group = group_id(2);
    let memberships = GroupMemberships::from_groups([(
        shared_group,
        group_members([local_member.clone(), remote_member.clone()]),
    )]);
    let memberships = SharedGroupMemberships::new(memberships);
    let matching_claim = HashSet::from([shared_group]);
    let unrelated_claim = HashSet::from([unrelated_group]);

    let snapshot = memberships.snapshot();

    assert!(claim_matches_group_memberships(
        snapshot.as_ref(),
        &remote_member,
        &matching_claim,
    ));
    assert!(!claim_matches_group_memberships(
        snapshot.as_ref(),
        &remote_member,
        &unrelated_claim,
    ));
    assert!(!claim_matches_group_memberships(
        snapshot.as_ref(),
        &unknown_member,
        &matching_claim,
    ));
}
