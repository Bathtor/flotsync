//! Tests for published-route withdrawal and endpoint lifecycle changes.

use super::{fixtures::*, *};

#[test]
fn clearing_manual_route_watch_withdraws_published_route() {
    let local_member = member(["alice"]);
    let remote_member = member(["bob"]);
    let memberships = shared_memberships(&local_member, &remote_member);
    let local_endpoint = SocketAddr::from(([127, 0, 0, 1], 49115));
    let remote_route = SocketAddr::from(([127, 0, 0, 1], 62176));
    let remote_instance = Uuid::from_u128(76);
    let harness = RouteEstablishmentHarness::new(local_member, memberships);
    let nonce = harness.probe_manual_route(
        SocketId(96),
        local_endpoint,
        [watched_udp_route(remote_route, Some(remote_member.clone()))],
        remote_route,
    );
    let payload =
        IntroductionSpec::new(&remote_member, remote_instance, remote_route, [group_id(1)])
            .encode(nonce);
    harness.receive_transport(remote_route, payload);
    harness.expect_peer_route_update(&remote_member, &[remote_route], Some(local_endpoint));

    harness.clear_manual_route_watches();

    harness.expect_peer_route_update(&remote_member, &[], Some(local_endpoint));
    harness.shutdown();
}

#[test]
fn endpoint_rebinding_withdraws_routes_and_reprobes() {
    let local_member = member(["alice"]);
    let remote_member = member(["bob"]);
    let memberships = shared_memberships(&local_member, &remote_member);
    let first_local_endpoint = SocketAddr::from(([127, 0, 0, 1], 49102));
    let second_local_endpoint = SocketAddr::from(([127, 0, 0, 1], 49103));
    let remote_route = SocketAddr::from(([127, 0, 0, 1], 62158));
    let instance_id = Uuid::from_u128(42);
    let harness = RouteEstablishmentHarness::new(local_member, memberships);

    harness.observe_peer_route(instance_id, remote_route);
    harness.bind_endpoint(SocketId(83), first_local_endpoint);
    harness.expect_transport_probe(first_local_endpoint, remote_route);
    harness.mark_route_reachable(remote_route, [remote_member.clone()]);
    harness.expect_peer_route_update(&remote_member, &[remote_route], Some(first_local_endpoint));

    harness.close_endpoint(SocketId(83), first_local_endpoint);
    harness.expect_peer_route_update(&remote_member, &[], None);

    harness.bind_endpoint(SocketId(84), second_local_endpoint);
    harness.expect_transport_probe(second_local_endpoint, remote_route);
    harness.shutdown();
}

#[test]
fn published_routes_are_rebuilt_from_current_reachable_state() {
    let local_member = member(["alice"]);
    let remote_member = member(["bob"]);
    let memberships = shared_memberships(&local_member, &remote_member);
    let local_endpoint = SocketAddr::from(([127, 0, 0, 1], 49104));
    let remote_route = SocketAddr::from(([127, 0, 0, 1], 62159));
    let first_instance = Uuid::from_u128(51);
    let second_instance = Uuid::from_u128(52);
    let harness = RouteEstablishmentHarness::new(local_member, memberships);

    harness.observe_peer_route(first_instance, remote_route);
    harness.observe_peer_route(second_instance, remote_route);
    harness.bind_endpoint(SocketId(85), local_endpoint);
    harness.expect_transport_probe(local_endpoint, remote_route);
    harness.expect_no_transport_submit(
        "duplicate announcements for one route should share a single route state",
    );
    harness.mark_route_reachable(remote_route, [remote_member.clone()]);
    harness.expect_peer_route_update(&remote_member, &[remote_route], Some(local_endpoint));
    harness.expect_no_route_update(
        "adding the same reachable route from another instance should not republish",
    );

    harness.mark_route_stale(remote_route);

    harness.expect_peer_route_update(&remote_member, &[], None);
    harness.shutdown();
}
