use super::{
    component::{claim_matches_group_memberships, local_claim_group_ids},
    observation::SocketState,
    *,
};
use crate::{
    DEFAULT_DISCOVERY_PORT,
    config_keys,
    route_publication::{DiscoveryRoutePort, DiscoveryRouteUpdate},
};
use flotsync_core::{
    GroupId,
    member::{Identifier, TrieSet},
    membership::{GroupMembers, GroupMemberships, SharedGroupMemberships},
};
use flotsync_io::{
    prelude::{
        EgressPool,
        IoBufferConfig,
        IoBufferPools,
        SocketId,
        UdpBindOptions,
        UdpCloseReason,
        UdpIndication,
        UdpLocalBind,
        UdpOpenRequestId,
        UdpPort,
        UdpRequest,
    },
    test_support::{
        build_test_kompact_system,
        build_test_kompact_system_with,
        kill_component,
        recv_until,
        start_component,
    },
};
use flotsync_security::{FrameSignature, SIGNATURE_LENGTH};
use flotsync_utils::BoxError;
use kompact::prelude::*;
use std::{
    collections::HashSet,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, mpsc},
    time::Duration,
};
use uuid::Uuid;

fn member<const N: usize>(segments: [&str; N]) -> MemberIdentity {
    Identifier::from_array(segments)
}

fn group_id(value: u128) -> GroupId {
    GroupId(Uuid::from_u128(value))
}

fn group_members(members: impl IntoIterator<Item = MemberIdentity>) -> GroupMembers {
    GroupMembers::from_ordered_members(members).expect("group members should build")
}

fn member_set(members: impl IntoIterator<Item = MemberIdentity>) -> TrieSet {
    let mut set = TrieSet::new();
    for member in members {
        set.insert(member);
    }
    set
}

#[derive(ComponentDefinition)]
struct UdpRequestProbe {
    ctx: ComponentContext<Self>,
    udp: ProvidedPort<UdpPort>,
    requests: mpsc::Sender<UdpRequest>,
}

impl UdpRequestProbe {
    fn new(requests: mpsc::Sender<UdpRequest>) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            udp: ProvidedPort::uninitialised(),
            requests,
        }
    }
}

ignore_lifecycle!(UdpRequestProbe);

impl Provide<UdpPort> for UdpRequestProbe {
    fn handle(&mut self, request: UdpRequest) -> HandlerResult {
        self.requests
            .send(request)
            .expect("UDP request receiver must stay live during tests");
        Handled::OK
    }
}

impl Actor for UdpRequestProbe {
    type Message = Never;

    fn receive_local(&mut self, message: Self::Message) -> HandlerResult {
        match message {}
    }
}

#[derive(ComponentDefinition)]
struct DiscoveryRouteUpdateProbe {
    ctx: ComponentContext<Self>,
    discovery: RequiredPort<DiscoveryRoutePort>,
    updates: mpsc::Sender<DiscoveryRouteUpdate>,
}

impl DiscoveryRouteUpdateProbe {
    fn new(updates: mpsc::Sender<DiscoveryRouteUpdate>) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            discovery: RequiredPort::uninitialised(),
            updates,
        }
    }
}

ignore_lifecycle!(DiscoveryRouteUpdateProbe);

impl Require<DiscoveryRoutePort> for DiscoveryRouteUpdateProbe {
    fn handle(&mut self, update: DiscoveryRouteUpdate) -> HandlerResult {
        self.updates
            .send(update)
            .expect("route update receiver must stay live during tests");
        Handled::OK
    }
}

impl Actor for DiscoveryRouteUpdateProbe {
    type Message = Never;

    fn receive_local(&mut self, message: Self::Message) -> HandlerResult {
        match message {}
    }
}

struct NoopDiscoveryCredentials;

impl DiscoveryCredentials for NoopDiscoveryCredentials {
    fn sign_discovery_claim_payload(&self, _payload: &[u8]) -> Result<FrameSignature, BoxError> {
        Ok(FrameSignature::from_bytes([0; SIGNATURE_LENGTH]))
    }

    fn verify_discovery_claim_payload<'a>(
        &'a self,
        _member: &'a MemberIdentity,
        _payload: &'a [u8],
        _signature: &'a FrameSignature,
    ) -> DiscoveryCredentialFuture<'a> {
        Box::pin(std::future::ready(Ok(())))
    }
}

fn shared_memberships(
    local_member: &MemberIdentity,
    remote_member: &MemberIdentity,
) -> SharedGroupMemberships {
    SharedGroupMemberships::new(GroupMemberships::from_groups([(
        group_id(1),
        group_members([local_member.clone(), remote_member.clone()]),
    )]))
}

fn route_establishment_component(
    local_member: MemberIdentity,
    group_memberships: SharedGroupMemberships,
) -> RouteEstablishmentComponent {
    RouteEstablishmentComponent::new(
        RouteEstablishmentConfig::new(SocketAddr::from(([127, 0, 0, 1], 52157)))
            .with_instance_id(Uuid::from_u128(11)),
        local_member,
        Arc::new(NoopDiscoveryCredentials),
        group_memberships,
        test_egress_pool(),
    )
}

fn observe_peer_route(component: &mut RouteEstablishmentComponent, key: RouteProbeKey) {
    component.record_peer_announcement(PeerAnnouncementObserved {
        instance_id: key.instance_id,
        routes: vec![DiscoveryRoute::Udp(key.route)],
    });
}

fn assert_probe_send(
    requests_rx: &mpsc::Receiver<UdpRequest>,
    expected_socket_id: SocketId,
    expected_target: SocketAddr,
) {
    match recv_until(requests_rx, |request| {
        matches!(request, UdpRequest::Send { .. })
    }) {
        UdpRequest::Send {
            socket_id, target, ..
        } => {
            assert_eq!(socket_id, expected_socket_id);
            assert_eq!(target, Some(expected_target));
        }
        other => unreachable!("filtered to send request, got {other:?}"),
    }
}

fn test_egress_pool() -> EgressPool {
    IoBufferPools::new(IoBufferConfig::default())
        .expect("test IO buffers should build")
        .egress()
}

fn assert_peer_route_update(
    update: DiscoveryRouteUpdate,
    expected_peer: &MemberIdentity,
    expected_routes: &[SocketAddr],
    expected_local_bind: Option<SocketAddr>,
) {
    match update {
        DiscoveryRouteUpdate::PeerRoutes { peer, routes } => {
            assert_eq!(&peer, expected_peer);
            let actual_routes = routes
                .into_iter()
                .map(|candidate| {
                    assert_eq!(candidate.local_bind, expected_local_bind);
                    match candidate.route {
                        DiscoveryRoute::Udp(route) => route,
                    }
                })
                .collect::<Vec<_>>();
            assert_eq!(actual_routes, expected_routes);
        }
    }
}

#[test]
fn config_rejects_wildcard_advertised_route() {
    let result = RouteEstablishmentConfig::new(SocketAddr::from(([127, 0, 0, 1], 52156)))
        .with_advertised_routes([SocketAddr::from(([0, 0, 0, 0], 52156))]);

    assert!(matches!(
        result,
        Err(RouteEstablishmentConfigError::InvalidAdvertisedRoute { .. })
    ));
}

#[test]
fn config_rejects_port_zero_advertised_route() {
    let result = RouteEstablishmentConfig::new(SocketAddr::from(([127, 0, 0, 1], 52156)))
        .with_advertised_routes([SocketAddr::from(([127, 0, 0, 1], 0))]);

    assert!(matches!(
        result,
        Err(RouteEstablishmentConfigError::InvalidAdvertisedRoute { .. })
    ));
}

#[test]
fn config_accepts_concrete_advertised_route() {
    let route = SocketAddr::from(([127, 0, 0, 1], 52156));
    let config = RouteEstablishmentConfig::new(route)
        .with_advertised_routes([route])
        .expect("concrete route should be accepted");

    assert_eq!(
        config
            .advertised_routes()
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![route]
    );
}

#[test]
fn combined_peer_announcement_setup_rejects_bind_port_mismatch() {
    let mut config = RouteEstablishmentConfig::new(SocketAddr::from(([127, 0, 0, 1], 52157)));
    config.peer_announcement_bind_addr = SocketAddr::new(
        IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        *DEFAULT_DISCOVERY_PORT + 1,
    );

    let result =
        peer_announcement_and_observation_components(PeerAnnouncementOptions::DEFAULT, config);

    assert_eq!(
        result.err(),
        Some(PeerAnnouncementSetupError::BindPortMismatch {
            sender_bind_port: *DEFAULT_DISCOVERY_PORT,
            observer_bind_port: *DEFAULT_DISCOVERY_PORT + 1,
        })
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

#[test]
fn observing_peer_announcement_component_infers_socket_id_by_configured_port() {
    let peer_port = 53156;
    let mut config = RouteEstablishmentConfig::new(SocketAddr::from(([127, 0, 0, 1], 52157)));
    config.peer_announcement_bind_addr =
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), peer_port);
    config.instance_id = Uuid::from_u128(9);
    let system = build_test_kompact_system();
    let component = system.create(move || {
        PeerAnnouncementObservationComponent::with_socket_maintenance(
            config,
            PeerAnnouncementSocketMaintenance::Observe,
        )
    });

    component.on_definition(|component| {
        let _handled = <PeerAnnouncementObservationComponent as Require<UdpPort>>::handle(
            component,
            UdpIndication::Bound {
                request_id: UdpOpenRequestId::new(),
                socket_id: SocketId(77),
                local_addr: SocketAddr::from(([127, 0, 0, 1], peer_port)),
            },
        )
        .expect("bound indication should be handled");

        assert_eq!(
            component.socket_state(),
            &SocketState::Listening {
                socket_id: SocketId(77)
            }
        );

        let _handled = <PeerAnnouncementObservationComponent as Require<UdpPort>>::handle(
            component,
            UdpIndication::Closed {
                socket_id: SocketId(77),
                remote_addr: None,
                reason: UdpCloseReason::Requested,
            },
        )
        .expect("close indication should be handled");

        assert_eq!(component.socket_state(), &SocketState::WaitingForSocket);
    });

    system.shutdown().wait().expect("Kompact shutdown");
}

#[test]
fn observing_peer_announcement_component_ignores_other_bound_ports() {
    let peer_port = 53157;
    let mut config = RouteEstablishmentConfig::new(SocketAddr::from(([127, 0, 0, 1], 52157)));
    config.peer_announcement_bind_addr =
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), peer_port);
    config.instance_id = Uuid::from_u128(10);
    let system = build_test_kompact_system();
    let component = system.create(move || {
        PeerAnnouncementObservationComponent::with_socket_maintenance(
            config,
            PeerAnnouncementSocketMaintenance::Observe,
        )
    });

    component.on_definition(|component| {
        let _handled = <PeerAnnouncementObservationComponent as Require<UdpPort>>::handle(
            component,
            UdpIndication::Bound {
                request_id: UdpOpenRequestId::new(),
                socket_id: SocketId(78),
                local_addr: SocketAddr::from(([127, 0, 0, 1], peer_port + 1)),
            },
        )
        .expect("bound indication should be handled");

        assert_eq!(component.socket_state(), &SocketState::WaitingForSocket);
    });

    system.shutdown().wait().expect("Kompact shutdown");
}

#[test]
fn observing_peer_announcement_maintainer_uses_shared_bind_reuse_config() {
    let peer_port = 53158;
    let mut config = RouteEstablishmentConfig::new(SocketAddr::from(([127, 0, 0, 1], 52157)));
    config.peer_announcement_bind_addr =
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), peer_port);
    let system = build_test_kompact_system_with(|config| {
        config.set_config_value(&config_keys::PEER_ANNOUNCEMENT_BIND_REUSE_ADDRESS, false);
    });
    let (requests_tx, requests_rx) = mpsc::channel();
    let probe = system.create(move || UdpRequestProbe::new(requests_tx));
    let component = system.create(move || {
        PeerAnnouncementObservationComponent::with_socket_maintenance(
            config,
            PeerAnnouncementSocketMaintenance::Maintain,
        )
    });
    biconnect_components::<UdpPort, _, _>(&probe, &component).expect("connect probe/component");

    start_component(&system, &probe);
    start_component(&system, &component);

    match recv_until(&requests_rx, |request| {
        matches!(request, UdpRequest::Bind { .. })
    }) {
        UdpRequest::Bind { bind, options, .. } => {
            assert_eq!(
                bind,
                UdpLocalBind::Exact(SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                    peer_port
                ))
            );
            assert_eq!(options, UdpBindOptions::default().with_socket_reuse(false));
        }
        other => unreachable!("filtered to bind request, got {other:?}"),
    }

    kill_component(&system, component);
    kill_component(&system, probe);
    system.shutdown().wait().expect("Kompact shutdown");
}

#[test]
fn route_establishment_ignores_raw_udp_bound_indications_for_endpoint() {
    let local_member = member(["alice"]);
    let remote_member = member(["bob"]);
    let memberships = shared_memberships(&local_member, &remote_member);
    let system = build_test_kompact_system();
    let component = system.create(move || route_establishment_component(local_member, memberships));

    component.on_definition(|component| {
        let _handled = component
            .handle_udp_indication(UdpIndication::Bound {
                request_id: UdpOpenRequestId::new(),
                socket_id: SocketId(81),
                local_addr: SocketAddr::from(([127, 0, 0, 1], 49100)),
            })
            .expect("bound indication should be handled");

        assert_eq!(component.local_endpoint().binding(), None);
    });

    system.shutdown().wait().expect("Kompact shutdown");
}

#[test]
fn endpoint_binding_report_probes_inactive_routes() {
    let local_member = member(["alice"]);
    let remote_member = member(["bob"]);
    let memberships = shared_memberships(&local_member, &remote_member);
    let remote_route = SocketAddr::from(([127, 0, 0, 1], 62157));
    let key = RouteProbeKey {
        instance_id: Uuid::from_u128(41),
        route: remote_route,
    };
    let system = build_test_kompact_system();
    let (requests_tx, requests_rx) = mpsc::channel();
    let udp_probe = system.create(move || UdpRequestProbe::new(requests_tx));
    let component = system.create(move || route_establishment_component(local_member, memberships));
    biconnect_components::<UdpPort, _, _>(&udp_probe, &component).expect("connect UDP probe");

    start_component(&system, &udp_probe);
    start_component(&system, &component);

    component.on_definition(|component| {
        observe_peer_route(component, key);
    });
    component
        .actor_ref()
        .tell(RouteEstablishmentMessage::LocalEndpointBound {
            socket_id: SocketId(82),
            local_addr: SocketAddr::from(([127, 0, 0, 1], 49101)),
        });

    assert_probe_send(&requests_rx, SocketId(82), remote_route);

    kill_component(&system, component);
    kill_component(&system, udp_probe);
    system.shutdown().wait().expect("Kompact shutdown");
}

#[test]
fn endpoint_close_withdraws_routes_and_rebinding_reprobes() {
    let local_member = member(["alice"]);
    let remote_member = member(["bob"]);
    let memberships = shared_memberships(&local_member, &remote_member);
    let first_local_endpoint = SocketAddr::from(([127, 0, 0, 1], 49102));
    let second_local_endpoint = SocketAddr::from(([127, 0, 0, 1], 49103));
    let remote_route = SocketAddr::from(([127, 0, 0, 1], 62158));
    let key = RouteProbeKey {
        instance_id: Uuid::from_u128(42),
        route: remote_route,
    };
    let system = build_test_kompact_system();
    let (requests_tx, requests_rx) = mpsc::channel();
    let udp_probe = system.create(move || UdpRequestProbe::new(requests_tx));
    let (updates_tx, updates_rx) = mpsc::channel();
    let update_probe = system.create(move || DiscoveryRouteUpdateProbe::new(updates_tx));
    let expected_remote_member = remote_member.clone();
    let component = system.create(move || route_establishment_component(local_member, memberships));
    biconnect_components::<UdpPort, _, _>(&udp_probe, &component).expect("connect UDP probe");
    biconnect_components::<DiscoveryRoutePort, _, _>(&component, &update_probe)
        .expect("connect route update probe");

    start_component(&system, &udp_probe);
    start_component(&system, &update_probe);
    start_component(&system, &component);

    component.on_definition(|component| observe_peer_route(component, key));
    component
        .actor_ref()
        .tell(RouteEstablishmentMessage::LocalEndpointBound {
            socket_id: SocketId(83),
            local_addr: first_local_endpoint,
        });
    assert_probe_send(&requests_rx, SocketId(83), remote_route);
    component.on_definition(|component| {
        component.mark_route_reachable(key, member_set([remote_member.clone()]));
    });
    assert_peer_route_update(
        recv_until(&updates_rx, |_| true),
        &expected_remote_member,
        &[remote_route],
        Some(first_local_endpoint),
    );

    component.on_definition(|component| {
        let _handled = component
            .handle_udp_indication(UdpIndication::Closed {
                socket_id: SocketId(83),
                remote_addr: None,
                reason: UdpCloseReason::Requested,
            })
            .expect("endpoint close should be handled");
    });
    assert_peer_route_update(
        recv_until(&updates_rx, |_| true),
        &expected_remote_member,
        &[],
        None,
    );

    component
        .actor_ref()
        .tell(RouteEstablishmentMessage::LocalEndpointBound {
            socket_id: SocketId(84),
            local_addr: second_local_endpoint,
        });
    assert_probe_send(&requests_rx, SocketId(84), remote_route);

    kill_component(&system, component);
    kill_component(&system, update_probe);
    kill_component(&system, udp_probe);
    system.shutdown().wait().expect("Kompact shutdown");
}

#[test]
fn published_routes_are_rebuilt_from_current_reachable_state() {
    let local_member = member(["alice"]);
    let remote_member = member(["bob"]);
    let memberships = shared_memberships(&local_member, &remote_member);
    let local_endpoint = SocketAddr::from(([127, 0, 0, 1], 49104));
    let remote_route = SocketAddr::from(([127, 0, 0, 1], 62159));
    let first_key = RouteProbeKey {
        instance_id: Uuid::from_u128(51),
        route: remote_route,
    };
    let second_key = RouteProbeKey {
        instance_id: Uuid::from_u128(52),
        route: remote_route,
    };
    let system = build_test_kompact_system();
    let (requests_tx, requests_rx) = mpsc::channel();
    let udp_probe = system.create(move || UdpRequestProbe::new(requests_tx));
    let (updates_tx, updates_rx) = mpsc::channel();
    let update_probe = system.create(move || DiscoveryRouteUpdateProbe::new(updates_tx));
    let expected_remote_member = remote_member.clone();
    let component = system.create(move || route_establishment_component(local_member, memberships));
    biconnect_components::<UdpPort, _, _>(&udp_probe, &component).expect("connect UDP probe");
    biconnect_components::<DiscoveryRoutePort, _, _>(&component, &update_probe)
        .expect("connect route update probe");

    start_component(&system, &udp_probe);
    start_component(&system, &update_probe);
    start_component(&system, &component);

    component.on_definition(|component| {
        observe_peer_route(component, first_key);
        observe_peer_route(component, second_key);
    });
    component
        .actor_ref()
        .tell(RouteEstablishmentMessage::LocalEndpointBound {
            socket_id: SocketId(85),
            local_addr: local_endpoint,
        });
    assert_probe_send(&requests_rx, SocketId(85), remote_route);
    assert_probe_send(&requests_rx, SocketId(85), remote_route);
    component.on_definition(|component| {
        component.mark_route_reachable(first_key, member_set([remote_member.clone()]));
        component.mark_route_reachable(second_key, member_set([remote_member.clone()]));
    });
    assert_peer_route_update(
        recv_until(&updates_rx, |_| true),
        &expected_remote_member,
        &[remote_route],
        Some(local_endpoint),
    );
    assert!(
        updates_rx.recv_timeout(Duration::from_millis(100)).is_err(),
        "adding the same reachable route from another instance should not republish"
    );

    component.on_definition(|component| {
        component.mark_route_stale(first_key);
    });
    assert!(
        updates_rx.recv_timeout(Duration::from_millis(100)).is_err(),
        "staling one duplicate route should keep the published route"
    );

    component.on_definition(|component| {
        component.mark_route_stale(second_key);
    });
    assert_peer_route_update(
        recv_until(&updates_rx, |_| true),
        &expected_remote_member,
        &[],
        None,
    );

    kill_component(&system, component);
    kill_component(&system, update_probe);
    kill_component(&system, udp_probe);
    system.shutdown().wait().expect("Kompact shutdown");
}
