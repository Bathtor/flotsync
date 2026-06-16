use super::{
    component::{claim_matches_group_memberships, local_claim_group_ids},
    observation::SocketState,
    *,
};
use crate::{
    DEFAULT_DISCOVERY_PORT,
    config_keys,
    endpoint_selection::EndpointSelection,
    protocol::{
        decode_endpoint_discovery_frame_from_buf,
        discovery_route_from_wire,
        introduction_endpoint_frame,
        introduction_request_endpoint_frame,
    },
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
        IoPayload,
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
        eventually_component_state,
        kill_component,
        recv_until,
        start_component,
    },
};
use flotsync_messages::{
    buffa::{Message as _, MessageField},
    discovery as discovery_proto,
    serialisation::encode_message_payload,
    wire::{group_id_to_wire_bytes, member_identity_to_wire_format, uuid_to_wire_bytes},
};
use flotsync_security::{FrameSignature, SIGNATURE_LENGTH};
use flotsync_utils::BoxError;
use futures_util::FutureExt as _;
use kompact::prelude::*;
use std::{
    collections::HashSet,
    io,
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
        std::future::ready(Ok(())).boxed()
    }
}

struct RejectingDiscoveryCredentials;

impl DiscoveryCredentials for RejectingDiscoveryCredentials {
    fn sign_discovery_claim_payload(&self, _payload: &[u8]) -> Result<FrameSignature, BoxError> {
        Ok(FrameSignature::from_bytes([0; SIGNATURE_LENGTH]))
    }

    fn verify_discovery_claim_payload<'a>(
        &'a self,
        _member: &'a MemberIdentity,
        _payload: &'a [u8],
        _signature: &'a FrameSignature,
    ) -> DiscoveryCredentialFuture<'a> {
        std::future::ready(Err(
            Box::new(io::Error::other("signature rejected")) as BoxError
        ))
        .boxed()
    }
}

fn shared_memberships(
    local_member: &MemberIdentity,
    remote_member: &MemberIdentity,
) -> SharedGroupMemberships {
    single_group_memberships([local_member.clone(), remote_member.clone()])
}

fn single_group_memberships(
    members: impl IntoIterator<Item = MemberIdentity>,
) -> SharedGroupMemberships {
    SharedGroupMemberships::new(GroupMemberships::from_groups([(
        group_id(1),
        group_members(members),
    )]))
}

fn route_establishment_component(
    local_member: MemberIdentity,
    group_memberships: SharedGroupMemberships,
) -> RouteEstablishmentComponent {
    route_establishment_component_with_credentials(
        local_member,
        group_memberships,
        Arc::new(NoopDiscoveryCredentials),
    )
}

fn route_establishment_component_with_credentials(
    local_member: MemberIdentity,
    group_memberships: SharedGroupMemberships,
    credentials: Arc<dyn DiscoveryCredentials>,
) -> RouteEstablishmentComponent {
    RouteEstablishmentComponent::new(
        RouteEstablishmentConfig::new(SocketAddr::from(([127, 0, 0, 1], 52157)))
            .with_instance_id(Uuid::from_u128(11)),
        local_member,
        credentials,
        group_memberships,
        test_egress_pool(),
    )
}

fn observe_peer_route(
    component: &mut RouteEstablishmentComponent,
    instance_id: Uuid,
    route: SocketAddr,
) {
    component.record_peer_announcement(PeerAnnouncementObserved {
        instance_id,
        routes: vec![DiscoveryRoute::Udp(route)],
    });
}

fn watched_udp_route(route: SocketAddr, expected_member: Option<MemberIdentity>) -> WatchedRoute {
    WatchedRoute {
        route: DiscoveryRoute::Udp(route),
        expected_member,
    }
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

fn assert_probe_send_and_nonce(
    requests_rx: &mpsc::Receiver<UdpRequest>,
    expected_socket_id: SocketId,
    expected_target: SocketAddr,
) -> Vec<u8> {
    match recv_until(requests_rx, |request| {
        matches!(request, UdpRequest::Send { .. })
    }) {
        UdpRequest::Send {
            socket_id,
            target,
            payload,
            ..
        } => {
            assert_eq!(socket_id, expected_socket_id);
            assert_eq!(target, Some(expected_target));
            let mut cursor = payload.cursor();
            let discovery_frame = decode_endpoint_discovery_frame_from_buf(&mut cursor)
                .expect("probe payload should decode")
                .expect("probe payload should be a discovery frame");
            match discovery_frame.body {
                Some(discovery_proto::discovery_frame::Body::IntroductionRequest(request)) => {
                    request.request_nonce
                }
                other => panic!("expected introduction request, got {other:?}"),
            }
        }
        other => unreachable!("filtered to send request, got {other:?}"),
    }
}

fn trigger_udp_received(
    udp_probe: &Arc<Component<UdpRequestProbe>>,
    socket_id: SocketId,
    source: SocketAddr,
    payload: IoPayload,
) {
    udp_probe.on_definition(move |probe| {
        probe.udp.trigger(UdpIndication::Received {
            socket_id,
            source,
            payload,
        });
    });
}

fn test_egress_pool() -> EgressPool {
    IoBufferPools::new(IoBufferConfig::default())
        .expect("test IO buffers should build")
        .egress()
}

/// Builds introduction replies while keeping each mismatch case explicit.
struct IntroductionSpec<'a> {
    member: &'a MemberIdentity,
    top_level_instance_id: Uuid,
    claim_instance_id: Uuid,
    top_level_nonce: Option<Vec<u8>>,
    claim_nonce: Option<Vec<u8>>,
    claimed_route: SocketAddr,
    group_ids: Vec<GroupId>,
}

impl<'a> IntroductionSpec<'a> {
    fn new(
        member: &'a MemberIdentity,
        instance_id: Uuid,
        claimed_route: SocketAddr,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Self {
        Self {
            member,
            top_level_instance_id: instance_id,
            claim_instance_id: instance_id,
            top_level_nonce: None,
            claim_nonce: None,
            claimed_route,
            group_ids: group_ids.into_iter().collect(),
        }
    }

    fn with_top_level_nonce(mut self, nonce: Vec<u8>) -> Self {
        self.top_level_nonce = Some(nonce);
        self
    }

    fn with_claim_nonce(mut self, nonce: Vec<u8>) -> Self {
        self.claim_nonce = Some(nonce);
        self
    }

    fn with_claim_instance(mut self, instance_id: Uuid) -> Self {
        self.claim_instance_id = instance_id;
        self
    }

    fn with_claimed_route(mut self, route: SocketAddr) -> Self {
        self.claimed_route = route;
        self
    }

    fn encode(self, request_nonce: Vec<u8>) -> IoPayload {
        let Self {
            member,
            top_level_instance_id,
            claim_instance_id,
            top_level_nonce,
            claim_nonce,
            claimed_route,
            group_ids,
        } = self;
        let top_level_nonce = top_level_nonce.unwrap_or_else(|| request_nonce.clone());
        let claim_nonce = claim_nonce.unwrap_or(request_nonce);
        let instance_uuid = uuid_to_wire_bytes(top_level_instance_id);
        let claim_instance_uuid = uuid_to_wire_bytes(claim_instance_id);
        let claim_payload = discovery_proto::IntroductionClaimPayload {
            instance_uuid: claim_instance_uuid,
            request_nonce: claim_nonce,
            route: MessageField::some(DiscoveryRoute::Udp(claimed_route).to_wire_format()),
            group_ids: group_ids.into_iter().map(group_id_to_wire_bytes).collect(),
            ..discovery_proto::IntroductionClaimPayload::default()
        };
        let claim_payload = claim_payload.encode_to_vec();
        let signature = FrameSignature::from_bytes([0; SIGNATURE_LENGTH]);
        let introduction = discovery_proto::Introduction {
            instance_uuid,
            request_nonce: top_level_nonce,
            claims: vec![discovery_proto::SignedIntroductionClaim {
                member_id: MessageField::some(member_identity_to_wire_format(member)),
                claim_payload,
                signature: MessageField::some(super::wire::discovery_signature_to_wire(&signature)),
                ..discovery_proto::SignedIntroductionClaim::default()
            }],
            ..discovery_proto::Introduction::default()
        };
        block_on(encode_message_payload(
            &test_egress_pool(),
            &introduction_endpoint_frame(introduction),
        ))
        .expect("introduction payload should encode")
    }
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

/// Assert that one UDP send carries an introduction claim for `expected_route`.
fn assert_introduction_claims_route(
    request: UdpRequest,
    expected_socket_id: SocketId,
    expected_target: SocketAddr,
    expected_nonce: &[u8],
    expected_route: SocketAddr,
) {
    match request {
        UdpRequest::Send {
            socket_id,
            target,
            payload,
            ..
        } => {
            assert_eq!(socket_id, expected_socket_id);
            assert_eq!(target, Some(expected_target));
            let mut cursor = payload.cursor();
            let discovery_frame = decode_endpoint_discovery_frame_from_buf(&mut cursor)
                .expect("introduction response should decode")
                .expect("introduction response should be a discovery frame");
            let Some(discovery_proto::discovery_frame::Body::Introduction(introduction)) =
                discovery_frame.body
            else {
                panic!("expected introduction response");
            };
            assert_eq!(introduction.request_nonce, expected_nonce);
            assert_eq!(introduction.claims.len(), 1);
            let claim_payload = discovery_proto::IntroductionClaimPayload::decode_from_slice(
                &introduction.claims[0].claim_payload,
            )
            .expect("claim payload should decode");
            let route = claim_payload
                .route
                .as_option()
                .expect("claim payload route should be present");
            assert_eq!(
                discovery_route_from_wire(route, "IntroductionClaimPayload.route")
                    .expect("claim route should decode"),
                DiscoveryRoute::Udp(expected_route)
            );
        }
        other => unreachable!("filtered to send request, got {other:?}"),
    }
}

/// Owns the three-component route-establishment test topology.
struct RouteEstablishmentHarness {
    system: KompactSystem,
    udp_probe: Arc<Component<UdpRequestProbe>>,
    update_probe: Arc<Component<DiscoveryRouteUpdateProbe>>,
    component: Arc<Component<RouteEstablishmentComponent>>,
    requests_rx: mpsc::Receiver<UdpRequest>,
    updates_rx: mpsc::Receiver<DiscoveryRouteUpdate>,
}

impl RouteEstablishmentHarness {
    fn new(local_member: MemberIdentity, group_memberships: SharedGroupMemberships) -> Self {
        Self::with_credentials(
            local_member,
            group_memberships,
            Arc::new(NoopDiscoveryCredentials),
        )
    }

    fn with_credentials(
        local_member: MemberIdentity,
        group_memberships: SharedGroupMemberships,
        credentials: Arc<dyn DiscoveryCredentials>,
    ) -> Self {
        let system = build_test_kompact_system();
        let (requests_tx, requests_rx) = mpsc::channel();
        let udp_probe = system.create(move || UdpRequestProbe::new(requests_tx));
        let (updates_tx, updates_rx) = mpsc::channel();
        let update_probe = system.create(move || DiscoveryRouteUpdateProbe::new(updates_tx));
        let component = system.create(move || {
            route_establishment_component_with_credentials(
                local_member,
                group_memberships,
                credentials,
            )
        });
        biconnect_components::<UdpPort, _, _>(&udp_probe, &component).expect("connect UDP probe");
        biconnect_components::<DiscoveryRoutePort, _, _>(&component, &update_probe)
            .expect("connect route update probe");

        start_component(&system, &udp_probe);
        start_component(&system, &update_probe);
        start_component(&system, &component);

        Self {
            system,
            udp_probe,
            update_probe,
            component,
            requests_rx,
            updates_rx,
        }
    }

    fn observe_peer_route(&self, instance_id: Uuid, route: SocketAddr) {
        self.component
            .on_definition(|component| observe_peer_route(component, instance_id, route));
    }

    fn mark_route_reachable(
        &self,
        route: SocketAddr,
        members: impl IntoIterator<Item = MemberIdentity>,
    ) {
        let reachable_members = member_set(members);
        self.component.on_definition(move |component| {
            component.mark_route_reachable(DiscoveryRoute::Udp(route), reachable_members);
        });
    }

    fn mark_route_stale(&self, route: SocketAddr) {
        self.component.on_definition(move |component| {
            component.mark_route_stale(DiscoveryRoute::Udp(route));
        });
    }

    fn handle_udp_indication(&self, indication: UdpIndication, context: &'static str) {
        self.component.on_definition(move |component| {
            let _handled = component.handle_udp_indication(indication).expect(context);
        });
    }

    fn replace_manual_route_watches(
        &self,
        watches: impl IntoIterator<Item = WatchedRoute>,
    ) -> Result<(), ManualRouteWatchError> {
        let watches = watches.into_iter().collect();
        let future = self.component.actor_ref().ask_with(|promise| {
            RouteEstablishmentMessage::ReplaceManualRouteWatches(Ask::new(promise, watches))
        });
        block_on(future).expect("manual route watch ask should complete")
    }

    fn clear_manual_route_watches(&self) {
        self.replace_manual_route_watches(Vec::new())
            .expect("clearing manual route watches should succeed");
    }

    fn bind_endpoint(&self, socket_id: SocketId, local_addr: SocketAddr) {
        self.component
            .actor_ref()
            .tell(RouteEstablishmentMessage::LocalEndpointBound {
                socket_id,
                local_addr,
            });
    }

    /// Publish selected local endpoints and wait until they replace introduction claim routes.
    fn publish_endpoint_selection_and_wait_until_applied(
        &self,
        endpoints: impl IntoIterator<Item = SocketAddr>,
    ) {
        let selection = EndpointSelection::from_endpoints(endpoints);
        let expected_endpoints = selection.endpoints.clone();
        let endpoint_selection_port = self
            .component
            .on_definition(RouteEstablishmentComponent::endpoint_selection_port);
        self.system.trigger_i(selection, &endpoint_selection_port);
        eventually_component_state(
            Duration::from_secs(1),
            &self.component,
            |component| component.advertised_routes() == &expected_endpoints,
            "endpoint selection should replace route-establishment claim routes",
        );
    }

    fn probe_manual_route(
        &self,
        socket_id: SocketId,
        local_addr: SocketAddr,
        watches: impl IntoIterator<Item = WatchedRoute>,
        remote_route: SocketAddr,
    ) -> Vec<u8> {
        self.replace_manual_route_watches(watches)
            .expect("manual route watch replacement should succeed");
        self.bind_endpoint(socket_id, local_addr);
        self.expect_udp_probe_with_nonce(socket_id, remote_route)
    }

    fn expect_udp_probe(&self, socket_id: SocketId, remote_route: SocketAddr) {
        assert_probe_send(&self.requests_rx, socket_id, remote_route);
    }

    fn expect_udp_probe_with_nonce(
        &self,
        socket_id: SocketId,
        remote_route: SocketAddr,
    ) -> Vec<u8> {
        assert_probe_send_and_nonce(&self.requests_rx, socket_id, remote_route)
    }

    fn expect_no_udp_request(&self, reason: &'static str) {
        assert!(
            self.requests_rx
                .recv_timeout(Duration::from_millis(100))
                .is_err(),
            "{reason}"
        );
    }

    fn receive_udp(&self, socket_id: SocketId, source: SocketAddr, payload: IoPayload) {
        trigger_udp_received(&self.udp_probe, socket_id, source, payload);
    }

    fn expect_peer_route_update(
        &self,
        expected_peer: &MemberIdentity,
        expected_routes: &[SocketAddr],
        expected_local_bind: Option<SocketAddr>,
    ) {
        assert_peer_route_update(
            recv_until(&self.updates_rx, |_| true),
            expected_peer,
            expected_routes,
            expected_local_bind,
        );
    }

    fn expect_no_route_update(&self, reason: &'static str) {
        assert!(
            self.updates_rx
                .recv_timeout(Duration::from_millis(100))
                .is_err(),
            "{reason}"
        );
    }

    fn shutdown(self) {
        let Self {
            system,
            udp_probe,
            update_probe,
            component,
            requests_rx: _,
            updates_rx: _,
        } = self;
        kill_component(&system, component);
        kill_component(&system, update_probe);
        kill_component(&system, udp_probe);
        system.shutdown().wait().expect("Kompact shutdown");
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
            .routes()
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
fn endpoint_selection_port_updates_introduction_claim_routes() {
    let local_member = member(["alice"]);
    let remote_member = member(["bob"]);
    let memberships = shared_memberships(&local_member, &remote_member);
    let local_endpoint = SocketAddr::from(([0, 0, 0, 0], 45_100));
    let selected_endpoint = SocketAddr::from(([192, 168, 1, 20], 45_100));
    let remote_route = SocketAddr::from(([127, 0, 0, 1], 62_100));
    let request_nonce = uuid_to_wire_bytes(Uuid::from_u128(42_100));
    let harness = RouteEstablishmentHarness::new(local_member, memberships);

    harness.publish_endpoint_selection_and_wait_until_applied([selected_endpoint]);
    harness.bind_endpoint(SocketId(42), local_endpoint);
    let frame = introduction_request_endpoint_frame(request_nonce.clone());
    let payload = block_on(encode_message_payload(&test_egress_pool(), &frame))
        .expect("introduction request payload should encode");
    harness.receive_udp(SocketId(42), remote_route, payload);
    let response = recv_until(&harness.requests_rx, |request| {
        matches!(request, UdpRequest::Send { .. })
    });

    assert_introduction_claims_route(
        response,
        SocketId(42),
        remote_route,
        &request_nonce,
        selected_endpoint,
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
    let instance_id = Uuid::from_u128(41);
    let harness = RouteEstablishmentHarness::new(local_member, memberships);

    harness.observe_peer_route(instance_id, remote_route);
    harness.bind_endpoint(SocketId(82), SocketAddr::from(([127, 0, 0, 1], 49101)));

    harness.expect_udp_probe(SocketId(82), remote_route);
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

    harness.expect_no_udp_request("local peer announcements must not produce probes");
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

    harness.receive_udp(SocketId(91), remote_route, payload);

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

    harness.receive_udp(SocketId(97), remote_route, payload);

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

    harness.receive_udp(SocketId(98), remote_route, payload);

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
    let nonce = harness.expect_udp_probe_with_nonce(SocketId(101), remote_route);
    let payload =
        IntroductionSpec::new(&other_member, remote_instance, remote_route, [group_id(1)])
            .encode(nonce);
    harness.receive_udp(SocketId(101), remote_route, payload);
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

    harness.receive_udp(SocketId(102), remote_route, payload);

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

    harness.receive_udp(SocketId(92), remote_route, payload);

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
        Arc::new(RejectingDiscoveryCredentials),
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

    harness.receive_udp(SocketId(93), remote_route, payload);

    harness.expect_no_route_update("unverifiable claim should not publish a watched route");
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
            .with_top_level_nonce(uuid_to_wire_bytes(Uuid::from_u128(7400)))
            .encode(nonce);

    harness.receive_udp(SocketId(94), remote_route, payload);

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
            .with_claim_nonce(uuid_to_wire_bytes(Uuid::from_u128(7900)))
            .encode(nonce);

    harness.receive_udp(SocketId(99), remote_route, payload);

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

    harness.receive_udp(SocketId(100), remote_route, payload);

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

    harness.receive_udp(SocketId(95), remote_route, payload);

    harness.expect_no_route_update("route mismatch should not publish a watched route");
    harness.shutdown();
}

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
    harness.receive_udp(SocketId(96), remote_route, payload);
    harness.expect_peer_route_update(&remote_member, &[remote_route], Some(local_endpoint));

    harness.clear_manual_route_watches();

    harness.expect_peer_route_update(&remote_member, &[], Some(local_endpoint));
    harness.shutdown();
}

#[test]
fn endpoint_close_withdraws_routes_and_rebinding_reprobes() {
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
    harness.expect_udp_probe(SocketId(83), remote_route);
    harness.mark_route_reachable(remote_route, [remote_member.clone()]);
    harness.expect_peer_route_update(&remote_member, &[remote_route], Some(first_local_endpoint));

    harness.handle_udp_indication(
        UdpIndication::Closed {
            socket_id: SocketId(83),
            remote_addr: None,
            reason: UdpCloseReason::Requested,
        },
        "endpoint close should be handled",
    );
    harness.expect_peer_route_update(&remote_member, &[], None);

    harness.bind_endpoint(SocketId(84), second_local_endpoint);
    harness.expect_udp_probe(SocketId(84), remote_route);
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
    harness.expect_udp_probe(SocketId(85), remote_route);
    harness.expect_no_udp_request(
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
