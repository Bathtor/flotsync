//! Route-establishment component tests.

use super::{
    component::{claim_matches_group_memberships, local_claim_group_ids},
    *,
};
use crate::{
    DatagramRouteScope,
    DiscoveryRouteUpdate,
    InboundTransportMeta,
    RouteDiscoveryPort,
    RouteEndpointBinding,
    RouteEndpointLifecycle,
    RouteEndpointUnavailableReason,
    RoutePreferenceRank,
    RouteSharingKind,
    RouteTransportActorMessage,
    RouteTransportInboundDeliver,
    RouteTransportPort,
    RouteTransportSend,
    RouteTransportSubmitResult,
    TransportRouteKey,
    UdpRouteKey,
    protocol::{
        decode_endpoint_discovery_frame_from_buf,
        introduction_endpoint_frame,
        introduction_request_endpoint_frame,
    },
};
use flotsync_core::{
    GroupId,
    member::{Identifier, TrieSet},
    membership::{GroupMembers, GroupMemberships, SharedGroupMemberships},
};
use flotsync_discovery::{
    endpoint_selection::EndpointSelection,
    protocol::{DiscoveryRoute, discovery_route_from_wire},
    services::PeerAnnouncementObserved,
};
use flotsync_io::{
    prelude::{
        EgressPool,
        IoBufferConfig,
        IoBufferPools,
        IoPayload,
        MAX_UDP_PAYLOAD_BYTES,
        SocketId,
    },
    test_support::{
        build_test_kompact_system,
        eventually_component_state,
        kill_component,
        start_component,
    },
};
use flotsync_messages::{
    buffa::{Message as _, MessageField},
    discovery as discovery_proto,
    serialisation::{FlotsyncSerializable, encode_message_payload},
    wire::{group_id_to_wire_bytes, member_identity_to_wire_format, uuid_to_wire_bytes},
};
use flotsync_security::{FrameSignature, SIGNATURE_LENGTH};
use flotsync_utils::{
    BoxError,
    kompact_testing::{PortTesterComponent, PortTestingExt, PortTestingRefExt},
};
use futures_util::FutureExt as _;
use kompact::prelude::*;
use std::{
    cell::Cell,
    collections::HashSet,
    io,
    net::SocketAddr,
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

// TODO(flotsync-h1z0): Replace this local actor recorder once Kompact actor-message
// probes provide the same indexed observation and negative-assertion helpers as port probes.
/// Actor test double that records outbound route-transport submits and acknowledges them.
#[derive(ComponentDefinition)]
struct RouteTransportRecorderComponent {
    /// Kompact component context for the recorder actor.
    ctx: ComponentContext<Self>,
    /// Channel receiving every route-transport submit issued by route establishment.
    submits: mpsc::Sender<RouteTransportSend<TransportRouteKey>>,
}

impl RouteTransportRecorderComponent {
    fn new(submits: mpsc::Sender<RouteTransportSend<TransportRouteKey>>) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            submits,
        }
    }
}

ignore_lifecycle!(RouteTransportRecorderComponent);

impl Actor for RouteTransportRecorderComponent {
    type Message = RouteTransportActorMessage<TransportRouteKey>;

    fn receive_local(&mut self, msg: Self::Message) -> HandlerResult {
        match msg {
            RouteTransportActorMessage::Submit(ask) => {
                let (promise, send) = ask.take();
                let coverage_key = send.route.coverage_key;
                self.submits
                    .send(send)
                    .expect("route transport recorder receiver should stay live");
                let _ = promise.fulfil(RouteTransportSubmitResult::Sent { coverage_key });
                Handled::OK
            }
            RouteTransportActorMessage::RegisterExternalUdpSocket(ask) => {
                let (promise, _registration) = ask.take();
                let _ = promise.fulfil(Ok(()));
                Handled::OK
            }
        }
    }
}

fn shared_memberships(
    local_member: &MemberIdentity,
    remote_member: &MemberIdentity,
) -> SharedGroupMemberships {
    single_group_memberships([local_member.clone(), remote_member.clone()])
}

/// Build `group_count` local shared groups for introduction-size tests.
fn many_shared_group_memberships(
    local_member: &MemberIdentity,
    remote_member: &MemberIdentity,
    group_count: usize,
) -> SharedGroupMemberships {
    let groups = (0..group_count).map(|offset| {
        let group_index = u128::try_from(offset).expect("test group index should fit u128");
        (
            group_id(10_000 + group_index),
            group_members([local_member.clone(), remote_member.clone()]),
        )
    });
    SharedGroupMemberships::new(GroupMemberships::from_groups(groups))
}

fn single_group_memberships(
    members: impl IntoIterator<Item = MemberIdentity>,
) -> SharedGroupMemberships {
    SharedGroupMemberships::new(GroupMemberships::from_groups([(
        group_id(1),
        group_members(members),
    )]))
}

fn route_establishment_component_with_credentials(
    local_member: MemberIdentity,
    group_memberships: SharedGroupMemberships,
    credentials: Arc<dyn DiscoveryCredentials>,
    route_transport: ActorRefStrong<RouteTransportActorMessage<TransportRouteKey>>,
) -> RouteEstablishmentComponent {
    RouteEstablishmentComponent::new(
        RouteEstablishmentConfig::new().with_instance_id(Uuid::from_u128(11)),
        route_transport,
        local_member,
        credentials,
        group_memberships,
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

fn assert_udp_transport_route(
    send: &RouteTransportSend<TransportRouteKey>,
    expected_local_bind: SocketAddr,
    expected_target: SocketAddr,
) {
    assert_eq!(send.route.sharing, RouteSharingKind::Exclusive);
    match send.route.coverage_key {
        TransportRouteKey::Udp(route) => {
            assert_eq!(route.remote_addr, expected_target);
            assert_eq!(route.scope, DatagramRouteScope::Unicast);
            assert_eq!(route.local_bind, Some(expected_local_bind));
        }
        TransportRouteKey::Tcp(route) => {
            panic!("expected UDP route transport key, got TCP route {route:?}");
        }
    }
}

fn encode_transport_payload(payload: &Arc<dyn FlotsyncSerializable>) -> IoPayload {
    block_on(encode_message_payload(
        &test_egress_pool(),
        payload.as_ref(),
    ))
    .expect("route transport payload should encode")
}

fn assert_probe_submit_and_get_nonce(
    send: &RouteTransportSend<TransportRouteKey>,
    expected_local_bind: SocketAddr,
    expected_target: SocketAddr,
) -> Vec<u8> {
    assert_udp_transport_route(send, expected_local_bind, expected_target);
    let payload = encode_transport_payload(&send.payload);
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
    update: &DiscoveryRouteUpdate<TransportRouteKey>,
    expected_peer: &MemberIdentity,
    expected_routes: &[SocketAddr],
    expected_local_bind: Option<SocketAddr>,
) {
    match update {
        DiscoveryRouteUpdate::PeerRoutes { peer, routes } => {
            assert_eq!(peer, expected_peer);
            let actual_routes = routes
                .iter()
                .map(|candidate| {
                    assert_eq!(candidate.sharing, RouteSharingKind::Exclusive);
                    assert_eq!(candidate.preference_rank, RoutePreferenceRank::new(1));
                    match candidate.coverage_key {
                        TransportRouteKey::Udp(route) => {
                            assert_eq!(route.scope, DatagramRouteScope::Unicast);
                            assert_eq!(route.local_bind, expected_local_bind);
                            route.remote_addr
                        }
                        TransportRouteKey::Tcp(route) => {
                            panic!("expected UDP published route, got TCP route {route:?}");
                        }
                    }
                })
                .collect::<Vec<_>>();
            assert_eq!(actual_routes, expected_routes);
        }
        DiscoveryRouteUpdate::RelayRoutes { relay, routes } => {
            panic!("expected peer route update, got relay {relay:?} with routes {routes:?}");
        }
    }
}

/// Assert that one route-transport submit carries an introduction claim for `expected_route`.
fn assert_introduction_claims_route(
    send: &RouteTransportSend<TransportRouteKey>,
    expected_local_bind: SocketAddr,
    expected_target: SocketAddr,
    expected_nonce: &[u8],
    expected_route: SocketAddr,
) {
    assert_udp_transport_route(send, expected_local_bind, expected_target);
    let payload = encode_transport_payload(&send.payload);
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

/// Concrete route-transport test port used for inbound route-establishment deliveries.
type TestRouteTransportPort = RouteTransportPort<TransportRouteKey>;
/// Concrete route-discovery test port used for route-establishment publications.
type TestRouteDiscoveryPort = RouteDiscoveryPort<TransportRouteKey>;

/// Owns the route-establishment test topology.
struct RouteEstablishmentHarness {
    system: KompactSystem,
    route_transport: Arc<Component<RouteTransportRecorderComponent>>,
    route_transport_rx: mpsc::Receiver<RouteTransportSend<TransportRouteKey>>,
    inbound_transport: Arc<Component<PortTesterComponent<TestRouteTransportPort>>>,
    update_probe: Arc<Component<PortTesterComponent<TestRouteDiscoveryPort>>>,
    component: Arc<Component<RouteEstablishmentComponent>>,
    update_cursor: Cell<usize>,
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
        let (route_transport_tx, route_transport_rx) = mpsc::channel();
        let route_transport =
            system.create(move || RouteTransportRecorderComponent::new(route_transport_tx));
        let route_transport_ref = route_transport
            .actor_ref()
            .hold()
            .expect("route transport recorder must expose a strong actor ref");
        let inbound_transport = system.create(TestRouteTransportPort::tester_component_sidecar);
        let update_probe = system.create(TestRouteDiscoveryPort::tester_component_sidecar);
        let component = system.create(move || {
            route_establishment_component_with_credentials(
                local_member,
                group_memberships,
                credentials,
                route_transport_ref,
            )
        });
        biconnect_components::<TestRouteTransportPort, _, _>(&inbound_transport, &component)
            .expect("connect route transport probe");
        biconnect_components::<TestRouteDiscoveryPort, _, _>(&component, &update_probe)
            .expect("connect route update probe");

        start_component(&system, &route_transport);
        start_component(&system, &inbound_transport);
        start_component(&system, &update_probe);
        start_component(&system, &component);

        Self {
            system,
            route_transport,
            route_transport_rx,
            inbound_transport,
            update_probe,
            component,
            update_cursor: Cell::new(0),
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
        let route_endpoint_lifecycle_port = self
            .component
            .on_definition(RouteEstablishmentComponent::route_endpoint_lifecycle_port);
        self.system.trigger_i(
            RouteEndpointLifecycle::Available(RouteEndpointBinding {
                socket_id,
                socket_bound_addr: local_addr,
            }),
            &route_endpoint_lifecycle_port,
        );
    }

    fn close_endpoint(&self, socket_id: SocketId, local_addr: SocketAddr) {
        let route_endpoint_lifecycle_port = self
            .component
            .on_definition(RouteEstablishmentComponent::route_endpoint_lifecycle_port);
        self.system.trigger_i(
            RouteEndpointLifecycle::Unavailable {
                binding: RouteEndpointBinding {
                    socket_id,
                    socket_bound_addr: local_addr,
                },
                reason: RouteEndpointUnavailableReason::Closed {
                    reason: flotsync_io::prelude::UdpCloseReason::Requested,
                },
            },
            &route_endpoint_lifecycle_port,
        );
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
        self.expect_transport_probe_with_nonce(local_addr, remote_route)
    }

    fn expect_transport_probe(&self, local_bind: SocketAddr, remote_route: SocketAddr) {
        let submit = self.recv_transport_submit();
        assert_probe_submit_and_get_nonce(&submit, local_bind, remote_route);
    }

    fn expect_transport_probe_with_nonce(
        &self,
        local_bind: SocketAddr,
        remote_route: SocketAddr,
    ) -> Vec<u8> {
        let submit = self.recv_transport_submit();
        assert_probe_submit_and_get_nonce(&submit, local_bind, remote_route)
    }

    fn recv_transport_submit(&self) -> RouteTransportSend<TransportRouteKey> {
        self.route_transport_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("route transport submit should be observed")
    }

    fn expect_no_transport_submit(&self, reason: &'static str) {
        match self
            .route_transport_rx
            .recv_timeout(Duration::from_millis(100))
        {
            Ok(submit) => panic!("{reason}: unexpected submit {submit:?}"),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!("{reason}: route transport recorder disconnected");
            }
        }
    }

    fn receive_transport(&self, source: SocketAddr, payload: IoPayload) {
        let local_bind = self
            .component
            .on_definition(|component| component.local_endpoint().binding())
            .map(|endpoint| endpoint.local_addr);
        self.inbound_transport
            .actor_ref()
            .inject_indication(RouteTransportInboundDeliver {
                payload,
                transport: InboundTransportMeta {
                    route: TransportRouteKey::Udp(UdpRouteKey {
                        remote_addr: source,
                        scope: DatagramRouteScope::Unicast,
                        local_bind,
                    }),
                    remote_addr: Some(source),
                },
            });
    }

    fn expect_peer_route_update(
        &self,
        expected_peer: &MemberIdentity,
        expected_routes: &[SocketAddr],
        expected_local_bind: Option<SocketAddr>,
    ) {
        let observed = self
            .update_probe
            .actor_ref()
            .observe_indication_from(self.update_cursor.get(), |_| true)
            .wait_timeout(Duration::from_secs(1))
            .expect("route update should be observed")
            .expect("route update probe should stay live");
        self.update_cursor.set(observed.index() + 1);
        assert_peer_route_update(
            observed.indication(),
            expected_peer,
            expected_routes,
            expected_local_bind,
        );
    }

    fn expect_no_route_update(&self, reason: &'static str) {
        self.update_probe
            .actor_ref()
            .fail_if_indication_observed_from(
                self.update_cursor.get(),
                Duration::from_millis(100),
                |_| true,
            )
            .wait_timeout(Duration::from_secs(1))
            .expect("route update absence check should complete")
            .expect("route update probe should stay live")
            .expect(reason);
    }

    fn shutdown(self) {
        let Self {
            system,
            route_transport,
            route_transport_rx: _,
            inbound_transport,
            update_probe,
            component,
            update_cursor: _,
        } = self;
        kill_component(&system, component);
        kill_component(&system, update_probe);
        kill_component(&system, inbound_transport);
        kill_component(&system, route_transport);
        system.shutdown().wait().expect("Kompact shutdown");
    }
}

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
    let request_nonce = uuid_to_wire_bytes(Uuid::from_u128(42_100));
    let harness = RouteEstablishmentHarness::new(local_member, memberships);

    harness.publish_endpoint_selection_and_wait_until_applied([selected_endpoint]);
    harness.bind_endpoint(SocketId(42), local_endpoint);
    let frame = introduction_request_endpoint_frame(request_nonce.clone());
    let payload = block_on(encode_message_payload(&test_egress_pool(), &frame))
        .expect("introduction request payload should encode");
    harness.receive_transport(remote_route, payload);
    let response = harness.recv_transport_submit();

    assert_introduction_claims_route(
        &response,
        local_endpoint,
        remote_route,
        &request_nonce,
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
    let request_nonce = uuid_to_wire_bytes(Uuid::from_u128(42_101));
    let harness = RouteEstablishmentHarness::new(local_member, memberships);

    harness.publish_endpoint_selection_and_wait_until_applied([selected_endpoint]);
    harness.bind_endpoint(SocketId(43), local_endpoint);
    let frame = introduction_request_endpoint_frame(request_nonce.clone());
    let payload = block_on(encode_message_payload(&test_egress_pool(), &frame))
        .expect("introduction request payload should encode");
    harness.receive_transport(remote_route, payload);
    let response = harness.recv_transport_submit();

    assert_introduction_claims_route(
        &response,
        local_endpoint,
        remote_route,
        &request_nonce,
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

    harness.receive_transport(remote_route, payload);

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
            .with_claim_nonce(uuid_to_wire_bytes(Uuid::from_u128(7900)))
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
