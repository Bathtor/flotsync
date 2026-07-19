//! Shared harness and credential doubles for route-establishment component tests.

use super::*;

pub(super) fn group_id(value: u128) -> GroupId {
    GroupId(Uuid::from_u128(value))
}

pub(super) const TEST_DISCOVERY_KEY_FINGERPRINT: KeyFingerprint =
    KeyFingerprint::from_bytes([7; 32]);

pub(super) fn group_members(members: impl IntoIterator<Item = MemberIdentity>) -> GroupMembers {
    GroupMembers::from_ordered_members(members).expect("group members should build")
}

pub(super) fn member_set(members: impl IntoIterator<Item = MemberIdentity>) -> TrieSet {
    let mut set = TrieSet::new();
    for member in members {
        set.insert(member);
    }
    set
}

/// Configurable route-establishment credential double for claim-handling tests.
#[derive(Clone, Copy, Debug)]
pub(super) struct RouteEstablishmentTestCredentials {
    /// Key-material state reported before claim verification is attempted.
    key_material_status: DiscoveryKeyMaterialStatus,
    /// Outcome of claim signature verification.
    claim_verification: TestCredentialDecision,
    /// Outcome of route-publication permission checks.
    route_publication: TestCredentialDecision,
}

impl RouteEstablishmentTestCredentials {
    pub(super) fn allow_all() -> Self {
        Self {
            key_material_status: DiscoveryKeyMaterialStatus::Available,
            claim_verification: TestCredentialDecision::Allow,
            route_publication: TestCredentialDecision::Allow,
        }
    }

    pub(super) fn reject_claim_verification() -> Self {
        Self {
            claim_verification: TestCredentialDecision::Reject("signature rejected"),
            ..Self::allow_all()
        }
    }

    pub(super) fn missing_key_material() -> Self {
        Self {
            key_material_status: DiscoveryKeyMaterialStatus::Missing,
            ..Self::allow_all()
        }
    }

    pub(super) fn reject_route_publication() -> Self {
        Self {
            route_publication: TestCredentialDecision::Reject("route publication rejected"),
            ..Self::allow_all()
        }
    }
}

impl DiscoveryCredentials for RouteEstablishmentTestCredentials {
    fn local_discovery_key_fingerprint(&self) -> KeyFingerprint {
        TEST_DISCOVERY_KEY_FINGERPRINT
    }

    fn local_discovery_public_key_bundle(&self) -> PublicKeyBundle {
        panic!("route-establishment test credentials do not expose local key bundles")
    }

    fn sign_discovery_payload(&self, _payload: &[u8]) -> Result<FrameSignature, BoxError> {
        Ok(FrameSignature::from_bytes([0; SIGNATURE_LENGTH]))
    }

    fn discovery_key_material_status<'a>(
        &'a self,
        _member: &'a MemberIdentity,
        _key_fingerprint: KeyFingerprint,
    ) -> DiscoveryKeyMaterialStatusFuture<'a> {
        std::future::ready(Ok(self.key_material_status)).boxed()
    }

    fn verify_discovery_claim_payload<'a>(
        &'a self,
        _member: &'a MemberIdentity,
        _key_fingerprint: KeyFingerprint,
        _payload: &'a [u8],
        _signature: &'a FrameSignature,
    ) -> DiscoveryCredentialFuture<'a> {
        std::future::ready(self.claim_verification.into_result()).boxed()
    }

    fn permit_member_route_publication<'a>(
        &'a self,
        _member: &'a MemberIdentity,
        _key_fingerprint: KeyFingerprint,
    ) -> DiscoveryCredentialFuture<'a> {
        std::future::ready(self.route_publication.into_result()).boxed()
    }

    fn ensure_discovery_public_key_bundle<'a>(
        &'a self,
        _member: &'a MemberIdentity,
        _bundle: PublicKeyBundle,
    ) -> DiscoveryCredentialFuture<'a> {
        std::future::ready(Ok(())).boxed()
    }
}

/// One fallible credential operation outcome used by the route-establishment test double.
#[derive(Clone, Copy, Debug)]
pub(super) enum TestCredentialDecision {
    /// The credential operation succeeds.
    Allow,
    /// The credential operation fails with a fixed test error message.
    Reject(&'static str),
}

impl TestCredentialDecision {
    pub(super) fn into_result(self) -> Result<(), BoxError> {
        match self {
            Self::Allow => Ok(()),
            Self::Reject(reason) => Err(Box::new(io::Error::other(reason)) as BoxError),
        }
    }
}

pub(super) fn shared_memberships(
    local_member: &MemberIdentity,
    remote_member: &MemberIdentity,
) -> SharedGroupMemberships {
    single_group_memberships([local_member.clone(), remote_member.clone()])
}

/// Build `group_count` local shared groups for introduction-size tests.
pub(super) fn many_shared_group_memberships(
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

pub(super) fn single_group_memberships(
    members: impl IntoIterator<Item = MemberIdentity>,
) -> SharedGroupMemberships {
    SharedGroupMemberships::new(GroupMemberships::from_groups([(
        group_id(1),
        group_members(members),
    )]))
}

pub(super) fn route_establishment_component_with_credentials(
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

pub(super) fn observe_peer_route(
    component: &mut RouteEstablishmentComponent,
    instance_id: Uuid,
    route: SocketAddr,
) {
    component.record_peer_announcement(PeerAnnouncementObserved {
        instance_id,
        routes: vec![DiscoveryRoute::Udp(route)],
    });
}

pub(super) fn watched_udp_route(
    route: SocketAddr,
    expected_member: Option<MemberIdentity>,
) -> WatchedRoute {
    WatchedRoute {
        route: DiscoveryRoute::Udp(route),
        expected_member,
    }
}

pub(super) fn assert_probe_submit_and_get_nonce(
    send: &RouteTransportSend<TransportRouteKey>,
    expected_local_bind: SocketAddr,
    expected_target: SocketAddr,
) -> Uuid {
    assert_udp_transport_route(send, expected_local_bind, expected_target);
    let payload = encode_transport_payload(&send.payload);
    let mut cursor = payload.cursor();
    let discovery_frame = decode_endpoint_discovery_frame_from_buf(&mut cursor)
        .expect("probe payload should decode")
        .expect("probe payload should be a discovery frame");
    match discovery_frame.body {
        Some(discovery_proto::discovery_frame::Body::IntroductionRequest(request)) => {
            uuid_from_wire_bytes(&request.request_nonce, "IntroductionRequest.request_nonce")
                .expect("introduction request nonce should be a UUID")
        }
        other => panic!("expected introduction request, got {other:?}"),
    }
}

/// Builds introduction replies while keeping each mismatch case explicit.
pub(super) struct IntroductionSpec<'a> {
    member: &'a MemberIdentity,
    key_fingerprint: KeyFingerprint,
    top_level_instance_id: Uuid,
    claim_instance_id: Uuid,
    top_level_nonce: Option<Uuid>,
    claim_nonce: Option<Uuid>,
    claimed_route: SocketAddr,
    group_ids: Vec<GroupId>,
}

impl<'a> IntroductionSpec<'a> {
    pub(super) fn new(
        member: &'a MemberIdentity,
        instance_id: Uuid,
        claimed_route: SocketAddr,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> Self {
        Self {
            member,
            key_fingerprint: TEST_DISCOVERY_KEY_FINGERPRINT,
            top_level_instance_id: instance_id,
            claim_instance_id: instance_id,
            top_level_nonce: None,
            claim_nonce: None,
            claimed_route,
            group_ids: group_ids.into_iter().collect(),
        }
    }

    pub(super) fn with_top_level_nonce(mut self, nonce: Uuid) -> Self {
        self.top_level_nonce = Some(nonce);
        self
    }

    pub(super) fn with_claim_nonce(mut self, nonce: Uuid) -> Self {
        self.claim_nonce = Some(nonce);
        self
    }

    pub(super) fn with_claim_instance(mut self, instance_id: Uuid) -> Self {
        self.claim_instance_id = instance_id;
        self
    }

    pub(super) fn with_claimed_route(mut self, route: SocketAddr) -> Self {
        self.claimed_route = route;
        self
    }

    pub(super) fn encode(self, request_nonce: Uuid) -> IoPayload {
        let Self {
            member,
            key_fingerprint,
            top_level_instance_id,
            claim_instance_id,
            top_level_nonce,
            claim_nonce,
            claimed_route,
            group_ids,
        } = self;
        let top_level_nonce = top_level_nonce.unwrap_or(request_nonce);
        let claim_nonce = claim_nonce.unwrap_or(request_nonce);
        let instance_uuid = uuid_to_wire_bytes(top_level_instance_id);
        let claim_instance_uuid = uuid_to_wire_bytes(claim_instance_id);
        let claim_payload = discovery_proto::IntroductionClaimPayload {
            instance_uuid: claim_instance_uuid,
            request_nonce: uuid_to_wire_bytes(claim_nonce),
            route: MessageField::some(DiscoveryRoute::Udp(claimed_route).encode_proto()),
            group_ids: group_ids.into_iter().map(group_id_to_wire_bytes).collect(),
            member_id: MessageField::some(member_identity_to_wire_format(member)),
            key_fingerprint: key_fingerprint.as_ref().to_vec(),
            ..discovery_proto::IntroductionClaimPayload::default()
        };
        let claim_payload = claim_payload.encode_to_vec();
        let signature = FrameSignature::from_bytes([0; SIGNATURE_LENGTH]);
        let introduction = discovery_proto::Introduction {
            instance_uuid,
            request_nonce: uuid_to_wire_bytes(top_level_nonce),
            claims: vec![discovery_proto::SignedIntroductionClaim {
                claim_payload,
                signature: MessageField::some(signature.encode_proto()),
                ..discovery_proto::SignedIntroductionClaim::default()
            }],
            ..discovery_proto::Introduction::default()
        };
        let frame = DiscoveryEndpointFrameView::Introduction {
            introduction: &introduction,
        }
        .encode_proto();
        endpoint_payload(&frame)
    }
}

pub(super) fn assert_peer_route_update(
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
pub(super) fn assert_introduction_claims_route(
    send: &RouteTransportSend<TransportRouteKey>,
    expected_local_bind: SocketAddr,
    expected_target: SocketAddr,
    expected_member: &MemberIdentity,
    expected_key_fingerprint: KeyFingerprint,
    expected_nonce: Uuid,
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
    assert_eq!(
        introduction.request_nonce,
        uuid_to_wire_bytes(expected_nonce)
    );
    assert_eq!(introduction.claims.len(), 1);
    let mut claim_payload = discovery_proto::IntroductionClaimPayload::decode_from_slice(
        &introduction.claims[0].claim_payload,
    )
    .expect("claim payload should decode");
    let member_id = claim_payload
        .member_id
        .take()
        .expect("claim payload member should be present");
    assert_eq!(member_id, member_identity_to_wire_format(expected_member));
    assert_eq!(
        claim_payload.key_fingerprint,
        expected_key_fingerprint.as_ref()
    );
    let route = claim_payload
        .route
        .take()
        .expect("claim payload route should be present");
    assert_eq!(
        DiscoveryRoute::decode_proto(route).expect("claim route should decode"),
        DiscoveryRoute::Udp(expected_route)
    );
}

/// Concrete route-discovery test port used for route-establishment publications.
type TestRouteDiscoveryPort = RouteDiscoveryPort<TransportRouteKey>;
/// Concrete key-material-discovery test port used for key-material fetch requests.
type TestKeyMaterialDiscoveryPort = KeyMaterialDiscoveryPort;

/// Owns the route-establishment test topology.
pub(super) struct RouteEstablishmentHarness {
    system: KompactSystem,
    route_transport: Arc<Component<RouteTransportRecorderComponent>>,
    route_transport_rx: mpsc::Receiver<RouteTransportSend<TransportRouteKey>>,
    inbound_transport: Arc<Component<PortTesterComponent<TestRouteTransportPort>>>,
    update_probe: Arc<Component<PortTesterComponent<TestRouteDiscoveryPort>>>,
    key_material_probe: Arc<Component<PortTesterComponent<TestKeyMaterialDiscoveryPort>>>,
    component: Arc<Component<RouteEstablishmentComponent>>,
    update_cursor: Cell<usize>,
    key_material_cursor: Cell<usize>,
}

impl RouteEstablishmentHarness {
    pub(super) fn new(
        local_member: MemberIdentity,
        group_memberships: SharedGroupMemberships,
    ) -> Self {
        Self::with_credentials(
            local_member,
            group_memberships,
            Arc::new(RouteEstablishmentTestCredentials::allow_all()),
        )
    }

    pub(super) fn with_credentials(
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
        let key_material_probe =
            system.create(TestKeyMaterialDiscoveryPort::tester_component_sidecar);
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
        biconnect_components::<TestKeyMaterialDiscoveryPort, _, _>(&key_material_probe, &component)
            .expect("connect key-material discovery probe");

        start_component(&system, &route_transport);
        start_component(&system, &inbound_transport);
        start_component(&system, &update_probe);
        start_component(&system, &key_material_probe);
        start_component(&system, &component);

        Self {
            system,
            route_transport,
            route_transport_rx,
            inbound_transport,
            update_probe,
            key_material_probe,
            component,
            update_cursor: Cell::new(0),
            key_material_cursor: Cell::new(0),
        }
    }

    pub(super) fn observe_peer_route(&self, instance_id: Uuid, route: SocketAddr) {
        self.component
            .on_definition(|component| observe_peer_route(component, instance_id, route));
    }

    pub(super) fn mark_route_reachable(
        &self,
        route: SocketAddr,
        members: impl IntoIterator<Item = MemberIdentity>,
    ) {
        let reachable_members = member_set(members);
        self.component.on_definition(move |component| {
            component.mark_route_reachable(DiscoveryRoute::Udp(route), reachable_members);
        });
    }

    pub(super) fn mark_route_stale(&self, route: SocketAddr) {
        self.component.on_definition(move |component| {
            component.mark_route_stale(DiscoveryRoute::Udp(route));
        });
    }

    pub(super) fn replace_manual_route_watches(
        &self,
        watches: impl IntoIterator<Item = WatchedRoute>,
    ) -> Result<(), ManualRouteWatchError> {
        let watches = watches.into_iter().collect();
        let future = self.component.actor_ref().ask_with(|promise| {
            RouteEstablishmentMessage::ReplaceManualRouteWatches(Ask::new(promise, watches))
        });
        block_on(future).expect("manual route watch ask should complete")
    }

    pub(super) fn clear_manual_route_watches(&self) {
        self.replace_manual_route_watches(Vec::new())
            .expect("clearing manual route watches should succeed");
    }

    pub(super) fn bind_endpoint(&self, socket_id: SocketId, local_addr: SocketAddr) {
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

    pub(super) fn close_endpoint(&self, socket_id: SocketId, local_addr: SocketAddr) {
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
    pub(super) fn publish_endpoint_selection_and_wait_until_applied(
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

    pub(super) fn probe_manual_route(
        &self,
        socket_id: SocketId,
        local_addr: SocketAddr,
        watches: impl IntoIterator<Item = WatchedRoute>,
        remote_route: SocketAddr,
    ) -> Uuid {
        self.replace_manual_route_watches(watches)
            .expect("manual route watch replacement should succeed");
        self.bind_endpoint(socket_id, local_addr);
        self.expect_transport_probe_with_nonce(local_addr, remote_route)
    }

    pub(super) fn expect_transport_probe(&self, local_bind: SocketAddr, remote_route: SocketAddr) {
        let submit = self.recv_transport_submit();
        assert_probe_submit_and_get_nonce(&submit, local_bind, remote_route);
    }

    pub(super) fn expect_transport_probe_with_nonce(
        &self,
        local_bind: SocketAddr,
        remote_route: SocketAddr,
    ) -> Uuid {
        let submit = self.recv_transport_submit();
        assert_probe_submit_and_get_nonce(&submit, local_bind, remote_route)
    }

    pub(super) fn recv_transport_submit(&self) -> RouteTransportSend<TransportRouteKey> {
        self.route_transport_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("route transport submit should be observed")
    }

    pub(super) fn expect_no_transport_submit(&self, reason: &'static str) {
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

    pub(super) fn receive_transport(&self, source: SocketAddr, payload: IoPayload) {
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

    pub(super) fn expect_peer_route_update(
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

    pub(super) fn expect_no_route_update(&self, reason: &'static str) {
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

    pub(super) fn expect_fetch_key_material_request(
        &self,
        expected_route: SocketAddr,
        expected_member: &MemberIdentity,
        expected_fingerprint: KeyFingerprint,
    ) {
        let observed = self
            .key_material_probe
            .actor_ref()
            .observe_request_from(self.key_material_cursor.get(), |_| true)
            .wait_timeout(Duration::from_secs(1))
            .expect("key-material fetch request should be observed")
            .expect("key-material discovery probe should stay live");
        self.key_material_cursor.set(observed.index() + 1);
        assert_eq!(
            observed.request(),
            &FetchKeyMaterial {
                route: DiscoveryRoute::Udp(expected_route),
                member: expected_member.clone(),
                key_fingerprint: expected_fingerprint,
            }
        );
    }

    pub(super) fn shutdown(self) {
        let Self {
            system,
            route_transport,
            route_transport_rx: _,
            inbound_transport,
            update_probe,
            key_material_probe,
            component,
            update_cursor: _,
            key_material_cursor: _,
        } = self;
        kill_component(&system, component);
        kill_component(&system, key_material_probe);
        kill_component(&system, update_probe);
        kill_component(&system, inbound_transport);
        kill_component(&system, route_transport);
        system.shutdown().wait().expect("Kompact shutdown");
    }
}
