//! Key-material discovery component tests.

use super::*;
use crate::{
    protocol::{
        DiscoveryEndpointFrameSrc,
        KeyBundleLookupResponsePayloadSrc,
        decode_endpoint_discovery_frame_from_buf,
    },
    route_establishment::{DiscoveryCredentialFuture, DiscoveryKeyMaterialStatusFuture},
    test_support::{endpoint_payload, generated_keys, member, test_egress_pool},
};
use flotsync_io::{
    prelude::{IoPayload, SocketId, UdpCloseReason},
    test_support::{
        build_test_kompact_system_with,
        eventually,
        eventually_component_state,
        kill_component,
        start_component,
    },
};
use flotsync_messages::{
    buffa::MessageField,
    discovery::discovery_frame,
    wire::uuid_to_wire_bytes,
};
use flotsync_security::{
    FrameSignature,
    LocalMemberKeys,
    PublicKeyBundle,
    SIGNATURE_LENGTH,
    sign_discovery_payload,
};
use flotsync_utils::{
    BoxError,
    kompact_testing::{PortTesterComponent, PortTestingExt, PortTestingRefExt},
};
use futures_util::FutureExt as _;
use std::{cell::Cell, collections::HashMap, io, sync::Mutex, time::Duration};

/// One observed direct UDP send from key-material discovery.
struct ObservedUdpSend {
    /// Socket used for the send request.
    socket_id: SocketId,
    /// Destination address for the datagram.
    target: Option<SocketAddr>,
    /// Encoded endpoint frame sent as the UDP payload.
    payload: IoPayload,
}

fn decode_lookup_request(send: &ObservedUdpSend) -> DecodedKeyBundleLookupRequest {
    let mut cursor = send.payload.cursor();
    let discovery_frame = decode_endpoint_discovery_frame_from_buf(&mut cursor)
        .expect("endpoint frame should decode")
        .expect("submit should contain discovery frame");
    match discovery_frame.body {
        Some(discovery_frame::Body::KeyBundleLookupRequest(request)) => {
            DecodedKeyBundleLookupRequest::decode_proto(*request)
                .expect("lookup request should decode")
        }
        other => panic!("expected lookup request, got {other:?}"),
    }
}

fn loopback(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

#[derive(Clone, Copy)]
enum RequestedFingerprint {
    /// Use the generated bundle's real fingerprint as the lookup selector.
    Bundle,
    /// Use an explicit fingerprint that may intentionally differ from the bundle.
    Exact(KeyFingerprint),
}

impl RequestedFingerprint {
    fn resolve(self, bundle: &PublicKeyBundle) -> KeyFingerprint {
        match self {
            Self::Bundle => bundle.fingerprint(),
            Self::Exact(fingerprint) => fingerprint,
        }
    }
}

fn signed_lookup_response(
    member: &MemberIdentity,
    fingerprint: KeyFingerprint,
    nonce: Uuid,
    bundle: &PublicKeyBundle,
    signer: &LocalMemberKeys,
) -> discovery_proto::SignedKeyBundleLookupResponse {
    let payload = KeyBundleLookupResponsePayloadSrc {
        member,
        key_fingerprint: fingerprint,
        request_nonce: nonce,
        public_key_bundle: bundle,
    }
    .encode_proto();
    let payload = payload.encode_to_vec();
    let signature = sign_discovery_payload(signer, &payload).expect("response should sign");
    discovery_proto::SignedKeyBundleLookupResponse {
        response_payload: payload,
        signature: MessageField::some(signature.encode_proto()),
        ..discovery_proto::SignedKeyBundleLookupResponse::default()
    }
}

fn malformed_bundle_response(
    member: &MemberIdentity,
    fingerprint: KeyFingerprint,
    nonce: Uuid,
) -> discovery_proto::SignedKeyBundleLookupResponse {
    let (member_id, key_fingerprint) =
        crate::protocol::encode_member_key_selector_fields(member, fingerprint);
    let payload = discovery_proto::KeyBundleLookupResponsePayload {
        member_id: MessageField::some(member_id),
        key_fingerprint,
        request_nonce: uuid_to_wire_bytes(nonce),
        public_key_bundle: MessageField::some(flotsync_messages::security::PublicKeyBundle {
            format_version: 1,
            ..flotsync_messages::security::PublicKeyBundle::default()
        }),
        ..discovery_proto::KeyBundleLookupResponsePayload::default()
    };
    discovery_proto::SignedKeyBundleLookupResponse {
        response_payload: payload.encode_to_vec(),
        signature: MessageField::some(
            FrameSignature::from_bytes([0; SIGNATURE_LENGTH]).encode_proto(),
        ),
        ..discovery_proto::SignedKeyBundleLookupResponse::default()
    }
}

struct TestDiscoveryCredentials {
    /// Local signing keys exposed through the discovery credentials trait.
    local_keys: LocalMemberKeys,
    /// Public bundle returned by responder-side lookup tests.
    local_bundle: PublicKeyBundle,
    /// Per-key availability status returned to the component under test.
    statuses: Mutex<HashMap<(MemberIdentity, KeyFingerprint), DiscoveryKeyMaterialStatus>>,
    /// Status checks made by the component under test.
    status_checks: Mutex<Vec<(MemberIdentity, KeyFingerprint)>>,
    /// Bundles ensured by successful requester-side lookup responses.
    stored: Mutex<Vec<(MemberIdentity, PublicKeyBundle)>>,
}

impl TestDiscoveryCredentials {
    fn new(local_member: MemberIdentity) -> Self {
        let (local_keys, local_bundle) = generated_keys(local_member);
        Self {
            local_keys,
            local_bundle,
            statuses: Mutex::new(HashMap::new()),
            status_checks: Mutex::new(Vec::new()),
            stored: Mutex::new(Vec::new()),
        }
    }

    fn set_status(
        &self,
        member: MemberIdentity,
        fingerprint: KeyFingerprint,
        status: DiscoveryKeyMaterialStatus,
    ) {
        self.statuses
            .lock()
            .expect("status mutex should lock")
            .insert((member, fingerprint), status);
    }

    fn stored_fingerprints(&self) -> Vec<(MemberIdentity, KeyFingerprint)> {
        self.stored
            .lock()
            .expect("stored mutex should lock")
            .iter()
            .map(|(member, bundle)| (member.clone(), bundle.fingerprint()))
            .collect()
    }

    fn status_check_count(&self) -> usize {
        self.status_checks
            .lock()
            .expect("status check mutex should lock")
            .len()
    }
}

impl DiscoveryCredentials for TestDiscoveryCredentials {
    fn local_discovery_key_fingerprint(&self) -> KeyFingerprint {
        self.local_bundle.fingerprint()
    }

    fn local_discovery_public_key_bundle(&self) -> PublicKeyBundle {
        self.local_bundle.clone()
    }

    fn sign_discovery_payload(&self, payload: &[u8]) -> Result<FrameSignature, BoxError> {
        sign_discovery_payload(&self.local_keys, payload).map_err(Into::into)
    }

    fn discovery_key_material_status<'a>(
        &'a self,
        member: &'a MemberIdentity,
        key_fingerprint: KeyFingerprint,
    ) -> DiscoveryKeyMaterialStatusFuture<'a> {
        self.status_checks
            .lock()
            .expect("status check mutex should lock")
            .push((member.clone(), key_fingerprint));
        let status = self
            .statuses
            .lock()
            .expect("status mutex should lock")
            .get(&(member.clone(), key_fingerprint))
            .copied()
            .unwrap_or(DiscoveryKeyMaterialStatus::Available);
        std::future::ready(Ok(status)).boxed()
    }

    fn verify_discovery_claim_payload<'a>(
        &'a self,
        _member: &'a MemberIdentity,
        _key_fingerprint: KeyFingerprint,
        _payload: &'a [u8],
        _signature: &'a FrameSignature,
    ) -> DiscoveryCredentialFuture<'a> {
        std::future::ready(Err(Box::new(io::Error::other(
            "claim verification is unused in key-material tests",
        )) as BoxError))
        .boxed()
    }

    fn permit_member_route_publication<'a>(
        &'a self,
        _member: &'a MemberIdentity,
        _key_fingerprint: KeyFingerprint,
    ) -> DiscoveryCredentialFuture<'a> {
        std::future::ready(Ok(())).boxed()
    }

    fn ensure_discovery_public_key_bundle<'a>(
        &'a self,
        member: &'a MemberIdentity,
        bundle: PublicKeyBundle,
    ) -> DiscoveryCredentialFuture<'a> {
        self.stored
            .lock()
            .expect("stored mutex should lock")
            .push((member.clone(), bundle.clone()));
        self.statuses
            .lock()
            .expect("status mutex should lock")
            .insert(
                (member.clone(), bundle.fingerprint()),
                DiscoveryKeyMaterialStatus::Available,
            );
        std::future::ready(Ok(())).boxed()
    }
}

struct KeyMaterialDiscoveryHarness {
    /// Kompact system owning the test topology.
    system: KompactSystem,
    /// Port tester used to observe direct UDP sends and inject inbound UDP datagrams.
    udp: Arc<Component<PortTesterComponent<UdpPort>>>,
    /// Cursor into the UDP tester event log.
    udp_cursor: Cell<usize>,
    /// Key-material discovery component under test.
    component: Arc<Component<KeyMaterialDiscoveryComponent>>,
    /// Test credentials backing key-status checks, responses, and stored bundles.
    credentials: Arc<TestDiscoveryCredentials>,
}

impl KeyMaterialDiscoveryHarness {
    fn new(local_member: MemberIdentity) -> Self {
        Self::new_with_config(local_member, |_| {})
    }

    fn new_with_config(
        local_member: MemberIdentity,
        configure: impl FnOnce(&mut KompactConfig),
    ) -> Self {
        let credentials = Arc::new(TestDiscoveryCredentials::new(local_member.clone()));
        Self::with_credentials_and_config(local_member, credentials, configure)
    }

    fn with_credentials_and_config(
        local_member: MemberIdentity,
        credentials: Arc<TestDiscoveryCredentials>,
        configure: impl FnOnce(&mut KompactConfig),
    ) -> Self {
        let system = build_test_kompact_system_with(configure);
        let udp = system.create(UdpPort::tester_component_sidecar);
        let component_credentials: Arc<dyn DiscoveryCredentials> = credentials.clone();
        let component = system.create(move || {
            KeyMaterialDiscoveryComponent::new(
                local_member,
                component_credentials,
                test_egress_pool(),
            )
        });
        biconnect_components::<UdpPort, _, _>(&udp, &component).expect("connect UDP probe");
        start_component(&system, &udp);
        start_component(&system, &component);
        Self {
            system,
            udp,
            udp_cursor: Cell::new(0),
            component,
            credentials,
        }
    }

    fn bind_endpoint(&self, socket_id: SocketId, local_addr: SocketAddr) {
        let route_endpoint_lifecycle_port = self
            .component
            .on_definition(KeyMaterialDiscoveryComponent::route_endpoint_lifecycle_port);
        self.system.trigger_i(
            RouteEndpointLifecycle::Available(RouteEndpointBinding {
                socket_id,
                socket_bound_addr: local_addr,
            }),
            &route_endpoint_lifecycle_port,
        );
        eventually_component_state(
            Duration::from_secs(1),
            &self.component,
            |component| {
                component.local_endpoint.binding().is_some_and(|binding| {
                    binding.socket_id == socket_id && binding.local_addr == local_addr
                })
            },
            "key-material discovery should observe the bound endpoint",
        );
    }

    fn unbind_endpoint(&self, socket_id: SocketId, local_addr: SocketAddr) {
        let route_endpoint_lifecycle_port = self
            .component
            .on_definition(KeyMaterialDiscoveryComponent::route_endpoint_lifecycle_port);
        self.system.trigger_i(
            RouteEndpointLifecycle::Unavailable {
                binding: RouteEndpointBinding {
                    socket_id,
                    socket_bound_addr: local_addr,
                },
                reason: RouteEndpointUnavailableReason::Closed {
                    reason: UdpCloseReason::Requested,
                },
            },
            &route_endpoint_lifecycle_port,
        );
    }

    fn fetch_key_material(
        &self,
        route: SocketAddr,
        member: MemberIdentity,
        key_fingerprint: KeyFingerprint,
    ) {
        let key_material_discovery_port = self
            .component
            .on_definition(KeyMaterialDiscoveryComponent::key_material_discovery_port);
        self.system.trigger_r(
            FetchKeyMaterial {
                route: DiscoveryRoute::Udp(route),
                member,
                key_fingerprint,
            },
            &key_material_discovery_port,
        );
    }

    fn begin_missing_lookup(
        &self,
        socket_id: SocketId,
        local_endpoint: SocketAddr,
        remote_endpoint: SocketAddr,
        member: MemberIdentity,
        key_fingerprint: KeyFingerprint,
    ) -> DecodedKeyBundleLookupRequest {
        self.credentials.set_status(
            member.clone(),
            key_fingerprint,
            DiscoveryKeyMaterialStatus::Missing,
        );
        self.bind_endpoint(socket_id, local_endpoint);
        self.fetch_key_material(remote_endpoint, member, key_fingerprint);
        let send = self.recv_udp_send();
        assert_udp_send(&send, socket_id, remote_endpoint);
        decode_lookup_request(&send)
    }

    fn receive_udp(&self, socket_id: SocketId, source: SocketAddr, payload: IoPayload) {
        self.udp
            .actor_ref()
            .inject_indication(UdpIndication::Received {
                socket_id,
                source,
                payload,
            });
    }

    fn recv_udp_send(&self) -> ObservedUdpSend {
        let observed = self
            .udp
            .actor_ref()
            .observe_request_from(self.udp_cursor.get(), |request| {
                matches!(request, UdpRequest::Send { .. })
            })
            .wait_timeout(Duration::from_secs(1))
            .expect("UDP send should be observed")
            .expect("UDP probe should stay live");
        self.udp_cursor.set(observed.index() + 1);
        match observed.request() {
            UdpRequest::Send {
                socket_id,
                payload,
                target,
                ..
            } => ObservedUdpSend {
                socket_id: *socket_id,
                target: *target,
                payload: payload.clone(),
            },
            other => panic!("expected UDP send, got {other:?}"),
        }
    }

    fn expect_no_udp_send(&self, reason: &'static str) {
        self.udp
            .actor_ref()
            .fail_if_request_observed_from(
                self.udp_cursor.get(),
                Duration::from_millis(100),
                |request| matches!(request, UdpRequest::Send { .. }),
            )
            .wait_timeout(Duration::from_secs(1))
            .expect("UDP send absence check should complete")
            .expect("UDP probe should stay live")
            .expect(reason);
    }

    fn shutdown(self) {
        let Self {
            system,
            udp,
            udp_cursor: _,
            component,
            credentials: _,
        } = self;
        kill_component(&system, component);
        kill_component(&system, udp);
        system.shutdown().wait().expect("Kompact shutdown");
    }
}

fn assert_udp_send(send: &ObservedUdpSend, expected_socket: SocketId, expected_target: SocketAddr) {
    assert_eq!(send.socket_id, expected_socket);
    assert_eq!(send.target, Some(expected_target));
}

#[test]
fn key_material_discovery_loads_suppression_lease_from_config() {
    let configured_lease = Duration::from_millis(75);
    let harness = KeyMaterialDiscoveryHarness::new_with_config(member(["alice"]), |config| {
        config.set_config_value(
            &config_keys::KEY_MATERIAL_DISCOVERY_SUPPRESSION_LEASE,
            configured_lease,
        );
    });

    eventually_component_state(
        Duration::from_secs(1),
        &harness.component,
        |component| component.suppression_lease == configured_lease,
        "key-material discovery should load configured suppression lease",
    );
    harness.shutdown();
}

#[test]
fn fetch_key_material_sends_lookup_request() {
    let local_member = member(["alice"]);
    let remote_member = member(["bob"]);
    let (_remote_keys, remote_bundle) = generated_keys(remote_member.clone());
    let remote_fingerprint = remote_bundle.fingerprint();
    let local_endpoint = loopback(49131);
    let remote_endpoint = loopback(62191);
    let harness = KeyMaterialDiscoveryHarness::new(local_member);
    let request = harness.begin_missing_lookup(
        SocketId(131),
        local_endpoint,
        remote_endpoint,
        remote_member.clone(),
        remote_fingerprint,
    );

    assert_eq!(request.member, remote_member);
    assert_eq!(request.key_fingerprint, remote_fingerprint);
    assert_ne!(request.request_nonce, Uuid::nil());
    harness.shutdown();
}

#[test]
fn fetch_key_material_without_endpoint_does_not_check_storage() {
    let local_member = member(["alice"]);
    let remote_member = member(["bob"]);
    let (_remote_keys, remote_bundle) = generated_keys(remote_member.clone());
    let remote_fingerprint = remote_bundle.fingerprint();
    let remote_endpoint = loopback(62190);
    let harness = KeyMaterialDiscoveryHarness::new(local_member);

    harness.fetch_key_material(remote_endpoint, remote_member, remote_fingerprint);

    harness.expect_no_udp_send("unbound endpoint should prevent lookup");
    assert_eq!(harness.credentials.status_check_count(), 0);
    harness.shutdown();
}

#[test]
fn duplicate_fetch_key_material_is_suppressed_before_storage_check() {
    let local_member = member(["alice"]);
    let remote_member = member(["bob"]);
    let (_remote_keys, remote_bundle) = generated_keys(remote_member.clone());
    let remote_fingerprint = remote_bundle.fingerprint();
    let local_endpoint = loopback(49132);
    let remote_endpoint = loopback(62192);
    let harness = KeyMaterialDiscoveryHarness::new(local_member);
    harness.credentials.set_status(
        remote_member.clone(),
        remote_fingerprint,
        DiscoveryKeyMaterialStatus::Missing,
    );
    harness.bind_endpoint(SocketId(132), local_endpoint);

    harness.fetch_key_material(remote_endpoint, remote_member.clone(), remote_fingerprint);
    harness.fetch_key_material(remote_endpoint, remote_member, remote_fingerprint);

    let _send = harness.recv_udp_send();
    harness.expect_no_udp_send("duplicate lookup should be suppressed");
    assert_eq!(harness.credentials.status_check_count(), 1);
    harness.shutdown();
}

#[test]
fn endpoint_rebinding_cancels_pending_lookup_suppression() {
    let local_member = member(["alice"]);
    let remote_member = member(["bob"]);
    let (_remote_keys, remote_bundle) = generated_keys(remote_member.clone());
    let remote_fingerprint = remote_bundle.fingerprint();
    let first_local_endpoint = loopback(49140);
    let second_local_endpoint = loopback(49141);
    let remote_endpoint = loopback(62200);
    let harness = KeyMaterialDiscoveryHarness::new(local_member);

    let _request = harness.begin_missing_lookup(
        SocketId(140),
        first_local_endpoint,
        remote_endpoint,
        remote_member.clone(),
        remote_fingerprint,
    );
    harness.bind_endpoint(SocketId(141), second_local_endpoint);

    harness.fetch_key_material(remote_endpoint, remote_member.clone(), remote_fingerprint);

    let send = harness.recv_udp_send();
    assert_udp_send(&send, SocketId(141), remote_endpoint);
    let request = decode_lookup_request(&send);
    assert_eq!(request.member, remote_member);
    assert_eq!(request.key_fingerprint, remote_fingerprint);
    harness.shutdown();
}

#[test]
fn stale_endpoint_unavailable_does_not_clear_current_binding() {
    let local_member = member(["alice"]);
    let remote_member = member(["bob"]);
    let (_remote_keys, remote_bundle) = generated_keys(remote_member.clone());
    let remote_fingerprint = remote_bundle.fingerprint();
    let stale_local_endpoint = loopback(49142);
    let current_local_endpoint = loopback(49143);
    let remote_endpoint = loopback(62201);
    let harness = KeyMaterialDiscoveryHarness::new(local_member);
    harness.bind_endpoint(SocketId(142), current_local_endpoint);

    harness.unbind_endpoint(SocketId(142), stale_local_endpoint);
    harness.credentials.set_status(
        remote_member.clone(),
        remote_fingerprint,
        DiscoveryKeyMaterialStatus::Missing,
    );
    harness.fetch_key_material(remote_endpoint, remote_member.clone(), remote_fingerprint);

    let send = harness.recv_udp_send();
    assert_udp_send(&send, SocketId(142), remote_endpoint);
    let request = decode_lookup_request(&send);
    assert_eq!(request.member, remote_member);
    assert_eq!(request.key_fingerprint, remote_fingerprint);
    harness.shutdown();
}

#[test]
fn responder_ignores_non_local_lookup_request() {
    let local_member = member(["alice"]);
    let remote_member = member(["bob"]);
    let (_remote_keys, remote_bundle) = generated_keys(remote_member.clone());
    let local_endpoint = loopback(49133);
    let remote_endpoint = loopback(62193);
    let harness = KeyMaterialDiscoveryHarness::new(local_member);
    harness.bind_endpoint(SocketId(133), local_endpoint);
    let request = DiscoveryEndpointFrameSrc::KeyBundleLookupRequest {
        member: &remote_member,
        key_fingerprint: remote_bundle.fingerprint(),
        request_nonce: Uuid::from_u128(1330),
    }
    .encode_proto();
    let payload = endpoint_payload(&request);

    harness.receive_udp(SocketId(133), remote_endpoint, payload);

    harness.expect_no_udp_send("non-local lookup request should not be answered");
    harness.shutdown();
}

#[test]
fn responder_ignores_lookup_request_on_non_endpoint_socket() {
    let local_member = member(["alice"]);
    let local_endpoint = loopback(49144);
    let remote_endpoint = loopback(62193);
    let harness = KeyMaterialDiscoveryHarness::new(local_member.clone());
    harness.bind_endpoint(SocketId(144), local_endpoint);
    let request = DiscoveryEndpointFrameSrc::KeyBundleLookupRequest {
        member: &local_member,
        key_fingerprint: harness.credentials.local_discovery_key_fingerprint(),
        request_nonce: Uuid::from_u128(1440),
    }
    .encode_proto();
    let payload = endpoint_payload(&request);

    harness.receive_udp(SocketId(145), remote_endpoint, payload);

    harness.expect_no_udp_send("traffic on another socket should be ignored");
    harness.shutdown();
}

#[test]
fn responder_ignores_non_endpoint_bytes_on_active_socket() {
    let local_member = member(["alice"]);
    let local_endpoint = loopback(49145);
    let remote_endpoint = loopback(62193);
    let harness = KeyMaterialDiscoveryHarness::new(local_member);
    harness.bind_endpoint(SocketId(145), local_endpoint);

    harness.receive_udp(
        SocketId(145),
        remote_endpoint,
        IoPayload::from_static(b"\0"),
    );

    harness.expect_no_udp_send("non-endpoint bytes should be ignored");
    harness.shutdown();
}

#[test]
fn local_lookup_request_sends_signed_response() {
    let local_member = member(["alice"]);
    let local_endpoint = loopback(49134);
    let remote_endpoint = loopback(62194);
    let harness = KeyMaterialDiscoveryHarness::new(local_member.clone());
    harness.bind_endpoint(SocketId(134), local_endpoint);
    let request_nonce = Uuid::from_u128(1340);
    let request = DiscoveryEndpointFrameSrc::KeyBundleLookupRequest {
        member: &local_member,
        key_fingerprint: harness.credentials.local_discovery_key_fingerprint(),
        request_nonce,
    }
    .encode_proto();
    let payload = endpoint_payload(&request);

    harness.receive_udp(SocketId(134), remote_endpoint, payload);

    let send = harness.recv_udp_send();
    assert_udp_send(&send, SocketId(134), remote_endpoint);
    let mut cursor = send.payload.cursor();
    let discovery_frame = decode_endpoint_discovery_frame_from_buf(&mut cursor)
        .expect("response endpoint frame should decode")
        .expect("response should contain discovery frame");
    let response = match discovery_frame.body {
        Some(discovery_frame::Body::KeyBundleLookupResponse(response)) => *response,
        other => panic!("expected lookup response, got {other:?}"),
    };
    let decoded = DecodedKeyBundleLookupResponsePayload::try_decode_proto_from_slice(
        &response.response_payload,
    )
    .expect("response payload should validate");
    assert_eq!(decoded.member, local_member);
    assert_eq!(decoded.request_nonce, request_nonce);
    assert_eq!(
        decoded.key_fingerprint,
        harness.credentials.local_discovery_key_fingerprint()
    );
    harness.shutdown();
}

#[test]
fn valid_lookup_response_stores_candidate_bundle() {
    let local_member = member(["alice"]);
    let remote_member = member(["bob"]);
    let (remote_keys, remote_bundle) = generated_keys(remote_member.clone());
    let remote_fingerprint = remote_bundle.fingerprint();
    let local_endpoint = loopback(49135);
    let remote_endpoint = loopback(62195);
    let harness = KeyMaterialDiscoveryHarness::new(local_member);
    let request = harness.begin_missing_lookup(
        SocketId(135),
        local_endpoint,
        remote_endpoint,
        remote_member.clone(),
        remote_fingerprint,
    );
    let response = signed_lookup_response(
        &remote_member,
        remote_fingerprint,
        request.request_nonce,
        &remote_bundle,
        &remote_keys,
    );
    let frame = DiscoveryEndpointFrameSrc::KeyBundleLookupResponse {
        response: &response,
    }
    .encode_proto();

    harness.receive_udp(SocketId(135), remote_endpoint, endpoint_payload(&frame));

    eventually(
        Duration::from_secs(1),
        || {
            harness
                .credentials
                .stored_fingerprints()
                .contains(&(remote_member.clone(), remote_fingerprint))
        },
        "valid response should store candidate bundle",
    );
    harness.shutdown();
}

fn assert_invalid_lookup_response_is_not_stored(
    case: &'static str,
    socket_id: SocketId,
    local_endpoint: SocketAddr,
    remote_endpoint: SocketAddr,
    requested_fingerprint: RequestedFingerprint,
    build_response: impl FnOnce(
        &MemberIdentity,
        &LocalMemberKeys,
        &PublicKeyBundle,
        Uuid,
        KeyFingerprint,
    ) -> discovery_proto::SignedKeyBundleLookupResponse,
) {
    let local_member = member(["alice"]);
    let remote_member = member(["bob"]);
    let (remote_keys, remote_bundle) = generated_keys(remote_member.clone());
    let requested_fingerprint = requested_fingerprint.resolve(&remote_bundle);
    let harness = KeyMaterialDiscoveryHarness::new(local_member);
    let request = harness.begin_missing_lookup(
        socket_id,
        local_endpoint,
        remote_endpoint,
        remote_member.clone(),
        requested_fingerprint,
    );
    let response = build_response(
        &remote_member,
        &remote_keys,
        &remote_bundle,
        request.request_nonce,
        requested_fingerprint,
    );
    let frame = DiscoveryEndpointFrameSrc::KeyBundleLookupResponse {
        response: &response,
    }
    .encode_proto();

    harness.receive_udp(socket_id, remote_endpoint, endpoint_payload(&frame));

    harness.expect_no_udp_send("invalid response should not trigger new traffic");
    assert!(
        harness.credentials.stored_fingerprints().is_empty(),
        "{case} should not store key material",
    );
    harness.shutdown();
}

#[test]
fn invalid_lookup_responses_are_not_stored() {
    assert_invalid_lookup_response_is_not_stored(
        "wrong fingerprint",
        SocketId(136),
        loopback(49136),
        loopback(62196),
        RequestedFingerprint::Exact(KeyFingerprint::from_bytes([9; 32])),
        |remote_member, remote_keys, remote_bundle, nonce, requested_fingerprint| {
            signed_lookup_response(
                remote_member,
                requested_fingerprint,
                nonce,
                remote_bundle,
                remote_keys,
            )
        },
    );
    assert_invalid_lookup_response_is_not_stored(
        "wrong member",
        SocketId(137),
        loopback(49137),
        loopback(62197),
        RequestedFingerprint::Bundle,
        |_remote_member, remote_keys, remote_bundle, nonce, requested_fingerprint| {
            let other_member = member(["charlie"]);
            signed_lookup_response(
                &other_member,
                requested_fingerprint,
                nonce,
                remote_bundle,
                remote_keys,
            )
        },
    );
    assert_invalid_lookup_response_is_not_stored(
        "nonce mismatch",
        SocketId(138),
        loopback(49138),
        loopback(62198),
        RequestedFingerprint::Bundle,
        |remote_member, remote_keys, remote_bundle, _nonce, requested_fingerprint| {
            signed_lookup_response(
                remote_member,
                requested_fingerprint,
                Uuid::from_u128(1380),
                remote_bundle,
                remote_keys,
            )
        },
    );
    assert_invalid_lookup_response_is_not_stored(
        "malformed bundle",
        SocketId(139),
        loopback(49139),
        loopback(62199),
        RequestedFingerprint::Bundle,
        |remote_member, _remote_keys, _remote_bundle, nonce, requested_fingerprint| {
            malformed_bundle_response(remote_member, requested_fingerprint, nonce)
        },
    );
}
