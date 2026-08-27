//! Unit tests for reliable delivery.

use super::*;
use crate::{
    delivery::ingress::{DeliveryIngressComponent, DeliveryInterestConfig, DeliveryTargetHint},
    store::test_support::{ControlledStore, ReliableDeliveryLoadFault, classified_store_error},
    test_support::{
        SqliteStoreTestOwner,
        TestGroupMemberships,
        load_test_delivery_security,
        provision_test_security,
        provisioned_sqlite_store,
    },
};
use flotsync_core::{ApplicationId, GroupId};
use flotsync_io::{
    prelude::UdpLocalBind,
    test_support::{
        WAIT_TIMEOUT,
        assert_never,
        eventually,
        eventually_component_state,
        localhost,
        start_component,
    },
};
use flotsync_routes::{
    DatagramRouteScope,
    InboundTransportMeta,
    RoutePreferenceRank,
    RouteTransportNackReason,
    RouteTransportPort,
    UdpRouteKey,
    test_support::{
        FULL_STACK_WAIT_TIMEOUT,
        TransportHarnessCore,
        build_delivery_test_system,
        build_delivery_test_system_with,
        default_udpour_config,
        member_identity,
    },
};
use flotsync_utils::kompact_testing::{PortTesterComponent, PortTestingExt, PortTestingRefExt};
use futures_util::{pin_mut, task::noop_waker_ref};
use snafu::ResultExt as _;
use std::{
    cell::Cell,
    collections::{BTreeSet, HashSet},
    future::Future as _,
    net::SocketAddr,
    sync::mpsc,
    task::Context,
    time::{Duration, Instant, SystemTime},
};

const TEST_RECIPIENT_ACK_TIMEOUT: Duration = Duration::from_millis(50);
/// Observation window used by negative ack tests to catch accidental async
/// state transitions without sleeping a fixed one-shot delay.
const REJECTED_ACK_OBSERVATION_WINDOW: Duration = Duration::from_millis(100);

type TestDeliverySecurity = SqliteStoreTestOwner<DeliverySecurity>;

// TODO(flotsync-h1z0): Replace this custom probe once generic testing
// helpers can hand owned indication payloads such as processed handles to
// tests.
#[derive(ComponentDefinition)]
struct ReliableDeliveryClientProbe {
    /// Component context for Kompact lifecycle integration.
    ctx: ComponentContext<Self>,
    /// Required external reliable-delivery port observed by the probe.
    delivery: RequiredPort<ReliableDeliveryPort>,
    /// Channel receiving owned client indications for assertions.
    indications: mpsc::Sender<ReliableDeliveryPortIndication>,
}

impl ReliableDeliveryClientProbe {
    fn new(indications: mpsc::Sender<ReliableDeliveryPortIndication>) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            delivery: RequiredPort::uninitialised(),
            indications,
        }
    }
}

ignore_lifecycle!(ReliableDeliveryClientProbe);

impl Require<ReliableDeliveryPort> for ReliableDeliveryClientProbe {
    fn handle(&mut self, indication: ReliableDeliveryPortIndication) -> HandlerResult {
        self.indications
            .send(indication)
            .expect("reliable delivery indication receiver must stay live");
        Handled::OK
    }
}

impl Actor for ReliableDeliveryClientProbe {
    type Message = Never;

    fn receive_local(&mut self, _msg: Self::Message) -> HandlerResult {
        unreachable!("Never type is empty")
    }
}

/// One route-transport submission retained until the test completes it.
struct ControlledTransportSubmit {
    /// Submitted route and payload owned by the transport boundary.
    send: RouteTransportSend<TransportRouteKey>,
    /// Completion handle that releases the reliable-delivery attempt slot.
    completion: KPromise<RouteTransportSubmitResult<TransportRouteKey>>,
}

/// Route-transport actor that exposes submissions without completing them.
#[derive(ComponentDefinition)]
struct ControlledRouteTransportComponent {
    /// Kompact component context for actor scheduling.
    ctx: ComponentContext<Self>,
    /// Observation channel receiving each pending submission.
    submits: mpsc::Sender<ControlledTransportSubmit>,
}

impl ControlledRouteTransportComponent {
    /// Build one controlled route-transport actor around the observation channel.
    fn new(submits: mpsc::Sender<ControlledTransportSubmit>) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            submits,
        }
    }
}

ignore_lifecycle!(ControlledRouteTransportComponent);

impl Actor for ControlledRouteTransportComponent {
    type Message = RouteTransportActorMessage<TransportRouteKey>;

    fn receive_local(&mut self, message: Self::Message) -> HandlerResult {
        match message {
            RouteTransportActorMessage::Submit(ask) => {
                let (completion, send) = ask.take();
                self.submits
                    .send(ControlledTransportSubmit { send, completion })
                    .expect("controlled route-transport observer must stay live");
            }
            RouteTransportActorMessage::RegisterExternalUdpSocket(ask) => {
                let (completion, _registration) = ask.take();
                let _fulfilled = completion.fulfil(Ok(()));
            }
        }
        Handled::OK
    }
}

struct FullStackHarness {
    core: TransportHarnessCore,
    store: Arc<crate::SqliteReplicationStore>,
    /// Whether this harness represents the final owner of the shared application store.
    close_store_on_drop: bool,
    ingress: Arc<Component<DeliveryIngressComponent>>,
    reliable: Arc<Component<ReliableDeliveryComponent>>,
    ingress_probe: Arc<Component<PortTesterComponent<TransportReliableDeliveryInboundPort>>>,
    discovery_source: Arc<Component<PortTesterComponent<TransportRouteDiscoveryPort>>>,
    client: Arc<Component<ReliableDeliveryClientProbe>>,
    ingress_cursor: Cell<usize>,
    client_rx: mpsc::Receiver<ReliableDeliveryPortIndication>,
    local_addr: SocketAddr,
}

impl FullStackHarness {
    fn new(local_member: MemberIdentity) -> Self {
        Self::with_system(local_member, build_delivery_test_system())
    }

    fn with_recipient_ack_timeout(
        local_member: MemberIdentity,
        recipient_ack_timeout: Duration,
    ) -> Self {
        let system = build_delivery_test_system_with(|config| {
            config.set_config_value(&config_keys::RECIPIENT_ACK_TIMEOUT, recipient_ack_timeout);
        });
        Self::with_system(local_member, system)
    }

    fn with_system(local_member: MemberIdentity, system: KompactSystem) -> Self {
        let (security, store) = test_delivery_security_and_store(&local_member);
        let reliable_store: Arc<dyn ReliableDeliveryStore> = store.clone();
        Self::with_system_security_and_store(
            local_member,
            system,
            security,
            store,
            reliable_store,
            None,
        )
    }

    fn with_system_security_and_store(
        local_member: MemberIdentity,
        system: KompactSystem,
        security: DeliverySecurity,
        store: Arc<crate::SqliteReplicationStore>,
        reliable_store: Arc<dyn ReliableDeliveryStore>,
        controlled_transport: Option<&Arc<Component<ControlledRouteTransportComponent>>>,
    ) -> Self {
        let core = TransportHarnessCore::with_socket_budgets(
            system,
            default_udpour_config(),
            true,
            &[],
            0,
        );
        let manager_ref = controlled_transport.map_or_else(
            || core.manager_ref(),
            |transport| {
                transport
                    .actor_ref()
                    .hold()
                    .expect("controlled route transport must expose a strong actor ref")
            },
        );
        let local_members: Arc<HashSet<MemberIdentity>> =
            Arc::new([local_member].into_iter().collect());
        let ingress = core.system().create(move || {
            DeliveryIngressComponent::new(DeliveryInterestConfig {
                group_memberships: TestGroupMemberships::default().shared(),
                local_members,
                hosted_mailboxes: Arc::new(HashSet::new()),
            })
        });
        let reliable_security = security;
        let reliable = core.system().create(move || {
            ReliableDeliveryComponent::new(
                manager_ref.clone(),
                reliable_security.clone(),
                reliable_store.clone(),
            )
        });
        let ingress_probe = core
            .system()
            .create(TransportReliableDeliveryInboundPort::tester_component_sidecar);
        let discovery_source = core
            .system()
            .create(TransportRouteDiscoveryPort::tester_component_sidecar);
        let (client_tx, client_rx) = mpsc::channel();
        let client = core
            .system()
            .create(move || ReliableDeliveryClientProbe::new(client_tx));

        biconnect_components::<RouteTransportPort<TransportRouteKey>, _, _>(
            core.manager(),
            &ingress,
        )
        .expect("route transport manager must connect to delivery ingress");
        biconnect_components::<TransportReliableDeliveryInboundPort, _, _>(&ingress, &reliable)
            .expect("delivery ingress must connect to reliable delivery");
        biconnect_components::<TransportReliableDeliveryInboundPort, _, _>(
            &ingress,
            &ingress_probe,
        )
        .expect("delivery ingress must connect to reliable delivery ingress probe");
        biconnect_components::<TransportRouteDiscoveryPort, _, _>(&discovery_source, &reliable)
            .expect("discovery source must connect to reliable delivery");
        biconnect_components::<ReliableDeliveryPort, _, _>(&reliable, &client)
            .expect("reliable delivery must connect to the external client probe");

        core.start();
        if let Some(transport) = controlled_transport {
            start_component(core.system(), transport);
        }
        start_component(core.system(), &ingress);
        start_component(core.system(), &reliable);
        start_component(core.system(), &ingress_probe);
        start_component(core.system(), &discovery_source);
        start_component(core.system(), &client);

        let (socket_id, local_addr) =
            core.bind_external_socket(UdpLocalBind::Exact(localhost(0)), FULL_STACK_WAIT_TIMEOUT);
        core.wait_for_manager_external_socket_binding(
            socket_id,
            local_addr,
            FULL_STACK_WAIT_TIMEOUT,
        );

        Self {
            core,
            store,
            close_store_on_drop: true,
            ingress,
            reliable,
            ingress_probe,
            discovery_source,
            client,
            ingress_cursor: Cell::new(0),
            client_rx,
            local_addr,
        }
    }

    /// Shut down this runtime while leaving its store open for a replacement runtime.
    fn shutdown_for_restart(mut self) {
        self.close_store_on_drop = false;
    }

    fn publish_direct_route(&self, peer: MemberIdentity, remote_addr: SocketAddr) {
        let route = SendRouteCandidate {
            coverage_key: TransportRouteKey::Udp(UdpRouteKey {
                remote_addr,
                scope: DatagramRouteScope::Unicast,
                local_bind: Some(self.local_addr),
            }),
            sharing: RouteSharingKind::Exclusive,
            preference_rank: RoutePreferenceRank::new(1),
        };
        let expected_peer = peer.clone();
        self.discovery_source.actor_ref().inject_indication(
            TransportDiscoveryRouteUpdate::PeerRoutes {
                peer,
                routes: vec![route],
            },
        );
        eventually_component_state(
            WAIT_TIMEOUT,
            &self.reliable,
            |component| component.knows_direct_route(&expected_peer),
            "timed out waiting for reliable-delivery route publication",
        );
    }

    fn submit(&self, submit: ReliableDeliverySubmit) {
        self.client.on_definition(|component| {
            component
                .delivery
                .trigger(ReliableDeliveryPortRequest::Submit(submit));
        });
    }

    fn wait_for_delivery(&self) -> ReliableDeliveryDeliver {
        match self
            .client_rx
            .recv_timeout(WAIT_TIMEOUT)
            .expect("timed out waiting for reliable delivery indication")
        {
            ReliableDeliveryPortIndication::Deliver(deliver) => deliver,
        }
    }

    fn expect_no_delivery(&self, timeout: Duration) {
        match self.client_rx.recv_timeout(timeout) {
            Ok(ReliableDeliveryPortIndication::Deliver(deliver)) => panic!(
                "unexpected reliable delivery indication for message_id={}",
                deliver.envelope.header.message_id
            ),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!("reliable delivery indication sender disconnected")
            }
        }
    }

    /// Return the next ingress envelope for `message_id` so a test can replay its exact frame.
    fn wait_for_ingress_envelope(
        &self,
        message_id: MessageId,
    ) -> delivery_proto::ReliableDeliveryFrame {
        let deadline = Instant::now() + WAIT_TIMEOUT;
        loop {
            let timeout = deadline.saturating_duration_since(Instant::now());
            let observed = self
                .ingress_probe
                .actor_ref()
                .observe_indication_from(self.ingress_cursor.get(), |_| true)
                .wait_timeout(timeout)
                .expect("timed out waiting for reliable delivery ingress envelope")
                .expect("reliable delivery ingress probe should stay live");
            self.ingress_cursor.set(observed.index() + 1);
            let frame = observed.indication().frame.clone();
            let Some(delivery_proto::reliable_delivery_frame::Body::Envelope(envelope)) =
                frame.body.as_ref()
            else {
                continue;
            };
            let envelope = reliable_envelope_from_wire((**envelope).clone())
                .expect("ingress probe should observe decodable reliable envelopes");
            if envelope.header.message_id == message_id {
                return frame;
            }
        }
    }

    fn inject_recipient_ack(&self, ack: &RecipientAck) {
        let frame = ack.to_wire_format();
        let Some(endpoint_proto::endpoint_frame::Boundary::ReliableDelivery(frame)) =
            frame.boundary
        else {
            panic!("recipient ack must encode as reliable delivery endpoint branch");
        };
        self.inject_reliable_frame(*frame);
    }

    fn inject_recipient_ack_wire(&self, ack: delivery_proto::RecipientAckWire) {
        self.inject_reliable_frame(delivery_proto::ReliableDeliveryFrame {
            body: Some(delivery_proto::reliable_delivery_frame::Body::RecipientAck(
                Box::new(ack),
            )),
            ..Default::default()
        });
    }

    /// Inject one decoded reliable-delivery frame through the component's real
    /// required port so returned `Handled::BlockOn` state is honoured.
    fn inject_reliable_frame(&self, frame: delivery_proto::ReliableDeliveryFrame) {
        let ingress_port = self
            .reliable
            .on_definition(|component| component.ingress_inbound_port.share());
        let placeholder_message_id = MessageId(Uuid::nil());
        self.core.system().trigger_i(
            ReliableDeliveryInboundDeliver {
                meta: InboundDeliveryMeta {
                    transport: InboundTransportMeta {
                        route: TransportRouteKey::Udp(UdpRouteKey {
                            remote_addr: localhost(9),
                            scope: DatagramRouteScope::Unicast,
                            local_bind: Some(self.local_addr),
                        }),
                        remote_addr: Some(localhost(9)),
                    },
                    target: DeliveryTargetHint::OriginalSender {
                        original_sender: member_identity(&["injected-ack-sender"]),
                        delivery_message_id: placeholder_message_id,
                    },
                    delivery_message_id: Some(placeholder_message_id),
                    verified_sender: None,
                },
                frame,
            },
            &ingress_port,
        );
    }

    fn wait_for_sender_work_clear(&self, message_id: MessageId) {
        eventually_component_state(
            WAIT_TIMEOUT,
            &self.reliable,
            |component| component.sender_work_item(message_id).is_none(),
            format_args!("timed out waiting for sender work cleanup for {message_id:?}"),
        );
        assert!(
            block_on(self.store.load_reliable_delivery_work(message_id))
                .expect("stored reliable work load should succeed")
                .is_none(),
            "acknowledged sender work should be removed from storage"
        );
    }

    fn wait_for_ack_timeout_reported(&self, message_id: MessageId, expected: bool) {
        eventually_component_state(
            WAIT_TIMEOUT,
            &self.reliable,
            |component| component.reported_recipient_ack_timeout(message_id) == expected,
            format_args!(
                "timed out waiting for recipient-ack timeout reporting state {expected} for {message_id:?}"
            ),
        );
    }

    fn expect_sender_work_retained(&self, message_id: MessageId) {
        assert_never(
            REJECTED_ACK_OBSERVATION_WINDOW,
            || {
                self.reliable
                    .on_definition(|component| component.sender_work_item(message_id).is_none())
            },
            format_args!("rejected recipient ack must not remove sender work for {message_id:?}"),
        );
        self.reliable.on_definition(|component| {
            assert!(component.sender_work_item(message_id).is_some());
        });
    }

    fn wait_for_sender_route_state(&self, message_id: MessageId, expected: &RouteActiveState) {
        eventually_component_state(
            WAIT_TIMEOUT,
            &self.reliable,
            |component| {
                component
                    .sender_work_item(message_id)
                    .is_some_and(|work_item| &work_item.recipient_route.state == expected)
            },
            format_args!(
                "timed out waiting for sender-side route state {expected:?} for {message_id:?}"
            ),
        );
    }

    fn wait_for_inbound_state(&self, message_id: MessageId, expected: PendingInboundDeliveryState) {
        eventually_component_state(
            WAIT_TIMEOUT,
            &self.reliable,
            |component| component.inbound_delivery_state(message_id) == Some(expected),
            format_args!("timed out waiting for inbound state {expected:?} for {message_id:?}"),
        );
    }

    fn wait_for_inbound_clear(&self, message_id: MessageId) {
        eventually_component_state(
            WAIT_TIMEOUT,
            &self.reliable,
            |component| component.inbound_delivery_state(message_id).is_none(),
            format_args!("timed out waiting for inbound delivery cleanup for {message_id:?}"),
        );
    }

    fn wait_for_stored_envelope(&self, message_id: MessageId) -> Bytes {
        eventually_component_state(
            WAIT_TIMEOUT,
            &self.reliable,
            |component| component.sender_work_item(message_id).is_some(),
            format_args!("timed out waiting for persisted sender work for {message_id}"),
        );
        block_on(self.store.load_reliable_delivery_work(message_id))
            .expect("stored reliable work load should succeed")
            .expect("stored reliable work should exist")
            .encoded_envelope
    }
}

/// Build one complete test classification.
fn store_classification(
    scope: StoreErrorScope,
    class: StoreErrorClass,
    resolution: StoreErrorResolution,
) -> StoreErrorClassification {
    StoreErrorClassification::UNKNOWN
        .with_scope(scope)
        .with_class(class)
        .with_resolution(resolution)
}

/// Build the ordinary transient classification used by retry behaviour tests.
fn retryable_store_classification() -> StoreErrorClassification {
    store_classification(
        StoreErrorScope::Transaction,
        StoreErrorClass::ConcurrentAccess,
        StoreErrorResolution::Retry,
    )
}

/// Build one non-retryable failure which cannot safely be isolated to one stored row.
fn fatal_store_classification() -> StoreErrorClassification {
    store_classification(
        StoreErrorScope::Store,
        StoreErrorClass::Configuration,
        StoreErrorResolution::Reconfigure,
    )
}

/// Build the exact selected-row classification which authorises record isolation.
fn invalid_record_classification() -> StoreErrorClassification {
    store_classification(
        StoreErrorScope::Record,
        StoreErrorClass::InvalidData,
        StoreErrorResolution::Repair,
    )
}

impl Drop for FullStackHarness {
    fn drop(&mut self) {
        let _ = self
            .core
            .system()
            .kill_notify(self.client.clone())
            .wait_timeout(WAIT_TIMEOUT);
        let _ = self
            .core
            .system()
            .kill_notify(self.ingress_probe.clone())
            .wait_timeout(WAIT_TIMEOUT);
        let _ = self
            .core
            .system()
            .kill_notify(self.discovery_source.clone())
            .wait_timeout(WAIT_TIMEOUT);
        let _ = self
            .core
            .system()
            .kill_notify(self.reliable.clone())
            .wait_timeout(WAIT_TIMEOUT);
        let _ = self
            .core
            .system()
            .kill_notify(self.ingress.clone())
            .wait_timeout(WAIT_TIMEOUT);
        if self.close_store_on_drop {
            block_on(self.store.close()).expect("reliable-delivery test store should close");
        }
    }
}

/// Shared fixture for sender-side recipient-ack tests that need the same
/// Alice/Bob pending work item plus Bob/Charlie signing contexts.
struct RecipientAckScenario {
    alice: MemberIdentity,
    bob: MemberIdentity,
    charlie: MemberIdentity,
    sender: FullStackHarness,
    bob_security: TestDeliverySecurity,
    charlie_security: TestDeliverySecurity,
}

impl RecipientAckScenario {
    fn new() -> Self {
        let alice = member_identity(&["alice"]);
        let bob = member_identity(&["bob"]);
        let charlie = member_identity(&["charlie"]);
        let sender = FullStackHarness::new(alice.clone());
        let bob_security = test_delivery_security(&bob);
        let charlie_security = test_delivery_security(&charlie);
        Self {
            alice,
            bob,
            charlie,
            sender,
            bob_security,
            charlie_security,
        }
    }

    fn submit_pending(&self, message_id: MessageId, payload: &'static [u8]) {
        self.sender.submit(reliable_submit(
            self.alice.clone(),
            self.bob.clone(),
            message_id,
            payload,
        ));
        self.sender.wait_for_sender_route_state(
            message_id,
            &RouteActiveState::WaitingForRoute {
                reason: PendingRouteReason::ReachabilityUnknown,
            },
        );
    }

    fn bob_ack(&self, message_id: MessageId) -> RecipientAck {
        self.bob_ack_for(&self.alice, &self.bob, message_id)
    }

    fn bob_ack_for(
        &self,
        original_sender: &MemberIdentity,
        recipient: &MemberIdentity,
        message_id: MessageId,
    ) -> RecipientAck {
        recipient_ack(&self.bob_security, original_sender, recipient, message_id)
    }

    fn charlie_ack_for(
        &self,
        original_sender: &MemberIdentity,
        recipient: &MemberIdentity,
        message_id: MessageId,
    ) -> RecipientAck {
        recipient_ack(
            &self.charlie_security,
            original_sender,
            recipient,
            message_id,
        )
    }
}

fn reliable_submit(
    sender: MemberIdentity,
    recipient: MemberIdentity,
    message_id: MessageId,
    payload: &'static [u8],
) -> ReliableDeliverySubmit {
    ReliableDeliverySubmit {
        envelope: ReliableMessageEnvelope::<PlaintextPayload> {
            header: ReliableMessageHeader {
                sender,
                recipient,
                message_id,
                scope: ReliableMessageScope::DirectMessage,
            },
            payload: PlaintextPayload {
                bytes: Bytes::from_static(payload),
            },
        },
    }
}

fn test_send_route(
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
) -> SendRouteCandidate<TransportRouteKey> {
    SendRouteCandidate {
        coverage_key: TransportRouteKey::Udp(UdpRouteKey {
            remote_addr,
            scope: DatagramRouteScope::Unicast,
            local_bind: Some(local_addr),
        }),
        sharing: RouteSharingKind::Exclusive,
        preference_rank: RoutePreferenceRank::new(1),
    }
}

fn reliable_encrypted_envelope(
    scope: ReliableMessageScope,
) -> ReliableMessageEnvelope<EncryptedPayload> {
    ReliableMessageEnvelope::<EncryptedPayload> {
        header: ReliableMessageHeader {
            sender: member_identity(&["alice"]),
            recipient: member_identity(&["bob"]),
            message_id: MessageId(Uuid::from_u128(909)),
            scope,
        },
        payload: EncryptedPayload {
            sealed: SealedHPKEPayload {
                encapsulated_key: [7_u8; 32],
                ciphertext: vec![8_u8, 9_u8],
                signature: [10_u8; flotsync_security::SIGNATURE_LENGTH],
            },
        },
    }
}

fn round_trip_reliable_encrypted_envelope(
    scope: ReliableMessageScope,
) -> ReliableMessageEnvelope<EncryptedPayload> {
    let envelope = reliable_encrypted_envelope(scope);
    let endpoint = envelope.to_wire_format();
    let Some(endpoint_proto::endpoint_frame::Boundary::ReliableDelivery(frame)) = endpoint.boundary
    else {
        panic!("reliable envelope should encode as reliable delivery endpoint branch");
    };
    let Some(delivery_proto::reliable_delivery_frame::Body::Envelope(wire)) = frame.body else {
        panic!("reliable envelope should encode as envelope branch");
    };
    reliable_envelope_from_wire(*wire).expect("reliable envelope should decode")
}

#[test]
fn reliable_envelope_wire_round_trips_direct_message_scope() {
    let decoded = round_trip_reliable_encrypted_envelope(ReliableMessageScope::DirectMessage);

    assert_eq!(decoded.header.scope, ReliableMessageScope::DirectMessage);
}

#[test]
fn reliable_envelope_wire_round_trips_group_scope() {
    let scope = ReliableMessageScope::Group {
        group_id: GroupId(Uuid::from_u128(910)),
    };

    let decoded = round_trip_reliable_encrypted_envelope(scope);

    assert_eq!(decoded.header.scope, scope);
}

fn test_delivery_security_and_store(
    local_member: &MemberIdentity,
) -> (DeliverySecurity, Arc<crate::SqliteReplicationStore>) {
    let store = provisioned_sqlite_store(local_member);
    let application_id = ApplicationId::from_array(["reliable-delivery", "security-test"]);
    let trusted_members = [
        member_identity(&["alice"]),
        member_identity(&["bob"]),
        member_identity(&["charlie"]),
    ]
    .into_iter()
    .filter(|member| member != local_member);
    block_on(provision_test_security(
        application_id.clone(),
        store.as_ref(),
        local_member,
        trusted_members,
    ))
    .expect("test security should provision");
    let replication_store: Arc<dyn crate::api::ReplicationStore> = store.clone();
    let security = block_on(load_test_delivery_security(
        application_id,
        replication_store,
        local_member,
    ))
    .expect("test security should load");
    (security, store)
}

fn test_delivery_security(local_member: &MemberIdentity) -> TestDeliverySecurity {
    let (security, store) = test_delivery_security_and_store(local_member);
    SqliteStoreTestOwner::new(security, store)
}

fn recipient_ack(
    security: &DeliverySecurity,
    original_sender: &MemberIdentity,
    recipient: &MemberIdentity,
    message_id: MessageId,
) -> RecipientAck {
    let header = RecipientAckHeader {
        message_id,
        original_sender: original_sender.clone(),
        recipient: recipient.clone(),
    };
    let public_header = recipient_ack_public_header_bytes(&header);
    let signature = security
        .sign_recipient_ack(&header, public_header.as_ref())
        .expect("test recipient ack should sign");
    RecipientAck { header, signature }
}

fn malformed_recipient_ack_wire(
    original_sender: &MemberIdentity,
    recipient: &MemberIdentity,
    message_id: MessageId,
) -> delivery_proto::RecipientAckWire {
    delivery_proto::RecipientAckWire {
        public_header: flotsync_messages::buffa::MessageField::some(
            delivery_proto::RecipientAckHeader {
                message_id: message_id.0.as_bytes().to_vec(),
                original_sender: flotsync_messages::buffa::MessageField::some(
                    crate::delivery::wire::member_identity_to_wire_format(original_sender),
                ),
                recipient: flotsync_messages::buffa::MessageField::some(
                    crate::delivery::wire::member_identity_to_wire_format(recipient),
                ),
                ..delivery_proto::RecipientAckHeader::default()
            },
        ),
        signature: flotsync_messages::buffa::MessageField::some(
            delivery_proto::DetachedSignature {
                scheme: flotsync_messages::buffa::EnumValue::from(
                    flotsync_messages::security::SignatureScheme::SIGNATURE_SCHEME_ED25519PH,
                ),
                signature_bytes: Bytes::from_static(b"short"),
                ..delivery_proto::DetachedSignature::default()
            },
        ),
        ..delivery_proto::RecipientAckWire::default()
    }
}

#[test]
fn reliable_delivery_round_trips_direct_envelope_and_processed_ack() {
    let alice = member_identity(&["alice"]);
    let bob = member_identity(&["bob"]);
    let sender = FullStackHarness::new(alice.clone());
    let receiver = FullStackHarness::new(bob.clone());

    sender.publish_direct_route(bob.clone(), receiver.local_addr);
    receiver.publish_direct_route(alice.clone(), sender.local_addr);

    let message_id = MessageId(Uuid::from_u128(1));
    sender.submit(ReliableDeliverySubmit {
        envelope: ReliableMessageEnvelope::<PlaintextPayload> {
            header: ReliableMessageHeader {
                sender: alice,
                recipient: bob,
                message_id,
                scope: ReliableMessageScope::DirectMessage,
            },
            payload: PlaintextPayload {
                bytes: Bytes::from_static(b"bootstrap payload"),
            },
        },
    });

    let deliver = receiver.wait_for_delivery();
    assert_eq!(deliver.envelope.header.message_id, message_id);
    sender.wait_for_sender_route_state(message_id, &RouteActiveState::AwaitingRecipientAck);
    receiver.wait_for_inbound_state(message_id, PendingInboundDeliveryState::AwaitingProcessed);

    deliver
        .processed
        .complete()
        .expect("processed completion should succeed exactly once");

    sender.wait_for_sender_work_clear(message_id);
    receiver.wait_for_inbound_clear(message_id);
}

#[test]
fn recipient_ack_timeout_redelivers_unprocessed_envelope() {
    let alice = member_identity(&["alice"]);
    let bob = member_identity(&["bob"]);
    let sender =
        FullStackHarness::with_recipient_ack_timeout(alice.clone(), TEST_RECIPIENT_ACK_TIMEOUT);
    let receiver = FullStackHarness::new(bob.clone());

    sender.publish_direct_route(bob.clone(), receiver.local_addr);

    let message_id = MessageId(Uuid::from_u128(41));
    sender.submit(reliable_submit(
        alice.clone(),
        bob.clone(),
        message_id,
        b"retry bootstrap payload",
    ));

    let deliver = receiver.wait_for_delivery();
    assert_eq!(deliver.envelope.header.message_id, message_id);
    sender.wait_for_sender_route_state(message_id, &RouteActiveState::AwaitingRecipientAck);
    drop(deliver);
    receiver.wait_for_inbound_clear(message_id);

    let redelivered = receiver.wait_for_delivery();
    sender.wait_for_ack_timeout_reported(message_id, true);
    assert_eq!(redelivered.envelope.header.message_id, message_id);
    assert_eq!(
        redelivered.envelope.payload.bytes,
        Bytes::from_static(b"retry bootstrap payload")
    );
    receiver.publish_direct_route(alice, sender.local_addr);
    redelivered
        .processed
        .complete()
        .expect("redelivered processing should complete exactly once");
    sender.wait_for_sender_work_clear(message_id);
    sender.wait_for_ack_timeout_reported(message_id, false);
}

#[test]
fn recipient_ack_timeout_default_targets_interactive_recovery() {
    assert_eq!(DEFAULT_RECIPIENT_ACK_TIMEOUT, Duration::from_secs(10));
}

#[test]
fn outbound_attempt_limit_defaults_to_ten() {
    assert_eq!(DEFAULT_MAX_CONCURRENT_OUTBOUND_ATTEMPTS, 10);
}

#[test]
fn zero_outbound_attempt_limit_is_rejected_by_config_validation() {
    let system = build_delivery_test_system_with(|config| {
        config.set_config_value(&config_keys::MAX_CONCURRENT_OUTBOUND_ATTEMPTS, 0_usize);
    });
    let sender = FullStackHarness::with_system(member_identity(&["alice"]), system);

    sender.reliable.on_definition(|component| {
        assert_eq!(
            component.config.max_concurrent_outbound_attempts,
            DEFAULT_MAX_CONCURRENT_OUTBOUND_ATTEMPTS
        );
    });
}

#[test]
fn configured_outbound_attempt_limit_defers_the_third_full_envelope_load() {
    let system = build_delivery_test_system_with(|config| {
        config.set_config_value(&config_keys::MAX_CONCURRENT_OUTBOUND_ATTEMPTS, 2_usize);
    });
    let alice = member_identity(&["alice"]);
    let bob = member_identity(&["bob"]);
    let (transport_submits, pending_submits) = mpsc::channel();
    let controlled_transport =
        system.create(move || ControlledRouteTransportComponent::new(transport_submits));
    let (security, sqlite_store) = test_delivery_security_and_store(&alice);
    let observed_store = Arc::new(ControlledStore::new(sqlite_store.clone()));
    let reliable_store: Arc<dyn ReliableDeliveryStore> = observed_store.clone();
    let sender = FullStackHarness::with_system_security_and_store(
        alice.clone(),
        system,
        security,
        sqlite_store,
        reliable_store,
        Some(&controlled_transport),
    );
    let message_ids = [
        MessageId(Uuid::from_u128(420)),
        MessageId(Uuid::from_u128(421)),
        MessageId(Uuid::from_u128(422)),
    ];
    for (message_id, payload) in message_ids.into_iter().zip([
        b"first".as_slice(),
        b"second".as_slice(),
        b"third".as_slice(),
    ]) {
        sender.submit(reliable_submit(
            alice.clone(),
            bob.clone(),
            message_id,
            payload,
        ));
        sender.wait_for_stored_envelope(message_id);
    }

    sender.discovery_source.actor_ref().inject_indication(
        TransportDiscoveryRouteUpdate::PeerRoutes {
            peer: bob,
            routes: vec![test_send_route(sender.local_addr, localhost(9))],
        },
    );
    let first = pending_submits
        .recv_timeout(WAIT_TIMEOUT)
        .expect("first configured transport slot should start");
    let second = pending_submits
        .recv_timeout(WAIT_TIMEOUT)
        .expect("second configured transport slot should start");
    assert_eq!(observed_store.full_loads(), message_ids[..2]);
    assert!(
        matches!(
            pending_submits.recv_timeout(REJECTED_ACK_OBSERVATION_WINDOW),
            Err(mpsc::RecvTimeoutError::Timeout)
        ),
        "third transport submission must wait while both configured slots are active"
    );
    let first_coverage_key = first.send.route.coverage_key;
    first
        .completion
        .fulfil(RouteTransportSubmitResult::Sent {
            coverage_key: first_coverage_key,
        })
        .expect("first controlled transport attempt should complete exactly once");
    let third = pending_submits
        .recv_timeout(WAIT_TIMEOUT)
        .expect("third transport submission should start after one slot is released");
    assert_eq!(observed_store.full_loads(), message_ids);

    for pending in [second, third] {
        let coverage_key = pending.send.route.coverage_key;
        pending
            .completion
            .fulfil(RouteTransportSubmitResult::Sent { coverage_key })
            .expect("remaining controlled transport attempt should complete exactly once");
    }
}

#[test]
fn route_active_state_reports_only_active_transport_send_ids() {
    let direct_send_id = RouteSendId::new_random();
    let relay_send_id = RouteSendId::new_random();

    assert_eq!(
        RouteActiveState::AttemptingDirect {
            send_id: direct_send_id,
        }
        .active_send_id(),
        Some(direct_send_id)
    );
    assert_eq!(
        RouteActiveState::AwaitingRelayStore {
            send_id: relay_send_id,
        }
        .active_send_id(),
        Some(relay_send_id)
    );
    for inactive_state in [
        RouteActiveState::Queued,
        RouteActiveState::AwaitingRecipientAck,
        RouteActiveState::WaitingForRoute {
            reason: PendingRouteReason::PeerCurrentlyUnreachable,
        },
        RouteActiveState::RetryScheduled {
            reason: PendingRouteReason::BackoffInEffect,
        },
    ] {
        assert!(inactive_state.active_send_id().is_none());
    }
}

#[test]
fn sender_route_collections_are_exclusive_and_retry_timeout_controls_readiness() {
    let alice = member_identity(&["alice"]);
    let bob = member_identity(&["bob"]);
    let sender = FullStackHarness::new(alice);
    let message_id = MessageId(Uuid::from_u128(423));
    let metadata = StoredReliableDeliveryWorkMetadata {
        message_id,
        recipient: bob.clone(),
        first_submitted_at: SystemTime::now(),
    };
    let ready_key = ReadyKey::from_metadata(&metadata);
    let route = test_send_route(sender.local_addr, localhost(9));

    sender.reliable.on_definition(|component| {
        component.outbound.work_items.insert(
            message_id,
            OutboundState::new_work_item(metadata, PendingRouteReason::ReachabilityUnknown),
        );
        component.place_outbound_by_route(message_id);
        assert!(component.outbound.ready.is_empty());
        assert!(
            component
                .outbound
                .waiting
                .get(&bob)
                .is_some_and(|waiting| waiting.contains(&ready_key))
        );

        component
            .shared
            .direct_peer_routes
            .insert(bob.clone(), route);
        let logger = component.log().clone();
        let purged = component
            .outbound
            .ready_waiting_items_for_route(&bob, &logger);
        assert!(purged.is_empty());
        assert!(component.outbound.waiting.get(&bob).is_none());
        assert!(component.outbound.ready.contains(&ready_key));

        component
            .outbound
            .mark_retry_scheduled(message_id, PendingRouteReason::LocalResourcePressure);
        assert!(component.outbound.ready.is_empty());
        assert!(component.outbound.waiting.get(&bob).is_none());

        component.handle_sender_retry_timeout(message_id);
        assert!(component.outbound.ready.contains(&ready_key));
        assert!(component.outbound.waiting.get(&bob).is_none());

        component.shared.direct_peer_routes.remove(&bob);
        let logger = component.log().clone();
        let purged = component.outbound.withdraw_ready(&bob, &logger);
        assert!(purged.is_empty());
        assert!(component.outbound.ready.is_empty());
        assert!(
            component
                .outbound
                .waiting
                .get(&bob)
                .is_some_and(|waiting| waiting.contains(&ready_key))
        );
    });
}

#[test]
fn mismatched_ready_key_purges_only_its_message_and_cancels_sender_retry() {
    let alice = member_identity(&["alice"]);
    let bob = member_identity(&["bob"]);
    let sender = FullStackHarness::new(alice);
    let message_id = MessageId(Uuid::from_u128(432));
    let metadata = StoredReliableDeliveryWorkMetadata {
        message_id,
        recipient: bob,
        first_submitted_at: SystemTime::UNIX_EPOCH,
    };
    let mismatched_key = ReadyKey {
        first_submitted_at: SystemTime::UNIX_EPOCH + Duration::from_secs(1),
        message_id,
    };

    sender.reliable.on_definition(|component| {
        let mut work_item =
            OutboundState::new_work_item(metadata, PendingRouteReason::ReachabilityUnknown);
        work_item.recipient_route.state = RouteActiveState::Queued;
        component.outbound.work_items.insert(message_id, work_item);
        component.outbound.ready.insert(mismatched_key);
        let now = component.now();
        component
            .shared
            .retry_queue
            .schedule(RetryKey::Sender(message_id), now);

        let selected = component.select_ready_outbound_attempts();

        assert!(selected.is_empty());
        assert!(!component.outbound.contains_message(message_id));
        assert!(
            !component
                .shared
                .retry_queue
                .contains(RetryKey::Sender(message_id))
        );
    });
}

#[test]
fn route_withdrawal_partitions_ready_work_by_recipient() {
    let alice = member_identity(&["alice"]);
    let bob = member_identity(&["bob"]);
    let charlie = member_identity(&["charlie"]);
    let sender = FullStackHarness::new(alice);
    let bob_message_id = MessageId(Uuid::from_u128(433));
    let charlie_message_id = MessageId(Uuid::from_u128(434));
    let bob_metadata = StoredReliableDeliveryWorkMetadata {
        message_id: bob_message_id,
        recipient: bob.clone(),
        first_submitted_at: SystemTime::UNIX_EPOCH,
    };
    let charlie_metadata = StoredReliableDeliveryWorkMetadata {
        message_id: charlie_message_id,
        recipient: charlie,
        first_submitted_at: SystemTime::UNIX_EPOCH + Duration::from_secs(1),
    };
    let bob_key = ReadyKey::from_metadata(&bob_metadata);
    let charlie_key = ReadyKey::from_metadata(&charlie_metadata);

    sender.reliable.on_definition(|component| {
        for metadata in [bob_metadata, charlie_metadata] {
            let message_id = metadata.message_id;
            let mut work_item =
                OutboundState::new_work_item(metadata, PendingRouteReason::ReachabilityUnknown);
            work_item.recipient_route.state = RouteActiveState::Queued;
            component.outbound.work_items.insert(message_id, work_item);
        }
        component.outbound.ready.insert(bob_key);
        component.outbound.ready.insert(charlie_key);

        let logger = component.log().clone();
        let purged = component.outbound.withdraw_ready(&bob, &logger);

        assert!(purged.is_empty());
        assert_eq!(component.outbound.ready, BTreeSet::from([charlie_key]));
        assert!(
            component
                .outbound
                .waiting
                .get(&bob)
                .is_some_and(|waiting| waiting == &BTreeSet::from([bob_key]))
        );
    });
}

#[test]
fn classified_store_failures_select_only_explicit_reliable_delivery_actions() {
    let retry = retryable_store_classification();
    let wait_for_resume = store_classification(
        StoreErrorScope::Connection,
        StoreErrorClass::Unavailable,
        StoreErrorResolution::WaitForResume,
    );
    let invalid_record = invalid_record_classification();
    let incomplete_invalid_record = StoreErrorClassification::UNKNOWN
        .with_scope(StoreErrorScope::Record)
        .with_class(StoreErrorClass::InvalidData);
    let recreate_connection = store_classification(
        StoreErrorScope::Connection,
        StoreErrorClass::Unavailable,
        StoreErrorResolution::Recreate,
    );

    for classification in [retry, wait_for_resume] {
        assert!(store_failure_can_retry_later(classification));
        assert_eq!(
            selected_load_failure_policy(classification),
            SelectedLoadFailurePolicy::RetryLater
        );
    }
    for classification in [
        StoreErrorClassification::UNKNOWN,
        incomplete_invalid_record,
        recreate_connection,
        fatal_store_classification(),
    ] {
        assert!(!store_failure_can_retry_later(classification));
        assert_eq!(
            selected_load_failure_policy(classification),
            SelectedLoadFailurePolicy::FailComponent
        );
    }
    assert_eq!(
        selected_load_failure_policy(invalid_record),
        SelectedLoadFailurePolicy::RemoveInvalidRecord
    );
}

#[test]
fn unpersisted_security_failures_keep_only_retryable_submissions() {
    let alice = member_identity(&["alice"]);
    let bob = member_identity(&["bob"]);
    let sender = FullStackHarness::new(alice.clone());
    let retryable_message_id = MessageId(Uuid::from_u128(424));
    let permanent_message_id = MessageId(Uuid::from_u128(425));
    let retryable_non_store_message_id = MessageId(Uuid::from_u128(426));

    sender.reliable.on_definition(|component| {
        for (message_id, recipient) in [
            (retryable_message_id, bob.clone()),
            (permanent_message_id, alice.clone()),
            (retryable_non_store_message_id, bob.clone()),
        ] {
            component.outbound.unpersisted_submissions.insert(
                message_id,
                UnpersistedSubmission::Plaintext {
                    submit: reliable_submit(alice.clone(), recipient, message_id, b"pending"),
                    first_submitted_at: SystemTime::now(),
                },
            );
        }

        let store_error =
            classified_store_error(retryable_store_classification(), "injected store failure");
        let retryable_error = Err::<(), _>(store_error)
            .context(crate::delivery::security::StoreAccessSnafu)
            .expect_err("injected delivery-security failure should remain an error");
        let _handled = component
            .handle_unpersisted_security_error(retryable_message_id, &bob, retryable_error)
            .expect("retryable security failure should remain benign");
        assert!(
            component
                .outbound
                .unpersisted_submissions
                .contains_key(&retryable_message_id)
        );
        assert!(
            component
                .shared
                .retry_queue
                .contains(RetryKey::Unpersisted(retryable_message_id))
        );

        let permanent_error = DeliverySecurityError::ReliableSelfMessage {
            member_id: alice.clone(),
        };
        let _handled = component
            .handle_unpersisted_security_error(permanent_message_id, &alice, permanent_error)
            .expect("non-store security rejection should remain benign");
        assert!(
            !component
                .outbound
                .unpersisted_submissions
                .contains_key(&permanent_message_id)
        );
        assert!(
            !component
                .shared
                .retry_queue
                .contains(RetryKey::Unpersisted(permanent_message_id))
        );

        let retryable_non_store_error = DeliverySecurityError::GenerateGroupKey {
            source: std::io::Error::other("injected random source failure").into(),
        };
        let _handled = component
            .handle_unpersisted_security_error(
                retryable_non_store_message_id,
                &bob,
                retryable_non_store_error,
            )
            .expect("retryable non-store security failure should remain benign");
        assert!(
            component
                .outbound
                .unpersisted_submissions
                .contains_key(&retryable_non_store_message_id)
        );
        assert!(
            component
                .shared
                .retry_queue
                .contains(RetryKey::Unpersisted(retryable_non_store_message_id))
        );
    });
}

#[test]
fn permanent_unpersisted_store_security_failure_is_unrecoverable() {
    let alice = member_identity(&["alice"]);
    let bob = member_identity(&["bob"]);
    let sender = FullStackHarness::new(alice.clone());
    let message_id = MessageId(Uuid::from_u128(427));

    sender.reliable.on_definition(|component| {
        component.outbound.unpersisted_submissions.insert(
            message_id,
            UnpersistedSubmission::Plaintext {
                submit: reliable_submit(alice, bob.clone(), message_id, b"pending"),
                first_submitted_at: SystemTime::now(),
            },
        );
        let store_error = classified_store_error(
            fatal_store_classification(),
            "injected permanent security-store failure",
        );
        let error = Err::<(), _>(store_error)
            .context(crate::delivery::security::StoreAccessSnafu)
            .expect_err("injected delivery-security failure should remain an error");

        let handled = component.handle_unpersisted_security_error(message_id, &bob, error);

        assert!(matches!(handled, Err(HandlerError::Unrecoverable(_))));
        assert!(
            component
                .outbound
                .unpersisted_submissions
                .contains_key(&message_id)
        );
        assert!(
            !component
                .shared
                .retry_queue
                .contains(RetryKey::Unpersisted(message_id))
        );
    });
}

#[test]
fn persistence_retry_reuses_the_already_encoded_envelope() {
    let alice = member_identity(&["alice"]);
    let bob = member_identity(&["bob"]);
    let (security, sqlite_store) = test_delivery_security_and_store(&alice);
    let fail_first_store = Arc::new(ControlledStore::failing_first_persistence(
        sqlite_store.clone(),
        retryable_store_classification(),
    ));
    let reliable_store: Arc<dyn ReliableDeliveryStore> = fail_first_store.clone();
    let sender = FullStackHarness::with_system_security_and_store(
        alice.clone(),
        build_delivery_test_system(),
        security,
        sqlite_store.clone(),
        reliable_store,
        None,
    );
    let message_id = MessageId(Uuid::from_u128(428));
    sender.submit(reliable_submit(
        alice,
        bob,
        message_id,
        b"persist the same encoded envelope",
    ));

    eventually_component_state(
        WAIT_TIMEOUT,
        &sender.reliable,
        |component| {
            matches!(
                component.outbound.unpersisted_submissions.get(&message_id),
                Some(UnpersistedSubmission::Encoded { .. })
            ) && component
                .shared
                .retry_queue
                .contains(RetryKey::Unpersisted(message_id))
        },
        "timed out waiting for encoded admission state after persistence failure",
    );
    sender.reliable.on_definition(|component| {
        let _handled = block_on(component.persist_one_unpersisted_submission(message_id))
            .expect("manual persistence retry should remain benign");
    });
    eventually_component_state(
        WAIT_TIMEOUT,
        &sender.reliable,
        |component| {
            component.sender_work_item(message_id).is_some()
                && !component
                    .outbound
                    .unpersisted_submissions
                    .contains_key(&message_id)
        },
        "timed out waiting for persistence retry to create sender work",
    );

    let attempts = fail_first_store.store_attempts();
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0], attempts[1]);
    assert_eq!(
        block_on(sqlite_store.load_reliable_delivery_work(message_id))
            .expect("persisted retry work should load"),
        Some(attempts[0].clone())
    );
}

#[test]
fn permanent_persistence_failure_terminates_reliable_delivery() {
    let alice = member_identity(&["alice"]);
    let bob = member_identity(&["bob"]);
    let (security, sqlite_store) = test_delivery_security_and_store(&alice);
    let fault_store = Arc::new(ControlledStore::failing_first_persistence(
        sqlite_store.clone(),
        fatal_store_classification(),
    ));
    let reliable_store: Arc<dyn ReliableDeliveryStore> = fault_store.clone();
    let sender = FullStackHarness::with_system_security_and_store(
        alice.clone(),
        build_delivery_test_system(),
        security,
        sqlite_store.clone(),
        reliable_store,
        None,
    );
    let message_id = MessageId(Uuid::from_u128(428_001));

    sender.submit(reliable_submit(
        alice,
        bob,
        message_id,
        b"permanent persistence failure",
    ));

    eventually(
        WAIT_TIMEOUT,
        || sender.reliable.is_destroyed(),
        "permanent persistence failure did not terminate reliable delivery",
    );
    assert_eq!(fault_store.store_attempts().len(), 1);
    assert!(
        block_on(sqlite_store.load_reliable_delivery_work(message_id))
            .expect("underlying store should remain readable")
            .is_none()
    );
}

#[test]
fn missing_stored_envelope_purges_only_the_affected_outbound_state() {
    let alice = member_identity(&["alice"]);
    let bob = member_identity(&["bob"]);
    let (security, sqlite_store) = test_delivery_security_and_store(&alice);
    let fault_store = Arc::new(ControlledStore::with_full_load_fault(
        sqlite_store.clone(),
        ReliableDeliveryLoadFault::Missing,
    ));
    let reliable_store: Arc<dyn ReliableDeliveryStore> = fault_store.clone();
    let sender = FullStackHarness::with_system_security_and_store(
        alice.clone(),
        build_delivery_test_system(),
        security,
        sqlite_store,
        reliable_store,
        None,
    );
    let message_id = MessageId(Uuid::from_u128(429));
    sender.submit(reliable_submit(
        alice,
        bob.clone(),
        message_id,
        b"missing stored envelope",
    ));
    sender.wait_for_stored_envelope(message_id);
    sender.publish_direct_route(bob, localhost(9));

    eventually_component_state(
        WAIT_TIMEOUT,
        &sender.reliable,
        |component| {
            !component.outbound.contains_message(message_id)
                && !component
                    .shared
                    .retry_queue
                    .contains(RetryKey::Sender(message_id))
        },
        "timed out waiting for missing stored work to be purged from memory",
    );
    assert_eq!(fault_store.full_load_calls(), 1);
}

#[test]
fn full_load_store_error_releases_the_slot_and_schedules_sender_backoff() {
    let alice = member_identity(&["alice"]);
    let bob = member_identity(&["bob"]);
    let (security, sqlite_store) = test_delivery_security_and_store(&alice);
    let fault_store = Arc::new(ControlledStore::with_full_load_fault(
        sqlite_store.clone(),
        ReliableDeliveryLoadFault::Error(retryable_store_classification()),
    ));
    let reliable_store: Arc<dyn ReliableDeliveryStore> = fault_store.clone();
    let sender = FullStackHarness::with_system_security_and_store(
        alice.clone(),
        build_delivery_test_system(),
        security,
        sqlite_store,
        reliable_store,
        None,
    );
    let message_id = MessageId(Uuid::from_u128(430));
    sender.submit(reliable_submit(
        alice,
        bob.clone(),
        message_id,
        b"retry failed full load",
    ));
    sender.wait_for_stored_envelope(message_id);
    sender.publish_direct_route(bob, localhost(9));

    eventually_component_state(
        WAIT_TIMEOUT,
        &sender.reliable,
        |component| {
            !component.outbound.attempts_in_flight.contains(&message_id)
                && component
                    .sender_work_item(message_id)
                    .is_some_and(|work_item| {
                        work_item.recipient_route.state
                            == RouteActiveState::RetryScheduled {
                                reason: PendingRouteReason::LocalResourcePressure,
                            }
                    })
                && component
                    .shared
                    .retry_queue
                    .contains(RetryKey::Sender(message_id))
        },
        "timed out waiting for a full-load store error to enter sender backoff",
    );
    assert_eq!(fault_store.full_load_calls(), 1);
}

#[test]
fn invalid_selected_record_is_removed_without_faulting_reliable_delivery() {
    let alice = member_identity(&["alice"]);
    let bob = member_identity(&["bob"]);
    let (security, sqlite_store) = test_delivery_security_and_store(&alice);
    let fault_store = Arc::new(ControlledStore::with_full_load_fault(
        sqlite_store.clone(),
        ReliableDeliveryLoadFault::Error(invalid_record_classification()),
    ));
    let reliable_store: Arc<dyn ReliableDeliveryStore> = fault_store.clone();
    let sender = FullStackHarness::with_system_security_and_store(
        alice.clone(),
        build_delivery_test_system(),
        security,
        sqlite_store.clone(),
        reliable_store,
        None,
    );
    let message_id = MessageId(Uuid::from_u128(430_001));
    sender.submit(reliable_submit(
        alice,
        bob.clone(),
        message_id,
        b"remove invalid stored record",
    ));
    sender.wait_for_stored_envelope(message_id);

    sender.publish_direct_route(bob, localhost(9));

    eventually_component_state(
        WAIT_TIMEOUT,
        &sender.reliable,
        |component| !component.outbound.contains_message(message_id),
        "timed out waiting for invalid stored work to be isolated and removed",
    );
    assert!(sender.reliable.is_active());
    assert_eq!(fault_store.full_load_calls(), 1);
    assert_eq!(fault_store.removal_calls(), 1);
    assert!(
        block_on(sqlite_store.load_reliable_delivery_work(message_id))
            .expect("underlying store should remain readable")
            .is_none()
    );
}

#[test]
fn permanent_selected_load_failure_terminates_reliable_delivery() {
    let alice = member_identity(&["alice"]);
    let bob = member_identity(&["bob"]);
    let (security, sqlite_store) = test_delivery_security_and_store(&alice);
    let fault_store = Arc::new(ControlledStore::with_full_load_fault(
        sqlite_store.clone(),
        ReliableDeliveryLoadFault::Error(fatal_store_classification()),
    ));
    let reliable_store: Arc<dyn ReliableDeliveryStore> = fault_store.clone();
    let sender = FullStackHarness::with_system_security_and_store(
        alice.clone(),
        build_delivery_test_system(),
        security,
        sqlite_store,
        reliable_store,
        None,
    );
    let message_id = MessageId(Uuid::from_u128(430_002));
    sender.submit(reliable_submit(
        alice,
        bob.clone(),
        message_id,
        b"permanent selected load failure",
    ));
    sender.wait_for_stored_envelope(message_id);

    sender.publish_direct_route(bob, localhost(9));

    eventually(
        WAIT_TIMEOUT,
        || sender.reliable.is_destroyed(),
        "permanent selected-load failure did not terminate reliable delivery",
    );
    assert_eq!(fault_store.full_load_calls(), 1);
}

#[test]
fn stored_row_removal_error_keeps_cleanup_pending_without_resending() {
    let alice = member_identity(&["alice"]);
    let bob = member_identity(&["bob"]);
    let (security, sqlite_store) = test_delivery_security_and_store(&alice);
    let fault_store = Arc::new(ControlledStore::failing_first_removal(
        sqlite_store.clone(),
        retryable_store_classification(),
    ));
    let reliable_store: Arc<dyn ReliableDeliveryStore> = fault_store.clone();
    let sender = FullStackHarness::with_system_security_and_store(
        alice.clone(),
        build_delivery_test_system(),
        security,
        sqlite_store.clone(),
        reliable_store,
        None,
    );
    let bob_security = test_delivery_security(&bob);
    let message_id = MessageId(Uuid::from_u128(431));
    sender.submit(reliable_submit(
        alice.clone(),
        bob.clone(),
        message_id,
        b"retry stored-row removal",
    ));
    sender.wait_for_stored_envelope(message_id);
    sender.inject_recipient_ack(&recipient_ack(&bob_security, &alice, &bob, message_id));

    eventually_component_state(
        WAIT_TIMEOUT,
        &sender.reliable,
        |component| {
            component
                .sender_work_item(message_id)
                .is_some_and(|work_item| {
                    work_item.pending_removal
                        == Some(PendingRemoval {
                            reason: PendingRemovalReason::RecipientAcknowledged,
                            stored_row_pending: true,
                            outstanding_send_id: None,
                        })
                })
                && component
                    .shared
                    .retry_queue
                    .contains(RetryKey::SenderRemoval(message_id))
        },
        "timed out waiting for failed stored-row removal to remain cleanup-pending",
    );
    sender.reliable.on_definition(|component| {
        let _handled = block_on(component.attempt_stored_sender_removal(message_id))
            .expect("manual stored-row removal retry should remain benign");
        assert!(component.sender_work_item(message_id).is_none());
        assert!(
            !component
                .shared
                .retry_queue
                .contains(RetryKey::SenderRemoval(message_id))
        );
    });
    assert_eq!(fault_store.removal_calls(), 2);
    assert_eq!(
        block_on(sqlite_store.load_reliable_delivery_work(message_id))
            .expect("stored row should remain readable after cleanup"),
        None
    );
}

#[test]
fn permanent_stored_row_removal_failure_terminates_reliable_delivery() {
    let alice = member_identity(&["alice"]);
    let bob = member_identity(&["bob"]);
    let (security, sqlite_store) = test_delivery_security_and_store(&alice);
    let fault_store = Arc::new(ControlledStore::failing_first_removal(
        sqlite_store.clone(),
        fatal_store_classification(),
    ));
    let reliable_store: Arc<dyn ReliableDeliveryStore> = fault_store.clone();
    let sender = FullStackHarness::with_system_security_and_store(
        alice.clone(),
        build_delivery_test_system(),
        security,
        sqlite_store.clone(),
        reliable_store,
        None,
    );
    let bob_security = test_delivery_security(&bob);
    let message_id = MessageId(Uuid::from_u128(431_001));
    sender.submit(reliable_submit(
        alice.clone(),
        bob.clone(),
        message_id,
        b"permanent stored-row removal failure",
    ));
    sender.wait_for_stored_envelope(message_id);

    sender.inject_recipient_ack(&recipient_ack(&bob_security, &alice, &bob, message_id));

    eventually(
        WAIT_TIMEOUT,
        || sender.reliable.is_destroyed(),
        "permanent stored-row removal failure did not terminate reliable delivery",
    );
    sender.reliable.on_definition(|component| {
        assert!(
            component
                .sender_work_item(message_id)
                .is_some_and(|work_item| work_item.pending_removal.is_some())
        );
        assert!(
            !component
                .shared
                .retry_queue
                .contains(RetryKey::SenderRemoval(message_id))
        );
    });
    assert_eq!(fault_store.removal_calls(), 1);
    assert!(
        block_on(sqlite_store.load_reliable_delivery_work(message_id))
            .expect("underlying store should remain readable")
            .is_some()
    );
}

#[test]
fn pending_transport_result_keeps_its_outbound_attempt_slot() {
    let system = build_delivery_test_system_with(|config| {
        config.set_config_value(&config_keys::MAX_CONCURRENT_OUTBOUND_ATTEMPTS, 1_usize);
    });
    let sender = FullStackHarness::with_system(member_identity(&["alice"]), system);
    let message_id = MessageId(Uuid::from_u128(410));

    sender.reliable.on_definition(|component| {
        assert_eq!(component.config.max_concurrent_outbound_attempts, 1);
        let send_id = RouteSendId::new_random();
        component.outbound.attempts_in_flight.insert(message_id);
        let (_promise, pending_result) = promise();
        {
            let finishing =
                component.finish_outbound_envelope_submit(message_id, send_id, pending_result);
            pin_mut!(finishing);
            let mut context = Context::from_waker(noop_waker_ref());
            assert!(finishing.as_mut().poll(&mut context).is_pending());
        }
        assert!(component.outbound.attempts_in_flight.contains(&message_id));
    });
}

#[test]
fn cleanup_retains_work_until_outstanding_transport_result_converges() {
    let alice = member_identity(&["alice"]);
    let bob = member_identity(&["bob"]);
    let sender = FullStackHarness::new(alice);
    let message_id = MessageId(Uuid::from_u128(432));
    let send_id = RouteSendId::new_random();
    let coverage_key = test_send_route(sender.local_addr, localhost(9)).coverage_key;

    sender.reliable.on_definition(|component| {
        let metadata = StoredReliableDeliveryWorkMetadata {
            message_id,
            recipient: bob,
            first_submitted_at: SystemTime::now(),
        };
        let mut work_item =
            OutboundState::new_work_item(metadata, PendingRouteReason::ReachabilityUnknown);
        work_item.recipient_route.state = RouteActiveState::AttemptingDirect { send_id };
        component.outbound.work_items.insert(message_id, work_item);
        component.outbound.attempts_in_flight.insert(message_id);

        assert!(
            component
                .outbound
                .begin_removal(message_id, PendingRemovalReason::RecipientAcknowledged)
        );
        component.outbound.finish_stored_removal(message_id);
        assert!(component.outbound.contains_message(message_id));

        let logger = component.log().clone();
        let completion = component.outbound.finish_outbound_attempt(
            message_id,
            send_id,
            OutboundAttemptResult::Transport(RouteTransportSubmitResult::Sent { coverage_key }),
            &logger,
        );
        assert_eq!(completion, AttemptCompletion::CleanupOnly);
        assert!(!component.outbound.contains_message(message_id));
        assert!(!component.outbound.attempts_in_flight.contains(&message_id));
    });
}

#[test]
fn transient_transport_failure_retains_the_stored_envelope_for_retry() {
    let alice = member_identity(&["alice"]);
    let bob = member_identity(&["bob"]);
    let sender = FullStackHarness::new(alice.clone());
    let message_id = MessageId(Uuid::from_u128(414));
    sender.submit(reliable_submit(
        alice,
        bob,
        message_id,
        b"retain after transient failure",
    ));
    let original_envelope = sender.wait_for_stored_envelope(message_id);
    let coverage_key = TransportRouteKey::Udp(UdpRouteKey {
        remote_addr: localhost(9),
        scope: DatagramRouteScope::Unicast,
        local_bind: Some(sender.local_addr),
    });

    sender.reliable.on_definition(|component| {
        let send_id = RouteSendId::new_random();
        component
            .outbound
            .work_items
            .get_mut(&message_id)
            .expect("stored sender work should exist")
            .recipient_route
            .state = RouteActiveState::AttemptingDirect { send_id };
        component.outbound.attempts_in_flight.insert(message_id);
        let (promise, result) = promise();
        promise
            .fulfil(RouteTransportSubmitResult::SendFailed {
                coverage_key,
                reason: RouteTransportNackReason::Backpressure,
            })
            .expect("test transport result should fulfil");
        let _handled =
            block_on(component.finish_outbound_envelope_submit(message_id, send_id, result))
                .expect("transport failure handling should remain benign");
        assert!(!component.outbound.attempts_in_flight.contains(&message_id));
        assert!(component.sender_work_item(message_id).is_some_and(|work| {
            work.recipient_route.state
                == RouteActiveState::RetryScheduled {
                    reason: PendingRouteReason::LocalResourcePressure,
                }
        }));
    });
    assert_eq!(
        sender.wait_for_stored_envelope(message_id),
        original_envelope
    );
}

#[test]
fn invalid_payload_failure_removes_the_permanently_unusable_stored_envelope() {
    let alice = member_identity(&["alice"]);
    let bob = member_identity(&["bob"]);
    let sender = FullStackHarness::new(alice.clone());
    let message_id = MessageId(Uuid::from_u128(427));
    sender.submit(reliable_submit(
        alice,
        bob,
        message_id,
        b"permanent invalid payload",
    ));
    sender.wait_for_stored_envelope(message_id);
    let coverage_key = test_send_route(sender.local_addr, localhost(9)).coverage_key;

    sender.reliable.on_definition(|component| {
        let send_id = RouteSendId::new_random();
        component
            .outbound
            .work_items
            .get_mut(&message_id)
            .expect("stored sender work should exist")
            .recipient_route
            .state = RouteActiveState::AttemptingDirect { send_id };
        component.outbound.attempts_in_flight.insert(message_id);
        let (promise, result) = promise();
        promise
            .fulfil(RouteTransportSubmitResult::SendFailed {
                coverage_key,
                reason: RouteTransportNackReason::InvalidPayload,
            })
            .expect("test permanent transport result should fulfil");
        component.spawn_local(async move |mut async_self| {
            async_self
                .finish_outbound_envelope_submit(message_id, send_id, result)
                .await
        });
    });

    sender.wait_for_sender_work_clear(message_id);
}

#[test]
fn route_selection_skips_full_load_for_older_unreachable_work() {
    let alice = member_identity(&["alice"]);
    let bob = member_identity(&["bob"]);
    let charlie = member_identity(&["charlie"]);
    let (security, sqlite_store) = test_delivery_security_and_store(&alice);
    let observed_store = Arc::new(ControlledStore::new(sqlite_store.clone()));
    let reliable_store: Arc<dyn ReliableDeliveryStore> = observed_store.clone();
    let sender = FullStackHarness::with_system_security_and_store(
        alice.clone(),
        build_delivery_test_system(),
        security,
        sqlite_store,
        reliable_store,
        None,
    );
    let receiver = FullStackHarness::new(charlie.clone());
    sender.publish_direct_route(charlie.clone(), receiver.local_addr);

    let unreachable_message_id = MessageId(Uuid::from_u128(411));
    sender.submit(reliable_submit(
        alice.clone(),
        bob,
        unreachable_message_id,
        b"older unreachable work",
    ));
    sender.wait_for_sender_route_state(
        unreachable_message_id,
        &RouteActiveState::WaitingForRoute {
            reason: PendingRouteReason::ReachabilityUnknown,
        },
    );

    let reachable_message_id = MessageId(Uuid::from_u128(412));
    sender.submit(reliable_submit(
        alice,
        charlie,
        reachable_message_id,
        b"newer reachable work",
    ));
    assert_eq!(
        receiver.wait_for_delivery().envelope.header.message_id,
        reachable_message_id
    );
    assert_eq!(observed_store.full_loads(), vec![reachable_message_id]);
}

#[test]
fn acknowledgement_cleanup_waits_for_the_blocking_envelope_load() {
    let alice = member_identity(&["alice"]);
    let bob = member_identity(&["bob"]);
    let (security, sqlite_store) = test_delivery_security_and_store(&alice);
    let (blocking_store, load_started, mut load_releases) =
        ControlledStore::gating_full_loads(sqlite_store.clone(), 1);
    let load_release = load_releases
        .pop()
        .expect("one configured load gate must exist");
    let blocking_store = Arc::new(blocking_store);
    let reliable_store: Arc<dyn ReliableDeliveryStore> = blocking_store.clone();
    let sender = FullStackHarness::with_system_security_and_store(
        alice.clone(),
        build_delivery_test_system(),
        security,
        sqlite_store,
        reliable_store,
        None,
    );
    let bob_security = test_delivery_security(&bob);
    let message_id = MessageId(Uuid::from_u128(426));
    sender.submit(reliable_submit(
        alice.clone(),
        bob.clone(),
        message_id,
        b"blocking load boundary",
    ));
    sender.wait_for_stored_envelope(message_id);

    sender.discovery_source.actor_ref().inject_indication(
        TransportDiscoveryRouteUpdate::PeerRoutes {
            peer: bob.clone(),
            routes: vec![test_send_route(sender.local_addr, localhost(9))],
        },
    );
    assert_eq!(
        load_started
            .recv_timeout(WAIT_TIMEOUT)
            .expect("stored-envelope load should reach the blocking wrapper"),
        message_id
    );

    let ack = recipient_ack(&bob_security, &alice, &bob, message_id);
    sender.inject_recipient_ack(&ack);
    assert_never(
        REJECTED_ACK_OBSERVATION_WINDOW,
        || blocking_store.removal_calls() != 0,
        "acknowledgement cleanup must not interleave with a blocking envelope load",
    );

    load_release
        .complete()
        .expect("blocked stored-envelope load should release exactly once");
    sender.wait_for_sender_work_clear(message_id);
    assert_eq!(blocking_store.removal_calls(), 1);
}

#[test]
fn restart_restores_metadata_and_reuses_the_stored_envelope_after_route_discovery() {
    let alice = member_identity(&["alice"]);
    let bob = member_identity(&["bob"]);
    let (security, sqlite_store) = test_delivery_security_and_store(&alice);
    let message_id = MessageId(Uuid::from_u128(413));

    let first_sender = FullStackHarness::with_system_security_and_store(
        alice.clone(),
        build_delivery_test_system(),
        security.clone(),
        sqlite_store.clone(),
        sqlite_store.clone(),
        None,
    );
    first_sender.submit(reliable_submit(
        alice.clone(),
        bob.clone(),
        message_id,
        b"restart envelope",
    ));
    let original_envelope = first_sender.wait_for_stored_envelope(message_id);
    first_sender.shutdown_for_restart();

    let observed_store = Arc::new(ControlledStore::new(sqlite_store.clone()));
    let reliable_store: Arc<dyn ReliableDeliveryStore> = observed_store.clone();
    let restarted_sender = FullStackHarness::with_system_security_and_store(
        alice,
        build_delivery_test_system(),
        security,
        sqlite_store,
        reliable_store,
        None,
    );
    restarted_sender.wait_for_sender_route_state(
        message_id,
        &RouteActiveState::WaitingForRoute {
            reason: PendingRouteReason::RecoveredAfterRestart,
        },
    );
    assert!(observed_store.full_loads().is_empty());

    let receiver = FullStackHarness::new(bob.clone());
    restarted_sender.publish_direct_route(bob, receiver.local_addr);
    assert_eq!(
        receiver.wait_for_delivery().envelope.header.message_id,
        message_id
    );
    assert_eq!(observed_store.full_loads(), vec![message_id]);
    assert_eq!(
        restarted_sender.wait_for_stored_envelope(message_id),
        original_envelope
    );
}

#[test]
fn recipient_ack_cancels_timeout_redelivery() {
    let alice = member_identity(&["alice"]);
    let bob = member_identity(&["bob"]);
    let sender =
        FullStackHarness::with_recipient_ack_timeout(alice.clone(), TEST_RECIPIENT_ACK_TIMEOUT);
    let receiver = FullStackHarness::new(bob.clone());

    sender.publish_direct_route(bob.clone(), receiver.local_addr);
    receiver.publish_direct_route(alice.clone(), sender.local_addr);

    let message_id = MessageId(Uuid::from_u128(42));
    sender.submit(reliable_submit(
        alice,
        bob,
        message_id,
        b"ack cancels timeout",
    ));

    let deliver = receiver.wait_for_delivery();
    deliver
        .processed
        .complete()
        .expect("processed completion should succeed exactly once");
    sender.wait_for_sender_route_state(message_id, &RouteActiveState::AwaitingRecipientAck);
    sender.wait_for_sender_work_clear(message_id);

    receiver.expect_no_delivery(TEST_RECIPIENT_ACK_TIMEOUT * 2);
}

#[test]
fn duplicate_inbound_envelope_is_dropped_while_awaiting_processed() {
    let alice = member_identity(&["alice"]);
    let bob = member_identity(&["bob"]);
    let sender =
        FullStackHarness::with_recipient_ack_timeout(alice.clone(), TEST_RECIPIENT_ACK_TIMEOUT);
    let receiver = FullStackHarness::new(bob.clone());

    sender.publish_direct_route(bob.clone(), receiver.local_addr);

    let message_id = MessageId(Uuid::from_u128(43));
    sender.submit(reliable_submit(
        alice,
        bob,
        message_id,
        b"duplicate while processing",
    ));

    let deliver = receiver.wait_for_delivery();
    assert_eq!(deliver.envelope.header.message_id, message_id);
    sender.wait_for_sender_route_state(message_id, &RouteActiveState::AwaitingRecipientAck);
    receiver.wait_for_inbound_state(message_id, PendingInboundDeliveryState::AwaitingProcessed);

    receiver.expect_no_delivery(TEST_RECIPIENT_ACK_TIMEOUT * 2);
    deliver
        .processed
        .complete()
        .expect("processed completion should succeed exactly once");
}

#[test]
fn duplicate_inbound_envelope_retries_pending_recipient_ack_without_redelivery() {
    let alice = member_identity(&["alice"]);
    let bob = member_identity(&["bob"]);
    let sender = FullStackHarness::new(alice.clone());
    let receiver = FullStackHarness::new(bob.clone());

    sender.publish_direct_route(bob.clone(), receiver.local_addr);

    let message_id = MessageId(Uuid::from_u128(44));
    sender.submit(reliable_submit(
        alice.clone(),
        bob.clone(),
        message_id,
        b"duplicate while ack pending",
    ));

    let deliver = receiver.wait_for_delivery();
    let duplicate = receiver.wait_for_ingress_envelope(message_id);
    sender.wait_for_sender_route_state(message_id, &RouteActiveState::AwaitingRecipientAck);
    deliver
        .processed
        .complete()
        .expect("processed completion should succeed exactly once");
    receiver.wait_for_inbound_state(message_id, PendingInboundDeliveryState::AckPending);

    receiver.inject_reliable_frame(duplicate);
    receiver.expect_no_delivery(Duration::from_millis(20));
    receiver.publish_direct_route(alice, sender.local_addr);
    sender.wait_for_sender_work_clear(message_id);
    receiver.wait_for_inbound_clear(message_id);
}

#[test]
fn late_recipient_ack_is_accepted_for_pending_sender_work() {
    let scenario = RecipientAckScenario::new();

    let message_id = MessageId(Uuid::from_u128(45));
    scenario.submit_pending(message_id, b"pending route late ack");

    scenario
        .sender
        .inject_recipient_ack(&scenario.bob_ack(message_id));
    scenario.sender.wait_for_sender_work_clear(message_id);
}

#[test]
fn recipient_ack_with_wrong_message_id_is_rejected() {
    let scenario = RecipientAckScenario::new();

    let message_id = MessageId(Uuid::from_u128(46));
    scenario.submit_pending(message_id, b"wrong message ack");

    let wrong_message_id = MessageId(Uuid::from_u128(47));
    scenario
        .sender
        .inject_recipient_ack(&scenario.bob_ack(wrong_message_id));
    scenario.sender.expect_sender_work_retained(message_id);
}

#[test]
fn recipient_ack_with_wrong_original_sender_is_rejected() {
    let scenario = RecipientAckScenario::new();

    let message_id = MessageId(Uuid::from_u128(48));
    scenario.submit_pending(message_id, b"wrong original sender ack");

    let ack = scenario.bob_ack_for(&scenario.charlie, &scenario.bob, message_id);
    scenario.sender.inject_recipient_ack(&ack);
    scenario.sender.expect_sender_work_retained(message_id);
}

#[test]
fn recipient_ack_with_wrong_recipient_is_rejected() {
    let scenario = RecipientAckScenario::new();

    let message_id = MessageId(Uuid::from_u128(49));
    scenario.submit_pending(message_id, b"wrong recipient ack");

    let ack = scenario.charlie_ack_for(&scenario.alice, &scenario.charlie, message_id);
    scenario.sender.inject_recipient_ack(&ack);
    scenario.sender.expect_sender_work_retained(message_id);
}

#[test]
fn recipient_ack_with_tampered_signature_is_rejected() {
    let scenario = RecipientAckScenario::new();

    let message_id = MessageId(Uuid::from_u128(50));
    scenario.submit_pending(message_id, b"tampered signature ack");

    let mut ack = scenario.bob_ack(message_id);
    let signature_bytes = ack.signature.bytes.as_ref();
    let mut tampered_signature = signature_bytes.to_vec();
    tampered_signature[0] ^= 0x01;
    ack.signature.bytes = Bytes::from(tampered_signature);
    scenario.sender.inject_recipient_ack(&ack);
    scenario.sender.expect_sender_work_retained(message_id);
}

#[test]
fn recipient_ack_with_tampered_public_header_is_rejected() {
    let scenario = RecipientAckScenario::new();

    let signed_message_id = MessageId(Uuid::from_u128(53));
    let tampered_message_id = MessageId(Uuid::from_u128(54));
    scenario.submit_pending(signed_message_id, b"signed header ack");
    scenario.submit_pending(tampered_message_id, b"tampered header ack");

    let mut ack = scenario.bob_ack(signed_message_id);
    ack.header.message_id = tampered_message_id;
    scenario.sender.inject_recipient_ack(&ack);
    scenario
        .sender
        .expect_sender_work_retained(tampered_message_id);
    scenario
        .sender
        .expect_sender_work_retained(signed_message_id);
}

#[test]
fn malformed_recipient_ack_is_rejected() {
    let scenario = RecipientAckScenario::new();

    let message_id = MessageId(Uuid::from_u128(51));
    scenario.submit_pending(message_id, b"malformed ack");

    scenario
        .sender
        .inject_recipient_ack_wire(malformed_recipient_ack_wire(
            &scenario.alice,
            &scenario.bob,
            message_id,
        ));
    scenario.sender.expect_sender_work_retained(message_id);
}

#[test]
fn retry_queue_keeps_overdue_entries_ready_after_timer_reset() {
    let mut queue = RetryQueue::new();
    let first = RetryKey::Sender(MessageId(Uuid::from_u128(1)));
    let second = RetryKey::InboundAck(MessageId(Uuid::from_u128(2)));
    let base = Instant::now();

    queue.schedule(first, base);
    queue.schedule(second, base + Duration::from_secs(30));
    queue.remove_stale_entries();

    let ready = queue.take_ready(base + Duration::from_millis(1));
    assert_eq!(ready, vec![first]);
    assert_eq!(queue.next_due_at(), Some(base + Duration::from_secs(30)));
}

#[test]
fn duplicate_submit_keeps_the_original_work_item() {
    let alice = member_identity(&["alice"]);
    let bob = member_identity(&["bob"]);
    let sender = FullStackHarness::new(alice.clone());
    let message_id = MessageId(Uuid::from_u128(7));

    sender.submit(ReliableDeliverySubmit {
        envelope: ReliableMessageEnvelope::<PlaintextPayload> {
            header: ReliableMessageHeader {
                sender: alice.clone(),
                recipient: bob.clone(),
                message_id,
                scope: ReliableMessageScope::DirectMessage,
            },
            payload: PlaintextPayload {
                bytes: Bytes::from_static(b"first payload"),
            },
        },
    });
    let original_envelope = sender.wait_for_stored_envelope(message_id);
    sender.submit(ReliableDeliverySubmit {
        envelope: ReliableMessageEnvelope::<PlaintextPayload> {
            header: ReliableMessageHeader {
                sender: alice,
                recipient: bob,
                message_id,
                scope: ReliableMessageScope::DirectMessage,
            },
            payload: PlaintextPayload {
                bytes: Bytes::from_static(b"second payload"),
            },
        },
    });

    let retained_envelope = sender.wait_for_stored_envelope(message_id);
    assert_eq!(retained_envelope, original_envelope);
}
