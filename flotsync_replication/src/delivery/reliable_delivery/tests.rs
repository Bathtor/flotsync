//! Unit tests for reliable delivery.

use super::*;
use crate::{
    SqliteReplicationStore,
    delivery::ingress::{DeliveryIngressComponent, DeliveryInterestConfig},
    test_support::{load_test_delivery_security, provision_test_security},
};
use flotsync_core::{
    GroupId,
    membership::{GroupMemberships, SharedGroupMemberships},
};
use flotsync_io::{
    prelude::UdpLocalBind,
    test_support::{
        WAIT_TIMEOUT,
        assert_never,
        eventually_component_state,
        localhost,
        start_component,
    },
};
use flotsync_routes::{
    DatagramRouteScope,
    RoutePreferenceRank,
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
use std::{
    cell::Cell,
    collections::HashSet,
    net::SocketAddr,
    sync::mpsc,
    time::{Duration, Instant},
};

const TEST_RECIPIENT_ACK_TIMEOUT: Duration = Duration::from_millis(50);
/// Observation window used by negative ack tests to catch accidental async
/// state transitions without sleeping a fixed one-shot delay.
const REJECTED_ACK_OBSERVATION_WINDOW: Duration = Duration::from_millis(100);

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

struct FullStackHarness {
    core: TransportHarnessCore,
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
        let security = test_delivery_security(&local_member);
        let core = TransportHarnessCore::with_socket_budgets(
            system,
            default_udpour_config(),
            true,
            &[],
            0,
        );
        let manager_ref = core.manager_ref();
        let local_members: Arc<HashSet<MemberIdentity>> =
            Arc::new([local_member].into_iter().collect());
        let ingress = core.system().create(move || {
            DeliveryIngressComponent::new(DeliveryInterestConfig {
                group_memberships: SharedGroupMemberships::new(GroupMemberships::new()),
                local_members,
                hosted_mailboxes: Arc::new(HashSet::new()),
            })
        });
        let reliable_security = security.clone();
        let reliable = core.system().create(move || {
            ReliableDeliveryComponent::new(manager_ref.clone(), reliable_security.clone())
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

    fn wait_for_ingress_envelope_count(&self, message_id: MessageId, expected_count: usize) {
        let deadline = Instant::now() + WAIT_TIMEOUT;
        let mut observed_count = 0;
        while observed_count < expected_count {
            let timeout = deadline.saturating_duration_since(Instant::now());
            let observed = self
                .ingress_probe
                .actor_ref()
                .observe_indication_from(self.ingress_cursor.get(), |_| true)
                .wait_timeout(timeout)
                .expect("timed out waiting for reliable delivery ingress envelope")
                .expect("reliable delivery ingress probe should stay live");
            self.ingress_cursor.set(observed.index() + 1);
            let Some(delivery_proto::reliable_delivery_frame::Body::Envelope(envelope)) =
                observed.indication().frame.body.as_ref()
            else {
                continue;
            };
            let envelope = reliable_envelope_from_wire((**envelope).clone())
                .expect("ingress probe should observe decodable reliable envelopes");
            if envelope.header.message_id == message_id {
                observed_count += 1;
            }
        }
    }

    fn inject_recipient_ack(&self, ack: &RecipientAck) {
        self.reliable.on_definition(|component| {
            let frame = ack.to_wire_format();
            let Some(endpoint_proto::endpoint_frame::Boundary::ReliableDelivery(frame)) =
                frame.boundary
            else {
                panic!("recipient ack must encode as reliable delivery endpoint branch");
            };
            let Some(delivery_proto::reliable_delivery_frame::Body::RecipientAck(ack)) = frame.body
            else {
                panic!("recipient ack must encode as recipient ack body");
            };
            let _ = component.handle_inbound_recipient_ack(*ack);
        });
    }

    fn inject_recipient_ack_wire(&self, ack: delivery_proto::RecipientAckWire) {
        self.reliable.on_definition(|component| {
            let _ = component.handle_inbound_recipient_ack(ack);
        });
    }

    fn wait_for_sender_ack_observed(&self, message_id: MessageId) {
        eventually_component_state(
            WAIT_TIMEOUT,
            &self.reliable,
            |component| {
                component
                    .sender_work_item(message_id)
                    .is_some_and(|work_item| {
                        matches!(work_item.recipient_ack, RecipientAckStatus::Observed { .. })
                    })
            },
            format_args!(
                "timed out waiting for sender-side recipient ack observation for {message_id:?}"
            ),
        );
    }

    fn expect_sender_ack_never_observed(&self, message_id: MessageId) {
        assert_never(
            REJECTED_ACK_OBSERVATION_WINDOW,
            || {
                self.reliable.on_definition(|component| {
                    component
                        .sender_work_item(message_id)
                        .is_some_and(|work_item| {
                            matches!(work_item.recipient_ack, RecipientAckStatus::Observed { .. })
                        })
                })
            },
            format_args!("sender-side recipient ack should not be observed for {message_id:?}"),
        );
        self.reliable.on_definition(|component| {
            let work_item = component
                .sender_work_item(message_id)
                .expect("sender work item should still exist");
            assert_eq!(work_item.recipient_ack, RecipientAckStatus::Pending);
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

    fn wait_for_sender_ciphertext(&self, message_id: MessageId, expected: &Bytes) {
        eventually_component_state(
            WAIT_TIMEOUT,
            &self.reliable,
            |component| {
                component
                    .sender_work_item(message_id)
                    .is_some_and(|work_item| {
                        work_item.submit.envelope.payload.bytes.as_ref() == expected.as_ref()
                    })
            },
            format_args!(
                "timed out waiting for sender-side ciphertext {expected:?} for {message_id}"
            ),
        );
    }
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
    }
}

/// Shared fixture for sender-side recipient-ack tests that need the same
/// Alice/Bob pending work item plus Bob/Charlie signing contexts.
struct RecipientAckScenario {
    alice: MemberIdentity,
    bob: MemberIdentity,
    charlie: MemberIdentity,
    sender: FullStackHarness,
    bob_security: DeliverySecurity,
    charlie_security: DeliverySecurity,
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
            &RouteActiveState::PendingRoute {
                retry_after: None,
                reason: PendingRouteReason::PeerCurrentlyUnreachable,
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

fn test_delivery_security(local_member: &MemberIdentity) -> DeliverySecurity {
    let store = block_on(SqliteReplicationStore::in_memory(local_member.clone()))
        .expect("security store should build");
    let store = Arc::new(store);
    let trusted_members = [member_identity(&["alice"]), member_identity(&["bob"])]
        .into_iter()
        .filter(|member| member != local_member);
    block_on(provision_test_security(
        local_member.clone(),
        store.as_ref(),
        local_member,
        trusted_members,
    ))
    .expect("test security should provision");
    let store: Arc<dyn crate::api::ReplicationStore> = store;
    block_on(load_test_delivery_security(
        local_member.clone(),
        store,
        local_member,
    ))
    .expect("test security should load")
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

    sender.wait_for_sender_ack_observed(message_id);
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
        alice,
        bob,
        message_id,
        b"retry bootstrap payload",
    ));

    let deliver = receiver.wait_for_delivery();
    assert_eq!(deliver.envelope.header.message_id, message_id);
    sender.wait_for_sender_route_state(message_id, &RouteActiveState::AwaitingRecipientAck);
    drop(deliver);
    receiver.wait_for_inbound_clear(message_id);

    let redelivered = receiver.wait_for_delivery();
    assert_eq!(redelivered.envelope.header.message_id, message_id);
    assert_eq!(
        redelivered.envelope.payload.bytes,
        Bytes::from_static(b"retry bootstrap payload")
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
    sender.wait_for_sender_ack_observed(message_id);

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
    let sender =
        FullStackHarness::with_recipient_ack_timeout(alice.clone(), TEST_RECIPIENT_ACK_TIMEOUT);
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
    sender.wait_for_sender_route_state(message_id, &RouteActiveState::AwaitingRecipientAck);
    deliver
        .processed
        .complete()
        .expect("processed completion should succeed exactly once");
    receiver.wait_for_inbound_state(message_id, PendingInboundDeliveryState::AckPending);

    receiver.wait_for_ingress_envelope_count(message_id, 2);
    receiver.expect_no_delivery(Duration::from_millis(20));
    receiver.publish_direct_route(alice, sender.local_addr);
    sender.wait_for_sender_ack_observed(message_id);
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
    scenario.sender.wait_for_sender_ack_observed(message_id);
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
    scenario.sender.expect_sender_ack_never_observed(message_id);
}

#[test]
fn recipient_ack_with_wrong_original_sender_is_rejected() {
    let scenario = RecipientAckScenario::new();

    let message_id = MessageId(Uuid::from_u128(48));
    scenario.submit_pending(message_id, b"wrong original sender ack");

    let ack = scenario.bob_ack_for(&scenario.charlie, &scenario.bob, message_id);
    scenario.sender.inject_recipient_ack(&ack);
    scenario.sender.expect_sender_ack_never_observed(message_id);
}

#[test]
fn recipient_ack_with_wrong_recipient_is_rejected() {
    let scenario = RecipientAckScenario::new();

    let message_id = MessageId(Uuid::from_u128(49));
    scenario.submit_pending(message_id, b"wrong recipient ack");

    let ack = scenario.charlie_ack_for(&scenario.alice, &scenario.charlie, message_id);
    scenario.sender.inject_recipient_ack(&ack);
    scenario.sender.expect_sender_ack_never_observed(message_id);
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
    scenario.sender.expect_sender_ack_never_observed(message_id);
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
        .expect_sender_ack_never_observed(tampered_message_id);
    scenario
        .sender
        .expect_sender_ack_never_observed(signed_message_id);
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
    scenario.sender.expect_sender_ack_never_observed(message_id);
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

    sender.wait_for_sender_ciphertext(message_id, &Bytes::from_static(b"first payload"));
}
