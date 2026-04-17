//! Group-scoped delivery-domain types and the minimal direct-runtime slice.

use super::{
    contracts::{GroupBroadcastPort, GroupBroadcastPortIndication, GroupBroadcastPortRequest},
    ingress::InboundDeliveryMeta,
    route_transport::*,
    shared::{DeliveryClass, EncryptedPayload, MessageId, RouteSendId, SignedEnvelopeFooter},
};
use crate::{
    GroupMembers,
    SharedGroupMemberships,
    api::{GroupId, MemberIdentity},
};
use flotsync_core::member::TrieMap;
use flotsync_messages::delivery as delivery_proto;
use flotsync_utils::{LocalActor, NonOwningPhantomData, impl_local_actor, option_when};
use kompact::prelude::*;
use std::{collections::HashSet, sync::Arc};
use uuid::Uuid;

mod wire;
use wire::{group_envelope_from_wire, group_envelope_to_wire_format};

/// Plaintext group-scoped envelope header.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupMessageHeader {
    pub group_id: GroupId,
    pub sender: MemberIdentity,
    pub message_id: MessageId,
}

/// Immutable group-scoped fan-out envelope.
///
/// The delivery class is intentionally not part of the transmitted envelope. It
/// is a local scheduling policy on the submit request, not payload content.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupMessageEnvelope {
    pub header: GroupMessageHeader,
    pub payload: EncryptedPayload,
    pub footer: SignedEnvelopeFooter,
}

impl GroupMessageEnvelope {
    fn to_wire_format(&self) -> delivery_proto::DeliveryBoundaryFrame {
        group_envelope_to_wire_format(self)
    }
}

/// Replication-to-broadcast request.
///
/// Route expansion is intentionally not part of this message. Broadcast owns
/// route resolution based on current discovery output and configured relays.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupBroadcastSubmit {
    pub delivery_class: DeliveryClass,
    pub envelope: GroupMessageEnvelope,
    /// Skip the immediate local echo that would otherwise be emitted when the
    /// submitter is a member of the target group.
    pub suppress_self_delivery: bool,
}

/// Inbound group message delivered by the network-facing broadcast service.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupBroadcastDeliver {
    pub envelope: GroupMessageEnvelope,
}

/// Inbound group-broadcast payload handed to the semantic owner from delivery
/// ingress.
#[derive(Clone, Debug, PartialEq)]
pub struct GroupBroadcastInboundDeliver<R> {
    /// Shared ingress metadata derived before the semantic handoff.
    pub meta: InboundDeliveryMeta<R>,
    /// Fully decoded group-broadcast boundary frame owned by the generated
    /// protobuf types.
    pub frame: delivery_proto::GroupBroadcastFrame,
}

/// Internal ingress port that feeds decoded group-broadcast boundary frames
/// into the group-broadcast service.
#[derive(Clone, Copy, Debug, Default)]
pub struct GroupBroadcastInboundPort<R>(NonOwningPhantomData<R>);

impl<R> Port for GroupBroadcastInboundPort<R>
where
    R: Clone + std::fmt::Debug + Send + Sync + 'static,
{
    /// This boundary is indication-only because group broadcast never needs to
    /// send control requests back into delivery ingress.
    type Request = Never;
    type Indication = GroupBroadcastInboundDeliver<R>;
}

/// Owner component for direct group-broadcast fan-out.
#[derive(ComponentDefinition)]
pub struct GroupBroadcastComponent {
    ctx: ComponentContext<Self>,
    delivery_port: ProvidedPort<GroupBroadcastPort>,
    ingress_inbound_port: RequiredPort<TransportGroupBroadcastInboundPort>,
    discovery_port: RequiredPort<TransportRouteDiscoveryPort>,
    /// Shared read-mostly group-membership snapshot used both for local
    /// self-delivery decisions and outbound direct fan-out.
    group_memberships: SharedGroupMemberships,
    /// Route-transport actor used for one-shot direct fan-out submissions.
    route_transport: ActorRefStrong<RouteTransportActorMessage<TransportRouteKey>>,
    /// Best currently known direct route per remote peer.
    direct_peer_routes: TrieMap<SendRouteCandidate<TransportRouteKey>>,
    /// Deduplication set for submits already accepted into this component.
    accepted_submits: HashSet<MessageId>,
}

impl GroupBroadcastComponent {
    /// Create one new group-broadcast component around the shared
    /// group-membership view and route-transport actor.
    pub fn new(
        group_memberships: SharedGroupMemberships,
        route_transport: ActorRefStrong<RouteTransportActorMessage<TransportRouteKey>>,
    ) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            delivery_port: ProvidedPort::uninitialised(),
            ingress_inbound_port: RequiredPort::uninitialised(),
            discovery_port: RequiredPort::uninitialised(),
            group_memberships,
            route_transport,
            direct_peer_routes: TrieMap::new(),
            accepted_submits: HashSet::new(),
        }
    }

    fn handle_submit_request(&mut self, submit: GroupBroadcastSubmit) -> Handled {
        let envelope = submit.envelope.clone();
        let message_id = envelope.header.message_id;
        if self.accepted_submits.contains(&message_id) {
            warn!(
                self.log(),
                "Group broadcast rejected duplicate submit for existing {message_id}"
            );
            return Handled::Ok;
        }

        let group_memberships = self.group_memberships.snapshot();
        let Some(group_members) = group_memberships.members(&envelope.header.group_id) else {
            warn!(
                self.log(),
                "Group broadcast dropped submit for unknown group_id={}",
                envelope.header.group_id.0
            );
            return Handled::Ok;
        };

        let should_self_deliver =
            !submit.suppress_self_delivery && group_members.contains(&envelope.header.sender);

        let recipients = self.collect_reachable_remote_recipients(group_members, &envelope.header);
        if !should_self_deliver && recipients.is_empty() {
            return Handled::Ok;
        }

        let payload = option_when!(!recipients.is_empty(), {
            let payload: Arc<dyn FlotsyncSerializable> = Arc::new(envelope.to_wire_format());
            payload
        });

        self.accepted_submits.insert(message_id);

        if should_self_deliver {
            self.delivery_port
                .trigger(GroupBroadcastPortIndication::Deliver(
                    GroupBroadcastDeliver {
                        envelope: envelope.clone(),
                    },
                ));
        }

        if let Some(payload) = payload {
            for (recipient, route) in recipients {
                self.dispatch_direct_send(message_id, recipient, route, Arc::clone(&payload));
            }
        }
        Handled::Ok
    }

    /// Collect one currently reachable direct recipient route per remote group
    /// member for this minimal direct-only slice.
    ///
    /// This intentionally excludes shared-coverage direct routes for now, so
    /// every returned entry represents exactly one recipient-specific send.
    fn collect_reachable_remote_recipients(
        &self,
        group_members: &GroupMembers,
        header: &GroupMessageHeader,
    ) -> Vec<(MemberIdentity, SendRouteCandidate<TransportRouteKey>)> {
        // This slice is intentionally stateless with respect to retries: we
        // look up the currently best direct route at submit time and drop
        // unreachable recipients immediately.
        let mut recipients = Vec::new();
        'group_loop: for recipient in group_members.iter() {
            if recipient == header.sender {
                continue 'group_loop;
            }
            let Some(route) = self.direct_peer_routes.get(&recipient).cloned() else {
                debug!(
                    self.log(),
                    "Group broadcast dropped currently unreachable recipient={} for message_id={}",
                    recipient,
                    header.message_id
                );
                continue 'group_loop;
            };
            recipients.push((recipient, route));
        }
        recipients
    }

    fn dispatch_direct_send(
        &mut self,
        message_id: MessageId,
        recipient: MemberIdentity,
        route: SendRouteCandidate<TransportRouteKey>,
        payload: Arc<dyn FlotsyncSerializable>,
    ) {
        // Route-transport owns the actual IO lifecycle; broadcast only needs a
        // one-shot submission and observability for this direct-only slice.
        let route_transport = self.route_transport.clone();
        self.spawn_local(move |async_self| async move {
            let send = RouteTransportSend {
                send_id: RouteSendId(Uuid::new_v4()),
                route,
                payload,
            };
            let future = route_transport
                .ask_with(|promise| RouteTransportActorMessage::Submit(Ask::new(promise, send)));
            match future.await {
                Ok(RouteTransportSubmitResult::Sent { .. }) => {
                    debug!(
                        async_self.log(),
                        "Group broadcast submitted direct send for message_id={} recipient={}",
                        message_id,
                        recipient
                    );
                }
                Ok(RouteTransportSubmitResult::SendFailed { reason, .. }) => {
                    warn!(
                        async_self.log(),
                        "Group broadcast direct send failed for message_id={} recipient={}: {reason:?}",
                        message_id,
                        recipient
                    );
                }
                Err(_error) => {
                    error!(
                        async_self.log(),
                        "Group broadcast direct send promise dropped unexpectedly for message_id={} recipient={}; shutting the component down",
                        message_id,
                        recipient
                    );
                    async_self.ctx.suicide();
                }
            }
            Handled::Ok
        });
    }

    fn handle_discovery_update(&mut self, update: TransportDiscoveryRouteUpdate) -> Handled {
        match update {
            TransportDiscoveryRouteUpdate::PeerRoutes { peer, routes, .. } => {
                if let Some(route) = select_best_direct_route(routes) {
                    self.direct_peer_routes.insert(peer.clone(), route);
                } else {
                    self.direct_peer_routes.remove(&peer);
                }
            }
            TransportDiscoveryRouteUpdate::RelayRoutes { .. } => {
                // TODO(flotsync-sfo): Consume relay route updates once the
                // group-broadcast relay path exists.
                debug!(
                    self.log(),
                    "Group broadcast ignored relay route update in the direct-only slice"
                );
            }
        }
        Handled::Ok
    }

    fn handle_ingress_indication(
        &mut self,
        indication: GroupBroadcastInboundDeliver<TransportRouteKey>,
    ) -> Handled {
        let Some(body) = indication.frame.body else {
            warn!(
                self.log(),
                "Group broadcast dropped inbound frame with empty body target={:?}",
                indication.meta.target
            );
            return Handled::Ok;
        };

        match body {
            delivery_proto::group_broadcast_frame::Body::Envelope(envelope) => {
                self.handle_inbound_envelope(*envelope)
            }
            delivery_proto::group_broadcast_frame::Body::RelayStoreConfirmation(_) => {
                // TODO(flotsync-sfo): Handle relay-store confirmations once
                // relay-backed group delivery is implemented.
                debug!(
                    self.log(),
                    "Group broadcast ignored relay-store confirmation in the direct-only slice"
                );
                Handled::Ok
            }
        }
    }

    fn handle_inbound_envelope(&mut self, envelope: delivery_proto::GroupEnvelopeWire) -> Handled {
        match group_envelope_from_wire(envelope) {
            Ok(envelope) => {
                self.delivery_port
                    .trigger(GroupBroadcastPortIndication::Deliver(
                        GroupBroadcastDeliver { envelope },
                    ));
                Handled::Ok
            }
            Err(error) => {
                warn!(
                    self.log(),
                    "Group broadcast dropped inbound envelope that failed to decode: {error}"
                );
                Handled::Ok
            }
        }
    }

    #[cfg(test)]
    fn knows_submit(&self, message_id: MessageId) -> bool {
        self.accepted_submits.contains(&message_id)
    }

    #[cfg(test)]
    pub(crate) fn knows_direct_route(&self, peer: &MemberIdentity) -> bool {
        self.direct_peer_routes.get(peer).is_some()
    }
}

ignore_lifecycle!(GroupBroadcastComponent);

impl Provide<GroupBroadcastPort> for GroupBroadcastComponent {
    fn handle(&mut self, request: GroupBroadcastPortRequest) -> Handled {
        match request {
            GroupBroadcastPortRequest::Submit(submit) => self.handle_submit_request(submit),
        }
    }
}

impl Require<TransportGroupBroadcastInboundPort> for GroupBroadcastComponent {
    fn handle(&mut self, indication: GroupBroadcastInboundDeliver<TransportRouteKey>) -> Handled {
        self.handle_ingress_indication(indication)
    }
}

impl Require<TransportRouteDiscoveryPort> for GroupBroadcastComponent {
    fn handle(&mut self, indication: TransportDiscoveryRouteUpdate) -> Handled {
        self.handle_discovery_update(indication)
    }
}

impl LocalActor for GroupBroadcastComponent {
    type Message = Never;

    fn receive(&mut self, _msg: Self::Message) -> Handled {
        unreachable!("Message type cannot be instantiated");
    }
}

impl_local_actor!(GroupBroadcastComponent);

type TransportGroupBroadcastInboundPort = GroupBroadcastInboundPort<TransportRouteKey>;
type TransportRouteDiscoveryPort = RouteDiscoveryPort<TransportRouteKey>;
type TransportDiscoveryRouteUpdate = super::contracts::DiscoveryRouteUpdate<TransportRouteKey>;

fn select_best_direct_route(
    routes: Vec<SendRouteCandidate<TransportRouteKey>>,
) -> Option<SendRouteCandidate<TransportRouteKey>> {
    // TODO(flotsync-kdg): Handle shared-coverage direct routes by grouping
    // recipients per route instead of discarding non-exclusive candidates.
    routes
        .into_iter()
        .filter(|route| route.sharing == RouteSharingKind::Exclusive)
        .max_by_key(|route| route.preference_rank)
}

#[cfg(test)]
fn placeholder_signed_footer() -> SignedEnvelopeFooter {
    SignedEnvelopeFooter {
        // TODO(flotsync-d8d): Replace this placeholder once the real delivery
        // signing boundary is available.
        signature: super::shared::DetachedSignature {
            scheme: super::shared::SignatureScheme::Ed25519,
            bytes: bytes::Bytes::from_static(b"placeholder-signature"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        GroupMemberships,
        delivery::{
            ingress::{DeliveryIngressComponent, DeliveryInterestConfig},
            route_transport::{
                DatagramRouteScope,
                RoutePreferenceRank,
                RouteTransportPort,
                UdpRouteKey,
            },
            test_support::{
                DiscoveryRouteSource,
                FULL_STACK_WAIT_TIMEOUT,
                TransportHarnessCore,
                build_delivery_test_system,
                default_udpour_config,
                member_identity,
            },
        },
    };
    use bytes::Bytes;
    use flotsync_io::{
        prelude::UdpLocalBind,
        test_support::{WAIT_TIMEOUT, eventually_component_state, localhost, start_component},
    };
    use std::{
        net::SocketAddr,
        sync::{Arc, mpsc},
        time::Duration,
    };

    #[derive(ComponentDefinition)]
    struct GroupBroadcastClientProbe {
        ctx: ComponentContext<Self>,
        delivery: RequiredPort<GroupBroadcastPort>,
        indications: mpsc::Sender<GroupBroadcastPortIndication>,
    }

    impl GroupBroadcastClientProbe {
        fn new(indications: mpsc::Sender<GroupBroadcastPortIndication>) -> Self {
            Self {
                ctx: ComponentContext::uninitialised(),
                delivery: RequiredPort::uninitialised(),
                indications,
            }
        }
    }

    ignore_lifecycle!(GroupBroadcastClientProbe);

    impl Require<GroupBroadcastPort> for GroupBroadcastClientProbe {
        fn handle(&mut self, indication: GroupBroadcastPortIndication) -> Handled {
            self.indications
                .send(indication)
                .expect("group-broadcast indication receiver must stay live");
            Handled::Ok
        }
    }

    impl Actor for GroupBroadcastClientProbe {
        type Message = Never;

        fn receive_local(&mut self, _msg: Self::Message) -> Handled {
            unreachable!("Never type is empty")
        }

        fn receive_network(&mut self, _msg: NetMessage) -> Handled {
            unreachable!("client probe does not use network actor messages")
        }
    }

    struct FullStackHarness {
        core: TransportHarnessCore,
        ingress: Arc<Component<DeliveryIngressComponent>>,
        broadcast: Arc<Component<GroupBroadcastComponent>>,
        discovery_source: Arc<Component<DiscoveryRouteSource>>,
        client: Arc<Component<GroupBroadcastClientProbe>>,
        client_rx: mpsc::Receiver<GroupBroadcastPortIndication>,
        local_addr: Option<SocketAddr>,
    }

    impl FullStackHarness {
        fn new(
            local_member: MemberIdentity,
            group_memberships: GroupMemberships,
            bind_external_socket: bool,
        ) -> Self {
            let system = build_delivery_test_system();
            let core = TransportHarnessCore::new(system, default_udpour_config());
            let shared_group_memberships = SharedGroupMemberships::new(group_memberships);
            let manager_ref = core.manager_ref();
            let ingress_group_memberships = shared_group_memberships.clone();
            let ingress = core.system().create(move || {
                DeliveryIngressComponent::new(DeliveryInterestConfig {
                    group_memberships: ingress_group_memberships,
                    local_members: Arc::new([local_member.clone()].into_iter().collect()),
                    hosted_mailboxes: Arc::new(HashSet::new()),
                })
            });
            let broadcast = core.system().create(move || {
                GroupBroadcastComponent::new(shared_group_memberships, manager_ref)
            });
            let discovery_source = core.system().create(DiscoveryRouteSource::new);
            let (client_tx, client_rx) = mpsc::channel();
            let client = core
                .system()
                .create(move || GroupBroadcastClientProbe::new(client_tx));

            biconnect_components::<RouteTransportPort<TransportRouteKey>, _, _>(
                core.manager(),
                &ingress,
            )
            .expect("route transport manager must connect to delivery ingress");
            biconnect_components::<TransportGroupBroadcastInboundPort, _, _>(&ingress, &broadcast)
                .expect("delivery ingress must connect to group broadcast");
            biconnect_components::<TransportRouteDiscoveryPort, _, _>(
                &discovery_source,
                &broadcast,
            )
            .expect("discovery source must connect to group broadcast");
            biconnect_components::<GroupBroadcastPort, _, _>(&broadcast, &client)
                .expect("group broadcast must connect to the external client probe");

            core.start();
            start_component(core.system(), &ingress);
            start_component(core.system(), &broadcast);
            start_component(core.system(), &discovery_source);
            start_component(core.system(), &client);

            let local_addr = if bind_external_socket {
                let (socket_id, local_addr) = core.bind_external_socket(
                    UdpLocalBind::Exact(localhost(0)),
                    FULL_STACK_WAIT_TIMEOUT,
                );
                core.wait_for_manager_external_socket_binding(
                    socket_id,
                    local_addr,
                    FULL_STACK_WAIT_TIMEOUT,
                );
                Some(local_addr)
            } else {
                None
            };

            Self {
                core,
                ingress,
                broadcast,
                discovery_source,
                client,
                client_rx,
                local_addr,
            }
        }

        fn publish_direct_route(&self, peer: MemberIdentity, remote_addr: SocketAddr) {
            let route = SendRouteCandidate {
                coverage_key: TransportRouteKey::Udp(UdpRouteKey {
                    remote_addr,
                    scope: DatagramRouteScope::Unicast,
                    local_bind: self.local_addr,
                }),
                sharing: RouteSharingKind::Exclusive,
                preference_rank: RoutePreferenceRank::new(1),
            };
            let route_peer = peer.clone();
            self.discovery_source.on_definition(|component| {
                component
                    .discovery
                    .trigger(TransportDiscoveryRouteUpdate::PeerRoutes {
                        peer: route_peer,
                        classification: super::super::shared::ReachabilityClass::Reachable,
                        routes: vec![route],
                    });
            });
            eventually_component_state(
                FULL_STACK_WAIT_TIMEOUT,
                &self.broadcast,
                |component| component.knows_direct_route(&peer),
                "timed out waiting for group-broadcast route publication",
            );
        }

        fn submit(&self, submit: GroupBroadcastSubmit) {
            self.client.on_definition(|component| {
                component
                    .delivery
                    .trigger(GroupBroadcastPortRequest::Submit(submit));
            });
        }

        fn wait_for_delivery(&self) -> GroupBroadcastDeliver {
            match self
                .client_rx
                .recv_timeout(FULL_STACK_WAIT_TIMEOUT)
                .expect("timed out waiting for group-broadcast indication")
            {
                GroupBroadcastPortIndication::Deliver(deliver) => deliver,
            }
        }

        fn expect_no_delivery(&self, timeout: Duration) {
            match self.client_rx.recv_timeout(timeout) {
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    panic!("group-broadcast test client disconnected while expecting silence")
                }
                Ok(indication) => {
                    panic!(
                        "unexpected group-broadcast indication while expecting silence: {indication:?}"
                    )
                }
            }
        }

        fn wait_for_known_submit(&self, message_id: MessageId) {
            eventually_component_state(
                FULL_STACK_WAIT_TIMEOUT,
                &self.broadcast,
                |component| component.knows_submit(message_id),
                "timed out waiting for group-broadcast submit admission",
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
                .kill_notify(self.discovery_source.clone())
                .wait_timeout(WAIT_TIMEOUT);
            let _ = self
                .core
                .system()
                .kill_notify(self.broadcast.clone())
                .wait_timeout(WAIT_TIMEOUT);
            let _ = self
                .core
                .system()
                .kill_notify(self.ingress.clone())
                .wait_timeout(WAIT_TIMEOUT);
        }
    }

    #[test]
    fn group_broadcast_fans_out_to_two_receivers_and_self() {
        let group_id = GroupId(Uuid::from_u128(1));
        let alice = member_identity(&["alice"]);
        let bob = member_identity(&["bob"]);
        let charlie = member_identity(&["charlie"]);
        let memberships =
            group_memberships(group_id, [alice.clone(), bob.clone(), charlie.clone()]);
        let sender = FullStackHarness::new(alice.clone(), memberships.clone(), false);
        let receiver_bob = FullStackHarness::new(bob.clone(), memberships.clone(), true);
        let receiver_charlie = FullStackHarness::new(charlie.clone(), memberships, true);

        sender.publish_direct_route(
            bob.clone(),
            receiver_bob
                .local_addr
                .expect("receiver harness must bind an external socket"),
        );
        sender.publish_direct_route(
            charlie.clone(),
            receiver_charlie
                .local_addr
                .expect("receiver harness must bind an external socket"),
        );

        let message_id = MessageId(Uuid::from_u128(2));
        let submit = submit(
            group_id,
            alice.clone(),
            message_id,
            Bytes::from_static(b"group payload"),
            false,
        );
        sender.submit(submit.clone());

        sender.wait_for_known_submit(message_id);

        let sender_deliver = sender.wait_for_delivery();
        let bob_deliver = receiver_bob.wait_for_delivery();
        let charlie_deliver = receiver_charlie.wait_for_delivery();

        assert_eq!(sender_deliver.envelope, submit.envelope);
        assert_eq!(bob_deliver.envelope, submit.envelope);
        assert_eq!(charlie_deliver.envelope, submit.envelope);
    }

    #[test]
    fn suppress_self_delivery_keeps_remote_fanout() {
        let group_id = GroupId(Uuid::from_u128(11));
        let alice = member_identity(&["alice"]);
        let bob = member_identity(&["bob"]);
        let charlie = member_identity(&["charlie"]);
        let memberships =
            group_memberships(group_id, [alice.clone(), bob.clone(), charlie.clone()]);
        let sender = FullStackHarness::new(alice.clone(), memberships.clone(), false);
        let receiver_bob = FullStackHarness::new(bob.clone(), memberships.clone(), true);
        let receiver_charlie = FullStackHarness::new(charlie.clone(), memberships, true);

        sender.publish_direct_route(
            bob.clone(),
            receiver_bob
                .local_addr
                .expect("receiver harness must bind an external socket"),
        );
        sender.publish_direct_route(
            charlie.clone(),
            receiver_charlie
                .local_addr
                .expect("receiver harness must bind an external socket"),
        );

        let submit = submit(
            group_id,
            alice,
            MessageId(Uuid::from_u128(12)),
            Bytes::from_static(b"group payload"),
            true,
        );
        sender.submit(submit.clone());

        // `suppress_self_delivery` is only observable by proving the sender's
        // client port stays silent after the submit is accepted.
        sender.expect_no_delivery(Duration::from_millis(500));
        assert_eq!(receiver_bob.wait_for_delivery().envelope, submit.envelope);
        assert_eq!(
            receiver_charlie.wait_for_delivery().envelope,
            submit.envelope
        );
    }

    #[test]
    fn currently_unreachable_member_is_dropped_without_retry_state() {
        let group_id = GroupId(Uuid::from_u128(21));
        let alice = member_identity(&["alice"]);
        let bob = member_identity(&["bob"]);
        let charlie = member_identity(&["charlie"]);
        let memberships =
            group_memberships(group_id, [alice.clone(), bob.clone(), charlie.clone()]);
        let sender = FullStackHarness::new(alice.clone(), memberships.clone(), false);
        let receiver_bob = FullStackHarness::new(bob.clone(), memberships.clone(), true);
        let receiver_charlie = FullStackHarness::new(charlie.clone(), memberships, true);

        sender.publish_direct_route(
            bob.clone(),
            receiver_bob
                .local_addr
                .expect("receiver harness must bind an external socket"),
        );

        let submit = submit(
            group_id,
            alice,
            MessageId(Uuid::from_u128(22)),
            Bytes::from_static(b"group payload"),
            false,
        );
        sender.submit(submit.clone());

        assert_eq!(sender.wait_for_delivery().envelope, submit.envelope);
        assert_eq!(receiver_bob.wait_for_delivery().envelope, submit.envelope);
        // Charlie has no direct route at submit time, so the message must be
        // dropped rather than queued for a later delivery.
        receiver_charlie.expect_no_delivery(Duration::from_millis(500));

        sender.publish_direct_route(
            charlie,
            receiver_charlie
                .local_addr
                .expect("receiver harness must bind an external socket"),
        );
        // Publishing a route after the submit must not retroactively fan out
        // the already dropped message.
        receiver_charlie.expect_no_delivery(Duration::from_millis(500));
    }

    fn group_memberships(
        group_id: GroupId,
        members: impl IntoIterator<Item = MemberIdentity>,
    ) -> GroupMemberships {
        let group_members =
            GroupMembers::from_ordered_members(members).expect("test memberships should build");
        GroupMemberships::from_groups([(group_id, group_members)])
    }

    fn submit(
        group_id: GroupId,
        sender: MemberIdentity,
        message_id: MessageId,
        ciphertext: Bytes,
        suppress_self_delivery: bool,
    ) -> GroupBroadcastSubmit {
        GroupBroadcastSubmit {
            delivery_class: DeliveryClass::BestEffort,
            envelope: GroupMessageEnvelope {
                header: GroupMessageHeader {
                    group_id,
                    sender,
                    message_id,
                },
                payload: EncryptedPayload { ciphertext },
                footer: placeholder_signed_footer(),
            },
            suppress_self_delivery,
        }
    }
}
