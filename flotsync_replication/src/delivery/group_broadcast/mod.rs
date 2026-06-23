//! Group-scoped delivery-domain types and the minimal direct-runtime slice.

use super::{
    contracts::{GroupBroadcastPort, GroupBroadcastPortIndication, GroupBroadcastPortRequest},
    ingress::InboundDeliveryMeta,
    security::{DeliverySecurity, DeliverySecurityError},
    shared::{DeliveryClass, MessageId, PlaintextPayload},
};
use bytes::Bytes;
use flotsync_core::{
    GroupId,
    MemberIdentity,
    member::TrieMap,
    membership::{GroupMembers, SharedGroupMemberships},
};
use flotsync_messages::{
    delivery as delivery_proto,
    endpoint as endpoint_proto,
    serialisation::FlotsyncSerializable,
};
use flotsync_route_transport::{
    RouteDiscoveryPort,
    RouteSendId,
    RouteSharingKind,
    RouteTransportActorMessage,
    RouteTransportSend,
    RouteTransportSubmitResult,
    SendRouteCandidate,
    TransportRouteKey,
};
use flotsync_security::SealedPSKPayload;
use flotsync_utils::{NonOwningPhantomData, OptionExt as _, ResultExt as _};
use kompact::prelude::*;
use std::{collections::HashSet, sync::Arc};
use uuid::Uuid;

mod wire;
use wire::{group_envelope_from_wire, group_envelope_to_wire_format, group_public_header_bytes};

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
pub struct GroupMessageEnvelope<P> {
    pub header: GroupMessageHeader,
    pub payload: P,
}

impl GroupMessageEnvelope<SealedPSKPayload> {
    fn to_wire_format(&self) -> endpoint_proto::EndpointFrame {
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
    pub envelope: GroupMessageEnvelope<PlaintextPayload>,
    /// Skip the immediate local echo that would otherwise be emitted when the
    /// submitter is a member of the target group.
    pub suppress_self_delivery: bool,
}

/// First stage of a group-broadcast submit builder.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GroupBroadcastSubmitBuilder {
    delivery_class: DeliveryClass,
}

/// Submit builder stage after the sender and group have been selected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupBroadcastSubmitMemberBuilder {
    delivery_class: DeliveryClass,
    group_id: GroupId,
    sender: MemberIdentity,
}

impl GroupBroadcastPortRequest {
    /// Start building one signed submit request with a fresh group-broadcast message id.
    #[must_use]
    pub fn build_submit(delivery_class: DeliveryClass) -> GroupBroadcastSubmitBuilder {
        GroupBroadcastSubmitBuilder { delivery_class }
    }
}

impl GroupBroadcastSubmitBuilder {
    /// Select the logical sender and group for this submit request.
    #[must_use]
    pub fn for_member_in_group(
        self,
        sender: MemberIdentity,
        group_id: GroupId,
    ) -> GroupBroadcastSubmitMemberBuilder {
        GroupBroadcastSubmitMemberBuilder {
            delivery_class: self.delivery_class,
            group_id,
            sender,
        }
    }
}

impl GroupBroadcastSubmitMemberBuilder {
    /// Finish the submit with plaintext runtime payload bytes.
    ///
    /// Group broadcast owns the generated envelope identity, endpoint-frame
    /// wrapping, sealing, and default self-delivery suppression policy.
    #[must_use]
    pub fn with_payload(self, bytes: Bytes) -> GroupBroadcastPortRequest {
        GroupBroadcastPortRequest::Submit(GroupBroadcastSubmit {
            delivery_class: self.delivery_class,
            envelope: GroupMessageEnvelope {
                header: GroupMessageHeader {
                    group_id: self.group_id,
                    sender: self.sender,
                    message_id: MessageId(Uuid::new_v4()),
                },
                payload: PlaintextPayload { bytes },
            },
            suppress_self_delivery: true,
        })
    }
}

/// Inbound group message delivered by the network-facing broadcast service.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupBroadcastDeliver {
    pub envelope: GroupMessageEnvelope<PlaintextPayload>,
}

/// Inbound group-broadcast payload handed to the semantic owner from delivery
/// ingress.
#[derive(Clone, Debug, PartialEq)]
pub struct GroupBroadcastInboundDeliver<R> {
    /// Shared ingress metadata derived before the semantic handoff.
    pub meta: InboundDeliveryMeta<R>,
    /// Fully decoded group-broadcast endpoint branch owned by the generated
    /// protobuf types.
    pub frame: delivery_proto::GroupBroadcastFrame,
}

/// Internal ingress port that feeds decoded group-broadcast endpoint branches
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
    /// Security state used to seal outbound payloads and open inbound payloads.
    security: DeliverySecurity,
    /// Best currently known direct route per remote peer.
    direct_peer_routes: TrieMap<SendRouteCandidate<TransportRouteKey>>,
    /// Deduplication set for submits already accepted into this component.
    accepted_submits: HashSet<MessageId>,
}

impl GroupBroadcastComponent {
    /// Create one new group-broadcast component around the shared
    /// group-membership view and route-transport actor.
    #[must_use]
    pub(crate) fn new(
        group_memberships: SharedGroupMemberships,
        route_transport: ActorRefStrong<RouteTransportActorMessage<TransportRouteKey>>,
        security: DeliverySecurity,
    ) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            delivery_port: ProvidedPort::uninitialised(),
            ingress_inbound_port: RequiredPort::uninitialised(),
            discovery_port: RequiredPort::uninitialised(),
            group_memberships,
            route_transport,
            security,
            direct_peer_routes: TrieMap::new(),
            accepted_submits: HashSet::new(),
        }
    }

    fn handle_submit_request(&mut self, submit: &GroupBroadcastSubmit) -> HandlerResult {
        let envelope = submit.envelope.clone();
        let message_id = envelope.header.message_id;
        if self.accepted_submits.contains(&message_id) {
            warn!(
                self.log(),
                "Group broadcast rejected duplicate submit for existing {message_id}"
            );
            return Handled::OK;
        }

        let group_memberships = self.group_memberships.snapshot();
        let group_members = group_memberships
            .members(&envelope.header.group_id)
            .with_whatever_benign(|| {
                format!(
                    "Group broadcast dropped submit for unknown group_id={}",
                    envelope.header.group_id.0
                )
            })?;

        let should_self_deliver =
            !submit.suppress_self_delivery && group_members.contains(&envelope.header.sender);

        let recipients = self.collect_reachable_remote_recipients(group_members, &envelope.header);
        if !should_self_deliver && recipients.is_empty() {
            return Handled::OK;
        }

        self.accepted_submits.insert(message_id);
        if recipients.is_empty() {
            if should_self_deliver {
                self.delivery_port
                    .trigger(GroupBroadcastPortIndication::Deliver(
                        GroupBroadcastDeliver { envelope },
                    ));
            }
            return Handled::OK;
        }

        if should_self_deliver {
            self.delivery_port
                .trigger(GroupBroadcastPortIndication::Deliver(
                    GroupBroadcastDeliver {
                        envelope: envelope.clone(),
                    },
                ));
        }

        Handled::block_on(self, async move |mut async_self| {
            let sealed = async_self
                .seal_outbound_envelope(envelope)
                .await
                .benign_err()?;
            let payload: Arc<dyn FlotsyncSerializable> = Arc::new(sealed.to_wire_format());
            for (recipient, route) in recipients {
                async_self.dispatch_direct_send(message_id, recipient, route, Arc::clone(&payload));
            }
            Handled::OK
        })
    }

    /// Seal one accepted plaintext envelope for delivery across the transport boundary.
    async fn seal_outbound_envelope(
        &mut self,
        envelope: GroupMessageEnvelope<PlaintextPayload>,
    ) -> Result<GroupMessageEnvelope<SealedPSKPayload>, DeliverySecurityError> {
        let GroupMessageEnvelope { header, payload } = envelope;
        let public_header = group_public_header_bytes(&header);
        let sealed = self
            .security
            .seal_group_payload(&header, public_header.as_ref(), payload.bytes.as_ref())
            .await?;
        Ok(GroupMessageEnvelope {
            header,
            payload: sealed,
        })
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
        self.spawn_local(move |async_self| async move {
            let send = RouteTransportSend {
                send_id: RouteSendId(Uuid::new_v4()),
                route,
                payload,
            };
            let future = async_self
                .route_transport
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
            Handled::OK
        });
    }

    fn handle_discovery_update(&mut self, update: TransportDiscoveryRouteUpdate) -> HandlerResult {
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
        Handled::OK
    }

    fn handle_ingress_indication(
        &mut self,
        indication: GroupBroadcastInboundDeliver<TransportRouteKey>,
    ) -> HandlerResult {
        let body = indication.frame.body.with_whatever_benign(|| {
            format!(
                "Group broadcast dropped inbound frame with empty body target={:?}",
                indication.meta.target
            )
        })?;

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
                Handled::OK
            }
        }
    }

    fn handle_inbound_envelope(
        &mut self,
        envelope: delivery_proto::GroupEnvelopeWire,
    ) -> HandlerResult {
        match group_envelope_from_wire(envelope) {
            Ok(envelope) => {
                self.spawn_local(async move |mut async_self| {
                    let envelope = async_self
                        .open_inbound_envelope(envelope)
                        .await
                        .benign_err()?;
                    async_self
                        .delivery_port
                        .trigger(GroupBroadcastPortIndication::Deliver(
                            GroupBroadcastDeliver { envelope },
                        ));
                    Handled::OK
                });
                Handled::OK
            }
            Err(error) => Err(error)
                .whatever_benign("Group broadcast dropped inbound envelope that failed to decode"),
        }
    }

    /// Open one inbound sealed envelope before handing plaintext to semantic owners.
    async fn open_inbound_envelope(
        &mut self,
        envelope: GroupMessageEnvelope<SealedPSKPayload>,
    ) -> Result<GroupMessageEnvelope<PlaintextPayload>, DeliverySecurityError> {
        let GroupMessageEnvelope { header, payload } = envelope;
        let public_header = group_public_header_bytes(&header);
        let plaintext = self
            .security
            .open_group_payload(&header, public_header.as_ref(), &payload)
            .await?;
        Ok(GroupMessageEnvelope {
            header,
            payload: PlaintextPayload { bytes: plaintext },
        })
    }

    #[cfg(test)]
    fn knows_submit(&self, message_id: MessageId) -> bool {
        self.accepted_submits.contains(&message_id)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn knows_direct_route(&self, peer: &MemberIdentity) -> bool {
        self.direct_peer_routes.get(peer).is_some()
    }
}

ignore_lifecycle!(GroupBroadcastComponent);

impl Provide<GroupBroadcastPort> for GroupBroadcastComponent {
    fn handle(&mut self, request: GroupBroadcastPortRequest) -> HandlerResult {
        match request {
            GroupBroadcastPortRequest::Submit(submit) => self.handle_submit_request(&submit),
        }
    }
}

impl Require<TransportGroupBroadcastInboundPort> for GroupBroadcastComponent {
    fn handle(
        &mut self,
        indication: GroupBroadcastInboundDeliver<TransportRouteKey>,
    ) -> HandlerResult {
        self.handle_ingress_indication(indication)
    }
}

impl Require<TransportRouteDiscoveryPort> for GroupBroadcastComponent {
    fn handle(&mut self, indication: TransportDiscoveryRouteUpdate) -> HandlerResult {
        self.handle_discovery_update(indication)
    }
}

impl Actor for GroupBroadcastComponent {
    type Message = Never;

    fn receive_local(&mut self, _msg: Self::Message) -> HandlerResult {
        unreachable!("Message type cannot be instantiated");
    }
}

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
mod tests {
    use super::*;
    use crate::{
        SqliteReplicationStore,
        api::{ReplicationGroupRecord, ReplicationStore},
        delivery::{
            ingress::{DeliveryIngressComponent, DeliveryInterestConfig, DeliveryTargetHint},
            test_support::{
                FULL_STACK_WAIT_TIMEOUT,
                TransportHarnessCore,
                build_delivery_test_system,
                default_udpour_config,
                member_identity,
            },
            wire::{group_id_from_wire, message_id_from_wire},
        },
        test_support::{load_test_delivery_security, provision_test_security, test_group_key},
    };
    use bytes::Bytes;
    use flotsync_core::{MemberIndex, membership::GroupMemberships, versions::VersionVector};
    use flotsync_io::{
        prelude::UdpLocalBind,
        test_support::{WAIT_TIMEOUT, eventually_component_state, localhost, start_component},
    };
    use flotsync_route_transport::{
        DatagramRouteScope,
        InboundTransportMeta,
        RoutePreferenceRank,
        RouteTransportPort,
        UdpRouteKey,
    };
    use flotsync_utils::kompact_testing::{PortTesterComponent, PortTestingExt, PortTestingRefExt};
    use std::{cell::Cell, net::SocketAddr, num::NonZeroUsize, sync::Arc, time::Duration};

    struct FullStackHarness {
        core: TransportHarnessCore,
        ingress: Arc<Component<DeliveryIngressComponent>>,
        inbound_source: Arc<Component<PortTesterComponent<TransportGroupBroadcastInboundPort>>>,
        broadcast: Arc<Component<GroupBroadcastComponent>>,
        discovery_source: Arc<Component<PortTesterComponent<TransportRouteDiscoveryPort>>>,
        client: Arc<Component<PortTesterComponent<GroupBroadcastPort>>>,
        client_cursor: Cell<usize>,
        local_addr: Option<SocketAddr>,
    }

    impl FullStackHarness {
        fn new(
            local_member: MemberIdentity,
            group_memberships: GroupMemberships,
            bind_external_socket: bool,
        ) -> Self {
            let system = build_delivery_test_system();
            let manager_owned_udp_sockets = usize::from(!bind_external_socket);
            let core = TransportHarnessCore::with_socket_budgets(
                system,
                default_udpour_config(),
                bind_external_socket,
                &[],
                manager_owned_udp_sockets,
            );
            let security = test_group_security(&local_member, &group_memberships);
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
            let inbound_source = core
                .system()
                .create(TransportGroupBroadcastInboundPort::tester_component_sidecar);
            let broadcast = core.system().create(move || {
                GroupBroadcastComponent::new(shared_group_memberships, manager_ref, security)
            });
            let client = core
                .system()
                .create(GroupBroadcastPort::tester_component_sidecar);
            let discovery_source = core
                .system()
                .create(TransportRouteDiscoveryPort::tester_component_sidecar);

            biconnect_components::<RouteTransportPort<TransportRouteKey>, _, _>(
                core.manager(),
                &ingress,
            )
            .expect("route transport manager must connect to delivery ingress");
            biconnect_components::<TransportGroupBroadcastInboundPort, _, _>(&ingress, &broadcast)
                .expect("delivery ingress must connect to group broadcast");
            biconnect_components::<TransportGroupBroadcastInboundPort, _, _>(
                &inbound_source,
                &broadcast,
            )
            .expect("inbound test source must connect to group broadcast");
            biconnect_components::<TransportRouteDiscoveryPort, _, _>(
                &discovery_source,
                &broadcast,
            )
            .expect("discovery source must connect to group broadcast");
            biconnect_components::<GroupBroadcastPort, _, _>(&broadcast, &client)
                .expect("group broadcast must connect to the external client probe");

            core.start();
            start_component(core.system(), &ingress);
            start_component(core.system(), &inbound_source);
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
                inbound_source,
                broadcast,
                discovery_source,
                client,
                client_cursor: Cell::new(0),
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
            let expected_peer = peer.clone();
            self.discovery_source.actor_ref().inject_indication(
                TransportDiscoveryRouteUpdate::PeerRoutes {
                    peer,
                    routes: vec![route],
                },
            );
            eventually_component_state(
                FULL_STACK_WAIT_TIMEOUT,
                &self.broadcast,
                |component| component.knows_direct_route(&expected_peer),
                "timed out waiting for group-broadcast route publication",
            );
        }

        fn submit(&self, submit: GroupBroadcastSubmit) {
            self.client
                .actor_ref()
                .inject_request(GroupBroadcastPortRequest::Submit(submit));
        }

        fn wait_for_delivery(&self) -> GroupBroadcastDeliver {
            let observed = self
                .client
                .actor_ref()
                .observe_indication_from(self.client_cursor.get(), |_| true)
                .wait_timeout(FULL_STACK_WAIT_TIMEOUT)
                .expect("timed out waiting for group-broadcast indication")
                .expect("group-broadcast client probe should stay live");
            self.client_cursor.set(observed.index() + 1);
            match observed.indication() {
                GroupBroadcastPortIndication::Deliver(deliver) => deliver.clone(),
            }
        }

        fn expect_no_delivery(&self, timeout: Duration) {
            self.client
                .actor_ref()
                .fail_if_indication_observed_from(self.client_cursor.get(), timeout, |_| true)
                .wait_timeout(FULL_STACK_WAIT_TIMEOUT)
                .expect("group-broadcast absence check should complete")
                .expect("group-broadcast client probe should stay live")
                .expect("group-broadcast client should stay silent");
        }

        fn wait_for_known_submit(&self, message_id: MessageId) {
            eventually_component_state(
                FULL_STACK_WAIT_TIMEOUT,
                &self.broadcast,
                |component| component.knows_submit(message_id),
                "timed out waiting for group-broadcast submit admission",
            );
        }

        /// Inject one group envelope through the inbound port with matching delivery metadata.
        fn inject_inbound_envelope(&self, envelope: delivery_proto::GroupEnvelopeWire) {
            let group_id = group_id_from_wire(
                &envelope.public_header.group_id,
                "GroupEnvelopeHeader.group_id",
            )
            .expect("test group header should contain a valid group id");
            let message_id = message_id_from_wire(
                &envelope.public_header.message_id,
                "GroupEnvelopeHeader.message_id",
            )
            .expect("test group header should contain a valid message id");
            let frame = delivery_proto::GroupBroadcastFrame {
                body: Some(delivery_proto::group_broadcast_frame::Body::Envelope(
                    Box::new(envelope),
                )),
                ..delivery_proto::GroupBroadcastFrame::default()
            };
            self.inbound_source
                .actor_ref()
                .inject_indication(GroupBroadcastInboundDeliver {
                    meta: InboundDeliveryMeta {
                        transport: test_inbound_transport_meta(),
                        target: DeliveryTargetHint::GroupBroadcast {
                            group_id,
                            delivery_message_id: message_id,
                        },
                        delivery_message_id: Some(message_id),
                        verified_sender: None,
                    },
                    frame,
                });
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
                .kill_notify(self.inbound_source.clone())
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

    #[test]
    fn inbound_sealed_group_wire_opens_to_plaintext_delivery() {
        let group_id = GroupId(Uuid::from_u128(31));
        let alice = member_identity(&["alice"]);
        let bob = member_identity(&["bob"]);
        let memberships = group_memberships(group_id, [alice.clone(), bob.clone()]);
        let alice_security = test_group_security(&alice, &memberships);
        let receiver_bob = FullStackHarness::new(bob, memberships, false);
        let message_id = MessageId(Uuid::from_u128(32));
        let wire = sealed_inbound_wire(
            &alice_security,
            group_id,
            alice.clone(),
            message_id,
            b"group payload",
        );

        receiver_bob.inject_inbound_envelope(wire);

        let deliver = receiver_bob.wait_for_delivery();
        assert_eq!(
            deliver.envelope,
            GroupMessageEnvelope {
                header: GroupMessageHeader {
                    group_id,
                    sender: alice,
                    message_id,
                },
                payload: PlaintextPayload {
                    bytes: Bytes::from_static(b"group payload"),
                },
            }
        );
    }

    #[test]
    fn inbound_tampered_group_wire_is_dropped_before_delivery() {
        let group_id = GroupId(Uuid::from_u128(41));
        let alice = member_identity(&["alice"]);
        let bob = member_identity(&["bob"]);
        let memberships = group_memberships(group_id, [alice.clone(), bob.clone()]);
        let alice_security = test_group_security(&alice, &memberships);
        let receiver_bob = FullStackHarness::new(bob, memberships, false);

        for tamper in [
            InboundWireTamper::Signature,
            InboundWireTamper::Ciphertext,
            InboundWireTamper::PublicHeader,
        ] {
            let message_id = MessageId(Uuid::from_u128(42 + tamper.offset()));
            let mut wire = sealed_inbound_wire(
                &alice_security,
                group_id,
                alice.clone(),
                message_id,
                b"group payload",
            );
            tamper.apply(&mut wire);

            receiver_bob.inject_inbound_envelope(wire);
            receiver_bob.expect_no_delivery(Duration::from_millis(500));
        }
    }

    fn group_memberships(
        group_id: GroupId,
        members: impl IntoIterator<Item = MemberIdentity>,
    ) -> GroupMemberships {
        let group_members =
            GroupMembers::from_ordered_members(members).expect("test memberships should build");
        GroupMemberships::from_groups([(group_id, group_members)])
    }

    fn test_group_security(
        local_member: &MemberIdentity,
        group_memberships: &GroupMemberships,
    ) -> DeliverySecurity {
        let store = Arc::new(
            SqliteReplicationStore::in_memory(local_member.clone())
                .expect("security store should build"),
        );
        let trusted_members = trusted_members_for(local_member, group_memberships);
        block_on(provision_test_security(
            local_member.clone(),
            store.as_ref(),
            local_member,
            trusted_members,
        ))
        .expect("test security should provision");
        let security_store: Arc<dyn ReplicationStore> = store.clone();
        let security = block_on(load_test_delivery_security(
            local_member.clone(),
            security_store,
            local_member,
        ))
        .expect("test security should load");
        persist_group_security(store.as_ref(), local_member, group_memberships, &security);
        security
    }

    fn trusted_members_for(
        local_member: &MemberIdentity,
        group_memberships: &GroupMemberships,
    ) -> HashSet<MemberIdentity> {
        let mut trusted_members = HashSet::new();
        for group_id in group_memberships.group_ids() {
            let members = group_memberships
                .members(group_id)
                .expect("group id came from the same membership snapshot");
            for member in members.iter() {
                if &member != local_member {
                    trusted_members.insert(member);
                }
            }
        }
        trusted_members
    }

    fn persist_group_security(
        store: &dyn ReplicationStore,
        local_member: &MemberIdentity,
        group_memberships: &GroupMemberships,
        security: &DeliverySecurity,
    ) {
        let mut transaction =
            block_on(store.begin_transaction()).expect("security transaction should start");
        for group_id in group_memberships.group_ids() {
            let members = group_memberships
                .members(group_id)
                .expect("group id came from the same membership snapshot");
            let ordered_members = members.ordered_members();
            let local_member_index = ordered_members
                .iter()
                .position(|member| member == local_member)
                .expect("test local member should belong to every hosted group");
            let security_material = security
                .seal_group_secret(group_id.0, &test_group_key(*group_id))
                .expect("test group secret should seal");
            let group = ReplicationGroupRecord {
                group_id: *group_id,
                members: ordered_members,
                local_member_index: MemberIndex::new(
                    u32::try_from(local_member_index).expect("test group index should fit u32"),
                ),
                version_vector: VersionVector::initial(
                    NonZeroUsize::new(members.len()).expect("test group must have members"),
                ),
                security_material,
            };
            block_on(transaction.insert_replication_group(group))
                .expect("test group security should persist");
        }
        block_on(transaction.commit()).expect("security transaction should commit");
    }

    fn submit(
        group_id: GroupId,
        sender: MemberIdentity,
        message_id: MessageId,
        bytes: Bytes,
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
                payload: PlaintextPayload { bytes },
            },
            suppress_self_delivery,
        }
    }

    /// Build one signed and sealed inbound wire envelope using sender-side security.
    fn sealed_inbound_wire(
        sender_security: &DeliverySecurity,
        group_id: GroupId,
        sender: MemberIdentity,
        message_id: MessageId,
        bytes: &[u8],
    ) -> delivery_proto::GroupEnvelopeWire {
        let header = GroupMessageHeader {
            group_id,
            sender,
            message_id,
        };
        let public_header = group_public_header_bytes(&header);
        let sealed =
            block_on(sender_security.seal_group_payload(&header, public_header.as_ref(), bytes))
                .expect("test inbound group payload should seal");
        let envelope = GroupMessageEnvelope {
            header,
            payload: sealed,
        };
        let boundary = envelope.to_wire_format();
        let Some(endpoint_proto::endpoint_frame::Boundary::GroupBroadcast(frame)) =
            boundary.boundary
        else {
            panic!("sealed test envelope should encode as group broadcast endpoint branch");
        };
        let Some(delivery_proto::group_broadcast_frame::Body::Envelope(envelope)) = frame.body
        else {
            panic!("sealed test envelope should encode as group envelope");
        };
        *envelope
    }

    /// Build deterministic transport metadata for direct inbound-port tests.
    fn test_inbound_transport_meta() -> InboundTransportMeta<TransportRouteKey> {
        let remote_addr: SocketAddr = "127.0.0.1:39000"
            .parse()
            .expect("test remote address should parse");
        InboundTransportMeta {
            route: TransportRouteKey::Udp(UdpRouteKey {
                remote_addr,
                scope: DatagramRouteScope::Unicast,
                local_bind: None,
            }),
            remote_addr: Some(remote_addr),
        }
    }

    /// Post-seal mutation proving inbound verification happens before delivery.
    #[derive(Clone, Copy, Debug)]
    enum InboundWireTamper {
        Signature,
        Ciphertext,
        PublicHeader,
    }

    impl InboundWireTamper {
        fn offset(self) -> u128 {
            match self {
                Self::Signature => 1,
                Self::Ciphertext => 2,
                Self::PublicHeader => 3,
            }
        }

        fn apply(self, envelope: &mut delivery_proto::GroupEnvelopeWire) {
            match self {
                Self::Signature => {
                    let sealed_payload = envelope
                        .sealed_payload
                        .as_option_mut()
                        .expect("test sealed payload should be present");
                    sealed_payload.signature[0] ^= 0x01;
                }
                Self::Ciphertext => {
                    let sealed_payload = envelope
                        .sealed_payload
                        .as_option_mut()
                        .expect("test sealed payload should be present");
                    let mut ciphertext = sealed_payload.ciphertext.to_vec();
                    ciphertext[0] ^= 0x01;
                    sealed_payload.ciphertext = Bytes::from(ciphertext);
                }
                Self::PublicHeader => {
                    let public_header = envelope
                        .public_header
                        .as_option_mut()
                        .expect("test public header should be present");
                    public_header.message_id = Uuid::from_u128(999).as_bytes().to_vec();
                }
            }
        }
    }
}
