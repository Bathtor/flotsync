//! Delivery-ingress boundary between route transport and semantic delivery.
//!
//! `DeliveryIngress` owns early local-interest checks and delivery-wire
//! demultiplexing after route transport reassembles one full logical payload.
//!
//! The intended flow is:
//!
//! - route transport emits one [`RouteTransportInboundDeliver`] for each fully
//!   reassembled transport payload;
//! - delivery ingress cheaply classifies that payload against local interest;
//! - irrelevant payloads are dropped before full protobuf decode;
//! - relevant payloads are fully decoded and handed to the semantic owner.

use super::{
    group_broadcast::{GroupBroadcastInboundDeliver, GroupBroadcastInboundPort},
    reliable_delivery::{ReliableDeliveryInboundDeliver, ReliableDeliveryInboundPort},
    shared::{MessageId, RouteEndpoint},
    wire::{
        DecodedDeliveryFrame,
        DeliveryInterestView,
        decode_endpoint_frame_if_delivery_relevant,
    },
};
use flotsync_core::{GroupId, MemberIdentity, membership::SharedGroupMemberships};
use flotsync_routes::{
    InboundTransportMeta,
    RouteTransportInboundDeliver,
    RouteTransportPort,
    TransportRouteKey,
};
use flotsync_utils::ResultExt as _;
use kompact::{Never, prelude::*};
use std::{collections::HashSet, sync::Arc};

/// Shared local-interest sets consulted before expensive delivery-wire decode.
#[derive(Clone)]
pub struct DeliveryInterestConfig {
    /// Dynamically changing group-membership view used for early local
    /// admission checks and group-broadcast fan-out.
    pub group_memberships: SharedGroupMemberships,
    /// Member identities hosted locally by this node.
    pub local_members: Arc<HashSet<MemberIdentity>>,
    /// Mailboxes this node is currently willing to serve as a relay.
    pub hosted_mailboxes: Arc<HashSet<MemberIdentity>>,
}

impl Default for DeliveryInterestConfig {
    fn default() -> Self {
        Self {
            group_memberships: SharedGroupMemberships::default(),
            local_members: Arc::new(HashSet::new()),
            hosted_mailboxes: Arc::new(HashSet::new()),
        }
    }
}

/// Early routing hint derived from the shallow public delivery-wire header.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeliveryTargetHint {
    /// Group-scoped traffic that should only be admitted when the group is
    /// currently active locally.
    GroupBroadcast {
        group_id: GroupId,
        delivery_message_id: MessageId,
    },
    /// Recipient-scoped traffic owned by reliable delivery.
    ///
    /// `delivery_message_id` is `None` for mailbox batch traffic, which is
    /// still recipient-scoped but does not correspond to one single
    /// delivery-domain message id.
    ReliableRecipient {
        recipient: MemberIdentity,
        delivery_message_id: Option<MessageId>,
    },
    /// Control traffic that is only relevant to the original sender of one
    /// earlier delivery-domain message.
    OriginalSender {
        original_sender: MemberIdentity,
        delivery_message_id: MessageId,
    },
    /// Relay-mailbox management traffic targeting a mailbox hosted locally.
    HostedMailbox { recipient: MemberIdentity },
}

impl DeliveryTargetHint {
    /// Return the delivery-domain message id when this target corresponds to
    /// one concrete message.
    #[must_use]
    pub fn delivery_message_id(&self) -> Option<MessageId> {
        match self {
            Self::GroupBroadcast {
                delivery_message_id,
                ..
            }
            | Self::OriginalSender {
                delivery_message_id,
                ..
            } => Some(*delivery_message_id),
            Self::ReliableRecipient {
                delivery_message_id,
                ..
            } => *delivery_message_id,
            Self::HostedMailbox { .. } => None,
        }
    }
}

/// Delivery-layer metadata after shallow classification and optional sender
/// verification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InboundDeliveryMeta<R> {
    /// Transport-facing origin metadata preserved from route transport.
    pub transport: InboundTransportMeta<R>,
    /// Cheap target classification derived before full semantic handling.
    pub target: DeliveryTargetHint,
    /// Delivery-domain message id when the routed frame corresponds to exactly
    /// one message.
    pub delivery_message_id: Option<MessageId>,
    /// Verified delivery-layer sender identity when a real signature verifier
    /// is available. The current ingress slice leaves this unset.
    pub verified_sender: Option<RouteEndpoint>,
}

/// Delivery-ingress adapter that owns shallow classification, full
/// delivery-wire protobuf decode, and semantic demux.
///
/// Message-level signature verification is intentionally left as a follow-up
/// once the codebase has a concrete verifier implementation to call here.
#[derive(ComponentDefinition)]
pub struct DeliveryIngressComponent {
    ctx: ComponentContext<Self>,
    /// Upstream indication-only transport stream.
    transport_inbound_port: RequiredPort<TransportInboundPort>,
    /// Downstream semantic stream for group-scoped traffic.
    group_broadcast_inbound_port: ProvidedPort<TransportGroupBroadcastInboundPort>,
    /// Downstream semantic stream for recipient-scoped traffic and delivery
    /// control traffic.
    reliable_delivery_inbound_port: ProvidedPort<TransportReliableDeliveryInboundPort>,
    /// Shared local-interest configuration consulted during shallow
    /// delivery-wire classification.
    interest: DeliveryInterestConfig,
}

impl DeliveryIngressComponent {
    /// Creates one new delivery-ingress adapter with the current local-interest
    /// snapshot handles.
    #[must_use]
    pub fn new(interest: DeliveryInterestConfig) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            transport_inbound_port: RequiredPort::uninitialised(),
            group_broadcast_inbound_port: ProvidedPort::uninitialised(),
            reliable_delivery_inbound_port: ProvidedPort::uninitialised(),
            interest,
        }
    }

    fn handle_transport_inbound(
        &mut self,
        mut inbound: RouteTransportInboundDeliver<TransportRouteKey>,
    ) -> HandlerResult {
        let group_memberships = self.interest.group_memberships.snapshot();
        let route = inbound.transport.route;
        // `Ok(None)` means the payload was syntactically valid enough to
        // classify, but the shallow public header showed it is irrelevant to
        // the current local-interest snapshot.
        let decoded_frame = decode_endpoint_frame_if_delivery_relevant(
            &mut inbound.payload,
            DeliveryInterestView {
                group_memberships: group_memberships.as_ref(),
                local_members: self.interest.local_members.as_ref(),
                hosted_mailboxes: self.interest.hosted_mailboxes.as_ref(),
            },
        )
        .with_whatever_benign(|_| {
            format!("Delivery ingress dropped one malformed inbound payload via {route:?}")
        })?;
        match decoded_frame {
            Some(DecodedDeliveryFrame::GroupBroadcast { target, frame }) => {
                let meta = InboundDeliveryMeta {
                    transport: inbound.transport,
                    delivery_message_id: target.delivery_message_id(),
                    target,
                    verified_sender: None,
                };
                debug!(
                    self.log(),
                    "Delivery ingress admitted one group-broadcast frame via {:?}", route
                );
                self.group_broadcast_inbound_port
                    .trigger(GroupBroadcastInboundDeliver { meta, frame });
            }
            Some(DecodedDeliveryFrame::ReliableDelivery { target, frame }) => {
                let meta = InboundDeliveryMeta {
                    transport: inbound.transport,
                    delivery_message_id: target.delivery_message_id(),
                    target,
                    verified_sender: None,
                };
                debug!(
                    self.log(),
                    "Delivery ingress admitted one reliable-delivery frame via {:?}", route
                );
                self.reliable_delivery_inbound_port
                    .trigger(ReliableDeliveryInboundDeliver { meta, frame });
            }
            None => {
                debug!(
                    self.log(),
                    "Delivery ingress dropped one inbound payload via {:?} because it is not locally relevant",
                    route
                );
            }
        }
        Handled::OK
    }
}

ignore_lifecycle!(DeliveryIngressComponent);
ignore_requests!(TransportGroupBroadcastInboundPort, DeliveryIngressComponent);
ignore_requests!(
    TransportReliableDeliveryInboundPort,
    DeliveryIngressComponent
);
impl Require<TransportInboundPort> for DeliveryIngressComponent {
    fn handle(
        &mut self,
        indication: RouteTransportInboundDeliver<TransportRouteKey>,
    ) -> HandlerResult {
        self.handle_transport_inbound(indication)
    }
}
impl Actor for DeliveryIngressComponent {
    type Message = Never;

    fn receive_local(&mut self, _msg: Self::Message) -> HandlerResult {
        unreachable!("Message type cannot be instantiated");
    }
}

type TransportInboundPort = RouteTransportPort<TransportRouteKey>;
type TransportGroupBroadcastInboundPort = GroupBroadcastInboundPort<TransportRouteKey>;
type TransportReliableDeliveryInboundPort = ReliableDeliveryInboundPort<TransportRouteKey>;

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use flotsync_core::{
        member::{IdentifierBuf, IdentifierLike},
        membership::GroupMemberships,
    };
    use flotsync_io::{
        prelude::IoPayload,
        test_support::{build_test_kompact_system, kill_component, start_component},
    };
    use flotsync_messages::{
        buffa::{Message, MessageField},
        delivery as proto,
        discovery as discovery_proto,
        endpoint as endpoint_proto,
    };
    use flotsync_utils::kompact_testing::{PortTestingExt, PortTestingRefExt};
    use std::time::Duration;
    use uuid::Uuid;

    const WAIT_TIMEOUT: Duration = Duration::from_secs(3);

    #[test]
    fn ingress_routes_active_group_frame_to_group_broadcast_port() {
        let system = build_test_kompact_system();
        let active_group = GroupId(Uuid::from_u128(1));
        let route = test_route();

        let transport = system.create(TransportInboundPort::tester_component_sidecar);
        let transport_ref = transport.actor_ref();
        let ingress = system.create(|| {
            DeliveryIngressComponent::new(DeliveryInterestConfig {
                group_memberships: SharedGroupMemberships::new(GroupMemberships::from_groups([(
                    active_group,
                    flotsync_core::membership::GroupMembers::from_ordered_members([member(&[
                        "probe",
                    ])])
                    .expect("probe group members should build"),
                )])),
                local_members: Arc::new(HashSet::new()),
                hosted_mailboxes: Arc::new(HashSet::new()),
            })
        });
        let group_probe =
            system.create(TransportGroupBroadcastInboundPort::tester_component_sidecar);
        let group_probe_ref = group_probe.actor_ref();
        let reliable_probe =
            system.create(TransportReliableDeliveryInboundPort::tester_component_sidecar);
        let reliable_probe_ref = reliable_probe.actor_ref();

        let _transport_to_ingress =
            biconnect_components::<TransportInboundPort, _, _>(&transport, &ingress)
                .expect("transport probe must connect to ingress");
        let _ingress_to_group = biconnect_components::<TransportGroupBroadcastInboundPort, _, _>(
            &ingress,
            &group_probe,
        )
        .expect("ingress must connect to the group probe");
        let _ingress_to_reliable =
            biconnect_components::<TransportReliableDeliveryInboundPort, _, _>(
                &ingress,
                &reliable_probe,
            )
            .expect("ingress must connect to the reliable probe");

        start_component(&system, &transport);
        start_component(&system, &ingress);
        start_component(&system, &group_probe);
        start_component(&system, &reliable_probe);

        let group_event = group_probe_ref.observe_indication(|_| true);
        let reliable_absence =
            reliable_probe_ref.fail_if_indication_observed(Duration::from_millis(50), |_| true);
        transport_ref.inject_indication(RouteTransportInboundDeliver {
            payload: encode_group_endpoint_frame(active_group, MessageId(Uuid::from_u128(2))),
            transport: InboundTransportMeta {
                route,
                remote_addr: Some("127.0.0.1:30101".parse().expect("test remote address")),
            },
        });

        let event = group_event
            .wait_timeout(WAIT_TIMEOUT)
            .expect("group-broadcast ingress event")
            .expect("group-broadcast probe should stay live");
        assert_eq!(
            &event.indication().meta.target,
            &DeliveryTargetHint::GroupBroadcast {
                group_id: active_group,
                delivery_message_id: MessageId(Uuid::from_u128(2)),
            }
        );
        reliable_absence
            .wait_timeout(WAIT_TIMEOUT)
            .expect("reliable-delivery absence check should complete")
            .expect("reliable-delivery probe should stay live")
            .expect("reliable-delivery ingress should stay silent");

        kill_component(&system, reliable_probe);
        kill_component(&system, group_probe);
        kill_component(&system, ingress);
        kill_component(&system, transport);
    }

    #[test]
    fn ingress_routes_local_recipient_ack_to_reliable_delivery_port() {
        let system = build_test_kompact_system();
        let original_sender = member(&["alice"]);
        let route = test_route();

        let transport = system.create(TransportInboundPort::tester_component_sidecar);
        let transport_ref = transport.actor_ref();
        let ingress = system.create(|| {
            DeliveryIngressComponent::new(DeliveryInterestConfig {
                group_memberships: SharedGroupMemberships::default(),
                local_members: Arc::new([original_sender.clone()].into_iter().collect()),
                hosted_mailboxes: Arc::new(HashSet::new()),
            })
        });
        let group_probe =
            system.create(TransportGroupBroadcastInboundPort::tester_component_sidecar);
        let group_probe_ref = group_probe.actor_ref();
        let reliable_probe =
            system.create(TransportReliableDeliveryInboundPort::tester_component_sidecar);
        let reliable_probe_ref = reliable_probe.actor_ref();

        let _transport_to_ingress =
            biconnect_components::<TransportInboundPort, _, _>(&transport, &ingress)
                .expect("transport probe must connect to ingress");
        let _ingress_to_group = biconnect_components::<TransportGroupBroadcastInboundPort, _, _>(
            &ingress,
            &group_probe,
        )
        .expect("ingress must connect to the group probe");
        let _ingress_to_reliable =
            biconnect_components::<TransportReliableDeliveryInboundPort, _, _>(
                &ingress,
                &reliable_probe,
            )
            .expect("ingress must connect to the reliable probe");

        start_component(&system, &transport);
        start_component(&system, &ingress);
        start_component(&system, &group_probe);
        start_component(&system, &reliable_probe);

        let reliable_event = reliable_probe_ref.observe_indication(|_| true);
        let group_absence =
            group_probe_ref.fail_if_indication_observed(Duration::from_millis(50), |_| true);
        transport_ref.inject_indication(RouteTransportInboundDeliver {
            payload: encode_recipient_ack_endpoint_frame(
                MessageId(Uuid::from_u128(3)),
                &original_sender,
                &member(&["bob"]),
            ),
            transport: InboundTransportMeta {
                route,
                remote_addr: Some("127.0.0.1:30102".parse().expect("test remote address")),
            },
        });

        let event = reliable_event
            .wait_timeout(WAIT_TIMEOUT)
            .expect("reliable-delivery ingress event")
            .expect("reliable-delivery probe should stay live");
        assert_eq!(
            &event.indication().meta.target,
            &DeliveryTargetHint::OriginalSender {
                original_sender,
                delivery_message_id: MessageId(Uuid::from_u128(3)),
            }
        );
        group_absence
            .wait_timeout(WAIT_TIMEOUT)
            .expect("group-broadcast absence check should complete")
            .expect("group-broadcast probe should stay live")
            .expect("group-broadcast ingress should stay silent");

        kill_component(&system, reliable_probe);
        kill_component(&system, group_probe);
        kill_component(&system, ingress);
        kill_component(&system, transport);
    }

    fn test_route() -> TransportRouteKey {
        TransportRouteKey::Udp(flotsync_routes::UdpRouteKey {
            remote_addr: "127.0.0.1:40100".parse().expect("test remote address"),
            scope: flotsync_routes::DatagramRouteScope::Unicast,
            local_bind: Some("127.0.0.1:40200".parse().expect("test local bind")),
        })
    }

    fn member(segments: &[&str]) -> MemberIdentity {
        let mut buffer = IdentifierBuf::new();
        for segment in segments {
            buffer
                .push_checked((*segment).to_owned())
                .expect("test identifier segment must be valid");
        }
        buffer.into_identifier()
    }

    fn proto_identifier(member: &MemberIdentity) -> discovery_proto::Identifier {
        let segments = member
            .segments()
            .map(|segment| segment.as_ref().to_owned())
            .collect();
        discovery_proto::Identifier {
            segments,
            ..discovery_proto::Identifier::default()
        }
    }

    fn encode_group_endpoint_frame(group_id: GroupId, delivery_message_id: MessageId) -> IoPayload {
        let header = proto::GroupEnvelopeHeader {
            group_id: group_id.0.as_bytes().to_vec(),
            sender: MessageField::some(proto_identifier(&member(&["alice"]))),
            message_id: delivery_message_id.0.as_bytes().to_vec(),
            ..proto::GroupEnvelopeHeader::default()
        };
        let envelope = proto::GroupEnvelopeWire {
            public_header: MessageField::some(header),
            sealed_payload: MessageField::some(proto::SealedPSKPayload {
                ciphertext: Bytes::from_static(b"ciphertext"),
                signature: vec![0; 64],
                ..proto::SealedPSKPayload::default()
            }),
            ..proto::GroupEnvelopeWire::default()
        };
        let frame = proto::GroupBroadcastFrame {
            body: Some(proto::group_broadcast_frame::Body::Envelope(Box::new(
                envelope,
            ))),
            ..proto::GroupBroadcastFrame::default()
        };
        let boundary = endpoint_proto::EndpointFrame {
            boundary: Some(endpoint_proto::endpoint_frame::Boundary::GroupBroadcast(
                Box::new(frame),
            )),
            ..endpoint_proto::EndpointFrame::default()
        };
        encode_endpoint_frame(&boundary)
    }

    fn encode_recipient_ack_endpoint_frame(
        delivery_message_id: MessageId,
        original_sender: &MemberIdentity,
        recipient: &MemberIdentity,
    ) -> IoPayload {
        let header = proto::RecipientAckHeader {
            message_id: delivery_message_id.0.as_bytes().to_vec(),
            original_sender: MessageField::some(proto_identifier(original_sender)),
            recipient: MessageField::some(proto_identifier(recipient)),
            ..proto::RecipientAckHeader::default()
        };
        let ack = proto::RecipientAckWire {
            public_header: MessageField::some(header),
            ..proto::RecipientAckWire::default()
        };
        let frame = proto::ReliableDeliveryFrame {
            body: Some(proto::reliable_delivery_frame::Body::RecipientAck(
                Box::new(ack),
            )),
            ..proto::ReliableDeliveryFrame::default()
        };
        let boundary = endpoint_proto::EndpointFrame {
            boundary: Some(endpoint_proto::endpoint_frame::Boundary::ReliableDelivery(
                Box::new(frame),
            )),
            ..endpoint_proto::EndpointFrame::default()
        };
        encode_endpoint_frame(&boundary)
    }

    fn encode_endpoint_frame(boundary: &endpoint_proto::EndpointFrame) -> IoPayload {
        IoPayload::from(boundary.encode_to_bytes())
    }
}
