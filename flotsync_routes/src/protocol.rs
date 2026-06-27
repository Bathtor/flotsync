//! Route-establishment introduction protocol helpers.

use flotsync_core::GroupId;
use flotsync_discovery::protocol::{DiscoveryProtocolError, discovery_protocol_error};
use flotsync_messages::{
    buffa::{self, Message, MessageView},
    discovery::{
        DiscoveryFrame,
        Introduction,
        IntroductionClaimPayload,
        IntroductionClaimPayloadView,
        IntroductionRequest,
        discovery_frame,
    },
    endpoint::{EndpointFrame, EndpointFrameView, endpoint_frame},
    proto::{DecodeProto, DecodeProtoView},
    wire::{UUID_BYTE_LENGTH, fixed_bytes_field, group_id_from_wire_bytes},
};
use snafu::prelude::*;
use std::collections::HashSet;
use uuid::Uuid;

pub use flotsync_discovery::protocol::DiscoveryRoute;

/// A decoded signed-claim payload before signature verification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedIntroductionClaimPayload {
    /// Running process id for the peer instance making this claim.
    pub instance_id: Uuid,
    /// Receiver-generated freshness challenge echoed by this claim.
    pub request_nonce: Vec<u8>,
    /// Route endpoint covered by this claim.
    pub route: DiscoveryRoute,
    /// Replication group ids covered by this claim.
    pub group_ids: HashSet<GroupId>,
}

impl DecodeProto for DecodedIntroductionClaimPayload {
    type Error = DiscoveryProtocolError;
    type Proto = IntroductionClaimPayload;

    fn decode_proto(mut payload: Self::Proto) -> Result<Self, Self::Error> {
        let instance_id = uuid_from_wire(
            &payload.instance_uuid,
            "IntroductionClaimPayload.instance_uuid",
        )?;
        ensure!(
            !payload.request_nonce.is_empty(),
            discovery_protocol_error::EmptyBytesSnafu {
                field: "IntroductionClaimPayload.request_nonce"
            }
        );
        let route = payload
            .route
            .take()
            .context(discovery_protocol_error::MissingFieldSnafu {
                message: "IntroductionClaimPayload",
                field: "route",
            })?;
        let route = DiscoveryRoute::decode_proto(route)?;
        let group_ids = decode_claim_group_ids(
            payload.group_ids.iter().map(Vec::as_slice),
            payload.group_ids.len(),
        )?;
        Ok(Self {
            instance_id,
            request_nonce: payload.request_nonce,
            route,
            group_ids,
        })
    }
}

impl DecodeProtoView for DecodedIntroductionClaimPayload {
    type Error = DiscoveryProtocolError;
    type ProtoView<'a> = IntroductionClaimPayloadView<'a>;

    fn decode_proto_view(payload: &Self::ProtoView<'_>) -> Result<Self, Self::Error> {
        let instance_id = uuid_from_wire(
            payload.instance_uuid,
            "IntroductionClaimPayload.instance_uuid",
        )?;
        ensure!(
            !payload.request_nonce.is_empty(),
            discovery_protocol_error::EmptyBytesSnafu {
                field: "IntroductionClaimPayload.request_nonce"
            }
        );
        let route =
            payload
                .route
                .as_option()
                .context(discovery_protocol_error::MissingFieldSnafu {
                    message: "IntroductionClaimPayload",
                    field: "route",
                })?;
        let route = DiscoveryRoute::decode_proto_view(route)?;
        let group_ids =
            decode_claim_group_ids(payload.group_ids.iter().copied(), payload.group_ids.len())?;
        Ok(Self {
            instance_id,
            request_nonce: payload.request_nonce.to_vec(),
            route,
            group_ids,
        })
    }
}

/// Decode and validate the group ids in one signed claim payload.
///
/// # Errors
///
/// Returns [`DiscoveryProtocolError`] if the bytes are not a valid claim payload or if the group-id
/// field is empty, malformed, or contains duplicates.
pub fn decode_introduction_claim_group_ids(
    bytes: &[u8],
) -> Result<HashSet<GroupId>, DiscoveryProtocolError> {
    let payload = IntroductionClaimPayloadView::decode_view(bytes)
        .context(discovery_protocol_error::DecodeSnafu)?;
    decode_claim_group_ids(payload.group_ids.iter().copied(), payload.group_ids.len())
}

/// Encode one discovery introduction request into the shared endpoint envelope.
#[must_use]
pub fn introduction_request_endpoint_frame(request_nonce: Vec<u8>) -> EndpointFrame {
    let discovery = DiscoveryFrame {
        body: Some(discovery_frame::Body::IntroductionRequest(Box::new(
            IntroductionRequest {
                request_nonce,
                ..IntroductionRequest::default()
            },
        ))),
        ..DiscoveryFrame::default()
    };
    discovery_endpoint_frame(discovery)
}

/// Encode one discovery introduction into the shared endpoint envelope.
#[must_use]
pub fn introduction_endpoint_frame(introduction: Introduction) -> EndpointFrame {
    let discovery = DiscoveryFrame {
        body: Some(discovery_frame::Body::Introduction(Box::new(introduction))),
        ..DiscoveryFrame::default()
    };
    discovery_endpoint_frame(discovery)
}

/// Decode one discovery frame from a shared endpoint envelope.
///
/// Returns `Ok(None)` for well-formed non-discovery endpoint frames so callers sharing the runtime
/// endpoint with delivery can ignore traffic owned by another semantic layer.
///
/// # Errors
///
/// Returns [`DiscoveryProtocolError`] when the endpoint frame is malformed.
pub fn decode_endpoint_discovery_frame(
    bytes: &[u8],
) -> Result<Option<DiscoveryFrame>, DiscoveryProtocolError> {
    let frame =
        EndpointFrameView::decode_view(bytes).context(discovery_protocol_error::DecodeSnafu)?;
    let boundary =
        frame
            .boundary
            .as_ref()
            .context(discovery_protocol_error::MissingOneofSnafu {
                name: "EndpointFrame.boundary",
            })?;
    match boundary {
        endpoint_frame::BoundaryView::Discovery(discovery) => {
            Ok(Some(discovery.to_owned_message()))
        }
        endpoint_frame::BoundaryView::GroupBroadcast(_)
        | endpoint_frame::BoundaryView::ReliableDelivery(_) => Ok(None),
    }
}

/// Decode one discovery frame from a shared endpoint envelope without requiring contiguous input.
///
/// Returns `Ok(None)` for well-formed non-discovery endpoint frames so callers sharing the runtime
/// endpoint with delivery can ignore traffic owned by another semantic layer.
///
/// # Errors
///
/// Returns [`DiscoveryProtocolError`] when the endpoint frame is malformed.
pub fn decode_endpoint_discovery_frame_from_buf(
    buf: &mut impl buffa::bytes::Buf,
) -> Result<Option<DiscoveryFrame>, DiscoveryProtocolError> {
    let frame = EndpointFrame::decode(buf).context(discovery_protocol_error::DecodeSnafu)?;
    let boundary = frame
        .boundary
        .context(discovery_protocol_error::MissingOneofSnafu {
            name: "EndpointFrame.boundary",
        })?;
    match boundary {
        endpoint_frame::Boundary::Discovery(discovery) => Ok(Some(*discovery)),
        endpoint_frame::Boundary::GroupBroadcast(_)
        | endpoint_frame::Boundary::ReliableDelivery(_) => Ok(None),
    }
}

fn discovery_endpoint_frame(discovery: DiscoveryFrame) -> EndpointFrame {
    EndpointFrame {
        boundary: Some(endpoint_frame::Boundary::Discovery(Box::new(discovery))),
        ..EndpointFrame::default()
    }
}

/// Decode one fixed-width UUID byte field from a discovery protobuf value.
fn uuid_from_wire(bytes: &[u8], field: &'static str) -> Result<Uuid, DiscoveryProtocolError> {
    let bytes = fixed_bytes_field::<UUID_BYTE_LENGTH>(field, bytes).map_err(|_| {
        DiscoveryProtocolError::InvalidByteLength {
            field,
            expected: UUID_BYTE_LENGTH,
            actual: bytes.len(),
        }
    })?;
    Ok(Uuid::from_bytes(bytes))
}

fn group_id_from_wire(
    bytes: &[u8],
    field: &'static str,
) -> Result<GroupId, DiscoveryProtocolError> {
    group_id_from_wire_bytes(bytes, field).map_err(|_| DiscoveryProtocolError::InvalidByteLength {
        field,
        expected: UUID_BYTE_LENGTH,
        actual: bytes.len(),
    })
}

/// Decode and validate the repeated group-id field shared by owned and view payload decoders.
fn decode_claim_group_ids<'a>(
    raw_group_ids: impl IntoIterator<Item = &'a [u8]>,
    group_count: usize,
) -> Result<HashSet<GroupId>, DiscoveryProtocolError> {
    ensure!(
        group_count > 0,
        discovery_protocol_error::EmptyBytesSnafu {
            field: "IntroductionClaimPayload.group_ids"
        }
    );
    let mut group_ids = HashSet::with_capacity(group_count);
    for raw_group_id in raw_group_ids {
        let group_id = group_id_from_wire(raw_group_id, "IntroductionClaimPayload.group_ids")?;
        ensure!(
            group_ids.insert(group_id),
            discovery_protocol_error::DuplicateClaimGroupSnafu { group_id }
        );
    }
    Ok(group_ids)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flotsync_messages::{
        buffa::MessageField,
        discovery::socket_address,
        proto::{DecodeProto, DecodeProtoView, EncodeProto},
        wire::{group_id_to_wire_bytes, uuid_to_wire_bytes},
    };
    use std::net::SocketAddr;

    #[test]
    fn decodes_claim_payload_with_multiple_groups() {
        let route = DiscoveryRoute::Udp(SocketAddr::from(([127, 0, 0, 1], 52156)));
        let first_group = GroupId(Uuid::from_u128(0x1111));
        let second_group = GroupId(Uuid::from_u128(0x2222));
        let payload = IntroductionClaimPayload {
            instance_uuid: uuid_to_wire_bytes(Uuid::from_u128(0x1234)),
            request_nonce: vec![0x42; 16],
            route: MessageField::some(route.encode_proto()),
            group_ids: vec![
                group_id_to_wire_bytes(first_group),
                group_id_to_wire_bytes(second_group),
            ],
            ..IntroductionClaimPayload::default()
        };

        let payload_bytes = payload.encode_to_vec();
        let decoded = DecodedIntroductionClaimPayload::decode_proto_view_from_slice(&payload_bytes)
            .expect("valid claim should decode");
        let payload_view = IntroductionClaimPayloadView::decode_view(&payload_bytes)
            .expect("claim payload view should decode");

        assert_eq!(decoded.instance_id, Uuid::from_u128(0x1234));
        assert_eq!(decoded.request_nonce, vec![0x42; 16]);
        assert_eq!(decoded.route, route);
        assert_eq!(
            decoded.group_ids,
            HashSet::from([first_group, second_group])
        );
        assert_eq!(
            DecodedIntroductionClaimPayload::decode_proto_view(&payload_view)
                .expect("claim payload view should convert"),
            decoded
        );
    }

    #[test]
    fn rejects_claim_payload_with_duplicate_groups() {
        let group = GroupId(Uuid::from_u128(0x1111));
        let payload = IntroductionClaimPayload {
            instance_uuid: uuid_to_wire_bytes(Uuid::from_u128(0x1234)),
            request_nonce: vec![0x42; 16],
            route: MessageField::some(
                DiscoveryRoute::Udp(SocketAddr::from(([127, 0, 0, 1], 52156))).encode_proto(),
            ),
            group_ids: vec![group_id_to_wire_bytes(group), group_id_to_wire_bytes(group)],
            ..IntroductionClaimPayload::default()
        };

        let result =
            DecodedIntroductionClaimPayload::decode_proto_view_from_slice(&payload.encode_to_vec());

        assert!(matches!(
            result,
            Err(DiscoveryProtocolError::DuplicateClaimGroup { group_id }) if group_id == group
        ));
    }

    #[test]
    fn endpoint_decoder_rejects_missing_boundary() {
        let frame = EndpointFrame::default();

        let result = decode_endpoint_discovery_frame(&frame.encode_to_vec());

        assert!(matches!(
            result,
            Err(DiscoveryProtocolError::MissingOneof {
                name: "EndpointFrame.boundary"
            })
        ));
    }

    #[test]
    fn endpoint_decoder_ignores_non_discovery_frames() {
        let frame = EndpointFrame {
            boundary: Some(endpoint_frame::Boundary::GroupBroadcast(Box::default())),
            ..EndpointFrame::default()
        };

        let decoded = decode_endpoint_discovery_frame(&frame.encode_to_vec())
            .expect("endpoint frame should decode");

        assert!(decoded.is_none());
    }

    #[test]
    fn endpoint_decoder_returns_discovery_frame() {
        let request = introduction_request_endpoint_frame(vec![0x42; 16]);

        let decoded = decode_endpoint_discovery_frame(&request.encode_to_vec())
            .expect("endpoint frame should decode")
            .expect("discovery frame should be returned");

        assert!(matches!(
            decoded.body,
            Some(discovery_frame::Body::IntroductionRequest(request))
                if request.request_nonce == vec![0x42; 16]
        ));
    }

    #[test]
    fn route_conversion_accepts_udp_routes_from_discovery_protocol() {
        let route = DiscoveryRoute::Udp(SocketAddr::from(([127, 0, 0, 1], 52156))).encode_proto();

        let decoded = DiscoveryRoute::decode_proto(route).expect("UDP route should decode");

        assert_eq!(
            decoded,
            DiscoveryRoute::Udp(SocketAddr::from(([127, 0, 0, 1], 52156)))
        );
    }

    #[test]
    fn route_conversion_rejects_tcp_routes_from_discovery_protocol() {
        let mut route =
            DiscoveryRoute::Udp(SocketAddr::from(([127, 0, 0, 1], 52156))).encode_proto();
        route.protocol =
            flotsync_messages::buffa::EnumValue::from(socket_address::Protocol::PROTOCOL_TCP);

        let result = DiscoveryRoute::decode_proto(route);

        assert!(matches!(
            result,
            Err(DiscoveryProtocolError::UnsupportedRouteProtocol { .. })
        ));
    }
}
