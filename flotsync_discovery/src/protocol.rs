//! Shared route establishment protocol helpers.

use flotsync_core::GroupId;
use flotsync_messages::{
    buffa::{self, DecodeError, EnumValue, Message, MessageField, MessageView},
    discovery::{
        DiscoveryFrame,
        IPAddress,
        IPAddressView,
        Introduction,
        IntroductionClaimPayload,
        IntroductionClaimPayloadView,
        IntroductionRequest,
        Peer,
        SocketAddress,
        SocketAddressView,
        discovery_frame,
        ip_address,
        socket_address,
    },
    endpoint::{EndpointFrame, EndpointFrameView, endpoint_frame},
    wire::{UUID_BYTE_LENGTH, fixed_bytes_field, group_id_from_wire_bytes},
};
use snafu::prelude::*;
use std::{
    collections::HashSet,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
};
use uuid::Uuid;

/// Number of bytes in a discovery peer-instance id.
pub const INSTANCE_ID_LENGTH: usize = UUID_BYTE_LENGTH;

/// One route advertised or verified by the route establishment protocol.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DiscoveryRoute {
    /// A UDP unicast endpoint.
    Udp(SocketAddr),
}

impl DiscoveryRoute {
    /// Return the UDP socket address when this is a UDP route.
    #[must_use]
    pub const fn udp_addr(self) -> SocketAddr {
        match self {
            Self::Udp(address) => address,
        }
    }

    /// Convert this route into the protobuf wire representation.
    #[must_use]
    pub fn to_wire_format(self) -> SocketAddress {
        match self {
            Self::Udp(address) => udp_socket_address_to_wire_format(address),
        }
    }
}

/// A decoded plaintext peer announcement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedPeer {
    /// Running process id for the announcing peer instance.
    pub instance_id: Uuid,
    /// Reachability endpoints advertised by this peer instance.
    pub listening_on: Vec<DiscoveryRoute>,
}

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

/// Protocol-level decode and validation failures.
#[derive(Debug, Snafu)]
#[non_exhaustive]
pub enum DiscoveryProtocolError {
    /// A protobuf message could not be decoded.
    #[snafu(display("Failed to decode discovery protobuf message: {source}"))]
    Decode { source: DecodeError },
    /// A required protobuf message field was absent.
    #[snafu(display("Protobuf message '{message}' is missing required field '{field}'."))]
    MissingField {
        message: &'static str,
        field: &'static str,
    },
    /// A required protobuf oneof branch was absent.
    #[snafu(display("Protobuf oneof '{name}' has no selected value."))]
    MissingOneof { name: &'static str },
    /// A fixed-width byte field had the wrong size.
    #[snafu(display(
        "Field '{field}' had {actual} byte(s), but the protocol requires exactly {expected}."
    ))]
    InvalidByteLength {
        field: &'static str,
        expected: usize,
        actual: usize,
    },
    /// A byte field that must not be empty was empty.
    #[snafu(display("Field '{field}' must not be empty."))]
    EmptyBytes { field: &'static str },
    /// A route used an unsupported transport protocol.
    #[snafu(display("Field '{field}' used unsupported route protocol value {value}."))]
    UnsupportedRouteProtocol { field: &'static str, value: i32 },
    /// A route used an invalid UDP/TCP port.
    #[snafu(display("Field '{field}' used invalid socket port {port}."))]
    InvalidRoutePort { field: &'static str, port: u32 },
    /// A peer announcement did not advertise any route.
    #[snafu(display("Peer announcement did not contain any listening routes."))]
    EmptyPeerRoutes,
    /// A signed claim repeated the same group id.
    #[snafu(display("Introduction claim repeated group id {group_id}."))]
    DuplicateClaimGroup { group_id: GroupId },
}

/// Decode and validate one plaintext peer announcement from bytes.
///
/// # Errors
///
/// Returns [`DiscoveryProtocolError`] if the bytes are not a valid `Peer` or if any required
/// protocol field is malformed.
pub fn decode_peer_bytes(bytes: &[u8]) -> Result<DecodedPeer, DiscoveryProtocolError> {
    let peer = Peer::decode_from_slice(bytes).context(DecodeSnafu)?;
    decode_peer(&peer)
}

/// Decode and validate one plaintext peer announcement.
///
/// # Errors
///
/// Returns [`DiscoveryProtocolError`] if any required protocol field is malformed.
pub fn decode_peer(peer: &Peer) -> Result<DecodedPeer, DiscoveryProtocolError> {
    ensure!(!peer.listening_on.is_empty(), EmptyPeerRoutesSnafu);
    let instance_id = uuid_from_wire(&peer.instance_uuid, "Peer.instance_uuid")?;
    let mut listening_on = Vec::with_capacity(peer.listening_on.len());
    for route in &peer.listening_on {
        let route = discovery_route_from_wire(route, "Peer.listening_on")?;
        listening_on.push(route);
    }
    Ok(DecodedPeer {
        instance_id,
        listening_on,
    })
}

/// Decode and validate one signed claim payload from its exact transmitted bytes.
///
/// # Errors
///
/// Returns [`DiscoveryProtocolError`] if the bytes are not a valid claim payload or if any
/// required protocol field is malformed.
pub fn decode_introduction_claim_payload(
    bytes: &[u8],
) -> Result<DecodedIntroductionClaimPayload, DiscoveryProtocolError> {
    let payload = IntroductionClaimPayload::decode_from_slice(bytes).context(DecodeSnafu)?;
    let instance_id = uuid_from_wire(
        &payload.instance_uuid,
        "IntroductionClaimPayload.instance_uuid",
    )?;
    ensure!(
        !payload.request_nonce.is_empty(),
        EmptyBytesSnafu {
            field: "IntroductionClaimPayload.request_nonce"
        }
    );
    let route = payload.route.as_option().context(MissingFieldSnafu {
        message: "IntroductionClaimPayload",
        field: "route",
    })?;
    let route = discovery_route_from_wire(route, "IntroductionClaimPayload.route")?;
    let group_ids = decode_claim_group_ids(
        payload.group_ids.iter().map(Vec::as_slice),
        payload.group_ids.len(),
    )?;
    Ok(DecodedIntroductionClaimPayload {
        instance_id,
        request_nonce: payload.request_nonce,
        route,
        group_ids,
    })
}

/// Decode one signed claim payload view from its exact transmitted bytes.
///
/// # Errors
///
/// Returns [`DiscoveryProtocolError`] if the bytes are not a valid claim payload.
pub fn decode_introduction_claim_payload_view(
    bytes: &[u8],
) -> Result<IntroductionClaimPayloadView<'_>, DiscoveryProtocolError> {
    IntroductionClaimPayloadView::decode_view(bytes).context(DecodeSnafu)
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
    let payload = decode_introduction_claim_payload_view(bytes)?;
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
    let frame = EndpointFrameView::decode_view(bytes).context(DecodeSnafu)?;
    let boundary = frame.boundary.as_ref().context(MissingOneofSnafu {
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
    let frame = EndpointFrame::decode(buf).context(DecodeSnafu)?;
    let boundary = frame.boundary.context(MissingOneofSnafu {
        name: "EndpointFrame.boundary",
    })?;
    match boundary {
        endpoint_frame::Boundary::Discovery(discovery) => Ok(Some(*discovery)),
        endpoint_frame::Boundary::GroupBroadcast(_)
        | endpoint_frame::Boundary::ReliableDelivery(_) => Ok(None),
    }
}

/// Convert a UDP socket address into the discovery protobuf route shape.
#[must_use]
pub fn udp_socket_address_to_wire_format(address: SocketAddr) -> SocketAddress {
    SocketAddress {
        protocol: EnumValue::from(socket_address::Protocol::PROTOCOL_UDP),
        address: MessageField::some(ip_address_to_wire_format(address.ip())),
        port: u32::from(address.port()),
        ..SocketAddress::default()
    }
}

/// Decode and validate one discovery route.
///
/// # Errors
///
/// Returns [`DiscoveryProtocolError`] if the route protocol, address, or port is unsupported.
pub fn discovery_route_from_wire(
    route: &SocketAddress,
    field: &'static str,
) -> Result<DiscoveryRoute, DiscoveryProtocolError> {
    let protocol = route.protocol.as_known().ok_or_else(|| {
        DiscoveryProtocolError::UnsupportedRouteProtocol {
            field,
            value: route.protocol.to_i32(),
        }
    })?;
    match protocol {
        socket_address::Protocol::PROTOCOL_UDP => {
            let address = route.address.as_option().context(MissingFieldSnafu {
                message: "SocketAddress",
                field: "address",
            })?;
            let ip = ip_address_from_wire(address, field)?;
            let port = socket_port_from_wire(route.port, field)?;
            Ok(DiscoveryRoute::Udp(SocketAddr::new(ip, port)))
        }
        socket_address::Protocol::PROTOCOL_UNSPECIFIED | socket_address::Protocol::PROTOCOL_TCP => {
            UnsupportedRouteProtocolSnafu {
                field,
                value: route.protocol.to_i32(),
            }
            .fail()
        }
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
    let bytes = fixed_bytes_field::<INSTANCE_ID_LENGTH>(field, bytes).map_err(|_| {
        DiscoveryProtocolError::InvalidByteLength {
            field,
            expected: INSTANCE_ID_LENGTH,
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
        expected: INSTANCE_ID_LENGTH,
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
        EmptyBytesSnafu {
            field: "IntroductionClaimPayload.group_ids"
        }
    );
    let mut group_ids = HashSet::with_capacity(group_count);
    for raw_group_id in raw_group_ids {
        let group_id = group_id_from_wire(raw_group_id, "IntroductionClaimPayload.group_ids")?;
        ensure!(
            group_ids.insert(group_id),
            DuplicateClaimGroupSnafu { group_id }
        );
    }
    Ok(group_ids)
}

fn socket_port_from_wire(port: u32, field: &'static str) -> Result<u16, DiscoveryProtocolError> {
    if port == 0 || port > u32::from(u16::MAX) {
        return InvalidRoutePortSnafu { field, port }.fail();
    }
    u16::try_from(port).map_err(|_| DiscoveryProtocolError::InvalidRoutePort { field, port })
}

fn ip_address_to_wire_format(address: IpAddr) -> IPAddress {
    match address {
        IpAddr::V4(address) => ipv4_address_to_wire_format(address),
        IpAddr::V6(address) => ipv6_address_to_wire_format(address),
    }
}

fn ipv4_address_to_wire_format(address: Ipv4Addr) -> IPAddress {
    IPAddress {
        address: Some(ip_address::Address::Ipv4Bytes(address.octets().to_vec())),
        ..IPAddress::default()
    }
}

fn ipv6_address_to_wire_format(address: Ipv6Addr) -> IPAddress {
    IPAddress {
        address: Some(ip_address::Address::Ipv6Bytes(address.octets().to_vec())),
        ..IPAddress::default()
    }
}

fn ip_address_from_wire(
    address: &IPAddress,
    field: &'static str,
) -> Result<IpAddr, DiscoveryProtocolError> {
    let address = address.address.as_ref().context(MissingOneofSnafu {
        name: "IPAddress.address",
    })?;
    match address {
        ip_address::Address::Ipv4Bytes(bytes) => {
            let bytes: [u8; 4] = bytes.as_slice().try_into().map_err(|_| {
                DiscoveryProtocolError::InvalidByteLength {
                    field,
                    expected: 4,
                    actual: bytes.len(),
                }
            })?;
            Ok(IpAddr::V4(Ipv4Addr::from(bytes)))
        }
        ip_address::Address::Ipv6Bytes(bytes) => {
            let bytes: [u8; 16] = bytes.as_slice().try_into().map_err(|_| {
                DiscoveryProtocolError::InvalidByteLength {
                    field,
                    expected: 16,
                    actual: bytes.len(),
                }
            })?;
            Ok(IpAddr::V6(Ipv6Addr::from(bytes)))
        }
    }
}

/// Decode and validate a borrowed discovery route view.
pub(crate) fn discovery_route_from_wire_view(
    route: &SocketAddressView<'_>,
    field: &'static str,
) -> Result<DiscoveryRoute, DiscoveryProtocolError> {
    let protocol = route.protocol.as_known().ok_or_else(|| {
        DiscoveryProtocolError::UnsupportedRouteProtocol {
            field,
            value: route.protocol.to_i32(),
        }
    })?;
    match protocol {
        socket_address::Protocol::PROTOCOL_UDP => {
            let address = route.address.as_option().context(MissingFieldSnafu {
                message: "SocketAddress",
                field: "address",
            })?;
            let ip = ip_address_from_wire_view(address, field)?;
            let port = socket_port_from_wire(route.port, field)?;
            Ok(DiscoveryRoute::Udp(SocketAddr::new(ip, port)))
        }
        socket_address::Protocol::PROTOCOL_UNSPECIFIED | socket_address::Protocol::PROTOCOL_TCP => {
            UnsupportedRouteProtocolSnafu {
                field,
                value: route.protocol.to_i32(),
            }
            .fail()
        }
    }
}

/// Decode and validate a borrowed discovery route IP-address view.
fn ip_address_from_wire_view(
    address: &IPAddressView<'_>,
    field: &'static str,
) -> Result<IpAddr, DiscoveryProtocolError> {
    let address = address.address.as_ref().context(MissingOneofSnafu {
        name: "IPAddress.address",
    })?;
    match address {
        ip_address::AddressView::Ipv4Bytes(bytes) => {
            let bytes: [u8; 4] =
                (*bytes)
                    .try_into()
                    .map_err(|_| DiscoveryProtocolError::InvalidByteLength {
                        field,
                        expected: 4,
                        actual: bytes.len(),
                    })?;
            Ok(IpAddr::V4(Ipv4Addr::from(bytes)))
        }
        ip_address::AddressView::Ipv6Bytes(bytes) => {
            let bytes: [u8; 16] =
                (*bytes)
                    .try_into()
                    .map_err(|_| DiscoveryProtocolError::InvalidByteLength {
                        field,
                        expected: 16,
                        actual: bytes.len(),
                    })?;
            Ok(IpAddr::V6(Ipv6Addr::from(bytes)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flotsync_messages::wire::{group_id_to_wire_bytes, uuid_to_wire_bytes};

    #[test]
    fn decodes_valid_peer_with_udp_route() {
        let peer = Peer {
            instance_uuid: uuid_to_wire_bytes(Uuid::from_u128(0x1234)),
            listening_on: vec![udp_socket_address_to_wire_format(SocketAddr::from((
                [127, 0, 0, 1],
                52156,
            )))],
            ..Peer::default()
        };

        let decoded = decode_peer(&peer).expect("valid peer should decode");

        assert_eq!(decoded.instance_id, Uuid::from_u128(0x1234));
        assert_eq!(
            decoded.listening_on,
            vec![DiscoveryRoute::Udp(SocketAddr::from((
                [127, 0, 0, 1],
                52156
            )))]
        );
    }

    #[test]
    fn rejects_peer_with_malformed_instance_id() {
        let peer = Peer {
            instance_uuid: vec![0x42; 15],
            listening_on: vec![udp_socket_address_to_wire_format(SocketAddr::from((
                [127, 0, 0, 1],
                52156,
            )))],
            ..Peer::default()
        };

        let result = decode_peer(&peer);

        assert!(matches!(
            result,
            Err(DiscoveryProtocolError::InvalidByteLength {
                field: "Peer.instance_uuid",
                ..
            })
        ));
    }

    #[test]
    fn rejects_tcp_peer_route_for_route_establishment_path() {
        let peer = Peer {
            instance_uuid: uuid_to_wire_bytes(Uuid::from_u128(0x1234)),
            listening_on: vec![SocketAddress {
                protocol: EnumValue::from(socket_address::Protocol::PROTOCOL_TCP),
                address: MessageField::some(ipv4_address_to_wire_format(Ipv4Addr::LOCALHOST)),
                port: 52156,
                ..SocketAddress::default()
            }],
            ..Peer::default()
        };

        let result = decode_peer(&peer);

        assert!(matches!(
            result,
            Err(DiscoveryProtocolError::UnsupportedRouteProtocol { .. })
        ));
    }

    #[test]
    fn decodes_claim_payload_with_multiple_groups() {
        let route = DiscoveryRoute::Udp(SocketAddr::from(([127, 0, 0, 1], 52156)));
        let first_group = GroupId(Uuid::from_u128(0x1111));
        let second_group = GroupId(Uuid::from_u128(0x2222));
        let payload = IntroductionClaimPayload {
            instance_uuid: uuid_to_wire_bytes(Uuid::from_u128(0x1234)),
            request_nonce: vec![0x42; 16],
            route: MessageField::some(route.to_wire_format()),
            group_ids: vec![
                group_id_to_wire_bytes(first_group),
                group_id_to_wire_bytes(second_group),
            ],
            ..IntroductionClaimPayload::default()
        };

        let decoded = decode_introduction_claim_payload(&payload.encode_to_vec())
            .expect("valid claim should decode");

        assert_eq!(decoded.instance_id, Uuid::from_u128(0x1234));
        assert_eq!(decoded.request_nonce, vec![0x42; 16]);
        assert_eq!(decoded.route, route);
        assert_eq!(
            decoded.group_ids,
            HashSet::from([first_group, second_group])
        );
    }

    #[test]
    fn rejects_claim_payload_with_duplicate_groups() {
        let group = GroupId(Uuid::from_u128(0x1111));
        let payload = IntroductionClaimPayload {
            instance_uuid: uuid_to_wire_bytes(Uuid::from_u128(0x1234)),
            request_nonce: vec![0x42; 16],
            route: MessageField::some(
                DiscoveryRoute::Udp(SocketAddr::from(([127, 0, 0, 1], 52156))).to_wire_format(),
            ),
            group_ids: vec![group_id_to_wire_bytes(group), group_id_to_wire_bytes(group)],
            ..IntroductionClaimPayload::default()
        };

        let result = decode_introduction_claim_payload(&payload.encode_to_vec());

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
}
