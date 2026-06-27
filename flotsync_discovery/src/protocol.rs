//! Shared peer-discovery protocol helpers.

use flotsync_core::GroupId;
use flotsync_messages::{
    buffa::{DecodeError, EnumValue, Message, MessageField},
    discovery::{
        IPAddress,
        IPAddressView,
        Peer,
        SocketAddress,
        SocketAddressView,
        ip_address,
        socket_address,
    },
    proto::{self, DecodeProto},
    wire::{UUID_BYTE_LENGTH, fixed_bytes_field},
};
use snafu::prelude::*;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
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
}

/// A decoded plaintext peer announcement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedPeer {
    /// Running process id for the announcing peer instance.
    pub instance_id: Uuid,
    /// Reachability endpoints advertised by this peer instance.
    pub listening_on: Vec<DiscoveryRoute>,
}

impl proto::ProtoCodec for DiscoveryRoute {
    type DecodeError = DiscoveryProtocolError;
    type Proto = SocketAddress;

    fn to_proto(&self) -> Self::Proto {
        match self {
            Self::Udp(address) => udp_socket_address_proto(*address),
        }
    }

    fn from_proto(mut route: Self::Proto) -> Result<Self, Self::DecodeError> {
        let protocol = route.protocol.as_known().ok_or_else(|| {
            DiscoveryProtocolError::UnsupportedRouteProtocol {
                field: "SocketAddress.protocol",
                value: route.protocol.to_i32(),
            }
        })?;
        match protocol {
            socket_address::Protocol::PROTOCOL_UDP => {
                let address =
                    route
                        .address
                        .take()
                        .context(discovery_protocol_error::MissingFieldSnafu {
                            message: "SocketAddress",
                            field: "address",
                        })?;
                let ip = ip_address_from_wire(address, "SocketAddress.address")?;
                let port = socket_port_from_wire(route.port, "SocketAddress.port")?;
                Ok(Self::Udp(SocketAddr::new(ip, port)))
            }
            socket_address::Protocol::PROTOCOL_UNSPECIFIED
            | socket_address::Protocol::PROTOCOL_TCP => {
                discovery_protocol_error::UnsupportedRouteProtocolSnafu {
                    field: "SocketAddress.protocol",
                    value: route.protocol.to_i32(),
                }
                .fail()
            }
        }
    }
}

impl DecodeProto for DecodedPeer {
    type Error = DiscoveryProtocolError;
    type Proto = Peer;

    fn decode_proto(peer: Self::Proto) -> Result<Self, Self::Error> {
        ensure!(
            !peer.listening_on.is_empty(),
            discovery_protocol_error::EmptyPeerRoutesSnafu
        );
        let instance_id = uuid_from_wire(&peer.instance_uuid, "Peer.instance_uuid")?;
        let mut listening_on = Vec::with_capacity(peer.listening_on.len());
        for route in peer.listening_on {
            let route = DiscoveryRoute::decode_proto(route)?;
            listening_on.push(route);
        }
        Ok(Self {
            instance_id,
            listening_on,
        })
    }
}

/// Protocol-level decode and validation failures.
#[derive(Debug, Snafu)]
#[snafu(module(discovery_protocol_error), visibility(pub))]
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
    let peer = Peer::decode_from_slice(bytes).context(discovery_protocol_error::DecodeSnafu)?;
    DecodedPeer::decode_proto(peer)
}

fn udp_socket_address_proto(address: SocketAddr) -> SocketAddress {
    SocketAddress {
        protocol: EnumValue::from(socket_address::Protocol::PROTOCOL_UDP),
        address: MessageField::some(ip_address_proto(address.ip())),
        port: u32::from(address.port()),
        ..SocketAddress::default()
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

fn socket_port_from_wire(port: u32, field: &'static str) -> Result<u16, DiscoveryProtocolError> {
    if port == 0 || port > u32::from(u16::MAX) {
        return discovery_protocol_error::InvalidRoutePortSnafu { field, port }.fail();
    }
    u16::try_from(port).map_err(|_| DiscoveryProtocolError::InvalidRoutePort { field, port })
}

fn ip_address_proto(address: IpAddr) -> IPAddress {
    match address {
        IpAddr::V4(address) => ipv4_address_proto(address),
        IpAddr::V6(address) => ipv6_address_proto(address),
    }
}

fn ipv4_address_proto(address: Ipv4Addr) -> IPAddress {
    IPAddress {
        address: Some(ip_address::Address::Ipv4Bytes(address.octets().to_vec())),
        ..IPAddress::default()
    }
}

fn ipv6_address_proto(address: Ipv6Addr) -> IPAddress {
    IPAddress {
        address: Some(ip_address::Address::Ipv6Bytes(address.octets().to_vec())),
        ..IPAddress::default()
    }
}

fn ip_address_from_wire(
    mut address: IPAddress,
    field: &'static str,
) -> Result<IpAddr, DiscoveryProtocolError> {
    let address = address
        .address
        .take()
        .context(discovery_protocol_error::MissingOneofSnafu {
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
///
/// # Errors
///
/// Returns [`DiscoveryProtocolError`] if the route protocol, address, or port is unsupported.
pub fn discovery_route_from_wire_view(
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
            let address =
                route
                    .address
                    .as_option()
                    .context(discovery_protocol_error::MissingFieldSnafu {
                        message: "SocketAddress",
                        field: "address",
                    })?;
            let ip = ip_address_from_wire_view(address, field)?;
            let port = socket_port_from_wire(route.port, field)?;
            Ok(DiscoveryRoute::Udp(SocketAddr::new(ip, port)))
        }
        socket_address::Protocol::PROTOCOL_UNSPECIFIED | socket_address::Protocol::PROTOCOL_TCP => {
            discovery_protocol_error::UnsupportedRouteProtocolSnafu {
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
    let address =
        address
            .address
            .as_ref()
            .context(discovery_protocol_error::MissingOneofSnafu {
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
    use flotsync_messages::{proto::EncodeProto, wire::uuid_to_wire_bytes};

    #[test]
    fn decodes_valid_peer_with_udp_route() {
        let peer = Peer {
            instance_uuid: uuid_to_wire_bytes(Uuid::from_u128(0x1234)),
            listening_on: vec![
                DiscoveryRoute::Udp(SocketAddr::from(([127, 0, 0, 1], 52156))).encode_proto(),
            ],
            ..Peer::default()
        };

        let decoded = DecodedPeer::decode_proto(peer).expect("valid peer should decode");

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
            listening_on: vec![
                DiscoveryRoute::Udp(SocketAddr::from(([127, 0, 0, 1], 52156))).encode_proto(),
            ],
            ..Peer::default()
        };

        let result = DecodedPeer::decode_proto(peer);

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
                address: MessageField::some(ipv4_address_proto(Ipv4Addr::LOCALHOST)),
                port: 52156,
                ..SocketAddress::default()
            }],
            ..Peer::default()
        };

        let result = DecodedPeer::decode_proto(peer);

        assert!(matches!(
            result,
            Err(DiscoveryProtocolError::UnsupportedRouteProtocol { .. })
        ));
    }
}
