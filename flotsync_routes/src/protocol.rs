//! Route-establishment introduction protocol helpers.

use borrowize::View;
use flotsync_core::{GroupId, MemberIdentity};
use flotsync_discovery::protocol::{DiscoveryProtocolError, discovery_protocol_error};
use flotsync_messages::{
    buffa::{self, Message, MessageField, MessageView},
    discovery::{
        DiscoveryFrame,
        Introduction,
        IntroductionClaimPayload,
        IntroductionClaimPayloadView,
        IntroductionRequest,
        KeyBundleLookupRequest,
        KeyBundleLookupResponsePayload as KeyBundleLookupResponsePayloadProto,
        SignedKeyBundleLookupResponse,
        discovery_frame,
    },
    endpoint::{EndpointFrame, EndpointFrameView, endpoint_frame},
    proto::{DecodeProto, DecodeProtoView, EncodeProto, FromProtoDecodeError},
    wire::{
        UUID_BYTE_LENGTH,
        fixed_bytes_field,
        group_id_from_wire_bytes,
        member_identity_from_wire_format,
        member_identity_from_wire_view,
        member_identity_to_wire_format,
        uuid_from_wire_bytes,
        uuid_to_wire_bytes,
    },
};
use flotsync_security::{KEY_FINGERPRINT_LENGTH, KeyFingerprint, PublicKeyBundle, SecurityError};
use snafu::prelude::*;
use std::collections::HashSet;
use uuid::Uuid;

pub use flotsync_discovery::protocol::DiscoveryRoute;

/// Maximum encoded size accepted by default for safe route-protocol view decoding.
pub const ROUTE_SAFE_DECODE_MAX_BYTES: usize = 16 * 1024;

/// Maximum nested protobuf message depth accepted for safe route-protocol view decoding.
pub const ROUTE_SAFE_DECODE_RECURSION_LIMIT: u32 = 8;

/// Maximum number of unknown fields accepted for safe route-protocol view decoding.
pub const ROUTE_SAFE_DECODE_UNKNOWN_FIELD_LIMIT: usize = 8;

/// A decoded signed-claim payload before signature verification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedIntroductionClaimPayload {
    /// Member identity whose key signs this claim.
    pub member: MemberIdentity,
    /// Fingerprint of the public key bundle whose signing key signs this claim.
    pub key_fingerprint: KeyFingerprint,
    /// Running process id for the peer instance making this claim.
    pub instance_id: Uuid,
    /// Receiver-generated freshness challenge echoed by this claim.
    pub request_nonce: Uuid,
    /// Route endpoint covered by this claim.
    pub route: DiscoveryRoute,
    /// Replication group ids covered by this claim.
    pub group_ids: HashSet<GroupId>,
}

/// Decoded direct key-bundle lookup request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedKeyBundleLookupRequest {
    /// Member identity whose public key bundle is requested.
    pub member: MemberIdentity,
    /// Exact public key bundle fingerprint requested.
    pub key_fingerprint: KeyFingerprint,
    /// Receiver-generated freshness challenge to echo in the signed response.
    pub request_nonce: Uuid,
}

impl DecodeProto for DecodedKeyBundleLookupRequest {
    type Error = DiscoveryProtocolError;
    type Proto = KeyBundleLookupRequest;

    fn decode_proto(mut request: Self::Proto) -> Result<Self, Self::Error> {
        let member_id =
            request
                .member_id
                .take()
                .context(discovery_protocol_error::MissingFieldSnafu {
                    message: "KeyBundleLookupRequest",
                    field: "member_id",
                })?;
        let member =
            member_identity_from_wire_format(member_id, "KeyBundleLookupRequest.member_id")
                .context(discovery_protocol_error::InvalidMemberIdentitySnafu {
                    field: "KeyBundleLookupRequest.member_id",
                })?;
        let key_fingerprint = key_fingerprint_from_wire(
            &request.key_fingerprint,
            "KeyBundleLookupRequest.key_fingerprint",
        )?;
        let request_nonce = uuid_from_wire_bytes(
            &request.request_nonce,
            "KeyBundleLookupRequest.request_nonce",
        )
        .context(discovery_protocol_error::InvalidWireValueSnafu)?;
        Ok(Self {
            member,
            key_fingerprint,
            request_nonce,
        })
    }
}

/// Direct key-bundle lookup response payload.
#[derive(Clone, Debug, PartialEq, Eq, View)]
pub struct KeyBundleLookupResponsePayload {
    /// Member identity supplied by the requester.
    pub member: MemberIdentity,
    /// Exact public key bundle fingerprint requested by the requester.
    #[borrowize(
        borrowed_type = "KeyFingerprint",
        generation_expression = "self.key_fingerprint"
    )]
    pub key_fingerprint: KeyFingerprint,
    /// Freshness challenge echoed from the request.
    #[borrowize(borrowed_type = "Uuid", generation_expression = "self.request_nonce")]
    pub request_nonce: Uuid,
    /// Identity-free public key bundle returned by the responder.
    pub public_key_bundle: PublicKeyBundle,
}

impl DecodeProto for KeyBundleLookupResponsePayload {
    type Error = KeyBundleLookupPayloadError;
    type Proto = KeyBundleLookupResponsePayloadProto;

    fn decode_proto(mut payload: Self::Proto) -> Result<Self, Self::Error> {
        let member_id = payload
            .member_id
            .take()
            .context(discovery_protocol_error::MissingFieldSnafu {
                message: "KeyBundleLookupResponsePayload",
                field: "member_id",
            })
            .context(key_bundle_lookup_payload_error::DiscoverySnafu)?;
        let member =
            member_identity_from_wire_format(member_id, "KeyBundleLookupResponsePayload.member_id")
                .context(discovery_protocol_error::InvalidMemberIdentitySnafu {
                    field: "KeyBundleLookupResponsePayload.member_id",
                })
                .context(key_bundle_lookup_payload_error::DiscoverySnafu)?;
        let key_fingerprint = key_fingerprint_from_wire(
            &payload.key_fingerprint,
            "KeyBundleLookupResponsePayload.key_fingerprint",
        )
        .context(key_bundle_lookup_payload_error::DiscoverySnafu)?;
        let request_nonce = uuid_from_wire_bytes(
            &payload.request_nonce,
            "KeyBundleLookupResponsePayload.request_nonce",
        )
        .context(discovery_protocol_error::InvalidWireValueSnafu)
        .context(key_bundle_lookup_payload_error::DiscoverySnafu)?;
        let public_key_bundle = payload
            .public_key_bundle
            .take()
            .context(discovery_protocol_error::MissingFieldSnafu {
                message: "KeyBundleLookupResponsePayload",
                field: "public_key_bundle",
            })
            .context(key_bundle_lookup_payload_error::DiscoverySnafu)?;
        let public_key_bundle = PublicKeyBundle::decode_proto(public_key_bundle)
            .context(key_bundle_lookup_payload_error::PublicKeyBundleSnafu)?;
        let derived_fingerprint = public_key_bundle.fingerprint();
        ensure!(
            derived_fingerprint == key_fingerprint,
            key_bundle_lookup_payload_error::FingerprintMismatchSnafu {
                declared: key_fingerprint,
                derived: derived_fingerprint,
            }
        );
        Ok(Self {
            member,
            key_fingerprint,
            request_nonce,
            public_key_bundle,
        })
    }
}

/// Decode and validation errors for key-bundle lookup response payloads.
#[derive(Debug, Snafu)]
#[snafu(module(key_bundle_lookup_payload_error), visibility(pub))]
pub enum KeyBundleLookupPayloadError {
    /// The response payload bytes could not be decoded as protobuf.
    #[snafu(display("key-bundle lookup response payload bytes were malformed: {source}"))]
    Decode {
        /// Generated protobuf decode failure.
        source: buffa::DecodeError,
    },
    /// The discovery selector or nonce fields were malformed.
    #[snafu(display("key-bundle lookup response discovery fields were invalid: {source}"))]
    Discovery { source: DiscoveryProtocolError },
    /// The returned public key bundle was malformed.
    #[snafu(display("key-bundle lookup response public key bundle was invalid: {source}"))]
    PublicKeyBundle { source: SecurityError },
    /// The returned public key bundle did not derive to the declared fingerprint.
    #[snafu(display(
        "key-bundle lookup response declared fingerprint {declared}, but returned bundle derives to {derived}"
    ))]
    FingerprintMismatch {
        /// Fingerprint named by the response selector.
        declared: KeyFingerprint,
        /// Fingerprint derived from the returned public key bundle.
        derived: KeyFingerprint,
    },
}

impl FromProtoDecodeError for KeyBundleLookupPayloadError {
    fn from_proto_decode_error(source: buffa::DecodeError) -> Self {
        Self::Decode { source }
    }
}

impl DecodeProto for DecodedIntroductionClaimPayload {
    type Error = DiscoveryProtocolError;
    type Proto = IntroductionClaimPayload;

    fn decode_proto(mut payload: Self::Proto) -> Result<Self, Self::Error> {
        let member_id =
            payload
                .member_id
                .take()
                .context(discovery_protocol_error::MissingFieldSnafu {
                    message: "IntroductionClaimPayload",
                    field: "member_id",
                })?;
        let member =
            member_identity_from_wire_format(member_id, "IntroductionClaimPayload.member_id")
                .context(discovery_protocol_error::InvalidMemberIdentitySnafu {
                    field: "IntroductionClaimPayload.member_id",
                })?;
        let key_fingerprint = key_fingerprint_from_wire(
            &payload.key_fingerprint,
            "IntroductionClaimPayload.key_fingerprint",
        )?;
        let instance_id = uuid_from_wire_bytes(
            &payload.instance_uuid,
            "IntroductionClaimPayload.instance_uuid",
        )
        .context(discovery_protocol_error::InvalidWireValueSnafu)?;
        let request_nonce = uuid_from_wire_bytes(
            &payload.request_nonce,
            "IntroductionClaimPayload.request_nonce",
        )
        .context(discovery_protocol_error::InvalidWireValueSnafu)?;
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
            member,
            key_fingerprint,
            instance_id,
            request_nonce,
            route,
            group_ids,
        })
    }
}

impl DecodeProtoView for DecodedIntroductionClaimPayload {
    type Error = DiscoveryProtocolError;
    type ProtoView<'a> = IntroductionClaimPayloadView<'a>;

    fn decode_proto_view(payload: &Self::ProtoView<'_>) -> Result<Self, Self::Error> {
        let member_id =
            payload
                .member_id
                .as_option()
                .context(discovery_protocol_error::MissingFieldSnafu {
                    message: "IntroductionClaimPayload",
                    field: "member_id",
                })?;
        let member =
            member_identity_from_wire_view(member_id, "IntroductionClaimPayload.member_id")
                .context(discovery_protocol_error::InvalidMemberIdentitySnafu {
                    field: "IntroductionClaimPayload.member_id",
                })?;
        let key_fingerprint = key_fingerprint_from_wire(
            payload.key_fingerprint,
            "IntroductionClaimPayload.key_fingerprint",
        )?;
        let instance_id = uuid_from_wire_bytes(
            payload.instance_uuid,
            "IntroductionClaimPayload.instance_uuid",
        )
        .context(discovery_protocol_error::InvalidWireValueSnafu)?;
        let request_nonce = uuid_from_wire_bytes(
            payload.request_nonce,
            "IntroductionClaimPayload.request_nonce",
        )
        .context(discovery_protocol_error::InvalidWireValueSnafu)?;
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
            member,
            key_fingerprint,
            instance_id,
            request_nonce,
            route,
            group_ids,
        })
    }
}

/// Member and key-material selector extracted from a signed claim payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IntroductionClaimSelector {
    /// Member identity whose stored key material should verify the claim.
    pub member: MemberIdentity,
    /// Fingerprint of the exact stored public key bundle used for verification.
    pub key_fingerprint: KeyFingerprint,
}

/// Decode one signed introduction claim payload using route-establishment limits.
///
/// # Errors
///
/// Returns [`DiscoveryProtocolError`] when the payload bytes are malformed or exceed the
/// route safe-decode limits.
pub fn decode_introduction_claim_payload_view(
    bytes: &[u8],
) -> Result<IntroductionClaimPayloadView<'_>, DiscoveryProtocolError> {
    safe_decode_options()
        .decode_view(bytes)
        .context(discovery_protocol_error::DecodeSnafu)
}

/// Build conservative decode options for untrusted route-protocol borrowed views.
///
/// Use this when route/discovery code needs to decode untrusted protobuf bytes into a borrowed
/// view before authentication or full semantic validation. Callers must still verify or validate
/// the original bytes before trusting the contents; this helper only applies the shared route
/// protocol size, recursion, and unknown-field limits.
#[must_use]
pub fn safe_decode_options() -> buffa::DecodeOptions {
    buffa::DecodeOptions::new()
        .with_max_message_size(ROUTE_SAFE_DECODE_MAX_BYTES)
        .with_recursion_limit(ROUTE_SAFE_DECODE_RECURSION_LIMIT)
        .with_unknown_field_limit(ROUTE_SAFE_DECODE_UNKNOWN_FIELD_LIMIT)
}

/// Decode the member/key selector from one already-decoded signed claim payload view.
///
/// # Errors
///
/// Returns [`DiscoveryProtocolError`] when either selector field is absent or malformed.
pub fn introduction_claim_selector(
    payload: &IntroductionClaimPayloadView<'_>,
) -> Result<IntroductionClaimSelector, DiscoveryProtocolError> {
    let member_id =
        payload
            .member_id
            .as_option()
            .context(discovery_protocol_error::MissingFieldSnafu {
                message: "IntroductionClaimPayload",
                field: "member_id",
            })?;
    let member = member_identity_from_wire_view(member_id, "IntroductionClaimPayload.member_id")
        .context(discovery_protocol_error::InvalidMemberIdentitySnafu {
            field: "IntroductionClaimPayload.member_id",
        })?;
    let key_fingerprint = key_fingerprint_from_wire(
        payload.key_fingerprint,
        "IntroductionClaimPayload.key_fingerprint",
    )?;
    Ok(IntroductionClaimSelector {
        member,
        key_fingerprint,
    })
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

/// Encode the exact member/key selector fields shared by endpoint discovery payloads.
#[must_use]
pub fn encode_member_key_selector_fields(
    member: &MemberIdentity,
    key_fingerprint: KeyFingerprint,
) -> (flotsync_messages::discovery::Identifier, Vec<u8>) {
    (
        member_identity_to_wire_format(member),
        key_fingerprint.as_ref().to_vec(),
    )
}

/// One discovery message to encode into the shared endpoint envelope.
#[derive(View)]
pub enum DiscoveryEndpointFrame {
    /// Introduction request for probing one candidate route.
    IntroductionRequest {
        /// Request freshness challenge that the signed response must echo.
        #[borrowize(borrowed_type = "Uuid", generation_expression = "*request_nonce")]
        request_nonce: Uuid,
    },
    /// Introduction response with signed route claims.
    Introduction {
        /// Introduction payload to wrap in the endpoint envelope.
        introduction: Introduction,
    },
    /// Direct key-bundle lookup request.
    KeyBundleLookupRequest {
        /// Member identity whose public key bundle is requested.
        member: MemberIdentity,
        /// Exact public key bundle fingerprint requested.
        #[borrowize(
            borrowed_type = "KeyFingerprint",
            generation_expression = "*key_fingerprint"
        )]
        key_fingerprint: KeyFingerprint,
        /// Request freshness challenge that the signed response must echo.
        #[borrowize(borrowed_type = "Uuid", generation_expression = "*request_nonce")]
        request_nonce: Uuid,
    },
    /// Signed direct key-bundle lookup response.
    KeyBundleLookupResponse {
        /// Signed response to wrap in the endpoint envelope.
        response: SignedKeyBundleLookupResponse,
    },
}

impl EncodeProto for DiscoveryEndpointFrame {
    type Proto = EndpointFrame;

    fn encode_proto(&self) -> Self::Proto {
        self.view().encode_proto()
    }
}

impl EncodeProto for DiscoveryEndpointFrameView<'_> {
    type Proto = EndpointFrame;

    fn encode_proto(&self) -> Self::Proto {
        let body = match self {
            Self::IntroductionRequest { request_nonce } => {
                discovery_frame::Body::IntroductionRequest(Box::new(IntroductionRequest {
                    request_nonce: uuid_to_wire_bytes(*request_nonce),
                    ..IntroductionRequest::default()
                }))
            }
            Self::Introduction { introduction } => {
                discovery_frame::Body::Introduction(Box::new((*introduction).clone()))
            }
            Self::KeyBundleLookupRequest {
                member,
                key_fingerprint,
                request_nonce,
            } => {
                let (member_id, key_fingerprint) =
                    encode_member_key_selector_fields(member, *key_fingerprint);
                discovery_frame::Body::KeyBundleLookupRequest(Box::new(KeyBundleLookupRequest {
                    member_id: MessageField::some(member_id),
                    key_fingerprint,
                    request_nonce: uuid_to_wire_bytes(*request_nonce),
                    ..KeyBundleLookupRequest::default()
                }))
            }
            Self::KeyBundleLookupResponse { response } => {
                discovery_frame::Body::KeyBundleLookupResponse(Box::new((*response).clone()))
            }
        };
        let discovery = DiscoveryFrame {
            body: Some(body),
            ..DiscoveryFrame::default()
        };
        discovery_endpoint_frame(discovery)
    }
}

impl EncodeProto for KeyBundleLookupResponsePayloadView<'_> {
    type Proto = KeyBundleLookupResponsePayloadProto;

    fn encode_proto(&self) -> Self::Proto {
        let (member_id, key_fingerprint) =
            encode_member_key_selector_fields(self.member, self.key_fingerprint);
        KeyBundleLookupResponsePayloadProto {
            member_id: MessageField::some(member_id),
            key_fingerprint,
            request_nonce: uuid_to_wire_bytes(self.request_nonce),
            public_key_bundle: MessageField::some(self.public_key_bundle.encode_proto()),
            ..KeyBundleLookupResponsePayloadProto::default()
        }
    }
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
            let discovery = discovery
                .to_owned_message()
                .context(discovery_protocol_error::DecodeSnafu)?;
            Ok(Some(discovery))
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

/// Classify one payload from a shared UDP endpoint as an optional discovery frame.
///
/// This helper is intentionally lossy at the byte-decode boundary: runtime UDP
/// endpoints may receive unrelated datagrams, so bytes that are not even an
/// endpoint frame are classified as irrelevant traffic. Well-formed endpoint
/// frames with invalid discovery contents still return an error.
///
/// # Errors
///
/// Returns [`DiscoveryProtocolError`] when bytes decode as an endpoint frame but
/// violate endpoint-discovery invariants.
pub fn classify_shared_endpoint_discovery_frame_from_buf(
    buf: &mut impl buffa::bytes::Buf,
) -> Result<Option<DiscoveryFrame>, DiscoveryProtocolError> {
    match decode_endpoint_discovery_frame_from_buf(buf) {
        Ok(frame) => Ok(frame),
        Err(DiscoveryProtocolError::Decode { .. }) => Ok(None),
        Err(error) => Err(error),
    }
}

fn discovery_endpoint_frame(discovery: DiscoveryFrame) -> EndpointFrame {
    EndpointFrame {
        boundary: Some(endpoint_frame::Boundary::Discovery(Box::new(discovery))),
        ..EndpointFrame::default()
    }
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

fn key_fingerprint_from_wire(
    bytes: &[u8],
    field: &'static str,
) -> Result<KeyFingerprint, DiscoveryProtocolError> {
    let bytes = fixed_bytes_field::<KEY_FINGERPRINT_LENGTH>(field, bytes).map_err(|_| {
        DiscoveryProtocolError::InvalidByteLength {
            field,
            expected: KEY_FINGERPRINT_LENGTH,
            actual: bytes.len(),
        }
    })?;
    Ok(KeyFingerprint::from_bytes(bytes))
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
        let member = MemberIdentity::from_array(["test", "alice"]);
        let key = KeyFingerprint::from_bytes([7; KEY_FINGERPRINT_LENGTH]);
        let request_nonce = Uuid::from_bytes([0x42; 16]);
        let (member_id, key_fingerprint) = encode_member_key_selector_fields(&member, key);
        let payload = IntroductionClaimPayload {
            instance_uuid: uuid_to_wire_bytes(Uuid::from_u128(0x1234)),
            request_nonce: uuid_to_wire_bytes(request_nonce),
            route: MessageField::some(route.encode_proto()),
            group_ids: vec![
                group_id_to_wire_bytes(first_group),
                group_id_to_wire_bytes(second_group),
            ],
            member_id: MessageField::some(member_id),
            key_fingerprint,
            ..IntroductionClaimPayload::default()
        };

        let payload_bytes = payload.encode_to_vec();
        let decoded = DecodedIntroductionClaimPayload::decode_proto_view_from_slice(&payload_bytes)
            .expect("valid claim should decode");
        let payload_view = IntroductionClaimPayloadView::decode_view(&payload_bytes)
            .expect("claim payload view should decode");

        assert_eq!(decoded.instance_id, Uuid::from_u128(0x1234));
        assert_eq!(decoded.member, member);
        assert_eq!(decoded.key_fingerprint, key);
        assert_eq!(decoded.request_nonce, request_nonce);
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
        let member = MemberIdentity::from_array(["test", "alice"]);
        let key = KeyFingerprint::from_bytes([7; KEY_FINGERPRINT_LENGTH]);
        let (member_id, key_fingerprint) = encode_member_key_selector_fields(&member, key);
        let payload = IntroductionClaimPayload {
            instance_uuid: uuid_to_wire_bytes(Uuid::from_u128(0x1234)),
            request_nonce: uuid_to_wire_bytes(Uuid::from_bytes([0x42; 16])),
            route: MessageField::some(
                DiscoveryRoute::Udp(SocketAddr::from(([127, 0, 0, 1], 52156))).encode_proto(),
            ),
            group_ids: vec![group_id_to_wire_bytes(group), group_id_to_wire_bytes(group)],
            member_id: MessageField::some(member_id),
            key_fingerprint,
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
    fn shared_endpoint_classifier_ignores_non_endpoint_bytes() {
        let mut bytes = &b"\0"[..];

        let decoded = classify_shared_endpoint_discovery_frame_from_buf(&mut bytes)
            .expect("non-endpoint bytes should classify as irrelevant traffic");

        assert!(decoded.is_none());
    }

    #[test]
    fn endpoint_decoder_returns_discovery_frame() {
        let request_nonce = Uuid::from_bytes([0x42; 16]);
        let request =
            DiscoveryEndpointFrameView::IntroductionRequest { request_nonce }.encode_proto();

        let decoded = decode_endpoint_discovery_frame(&request.encode_to_vec())
            .expect("endpoint frame should decode")
            .expect("discovery frame should be returned");

        assert!(matches!(
            decoded.body,
            Some(discovery_frame::Body::IntroductionRequest(request))
                if request.request_nonce == uuid_to_wire_bytes(request_nonce)
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
