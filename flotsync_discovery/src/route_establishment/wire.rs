use super::state::RouteProbeKey;
use crate::protocol::{
    DiscoveryProtocolError,
    DiscoveryRoute,
    decode_introduction_claim_payload_view,
    discovery_route_from_wire_view,
};
use flotsync_core::MemberIdentity;
use flotsync_messages::{
    buffa::EnumValue,
    discovery::{self as discovery_proto, IntroductionClaimPayloadView},
    wire::{WireValueDecodeError, member_identity_from_wire_format},
};
use flotsync_security::{FrameSignature, SIGNATURE_LENGTH};
use snafu::prelude::*;

/// Decode and validation errors for route establishment frames.
#[derive(Debug, Snafu)]
pub enum RouteEstablishmentError {
    /// A required protobuf message field was absent.
    #[snafu(display("message '{message}' is missing required field '{field}'"))]
    MissingField {
        message: &'static str,
        field: &'static str,
    },
    /// A claimed member id could not be decoded into the local identity type.
    #[snafu(display("member identity could not be decoded: {source}"))]
    DecodeMember { source: WireValueDecodeError },
    /// The exact signed payload bytes were not a valid claim payload.
    #[snafu(display("claim payload could not be decoded: {source}"))]
    DecodeClaimPayload { source: DiscoveryProtocolError },
    /// A decoded claim did not match the active route probe.
    #[snafu(display("claim payload field '{field}' did not match the active probe"))]
    ClaimMismatch { field: &'static str },
    /// A discovery signature declared a scheme this runtime does not understand.
    #[snafu(display("signature used unsupported scheme value {value}"))]
    UnsupportedSignatureScheme { value: i32 },
    /// A discovery signature did not have the Ed25519 signature byte length.
    #[snafu(display("signature had {actual} byte(s), expected {SIGNATURE_LENGTH}"))]
    InvalidSignatureLength { actual: usize },
}

/// Decode and validate one signed introduction claim before asynchronous signature verification.
///
/// Returns `Ok(Some(_))` when the claim was made by a non-local member, matches the active probe,
/// and is ready for asynchronous signature verification. Returns `Ok(None)` for a structurally
/// valid claim made by the local member, because self-claims do not prove a remote route.
///
/// # Errors
///
/// Returns [`RouteEstablishmentError`] when required fields are absent, values cannot be decoded,
/// the payload does not match the active probe, or the discovery signature wrapper is unsupported.
pub fn prepare_claim_for_verification(
    key: &RouteProbeKey,
    expected_nonce: &[u8],
    local_member: &MemberIdentity,
    claim: discovery_proto::SignedIntroductionClaim,
) -> Result<Option<super::state::PendingClaimVerification>, RouteEstablishmentError> {
    let member_id = claim.member_id.as_option().context(MissingFieldSnafu {
        message: "SignedIntroductionClaim",
        field: "member_id",
    })?;
    let member =
        member_identity_from_wire_format(member_id.clone(), "SignedIntroductionClaim.member_id")
            .context(DecodeMemberSnafu)?;
    if member == *local_member {
        return Ok(None);
    }
    {
        let payload = decode_introduction_claim_payload_view(&claim.claim_payload)
            .context(DecodeClaimPayloadSnafu)?;
        validate_claim_payload(key, expected_nonce, &payload)?;
    }
    let signature = claim.signature.as_option().context(MissingFieldSnafu {
        message: "SignedIntroductionClaim",
        field: "signature",
    })?;
    let signature = frame_signature_from_wire(signature)?;
    Ok(Some(super::state::PendingClaimVerification {
        member,
        claim,
        signature,
    }))
}

/// Validate claim fields that must match the receiver's active route probe.
pub fn validate_claim_payload(
    key: &RouteProbeKey,
    expected_nonce: &[u8],
    payload: &IntroductionClaimPayloadView<'_>,
) -> Result<(), RouteEstablishmentError> {
    ensure!(
        payload.instance_uuid == key.instance_id.as_bytes().as_slice(),
        ClaimMismatchSnafu {
            field: "instance_uuid"
        }
    );
    if payload.request_nonce.is_empty() {
        return Err(RouteEstablishmentError::DecodeClaimPayload {
            source: DiscoveryProtocolError::EmptyBytes {
                field: "IntroductionClaimPayload.request_nonce",
            },
        });
    }
    ensure!(
        payload.request_nonce == expected_nonce,
        ClaimMismatchSnafu {
            field: "request_nonce"
        }
    );
    let route = payload.route.as_option().context(MissingFieldSnafu {
        message: "IntroductionClaimPayload",
        field: "route",
    })?;
    let route = discovery_route_from_wire_view(route, "IntroductionClaimPayload.route")
        .context(DecodeClaimPayloadSnafu)?;
    ensure!(
        route == DiscoveryRoute::Udp(key.route),
        ClaimMismatchSnafu { field: "route" }
    );
    Ok(())
}

/// Convert an internal discovery signature into the protobuf wire shape.
#[must_use]
pub fn discovery_signature_to_wire(
    signature: &FrameSignature,
) -> discovery_proto::DiscoverySignature {
    discovery_proto::DiscoverySignature {
        scheme: EnumValue::from(discovery_proto::discovery_signature::Scheme::SCHEME_ED25519PH),
        signature_bytes: signature.as_bytes().to_vec(),
        ..discovery_proto::DiscoverySignature::default()
    }
}

/// Decode the protobuf discovery signature wrapper into raw signature bytes.
///
/// # Errors
///
/// Returns [`RouteEstablishmentError`] when the signature scheme or byte width is unsupported.
pub fn frame_signature_from_wire(
    signature: &discovery_proto::DiscoverySignature,
) -> Result<FrameSignature, RouteEstablishmentError> {
    let scheme = signature.scheme.as_known().ok_or_else(|| {
        RouteEstablishmentError::UnsupportedSignatureScheme {
            value: signature.scheme.to_i32(),
        }
    })?;
    ensure!(
        scheme == discovery_proto::discovery_signature::Scheme::SCHEME_ED25519PH,
        UnsupportedSignatureSchemeSnafu {
            value: signature.scheme.to_i32()
        }
    );
    let bytes: [u8; SIGNATURE_LENGTH] =
        signature
            .signature_bytes
            .as_slice()
            .try_into()
            .map_err(|_| RouteEstablishmentError::InvalidSignatureLength {
                actual: signature.signature_bytes.len(),
            })?;
    Ok(FrameSignature::from_bytes(bytes))
}
