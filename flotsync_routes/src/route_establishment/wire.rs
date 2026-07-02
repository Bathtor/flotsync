//! Wire-level validation helpers for signed route-establishment introductions.

use flotsync_core::MemberIdentity;
use flotsync_discovery::protocol::{DiscoveryProtocolError, DiscoveryRoute};
use flotsync_messages::{
    discovery::{self as discovery_proto, IntroductionClaimPayloadView},
    proto::{DecodeProto, DecodeProtoView},
};
use flotsync_security::{FrameSignature, FrameSignatureProtoError};
use snafu::prelude::*;
use uuid::Uuid;

use crate::protocol::{decode_introduction_claim_payload_view, introduction_claim_selector};

/// Decode and validation errors for route establishment frames.
#[derive(Debug, Snafu)]
pub enum RouteEstablishmentError {
    /// A required protobuf message field was absent.
    #[snafu(display("message '{message}' is missing required field '{field}'"))]
    MissingField {
        message: &'static str,
        field: &'static str,
    },
    /// The exact signed payload bytes were not a valid claim payload.
    #[snafu(display("claim payload could not be decoded: {source}"))]
    DecodeClaimPayload { source: DiscoveryProtocolError },
    /// A decoded claim did not match the active route probe.
    #[snafu(display("claim payload field '{field}' did not match the active probe"))]
    ClaimMismatch { field: &'static str },
    /// A discovery signature wrapper could not be decoded into a frame signature.
    #[snafu(display("discovery signature could not be decoded: {source}"))]
    DecodeSignature { source: FrameSignatureProtoError },
}

/// Decode and validate one signed introduction claim before asynchronous signature verification.
///
/// Returns `Ok(Some(_))` when the claim was made by a non-local member, matches the active probe,
/// and is ready for asynchronous signature verification. Returns `Ok(None)` when the signed
/// payload selector names the local member, because self-claims do not prove a remote route.
///
/// # Errors
///
/// Returns [`RouteEstablishmentError`] when required fields are absent, values cannot be decoded,
/// the payload does not match the active probe, or the discovery signature wrapper is malformed.
pub fn prepare_claim_for_verification(
    expected_route: DiscoveryRoute,
    expected_instance_id: Uuid,
    expected_nonce: &[u8],
    local_member: &MemberIdentity,
    claim: discovery_proto::SignedIntroductionClaim,
) -> Result<Option<super::state::PendingClaimVerification>, RouteEstablishmentError> {
    let payload = decode_introduction_claim_payload_view(&claim.claim_payload)
        .context(DecodeClaimPayloadSnafu)?;
    let selector = introduction_claim_selector(&payload).context(DecodeClaimPayloadSnafu)?;
    let member = selector.member;
    if member == *local_member {
        return Ok(None);
    }
    validate_claim_payload(
        expected_route,
        expected_instance_id,
        expected_nonce,
        &payload,
    )?;
    let signature = claim.signature.as_option().context(MissingFieldSnafu {
        message: "SignedIntroductionClaim",
        field: "signature",
    })?;
    let signature =
        FrameSignature::decode_proto(signature.clone()).context(DecodeSignatureSnafu)?;
    Ok(Some(super::state::PendingClaimVerification {
        member,
        key_fingerprint: selector.key_fingerprint,
        claim,
        signature,
    }))
}

/// Validate claim fields that must match the receiver's active route probe.
pub fn validate_claim_payload(
    expected_route: DiscoveryRoute,
    expected_instance_id: Uuid,
    expected_nonce: &[u8],
    payload: &IntroductionClaimPayloadView<'_>,
) -> Result<(), RouteEstablishmentError> {
    ensure!(
        payload.instance_uuid == expected_instance_id.as_bytes().as_slice(),
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
    let claimed_route =
        DiscoveryRoute::decode_proto_view(route).context(DecodeClaimPayloadSnafu)?;
    ensure!(
        claimed_route == expected_route,
        ClaimMismatchSnafu { field: "route" }
    );
    Ok(())
}
