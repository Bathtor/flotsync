use crate::{
    ContextMemberRole,
    LocalMemberKeys,
    error::{ContextMemberMismatchSnafu, Result},
    hpke::{
        HPKE_ENCAPSULATED_KEY_LENGTH,
        HpkeCiphertext,
        HpkeContext,
        HpkeEnvelopePurpose,
        HpkeEnvelopeScope,
        hpke_open,
        hpke_seal,
    },
    identity::{MemberIdentity, PublicMemberKeys},
    signature::{
        FrameSignature,
        SIGNATURE_LENGTH,
        SignedFrameParts,
        sign_frame,
        verify_frame_signature,
    },
    util::{append_len_prefixed, append_member_identity},
};
use bytes::{BufMut, Bytes, BytesMut};
use hpke::rand_core::{CryptoRng, OsRng, RngCore, TryRngCore};
use snafu::prelude::*;
use uuid::Uuid;
use zeroize::Zeroizing;

const DOMAIN_RELIABLE_PAYLOAD_HEADER: &[u8] = b"flotsync/security/reliable-payload-header/v1";

/// Public routing and identity context for one recipient-specific reliable payload.
#[derive(Clone, Copy, Debug)]
pub struct ReliablePayloadContext<'a> {
    /// Stable protocol frame kind used for signature domain separation.
    pub frame_kind: &'static str,
    /// Member identity of the sender that signs the payload.
    pub sender: &'a MemberIdentity,
    /// Member identity of the recipient that can open the HPKE payload.
    pub recipient: &'a MemberIdentity,
    /// Delivery scope that disambiguates direct and group-scoped reliable payloads.
    pub scope: HpkeEnvelopeScope,
    /// Reliable-delivery message id bound into HPKE AAD and signature input.
    pub message_id: Uuid,
}

impl ReliablePayloadContext<'_> {
    /// Ensure the context sender identity matches the supplied signing key owner.
    fn ensure_sender_matches_key(self, key_member: &MemberIdentity) -> Result<()> {
        self.ensure_member_matches_key(ContextMemberRole::Sender, key_member)
    }

    /// Ensure the context recipient identity matches the supplied HPKE key owner.
    fn ensure_recipient_matches_key(self, key_member: &MemberIdentity) -> Result<()> {
        self.ensure_member_matches_key(ContextMemberRole::Recipient, key_member)
    }

    /// Ensure one context identity matches the supplied key owner.
    fn ensure_member_matches_key(
        self,
        member_role: ContextMemberRole,
        key_member: &MemberIdentity,
    ) -> Result<()> {
        let context_member = match member_role {
            ContextMemberRole::Sender => self.sender,
            ContextMemberRole::Recipient => self.recipient,
        };
        ensure!(
            context_member == key_member,
            ContextMemberMismatchSnafu {
                member_role,
                context_member: context_member.clone(),
                key_member: key_member.clone(),
            }
        );
        Ok(())
    }
}

/// Payload encrypted to one recipient with HPKE and authenticated by a detached
/// frame signature.
///
/// HPKE uses the recipient's public key and an encapsulated ephemeral key to
/// derive a message-specific content-encryption key. The signature covers the
/// public header plus the HPKE body, so receivers can authenticate the sender
/// before opening the ciphertext.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SealedHPKEPayload {
    /// Encapsulated ephemeral public key bytes needed by the recipient.
    pub encapsulated_key: [u8; HPKE_ENCAPSULATED_KEY_LENGTH],
    /// HPKE ciphertext including its AEAD authentication tag.
    pub ciphertext: Vec<u8>,
    /// Sender signature over the public header and sealed HPKE body.
    pub signature: [u8; SIGNATURE_LENGTH],
}

/// HPKE-seal and sign one recipient-specific reliable payload.
///
/// # Errors
///
/// Returns [`crate::SecurityError`] if HPKE sealing or signature creation fails.
pub fn seal_reliable_payload<R>(
    sender_keys: &LocalMemberKeys,
    recipient_keys: &PublicMemberKeys,
    context: ReliablePayloadContext<'_>,
    plaintext: &[u8],
    rng: &mut R,
) -> Result<SealedHPKEPayload>
where
    R: CryptoRng + RngCore,
{
    context.ensure_sender_matches_key(sender_keys.member_id())?;
    context.ensure_recipient_matches_key(recipient_keys.member_id())?;
    let public_header = reliable_payload_public_header(context);
    let hpke_context = reliable_payload_hpke_context(context, public_header.as_ref());
    let sealed = hpke_seal(recipient_keys, hpke_context, plaintext, rng)?;
    let (encapsulated_key, ciphertext) = sealed.into_parts();
    let signed_body = reliable_payload_signed_body(&encapsulated_key, &ciphertext);
    let signature = sign_frame(
        sender_keys,
        SignedFrameParts {
            frame_kind: context.frame_kind,
            public_header: &public_header,
            ciphertext: &signed_body,
        },
    )?;
    Ok(SealedHPKEPayload {
        encapsulated_key,
        ciphertext,
        signature: *signature.as_bytes(),
    })
}

/// HPKE-seal and sign one reliable payload with OS randomness.
///
/// # Errors
///
/// Returns [`crate::SecurityError`] if HPKE sealing or signature creation fails.
///
/// # Panics
///
/// Panics if the operating system random source fails. The underlying HPKE API
/// accepts an infallible [`RngCore`], so this helper uses `rand_core`'s direct
/// `OsRng` adapter. Tests or specialised callers that need explicit RNG control
/// should call [`seal_reliable_payload`] instead.
pub fn seal_reliable_payload_with_os_rng(
    sender_keys: &LocalMemberKeys,
    recipient_keys: &PublicMemberKeys,
    context: ReliablePayloadContext<'_>,
    plaintext: &[u8],
) -> Result<SealedHPKEPayload> {
    let mut rng = OsRng.unwrap_err();
    seal_reliable_payload(sender_keys, recipient_keys, context, plaintext, &mut rng)
}

/// Verify, HPKE-open, and return one recipient-specific reliable payload.
///
/// # Errors
///
/// Returns [`crate::SecurityError`] if the signature fails or HPKE opening fails.
pub fn open_reliable_payload(
    sender_keys: &PublicMemberKeys,
    recipient_keys: &LocalMemberKeys,
    context: ReliablePayloadContext<'_>,
    sealed: &SealedHPKEPayload,
) -> Result<Vec<u8>> {
    context.ensure_sender_matches_key(sender_keys.member_id())?;
    context.ensure_recipient_matches_key(recipient_keys.member_id())?;
    let public_header = reliable_payload_public_header(context);
    let signed_body = reliable_payload_signed_body(&sealed.encapsulated_key, &sealed.ciphertext);
    verify_frame_signature(
        sender_keys,
        SignedFrameParts {
            frame_kind: context.frame_kind,
            public_header: &public_header,
            ciphertext: &signed_body,
        },
        &FrameSignature::from_bytes(sealed.signature),
    )?;
    let hpke_ciphertext =
        HpkeCiphertext::from_parts(sealed.encapsulated_key, sealed.ciphertext.clone());
    let hpke_context = reliable_payload_hpke_context(context, public_header.as_ref());
    let plaintext = Zeroizing::new(hpke_open(recipient_keys, hpke_context, &hpke_ciphertext)?);
    Ok(plaintext.as_slice().to_vec())
}

/// Build public routing bytes authenticated by both signature and HPKE AAD.
fn reliable_payload_public_header(context: ReliablePayloadContext<'_>) -> Bytes {
    let mut output = BytesMut::new();
    append_len_prefixed(&mut output, DOMAIN_RELIABLE_PAYLOAD_HEADER);
    append_len_prefixed(&mut output, context.frame_kind.as_bytes());
    context.scope.append_to(&mut output);
    append_member_identity(&mut output, context.sender);
    append_member_identity(&mut output, context.recipient);
    output.put_slice(context.message_id.as_bytes());
    output.freeze()
}

/// Build the HPKE context that protects one reliable payload.
fn reliable_payload_hpke_context<'a>(
    context: ReliablePayloadContext<'a>,
    public_header: &'a [u8],
) -> HpkeContext<'a> {
    HpkeContext {
        purpose: HpkeEnvelopePurpose::ReliablePayload,
        sender: context.sender,
        recipient: context.recipient,
        scope: context.scope,
        delivery_message_id: context.message_id,
        authenticated_public_metadata: public_header,
    }
}

/// Build the sealed body covered by the sender signature.
fn reliable_payload_signed_body(
    encapsulated_key: &[u8; HPKE_ENCAPSULATED_KEY_LENGTH],
    ciphertext: &[u8],
) -> Bytes {
    let mut body =
        BytesMut::with_capacity(HPKE_ENCAPSULATED_KEY_LENGTH + len_prefixed_capacity(ciphertext));
    body.put_slice(encapsulated_key);
    append_len_prefixed(&mut body, ciphertext);
    body.freeze()
}

/// Return the encoded size of one `append_len_prefixed` field.
const fn len_prefixed_capacity(value: &[u8]) -> usize {
    std::mem::size_of::<u64>() + value.len()
}
