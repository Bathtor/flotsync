use crate::{
    LocalMemberKeys,
    error::Result,
    hpke::{HPKE_ENCAPSULATED_KEY_LENGTH, HpkeCiphertext, hpke_open, hpke_seal},
    identity::{MemberIdentity, PublicMemberKeys},
    signature::{
        FrameSignature,
        SIGNATURE_LENGTH,
        SignedFrameParts,
        sign_frame,
        verify_frame_signature,
    },
    util::append_len_prefixed,
};
use bytes::{BufMut, Bytes, BytesMut};
use hpke::rand_core::{CryptoRng, OsRng, RngCore, TryRngCore};
use uuid::Uuid;
use zeroize::Zeroizing;

const DOMAIN_RELIABLE_PAYLOAD_HEADER: &[u8] = b"flotsync/security/reliable-payload-header/v1";
const DOMAIN_RELIABLE_PAYLOAD_HPKE_INFO: &[u8] = b"flotsync/security/reliable-payload-hpke-info/v1";

/// Public routing and identity context for one recipient-specific reliable payload.
#[derive(Clone, Copy, Debug)]
pub struct ReliablePayloadContext<'a> {
    /// Stable protocol frame kind used for signature domain separation.
    pub frame_kind: &'static str,
    /// Member identity of the sender that signs the payload.
    pub sender: &'a MemberIdentity,
    /// Member identity of the recipient that can open the HPKE payload.
    pub recipient: &'a MemberIdentity,
    /// Reliable-delivery message id bound into HPKE AAD and signature input.
    pub message_id: Uuid,
}

/// HPKE-sealed and signed reliable payload fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SealedReliablePayload {
    /// HPKE encapsulated ephemeral public key bytes.
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
) -> Result<SealedReliablePayload>
where
    R: CryptoRng + RngCore,
{
    let public_header = reliable_payload_public_header(context);
    let info = reliable_payload_hpke_info(context);
    let sealed = hpke_seal(recipient_keys, &info, &public_header, plaintext, rng)?;
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
    Ok(SealedReliablePayload {
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
) -> Result<SealedReliablePayload> {
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
    sealed: &SealedReliablePayload,
) -> Result<Vec<u8>> {
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
    let info = reliable_payload_hpke_info(context);
    let hpke_ciphertext =
        HpkeCiphertext::from_parts(sealed.encapsulated_key, sealed.ciphertext.clone());
    let plaintext = Zeroizing::new(hpke_open(
        recipient_keys,
        &info,
        &public_header,
        &hpke_ciphertext,
    )?);
    Ok(plaintext.as_slice().to_vec())
}

/// Build public routing bytes authenticated by both signature and HPKE AAD.
fn reliable_payload_public_header(context: ReliablePayloadContext<'_>) -> Bytes {
    reliable_payload_context_bytes(DOMAIN_RELIABLE_PAYLOAD_HEADER, context)
}

/// Build HPKE info bytes that bind key schedule derivation to this delivery context.
fn reliable_payload_hpke_info(context: ReliablePayloadContext<'_>) -> Bytes {
    reliable_payload_context_bytes(DOMAIN_RELIABLE_PAYLOAD_HPKE_INFO, context)
}

/// Build the shared reliable payload context bytes under a specific domain.
fn reliable_payload_context_bytes(domain: &[u8], context: ReliablePayloadContext<'_>) -> Bytes {
    let sender = context.sender.to_string();
    let recipient = context.recipient.to_string();
    let capacity = len_prefixed_capacity(domain)
        + len_prefixed_capacity(context.frame_kind.as_bytes())
        + len_prefixed_capacity(sender.as_bytes())
        + len_prefixed_capacity(recipient.as_bytes())
        + context.message_id.as_bytes().len();
    let mut output = BytesMut::with_capacity(capacity);
    append_len_prefixed(&mut output, domain);
    append_len_prefixed(&mut output, context.frame_kind.as_bytes());
    append_len_prefixed(&mut output, sender.as_bytes());
    append_len_prefixed(&mut output, recipient.as_bytes());
    output.put_slice(context.message_id.as_bytes());
    output.freeze()
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
