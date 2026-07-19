use crate::{
    ContextMemberRole,
    FrameSignature,
    GROUP_CIPHER_SUITE_CHACHA20_POLY1305,
    GROUP_KEY_LENGTH,
    GroupKey,
    GroupMessageContext,
    HpkeCiphertext,
    HpkeContext,
    HpkeEnvelopePurpose,
    HpkeEnvelopeScope,
    KEY_FINGERPRINT_LENGTH,
    KeyFingerprint,
    KeyFingerprintParseError,
    LocalMemberKeys,
    LocalStoreSecretError,
    LocalStoreSecretProfile,
    MemberIdentity,
    PublicKeyBundle,
    ReliablePayloadContext,
    SecurityError,
    SignedFrameParts,
    StoreSecretContext,
    StoreSecretCryptoVersion,
    StoreSecretKey,
    encode_public_key_bundle,
    group_key_from_stored_secret_plaintext,
    hpke_open,
    hpke_seal,
    identity::{MEMBER_KEY_SEED_LENGTH, generate_member_key_bundles_from_seed},
    install_local_store_secret_test_store,
    load_local_store_secret,
    load_or_create_local_store_secret,
    local_member_keys_from_private_bundle,
    open_group_message,
    open_group_payload,
    open_reliable_payload,
    open_store_secret,
    public_member_keys_from_public_bundle,
    seal_group_message,
    seal_group_payload,
    seal_reliable_payload,
    seal_store_secret_for_test,
    sign_frame,
    test_support::rng_from_seed,
    verify_frame_signature,
};
use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, URL_SAFE},
};
use bytes::Bytes;
use flotsync_core::{GroupId, member::Identifier};
use flotsync_messages::{
    buffa::{EnumValue, Message as _, MessageField, MessageView as _},
    discovery::DiscoverySignatureView,
    proto::{DecodeProtoView, EncodeProto},
    security as security_proto,
};
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

mod fixtures;
mod group_frames;
mod hpke;
mod keys;
mod reliable;
mod store_secret;
