//! Cryptographic building blocks for authenticated encrypted replication frames.

pub use error::{Result, SecurityError};
pub use fingerprint::{KEY_FINGERPRINT_LENGTH, KeyFingerprint, KeyFingerprintParseError};
#[cfg(any(test, feature = "test-support"))]
pub use group::test_group_key_from_id;
pub use group::{
    GROUP_CIPHER_SUITE_CHACHA20_POLY1305,
    GROUP_KEY_LENGTH,
    GROUP_NONCE_LENGTH,
    GroupCipherSuite,
    GroupKey,
    GroupMessageContext,
    group_key_from_stored_secret_plaintext,
    open_group_message,
    open_group_payload,
    open_stored_group_key,
    seal_group_message,
    seal_group_payload,
};
pub use hpke::{HPKE_ENCAPSULATED_KEY_LENGTH, HpkeCiphertext, hpke_open, hpke_seal};
pub use identity::{
    ED25519_KEY_LENGTH,
    EncodedLocalPrivateKeyBundle,
    GeneratedMemberKeyBundles,
    KeyRole,
    LocalMemberKeys,
    MemberIdentity,
    PublicKeyBundle,
    PublicMemberKeys,
    X25519_KEY_LENGTH,
    encode_local_private_key_bundle,
    encode_public_key_bundle,
    generate_member_key_bundles,
    local_member_keys_from_private_bundle,
    public_member_keys_from_public_bundle,
};
#[cfg(any(test, feature = "test-support"))]
pub use local_store_secret::install_local_store_secret_test_store;
pub use local_store_secret::{
    LoadedLocalStoreSecret,
    LocalStoreSecretError,
    LocalStoreSecretProfile,
    LocalStoreSecretResult,
    load_local_store_secret,
    load_or_create_local_store_secret,
};
pub use reliable_payload::{
    ReliablePayloadContext,
    SealedHPKEPayload,
    open_reliable_payload,
    seal_reliable_payload,
    seal_reliable_payload_with_os_rng,
};
pub use sealed_psk_payload::SealedPSKPayload;
pub use signature::{
    FrameSignature,
    FrameSignatureProtoError,
    SIGNATURE_LENGTH,
    SignedFrameParts,
    sign_discovery_payload,
    sign_frame,
    verify_discovery_payload_signature,
    verify_frame_signature,
};
#[cfg(any(test, feature = "test-support"))]
pub use store_secret::seal_store_secret_for_test;
pub use store_secret::{
    STORE_SECRET_CRYPTO_VERSION_V1,
    STORE_SECRET_KEY_LENGTH,
    STORE_SECRET_NONCE_LENGTH,
    StoreSecretCiphertext,
    StoreSecretContext,
    StoreSecretCryptoVersion,
    StoreSecretKey,
    StoreSecretKeyId,
    StoreSecretKeyIdParseError,
    open_store_secret,
    seal_store_secret,
};

mod error;
mod fingerprint;
mod group;
mod hpke;
mod identity;
mod local_store_secret;
mod reliable_payload;
mod sealed_psk_payload;
mod signature;
mod store_secret;
mod util;

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

#[cfg(test)]
mod tests;
