//! Cryptographic building blocks for authenticated encrypted replication frames.

pub use error::{Result, SecurityError};
pub use group::{
    GROUP_KEY_LENGTH,
    GROUP_NONCE_LENGTH,
    GroupKey,
    GroupMessageContext,
    open_group_message,
    seal_group_message,
};
pub use hpke::{HPKE_ENCAPSULATED_KEY_LENGTH, HpkeCiphertext, hpke_open, hpke_seal};
pub use identity::{
    GeneratedMemberKeyFiles,
    KeyRole,
    LocalMemberKeys,
    MemberIdentity,
    PrivateJwks,
    PublicMemberKeys,
    generate_member_key_files,
    local_member_keys_from_jwks,
    public_member_keys_from_jwks,
};
pub use signature::{
    FrameSignature,
    SIGNATURE_LENGTH,
    SignedFrameParts,
    sign_frame,
    verify_frame_signature,
};

mod error;
mod group;
mod hpke;
mod identity;
mod signature;
mod util;

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

#[cfg(test)]
mod tests;
