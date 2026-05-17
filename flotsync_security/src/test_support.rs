use crate::{
    GeneratedMemberKeyFiles,
    MemberIdentity,
    Result,
    identity::{MEMBER_KEY_SEED_LENGTH, generate_member_key_files_from_seed},
};
use rand_chacha::ChaCha20Rng;
use rand_core::SeedableRng;

/// Byte length of deterministic seed material for repeatable member keys.
pub const TEST_MEMBER_KEY_SEED_LENGTH: usize = MEMBER_KEY_SEED_LENGTH;

/// Build a deterministic RNG for repeatable crypto tests.
#[must_use]
pub fn rng_from_seed(seed: [u8; 32]) -> ChaCha20Rng {
    ChaCha20Rng::from_seed(seed)
}

/// Generate deterministic member key files for repeatable crypto tests.
///
/// # Errors
///
/// Returns [`crate::SecurityError::SerialiseJwks`] if the generated keys cannot
/// be encoded as JWKS JSON.
pub fn member_key_files_from_seed(
    member_id: MemberIdentity,
    seed: &[u8; TEST_MEMBER_KEY_SEED_LENGTH],
) -> Result<GeneratedMemberKeyFiles> {
    generate_member_key_files_from_seed(member_id, seed)
}
