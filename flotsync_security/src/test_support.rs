use rand_chacha::ChaCha20Rng;
use rand_core::SeedableRng;

/// Build a deterministic RNG for repeatable crypto tests.
#[must_use]
pub fn rng_from_seed(seed: [u8; 32]) -> ChaCha20Rng {
    ChaCha20Rng::from_seed(seed)
}
