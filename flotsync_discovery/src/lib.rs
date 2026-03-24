use derive_more::{Deref, Display, From};

#[cfg(feature = "kompact-runtime")]
pub use kompact;
pub use uuid;
pub mod errors;
#[cfg(feature = "kompact-runtime")]
pub mod kompact_fsm;
pub mod services;
pub mod utils;

#[cfg(feature = "zeroconf-support")]
pub use zeroconf;

/// A new-type wrapper for socket ports.
#[derive(Clone, Copy, Debug, Deref, Display, PartialEq, Eq, Hash, From, PartialOrd, Ord)]
pub struct SocketPort(u16);

#[cfg(test)]
mod tests {
    #[allow(unused)]
    use super::*;
}
