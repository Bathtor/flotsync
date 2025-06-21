use derive_more::{Deref, Display, From};

#[cfg(feature = "kompact")]
pub use kompact;
pub use uuid;
pub mod errors;
#[cfg(feature = "kompact")]
pub mod kompact_fsm;
pub mod services;
pub mod utils;

#[cfg(all(feature = "zeroconf", not(feature = "zeroconf-tokio")))]
pub use zeroconf;
#[cfg(feature = "zeroconf-tokio")]
pub use zeroconf_tokio as zeroconf;

/// A new-type wrapper for socket ports.
#[derive(Clone, Copy, Debug, Deref, Display, PartialEq, Eq, Hash, From, PartialOrd, Ord)]
pub struct SocketPort(u16);

#[cfg(test)]
mod tests {
    #[allow(unused)]
    use super::*;
}
