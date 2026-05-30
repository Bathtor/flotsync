use derive_more::{Deref, Display, From};

#[cfg(feature = "kompact-runtime")]
pub use kompact;
pub use uuid;
/// Kompact configuration keys consumed by custom UDP discovery components.
#[cfg(feature = "peer-announcement-via-kompact")]
pub mod config_keys {
    use kompact::{config::BooleanValue, kompact_config};

    kompact_config! {
        PEER_ANNOUNCEMENT_BIND_REUSE_ADDRESS,
        key = "flotsync.discovery.peer-announcement.bind-reuse-address",
        type = BooleanValue,
        default = true,
        doc = "Whether the custom UDP peer-announcement socket should opt into platform socket re-use options.",
        version = "0.1.0"
    }
}
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
