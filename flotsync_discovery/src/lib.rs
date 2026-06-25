//! Discovery services for finding peers and publishing local peer announcements.

use derive_more::{Deref, Display, From};

#[cfg(feature = "kompact-runtime")]
pub use kompact;
pub use uuid;
/// Kompact configuration keys consumed by peer-announcement components.
#[cfg(feature = "peer-announcement-via-kompact")]
pub mod config_keys {
    use crate::DEFAULT_DISCOVERY_PORT;
    use kompact::{
        config::{BooleanValue, StringValue},
        kompact_config,
    };

    fn default_peer_announcement_bind_addr() -> String {
        format!("0.0.0.0:{DEFAULT_DISCOVERY_PORT}")
    }

    kompact_config! {
        PEER_ANNOUNCEMENT_BIND_ADDR,
        key = "flotsync.discovery.peer-announcement.bind-addr",
        type = StringValue,
        default = default_peer_announcement_bind_addr(),
        doc = "Local UDP socket address used by peer-announcement senders and observers. This controls where peer-announcement components bind or match an observed socket; it does not control advertised delivery routes or the broadcast target port.",
        version = "0.1.0"
    }

    kompact_config! {
        PEER_ANNOUNCEMENT_BIND_REUSE_ADDRESS,
        key = "flotsync.discovery.peer-announcement.bind-reuse-address",
        type = BooleanValue,
        default = true,
        doc = "Whether peer-announcement sockets should opt into platform socket re-use options.",
        version = "0.1.0"
    }
}
pub mod endpoint_selection;
pub mod errors;
pub mod protocol;
pub mod services;
pub mod utils;

#[cfg(feature = "zeroconf-support")]
pub use zeroconf;

/// A new-type wrapper for socket ports.
#[derive(Clone, Copy, Debug, Deref, Display, PartialEq, Eq, Hash, From, PartialOrd, Ord)]
pub struct SocketPort(u16);

/// Default UDP port for Flotsync peer discovery.
pub const DEFAULT_DISCOVERY_PORT: SocketPort = SocketPort(52156);

#[cfg(test)]
mod tests {
    #[allow(unused)]
    use super::*;
}
