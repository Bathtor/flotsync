use derive_more::{Deref, Display, From};

#[cfg(feature = "kompact-runtime")]
pub use kompact;
pub use uuid;
/// Kompact configuration keys consumed by route establishment components.
#[cfg(feature = "peer-announcement-via-kompact")]
pub mod config_keys {
    use kompact::{
        config::{BooleanValue, DurationValue},
        kompact_config,
    };
    use std::time::Duration;

    kompact_config! {
        PEER_ANNOUNCEMENT_BIND_REUSE_ADDRESS,
        key = "flotsync.discovery.peer-announcement.bind-reuse-address",
        type = BooleanValue,
        default = true,
        doc = "Whether peer-announcement sockets should opt into platform socket re-use options.",
        version = "0.1.0"
    }

    kompact_config! {
        ROUTE_ESTABLISHMENT_PROBE_TIMEOUT,
        key = "flotsync.discovery.route-establishment.probe-timeout",
        type = DurationValue,
        default = Duration::from_secs(2),
        doc = "Maximum wait for a route establishment introduction response before a probe is considered stale.",
        version = "0.1.0"
    }

    kompact_config! {
        ROUTE_ESTABLISHMENT_REACHABLE_LEASE,
        key = "flotsync.discovery.route-establishment.reachable-lease",
        type = DurationValue,
        default = Duration::from_secs(30),
        doc = "Time for which one verified route remains published before refresh is required.",
        version = "0.1.0"
    }
}
pub mod errors;
#[cfg(feature = "kompact-runtime")]
pub mod kompact_fsm;
pub mod services;
pub mod utils;

#[cfg(feature = "peer-announcement-via-kompact")]
pub mod route_establishment;
#[cfg(feature = "peer-announcement-via-kompact")]
pub mod route_publication;

#[cfg(feature = "zeroconf-support")]
pub use zeroconf;

/// A new-type wrapper for socket ports.
#[derive(Clone, Copy, Debug, Deref, Display, PartialEq, Eq, Hash, From, PartialOrd, Ord)]
pub struct SocketPort(u16);

/// Default UDP port for Flotsync peer discovery.
pub const DEFAULT_DISCOVERY_PORT: SocketPort = SocketPort(52156);

pub mod protocol;

#[cfg(test)]
mod tests {
    #[allow(unused)]
    use super::*;
}
