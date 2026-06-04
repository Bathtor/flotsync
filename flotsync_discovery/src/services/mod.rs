#[cfg(feature = "zeroconf-support")]
#[allow(unused)]
use crate::errors::{Result, ZeroconfSnafu};
#[cfg(feature = "zeroconf-support")]
#[allow(unused)]
use snafu::prelude::*;

#[cfg(feature = "peer-announcement-via-kompact")]
mod peer_announcement;
#[cfg(feature = "peer-announcement-via-kompact")]
pub use peer_announcement::{
    Options as PeerAnnouncementOptions,
    PEER_ANNOUNCEMENT_DEFAULT_OPTIONS,
    PeerAnnouncementComponent,
    PeerAnnouncementMessage,
    PeerAnnouncementRoute,
    PeerAnnouncementSocketMaintenance,
    PeerAnnouncementStartupError,
    PeerAnnouncementStartupResult,
    peer_announcement_bind_options_from_config,
    peer_announcement_startup_signal,
};

#[cfg(feature = "zeroconf-support")]
mod mdns_announcement;
#[cfg(feature = "zeroconf-via-kompact")]
pub use mdns_announcement::{
    MDNS_ANNOUNCEMENT_SERVICE_DEFAULT_OPTIONS,
    MdnsAnnouncementComponent,
    MdnsAnnouncementMessage,
    MdnsAnnouncementMessages,
};

#[cfg(feature = "zeroconf-support")]
mod mdns_browser;
