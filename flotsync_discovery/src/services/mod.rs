#[allow(unused)]
use crate::errors::*;
#[allow(unused)]
use snafu::prelude::*;

#[cfg(feature = "peer-announcement-via-kompact")]
mod peer_announcement;
#[cfg(feature = "peer-announcement-via-kompact")]
pub use peer_announcement::{
    PEER_ANNOUNCEMENT_DEFAULT_OPTIONS,
    PeerAnnouncementComponent,
    PeerAnnouncementStartupError,
    PeerAnnouncementStartupResult,
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
