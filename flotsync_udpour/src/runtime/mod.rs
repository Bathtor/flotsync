//! Kompact runtime adapter for the `UDPour` protocol.
//!
//! This module binds the pure sender/receiver state machines to one shared UDP
//! socket from `flotsync_io`, owns the mapping from logical multipart messages
//! to concrete UDP sends, and surfaces route-transport failures at the
//! component boundary.

#[cfg(test)]
use crate::types::UDPourHeader;
use crate::{
    codec::{FRAME_HEADER_LEN, decode_frame, encoded_frame_len},
    receiver::{ReceiverAction, ReceiverConfig, ReceiverMachine},
    roaring_helpers::{MIN_ENCODED_NON_EMPTY_BITMAP_LEN, RoaringBitmapError},
    sender::{SenderAction, SenderConfig, SenderError, SenderMachine},
    types::{
        AckFrame,
        MessageId,
        NeedPartsFrame,
        NoLongerAvailableFrame,
        PayloadFrame,
        UDPourFrame,
    },
    wire::EncodeToBufMut,
};
use flotsync_io::prelude::*;
use flotsync_utils::{ResultExt as _, kompact_config::ConfigReadExt as _};
use kompact::{
    config::{DurationValue, UsizeValue},
    kompact_config,
    prelude::*,
};
use snafu::prelude::*;
use std::{
    collections::{HashMap, VecDeque},
    fmt,
    net::SocketAddr,
    time::{Duration, Instant},
};

mod component;
mod public;
mod queue;

#[cfg(test)]
mod tests;

pub use component::{UDPourComponent, UDPourComponentMessage};
pub use public::{
    UDPourConfig,
    UDPourConfigError,
    UDPourDeliver,
    UDPourEncodeFailure,
    UDPourPort,
    UDPourSend,
    UDPourSendFailureReason,
    UDPourStateFailure,
    UDPourSubmitResult,
    config_keys,
};

#[cfg(test)]
pub(crate) use component::{ActiveTimerSnapshot, RuntimeTimerSnapshot};
