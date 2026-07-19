use crate::{
    ReceiverConfig,
    SenderConfig,
    codec::{decode_frame, encode_frame},
    config_keys,
    runtime::{
        ActiveTimerSnapshot,
        RuntimeTimerSnapshot,
        UDPourComponent,
        UDPourComponentMessage,
        UDPourConfig,
        UDPourDeliver,
        UDPourPort,
        UDPourSend,
        UDPourSendFailureReason,
        UDPourSubmitResult,
    },
    types::{
        AckFrame,
        Checksum,
        FrameType,
        MessageId,
        NeedPartsFrame,
        PROTOCOL_VERSION,
        PartCount,
        PartNumber,
        PayloadFrame,
        UDPourFrame,
        UDPourHeader,
    },
};
use bytes::Bytes;
use flotsync_io::{
    prelude::{
        DriverConfig,
        IoBridge,
        IoBridgeHandle,
        IoDriverComponent,
        IoPayload,
        SendFailureReason,
        SocketId,
        UdpBindOptions,
        UdpIndication,
        UdpLocalBind,
        UdpOpenRequestId,
        UdpPort,
        UdpRequest,
        UdpSendResult,
    },
    test_support::{
        BufferedReceiver,
        ReservedSocketKind,
        ReservedSocketLease,
        UdpObserver,
        UdpObserverMessage,
        WAIT_TIMEOUT,
        build_test_kompact_system_with,
        build_test_kompact_system_with_manual_timer,
        enable_bind_reuse_address,
        eventually_component_state,
        kill_component,
        localhost,
        reserve_sockets,
        start_component,
    },
};
use kompact::{prelude::*, timer::ManualTimer as KompactManualTimer};
use roaring::RoaringBitmap;
use std::{
    cmp::Reverse,
    net::SocketAddr,
    num::NonZeroUsize,
    sync::{Arc, mpsc},
    time::{Duration, Instant},
};

mod basic;
mod frames;
mod harness;
mod resilience;
mod support;
mod timing;
