//! Shared test helpers for `flotsync_io`.
//!
//! This module is public behind the `test-support` feature so other crates can reuse the same
//! probes, wait helpers, and captured logging setup that the crate's own tests use.

use crate::{
    config_keys,
    prelude::*,
    socket_support::{configure_bind_reuse, socket_domain},
};
use flotsync_utils::option_when;
use futures_util::FutureExt;
use kompact::{config_keys::system, default_components::install_manual_timer, prelude::*};
use socket2::{Protocol, SockAddr, Socket, Type};
use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    fmt::{Debug, Display},
    future::Future,
    io,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    pin::pin,
    process::Command,
    sync::{
        Arc,
        Condvar,
        LazyLock,
        Mutex,
        MutexGuard,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread::{self, ThreadId},
    time::{Duration, Instant},
};

pub use kompact::test_support::{build_test_kompact_system, init_test_logger};

/// Shared timeout used by the crate's synchronous test wait helpers.
pub const WAIT_TIMEOUT: Duration = Duration::from_secs(5);

/// Default poll cadence for eventually-style synchronous test waits.
pub const EVENTUALLY_POLL_INTERVAL: Duration = Duration::from_millis(1);

/// Retry cadence for the reserved-socket broker while waiting for the OS to
/// make more loopback sockets available.
pub const RESERVED_SOCKET_RETRY_INTERVAL: Duration = Duration::from_millis(10);

/// Maximum time one test will wait for its full reserved-socket request before
/// failing as a last-resort safety fuse.
pub const RESERVED_SOCKET_ACQUIRE_TIMEOUT: Duration = Duration::from_mins(3);

/// Maximum time one reservation request may observe no broker-local progress
/// before failing.
///
/// This is essentially the maximum time any individual test case is actually allowed to run,
/// not considering waiting on its reservations.
pub const RESERVED_SOCKET_NO_PROGRESS_TIMEOUT: Duration = Duration::from_secs(30);

/// Maximum attempts to acquire one hidden reservation socket directly from the
/// OS while still validating that the broker never tracks the same
/// `(kind, addr)` twice.
const RESERVED_SOCKET_BIND_ATTEMPTS: usize = 16;

mod driver;
mod probes;
mod socket_broker;
mod waits;

pub use driver::{
    assert_no_driver_event,
    kill_component,
    recv_until,
    start_component,
    wait_for_driver_event,
    wait_for_driver_request,
};
pub use probes::{
    BufferedReceiver,
    TcpListenerEventProbe,
    TcpSessionEventProbe,
    UdpObserver,
    UdpObserverMessage,
    UdpSendResultProbe,
};
pub use socket_broker::{
    ReservedSocketKind,
    ReservedSocketLease,
    bind_reserved_tcp_listener,
    bind_reserved_udp_socket,
    build_test_kompact_system_with,
    build_test_kompact_system_with_manual_timer,
    enable_bind_reuse_address,
    reserve_sockets,
    set_test_system_label,
};
pub use waits::{
    assert_never,
    eventually,
    eventually_component_state,
    eventually_value,
    eventually_value_with_poll,
    localhost,
    wait_for_future,
};
