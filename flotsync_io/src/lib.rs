//! Freeform network I/O skeleton for the upcoming mio-backed Kompact integration.

pub mod api;
/// Kompact configuration keys consumed by `flotsync_io` components.
pub mod config_keys {
    use kompact::{config::BooleanValue, kompact_config};

    kompact_config! {
        BIND_REUSE_ADDRESS,
        key = "flotsync.io.bind-reuse-address",
        type = BooleanValue,
        default = false,
        doc = "Whether flotsync_io bind paths should opt into platform socket re-use options. This is intended for tests that coordinate reserved ports outside the driver.",
        version = "0.1.0"
    }
}
pub mod driver;
pub mod errors;
pub mod framing;
pub mod kompact;
mod logging;
pub mod pool;
pub mod socket_support;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

/// Common imports for consumers of the `flotsync_io` API surface.
pub mod prelude {
    pub use crate::{
        api::{
            CloseReason,
            ConnectionId,
            IoPayload,
            ListenerId,
            MAX_UDP_PAYLOAD_BYTES,
            SendFailureReason,
            SocketId,
            TcpCommand,
            TcpEvent,
            TransmissionId,
            UdpBindOptions,
            UdpCloseReason,
            UdpCommand,
            UdpEvent,
            UdpLocalBind,
            UdpSocketOption,
        },
        driver::{
            DriverCommand,
            DriverConfig,
            DriverEvent,
            DriverEventSink,
            DriverRequest,
            DriverToken,
            IoDriver,
        },
        errors::{Error, Result},
        framing::{BoundedCollector, CollectUntil},
        kompact::{
            ConfigureFailureReason,
            IoBridge,
            IoBridgeHandle,
            IoDriverComponent,
            IoRuntime,
            IoRuntimeError,
            OpenFailureReason,
            OpenTcpListener,
            OpenTcpSession,
            OpenedTcpListener,
            OpenedTcpSession,
            PendingTcpSession,
            TcpListenerEvent,
            TcpListenerRef,
            TcpListenerRequest,
            TcpSessionEvent,
            TcpSessionEventTarget,
            TcpSessionRef,
            TcpSessionRequest,
            UdpIndication,
            UdpOpenRequestId,
            UdpPort,
            UdpRequest,
            UdpSendResult,
            tagged_tcp_session_event_target,
        },
        pool::{
            EgressAsyncWriter,
            EgressPool,
            EgressReservation,
            EgressReservedWriter,
            IngressBuffer,
            IngressPool,
            IoBufferConfig,
            IoBufferPools,
            IoCursor,
            IoLease,
            IoPoolConfig,
            PayloadWriter,
            PoolRequest,
        },
    };
}
