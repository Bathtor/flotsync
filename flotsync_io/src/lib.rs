//! Freeform network I/O skeleton for the upcoming mio-backed Kompact integration.

pub mod api;
pub mod driver;
pub mod errors;
pub mod framing;
pub mod kompact;
mod logging;
pub mod pool;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

/// Common imports for consumers of the flotsync_io API surface.
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
            EgressPool,
            EgressReservation,
            IngressBuffer,
            IngressPool,
            IoBufWriter,
            IoBufferConfig,
            IoBufferPools,
            IoCursor,
            IoLease,
            IoPoolConfig,
            PoolRequest,
        },
    };
}
