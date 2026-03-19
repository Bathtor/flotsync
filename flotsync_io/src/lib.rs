//! Freeform network I/O skeleton for the upcoming mio-backed Kompact integration.

pub mod api;
pub mod driver;
pub mod errors;
pub mod kompact;
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
            UdpCommand,
            UdpEvent,
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
        kompact::{
            ConfigureFailureReason,
            IoBridge,
            IoBridgeHandle,
            IoDriverComponent,
            OpenFailureReason,
            OpenTcpListener,
            OpenTcpSession,
            PendingTcpSession,
            TcpListenerEvent,
            TcpListenerRef,
            TcpListenerRequest,
            TcpSessionEvent,
            TcpSessionRef,
            TcpSessionRequest,
            UdpIndication,
            UdpPort,
            UdpRequest,
            UdpSendResult,
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
