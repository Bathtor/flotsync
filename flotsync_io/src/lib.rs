//! Freeform network I/O skeleton for the upcoming mio-backed Kompact integration.

pub mod api;
pub mod driver;
pub mod errors;
pub mod kompact;
pub mod pool;
#[cfg(test)]
mod test_support;

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
            IoBridge,
            IoBridgeHandle,
            IoDriverComponent,
            OpenFailureReason,
            OpenTcpSession,
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
