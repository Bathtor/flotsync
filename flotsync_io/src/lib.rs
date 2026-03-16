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
            TcpPort,
            TransmissionId,
            UdpCommand,
            UdpEvent,
            UdpPort,
        },
        driver::{DriverCommand, DriverConfig, DriverEvent, DriverRequest, DriverToken, IoDriver},
        errors::{Error, Result},
        kompact::{IoBridge, IoDriverComponent},
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
