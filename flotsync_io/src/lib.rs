//! Freeform network I/O skeleton for the upcoming mio-backed Kompact integration.

pub mod api;
pub mod driver;
pub mod errors;
pub mod kompact;

/// Common imports for consumers of the flotsync_io API surface.
pub mod prelude {
    pub use crate::{
        api::{
            CloseReason,
            ConnectionId,
            IoPayload,
            ListenerId,
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
        driver::{DriverCommand, DriverConfig, DriverEvent, DriverToken, IoDriver},
        errors::{Error, Result},
        kompact::{IoBridge, IoDriverComponent},
    };
}
