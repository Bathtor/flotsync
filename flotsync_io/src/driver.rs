//! Skeleton types for the future single-threaded mio driver core.

use crate::api::{TcpCommand, TcpEvent, UdpCommand, UdpEvent};

/// Driver-owned registration token space reserved for mio resources.
pub type DriverToken = ::mio::Token;

/// Placeholder configuration surface for the future mio driver.
#[derive(Clone, Debug, Default)]
pub struct DriverConfig;

/// Shared driver handle that will later own the mio poll loop and socket tables.
#[derive(Debug, Default)]
pub struct IoDriver {
    pub config: DriverConfig,
}

/// Commands entering the mio-backed driver core.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum DriverCommand {
    /// Forwards a TCP command from a bridge or client-facing adapter into the shared driver.
    Tcp(TcpCommand),
    /// Forwards a UDP command from a bridge or client-facing adapter into the shared driver.
    Udp(UdpCommand),
}

/// Events emitted by the mio-backed driver core.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum DriverEvent {
    /// Emits a TCP-facing event produced by the shared driver.
    Tcp(TcpEvent),
    /// Emits a UDP-facing event produced by the shared driver.
    Udp(UdpEvent),
}
