//! Developer-facing socket helpers shared by `flotsync_io` and its tests.
//!
//! These functions are public so external harnesses and example smoke tests can
//! construct sockets with the same platform options as the driver itself. They
//! are primarily intended for tests and other developer tooling.

use socket2::{Domain, Socket};
use std::{io, net::SocketAddr};

/// Enables the platform socket re-use options used by the reservation-backed
/// test bind paths.
///
/// On Unix this enables both `SO_REUSEADDR` and `SO_REUSEPORT`, because the
/// reservation strategy keeps one socket bound while the real test subject
/// binds the same port.
///
/// # Errors
///
/// See `io::Error` for failure conditions.
pub fn configure_bind_reuse(socket: &Socket) -> io::Result<()> {
    socket.set_reuse_address(true)?;
    #[cfg(unix)]
    socket.set_reuse_port(true)?;
    Ok(())
}

/// Returns the socket domain that matches the supplied address family.
#[must_use]
pub fn socket_domain(addr: SocketAddr) -> Domain {
    match addr {
        SocketAddr::V4(_) => Domain::IPV4,
        SocketAddr::V6(_) => Domain::IPV6,
    }
}
