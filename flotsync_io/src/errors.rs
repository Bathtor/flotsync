//! Error surface for flotsync_io.

use snafu::Snafu;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[non_exhaustive]
pub enum Error {
    #[snafu(display("the mio-backed flotsync_io runtime is not implemented yet"))]
    Unimplemented,
}
