use flotsync_messages::protobuf;
use snafu::prelude::*;
use tokio::task::JoinError;

pub type Result<T> = std::result::Result<T, ServiceError>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum ServiceError {
    #[snafu(display("A channel was already dropped when we tried to access it here: {loc}"))]
    ChannelDropped {
        #[snafu(implicit)]
        loc: snafu::Location,
    },
    #[snafu(display("An error occurred interacting with the system: {source}"))]
    Io { source: std::io::Error },
    #[snafu(display("A thread panicked"))]
    ThreadJoin,
    #[snafu(display("A task failed to shutdown properly: {source}"))]
    Join { source: JoinError },
    #[snafu(display("Error during protobuf operations: {source}"))]
    Proto { source: protobuf::Error },
    #[snafu(display("Error with a zeroconf service operation: {source}"))]
    Zeroconf {
        source: zeroconf_tokio::error::Error,
    },
    #[snafu(display("External service error: {}", source))]
    External {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

impl ServiceError {
    pub fn external<E>(e: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        ServiceError::External {
            source: Box::new(e),
        }
    }
}
