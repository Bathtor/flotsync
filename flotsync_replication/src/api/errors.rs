use flotsync_core::member::Identifier;
use snafu::prelude::*;
use std::error::Error;

pub type BoxError = Box<dyn Error + Send + Sync + 'static>;
pub type ApiResult<T> = Result<T, ApiError>;

#[derive(Debug, Snafu)]
pub enum DatasetIdError {
    #[snafu(display("Dataset identifier must not be empty."))]
    Empty,
    #[snafu(display(
        "Dataset identifier '{value}' has an invalid first character. Use [A-Za-z_]."
    ))]
    InvalidStartCharacter { value: String },
    #[snafu(display(
        "Dataset identifier '{value}' contains invalid character '{character}' at byte index {index}. Only [A-Za-z0-9_] are allowed."
    ))]
    InvalidCharacter {
        value: String,
        index: usize,
        character: char,
    },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum RowProviderError {
    #[snafu(display("Row provider failed: {source}"))]
    ProviderExternal { source: BoxError },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum ListenerError {
    #[snafu(display("Listener rejected event: {message}"))]
    Rejected { message: String },
    #[snafu(display("Listener failed: {source}"))]
    ListenerExternal { source: BoxError },
}

impl From<BoxError> for ListenerError {
    fn from(source: BoxError) -> Self {
        Self::ListenerExternal { source }
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum ApiError {
    #[snafu(display("Replication API operation failed: {source}"))]
    ApiExternal { source: BoxError },
    #[snafu(display("Replication runtime component became unavailable."))]
    RuntimeUnavailable,
    #[snafu(display("Replication runtime operation '{operation}' is not implemented yet."))]
    UnsupportedOperation { operation: &'static str },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum StoreError {
    #[snafu(display("Replication store failed: {source}"))]
    StoreExternal { source: BoxError },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum LoadError {
    #[snafu(display("Failed to load replication for application '{application_id}': {source}"))]
    Runtime {
        application_id: Identifier,
        source: BoxError,
    },
    #[snafu(display("Replication runtime is not available for application '{application_id}'."))]
    Unavailable { application_id: Identifier },
}
