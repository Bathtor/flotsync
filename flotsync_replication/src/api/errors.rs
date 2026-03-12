use flotsync_core::member::Identifier;
use snafu::prelude::*;
use std::error::Error;

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
pub enum RowProviderError {
    #[snafu(display("Row provider failed: {source}"))]
    ProviderExternal {
        source: Box<dyn Error + Send + Sync + 'static>,
    },
}

#[derive(Debug, Snafu)]
pub enum ListenerError {
    #[snafu(display("Listener rejected event: {message}"))]
    Rejected { message: String },
    #[snafu(display("Listener failed: {source}"))]
    ListenerExternal {
        source: Box<dyn Error + Send + Sync + 'static>,
    },
}

#[derive(Debug, Snafu)]
pub enum ApiError {
    #[snafu(display("Replication API operation failed: {source}"))]
    ApiExternal {
        source: Box<dyn Error + Send + Sync + 'static>,
    },
}

#[derive(Debug, Snafu)]
pub enum LoadError {
    #[snafu(display("Failed to load replication for application '{application_id}': {source}"))]
    Runtime {
        application_id: Identifier,
        source: Box<dyn Error + Send + Sync + 'static>,
    },
    #[snafu(display("Replication runtime is not available for application '{application_id}'."))]
    Unavailable { application_id: Identifier },
}

impl LoadError {
    pub fn runtime<E>(application_id: Identifier, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::Runtime {
            application_id,
            source: Box::new(source),
        }
    }
}
