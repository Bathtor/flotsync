//! Runtime-host configuration and startup/control error types.

use super::*;

/// Startup and shutdown failures for the internal delivery runtime host.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub(crate) enum RuntimeHostError {
    #[snafu(display("Failed to build Kompact system for the replication runtime: {source}"))]
    BuildSystem {
        #[snafu(source(from(KompactError, RuntimeBuildSystemError::from)))]
        source: RuntimeBuildSystemError,
    },
    #[snafu(display("Invalid replication runtime host config at {key}: {message}"))]
    InvalidConfig { key: &'static str, message: String },
    #[snafu(display("Failed to start runtime component '{component}': {source}"))]
    StartComponent {
        component: &'static str,
        source: RuntimeControlError,
    },
    #[snafu(display("Failed to stop runtime component '{component}': {source}"))]
    StopComponent {
        component: &'static str,
        source: RuntimeControlError,
    },
    #[snafu(display("Failed to shut down replication runtime system: {source}"))]
    ShutdownSystem { source: RuntimeControlError },
    #[snafu(display("Failed to connect runtime bridge UDP to '{component}': {message}"))]
    ConnectUdp {
        component: &'static str,
        message: String,
    },
    #[snafu(display("Failed to bind the runtime local delivery endpoint: {source}"))]
    BindLocalEndpoint { source: RuntimeControlError },
    #[snafu(display(
        "Failed to bind the runtime local delivery endpoint: {source}; configured_bind_addr={configured_bind_addr}; system_label={system_label}"
    ))]
    BindLocalEndpointInSystem {
        source: RuntimeControlError,
        configured_bind_addr: SocketAddr,
        system_label: String,
    },
    #[snafu(display("Failed to configure route-establishment manual watches: {source}"))]
    ConfigureRouteEstablishmentWatches { source: RuntimeControlError },
    #[snafu(display("Invalid route-establishment manual watches: {source}"))]
    InvalidRouteEstablishmentManualWatches { source: ManualRouteWatchError },
    #[snafu(display("Failed to connect runtime components for {link}: {message}"))]
    ConnectComponents { link: &'static str, message: String },
}

/// Send-safe wrapper around Kompact build failures.
///
/// This exists because `KompactError::Other` stores `Box<dyn Error>` without
/// `Send + Sync`, while replication load errors eventually box runtime-host
/// failures into the public send-safe `BoxError` shape.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub(crate) enum RuntimeBuildSystemError {
    #[snafu(display("Kompact system state was poisoned"))]
    Poisoned,
    #[snafu(display("Kompact config loading failed: {source}"))]
    ConfigLoading { source: ConfigLoadingError },
    #[snafu(display("Kompact config lookup failed: {source}"))]
    Config { source: ConfigError },
    #[snafu(display("Kompact reported an opaque build error: {message}"))]
    Other { message: String },
}

impl From<KompactError> for RuntimeBuildSystemError {
    fn from(error: KompactError) -> Self {
        match error {
            KompactError::Poisoned => Self::Poisoned,
            KompactError::ConfigLoadingError(source) => Self::ConfigLoading { source },
            KompactError::ConfigError(source) => Self::Config { source },
            KompactError::Other(source) => Self::Other {
                message: source.to_string(),
            },
        }
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub(crate) enum RuntimeControlError {
    #[snafu(display("timed out: {source}"))]
    Timeout { source: TimeoutError },
    #[snafu(display("Kompact control future failed: {source}"))]
    ControlFuture { source: BoxError },
    #[snafu(display("{message}"))]
    Failed { message: String },
}

impl RuntimeControlError {
    pub(in crate::runtime::host) fn failed(message: impl Into<String>) -> Self {
        Self::Failed {
            message: message.into(),
        }
    }
}

impl From<TimeoutError> for RuntimeControlError {
    fn from(source: TimeoutError) -> Self {
        Self::Timeout { source }
    }
}

#[derive(Clone, Copy, Debug)]
pub(in crate::runtime::host) struct DeliveryRuntimeHostConfig {
    pub(in crate::runtime::host) control_timeout: Duration,
    pub(in crate::runtime::host) summary_request_timeout: Duration,
    pub(in crate::runtime::host) local_endpoint_bind_addr: SocketAddr,
    pub(in crate::runtime::host) peer_announcement_bind_addr: SocketAddr,
}

impl DeliveryRuntimeHostConfig {
    pub(in crate::runtime::host) fn from_system_config(
        system: &KompactSystem,
    ) -> Result<Self, RuntimeHostError> {
        let control_timeout = system
            .config()
            .read_or_default(&config_keys::CONTROL_TIMEOUT)
            .map_err(|error| RuntimeHostError::InvalidConfig {
                key: config_keys::CONTROL_TIMEOUT.key,
                message: error.to_string(),
            })?;
        let summary_request_timeout = system
            .config()
            .read_or_default(&config_keys::SUMMARY_REQUEST_TIMEOUT)
            .map_err(|error| RuntimeHostError::InvalidConfig {
                key: config_keys::SUMMARY_REQUEST_TIMEOUT.key,
                message: error.to_string(),
            })?;
        let local_endpoint_bind_addr = system
            .config()
            .read_or_default(&config_keys::LOCAL_ENDPOINT_BIND_ADDR)
            .map_err(|error| RuntimeHostError::InvalidConfig {
                key: config_keys::LOCAL_ENDPOINT_BIND_ADDR.key,
                message: error.to_string(),
            })?;
        let local_endpoint_bind_addr =
            local_endpoint_bind_addr
                .parse()
                .map_err(
                    |error: std::net::AddrParseError| RuntimeHostError::InvalidConfig {
                        key: config_keys::LOCAL_ENDPOINT_BIND_ADDR.key,
                        message: error.to_string(),
                    },
                )?;
        let peer_announcement_bind_addr = system
            .config()
            .read_or_default(&discovery_config_keys::PEER_ANNOUNCEMENT_BIND_ADDR)
            .map_err(|error| RuntimeHostError::InvalidConfig {
                key: discovery_config_keys::PEER_ANNOUNCEMENT_BIND_ADDR.key,
                message: error.to_string(),
            })?;
        let peer_announcement_bind_addr =
            peer_announcement_bind_addr
                .parse()
                .map_err(
                    |error: std::net::AddrParseError| RuntimeHostError::InvalidConfig {
                        key: discovery_config_keys::PEER_ANNOUNCEMENT_BIND_ADDR.key,
                        message: error.to_string(),
                    },
                )?;
        Ok(Self {
            control_timeout,
            summary_request_timeout,
            local_endpoint_bind_addr,
            peer_announcement_bind_addr,
        })
    }
}
