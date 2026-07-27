//! Application-facing diagnostics for a running Flotsync runtime.

use super::{ApiError, BoxFuture};

pub use flotsync_routes::route_establishment::{
    ConfiguredRouteMembers,
    DiscoveryRoute,
    RouteDiagnostic,
    RouteDiagnosticPhase,
    RouteEstablishmentDiagnostics,
};

/// Read-only operational diagnostics separated from replication control operations.
pub trait FlotsyncDiagnostics: Send + Sync {
    /// Return one point-in-time snapshot of peer-route establishment.
    ///
    /// Socket addresses are local diagnostic information. Applications should sanitise them
    /// before exporting this snapshot into telemetry or other externally visible reports.
    ///
    /// # Errors
    ///
    /// Returns [`ApiError::RuntimeUnavailable`] after shutdown or when route establishment can no
    /// longer answer the query. Returns another [`ApiError`] when runtime lifecycle access fails.
    fn peer_routes(&self) -> BoxFuture<'_, Result<RouteEstablishmentDiagnostics, ApiError>>;
}
