//! Route establishment runtime components.

use flotsync_core::MemberIdentity;
use flotsync_security::FrameSignature;
use flotsync_utils::{BoxError, BoxFuture};

mod component;
mod config;
mod state;
mod wire;

pub use component::{RouteEstablishmentComponent, RouteEstablishmentMessage};
pub use config::{ConcreteRoutes, RouteEstablishmentConfig, RouteEstablishmentConfigError};
pub use state::{ManualRouteWatchError, WatchedRoute};
pub use wire::RouteEstablishmentError;

/// Future returned by asynchronous discovery credential verification.
pub type DiscoveryCredentialFuture<'a> = BoxFuture<'a, Result<(), BoxError>>;

/// Security operations required by the route establishment protocol.
pub trait DiscoveryCredentials: Send + Sync {
    /// Sign one exact encoded discovery claim payload.
    ///
    /// # Errors
    ///
    /// Returns an implementation-specific error when the local signing key is unavailable or
    /// signing fails.
    fn sign_discovery_claim_payload(&self, payload: &[u8]) -> Result<FrameSignature, BoxError>;

    /// Verify one exact encoded discovery claim payload against the claimed member.
    ///
    /// # Errors
    ///
    /// The returned future resolves to an implementation-specific error when the claimed member's
    /// verification key is unavailable, the signature is malformed, or the signature does not
    /// verify for the supplied payload.
    fn verify_discovery_claim_payload<'a>(
        &'a self,
        member: &'a MemberIdentity,
        payload: &'a [u8],
        signature: &'a FrameSignature,
    ) -> DiscoveryCredentialFuture<'a>;
}

#[cfg(test)]
mod tests;
