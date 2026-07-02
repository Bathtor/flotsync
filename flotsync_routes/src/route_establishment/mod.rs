//! Route establishment runtime components.

use flotsync_core::MemberIdentity;
use flotsync_security::{FrameSignature, KeyFingerprint};
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
    /// Return the fingerprint of the local public key bundle used for discovery signatures.
    fn local_discovery_key_fingerprint(&self) -> KeyFingerprint;

    /// Sign one exact encoded discovery claim payload.
    ///
    /// # Errors
    ///
    /// Returns an implementation-specific error when the local signing key is unavailable or
    /// signing fails.
    fn sign_discovery_claim_payload(&self, payload: &[u8]) -> Result<FrameSignature, BoxError>;

    /// Verify one exact encoded discovery claim payload against the claimed member key.
    ///
    /// # Errors
    ///
    /// The returned future resolves to an implementation-specific error when the exact claimed
    /// member key material is unavailable or blocked, the signature is malformed, or the signature
    /// does not verify for the supplied payload.
    fn verify_discovery_claim_payload<'a>(
        &'a self,
        member: &'a MemberIdentity,
        key_fingerprint: KeyFingerprint,
        payload: &'a [u8],
        signature: &'a FrameSignature,
    ) -> DiscoveryCredentialFuture<'a>;

    /// Confirm that a verified discovery claim may publish a route for the claimed member.
    ///
    /// # Errors
    ///
    /// The returned future resolves to an implementation-specific error when local policy does not
    /// grant `MemberRoutePublication` authority to the exact member key.
    fn permit_member_route_publication<'a>(
        &'a self,
        member: &'a MemberIdentity,
        key_fingerprint: KeyFingerprint,
    ) -> DiscoveryCredentialFuture<'a>;
}

#[cfg(test)]
mod tests;
