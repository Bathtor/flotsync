//! Route establishment runtime components.

use flotsync_core::MemberIdentity;
use flotsync_security::{FrameSignature, KeyFingerprint, PublicKeyBundle};
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

/// Future returned by asynchronous discovery key-material availability checks.
pub type DiscoveryKeyMaterialStatusFuture<'a> =
    BoxFuture<'a, Result<DiscoveryKeyMaterialStatus, BoxError>>;

/// Local availability of exact discovery key material.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiscoveryKeyMaterialStatus {
    /// Exact public key material is stored and usable for signature verification.
    Available,
    /// Exact public key material is not stored locally.
    Missing,
    /// Exact public key material should not be acquired or used, for example because it is blocked.
    Unusable,
}

/// Security operations required by endpoint discovery and route establishment protocols.
pub trait DiscoveryCredentials: Send + Sync {
    /// Return the fingerprint of the local public key bundle used for discovery signatures.
    fn local_discovery_key_fingerprint(&self) -> KeyFingerprint;

    /// Return the local public key bundle used for direct discovery key-material responses.
    fn local_discovery_public_key_bundle(&self) -> PublicKeyBundle;

    /// Sign one exact encoded endpoint-discovery payload.
    ///
    /// # Errors
    ///
    /// Returns an implementation-specific error when the local signing key is unavailable or
    /// signing fails.
    fn sign_discovery_payload(&self, payload: &[u8]) -> Result<FrameSignature, BoxError>;

    /// Return local availability of exact public key material for discovery verification.
    ///
    /// # Errors
    ///
    /// The returned future resolves to an implementation-specific error when the local security
    /// store cannot be queried.
    fn discovery_key_material_status<'a>(
        &'a self,
        member: &'a MemberIdentity,
        key_fingerprint: KeyFingerprint,
    ) -> DiscoveryKeyMaterialStatusFuture<'a>;

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

    /// Ensure one directly acquired public key bundle as candidate discovery key material.
    ///
    /// Implementations should make this atomic with their local security state: if the exact
    /// member/fingerprint material is already present or unusable, this method should no-op
    /// successfully instead of racing a separate status check.
    ///
    /// # Errors
    ///
    /// The returned future resolves to an implementation-specific error when the bundle cannot be
    /// checked or stored.
    fn ensure_discovery_public_key_bundle<'a>(
        &'a self,
        member: &'a MemberIdentity,
        bundle: PublicKeyBundle,
    ) -> DiscoveryCredentialFuture<'a>;
}

#[cfg(test)]
mod tests;
