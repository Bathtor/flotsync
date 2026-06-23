//! Route establishment runtime components.

use crate::{
    protocol::DiscoveryRoute,
    services::{
        PeerAnnouncementComponent,
        PeerAnnouncementOptions,
        PeerAnnouncementSocketMaintenance,
    },
};
use flotsync_core::MemberIdentity;
use flotsync_security::FrameSignature;
use flotsync_utils::{BoxError, BoxFuture};
use kompact::prelude::{Never, Port};
use snafu::Snafu;
use uuid::Uuid;

mod component;
mod config;
mod observation;
mod state;
mod wire;

pub use component::{RouteEstablishmentComponent, RouteEstablishmentMessage};
pub use config::{ConcreteRoutes, RouteEstablishmentConfig, RouteEstablishmentConfigError};
pub use observation::PeerAnnouncementObservationComponent;
pub use state::{ManualRouteWatchError, WatchedRoute};
pub use wire::{RouteEstablishmentError, frame_signature_from_wire};

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
    fn verify_discovery_claim_payload<'a>(
        &'a self,
        member: &'a MemberIdentity,
        payload: &'a [u8],
        signature: &'a FrameSignature,
    ) -> DiscoveryCredentialFuture<'a>;
}

/// Invalid local peer-announcement component setup.
#[derive(Clone, Debug, PartialEq, Eq, Snafu)]
pub enum PeerAnnouncementSetupError {
    /// The component maintaining the peer-announcement socket and the local observer disagree on
    /// the UDP port that identifies the shared peer-announcement socket.
    #[snafu(display(
        "peer-announcement sender bind port {sender_bind_port} does not match observer bind port {observer_bind_port}"
    ))]
    BindPortMismatch {
        sender_bind_port: u16,
        observer_bind_port: u16,
    },
}

/// One decoded plaintext peer announcement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerAnnouncementObserved {
    /// Running process id for the announcing peer instance.
    pub instance_id: Uuid,
    /// Reachability endpoints advertised by this peer instance.
    pub routes: Vec<DiscoveryRoute>,
}

/// Port used by announcement protocols to publish decoded peer announcements.
#[derive(Clone, Copy, Debug, Default)]
pub struct PeerAnnouncementObservationPort;

impl Port for PeerAnnouncementObservationPort {
    type Request = Never;
    type Indication = PeerAnnouncementObserved;
}

/// Component definitions for a runtime that both sends and observes peer announcements.
///
/// The caller must connect both components to the same IO `UdpPort` before starting the
/// maintaining component so the observer can infer the socket id from the shared bind indication.
pub struct PeerAnnouncementAndObservationComponents {
    /// Component definition that sends plaintext `Peer` announcements and maintains the socket.
    pub announcement: PeerAnnouncementComponent,
    /// Component definition that observes plaintext `Peer` announcements from the shared socket.
    pub observation: PeerAnnouncementObservationComponent,
}

/// Build the announcement-only setup. The sender maintains the peer-announcement socket.
#[must_use]
pub fn peer_announcement_only_component(
    options: PeerAnnouncementOptions,
) -> PeerAnnouncementComponent {
    PeerAnnouncementComponent::with_options(
        options.with_socket_maintenance(PeerAnnouncementSocketMaintenance::Maintain),
    )
}

/// Build the observation-only setup. The observer maintains the peer-announcement socket.
#[must_use]
pub fn peer_announcement_observation_only_component(
    config: RouteEstablishmentConfig,
) -> PeerAnnouncementObservationComponent {
    PeerAnnouncementObservationComponent::with_socket_maintenance(
        config,
        PeerAnnouncementSocketMaintenance::Maintain,
    )
}

/// Build the combined peer-announcement setup. The sender maintains and the observer follows.
///
/// # Errors
///
/// Returns [`PeerAnnouncementSetupError`] when the sender and observer configuration would not
/// refer to the same peer-announcement bind port.
pub fn peer_announcement_and_observation_components(
    options: PeerAnnouncementOptions,
    config: RouteEstablishmentConfig,
) -> Result<PeerAnnouncementAndObservationComponents, PeerAnnouncementSetupError> {
    let sender_bind_port = options.socket_bind_addr().port();
    let observer_bind_port = config.peer_announcement_bind_addr.port();
    if sender_bind_port != observer_bind_port {
        return Err(PeerAnnouncementSetupError::BindPortMismatch {
            sender_bind_port,
            observer_bind_port,
        });
    }
    Ok(PeerAnnouncementAndObservationComponents {
        announcement: PeerAnnouncementComponent::with_options(
            options.with_socket_maintenance(PeerAnnouncementSocketMaintenance::Maintain),
        ),
        observation: PeerAnnouncementObservationComponent::with_socket_maintenance(
            config,
            PeerAnnouncementSocketMaintenance::Observe,
        ),
    })
}

#[cfg(test)]
mod tests;
