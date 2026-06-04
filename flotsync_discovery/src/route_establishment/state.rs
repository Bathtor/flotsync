use flotsync_core::{MemberIdentity, member::TrieSet};
use flotsync_messages::discovery as discovery_proto;
use flotsync_security::FrameSignature;
use kompact::prelude::ScheduledTimer;
use std::net::SocketAddr;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RouteProbeKey {
    /// Peer process instance that advertised this route.
    pub instance_id: Uuid,
    /// Exact endpoint being probed for reachability.
    pub route: SocketAddr,
}

/// Mutable verification state for one advertised peer-instance route.
pub enum PeerRouteState {
    /// Route has been announced but not verified by a signed introduction.
    Known,
    /// Route has an active introduction probe.
    Probing { pending_probe: PendingProbe },
    /// Route has a verified claim and is published until its lease expires.
    Reachable {
        reachable_lease: ScheduledTimer,
        published_members: TrieSet,
    },
    /// Route previously failed or expired and is not currently published.
    Stale,
}

impl PeerRouteState {
    /// Initial state for a route learned from a plaintext `Peer` announcement.
    pub const KNOWN: Self = Self::Known;

    /// Returns whether this route already has a scheduled probe or reachability timeout.
    pub fn has_active_timeout(&self) -> bool {
        matches!(self, Self::Probing { .. } | Self::Reachable { .. })
    }

    /// Return the active probe state, if this route is being probed.
    pub fn pending_probe(&self) -> Option<&PendingProbe> {
        match self {
            Self::Probing { pending_probe } => Some(pending_probe),
            Self::Known | Self::Reachable { .. } | Self::Stale => None,
        }
    }

    /// Return members currently published through this route without cloning the set.
    pub fn reachable_members(&self) -> Option<&TrieSet> {
        match self {
            Self::Reachable {
                published_members, ..
            } => Some(published_members),
            Self::Known | Self::Probing { .. } | Self::Stale => None,
        }
    }

    /// Move this route into probing state.
    ///
    /// Returns the previous probe or reachable-lease timer, if this transition replaced an
    /// already timed state. The caller must cancel that timer after ownership leaves the state.
    pub fn mark_probing(&mut self, pending_probe: PendingProbe) -> Option<ScheduledTimer> {
        match std::mem::replace(self, Self::Probing { pending_probe }) {
            Self::Known | Self::Stale => None,
            Self::Probing { pending_probe } => Some(pending_probe.timer),
            Self::Reachable {
                reachable_lease, ..
            } => Some(reachable_lease),
        }
    }

    /// Mark a matching probe as timed out.
    ///
    /// Returns `true` when this state contained an active probe matching both `nonce` and
    /// `expected_timer`, and the route was moved to [`Self::Stale`]. Returns `false` without
    /// changing state when the route is not probing or when either value does not match.
    pub fn expire_probe_if_matches(&mut self, nonce: Uuid, actual_timer: &ScheduledTimer) -> bool {
        let Self::Probing { pending_probe } = self else {
            return false;
        };
        if pending_probe.nonce != nonce || &pending_probe.timer != actual_timer {
            return false;
        }
        *self = Self::Stale;
        true
    }

    /// Move this route into reachable state.
    ///
    /// Returns the previous probe or reachable-lease timer, if this transition replaced an
    /// already timed state. The caller must cancel that timer after ownership leaves the state.
    pub fn mark_reachable(
        &mut self,
        reachable_lease: ScheduledTimer,
        published_members: TrieSet,
    ) -> Option<ScheduledTimer> {
        match std::mem::replace(
            self,
            Self::Reachable {
                reachable_lease,
                published_members,
            },
        ) {
            Self::Known | Self::Stale => None,
            Self::Probing { pending_probe } => Some(pending_probe.timer),
            Self::Reachable {
                reachable_lease, ..
            } => Some(reachable_lease),
        }
    }

    /// Expire a matching reachable lease.
    ///
    /// Returns `true` when this state contained a reachable route matching `expected_timer`, and
    /// the route was moved to [`Self::Stale`]. Returns `false` without changing state when the
    /// route is not reachable or when the timer does not match.
    pub fn expire_reachable_lease_if_matches(&mut self, actual_timer: &ScheduledTimer) -> bool {
        let Self::Reachable {
            reachable_lease, ..
        } = self
        else {
            return false;
        };
        if reachable_lease != actual_timer {
            return false;
        }
        *self = Self::Stale;
        true
    }

    /// Move this route into stale state.
    ///
    /// Returns the previous probe or reachable-lease timer, if this transition withdrew a timed
    /// state. The caller must cancel that timer after ownership leaves the state.
    pub fn mark_stale(&mut self) -> Option<ScheduledTimer> {
        match std::mem::replace(self, Self::Stale) {
            Self::Known | Self::Stale => None,
            Self::Probing { pending_probe } => Some(pending_probe.timer),
            Self::Reachable {
                reachable_lease, ..
            } => Some(reachable_lease),
        }
    }
}

/// Active introduction probe state.
pub struct PendingProbe {
    /// Receiver-generated challenge expected in the introduction response.
    pub nonce: Uuid,
    /// Timer that owns this probe attempt.
    pub timer: ScheduledTimer,
}

/// Introduction response after local probe matching but before trusted-key verification.
pub struct PartiallyVerifiedIntroduction {
    /// Active probe matched by the top-level introduction and each retained claim payload.
    pub key: RouteProbeKey,
    /// Claims whose structure and probe fields matched, but whose signatures are not yet trusted.
    pub claims: Vec<PendingClaimVerification>,
}

/// Signed claim material awaiting trusted-key verification.
pub struct PendingClaimVerification {
    /// Claimed member whose trusted key should verify this payload.
    pub member: MemberIdentity,
    /// Original signed claim, including the exact encoded payload covered by `signature`.
    pub claim: discovery_proto::SignedIntroductionClaim,
    /// Detached signature over `claim.claim_payload`.
    pub signature: FrameSignature,
}
