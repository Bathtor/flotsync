//! Mutable route-establishment state and manual watch reconciliation.

use flotsync_core::{MemberIdentity, member::TrieSet};
use flotsync_discovery::protocol::DiscoveryRoute;
use flotsync_messages::discovery as discovery_proto;
use flotsync_security::FrameSignature;
use kompact::prelude::ScheduledTimer;
use snafu::Snafu;
use std::borrow::Cow;
use uuid::Uuid;

/// Manual route-establishment watch supplied by a local caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WatchedRoute {
    /// Route endpoint that should be probed through the route-establishment protocol.
    pub route: DiscoveryRoute,
    /// Optional member that must prove ownership of this route before publication.
    pub expected_member: Option<MemberIdentity>,
}

/// Mutable state for one watched route.
pub struct WatchedRouteState {
    /// Current reasons to keep probing or publishing this route.
    pub interest: RouteInterest,
    /// Current verification lifecycle for this route.
    pub verification: RouteVerificationState,
}

impl WatchedRouteState {
    /// Initial route state for a route with newly observed interest.
    pub const NEW: Self = Self {
        interest: RouteInterest::NONE,
        verification: RouteVerificationState::Known,
    };

    /// Return whether any source currently wants this route watched.
    pub fn should_watch(&self) -> bool {
        self.interest.should_watch()
    }

    /// Return whether this route already has a scheduled probe or reachability timeout.
    pub fn has_active_timeout(&self) -> bool {
        self.verification.has_active_timeout()
    }

    /// Return the active probe state, if this route is being probed.
    pub fn pending_probe(&self) -> Option<&PendingProbe> {
        self.verification.pending_probe()
    }

    /// Return members currently published through this route without cloning the set.
    pub fn reachable_members(&self) -> Option<&TrieSet> {
        self.verification.reachable_members()
    }

    /// Reconcile currently published members after route interest changes.
    ///
    /// This only narrows or withdraws verification state. It must not change the route's interest
    /// sources; callers still decide whether a route with no remaining interest should be staled.
    pub(super) fn reconcile_reachable_members_with_interest(
        &mut self,
    ) -> RouteInterestReconciliation {
        let Some(current_members) = self.reachable_members() else {
            return RouteInterestReconciliation::UNCHANGED;
        };
        let permitted_members = permitted_reachable_members(&self.interest, current_members);
        let Cow::Owned(permitted_members) = permitted_members else {
            // If the permitted set is just a reference to `current_members`,
            // then nothing has changed.
            return RouteInterestReconciliation::UNCHANGED;
        };
        if permitted_members.is_empty() {
            return RouteInterestReconciliation {
                requires_snapshot_changes: true,
                timer_to_cancel: self.verification.mark_stale(),
            };
        }
        self.verification
            .replace_reachable_members(permitted_members);
        RouteInterestReconciliation {
            requires_snapshot_changes: true,
            timer_to_cancel: None,
        }
    }
}

/// Interest sources for one watched route.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteInterest {
    /// Whether at least one plaintext `Peer` announcement named this route.
    pub peer_announced: bool,
    /// Manual member filter supplied through local route watch messages.
    pub manual: ManualMemberFilter,
}

impl RouteInterest {
    /// Empty route interest.
    pub const NONE: Self = Self {
        peer_announced: false,
        manual: ManualMemberFilter::None,
    };

    /// Return whether this route has any active interest source.
    pub fn should_watch(&self) -> bool {
        self.peer_announced || self.manual.should_watch()
    }

    /// Return whether this interest allows publishing a verified route for `member`.
    pub fn permits_member(&self, member: &MemberIdentity) -> bool {
        if self.manual.should_watch() {
            self.manual.permits_member(member)
        } else {
            self.peer_announced
        }
    }
}

/// Invalid manual route watch configuration.
#[derive(Clone, Debug, PartialEq, Eq, Snafu)]
pub enum ManualRouteWatchError {
    /// One route mixed constrained expected-member watches with an unconstrained watch.
    #[snafu(display(
        "manual route watches for {route:?} mix constrained and unconstrained member filters"
    ))]
    ConflictingMemberFilters {
        /// Route with conflicting manual watch filters.
        route: DiscoveryRoute,
    },
}

/// Manual member filter for a watched route.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum ManualMemberFilter {
    /// No manual route watch names this route.
    #[default]
    None,
    /// Manual watches accept any verified local-group member.
    Any,
    /// Manual watches accept exactly this one member.
    /// (This is a memory optimisation of [[`Self::Members`]],
    /// to avoid the large Trie allocation for the common case where only a single member is
    /// expected.)
    Member(MemberIdentity),
    /// Manual watches accept only this set of expected members.
    Members(TrieSet),
}

impl ManualMemberFilter {
    /// Return whether this filter represents at least one manual watch.
    pub fn should_watch(&self) -> bool {
        !matches!(self, Self::None)
    }

    /// Add one expected member constraint, rejecting constrained/unconstrained conflicts.
    ///
    /// Multiple constrained watches union their expected members. Multiple unconstrained watches
    /// are idempotent. Mixing both forms is rejected so one caller cannot silently widen another
    /// caller's member constraint for the same route.
    pub fn try_add_expected_member(
        &mut self,
        route: DiscoveryRoute,
        expected_member: Option<MemberIdentity>,
    ) -> Result<(), ManualRouteWatchError> {
        if let Some(member) = expected_member {
            match self {
                Self::None => {
                    *self = Self::Member(member);
                    Ok(())
                }
                Self::Any => Err(ManualRouteWatchError::ConflictingMemberFilters { route }),
                Self::Member(existing_member) => {
                    if existing_member != &member {
                        let mut members = TrieSet::new();
                        members.insert(existing_member.clone());
                        members.insert(member);
                        *self = Self::Members(members);
                    }
                    Ok(())
                }
                Self::Members(members) => {
                    members.insert(member);
                    Ok(())
                }
            }
        } else {
            match self {
                Self::Members(_) | Self::Member(_) => {
                    Err(ManualRouteWatchError::ConflictingMemberFilters { route })
                }
                Self::Any => Ok(()),
                Self::None => {
                    *self = Self::Any;
                    Ok(())
                }
            }
        }
    }

    /// Return whether this filter allows publishing a verified route for `member`.
    pub fn permits_member(&self, member: &MemberIdentity) -> bool {
        match self {
            Self::None => false,
            Self::Any => true,
            Self::Member(permitted_member) => permitted_member == member,
            Self::Members(members) => members.contains(member),
        }
    }
}

/// Side effects required after reconciling route interest against reachable members.
pub(super) struct RouteInterestReconciliation {
    /// Whether the published member snapshot needs to be rebuilt.
    pub(super) requires_snapshot_changes: bool,
    /// Probe or reachable-lease timer replaced by reconciliation and still requiring cancellation.
    pub(super) timer_to_cancel: Option<ScheduledTimer>,
}

impl RouteInterestReconciliation {
    const UNCHANGED: Self = Self {
        requires_snapshot_changes: false,
        timer_to_cancel: None,
    };
}

/// Return current members when all are permitted, otherwise allocate only the permitted subset.
fn permitted_reachable_members<'a>(
    interest: &RouteInterest,
    members: &'a TrieSet,
) -> Cow<'a, TrieSet> {
    let all_members_permitted = members
        .owned_keys()
        .all(|member| interest.permits_member(&member));
    if all_members_permitted {
        return Cow::Borrowed(members);
    }

    let permitted = members
        .owned_keys()
        .filter(|member| interest.permits_member(member))
        .collect();
    Cow::Owned(permitted)
}

/// Mutable verification state for one route.
pub enum RouteVerificationState {
    /// Route is watched but not verified by a signed introduction.
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

impl RouteVerificationState {
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

    /// Replace the published member set while keeping the current reachable lease.
    pub fn replace_reachable_members(&mut self, published_members: TrieSet) {
        if let Self::Reachable {
            published_members: current,
            ..
        } = self
        {
            *current = published_members;
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
    /// Watched route matched by the top-level introduction and each retained claim payload.
    pub route: DiscoveryRoute,
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
