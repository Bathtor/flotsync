//! Local actor request vocabulary for the replication runtime.
//!
//! Delivery port events stay in the delivery contracts; application events
//! stay in the public API. Re-exporting these requests preserves the runtime boundary.

#[cfg(any(test, feature = "test-support"))]
use super::ReplicationRuntimeTestMessage;
use crate::api::{
    ApiError,
    ChangeGroupMembershipRequest,
    CreateGroupRequest,
    MigrationId,
    PendingGroupWorkKey,
    PublishChangesRequest,
    PublishReceipt,
    RejectionReason,
    Summary,
    SummaryRequest,
    security::{
        AssessPublicKeyBundleRequest,
        KnownMemberKeysReport,
        PublicKeyBundleReport,
        RecordPublicKeyBundleFeedbackRequest,
    },
};
use flotsync_core::GroupId;
use flotsync_security::PublicKeyBundle;
use kompact::prelude::Ask;

/// Application response to one pending group decision replayed through the listener.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingGroupDecisionResponse {
    /// Stable idempotence key of the pending decision being resolved.
    key: PendingGroupWorkKey,
    /// Requested resolution for the pending decision.
    response: PendingGroupDecisionResponseKind,
}

impl PendingGroupDecisionResponse {
    /// Pair a listener resolution with the stable key of its pending work.
    pub(super) fn new(
        key: PendingGroupWorkKey,
        response: PendingGroupDecisionResponseKind,
    ) -> Self {
        Self { key, response }
    }

    /// Consume the mailbox payload into its work key and requested resolution.
    pub(super) fn into_parts(self) -> (PendingGroupWorkKey, PendingGroupDecisionResponseKind) {
        (self.key, self.response)
    }
}

/// Local-actor messages understood by [`super::ReplicationRuntimeComponent`].
///
/// This is the imperative bridge surface between the public async runtime
/// handle and the Kompact-hosted replication component.
#[derive(Debug)]
pub enum ReplicationRuntimeMessage {
    /// Return the local member's shareable public key bundle through the component interface.
    LocalPublicKeyBundle(Ask<(), Result<PublicKeyBundle, ApiError>>),
    /// Return stored member-key bindings and local trust through the component interface.
    KnownMemberKeys(Ask<(), Result<KnownMemberKeysReport, ApiError>>),
    /// Assess one decoded public key bundle through the component interface.
    AssessPublicKeyBundle(
        Ask<AssessPublicKeyBundleRequest, Result<PublicKeyBundleReport, ApiError>>,
    ),
    /// Record public key bundle feedback through the component interface.
    RecordPublicKeyBundleFeedback(Ask<RecordPublicKeyBundleFeedbackRequest, Result<(), ApiError>>),
    /// Submit one local publish request through the component interface.
    PublishChanges(Ask<PublishChangesRequest, Result<PublishReceipt, ApiError>>),
    /// Ask one group member for its current group version vector.
    RequestSummary(Ask<SummaryRequest, Result<Summary, ApiError>>),
    /// Create one new fixed-membership group through the component interface.
    CreateGroup(Ask<CreateGroupRequest, Result<GroupId, ApiError>>),
    /// Request one group-membership change through the component interface.
    ChangeGroupMembership(Ask<ChangeGroupMembershipRequest, Result<MigrationId, ApiError>>),
    /// Resolve one listener-mediated pending group decision through the component.
    PendingGroupDecisionResponse(Ask<PendingGroupDecisionResponse, Result<(), ApiError>>),
    /// Test-support command channel for runtime fixture setup and assertions.
    #[cfg(any(test, feature = "test-support"))]
    Test(ReplicationRuntimeTestMessage),
}

/// Resolution requested by a listener-facing pending group responder.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PendingGroupDecisionResponseKind {
    /// Accept the pending decision and move it into activation work.
    Accept,
    /// Reject the pending decision and discard it.
    Reject {
        /// Application-supplied reason for the rejection.
        reason: RejectionReason,
    },
}
