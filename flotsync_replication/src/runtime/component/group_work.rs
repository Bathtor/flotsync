//! Group-setup and pending-group support local to the runtime component.

use super::*;

/// Security records and private wire setup prepared for one new group.
pub(in crate::runtime) struct PreparedGroupSetup {
    pub(super) security_material: EncryptedGroupSecurityMaterial,
    pub(super) group_setup: Arc<GroupSetupMessage>,
}

impl PreparedGroupSetup {
    /// Return the plaintext group setup prepared for group creation.
    #[cfg(test)]
    pub(in crate::runtime) fn group_setup(&self) -> &GroupSetupMessage {
        self.group_setup.as_ref()
    }
}

/// Locally prepared membership migration before outbound reliable messages are sent.
pub(super) struct PreparedMembershipMigration {
    pub(super) migration_id: MigrationId,
    pub(super) final_versions: VersionVector,
    pub(super) group_schema: GroupSchema,
    pub(super) initial_snapshot: InitialSnapshot,
    pub(super) prepared_setup: PreparedGroupSetup,
    /// Sparse proposed-member indices for newly added recipients.
    pub(super) added_member_indices: RoaringBitmap,
    pub(super) group_name: Option<String>,
    pub(super) message: Option<String>,
}

/// Proposed member set plus recipient classifications for a membership change.
pub(super) struct ProposedMembershipChange {
    pub(super) proposed_members: GroupMembers,
    pub(super) added_member_indices: RoaringBitmap,
}

/// Encoded migration payloads and sparse recipient classes ready for fan-out.
pub(super) struct PreparedMembershipDispatch {
    pub(super) migration_id: MigrationId,
    pub(super) group_setup: Arc<GroupSetupMessage>,
    pub(super) migration_payload: bytes::Bytes,
    pub(super) invitation_payload: bytes::Bytes,
    pub(super) added_member_indices: RoaringBitmap,
}

/// Result of activating accepted group work into externally readable row state.
pub(super) struct PendingGroupActivationOutcome {
    pub(super) read_token: ReadToken,
    pub(super) row_changes: Vec<RowChange>,
}

/// Verified target-group material together with its current activation state.
pub(super) struct ValidatedInboundGroupSetup {
    pub(super) material: ReplicationGroupMaterialRecord,
    pub(super) already_active: bool,
}

/// Classification of an inbound proposal against the old group's accepted direction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MigrationProposalArrival {
    /// The old group is open and may collect this undecided candidate.
    Candidate,
    /// The proposal exactly replays the already accepted successor and cut.
    AcceptedReplay,
    /// A different direction was accepted, so this proposal is terminally processed and rejected.
    RejectedConflict,
}

/// Stored outcome of accepting one listener-mediated pending work item.
pub(super) enum PendingGroupAcceptOutcome {
    /// The responder was stale and no matching work remains.
    Missing,
    /// The work was removed because its metadata snapshot cannot activate yet.
    UnsupportedSnapshot(GroupId),
    /// The work moved to accepted activation state.
    Activation(Box<PendingGroupActivationRecord>),
}

/// Listener responder that routes pending-group decisions to the component mailbox.
pub(super) struct ComponentBackedPendingGroupResponder {
    runtime_ref: ActorRefStrong<ReplicationRuntimeMessage>,
    work_key: PendingGroupWorkKey,
}

impl ComponentBackedPendingGroupResponder {
    /// Build a responder for one target-keyed pending work item.
    pub(super) fn new(
        runtime_ref: ActorRefStrong<ReplicationRuntimeMessage>,
        work_key: PendingGroupWorkKey,
    ) -> Self {
        Self {
            runtime_ref,
            work_key,
        }
    }

    /// Send the listener's decision back through the runtime component mailbox.
    fn respond(
        self,
        response: PendingGroupDecisionResponseKind,
    ) -> BoxFuture<'static, Result<(), ApiError>> {
        let Self {
            runtime_ref,
            work_key,
        } = self;
        let request = PendingGroupDecisionResponse {
            key: work_key,
            response,
        };
        let future = runtime_ref.ask_with(move |promise| {
            ReplicationRuntimeMessage::PendingGroupDecisionResponse(Ask::new(promise, request))
        });
        async move {
            match future.await {
                Ok(reply) => reply,
                Err(_) => Err(ApiError::RuntimeUnavailable),
            }
        }
        .boxed()
    }
}

impl GroupInvitationResponder for ComponentBackedPendingGroupResponder {
    fn accept(self: Box<Self>) -> BoxFuture<'static, Result<(), ApiError>> {
        (*self).respond(PendingGroupDecisionResponseKind::Accept)
    }

    fn reject(
        self: Box<Self>,
        reason: RejectionReason,
    ) -> BoxFuture<'static, Result<(), ApiError>> {
        (*self).respond(PendingGroupDecisionResponseKind::Reject { reason })
    }
}

impl MigrationProposalResponder for ComponentBackedPendingGroupResponder {
    fn accept(self: Box<Self>) -> BoxFuture<'static, Result<(), ApiError>> {
        (*self).respond(PendingGroupDecisionResponseKind::Accept)
    }

    fn reject(
        self: Box<Self>,
        reason: RejectionReason,
    ) -> BoxFuture<'static, Result<(), ApiError>> {
        (*self).respond(PendingGroupDecisionResponseKind::Reject { reason })
    }
}

/// Normalise requested group members so the local creator occupies member index 0.
pub(super) fn creator_first_member_order(
    mut members: Vec<MemberIdentity>,
    creator: &MemberIdentity,
) -> Result<Vec<MemberIdentity>, CreateGroupError> {
    let creator_position = members
        .iter()
        .position(|member| member == creator)
        .context(CreatorNotInMembersSnafu {
            creator: creator.clone(),
        })?;
    members.swap(0, creator_position);
    Ok(members)
}
