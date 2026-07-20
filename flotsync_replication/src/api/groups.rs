//! Group, pending-group, invitation, and lifecycle API types.

use super::*;

/// Policy decision for one invitation or migration classification.
///
/// The enum order is the restrictiveness order: automatic acceptance is the
/// least restrictive outcome, and automatic rejection is the most restrictive.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PolicyDecision {
    /// Accept automatically after validation succeeds.
    AutoAccept,
    /// Forward to the application listener for an explicit decision.
    AskListener,
    /// Reject automatically after validation succeeds.
    AutoReject,
}

impl PolicyDecision {
    /// Return the more restrictive of two policy decisions.
    #[must_use]
    pub fn most_restrictive(self, other: Self) -> Self {
        self.max(other)
    }
}

/// Policy for handling group invitations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupInvitationPolicy {
    /// Applied to ordinary group invitations.
    pub creation: PolicyDecision,
    /// Applied when this member is added through a migration and receives the
    /// new group as an invitation rather than as an old-group proposal.
    pub migration_added_member: PolicyDecision,
}

impl Default for GroupInvitationPolicy {
    fn default() -> Self {
        Self {
            creation: PolicyDecision::AskListener,
            migration_added_member: PolicyDecision::AskListener,
        }
    }
}

/// Policy for handling membership migration proposals.
///
/// The current inline migration flow classifies top-level member additions and
/// removals. Device-level and local-member-removal classifications are reserved
/// for later identity and local-removal flows.
///
/// If more than one supported behaviour applies to a change, the most
/// restrictive behaviour is used.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupMigrationPolicy {
    /// Applied if the migration contains only an epoch change, so that the group does not run out
    /// of change versions.
    pub epoch_change: PolicyDecision,
    /// Applied when a new top-level member was added to the group.
    pub member_added: PolicyDecision,
    /// Reserved for classifying a new device under an existing top-level member.
    pub member_device_added: PolicyDecision,
    /// Applied when a top-level member is removed from the group.
    pub member_removed: PolicyDecision,
    /// Reserved for classifying removal of a device under an existing member.
    pub member_device_removed: PolicyDecision,
    /// Reserved for a future flow that informs a removed local member.
    ///
    /// A migration proposal delivered to this runtime must include its local
    /// member in the proposed group.
    pub local_member_removed: PolicyDecision,
}

impl Default for GroupMigrationPolicy {
    fn default() -> Self {
        Self {
            epoch_change: PolicyDecision::AutoAccept,
            member_added: PolicyDecision::AskListener,
            member_device_added: PolicyDecision::AutoAccept,
            member_removed: PolicyDecision::AskListener,
            member_device_removed: PolicyDecision::AutoAccept,
            local_member_removed: PolicyDecision::AskListener,
        }
    }
}

/// Local access policy after a future standalone group-close signal.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum GroupClosePolicy {
    /// Keep local external access unchanged and only record/observe the signal.
    ObserveOnly,
    /// Reject new external writes while keeping external reads available.
    #[default]
    CloseLocalWrites,
    /// Reject external reads and writes.
    ///
    /// Runtime internals may still read stored updates or snapshots for
    /// activation, catch-up, or diagnostics.
    CloseReadsAndWrites,
}

/// Runtime configuration passed during `load`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReplicationConfig {
    /// Policy used to derive runtime permissions from stored trust evidence.
    pub trust_policy: TrustPolicy,
    /// Policy for handling group invitations.
    pub group_invitation_policy: GroupInvitationPolicy,
    /// Policy for handling group membership migration proposals.
    pub group_migration_policy: GroupMigrationPolicy,
    /// Local access policy reserved for the future standalone group-close flow.
    pub group_close_policy: GroupClosePolicy,
}

/// Device-local security input required while loading one replication runtime.
///
/// This value is intentionally separate from [`ReplicationConfig`]: it carries
/// secret-bearing key material for the current process/device, while
/// `ReplicationConfig` contains cloneable policy knobs that are safe to compare
/// and log normally.
#[derive(Clone, Debug)]
pub struct ReplicationSecuritySecrets {
    store_secret_key_id: StoreSecretKeyId,
    store_secret_key: Arc<StoreSecretKey>,
}

impl ReplicationSecuritySecrets {
    /// Load the existing device-local store secret for an application/profile.
    ///
    /// The profile is scoped by application id, not by member identity. This
    /// keeps the API ready for stores that host multiple local identities under
    /// the same encrypted-store secret.
    ///
    /// # Errors
    ///
    /// Returns [`LoadSecurityError::LocalStoreSecret`] if the local secret slot
    /// is missing, inaccessible, or malformed.
    pub fn load_local(
        application_id: &Identifier,
        profile: &LocalStoreSecretProfile,
    ) -> Result<Self, LoadSecurityError> {
        let secret =
            load_local_store_secret(application_id, profile).context(LocalStoreSecretSnafu)?;
        Ok(Self::from_loaded_local_store_secret(secret))
    }

    /// Load or first-run initialise the device-local store secret.
    ///
    /// # Errors
    ///
    /// Returns [`LoadSecurityError::LocalStoreSecret`] if the local secret slot
    /// cannot be loaded or created.
    pub fn load_or_create_local(
        application_id: &Identifier,
        profile: &LocalStoreSecretProfile,
    ) -> Result<Self, LoadSecurityError> {
        let secret = load_or_create_local_store_secret(application_id, profile)
            .context(LocalStoreSecretSnafu)?;
        Ok(Self::from_loaded_local_store_secret(secret))
    }

    /// Build runtime security input from caller-managed store-secret material.
    ///
    /// This bypasses the platform local-secret store. Prefer
    /// [`Self::load_or_create_local`] unless the caller intentionally manages
    /// the local store secret through another mechanism.
    ///
    /// TODO(flotsync-lsi8): Re-evaluate or remove this escape hatch once
    /// headless local store-secret backends can provide managed secrets.
    #[doc(hidden)]
    #[must_use]
    pub fn from_unmanaged_store_secret(
        store_secret_key_id: StoreSecretKeyId,
        store_secret_key: StoreSecretKey,
    ) -> Self {
        Self {
            store_secret_key_id,
            store_secret_key: Arc::new(store_secret_key),
        }
    }

    /// Build runtime security input from a shared store-secret key handle.
    ///
    /// This is intentionally test-support only: some security validation tests
    /// need to construct deliberately mismatched or malformed key inputs.
    #[must_use]
    #[cfg(any(test, feature = "test-support"))]
    pub fn new(
        store_secret_key_id: StoreSecretKeyId,
        store_secret_key: Arc<StoreSecretKey>,
    ) -> Self {
        Self {
            store_secret_key_id,
            store_secret_key,
        }
    }

    fn from_loaded_local_store_secret(secret: flotsync_security::LoadedLocalStoreSecret) -> Self {
        let (key_id, store_secret_key) = secret.into_parts();
        Self {
            store_secret_key_id: key_id,
            store_secret_key: Arc::new(store_secret_key),
        }
    }

    /// Return the generated store-secret key id expected by encrypted cells.
    #[must_use]
    pub fn store_secret_key_id(&self) -> &StoreSecretKeyId {
        &self.store_secret_key_id
    }

    /// Return the shared store-secret key handle for internal runtime security loading.
    pub(crate) fn store_secret_key(&self) -> &Arc<StoreSecretKey> {
        &self.store_secret_key
    }
}

/// Request to create a new replication group.
#[derive(Clone, PartialEq, Eq)]
pub struct CreateGroupRequest {
    pub members: Vec<MemberIdentity>,
    /// Dataset schemas fixed for the lifetime of the new group.
    pub group_schema: GroupSchema,
}

impl fmt::Debug for CreateGroupRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreateGroupRequest")
            .field("members", &self.members)
            .field("group_schema", &self.group_schema)
            .finish()
    }
}

/// Request to change membership of an existing group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangeGroupMembershipRequest {
    pub group_id: GroupId,
    pub add_members: HashSet<MemberIdentity>,
    pub remove_members: HashSet<MemberIdentity>,
    pub group_name: Option<String>,
    pub message: Option<String>,
}

/// Receipt returned by `publish_changes`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishReceipt {
    pub update_id: UpdateId,
    pub read_token: ReadToken,
}

/// Source that explains why a group invitation was received.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum GroupInvitationSource {
    /// Ordinary group creation or invitation into an existing group.
    Creation,
    /// The local member is joining the new group as part of a migration.
    Migration { migration_id: MigrationId },
}

/// Invitation details surfaced when group-invitation policy requires listener mediation.
#[derive(Clone, PartialEq, Eq)]
pub struct GroupInvitation {
    /// Group the recipient is being invited to activate.
    pub group_id: GroupId,
    /// Local perspective for this invitation.
    pub source: GroupInvitationSource,
    /// Proposed canonical member order for the invited group.
    pub proposed_members: Vec<MemberIdentity>,
    /// Dataset schemas fixed for the lifetime of the invited group.
    pub group_schema: GroupSchema,
    /// Initial dataset state required before the invited group becomes active.
    pub initial_snapshot: InitialSnapshot,
    /// Optional display name supplied by the proposer.
    pub group_name: Option<String>,
    /// Optional user-facing note supplied by the proposer.
    pub message: Option<String>,
}

impl fmt::Debug for GroupInvitation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GroupInvitation")
            .field("group_id", &self.group_id)
            .field("source", &self.source)
            .field("proposed_member_count", &self.proposed_members.len())
            .field("group_schema", &self.group_schema)
            .field("initial_snapshot", &self.initial_snapshot)
            .field("has_group_name", &self.group_name.is_some())
            .field("has_message", &self.message.is_some())
            .finish()
    }
}

/// Failed construction of a group invitation.
#[derive(Debug, Snafu)]
#[snafu(module(group_invitation))]
pub enum GroupInvitationError {
    /// A migration invitation named a target group different from its migration id.
    #[snafu(display(
        "Migration invitation group id {group_id} did not match migration target group {new_group_id}."
    ))]
    GroupMismatch {
        group_id: GroupId,
        new_group_id: GroupId,
    },
}

impl GroupInvitation {
    /// Build an ordinary group invitation.
    #[must_use]
    pub fn new_creation(
        group_id: GroupId,
        proposed_members: Vec<MemberIdentity>,
        group_schema: GroupSchema,
        initial_snapshot: InitialSnapshot,
        group_name: Option<String>,
        message: Option<String>,
    ) -> Self {
        Self {
            group_id,
            source: GroupInvitationSource::Creation,
            proposed_members,
            group_schema,
            initial_snapshot,
            group_name,
            message,
        }
    }

    /// Build a migration-sourced group invitation.
    ///
    /// The invitation's `group_id` is derived from `migration_id.new_group_id` so
    /// the duplicated target-group identity cannot diverge.
    #[must_use]
    pub fn new_migration(
        migration_id: MigrationId,
        proposed_members: Vec<MemberIdentity>,
        group_schema: GroupSchema,
        initial_snapshot: InitialSnapshot,
        group_name: Option<String>,
        message: Option<String>,
    ) -> Self {
        Self {
            group_id: migration_id.new_group_id,
            source: GroupInvitationSource::Migration { migration_id },
            proposed_members,
            group_schema,
            initial_snapshot,
            group_name,
            message,
        }
    }

    /// Build a group invitation from explicit API parts.
    ///
    /// This validates that migration-sourced invitations name the migration's
    /// target group as `group_id`.
    ///
    /// # Errors
    ///
    /// Returns [`GroupInvitationError::GroupMismatch`] when `source` is
    /// migration-sourced and `group_id` differs from `source.migration_id.new_group_id`.
    pub fn try_new(
        group_id: GroupId,
        source: GroupInvitationSource,
        proposed_members: Vec<MemberIdentity>,
        group_schema: GroupSchema,
        initial_snapshot: InitialSnapshot,
        group_name: Option<String>,
        message: Option<String>,
    ) -> Result<Self, GroupInvitationError> {
        match source {
            GroupInvitationSource::Creation => Ok(Self::new_creation(
                group_id,
                proposed_members,
                group_schema,
                initial_snapshot,
                group_name,
                message,
            )),
            GroupInvitationSource::Migration { migration_id } => {
                ensure!(
                    group_id == migration_id.new_group_id,
                    group_invitation::GroupMismatchSnafu {
                        group_id,
                        new_group_id: migration_id.new_group_id,
                    }
                );
                Ok(Self::new_migration(
                    migration_id,
                    proposed_members,
                    group_schema,
                    initial_snapshot,
                    group_name,
                    message,
                ))
            }
        }
    }
}

/// Old-group member proposal surfaced when migration policy requires listener mediation.
#[derive(Clone, PartialEq, Eq)]
pub struct MigrationProposal {
    /// Old-to-new group migration being proposed.
    pub migration_id: MigrationId,
    /// Old-group final externally writable version vector for this proposal.
    pub final_versions: VersionVector,
    /// Proposed canonical member order for the new group.
    pub proposed_members: Vec<MemberIdentity>,
    /// Dataset schemas fixed for the lifetime of the new group.
    pub group_schema: GroupSchema,
    /// Initial dataset state required before the new group becomes active.
    pub initial_snapshot: InitialSnapshot,
    /// Optional display name supplied by the proposer.
    pub group_name: Option<String>,
    /// Optional user-facing note supplied by the proposer.
    pub message: Option<String>,
}

impl fmt::Debug for MigrationProposal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MigrationProposal")
            .field("migration_id", &self.migration_id)
            .field("final_versions", &self.final_versions)
            .field("proposed_member_count", &self.proposed_members.len())
            .field("group_schema", &self.group_schema)
            .field("initial_snapshot", &self.initial_snapshot)
            .field("has_group_name", &self.group_name.is_some())
            .field("has_message", &self.message.is_some())
            .finish()
    }
}

/// Stable identity for pending group decision or activation work.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PendingGroupWorkKey {
    /// Decision for a group invitation.
    GroupInvitation {
        /// Group being invited or activated.
        group_id: GroupId,
        /// Local source perspective for the invitation.
        source: GroupInvitationSource,
    },
    /// Decision for an old-group migration proposal.
    MigrationProposal {
        /// Old-to-new group migration being proposed.
        migration_id: MigrationId,
    },
}

impl PendingGroupWorkKey {
    /// Return the target group for the pending work.
    #[must_use]
    pub const fn group_id(self) -> GroupId {
        match self {
            Self::GroupInvitation { group_id, .. } => group_id,
            Self::MigrationProposal { migration_id } => migration_id.new_group_id,
        }
    }
}

/// Store-owned unresolved listener-mediated group decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PendingGroupDecisionRecord {
    /// Pending group invitation decision.
    GroupInvitation(GroupInvitation),
    /// Pending migration proposal decision.
    MigrationProposal(MigrationProposal),
}

impl PendingGroupDecisionRecord {
    /// Return the stable idempotence key for this pending decision.
    #[must_use]
    pub const fn key(&self) -> PendingGroupWorkKey {
        match self {
            Self::GroupInvitation(invitation) => PendingGroupWorkKey::GroupInvitation {
                group_id: invitation.group_id,
                source: invitation.source,
            },
            Self::MigrationProposal(proposal) => PendingGroupWorkKey::MigrationProposal {
                migration_id: proposal.migration_id,
            },
        }
    }

    /// Return the target group for this pending decision.
    #[must_use]
    pub const fn group_id(&self) -> GroupId {
        self.key().group_id()
    }

    /// Return whether accepting this decision requires fetching snapshot data first.
    #[must_use]
    pub fn requires_snapshot_fetch(&self) -> bool {
        match self {
            Self::GroupInvitation(invitation) => {
                invitation.initial_snapshot.requires_snapshot_fetch()
            }
            Self::MigrationProposal(proposal) => {
                proposal.initial_snapshot.requires_snapshot_fetch()
            }
        }
    }

    /// Return proposed target-group members in canonical order.
    #[must_use]
    pub fn proposed_members(&self) -> &[MemberIdentity] {
        match self {
            Self::GroupInvitation(invitation) => &invitation.proposed_members,
            Self::MigrationProposal(proposal) => &proposal.proposed_members,
        }
    }

    /// Return the target-group schema carried by this work.
    #[must_use]
    pub const fn group_schema(&self) -> &GroupSchema {
        match self {
            Self::GroupInvitation(invitation) => &invitation.group_schema,
            Self::MigrationProposal(proposal) => &proposal.group_schema,
        }
    }

    /// Convert this pending decision into the listener event that asks for the decision.
    #[must_use]
    pub fn to_event<R>(self, responder: R) -> ReplicationEvent
    where
        R: GroupInvitationResponder + MigrationProposalResponder + 'static,
    {
        match self {
            Self::GroupInvitation(invitation) => ReplicationEvent::GroupInvitation {
                invitation,
                respond: Box::new(responder),
            },
            Self::MigrationProposal(proposal) => ReplicationEvent::MigrationProposals {
                proposals: smallvec![MigrationCandidateProposal {
                    proposal,
                    respond: Box::new(responder),
                }],
            },
        }
    }

    /// Convert an accepted pending decision into accepted activation work.
    #[must_use]
    pub fn into_activation(self) -> PendingGroupActivationRecord {
        match self {
            Self::GroupInvitation(invitation) => {
                PendingGroupActivationRecord::GroupInvitation(invitation)
            }
            Self::MigrationProposal(proposal) => {
                PendingGroupActivationRecord::MigrationProposal(proposal)
            }
        }
    }
}

/// Store-owned accepted group work that is not externally active yet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PendingGroupActivationRecord {
    /// Accepted group invitation activation work.
    GroupInvitation(GroupInvitation),
    /// Accepted migration proposal activation work.
    MigrationProposal(MigrationProposal),
}

impl PendingGroupActivationRecord {
    /// Return the stable idempotence key for this pending activation.
    #[must_use]
    pub const fn key(&self) -> PendingGroupWorkKey {
        match self {
            Self::GroupInvitation(invitation) => PendingGroupWorkKey::GroupInvitation {
                group_id: invitation.group_id,
                source: invitation.source,
            },
            Self::MigrationProposal(proposal) => PendingGroupWorkKey::MigrationProposal {
                migration_id: proposal.migration_id,
            },
        }
    }

    /// Return the new group activated by this record.
    #[must_use]
    pub const fn group_id(&self) -> GroupId {
        self.key().group_id()
    }

    /// Return whether activation requires fetching snapshot data first.
    #[must_use]
    pub fn requires_snapshot_fetch(&self) -> bool {
        match self {
            Self::GroupInvitation(invitation) => {
                invitation.initial_snapshot.requires_snapshot_fetch()
            }
            Self::MigrationProposal(proposal) => {
                proposal.initial_snapshot.requires_snapshot_fetch()
            }
        }
    }

    /// Split this record into the fields required by the activation pipeline.
    #[must_use]
    pub fn into_activation_record(self) -> AcceptedGroupActivationRecord {
        let key = self.key();
        match self {
            Self::GroupInvitation(invitation) => AcceptedGroupActivationRecord {
                key,
                group_id: invitation.group_id,
                migration_cutover: None,
                proposed_members: invitation.proposed_members,
                group_schema: invitation.group_schema,
                initial_snapshot: invitation.initial_snapshot,
            },
            Self::MigrationProposal(proposal) => AcceptedGroupActivationRecord {
                key,
                group_id: proposal.migration_id.new_group_id,
                migration_cutover: Some(AcceptedMigrationCutover {
                    old_group_id: proposal.migration_id.old_group_id,
                    final_versions: proposal.final_versions,
                }),
                proposed_members: proposal.proposed_members,
                group_schema: proposal.group_schema,
                initial_snapshot: proposal.initial_snapshot,
            },
        }
    }
}

/// Decomposed accepted group work ready for activation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcceptedGroupActivationRecord {
    /// Stable store key for this accepted activation.
    pub key: PendingGroupWorkKey,
    /// New group that becomes externally active after activation succeeds.
    pub group_id: GroupId,
    /// Old-group cutover performed after target activation, when applicable.
    pub migration_cutover: Option<AcceptedMigrationCutover>,
    /// Proposed canonical member order for the activated group.
    pub proposed_members: Vec<MemberIdentity>,
    /// Dataset schemas fixed for the activated group.
    pub group_schema: GroupSchema,
    /// Initial dataset state required before the group becomes active.
    pub initial_snapshot: InitialSnapshot,
}

/// Immutable old-group closure context retained by accepted migration work.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcceptedMigrationCutover {
    /// Existing group that becomes closed after target activation.
    pub old_group_id: GroupId,
    /// Replay cut fixed by the accepted migration proposal.
    pub final_versions: VersionVector,
}

/// Listener-visible replication events.
#[allow(
    clippy::large_enum_variant,
    reason = "Listener events are infrequent, and keeping invitation values direct avoids public API indirection."
)]
pub enum ReplicationEvent {
    DataChanged {
        /// Read position reached by the row changes in this event.
        ///
        /// Applications that keep local mutable state should merge this into
        /// their stored [`ReadToken`] after applying all rows from the event.
        /// This avoids replacing newer local publish progress with an older
        /// listener token when local and inbound events are consumed out of
        /// order.
        read_token: ReadToken,
        rows: Box<RowProvider>,
    },
    GroupInvitation {
        invitation: GroupInvitation,
        respond: Box<dyn GroupInvitationResponder>,
    },
    /// Current undecided migration candidates for one old group.
    MigrationProposals {
        /// Complete candidate set known when this event was emitted.
        proposals: SmallVec<[MigrationCandidateProposal; 1]>,
    },
}

/// One listener-mediated candidate within a grouped migration proposal event.
pub struct MigrationCandidateProposal {
    /// Proposed target group and immutable old-group replay cut.
    pub proposal: MigrationProposal,
    /// One-shot response for this specific candidate.
    pub respond: Box<dyn MigrationProposalResponder>,
}

/// Callback for applications to accept or reject one group invitation.
///
/// The callback is one-shot by construction: calling either method consumes the responder.
pub trait GroupInvitationResponder: Send {
    /// Accept the invitation into local activation state.
    ///
    /// Empty and inline initial snapshots activate before this call succeeds.
    /// Metadata-only snapshots are not fetchable by the current runtime and
    /// therefore cannot be accepted yet.
    fn accept(self: Box<Self>) -> BoxFuture<'static, Result<(), ApiError>>;

    /// Refuse to join the invited group.
    fn reject(self: Box<Self>, reason: RejectionReason)
    -> BoxFuture<'static, Result<(), ApiError>>;
}

/// Callback for applications to accept or reject one migration proposal.
///
/// The callback is one-shot by construction: calling either method consumes the responder.
pub trait MigrationProposalResponder: Send {
    /// Accept the proposal into local migration activation state.
    ///
    /// Empty and inline initial snapshots activate the new group before this
    /// call succeeds. Metadata-only snapshots are not fetchable by the current
    /// runtime and therefore cannot be accepted yet.
    fn accept(self: Box<Self>) -> BoxFuture<'static, Result<(), ApiError>>;

    /// Refuse the proposed migration.
    fn reject(self: Box<Self>, reason: RejectionReason)
    -> BoxFuture<'static, Result<(), ApiError>>;
}

/// Optional rejection reasons reported back to the framework.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RejectionReason {
    PolicyDenied,
    UserDenied,
    UnsupportedInitialSnapshot,
    UnsupportedSchemaEpoch,
    IncompatibleMembership,
    Other(String),
}

/// Application listener callback interface.
pub trait ReplicationEventListener: Send + Sync {
    fn on_event(&self, event: ReplicationEvent) -> BoxFuture<'_, Result<(), ListenerError>>;
}

/// Application-facing replication control surface.
pub trait ReplicationApi: Send + Sync {
    /// Shut this runtime down gracefully.
    ///
    /// Shutdown stops the live replication components in dependency order and
    /// then shuts down the underlying runtime system. After shutdown starts,
    /// other API calls on any clone of this runtime handle report
    /// [`ApiError::RuntimeUnavailable`]. Calling shutdown again after a
    /// previous shutdown completed is a no-op.
    fn shutdown(&self) -> BoxFuture<'_, Result<(), ApiError>>;

    /// Return the local member's shareable identity-free public key bundle.
    ///
    /// Applications can encode this bundle for transfer to another user or
    /// device. The returned key material is public; it does not include member
    /// identity, local trust evidence, private keys, or policy state.
    fn local_public_key_bundle(&self) -> BoxFuture<'_, Result<PublicKeyBundle, ApiError>>;

    /// Assess one decoded public key bundle against the running system's local security state.
    ///
    /// Read-only assessment does not store observed key material, record trust
    /// evidence, or change blocked-key state. Requests may instead ask the
    /// running system to store candidate member bindings before reporting, still
    /// without adding trust evidence. Applications use the returned report to
    /// present fingerprint, known identity bindings, local trust, blocked status,
    /// and authority decisions before recording user feedback.
    fn assess_public_key_bundle(
        &self,
        request: security::AssessPublicKeyBundleRequest,
    ) -> BoxFuture<'_, Result<security::PublicKeyBundleReport, ApiError>>;

    /// Record user/application feedback for one decoded public key bundle.
    ///
    /// Trust feedback records local trust for an explicit member identity plus
    /// the bundle fingerprint. Block feedback records the fingerprint as
    /// globally blocked.
    fn record_public_key_bundle_feedback(
        &self,
        request: security::RecordPublicKeyBundleFeedbackRequest,
    ) -> BoxFuture<'_, Result<(), ApiError>>;

    /// Publish one local set of row mutations from a known read token.
    ///
    /// The request token is the read position of the application state used to
    /// decide the mutation list. Mutations are interpreted as sparse field
    /// patches for each [`RowMutation::Upsert`]:
    /// fields omitted from the submitted [`RowValuesPatch`] are intentionally left
    /// unchanged in the current local state. [`RowMutation::Delete`] records a
    /// replicated row tombstone rather than physically removing the row from the
    /// store.
    ///
    /// On success, the runtime has applied the mutations transactionally to the
    /// local store, persisted the corresponding replication update, notified the
    /// local [`ReplicationEventListener`] with the locally applied row changes,
    /// and submitted a best-effort live update broadcast to currently configured
    /// peers. The returned [`PublishReceipt`] identifies the local update that
    /// was recorded in the store.
    ///
    /// The method returns [`ApiError`] if validation, local apply, store
    /// access, listener notification, or runtime availability fails. Failed calls
    /// must not be treated as partially published by applications; callers should
    /// keep their own pending changes until a receipt is returned.
    fn publish_changes(
        &self,
        request: PublishChangesRequest,
    ) -> BoxFuture<'_, Result<PublishReceipt, ApiError>>;

    /// Open a batched stream over the latest locally stored projected row values for selected datasets.
    ///
    /// The request is scoped to one replication group and an explicit set of
    /// application datasets. The stream reflects the latest state known to the
    /// local store when the snapshot is opened; it does not wait for remote peers
    /// and it does not perform catch-up. When `include_tombstones` is false, the
    /// provider should emit only application-visible rows. When it is true,
    /// retained delete tombstones are emitted as [`SnapshotValueRow`] views with
    /// [`SnapshotValueRow::is_tombstoned`] set.
    ///
    /// The returned [`SnapshotValueRows`] provider may hold a store read transaction
    /// while it is alive, so callers should drain or drop it promptly. Batches
    /// are bounded by [`SnapshotRowsRequest::max_rows_per_batch`] and are emitted
    /// through the same [`BatchProvider`] end-of-stream contract as listener row
    /// providers.
    ///
    /// The method returns [`ApiError`] when the group is unknown, the request is
    /// invalid, the runtime is unavailable, or the store cannot open the
    /// snapshot.
    fn snapshot_rows(
        &self,
        request: SnapshotRowsRequest,
    ) -> BoxFuture<'_, Result<SnapshotValueRows, ApiError>>;

    /// Ask one group member for its current group version vector.
    fn request_summary(&self, request: SummaryRequest) -> BoxFuture<'_, Result<Summary, ApiError>>;

    /// Create one new fixed-membership replication group rooted at this member.
    ///
    /// `req.members` defines the canonical member order for the new group and
    /// must include the local member. On success, the runtime stores the
    /// group record, installs it in the live runtime view, broadcasts bootstrap
    /// messages to the configured remote members, and returns the newly allocated
    /// [`GroupId`]. New groups are created empty; callers publish initial
    /// dataset contents through ordinary [`Self::publish_changes`] updates.
    ///
    /// The method returns [`ApiError`] when membership validation fails, the
    /// runtime is unavailable, or group storage fails.
    fn create_group(&self, req: CreateGroupRequest) -> BoxFuture<'_, Result<GroupId, ApiError>>;

    /// Request a membership migration for an existing replication group.
    ///
    /// The runtime derives the migration state from `req.group_id`, creates and
    /// locally activates a new group for the requested member set, and sends
    /// recipient-protected setup messages. Applications do not supply the
    /// initial snapshot.
    ///
    /// Continuing remote members receive old-group-scoped migration proposals;
    /// newly added members receive new-group-scoped invitations with migration
    /// source context.
    ///
    /// Receiver-side mediation is governed by [`GroupInvitationPolicy`] and
    /// [`GroupMigrationPolicy`].
    ///
    /// The old group becomes read-only when the migration is accepted and
    /// becomes closed when local target-group activation commits. Closed groups
    /// remain available to the replication protocol for bounded catch-up, but
    /// no longer interact with the application.
    ///
    /// # Errors
    ///
    /// Returns [`ApiError`] when the old group is unknown or invalid, the
    /// requested members exclude the local member, migration preparation or
    /// activation fails, listener notification fails, or the runtime is
    /// unavailable.
    fn change_group_membership(
        &self,
        req: ChangeGroupMembershipRequest,
    ) -> BoxFuture<'_, Result<MigrationId, ApiError>>;
}

/// Current encrypted-store-secret setup for the active security-storage slice.
///
/// Version `1` denotes XChaCha20-Poly1305 with random nonces and authenticated
/// data chosen by the encryption boundary. Store implementations persist the
/// numeric value opaquely and must not derive cipher behaviour from it.
pub(crate) const STORE_SECRET_CRYPTO_V1: StoreSecretCryptoVersion =
    StoreSecretCryptoVersion::new(1);

/// Canonical ordered member-key set for one persisted replication group.
#[derive(Clone)]
pub struct GroupMemberKeys {
    /// Exact member-key bindings in canonical group order.
    ordered_member_keys: Vec<MemberKeyId>,
    /// Member identity to canonical index lookup derived from `ordered_member_keys`.
    member_indices: TrieMap<MemberIndex>,
}

impl GroupMemberKeys {
    /// Build one indexed member-key set from canonical group member-key order.
    ///
    /// # Errors
    ///
    /// Returns [`GroupMembersError`] when the member-key list is empty, contains
    /// duplicate member identities, or exceeds the supported member count.
    ///
    /// # Panics
    ///
    /// Panics only if a member index cannot be found after the same member keys
    /// were already accepted by [`GroupMembers`].
    pub fn from_ordered_member_keys(
        ordered_member_keys: impl IntoIterator<Item = MemberKeyId>,
    ) -> Result<Self, GroupMembersError> {
        let ordered_member_keys: Vec<_> = ordered_member_keys.into_iter().collect();
        let members = GroupMembers::from_ordered_members(
            ordered_member_keys
                .iter()
                .map(|member_key| member_key.member_id.clone()),
        )?;
        let mut member_indices = TrieMap::new();
        for member_key in &ordered_member_keys {
            let member_index = members
                .member_index(&member_key.member_id)
                .expect("member index exists after building members from the same keys");
            member_indices.insert(member_key.member_id.clone(), member_index);
        }
        Ok(Self {
            ordered_member_keys,
            member_indices,
        })
    }

    /// Return the exact member-key bindings in canonical group order.
    #[must_use]
    pub fn ordered_member_keys(&self) -> &[MemberKeyId] {
        &self.ordered_member_keys
    }

    /// Return the member identities in canonical group order.
    #[must_use]
    pub fn member_ids(&self) -> impl ExactSizeIterator<Item = &MemberIdentity> + '_ {
        self.ordered_member_keys
            .iter()
            .map(|member_key| &member_key.member_id)
    }

    /// Convert this exact key set into an identity-only indexed group view.
    ///
    /// # Errors
    ///
    /// See [`GroupMembersError`] for failure conditions. Construction should
    /// only fail if this value was built from inconsistent internal state.
    pub fn to_group_members(&self) -> Result<GroupMembers, GroupMembersError> {
        GroupMembers::from_ordered_members(self.member_ids().cloned())
    }

    /// Return whether this group contains `member_id`.
    #[must_use]
    pub fn contains_member(&self, member_id: &MemberIdentity) -> bool {
        self.member_indices.get(member_id).is_some()
    }

    /// Return the canonical member index assigned to `member_id`, if present.
    #[must_use]
    pub fn member_index(&self, member_id: &MemberIdentity) -> Option<MemberIndex> {
        self.member_indices.get(member_id).copied()
    }

    /// Return the exact key binding for `member_id`, if present.
    #[must_use]
    pub fn member_key(&self, member_id: &MemberIdentity) -> Option<&MemberKeyId> {
        let member_index = self.member_index(member_id)?;
        self.member_key_at_index(member_index)
    }

    /// Return the exact key binding assigned to one canonical group index.
    #[must_use]
    pub fn member_key_at_index(&self, index: MemberIndex) -> Option<&MemberKeyId> {
        self.ordered_member_keys.get(index.as_u32() as usize)
    }

    /// Return whether this member-key set is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ordered_member_keys.is_empty()
    }

    /// Return the number of exact member-key bindings in this group.
    #[must_use]
    pub fn len(&self) -> usize {
        self.ordered_member_keys.len()
    }
}

impl fmt::Debug for GroupMemberKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GroupMemberKeys")
            .field("ordered_member_keys", &self.ordered_member_keys)
            .finish_non_exhaustive()
    }
}

impl PartialEq for GroupMemberKeys {
    fn eq(&self, other: &Self) -> bool {
        self.ordered_member_keys == other.ordered_member_keys
    }
}

impl Eq for GroupMemberKeys {}

/// Application-access lifecycle for one hosted replication group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplicationGroupLifecycle {
    /// The group accepts application reads, writes, and membership changes.
    Open,
    /// Local writes are disabled while accepted migration activation completes.
    ReadOnly {
        /// Accepted target group used to identify exact proposal replays.
        successor_group_id: GroupId,
        /// Immutable old-group replay cut carried by the accepted proposal.
        final_versions: VersionVector,
    },
    /// The group remains available only for bounded replication and catch-up.
    Closed {
        /// Accepted target group used to identify exact proposal replays.
        successor_group_id: GroupId,
        /// Immutable old-group replay cut carried by the accepted proposal.
        final_versions: VersionVector,
    },
}

impl ReplicationGroupLifecycle {
    /// Return whether application code may publish or change membership.
    #[must_use]
    pub const fn is_writable(&self) -> bool {
        matches!(self, Self::Open)
    }

    /// Return whether application code may read rows or request summaries.
    #[must_use]
    pub const fn is_readable(&self) -> bool {
        !matches!(self, Self::Closed { .. })
    }

    /// Return whether newly applied remote rows should reach the listener.
    #[must_use]
    pub const fn emits_data_changes(&self) -> bool {
        self.is_readable()
    }

    /// Return the immutable migration cut for a non-open group.
    #[must_use]
    pub const fn final_versions(&self) -> Option<&VersionVector> {
        match self {
            Self::Open => None,
            Self::ReadOnly { final_versions, .. } | Self::Closed { final_versions, .. } => {
                Some(final_versions)
            }
        }
    }

    /// Return the accepted successor for a non-open group.
    #[must_use]
    pub const fn successor_group_id(&self) -> Option<GroupId> {
        match self {
            Self::Open => None,
            Self::ReadOnly {
                successor_group_id, ..
            }
            | Self::Closed {
                successor_group_id, ..
            } => Some(*successor_group_id),
        }
    }

    /// Bound one replication frontier by this lifecycle's migration cut.
    #[must_use]
    pub fn bound_versions(&self, versions: &VersionVector) -> VersionVector {
        match self.final_versions() {
            Some(final_versions) => versions.greatest_lower_bound(final_versions),
            None => versions.clone(),
        }
    }
}

/// One persisted replication group together with its local progress.
///
/// This is the group-level record that anchors dataset snapshots and persisted
/// replication updates in the store. The canonical member order is significant
/// because it defines the stable `MemberIndex` values used in version vectors
/// and `UpdateId.node_index`. Sensitive group-security material is already
/// encrypted by the setup/runtime boundary before it enters the store.
#[derive(Clone, PartialEq, Eq)]
pub struct ReplicationGroupRecord {
    /// Stable replication-group identifier.
    pub group_id: GroupId,
    /// Canonical exact member-key order for the group.
    pub member_keys: GroupMemberKeys,
    /// Position of the local member within `member_keys`.
    pub local_member_index: MemberIndex,
    /// Dataset schemas fixed for the lifetime of this group.
    pub group_schema: GroupSchema,
    /// Last applied version vector stored for this group.
    pub version_vector: VersionVector,
    /// Current application-access and replication lifecycle.
    pub lifecycle: ReplicationGroupLifecycle,
    /// Already-encrypted group-security material needed by runtime operation.
    pub security_material: EncryptedGroupSecurityMaterial,
}

impl ReplicationGroupRecord {
    /// Split active progress from group material shared with pending setup.
    #[must_use]
    pub fn into_parts(self) -> (ReplicationGroupMaterialRecord, ReplicationGroupActiveState) {
        let material = ReplicationGroupMaterialRecord {
            group_id: self.group_id,
            member_keys: self.member_keys,
            local_member_index: self.local_member_index,
            group_schema: self.group_schema,
            security_material: self.security_material,
        };
        let active_state = ReplicationGroupActiveState {
            version_vector: self.version_vector,
            lifecycle: self.lifecycle,
        };
        (material, active_state)
    }

    /// Return the number of members encoded in this record.
    ///
    /// # Panics
    ///
    /// Panics if the record contains an empty member set.
    #[must_use]
    pub fn member_count(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.member_keys.len())
            .expect("replication group records must not contain an empty member set")
    }

    /// Return member identities in canonical group order.
    #[must_use]
    pub fn member_ids(&self) -> impl ExactSizeIterator<Item = &MemberIdentity> + '_ {
        self.member_keys.member_ids()
    }

    /// Return the local member identity referenced by `local_member_index`.
    ///
    /// Store implementations must preserve the invariant that `local_member_index
    /// < member_keys.len()`.
    ///
    /// # Panics
    ///
    /// Panics if the stored local member index is outside the stored member-key
    /// order.
    #[must_use]
    pub fn local_member(&self) -> &MemberIdentity {
        &self
            .member_keys
            .member_key_at_index(self.local_member_index)
            .expect("replication group local member index must be in bounds")
            .member_id
    }

    /// Return whether another active record has the same group definition.
    ///
    /// Active progress and encrypted security material are intentionally not
    /// part of the definition comparison.
    #[must_use]
    pub fn matches_definition(&self, other: &Self) -> bool {
        self.group_id == other.group_id
            && self.member_keys == other.member_keys
            && self.local_member_index == other.local_member_index
            && self.group_schema == other.group_schema
    }

    /// Return whether this active group is compatible with stored material
    /// for the same group id.
    ///
    /// This intentionally ignores `version_vector`: active groups may already
    /// have applied local progress beyond the initial bootstrap state. All
    /// identity, schema, and security material must still match exactly.
    #[must_use]
    pub fn matches_group_material(&self, material: &ReplicationGroupMaterialRecord) -> bool {
        material.matches_definition(
            self.group_id,
            &self.member_keys,
            self.local_member_index,
            &self.group_schema,
        ) && self.security_material == material.security_material
    }
}

/// Persisted state that distinguishes active group material from inactive setup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicationGroupActiveState {
    /// Last applied version vector stored for the active group.
    pub version_vector: VersionVector,
    /// Current application-access and replication lifecycle.
    pub lifecycle: ReplicationGroupLifecycle,
}

/// Stored group definition and local security material, independent of activation.
///
/// Material may exist before the group becomes externally active. Dataset rows,
/// update logs, and active progress remain anchored to [`ReplicationGroupRecord`]
/// through the store's separate active-group marker.
#[derive(Clone, PartialEq, Eq)]
pub struct ReplicationGroupMaterialRecord {
    /// Stable replication-group identifier.
    pub group_id: GroupId,
    /// Canonical exact member-key order for the group.
    pub member_keys: GroupMemberKeys,
    /// Position of the local member within `member_keys`.
    pub local_member_index: MemberIndex,
    /// Dataset schemas fixed for the lifetime of this group.
    pub group_schema: GroupSchema,
    /// Already-encrypted group-security material needed by runtime operation.
    pub security_material: EncryptedGroupSecurityMaterial,
}

impl ReplicationGroupMaterialRecord {
    /// Return whether this material has the supplied group definition.
    ///
    /// Encrypted security material is intentionally excluded so callers can
    /// reject structural conflicts before performing cryptographic checks.
    #[must_use]
    pub fn matches_definition(
        &self,
        group_id: GroupId,
        member_keys: &GroupMemberKeys,
        local_member_index: MemberIndex,
        group_schema: &GroupSchema,
    ) -> bool {
        self.group_id == group_id
            && self.member_keys == *member_keys
            && self.local_member_index == local_member_index
            && self.group_schema == *group_schema
    }

    /// Return the number of members encoded in this material.
    ///
    /// # Panics
    ///
    /// Panics if an invalid material record was constructed with no member keys.
    #[must_use]
    pub fn member_count(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.member_keys.len())
            .expect("replication group material must not contain an empty member set")
    }

    /// Combine this material with active progress.
    #[must_use]
    pub fn activate(self, version_vector: VersionVector) -> ReplicationGroupRecord {
        ReplicationGroupRecord {
            group_id: self.group_id,
            member_keys: self.member_keys,
            local_member_index: self.local_member_index,
            group_schema: self.group_schema,
            version_vector,
            lifecycle: ReplicationGroupLifecycle::Open,
            security_material: self.security_material,
        }
    }
}

impl fmt::Debug for ReplicationGroupMaterialRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplicationGroupMaterialRecord")
            .field("group_id", &self.group_id)
            .field("member_keys", &self.member_keys)
            .field("local_member_index", &self.local_member_index)
            .field("group_schema", &self.group_schema)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for ReplicationGroupRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicationGroupRecord")
            .field("group_id", &self.group_id)
            .field("member_keys", &self.member_keys)
            .field("local_member_index", &self.local_member_index)
            .field("group_schema", &self.group_schema)
            .field("version_vector", &self.version_vector)
            .field("lifecycle", &self.lifecycle)
            .field("security_material", &self.security_material)
            .finish()
    }
}
