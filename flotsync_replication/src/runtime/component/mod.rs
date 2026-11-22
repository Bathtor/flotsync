//! Main replication runtime component and its group, update, and API workflows.

use super::{
    DEFAULT_MAX_INLINE_BOOTSTRAP_PUBLIC_KEY_BUNDLES,
    catch_up_manager::{CatchUpManagerMessage, NeedVersions, ObservedAvailable},
    config_keys,
    errors::{
        AcceptMigrationError,
        BuildGroupSetupSnafu,
        ChangeGroupMembershipError,
        ConflictingExistingGroupSnafu,
        CreateGroupError,
        CreatorNotInMembersSnafu,
        EmptyGroupNameSnafu,
        GroupActivationError,
        GroupInstallError,
        GroupLifecycleTransitionError,
        InboundDeliveryError,
        InboundFailureAction,
        InvalidMembersSnafu,
        LocalMemberMissingSnafu,
        PendingGroupActivationResumeSnafu,
        PublishChangesError,
        ReplayPendingDecisionSnafu,
        RuntimeStartupError,
        SecuritySnafu,
        StoreGroupSnafu,
        StoreStartupSnafu,
        accept_migration,
        activation,
        change_membership,
        group_lifecycle,
        inbound,
        publish,
    },
    group_state::SharedGroupState,
    in_memory::LoadedGroupMeta,
    pending_group,
    replies::{ReplyOutcome, complete_reply},
    summary_request_manager::SummaryRequestManagerMessage,
};
#[cfg(test)]
use crate::api::StoreError;
use crate::{
    api::{
        ApiError,
        BatchProvider,
        ChangeGroupMembershipRequest,
        CreateGroupRequest,
        DataChangeLineage,
        DataChangeReadPosition,
        DatasetId,
        EncryptedGroupSecurityMaterial,
        GroupInvitation,
        GroupInvitationResponder,
        GroupMemberKeys,
        GroupMigrationPolicy,
        GroupNameUpdate,
        GroupReadToken,
        GroupSchema,
        InitialSnapshot,
        ListenerError,
        MigrationCandidateProposal,
        MigrationId,
        MigrationProposal,
        MigrationProposalResponder,
        PendingGroupActivationRecord,
        PendingGroupDecisionRecord,
        PendingGroupWorkKey,
        PolicyDecision,
        ProviderExternalSnafu,
        PublishChangesRequest,
        PublishReceipt,
        RejectionReason,
        ReplicationConfig,
        ReplicationEvent,
        ReplicationEventListener,
        ReplicationGroupLifecycle,
        ReplicationGroupMaterialRecord,
        ReplicationGroupRecord,
        ReplicationStore,
        ReplicationStoreReadTransaction,
        ReplicationStoreTransaction,
        RowChange,
        RowId,
        RowKey,
        RowProvider,
        RowProviderError,
        api_error::ApiExternalSnafu,
        providers::VecRowProvider,
    },
    codecs::messages::{
        BootstrapMemberKeyMessage,
        GroupInvitationMessage,
        GroupSetupKey,
        GroupSetupMessage,
        MigrationProposalMessage,
        RuntimeMessage,
        RuntimeMessageDecodeContext,
        UpdateBatchMessage,
        UpdateMessage,
        UpdateRangeMessage,
    },
    delivery::{
        contracts::{
            GroupBroadcastPort,
            GroupBroadcastPortIndication,
            GroupBroadcastPortRequest,
            ReliableDeliveryPort,
            ReliableDeliveryPortIndication,
            ReliableDeliveryPortRequest,
        },
        group_broadcast::GroupBroadcastDeliver,
        reliable_delivery::{
            ReliableDeliveryDeliver,
            ReliableDeliverySubmit,
            ReliableMessageEnvelope,
            ReliableMessageHeader,
        },
        security::DeliverySecurity,
        shared::{DeliveryClass, MessageId, PlaintextPayload, ReliableMessageScope},
    },
};
use flotsync_core::{
    GroupId,
    MemberIdentity,
    MemberIndex,
    membership::{GroupMembers, SharedGroupMemberships},
    versions::{UpdateId, VersionVector},
};
use flotsync_messages::proto::{DecodeProtoViewWith, EncodeProto};
use flotsync_security::GROUP_CIPHER_SUITE_CHACHA20_POLY1305;
use flotsync_utils::{
    BoxFuture,
    OptionExt as _,
    ResultExt as _,
    kompact_config::ConfigReadExt as _,
};
use futures_util::FutureExt;
use kompact::prelude::*;
use roaring::RoaringBitmap;
use smallvec::{SmallVec, smallvec};
use snafu::{IntoError, prelude::*};
use std::{
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
    sync::Arc,
};
use uuid::Uuid;

use messages::PendingGroupDecisionResponseKind;
pub use messages::{PendingGroupDecisionResponse, ReplicationRuntimeMessage};
use summary_api::SummaryReplyRoute;
#[cfg(any(test, feature = "test-support"))]
pub use test_support::ReplicationRuntimeTestMessage;

use activation_provider::StoreActivationRowProvider;
use flow_driver::{FlowEffect, FlowIdAllocator, FlowProtocolError, FlowSlot, FlowStep};
use group_work::{
    ComponentBackedPendingGroupResponder,
    MigrationProposalArrival,
    PendingGroupAcceptOutcome,
    PendingGroupActivationOutcome,
    PreparedCreatedGroup,
    PreparedGroupSetup,
    PreparedMembershipDispatch,
    PreparedMembershipMigration,
    ProposedMembershipChange,
    ValidatedInboundGroupSetup,
    creator_first_member_order,
};
use inbound_flow::{InboundCompletion, InboundEffect, InboundFlow, InboundRequest};
use inbound_support::{
    InboundDeliveryContext,
    InboundDeliveryFailure,
    InboundUpdateOrigin,
    InboundUpdateOutcome,
    handled_after_inbound_failure,
    panic_if_fatal_inbound_failure,
    summary_from_message,
};
use listeners::{
    ListenerDataChanges,
    notify_listener_batches,
    notify_listener_data_change,
    notify_pending_activation_data_changes,
};
use publication::ScopedPublication;
use publication_flow::{PublicationCompletion, PublicationEffect, PublicationFlow};
use runtime_flow::{InboundResult, RuntimeCompletion, RuntimeEffect, RuntimeFlow, RuntimeOutcome};

mod activation_provider;
mod flow_driver;
mod group_work;
mod inbound_flow;
mod inbound_support;
mod listener_flow;
mod listeners;
mod messages;
mod publication;
mod publication_flow;
mod row_patches;
mod runtime_flow;
mod security_api;
mod summary_api;
#[cfg(any(test, feature = "test-support"))]
mod test_support;
mod update_ranges;

/// Stateful Kompact component that hosts one replication runtime.
///
/// Durable replication state now lives entirely in `ReplicationStore`. This
/// component owns the authoritative runtime group-state snapshot plus the
/// imperative bridge from public API calls and delivery events into
/// transactional store work.
#[derive(ComponentDefinition)]
pub struct ReplicationRuntimeComponent {
    ctx: ComponentContext<Self>,
    group_broadcast: RequiredPort<GroupBroadcastPort>,
    reliable_delivery: RequiredPort<ReliableDeliveryPort>,
    local_member: MemberIdentity,
    store: Arc<dyn ReplicationStore>,
    listener: Arc<dyn ReplicationEventListener>,
    config: ReplicationConfig,
    security: DeliverySecurity,
    group_memberships: Arc<SharedGroupState>,
    summary_request_manager: ActorRefStrong<SummaryRequestManagerMessage>,
    catch_up_manager: ActorRefStrong<CatchUpManagerMessage>,
    /// Admission slot for publication and inbound flows.
    /// Holds group/listener claims through transactions, submissions, callbacks and publication replies.
    operation_slot: FlowSlot<RuntimeFlow>,
    /// Shared ID sequence for all flows owned by this component; slot release does not reset it.
    flow_ids: FlowIdAllocator,
    /// Resolved group-size limit for including inline public key bundles in bootstrap messages.
    max_inline_bootstrap_public_key_bundles: usize,
}

impl ReplicationRuntimeComponent {
    /// Construct one replication runtime component for one local member.
    pub(super) fn new(
        identity: RuntimeIdentityContext,
        services: RuntimeApplicationServices,
        security: RuntimeSecurityContext,
        actors: RuntimeComponentActors,
    ) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            group_broadcast: RequiredPort::uninitialised(),
            reliable_delivery: RequiredPort::uninitialised(),
            local_member: identity.local_member,
            store: services.store,
            listener: services.listener,
            config: services.config,
            security: security.security,
            group_memberships: identity.group_memberships,
            summary_request_manager: actors.summary_request_manager,
            catch_up_manager: actors.catch_up_manager,
            operation_slot: FlowSlot::Available,
            flow_ids: FlowIdAllocator::default(),
            max_inline_bootstrap_public_key_bundles:
                DEFAULT_MAX_INLINE_BOOTSTRAP_PUBLIC_KEY_BUNDLES,
        }
    }

    /// Replace the listener while this component is still inactive.
    ///
    /// This method does not inspect Kompact lifecycle state. Calling it after
    /// activation can redirect callbacks while an event is already in flight
    /// and therefore violates the runtime's ordered-listener delivery contract.
    pub(super) fn replace_listener_before_start(
        &mut self,
        listener: Arc<dyn ReplicationEventListener>,
    ) {
        self.listener = listener;
    }

    /// Attempt one API reply, log a dropped receiver, and return the reply outcome.
    fn reply_api<T>(
        &self,
        promise: KPromise<Result<T, ApiError>>,
        operation: &'static str,
        reply: Result<T, ApiError>,
    ) -> ReplyOutcome
    where
        T: Send + 'static,
    {
        let outcome = complete_reply(promise, reply);
        if matches!(outcome, ReplyOutcome::ReceiverDropped) {
            warn!(self.log(), "dropping {operation} reply");
        }
        outcome
    }

    fn record_inbound_failure(&self, failure: &InboundDeliveryFailure) -> InboundFailureAction {
        let action = failure.error.failure_action();
        match action {
            InboundFailureAction::Drop => {
                warn!(
                    self.log(),
                    "dropping inbound {} after recoverable error: {}",
                    failure.context,
                    failure.error
                );
            }
            InboundFailureAction::Fatal => {
                error!(
                    self.log(),
                    "fatal inbound {} failure: {}", failure.context, failure.error
                );
            }
        }
        action
    }

    /// Validate stored material against the members and schema from accepted work.
    fn validate_activation_group_material(
        &self,
        group_id: GroupId,
        proposed_members: Vec<MemberIdentity>,
        group_schema: &GroupSchema,
        material: &ReplicationGroupMaterialRecord,
    ) -> Result<ReplicationGroupRecord, GroupActivationError> {
        let expected_members = GroupMembers::from_ordered_members(proposed_members)
            .context(activation::InvalidMembersSnafu)?;
        ensure!(
            expected_members.contains(&self.local_member),
            activation::LocalMemberMissingSnafu {
                local_member: self.local_member.clone(),
            }
        );
        let initial_versions = VersionVector::initial(material.member_count());
        let group_record = material.clone().activate(initial_versions);
        let local_group = LoadedGroupMeta::from_replication_group_record(
            &self.local_member,
            group_record.clone(),
        )
        .context(activation::InvalidPersistedGroupSnafu { group_id })?;
        ensure!(
            local_group.members.ordered_members() == expected_members.ordered_members()
                && group_record
                    .group_schema
                    .has_same_schema_definitions(group_schema),
            activation::ConflictingGroupMaterialSnafu { group_id }
        );
        Ok(group_record)
    }

    /// Enforce the unchanged-schema invariant for one hosted group replacement.
    fn validate_migration_activation_schema(
        old_group: &ReplicationGroupRecord,
        new_group_id: GroupId,
        new_group_schema: &GroupSchema,
    ) -> Result<(), GroupActivationError> {
        ensure!(
            old_group
                .group_schema
                .has_same_schema_definitions(new_group_schema),
            activation::MigrationSchemaMismatchSnafu {
                old_group_id: old_group.group_id,
                new_group_id,
            }
        );
        Ok(())
    }

    /// Build the canonical persisted record for one group definition whose
    /// exact member keys have already passed local membership validation.
    fn build_replication_group_record(
        &self,
        group_id: GroupId,
        group_name: Option<String>,
        member_keys: GroupMemberKeys,
        group_schema: GroupSchema,
        security_material: crate::api::EncryptedGroupSecurityMaterial,
    ) -> ReplicationGroupRecord {
        let material = self.build_replication_group_material_record(
            group_id,
            group_name,
            member_keys,
            group_schema,
            security_material,
        );
        let version_vector = VersionVector::initial(material.member_count());
        material.activate(version_vector)
    }

    /// Build stored group material without making the group active.
    fn build_replication_group_material_record(
        &self,
        group_id: GroupId,
        group_name: Option<String>,
        member_keys: GroupMemberKeys,
        group_schema: GroupSchema,
        security_material: crate::api::EncryptedGroupSecurityMaterial,
    ) -> ReplicationGroupMaterialRecord {
        NonZeroUsize::new(member_keys.len())
            .expect("group installation must keep members non-empty");
        let local_member_index = member_keys
            .member_index(&self.local_member)
            .expect("group installation validates the local member before persistence");
        ReplicationGroupMaterialRecord {
            group_id,
            group_name,
            member_keys,
            local_member_index,
            group_schema,
            security_material,
        }
    }

    /// Build one opaque group read token from currently stored group progress.
    fn group_read_token_from_groups(
        groups: impl IntoIterator<Item = ReplicationGroupRecord>,
        group_id: GroupId,
    ) -> GroupReadToken {
        let group = groups
            .into_iter()
            .find(|group| group.group_id == group_id)
            .expect("installed group progress must include the activated group");
        GroupReadToken::from_group_version(group_id, group.version_vector)
    }

    /// Validate an open old group and build its accepted read-only lifecycle.
    fn accepted_read_only_lifecycle(
        group: &ReplicationGroupRecord,
        successor_group_id: GroupId,
        final_versions: &VersionVector,
    ) -> Result<ReplicationGroupLifecycle, GroupLifecycleTransitionError> {
        ensure!(
            group.version_vector.num_members() == final_versions.num_members(),
            group_lifecycle::MemberCountMismatchSnafu {
                group_id: group.group_id,
                final_member_count: final_versions.num_members().get(),
                group_member_count: group.version_vector.num_members().get(),
            }
        );
        ensure!(
            group.lifecycle.is_writable(),
            group_lifecycle::NotOpenSnafu {
                group_id: group.group_id,
                lifecycle: group.lifecycle.clone(),
            }
        );
        Ok(ReplicationGroupLifecycle::ReadOnly {
            successor_group_id,
            final_versions: final_versions.clone(),
        })
    }

    /// Validate accepted old-group state before completing target activation.
    fn accepted_closed_lifecycle(
        group: &ReplicationGroupRecord,
        successor_group_id: GroupId,
        final_versions: &VersionVector,
    ) -> Result<ReplicationGroupLifecycle, GroupLifecycleTransitionError> {
        ensure!(
            group.version_vector.num_members() == final_versions.num_members(),
            group_lifecycle::MemberCountMismatchSnafu {
                group_id: group.group_id,
                final_member_count: final_versions.num_members().get(),
                group_member_count: group.version_vector.num_members().get(),
            }
        );
        match &group.lifecycle {
            ReplicationGroupLifecycle::ReadOnly {
                successor_group_id: accepted_successor,
                final_versions: accepted_versions,
            }
            | ReplicationGroupLifecycle::Closed {
                successor_group_id: accepted_successor,
                final_versions: accepted_versions,
            } if *accepted_successor == successor_group_id
                && accepted_versions == final_versions =>
            {
                Ok(ReplicationGroupLifecycle::Closed {
                    successor_group_id,
                    final_versions: final_versions.clone(),
                })
            }
            _ => group_lifecycle::AcceptedCutMismatchSnafu {
                group_id: group.group_id,
            }
            .fail(),
        }
    }

    /// Validate that group setup authorises both its sender and the local member.
    fn validate_group_setup_membership(
        group_id: GroupId,
        members: &GroupMembers,
        local_member: &MemberIdentity,
        sender: &MemberIdentity,
    ) -> Result<(), InboundDeliveryError> {
        let Some(sender_index) = members.member_index(sender) else {
            return Err(InboundDeliveryError::GroupSetupSenderNotInGroup {
                group_id,
                sender: sender.clone(),
            });
        };
        if sender_index != MemberIndex::new(0) {
            return Err(InboundDeliveryError::GroupSetupSenderNotFirstMember {
                group_id,
                sender: sender.clone(),
                sender_index: Some(sender_index),
            });
        }
        if !members.contains(local_member) {
            return Err(InboundDeliveryError::GroupSetupMissingLocalMember {
                group_id,
                local_member: local_member.clone(),
            });
        }
        Ok(())
    }

    /// Validate that pending work names a coherent member set including this runtime.
    fn validate_pending_group_members(
        &self,
        group_id: GroupId,
        proposed_members: &[MemberIdentity],
    ) -> Result<GroupMembers, InboundDeliveryError> {
        let members = GroupMembers::from_ordered_members(proposed_members.iter().cloned())
            .context(inbound::InvalidPendingGroupMembersSnafu)?;
        ensure!(
            members.contains(&self.local_member),
            inbound::PendingGroupMissingLocalMemberSnafu {
                group_id,
                local_member: self.local_member.clone(),
            }
        );
        Ok(members)
    }

    /// Resolve local policy for one validated group invitation.
    fn invitation_policy_decision(
        &self,
        invitation: &GroupInvitation,
    ) -> Result<PolicyDecision, InboundDeliveryError> {
        self.validate_pending_group_members(invitation.group_id, &invitation.proposed_members)?;
        Ok(match invitation.source {
            crate::api::GroupInvitationSource::Creation => {
                self.config.group_invitation_policy.creation
            }
            crate::api::GroupInvitationSource::Migration { .. } => {
                self.config.group_invitation_policy.migration_added_member
            }
        })
    }

    /// Resolve local policy for one migration proposal by comparing old and proposed members.
    async fn migration_policy_decision(
        &mut self,
        proposal: &MigrationProposal,
    ) -> Result<PolicyDecision, InboundDeliveryError> {
        let group_id = proposal.migration_id.new_group_id;
        let proposed_members =
            self.validate_pending_group_members(group_id, &proposal.proposed_members)?;
        let mut transaction = self
            .store
            .begin_read_transaction()
            .await
            .context(inbound::StoreAccessSnafu)?;
        let old_group_id = proposal.migration_id.old_group_id;
        let persisted_group = transaction
            .load_replication_group(&old_group_id)
            .await
            .context(inbound::StoreAccessSnafu)?
            .context(inbound::UnknownHostedGroupSnafu {
                group_id: old_group_id,
            })?;
        let schema_matches = persisted_group
            .group_schema
            .has_same_schema_definitions(&proposal.group_schema);
        transaction
            .release()
            .await
            .context(inbound::StoreAccessSnafu)?;
        ensure!(
            schema_matches,
            inbound::MigrationSchemaMismatchSnafu {
                old_group_id,
                new_group_id: proposal.migration_id.new_group_id,
            }
        );
        let local_group =
            LoadedGroupMeta::from_replication_group_record(&self.local_member, persisted_group)
                .context(inbound::InvalidPersistedGroupSnafu {
                    group_id: old_group_id,
                })?;
        Ok(Self::migration_policy_decision_for_members(
            &self.config.group_migration_policy,
            &local_group.members,
            &proposed_members,
        ))
    }

    /// Derive the configured migration policy from top-level membership differences.
    fn migration_policy_decision_for_members(
        policy: &GroupMigrationPolicy,
        old_members: &GroupMembers,
        proposed_members: &GroupMembers,
    ) -> PolicyDecision {
        let mut decision = policy.epoch_change;
        if old_members
            .iter()
            .any(|member| !proposed_members.contains(&member))
        {
            decision = decision.most_restrictive(policy.member_removed);
        }
        if proposed_members
            .iter()
            .any(|member| !old_members.contains(&member))
        {
            decision = decision.most_restrictive(policy.member_added);
        }
        decision
    }

    /// Resolve local policy for pending group work before accepting activation.
    async fn pending_group_policy_decision(
        &mut self,
        record: &PendingGroupDecisionRecord,
    ) -> Result<PolicyDecision, InboundDeliveryError> {
        if record.requires_snapshot_fetch() {
            return Ok(PolicyDecision::AutoReject);
        }
        match record {
            PendingGroupDecisionRecord::GroupInvitation(invitation) => {
                self.invitation_policy_decision(invitation)
            }
            PendingGroupDecisionRecord::MigrationProposal(proposal) => {
                self.migration_policy_decision(proposal).await
            }
        }
    }

    /// Notify the listener about already stored pending work.
    async fn notify_pending_group_decision_listener(
        &mut self,
        record: PendingGroupDecisionRecord,
    ) -> Result<(), InboundDeliveryError> {
        let runtime_ref = self
            .ctx
            .actor_ref()
            .hold()
            .expect("replication runtime actor ref must be available while running");
        match record {
            PendingGroupDecisionRecord::GroupInvitation(invitation) => {
                let key = PendingGroupDecisionRecord::GroupInvitation(invitation.clone()).key();
                let responder = ComponentBackedPendingGroupResponder::new(runtime_ref, key);
                self.listener
                    .on_event(ReplicationEvent::GroupInvitation {
                        invitation,
                        respond: Box::new(responder),
                    })
                    .await
                    .context(inbound::NotifyPendingGroupDecisionSnafu)
            }
            PendingGroupDecisionRecord::MigrationProposal(proposal) => {
                self.notify_migration_proposal_candidates(
                    runtime_ref,
                    proposal.migration_id.old_group_id,
                )
                .await
            }
        }
    }

    /// Load and emit the complete undecided candidate set for one old group.
    async fn notify_migration_proposal_candidates(
        &mut self,
        runtime_ref: ActorRefStrong<ReplicationRuntimeMessage>,
        old_group_id: GroupId,
    ) -> Result<(), InboundDeliveryError> {
        let mut transaction = self
            .store
            .begin_read_transaction()
            .await
            .context(inbound::StoreAccessSnafu)?;
        let pending_decisions = transaction
            .load_pending_group_decisions()
            .await
            .context(inbound::StoreAccessSnafu)?;
        transaction
            .release()
            .await
            .context(inbound::StoreAccessSnafu)?;
        let proposals = pending_decisions
            .into_iter()
            .filter_map(|record| match record {
                PendingGroupDecisionRecord::MigrationProposal(proposal)
                    if proposal.migration_id.old_group_id == old_group_id =>
                {
                    let key = PendingGroupWorkKey::MigrationProposal {
                        migration_id: proposal.migration_id,
                    };
                    let responder =
                        ComponentBackedPendingGroupResponder::new(runtime_ref.clone(), key);
                    Some(MigrationCandidateProposal {
                        proposal,
                        respond: Box::new(responder),
                    })
                }
                _ => None,
            })
            .collect::<SmallVec<[_; 1]>>();
        if proposals.is_empty() {
            return Ok(());
        }
        self.listener
            .on_event(ReplicationEvent::MigrationProposals { proposals })
            .await
            .context(inbound::NotifyPendingGroupDecisionSnafu)
    }

    fn notify_catch_up_needed(&self, group_id: GroupId, ranges: Vec<UpdateRangeMessage>) {
        if ranges.is_empty() {
            return;
        }
        self.catch_up_manager
            .tell(CatchUpManagerMessage::NeedVersions(NeedVersions {
                group_id,
                ranges,
            }));
    }

    fn notify_catch_up_available(&self, group_id: GroupId, ranges: Vec<UpdateRangeMessage>) {
        if ranges.is_empty() {
            return;
        }
        self.catch_up_manager
            .tell(CatchUpManagerMessage::ObservedAvailable(
                ObservedAvailable { group_id, ranges },
            ));
    }

    /// Bound old-group catch-up traffic after one migration direction is accepted.
    fn notify_catch_up_finalised(&self, group_id: GroupId, final_versions: VersionVector) {
        self.catch_up_manager
            .tell(CatchUpManagerMessage::FinaliseGroup {
                group_id,
                final_versions,
            });
    }

    /// Persist one newly observed group definition.
    ///
    /// If the same group already exists with the same canonical membership
    /// definition, this returns the stored record without mutating it.
    async fn store_new_replication_group(
        &mut self,
        record: ReplicationGroupRecord,
    ) -> Result<ReplicationGroupRecord, GroupInstallError> {
        let group_id = record.group_id;
        let mut transaction = self
            .store
            .begin_transaction()
            .await
            .context(StoreGroupSnafu { group_id })?;
        let existing = transaction
            .load_replication_group(&group_id)
            .await
            .context(StoreGroupSnafu { group_id })?;
        let persisted_group = if let Some(existing) = existing {
            ensure!(
                existing.matches_definition(&record),
                ConflictingExistingGroupSnafu { group_id }
            );
            existing
        } else {
            transaction
                .insert_replication_group(record.clone())
                .await
                .context(StoreGroupSnafu { group_id })?;
            record
        };
        let active_groups = transaction
            .load_replication_groups()
            .await
            .context(StoreGroupSnafu { group_id })?;
        let next_group_state = self
            .group_memberships
            .build_runtime_state_from_active_records(&self.local_member, active_groups)?;
        transaction
            .commit()
            .await
            .context(StoreGroupSnafu { group_id })?;
        self.group_memberships.replace(next_group_state);
        Ok(persisted_group)
    }

    /// Re-fire unresolved listener-mediated group decisions after startup.
    async fn replay_pending_group_decisions(
        &mut self,
        runtime_ref: ActorRefStrong<ReplicationRuntimeMessage>,
    ) -> Result<(), RuntimeStartupError> {
        let mut transaction = self
            .store
            .begin_read_transaction()
            .await
            .context(StoreStartupSnafu)?;
        let pending_work_items = transaction
            .load_pending_group_decisions()
            .await
            .context(StoreStartupSnafu)?;
        transaction.release().await.context(StoreStartupSnafu)?;

        let mut migration_groups: HashMap<GroupId, Vec<MigrationProposal>> = HashMap::new();
        for work in pending_work_items {
            match work {
                PendingGroupDecisionRecord::GroupInvitation(invitation) => {
                    let key = PendingGroupDecisionRecord::GroupInvitation(invitation.clone()).key();
                    let responder =
                        ComponentBackedPendingGroupResponder::new(runtime_ref.clone(), key);
                    let event = ReplicationEvent::GroupInvitation {
                        invitation,
                        respond: Box::new(responder),
                    };
                    self.listener
                        .on_event(event)
                        .await
                        .context(ReplayPendingDecisionSnafu)?;
                }
                PendingGroupDecisionRecord::MigrationProposal(proposal) => {
                    migration_groups
                        .entry(proposal.migration_id.old_group_id)
                        .or_default()
                        .push(proposal);
                }
            }
        }
        for proposals in migration_groups.into_values() {
            let proposals = proposals
                .into_iter()
                .map(|proposal| {
                    let key = PendingGroupWorkKey::MigrationProposal {
                        migration_id: proposal.migration_id,
                    };
                    let responder =
                        ComponentBackedPendingGroupResponder::new(runtime_ref.clone(), key);
                    MigrationCandidateProposal {
                        proposal,
                        respond: Box::new(responder),
                    }
                })
                .collect::<SmallVec<[_; 1]>>();
            self.listener
                .on_event(ReplicationEvent::MigrationProposals { proposals })
                .await
                .context(ReplayPendingDecisionSnafu)?;
        }
        Ok(())
    }

    /// Resume accepted group activations without re-asking the listener.
    async fn resume_pending_group_activations(&mut self) -> Result<(), RuntimeStartupError> {
        let mut transaction = self
            .store
            .begin_read_transaction()
            .await
            .context(StoreStartupSnafu)?;
        let pending_activations = transaction
            .load_pending_group_activations()
            .await
            .context(StoreStartupSnafu)?;
        transaction.release().await.context(StoreStartupSnafu)?;

        for activation in pending_activations {
            let outcome = self
                .activate_pending_group_record(activation)
                .await
                .context(PendingGroupActivationResumeSnafu)?;
            notify_pending_activation_data_changes(self.listener.clone(), outcome)
                .await
                .context(activation::NotifyListenerSnafu)
                .context(PendingGroupActivationResumeSnafu)?;
        }
        Ok(())
    }

    /// Accept one migration direction and discard all still-undecided competitors.
    async fn accept_migration_proposal(
        transaction: &mut dyn ReplicationStoreTransaction,
        proposal: MigrationProposal,
    ) -> Result<PendingGroupActivationRecord, AcceptMigrationError> {
        let old_group_id = proposal.migration_id.old_group_id;
        let old_group = transaction
            .load_replication_group(&old_group_id)
            .await
            .context(accept_migration::StoreAccessSnafu)?
            .context(group_lifecycle::UnknownGroupSnafu {
                group_id: old_group_id,
            })
            .context(accept_migration::LifecycleSnafu)?;
        let read_only = Self::accepted_read_only_lifecycle(
            &old_group,
            proposal.migration_id.new_group_id,
            &proposal.final_versions,
        )
        .context(accept_migration::LifecycleSnafu)?;

        let chosen_key = PendingGroupWorkKey::MigrationProposal {
            migration_id: proposal.migration_id,
        };
        let pending_decisions = transaction
            .load_pending_group_decisions()
            .await
            .context(accept_migration::StoreAccessSnafu)?;
        for candidate in pending_decisions {
            let PendingGroupDecisionRecord::MigrationProposal(candidate) = candidate else {
                continue;
            };
            if candidate.migration_id.old_group_id != old_group_id
                || candidate.migration_id == proposal.migration_id
            {
                continue;
            }
            let candidate_key = PendingGroupWorkKey::MigrationProposal {
                migration_id: candidate.migration_id,
            };
            transaction
                .remove_pending_group_decision(candidate_key)
                .await
                .context(accept_migration::StoreAccessSnafu)?;
            transaction
                .remove_inactive_replication_group_material(candidate.migration_id.new_group_id)
                .await
                .context(accept_migration::StoreAccessSnafu)?;
        }
        transaction
            .update_replication_group_lifecycle(&old_group_id, read_only)
            .await
            .context(accept_migration::StoreAccessSnafu)?;
        let activation = PendingGroupActivationRecord::MigrationProposal(proposal);
        debug_assert_eq!(activation.key(), chosen_key);
        transaction
            .upsert_pending_group_activation(activation.clone())
            .await
            .context(accept_migration::StoreAccessSnafu)?;
        Ok(activation)
    }

    /// Transition one matching listener decision into its accepted store state.
    async fn accept_pending_group_work(
        transaction: &mut dyn ReplicationStoreTransaction,
        key: PendingGroupWorkKey,
    ) -> Result<PendingGroupAcceptOutcome, ApiError> {
        let group_id = key.group_id();
        let pending_work = transaction
            .load_pending_group_decision(&group_id)
            .await
            .map_err(ApiError::from_store_classification_source)?;
        let Some(work) = pending_work.filter(|value| value.key() == key) else {
            return Ok(PendingGroupAcceptOutcome::Missing);
        };
        if work.requires_snapshot_fetch() {
            transaction
                .remove_pending_group_decision(key)
                .await
                .map_err(ApiError::from_store_classification_source)?;
            transaction
                .remove_inactive_replication_group_material(group_id)
                .await
                .map_err(ApiError::from_store_classification_source)?;
            return Ok(PendingGroupAcceptOutcome::UnsupportedSnapshot(group_id));
        }
        let activation = match work {
            PendingGroupDecisionRecord::GroupInvitation(invitation) => {
                let activation = PendingGroupActivationRecord::GroupInvitation(invitation);
                transaction
                    .upsert_pending_group_activation(activation.clone())
                    .await
                    .map_err(ApiError::from_store_classification_source)?;
                activation
            }
            PendingGroupDecisionRecord::MigrationProposal(proposal) => {
                Self::accept_migration_proposal(transaction, proposal)
                    .await
                    .map_err(ApiError::from_store_classification_source)?
            }
        };
        Ok(PendingGroupAcceptOutcome::Activation(Box::new(activation)))
    }

    /// Apply one listener response and return accepted work requiring activation.
    async fn apply_pending_group_decision_response(
        &mut self,
        response: PendingGroupDecisionResponse,
    ) -> Result<Option<Box<PendingGroupActivationRecord>>, ApiError> {
        let (work_key, response_kind) = response.into_parts();
        let mut accepted_activation = None;
        let mut unsupported_activation = None;
        let mut transaction = self
            .store
            .begin_transaction()
            .await
            .map_err(ApiError::from_store_classification_source)?;
        match response_kind {
            PendingGroupDecisionResponseKind::Accept => {
                match Self::accept_pending_group_work(transaction.as_mut(), work_key).await? {
                    PendingGroupAcceptOutcome::Missing => {
                        // A responder may outlive work resolved through another
                        // listener event. Acceptance is idempotent once no work
                        // remains, but log the stale response for diagnostics.
                        debug!(
                            self.log(),
                            "ignoring acceptance for missing pending group work {:?}", work_key,
                        );
                    }
                    PendingGroupAcceptOutcome::UnsupportedSnapshot(group_id) => {
                        unsupported_activation = Some(group_id);
                    }
                    PendingGroupAcceptOutcome::Activation(activation) => {
                        accepted_activation = Some(activation);
                    }
                }
            }
            PendingGroupDecisionResponseKind::Reject { .. } => {
                let rejected_group_id = match work_key {
                    PendingGroupWorkKey::GroupInvitation { group_id, .. } => group_id,
                    PendingGroupWorkKey::MigrationProposal { migration_id } => {
                        migration_id.new_group_id
                    }
                };
                transaction
                    .remove_pending_group_decision(work_key)
                    .await
                    .map_err(ApiError::from_store_classification_source)?;
                transaction
                    .remove_inactive_replication_group_material(rejected_group_id)
                    .await
                    .map_err(ApiError::from_store_classification_source)?;
            }
        }
        let active_groups = transaction
            .load_replication_groups()
            .await
            .map_err(ApiError::from_store_classification_source)?;
        let next_group_state = self
            .group_memberships
            .build_runtime_state_from_active_records(&self.local_member, active_groups)
            .boxed()
            .context(ApiExternalSnafu)?;
        transaction
            .commit()
            .await
            .map_err(ApiError::from_store_classification_source)?;
        self.group_memberships.replace(next_group_state);
        if let Some(PendingGroupActivationRecord::MigrationProposal(proposal)) =
            accepted_activation.as_deref()
        {
            self.notify_catch_up_finalised(
                proposal.migration_id.old_group_id,
                proposal.final_versions.clone(),
            );
        }
        if let Some(group_id) = unsupported_activation {
            return activation::UnsupportedInitialSnapshotSnafu { group_id }
                .fail::<Option<Box<PendingGroupActivationRecord>>>()
                .boxed()
                .context(ApiExternalSnafu);
        }
        Ok(accepted_activation)
    }

    /// Submit one runtime envelope through reliable delivery using its authority scope.
    fn submit_reliable_runtime_message(
        &mut self,
        recipient: MemberIdentity,
        message: &RuntimeMessage,
    ) {
        let payload = message.encode_proto_to_bytes();
        self.submit_reliable_runtime_payload(recipient, message.group_id(), payload);
    }

    /// Submit one already encoded runtime payload through reliable delivery.
    fn submit_reliable_runtime_payload(
        &mut self,
        recipient: MemberIdentity,
        scope_group_id: GroupId,
        payload: bytes::Bytes,
    ) {
        self.reliable_delivery
            .trigger(ReliableDeliveryPortRequest::Submit(
                ReliableDeliverySubmit {
                    envelope: ReliableMessageEnvelope::<PlaintextPayload> {
                        header: ReliableMessageHeader {
                            sender: self.local_member.clone(),
                            recipient,
                            message_id: MessageId(Uuid::new_v4()),
                            scope: ReliableMessageScope::Group {
                                group_id: scope_group_id,
                            },
                        },
                        payload: PlaintextPayload { bytes: payload },
                    },
                },
            ));
    }

    /// Fan one encoded creation invitation out to every remote target-group member.
    fn submit_group_creation_invitation_messages(
        &mut self,
        group_id: GroupId,
        group_setup: &GroupSetupMessage,
        payload: &bytes::Bytes,
    ) {
        // The current fire-and-forget delivery port cannot confirm that every
        // invitation was persisted before group creation returns. A crash can
        // therefore leave a locally created group with only a subset of its
        // remote bootstrap invitations recorded for delivery.
        let local_member = self.local_member.clone();
        for recipient in group_setup
            .members()
            .iter()
            .filter(|member| *member != &local_member)
            .cloned()
        {
            self.submit_reliable_runtime_payload(recipient, group_id, payload.clone());
        }
    }

    /// Send old-group migration proposals and new-group invitations for one change.
    fn submit_membership_migration_messages(&mut self, dispatch: &PreparedMembershipDispatch) {
        // The current fire-and-forget delivery port cannot confirm that every
        // bootstrap message was persisted before this fan-out returns. A crash
        // can therefore leave only a subset of the migration dispatch recorded.
        let proposed_members = dispatch.group_setup.members();
        // Index zero is the local member. Dispatch classifies only remote
        // recipients; every remote index absent from `added_member_indices`
        // necessarily names a continuing old-group member.
        for (member_index, recipient) in proposed_members.iter().enumerate().skip(1) {
            let member_index = u32::try_from(member_index).expect("group member indices fit u32");
            if dispatch.added_member_indices.contains(member_index) {
                self.submit_reliable_runtime_payload(
                    recipient.clone(),
                    dispatch.migration_id.new_group_id,
                    dispatch.invitation_payload.clone(),
                );
            } else {
                self.submit_reliable_runtime_payload(
                    recipient.clone(),
                    dispatch.migration_id.old_group_id,
                    dispatch.migration_payload.clone(),
                );
            }
        }
    }

    /// Encode one proposal and one invitation before local activation consumes the snapshot.
    fn prepare_membership_dispatch(
        prepared: &PreparedMembershipMigration,
    ) -> PreparedMembershipDispatch {
        let proposal = Self::prepared_migration_proposal(prepared);
        let proposed_members = proposal.proposed_members.clone();
        let proposal_message = MigrationProposalMessage::try_new(
            proposal,
            Arc::clone(&prepared.prepared_setup.group_setup),
        )
        .expect("prepared proposal members match prepared group setup");
        let migration_payload =
            RuntimeMessage::MigrationProposal(proposal_message).encode_proto_to_bytes();

        let invitation = GroupInvitation::new_migration(
            prepared.migration_id,
            proposed_members,
            prepared.group_schema.clone(),
            prepared.initial_snapshot.clone(),
            prepared.group_name.clone(),
            prepared.message.clone(),
        );
        let invitation_message = GroupInvitationMessage::try_new(
            invitation,
            Arc::clone(&prepared.prepared_setup.group_setup),
        )
        .expect("prepared invitation members match prepared group setup");
        let invitation_payload =
            RuntimeMessage::GroupInvitation(invitation_message).encode_proto_to_bytes();

        PreparedMembershipDispatch {
            migration_id: prepared.migration_id,
            group_setup: Arc::clone(&prepared.prepared_setup.group_setup),
            migration_payload,
            invitation_payload,
            added_member_indices: prepared.added_member_indices.clone(),
        }
    }

    /// Build the proposal value retained by reliable delivery and activation recovery.
    fn prepared_migration_proposal(prepared: &PreparedMembershipMigration) -> MigrationProposal {
        MigrationProposal {
            migration_id: prepared.migration_id,
            final_versions: prepared.final_versions.clone(),
            proposed_members: prepared.prepared_setup.group_setup.members().to_vec(),
            group_schema: prepared.group_schema.clone(),
            initial_snapshot: prepared.initial_snapshot.clone(),
            group_name: prepared.group_name.clone(),
            message: prepared.message.clone(),
        }
    }

    /// Validate one create-group request and derive the canonical persisted
    /// group record that should be written if the request succeeds.
    fn prepare_created_group(
        &self,
        req: CreateGroupRequest,
    ) -> Result<PreparedCreatedGroup, CreateGroupError> {
        let CreateGroupRequest {
            group_name,
            message,
            members,
            group_schema,
        } = req;
        let group_name = group_name.map(|name| name.trim().to_owned());
        ensure!(
            group_name.as_ref().is_none_or(|name| !name.is_empty()),
            EmptyGroupNameSnafu
        );
        let requested_members = creator_first_member_order(members, &self.local_member)?;
        let members =
            GroupMembers::from_ordered_members(requested_members).context(InvalidMembersSnafu)?;
        ensure!(
            members.contains(&self.local_member),
            LocalMemberMissingSnafu {
                local_member: self.local_member.clone(),
            }
        );

        Ok(PreparedCreatedGroup {
            group_id: GroupId::new_random(),
            group_name,
            message,
            members,
            group_schema,
        })
    }

    /// Read the maximum group size for inlining bootstrap public key bundles.
    fn read_max_inline_bootstrap_public_key_bundles(&self) -> usize {
        self.ctx.config().read_or_default_warn(
            self.log(),
            &config_keys::BOOTSTRAP_MAX_INLINE_PUBLIC_KEY_BUNDLES,
        )
    }

    pub(super) async fn prepare_group_setup(
        security: &DeliverySecurity,
        max_inline_public_key_bundles: usize,
        group_id: GroupId,
        members: &GroupMembers,
    ) -> Result<PreparedGroupSetup, CreateGroupError> {
        let group_key = DeliverySecurity::generate_group_key().context(SecuritySnafu)?;
        let public_keys = security
            .public_keys_for_members(members)
            .await
            .context(SecuritySnafu)?;
        let inline_public_bundles = members.len() <= max_inline_public_key_bundles;
        let setup_member_keys = public_keys.map_values(|public_keys| {
            if inline_public_bundles {
                BootstrapMemberKeyMessage::from_public_keys(public_keys)
            } else {
                BootstrapMemberKeyMessage::from_fingerprint(public_keys.fingerprint())
            }
        });
        let security_material = security
            .seal_group_secret(group_id.0, &group_key)
            .context(SecuritySnafu)?;
        let group_setup = GroupSetupMessage::new(
            members.ordered_members(),
            setup_member_keys,
            GROUP_CIPHER_SUITE_CHACHA20_POLY1305,
            GroupSetupKey::from_group_key(group_key),
        )
        .context(BuildGroupSetupSnafu)?;
        Ok(PreparedGroupSetup {
            security_material,
            group_setup: Arc::new(group_setup),
        })
    }

    /// Translate group-bootstrap preparation errors into membership-change failures.
    fn map_create_group_error(error: CreateGroupError) -> ChangeGroupMembershipError {
        match error {
            CreateGroupError::EmptyGroupName => ChangeGroupMembershipError::EmptyGroupName,
            CreateGroupError::LocalMemberMissing { local_member } => {
                ChangeGroupMembershipError::LocalMemberMissing { local_member }
            }
            CreateGroupError::CreatorNotInMembers { creator } => {
                ChangeGroupMembershipError::LocalMemberMissing {
                    local_member: creator,
                }
            }
            CreateGroupError::InvalidMembers { source } => {
                ChangeGroupMembershipError::InvalidMembers { source }
            }
            CreateGroupError::Security { source } => {
                ChangeGroupMembershipError::Security { source }
            }
            CreateGroupError::BuildGroupSetup { source } => {
                ChangeGroupMembershipError::BuildGroupSetup { source }
            }
        }
    }

    /// Compute canonical proposed membership and outbound recipient classifications.
    fn proposed_members_for_change(
        &self,
        current_members: &GroupMembers,
        req: &ChangeGroupMembershipRequest,
    ) -> Result<ProposedMembershipChange, ChangeGroupMembershipError> {
        let current_member_set = current_members.iter().collect::<HashSet<_>>();
        ensure!(
            current_member_set.contains(&self.local_member),
            change_membership::LocalMemberMissingSnafu {
                local_member: self.local_member.clone(),
            }
        );
        let mut final_members = current_member_set.clone();
        for member in &req.remove_members {
            final_members.remove(member);
        }
        final_members.extend(req.add_members.iter().cloned());
        ensure!(
            final_members.remove(&self.local_member),
            change_membership::LocalMemberMissingSnafu {
                local_member: self.local_member.clone(),
            }
        );

        let mut proposed_member_list = Vec::with_capacity(final_members.len() + 1);
        proposed_member_list.push(self.local_member.clone());
        let mut added_member_indices = RoaringBitmap::new();
        for member in final_members {
            let member_index = u32::try_from(proposed_member_list.len())
                .expect("group member indices fit into u32");
            if !current_member_set.contains(&member) {
                added_member_indices.insert(member_index);
            }
            proposed_member_list.push(member);
        }
        let proposed_members = GroupMembers::from_ordered_members(proposed_member_list)
            .context(change_membership::InvalidMembersSnafu)?;
        Ok(ProposedMembershipChange {
            proposed_members,
            added_member_indices,
        })
    }

    /// Prepare local state and outbound payloads for a membership change request.
    async fn prepare_membership_migration(
        &mut self,
        req: ChangeGroupMembershipRequest,
    ) -> Result<PreparedMembershipMigration, ChangeGroupMembershipError> {
        let old_group_id = req.group_id;
        ensure!(
            old_group_id != GroupId::NIL,
            change_membership::NilGroupIdSnafu
        );
        let mut transaction = self
            .store
            .begin_read_transaction()
            .await
            .context(change_membership::StoreAccessSnafu)?;
        let persisted_group = transaction
            .load_replication_group(&old_group_id)
            .await
            .context(change_membership::StoreAccessSnafu)?
            .context(change_membership::UnknownGroupSnafu {
                group_id: old_group_id,
            })?;
        ensure!(
            persisted_group.lifecycle.is_writable(),
            change_membership::GroupNotWritableSnafu {
                group_id: old_group_id,
            }
        );
        let group_schema = persisted_group.group_schema.clone();
        let inherited_group_name = persisted_group.group_name.clone();
        let local_group =
            LoadedGroupMeta::from_replication_group_record(&self.local_member, persisted_group)
                .context(change_membership::InvalidPersistedGroupSnafu {
                    group_id: old_group_id,
                })?;
        let proposed_change = self.proposed_members_for_change(&local_group.members, &req)?;
        let group_name = match req.group_name {
            GroupNameUpdate::Clear => None,
            GroupNameUpdate::Inherit => inherited_group_name,
            GroupNameUpdate::Set(name) => {
                let name = name.trim().to_owned();
                ensure!(!name.is_empty(), change_membership::EmptyGroupNameSnafu);
                Some(name)
            }
        };
        let final_versions = local_group.version_vector.clone();
        let initial_snapshot = pending_group::build_inline_initial_snapshot(
            transaction.as_mut(),
            old_group_id,
            &group_schema,
        )
        .await?;
        transaction
            .release()
            .await
            .context(change_membership::StoreAccessSnafu)?;

        let new_group_id = GroupId::new_random();
        let prepared_setup = Self::prepare_group_setup(
            &self.security,
            self.max_inline_bootstrap_public_key_bundles,
            new_group_id,
            &proposed_change.proposed_members,
        )
        .await
        .map_err(Self::map_create_group_error)?;
        let migration_id = MigrationId {
            old_group_id,
            new_group_id,
        };
        Ok(PreparedMembershipMigration {
            migration_id,
            final_versions,
            group_schema,
            initial_snapshot,
            prepared_setup,
            added_member_indices: proposed_change.added_member_indices,
            group_name,
            message: req.message,
        })
    }

    /// Build the stored group record represented by prepared bootstrap material.
    fn prepared_migration_group_record(
        &self,
        prepared: &PreparedMembershipMigration,
    ) -> Result<ReplicationGroupRecord, GroupActivationError> {
        let member_keys = GroupMemberKeys::from_ordered_member_keys(
            prepared.prepared_setup.group_setup.ordered_member_key_ids(),
        )
        .context(activation::InvalidMembersSnafu)?;
        Ok(self.build_replication_group_record(
            prepared.migration_id.new_group_id,
            prepared.group_name.clone(),
            member_keys,
            prepared.group_schema.clone(),
            prepared.prepared_setup.security_material.clone(),
        ))
    }

    /// Persist locally accepted migration work before target activation starts.
    async fn stage_prepared_membership_migration(
        &mut self,
        prepared: &PreparedMembershipMigration,
    ) -> Result<PendingGroupActivationRecord, AcceptMigrationError> {
        let record = self
            .prepared_migration_group_record(prepared)
            .context(accept_migration::PrepareTargetSnafu)?;
        let (material, _) = record.into_parts();
        let proposal = Self::prepared_migration_proposal(prepared);
        let mut transaction = self
            .store
            .begin_transaction()
            .await
            .context(accept_migration::StoreAccessSnafu)?;
        transaction
            .ensure_replication_group_material(material)
            .await
            .context(accept_migration::StoreAccessSnafu)?;
        let activation = Self::accept_migration_proposal(transaction.as_mut(), proposal).await?;
        let active_groups = transaction
            .load_replication_groups()
            .await
            .context(accept_migration::StoreAccessSnafu)?;
        let next_group_state = self
            .group_memberships
            .build_runtime_state_from_active_records(&self.local_member, active_groups)
            .context(activation::InstallGroupSnafu {
                group_id: prepared.migration_id.new_group_id,
            })
            .context(accept_migration::PrepareTargetSnafu)?;
        transaction
            .commit()
            .await
            .context(accept_migration::StoreAccessSnafu)?;
        self.group_memberships.replace(next_group_state);
        self.notify_catch_up_finalised(
            prepared.migration_id.old_group_id,
            prepared.final_versions.clone(),
        );
        Ok(activation)
    }

    /// Activate accepted pending work using its atomically stored group material.
    async fn activate_pending_group_record(
        &mut self,
        record: PendingGroupActivationRecord,
    ) -> Result<PendingGroupActivationOutcome, GroupActivationError> {
        let activation_record = record.into_activation_record();
        let key = activation_record.key;
        let group_id = activation_record.group_id;
        let migration_cutover = activation_record.migration_cutover;
        let group_schema = activation_record.group_schema;
        let mut transaction = self
            .store
            .begin_transaction()
            .await
            .context(activation::StoreAccessSnafu)?;
        let mut material = transaction
            .load_replication_group_material(&group_id)
            .await
            .context(activation::StoreAccessSnafu)?
            .context(activation::MissingGroupMaterialSnafu { group_id })?;
        material.group_name = activation_record.group_name;
        transaction
            .ensure_replication_group_material(material.clone())
            .await
            .context(activation::StoreAccessSnafu)?;
        let group_record = self.validate_activation_group_material(
            group_id,
            activation_record.proposed_members,
            &group_schema,
            &material,
        )?;
        let member_count = group_record.member_count();
        transaction
            .activate_replication_group(group_id, group_record.version_vector.clone())
            .await
            .context(activation::StoreAccessSnafu)?;
        pending_group::embed_initial_snapshot(
            transaction.as_mut(),
            group_id,
            member_count,
            &group_schema,
            activation_record.initial_snapshot,
        )
        .await?;
        let mut hosted_predecessor = None;
        if let Some(cutover) = &migration_cutover {
            let old_group = transaction
                .load_replication_group(&cutover.old_group_id)
                .await
                .context(activation::StoreAccessSnafu)?
                .context(group_lifecycle::UnknownGroupSnafu {
                    group_id: cutover.old_group_id,
                })
                .context(activation::CloseOldGroupSnafu)?;
            Self::validate_migration_activation_schema(&old_group, group_id, &group_schema)?;
            let closed =
                Self::accepted_closed_lifecycle(&old_group, group_id, &cutover.final_versions)
                    .context(activation::CloseOldGroupSnafu)?;
            hosted_predecessor =
                Some((old_group.local_member_index, cutover.final_versions.clone()));
            transaction
                .update_replication_group_lifecycle(&cutover.old_group_id, closed)
                .await
                .context(activation::StoreAccessSnafu)?;
        }
        transaction
            .remove_pending_group_activation(key)
            .await
            .context(activation::StoreAccessSnafu)?;
        let active_groups = transaction
            .load_replication_groups()
            .await
            .context(activation::StoreAccessSnafu)?;
        let next_group_state = self
            .group_memberships
            .build_runtime_state_from_active_records(
                &self.local_member,
                active_groups.iter().cloned(),
            )
            .context(activation::InstallGroupSnafu { group_id })?;
        let read_token = Self::group_read_token_from_groups(active_groups, group_id);
        transaction
            .commit()
            .await
            .context(activation::StoreAccessSnafu)?;
        self.group_memberships.replace(next_group_state);
        let outcome = self
            .build_pending_activation_outcome(key, &group_schema, hosted_predecessor, read_token)
            .await?;
        Ok(outcome)
    }

    /// Build the listener event payload after the accepted activation has committed.
    ///
    /// Failure to open the provider transaction occurs after pending work has
    /// been removed. Callers must turn that error into an unrecoverable runtime
    /// fault so the application can restart and explicitly resynchronise rows.
    async fn build_pending_activation_outcome(
        &mut self,
        key: PendingGroupWorkKey,
        group_schema: &GroupSchema,
        hosted_predecessor: Option<(MemberIndex, VersionVector)>,
        read_token: GroupReadToken,
    ) -> Result<PendingGroupActivationOutcome, GroupActivationError> {
        let group_id = key.group_id();
        let read_transaction = self
            .store
            .begin_read_transaction()
            .await
            .context(activation::PostCommitStoreAccessSnafu { group_id })?;
        let (read_position, rows): (DataChangeReadPosition, Box<RowProvider>) = match key {
            PendingGroupWorkKey::GroupInvitation {
                group_id,
                source: crate::api::GroupInvitationSource::Creation,
            } => (
                DataChangeReadPosition::new(DataChangeLineage::Update, read_token),
                Box::new(StoreActivationRowProvider::for_creation(
                    read_transaction,
                    group_id,
                    group_schema,
                )),
            ),
            PendingGroupWorkKey::GroupInvitation {
                source: crate::api::GroupInvitationSource::Migration { migration_id },
                ..
            } => (
                DataChangeReadPosition::new(
                    DataChangeLineage::GroupReplacement { migration_id },
                    read_token,
                ),
                Box::new(StoreActivationRowProvider::unavailable_replacement(
                    read_transaction,
                    migration_id,
                    group_schema,
                )),
            ),
            PendingGroupWorkKey::MigrationProposal { migration_id } => {
                let (local_member_index, final_versions) = hosted_predecessor
                    .expect("migration proposal activation must close its hosted predecessor");
                (
                    DataChangeReadPosition::new(
                        DataChangeLineage::GroupReplacement { migration_id },
                        read_token,
                    ),
                    Box::new(StoreActivationRowProvider::hosted_replacement(
                        read_transaction,
                        migration_id,
                        group_schema,
                        local_member_index,
                        final_versions,
                    )),
                )
            }
        };
        Ok(PendingGroupActivationOutcome {
            read_position,
            rows,
        })
    }

    /// Scope the request and resolve its immutable schema before admitting store work.
    fn prepare_publication(
        &self,
        request: PublishChangesRequest,
    ) -> Result<(ScopedPublication, Arc<GroupSchema>), PublishChangesError> {
        let scoped = publication::scope_request(request)?;
        let group_id = scoped.group_id();
        let group_schema = self
            .group_memberships
            .group_schema(&group_id)
            .context(publish::UnknownGroupSnafu { group_id })?;
        Ok((scoped, group_schema))
    }

    fn handle_inbound_pending_group_decision(
        &mut self,
        context: InboundDeliveryContext,
        deliver: ReliableDeliveryDeliver,
        record: PendingGroupDecisionRecord,
        group_setup: Arc<GroupSetupMessage>,
    ) -> HandlerResult {
        let group_id = record.group_id();
        let sender = deliver.envelope.header.sender.clone();
        let processed = deliver.processed;
        Handled::block_on(self, async move |mut async_self| {
            let reply = async {
                async_self
                    .install_pending_group_delivery(record, group_setup, sender)
                    .await?;
                processed
                    .complete()
                    .context(inbound::CompleteProcessedPromiseSnafu { group_id })
            }
            .await;
            if let Err(error) = reply {
                if error.is_unrecoverable() {
                    return Err(error)
                        .whatever_unrecoverable("inbound group activation failed after commit");
                }
                let failure = InboundDeliveryFailure::new(context, error);
                let action = async_self.record_inbound_failure(&failure);
                panic_if_fatal_inbound_failure(action, &failure);
            }
            Handled::OK
        })
    }

    /// Classify a proposal using cheap persisted ids and cuts before setup decryption.
    async fn classify_migration_proposal_arrival(
        &mut self,
        proposal: &MigrationProposal,
    ) -> Result<MigrationProposalArrival, InboundDeliveryError> {
        let old_group_id = proposal.migration_id.old_group_id;
        let mut transaction = self
            .store
            .begin_read_transaction()
            .await
            .context(inbound::StoreAccessSnafu)?;
        let old_group = transaction
            .load_replication_group(&old_group_id)
            .await
            .context(inbound::StoreAccessSnafu)?
            .context(inbound::UnknownHostedGroupSnafu {
                group_id: old_group_id,
            })?;
        let schema_matches = old_group
            .group_schema
            .has_same_schema_definitions(&proposal.group_schema);
        let arrival = Self::classify_migration_proposal_lifecycle(&old_group.lifecycle, proposal);
        transaction
            .release()
            .await
            .context(inbound::StoreAccessSnafu)?;
        ensure!(
            schema_matches,
            inbound::MigrationSchemaMismatchSnafu {
                old_group_id,
                new_group_id: proposal.migration_id.new_group_id,
            }
        );
        Ok(arrival)
    }

    /// Compare one proposal with the accepted target and cut of an old group.
    fn classify_migration_proposal_lifecycle(
        lifecycle: &ReplicationGroupLifecycle,
        proposal: &MigrationProposal,
    ) -> MigrationProposalArrival {
        let new_group_id = proposal.migration_id.new_group_id;
        match lifecycle {
            ReplicationGroupLifecycle::Open => MigrationProposalArrival::Candidate,
            ReplicationGroupLifecycle::ReadOnly {
                successor_group_id,
                final_versions,
            }
            | ReplicationGroupLifecycle::Closed {
                successor_group_id,
                final_versions,
            } if *successor_group_id == new_group_id
                && *final_versions == proposal.final_versions =>
            {
                MigrationProposalArrival::AcceptedReplay
            }
            _ => MigrationProposalArrival::RejectedConflict,
        }
    }

    /// Validate inbound setup and reuse matching encrypted material on replay.
    ///
    /// Validation intentionally finishes its read transaction before security
    /// checks because those checks may perform independent security-store
    /// operations. Kompact serialises work through this component, so its group
    /// state cannot change through the runtime between this phase and pending
    /// persistence. The later write transaction remains authoritative: it
    /// ensures compatible group material, refreshes metadata, and rejects
    /// conflicting target-keyed work.
    async fn validate_inbound_group_setup(
        &mut self,
        record: &PendingGroupDecisionRecord,
        group_setup: &GroupSetupMessage,
        sender: &MemberIdentity,
    ) -> Result<ValidatedInboundGroupSetup, InboundDeliveryError> {
        let group_id = record.group_id();
        let group_name = record.group_name().map(str::to_owned);
        let members = GroupMembers::from_ordered_members(group_setup.members().to_vec())
            .context(inbound::InvalidGroupSetupMembersSnafu)?;
        Self::validate_group_setup_membership(group_id, &members, &self.local_member, sender)?;
        let member_keys =
            GroupMemberKeys::from_ordered_member_keys(group_setup.ordered_member_key_ids())
                .context(inbound::InvalidGroupSetupMembersSnafu)?;
        let local_member_index = member_keys
            .member_index(&self.local_member)
            .expect("group setup membership validation requires the local member");
        let group_schema = record.group_schema().clone();
        let mut read_transaction = self
            .store
            .begin_read_transaction()
            .await
            .context(inbound::StoreAccessSnafu)?;
        let existing_material = read_transaction
            .load_replication_group_material(&group_id)
            .await
            .context(inbound::StoreAccessSnafu)?;
        let already_active = read_transaction
            .load_replication_group(&group_id)
            .await
            .context(inbound::StoreAccessSnafu)?
            .is_some();
        read_transaction
            .release()
            .await
            .context(inbound::StoreAccessSnafu)?;

        let material = if let Some(mut existing_material) = existing_material {
            let definition_matches = existing_material.matches_definition(
                group_id,
                &member_keys,
                local_member_index,
                &group_schema,
            );
            if !definition_matches {
                let result: Result<(), GroupInstallError> =
                    ConflictingExistingGroupSnafu { group_id }.fail();
                result.context(inbound::InstallGroupSetupSnafu { group_id })?;
            }
            self.security
                .validate_existing_group_setup_security(
                    group_id,
                    group_setup,
                    sender,
                    &existing_material.security_material,
                )
                .await
                .context(inbound::GroupSetupSecuritySnafu)?;
            existing_material.group_name = group_name;
            existing_material
        } else {
            let security_material = self
                .security
                .prepare_security_material_from_group_setup(group_id, group_setup, sender)
                .await
                .context(inbound::GroupSetupSecuritySnafu)?;
            ReplicationGroupMaterialRecord {
                group_id,
                group_name,
                member_keys,
                local_member_index,
                group_schema,
                security_material,
            }
        };
        Ok(ValidatedInboundGroupSetup {
            material,
            already_active,
        })
    }

    /// Remove any inactive state for work rejected by current policy.
    async fn reject_pending_group_delivery(
        &mut self,
        key: PendingGroupWorkKey,
        group_id: GroupId,
    ) -> Result<(), InboundDeliveryError> {
        let mut transaction = self
            .store
            .begin_transaction()
            .await
            .context(inbound::StoreAccessSnafu)?;
        transaction
            .remove_pending_group_decision(key)
            .await
            .context(inbound::StoreAccessSnafu)?;
        transaction
            .remove_pending_group_activation(key)
            .await
            .context(inbound::StoreAccessSnafu)?;
        transaction
            .remove_inactive_replication_group_material(group_id)
            .await
            .context(inbound::StoreAccessSnafu)?;
        transaction
            .commit()
            .await
            .context(inbound::StoreAccessSnafu)
    }

    /// Atomically persist verified material and unresolved listener work.
    async fn persist_pending_group_listener_decision(
        &mut self,
        material: ReplicationGroupMaterialRecord,
        record: PendingGroupDecisionRecord,
    ) -> Result<(), InboundDeliveryError> {
        let mut transaction = self
            .store
            .begin_transaction()
            .await
            .context(inbound::StoreAccessSnafu)?;
        transaction
            .ensure_replication_group_material(material)
            .await
            .context(inbound::StoreAccessSnafu)?;
        transaction
            .upsert_pending_group_decision(record)
            .await
            .context(inbound::StoreAccessSnafu)?;
        transaction
            .commit()
            .await
            .context(inbound::StoreAccessSnafu)
    }

    /// Atomically persist verified material and accepted activation work.
    async fn persist_pending_group_auto_activation(
        &mut self,
        material: ReplicationGroupMaterialRecord,
        record: PendingGroupDecisionRecord,
    ) -> Result<PendingGroupActivationRecord, InboundDeliveryError> {
        let mut transaction = self
            .store
            .begin_transaction()
            .await
            .context(inbound::StoreAccessSnafu)?;
        transaction
            .ensure_replication_group_material(material)
            .await
            .context(inbound::StoreAccessSnafu)?;
        let activation = match record {
            PendingGroupDecisionRecord::GroupInvitation(invitation) => {
                let activation = PendingGroupActivationRecord::GroupInvitation(invitation);
                transaction
                    .upsert_pending_group_activation(activation.clone())
                    .await
                    .context(inbound::StoreAccessSnafu)?;
                activation
            }
            PendingGroupDecisionRecord::MigrationProposal(proposal) => {
                Self::accept_migration_proposal(transaction.as_mut(), proposal)
                    .await
                    .context(inbound::AcceptMigrationSnafu)?
            }
        };
        let next_group_state =
            if let PendingGroupActivationRecord::MigrationProposal(proposal) = &activation {
                let group_id = proposal.migration_id.old_group_id;
                let active_groups = transaction
                    .load_replication_groups()
                    .await
                    .context(inbound::StoreAccessSnafu)?;
                let snapshot = self
                    .group_memberships
                    .build_runtime_state_from_active_records(&self.local_member, active_groups)
                    .context(inbound::InvalidPersistedGroupSnafu { group_id })?;
                Some(snapshot)
            } else {
                None
            };
        transaction
            .commit()
            .await
            .context(inbound::StoreAccessSnafu)?;
        if let Some(next_group_state) = next_group_state {
            self.group_memberships.replace(next_group_state);
        }
        if let PendingGroupActivationRecord::MigrationProposal(proposal) = &activation {
            self.notify_catch_up_finalised(
                proposal.migration_id.old_group_id,
                proposal.final_versions.clone(),
            );
        }
        Ok(activation)
    }

    /// Verify and persist one invitation or admissible migration candidate.
    ///
    /// Once a target is active, source and consumed-snapshot replay validation
    /// is optional; this implementation still validates material and security
    /// before refreshing metadata without reopening the decision.
    async fn install_pending_group_candidate(
        &mut self,
        record: PendingGroupDecisionRecord,
        group_setup: Arc<GroupSetupMessage>,
        sender: MemberIdentity,
    ) -> Result<(), InboundDeliveryError> {
        let group_id = record.group_id();
        let validated = self
            .validate_inbound_group_setup(&record, group_setup.as_ref(), &sender)
            .await?;
        if validated.already_active {
            let mut transaction = self
                .store
                .begin_transaction()
                .await
                .context(inbound::StoreAccessSnafu)?;
            transaction
                .ensure_replication_group_material(validated.material)
                .await
                .context(inbound::StoreAccessSnafu)?;
            let active_groups = transaction
                .load_replication_groups()
                .await
                .context(inbound::StoreAccessSnafu)?;
            let next_group_state = self
                .group_memberships
                .build_runtime_state_from_active_records(&self.local_member, active_groups)
                .context(inbound::InvalidPersistedGroupSnafu { group_id })?;
            transaction
                .commit()
                .await
                .context(inbound::StoreAccessSnafu)?;
            self.group_memberships.replace(next_group_state);
            return Ok(());
        }

        match self.pending_group_policy_decision(&record).await? {
            PolicyDecision::AutoReject => {
                self.reject_pending_group_delivery(record.key(), group_id)
                    .await
            }
            PolicyDecision::AskListener => {
                self.persist_pending_group_listener_decision(validated.material, record.clone())
                    .await?;
                self.notify_pending_group_decision_listener(record).await
            }
            PolicyDecision::AutoAccept => {
                let activation = self
                    .persist_pending_group_auto_activation(validated.material, record)
                    .await?;
                let outcome = self
                    .activate_pending_group_record(activation)
                    .await
                    .context(inbound::PendingGroupActivationSnafu)?;
                notify_pending_activation_data_changes(self.listener.clone(), outcome)
                    .await
                    .context(inbound::NotifyListenerSnafu)
            }
        }
    }

    /// Refresh metadata carried by a replay of an already accepted migration.
    ///
    /// The accepted old-group direction validates the target and cut. The
    /// consumed initial snapshot is intentionally not revalidated.
    async fn refresh_accepted_migration_replay(
        &mut self,
        record: PendingGroupDecisionRecord,
        group_setup: Arc<GroupSetupMessage>,
        sender: MemberIdentity,
    ) -> Result<(), InboundDeliveryError> {
        let validated = self
            .validate_inbound_group_setup(&record, group_setup.as_ref(), &sender)
            .await?;
        let group_id = record.group_id();
        let already_active = validated.already_active;
        let mut transaction = self
            .store
            .begin_transaction()
            .await
            .context(inbound::StoreAccessSnafu)?;
        transaction
            .ensure_replication_group_material(validated.material)
            .await
            .context(inbound::StoreAccessSnafu)?;
        if !already_active {
            transaction
                .upsert_pending_group_activation(record.into_activation())
                .await
                .context(inbound::StoreAccessSnafu)?;
        }
        let next_group_state = if already_active {
            let active_groups = transaction
                .load_replication_groups()
                .await
                .context(inbound::StoreAccessSnafu)?;
            let snapshot = self
                .group_memberships
                .build_runtime_state_from_active_records(&self.local_member, active_groups)
                .context(inbound::InvalidPersistedGroupSnafu { group_id })?;
            Some(snapshot)
        } else {
            None
        };
        transaction
            .commit()
            .await
            .context(inbound::StoreAccessSnafu)?;
        if let Some(next_group_state) = next_group_state {
            self.group_memberships.replace(next_group_state);
        }
        Ok(())
    }

    /// Classify migration arrivals before installing admissible pending work.
    async fn install_pending_group_delivery(
        &mut self,
        record: PendingGroupDecisionRecord,
        group_setup: Arc<GroupSetupMessage>,
        sender: MemberIdentity,
    ) -> Result<(), InboundDeliveryError> {
        let migration_arrival = match &record {
            PendingGroupDecisionRecord::GroupInvitation(_) => None,
            PendingGroupDecisionRecord::MigrationProposal(proposal) => {
                let arrival = self.classify_migration_proposal_arrival(proposal).await?;
                Some((proposal.migration_id, arrival))
            }
        };
        match migration_arrival {
            None | Some((_, MigrationProposalArrival::Candidate)) => {
                self.install_pending_group_candidate(record, group_setup, sender)
                    .await
            }
            Some((_, MigrationProposalArrival::AcceptedReplay)) => {
                self.refresh_accepted_migration_replay(record, group_setup, sender)
                    .await
            }
            Some((migration_id, MigrationProposalArrival::RejectedConflict)) => {
                warn!(
                    self.log(),
                    "rejecting migration proposal from old group {} to target {} because another direction was already accepted",
                    migration_id.old_group_id,
                    migration_id.new_group_id,
                );
                // Retrying cannot change the immutable accepted direction, so
                // successful processing lets reliable delivery retire it.
                Ok(())
            }
        }
    }

    fn handle_reliable_delivery(
        &mut self,
        deliver: ReliableDeliveryDeliver,
    ) -> Result<HandlerResult, InboundDeliveryFailure> {
        let context = InboundDeliveryContext::reliable(&deliver.envelope.header);
        let memberships = self.group_memberships.snapshot();
        let decode_context = RuntimeMessageDecodeContext::new(memberships.as_ref());
        let message_res = RuntimeMessage::decode_proto_view_from_slice_with(
            &deliver.envelope.payload.bytes,
            decode_context,
        )
        .context(inbound::DecodeMessageSnafu);
        let message = match message_res {
            Ok(message) => message,
            Err(error) => return Err(InboundDeliveryFailure::new(context, error)),
        };
        let message_group_id = message.group_id();
        let ReliableMessageScope::Group {
            group_id: envelope_group_id,
        } = deliver.envelope.header.scope
        else {
            let error = InboundDeliveryError::ReliableMessageMissingGroupScope { message_group_id };
            return Err(InboundDeliveryFailure::new(context, error));
        };
        if envelope_group_id != message_group_id {
            let error = InboundDeliveryError::ReliableMessageGroupMismatch {
                envelope_group_id,
                message_group_id,
            };
            return Err(InboundDeliveryFailure::new(context, error));
        }
        match message {
            RuntimeMessage::GroupInvitation(message) => {
                let (invitation, group_setup) = message.into_parts();
                Ok(self.handle_inbound_pending_group_decision(
                    context,
                    deliver,
                    PendingGroupDecisionRecord::GroupInvitation(invitation),
                    group_setup,
                ))
            }
            RuntimeMessage::MigrationProposal(message) => {
                let (proposal, group_setup) = message.into_parts();
                Ok(self.handle_inbound_pending_group_decision(
                    context,
                    deliver,
                    PendingGroupDecisionRecord::MigrationProposal(proposal),
                    group_setup,
                ))
            }
            RuntimeMessage::Update(_) => Err(InboundDeliveryFailure::new(
                context,
                InboundDeliveryError::UnexpectedReliableMessage,
            )),
            RuntimeMessage::NeedRange(_) | RuntimeMessage::UpdateBatch(_) => {
                Err(InboundDeliveryFailure::new(
                    context,
                    InboundDeliveryError::UnexpectedReliableMessage,
                ))
            }
            RuntimeMessage::SummaryRequest(message) => {
                let sender = deliver.envelope.header.sender.clone();
                Ok(self.handle_inbound_summary_request(
                    context,
                    SummaryReplyRoute::Reliable {
                        recipient: sender,
                        processed: deliver.processed,
                    },
                    message,
                ))
            }
            RuntimeMessage::Summary(message) => {
                let sender = deliver.envelope.header.sender.clone();
                let summary = summary_from_message(sender, message);
                Ok(self.handle_observed_summary(summary))
            }
        }
    }

    fn handle_group_delivery(
        &mut self,
        deliver: &GroupBroadcastDeliver,
    ) -> Result<HandlerResult, InboundDeliveryFailure> {
        let context = InboundDeliveryContext::group(&deliver.envelope.header);
        let sender = deliver.envelope.header.sender.clone();
        let memberships = self.group_memberships.snapshot();
        let decode_context = RuntimeMessageDecodeContext::new(memberships.as_ref());
        let message = match RuntimeMessage::decode_proto_view_from_slice_with(
            &deliver.envelope.payload.bytes,
            decode_context,
        )
        .context(inbound::DecodeMessageSnafu)
        {
            Ok(message) => message,
            Err(error) => return Err(InboundDeliveryFailure::new(context, error)),
        };
        match message {
            RuntimeMessage::GroupInvitation(_) | RuntimeMessage::MigrationProposal(_) => Err(
                InboundDeliveryFailure::new(context, InboundDeliveryError::UnexpectedGroupMessage),
            ),
            RuntimeMessage::Update(message) => Ok(self.handle_update(context, sender, *message)),
            RuntimeMessage::UpdateBatch(message) => {
                Ok(self.handle_update_batch(context, sender, message))
            }
            RuntimeMessage::NeedRange(_) => {
                // CatchUpManagerComponent owns NeedRange processing on group broadcast.
                Ok(Handled::OK)
            }
            RuntimeMessage::Summary(message) => {
                let summary = summary_from_message(sender, message);
                Ok(self.handle_observed_summary(summary))
            }
            RuntimeMessage::SummaryRequest(message) => Ok(self.handle_inbound_summary_request(
                context,
                SummaryReplyRoute::GroupBroadcast,
                message,
            )),
        }
    }

    /// Resolve the hosted schema before calling the inbound transaction service.
    async fn persist_and_apply_update(
        &mut self,
        origin: InboundUpdateOrigin,
        message: UpdateMessage,
    ) -> Result<InboundUpdateOutcome, InboundDeliveryError> {
        let group_id = message.group_id;
        let group_schema = self
            .group_memberships
            .group_schema(&group_id)
            .context(inbound::UnknownHostedGroupSnafu { group_id })?;
        inbound_support::persist_and_apply_update(
            self.store.as_ref(),
            &self.local_member,
            group_schema.as_ref(),
            origin,
            message,
        )
        .await
    }

    /// Apply a direct producer delivery and classify failures with its authenticated envelope.
    fn handle_update(
        &mut self,
        context: InboundDeliveryContext,
        sender: MemberIdentity,
        message: UpdateMessage,
    ) -> HandlerResult {
        self.handle_inbound_apply(
            context,
            InboundRequest::Single {
                origin: InboundUpdateOrigin::Producer { sender },
                message,
            },
        )
    }

    /// Keep forwarded inputs and callbacks contiguous through the final catch-up observation flush.
    fn handle_update_batch(
        &mut self,
        context: InboundDeliveryContext,
        batch_sender: MemberIdentity,
        message: UpdateBatchMessage,
    ) -> HandlerResult {
        self.handle_inbound_apply(
            context,
            InboundRequest::Batch {
                sender: batch_sender,
                message,
            },
        )
    }

    /// Acquire the same group/listener slot used by publication before starting inbound work.
    ///
    /// Context is present for authenticated delivery and absent only for direct fixtures.
    /// An occupied or faulted slot rejects admission without starting transaction work.
    fn admit_inbound(
        &mut self,
        context: Option<InboundDeliveryContext>,
        request: InboundRequest,
    ) -> Result<FlowEffect<RuntimeEffect>, FlowProtocolError> {
        self.operation_slot
            .admit(&mut self.flow_ids, request.group_id(), |ownership| {
                let (flow, effect) = InboundFlow::new(ownership, context, request);
                (
                    RuntimeFlow::Inbound(Box::new(flow)),
                    RuntimeEffect::Inbound(effect),
                )
            })
    }

    /// Retain blocking execution for the whole single/batch flow, including its final observations.
    fn handle_inbound_apply(
        &mut self,
        context: InboundDeliveryContext,
        request: InboundRequest,
    ) -> HandlerResult {
        match self.admit_inbound(Some(context), request) {
            Ok(initial) => Handled::block_on(self, async move |mut async_self| {
                let flow_id = initial.flow_id;
                let result = async_self
                    .drive_inbound(initial)
                    .await
                    .whatever_unrecoverable(
                        "inbound driver violated its ownership/phase protocol",
                    )?;
                if let Err(error) = result.outcome.into_result() {
                    let context = result
                        .context
                        .context(flow_driver::protocol::MissingStateSnafu {
                            flow_id,
                            field: "inbound delivery context",
                        })
                        .whatever_unrecoverable("production inbound flow lost its envelope")?;
                    let failure = InboundDeliveryFailure::new(context, error);
                    let action = async_self.record_inbound_failure(&failure);
                    panic_if_fatal_inbound_failure(action, &failure);
                }
                Handled::OK
            }),
            Err(error) => {
                let error = self.operation_slot.fault(error);
                Err(error).whatever_unrecoverable("inbound admission violated ownership")
            }
        }
    }

    /// Execute an inbound flow and return its application outcome and authenticated context.
    ///
    /// Returns the domain result after the shared driver restores ownership, or a protocol
    /// error if execution or operation-kind routing fails. Context is absent only for fixtures.
    async fn drive_inbound(
        &mut self,
        initial: FlowEffect<RuntimeEffect>,
    ) -> Result<InboundResult, FlowProtocolError> {
        let flow_id = initial.flow_id;
        let outcome = self.drive_flow(initial).await?;
        match outcome {
            RuntimeOutcome::Inbound(result) => Ok(result),
            RuntimeOutcome::Publication { .. } => {
                let error = flow_driver::protocol::UnexpectedCompletionSnafu {
                    flow_id,
                    expected: "Inbound",
                    received: "Publication",
                }
                .build();
                Err(self.operation_slot.fault(error))
            }
        }
    }

    /// Drive a fixture through production admission, effects and terminal release without an envelope.
    /// The outer result reports protocol faults; the inner result is the ordinary inbound outcome.
    #[cfg(test)]
    async fn apply_inbound_for_test(
        &mut self,
        request: InboundRequest,
    ) -> Result<Result<(), InboundDeliveryError>, FlowProtocolError> {
        let initial = self.admit_inbound(None, request)?;
        let result = self.drive_inbound(initial).await?;
        Ok(result.outcome.into_result())
    }

    fn handle_publish_changes(
        &mut self,
        ask: Ask<PublishChangesRequest, Result<PublishReceipt, ApiError>>,
    ) -> HandlerResult {
        let (promise, request) = ask.take();
        match self.prepare_publication(request) {
            Ok((scoped, schema)) => {
                let group_id = scoped.group_id();
                let admitted =
                    self.operation_slot
                        .admit(&mut self.flow_ids, group_id, |ownership| {
                            let (flow, effect) =
                                PublicationFlow::new(ownership, scoped, schema, promise);
                            (
                                RuntimeFlow::Publication(flow),
                                RuntimeEffect::Publication(effect),
                            )
                        });
                match admitted {
                    Ok(initial) => Handled::block_on(self, async move |mut async_self| {
                        async_self
                            .drive_flow(initial)
                            .await
                            .whatever_unrecoverable(
                                "publication driver violated its ownership/phase protocol",
                            )?;
                        Handled::OK
                    }),
                    Err(error) => {
                        let error = self.operation_slot.fault(error);
                        Err(error)
                            .whatever_unrecoverable("publication admission violated ownership")
                    }
                }
            }
            Err(error) => {
                let reply = Err(ApiError::from_store_classification_source(error));
                self.reply_api(promise, "publish_changes", reply);
                Handled::OK
            }
        }
    }

    /// Execute concrete actions while their recorded owner retains group and listener claims.
    async fn drive_flow(
        &mut self,
        initial: FlowEffect<RuntimeEffect>,
    ) -> Result<RuntimeOutcome, FlowProtocolError> {
        let flow_id = initial.flow_id;
        let mut step = FlowStep::Continue(initial);
        while let FlowStep::Continue(effect) = step {
            let completion = match effect.effect {
                RuntimeEffect::Publication(effect) => {
                    RuntimeCompletion::Publication(self.execute_publication_effect(effect).await)
                }
                RuntimeEffect::Inbound(effect) => {
                    RuntimeCompletion::Inbound(self.execute_inbound_effect(effect).await)
                }
            };
            step = self
                .operation_slot
                .transition(|flow| flow.complete(effect.flow_id, completion))?;
        }
        match step {
            FlowStep::Finished(finished) => {
                match &finished.outcome {
                    RuntimeOutcome::Publication { outcome, reply } => {
                        debug!(
                            self.log(),
                            "publication finished: {:?}, reply {:?}", outcome, reply
                        );
                    }
                    RuntimeOutcome::Inbound(_) => {
                        // The caller logs inbound failures with their authenticated envelope.
                    }
                }
                self.operation_slot.restore(finished.ownership)?;
                Ok(finished.outcome)
            }
            FlowStep::Continue(_) => {
                let error = flow_driver::protocol::MissingStateSnafu {
                    flow_id,
                    field: "terminal flow result",
                }
                .build();
                Err(self.operation_slot.fault(error))
            }
        }
    }

    /// Execute inbound IO/port effects without giving the child access to component services.
    async fn execute_inbound_effect(&mut self, effect: InboundEffect) -> InboundCompletion {
        match effect {
            InboundEffect::Apply { origin, message } => {
                let result = self.persist_and_apply_update(origin, message).await;
                InboundCompletion::Applied(result)
            }
            InboundEffect::Advertise {
                group_id,
                observations,
            } => {
                self.notify_catch_up_available(group_id, observations.available);
                self.notify_catch_up_needed(group_id, observations.needed);
                InboundCompletion::Advertised
            }
            InboundEffect::NotifyListener(event) => {
                let result = self.listener.on_event(event.into_event()).await;
                InboundCompletion::ListenerNotified(result)
            }
        }
    }

    /// Execute one action using component services; the flow receives only the result.
    async fn execute_publication_effect(
        &mut self,
        effect: PublicationEffect,
    ) -> PublicationCompletion {
        match effect {
            PublicationEffect::Write { scoped, schema } => {
                let result = publication::publish_changes_transactionally(
                    self.store.as_ref(),
                    &self.local_member,
                    schema.as_ref(),
                    scoped,
                )
                .await;
                PublicationCompletion::Written(result)
            }
            PublicationEffect::Broadcast(committed) => {
                self.group_broadcast.trigger(
                    GroupBroadcastPortRequest::build_submit(DeliveryClass::BestEffort)
                        .for_member_in_group(self.local_member.clone(), committed.group_id())
                        .with_payload(committed.payload().clone()),
                );
                PublicationCompletion::Broadcasted(committed)
            }
            PublicationEffect::Advertise(committed) => {
                self.notify_catch_up_available(
                    committed.group_id(),
                    vec![UpdateRangeMessage::from(committed.update_id())],
                );
                PublicationCompletion::Advertised(committed)
            }
            PublicationEffect::NotifyListener(listener_data) => {
                let result =
                    notify_listener_batches(self.listener.clone(), smallvec![listener_data]).await;
                PublicationCompletion::ListenerNotified(result)
            }
            PublicationEffect::Reply { promise, reply } => {
                let outcome = self.reply_api(promise, "publish_changes", reply);
                PublicationCompletion::Replied(outcome)
            }
        }
    }

    /// Persist and install a prepared local group, notify the application, and invite peers.
    async fn complete_group_creation(
        &mut self,
        group_id: GroupId,
        record: ReplicationGroupRecord,
        group_setup: &GroupSetupMessage,
        invitation_payload: &bytes::Bytes,
    ) -> Result<GroupId, ApiError> {
        let persisted_group = self
            .store_new_replication_group(record)
            .await
            .map_err(ApiError::from_store_classification_source)?;
        let read_token =
            GroupReadToken::from_group_version(group_id, persisted_group.version_vector.clone());
        notify_listener_data_change(
            self.listener.clone(),
            ListenerDataChanges {
                read_token,
                row_changes: Vec::new(),
            },
        )
        .await
        .boxed()
        .context(ApiExternalSnafu)?;
        self.submit_group_creation_invitation_messages(group_id, group_setup, invitation_payload);
        Ok(group_id)
    }

    fn handle_create_group(
        &mut self,
        ask: Ask<CreateGroupRequest, Result<GroupId, ApiError>>,
    ) -> HandlerResult {
        let (promise, req) = ask.take();
        let created_group = self.prepare_created_group(req);
        let created_group = match created_group {
            Ok(created_group) => created_group,
            Err(error) => {
                let reply = Err(ApiError::from_store_classification_source(error));
                self.reply_api(promise, "create_group", reply);
                return Handled::OK;
            }
        };
        let PreparedCreatedGroup {
            group_id,
            group_name,
            message,
            members,
            group_schema,
        } = created_group;
        Handled::block_on(self, async move |mut async_self| {
            let prepared_setup = Self::prepare_group_setup(
                &async_self.security,
                async_self.max_inline_bootstrap_public_key_bundles,
                group_id,
                &members,
            )
            .await;
            let prepared_setup = match prepared_setup {
                Ok(prepared_setup) => prepared_setup,
                Err(error) => {
                    let reply = Err(ApiError::from_store_classification_source(error));
                    async_self.reply_api(promise, "create_group", reply);
                    return Handled::OK;
                }
            };
            let member_keys = match GroupMemberKeys::from_ordered_member_keys(
                prepared_setup.group_setup.ordered_member_key_ids(),
            ) {
                Ok(member_keys) => member_keys,
                Err(source) => {
                    let error = CreateGroupError::InvalidMembers { source };
                    let reply = Err(ApiError::from_store_classification_source(error));
                    async_self.reply_api(promise, "create_group", reply);
                    return Handled::OK;
                }
            };
            let invitation = GroupInvitation::new_creation(
                group_id,
                prepared_setup.group_setup.members().to_vec(),
                group_schema.clone(),
                InitialSnapshot::Empty,
                group_name.clone(),
                message.clone(),
            );
            let invitation_message = GroupInvitationMessage::try_new(
                invitation,
                Arc::clone(&prepared_setup.group_setup),
            )
            .expect("created-group invitation members match prepared group setup");
            let invitation_payload =
                RuntimeMessage::GroupInvitation(invitation_message).encode_proto_to_bytes();
            let record = async_self.build_replication_group_record(
                group_id,
                group_name,
                member_keys,
                group_schema,
                prepared_setup.security_material,
            );
            let reply = async_self
                .complete_group_creation(
                    group_id,
                    record,
                    &prepared_setup.group_setup,
                    &invitation_payload,
                )
                .await;
            async_self.reply_api(promise, "create_group", reply);
            Handled::OK
        })
    }

    fn handle_change_group_membership(
        &mut self,
        ask: Ask<ChangeGroupMembershipRequest, Result<MigrationId, ApiError>>,
    ) -> HandlerResult {
        let (promise, req) = ask.take();
        Handled::block_on(self, async move |mut async_self| {
            let reply = match async_self.prepare_membership_migration(req).await {
                Ok(prepared) => {
                    let migration_id = prepared.migration_id;
                    let dispatch = Self::prepare_membership_dispatch(&prepared);
                    let staging_result = async_self
                        .stage_prepared_membership_migration(&prepared)
                        .await;
                    match staging_result {
                        Ok(activation) => {
                            let activation_result =
                                async_self.activate_pending_group_record(activation).await;
                            match activation_result {
                                Ok(outcome) => {
                                    async_self.submit_membership_migration_messages(&dispatch);
                                    let notification_result =
                                        notify_pending_activation_data_changes(
                                            async_self.listener.clone(),
                                            outcome,
                                        )
                                        .await;
                                    match notification_result {
                                        Ok(()) => Ok(migration_id),
                                        Err(error) => {
                                            let error =
                                                ChangeGroupMembershipError::NotifyListener {
                                                    source: error,
                                                };
                                            Err(ApiError::from_store_classification_source(error))
                                        }
                                    }
                                }
                                Err(error) if error.is_unrecoverable() => {
                                    return Err(error).whatever_unrecoverable(
                                        "membership activation failed after commit",
                                    );
                                }
                                Err(error) => {
                                    let error =
                                        change_membership::ActivateGroupSnafu.into_error(error);
                                    Err(ApiError::from_store_classification_source(error))
                                }
                            }
                        }
                        Err(error) => Err(ApiError::from_store_classification_source(error)),
                    }
                }
                Err(error) => Err(ApiError::from_store_classification_source(error)),
            };
            async_self.reply_api(promise, "change_group_membership", reply);
            Handled::OK
        })
    }

    fn handle_pending_group_decision_response(
        &mut self,
        ask: Ask<PendingGroupDecisionResponse, Result<(), ApiError>>,
    ) -> HandlerResult {
        let (promise, response) = ask.take();
        Handled::block_on(self, async move |mut async_self| {
            let reply = match async_self
                .apply_pending_group_decision_response(response)
                .await
            {
                Ok(Some(activation)) => {
                    match async_self.activate_pending_group_record(*activation).await {
                        Ok(outcome) => {
                            let res = notify_pending_activation_data_changes(
                                async_self.listener.clone(),
                                outcome,
                            )
                            .await;
                            res.boxed().context(ApiExternalSnafu)
                        }
                        Err(error) if error.is_unrecoverable() => {
                            return Err(error).whatever_unrecoverable(
                                "pending group activation failed after commit",
                            );
                        }
                        Err(error) => Err(ApiError::from_store_classification_source(error)),
                    }
                }
                Ok(None) => Ok(()),
                Err(error) => Err(error),
            };
            async_self.reply_api(promise, "pending_group_decision_response", reply);
            Handled::OK
        })
    }
}

impl ComponentLifecycle for ReplicationRuntimeComponent {
    fn on_start(&mut self) -> HandlerResult {
        self.max_inline_bootstrap_public_key_bundles =
            self.read_max_inline_bootstrap_public_key_bundles();
        Handled::block_on(self, async move |mut async_self| {
            let runtime_ref = async_self
                .ctx
                .actor_ref()
                .hold()
                .whatever_unrecoverable("replication runtime actor ref was unavailable")?;
            async_self
                .replay_pending_group_decisions(runtime_ref)
                .await
                .whatever_unrecoverable("replication runtime startup failed")?;
            async_self
                .resume_pending_group_activations()
                .await
                .whatever_unrecoverable("replication runtime startup failed")?;
            Handled::OK
        })
    }

    fn on_stop(&mut self) -> HandlerResult {
        Handled::OK
    }

    fn on_kill(&mut self) -> HandlerResult {
        Handled::OK
    }
}

impl Require<ReliableDeliveryPort> for ReplicationRuntimeComponent {
    fn handle(&mut self, indication: ReliableDeliveryPortIndication) -> HandlerResult {
        let ReliableDeliveryPortIndication::Deliver(deliver) = indication;
        match self.handle_reliable_delivery(deliver) {
            Ok(handled) => handled,
            Err(failure) => {
                let action = self.record_inbound_failure(&failure);
                handled_after_inbound_failure(action, &failure)
            }
        }
    }
}

impl Require<GroupBroadcastPort> for ReplicationRuntimeComponent {
    fn handle(&mut self, indication: GroupBroadcastPortIndication) -> HandlerResult {
        let GroupBroadcastPortIndication::Deliver(deliver) = indication;
        match self.handle_group_delivery(&deliver) {
            Ok(handled) => handled,
            Err(failure) => {
                let action = self.record_inbound_failure(&failure);
                handled_after_inbound_failure(action, &failure)
            }
        }
    }
}

impl Actor for ReplicationRuntimeComponent {
    type Message = ReplicationRuntimeMessage;

    fn receive_local(&mut self, msg: Self::Message) -> HandlerResult {
        match msg {
            ReplicationRuntimeMessage::LocalPublicKeyBundle(ask) => {
                self.handle_local_public_key_bundle(ask)
            }
            ReplicationRuntimeMessage::KnownMemberKeys(ask) => self.handle_known_member_keys(ask),
            ReplicationRuntimeMessage::AssessPublicKeyBundle(ask) => {
                self.handle_assess_public_key_bundle(ask)
            }
            ReplicationRuntimeMessage::RecordPublicKeyBundleFeedback(ask) => {
                self.handle_record_public_key_bundle_feedback(ask)
            }
            ReplicationRuntimeMessage::PublishChanges(ask) => self.handle_publish_changes(ask),
            ReplicationRuntimeMessage::RequestSummary(ask) => self.handle_request_summary(ask),
            ReplicationRuntimeMessage::CreateGroup(ask) => self.handle_create_group(ask),
            ReplicationRuntimeMessage::ChangeGroupMembership(ask) => {
                self.handle_change_group_membership(ask)
            }
            ReplicationRuntimeMessage::PendingGroupDecisionResponse(ask) => {
                self.handle_pending_group_decision_response(ask)
            }
            #[cfg(any(test, feature = "test-support"))]
            ReplicationRuntimeMessage::Test(message) => self.handle_test_message(message),
        }
    }
}

/// Identity and membership views shared by runtime logic components.
#[derive(Clone)]
pub(super) struct RuntimeIdentityContext {
    pub(super) local_member: MemberIdentity,
    pub(super) group_memberships: Arc<SharedGroupState>,
}

/// Application-facing services consumed by the replication runtime component.
#[derive(Clone)]
pub(super) struct RuntimeApplicationServices {
    pub(super) store: Arc<dyn ReplicationStore>,
    pub(super) listener: Arc<dyn ReplicationEventListener>,
    pub(super) config: ReplicationConfig,
}

/// Security services used for delivery and bootstrap material handling.
#[derive(Clone)]
pub(super) struct RuntimeSecurityContext {
    pub(super) security: DeliverySecurity,
}

/// Actor dependencies created by the runtime logic topology.
pub(super) struct RuntimeComponentActors {
    pub(super) summary_request_manager: ActorRefStrong<SummaryRequestManagerMessage>,
    pub(super) catch_up_manager: ActorRefStrong<CatchUpManagerMessage>,
}

/// Pure validation and classification tests for component-level inbound and group policies.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::ListenerError;
    use flotsync_core::versions::PureVersionVector;
    use std::assert_matches;

    /// Build one valid member set for pure policy-derivation tests.
    fn policy_test_members(names: &[&str]) -> GroupMembers {
        GroupMembers::from_ordered_members(
            names
                .iter()
                .map(|name| MemberIdentity::from_array(["policy", *name])),
        )
        .expect("policy test members should be valid")
    }

    #[test]
    fn migration_policy_classifies_top_level_membership_differences() {
        let policy = GroupMigrationPolicy {
            epoch_change: PolicyDecision::AutoAccept,
            member_added: PolicyDecision::AskListener,
            member_removed: PolicyDecision::AutoReject,
            ..GroupMigrationPolicy::default()
        };
        let alice_bob = policy_test_members(&["alice", "bob"]);
        let alice_bob_carol = policy_test_members(&["alice", "bob", "carol"]);
        let alice_carol = policy_test_members(&["alice", "carol"]);

        assert_eq!(
            ReplicationRuntimeComponent::migration_policy_decision_for_members(
                &policy, &alice_bob, &alice_bob,
            ),
            PolicyDecision::AutoAccept
        );
        assert_eq!(
            ReplicationRuntimeComponent::migration_policy_decision_for_members(
                &policy,
                &alice_bob,
                &alice_bob_carol,
            ),
            PolicyDecision::AskListener
        );
        assert_eq!(
            ReplicationRuntimeComponent::migration_policy_decision_for_members(
                &policy,
                &alice_bob_carol,
                &alice_bob,
            ),
            PolicyDecision::AutoReject
        );
        assert_eq!(
            ReplicationRuntimeComponent::migration_policy_decision_for_members(
                &policy,
                &alice_bob,
                &alice_carol,
            ),
            PolicyDecision::AutoReject
        );
    }

    #[test]
    fn proposal_lifecycle_classification_accepts_only_exact_direction_replays() {
        let old_group_id = GroupId(Uuid::from_u128(90_001));
        let new_group_id = GroupId(Uuid::from_u128(90_002));
        let final_versions = VersionVector::Full(PureVersionVector::from([3, 1]));
        let proposal = MigrationProposal {
            migration_id: MigrationId {
                old_group_id,
                new_group_id,
            },
            final_versions: final_versions.clone(),
            proposed_members: Vec::new(),
            group_schema: GroupSchema::default(),
            initial_snapshot: InitialSnapshot::Empty,
            group_name: None,
            message: None,
        };

        assert_eq!(
            ReplicationRuntimeComponent::classify_migration_proposal_lifecycle(
                &ReplicationGroupLifecycle::Open,
                &proposal,
            ),
            MigrationProposalArrival::Candidate
        );
        for lifecycle in [
            ReplicationGroupLifecycle::ReadOnly {
                successor_group_id: new_group_id,
                final_versions: final_versions.clone(),
            },
            ReplicationGroupLifecycle::Closed {
                successor_group_id: new_group_id,
                final_versions: final_versions.clone(),
            },
        ] {
            assert_eq!(
                ReplicationRuntimeComponent::classify_migration_proposal_lifecycle(
                    &lifecycle, &proposal,
                ),
                MigrationProposalArrival::AcceptedReplay
            );
        }

        let conflicting_target = ReplicationGroupLifecycle::ReadOnly {
            successor_group_id: GroupId(Uuid::from_u128(90_003)),
            final_versions: final_versions.clone(),
        };
        assert_eq!(
            ReplicationRuntimeComponent::classify_migration_proposal_lifecycle(
                &conflicting_target,
                &proposal,
            ),
            MigrationProposalArrival::RejectedConflict,
        );
        let conflicting_cut = ReplicationGroupLifecycle::Closed {
            successor_group_id: new_group_id,
            final_versions: VersionVector::Full(PureVersionVector::from([2, 1])),
        };
        assert_eq!(
            ReplicationRuntimeComponent::classify_migration_proposal_lifecycle(
                &conflicting_cut,
                &proposal,
            ),
            MigrationProposalArrival::RejectedConflict,
        );
    }

    #[test]
    fn inbound_error_classification_matches_manual_slice_policy() {
        let group_id = GroupId(Uuid::from_u128(91));
        let update_id = UpdateId {
            version: 1,
            node_index: 0,
        };

        assert_eq!(
            InboundDeliveryError::UnknownHostedGroup { group_id }.failure_action(),
            InboundFailureAction::Drop
        );
        assert_eq!(
            InboundDeliveryError::UnexpectedGroupMessage.failure_action(),
            InboundFailureAction::Drop
        );
        assert_eq!(
            InboundDeliveryError::GroupSetupSenderNotInGroup {
                group_id,
                sender: MemberIdentity::from_array(["runtime", "sender"]),
            }
            .failure_action(),
            InboundFailureAction::Drop
        );
        assert_eq!(
            InboundDeliveryError::GroupSetupSenderNotFirstMember {
                group_id,
                sender: MemberIdentity::from_array(["runtime", "sender"]),
                sender_index: Some(MemberIndex::new(1)),
            }
            .failure_action(),
            InboundFailureAction::Drop
        );
        assert_eq!(
            InboundDeliveryError::ConflictingPersistedUpdate {
                group: group_id,
                update: update_id,
            }
            .failure_action(),
            InboundFailureAction::Drop
        );
        assert_eq!(
            InboundDeliveryError::NotifyListener {
                source: ListenerError::Rejected {
                    message: "listener closed permanently".to_owned(),
                },
            }
            .failure_action(),
            InboundFailureAction::Fatal
        );
    }

    #[test]
    fn group_setup_membership_validation_rejects_sender_outside_group() {
        let group_id = GroupId(Uuid::from_u128(92));
        let local_member = MemberIdentity::from_array(["runtime", "local"]);
        let sender = MemberIdentity::from_array(["runtime", "sender"]);
        let members = GroupMembers::from_ordered_members(vec![local_member.clone()])
            .expect("member set should build");

        let error = ReplicationRuntimeComponent::validate_group_setup_membership(
            group_id,
            &members,
            &local_member,
            &sender,
        )
        .expect_err("sender outside the group should be rejected");

        match error {
            InboundDeliveryError::GroupSetupSenderNotInGroup {
                group_id: rejected_group_id,
                sender: rejected_sender,
            } => {
                assert_eq!(rejected_group_id, group_id);
                assert_eq!(rejected_sender, sender);
            }
            other => panic!("unexpected bootstrap membership error: {other:?}"),
        }
    }

    #[test]
    fn creator_first_member_order_places_creator_at_zero() {
        let creator = MemberIdentity::from_array(["runtime", "creator"]);
        let bob = MemberIdentity::from_array(["runtime", "bob"]);
        let carol = MemberIdentity::from_array(["runtime", "carol"]);

        let ordered =
            creator_first_member_order(vec![bob.clone(), carol.clone(), creator.clone()], &creator)
                .expect("creator in member list should be reordered");

        assert_eq!(ordered, vec![creator, carol, bob]);
    }

    #[test]
    fn creator_first_member_order_rejects_absent_creator() {
        let creator = MemberIdentity::from_array(["runtime", "creator"]);
        let bob = MemberIdentity::from_array(["runtime", "bob"]);

        let error = creator_first_member_order(vec![bob], &creator)
            .expect_err("creator must be part of the requested member list");

        assert_matches!(
            error,
            CreateGroupError::CreatorNotInMembers {
                creator: rejected_creator,
            } if rejected_creator == creator
        );
    }

    #[test]
    fn group_setup_membership_validation_rejects_sender_not_first_member() {
        let group_id = GroupId(Uuid::from_u128(93));
        let local_member = MemberIdentity::from_array(["runtime", "local"]);
        let sender = MemberIdentity::from_array(["runtime", "sender"]);
        let members =
            GroupMembers::from_ordered_members(vec![local_member.clone(), sender.clone()])
                .expect("member set should build");

        let error = ReplicationRuntimeComponent::validate_group_setup_membership(
            group_id,
            &members,
            &local_member,
            &sender,
        )
        .expect_err("sender at non-zero member index should be rejected");

        match error {
            InboundDeliveryError::GroupSetupSenderNotFirstMember {
                group_id: rejected_group_id,
                sender: rejected_sender,
                sender_index,
            } => {
                assert_eq!(rejected_group_id, group_id);
                assert_eq!(rejected_sender, sender);
                assert_eq!(sender_index, Some(MemberIndex::new(1)));
            }
            other => panic!("unexpected bootstrap membership error: {other:?}"),
        }
    }
}
