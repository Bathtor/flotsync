//! Persists inbound updates, applies them in causal order, and prepares delivery outcomes.

use super::{
    listeners::{ListenerDataChangeBatches, ListenerDataChanges},
    row_patches::apply_dataset_row_patches,
    update_ranges::{bound_update_ranges, missing_ranges_for_update_frontier},
};
use crate::{
    api::{
        DatasetUpdateRecord,
        GroupReadToken,
        GroupSchema,
        ReplicationGroupLifecycle,
        ReplicationStore,
        ReplicationStoreTransaction,
        ReplicationUpdateFilter,
        ReplicationUpdateRecord,
        Summary,
    },
    codecs::messages::{SummaryMessage, UpdateMessage, UpdateRangeMessage},
    delivery::{
        group_broadcast::GroupMessageHeader,
        reliable_delivery::ReliableMessageHeader,
        shared::MessageId,
    },
    runtime::{
        errors::{InboundDeliveryError, InboundFailureAction, inbound},
        in_memory::{
            LoadedGroupMeta,
            PendingUpdateSet,
            UpdateDependency,
            apply_one_update,
            load_ready_row_scope,
            load_scheduled_pending_update,
            load_update_dependencies,
            validate_inbound_update_read_versions,
            validate_update_mapping,
        },
        replay,
    },
};
use flotsync_core::{
    GroupId,
    MemberIdentity,
    MemberIndex,
    versions::{UpdateId, VersionVector},
};
use kompact::prelude::{Handled, HandlerResult};
use snafu::prelude::*;
use std::collections::HashSet;

/// Persist an inbound update and apply updates in causal order within one transaction.
///
/// The schema must belong to the message's group. Success returns committed listener
/// data and catch-up observations. Inputs beyond the final cut produce no observations;
/// applied duplicates advertise availability, and blocked inputs remain stored pending.
pub(super) async fn persist_and_apply_update(
    store: &dyn ReplicationStore,
    local_member: &MemberIdentity,
    group_schema: &GroupSchema,
    origin: InboundUpdateOrigin,
    message: UpdateMessage,
) -> Result<InboundUpdateOutcome, InboundDeliveryError> {
    let group_id = message.group_id;
    let mut transaction = store
        .begin_transaction()
        .await
        .context(inbound::StoreAccessSnafu)?;
    let (mut local_group, lifecycle) =
        load_inbound_group(transaction.as_mut(), local_member, group_id).await?;
    let producer =
        validated_inbound_update_producer(&local_group, group_id, origin, message.update_id)?;
    let exceeds_final_versions = lifecycle.final_versions().is_some_and(|final_versions| {
        message.update_id.version > final_versions.version_at(message.update_id.node_index as usize)
    });
    if exceeds_final_versions {
        transaction
            .commit()
            .await
            .context(inbound::StoreAccessSnafu)?;
        return Ok(InboundUpdateOutcome {
            event_batches: ListenerDataChangeBatches::new(),
            needed_ranges: Vec::new(),
            observed_available: Vec::new(),
        });
    }
    let incoming_available_range = UpdateRangeMessage::from(message.update_id);
    let needed_ranges = missing_ranges_for_update_frontier(
        &local_group.version_vector,
        group_id,
        message.update_id,
        &message.read_versions,
    )?;
    let mut needed_ranges = bound_update_ranges(&lifecycle, needed_ranges);
    let inbound_update = inbound_record_for_message(producer, message);
    validate_inbound_update_read_versions(&inbound_update)?;
    if local_group.has_applied(inbound_update.update_id) {
        transaction
            .commit()
            .await
            .context(inbound::StoreAccessSnafu)?;
        return Ok(InboundUpdateOutcome {
            event_batches: ListenerDataChangeBatches::new(),
            needed_ranges,
            observed_available: vec![incoming_available_range],
        });
    }
    persist_pending_update(transaction.as_mut(), group_schema, &inbound_update).await?;
    let mut observed_available = vec![incoming_available_range];
    let drain_outcome = drain_pending_updates(
        transaction.as_mut(),
        group_schema,
        group_id,
        &lifecycle,
        &mut local_group,
        &mut needed_ranges,
        &mut observed_available,
    )
    .await?;
    let event_batches = match drain_outcome {
        PendingDrainOutcome::NoReadyUpdates => ListenerDataChangeBatches::new(),
        PendingDrainOutcome::AppliedReadyUpdates { event_batches } => {
            let applied_versions = local_group.version_vector;
            transaction
                .update_replication_group_version_vector(&group_id, applied_versions)
                .await
                .context(inbound::StoreAccessSnafu)?;
            event_batches
        }
    };
    transaction
        .commit()
        .await
        .context(inbound::StoreAccessSnafu)?;
    Ok(InboundUpdateOutcome {
        event_batches,
        needed_ranges,
        observed_available,
    })
}

/// Convert a decoded summary message into the runtime summary representation.
pub(super) fn summary_from_message(sender: MemberIdentity, message: SummaryMessage) -> Summary {
    Summary {
        group_id: message.group_id,
        responder: sender,
        has_versions: message.has_versions,
    }
}

/// Finish inbound handling after escalating fatal failures.
pub(super) fn handled_after_inbound_failure(
    action: InboundFailureAction,
    failure: &InboundDeliveryFailure,
) -> HandlerResult {
    panic_if_fatal_inbound_failure(action, failure);
    Handled::OK
}

/// Escalate inbound delivery failures that the runtime classifies as fatal.
pub(super) fn panic_if_fatal_inbound_failure(
    action: InboundFailureAction,
    failure: &InboundDeliveryFailure,
) {
    if matches!(action, InboundFailureAction::Fatal) {
        panic!(
            "fatal inbound {} failure: {}",
            failure.context, failure.error
        );
    }
}

/// Source-specific sender validation for an inbound update.
pub(super) enum InboundUpdateOrigin {
    /// Direct `Update` delivery where the envelope sender must be the producer.
    Producer { sender: MemberIdentity },
    /// Catch-up delivery where the envelope sender forwards another producer's update.
    Forwarder { sender: MemberIdentity },
}

/// Committed result of processing one inbound input and pending updates in causal order.
///
/// A pending input can be present in the update log while still awaiting dependencies.
/// Applied duplicates retain availability observations; inputs beyond the final cut
/// produce empty observations. Every returned result follows a successful commit.
pub(super) struct InboundUpdateOutcome {
    /// Listener batches produced by updates that became newly applicable.
    pub(super) event_batches: ListenerDataChangeBatches,
    /// Producer-version ranges still needed before blocked updates can apply.
    pub(super) needed_ranges: Vec<UpdateRangeMessage>,
    /// Producer versions now known to be present in the local update log.
    pub(super) observed_available: Vec<UpdateRangeMessage>,
}

/// Catch-up notifications derived from one observed summary message.
pub(super) struct SummaryCatchUpObservation {
    /// Group whose local progress was compared with the observed summary.
    pub(super) group_id: GroupId,
    /// Local pending update-log versions that can be advertised as available.
    pub(super) observed_available: Vec<UpdateRangeMessage>,
    /// Versions the summary says exist elsewhere and are not already pending locally.
    pub(super) needed_ranges: Vec<UpdateRangeMessage>,
}

/// Envelope metadata included in inbound delivery fault logs.
#[derive(Clone, Debug)]
pub(super) enum InboundDeliveryContext {
    /// Recipient-addressed envelope handed over by reliable delivery.
    Reliable {
        /// Authenticated member that sent the envelope.
        sender: MemberIdentity,
        /// Member addressed by the envelope.
        recipient: MemberIdentity,
        /// Delivery identity used to correlate the failure with its envelope.
        message_id: MessageId,
    },
    /// Group-scoped envelope handed over by group broadcast.
    Group {
        /// Group addressed by the envelope.
        group_id: GroupId,
        /// Authenticated member that sent the envelope.
        sender: MemberIdentity,
        /// Delivery identity used to correlate the failure with its envelope.
        message_id: MessageId,
    },
}

impl InboundDeliveryContext {
    /// Capture the authenticated group-envelope metadata for failure reporting.
    pub(super) fn group(header: &GroupMessageHeader) -> Self {
        Self::Group {
            group_id: header.group_id,
            sender: header.sender.clone(),
            message_id: header.message_id,
        }
    }

    /// Capture the authenticated recipient-envelope metadata for failure reporting.
    pub(super) fn reliable(header: &ReliableMessageHeader) -> Self {
        Self::Reliable {
            sender: header.sender.clone(),
            recipient: header.recipient.clone(),
            message_id: header.message_id,
        }
    }
}

impl std::fmt::Display for InboundDeliveryContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Reliable {
                sender,
                recipient,
                message_id,
            } => write!(
                f,
                "reliable delivery message {message_id} from {sender} to {recipient}"
            ),
            Self::Group {
                group_id,
                sender,
                message_id,
            } => write!(
                f,
                "group broadcast message {message_id} for group {group_id} from {sender}"
            ),
        }
    }
}

/// Inbound delivery failure paired with the envelope that caused it.
pub(super) struct InboundDeliveryFailure {
    /// Envelope identity retained for contextual diagnostics.
    pub(super) context: InboundDeliveryContext,
    /// Classified processing failure paired with that envelope.
    pub(super) error: Box<InboundDeliveryError>,
}

impl InboundDeliveryFailure {
    /// Pair a processing failure with the envelope that caused it.
    pub(super) fn new(context: InboundDeliveryContext, error: InboundDeliveryError) -> Self {
        Self {
            context,
            error: Box::new(error),
        }
    }
}

/// Validate the envelope sender for one inbound update and return the producer identity.
///
/// Direct producer sends must come from the canonical producer index.
/// Forwarded catch-up updates only require the envelope sender to be a
/// group member; the persisted sender remains the canonical producer.
fn validated_inbound_update_producer(
    local_group: &LoadedGroupMeta,
    group_id: GroupId,
    origin: InboundUpdateOrigin,
    update_id: UpdateId,
) -> Result<MemberIdentity, InboundDeliveryError> {
    let producer_index = MemberIndex::new(update_id.node_index);
    let producer = local_group
        .members
        .member_at_index(producer_index)
        .context(inbound::UpdateProducerIndexNotInGroupSnafu {
            group_id,
            producer_index,
        })?;
    match origin {
        InboundUpdateOrigin::Producer { sender } => {
            let expected_sender_index =
                local_group.members.member_index(&sender).with_context(|| {
                    inbound::UpdateSenderNotInGroupSnafu {
                        group_id,
                        sender: sender.clone(),
                    }
                })?;
            ensure!(
                expected_sender_index == producer_index,
                inbound::UpdateSenderIndexMismatchSnafu {
                    group_id,
                    sender: sender.clone(),
                    expected_index: expected_sender_index,
                    actual_index: producer_index,
                }
            );
            Ok(sender)
        }
        InboundUpdateOrigin::Forwarder { sender } => {
            local_group
                .members
                .member_index(&sender)
                .context(inbound::UpdateSenderNotInGroupSnafu { group_id, sender })?;
            Ok(producer)
        }
    }
}

/// Load the transaction's hosted group and validate its metadata for the local member.
///
/// Absence reports `UnknownHostedGroup`; inconsistent metadata reports
/// `InvalidPersistedGroup`. Store access retains its classified source.
/// The lifecycle accompanies the metadata to govern application and observation bounds.
async fn load_inbound_group(
    transaction: &mut dyn ReplicationStoreTransaction,
    local_member: &MemberIdentity,
    group_id: GroupId,
) -> Result<(LoadedGroupMeta, ReplicationGroupLifecycle), InboundDeliveryError> {
    let persisted_group = transaction
        .load_replication_group(&group_id)
        .await
        .context(inbound::StoreAccessSnafu)?;
    let persisted_group = persisted_group.context(inbound::UnknownHostedGroupSnafu { group_id })?;
    let lifecycle = persisted_group.lifecycle.clone();
    let local_group = LoadedGroupMeta::from_replication_group_record(local_member, persisted_group)
        .context(inbound::InvalidPersistedGroupSnafu { group_id })?;
    Ok((local_group, lifecycle))
}

/// Convert a validated producer's message to a record awaiting local application.
fn inbound_record_for_message(
    sender: MemberIdentity,
    message: UpdateMessage,
) -> ReplicationUpdateRecord {
    ReplicationUpdateRecord {
        group_id: message.group_id,
        update_id: message.update_id,
        sender,
        read_versions: message.read_versions,
        dataset_updates: message
            .dataset_updates
            .into_iter()
            .map(DatasetUpdateRecord::from)
            .collect(),
        applied_locally: false,
    }
}

/// Validate and append a new pending record, or require an existing record to match exactly.
///
/// The caller has validated sender identity and the read frontier and excluded applied
/// duplicates. Existing pending records bypass new-record schema/mapping validation;
/// a differing payload reports `ConflictingPersistedUpdate`. New records require
/// schemas for all touched datasets and valid operation mappings before append.
async fn persist_pending_update(
    transaction: &mut dyn ReplicationStoreTransaction,
    group_schema: &GroupSchema,
    inbound_update: &ReplicationUpdateRecord,
) -> Result<(), InboundDeliveryError> {
    let group_id = inbound_update.group_id;
    let existing_update = transaction
        .load_replication_update(&group_id, inbound_update.update_id)
        .await
        .context(inbound::StoreAccessSnafu)?;
    if let Some(existing_update) = existing_update {
        ensure!(
            existing_update == *inbound_update,
            inbound::ConflictingPersistedUpdateSnafu {
                group: group_id,
                update: inbound_update.update_id,
            }
        );
    } else {
        let touched_dataset_ids = inbound_update
            .dataset_updates
            .iter()
            .map(|dataset_update| dataset_update.dataset_id.clone())
            .collect::<HashSet<_>>();
        for dataset_id in touched_dataset_ids {
            ensure!(
                group_schema.schema(&dataset_id).is_some(),
                inbound::MissingDatasetSchemaSnafu {
                    group_id,
                    dataset_id
                }
            );
        }
        validate_update_mapping(inbound_update, group_schema)?;
        transaction
            .append_replication_update(inbound_update.clone())
            .await
            .context(inbound::StoreAccessSnafu)?;
    }
    Ok(())
}

/// Plan and apply pending updates while accumulating transaction-local catch-up evidence.
///
/// The caller supplies the incoming update's needs/availability; this appends pending
/// availability and bounded dependency gaps. `NoReadyUpdates` includes stale pending
/// records marked applied; `AppliedReadyUpdates` advances the private group metadata
/// and owns prepared callbacks. The caller commits and publishes the returned effects.
async fn drain_pending_updates(
    transaction: &mut dyn ReplicationStoreTransaction,
    group_schema: &GroupSchema,
    group_id: GroupId,
    lifecycle: &ReplicationGroupLifecycle,
    local_group: &mut LoadedGroupMeta,
    needed_ranges: &mut Vec<UpdateRangeMessage>,
    observed_available: &mut Vec<UpdateRangeMessage>,
) -> Result<PendingDrainOutcome, InboundDeliveryError> {
    let pending_updates =
        load_update_dependencies(transaction, group_id, ReplicationUpdateFilter::PendingApply)
            .await
            .context(inbound::StoreAccessSnafu)?;
    observed_available.extend(
        pending_updates
            .iter()
            .map(|update| UpdateRangeMessage::from(update.update_id)),
    );
    let mut pending_updates = PendingUpdateSet::from_updates(pending_updates);
    let apply_plan = pending_updates.plan_ready_updates(local_group);
    for applied_update_id in &apply_plan.already_applied {
        transaction
            .mark_replication_update_applied(&group_id, *applied_update_id)
            .await
            .context(inbound::StoreAccessSnafu)?;
    }
    let outcome = if apply_plan.ready_updates.is_empty() {
        PendingDrainOutcome::NoReadyUpdates
    } else {
        let event_batches = apply_ready_updates(
            transaction,
            group_schema,
            group_id,
            lifecycle,
            local_group,
            &apply_plan.ready_updates,
        )
        .await?;
        PendingDrainOutcome::AppliedReadyUpdates { event_batches }
    };
    extend_blocked_update_ranges(
        needed_ranges,
        &local_group.version_vector,
        group_id,
        lifecycle,
        &apply_plan.blocked_updates,
    )?;
    Ok(outcome)
}

/// Apply the planner's updates in order using one shared working set of touched rows.
///
/// Each update must become ready after its predecessors in this order. The caller's
/// transaction owns every row write and applied marker. Listener batches follow that
/// same order, carrying each resulting vector; closed lifecycles and empty visible
/// changes produce no batch. Returned batches await the caller's successful commit.
async fn apply_ready_updates(
    transaction: &mut dyn ReplicationStoreTransaction,
    group_schema: &GroupSchema,
    group_id: GroupId,
    lifecycle: &ReplicationGroupLifecycle,
    local_group: &mut LoadedGroupMeta,
    ready_updates: &[UpdateDependency],
) -> Result<ListenerDataChangeBatches, InboundDeliveryError> {
    let ready_ids = ready_updates
        .iter()
        .map(|update| update.update_id)
        .collect::<HashSet<_>>();
    let touched_dataset_rows =
        load_ready_row_scope(transaction, group_id, group_schema, &ready_ids).await?;
    let touched_dataset_slices = replay::load_touched_dataset_slices(
        transaction,
        group_schema,
        group_id,
        &touched_dataset_rows,
    )
    .await
    .context(inbound::StoreAccessSnafu)?;
    let mut working_datasets =
        replay::materialise_dataset_slices(group_schema, touched_dataset_slices);
    let mut event_batches = ListenerDataChangeBatches::new();
    for scheduled_update in ready_updates {
        let ready_update =
            load_scheduled_pending_update(transaction, group_id, scheduled_update).await?;
        let applied_batch = apply_one_update(local_group, &mut working_datasets, &ready_update)?;
        if lifecycle.emits_data_changes() && !applied_batch.row_changes.is_empty() {
            let read_token =
                GroupReadToken::from_group_version(group_id, local_group.version_vector.clone());
            event_batches.push(ListenerDataChanges {
                read_token,
                row_changes: applied_batch.row_changes,
            });
        }
        apply_dataset_row_patches(transaction, group_schema, applied_batch.row_patches)
            .await
            .context(inbound::StoreAccessSnafu)?;
        transaction
            .mark_replication_update_applied(&group_id, scheduled_update.update_id)
            .await
            .context(inbound::StoreAccessSnafu)?;
    }
    Ok(event_batches)
}

/// Append bounded dependency gaps for updates still blocked at the applied vector.
///
/// Ranges follow pending-update order and are appended to the incoming update's needs.
fn extend_blocked_update_ranges(
    needed_ranges: &mut Vec<UpdateRangeMessage>,
    local_versions: &VersionVector,
    group_id: GroupId,
    lifecycle: &ReplicationGroupLifecycle,
    blocked_updates: &[UpdateDependency],
) -> Result<(), InboundDeliveryError> {
    for blocked_update in blocked_updates {
        let blocked_ranges = missing_ranges_for_update_frontier(
            local_versions,
            group_id,
            blocked_update.update_id,
            &blocked_update.read_versions,
        )?;
        needed_ranges.extend(bound_update_ranges(lifecycle, blocked_ranges));
    }
    Ok(())
}

/// Transaction-local distinction between waiting work and newly applied updates.
enum PendingDrainOutcome {
    /// No update can advance the vector; pending storage and stale-marker cleanup may change.
    NoReadyUpdates,
    /// Updates advanced the vector and prepared lifecycle-permitted listener batches.
    /// An empty collection means no application-visible callback is required.
    AppliedReadyUpdates {
        /// Ordered callback payloads prepared by the transaction's applied updates.
        event_batches: ListenerDataChangeBatches,
    },
}
