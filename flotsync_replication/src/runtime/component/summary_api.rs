//! Summary API, delivery-reply and advisory catch-up scheduling adapters.
//!
//! Remote correlation and reliable Summary response completion remain in the
//! summary manager. This module completes only runtime-owned `SummaryRequest` inputs.

use super::{
    ReplicationRuntimeComponent,
    inbound_support::{
        InboundDeliveryContext,
        InboundDeliveryFailure,
        SummaryCatchUpObservation,
        panic_if_fatal_inbound_failure,
    },
    update_ranges::bound_update_ranges,
};
use crate::{
    api::{
        ApiError,
        OwnedPageBatchInput,
        PageCursor,
        ReplicationStore,
        ReplicationUpdateFilter,
        ReplicationUpdatesQuery,
        StoreError,
        Summary,
        SummaryRequest,
        VecPageBatch,
    },
    codecs::messages::{RuntimeMessage, SummaryMessage, SummaryRequestMessage, UpdateRangeMessage},
    delivery::{contracts::GroupBroadcastPortRequest, shared::DeliveryClass},
    runtime::{
        catch_up_manager::subtract_available_ranges,
        errors::{InboundDeliveryError, SummaryError, inbound, summary},
        group_state::SharedGroupState,
        in_memory::LoadedGroupMeta,
        summary_request_manager::SummaryRequestManagerMessage,
    },
};
use flotsync_core::{
    GroupId,
    MemberIdentity,
    membership::SharedGroupMemberships,
    versions::{UpdateId, VersionVector},
};
use flotsync_messages::proto::EncodeProto;
use flotsync_utils::{BoxError, KClaimablePromise, ResultExt as _};
use itertools::Itertools;
use kompact::prelude::*;
use snafu::prelude::*;

/// Delivery path that should carry one inbound summary response.
pub(super) enum SummaryReplyRoute {
    /// Reply to a recipient-addressed reliable request and acknowledge it after
    /// the response is submitted.
    Reliable {
        /// Authenticated sender to which the requested reply is addressed.
        recipient: MemberIdentity,
        /// Runtime-owned processing obligation, completed only after reply submission.
        processed: KClaimablePromise<()>,
    },
    /// Reply to a group-broadcast request by publishing the summary to the group.
    GroupBroadcast,
}

/// Validate the target against one coherent published membership snapshot.
fn validate_summary_request(
    group_memberships: &SharedGroupState,
    request: &SummaryRequest,
) -> Result<(), SummaryError> {
    let memberships = group_memberships.snapshot();
    let members = memberships
        .members(&request.group_id)
        .context(summary::UnknownGroupSnafu {
            group_id: request.group_id,
        })?;
    ensure!(
        members.contains(&request.target),
        summary::TargetNotInGroupSnafu {
            group_id: request.group_id,
            target: request.target.clone(),
        }
    );
    Ok(())
}

/// Read lifecycle-bounded progress for a protocol reply.
///
/// `Some` is the hosted group's bounded vector; `None` means the group is absent.
/// The read transaction is dropped on return, preserving the protocol read boundary.
async fn load_summary_versions_from_store(
    store: &dyn ReplicationStore,
    group_id: GroupId,
) -> Result<Option<VersionVector>, StoreError> {
    let mut transaction = store.begin_read_transaction().await?;
    let group = transaction.load_replication_group(&group_id).await?;
    Ok(group.map(|group| group.lifecycle.bound_versions(&group.version_vector)))
}

/// Preserve the protocol correlation id when constructing a summary reply.
fn summary_message_for_request(
    message: SummaryRequestMessage,
    has_versions: VersionVector,
) -> RuntimeMessage {
    RuntimeMessage::Summary(SummaryMessage::new(
        message.group_id,
        message.correlation_id,
        has_versions,
    ))
}

/// Build a protocol reply or classify an absent hosted group/store failure.
async fn load_summary_message_from_store(
    store: &dyn ReplicationStore,
    message: SummaryRequestMessage,
) -> Result<RuntimeMessage, InboundDeliveryError> {
    let has_versions = load_summary_versions_from_store(store, message.group_id)
        .await
        .context(inbound::StoreAccessSnafu)?;
    let has_versions = has_versions.context(inbound::UnknownHostedGroupSnafu {
        group_id: message.group_id,
    })?;
    Ok(summary_message_for_request(message, has_versions))
}

/// Attribute an API summary to the responding member and requested group.
fn summary_for_request(
    group_id: GroupId,
    responder: MemberIdentity,
    has_versions: VersionVector,
) -> Summary {
    Summary {
        group_id,
        responder,
        has_versions,
    }
}

/// Read the current vector for an API query, releasing the read before lifecycle validation.
///
/// Absent and closed groups retain their API-facing summary errors; store access
/// failures include read-release errors. This policy differs from protocol replies.
async fn load_api_summary_versions(
    store: &dyn ReplicationStore,
    group_id: GroupId,
) -> Result<VersionVector, SummaryError> {
    let mut transaction = store
        .begin_read_transaction()
        .await
        .context(summary::StoreAccessSnafu)?;
    let group = transaction
        .load_replication_group(&group_id)
        .await
        .context(summary::StoreAccessSnafu)?
        .context(summary::UnknownGroupSnafu { group_id })?;
    transaction
        .release()
        .await
        .context(summary::StoreAccessSnafu)?;
    ensure!(
        group.lifecycle.is_readable(),
        summary::GroupClosedSnafu { group_id }
    );
    Ok(group.version_vector)
}

/// Inspect local progress after a peer summary and derive catch-up notifications.
///
/// `Some` contains bounded observations for a hosted group; `None` means it is absent.
/// Store failures and invalid persisted group metadata retain their classified errors.
/// The read transaction ends when this inspection returns.
async fn inspect_summary_catch_up(
    store: &dyn ReplicationStore,
    local_member: &MemberIdentity,
    summary: Summary,
) -> Result<Option<SummaryCatchUpObservation>, InboundDeliveryError> {
    let mut transaction = store
        .begin_read_transaction()
        .await
        .context(inbound::StoreAccessSnafu)?;
    let Some(persisted_group) = transaction
        .load_replication_group(&summary.group_id)
        .await
        .context(inbound::StoreAccessSnafu)?
    else {
        return Ok(None);
    };
    let mut pending_cursor = PageCursor::new(ReplicationUpdatesQuery::new(
        summary.group_id,
        ReplicationUpdateFilter::PendingApply,
    ));
    let mut pending_batch =
        VecPageBatch::<UpdateRangeMessage, (), OwnedPageBatchInput<UpdateId>, _>::unlimited_with(
            |update_id| Ok::<_, BoxError>(UpdateRangeMessage::from(update_id)),
        );
    transaction
        .load_replication_update_ids_into(&mut pending_cursor, &mut pending_batch)
        .await
        .map_err(StoreError::from)
        .context(inbound::StoreAccessSnafu)?;
    let lifecycle = persisted_group.lifecycle.clone();
    let bounded_summary_versions = lifecycle.bound_versions(&summary.has_versions);
    let local_group = LoadedGroupMeta::from_replication_group_record(local_member, persisted_group)
        .context(inbound::InvalidPersistedGroupSnafu {
            group_id: summary.group_id,
        })?;
    let summary_needed_ranges = local_group
        .version_vector
        .missing_version_ranges_to(&bounded_summary_versions)
        .into_iter()
        .map(UpdateRangeMessage::from)
        .collect_vec();
    let observed_available = bound_update_ranges(&lifecycle, pending_batch.into_values());
    let needed_ranges = subtract_available_ranges(&summary_needed_ranges, &observed_available);
    Ok(Some(SummaryCatchUpObservation {
        group_id: summary.group_id,
        observed_available,
        needed_ranges,
    }))
}

// TODO(flotsync-zop): Replace this temporary component extension with finite summary
// flow impls beside their own types in flotsync-zop.14. Keep thin component dispatch
// and scheduling glue in component/mod.rs; all component impls must be there before
// parent completion, as checked by flotsync-zop.24.
impl ReplicationRuntimeComponent {
    /// Answer a local query or transfer the original promise to the remote summary manager.
    pub(super) fn handle_request_summary(
        &mut self,
        ask: Ask<SummaryRequest, Result<Summary, ApiError>>,
    ) -> HandlerResult {
        let (promise, request) = ask.take();
        if let Err(error) = validate_summary_request(self.group_memberships.as_ref(), &request) {
            let reply = Err(ApiError::from_store_classification_source(error));
            self.reply_api(promise, "request_summary", reply);
            return Handled::OK;
        }
        Handled::block_on(self, async move |async_self| {
            let reply =
                load_api_summary_versions(async_self.store.as_ref(), request.group_id).await;
            match reply {
                Ok(has_versions) if request.target == async_self.local_member => {
                    let summary = summary_for_request(
                        request.group_id,
                        async_self.local_member.clone(),
                        has_versions,
                    );
                    async_self.reply_api(promise, "request_summary", Ok(summary));
                }
                Ok(_) => {
                    // Temporary glue: public runtime API requests currently enter through
                    // this component and are then routed to the dedicated summary manager.
                    // See flotsync-ozi for revisiting this component boundary.
                    async_self.summary_request_manager.tell(
                        SummaryRequestManagerMessage::RequestSummary(Ask::new(promise, request)),
                    );
                }
                Err(error) => {
                    let reply = Err(ApiError::from_store_classification_source(error));
                    async_self.reply_api(promise, "request_summary", reply);
                }
            }
            Handled::OK
        })
    }

    /// Build and submit an inbound reply before completing runtime-owned reliable processing.
    pub(super) fn handle_inbound_summary_request(
        &mut self,
        context: InboundDeliveryContext,
        route: SummaryReplyRoute,
        message: SummaryRequestMessage,
    ) -> HandlerResult {
        Handled::block_on(self, async move |mut async_self| {
            let reply = async {
                let summary =
                    load_summary_message_from_store(async_self.store.as_ref(), message).await?;
                async_self.submit_summary_reply(route, message, &summary)
            }
            .await;
            if let Err(error) = reply {
                let failure = InboundDeliveryFailure::new(context, error);
                let action = async_self.record_inbound_failure(&failure);
                panic_if_fatal_inbound_failure(action, &failure);
            }
            Handled::OK
        })
    }

    /// Finish the peer-progress read before admitting another runtime operation.
    pub(super) fn handle_observed_summary(&mut self, summary: Summary) -> HandlerResult {
        // block_on suspends this component's spawn_local tasks. A spawned summary
        // read could retain SQLite locks needed by a later blocking update, leaving
        // the update waiting for a read task it prevents from resuming. Complete
        // this read in the blocking turn so its transaction ends before writes enter.
        // TODO(flotsync-zop.21, flotsync-zop.22): Replace this blocking boundary
        // once lock-holding effects can keep progressing while other effects await
        // their locks or pool connections. Prove an overlapping summary reader can
        // resume, release its transaction and let a conflicting writer complete.
        Handled::block_on(self, async move |async_self| {
            if let Some(observation) = inspect_summary_catch_up(
                async_self.store.as_ref(),
                &async_self.local_member,
                summary,
            )
            .await
            .whatever_benign("failed to inspect local progress for summary catch-up")?
            {
                async_self.notify_catch_up_available(
                    observation.group_id,
                    observation.observed_available,
                );
                async_self.notify_catch_up_needed(observation.group_id, observation.needed_ranges);
            } else {
                // The summary refers to a group this runtime no longer hosts.
            }
            Handled::OK
        })
    }

    /// Submit through the request's delivery path, then complete reliable processing.
    ///
    /// Failure means the processed promise could not be completed after reliable submission.
    fn submit_summary_reply(
        &mut self,
        route: SummaryReplyRoute,
        message: SummaryRequestMessage,
        summary: &RuntimeMessage,
    ) -> Result<(), InboundDeliveryError> {
        match route {
            SummaryReplyRoute::Reliable {
                recipient,
                processed,
            } => {
                self.submit_reliable_runtime_message(recipient, summary);
                processed
                    .complete()
                    .context(inbound::CompleteProcessedPromiseSnafu {
                        group_id: message.group_id,
                    })?;
            }
            SummaryReplyRoute::GroupBroadcast => {
                self.group_broadcast.trigger(
                    GroupBroadcastPortRequest::build_submit(DeliveryClass::BestEffort)
                        .for_member_in_group(self.local_member.clone(), message.group_id)
                        .with_payload(summary.encode_proto_to_bytes()),
                );
            }
        }
        Ok(())
    }
}
