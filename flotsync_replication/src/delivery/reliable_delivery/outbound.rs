//! Persisted outbound admission, queueing, transport, and cleanup state.

use super::{
    ActiveRouteRecord,
    DeliverySecurity,
    DeliverySecurityError,
    EncryptedPayload,
    LogicalRouteId,
    MemberIdentity,
    MessageId,
    PendingRemoval,
    PendingRemovalReason,
    PendingRouteReason,
    ReliableDeliveryStore,
    ReliableDeliverySubmit,
    ReliableDeliveryWorkItem,
    ReliableMessageEnvelope,
    RouteActiveState,
    RouteSendId,
    SendRouteCandidate,
    StableRouteKey,
    StoredReliableDeliveryWork,
    StoredReliableDeliveryWorkMetadata,
    TransportRouteKey,
    WorkScopeKey,
};
use bytes::Bytes;
use flotsync_core::member::TrieMap;
use flotsync_io::prelude::{EgressAsyncWriter, PayloadWriter as _};
use flotsync_messages::{
    buffa::Message as _,
    serialisation::{FlotsyncSerializable, FlotsyncSerializeError, SizeHint},
};
use flotsync_routes::{RouteTransportNackReason, RouteTransportSubmitResult};
use flotsync_utils::BoxFuture;
use futures_util::FutureExt as _;
use kompact::{
    KompactLogger,
    prelude::{error, warn},
};
use smallvec::SmallVec;
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
    time::SystemTime,
};

/// Sender-side admission, persistence, queueing, and cleanup state.
pub(super) struct OutboundState {
    /// Storage access for envelopes that passed security admission.
    pub(super) store: Arc<dyn ReliableDeliveryStore>,
    /// Submissions that have not yet produced a persisted encrypted envelope.
    pub(super) unpersisted_submissions: HashMap<MessageId, UnpersistedSubmission>,
    /// Lifecycle state for every persisted envelope until final cleanup converges.
    pub(super) work_items: HashMap<MessageId, ReliableDeliveryWorkItem>,
    /// Oldest-first keys for persisted work with an available route.
    pub(super) ready: BTreeSet<ReadyKey>,
    /// Persisted work without an available route, grouped by recipient.
    pub(super) waiting: TrieMap<BTreeSet<ReadyKey>>,
    /// Message ids reserving a configured outbound-attempt slot.
    pub(super) attempts_in_flight: HashSet<MessageId>,
}

impl OutboundState {
    /// Build empty outbound state around its persistence boundary.
    pub(super) fn new(store: Arc<dyn ReliableDeliveryStore>) -> Self {
        Self {
            store,
            unpersisted_submissions: HashMap::new(),
            work_items: HashMap::new(),
            ready: BTreeSet::new(),
            waiting: TrieMap::new(),
            attempts_in_flight: HashSet::new(),
        }
    }

    /// Return whether the message id belongs to any live or cleanup-pending outbound work.
    pub(super) fn contains_message(&self, message_id: MessageId) -> bool {
        self.work_items.contains_key(&message_id)
            || self.unpersisted_submissions.contains_key(&message_id)
    }

    /// Seal one plaintext submission and build the exact work value persisted across retries.
    pub(super) async fn seal_submission(
        security: &DeliverySecurity,
        submit: ReliableDeliverySubmit,
        first_submitted_at: SystemTime,
    ) -> Result<StoredReliableDeliveryWork, DeliverySecurityError> {
        let envelope = submit.envelope;
        let recipient = envelope.header.recipient.clone();
        let message_id = envelope.header.message_id;
        let sealed = security
            .seal_reliable_payload(&envelope.header, envelope.payload.bytes.as_ref())
            .await?;
        let envelope = ReliableMessageEnvelope::<EncryptedPayload> {
            header: envelope.header,
            payload: EncryptedPayload { sealed },
        };
        Ok(StoredReliableDeliveryWork {
            metadata: StoredReliableDeliveryWorkMetadata {
                message_id,
                recipient,
                first_submitted_at,
            },
            encoded_envelope: envelope.to_wire_format().encode_to_bytes(),
        })
    }

    /// Rebuild one lightweight sender item without loading its stored envelope.
    pub(super) fn new_work_item(
        metadata: StoredReliableDeliveryWorkMetadata,
        reason: PendingRouteReason,
    ) -> ReliableDeliveryWorkItem {
        let recipient = metadata.recipient.clone();
        let message_id = metadata.message_id;
        ReliableDeliveryWorkItem {
            metadata,
            recipient_route: direct_route_record(
                &recipient,
                message_id,
                RouteActiveState::WaitingForRoute { reason },
            ),
            relay_routes: Vec::new(),
            reported_ack_timeout: false,
            pending_removal: None,
        }
    }

    /// Put an item with an available route into `ready`, or otherwise into `waiting`.
    ///
    /// Returns an error after purging the message when its authoritative work is missing.
    pub(super) fn place_by_route(
        &mut self,
        message_id: MessageId,
        direct_routes: &TrieMap<SendRouteCandidate<TransportRouteKey>>,
        logger: &KompactLogger,
    ) -> Result<(), PurgedIndexedWork> {
        let Some(work_item) = self.work_items.get(&message_id) else {
            error!(
                logger,
                "Reliable delivery could not place missing outbound work message_id={message_id}; purging its in-memory state"
            );
            self.purge_message(message_id);
            return Err(PurgedIndexedWork);
        };
        if work_item.pending_removal.is_some() {
            error!(
                logger,
                "Reliable delivery tried to make cleanup-pending work sendable message_id={message_id}; purging its in-memory state"
            );
            self.purge_message(message_id);
            return Err(PurgedIndexedWork);
        }
        let ready_key = ReadyKey::from_metadata(&work_item.metadata);
        let recipient = work_item.metadata.recipient.clone();
        let waiting_reason = match &work_item.recipient_route.state {
            RouteActiveState::WaitingForRoute { reason }
            | RouteActiveState::RetryScheduled { reason } => reason.clone(),
            _ => PendingRouteReason::PeerCurrentlyUnreachable,
        };
        self.remove_route_references(ready_key, &recipient);
        let Some(work_item) = self.work_items.get_mut(&message_id) else {
            error!(
                logger,
                "Reliable delivery lost outbound work while placing message_id={message_id}; purging its in-memory state"
            );
            self.purge_message(message_id);
            return Err(PurgedIndexedWork);
        };
        work_item.recipient_route.state = RouteActiveState::Queued;
        if direct_routes.get(&recipient).is_some() {
            self.ready.insert(ready_key);
        } else {
            return self.move_to_waiting(ready_key, waiting_reason, logger);
        }
        Ok(())
    }

    /// Reserve the oldest ready attempts without performing asynchronous storage access.
    ///
    /// Returned attempts form a stack with the oldest item at the end.
    pub(super) fn select_ready_attempts(
        &mut self,
        available_slots: usize,
        direct_routes: &TrieMap<SendRouteCandidate<TransportRouteKey>>,
        logger: &KompactLogger,
    ) -> SelectedAttempts {
        let mut selected = SelectedAttempts::default();
        while selected.attempts.len() < available_slots
            && let Some(ready_key) = self.ready.pop_first()
        {
            let Ok(recipient) = self.with_validated_indexed_work_item(
                ready_key,
                IndexedRouteState::Queued,
                "ready",
                logger,
                |work_item| work_item.metadata.recipient.clone(),
            ) else {
                selected.purged.push(ready_key.message_id);
                continue;
            };
            let Some(route) = direct_routes.get(&recipient).cloned() else {
                if self
                    .move_to_waiting(
                        ready_key,
                        PendingRouteReason::PeerCurrentlyUnreachable,
                        logger,
                    )
                    .is_err()
                {
                    selected.purged.push(ready_key.message_id);
                }
                continue;
            };
            let send_id = RouteSendId::new_random();
            if self
                .with_validated_indexed_work_item(
                    ready_key,
                    IndexedRouteState::Queued,
                    "selected ready",
                    logger,
                    |work_item| {
                        work_item.recipient_route.state =
                            RouteActiveState::AttemptingDirect { send_id };
                    },
                )
                .is_err()
            {
                selected.purged.push(ready_key.message_id);
                continue;
            }
            if !self.attempts_in_flight.insert(ready_key.message_id) {
                error!(
                    logger,
                    "Reliable delivery selected duplicate in-flight message_id={}; purging its in-memory state",
                    ready_key.message_id
                );
                self.purge_message(ready_key.message_id);
                selected.purged.push(ready_key.message_id);
                continue;
            }
            selected.attempts.insert(
                0,
                OutboundAttempt {
                    message_id: ready_key.message_id,
                    route,
                    send_id,
                },
            );
        }
        selected
    }

    /// Mark one persisted item as ineligible until its retry timeout fires.
    pub(super) fn mark_retry_scheduled(
        &mut self,
        message_id: MessageId,
        reason: PendingRouteReason,
    ) {
        if let Some(work_item) = self.work_items.get_mut(&message_id) {
            work_item.recipient_route.state = RouteActiveState::RetryScheduled { reason };
            let ready_key = ReadyKey::from_metadata(&work_item.metadata);
            let recipient = work_item.metadata.recipient.clone();
            self.remove_route_references(ready_key, &recipient);
        }
    }

    /// Move waiting work for a newly available recipient route into the ready queue.
    ///
    /// Returned ids were purged after an invariant mismatch and need their shared retries
    /// cancelled by the component.
    pub(super) fn ready_waiting_items_for_route(
        &mut self,
        recipient: &MemberIdentity,
        logger: &KompactLogger,
    ) -> SmallVec<[MessageId; 1]> {
        let mut purged = SmallVec::new();
        let Some(waiting) = self.waiting.remove(recipient) else {
            return purged;
        };
        for ready_key in waiting {
            if self
                .with_validated_indexed_work_item(
                    ready_key,
                    IndexedRouteState::WaitingForRoute,
                    "waiting",
                    logger,
                    |work_item| {
                        work_item.recipient_route.state = RouteActiveState::Queued;
                    },
                )
                .is_err()
            {
                purged.push(ready_key.message_id);
                continue;
            }
            self.ready.insert(ready_key);
        }
        purged
    }

    /// Move all ready work for a recipient into its waiting set in one partitioning pass.
    ///
    /// Returned ids were purged after an invariant mismatch and need their shared retries
    /// cancelled by the component.
    pub(super) fn withdraw_ready(
        &mut self,
        recipient: &MemberIdentity,
        logger: &KompactLogger,
    ) -> SmallVec<[MessageId; 1]> {
        let ready = std::mem::take(&mut self.ready);
        let (affected, unaffected) = ready.into_iter().partition(|ready_key| {
            self.work_items
                .get(&ready_key.message_id)
                .is_none_or(|work_item| work_item.metadata.recipient == *recipient)
        });
        self.ready = unaffected;

        let mut purged = SmallVec::new();
        for ready_key in affected {
            if self
                .move_to_waiting(
                    ready_key,
                    PendingRouteReason::PeerCurrentlyUnreachable,
                    logger,
                )
                .is_err()
            {
                purged.push(ready_key.message_id);
            }
        }
        purged
    }

    /// Stop sending one item while retaining its work record through final cleanup.
    ///
    /// Returns `true` when authoritative work was found and marked, and `false` otherwise.
    pub(super) fn begin_removal(
        &mut self,
        message_id: MessageId,
        reason: PendingRemovalReason,
    ) -> bool {
        let outstanding_send_id = self
            .work_items
            .get(&message_id)
            .and_then(|work_item| work_item.recipient_route.state.active_send_id());
        self.begin_removal_with_outstanding_attempt(message_id, reason, outstanding_send_id)
    }

    /// Return the removal reason if this message is marked for removal, or `None` otherwise.
    pub(super) fn pending_removal_reason(
        &self,
        message_id: MessageId,
    ) -> Option<PendingRemovalReason> {
        let work_item = self.work_items.get(&message_id)?;
        work_item.pending_removal.map(|removal| removal.reason)
    }

    /// Record successful stored-row cleanup and discard converged work.
    pub(super) fn finish_stored_removal(&mut self, message_id: MessageId) {
        if let Some(work_item) = self.work_items.get_mut(&message_id)
            && let Some(removal) = work_item.pending_removal.as_mut()
        {
            removal.stored_row_pending = false;
        }
        self.remove_if_cleanup_complete(message_id);
    }

    /// Apply one completed transport result and release its reserved capacity slot.
    pub(super) fn finish_outbound_attempt(
        &mut self,
        message_id: MessageId,
        send_id: RouteSendId,
        result: OutboundAttemptResult,
        logger: &KompactLogger,
    ) -> AttemptCompletion {
        if !self.attempts_in_flight.remove(&message_id) {
            error!(
                logger,
                "Reliable delivery completed unindexed outbound attempt message_id={message_id} send_id={send_id:?}; purging its in-memory state"
            );
            self.purge_message(message_id);
            return AttemptCompletion::Purged;
        }
        let Some(work_item) = self.work_items.get(&message_id) else {
            error!(
                logger,
                "Reliable delivery completed outbound attempt for missing work message_id={message_id} send_id={send_id:?}; purging its in-memory state"
            );
            self.purge_message(message_id);
            return AttemptCompletion::Purged;
        };
        if work_item.recipient_route.state != (RouteActiveState::AttemptingDirect { send_id }) {
            error!(
                logger,
                "Reliable delivery completed mismatched outbound attempt message_id={message_id} send_id={send_id:?} route_state={:?}; purging its in-memory state",
                work_item.recipient_route.state
            );
            self.purge_message(message_id);
            return AttemptCompletion::Purged;
        }
        if let Some(removal) = work_item.pending_removal {
            if removal.outstanding_send_id != Some(send_id) {
                error!(
                    logger,
                    "Reliable delivery cleanup tracked a mismatched outbound attempt message_id={message_id} completed_send_id={send_id:?} outstanding_send_id={:?}; purging its in-memory state",
                    removal.outstanding_send_id
                );
                self.purge_message(message_id);
                return AttemptCompletion::Purged;
            }
            self.work_items
                .get_mut(&message_id)
                .expect("validated outbound work must remain present")
                .pending_removal
                .as_mut()
                .expect("cleanup-pending work must retain its cleanup state")
                .outstanding_send_id = None;
            self.remove_if_cleanup_complete(message_id);
            return AttemptCompletion::CleanupOnly;
        }

        match result {
            OutboundAttemptResult::Transport(RouteTransportSubmitResult::Sent { .. }) => {
                self.work_items
                    .get_mut(&message_id)
                    .expect("validated outbound work must remain present")
                    .recipient_route
                    .state = RouteActiveState::AwaitingRecipientAck;
                AttemptCompletion::AwaitingRecipientAck
            }
            OutboundAttemptResult::Transport(RouteTransportSubmitResult::SendFailed {
                reason: RouteTransportNackReason::InvalidPayload,
                ..
            }) => {
                self.begin_removal_with_outstanding_attempt(
                    message_id,
                    PendingRemovalReason::PermanentTransportFailure,
                    None,
                );
                AttemptCompletion::PermanentFailure
            }
            OutboundAttemptResult::Transport(RouteTransportSubmitResult::SendFailed {
                reason,
                ..
            }) => {
                self.mark_retry_scheduled(message_id, sender_retry_reason(&reason));
                warn!(
                    logger,
                    "Reliable delivery outbound envelope send failed for {message_id}: {reason:?}"
                );
                AttemptCompletion::RetryScheduled
            }
            OutboundAttemptResult::PromiseDropped => {
                self.mark_retry_scheduled(message_id, PendingRouteReason::LocalResourcePressure);
                warn!(
                    logger,
                    "Reliable delivery outbound envelope promise dropped for {message_id}"
                );
                AttemptCompletion::RetryScheduled
            }
            OutboundAttemptResult::LoadFailed => {
                self.mark_retry_scheduled(message_id, PendingRouteReason::LocalResourcePressure);
                AttemptCompletion::RetryScheduled
            }
        }
    }

    /// Attempt cleanup of one stored row while keeping retry policy outside this state owner.
    pub(super) async fn attempt_stored_removal(
        &mut self,
        message_id: MessageId,
        logger: &KompactLogger,
    ) -> StoredRemovalAttempt {
        let Some(reason) = self.pending_removal_reason(message_id) else {
            return StoredRemovalAttempt::Stale;
        };
        match self.store.remove_reliable_delivery_work(message_id).await {
            Ok(removed) => {
                self.finish_stored_removal(message_id);
                if !removed {
                    error!(
                        logger,
                        "Reliable delivery stored row for message_id={} was already absent while completing {}; cleanup converged and processing will continue",
                        message_id,
                        reason.description()
                    );
                }
                StoredRemovalAttempt::Completed
            }
            Err(error) => {
                // TODO(flotsync-7g5): Classify store errors so permanent
                // removal failures can terminate cleanup explicitly.
                error!(
                    logger,
                    "Reliable delivery failed to remove stored outbound message_id={} after {}; treating the store error as retryable and keeping the message excluded from sending: {}",
                    message_id,
                    reason.description(),
                    error
                );
                StoredRemovalAttempt::Retry
            }
        }
    }

    /// Purge every discoverable outbound in-memory reference to one inconsistent message id.
    ///
    /// When authoritative work is missing, indexed callers have already detached the stale key
    /// that exposed the mismatch, so no global index scan is necessary.
    pub(super) fn purge_message(&mut self, message_id: MessageId) {
        if let Some(work_item) = self.work_items.remove(&message_id) {
            let ready_key = ReadyKey::from_metadata(&work_item.metadata);
            let recipient = work_item.metadata.recipient;
            self.remove_route_references(ready_key, &recipient);
        }
        self.unpersisted_submissions.remove(&message_id);
        self.attempts_in_flight.remove(&message_id);
    }

    /// Remove one exact work key from either route-eligibility collection.
    pub(super) fn remove_route_references(
        &mut self,
        ready_key: ReadyKey,
        recipient: &MemberIdentity,
    ) {
        self.ready.remove(&ready_key);
        let remove_recipient = self.waiting.get_mut(recipient).is_some_and(|waiting| {
            waiting.remove(&ready_key);
            waiting.is_empty()
        });
        if remove_recipient {
            self.waiting.remove(recipient);
        }
    }

    /// Move one known work key into its recipient route-waiting collection.
    ///
    /// Returns an error after purging the message when its indexed work is inconsistent.
    fn move_to_waiting(
        &mut self,
        ready_key: ReadyKey,
        reason: PendingRouteReason,
        logger: &KompactLogger,
    ) -> Result<(), PurgedIndexedWork> {
        let recipient = self.with_validated_indexed_work_item(
            ready_key,
            IndexedRouteState::Queued,
            "ready-to-waiting",
            logger,
            |work_item| {
                let recipient = work_item.metadata.recipient.clone();
                work_item.recipient_route.state = RouteActiveState::WaitingForRoute { reason };
                recipient
            },
        )?;
        self.waiting
            .get_mut_or_default(&recipient)
            .insert(ready_key);
        Ok(())
    }

    /// Validate one queue index entry and mutate its authoritative work item.
    fn with_validated_indexed_work_item<T>(
        &mut self,
        ready_key: ReadyKey,
        expected_state: IndexedRouteState,
        index_name: &'static str,
        logger: &KompactLogger,
        update: impl FnOnce(&mut ReliableDeliveryWorkItem) -> T,
    ) -> Result<T, PurgedIndexedWork> {
        let updated = if let Some(work_item) = self.work_items.get_mut(&ready_key.message_id) {
            let actual_key = ReadyKey::from_metadata(&work_item.metadata);
            let is_active = work_item.pending_removal.is_none();
            if actual_key == ready_key
                && expected_state.matches(&work_item.recipient_route.state)
                && is_active
            {
                Some(update(work_item))
            } else {
                error!(
                    logger,
                    "Reliable delivery {index_name} index mismatch for message_id={}: indexed_key={ready_key:?} actual_key={actual_key:?} route_state={:?} cleanup_pending={}; purging its in-memory state",
                    ready_key.message_id,
                    work_item.recipient_route.state,
                    !is_active
                );
                None
            }
        } else {
            error!(
                logger,
                "Reliable delivery {index_name} index referenced missing work message_id={}; purging its in-memory state",
                ready_key.message_id
            );
            None
        };
        if let Some(updated) = updated {
            Ok(updated)
        } else {
            self.purge_message(ready_key.message_id);
            Err(PurgedIndexedWork)
        }
    }

    /// Discard cleanup work once storage and transport have both converged.
    fn remove_if_cleanup_complete(&mut self, message_id: MessageId) {
        let cleanup_complete = self
            .work_items
            .get(&message_id)
            .and_then(|work_item| work_item.pending_removal)
            .is_some_and(|removal| {
                !removal.stored_row_pending
                    && removal.outstanding_send_id.is_none()
                    && !self.attempts_in_flight.contains(&message_id)
            });
        if cleanup_complete {
            self.work_items.remove(&message_id);
        }
    }

    /// Enter cleanup with the exact transport result that must still converge.
    ///
    /// Returns `true` if the work item for `message_id` existed.
    fn begin_removal_with_outstanding_attempt(
        &mut self,
        message_id: MessageId,
        reason: PendingRemovalReason,
        outstanding_send_id: Option<RouteSendId>,
    ) -> bool {
        self.unpersisted_submissions.remove(&message_id);
        if let Some(work_item) = self.work_items.get_mut(&message_id) {
            work_item.pending_removal = Some(PendingRemoval {
                reason,
                stored_row_pending: true,
                outstanding_send_id,
            });
            let ready_key = ReadyKey::from_metadata(&work_item.metadata);
            let recipient = work_item.metadata.recipient.clone();
            self.remove_route_references(ready_key, &recipient);
            true
        } else {
            false
        }
    }
}

/// Component-visible scheduling effect of a completed transport attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AttemptCompletion {
    /// Transport accepted the envelope; schedule the recipient-ack timeout.
    AwaitingRecipientAck,
    /// Transport did not accept the envelope; schedule the ordinary sender retry.
    RetryScheduled,
    /// Transport permanently rejected the envelope; attempt stored-row cleanup.
    PermanentFailure,
    /// Cleanup already owns the message; only capacity accounting changed.
    CleanupOnly,
    /// An invariant mismatch caused the message to be purged.
    Purged,
}

/// Local or transport result that completes one reserved outbound attempt.
pub(super) enum OutboundAttemptResult {
    /// The stored envelope could not be loaded before transport submission.
    LoadFailed,
    /// The transport promise was dropped before returning a submission result.
    PromiseDropped,
    /// Route transport returned its submission result.
    Transport(RouteTransportSubmitResult<TransportRouteKey>),
}

/// Result of attempting one cleanup-pending stored-row removal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum StoredRemovalAttempt {
    /// Storage cleanup converged, whether or not the row was already absent.
    Completed,
    /// Storage returned an error currently treated as retryable.
    Retry,
    /// No cleanup-pending work exists for this message id.
    Stale,
}

/// Expected authoritative route state for one scheduling index entry.
#[derive(Clone, Copy)]
enum IndexedRouteState {
    /// Entry belongs to the ready index.
    Queued,
    /// Entry belongs to the route-waiting index.
    WaitingForRoute,
}

impl IndexedRouteState {
    /// Return `true` when the authoritative route state belongs to this scheduling index.
    fn matches(self, state: &RouteActiveState) -> bool {
        match self {
            Self::Queued => matches!(state, RouteActiveState::Queued),
            Self::WaitingForRoute => matches!(state, RouteActiveState::WaitingForRoute { .. }),
        }
    }
}

/// Marker returned after inconsistent indexed work has been purged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct PurgedIndexedWork;

/// Admission state that has not yet produced a persisted sender row.
#[derive(Clone)]
pub(super) enum UnpersistedSubmission {
    /// Original plaintext awaiting its first successful security admission.
    Plaintext {
        /// One-to-one request retained unchanged after retryable security failures.
        submit: ReliableDeliverySubmit,
        /// Wall-clock time retained for inspection and oldest-first ordering.
        first_submitted_at: SystemTime,
    },
    /// Successfully sealed envelope awaiting idempotent storage.
    Encoded {
        /// Complete encoded envelope reused unchanged across persistence retries.
        work: StoredReliableDeliveryWork,
    },
}

/// Stable oldest-first queue key for one persisted sender item.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct ReadyKey {
    /// Primary ordering value retained across retries and restart recovery.
    pub(super) first_submitted_at: SystemTime,
    /// Deterministic tie-breaker and lookup key into outbound work items.
    pub(super) message_id: MessageId,
}

impl ReadyKey {
    /// Construct the sole queue key corresponding to stored metadata.
    pub(super) fn from_metadata(metadata: &StoredReliableDeliveryWorkMetadata) -> Self {
        Self {
            first_submitted_at: metadata.first_submitted_at,
            message_id: metadata.message_id,
        }
    }
}

/// Selected attempts plus inconsistent message ids purged during selection.
#[derive(Default)]
pub(super) struct SelectedAttempts {
    /// Slot-reserving attempt stack with the oldest item at the end.
    pub(super) attempts: SmallVec<[OutboundAttempt; 1]>,
    /// Message ids whose shared retry keys must also be cancelled.
    pub(super) purged: SmallVec<[MessageId; 1]>,
}

/// One slot-reserving outbound attempt selected before its envelope is loaded.
pub(super) struct OutboundAttempt {
    /// Persisted work item whose full encoded envelope must be loaded.
    pub(super) message_id: MessageId,
    /// Route snapshot chosen when the ready key reserved its active slot.
    pub(super) route: SendRouteCandidate<TransportRouteKey>,
    /// Stable identifier shared with the work item attempting-route state.
    pub(super) send_id: RouteSendId,
}

/// Exact persisted endpoint-frame bytes reused for every transport retry.
pub(super) struct EncodedReliableEnvelope(pub(super) Bytes);

impl FlotsyncSerializable for EncodedReliableEnvelope {
    fn serialized_size_hint(&self) -> SizeHint {
        SizeHint::Exact(self.0.len())
    }

    fn serialize_into<'a>(
        &'a self,
        writer: &'a mut EgressAsyncWriter,
    ) -> BoxFuture<'a, Result<(), FlotsyncSerializeError>> {
        async move {
            writer
                .splice_bytes(self.0.clone())
                .await
                .map_err(|source| FlotsyncSerializeError::Io { source })?;
            Ok(())
        }
        .boxed()
    }
}

/// Build the direct-route record for one outbound work item.
fn direct_route_record(
    recipient: &MemberIdentity,
    message_id: MessageId,
    state: RouteActiveState,
) -> ActiveRouteRecord {
    ActiveRouteRecord {
        key: StableRouteKey {
            scope: WorkScopeKey::Reliable {
                recipient: recipient.clone(),
                message_id,
            },
            route_id: LogicalRouteId::peer(recipient.clone()),
        },
        state,
    }
}

/// Map a retryable transport rejection to the scheduler-facing route reason.
fn sender_retry_reason(reason: &RouteTransportNackReason) -> PendingRouteReason {
    match reason {
        RouteTransportNackReason::RouteUnknown | RouteTransportNackReason::RouteUnavailable => {
            PendingRouteReason::PeerCurrentlyUnreachable
        }
        RouteTransportNackReason::Backpressure
        | RouteTransportNackReason::LocalResourcePressure => {
            PendingRouteReason::LocalResourcePressure
        }
        RouteTransportNackReason::InvalidPayload | RouteTransportNackReason::Other(_) => {
            PendingRouteReason::BackoffInEffect
        }
    }
}
