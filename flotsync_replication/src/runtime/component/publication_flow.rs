//! Handles one local publication from its write transaction until its API reply finishes.
//!
//! [`PublicationFlow`] records progress and returns actions for the component to execute,
//! following the flow/action/result model described in [`super::flow_driver`]. Success
//! follows `Writing -> Broadcasting -> Advertising -> NotifyingListener -> Replying -> Finished`:
//! commit the update, submit its bytes to broadcast, advertise its availability to
//! catch-up, await the listener callback, then attempt the API reply. Submission means
//! sending to the output port; it does not confirm delivery to peers. A write failure
//! goes straight to Replying. A callback failure also produces an error reply, but the
//! update is already committed and must not be retried.
//!
//! Advertising reports availability to catch-up. Listener notification invokes the
//! application's callback. Replying completes the API caller's promise.
//!
//! The flow owns replication group and listener claims until the reply attempt finishes,
//! including while an action is awaiting completion. It then returns both claims for
//! the component to release the slot explicitly. The write action owns transaction
//! locals; committed data moves into each later action. There is no mutable database
//! copy in the flow. Actions use local identity, the store, the membership snapshot,
//! the listener, and broadcast/catch-up outputs. See the driver for current exclusion
//! guarantees and fault handling.

use super::{
    flow_driver::{FlowEffect, FlowId, FlowOwnership, FlowProtocolError, FlowStep, protocol},
    listeners::ListenerDataChanges,
    publication::{CommittedPublication, ScopedPublication},
};
use crate::{
    api::{ApiError, GroupSchema, PublishReceipt, api_error::ApiExternalSnafu},
    runtime::{
        errors::{InboundDeliveryError, PublishChangesError},
        replies::ReplyOutcome,
    },
};
use flotsync_core::versions::UpdateId;
use kompact::prelude::KPromise;
use snafu::prelude::*;
use std::sync::Arc;

/// Actions executed by the component after the flow records its next waiting state.
pub enum PublicationEffect {
    /// Validate and commit one batch using the existing transaction helper.
    Write {
        /// Request scoped to one Flotsync replication group.
        scoped: ScopedPublication,
        /// Schema used to validate and apply the scoped mutations.
        schema: Arc<GroupSchema>,
    },
    /// Submit committed bytes to the broadcast port, then return the data to the flow.
    Broadcast(CommittedPublication),
    /// Advertise the submitted update's availability before notifying the listener.
    Advertise(CommittedPublication),
    /// Await the existing filtered listener callback with no open write transaction.
    NotifyListener(ListenerDataChanges),
    /// Attempt the API reply once, even if the caller dropped its receiver.
    Reply {
        /// Caller promise, moved out of the flow for this one reply attempt.
        promise: KPromise<Result<PublishReceipt, ApiError>>,
        /// Publication receipt or write/listener error to return to the caller.
        reply: Result<PublishReceipt, ApiError>,
    },
}

/// Action results accepted only when the flow is waiting for that action.
pub enum PublicationCompletion {
    /// Write success provides committed data; failure provides an error before commit.
    Written(Result<CommittedPublication, PublishChangesError>),
    /// Broadcast submission finished; availability and listener notification come next.
    Broadcasted(CommittedPublication),
    /// Availability was advertised to catch-up; the listener callback comes next.
    Advertised(CommittedPublication),
    /// Listener success or rejection after commit and submission; neither permits write retry.
    ListenerNotified(Result<(), InboundDeliveryError>),
    /// The reply attempt finished, whether delivered or rejected by a dropped receiver.
    Replied(ReplyOutcome),
}

impl PublicationCompletion {
    /// Identify the only phase allowed to accept this action's result.
    const fn phase(&self) -> PublicationPhase {
        match self {
            Self::Written(_) => PublicationPhase::Writing,
            Self::Broadcasted(_) => PublicationPhase::Broadcasting,
            Self::Advertised(_) => PublicationPhase::Advertising,
            Self::ListenerNotified(_) => PublicationPhase::NotifyingListener,
            Self::Replied(_) => PublicationPhase::Replying,
        }
    }
}

/// Whether publication failed before commit, failed at the callback, or succeeded.
#[derive(Debug, PartialEq, Eq)]
pub enum PublicationOutcome {
    /// Transaction service failed before producing a committed result.
    BeforeCommitFailed,
    /// The update was committed and submitted, but its listener rejected the notification.
    CommittedListenerFailed { update_id: UpdateId },
    /// The update was committed and submitted, and its listener notification succeeded.
    Published { update_id: UpdateId },
}

/// Finished publication result with resource claims ready for explicit slot release.
pub struct FinishedPublication {
    /// Publication outcome, independent of whether the reply receiver still exists.
    pub outcome: PublicationOutcome,
    /// Outcome of the one API reply attempt.
    pub reply: ReplyOutcome,
    /// Replication group and listener claims returned after the reply attempt finishes.
    pub ownership: FlowOwnership,
}

/// One publication's progress, resource ownership and reply promise.
pub struct PublicationFlow {
    /// Flow ID, kept here even after the finished result takes its ownership value.
    flow_id: FlowId,
    /// Current waiting phase and its data; the write transaction belongs to the write action.
    state: PublicationState,
    /// `Some` until Replying moves the promise into its action; `None` afterwards.
    promise: Option<KPromise<Result<PublishReceipt, ApiError>>>,
    /// `Some` until Finished returns the claims; `None` afterwards. Drop does not release the slot.
    ownership: Option<FlowOwnership>,
}

impl PublicationFlow {
    /// Start in Writing and move the scoped request and schema into the first action.
    pub fn new(
        ownership: FlowOwnership,
        scoped: ScopedPublication,
        schema: Arc<GroupSchema>,
        promise: KPromise<Result<PublishReceipt, ApiError>>,
    ) -> (Self, PublicationEffect) {
        debug_assert_eq!(ownership.group_id(), scoped.group_id());
        let flow = Self {
            flow_id: ownership.flow_id(),
            state: PublicationState::Writing,
            promise: Some(promise),
            ownership: Some(ownership),
        };
        (flow, PublicationEffect::Write { scoped, schema })
    }

    /// Check an action's flow ID and result, then record the next state and return its action.
    ///
    /// A wrong flow ID or phase leaves the state, promise and claims untouched.
    /// Write and callback failures proceed to Replying; protocol errors stop the owning slot.
    pub fn complete(
        &mut self,
        flow_id: FlowId,
        completion: PublicationCompletion,
    ) -> Result<FlowStep<PublicationEffect, FinishedPublication>, FlowProtocolError> {
        ensure!(
            flow_id == self.flow_id,
            protocol::WrongOwnerSnafu {
                expected: self.flow_id,
                received: flow_id
            }
        );
        let expected = self.state.phase();
        let received = completion.phase();
        if expected != received {
            return Err(self.unexpected_completion(received));
        }
        match completion {
            PublicationCompletion::Written(result) => self.after_write(result),
            PublicationCompletion::Broadcasted(committed) => {
                self.state = PublicationState::Advertising;
                Ok(self.continue_with(PublicationEffect::Advertise(committed)))
            }
            PublicationCompletion::Advertised(committed) => {
                let (receipt, row_changes) = committed.into_listener_data();
                let listener_data = ListenerDataChanges {
                    read_token: receipt.read_token.clone(),
                    row_changes,
                };
                self.state = PublicationState::NotifyingListener { receipt };
                Ok(self.continue_with(PublicationEffect::NotifyListener(listener_data)))
            }
            PublicationCompletion::ListenerNotified(result) => self.after_listener(result),
            PublicationCompletion::Replied(reply) => self.finish(reply),
        }
    }

    /// Submit committed data for broadcast, or reply with the write error before any submission.
    fn after_write(
        &mut self,
        result: Result<CommittedPublication, PublishChangesError>,
    ) -> Result<FlowStep<PublicationEffect, FinishedPublication>, FlowProtocolError> {
        match result {
            Ok(committed) => {
                self.state = PublicationState::Broadcasting;
                Ok(self.continue_with(PublicationEffect::Broadcast(committed)))
            }
            Err(error) => self.begin_reply(
                PublicationOutcome::BeforeCommitFailed,
                Err(ApiError::from_store_classification_source(error)),
            ),
        }
    }

    /// Prepare a receipt reply on callback success, or an error reply for the committed update.
    fn after_listener(
        &mut self,
        result: Result<(), InboundDeliveryError>,
    ) -> Result<FlowStep<PublicationEffect, FinishedPublication>, FlowProtocolError> {
        let state = std::mem::replace(&mut self.state, PublicationState::Finished);
        match state {
            PublicationState::NotifyingListener { receipt } => {
                let update_id = receipt.update_id;
                match result {
                    Ok(()) => {
                        self.begin_reply(PublicationOutcome::Published { update_id }, Ok(receipt))
                    }
                    Err(error) => {
                        let reply = Err(error).boxed().context(ApiExternalSnafu);
                        self.begin_reply(
                            PublicationOutcome::CommittedListenerFailed { update_id },
                            reply,
                        )
                    }
                }
            }
            state => {
                self.state = state;
                Err(self.unexpected_completion(PublicationPhase::NotifyingListener))
            }
        }
    }

    /// Move the promise into one reply action after recording the publication outcome.
    fn begin_reply(
        &mut self,
        outcome: PublicationOutcome,
        reply: Result<PublishReceipt, ApiError>,
    ) -> Result<FlowStep<PublicationEffect, FinishedPublication>, FlowProtocolError> {
        let promise = self.promise.take().context(protocol::MissingStateSnafu {
            flow_id: self.flow_id,
            field: "reply promise",
        })?;
        self.state = PublicationState::Replying { outcome };
        Ok(self.continue_with(PublicationEffect::Reply { promise, reply }))
    }

    /// Return the claims after the reply attempt finishes; a second reply result is invalid.
    fn finish(
        &mut self,
        reply: ReplyOutcome,
    ) -> Result<FlowStep<PublicationEffect, FinishedPublication>, FlowProtocolError> {
        let state = std::mem::replace(&mut self.state, PublicationState::Finished);
        match state {
            PublicationState::Replying { outcome } => {
                let ownership = self.ownership.take().context(protocol::MissingStateSnafu {
                    flow_id: self.flow_id,
                    field: "resource ownership",
                })?;
                Ok(FlowStep::Finished(FinishedPublication {
                    outcome,
                    reply,
                    ownership,
                }))
            }
            state => {
                self.state = state;
                Err(self.unexpected_completion(PublicationPhase::Replying))
            }
        }
    }

    /// Attach this flow's ID to its next action after recording the waiting state.
    fn continue_with(
        &self,
        effect: PublicationEffect,
    ) -> FlowStep<PublicationEffect, FinishedPublication> {
        FlowStep::Continue(FlowEffect {
            flow_id: self.flow_id,
            effect,
        })
    }

    /// Describe a result rejected by the current phase without moving the promise or claims.
    fn unexpected_completion(&self, received: PublicationPhase) -> FlowProtocolError {
        protocol::UnexpectedCompletionSnafu {
            flow_id: self.flow_id,
            expected: self.state.phase().name(),
            received: received.name(),
        }
        .build()
    }
}

/// Progress recorded while the component executes each action.
enum PublicationState {
    /// Waiting for the write transaction; the flow owns the promise and claims.
    Writing,
    /// Waiting for broadcast submission; the broadcast action owns the committed data.
    Broadcasting,
    /// Waiting for availability advertising; the catch-up action owns the committed data.
    Advertising,
    /// Waiting for the listener callback; the flow holds the receipt for the API reply.
    NotifyingListener { receipt: PublishReceipt },
    /// Waiting for the reply attempt; the action owns the promise and reply value.
    Replying { outcome: PublicationOutcome },
    /// Claims have moved into the finished result; no further action result is accepted.
    Finished,
}

impl PublicationState {
    /// Identify the current phase for result validation, including Finished.
    const fn phase(&self) -> PublicationPhase {
        match self {
            Self::Writing => PublicationPhase::Writing,
            Self::Broadcasting => PublicationPhase::Broadcasting,
            Self::Advertising => PublicationPhase::Advertising,
            Self::NotifyingListener { .. } => PublicationPhase::NotifyingListener,
            Self::Replying { .. } => PublicationPhase::Replying,
            Self::Finished => PublicationPhase::Finished,
        }
    }
}

/// Phase names used to check an action's result before moving data out of the state.
#[derive(PartialEq, Eq)]
enum PublicationPhase {
    /// Await transaction completion.
    Writing,
    /// Await committed broadcast submission.
    Broadcasting,
    /// Await committed availability submission.
    Advertising,
    /// Await listener completion.
    NotifyingListener,
    /// Await the API reply attempt.
    Replying,
    /// Finished; no further action result is accepted.
    Finished,
}

impl PublicationPhase {
    /// Name the phase in protocol errors without including its data.
    const fn name(self) -> &'static str {
        match self {
            Self::Writing => "Writing",
            Self::Broadcasting => "Broadcasting",
            Self::Advertising => "Advertising",
            Self::NotifyingListener => "NotifyingListener",
            Self::Replying => "Replying",
            Self::Finished => "Finished",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        api::{
            DatasetId,
            GroupReadToken,
            ListenerError,
            PublishChangesRequest,
            RowId,
            RowKey,
            RowMutation,
        },
        runtime::{
            component::{
                flow_driver::{FlowIdAllocator, FlowSlot, FlowState},
                publication::{committed_publication_for_test, scope_request},
            },
            errors::{inbound, publish},
            replies::complete_reply,
        },
    };
    use flotsync_core::{GroupId, versions::VersionVector};
    use kompact::prelude::{KFuture, promise};
    use std::{assert_matches, num::NonZeroUsize, time::Duration};
    use uuid::Uuid;

    /// Update represented by every synthetic committed result in these phase tests.
    const COMMITTED_UPDATE: UpdateId = UpdateId {
        node_index: 0,
        version: 1,
    };

    /// Start a pure publication with a synthetic one-group delete and no backend execution.
    fn admitted_publication(flow_ids: &mut FlowIdAllocator) -> TestPublication {
        let group_id = GroupId(Uuid::new_v4());
        let read_token = GroupReadToken::from_group_version(
            group_id,
            VersionVector::initial(NonZeroUsize::new(1).expect("one member")),
        );
        let scoped = scope_request(PublishChangesRequest {
            read_token,
            changes: vec![RowMutation::Delete {
                row_id: row_id(group_id),
            }],
        })
        .expect("one scoped mutation");
        let (promise, receiver) = promise();
        let mut slot = FlowSlot::Available;
        let effect = slot
            .admit(flow_ids, group_id, |ownership| {
                PublicationFlow::new(ownership, scoped, Arc::new(GroupSchema::default()), promise)
            })
            .expect("first admission");
        TestPublication {
            slot,
            initial: effect,
            receiver,
            group_id,
        }
    }

    /// Name the deleted row consistently across scoped input and synthetic committed output.
    fn row_id(group_id: GroupId) -> RowId {
        RowId::new(
            group_id,
            DatasetId::try_from_static("docs").expect("dataset id"),
            RowKey(Uuid::nil()),
        )
    }

    /// Fabricate exactly one committed update, independently of any store implementation.
    fn committed(group_id: GroupId) -> CommittedPublication {
        let update_id = COMMITTED_UPDATE;
        let versions = VersionVector::initial(NonZeroUsize::new(1).expect("one member"))
            .with_update_applied(update_id);
        let receipt = PublishReceipt {
            update_id,
            read_token: GroupReadToken::from_group_version(group_id, versions),
        };
        committed_publication_for_test(
            group_id,
            receipt,
            vec![crate::api::RowChange::ordinary_delete(row_id(group_id))],
        )
    }

    /// Pass a valid action result to the flow and require another action before slot release.
    fn next(
        slot: &mut FlowSlot<PublicationFlow>,
        flow_id: FlowId,
        completion: PublicationCompletion,
    ) -> PublicationEffect {
        let step = slot
            .transition(|flow| flow.complete(flow_id, completion))
            .expect("legal transition");
        match step {
            FlowStep::Continue(effect) => {
                assert_eq!(effect.flow_id, flow_id);
                effect.effect
            }
            FlowStep::Finished(_) => panic!("operation must still own its claims"),
        }
    }

    /// Check that broadcast and availability precede the callback and keep the committed data.
    fn to_listener(slot: &mut FlowSlot<PublicationFlow>, flow_id: FlowId, group_id: GroupId) {
        let effect = next(
            slot,
            flow_id,
            PublicationCompletion::Written(Ok(committed(group_id))),
        );
        let PublicationEffect::Broadcast(committed) = effect else {
            panic!("broadcast follows commit")
        };
        assert_eq!(committed.group_id(), group_id);
        assert_eq!(committed.payload().as_ref(), b"committed update");
        let effect = next(slot, flow_id, PublicationCompletion::Broadcasted(committed));
        let PublicationEffect::Advertise(committed) = effect else {
            panic!("availability follows broadcast")
        };
        assert_eq!(committed.update_id(), COMMITTED_UPDATE);
        let effect = next(slot, flow_id, PublicationCompletion::Advertised(committed));
        let PublicationEffect::NotifyListener(listener_data) = effect else {
            panic!("listener follows availability")
        };
        assert_eq!(listener_data.read_token.group_id(), group_id);
        assert_eq!(listener_data.row_changes.len(), 1);
    }

    /// Attempt the promise reply once and report whether the receiver accepted it.
    fn reply_completion(effect: PublicationEffect) -> PublicationCompletion {
        match effect {
            PublicationEffect::Reply { promise, reply } => {
                PublicationCompletion::Replied(complete_reply(promise, reply))
            }
            _ => panic!("expected the terminal reply effect"),
        }
    }

    /// Require a finished result; the test releases the slot separately.
    fn finish(
        slot: &mut FlowSlot<PublicationFlow>,
        flow_id: FlowId,
        completion: PublicationCompletion,
    ) -> FinishedPublication {
        match slot
            .transition(|flow| flow.complete(flow_id, completion))
            .expect("reply completion")
        {
            FlowStep::Finished(finished) => finished,
            FlowStep::Continue(_) => panic!("reply must end the operation"),
        }
    }

    /// Fixture containing the slot and first action so tests can complete each step separately.
    struct TestPublication {
        /// Owns the flow while tests provide individual action results.
        slot: FlowSlot<PublicationFlow>,
        /// First action returned after the flow records Writing.
        initial: FlowEffect<PublicationEffect>,
        /// Caller receiver, optionally dropped during the test.
        receiver: KFuture<Result<PublishReceipt, ApiError>>,
        /// Flotsync replication group shared by the request and synthetic committed result.
        group_id: GroupId,
    }

    #[test]
    fn success_orders_effects_and_restores_only_after_reply() {
        let mut flow_ids = FlowIdAllocator::default();
        let TestPublication {
            mut slot,
            initial,
            receiver,
            group_id,
        } = admitted_publication(&mut flow_ids);
        assert!(matches!(initial.effect, PublicationEffect::Write { .. }));
        to_listener(&mut slot, initial.flow_id, group_id);
        let denied: Result<FlowEffect<()>, _> = slot.admit(&mut flow_ids, group_id, |_| {
            panic!("occupied admission must not construct a flow")
        });
        assert_matches!(denied.err(), Some(FlowProtocolError::Occupied { owner, .. }) if owner == initial.flow_id);
        let reply = next(
            &mut slot,
            initial.flow_id,
            PublicationCompletion::ListenerNotified(Ok(())),
        );
        assert!(
            matches!(&slot, FlowSlot::Occupied { flow, .. } if flow.ownership.as_ref().expect("flow owns claims").group_id() == group_id)
        );
        let finished = finish(&mut slot, initial.flow_id, reply_completion(reply));
        assert_eq!(
            finished.outcome,
            PublicationOutcome::Published {
                update_id: COMMITTED_UPDATE
            }
        );
        assert_eq!(finished.reply, ReplyOutcome::Delivered);
        let receipt = receiver
            .wait_timeout(Duration::from_secs(1))
            .expect("reply delivered")
            .expect("publication succeeded");
        assert_eq!(receipt.update_id.version, 1);
        assert!(matches!(&slot, FlowSlot::Occupied { .. }));
        slot.restore(finished.ownership)
            .expect("explicit restoration");
        assert!(matches!(slot, FlowSlot::Available));
    }

    #[test]
    fn write_failure_skips_broadcast_and_listener() {
        let mut flow_ids = FlowIdAllocator::default();
        let TestPublication {
            mut slot,
            initial,
            receiver,
            group_id,
        } = admitted_publication(&mut flow_ids);
        let error = publish::ExhaustedUpdateIdsSnafu { group_id }.build();
        let reply = next(
            &mut slot,
            initial.flow_id,
            PublicationCompletion::Written(Err(error)),
        );
        let finished = finish(&mut slot, initial.flow_id, reply_completion(reply));
        assert_eq!(finished.outcome, PublicationOutcome::BeforeCommitFailed);
        assert_matches!(
            receiver
                .wait_timeout(Duration::from_secs(1))
                .expect("error reply"),
            Err(ApiError::ApiExternal { .. })
        );
        slot.restore(finished.ownership)
            .expect("failed transaction restores claims");
    }

    #[test]
    fn listener_failure_preserves_committed_outcome_and_api_source() {
        let mut flow_ids = FlowIdAllocator::default();
        let TestPublication {
            mut slot,
            initial,
            receiver,
            group_id,
        } = admitted_publication(&mut flow_ids);
        to_listener(&mut slot, initial.flow_id, group_id);
        let error = Err::<(), _>(ListenerError::Rejected {
            message: "rejected".to_owned(),
        })
        .context(inbound::NotifyListenerSnafu)
        .expect_err("listener rejection");
        let reply = next(
            &mut slot,
            initial.flow_id,
            PublicationCompletion::ListenerNotified(Err(error)),
        );
        let finished = finish(&mut slot, initial.flow_id, reply_completion(reply));
        assert_eq!(
            finished.outcome,
            PublicationOutcome::CommittedListenerFailed {
                update_id: COMMITTED_UPDATE
            }
        );
        let error = receiver
            .wait_timeout(Duration::from_secs(1))
            .expect("reply delivered")
            .expect_err("listener callback failed");
        let ApiError::ApiExternal { source } = error else {
            panic!("unchanged API wrapper")
        };
        assert_matches!(
            source.downcast_ref::<InboundDeliveryError>(),
            Some(InboundDeliveryError::NotifyListener {
                source: ListenerError::Rejected { .. }
            })
        );
        slot.restore(finished.ownership)
            .expect("commit remains valid after rejection");
    }

    #[test]
    fn dropped_receiver_still_finishes_all_effects_and_returns_claims() {
        let mut flow_ids = FlowIdAllocator::default();
        let TestPublication {
            mut slot,
            initial,
            receiver,
            group_id,
        } = admitted_publication(&mut flow_ids);
        drop(receiver);
        to_listener(&mut slot, initial.flow_id, group_id);
        let reply = next(
            &mut slot,
            initial.flow_id,
            PublicationCompletion::ListenerNotified(Ok(())),
        );
        let finished = finish(&mut slot, initial.flow_id, reply_completion(reply));
        assert_eq!(finished.reply, ReplyOutcome::ReceiverDropped);
        assert_matches!(finished.outcome, PublicationOutcome::Published { .. });
        slot.restore(finished.ownership)
            .expect("dropped receiver still allows claims to be released");
    }

    #[test]
    fn terminal_duplicate_prevents_restoration_even_with_returned_claims() {
        let mut flow_ids = FlowIdAllocator::default();
        let TestPublication {
            mut slot,
            initial,
            receiver: _receiver,
            group_id,
        } = admitted_publication(&mut flow_ids);
        to_listener(&mut slot, initial.flow_id, group_id);
        let reply = next(
            &mut slot,
            initial.flow_id,
            PublicationCompletion::ListenerNotified(Ok(())),
        );
        let finished = finish(&mut slot, initial.flow_id, reply_completion(reply));
        let duplicate = slot.transition(|flow| {
            flow.complete(
                initial.flow_id,
                PublicationCompletion::Replied(ReplyOutcome::Delivered),
            )
        });
        assert_matches!(
            duplicate.err(),
            Some(FlowProtocolError::UnexpectedCompletion {
                expected: "Finished",
                received: "Replying",
                ..
            })
        );
        assert_matches!(
            slot.restore(finished.ownership),
            Err(FlowProtocolError::UnexpectedCompletion { .. })
        );
        let saved_claims = match &slot {
            FlowSlot::Occupied {
                flow,
                state: FlowState::Faulted(fault),
                ..
            } => {
                assert!(flow.ownership.is_none());
                assert_matches!(
                    fault.error,
                    FlowProtocolError::UnexpectedCompletion {
                        expected: "Finished",
                        received: "Replying",
                        ..
                    }
                );
                &fault.returned_claims
            }
            _ => panic!("failed release saves the returned claims"),
        };
        assert_eq!(saved_claims.len(), 1);
        assert_eq!(saved_claims[0].flow_id(), initial.flow_id);
        assert_eq!(saved_claims[0].group_id(), group_id);
        slot.fault(protocol::UnavailableSnafu.build());
        assert!(matches!(&slot, FlowSlot::Occupied {
            state: FlowState::Faulted(fault), ..
        } if fault.returned_claims.len() == 1 && matches!(fault.error, FlowProtocolError::UnexpectedCompletion { .. })));
    }

    #[test]
    fn impossible_completion_faults_without_releasing_ownership() {
        let mut flow_ids = FlowIdAllocator::default();
        let TestPublication {
            mut slot,
            initial,
            receiver: _receiver,
            group_id,
        } = admitted_publication(&mut flow_ids);
        let result = slot.transition(|flow| {
            flow.complete(
                initial.flow_id,
                PublicationCompletion::Replied(ReplyOutcome::Delivered),
            )
        });
        let error = result.err().expect("wrong phase is internal failure");
        assert_matches!(error, FlowProtocolError::UnexpectedCompletion { expected: "Writing", received: "Replying", location, .. } if location.file().ends_with("publication_flow.rs"));
        assert!(
            matches!(&slot, FlowSlot::Occupied { flow, state: FlowState::Faulted(_), .. } if flow.ownership.as_ref().expect("faulted flow owns claims").group_id() == group_id && flow.promise.is_some())
        );
        let retry = slot.transition(|flow| {
            flow.complete(
                initial.flow_id,
                PublicationCompletion::Written(Ok(committed(group_id))),
            )
        });
        assert_matches!(
            retry.err(),
            Some(FlowProtocolError::UnexpectedCompletion { .. })
        );
    }

    #[test]
    fn wrong_operation_and_duplicate_completion_are_rejected() {
        let mut flow_ids = FlowIdAllocator::default();
        let TestPublication {
            mut slot,
            initial,
            receiver: _receiver,
            group_id,
        } = admitted_publication(&mut flow_ids);
        let TestPublication {
            initial: other,
            slot: _other_slot,
            receiver: _other_receiver,
            ..
        } = admitted_publication(&mut flow_ids);
        let result = slot.transition(|flow| {
            flow.complete(
                other.flow_id,
                PublicationCompletion::Written(Ok(committed(group_id))),
            )
        });
        assert_matches!(result.err(), Some(FlowProtocolError::WrongOwner { expected, received, .. }) if expected == initial.flow_id && received == other.flow_id);

        let TestPublication {
            mut slot,
            initial,
            receiver: _receiver,
            group_id,
        } = admitted_publication(&mut flow_ids);
        let effect = next(
            &mut slot,
            initial.flow_id,
            PublicationCompletion::Written(Ok(committed(group_id))),
        );
        assert!(matches!(effect, PublicationEffect::Broadcast(_)));
        let duplicate = slot.transition(|flow| {
            flow.complete(
                initial.flow_id,
                PublicationCompletion::Written(Ok(committed(group_id))),
            )
        });
        assert_matches!(
            duplicate.err(),
            Some(FlowProtocolError::UnexpectedCompletion {
                expected: "Broadcasting",
                received: "Writing",
                ..
            })
        );
        assert!(matches!(
            slot,
            FlowSlot::Occupied {
                state: FlowState::Faulted(_),
                ..
            }
        ));
    }
}
