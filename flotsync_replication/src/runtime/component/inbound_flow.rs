//! Composes inbound transactions, catch-up observations and ordered listener delivery.
//!
//! The root owns the authenticated envelope and group/listener claims through all
//! transactions, callbacks and catch-up submissions. Each `ApplyFlow` covers one input's
//! transaction and callbacks. A batch retains its cursor and accumulated observations
//! around these children; it never starts another transaction during a callback.
//! Transaction bodies remain in `inbound_support`, including atomic ready-chain
//! application. A committed causal wait ends its child without waiting for new input.
//!
//! Single input: apply -> advertise availability/needs -> callbacks -> finish.
//! Batch: (apply -> accumulate -> callbacks)* -> advertise availability/needs -> finish.
//! Batch failure follows the same final observation step, preserving committed prefixes.
//! No child takes a new resource claim or receives component/service access.

use super::{
    flow_driver::{FlowEffect, FlowId, FlowOwnership, FlowProtocolError, FlowStep, protocol},
    inbound_support::{InboundDeliveryContext, InboundUpdateOrigin, InboundUpdateOutcome},
    listener_flow::{DeliveringListener, ListenerDataChange, ListenerFlow, ListenerStep},
};
use crate::{
    api::ListenerError,
    codecs::messages::{UpdateBatchMessage, UpdateMessage, UpdateRangeMessage},
    runtime::errors::{InboundDeliveryError, inbound},
};
use flotsync_core::{GroupId, MemberIdentity};
use snafu::{IntoError, prelude::*};

/// Decoded input whose sender authority is checked by the transaction service.
pub enum InboundRequest {
    /// Direct producer update.
    Single {
        /// Authenticated sender and source-specific producer validation policy.
        origin: InboundUpdateOrigin,
        /// Decoded input transferred to the existing transaction service.
        message: UpdateMessage,
    },
    /// Catch-up updates forwarded by one authenticated group member.
    Batch {
        /// Authenticated forwarder used for each input's sender validation.
        sender: MemberIdentity,
        /// Ordered inputs; each receives its own transaction and completes its callbacks first.
        message: UpdateBatchMessage,
    },
}

impl InboundRequest {
    /// Identify the group whose sequencing claims must be acquired before execution.
    pub fn group_id(&self) -> GroupId {
        match self {
            Self::Single { message, .. } => message.group_id,
            Self::Batch { message, .. } => message.group_id,
        }
    }
}

/// Transaction, catch-up advertisement or listener action requested by an inbound flow.
///
/// The flow installs the matching waiting state before returning an effect.
#[derive(Debug)]
pub enum InboundEffect {
    /// Resolve the schema and run one complete causal-chain transaction.
    Apply {
        /// Source-specific sender authority required by the transaction service.
        origin: InboundUpdateOrigin,
        /// Owned input; no copy is retained in the waiting child.
        message: UpdateMessage,
    },
    /// Advertise available versions and missing dependency ranges to the catch-up manager.
    ///
    /// Availability is submitted before needs; empty vectors produce no manager messages.
    Advertise {
        /// Group claimed by the root for the whole operation.
        group_id: GroupId,
        /// Committed availability and dependency evidence, preserving its order and bounds.
        observations: CatchUpObservations,
    },
    /// Invoke one callback; the effect owns the event and its provider.
    NotifyListener(ListenerDataChange),
}

/// Typed results of the root's current effect.
pub enum InboundCompletion {
    /// A committed transaction result, or a classified transaction-service failure.
    Applied(Result<InboundUpdateOutcome, InboundDeliveryError>),
    /// Availability and dependency needs have been submitted to the catch-up manager.
    ///
    /// Submission does not imply that the manager has processed the messages.
    Advertised,
    /// The callback accepted or rejected its event, independently of provider exhaustion.
    ListenerNotified(Result<(), ListenerError>),
}

impl InboundCompletion {
    /// Identify the waiting state required to accept this completion.
    fn phase(&self) -> InboundPhase {
        match self {
            Self::Applied(_) => InboundPhase::Applying,
            Self::Advertised => InboundPhase::Advertising,
            Self::ListenerNotified(_) => InboundPhase::NotifyingListener,
        }
    }
}

/// Outcome of inbound application and listener delivery for a single input or batch.
///
/// Failures distinguish a transaction that did not commit from a rejected callback
/// for a committed input. Earlier batch inputs may have committed in either case.
#[derive(Debug)]
pub enum InboundOutcome {
    /// All inputs completed, including duplicates, final-cut exclusions and stored causal waits.
    Completed,
    /// The current transaction service failed; any earlier batch commits remain applied.
    TransactionFailed(InboundDeliveryError),
    /// A committed input's callback failed; its observations still precede terminal release.
    CommittedListenerFailed(InboundDeliveryError),
}

impl InboundOutcome {
    /// Convert this outcome to success or the original transaction/listener error.
    ///
    /// `Completed` returns `Ok(())`; both failure variants return their contained error
    /// without changing its classification or source chain.
    pub fn into_result(self) -> Result<(), InboundDeliveryError> {
        match self {
            Self::Completed => Ok(()),
            Self::TransactionFailed(error) | Self::CommittedListenerFailed(error) => Err(error),
        }
    }
}

/// Completed inbound outcome, authenticated delivery context and resource claims.
///
/// Transactions, callbacks and catch-up submissions have finished. The caller may
/// now restore the returned claims to the operation slot.
pub struct FinishedInbound {
    /// Authenticated envelope for production diagnostics; absent for direct test requests.
    pub context: Option<InboundDeliveryContext>,
    /// Classified application/callback outcome, distinct from internal protocol faults.
    pub outcome: InboundOutcome,
    /// Group and listener ownership retained through callbacks and final observations.
    pub ownership: FlowOwnership,
}

/// Available versions and missing dependency ranges reported by committed inbound transactions.
#[derive(Debug, Default)]
pub struct CatchUpObservations {
    /// Versions available in the update log, submitted before dependency needs.
    pub available: Vec<UpdateRangeMessage>,
    /// Missing dependency ranges, retaining the transaction service's ordering and bounds.
    pub needed: Vec<UpdateRangeMessage>,
}

impl CatchUpObservations {
    /// Append one committed child's evidence without changing its order or deduplicating it.
    fn extend(&mut self, observations: Self) {
        self.available.extend(observations.available);
        self.needed.extend(observations.needed);
    }
}

/// Root operation retaining ownership around a single child or the complete batch.
pub struct InboundFlow {
    /// Component-scoped ID, retained after claims move into the terminal result.
    flow_id: FlowId,
    /// Logical group scope used for final observation effects.
    group_id: GroupId,
    /// Authenticated delivery context retained until the flow finishes.
    /// `Some` holds the production envelope; `None` means a fixture request or completed transfer.
    context: Option<InboundDeliveryContext>,
    /// Group and listener claims retained until the flow finishes.
    /// `Some` holds the claims; `None` means they have been returned. Drop never releases the slot.
    ownership: Option<FlowOwnership>,
    /// Single/batch child or final flush, retained across each suspended effect.
    state: InboundState,
}

impl InboundFlow {
    /// Create an inbound flow for an already claimed group and return its first effect.
    ///
    /// A non-empty request starts its first transaction; an empty batch starts its final
    /// catch-up submission. The matching waiting state is installed before returning.
    pub fn new(
        ownership: FlowOwnership,
        context: Option<InboundDeliveryContext>,
        request: InboundRequest,
    ) -> (Self, InboundEffect) {
        debug_assert_eq!(ownership.group_id(), request.group_id());
        let group_id = ownership.group_id();
        let (state, effect) = match request {
            InboundRequest::Single { origin, message } => {
                let (apply, effect) = ApplyFlow::new(origin, message);
                (InboundState::Single(apply), effect)
            }
            InboundRequest::Batch { sender, message } => {
                let (batch, first) = BatchApplyFlow::new(sender, message);
                match first {
                    BatchStep::Continue(effect) => (InboundState::Batch(batch), effect),
                    BatchStep::Finished {
                        observations,
                        outcome,
                    } => (
                        InboundState::AdvertisingFinal(outcome),
                        InboundEffect::Advertise {
                            group_id,
                            observations,
                        },
                    ),
                }
            }
        };
        (
            Self {
                flow_id: ownership.flow_id(),
                group_id,
                context,
                ownership: Some(ownership),
                state,
            },
            effect,
        )
    }

    /// Identify the root for component-scoped completion validation.
    pub const fn flow_id(&self) -> FlowId {
        self.flow_id
    }

    /// Advance inbound application after the current effect completes.
    ///
    /// Returns the next effect or the completed outcome and claims. Identity/phase
    /// mismatches return a protocol error before changing the flow's state.
    pub fn complete(
        &mut self,
        flow_id: FlowId,
        completion: InboundCompletion,
    ) -> Result<FlowStep<InboundEffect, FinishedInbound>, FlowProtocolError> {
        ensure!(
            flow_id == self.flow_id,
            protocol::WrongOwnerSnafu {
                expected: self.flow_id,
                received: flow_id,
            }
        );
        self.state.phase().validate(flow_id, completion.phase())?;
        match &mut self.state {
            InboundState::Single(apply) => {
                let step = apply.complete(flow_id, completion)?;
                match step {
                    ApplyStep::Committed(observations) => Ok(self.advertise(observations)),
                    ApplyStep::Continue(effect) => Ok(self.continue_with(effect)),
                    ApplyStep::Finished(outcome) => self.finish(outcome),
                }
            }
            InboundState::Batch(batch) => {
                let step = batch.complete(flow_id, completion)?;
                match step {
                    BatchStep::Continue(effect) => Ok(self.continue_with(effect)),
                    BatchStep::Finished {
                        observations,
                        outcome,
                    } => {
                        self.state = InboundState::AdvertisingFinal(outcome);
                        Ok(self.advertise(observations))
                    }
                }
            }
            InboundState::AdvertisingFinal(_) => {
                let state = std::mem::replace(&mut self.state, InboundState::Finished);
                match state {
                    InboundState::AdvertisingFinal(outcome) => self.finish(outcome),
                    _ => protocol::MissingStateSnafu {
                        flow_id,
                        field: "final observation outcome",
                    }
                    .fail(),
                }
            }
            InboundState::Finished => protocol::MissingStateSnafu {
                flow_id,
                field: "active inbound operation",
            }
            .fail(),
        }
    }

    /// Finish the inbound flow, returning its outcome, context and resource claims.
    ///
    /// Called after transactions, callbacks and catch-up submissions finish. Returns
    /// a protocol error if the flow no longer holds its resource claims.
    fn finish(
        &mut self,
        outcome: InboundOutcome,
    ) -> Result<FlowStep<InboundEffect, FinishedInbound>, FlowProtocolError> {
        let ownership = self.ownership.take().context(protocol::MissingStateSnafu {
            flow_id: self.flow_id,
            field: "resource ownership",
        })?;
        self.state = InboundState::Finished;
        Ok(FlowStep::Finished(FinishedInbound {
            context: self.context.take(),
            outcome,
            ownership,
        }))
    }

    /// Attach identity to an effect whose waiting state has already been installed.
    fn continue_with(&self, effect: InboundEffect) -> FlowStep<InboundEffect, FinishedInbound> {
        FlowStep::Continue(FlowEffect {
            flow_id: self.flow_id,
            effect,
        })
    }

    /// Submit observations with the root's claimed group, preserving its installed continuation.
    fn advertise(
        &self,
        observations: CatchUpObservations,
    ) -> FlowStep<InboundEffect, FinishedInbound> {
        self.continue_with(InboundEffect::Advertise {
            group_id: self.group_id,
            observations,
        })
    }
}

/// Whole-batch state retained while one item owns its transaction or callback effect.
struct BatchApplyFlow {
    /// Authenticated forwarder, cloned only to construct each owned transaction input.
    sender: MemberIdentity,
    /// Inputs not yet attempted; committed prefixes are never put back into this cursor.
    remaining: std::vec::IntoIter<UpdateMessage>,
    /// Evidence from committed children, moved into exactly one terminal observation effect.
    observations: CatchUpObservations,
    /// Current child, or terminal state once the parent has taken the accumulated evidence.
    state: BatchState,
}

impl BatchApplyFlow {
    /// Own the input cursor and install the first child, or finish an empty batch without a transaction.
    fn new(sender: MemberIdentity, message: UpdateBatchMessage) -> (Self, BatchStep) {
        let mut remaining = message.updates.into_iter();
        let observations = CatchUpObservations::default();
        let (state, first) = match remaining.next() {
            Some(message) => {
                let (apply, effect) = Self::start_input(&sender, message);
                (BatchState::Running(apply), BatchStep::Continue(effect))
            }
            None => (
                BatchState::Finished,
                BatchStep::Finished {
                    observations: CatchUpObservations::default(),
                    outcome: InboundOutcome::Completed,
                },
            ),
        };
        let batch = Self {
            sender,
            remaining,
            observations,
            state,
        };
        (batch, first)
    }

    /// Identify the completion expected by the currently owned child.
    fn phase(&self) -> InboundPhase {
        match &self.state {
            BatchState::Running(apply) => apply.state.phase(),
            BatchState::Finished => InboundPhase::Finished,
        }
    }

    /// Advance only the current child; callbacks must finish before another input is started.
    fn complete(
        &mut self,
        flow_id: FlowId,
        completion: InboundCompletion,
    ) -> Result<BatchStep, FlowProtocolError> {
        self.phase().validate(flow_id, completion.phase())?;
        match &mut self.state {
            BatchState::Running(apply) => {
                let step = apply.complete(flow_id, completion)?;
                self.after_child_step(flow_id, step)
            }
            BatchState::Finished => protocol::MissingStateSnafu {
                flow_id,
                field: "current batch apply child",
            }
            .fail(),
        }
    }

    /// Process a child's progress into the next batch effect or completed batch outcome.
    ///
    /// Committed evidence is accumulated before callbacks resume. A completed child
    /// starts the next input on success or ends the batch on failure. Returns a protocol
    /// error if committed evidence has no corresponding active child.
    fn after_child_step(
        &mut self,
        flow_id: FlowId,
        step: ApplyStep,
    ) -> Result<BatchStep, FlowProtocolError> {
        match step {
            ApplyStep::Committed(observations) => {
                self.observations.extend(observations);
                match &mut self.state {
                    BatchState::Running(apply) => {
                        let next = apply.resume_listener(flow_id)?;
                        // Resumption yields a callback or terminal outcome, never another commit.
                        self.after_child_step(flow_id, next)
                    }
                    BatchState::Finished => protocol::MissingStateSnafu {
                        flow_id,
                        field: "committed batch apply child",
                    }
                    .fail(),
                }
            }
            ApplyStep::Continue(effect) => Ok(BatchStep::Continue(effect)),
            ApplyStep::Finished(outcome) => {
                if matches!(outcome, InboundOutcome::Completed) {
                    Ok(self.next_input())
                } else {
                    Ok(self.finish(outcome))
                }
            }
        }
    }

    /// Consume the next input and return its transaction effect, or finish an exhausted batch.
    ///
    /// Called only after the previous child completes successfully.
    fn next_input(&mut self) -> BatchStep {
        match self.remaining.next() {
            Some(message) => {
                let (apply, effect) = Self::start_input(&self.sender, message);
                self.state = BatchState::Running(apply);
                BatchStep::Continue(effect)
            }
            None => self.finish(InboundOutcome::Completed),
        }
    }

    /// Start one transaction child using the batch member as its authenticated forwarder.
    fn start_input(sender: &MemberIdentity, message: UpdateMessage) -> (ApplyFlow, InboundEffect) {
        let origin = InboundUpdateOrigin::Forwarder {
            sender: sender.clone(),
        };
        ApplyFlow::new(origin, message)
    }

    /// Finish the batch and return its outcome and accumulated catch-up evidence to the root.
    ///
    /// Remaining inputs are abandoned; committed inputs are never replayed.
    fn finish(&mut self, outcome: InboundOutcome) -> BatchStep {
        self.state = BatchState::Finished;
        BatchStep::Finished {
            observations: std::mem::take(&mut self.observations),
            outcome,
        }
    }
}

/// One input's transaction and listener machine, reused by single and batch parents.
///
/// The input/origin transfer to its transaction effect at admission. After commit,
/// this child retains the listener continuation while its parent submits or collects
/// observations. It resumes callbacks only when the parent explicitly permits it.
struct ApplyFlow {
    /// Transaction wait, committed listener continuation, in-flight callback or terminal state.
    state: ApplyState,
}

impl ApplyFlow {
    /// Create a child waiting for its input's transaction and return that transaction effect.
    fn new(origin: InboundUpdateOrigin, message: UpdateMessage) -> (Self, InboundEffect) {
        (
            Self {
                state: ApplyState::Applying,
            },
            InboundEffect::Apply { origin, message },
        )
    }

    /// Advance this input after its transaction, parent permission or listener callback completes.
    ///
    /// A successful transaction returns `Committed` evidence while retaining callbacks.
    /// `Advertised` resumes callbacks after the single-input parent submits that evidence;
    /// a batch parent instead calls `resume_listener` after accumulating it. Callback
    /// completion returns the next callback effect or the final input outcome. Transaction
    /// failures and callback rejections return distinct `Finished` outcomes.
    ///
    /// Returns a protocol error for a completion that does not match the current phase,
    /// leaving the child's state intact.
    fn complete(
        &mut self,
        flow_id: FlowId,
        completion: InboundCompletion,
    ) -> Result<ApplyStep, FlowProtocolError> {
        self.state.phase().validate(flow_id, completion.phase())?;
        match completion {
            InboundCompletion::Advertised => self.resume_listener(flow_id),
            completion => {
                let state = std::mem::replace(&mut self.state, ApplyState::Finished);
                match (state, completion) {
                    (ApplyState::Applying, InboundCompletion::Applied(result)) => match result {
                        Ok(outcome) => {
                            self.state = ApplyState::AwaitingParent(ListenerFlow::updates(
                                outcome.event_batches,
                                (),
                            ));
                            Ok(ApplyStep::Committed(CatchUpObservations {
                                available: outcome.observed_available,
                                needed: outcome.needed_ranges,
                            }))
                        }
                        Err(error) => Ok(ApplyStep::Finished(InboundOutcome::TransactionFailed(
                            error,
                        ))),
                    },
                    (
                        ApplyState::Listening(listener),
                        InboundCompletion::ListenerNotified(result),
                    ) => Ok(self.after_listener_step(listener.complete(result))),
                    (state, completion) => {
                        self.state = state;
                        protocol::UnexpectedCompletionSnafu {
                            flow_id,
                            expected: self.state.phase().name(),
                            received: completion.phase().name(),
                        }
                        .fail()
                    }
                }
            }
        }
    }

    /// Resume committed callbacks after the parent has taken responsibility for the observations.
    fn resume_listener(&mut self, flow_id: FlowId) -> Result<ApplyStep, FlowProtocolError> {
        self.state
            .phase()
            .validate(flow_id, InboundPhase::Advertising)?;
        let state = std::mem::replace(&mut self.state, ApplyState::Finished);
        match state {
            ApplyState::AwaitingParent(listener) => Ok(self.after_listener_step(listener.start())),
            state => {
                self.state = state;
                protocol::MissingStateSnafu {
                    flow_id,
                    field: "committed listener continuation",
                }
                .fail()
            }
        }
    }

    /// Record callback waiting state or classify its terminal result before returning to the parent.
    fn after_listener_step(&mut self, step: ListenerStep<()>) -> ApplyStep {
        match step {
            ListenerStep::Deliver { event, waiting } => {
                self.state = ApplyState::Listening(waiting);
                ApplyStep::Continue(InboundEffect::NotifyListener(event))
            }
            ListenerStep::Accepted(()) => {
                self.state = ApplyState::Finished;
                ApplyStep::Finished(InboundOutcome::Completed)
            }
            ListenerStep::Failed {
                continuation: (),
                error,
            } => {
                self.state = ApplyState::Finished;
                let error = inbound::NotifyListenerSnafu.into_error(error);
                ApplyStep::Finished(InboundOutcome::CommittedListenerFailed(error))
            }
        }
    }
}

/// Current input child or completed state of an inbound batch.
enum BatchState {
    /// Active transaction or listener child for the current input.
    Running(ApplyFlow),
    /// The batch has returned its observations; it cannot accept more completions.
    Finished,
}

/// Per-item progress exposed to the parent that controls observation timing.
#[derive(Debug)]
enum ApplyStep {
    /// Catch-up evidence produced by a committed transaction, with callbacks still suspended.
    Committed(CatchUpObservations),
    /// Next listener callback effect, with its waiting state installed in the child.
    Continue(InboundEffect),
    /// Completed input outcome distinguishing transaction failure from committed callback rejection.
    Finished(InboundOutcome),
}

/// Batch progress exposed to the root that owns terminal observation submission.
#[derive(Debug)]
enum BatchStep {
    /// Next transaction or listener effect for the active child.
    Continue(InboundEffect),
    /// Completed batch outcome and committed evidence for the root's final catch-up submission.
    Finished {
        /// Evidence from every committed input, including an input whose callback failed.
        observations: CatchUpObservations,
        /// Terminal success or original transaction/listener failure classification.
        outcome: InboundOutcome,
    },
}

/// Stages of one inbound input, with the listener data retained after its transaction commits.
///
/// The parent controls when committed callbacks resume: a single-input parent first
/// advertises the evidence, while a batch parent accumulates it for final submission.
enum ApplyState {
    /// Transaction in progress; the input and origin belong to its effect.
    Applying,
    /// Committed listener work waiting for its parent to submit or accumulate the evidence.
    AwaitingParent(ListenerFlow<()>),
    /// Callback in progress, retaining the remaining listener work and its continuation.
    /// The callback effect owns the current event and provider.
    Listening(DeliveringListener<()>),
    /// This child has completed or failed and cannot accept another completion.
    Finished,
}

impl ApplyState {
    /// Identify the legal next completion without moving any continuation.
    fn phase(&self) -> InboundPhase {
        match self {
            Self::Applying => InboundPhase::Applying,
            Self::AwaitingParent(_) => InboundPhase::Advertising,
            Self::Listening(_) => InboundPhase::NotifyingListener,
            Self::Finished => InboundPhase::Finished,
        }
    }
}

/// Stages of inbound application: a single input, an active batch or final catch-up submission.
enum InboundState {
    /// Single-input child whose catch-up evidence is submitted before its callbacks.
    Single(ApplyFlow),
    /// The batch parent owns its current child and accumulates committed observations.
    Batch(BatchApplyFlow),
    /// Completed batch outcome waiting for the final catch-up submission to finish.
    AdvertisingFinal(InboundOutcome),
    /// Claims moved to the terminal result; no further completion is accepted.
    Finished,
}

impl InboundState {
    /// Identify the legal completion of the active child or terminal observation effect.
    fn phase(&self) -> InboundPhase {
        match self {
            Self::Single(apply) => apply.state.phase(),
            Self::Batch(batch) => batch.phase(),
            Self::AdvertisingFinal(_) => InboundPhase::Advertising,
            Self::Finished => InboundPhase::Finished,
        }
    }
}

/// Completion phase expected by an inbound flow or its current child.
///
/// Enum values control protocol validation; their names are rendered only for diagnostics.
#[derive(Clone, Copy, PartialEq, Eq)]
enum InboundPhase {
    /// Await transaction completion.
    Applying,
    /// Await parent permission after observation submission or accumulation.
    Advertising,
    /// Await callback completion.
    NotifyingListener,
    /// No completion is legal after terminal result creation.
    Finished,
}

impl InboundPhase {
    /// Reject a mismatched phase before any owner consumes its state.
    fn validate(self, flow_id: FlowId, received: Self) -> Result<(), FlowProtocolError> {
        ensure!(
            self == received,
            protocol::UnexpectedCompletionSnafu {
                flow_id,
                expected: self.name(),
                received: received.name(),
            }
        );
        Ok(())
    }

    /// Render phase names for the existing contextual protocol error payload.
    const fn name(self) -> &'static str {
        match self {
            Self::Applying => "Applying",
            Self::Advertising => "Advertising",
            Self::NotifyingListener => "NotifyingListener",
            Self::Finished => "Finished",
        }
    }
}

/// Pure child/root traces for observation timing, batch ownership and protocol fault retention.
#[cfg(test)]
mod tests {
    use super::{
        super::{
            flow_driver::{FlowIdAllocator, FlowSlot, FlowState},
            listeners::{ListenerDataChangeBatches, ListenerDataChanges},
        },
        *,
    };
    use crate::api::{DatasetId, GroupReadToken, RowChange, RowId, RowKey};
    use flotsync_core::versions::{UpdateId, VersionVector};
    use std::{assert_matches, num::NonZeroUsize};
    use uuid::Uuid;

    /// Pure fixture admitting synthetic inputs without executing their transaction effects.
    fn admitted(
        input: TestInput,
        ids: &mut FlowIdAllocator,
    ) -> (FlowSlot<InboundFlow>, FlowEffect<InboundEffect>) {
        let group_id = GroupId(Uuid::nil());
        let sender = MemberIdentity::from_array(["app", "producer"]);
        let count = match input {
            TestInput::Single => 1,
            TestInput::Batch(count) => count,
        };
        let updates: Vec<_> = (1..=count).map(message).collect();
        let context = InboundDeliveryContext::Group {
            group_id,
            sender: sender.clone(),
            message_id: crate::delivery::shared::MessageId(Uuid::nil()),
        };
        let request = match input {
            TestInput::Single => InboundRequest::Single {
                origin: InboundUpdateOrigin::Producer { sender },
                message: updates.into_iter().next().expect("single input"),
            },
            TestInput::Batch(_) => InboundRequest::Batch {
                sender,
                message: UpdateBatchMessage { group_id, updates },
            },
        };
        let mut slot = FlowSlot::Available;
        let first = slot
            .admit(ids, group_id, |ownership| {
                InboundFlow::new(ownership, Some(context), request)
            })
            .expect("admission");
        (slot, first)
    }

    /// Base vector for one synthetic producer.
    fn versions() -> VersionVector {
        VersionVector::initial(NonZeroUsize::new(1).expect("one producer"))
    }

    /// Identify synthetic available/needed ranges independently of real transaction logic.
    fn update_id(version: u32) -> UpdateId {
        UpdateId {
            node_index: 0,
            version: u64::from(version),
        }
    }

    /// Synthetic transaction input shared by root and independent child fixtures.
    fn message(version: u32) -> UpdateMessage {
        UpdateMessage {
            group_id: GroupId(Uuid::nil()),
            update_id: update_id(version),
            read_versions: versions(),
            dataset_updates: Vec::new(),
        }
    }

    /// Start the same synthetic producer input for independent apply-child transition tests.
    fn producer_apply() -> (ApplyFlow, InboundEffect) {
        let origin = InboundUpdateOrigin::Producer {
            sender: MemberIdentity::from_array(["app", "producer"]),
        };
        ApplyFlow::new(origin, message(1))
    }

    /// One committed result with zero or several ordered callbacks and distinct observation tags.
    fn committed(version: u32, callbacks: usize) -> InboundUpdateOutcome {
        let group_id = GroupId(Uuid::nil());
        let mut event_batches = ListenerDataChangeBatches::new();
        for index in 0..callbacks {
            event_batches.push(ListenerDataChanges {
                read_token: GroupReadToken::from_group_version(
                    group_id,
                    versions().with_update_applied(update_id(version)),
                ),
                row_changes: vec![RowChange::ordinary_delete(RowId::new(
                    group_id,
                    DatasetId::try_from_static("docs").expect("dataset"),
                    RowKey(Uuid::from_u128(index as u128)),
                ))],
            });
        }
        InboundUpdateOutcome {
            event_batches,
            observed_available: vec![UpdateRangeMessage::from(update_id(version))],
            needed_ranges: vec![UpdateRangeMessage::from(update_id(version + 10))],
        }
    }

    /// Advance one legal completion while retaining slot ownership.
    fn next(
        slot: &mut FlowSlot<InboundFlow>,
        id: FlowId,
        completion: InboundCompletion,
    ) -> InboundEffect {
        match slot
            .transition(|flow| flow.complete(id, completion))
            .expect("legal completion")
        {
            FlowStep::Continue(effect) => {
                assert_eq!(effect.flow_id, id);
                effect.effect
            }
            FlowStep::Finished(_) => panic!("expected a pending effect"),
        }
    }

    /// Require a terminal result; restoration remains an explicit test action.
    fn finished(
        slot: &mut FlowSlot<InboundFlow>,
        id: FlowId,
        completion: InboundCompletion,
    ) -> FinishedInbound {
        match slot
            .transition(|flow| flow.complete(id, completion))
            .expect("terminal completion")
        {
            FlowStep::Finished(finished) => finished,
            FlowStep::Continue(_) => panic!("expected terminal result"),
        }
    }

    /// Require committed observation tags and their original order, including needed ranges.
    fn observations(effect: InboundEffect, committed_versions: &[u32]) {
        let InboundEffect::Advertise {
            group_id,
            observations,
        } = effect
        else {
            panic!("observations required")
        };
        assert_eq!(group_id, GroupId(Uuid::nil()));
        assert_eq!(
            observations.available,
            committed_versions
                .iter()
                .map(|version| UpdateRangeMessage::from(update_id(*version)))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            observations.needed,
            committed_versions
                .iter()
                .map(|version| UpdateRangeMessage::from(update_id(*version + 10)))
                .collect::<Vec<_>>()
        );
    }

    /// Select production observation timing while constructing pure root fixtures.
    #[derive(Clone, Copy)]
    enum TestInput {
        /// One producer input, with observations before callbacks.
        Single,
        /// This many forwarded inputs, accumulating evidence until batch termination.
        Batch(u32),
    }

    #[test]
    fn apply_child_waits_for_parent_then_owns_callback_advancement_and_failure() {
        let mut ids = FlowIdAllocator::default();
        let (_, first) = admitted(TestInput::Single, &mut ids);
        let (mut apply, effect) = producer_apply();
        assert_matches!(effect, InboundEffect::Apply { message, .. } if message.update_id == update_id(1));
        let committed_step = apply
            .complete(
                first.flow_id,
                InboundCompletion::Applied(Ok(committed(1, 2))),
            )
            .expect("child commit");
        assert_matches!(committed_step, ApplyStep::Committed(observations)
            if observations.available == vec![UpdateRangeMessage::from(update_id(1))]
                && observations.needed == vec![UpdateRangeMessage::from(update_id(11))]);
        assert!(matches!(&apply.state, ApplyState::AwaitingParent(_)));
        assert_matches!(
            apply.complete(first.flow_id, InboundCompletion::ListenerNotified(Ok(()))),
            Err(FlowProtocolError::UnexpectedCompletion { .. })
        );
        assert!(matches!(&apply.state, ApplyState::AwaitingParent(_)));
        assert_matches!(
            apply
                .resume_listener(first.flow_id)
                .expect("parent permission"),
            ApplyStep::Continue(InboundEffect::NotifyListener(_))
        );
        assert_matches!(
            apply
                .complete(first.flow_id, InboundCompletion::ListenerNotified(Ok(())))
                .expect("first callback"),
            ApplyStep::Continue(InboundEffect::NotifyListener(_))
        );
        let error = ListenerError::Rejected {
            message: "second callback".to_owned(),
        };
        assert_matches!(
            apply
                .complete(
                    first.flow_id,
                    InboundCompletion::ListenerNotified(Err(error))
                )
                .expect("classified failure"),
            ApplyStep::Finished(InboundOutcome::CommittedListenerFailed(
                InboundDeliveryError::NotifyListener {
                    source: ListenerError::Rejected { .. }
                }
            ))
        );
        assert!(matches!(&apply.state, ApplyState::Finished));
        assert_matches!(
            apply.complete(first.flow_id, InboundCompletion::ListenerNotified(Ok(()))),
            Err(FlowProtocolError::UnexpectedCompletion {
                expected: "Finished",
                ..
            })
        );
    }

    #[test]
    fn apply_child_finishes_transaction_failure_without_listener_work() {
        let mut ids = FlowIdAllocator::default();
        let (_, first) = admitted(TestInput::Single, &mut ids);
        let (mut apply, _) = producer_apply();
        let error = inbound::UnknownHostedGroupSnafu {
            group_id: GroupId(Uuid::nil()),
        }
        .build();
        assert_matches!(
            apply
                .complete(first.flow_id, InboundCompletion::Applied(Err(error)))
                .expect("classified transaction failure"),
            ApplyStep::Finished(InboundOutcome::TransactionFailed(
                InboundDeliveryError::UnknownHostedGroup { .. }
            ))
        );
        assert!(matches!(&apply.state, ApplyState::Finished));
    }

    #[test]
    fn batch_parent_keeps_current_child_and_cursor_through_pending_callbacks() {
        let mut ids = FlowIdAllocator::default();
        let (_, first) = admitted(TestInput::Single, &mut ids);
        let (mut batch, first_step) = BatchApplyFlow::new(
            MemberIdentity::from_array(["app", "producer"]),
            UpdateBatchMessage {
                group_id: GroupId(Uuid::nil()),
                updates: vec![message(1), message(2)],
            },
        );
        assert_matches!(first_step, BatchStep::Continue(InboundEffect::Apply { message, .. })
            if message.update_id == update_id(1));
        assert_matches!(
            batch
                .complete(
                    first.flow_id,
                    InboundCompletion::Applied(Ok(committed(1, 2)))
                )
                .expect("first commit"),
            BatchStep::Continue(InboundEffect::NotifyListener(_))
        );
        assert_eq!(batch.remaining.len(), 1);
        assert_eq!(
            batch.observations.available,
            vec![UpdateRangeMessage::from(update_id(1))]
        );
        assert!(matches!(
            &batch.state,
            BatchState::Running(ApplyFlow {
                state: ApplyState::Listening(_),
                ..
            })
        ));
        assert_matches!(
            batch
                .complete(first.flow_id, InboundCompletion::ListenerNotified(Ok(())))
                .expect("first callback"),
            BatchStep::Continue(InboundEffect::NotifyListener(_))
        );
        assert_eq!(batch.remaining.len(), 1);
        assert_matches!(batch.complete(first.flow_id, InboundCompletion::ListenerNotified(Ok(())))
            .expect("second callback"), BatchStep::Continue(InboundEffect::Apply { message, .. })
            if message.update_id == update_id(2));
        assert_eq!(batch.remaining.len(), 0);
        assert!(matches!(
            &batch.state,
            BatchState::Running(ApplyFlow {
                state: ApplyState::Applying,
                ..
            })
        ));
    }

    #[test]
    fn single_observes_before_every_callback_and_keeps_claims_until_completion() {
        let mut ids = FlowIdAllocator::default();
        let (mut slot, first) = admitted(TestInput::Single, &mut ids);
        assert_matches!(first.effect, InboundEffect::Apply { .. });
        observations(
            next(
                &mut slot,
                first.flow_id,
                InboundCompletion::Applied(Ok(committed(1, 2))),
            ),
            &[1],
        );
        assert_matches!(
            next(&mut slot, first.flow_id, InboundCompletion::Advertised),
            InboundEffect::NotifyListener(_)
        );
        assert_matches!(
            next(
                &mut slot,
                first.flow_id,
                InboundCompletion::ListenerNotified(Ok(()))
            ),
            InboundEffect::NotifyListener(_)
        );
        let denied = slot.admit(&mut ids, GroupId(Uuid::nil()), |_| -> (InboundFlow, ()) {
            panic!("claims still held")
        });
        assert_matches!(denied.err(), Some(FlowProtocolError::Occupied { .. }));
        let result = finished(
            &mut slot,
            first.flow_id,
            InboundCompletion::ListenerNotified(Ok(())),
        );
        assert_matches!(result.outcome, InboundOutcome::Completed);
        assert_matches!(result.context, Some(InboundDeliveryContext::Group { group_id, .. }) if group_id == GroupId(Uuid::nil()));
        assert!(matches!(&slot, FlowSlot::Occupied { .. }));
        slot.restore(result.ownership)
            .expect("explicit restoration");
        assert!(matches!(slot, FlowSlot::Available));
    }

    #[test]
    fn batch_finishes_all_callbacks_before_next_transaction_then_flushes_once() {
        let mut ids = FlowIdAllocator::default();
        let (mut slot, first) = admitted(TestInput::Batch(2), &mut ids);
        for callback in 0..2 {
            let completion = if callback == 0 {
                InboundCompletion::Applied(Ok(committed(1, 2)))
            } else {
                InboundCompletion::ListenerNotified(Ok(()))
            };
            assert_matches!(
                next(&mut slot, first.flow_id, completion),
                InboundEffect::NotifyListener(_)
            );
        }
        assert_matches!(next(&mut slot, first.flow_id, InboundCompletion::ListenerNotified(Ok(()))),
            InboundEffect::Apply { message, .. } if message.update_id == update_id(2));
        // A causal wait or duplicate has no callback but still contributes committed evidence.
        observations(
            next(
                &mut slot,
                first.flow_id,
                InboundCompletion::Applied(Ok(committed(2, 0))),
            ),
            &[1, 2],
        );
        let result = finished(&mut slot, first.flow_id, InboundCompletion::Advertised);
        assert_matches!(result.outcome, InboundOutcome::Completed);
        slot.restore(result.ownership)
            .expect("restore after final flush");
    }

    #[test]
    fn batch_transaction_failure_flushes_only_the_committed_prefix() {
        let mut ids = FlowIdAllocator::default();
        let (mut slot, first) = admitted(TestInput::Batch(2), &mut ids);
        assert_matches!(
            next(
                &mut slot,
                first.flow_id,
                InboundCompletion::Applied(Ok(committed(1, 1)))
            ),
            InboundEffect::NotifyListener(_)
        );
        assert_matches!(
            next(
                &mut slot,
                first.flow_id,
                InboundCompletion::ListenerNotified(Ok(()))
            ),
            InboundEffect::Apply { .. }
        );
        let error = inbound::UnknownHostedGroupSnafu {
            group_id: GroupId(Uuid::nil()),
        }
        .build();
        observations(
            next(
                &mut slot,
                first.flow_id,
                InboundCompletion::Applied(Err(error)),
            ),
            &[1],
        );
        let result = finished(&mut slot, first.flow_id, InboundCompletion::Advertised);
        assert_matches!(
            result.outcome,
            InboundOutcome::TransactionFailed(InboundDeliveryError::UnknownHostedGroup { .. })
        );
        slot.restore(result.ownership)
            .expect("restore after failed item and flush");
    }

    #[test]
    fn batch_rejected_callback_flushes_its_commit_and_abandons_remaining_inputs() {
        let mut ids = FlowIdAllocator::default();
        let (mut slot, first) = admitted(TestInput::Batch(2), &mut ids);
        assert_matches!(
            next(
                &mut slot,
                first.flow_id,
                InboundCompletion::Applied(Ok(committed(1, 2)))
            ),
            InboundEffect::NotifyListener(_)
        );
        let error = ListenerError::Rejected {
            message: "rejected".to_owned(),
        };
        observations(
            next(
                &mut slot,
                first.flow_id,
                InboundCompletion::ListenerNotified(Err(error)),
            ),
            &[1],
        );
        let result = finished(&mut slot, first.flow_id, InboundCompletion::Advertised);
        assert_matches!(
            result.outcome,
            InboundOutcome::CommittedListenerFailed(InboundDeliveryError::NotifyListener {
                source: ListenerError::Rejected { .. }
            })
        );
        slot.restore(result.ownership)
            .expect("restore without retrying committed input");
    }

    #[test]
    fn empty_batch_flushes_no_evidence_and_never_requests_a_transaction() {
        let mut ids = FlowIdAllocator::default();
        let (mut slot, first) = admitted(TestInput::Batch(0), &mut ids);
        observations(first.effect, &[]);
        let result = finished(&mut slot, first.flow_id, InboundCompletion::Advertised);
        assert_matches!(result.outcome, InboundOutcome::Completed);
        slot.restore(result.ownership)
            .expect("empty batch restoration");
    }

    #[test]
    fn invalid_identity_phase_and_terminal_duplicates_retain_faulted_ownership() {
        let mut ids = FlowIdAllocator::default();
        let (_, other) = admitted(TestInput::Single, &mut ids);
        let (mut slot, _first) = admitted(TestInput::Single, &mut ids);
        assert_matches!(
            slot.transition(|flow| flow.complete(other.flow_id, InboundCompletion::Advertised))
                .err(),
            Some(FlowProtocolError::WrongOwner { .. })
        );
        assert!(
            matches!(&slot, FlowSlot::Occupied { flow, state: FlowState::Faulted(_), .. } if flow.ownership.is_some())
        );

        let (mut slot, first) = admitted(TestInput::Single, &mut ids);
        assert_matches!(
            slot.transition(|flow| flow.complete(first.flow_id, InboundCompletion::Advertised))
                .err(),
            Some(FlowProtocolError::UnexpectedCompletion { .. })
        );
        assert!(
            matches!(&slot, FlowSlot::Occupied { flow, state: FlowState::Faulted(_), .. } if flow.ownership.is_some())
        );

        let (mut slot, first) = admitted(TestInput::Batch(0), &mut ids);
        let result = finished(&mut slot, first.flow_id, InboundCompletion::Advertised);
        assert_matches!(
            slot.transition(|flow| flow.complete(first.flow_id, InboundCompletion::Advertised))
                .err(),
            Some(FlowProtocolError::UnexpectedCompletion {
                expected: "Finished",
                ..
            })
        );
        assert_matches!(
            slot.restore(result.ownership),
            Err(FlowProtocolError::UnexpectedCompletion { .. })
        );
        assert!(
            matches!(&slot, FlowSlot::Occupied { state: FlowState::Faulted(fault), .. } if fault.returned_claims.len() == 1)
        );
    }

    #[test]
    fn single_transaction_and_committed_listener_failures_have_distinct_terminal_outcomes() {
        let mut ids = FlowIdAllocator::default();
        let (mut slot, first) = admitted(TestInput::Single, &mut ids);
        let error = inbound::UnknownHostedGroupSnafu {
            group_id: GroupId(Uuid::nil()),
        }
        .build();
        let result = finished(
            &mut slot,
            first.flow_id,
            InboundCompletion::Applied(Err(error)),
        );
        assert_matches!(result.outcome, InboundOutcome::TransactionFailed(_));
        slot.restore(result.ownership)
            .expect("no callback or observation after transaction failure");

        let (mut slot, first) = admitted(TestInput::Single, &mut ids);
        observations(
            next(
                &mut slot,
                first.flow_id,
                InboundCompletion::Applied(Ok(committed(1, 1))),
            ),
            &[1],
        );
        assert_matches!(
            next(&mut slot, first.flow_id, InboundCompletion::Advertised),
            InboundEffect::NotifyListener(_)
        );
        let result = finished(
            &mut slot,
            first.flow_id,
            InboundCompletion::ListenerNotified(Err(ListenerError::Rejected {
                message: "rejected".to_owned(),
            })),
        );
        assert_matches!(result.outcome, InboundOutcome::CommittedListenerFailed(_));
        slot.restore(result.ownership)
            .expect("observations already submitted before rejected callback");
    }
}
