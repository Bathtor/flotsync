//! Concrete owners admitted to the runtime's shared blocking operation slot.
//!
//! Publication and inbound application use the same group/listener admission slot
//! and component-scoped ID sequence. This enum only routes their typed effects and
//! results; their domain machines retain phase data through transactions,
//! catch-up submissions, callbacks and API replies.

use super::{
    flow_driver::{FlowId, FlowOwnership, FlowProtocolError, FlowStep, protocol},
    inbound_flow::{
        FinishedInbound,
        InboundCompletion,
        InboundEffect,
        InboundFlow,
        InboundOutcome,
    },
    inbound_support::InboundDeliveryContext,
    publication_flow::{
        FinishedPublication,
        PublicationCompletion,
        PublicationEffect,
        PublicationFlow,
        PublicationOutcome,
    },
};
use crate::runtime::replies::ReplyOutcome;
use snafu::prelude::*;

/// One concrete operation admitted with group and listener claims acquired together.
pub enum RuntimeFlow {
    /// Local write, broadcast/availability, callbacks and API reply.
    Publication(PublicationFlow),
    /// Direct or batch application, callbacks and catch-up observations.
    Inbound(Box<InboundFlow>),
}

impl RuntimeFlow {
    /// Validate root identity and route only the completion belonging to this operation kind.
    pub fn complete(
        &mut self,
        flow_id: FlowId,
        completion: RuntimeCompletion,
    ) -> Result<FlowStep<RuntimeEffect, FinishedRuntimeFlow>, FlowProtocolError> {
        let expected = match self {
            Self::Publication(flow) => flow.flow_id(),
            Self::Inbound(flow) => flow.flow_id(),
        };
        ensure!(
            flow_id == expected,
            protocol::WrongOwnerSnafu {
                expected,
                received: flow_id
            }
        );
        match (self, completion) {
            (Self::Publication(flow), RuntimeCompletion::Publication(completion)) => {
                let step = flow.complete(flow_id, completion)?;
                Ok(step.map(RuntimeEffect::Publication, FinishedRuntimeFlow::publication))
            }
            (Self::Inbound(flow), RuntimeCompletion::Inbound(completion)) => {
                let step = flow.complete(flow_id, completion)?;
                Ok(step.map(RuntimeEffect::Inbound, FinishedRuntimeFlow::inbound))
            }
            (flow, completion) => {
                let expected = match flow {
                    Self::Publication(_) => "Publication",
                    Self::Inbound(_) => "Inbound",
                };
                let received = match completion {
                    RuntimeCompletion::Publication(_) => "Publication",
                    RuntimeCompletion::Inbound(_) => "Inbound",
                };
                protocol::UnexpectedCompletionSnafu {
                    flow_id,
                    expected,
                    received,
                }
                .fail()
            }
        }
    }
}

/// Effects executed by the component while the concrete domain owner remains in the slot.
pub enum RuntimeEffect {
    /// Existing publication effect, retaining operation-specific effect order.
    Publication(PublicationEffect),
    /// Inbound transaction, observation submission or one listener callback.
    Inbound(InboundEffect),
}

/// Concrete completion returned to the operation that requested its effect.
pub enum RuntimeCompletion {
    /// Publication effect completion.
    Publication(PublicationCompletion),
    /// Inbound effect completion.
    Inbound(InboundCompletion),
}

/// Completed operation outcome and resource claims ready for slot restoration.
pub struct FinishedRuntimeFlow {
    /// Group and listener claims to restore explicitly before admitting another operation.
    pub ownership: FlowOwnership,
    /// Domain outcome to report to the API caller or inbound diagnostics.
    pub outcome: RuntimeOutcome,
}

impl FinishedRuntimeFlow {
    /// Wrap a completed publication and its claims for the shared runtime driver.
    fn publication(finished: FinishedPublication) -> Self {
        Self {
            ownership: finished.ownership,
            outcome: RuntimeOutcome::Publication {
                outcome: finished.outcome,
                reply: finished.reply,
            },
        }
    }

    /// Wrap completed inbound application and its claims for the shared runtime driver.
    fn inbound(finished: FinishedInbound) -> Self {
        Self {
            ownership: finished.ownership,
            outcome: RuntimeOutcome::Inbound(InboundResult {
                context: finished.context,
                outcome: finished.outcome,
            }),
        }
    }
}

/// Domain result after the shared slot can be restored.
pub enum RuntimeOutcome {
    /// Publication has attempted its API reply, including dropped receivers.
    Publication {
        /// Publication's write/listener outcome after its reply attempt.
        outcome: PublicationOutcome,
        /// Whether the API reply was delivered or its receiver was dropped.
        reply: ReplyOutcome,
    },
    /// Completed inbound application and its authenticated diagnostic context.
    Inbound(InboundResult),
}

/// Completed inbound application outcome and authenticated delivery context.
pub struct InboundResult {
    /// Authenticated delivery envelope: `Some` for production, `None` for direct fixtures.
    pub context: Option<InboundDeliveryContext>,
    /// Application outcome distinguishing transaction failure from committed callback rejection.
    pub outcome: InboundOutcome,
}

/// Shared-slot routing tests that reject completions from a different operation kind.
#[cfg(test)]
mod tests {
    use super::{
        super::{
            flow_driver::{FlowIdAllocator, FlowSlot, FlowState},
            inbound_flow::InboundRequest,
        },
        *,
    };
    use crate::{codecs::messages::UpdateBatchMessage, runtime::replies::ReplyOutcome};
    use flotsync_core::{GroupId, MemberIdentity};
    use std::assert_matches;
    use uuid::Uuid;

    #[test]
    fn wrong_completion_kind_faults_shared_slot_without_releasing_inbound_claims() {
        let mut ids = FlowIdAllocator::default();
        let group_id = GroupId(Uuid::nil());
        let request = InboundRequest::Batch {
            sender: MemberIdentity::from_array(["app", "sender"]),
            message: UpdateBatchMessage {
                group_id,
                updates: Vec::new(),
            },
        };
        let mut slot = FlowSlot::Available;
        let first = slot
            .admit(&mut ids, group_id, |ownership| {
                let (flow, effect) = InboundFlow::new(ownership, None, request);
                (
                    RuntimeFlow::Inbound(Box::new(flow)),
                    RuntimeEffect::Inbound(effect),
                )
            })
            .expect("shared admission");
        let error = slot.transition(|flow| {
            flow.complete(
                first.flow_id,
                RuntimeCompletion::Publication(PublicationCompletion::Replied(
                    ReplyOutcome::Delivered,
                )),
            )
        });
        assert_matches!(
            error.err(),
            Some(FlowProtocolError::UnexpectedCompletion {
                expected: "Inbound",
                received: "Publication",
                ..
            })
        );
        assert!(matches!(
            &slot,
            FlowSlot::Occupied {
                state: FlowState::Faulted(_),
                ..
            }
        ));
        let admission = slot.admit(&mut ids, group_id, |_| -> (RuntimeFlow, ()) {
            panic!("faulted owner must prevent any subsequent operation")
        });
        assert_matches!(admission.err(), Some(FlowProtocolError::Occupied { .. }));
    }
}
