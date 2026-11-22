//! Operation admission, resource ownership and completion tracking for runtime flows.
//!
//! A flow is a state machine for one request. Its state records which step, or phase,
//! it is waiting for. The flow returns an action, called an effect, for the component
//! to execute. The component passes the result, called a completion, back to the flow
//! with the same flow ID. The flow checks the ID and phase, then records its next state
//! before returning another effect. For example, publication writes an update, submits
//! it for broadcast, advertises its availability, calls the listener, and attempts the
//! API reply. Inbound operations instead apply transactions, submit observations
//! and finish their listener callbacks. Domain modules define these actions and
//! states; this module supplies the slot, IDs and ownership shared by those steps.
//!
//! Starting a flow requires an available slot. The slot creates two resource claims:
//! ownership markers for sequencing changes to one Flotsync replication group and for
//! invoking the component's listener. They move together in [`FlowOwnership`] and
//! cannot be cloned. The flow owns them through transactions, submissions, callbacks
//! and replies, then returns them to the component, which explicitly releases the slot.
//! Dropping the caller's reply receiver still allows the flow to finish. An internal protocol error stops
//! further actions; the slot keeps the flow, first error and any returned claims for
//! component fault handling. Ordinary transaction/callback failures retain their
//! domain's API reply or inbound classification policy.
//!
//! The shared slot serialises publication and inbound operations for all groups.
//! `Handled::block_on` also prevents other handlers from running during these flows.
//! Claims express runtime ownership; they do not lock the store or prevent other
//! components from reading committed records.
//!
//! TODO(flotsync-zop.15 — Implement flow-owned partition registry): replace the single
//! admission slot with the shared resource registry.

use flotsync_core::GroupId;
use snafu::{Location, prelude::*};

/// Component-local identity carried by the actions and results of one flow.
///
/// Compare IDs only within the runtime component whose allocator created them.
/// Different components may use the same number. IDs wrap after `2^64` allocations
/// in that component; no old flow or result may survive that full allocation cycle.
/// IDs are neither persisted nor sent to other components or processes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FlowId(u64);

impl std::fmt::Display for FlowId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// One runtime component's flow ID sequence, shared across its slots and flow kinds.
///
/// Create a fresh allocator with `FlowIdAllocator::default()` when constructing the
/// runtime component. Keep it for the component's lifetime; releasing a slot must not reset it.
#[derive(Debug, Default)]
pub struct FlowIdAllocator {
    /// Next ID to issue, initially zero and advanced with wrapping arithmetic.
    next: u64,
}

impl FlowIdAllocator {
    /// Issue the next ID after admission has established that a slot is available.
    fn next_id(&mut self) -> FlowId {
        let id = FlowId(self.next);
        self.next = self.next.wrapping_add(1);
        id
    }
}

/// Action returned after the flow has recorded the state that will accept its result.
pub struct FlowEffect<E> {
    /// Flow that is waiting for this action's result.
    pub flow_id: FlowId,
    /// Action and its input data, executed without borrowing the component in the flow.
    pub effect: E,
}

/// Next action or finished result selected by a flow's state transition.
pub enum FlowStep<E, T> {
    /// Execute this action and pass its result back with the same flow ID.
    Continue(FlowEffect<E>),
    /// Completed flow result containing the resource claims to release.
    Finished(T),
}

impl<E, T> FlowStep<E, T> {
    /// Wrap concrete child effects/results for the shared runtime owner without changing identity.
    pub fn map<U, V>(
        self,
        effect: impl FnOnce(E) -> U,
        finished: impl FnOnce(T) -> V,
    ) -> FlowStep<U, V> {
        match self {
            Self::Continue(next) => FlowStep::Continue(FlowEffect {
                flow_id: next.flow_id,
                effect: effect(next.effect),
            }),
            Self::Finished(result) => FlowStep::Finished(finished(result)),
        }
    }
}

/// Resource claims for one Flotsync replication group and the component's listener.
///
/// The flow owns this non-cloneable value through its transactions, submissions,
/// callbacks and replies, then returns it in the finished result. The slot excludes
/// other publication/inbound flows; `block_on` excludes other handlers. Shared store
/// handles remain usable by other components.
#[derive(Debug)]
pub struct FlowOwnership {
    /// Flow ID checked when accepting an action's result or releasing the slot.
    flow_id: FlowId,
    /// Flotsync replication group owned through callbacks, observations and any reply attempt.
    group: ReplicationGroupClaim,
    /// Listener ownership, held throughout the operation, including complete batches.
    _listener: ListenerClaim,
}

impl FlowOwnership {
    /// Identify the flow that must return these claims before the slot can be released.
    pub const fn flow_id(&self) -> FlowId {
        self.flow_id
    }

    /// Identify the Flotsync replication group held until the flow returns these claims.
    pub const fn group_id(&self) -> GroupId {
        self.group.group_id
    }
}

/// Single operation slot, recording its owner and whether that flow can continue.
pub enum FlowSlot<F> {
    /// A new flow can acquire the replication group and listener claims.
    Available,
    /// A flow owns the slot; its claims are in the flow, an action, a result or its fault.
    Occupied {
        /// Owner's ID, available even when its data moves into an action or result.
        owner: FlowId,
        /// Operation state, including pending transactions, submissions, callbacks or replies.
        flow: F,
        /// Whether the flow can accept another result or has stopped with an internal error.
        state: FlowState,
    },
    /// Release failed without a flow in the slot; the error and returned claims are saved here.
    Faulted(FlowFault),
}

impl<F> FlowSlot<F> {
    /// Acquire both claims, create and record the flow, then return its first action.
    ///
    /// Use the same component allocator for every slot. Occupied/faulted slots reject
    /// admission without allocating an ID or invoking `create`.
    pub fn admit<E>(
        &mut self,
        flow_ids: &mut FlowIdAllocator,
        group_id: GroupId,
        create: impl FnOnce(FlowOwnership) -> (F, E),
    ) -> Result<FlowEffect<E>, FlowProtocolError> {
        match self {
            Self::Available => {
                let owner = flow_ids.next_id();
                let ownership = FlowOwnership {
                    flow_id: owner,
                    group: ReplicationGroupClaim { group_id },
                    _listener: ListenerClaim,
                };
                let (flow, effect) = create(ownership);
                *self = Self::Occupied {
                    owner,
                    flow,
                    state: FlowState::Running,
                };
                Ok(FlowEffect {
                    flow_id: owner,
                    effect,
                })
            }
            Self::Occupied { owner, .. } => protocol::OccupiedSnafu { owner: *owner }.fail(),
            Self::Faulted(fault) => Err(fault.error.clone()),
        }
    }

    /// Borrow a running flow; an empty or faulted slot returns a protocol error.
    fn running_mut(&mut self) -> Result<&mut F, FlowProtocolError> {
        match self {
            Self::Available => protocol::UnavailableSnafu.fail(),
            Self::Occupied {
                flow,
                state: FlowState::Running,
                ..
            } => Ok(flow),
            Self::Occupied {
                state: FlowState::Faulted(fault),
                ..
            }
            | Self::Faulted(fault) => Err(fault.error.clone()),
        }
    }

    /// Advance the flow synchronously; an internal error stops it without releasing its claims.
    pub fn transition<T>(
        &mut self,
        transition: impl FnOnce(&mut F) -> Result<T, FlowProtocolError>,
    ) -> Result<T, FlowProtocolError> {
        let result = self.running_mut().and_then(transition);
        self.record_error(result)
    }

    /// Record the first internal error alongside the flow and return the supplied error.
    pub fn fault(&mut self, error: FlowProtocolError) -> FlowProtocolError {
        if let Self::Occupied {
            state: state @ FlowState::Running,
            ..
        } = self
        {
            *state = FlowState::Faulted(FlowFault {
                error: error.clone(),
                returned_claims: Vec::new(),
            });
        } else {
            // An existing fault already owns its first error and any returned claims.
            // A failure before admission has no flow or claims to save.
        }
        error
    }

    /// Release the slot using the resource claims returned by a completed flow.
    ///
    /// Failed release saves the ownership value with the error, including attempts with
    /// another flow's claims or an empty slot. Existing errors and claims remain intact.
    pub fn restore(&mut self, ownership: FlowOwnership) -> Result<(), FlowProtocolError> {
        let flow_id = ownership.flow_id();
        let result = match self {
            Self::Available => protocol::UnavailableSnafu.fail(),
            Self::Occupied {
                owner,
                state: FlowState::Running,
                ..
            } => {
                if *owner == flow_id {
                    *self = Self::Available;
                    Ok(())
                } else {
                    protocol::WrongOwnerSnafu {
                        expected: *owner,
                        received: flow_id,
                    }
                    .fail()
                }
            }
            Self::Occupied {
                state: FlowState::Faulted(fault),
                ..
            }
            | Self::Faulted(fault) => Err(fault.error.clone()),
        };
        match result {
            Ok(()) => Ok(()),
            Err(error) => {
                self.save_rejected_ownership(&error, ownership);
                Err(error)
            }
        }
    }

    /// Save claims from a failed release before returning its error.
    fn save_rejected_ownership(&mut self, error: &FlowProtocolError, ownership: FlowOwnership) {
        match self {
            Self::Occupied {
                state: FlowState::Faulted(fault),
                ..
            }
            | Self::Faulted(fault) => fault.returned_claims.push(ownership),
            Self::Occupied { state, .. } => {
                *state = FlowState::Faulted(FlowFault::for_restoration(error.clone(), ownership));
            }
            Self::Available => {
                *self = Self::Faulted(FlowFault::for_restoration(error.clone(), ownership));
            }
        }
    }

    /// Return the transition result, recording an error in the slot if it failed.
    fn record_error<T>(
        &mut self,
        result: Result<T, FlowProtocolError>,
    ) -> Result<T, FlowProtocolError> {
        match result {
            Ok(value) => Ok(value),
            Err(error) => Err(self.fault(error)),
        }
    }
}

/// Invalid flow operation; ordinary write and callback failures are handled by the flow.
#[derive(Clone, Debug, Snafu)]
#[snafu(visibility(pub), module(protocol))]
pub enum FlowProtocolError {
    /// A second operation attempted to acquire an occupied slot.
    #[snafu(display("Flow {owner} still owns the admission slot"))]
    Occupied {
        owner: FlowId,
        #[snafu(implicit)]
        location: Location,
    },
    /// A result or slot release was attempted without a flow owning the slot.
    #[snafu(display("No flow owns the admission slot"))]
    Unavailable {
        #[snafu(implicit)]
        location: Location,
    },
    /// An action's result or the claims used to release the slot belong to another flow.
    #[snafu(display("Expected flow {expected}, received flow {received}"))]
    WrongOwner {
        expected: FlowId,
        received: FlowId,
        #[snafu(implicit)]
        location: Location,
    },
    /// The current phase cannot accept this result, including a repeated result.
    #[snafu(display("Flow {flow_id} in {expected} cannot accept {received} completion"))]
    UnexpectedCompletion {
        flow_id: FlowId,
        expected: &'static str,
        received: &'static str,
        #[snafu(implicit)]
        location: Location,
    },
    /// The flow lacks a field needed for its next step, such as its reply promise or ownership.
    #[snafu(display("Flow {flow_id} is missing its {field}"))]
    MissingState {
        flow_id: FlowId,
        field: &'static str,
        #[snafu(implicit)]
        location: Location,
    },
}

/// Whether the slot's flow can advance, independent of its request-specific phase.
pub enum FlowState {
    /// The flow may accept its next action's result.
    Running,
    /// An internal error stopped the flow; the component must handle the error and claims.
    Faulted(FlowFault),
}

/// First internal error and resource claims saved when slot release fails.
pub struct FlowFault {
    /// First error, unchanged when later actions also fail.
    pub error: FlowProtocolError,
    /// Complete replication group and listener ownership values saved after failed release.
    /// Empty means ownership is still in the flow, an action or a finished result.
    pub returned_claims: Vec<FlowOwnership>,
}

impl FlowFault {
    /// Save the first release error with the complete ownership value.
    fn for_restoration(error: FlowProtocolError, ownership: FlowOwnership) -> Self {
        Self {
            error,
            returned_claims: vec![ownership],
        }
    }
}

/// Ownership marker for sequencing changes to one Flotsync replication group.
#[derive(Debug)]
struct ReplicationGroupClaim {
    /// Flotsync replication group whose operation owns this claim.
    group_id: GroupId,
}

/// Ownership marker for invoking the component's listener, acquired with the replication group.
#[derive(Debug)]
struct ListenerClaim;

/// Admission, explicit release and protocol fault tests for non-cloneable ownership claims.
#[cfg(test)]
mod tests {
    use super::*;
    use std::assert_matches;
    use uuid::Uuid;

    /// Admit a claim-only driver fixture with ownership held by its synthetic effect.
    fn admitted_claims(
        flow_ids: &mut FlowIdAllocator,
        group_id: GroupId,
    ) -> (FlowSlot<()>, FlowOwnership) {
        let mut slot = FlowSlot::Available;
        let initial = slot
            .admit(flow_ids, group_id, |ownership| ((), ownership))
            .expect("initial admission");
        (slot, initial.effect)
    }

    #[test]
    fn wrong_owner_restoration_keeps_both_groups_and_original_fault() {
        let mut flow_ids = FlowIdAllocator::default();
        let first_group = GroupId(Uuid::new_v4());
        let second_group = GroupId(Uuid::new_v4());
        let (mut slot, first_claims) = admitted_claims(&mut flow_ids, first_group);
        let (_second_slot, second_claims) = admitted_claims(&mut flow_ids, second_group);
        let expected = first_claims.flow_id();
        let received = second_claims.flow_id();
        assert_matches!(slot.restore(second_claims), Err(FlowProtocolError::WrongOwner {
            expected: owner, received: other, ..
        }) if owner == expected && other == received);
        assert_matches!(
            slot.restore(first_claims),
            Err(FlowProtocolError::WrongOwner { .. })
        );
        let FlowSlot::Occupied {
            state: FlowState::Faulted(fault),
            ..
        } = &slot
        else {
            panic!("failed restoration keeps the owned claims");
        };
        assert_matches!(fault.error, FlowProtocolError::WrongOwner {
            expected: owner, received: other, ..
        } if owner == expected && other == received);
        assert_eq!(fault.returned_claims.len(), 2);
        assert_eq!(fault.returned_claims[0].group_id(), second_group);
        assert_eq!(fault.returned_claims[1].group_id(), first_group);
    }

    #[test]
    fn vacant_slot_restoration_keeps_claims_and_rejects_admission() {
        let mut flow_ids = FlowIdAllocator::default();
        let group_id = GroupId(Uuid::new_v4());
        let (_owner_slot, claims) = admitted_claims(&mut flow_ids, group_id);
        let flow_id = claims.flow_id();
        let mut slot = FlowSlot::<()>::Available;
        assert_matches!(
            slot.restore(claims),
            Err(FlowProtocolError::Unavailable { .. })
        );
        let admission: Result<FlowEffect<()>, _> = slot.admit(&mut flow_ids, group_id, |_| {
            panic!("faulted vacant slot must reject admission")
        });
        assert_matches!(admission.err(), Some(FlowProtocolError::Unavailable { .. }));
        let transition = slot.transition(|()| -> Result<(), FlowProtocolError> {
            panic!("faulted vacant slot must reject transitions")
        });
        assert_matches!(transition, Err(FlowProtocolError::Unavailable { .. }));
        let FlowSlot::Faulted(fault) = &slot else {
            panic!("vacant restoration fault keeps returned claims");
        };
        assert_eq!(fault.returned_claims.len(), 1);
        assert_eq!(fault.returned_claims[0].flow_id(), flow_id);
        assert_eq!(fault.returned_claims[0].group_id(), group_id);
    }
}
