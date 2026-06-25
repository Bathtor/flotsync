//! Small state-transition helpers for Kompact components.

use kompact::prelude::*;
use snafu::Snafu;

/// Recoverable component fault emitted for invalid FSM transition tables.
#[derive(Debug, Snafu)]
#[snafu(display(
    "The component signalled an invalid state transition: {msg}\nMarking the component as recoverably faulty so it can be re-initialised into a legal state."
))]
pub struct InvalidStateTransition {
    /// Description of the illegal state transition.
    msg: String,
}

impl InvalidStateTransition {
    /// Build one invalid transition error.
    #[must_use]
    pub fn new(msg: String) -> Self {
        Self { msg }
    }
}

/// Owned component state slot used by [`transform_state_match!`].
///
/// The state is temporarily taken while matching a transition and must be set again before the
/// component returns from the handler.
#[derive(Debug)]
#[repr(transparent)]
pub struct State<T>(Option<T>);

impl<T> State<T> {
    /// Build a state slot from its initial value.
    pub fn new(initial: T) -> Self {
        Self(Some(initial))
    }

    /// # Panics
    ///
    /// Panics if the state was already taken.
    pub fn take(&mut self) -> T {
        self.0.take().expect("Illegal take on dangling state.")
    }

    /// Borrow the current state.
    ///
    /// # Panics
    ///
    /// Panics if the state was already taken.
    pub fn get(&self) -> &T {
        self.0.as_ref().expect("Illegal borrow on dangling state.")
    }

    /// Replace the current state value.
    pub fn set(&mut self, v: T) {
        self.0 = Some(v);
    }
}

/// Result of applying one state-transition handler.
#[derive(Debug)]
pub enum StateUpdate<T> {
    /// Keep the previous state and return the supplied Kompact handler result.
    NoUpdate {
        /// State value to restore.
        old_state: T,
        /// Handler result to return from the component.
        result: HandlerResult,
    },
    /// Replace the previous state and return the supplied Kompact handler result.
    Update {
        /// New state value to install.
        new_state: T,
        /// Handler result to return from the component.
        result: HandlerResult,
    },
    /// Report an invalid transition and mark the component recoverably faulty.
    Invalid {
        /// Human-readable transition error.
        msg: String,
    },
}
impl<T> StateUpdate<T> {
    /// No change needed, just keep the current state and move on.
    pub const fn ok(old_state: T) -> Self {
        StateUpdate::NoUpdate {
            old_state,
            result: Handled::OK,
        }
    }

    /// Change the state to `new_state` and move on.
    pub const fn transition(new_state: T) -> Self {
        StateUpdate::Update {
            new_state,
            result: Handled::OK,
        }
    }

    /// Mark this transition as invalid.
    pub fn invalid<I>(msg: I) -> Self
    where
        I: Into<String>,
    {
        StateUpdate::Invalid { msg: msg.into() }
    }

    /// If this is an Update variant, then replace it with a new Update variant that updates the state to `replacement` instead.
    ///
    /// Otherwise leave this unchanged.
    #[must_use]
    pub fn replace_new_state(self, replacement: T) -> Self {
        match self {
            StateUpdate::Update { result, .. } => StateUpdate::Update {
                new_state: replacement,
                result,
            },
            u => u,
        }
    }
}

/// Convenience extension for attaching state transitions to Kompact handler results.
pub trait StateHandled
where
    Self: Sized,
{
    /// Keep `old_state` and return this handler result.
    fn stay_in<T>(self, old_state: T) -> StateUpdate<T>;
    /// Transition to `new_state` and return this handler result.
    fn and_transition<T>(self, new_state: T) -> StateUpdate<T>;
}

impl StateHandled for HandlerResult {
    fn stay_in<T>(self, old_state: T) -> StateUpdate<T> {
        StateUpdate::NoUpdate {
            old_state,
            result: self,
        }
    }

    fn and_transition<T>(self, new_state: T) -> StateUpdate<T> {
        StateUpdate::Update {
            new_state,
            result: self,
        }
    }
}

/// Run a state-machine match while guaranteeing the state slot is restored.
#[macro_export]
macro_rules! transform_state_match {
    ($comp:ident, $state:ident, { $($tokens:tt)* }) => {{
        let tmp = $comp.$state.take();
        let res = match tmp { $($tokens)* };
        match res {
            StateUpdate::NoUpdate {old_state, result} => {
                $comp.$state.set(old_state);
                result
            }
            StateUpdate::Update {new_state, result} => {
                $comp.$state.set(new_state);
                result
            }
            StateUpdate::Invalid {msg} => {
                Err($crate::kompact::prelude::HandlerError::recoverable(
                    $crate::kompact_fsm::InvalidStateTransition::new(msg)
                ))
            }
        }
    }};
}

#[cfg(test)]
mod tests {
    use super::{State, StateUpdate};
    use crate::kompact_testing::{eventually, eventually_component_state};
    use kompact::prelude::*;
    use std::{sync::mpsc, time::Duration};

    const WAIT_TIMEOUT: Duration = Duration::from_secs(3);

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum TestState {
        Initial,
        Dirty,
    }

    #[derive(Debug)]
    enum TestMessage {
        MarkDirty,
        TriggerInvalidTransition,
    }

    #[derive(ComponentDefinition)]
    struct TestComponent {
        ctx: ComponentContext<Self>,
        state: State<TestState>,
    }

    impl TestComponent {
        fn new() -> Self {
            Self {
                ctx: ComponentContext::uninitialised(),
                state: State::new(TestState::Initial),
            }
        }
    }

    ignore_lifecycle!(TestComponent);

    impl Actor for TestComponent {
        type Message = TestMessage;

        fn receive_local(&mut self, msg: Self::Message) -> HandlerResult {
            transform_state_match!(self, state, {
                TestState::Initial => match msg {
                    TestMessage::MarkDirty => StateUpdate::transition(TestState::Dirty),
                    TestMessage::TriggerInvalidTransition => {
                        StateUpdate::invalid("invalid transition from initial")
                    }
                },
                TestState::Dirty => match msg {
                    TestMessage::MarkDirty => StateUpdate::ok(TestState::Dirty),
                    TestMessage::TriggerInvalidTransition => {
                        StateUpdate::invalid("test transition failed")
                    }
                },
            })
        }
    }

    #[test]
    fn invalid_state_update_recovers_component_to_initial_state() {
        let system = KompactConfig::default()
            .build()
            .wait()
            .expect("test Kompact system");
        let component = system.create(TestComponent::new);
        let component_ref = component.actor_ref();
        let (recovered_tx, recovered_rx) = mpsc::channel();

        component.set_recovery_function(move |fault| {
            fault.recover_with(move |_context, system, _log| {
                let recovered = system.create(TestComponent::new);
                system.start(&recovered);
                recovered_tx
                    .send(recovered)
                    .expect("recovered component should be sent");
            })
        });

        system.start(&component);
        component_ref.tell(TestMessage::MarkDirty);
        eventually_component_state(
            WAIT_TIMEOUT,
            &component,
            |component| component.state.get() == &TestState::Dirty,
            "component should enter dirty state before the invalid transition",
        );

        component_ref.tell(TestMessage::TriggerInvalidTransition);
        let recovered = recovered_rx
            .recv_timeout(WAIT_TIMEOUT)
            .expect("recovered component should be created");

        eventually(
            Duration::from_secs(1),
            || component.is_faulty(),
            "original component should be marked faulty",
        );
        eventually_component_state(
            WAIT_TIMEOUT,
            &recovered,
            |component| component.state.get() == &TestState::Initial,
            "recovered component should start in the initial state",
        );

        recovered.actor_ref().tell(TestMessage::MarkDirty);
        eventually_component_state(
            WAIT_TIMEOUT,
            &recovered,
            |component| component.state.get() == &TestState::Dirty,
            "recovered component should handle messages after recovery",
        );

        system.shutdown().wait().expect("Kompact shutdown");
    }
}
