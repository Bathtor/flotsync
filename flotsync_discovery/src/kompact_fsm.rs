use crate::kompact::prelude::*;

#[derive(Debug)]
#[repr(transparent)]
pub struct State<T>(Option<T>);

impl<T> State<T> {
    pub fn new(initial: T) -> Self {
        Self(Some(initial))
    }

    /// # Panics
    ///
    /// Panics if the state was already taken.
    pub fn take(&mut self) -> T {
        self.0.take().expect("Illegal take on dangling state.")
    }

    pub fn set(&mut self, v: T) {
        self.0 = Some(v);
    }
}

#[derive(Debug)]
pub enum StateUpdate<T> {
    NoUpdate { old_state: T, result: HandlerResult },
    Update { new_state: T, result: HandlerResult },
    Invalid { msg: String },
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

pub trait StateHandled
where
    Self: Sized,
{
    fn stay_in<T>(self, old_state: T) -> StateUpdate<T>;
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
                error!($comp.log(), "The component signalled an invalid state transition: {msg}\nKilling the component so it can be re-initialised into a legal state.");
                Handled::SHUTDOWN
            }
        }
    }};
}
