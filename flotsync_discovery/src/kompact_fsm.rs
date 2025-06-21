use crate::kompact::prelude::*;

#[derive(Debug)]
#[repr(transparent)]
pub struct State<T>(Option<T>);

impl<T> State<T> {
    pub fn new(initial: T) -> Self {
        Self(Some(initial))
    }

    pub fn take(&mut self) -> T {
        self.0.take().expect("Illegal take on dangling state.")
    }

    pub fn set(&mut self, v: T) {
        self.0 = Some(v);
    }

    // pub fn transform<F>(&mut self, transformer: F) -> Handled
    // where
    //     F: FnOnce(T) -> StateUpdate<T>,
    // {
    //     let t = self.take();
    //     let res = transformer(t);
    //     match res {
    //         StateUpdate::NoUpdate { old_state, result } => {
    //             self.set(old_state);
    //             result
    //         }
    //         StateUpdate::Update { new_state, result } => {
    //             self.set(new_state);
    //             result
    //         }
    //         StateUpdate::Invalid { msg } => Handled::DieNow,
    //     }
    // }
}

#[derive(Debug)]
pub enum StateUpdate<T> {
    NoUpdate { old_state: T, result: Handled },
    Update { new_state: T, result: Handled },
    Invalid { msg: String },
}
impl<T> StateUpdate<T> {
    /// No change needed, just keep the current state and move on.
    pub const fn ok(old_state: T) -> Self {
        StateUpdate::NoUpdate {
            old_state,
            result: Handled::Ok,
        }
    }

    /// Change the state to `new_state` and move on.
    pub const fn transition(new_state: T) -> Self {
        StateUpdate::Update {
            new_state,
            result: Handled::Ok,
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

impl StateHandled for Handled {
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

// #[macro_export]
// macro_rules! transform_state {
//     ($state:expr, $func:expr) => {{
//         // let tmp = $state.take();
//         // let (res, new_state) = $func(tmp);
//         // $state.set(new_state);
//         // res
//         $state.transform($func)
//     }};
// }

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
                Handled::DieNow
            }
        }
    }};
}
