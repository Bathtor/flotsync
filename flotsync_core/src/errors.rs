use core::fmt;
use std::error::Error;

use itertools::Itertools;

pub type Result<T, E> = std::result::Result<T, Errors<E>>;

#[derive(Debug)]
pub enum Errors<T: Error> {
    Single(T),
    Multiple { errors: Vec<T> },
}
impl<T: Error> Errors<T> {
    const EMPTY: Self = Errors::Multiple { errors: vec![] };

    /// Whether there are no errors stored in here.
    ///
    /// This would generally be considered an somewhat illegal state.
    pub fn is_empty(&self) -> bool {
        match self {
            Errors::Single(_) => false,
            Errors::Multiple { errors } => errors.is_empty(),
        }
    }

    /// The number of errors currently stored.
    pub fn len(&self) -> usize {
        match self {
            Errors::Single(_) => 1,
            Errors::Multiple { errors } => errors.len(),
        }
    }

    /// Add `error` to the existing errors at the end.
    pub fn push(&mut self, error: T) {
        // Just temporarily swap an empty instance in place, to avoid complicated variant matching code.
        let mut vec = match std::mem::replace(self, Self::EMPTY) {
            Errors::Single(e) => vec![e],
            Errors::Multiple { errors } => errors,
        };
        vec.push(error);
        *self = Errors::Multiple { errors: vec };
    }
}

impl<T: Error> fmt::Display for Errors<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Errors::Single(e) => write!(f, "{e}"),
            Errors::Multiple { errors } => {
                write!(
                    f,
                    "Encountered multiple errors:\n{}",
                    errors.iter().map(|e| format!(" - {e}")).join("\n")
                )
            }
        }
    }
}
impl<T: Error> Error for Errors<T> {}

/// Extension methods for [[Result]] over [[Errors]] values.
pub trait ErrorsResultExt {
    type Error;

    /// Update the errors with `error`.
    fn push_err(&mut self, error: Self::Error);

    /// Produce a new value with `error`` appended.
    fn append_err(self, error: Self::Error) -> Self;
}

impl<T, E> ErrorsResultExt for Result<T, E>
where
    E: Error,
{
    type Error = E;

    fn push_err(&mut self, error: Self::Error) {
        match self {
            Ok(_) => {
                *self = Err(Errors::Multiple {
                    errors: vec![error],
                })
            }
            Err(e) => e.push(error),
        }
    }

    fn append_err(mut self, error: Self::Error) -> Self {
        self.push_err(error);
        self
    }
}

/// Extension methods to return [[Errors]] values.
pub trait ErrorsExt {
    type Item;

    /// Return Ok if `predicate` returns `Ok` for all members, otherwise return the collected Err values.
    fn ensure_for_all<P, E>(self, predicate: P) -> Result<(), E>
    where
        P: Fn(Self::Item) -> std::result::Result<(), E>,
        E: Error;
}

impl<I> ErrorsExt for I
where
    I: Iterator,
{
    type Item = I::Item;

    fn ensure_for_all<P, E>(self, predicate: P) -> Result<(), E>
    where
        P: Fn(Self::Item) -> std::result::Result<(), E>,
        E: Error,
    {
        #[allow(clippy::manual_try_fold)]
        self.fold(Ok(()), |acc, item| match predicate(item) {
            Ok(_) => acc,
            Err(e) => acc.append_err(e),
        })
    }
}
