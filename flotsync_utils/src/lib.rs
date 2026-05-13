use std::{fmt, future::Future, marker::PhantomData, pin::Pin};

use kompact::prelude::{HandlerError, HandlerResultExt as _};
use snafu::{FromString, OptionExt as SnafuOptionExt, ResultExt as SnafuResultExt};

pub mod claimable_promise;
pub mod debugging;
pub mod err;
pub mod testing;

pub use claimable_promise::KClaimablePromise;

/// Heap-allocated, `Send` future used by dyn-friendly async APIs.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Non-owning phantom marker for generic parameters that affect a type's API
/// surface but are neither stored nor dropped by that type.
///
/// This is equivalent to `PhantomData<fn() -> T>`, which keeps the generic
/// parameter visible to the type system without modeling the enclosing type as
/// logically owning a `T`.
pub type NonOwningPhantomData<T> = PhantomData<fn() -> T>;

/// Snafu `Whatever` context helpers that immediately classify handler errors.
pub trait ResultExt<T, E>: Sized {
    /// Add eager `Whatever` context and classify failures as benign handler errors.
    ///
    /// # Errors
    ///
    /// Returns [`HandlerError::Benign`] when `self` is `Err`.
    fn whatever_benign<S>(self, context: S) -> Result<T, HandlerError>
    where
        S: Into<String>,
        E: Into<<snafu::Whatever as FromString>::Source>;

    /// Add lazy `Whatever` context and classify failures as benign handler errors.
    ///
    /// # Errors
    ///
    /// Returns [`HandlerError::Benign`] when `self` is `Err`.
    fn with_whatever_benign<F, S>(self, context: F) -> Result<T, HandlerError>
    where
        F: FnOnce(&mut E) -> S,
        S: Into<String>,
        E: Into<<snafu::Whatever as FromString>::Source>;

    /// Add eager `Whatever` context and classify failures as recoverable handler errors.
    ///
    /// # Errors
    ///
    /// Returns [`HandlerError::Recoverable`] when `self` is `Err`.
    fn whatever_recoverable<S>(self, context: S) -> Result<T, HandlerError>
    where
        S: Into<String>,
        E: Into<<snafu::Whatever as FromString>::Source>;

    /// Add lazy `Whatever` context and classify failures as recoverable handler errors.
    ///
    /// # Errors
    ///
    /// Returns [`HandlerError::Recoverable`] when `self` is `Err`.
    fn with_whatever_recoverable<F, S>(self, context: F) -> Result<T, HandlerError>
    where
        F: FnOnce(&mut E) -> S,
        S: Into<String>,
        E: Into<<snafu::Whatever as FromString>::Source>;

    /// Add eager `Whatever` context and classify failures as unrecoverable handler errors.
    ///
    /// # Errors
    ///
    /// Returns [`HandlerError::Unrecoverable`] when `self` is `Err`.
    fn whatever_unrecoverable<S>(self, context: S) -> Result<T, HandlerError>
    where
        S: Into<String>,
        E: Into<<snafu::Whatever as FromString>::Source>;

    /// Add lazy `Whatever` context and classify failures as unrecoverable handler errors.
    ///
    /// # Errors
    ///
    /// Returns [`HandlerError::Unrecoverable`] when `self` is `Err`.
    fn with_whatever_unrecoverable<F, S>(self, context: F) -> Result<T, HandlerError>
    where
        F: FnOnce(&mut E) -> S,
        S: Into<String>,
        E: Into<<snafu::Whatever as FromString>::Source>;
}

impl<T, E> ResultExt<T, E> for Result<T, E> {
    fn whatever_benign<S>(self, context: S) -> Result<T, HandlerError>
    where
        S: Into<String>,
        E: Into<<snafu::Whatever as FromString>::Source>,
    {
        SnafuResultExt::whatever_context::<S, snafu::Whatever>(self, context).benign_err()
    }

    fn with_whatever_benign<F, S>(self, context: F) -> Result<T, HandlerError>
    where
        F: FnOnce(&mut E) -> S,
        S: Into<String>,
        E: Into<<snafu::Whatever as FromString>::Source>,
    {
        SnafuResultExt::with_whatever_context::<F, S, snafu::Whatever>(self, context).benign_err()
    }

    fn whatever_recoverable<S>(self, context: S) -> Result<T, HandlerError>
    where
        S: Into<String>,
        E: Into<<snafu::Whatever as FromString>::Source>,
    {
        SnafuResultExt::whatever_context::<S, snafu::Whatever>(self, context).recoverable_err()
    }

    fn with_whatever_recoverable<F, S>(self, context: F) -> Result<T, HandlerError>
    where
        F: FnOnce(&mut E) -> S,
        S: Into<String>,
        E: Into<<snafu::Whatever as FromString>::Source>,
    {
        SnafuResultExt::with_whatever_context::<F, S, snafu::Whatever>(self, context)
            .recoverable_err()
    }

    fn whatever_unrecoverable<S>(self, context: S) -> Result<T, HandlerError>
    where
        S: Into<String>,
        E: Into<<snafu::Whatever as FromString>::Source>,
    {
        SnafuResultExt::whatever_context::<S, snafu::Whatever>(self, context).unrecoverable_err()
    }

    fn with_whatever_unrecoverable<F, S>(self, context: F) -> Result<T, HandlerError>
    where
        F: FnOnce(&mut E) -> S,
        S: Into<String>,
        E: Into<<snafu::Whatever as FromString>::Source>,
    {
        SnafuResultExt::with_whatever_context::<F, S, snafu::Whatever>(self, context)
            .unrecoverable_err()
    }
}

pub trait OptionExt<T> {
    /// Construct `Some` only when `cond` is true.
    fn when(cond: bool, thunk: impl FnOnce() -> T) -> Option<T>;

    /// Add eager `Whatever` context and classify absence as a benign handler error.
    ///
    /// # Errors
    ///
    /// Returns [`HandlerError::Benign`] when `self` is `None`.
    fn whatever_benign<S>(self, context: S) -> Result<T, HandlerError>
    where
        S: Into<String>;

    /// Add lazy `Whatever` context and classify absence as a benign handler error.
    ///
    /// # Errors
    ///
    /// Returns [`HandlerError::Benign`] when `self` is `None`.
    fn with_whatever_benign<F, S>(self, context: F) -> Result<T, HandlerError>
    where
        F: FnOnce() -> S,
        S: Into<String>;

    /// Add eager `Whatever` context and classify absence as a recoverable handler error.
    ///
    /// # Errors
    ///
    /// Returns [`HandlerError::Recoverable`] when `self` is `None`.
    fn whatever_recoverable<S>(self, context: S) -> Result<T, HandlerError>
    where
        S: Into<String>;

    /// Add lazy `Whatever` context and classify absence as a recoverable handler error.
    ///
    /// # Errors
    ///
    /// Returns [`HandlerError::Recoverable`] when `self` is `None`.
    fn with_whatever_recoverable<F, S>(self, context: F) -> Result<T, HandlerError>
    where
        F: FnOnce() -> S,
        S: Into<String>;

    /// Add eager `Whatever` context and classify absence as an unrecoverable handler error.
    ///
    /// # Errors
    ///
    /// Returns [`HandlerError::Unrecoverable`] when `self` is `None`.
    fn whatever_unrecoverable<S>(self, context: S) -> Result<T, HandlerError>
    where
        S: Into<String>;

    /// Add lazy `Whatever` context and classify absence as an unrecoverable handler error.
    ///
    /// # Errors
    ///
    /// Returns [`HandlerError::Unrecoverable`] when `self` is `None`.
    fn with_whatever_unrecoverable<F, S>(self, context: F) -> Result<T, HandlerError>
    where
        F: FnOnce() -> S,
        S: Into<String>;
}

impl<T> OptionExt<T> for Option<T> {
    fn when(cond: bool, thunk: impl FnOnce() -> T) -> Option<T> {
        if cond { Some(thunk()) } else { None }
    }

    fn whatever_benign<S>(self, context: S) -> Result<T, HandlerError>
    where
        S: Into<String>,
    {
        SnafuOptionExt::whatever_context::<S, snafu::Whatever>(self, context).benign_err()
    }

    fn with_whatever_benign<F, S>(self, context: F) -> Result<T, HandlerError>
    where
        F: FnOnce() -> S,
        S: Into<String>,
    {
        SnafuOptionExt::with_whatever_context::<F, S, snafu::Whatever>(self, context).benign_err()
    }

    fn whatever_recoverable<S>(self, context: S) -> Result<T, HandlerError>
    where
        S: Into<String>,
    {
        SnafuOptionExt::whatever_context::<S, snafu::Whatever>(self, context).recoverable_err()
    }

    fn with_whatever_recoverable<F, S>(self, context: F) -> Result<T, HandlerError>
    where
        F: FnOnce() -> S,
        S: Into<String>,
    {
        SnafuOptionExt::with_whatever_context::<F, S, snafu::Whatever>(self, context)
            .recoverable_err()
    }

    fn whatever_unrecoverable<S>(self, context: S) -> Result<T, HandlerError>
    where
        S: Into<String>,
    {
        SnafuOptionExt::whatever_context::<S, snafu::Whatever>(self, context).unrecoverable_err()
    }

    fn with_whatever_unrecoverable<F, S>(self, context: F) -> Result<T, HandlerError>
    where
        F: FnOnce() -> S,
        S: Into<String>,
    {
        SnafuOptionExt::with_whatever_context::<F, S, snafu::Whatever>(self, context)
            .unrecoverable_err()
    }
}

#[macro_export]
macro_rules! option_when {
    ($cond:expr, $then:expr) => {
        if $cond { Some($then) } else { None }
    };
}

#[macro_export]
macro_rules! require {
    ($cond:expr, $err:expr) => {
        if !$cond {
            return Err($err);
        }
    };
}

/// An immutable wrapper around strings.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IString(Box<str>);

impl IString {
    #[must_use]
    pub fn new(s: String) -> Self {
        Self(s.into_boxed_str())
    }
}

impl From<String> for IString {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for IString {
    fn from(value: &str) -> Self {
        Self(Box::<str>::from(value))
    }
}
impl fmt::Debug for IString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // A bit shorter than the full generate Debug.
        write!(f, "i\"{}\"", self.0)
    }
}
impl fmt::Display for IString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for IString {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn istring_invariants(s in "\\PC*") {
            let s_ref: &str = &s;
            let istring = IString::from(s_ref);

            assert_eq!(istring, istring);
            assert_eq!(istring.as_ref(), s_ref);
            assert_eq!(istring.to_string(), s.clone());
        }
    }
}
