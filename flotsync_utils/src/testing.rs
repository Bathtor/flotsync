//! Shared helpers for compile-time assertions and compact test fixtures.

use std::fmt::Debug;

/// A constant with all possible boolean values.
pub const BOOLEAN_DOMAIN: [bool; 2] = [true, false];

/// Testing helpers for types that implement [[Clone]].
pub trait CloneExt: Sized {
    /// Execute `thunk` with a copy of `self`.
    ///
    /// Convenient for creating scoped clone.
    fn with_copy<F>(&self, thunk: F)
    where
        F: FnOnce(Self);
}

impl<T> CloneExt for T
where
    T: Clone,
{
    fn with_copy<F>(&self, thunk: F)
    where
        F: FnOnce(T),
    {
        let copy: T = self.clone();
        thunk(copy);
    }
}

pub type SVec16<T> = SmallVec<T, 16>;

/// Infer the type of `value` and require it to implement [`Send`].
///
/// This supports compile-time assertions for opaque return types which cannot
/// be named in a type-only assertion.
pub fn assert_inferred_send<T: Send + ?Sized>(_: &T) {}

/// Require one named application-facing type to support executor hand-off.
pub fn assert_send<T: Send + ?Sized>() {}

/// Require one named application-facing type to support shared access.
pub fn assert_sync<T: Sync + ?Sized>() {}

/// Assert that two slices contain equal values with equal multiplicities.
///
/// Element order may differ. The comparison only requires [`PartialEq`] and
/// therefore uses a quadratic search suitable for test assertions.
///
/// # Panics
///
/// Panics when the slices have different lengths or an expected value cannot be
/// matched to a distinct actual value.
pub fn assert_unordered_eq<T>(actual: &[T], expected: &[T])
where
    T: Debug + PartialEq,
{
    assert_eq!(
        actual.len(),
        expected.len(),
        "unordered collections have different lengths; actual: {actual:?}; expected: {expected:?}"
    );
    let mut unmatched = actual.iter().collect::<Vec<_>>();
    for expected_value in expected {
        let position = unmatched
            .iter()
            .position(|actual_value| *actual_value == expected_value);
        if let Some(position) = position {
            unmatched.swap_remove(position);
        } else {
            panic!(
                "missing expected collection value {expected_value:?}; actual values: {actual:?}"
            );
        }
    }
}

#[macro_export]
macro_rules! svec16 {
    ($($elem:expr),* $(,)?) => {{
        SVec16::from_array([$($elem),*])
    }};
}

/// A simpler variant of the actual smallvec crate, that allows easier const creation
/// from arrays of mismatching sizes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SmallVec<T, const N: usize> {
    data: [Option<T>; N],
    len: usize,
}

impl<T: Copy, const N: usize> SmallVec<T, N> {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            data: [None; N],
            len: 0,
        }
    }

    /// # Panics
    ///
    /// Panics if `M` is larger than this `SmallVec`'s fixed capacity `N`.
    pub const fn from_array<const M: usize>(input: [T; M]) -> Self {
        assert!(M <= N);

        let mut data = [None; N];
        let mut i = 0;
        while i < M {
            data[i] = Some(input[i]);
            i += 1;
        }

        Self { data, len: M }
    }

    pub const fn len(&self) -> usize {
        self.len
    }
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub const fn get(&self, idx: usize) -> Option<&T> {
        if idx < self.len {
            self.data[idx].as_ref()
        } else {
            None
        }
    }

    /// # Panics
    ///
    /// Panics if `idx` is outside the initialised prefix.
    pub const fn at(&self, idx: usize) -> &T {
        match self.get(idx) {
            Some(v) => v,
            None => panic!("index out of bounds"),
        }
    }

    /// # Panics
    ///
    /// Panics if any slot inside the initialised prefix is empty.
    pub fn iter(&self) -> impl Iterator<Item = T> + '_ {
        self.data[..self.len].iter().map(|x| x.unwrap())
    }
}
impl<T: Copy, const N: usize> Default for SmallVec<T, N> {
    fn default() -> Self {
        Self::new()
    }
}
impl<T: Copy, const N: usize> FromIterator<T> for SmallVec<T, N> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut out = SmallVec::<T, N>::new();
        let mut i = 0;

        for v in iter {
            assert!(i < N, "SmallVec capacity exceeded");
            out.data[i] = Some(v);
            i += 1;
        }

        out.len = i;
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unordered_equality_accepts_distinct_orders() {
        assert_unordered_eq(&[1, 2, 3], &[3, 1, 2]);
    }

    #[test]
    #[should_panic(expected = "missing expected collection value")]
    fn unordered_equality_respects_multiplicity() {
        assert_unordered_eq(&[1, 1, 2], &[1, 2, 2]);
    }
}
