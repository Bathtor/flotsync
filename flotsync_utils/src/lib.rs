use std::fmt;

pub mod err;

pub trait OptionExt<T> {
    fn when(cond: bool, thunk: impl FnOnce() -> T) -> Option<T>;
}

impl<T> OptionExt<T> for Option<T> {
    fn when(cond: bool, thunk: impl FnOnce() -> T) -> Option<T> {
        if cond { Some(thunk()) } else { None }
    }
}

#[macro_export]
macro_rules! option_when {
    ($cond:expr, $then:expr) => {
        if $cond { Some($then) } else { None }
    };
}

/// An immutable wrapper around strings.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IString(Box<str>);

impl IString {
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
            assert_eq!(istring.to_string(), s.to_string());
        }
    }
}
