use std::fmt;

/// A trait for types that support a third type of formatting in addition to [[fmt::Display]]
/// and [[fmt::Debug]].
///
/// Typically a tighter debugging layout that highlights important information and hides some noise
/// present in the full [[fmt::Debug]] format.
pub trait DebugFormatting {
    /// Same signature and behaviour as [[std::fmt::Debug::fmt]];
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error>;

    /// Wrap this reference into an instance of [[DebugFormatter]],
    /// which has a [[fmt::Display]] implementation that uses [[DebugFormatting::fmt]] under the hood.
    ///
    /// # Example
    ///
    /// ```
    /// use std::fmt;
    /// use flotsync_utils::debugging::DebugFormatting;
    ///
    /// struct TestValue(usize);
    /// impl DebugFormatting for TestValue {
    ///   fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
    ///     write!(f, "TV({})", self.0)
    ///   }
    /// }
    ///
    /// let v = TestValue(42);
    /// assert_eq!(format!("{}", v.debug_fmt()), "TV(42)");
    /// ```
    fn debug_fmt(&self) -> DebugFormatter<'_, Self> {
        DebugFormatter(self)
    }
}

/// A convenient wrapper to use [[DebugFormatting]] in string formatting.
///
/// See [[DebugFormatting::debug_fmt]] for more info.
pub struct DebugFormatter<'a, T>(&'a T)
where
    T: ?Sized;

impl<'a, T> fmt::Display for DebugFormatter<'a, T>
where
    T: DebugFormatting + ?Sized,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        DebugFormatting::fmt(self.0, f)
    }
}
