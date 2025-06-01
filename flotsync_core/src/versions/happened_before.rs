use std::cmp;

/// Establishes the "happened-before" order.
///
/// This is a form of partial order, but an additional variant of incomparable is "concurrent".
///
/// Otherwise it just has better naming to clarify the implication.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum HappenedBeforeOrdering {
    /// `a` happened strictly before `b`.
    Before,
    /// `a == b` (exactly at the same time).
    Equal,
    /// `a` happened strictly after `b`.
    After,
    /// `a` and `b` are concurrent.
    Concurrent,
    /// `a` and `b` are incomparable (e.g. due to different vector sizes).
    Incomparable,
}
impl HappenedBeforeOrdering {
    /// Reverses the ordering.
    ///
    /// - `Before` becomes `After`.
    /// - `After` becomes `Before`.
    /// - Everything else stays the same.
    pub const fn reverse(self) -> HappenedBeforeOrdering {
        match self {
            HappenedBeforeOrdering::Before => HappenedBeforeOrdering::After,
            HappenedBeforeOrdering::After => HappenedBeforeOrdering::Before,
            _ => self,
        }
    }
}

impl From<cmp::Ordering> for HappenedBeforeOrdering {
    fn from(value: cmp::Ordering) -> Self {
        match value {
            cmp::Ordering::Less => HappenedBeforeOrdering::Before,
            cmp::Ordering::Equal => HappenedBeforeOrdering::Equal,
            cmp::Ordering::Greater => HappenedBeforeOrdering::After,
        }
    }
}

impl From<HappenedBeforeOrdering> for Option<cmp::Ordering> {
    fn from(val: HappenedBeforeOrdering) -> Self {
        match val {
            HappenedBeforeOrdering::Before => Some(cmp::Ordering::Less),
            HappenedBeforeOrdering::Equal => Some(cmp::Ordering::Equal),
            HappenedBeforeOrdering::After => Some(cmp::Ordering::Greater),
            HappenedBeforeOrdering::Concurrent => None,
            HappenedBeforeOrdering::Incomparable => None,
        }
    }
}

/// Trait for types that can establish a [happened-before order](HappenedBeforeOrdering).
///
/// This is a form of partial order, so the same rules as [[PartialOrd]] apply, but an additional variants of incomparable is "concurrent".
pub trait HappenedBeforeOrd<Rhs = Self>: PartialEq<Rhs>
where
    Rhs: ?Sized,
{
    fn hb_cmp(&self, other: &Rhs) -> HappenedBeforeOrdering;

    /// Get something that can be used to compare using [[PartialOrd]] for this instance.
    fn ord(&self) -> HappenedBeforePartialOrdWrapper<Self> {
        HappenedBeforePartialOrdWrapper(self)
    }
}

/// A wrapper that allows [[HappenedBeforeOrd]] types to be treated as [[PartialOrd]].
///
/// This is just a workaround for the orphan rules.
pub struct HappenedBeforePartialOrdWrapper<'a, T: ?Sized>(&'a T);

impl<T> PartialEq for HappenedBeforePartialOrdWrapper<'_, T>
where
    T: HappenedBeforeOrd + ?Sized,
{
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<T> PartialOrd for HappenedBeforePartialOrdWrapper<'_, T>
where
    T: HappenedBeforeOrd + ?Sized,
{
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        self.0.hb_cmp(other.0).into()
    }
}
