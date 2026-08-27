//! Map with entries stored contiguously in sorted order.

use flotsync_utils::NonOwningPhantomData;
use std::{borrow::Borrow, cmp::Ordering, error::Error, fmt, marker::PhantomData, mem, ops::Deref};

/// Ordering policy for keys stored in a [`SortedArrayMap`].
///
/// Implementations must define a total order. Two stored keys are duplicates
/// exactly when [`Self::compare`] returns [`Ordering::Equal`] for their borrowed
/// lookup keys. The comparison must remain stable for the lifetime of every map
/// using the policy.
pub trait SortOrder<K> {
    /// Borrowed key representation accepted by map lookup.
    type LookupKey: ?Sized;

    /// Compare two borrowed key representations in storage order.
    fn compare(left: &Self::LookupKey, right: &Self::LookupKey) -> Ordering;
}

/// Default [`SortOrder`] which uses the key's natural [`Ord`] implementation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NaturalOrder;

impl<K: Ord> SortOrder<K> for NaturalOrder {
    type LookupKey = K;

    fn compare(left: &Self::LookupKey, right: &Self::LookupKey) -> Ordering {
        left.cmp(right)
    }
}

/// Duplicate key rejected while constructing a [`SortedArrayMap`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DuplicateKeyError<K> {
    /// One of the keys which compared equal in the supplied entries.
    key: K,
}

impl<K> DuplicateKeyError<K> {
    /// Return the rejected duplicate key.
    #[must_use]
    pub fn key(&self) -> &K {
        &self.key
    }

    /// Consume the error and return the rejected duplicate key.
    #[must_use]
    pub fn into_key(self) -> K {
        self.key
    }
}

impl<K: fmt::Debug> fmt::Display for DuplicateKeyError<K> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Duplicate sorted-array-map key: {:?}.", self.key)
    }
}

impl<K: fmt::Debug> Error for DuplicateKeyError<K> {}

/// Map whose entries are stored contiguously and sorted by the configured key order.
pub struct SortedArrayMap<K, V, O = NaturalOrder> {
    /// Entries kept unique and in configured key order.
    entries: Vec<(K, V)>,
    /// Ordering is fixed by its type and occupies no space in each map value.
    order: NonOwningPhantomData<O>,
}

impl<K, V, O> SortedArrayMap<K, V, O> {
    /// Construct an empty map without allocating.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
            order: PhantomData,
        }
    }

    /// Construct a map containing exactly one entry.
    #[must_use]
    pub fn from_entry(key: K, value: V) -> Self {
        Self {
            entries: vec![(key, value)],
            order: PhantomData,
        }
    }

    /// Return true iff the map contains no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Return the number of entries in the map.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Iterate over entries in the configured key order.
    #[must_use]
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&K, &V)> + DoubleEndedIterator + '_ {
        self.entries.iter().map(|(key, value)| (key, value))
    }

    /// Return the entries contiguously in configured key order.
    #[must_use]
    pub fn as_slice(&self) -> &[(K, V)] {
        &self.entries
    }
}

impl<K, V, O> SortedArrayMap<K, V, O>
where
    O: SortOrder<K>,
    K: Borrow<O::LookupKey>,
{
    /// Build a map from entries supplied in any order.
    ///
    /// # Errors
    ///
    /// Returns [`DuplicateKeyError`] when two supplied keys compare equal under
    /// `O`.
    pub fn try_from_entries(
        entries: impl IntoIterator<Item = (K, V)>,
    ) -> Result<Self, DuplicateKeyError<K>> {
        let mut entries = entries.into_iter().collect::<Vec<_>>();
        entries.sort_unstable_by(|(left, _), (right, _)| Self::compare_keys(left, right));

        let duplicate_index = entries
            .windows(2)
            .position(|window| Self::compare_keys(&window[0].0, &window[1].0) == Ordering::Equal);
        if let Some(duplicate_index) = duplicate_index {
            // Since we are about to throw away the entries anyway,
            // we can just as well mess up the ordering again with swap_remove.
            let (key, _) = entries.swap_remove(duplicate_index);
            return Err(DuplicateKeyError { key });
        }

        Ok(Self {
            entries,
            order: PhantomData,
        })
    }

    /// Return the value stored for `key`, if present.
    #[must_use]
    pub fn get(&self, key: &O::LookupKey) -> Option<&V> {
        let index = self
            .entries
            .binary_search_by(|(candidate, _)| O::compare(candidate.borrow(), key))
            .ok()?;
        Some(&self.entries[index].1)
    }

    /// Return the value stored for `key` mutably, if present.
    #[must_use]
    pub fn get_mut(&mut self, key: &O::LookupKey) -> Option<&mut V> {
        let index = self
            .entries
            .binary_search_by(|(candidate, _)| O::compare(candidate.borrow(), key))
            .ok()?;
        Some(&mut self.entries[index].1)
    }

    /// Insert one entry while retaining the configured storage order.
    ///
    /// When an equal key is already stored, that key is retained and only its
    /// value is replaced. Returns the replaced value, or `None` when a new entry
    /// was inserted.
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let insertion = self
            .entries
            .binary_search_by(|(candidate, _)| Self::compare_keys(candidate, &key));
        match insertion {
            Ok(index) => Some(mem::replace(&mut self.entries[index].1, value)),
            Err(index) => {
                self.entries.insert(index, (key, value));
                None
            }
        }
    }

    /// Compare two owned keys through their borrowed lookup representation.
    fn compare_keys(left: &K, right: &K) -> Ordering {
        O::compare(left.borrow(), right.borrow())
    }
}

impl<K: Clone, V: Clone, O> Clone for SortedArrayMap<K, V, O> {
    fn clone(&self) -> Self {
        Self {
            entries: self.entries.clone(),
            order: PhantomData,
        }
    }
}

impl<K, V, O> AsRef<[(K, V)]> for SortedArrayMap<K, V, O> {
    fn as_ref(&self) -> &[(K, V)] {
        self.as_slice()
    }
}

impl<K, V, O> Deref for SortedArrayMap<K, V, O> {
    type Target = [(K, V)];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<K: fmt::Debug, V: fmt::Debug, O> fmt::Debug for SortedArrayMap<K, V, O> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_map().entries(self.iter()).finish()
    }
}

impl<K: PartialEq, V: PartialEq, O> PartialEq for SortedArrayMap<K, V, O> {
    fn eq(&self, other: &Self) -> bool {
        self.entries == other.entries
    }
}

impl<K: Eq, V: Eq, O> Eq for SortedArrayMap<K, V, O> {}

impl<K, V, O> Default for SortedArrayMap<K, V, O> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Length-first string ordering used to exercise borrowed custom lookup.
    struct LengthThenLexical;

    impl SortOrder<&'static str> for LengthThenLexical {
        type LookupKey = str;

        fn compare(left: &Self::LookupKey, right: &Self::LookupKey) -> Ordering {
            left.len().cmp(&right.len()).then_with(|| left.cmp(right))
        }
    }

    #[test]
    fn empty_and_singleton_maps_report_their_entries() {
        let empty = SortedArrayMap::<u32, &'static str>::new();
        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);
        assert_eq!(empty.iter().len(), 0);
        assert_eq!(empty.get(&7), None);
        assert_eq!(empty.as_slice(), &[]);

        let singleton = SortedArrayMap::<u32, &'static str>::from_entry(7, "seven");
        assert!(!singleton.is_empty());
        assert_eq!(singleton.len(), 1);
        assert_eq!(singleton.get(&7), Some(&"seven"));
        assert_eq!(singleton.as_ref(), &[(7, "seven")]);
        assert_eq!(singleton.first(), Some(&(7, "seven")));
    }

    #[test]
    fn bulk_construction_sorts_entries_and_rejects_duplicates() {
        let map = SortedArrayMap::<u32, &'static str>::try_from_entries([
            (30, "thirty"),
            (10, "ten"),
            (20, "twenty"),
        ])
        .expect("unique entries should build");
        assert_eq!(
            map.iter()
                .map(|(key, value)| (*key, *value))
                .collect::<Vec<_>>(),
            vec![(10, "ten"), (20, "twenty"), (30, "thirty")]
        );

        let duplicate = SortedArrayMap::<u32, _>::try_from_entries([
            (20, "first"),
            (10, "ten"),
            (20, "second"),
        ])
        .expect_err("duplicate key should fail");
        assert_eq!(duplicate.into_key(), 20);
    }

    #[test]
    fn custom_order_supports_a_distinct_borrowed_lookup_key() {
        let map = SortedArrayMap::<&'static str, u32, LengthThenLexical>::try_from_entries([
            ("long", 4),
            ("b", 2),
            ("aa", 3),
            ("a", 1),
        ])
        .expect("unique strings should build");
        assert_eq!(
            map.iter().map(|(key, _)| *key).collect::<Vec<_>>(),
            vec!["a", "b", "aa", "long"]
        );
        assert_eq!(map.get("aa"), Some(&3));
        assert_eq!(map.get("missing"), None);
    }

    #[test]
    fn insert_places_new_keys_and_replaces_existing_values() {
        let mut map = SortedArrayMap::<u32, &'static str>::new();
        assert_eq!(map.insert(20, "twenty"), None);
        assert_eq!(map.insert(10, "ten"), None);
        assert_eq!(map.insert(30, "thirty"), None);
        *map.get_mut(&30).expect("inserted key should be mutable") = "THIRTY";
        assert_eq!(map.insert(20, "TWENTY"), Some("twenty"));
        assert_eq!(
            map.iter()
                .map(|(key, value)| (*key, *value))
                .collect::<Vec<_>>(),
            vec![(10, "ten"), (20, "TWENTY"), (30, "THIRTY")]
        );
        assert_eq!(map.get_mut(&40), None);
    }

    #[test]
    fn clone_equality_and_debug_follow_the_canonical_entries() {
        let first = SortedArrayMap::<u32, &'static str>::try_from_entries([(2, "two"), (1, "one")])
            .expect("first map should build");
        let second = first.clone();

        assert_eq!(first, second);
        assert_eq!(format!("{first:?}"), "{1: \"one\", 2: \"two\"}");
    }
}
