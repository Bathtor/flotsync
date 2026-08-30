//! Contiguous values indexed by a key projected from each value.

use crate::{KeyProjection, NonOwningPhantomData};
use hashbrown::HashTable;
use snafu::Snafu;
use std::{collections::hash_map::RandomState, hash::BuildHasher, marker::PhantomData, slice};

/// Duplicate projected key rejected while constructing a [`ProjectedIndexHashMap`].
#[derive(Clone, Debug, PartialEq, Eq, Snafu)]
#[snafu(display(
    "Values at indices {first_index} and {duplicate_index} have the same projected key."
))]
pub struct DuplicateProjectedIndexKeyError {
    /// Index of the first value with the projected key.
    pub first_index: usize,
    /// Index of the later value with the same projected key.
    pub duplicate_index: usize,
}

/// Contiguous values with expected-constant lookup by a projected key.
///
/// Values retain their input order. The hash table stores only indices into the
/// boxed value slice, so a key that is already part of a larger value is not
/// stored or allocated again.
pub struct ProjectedIndexHashMap<Value, Projection, HashBuilder = RandomState> {
    /// Values in their input order.
    values: Box<[Value]>,
    /// Indices into `values`, hashed by each value's projected key.
    index: HashTable<usize>,
    /// Hash builder shared by construction and lookup.
    hash_builder: HashBuilder,
    /// Projection policy, represented only at the type level.
    projection: NonOwningPhantomData<Projection>,
}

impl<Value, Projection> ProjectedIndexHashMap<Value, Projection>
where
    Projection: KeyProjection<Value>,
{
    /// Build an indexed value collection using the default hash builder.
    ///
    /// # Errors
    ///
    /// Returns [`DuplicateProjectedIndexKeyError`] when two values project to
    /// equal keys.
    pub fn try_from_values(
        values: impl IntoIterator<Item = Value>,
    ) -> Result<Self, DuplicateProjectedIndexKeyError> {
        Self::try_from_values_with_hasher(values, RandomState::new())
    }

    /// Build an indexed value collection without checking projected-key uniqueness.
    ///
    /// Every value must project to a unique key. If this contract is violated,
    /// lookup behaviour for duplicate keys is unspecified.
    #[must_use]
    pub fn from_values_unchecked(values: impl IntoIterator<Item = Value>) -> Self {
        Self::from_values_unchecked_with_hasher(values, RandomState::new())
    }
}

impl<Value, Projection, HashBuilder> ProjectedIndexHashMap<Value, Projection, HashBuilder> {
    /// Return true iff no values are stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Return the number of stored values.
    #[must_use]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Return all values contiguously in their input order.
    #[must_use]
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// Iterate over values in their input order.
    pub fn iter(&self) -> slice::Iter<'_, Value> {
        self.values.iter()
    }

    /// Return the value at `index`.
    #[must_use]
    pub fn get_index(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    /// Consume the map and return its values in their input order.
    #[must_use]
    pub fn into_values(self) -> Box<[Value]> {
        self.values
    }
}

impl<'map, Value, Projection, HashBuilder> IntoIterator
    for &'map ProjectedIndexHashMap<Value, Projection, HashBuilder>
{
    type Item = &'map Value;
    type IntoIter = slice::Iter<'map, Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<Value, Projection, HashBuilder> ProjectedIndexHashMap<Value, Projection, HashBuilder>
where
    Projection: KeyProjection<Value>,
    HashBuilder: BuildHasher,
{
    /// Build an indexed value collection with an explicit hash builder.
    ///
    /// Hash collisions do not affect projected-key correctness.
    ///
    /// # Errors
    ///
    /// Returns [`DuplicateProjectedIndexKeyError`] when two values project to
    /// equal keys.
    pub fn try_from_values_with_hasher(
        values: impl IntoIterator<Item = Value>,
        hash_builder: HashBuilder,
    ) -> Result<Self, DuplicateProjectedIndexKeyError> {
        let values = values.into_iter().collect::<Box<[_]>>();
        let mut index_by_key = HashTable::with_capacity(values.len());

        for (index, value) in values.iter().enumerate() {
            let key = Projection::key(value);
            let hash = hash_builder.hash_one(key);
            let first_index = index_by_key.find(hash, |&candidate_index| {
                Projection::key(&values[candidate_index]) == key
            });
            if let Some(&first_index) = first_index {
                return Err(DuplicateProjectedIndexKeyError {
                    first_index,
                    duplicate_index: index,
                });
            }

            index_by_key.insert_unique(hash, index, |&candidate_index| {
                hash_builder.hash_one(Projection::key(&values[candidate_index]))
            });
        }

        Ok(Self {
            values,
            index: index_by_key,
            hash_builder,
            projection: PhantomData,
        })
    }

    /// Build an indexed value collection without checking projected-key uniqueness.
    ///
    /// Every value must project to a unique key. If this contract is violated,
    /// lookup behaviour for duplicate keys is unspecified.
    #[must_use]
    pub fn from_values_unchecked_with_hasher(
        values: impl IntoIterator<Item = Value>,
        hash_builder: HashBuilder,
    ) -> Self {
        let values = values.into_iter().collect::<Box<[_]>>();
        let mut index_by_key = HashTable::with_capacity(values.len());

        for (index, value) in values.iter().enumerate() {
            let hash = hash_builder.hash_one(Projection::key(value));
            index_by_key.insert_unique(hash, index, |&candidate_index| {
                hash_builder.hash_one(Projection::key(&values[candidate_index]))
            });
        }

        Self {
            values,
            index: index_by_key,
            hash_builder,
            projection: PhantomData,
        }
    }

    /// Look up a value by its projected key.
    #[must_use]
    pub fn get(&self, key: &Projection::Key) -> Option<&Value> {
        self.get_index_of(key).map(|index| &self.values[index])
    }

    /// Return the contiguous value index associated with `key`.
    #[must_use]
    pub fn get_index_of(&self, key: &Projection::Key) -> Option<usize> {
        let hash = self.hash_builder.hash_one(key);
        self.index
            .find(hash, |&index| Projection::key(&self.values[index]) == key)
            .copied()
    }
}

impl<Value, Projection, HashBuilder> Clone for ProjectedIndexHashMap<Value, Projection, HashBuilder>
where
    Value: Clone,
    HashBuilder: Clone,
{
    fn clone(&self) -> Self {
        Self {
            values: self.values.clone(),
            index: self.index.clone(),
            hash_builder: self.hash_builder.clone(),
            projection: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        hash::{BuildHasherDefault, Hasher},
        iter,
    };

    struct NameProjection;

    impl KeyProjection<(String, usize)> for NameProjection {
        type Key = str;

        fn key(value: &(String, usize)) -> &Self::Key {
            &value.0
        }
    }

    #[derive(Default)]
    struct CollisionHasher;

    impl Hasher for CollisionHasher {
        fn finish(&self) -> u64 {
            0
        }

        fn write(&mut self, _bytes: &[u8]) {}
    }

    #[test]
    fn indexes_projected_keys_without_reordering_values() {
        let values = vec![("beta".to_owned(), 2), ("alpha".to_owned(), 1)];
        let map = ProjectedIndexHashMap::<_, NameProjection>::try_from_values(values).unwrap();

        assert_eq!(map.values()[0], ("beta".to_owned(), 2));
        assert_eq!(map.get("alpha"), Some(&("alpha".to_owned(), 1)));
        assert_eq!(map.get_index_of("alpha"), Some(1));
        assert_eq!(map.get_index(0), Some(&("beta".to_owned(), 2)));
        assert_eq!(map.get("missing"), None);
    }

    #[test]
    fn rejects_duplicate_projected_keys() {
        let values = vec![("same".to_owned(), 1), ("same".to_owned(), 2)];
        let Err(error) = ProjectedIndexHashMap::<_, NameProjection>::try_from_values(values) else {
            panic!("duplicate projected keys should be rejected");
        };

        assert_eq!(error.first_index, 0);
        assert_eq!(error.duplicate_index, 1);
    }

    #[test]
    fn unchecked_construction_indexes_unique_values() {
        let values = vec![("beta".to_owned(), 2), ("alpha".to_owned(), 1)];
        let map = ProjectedIndexHashMap::<_, NameProjection>::from_values_unchecked(values);

        assert_eq!(map.get("alpha"), Some(&("alpha".to_owned(), 1)));
        assert_eq!(map.get_index_of("beta"), Some(0));
    }

    #[test]
    fn resolves_hash_collisions_with_projected_key_equality() {
        let values = vec![("alpha".to_owned(), 1), ("beta".to_owned(), 2)];
        let hasher = BuildHasherDefault::<CollisionHasher>::default();
        let map = ProjectedIndexHashMap::<_, NameProjection, _>::try_from_values_with_hasher(
            values, hasher,
        )
        .unwrap();

        assert_eq!(map.get("alpha"), Some(&("alpha".to_owned(), 1)));
        assert_eq!(map.get("beta"), Some(&("beta".to_owned(), 2)));
    }

    #[test]
    fn supports_empty_iterators() {
        let map = ProjectedIndexHashMap::<(String, usize), NameProjection>::try_from_values(
            iter::empty(),
        )
        .unwrap();

        assert!(map.is_empty());
        assert_eq!(map.len(), 0);
        assert!(map.iter().next().is_none());
        assert!(map.into_values().is_empty());
    }
}
