//! Values indexed by a key projected from each value.
//!
//! [`ProjectedHashMap`] stores values in arbitrary hash-table order, while
//! [`ProjectedIndexHashMap`] retains input order and supports positional access.

use iddqd::{IdHashItem, IdHashMap};
use snafu::Snafu;
use std::{
    collections::hash_map::RandomState,
    fmt,
    hash::{BuildHasher, Hash},
    iter::FusedIterator,
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

use crate::NonOwningPhantomData;

pub use crate::projected_index_hash_map::{DuplicateProjectedIndexKeyError, ProjectedIndexHashMap};

/// Projects the lookup key stored within a value.
///
/// The projected key must not change while its value is indexed. Projected map
/// types maintain that invariant by not exposing unrestricted mutable access to
/// their values.
pub trait KeyProjection<Value> {
    /// Borrowed key representation accepted by lookup.
    type Key: ?Sized + Eq + Hash;

    /// Borrow the lookup key stored in `value`.
    fn key(value: &Value) -> &Self::Key;
}

/// A value rejected because its projected key is already present.
#[derive(Debug, Snafu)]
#[snafu(display("A value with the same projected key is already present."))]
pub struct DuplicateProjectedKeyError<Value> {
    /// Rejected value, retained so the caller can recover it.
    value: Value,
}

impl<Value> DuplicateProjectedKeyError<Value> {
    /// Borrow the rejected value.
    #[must_use]
    pub fn value(&self) -> &Value {
        &self.value
    }

    /// Recover the rejected value.
    #[must_use]
    pub fn into_value(self) -> Value {
        self.value
    }
}

/// Unordered values with expected-constant lookup by a projected key.
///
/// Keys are borrowed directly from their values and are therefore not stored
/// or allocated separately. Iteration order is arbitrary.
pub struct ProjectedHashMap<Value, Projection, HashBuilder = RandomState>
where
    Projection: KeyProjection<Value>,
{
    /// Representation hidden behind the Flotsync collection contract.
    inner: IdHashMap<ProjectedValue<Value, Projection>, HashBuilder>,
}

impl<Value, Projection> ProjectedHashMap<Value, Projection>
where
    Projection: KeyProjection<Value>,
{
    /// Create an empty map using the default hash builder.
    #[must_use]
    pub fn new() -> Self {
        Self::with_hasher(RandomState::new())
    }

    /// Create an empty map with space for at least `capacity` values.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_and_hasher(capacity, RandomState::new())
    }

    /// Build a map from values using the default hash builder.
    ///
    /// # Errors
    ///
    /// Returns [`DuplicateProjectedKeyError`] when two values project to equal
    /// keys.
    pub fn try_from_values(
        values: impl IntoIterator<Item = Value>,
    ) -> Result<Self, DuplicateProjectedKeyError<Value>> {
        Self::try_from_values_with_hasher(values, RandomState::new())
    }
}

impl<Value, Projection> Default for ProjectedHashMap<Value, Projection>
where
    Projection: KeyProjection<Value>,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<Value, Projection, HashBuilder> ProjectedHashMap<Value, Projection, HashBuilder>
where
    Projection: KeyProjection<Value>,
    HashBuilder: BuildHasher + Clone,
{
    /// Create an empty map with an explicit hash builder.
    #[must_use]
    pub fn with_hasher(hash_builder: HashBuilder) -> Self {
        Self {
            inner: IdHashMap::with_hasher(hash_builder),
        }
    }

    /// Create an empty map with capacity and an explicit hash builder.
    #[must_use]
    pub fn with_capacity_and_hasher(capacity: usize, hash_builder: HashBuilder) -> Self {
        Self {
            inner: IdHashMap::with_capacity_and_hasher(capacity, hash_builder),
        }
    }

    /// Build a map from values with an explicit hash builder.
    ///
    /// # Errors
    ///
    /// Returns [`DuplicateProjectedKeyError`] when two values project to equal
    /// keys.
    pub fn try_from_values_with_hasher(
        values: impl IntoIterator<Item = Value>,
        hash_builder: HashBuilder,
    ) -> Result<Self, DuplicateProjectedKeyError<Value>> {
        let values = values.into_iter();
        let mut map = Self::with_capacity_and_hasher(values.size_hint().0, hash_builder);
        for value in values {
            map.insert_unique(value)?;
        }
        Ok(map)
    }

    /// Return true iff no values are stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Return the number of stored values.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Look up a value by its projected key.
    #[must_use]
    pub fn get(&self, key: &Projection::Key) -> Option<&Value> {
        self.inner.get(key).map(ProjectedValue::value)
    }

    /// Mutably borrow a value while protecting its projected key.
    ///
    /// The returned guard checks the projected-key hash when dropped.
    ///
    /// # Panics
    ///
    /// Panics when the guard is dropped if the projected-key hash changed.
    pub fn get_mut(
        &mut self,
        key: &Projection::Key,
    ) -> Option<ProjectedHashMapRefMut<'_, Value, Projection, HashBuilder>> {
        self.inner
            .get_mut(key)
            .map(|inner| ProjectedHashMapRefMut { inner })
    }

    /// Look up a projected key and its value.
    #[must_use]
    pub fn get_key_value(&self, key: &Projection::Key) -> Option<(&Projection::Key, &Value)> {
        self.get(key).map(|value| (Projection::key(value), value))
    }

    /// Return true iff a value with `key` is stored.
    #[must_use]
    pub fn contains_key(&self, key: &Projection::Key) -> bool {
        self.inner.contains_key(key)
    }

    /// Insert `value` only when its projected key is absent.
    ///
    /// # Errors
    ///
    /// Returns [`DuplicateProjectedKeyError`] containing `value` when another
    /// value with the same projected key is already present.
    pub fn insert_unique(&mut self, value: Value) -> Result<(), DuplicateProjectedKeyError<Value>> {
        let value = ProjectedValue::new(value);
        match self.inner.insert_unique(value) {
            Ok(()) => Ok(()),
            Err(error) => {
                let (value, _duplicates) = error.into_parts();
                Err(DuplicateProjectedKeyError {
                    value: value.into_value(),
                })
            }
        }
    }

    /// Insert `value`, replacing and returning the existing value with the same key.
    pub fn insert_overwrite(&mut self, value: Value) -> Option<Value> {
        self.inner
            .insert_overwrite(ProjectedValue::new(value))
            .map(ProjectedValue::into_value)
    }

    /// Remove and return the value with `key`.
    pub fn remove(&mut self, key: &Projection::Key) -> Option<Value> {
        self.inner.remove(key).map(ProjectedValue::into_value)
    }

    /// Iterate over values in arbitrary order.
    pub fn iter(&self) -> ProjectedHashMapIter<'_, Value, Projection> {
        ProjectedHashMapIter {
            inner: self.inner.iter(),
        }
    }

    /// Iterate over projected keys in arbitrary order.
    pub fn keys(&self) -> impl ExactSizeIterator<Item = &Projection::Key> + FusedIterator {
        self.iter().map(Projection::key)
    }

    /// Iterate over values in arbitrary order.
    pub fn values(&self) -> ProjectedHashMapIter<'_, Value, Projection> {
        self.iter()
    }

    /// Consume the map and iterate over its values in arbitrary order.
    #[must_use]
    pub fn into_values(self) -> ProjectedHashMapIntoIter<Value, Projection> {
        ProjectedHashMapIntoIter {
            inner: self.inner.into_iter(),
        }
    }
}

impl<'map, Value, Projection, HashBuilder> IntoIterator
    for &'map ProjectedHashMap<Value, Projection, HashBuilder>
where
    Projection: KeyProjection<Value>,
    HashBuilder: BuildHasher + Clone,
{
    type Item = &'map Value;
    type IntoIter = ProjectedHashMapIter<'map, Value, Projection>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<Value, Projection, HashBuilder> IntoIterator
    for ProjectedHashMap<Value, Projection, HashBuilder>
where
    Projection: KeyProjection<Value>,
    HashBuilder: BuildHasher + Clone,
{
    type Item = Value;
    type IntoIter = ProjectedHashMapIntoIter<Value, Projection>;

    fn into_iter(self) -> Self::IntoIter {
        self.into_values()
    }
}

impl<Value, Projection, HashBuilder> Clone for ProjectedHashMap<Value, Projection, HashBuilder>
where
    Value: Clone,
    Projection: KeyProjection<Value>,
    HashBuilder: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<Value, Projection, HashBuilder> fmt::Debug for ProjectedHashMap<Value, Projection, HashBuilder>
where
    Value: fmt::Debug,
    Projection: KeyProjection<Value>,
    HashBuilder: BuildHasher + Clone,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_set().entries(self.iter()).finish()
    }
}

impl<Value, Projection, HashBuilder> PartialEq for ProjectedHashMap<Value, Projection, HashBuilder>
where
    Value: PartialEq,
    Projection: KeyProjection<Value>,
    HashBuilder: BuildHasher + Clone,
{
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl<Value, Projection, HashBuilder> Eq for ProjectedHashMap<Value, Projection, HashBuilder>
where
    Value: Eq,
    Projection: KeyProjection<Value>,
    HashBuilder: BuildHasher + Clone,
{
}

/// Mutable projected-map value guard that verifies key-hash stability when dropped.
pub struct ProjectedHashMapRefMut<'map, Value, Projection, HashBuilder>
where
    Projection: KeyProjection<Value>,
    HashBuilder: BuildHasher + Clone,
{
    /// Guard supplied by the private projected-map representation.
    inner: iddqd::id_hash_map::RefMut<'map, ProjectedValue<Value, Projection>, HashBuilder>,
}

impl<Value, Projection, HashBuilder> Deref
    for ProjectedHashMapRefMut<'_, Value, Projection, HashBuilder>
where
    Projection: KeyProjection<Value>,
    HashBuilder: BuildHasher + Clone,
{
    type Target = Value;

    fn deref(&self) -> &Self::Target {
        self.inner.value()
    }
}

impl<Value, Projection, HashBuilder> DerefMut
    for ProjectedHashMapRefMut<'_, Value, Projection, HashBuilder>
where
    Projection: KeyProjection<Value>,
    HashBuilder: BuildHasher + Clone,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.value_mut()
    }
}

/// Iterator over unordered projected-map values.
pub struct ProjectedHashMapIter<'map, Value, Projection>
where
    Projection: KeyProjection<Value>,
{
    /// Iterator over private storage adapters.
    inner: iddqd::id_hash_map::Iter<'map, ProjectedValue<Value, Projection>>,
}

impl<'map, Value, Projection> Iterator for ProjectedHashMapIter<'map, Value, Projection>
where
    Projection: KeyProjection<Value>,
{
    type Item = &'map Value;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(ProjectedValue::value)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<Value, Projection> ExactSizeIterator for ProjectedHashMapIter<'_, Value, Projection>
where
    Projection: KeyProjection<Value>,
{
    fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<Value, Projection> FusedIterator for ProjectedHashMapIter<'_, Value, Projection> where
    Projection: KeyProjection<Value>
{
}

/// Owning iterator over unordered projected-map values.
pub struct ProjectedHashMapIntoIter<Value, Projection>
where
    Projection: KeyProjection<Value>,
{
    /// Iterator over private storage adapters.
    inner: iddqd::id_hash_map::IntoIter<ProjectedValue<Value, Projection>>,
}

impl<Value, Projection> Iterator for ProjectedHashMapIntoIter<Value, Projection>
where
    Projection: KeyProjection<Value>,
{
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(ProjectedValue::into_value)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<Value, Projection> ExactSizeIterator for ProjectedHashMapIntoIter<Value, Projection>
where
    Projection: KeyProjection<Value>,
{
    fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<Value, Projection> FusedIterator for ProjectedHashMapIntoIter<Value, Projection> where
    Projection: KeyProjection<Value>
{
}

/// Private adapter connecting [`KeyProjection`] to [`IdHashItem`].
struct ProjectedValue<Value, Projection> {
    /// Stored user value.
    value: Value,
    /// Projection policy, represented only at the type level.
    projection: NonOwningPhantomData<Projection>,
}

impl<Value, Projection> ProjectedValue<Value, Projection> {
    /// Wrap a user value for storage.
    fn new(value: Value) -> Self {
        Self {
            value,
            projection: PhantomData,
        }
    }

    /// Borrow the stored user value.
    fn value(&self) -> &Value {
        &self.value
    }

    /// Mutably borrow the stored user value through a key-checking guard.
    fn value_mut(&mut self) -> &mut Value {
        &mut self.value
    }

    /// Unwrap the stored user value.
    fn into_value(self) -> Value {
        self.value
    }
}

impl<Value, Projection> IdHashItem for ProjectedValue<Value, Projection>
where
    Projection: KeyProjection<Value>,
{
    type Key<'value>
        = &'value Projection::Key
    where
        Self: 'value;

    fn key(&self) -> Self::Key<'_> {
        Projection::key(&self.value)
    }

    fn upcast_key<'short, 'long: 'short>(long: Self::Key<'long>) -> Self::Key<'short>
    where
        Self: 'long,
    {
        long
    }
}

impl<Value, Projection> Clone for ProjectedValue<Value, Projection>
where
    Value: Clone,
{
    fn clone(&self) -> Self {
        Self::new(self.value.clone())
    }
}

impl<Value, Projection> PartialEq for ProjectedValue<Value, Projection>
where
    Value: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<Value, Projection> Eq for ProjectedValue<Value, Projection> where Value: Eq {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::{BuildHasherDefault, Hasher};

    struct NameProjection;

    impl KeyProjection<(String, usize)> for NameProjection {
        type Key = str;

        fn key(value: &(String, usize)) -> &Self::Key {
            &value.0
        }
    }

    #[derive(Clone, Default)]
    struct CollisionHasher;

    impl Hasher for CollisionHasher {
        fn finish(&self) -> u64 {
            0
        }

        fn write(&mut self, _bytes: &[u8]) {}
    }

    #[test]
    fn stores_and_looks_up_values_without_separate_keys() {
        let map = ProjectedHashMap::<_, NameProjection>::try_from_values([
            ("beta".to_owned(), 2),
            ("alpha".to_owned(), 1),
        ])
        .unwrap();

        assert_eq!(map.get("alpha"), Some(&("alpha".to_owned(), 1)));
        assert!(map.contains_key("beta"));
        assert_eq!(map.get("missing"), None);
    }

    #[test]
    fn rejects_duplicate_projected_keys_and_returns_the_value() {
        let mut map = ProjectedHashMap::<_, NameProjection>::new();
        map.insert_unique(("same".to_owned(), 1)).unwrap();
        let error = map
            .insert_unique(("same".to_owned(), 2))
            .expect_err("duplicate projected key should be rejected");

        assert_eq!(error.into_value(), ("same".to_owned(), 2));
        assert_eq!(map.get("same"), Some(&("same".to_owned(), 1)));
    }

    #[test]
    fn overwrite_and_remove_return_previous_values() {
        let mut map = ProjectedHashMap::<_, NameProjection>::new();
        assert_eq!(map.insert_overwrite(("same".to_owned(), 1)), None);
        assert_eq!(
            map.insert_overwrite(("same".to_owned(), 2)),
            Some(("same".to_owned(), 1))
        );
        assert_eq!(map.remove("same"), Some(("same".to_owned(), 2)));
        assert!(map.is_empty());
    }

    #[test]
    fn mutable_access_preserves_non_key_changes() {
        let mut map =
            ProjectedHashMap::<_, NameProjection>::try_from_values([("alpha".to_owned(), 1)])
                .unwrap();

        map.get_mut("alpha").unwrap().1 = 2;

        assert_eq!(map.get("alpha"), Some(&("alpha".to_owned(), 2)));
    }

    #[test]
    #[should_panic(expected = "key changed during RefMut borrow")]
    fn mutable_access_rejects_projected_key_changes() {
        let mut map =
            ProjectedHashMap::<_, NameProjection>::try_from_values([("alpha".to_owned(), 1)])
                .unwrap();

        map.get_mut("alpha").unwrap().0 = "changed".to_owned();
    }

    #[test]
    fn equality_ignores_iteration_order() {
        let first = ProjectedHashMap::<_, NameProjection>::try_from_values([
            ("alpha".to_owned(), 1),
            ("beta".to_owned(), 2),
        ])
        .unwrap();
        let second = ProjectedHashMap::<_, NameProjection>::try_from_values([
            ("beta".to_owned(), 2),
            ("alpha".to_owned(), 1),
        ])
        .unwrap();

        assert_eq!(first, second);
    }

    #[test]
    fn resolves_hash_collisions_with_projected_key_equality() {
        let hasher = BuildHasherDefault::<CollisionHasher>::default();
        let map = ProjectedHashMap::<_, NameProjection, _>::try_from_values_with_hasher(
            [("alpha".to_owned(), 1), ("beta".to_owned(), 2)],
            hasher,
        )
        .unwrap();

        assert_eq!(map.get("alpha"), Some(&("alpha".to_owned(), 1)));
        assert_eq!(map.get("beta"), Some(&("beta".to_owned(), 2)));
    }

    #[test]
    fn iterators_hide_the_internal_adapter() {
        let map = ProjectedHashMap::<_, NameProjection>::try_from_values([
            ("alpha".to_owned(), 1),
            ("beta".to_owned(), 2),
        ])
        .unwrap();
        let borrowed_values = map.iter().cloned().collect::<Vec<_>>();
        let owned_values = map.into_values().collect::<Vec<_>>();

        assert_eq!(borrowed_values.len(), 2);
        assert_eq!(owned_values.len(), 2);
        for expected in borrowed_values {
            assert!(owned_values.contains(&expected));
        }
    }
}
