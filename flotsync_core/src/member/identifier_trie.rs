//! [[`TrieMap`]] and [[`TrieSet`]] offer more compressed memory layout for larger groups by re-using parent identifiers.
//! Their primary traversal API lends identifier views from the traversal path instead of
//! materialising an owned identifier for every key.

use crate::member::{Identifier, IdentifierLike, IdentifierRef, IdentifierSegment};
use ahash::AHashMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrieMap<V> {
    root: TrieNode<V>,
}

impl<V> TrieMap<V> {
    #[must_use]
    pub fn new() -> Self {
        TrieMap {
            root: TrieNode {
                value: None,
                children: AHashMap::new(),
            },
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.root.children.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.root.count()
    }

    pub fn insert(&mut self, key: Identifier, value: V) -> Option<V> {
        let mut node = &mut self.root;
        for segment in key.into_segments() {
            node = node.children.entry(segment).or_insert_with(|| TrieNode {
                value: None,
                children: AHashMap::new(),
            });
        }
        node.value.replace(value)
    }

    #[must_use]
    pub fn get<I: IdentifierLike>(&self, key: &I) -> Option<&V> {
        let mut node = &self.root;
        for segment in key.segments() {
            node = node.children.get(segment)?;
        }
        node.value.as_ref()
    }

    pub fn remove<I: IdentifierLike>(&mut self, key: &I) -> Option<V> {
        let removed = Self::remove_from_node(&mut self.root, key.segments());
        debug_assert!(
            self.root.value.is_none(),
            "trie root must stay valueless after keyed removal"
        );
        removed
    }

    #[allow(dead_code)] // for later
    #[must_use]
    pub fn get_mut<I: IdentifierLike>(&mut self, key: &I) -> Option<&mut V> {
        let mut node = &mut self.root;
        for segment in key.segments() {
            node = node.children.get_mut(segment)?;
        }
        node.value.as_mut()
    }

    /// Traverse all entries with keys borrowed from the iterator's reusable path buffer.
    ///
    /// This is a lending iterator: each [`IdentifierRef`] returned by [`TrieEntries::next`]
    /// borrows from the iterator itself and must not outlive that call's mutable borrow. Use
    /// [`Self::owned_entries`] when standard [`Iterator`] combinators are more important than
    /// avoiding per-entry key materialisation.
    #[must_use]
    pub fn entries(&self) -> TrieEntries<'_, V> {
        TrieEntries {
            stack: vec![(&self.root, self.root.children.iter())],
            path: Vec::new(),
            yield_self: true,
            current: Some(&self.root),
        }
    }

    /// Traverse all keys with key views borrowed from the iterator's reusable path buffer.
    ///
    /// This has the same lending lifetime constraints as [`Self::entries`].
    #[must_use]
    pub fn keys(&self) -> TrieKeys<'_, V> {
        TrieKeys {
            entries: self.entries(),
        }
    }

    /// Traverse all entries as a standard [`Iterator`] by materialising an owned key per entry.
    ///
    /// This adapter exists for call sites that benefit from ordinary iterator combinators. Prefer
    /// [`Self::entries`] when the key only needs to be inspected during traversal.
    #[must_use]
    pub fn owned_entries(&self) -> TrieOwnedEntries<'_, V> {
        TrieOwnedEntries {
            entries: self.entries(),
        }
    }

    /// Traverse all keys as a standard [`Iterator`] by materialising an owned key per entry.
    ///
    /// Prefer [`Self::keys`] when the key only needs to be inspected during traversal.
    #[must_use]
    pub fn owned_keys(&self) -> TrieOwnedKeys<'_, V> {
        TrieOwnedKeys {
            entries: self.owned_entries(),
        }
    }

    /// Build a new trie with the same keys and mapped values.
    #[must_use]
    pub fn map_values<U>(&self, mut mapper: impl FnMut(&V) -> U) -> TrieMap<U> {
        let mut output = TrieMap::new();
        let mut entries = self.entries();
        while let Some((key, value)) = entries.next() {
            output.insert(key.to_owned(), mapper(value));
        }
        output
    }

    fn remove_from_node<'a>(
        node: &mut TrieNode<V>,
        mut path: impl Iterator<Item = &'a IdentifierSegment>,
    ) -> Option<V> {
        if let Some(segment) = path.next() {
            let child = node.children.get_mut(segment)?;
            let removed = Self::remove_from_node(child, path);
            if child.value.is_none() && child.children.is_empty() {
                node.children.remove(segment);
            }
            removed
        } else {
            node.value.take()
        }
    }
}
impl<V> Default for TrieMap<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V> FromIterator<(Identifier, V)> for TrieMap<V> {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (Identifier, V)>,
    {
        let mut map = TrieMap::new();
        for (id, value) in iter {
            map.insert(id, value);
        }
        map
    }
}

pub struct TrieEntries<'a, V> {
    /// DFS stack of nodes and their partially consumed child iterators.
    stack: Vec<(&'a TrieNode<V>, ChildrenIterator<'a, V>)>,
    /// Reusable root-to-current-node path lent as [`IdentifierRef`] values.
    path: Vec<&'a IdentifierSegment>,
    /// Whether `current` should be yielded before descending further.
    yield_self: bool,
    /// Node corresponding to `path` while `yield_self` is set.
    current: Option<&'a TrieNode<V>>,
}

impl<'a, V> TrieEntries<'a, V> {
    /// Advance to the next entry.
    ///
    /// The returned key view borrows the iterator's internal path buffer. Materialise it with
    /// [`IdentifierRef::to_owned`] if it needs to be kept after the next call to this method.
    pub fn next<'next>(&'next mut self) -> Option<(IdentifierRef<'next>, &'a V)> {
        loop {
            if self.yield_self {
                self.yield_self = false;
                if let Some(node) = self.current
                    && let Some(ref value) = node.value
                {
                    return Some((IdentifierRef::from_segments_unchecked(&self.path), value));
                }
            }

            if let Some((node, mut children)) = self.stack.pop() {
                match children.next() {
                    Some((seg, child)) => {
                        // Put the updated children back before recursing
                        self.stack.push((node, children));
                        self.path.push(seg);
                        self.current = Some(child);
                        self.stack.push((child, child.children.iter()));
                        self.yield_self = true;
                    }
                    None => {
                        self.path.pop();
                    }
                }
            } else {
                return None;
            }
        }
    }
}

pub struct TrieKeys<'a, V> {
    /// Entry traversal reused to expose only the key part.
    entries: TrieEntries<'a, V>,
}

impl<V> TrieKeys<'_, V> {
    /// Advance to the next key.
    ///
    /// The returned key view has the same borrowing constraints as [`TrieEntries::next`].
    pub fn next(&mut self) -> Option<IdentifierRef<'_>> {
        let (key, _) = self.entries.next()?;
        Some(key)
    }
}

pub struct TrieOwnedEntries<'a, V> {
    /// Lending traversal whose keys are materialised before yielding.
    entries: TrieEntries<'a, V>,
}

impl<'a, V> Iterator for TrieOwnedEntries<'a, V> {
    type Item = (Identifier, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        let (key, value) = self.entries.next()?;
        Some((key.to_owned(), value))
    }
}

pub struct TrieOwnedKeys<'a, V> {
    /// Owned-entry traversal reused to expose only the materialised key.
    entries: TrieOwnedEntries<'a, V>,
}

impl<V> Iterator for TrieOwnedKeys<'_, V> {
    type Item = Identifier;

    fn next(&mut self) -> Option<Self::Item> {
        self.entries.next().map(|(key, _)| key)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrieSet(TrieMap<()>);
impl TrieSet {
    #[must_use]
    pub fn new() -> Self {
        Self(TrieMap::new())
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn insert(&mut self, key: Identifier) -> bool {
        self.0.insert(key, ()).is_none()
    }

    #[must_use]
    pub fn contains(&self, key: &Identifier) -> bool {
        self.0.get(key).is_some()
    }

    /// Traverse all keys with key views borrowed from the iterator's reusable path buffer.
    #[must_use]
    pub fn keys(&self) -> TrieKeys<'_, ()> {
        self.0.keys()
    }

    /// Traverse all keys as a standard [`Iterator`] by materialising an owned key per entry.
    #[must_use]
    pub fn owned_keys(&self) -> TrieOwnedKeys<'_, ()> {
        self.0.owned_keys()
    }
}

impl Default for TrieSet {
    fn default() -> Self {
        Self::new()
    }
}

impl FromIterator<Identifier> for TrieSet {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = Identifier>,
    {
        let mut set = TrieSet::new();
        for id in iter {
            set.insert(id);
        }
        set
    }
}

/// Iterator over one node's child segment map.
type ChildrenIterator<'a, V> = std::collections::hash_map::Iter<'a, IdentifierSegment, TrieNode<V>>;

#[derive(Clone, Debug, PartialEq, Eq)]
struct TrieNode<V> {
    value: Option<V>,
    children: AHashMap<IdentifierSegment, TrieNode<V>>,
}

impl<V> TrieNode<V> {
    fn count(&self) -> usize {
        let mut total = usize::from(self.value.is_some());
        for child in self.children.values() {
            total += child.count();
        }
        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::Itertools;

    fn id<const N: usize>(segments: [&str; N]) -> Identifier {
        Identifier::from_array(segments)
    }

    #[test]
    fn test_insert_and_get() {
        let mut trie_map = TrieMap::new();
        let mut trie_set = TrieSet::new();
        let a = id(["a"]);
        let b = id(["a", "b"]);
        let c = id(["a", "b", "c"]);
        let d = id(["d"]);

        trie_map.insert(a.clone(), 1);
        trie_set.insert(a.clone());
        trie_map.insert(b.clone(), 2);
        trie_set.insert(b.clone());
        trie_map.insert(c.clone(), 3);
        trie_set.insert(c.clone());
        trie_map.insert(d.clone(), 4);
        trie_set.insert(d.clone());

        assert_eq!(trie_map.get(&a), Some(&1));
        assert!(trie_set.contains(&a));
        assert_eq!(trie_map.get(&b), Some(&2));
        assert!(trie_set.contains(&b));
        assert_eq!(trie_map.get(&c), Some(&3));
        assert!(trie_set.contains(&c));
        assert_eq!(trie_map.get(&d), Some(&4));
        assert!(trie_set.contains(&d));
        assert_eq!(trie_map.get(&id(["x"])), None);
        assert!(!trie_set.contains(&id(["x"])));
    }

    #[test]
    fn test_len_and_is_empty() {
        let mut trie_map = TrieMap::new();
        let mut trie_set = TrieSet::new();
        assert_eq!(trie_map.len(), 0);
        assert_eq!(trie_set.len(), 0);
        assert!(trie_map.is_empty());
        assert!(trie_set.is_empty());

        trie_map.insert(id(["x"]), 42);
        trie_set.insert(id(["x"]));
        assert_eq!(trie_map.len(), 1);
        assert_eq!(trie_set.len(), 1);
        assert!(!trie_map.is_empty());
        assert!(!trie_set.is_empty());
    }

    #[test]
    fn test_entries_and_owned_iterators() {
        let mut trie_map = TrieMap::new();
        let mut trie_set = TrieSet::new();
        let keys = [id(["a"]), id(["a", "b"]), id(["x", "y"])];

        for (i, k) in keys.iter().enumerate() {
            trie_map.insert(k.clone(), i);
            trie_set.insert(k.clone());
        }

        let mut entries = trie_map.entries();
        let mut seen = Vec::new();
        while let Some((id, value)) = entries.next() {
            seen.push((id.to_owned(), *value));
        }
        seen.sort_by_key(|id| id.0.clone());
        let mut expected = keys
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, id)| (id, index))
            .collect_vec();
        expected.sort_by_key(|id| id.0.clone());

        assert_eq!(seen, expected);

        let mut seen = trie_map
            .owned_entries()
            .map(|(id, value)| (id, *value))
            .collect_vec();
        seen.sort_by_key(|id| id.0.clone());

        assert_eq!(seen, expected);

        let mut set_keys = trie_set.keys();
        let mut seen_set_keys = Vec::new();
        while let Some(id) = set_keys.next() {
            seen_set_keys.push(id.to_owned());
        }
        seen_set_keys.sort();

        let mut expected_keys = keys.to_vec();
        expected_keys.sort();

        assert_eq!(seen_set_keys, expected_keys);

        let mut seen_owned_set_keys = trie_set.owned_keys().collect_vec();
        seen_owned_set_keys.sort();

        assert_eq!(seen_owned_set_keys, expected_keys);
    }

    #[test]
    fn test_remove_prunes_empty_branches() {
        let mut trie_map = TrieMap::new();
        let a = id(["a"]);
        let ab = id(["a", "b"]);
        let abc = id(["a", "b", "c"]);

        trie_map.insert(a.clone(), 1);
        trie_map.insert(ab.clone(), 2);
        trie_map.insert(abc.clone(), 3);

        assert_eq!(trie_map.remove(&abc), Some(3));
        assert_eq!(trie_map.get(&abc), None);
        assert_eq!(trie_map.get(&ab), Some(&2));
        assert_eq!(trie_map.remove(&ab), Some(2));
        assert_eq!(trie_map.get(&ab), None);
        assert_eq!(trie_map.get(&a), Some(&1));
        assert_eq!(trie_map.remove(&a), Some(1));
        assert!(trie_map.is_empty());
    }
}
