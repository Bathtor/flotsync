//! [[`TrieMap`]] and [[`TrieSet`]] offer more compressed memory layout for larger groups by re-using parent identifiers,
//! but they cannot implement [[`GroupMembership`]] as the full identifiers cannot be pointed to
//! (need to be owned).
//! They do offer efficient iteration, however.
use super::{Identifier, IdentifierSegment};
use ahash::AHashMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrieMap<V> {
    root: TrieNode<V>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TrieNode<V> {
    value: Option<V>,
    children: AHashMap<IdentifierSegment, TrieNode<V>>,
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
        for segment in key.segments() {
            node = node.children.entry(segment).or_insert_with(|| TrieNode {
                value: None,
                children: AHashMap::new(),
            });
        }
        node.value.replace(value)
    }

    #[must_use]
    pub fn get(&self, key: &Identifier) -> Option<&V> {
        let mut node = &self.root;
        for segment in key.segments_iter() {
            node = node.children.get(segment)?;
        }
        node.value.as_ref()
    }

    pub fn remove(&mut self, key: &Identifier) -> Option<V> {
        let removed = Self::remove_from_node(&mut self.root, key.segments_iter());
        debug_assert!(
            self.root.value.is_none(),
            "trie root must stay valueless after keyed removal"
        );
        removed
    }

    #[allow(dead_code)] // for later
    #[must_use]
    pub fn get_mut(&mut self, key: &Identifier) -> Option<&mut V> {
        let mut node = &mut self.root;
        for segment in key.segments_iter() {
            node = node.children.get_mut(segment)?;
        }
        node.value.as_mut()
    }

    #[must_use]
    pub fn iter(&self) -> TrieIter<'_, V> {
        TrieIter {
            stack: vec![(&self.root, self.root.children.iter())],
            path: Vec::new(),
            yield_self: true,
            current: Some(&self.root),
        }
    }

    #[must_use]
    pub fn iter_keys(&self) -> TrieIdentifierIter<'_, V> {
        TrieIdentifierIter { inner: self.iter() }
    }

    /// Build a new trie with the same keys and mapped values.
    #[must_use]
    pub fn map_values<U>(&self, mut mapper: impl FnMut(&V) -> U) -> TrieMap<U> {
        let mut output = TrieMap::new();
        for (key, value) in self {
            output.insert(key, mapper(value));
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

impl<V> TrieNode<V> {
    fn count(&self) -> usize {
        let mut total = usize::from(self.value.is_some());
        for child in self.children.values() {
            total += child.count();
        }
        total
    }
}

type ChildrenIterator<'a, V> = std::collections::hash_map::Iter<'a, IdentifierSegment, TrieNode<V>>;

pub struct TrieIter<'a, V> {
    stack: Vec<(&'a TrieNode<V>, ChildrenIterator<'a, V>)>,
    path: Vec<IdentifierSegment>,
    yield_self: bool,
    current: Option<&'a TrieNode<V>>,
}

impl<'a, V> Iterator for TrieIter<'a, V> {
    type Item = (Identifier, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.yield_self {
                self.yield_self = false;
                if let Some(node) = self.current
                    && let Some(ref value) = node.value
                {
                    return Some((
                        Identifier::from_segments_unchecked(self.path.clone().into_boxed_slice()),
                        value,
                    ));
                }
            }

            if let Some((node, mut children)) = self.stack.pop() {
                match children.next() {
                    Some((seg, child)) => {
                        // Put the updated children back before recursing
                        self.stack.push((node, children));
                        self.path.push(seg.clone());
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

pub struct TrieIdentifierIter<'a, V> {
    inner: TrieIter<'a, V>,
}
impl<V> Iterator for TrieIdentifierIter<'_, V> {
    type Item = Identifier;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|t| t.0)
    }
}

#[derive(Clone, Debug)]
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

    #[must_use]
    pub fn iter(&self) -> TrieIdentifierIter<'_, ()> {
        self.0.iter_keys()
    }
}

impl Default for TrieSet {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> IntoIterator for &'a TrieSet {
    type Item = Identifier;
    type IntoIter = TrieIdentifierIter<'a, ()>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, V> IntoIterator for &'a TrieMap<V> {
    type Item = (Identifier, &'a V);
    type IntoIter = TrieIter<'a, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
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
    fn test_iter_and_into_iter() {
        let mut trie_map = TrieMap::new();
        let mut trie_set = TrieSet::new();
        let keys = [id(["a"]), id(["a", "b"]), id(["x", "y"])];

        for (i, k) in keys.iter().enumerate() {
            trie_map.insert(k.clone(), i);
            trie_set.insert(k.clone());
        }

        let mut seen = trie_map
            .iter()
            .map(|(id, value)| (id, *value))
            .collect_vec();
        seen.sort_by_key(|id| id.0.clone());
        let mut expected = keys
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, id)| (id, index))
            .collect_vec();
        expected.sort_by_key(|id| id.0.clone());

        assert_eq!(seen, expected);
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
