//! [[TrieMap]] and [[TrieSet]] offer more compressed memory layout for larger groups by re-using parent identifiers,
//! but they cannot implement [[GroupMembership]] as the full Identifiers cannot be pointed to
//! (need to be owned).
//! They do offer efficient iteration, however.
use super::*;
use ahash::AHashMap;

#[derive(Clone, Debug)]
pub struct TrieMap<V> {
    root: TrieNode<V>,
}

#[derive(Clone, Debug)]
struct TrieNode<V> {
    value: Option<V>,
    children: AHashMap<IdentifierSegment, TrieNode<V>>,
}

impl<V> TrieMap<V> {
    pub fn new() -> Self {
        TrieMap {
            root: TrieNode {
                value: None,
                children: AHashMap::new(),
            },
        }
    }

    pub fn is_empty(&self) -> bool {
        self.root.children.is_empty()
    }

    pub fn len(&self) -> usize {
        self.root.count()
    }

    pub fn insert(&mut self, key: Identifier, value: V) -> Option<V> {
        let mut node = &mut self.root;
        for segment in key.segments {
            node = node.children.entry(segment).or_insert_with(|| TrieNode {
                value: None,
                children: AHashMap::new(),
            });
        }
        node.value.replace(value)
    }

    pub fn get(&self, key: &Identifier) -> Option<&V> {
        let mut node = &self.root;
        for segment in &key.segments {
            node = node.children.get(segment)?;
        }
        node.value.as_ref()
    }

    pub fn get_mut(&mut self, key: &Identifier) -> Option<&mut V> {
        let mut node = &mut self.root;
        for segment in &key.segments {
            node = node.children.get_mut(segment)?;
        }
        node.value.as_mut()
    }

    pub fn iter(&self) -> TrieIter<'_, V> {
        TrieIter {
            stack: vec![(&self.root, self.root.children.iter())],
            path: Vec::new(),
            yield_self: true,
            current: Some(&self.root),
        }
    }

    pub fn iter_keys(&self) -> TrieIdentifierIter<'_, V> {
        TrieIdentifierIter { inner: self.iter() }
    }
}

impl<V> TrieNode<V> {
    fn count(&self) -> usize {
        let mut total = if self.value.is_some() { 1 } else { 0 };
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
                        Identifier {
                            segments: self.path.clone(),
                        },
                        value,
                    ));
                }
            }

            if let Some((node, mut children)) = self.stack.pop() {
                if let Some((seg, child)) = children.next() {
                    // Put the updated children back before recursing
                    self.stack.push((node, children));
                    self.path.push(seg.clone());
                    self.current = Some(child);
                    self.stack.push((child, child.children.iter()));
                    self.yield_self = true;
                    continue;
                } else {
                    self.path.pop();
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
impl<'a, V> Iterator for TrieIdentifierIter<'a, V> {
    type Item = Identifier;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|t| t.0)
    }
}

#[derive(Clone, Debug)]
pub struct TrieSet(TrieMap<()>);
impl TrieSet {
    pub fn new() -> Self {
        Self(TrieMap::new())
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn insert(&mut self, key: Identifier) -> bool {
        self.0.insert(key, ()).is_none()
    }

    pub fn contains(&self, key: &Identifier) -> bool {
        self.0.get(key).is_some()
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
        self.0.iter_keys()
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
        let keys = vec![id(["a"]), id(["a", "b"]), id(["x", "y"])];

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
}
