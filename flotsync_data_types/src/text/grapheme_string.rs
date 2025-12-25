use super::*;

#[derive(Clone)]
pub struct GraphemeString {
    len: usize,
    base: String,
}
impl GraphemeString {
    pub fn new(base: String) -> Self {
        let len = base.graphemes(true).count();
        Self { len, base }
    }

    pub const EMPTY: Self = Self {
        len: 0,
        base: String::new(),
    };

    pub fn unwrap(self) -> String {
        self.base
    }

    fn graphemes(&self) -> Graphemes<'_> {
        self.base.graphemes(true)
    }

    pub fn take(&mut self, max_elements: usize) -> GraphemeString {
        if self.len <= max_elements {
            std::mem::replace(self, Self::EMPTY)
        } else {
            // Temporarily move the contents out of self, so we can re-use split_at.
            let current = std::mem::replace(self, Self::EMPTY);
            let (res, rest) = current.split_at(max_elements);
            *self = rest;
            res
        }
    }
}
impl Composite for GraphemeString {
    type Element = str;
    type Iter<'a> = Graphemes<'a>;

    fn len(&self) -> usize {
        self.len
    }

    fn get(&self, index: usize) -> Option<&Self::Element> {
        self.graphemes().nth(index)
    }

    fn split_at(mut self, index: usize) -> (Self, Self) {
        assert!(index < self.len);
        let (split_index, _) = self.base.grapheme_indices(true).nth(index).unwrap();
        let rest_string = self.base.split_off(split_index);
        let new_string = GraphemeString {
            len: self.len - index,
            base: rest_string,
        };
        self.len = index;
        (self, new_string)
    }

    fn concat(mut self, other: Self) -> Self {
        self.base.push_str(&other.base);
        self.len += other.len;
        self
    }

    fn iter(&self) -> Self::Iter<'_> {
        self.graphemes()
    }
}
impl fmt::Debug for GraphemeString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.base)
    }
}
impl fmt::Display for GraphemeString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.base)
    }
}
