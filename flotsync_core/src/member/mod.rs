use std::ops::Index;

mod identifier;
pub use identifier::*;
mod identifier_trie;

/// Some representation of flotsync group's members.
pub trait GroupMembership:
    // Note: While this is `usize` in memory for efficiency on 64bit platforms, the actual wire format only handles u32, so we must not construct larger groups.
    Index<usize, Output = Identifier> + IntoIterator<Item = Identifier>
{
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The number of members in the group.
    fn len(&self) -> usize;

    fn iter(&self) -> impl Iterator<Item = &Identifier>;
}

// Trivial implementation.
impl GroupMembership for Vec<Identifier> {
    fn len(&self) -> usize {
        self.len()
    }

    fn iter(&self) -> impl Iterator<Item = &Identifier> {
        Vec::as_slice(self).iter()
    }
}
