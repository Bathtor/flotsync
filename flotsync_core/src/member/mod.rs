use std::ops::Index;

mod identifier;
pub use identifier::*;
mod identifier_trie;

/// Some representation of flotsync group's members.
pub trait GroupMembership:
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
