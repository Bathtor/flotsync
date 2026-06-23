use crate::member::Identifier;
use uuid::Uuid;

/// Member identity used by Flotsync protocols and security material.
pub type MemberIdentity = Identifier;

/// A stable identifier for a replication/discovery group.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GroupId(pub Uuid);

impl std::fmt::Display for GroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Fixed canonical member position within one replication group.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MemberIndex(u32);

impl MemberIndex {
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn as_u32(self) -> u32 {
        self.0
    }

    #[must_use]
    pub const fn as_usize(self) -> usize {
        self.0 as usize
    }
}

impl std::fmt::Display for MemberIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<MemberIndex> for u32 {
    fn from(value: MemberIndex) -> Self {
        value.0
    }
}

impl From<MemberIndex> for usize {
    fn from(value: MemberIndex) -> Self {
        value.as_usize()
    }
}

impl TryFrom<usize> for MemberIndex {
    type Error = std::num::TryFromIntError;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        u32::try_from(value).map(Self)
    }
}
