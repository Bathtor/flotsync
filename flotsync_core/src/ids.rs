use crate::{
    errors::Errors,
    member::{
        Identifier,
        IdentifierError,
        IdentifierLike,
        IdentifierParseError,
        IdentifierSegment,
    },
};
use std::{fmt, str::FromStr};
use uuid::Uuid;

/// Stable identifier selecting one application namespace.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ApplicationId(Identifier);

impl ApplicationId {
    /// Build an application identifier from validated hierarchical segments.
    ///
    /// # Errors
    ///
    /// Returns every segment-validation error reported by [`Identifier::try_from_array`].
    pub fn try_from_array<I, const N: usize>(
        segments: [I; N],
    ) -> Result<Self, Errors<IdentifierError>>
    where
        I: Into<IdentifierSegment>,
    {
        Identifier::try_from_array(segments).map(Self)
    }

    /// Build an application identifier from hierarchical segments.
    ///
    /// # Panics
    ///
    /// Panics when a segment is invalid or the identifier exceeds the segment-count limit.
    #[must_use]
    pub fn from_array<I, const N: usize>(segments: [I; N]) -> Self
    where
        I: Into<IdentifierSegment>,
    {
        Self(Identifier::from_array(segments))
    }

    /// Borrow the underlying hierarchical identifier for generic identifier algorithms.
    #[must_use]
    pub const fn as_identifier(&self) -> &Identifier {
        &self.0
    }

    /// Consume this application identifier and return its hierarchical representation.
    #[must_use]
    pub fn into_identifier(self) -> Identifier {
        self.0
    }
}

impl IdentifierLike for ApplicationId {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn segments(&self) -> impl Iterator<Item = &IdentifierSegment> {
        self.0.segments()
    }
}

impl From<Identifier> for ApplicationId {
    fn from(identifier: Identifier) -> Self {
        Self(identifier)
    }
}

impl From<ApplicationId> for Identifier {
    fn from(application_id: ApplicationId) -> Self {
        application_id.into_identifier()
    }
}

impl FromStr for ApplicationId {
    type Err = IdentifierParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        input.parse().map(Self)
    }
}

impl fmt::Display for ApplicationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Member identity used by Flotsync protocols and security material.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MemberIdentity(Identifier);

impl MemberIdentity {
    /// Build a member identity from validated hierarchical segments.
    ///
    /// # Errors
    ///
    /// Returns every segment-validation error reported by [`Identifier::try_from_array`].
    pub fn try_from_array<I, const N: usize>(
        segments: [I; N],
    ) -> Result<Self, Errors<IdentifierError>>
    where
        I: Into<IdentifierSegment>,
    {
        Identifier::try_from_array(segments).map(Self)
    }

    /// Build a member identity from hierarchical segments.
    ///
    /// # Panics
    ///
    /// Panics when a segment is invalid or the identifier exceeds the segment-count limit.
    #[must_use]
    pub fn from_array<I, const N: usize>(segments: [I; N]) -> Self
    where
        I: Into<IdentifierSegment>,
    {
        Self(Identifier::from_array(segments))
    }

    /// Borrow the underlying hierarchical identifier for generic identifier algorithms.
    #[must_use]
    pub const fn as_identifier(&self) -> &Identifier {
        &self.0
    }

    /// Consume this member identity and return its hierarchical representation.
    #[must_use]
    pub fn into_identifier(self) -> Identifier {
        self.0
    }
}

impl IdentifierLike for MemberIdentity {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn segments(&self) -> impl Iterator<Item = &IdentifierSegment> {
        self.0.segments()
    }
}

impl From<Identifier> for MemberIdentity {
    fn from(identifier: Identifier) -> Self {
        Self(identifier)
    }
}

impl From<MemberIdentity> for Identifier {
    fn from(member_identity: MemberIdentity) -> Self {
        member_identity.into_identifier()
    }
}

impl FromStr for MemberIdentity {
    type Err = IdentifierParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        input.parse().map(Self)
    }
}

impl fmt::Display for MemberIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// A stable identifier for a replication/discovery group.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GroupId(pub Uuid);

impl GroupId {
    /// Placeholder group id used by aggregate defaults.
    pub const NIL: Self = Self(Uuid::nil());

    /// Generate a fresh random UUID-v4 group id.
    #[must_use]
    pub fn new_random() -> Self {
        Self(Uuid::new_v4())
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn application_id_round_trips_through_text_and_generic_identifier() {
        let application_id = ApplicationId::from_array(["checklist", "desktop"]);

        assert_eq!(
            application_id.to_string().parse::<ApplicationId>().unwrap(),
            application_id
        );
        assert_eq!(
            ApplicationId::from(application_id.clone().into_identifier()),
            application_id
        );
    }

    #[test]
    fn member_identity_round_trips_through_text_and_generic_identifier() {
        let member_id = MemberIdentity::from_array(["alice", "laptop"]);

        assert_eq!(
            member_id.to_string().parse::<MemberIdentity>().unwrap(),
            member_id
        );
        assert_eq!(
            MemberIdentity::from(member_id.clone().into_identifier()),
            member_id
        );
    }

    #[test]
    fn group_id_nil_wraps_the_nil_uuid() {
        assert_eq!(GroupId::NIL.0, Uuid::nil());
    }

    #[test]
    fn group_id_new_random_builds_a_random_uuid() {
        let group_id = GroupId::new_random();

        assert_ne!(group_id, GroupId::NIL);
        assert_eq!(group_id.0.get_version(), Some(uuid::Version::Random));
    }
}
