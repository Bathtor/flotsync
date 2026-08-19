use super::DatasetIdError;
use flotsync_core::GroupId;
use std::{borrow::Cow, str::FromStr};
use uuid::Uuid;

/// Dataset identifier used in public replication APIs.
///
/// Validation follows SQL-like unquoted identifiers:
/// - first character: `[A-Za-z_]`
/// - remaining characters: `[A-Za-z0-9_]`
///
/// Identifiers created from process-static strings may borrow that storage. Parsed identifiers and
/// identifiers created from owned strings retain an owned value. Storage does not affect equality,
/// ordering, or hashing.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DatasetId(Cow<'static, str>);

impl DatasetId {
    /// Validate one dataset identifier without retaining it or allocating on success.
    ///
    /// # Errors
    ///
    /// Returns [`DatasetIdError::Empty`] for an empty value,
    /// [`DatasetIdError::InvalidStartCharacter`] when the first character is not an ASCII letter
    /// or underscore, or [`DatasetIdError::InvalidCharacter`] with the offending byte index when a
    /// later character is not an ASCII letter, digit, or underscore.
    pub fn validate(value: &str) -> Result<(), DatasetIdError> {
        let mut characters = value.char_indices();
        let Some((_, first_char)) = characters.next() else {
            return Err(DatasetIdError::Empty);
        };

        if !is_dataset_id_start_char(first_char) {
            return Err(DatasetIdError::InvalidStartCharacter {
                value: value.to_owned(),
            });
        }

        for (index, character) in characters {
            if !is_dataset_id_continue_char(character) {
                return Err(DatasetIdError::InvalidCharacter {
                    value: value.to_owned(),
                    index,
                    character,
                });
            }
        }

        Ok(())
    }

    /// Validate and retain an owned dataset identifier.
    ///
    /// Successful construction takes ownership of `value` without copying its contents.
    ///
    /// # Errors
    ///
    /// Returns the same [`DatasetIdError`] variants as [`Self::validate`].
    pub fn try_from_owned(value: String) -> Result<Self, DatasetIdError> {
        Self::validate(&value)?;
        Ok(Self(Cow::Owned(value)))
    }

    /// Validate and borrow a process-static dataset identifier.
    ///
    /// Successful construction retains `value` without allocating.
    ///
    /// # Errors
    ///
    /// Returns the same [`DatasetIdError`] variants as [`Self::validate`].
    pub fn try_from_static(value: &'static str) -> Result<Self, DatasetIdError> {
        Self::validate(value)?;
        Ok(Self::from_static_unchecked(value))
    }

    /// Return this identifier as a borrowed string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }

    /// Return this identifier as an owned string.
    ///
    /// An already owned identifier returns its retained string without copying. A process-static
    /// identifier allocates and copies its borrowed value.
    #[must_use]
    pub fn into_string(self) -> String {
        self.0.into_owned()
    }

    /// Borrow a process-static identifier without validating it.
    ///
    /// Callers must ensure `value` satisfies the [`DatasetId`] syntax. This is restricted to the
    /// crate so public construction cannot bypass that invariant.
    #[must_use]
    pub(crate) const fn from_static_unchecked(value: &'static str) -> Self {
        Self(Cow::Borrowed(value))
    }
}

impl AsRef<str> for DatasetId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::fmt::Display for DatasetId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<String> for DatasetId {
    type Error = DatasetIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from_owned(value)
    }
}

impl FromStr for DatasetId {
    type Err = DatasetIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::validate(value)?;
        Ok(Self(Cow::Owned(value.to_owned())))
    }
}

impl From<DatasetId> for String {
    fn from(value: DatasetId) -> Self {
        value.into_string()
    }
}

/// A globally unique row key inside one dataset.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RowKey(pub Uuid);

impl std::fmt::Display for RowKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Public row identifier that combines group, dataset, and row identity.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RowId {
    pub group_id: GroupId,
    pub dataset_id: DatasetId,
    pub row_key: RowKey,
}

impl std::fmt::Display for RowId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}/{}", self.group_id, self.dataset_id, self.row_key)
    }
}

/// Stable identifier for one old-to-new group migration.
///
/// The old group is the authority context for the proposal and any close
/// signal. The new group is the target replication group and group-key epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MigrationId {
    pub old_group_id: GroupId,
    pub new_group_id: GroupId,
}

impl std::fmt::Display for MigrationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}->{}", self.old_group_id, self.new_group_id)
    }
}

/// Return whether `character` may begin a dataset identifier.
fn is_dataset_id_start_char(character: char) -> bool {
    character.is_ascii_alphabetic() || character == '_'
}

/// Return whether `character` may follow the first dataset identifier character.
fn is_dataset_id_continue_char(character: char) -> bool {
    character.is_ascii_alphanumeric() || character == '_'
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
    };

    #[test]
    fn validation_accepts_sql_like_ascii_identifiers() {
        for value in ["_", "a", "Z", "docs_2"] {
            assert!(DatasetId::validate(value).is_ok(), "{value}");
        }
    }

    #[test]
    fn validation_reports_each_invalid_character_category() {
        assert!(matches!(
            DatasetId::validate(""),
            Err(DatasetIdError::Empty)
        ));
        assert!(matches!(
            DatasetId::validate("1docs"),
            Err(DatasetIdError::InvalidStartCharacter { value }) if value == "1docs"
        ));
        assert!(matches!(
            DatasetId::validate("doc-s"),
            Err(DatasetIdError::InvalidCharacter {
                value,
                index: 3,
                character: '-'
            }) if value == "doc-s"
        ));
        assert!(matches!(
            DatasetId::validate("döcs"),
            Err(DatasetIdError::InvalidCharacter {
                value,
                index: 1,
                character: 'ö'
            }) if value == "döcs"
        ));
    }

    #[test]
    fn checked_construction_paths_reject_invalid_identifiers() {
        assert!(matches!(
            DatasetId::try_from_static("-docs"),
            Err(DatasetIdError::InvalidStartCharacter { .. })
        ));
        assert!(matches!(
            DatasetId::try_from_owned(String::from("doc-s")),
            Err(DatasetIdError::InvalidCharacter { .. })
        ));
        assert!(matches!(
            "".parse::<DatasetId>(),
            Err(DatasetIdError::Empty)
        ));
    }

    #[test]
    fn static_owned_and_parsed_identifiers_have_equal_value_semantics() {
        let static_id = DatasetId::try_from_static("docs").expect("static dataset id should build");
        let owned_id =
            DatasetId::try_from_owned(String::from("docs")).expect("owned dataset id should build");
        let parsed_id = "docs"
            .parse::<DatasetId>()
            .expect("parsed dataset id should build");

        assert!(matches!(static_id.0, Cow::Borrowed("docs")));
        assert!(matches!(owned_id.0, Cow::Owned(_)));
        assert!(matches!(parsed_id.0, Cow::Owned(_)));
        assert_eq!(static_id, owned_id);
        assert_eq!(static_id, parsed_id);
        assert_eq!(static_id.cmp(&owned_id), std::cmp::Ordering::Equal);

        let mut static_hasher = DefaultHasher::new();
        static_id.hash(&mut static_hasher);
        let mut owned_hasher = DefaultHasher::new();
        owned_id.hash(&mut owned_hasher);
        assert_eq!(static_hasher.finish(), owned_hasher.finish());
    }
}
