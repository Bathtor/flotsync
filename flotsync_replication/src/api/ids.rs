use super::DatasetIdError;
use flotsync_core::member::Identifier;
use uuid::Uuid;

/// Member identity in group membership APIs.
pub type MemberIdentity = Identifier;

/// A stable identifier for a replication group.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GroupId(pub Uuid);

/// Dataset identifier used in public replication APIs.
///
/// Validation follows SQL-like unquoted identifiers:
/// - first character: `[A-Za-z_]`
/// - remaining characters: `[A-Za-z0-9_]`
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DatasetId(String);

impl DatasetId {
    pub fn try_new(value: impl Into<String>) -> Result<Self, DatasetIdError> {
        let value = value.into();

        let mut characters = value.char_indices();
        let Some((_, first_char)) = characters.next() else {
            return Err(DatasetIdError::Empty);
        };

        if !is_dataset_id_start_char(first_char) {
            return Err(DatasetIdError::InvalidStartCharacter { value });
        }

        for (index, character) in characters {
            if !is_dataset_id_continue_char(character) {
                return Err(DatasetIdError::InvalidCharacter {
                    value,
                    index,
                    character,
                });
            }
        }

        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn into_string(self) -> String {
        self.0
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

impl TryFrom<&str> for DatasetId {
    type Error = DatasetIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::try_new(value.to_owned())
    }
}

impl TryFrom<String> for DatasetId {
    type Error = DatasetIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl From<DatasetId> for String {
    fn from(value: DatasetId) -> Self {
        value.into_string()
    }
}

fn is_dataset_id_start_char(character: char) -> bool {
    character.is_ascii_alphabetic() || character == '_'
}

fn is_dataset_id_continue_char(character: char) -> bool {
    character.is_ascii_alphanumeric() || character == '_'
}

/// A globally unique row key inside one dataset.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RowKey(pub Uuid);

/// Public row identifier that combines group, dataset, and row identity.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RowId {
    pub group_id: GroupId,
    pub dataset_id: DatasetId,
    pub row_key: RowKey,
}

/// Migration represented as old/new group pair.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct GroupMigration {
    pub old_group_id: GroupId,
    pub new_group_id: GroupId,
}
