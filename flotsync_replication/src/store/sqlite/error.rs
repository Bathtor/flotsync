//! SQLite store errors and project-wide failure classification.
//!
//! This module is the single classification boundary for SQLite-specific semantic failures,
//! `SQLx` lifecycle and protocol failures, and SQLite primary or extended result codes. Query
//! modules attach an operation phase but do not make independent retry or repair decisions.

use crate::api::{
    DatasetId,
    RowKey,
    StoreError,
    StoreErrorClass,
    StoreErrorClassification,
    StoreErrorClassificationSource,
    StoreErrorResolution,
    StoreErrorScope,
};
use flotsync_core::{GroupId, MemberIdentity, member::IdentifierParseError, versions::UpdateId};
use snafu::Snafu;
use std::{error::Error as StdError, io::ErrorKind, path::PathBuf};

/// Phase of a `SQLx` operation crossing the SQLite store boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SqliteOperation {
    /// Establish the initial pool and its connections.
    Connect,
    /// Acquire one pooled connection.
    AcquireConnection,
    /// Begin an explicit read or write transaction.
    BeginTransaction,
    /// Execute or fetch one SQL statement.
    ExecuteQuery,
    /// Commit an explicit transaction.
    CommitTransaction,
    /// Roll back an explicit transaction.
    RollbackTransaction,
    /// Create the schema required by a newly opened store.
    InitialiseSchema,
}

impl SqliteOperation {
    /// Return the operation label used in the preserved SQLite source error.
    const fn description(self) -> &'static str {
        match self {
            Self::Connect => "connection",
            Self::AcquireConnection => "connection acquisition",
            Self::BeginTransaction => "transaction begin",
            Self::ExecuteQuery => "query execution",
            Self::CommitTransaction => "transaction commit",
            Self::RollbackTransaction => "transaction rollback",
            Self::InitialiseSchema => "schema initialisation",
        }
    }

    /// Return the conservative scope for a phase-specific failure without a more precise code.
    const fn scope(self) -> StoreErrorScope {
        match self {
            Self::Connect | Self::AcquireConnection => StoreErrorScope::Connection,
            Self::BeginTransaction | Self::CommitTransaction | Self::RollbackTransaction => {
                StoreErrorScope::Transaction
            }
            Self::ExecuteQuery => StoreErrorScope::Operation,
            Self::InitialiseSchema => StoreErrorScope::Store,
        }
    }
}

/// Concrete failures raised by the SQLite replication-store implementation.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(super)))]
pub(super) enum SqliteStoreError {
    /// `SQLx` failed while executing or decoding one query.
    #[snafu(display("SQLite query execution failed: {source}"))]
    Sqlx {
        /// Original `SQLx` failure, including any SQLite extended result code.
        source: sqlx::Error,
    },
    /// `SQLx` failed during an explicit connection or transaction phase.
    #[snafu(display("SQLite {} failed: {source}", operation.description()))]
    SqlxPhase {
        /// Phase that determines the smallest safe recovery scope.
        operation: SqliteOperation,
        /// Original `SQLx` failure, including any SQLite extended result code.
        source: sqlx::Error,
    },
    /// The generated or supplied SQLite connection URL was invalid.
    #[snafu(display("SQLite connection URL '{database_url}' was invalid: {source}"))]
    ParseSqliteUrl {
        /// Rejected connection URL.
        database_url: String,
        /// Original `SQLx` configuration failure.
        source: sqlx::Error,
    },
    /// Reserving a new database file failed before SQLite opened it.
    #[snafu(display("Failed to create new SQLite database file '{}': {source}", path.display()))]
    CreateDatabaseFile {
        /// Requested database path.
        path: PathBuf,
        /// Original filesystem failure.
        source: std::io::Error,
    },
    /// A SQLite store operation was requested after orderly closure started.
    #[snafu(display("The SQLite replication store is closed."))]
    Closed,
    /// An activated replication store has no provisioned local identity.
    #[snafu(display("The replication store has no provisioned local member identity."))]
    MissingLocalMemberIdentity,
    /// Stored private material identifies more than one local member.
    #[snafu(display(
        "The replication store contains local-private key material for several member identities: '{first}' and '{second}'."
    ))]
    AmbiguousLocalMemberIdentities {
        /// First stored local identity.
        first: MemberIdentity,
        /// Conflicting second stored local identity.
        second: MemberIdentity,
    },
    /// Provisioning requested a different local member than the existing store identity.
    #[snafu(display(
        "The replication store is provisioned for local member '{existing}' and cannot store local-private key material for '{requested}'."
    ))]
    ConflictingLocalMemberIdentity {
        /// Existing authoritative local member.
        existing: MemberIdentity,
        /// Conflicting requested local member.
        requested: MemberIdentity,
    },
    /// A stored group id was not a UUID.
    #[snafu(display("Stored group id was not a valid UUID: {source}"))]
    InvalidGroupId { source: uuid::Error },
    /// Caller-provided group material used the reserved nil id where storage requires a non-nil id.
    #[snafu(display(
        "Caller-provided replication group material has a nil group id, but stored groups require a non-nil id."
    ))]
    NilGroupId,
    /// A stored row key was not a UUID.
    #[snafu(display("Stored row key was not a valid UUID: {source}"))]
    InvalidRowKey { source: uuid::Error },
    /// A stored store-secret key id was malformed.
    #[snafu(display("Stored store-secret key id was invalid: {source}"))]
    InvalidStoreSecretKeyId {
        source: flotsync_security::StoreSecretKeyIdParseError,
    },
    /// A stored member identity was malformed.
    #[snafu(display("Stored member identity '{raw}' was invalid: {source}"))]
    InvalidMemberIdentity {
        /// Rejected persisted identifier.
        raw: String,
        source: IdentifierParseError,
    },
    /// Caller-provided group material contained no members.
    #[snafu(display("A replication group requires at least one member."))]
    EmptyGroupMembers,
    /// A stored group declared no members.
    #[snafu(display("A stored replication group requires at least one member."))]
    StoredEmptyGroupMembers,
    /// Caller-provided group material retained a forbidden placeholder secret.
    #[snafu(display(
        "Replication group {group_id} still carries invalid default security material."
    ))]
    InvalidDefaultGroupSecurityMaterial { group_id: GroupId },
    /// A caller-provided member count could not be represented by SQLite.
    #[snafu(display("Member count overflowed SQLite's supported range: {source}"))]
    MemberCountOverflow { source: std::num::TryFromIntError },
    /// A stored member count could not be represented by the public type.
    #[snafu(display("Stored member count overflowed the supported range: {source}"))]
    StoredMemberCountOverflow { source: std::num::TryFromIntError },
    /// A member index could not be represented by the target type.
    #[snafu(display("Stored member index overflowed the supported range: {source}"))]
    MemberIndexOverflow { source: std::num::TryFromIntError },
    /// A stored secret crypto version could not be represented by the public type.
    #[snafu(display("Stored secret crypto version overflowed the supported range: {source}"))]
    SecretCryptoVersionOverflow { source: std::num::TryFromIntError },
    /// A caller-provided local member index fell outside its group member set.
    #[snafu(display(
        "Local member index {local_member_index} is out of bounds for {member_count} members."
    ))]
    InvalidLocalMemberIndex {
        /// Rejected zero-based member index.
        local_member_index: u32,
        /// Number of members in the associated group.
        member_count: usize,
    },
    /// A stored local member index fell outside its group member set.
    #[snafu(display(
        "Stored local member index {local_member_index} is out of bounds for {member_count} members."
    ))]
    InvalidStoredLocalMemberIndex {
        /// Rejected zero-based member index.
        local_member_index: u32,
        /// Number of members in the associated group.
        member_count: usize,
    },
    /// Caller-provided active progress used the wrong member width.
    #[snafu(display(
        "Replication group {group_id} has {version_member_count} active version-vector members, but its material has {member_count} members."
    ))]
    ActiveVersionMemberCountMismatch {
        group_id: GroupId,
        version_member_count: usize,
        member_count: usize,
    },
    /// Caller-provided lifecycle progress used the wrong member width.
    #[snafu(display(
        "Replication group {group_id} has {version_member_count} lifecycle-cut members, but its material has {member_count} members."
    ))]
    LifecycleVersionMemberCountMismatch {
        group_id: GroupId,
        version_member_count: usize,
        member_count: usize,
    },
    /// Stored member rows did not match the material member count.
    #[snafu(display(
        "Stored group '{group_id}' expected {expected_member_count} members, but loaded {actual_member_count}."
    ))]
    StoredGroupMemberCountMismatch {
        group_id: GroupId,
        expected_member_count: usize,
        actual_member_count: usize,
    },
    /// Application schema configuration omitted a referenced dataset.
    #[snafu(display(
        "Stored schema source for dataset '{dataset_id}' in group '{group_id}' was missing."
    ))]
    MissingSchema {
        group_id: GroupId,
        dataset_id: DatasetId,
    },
    /// A stored protobuf blob could not be decoded.
    #[snafu(display("Stored {object} blob could not be decoded: {source}"))]
    DecodeStoredProto {
        /// Logical stored object kind.
        object: &'static str,
        source: flotsync_messages::buffa::DecodeError,
    },
    /// A stored object failed domain validation.
    #[snafu(display("Stored {object} was invalid: {source}"))]
    InvalidStoredObject {
        /// Logical stored object kind.
        object: &'static str,
        source: Box<dyn StdError + Send + Sync>,
    },
    /// A stored sort key had the wrong fixed width.
    #[snafu(display("Stored {object} sort key had invalid length {len}."))]
    InvalidStoredSortKey {
        /// Logical stored object kind.
        object: &'static str,
        /// Actual encoded width.
        len: usize,
    },
    /// Existing member-security material conflicts with the requested value.
    #[snafu(display(
        "Stored {object} for member '{member_id}' conflicts with requested material."
    ))]
    ConflictingMemberSecurityMaterial {
        object: &'static str,
        member_id: MemberIdentity,
    },
    /// Existing group material conflicts with the requested definition.
    #[snafu(display(
        "Stored group material for group '{group_id}' conflicts with requested material."
    ))]
    ConflictingGroupMaterial { group_id: GroupId },
    /// Existing pending work conflicts with the requested work.
    #[snafu(display(
        "Stored pending work for target group '{group_id}' conflicts with requested work."
    ))]
    ConflictingPendingGroupWork { group_id: GroupId },
    /// A stored update payload names the wrong group.
    #[snafu(display(
        "Stored update belonged to group '{actual_group_id}', expected '{expected_group_id}'."
    ))]
    StoredUpdateGroupMismatch {
        expected_group_id: GroupId,
        actual_group_id: GroupId,
    },
    /// A stored update payload names the wrong update id.
    #[snafu(display(
        "Stored update contained update id '{actual_update_id:?}', expected '{expected_update_id:?}'."
    ))]
    StoredUpdateIdMismatch {
        expected_update_id: UpdateId,
        actual_update_id: UpdateId,
    },
    /// A stored schema payload names a different dataset than its key.
    #[snafu(display(
        "Stored schema payload for group '{group}' was keyed as dataset '{key_dataset}' but contained dataset '{payload_dataset}'."
    ))]
    StoredDatasetSchemaKeyMismatch {
        group: GroupId,
        key_dataset: DatasetId,
        payload_dataset: DatasetId,
    },
    /// An operation expected an authoritative group row that was absent.
    #[snafu(display("Stored group '{group_id}' was missing."))]
    MissingStoredGroup { group_id: GroupId },
    /// A row-state transition conflicts with the currently persisted state.
    #[snafu(display(
        "Stored dataset row '{group_id}/{dataset_id}/{row_key}' cannot transition from {from} to {to}."
    ))]
    InvalidDatasetRowStateTransition {
        group_id: GroupId,
        dataset_id: DatasetId,
        row_key: RowKey,
        from: &'static str,
        to: &'static str,
    },
    /// An operation expected an authoritative update row that was absent.
    #[snafu(display("Stored update '{group_id}/{update_id:?}' was missing."))]
    MissingStoredUpdate {
        group_id: GroupId,
        update_id: UpdateId,
    },
}

/// `SQLx` context for establishing the initial pool and its connections.
pub(super) const SQLX_CONNECT_SNAFU: SqlxPhaseSnafu<SqliteOperation> = SqlxPhaseSnafu {
    operation: SqliteOperation::Connect,
};
/// `SQLx` context for acquiring one pooled connection.
pub(super) const SQLX_ACQUIRE_CONNECTION_SNAFU: SqlxPhaseSnafu<SqliteOperation> = SqlxPhaseSnafu {
    operation: SqliteOperation::AcquireConnection,
};
/// `SQLx` context for beginning an explicit transaction.
pub(super) const SQLX_BEGIN_TRANSACTION_SNAFU: SqlxPhaseSnafu<SqliteOperation> = SqlxPhaseSnafu {
    operation: SqliteOperation::BeginTransaction,
};
/// `SQLx` context for committing an explicit transaction.
pub(super) const SQLX_COMMIT_TRANSACTION_SNAFU: SqlxPhaseSnafu<SqliteOperation> = SqlxPhaseSnafu {
    operation: SqliteOperation::CommitTransaction,
};
/// `SQLx` context for rolling back an explicit transaction.
pub(super) const SQLX_ROLLBACK_TRANSACTION_SNAFU: SqlxPhaseSnafu<SqliteOperation> =
    SqlxPhaseSnafu {
        operation: SqliteOperation::RollbackTransaction,
    };
/// `SQLx` context for creating the schema of a newly opened store.
pub(super) const SQLX_INITIALISE_SCHEMA_SNAFU: SqlxPhaseSnafu<SqliteOperation> = SqlxPhaseSnafu {
    operation: SqliteOperation::InitialiseSchema,
};

impl SqliteStoreError {
    /// Return the project-wide classification chosen at the SQLite source boundary.
    pub(super) fn classification(&self) -> StoreErrorClassification {
        match self {
            Self::Sqlx { source } => classify_sqlx(SqliteOperation::ExecuteQuery, source),
            Self::SqlxPhase { operation, source } => classify_sqlx(*operation, source),
            Self::ParseSqliteUrl { .. }
            | Self::MissingLocalMemberIdentity
            | Self::MissingSchema { .. } => configuration(StoreErrorScope::Store),
            Self::CreateDatabaseFile { source, .. } => classify_create_file_error(source),
            Self::Closed => StoreErrorClassification::UNKNOWN
                .with_scope(StoreErrorScope::Connection)
                .with_class(StoreErrorClass::Unavailable)
                .with_resolution(StoreErrorResolution::Recreate),
            Self::AmbiguousLocalMemberIdentities { .. } => invalid_data(StoreErrorScope::Store),
            Self::ConflictingLocalMemberIdentity { .. } => {
                conflicting_state(StoreErrorScope::Store)
            }
            Self::InvalidGroupId { .. }
            | Self::InvalidRowKey { .. }
            | Self::InvalidStoreSecretKeyId { .. }
            | Self::InvalidMemberIdentity { .. }
            | Self::StoredEmptyGroupMembers
            | Self::StoredMemberCountOverflow { .. }
            | Self::MemberIndexOverflow { .. }
            | Self::SecretCryptoVersionOverflow { .. }
            | Self::InvalidStoredLocalMemberIndex { .. }
            | Self::StoredGroupMemberCountMismatch { .. }
            | Self::DecodeStoredProto { .. }
            | Self::InvalidStoredObject { .. }
            | Self::InvalidStoredSortKey { .. }
            | Self::StoredUpdateGroupMismatch { .. }
            | Self::StoredUpdateIdMismatch { .. }
            | Self::StoredDatasetSchemaKeyMismatch { .. } => invalid_data(StoreErrorScope::Record),
            Self::NilGroupId
            | Self::EmptyGroupMembers
            | Self::MemberCountOverflow { .. }
            | Self::InvalidLocalMemberIndex { .. }
            | Self::InvalidDefaultGroupSecurityMaterial { .. }
            | Self::ActiveVersionMemberCountMismatch { .. }
            | Self::LifecycleVersionMemberCountMismatch { .. } => {
                contract(StoreErrorScope::Operation)
            }
            Self::ConflictingMemberSecurityMaterial { .. }
            | Self::ConflictingGroupMaterial { .. }
            | Self::ConflictingPendingGroupWork { .. }
            | Self::MissingStoredGroup { .. }
            | Self::InvalidDatasetRowStateTransition { .. }
            | Self::MissingStoredUpdate { .. } => conflicting_state(StoreErrorScope::Record),
        }
    }
}

impl StoreErrorClassificationSource for SqliteStoreError {
    fn store_error_classification(&self) -> Option<StoreErrorClassification> {
        Some(self.classification())
    }
}

impl From<SqliteStoreError> for StoreError {
    fn from(value: SqliteStoreError) -> Self {
        StoreError::from_classification_source(value)
    }
}

/// SQLite result code mask selecting the primary result-code byte.
const SQLITE_PRIMARY_RESULT_MASK: i32 = 0xff;
/// Generic SQL or schema error.
const SQLITE_ERROR: i32 = 1;
/// Internal SQLite invariant failure.
const SQLITE_INTERNAL: i32 = 2;
/// Access permission denied.
const SQLITE_PERM: i32 = 3;
/// Operation aborted, commonly because its transaction rolled back.
const SQLITE_ABORT: i32 = 4;
/// Contention with another database connection.
const SQLITE_BUSY: i32 = 5;
/// Contention within one connection or a shared cache.
const SQLITE_LOCKED: i32 = 6;
/// Memory allocation failed.
const SQLITE_NOMEM: i32 = 7;
/// A write was attempted through read-only access.
const SQLITE_READONLY: i32 = 8;
/// The operation was interrupted.
const SQLITE_INTERRUPT: i32 = 9;
/// The environment reported an I/O failure.
const SQLITE_IOERR: i32 = 10;
/// SQLite detected database corruption.
const SQLITE_CORRUPT: i32 = 11;
/// The database or filesystem is full.
const SQLITE_FULL: i32 = 13;
/// SQLite could not open the database or an auxiliary file.
const SQLITE_CANTOPEN: i32 = 14;
/// The database locking protocol failed.
const SQLITE_PROTOCOL: i32 = 15;
/// A prepared statement observed a changed schema.
const SQLITE_SCHEMA: i32 = 17;
/// A value exceeded SQLite's supported size.
const SQLITE_TOOBIG: i32 = 18;
/// A database constraint rejected the operation.
const SQLITE_CONSTRAINT: i32 = 19;
/// A value had an incompatible type.
const SQLITE_MISMATCH: i32 = 20;
/// The SQLite API was used incorrectly.
const SQLITE_MISUSE: i32 = 21;
/// The environment lacks required large-file support.
const SQLITE_NOLFS: i32 = 22;
/// SQLite authorisation rejected the operation.
const SQLITE_AUTH: i32 = 23;
/// The file uses an unsupported or malformed format.
const SQLITE_FORMAT: i32 = 24;
/// A parameter index was outside the supported range.
const SQLITE_RANGE: i32 = 25;
/// The opened file was not a database.
const SQLITE_NOTADB: i32 = 26;
/// A stale read transaction could not be promoted to a writer.
const SQLITE_BUSY_SNAPSHOT: i32 = SQLITE_BUSY | (2 << 8);
/// An operation aborted because SQLite rolled back its explicit transaction.
const SQLITE_ABORT_ROLLBACK: i32 = SQLITE_ABORT | (2 << 8);

/// Classify one `SQLx` failure without flattening its concrete source.
fn classify_sqlx(operation: SqliteOperation, source: &sqlx::Error) -> StoreErrorClassification {
    match source {
        sqlx::Error::Configuration(_) => configuration(StoreErrorScope::Store),
        sqlx::Error::InvalidArgument(_)
        | sqlx::Error::RowNotFound
        | sqlx::Error::TypeNotFound { .. }
        | sqlx::Error::ColumnIndexOutOfBounds { .. }
        | sqlx::Error::ColumnNotFound(_)
        | sqlx::Error::Encode(_)
        | sqlx::Error::AnyDriverError(_) => contract(operation.scope()),
        sqlx::Error::Database(source) => classify_database_error(operation, source.as_ref()),
        sqlx::Error::Io(source) => classify_io_error(operation, source),
        sqlx::Error::Tls(_)
        | sqlx::Error::Protocol(_)
        | sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed => StoreErrorClassification::UNKNOWN
            .with_scope(StoreErrorScope::Connection)
            .with_class(StoreErrorClass::Unavailable)
            .with_resolution(StoreErrorResolution::Recreate),
        sqlx::Error::ColumnDecode { .. } | sqlx::Error::Decode(_) => {
            invalid_data(StoreErrorScope::Record)
        }
        sqlx::Error::PoolTimedOut => StoreErrorClassification::UNKNOWN
            .with_scope(StoreErrorScope::Connection)
            .with_class(StoreErrorClass::ConcurrentAccess)
            .with_resolution(StoreErrorResolution::Retry),
        sqlx::Error::InvalidSavePointStatement | sqlx::Error::BeginFailed => {
            contract(StoreErrorScope::Transaction)
        }
        _ => StoreErrorClassification::UNKNOWN,
    }
}

/// Classify a SQLite database error from its stable numeric result code.
fn classify_database_error(
    operation: SqliteOperation,
    source: &(dyn sqlx::error::DatabaseError + 'static),
) -> StoreErrorClassification {
    let Some(code) = source.code() else {
        return StoreErrorClassification::UNKNOWN;
    };
    let Ok(code) = code.parse::<i32>() else {
        return StoreErrorClassification::UNKNOWN;
    };
    match code {
        SQLITE_BUSY_SNAPSHOT => {
            return StoreErrorClassification::UNKNOWN
                .with_scope(StoreErrorScope::Transaction)
                .with_class(StoreErrorClass::ConcurrentAccess)
                .with_resolution(StoreErrorResolution::Retry);
        }
        SQLITE_ABORT_ROLLBACK => {
            return StoreErrorClassification::UNKNOWN
                .with_scope(StoreErrorScope::Transaction)
                .with_class(StoreErrorClass::Unavailable)
                .with_resolution(StoreErrorResolution::Retry);
        }
        _ => {}
    }
    match code & SQLITE_PRIMARY_RESULT_MASK {
        SQLITE_BUSY | SQLITE_LOCKED => StoreErrorClassification::UNKNOWN
            .with_scope(StoreErrorScope::Transaction)
            .with_class(StoreErrorClass::ConcurrentAccess)
            .with_resolution(StoreErrorResolution::Retry),
        SQLITE_ABORT => StoreErrorClassification::UNKNOWN
            .with_scope(StoreErrorScope::Transaction)
            .with_class(StoreErrorClass::Unavailable),
        SQLITE_INTERRUPT => StoreErrorClassification::UNKNOWN
            .with_scope(StoreErrorScope::Connection)
            .with_class(StoreErrorClass::Unavailable)
            .with_resolution(StoreErrorResolution::WaitForResume),
        SQLITE_PROTOCOL => StoreErrorClassification::UNKNOWN
            .with_scope(StoreErrorScope::Connection)
            .with_class(StoreErrorClass::Unavailable)
            .with_resolution(StoreErrorResolution::Recreate),
        SQLITE_IOERR => StoreErrorClassification::UNKNOWN
            .with_scope(StoreErrorScope::Environment)
            .with_class(StoreErrorClass::Unavailable),
        SQLITE_CORRUPT | SQLITE_FORMAT | SQLITE_NOTADB => invalid_data(StoreErrorScope::Store),
        SQLITE_FULL | SQLITE_NOMEM => StoreErrorClassification::UNKNOWN
            .with_scope(StoreErrorScope::Environment)
            .with_class(StoreErrorClass::ResourceExhaustion),
        SQLITE_TOOBIG => StoreErrorClassification::UNKNOWN
            .with_scope(StoreErrorScope::Operation)
            .with_class(StoreErrorClass::ResourceExhaustion),
        SQLITE_PERM | SQLITE_READONLY | SQLITE_CANTOPEN | SQLITE_NOLFS | SQLITE_AUTH => {
            configuration(StoreErrorScope::Store)
        }
        SQLITE_CONSTRAINT => StoreErrorClassification::UNKNOWN
            .with_scope(StoreErrorScope::Operation)
            .with_class(StoreErrorClass::ConflictingState),
        SQLITE_ERROR | SQLITE_INTERNAL | SQLITE_SCHEMA | SQLITE_MISMATCH | SQLITE_MISUSE
        | SQLITE_RANGE => contract(operation.scope()),
        _ => StoreErrorClassification::UNKNOWN,
    }
}

/// Classify an operating-system I/O failure raised directly by `SQLx`.
fn classify_io_error(
    operation: SqliteOperation,
    source: &std::io::Error,
) -> StoreErrorClassification {
    match source.kind() {
        ErrorKind::Interrupted | ErrorKind::WouldBlock | ErrorKind::TimedOut => {
            StoreErrorClassification::UNKNOWN
                .with_scope(operation.scope())
                .with_class(StoreErrorClass::Unavailable)
                .with_resolution(StoreErrorResolution::Retry)
        }
        ErrorKind::PermissionDenied => configuration(StoreErrorScope::Environment),
        _ => StoreErrorClassification::UNKNOWN
            .with_scope(StoreErrorScope::Environment)
            .with_class(StoreErrorClass::Unavailable),
    }
}

/// Classify failure to reserve a caller-requested database file.
fn classify_create_file_error(source: &std::io::Error) -> StoreErrorClassification {
    match source.kind() {
        ErrorKind::AlreadyExists => conflicting_state(StoreErrorScope::Store),
        ErrorKind::PermissionDenied => configuration(StoreErrorScope::Environment),
        ErrorKind::Interrupted | ErrorKind::WouldBlock | ErrorKind::TimedOut => {
            StoreErrorClassification::UNKNOWN
                .with_scope(StoreErrorScope::Environment)
                .with_class(StoreErrorClass::Unavailable)
                .with_resolution(StoreErrorResolution::Retry)
        }
        _ => StoreErrorClassification::UNKNOWN
            .with_scope(StoreErrorScope::Environment)
            .with_class(StoreErrorClass::Unavailable),
    }
}

/// Build a configuration classification at the supplied affected scope.
const fn configuration(scope: StoreErrorScope) -> StoreErrorClassification {
    StoreErrorClassification::UNKNOWN
        .with_scope(scope)
        .with_class(StoreErrorClass::Configuration)
        .with_resolution(StoreErrorResolution::Reconfigure)
}

/// Build an invalid-data classification at the supplied affected scope.
const fn invalid_data(scope: StoreErrorScope) -> StoreErrorClassification {
    StoreErrorClassification::UNKNOWN
        .with_scope(scope)
        .with_class(StoreErrorClass::InvalidData)
        .with_resolution(StoreErrorResolution::Repair)
}

/// Build a conflicting-state classification at the supplied affected scope.
const fn conflicting_state(scope: StoreErrorScope) -> StoreErrorClassification {
    StoreErrorClassification::UNKNOWN
        .with_scope(scope)
        .with_class(StoreErrorClass::ConflictingState)
        .with_resolution(StoreErrorResolution::ResolveConflict)
}

/// Build a persistence-contract classification at the supplied affected scope.
const fn contract(scope: StoreErrorScope) -> StoreErrorClassification {
    StoreErrorClassification::UNKNOWN
        .with_scope(scope)
        .with_class(StoreErrorClass::Contract)
        .with_resolution(StoreErrorResolution::FixBug)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{borrow::Cow, fmt};

    /// Minimal database error exposing one injected SQLite numeric code.
    #[derive(Debug)]
    struct TestDatabaseError {
        code: i32,
    }

    impl fmt::Display for TestDatabaseError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(formatter, "injected SQLite result code {}", self.code)
        }
    }

    impl StdError for TestDatabaseError {}

    impl sqlx::error::DatabaseError for TestDatabaseError {
        fn message(&self) -> &'static str {
            "injected SQLite database error"
        }

        fn code(&self) -> Option<Cow<'_, str>> {
            Some(Cow::Owned(self.code.to_string()))
        }

        fn as_error(&self) -> &(dyn StdError + Send + Sync + 'static) {
            self
        }

        fn as_error_mut(&mut self) -> &mut (dyn StdError + Send + Sync + 'static) {
            self
        }

        fn into_error(self: Box<Self>) -> Box<dyn StdError + Send + Sync + 'static> {
            self
        }

        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }

    /// Build a `SQLx` database error retaining one injected SQLite numeric code.
    fn database_error(code: i32) -> sqlx::Error {
        sqlx::Error::Database(Box::new(TestDatabaseError { code }))
    }

    /// Assert all three typed dimensions for one classification.
    fn assert_classification(
        classification: StoreErrorClassification,
        scope: StoreErrorScope,
        class: StoreErrorClass,
        resolution: StoreErrorResolution,
    ) {
        assert_eq!(classification.scope, scope);
        assert_eq!(classification.class, class);
        assert_eq!(classification.resolution, resolution);
    }

    #[test]
    fn sqlite_busy_retries_the_complete_transaction() {
        let error = database_error(SQLITE_BUSY);

        let classification = classify_sqlx(SqliteOperation::ExecuteQuery, &error);

        assert_classification(
            classification,
            StoreErrorScope::Transaction,
            StoreErrorClass::ConcurrentAccess,
            StoreErrorResolution::Retry,
        );
    }

    #[test]
    fn sqlite_busy_commit_retries_the_complete_transaction() {
        let error = database_error(SQLITE_BUSY);

        let classification = classify_sqlx(SqliteOperation::CommitTransaction, &error);

        assert_classification(
            classification,
            StoreErrorScope::Transaction,
            StoreErrorClass::ConcurrentAccess,
            StoreErrorResolution::Retry,
        );
    }

    #[test]
    fn sqlite_busy_snapshot_retries_the_transaction() {
        let error = database_error(SQLITE_BUSY_SNAPSHOT);

        let classification = classify_sqlx(SqliteOperation::ExecuteQuery, &error);

        assert_classification(
            classification,
            StoreErrorScope::Transaction,
            StoreErrorClass::ConcurrentAccess,
            StoreErrorResolution::Retry,
        );
    }

    #[test]
    fn sqlite_abort_rollback_retries_the_complete_transaction() {
        let error = database_error(SQLITE_ABORT_ROLLBACK);

        let classification = classify_sqlx(SqliteOperation::ExecuteQuery, &error);

        assert_classification(
            classification,
            StoreErrorScope::Transaction,
            StoreErrorClass::Unavailable,
            StoreErrorResolution::Retry,
        );
    }

    #[test]
    fn base_sqlite_abort_retains_known_dimensions_without_assuming_retry() {
        let error = database_error(SQLITE_ABORT);

        let classification = classify_sqlx(SqliteOperation::ExecuteQuery, &error);

        assert_classification(
            classification,
            StoreErrorScope::Transaction,
            StoreErrorClass::Unavailable,
            StoreErrorResolution::Unknown,
        );
    }

    #[test]
    fn sqlite_interrupt_waits_for_the_connection_to_resume() {
        let error = database_error(SQLITE_INTERRUPT);

        let classification = classify_sqlx(SqliteOperation::ExecuteQuery, &error);

        assert_classification(
            classification,
            StoreErrorScope::Connection,
            StoreErrorClass::Unavailable,
            StoreErrorResolution::WaitForResume,
        );
    }

    #[test]
    fn pool_timeout_retries_connection_acquisition() {
        let classification = classify_sqlx(
            SqliteOperation::AcquireConnection,
            &sqlx::Error::PoolTimedOut,
        );

        assert_classification(
            classification,
            StoreErrorScope::Connection,
            StoreErrorClass::ConcurrentAccess,
            StoreErrorResolution::Retry,
        );
    }

    #[test]
    fn closed_pool_requires_connection_recreation() {
        let classification =
            classify_sqlx(SqliteOperation::AcquireConnection, &sqlx::Error::PoolClosed);

        assert_classification(
            classification,
            StoreErrorScope::Connection,
            StoreErrorClass::Unavailable,
            StoreErrorResolution::Recreate,
        );
    }

    #[test]
    fn stored_semantic_failure_requires_record_repair() {
        let classification = SqliteStoreError::StoredEmptyGroupMembers.classification();

        assert_classification(
            classification,
            StoreErrorScope::Record,
            StoreErrorClass::InvalidData,
            StoreErrorResolution::Repair,
        );
    }

    #[test]
    fn sqlite_corruption_requires_store_repair() {
        let error = database_error(SQLITE_CORRUPT);

        let classification = classify_sqlx(SqliteOperation::ExecuteQuery, &error);

        assert_classification(
            classification,
            StoreErrorScope::Store,
            StoreErrorClass::InvalidData,
            StoreErrorResolution::Repair,
        );
    }

    #[test]
    fn invalid_url_requires_store_reconfiguration() {
        let classification = SqliteStoreError::ParseSqliteUrl {
            database_url: "invalid".to_owned(),
            source: sqlx::Error::Configuration(Box::new(std::io::Error::other("invalid URL"))),
        }
        .classification();

        assert_classification(
            classification,
            StoreErrorScope::Store,
            StoreErrorClass::Configuration,
            StoreErrorResolution::Reconfigure,
        );
    }

    #[test]
    fn sqlite_constraint_retains_conflicting_state_without_assuming_a_resolution() {
        let error = database_error(SQLITE_CONSTRAINT);

        let classification = classify_sqlx(SqliteOperation::ExecuteQuery, &error);

        assert_classification(
            classification,
            StoreErrorScope::Operation,
            StoreErrorClass::ConflictingState,
            StoreErrorResolution::Unknown,
        );
    }

    #[test]
    fn caller_semantic_failure_requires_bug_fix() {
        let classification = SqliteStoreError::EmptyGroupMembers.classification();

        assert_classification(
            classification,
            StoreErrorScope::Operation,
            StoreErrorClass::Contract,
            StoreErrorResolution::FixBug,
        );
    }

    #[test]
    fn sqlite_full_retains_resource_exhaustion_without_assuming_a_resolution() {
        let error = database_error(SQLITE_FULL);

        let classification = classify_sqlx(SqliteOperation::ExecuteQuery, &error);

        assert_classification(
            classification,
            StoreErrorScope::Environment,
            StoreErrorClass::ResourceExhaustion,
            StoreErrorResolution::Unknown,
        );
    }

    #[test]
    fn sqlite_too_big_retains_operation_resource_exhaustion_without_assuming_a_resolution() {
        let error = database_error(SQLITE_TOOBIG);

        let classification = classify_sqlx(SqliteOperation::ExecuteQuery, &error);

        assert_classification(
            classification,
            StoreErrorScope::Operation,
            StoreErrorClass::ResourceExhaustion,
            StoreErrorResolution::Unknown,
        );
    }

    #[test]
    fn store_boundary_preserves_sqlite_result_code_and_source_type() {
        let sqlite_error = SqliteStoreError::Sqlx {
            source: database_error(SQLITE_BUSY_SNAPSHOT),
        };

        let error = StoreError::from(sqlite_error);

        assert_classification(
            error.classification(),
            StoreErrorScope::Transaction,
            StoreErrorClass::ConcurrentAccess,
            StoreErrorResolution::Retry,
        );
        let StoreError::StoreExternal { source, .. } = &error;
        let preserved = source
            .downcast_ref::<SqliteStoreError>()
            .expect("store boundary should retain the concrete SQLite error");
        let SqliteStoreError::Sqlx {
            source: sqlx::Error::Database(database),
        } = preserved
        else {
            panic!("store boundary should retain the SQLx database error");
        };
        assert_eq!(database.code().as_deref(), Some("517"));
    }

    #[test]
    fn unknown_sqlite_result_is_conservative() {
        let error = database_error(30);

        let classification = classify_sqlx(SqliteOperation::ExecuteQuery, &error);

        assert_eq!(classification, StoreErrorClassification::UNKNOWN);
    }
}
