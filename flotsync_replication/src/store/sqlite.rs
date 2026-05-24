use crate::{
    api::{
        DatasetId,
        DatasetRowPatch,
        DatasetRowSlice,
        DatasetRowWrite,
        DatasetRowsBatch,
        DatasetUpdateRecord,
        EncryptedGroupSecurityMaterial,
        EncryptedLocalMemberPrivateKeys,
        EncryptedStoreSecret,
        GroupId,
        LocalMemberPrivateKeysRecord,
        MemberIdentity,
        MemberIndex,
        ReplicationGroupRecord,
        ReplicationRowRecord,
        ReplicationRowSnapshot,
        ReplicationStore,
        ReplicationStoreReadTransaction,
        ReplicationStoreTransaction,
        ReplicationUpdateFilter,
        ReplicationUpdateRecord,
        RowKey,
        RowKeyIterator,
        SchemaSource,
        StoreError,
        StoreSecretCryptoVersion,
        StoreSecretKeyId,
        TrustedMemberPublicKeysRecord,
    },
    runtime::messages::{
        DatasetUpdateMessage,
        UpdateMessage,
        decode_update_proto,
        decode_version_vector_proto,
        encode_update_proto,
        encode_version_vector_proto,
    },
};
use flotsync_core::{
    member::IdentifierParseError,
    versions::{UpdateId, VersionVector},
};
use flotsync_messages::{
    buffa::Message as _,
    codecs::datamodel::{decode_row_snapshot, encode_row_snapshot},
    datamodel as datamodel_proto,
    replication as replication_proto,
    versions as versions_proto,
};
use flotsync_utils::BoxFuture;
use futures_util::{FutureExt, future};
use kompact::prelude::block_on;
use log::warn;
use snafu::prelude::*;
use sqlx::{
    QueryBuilder,
    Row,
    Sqlite,
    SqliteConnection,
    SqlitePool,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};
use std::{
    collections::{HashMap, HashSet},
    error::Error as StdError,
    num::NonZeroUsize,
    path::Path,
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use uuid::Uuid;

const STATEMENT_CACHE_CAPACITY: usize = 64;
const POOL_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(5);

/// In-memory SQLite-backed [`ReplicationStore`] implementation for the first
/// replication storage slice.
///
/// The named in-memory database is owned by the `sqlx` pool. Keeping one
/// minimum pooled connection alive avoids an extra keeper connection while
/// still preserving the shared-cache memory database across transaction
/// acquisitions. `sqlx` caches prepared statements per connection, so the store
/// keeps query shapes stable and relies on a modest per-connection cache rather
/// than trying to share prepared handles globally.
pub struct SqliteReplicationStore {
    local_member: MemberIdentity,
    schema_sources: Arc<HashMap<DatasetId, SchemaSource>>,
    pool: Arc<SqlitePool>,
}

impl SqliteReplicationStore {
    /// Create one empty in-memory store for `local_member`.
    ///
    /// # Errors
    ///
    /// See `StoreError` for failure conditions.
    pub fn in_memory(local_member: MemberIdentity) -> Result<Self, StoreError> {
        Self::in_memory_with_schema_sources(
            local_member,
            std::iter::empty::<(DatasetId, SchemaSource)>(),
        )
    }

    /// Open one disk-backed `SQLite` store for `local_member`.
    ///
    /// # Errors
    ///
    /// See `StoreError` for failure conditions.
    pub fn file(local_member: MemberIdentity, path: impl AsRef<Path>) -> Result<Self, StoreError> {
        Self::file_with_schema_sources(
            local_member,
            path,
            std::iter::empty::<(DatasetId, SchemaSource)>(),
        )
    }

    /// Create one in-memory store with the provided application schema sources.
    ///
    /// # Errors
    ///
    /// See `StoreError` for failure conditions.
    pub fn in_memory_with_schema_sources<I, S>(
        local_member: MemberIdentity,
        schema_sources: I,
    ) -> Result<Self, StoreError>
    where
        I: IntoIterator<Item = (DatasetId, S)>,
        S: Into<SchemaSource>,
    {
        let database_url = format!(
            "sqlite:file:flotsync-replication-{}?mode=memory&cache=shared",
            Uuid::new_v4()
        );
        let connect_options = SqliteConnectOptions::from_str(&database_url)
            .context(ParseSqliteUrlSnafu {
                database_url: database_url.clone(),
            })?
            .foreign_keys(true)
            .statement_cache_capacity(STATEMENT_CACHE_CAPACITY);
        Self::from_connect_options(local_member, schema_sources, connect_options)
    }

    /// Open one disk-backed `SQLite` store with the provided application schema sources.
    ///
    /// # Errors
    ///
    /// See `StoreError` for failure conditions.
    pub fn file_with_schema_sources<I, S>(
        local_member: MemberIdentity,
        path: impl AsRef<Path>,
        schema_sources: I,
    ) -> Result<Self, StoreError>
    where
        I: IntoIterator<Item = (DatasetId, S)>,
        S: Into<SchemaSource>,
    {
        let connect_options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true)
            .statement_cache_capacity(STATEMENT_CACHE_CAPACITY);
        Self::from_connect_options(local_member, schema_sources, connect_options)
    }

    fn from_connect_options<I, S>(
        local_member: MemberIdentity,
        schema_sources: I,
        connect_options: SqliteConnectOptions,
    ) -> Result<Self, StoreError>
    where
        I: IntoIterator<Item = (DatasetId, S)>,
        S: Into<SchemaSource>,
    {
        let pool = block_on(
            SqlitePoolOptions::new()
                .min_connections(1)
                .max_connections(8)
                .acquire_timeout(POOL_ACQUIRE_TIMEOUT)
                .idle_timeout(None)
                .max_lifetime(None)
                .connect_with(connect_options),
        )
        .context(SqlxSnafu)?;
        let mut connection = block_on(pool.acquire()).context(SqlxSnafu)?;
        block_on(initialise_schema(&mut connection))?;
        drop(connection);

        let schema_sources = schema_sources
            .into_iter()
            .map(|(dataset_id, schema)| (dataset_id, schema.into()))
            .collect();
        Ok(Self {
            local_member,
            schema_sources: Arc::new(schema_sources),
            pool: Arc::new(pool),
        })
    }
}

impl ReplicationStore for SqliteReplicationStore {
    fn local_member_identity(&self) -> BoxFuture<'_, Result<MemberIdentity, StoreError>> {
        future::ok(self.local_member.clone()).boxed()
    }

    fn load_dataset_schema(
        &self,
        dataset_id: &DatasetId,
    ) -> BoxFuture<'_, Result<Option<SchemaSource>, StoreError>> {
        future::ok(self.schema_sources.get(dataset_id).cloned()).boxed()
    }

    fn begin_transaction(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ReplicationStoreTransaction>, StoreError>> {
        let pool = self.pool.clone();
        let schema_sources = self.schema_sources.clone();
        async move {
            let connection = pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .context(SqlxSnafu)?;
            Ok(Box::new(SqliteReplicationStoreTransaction::new(
                connection,
                schema_sources,
                SqliteReplicationTransactionKind::Write,
            )) as Box<dyn ReplicationStoreTransaction>)
        }
        .boxed()
    }

    fn begin_read_transaction(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ReplicationStoreReadTransaction>, StoreError>> {
        let pool = self.pool.clone();
        let schema_sources = self.schema_sources.clone();
        async move {
            let connection = pool.begin_with("BEGIN").await.context(SqlxSnafu)?;
            Ok(Box::new(SqliteReplicationStoreTransaction::new(
                connection,
                schema_sources,
                SqliteReplicationTransactionKind::Read,
            )) as Box<dyn ReplicationStoreReadTransaction>)
        }
        .boxed()
    }
}

/// One open store transaction backed by `SQLx`'s transaction guard.
///
/// `connection` becomes `None` after explicit commit or rollback. Dropping an
/// open transaction lets `SQLx` queue a rollback before returning the connection
/// to the pool.
struct SqliteReplicationStoreTransaction {
    connection: Option<SqliteStoreTransaction>,
    schema_sources: Arc<HashMap<DatasetId, SchemaSource>>,
    kind: SqliteReplicationTransactionKind,
}

impl SqliteReplicationStoreTransaction {
    fn new(
        connection: SqliteStoreTransaction,
        schema_sources: Arc<HashMap<DatasetId, SchemaSource>>,
        kind: SqliteReplicationTransactionKind,
    ) -> Self {
        Self {
            connection: Some(connection),
            schema_sources,
            kind,
        }
    }

    fn assert_open_connection(&mut self) -> &mut SqliteStoreTransaction {
        self.connection.as_mut().expect(
            "sqlite replication transaction must not be used after commit, rollback, or release",
        )
    }
}

impl Drop for SqliteReplicationStoreTransaction {
    fn drop(&mut self) {
        if self.connection.is_some() && self.kind == SqliteReplicationTransactionKind::Write {
            warn!(
                "dropping open sqlite replication transaction; SQLx will roll it back before returning the connection to the pool"
            );
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SqliteReplicationTransactionKind {
    /// Read-only transaction; dropping it is normal cleanup and should stay quiet.
    Read,
    /// Mutable transaction; dropping it means the caller abandoned uncommitted writes.
    Write,
}

impl ReplicationStoreReadTransaction for SqliteReplicationStoreTransaction {
    fn load_replication_group<'a>(
        &'a mut self,
        group_id: &'a GroupId,
    ) -> BoxFuture<'a, Result<Option<ReplicationGroupRecord>, StoreError>> {
        async move { load_replication_group(self.assert_open_connection(), group_id).await }.boxed()
    }

    fn load_replication_groups(
        &mut self,
    ) -> BoxFuture<'_, Result<Vec<ReplicationGroupRecord>, StoreError>> {
        async move { load_replication_groups(self.assert_open_connection()).await }.boxed()
    }

    fn load_replication_groups_for_ids<'a>(
        &'a mut self,
        group_ids: &'a HashSet<GroupId>,
    ) -> BoxFuture<'a, Result<Vec<ReplicationGroupRecord>, StoreError>> {
        async move { load_replication_groups_for_ids(self.assert_open_connection(), group_ids).await }
            .boxed()
    }

    fn load_local_member_private_keys<'a>(
        &'a mut self,
        member_id: &'a MemberIdentity,
    ) -> BoxFuture<'a, Result<Option<LocalMemberPrivateKeysRecord>, StoreError>> {
        async move { load_local_member_private_keys(self.assert_open_connection(), member_id).await }
            .boxed()
    }

    fn load_trusted_member_public_keys<'a>(
        &'a mut self,
        member_id: &'a MemberIdentity,
    ) -> BoxFuture<'a, Result<Option<TrustedMemberPublicKeysRecord>, StoreError>> {
        async move { load_trusted_member_public_keys(self.assert_open_connection(), member_id).await }
            .boxed()
    }

    fn load_replication_update<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        update_id: UpdateId,
    ) -> BoxFuture<'a, Result<Option<ReplicationUpdateRecord>, StoreError>> {
        async move {
            load_replication_update(self.assert_open_connection(), group_id, update_id).await
        }
        .boxed()
    }

    fn load_replication_updates<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        filter: ReplicationUpdateFilter,
        limit: Option<NonZeroUsize>,
    ) -> BoxFuture<'a, Result<Vec<ReplicationUpdateRecord>, StoreError>> {
        async move {
            load_replication_updates(self.assert_open_connection(), group_id, filter, limit).await
        }
        .boxed()
    }

    fn load_replication_update_ids<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        filter: ReplicationUpdateFilter,
        limit: Option<NonZeroUsize>,
    ) -> BoxFuture<'a, Result<Vec<UpdateId>, StoreError>> {
        async move {
            load_replication_update_ids(self.assert_open_connection(), group_id, filter, limit)
                .await
        }
        .boxed()
    }

    fn scan_dataset_row_batch<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        dataset_id: &'a DatasetId,
        after: Option<RowKey>,
        limit: NonZeroUsize,
    ) -> BoxFuture<'a, Result<DatasetRowsBatch, StoreError>> {
        let schema_sources = self.schema_sources.clone();
        async move {
            scan_dataset_row_batch(
                self.assert_open_connection(),
                schema_sources.as_ref(),
                group_id,
                dataset_id,
                after,
                limit,
            )
            .await
        }
        .boxed()
    }

    fn release(mut self: Box<Self>) -> BoxFuture<'static, Result<(), StoreError>> {
        async move {
            let connection = self
                .connection
                .take()
                .expect("sqlite replication read transaction must not release twice");
            connection.rollback().await.context(SqlxSnafu)?;
            Ok(())
        }
        .boxed()
    }
}

impl ReplicationStoreTransaction for SqliteReplicationStoreTransaction {
    fn load_replication_group<'a>(
        &'a mut self,
        group_id: &'a GroupId,
    ) -> BoxFuture<'a, Result<Option<ReplicationGroupRecord>, StoreError>> {
        async move { load_replication_group(self.assert_open_connection(), group_id).await }.boxed()
    }

    fn load_replication_groups(
        &mut self,
    ) -> BoxFuture<'_, Result<Vec<ReplicationGroupRecord>, StoreError>> {
        async move { load_replication_groups(self.assert_open_connection()).await }.boxed()
    }

    fn load_replication_groups_for_ids<'a>(
        &'a mut self,
        group_ids: &'a HashSet<GroupId>,
    ) -> BoxFuture<'a, Result<Vec<ReplicationGroupRecord>, StoreError>> {
        async move { load_replication_groups_for_ids(self.assert_open_connection(), group_ids).await }
            .boxed()
    }

    fn insert_replication_group(
        &mut self,
        group: ReplicationGroupRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        async move { insert_replication_group(self.assert_open_connection(), &group).await }.boxed()
    }

    fn load_local_member_private_keys<'a>(
        &'a mut self,
        member_id: &'a MemberIdentity,
    ) -> BoxFuture<'a, Result<Option<LocalMemberPrivateKeysRecord>, StoreError>> {
        async move { load_local_member_private_keys(self.assert_open_connection(), member_id).await }
            .boxed()
    }

    fn ensure_local_member_private_keys(
        &mut self,
        record: LocalMemberPrivateKeysRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        async move { ensure_local_member_private_keys(self.assert_open_connection(), &record).await }
            .boxed()
    }

    fn load_trusted_member_public_keys<'a>(
        &'a mut self,
        member_id: &'a MemberIdentity,
    ) -> BoxFuture<'a, Result<Option<TrustedMemberPublicKeysRecord>, StoreError>> {
        async move { load_trusted_member_public_keys(self.assert_open_connection(), member_id).await }
            .boxed()
    }

    fn ensure_trusted_member_public_keys(
        &mut self,
        record: TrustedMemberPublicKeysRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        async move {
            ensure_trusted_member_public_keys(self.assert_open_connection(), &record).await
        }
        .boxed()
    }

    fn update_replication_group_version_vector<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        version_vector: VersionVector,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        async move {
            update_replication_group_version_vector(
                self.assert_open_connection(),
                group_id,
                &version_vector,
            )
            .await
        }
        .boxed()
    }

    fn load_dataset_rows<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        dataset_id: &'a DatasetId,
        row_keys: &'a mut RowKeyIterator<'a>,
    ) -> BoxFuture<'a, Result<DatasetRowSlice, StoreError>> {
        let schema_sources = self.schema_sources.clone();
        async move {
            load_dataset_rows(
                self.assert_open_connection(),
                schema_sources.as_ref(),
                group_id,
                dataset_id,
                row_keys,
            )
            .await
        }
        .boxed()
    }

    fn apply_dataset_row_patch(
        &mut self,
        patch: DatasetRowPatch,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        let schema_sources = self.schema_sources.clone();
        async move {
            apply_dataset_row_patch(
                self.assert_open_connection(),
                schema_sources.as_ref(),
                &patch,
            )
            .await
        }
        .boxed()
    }

    fn load_replication_update<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        update_id: UpdateId,
    ) -> BoxFuture<'a, Result<Option<ReplicationUpdateRecord>, StoreError>> {
        async move {
            load_replication_update(self.assert_open_connection(), group_id, update_id).await
        }
        .boxed()
    }

    fn load_replication_updates<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        filter: ReplicationUpdateFilter,
        limit: Option<NonZeroUsize>,
    ) -> BoxFuture<'a, Result<Vec<ReplicationUpdateRecord>, StoreError>> {
        async move {
            load_replication_updates(self.assert_open_connection(), group_id, filter, limit).await
        }
        .boxed()
    }

    fn load_replication_update_ids<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        filter: ReplicationUpdateFilter,
        limit: Option<NonZeroUsize>,
    ) -> BoxFuture<'a, Result<Vec<UpdateId>, StoreError>> {
        async move {
            load_replication_update_ids(self.assert_open_connection(), group_id, filter, limit)
                .await
        }
        .boxed()
    }

    fn append_replication_update(
        &mut self,
        update: ReplicationUpdateRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        async move { append_replication_update(self.assert_open_connection(), &update).await }
            .boxed()
    }

    fn mark_replication_update_applied<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        update_id: UpdateId,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        async move {
            mark_replication_update_applied(self.assert_open_connection(), group_id, update_id)
                .await
        }
        .boxed()
    }

    fn commit(mut self: Box<Self>) -> BoxFuture<'static, Result<(), StoreError>> {
        async move {
            let connection = self
                .connection
                .take()
                .expect("sqlite replication transaction must not commit twice");
            connection.commit().await.context(SqlxSnafu)?;
            Ok(())
        }
        .boxed()
    }

    fn rollback(mut self: Box<Self>) -> BoxFuture<'static, Result<(), StoreError>> {
        async move {
            let connection = self
                .connection
                .take()
                .expect("sqlite replication transaction must not roll back twice");
            connection.rollback().await.context(SqlxSnafu)?;
            Ok(())
        }
        .boxed()
    }
}

type SqliteStoreConnection = SqliteConnection;
type SqliteStoreTransaction = sqlx::Transaction<'static, Sqlite>;

/// `SQLite` compares BLOBs lexicographically. Fixed-width big-endian encodings
/// therefore preserve the natural ordering of `u64` values across the full
/// range, so `ORDER BY update_version` remains numerically correct even above
/// `i64::MAX`.
const UPDATE_VERSION_SORT_KEY_BYTES: usize = 8;

async fn initialise_schema(connection: &mut SqliteStoreConnection) -> Result<(), StoreError> {
    let schema_statements = [
        "PRAGMA foreign_keys = ON;",
        "
CREATE TABLE IF NOT EXISTS replication_groups (
    group_id TEXT PRIMARY KEY NOT NULL,
    member_count INTEGER NOT NULL,
    local_member_index INTEGER NOT NULL,
    version_vector BLOB NOT NULL,
    group_secret_crypto_version INTEGER NOT NULL,
    group_secret_key_id TEXT NOT NULL,
    group_secret_nonce BLOB NOT NULL,
    group_secret_ciphertext BLOB NOT NULL
);
",
        "
CREATE TABLE IF NOT EXISTS group_members (
    group_id TEXT NOT NULL,
    member_index INTEGER NOT NULL,
    member_identity TEXT NOT NULL,
    PRIMARY KEY (group_id, member_index),
    UNIQUE (group_id, member_identity),
    FOREIGN KEY (group_id) REFERENCES replication_groups(group_id) ON DELETE CASCADE
);
",
        "
CREATE TABLE IF NOT EXISTS datasets (
    group_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    PRIMARY KEY (group_id, dataset_id),
    FOREIGN KEY (group_id) REFERENCES replication_groups(group_id) ON DELETE CASCADE
);
",
        "
CREATE TABLE IF NOT EXISTS dataset_rows (
    group_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    row_key TEXT NOT NULL,
    row_snapshot BLOB NOT NULL,
    row_tombstoned INTEGER NOT NULL DEFAULT 0,
    row_last_changed_versions BLOB NOT NULL,
    PRIMARY KEY (group_id, dataset_id, row_key),
    FOREIGN KEY (group_id, dataset_id) REFERENCES datasets(group_id, dataset_id) ON DELETE CASCADE
);
",
        "
CREATE TABLE IF NOT EXISTS dataset_updates (
    group_id TEXT NOT NULL,
    update_node_index INTEGER NOT NULL,
    update_version BLOB NOT NULL,
    sender TEXT NOT NULL,
    applied_locally INTEGER NOT NULL,
    update_message BLOB NOT NULL,
    PRIMARY KEY (group_id, update_node_index, update_version),
    FOREIGN KEY (group_id) REFERENCES replication_groups(group_id) ON DELETE CASCADE
);
",
        "
CREATE TABLE IF NOT EXISTS local_members (
    member_identity TEXT PRIMARY KEY NOT NULL,
    private_keys_crypto_version INTEGER NOT NULL,
    private_keys_key_id TEXT NOT NULL,
    private_keys_nonce BLOB NOT NULL,
    private_keys_ciphertext BLOB NOT NULL
);
",
        "
CREATE TABLE IF NOT EXISTS known_peers (
    member_identity TEXT PRIMARY KEY NOT NULL,
    signing_public_key BLOB NOT NULL,
    encryption_public_key BLOB NOT NULL
);
",
    ];
    for statement in schema_statements {
        sqlx::query(statement)
            .execute(&mut *connection)
            .await
            .context(SqlxSnafu)?;
    }
    Ok(())
}

async fn load_replication_group(
    connection: &mut SqliteStoreConnection,
    group_id: &GroupId,
) -> Result<Option<ReplicationGroupRecord>, StoreError> {
    let row = sqlx::query(
        "
SELECT
    member_count,
    local_member_index,
    version_vector,
    group_secret_crypto_version,
    group_secret_key_id,
    group_secret_nonce,
    group_secret_ciphertext
FROM replication_groups
WHERE group_id = ?1
",
    )
    .bind(group_id.to_string())
    .fetch_optional(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    let Some(row) = row else {
        return Ok(None);
    };

    let member_count = decode_non_zero_member_count(row.get::<i64, _>("member_count"))?;
    let local_member_index =
        decode_member_index(row.get::<i64, _>("local_member_index"), member_count)?;
    let version_vector =
        decode_stored_version_vector(&row.get::<Vec<u8>, _>("version_vector"), member_count)?;
    let encrypted_group_secret = decode_encrypted_store_secret(
        row.get("group_secret_crypto_version"),
        row.get("group_secret_key_id"),
        row.get("group_secret_nonce"),
        row.get("group_secret_ciphertext"),
    )?;
    let security_material = EncryptedGroupSecurityMaterial {
        encrypted_group_secret,
    };
    let members = load_group_members(connection, group_id, member_count).await?;

    Ok(Some(ReplicationGroupRecord {
        group_id: *group_id,
        members,
        local_member_index,
        version_vector,
        security_material,
    }))
}

async fn load_replication_groups(
    connection: &mut SqliteStoreConnection,
) -> Result<Vec<ReplicationGroupRecord>, StoreError> {
    let group_ids = sqlx::query_scalar::<_, String>(
        "
SELECT group_id
FROM replication_groups
ORDER BY group_id
",
    )
    .fetch_all(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    let mut groups = Vec::with_capacity(group_ids.len());
    for group_id in group_ids {
        let group_id = decode_group_id(&group_id)?;
        if let Some(group) = load_replication_group(connection, &group_id).await? {
            groups.push(group);
        }
    }
    Ok(groups)
}

async fn load_replication_groups_for_ids(
    connection: &mut SqliteStoreConnection,
    requested_group_ids: &HashSet<GroupId>,
) -> Result<Vec<ReplicationGroupRecord>, StoreError> {
    if requested_group_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut query_builder = QueryBuilder::<Sqlite>::new(
        "
SELECT group_id
FROM replication_groups
WHERE group_id IN (",
    );
    {
        let mut separated = query_builder.separated(", ");
        for group_id in requested_group_ids {
            separated.push_bind(group_id.to_string());
        }
    }
    query_builder.push(") ORDER BY group_id");

    let group_ids = query_builder
        .build_query_scalar::<String>()
        .fetch_all(&mut *connection)
        .await
        .context(SqlxSnafu)?;
    let mut groups = Vec::with_capacity(group_ids.len());
    for group_id in group_ids {
        let group_id = decode_group_id(&group_id)?;
        if let Some(group) = load_replication_group(connection, &group_id).await? {
            groups.push(group);
        }
    }
    Ok(groups)
}

async fn insert_replication_group(
    connection: &mut SqliteStoreConnection,
    group: &ReplicationGroupRecord,
) -> Result<(), StoreError> {
    let member_count = group.member_count();
    ensure_member_index_in_bounds(group.local_member_index, member_count)?;

    let version_vector = encode_stored_version_vector(&group.version_vector);
    let stored_member_count =
        i64::try_from(member_count.get()).context(MemberCountOverflowSnafu)?;
    sqlx::query(
        "
INSERT INTO replication_groups (
    group_id,
    member_count,
    local_member_index,
    version_vector,
    group_secret_crypto_version,
    group_secret_key_id,
    group_secret_nonce,
    group_secret_ciphertext
)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
",
    )
    .bind(group.group_id.to_string())
    .bind(stored_member_count)
    .bind(i64::from(group.local_member_index.as_u32()))
    .bind(version_vector)
    .bind(i64::from(
        group
            .security_material
            .encrypted_group_secret
            .crypto_version
            .as_u16(),
    ))
    .bind(
        group
            .security_material
            .encrypted_group_secret
            .key_id
            .to_string(),
    )
    .bind(
        group
            .security_material
            .encrypted_group_secret
            .nonce
            .as_ref(),
    )
    .bind(
        group
            .security_material
            .encrypted_group_secret
            .ciphertext
            .as_ref(),
    )
    .execute(&mut *connection)
    .await
    .context(SqlxSnafu)?;

    for (member_index, member) in group.members.iter().enumerate() {
        let member_index = i64::try_from(member_index).context(MemberCountOverflowSnafu)?;
        sqlx::query(
            "
INSERT INTO group_members (group_id, member_index, member_identity)
VALUES (?1, ?2, ?3)
",
        )
        .bind(group.group_id.to_string())
        .bind(member_index)
        .bind(member.to_string())
        .execute(&mut *connection)
        .await
        .context(SqlxSnafu)?;
    }
    Ok(())
}

async fn load_local_member_private_keys(
    connection: &mut SqliteStoreConnection,
    member_id: &MemberIdentity,
) -> Result<Option<LocalMemberPrivateKeysRecord>, StoreError> {
    let row = sqlx::query(
        "
SELECT private_keys_crypto_version, private_keys_key_id, private_keys_nonce, private_keys_ciphertext
FROM local_members
WHERE member_identity = ?1
",
    )
    .bind(member_id.to_string())
    .fetch_optional(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    let Some(row) = row else {
        return Ok(None);
    };

    let encrypted_private_keys = decode_encrypted_store_secret(
        row.get("private_keys_crypto_version"),
        row.get("private_keys_key_id"),
        row.get("private_keys_nonce"),
        row.get("private_keys_ciphertext"),
    )?;
    Ok(Some(LocalMemberPrivateKeysRecord {
        member_id: member_id.clone(),
        private_keys: EncryptedLocalMemberPrivateKeys {
            secret: encrypted_private_keys,
        },
    }))
}

async fn ensure_local_member_private_keys(
    connection: &mut SqliteStoreConnection,
    record: &LocalMemberPrivateKeysRecord,
) -> Result<(), StoreError> {
    if let Some(existing) = load_local_member_private_keys(connection, &record.member_id).await? {
        ensure!(
            existing == *record,
            ConflictingMemberSecurityMaterialSnafu {
                object: "local member private keys",
                member_id: record.member_id.clone(),
            }
        );
        return Ok(());
    }

    let secret = &record.private_keys.secret;
    sqlx::query(
        "
INSERT INTO local_members (
    member_identity,
    private_keys_crypto_version,
    private_keys_key_id,
    private_keys_nonce,
    private_keys_ciphertext
)
VALUES (?1, ?2, ?3, ?4, ?5)
",
    )
    .bind(record.member_id.to_string())
    .bind(i64::from(secret.crypto_version.as_u16()))
    .bind(secret.key_id.to_string())
    .bind(secret.nonce.as_ref())
    .bind(secret.ciphertext.as_ref())
    .execute(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    Ok(())
}

async fn load_trusted_member_public_keys(
    connection: &mut SqliteStoreConnection,
    member_id: &MemberIdentity,
) -> Result<Option<TrustedMemberPublicKeysRecord>, StoreError> {
    let row = sqlx::query(
        "
SELECT signing_public_key, encryption_public_key
FROM known_peers
WHERE member_identity = ?1
",
    )
    .bind(member_id.to_string())
    .fetch_optional(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    let Some(row) = row else {
        return Ok(None);
    };

    Ok(Some(TrustedMemberPublicKeysRecord {
        member_id: member_id.clone(),
        signing_public_key: row
            .get::<Vec<u8>, _>("signing_public_key")
            .into_boxed_slice(),
        encryption_public_key: row
            .get::<Vec<u8>, _>("encryption_public_key")
            .into_boxed_slice(),
    }))
}

async fn ensure_trusted_member_public_keys(
    connection: &mut SqliteStoreConnection,
    record: &TrustedMemberPublicKeysRecord,
) -> Result<(), StoreError> {
    if let Some(existing) = load_trusted_member_public_keys(connection, &record.member_id).await? {
        ensure!(
            existing == *record,
            ConflictingMemberSecurityMaterialSnafu {
                object: "trusted member public keys",
                member_id: record.member_id.clone(),
            }
        );
        return Ok(());
    }

    sqlx::query(
        "
INSERT INTO known_peers (
    member_identity,
    signing_public_key,
    encryption_public_key
)
VALUES (?1, ?2, ?3)
",
    )
    .bind(record.member_id.to_string())
    .bind(record.signing_public_key.as_ref())
    .bind(record.encryption_public_key.as_ref())
    .execute(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    Ok(())
}

async fn update_replication_group_version_vector(
    connection: &mut SqliteStoreConnection,
    group_id: &GroupId,
    version_vector: &VersionVector,
) -> Result<(), StoreError> {
    let rows_affected = sqlx::query(
        "
UPDATE replication_groups
SET version_vector = ?2
WHERE group_id = ?1
",
    )
    .bind(group_id.to_string())
    .bind(encode_stored_version_vector(version_vector))
    .execute(&mut *connection)
    .await
    .context(SqlxSnafu)?
    .rows_affected();
    ensure!(
        rows_affected == 1,
        MissingStoredGroupSnafu {
            group_id: *group_id
        }
    );
    Ok(())
}

async fn load_dataset_rows(
    connection: &mut SqliteStoreConnection,
    schema_sources: &HashMap<DatasetId, SchemaSource>,
    group_id: &GroupId,
    dataset_id: &DatasetId,
    row_keys: &mut RowKeyIterator<'_>,
) -> Result<DatasetRowSlice, StoreError> {
    let dataset_exists = dataset_exists_in_group(connection, group_id, dataset_id).await?;
    let mut row_keys = row_keys.peekable();
    if row_keys.peek().is_none() {
        return Ok(DatasetRowSlice {
            group_id: *group_id,
            dataset_id: dataset_id.clone(),
            dataset_exists,
            rows: HashMap::new(),
        });
    }
    let mut rows = row_keys
        .map(|row_key| (*row_key, None))
        .collect::<HashMap<_, _>>();
    if !dataset_exists {
        return Ok(DatasetRowSlice {
            group_id: *group_id,
            dataset_id: dataset_id.clone(),
            dataset_exists,
            rows,
        });
    }

    let schema = schema_sources
        .get(dataset_id)
        .cloned()
        .context(MissingSchemaSnafu {
            dataset_id: dataset_id.clone(),
        })?;
    let member_count = load_group_member_count(connection, group_id).await?;
    let mut query_builder = QueryBuilder::<Sqlite>::new(
        "
SELECT row_key, row_snapshot, row_tombstoned, row_last_changed_versions
FROM dataset_rows
WHERE group_id = ",
    );
    query_builder.push_bind(group_id.to_string());
    query_builder.push(" AND dataset_id = ");
    query_builder.push_bind(dataset_id.as_str());
    query_builder.push(" AND row_key IN (");
    {
        let mut separated = query_builder.separated(", ");
        for row_key in rows.keys() {
            separated.push_bind(row_key.to_string());
        }
    }
    query_builder.push(")");
    let stored_rows = query_builder
        .build()
        .fetch_all(&mut *connection)
        .await
        .context(SqlxSnafu)?;
    for row in stored_rows {
        let row_key = decode_row_key(&row.get::<String, _>("row_key"))?;
        let row_snapshot = decode_dataset_row_snapshot(
            schema.as_schema(),
            &row.get::<Vec<u8>, _>("row_snapshot"),
        )?;
        rows.insert(
            row_key,
            Some(ReplicationRowRecord {
                row_id: row_key,
                snapshot: row_snapshot,
                tombstoned: row.get::<bool, _>("row_tombstoned"),
                last_changed_versions: decode_dataset_row_last_changed_versions(
                    &row,
                    member_count,
                )?,
            }),
        );
    }
    Ok(DatasetRowSlice {
        group_id: *group_id,
        dataset_id: dataset_id.clone(),
        dataset_exists,
        rows,
    })
}

/// Scan rows in lexicographic row-key order.
///
/// `after` is an exclusive lower bound. When the result contains exactly
/// `limit` rows, `next_after` is set to the last returned row key so callers can
/// continue with `row_key > next_after`.
async fn scan_dataset_row_batch(
    connection: &mut SqliteStoreConnection,
    schema_sources: &HashMap<DatasetId, SchemaSource>,
    group_id: &GroupId,
    dataset_id: &DatasetId,
    after: Option<RowKey>,
    limit: NonZeroUsize,
) -> Result<DatasetRowsBatch, StoreError> {
    let dataset_exists = dataset_exists_in_group(connection, group_id, dataset_id).await?;
    if !dataset_exists {
        return Ok(DatasetRowsBatch {
            group_id: *group_id,
            dataset_id: dataset_id.clone(),
            dataset_exists,
            rows: Vec::new(),
            next_after: None,
        });
    }

    let schema = schema_sources
        .get(dataset_id)
        .cloned()
        .context(MissingSchemaSnafu {
            dataset_id: dataset_id.clone(),
        })?;
    let member_count = load_group_member_count(connection, group_id).await?;
    let mut query_builder = QueryBuilder::<Sqlite>::new(
        "
SELECT row_key, row_snapshot, row_tombstoned, row_last_changed_versions
FROM dataset_rows
WHERE group_id = ",
    );
    query_builder.push_bind(group_id.to_string());
    query_builder.push(" AND dataset_id = ");
    query_builder.push_bind(dataset_id.as_str());
    if let Some(after) = after {
        query_builder.push(" AND row_key > ");
        query_builder.push_bind(after.to_string());
    }
    query_builder.push(" ORDER BY row_key LIMIT ");
    query_builder.push_bind(i64::try_from(limit.get()).context(RowLimitOverflowSnafu)?);

    let stored_rows = query_builder
        .build()
        .fetch_all(&mut *connection)
        .await
        .context(SqlxSnafu)?;
    let mut rows = Vec::with_capacity(stored_rows.len());
    for row in stored_rows {
        let row_key = decode_row_key(&row.get::<String, _>("row_key"))?;
        let row_snapshot = decode_dataset_row_snapshot(
            schema.as_schema(),
            &row.get::<Vec<u8>, _>("row_snapshot"),
        )?;
        rows.push(ReplicationRowRecord {
            row_id: row_key,
            snapshot: row_snapshot,
            tombstoned: row.get::<bool, _>("row_tombstoned"),
            last_changed_versions: decode_dataset_row_last_changed_versions(&row, member_count)?,
        });
    }
    let next_after = if rows.len() == limit.get() {
        rows.last().map(|row| row.row_id)
    } else {
        None
    };
    Ok(DatasetRowsBatch {
        group_id: *group_id,
        dataset_id: dataset_id.clone(),
        dataset_exists,
        rows,
        next_after,
    })
}

async fn apply_dataset_row_patch(
    connection: &mut SqliteStoreConnection,
    schema_sources: &HashMap<DatasetId, SchemaSource>,
    patch: &DatasetRowPatch,
) -> Result<(), StoreError> {
    if patch.actions.is_empty() {
        return Ok(());
    }

    let schema = schema_sources
        .get(&patch.dataset_id)
        .cloned()
        .context(MissingSchemaSnafu {
            dataset_id: patch.dataset_id.clone(),
        })?;
    ensure_dataset_exists(connection, &patch.group_id, &patch.dataset_id).await?;

    for action in &patch.actions {
        let (row_key, snapshot, tombstoned) = match action {
            DatasetRowWrite::UpsertActive { row_key, snapshot } => {
                ensure_dataset_row_upsert_active_is_valid(
                    connection,
                    &patch.group_id,
                    &patch.dataset_id,
                    row_key,
                )
                .await?;
                (row_key, snapshot, false)
            }
            DatasetRowWrite::UpsertTombstone { row_key, snapshot } => (row_key, snapshot, true),
        };
        let row_snapshot = encode_dataset_row_snapshot(schema.as_schema(), snapshot)?;
        sqlx::query(
            "
INSERT INTO dataset_rows (
    group_id,
    dataset_id,
    row_key,
    row_snapshot,
    row_tombstoned,
    row_last_changed_versions
)
VALUES (?1, ?2, ?3, ?4, ?5, ?6)
ON CONFLICT(group_id, dataset_id, row_key) DO UPDATE
SET row_snapshot = excluded.row_snapshot,
    row_tombstoned = excluded.row_tombstoned,
    row_last_changed_versions = excluded.row_last_changed_versions
",
        )
        .bind(patch.group_id.to_string())
        .bind(patch.dataset_id.as_str())
        .bind(row_key.to_string())
        .bind(row_snapshot)
        .bind(tombstoned)
        .bind(encode_stored_version_vector(&patch.last_changed_versions))
        .execute(&mut *connection)
        .await
        .context(SqlxSnafu)?;
    }
    Ok(())
}

async fn ensure_dataset_row_upsert_active_is_valid(
    connection: &mut SqliteStoreConnection,
    group_id: &GroupId,
    dataset_id: &DatasetId,
    row_key: &RowKey,
) -> Result<(), StoreError> {
    let existing_tombstoned =
        load_dataset_row_tombstoned(connection, group_id, dataset_id, row_key).await?;
    ensure!(
        existing_tombstoned != Some(true),
        InvalidDatasetRowStateTransitionSnafu {
            group_id: *group_id,
            dataset_id: dataset_id.clone(),
            row_key: *row_key,
            from: "tombstone",
            to: "active",
        }
    );
    Ok(())
}

async fn load_dataset_row_tombstoned(
    connection: &mut SqliteStoreConnection,
    group_id: &GroupId,
    dataset_id: &DatasetId,
    row_key: &RowKey,
) -> Result<Option<bool>, StoreError> {
    let row = sqlx::query(
        "
SELECT row_tombstoned
FROM dataset_rows
WHERE group_id = ?1 AND dataset_id = ?2 AND row_key = ?3
",
    )
    .bind(group_id.to_string())
    .bind(dataset_id.as_str())
    .bind(row_key.to_string())
    .fetch_optional(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    Ok(row.map(|row| row.get::<bool, _>("row_tombstoned")))
}

async fn load_replication_update(
    connection: &mut SqliteStoreConnection,
    group_id: &GroupId,
    update_id: UpdateId,
) -> Result<Option<ReplicationUpdateRecord>, StoreError> {
    let member_count = load_group_member_count(connection, group_id).await?;
    let row = sqlx::query(
        "
SELECT update_node_index, update_version, sender, applied_locally, update_message
FROM dataset_updates
WHERE group_id = ?1
  AND update_node_index = ?2
  AND update_version = ?3
",
    )
    .bind(group_id.to_string())
    .bind(i64::from(update_id.node_index))
    .bind(encode_update_version_sort_key_vec(update_id.version))
    .fetch_optional(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    let Some(row) = row else {
        return Ok(None);
    };
    Ok(Some(decode_stored_update_row(
        group_id,
        member_count,
        update_id,
        &row,
    )?))
}

async fn load_replication_updates(
    connection: &mut SqliteStoreConnection,
    group_id: &GroupId,
    filter: ReplicationUpdateFilter,
    limit: Option<NonZeroUsize>,
) -> Result<Vec<ReplicationUpdateRecord>, StoreError> {
    let member_count = load_group_member_count(connection, group_id).await?;
    let mut query_builder = QueryBuilder::<Sqlite>::new(
        "
SELECT update_node_index, update_version, sender, applied_locally, update_message
FROM dataset_updates
WHERE group_id = ",
    );
    query_builder.push_bind(group_id.to_string());
    push_replication_update_filter(&mut query_builder, filter);

    query_builder.push(" ORDER BY update_version, update_node_index");
    if let Some(limit) = limit {
        query_builder.push(" LIMIT ");
        query_builder.push_bind(i64::try_from(limit.get()).context(RowLimitOverflowSnafu)?);
    }
    let rows = query_builder
        .build()
        .fetch_all(&mut *connection)
        .await
        .context(SqlxSnafu)?;

    let mut updates = Vec::with_capacity(rows.len());
    for row in rows {
        let update_id = UpdateId {
            node_index: decode_member_index_value(row.get::<i64, _>("update_node_index"))?,
            version: decode_update_version_sort_key(&row.get::<Vec<u8>, _>("update_version"))?,
        };
        updates.push(decode_stored_update_row(
            group_id,
            member_count,
            update_id,
            &row,
        )?);
    }
    Ok(updates)
}

async fn load_replication_update_ids(
    connection: &mut SqliteStoreConnection,
    group_id: &GroupId,
    filter: ReplicationUpdateFilter,
    limit: Option<NonZeroUsize>,
) -> Result<Vec<UpdateId>, StoreError> {
    let mut query_builder = QueryBuilder::<Sqlite>::new(
        "
SELECT update_node_index, update_version
FROM dataset_updates
WHERE group_id = ",
    );
    query_builder.push_bind(group_id.to_string());
    push_replication_update_filter(&mut query_builder, filter);

    query_builder.push(" ORDER BY update_version, update_node_index");
    if let Some(limit) = limit {
        query_builder.push(" LIMIT ");
        query_builder.push_bind(i64::try_from(limit.get()).context(RowLimitOverflowSnafu)?);
    }
    let rows = query_builder
        .build()
        .fetch_all(&mut *connection)
        .await
        .context(SqlxSnafu)?;

    let mut update_ids = Vec::with_capacity(rows.len());
    for row in rows {
        update_ids.push(UpdateId {
            node_index: decode_member_index_value(row.get::<i64, _>("update_node_index"))?,
            version: decode_update_version_sort_key(&row.get::<Vec<u8>, _>("update_version"))?,
        });
    }
    Ok(update_ids)
}

fn push_replication_update_filter(
    query_builder: &mut QueryBuilder<Sqlite>,
    filter: ReplicationUpdateFilter,
) {
    match filter {
        ReplicationUpdateFilter::All => {}
        ReplicationUpdateFilter::PendingApply => {
            query_builder.push(" AND applied_locally = ");
            query_builder.push_bind(false);
        }
        ReplicationUpdateFilter::Applied => {
            query_builder.push(" AND applied_locally = ");
            query_builder.push_bind(true);
        }
        ReplicationUpdateFilter::ProducerRange {
            producer_index,
            start_version,
            end_version,
        } => {
            query_builder.push(" AND update_node_index = ");
            query_builder.push_bind(i64::from(producer_index.as_u32()));
            query_builder.push(" AND update_version >= ");
            query_builder.push_bind(encode_update_version_sort_key_vec(start_version));
            query_builder.push(" AND update_version <= ");
            query_builder.push_bind(encode_update_version_sort_key_vec(end_version));
        }
    }
}

async fn append_replication_update(
    connection: &mut SqliteStoreConnection,
    update: &ReplicationUpdateRecord,
) -> Result<(), StoreError> {
    let update_message = encode_stored_update(update);
    sqlx::query(
        "
INSERT INTO dataset_updates (
    group_id,
    update_node_index,
    update_version,
    sender,
    applied_locally,
    update_message
)
VALUES (?1, ?2, ?3, ?4, ?5, ?6)
",
    )
    .bind(update.group_id.to_string())
    .bind(i64::from(update.update_id.node_index))
    .bind(encode_update_version_sort_key_vec(update.update_id.version))
    .bind(update.sender.to_string())
    .bind(update.applied_locally)
    .bind(update_message)
    .execute(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    Ok(())
}

async fn mark_replication_update_applied(
    connection: &mut SqliteStoreConnection,
    group_id: &GroupId,
    update_id: UpdateId,
) -> Result<(), StoreError> {
    let rows_affected = sqlx::query(
        "
UPDATE dataset_updates
SET applied_locally = 1
WHERE group_id = ?1
  AND update_node_index = ?2
  AND update_version = ?3
",
    )
    .bind(group_id.to_string())
    .bind(i64::from(update_id.node_index))
    .bind(encode_update_version_sort_key_vec(update_id.version))
    .execute(&mut *connection)
    .await
    .context(SqlxSnafu)?
    .rows_affected();
    ensure!(
        rows_affected == 1,
        MissingStoredUpdateSnafu {
            group_id: *group_id,
            update_id,
        }
    );
    Ok(())
}

async fn load_group_members(
    connection: &mut SqliteStoreConnection,
    group_id: &GroupId,
    expected_member_count: NonZeroUsize,
) -> Result<Vec<MemberIdentity>, StoreError> {
    let raw_members = sqlx::query_scalar::<_, String>(
        "
SELECT member_identity
FROM group_members
WHERE group_id = ?1
ORDER BY member_index
",
    )
    .bind(group_id.to_string())
    .fetch_all(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    ensure!(
        raw_members.len() == expected_member_count.get(),
        StoredGroupMemberCountMismatchSnafu {
            group_id: *group_id,
            expected_member_count: expected_member_count.get(),
            actual_member_count: raw_members.len(),
        }
    );

    let mut members = Vec::with_capacity(raw_members.len());
    for raw_member in raw_members {
        members.push(decode_member_identity(&raw_member)?);
    }
    Ok(members)
}

async fn load_group_member_count(
    connection: &mut SqliteStoreConnection,
    group_id: &GroupId,
) -> Result<NonZeroUsize, StoreError> {
    let member_count = sqlx::query_scalar::<_, i64>(
        "
SELECT member_count
FROM replication_groups
WHERE group_id = ?1
",
    )
    .bind(group_id.to_string())
    .fetch_optional(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    let Some(member_count) = member_count else {
        return MissingStoredGroupSnafu {
            group_id: *group_id,
        }
        .fail()
        .map_err(StoreError::from);
    };
    decode_non_zero_member_count(member_count)
}

async fn dataset_exists_in_group(
    connection: &mut SqliteStoreConnection,
    group_id: &GroupId,
    dataset_id: &DatasetId,
) -> Result<bool, StoreError> {
    let exists = sqlx::query_scalar::<_, i64>(
        "
SELECT 1
FROM datasets
WHERE group_id = ?1 AND dataset_id = ?2
",
    )
    .bind(group_id.to_string())
    .bind(dataset_id.as_str())
    .fetch_optional(&mut *connection)
    .await
    .context(SqlxSnafu)?
    .is_some();
    Ok(exists)
}

async fn ensure_dataset_exists(
    connection: &mut SqliteStoreConnection,
    group_id: &GroupId,
    dataset_id: &DatasetId,
) -> Result<(), StoreError> {
    // Persist the parent dataset entry even when this snapshot currently has no rows.
    sqlx::query(
        "
INSERT INTO datasets (group_id, dataset_id)
VALUES (?1, ?2)
ON CONFLICT(group_id, dataset_id) DO NOTHING
",
    )
    .bind(group_id.to_string())
    .bind(dataset_id.as_str())
    .execute(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    Ok(())
}

fn encode_dataset_row_snapshot(
    schema: &flotsync_data_types::schema::Schema,
    row: &ReplicationRowSnapshot,
) -> Result<Vec<u8>, StoreError> {
    let row = encode_row_snapshot(row, schema)
        .map_err(|source| invalid_stored_object("dataset row snapshot", source))?;
    Ok(row.encode_to_bytes().to_vec())
}

fn decode_dataset_row_snapshot(
    schema: &flotsync_data_types::schema::Schema,
    bytes: &[u8],
) -> Result<ReplicationRowSnapshot, StoreError> {
    let row = datamodel_proto::RowSnapshot::decode_from_slice(bytes).map_err(|source| {
        SqliteStoreError::DecodeStoredProto {
            object: "dataset row snapshot",
            source,
        }
    })?;
    decode_row_snapshot(row, schema)
        .map_err(|source| invalid_stored_object("dataset row snapshot", source))
}

fn decode_dataset_row_last_changed_versions(
    row: &sqlx::sqlite::SqliteRow,
    member_count: NonZeroUsize,
) -> Result<VersionVector, StoreError> {
    let versions = row
        .try_get::<Vec<u8>, _>("row_last_changed_versions")
        .context(SqlxSnafu)?;
    decode_stored_version_vector(&versions, member_count)
}

fn encode_stored_version_vector(version_vector: &VersionVector) -> Vec<u8> {
    encode_version_vector_proto(version_vector)
        .encode_to_bytes()
        .to_vec()
}

fn decode_stored_version_vector(
    bytes: &[u8],
    member_count: NonZeroUsize,
) -> Result<VersionVector, StoreError> {
    let version_vector =
        versions_proto::VersionVector::decode_from_slice(bytes).map_err(|source| {
            SqliteStoreError::DecodeStoredProto {
                object: "version vector",
                source,
            }
        })?;
    decode_version_vector_proto(version_vector, member_count)
        .map_err(|source| invalid_stored_object("version vector", source))
}

fn encode_stored_update(update: &ReplicationUpdateRecord) -> Vec<u8> {
    let message = UpdateMessage {
        group_id: update.group_id,
        update_id: update.update_id,
        read_versions: update.read_versions.clone(),
        dataset_updates: update
            .dataset_updates
            .iter()
            .map(|dataset_update| DatasetUpdateMessage {
                dataset_id: dataset_update.dataset_id.clone(),
                operations: dataset_update.operations.clone(),
            })
            .collect(),
    };
    encode_update_proto(&message).encode_to_bytes().to_vec()
}

fn decode_stored_update_row(
    expected_group_id: &GroupId,
    member_count: NonZeroUsize,
    update_id: UpdateId,
    row: &sqlx::sqlite::SqliteRow,
) -> Result<ReplicationUpdateRecord, StoreError> {
    let sender = decode_member_identity(&row.get::<String, _>("sender"))?;
    let applied_locally = row.get::<bool, _>("applied_locally");
    let update_message =
        replication_proto::Update::decode_from_slice(&row.get::<Vec<u8>, _>("update_message"))
            .map_err(|source| SqliteStoreError::DecodeStoredProto {
                object: "update",
                source,
            })?;
    let message = decode_update_proto(update_message, member_count)
        .map_err(|source| invalid_stored_object("update", source))?;
    ensure!(
        message.group_id == *expected_group_id,
        StoredUpdateGroupMismatchSnafu {
            expected_group_id: *expected_group_id,
            actual_group_id: message.group_id,
        }
    );
    ensure!(
        message.update_id == update_id,
        StoredUpdateIdMismatchSnafu {
            expected_update_id: update_id,
            actual_update_id: message.update_id,
        }
    );
    Ok(ReplicationUpdateRecord {
        group_id: *expected_group_id,
        update_id,
        sender,
        read_versions: message.read_versions,
        dataset_updates: message
            .dataset_updates
            .into_iter()
            .map(|dataset_update| DatasetUpdateRecord {
                dataset_id: dataset_update.dataset_id,
                operations: dataset_update.operations,
            })
            .collect(),
        applied_locally,
    })
}

fn encode_update_version_sort_key(version: u64) -> [u8; UPDATE_VERSION_SORT_KEY_BYTES] {
    version.to_be_bytes()
}

fn encode_update_version_sort_key_vec(version: u64) -> Vec<u8> {
    encode_update_version_sort_key(version).to_vec()
}

fn decode_update_version_sort_key(bytes: &[u8]) -> Result<u64, StoreError> {
    let bytes: [u8; UPDATE_VERSION_SORT_KEY_BYTES] =
        bytes
            .try_into()
            .map_err(|_| SqliteStoreError::InvalidStoredSortKey {
                object: "update version",
                len: bytes.len(),
            })?;
    Ok(u64::from_be_bytes(bytes))
}

/// Decode one encrypted secret cell from `SQLite` scalar column values.
#[allow(
    clippy::needless_pass_by_value,
    reason = "Owned strings keep sqlx row.get call sites type-inference friendly."
)]
fn decode_encrypted_store_secret(
    raw_crypto_version: i64,
    key_id: String,
    nonce: Vec<u8>,
    ciphertext: Vec<u8>,
) -> Result<EncryptedStoreSecret, StoreError> {
    let crypto_version = u16::try_from(raw_crypto_version)
        .context(SecretCryptoVersionOverflowSnafu)
        .map_err(StoreError::from)?;
    let key_id: StoreSecretKeyId = key_id
        .parse()
        .context(InvalidStoreSecretKeyIdSnafu)
        .map_err(StoreError::from)?;
    Ok(EncryptedStoreSecret {
        crypto_version: StoreSecretCryptoVersion::new(crypto_version),
        key_id,
        nonce: nonce.into_boxed_slice(),
        ciphertext: ciphertext.into_boxed_slice(),
    })
}

fn decode_non_zero_member_count(member_count: i64) -> Result<NonZeroUsize, StoreError> {
    let member_count = usize::try_from(member_count).context(MemberCountOverflowSnafu)?;
    NonZeroUsize::new(member_count)
        .context(EmptyGroupMembersSnafu)
        .map_err(StoreError::from)
}

fn ensure_member_index_in_bounds(
    member_index: MemberIndex,
    member_count: NonZeroUsize,
) -> Result<(), StoreError> {
    ensure!(
        (member_index.as_u32() as usize) < member_count.get(),
        InvalidLocalMemberIndexSnafu {
            local_member_index: member_index.as_u32(),
            member_count: member_count.get(),
        }
    );
    Ok(())
}

fn decode_member_index(raw: i64, member_count: NonZeroUsize) -> Result<MemberIndex, StoreError> {
    let member_index = u32::try_from(raw).context(MemberIndexOverflowSnafu)?;
    let member_index = MemberIndex::new(member_index);
    ensure_member_index_in_bounds(member_index, member_count)?;
    Ok(member_index)
}

fn decode_member_index_value(raw: i64) -> Result<u32, StoreError> {
    u32::try_from(raw)
        .context(MemberIndexOverflowSnafu)
        .map_err(StoreError::from)
}

fn decode_group_id(raw: &str) -> Result<GroupId, StoreError> {
    let group_id = Uuid::parse_str(raw).context(InvalidGroupIdSnafu)?;
    Ok(GroupId(group_id))
}

fn decode_row_key(raw: &str) -> Result<RowKey, StoreError> {
    let row_key = Uuid::parse_str(raw).context(InvalidRowKeySnafu)?;
    Ok(RowKey(row_key))
}

fn decode_member_identity(raw: &str) -> Result<MemberIdentity, StoreError> {
    Ok(raw.parse().context(InvalidMemberIdentitySnafu {
        raw: raw.to_owned(),
    })?)
}

fn invalid_stored_object(
    object: &'static str,
    source: impl StdError + Send + Sync + 'static,
) -> StoreError {
    SqliteStoreError::InvalidStoredObject {
        object,
        source: Box::new(source),
    }
    .into()
}

#[derive(Debug, Snafu)]
enum SqliteStoreError {
    #[snafu(display("SQLite operation failed: {source}"))]
    Sqlx { source: sqlx::Error },
    #[snafu(display("SQLite connection URL '{database_url}' was invalid: {source}"))]
    ParseSqliteUrl {
        database_url: String,
        source: sqlx::Error,
    },
    #[snafu(display("Stored group id was not a valid UUID: {source}"))]
    InvalidGroupId { source: uuid::Error },
    #[snafu(display("Stored row key was not a valid UUID: {source}"))]
    InvalidRowKey { source: uuid::Error },
    #[snafu(display("Stored store-secret key id was invalid: {source}"))]
    InvalidStoreSecretKeyId {
        source: flotsync_security::StoreSecretKeyIdParseError,
    },
    #[snafu(display("Stored member identity '{raw}' was invalid: {source}"))]
    InvalidMemberIdentity {
        raw: String,
        source: IdentifierParseError,
    },
    #[snafu(display("Stored group had no members."))]
    EmptyGroupMembers,
    #[snafu(display("Stored member count overflowed the supported range: {source}"))]
    MemberCountOverflow { source: std::num::TryFromIntError },
    #[snafu(display("Stored member index overflowed the supported range: {source}"))]
    MemberIndexOverflow { source: std::num::TryFromIntError },
    #[snafu(display("Stored secret crypto version overflowed the supported range: {source}"))]
    SecretCryptoVersionOverflow { source: std::num::TryFromIntError },
    #[snafu(display("Dataset row scan limit overflowed the supported range: {source}"))]
    RowLimitOverflow { source: std::num::TryFromIntError },
    #[snafu(display(
        "Stored local member index {local_member_index} is out of bounds for {member_count} members."
    ))]
    InvalidLocalMemberIndex {
        local_member_index: u32,
        member_count: usize,
    },
    #[snafu(display(
        "Stored group '{group_id}' expected {expected_member_count} members, but loaded {actual_member_count}."
    ))]
    StoredGroupMemberCountMismatch {
        group_id: GroupId,
        expected_member_count: usize,
        actual_member_count: usize,
    },
    #[snafu(display("Stored schema source for dataset '{dataset_id}' was missing."))]
    MissingSchema { dataset_id: DatasetId },
    #[snafu(display("Stored {object} blob could not be decoded: {source}"))]
    DecodeStoredProto {
        object: &'static str,
        source: flotsync_messages::buffa::DecodeError,
    },
    #[snafu(display("Stored {object} was invalid: {source}"))]
    InvalidStoredObject {
        object: &'static str,
        source: Box<dyn StdError + Send + Sync>,
    },
    #[snafu(display("Stored {object} sort key had invalid length {len}."))]
    InvalidStoredSortKey { object: &'static str, len: usize },
    #[snafu(display(
        "Stored {object} for member '{member_id}' conflicts with requested material."
    ))]
    ConflictingMemberSecurityMaterial {
        object: &'static str,
        member_id: MemberIdentity,
    },
    #[snafu(display(
        "Stored update belonged to group '{actual_group_id}', expected '{expected_group_id}'."
    ))]
    StoredUpdateGroupMismatch {
        expected_group_id: GroupId,
        actual_group_id: GroupId,
    },
    #[snafu(display(
        "Stored update contained update id '{actual_update_id:?}', expected '{expected_update_id:?}'."
    ))]
    StoredUpdateIdMismatch {
        expected_update_id: UpdateId,
        actual_update_id: UpdateId,
    },
    #[snafu(display("Stored group '{group_id}' was missing."))]
    MissingStoredGroup { group_id: GroupId },
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
    #[snafu(display("Stored update '{group_id}/{update_id:?}' was missing."))]
    MissingStoredUpdate {
        group_id: GroupId,
        update_id: UpdateId,
    },
}

impl From<SqliteStoreError> for StoreError {
    fn from(value: SqliteStoreError) -> Self {
        StoreError::StoreExternal {
            source: Box::new(value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{
        DatasetRowPatch,
        DatasetRowWrite,
        ReplicationRowRecord,
        ReplicationUpdateFilter,
        current_slice_placeholder_group_security_material,
    };
    use flotsync_core::member::Identifier;
    use flotsync_data_types::{Field, Schema, TableOperations, schema::datamodel::RowOperation};
    use flotsync_messages::codecs::datamodel::encode_schema_operation;
    use std::{collections::HashSet, time::Duration};

    const STORE_FUTURE_TIMEOUT: Duration = Duration::from_secs(5);

    fn wait_for_store_future<F>(future: F) -> F::Output
    where
        F: std::future::Future,
    {
        flotsync_io::test_support::wait_for_future(
            STORE_FUTURE_TIMEOUT,
            future,
            "timed out waiting for sqlite store future",
        )
    }

    fn docs_dataset_id() -> DatasetId {
        DatasetId::try_new("docs").expect("dataset id should build")
    }

    fn local_member() -> MemberIdentity {
        Identifier::from_array(["app", "alice"])
    }

    fn remote_member() -> MemberIdentity {
        Identifier::from_array(["app", "bob"])
    }

    fn title_schema() -> Arc<Schema> {
        Arc::new(Schema::from_fields([Field::linear_string("title")]))
    }

    fn sample_encrypted_secret(seed: u8) -> EncryptedStoreSecret {
        EncryptedStoreSecret {
            crypto_version: StoreSecretCryptoVersion::new(1),
            key_id: StoreSecretKeyId::from_u128_for_test(u128::from(seed)),
            nonce: Vec::from([seed, seed.wrapping_add(1)]).into_boxed_slice(),
            ciphertext: Box::from([seed, seed.wrapping_add(1), seed.wrapping_add(2)]),
        }
    }

    fn sample_group(group_id: GroupId) -> ReplicationGroupRecord {
        let members = vec![local_member(), remote_member()];
        let mut version_vector = VersionVector::initial(NonZeroUsize::new(2).unwrap());
        version_vector.increment_at(0);
        ReplicationGroupRecord {
            group_id,
            members,
            local_member_index: MemberIndex::new(0),
            version_vector,
            security_material: current_slice_placeholder_group_security_material(group_id),
        }
    }

    fn is_conflicting_member_security_material(
        error: &StoreError,
        object: &'static str,
        member_id: &MemberIdentity,
    ) -> bool {
        match error {
            StoreError::StoreExternal { source } => matches!(
                source.downcast_ref::<SqliteStoreError>(),
                Some(SqliteStoreError::ConflictingMemberSecurityMaterial {
                    object: stored_object,
                    member_id: stored_member_id,
                }) if *stored_object == object && stored_member_id == member_id
            ),
        }
    }

    fn sample_last_changed_versions() -> VersionVector {
        let mut version_vector = VersionVector::initial(NonZeroUsize::new(2).unwrap());
        version_vector.increment_at(0);
        version_vector
    }

    fn insert_row_patch(
        group_id: GroupId,
        dataset_id: &DatasetId,
        row_key: RowKey,
        operation: &flotsync_messages::SchemaOperation<'_>,
    ) -> DatasetRowPatch {
        let RowOperation::Insert { snapshot, .. } = &operation.operation else {
            panic!("expected insert operation");
        };
        DatasetRowPatch {
            group_id,
            dataset_id: dataset_id.clone(),
            actions: vec![DatasetRowWrite::UpsertActive {
                row_key,
                snapshot: snapshot.clone().into_owned(),
            }],
            last_changed_versions: sample_last_changed_versions(),
        }
    }

    fn title_snapshot(
        schema: &Arc<Schema>,
        row_key: RowKey,
        title: &str,
    ) -> ReplicationRowSnapshot {
        let mut source_data = flotsync_messages::InMemoryData::new(schema.clone());
        let operation = source_data
            .insert_row(
                UpdateId {
                    node_index: 0,
                    version: 1,
                },
                row_key.0,
                vec![
                    schema
                        .columns
                        .get("title")
                        .expect("title field should exist")
                        .initial(title)
                        .expect("field value should build"),
                ],
            )
            .expect("row insert should succeed");
        let RowOperation::Insert { snapshot, .. } = operation.operation else {
            panic!("expected insert operation");
        };
        snapshot.into_owned()
    }

    fn encoded_insert_snapshot(
        title: &str,
        schema: &Arc<Schema>,
    ) -> flotsync_messages::datamodel::SchemaOperation {
        let mut source_data = flotsync_messages::InMemoryData::new(schema.clone());
        let operation = source_data
            .insert_row(
                UpdateId {
                    node_index: 0,
                    version: 1,
                },
                Uuid::from_u128(30_001),
                vec![
                    schema
                        .columns
                        .get("title")
                        .expect("title field should exist")
                        .initial(title)
                        .expect("field value should build"),
                ],
            )
            .expect("row insert should succeed");
        encode_schema_operation(&operation, schema.as_ref()).expect("operation should encode")
    }

    #[test]
    fn dropping_open_sqlite_transaction_releases_store() {
        let store = Arc::new(SqliteReplicationStore::in_memory(local_member()).unwrap());
        let transaction =
            wait_for_store_future(store.begin_transaction()).expect("transaction should start");
        drop(transaction);

        let (probe_result_tx, probe_result_rx) = std::sync::mpsc::channel();
        let store = store.clone();
        std::thread::spawn(move || {
            let probe_result =
                wait_for_store_future(store.begin_transaction()).map(|transaction| {
                    wait_for_store_future(transaction.rollback())
                        .expect("probe transaction should roll back");
                });
            let _ = probe_result_tx.send(probe_result);
        });

        let probe_result = probe_result_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("dropping an open transaction should release the SQLite store promptly");
        probe_result.expect("dropped transaction should leave the SQLite store usable");
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "This store roundtrip test keeps related group, dataset, and update assertions in one fixture."
    )]
    fn sqlite_store_roundtrips_group_dataset_and_update_records() {
        let dataset_id = docs_dataset_id();
        let schema = title_schema();
        let store = SqliteReplicationStore::in_memory_with_schema_sources(
            local_member(),
            [(dataset_id.clone(), schema.clone())],
        )
        .expect("store should build");
        let group_id = GroupId(Uuid::from_u128(101));
        let row_key = RowKey(Uuid::from_u128(202));
        let group = sample_group(group_id);
        let mut updated_version_vector = group.version_vector.clone();
        updated_version_vector.increment_at(1);
        let mut source_data = flotsync_messages::InMemoryData::new(schema.clone());
        let operation = source_data
            .insert_row(
                UpdateId {
                    node_index: 0,
                    version: 1,
                },
                row_key.0,
                vec![
                    schema
                        .columns
                        .get("title")
                        .expect("title field should exist")
                        .initial("hello")
                        .expect("field value should build"),
                ],
            )
            .expect("row insert should succeed");
        let encoded_operation =
            encode_schema_operation(&operation, schema.as_ref()).expect("operation should encode");
        let row_patch = insert_row_patch(group_id, &dataset_id, row_key, &operation);
        let expected_row = match &row_patch.actions[0] {
            DatasetRowWrite::UpsertActive { row_key, snapshot } => ReplicationRowRecord {
                row_id: *row_key,
                snapshot: snapshot.clone(),
                tombstoned: false,
                last_changed_versions: row_patch.last_changed_versions.clone(),
            },
            DatasetRowWrite::UpsertTombstone { .. } => panic!("expected active row patch"),
        };
        let update = ReplicationUpdateRecord {
            group_id,
            update_id: UpdateId {
                node_index: 0,
                version: 1,
            },
            sender: local_member(),
            read_versions: VersionVector::initial(NonZeroUsize::new(2).unwrap()),
            dataset_updates: vec![DatasetUpdateRecord {
                dataset_id: dataset_id.clone(),
                operations: vec![encoded_operation.clone()],
            }],
            applied_locally: false,
        };

        let mut transaction =
            wait_for_store_future(store.begin_transaction()).expect("transaction should start");
        wait_for_store_future(transaction.insert_replication_group(group.clone()))
            .expect("group should store");
        wait_for_store_future(
            transaction
                .update_replication_group_version_vector(&group_id, updated_version_vector.clone()),
        )
        .expect("group version vector should update");
        wait_for_store_future(transaction.apply_dataset_row_patch(row_patch))
            .expect("row patch should store");
        wait_for_store_future(transaction.append_replication_update(update.clone()))
            .expect("update should store");
        wait_for_store_future(transaction.commit()).expect("commit should succeed");

        let mut transaction =
            wait_for_store_future(store.begin_transaction()).expect("transaction should start");
        let loaded_group = wait_for_store_future(transaction.load_replication_group(&group_id))
            .expect("group should load")
            .expect("group should exist");
        assert_eq!(loaded_group.group_id, group.group_id);
        assert_eq!(loaded_group.members, group.members);
        assert_eq!(loaded_group.security_material, group.security_material);
        assert_eq!(
            loaded_group.version_vector.iter().collect::<Vec<_>>(),
            updated_version_vector.iter().collect::<Vec<_>>()
        );

        let missing_row_key = RowKey(Uuid::from_u128(203));
        let requested_row_keys = [row_key, missing_row_key];
        let mut requested_row_keys = requested_row_keys.iter();
        let loaded_snapshot = wait_for_store_future(transaction.load_dataset_rows(
            &group_id,
            &dataset_id,
            &mut requested_row_keys,
        ))
        .expect("row slice should load");
        assert!(loaded_snapshot.dataset_exists);
        assert_eq!(loaded_snapshot.rows.len(), 2);
        assert_eq!(
            loaded_snapshot.rows.get(&row_key).cloned().flatten(),
            Some(expected_row)
        );
        assert_eq!(loaded_snapshot.rows.get(&missing_row_key), Some(&None));

        let loaded_update =
            wait_for_store_future(transaction.load_replication_update(&group_id, update.update_id))
                .expect("update should load")
                .expect("update should exist");
        assert_eq!(loaded_update, update);
        assert!(matches!(
            wait_for_store_future(
                transaction.load_replication_updates(
                    &group_id,
                    ReplicationUpdateFilter::PendingApply,
                    None,
                )
            )
            .expect("updates should load")
            .as_slice(),
            [only] if only == &update
        ));
    }

    #[test]
    fn sqlite_store_loads_replication_groups_by_requested_ids() {
        let store = SqliteReplicationStore::in_memory(local_member()).unwrap();
        let first_group_id = GroupId(Uuid::from_u128(10_001));
        let second_group_id = GroupId(Uuid::from_u128(10_002));
        let missing_group_id = GroupId(Uuid::from_u128(10_003));
        let first_group = sample_group(first_group_id);
        let second_group = sample_group(second_group_id);

        let mut transaction =
            wait_for_store_future(store.begin_transaction()).expect("transaction should start");
        wait_for_store_future(transaction.insert_replication_group(first_group))
            .expect("first group should store");
        wait_for_store_future(transaction.insert_replication_group(second_group))
            .expect("second group should store");
        wait_for_store_future(transaction.commit()).expect("commit should succeed");

        let requested_group_ids = HashSet::from([first_group_id, missing_group_id]);
        let mut write_transaction =
            wait_for_store_future(store.begin_transaction()).expect("transaction should start");
        let loaded_groups = wait_for_store_future(
            write_transaction.load_replication_groups_for_ids(&requested_group_ids),
        )
        .expect("requested groups should load through write transaction");
        assert_eq!(
            loaded_groups
                .iter()
                .map(|group| group.group_id)
                .collect::<Vec<_>>(),
            vec![first_group_id]
        );
        wait_for_store_future(write_transaction.rollback()).expect("rollback should succeed");

        let mut read_transaction = wait_for_store_future(store.begin_read_transaction())
            .expect("transaction should start");
        let loaded_groups = wait_for_store_future(
            read_transaction.load_replication_groups_for_ids(&requested_group_ids),
        )
        .expect("requested groups should load through read transaction");
        assert_eq!(
            loaded_groups
                .iter()
                .map(|group| group.group_id)
                .collect::<Vec<_>>(),
            vec![first_group_id]
        );
        wait_for_store_future(read_transaction.release()).expect("release should succeed");
    }

    #[test]
    fn sqlite_store_roundtrips_local_member_private_keys() {
        let store = SqliteReplicationStore::in_memory(local_member()).unwrap();
        let record = LocalMemberPrivateKeysRecord {
            member_id: local_member(),
            private_keys: EncryptedLocalMemberPrivateKeys {
                secret: sample_encrypted_secret(42),
            },
        };

        let mut transaction =
            wait_for_store_future(store.begin_transaction()).expect("transaction should start");
        wait_for_store_future(transaction.ensure_local_member_private_keys(record.clone()))
            .expect("private keys should store");
        wait_for_store_future(transaction.ensure_local_member_private_keys(record.clone()))
            .expect("same private keys should be accepted");
        let loaded =
            wait_for_store_future(transaction.load_local_member_private_keys(&record.member_id))
                .expect("private keys should load")
                .expect("private keys should exist");
        assert_eq!(loaded, record);
        wait_for_store_future(transaction.commit()).expect("commit should succeed");

        let mut read_transaction = wait_for_store_future(store.begin_read_transaction())
            .expect("transaction should start");
        let loaded = wait_for_store_future(
            read_transaction.load_local_member_private_keys(&record.member_id),
        )
        .expect("private keys should load through read transaction")
        .expect("private keys should exist");
        assert_eq!(loaded, record);
        wait_for_store_future(read_transaction.release()).expect("release should succeed");
    }

    #[test]
    fn sqlite_store_rejects_conflicting_local_member_private_keys() {
        let store = SqliteReplicationStore::in_memory(local_member()).unwrap();
        let record = LocalMemberPrivateKeysRecord {
            member_id: local_member(),
            private_keys: EncryptedLocalMemberPrivateKeys {
                secret: sample_encrypted_secret(42),
            },
        };
        let conflicting_record = LocalMemberPrivateKeysRecord {
            member_id: record.member_id.clone(),
            private_keys: EncryptedLocalMemberPrivateKeys {
                secret: sample_encrypted_secret(43),
            },
        };

        let mut transaction =
            wait_for_store_future(store.begin_transaction()).expect("transaction should start");
        wait_for_store_future(transaction.ensure_local_member_private_keys(record.clone()))
            .expect("private keys should store");
        let error =
            wait_for_store_future(transaction.ensure_local_member_private_keys(conflicting_record))
                .expect_err("conflicting private keys should fail");
        assert!(is_conflicting_member_security_material(
            &error,
            "local member private keys",
            &record.member_id,
        ));
        wait_for_store_future(transaction.rollback()).expect("rollback should succeed");
    }

    #[test]
    fn sqlite_store_roundtrips_trusted_member_public_keys() {
        let store = SqliteReplicationStore::in_memory(local_member()).unwrap();
        let record = TrustedMemberPublicKeysRecord {
            member_id: remote_member(),
            signing_public_key: Vec::from([1_u8, 2, 3, 4]).into_boxed_slice(),
            encryption_public_key: Vec::from([5_u8, 6, 7, 8, 9]).into_boxed_slice(),
        };

        let mut transaction =
            wait_for_store_future(store.begin_transaction()).expect("transaction should start");
        wait_for_store_future(transaction.ensure_trusted_member_public_keys(record.clone()))
            .expect("trusted public keys should store");
        wait_for_store_future(transaction.ensure_trusted_member_public_keys(record.clone()))
            .expect("same trusted public keys should be accepted");
        let loaded =
            wait_for_store_future(transaction.load_trusted_member_public_keys(&record.member_id))
                .expect("trusted public keys should load")
                .expect("trusted public keys should exist");
        assert_eq!(loaded, record);
        wait_for_store_future(transaction.commit()).expect("commit should succeed");

        let mut read_transaction = wait_for_store_future(store.begin_read_transaction())
            .expect("transaction should start");
        let loaded = wait_for_store_future(
            read_transaction.load_trusted_member_public_keys(&record.member_id),
        )
        .expect("trusted public keys should load through read transaction")
        .expect("trusted public keys should exist");
        assert_eq!(loaded, record);
        wait_for_store_future(read_transaction.release()).expect("release should succeed");
    }

    #[test]
    fn sqlite_store_rejects_conflicting_trusted_member_public_keys() {
        let store = SqliteReplicationStore::in_memory(local_member()).unwrap();
        let record = TrustedMemberPublicKeysRecord {
            member_id: remote_member(),
            signing_public_key: Vec::from([1_u8, 2, 3, 4]).into_boxed_slice(),
            encryption_public_key: Vec::from([5_u8, 6, 7, 8, 9]).into_boxed_slice(),
        };
        let conflicting_record = TrustedMemberPublicKeysRecord {
            member_id: record.member_id.clone(),
            signing_public_key: Vec::from([9_u8, 8, 7, 6]).into_boxed_slice(),
            encryption_public_key: Vec::from([5_u8, 6, 7, 8, 9]).into_boxed_slice(),
        };

        let mut transaction =
            wait_for_store_future(store.begin_transaction()).expect("transaction should start");
        wait_for_store_future(transaction.ensure_trusted_member_public_keys(record.clone()))
            .expect("trusted public keys should store");
        let error = wait_for_store_future(
            transaction.ensure_trusted_member_public_keys(conflicting_record),
        )
        .expect_err("conflicting trusted public keys should fail");
        assert!(is_conflicting_member_security_material(
            &error,
            "trusted member public keys",
            &record.member_id,
        ));
        wait_for_store_future(transaction.rollback()).expect("rollback should succeed");
    }

    #[test]
    fn sqlite_store_filters_replication_updates_by_producer_range() {
        let dataset_id = docs_dataset_id();
        let schema = title_schema();
        let store = SqliteReplicationStore::in_memory_with_schema_sources(
            local_member(),
            [(dataset_id.clone(), schema.clone())],
        )
        .expect("store should build");
        let group_id = GroupId(Uuid::from_u128(10_011));
        let group = sample_group(group_id);
        let encoded_operation = encoded_insert_snapshot("range query", &schema);
        let update = |node_index, version, sender| ReplicationUpdateRecord {
            group_id,
            update_id: UpdateId {
                node_index,
                version,
            },
            sender,
            read_versions: VersionVector::initial(NonZeroUsize::new(2).unwrap()),
            dataset_updates: vec![DatasetUpdateRecord {
                dataset_id: dataset_id.clone(),
                operations: vec![encoded_operation.clone()],
            }],
            applied_locally: true,
        };
        let alice_v1 = update(0, 1, local_member());
        let alice_v2 = update(0, 2, local_member());
        let alice_v3 = update(0, 3, local_member());
        let bob_v1 = update(1, 1, remote_member());

        let mut transaction =
            wait_for_store_future(store.begin_transaction()).expect("transaction should start");
        wait_for_store_future(transaction.insert_replication_group(group))
            .expect("group should store");
        for update in [
            alice_v1.clone(),
            alice_v2.clone(),
            alice_v3.clone(),
            bob_v1.clone(),
        ] {
            wait_for_store_future(transaction.append_replication_update(update))
                .expect("update should store");
        }

        let limited_alice = wait_for_store_future(transaction.load_replication_updates(
            &group_id,
            ReplicationUpdateFilter::ProducerRange {
                producer_index: MemberIndex::new(0),
                start_version: 2,
                end_version: 3,
            },
            NonZeroUsize::new(1),
        ))
        .expect("range should load");
        assert_eq!(limited_alice, vec![alice_v2.clone()]);

        let limited_alice_ids = wait_for_store_future(transaction.load_replication_update_ids(
            &group_id,
            ReplicationUpdateFilter::ProducerRange {
                producer_index: MemberIndex::new(0),
                start_version: 2,
                end_version: 3,
            },
            NonZeroUsize::new(1),
        ))
        .expect("range ids should load");
        assert_eq!(limited_alice_ids, vec![alice_v2.update_id]);

        let full_alice = wait_for_store_future(transaction.load_replication_updates(
            &group_id,
            ReplicationUpdateFilter::ProducerRange {
                producer_index: MemberIndex::new(0),
                start_version: 2,
                end_version: 3,
            },
            NonZeroUsize::new(4),
        ))
        .expect("range should load");
        assert_eq!(full_alice, vec![alice_v2, alice_v3]);

        let bob = wait_for_store_future(transaction.load_replication_updates(
            &group_id,
            ReplicationUpdateFilter::ProducerRange {
                producer_index: MemberIndex::new(1),
                start_version: 1,
                end_version: 3,
            },
            NonZeroUsize::new(4),
        ))
        .expect("range should load");
        assert_eq!(bob, vec![bob_v1]);
        wait_for_store_future(transaction.rollback()).expect("rollback should succeed");
    }

    #[test]
    fn sqlite_store_roundtrips_tombstoned_dataset_rows() {
        let dataset_id = docs_dataset_id();
        let schema = title_schema();
        let store = SqliteReplicationStore::in_memory_with_schema_sources(
            local_member(),
            [(dataset_id.clone(), schema.clone())],
        )
        .expect("store should build");
        let group_id = GroupId(Uuid::from_u128(104));
        let row_key = RowKey(Uuid::from_u128(204));
        let mut source_data = flotsync_messages::InMemoryData::new(schema.clone());
        let operation = source_data
            .insert_row(
                UpdateId {
                    node_index: 0,
                    version: 1,
                },
                row_key.0,
                vec![
                    schema
                        .columns
                        .get("title")
                        .expect("title field should exist")
                        .initial("deleted")
                        .expect("field value should build"),
                ],
            )
            .expect("row insert should succeed");
        let RowOperation::Insert { snapshot, .. } = &operation.operation else {
            panic!("expected insert operation");
        };
        let stored_row = ReplicationRowRecord {
            row_id: row_key,
            snapshot: snapshot.clone().into_owned(),
            tombstoned: true,
            last_changed_versions: sample_last_changed_versions(),
        };

        let mut transaction =
            wait_for_store_future(store.begin_transaction()).expect("transaction should start");
        wait_for_store_future(transaction.insert_replication_group(sample_group(group_id)))
            .expect("group should store");
        wait_for_store_future(transaction.apply_dataset_row_patch(DatasetRowPatch {
            group_id,
            dataset_id: dataset_id.clone(),
            actions: vec![DatasetRowWrite::UpsertTombstone {
                row_key,
                snapshot: stored_row.snapshot.clone(),
            }],
            last_changed_versions: stored_row.last_changed_versions.clone(),
        }))
        .expect("row patch should store");
        wait_for_store_future(transaction.commit()).expect("commit should succeed");

        let mut transaction =
            wait_for_store_future(store.begin_transaction()).expect("transaction should start");
        let requested_row_keys = [row_key];
        let mut requested_row_keys = requested_row_keys.iter();
        let loaded_rows = wait_for_store_future(transaction.load_dataset_rows(
            &group_id,
            &dataset_id,
            &mut requested_row_keys,
        ))
        .expect("row slice should load");
        wait_for_store_future(transaction.commit()).expect("commit should succeed");

        assert_eq!(
            loaded_rows.rows.get(&row_key).cloned().flatten(),
            Some(stored_row)
        );
    }

    #[test]
    fn sqlite_store_scans_dataset_rows_in_key_order() {
        let dataset_id = docs_dataset_id();
        let schema = title_schema();
        let store = SqliteReplicationStore::in_memory_with_schema_sources(
            local_member(),
            [(dataset_id.clone(), schema.clone())],
        )
        .expect("store should build");
        let group_id = GroupId(Uuid::from_u128(106));
        let first_row_key = RowKey(Uuid::from_u128(206));
        let second_row_key = RowKey(Uuid::from_u128(207));

        let mut transaction =
            wait_for_store_future(store.begin_transaction()).expect("transaction should start");
        wait_for_store_future(transaction.insert_replication_group(sample_group(group_id)))
            .expect("group should store");
        wait_for_store_future(transaction.apply_dataset_row_patch(DatasetRowPatch {
            group_id,
            dataset_id: dataset_id.clone(),
            actions: vec![
                DatasetRowWrite::UpsertActive {
                    row_key: second_row_key,
                    snapshot: title_snapshot(&schema, second_row_key, "second"),
                },
                DatasetRowWrite::UpsertActive {
                    row_key: first_row_key,
                    snapshot: title_snapshot(&schema, first_row_key, "first"),
                },
            ],
            last_changed_versions: sample_last_changed_versions(),
        }))
        .expect("rows should store");
        wait_for_store_future(transaction.commit()).expect("transaction should commit");

        let mut transaction =
            wait_for_store_future(store.begin_read_transaction()).expect("read should start");
        let first_batch = wait_for_store_future(transaction.scan_dataset_row_batch(
            &group_id,
            &dataset_id,
            None,
            NonZeroUsize::new(1).expect("limit should be non-zero"),
        ))
        .expect("first batch should scan");
        let second_batch = wait_for_store_future(transaction.scan_dataset_row_batch(
            &group_id,
            &dataset_id,
            first_batch.next_after,
            NonZeroUsize::new(1).expect("limit should be non-zero"),
        ))
        .expect("second batch should scan");
        wait_for_store_future(transaction.release()).expect("read should release");

        assert_eq!(first_batch.rows[0].row_id, first_row_key);
        assert_eq!(first_batch.next_after, Some(first_row_key));
        assert_eq!(second_batch.rows[0].row_id, second_row_key);
        assert_eq!(second_batch.next_after, Some(second_row_key));
    }

    #[test]
    fn sqlite_store_rejects_tombstone_to_active_row_transition() {
        let dataset_id = docs_dataset_id();
        let schema = title_schema();
        let store = SqliteReplicationStore::in_memory_with_schema_sources(
            local_member(),
            [(dataset_id.clone(), schema.clone())],
        )
        .expect("store should build");
        let group_id = GroupId(Uuid::from_u128(105));
        let row_key = RowKey(Uuid::from_u128(205));
        let tombstone_snapshot = title_snapshot(&schema, row_key, "deleted");
        let active_snapshot = title_snapshot(&schema, row_key, "resurrected");

        let mut transaction =
            wait_for_store_future(store.begin_transaction()).expect("transaction should start");
        wait_for_store_future(transaction.insert_replication_group(sample_group(group_id)))
            .expect("group should store");
        wait_for_store_future(transaction.apply_dataset_row_patch(DatasetRowPatch {
            group_id,
            dataset_id: dataset_id.clone(),
            actions: vec![DatasetRowWrite::UpsertTombstone {
                row_key,
                snapshot: tombstone_snapshot,
            }],
            last_changed_versions: sample_last_changed_versions(),
        }))
        .expect("missing-to-tombstone upsert should store");

        let error = wait_for_store_future(transaction.apply_dataset_row_patch(DatasetRowPatch {
            group_id,
            dataset_id,
            actions: vec![DatasetRowWrite::UpsertActive {
                row_key,
                snapshot: active_snapshot,
            }],
            last_changed_versions: sample_last_changed_versions(),
        }))
        .expect_err("tombstone-to-active upsert should fail");
        wait_for_store_future(transaction.rollback()).expect("transaction should roll back");

        assert!(matches!(
            error,
            StoreError::StoreExternal { ref source }
                if source.to_string().contains("cannot transition from tombstone to active")
        ));
    }

    #[test]
    fn sqlite_store_rejects_duplicate_group_insert() {
        let dataset_id = docs_dataset_id();
        let schema = title_schema();
        let store = SqliteReplicationStore::in_memory_with_schema_sources(
            local_member(),
            [(dataset_id, schema)],
        )
        .expect("store should build");
        let group_id = GroupId(Uuid::from_u128(303));
        let group = sample_group(group_id);

        let mut transaction =
            wait_for_store_future(store.begin_transaction()).expect("transaction should start");
        wait_for_store_future(transaction.insert_replication_group(group.clone()))
            .expect("group should store");
        wait_for_store_future(transaction.commit()).expect("commit should succeed");

        let mut transaction =
            wait_for_store_future(store.begin_transaction()).expect("transaction should start");
        let error = wait_for_store_future(transaction.insert_replication_group(group))
            .expect_err("duplicate group insert should fail");
        assert!(matches!(error, StoreError::StoreExternal { .. }));
    }

    #[test]
    fn sqlite_store_rejects_duplicate_update_insert_but_allows_applied_toggle() {
        let dataset_id = docs_dataset_id();
        let schema = title_schema();
        let store = SqliteReplicationStore::in_memory_with_schema_sources(
            local_member(),
            [(dataset_id.clone(), schema.clone())],
        )
        .expect("store should build");
        let group_id = GroupId(Uuid::from_u128(404));
        let group = sample_group(group_id);
        let mut source_data = flotsync_messages::InMemoryData::new(schema.clone());
        let operation = source_data
            .insert_row(
                UpdateId {
                    node_index: 0,
                    version: 1,
                },
                Uuid::from_u128(505),
                vec![
                    schema
                        .columns
                        .get("title")
                        .expect("title field should exist")
                        .initial("goodbye")
                        .expect("field value should build"),
                ],
            )
            .expect("row insert should succeed");
        let encoded_operation =
            encode_schema_operation(&operation, schema.as_ref()).expect("operation should encode");
        let update = ReplicationUpdateRecord {
            group_id,
            update_id: UpdateId {
                node_index: 0,
                version: u64::MAX - 1,
            },
            sender: local_member(),
            read_versions: VersionVector::initial(NonZeroUsize::new(2).unwrap()),
            dataset_updates: vec![DatasetUpdateRecord {
                dataset_id,
                operations: vec![encoded_operation],
            }],
            applied_locally: false,
        };

        let mut transaction =
            wait_for_store_future(store.begin_transaction()).expect("transaction should start");
        wait_for_store_future(transaction.insert_replication_group(group))
            .expect("group should store");
        wait_for_store_future(transaction.append_replication_update(update.clone()))
            .expect("update should store");
        let duplicate_error =
            wait_for_store_future(transaction.append_replication_update(update.clone()))
                .expect_err("duplicate update insert should fail");
        assert!(matches!(duplicate_error, StoreError::StoreExternal { .. }));
        wait_for_store_future(
            transaction.mark_replication_update_applied(&group_id, update.update_id),
        )
        .expect("applied toggle should succeed");
        wait_for_store_future(transaction.commit()).expect("commit should succeed");

        let mut transaction =
            wait_for_store_future(store.begin_transaction()).expect("transaction should start");
        let loaded_update =
            wait_for_store_future(transaction.load_replication_update(&group_id, update.update_id))
                .expect("update should load")
                .expect("update should exist");
        assert!(loaded_update.applied_locally);
        assert_eq!(loaded_update.update_id.version, u64::MAX - 1);
    }
}
