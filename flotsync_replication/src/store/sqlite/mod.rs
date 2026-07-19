use crate::{
    api::{
        DatasetId,
        DatasetRowStateBatch,
        DatasetRowStatePatch,
        DatasetRowStateSlice,
        DatasetRowStateWrite,
        DatasetSchema,
        DatasetUpdateRecord,
        EncryptedGroupSecurityMaterial,
        EncryptedLocalMemberPrivateKeys,
        EncryptedStoreSecret,
        GroupMemberKeys,
        GroupSchema,
        LocalMemberPrivateKeysRecord,
        MemberKeyId,
        MemberKeyTrustEvidenceKind,
        MemberKeyTrustEvidenceRecord,
        MemberKeyTrustEvidenceSet,
        MemberPublicKeysRecord,
        PendingGroupActivationRecord,
        PendingGroupDecisionRecord,
        PendingGroupWorkKey,
        ReplicationGroupLifecycle,
        ReplicationGroupMaterialRecord,
        ReplicationGroupRecord,
        ReplicationRowStateRecord,
        ReplicationRowStateSnapshot,
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
    },
    codecs::{
        messages::{
            MemberCountContext,
            RuntimeVersionVectorProtoSource,
            UpdateMessage,
            UpdateMessageProtoSource,
            WireVersionVector,
        },
        pending_group::{
            PendingGroupPayloadDecodeContext,
            PendingGroupPayloadKind,
            decode_pending_group_activation_payload,
            decode_pending_group_decision_payload,
            encode_pending_group_activation_payload,
            encode_pending_group_decision_payload,
        },
    },
};
use flotsync_core::{
    GroupId,
    MemberIdentity,
    MemberIndex,
    member::IdentifierParseError,
    versions::{UpdateId, VersionVector},
};
use flotsync_messages::{
    buffa::Message as _,
    codecs::datamodel::{decode_row_snapshot, encode_row_snapshot},
    datamodel as datamodel_proto,
    proto::{DecodeProto, DecodeProtoWith, EncodeProto, ProtoInputDecodeError},
};
use flotsync_security::{KeyFingerprint, PublicMemberKeys};
use flotsync_utils::BoxFuture;
use futures_util::{FutureExt, future};
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
    pub async fn in_memory(local_member: MemberIdentity) -> Result<Self, StoreError> {
        Self::in_memory_with_schema_sources(
            local_member,
            std::iter::empty::<(DatasetId, SchemaSource)>(),
        )
        .await
    }

    /// Open one disk-backed `SQLite` store for `local_member`.
    ///
    /// # Errors
    ///
    /// See `StoreError` for failure conditions.
    pub async fn file(
        local_member: MemberIdentity,
        path: impl AsRef<Path>,
    ) -> Result<Self, StoreError> {
        Self::file_with_schema_sources(
            local_member,
            path,
            std::iter::empty::<(DatasetId, SchemaSource)>(),
        )
        .await
    }

    /// Create one in-memory store with the provided application schema sources.
    ///
    /// # Errors
    ///
    /// See `StoreError` for failure conditions.
    pub async fn in_memory_with_schema_sources<I, S>(
        local_member: MemberIdentity,
        schema_sources: I,
    ) -> Result<Self, StoreError>
    where
        I: IntoIterator<Item = (DatasetId, S)>,
        S: Into<SchemaSource>,
    {
        let schema_sources = collect_schema_sources(schema_sources);
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
        Self::from_connect_options(local_member, schema_sources, connect_options).await
    }

    /// Open one disk-backed `SQLite` store with the provided application schema sources.
    ///
    /// # Errors
    ///
    /// See `StoreError` for failure conditions.
    pub async fn file_with_schema_sources<I, S>(
        local_member: MemberIdentity,
        path: impl AsRef<Path>,
        schema_sources: I,
    ) -> Result<Self, StoreError>
    where
        I: IntoIterator<Item = (DatasetId, S)>,
        S: Into<SchemaSource>,
    {
        let schema_sources = collect_schema_sources(schema_sources);
        let connect_options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true)
            .statement_cache_capacity(STATEMENT_CACHE_CAPACITY);
        Self::from_connect_options(local_member, schema_sources, connect_options).await
    }

    async fn from_connect_options(
        local_member: MemberIdentity,
        schema_sources: HashMap<DatasetId, SchemaSource>,
        connect_options: SqliteConnectOptions,
    ) -> Result<Self, StoreError> {
        let pool = SqlitePoolOptions::new()
            .min_connections(1)
            .max_connections(8)
            .acquire_timeout(POOL_ACQUIRE_TIMEOUT)
            .idle_timeout(None)
            .max_lifetime(None)
            .connect_with(connect_options)
            .await
            .context(SqlxSnafu)?;
        let mut connection = pool.acquire().await.context(SqlxSnafu)?;
        initialise_schema(&mut connection).await?;
        drop(connection);

        Ok(Self {
            local_member,
            schema_sources: Arc::new(schema_sources),
            pool: Arc::new(pool),
        })
    }
}

fn collect_schema_sources<I, S>(schema_sources: I) -> HashMap<DatasetId, SchemaSource>
where
    I: IntoIterator<Item = (DatasetId, S)>,
    S: Into<SchemaSource>,
{
    schema_sources
        .into_iter()
        .map(|(dataset_id, schema)| (dataset_id, schema.into()))
        .collect()
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

    fn load_group_dataset_schema<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        dataset_id: &'a DatasetId,
    ) -> BoxFuture<'a, Result<Option<SchemaSource>, StoreError>> {
        async move {
            load_group_dataset_schema(self.assert_open_connection(), group_id, dataset_id).await
        }
        .boxed()
    }

    fn load_local_member_private_keys<'a>(
        &'a mut self,
        member_id: &'a MemberIdentity,
    ) -> BoxFuture<'a, Result<Option<LocalMemberPrivateKeysRecord>, StoreError>> {
        async move { load_local_member_private_keys(self.assert_open_connection(), member_id).await }
            .boxed()
    }

    fn load_member_public_keys<'a>(
        &'a mut self,
        key_id: &'a MemberKeyId,
    ) -> BoxFuture<'a, Result<Option<MemberPublicKeysRecord>, StoreError>> {
        async move { load_member_public_keys(self.assert_open_connection(), key_id).await }.boxed()
    }

    fn load_member_public_keys_for_member<'a>(
        &'a mut self,
        member_id: &'a MemberIdentity,
    ) -> BoxFuture<'a, Result<Vec<MemberPublicKeysRecord>, StoreError>> {
        async move { load_member_public_keys_for_member(self.assert_open_connection(), member_id).await }
            .boxed()
    }

    fn load_member_public_keys_for_fingerprint<'a>(
        &'a mut self,
        fingerprint: &'a KeyFingerprint,
    ) -> BoxFuture<'a, Result<Vec<MemberPublicKeysRecord>, StoreError>> {
        async move {
            load_member_public_keys_for_fingerprint(self.assert_open_connection(), fingerprint)
                .await
        }
        .boxed()
    }

    fn load_member_key_trust_evidence<'a>(
        &'a mut self,
        key_id: &'a MemberKeyId,
    ) -> BoxFuture<'a, Result<MemberKeyTrustEvidenceSet, StoreError>> {
        async move { load_member_key_trust_evidence(self.assert_open_connection(), key_id).await }
            .boxed()
    }

    fn is_key_fingerprint_blocked<'a>(
        &'a mut self,
        fingerprint: &'a KeyFingerprint,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        async move { is_key_fingerprint_blocked(self.assert_open_connection(), fingerprint).await }
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

    fn load_dataset_rows<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        dataset_id: &'a DatasetId,
        row_keys: &'a mut RowKeyIterator<'a>,
    ) -> BoxFuture<'a, Result<DatasetRowStateSlice, StoreError>> {
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

    fn scan_dataset_row_batch<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        dataset_id: &'a DatasetId,
        after: Option<RowKey>,
        limit: NonZeroUsize,
    ) -> BoxFuture<'a, Result<DatasetRowStateBatch, StoreError>> {
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

    fn load_pending_group_decisions(
        &mut self,
    ) -> BoxFuture<'_, Result<Vec<PendingGroupDecisionRecord>, StoreError>> {
        async move { load_pending_group_decisions(self.assert_open_connection()).await }.boxed()
    }

    fn load_pending_group_decision<'a>(
        &'a mut self,
        group_id: &'a GroupId,
    ) -> BoxFuture<'a, Result<Option<PendingGroupDecisionRecord>, StoreError>> {
        async move { load_pending_group_decision(self.assert_open_connection(), group_id).await }
            .boxed()
    }

    fn load_pending_group_activations(
        &mut self,
    ) -> BoxFuture<'_, Result<Vec<PendingGroupActivationRecord>, StoreError>> {
        async move { load_pending_group_activations(self.assert_open_connection()).await }.boxed()
    }

    fn load_pending_group_activation<'a>(
        &'a mut self,
        group_id: &'a GroupId,
    ) -> BoxFuture<'a, Result<Option<PendingGroupActivationRecord>, StoreError>> {
        async move { load_pending_group_activation(self.assert_open_connection(), group_id).await }
            .boxed()
    }

    fn load_replication_group_material<'a>(
        &'a mut self,
        group_id: &'a GroupId,
    ) -> BoxFuture<'a, Result<Option<ReplicationGroupMaterialRecord>, StoreError>> {
        async move {
            load_replication_group_material(self.assert_open_connection(), group_id).await
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
    fn insert_replication_group(
        &mut self,
        group: ReplicationGroupRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        async move { insert_replication_group(self.assert_open_connection(), &group).await }.boxed()
    }

    fn ensure_replication_group_material(
        &mut self,
        material: ReplicationGroupMaterialRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        async move {
            ensure_replication_group_material(self.assert_open_connection(), &material).await
        }
        .boxed()
    }

    fn activate_replication_group(
        &mut self,
        group_id: GroupId,
        version_vector: VersionVector,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        async move {
            activate_replication_group(self.assert_open_connection(), group_id, &version_vector)
                .await
        }
        .boxed()
    }

    fn ensure_local_member_private_keys(
        &mut self,
        record: LocalMemberPrivateKeysRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        async move { ensure_local_member_private_keys(self.assert_open_connection(), &record).await }
            .boxed()
    }

    fn ensure_member_public_keys(
        &mut self,
        record: MemberPublicKeysRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        async move { ensure_member_public_keys(self.assert_open_connection(), &record).await }
            .boxed()
    }

    fn ensure_member_key_trust_evidence(
        &mut self,
        record: MemberKeyTrustEvidenceRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        async move { ensure_member_key_trust_evidence(self.assert_open_connection(), &record).await }
            .boxed()
    }

    fn ensure_blocked_key_fingerprint(
        &mut self,
        fingerprint: KeyFingerprint,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        async move { ensure_blocked_key_fingerprint(self.assert_open_connection(), &fingerprint).await }
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

    fn update_replication_group_lifecycle<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        lifecycle: ReplicationGroupLifecycle,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        async move {
            update_replication_group_lifecycle(self.assert_open_connection(), group_id, &lifecycle)
                .await
        }
        .boxed()
    }

    fn apply_dataset_row_patch(
        &mut self,
        patch: DatasetRowStatePatch,
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

    fn upsert_pending_group_decision(
        &mut self,
        record: PendingGroupDecisionRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        async move { upsert_pending_group_decision(self.assert_open_connection(), &record).await }
            .boxed()
    }

    fn remove_pending_group_decision(
        &mut self,
        key: PendingGroupWorkKey,
    ) -> BoxFuture<'_, Result<bool, StoreError>> {
        async move { remove_pending_group_decision(self.assert_open_connection(), key).await }
            .boxed()
    }

    fn upsert_pending_group_activation(
        &mut self,
        record: PendingGroupActivationRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        async move { upsert_pending_group_activation(self.assert_open_connection(), &record).await }
            .boxed()
    }

    fn remove_pending_group_activation(
        &mut self,
        key: PendingGroupWorkKey,
    ) -> BoxFuture<'_, Result<bool, StoreError>> {
        async move { remove_pending_group_activation(self.assert_open_connection(), key).await }
            .boxed()
    }

    fn remove_inactive_replication_group_material(
        &mut self,
        group_id: GroupId,
    ) -> BoxFuture<'_, Result<bool, StoreError>> {
        async move {
            remove_inactive_replication_group_material(self.assert_open_connection(), group_id)
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

const GROUP_LIFECYCLE_OPEN_SQL: &str = "open";
const GROUP_LIFECYCLE_READ_ONLY_SQL: &str = "read_only";
const GROUP_LIFECYCLE_CLOSED_SQL: &str = "closed";

// SQLite schema strings cannot interpolate the constants above. Keep this
// adjacent CHECK constraint aligned when lifecycle labels change.
const REPLICATION_GROUPS_SCHEMA_STATEMENT: &str = concat!(
    "\nCREATE TABLE IF NOT EXISTS replication_groups (\n",
    "    group_id TEXT PRIMARY KEY NOT NULL,\n",
    "    version_vector BLOB NOT NULL,\n",
    "    lifecycle TEXT NOT NULL CHECK (lifecycle IN ('open', 'read_only', 'closed')),\n",
    "    successor_group_id TEXT,\n",
    "    final_versions BLOB,\n",
    "    FOREIGN KEY (group_id) REFERENCES replication_group_material(group_id) ON DELETE CASCADE\n",
    ");\n",
);

/// `SQLite` compares BLOBs lexicographically. Fixed-width big-endian encodings
/// therefore preserve the natural ordering of `u64` values across the full
/// range, so `ORDER BY update_version` remains numerically correct even above
/// `i64::MAX`.
const UPDATE_VERSION_SORT_KEY_BYTES: usize = 8;

// Group persistence is normalised around one material record per group:
//
// replication_group_material -> group_members
//                            -> group_dataset_schemas
//                                      |
// replication_groups (active marker + version vector)
//                                      |
//                         datasets / rows / updates
//
// Invitations and proposals store verified material before policy acceptance.
// Only inserting the replication_groups marker makes that material active and
// permits data-state writes through SQLite foreign keys. pending_group_work has
// exactly one decision or activation row per target group and changes state in
// place when accepted.

const SCHEMA_STATEMENTS: &[&str] = &[
    "PRAGMA foreign_keys = ON;",
    "
CREATE TABLE IF NOT EXISTS replication_group_material (
    group_id TEXT PRIMARY KEY NOT NULL,
    member_count INTEGER NOT NULL,
    local_member_index INTEGER NOT NULL,
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
    key_fingerprint BLOB NOT NULL,
    PRIMARY KEY (group_id, member_index),
    UNIQUE (group_id, member_identity),
    FOREIGN KEY (group_id) REFERENCES replication_group_material(group_id) ON DELETE CASCADE
);
",
    "
CREATE TABLE IF NOT EXISTS group_dataset_schemas (
    group_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    payload BLOB NOT NULL,
    PRIMARY KEY (group_id, dataset_id),
    FOREIGN KEY (group_id) REFERENCES replication_group_material(group_id) ON DELETE CASCADE
);
",
    REPLICATION_GROUPS_SCHEMA_STATEMENT,
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
CREATE TABLE IF NOT EXISTS member_public_keys (
    member_identity TEXT NOT NULL,
    key_fingerprint BLOB NOT NULL,
    signing_public_key BLOB NOT NULL,
    encryption_public_key BLOB NOT NULL,
    PRIMARY KEY (member_identity, key_fingerprint)
);
",
    "
CREATE TABLE IF NOT EXISTS member_key_trust_evidence (
    member_identity TEXT NOT NULL,
    key_fingerprint BLOB NOT NULL,
    evidence_kind TEXT NOT NULL,
    PRIMARY KEY (member_identity, key_fingerprint, evidence_kind),
    FOREIGN KEY (member_identity, key_fingerprint)
        REFERENCES member_public_keys(member_identity, key_fingerprint) ON DELETE RESTRICT
);
",
    "
CREATE TABLE IF NOT EXISTS blocked_key_fingerprints (
    key_fingerprint BLOB PRIMARY KEY NOT NULL
);
",
    "
CREATE TABLE IF NOT EXISTS pending_group_work (
    new_group_id TEXT PRIMARY KEY NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('decision', 'activation')),
    work_kind TEXT NOT NULL,
    old_group_id TEXT,
    payload BLOB NOT NULL,
    FOREIGN KEY (new_group_id) REFERENCES replication_group_material(group_id) ON DELETE CASCADE
);
",
];

async fn initialise_schema(connection: &mut SqliteStoreConnection) -> Result<(), StoreError> {
    for statement in SCHEMA_STATEMENTS {
        sqlx::query(*statement)
            .execute(&mut *connection)
            .await
            .context(SqlxSnafu)?;
    }
    Ok(())
}

mod groups;
mod pending_groups;
mod rows;
mod security;
mod shared;
mod updates;

use groups::*;
use pending_groups::*;
use rows::*;
use security::*;
use shared::*;
use updates::*;

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
    #[snafu(display(
        "Stored schema source for dataset '{dataset_id}' in group '{group_id}' was missing."
    ))]
    MissingSchema {
        group_id: GroupId,
        dataset_id: DatasetId,
    },
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
        "Stored group material for group '{group_id}' conflicts with requested material."
    ))]
    ConflictingGroupMaterial { group_id: GroupId },
    #[snafu(display(
        "Stored pending work for target group '{group_id}' conflicts with requested work."
    ))]
    ConflictingPendingGroupWork { group_id: GroupId },
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
    #[snafu(display(
        "Stored schema payload for group '{group}' was keyed as dataset '{key_dataset}' but contained dataset '{payload_dataset}'."
    ))]
    StoredDatasetSchemaKeyMismatch {
        group: GroupId,
        key_dataset: DatasetId,
        payload_dataset: DatasetId,
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
mod tests;
