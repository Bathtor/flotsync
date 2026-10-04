use crate::{
    api::{
        DatasetId,
        DatasetRowPageBatch,
        DatasetRowPageMetadata,
        DatasetRowStatePatch,
        DatasetRowStateWrite,
        DatasetRowTransitionPageBatch,
        DatasetRowTransitionPageMetadata,
        DatasetRowTransitionQuery,
        DatasetRowsQuery,
        DatasetSchema,
        EncryptedGroupSecurityMaterial,
        EncryptedLocalMemberPrivateKeys,
        EncryptedStoreSecret,
        GroupDatasetSchemaRef,
        GroupMemberKeys,
        GroupSchema,
        LocalIdentityProvisioningStore,
        LocalMemberPrivateKeysRecord,
        MemberKeyId,
        MemberKeyTrustEvidenceKind,
        MemberKeyTrustEvidenceRecord,
        MemberPublicKeyPredicate,
        MemberPublicKeysRecord,
        OwnedPageBatchInput,
        PageAttempt,
        PageBatch,
        PageCursor,
        PageEnd,
        PageError,
        PageLimit,
        PendingGroupActivationRecord,
        PendingGroupDecisionRecord,
        PendingGroupWorkKey,
        ReplicationGroupLifecycle,
        ReplicationGroupMaterialRecord,
        ReplicationGroupPredicate,
        ReplicationGroupRecord,
        ReplicationRowMetadata,
        ReplicationRowStateSnapshot,
        ReplicationStateRowBatch,
        ReplicationStateRowSource,
        ReplicationStateRowTransitionInput,
        ReplicationStore,
        ReplicationStoreReadTransaction,
        ReplicationStoreTransaction,
        ReplicationUpdateFilter,
        ReplicationUpdatePageInput,
        ReplicationUpdateRecord,
        ReplicationUpdateView,
        ReplicationUpdatesQuery,
        RequestedDatasetRowInput,
        RequestedDatasetRowPageBatch,
        RequestedDatasetRowsQuery,
        RowKey,
        SchemaSource,
        StoreError,
        StoreSecretCryptoVersion,
        StoreSecretKeyId,
        StoreTransactionId,
        WritableReplicationGroupVersionRecord,
        ensure_matching_transition_dataset_references,
        invalid_default_group_security_material,
    },
    codecs::{
        messages::{UpdateMessageProtoSource, VersionVectorCodecError, VersionVectorProtoCodec},
        pending_group::{
            PendingGroupPayloadKind,
            decode_pending_group_activation_payload,
            decode_pending_group_decision_payload,
            encode_pending_group_activation_payload,
            encode_pending_group_decision_payload,
        },
    },
    delivery::contracts::{
        ReliableDeliveryReadSession,
        ReliableDeliveryStore,
        StoredReliableDeliveryWork,
        StoredReliableDeliveryWorkMetadata,
    },
};
use flotsync_core::{
    GroupId,
    MemberIdentity,
    MemberIndex,
    versions::{UpdateId, VersionVector},
};
use flotsync_messages::{
    buffa::{Message as _, MessageView as _},
    codecs::datamodel::encode_row_snapshot,
    datamodel as datamodel_proto,
    proto::{DecodeProto, EncodeProto, ProtoInputDecodeError},
    replication as replication_proto,
    snapshots::datamodel::ProtoSchemaSnapshotDecoder,
};
use flotsync_security::{KeyFingerprint, PublicMemberKeys};
use flotsync_utils::{BoxError, BoxFuture};
use futures_util::FutureExt;
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
    collections::HashMap,
    error::Error as StdError,
    fs::OpenOptions,
    num::NonZeroUsize,
    path::Path,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
    time::Duration,
};
use uuid::Uuid;

const STATEMENT_CACHE_CAPACITY: usize = 64;
const POOL_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(5);
/// Next process-wide numeric identity assigned to a SQLite pool.
///
/// The sequence wraps after every `u64` value has been used. Pool identity
/// uniqueness therefore assumes that no pool survives a complete sequence
/// cycle, which would require creating `2^64` later pools in one process.
static NEXT_POOL_ID: AtomicU64 = AtomicU64::new(1);

/// SQLite store handle used before a local identity has been provisioned.
///
/// The named in-memory database is owned by the `sqlx` pool. Keeping one
/// minimum pooled connection alive avoids an extra keeper connection while
/// still preserving the shared-cache memory database across transaction
/// acquisitions. `sqlx` caches prepared statements per connection, so the store
/// keeps query shapes stable and relies on a modest per-connection cache rather
/// than trying to share prepared handles globally. A provisioner may represent
/// an empty store; consume it with [`Self::into_replication_store`] only after
/// local identity material has been provisioned.
pub struct SqliteReplicationStoreProvisioner {
    pool: SqliteStorePool,
}

impl SqliteReplicationStoreProvisioner {
    /// Create one empty in-memory store provisioner.
    ///
    /// # Errors
    ///
    /// See `StoreError` for failure conditions.
    pub async fn in_memory() -> Result<Self, StoreError> {
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
        Self::from_connect_options(connect_options).await
    }

    /// Create one new disk-backed `SQLite` store provisioner.
    ///
    /// # Errors
    ///
    /// See `StoreError` for failure conditions.
    pub async fn create_file(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref().to_path_buf();
        let reserved_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .with_context(|_| CreateDatabaseFileSnafu { path: path.clone() })?;
        drop(reserved_file);
        let connect_options = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(false)
            .foreign_keys(true)
            .statement_cache_capacity(STATEMENT_CACHE_CAPACITY);
        Self::from_connect_options(connect_options).await
    }

    /// Open one existing disk-backed `SQLite` store provisioner.
    ///
    /// # Errors
    ///
    /// See `StoreError` for failure conditions.
    pub async fn open_file(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let connect_options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(false)
            .foreign_keys(true)
            .statement_cache_capacity(STATEMENT_CACHE_CAPACITY);
        Self::from_connect_options(connect_options).await
    }

    /// Close every SQLite connection before releasing this provisioner.
    ///
    /// Await this operation before dropping a provisioner to ensure its connections have closed.
    /// Concurrent calls wait for the same closure, and calls after completed closure succeed.
    ///
    /// # Errors
    ///
    /// See `StoreError` for failure conditions.
    pub async fn close(&self) -> Result<(), StoreError> {
        self.pool.close().await
    }

    /// Consume this provisioner and construct a replication-ready store.
    ///
    /// # Errors
    ///
    /// Returns an error when no local identity has been provisioned or when stored local-private
    /// material represents more than one distinct member identity.
    pub async fn into_replication_store(self) -> Result<SqliteReplicationStore, StoreError> {
        let local_member = self
            .local_member_identity()
            .await?
            .ok_or_else(|| StoreError::from(MissingLocalMemberIdentitySnafu.build()))?;
        Ok(SqliteReplicationStore {
            local_member,
            pool: self.pool,
        })
    }

    /// Build a provisioner from fully configured `SQLite` connection options.
    async fn from_connect_options(
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
            .context(SQLX_CONNECT_SNAFU)?;
        let mut connection = pool
            .acquire()
            .await
            .context(SQLX_ACQUIRE_CONNECTION_SNAFU)?;
        initialise_schema(&mut connection).await?;
        drop(connection);

        Ok(Self {
            pool: SqliteStorePool::new(pool),
        })
    }
}

impl LocalIdentityProvisioningStore for SqliteReplicationStoreProvisioner {
    fn local_member_identity(&self) -> BoxFuture<'_, Result<Option<MemberIdentity>, StoreError>> {
        let pool = &self.pool;
        async move {
            pool.ensure_open()?;
            let mut connection = pool
                .connections
                .acquire()
                .await
                .context(SQLX_ACQUIRE_CONNECTION_SNAFU)?;
            load_local_member_identity(&mut connection).await
        }
        .boxed()
    }

    fn begin_transaction(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ReplicationStoreTransaction>, StoreError>> {
        let pool = &self.pool;
        async move {
            pool.ensure_open()?;
            let connection = pool
                .connections
                .begin_with("BEGIN IMMEDIATE")
                .await
                .context(SQLX_BEGIN_TRANSACTION_SNAFU)?;
            Ok(Box::new(SqliteReplicationStoreTransaction::new(
                connection,
                SqliteReplicationTransactionKind::Write,
                pool.next_transaction_id(),
            )) as Box<dyn ReplicationStoreTransaction>)
        }
        .boxed()
    }
}

/// SQLite-backed [`ReplicationStore`] with exactly one authoritative local identity.
///
/// # Shutdown
///
/// Applications must stop every runtime and other store user, then await [`Self::close`] before
/// dropping the final handle. In debug builds, dropping an activated store without completing
/// this sequence is a programming error.
pub struct SqliteReplicationStore {
    local_member: MemberIdentity,
    pool: SqliteStorePool,
}

impl SqliteReplicationStore {
    /// Close every SQLite connection during orderly application shutdown.
    ///
    /// The application must first stop every runtime and other user of this store. Concurrent calls
    /// wait for the same closure, and calls after completed closure succeed. In debug builds,
    /// dropping an activated store before closure completes is a programming error.
    ///
    /// # Errors
    ///
    /// See `StoreError` for failure conditions.
    pub async fn close(&self) -> Result<(), StoreError> {
        self.pool.close().await
    }
}

impl ReplicationStore for SqliteReplicationStore {
    fn local_member_identity(&self) -> BoxFuture<'_, Result<MemberIdentity, StoreError>> {
        async move {
            self.pool.ensure_open()?;
            Ok(self.local_member.clone())
        }
        .boxed()
    }

    fn begin_transaction(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ReplicationStoreTransaction>, StoreError>> {
        let pool = &self.pool;
        async move {
            pool.ensure_open()?;
            let connection = pool
                .connections
                .begin_with("BEGIN IMMEDIATE")
                .await
                .context(SQLX_BEGIN_TRANSACTION_SNAFU)?;
            Ok(Box::new(SqliteReplicationStoreTransaction::new(
                connection,
                SqliteReplicationTransactionKind::Write,
                pool.next_transaction_id(),
            )) as Box<dyn ReplicationStoreTransaction>)
        }
        .boxed()
    }

    fn begin_read_transaction(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ReplicationStoreReadTransaction>, StoreError>> {
        let pool = &self.pool;
        async move {
            let transaction = pool.begin_read_transaction().await?;
            Ok(Box::new(transaction) as Box<dyn ReplicationStoreReadTransaction>)
        }
        .boxed()
    }
}

impl ReliableDeliveryStore for SqliteReplicationStore {
    fn begin_read_session(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ReliableDeliveryReadSession>, StoreError>> {
        let pool = &self.pool;
        async move {
            let transaction = pool.begin_read_transaction().await?;
            Ok(Box::new(transaction) as Box<dyn ReliableDeliveryReadSession>)
        }
        .boxed()
    }

    fn load_reliable_delivery_work(
        &self,
        message_id: crate::delivery::shared::MessageId,
    ) -> BoxFuture<'_, Result<Option<StoredReliableDeliveryWork>, StoreError>> {
        let pool = &self.pool;
        async move {
            pool.ensure_open()?;
            let mut connection = pool
                .connections
                .acquire()
                .await
                .context(SQLX_ACQUIRE_CONNECTION_SNAFU)?;
            load_reliable_delivery_work(&mut connection, message_id).await
        }
        .boxed()
    }

    fn store_reliable_delivery_work(
        &self,
        work: StoredReliableDeliveryWork,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        let pool = &self.pool;
        async move {
            pool.ensure_open()?;
            let mut connection = pool
                .connections
                .acquire()
                .await
                .context(SQLX_ACQUIRE_CONNECTION_SNAFU)?;
            store_reliable_delivery_work(&mut connection, &work).await
        }
        .boxed()
    }

    fn remove_reliable_delivery_work(
        &self,
        message_id: crate::delivery::shared::MessageId,
    ) -> BoxFuture<'_, Result<bool, StoreError>> {
        let pool = &self.pool;
        async move {
            pool.ensure_open()?;
            let mut connection = pool
                .connections
                .acquire()
                .await
                .context(SQLX_ACQUIRE_CONNECTION_SNAFU)?;
            remove_reliable_delivery_work(&mut connection, message_id).await
        }
        .boxed()
    }
}

impl ReliableDeliveryReadSession for SqliteReplicationStoreTransaction {
    fn load_reliable_delivery_work_metadata_into<'a>(
        &'a mut self,
        cursor: &'a mut PageCursor<()>,
        batch: &'a mut OwnedNoMetadataPageBatch<'a, StoredReliableDeliveryWorkMetadata>,
    ) -> BoxFuture<'a, Result<(), PageError>> {
        let transaction_id = self.transaction_id;
        async move {
            let page = cursor.begin_page::<SqliteTextPageContinuation, _>(transaction_id, batch)?;
            reliable_delivery::load_reliable_delivery_work_metadata_into(
                self.assert_open_connection(),
                page,
            )
            .await
        }
        .boxed()
    }

    fn release(self: Box<Self>) -> BoxFuture<'static, Result<(), StoreError>> {
        ReplicationStoreReadTransaction::release(self)
    }
}

impl Drop for SqliteReplicationStore {
    fn drop(&mut self) {
        // A destructor panic during unwinding aborts the process and masks the
        // original test failure which prevented orderly shutdown.
        if !std::thread::panicking() {
            debug_assert_eq!(
                self.pool.state(),
                SqliteStoreState::Closed,
                "SqliteReplicationStore must complete explicit close before drop"
            );
        }
    }
}

/// Take the current sequence value and advance it with wrapping arithmetic.
///
/// Callers rely on no object carrying an earlier value surviving the complete
/// `u64` allocation cycle. Tracking live identities would add coordination for
/// a collision that requires `2^64` intervening allocations.
fn allocate_sequence_id(sequence: &AtomicU64) -> u64 {
    sequence.fetch_add(1, Ordering::Relaxed)
}

/// Transferable owner of one SQLite connection pool and its orderly-shutdown state.
struct SqliteStorePool {
    connections: Arc<SqlitePool>,
    /// Monotonic application-owned state shared by every activated-store `Arc` handle.
    state: AtomicU8,
    /// Process-wide sequence value allocated to this concrete pool.
    pool_id: u64,
    /// Next sequence value available to a transaction created from this pool.
    ///
    /// A value can repeat only after this pool creates `2^64` transactions. The
    /// identity scheme assumes no transaction or cursor survives that cycle.
    next_transaction_id: AtomicU64,
}

impl SqliteStorePool {
    /// Build one open pool resource.
    fn new(connections: SqlitePool) -> Self {
        let pool_id = allocate_sequence_id(&NEXT_POOL_ID);
        Self {
            connections: Arc::new(connections),
            state: AtomicU8::new(SqliteStoreState::Open as u8),
            pool_id,
            next_transaction_id: AtomicU64::new(1),
        }
    }

    /// Allocate a practically unique identity among transactions created by
    /// SQLite pools in this process.
    ///
    /// The pool and transaction halves may each wrap. Reuse requires a complete
    /// `u64` allocation cycle while an object carrying the original pair remains
    /// available.
    fn next_transaction_id(&self) -> StoreTransactionId {
        let transaction_id = allocate_sequence_id(&self.next_transaction_id);
        StoreTransactionId::from_uuid(Uuid::from_u64_pair(self.pool_id, transaction_id))
    }

    /// Start one read transaction shared by the replication and delivery views.
    async fn begin_read_transaction(
        &self,
    ) -> Result<SqliteReplicationStoreTransaction, StoreError> {
        self.ensure_open()?;
        let connection = self
            .connections
            .begin_with("BEGIN")
            .await
            .context(SQLX_BEGIN_TRANSACTION_SNAFU)?;
        Ok(SqliteReplicationStoreTransaction::new(
            connection,
            SqliteReplicationTransactionKind::Read,
            self.next_transaction_id(),
        ))
    }

    /// Close every connection and publish completed closure.
    async fn close(&self) -> Result<(), StoreError> {
        let state = match self.state.compare_exchange(
            SqliteStoreState::Open as u8,
            SqliteStoreState::Closing as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => SqliteStoreState::Closing,
            Err(value) => SqliteStoreState::from_atomic(value),
        };
        match state {
            SqliteStoreState::Open => {
                panic!("SQLite store state compare-exchange reported an unchanged open state")
            }
            SqliteStoreState::Closing => {
                self.connections.close().await;
                self.state
                    .store(SqliteStoreState::Closed as u8, Ordering::Release);
                Ok(())
            }
            SqliteStoreState::Closed => Ok(()),
        }
    }

    /// Reject a new operation once orderly shutdown has started.
    fn ensure_open(&self) -> Result<(), StoreError> {
        ensure!(self.state() == SqliteStoreState::Open, ClosedSnafu);
        Ok(())
    }

    /// Load and decode the state value written only by this type.
    fn state(&self) -> SqliteStoreState {
        SqliteStoreState::from_atomic(self.state.load(Ordering::Acquire))
    }
}

/// Monotonic lifecycle for one concrete SQLite pool owner.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SqliteStoreState {
    /// The store accepts new operations.
    Open = 0,
    /// New work is rejected while existing connections finish closing.
    Closing = 1,
    /// Every connection completed orderly closure.
    Closed = 2,
}

impl SqliteStoreState {
    /// Decode one value previously stored in the state atomic.
    fn from_atomic(value: u8) -> Self {
        match value {
            0 => Self::Open,
            1 => Self::Closing,
            2 => Self::Closed,
            _ => panic!("invalid SQLite store state value {value}"),
        }
    }
}

/// One open store transaction backed by `SQLx`'s transaction guard.
///
/// `connection` becomes `None` after explicit commit or rollback. Dropping an
/// open transaction lets `SQLx` queue a rollback before returning the connection
/// to the pool.
struct SqliteReplicationStoreTransaction {
    /// Stable identity of this transaction instance.
    transaction_id: StoreTransactionId,
    connection: Option<SqliteStoreTransaction>,
    kind: SqliteReplicationTransactionKind,
}

impl SqliteReplicationStoreTransaction {
    fn new(
        connection: SqliteStoreTransaction,
        kind: SqliteReplicationTransactionKind,
        transaction_id: StoreTransactionId,
    ) -> Self {
        Self {
            transaction_id,
            connection: Some(connection),
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
    fn transaction_id(&self) -> StoreTransactionId {
        self.transaction_id
    }

    fn load_replication_group<'a>(
        &'a mut self,
        group_id: &'a GroupId,
    ) -> BoxFuture<'a, Result<Option<ReplicationGroupRecord>, StoreError>> {
        async move { load_replication_group(self.assert_open_connection(), group_id).await }.boxed()
    }

    fn load_replication_groups_into<'call, 'predicate: 'call>(
        &'call mut self,
        cursor: &'call mut PageCursor<ReplicationGroupPredicate<'predicate>>,
        batch: &'call mut OwnedNoMetadataPageBatch<'call, ReplicationGroupRecord>,
    ) -> BoxFuture<'call, Result<(), PageError>> {
        let transaction_id = self.transaction_id;
        async move {
            let page = cursor.begin_page::<SqliteTextPageContinuation, _>(transaction_id, batch)?;
            groups::load_replication_groups_into(self.assert_open_connection(), page).await
        }
        .boxed()
    }

    fn load_writable_replication_group_versions_into<'a>(
        &'a mut self,
        cursor: &'a mut PageCursor<()>,
        batch: &'a mut OwnedNoMetadataPageBatch<'a, WritableReplicationGroupVersionRecord>,
    ) -> BoxFuture<'a, Result<(), PageError>> {
        let transaction_id = self.transaction_id;
        async move {
            let page = cursor.begin_page::<SqliteTextPageContinuation, _>(transaction_id, batch)?;
            groups::load_writable_replication_group_versions_into(
                self.assert_open_connection(),
                page,
            )
            .await
        }
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

    fn load_local_member_identity(
        &mut self,
    ) -> BoxFuture<'_, Result<Option<MemberIdentity>, StoreError>> {
        async move { load_local_member_identity(self.assert_open_connection()).await }.boxed()
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

    fn load_member_public_key_ids_into<'a>(
        &'a mut self,
        cursor: &'a mut PageCursor<()>,
        batch: &'a mut OwnedNoMetadataPageBatch<'a, MemberKeyId>,
    ) -> BoxFuture<'a, Result<(), PageError>> {
        let transaction_id = self.transaction_id;
        async move {
            let page = cursor
                .begin_page::<security::MemberKeyPageContinuation, _>(transaction_id, batch)?;
            security::load_member_public_key_ids_into(self.assert_open_connection(), page).await
        }
        .boxed()
    }

    fn load_member_public_keys_into<'call, 'predicate: 'call>(
        &'call mut self,
        cursor: &'call mut PageCursor<MemberPublicKeyPredicate<'predicate>>,
        batch: &'call mut OwnedNoMetadataPageBatch<'call, MemberPublicKeysRecord>,
    ) -> BoxFuture<'call, Result<(), PageError>> {
        let transaction_id = self.transaction_id;
        async move {
            let page = cursor
                .begin_page::<security::MemberKeyPageContinuation, _>(transaction_id, batch)?;
            security::load_member_public_keys_into(self.assert_open_connection(), page).await
        }
        .boxed()
    }

    fn load_member_key_trust_evidence_into<'call, 'predicate: 'call>(
        &'call mut self,
        cursor: &'call mut PageCursor<&'predicate MemberKeyId>,
        batch: &'call mut OwnedNoMetadataPageBatch<'call, MemberKeyTrustEvidenceKind>,
    ) -> BoxFuture<'call, Result<(), PageError>> {
        let transaction_id = self.transaction_id;
        async move {
            let page = cursor.begin_page::<SqliteTextPageContinuation, _>(transaction_id, batch)?;
            security::load_member_key_trust_evidence_into(self.assert_open_connection(), page).await
        }
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

    fn load_replication_updates_into<'call, 'query: 'call>(
        &'call mut self,
        cursor: &'call mut PageCursor<ReplicationUpdatesQuery<'query>>,
        batch: &'call mut dyn PageBatch<Input = ReplicationUpdatePageInput, Metadata = ()>,
    ) -> BoxFuture<'call, Result<(), PageError>> {
        let transaction_id = self.transaction_id;
        async move {
            let page = cursor
                .begin_page::<updates::SqliteUpdatePageContinuation, _>(transaction_id, batch)?;
            updates::load_replication_updates_into(self.assert_open_connection(), page).await
        }
        .boxed()
    }

    fn load_replication_update_ids_into<'call, 'query: 'call>(
        &'call mut self,
        cursor: &'call mut PageCursor<ReplicationUpdatesQuery<'query>>,
        batch: &'call mut dyn PageBatch<Input = OwnedPageBatchInput<UpdateId>, Metadata = ()>,
    ) -> BoxFuture<'call, Result<(), PageError>> {
        let transaction_id = self.transaction_id;
        async move {
            let page = cursor
                .begin_page::<updates::SqliteUpdatePageContinuation, _>(transaction_id, batch)?;
            updates::load_replication_update_ids_into(self.assert_open_connection(), page).await
        }
        .boxed()
    }

    fn load_dataset_rows_into<'call, 'query: 'call>(
        &'call mut self,
        cursor: &'call mut PageCursor<RequestedDatasetRowsQuery<'query>>,
        batch: &'call mut RequestedDatasetRowPageBatch,
    ) -> BoxFuture<'call, Result<(), PageError>> {
        let transaction_id = self.transaction_id;
        async move {
            let schema = cursor.params().dataset().context().schema;
            batch.prepare_for_schema(schema);
            let page =
                cursor.begin_page::<SqliteRequestedRowsContinuation, _>(transaction_id, batch)?;
            rows::load_dataset_rows_into(self.assert_open_connection(), page).await
        }
        .boxed()
    }

    fn scan_dataset_rows_into<'call, 'query: 'call>(
        &'call mut self,
        cursor: &'call mut PageCursor<DatasetRowsQuery<'query>>,
        batch: &'call mut DatasetRowPageBatch,
    ) -> BoxFuture<'call, Result<(), PageError>> {
        let transaction_id = self.transaction_id;
        async move {
            let schema = cursor.params().context().schema;
            batch.prepare_for_schema(schema);
            let page = cursor.begin_page::<SqliteTextPageContinuation, _>(transaction_id, batch)?;
            rows::scan_dataset_rows_into(self.assert_open_connection(), page).await
        }
        .boxed()
    }

    fn scan_dataset_row_transitions_into<'call, 'query: 'call>(
        &'call mut self,
        cursor: &'call mut PageCursor<DatasetRowTransitionQuery<'query>>,
        batch: &'call mut DatasetRowTransitionPageBatch,
    ) -> BoxFuture<'call, Result<(), PageError>> {
        let transaction_id = self.transaction_id;
        async move {
            let previous_schema = cursor.params().previous().context().schema;
            let current_schema = cursor.params().current().context().schema;
            batch.prepare_for_schemas(previous_schema, current_schema);
            let page = cursor.begin_page::<SqliteTextPageContinuation, _>(transaction_id, batch)?;
            rows::scan_dataset_row_transitions_into(self.assert_open_connection(), page).await
        }
        .boxed()
    }

    fn load_pending_group_decisions_into<'a>(
        &'a mut self,
        cursor: &'a mut PageCursor<()>,
        batch: &'a mut OwnedNoMetadataPageBatch<'a, PendingGroupDecisionRecord>,
    ) -> BoxFuture<'a, Result<(), PageError>> {
        let transaction_id = self.transaction_id;
        async move {
            let page = cursor.begin_page::<SqliteTextPageContinuation, _>(transaction_id, batch)?;
            pending_groups::load_pending_group_decisions_into(self.assert_open_connection(), page)
                .await
        }
        .boxed()
    }

    fn load_pending_group_decision<'a>(
        &'a mut self,
        group_id: &'a GroupId,
    ) -> BoxFuture<'a, Result<Option<PendingGroupDecisionRecord>, StoreError>> {
        async move { load_pending_group_decision(self.assert_open_connection(), group_id).await }
            .boxed()
    }

    fn load_pending_group_activations_into<'a>(
        &'a mut self,
        cursor: &'a mut PageCursor<()>,
        batch: &'a mut OwnedNoMetadataPageBatch<'a, PendingGroupActivationRecord>,
    ) -> BoxFuture<'a, Result<(), PageError>> {
        let transaction_id = self.transaction_id;
        async move {
            let page = cursor.begin_page::<SqliteTextPageContinuation, _>(transaction_id, batch)?;
            pending_groups::load_pending_group_activations_into(self.assert_open_connection(), page)
                .await
        }
        .boxed()
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
            connection
                .rollback()
                .await
                .context(SQLX_ROLLBACK_TRANSACTION_SNAFU)?;
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

    fn apply_dataset_row_patch<'a>(
        &'a mut self,
        dataset: GroupDatasetSchemaRef<'a>,
        patch: &'a DatasetRowStatePatch,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        async move { apply_dataset_row_patch(self.assert_open_connection(), dataset, patch).await }
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
            connection
                .commit()
                .await
                .context(SQLX_COMMIT_TRANSACTION_SNAFU)?;
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
            connection
                .rollback()
                .await
                .context(SQLX_ROLLBACK_TRANSACTION_SNAFU)?;
            Ok(())
        }
        .boxed()
    }
}

type SqliteStoreConnection = SqliteConnection;
type SqliteStoreTransaction = sqlx::Transaction<'static, Sqlite>;
/// Page batch shape used by SQLite metadata queries which accept owned records
/// and produce no query-specific metadata.
type OwnedNoMetadataPageBatch<'a, Record> =
    dyn PageBatch<Input = OwnedPageBatchInput<Record>, Metadata = ()> + 'a;

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
    group_name TEXT,
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
    row_created_by_node_index INTEGER,
    -- Stores the creator's u64 version bits in SQLite's signed INTEGER domain.
    row_created_by_version INTEGER,
    row_last_changed_versions BLOB NOT NULL,
    CHECK ((row_created_by_node_index IS NULL) = (row_created_by_version IS NULL)),
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
CREATE TABLE IF NOT EXISTS reliable_delivery_work (
    message_id TEXT PRIMARY KEY NOT NULL,
    recipient TEXT NOT NULL,
    first_submitted_at TEXT NOT NULL,
    encoded_envelope BLOB NOT NULL
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
            .context(SQLX_INITIALISE_SCHEMA_SNAFU)?;
    }
    Ok(())
}

mod error;
mod groups;
mod paging;
mod pending_groups;
mod reliable_delivery;
mod rows;
mod security;
mod shared;
mod updates;

#[allow(
    clippy::wildcard_imports,
    reason = "The SQLite facade reuses local persistence-domain helpers across transaction methods."
)]
use error::*;
#[allow(
    clippy::wildcard_imports,
    reason = "The SQLite facade reuses local persistence-domain helpers across transaction methods."
)]
use groups::*;
pub(crate) use paging::SqliteTextPageContinuation;
use paging::{
    continuation_record_index,
    finish_page,
    finish_page_with_metadata,
    push_initial_text_page_window,
    push_ordered_text_page_window,
    push_page_order_and_limit,
    push_text_page_window,
    push_text_window,
};
#[allow(
    clippy::wildcard_imports,
    reason = "The SQLite facade reuses local persistence-domain helpers across transaction methods."
)]
use pending_groups::*;
use reliable_delivery::{
    load_reliable_delivery_work,
    remove_reliable_delivery_work,
    store_reliable_delivery_work,
};
#[allow(
    clippy::wildcard_imports,
    reason = "The SQLite facade reuses local persistence-domain helpers across transaction methods."
)]
use rows::*;
#[allow(
    clippy::wildcard_imports,
    reason = "The SQLite facade reuses local persistence-domain helpers across transaction methods."
)]
use security::*;
#[allow(
    clippy::wildcard_imports,
    reason = "The SQLite facade reuses local persistence-domain helpers across transaction methods."
)]
use shared::*;
#[cfg(test)]
pub(crate) use updates::SqliteUpdatePageContinuation;
#[allow(
    clippy::wildcard_imports,
    reason = "The SQLite facade reuses local persistence-domain helpers across transaction methods."
)]
use updates::*;

#[cfg(test)]
mod tests;
