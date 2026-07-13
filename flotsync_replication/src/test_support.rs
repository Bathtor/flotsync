use crate::{
    SqliteReplicationStore,
    api::{
        DatasetId,
        DatasetSchema,
        EncryptedLocalMemberPrivateKeys,
        EncryptedStoreSecret,
        GroupSchema,
        ListenerError,
        ListenerExternalSnafu,
        LoadError,
        LocalMemberPrivateKeysRecord,
        MemberKeyTrustEvidenceKind,
        MemberKeyTrustEvidenceRecord,
        MemberPublicKeysRecord,
        ProviderExternalSnafu,
        PublishChangesRequest,
        PublishReceipt,
        ReadToken,
        ReplicationApi,
        ReplicationConfig,
        ReplicationEvent,
        ReplicationEventListener,
        ReplicationSecuritySecrets,
        ReplicationStore,
        RowChange,
        RowChangeBatch,
        RowId,
        RowMutation,
        RuntimeSnafu,
        SchemaSource,
        SnapshotRow,
        SnapshotRowsRequest,
        StoreSecretKeyId,
        process_batches,
    },
    delivery::security::DeliverySecurity,
    runtime::handle::{
        ReplicationRuntime,
        load_replication_runtime_typed_with_security_for_test,
        load_replication_runtime_with_runtime_config_toml,
    },
    security_store::SecurityStore,
};
use flotsync_core::{GroupId, MemberIdentity, member::Identifier, membership::GroupMembers};
use flotsync_data_types::{Field, RowOperations, Schema};
use flotsync_security::{
    GroupKey,
    PublicMemberKeys,
    STORE_SECRET_CRYPTO_VERSION_V1,
    STORE_SECRET_NONCE_LENGTH,
    StoreSecretContext,
    StoreSecretKey,
    public_member_keys_from_public_bundle,
    seal_store_secret_for_test,
    test_group_key_from_id,
    test_support::{TEST_MEMBER_KEY_SEED_LENGTH, member_key_bundles_from_seed},
};
use flotsync_utils::BoxFuture;
use futures_util::FutureExt;
use snafu::prelude::*;
use std::{
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
    sync::{Arc, Mutex, mpsc},
    time::Duration,
};

const TEST_STORE_SECRET_KEY_ID: StoreSecretKeyId = StoreSecretKeyId::from_u128_for_test(1);
const TEST_STORE_SECRET_KEY_BYTES: [u8; 32] = [149; 32];
const TEST_WAIT_TIMEOUT: Duration = Duration::from_secs(5);

/// Dataset id used by the common title-schema fixtures.
///
/// # Panics
///
/// Panics if the hard-coded fixture dataset id no longer satisfies
/// [`DatasetId`] validation.
#[must_use]
pub fn docs_dataset_id() -> DatasetId {
    DatasetId::try_new("docs").expect("test dataset id should build")
}

/// Dataset schema entry with one replicated linear `title` field.
#[must_use]
pub fn docs_dataset_schema() -> DatasetSchema {
    DatasetSchema {
        dataset_id: docs_dataset_id(),
        schema: docs_schema_source(),
    }
}

/// Schema source with one replicated linear `title` field.
#[must_use]
pub fn docs_schema_source() -> SchemaSource {
    SchemaSource::from(Schema::from_fields([Field::linear_string("title")]))
}

/// Group schema containing only the common docs dataset schema.
#[must_use]
pub fn docs_group_schema() -> GroupSchema {
    let dataset_schema = docs_dataset_schema();
    GroupSchema::new(HashMap::from([(
        dataset_schema.dataset_id,
        dataset_schema.schema,
    )]))
}

/// Load a replication runtime for tests that have not yet gained real security setup.
///
/// This helper provisions deterministic local-private member keys into the
/// supplied store, loads runtime security from those records, and then starts
/// the same concrete runtime host used by normal runtime tests. It is temporary
/// test scaffolding for slices before replicated-checklist security setup
/// lands.
///
/// # Errors
///
/// Returns [`LoadError`] when store access, deterministic key generation,
/// local-private-key sealing, or runtime startup fails.
pub async fn load_replication_runtime_with_test_security_toml(
    application_id: Identifier,
    store: Arc<dyn ReplicationStore>,
    listener: Arc<dyn ReplicationEventListener>,
    config: ReplicationConfig,
    runtime_config_toml: &str,
) -> Result<Arc<dyn ReplicationApi>, LoadError> {
    let local_member = store
        .local_member_identity()
        .await
        .boxed()
        .context(RuntimeSnafu {
            application_id: application_id.clone(),
        })?;
    provision_test_security(
        application_id.clone(),
        store.as_ref(),
        &local_member,
        std::iter::empty::<MemberIdentity>(),
    )
    .await?;
    let runtime = load_replication_runtime_with_runtime_config_toml(
        application_id,
        store,
        listener,
        config,
        test_replication_security_secrets(),
        runtime_config_toml,
    )
    .await?;
    Ok(runtime)
}

/// Wait for one test future to resolve within the standard replication timeout.
pub fn wait_for_test_future<F>(future: F) -> F::Output
where
    F: std::future::Future,
{
    flotsync_io::test_support::wait_for_future(
        TEST_WAIT_TIMEOUT,
        future,
        "timed out waiting for test future to resolve",
    )
}

/// Wait for one test reply to resolve within the standard replication timeout.
pub fn wait_for_test_reply<F>(future: F) -> F::Output
where
    F: std::future::Future,
{
    flotsync_io::test_support::wait_for_future(
        TEST_WAIT_TIMEOUT,
        future,
        "timed out waiting for test reply",
    )
}

/// Publish one set of row mutations and wait for the API reply.
///
/// # Panics
///
/// Panics if the publish request is rejected or the runtime does not reply
/// within the test timeout.
pub fn publish_changes(
    runtime: &dyn ReplicationApi,
    read_token: ReadToken,
    changes: Vec<RowMutation>,
) -> PublishReceipt {
    wait_for_test_reply(runtime.publish_changes(PublishChangesRequest {
        read_token,
        changes,
    }))
    .expect("publish should succeed")
}

/// Read a snapshot only to obtain a current read token for later publish calls.
///
/// # Panics
///
/// Panics if the snapshot request or any snapshot batch fails, or the runtime
/// does not reply within the test timeout.
pub fn snapshot_read_token(
    runtime: &dyn ReplicationApi,
    group_id: GroupId,
    dataset_id: DatasetId,
) -> ReadToken {
    let mut snapshot = wait_for_test_reply(runtime.snapshot_rows(SnapshotRowsRequest {
        group_id,
        datasets: HashSet::from([dataset_id]),
        max_rows_per_batch: NonZeroUsize::new(16).expect("snapshot batch size is non-zero"),
        include_tombstones: false,
    }))
    .expect("snapshot should start");
    let read_token = snapshot.read_token.clone();
    while let Some(_batch) =
        wait_for_test_reply(snapshot.rows.next_batch()).expect("snapshot batch should load")
    {}
    read_token
}

/// Drain one snapshot request into decoded title-schema row changes.
///
/// # Panics
///
/// Panics if the snapshot request, snapshot batch stream, or title-schema row
/// decoding fails, or the runtime does not reply within the test timeout.
pub fn drain_title_snapshot_rows(
    runtime: &dyn ReplicationApi,
    request: SnapshotRowsRequest,
) -> Vec<CapturedRowChange> {
    let mut snapshot =
        wait_for_test_reply(runtime.snapshot_rows(request)).expect("snapshot should start");
    let mut rows = Vec::new();
    while let Some(batch) =
        wait_for_test_reply(snapshot.rows.next_batch()).expect("snapshot batch should load")
    {
        for row in batch {
            rows.push(
                CapturedRowChange::capture_snapshot(row).expect("snapshot row should decode"),
            );
        }
    }
    rows
}

/// Row change captured by [`TestEventListener`] from title-schema tests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CapturedRowChange {
    /// A visible upsert with the row id and decoded `title` field.
    Upsert {
        /// Replicated row id.
        row_id: RowId,
        /// Decoded `title` field value.
        title: String,
    },
    /// A delete/tombstone for one row id.
    Delete {
        /// Replicated row id.
        row_id: RowId,
    },
}

impl CapturedRowChange {
    fn capture(change: RowChange) -> Result<Self, ListenerError> {
        match change {
            RowChange::Upsert { row_id, row } => {
                let title = row
                    .get_field_value::<str>("title")
                    .boxed()
                    .context(ListenerExternalSnafu)?
                    .into_owned();
                Ok(Self::Upsert { row_id, title })
            }
            RowChange::Delete { row_id } => Ok(Self::Delete { row_id }),
        }
    }

    fn capture_snapshot(row: SnapshotRow) -> Result<Self, ListenerError> {
        if row.deleted {
            return Ok(Self::Delete { row_id: row.row_id });
        }
        let title = row
            .row
            .get_field_value::<str>("title")
            .boxed()
            .context(ListenerExternalSnafu)?
            .into_owned();
        Ok(Self::Upsert {
            row_id: row.row_id,
            title,
        })
    }
}

/// Data-change event captured by [`TestEventListener`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapturedDataChange {
    /// Decoded row changes in delivery order.
    pub rows: Vec<CapturedRowChange>,
}

/// Event listener used by runtime integration tests.
pub struct TestEventListener {
    data_changes: Mutex<Vec<CapturedDataChange>>,
    data_change_read_tokens: Mutex<Vec<ReadToken>>,
    buffered_events: Mutex<mpsc::Receiver<CapturedDataChange>>,
    buffered_event_tx: mpsc::Sender<CapturedDataChange>,
}

impl Default for TestEventListener {
    fn default() -> Self {
        let (buffered_event_tx, buffered_events) = mpsc::channel();
        Self {
            data_changes: Mutex::new(Vec::new()),
            data_change_read_tokens: Mutex::new(Vec::new()),
            buffered_events: Mutex::new(buffered_events),
            buffered_event_tx,
        }
    }
}

impl TestEventListener {
    fn drain_buffered_events(&self) {
        let receiver = self
            .buffered_events
            .lock()
            .expect("listener event receiver mutex must not be poisoned");
        let mut data_changes = self
            .data_changes
            .lock()
            .expect("listener capture mutex must not be poisoned");
        while let Ok(change) = receiver.try_recv() {
            data_changes.push(change);
        }
    }

    /// Wait for and return the next captured data-change event.
    ///
    /// # Panics
    ///
    /// Panics if no event arrives within the test timeout or the listener state
    /// mutex is poisoned.
    #[must_use]
    pub fn wait_for_next_data_change(&self) -> CapturedDataChange {
        let change = self
            .buffered_events
            .lock()
            .expect("listener event receiver mutex must not be poisoned")
            .recv_timeout(TEST_WAIT_TIMEOUT)
            .expect("timed out waiting for listener data-change event");
        self.data_changes
            .lock()
            .expect("listener capture mutex must not be poisoned")
            .push(change.clone());
        change
    }

    /// Wait until at least `count` data-change events have been captured.
    ///
    /// # Panics
    ///
    /// Panics if the expected event count is not reached within the test
    /// timeout or the listener state mutex is poisoned.
    pub fn wait_for_data_change_count(&self, count: usize) {
        flotsync_io::test_support::eventually(
            TEST_WAIT_TIMEOUT,
            || {
                self.drain_buffered_events();
                self.data_changes
                    .lock()
                    .expect("listener capture mutex must not be poisoned")
                    .len()
                    >= count
            },
            format!("timed out waiting for {count} listener data-change events"),
        );
    }

    /// Return all captured data-change events observed so far.
    ///
    /// # Panics
    ///
    /// Panics if the listener state mutex is poisoned.
    #[must_use]
    pub fn captured_data_changes(&self) -> Vec<CapturedDataChange> {
        self.drain_buffered_events();
        self.data_changes
            .lock()
            .expect("listener capture mutex must not be poisoned")
            .clone()
    }

    /// Return read tokens attached to captured data-change events.
    ///
    /// # Panics
    ///
    /// Panics if the listener state mutex is poisoned.
    #[must_use]
    pub fn captured_data_change_read_tokens(&self) -> Vec<ReadToken> {
        self.drain_buffered_events();
        self.data_change_read_tokens
            .lock()
            .expect("listener read-token capture mutex must not be poisoned")
            .clone()
    }
}

impl ReplicationEventListener for TestEventListener {
    fn on_event(&self, event: ReplicationEvent) -> BoxFuture<'_, Result<(), ListenerError>> {
        async move {
            match event {
                ReplicationEvent::DataChanged {
                    read_token,
                    mut rows,
                } => {
                    let mut captured_rows = Vec::new();
                    process_batches::<RowChangeBatch>(rows.as_mut(), |batch| {
                        for change in batch.drain(..) {
                            let captured = CapturedRowChange::capture(change)
                                .boxed()
                                .context(ProviderExternalSnafu)?;
                            captured_rows.push(captured);
                        }
                        Ok(())
                    })
                    .await
                    .boxed()
                    .context(ListenerExternalSnafu)?;
                    self.data_change_read_tokens
                        .lock()
                        .expect("listener read-token capture mutex must not be poisoned")
                        .push(read_token);
                    self.buffered_event_tx
                        .send(CapturedDataChange {
                            rows: captured_rows,
                        })
                        .expect("listener event channel must remain open while tests are running");
                }
                ReplicationEvent::GroupInvitation { .. }
                | ReplicationEvent::MigrationProposal { .. } => {}
            }
            Ok(())
        }
        .boxed()
    }
}

/// Concrete runtime fixture for integration tests that need delivery host access.
///
/// In `test-support` builds, the runtime host reserves and owns its UDP socket
/// through the socket broker internally. Callers should not reserve a separate
/// endpoint for this fixture; doing so would not affect the host bind address.
pub struct RuntimeTestFixture {
    /// Local member identity loaded from the store.
    pub local_member: MemberIdentity,
    runtime: Arc<ReplicationRuntime>,
    listener: Arc<TestEventListener>,
    store: Arc<SqliteReplicationStore>,
}

impl RuntimeTestFixture {
    /// Build one in-memory SQLite-backed runtime fixture with deterministic security.
    ///
    /// # Panics
    ///
    /// Panics if the store cannot be created, deterministic security cannot be
    /// provisioned, or the runtime cannot be loaded.
    #[must_use]
    pub fn load<I, S>(
        application_id: Identifier,
        local_member: &MemberIdentity,
        schemas: I,
        trusted_members: impl IntoIterator<Item = MemberIdentity>,
    ) -> Self
    where
        I: IntoIterator<Item = (DatasetId, S)>,
        S: Into<SchemaSource>,
    {
        let store = sqlite_store_with_schemas(local_member.clone(), schemas);
        wait_for_test_reply(provision_test_security(
            application_id.clone(),
            store.as_ref(),
            local_member,
            trusted_members,
        ))
        .expect("test security should provision");
        Self::load_from_store(application_id, store)
    }

    /// Build one runtime fixture from an already provisioned `SQLite` store.
    ///
    /// # Panics
    ///
    /// Panics if the local member cannot be loaded, deterministic runtime
    /// security cannot be loaded, or the runtime cannot be started.
    #[must_use]
    pub fn load_from_store(application_id: Identifier, store: Arc<SqliteReplicationStore>) -> Self {
        let listener = Arc::new(TestEventListener::default());
        let local_member = wait_for_test_reply(store.local_member_identity())
            .expect("local member identity should load");
        let security = wait_for_test_reply(load_test_delivery_security(
            application_id.clone(),
            store.clone(),
            &local_member,
        ))
        .expect("runtime security state should load");
        let store_for_runtime: Arc<dyn ReplicationStore> = store.clone();
        let listener_for_runtime: Arc<dyn ReplicationEventListener> = listener.clone();
        let runtime = wait_for_test_reply(load_replication_runtime_typed_with_security_for_test(
            application_id,
            store_for_runtime,
            listener_for_runtime,
            ReplicationConfig::default(),
            security,
            None,
        ))
        .expect("runtime should load");
        Self {
            local_member,
            runtime,
            listener,
            store,
        }
    }

    /// Return the runtime as the public replication API.
    #[must_use]
    pub fn api(&self) -> &dyn ReplicationApi {
        self.runtime.as_ref()
    }

    /// Return the listener that captures runtime data-change events.
    #[must_use]
    pub fn listener(&self) -> &TestEventListener {
        self.listener.as_ref()
    }

    /// Return the backing store.
    #[must_use]
    pub fn store(&self) -> &Arc<SqliteReplicationStore> {
        &self.store
    }

    /// Publish direct peer routes between this fixture and another runtime.
    pub fn connect_direct_peer_routes(&self, peer: &Self) {
        self.runtime.publish_direct_peer_route_for_test(
            peer.local_member.clone(),
            peer.runtime.advertised_loopback_udp_addr_for_test(),
        );
        peer.runtime.publish_direct_peer_route_for_test(
            self.local_member.clone(),
            self.runtime.advertised_loopback_udp_addr_for_test(),
        );
    }

    /// Wait until this runtime has installed one group.
    pub fn wait_for_group_install(&self, group_id: GroupId) {
        flotsync_io::test_support::eventually(
            TEST_WAIT_TIMEOUT,
            || self.contains_group(group_id),
            "timed out waiting for runtime to install group",
        );
    }

    /// Assert this runtime does not install one group during the observation window.
    pub fn assert_group_never_installed(&self, group_id: GroupId) {
        flotsync_io::test_support::assert_never(
            TEST_WAIT_TIMEOUT,
            || self.contains_group(group_id),
            "runtime should not install group",
        );
    }

    /// Return whether this runtime's delivery view contains one group.
    #[must_use]
    pub fn contains_group(&self, group_id: GroupId) -> bool {
        self.runtime
            .membership_snapshot_for_test()
            .contains_group(&group_id)
    }

    /// Return a cloned member set for one installed group.
    #[must_use]
    pub fn group_members(&self, group_id: GroupId) -> Option<GroupMembers> {
        self.runtime
            .membership_snapshot_for_test()
            .members(&group_id)
            .cloned()
    }

    /// Temporarily install one group with deterministic group security material.
    ///
    /// TODO(flotsync-sec.9): Remove this helper once production group creation
    /// installs real group secrets through the normal API.
    ///
    /// # Panics
    ///
    /// Panics if the runtime rejects the test group installation or does not
    /// reply within the test timeout.
    pub fn install_group_for_test(&self, group_id: GroupId, members: GroupMembers) {
        self.runtime
            .install_group_for_test(group_id, members)
            .expect("test group should install");
    }
}

fn sqlite_store_with_schemas<I, S>(
    local_member: MemberIdentity,
    schemas: I,
) -> Arc<SqliteReplicationStore>
where
    I: IntoIterator<Item = (DatasetId, S)>,
    S: Into<SchemaSource>,
{
    let store = wait_for_test_future(SqliteReplicationStore::in_memory_with_schema_sources(
        local_member,
        schemas,
    ))
    .expect("store should build");
    Arc::new(store)
}

/// Provision deterministic local-private keys and trusted peer keys into one store.
///
/// # Errors
///
/// Returns [`LoadError`] when store-secret sealing or any store transaction
/// operation fails.
pub async fn provision_test_security(
    application_id: Identifier,
    store: &dyn ReplicationStore,
    local_member: &Identifier,
    trusted_members: impl IntoIterator<Item = MemberIdentity>,
) -> Result<(), LoadError> {
    let local_seed = test_member_seed(local_member);
    let generated = member_key_bundles_from_seed(local_member.clone(), &local_seed);
    let row_id = local_member.to_string();
    let context = StoreSecretContext {
        table: "local_member",
        column: "private_keys",
        row_id: row_id.as_bytes(),
        key_id: TEST_STORE_SECRET_KEY_ID.as_bytes(),
        crypto_version: STORE_SECRET_CRYPTO_VERSION_V1,
    };
    let sealed = seal_store_secret_for_test(
        &test_store_secret_key(),
        context,
        generated.local_private_bundle.as_bytes(),
        test_store_secret_nonce(local_member),
    )
    .boxed()
    .context(RuntimeSnafu {
        application_id: application_id.clone(),
    })?;
    let record = LocalMemberPrivateKeysRecord {
        member_id: local_member.clone(),
        private_keys: EncryptedLocalMemberPrivateKeys {
            secret: EncryptedStoreSecret::from_store_secret_ciphertext(
                TEST_STORE_SECRET_KEY_ID,
                sealed,
            ),
        },
    };
    let mut transaction = store
        .begin_transaction()
        .await
        .boxed()
        .context(RuntimeSnafu {
            application_id: application_id.clone(),
        })?;
    transaction
        .ensure_local_member_private_keys(record)
        .await
        .boxed()
        .context(RuntimeSnafu {
            application_id: application_id.clone(),
        })?;
    for trusted_member in trusted_members {
        let trusted_keys = test_public_member_keys(&trusted_member);
        let record = MemberPublicKeysRecord::from_public_keys(&trusted_keys);
        let key_id = record.key_id.clone();
        transaction
            .ensure_member_public_keys(record)
            .await
            .boxed()
            .context(RuntimeSnafu {
                application_id: application_id.clone(),
            })?;
        transaction
            .ensure_member_key_trust_evidence(MemberKeyTrustEvidenceRecord {
                key_id,
                evidence_kind: MemberKeyTrustEvidenceKind::LocalExplicitTrust,
            })
            .await
            .boxed()
            .context(RuntimeSnafu {
                application_id: application_id.clone(),
            })?;
    }
    transaction
        .commit()
        .await
        .boxed()
        .context(RuntimeSnafu { application_id })
}

/// Provision one trusted public-key record into an already prepared test store.
///
/// This is used by security integration tests that need present-but-wrong trust
/// records rather than the deterministic keys derived from the member id.
///
/// # Errors
///
/// Returns [`LoadError`] when the store transaction or trust-evidence write fails.
pub async fn provision_test_trusted_public_keys(
    application_id: Identifier,
    store: &dyn ReplicationStore,
    member_id: MemberIdentity,
    public_keys: &PublicMemberKeys,
) -> Result<(), LoadError> {
    let mut record = MemberPublicKeysRecord::from_public_keys(public_keys);
    record.key_id.member_id = member_id;
    let key_id = record.key_id.clone();
    let mut transaction = store
        .begin_transaction()
        .await
        .boxed()
        .context(RuntimeSnafu {
            application_id: application_id.clone(),
        })?;
    transaction
        .ensure_member_public_keys(record)
        .await
        .boxed()
        .context(RuntimeSnafu {
            application_id: application_id.clone(),
        })?;
    transaction
        .ensure_member_key_trust_evidence(MemberKeyTrustEvidenceRecord {
            key_id,
            evidence_kind: MemberKeyTrustEvidenceKind::LocalExplicitTrust,
        })
        .await
        .boxed()
        .context(RuntimeSnafu {
            application_id: application_id.clone(),
        })?;
    transaction
        .commit()
        .await
        .boxed()
        .context(RuntimeSnafu { application_id })
}

/// Load delivery security from deterministic test records already provisioned in a store.
#[cfg(any(test, feature = "test-support"))]
pub(crate) async fn load_test_delivery_security(
    application_id: Identifier,
    store: Arc<dyn ReplicationStore>,
    local_member: &MemberIdentity,
) -> Result<DeliverySecurity, LoadError> {
    DeliverySecurity::load(
        SecurityStore::new(store, ReplicationConfig::default().trust_policy),
        local_member,
        Arc::new(test_store_secret_key()),
        TEST_STORE_SECRET_KEY_ID,
    )
    .await
    .boxed()
    .context(RuntimeSnafu { application_id })
}

/// Build the deterministic runtime security input used by test runtime support.
#[must_use]
pub fn test_replication_security_secrets() -> ReplicationSecuritySecrets {
    ReplicationSecuritySecrets::new(TEST_STORE_SECRET_KEY_ID, Arc::new(test_store_secret_key()))
}

/// Build deterministic seed material for one test member identity.
fn test_member_seed(member: &Identifier) -> [u8; TEST_MEMBER_KEY_SEED_LENGTH] {
    let mut seed = [0u8; TEST_MEMBER_KEY_SEED_LENGTH];
    for (index, byte) in member.to_string().bytes().enumerate() {
        let slot = index % seed.len();
        seed[slot] = seed[slot]
            .wrapping_add(byte)
            .wrapping_add(u8::try_from(index % 251).expect("modulo bounds the test seed offset"));
    }
    seed
}

/// Build deterministic public keys for one test member identity.
///
/// # Panics
///
/// Panics if public-key parsing fails.
#[must_use]
pub fn test_public_member_keys(member: &MemberIdentity) -> PublicMemberKeys {
    let seed = test_member_seed(member);
    let generated = member_key_bundles_from_seed(member.clone(), &seed);
    public_member_keys_from_public_bundle(&generated.public_bundle, member.clone())
        .expect("test public keys should parse")
}

/// Build a deterministic test group key from the full group id.
#[cfg(any(test, feature = "test-support"))]
#[must_use]
pub(crate) fn test_group_key(group_id: GroupId) -> GroupKey {
    test_group_key_from_id(group_id.0)
}

/// Build the deterministic store-secret key used by test runtime support.
fn test_store_secret_key() -> StoreSecretKey {
    StoreSecretKey::from_bytes(TEST_STORE_SECRET_KEY_BYTES)
}

/// Build the deterministic nonce used for one member's encrypted local keys.
fn test_store_secret_nonce(member: &Identifier) -> [u8; STORE_SECRET_NONCE_LENGTH] {
    [test_member_seed(member)[0]; STORE_SECRET_NONCE_LENGTH]
}
