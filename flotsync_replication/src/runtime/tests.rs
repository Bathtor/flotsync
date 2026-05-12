use super::{
    errors::{InboundDeliveryError, PublishChangesError},
    handle::{
        ReplicationRuntime,
        load_replication_runtime_typed,
        load_replication_runtime_typed_with_runtime_config_toml,
        wait_for_test_reply,
    },
    host::{DeliveryRuntimeHost, DeliveryRuntimeHostTestExt, PreconfiguredPeerRoutesPublishMode},
    in_memory::{
        LocalDataset,
        apply_local_delete,
        apply_local_upsert,
        apply_rebased_local_upsert,
        validate_inbound_update_read_versions,
        validate_update_mapping,
    },
    load_replication_runtime,
    messages::{DatasetUpdateMessage, UpdateBatchMessage, UpdateMessage},
};
use crate::{
    GroupMembers,
    GroupMemberships,
    MAX_VERSION_VALUE,
    SqliteReplicationStore,
    api::{
        ApiError,
        CreateGroupRequest,
        DatasetId,
        DatasetRowPatch,
        DatasetRowSlice,
        DatasetUpdateRecord,
        GroupId,
        ListenerError,
        ListenerExternalSnafu,
        MemberIdentity,
        MemberIndex,
        ProviderExternalSnafu,
        PublishChangesRequest,
        PublishReceipt,
        ReadToken,
        ReplicationApi,
        ReplicationConfig,
        ReplicationEvent,
        ReplicationEventListener,
        ReplicationGroupRecord,
        ReplicationStore,
        ReplicationStoreReadTransaction,
        ReplicationStoreTransaction,
        ReplicationUpdateFilter,
        ReplicationUpdateRecord,
        RowChange,
        RowChangeBatch,
        RowId,
        RowKey,
        RowKeyIterator,
        RowMutation,
        SchemaSource,
        SnapshotRow,
        SnapshotRowsRequest,
        StoreError,
        SummaryRequest,
        process_batches,
    },
};
use flotsync_core::{
    member::Identifier,
    versions::{UpdateId, VersionVector},
};
use flotsync_data_types::{Field, RowOperations, Schema, TableOperations};
use flotsync_io::test_support::{ReservedSocketKind, eventually, reserve_sockets};
use flotsync_utils::BoxFuture;
use futures_util::FutureExt;
use snafu::ResultExt;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    num::NonZeroUsize,
    sync::{Arc, LazyLock, Mutex, mpsc},
    time::Duration,
};
use uuid::Uuid;

const TEST_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
const ALICE_MEMBER_SEGMENTS: [&str; 1] = ["alice"];
const BOB_MEMBER_SEGMENTS: [&str; 1] = ["bob"];
const PROBE_MEMBER_SEGMENTS: [&str; 1] = ["probe"];
const APP_ALICE_SEGMENTS: [&str; 2] = ["app", "alice"];
const APP_BOB_SEGMENTS: [&str; 2] = ["app", "bob"];
const APP_PROBE_SEGMENTS: [&str; 2] = ["app", "probe"];

static STATIC_TITLE_SCHEMA: LazyLock<Schema> =
    LazyLock::new(|| Schema::from_fields([Field::linear_string("title")]));

struct RuntimeFixture<S> {
    runtime: Arc<ReplicationRuntime>,
    listener: Arc<ListenerStub>,
    store: Arc<S>,
}

/// Test-only store wrapper that can fail one future row-patch write while
/// delegating all durable state to the wrapped `SQLite` store.
struct FailingStore<S> {
    inner: Arc<S>,
    fail_next_apply_dataset_row_patch: Arc<Mutex<Option<DatasetId>>>,
}

impl<S> FailingStore<S> {
    fn new(inner: Arc<S>) -> Self {
        Self {
            inner,
            fail_next_apply_dataset_row_patch: Arc::new(Mutex::new(None)),
        }
    }

    fn fail_next_apply_dataset_row_patch(&self, dataset_id: DatasetId) {
        *self
            .fail_next_apply_dataset_row_patch
            .lock()
            .expect("failing store mutex must not be poisoned") = Some(dataset_id);
    }
}

impl<S> ReplicationStore for FailingStore<S>
where
    S: ReplicationStore + 'static,
{
    fn local_member_identity(&self) -> BoxFuture<'_, Result<MemberIdentity, StoreError>> {
        self.inner.local_member_identity()
    }

    fn load_dataset_schema(
        &self,
        dataset_id: &DatasetId,
    ) -> BoxFuture<'_, Result<Option<SchemaSource>, StoreError>> {
        self.inner.load_dataset_schema(dataset_id)
    }

    fn begin_transaction(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ReplicationStoreTransaction>, StoreError>> {
        let inner = self.inner.clone();
        let fail_next_apply_dataset_row_patch = self.fail_next_apply_dataset_row_patch.clone();
        async move {
            let inner = inner.begin_transaction().await?;
            Ok(Box::new(FailingStoreTransaction {
                inner: Some(inner),
                fail_next_apply_dataset_row_patch,
            }) as Box<dyn ReplicationStoreTransaction>)
        }
        .boxed()
    }

    fn begin_read_transaction(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ReplicationStoreReadTransaction>, StoreError>> {
        self.inner.begin_read_transaction()
    }
}

struct FailingStoreTransaction {
    inner: Option<Box<dyn ReplicationStoreTransaction>>,
    fail_next_apply_dataset_row_patch: Arc<Mutex<Option<DatasetId>>>,
}

impl ReplicationStoreTransaction for FailingStoreTransaction {
    fn load_replication_group<'a>(
        &'a mut self,
        group_id: &'a GroupId,
    ) -> BoxFuture<'a, Result<Option<ReplicationGroupRecord>, StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated reads")
            .load_replication_group(group_id)
    }

    fn load_replication_groups(
        &mut self,
    ) -> BoxFuture<'_, Result<Vec<ReplicationGroupRecord>, StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated reads")
            .load_replication_groups()
    }

    fn load_replication_groups_for_ids<'a>(
        &'a mut self,
        group_ids: &'a HashSet<GroupId>,
    ) -> BoxFuture<'a, Result<Vec<ReplicationGroupRecord>, StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated reads")
            .load_replication_groups_for_ids(group_ids)
    }

    fn insert_replication_group(
        &mut self,
        group: ReplicationGroupRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated writes")
            .insert_replication_group(group)
    }

    fn update_replication_group_version_vector<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        version_vector: VersionVector,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated writes")
            .update_replication_group_version_vector(group_id, version_vector)
    }

    fn load_dataset_rows<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        dataset_id: &'a DatasetId,
        row_keys: &'a mut RowKeyIterator<'a>,
    ) -> BoxFuture<'a, Result<DatasetRowSlice, StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated reads")
            .load_dataset_rows(group_id, dataset_id, row_keys)
    }

    fn apply_dataset_row_patch(
        &mut self,
        patch: DatasetRowPatch,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        let failure = self.fail_next_apply_dataset_row_patch.clone();
        async move {
            let should_fail = {
                let mut failure = failure
                    .lock()
                    .expect("failing store mutex must not be poisoned");
                if failure.as_ref() == Some(&patch.dataset_id) {
                    *failure = None;
                    true
                } else {
                    false
                }
            };
            if should_fail {
                self.inner
                    .take()
                    .expect("failing store transaction must remain open during rollback")
                    .rollback()
                    .await?;
                return Err(StoreError::StoreExternal {
                    source: Box::new(std::io::Error::other(format!(
                        "failing store intentionally failed dataset row patch apply for '{}'",
                        patch.dataset_id
                    ))),
                });
            }
            self.inner
                .as_mut()
                .expect("failing store transaction must remain open during delegated writes")
                .apply_dataset_row_patch(patch)
                .await
        }
        .boxed()
    }

    fn load_replication_update<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        update_id: UpdateId,
    ) -> BoxFuture<'a, Result<Option<ReplicationUpdateRecord>, StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated reads")
            .load_replication_update(group_id, update_id)
    }

    fn load_replication_updates<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        filter: ReplicationUpdateFilter,
        limit: Option<NonZeroUsize>,
    ) -> BoxFuture<'a, Result<Vec<ReplicationUpdateRecord>, StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated reads")
            .load_replication_updates(group_id, filter, limit)
    }

    fn load_replication_update_ids<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        filter: ReplicationUpdateFilter,
        limit: Option<NonZeroUsize>,
    ) -> BoxFuture<'a, Result<Vec<UpdateId>, StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated reads")
            .load_replication_update_ids(group_id, filter, limit)
    }

    fn append_replication_update(
        &mut self,
        update: ReplicationUpdateRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated writes")
            .append_replication_update(update)
    }

    fn mark_replication_update_applied<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        update_id: UpdateId,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated writes")
            .mark_replication_update_applied(group_id, update_id)
    }

    fn commit(self: Box<Self>) -> BoxFuture<'static, Result<(), StoreError>> {
        let Self { inner, .. } = *self;
        inner
            .expect("failing store transaction must remain open until commit")
            .commit()
    }

    fn rollback(self: Box<Self>) -> BoxFuture<'static, Result<(), StoreError>> {
        let Self { inner, .. } = *self;
        inner
            .expect("failing store transaction must remain open until rollback")
            .rollback()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CapturedDataChange {
    rows: Vec<CapturedRowChange>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CapturedRowChange {
    Upsert { row_id: RowId, title: String },
    Delete { row_id: RowId },
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

struct ListenerStub {
    data_changes: Mutex<Vec<CapturedDataChange>>,
    data_change_read_tokens: Mutex<Vec<ReadToken>>,
    buffered_events: Mutex<mpsc::Receiver<CapturedDataChange>>,
    buffered_event_tx: mpsc::Sender<CapturedDataChange>,
}

impl Default for ListenerStub {
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

impl ListenerStub {
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

    fn wait_for_next_data_change(&self) -> CapturedDataChange {
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

    fn wait_for_data_change_count(&self, count: usize) {
        eventually(
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

    fn captured_data_changes(&self) -> Vec<CapturedDataChange> {
        self.drain_buffered_events();
        self.data_changes
            .lock()
            .expect("listener capture mutex must not be poisoned")
            .clone()
    }

    fn captured_data_change_read_tokens(&self) -> Vec<ReadToken> {
        self.drain_buffered_events();
        self.data_change_read_tokens
            .lock()
            .expect("listener read-token capture mutex must not be poisoned")
            .clone()
    }
}

impl ReplicationEventListener for ListenerStub {
    fn on_event(&self, event: ReplicationEvent) -> BoxFuture<'_, Result<(), ListenerError>> {
        Box::pin(async move {
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
                ReplicationEvent::GroupInvitation { .. } => {}
            }
            Ok(())
        })
    }
}

fn docs_dataset_id() -> DatasetId {
    DatasetId::try_new("docs").expect("dataset id should be valid")
}

fn alice_member() -> Identifier {
    Identifier::from_array(ALICE_MEMBER_SEGMENTS)
}

fn bob_member() -> Identifier {
    Identifier::from_array(BOB_MEMBER_SEGMENTS)
}

fn app_alice_id() -> Identifier {
    Identifier::from_array(APP_ALICE_SEGMENTS)
}

fn app_bob_id() -> Identifier {
    Identifier::from_array(APP_BOB_SEGMENTS)
}

fn app_probe_id() -> Identifier {
    Identifier::from_array(APP_PROBE_SEGMENTS)
}

fn title_schema_shared() -> Arc<Schema> {
    Arc::new(STATIC_TITLE_SCHEMA.clone())
}

fn title_schema_static() -> &'static Schema {
    &STATIC_TITLE_SCHEMA
}

fn title_note_schema_shared() -> Arc<Schema> {
    Arc::new(Schema::from_fields([
        Field::linear_string("title"),
        Field::linear_string("note"),
    ]))
}

fn test_row_id(group_id: GroupId, dataset_id: DatasetId, raw: u128) -> RowId {
    RowId {
        group_id,
        dataset_id,
        row_key: RowKey(Uuid::from_u128(raw)),
    }
}

fn load_runtime(
    application_id: Identifier,
    local_member: MemberIdentity,
) -> Arc<ReplicationRuntime> {
    let store =
        Arc::new(SqliteReplicationStore::in_memory(local_member).expect("store should build"));
    let listener = Arc::new(ListenerStub::default());
    load_runtime_with_parts(application_id, store, listener)
}

fn sqlite_store_with_schemas<I, S>(
    local_member: MemberIdentity,
    schemas: I,
) -> Arc<SqliteReplicationStore>
where
    I: IntoIterator<Item = (DatasetId, S)>,
    S: Into<SchemaSource>,
{
    Arc::new(
        SqliteReplicationStore::in_memory_with_schema_sources(local_member, schemas)
            .expect("store should build"),
    )
}

fn load_runtime_fixture<I, S>(
    application_id: Identifier,
    local_member: MemberIdentity,
    schemas: I,
) -> RuntimeFixture<SqliteReplicationStore>
where
    I: IntoIterator<Item = (DatasetId, S)>,
    S: Into<SchemaSource>,
{
    let listener = Arc::new(ListenerStub::default());
    let store = sqlite_store_with_schemas(local_member, schemas);
    let runtime = load_runtime_with_parts(application_id, store.clone(), listener.clone());
    RuntimeFixture {
        runtime,
        listener,
        store,
    }
}

fn start_host(local_member: &MemberIdentity) -> DeliveryRuntimeHost {
    let store = Arc::new(
        SqliteReplicationStore::in_memory(local_member.clone()).expect("store should build"),
    );
    let listener = Arc::new(ListenerStub::default());
    let host =
        DeliveryRuntimeHost::start(local_member, store, listener).expect("host should start");
    host.wait_for_runtime_startup();
    host
}

fn load_runtime_with_parts<S>(
    application_id: Identifier,
    store: Arc<S>,
    listener: Arc<ListenerStub>,
) -> Arc<ReplicationRuntime>
where
    S: ReplicationStore + 'static,
{
    wait_for_test_reply(load_replication_runtime_typed(
        application_id,
        store,
        listener,
        ReplicationConfig::default(),
    ))
    .expect("runtime should load")
}

fn load_runtime_with_parts_and_runtime_config_toml<S>(
    application_id: Identifier,
    store: Arc<S>,
    listener: Arc<ListenerStub>,
    runtime_config_toml: &str,
) -> Arc<ReplicationRuntime>
where
    S: ReplicationStore + 'static,
{
    wait_for_test_reply(load_replication_runtime_typed_with_runtime_config_toml(
        application_id,
        store,
        listener,
        ReplicationConfig::default(),
        Some(runtime_config_toml),
    ))
    .expect("runtime should load")
}

fn static_peer_route_toml(peer: &MemberIdentity, remote_addr: SocketAddr) -> String {
    format!(
        r#"
        [[flotsync.replication.runtime.static-peer-routes]]
        name = "{peer}"
        protocol = "udp"
        ip = "{ip}"
        port = {port}
        "#,
        ip = remote_addr.ip(),
        port = remote_addr.port(),
    )
}

fn persist_group_in_store<S>(store: &S, group: ReplicationGroupRecord)
where
    S: ReplicationStore + ?Sized,
{
    let mut transaction =
        wait_for_test_reply(store.begin_transaction()).expect("transaction should start");
    wait_for_test_reply(transaction.insert_replication_group(group)).expect("group should persist");
    wait_for_test_reply(transaction.commit()).expect("transaction should commit");
}

fn load_persisted_group<S>(store: &S, group_id: GroupId) -> ReplicationGroupRecord
where
    S: ReplicationStore + ?Sized,
{
    let mut transaction =
        wait_for_test_reply(store.begin_transaction()).expect("transaction should start");
    let group = wait_for_test_reply(transaction.load_replication_group(&group_id))
        .expect("group should load")
        .expect("group should exist");
    wait_for_test_reply(transaction.commit()).expect("transaction should commit");
    group
}

fn load_persisted_update<S>(
    store: &S,
    group_id: GroupId,
    update_id: UpdateId,
) -> Option<ReplicationUpdateRecord>
where
    S: ReplicationStore + ?Sized,
{
    let mut transaction =
        wait_for_test_reply(store.begin_transaction()).expect("transaction should start");
    let update = wait_for_test_reply(transaction.load_replication_update(&group_id, update_id))
        .expect("update should load");
    wait_for_test_reply(transaction.commit()).expect("transaction should commit");
    update
}

fn load_persisted_row_slice<S>(
    store: &S,
    group_id: GroupId,
    dataset_id: &DatasetId,
    row_keys: impl IntoIterator<Item = RowKey>,
) -> DatasetRowSlice
where
    S: ReplicationStore + ?Sized,
{
    let requested_row_keys: Vec<_> = row_keys.into_iter().collect();
    let mut requested_row_keys = requested_row_keys.iter();
    let mut transaction =
        wait_for_test_reply(store.begin_transaction()).expect("transaction should start");
    let row_slice = wait_for_test_reply(transaction.load_dataset_rows(
        &group_id,
        dataset_id,
        &mut requested_row_keys,
    ))
    .expect("row slice should load");
    wait_for_test_reply(transaction.commit()).expect("transaction should commit");
    row_slice
}

fn drain_snapshot_rows(
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

fn snapshot_read_token(
    runtime: &dyn ReplicationApi,
    group_id: GroupId,
    dataset_id: DatasetId,
) -> ReadToken {
    let mut snapshot = wait_for_test_reply(runtime.snapshot_rows(SnapshotRowsRequest {
        group_id,
        datasets: HashSet::from([dataset_id]),
        max_rows_per_batch: NonZeroUsize::new(16).unwrap(),
        include_tombstones: false,
    }))
    .expect("snapshot should start");
    let read_token = snapshot.read_token.clone();
    while let Some(_batch) =
        wait_for_test_reply(snapshot.rows.next_batch()).expect("snapshot batch should load")
    {}
    read_token
}

fn publish_changes(
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

fn title_update_message(
    group_id: GroupId,
    dataset_id: DatasetId,
    row_raw: u128,
    title: &str,
    update_id: UpdateId,
    read_versions: VersionVector,
) -> (RowId, UpdateMessage) {
    let row_id = test_row_id(group_id, dataset_id.clone(), row_raw);
    let mut source_dataset = LocalDataset::new(title_schema_static());
    let operation = apply_local_upsert(
        &mut source_dataset,
        &row_id,
        crate::row_values! { "title" => title },
        update_id,
    )
    .expect("operation should build")
    .expect("operation should apply")
    .encoded_operation;
    (
        row_id,
        UpdateMessage {
            group_id,
            update_id,
            read_versions,
            dataset_updates: vec![DatasetUpdateMessage {
                dataset_id,
                operations: vec![operation],
            }],
        },
    )
}

fn sort_captured_rows(rows: &mut [CapturedRowChange]) {
    rows.sort_by_key(|row| match row {
        CapturedRowChange::Upsert { row_id, .. } | CapturedRowChange::Delete { row_id } => {
            row_id.to_string()
        }
    });
}

fn snapshot_string_field(
    runtime: &dyn ReplicationApi,
    group_id: GroupId,
    dataset_id: DatasetId,
    row_id: &RowId,
    field_name: &str,
) -> String {
    let mut snapshot = wait_for_test_reply(runtime.snapshot_rows(SnapshotRowsRequest {
        group_id,
        datasets: HashSet::from([dataset_id]),
        max_rows_per_batch: NonZeroUsize::new(16).unwrap(),
        include_tombstones: false,
    }))
    .expect("snapshot should start");
    while let Some(batch) =
        wait_for_test_reply(snapshot.rows.next_batch()).expect("snapshot batch should load")
    {
        for row in batch {
            if row.row_id == *row_id {
                return row
                    .row
                    .get_field_value::<str>(field_name)
                    .expect("snapshot field should decode")
                    .into_owned();
            }
        }
    }
    panic!("snapshot row {row_id} should exist");
}

fn wait_for_group_install(runtime: &Arc<ReplicationRuntime>, group_id: GroupId) {
    eventually(
        TEST_WAIT_TIMEOUT,
        || {
            runtime
                .host()
                .membership_snapshot()
                .contains_group(&group_id)
        },
        "timed out waiting for runtime to install group",
    );
}

#[test]
fn delivery_runtime_host_updates_shared_group_memberships() {
    let local_member = Identifier::from_array(ALICE_MEMBER_SEGMENTS);
    let mut host = start_host(&local_member);
    let group_id = GroupId(Uuid::from_u128(1));
    let memberships = GroupMemberships::from_groups([(
        group_id,
        GroupMembers::singleton(local_member).expect("group should build"),
    )]);

    host.replace_group_memberships(memberships);

    assert!(host.membership_snapshot().contains_group(&group_id));
    host.shutdown();
}

#[test]
fn load_replication_runtime_returns_concrete_runtime() {
    let application_id = app_probe_id();
    let store =
        Arc::new(SqliteReplicationStore::in_memory(alice_member()).expect("store should build"));
    let listener = Arc::new(ListenerStub::default());

    let runtime = wait_for_test_reply(load_replication_runtime(
        application_id,
        store,
        listener,
        ReplicationConfig::default(),
    ));

    if let Err(error) = runtime {
        panic!("runtime load should succeed: {error}");
    }
}

#[test]
fn delivery_runtime_host_defaults_to_loopback_local_endpoint_bind_in_tests() {
    let mut host = start_host(&Identifier::from_array(PROBE_MEMBER_SEGMENTS));

    assert!(host.external_udp_bind_addr().ip().is_loopback());
    host.shutdown();
}

#[test]
fn runtime_host_publishes_static_peer_routes_after_local_endpoint_bind() {
    let remote_endpoint_lease = reserve_sockets(&[ReservedSocketKind::UdpSocket]);
    let remote_addr = remote_endpoint_lease.addr(0);
    let bob_member = bob_member();
    let runtime_config_toml = static_peer_route_toml(&bob_member, remote_addr);
    let store =
        Arc::new(SqliteReplicationStore::in_memory(alice_member()).expect("store should build"));
    let listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts_and_runtime_config_toml(
        app_alice_id(),
        store,
        listener,
        runtime_config_toml.as_str(),
    );

    runtime.host().wait_for_direct_peer_route(&bob_member);
}

#[test]
fn runtime_host_can_publish_static_peer_routes_manually_in_tests() {
    let remote_endpoint_lease = reserve_sockets(&[ReservedSocketKind::UdpSocket]);
    let remote_addr = remote_endpoint_lease.addr(0);
    let alice_member = alice_member();
    let bob_member = bob_member();
    let runtime_config_toml = static_peer_route_toml(&bob_member, remote_addr);
    let store = Arc::new(
        SqliteReplicationStore::in_memory(alice_member.clone()).expect("store should build"),
    );
    let listener = Arc::new(ListenerStub::default());
    let mut host = DeliveryRuntimeHost::start_with_route_publish_mode_for_test(
        &alice_member,
        store,
        listener,
        Some(runtime_config_toml.as_str()),
        PreconfiguredPeerRoutesPublishMode::ManualForTest,
    )
    .expect("host should start");
    host.wait_for_runtime_startup();

    host.publish_preconfigured_peer_routes();
    host.wait_for_direct_peer_route(&bob_member);
    host.shutdown();
}

#[test]
fn runtime_host_treats_zero_catch_up_batch_size_as_unlimited() {
    let alice_member = alice_member();
    let store = Arc::new(
        SqliteReplicationStore::in_memory(alice_member.clone()).expect("store should build"),
    );
    let listener = Arc::new(ListenerStub::default());
    let mut host = DeliveryRuntimeHost::start_with_route_publish_mode_for_test(
        &alice_member,
        store,
        listener,
        Some(
            r"
            [flotsync.replication.runtime.catch-up]
            max-updates-per-batch = 0
            ",
        ),
        PreconfiguredPeerRoutesPublishMode::ManualForTest,
    )
    .expect("zero catch-up batch size should mean unlimited");
    host.wait_for_runtime_startup();
    host.shutdown();
}

#[test]
fn runtime_startup_hydrates_persisted_group_memberships_from_store() {
    let alice_member = alice_member();
    let dataset_id = docs_dataset_id();
    let store = sqlite_store_with_schemas(
        alice_member.clone(),
        [(dataset_id.clone(), title_schema_static())],
    );
    let group_id = GroupId(Uuid::from_u128(31));
    let members = GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member()])
        .expect("group should build");
    persist_group_in_store(
        store.as_ref(),
        ReplicationGroupRecord {
            group_id,
            members: members.ordered_members(),
            local_member_index: MemberIndex::new(0),
            version_vector: VersionVector::initial(
                NonZeroUsize::new(2).expect("group should have two members"),
            ),
        },
    );
    let listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts(app_alice_id(), store, listener);
    let row_id = test_row_id(group_id, dataset_id, 32);

    wait_for_group_install(&runtime, group_id);
    let read_token = snapshot_read_token(runtime.as_ref(), group_id, docs_dataset_id());
    publish_changes(
        runtime.as_ref(),
        read_token,
        vec![RowMutation::Upsert {
            row_id,
            row: crate::row_values! {
                "title" => "hydrated on startup",
            },
        }],
    );
}

#[test]
fn create_group_persists_membership_across_runtime_restart() {
    let alice_member = alice_member();
    let dataset_id = docs_dataset_id();
    let store = sqlite_store_with_schemas(
        alice_member.clone(),
        [(dataset_id.clone(), title_schema_static())],
    );
    let first_listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts(app_alice_id(), store.clone(), first_listener);
    let group_id = wait_for_test_reply(runtime.create_group(CreateGroupRequest {
        members: vec![alice_member.clone()],
        initial_state: None,
    }))
    .expect("create_group should succeed");
    drop(runtime);

    let restarted_listener = Arc::new(ListenerStub::default());
    let restarted_runtime = load_runtime_with_parts(app_alice_id(), store, restarted_listener);
    let row_id = test_row_id(group_id, dataset_id, 33);

    wait_for_group_install(&restarted_runtime, group_id);
    let read_token = snapshot_read_token(restarted_runtime.as_ref(), group_id, docs_dataset_id());
    publish_changes(
        restarted_runtime.as_ref(),
        read_token,
        vec![RowMutation::Upsert {
            row_id,
            row: crate::row_values! {
                "title" => "after restart",
            },
        }],
    );
}

#[test]
fn publish_changes_persists_applied_update_and_snapshot_state() {
    let alice_member = alice_member();
    let dataset_id = docs_dataset_id();
    let fixture = load_runtime_fixture(
        app_alice_id(),
        alice_member.clone(),
        [(dataset_id.clone(), title_schema_shared())],
    );
    let group_id = wait_for_test_reply(fixture.runtime.create_group(CreateGroupRequest {
        members: vec![alice_member],
        initial_state: None,
    }))
    .expect("create_group should succeed");
    let row_id = test_row_id(group_id, dataset_id.clone(), 34);

    let read_token = snapshot_read_token(fixture.runtime.as_ref(), group_id, dataset_id.clone());
    let receipt = publish_changes(
        fixture.runtime.as_ref(),
        read_token,
        vec![RowMutation::Upsert {
            row_id: row_id.clone(),
            row: crate::row_values! {
                "title" => "durable publish",
            },
        }],
    );

    let persisted_group = load_persisted_group(fixture.store.as_ref(), group_id);
    assert_eq!(persisted_group.version_vector.version_at(0), 1);
    let persisted_update =
        load_persisted_update(fixture.store.as_ref(), group_id, receipt.update_id)
            .expect("published update should persist");
    assert!(persisted_update.applied_locally);
    let row_slice = load_persisted_row_slice(
        fixture.store.as_ref(),
        group_id,
        &dataset_id,
        [row_id.row_key],
    );
    assert!(row_slice.dataset_exists);
    assert!(
        row_slice
            .rows
            .get(&row_id.row_key)
            .cloned()
            .flatten()
            .is_some()
    );
}

#[test]
fn publish_changes_linear_string_update_with_two_insert_hunks_reuses_operation_id() {
    let alice_member = alice_member();
    let dataset_id = docs_dataset_id();
    let fixture = load_runtime_fixture(
        app_alice_id(),
        alice_member.clone(),
        [(dataset_id.clone(), title_schema_shared())],
    );
    let group_id = wait_for_test_reply(fixture.runtime.create_group(CreateGroupRequest {
        members: vec![alice_member],
        initial_state: None,
    }))
    .expect("create_group should succeed");
    let row_id = test_row_id(group_id, dataset_id.clone(), 121_000);

    let read_token = snapshot_read_token(fixture.runtime.as_ref(), group_id, dataset_id.clone());
    let insert_receipt = publish_changes(
        fixture.runtime.as_ref(),
        read_token,
        vec![RowMutation::Upsert {
            row_id: row_id.clone(),
            row: crate::row_values! {
                "title" => "a",
            },
        }],
    );

    let update_receipt = publish_changes(
        fixture.runtime.as_ref(),
        insert_receipt.read_token,
        vec![RowMutation::Upsert {
            row_id: row_id.clone(),
            row: crate::row_values! {
                "title" => "xay",
            },
        }],
    );

    assert_eq!(update_receipt.update_id.version, 2);
    assert_eq!(
        snapshot_string_field(
            fixture.runtime.as_ref(),
            group_id,
            dataset_id,
            &row_id,
            "title",
        ),
        "xay"
    );
}

#[test]
fn request_summary_reports_local_versions() {
    let alice_member = alice_member();
    let dataset_id = docs_dataset_id();
    let fixture = load_runtime_fixture(
        app_alice_id(),
        alice_member.clone(),
        [(dataset_id.clone(), title_schema_shared())],
    );
    let group_id = wait_for_test_reply(fixture.runtime.create_group(CreateGroupRequest {
        members: vec![alice_member.clone()],
        initial_state: None,
    }))
    .expect("create_group should succeed");
    let read_token = snapshot_read_token(fixture.runtime.as_ref(), group_id, dataset_id.clone());
    let receipt = publish_changes(
        fixture.runtime.as_ref(),
        read_token,
        vec![RowMutation::Upsert {
            row_id: test_row_id(group_id, dataset_id, 120_001),
            row: crate::row_values! {
                "title" => "local summary",
            },
        }],
    );

    let summary = wait_for_test_reply(fixture.runtime.request_summary(SummaryRequest {
        group_id,
        target: alice_member.clone(),
    }))
    .expect("summary request should succeed");

    assert_eq!(summary.responder, alice_member);
    assert_eq!(
        summary.has_versions,
        VersionVector::initial(NonZeroUsize::new(1).expect("one member"))
            .with_update_applied(receipt.update_id)
    );
}

#[test]
fn snapshot_rows_streams_visible_rows_and_optional_tombstones() {
    let alice_member = alice_member();
    let dataset_id = docs_dataset_id();
    let fixture = load_runtime_fixture(
        app_alice_id(),
        alice_member.clone(),
        [(dataset_id.clone(), title_schema_shared())],
    );
    let group_id = wait_for_test_reply(fixture.runtime.create_group(CreateGroupRequest {
        members: vec![alice_member],
        initial_state: None,
    }))
    .expect("create_group should succeed");
    let active_row_id = test_row_id(group_id, dataset_id.clone(), 35);
    let deleted_row_id = test_row_id(group_id, dataset_id.clone(), 36);

    let read_token = snapshot_read_token(fixture.runtime.as_ref(), group_id, dataset_id.clone());
    let receipt = publish_changes(
        fixture.runtime.as_ref(),
        read_token,
        vec![
            RowMutation::Upsert {
                row_id: active_row_id.clone(),
                row: crate::row_values! {
                    "title" => "still visible",
                },
            },
            RowMutation::Upsert {
                row_id: deleted_row_id.clone(),
                row: crate::row_values! {
                    "title" => "now deleted",
                },
            },
        ],
    );
    publish_changes(
        fixture.runtime.as_ref(),
        receipt.read_token,
        vec![RowMutation::Delete {
            row_id: deleted_row_id.clone(),
        }],
    );

    let mut visible_rows = drain_snapshot_rows(
        fixture.runtime.as_ref(),
        SnapshotRowsRequest {
            group_id,
            datasets: HashSet::from([dataset_id.clone()]),
            max_rows_per_batch: NonZeroUsize::new(1).expect("batch size should be non-zero"),
            include_tombstones: false,
        },
    );
    sort_captured_rows(&mut visible_rows);

    assert_eq!(
        visible_rows,
        vec![CapturedRowChange::Upsert {
            row_id: active_row_id.clone(),
            title: "still visible".to_owned(),
        }]
    );

    let mut all_rows = drain_snapshot_rows(
        fixture.runtime.as_ref(),
        SnapshotRowsRequest {
            group_id,
            datasets: HashSet::from([dataset_id]),
            max_rows_per_batch: NonZeroUsize::new(1).expect("batch size should be non-zero"),
            include_tombstones: true,
        },
    );
    sort_captured_rows(&mut all_rows);

    assert_eq!(
        all_rows,
        vec![
            CapturedRowChange::Upsert {
                row_id: active_row_id,
                title: "still visible".to_owned(),
            },
            CapturedRowChange::Delete {
                row_id: deleted_row_id,
            },
        ]
    );
}

#[test]
fn publish_changes_emits_local_data_changed_event_before_reply() {
    let alice_member = alice_member();
    let dataset_id = docs_dataset_id();
    let fixture = load_runtime_fixture(
        app_alice_id(),
        alice_member.clone(),
        [(dataset_id.clone(), title_schema_shared())],
    );
    let group_id = wait_for_test_reply(fixture.runtime.create_group(CreateGroupRequest {
        members: vec![alice_member],
        initial_state: None,
    }))
    .expect("create_group should succeed");
    let row_id = test_row_id(group_id, dataset_id, 39);

    let read_token = snapshot_read_token(fixture.runtime.as_ref(), group_id, docs_dataset_id());
    publish_changes(
        fixture.runtime.as_ref(),
        read_token,
        vec![RowMutation::Upsert {
            row_id: row_id.clone(),
            row: crate::row_values! {
                "title" => "local event",
            },
        }],
    );

    assert_eq!(
        fixture.listener.captured_data_changes(),
        vec![CapturedDataChange {
            rows: vec![CapturedRowChange::Upsert {
                row_id,
                title: "local event".to_owned(),
            }],
        }]
    );
}

#[test]
fn rebased_local_upsert_applies_only_staged_field_to_current_dataset() {
    let group_id = GroupId(Uuid::from_u128(710));
    let dataset_id = docs_dataset_id();
    let row_id = test_row_id(group_id, dataset_id, 42);
    let mut base_dataset = LocalDataset::new(title_note_schema_shared());
    let mut current_dataset = LocalDataset::new(title_note_schema_shared());

    let insert_update_id = UpdateId {
        version: 1,
        node_index: 0,
    };
    for dataset in [&mut base_dataset, &mut current_dataset] {
        apply_local_upsert(
            dataset,
            &row_id,
            crate::row_values! {
                "title" => "original title",
                "note" => "original note",
            },
            insert_update_id,
        )
        .expect("insert should apply")
        .expect("insert should produce an operation");
    }
    apply_local_upsert(
        &mut current_dataset,
        &row_id,
        crate::row_values! {
            "note" => "newer note",
        },
        UpdateId {
            version: 2,
            node_index: 0,
        },
    )
    .expect("note update should apply")
    .expect("note update should produce an operation");

    apply_rebased_local_upsert(
        &mut base_dataset,
        &mut current_dataset,
        &row_id,
        crate::row_values! {
            "title" => "original title updated",
        },
        UpdateId {
            version: 3,
            node_index: 0,
        },
    )
    .expect("rebased title update should apply")
    .expect("rebased title update should produce an operation");

    let row = current_dataset
        .data
        .get_row(&row_id.row_key.0)
        .expect("row should exist");
    assert_eq!(
        row.get_field_value::<str>("title")
            .expect("title should decode")
            .as_ref(),
        "original title updated"
    );
    assert_eq!(
        row.get_field_value::<str>("note")
            .expect("note should decode")
            .as_ref(),
        "newer note"
    );
}

#[test]
fn publish_changes_rebases_stale_field_patch_without_overwriting_newer_fields() {
    let alice_member = alice_member();
    let dataset_id = docs_dataset_id();
    let fixture = load_runtime_fixture(
        app_alice_id(),
        alice_member.clone(),
        [(dataset_id.clone(), title_note_schema_shared())],
    );
    let group_id = wait_for_test_reply(fixture.runtime.create_group(CreateGroupRequest {
        members: vec![alice_member],
        initial_state: None,
    }))
    .expect("create_group should succeed");
    let row_id = test_row_id(group_id, dataset_id.clone(), 41);

    let initial_token = snapshot_read_token(fixture.runtime.as_ref(), group_id, dataset_id.clone());
    let insert_receipt = publish_changes(
        fixture.runtime.as_ref(),
        initial_token,
        vec![RowMutation::Upsert {
            row_id: row_id.clone(),
            row: crate::row_values! {
                "title" => "original title",
                "note" => "original note",
            },
        }],
    );
    let stale_edit_token = insert_receipt.read_token.clone();

    let note_receipt = publish_changes(
        fixture.runtime.as_ref(),
        insert_receipt.read_token,
        vec![RowMutation::Upsert {
            row_id: row_id.clone(),
            row: crate::row_values! {
                "note" => "newer note",
            },
        }],
    );
    assert_eq!(note_receipt.update_id.version, 2);

    let title_receipt = publish_changes(
        fixture.runtime.as_ref(),
        stale_edit_token,
        vec![RowMutation::Upsert {
            row_id: row_id.clone(),
            row: crate::row_values! {
                "title" => "original title updated",
            },
        }],
    );
    assert_eq!(title_receipt.update_id.version, 3);

    assert_eq!(
        snapshot_string_field(
            fixture.runtime.as_ref(),
            group_id,
            dataset_id.clone(),
            &row_id,
            "title",
        ),
        "original title updated"
    );
    assert_eq!(
        snapshot_string_field(
            fixture.runtime.as_ref(),
            group_id,
            dataset_id,
            &row_id,
            "note"
        ),
        "newer note"
    );
}

#[test]
fn publish_changes_error_display_includes_local_operation_source() {
    let alice_member = alice_member();
    let dataset_id = docs_dataset_id();
    let fixture = load_runtime_fixture(
        app_alice_id(),
        alice_member.clone(),
        [(
            dataset_id.clone(),
            Arc::new(Schema::from_fields([
                Field::linear_string("title"),
                Field::monotonic_counter("edit_count"),
            ])),
        )],
    );
    let group_id = wait_for_test_reply(fixture.runtime.create_group(CreateGroupRequest {
        members: vec![alice_member],
        initial_state: None,
    }))
    .expect("create_group should succeed");
    let row_id = test_row_id(group_id, dataset_id, 40);

    let read_token = snapshot_read_token(fixture.runtime.as_ref(), group_id, docs_dataset_id());
    let receipt = publish_changes(
        fixture.runtime.as_ref(),
        read_token,
        vec![RowMutation::Upsert {
            row_id: row_id.clone(),
            row: crate::row_values! {
                "title" => "counted row",
                "edit_count" => 5_u64,
            },
        }],
    );

    let error = wait_for_test_reply(fixture.runtime.publish_changes(PublishChangesRequest {
        read_token: receipt.read_token,
        changes: vec![RowMutation::Upsert {
            row_id,
            row: crate::row_values! {
                "edit_count" => 4_u64,
            },
        }],
    }))
    .expect_err("counter decrease should fail publish");
    let message = error.to_string();

    assert!(
        message.contains("monotonic and cannot decrease from 5 to 4"),
        "{message}"
    );
}

#[test]
fn publish_changes_rejects_reserved_local_update_version() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let group_id = GroupId(Uuid::from_u128(40_101));
    let member_count = NonZeroUsize::new(2).expect("group has two members");
    let version_vector = VersionVector::initial(member_count).with_update_applied(UpdateId {
        node_index: 0,
        version: MAX_VERSION_VALUE,
    });
    let store = sqlite_store_with_schemas(
        alice_member.clone(),
        [(dataset_id.clone(), title_schema_static())],
    );
    persist_group_in_store(
        store.as_ref(),
        ReplicationGroupRecord {
            group_id,
            members: vec![alice_member.clone(), bob_member],
            local_member_index: MemberIndex::new(0),
            version_vector: version_vector.clone(),
        },
    );
    let listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts(app_alice_id(), store.clone(), listener.clone());
    let row_id = test_row_id(group_id, dataset_id, 40_102);
    let read_token = ReadToken::from_group_versions(HashMap::from([(group_id, version_vector)]));

    let error = wait_for_test_reply(runtime.publish_changes(PublishChangesRequest {
        read_token,
        changes: vec![RowMutation::Upsert {
            row_id,
            row: crate::row_values! {
                "title" => "too late",
            },
        }],
    }))
    .expect_err("reserved local update version should fail publish");

    match error {
        ApiError::ApiExternal { source } => match source.downcast_ref::<PublishChangesError>() {
            Some(PublishChangesError::ExhaustedUpdateIds {
                group_id: exhausted_group_id,
            }) => assert_eq!(*exhausted_group_id, group_id),
            other => panic!("unexpected publish error source: {other:?}"),
        },
        error => panic!("unexpected API error: {error:?}"),
    }
    assert_eq!(
        load_persisted_group(store.as_ref(), group_id).version_vector,
        VersionVector::initial(member_count).with_update_applied(UpdateId {
            node_index: 0,
            version: MAX_VERSION_VALUE,
        })
    );
    assert_eq!(
        load_persisted_update(
            store.as_ref(),
            group_id,
            UpdateId {
                node_index: 0,
                version: u64::MAX,
            },
        ),
        None
    );
    assert!(listener.captured_data_changes().is_empty());
}

#[test]
fn create_group_bootstrap_installs_remote_membership() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let alice_runtime = load_runtime(app_alice_id(), alice_member.clone());
    let bob_runtime = load_runtime(app_bob_id(), bob_member.clone());

    assert!(
        alice_runtime
            .host()
            .external_udp_bind_addr()
            .ip()
            .is_loopback()
    );
    assert!(
        bob_runtime
            .host()
            .external_udp_bind_addr()
            .ip()
            .is_loopback()
    );

    alice_runtime.host().publish_direct_peer_route(
        bob_member.clone(),
        bob_runtime.host().advertised_loopback_udp_addr(),
    );
    bob_runtime.host().publish_direct_peer_route(
        alice_member.clone(),
        alice_runtime.host().advertised_loopback_udp_addr(),
    );

    let group_id = wait_for_test_reply(alice_runtime.create_group(CreateGroupRequest {
        members: vec![alice_member.clone(), bob_member.clone()],
        initial_state: None,
    }))
    .expect("create_group should succeed");
    wait_for_group_install(&bob_runtime, group_id);

    let alice_snapshot = alice_runtime.host().membership_snapshot();
    let bob_snapshot = bob_runtime.host().membership_snapshot();
    let alice_members = alice_snapshot
        .members(&group_id)
        .expect("local runtime should host the created group");
    let bob_members = bob_snapshot
        .members(&group_id)
        .expect("remote runtime should install the bootstrap group");

    assert_eq!(
        alice_members.member_index(&alice_member),
        Some(MemberIndex::new(0))
    );
    assert_eq!(
        alice_members.member_index(&bob_member),
        Some(MemberIndex::new(1))
    );
    assert_eq!(
        bob_members.member_index(&alice_member),
        Some(MemberIndex::new(0))
    );
    assert_eq!(
        bob_members.member_index(&bob_member),
        Some(MemberIndex::new(1))
    );
}

#[test]
fn publish_changes_delivers_remote_data_changed_event() {
    // End-to-end happy path:
    // 1. start two runtimes with the same dataset schema,
    // 2. connect them with direct peer routes,
    // 3. create one fixed-membership group from Alice,
    // 4. publish one upsert from Alice, and
    // 5. assert that Bob observes the replicated row change.
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let alice_fixture = load_runtime_fixture(
        app_alice_id(),
        alice_member.clone(),
        [(dataset_id.clone(), title_schema_shared())],
    );
    let bob_fixture = load_runtime_fixture(
        app_bob_id(),
        bob_member.clone(),
        [(dataset_id.clone(), title_schema_static())],
    );
    let alice_runtime = &alice_fixture.runtime;
    let bob_runtime = &bob_fixture.runtime;

    alice_runtime.host().publish_direct_peer_route(
        bob_member.clone(),
        bob_runtime.host().advertised_loopback_udp_addr(),
    );
    bob_runtime.host().publish_direct_peer_route(
        alice_member.clone(),
        alice_runtime.host().advertised_loopback_udp_addr(),
    );

    let group_id = wait_for_test_reply(alice_runtime.create_group(CreateGroupRequest {
        members: vec![alice_member.clone(), bob_member.clone()],
        initial_state: None,
    }))
    .expect("create_group should succeed");
    wait_for_group_install(bob_runtime, group_id);
    let row_id = test_row_id(group_id, dataset_id.clone(), 11);

    let read_token = snapshot_read_token(alice_runtime.as_ref(), group_id, dataset_id.clone());
    let receipt = publish_changes(
        alice_runtime.as_ref(),
        read_token,
        vec![RowMutation::Upsert {
            row_id: row_id.clone(),
            row: crate::row_values! {
                "title" => "hello from alice",
            },
        }],
    );

    assert_eq!(
        receipt.update_id,
        UpdateId {
            version: 1,
            node_index: 0,
        }
    );

    let delivered = bob_fixture.listener.wait_for_next_data_change();
    assert_eq!(
        delivered,
        CapturedDataChange {
            rows: vec![CapturedRowChange::Upsert {
                row_id: row_id.clone(),
                title: "hello from alice".to_owned(),
            }],
        }
    );

    assert!(
        bob_runtime
            .host()
            .membership_snapshot()
            .contains_group(&group_id),
        "remote runtime should still host the replicated group"
    );
}

#[test]
fn update_gap_triggers_need_range_and_update_batch_catch_up() {
    let _runtime_endpoint_leases =
        reserve_sockets(&[ReservedSocketKind::UdpSocket, ReservedSocketKind::UdpSocket]);
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let alice_fixture = load_runtime_fixture(
        app_alice_id(),
        alice_member.clone(),
        [(dataset_id.clone(), title_schema_shared())],
    );
    let bob_fixture = load_runtime_fixture(
        app_bob_id(),
        bob_member.clone(),
        [(dataset_id.clone(), title_schema_static())],
    );
    let alice_runtime = &alice_fixture.runtime;
    let bob_runtime = &bob_fixture.runtime;
    let group_id = GroupId(Uuid::from_u128(50_001));
    let members =
        GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member.clone()])
            .expect("group members should build");
    alice_runtime
        .install_group_for_test(group_id, members.clone())
        .expect("alice group should install");
    bob_runtime
        .install_group_for_test(group_id, members)
        .expect("bob group should install");

    let first_row_id = test_row_id(group_id, dataset_id.clone(), 50_011);
    let second_row_id = test_row_id(group_id, dataset_id.clone(), 50_012);
    let first_read_token =
        snapshot_read_token(alice_runtime.as_ref(), group_id, dataset_id.clone());
    let first_receipt = publish_changes(
        alice_runtime.as_ref(),
        first_read_token,
        vec![RowMutation::Upsert {
            row_id: first_row_id.clone(),
            row: crate::row_values! {
                "title" => "missed first",
            },
        }],
    );

    alice_runtime.host().publish_direct_peer_route(
        bob_member.clone(),
        bob_runtime.host().advertised_loopback_udp_addr(),
    );
    bob_runtime.host().publish_direct_peer_route(
        alice_member,
        alice_runtime.host().advertised_loopback_udp_addr(),
    );

    let second_receipt = publish_changes(
        alice_runtime.as_ref(),
        first_receipt.read_token,
        vec![RowMutation::Upsert {
            row_id: second_row_id.clone(),
            row: crate::row_values! {
                "title" => "live second",
            },
        }],
    );

    assert_eq!(
        second_receipt.update_id,
        UpdateId {
            node_index: 0,
            version: 2,
        }
    );
    bob_fixture.listener.wait_for_data_change_count(2);
    assert_eq!(
        bob_fixture.listener.captured_data_changes(),
        vec![
            CapturedDataChange {
                rows: vec![CapturedRowChange::Upsert {
                    row_id: first_row_id,
                    title: "missed first".to_owned(),
                }],
            },
            CapturedDataChange {
                rows: vec![CapturedRowChange::Upsert {
                    row_id: second_row_id,
                    title: "live second".to_owned(),
                }],
            },
        ]
    );
}

#[test]
fn request_summary_returns_remote_current_version_vector() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let alice_fixture = load_runtime_fixture(
        app_alice_id(),
        alice_member.clone(),
        [(dataset_id.clone(), title_schema_shared())],
    );
    let bob_fixture = load_runtime_fixture(
        app_bob_id(),
        bob_member.clone(),
        [(dataset_id.clone(), title_schema_static())],
    );
    let alice_runtime = &alice_fixture.runtime;
    let bob_runtime = &bob_fixture.runtime;

    alice_runtime.host().publish_direct_peer_route(
        bob_member.clone(),
        bob_runtime.host().advertised_loopback_udp_addr(),
    );
    bob_runtime.host().publish_direct_peer_route(
        alice_member.clone(),
        alice_runtime.host().advertised_loopback_udp_addr(),
    );

    let group_id = wait_for_test_reply(alice_runtime.create_group(CreateGroupRequest {
        members: vec![alice_member, bob_member.clone()],
        initial_state: None,
    }))
    .expect("create_group should succeed");
    wait_for_group_install(bob_runtime, group_id);

    let summary = wait_for_test_reply(alice_runtime.request_summary(SummaryRequest {
        group_id,
        target: bob_member.clone(),
    }))
    .expect("summary request should succeed");

    assert_eq!(summary.group_id, group_id);
    assert_eq!(summary.responder, bob_member);
    assert_eq!(
        summary.has_versions,
        VersionVector::initial(NonZeroUsize::new(2).expect("two members"))
    );
}

#[test]
fn inbound_updates_buffer_until_causal_dependencies_are_met_and_ignore_duplicates() {
    // Causal buffering path:
    // 1. install a two-member group only on Bob,
    // 2. deliver Alice's second update first so it must buffer,
    // 3. deliver the missing first update,
    // 4. assert that Bob drains the buffered update in causal order, and
    // 5. verify a duplicate of the first update is ignored.
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let schema = title_schema_static();
    let bob_fixture = load_runtime_fixture(
        app_bob_id(),
        bob_member.clone(),
        [(dataset_id.clone(), title_schema_static())],
    );
    let bob_runtime = &bob_fixture.runtime;
    let group_id = GroupId(Uuid::from_u128(22));
    bob_runtime
        .install_group_for_test(
            group_id,
            GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member.clone()])
                .expect("group should build"),
        )
        .expect("group should install");

    let row_id = test_row_id(group_id, dataset_id.clone(), 23);
    let mut source_dataset = LocalDataset::new(schema);
    let first_operation = apply_local_upsert(
        &mut source_dataset,
        &row_id,
        crate::row_values! { "title" => "first" },
        UpdateId {
            version: 1,
            node_index: 0,
        },
    )
    .expect("first operation should build")
    .expect("first operation should apply")
    .encoded_operation;
    let second_operation = apply_local_upsert(
        &mut source_dataset,
        &row_id,
        crate::row_values! { "title" => "second" },
        UpdateId {
            version: 2,
            node_index: 0,
        },
    )
    .expect("second operation should build")
    .expect("second operation should apply")
    .encoded_operation;

    let member_count = NonZeroUsize::new(2).expect("group has two members");
    let first_message = UpdateMessage {
        group_id,
        update_id: UpdateId {
            version: 1,
            node_index: 0,
        },
        read_versions: VersionVector::initial(member_count),
        dataset_updates: vec![DatasetUpdateMessage {
            dataset_id: dataset_id.clone(),
            operations: vec![first_operation],
        }],
    };
    let mut second_read_versions = VersionVector::initial(member_count);
    second_read_versions.increment_at(0);
    let second_message = UpdateMessage {
        group_id,
        update_id: UpdateId {
            version: 2,
            node_index: 0,
        },
        read_versions: second_read_versions,
        dataset_updates: vec![DatasetUpdateMessage {
            dataset_id: dataset_id.clone(),
            operations: vec![second_operation],
        }],
    };

    bob_runtime
        .apply_update_for_test(alice_member.clone(), second_message)
        .expect("out-of-order update should buffer");
    assert!(bob_fixture.listener.captured_data_changes().is_empty());

    bob_runtime
        .apply_update_for_test(alice_member.clone(), first_message.clone())
        .expect("first update should apply and drain the pending second update");
    bob_fixture.listener.wait_for_data_change_count(2);
    assert_eq!(
        bob_fixture.listener.captured_data_changes(),
        vec![
            CapturedDataChange {
                rows: vec![CapturedRowChange::Upsert {
                    row_id: row_id.clone(),
                    title: "first".to_owned(),
                }],
            },
            CapturedDataChange {
                rows: vec![CapturedRowChange::Upsert {
                    row_id: row_id.clone(),
                    title: "second".to_owned(),
                }],
            },
        ]
    );
    bob_runtime
        .apply_update_for_test(alice_member, first_message)
        .expect("duplicate update should be ignored");
    assert_eq!(bob_fixture.listener.captured_data_changes().len(), 2);
}

#[test]
fn duplicate_update_batch_delivery_is_ignored() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let schema = title_schema_static();
    let bob_fixture = load_runtime_fixture(
        app_bob_id(),
        bob_member.clone(),
        [(dataset_id.clone(), title_schema_static())],
    );
    let bob_runtime = &bob_fixture.runtime;
    let group_id = GroupId(Uuid::from_u128(22_001));
    bob_runtime
        .install_group_for_test(
            group_id,
            GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member])
                .expect("group should build"),
        )
        .expect("group should install");

    let row_id = test_row_id(group_id, dataset_id.clone(), 22_002);
    let mut source_dataset = LocalDataset::new(schema);
    let operation = apply_local_upsert(
        &mut source_dataset,
        &row_id,
        crate::row_values! { "title" => "batch first" },
        UpdateId {
            version: 1,
            node_index: 0,
        },
    )
    .expect("operation should build")
    .expect("operation should apply")
    .encoded_operation;

    let update = UpdateMessage {
        group_id,
        update_id: UpdateId {
            version: 1,
            node_index: 0,
        },
        read_versions: VersionVector::initial(NonZeroUsize::new(2).expect("group has two members")),
        dataset_updates: vec![DatasetUpdateMessage {
            dataset_id: dataset_id.clone(),
            operations: vec![operation],
        }],
    };
    let batch = UpdateBatchMessage {
        group_id,
        updates: vec![update],
    };

    bob_runtime
        .apply_update_batch_for_test(alice_member.clone(), batch.clone())
        .expect("first update batch should apply");
    bob_fixture.listener.wait_for_data_change_count(1);
    assert_eq!(
        bob_fixture.listener.captured_data_changes(),
        vec![CapturedDataChange {
            rows: vec![CapturedRowChange::Upsert {
                row_id: row_id.clone(),
                title: "batch first".to_owned(),
            }],
        }]
    );

    bob_runtime
        .apply_update_batch_for_test(alice_member, batch)
        .expect("duplicate update batch should be ignored");
    assert_eq!(bob_fixture.listener.captured_data_changes().len(), 1);
}

#[test]
fn inbound_update_with_out_of_range_producer_index_is_rejected() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let bob_fixture = load_runtime_fixture(
        app_bob_id(),
        bob_member.clone(),
        [(dataset_id.clone(), title_schema_static())],
    );
    let bob_runtime = &bob_fixture.runtime;
    let group_id = GroupId(Uuid::from_u128(22_101));
    bob_runtime
        .install_group_for_test(
            group_id,
            GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member])
                .expect("group should build"),
        )
        .expect("group should install");
    let member_count = NonZeroUsize::new(2).expect("group has two members");
    let (_, update) = title_update_message(
        group_id,
        dataset_id,
        22_102,
        "invalid producer",
        UpdateId {
            version: 1,
            node_index: 2,
        },
        VersionVector::initial(member_count),
    );

    let error = bob_runtime
        .apply_update_for_test(alice_member, update)
        .expect_err("out-of-range producer index should fail cleanly");
    match error {
        InboundDeliveryError::UpdateProducerIndexNotInGroup { producer_index, .. } => {
            assert_eq!(producer_index, MemberIndex::new(2));
        }
        error => panic!("unexpected inbound update error: {error:?}"),
    }
    assert!(bob_fixture.listener.captured_data_changes().is_empty());
}

#[test]
fn update_batch_failure_after_first_update_keeps_first_notifications() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let bob_fixture = load_runtime_fixture(
        app_bob_id(),
        bob_member.clone(),
        [(dataset_id.clone(), title_schema_static())],
    );
    let bob_runtime = &bob_fixture.runtime;
    let group_id = GroupId(Uuid::from_u128(22_201));
    bob_runtime
        .install_group_for_test(
            group_id,
            GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member])
                .expect("group should build"),
        )
        .expect("group should install");
    let member_count = NonZeroUsize::new(2).expect("group has two members");
    let (first_row_id, first_update) = title_update_message(
        group_id,
        dataset_id.clone(),
        22_202,
        "first survives",
        UpdateId {
            version: 1,
            node_index: 0,
        },
        VersionVector::initial(member_count),
    );
    let (_, invalid_update) = title_update_message(
        group_id,
        dataset_id,
        22_203,
        "invalid producer",
        UpdateId {
            version: 1,
            node_index: 2,
        },
        VersionVector::initial(member_count),
    );
    let batch = UpdateBatchMessage {
        group_id,
        updates: vec![first_update, invalid_update],
    };

    let error = bob_runtime
        .apply_update_batch_for_test(alice_member, batch)
        .expect_err("second batch update should fail cleanly");
    match error {
        InboundDeliveryError::UpdateProducerIndexNotInGroup { producer_index, .. } => {
            assert_eq!(producer_index, MemberIndex::new(2));
        }
        error => panic!("unexpected update batch error: {error:?}"),
    }
    assert_eq!(
        bob_fixture.listener.captured_data_changes(),
        vec![CapturedDataChange {
            rows: vec![CapturedRowChange::Upsert {
                row_id: first_row_id,
                title: "first survives".to_owned(),
            }],
        }]
    );
}

#[test]
fn inbound_listener_read_token_preserves_unrelated_hosted_group_progress() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let schema = title_schema_static();
    let bob_fixture = load_runtime_fixture(
        app_bob_id(),
        bob_member.clone(),
        [(dataset_id.clone(), schema)],
    );
    let bob_runtime = &bob_fixture.runtime;
    let inbound_group_id = GroupId(Uuid::from_u128(23_001));
    let unrelated_group_id = GroupId(Uuid::from_u128(23_002));
    let members =
        GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member.clone()])
            .expect("group should build");
    bob_runtime
        .install_group_for_test(inbound_group_id, members.clone())
        .expect("inbound group should install");
    bob_runtime
        .install_group_for_test(unrelated_group_id, members)
        .expect("unrelated group should install");

    let unrelated_row_id = test_row_id(unrelated_group_id, dataset_id.clone(), 23_010);
    let unrelated_read_token =
        snapshot_read_token(bob_runtime.as_ref(), unrelated_group_id, dataset_id.clone());
    publish_changes(
        bob_runtime.as_ref(),
        unrelated_read_token,
        vec![RowMutation::Upsert {
            row_id: unrelated_row_id,
            row: crate::row_values! { "title" => "local unrelated progress" },
        }],
    );
    bob_fixture.listener.wait_for_data_change_count(1);

    let inbound_row_id = test_row_id(inbound_group_id, dataset_id.clone(), 23_011);
    let mut source_dataset = LocalDataset::new(schema);
    let inbound_operation = apply_local_upsert(
        &mut source_dataset,
        &inbound_row_id,
        crate::row_values! { "title" => "remote inbound progress" },
        UpdateId {
            version: 1,
            node_index: 0,
        },
    )
    .expect("inbound operation should build")
    .expect("inbound operation should apply")
    .encoded_operation;
    let member_count = NonZeroUsize::new(2).expect("group has two members");
    bob_runtime
        .apply_update_for_test(
            alice_member,
            UpdateMessage {
                group_id: inbound_group_id,
                update_id: UpdateId {
                    version: 1,
                    node_index: 0,
                },
                read_versions: VersionVector::initial(member_count),
                dataset_updates: vec![DatasetUpdateMessage {
                    dataset_id,
                    operations: vec![inbound_operation],
                }],
            },
        )
        .expect("inbound update should apply");
    bob_fixture.listener.wait_for_data_change_count(2);

    let read_tokens = bob_fixture.listener.captured_data_change_read_tokens();
    let inbound_read_token = read_tokens
        .last()
        .expect("inbound event should have a read token");
    let mut expected_inbound_versions = VersionVector::initial(member_count);
    expected_inbound_versions.increment_at(0);
    let mut expected_unrelated_versions = VersionVector::initial(member_count);
    expected_unrelated_versions.increment_at(1);
    assert_eq!(
        inbound_read_token.group_version(&inbound_group_id),
        Some(&expected_inbound_versions)
    );
    assert_eq!(
        inbound_read_token.group_version(&unrelated_group_id),
        Some(&expected_unrelated_versions)
    );
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "This scenario spells out a full conflict timeline; splitting it would hide the causal setup."
)]
fn inbound_update_after_local_delete_updates_tombstone_without_resurrection() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let schema = title_schema_static();
    let RuntimeFixture {
        runtime: bob_runtime,
        listener: bob_listener,
        store: bob_store,
    } = load_runtime_fixture(
        app_bob_id(),
        bob_member.clone(),
        [(dataset_id.clone(), schema)],
    );
    let group_id = GroupId(Uuid::from_u128(24));
    bob_runtime
        .install_group_for_test(
            group_id,
            GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member])
                .expect("group should build"),
        )
        .expect("group should install");

    let row_id = test_row_id(group_id, dataset_id.clone(), 25);
    let mut source_dataset = LocalDataset::new(schema);
    let first_operation = apply_local_upsert(
        &mut source_dataset,
        &row_id,
        crate::row_values! { "title" => "first" },
        UpdateId {
            version: 1,
            node_index: 0,
        },
    )
    .expect("first operation should build")
    .expect("first operation should apply")
    .encoded_operation;
    let edit_operation = apply_local_upsert(
        &mut source_dataset,
        &row_id,
        crate::row_values! { "title" => "second" },
        UpdateId {
            version: 2,
            node_index: 0,
        },
    )
    .expect("edit operation should build")
    .expect("edit operation should apply")
    .encoded_operation;
    let delete_operation = apply_local_delete(
        &mut source_dataset,
        &row_id,
        UpdateId {
            version: 3,
            node_index: 0,
        },
    )
    .expect("delete operation should build")
    .encoded_operation;

    let member_count = NonZeroUsize::new(2).expect("group has two members");
    bob_runtime
        .apply_update_for_test(
            alice_member.clone(),
            UpdateMessage {
                group_id,
                update_id: UpdateId {
                    version: 1,
                    node_index: 0,
                },
                read_versions: VersionVector::initial(member_count),
                dataset_updates: vec![DatasetUpdateMessage {
                    dataset_id: dataset_id.clone(),
                    operations: vec![first_operation],
                }],
            },
        )
        .expect("first update should apply");
    bob_listener.wait_for_data_change_count(1);
    assert_eq!(
        bob_listener.captured_data_changes(),
        vec![CapturedDataChange {
            rows: vec![CapturedRowChange::Upsert {
                row_id: row_id.clone(),
                title: "first".to_owned(),
            }],
        }]
    );

    let read_token = snapshot_read_token(bob_runtime.as_ref(), group_id, dataset_id.clone());
    publish_changes(
        bob_runtime.as_ref(),
        read_token,
        vec![RowMutation::Delete {
            row_id: row_id.clone(),
        }],
    );
    bob_listener.wait_for_data_change_count(2);
    assert_eq!(
        bob_listener.captured_data_changes()[1],
        CapturedDataChange {
            rows: vec![CapturedRowChange::Delete {
                row_id: row_id.clone(),
            }],
        }
    );
    let deleted_row =
        load_persisted_row_slice(bob_store.as_ref(), group_id, &dataset_id, [row_id.row_key])
            .rows
            .get(&row_id.row_key)
            .cloned()
            .flatten()
            .expect("deleted row should persist as a tombstone");
    assert!(deleted_row.tombstoned);
    drop(bob_runtime);

    let restarted_listener = Arc::new(ListenerStub::default());
    let restarted_runtime =
        load_runtime_with_parts(app_bob_id(), bob_store.clone(), restarted_listener.clone());
    wait_for_group_install(&restarted_runtime, group_id);
    let mut edit_read_versions = VersionVector::initial(member_count);
    edit_read_versions.increment_at(0);
    restarted_runtime
        .apply_update_for_test(
            alice_member.clone(),
            UpdateMessage {
                group_id,
                update_id: UpdateId {
                    version: 2,
                    node_index: 0,
                },
                read_versions: edit_read_versions,
                dataset_updates: vec![DatasetUpdateMessage {
                    dataset_id: dataset_id.clone(),
                    operations: vec![edit_operation],
                }],
            },
        )
        .expect("concurrent edit after local delete should apply to the tombstone");
    assert!(restarted_listener.captured_data_changes().is_empty());

    let edited_tombstone =
        load_persisted_row_slice(bob_store.as_ref(), group_id, &dataset_id, [row_id.row_key])
            .rows
            .get(&row_id.row_key)
            .cloned()
            .flatten()
            .expect("edited tombstone should persist");
    assert!(edited_tombstone.tombstoned);
    let materialised_tombstone =
        flotsync_messages::InMemoryData::from_row_snapshots_with_tombstones(
            schema,
            [flotsync_data_types::schema::datamodel::RowRecord {
                row_id: row_id.row_key.0,
                snapshot: edited_tombstone.snapshot,
                tombstoned: edited_tombstone.tombstoned,
            }],
        )
        .expect("tombstone should rehydrate");
    assert_eq!(materialised_tombstone.num_active_rows(), 0);
    let materialised_row = materialised_tombstone
        .get_row(&row_id.row_key.0)
        .expect("tombstoned row should remain addressable");
    let title = materialised_row
        .get_field_value::<str>("title")
        .expect("title should decode");
    assert_eq!(title.as_ref(), "second");

    let mut delete_read_versions = VersionVector::initial(member_count);
    delete_read_versions.increment_at(0);
    delete_read_versions.increment_at(0);
    restarted_runtime
        .apply_update_for_test(
            alice_member,
            UpdateMessage {
                group_id,
                update_id: UpdateId {
                    version: 3,
                    node_index: 0,
                },
                read_versions: delete_read_versions,
                dataset_updates: vec![DatasetUpdateMessage {
                    dataset_id,
                    operations: vec![delete_operation],
                }],
            },
        )
        .expect("delete against an existing tombstone should be idempotent");
    assert!(restarted_listener.captured_data_changes().is_empty());
}

#[test]
fn inbound_update_rejects_operation_change_id_mismatch_before_persisting() {
    let dataset_id = docs_dataset_id();
    let schema = title_schema_static();
    let group_id = GroupId(Uuid::from_u128(26));
    let row_id = test_row_id(group_id, dataset_id.clone(), 27);
    let batch_update_id = UpdateId {
        version: 1,
        node_index: 0,
    };
    let operation_change_id = UpdateId {
        version: 2,
        node_index: 0,
    };
    let mut source_dataset = LocalDataset::new(schema);
    let encoded_operation = apply_local_upsert(
        &mut source_dataset,
        &row_id,
        crate::row_values! { "title" => "wrong change id" },
        operation_change_id,
    )
    .expect("operation should build")
    .expect("operation should apply")
    .encoded_operation;
    let member_count = NonZeroUsize::new(2).expect("group has two members");
    let update = ReplicationUpdateRecord {
        group_id,
        update_id: batch_update_id,
        sender: alice_member(),
        read_versions: VersionVector::initial(member_count),
        dataset_updates: vec![DatasetUpdateRecord {
            dataset_id: dataset_id.clone(),
            operations: vec![encoded_operation],
        }],
        applied_locally: false,
    };
    let schemas = std::collections::HashMap::from([(
        dataset_id.clone(),
        SchemaSource::from(title_schema_static()),
    )]);

    let error =
        validate_update_mapping(&update, &schemas).expect_err("change-id mismatch should reject");

    match error {
        InboundDeliveryError::UpdateOperationIdMismatch {
            group: actual_group_id,
            update: update_id,
            dataset: actual_dataset_id,
            operation_change: actual_operation_change_id,
        } => {
            assert_eq!(actual_group_id, group_id);
            assert_eq!(update_id, batch_update_id);
            assert_eq!(actual_dataset_id, dataset_id);
            assert_eq!(actual_operation_change_id, operation_change_id);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn inbound_update_rejects_self_dependent_read_versions_before_persisting() {
    let dataset_id = docs_dataset_id();
    let group_id = GroupId(Uuid::from_u128(28));
    let update_id = UpdateId {
        version: 1,
        node_index: 0,
    };
    let member_count = NonZeroUsize::new(2).expect("group has two members");
    let mut read_versions = VersionVector::initial(member_count);
    read_versions.increment_at(0);
    let update = ReplicationUpdateRecord {
        group_id,
        update_id,
        sender: alice_member(),
        read_versions,
        dataset_updates: vec![DatasetUpdateRecord {
            dataset_id,
            operations: Vec::new(),
        }],
        applied_locally: false,
    };

    let error = validate_inbound_update_read_versions(&update)
        .expect_err("self-dependent read versions should be rejected");

    match error {
        InboundDeliveryError::SelfDependentReadVersions {
            group_id: actual_group_id,
            update_id: actual_update_id,
            producer_read_version,
        } => {
            assert_eq!(actual_group_id, group_id);
            assert_eq!(actual_update_id, update_id);
            assert_eq!(producer_read_version, 1);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "The restart scenario keeps pre- and post-restart assertions together for readability."
)]
fn buffered_updates_survive_runtime_restart_and_drain_from_store() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let schema = title_schema_static();
    let store = sqlite_store_with_schemas(bob_member.clone(), [(dataset_id.clone(), schema)]);
    let first_listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts(app_bob_id(), store.clone(), first_listener);
    let group_id = GroupId(Uuid::from_u128(35));
    runtime
        .install_group_for_test(
            group_id,
            GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member.clone()])
                .expect("group should build"),
        )
        .expect("group should install");

    let row_id = test_row_id(group_id, dataset_id.clone(), 36);
    let mut source_dataset = LocalDataset::new(schema);
    let first_operation = apply_local_upsert(
        &mut source_dataset,
        &row_id,
        crate::row_values! { "title" => "first" },
        UpdateId {
            version: 1,
            node_index: 0,
        },
    )
    .expect("first operation should build")
    .expect("first operation should apply")
    .encoded_operation;
    let second_operation = apply_local_upsert(
        &mut source_dataset,
        &row_id,
        crate::row_values! { "title" => "second" },
        UpdateId {
            version: 2,
            node_index: 0,
        },
    )
    .expect("second operation should build")
    .expect("second operation should apply")
    .encoded_operation;
    let member_count = NonZeroUsize::new(2).expect("group has two members");
    let first_message = UpdateMessage {
        group_id,
        update_id: UpdateId {
            version: 1,
            node_index: 0,
        },
        read_versions: VersionVector::initial(member_count),
        dataset_updates: vec![DatasetUpdateMessage {
            dataset_id: dataset_id.clone(),
            operations: vec![first_operation],
        }],
    };
    let mut second_read_versions = VersionVector::initial(member_count);
    second_read_versions.increment_at(0);
    let second_message = UpdateMessage {
        group_id,
        update_id: UpdateId {
            version: 2,
            node_index: 0,
        },
        read_versions: second_read_versions,
        dataset_updates: vec![DatasetUpdateMessage {
            dataset_id: dataset_id.clone(),
            operations: vec![second_operation],
        }],
    };

    runtime
        .apply_update_for_test(alice_member.clone(), second_message)
        .expect("out-of-order update should persist pending state");
    drop(runtime);

    let restarted_listener = Arc::new(ListenerStub::default());
    let restarted_runtime =
        load_runtime_with_parts(app_bob_id(), store.clone(), restarted_listener.clone());
    restarted_runtime
        .apply_update_for_test(alice_member, first_message)
        .expect("missing predecessor should apply and drain the persisted successor");
    restarted_listener.wait_for_data_change_count(2);
    assert_eq!(
        restarted_listener.captured_data_changes(),
        vec![
            CapturedDataChange {
                rows: vec![CapturedRowChange::Upsert {
                    row_id: row_id.clone(),
                    title: "first".to_owned(),
                }],
            },
            CapturedDataChange {
                rows: vec![CapturedRowChange::Upsert {
                    row_id,
                    title: "second".to_owned(),
                }],
            },
        ]
    );

    assert!(
        load_persisted_update(
            store.as_ref(),
            group_id,
            UpdateId {
                version: 1,
                node_index: 0,
            },
        )
        .expect("first update should persist")
        .applied_locally
    );
    assert!(
        load_persisted_update(
            store.as_ref(),
            group_id,
            UpdateId {
                version: 2,
                node_index: 0,
            },
        )
        .expect("second update should persist")
        .applied_locally
    );
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "The rollback scenario needs the complete ready-update chain inline to show the failing write boundary."
)]
fn causally_ready_apply_chain_rolls_back_when_store_write_fails() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let schema = title_schema_static();
    let sqlite_store =
        sqlite_store_with_schemas(bob_member.clone(), [(dataset_id.clone(), schema)]);
    let store = Arc::new(FailingStore::new(sqlite_store.clone()));
    let listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts(app_bob_id(), store.clone(), listener.clone());
    let group_id = GroupId(Uuid::from_u128(37));
    runtime
        .install_group_for_test(
            group_id,
            GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member.clone()])
                .expect("group should build"),
        )
        .expect("group should install");

    let row_id = test_row_id(group_id, dataset_id.clone(), 38);
    let mut source_dataset = LocalDataset::new(schema);
    let first_operation = apply_local_upsert(
        &mut source_dataset,
        &row_id,
        crate::row_values! { "title" => "first" },
        UpdateId {
            version: 1,
            node_index: 0,
        },
    )
    .expect("first operation should build")
    .expect("first operation should apply")
    .encoded_operation;
    let second_operation = apply_local_upsert(
        &mut source_dataset,
        &row_id,
        crate::row_values! { "title" => "second" },
        UpdateId {
            version: 2,
            node_index: 0,
        },
    )
    .expect("second operation should build")
    .expect("second operation should apply")
    .encoded_operation;
    let member_count = NonZeroUsize::new(2).expect("group has two members");
    let first_message = UpdateMessage {
        group_id,
        update_id: UpdateId {
            version: 1,
            node_index: 0,
        },
        read_versions: VersionVector::initial(member_count),
        dataset_updates: vec![DatasetUpdateMessage {
            dataset_id: dataset_id.clone(),
            operations: vec![first_operation],
        }],
    };
    let mut second_read_versions = VersionVector::initial(member_count);
    second_read_versions.increment_at(0);
    let second_message = UpdateMessage {
        group_id,
        update_id: UpdateId {
            version: 2,
            node_index: 0,
        },
        read_versions: second_read_versions,
        dataset_updates: vec![DatasetUpdateMessage {
            dataset_id: dataset_id.clone(),
            operations: vec![second_operation],
        }],
    };

    runtime
        .apply_update_for_test(alice_member.clone(), second_message)
        .expect("out-of-order update should persist pending state");
    store.fail_next_apply_dataset_row_patch(dataset_id.clone());
    let error = runtime
        .apply_update_for_test(alice_member.clone(), first_message.clone())
        .expect_err("store write failure should abort the whole ready chain");
    assert!(matches!(error, InboundDeliveryError::StoreAccess { .. }));
    assert!(listener.captured_data_changes().is_empty());

    assert!(
        load_persisted_update(
            sqlite_store.as_ref(),
            group_id,
            UpdateId {
                version: 1,
                node_index: 0,
            },
        )
        .is_none(),
        "the newly ready predecessor must roll back with the failed transaction"
    );
    assert!(
        !load_persisted_update(
            sqlite_store.as_ref(),
            group_id,
            UpdateId {
                version: 2,
                node_index: 0,
            },
        )
        .expect("pending successor should still exist")
        .applied_locally,
        "the previously buffered successor must stay pending after rollback"
    );
    let row_slice = load_persisted_row_slice(
        sqlite_store.as_ref(),
        group_id,
        &dataset_id,
        [row_id.row_key],
    );
    assert!(!row_slice.dataset_exists);
    assert_eq!(row_slice.rows.get(&row_id.row_key), Some(&None));
    assert_eq!(
        load_persisted_group(sqlite_store.as_ref(), group_id)
            .version_vector
            .version_at(0),
        0
    );

    runtime
        .apply_update_for_test(alice_member, first_message)
        .expect("retry after rollback should succeed");
    listener.wait_for_data_change_count(2);
}

#[test]
fn buffered_updates_reject_conflicting_duplicate_payloads() {
    // Conflicting duplicate protection:
    // 1. buffer one out-of-order update on Bob,
    // 2. deliver a second message with the same UpdateId but different payload,
    // 3. assert that the runtime rejects the conflicting duplicate explicitly.
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let schema = title_schema_static();
    let bob_runtime = load_runtime_fixture(
        app_bob_id(),
        bob_member.clone(),
        [(dataset_id.clone(), title_schema_static())],
    );
    let bob_runtime = bob_runtime.runtime;
    let group_id = GroupId(Uuid::from_u128(24));
    bob_runtime
        .install_group_for_test(
            group_id,
            GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member.clone()])
                .expect("group should build"),
        )
        .expect("group should install");

    let row_id = test_row_id(group_id, dataset_id.clone(), 25);
    let member_count = NonZeroUsize::new(2).expect("group has two members");

    let mut first_source_dataset = LocalDataset::new(schema);
    let first_operation = apply_local_upsert(
        &mut first_source_dataset,
        &row_id,
        crate::row_values! { "title" => "first" },
        UpdateId {
            version: 2,
            node_index: 0,
        },
    )
    .expect("first operation should build")
    .expect("first operation should apply")
    .encoded_operation;

    let mut conflicting_source_dataset = LocalDataset::new(schema);
    let conflicting_operation = apply_local_upsert(
        &mut conflicting_source_dataset,
        &row_id,
        crate::row_values! { "title" => "conflict" },
        UpdateId {
            version: 2,
            node_index: 0,
        },
    )
    .expect("conflicting operation should build")
    .expect("conflicting operation should apply")
    .encoded_operation;

    let buffered_message = UpdateMessage {
        group_id,
        update_id: UpdateId {
            version: 2,
            node_index: 0,
        },
        read_versions: {
            let mut read_versions = VersionVector::initial(member_count);
            read_versions.increment_at(0);
            read_versions
        },
        dataset_updates: vec![DatasetUpdateMessage {
            dataset_id: dataset_id.clone(),
            operations: vec![first_operation],
        }],
    };
    let conflicting_message = UpdateMessage {
        group_id,
        update_id: UpdateId {
            version: 2,
            node_index: 0,
        },
        read_versions: {
            let mut read_versions = VersionVector::initial(member_count);
            read_versions.increment_at(0);
            read_versions
        },
        dataset_updates: vec![DatasetUpdateMessage {
            dataset_id,
            operations: vec![conflicting_operation],
        }],
    };

    bob_runtime
        .apply_update_for_test(alice_member.clone(), buffered_message)
        .expect("first out-of-order update should buffer");
    let error = bob_runtime
        .apply_update_for_test(alice_member, conflicting_message)
        .expect_err("conflicting duplicate payload should fail");
    match error {
        InboundDeliveryError::ConflictingPersistedUpdate {
            group: actual_group_id,
            update: update_id,
        } => {
            assert_eq!(actual_group_id, group_id);
            assert_eq!(
                update_id,
                UpdateId {
                    version: 2,
                    node_index: 0,
                }
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
}
