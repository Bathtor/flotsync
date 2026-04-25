use super::{
    errors::InboundDeliveryError,
    handle::{ReplicationRuntime, load_replication_runtime_typed, wait_for_test_reply},
    host::{DeliveryRuntimeHost, DeliveryRuntimeHostTestExt},
    in_memory::{LocalDataset, apply_local_upsert},
    load_replication_runtime,
    messages::{DatasetUpdateMessage, UpdateBatchMessage},
};
use crate::{
    GroupMembers,
    GroupMemberships,
    api::{
        CreateGroupRequest,
        DatasetId,
        DatasetRowPatch,
        DatasetRowSlice,
        DatasetRowWrite,
        GroupId,
        ListenerError,
        ListenerExternalSnafu,
        MemberIdentity,
        MemberIndex,
        ReplicationApi,
        ReplicationConfig,
        ReplicationEvent,
        ReplicationEventListener,
        ReplicationGroupRecord,
        ReplicationStore,
        ReplicationStoreTransaction,
        ReplicationUpdateFilter,
        ReplicationUpdateRecord,
        RowChange,
        RowId,
        RowKey,
        RowKeyIterator,
        RowMutation,
        SchemaSource,
        StoreError,
    },
};
use flotsync_core::{
    member::Identifier,
    versions::{UpdateId, VersionVector},
};
use flotsync_data_types::{Field, RowOperations, Schema, schema::datamodel::RowSnapshot};
use flotsync_io::test_support::eventually;
use flotsync_utils::BoxFuture;
use futures_util::{FutureExt, future};
use snafu::ResultExt;
use std::{
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
    sync::{Arc, Mutex, mpsc},
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

struct RuntimeFixture {
    runtime: Arc<ReplicationRuntime>,
    listener: Arc<ListenerStub>,
}

struct StoreStub {
    local_member: MemberIdentity,
    schemas: HashMap<DatasetId, SchemaSource>,
    state: Arc<Mutex<StoreStubState>>,
    fail_next_apply_dataset_row_patch: Arc<Mutex<Option<DatasetId>>>,
}

impl StoreStub {
    fn new(local_member: MemberIdentity) -> Self {
        Self {
            local_member,
            schemas: HashMap::new(),
            state: Arc::new(Mutex::new(StoreStubState::default())),
            fail_next_apply_dataset_row_patch: Arc::new(Mutex::new(None)),
        }
    }

    fn with_schema(mut self, dataset_id: DatasetId, schema: Arc<Schema>) -> Self {
        self.schemas.insert(dataset_id, SchemaSource::from(schema));
        self
    }

    fn snapshot_state(&self) -> StoreStubState {
        self.state
            .lock()
            .expect("store stub state mutex must not be poisoned")
            .clone()
    }

    fn fail_next_apply_dataset_row_patch(&self, dataset_id: DatasetId) {
        *self
            .fail_next_apply_dataset_row_patch
            .lock()
            .expect("store stub failure mutex must not be poisoned") = Some(dataset_id);
    }
}

impl ReplicationStore for StoreStub {
    fn local_member_identity(&self) -> BoxFuture<'_, Result<MemberIdentity, StoreError>> {
        future::ok(self.local_member.clone()).boxed()
    }

    fn load_dataset_schema(
        &self,
        dataset_id: &DatasetId,
    ) -> BoxFuture<'_, Result<Option<SchemaSource>, StoreError>> {
        future::ok(self.schemas.get(dataset_id).cloned()).boxed()
    }

    fn begin_transaction(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ReplicationStoreTransaction>, StoreError>> {
        let state = self
            .state
            .lock()
            .expect("store stub state mutex must not be poisoned")
            .clone();
        let transaction = StoreStubTransaction {
            shared_state: self.state.clone(),
            fail_next_apply_dataset_row_patch: self.fail_next_apply_dataset_row_patch.clone(),
            working_state: state,
            closed: false,
        };
        future::ok(Box::new(transaction) as Box<dyn ReplicationStoreTransaction>).boxed()
    }
}

#[derive(Clone, Default)]
struct StoreStubState {
    groups: HashMap<GroupId, ReplicationGroupRecord>,
    datasets: HashSet<(GroupId, DatasetId)>,
    dataset_rows: HashMap<(GroupId, DatasetId, RowKey), RowSnapshot<'static, UpdateId>>,
    updates: HashMap<(GroupId, UpdateId), ReplicationUpdateRecord>,
}

struct StoreStubTransaction {
    shared_state: Arc<Mutex<StoreStubState>>,
    fail_next_apply_dataset_row_patch: Arc<Mutex<Option<DatasetId>>>,
    working_state: StoreStubState,
    closed: bool,
}

impl StoreStubTransaction {
    fn assert_open(&self) {
        assert!(
            !self.closed,
            "store stub transaction must not be used after commit or rollback"
        );
    }
}

impl ReplicationStoreTransaction for StoreStubTransaction {
    fn load_replication_group<'a>(
        &'a mut self,
        group_id: &'a GroupId,
    ) -> BoxFuture<'a, Result<Option<ReplicationGroupRecord>, StoreError>> {
        self.assert_open();
        future::ok(self.working_state.groups.get(group_id).cloned()).boxed()
    }

    fn load_replication_groups<'a>(
        &'a mut self,
    ) -> BoxFuture<'a, Result<Vec<ReplicationGroupRecord>, StoreError>> {
        self.assert_open();
        let groups = self.working_state.groups.values().cloned().collect();
        future::ok(groups).boxed()
    }

    fn insert_replication_group<'a>(
        &'a mut self,
        group: ReplicationGroupRecord,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        self.assert_open();
        if self
            .working_state
            .groups
            .insert(group.group_id, group)
            .is_some()
        {
            return future::err(StoreError::StoreExternal {
                source: Box::new(std::io::Error::other(
                    "store stub does not allow duplicate group inserts",
                )),
            })
            .boxed();
        }
        future::ok(()).boxed()
    }

    fn update_replication_group_version_vector<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        version_vector: VersionVector,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        self.assert_open();
        let Some(group) = self.working_state.groups.get_mut(group_id) else {
            return future::err(StoreError::StoreExternal {
                source: Box::new(std::io::Error::other(format!(
                    "store stub could not find group '{group_id}'"
                ))),
            })
            .boxed();
        };
        group.version_vector = version_vector;
        future::ok(()).boxed()
    }

    fn load_dataset_rows<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        dataset_id: &'a DatasetId,
        row_keys: &'a mut RowKeyIterator<'a>,
    ) -> BoxFuture<'a, Result<DatasetRowSlice, StoreError>> {
        self.assert_open();
        let row_keys: Vec<_> = row_keys.copied().collect();
        let dataset_exists = self
            .working_state
            .datasets
            .contains(&(*group_id, dataset_id.clone()));
        let rows = row_keys
            .iter()
            .copied()
            .map(|row_key| {
                (
                    row_key,
                    self.working_state
                        .dataset_rows
                        .get(&(*group_id, dataset_id.clone(), row_key))
                        .cloned(),
                )
            })
            .collect();
        future::ok(DatasetRowSlice {
            group_id: *group_id,
            dataset_id: dataset_id.clone(),
            dataset_exists,
            rows,
        })
        .boxed()
    }

    fn apply_dataset_row_patch<'a>(
        &'a mut self,
        patch: DatasetRowPatch,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        self.assert_open();
        let mut failure = self
            .fail_next_apply_dataset_row_patch
            .lock()
            .expect("store stub failure mutex must not be poisoned");
        if failure.as_ref() == Some(&patch.dataset_id) {
            *failure = None;
            return future::err(StoreError::StoreExternal {
                source: Box::new(std::io::Error::other(format!(
                    "store stub intentionally failed dataset row patch apply for '{}'",
                    patch.dataset_id
                ))),
            })
            .boxed();
        }
        if patch
            .actions
            .iter()
            .any(|action| matches!(action, DatasetRowWrite::Put { .. }))
        {
            self.working_state
                .datasets
                .insert((patch.group_id, patch.dataset_id.clone()));
        }
        for action in patch.actions {
            match action {
                DatasetRowWrite::Put { row_key, row } => {
                    self.working_state
                        .dataset_rows
                        .insert((patch.group_id, patch.dataset_id.clone(), row_key), row);
                }
                DatasetRowWrite::Delete { row_key } => {
                    let removed = self.working_state.dataset_rows.remove(&(
                        patch.group_id,
                        patch.dataset_id.clone(),
                        row_key,
                    ));
                    if removed.is_none() {
                        return future::err(StoreError::StoreExternal {
                            source: Box::new(std::io::Error::other(format!(
                                "store stub could not find row '{}/{}/{}'",
                                patch.group_id, patch.dataset_id, row_key
                            ))),
                        })
                        .boxed();
                    }
                }
            }
        }
        future::ok(()).boxed()
    }

    fn load_replication_update<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        update_id: UpdateId,
    ) -> BoxFuture<'a, Result<Option<ReplicationUpdateRecord>, StoreError>> {
        self.assert_open();
        let update = self
            .working_state
            .updates
            .get(&(*group_id, update_id))
            .cloned();
        future::ok(update).boxed()
    }

    fn load_replication_updates<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        filter: ReplicationUpdateFilter,
    ) -> BoxFuture<'a, Result<Vec<ReplicationUpdateRecord>, StoreError>> {
        self.assert_open();
        let updates = self
            .working_state
            .updates
            .values()
            .filter(|update| update.group_id == *group_id)
            .filter(|update| match filter {
                ReplicationUpdateFilter::All => true,
                ReplicationUpdateFilter::PendingApply => !update.applied_locally,
                ReplicationUpdateFilter::Applied => update.applied_locally,
            })
            .cloned()
            .collect();
        future::ok(updates).boxed()
    }

    fn append_replication_update<'a>(
        &'a mut self,
        update: ReplicationUpdateRecord,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        self.assert_open();
        if self
            .working_state
            .updates
            .insert((update.group_id, update.update_id), update)
            .is_some()
        {
            return future::err(StoreError::StoreExternal {
                source: Box::new(std::io::Error::other(
                    "store stub does not allow duplicate update inserts",
                )),
            })
            .boxed();
        }
        future::ok(()).boxed()
    }

    fn mark_replication_update_applied<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        update_id: UpdateId,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        self.assert_open();
        if let Some(update) = self.working_state.updates.get_mut(&(*group_id, update_id)) {
            update.applied_locally = true;
        }
        future::ok(()).boxed()
    }

    fn commit(mut self: Box<Self>) -> BoxFuture<'static, Result<(), StoreError>> {
        self.assert_open();
        self.closed = true;
        let working_state = self.working_state;
        let shared_state = self.shared_state;
        async move {
            *shared_state
                .lock()
                .expect("store stub state mutex must not be poisoned") = working_state;
            Ok(())
        }
        .boxed()
    }

    fn rollback(mut self: Box<Self>) -> BoxFuture<'static, Result<(), StoreError>> {
        self.assert_open();
        self.closed = true;
        future::ok(()).boxed()
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
}

struct ListenerStub {
    data_changes: Mutex<Vec<CapturedDataChange>>,
    buffered_events: Mutex<mpsc::Receiver<CapturedDataChange>>,
    buffered_event_tx: mpsc::Sender<CapturedDataChange>,
}

impl Default for ListenerStub {
    fn default() -> Self {
        let (buffered_event_tx, buffered_events) = mpsc::channel();
        Self {
            data_changes: Mutex::new(Vec::new()),
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
}

impl ReplicationEventListener for ListenerStub {
    fn on_event(&self, event: ReplicationEvent) -> BoxFuture<'_, Result<(), ListenerError>> {
        Box::pin(async move {
            match event {
                ReplicationEvent::DataChanged { mut rows } => {
                    let mut captured_rows = Vec::new();
                    loop {
                        let batch = rows
                            .next_batch()
                            .await
                            .boxed()
                            .context(ListenerExternalSnafu)?;
                        if batch.is_empty() {
                            break;
                        }
                        for change in batch {
                            captured_rows.push(CapturedRowChange::capture(change)?);
                        }
                    }
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

fn title_schema() -> Arc<Schema> {
    Arc::new(Schema::from_fields([Field::linear_string("title")]))
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
    let store = Arc::new(StoreStub::new(local_member));
    let listener = Arc::new(ListenerStub::default());
    load_runtime_with_parts(application_id, store, listener)
}

fn load_runtime_fixture(
    application_id: Identifier,
    local_member: MemberIdentity,
    schemas: impl IntoIterator<Item = (DatasetId, Arc<Schema>)>,
) -> RuntimeFixture {
    let listener = Arc::new(ListenerStub::default());
    let mut store = StoreStub::new(local_member);
    for (dataset_id, schema) in schemas {
        store = store.with_schema(dataset_id, schema);
    }
    let runtime = load_runtime_with_parts(application_id, Arc::new(store), listener.clone());
    RuntimeFixture { runtime, listener }
}

fn start_host(local_member: MemberIdentity) -> DeliveryRuntimeHost {
    let store = Arc::new(StoreStub::new(local_member.clone()));
    let listener = Arc::new(ListenerStub::default());
    DeliveryRuntimeHost::start(local_member, store, listener).expect("host should start")
}

fn load_runtime_with_parts(
    application_id: Identifier,
    store: Arc<StoreStub>,
    listener: Arc<ListenerStub>,
) -> Arc<ReplicationRuntime> {
    wait_for_test_reply(load_replication_runtime_typed(
        application_id,
        store,
        listener,
        ReplicationConfig::default(),
    ))
    .expect("runtime should load")
}

fn persist_group_in_store(store: &StoreStub, group: ReplicationGroupRecord) {
    let mut transaction =
        wait_for_test_reply(store.begin_transaction()).expect("transaction should start");
    wait_for_test_reply(transaction.insert_replication_group(group)).expect("group should persist");
    wait_for_test_reply(transaction.commit()).expect("transaction should commit");
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
    let mut host = start_host(local_member.clone());
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
    let store = Arc::new(StoreStub::new(alice_member()));
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
    let mut host = start_host(Identifier::from_array(PROBE_MEMBER_SEGMENTS));

    assert!(host.external_udp_bind_addr().ip().is_loopback());
    host.shutdown();
}

#[test]
fn runtime_startup_hydrates_persisted_group_memberships_from_store() {
    let alice_member = alice_member();
    let dataset_id = docs_dataset_id();
    let schema = title_schema();
    let store =
        Arc::new(StoreStub::new(alice_member.clone()).with_schema(dataset_id.clone(), schema));
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

    assert!(
        runtime
            .host()
            .membership_snapshot()
            .contains_group(&group_id)
    );
    wait_for_test_reply(runtime.publish_changes(vec![RowMutation::Upsert {
        row_id,
        row: crate::row_values! {
            "title" => "hydrated on startup",
        },
    }]))
    .expect("publish_changes should succeed for the hydrated group");
}

#[test]
fn create_group_persists_membership_across_runtime_restart() {
    let alice_member = alice_member();
    let dataset_id = docs_dataset_id();
    let schema = title_schema();
    let store =
        Arc::new(StoreStub::new(alice_member.clone()).with_schema(dataset_id.clone(), schema));
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

    assert!(
        restarted_runtime
            .host()
            .membership_snapshot()
            .contains_group(&group_id),
        "restarted runtime should hydrate the persisted group"
    );
    wait_for_test_reply(restarted_runtime.publish_changes(vec![RowMutation::Upsert {
        row_id,
        row: crate::row_values! {
            "title" => "after restart",
        },
    }]))
    .expect("publish_changes should succeed after restart hydration");
}

#[test]
fn publish_changes_persists_applied_update_and_snapshot_state() {
    let alice_member = alice_member();
    let dataset_id = docs_dataset_id();
    let schema = title_schema();
    let store =
        Arc::new(StoreStub::new(alice_member.clone()).with_schema(dataset_id.clone(), schema));
    let listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts(app_alice_id(), store.clone(), listener);
    let group_id = wait_for_test_reply(runtime.create_group(CreateGroupRequest {
        members: vec![alice_member],
        initial_state: None,
    }))
    .expect("create_group should succeed");
    let row_id = test_row_id(group_id, dataset_id.clone(), 34);

    let receipt = wait_for_test_reply(runtime.publish_changes(vec![RowMutation::Upsert {
        row_id: row_id.clone(),
        row: crate::row_values! {
            "title" => "durable publish",
        },
    }]))
    .expect("publish_changes should succeed");

    let persisted_state = store.snapshot_state();
    let persisted_group = persisted_state
        .groups
        .get(&group_id)
        .expect("group should persist");
    assert_eq!(persisted_group.version_vector.version_at(0), 1);
    let persisted_update = persisted_state
        .updates
        .get(&(group_id, receipt.update_id))
        .expect("published update should persist");
    assert!(persisted_update.applied_locally);
    assert!(
        persisted_state
            .dataset_rows
            .contains_key(&(group_id, dataset_id, row_id.row_key))
    );
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
    let schema = title_schema();
    let alice_fixture = load_runtime_fixture(
        app_alice_id(),
        alice_member.clone(),
        [(dataset_id.clone(), schema.clone())],
    );
    let bob_fixture = load_runtime_fixture(
        app_bob_id(),
        bob_member.clone(),
        [(dataset_id.clone(), schema.clone())],
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

    let receipt = wait_for_test_reply(alice_runtime.publish_changes(vec![RowMutation::Upsert {
        row_id: row_id.clone(),
        row: crate::row_values! {
            "title" => "hello from alice",
        },
    }]))
    .expect("publish_changes should succeed");

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
    let schema = title_schema();
    let bob_fixture = load_runtime_fixture(
        app_bob_id(),
        bob_member.clone(),
        [(dataset_id.clone(), schema.clone())],
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
    let first_message = UpdateBatchMessage {
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
    let second_message = UpdateBatchMessage {
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
        .apply_update_batch_for_test(alice_member.clone(), second_message)
        .expect("out-of-order update should buffer");
    assert!(bob_fixture.listener.captured_data_changes().is_empty());

    bob_runtime
        .apply_update_batch_for_test(alice_member.clone(), first_message.clone())
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
        .apply_update_batch_for_test(alice_member, first_message)
        .expect("duplicate update should be ignored");
    assert_eq!(bob_fixture.listener.captured_data_changes().len(), 2);
}

#[test]
fn buffered_updates_survive_runtime_restart_and_drain_from_store() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let schema = title_schema();
    let store = Arc::new(
        StoreStub::new(bob_member.clone()).with_schema(dataset_id.clone(), schema.clone()),
    );
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
    let first_message = UpdateBatchMessage {
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
    let second_message = UpdateBatchMessage {
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
        .apply_update_batch_for_test(alice_member.clone(), second_message)
        .expect("out-of-order update should persist pending state");
    drop(runtime);

    let restarted_listener = Arc::new(ListenerStub::default());
    let restarted_runtime =
        load_runtime_with_parts(app_bob_id(), store.clone(), restarted_listener.clone());
    restarted_runtime
        .apply_update_batch_for_test(alice_member, first_message)
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

    let persisted_state = store.snapshot_state();
    assert!(
        persisted_state
            .updates
            .get(&(
                group_id,
                UpdateId {
                    version: 1,
                    node_index: 0,
                },
            ))
            .expect("first update should persist")
            .applied_locally
    );
    assert!(
        persisted_state
            .updates
            .get(&(
                group_id,
                UpdateId {
                    version: 2,
                    node_index: 0,
                },
            ))
            .expect("second update should persist")
            .applied_locally
    );
}

#[test]
fn causally_ready_apply_chain_rolls_back_when_store_write_fails() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let schema = title_schema();
    let store = Arc::new(
        StoreStub::new(bob_member.clone()).with_schema(dataset_id.clone(), schema.clone()),
    );
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
    let first_message = UpdateBatchMessage {
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
    let second_message = UpdateBatchMessage {
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
        .apply_update_batch_for_test(alice_member.clone(), second_message)
        .expect("out-of-order update should persist pending state");
    store.fail_next_apply_dataset_row_patch(dataset_id.clone());
    let error = runtime
        .apply_update_batch_for_test(alice_member.clone(), first_message.clone())
        .expect_err("store write failure should abort the whole ready chain");
    assert!(matches!(error, InboundDeliveryError::StoreAccess { .. }));
    assert!(listener.captured_data_changes().is_empty());

    let persisted_state = store.snapshot_state();
    assert!(
        !persisted_state.updates.contains_key(&(
            group_id,
            UpdateId {
                version: 1,
                node_index: 0,
            },
        )),
        "the newly ready predecessor must roll back with the failed transaction"
    );
    assert!(
        !persisted_state
            .updates
            .get(&(
                group_id,
                UpdateId {
                    version: 2,
                    node_index: 0,
                },
            ))
            .expect("pending successor should still exist")
            .applied_locally,
        "the previously buffered successor must stay pending after rollback"
    );
    assert!(persisted_state.dataset_rows.is_empty());
    assert_eq!(
        persisted_state
            .groups
            .get(&group_id)
            .expect("group should still exist")
            .version_vector
            .version_at(0),
        0
    );

    runtime
        .apply_update_batch_for_test(alice_member, first_message)
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
    let schema = title_schema();
    let bob_runtime = load_runtime_fixture(
        app_bob_id(),
        bob_member.clone(),
        [(dataset_id.clone(), schema.clone())],
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

    let mut first_source_dataset = LocalDataset::new(schema.clone());
    let first_operation = apply_local_upsert(
        &mut first_source_dataset,
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

    let mut conflicting_source_dataset = LocalDataset::new(schema);
    let conflicting_operation = apply_local_upsert(
        &mut conflicting_source_dataset,
        &row_id,
        crate::row_values! { "title" => "conflict" },
        UpdateId {
            version: 1,
            node_index: 0,
        },
    )
    .expect("conflicting operation should build")
    .expect("conflicting operation should apply")
    .encoded_operation;

    let buffered_message = UpdateBatchMessage {
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
    let conflicting_message = UpdateBatchMessage {
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
        .apply_update_batch_for_test(alice_member.clone(), buffered_message)
        .expect("first out-of-order update should buffer");
    let error = bob_runtime
        .apply_update_batch_for_test(alice_member, conflicting_message)
        .expect_err("conflicting duplicate payload should fail");
    match error {
        InboundDeliveryError::ConflictingPersistedUpdate {
            group_id: actual_group_id,
            update_id,
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
