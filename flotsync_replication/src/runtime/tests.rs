use super::{
    handle::{ReplicationRuntime, load_replication_runtime_typed, wait_for_test_reply},
    *,
};
use crate::{
    GroupMembers,
    GroupMemberships,
    api::{DatasetId, ListenerError, MemberIndex},
};
use flotsync_data_types::{Field, Schema};
use std::{
    collections::HashMap,
    future::Future,
    sync::{Mutex, mpsc},
    thread,
    time::{Duration, Instant},
};
use uuid::Uuid;

const TEST_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
const TEST_POLL_INTERVAL: Duration = Duration::from_millis(10);

struct StoreStub {
    local_member: crate::api::MemberIdentity,
    schemas: HashMap<DatasetId, Arc<Schema>>,
}

impl StoreStub {
    fn new(local_member: crate::api::MemberIdentity) -> Self {
        Self {
            local_member,
            schemas: HashMap::new(),
        }
    }

    fn with_schema(mut self, dataset_id: DatasetId, schema: Arc<Schema>) -> Self {
        self.schemas.insert(dataset_id, schema);
        self
    }
}

impl crate::api::ReplicationStore for StoreStub {
    fn local_member_identity(
        &self,
    ) -> BoxFuture<'_, Result<crate::api::MemberIdentity, crate::api::StoreError>> {
        let local_member = self.local_member.clone();
        Box::pin(async move { Ok(local_member) })
    }

    fn load_dataset_schema(
        &self,
        dataset_id: &DatasetId,
    ) -> BoxFuture<
        '_,
        Result<Option<Arc<flotsync_data_types::schema::Schema>>, crate::api::StoreError>,
    > {
        let schema = self.schemas.get(dataset_id).cloned();
        Box::pin(async move { Ok(schema) })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CapturedDataChange {
    rows: Vec<CapturedRowChange>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CapturedRowChange {
    Upsert {
        row_id: crate::api::RowId,
        title: String,
    },
    Delete {
        row_id: crate::api::RowId,
    },
}

impl CapturedRowChange {
    fn capture(change: RowChange) -> Result<Self, ListenerError> {
        match change {
            RowChange::Upsert { row_id, row } => {
                let title = row
                    .get_field_value::<str>("title")
                    .boxed()
                    .context(crate::ListenerExternalSnafu)?
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
        wait_until(
            || format!("{count} listener data-change events"),
            || {
                self.drain_buffered_events();
                self.data_changes
                    .lock()
                    .expect("listener capture mutex must not be poisoned")
                    .len()
                    >= count
            },
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

impl crate::api::ReplicationEventListener for ListenerStub {
    fn on_event(
        &self,
        event: crate::api::ReplicationEvent,
    ) -> BoxFuture<'_, Result<(), ListenerError>> {
        Box::pin(async move {
            match event {
                crate::api::ReplicationEvent::DataChanged { mut rows } => {
                    let mut captured_rows = Vec::new();
                    loop {
                        let batch = rows
                            .next_batch()
                            .await
                            .boxed()
                            .context(crate::ListenerExternalSnafu)?;
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
                crate::api::ReplicationEvent::GroupInvitation { .. } => {}
            }
            Ok(())
        })
    }
}

fn member<const N: usize>(segments: [&str; N]) -> crate::api::MemberIdentity {
    Identifier::from_array(segments)
}

fn poll_ready<F>(future: F) -> F::Output
where
    F: Future,
{
    wait_for_test_reply(future)
}

fn load_runtime(
    application_id: Identifier,
    local_member: crate::api::MemberIdentity,
) -> Arc<ReplicationRuntime> {
    let store = Arc::new(StoreStub::new(local_member));
    let listener = Arc::new(ListenerStub::default());
    load_runtime_with_parts(application_id, store, listener)
}

fn load_runtime_with_parts(
    application_id: Identifier,
    store: Arc<StoreStub>,
    listener: Arc<ListenerStub>,
) -> Arc<ReplicationRuntime> {
    poll_ready(load_replication_runtime_typed(
        application_id,
        store,
        listener,
        ReplicationConfig::default(),
    ))
    .expect("runtime should load")
}

fn wait_for_group_install(runtime: &Arc<ReplicationRuntime>, group_id: GroupId) {
    wait_until(
        || "runtime to install group".to_owned(),
        || {
            runtime
                .host()
                .membership_snapshot()
                .contains_group(&group_id)
        },
    );
}

fn wait_until(description: impl Fn() -> String, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + TEST_WAIT_TIMEOUT;
    loop {
        if condition() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {}",
            description()
        );
        thread::sleep(TEST_POLL_INTERVAL);
    }
}

#[test]
fn delivery_runtime_host_updates_shared_group_memberships() {
    let local_member = member(["alice"]);
    let mut host = DeliveryRuntimeHost::new(local_member.clone()).expect("host should start");
    let group_id = crate::api::GroupId(Uuid::from_u128(1));
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
    let application_id = Identifier::from_array(["app"]);
    let store = Arc::new(StoreStub::new(member(["alice"])));
    let listener = Arc::new(ListenerStub::default());

    let runtime = poll_ready(load_replication_runtime(
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
    let mut host = DeliveryRuntimeHost::new(member(["probe"])).expect("host should start");

    assert!(host.external_udp_bind_addr().ip().is_loopback());
    host.shutdown();
}

#[test]
fn create_group_bootstrap_installs_remote_membership() {
    let alice_member = member(["alice"]);
    let bob_member = member(["bob"]);
    let alice = load_runtime(
        Identifier::from_array(["app", "alice"]),
        alice_member.clone(),
    );
    let bob = load_runtime(Identifier::from_array(["app", "bob"]), bob_member.clone());

    assert!(alice.host().external_udp_bind_addr().ip().is_loopback());
    assert!(bob.host().external_udp_bind_addr().ip().is_loopback());

    alice.host().publish_direct_peer_route(
        bob_member.clone(),
        bob.host().advertised_loopback_udp_addr(),
    );
    bob.host().publish_direct_peer_route(
        alice_member.clone(),
        alice.host().advertised_loopback_udp_addr(),
    );

    let group_id = poll_ready(alice.create_group(CreateGroupRequest {
        members: vec![alice_member.clone(), bob_member.clone()],
        initial_state: None,
    }))
    .expect("create_group should succeed");
    wait_for_group_install(&bob, group_id);

    let alice_snapshot = alice.host().membership_snapshot();
    let bob_snapshot = bob.host().membership_snapshot();
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
    let alice_member = member(["alice"]);
    let bob_member = member(["bob"]);
    let dataset_id = DatasetId::try_new("docs").expect("dataset id should be valid");
    let schema = Arc::new(Schema::from_fields([Field::linear_string("title")]));
    let alice_listener = Arc::new(ListenerStub::default());
    let bob_listener = Arc::new(ListenerStub::default());
    let alice_store = Arc::new(
        StoreStub::new(alice_member.clone()).with_schema(dataset_id.clone(), schema.clone()),
    );
    let bob_store = Arc::new(
        StoreStub::new(bob_member.clone()).with_schema(dataset_id.clone(), schema.clone()),
    );
    let alice = load_runtime_with_parts(
        Identifier::from_array(["app", "alice"]),
        alice_store,
        alice_listener,
    );
    let bob = load_runtime_with_parts(
        Identifier::from_array(["app", "bob"]),
        bob_store,
        bob_listener.clone(),
    );

    alice.host().publish_direct_peer_route(
        bob_member.clone(),
        bob.host().advertised_loopback_udp_addr(),
    );
    bob.host().publish_direct_peer_route(
        alice_member.clone(),
        alice.host().advertised_loopback_udp_addr(),
    );

    let group_id = poll_ready(alice.create_group(CreateGroupRequest {
        members: vec![alice_member.clone(), bob_member.clone()],
        initial_state: None,
    }))
    .expect("create_group should succeed");
    wait_for_group_install(&bob, group_id);
    let row_id = crate::api::RowId {
        group_id,
        dataset_id: dataset_id.clone(),
        row_key: crate::api::RowKey(Uuid::from_u128(11)),
    };

    let receipt = poll_ready(alice.publish_changes(vec![RowMutation::Upsert {
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

    let delivered = bob_listener.wait_for_next_data_change();
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
        bob.host().membership_snapshot().contains_group(&group_id),
        "remote runtime should still host the replicated group"
    );
}

#[test]
fn inbound_updates_buffer_until_causal_dependencies_are_met_and_ignore_duplicates() {
    let alice_member = member(["alice"]);
    let bob_member = member(["bob"]);
    let dataset_id = DatasetId::try_new("docs").expect("dataset id should be valid");
    let schema = Arc::new(Schema::from_fields([Field::linear_string("title")]));
    let bob_listener = Arc::new(ListenerStub::default());
    let bob_store = Arc::new(
        StoreStub::new(bob_member.clone()).with_schema(dataset_id.clone(), schema.clone()),
    );
    let bob = load_runtime_with_parts(
        Identifier::from_array(["app", "bob"]),
        bob_store,
        bob_listener.clone(),
    );
    let group_id = GroupId(Uuid::from_u128(22));
    bob.install_group_for_test(
        group_id,
        GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member.clone()])
            .expect("group should build"),
    )
    .expect("group should install");

    let row_id = crate::api::RowId {
        group_id,
        dataset_id: dataset_id.clone(),
        row_key: crate::api::RowKey(Uuid::from_u128(23)),
    };
    let mut source_dataset = LocalDataset::new(schema);
    let first_operation = apply_local_upsert(
        &mut source_dataset,
        &row_id,
        &crate::row_values! { "title" => "first" },
        UpdateId {
            version: 1,
            node_index: 0,
        },
    )
    .expect("first operation should build")
    .expect("first operation should apply");
    let second_operation = apply_local_upsert(
        &mut source_dataset,
        &row_id,
        &crate::row_values! { "title" => "second" },
        UpdateId {
            version: 2,
            node_index: 0,
        },
    )
    .expect("second operation should build")
    .expect("second operation should apply");

    let member_count = NonZeroUsize::new(2).expect("group has two members");
    let first_message = UpdateBatchMessage {
        group_id,
        update_id: UpdateId {
            version: 1,
            node_index: 0,
        },
        read_versions: initial_version_vector(member_count),
        dataset_updates: vec![DatasetUpdateMessage {
            dataset_id: dataset_id.clone(),
            operations: vec![first_operation],
        }],
    };
    let mut second_read_versions = initial_version_vector(member_count);
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

    bob.apply_update_batch_for_test(alice_member.clone(), second_message)
        .expect("out-of-order update should buffer");
    assert!(bob_listener.captured_data_changes().is_empty());

    bob.apply_update_batch_for_test(alice_member.clone(), first_message.clone())
        .expect("first update should apply and drain the pending second update");
    bob_listener.wait_for_data_change_count(2);
    assert_eq!(
        bob_listener.captured_data_changes(),
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
    bob.apply_update_batch_for_test(alice_member, first_message)
        .expect("duplicate update should be ignored");
    assert_eq!(bob_listener.captured_data_changes().len(), 2);
}

#[test]
fn buffered_updates_reject_conflicting_duplicate_payloads() {
    let alice_member = member(["alice"]);
    let bob_member = member(["bob"]);
    let dataset_id = DatasetId::try_new("docs").expect("dataset id should be valid");
    let schema = Arc::new(Schema::from_fields([Field::linear_string("title")]));
    let bob_store = Arc::new(
        StoreStub::new(bob_member.clone()).with_schema(dataset_id.clone(), schema.clone()),
    );
    let bob = load_runtime_with_parts(
        Identifier::from_array(["app", "bob"]),
        bob_store,
        Arc::new(ListenerStub::default()),
    );
    let group_id = GroupId(Uuid::from_u128(24));
    bob.install_group_for_test(
        group_id,
        GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member.clone()])
            .expect("group should build"),
    )
    .expect("group should install");

    let row_id = crate::api::RowId {
        group_id,
        dataset_id: dataset_id.clone(),
        row_key: crate::api::RowKey(Uuid::from_u128(25)),
    };
    let member_count = NonZeroUsize::new(2).expect("group has two members");

    let mut first_source_dataset = LocalDataset::new(schema.clone());
    let first_operation = apply_local_upsert(
        &mut first_source_dataset,
        &row_id,
        &crate::row_values! { "title" => "first" },
        UpdateId {
            version: 1,
            node_index: 0,
        },
    )
    .expect("first operation should build")
    .expect("first operation should apply");

    let mut conflicting_source_dataset = LocalDataset::new(schema);
    let conflicting_operation = apply_local_upsert(
        &mut conflicting_source_dataset,
        &row_id,
        &crate::row_values! { "title" => "conflict" },
        UpdateId {
            version: 1,
            node_index: 0,
        },
    )
    .expect("conflicting operation should build")
    .expect("conflicting operation should apply");

    let buffered_message = UpdateBatchMessage {
        group_id,
        update_id: UpdateId {
            version: 2,
            node_index: 0,
        },
        read_versions: {
            let mut read_versions = initial_version_vector(member_count);
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
            let mut read_versions = initial_version_vector(member_count);
            read_versions.increment_at(0);
            read_versions
        },
        dataset_updates: vec![DatasetUpdateMessage {
            dataset_id,
            operations: vec![conflicting_operation],
        }],
    };

    bob.apply_update_batch_for_test(alice_member.clone(), buffered_message)
        .expect("first out-of-order update should buffer");
    let error = bob
        .apply_update_batch_for_test(alice_member, conflicting_message)
        .expect_err("conflicting duplicate payload should fail");
    match error {
        InboundDeliveryError::ConflictingBufferedUpdate {
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
