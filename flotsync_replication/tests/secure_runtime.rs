#![cfg(feature = "test-support")]

use flotsync_core::{
    GroupId,
    MemberIdentity,
    MemberIndex,
    member::Identifier,
    membership::GroupMembers,
    versions::{PureVersionVector, UpdateId, VersionVector},
};
use flotsync_data_types::{Field, Schema};
use flotsync_replication::{
    ChangeGroupMembershipRequest,
    CreateGroupRequest,
    DatasetId,
    GroupInvitationPolicy,
    GroupMigrationPolicy,
    PolicyDecision,
    ReplicationConfig,
    ReplicationStore,
    RowId,
    RowKey,
    RowMutation,
    SqliteReplicationStore,
    SummaryRequest,
    test_support::{
        CapturedDataChange,
        CapturedRowChange,
        RuntimeTestFixture,
        docs_group_schema,
        provision_test_security,
        provision_test_trusted_public_keys,
        publish_changes,
        snapshot_read_token,
        test_public_member_keys,
        wait_for_test_future,
        wait_for_test_reply,
    },
};
use std::{
    collections::HashSet,
    num::NonZeroUsize,
    sync::{Arc, LazyLock},
};
use uuid::Uuid;

const ALICE_MEMBER_SEGMENTS: [&str; 2] = ["alice", "laptop"];
const BOB_MEMBER_SEGMENTS: [&str; 2] = ["bob", "laptop"];
const CHARLIE_MEMBER_SEGMENTS: [&str; 2] = ["charlie", "laptop"];
const PROBE_MEMBER_SEGMENTS: [&str; 2] = ["probe", "laptop"];
const APP_ALICE_SEGMENTS: [&str; 2] = ["app", "alice"];
const APP_BOB_SEGMENTS: [&str; 2] = ["app", "bob"];
const APP_CHARLIE_SEGMENTS: [&str; 2] = ["app", "charlie"];

static STATIC_TITLE_SCHEMA: LazyLock<Schema> =
    LazyLock::new(|| Schema::from_fields([Field::linear_string("title")]));

#[test]
fn create_group_bootstrap_installs_remote_membership() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let (alice_fixture, bob_fixture) = load_title_runtime_pair_with_trust(&dataset_id);

    alice_fixture.connect_direct_peer_routes(&bob_fixture);
    let group_id = wait_for_test_reply(alice_fixture.api().create_group(CreateGroupRequest {
        members: vec![alice_member.clone(), bob_member.clone()],
        group_schema: docs_group_schema(),
    }))
    .expect("create_group should succeed");
    bob_fixture.wait_for_group_install(group_id);

    let alice_members = alice_fixture
        .group_members(group_id)
        .expect("local runtime should host the created group");
    let bob_members = bob_fixture
        .group_members(group_id)
        .expect("remote runtime should install the invited group");

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
fn membership_change_delivers_proposal_to_continuing_member_and_invitation_to_added_member() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let charlie_member = charlie_member();
    let dataset_id = docs_dataset_id();
    let alice_fixture = RuntimeTestFixture::load(
        app_alice_id(),
        &alice_member,
        [(dataset_id.clone(), title_schema_shared())],
        [bob_member.clone(), charlie_member.clone()],
    );
    let bob_fixture = RuntimeTestFixture::load_with_config(
        app_bob_id(),
        &bob_member,
        [(dataset_id.clone(), title_schema_shared())],
        [alice_member.clone()],
        ReplicationConfig {
            group_invitation_policy: GroupInvitationPolicy {
                creation: PolicyDecision::AutoAccept,
                ..GroupInvitationPolicy::default()
            },
            group_migration_policy: GroupMigrationPolicy {
                member_added: PolicyDecision::AutoAccept,
                ..GroupMigrationPolicy::default()
            },
            ..ReplicationConfig::default()
        },
    );
    let charlie_fixture = RuntimeTestFixture::load_with_config(
        app_charlie_id(),
        &charlie_member,
        [(dataset_id, title_schema_shared())],
        [alice_member.clone()],
        ReplicationConfig {
            group_invitation_policy: GroupInvitationPolicy {
                migration_added_member: PolicyDecision::AutoAccept,
                ..GroupInvitationPolicy::default()
            },
            ..ReplicationConfig::default()
        },
    );

    alice_fixture.connect_direct_peer_routes(&bob_fixture);
    alice_fixture.connect_direct_peer_routes(&charlie_fixture);
    let old_group_id = wait_for_test_reply(alice_fixture.api().create_group(CreateGroupRequest {
        members: vec![alice_member.clone(), bob_member.clone()],
        group_schema: docs_group_schema(),
    }))
    .expect("old group should be created");
    bob_fixture.wait_for_group_install(old_group_id);

    // TODO(flotsync-git-i20): remove this forced full vector and cover natural
    // compact Synced and Override proposal vectors once inbound decode receives
    // the old-group member count.
    let mut transaction = wait_for_test_future(alice_fixture.store().begin_transaction())
        .expect("version-vector transaction should start");
    wait_for_test_future(transaction.update_replication_group_version_vector(
        &old_group_id,
        VersionVector::Full(PureVersionVector::from([0, 0])),
    ))
    .expect("full old-group vector should store");
    wait_for_test_future(transaction.commit()).expect("version-vector transaction should commit");

    let migration_id = wait_for_test_reply(alice_fixture.api().change_group_membership(
        ChangeGroupMembershipRequest {
            group_id: old_group_id,
            add_members: HashSet::from([charlie_member.clone()]),
            remove_members: HashSet::new(),
            group_name: None,
            message: None,
        },
    ))
    .expect("membership change should succeed");
    bob_fixture.wait_for_group_install(migration_id.new_group_id);
    charlie_fixture.wait_for_group_install(migration_id.new_group_id);

    for fixture in [&alice_fixture, &bob_fixture, &charlie_fixture] {
        let members = fixture
            .group_members(migration_id.new_group_id)
            .expect("new group should be installed");
        assert!(members.contains(&alice_member));
        assert!(members.contains(&bob_member));
        assert!(members.contains(&charlie_member));
    }
}

#[test]
fn publish_changes_delivers_remote_data_changed_event() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let (alice_fixture, bob_fixture) = load_title_runtime_pair_with_trust(&dataset_id);

    alice_fixture.connect_direct_peer_routes(&bob_fixture);
    let group_id = wait_for_test_reply(alice_fixture.api().create_group(CreateGroupRequest {
        members: vec![alice_member, bob_member],
        group_schema: docs_group_schema(),
    }))
    .expect("create_group should succeed");
    bob_fixture.wait_for_group_install(group_id);
    let row_id = test_row_id(group_id, dataset_id.clone(), 11);

    let read_token = snapshot_read_token(alice_fixture.api(), group_id, dataset_id.clone());
    let receipt = publish_changes(
        alice_fixture.api(),
        read_token,
        vec![RowMutation::Upsert {
            row_id: row_id.clone(),
            row: flotsync_replication::row_values! {
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

    let delivered = bob_fixture.listener().wait_for_next_data_change();
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
        bob_fixture.contains_group(group_id),
        "remote runtime should still host the replicated group"
    );
}

#[test]
fn update_gap_triggers_need_range_and_update_batch_catch_up() {
    let dataset_id = docs_dataset_id();
    let (alice_fixture, bob_fixture) = load_title_runtime_pair_with_trust(&dataset_id);
    let alice_member = alice_fixture.local_member.clone();
    let bob_member = bob_fixture.local_member.clone();
    let group_id = GroupId(Uuid::from_u128(50_001));
    let members =
        GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member.clone()])
            .expect("group members should build");
    alice_fixture.install_group_for_test(group_id, members.clone());
    bob_fixture.install_group_for_test(group_id, members);

    let first_row_id = test_row_id(group_id, dataset_id.clone(), 50_011);
    let second_row_id = test_row_id(group_id, dataset_id.clone(), 50_012);
    let first_read_token = snapshot_read_token(alice_fixture.api(), group_id, dataset_id.clone());
    let first_receipt = publish_changes(
        alice_fixture.api(),
        first_read_token,
        vec![RowMutation::Upsert {
            row_id: first_row_id.clone(),
            row: flotsync_replication::row_values! {
                "title" => "missed first",
            },
        }],
    );

    alice_fixture.connect_direct_peer_routes(&bob_fixture);
    let second_receipt = publish_changes(
        alice_fixture.api(),
        first_receipt.read_token,
        vec![RowMutation::Upsert {
            row_id: second_row_id.clone(),
            row: flotsync_replication::row_values! {
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
    bob_fixture.listener().wait_for_data_change_count(2);
    assert_eq!(
        bob_fixture.listener().captured_data_changes(),
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
fn observed_summary_triggers_need_range_and_update_batch_catch_up() {
    let dataset_id = docs_dataset_id();
    let (alice_fixture, bob_fixture) = load_title_runtime_pair_with_trust(&dataset_id);
    let alice_member = alice_fixture.local_member.clone();
    let bob_member = bob_fixture.local_member.clone();
    let group_id = GroupId(Uuid::from_u128(50_101));
    let members =
        GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member.clone()])
            .expect("group members should build");
    alice_fixture.install_group_for_test(group_id, members.clone());
    bob_fixture.install_group_for_test(group_id, members);

    let row_id = test_row_id(group_id, dataset_id.clone(), 50_111);
    let read_token = snapshot_read_token(alice_fixture.api(), group_id, dataset_id.clone());
    publish_changes(
        alice_fixture.api(),
        read_token,
        vec![RowMutation::Upsert {
            row_id: row_id.clone(),
            row: flotsync_replication::row_values! {
                "title" => "summary catch-up",
            },
        }],
    );
    assert!(bob_fixture.listener().captured_data_changes().is_empty());

    alice_fixture.connect_direct_peer_routes(&bob_fixture);
    let summary = wait_for_test_reply(bob_fixture.api().request_summary(SummaryRequest {
        group_id,
        target: alice_member,
    }))
    .expect("summary request should succeed");
    assert_eq!(
        summary.has_versions,
        VersionVector::initial(NonZeroUsize::new(2).expect("group has two members"))
            .with_update_applied(UpdateId {
                node_index: 0,
                version: 1,
            })
    );

    bob_fixture.listener().wait_for_data_change_count(1);
    assert_eq!(
        bob_fixture.listener().captured_data_changes(),
        vec![CapturedDataChange {
            rows: vec![CapturedRowChange::Upsert {
                row_id,
                title: "summary catch-up".to_owned(),
            }],
        }]
    );
}

#[test]
fn bootstrap_with_mismatched_trusted_sender_keys_does_not_install_group() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let alice_fixture = RuntimeTestFixture::load(
        app_alice_id(),
        &alice_member,
        [(dataset_id.clone(), title_schema_shared())],
        [bob_member.clone()],
    );
    let bob_store = wait_for_test_future(SqliteReplicationStore::in_memory_with_schema_sources(
        bob_member.clone(),
        [(dataset_id.clone(), title_schema_shared())],
    ))
    .expect("store should build");
    let bob_store = Arc::new(bob_store);
    wait_for_test_reply(provision_test_security(
        app_bob_id(),
        bob_store.as_ref(),
        &bob_member,
        std::iter::empty::<MemberIdentity>(),
    ))
    .expect("bob local security should provision");
    wait_for_test_reply(provision_test_trusted_public_keys(
        app_bob_id(),
        bob_store.as_ref(),
        alice_member.clone(),
        &test_public_member_keys(&probe_member()),
    ))
    .expect("bob mismatched trusted public keys should provision");
    let bob_fixture = RuntimeTestFixture::load_from_store(app_bob_id(), bob_store);

    alice_fixture.connect_direct_peer_routes(&bob_fixture);
    let group_id = wait_for_test_reply(alice_fixture.api().create_group(CreateGroupRequest {
        members: vec![alice_member, bob_member],
        group_schema: docs_group_schema(),
    }))
    .expect("create_group should succeed locally");
    bob_fixture.assert_group_never_installed(group_id);
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

fn charlie_member() -> Identifier {
    Identifier::from_array(CHARLIE_MEMBER_SEGMENTS)
}

fn probe_member() -> Identifier {
    Identifier::from_array(PROBE_MEMBER_SEGMENTS)
}

fn app_alice_id() -> Identifier {
    Identifier::from_array(APP_ALICE_SEGMENTS)
}

fn app_bob_id() -> Identifier {
    Identifier::from_array(APP_BOB_SEGMENTS)
}

fn app_charlie_id() -> Identifier {
    Identifier::from_array(APP_CHARLIE_SEGMENTS)
}

fn title_schema_shared() -> Arc<Schema> {
    Arc::new(STATIC_TITLE_SCHEMA.clone())
}

fn test_row_id(group_id: GroupId, dataset_id: DatasetId, raw: u128) -> RowId {
    RowId {
        group_id,
        dataset_id,
        row_key: RowKey(Uuid::from_u128(raw)),
    }
}

fn load_title_runtime_pair_with_trust(
    dataset_id: &DatasetId,
) -> (RuntimeTestFixture, RuntimeTestFixture) {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let alice_fixture = RuntimeTestFixture::load(
        app_alice_id(),
        &alice_member,
        [(dataset_id.clone(), title_schema_shared())],
        [bob_member.clone()],
    );
    let bob_fixture = RuntimeTestFixture::load_with_config(
        app_bob_id(),
        &bob_member,
        [(dataset_id.clone(), title_schema_shared())],
        [alice_member],
        ReplicationConfig {
            group_invitation_policy: GroupInvitationPolicy {
                creation: PolicyDecision::AutoAccept,
                ..GroupInvitationPolicy::default()
            },
            ..ReplicationConfig::default()
        },
    );
    (alice_fixture, bob_fixture)
}
