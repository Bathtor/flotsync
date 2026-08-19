//! Shared runtime-test fixtures and assertions.

use super::*;
use itertools::Itertools;

pub(super) type TestSqliteStore = SqliteStoreTestOwner<Arc<SqliteReplicationStore>>;
pub(super) type TestDeliveryRuntimeHost = SqliteStoreTestOwner<DeliveryRuntimeHost>;

pub(super) fn docs_dataset_id() -> DatasetId {
    DatasetId::try_from_static("docs").expect("dataset id should be valid")
}

pub(super) fn alice_member() -> MemberIdentity {
    MemberIdentity::from_array(ALICE_MEMBER_SEGMENTS)
}

pub(super) fn bob_member() -> MemberIdentity {
    MemberIdentity::from_array(BOB_MEMBER_SEGMENTS)
}

pub(super) fn carol_member() -> MemberIdentity {
    MemberIdentity::from_array(["app", "carol"])
}

pub(super) fn runtime_test_migration_id() -> MigrationId {
    MigrationId {
        old_group_id: GroupId(Uuid::from_u128(70_001)),
        new_group_id: GroupId(Uuid::from_u128(70_002)),
    }
}

pub(super) fn runtime_test_invitation_decision(group_id: GroupId) -> PendingGroupDecisionRecord {
    PendingGroupDecisionRecord::GroupInvitation(GroupInvitation::new_creation(
        group_id,
        vec![alice_member(), bob_member()],
        GroupSchema::default(),
        InitialSnapshot::Empty,
        Some("runtime docs".to_owned()),
        Some("runtime replay".to_owned()),
    ))
}

pub(super) fn runtime_test_migration_proposal_decision() -> PendingGroupDecisionRecord {
    let migration_id = runtime_test_migration_id();
    PendingGroupDecisionRecord::MigrationProposal(MigrationProposal {
        migration_id,
        final_versions: VersionVector::Full(PureVersionVector::from([4, 0])),
        proposed_members: vec![alice_member(), bob_member(), carol_member()],
        group_schema: GroupSchema::default(),
        initial_snapshot: InitialSnapshot::Empty,
        group_name: Some("runtime migration".to_owned()),
        message: None,
    })
}

pub(super) fn migration_proposal_decision(
    old_group_id: GroupId,
    new_group_id: GroupId,
) -> PendingGroupDecisionRecord {
    PendingGroupDecisionRecord::MigrationProposal(MigrationProposal {
        migration_id: MigrationId {
            old_group_id,
            new_group_id,
        },
        final_versions: VersionVector::initial(
            NonZeroUsize::new(2).expect("two old-group members"),
        ),
        proposed_members: vec![alice_member(), bob_member()],
        group_schema: GroupSchema::default(),
        initial_snapshot: InitialSnapshot::Empty,
        group_name: None,
        message: None,
    })
}

pub(super) fn test_public_keys(member: &MemberIdentity) -> PublicMemberKeys {
    test_public_member_keys(member)
}

/// Build the deterministic exact member-key id used by runtime store fixtures.
pub(super) fn test_member_key_id(member_id: MemberIdentity) -> MemberKeyId {
    let fingerprint = test_public_keys(&member_id).fingerprint();
    MemberKeyId {
        member_id,
        fingerprint,
    }
}

/// Convert ordered member identities into exact member-key groups for store fixtures.
pub(super) fn test_group_member_keys(members: Vec<MemberIdentity>) -> GroupMemberKeys {
    GroupMemberKeys::from_ordered_member_keys(members.into_iter().map(test_member_key_id))
        .expect("test group members should build")
}

pub(super) fn bootstrap_member_keys(
    entries: impl IntoIterator<Item = (MemberIdentity, BootstrapMemberKeyMessage)>,
) -> TrieMap<BootstrapMemberKeyMessage> {
    let mut member_keys = TrieMap::new();
    for (member_id, entry) in entries {
        member_keys.insert(member_id, entry);
    }
    member_keys
}

pub(super) fn bootstrap_member_key(
    public_keys: &PublicMemberKeys,
) -> (MemberIdentity, BootstrapMemberKeyMessage) {
    (
        public_keys.member_id().clone(),
        BootstrapMemberKeyMessage::from_public_keys(public_keys),
    )
}

pub(super) fn provision_test_security<S>(
    store: &S,
    local_member: &MemberIdentity,
    trusted_members: impl IntoIterator<Item = MemberIdentity>,
) where
    S: ReplicationStore,
{
    wait_for_test_reply(provision_shared_test_security(
        app_probe_id(),
        store,
        local_member,
        trusted_members,
    ))
    .expect("test security should provision");
}

pub(super) fn setup_api_test_security_secrets() -> ReplicationSecuritySecrets {
    ReplicationSecuritySecrets::from_unmanaged_store_secret(
        StoreSecretKeyId::from_bytes(*b"setup-api-test!!"),
        StoreSecretKey::from_bytes([91; 32]),
    )
}

pub(super) fn load_test_runtime_security<S>(
    store: Arc<S>,
    local_member: &MemberIdentity,
) -> DeliverySecurity
where
    S: ReplicationStore + 'static,
{
    let store: Arc<dyn ReplicationStore> = store;
    wait_for_test_reply(load_test_delivery_security(
        app_probe_id(),
        store.clone(),
        local_member,
    ))
    .expect("runtime security state should load")
}

pub(super) fn app_alice_id() -> ApplicationId {
    ApplicationId::from_array(APP_ALICE_SEGMENTS)
}

pub(super) fn app_bob_id() -> ApplicationId {
    ApplicationId::from_array(APP_BOB_SEGMENTS)
}

pub(super) fn app_probe_id() -> ApplicationId {
    ApplicationId::from_array(APP_PROBE_SEGMENTS)
}

pub(super) fn title_schema_shared() -> Arc<Schema> {
    Arc::new(STATIC_TITLE_SCHEMA.clone())
}

pub(super) fn docs_group_schema_from_schema<S>(schema: S) -> GroupSchema
where
    S: Into<SchemaSource>,
{
    GroupSchema::new(HashMap::from([(docs_dataset_id(), schema.into())]))
}

pub(super) fn docs_group_schema() -> GroupSchema {
    docs_group_schema_from_schema(title_schema_shared())
}

/// Coerce the title-schema `LazyLock` into the process-static reference expected by APIs.
pub(super) fn title_schema_static() -> &'static Schema {
    &STATIC_TITLE_SCHEMA
}

/// Coerce the title/note-schema `LazyLock` into the process-static reference expected by APIs.
pub(super) fn title_note_schema_static() -> &'static Schema {
    &STATIC_TITLE_NOTE_SCHEMA
}

/// Coerce the title/edit-count-schema `LazyLock` into the static reference expected by APIs.
pub(super) fn title_edit_count_schema_static() -> &'static Schema {
    &STATIC_TITLE_EDIT_COUNT_SCHEMA
}

pub(super) fn title_row_values(title: &str) -> RowValues {
    RowValues::try_from_fields(
        title_schema_static(),
        HashMap::from([("title".to_owned(), title.into())]),
    )
    .expect("test title row must match the title schema")
}

pub(super) fn title_note_schema_shared() -> Arc<Schema> {
    Arc::new(Schema::from_fields([
        Field::linear_string("title"),
        Field::linear_string("note"),
    ]))
}

pub(super) fn test_row_id(group_id: GroupId, dataset_id: DatasetId, raw: u128) -> RowId {
    RowId {
        group_id,
        dataset_id,
        row_key: RowKey(Uuid::from_u128(raw)),
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "Runtime fixture callers consistently transfer or clone member identities into store construction."
)]
pub(super) fn sqlite_store(local_member: MemberIdentity) -> TestSqliteStore {
    let provisioner = wait_for_test_future(SqliteReplicationStoreProvisioner::in_memory())
        .expect("store provisioner should build");
    wait_for_test_reply(provision_shared_test_identity(
        app_probe_id(),
        &provisioner,
        &local_member,
        [],
    ))
    .expect("local test identity should provision");
    let store = wait_for_test_future(provisioner.into_replication_store())
        .expect("provisioned store should activate");
    SqliteStoreTestOwner::from_store(Arc::new(store))
}

pub(super) fn store_pending_group_decision(
    store: &dyn ReplicationStore,
    record: PendingGroupDecisionRecord,
) {
    let group_record = inactive_group_record(
        record.group_id(),
        record.proposed_members().to_vec(),
        record.group_schema().clone(),
    );
    let (material, _) = group_record.into_parts();
    wait_for_test_future(async {
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("transaction should open");
        transaction
            .ensure_replication_group_material(material)
            .await
            .expect("pending decision material should store");
        transaction
            .upsert_pending_group_decision(record)
            .await
            .expect("pending decision should store");
        transaction
            .commit()
            .await
            .expect("transaction should commit");
    });
}

pub(super) fn store_pending_group_activation(
    store: &dyn ReplicationStore,
    record: PendingGroupActivationRecord,
) {
    let activation = record.clone().into_activation_record();
    let group_record = inactive_group_record(
        activation.group_id,
        activation.proposed_members,
        activation.group_schema,
    );
    let (material, _) = group_record.into_parts();
    wait_for_test_future(async {
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("transaction should open");
        transaction
            .ensure_replication_group_material(material)
            .await
            .expect("pending activation material should store");
        transaction
            .upsert_pending_group_activation(record)
            .await
            .expect("pending activation should store");
        transaction
            .commit()
            .await
            .expect("transaction should commit");
    });
}

pub(super) fn store_inactive_group_material(
    store: &dyn ReplicationStore,
    record: ReplicationGroupRecord,
) {
    let (material, _) = record.into_parts();
    wait_for_test_future(async {
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("transaction should open");
        transaction
            .ensure_replication_group_material(material)
            .await
            .expect("inactive group material should store");
        transaction
            .commit()
            .await
            .expect("transaction should commit");
    });
}

pub(super) fn load_group_material(
    store: &dyn ReplicationStore,
    group_id: GroupId,
) -> Option<ReplicationGroupMaterialRecord> {
    wait_for_test_future(async {
        let mut transaction = store
            .begin_read_transaction()
            .await
            .expect("transaction should open");
        transaction
            .load_replication_group_material(&group_id)
            .await
            .expect("group material should load")
    })
}

pub(super) fn load_pending_group_decisions(
    store: &dyn ReplicationStore,
) -> Vec<PendingGroupDecisionRecord> {
    wait_for_test_future(async {
        let mut transaction = store
            .begin_read_transaction()
            .await
            .expect("read transaction should open");
        let records = transaction
            .load_pending_group_decisions()
            .await
            .expect("pending decisions should load");
        transaction
            .release()
            .await
            .expect("read transaction should release");
        records
    })
}

pub(super) fn load_pending_group_activations(
    store: &dyn ReplicationStore,
) -> Vec<PendingGroupActivationRecord> {
    wait_for_test_future(async {
        let mut transaction = store
            .begin_read_transaction()
            .await
            .expect("read transaction should open");
        let records = transaction
            .load_pending_group_activations()
            .await
            .expect("pending activations should load");
        transaction
            .release()
            .await
            .expect("read transaction should release");
        records
    })
}

pub(super) fn replay_one_pending_invitation(
    store: Arc<SqliteReplicationStore>,
    group_id: GroupId,
) -> (Arc<ReplicationRuntime>, Box<dyn GroupInvitationResponder>) {
    let listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts(app_alice_id(), store, listener.clone());
    listener.wait_for_pending_group_event_count(1);
    let mut events = listener.take_pending_group_events();
    assert_eq!(events.len(), 1);
    match events.pop().expect("one pending event should replay") {
        CapturedPendingGroupEvent::GroupInvitation {
            invitation,
            respond,
        } => {
            assert_eq!(invitation.group_id, group_id);
            (runtime, respond)
        }
        CapturedPendingGroupEvent::MigrationProposal { .. } => {
            panic!("expected replayed group invitation")
        }
    }
}

pub(super) fn accept_one_creation_invitation(
    listener: &ListenerStub,
    group_id: GroupId,
    expected_members: &[MemberIdentity],
) {
    listener.wait_for_pending_group_event_count(1);
    let mut events = listener.take_pending_group_events();
    assert_eq!(events.len(), 1);
    match events.pop().expect("one pending event should arrive") {
        CapturedPendingGroupEvent::GroupInvitation {
            invitation,
            respond,
        } => {
            assert_eq!(invitation.group_id, group_id);
            assert_eq!(invitation.source, GroupInvitationSource::Creation);
            assert_eq!(invitation.proposed_members.as_slice(), expected_members);
            wait_for_test_reply(respond.accept())
                .expect("creation invitation accept should persist");
        }
        CapturedPendingGroupEvent::MigrationProposal { .. } => {
            panic!("expected creation group invitation")
        }
    }
}

pub(super) fn load_runtime_fixture(
    application_id: ApplicationId,
    local_member: MemberIdentity,
    application_schemas: &'static ApplicationSchemas,
) -> RuntimeFixture<SqliteReplicationStore> {
    let listener = Arc::new(ListenerStub::default());
    let store = sqlite_store(local_member);
    let local_member = wait_for_test_reply(store.local_member_identity())
        .expect("local member identity should load");
    let runtime = load_runtime_with_parts_and_application_schemas(
        application_id,
        application_schemas,
        store.clone(),
        listener.clone(),
    );
    RuntimeFixture {
        local_member,
        runtime,
        listener,
        store: store.clone(),
        sqlite_owner: store,
    }
}

#[test]
pub(super) fn runtime_api_returns_local_public_key_bundle() {
    let fixture = load_runtime_fixture(app_alice_id(), alice_member(), &ApplicationSchemas::EMPTY);

    let bundle = wait_for_test_reply(fixture.runtime.local_public_key_bundle())
        .expect("local public key bundle should load");

    assert_eq!(
        bundle,
        test_public_keys(&alice_member()).public_key_bundle()
    );
}

#[test]
pub(super) fn runtime_api_reports_known_member_key_trust() {
    let alice = alice_member();
    let fixture = load_runtime_fixture(app_alice_id(), alice.clone(), &ApplicationSchemas::EMPTY);
    let record = MemberPublicKeysRecord::from_public_keys(&test_public_keys(&alice));
    let mut transaction =
        wait_for_test_reply(fixture.store.begin_transaction()).expect("transaction should start");
    wait_for_test_reply(transaction.ensure_member_public_keys(record.clone()))
        .expect("local public keys should store");
    wait_for_test_reply(transaction.ensure_member_key_trust_evidence(
        MemberKeyTrustEvidenceRecord {
            key_id: record.key_id,
            evidence_kind: MemberKeyTrustEvidenceKind::LocalExplicitTrust,
        },
    ))
    .expect("local trust evidence should store");
    wait_for_test_reply(transaction.commit()).expect("transaction should commit");

    let report = wait_for_test_reply(fixture.runtime.known_member_keys())
        .expect("known-member keys should load");

    let local = report
        .members
        .iter()
        .find(|member| member.member_id == alice)
        .expect("local member keys should be reported");
    assert_eq!(local.keys.len(), 1);
    assert!(local.keys[0].trust.has_local_explicit_trust);
    wait_for_test_reply(fixture.runtime.shutdown()).expect("runtime should shut down");
}

#[test]
pub(super) fn runtime_api_assesses_and_records_public_key_bundle_feedback() {
    let bob = bob_member();
    let fixture = load_runtime_fixture(app_alice_id(), alice_member(), &ApplicationSchemas::EMPTY);
    let bundle = test_public_keys(&bob).public_key_bundle();
    let fingerprint = bundle.fingerprint();
    let key_id = MemberKeyId {
        member_id: bob.clone(),
        fingerprint,
    };

    let initial_report = wait_for_test_reply(fixture.runtime.assess_public_key_bundle(
        AssessPublicKeyBundleRequest {
            bundle: bundle.clone(),
            candidate_member_ids: HashSet::from([bob.clone()]),
            material_storage: PublicKeyBundleAssessmentStorage::ReadOnly,
        },
    ))
    .expect("assessment should succeed");

    assert!(initial_report.known_bindings.is_empty());
    assert!(
        initial_report.candidate_members[0]
            .binding_for_bundle
            .is_none()
    );

    let stored_report = wait_for_test_reply(fixture.runtime.assess_public_key_bundle(
        AssessPublicKeyBundleRequest {
            bundle: bundle.clone(),
            candidate_member_ids: HashSet::from([bob.clone()]),
            material_storage: PublicKeyBundleAssessmentStorage::StoreCandidateBindings,
        },
    ))
    .expect("assessment should store candidate key material");

    let stored_binding = stored_report
        .known_bindings
        .iter()
        .find(|binding| binding.key_id == key_id)
        .expect("stored binding should be reported");
    assert!(!stored_binding.trust.has_local_explicit_trust);

    wait_for_test_reply(fixture.runtime.record_public_key_bundle_feedback(
        RecordPublicKeyBundleFeedbackRequest {
            bundle: bundle.clone(),
            feedback: PublicKeyBundleFeedback::TrustMember {
                member_id: bob.clone(),
            },
        },
    ))
    .expect("feedback should store");

    let updated_report = wait_for_test_reply(fixture.runtime.assess_public_key_bundle(
        AssessPublicKeyBundleRequest {
            bundle,
            candidate_member_ids: HashSet::from([bob]),
            material_storage: PublicKeyBundleAssessmentStorage::ReadOnly,
        },
    ))
    .expect("updated assessment should succeed");

    let updated_binding = updated_report
        .known_bindings
        .iter()
        .find(|binding| binding.key_id == key_id)
        .expect("trusted binding should be reported");
    assert!(updated_binding.trust.has_local_explicit_trust);
}

pub(super) fn load_title_runtime_pair_with_trust() -> (
    RuntimeFixture<SqliteReplicationStore>,
    RuntimeFixture<SqliteReplicationStore>,
) {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let alice_fixture = load_runtime_fixture(
        app_alice_id(),
        alice_member.clone(),
        &TITLE_APPLICATION_SCHEMAS,
    );
    let bob_fixture =
        load_runtime_fixture(app_bob_id(), bob_member.clone(), &TITLE_APPLICATION_SCHEMAS);
    provision_test_security(
        alice_fixture.store.as_ref(),
        &alice_member,
        [bob_member.clone()],
    );
    provision_test_security(
        bob_fixture.store.as_ref(),
        &bob_member,
        [alice_member.clone()],
    );
    (alice_fixture, bob_fixture)
}

/// Load runtimes with mutual explicit trust and all-to-all route-establishment watches.
pub(super) fn load_mutually_trusted_runtime_mesh<const N: usize>(
    entries: &[(ApplicationId, MemberIdentity); N],
) -> (
    ReservedSocketLease,
    [RuntimeFixture<SqliteReplicationStore>; N],
) {
    let endpoint_lease = reserve_sockets(&[ReservedSocketKind::UdpSocket; N]);
    let stores = entries
        .each_ref()
        .map(|(_, member)| sqlite_store(member.clone()));

    for (local_index, ((_, local_member), store)) in entries.iter().zip(stores.iter()).enumerate() {
        let mut trusted_members = Vec::with_capacity(N.saturating_sub(1));
        for (peer_index, (_, peer_member)) in entries.iter().enumerate() {
            if peer_index != local_index {
                trusted_members.push(peer_member.clone());
            }
        }
        provision_test_security(store.as_ref(), local_member, trusted_members);
    }

    let fixtures = entries
        .iter()
        .zip(stores)
        .map(|((application_id, local_member), store)| {
            let listener = Arc::new(ListenerStub::default());
            let runtime =
                load_runtime_with_parts(application_id.clone(), store.clone(), listener.clone());
            RuntimeFixture {
                local_member: local_member.clone(),
                runtime,
                listener,
                store: store.clone(),
                sqlite_owner: store,
            }
        })
        .collect_array()
        .expect("entry and store arrays should retain their shared const-generic length");
    for (local_index, fixture) in fixtures.iter().enumerate() {
        let mut watches = Vec::with_capacity(N.saturating_sub(1));
        for (peer_index, peer_fixture) in fixtures.iter().enumerate() {
            if peer_index != local_index {
                watches.push(flotsync_routes::route_establishment::WatchedRoute {
                    route: flotsync_discovery::protocol::DiscoveryRoute::Udp(
                        peer_fixture.runtime.advertised_loopback_udp_addr_for_test(),
                    ),
                    expected_member: Some(peer_fixture.local_member.clone()),
                });
            }
        }
        fixture
            .runtime
            .replace_route_establishment_watches_for_test(watches);
    }
    (endpoint_lease, fixtures)
}

pub(super) fn start_host(local_member: &MemberIdentity) -> TestDeliveryRuntimeHost {
    let store = provisioned_sqlite_store(local_member);
    provision_test_security(store.as_ref(), local_member, []);
    let security = load_test_runtime_security(store.clone(), local_member);
    let listener = Arc::new(ListenerStub::default());
    let host = kompact::prelude::block_on(DeliveryRuntimeHost::start_with_runtime_config_toml(
        local_member,
        &ApplicationSchemas::EMPTY,
        store.clone(),
        listener,
        ReplicationConfig::default(),
        security,
        None,
    ))
    .expect("host should start");
    host.wait_for_runtime_startup();
    SqliteStoreTestOwner::new(host, store)
}

pub(super) fn load_runtime_with_parts<S>(
    application_id: ApplicationId,
    store: Arc<S>,
    listener: Arc<ListenerStub>,
) -> Arc<ReplicationRuntime>
where
    S: ReplicationStore + 'static,
{
    load_runtime_with_parts_and_application_schemas_and_config(
        application_id,
        &ApplicationSchemas::EMPTY,
        store,
        listener,
        ReplicationConfig::default(),
    )
}

pub(super) fn load_runtime_with_parts_and_application_schemas<S>(
    application_id: ApplicationId,
    application_schemas: &'static ApplicationSchemas,
    store: Arc<S>,
    listener: Arc<ListenerStub>,
) -> Arc<ReplicationRuntime>
where
    S: ReplicationStore + 'static,
{
    load_runtime_with_parts_and_application_schemas_and_config(
        application_id,
        application_schemas,
        store,
        listener,
        ReplicationConfig::default(),
    )
}

pub(super) fn load_runtime_with_parts_and_config<S>(
    application_id: ApplicationId,
    store: Arc<S>,
    listener: Arc<ListenerStub>,
    config: ReplicationConfig,
) -> Arc<ReplicationRuntime>
where
    S: ReplicationStore + 'static,
{
    load_runtime_with_parts_and_application_schemas_and_config(
        application_id,
        &ApplicationSchemas::EMPTY,
        store,
        listener,
        config,
    )
}

fn load_runtime_with_parts_and_application_schemas_and_config<S>(
    application_id: ApplicationId,
    application_schemas: &'static ApplicationSchemas,
    store: Arc<S>,
    listener: Arc<ListenerStub>,
    config: ReplicationConfig,
) -> Arc<ReplicationRuntime>
where
    S: ReplicationStore + 'static,
{
    let local_member = wait_for_test_reply(store.local_member_identity())
        .expect("local member identity should load");
    let security = load_test_runtime_security(store.clone(), &local_member);
    wait_for_test_reply(load_replication_runtime_typed_with_security_for_test(
        application_id,
        application_schemas,
        store,
        listener,
        config,
        security,
        None,
    ))
    .expect("runtime should load")
}

pub(super) fn load_runtime_with_parts_and_runtime_config_toml<S>(
    application_id: ApplicationId,
    application_schemas: &'static ApplicationSchemas,
    store: Arc<S>,
    listener: Arc<ListenerStub>,
    runtime_config_toml: &str,
) -> Arc<ReplicationRuntime>
where
    S: ReplicationStore + 'static,
{
    let local_member = wait_for_test_reply(store.local_member_identity())
        .expect("local member identity should load");
    let security = load_test_runtime_security(store.clone(), &local_member);
    wait_for_test_reply(load_replication_runtime_typed_with_security_for_test(
        application_id,
        application_schemas,
        store,
        listener,
        ReplicationConfig::default(),
        security,
        Some(runtime_config_toml),
    ))
    .expect("runtime should load")
}

pub(super) fn static_peer_route_toml(peer: &MemberIdentity, remote_addr: SocketAddr) -> String {
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

pub(super) fn local_endpoint_toml(local_addr: SocketAddr) -> String {
    format!(
        r#"
        [flotsync.io]
        bind-reuse-address = true

        [flotsync.replication.runtime]
        local-endpoint-bind-addr = "{local_addr}"
        "#,
    )
}

pub(super) fn persist_group_in_store<S>(store: &S, group: ReplicationGroupRecord)
where
    S: ReplicationStore + ?Sized,
{
    let mut transaction =
        wait_for_test_reply(store.begin_transaction()).expect("transaction should start");
    wait_for_test_reply(transaction.insert_replication_group(group)).expect("group should persist");
    wait_for_test_reply(transaction.commit()).expect("transaction should commit");
}

pub(super) fn persist_group_lifecycle(
    store: &dyn ReplicationStore,
    group_id: &GroupId,
    lifecycle: ReplicationGroupLifecycle,
) {
    let mut transaction =
        wait_for_test_reply(store.begin_transaction()).expect("transaction should start");
    wait_for_test_reply(transaction.update_replication_group_lifecycle(group_id, lifecycle))
        .expect("group lifecycle should store");
    wait_for_test_reply(transaction.commit()).expect("transaction should commit");
}

pub(super) fn inactive_group_record(
    group_id: GroupId,
    members: Vec<MemberIdentity>,
    group_schema: GroupSchema,
) -> ReplicationGroupRecord {
    let member_count = members.len();
    ReplicationGroupRecord {
        group_id,
        member_keys: test_group_member_keys(members),
        local_member_index: MemberIndex::new(0),
        group_schema,
        version_vector: VersionVector::initial(
            NonZeroUsize::new(member_count).expect("group should not be empty"),
        ),
        lifecycle: ReplicationGroupLifecycle::Open,
        security_material: current_slice_placeholder_group_security_material(group_id),
        ..Default::default()
    }
}

pub(super) fn metadata_initial_snapshot(
    group_id: GroupId,
    member_count: NonZeroUsize,
) -> InitialSnapshot {
    InitialSnapshot::Metadata(InitialSnapshotMetadata {
        primary_ref: SnapshotRef {
            group_id,
            versions: VersionVector::initial(member_count),
        },
        equivalent_refs: smallvec::SmallVec::default(),
        record_count: Some(1),
    })
}

pub(super) fn persist_group_membership_for_member<S>(
    store: &S,
    group_id: GroupId,
    members: Vec<MemberIdentity>,
    local_member_index: u32,
) where
    S: ReplicationStore + ?Sized,
{
    let member_count = members.len();
    persist_group_in_store(
        store,
        ReplicationGroupRecord {
            group_id,
            member_keys: test_group_member_keys(members),
            local_member_index: MemberIndex::new(local_member_index),
            group_schema: GroupSchema::default(),
            version_vector: VersionVector::initial(
                NonZeroUsize::new(member_count).expect("group should not be empty"),
            ),
            lifecycle: ReplicationGroupLifecycle::Open,
            security_material: current_slice_placeholder_group_security_material(group_id),
            ..Default::default()
        },
    );
}

pub(super) fn persist_alice_group_with_security_material(
    store: &dyn ReplicationStore,
    group_id: GroupId,
    security_material: EncryptedGroupSecurityMaterial,
) {
    persist_group_in_store(
        store,
        ReplicationGroupRecord {
            group_id,
            member_keys: test_group_member_keys(vec![alice_member()]),
            local_member_index: MemberIndex::new(0),
            group_schema: GroupSchema::default(),
            version_vector: VersionVector::initial(NonZeroUsize::new(1).unwrap()),
            lifecycle: ReplicationGroupLifecycle::Open,
            security_material,
            ..Default::default()
        },
    );
}

pub(super) fn security_load_error(
    error: LoadError,
    expected_application_id: &ApplicationId,
) -> LoadSecurityError {
    match error {
        LoadError::Security {
            application_id,
            source,
        } => {
            assert_eq!(&application_id, expected_application_id);
            *source
        }
        other => panic!("unexpected load error: {other:?}"),
    }
}

pub(super) fn load_persisted_group<S>(store: &S, group_id: GroupId) -> ReplicationGroupRecord
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

pub(super) fn load_persisted_groups<S>(store: &S) -> Vec<ReplicationGroupRecord>
where
    S: ReplicationStore + ?Sized,
{
    let mut transaction =
        wait_for_test_reply(store.begin_transaction()).expect("transaction should start");
    let groups =
        wait_for_test_reply(transaction.load_replication_groups()).expect("groups should load");
    wait_for_test_reply(transaction.commit()).expect("transaction should commit");
    groups
}

pub(super) fn load_persisted_update<S>(
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

pub(super) fn load_persisted_row_slice<S>(
    store: &S,
    group_id: GroupId,
    dataset_id: &DatasetId,
    row_keys: impl IntoIterator<Item = RowKey>,
) -> DatasetRowStateSlice
where
    S: ReplicationStore + ?Sized,
{
    let requested_row_keys: Vec<_> = row_keys.into_iter().collect();
    let mut requested_row_keys = requested_row_keys.iter();
    let mut transaction =
        wait_for_test_reply(store.begin_transaction()).expect("transaction should start");
    let group = wait_for_test_reply(transaction.load_replication_group(&group_id))
        .expect("group should load")
        .expect("group should exist");
    let schema = group
        .group_schema
        .schema(dataset_id)
        .expect("dataset should belong to the group");
    let row_slice = wait_for_test_reply(transaction.load_dataset_rows(
        GroupDatasetSchemaRef {
            group_id: &group_id,
            dataset_id,
            schema: schema.as_schema(),
        },
        &mut requested_row_keys,
    ))
    .expect("row slice should load");
    wait_for_test_reply(transaction.commit()).expect("transaction should commit");
    row_slice
}

pub(super) fn drain_snapshot_rows(
    runtime: &dyn ReplicationApi,
    request: SnapshotRowsRequest,
) -> Vec<CapturedRowChange> {
    let mut snapshot =
        wait_for_test_reply(runtime.snapshot_rows(request)).expect("snapshot should start");
    let mut rows = Vec::new();
    while let Some(batch) =
        wait_for_test_reply(snapshot.rows.next_batch()).expect("snapshot batch should load")
    {
        for row in batch.rows() {
            rows.push(
                CapturedRowChange::capture_snapshot(&row).expect("snapshot row should decode"),
            );
        }
    }
    rows
}

pub(super) fn snapshot_read_token(
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

pub(super) fn publish_changes(
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

pub(super) fn publish_direct_peer_routes(
    alice_runtime: &Arc<ReplicationRuntime>,
    alice_member: &MemberIdentity,
    bob_runtime: &Arc<ReplicationRuntime>,
    bob_member: &MemberIdentity,
) {
    alice_runtime.publish_direct_peer_route_for_test(
        bob_member.clone(),
        bob_runtime.advertised_loopback_udp_addr_for_test(),
    );
    bob_runtime.publish_direct_peer_route_for_test(
        alice_member.clone(),
        alice_runtime.advertised_loopback_udp_addr_for_test(),
    );
}

pub(super) fn title_update_message(
    group_id: GroupId,
    dataset_id: DatasetId,
    row_raw: u128,
    title: &str,
    update_id: UpdateId,
    read_versions: VersionVector,
) -> (RowId, UpdateMessage) {
    let row_id = test_row_id(group_id, dataset_id, row_raw);
    let mut source_dataset = LocalDataset::new(title_schema_static());
    let message = title_update_message_for_row(
        &mut source_dataset,
        &row_id,
        title,
        update_id,
        read_versions,
    );
    (row_id, message)
}

pub(super) fn title_update_message_for_row(
    source_dataset: &mut LocalDataset,
    row_id: &RowId,
    title: &str,
    update_id: UpdateId,
    read_versions: VersionVector,
) -> UpdateMessage {
    let operation = apply_local_upsert(
        source_dataset,
        row_id,
        crate::row_values! { "title" => title },
        update_id,
    )
    .expect("operation should build")
    .expect("operation should apply")
    .encoded_operation;
    UpdateMessage {
        group_id: row_id.group_id,
        update_id,
        read_versions,
        dataset_updates: vec![DatasetUpdateMessage {
            dataset_id: row_id.dataset_id.clone(),
            operations: vec![operation],
        }],
    }
}

pub(super) fn sort_captured_rows(rows: &mut [CapturedRowChange]) {
    rows.sort_by_key(|row| match row {
        CapturedRowChange::Upsert { row_id, .. } | CapturedRowChange::Delete { row_id } => {
            row_id.to_string()
        }
    });
}

pub(super) fn snapshot_string_field(
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
        for row in batch.rows() {
            if row.row_id() == row_id {
                return row
                    .get_field_value::<str>(field_name)
                    .expect("snapshot field should decode")
                    .into_owned();
            }
        }
    }
    panic!("snapshot row {row_id} should exist");
}

pub(super) fn wait_for_group_install(runtime: &Arc<ReplicationRuntime>, group_id: GroupId) {
    eventually(
        TEST_WAIT_TIMEOUT,
        || {
            runtime
                .membership_snapshot_for_test()
                .contains_group(&group_id)
        },
        "timed out waiting for runtime to install group",
    );
}
