use super::{
    component::ReplicationRuntimeComponent,
    errors::{
        ChangeGroupMembershipError,
        CreateGroupError,
        GroupInstallError,
        InboundDeliveryError,
        PublishChangesError,
    },
    group_state::{RuntimeGroupStateSnapshot, SharedGroupState},
    handle::{
        ReplicationRuntime,
        load_replication_runtime_typed_with_security_for_test,
        wait_for_test_reply,
    },
    host::{
        DeliveryRuntimeHost,
        DeliveryRuntimeHostTestExt,
        PreconfiguredPeerRoutesPublishMode,
        RuntimeHostError,
    },
    in_memory::{
        LocalDataset,
        apply_local_delete,
        apply_local_upsert,
        apply_rebased_local_upsert,
        validate_inbound_update_read_versions,
        validate_update_mapping,
    },
    load_replication_runtime,
    load_replication_runtime_with_runtime_config_toml,
};
use crate::{
    MAX_VERSION_VALUE,
    SqliteReplicationStore,
    SqliteReplicationStoreProvisioner,
    api::{
        ApiError,
        ApplicationSchemas,
        AuthorityScope,
        ChangeGroupMembershipRequest,
        CreateGroupRequest,
        DataChangeLineage,
        DatasetId,
        DatasetRowScanPage,
        DatasetRowStatePatch,
        DatasetRowStateSlice,
        DatasetRowStateTransitionBatch,
        DatasetUpdateRecord,
        EncryptedGroupSecurityMaterial,
        GroupDatasetSchemaRef,
        GroupInvitation,
        GroupInvitationPolicy,
        GroupInvitationResponder,
        GroupInvitationSource,
        GroupMemberKeys,
        GroupNameUpdate,
        GroupSchema,
        InitialDatasetValueRows,
        InitialGroupValueRows,
        InitialSnapshot,
        InitialSnapshotMetadata,
        InitialValueRow,
        ListenerError,
        ListenerExternalSnafu,
        LoadError,
        LoadSecurityError,
        LocalMemberPrivateKeysRecord,
        LocalStoreSecretProfile,
        MemberKeyId,
        MemberKeyTrustEvidenceKind,
        MemberKeyTrustEvidenceRecord,
        MemberKeyTrustEvidenceSet,
        MemberKeyTrustRequirement,
        MemberPublicKeysRecord,
        MigrationId,
        MigrationProposal,
        MigrationProposalResponder,
        PendingGroupActivationRecord,
        PendingGroupDecisionRecord,
        PendingGroupWorkKey,
        PermissionDenialReason,
        PolicyDecision,
        ProviderExternalSnafu,
        PublishChangesRequest,
        PublishReceipt,
        ReadToken,
        RejectionReason,
        ReplicationApi,
        ReplicationConfig,
        ReplicationEvent,
        ReplicationEventListener,
        ReplicationGroupLifecycle,
        ReplicationGroupMaterialRecord,
        ReplicationGroupRecord,
        ReplicationGroupSnapshot,
        ReplicationGroupView,
        ReplicationRowMetadata,
        ReplicationRowStateRecord,
        ReplicationSecuritySecrets,
        ReplicationStateRowBatch,
        ReplicationStore,
        ReplicationStoreReadTransaction,
        ReplicationStoreTransaction,
        ReplicationUpdateFilter,
        ReplicationUpdateRecord,
        RowChange,
        RowChangeBatch,
        RowChangeKind,
        RowId,
        RowKey,
        RowKeyIterator,
        RowMutation,
        STORE_EXTERNAL_UNCLASSIFIED_SNAFU,
        SchemaSource,
        SnapshotRef,
        SnapshotRowsRequest,
        SnapshotValueRow,
        StoreError,
        StoreSecretCryptoVersion,
        StoreSecretKeyId,
        SummaryRequest,
        TrustPolicy,
        WritableReplicationGroupVersionRecord,
        current_slice_placeholder_group_security_material,
        current_slice_placeholder_group_security_material_with_key_id,
        process_batches,
        security::{
            AssessPublicKeyBundleRequest,
            PublicKeyBundleAssessmentStorage,
            PublicKeyBundleFeedback,
            RecordPublicKeyBundleFeedbackRequest,
        },
    },
    codecs::messages::{
        BootstrapMemberKeyMessage,
        DatasetUpdateMessage,
        GroupSetupKey,
        GroupSetupMessage,
        UpdateBatchMessage,
        UpdateMessage,
    },
    delivery::{
        contracts::{
            ReliableDeliveryStore,
            StoredReliableDeliveryWork,
            StoredReliableDeliveryWorkMetadata,
        },
        security::{DeliverySecurity, DeliverySecurityError},
        shared::MessageId,
    },
    provision_local_identity,
    security_store::{SecurityStore, SecurityStoreError},
    test_support::{
        SqliteStoreTestOwner,
        load_test_delivery_security,
        provision_test_identity as provision_shared_test_identity,
        provision_test_security as provision_shared_test_security,
        provisioned_sqlite_store,
        test_public_member_keys,
        test_replication_security_secrets,
        wait_for_test_future,
    },
};
use flotsync_core::{
    ApplicationId,
    GroupId,
    MemberIdentity,
    MemberIndex,
    member::TrieMap,
    membership::GroupMembers,
    versions::{PureVersionVector, UpdateId, VersionVector},
};
use flotsync_data_types::{Field, RowOperations, RowValues, Schema, TableOperations};
use flotsync_io::test_support::{
    ReservedSocketKind,
    ReservedSocketLease,
    eventually,
    reserve_sockets,
};
use flotsync_security::{
    GROUP_CIPHER_SUITE_CHACHA20_POLY1305,
    KeyFingerprint,
    PublicMemberKeys,
    StoreSecretKey,
    install_local_store_secret_test_store,
};
use flotsync_utils::BoxFuture;
use futures_util::FutureExt;
use snafu::ResultExt;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::SocketAddr,
    num::NonZeroUsize,
    sync::{Arc, LazyLock, Mutex, mpsc},
    time::Duration,
};
use uuid::Uuid;

const TEST_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
const ALICE_MEMBER_SEGMENTS: [&str; 2] = ["alice", "laptop"];
const BOB_MEMBER_SEGMENTS: [&str; 2] = ["bob", "laptop"];
const PROBE_MEMBER_SEGMENTS: [&str; 2] = ["probe", "laptop"];
const APP_ALICE_SEGMENTS: [&str; 2] = ["app", "alice"];
const APP_BOB_SEGMENTS: [&str; 2] = ["app", "bob"];
const APP_PROBE_SEGMENTS: [&str; 2] = ["app", "probe"];
static STATIC_TITLE_SCHEMA: LazyLock<Schema> =
    LazyLock::new(|| Schema::from_fields([Field::linear_string("title")]));
static STATIC_TITLE_NOTE_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    Schema::from_fields([Field::linear_string("title"), Field::linear_string("note")])
});
static STATIC_TITLE_EDIT_COUNT_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    Schema::from_fields([
        Field::linear_string("title"),
        Field::monotonic_counter("edit_count"),
    ])
});
static TITLE_APPLICATION_SCHEMAS: LazyLock<ApplicationSchemas> = LazyLock::new(|| {
    ApplicationSchemas::try_from_lazy_entry("docs", &STATIC_TITLE_SCHEMA)
        .expect("title application schemas should build")
});
static TITLE_NOTE_APPLICATION_SCHEMAS: LazyLock<ApplicationSchemas> = LazyLock::new(|| {
    ApplicationSchemas::try_from_lazy_entry("docs", &STATIC_TITLE_NOTE_SCHEMA)
        .expect("title/note application schemas should build")
});
static TITLE_EDIT_COUNT_APPLICATION_SCHEMAS: LazyLock<ApplicationSchemas> = LazyLock::new(|| {
    ApplicationSchemas::try_from_lazy_entry("docs", &STATIC_TITLE_EDIT_COUNT_SCHEMA)
        .expect("title/edit-count application schemas should build")
});

struct RuntimeFixture<S> {
    local_member: MemberIdentity,
    runtime: Arc<ReplicationRuntime>,
    listener: Arc<ListenerStub>,
    store: Arc<S>,
    sqlite_owner: TestSqliteStore,
}

impl<S> Drop for RuntimeFixture<S> {
    fn drop(&mut self) {
        wait_for_test_reply(self.runtime.shutdown()).expect("test runtime should shut down");
        wait_for_test_future(self.sqlite_owner.close()).expect("runtime test store should close");
    }
}

/// State machine for failing the provider read opened after activation commits.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum ActivationReadFailure {
    /// Do not inject an activation-provider read failure.
    #[default]
    Disabled,
    /// Wait for the next transaction which commits an activation.
    AfterNextActivationCommit,
    /// Fail the next read transaction, then disable the injection.
    NextReadTransaction,
}

/// Shared failure controls consulted by the store and its delegated transactions.
#[derive(Default)]
struct FailingStoreControlState {
    fail_next_apply_dataset_row_patch: Option<DatasetId>,
    fail_next_activate_replication_group: bool,
    fail_after_next_pending_group_commit: bool,
    activation_read_failure: ActivationReadFailure,
}

/// Test-only store wrapper that can fail selected future writes while
/// delegating all stored state to the wrapped `SQLite` store.
struct FailingStore<S> {
    inner: Arc<S>,
    /// Whether delegated read transactions should hide all local-private key records.
    hide_local_private_keys: bool,
    /// Failure injection shared with every delegated transaction.
    control: Arc<Mutex<FailingStoreControlState>>,
}

impl<S> FailingStore<S> {
    fn new(inner: Arc<S>) -> Self {
        Self {
            inner,
            hide_local_private_keys: false,
            control: Arc::new(Mutex::new(FailingStoreControlState::default())),
        }
    }

    /// Configure this wrapper to emulate an unsupported store that exposes an identity without
    /// its corresponding local-private key record.
    fn with_hidden_local_private_keys(mut self) -> Self {
        self.hide_local_private_keys = true;
        self
    }

    fn fail_next_apply_dataset_row_patch(&self, dataset_id: DatasetId) {
        self.control
            .lock()
            .expect("failing store mutex must not be poisoned")
            .fail_next_apply_dataset_row_patch = Some(dataset_id);
    }

    fn fail_after_next_pending_group_commit(&self) {
        self.control
            .lock()
            .expect("failing store mutex must not be poisoned")
            .fail_after_next_pending_group_commit = true;
    }

    fn fail_next_activate_replication_group(&self) {
        self.control
            .lock()
            .expect("failing store mutex must not be poisoned")
            .fail_next_activate_replication_group = true;
    }

    /// Fail the provider read transaction opened after the next activation commit.
    fn fail_activation_read_after_next_commit(&self) {
        self.control
            .lock()
            .expect("failing store mutex must not be poisoned")
            .activation_read_failure = ActivationReadFailure::AfterNextActivationCommit;
    }
}

impl<S> ReplicationStore for FailingStore<S>
where
    S: ReplicationStore + 'static,
{
    fn local_member_identity(&self) -> BoxFuture<'_, Result<MemberIdentity, StoreError>> {
        self.inner.local_member_identity()
    }

    fn begin_transaction(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ReplicationStoreTransaction>, StoreError>> {
        let inner = self.inner.clone();
        let control = self.control.clone();
        let hide_local_private_keys = self.hide_local_private_keys;
        async move {
            let inner = inner.begin_transaction().await?;
            Ok(Box::new(FailingStoreTransaction {
                inner: Some(inner),
                control,
                hide_local_private_keys,
                provider_scan: None,
                wrote_pending_group_work: false,
                removed_pending_group_activation: false,
            }) as Box<dyn ReplicationStoreTransaction>)
        }
        .boxed()
    }

    fn begin_read_transaction(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ReplicationStoreReadTransaction>, StoreError>> {
        let should_fail = {
            let mut failure = self
                .control
                .lock()
                .expect("failing store mutex must not be poisoned");
            if failure.activation_read_failure == ActivationReadFailure::NextReadTransaction {
                failure.activation_read_failure = ActivationReadFailure::Disabled;
                true
            } else {
                false
            }
        };
        if should_fail {
            return async move {
                let source = std::io::Error::other(
                    "failing store intentionally failed activation provider read transaction",
                );
                Err::<Box<dyn ReplicationStoreReadTransaction>, _>(source)
                    .boxed()
                    .context(STORE_EXTERNAL_UNCLASSIFIED_SNAFU)
            }
            .boxed();
        }
        if !self.hide_local_private_keys {
            return self.inner.begin_read_transaction();
        }

        let inner = self.inner.clone();
        let control = self.control.clone();
        async move {
            let inner = inner.begin_transaction().await?;
            Ok(Box::new(FailingStoreTransaction {
                inner: Some(inner),
                hide_local_private_keys: true,
                control,
                provider_scan: None,
                wrote_pending_group_work: false,
                removed_pending_group_activation: false,
            }) as Box<dyn ReplicationStoreReadTransaction>)
        }
        .boxed()
    }
}

impl<S> ReliableDeliveryStore for FailingStore<S>
where
    S: ReplicationStore + 'static,
{
    fn load_reliable_delivery_work_metadata(
        &self,
    ) -> BoxFuture<'_, Result<Vec<StoredReliableDeliveryWorkMetadata>, StoreError>> {
        self.inner.load_reliable_delivery_work_metadata()
    }

    fn load_reliable_delivery_work(
        &self,
        message_id: MessageId,
    ) -> BoxFuture<'_, Result<Option<StoredReliableDeliveryWork>, StoreError>> {
        self.inner.load_reliable_delivery_work(message_id)
    }

    fn store_reliable_delivery_work(
        &self,
        work: StoredReliableDeliveryWork,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        self.inner.store_reliable_delivery_work(work)
    }

    fn remove_reliable_delivery_work(
        &self,
        message_id: MessageId,
    ) -> BoxFuture<'_, Result<bool, StoreError>> {
        self.inner.remove_reliable_delivery_work(message_id)
    }
}

struct FailingStoreTransaction {
    inner: Option<Box<dyn ReplicationStoreTransaction>>,
    /// Whether this transaction emulates an absent local-private key record.
    hide_local_private_keys: bool,
    /// Failure injection shared with the wrapping store.
    control: Arc<Mutex<FailingStoreControlState>>,
    /// Optional deterministic scan behaviour for replacement-provider tests.
    provider_scan: Option<ProviderTestScanBehaviour>,
    wrote_pending_group_work: bool,
    /// Whether this transaction removed a pending activation before committing.
    removed_pending_group_activation: bool,
}

impl ReplicationStoreReadTransaction for FailingStoreTransaction {
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

    fn load_writable_replication_group_versions(
        &mut self,
    ) -> BoxFuture<'_, Result<Vec<WritableReplicationGroupVersionRecord>, StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated reads")
            .load_writable_replication_group_versions()
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

    fn load_group_dataset_schema<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        dataset_id: &'a DatasetId,
    ) -> BoxFuture<'a, Result<Option<SchemaSource>, StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated reads")
            .load_group_dataset_schema(group_id, dataset_id)
    }

    fn load_local_member_identity(
        &mut self,
    ) -> BoxFuture<'_, Result<Option<MemberIdentity>, StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated reads")
            .load_local_member_identity()
    }

    fn load_local_member_private_keys<'a>(
        &'a mut self,
        member_id: &'a MemberIdentity,
    ) -> BoxFuture<'a, Result<Option<LocalMemberPrivateKeysRecord>, StoreError>> {
        if self.hide_local_private_keys {
            return futures_util::future::ready(Ok(None)).boxed();
        }
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated reads")
            .load_local_member_private_keys(member_id)
    }

    fn load_member_public_keys<'a>(
        &'a mut self,
        key_id: &'a MemberKeyId,
    ) -> BoxFuture<'a, Result<Option<MemberPublicKeysRecord>, StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated reads")
            .load_member_public_keys(key_id)
    }

    fn load_member_public_key_ids(
        &mut self,
    ) -> BoxFuture<'_, Result<Vec<MemberKeyId>, StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated reads")
            .load_member_public_key_ids()
    }

    fn load_member_public_keys_for_member<'a>(
        &'a mut self,
        member_id: &'a MemberIdentity,
    ) -> BoxFuture<'a, Result<Vec<MemberPublicKeysRecord>, StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated reads")
            .load_member_public_keys_for_member(member_id)
    }

    fn load_member_public_keys_for_fingerprint<'a>(
        &'a mut self,
        fingerprint: &'a KeyFingerprint,
    ) -> BoxFuture<'a, Result<Vec<MemberPublicKeysRecord>, StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated reads")
            .load_member_public_keys_for_fingerprint(fingerprint)
    }

    fn load_member_key_trust_evidence<'a>(
        &'a mut self,
        key_id: &'a MemberKeyId,
    ) -> BoxFuture<'a, Result<MemberKeyTrustEvidenceSet, StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated reads")
            .load_member_key_trust_evidence(key_id)
    }

    fn is_key_fingerprint_blocked<'a>(
        &'a mut self,
        fingerprint: &'a KeyFingerprint,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated reads")
            .is_key_fingerprint_blocked(fingerprint)
    }

    fn load_dataset_rows<'a>(
        &'a mut self,
        dataset: GroupDatasetSchemaRef<'a>,
        row_keys: &'a mut RowKeyIterator<'a>,
    ) -> BoxFuture<'a, Result<DatasetRowStateSlice, StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated reads")
            .load_dataset_rows(dataset, row_keys)
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

    fn scan_dataset_row_batch<'a>(
        &'a mut self,
        dataset: GroupDatasetSchemaRef<'a>,
        after: Option<RowKey>,
        limit: NonZeroUsize,
        output: &'a mut ReplicationStateRowBatch,
    ) -> BoxFuture<'a, Result<DatasetRowScanPage, StoreError>> {
        if let Some(provider_scan) = self.provider_scan.as_mut() {
            provider_scan
                .state
                .lock()
                .expect("provider transaction state mutex must not be poisoned")
                .row_requests
                .push(ProviderTestScanRequest {
                    dataset_id: dataset.dataset_id.clone(),
                    after,
                    limit,
                });
            let result = provider_scan
                .row_results
                .pop_front()
                .expect("provider test must supply one result per ordinary scan");
            let result = result.map(|result| {
                output.reuse_for_schema(dataset.schema);
                output.reserve_rows(result.rows.len());
                for row in result.rows {
                    let encoded = flotsync_messages::codecs::datamodel::encode_row_snapshot(
                        &row.snapshot,
                        dataset.schema,
                    )
                    .expect("provider test row must encode against its dataset schema");
                    let mut decoder =
                        flotsync_messages::snapshots::datamodel::ProtoSchemaSnapshotDecoder::new(
                            encoded,
                        )
                        .expect("provider test row must create a snapshot decoder");
                    output
                        .push_decoded_row(
                            ReplicationRowMetadata {
                                row_key: row.row_id,
                                tombstoned: row.tombstoned,
                                created_by: row.created_by,
                                last_changed_versions: row.last_changed_versions,
                            },
                            &mut decoder,
                        )
                        .expect("provider test row must decode into the reusable batch");
                }
                result.page
            });
            return futures_util::future::ready(result).boxed();
        }
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated reads")
            .scan_dataset_row_batch(dataset, after, limit, output)
    }

    fn scan_dataset_row_transition_batch<'a>(
        &'a mut self,
        previous_group: GroupDatasetSchemaRef<'a>,
        current_group: GroupDatasetSchemaRef<'a>,
        after: Option<RowKey>,
        limit: NonZeroUsize,
    ) -> BoxFuture<'a, Result<DatasetRowStateTransitionBatch, StoreError>> {
        if let Some(provider_scan) = self.provider_scan.as_mut() {
            assert_eq!(previous_group.dataset_id, current_group.dataset_id);
            provider_scan
                .state
                .lock()
                .expect("provider transaction state mutex must not be poisoned")
                .transition_requests
                .push(ProviderTestScanRequest {
                    dataset_id: previous_group.dataset_id.clone(),
                    after,
                    limit,
                });
            let result = provider_scan
                .transition_results
                .pop_front()
                .expect("provider test must supply one result per transition scan");
            return futures_util::future::ready(result).boxed();
        }
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated reads")
            .scan_dataset_row_transition_batch(previous_group, current_group, after, limit)
    }

    fn load_pending_group_decisions(
        &mut self,
    ) -> BoxFuture<'_, Result<Vec<PendingGroupDecisionRecord>, StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated reads")
            .load_pending_group_decisions()
    }

    fn load_pending_group_decision<'a>(
        &'a mut self,
        group_id: &'a GroupId,
    ) -> BoxFuture<'a, Result<Option<PendingGroupDecisionRecord>, StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated reads")
            .load_pending_group_decision(group_id)
    }

    fn load_pending_group_activations(
        &mut self,
    ) -> BoxFuture<'_, Result<Vec<PendingGroupActivationRecord>, StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated reads")
            .load_pending_group_activations()
    }

    fn load_pending_group_activation<'a>(
        &'a mut self,
        group_id: &'a GroupId,
    ) -> BoxFuture<'a, Result<Option<PendingGroupActivationRecord>, StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated reads")
            .load_pending_group_activation(group_id)
    }

    fn load_replication_group_material<'a>(
        &'a mut self,
        group_id: &'a GroupId,
    ) -> BoxFuture<'a, Result<Option<ReplicationGroupMaterialRecord>, StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated reads")
            .load_replication_group_material(group_id)
    }

    fn release(self: Box<Self>) -> BoxFuture<'static, Result<(), StoreError>> {
        let Self {
            inner,
            provider_scan,
            ..
        } = *self;
        if let Some(provider_scan) = provider_scan {
            provider_scan
                .state
                .lock()
                .expect("provider transaction state mutex must not be poisoned")
                .release_count += 1;
            return futures_util::future::ready(Ok(())).boxed();
        }
        inner
            .expect("failing store transaction must remain open until release")
            .rollback()
    }
}

impl ReplicationStoreTransaction for FailingStoreTransaction {
    fn insert_replication_group(
        &mut self,
        group: ReplicationGroupRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated writes")
            .insert_replication_group(group)
    }

    fn ensure_replication_group_material(
        &mut self,
        material: ReplicationGroupMaterialRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated writes")
            .ensure_replication_group_material(material)
    }

    fn activate_replication_group(
        &mut self,
        group_id: GroupId,
        version_vector: VersionVector,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        let should_fail = {
            let mut control = self
                .control
                .lock()
                .expect("failing store mutex must not be poisoned");
            std::mem::take(&mut control.fail_next_activate_replication_group)
        };
        if should_fail {
            return async move {
                let source =
                    std::io::Error::other("failing store intentionally rejected group activation");
                Err::<(), _>(source)
                    .boxed()
                    .context(STORE_EXTERNAL_UNCLASSIFIED_SNAFU)
            }
            .boxed();
        }
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated writes")
            .activate_replication_group(group_id, version_vector)
    }

    fn ensure_local_member_private_keys(
        &mut self,
        record: LocalMemberPrivateKeysRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated writes")
            .ensure_local_member_private_keys(record)
    }

    fn ensure_member_public_keys(
        &mut self,
        record: MemberPublicKeysRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated writes")
            .ensure_member_public_keys(record)
    }

    fn ensure_member_key_trust_evidence(
        &mut self,
        record: MemberKeyTrustEvidenceRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated writes")
            .ensure_member_key_trust_evidence(record)
    }

    fn ensure_blocked_key_fingerprint(
        &mut self,
        fingerprint: KeyFingerprint,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated writes")
            .ensure_blocked_key_fingerprint(fingerprint)
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

    fn update_replication_group_lifecycle<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        lifecycle: ReplicationGroupLifecycle,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated writes")
            .update_replication_group_lifecycle(group_id, lifecycle)
    }

    fn apply_dataset_row_patch<'a>(
        &'a mut self,
        dataset: GroupDatasetSchemaRef<'a>,
        patch: &'a DatasetRowStatePatch,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        let control = self.control.clone();
        async move {
            let should_fail = {
                let mut control = control
                    .lock()
                    .expect("failing store mutex must not be poisoned");
                if control.fail_next_apply_dataset_row_patch.as_ref() == Some(&patch.dataset_id) {
                    control.fail_next_apply_dataset_row_patch = None;
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
                let source = std::io::Error::other(format!(
                    "failing store intentionally failed dataset row patch apply for '{}'",
                    patch.dataset_id
                ));
                return Err::<(), _>(source)
                    .boxed()
                    .context(STORE_EXTERNAL_UNCLASSIFIED_SNAFU);
            }
            self.inner
                .as_mut()
                .expect("failing store transaction must remain open during delegated writes")
                .apply_dataset_row_patch(dataset, patch)
                .await
        }
        .boxed()
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

    fn upsert_pending_group_decision(
        &mut self,
        record: PendingGroupDecisionRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        self.wrote_pending_group_work = true;
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated writes")
            .upsert_pending_group_decision(record)
    }

    fn remove_pending_group_decision(
        &mut self,
        key: PendingGroupWorkKey,
    ) -> BoxFuture<'_, Result<bool, StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated writes")
            .remove_pending_group_decision(key)
    }

    fn upsert_pending_group_activation(
        &mut self,
        record: PendingGroupActivationRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        self.wrote_pending_group_work = true;
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated writes")
            .upsert_pending_group_activation(record)
    }

    fn remove_pending_group_activation(
        &mut self,
        key: PendingGroupWorkKey,
    ) -> BoxFuture<'_, Result<bool, StoreError>> {
        self.removed_pending_group_activation = true;
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated writes")
            .remove_pending_group_activation(key)
    }

    fn remove_inactive_replication_group_material(
        &mut self,
        group_id: GroupId,
    ) -> BoxFuture<'_, Result<bool, StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated writes")
            .remove_inactive_replication_group_material(group_id)
    }

    fn commit(self: Box<Self>) -> BoxFuture<'static, Result<(), StoreError>> {
        let Self {
            inner,
            control,
            wrote_pending_group_work,
            removed_pending_group_activation,
            ..
        } = *self;
        async move {
            inner
                .expect("failing store transaction must remain open until commit")
                .commit()
                .await?;
            let should_fail = {
                let mut control = control
                    .lock()
                    .expect("failing store mutex must not be poisoned");
                if removed_pending_group_activation
                    && control.activation_read_failure
                        == ActivationReadFailure::AfterNextActivationCommit
                {
                    control.activation_read_failure = ActivationReadFailure::NextReadTransaction;
                }
                wrote_pending_group_work
                    && std::mem::take(&mut control.fail_after_next_pending_group_commit)
            };
            if should_fail {
                let source = std::io::Error::other(
                    "failing store intentionally failed after committing pending group work",
                );
                return Err::<(), _>(source)
                    .boxed()
                    .context(STORE_EXTERNAL_UNCLASSIFIED_SNAFU);
            }
            Ok(())
        }
        .boxed()
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

enum CapturedPendingGroupEvent {
    GroupInvitation {
        invitation: GroupInvitation,
        respond: Box<dyn GroupInvitationResponder>,
    },
    MigrationProposal {
        proposal: MigrationProposal,
        respond: Box<dyn MigrationProposalResponder>,
    },
}

impl CapturedRowChange {
    fn capture(change: RowChange) -> Result<Self, ListenerError> {
        match change.change {
            RowChangeKind::Upsert { row_id, row, .. } => {
                let title = row
                    .get_field_value::<str>("title")
                    .boxed()
                    .context(ListenerExternalSnafu)?
                    .into_owned();
                Ok(Self::Upsert { row_id, title })
            }
            RowChangeKind::Delete { row_id } => Ok(Self::Delete { row_id }),
        }
    }

    fn capture_snapshot(row: &SnapshotValueRow<'_>) -> Result<Self, ListenerError> {
        let row_id = row.row_id().clone();
        if row.is_tombstoned() {
            return Ok(Self::Delete { row_id });
        }
        let title = row
            .get_field_value::<str>("title")
            .boxed()
            .context(ListenerExternalSnafu)?
            .into_owned();
        Ok(Self::Upsert { row_id, title })
    }
}

/// Captures and controls protected together by the listener test double.
struct ListenerStubState {
    data_changes: Vec<CapturedDataChange>,
    data_change_batch_sizes: Vec<Vec<usize>>,
    data_change_lineages: Vec<DataChangeLineage>,
    data_change_read_tokens: Vec<ReadToken>,
    pending_group_events: Vec<CapturedPendingGroupEvent>,
    migration_proposal_event_sizes: Vec<usize>,
    reject_pending_group_events: bool,
    rejected_pending_group_event_count: usize,
    buffered_events: mpsc::Receiver<CapturedDataChange>,
}

struct ListenerStub {
    state: Mutex<ListenerStubState>,
    buffered_event_tx: mpsc::Sender<CapturedDataChange>,
}

impl Default for ListenerStub {
    fn default() -> Self {
        let (buffered_event_tx, buffered_events) = mpsc::channel();
        Self {
            state: Mutex::new(ListenerStubState {
                data_changes: Vec::new(),
                data_change_batch_sizes: Vec::new(),
                data_change_lineages: Vec::new(),
                data_change_read_tokens: Vec::new(),
                pending_group_events: Vec::new(),
                migration_proposal_event_sizes: Vec::new(),
                reject_pending_group_events: false,
                rejected_pending_group_event_count: 0,
                buffered_events,
            }),
            buffered_event_tx,
        }
    }
}

impl ListenerStub {
    fn drain_buffered_events(&self) {
        let mut state = self
            .state
            .lock()
            .expect("listener state mutex must not be poisoned");
        while let Ok(change) = state.buffered_events.try_recv() {
            state.data_changes.push(change);
        }
    }

    fn wait_for_data_change_count(&self, count: usize) {
        eventually(
            TEST_WAIT_TIMEOUT,
            || {
                self.drain_buffered_events();
                self.state
                    .lock()
                    .expect("listener state mutex must not be poisoned")
                    .data_changes
                    .len()
                    >= count
            },
            format!("timed out waiting for {count} listener data-change events"),
        );
    }

    fn captured_data_changes(&self) -> Vec<CapturedDataChange> {
        self.drain_buffered_events();
        self.state
            .lock()
            .expect("listener state mutex must not be poisoned")
            .data_changes
            .clone()
    }

    fn captured_data_change_read_tokens(&self) -> Vec<ReadToken> {
        self.drain_buffered_events();
        self.state
            .lock()
            .expect("listener state mutex must not be poisoned")
            .data_change_read_tokens
            .clone()
    }

    fn captured_data_change_batch_sizes(&self) -> Vec<Vec<usize>> {
        self.drain_buffered_events();
        self.state
            .lock()
            .expect("listener state mutex must not be poisoned")
            .data_change_batch_sizes
            .clone()
    }

    fn captured_data_change_lineages(&self) -> Vec<DataChangeLineage> {
        self.drain_buffered_events();
        self.state
            .lock()
            .expect("listener state mutex must not be poisoned")
            .data_change_lineages
            .clone()
    }

    fn take_pending_group_events(&self) -> Vec<CapturedPendingGroupEvent> {
        std::mem::take(
            &mut self
                .state
                .lock()
                .expect("listener state mutex must not be poisoned")
                .pending_group_events,
        )
    }

    fn migration_proposal_event_sizes(&self) -> Vec<usize> {
        self.state
            .lock()
            .expect("listener state mutex must not be poisoned")
            .migration_proposal_event_sizes
            .clone()
    }

    fn wait_for_pending_group_event_count(&self, count: usize) {
        eventually(
            TEST_WAIT_TIMEOUT,
            || {
                self.state
                    .lock()
                    .expect("listener state mutex must not be poisoned")
                    .pending_group_events
                    .len()
                    >= count
            },
            format!("timed out waiting for {count} pending-group listener events"),
        );
    }

    fn reject_pending_group_events(&self) {
        self.state
            .lock()
            .expect("listener state mutex must not be poisoned")
            .reject_pending_group_events = true;
    }

    fn rejected_pending_group_event_count(&self) -> usize {
        self.state
            .lock()
            .expect("listener state mutex must not be poisoned")
            .rejected_pending_group_event_count
    }
}

impl ReplicationEventListener for ListenerStub {
    fn on_event(&self, event: ReplicationEvent) -> BoxFuture<'_, Result<(), ListenerError>> {
        async move {
            match event {
                ReplicationEvent::DataChanged {
                    lineage,
                    read_token,
                    mut rows,
                } => {
                    let mut captured_rows = Vec::new();
                    let mut batch_sizes = Vec::new();
                    process_batches::<RowChangeBatch>(rows.as_mut(), |batch| {
                        batch_sizes.push(batch.len());
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
                    {
                        let mut state = self
                            .state
                            .lock()
                            .expect("listener state mutex must not be poisoned");
                        state.data_change_batch_sizes.push(batch_sizes);
                        state.data_change_read_tokens.push(read_token);
                        state.data_change_lineages.push(lineage);
                    }
                    self.buffered_event_tx
                        .send(CapturedDataChange {
                            rows: captured_rows,
                        })
                        .expect("listener event channel must remain open while tests are running");
                }
                ReplicationEvent::GroupInvitation {
                    invitation,
                    respond,
                } => {
                    let mut state = self
                        .state
                        .lock()
                        .expect("listener state mutex must not be poisoned");
                    if state.reject_pending_group_events {
                        state.rejected_pending_group_event_count += 1;
                        return Err(ListenerError::Rejected {
                            message: "pending group event rejected by test listener".to_owned(),
                        });
                    }
                    state
                        .pending_group_events
                        .push(CapturedPendingGroupEvent::GroupInvitation {
                            invitation,
                            respond,
                        });
                }
                ReplicationEvent::MigrationProposals { proposals } => {
                    let mut state = self
                        .state
                        .lock()
                        .expect("listener state mutex must not be poisoned");
                    if state.reject_pending_group_events {
                        state.rejected_pending_group_event_count += 1;
                        return Err(ListenerError::Rejected {
                            message: "pending group event rejected by test listener".to_owned(),
                        });
                    }
                    state.migration_proposal_event_sizes.push(proposals.len());
                    for proposal in proposals {
                        state.pending_group_events.push(
                            CapturedPendingGroupEvent::MigrationProposal {
                                proposal: proposal.proposal,
                                respond: proposal.respond,
                            },
                        );
                    }
                }
            }
            Ok(())
        }
        .boxed()
    }
}

/// Deterministic scan results used by the existing store-transaction test wrapper.
struct ProviderTestScanBehaviour {
    /// Observations retained after the transaction is consumed.
    state: Arc<Mutex<ProviderTestTransactionState>>,
    /// Results returned by ordinary current-group scans.
    row_results: VecDeque<Result<ProviderTestRowScanResult, StoreError>>,
    /// Results returned by hosted transition scans.
    transition_results: VecDeque<Result<DatasetRowStateTransitionBatch, StoreError>>,
}

/// One deterministic ordinary scan page and the records decoded into its output batch.
pub(in crate::runtime) struct ProviderTestRowScanResult {
    /// Page metadata returned by the test transaction.
    pub(in crate::runtime) page: DatasetRowScanPage,
    /// Stored records decoded into the caller-owned state batch.
    pub(in crate::runtime) rows: Vec<ReplicationRowStateRecord>,
}

impl Drop for ProviderTestScanBehaviour {
    fn drop(&mut self) {
        self.state
            .lock()
            .expect("provider transaction state mutex must not be poisoned")
            .drop_count += 1;
    }
}

/// Build the existing store transaction wrapper with deterministic provider scans.
pub(in crate::runtime) fn provider_test_read_transaction(
    row_results: impl IntoIterator<Item = Result<ProviderTestRowScanResult, StoreError>>,
    transition_results: impl IntoIterator<Item = Result<DatasetRowStateTransitionBatch, StoreError>>,
) -> (
    Box<dyn ReplicationStoreReadTransaction>,
    Arc<Mutex<ProviderTestTransactionState>>,
) {
    let state = Arc::new(Mutex::new(ProviderTestTransactionState::default()));
    let transaction = FailingStoreTransaction {
        inner: None,
        hide_local_private_keys: false,
        control: Arc::new(Mutex::new(FailingStoreControlState::default())),
        provider_scan: Some(ProviderTestScanBehaviour {
            state: state.clone(),
            row_results: row_results.into_iter().collect(),
            transition_results: transition_results.into_iter().collect(),
        }),
        wrote_pending_group_work: false,
        removed_pending_group_activation: false,
    };
    (Box::new(transaction), state)
}

/// One scan request observed by replacement-provider tests.
#[derive(Debug, PartialEq, Eq)]
pub(in crate::runtime) struct ProviderTestScanRequest {
    /// Dataset requested by the provider.
    pub(in crate::runtime) dataset_id: DatasetId,
    /// Exclusive row-key lower bound supplied to storage.
    pub(in crate::runtime) after: Option<RowKey>,
    /// Requested page limit.
    pub(in crate::runtime) limit: NonZeroUsize,
}

/// Shared lifecycle and request observations for a provider test transaction.
#[derive(Default)]
pub(in crate::runtime) struct ProviderTestTransactionState {
    /// Ordinary current-group scans in call order.
    pub(in crate::runtime) row_requests: Vec<ProviderTestScanRequest>,
    /// Hosted transition scans in call order.
    pub(in crate::runtime) transition_requests: Vec<ProviderTestScanRequest>,
    /// Number of explicit release calls.
    pub(in crate::runtime) release_count: usize,
    /// Number of transaction values dropped.
    pub(in crate::runtime) drop_count: usize,
}

mod changes;
mod delivery;
mod fixtures;
mod groups;
mod host;
mod setup;

use fixtures::*;
