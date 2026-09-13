use super::{
    catch_up_manager::CatchUpManagerComponent,
    component::ReplicationRuntimeComponent,
    errors::{
        ChangeGroupMembershipError,
        CreateGroupError,
        GroupInstallError,
        InboundDeliveryError,
        PublishChangesError,
        RuntimeStartupError,
    },
    group_state::{RuntimeGroupStateSnapshot, SharedGroupState},
    handle::{
        ReplicationRuntime,
        ReplicationRuntimeBuilder,
        ReplicationRuntimeLoad,
        TypedReplicationRuntimeLoad,
        load_replication_runtime_typed_with_observed_startup_for_test,
        load_replication_runtime_typed_with_security_for_test,
        wait_for_test_reply,
    },
    host::{
        DeliveryRuntimeHost,
        DeliveryRuntimeHostPrepareArgs,
        DeliveryRuntimeHostTestExt,
        RuntimeHostError,
        StartupEventPolicy,
    },
    in_memory::{
        LocalDataset,
        apply_local_delete,
        apply_local_upsert,
        apply_rebased_local_upsert,
        validate_inbound_update_read_versions,
        validate_update_mapping,
    },
    synchronisation::prepare_application_state,
};
use crate::{
    MAX_VERSION_VALUE,
    SqliteReplicationStore,
    SqliteReplicationStoreProvisioner,
    api::{
        ApiError,
        ApplicationReadToken,
        ApplicationSchemas,
        AuthorityScope,
        ChangeGroupMembershipRequest,
        CreateGroupRequest,
        DataChangeLineage,
        DatasetId,
        DatasetRowScanPage,
        DatasetRowStatePatch,
        DatasetRowStateSlice,
        DatasetRowStateTransitionPage,
        DatasetUpdateRecord,
        EncryptedGroupSecurityMaterial,
        GroupDatasetSchemaRef,
        GroupInvitation,
        GroupInvitationPolicy,
        GroupInvitationResponder,
        GroupInvitationSource,
        GroupMemberKeys,
        GroupNameUpdate,
        GroupReadToken,
        GroupSchema,
        InMemoryStateRowView,
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
        ReplicationRowStateSnapshot,
        ReplicationSecuritySecrets,
        ReplicationStateRowBatch,
        ReplicationStateRowTransitionBatch,
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
        RowProviderError,
        STORE_EXTERNAL_UNCLASSIFIED_SNAFU,
        SchemaSource,
        SnapshotRef,
        StoreError,
        StoreErrorClass,
        StoreErrorClassification,
        StoreErrorResolution,
        StoreErrorScope,
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
        test_group_key,
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
static TWO_TITLE_APPLICATION_SCHEMAS: LazyLock<ApplicationSchemas> = LazyLock::new(|| {
    ApplicationSchemas::try_from_lazy_entries([
        ("docs", &STATIC_TITLE_SCHEMA),
        ("notes", &STATIC_TITLE_SCHEMA),
    ])
    .expect("two-dataset title application schemas should build")
});
static TITLE_NOTE_APPLICATION_SCHEMAS: LazyLock<ApplicationSchemas> = LazyLock::new(|| {
    ApplicationSchemas::try_from_lazy_entry("docs", &STATIC_TITLE_NOTE_SCHEMA)
        .expect("title/note application schemas should build")
});
static TITLE_EDIT_COUNT_APPLICATION_SCHEMAS: LazyLock<ApplicationSchemas> = LazyLock::new(|| {
    ApplicationSchemas::try_from_lazy_entry("docs", &STATIC_TITLE_EDIT_COUNT_SCHEMA)
        .expect("title/edit-count application schemas should build")
});

/// Find one present positional row in a point-load result.
pub(in crate::runtime) fn loaded_state_row<'a>(
    slice: &'a DatasetRowStateSlice,
    row_key: &RowKey,
) -> Option<InMemoryStateRowView<'a, ReplicationRowMetadata, UpdateId>> {
    slice
        .state_rows
        .rows()
        .find(|row| &row.metadata().row_key == row_key)
}

/// Owned row-state test fixture converted into positional store outputs.
#[derive(Clone, Debug, PartialEq)]
pub(in crate::runtime) struct ReplicationRowStateFixture {
    /// Stable row key in the fixture dataset.
    pub(in crate::runtime) row_id: RowKey,
    /// Complete owned state snapshot.
    pub(in crate::runtime) snapshot: ReplicationRowStateSnapshot,
    /// Whether the row is retained as a tombstone.
    pub(in crate::runtime) tombstoned: bool,
    /// Update which created the row, when known.
    pub(in crate::runtime) created_by: Option<UpdateId>,
    /// Causal frontier of the last state change.
    pub(in crate::runtime) last_changed_versions: VersionVector,
}

/// Owned transition fixture converted into one borrowed transition view.
#[derive(Clone, Debug, PartialEq)]
pub(in crate::runtime) struct DatasetRowStateTransitionFixture {
    /// Shared row key.
    pub(in crate::runtime) row_key: RowKey,
    /// Previous occurrence, when present.
    pub(in crate::runtime) previous: Option<ReplicationRowStateFixture>,
    /// Current occurrence, when present.
    pub(in crate::runtime) current: Option<ReplicationRowStateFixture>,
}

/// Owned transition-page fixture used by provider tests.
#[derive(Clone, Debug, PartialEq)]
pub(in crate::runtime) struct DatasetRowStateTransitionPageFixture {
    /// Previous group identifier.
    pub(in crate::runtime) previous_group_id: GroupId,
    /// Current group identifier.
    pub(in crate::runtime) current_group_id: GroupId,
    /// Shared dataset identifier.
    pub(in crate::runtime) dataset_id: DatasetId,
    /// Whether the previous dataset exists.
    pub(in crate::runtime) previous_dataset_exists: bool,
    /// Whether the current dataset exists.
    pub(in crate::runtime) current_dataset_exists: bool,
    /// Owned row fixtures for this page.
    pub(in crate::runtime) rows: Vec<DatasetRowStateTransitionFixture>,
    /// Exclusive lower bound for the next page.
    pub(in crate::runtime) next_after: Option<RowKey>,
}

/// One retained-history query observed by the failure-injecting store wrapper.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ReplicationUpdateLoadRequest {
    /// Group whose update history was queried.
    group_id: GroupId,
    /// History subset requested by the caller.
    filter: ReplicationUpdateFilter,
    /// Maximum records requested from the store.
    limit: Option<NonZeroUsize>,
}

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

/// State machine for failing one selected future read transaction.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum ReadTransactionFailure {
    /// Do not inject a read-transaction failure.
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
    read_transaction_failure: ReadTransactionFailure,
    fail_next_read_release: bool,
    read_release_count: usize,
    snapshot_scan_failures: VecDeque<StoreErrorClassification>,
    snapshot_scan_requests: Vec<ProviderTestScanRequest>,
    replication_update_load_failures: VecDeque<StoreErrorClassification>,
    replication_update_load_count: usize,
    replication_update_load_requests: Vec<ReplicationUpdateLoadRequest>,
    /// Optional deterministic replacement for all-groups reads in malformed-store tests.
    loaded_groups_override: Option<Vec<ReplicationGroupRecord>>,
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
    /// Acquire shared failure-injection state with the store's poison invariant.
    fn lock_control(
        control: &Mutex<FailingStoreControlState>,
    ) -> std::sync::MutexGuard<'_, FailingStoreControlState> {
        control
            .lock()
            .expect("failing store mutex must not be poisoned")
    }

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
        Self::lock_control(&self.control).fail_next_apply_dataset_row_patch = Some(dataset_id);
    }

    fn fail_after_next_pending_group_commit(&self) {
        Self::lock_control(&self.control).fail_after_next_pending_group_commit = true;
    }

    fn fail_next_activate_replication_group(&self) {
        Self::lock_control(&self.control).fail_next_activate_replication_group = true;
    }

    /// Fail the next read transaction opened after this call.
    fn fail_next_read_transaction(&self) {
        Self::lock_control(&self.control).read_transaction_failure =
            ReadTransactionFailure::NextReadTransaction;
    }

    /// Fail the provider read transaction opened after the next activation commit.
    fn fail_activation_read_after_next_commit(&self) {
        Self::lock_control(&self.control).read_transaction_failure =
            ReadTransactionFailure::AfterNextActivationCommit;
    }

    /// Fail the next explicit read-transaction release.
    fn fail_next_read_release(&self) {
        Self::lock_control(&self.control).fail_next_read_release = true;
    }

    /// Fail the next snapshot scan with the selected store classification.
    fn fail_next_snapshot_scan(&self, classification: StoreErrorClassification) {
        Self::lock_control(&self.control)
            .snapshot_scan_failures
            .push_back(classification);
    }

    /// Fail the next replication-update range load with the selected classification.
    fn fail_next_replication_update_load(&self, classification: StoreErrorClassification) {
        Self::lock_control(&self.control)
            .replication_update_load_failures
            .push_back(classification);
    }

    /// Return how many replication-update range loads passed through this wrapper.
    fn replication_update_load_count(&self) -> usize {
        Self::lock_control(&self.control).replication_update_load_count
    }

    /// Return every retained-history query observed by this wrapper.
    fn replication_update_load_requests(&self) -> Vec<ReplicationUpdateLoadRequest> {
        Self::lock_control(&self.control)
            .replication_update_load_requests
            .clone()
    }

    /// Return every ordinary snapshot scan request observed by this wrapper.
    fn snapshot_scan_requests(&self) -> Vec<ProviderTestScanRequest> {
        Self::lock_control(&self.control)
            .snapshot_scan_requests
            .clone()
    }

    /// Return the number of delegated read transactions explicitly released.
    fn read_release_count(&self) -> usize {
        Self::lock_control(&self.control).read_release_count
    }

    /// Return the supplied records from every delegated all-groups read.
    fn override_loaded_groups(&self, groups: Vec<ReplicationGroupRecord>) {
        Self::lock_control(&self.control).loaded_groups_override = Some(groups);
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
                inner: Some(FailingStoreTransactionInner::Write(inner)),
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
            let mut failure = Self::lock_control(&self.control);
            if failure.read_transaction_failure == ReadTransactionFailure::NextReadTransaction {
                failure.read_transaction_failure = ReadTransactionFailure::Disabled;
                true
            } else {
                false
            }
        };
        if should_fail {
            return async move {
                let source =
                    std::io::Error::other("failing store intentionally failed read transaction");
                Err::<Box<dyn ReplicationStoreReadTransaction>, _>(source)
                    .boxed()
                    .context(STORE_EXTERNAL_UNCLASSIFIED_SNAFU)
            }
            .boxed();
        }
        let inner = self.inner.clone();
        let control = self.control.clone();
        let hide_local_private_keys = self.hide_local_private_keys;
        async move {
            let inner = inner.begin_read_transaction().await?;
            Ok(Box::new(FailingStoreTransaction {
                inner: Some(FailingStoreTransactionInner::Read(inner)),
                hide_local_private_keys,
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

/// Actual store transaction kind retained by the shared failure-injection wrapper.
enum FailingStoreTransactionInner {
    /// Read-only transaction returned by `begin_read_transaction`.
    Read(Box<dyn ReplicationStoreReadTransaction>),
    /// Read-write transaction returned by `begin_transaction`.
    Write(Box<dyn ReplicationStoreTransaction>),
}

impl FailingStoreTransactionInner {
    /// Return the contained write transaction.
    fn write_mut(&mut self) -> &mut dyn ReplicationStoreTransaction {
        match self {
            Self::Write(transaction) => transaction.as_mut(),
            Self::Read(_) => panic!("a read-only transaction cannot perform delegated writes"),
        }
    }

    /// Consume and return the contained write transaction.
    fn into_write(self) -> Box<dyn ReplicationStoreTransaction> {
        match self {
            Self::Write(transaction) => transaction,
            Self::Read(_) => panic!("a read-only transaction cannot complete as a write"),
        }
    }

    /// Release either transaction kind using its matching terminal operation.
    fn release(self) -> BoxFuture<'static, Result<(), StoreError>> {
        match self {
            Self::Read(transaction) => transaction.release(),
            Self::Write(transaction) => transaction.rollback(),
        }
    }
}

impl std::ops::Deref for FailingStoreTransactionInner {
    type Target = dyn ReplicationStoreReadTransaction;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Read(transaction) => transaction.as_ref(),
            Self::Write(transaction) => transaction.as_ref(),
        }
    }
}

impl std::ops::DerefMut for FailingStoreTransactionInner {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Read(transaction) => transaction.as_mut(),
            Self::Write(transaction) => transaction.as_mut(),
        }
    }
}

/// Read or write transaction wrapper sharing the store's failure controls.
struct FailingStoreTransaction {
    /// Actual store transaction, absent only in deterministic provider unit tests.
    inner: Option<FailingStoreTransactionInner>,
    /// Whether this transaction emulates an absent local-private key record.
    hide_local_private_keys: bool,
    /// Failure injection shared with the wrapping store.
    control: Arc<Mutex<FailingStoreControlState>>,
    /// Optional deterministic scan behaviour for replacement-provider tests.
    provider_scan: Option<ProviderTestScanBehaviour>,
    /// Whether this transaction wrote pending group work before committing.
    wrote_pending_group_work: bool,
    /// Whether this transaction removed a pending activation before committing.
    removed_pending_group_activation: bool,
}

impl FailingStoreTransaction {
    /// Acquire shared failure-injection state with the transaction's poison invariant.
    fn lock_control(
        control: &Mutex<FailingStoreControlState>,
    ) -> std::sync::MutexGuard<'_, FailingStoreControlState> {
        control
            .lock()
            .expect("failing store mutex must not be poisoned")
    }

    /// Return the delegated write transaction; read wrappers never use this path.
    fn write_transaction(&mut self) -> &mut dyn ReplicationStoreTransaction {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated writes")
            .write_mut()
    }

    /// Consume the delegated write transaction; read wrappers never use this path.
    fn into_write_transaction(self) -> Box<dyn ReplicationStoreTransaction> {
        self.inner
            .expect("failing store transaction must remain open until completion")
            .into_write()
    }
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
        let groups = Self::lock_control(&self.control)
            .loaded_groups_override
            .clone();
        if let Some(groups) = groups {
            return futures_util::future::ready(Ok(groups)).boxed();
        }
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
        let injected_failure = {
            let mut control = Self::lock_control(&self.control);
            control.replication_update_load_count += 1;
            control
                .replication_update_load_requests
                .push(ReplicationUpdateLoadRequest {
                    group_id: *group_id,
                    filter,
                    limit,
                });
            control.replication_update_load_failures.pop_front()
        };
        match injected_failure {
            Some(classification) => {
                let source = std::io::Error::other(
                    "failing store intentionally failed one update range load",
                );
                futures_util::future::ready(Err(StoreError::new(classification, source))).boxed()
            }
            None => self
                .inner
                .as_mut()
                .expect("failing store transaction must remain open during delegated reads")
                .load_replication_updates(group_id, filter, limit),
        }
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
        let injected_failure = {
            let mut control = Self::lock_control(&self.control);
            control
                .snapshot_scan_requests
                .push(ProviderTestScanRequest {
                    dataset_id: dataset.dataset_id.clone(),
                    after,
                    limit,
                });
            control.snapshot_scan_failures.pop_front()
        };
        if let Some(classification) = injected_failure {
            let source =
                std::io::Error::other("failing store intentionally failed one snapshot scan");
            return futures_util::future::ready(Err(StoreError::new(classification, source)))
                .boxed();
        }
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
        output: &'a mut ReplicationStateRowTransitionBatch,
    ) -> BoxFuture<'a, Result<DatasetRowStateTransitionPage, StoreError>> {
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
            let result = result.map(|result| {
                output.reuse_for_schemas(previous_group.schema, current_group.schema);
                output.reserve_rows(result.rows.len());
                for transition in result.rows {
                    let previous_index = transition.previous.map(|row| {
                        assert_eq!(row.row_id, transition.row_key);
                        push_provider_test_row(
                            output.previous_rows_mut(),
                            previous_group.schema,
                            row,
                        )
                    });
                    let current_index = transition.current.map(|row| {
                        assert_eq!(row.row_id, transition.row_key);
                        push_provider_test_row(output.current_rows_mut(), current_group.schema, row)
                    });
                    output.push_alignment(previous_index, current_index);
                }
                DatasetRowStateTransitionPage {
                    previous_group_id: result.previous_group_id,
                    current_group_id: result.current_group_id,
                    dataset_id: result.dataset_id,
                    previous_dataset_exists: result.previous_dataset_exists,
                    current_dataset_exists: result.current_dataset_exists,
                    next_after: result.next_after,
                }
            });
            return futures_util::future::ready(result).boxed();
        }
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated reads")
            .scan_dataset_row_transition_batch(previous_group, current_group, after, limit, output)
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
            control,
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
        let should_fail = {
            let mut control = Self::lock_control(&control);
            control.read_release_count += 1;
            std::mem::take(&mut control.fail_next_read_release)
        };
        if should_fail {
            let source = std::io::Error::other(
                "failing store intentionally failed read transaction release",
            );
            let result = Err::<(), _>(source)
                .boxed()
                .context(STORE_EXTERNAL_UNCLASSIFIED_SNAFU);
            return futures_util::future::ready(result).boxed();
        }
        inner
            .expect("failing store transaction must remain open until release")
            .release()
    }
}

impl ReplicationStoreTransaction for FailingStoreTransaction {
    fn insert_replication_group(
        &mut self,
        group: ReplicationGroupRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        self.write_transaction().insert_replication_group(group)
    }

    fn ensure_replication_group_material(
        &mut self,
        material: ReplicationGroupMaterialRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        self.write_transaction()
            .ensure_replication_group_material(material)
    }

    fn activate_replication_group(
        &mut self,
        group_id: GroupId,
        version_vector: VersionVector,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        let should_fail = {
            let mut control = Self::lock_control(&self.control);
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
        self.write_transaction()
            .activate_replication_group(group_id, version_vector)
    }

    fn ensure_local_member_private_keys(
        &mut self,
        record: LocalMemberPrivateKeysRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        self.write_transaction()
            .ensure_local_member_private_keys(record)
    }

    fn ensure_member_public_keys(
        &mut self,
        record: MemberPublicKeysRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        self.write_transaction().ensure_member_public_keys(record)
    }

    fn ensure_member_key_trust_evidence(
        &mut self,
        record: MemberKeyTrustEvidenceRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        self.write_transaction()
            .ensure_member_key_trust_evidence(record)
    }

    fn ensure_blocked_key_fingerprint(
        &mut self,
        fingerprint: KeyFingerprint,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        self.write_transaction()
            .ensure_blocked_key_fingerprint(fingerprint)
    }

    fn update_replication_group_version_vector<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        version_vector: VersionVector,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        self.write_transaction()
            .update_replication_group_version_vector(group_id, version_vector)
    }

    fn update_replication_group_lifecycle<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        lifecycle: ReplicationGroupLifecycle,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        self.write_transaction()
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
                let mut control = Self::lock_control(&control);
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
                    .into_write()
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
            self.write_transaction()
                .apply_dataset_row_patch(dataset, patch)
                .await
        }
        .boxed()
    }

    fn append_replication_update(
        &mut self,
        update: ReplicationUpdateRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        self.write_transaction().append_replication_update(update)
    }

    fn mark_replication_update_applied<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        update_id: UpdateId,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        self.write_transaction()
            .mark_replication_update_applied(group_id, update_id)
    }

    fn upsert_pending_group_decision(
        &mut self,
        record: PendingGroupDecisionRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        self.wrote_pending_group_work = true;
        self.write_transaction()
            .upsert_pending_group_decision(record)
    }

    fn remove_pending_group_decision(
        &mut self,
        key: PendingGroupWorkKey,
    ) -> BoxFuture<'_, Result<bool, StoreError>> {
        self.write_transaction().remove_pending_group_decision(key)
    }

    fn upsert_pending_group_activation(
        &mut self,
        record: PendingGroupActivationRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        self.wrote_pending_group_work = true;
        self.write_transaction()
            .upsert_pending_group_activation(record)
    }

    fn remove_pending_group_activation(
        &mut self,
        key: PendingGroupWorkKey,
    ) -> BoxFuture<'_, Result<bool, StoreError>> {
        self.removed_pending_group_activation = true;
        self.write_transaction()
            .remove_pending_group_activation(key)
    }

    fn remove_inactive_replication_group_material(
        &mut self,
        group_id: GroupId,
    ) -> BoxFuture<'_, Result<bool, StoreError>> {
        self.write_transaction()
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
                .into_write()
                .commit()
                .await?;
            let should_fail = {
                let mut control = Self::lock_control(&control);
                if removed_pending_group_activation
                    && control.read_transaction_failure
                        == ReadTransactionFailure::AfterNextActivationCommit
                {
                    control.read_transaction_failure = ReadTransactionFailure::NextReadTransaction;
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
        (*self).into_write_transaction().rollback()
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
}

/// Captures and controls protected together by the listener test double.
struct ListenerStubState {
    data_changes: Vec<CapturedDataChange>,
    data_change_batch_sizes: Vec<Vec<usize>>,
    data_change_lineages: Vec<DataChangeLineage>,
    data_change_read_tokens: Vec<GroupReadToken>,
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

    fn captured_data_change_read_tokens(&self) -> Vec<GroupReadToken> {
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
                ReplicationEvent::DataChanged { position, mut rows } => {
                    let lineage = position.lineage();
                    let read_token = position.group_read_token().clone();
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
    transition_results: VecDeque<Result<DatasetRowStateTransitionPageFixture, StoreError>>,
}

/// One deterministic ordinary scan page and the fixtures decoded into its output batch.
pub(in crate::runtime) struct ProviderTestRowScanResult {
    /// Page metadata returned by the test transaction.
    pub(in crate::runtime) page: DatasetRowScanPage,
    /// Stored-row fixtures decoded into the caller-owned state batch.
    pub(in crate::runtime) rows: Vec<ReplicationRowStateFixture>,
}

/// Decode one owned provider fixture into a reusable positional row batch.
fn push_provider_test_row(
    output: &mut ReplicationStateRowBatch,
    schema: &Schema,
    row: ReplicationRowStateFixture,
) -> usize {
    let encoded = flotsync_messages::codecs::datamodel::encode_row_snapshot(&row.snapshot, schema)
        .expect("provider test row must encode against its dataset schema");
    let mut decoder =
        flotsync_messages::snapshots::datamodel::ProtoSchemaSnapshotDecoder::new(encoded)
            .expect("provider test row must create a snapshot decoder");
    let row_index = output.len();
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
    row_index
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
    transition_results: impl IntoIterator<
        Item = Result<DatasetRowStateTransitionPageFixture, StoreError>,
    >,
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
#[derive(Clone, Debug, PartialEq, Eq)]
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
