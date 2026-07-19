use super::{
    component::ReplicationRuntimeComponent,
    errors::{CreateGroupError, InboundDeliveryError, PublishChangesError},
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
    api::{
        ApiError,
        AuthorityScope,
        ChangeGroupMembershipRequest,
        CreateGroupRequest,
        DatasetId,
        DatasetRowStateBatch,
        DatasetRowStatePatch,
        DatasetRowStateSlice,
        DatasetUpdateRecord,
        EncryptedGroupSecurityMaterial,
        GroupInvitation,
        GroupInvitationPolicy,
        GroupInvitationResponder,
        GroupInvitationSource,
        GroupMemberKeys,
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
        ReplicationSecuritySecrets,
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
        SnapshotRef,
        SnapshotRowsRequest,
        SnapshotValueRow,
        StoreError,
        StoreExternalSnafu,
        StoreSecretCryptoVersion,
        StoreSecretKeyId,
        SummaryRequest,
        TrustPolicy,
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
    delivery::security::{DeliverySecurity, DeliverySecurityError},
    provision_replication_security,
    security_store::{SecurityStore, SecurityStoreError},
    test_support::{
        load_test_delivery_security,
        provision_test_security as provision_shared_test_security,
        test_public_member_keys,
        test_replication_security_secrets,
        wait_for_test_future,
    },
};
use flotsync_core::{
    GroupId,
    MemberIdentity,
    MemberIndex,
    member::{Identifier, TrieMap},
    membership::{GroupMembers, GroupMemberships},
    versions::{PureVersionVector, UpdateId, VersionVector},
};
use flotsync_data_types::{Field, RowOperations, RowValues, Schema, TableOperations};
use flotsync_io::test_support::{ReservedSocketKind, eventually, reserve_sockets};
use flotsync_security::{
    GROUP_CIPHER_SUITE_CHACHA20_POLY1305,
    KeyFingerprint,
    PublicKeyBundle,
    PublicMemberKeys,
    StoreSecretKey,
    install_local_store_secret_test_store,
    test_support::{TEST_MEMBER_KEY_SEED_LENGTH, member_key_bundles_from_seed},
};
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
const ALICE_MEMBER_SEGMENTS: [&str; 2] = ["alice", "laptop"];
const BOB_MEMBER_SEGMENTS: [&str; 2] = ["bob", "laptop"];
const PROBE_MEMBER_SEGMENTS: [&str; 2] = ["probe", "laptop"];
const APP_ALICE_SEGMENTS: [&str; 2] = ["app", "alice"];
const APP_BOB_SEGMENTS: [&str; 2] = ["app", "bob"];
const APP_PROBE_SEGMENTS: [&str; 2] = ["app", "probe"];
static STATIC_TITLE_SCHEMA: LazyLock<Schema> =
    LazyLock::new(|| Schema::from_fields([Field::linear_string("title")]));

struct RuntimeFixture<S> {
    local_member: MemberIdentity,
    runtime: Arc<ReplicationRuntime>,
    listener: Arc<ListenerStub>,
    store: Arc<S>,
}

/// Test-only store wrapper that can fail one future row-patch write while
/// delegating all stored state to the wrapped `SQLite` store.
struct FailingStore<S> {
    inner: Arc<S>,
    fail_next_apply_dataset_row_patch: Arc<Mutex<Option<DatasetId>>>,
    fail_after_next_pending_group_commit: Arc<Mutex<bool>>,
}

impl<S> FailingStore<S> {
    fn new(inner: Arc<S>) -> Self {
        Self {
            inner,
            fail_next_apply_dataset_row_patch: Arc::new(Mutex::new(None)),
            fail_after_next_pending_group_commit: Arc::new(Mutex::new(false)),
        }
    }

    fn fail_next_apply_dataset_row_patch(&self, dataset_id: DatasetId) {
        *self
            .fail_next_apply_dataset_row_patch
            .lock()
            .expect("failing store mutex must not be poisoned") = Some(dataset_id);
    }

    fn fail_after_next_pending_group_commit(&self) {
        *self
            .fail_after_next_pending_group_commit
            .lock()
            .expect("failing store mutex must not be poisoned") = true;
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
        let fail_after_next_pending_group_commit =
            self.fail_after_next_pending_group_commit.clone();
        async move {
            let inner = inner.begin_transaction().await?;
            Ok(Box::new(FailingStoreTransaction {
                inner: Some(inner),
                fail_next_apply_dataset_row_patch,
                fail_after_next_pending_group_commit,
                wrote_pending_group_work: false,
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
    fail_after_next_pending_group_commit: Arc<Mutex<bool>>,
    wrote_pending_group_work: bool,
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

    fn load_local_member_private_keys<'a>(
        &'a mut self,
        member_id: &'a MemberIdentity,
    ) -> BoxFuture<'a, Result<Option<LocalMemberPrivateKeysRecord>, StoreError>> {
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
        group_id: &'a GroupId,
        dataset_id: &'a DatasetId,
        row_keys: &'a mut RowKeyIterator<'a>,
    ) -> BoxFuture<'a, Result<DatasetRowStateSlice, StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated reads")
            .load_dataset_rows(group_id, dataset_id, row_keys)
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
        group_id: &'a GroupId,
        dataset_id: &'a DatasetId,
        after: Option<RowKey>,
        limit: NonZeroUsize,
    ) -> BoxFuture<'a, Result<DatasetRowStateBatch, StoreError>> {
        self.inner
            .as_mut()
            .expect("failing store transaction must remain open during delegated reads")
            .scan_dataset_row_batch(group_id, dataset_id, after, limit)
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
        let Self { inner, .. } = *self;
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

    fn apply_dataset_row_patch(
        &mut self,
        patch: DatasetRowStatePatch,
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
                let source = std::io::Error::other(format!(
                    "failing store intentionally failed dataset row patch apply for '{}'",
                    patch.dataset_id
                ));
                return Err::<(), _>(source).boxed().context(StoreExternalSnafu);
            }
            self.inner
                .as_mut()
                .expect("failing store transaction must remain open during delegated writes")
                .apply_dataset_row_patch(patch)
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
            fail_after_next_pending_group_commit,
            wrote_pending_group_work,
            ..
        } = *self;
        async move {
            inner
                .expect("failing store transaction must remain open until commit")
                .commit()
                .await?;
            let should_fail = if wrote_pending_group_work {
                let mut failure = fail_after_next_pending_group_commit
                    .lock()
                    .expect("failing store mutex must not be poisoned");
                std::mem::take(&mut *failure)
            } else {
                false
            };
            if should_fail {
                let source = std::io::Error::other(
                    "failing store intentionally failed after committing pending group work",
                );
                return Err::<(), _>(source).boxed().context(StoreExternalSnafu);
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

struct ListenerStub {
    data_changes: Mutex<Vec<CapturedDataChange>>,
    data_change_read_tokens: Mutex<Vec<ReadToken>>,
    pending_group_events: Mutex<Vec<CapturedPendingGroupEvent>>,
    migration_proposal_event_sizes: Mutex<Vec<usize>>,
    reject_pending_group_events: Mutex<bool>,
    rejected_pending_group_event_count: Mutex<usize>,
    buffered_events: Mutex<mpsc::Receiver<CapturedDataChange>>,
    buffered_event_tx: mpsc::Sender<CapturedDataChange>,
}

impl Default for ListenerStub {
    fn default() -> Self {
        let (buffered_event_tx, buffered_events) = mpsc::channel();
        Self {
            data_changes: Mutex::new(Vec::new()),
            data_change_read_tokens: Mutex::new(Vec::new()),
            pending_group_events: Mutex::new(Vec::new()),
            migration_proposal_event_sizes: Mutex::new(Vec::new()),
            reject_pending_group_events: Mutex::new(false),
            rejected_pending_group_event_count: Mutex::new(0),
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

    fn take_pending_group_events(&self) -> Vec<CapturedPendingGroupEvent> {
        std::mem::take(
            &mut *self
                .pending_group_events
                .lock()
                .expect("pending-group listener capture mutex must not be poisoned"),
        )
    }

    fn migration_proposal_event_sizes(&self) -> Vec<usize> {
        self.migration_proposal_event_sizes
            .lock()
            .expect("migration proposal event-size mutex must not be poisoned")
            .clone()
    }

    fn wait_for_pending_group_event_count(&self, count: usize) {
        eventually(
            TEST_WAIT_TIMEOUT,
            || {
                self.pending_group_events
                    .lock()
                    .expect("pending-group listener capture mutex must not be poisoned")
                    .len()
                    >= count
            },
            format!("timed out waiting for {count} pending-group listener events"),
        );
    }

    fn reject_pending_group_events(&self) {
        *self
            .reject_pending_group_events
            .lock()
            .expect("pending-group rejection flag mutex must not be poisoned") = true;
    }

    fn rejected_pending_group_event_count(&self) -> usize {
        *self
            .rejected_pending_group_event_count
            .lock()
            .expect("pending-group rejection count mutex must not be poisoned")
    }
}

impl ReplicationEventListener for ListenerStub {
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
                ReplicationEvent::GroupInvitation {
                    invitation,
                    respond,
                } => {
                    if *self
                        .reject_pending_group_events
                        .lock()
                        .expect("pending-group rejection flag mutex must not be poisoned")
                    {
                        *self
                            .rejected_pending_group_event_count
                            .lock()
                            .expect("pending-group rejection count mutex must not be poisoned") +=
                            1;
                        return Err(ListenerError::Rejected {
                            message: "pending group event rejected by test listener".to_owned(),
                        });
                    }
                    self.pending_group_events
                        .lock()
                        .expect("pending-group listener capture mutex must not be poisoned")
                        .push(CapturedPendingGroupEvent::GroupInvitation {
                            invitation,
                            respond,
                        });
                }
                ReplicationEvent::MigrationProposals { proposals } => {
                    if *self
                        .reject_pending_group_events
                        .lock()
                        .expect("pending-group rejection flag mutex must not be poisoned")
                    {
                        *self
                            .rejected_pending_group_event_count
                            .lock()
                            .expect("pending-group rejection count mutex must not be poisoned") +=
                            1;
                        return Err(ListenerError::Rejected {
                            message: "pending group event rejected by test listener".to_owned(),
                        });
                    }
                    self.migration_proposal_event_sizes
                        .lock()
                        .expect("migration proposal event-size mutex must not be poisoned")
                        .push(proposals.len());
                    let mut captured = self
                        .pending_group_events
                        .lock()
                        .expect("pending-group listener capture mutex must not be poisoned");
                    for proposal in proposals {
                        captured.push(CapturedPendingGroupEvent::MigrationProposal {
                            proposal: proposal.proposal,
                            respond: proposal.respond,
                        });
                    }
                }
            }
            Ok(())
        }
        .boxed()
    }
}

mod changes;
mod delivery;
mod fixtures;
mod groups;
mod host;
mod setup;

use fixtures::*;
