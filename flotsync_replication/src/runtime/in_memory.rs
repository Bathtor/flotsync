use super::errors::{
    ExactUpdateMismatch,
    GroupInstallError,
    InboundDeliveryError,
    InstallMissingLocalMemberSnafu,
    InvalidPersistedMembersSnafu,
    PersistedFinalVersionVectorMemberCountMismatchSnafu,
    PersistedLocalMemberIndexMismatchSnafu,
    PersistedVersionVectorMemberCountMismatchSnafu,
    PublishChangesError,
    inbound,
    publish,
};
use crate::api::{
    DatasetId,
    DatasetRowStatePatch,
    DatasetRowStateSlice,
    DatasetRowStateWrite,
    DatasetUpdateRecord,
    GroupSchema,
    PageCursor,
    ReplicationGroupRecord,
    ReplicationRowStateSnapshot,
    ReplicationStoreReadTransaction,
    ReplicationUpdateFilter,
    ReplicationUpdatePageInput,
    ReplicationUpdateRecord,
    ReplicationUpdateView,
    ReplicationUpdatesQuery,
    RowChange,
    RowId,
    RowKey,
    RowMutation,
    RowValuesPatch,
    SchemaSource,
    StoreError,
    VecPageBatch,
};
use flotsync_core::{
    GroupId,
    MemberIdentity,
    MemberIndex,
    membership::GroupMembers,
    versions::{UpdateId, VersionVector},
};
use flotsync_data_types::{
    InitialFieldValue,
    OperationOutcome,
    PendingFieldUpdate,
    RowValues,
    Schema,
    TableOperations,
    schema::datamodel::RowOperation,
};
use flotsync_messages::codecs::datamodel::{
    decode_schema_operation,
    decode_schema_operation_view_row_id,
    encode_schema_operation,
};
use flotsync_utils::{BoxError, option_when};
use snafu::prelude::*;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap, HashSet},
    num::NonZeroUsize,
    sync::Arc,
};

/// Maximum selected ready updates projected in one row-scope page.
pub(super) const READY_ROW_SCOPE_PAGE_SIZE: NonZeroUsize =
    NonZeroUsize::new(64).expect("64 is non-zero");

struct LocalStoredStateRow {
    snapshot: ReplicationRowStateSnapshot,
    tombstoned: bool,
}

/// One fixed-membership replication group loaded for one store transaction.
///
/// The runtime no longer keeps this state resident between messages. Instead,
/// each publish or inbound-apply flow loads the persisted group metadata, uses
/// it to drive one isolated transaction-scoped working set, and then writes the
/// updated durable state back through `ReplicationStore`.
#[derive(Clone)]
pub(super) struct LoadedGroupMeta {
    pub(super) members: GroupMembers,
    pub(super) local_member_index: MemberIndex,
    pub(super) version_vector: VersionVector,
}

impl LoadedGroupMeta {
    /// Rebuild one transaction-scoped group view from a persisted group record.
    pub(super) fn from_replication_group_record(
        local_member: &MemberIdentity,
        group: ReplicationGroupRecord,
    ) -> Result<Self, GroupInstallError> {
        let (members, local_member_index) =
            Self::validate_replication_group_record(local_member, &group)?;

        Ok(Self {
            members,
            local_member_index,
            version_vector: group.version_vector,
        })
    }

    /// Validate one persisted group and return its canonical member set.
    pub(super) fn validated_members_from_replication_group_record(
        local_member: &MemberIdentity,
        group: &ReplicationGroupRecord,
    ) -> Result<GroupMembers, GroupInstallError> {
        let (members, _local_member_index) =
            Self::validate_replication_group_record(local_member, group)?;
        Ok(members)
    }

    /// Validate invariants shared by transaction and runtime-state projections.
    fn validate_replication_group_record(
        local_member: &MemberIdentity,
        group: &ReplicationGroupRecord,
    ) -> Result<(GroupMembers, MemberIndex), GroupInstallError> {
        let group_id = group.group_id;
        let members = group
            .member_keys
            .to_group_members()
            .context(InvalidPersistedMembersSnafu { group_id })?;
        let local_member_index =
            members
                .member_index(local_member)
                .context(InstallMissingLocalMemberSnafu {
                    local_member: local_member.clone(),
                })?;
        ensure!(
            local_member_index == group.local_member_index,
            PersistedLocalMemberIndexMismatchSnafu {
                group_id,
                local_member: local_member.clone(),
                persisted_local_member_index: group.local_member_index,
                actual_local_member_index: local_member_index,
            }
        );

        let member_count =
            NonZeroUsize::new(members.len()).expect("persisted group members must not be empty");
        ensure!(
            group.version_vector.num_members() == member_count,
            PersistedVersionVectorMemberCountMismatchSnafu {
                group_id,
                persisted_member_count: group.version_vector.num_members().get(),
                actual_member_count: member_count.get(),
            }
        );
        if let Some(final_versions) = group.lifecycle.final_versions() {
            ensure!(
                final_versions.num_members() == member_count,
                PersistedFinalVersionVectorMemberCountMismatchSnafu {
                    group_id,
                    final_member_count: final_versions.num_members().get(),
                    actual_member_count: member_count.get(),
                }
            );
        }

        Ok((members, local_member_index))
    }

    /// Return the fixed member count for this transaction-scoped group view.
    pub(super) fn member_count(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.members.len()).expect("loaded group must be non-empty")
    }

    /// Return the durably applied version for the given member index.
    pub(super) fn applied_version(&self, member_index: MemberIndex) -> u64 {
        self.version_vector
            .version_at(member_index.as_u32() as usize)
    }

    /// Return `true` when `update_id` is already reflected in the group version vector.
    pub(super) fn has_applied(&self, update_id: UpdateId) -> bool {
        self.applied_version(MemberIndex::new(update_id.node_index)) >= update_id.version
    }

    /// Advance the group version vector to reflect one update that has now applied.
    pub(super) fn mark_applied(&mut self, update_id: UpdateId) {
        self.version_vector
            .increment_at(update_id.node_index as usize);
    }
}

/// Reject an inbound update whose read position includes its own producer version.
pub(super) fn validate_inbound_update_read_versions(
    update: &ReplicationUpdateRecord,
) -> Result<(), InboundDeliveryError> {
    match classify_producer_read_causality(update.update_id, &update.read_versions) {
        ProducerReadCausality::PrecedesUpdate => Ok(()),
        ProducerReadCausality::IncludesUpdate {
            producer_read_version,
        } => inbound::SelfDependentReadVersionsSnafu {
            group_id: update.group_id,
            update_id: update.update_id,
            producer_read_version,
        }
        .fail(),
    }
}

/// Classify whether an update's producer dependency precedes its own version.
///
/// The caller must first establish that the producer index is present in the
/// update's read-version vector.
pub(super) fn classify_producer_read_causality(
    update_id: UpdateId,
    read_versions: &VersionVector,
) -> ProducerReadCausality {
    let producer_read_version = read_versions.version_at(update_id.node_index as usize);
    if producer_read_version < update_id.version {
        ProducerReadCausality::PrecedesUpdate
    } else {
        ProducerReadCausality::IncludesUpdate {
            producer_read_version,
        }
    }
}

/// Relationship between an update and its own producer entry in its read position.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ProducerReadCausality {
    /// The read position ends before the update, as required for causal validity.
    PrecedesUpdate,
    /// The read position includes this update or a later producer version.
    IncludesUpdate {
        /// Producer version carried in the update's read position.
        producer_read_version: u64,
    },
}

/// Identity and causal dependencies needed to schedule one stored update.
#[derive(Clone)]
pub(super) struct UpdateDependency {
    /// Producer and version of the stored update.
    pub(super) update_id: UpdateId,
    /// Causal position the update requires before application.
    pub(super) read_versions: VersionVector,
}

impl UpdateDependency {
    /// Copy scheduling data from a temporary store projection.
    pub(super) fn from_view(view: &ReplicationUpdateView<'_>) -> Self {
        Self {
            update_id: view.update_id(),
            read_versions: view.read_versions().clone(),
        }
    }

    /// Compare candidates for historical replay by read position, then identity.
    ///
    /// Concurrent positions have no vector ordering and use the update ID as
    /// the deterministic tie-breaker.
    pub(super) fn compare_for_replay(&self, other: &Self) -> Ordering {
        self.read_versions
            .partial_cmp(&other.read_versions)
            .unwrap_or(Ordering::Equal)
            .then_with(|| self.update_id.cmp(&other.update_id))
    }

    /// Return `true` when all dependencies are applied and this update is the
    /// next version of its producer; `false` means it must wait.
    pub(super) fn is_ready_at(&self, versions: &VersionVector) -> bool {
        if !matches!(
            versions.partial_cmp(&self.read_versions),
            Some(Ordering::Greater | Ordering::Equal)
        ) {
            return false;
        }
        let producer = self.update_id.node_index as usize;
        let next_version = versions
            .version_at(producer)
            .checked_add(1)
            .expect("member version counter must not overflow");
        next_version == self.update_id.version
    }
}

/// Load one group's selected update dependencies into an unlimited projection.
///
/// The returned values contain no operation payloads. A store or page failure
/// remains a classified `StoreError` for the caller's own error context.
pub(super) async fn load_update_dependencies<Transaction>(
    transaction: &mut Transaction,
    group_id: GroupId,
    filter: ReplicationUpdateFilter,
) -> Result<Vec<UpdateDependency>, StoreError>
where
    Transaction: ReplicationStoreReadTransaction + ?Sized,
{
    let mut cursor = PageCursor::new(ReplicationUpdatesQuery::new(group_id, filter));
    let project =
        |view: ReplicationUpdateView<'_>| Ok::<_, BoxError>(UpdateDependency::from_view(&view));
    let mut batch =
        VecPageBatch::<UpdateDependency, (), ReplicationUpdatePageInput, _>::unlimited_with(
            project,
        );
    transaction
        .load_replication_updates_into(&mut cursor, &mut batch)
        .await
        .map_err(StoreError::from)?;
    Ok(batch.into_values())
}

/// Load one ready update selected from projected dependencies in the same transaction.
///
/// An absent record or changed causal position indicates a store inconsistency.
pub(super) async fn load_scheduled_pending_update<Transaction>(
    transaction: &mut Transaction,
    group_id: GroupId,
    scheduled: &UpdateDependency,
) -> Result<ReplicationUpdateRecord, InboundDeliveryError>
where
    Transaction: ReplicationStoreReadTransaction + ?Sized,
{
    let update = transaction
        .load_replication_update(&group_id, scheduled.update_id)
        .await
        .context(inbound::StoreAccessSnafu)?
        .context(inbound::MissingScheduledUpdateSnafu {
            group_id,
            update_id: scheduled.update_id,
        })?;
    if update.group_id == group_id
        && update.update_id == scheduled.update_id
        && update.read_versions == scheduled.read_versions
        && !update.applied_locally
    {
        Ok(update)
    } else {
        inbound::MismatchedScheduledUpdateSnafu {
            mismatch: Box::new(ExactUpdateMismatch::new(
                group_id,
                scheduled.update_id,
                &scheduled.read_versions,
                false,
                &update,
            )),
        }
        .fail()
    }
}

/// Persisted-but-not-yet-applied update dependencies loaded for one
/// transactional causality check.
pub(super) struct PendingUpdateSet {
    updates: BTreeMap<UpdateId, UpdateDependency>,
}

impl PendingUpdateSet {
    /// Build one deterministic pending-update index from projected dependencies.
    pub(super) fn from_updates(updates: Vec<UpdateDependency>) -> Self {
        let updates = updates
            .into_iter()
            .map(|update| (update.update_id, update))
            .collect();
        Self { updates }
    }

    /// Remove updates already represented by `group` and return one causally ready id.
    fn remove_applied_and_find_ready(
        &mut self,
        group: &LoadedGroupMeta,
        already_applied: &mut Vec<UpdateId>,
    ) -> Option<UpdateId> {
        let stale_ids = self
            .updates
            .keys()
            .copied()
            .filter(|update_id| group.has_applied(*update_id))
            .collect::<Vec<_>>();
        for update_id in stale_ids {
            self.updates.remove(&update_id);
            already_applied.push(update_id);
        }
        self.updates.iter().find_map(|(update_id, update)| {
            option_when!(update.is_ready_at(&group.version_vector), *update_id)
        })
    }

    /// Determine which pending updates are already reflected in group state
    /// and which additional updates can now apply in causal order.
    pub(super) fn plan_apply_chain(&mut self, group: &LoadedGroupMeta) -> PendingApplyPlan {
        let mut already_applied = Vec::new();
        let mut ready_chain = Vec::new();
        let mut simulated_group = group.clone();

        while let Some(ready_update_id) =
            self.remove_applied_and_find_ready(&simulated_group, &mut already_applied)
        {
            let ready_update = self
                .updates
                .remove(&ready_update_id)
                .expect("pending update must still exist while draining");
            simulated_group.mark_applied(ready_update_id);
            ready_chain.push(ready_update);
        }

        PendingApplyPlan {
            already_applied,
            ready_chain,
            blocked_updates: std::mem::take(&mut self.updates).into_values().collect(),
        }
    }
}

/// One transaction-local decision about pending persisted updates.
pub(super) struct PendingApplyPlan {
    pub(super) already_applied: Vec<UpdateId>,
    pub(super) ready_chain: Vec<UpdateDependency>,
    pub(super) blocked_updates: Vec<UpdateDependency>,
}

/// One local dataset together with its current replicated in-memory contents.
#[derive(Clone)]
pub(super) struct LocalDataset {
    pub(super) data: flotsync_messages::InMemoryStateData,
}

impl LocalDataset {
    pub(super) fn new(schema: impl Into<SchemaSource>) -> Self {
        Self {
            data: flotsync_messages::InMemoryStateData::new(schema),
        }
    }

    /// Rebuild one ephemeral in-memory dataset slice from store-loaded rows.
    pub(super) fn from_row_slice(schema: SchemaSource, slice: DatasetRowStateSlice) -> Self {
        let data = flotsync_messages::InMemoryStateData::from_row_batch(
            schema,
            slice.state_rows,
            |metadata| (metadata.row_key.0, metadata.tombstoned),
        )
        .expect("store-loaded dataset slice must not contain duplicate row keys");
        Self { data }
    }

    fn stored_row(&self, row_key: RowKey) -> Option<LocalStoredStateRow> {
        let row = self.data.get_row(&row_key.0)?;
        Some(LocalStoredStateRow {
            snapshot: row.snapshot(),
            tombstoned: row.is_tombstoned(),
        })
    }

    fn row_is_tombstoned(&self, row_key: RowKey) -> Option<bool> {
        self.data.row_is_tombstoned(&row_key.0)
    }

    pub(super) fn clone_value_row(&self, row_key: RowKey) -> Option<RowValues> {
        let row = self.data.get_row(&row_key.0)?;
        Some(
            RowValues::from_row(self.data.schema(), &row)
                .expect("value projection from a schema-owned state row must validate"),
        )
    }

    /// Snapshot the current row image for explicit durable row writes.
    pub(super) fn snapshot_row(&self, row_key: RowKey) -> Option<ReplicationRowStateSnapshot> {
        self.data.get_row(&row_key.0).map(|row| row.snapshot())
    }
}

impl RowValuesPatch {
    fn into_initial_values<'schema>(
        self,
        schema: &'schema Schema,
        row_id: &RowId,
    ) -> Result<Vec<InitialFieldValue<'schema>>, PublishChangesError> {
        let mut initial_values = Vec::with_capacity(self.fields.len());
        for (field_name, value) in self.fields {
            let field = schema.columns.get(field_name.as_str()).with_context(|| {
                publish::UnknownSchemaFieldSnafu {
                    row_id: row_id.clone(),
                    dataset_id: row_id.dataset_id.clone(),
                    field_name,
                }
            })?;
            let initial_value = field.initial(value).map_err(Into::into).with_context(|_| {
                publish::InvalidFieldValueSnafu {
                    row_id: row_id.clone(),
                    dataset_id: row_id.dataset_id.clone(),
                }
            })?;
            initial_values.push(initial_value);
        }
        Ok(initial_values)
    }

    fn into_pending_updates<'schema>(
        self,
        schema: &'schema Schema,
        row_id: &RowId,
    ) -> Result<Vec<PendingFieldUpdate<'schema>>, PublishChangesError> {
        let mut pending_updates = Vec::with_capacity(self.fields.len());
        for (field_name, value) in self.fields {
            let field = schema.columns.get(field_name.as_str()).with_context(|| {
                publish::UnknownSchemaFieldSnafu {
                    row_id: row_id.clone(),
                    dataset_id: row_id.dataset_id.clone(),
                    field_name,
                }
            })?;
            let pending_update = field.set(value).map_err(Into::into).with_context(|_| {
                publish::InvalidFieldValueSnafu {
                    row_id: row_id.clone(),
                    dataset_id: row_id.dataset_id.clone(),
                }
            })?;
            pending_updates.push(pending_update);
        }
        Ok(pending_updates)
    }
}

/// One publish batch scoped to a single group and the touched rows per dataset.
pub(super) struct TouchedGroupRows {
    pub(super) group_id: GroupId,
    pub(super) dataset_rows: HashMap<DatasetId, HashSet<RowKey>>,
}

/// Validate that one publish call targets exactly one group and collect the
/// touched row keys for each dataset without imposing semantic ordering on the
/// original mutation list.
pub(super) fn collect_group_row_scope(
    changes: &[RowMutation],
) -> Result<TouchedGroupRows, PublishChangesError> {
    let Some(first_change) = changes.first() else {
        return publish::EmptyChangesSnafu.fail();
    };
    let group_id = first_change.row_id().group_id;
    let mut dataset_rows: HashMap<DatasetId, HashSet<RowKey>> = HashMap::new();
    for change in changes {
        let row_id = change.row_id();
        ensure!(
            row_id.group_id == group_id,
            publish::MixedGroupsSnafu {
                first_group_id: group_id,
                other_group_id: row_id.group_id,
            }
        );
        dataset_rows
            .entry(row_id.dataset_id.clone())
            .or_default()
            .insert(row_id.row_key);
    }
    Ok(TouchedGroupRows {
        group_id,
        dataset_rows,
    })
}

/// Copy dataset and row identities from one ready update's temporary view.
///
/// Each dataset is checked against the current group schema before its row keys
/// are decoded. Full operation validation still occurs before the transaction
/// commits when the selected payload is loaded and applied.
pub(super) fn project_update_row_scope(
    view: &ReplicationUpdateView<'_>,
    group_schema: &GroupSchema,
) -> Result<Vec<(DatasetId, Vec<RowKey>)>, InboundDeliveryError> {
    let mut row_keys_by_dataset = Vec::new();
    for dataset_update in view.dataset_updates() {
        let raw_dataset_id = dataset_update.dataset_id();
        let dataset_id =
            DatasetId::try_from_owned(raw_dataset_id.to_owned()).with_context(|_| {
                inbound::InvalidProjectedDatasetIdSnafu {
                    group: view.group_id(),
                    update: view.update_id(),
                    dataset: raw_dataset_id.to_owned(),
                }
            })?;
        ensure!(
            group_schema.schema(&dataset_id).is_some(),
            inbound::MissingDatasetSchemaSnafu {
                group_id: view.group_id(),
                dataset_id: dataset_id.clone(),
            }
        );
        let mut row_keys = Vec::with_capacity(dataset_update.operations().len());
        for operation in dataset_update.operations() {
            let row_id = decode_schema_operation_view_row_id(operation).with_context(|_| {
                inbound::DecodeSchemaOperationSnafu {
                    dataset_id: dataset_id.clone(),
                }
            })?;
            row_keys.push(RowKey(row_id));
        }
        row_keys_by_dataset.push((dataset_id, row_keys));
    }
    Ok(row_keys_by_dataset)
}

/// Collect row keys touched by selected ready updates without keeping their
/// operation payloads or decoding operations from blocked updates. Every ready
/// update must still appear in this second projected scan.
pub(super) async fn load_ready_row_scope<Transaction>(
    transaction: &mut Transaction,
    group_id: GroupId,
    group_schema: &GroupSchema,
    ready_ids: &HashSet<UpdateId>,
) -> Result<HashMap<DatasetId, HashSet<RowKey>>, InboundDeliveryError>
where
    Transaction: ReplicationStoreReadTransaction + ?Sized,
{
    let query = ReplicationUpdatesQuery::new(group_id, ReplicationUpdateFilter::PendingApply)
        .with_update_ids(ready_ids);
    let mut cursor = PageCursor::new(query);
    let project = |view: ReplicationUpdateView<'_>| {
        Ok::<_, BoxError>((
            view.update_id(),
            project_update_row_scope(&view, group_schema),
        ))
    };
    let mut batch = VecPageBatch::<
        (
            UpdateId,
            Result<Vec<(DatasetId, Vec<RowKey>)>, InboundDeliveryError>,
        ),
        (),
        ReplicationUpdatePageInput,
        _,
    >::bounded_with(READY_ROW_SCOPE_PAGE_SIZE, project);
    let mut dataset_rows: HashMap<DatasetId, HashSet<RowKey>> = HashMap::new();
    let mut seen_ready_ids = HashSet::new();
    while cursor.has_more() {
        transaction
            .load_replication_updates_into(&mut cursor, &mut batch)
            .await
            .map_err(StoreError::from)
            .context(inbound::StoreAccessSnafu)?;
        for (update_id, row_keys_result) in batch.values_mut().drain(..) {
            seen_ready_ids.insert(update_id);
            let row_keys_by_dataset = row_keys_result?;
            for (dataset_id, row_keys) in row_keys_by_dataset {
                dataset_rows.entry(dataset_id).or_default().extend(row_keys);
            }
        }
    }
    for update_id in ready_ids {
        ensure!(
            seen_ready_ids.contains(update_id),
            inbound::MissingScheduledUpdateSnafu {
                group_id,
                update_id: *update_id,
            }
        );
    }
    Ok(dataset_rows)
}

/// Validate that every schema operation embedded in one replication update is
/// decodable for the local schema and bound to the update's `UpdateId`.
pub(super) fn validate_update_mapping(
    update: &ReplicationUpdateRecord,
    group_schema: &GroupSchema,
) -> Result<(), InboundDeliveryError> {
    for dataset_update in &update.dataset_updates {
        let schema = group_schema
            .schema(&dataset_update.dataset_id)
            .expect("inbound update schemas must be pre-loaded before validation");
        for operation in &dataset_update.operations {
            decode_update_schema_operation(
                update,
                dataset_update,
                operation.clone(),
                schema.as_schema(),
            )?;
        }
    }
    Ok(())
}

/// Returns the mutable working dataset image used for one inbound apply batch.
///
/// Callers are expected to pre-load every dataset touched by the causal apply
/// chain before the first operation is decoded.
fn working_dataset_for_inbound<'dataset>(
    working_datasets: &'dataset mut HashMap<DatasetId, LocalDataset>,
    dataset_id: &DatasetId,
) -> &'dataset mut LocalDataset {
    working_datasets
        .get_mut(dataset_id)
        .expect("touched inbound dataset must be pre-loaded before apply")
}

/// One prepared local publish batch together with the corresponding durable
/// row patches.
pub(super) struct PreparedLocalChanges {
    pub(super) dataset_updates: Vec<DatasetUpdateRecord>,
    pub(super) row_patches: Vec<DatasetRowStatePatch>,
    pub(super) row_changes: Vec<RowChange>,
}

/// One applied inbound batch together with the corresponding durable row patches.
pub(super) struct AppliedInboundBatch {
    pub(super) row_changes: Vec<RowChange>,
    pub(super) row_patches: Vec<DatasetRowStatePatch>,
}

/// One staged local mutation together with its explicit durable row write.
pub(super) struct AppliedLocalOperation {
    pub(super) encoded_operation: flotsync_messages::datamodel::SchemaOperation,
    pub(super) row_change: Option<RowChange>,
    pub(super) row_write: DatasetRowStateWrite,
}

struct AppliedRemoteOperation {
    /// Listener-visible change for this operation.
    ///
    /// `None` means the durable tombstone image changed, but the row was
    /// already deleted from the application's visible set and should not emit a
    /// second delete or a resurrection upsert.
    row_change: Option<RowChange>,
    row_write: DatasetRowStateWrite,
}

/// Applies one causally-ready inbound batch against one local group state.
///
/// All touched datasets are first materialised into working copies so the batch
/// either commits atomically into local state or returns an error without
/// partially replacing dataset maps.
pub(super) fn apply_one_update(
    group: &mut LoadedGroupMeta,
    working_datasets: &mut HashMap<DatasetId, LocalDataset>,
    update: &ReplicationUpdateRecord,
) -> Result<AppliedInboundBatch, InboundDeliveryError> {
    let mut row_changes = Vec::new();
    let mut row_patches = Vec::new();
    let last_changed_versions = update.read_versions.with_update_applied(update.update_id);
    for dataset_update in &update.dataset_updates {
        let working_dataset =
            working_dataset_for_inbound(working_datasets, &dataset_update.dataset_id);
        let mut row_writes = Vec::new();
        for operation in &dataset_update.operations {
            let schema = working_dataset.data.schema().clone();
            let operation =
                decode_update_schema_operation(update, dataset_update, operation.clone(), &schema)?;
            let applied_operation = apply_remote_operation(
                working_dataset,
                update.group_id,
                &dataset_update.dataset_id,
                operation,
            )?;
            if let Some(row_change) = applied_operation.row_change {
                row_changes.push(row_change);
            }
            row_writes.push(applied_operation.row_write);
        }
        if !row_writes.is_empty() {
            row_patches.push(DatasetRowStatePatch {
                group_id: update.group_id,
                dataset_id: dataset_update.dataset_id.clone(),
                actions: row_writes,
                change_id: update.update_id,
                last_changed_versions: last_changed_versions.clone(),
            });
        }
    }
    group.mark_applied(update.update_id);
    Ok(AppliedInboundBatch {
        row_changes,
        row_patches,
    })
}

fn decode_update_schema_operation<'schema>(
    update: &ReplicationUpdateRecord,
    dataset_update: &DatasetUpdateRecord,
    operation: flotsync_messages::datamodel::SchemaOperation,
    schema: &'schema Schema,
) -> Result<flotsync_messages::SchemaOperation<'schema>, InboundDeliveryError> {
    let operation = decode_schema_operation(operation, schema).context(
        inbound::DecodeSchemaOperationSnafu {
            dataset_id: dataset_update.dataset_id.clone(),
        },
    )?;
    ensure!(
        operation.change_id == update.update_id,
        inbound::UpdateOperationIdMismatchSnafu {
            group: update.group_id,
            update: update.update_id,
            dataset: dataset_update.dataset_id.clone(),
            operation_change: operation.change_id,
        }
    );
    Ok(operation)
}

/// Applies one local upsert and returns the encoded schema operation, if any.
///
/// A local upsert may still produce no transport operation when the new row
/// image is identical to what is already stored locally.
pub(super) fn apply_local_upsert(
    dataset: &mut LocalDataset,
    row_id: &RowId,
    row: RowValuesPatch,
    update_id: UpdateId,
) -> Result<Option<AppliedLocalOperation>, PublishChangesError> {
    let schema = dataset.data.schema().clone();
    let encoded_operation = {
        let Some(operation) = apply_local_upsert_operation(dataset, row_id, row, update_id)? else {
            return Ok(None);
        };
        encode_publish_operation(row_id, &schema, &operation)?
    };
    let row_snapshot = dataset
        .snapshot_row(row_id.row_key)
        .unwrap_or_else(|| panic!("applied local upsert must leave row {row_id} readable"));
    let row = dataset
        .clone_value_row(row_id.row_key)
        .unwrap_or_else(|| panic!("applied local upsert must leave row {row_id} readable"));
    Ok(Some(AppliedLocalOperation {
        encoded_operation,
        row_change: Some(RowChange::ordinary_upsert(row_id.clone(), Arc::new(row))),
        row_write: DatasetRowStateWrite::UpsertActive {
            row_key: row_id.row_key,
            snapshot: row_snapshot,
        },
    }))
}

/// Applies one local delete and encodes the resulting schema operation for transport.
pub(super) fn apply_local_delete(
    dataset: &mut LocalDataset,
    row_id: &RowId,
    update_id: UpdateId,
) -> Result<AppliedLocalOperation, PublishChangesError> {
    let schema = dataset.data.schema().clone();
    let encoded_operation = {
        let operation = apply_local_delete_operation(dataset, row_id, update_id)?;
        encode_publish_operation(row_id, &schema, &operation)?
    };
    let row = dataset
        .stored_row(row_id.row_key)
        .unwrap_or_else(|| panic!("applied local delete must leave row {row_id} snapshotable"));
    debug_assert!(row.tombstoned);
    Ok(AppliedLocalOperation {
        encoded_operation,
        row_change: Some(RowChange::ordinary_delete(row_id.clone())),
        row_write: DatasetRowStateWrite::UpsertTombstone {
            row_key: row_id.row_key,
            snapshot: row.snapshot,
        },
    })
}

/// Generate one local upsert operation from `base_dataset`, then apply that
/// encoded operation to `current_dataset`.
pub(super) fn apply_rebased_local_upsert(
    base_dataset: &mut LocalDataset,
    current_dataset: &mut LocalDataset,
    row_id: &RowId,
    row: RowValuesPatch,
    update_id: UpdateId,
) -> Result<Option<AppliedLocalOperation>, PublishChangesError> {
    let schema = base_dataset.data.schema().clone();
    let Some(operation) = apply_local_upsert_operation(base_dataset, row_id, row, update_id)?
    else {
        return Ok(None);
    };
    let encoded_operation = encode_publish_operation(row_id, &schema, &operation)?;
    apply_decoded_publish_operation(current_dataset, row_id, operation, encoded_operation).map(Some)
}

/// Generate one local delete operation from `base_dataset`, then apply that
/// encoded operation to `current_dataset`.
pub(super) fn apply_rebased_local_delete(
    base_dataset: &mut LocalDataset,
    current_dataset: &mut LocalDataset,
    row_id: &RowId,
    update_id: UpdateId,
) -> Result<AppliedLocalOperation, PublishChangesError> {
    let schema = base_dataset.data.schema().clone();
    let operation = apply_local_delete_operation(base_dataset, row_id, update_id)?;
    let encoded_operation = encode_publish_operation(row_id, &schema, &operation)?;
    apply_decoded_publish_operation(current_dataset, row_id, operation, encoded_operation)
}

fn apply_local_upsert_operation<'dataset>(
    dataset: &'dataset mut LocalDataset,
    row_id: &RowId,
    row: RowValuesPatch,
    update_id: UpdateId,
) -> Result<Option<flotsync_messages::SchemaOperation<'dataset>>, PublishChangesError> {
    let schema = dataset.data.schema().clone();
    if dataset.data.get_row(&row_id.row_key.0).is_some() {
        let pending_updates = row.into_pending_updates(&schema, row_id)?;
        match dataset
            .data
            .modify_row(update_id, row_id.row_key.0, pending_updates)
            .context(publish::ApplyLocalMutationSnafu {
                row_id: row_id.clone(),
            })? {
            OperationOutcome::Applied(operation) => Ok(Some(operation)),
            OperationOutcome::NoChanges => Ok(None),
        }
    } else {
        let initial_values = row.into_initial_values(&schema, row_id)?;
        let operation = dataset
            .data
            .insert_row(update_id, row_id.row_key.0, initial_values)
            .context(publish::ApplyLocalMutationSnafu {
                row_id: row_id.clone(),
            })?;
        Ok(Some(operation))
    }
}

fn apply_local_delete_operation<'dataset>(
    dataset: &'dataset mut LocalDataset,
    row_id: &RowId,
    update_id: UpdateId,
) -> Result<flotsync_messages::SchemaOperation<'dataset>, PublishChangesError> {
    dataset
        .data
        .delete_row(update_id, row_id.row_key.0)
        .context(publish::ApplyLocalMutationSnafu {
            row_id: row_id.clone(),
        })
}

fn encode_publish_operation(
    row_id: &RowId,
    schema: &Schema,
    operation: &flotsync_messages::SchemaOperation<'_>,
) -> Result<flotsync_messages::datamodel::SchemaOperation, PublishChangesError> {
    encode_schema_operation(operation, schema).context(publish::EncodeOperationSnafu {
        dataset_id: row_id.dataset_id.clone(),
    })
}

fn apply_decoded_publish_operation(
    dataset: &mut LocalDataset,
    row_id: &RowId,
    operation: flotsync_messages::SchemaOperation<'_>,
    encoded_operation: flotsync_messages::datamodel::SchemaOperation,
) -> Result<AppliedLocalOperation, PublishChangesError> {
    let was_tombstoned = dataset.row_is_tombstoned(row_id.row_key).unwrap_or(false);

    dataset.data = dataset
        .data
        .clone()
        .apply_schema_operation(operation)
        .context(publish::ApplyLocalMutationSnafu {
            row_id: row_id.clone(),
        })?;

    let stored_row = dataset
        .stored_row(row_id.row_key)
        .unwrap_or_else(|| panic!("applied local operation must leave row {row_id} snapshotable"));
    let row_change = if stored_row.tombstoned {
        if was_tombstoned {
            None
        } else {
            Some(RowChange::ordinary_delete(row_id.clone()))
        }
    } else {
        let row = dataset
            .clone_value_row(row_id.row_key)
            .unwrap_or_else(|| panic!("applied local upsert must leave row {row_id} readable"));
        Some(RowChange::ordinary_upsert(row_id.clone(), Arc::new(row)))
    };
    Ok(AppliedLocalOperation {
        encoded_operation,
        row_change,
        row_write: if stored_row.tombstoned {
            DatasetRowStateWrite::UpsertTombstone {
                row_key: row_id.row_key,
                snapshot: stored_row.snapshot,
            }
        } else {
            DatasetRowStateWrite::UpsertActive {
                row_key: row_id.row_key,
                snapshot: stored_row.snapshot,
            }
        },
    })
}

fn apply_remote_operation(
    dataset: &mut LocalDataset,
    group_id: GroupId,
    dataset_id: &DatasetId,
    operation: flotsync_messages::SchemaOperation<'_>,
) -> Result<AppliedRemoteOperation, InboundDeliveryError> {
    let api_row_id = match &operation.operation {
        RowOperation::Insert { row_id, .. }
        | RowOperation::Update { row_id, .. }
        | RowOperation::Delete { row_id } => RowId {
            group_id,
            dataset_id: dataset_id.clone(),
            row_key: RowKey(*row_id),
        },
    };
    let was_tombstoned = dataset
        .row_is_tombstoned(api_row_id.row_key)
        .unwrap_or(false);

    // flotsync_messages::InMemoryStateData currently consumes `self` when applying
    // one schema operation, so the runtime must clone the current dataset image
    // before replacing it with the updated result.
    dataset.data = dataset
        .data
        .clone()
        .apply_schema_operation(operation)
        .context(inbound::ApplyInboundMutationSnafu {
            row_id: api_row_id.clone(),
        })?;

    let stored_row = dataset.stored_row(api_row_id.row_key).unwrap_or_else(|| {
        panic!("applied inbound operation must leave row {api_row_id} snapshotable")
    });
    let row_change = if stored_row.tombstoned {
        if was_tombstoned {
            None
        } else {
            Some(RowChange::ordinary_delete(api_row_id.clone()))
        }
    } else {
        Some({
            let row = dataset
                .clone_value_row(api_row_id.row_key)
                .unwrap_or_else(|| {
                    panic!("applied inbound upsert must leave row {api_row_id} readable")
                });
            RowChange::ordinary_upsert(api_row_id.clone(), Arc::new(row))
        })
    };
    Ok(AppliedRemoteOperation {
        row_change,
        row_write: if stored_row.tombstoned {
            DatasetRowStateWrite::UpsertTombstone {
                row_key: api_row_id.row_key,
                snapshot: stored_row.snapshot,
            }
        } else {
            DatasetRowStateWrite::UpsertActive {
                row_key: api_row_id.row_key,
                snapshot: stored_row.snapshot,
            }
        },
    })
}
