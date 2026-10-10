//! Validates and commits local publication batches.
//!
//! Collects each request's group and touched rows, rebases its mutations,
//! persists the resulting update, and produces committed delivery and listener data.

use super::row_patches::apply_dataset_row_patches;
use crate::{
    MAX_VERSION_VALUE,
    api::{
        DatasetId,
        DatasetRowStatePatch,
        DatasetRowStateWrite,
        DatasetUpdateRecord,
        GroupReadToken,
        GroupSchema,
        PublishChangesRequest,
        PublishReceipt,
        ReplicationStore,
        ReplicationStoreTransaction,
        ReplicationUpdateRecord,
        RowChange,
        RowMutation,
    },
    codecs::messages::{RuntimeMessage, UpdateMessage},
    runtime::{
        errors::{PublishChangesError, publish},
        in_memory::{
            LoadedGroupMeta,
            PreparedLocalChanges,
            TouchedGroupRows,
            collect_group_row_scope,
        },
        replay,
    },
};
use flotsync_core::{
    GroupId,
    MemberIdentity,
    versions::{UpdateId, VersionVector},
};
use flotsync_messages::proto::EncodeProto;
use snafu::prelude::*;
use std::collections::HashMap;

/// Require a non-empty mutation list whose row identities all belong to one group.
///
/// Returns `EmptyChanges` for an empty list and `MixedGroups` for differing group ids.
/// Collects unique touched row keys per dataset and preserves the original mutation order.
pub(super) fn scope_request(
    request: PublishChangesRequest,
) -> Result<ScopedPublication, PublishChangesError> {
    let scope = collect_group_row_scope(&request.changes)?;
    Ok(ScopedPublication { request, scope })
}

/// Publish one local change batch through one store transaction.
///
/// The schema must be the resolved schema for the scoped request's group.
/// Success owns the encoded result of the committed write; failure reports the
/// publication validation error or the contextual store error.
pub(super) async fn publish_changes_transactionally(
    store: &dyn ReplicationStore,
    local_member: &MemberIdentity,
    group_schema: &GroupSchema,
    scoped: ScopedPublication,
) -> Result<CommittedPublication, PublishChangesError> {
    scoped.validate_dataset_schemas(group_schema)?;
    let ScopedPublication { request, scope } = scoped;
    let PublishChangesRequest {
        read_token,
        changes,
    } = request;
    let TouchedGroupRows {
        group_id,
        dataset_rows,
    } = scope;
    let mut transaction = store
        .begin_transaction()
        .await
        .context(publish::StoreAccessSnafu)?;
    let mut local_group = load_writable_group(transaction.as_mut(), local_member, group_id).await?;
    validate_publication_read_token(&read_token, group_id, &local_group)?;
    let read_versions = read_token.version().clone();
    let mut dataset_state = replay::load_publish_dataset_state(
        transaction.as_mut(),
        group_schema,
        group_id,
        local_group.member_count(),
        dataset_rows,
        &read_versions,
    )
    .await?;
    let update_id = next_local_update_id(&local_group, group_id)?;
    let last_changed_versions = read_versions.with_update_applied(update_id);
    let prepared_local_changes = build_local_dataset_updates(
        group_id,
        &mut dataset_state,
        changes,
        update_id,
        &last_changed_versions,
    )?;
    local_group.mark_applied(update_id);

    let persisted_update = ReplicationUpdateRecord {
        group_id,
        update_id,
        sender: local_member.clone(),
        read_versions: read_versions.clone(),
        dataset_updates: prepared_local_changes.dataset_updates.clone(),
        applied_locally: true,
    };
    apply_dataset_row_patches(
        transaction.as_mut(),
        group_schema,
        prepared_local_changes.row_patches,
    )
    .await
    .context(publish::StoreAccessSnafu)?;
    transaction
        .append_replication_update(persisted_update)
        .await
        .context(publish::StoreAccessSnafu)?;
    transaction
        .update_replication_group_version_vector(&group_id, local_group.version_vector)
        .await
        .context(publish::StoreAccessSnafu)?;
    let read_token = read_token.with_update_applied(update_id);
    transaction
        .commit()
        .await
        .context(publish::StoreAccessSnafu)?;

    let message = RuntimeMessage::Update(Box::new(UpdateMessage {
        group_id,
        update_id,
        read_versions,
        dataset_updates: prepared_local_changes
            .dataset_updates
            .into_iter()
            .map(Into::into)
            .collect(),
    }));
    let payload = message.encode_proto_to_bytes();
    Ok(CommittedPublication {
        group_id,
        update_id,
        read_token,
        payload,
        row_changes: prepared_local_changes.row_changes,
    })
}

/// Owned request containing at least one mutation, with all row identities in one group.
///
/// `scope_request` constructs this input and collects unique touched row keys per dataset.
/// The request keeps the original mutation order. Dataset schemas are checked before
/// the transaction; writable-group and read-token checks use its stored group state.
pub(super) struct ScopedPublication {
    /// Original mutation order and read position supplied by the application.
    request: PublishChangesRequest,
    /// Group and touched rows derived from the request exactly once.
    scope: TouchedGroupRows,
}

impl ScopedPublication {
    /// Return the group for which the component must resolve an immutable schema.
    pub(super) const fn group_id(&self) -> GroupId {
        self.scope.group_id
    }

    /// Require a schema for every touched dataset before opening the write transaction.
    ///
    /// Returns `MissingDatasetSchema` for a touched dataset absent from the group schema.
    fn validate_dataset_schemas(
        &self,
        group_schema: &GroupSchema,
    ) -> Result<(), PublishChangesError> {
        for dataset_id in self.scope.dataset_rows.keys() {
            ensure!(
                group_schema.schema(dataset_id).is_some(),
                publish::MissingDatasetSchemaSnafu {
                    group_id: self.scope.group_id,
                    dataset_id: dataset_id.clone(),
                }
            );
        }
        Ok(())
    }
}

/// Encoded publication returned only after its write transaction has committed.
///
/// Private fields prevent the component from constructing a successful result
/// before commit. The result owns post-commit delivery and listener data; it does
/// not assert that delivery or the application listener callback has already completed.
pub(super) struct CommittedPublication {
    /// Hosted group whose row state, update record and vector were committed together.
    group_id: GroupId,
    /// Local update allocated and applied by the committed transaction.
    update_id: UpdateId,
    /// Application read position extended by this publication.
    read_token: GroupReadToken,
    /// Protocol update encoded after commit for best-effort group submission.
    payload: bytes::Bytes,
    /// Application-visible row changes awaiting the listener's callback.
    row_changes: Vec<RowChange>,
}

impl CommittedPublication {
    /// Return the group for outbound submission and catch-up observation.
    pub(super) const fn group_id(&self) -> GroupId {
        self.group_id
    }

    /// Return the committed update for catch-up observation.
    pub(super) const fn update_id(&self) -> UpdateId {
        self.update_id
    }

    /// Borrow the encoded update until the component submits it.
    pub(super) fn payload(&self) -> &bytes::Bytes {
        &self.payload
    }

    /// Consume the result into its API receipt and pending listener row changes.
    ///
    /// The component must await the listener callback before replying with the receipt.
    pub(super) fn into_listener_data(self) -> (PublishReceipt, Vec<RowChange>) {
        let receipt = PublishReceipt {
            update_id: self.update_id,
            read_token: self.read_token,
        };
        (receipt, self.row_changes)
    }
}

/// Load an existing writable group and validate its persisted metadata for the local member.
///
/// Reports store access failures, `UnknownGroup` for absence, `GroupNotWritable` for a
/// lifecycle that rejects writes, and `InvalidPersistedGroup` for inconsistent metadata.
/// The caller owns the transaction until subsequent reads, writes and commit finish.
async fn load_writable_group(
    transaction: &mut dyn ReplicationStoreTransaction,
    local_member: &MemberIdentity,
    group_id: GroupId,
) -> Result<LoadedGroupMeta, PublishChangesError> {
    let persisted_group = transaction
        .load_replication_group(&group_id)
        .await
        .context(publish::StoreAccessSnafu)?;
    let persisted_group = persisted_group.context(publish::UnknownGroupSnafu { group_id })?;
    ensure!(
        persisted_group.lifecycle.is_writable(),
        publish::GroupNotWritableSnafu { group_id }
    );
    LoadedGroupMeta::from_replication_group_record(local_member, persisted_group)
        .context(publish::InvalidPersistedGroupSnafu { group_id })
}

/// Require this group's token with a matching member count and a frontier bounded by its vector.
///
/// Reports `ReadTokenGroupMismatch`, then `ReadTokenMemberCountMismatch`, then
/// `ReadTokenAheadOfLocalState` in that validation order. Success permits a token
/// behind the stored vector so publication can rebase mutations from its read position.
fn validate_publication_read_token(
    read_token: &GroupReadToken,
    group_id: GroupId,
    local_group: &LoadedGroupMeta,
) -> Result<(), PublishChangesError> {
    ensure!(
        read_token.group_id() == group_id,
        publish::ReadTokenGroupMismatchSnafu {
            group_id,
            read_token_group_id: read_token.group_id(),
        }
    );
    let read_versions = read_token.version();
    ensure!(
        read_versions.num_members() == local_group.member_count(),
        publish::ReadTokenMemberCountMismatchSnafu {
            group_id,
            read_token_member_count: read_versions.num_members().get(),
            persisted_member_count: local_group.member_count().get(),
        }
    );
    ensure!(
        local_group.version_vector >= *read_versions,
        publish::ReadTokenAheadOfLocalStateSnafu { group_id }
    );
    Ok(())
}

/// Allocate the next local update, rejecting the reserved/exhausted version range.
fn next_local_update_id(
    local_group: &LoadedGroupMeta,
    group_id: GroupId,
) -> Result<UpdateId, PublishChangesError> {
    let applied_version = local_group.applied_version(local_group.local_member_index);
    ensure!(
        applied_version < MAX_VERSION_VALUE,
        publish::ExhaustedUpdateIdsSnafu { group_id }
    );
    let next_local_version = applied_version + 1;
    Ok(UpdateId {
        version: next_local_version,
        node_index: local_group.local_member_index.as_u32(),
    })
}

/// Stage local row mutations as schema operations for one replication update.
///
/// This is the local `SchemaOperation -> Update(ReadVV)` boundary: the
/// caller supplies the freshly allocated `UpdateId`, and every effective
/// schema operation produced for the publish batch is encoded with exactly
/// that change id. The surrounding publish transaction captures `ReadVV`
/// before applying the mutations, persists the same `UpdateId` and `ReadVV`,
/// and sends both values in the outbound `Update`.
///
/// `last_changed_versions` is the causal frontier of the new update and is
/// stored on every row image affected by the publish.
fn build_local_dataset_updates(
    group_id: GroupId,
    dataset_state: &mut replay::PublishDatasetState,
    changes: Vec<RowMutation>,
    update_id: UpdateId,
    last_changed_versions: &VersionVector,
) -> Result<PreparedLocalChanges, PublishChangesError> {
    let mut encoded_operations: HashMap<
        DatasetId,
        Vec<flotsync_messages::datamodel::SchemaOperation>,
    > = HashMap::new();
    let mut row_writes: HashMap<DatasetId, Vec<DatasetRowStateWrite>> = HashMap::new();
    let mut row_changes = Vec::new();

    'changes: for mutation in changes {
        let row_id = mutation.row_id().clone();
        let applied_operation = match mutation {
            RowMutation::Upsert { row, .. } => {
                dataset_state.apply_upsert(&row_id, row, update_id)?
            }
            RowMutation::Delete { .. } => {
                let applied_delete = dataset_state.apply_delete(&row_id, update_id)?;
                Some(applied_delete)
            }
        };
        let Some(applied_operation) = applied_operation else {
            continue 'changes;
        };
        encoded_operations
            .entry(row_id.dataset_id.clone())
            .or_default()
            .push(applied_operation.encoded_operation);
        if let Some(row_change) = applied_operation.row_change {
            row_changes.push(row_change);
        }
        row_writes
            .entry(row_id.dataset_id.clone())
            .or_default()
            .push(applied_operation.row_write);
    }

    ensure!(
        !encoded_operations.is_empty(),
        publish::NoEffectiveChangesSnafu { group_id }
    );
    let dataset_updates = encoded_operations
        .into_iter()
        .map(|(dataset_id, operations)| DatasetUpdateRecord {
            dataset_id,
            operations,
        })
        .collect();
    let row_patches = row_writes
        .into_iter()
        .map(|(dataset_id, actions)| DatasetRowStatePatch {
            group_id,
            dataset_id,
            actions,
            change_id: update_id,
            last_changed_versions: last_changed_versions.clone(),
        })
        .collect();
    Ok(PreparedLocalChanges {
        dataset_updates,
        row_patches,
        row_changes,
    })
}

/// Construct a synthetic committed result for pure FSM tests that never run a store transaction.
#[cfg(test)]
pub(super) fn committed_publication_for_test(
    group_id: GroupId,
    receipt: PublishReceipt,
    row_changes: Vec<RowChange>,
) -> CommittedPublication {
    CommittedPublication {
        group_id,
        update_id: receipt.update_id,
        read_token: receipt.read_token,
        payload: bytes::Bytes::from_static(b"committed update"),
        row_changes,
    }
}
