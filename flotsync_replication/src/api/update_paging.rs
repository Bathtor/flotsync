//! Borrowed projections and query parameters for replication-update paging.

use super::{
    DatasetId,
    DatasetUpdateRecord,
    PageBatchInput,
    ReplicationUpdateFilter,
    ReplicationUpdateRecord,
};
use crate::codecs::messages::{MemberCountContext, validate_update_message_view};
use flotsync_core::{
    GroupId,
    MemberIdentity,
    versions::{UpdateId, VersionVector},
};
use flotsync_messages::{datamodel::SchemaOperationView, replication as replication_proto};
use flotsync_utils::BoxError;
use std::{collections::HashSet, num::NonZeroUsize};

/// Immutable selection for one pageable replication-update query.
///
/// A selected ID set is borrowed for the cursor's lifetime. Store backends may
/// apply that selection using their own data layout or query facilities.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicationUpdatesQuery<'a> {
    /// Group whose update log is selected.
    group_id: GroupId,
    /// Predicate applied to the group's update log.
    filter: ReplicationUpdateFilter,
    /// Optional exact update identities included in the selection.
    update_ids: Option<&'a HashSet<UpdateId>>,
}

impl<'a> ReplicationUpdatesQuery<'a> {
    /// Build one update-log selection.
    #[must_use]
    pub const fn new(group_id: GroupId, filter: ReplicationUpdateFilter) -> Self {
        Self {
            group_id,
            filter,
            update_ids: None,
        }
    }

    /// Intersect the update filter with these exact identities for this cursor.
    #[must_use]
    pub const fn with_update_ids(mut self, update_ids: &'a HashSet<UpdateId>) -> Self {
        self.update_ids = Some(update_ids);
        self
    }

    /// Return the selected replication group.
    #[must_use]
    pub const fn group_id(&self) -> GroupId {
        self.group_id
    }

    /// Return the update predicate applied within the group.
    #[must_use]
    pub const fn filter(&self) -> ReplicationUpdateFilter {
        self.filter
    }

    /// Return the optional exact-identity selection.
    ///
    /// `Some` restricts results to the supplied identities, including an empty
    /// set that selects no updates. `None` applies only the update filter.
    #[must_use]
    pub const fn update_ids(&self) -> Option<&'a HashSet<UpdateId>> {
        self.update_ids
    }
}

/// One validated dataset entry borrowed from a stored replication update.
#[derive(Clone, Copy)]
pub struct ReplicationDatasetUpdateView<'record> {
    /// Generated view whose dataset id and non-empty operation collection were validated.
    source: &'record replication_proto::DatasetUpdateView<'record>,
}

impl<'record> ReplicationDatasetUpdateView<'record> {
    /// Build a public projection over one validated generated view.
    pub(crate) const fn new<'payload: 'record>(
        source: &'record replication_proto::DatasetUpdateView<'payload>,
    ) -> Self {
        Self { source }
    }

    /// Return the validated dataset identifier text.
    #[must_use]
    pub const fn dataset_id(&self) -> &str {
        self.source.dataset_id
    }

    /// Iterate over the generated operation views in transport order.
    ///
    /// The returned operations borrow the temporary stored update payload and
    /// cannot outlive the surrounding page-batch projection call.
    #[must_use]
    pub fn operations(&self) -> impl ExactSizeIterator<Item = &SchemaOperationView<'record>> + '_ {
        self.source.operations.iter()
    }
}

/// One validated replication update borrowed for synchronous projection.
///
/// Scalar and small decoded metadata are exposed without retaining the encoded
/// update payload. Dataset operations remain generated borrowed views and are
/// valid only during the [`super::PageBatch::push`] call which received this
/// value.
pub struct ReplicationUpdateView<'record> {
    /// Group whose log contains the update.
    group_id: GroupId,
    /// Indexed and payload-validated update identity.
    update_id: UpdateId,
    /// Decoded sender stored alongside the payload.
    sender: &'record MemberIdentity,
    /// Validated sender frontier decoded from the payload.
    read_versions: VersionVector,
    /// Generated update payload containing validated dataset entries.
    source: &'record replication_proto::UpdateView<'record>,
    /// Whether the update is reflected in local dataset state.
    applied_locally: bool,
}

impl<'record> ReplicationUpdateView<'record> {
    /// Validate a decoded update payload for projection by a store backend.
    ///
    /// `member_count` is the size of the update's group at this stored version.
    /// The returned view borrows `source` and `sender`; neither may be released
    /// until the batch's synchronous `push` call has returned. Backends remain
    /// responsible for comparing the decoded group and update id with their
    /// indexed columns before offering this view to a batch.
    ///
    /// # Errors
    ///
    /// Returns the source validation error if required metadata, version bounds,
    /// dataset ids, or non-empty operation collections are invalid.
    pub fn try_from_proto_view<'payload: 'record>(
        sender: &'record MemberIdentity,
        source: &'record replication_proto::UpdateView<'payload>,
        applied_locally: bool,
        member_count: NonZeroUsize,
    ) -> Result<Self, BoxError> {
        let validated =
            validate_update_message_view(source, MemberCountContext::new(member_count))?;
        Ok(Self {
            group_id: validated.group_id,
            update_id: validated.update_id,
            sender,
            read_versions: validated.read_versions,
            source,
            applied_locally,
        })
    }

    /// Return the replication group containing the update.
    #[must_use]
    pub const fn group_id(&self) -> GroupId {
        self.group_id
    }

    /// Return the update's deterministic log identity.
    #[must_use]
    pub const fn update_id(&self) -> UpdateId {
        self.update_id
    }

    /// Return the logical sender stored with the update.
    #[must_use]
    pub const fn sender(&self) -> &MemberIdentity {
        self.sender
    }

    /// Return the validated sender frontier carried by the update.
    #[must_use]
    pub const fn read_versions(&self) -> &VersionVector {
        &self.read_versions
    }

    /// Iterate over validated dataset entries in transport order.
    pub fn dataset_updates(
        &self,
    ) -> impl ExactSizeIterator<Item = ReplicationDatasetUpdateView<'record>> + '_ {
        self.source
            .dataset_updates
            .iter()
            .map(ReplicationDatasetUpdateView::new)
    }

    /// Return `true` when this update is reflected in stored local state.
    #[must_use]
    pub const fn applied_locally(&self) -> bool {
        self.applied_locally
    }

    /// Materialise this validated temporary view for a legacy owned-result adapter.
    ///
    /// # Errors
    ///
    /// Returns the generated-view ownership error if a nested operation cannot
    /// be materialised, or a dataset-id error if the earlier validation and
    /// ownership conversion disagree.
    pub(crate) fn try_to_owned_record(self) -> Result<ReplicationUpdateRecord, BoxError> {
        let dataset_updates = self.try_to_owned_dataset_updates()?;
        Ok(ReplicationUpdateRecord {
            group_id: self.group_id,
            update_id: self.update_id,
            sender: self.sender.clone(),
            read_versions: self.read_versions,
            dataset_updates,
            applied_locally: self.applied_locally,
        })
    }

    /// Materialise dataset operations for a retained update or catch-up message.
    ///
    /// # Errors
    ///
    /// Returns an invalid dataset id or generated-view ownership error.
    pub(crate) fn try_to_owned_dataset_updates(
        &self,
    ) -> Result<Vec<DatasetUpdateRecord>, BoxError> {
        let mut dataset_updates = Vec::with_capacity(self.source.dataset_updates.len());
        for dataset_update in self.dataset_updates() {
            let dataset_id = DatasetId::try_from_owned(dataset_update.dataset_id().to_owned())?;
            let operations = dataset_update
                .operations()
                .map(flotsync_messages::buffa::MessageView::to_owned_message)
                .collect::<Result<Vec<_>, _>>()?;
            dataset_updates.push(DatasetUpdateRecord {
                dataset_id,
                operations,
            });
        }
        Ok(dataset_updates)
    }
}

/// Input family accepting one temporary validated replication-update view.
pub struct ReplicationUpdatePageInput;

impl PageBatchInput for ReplicationUpdatePageInput {
    type Value<'record> = ReplicationUpdateView<'record>;
}
