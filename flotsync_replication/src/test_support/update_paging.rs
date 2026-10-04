//! Reusable backend contract checks for projected replication-update pages.

use super::ExactPageCompletion;
use crate::api::{
    DatasetId,
    InlinePageBatch,
    PageCursor,
    ReplicationStoreReadTransaction,
    ReplicationUpdateFilter,
    ReplicationUpdatePageInput,
    ReplicationUpdateRecord,
    ReplicationUpdateView,
    ReplicationUpdatesQuery,
    VecPageBatch,
};
use flotsync_core::{GroupId, MemberIndex, versions::UpdateId};
use flotsync_utils::BoxError;
use itertools::Itertools;
use std::{collections::HashSet, num::NonZeroUsize};

/// Check update projections, lightweight IDs, selection and inclusive producer filters.
///
/// The caller prepares the fixture records through its backend's public write
/// interface. The same assertions can run before commit to check read-your-own-writes.
/// Expected traversal order comes from the backend-specific fixture, independently
/// of the record layout used to describe filter expectations.
/// Returns the observed exact-boundary completion policy so backend-specific
/// tests can assert their chosen policy without imposing it on other engines.
///
/// # Panics
///
/// Panics if a read fails, the fixture violates its requirements, or a backend
/// violates paging order, projection, selection or filter semantics.
pub async fn assert_update_paging_contract(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    fixture: &UpdatePagingFixtures<'_>,
) -> ExactPageCompletion {
    let completion = assert_projected_update_pages(transaction, fixture).await;
    assert_bounded_update_id_pages(transaction, fixture).await;
    assert_update_filters_and_limit(transaction, fixture).await;
    completion
}

/// Caller-prepared records for the update paging scenarios.
pub struct UpdatePagingFixtures<'a> {
    /// Group containing all five updates.
    pub group_id: GroupId,
    /// The single dataset in each update, with one operation per update.
    pub dataset_id: &'a DatasetId,
    /// Fixture records: producer 0/version 1 (pending),
    /// producer 1/version 1 (applied), producer 0/versions 2 and 3 (applied),
    /// and producer 1 at the maximum supported version (pending).
    pub updates: &'a [ReplicationUpdateRecord; 5],
    /// The five fixture identities in this backend's bounded traversal order.
    /// Filtered queries must preserve their relative order.
    pub traversal_order: [UpdateId; 5],
}

impl UpdatePagingFixtures<'_> {
    /// Find the expected record for an identity in the caller's traversal order.
    /// Panics if that order contains an identity absent from the fixture records.
    fn update(&self, update_id: UpdateId) -> &ReplicationUpdateRecord {
        self.updates
            .iter()
            .find(|update| update.update_id == update_id)
            .expect("traversal order must refer to a fixture update")
    }
}

/// Check inline and Vec projections, changed limits, and exact-page exhaustion.
/// Returns whether exhaustion was known with the last record or after an empty fill.
async fn assert_projected_update_pages(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    fixture: &UpdatePagingFixtures<'_>,
) -> ExactPageCompletion {
    let mut projected_cursor = PageCursor::new(ReplicationUpdatesQuery::new(
        fixture.group_id,
        ReplicationUpdateFilter::All,
    ));
    let mut projected = Vec::new();
    let mut first_batch =
        InlinePageBatch::<ProjectedUpdateSummary, (), 1, ReplicationUpdatePageInput, _>::new_with(
            project_update_summary,
        );
    transaction
        .load_replication_updates_into(&mut projected_cursor, &mut first_batch)
        .await
        .expect("first projected update page should load");
    assert_eq!(first_batch.values().len(), 1);
    projected.extend_from_slice(first_batch.values());

    let mut final_page_len = usize::MAX;
    while projected_cursor.has_more() {
        let mut batch =
            VecPageBatch::<ProjectedUpdateSummary, (), ReplicationUpdatePageInput, _>::bounded_with(
                NonZeroUsize::new(2).expect("two updates per page"),
                project_update_summary,
            );
        transaction
            .load_replication_updates_into(&mut projected_cursor, &mut batch)
            .await
            .expect("continued projected update page should load");
        final_page_len = batch.values().len();
        projected.extend(batch.into_values());
    }
    let completion = if final_page_len == 0 {
        ExactPageCompletion::AfterEmptyPage
    } else {
        ExactPageCompletion::WithLastRecord
    };
    assert_eq!(
        projected
            .iter()
            .map(|summary| summary.update_id)
            .collect::<Vec<_>>(),
        fixture.traversal_order
    );
    assert!(projected.iter().all(|summary| {
        summary.dataset_id == fixture.dataset_id.as_str() && summary.operation_count == 1
    }));
    for summary in &projected {
        assert_eq!(
            summary.applied_locally,
            fixture.update(summary.update_id).applied_locally
        );
    }

    let selected_ids = HashSet::from([fixture.updates[2].update_id, fixture.updates[4].update_id]);
    let query = ReplicationUpdatesQuery::new(fixture.group_id, ReplicationUpdateFilter::All)
        .with_update_ids(&selected_ids);
    let mut selected_cursor = PageCursor::new(query);
    let mut selected_batch =
        VecPageBatch::<ProjectedUpdateSummary, (), ReplicationUpdatePageInput, _>::bounded_with(
            NonZeroUsize::new(1).expect("one update per page"),
            project_update_summary,
        );
    let mut selected = Vec::new();
    while selected_cursor.has_more() {
        transaction
            .load_replication_updates_into(&mut selected_cursor, &mut selected_batch)
            .await
            .expect("selected update page should load");
        selected.extend(
            selected_batch
                .values()
                .iter()
                .map(|summary| summary.update_id),
        );
    }
    assert_eq!(
        selected,
        fixture
            .traversal_order
            .iter()
            .copied()
            .filter(|id| selected_ids.contains(id))
            .collect::<Vec<_>>()
    );
    completion
}

/// Check that lightweight ID reads cross the same-version producer boundary.
async fn assert_bounded_update_id_pages(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    fixture: &UpdatePagingFixtures<'_>,
) {
    let mut ids_cursor = PageCursor::new(ReplicationUpdatesQuery::new(
        fixture.group_id,
        ReplicationUpdateFilter::All,
    ));
    let mut ids_batch =
        VecPageBatch::<UpdateId, ()>::bounded(NonZeroUsize::new(1).expect("one update per page"));
    let mut paged_ids = Vec::new();
    while ids_cursor.has_more() {
        transaction
            .load_replication_update_ids_into(&mut ids_cursor, &mut ids_batch)
            .await
            .expect("update ids should load");
        paged_ids.extend_from_slice(ids_batch.values());
    }
    assert!(ids_cursor.is_exhausted());
    assert_eq!(paged_ids, fixture.traversal_order);

    let selected_ids = HashSet::from([fixture.updates[2].update_id]);
    let query = ReplicationUpdatesQuery::new(fixture.group_id, ReplicationUpdateFilter::All)
        .with_update_ids(&selected_ids);
    let mut selected_cursor = PageCursor::new(query);
    transaction
        .load_replication_update_ids_into(&mut selected_cursor, &mut ids_batch)
        .await
        .expect("selected update id should load");
    assert_eq!(ids_batch.values(), &[fixture.updates[2].update_id]);
}

/// Compare projected updates and ID selection for every filter and one finite limit.
async fn assert_update_filters_and_limit(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    fixture: &UpdatePagingFixtures<'_>,
) {
    let [alice_v1, bob_v1, alice_v2, alice_v3, bob_max] = fixture.updates;
    let filter_cases = [
        (
            ReplicationUpdateFilter::PendingApply,
            vec![alice_v1.update_id, bob_max.update_id],
        ),
        (
            ReplicationUpdateFilter::Applied,
            vec![bob_v1.update_id, alice_v2.update_id, alice_v3.update_id],
        ),
        (
            ReplicationUpdateFilter::ProducerRange {
                producer_index: MemberIndex::new(0),
                start_version: 2,
                end_version: 3,
            },
            vec![alice_v2.update_id, alice_v3.update_id],
        ),
        (
            ReplicationUpdateFilter::ProducerRange {
                producer_index: MemberIndex::new(0),
                start_version: 3,
                end_version: 2,
            },
            Vec::new(),
        ),
    ];
    for (filter, expected) in filter_cases {
        let query = ReplicationUpdatesQuery::new(fixture.group_id, filter);
        let mut updates_cursor = PageCursor::new(query);
        let mut updates = VecPageBatch::<
            ProjectedUpdateSummary,
            (),
            ReplicationUpdatePageInput,
            _,
        >::unlimited_with(project_update_summary);
        transaction
            .load_replication_updates_into(&mut updates_cursor, &mut updates)
            .await
            .expect("filtered updates should load");
        let mut ids_cursor = PageCursor::new(query);
        let mut ids = VecPageBatch::unlimited();
        transaction
            .load_replication_update_ids_into(&mut ids_cursor, &mut ids)
            .await
            .expect("filtered update ids should load");
        let update_ids = updates
            .values()
            .iter()
            .map(|update| update.update_id)
            .sorted()
            .collect::<Vec<_>>();
        assert_eq!(
            update_ids,
            expected.iter().copied().sorted().collect::<Vec<_>>()
        );
        assert_eq!(
            ids.into_values().into_iter().sorted().collect::<Vec<_>>(),
            update_ids
        );
    }

    let query = ReplicationUpdatesQuery::new(
        fixture.group_id,
        ReplicationUpdateFilter::ProducerRange {
            producer_index: MemberIndex::new(0),
            start_version: 2,
            end_version: 3,
        },
    );
    let mut cursor = PageCursor::new(query);
    let own_update = |view: ReplicationUpdateView<'_>| view.try_to_owned_record();
    let mut limited_alice = VecPageBatch::bounded_with(
        NonZeroUsize::new(1).expect("one update per page"),
        own_update,
    );
    transaction
        .load_replication_updates_into(&mut cursor, &mut limited_alice)
        .await
        .expect("bounded update query should load");
    let first_selected_id = fixture
        .traversal_order
        .iter()
        .find(|id| [alice_v2.update_id, alice_v3.update_id].contains(id))
        .expect("producer range must contain two fixture updates");
    let expected_update = fixture.update(*first_selected_id);
    assert_eq!(
        limited_alice.values(),
        std::slice::from_ref(expected_update)
    );
}

/// Copy only the fields needed by the projected-update paging scenario.
#[allow(
    clippy::needless_pass_by_value,
    clippy::unnecessary_wraps,
    reason = "page projections share the reusable fallible batch callback signature"
)]
fn project_update_summary(
    update: ReplicationUpdateView<'_>,
) -> Result<ProjectedUpdateSummary, BoxError> {
    let mut datasets = update.dataset_updates();
    let dataset = datasets
        .next()
        .expect("test updates should contain one dataset");
    assert!(
        datasets.next().is_none(),
        "test updates should contain exactly one dataset"
    );
    Ok(ProjectedUpdateSummary {
        update_id: update.update_id(),
        dataset_id: dataset.dataset_id().to_owned(),
        operation_count: dataset.operations().count(),
        applied_locally: update.applied_locally(),
    })
}

/// Small owned projection used to prove update pages need not own payloads.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ProjectedUpdateSummary {
    /// Selected update identity.
    update_id: UpdateId,
    /// Dataset identifier copied from the temporary payload view.
    dataset_id: String,
    /// Number of borrowed operations observed without materialising them.
    operation_count: usize,
    /// Whether this update is already reflected in local state.
    applied_locally: bool,
}
