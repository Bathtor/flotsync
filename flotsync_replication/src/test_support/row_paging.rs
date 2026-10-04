//! Reusable backend checks for ordinary, requested and transition row pages.

use super::ExactPageCompletion;
use crate::api::{
    DatasetRowPageBatch,
    DatasetRowTransitionPageBatch,
    DatasetRowTransitionQuery,
    DatasetRowsQuery,
    GroupDatasetSchemaRef,
    PageCursor,
    ReplicationRowMetadata,
    ReplicationStoreReadTransaction,
    RequestedDatasetRowPageBatch,
    RequestedDatasetRowView,
    RequestedDatasetRowsQuery,
    RowKey,
};
use flotsync_core::versions::UpdateId;
use std::num::NonZeroUsize;

/// Check two ordinary row pages with different batch sizes and preserved metadata.
///
/// The dataset must contain exactly the two `expected` records, supplied in
/// ascending backend traversal order. Fixture preparation remains with the caller.
///
/// # Panics
///
/// Panics if reads fail or paging returns unexpected row metadata or exhaustion.
pub async fn assert_row_scan_paging_contract(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    dataset: GroupDatasetSchemaRef<'_>,
    expected: &[ReplicationRowMetadata; 2],
) {
    let mut cursor = PageCursor::new(DatasetRowsQuery::borrowed(dataset));
    let mut first_page = DatasetRowPageBatch::bounded(
        dataset.schema,
        NonZeroUsize::new(1).expect("limit should be non-zero"),
    );
    transaction
        .scan_dataset_rows_into(&mut cursor, &mut first_page)
        .await
        .expect("first batch should scan");
    let first_row = first_page
        .rows()
        .row(0)
        .expect("first scan should return one row");
    assert_eq!(first_row.metadata(), &expected[0]);
    assert!(cursor.has_more());
    let mut second_page = DatasetRowPageBatch::bounded(
        dataset.schema,
        NonZeroUsize::new(2).expect("rotated limit should be non-zero"),
    );
    transaction
        .scan_dataset_rows_into(&mut cursor, &mut second_page)
        .await
        .expect("second batch should scan");
    let second_row = second_page
        .rows()
        .row(0)
        .expect("second scan should return one row");
    assert_eq!(second_row.metadata(), &expected[1]);
    assert!(cursor.is_exhausted());
    assert!(
        second_page
            .metadata()
            .expect("successful page should have metadata")
            .dataset_exists
    );
}

/// Check deduplicated requested keys and explicit present/missing outcomes.
///
/// `present` must exist in the dataset; `missing` must be absent and sort after
/// `present` in the requested-key selection. The repeated present key is returned once.
/// Returns whether the final outcome exhausted the cursor immediately or an empty
/// confirmation page established exhaustion.
///
/// # Panics
///
/// Panics if reads fail or requested-key outcomes, continuation or exhaustion differ.
pub async fn assert_requested_row_paging_contract(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    dataset: GroupDatasetSchemaRef<'_>,
    present: RowKey,
    missing: RowKey,
) -> ExactPageCompletion {
    let query = RequestedDatasetRowsQuery::new(
        DatasetRowsQuery::borrowed(dataset),
        [missing, present, present],
    );
    let mut cursor = PageCursor::new(query);
    let mut page = RequestedDatasetRowPageBatch::bounded(
        dataset.schema,
        NonZeroUsize::new(1).expect("requested-row page limit should be non-zero"),
    );
    transaction
        .load_dataset_rows_into(&mut cursor, &mut page)
        .await
        .expect("first requested-row page should load");
    assert!(matches!(
        page.outcomes().collect::<Vec<_>>().as_slice(),
        [RequestedDatasetRowView::Present(row)] if row.metadata().row_key == present
    ));
    assert!(cursor.has_more());
    transaction
        .load_dataset_rows_into(&mut cursor, &mut page)
        .await
        .expect("second requested-row page should load");
    assert!(matches!(
        page.outcomes().collect::<Vec<_>>().as_slice(),
        [RequestedDatasetRowView::Missing(key)] if *key == missing
    ));
    let completion = if cursor.has_more() {
        transaction
            .load_dataset_rows_into(&mut cursor, &mut page)
            .await
            .expect("empty requested-row confirmation page should load");
        assert!(page.outcomes().next().is_none());
        ExactPageCompletion::AfterEmptyPage
    } else {
        ExactPageCompletion::WithLastRecord
    };
    assert!(cursor.is_exhausted());
    completion
}

/// Check three transitions, including a row present on each side and a tombstone.
///
/// Both occurrences must exist and use the same dataset id. `keys` supplies the
/// backend traversal order: previous-only, current-only, then a row on both sides.
/// The shared row has `previous_creation` on the previous side; its current side
/// is a tombstone without a creation identity. The schemas may differ.
///
/// # Panics
///
/// Panics if reads fail or side presence, row metadata, query metadata or exhaustion differ.
pub async fn assert_transition_paging_contract(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    previous: GroupDatasetSchemaRef<'_>,
    current: GroupDatasetSchemaRef<'_>,
    keys: [RowKey; 3],
    previous_creation: UpdateId,
) {
    let mut cursor = PageCursor::new(DatasetRowTransitionQuery::new(
        DatasetRowsQuery::borrowed(previous),
        DatasetRowsQuery::borrowed(current),
    ));
    let mut page = DatasetRowTransitionPageBatch::bounded(
        previous.schema,
        current.schema,
        NonZeroUsize::new(2).expect("limit should be non-zero"),
    );
    transaction
        .scan_dataset_row_transitions_into(&mut cursor, &mut page)
        .await
        .expect("first transition batch should scan");
    let metadata = page
        .metadata()
        .expect("first transition batch should have metadata");
    assert_eq!(&metadata.dataset_id, previous.dataset_id);
    assert_eq!(metadata.previous_group_id, *previous.group_id);
    assert_eq!(metadata.current_group_id, *current.group_id);
    assert!(metadata.previous_dataset_exists);
    assert!(metadata.current_dataset_exists);
    let presence = page
        .transitions()
        .rows()
        .map(|row| {
            (
                row.row_key(),
                row.previous().is_some(),
                row.current().is_some(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        presence,
        vec![(keys[0], true, false), (keys[1], false, true)]
    );
    assert!(cursor.has_more());
    transaction
        .scan_dataset_row_transitions_into(&mut cursor, &mut page)
        .await
        .expect("second transition batch should scan");
    assert_eq!(page.transitions().len(), 1);
    let transition = page.transitions().row(0).expect("transition must exist");
    assert_eq!(transition.row_key(), keys[2]);
    assert_eq!(
        transition.previous().map(|row| row.metadata().created_by),
        Some(Some(previous_creation))
    );
    assert_eq!(
        transition
            .current()
            .map(|row| (row.metadata().created_by, row.metadata().tombstoned)),
        Some((None, true))
    );
    assert!(cursor.is_exhausted());
}
