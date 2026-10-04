//! Reusable backend checks for reliable-delivery metadata pagination.

use super::ExactPageCompletion;
use crate::{
    api::{PageCursor, PageError, VecPageBatch},
    delivery::contracts::{ReliableDeliveryStore, StoredReliableDeliveryWorkMetadata},
};
use std::num::NonZeroUsize;

/// Check metadata continuation, session binding and exact-size exhaustion.
///
/// The store must contain exactly `earlier` and `later`, in that backend's
/// bounded traversal order. No encoded envelopes are required by these checks.
/// Returns the observed exact-boundary completion policy for backend-specific assertions.
///
/// # Panics
///
/// Panics if session access fails or the backend returns unexpected metadata,
/// accepts a cursor from another session, or fails to exhaust the two-record query.
pub async fn assert_delivery_metadata_paging_contract(
    store: &dyn ReliableDeliveryStore,
    earlier: &StoredReliableDeliveryWorkMetadata,
    later: &StoredReliableDeliveryWorkMetadata,
) -> ExactPageCompletion {
    let mut session = store
        .begin_read_session()
        .await
        .expect("delivery metadata session should start");
    let mut cursor = PageCursor::new(());
    let mut batch = VecPageBatch::<StoredReliableDeliveryWorkMetadata, ()>::bounded(
        NonZeroUsize::new(1).expect("page size is non-zero"),
    );
    session
        .load_reliable_delivery_work_metadata_into(&mut cursor, &mut batch)
        .await
        .expect("first metadata page should load");
    assert_eq!(batch.values(), std::slice::from_ref(earlier));
    let mut other_session = store
        .begin_read_session()
        .await
        .expect("second delivery metadata session should start");
    assert!(matches!(
        other_session
            .load_reliable_delivery_work_metadata_into(&mut cursor, &mut batch)
            .await,
        Err(PageError::TransactionMismatch { .. })
    ));
    other_session
        .release()
        .await
        .expect("second session should release");
    session
        .load_reliable_delivery_work_metadata_into(&mut cursor, &mut batch)
        .await
        .expect("second metadata page should load");
    assert_eq!(batch.values(), std::slice::from_ref(later));
    let completion = if cursor.has_more() {
        session
            .load_reliable_delivery_work_metadata_into(&mut cursor, &mut batch)
            .await
            .expect("empty confirmation page should load");
        assert!(batch.values().is_empty());
        ExactPageCompletion::AfterEmptyPage
    } else {
        ExactPageCompletion::WithLastRecord
    };
    assert!(cursor.is_exhausted());
    session
        .release()
        .await
        .expect("metadata session should release");
    completion
}
