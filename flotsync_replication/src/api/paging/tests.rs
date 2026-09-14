//! Shared paging contract tests.

use super::*;
use crate::api::{
    StoreError,
    StoreErrorClass,
    StoreErrorClassification,
    StoreErrorClassificationSource,
    StoreErrorResolution,
    StoreErrorScope,
};

const TRANSACTION_A: StoreTransactionId = StoreTransactionId::from_uuid(Uuid::from_u128(20));
const TRANSACTION_B: StoreTransactionId = StoreTransactionId::from_uuid(Uuid::from_u128(21));

/// Exercise an associated input family with a temporary view through a trait object.
fn fill_borrowed_view_page(
    cursor: &mut PageCursor<(), u32>,
    batch: &mut dyn PageBatch<Input = BorrowedViews, Metadata = ()>,
) -> Result<(), PageError> {
    let mut page = cursor.begin_page(TRANSACTION_A, batch)?;
    let temporary = String::from("temporary");
    page.push(1, BorrowedView { value: &temporary })?;
    page.finish(())
}

/// Simulate backend failure after a batch accepted partial output.
fn fail_store_page_after_push<Batch>(
    cursor: &mut PageCursor<(), u32>,
    batch: &mut Batch,
    source: StoreError,
) -> Result<(), PageError>
where
    Batch: PageBatch<Input = OwnedPageBatchInput<u32>, Metadata = ()> + ?Sized,
{
    let mut page = cursor.begin_page(TRANSACTION_A, batch)?;
    page.push(1, 1)?;
    Err(PageError::from_store_error(source))
}

/// Input family which accepts temporary borrowed views.
struct BorrowedViews;

impl PageBatchInput for BorrowedViews {
    type Value<'record> = BorrowedView<'record>;
}

/// Temporary store view used to prove borrowed data cannot escape a push.
#[derive(Clone, Copy)]
struct BorrowedView<'a> {
    /// Value borrowed from backend-owned temporary storage.
    value: &'a str,
}

#[test]
fn bounded_pages_advance_exclusively_and_require_a_final_empty_page() {
    let mut cursor = PageCursor::new("query");
    let mut first = VecPageBatch::<u32, &str>::bounded(NonZeroUsize::new(2).unwrap());
    {
        let mut page = cursor.begin_page(TRANSACTION_A, &mut first).unwrap();
        assert_eq!(page.params(), &"query");
        assert_eq!(page.after(), None);
        page.push(1, 1).unwrap();
        page.push(2, 2).unwrap();
        page.finish("first").unwrap();
    }
    assert_eq!(first.values(), &[1, 2]);
    assert_eq!(first.metadata(), Some(&"first"));
    assert!(cursor.has_more());

    let mut second = VecPageBatch::<u32, &str>::bounded(NonZeroUsize::new(2).unwrap());
    {
        let page = cursor.begin_page(TRANSACTION_A, &mut second).unwrap();
        assert_eq!(page.after(), Some(&2));
        page.finish("empty").unwrap();
    }
    assert!(second.is_empty());
    assert_eq!(second.metadata(), Some(&"empty"));
    assert!(cursor.is_exhausted());
}

#[test]
fn exhausted_cursor_clears_and_completes_each_new_batch() {
    let mut cursor: PageCursor<(), u32> = PageCursor::new(());
    let mut batch = VecPageBatch::<u32, &str>::unlimited();
    {
        let page = cursor.begin_page(TRANSACTION_A, &mut batch).unwrap();
        page.finish("initial").unwrap();
    }
    PageBatch::push(&mut batch, 99).unwrap();
    assert_eq!(batch.values(), &[99]);

    {
        let page = cursor.begin_page(TRANSACTION_A, &mut batch).unwrap();
        assert!(page.is_exhausted());
        page.finish("repeated").unwrap();
    }
    assert!(batch.is_empty());
    assert_eq!(batch.metadata(), Some(&"repeated"));
    assert!(cursor.is_exhausted());
}

#[test]
fn unlimited_batch_consumes_remaining_records() {
    let mut cursor = PageCursor::new(());
    let mut batch = VecPageBatch::<u32, ()>::unlimited();
    {
        let mut page = cursor.begin_page(TRANSACTION_A, &mut batch).unwrap();
        for value in 1..=4 {
            page.push(value, value).unwrap();
        }
        page.finish(()).unwrap();
    }
    assert_eq!(batch.values(), &[1, 2, 3, 4]);
    assert_eq!(batch.metadata(), Some(&()));
    assert!(cursor.is_exhausted());
}

#[test]
fn cursor_accepts_rotated_batches_and_changed_limits() {
    let mut cursor = PageCursor::new(());
    let mut first = VecPageBatch::<u32, ()>::bounded(NonZeroUsize::new(1).unwrap());
    {
        let mut page = cursor.begin_page(TRANSACTION_A, &mut first).unwrap();
        page.push(1, 1).unwrap();
        page.finish(()).unwrap();
    }

    let mut second = VecPageBatch::<u32, (), OwnedPageBatchInput<u32>, _>::bounded_with(
        NonZeroUsize::new(2).unwrap(),
        |value| Ok::<_, BoxError>(value * 10),
    );
    {
        let mut page = cursor.begin_page(TRANSACTION_A, &mut second).unwrap();
        page.push(2, 2).unwrap();
        page.finish(()).unwrap();
    }
    assert_eq!(second.values(), &[20]);
    assert!(cursor.is_exhausted());
}

#[test]
fn wrong_transaction_does_not_invalidate_original_cursor() {
    let mut cursor = PageCursor::new(());
    let mut batch = VecPageBatch::<u32, ()>::bounded(NonZeroUsize::new(1).unwrap());
    {
        let mut page = cursor.begin_page(TRANSACTION_A, &mut batch).unwrap();
        page.push(1, 1).unwrap();
        page.finish(()).unwrap();
    }

    assert!(matches!(
        cursor.begin_page(TRANSACTION_B, &mut batch),
        Err(PageError::TransactionMismatch { .. })
    ));
    assert!(cursor.has_more());
}

#[test]
fn dropped_attempt_clears_output_and_metadata_and_fails_cursor() {
    let mut cursor = PageCursor::new(());
    let mut batch = VecPageBatch::<u32, &str>::unlimited();
    PageBatch::set_metadata(&mut batch, "old");
    {
        let mut page = cursor.begin_page(TRANSACTION_A, &mut batch).unwrap();
        page.push(1, 1).unwrap();
    }
    assert!(batch.is_empty());
    assert_eq!(batch.metadata(), None);
    assert!(cursor.is_failed());
    assert!(matches!(
        cursor.begin_page(TRANSACTION_A, &mut batch),
        Err(PageError::CursorFailed)
    ));
}

#[test]
fn contract_and_batch_failures_invalidate_and_clear() {
    let mut cursor = PageCursor::new(());
    let mut batch = VecPageBatch::<u32, ()>::unlimited();
    {
        let mut page = cursor.begin_page(TRANSACTION_A, &mut batch).unwrap();
        page.push(2, 2).unwrap();
        assert!(matches!(
            page.push(2, 2),
            Err(PageError::NonIncreasingKey {
                transaction_id: TRANSACTION_A,
            })
        ));
    }
    assert!(batch.is_empty());
    assert!(cursor.is_failed());

    let mut cursor = PageCursor::new(());
    let mut batch =
        VecPageBatch::<u32, (), OwnedPageBatchInput<u32>, _>::unlimited_with(|_: u32| {
            Err::<u32, _>(std::io::Error::other("batch failed").into())
        });
    {
        let mut page = cursor.begin_page(TRANSACTION_A, &mut batch).unwrap();
        assert!(matches!(page.push(1, 1), Err(PageError::BatchPush { .. })));
    }
    assert!(batch.is_empty());
    assert!(cursor.is_failed());
}

#[test]
fn store_failure_preserves_classification_and_attempt_cleanup() {
    let classification = StoreErrorClassification::UNKNOWN
        .with_scope(StoreErrorScope::Store)
        .with_class(StoreErrorClass::Unavailable)
        .with_resolution(StoreErrorResolution::Retry);
    let source = StoreError::new(
        classification,
        std::io::Error::other("injected store failure"),
    );
    let mut cursor = PageCursor::new(());
    let mut batch = VecPageBatch::<u32, ()>::unlimited();
    let error = fail_store_page_after_push(&mut cursor, &mut batch, source)
        .expect_err("injected store failure should escape the page fill");

    assert!(matches!(&error, PageError::Store { .. }));
    assert_eq!(error.store_error_classification(), Some(classification));
    assert!(batch.is_empty());
    assert!(cursor.is_failed());
}

#[test]
fn batch_is_object_safe_for_any_borrowed_input_lifetime() {
    let mut cursor = PageCursor::new(());
    let mut batch =
        VecPageBatch::<usize, (), BorrowedViews, _>::unlimited_with(|view: BorrowedView<'_>| {
            Ok::<_, BoxError>(view.value.len())
        });
    fill_borrowed_view_page(&mut cursor, &mut batch).unwrap();
    assert_eq!(batch.values(), &[9]);
    assert!(cursor.is_exhausted());
}

#[test]
fn page_attempt_rejects_records_over_the_sampled_limit() {
    let mut cursor = PageCursor::new(());
    let mut batch = VecPageBatch::<u32, ()>::bounded(NonZeroUsize::new(1).unwrap());
    {
        let mut page = cursor.begin_page(TRANSACTION_A, &mut batch).unwrap();
        page.push(1, 1).unwrap();
        assert!(matches!(page.push(2, 2), Err(PageError::PageLimitExceeded)));
        assert!(page.batch.is_empty());
        assert!(matches!(page.finish(()), Err(PageError::CursorFailed)));
    }
}
