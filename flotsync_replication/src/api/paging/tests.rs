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
use std::panic::{AssertUnwindSafe, catch_unwind};

const TRANSACTION_A: StoreTransactionId = StoreTransactionId::from_uuid(Uuid::from_u128(20));
const TRANSACTION_B: StoreTransactionId = StoreTransactionId::from_uuid(Uuid::from_u128(21));

/// Opaque test continuation which deliberately implements neither `Clone` nor `Ord`.
struct TestContinuation(u32);

/// Incompatible continuation representation used to exercise format checks.
struct OtherContinuation;

/// Exercise an associated input family with a temporary view through a trait object.
fn fill_borrowed_view_page(
    cursor: &mut PageCursor<()>,
    batch: &mut dyn PageBatch<Input = BorrowedViews, Metadata = ()>,
) -> Result<(), PageError> {
    let mut page = cursor.begin_page::<TestContinuation, _>(TRANSACTION_A, batch)?;
    let temporary = String::from("temporary");
    page.push(BorrowedView { value: &temporary })?;
    page.finish((), PageEnd::Exhausted)
}

/// Simulate backend failure after a batch accepted partial output.
fn fail_store_page_after_push<Batch>(
    cursor: &mut PageCursor<()>,
    batch: &mut Batch,
    source: StoreError,
) -> Result<(), PageError>
where
    Batch: PageBatch<Input = OwnedPageBatchInput<u32>, Metadata = ()> + ?Sized,
{
    let mut page = cursor.begin_page::<TestContinuation, _>(TRANSACTION_A, batch)?;
    page.push(1)?;
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

/// Batch which filters odd source values to verify source and output cardinality differ.
struct FilteringBatch {
    /// Even values retained by the latest fill.
    values: Vec<u32>,
    /// Metadata retained after successful completion.
    metadata: Option<()>,
}

impl PageBatch for FilteringBatch {
    type Input = OwnedPageBatchInput<u32>;
    type Metadata = ();

    fn page_limit(&self) -> PageLimit {
        PageLimit::Max(NonZeroUsize::new(1).unwrap())
    }

    fn clear(&mut self) {
        self.values.clear();
        self.metadata = None;
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn metadata(&self) -> Option<&Self::Metadata> {
        self.metadata.as_ref()
    }

    fn push(&mut self, input: u32) -> Result<(), PageError> {
        if input.is_multiple_of(2) {
            self.values.push(input);
        }
        Ok(())
    }

    fn set_metadata(&mut self, metadata: Self::Metadata) {
        self.metadata = Some(metadata);
    }
}

/// Vec-backed batch whose metadata setter panics after records were accepted.
struct PanickingMetadataBatch {
    /// Delegated record storage and page policy.
    inner: VecPageBatch<u32, ()>,
}

impl PageBatch for PanickingMetadataBatch {
    type Input = OwnedPageBatchInput<u32>;
    type Metadata = ();

    fn page_limit(&self) -> PageLimit {
        self.inner.page_limit()
    }

    fn clear(&mut self) {
        self.inner.clear();
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn metadata(&self) -> Option<&Self::Metadata> {
        self.inner.metadata()
    }

    fn push(&mut self, input: u32) -> Result<(), PageError> {
        self.inner.push(input)
    }

    fn set_metadata(&mut self, _metadata: Self::Metadata) {
        panic!("injected metadata setter panic");
    }
}

#[test]
fn bounded_pages_advance_exclusively_and_require_a_final_empty_page() {
    let mut cursor = PageCursor::new("query");
    let mut first = VecPageBatch::<u32, &str>::bounded(NonZeroUsize::new(2).unwrap());
    {
        let mut page = cursor
            .begin_page::<TestContinuation, _>(TRANSACTION_A, &mut first)
            .unwrap();
        assert_eq!(page.params(), &"query");
        assert!(page.after().is_none());
        page.push(1).unwrap();
        page.push(2).unwrap();
        page.finish("first", PageEnd::MayHaveMore(TestContinuation(2)))
            .unwrap();
    }
    assert_eq!(first.values(), &[1, 2]);
    assert_eq!(first.metadata(), Some(&"first"));
    assert!(cursor.has_more());

    let mut second = VecPageBatch::<u32, &str>::bounded(NonZeroUsize::new(2).unwrap());
    {
        let page = cursor
            .begin_page::<TestContinuation, _>(TRANSACTION_A, &mut second)
            .unwrap();
        assert_eq!(page.after().map(|after| after.0), Some(2));
        page.finish("empty", PageEnd::Exhausted).unwrap();
    }
    assert!(second.is_empty());
    assert_eq!(second.metadata(), Some(&"empty"));
    assert!(cursor.is_exhausted());
}

#[test]
fn exhausted_cursor_reuse_clears_the_batch_and_returns_an_error() {
    let mut cursor: PageCursor<()> = PageCursor::new(());
    let mut batch = VecPageBatch::<u32, &str>::unlimited();
    {
        let page = cursor
            .begin_page::<TestContinuation, _>(TRANSACTION_A, &mut batch)
            .unwrap();
        page.finish("initial", PageEnd::Exhausted).unwrap();
    }
    PageBatch::push(&mut batch, 99).unwrap();
    assert_eq!(batch.values(), &[99]);

    assert!(matches!(
        cursor.begin_page::<TestContinuation, _>(TRANSACTION_A, &mut batch),
        Err(PageError::CursorExhausted)
    ));
    assert!(batch.is_empty());
    assert_eq!(batch.metadata(), None);
    assert!(cursor.is_exhausted());
}

#[test]
fn unlimited_batch_consumes_remaining_records() {
    let mut cursor = PageCursor::new(());
    let mut batch = VecPageBatch::<u32, ()>::unlimited();
    {
        let mut page = cursor
            .begin_page::<TestContinuation, _>(TRANSACTION_A, &mut batch)
            .unwrap();
        for value in 1..=4 {
            page.push(value).unwrap();
        }
        page.finish((), PageEnd::Exhausted).unwrap();
    }
    assert_eq!(batch.values(), &[1, 2, 3, 4]);
    assert_eq!(batch.metadata(), Some(&()));
    assert!(cursor.is_exhausted());
}

#[test]
fn unlimited_batch_resumes_from_opaque_continuation_and_accepts_backend_order() {
    let mut cursor = PageCursor::new(());
    let mut bounded = VecPageBatch::<u32, ()>::bounded(NonZeroUsize::new(1).unwrap());
    {
        let mut page = cursor
            .begin_page::<TestContinuation, _>(TRANSACTION_A, &mut bounded)
            .unwrap();
        page.push(2).unwrap();
        page.finish((), PageEnd::MayHaveMore(TestContinuation(2)))
            .unwrap();
    }

    let mut unlimited = VecPageBatch::<u32, ()>::unlimited();
    {
        let mut page = cursor
            .begin_page::<TestContinuation, _>(TRANSACTION_A, &mut unlimited)
            .unwrap();
        assert_eq!(page.after().map(|after| after.0), Some(2));
        page.push(4).unwrap();
        page.push(3).unwrap();
        page.finish((), PageEnd::Exhausted).unwrap();
    }
    assert_eq!(unlimited.values(), &[4, 3]);
    assert!(cursor.is_exhausted());
}

#[test]
fn cursor_accepts_rotated_batches_and_changed_limits() {
    let mut cursor = PageCursor::new(());
    let mut first = VecPageBatch::<u32, ()>::bounded(NonZeroUsize::new(1).unwrap());
    {
        let mut page = cursor
            .begin_page::<TestContinuation, _>(TRANSACTION_A, &mut first)
            .unwrap();
        page.push(1).unwrap();
        page.finish((), PageEnd::MayHaveMore(TestContinuation(1)))
            .unwrap();
    }

    let mut second = VecPageBatch::<u32, (), OwnedPageBatchInput<u32>, _>::bounded_with(
        NonZeroUsize::new(2).unwrap(),
        |value| Ok::<_, BoxError>(value * 10),
    );
    {
        let mut page = cursor
            .begin_page::<TestContinuation, _>(TRANSACTION_A, &mut second)
            .unwrap();
        page.push(2).unwrap();
        page.finish((), PageEnd::Exhausted).unwrap();
    }
    assert_eq!(second.values(), &[20]);
    assert!(cursor.is_exhausted());
}

#[test]
fn source_limit_does_not_depend_on_output_cardinality() {
    let mut cursor = PageCursor::new(());
    let mut batch = FilteringBatch {
        values: Vec::new(),
        metadata: None,
    };
    {
        let mut page = cursor
            .begin_page::<TestContinuation, _>(TRANSACTION_A, &mut batch)
            .unwrap();
        page.push(1).unwrap();
        assert!(page.has_filled_limit());
        page.finish((), PageEnd::MayHaveMore(TestContinuation(1)))
            .unwrap();
    }
    assert!(batch.values.is_empty());
    assert!(cursor.has_more());

    let mut page = cursor
        .begin_page::<TestContinuation, _>(TRANSACTION_A, &mut batch)
        .unwrap();
    page.push(2).unwrap();
    page.finish((), PageEnd::Exhausted).unwrap();
    assert_eq!(batch.values, [2]);
}

#[test]
fn wrong_transaction_does_not_invalidate_original_cursor() {
    let mut cursor = PageCursor::new(());
    let mut batch = VecPageBatch::<u32, ()>::bounded(NonZeroUsize::new(1).unwrap());
    {
        let mut page = cursor
            .begin_page::<TestContinuation, _>(TRANSACTION_A, &mut batch)
            .unwrap();
        page.push(1).unwrap();
        page.finish((), PageEnd::MayHaveMore(TestContinuation(1)))
            .unwrap();
    }

    assert!(matches!(
        cursor.begin_page::<TestContinuation, _>(TRANSACTION_B, &mut batch),
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
        let mut page = cursor
            .begin_page::<TestContinuation, _>(TRANSACTION_A, &mut batch)
            .unwrap();
        page.push(1).unwrap();
    }
    assert!(batch.is_empty());
    assert_eq!(batch.metadata(), None);
    assert!(cursor.is_failed());
    assert!(matches!(
        cursor.begin_page::<TestContinuation, _>(TRANSACTION_A, &mut batch),
        Err(PageError::CursorFailed)
    ));
}

#[test]
fn batch_failures_invalidate_and_clear() {
    let mut cursor = PageCursor::new(());
    let mut batch =
        VecPageBatch::<u32, (), OwnedPageBatchInput<u32>, _>::unlimited_with(|_: u32| {
            Err::<u32, _>(std::io::Error::other("batch failed").into())
        });
    {
        let mut page = cursor
            .begin_page::<TestContinuation, _>(TRANSACTION_A, &mut batch)
            .unwrap();
        assert!(matches!(page.push(1), Err(PageError::BatchPush { .. })));
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
fn page_errors_convert_to_store_errors_without_hiding_backend_failures() {
    let classification = StoreErrorClassification::UNKNOWN
        .with_scope(StoreErrorScope::Store)
        .with_class(StoreErrorClass::Unavailable)
        .with_resolution(StoreErrorResolution::Retry);
    let backend = StoreError::new(
        classification,
        std::io::Error::other("preserved backend failure"),
    );
    let converted: StoreError = PageError::from_store_error(backend).into();
    assert_eq!(converted.classification(), classification);
    assert!(converted.to_string().contains("preserved backend failure"));

    let contract: StoreError = PageError::PageLimitExceeded.into();
    assert_eq!(contract.classification().class, StoreErrorClass::Contract);
    assert_eq!(contract.classification().scope, StoreErrorScope::Operation);
    assert_eq!(
        contract.classification().resolution,
        StoreErrorResolution::FixBug
    );
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
fn page_limit_failure_rejects_every_completion_value() {
    let endings = [
        PageEnd::Exhausted,
        PageEnd::MayHaveMore(TestContinuation(1)),
    ];
    for end in endings {
        let mut cursor = PageCursor::new(());
        let mut batch = VecPageBatch::<u32, ()>::bounded(NonZeroUsize::new(1).unwrap());
        let mut page = cursor
            .begin_page::<TestContinuation, _>(TRANSACTION_A, &mut batch)
            .unwrap();
        page.push(1).unwrap();
        assert!(matches!(page.push(2), Err(PageError::PageLimitExceeded)));
        assert!(page.batch.is_empty());
        assert!(matches!(page.finish((), end), Err(PageError::CursorFailed)));
        assert!(batch.is_empty());
        assert!(cursor.is_failed());
    }
}

#[test]
fn metadata_setter_panic_clears_output_and_fails_cursor() {
    let mut cursor = PageCursor::new(());
    let mut batch = PanickingMetadataBatch {
        inner: VecPageBatch::bounded(NonZeroUsize::new(1).unwrap()),
    };
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let mut page = cursor
            .begin_page::<TestContinuation, _>(TRANSACTION_A, &mut batch)
            .unwrap();
        page.push(1).unwrap();
        let _result = page.finish((), PageEnd::MayHaveMore(TestContinuation(1)));
    }));

    assert!(outcome.is_err());
    assert!(batch.is_empty());
    assert!(cursor.is_failed());
}

#[test]
fn continuation_type_mismatch_preserves_the_original_cursor() {
    let mut cursor = PageCursor::new(());
    let mut batch = VecPageBatch::<u32, ()>::bounded(NonZeroUsize::new(1).unwrap());
    {
        let mut page = cursor
            .begin_page::<TestContinuation, _>(TRANSACTION_A, &mut batch)
            .unwrap();
        page.push(1).unwrap();
        page.finish((), PageEnd::MayHaveMore(TestContinuation(7)))
            .unwrap();
    }

    assert!(matches!(
        cursor.begin_page::<OtherContinuation, _>(TRANSACTION_A, &mut batch),
        Err(PageError::ContinuationTypeMismatch)
    ));
    let page = cursor
        .begin_page::<TestContinuation, _>(TRANSACTION_A, &mut batch)
        .unwrap();
    assert_eq!(page.after().map(|after| after.0), Some(7));
    page.finish((), PageEnd::Exhausted).unwrap();
}

#[test]
fn unlimited_page_cannot_retain_a_continuation() {
    let mut cursor = PageCursor::new(());
    let mut batch = VecPageBatch::<u32, ()>::unlimited();
    let page = cursor
        .begin_page::<TestContinuation, _>(TRANSACTION_A, &mut batch)
        .unwrap();
    assert!(matches!(
        page.finish((), PageEnd::MayHaveMore(TestContinuation(1))),
        Err(PageError::UnlimitedPageContinuation)
    ));
    assert!(batch.is_empty());
    assert!(cursor.is_failed());
}

#[test]
fn empty_bounded_page_cannot_retain_a_continuation() {
    let mut cursor = PageCursor::new(());
    let mut batch = VecPageBatch::<u32, ()>::bounded(NonZeroUsize::new(1).unwrap());
    let page = cursor
        .begin_page::<TestContinuation, _>(TRANSACTION_A, &mut batch)
        .unwrap();
    assert!(matches!(
        page.finish((), PageEnd::MayHaveMore(TestContinuation(1))),
        Err(PageError::EmptyPageContinuation)
    ));
    assert!(batch.is_empty());
    assert!(cursor.is_failed());
}
