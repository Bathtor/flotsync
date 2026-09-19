//! SQLite query construction shared by pageable store operations.

use super::{
    PageAttempt,
    PageBatch,
    PageEnd,
    PageError,
    PageLimit,
    QueryBuilder,
    Sqlite,
    sqlite_limit_value,
};

/// SQLite-owned continuation containing one SQL `TEXT` value.
pub(super) struct SqliteTextPageContinuation(String);

impl SqliteTextPageContinuation {
    /// Capture the SQLite `TEXT` value of the final accepted source record.
    pub(super) fn new(value: String) -> Self {
        Self(value)
    }

    /// Borrow the value used by SQLite's exclusive lower-bound comparison.
    pub(super) fn as_str(&self) -> &str {
        &self.0
    }
}

/// Append a `TEXT` lower bound and the bounded ordering window.
///
/// `column` is a trusted SQLite column expression supplied by this backend. An
/// unlimited page adds only an existing lower bound and does not request an
/// ordering or limit.
pub(super) fn push_text_page_window<Params, Batch>(
    query_builder: &mut QueryBuilder<Sqlite>,
    page: &PageAttempt<'_, Params, SqliteTextPageContinuation, Batch>,
    column: &'static str,
) where
    Batch: PageBatch + ?Sized,
{
    push_text_window(
        query_builder,
        page.after().map(SqliteTextPageContinuation::as_str),
        page.limit(),
        column,
    );
}

/// Append a `TEXT` lower bound and bounded ordering from borrowed page values.
pub(super) fn push_text_window(
    query_builder: &mut QueryBuilder<Sqlite>,
    after: Option<&str>,
    limit: PageLimit,
    column: &'static str,
) {
    if let Some(after) = after {
        query_builder
            .push(" AND ")
            .push(column)
            .push(" > ")
            .push_bind(after);
    }
    push_page_order_and_limit(query_builder, limit, column);
}

/// Finish a SQLite page, retaining `continuation` only for a full bounded fill.
pub(super) fn finish_page<Params, Continuation, Batch>(
    page: PageAttempt<'_, Params, Continuation, Batch>,
    continuation: Option<Continuation>,
) -> Result<(), PageError>
where
    Continuation: Send + 'static,
    Batch: PageBatch<Metadata = ()> + ?Sized,
{
    let end = match (page.has_filled_limit(), continuation) {
        (true, Some(continuation)) => Ok(PageEnd::MayHaveMore(continuation)),
        (true, None) => Err(PageError::MissingContinuation),
        (false, _) => Ok(PageEnd::Exhausted),
    }?;
    page.finish((), end)
}

/// Return the final record index when this result fills one bounded page.
///
/// `Some` identifies the record whose backend position must become the next
/// continuation. `None` means the page is unlimited or shorter than its finite
/// limit and therefore needs no continuation.
pub(super) fn continuation_record_index(limit: PageLimit, records: usize) -> Option<usize> {
    match limit {
        PageLimit::Max(limit) if records == limit.get() => records.checked_sub(1),
        PageLimit::Max(_) | PageLimit::Unlimited => None,
    }
}

/// Append deterministic ordering and a limit for a bounded page.
///
/// `order_by` is a trusted SQLite ordering expression supplied by this backend.
/// Unlimited pages are left unchanged.
pub(super) fn push_page_order_and_limit(
    query_builder: &mut QueryBuilder<Sqlite>,
    limit: PageLimit,
    order_by: &'static str,
) {
    if let PageLimit::Max(limit) = limit {
        query_builder
            .push(" ORDER BY ")
            .push(order_by)
            .push(" LIMIT ")
            .push_bind(sqlite_limit_value(limit));
    }
}
