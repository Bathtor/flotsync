//! SQLite query construction shared by pageable store operations.

use super::{PageAttempt, PageBatch, PageLimit, QueryBuilder, Sqlite, sqlite_limit_value};

/// Append a text-compatible lower bound and the bounded ordering window.
///
/// `column` is a trusted SQLite column expression supplied by this backend. An
/// unlimited page adds only an existing lower bound and does not request an
/// ordering or limit.
pub(super) fn push_text_page_window<Params, Key, Batch>(
    query_builder: &mut QueryBuilder<Sqlite>,
    page: &PageAttempt<'_, Params, Key, Batch>,
    column: &'static str,
) where
    Key: Clone + Ord + ToString,
    Batch: PageBatch + ?Sized,
{
    push_text_window(query_builder, page.after(), page.limit(), column);
}

/// Append a text-compatible lower bound and bounded ordering from copied page values.
pub(super) fn push_text_window<Key>(
    query_builder: &mut QueryBuilder<Sqlite>,
    after: Option<&Key>,
    limit: PageLimit,
    column: &'static str,
) where
    Key: ToString,
{
    if let Some(after) = after {
        query_builder
            .push(" AND ")
            .push(column)
            .push(" > ")
            .push_bind(after.to_string());
    }
    push_page_order_and_limit(query_builder, limit, column);
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
