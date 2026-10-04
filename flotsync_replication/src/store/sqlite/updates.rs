//! SQLite persistence for replication updates.

use super::*;
use futures_util::TryStreamExt as _;
use std::collections::HashSet;

pub(super) async fn load_replication_update(
    connection: &mut SqliteStoreConnection,
    group_id: &GroupId,
    update_id: UpdateId,
) -> Result<Option<ReplicationUpdateRecord>, StoreError> {
    let member_count = load_group_member_count(connection, group_id).await?;
    let row = sqlx::query(
        "
SELECT update_node_index, update_version, sender, applied_locally, update_message
FROM dataset_updates
WHERE group_id = ?1
  AND update_node_index = ?2
  AND update_version = ?3
",
    )
    .bind(group_id.to_string())
    .bind(i64::from(update_id.node_index))
    .bind(encode_update_version_sort_key_vec(update_id.version))
    .fetch_optional(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let update = with_stored_update_view(group_id, member_count, update_id, &row, |view| {
        view.try_to_owned_record()
            .map_err(|source| PageError::from_store_error(invalid_stored_object("update", source)))
    })
    .map_err(StoreError::from)?;
    Ok(Some(update))
}

pub(super) async fn load_replication_updates_into<Batch>(
    connection: &mut SqliteStoreConnection,
    mut page: PageAttempt<'_, ReplicationUpdatesQuery<'_>, SqliteUpdatePageContinuation, Batch>,
) -> Result<(), PageError>
where
    Batch: PageBatch<Input = ReplicationUpdatePageInput, Metadata = ()> + ?Sized,
{
    let selected_ids = page.params().update_ids();
    if selected_ids.is_some_and(HashSet::is_empty) {
        return finish_page(page, None);
    }
    let group_id = page.params().group_id();
    let member_count = load_group_member_count(connection, &group_id).await?;
    let mut query_builder = build_replication_update_page_query(
        &page,
        "update_node_index, update_version, sender, applied_locally, update_message",
    );
    let query = query_builder.build();
    let mut rows = query.fetch(&mut *connection);
    let mut continuation = None;
    while let Some(row) = rows.try_next().await.context(SqlxSnafu)? {
        let update_id = decode_stored_update_id(&row)?;
        if selected_ids.is_some_and(|ids| !ids.contains(&update_id)) {
            continue;
        }
        with_stored_update_view(&group_id, member_count, update_id, &row, |view| {
            page.push(view)
        })?;
        continuation = Some(SqliteUpdatePageContinuation::new(update_id));
        if page.has_filled_limit() {
            break;
        }
    }
    finish_page(page, continuation)
}

pub(super) async fn load_replication_update_ids_into<Batch>(
    connection: &mut SqliteStoreConnection,
    mut page: PageAttempt<'_, ReplicationUpdatesQuery<'_>, SqliteUpdatePageContinuation, Batch>,
) -> Result<(), PageError>
where
    Batch: PageBatch<Input = OwnedPageBatchInput<UpdateId>, Metadata = ()> + ?Sized,
{
    let selected_ids = page.params().update_ids();
    if selected_ids.is_some_and(HashSet::is_empty) {
        return finish_page(page, None);
    }
    let mut query_builder =
        build_replication_update_page_query(&page, "update_node_index, update_version");
    let query = query_builder.build();
    let mut rows = query.fetch(&mut *connection);
    let mut continuation = None;
    while let Some(row) = rows.try_next().await.context(SqlxSnafu)? {
        let update_id = decode_stored_update_id(&row)?;
        if selected_ids.is_some_and(|ids| !ids.contains(&update_id)) {
            continue;
        }
        page.push(update_id)?;
        continuation = Some(SqliteUpdatePageContinuation::new(update_id));
        if page.has_filled_limit() {
            break;
        }
    }
    finish_page(page, continuation)
}

pub(super) fn push_replication_update_filter(
    query_builder: &mut QueryBuilder<Sqlite>,
    filter: ReplicationUpdateFilter,
) {
    match filter {
        ReplicationUpdateFilter::All => {
            // Selecting every update requires no additional predicate.
        }
        ReplicationUpdateFilter::PendingApply => {
            query_builder.push(" AND applied_locally = ");
            query_builder.push_bind(false);
        }
        ReplicationUpdateFilter::Applied => {
            query_builder.push(" AND applied_locally = ");
            query_builder.push_bind(true);
        }
        ReplicationUpdateFilter::ProducerRange {
            producer_index,
            start_version,
            end_version,
        } => {
            query_builder.push(" AND update_node_index = ");
            query_builder.push_bind(i64::from(producer_index.as_u32()));
            query_builder.push(" AND update_version >= ");
            query_builder.push_bind(encode_update_version_sort_key_vec(start_version));
            query_builder.push(" AND update_version <= ");
            query_builder.push_bind(encode_update_version_sort_key_vec(end_version));
        }
    }
}

/// SQLite-owned continuation for the update log's composite SQL ordering.
#[derive(Clone, Copy)]
pub(crate) struct SqliteUpdatePageContinuation {
    /// Last accepted update identity ordered by version and then producer index.
    update_id: UpdateId,
}

impl SqliteUpdatePageContinuation {
    /// Capture the final accepted update identity of one full bounded page.
    const fn new(update_id: UpdateId) -> Self {
        Self { update_id }
    }
}

/// Build the common update-log query for projected records or ids.
///
/// Bounded pages need the backend's stable composite order. An unlimited page
/// needs neither an ordering step nor an output limit.
fn build_replication_update_page_query<Batch>(
    page: &PageAttempt<'_, ReplicationUpdatesQuery<'_>, SqliteUpdatePageContinuation, Batch>,
    selected_columns: &'static str,
) -> QueryBuilder<Sqlite>
where
    Batch: PageBatch + ?Sized,
{
    let mut query_builder = QueryBuilder::<Sqlite>::new("SELECT ");
    query_builder
        .push(selected_columns)
        .push(" FROM dataset_updates WHERE group_id = ")
        .push_bind(page.params().group_id().to_string());
    push_replication_update_filter(&mut query_builder, page.params().filter());
    if let Some(after) = page.after() {
        let encoded_version = encode_update_version_sort_key_vec(after.update_id.version);
        query_builder
            .push(" AND (update_version > ")
            .push_bind(encoded_version.clone())
            .push(" OR (update_version = ")
            .push_bind(encoded_version)
            .push(" AND update_node_index > ")
            .push_bind(i64::from(after.update_id.node_index))
            .push("))");
    }
    if let PageLimit::Max(limit) = page.limit() {
        query_builder.push(" ORDER BY update_version, update_node_index");
        // SQL LIMIT would count excluded IDs and could end a selected page early.
        if page.params().update_ids().is_none() {
            query_builder
                .push(" LIMIT ")
                .push_bind(sqlite_limit_value(limit));
        }
    }
    query_builder
}

/// Decode the composite update identity selected from one stored row.
fn decode_stored_update_id(row: &sqlx::sqlite::SqliteRow) -> Result<UpdateId, StoreError> {
    let raw_version = row.get::<&[u8], _>("update_version");
    let version = decode_update_version_sort_key(raw_version)?;
    let raw_node_index = row.get::<i64, _>("update_node_index");
    let node_index = decode_member_index_value(raw_node_index)?;
    Ok(UpdateId {
        version,
        node_index,
    })
}

pub(super) async fn append_replication_update(
    connection: &mut SqliteStoreConnection,
    update: &ReplicationUpdateRecord,
) -> Result<(), StoreError> {
    let update_message = UpdateMessageProtoSource::from(update).encode_proto_to_vec();
    sqlx::query(
        "
INSERT INTO dataset_updates (
    group_id,
    update_node_index,
    update_version,
    sender,
    applied_locally,
    update_message
)
VALUES (?1, ?2, ?3, ?4, ?5, ?6)
",
    )
    .bind(update.group_id.to_string())
    .bind(i64::from(update.update_id.node_index))
    .bind(encode_update_version_sort_key_vec(update.update_id.version))
    .bind(update.sender.to_string())
    .bind(update.applied_locally)
    .bind(update_message)
    .execute(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    Ok(())
}

pub(super) async fn mark_replication_update_applied(
    connection: &mut SqliteStoreConnection,
    group_id: &GroupId,
    update_id: UpdateId,
) -> Result<(), StoreError> {
    let rows_affected = sqlx::query(
        "
UPDATE dataset_updates
SET applied_locally = 1
WHERE group_id = ?1
  AND update_node_index = ?2
  AND update_version = ?3
",
    )
    .bind(group_id.to_string())
    .bind(i64::from(update_id.node_index))
    .bind(encode_update_version_sort_key_vec(update_id.version))
    .execute(&mut *connection)
    .await
    .context(SqlxSnafu)?
    .rows_affected();
    ensure!(
        rows_affected == 1,
        MissingStoredUpdateSnafu {
            group_id: *group_id,
            update_id,
        }
    );
    Ok(())
}
