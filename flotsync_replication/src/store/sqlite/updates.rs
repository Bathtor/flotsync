//! SQLite persistence for replication updates.

use super::*;

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
    let update = decode_stored_update_row(group_id, member_count, update_id, &row)?;
    Ok(Some(update))
}

pub(super) async fn load_replication_updates(
    connection: &mut SqliteStoreConnection,
    group_id: &GroupId,
    filter: ReplicationUpdateFilter,
    limit: Option<NonZeroUsize>,
) -> Result<Vec<ReplicationUpdateRecord>, StoreError> {
    let member_count = load_group_member_count(connection, group_id).await?;
    let mut query_builder = QueryBuilder::<Sqlite>::new(
        "
SELECT update_node_index, update_version, sender, applied_locally, update_message
FROM dataset_updates
WHERE group_id = ",
    );
    query_builder.push_bind(group_id.to_string());
    push_replication_update_filter(&mut query_builder, filter);

    query_builder.push(" ORDER BY update_version, update_node_index");
    if let Some(limit) = limit {
        query_builder.push(" LIMIT ");
        query_builder.push_bind(sqlite_limit_value(limit));
    }
    let rows = query_builder
        .build()
        .fetch_all(&mut *connection)
        .await
        .context(SqlxSnafu)?;

    let mut updates = Vec::with_capacity(rows.len());
    for row in rows {
        let update_id = UpdateId {
            node_index: decode_member_index_value(row.get::<i64, _>("update_node_index"))?,
            version: decode_update_version_sort_key(&row.get::<Vec<u8>, _>("update_version"))?,
        };
        let update = decode_stored_update_row(group_id, member_count, update_id, &row)?;
        updates.push(update);
    }
    Ok(updates)
}

pub(super) async fn load_replication_update_ids(
    connection: &mut SqliteStoreConnection,
    group_id: &GroupId,
    filter: ReplicationUpdateFilter,
    limit: Option<NonZeroUsize>,
) -> Result<Vec<UpdateId>, StoreError> {
    let mut query_builder = QueryBuilder::<Sqlite>::new(
        "
SELECT update_node_index, update_version
FROM dataset_updates
WHERE group_id = ",
    );
    query_builder.push_bind(group_id.to_string());
    push_replication_update_filter(&mut query_builder, filter);

    query_builder.push(" ORDER BY update_version, update_node_index");
    if let Some(limit) = limit {
        query_builder.push(" LIMIT ");
        query_builder.push_bind(sqlite_limit_value(limit));
    }
    let rows = query_builder
        .build()
        .fetch_all(&mut *connection)
        .await
        .context(SqlxSnafu)?;

    let mut update_ids = Vec::with_capacity(rows.len());
    for row in rows {
        update_ids.push(UpdateId {
            node_index: decode_member_index_value(row.get::<i64, _>("update_node_index"))?,
            version: decode_update_version_sort_key(&row.get::<Vec<u8>, _>("update_version"))?,
        });
    }
    Ok(update_ids)
}

pub(super) fn push_replication_update_filter(
    query_builder: &mut QueryBuilder<Sqlite>,
    filter: ReplicationUpdateFilter,
) {
    match filter {
        ReplicationUpdateFilter::All => {}
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
