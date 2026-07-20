//! SQLite persistence for dataset row snapshots and patches.

use super::*;

pub(super) async fn load_dataset_rows(
    connection: &mut SqliteStoreConnection,
    schema_sources: &HashMap<DatasetId, SchemaSource>,
    group_id: &GroupId,
    dataset_id: &DatasetId,
    row_keys: &mut RowKeyIterator<'_>,
) -> Result<DatasetRowStateSlice, StoreError> {
    let dataset_exists = dataset_exists_in_group(connection, group_id, dataset_id).await?;
    let mut row_keys = row_keys.peekable();
    if row_keys.peek().is_none() {
        return Ok(DatasetRowStateSlice {
            group_id: *group_id,
            dataset_id: dataset_id.clone(),
            dataset_exists,
            rows: HashMap::new(),
        });
    }
    let mut rows = row_keys
        .map(|row_key| (*row_key, None))
        .collect::<HashMap<_, _>>();
    if !dataset_exists {
        return Ok(DatasetRowStateSlice {
            group_id: *group_id,
            dataset_id: dataset_id.clone(),
            dataset_exists,
            rows,
        });
    }

    let schema = schema_sources
        .get(dataset_id)
        .cloned()
        .context(MissingSchemaSnafu {
            group_id: *group_id,
            dataset_id: dataset_id.clone(),
        })?;
    let member_count = load_group_member_count(connection, group_id).await?;
    let mut query_builder = QueryBuilder::<Sqlite>::new(
        "
SELECT row_key, row_snapshot, row_tombstoned, row_last_changed_versions
FROM dataset_rows
WHERE group_id = ",
    );
    query_builder.push_bind(group_id.to_string());
    query_builder.push(" AND dataset_id = ");
    query_builder.push_bind(dataset_id.as_str());
    query_builder.push(" AND row_key IN (");
    {
        let mut separated = query_builder.separated(", ");
        for row_key in rows.keys() {
            separated.push_bind(row_key.to_string());
        }
    }
    query_builder.push(")");
    let stored_rows = query_builder
        .build()
        .fetch_all(&mut *connection)
        .await
        .context(SqlxSnafu)?;
    for row in stored_rows {
        let row_key = decode_row_key(&row.get::<String, _>("row_key"))?;
        let row_snapshot = decode_dataset_row_snapshot(
            schema.as_schema(),
            &row.get::<Vec<u8>, _>("row_snapshot"),
        )?;
        rows.insert(
            row_key,
            Some(ReplicationRowStateRecord {
                row_id: row_key,
                snapshot: row_snapshot,
                tombstoned: row.get::<bool, _>("row_tombstoned"),
                last_changed_versions: decode_dataset_row_last_changed_versions(
                    &row,
                    member_count,
                )?,
            }),
        );
    }
    Ok(DatasetRowStateSlice {
        group_id: *group_id,
        dataset_id: dataset_id.clone(),
        dataset_exists,
        rows,
    })
}

/// Scan rows in lexicographic row-key order.
///
/// `after` is an exclusive lower bound. When the result contains exactly
/// `limit` rows, `next_after` is set to the last returned row key so callers can
/// continue with `row_key > next_after`.
pub(super) async fn scan_dataset_row_batch(
    connection: &mut SqliteStoreConnection,
    schema_sources: &HashMap<DatasetId, SchemaSource>,
    group_id: &GroupId,
    dataset_id: &DatasetId,
    after: Option<RowKey>,
    limit: NonZeroUsize,
) -> Result<DatasetRowStateBatch, StoreError> {
    let dataset_exists = dataset_exists_in_group(connection, group_id, dataset_id).await?;
    if !dataset_exists {
        return Ok(DatasetRowStateBatch {
            group_id: *group_id,
            dataset_id: dataset_id.clone(),
            dataset_exists,
            rows: Vec::new(),
            next_after: None,
        });
    }

    let schema = schema_sources
        .get(dataset_id)
        .cloned()
        .context(MissingSchemaSnafu {
            group_id: *group_id,
            dataset_id: dataset_id.clone(),
        })?;
    let member_count = load_group_member_count(connection, group_id).await?;
    let mut query_builder = QueryBuilder::<Sqlite>::new(
        "
SELECT row_key, row_snapshot, row_tombstoned, row_last_changed_versions
FROM dataset_rows
WHERE group_id = ",
    );
    query_builder.push_bind(group_id.to_string());
    query_builder.push(" AND dataset_id = ");
    query_builder.push_bind(dataset_id.as_str());
    if let Some(after) = after {
        query_builder.push(" AND row_key > ");
        query_builder.push_bind(after.to_string());
    }
    query_builder.push(" ORDER BY row_key LIMIT ");
    query_builder.push_bind(sqlite_limit_value(limit));

    let stored_rows = query_builder
        .build()
        .fetch_all(&mut *connection)
        .await
        .context(SqlxSnafu)?;
    let mut rows = Vec::with_capacity(stored_rows.len());
    for row in stored_rows {
        let row_key = decode_row_key(&row.get::<String, _>("row_key"))?;
        let row_snapshot = decode_dataset_row_snapshot(
            schema.as_schema(),
            &row.get::<Vec<u8>, _>("row_snapshot"),
        )?;
        rows.push(ReplicationRowStateRecord {
            row_id: row_key,
            snapshot: row_snapshot,
            tombstoned: row.get::<bool, _>("row_tombstoned"),
            last_changed_versions: decode_dataset_row_last_changed_versions(&row, member_count)?,
        });
    }
    let next_after = if rows.len() == limit.get() {
        rows.last().map(|row| row.row_id)
    } else {
        None
    };
    Ok(DatasetRowStateBatch {
        group_id: *group_id,
        dataset_id: dataset_id.clone(),
        dataset_exists,
        rows,
        next_after,
    })
}

pub(super) async fn apply_dataset_row_patch(
    connection: &mut SqliteStoreConnection,
    schema_sources: &HashMap<DatasetId, SchemaSource>,
    patch: &DatasetRowStatePatch,
) -> Result<(), StoreError> {
    if patch.actions.is_empty() {
        return Ok(());
    }

    let schema = schema_sources
        .get(&patch.dataset_id)
        .cloned()
        .context(MissingSchemaSnafu {
            group_id: patch.group_id,
            dataset_id: patch.dataset_id.clone(),
        })?;
    ensure_dataset_exists(connection, &patch.group_id, &patch.dataset_id).await?;

    for action in &patch.actions {
        let (row_key, snapshot, tombstoned) = match action {
            DatasetRowStateWrite::UpsertActive { row_key, snapshot } => {
                ensure_dataset_row_upsert_active_is_valid(
                    connection,
                    &patch.group_id,
                    &patch.dataset_id,
                    row_key,
                )
                .await?;
                (row_key, snapshot, false)
            }
            DatasetRowStateWrite::UpsertTombstone { row_key, snapshot } => {
                (row_key, snapshot, true)
            }
        };
        let row_snapshot = encode_dataset_row_snapshot(schema.as_schema(), snapshot)?;
        sqlx::query(
            "
INSERT INTO dataset_rows (
    group_id,
    dataset_id,
    row_key,
    row_snapshot,
    row_tombstoned,
    row_last_changed_versions
)
VALUES (?1, ?2, ?3, ?4, ?5, ?6)
ON CONFLICT(group_id, dataset_id, row_key) DO UPDATE
SET row_snapshot = excluded.row_snapshot,
    row_tombstoned = excluded.row_tombstoned,
    row_last_changed_versions = excluded.row_last_changed_versions
",
        )
        .bind(patch.group_id.to_string())
        .bind(patch.dataset_id.as_str())
        .bind(row_key.to_string())
        .bind(row_snapshot)
        .bind(tombstoned)
        .bind(encode_stored_version_vector(&patch.last_changed_versions))
        .execute(&mut *connection)
        .await
        .context(SqlxSnafu)?;
    }
    Ok(())
}

pub(super) async fn ensure_dataset_row_upsert_active_is_valid(
    connection: &mut SqliteStoreConnection,
    group_id: &GroupId,
    dataset_id: &DatasetId,
    row_key: &RowKey,
) -> Result<(), StoreError> {
    let existing_tombstoned =
        load_dataset_row_tombstoned(connection, group_id, dataset_id, row_key).await?;
    ensure!(
        existing_tombstoned != Some(true),
        InvalidDatasetRowStateTransitionSnafu {
            group_id: *group_id,
            dataset_id: dataset_id.clone(),
            row_key: *row_key,
            from: "tombstone",
            to: "active",
        }
    );
    Ok(())
}

pub(super) async fn load_dataset_row_tombstoned(
    connection: &mut SqliteStoreConnection,
    group_id: &GroupId,
    dataset_id: &DatasetId,
    row_key: &RowKey,
) -> Result<Option<bool>, StoreError> {
    let row = sqlx::query(
        "
SELECT row_tombstoned
FROM dataset_rows
WHERE group_id = ?1 AND dataset_id = ?2 AND row_key = ?3
",
    )
    .bind(group_id.to_string())
    .bind(dataset_id.as_str())
    .bind(row_key.to_string())
    .fetch_optional(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    Ok(row.map(|row| row.get::<bool, _>("row_tombstoned")))
}
