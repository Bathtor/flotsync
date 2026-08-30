//! SQLite persistence for dataset row snapshots and patches.

use super::*;
use flotsync_utils::option_when;

pub(super) async fn load_dataset_rows(
    connection: &mut SqliteStoreConnection,
    dataset: GroupDatasetSchemaRef<'_>,
    row_keys: &mut RowKeyIterator<'_>,
) -> Result<DatasetRowStateSlice, StoreError> {
    let group_id = dataset.group_id;
    let dataset_id = dataset.dataset_id;
    let dataset_exists = dataset_exists_in_group(connection, group_id, dataset_id).await?;
    let mut row_keys = row_keys.peekable();
    let mut state_rows = ReplicationStateRowBatch::new(dataset.schema);
    if row_keys.peek().is_none() {
        return Ok(DatasetRowStateSlice {
            group_id: *group_id,
            dataset_id: dataset_id.clone(),
            dataset_exists,
            state_rows,
            missing_row_keys: HashSet::new(),
        });
    }
    let mut missing_row_keys = row_keys.copied().collect::<HashSet<_>>();
    if !dataset_exists {
        return Ok(DatasetRowStateSlice {
            group_id: *group_id,
            dataset_id: dataset_id.clone(),
            dataset_exists,
            state_rows,
            missing_row_keys,
        });
    }

    let member_count = load_group_member_count(connection, group_id).await?;
    let mut query_builder = QueryBuilder::<Sqlite>::new(
        "
SELECT row_key,
       row_snapshot,
       row_tombstoned,
       row_created_by_node_index,
       row_created_by_version,
       row_last_changed_versions
FROM dataset_rows
WHERE group_id = ",
    );
    query_builder.push_bind(group_id.to_string());
    query_builder.push(" AND dataset_id = ");
    query_builder.push_bind(dataset_id.as_str());
    query_builder.push(" AND row_key IN (");
    {
        let mut separated = query_builder.separated(", ");
        for row_key in &missing_row_keys {
            separated.push_bind(row_key.to_string());
        }
    }
    query_builder.push(")");
    let stored_rows = query_builder
        .build()
        .fetch_all(&mut *connection)
        .await
        .context(SqlxSnafu)?;
    state_rows.reserve_rows(stored_rows.len());
    for row in stored_rows {
        let row_key = decode_row_key(&row.get::<String, _>("row_key"))?;
        let mut row_decoder =
            decode_dataset_row_snapshot_decoder(&row.get::<Vec<u8>, _>("row_snapshot"))?;
        let created_by = decode_dataset_row_created_by(
            &row,
            row_key,
            "row_created_by_node_index",
            "row_created_by_version",
            member_count,
        )?;
        let last_changed_versions = decode_dataset_row_last_changed_versions(&row, member_count)?;
        let metadata = ReplicationRowMetadata {
            row_key,
            tombstoned: row.get::<bool, _>("row_tombstoned"),
            created_by,
            last_changed_versions,
        };
        state_rows
            .push_decoded_row(metadata, &mut row_decoder)
            .map_err(|source| invalid_stored_object("dataset row snapshot", source))?;
        missing_row_keys.remove(&row_key);
    }
    Ok(DatasetRowStateSlice {
        group_id: *group_id,
        dataset_id: dataset_id.clone(),
        dataset_exists,
        state_rows,
        missing_row_keys,
    })
}

/// Scan rows in lexicographic row-key order.
///
/// `after` is an exclusive lower bound. When the result contains exactly
/// `limit` rows, `next_after` is set to the last returned row key so callers can
/// continue with `row_key > next_after`.
pub(super) async fn scan_dataset_row_batch(
    connection: &mut SqliteStoreConnection,
    dataset: GroupDatasetSchemaRef<'_>,
    after: Option<RowKey>,
    limit: NonZeroUsize,
    output: &mut ReplicationStateRowBatch,
) -> Result<DatasetRowScanPage, StoreError> {
    let group_id = dataset.group_id;
    let dataset_id = dataset.dataset_id;
    output.reuse_for_schema(dataset.schema);
    let dataset_exists = dataset_exists_in_group(connection, group_id, dataset_id).await?;
    if !dataset_exists {
        return Ok(DatasetRowScanPage {
            group_id: *group_id,
            dataset_id: dataset_id.clone(),
            dataset_exists,
            next_after: None,
        });
    }

    let member_count = load_group_member_count(connection, group_id).await?;
    let mut query_builder = QueryBuilder::<Sqlite>::new(
        "
SELECT row_key,
       row_snapshot,
       row_tombstoned,
       row_created_by_node_index,
       row_created_by_version,
       row_last_changed_versions
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
    output.reserve_rows(stored_rows.len());
    for row in stored_rows {
        let row_key = decode_row_key(&row.get::<String, _>("row_key"))?;
        let mut row_decoder =
            decode_dataset_row_snapshot_decoder(&row.get::<Vec<u8>, _>("row_snapshot"))?;
        let created_by = decode_dataset_row_created_by(
            &row,
            row_key,
            "row_created_by_node_index",
            "row_created_by_version",
            member_count,
        )?;
        let last_changed_versions = decode_dataset_row_last_changed_versions(&row, member_count)?;
        let metadata = ReplicationRowMetadata {
            row_key,
            tombstoned: row.get::<bool, _>("row_tombstoned"),
            created_by,
            last_changed_versions,
        };
        output
            .push_decoded_row(metadata, &mut row_decoder)
            .map_err(|source| invalid_stored_object("dataset row snapshot", source))?;
    }
    let next_after = if output.len() == limit.get() {
        output.rows().next_back().map(|row| row.metadata().row_key)
    } else {
        None
    };
    Ok(DatasetRowScanPage {
        group_id: *group_id,
        dataset_id: dataset_id.clone(),
        dataset_exists,
        next_after,
    })
}

/// Scan a row-key-aligned transition page from two stored dataset occurrences.
///
/// The key-union page is selected before either row table is joined, so the
/// limit applies to emitted transitions rather than to either side independently.
pub(super) async fn scan_dataset_row_transition_batch(
    connection: &mut SqliteStoreConnection,
    previous_group: GroupDatasetSchemaRef<'_>,
    current_group: GroupDatasetSchemaRef<'_>,
    after: Option<RowKey>,
    limit: NonZeroUsize,
    output: &mut ReplicationStateRowTransitionBatch,
) -> Result<DatasetRowStateTransitionPage, StoreError> {
    ensure_matching_transition_dataset_references(previous_group, current_group)
        .map_err(StoreError::from_classification_source)?;
    output.reuse_for_schemas(previous_group.schema, current_group.schema);
    let dataset_id = previous_group.dataset_id;
    let metadata =
        load_transition_dataset_metadata(connection, previous_group, current_group).await?;
    let mut query_builder = QueryBuilder::<Sqlite>::new("WITH page_keys AS (");
    push_dataset_row_key_select(
        &mut query_builder,
        previous_group.group_id,
        dataset_id,
        after,
    );
    query_builder.push(" UNION ");
    push_dataset_row_key_select(
        &mut query_builder,
        current_group.group_id,
        dataset_id,
        after,
    );
    query_builder.push(" ORDER BY row_key LIMIT ");
    query_builder.push_bind(sqlite_limit_value(limit));
    query_builder.push(") SELECT page_keys.row_key");
    push_joined_row_projection(&mut query_builder, "previous_rows");
    push_joined_row_projection(&mut query_builder, "current_rows");
    query_builder.push(
        " FROM page_keys LEFT JOIN dataset_rows AS previous_rows ON previous_rows.group_id = ",
    );
    query_builder.push_bind(previous_group.group_id.to_string());
    query_builder.push(" AND previous_rows.dataset_id = ");
    query_builder.push_bind(dataset_id.as_str());
    query_builder.push(
        " AND previous_rows.row_key = page_keys.row_key LEFT JOIN dataset_rows AS current_rows ON current_rows.group_id = ",
    );
    query_builder.push_bind(current_group.group_id.to_string());
    query_builder.push(" AND current_rows.dataset_id = ");
    query_builder.push_bind(dataset_id.as_str());
    query_builder.push(" AND current_rows.row_key = page_keys.row_key ORDER BY page_keys.row_key");
    // TODO(flotsync-duu): Evaluate streaming together with reusable store output buffers.
    let stored_rows = query_builder
        .build()
        .fetch_all(&mut *connection)
        .await
        .context(SqlxSnafu)?;

    decode_dataset_row_transitions(
        stored_rows,
        metadata.previous_member_count,
        metadata.current_member_count,
        output,
    )?;

    let last_row_key = output
        .rows()
        .next_back()
        .map(|transition| transition.row_key());
    let next_after = option_when!(output.len() == limit.get(), last_row_key).flatten();
    Ok(DatasetRowStateTransitionPage {
        previous_group_id: *previous_group.group_id,
        current_group_id: *current_group.group_id,
        dataset_id: dataset_id.clone(),
        previous_dataset_exists: metadata.previous_dataset_exists,
        current_dataset_exists: metadata.current_dataset_exists,
        next_after,
    })
}

pub(super) async fn apply_dataset_row_patch(
    connection: &mut SqliteStoreConnection,
    dataset: GroupDatasetSchemaRef<'_>,
    patch: &DatasetRowStatePatch,
) -> Result<(), StoreError> {
    let context_matches_patch =
        dataset.group_id == &patch.group_id && dataset.dataset_id == &patch.dataset_id;
    ensure!(
        context_matches_patch,
        InvalidDatasetRowPatchContextSnafu {
            context_group: *dataset.group_id,
            context_dataset: dataset.dataset_id.clone(),
            patch_group: patch.group_id,
            patch_dataset: patch.dataset_id.clone(),
        }
    );
    if patch.actions.is_empty() {
        return Ok(());
    }

    ensure_dataset_exists(connection, &patch.group_id, &patch.dataset_id).await?;

    for action in &patch.actions {
        let (row_key, snapshot, tombstoned, created_by) = match action {
            DatasetRowStateWrite::UpsertActive { row_key, snapshot } => {
                ensure_dataset_row_upsert_active_is_valid(
                    connection,
                    &patch.group_id,
                    &patch.dataset_id,
                    row_key,
                )
                .await?;
                (row_key, snapshot, false, Some(patch.change_id))
            }
            DatasetRowStateWrite::UpsertTombstone { row_key, snapshot } => {
                (row_key, snapshot, true, None)
            }
        };
        let row_snapshot = encode_dataset_row_snapshot(dataset.schema, snapshot)?;
        sqlx::query(
            "
INSERT INTO dataset_rows (
    group_id,
    dataset_id,
    row_key,
    row_snapshot,
    row_tombstoned,
    row_created_by_node_index,
    row_created_by_version,
    row_last_changed_versions
)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
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
        .bind(created_by.map(|change_id| i64::from(change_id.node_index)))
        .bind(created_by.map(|change_id| i64::from(U64BitsInI64::from(change_id.version))))
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

/// Append one indexed dataset-row key selection to the transition CTE.
fn push_dataset_row_key_select(
    query_builder: &mut QueryBuilder<Sqlite>,
    group_id: &GroupId,
    dataset_id: &DatasetId,
    after: Option<RowKey>,
) {
    query_builder.push("SELECT row_key FROM dataset_rows WHERE group_id = ");
    query_builder.push_bind(group_id.to_string());
    query_builder.push(" AND dataset_id = ");
    query_builder.push_bind(dataset_id.as_str());
    if let Some(after) = after {
        query_builder.push(" AND row_key > ");
        query_builder.push_bind(after.to_string());
    }
}

/// Append one qualified stored-row projection in canonical decode order.
fn push_joined_row_projection(query_builder: &mut QueryBuilder<Sqlite>, table_alias: &'static str) {
    for column in JoinedRowColumnLayout::PROJECTION_COLUMNS {
        query_builder.push(format_args!(", {table_alias}.{column}"));
    }
}

/// Load dataset-presence flags and member widths through two focused queries.
async fn load_transition_dataset_metadata(
    connection: &mut SqliteStoreConnection,
    previous_group: GroupDatasetSchemaRef<'_>,
    current_group: GroupDatasetSchemaRef<'_>,
) -> Result<TransitionDatasetMetadata, StoreError> {
    let (previous_dataset_exists, current_dataset_exists) =
        load_transition_dataset_presence(connection, previous_group, current_group).await?;
    let (previous_member_count, current_member_count) =
        load_transition_group_member_counts(connection, previous_group, current_group).await?;
    Ok(TransitionDatasetMetadata {
        previous_dataset_exists,
        previous_member_count,
        current_dataset_exists,
        current_member_count,
    })
}

/// Load whether the shared dataset exists in each replication group.
async fn load_transition_dataset_presence(
    connection: &mut SqliteStoreConnection,
    previous_group: GroupDatasetSchemaRef<'_>,
    current_group: GroupDatasetSchemaRef<'_>,
) -> Result<(bool, bool), StoreError> {
    let row = sqlx::query(
        "
SELECT EXISTS(
           SELECT 1 FROM datasets WHERE group_id = ?1 AND dataset_id = ?3
       ) AS previous_dataset_exists,
       EXISTS(
           SELECT 1 FROM datasets WHERE group_id = ?2 AND dataset_id = ?3
       ) AS current_dataset_exists
",
    )
    .bind(previous_group.group_id.to_string())
    .bind(current_group.group_id.to_string())
    .bind(previous_group.dataset_id.as_str())
    .fetch_one(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    let previous_dataset_exists = row
        .try_get::<bool, _>("previous_dataset_exists")
        .context(SqlxSnafu)?;
    let current_dataset_exists = row
        .try_get::<bool, _>("current_dataset_exists")
        .context(SqlxSnafu)?;
    Ok((previous_dataset_exists, current_dataset_exists))
}

/// Load both group widths required to decode their causal row state.
async fn load_transition_group_member_counts(
    connection: &mut SqliteStoreConnection,
    previous_group: GroupDatasetSchemaRef<'_>,
    current_group: GroupDatasetSchemaRef<'_>,
) -> Result<(NonZeroUsize, NonZeroUsize), StoreError> {
    let stored_rows = sqlx::query(
        "
SELECT active.group_id, material.member_count
FROM replication_groups AS active
JOIN replication_group_material AS material ON material.group_id = active.group_id
WHERE active.group_id IN (?1, ?2)
",
    )
    .bind(previous_group.group_id.to_string())
    .bind(current_group.group_id.to_string())
    .fetch_all(&mut *connection)
    .await
    .context(SqlxSnafu)?;

    let previous_group_id = previous_group.group_id.to_string();
    let current_group_id = current_group.group_id.to_string();
    let mut previous_member_count = None;
    let mut current_member_count = None;
    for row in stored_rows {
        let group_id = row.try_get::<String, _>("group_id").context(SqlxSnafu)?;
        let member_count = row.try_get::<i64, _>("member_count").context(SqlxSnafu)?;
        let member_count = decode_non_zero_member_count(member_count)?;
        if group_id == previous_group_id {
            previous_member_count = Some(member_count);
        }
        if group_id == current_group_id {
            current_member_count = Some(member_count);
        }
    }

    let Some(previous_member_count) = previous_member_count else {
        return MissingStoredGroupSnafu {
            group_id: *previous_group.group_id,
        }
        .fail()
        .map_err(StoreError::from);
    };
    let Some(current_member_count) = current_member_count else {
        return MissingStoredGroupSnafu {
            group_id: *current_group.group_id,
        }
        .fail()
        .map_err(StoreError::from);
    };
    Ok((previous_member_count, current_member_count))
}

/// Decode all stored rows returned by one dataset-transition query.
fn decode_dataset_row_transitions(
    stored_rows: Vec<sqlx::sqlite::SqliteRow>,
    previous_member_count: NonZeroUsize,
    current_member_count: NonZeroUsize,
    output: &mut ReplicationStateRowTransitionBatch,
) -> Result<(), StoreError> {
    output.reserve_rows(stored_rows.len());
    for stored_row in stored_rows {
        let row_key =
            decode_row_key(&stored_row.get::<String, _>(JoinedRowColumnLayout::ROW_KEY_COLUMN))?;
        let previous_index = decode_transition_row_into_batch(
            &stored_row,
            row_key,
            previous_member_count,
            JoinedRowColumnLayout::PREVIOUS,
            output.previous_rows_mut(),
        )?;
        let current_index = decode_transition_row_into_batch(
            &stored_row,
            row_key,
            current_member_count,
            JoinedRowColumnLayout::CURRENT,
            output.current_rows_mut(),
        )?;
        output.push_alignment(previous_index, current_index);
    }
    Ok(())
}

/// Decode one nullable side of a dataset-row transition query result.
///
/// `None` means the corresponding side of the left join was `NULL`. A stored
/// row always has a non-null snapshot, so the snapshot column identifies
/// whether that side exists.
fn decode_transition_row_into_batch(
    row: &sqlx::sqlite::SqliteRow,
    row_key: RowKey,
    member_count: NonZeroUsize,
    columns: JoinedRowColumnLayout,
    output: &mut ReplicationStateRowBatch,
) -> Result<Option<usize>, StoreError> {
    let snapshot = row
        .try_get::<Option<Vec<u8>>, _>(columns.snapshot())
        .context(SqlxSnafu)?;
    let Some(snapshot) = snapshot else {
        return Ok(None);
    };
    let mut row_decoder = decode_dataset_row_snapshot_decoder(&snapshot)?;
    let tombstoned = row
        .try_get::<bool, _>(columns.tombstoned())
        .context(SqlxSnafu)?;
    let created_by_node_index = row
        .try_get::<Option<i64>, _>(columns.created_by_node_index())
        .context(SqlxSnafu)?;
    let created_by_version = row
        .try_get::<Option<i64>, _>(columns.created_by_version())
        .context(SqlxSnafu)?;
    let created_by = decode_dataset_row_created_by_values(
        row_key,
        created_by_node_index,
        created_by_version,
        member_count,
    )?;
    let last_changed_versions = row
        .try_get::<Vec<u8>, _>(columns.last_changed_versions())
        .context(SqlxSnafu)?;
    let last_changed_versions = decode_stored_version_vector(&last_changed_versions, member_count)?;
    let metadata = ReplicationRowMetadata {
        row_key,
        tombstoned,
        created_by,
        last_changed_versions,
    };
    let row_index = output.len();
    output
        .push_decoded_row(metadata, &mut row_decoder)
        .map_err(|source| invalid_stored_object("dataset row snapshot", source))?;
    Ok(Some(row_index))
}

/// Ordinal positions for one repeated joined-row projection.
#[derive(Clone, Copy)]
struct JoinedRowColumnLayout {
    /// First ordinal occupied by this joined row.
    first: usize,
}

impl JoinedRowColumnLayout {
    /// Stored row columns repeated for both sides of the transition projection.
    const PROJECTION_COLUMNS: [&str; 5] = [
        "row_snapshot",
        "row_tombstoned",
        "row_created_by_node_index",
        "row_created_by_version",
        "row_last_changed_versions",
    ];
    /// Ordinal of the shared row key in one transition query result.
    const ROW_KEY_COLUMN: usize = 0;
    /// Ordinals occupied by the previous joined row.
    const PREVIOUS: Self = Self::new(1);
    /// Ordinals occupied by the current joined row.
    const CURRENT: Self = Self::new(1 + Self::PROJECTION_COLUMNS.len());

    /// Build the column layout for one repeated projection group.
    const fn new(first: usize) -> Self {
        Self { first }
    }

    /// Nullable snapshot whose presence identifies an existing joined row.
    const fn snapshot(self) -> usize {
        self.first
    }

    /// Stored tombstone flag.
    const fn tombstoned(self) -> usize {
        self.first + 1
    }

    /// Nullable creator member index.
    const fn created_by_node_index(self) -> usize {
        self.first + 2
    }

    /// Nullable bit-reinterpreted creator version.
    const fn created_by_version(self) -> usize {
        self.first + 3
    }

    /// Stored causal version of the row image.
    const fn last_changed_versions(self) -> usize {
        self.first + 4
    }
}

/// Metadata required to decode one page of a dataset transition scan.
struct TransitionDatasetMetadata {
    /// Whether the previous group contains the requested dataset.
    previous_dataset_exists: bool,
    /// Member width required to decode previous-group causal state.
    previous_member_count: NonZeroUsize,
    /// Whether the current group contains the requested dataset.
    current_dataset_exists: bool,
    /// Member width required to decode current-group causal state.
    current_member_count: NonZeroUsize,
}
