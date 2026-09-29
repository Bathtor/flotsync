//! SQLite persistence for dataset row snapshots and patches.

use super::*;

/// Load one page of a fixed requested-row selection.
pub(super) async fn load_dataset_rows_into(
    connection: &mut SqliteStoreConnection,
    mut page: PageAttempt<
        '_,
        RequestedDatasetRowsQuery<'_>,
        SqliteRequestedRowsContinuation,
        RequestedDatasetRowPageBatch,
    >,
) -> Result<(), PageError> {
    let start = page
        .after()
        .map_or(0, |continuation| continuation.next_index);
    let maximum = page
        .limit()
        .into_option()
        .map_or(usize::MAX, NonZeroUsize::get);
    let row_key_count = page.params().row_keys().len();
    let end = start.saturating_add(maximum).min(row_key_count);
    let selected_row_keys = page.params().row_keys()[start..end].to_vec();
    let dataset = page.params().dataset().context();
    let group_id = *dataset.group_id;
    let dataset_id = dataset.dataset_id.clone();
    let dataset_exists = dataset_exists_in_group(connection, &group_id, &dataset_id).await?;
    let metadata = DatasetRowPageMetadata {
        group_id,
        dataset_id: dataset_id.clone(),
        dataset_exists,
    };
    if selected_row_keys.is_empty() {
        return page.finish(metadata, PageEnd::Exhausted);
    }

    let mut stored_rows = if dataset_exists {
        load_requested_dataset_rows(connection, &group_id, &dataset_id, &selected_row_keys).await?
    } else {
        HashMap::new()
    };
    let member_count = if stored_rows.is_empty() {
        None
    } else {
        let member_count = load_group_member_count(connection, &group_id).await?;
        Some(member_count)
    };
    page.reserve(selected_row_keys.len());
    for row_key in selected_row_keys {
        if let Some(row) = stored_rows.remove(&row_key) {
            let member_count = member_count.expect("stored rows require group member metadata");
            let mut source = decode_dataset_row_source(&row, member_count)?;
            page.push(RequestedDatasetRowInput::Present(&mut source))?;
        } else {
            page.push(RequestedDatasetRowInput::Missing(row_key))?;
        }
    }
    let page_end = if end < row_key_count {
        PageEnd::MayHaveMore(SqliteRequestedRowsContinuation { next_index: end })
    } else {
        PageEnd::Exhausted
    };
    page.finish(metadata, page_end)
}

/// Scan one page of rows in SQLite `TEXT` row-key order.
pub(super) async fn scan_dataset_rows_into(
    connection: &mut SqliteStoreConnection,
    mut page: PageAttempt<
        '_,
        DatasetRowsQuery<'_>,
        SqliteTextPageContinuation,
        DatasetRowPageBatch,
    >,
) -> Result<(), PageError> {
    let dataset = page.params().context();
    let group_id = *dataset.group_id;
    let dataset_id = dataset.dataset_id.clone();
    let dataset_exists = dataset_exists_in_group(connection, &group_id, &dataset_id).await?;
    let metadata = DatasetRowPageMetadata {
        group_id,
        dataset_id: dataset_id.clone(),
        dataset_exists,
    };
    if !dataset_exists {
        return page.finish(metadata, PageEnd::Exhausted);
    }

    let member_count = load_group_member_count(connection, &group_id).await?;
    let mut query_builder = dataset_rows_query(&group_id, &dataset_id);
    push_ordered_text_page_window(&mut query_builder, &page, "row_key");
    let stored_rows = query_builder
        .build()
        .fetch_all(&mut *connection)
        .await
        .context(SqlxSnafu)?;
    let continuation_index = continuation_record_index(page.limit(), stored_rows.len());
    let continuation = continuation_index.map(|index| {
        SqliteTextPageContinuation::new(stored_rows[index].get::<String, _>("row_key"))
    });
    page.reserve(stored_rows.len());
    for row in stored_rows {
        let mut source = decode_dataset_row_source(&row, member_count)?;
        page.push(&mut source)?;
    }
    finish_page_with_metadata(page, metadata, continuation)
}

/// Scan one row-key-aligned transition page from two stored dataset occurrences.
pub(super) async fn scan_dataset_row_transitions_into(
    connection: &mut SqliteStoreConnection,
    mut page: PageAttempt<
        '_,
        DatasetRowTransitionQuery<'_>,
        SqliteTextPageContinuation,
        DatasetRowTransitionPageBatch,
    >,
) -> Result<(), PageError> {
    let (previous_group_id, current_group_id, dataset_id, metadata) = {
        let previous_group = page.params().previous().context();
        let current_group = page.params().current().context();
        ensure_matching_transition_dataset_references(previous_group, current_group)
            .map_err(StoreError::from_classification_source)?;
        let metadata =
            load_transition_dataset_metadata(connection, previous_group, current_group).await?;
        (
            *previous_group.group_id,
            *current_group.group_id,
            previous_group.dataset_id.clone(),
            metadata,
        )
    };
    let after = page.after().map(SqliteTextPageContinuation::as_str);
    let limit = page.limit();
    let mut query_builder = QueryBuilder::<Sqlite>::new("WITH page_keys AS (");
    push_dataset_row_key_select(&mut query_builder, &previous_group_id, &dataset_id, after);
    query_builder.push(" UNION ");
    push_dataset_row_key_select(&mut query_builder, &current_group_id, &dataset_id, after);
    query_builder.push(" ORDER BY row_key");
    if let PageLimit::Max(limit) = limit {
        query_builder.push(" LIMIT ");
        query_builder.push_bind(sqlite_limit_value(limit));
    }
    query_builder.push(") SELECT page_keys.row_key");
    push_joined_row_projection(&mut query_builder, "previous_rows");
    push_joined_row_projection(&mut query_builder, "current_rows");
    query_builder.push(
        " FROM page_keys LEFT JOIN dataset_rows AS previous_rows ON previous_rows.group_id = ",
    );
    query_builder.push_bind(previous_group_id.to_string());
    query_builder.push(" AND previous_rows.dataset_id = ");
    query_builder.push_bind(dataset_id.as_str());
    query_builder.push(
        " AND previous_rows.row_key = page_keys.row_key LEFT JOIN dataset_rows AS current_rows ON current_rows.group_id = ",
    );
    query_builder.push_bind(current_group_id.to_string());
    query_builder.push(" AND current_rows.dataset_id = ");
    query_builder.push_bind(dataset_id.as_str());
    query_builder.push(" AND current_rows.row_key = page_keys.row_key ORDER BY page_keys.row_key");
    // TODO(flotsync-duu): Evaluate streaming together with reusable store output buffers.
    let stored_rows = query_builder
        .build()
        .fetch_all(&mut *connection)
        .await
        .context(SqlxSnafu)?;
    let continuation_index = continuation_record_index(limit, stored_rows.len());
    let continuation = continuation_index.map(|index| {
        SqliteTextPageContinuation::new(
            stored_rows[index].get::<String, _>(JoinedRowColumnLayout::ROW_KEY_COLUMN),
        )
    });
    page.reserve(stored_rows.len());
    for row in stored_rows {
        let row_key = decode_row_key(&row.get::<String, _>(JoinedRowColumnLayout::ROW_KEY_COLUMN))?;
        let mut previous = decode_transition_row_source(
            &row,
            row_key,
            metadata.previous_member_count,
            JoinedRowColumnLayout::PREVIOUS,
        )?;
        let mut current = decode_transition_row_source(
            &row,
            row_key,
            metadata.current_member_count,
            JoinedRowColumnLayout::CURRENT,
        )?;
        let input = ReplicationStateRowTransitionInput::new(
            row_key,
            previous
                .as_mut()
                .map(|source| source as &mut dyn ReplicationStateRowSource),
            current
                .as_mut()
                .map(|source| source as &mut dyn ReplicationStateRowSource),
        );
        page.push(input)?;
    }
    let page_metadata = DatasetRowTransitionPageMetadata {
        previous_group_id,
        current_group_id,
        dataset_id,
        previous_dataset_exists: metadata.previous_dataset_exists,
        current_dataset_exists: metadata.current_dataset_exists,
    };
    finish_page_with_metadata(page, page_metadata, continuation)
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

/// SQLite continuation identifying the next index in a fixed requested-key selection.
pub(super) struct SqliteRequestedRowsContinuation {
    /// Index of the first requested key not emitted by the previous page.
    next_index: usize,
}

/// Build the canonical stored-row selection for one dataset occurrence.
fn dataset_rows_query(group_id: &GroupId, dataset_id: &DatasetId) -> QueryBuilder<Sqlite> {
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
    query_builder
}

/// Load present rows for one requested-key slice, indexed by decoded row key.
async fn load_requested_dataset_rows(
    connection: &mut SqliteStoreConnection,
    group_id: &GroupId,
    dataset_id: &DatasetId,
    row_keys: &[RowKey],
) -> Result<HashMap<RowKey, sqlx::sqlite::SqliteRow>, StoreError> {
    let mut query_builder = dataset_rows_query(group_id, dataset_id);
    query_builder.push(" AND row_key IN (");
    {
        let mut separated = query_builder.separated(", ");
        for row_key in row_keys {
            separated.push_bind(row_key.to_string());
        }
    }
    query_builder.push(")");
    let rows = query_builder
        .build()
        .fetch_all(&mut *connection)
        .await
        .context(SqlxSnafu)?;
    let mut rows_by_key = HashMap::with_capacity(rows.len());
    for row in rows {
        let row_key = decode_row_key(&row.get::<String, _>("row_key"))?;
        rows_by_key.insert(row_key, row);
    }
    Ok(rows_by_key)
}

/// Decode one ordinary stored row into a temporary direct-append source.
fn decode_dataset_row_source(
    row: &sqlx::sqlite::SqliteRow,
    member_count: NonZeroUsize,
) -> Result<SqliteReplicationStateRowSource, StoreError> {
    let row_key = decode_row_key(&row.get::<String, _>("row_key"))?;
    let decoder = decode_dataset_row_snapshot_decoder(&row.get::<Vec<u8>, _>("row_snapshot"))?;
    let created_by = decode_dataset_row_created_by(
        row,
        row_key,
        "row_created_by_node_index",
        "row_created_by_version",
        member_count,
    )?;
    let last_changed_versions = decode_dataset_row_last_changed_versions(row, member_count)?;
    Ok(SqliteReplicationStateRowSource {
        metadata: Some(ReplicationRowMetadata {
            row_key,
            tombstoned: row.get::<bool, _>("row_tombstoned"),
            created_by,
            last_changed_versions,
        }),
        decoder,
    })
}

/// Append one indexed dataset-row key selection to the transition CTE.
fn push_dataset_row_key_select(
    query_builder: &mut QueryBuilder<Sqlite>,
    group_id: &GroupId,
    dataset_id: &DatasetId,
    after: Option<&str>,
) {
    query_builder.push("SELECT row_key FROM dataset_rows WHERE group_id = ");
    query_builder.push_bind(group_id.to_string());
    query_builder.push(" AND dataset_id = ");
    query_builder.push_bind(dataset_id.as_str());
    if let Some(after) = after {
        query_builder.push(" AND row_key > ");
        query_builder.push_bind(after);
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

/// Decode one nullable side of a dataset-row transition query result.
///
/// `None` means the corresponding side of the left join was `NULL`. A stored
/// row always has a non-null snapshot, so the snapshot column identifies
/// whether that side exists.
fn decode_transition_row_source(
    row: &sqlx::sqlite::SqliteRow,
    row_key: RowKey,
    member_count: NonZeroUsize,
    columns: JoinedRowColumnLayout,
) -> Result<Option<SqliteReplicationStateRowSource>, StoreError> {
    let snapshot = row
        .try_get::<Option<Vec<u8>>, _>(columns.snapshot())
        .context(SqlxSnafu)?;
    let Some(snapshot) = snapshot else {
        return Ok(None);
    };
    let decoder = decode_dataset_row_snapshot_decoder(&snapshot)?;
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
    Ok(Some(SqliteReplicationStateRowSource {
        metadata: Some(ReplicationRowMetadata {
            row_key,
            tombstoned,
            created_by,
            last_changed_versions,
        }),
        decoder,
    }))
}

/// SQLite decoder and metadata for one temporary direct positional append.
struct SqliteReplicationStateRowSource {
    /// Metadata consumed together with the row snapshot.
    metadata: Option<ReplicationRowMetadata>,
    /// Protobuf-backed field-state decoder.
    decoder: ProtoSchemaSnapshotDecoder,
}

impl ReplicationStateRowSource for SqliteReplicationStateRowSource {
    fn row_key(&self) -> RowKey {
        self.metadata
            .as_ref()
            .expect("SQLite row source cannot be inspected after it was consumed")
            .row_key
    }

    fn append_to(&mut self, output: &mut ReplicationStateRowBatch) -> Result<(), PageError> {
        let metadata = self
            .metadata
            .take()
            .expect("SQLite row source cannot be appended twice");
        output
            .push_decoded_row(metadata, &mut self.decoder)
            .map_err(|source| invalid_stored_object("dataset row snapshot", source))
            .map_err(PageError::from_store_error)
    }
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
