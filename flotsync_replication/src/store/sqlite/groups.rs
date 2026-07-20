//! SQLite persistence for replication-group material and lifecycle.

use super::*;

pub(super) async fn load_replication_group_material(
    connection: &mut SqliteStoreConnection,
    group_id: &GroupId,
) -> Result<Option<ReplicationGroupMaterialRecord>, StoreError> {
    let row = sqlx::query(
        "
SELECT
    member_count,
    local_member_index,
    group_secret_crypto_version,
    group_secret_key_id,
    group_secret_nonce,
    group_secret_ciphertext
FROM replication_group_material
WHERE group_id = ?1
",
    )
    .bind(group_id.to_string())
    .fetch_optional(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    let Some(row) = row else {
        return Ok(None);
    };

    let member_count = decode_non_zero_member_count(row.get::<i64, _>("member_count"))?;
    let local_member_index =
        decode_member_index(row.get::<i64, _>("local_member_index"), member_count)?;
    let encrypted_group_secret = decode_encrypted_store_secret(
        row.get("group_secret_crypto_version"),
        row.get("group_secret_key_id"),
        row.get("group_secret_nonce"),
        row.get("group_secret_ciphertext"),
    )?;
    let security_material = EncryptedGroupSecurityMaterial {
        encrypted_group_secret,
    };
    let member_keys = load_group_member_keys(connection, group_id, member_count).await?;
    let group_schema = load_group_schema(connection, group_id).await?;

    Ok(Some(ReplicationGroupMaterialRecord {
        group_id: *group_id,
        member_keys,
        local_member_index,
        group_schema,
        security_material,
    }))
}

pub(super) async fn load_replication_group(
    connection: &mut SqliteStoreConnection,
    group_id: &GroupId,
) -> Result<Option<ReplicationGroupRecord>, StoreError> {
    let row = sqlx::query(
        "
SELECT version_vector, lifecycle, successor_group_id, final_versions
FROM replication_groups
WHERE group_id = ?1
",
    )
    .bind(group_id.to_string())
    .fetch_optional(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let material = load_replication_group_material(connection, group_id)
        .await?
        .expect("active group foreign key guarantees matching group material");
    let version_vector = decode_stored_version_vector(
        &row.get::<Vec<u8>, _>("version_vector"),
        material.member_count(),
    )?;
    let lifecycle = decode_replication_group_lifecycle(&row, material.member_count())?;
    let mut group = material.activate(version_vector);
    group.lifecycle = lifecycle;
    Ok(Some(group))
}

pub(super) async fn load_replication_groups(
    connection: &mut SqliteStoreConnection,
) -> Result<Vec<ReplicationGroupRecord>, StoreError> {
    let group_ids = sqlx::query_scalar::<_, String>(
        "
SELECT group_id
FROM replication_groups
ORDER BY group_id
",
    )
    .fetch_all(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    let mut groups = Vec::with_capacity(group_ids.len());
    for group_id in group_ids {
        let group_id = decode_group_id(&group_id)?;
        if let Some(group) = load_replication_group(connection, &group_id).await? {
            groups.push(group);
        }
    }
    Ok(groups)
}

pub(super) async fn load_replication_groups_for_ids(
    connection: &mut SqliteStoreConnection,
    requested_group_ids: &HashSet<GroupId>,
) -> Result<Vec<ReplicationGroupRecord>, StoreError> {
    if requested_group_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut query_builder = QueryBuilder::<Sqlite>::new(
        "
SELECT group_id
FROM replication_groups
WHERE group_id IN (",
    );
    {
        let mut separated = query_builder.separated(", ");
        for group_id in requested_group_ids {
            separated.push_bind(group_id.to_string());
        }
    }
    query_builder.push(") ORDER BY group_id");

    let group_ids = query_builder
        .build_query_scalar::<String>()
        .fetch_all(&mut *connection)
        .await
        .context(SqlxSnafu)?;
    let mut groups = Vec::with_capacity(group_ids.len());
    for group_id in group_ids {
        let group_id = decode_group_id(&group_id)?;
        if let Some(group) = load_replication_group(connection, &group_id).await? {
            groups.push(group);
        }
    }
    Ok(groups)
}

pub(super) async fn load_group_schema(
    connection: &mut SqliteStoreConnection,
    group_id: &GroupId,
) -> Result<GroupSchema, StoreError> {
    let rows = sqlx::query(
        "
SELECT dataset_id, payload
FROM group_dataset_schemas
WHERE group_id = ?1
ORDER BY dataset_id
",
    )
    .bind(group_id.to_string())
    .fetch_all(&mut *connection)
    .await
    .context(SqlxSnafu)?;

    let mut datasets = HashMap::with_capacity(rows.len());
    for row in rows {
        let dataset_id = decode_dataset_id(&row.get::<String, _>("dataset_id"))?;
        let payload = row.get::<Vec<u8>, _>("payload");
        let dataset_schema = decode_group_dataset_schema_payload(&payload)?;
        ensure!(
            dataset_schema.dataset_id == dataset_id,
            StoredDatasetSchemaKeyMismatchSnafu {
                group: *group_id,
                key_dataset: dataset_id,
                payload_dataset: dataset_schema.dataset_id.clone(),
            }
        );
        datasets.insert(dataset_id, dataset_schema.schema);
    }
    Ok(GroupSchema::new(datasets))
}

pub(super) async fn load_group_dataset_schema(
    connection: &mut SqliteStoreConnection,
    group_id: &GroupId,
    dataset_id: &DatasetId,
) -> Result<Option<SchemaSource>, StoreError> {
    let row = sqlx::query(
        "
SELECT payload
FROM group_dataset_schemas
WHERE group_id = ?1 AND dataset_id = ?2
",
    )
    .bind(group_id.to_string())
    .bind(dataset_id.as_str())
    .fetch_optional(&mut *connection)
    .await
    .context(SqlxSnafu)?;

    if let Some(row) = row {
        let payload = row.get::<Vec<u8>, _>("payload");
        let dataset_schema = decode_group_dataset_schema_payload(&payload)?;
        ensure!(
            dataset_schema.dataset_id == *dataset_id,
            StoredDatasetSchemaKeyMismatchSnafu {
                group: *group_id,
                key_dataset: dataset_id.clone(),
                payload_dataset: dataset_schema.dataset_id.clone(),
            }
        );
        return Ok(Some(dataset_schema.schema));
    }

    Ok(None)
}

pub(super) async fn insert_replication_group(
    connection: &mut SqliteStoreConnection,
    group: &ReplicationGroupRecord,
) -> Result<(), StoreError> {
    let material = ReplicationGroupMaterialRecord {
        group_id: group.group_id,
        member_keys: group.member_keys.clone(),
        local_member_index: group.local_member_index,
        group_schema: group.group_schema.clone(),
        security_material: group.security_material.clone(),
    };
    ensure_replication_group_material(connection, &material).await?;
    activate_replication_group_with_lifecycle(
        connection,
        group.group_id,
        &group.version_vector,
        &group.lifecycle,
    )
    .await
}

/// Store group material, accepting only exact idempotent replays.
pub(super) async fn ensure_replication_group_material(
    connection: &mut SqliteStoreConnection,
    material: &ReplicationGroupMaterialRecord,
) -> Result<(), StoreError> {
    if let Some(existing) = load_replication_group_material(connection, &material.group_id).await? {
        ensure!(
            existing == *material,
            ConflictingGroupMaterialSnafu {
                group_id: material.group_id
            }
        );
        return Ok(());
    }
    let member_count = material.member_count();
    ensure_member_index_in_bounds(material.local_member_index, member_count)?;

    let stored_member_count =
        i64::try_from(member_count.get()).context(MemberCountOverflowSnafu)?;
    sqlx::query(
        "
INSERT INTO replication_group_material (
    group_id,
    member_count,
    local_member_index,
    group_secret_crypto_version,
    group_secret_key_id,
    group_secret_nonce,
    group_secret_ciphertext
)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
",
    )
    .bind(material.group_id.to_string())
    .bind(stored_member_count)
    .bind(i64::from(material.local_member_index.as_u32()))
    .bind(i64::from(
        material
            .security_material
            .encrypted_group_secret
            .crypto_version
            .as_u16(),
    ))
    .bind(
        material
            .security_material
            .encrypted_group_secret
            .key_id
            .to_string(),
    )
    .bind(
        material
            .security_material
            .encrypted_group_secret
            .nonce
            .as_ref(),
    )
    .bind(
        material
            .security_material
            .encrypted_group_secret
            .ciphertext
            .as_ref(),
    )
    .execute(&mut *connection)
    .await
    .context(SqlxSnafu)?;

    for (member_index, member_key) in material
        .member_keys
        .ordered_member_keys()
        .iter()
        .enumerate()
    {
        let member_index = i64::try_from(member_index).context(MemberCountOverflowSnafu)?;
        sqlx::query(
            "
INSERT INTO group_members (group_id, member_index, member_identity, key_fingerprint)
VALUES (?1, ?2, ?3, ?4)
",
        )
        .bind(material.group_id.to_string())
        .bind(member_index)
        .bind(member_key.member_id.to_string())
        .bind(member_key.fingerprint.as_ref())
        .execute(&mut *connection)
        .await
        .context(SqlxSnafu)?;
    }
    insert_group_schema(connection, material.group_id, &material.group_schema).await?;
    Ok(())
}

pub(super) async fn insert_group_schema(
    connection: &mut SqliteStoreConnection,
    group_id: GroupId,
    group_schema: &GroupSchema,
) -> Result<(), StoreError> {
    for dataset_schema in group_schema.datasets() {
        sqlx::query(
            "
INSERT INTO group_dataset_schemas (group_id, dataset_id, payload)
VALUES (?1, ?2, ?3)
",
        )
        .bind(group_id.to_string())
        .bind(dataset_schema.dataset_id.as_str())
        .bind(encode_group_dataset_schema_payload(&dataset_schema))
        .execute(&mut *connection)
        .await
        .context(SqlxSnafu)?;
    }
    Ok(())
}

/// Insert the active marker for existing material.
pub(super) async fn activate_replication_group(
    connection: &mut SqliteStoreConnection,
    group_id: GroupId,
    version_vector: &VersionVector,
) -> Result<(), StoreError> {
    activate_replication_group_with_lifecycle(
        connection,
        group_id,
        version_vector,
        &ReplicationGroupLifecycle::Open,
    )
    .await
}

/// Insert the active marker and its application-access lifecycle.
pub(super) async fn activate_replication_group_with_lifecycle(
    connection: &mut SqliteStoreConnection,
    group_id: GroupId,
    version_vector: &VersionVector,
    lifecycle: &ReplicationGroupLifecycle,
) -> Result<(), StoreError> {
    let lifecycle_sql = replication_group_lifecycle_sql_label(lifecycle);
    let successor_group_id = lifecycle
        .successor_group_id()
        .map(|group_id| group_id.to_string());
    let final_versions = lifecycle.final_versions().map(encode_stored_version_vector);
    sqlx::query(
        "
INSERT INTO replication_groups (
    group_id, version_vector, lifecycle, successor_group_id, final_versions
)
VALUES (?1, ?2, ?3, ?4, ?5)
",
    )
    .bind(group_id.to_string())
    .bind(encode_stored_version_vector(version_vector))
    .bind(lifecycle_sql)
    .bind(successor_group_id)
    .bind(final_versions)
    .execute(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    Ok(())
}

/// Remove material only while no active marker exists.
pub(super) async fn remove_inactive_replication_group_material(
    connection: &mut SqliteStoreConnection,
    group_id: GroupId,
) -> Result<bool, StoreError> {
    let result = sqlx::query(
        "
DELETE FROM replication_group_material
WHERE group_id = ?1
  AND NOT EXISTS (
      SELECT 1 FROM replication_groups WHERE replication_groups.group_id = ?1
  )
",
    )
    .bind(group_id.to_string())
    .execute(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    Ok(result.rows_affected() > 0)
}

pub(super) async fn update_replication_group_version_vector(
    connection: &mut SqliteStoreConnection,
    group_id: &GroupId,
    version_vector: &VersionVector,
) -> Result<(), StoreError> {
    let rows_affected = sqlx::query(
        "
UPDATE replication_groups
SET version_vector = ?2
WHERE group_id = ?1
",
    )
    .bind(group_id.to_string())
    .bind(encode_stored_version_vector(version_vector))
    .execute(&mut *connection)
    .await
    .context(SqlxSnafu)?
    .rows_affected();
    ensure!(
        rows_affected == 1,
        MissingStoredGroupSnafu {
            group_id: *group_id
        }
    );
    Ok(())
}

pub(super) async fn update_replication_group_lifecycle(
    connection: &mut SqliteStoreConnection,
    group_id: &GroupId,
    lifecycle: &ReplicationGroupLifecycle,
) -> Result<(), StoreError> {
    let lifecycle_sql = replication_group_lifecycle_sql_label(lifecycle);
    let successor_group_id = lifecycle
        .successor_group_id()
        .map(|group_id| group_id.to_string());
    let final_versions = lifecycle.final_versions().map(encode_stored_version_vector);
    let rows_affected = sqlx::query(
        "
UPDATE replication_groups
SET lifecycle = ?2, successor_group_id = ?3, final_versions = ?4
WHERE group_id = ?1
",
    )
    .bind(group_id.to_string())
    .bind(lifecycle_sql)
    .bind(successor_group_id)
    .bind(final_versions)
    .execute(&mut *connection)
    .await
    .context(SqlxSnafu)?
    .rows_affected();
    ensure!(
        rows_affected == 1,
        MissingStoredGroupSnafu {
            group_id: *group_id
        }
    );
    Ok(())
}
