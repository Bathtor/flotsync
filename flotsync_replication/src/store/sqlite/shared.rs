//! Shared SQLite decoding, validation, and representation helpers.

use super::*;

#[derive(Debug, Snafu)]
pub(super) enum StoredGroupLifecycleError {
    /// Open groups must not carry accepted migration data.
    #[snafu(display("Open replication group unexpectedly stored migration data."))]
    OpenWithMigrationData,
    /// Non-open groups require both the accepted successor and immutable cut.
    #[snafu(display(
        "Replication group lifecycle '{raw}' did not store required field '{field}'."
    ))]
    MissingMigrationData { raw: String, field: &'static str },
    /// Stored lifecycle discriminator is not supported.
    #[snafu(display("Unknown replication group lifecycle '{raw}'."))]
    Unknown { raw: String },
}

/// Origin of a member index being checked against its associated member set.
#[derive(Clone, Copy, Debug)]
pub(super) enum MemberIndexOrigin {
    /// The index came from caller-provided group material.
    Caller,
    /// The index was decoded from persisted group material.
    Stored,
}

/// One `u64` bit pattern represented in SQLite's signed `INTEGER` domain.
///
/// This representation is suitable only when SQL does not order or compare the
/// value numerically. Negative integers preserve the upper half of the `u64`
/// domain and convert back without loss.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct U64BitsInI64(i64);

impl From<u64> for U64BitsInI64 {
    fn from(value: u64) -> Self {
        Self(i64::from_ne_bytes(value.to_ne_bytes()))
    }
}

impl From<i64> for U64BitsInI64 {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl From<U64BitsInI64> for i64 {
    fn from(value: U64BitsInI64) -> Self {
        value.0
    }
}

impl From<U64BitsInI64> for u64 {
    fn from(value: U64BitsInI64) -> Self {
        Self::from_ne_bytes(value.0.to_ne_bytes())
    }
}

pub(super) async fn load_group_member_keys(
    connection: &mut SqliteStoreConnection,
    group_id: &GroupId,
    expected_member_count: NonZeroUsize,
) -> Result<GroupMemberKeys, StoreError> {
    let rows = sqlx::query(
        "
SELECT member_identity, key_fingerprint
FROM group_members
WHERE group_id = ?1
ORDER BY member_index
",
    )
    .bind(group_id.to_string())
    .fetch_all(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    ensure!(
        rows.len() == expected_member_count.get(),
        StoredGroupMemberCountMismatchSnafu {
            group_id: *group_id,
            expected_member_count: expected_member_count.get(),
            actual_member_count: rows.len(),
        }
    );

    let mut member_keys = Vec::with_capacity(rows.len());
    for row in rows {
        let raw_member = row.get::<String, _>("member_identity");
        let raw_fingerprint = row.get::<Vec<u8>, _>("key_fingerprint");
        let member_id = decode_member_identity(&raw_member)?;
        let fingerprint = decode_key_fingerprint(&raw_fingerprint)?;
        member_keys.push(MemberKeyId {
            member_id,
            fingerprint,
        });
    }
    GroupMemberKeys::from_ordered_member_keys(member_keys)
        .map_err(|source| invalid_stored_object("group member keys", source))
}

pub(super) async fn load_group_member_count(
    connection: &mut SqliteStoreConnection,
    group_id: &GroupId,
) -> Result<NonZeroUsize, StoreError> {
    let member_count = sqlx::query_scalar::<_, i64>(
        "
SELECT material.member_count
FROM replication_groups AS active
JOIN replication_group_material AS material ON material.group_id = active.group_id
WHERE active.group_id = ?1
",
    )
    .bind(group_id.to_string())
    .fetch_optional(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    let Some(member_count) = member_count else {
        return MissingStoredGroupSnafu {
            group_id: *group_id,
        }
        .fail()
        .map_err(StoreError::from);
    };
    decode_non_zero_member_count(member_count)
}

pub(super) async fn dataset_exists_in_group(
    connection: &mut SqliteStoreConnection,
    group_id: &GroupId,
    dataset_id: &DatasetId,
) -> Result<bool, StoreError> {
    let exists = sqlx::query_scalar::<_, i64>(
        "
SELECT 1
FROM datasets
WHERE group_id = ?1 AND dataset_id = ?2
",
    )
    .bind(group_id.to_string())
    .bind(dataset_id.as_str())
    .fetch_optional(&mut *connection)
    .await
    .context(SqlxSnafu)?
    .is_some();
    Ok(exists)
}

pub(super) async fn ensure_dataset_exists(
    connection: &mut SqliteStoreConnection,
    group_id: &GroupId,
    dataset_id: &DatasetId,
) -> Result<(), StoreError> {
    // Persist the parent dataset entry even when this snapshot currently has no rows.
    sqlx::query(
        "
INSERT INTO datasets (group_id, dataset_id)
VALUES (?1, ?2)
ON CONFLICT(group_id, dataset_id) DO NOTHING
",
    )
    .bind(group_id.to_string())
    .bind(dataset_id.as_str())
    .execute(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    Ok(())
}

pub(super) fn encode_dataset_row_snapshot(
    schema: &flotsync_data_types::schema::Schema,
    row: &ReplicationRowStateSnapshot,
) -> Result<Vec<u8>, StoreError> {
    let row = encode_row_snapshot(row, schema)
        .map_err(|source| invalid_stored_object("dataset row snapshot", source))?;
    Ok(row.encode_to_bytes().to_vec())
}

pub(super) fn decode_dataset_row_snapshot(
    schema: &flotsync_data_types::schema::Schema,
    bytes: &[u8],
) -> Result<ReplicationRowStateSnapshot, StoreError> {
    let row = datamodel_proto::RowSnapshot::decode_from_slice(bytes).map_err(|source| {
        SqliteStoreError::DecodeStoredProto {
            object: "dataset row snapshot",
            source,
        }
    })?;
    decode_row_snapshot(row, schema)
        .map_err(|source| invalid_stored_object("dataset row snapshot", source))
}

pub(super) fn decode_dataset_row_last_changed_versions(
    row: &sqlx::sqlite::SqliteRow,
    member_count: NonZeroUsize,
) -> Result<VersionVector, StoreError> {
    let versions = row
        .try_get::<Vec<u8>, _>("row_last_changed_versions")
        .context(SqlxSnafu)?;
    decode_stored_version_vector(&versions, member_count)
}

/// Decode row-creation provenance from the named columns of one stored row.
pub(super) fn decode_dataset_row_created_by(
    row: &sqlx::sqlite::SqliteRow,
    row_key: RowKey,
    node_index_column: &str,
    version_column: &str,
    member_count: NonZeroUsize,
) -> Result<Option<UpdateId>, StoreError> {
    let node_index = row
        .try_get::<Option<i64>, _>(node_index_column)
        .context(SqlxSnafu)?;
    let version = row
        .try_get::<Option<i64>, _>(version_column)
        .context(SqlxSnafu)?;
    decode_dataset_row_created_by_values(row_key, node_index, version, member_count)
}

/// Decode nullable row-creator scalar values after loading them from SQLite.
pub(super) fn decode_dataset_row_created_by_values(
    row_key: RowKey,
    node_index: Option<i64>,
    version: Option<i64>,
    member_count: NonZeroUsize,
) -> Result<Option<UpdateId>, StoreError> {
    match (node_index, version) {
        (Some(node_index), Some(version)) => {
            let node_index = match u32::try_from(node_index) {
                Ok(node_index) if (node_index as usize) < member_count.get() => node_index,
                Ok(_) | Err(_) => {
                    return InvalidStoredRowCreatorIndexSnafu {
                        row_key,
                        creator_index: node_index,
                        member_count: member_count.get(),
                    }
                    .fail()
                    .map_err(StoreError::from);
                }
            };
            let version = u64::from(U64BitsInI64::from(version));
            Ok(Some(UpdateId {
                node_index,
                version,
            }))
        }
        (None, None) => Ok(None),
        (Some(_), None) | (None, Some(_)) => IncompleteStoredRowCreationProvenanceSnafu { row_key }
            .fail()
            .map_err(StoreError::from),
    }
}

pub(super) fn encode_stored_version_vector(version_vector: &VersionVector) -> Vec<u8> {
    VersionVectorProtoCodec::from(version_vector).encode_proto_to_vec()
}

/// Return the stable `SQLite` label for one group lifecycle variant.
pub(super) fn replication_group_lifecycle_sql_label(
    lifecycle: &ReplicationGroupLifecycle,
) -> &'static str {
    match lifecycle {
        ReplicationGroupLifecycle::Open => GROUP_LIFECYCLE_OPEN_SQL,
        ReplicationGroupLifecycle::ReadOnly { .. } => GROUP_LIFECYCLE_READ_ONLY_SQL,
        ReplicationGroupLifecycle::Closed { .. } => GROUP_LIFECYCLE_CLOSED_SQL,
    }
}

/// Require one migration-specific column for a non-open lifecycle row.
pub(super) fn required_stored_group_lifecycle_field<T>(
    value: Option<T>,
    raw: &str,
    field: &'static str,
) -> Result<T, StoreError> {
    value
        .context(MissingMigrationDataSnafu {
            raw: raw.to_owned(),
            field,
        })
        .map_err(|source| invalid_stored_object("replication group lifecycle", source))
}

/// Ensure an open lifecycle row does not carry migration-only columns.
pub(super) fn validate_open_group_lifecycle_fields(
    successor_group_id: Option<&str>,
    final_versions: Option<&[u8]>,
) -> Result<(), StoredGroupLifecycleError> {
    ensure!(
        successor_group_id.is_none() && final_versions.is_none(),
        OpenWithMigrationDataSnafu
    );
    Ok(())
}

/// Decode lifecycle state and enforce the discriminator/cut relationship.
pub(super) fn decode_replication_group_lifecycle(
    row: &sqlx::sqlite::SqliteRow,
    member_count: NonZeroUsize,
) -> Result<ReplicationGroupLifecycle, StoreError> {
    let raw = row.get::<String, _>("lifecycle");
    let successor_group_id = row.get::<Option<String>, _>("successor_group_id");
    let final_versions = row.get::<Option<Vec<u8>>, _>("final_versions");
    match raw.as_str() {
        GROUP_LIFECYCLE_OPEN_SQL => {
            validate_open_group_lifecycle_fields(
                successor_group_id.as_deref(),
                final_versions.as_deref(),
            )
            .map_err(|source| invalid_stored_object("replication group lifecycle", source))?;
            Ok(ReplicationGroupLifecycle::Open)
        }
        GROUP_LIFECYCLE_READ_ONLY_SQL => {
            let successor_group_id = required_stored_group_lifecycle_field(
                successor_group_id,
                &raw,
                "successor_group_id",
            )?;
            let successor_group_id = decode_group_id(&successor_group_id)?;
            let final_versions =
                required_stored_group_lifecycle_field(final_versions, &raw, "final_versions")?;
            let final_versions = decode_stored_version_vector(&final_versions, member_count)?;
            Ok(ReplicationGroupLifecycle::ReadOnly {
                successor_group_id,
                final_versions,
            })
        }
        GROUP_LIFECYCLE_CLOSED_SQL => {
            let successor_group_id = required_stored_group_lifecycle_field(
                successor_group_id,
                &raw,
                "successor_group_id",
            )?;
            let successor_group_id = decode_group_id(&successor_group_id)?;
            let final_versions =
                required_stored_group_lifecycle_field(final_versions, &raw, "final_versions")?;
            let final_versions = decode_stored_version_vector(&final_versions, member_count)?;
            Ok(ReplicationGroupLifecycle::Closed {
                successor_group_id,
                final_versions,
            })
        }
        _ => Err(invalid_stored_object(
            "replication group lifecycle",
            StoredGroupLifecycleError::Unknown { raw },
        )),
    }
}

pub(super) fn encode_group_dataset_schema_payload(dataset_schema: &DatasetSchema) -> Vec<u8> {
    dataset_schema.encode_proto_to_vec()
}

pub(super) fn decode_group_dataset_schema_payload(
    bytes: &[u8],
) -> Result<DatasetSchema, StoreError> {
    decode_stored_proto(
        "group dataset schema",
        DatasetSchema::try_decode_proto_from_slice(bytes),
    )
}

pub(super) fn decode_stored_version_vector(
    bytes: &[u8],
    member_count: NonZeroUsize,
) -> Result<VersionVector, StoreError> {
    let version_vector = decode_stored_proto(
        "version vector",
        VersionVectorProtoCodec::try_decode_proto_from_slice(bytes),
    )?;
    let version_vector = version_vector.into_version_vector();
    if version_vector.num_members() != member_count {
        return Err(invalid_stored_object(
            "version vector",
            VersionVectorCodecError::MemberCountMismatch {
                expected_members: member_count.get(),
                actual_members: version_vector.num_members().get(),
            },
        ));
    }
    Ok(version_vector)
}

pub(super) fn decode_stored_update_row(
    expected_group_id: &GroupId,
    member_count: NonZeroUsize,
    update_id: UpdateId,
    row: &sqlx::sqlite::SqliteRow,
) -> Result<ReplicationUpdateRecord, StoreError> {
    let sender = decode_member_identity(&row.get::<String, _>("sender"))?;
    let applied_locally = row.get::<bool, _>("applied_locally");
    let update_message = row.get::<Vec<u8>, _>("update_message");
    let message = decode_stored_proto(
        "update",
        UpdateMessage::try_decode_proto_from_slice_with(
            &update_message,
            MemberCountContext::new(member_count),
        ),
    )?;
    ensure!(
        message.group_id == *expected_group_id,
        StoredUpdateGroupMismatchSnafu {
            expected_group_id: *expected_group_id,
            actual_group_id: message.group_id,
        }
    );
    ensure!(
        message.update_id == update_id,
        StoredUpdateIdMismatchSnafu {
            expected_update_id: update_id,
            actual_update_id: message.update_id,
        }
    );
    Ok(ReplicationUpdateRecord {
        group_id: *expected_group_id,
        update_id,
        sender,
        read_versions: message.read_versions,
        dataset_updates: message
            .dataset_updates
            .into_iter()
            .map(|dataset_update| DatasetUpdateRecord {
                dataset_id: dataset_update.dataset_id,
                operations: dataset_update.operations,
            })
            .collect(),
        applied_locally,
    })
}

pub(super) fn encode_update_version_sort_key(version: u64) -> [u8; UPDATE_VERSION_SORT_KEY_BYTES] {
    version.to_be_bytes()
}

pub(super) fn encode_update_version_sort_key_vec(version: u64) -> Vec<u8> {
    encode_update_version_sort_key(version).to_vec()
}

pub(super) fn decode_update_version_sort_key(bytes: &[u8]) -> Result<u64, StoreError> {
    let bytes: [u8; UPDATE_VERSION_SORT_KEY_BYTES] =
        bytes
            .try_into()
            .map_err(|_| SqliteStoreError::InvalidStoredSortKey {
                object: "update version",
                len: bytes.len(),
            })?;
    Ok(u64::from_be_bytes(bytes))
}

/// Decode one encrypted secret cell from `SQLite` scalar column values.
#[allow(
    clippy::needless_pass_by_value,
    reason = "Owned strings keep sqlx row.get call sites type-inference friendly."
)]
pub(super) fn decode_encrypted_store_secret(
    raw_crypto_version: i64,
    key_id: String,
    nonce: Vec<u8>,
    ciphertext: Vec<u8>,
) -> Result<EncryptedStoreSecret, StoreError> {
    let crypto_version = u16::try_from(raw_crypto_version)
        .context(SecretCryptoVersionOverflowSnafu)
        .map_err(StoreError::from)?;
    let key_id: StoreSecretKeyId = key_id
        .parse()
        .context(InvalidStoreSecretKeyIdSnafu)
        .map_err(StoreError::from)?;
    Ok(EncryptedStoreSecret {
        crypto_version: StoreSecretCryptoVersion::new(crypto_version),
        key_id,
        nonce: nonce.into_boxed_slice(),
        ciphertext: ciphertext.into_boxed_slice(),
    })
}

pub(super) fn decode_non_zero_member_count(member_count: i64) -> Result<NonZeroUsize, StoreError> {
    let member_count = usize::try_from(member_count).context(StoredMemberCountOverflowSnafu)?;
    NonZeroUsize::new(member_count)
        .context(StoredEmptyGroupMembersSnafu)
        .map_err(StoreError::from)
}

/// Convert caller-visible limits into `SQLite`'s signed `LIMIT` range.
///
/// Callers may pass `usize::MAX` as a practical "no limit" sentinel for a
/// single storage call. `SQLite` accepts signed 64-bit limits, so saturating keeps
/// that sentinel useful without changing smaller, exact limits.
pub(super) fn sqlite_limit_value(limit: NonZeroUsize) -> i64 {
    i64::try_from(limit.get()).unwrap_or(i64::MAX)
}

/// Ensure a member index fits its member set and classify failure by its origin.
pub(super) fn ensure_member_index_in_bounds(
    member_index: MemberIndex,
    member_count: NonZeroUsize,
    origin: MemberIndexOrigin,
) -> Result<(), StoreError> {
    let local_member_index = member_index.as_u32();
    let member_count = member_count.get();
    let in_bounds = (local_member_index as usize) < member_count;
    match origin {
        MemberIndexOrigin::Caller => ensure!(
            in_bounds,
            InvalidLocalMemberIndexSnafu {
                local_member_index,
                member_count,
            }
        ),
        MemberIndexOrigin::Stored => ensure!(
            in_bounds,
            InvalidStoredLocalMemberIndexSnafu {
                local_member_index,
                member_count,
            }
        ),
    }
    Ok(())
}

pub(super) fn decode_member_index(
    raw: i64,
    member_count: NonZeroUsize,
) -> Result<MemberIndex, StoreError> {
    let member_index = u32::try_from(raw).context(MemberIndexOverflowSnafu)?;
    let member_index = MemberIndex::new(member_index);
    ensure_member_index_in_bounds(member_index, member_count, MemberIndexOrigin::Stored)?;
    Ok(member_index)
}

pub(super) fn decode_member_index_value(raw: i64) -> Result<u32, StoreError> {
    u32::try_from(raw)
        .context(MemberIndexOverflowSnafu)
        .map_err(StoreError::from)
}

pub(super) fn decode_group_id(raw: &str) -> Result<GroupId, StoreError> {
    let group_id = Uuid::parse_str(raw).context(InvalidGroupIdSnafu)?;
    Ok(GroupId(group_id))
}

pub(super) fn decode_dataset_id(raw: &str) -> Result<DatasetId, StoreError> {
    raw.parse()
        .map_err(|source| invalid_stored_object("dataset id", source))
}

pub(super) fn decode_row_key(raw: &str) -> Result<RowKey, StoreError> {
    let row_key = Uuid::parse_str(raw).context(InvalidRowKeySnafu)?;
    Ok(RowKey(row_key))
}

pub(super) fn decode_member_identity(raw: &str) -> Result<MemberIdentity, StoreError> {
    let member_identity = raw.parse().context(InvalidMemberIdentitySnafu {
        raw: raw.to_owned(),
    })?;
    Ok(member_identity)
}

pub(super) fn decode_key_fingerprint(raw: &[u8]) -> Result<KeyFingerprint, StoreError> {
    KeyFingerprint::try_from_slice(raw)
        .map_err(|source| invalid_stored_object("key fingerprint", source))
}

pub(super) fn decode_member_key_trust_evidence_kind(
    raw: &str,
) -> Result<MemberKeyTrustEvidenceKind, StoreError> {
    raw.parse()
        .map_err(|source| invalid_stored_object("member key trust evidence kind", source))
}

pub(super) fn validate_member_public_keys_record(
    record: &MemberPublicKeysRecord,
) -> Result<(), StoreError> {
    let public_keys = public_keys_from_member_record(record)?;
    ensure!(
        public_keys.fingerprint() == record.key_id.fingerprint,
        ConflictingMemberSecurityMaterialSnafu {
            object: "member public keys fingerprint",
            member_id: record.key_id.member_id.clone(),
        }
    );
    Ok(())
}

pub(super) fn public_keys_from_member_record(
    record: &MemberPublicKeysRecord,
) -> Result<PublicMemberKeys, StoreError> {
    PublicMemberKeys::from_key_bytes(
        record.key_id.member_id.clone(),
        record.signing_public_key.as_ref(),
        record.encryption_public_key.as_ref(),
    )
    .map_err(|source| invalid_stored_object("member public keys", source))
}

pub(super) fn invalid_stored_object(
    object: &'static str,
    source: impl StdError + Send + Sync + 'static,
) -> StoreError {
    SqliteStoreError::InvalidStoredObject {
        object,
        source: Box::new(source),
    }
    .into()
}

pub(super) fn decode_stored_proto<T, E>(
    object: &'static str,
    result: Result<T, ProtoInputDecodeError<E>>,
) -> Result<T, StoreError>
where
    E: StdError + Send + Sync + 'static,
{
    match result {
        Ok(value) => Ok(value),
        Err(ProtoInputDecodeError::Decode { source }) => {
            Err(SqliteStoreError::DecodeStoredProto { object, source }.into())
        }
        Err(ProtoInputDecodeError::Convert { source }) => {
            Err(invalid_stored_object(object, source))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::StoreErrorClass;

    #[test]
    fn member_index_bounds_failure_reflects_the_index_origin() {
        let member_count = NonZeroUsize::new(1).expect("test member count is non-zero");
        let member_index = MemberIndex::new(1);

        let caller_error =
            ensure_member_index_in_bounds(member_index, member_count, MemberIndexOrigin::Caller)
                .expect_err("caller index should be out of bounds");
        assert!(matches!(
            sqlite_source(&caller_error),
            SqliteStoreError::InvalidLocalMemberIndex { .. }
        ));
        assert_eq!(
            caller_error.classification().class,
            StoreErrorClass::Contract
        );

        let stored_error =
            ensure_member_index_in_bounds(member_index, member_count, MemberIndexOrigin::Stored)
                .expect_err("stored index should be out of bounds");
        assert!(matches!(
            sqlite_source(&stored_error),
            SqliteStoreError::InvalidStoredLocalMemberIndex { .. }
        ));
        assert_eq!(
            stored_error.classification().class,
            StoreErrorClass::InvalidData
        );
    }

    /// Return the concrete SQLite source retained by a store error.
    fn sqlite_source(error: &StoreError) -> &SqliteStoreError {
        let StoreError::StoreExternal { source, .. } = error;
        source
            .downcast_ref::<SqliteStoreError>()
            .expect("store error should retain its SQLite source")
    }
}
