//! SQLite persistence for one-to-one reliable-delivery sender work.

use super::*;
use bytes::Bytes;
use chrono::{DateTime, SecondsFormat, Utc};
use sqlx::sqlite::SqliteRow;
use std::time::SystemTime;

/// Load all pending metadata without reading any encoded envelope blobs.
pub(super) async fn load_reliable_delivery_work_metadata(
    connection: &mut SqliteStoreConnection,
) -> Result<Vec<StoredReliableDeliveryWorkMetadata>, StoreError> {
    let rows = sqlx::query(
        "
SELECT message_id, recipient, first_submitted_at
FROM reliable_delivery_work
",
    )
    .fetch_all(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    rows.iter().map(decode_reliable_work_metadata).collect()
}

/// Load one complete envelope after the scheduler selected its recipient route.
pub(super) async fn load_reliable_delivery_work(
    connection: &mut SqliteStoreConnection,
    message_id: crate::delivery::shared::MessageId,
) -> Result<Option<StoredReliableDeliveryWork>, StoreError> {
    let row = sqlx::query(
        "
SELECT message_id, recipient, first_submitted_at, encoded_envelope
FROM reliable_delivery_work
WHERE message_id = ?1
",
    )
    .bind(message_id.0.to_string())
    .fetch_optional(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    row.map(|row| {
        let metadata = decode_reliable_work_metadata(&row)?;
        let encoded_envelope = row
            .try_get::<Vec<u8>, _>("encoded_envelope")
            .context(SqlxSnafu)?;
        Ok(StoredReliableDeliveryWork {
            metadata,
            encoded_envelope: Bytes::from(encoded_envelope),
        })
    })
    .transpose()
}

/// Store one newly sealed envelope before it becomes transport-eligible.
pub(super) async fn store_reliable_delivery_work(
    connection: &mut SqliteStoreConnection,
    work: &StoredReliableDeliveryWork,
) -> Result<(), StoreError> {
    sqlx::query(
        "
INSERT INTO reliable_delivery_work (
    message_id,
    recipient,
    first_submitted_at,
    encoded_envelope
) VALUES (?1, ?2, ?3, ?4)
ON CONFLICT(message_id) DO UPDATE SET
    recipient = excluded.recipient,
    first_submitted_at = excluded.first_submitted_at,
    encoded_envelope = excluded.encoded_envelope
",
    )
    .bind(work.metadata.message_id.0.to_string())
    .bind(work.metadata.recipient.to_string())
    .bind(encode_submission_timestamp(
        work.metadata.first_submitted_at,
    ))
    .bind(work.encoded_envelope.as_ref())
    .execute(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    Ok(())
}

/// Remove one semantically completed or permanently failed sender item.
pub(super) async fn remove_reliable_delivery_work(
    connection: &mut SqliteStoreConnection,
    message_id: crate::delivery::shared::MessageId,
) -> Result<bool, StoreError> {
    let result = sqlx::query(
        "
DELETE FROM reliable_delivery_work
WHERE message_id = ?1
",
    )
    .bind(message_id.0.to_string())
    .execute(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    Ok(result.rows_affected() > 0)
}

/// Decode the small scheduling projection shared by metadata and full-row loads.
fn decode_reliable_work_metadata(
    row: &SqliteRow,
) -> Result<StoredReliableDeliveryWorkMetadata, StoreError> {
    let raw_message_id = row.try_get::<String, _>("message_id").context(SqlxSnafu)?;
    let message_id = Uuid::parse_str(&raw_message_id)
        .map(crate::delivery::shared::MessageId)
        .map_err(|source| invalid_stored_object("reliable delivery message id", source))?;
    let raw_recipient = row.try_get::<String, _>("recipient").context(SqlxSnafu)?;
    let recipient = decode_member_identity(&raw_recipient)?;
    let raw_timestamp = row
        .try_get::<String, _>("first_submitted_at")
        .context(SqlxSnafu)?;
    let first_submitted_at = DateTime::parse_from_rfc3339(&raw_timestamp)
        .map(SystemTime::from)
        .map_err(|source| {
            invalid_stored_object("reliable delivery submission timestamp", source)
        })?;
    Ok(StoredReliableDeliveryWorkMetadata {
        message_id,
        recipient,
        first_submitted_at,
    })
}

/// Encode one stable, human-readable UTC timestamp for store inspection.
fn encode_submission_timestamp(timestamp: SystemTime) -> String {
    DateTime::<Utc>::from(timestamp).to_rfc3339_opts(SecondsFormat::Nanos, true)
}
