//! SQLite persistence for member and group security material.

use super::*;

pub(super) async fn load_local_member_private_keys(
    connection: &mut SqliteStoreConnection,
    member_id: &MemberIdentity,
) -> Result<Option<LocalMemberPrivateKeysRecord>, StoreError> {
    let row = sqlx::query(
        "
SELECT private_keys_crypto_version, private_keys_key_id, private_keys_nonce, private_keys_ciphertext
FROM local_members
WHERE member_identity = ?1
",
    )
    .bind(member_id.to_string())
    .fetch_optional(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    let Some(row) = row else {
        return Ok(None);
    };

    let encrypted_private_keys = decode_encrypted_store_secret(
        row.get("private_keys_crypto_version"),
        row.get("private_keys_key_id"),
        row.get("private_keys_nonce"),
        row.get("private_keys_ciphertext"),
    )?;
    Ok(Some(LocalMemberPrivateKeysRecord {
        member_id: member_id.clone(),
        private_keys: EncryptedLocalMemberPrivateKeys {
            secret: encrypted_private_keys,
        },
    }))
}

pub(super) async fn ensure_local_member_private_keys(
    connection: &mut SqliteStoreConnection,
    record: &LocalMemberPrivateKeysRecord,
) -> Result<(), StoreError> {
    if let Some(existing) = load_local_member_private_keys(connection, &record.member_id).await? {
        ensure!(
            existing == *record,
            ConflictingMemberSecurityMaterialSnafu {
                object: "local member private keys",
                member_id: record.member_id.clone(),
            }
        );
        return Ok(());
    }

    let secret = &record.private_keys.secret;
    sqlx::query(
        "
INSERT INTO local_members (
    member_identity,
    private_keys_crypto_version,
    private_keys_key_id,
    private_keys_nonce,
    private_keys_ciphertext
)
VALUES (?1, ?2, ?3, ?4, ?5)
",
    )
    .bind(record.member_id.to_string())
    .bind(i64::from(secret.crypto_version.as_u16()))
    .bind(secret.key_id.to_string())
    .bind(secret.nonce.as_ref())
    .bind(secret.ciphertext.as_ref())
    .execute(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    Ok(())
}

pub(super) async fn load_member_public_keys(
    connection: &mut SqliteStoreConnection,
    key_id: &MemberKeyId,
) -> Result<Option<MemberPublicKeysRecord>, StoreError> {
    let row = sqlx::query(
        "
SELECT signing_public_key, encryption_public_key
FROM member_public_keys
WHERE member_identity = ?1 AND key_fingerprint = ?2
",
    )
    .bind(key_id.member_id.to_string())
    .bind(key_id.fingerprint.as_ref())
    .fetch_optional(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    let Some(row) = row else {
        return Ok(None);
    };

    Ok(Some(MemberPublicKeysRecord {
        key_id: key_id.clone(),
        signing_public_key: row
            .get::<Vec<u8>, _>("signing_public_key")
            .into_boxed_slice(),
        encryption_public_key: row
            .get::<Vec<u8>, _>("encryption_public_key")
            .into_boxed_slice(),
    }))
}

pub(super) async fn load_member_public_keys_for_member(
    connection: &mut SqliteStoreConnection,
    member_id: &MemberIdentity,
) -> Result<Vec<MemberPublicKeysRecord>, StoreError> {
    let rows = sqlx::query(
        "
SELECT key_fingerprint, signing_public_key, encryption_public_key
FROM member_public_keys
WHERE member_identity = ?1
ORDER BY key_fingerprint
",
    )
    .bind(member_id.to_string())
    .fetch_all(&mut *connection)
    .await
    .context(SqlxSnafu)?;

    let mut records = Vec::with_capacity(rows.len());
    for row in rows {
        let fingerprint = decode_key_fingerprint(&row.get::<Vec<u8>, _>("key_fingerprint"))?;
        records.push(MemberPublicKeysRecord {
            key_id: MemberKeyId {
                member_id: member_id.clone(),
                fingerprint,
            },
            signing_public_key: row
                .get::<Vec<u8>, _>("signing_public_key")
                .into_boxed_slice(),
            encryption_public_key: row
                .get::<Vec<u8>, _>("encryption_public_key")
                .into_boxed_slice(),
        });
    }
    Ok(records)
}

pub(super) async fn load_member_public_keys_for_fingerprint(
    connection: &mut SqliteStoreConnection,
    fingerprint: &KeyFingerprint,
) -> Result<Vec<MemberPublicKeysRecord>, StoreError> {
    let rows = sqlx::query(
        "
SELECT member_identity, signing_public_key, encryption_public_key
FROM member_public_keys
WHERE key_fingerprint = ?1
ORDER BY member_identity
",
    )
    .bind(fingerprint.as_ref())
    .fetch_all(&mut *connection)
    .await
    .context(SqlxSnafu)?;

    let mut records = Vec::with_capacity(rows.len());
    for row in rows {
        let member_id = decode_member_identity(&row.get::<String, _>("member_identity"))?;
        records.push(MemberPublicKeysRecord {
            key_id: MemberKeyId {
                member_id,
                fingerprint: *fingerprint,
            },
            signing_public_key: row
                .get::<Vec<u8>, _>("signing_public_key")
                .into_boxed_slice(),
            encryption_public_key: row
                .get::<Vec<u8>, _>("encryption_public_key")
                .into_boxed_slice(),
        });
    }
    Ok(records)
}

pub(super) async fn ensure_member_public_keys(
    connection: &mut SqliteStoreConnection,
    record: &MemberPublicKeysRecord,
) -> Result<(), StoreError> {
    validate_member_public_keys_record(record)?;
    if let Some(existing) = load_member_public_keys(connection, &record.key_id).await? {
        ensure!(
            existing == *record,
            ConflictingMemberSecurityMaterialSnafu {
                object: "member public keys",
                member_id: record.key_id.member_id.clone(),
            }
        );
        return Ok(());
    }

    sqlx::query(
        "
INSERT INTO member_public_keys (
    member_identity,
    key_fingerprint,
    signing_public_key,
    encryption_public_key
)
VALUES (?1, ?2, ?3, ?4)
",
    )
    .bind(record.key_id.member_id.to_string())
    .bind(record.key_id.fingerprint.as_ref())
    .bind(record.signing_public_key.as_ref())
    .bind(record.encryption_public_key.as_ref())
    .execute(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    Ok(())
}

pub(super) async fn load_member_key_trust_evidence(
    connection: &mut SqliteStoreConnection,
    key_id: &MemberKeyId,
) -> Result<MemberKeyTrustEvidenceSet, StoreError> {
    let evidence_kinds = sqlx::query_scalar::<_, String>(
        "
SELECT evidence_kind
FROM member_key_trust_evidence
WHERE member_identity = ?1 AND key_fingerprint = ?2
",
    )
    .bind(key_id.member_id.to_string())
    .bind(key_id.fingerprint.as_ref())
    .fetch_all(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    let mut evidence = MemberKeyTrustEvidenceSet::empty();
    for evidence_kind in evidence_kinds {
        let evidence_kind = decode_member_key_trust_evidence_kind(&evidence_kind)?;
        evidence.insert(evidence_kind);
    }
    Ok(evidence)
}

pub(super) async fn ensure_member_key_trust_evidence(
    connection: &mut SqliteStoreConnection,
    record: &MemberKeyTrustEvidenceRecord,
) -> Result<(), StoreError> {
    sqlx::query(
        "
INSERT OR IGNORE INTO member_key_trust_evidence (
    member_identity,
    key_fingerprint,
    evidence_kind
)
VALUES (?1, ?2, ?3)
",
    )
    .bind(record.key_id.member_id.to_string())
    .bind(record.key_id.fingerprint.as_ref())
    .bind(record.evidence_kind.as_str())
    .execute(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    Ok(())
}

pub(super) async fn is_key_fingerprint_blocked(
    connection: &mut SqliteStoreConnection,
    fingerprint: &KeyFingerprint,
) -> Result<bool, StoreError> {
    let count = sqlx::query_scalar::<_, i64>(
        "
SELECT COUNT(*)
FROM blocked_key_fingerprints
WHERE key_fingerprint = ?1
",
    )
    .bind(fingerprint.as_ref())
    .fetch_one(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    Ok(count > 0)
}

pub(super) async fn ensure_blocked_key_fingerprint(
    connection: &mut SqliteStoreConnection,
    fingerprint: &KeyFingerprint,
) -> Result<(), StoreError> {
    sqlx::query(
        "
INSERT OR IGNORE INTO blocked_key_fingerprints (key_fingerprint)
VALUES (?1)
",
    )
    .bind(fingerprint.as_ref())
    .execute(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    Ok(())
}
