//! SQLite persistence for member and group security material.

use super::*;

pub(super) async fn load_local_member_identity(
    connection: &mut SqliteStoreConnection,
) -> Result<Option<MemberIdentity>, StoreError> {
    let rows = sqlx::query(
        "
SELECT DISTINCT member_identity
FROM local_members
LIMIT 2
",
    )
    .fetch_all(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    let mut rows = rows.into_iter();
    let Some(first_row) = rows.next() else {
        return Ok(None);
    };
    let first_identity_text = first_row.get::<String, _>("member_identity");
    let first = decode_member_identity(&first_identity_text)?;
    let Some(second_row) = rows.next() else {
        return Ok(Some(first));
    };
    let second_identity_text = second_row.get::<String, _>("member_identity");
    let second = decode_member_identity(&second_identity_text)?;
    Err(AmbiguousLocalMemberIdentitiesSnafu { first, second }
        .build()
        .into())
}

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
    if let Some(existing_member) = load_local_member_identity(connection).await? {
        ensure!(
            existing_member == record.member_id,
            ConflictingLocalMemberIdentitySnafu {
                existing: existing_member,
                requested: record.member_id.clone(),
            }
        );
    }
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

pub(super) async fn load_member_public_key_ids_into(
    connection: &mut SqliteStoreConnection,
    mut page: PageAttempt<
        '_,
        (),
        MemberKeyPageContinuation,
        OwnedNoMetadataPageBatch<'_, MemberKeyId>,
    >,
) -> Result<(), PageError> {
    let mut query_builder = QueryBuilder::<Sqlite>::new(
        "SELECT member_identity, key_fingerprint FROM member_public_keys WHERE 1 = 1",
    );
    if let Some(after) = page.after() {
        push_member_key_lower_bound(&mut query_builder, after);
    }
    push_page_order_and_limit(
        &mut query_builder,
        page.limit(),
        "member_identity, key_fingerprint",
    );

    let rows = query_builder
        .build()
        .fetch_all(&mut *connection)
        .await
        .context(SqlxSnafu)?;
    let continuation_index = continuation_record_index(page.limit(), rows.len());
    let mut continuation = None;
    for (index, row) in rows.into_iter().enumerate() {
        let key_id = decode_member_key_id(&row)?;
        let page_continuation = if continuation_index == Some(index) {
            Some(MemberKeyPageContinuation::from_key_id(&key_id))
        } else {
            None
        };
        page.push(key_id)?;
        continuation = page_continuation;
    }
    finish_page(page, continuation)
}

pub(super) async fn load_member_public_keys_into(
    connection: &mut SqliteStoreConnection,
    mut page: PageAttempt<
        '_,
        MemberPublicKeyPredicate<'_>,
        MemberKeyPageContinuation,
        OwnedNoMetadataPageBatch<'_, MemberPublicKeysRecord>,
    >,
) -> Result<(), PageError> {
    let mut query_builder = QueryBuilder::<Sqlite>::new(
        "SELECT member_identity, key_fingerprint, signing_public_key, encryption_public_key \
         FROM member_public_keys WHERE ",
    );
    match page.params() {
        MemberPublicKeyPredicate::MemberEq(member_id) => {
            query_builder
                .push("member_identity = ")
                .push_bind(member_id.to_string());
        }
        MemberPublicKeyPredicate::FingerprintEq(fingerprint) => {
            query_builder
                .push("key_fingerprint = ")
                .push_bind(fingerprint.as_ref().to_vec());
        }
    }
    if let Some(after) = page.after() {
        push_member_key_lower_bound(&mut query_builder, after);
    }
    push_page_order_and_limit(
        &mut query_builder,
        page.limit(),
        "member_identity, key_fingerprint",
    );

    let rows = query_builder
        .build()
        .fetch_all(&mut *connection)
        .await
        .context(SqlxSnafu)?;
    let continuation_index = continuation_record_index(page.limit(), rows.len());
    let mut continuation = None;
    for (index, row) in rows.into_iter().enumerate() {
        let record = decode_member_public_keys_row(&row)?;
        let page_continuation = if continuation_index == Some(index) {
            Some(MemberKeyPageContinuation::from_key_id(&record.key_id))
        } else {
            None
        };
        page.push(record)?;
        continuation = page_continuation;
    }
    finish_page(page, continuation)
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

pub(super) async fn load_member_key_trust_evidence_into(
    connection: &mut SqliteStoreConnection,
    mut page: PageAttempt<
        '_,
        &MemberKeyId,
        SqliteTextPageContinuation,
        OwnedNoMetadataPageBatch<'_, MemberKeyTrustEvidenceKind>,
    >,
) -> Result<(), PageError> {
    let member_id = page.params().member_id.to_string();
    let fingerprint = page.params().fingerprint.as_ref().to_vec();
    let mut query_builder = QueryBuilder::<Sqlite>::new(
        "SELECT evidence_kind FROM member_key_trust_evidence WHERE member_identity = ",
    );
    query_builder
        .push_bind(member_id)
        .push(" AND key_fingerprint = ")
        .push_bind(fingerprint);
    push_text_page_window(&mut query_builder, &page, "evidence_kind");

    let evidence_kinds = query_builder
        .build_query_scalar::<String>()
        .fetch_all(&mut *connection)
        .await
        .context(SqlxSnafu)?;
    let continuation_index = continuation_record_index(page.limit(), evidence_kinds.len());
    let mut continuation = None;
    for (index, raw_evidence_kind) in evidence_kinds.into_iter().enumerate() {
        let evidence_kind = decode_member_key_trust_evidence_kind(&raw_evidence_kind)?;
        page.push(evidence_kind)?;
        if continuation_index == Some(index) {
            continuation = Some(SqliteTextPageContinuation::new(raw_evidence_kind));
        }
    }
    finish_page(page, continuation)
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

/// SQLite-owned continuation for its composite member-key ordering.
pub(super) struct MemberKeyPageContinuation {
    /// Canonical member identity text compared by SQLite.
    member_identity: String,
    /// Fingerprint compared after equal member identity text.
    fingerprint: KeyFingerprint,
}

impl MemberKeyPageContinuation {
    /// Capture the SQLite ordering values for one accepted member-key record.
    fn from_key_id(key_id: &MemberKeyId) -> Self {
        Self {
            member_identity: key_id.member_id.to_string(),
            fingerprint: key_id.fingerprint,
        }
    }
}

/// Decode the composite member-key identity selected by a collection query.
fn decode_member_key_id(row: &sqlx::sqlite::SqliteRow) -> Result<MemberKeyId, StoreError> {
    let raw_member_id = row.get::<String, _>("member_identity");
    let member_id = decode_member_identity(&raw_member_id)?;
    let raw_fingerprint = row.get::<Vec<u8>, _>("key_fingerprint");
    let fingerprint = decode_key_fingerprint(&raw_fingerprint)?;
    Ok(MemberKeyId {
        member_id,
        fingerprint,
    })
}

/// Decode one complete public-key record selected by a collection query.
fn decode_member_public_keys_row(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<MemberPublicKeysRecord, StoreError> {
    let key_id = decode_member_key_id(row)?;
    Ok(MemberPublicKeysRecord {
        key_id,
        signing_public_key: row
            .get::<Vec<u8>, _>("signing_public_key")
            .into_boxed_slice(),
        encryption_public_key: row
            .get::<Vec<u8>, _>("encryption_public_key")
            .into_boxed_slice(),
    })
}

/// Add the exclusive lower bound for SQLite's composite member-key order.
fn push_member_key_lower_bound(
    query_builder: &mut QueryBuilder<Sqlite>,
    after: &MemberKeyPageContinuation,
) {
    let member_id = after.member_identity.clone();
    query_builder
        .push(" AND (member_identity > ")
        .push_bind(member_id.clone())
        .push(" OR (member_identity = ")
        .push_bind(member_id)
        .push(" AND key_fingerprint > ")
        .push_bind(after.fingerprint.as_ref().to_vec())
        .push("))");
}
