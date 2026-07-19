//! SQLite persistence for pending group work.

use super::*;

pub(super) struct StoredPendingGroupPayload {
    state: PendingGroupWorkState,
    key: PendingGroupSqlKey,
    context: PendingGroupPayloadDecodeContext,
    payload: Vec<u8>,
}

/// Lifecycle state stored in the single target-group keyed work table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PendingGroupWorkState {
    /// Listener-mediated work that has not yet been accepted or rejected.
    AwaitingDecision,
    /// Accepted work that must activate without consulting the listener again.
    AcceptedActivation,
}

impl PendingGroupWorkState {
    /// Return the stable SQL representation for this lifecycle state.
    const fn as_sql(self) -> &'static str {
        match self {
            Self::AwaitingDecision => "decision",
            Self::AcceptedActivation => "activation",
        }
    }
}

impl TryFrom<String> for PendingGroupWorkState {
    type Error = PendingGroupSqlKeyError;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        match raw.as_str() {
            "decision" => Ok(Self::AwaitingDecision),
            "activation" => Ok(Self::AcceptedActivation),
            _ => Err(PendingGroupSqlKeyError::UnknownWorkState { raw }),
        }
    }
}

pub(super) async fn stored_pending_group_payload_from_row(
    connection: &mut SqliteStoreConnection,
    row: sqlx::sqlite::SqliteRow,
) -> Result<StoredPendingGroupPayload, StoreError> {
    let raw_state = row.get::<String, _>("state");
    let state = PendingGroupWorkState::try_from(raw_state)
        .map_err(|source| invalid_stored_object("pending group work state", source))?;
    let key = decode_pending_group_sql_key(&row)?;
    let context = pending_group_payload_decode_context(connection, &key).await?;
    Ok(StoredPendingGroupPayload {
        state,
        key,
        context,
        payload: row.get::<Vec<u8>, _>("payload"),
    })
}

pub(super) async fn load_pending_group_payloads(
    connection: &mut SqliteStoreConnection,
    state: PendingGroupWorkState,
) -> Result<Vec<StoredPendingGroupPayload>, StoreError> {
    let rows = sqlx::query(
        "
SELECT state, work_kind, old_group_id, new_group_id, payload
FROM pending_group_work
WHERE state = ?1
",
    )
    .bind(state.as_sql())
    .fetch_all(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    let mut payloads = Vec::with_capacity(rows.len());
    for row in rows {
        payloads.push(stored_pending_group_payload_from_row(connection, row).await?);
    }
    Ok(payloads)
}

pub(super) async fn load_pending_group_payload(
    connection: &mut SqliteStoreConnection,
    group_id: &GroupId,
    state: Option<PendingGroupWorkState>,
) -> Result<Option<StoredPendingGroupPayload>, StoreError> {
    let row = if let Some(state) = state {
        let query = sqlx::query(
            "
SELECT state, work_kind, old_group_id, new_group_id, payload
FROM pending_group_work
WHERE new_group_id = ?1 AND state = ?2
",
        )
        .bind(group_id.to_string())
        .bind(state.as_sql())
        .fetch_optional(&mut *connection);
        query.await.context(SqlxSnafu)?
    } else {
        let query = sqlx::query(
            "
SELECT state, work_kind, old_group_id, new_group_id, payload
FROM pending_group_work
WHERE new_group_id = ?1
",
        )
        .bind(group_id.to_string())
        .fetch_optional(&mut *connection);
        query.await.context(SqlxSnafu)?
    };
    match row {
        Some(row) => stored_pending_group_payload_from_row(connection, row)
            .await
            .map(Some),
        None => Ok(None),
    }
}

pub(super) async fn load_pending_group_decisions(
    connection: &mut SqliteStoreConnection,
) -> Result<Vec<PendingGroupDecisionRecord>, StoreError> {
    let payloads =
        load_pending_group_payloads(connection, PendingGroupWorkState::AwaitingDecision).await?;
    let mut records = Vec::with_capacity(payloads.len());
    for stored in payloads {
        let record = decode_stored_proto(
            "pending group decision",
            decode_pending_group_decision_payload(stored.key.kind, &stored.payload, stored.context),
        )?;
        validate_pending_group_payload_key(record.key(), stored.key)?;
        records.push(record);
    }
    Ok(records)
}

pub(super) async fn load_pending_group_decision(
    connection: &mut SqliteStoreConnection,
    group_id: &GroupId,
) -> Result<Option<PendingGroupDecisionRecord>, StoreError> {
    let stored = load_pending_group_payload(
        connection,
        group_id,
        Some(PendingGroupWorkState::AwaitingDecision),
    )
    .await?;
    let Some(stored) = stored else {
        return Ok(None);
    };
    let record = decode_stored_proto(
        "pending group decision",
        decode_pending_group_decision_payload(stored.key.kind, &stored.payload, stored.context),
    )?;
    validate_pending_group_payload_key(record.key(), stored.key)?;
    Ok(Some(record))
}

pub(super) async fn load_pending_group_activations(
    connection: &mut SqliteStoreConnection,
) -> Result<Vec<PendingGroupActivationRecord>, StoreError> {
    let payloads =
        load_pending_group_payloads(connection, PendingGroupWorkState::AcceptedActivation).await?;
    let mut records = Vec::with_capacity(payloads.len());
    for stored in payloads {
        let record = decode_stored_proto(
            "pending group activation",
            decode_pending_group_activation_payload(
                stored.key.kind,
                &stored.payload,
                stored.context,
            ),
        )?;
        validate_pending_group_payload_key(record.key(), stored.key)?;
        records.push(record);
    }
    Ok(records)
}

pub(super) async fn load_pending_group_activation(
    connection: &mut SqliteStoreConnection,
    group_id: &GroupId,
) -> Result<Option<PendingGroupActivationRecord>, StoreError> {
    let stored = load_pending_group_payload(
        connection,
        group_id,
        Some(PendingGroupWorkState::AcceptedActivation),
    )
    .await?;
    let Some(stored) = stored else {
        return Ok(None);
    };
    let record = decode_stored_proto(
        "pending group activation",
        decode_pending_group_activation_payload(stored.key.kind, &stored.payload, stored.context),
    )?;
    validate_pending_group_payload_key(record.key(), stored.key)?;
    Ok(Some(record))
}

pub(super) async fn upsert_pending_group_payload_row(
    connection: &mut SqliteStoreConnection,
    state: PendingGroupWorkState,
    key: PendingGroupSqlKey,
    payload: Vec<u8>,
) -> Result<(), StoreError> {
    if let Some(existing) = load_pending_group_payload(connection, &key.new_group_id, None).await? {
        ensure!(
            existing.key == key && existing.payload == payload,
            ConflictingPendingGroupWorkSnafu {
                group_id: key.new_group_id
            }
        );
        match (existing.state, state) {
            (
                PendingGroupWorkState::AwaitingDecision,
                PendingGroupWorkState::AcceptedActivation,
            ) => {
                sqlx::query(
                    "
UPDATE pending_group_work
SET state = 'activation'
WHERE new_group_id = ?1
",
                )
                .bind(key.new_group_id.to_string())
                .execute(&mut *connection)
                .await
                .context(SqlxSnafu)?;
            }
            (existing_state, requested_state) => ensure!(
                existing_state == requested_state,
                ConflictingPendingGroupWorkSnafu {
                    group_id: key.new_group_id
                }
            ),
        }
        return Ok(());
    }
    sqlx::query(
        "
INSERT INTO pending_group_work (new_group_id, state, work_kind, old_group_id, payload)
VALUES (?1, ?2, ?3, ?4, ?5)
",
    )
    .bind(key.new_group_id.to_string())
    .bind(state.as_sql())
    .bind(key.kind.as_sql())
    .bind(key.old_group_id.map(|group_id| group_id.to_string()))
    .bind(payload)
    .execute(&mut *connection)
    .await
    .context(SqlxSnafu)?;
    Ok(())
}

pub(super) async fn upsert_pending_group_decision(
    connection: &mut SqliteStoreConnection,
    record: &PendingGroupDecisionRecord,
) -> Result<(), StoreError> {
    let (kind, payload) = encode_pending_group_decision_payload(record);
    let key = PendingGroupSqlKey::from_work_key(record.key(), kind);
    upsert_pending_group_payload_row(
        connection,
        PendingGroupWorkState::AwaitingDecision,
        key,
        payload,
    )
    .await
}

pub(super) async fn remove_pending_group_decision(
    connection: &mut SqliteStoreConnection,
    key: PendingGroupWorkKey,
) -> Result<bool, StoreError> {
    let key = PendingGroupSqlKey::from_work_key_only(key);
    remove_pending_group_payload_row(connection, PendingGroupWorkState::AwaitingDecision, &key)
        .await
}

pub(super) async fn upsert_pending_group_activation(
    connection: &mut SqliteStoreConnection,
    record: &PendingGroupActivationRecord,
) -> Result<(), StoreError> {
    let (kind, payload) = encode_pending_group_activation_payload(record);
    let key = PendingGroupSqlKey::from_work_key(record.key(), kind);
    upsert_pending_group_payload_row(
        connection,
        PendingGroupWorkState::AcceptedActivation,
        key,
        payload,
    )
    .await
}

pub(super) async fn remove_pending_group_activation(
    connection: &mut SqliteStoreConnection,
    key: PendingGroupWorkKey,
) -> Result<bool, StoreError> {
    let key = PendingGroupSqlKey::from_work_key_only(key);
    remove_pending_group_payload_row(connection, PendingGroupWorkState::AcceptedActivation, &key)
        .await
}

pub(super) async fn remove_pending_group_payload_row(
    connection: &mut SqliteStoreConnection,
    state: PendingGroupWorkState,
    key: &PendingGroupSqlKey,
) -> Result<bool, StoreError> {
    let rows_affected = sqlx::query(
        "
DELETE FROM pending_group_work
WHERE new_group_id = ?1
  AND state = ?2
  AND work_kind = ?3
  AND old_group_id IS ?4
",
    )
    .bind(key.new_group_id.to_string())
    .bind(state.as_sql())
    .bind(key.kind.as_sql())
    .bind(key.old_group_id.map(|group_id| group_id.to_string()))
    .execute(&mut *connection)
    .await
    .context(SqlxSnafu)?
    .rows_affected();
    Ok(rows_affected > 0)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct PendingGroupSqlKey {
    /// Stored payload discriminator matching the concrete encoded payload type.
    kind: PendingGroupPayloadKind,
    /// Existing group that authorises migration work, absent for ordinary invitations.
    old_group_id: Option<GroupId>,
    /// Target group for invitations and proposals.
    new_group_id: GroupId,
}

impl PendingGroupSqlKey {
    /// Convert a runtime work key into the SQL key and assert the encoded payload kind.
    fn from_work_key(key: PendingGroupWorkKey, kind: PendingGroupPayloadKind) -> Self {
        let sql_key = Self::from_work_key_only(key);
        debug_assert_eq!(sql_key.kind, kind);
        sql_key
    }

    /// Convert a runtime work key into the multi-column SQL key shape.
    fn from_work_key_only(key: PendingGroupWorkKey) -> Self {
        match key {
            PendingGroupWorkKey::GroupInvitation { group_id, source } => match source {
                crate::api::GroupInvitationSource::Creation => Self {
                    kind: PendingGroupPayloadKind::GroupInvitation,
                    old_group_id: None,
                    new_group_id: group_id,
                },
                crate::api::GroupInvitationSource::Migration { migration_id } => Self {
                    kind: PendingGroupPayloadKind::GroupInvitation,
                    old_group_id: Some(migration_id.old_group_id),
                    new_group_id: migration_id.new_group_id,
                },
            },
            PendingGroupWorkKey::MigrationProposal { migration_id } => Self {
                kind: PendingGroupPayloadKind::MigrationProposal,
                old_group_id: Some(migration_id.old_group_id),
                new_group_id: migration_id.new_group_id,
            },
        }
    }
}

impl PendingGroupPayloadKind {
    /// Return the stable SQL discriminator stored beside pending group payloads.
    fn as_sql(self) -> &'static str {
        match self {
            Self::GroupInvitation => "group_invitation",
            Self::MigrationProposal => "migration_proposal",
        }
    }
}

#[derive(Debug, Snafu)]
pub(super) enum PendingGroupSqlKeyError {
    /// Stored `work_kind` did not name any supported pending group payload.
    #[snafu(display("Unknown pending group work kind '{raw}'."))]
    UnknownWorkKind { raw: String },
    /// Stored lifecycle state did not name a supported pending-work state.
    #[snafu(display("Unknown pending group work state '{raw}'."))]
    UnknownWorkState { raw: String },
    /// Migration proposals must reference the existing group that authorised them.
    #[snafu(display("Pending migration proposal key did not contain old_group_id."))]
    MissingMigrationProposalOldGroupId,
    /// The decoded payload described different pending work from the SQL key.
    #[snafu(display("Pending payload key {payload_key:?} did not match SQL key {sql_key:?}."))]
    PayloadKeyMismatch {
        payload_key: PendingGroupSqlKey,
        sql_key: PendingGroupSqlKey,
    },
}

/// Decode one pending group SQL row key from its discriminator and group columns.
pub(super) fn decode_pending_group_sql_key(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<PendingGroupSqlKey, StoreError> {
    let raw_work_kind = row.get::<String, _>("work_kind");
    let kind = match raw_work_kind.as_str() {
        "group_invitation" => PendingGroupPayloadKind::GroupInvitation,
        "migration_proposal" => PendingGroupPayloadKind::MigrationProposal,
        _ => {
            return Err(invalid_stored_object(
                "pending group sql key",
                PendingGroupSqlKeyError::UnknownWorkKind { raw: raw_work_kind },
            ));
        }
    };
    let raw_old_group_id = row.get::<Option<String>, _>("old_group_id");
    let old_group_id = raw_old_group_id
        .as_deref()
        .map(decode_group_id)
        .transpose()?;
    let new_group_id = decode_group_id(&row.get::<String, _>("new_group_id"))?;
    if kind == PendingGroupPayloadKind::MigrationProposal && old_group_id.is_none() {
        return Err(invalid_stored_object(
            "pending group sql key",
            PendingGroupSqlKeyError::MissingMigrationProposalOldGroupId,
        ));
    }
    Ok(PendingGroupSqlKey {
        kind,
        old_group_id,
        new_group_id,
    })
}

/// Build the payload decode context available from the pending group SQL key.
pub(super) async fn pending_group_payload_decode_context(
    connection: &mut SqliteStoreConnection,
    key: &PendingGroupSqlKey,
) -> Result<PendingGroupPayloadDecodeContext, StoreError> {
    let old_group_member_count = match key.old_group_id {
        Some(group_id) => load_optional_group_member_count(connection, &group_id).await?,
        None => None,
    };
    Ok(PendingGroupPayloadDecodeContext {
        old_group_member_count,
    })
}

/// Validate that decoded pending work still matches the row key it was stored under.
pub(super) fn validate_pending_group_payload_key(
    payload_key: PendingGroupWorkKey,
    sql_key: PendingGroupSqlKey,
) -> Result<(), StoreError> {
    let payload_key = PendingGroupSqlKey::from_work_key_only(payload_key);
    if payload_key == sql_key {
        return Ok(());
    }
    Err(invalid_stored_object(
        "pending group sql key",
        PendingGroupSqlKeyError::PayloadKeyMismatch {
            payload_key,
            sql_key,
        },
    ))
}
