//! SQLite store tests.
use super::*;
use crate::{
    MAX_VERSION_VALUE,
    api::{
        DatasetRowPageBatch,
        DatasetRowStatePatch,
        DatasetRowStateSlice,
        DatasetRowStateWrite,
        DatasetRowTransitionPageBatch,
        DatasetRowTransitionQuery,
        DatasetRowsQuery,
        DatasetUpdateRecord,
        GroupDatasetSchemaRef,
        GroupInvitation,
        GroupSchema,
        InitialDatasetValueRows,
        InitialGroupValueRows,
        InitialSnapshot,
        InitialSnapshotMetadata,
        InitialValueRow,
        InlinePageBatch,
        MemberKeyTrustEvidenceKind,
        MemberKeyTrustEvidenceRecord,
        MemberPublicKeysRecord,
        MigrationId,
        MigrationProposal,
        PendingGroupDecisionRecord,
        ReplicationUpdateFilter,
        ReplicationUpdatePageInput,
        ReplicationUpdateView,
        ReplicationUpdatesQuery,
        RequestedDatasetRowPageBatch,
        RequestedDatasetRowView,
        RequestedDatasetRowsQuery,
        SnapshotRef,
        StoreErrorClass,
        StoreErrorClassificationSource as _,
        VecPageBatch,
        current_slice_placeholder_group_security_material,
    },
    delivery::shared::MessageId,
    provision_local_identity,
    test_support::{
        MetadataPagingFixtures,
        SqliteStoreTestOwner,
        assert_metadata_paging_contract,
        test_public_member_keys,
        test_replication_security_secrets,
    },
};
use bytes::Bytes;
use flotsync_core::member::{IdentifierParseError, MAX_IDENTIFIER_SEGMENTS};
use flotsync_data_types::{
    Field,
    RowValues,
    Schema,
    TableOperations,
    schema::datamodel::RowOperation,
};
use flotsync_messages::codecs::datamodel::encode_schema_operation;
use flotsync_utils::BoxError;
use futures_util::future;
use itertools::Itertools;
use std::{
    assert_matches,
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, SystemTime},
};

/// Owned row fixture used to prepare and inspect stored snapshots.
#[derive(Clone, Debug, PartialEq)]
struct ReplicationRowStateFixture {
    /// Stable row key.
    row_id: RowKey,
    /// Complete state snapshot.
    snapshot: ReplicationRowStateSnapshot,
    /// Whether the row is retained as a tombstone.
    tombstoned: bool,
    /// Update which created the row, when known.
    created_by: Option<UpdateId>,
    /// Causal frontier of the last state change.
    last_changed_versions: VersionVector,
}

/// Small retained projection used to prove update pages need not own payloads.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ProjectedUpdateSummary {
    /// Selected update identity.
    update_id: UpdateId,
    /// Dataset identifier copied from the temporary payload view.
    dataset_id: String,
    /// Number of borrowed operations observed without materialising them.
    operation_count: usize,
    /// Whether this update is already reflected in local state.
    applied_locally: bool,
}

const STORE_FUTURE_TIMEOUT: Duration = Duration::from_secs(5);

fn wait_for_store_future<F>(future: F) -> F::Output
where
    F: std::future::Future,
{
    flotsync_io::test_support::wait_for_future(
        STORE_FUTURE_TIMEOUT,
        future,
        "timed out waiting for sqlite store future",
    )
}

/// Retain only the fields needed by the projected-update paging scenario.
#[allow(
    clippy::needless_pass_by_value,
    clippy::unnecessary_wraps,
    reason = "page projections share the reusable fallible batch callback signature"
)]
fn project_update_summary(
    update: ReplicationUpdateView<'_>,
) -> Result<ProjectedUpdateSummary, BoxError> {
    let mut datasets = update.dataset_updates();
    let dataset = datasets
        .next()
        .expect("test updates should contain one dataset");
    assert!(
        datasets.next().is_none(),
        "test updates should contain exactly one dataset"
    );
    Ok(ProjectedUpdateSummary {
        update_id: update.update_id(),
        dataset_id: dataset.dataset_id().to_owned(),
        operation_count: dataset.operations().count(),
        applied_locally: update.applied_locally(),
    })
}

/// Materialise one loaded positional row for assertions which compare owned fixtures.
fn loaded_row_fixture(
    slice: &DatasetRowStateSlice,
    row_key: RowKey,
) -> Option<ReplicationRowStateFixture> {
    let row = slice
        .state_rows
        .rows()
        .find(|row| row.metadata().row_key == row_key)?;
    let metadata = row.metadata();
    Some(ReplicationRowStateFixture {
        row_id: metadata.row_key,
        snapshot: row.snapshot().into_owned(),
        tombstoned: metadata.tombstoned,
        created_by: metadata.created_by,
        last_changed_versions: metadata.last_changed_versions.clone(),
    })
}

/// Verify requested-row outcomes and empty selections against a missing dataset.
fn assert_requested_rows_for_missing_dataset(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    group_id: GroupId,
    missing_dataset_id: &DatasetId,
    schema: &Schema,
    first_missing_row_key: RowKey,
) {
    let second_missing_row_key = RowKey(Uuid::from_u128(1_207));
    let mut expected_missing_row_keys = vec![first_missing_row_key, second_missing_row_key];
    expected_missing_row_keys.sort_unstable();
    let mut requested_rows = RequestedDatasetRowPageBatch::unlimited(schema);
    let mut requested_missing_cursor = PageCursor::new(RequestedDatasetRowsQuery::new(
        DatasetRowsQuery::borrowed(GroupDatasetSchemaRef {
            group_id: &group_id,
            dataset_id: missing_dataset_id,
            schema,
        }),
        [second_missing_row_key, first_missing_row_key],
    ));
    wait_for_store_future(
        transaction.load_dataset_rows_into(&mut requested_missing_cursor, &mut requested_rows),
    )
    .expect("requested rows from a missing dataset should load");
    let loaded_missing_row_keys = requested_rows
        .outcomes()
        .map(|outcome| match outcome {
            RequestedDatasetRowView::Missing(row_key) => row_key,
            RequestedDatasetRowView::Present(_) => {
                panic!("a missing dataset cannot contain requested rows")
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(loaded_missing_row_keys, expected_missing_row_keys);
    assert!(
        !requested_rows
            .metadata()
            .expect("requested missing-dataset page should retain metadata")
            .dataset_exists
    );
    assert!(requested_missing_cursor.is_exhausted());

    let mut empty_request_cursor = PageCursor::new(RequestedDatasetRowsQuery::new(
        DatasetRowsQuery::borrowed(GroupDatasetSchemaRef {
            group_id: &group_id,
            dataset_id: missing_dataset_id,
            schema,
        }),
        [],
    ));
    wait_for_store_future(
        transaction.load_dataset_rows_into(&mut empty_request_cursor, &mut requested_rows),
    )
    .expect("an empty row selection should load");
    assert!(requested_rows.outcomes().next().is_none());
    assert!(
        !requested_rows
            .metadata()
            .expect("empty requested-row page should retain metadata")
            .dataset_exists
    );
    assert!(empty_request_cursor.is_exhausted());
}

async fn apply_row_patch(
    transaction: &mut dyn ReplicationStoreTransaction,
    schema: &Schema,
    patch: DatasetRowStatePatch,
) -> Result<(), StoreError> {
    let dataset = GroupDatasetSchemaRef {
        group_id: &patch.group_id,
        dataset_id: &patch.dataset_id,
        schema,
    };
    transaction.apply_dataset_row_patch(dataset, &patch).await
}

/// Replace persisted creator columns, bypassing their consistency check for corruption tests.
fn replace_raw_row_creator(
    store: &SqliteReplicationStore,
    group_id: GroupId,
    dataset_id: &DatasetId,
    row_key: RowKey,
    node_index: Option<i64>,
    version: Option<i64>,
) {
    wait_for_store_future(async {
        let mut connection = store.pool.connections.acquire().await.context(SqlxSnafu)?;
        sqlx::query("PRAGMA ignore_check_constraints = ON")
            .execute(&mut *connection)
            .await
            .context(SqlxSnafu)?;
        let update_result = sqlx::query(
            "
UPDATE dataset_rows
SET row_created_by_node_index = ?1,
    row_created_by_version = ?2
WHERE group_id = ?3 AND dataset_id = ?4 AND row_key = ?5
",
        )
        .bind(node_index)
        .bind(version)
        .bind(group_id.to_string())
        .bind(dataset_id.as_str())
        .bind(row_key.to_string())
        .execute(&mut *connection)
        .await
        .context(SqlxSnafu);
        let reset_result = sqlx::query("PRAGMA ignore_check_constraints = OFF")
            .execute(&mut *connection)
            .await
            .context(SqlxSnafu);
        update_result?;
        reset_result?;
        Ok::<_, StoreError>(())
    })
    .expect("raw row creator should update");
}

/// Replace one persisted row snapshot with arbitrary bytes for corruption tests.
fn replace_raw_row_snapshot(
    store: &SqliteReplicationStore,
    group_id: GroupId,
    dataset_id: &DatasetId,
    row_key: RowKey,
    snapshot: Vec<u8>,
) {
    wait_for_store_future(async {
        let mut connection = store.pool.connections.acquire().await.context(SqlxSnafu)?;
        sqlx::query(
            "
UPDATE dataset_rows
SET row_snapshot = ?1
WHERE group_id = ?2 AND dataset_id = ?3 AND row_key = ?4
",
        )
        .bind(snapshot)
        .bind(group_id.to_string())
        .bind(dataset_id.as_str())
        .bind(row_key.to_string())
        .execute(&mut *connection)
        .await
        .context(SqlxSnafu)?;
        Ok::<_, StoreError>(())
    })
    .expect("raw row snapshot should update");
}

/// Replace one stored update payload while preserving its indexed identity.
fn replace_raw_update_message(
    store: &SqliteReplicationStore,
    group_id: GroupId,
    update_id: UpdateId,
    update_message: Vec<u8>,
) {
    wait_for_store_future(async {
        let mut connection = store.pool.connections.acquire().await.context(SqlxSnafu)?;
        sqlx::query(
            "
UPDATE dataset_updates
SET update_message = ?1
WHERE group_id = ?2 AND update_node_index = ?3 AND update_version = ?4
",
        )
        .bind(update_message)
        .bind(group_id.to_string())
        .bind(i64::from(update_id.node_index))
        .bind(encode_update_version_sort_key_vec(update_id.version))
        .execute(&mut *connection)
        .await
        .context(SqlxSnafu)?;
        Ok::<_, StoreError>(())
    })
    .expect("raw update message should update");
}

type TestSqliteStore = SqliteStoreTestOwner<Arc<SqliteReplicationStore>>;

fn in_memory_store(local_member: MemberIdentity) -> TestSqliteStore {
    let provisioner = wait_for_store_future(SqliteReplicationStoreProvisioner::in_memory())
        .expect("provisioner should build");
    wait_for_store_future(provision_local_identity(
        &provisioner,
        local_member,
        &test_replication_security_secrets(),
    ))
    .expect("identity should provision");
    let store = wait_for_store_future(provisioner.into_replication_store())
        .expect("provisioned store should activate");
    SqliteStoreTestOwner::from_store(Arc::new(store))
}

fn in_memory_provisioner() -> SqliteReplicationStoreProvisioner {
    wait_for_store_future(SqliteReplicationStoreProvisioner::in_memory())
        .expect("provisioner should build")
}

fn docs_dataset_id() -> DatasetId {
    DatasetId::try_from_static("docs").expect("dataset id should build")
}

fn local_member() -> MemberIdentity {
    MemberIdentity::from_array(["app", "alice"])
}

fn remote_member() -> MemberIdentity {
    MemberIdentity::from_array(["app", "bob"])
}

fn third_member() -> MemberIdentity {
    MemberIdentity::from_array(["app", "carol"])
}

#[test]
fn sqlite_transactions_have_distinct_stable_identities() {
    let first_store = in_memory_store(local_member());
    let second_store = in_memory_store(local_member());
    wait_for_store_future(async {
        let first = first_store
            .begin_read_transaction()
            .await
            .expect("first transaction should start");
        let second = first_store
            .begin_read_transaction()
            .await
            .expect("second transaction should start");
        let other_pool = second_store
            .begin_read_transaction()
            .await
            .expect("transaction from second pool should start");

        assert_eq!(first.transaction_id(), first.transaction_id());
        assert_ne!(first.transaction_id(), second.transaction_id());
        assert_ne!(first.transaction_id(), other_pool.transaction_id());
        assert_ne!(second.transaction_id(), other_pool.transaction_id());

        first
            .release()
            .await
            .expect("first transaction should release");
        second
            .release()
            .await
            .expect("second transaction should release");
        other_pool
            .release()
            .await
            .expect("transaction from second pool should release");
    });
}

#[test]
fn sqlite_store_close_is_idempotent_and_rejects_new_operations() {
    let store = in_memory_store(local_member());

    wait_for_store_future(store.close()).expect("store should close");
    wait_for_store_future(store.close()).expect("completed close should be idempotent");

    let identity_error = wait_for_store_future(store.local_member_identity())
        .expect_err("cached identity access should be rejected after close");
    assert_eq!(
        identity_error.classification().class,
        StoreErrorClass::Unavailable
    );
    let Err(transaction_error) = wait_for_store_future(store.begin_read_transaction()) else {
        panic!("new transactions should be rejected after close");
    };
    assert_eq!(
        transaction_error.classification().class,
        StoreErrorClass::Unavailable
    );
}

#[test]
fn concurrent_sqlite_store_close_callers_wait_for_completed_closure() {
    let store = in_memory_store(local_member());
    wait_for_store_future(async {
        let transaction = store
            .begin_read_transaction()
            .await
            .expect("test transaction should start");
        let mut first_close = std::pin::pin!(store.close());
        future::poll_fn(
            |context| match std::future::Future::poll(first_close.as_mut(), context) {
                std::task::Poll::Pending => std::task::Poll::Ready(()),
                std::task::Poll::Ready(result) => {
                    panic!("first close should wait for the open transaction: {result:?}")
                }
            },
        )
        .await;

        let mut second_close = std::pin::pin!(store.close());
        future::poll_fn(|context| {
            match std::future::Future::poll(second_close.as_mut(), context) {
                std::task::Poll::Pending => std::task::Poll::Ready(()),
                std::task::Poll::Ready(result) => {
                    panic!("second close should wait for the open transaction: {result:?}")
                }
            }
        })
        .await;

        transaction
            .release()
            .await
            .expect("test transaction should release");
        first_close.await.expect("first close should complete");
        second_close.await.expect("second close should complete");
    });
}

#[test]
fn cancelled_sqlite_store_close_can_be_completed_by_a_later_caller() {
    let store = in_memory_store(local_member());
    wait_for_store_future(async {
        let transaction = store
            .begin_read_transaction()
            .await
            .expect("test transaction should start");
        let mut first_close = Box::pin(store.close());
        future::poll_fn(
            |context| match std::future::Future::poll(first_close.as_mut(), context) {
                std::task::Poll::Pending => std::task::Poll::Ready(()),
                std::task::Poll::Ready(result) => {
                    panic!("first close should wait for the open transaction: {result:?}")
                }
            },
        )
        .await;
        assert_eq!(store.pool.state(), SqliteStoreState::Closing);
        drop(first_close);

        transaction
            .release()
            .await
            .expect("test transaction should release");
        store
            .close()
            .await
            .expect("later close caller should complete closure");
        assert_eq!(store.pool.state(), SqliteStoreState::Closed);
    });
}

#[test]
fn sqlite_operation_futures_check_lifecycle_when_polled() {
    let store = in_memory_store(local_member());
    wait_for_store_future(async {
        let cached_identity = store.local_member_identity();
        let new_transaction = store.begin_read_transaction();
        let blocking_transaction = store
            .begin_read_transaction()
            .await
            .expect("blocking transaction should start");
        let mut close = std::pin::pin!(store.close());
        future::poll_fn(
            |context| match std::future::Future::poll(close.as_mut(), context) {
                std::task::Poll::Pending => std::task::Poll::Ready(()),
                std::task::Poll::Ready(result) => {
                    panic!("close should wait for the blocking transaction: {result:?}")
                }
            },
        )
        .await;

        let identity_error = cached_identity
            .await
            .expect_err("cached access should observe closure when polled");
        assert_eq!(
            identity_error.classification().class,
            StoreErrorClass::Unavailable
        );
        let Err(transaction_error) = new_transaction.await else {
            panic!("transaction future should observe closure when polled");
        };
        assert_eq!(
            transaction_error.classification().class,
            StoreErrorClass::Unavailable
        );

        blocking_transaction
            .release()
            .await
            .expect("blocking transaction should release");
        close.await.expect("store should close");
    });
}

#[test]
fn sqlite_store_provisioner_close_is_idempotent_and_rejects_new_operations() {
    let provisioner = in_memory_provisioner();

    wait_for_store_future(provisioner.close()).expect("provisioner should close");
    wait_for_store_future(provisioner.close())
        .expect("completed provisioner close should be idempotent");

    let error = wait_for_store_future(provisioner.local_member_identity())
        .expect_err("provisioning operations should be rejected after close");
    assert_eq!(error.classification().class, StoreErrorClass::Unavailable);
}

#[test]
fn dropping_an_open_activated_sqlite_store_panics_in_debug_builds() {
    if !cfg!(debug_assertions) {
        return;
    }
    let result = std::panic::catch_unwind(|| {
        let provisioner = in_memory_provisioner();
        wait_for_store_future(provision_local_identity(
            &provisioner,
            local_member(),
            &test_replication_security_secrets(),
        ))
        .expect("identity should provision");
        let store = wait_for_store_future(provisioner.into_replication_store())
            .expect("provisioned store should activate");
        drop(store);
    });

    assert!(result.is_err(), "dropping an open store should panic");
}

#[test]
fn reliable_delivery_store_round_trips_metadata_and_encoded_envelopes() {
    let store = in_memory_store(local_member());
    let earlier = StoredReliableDeliveryWork {
        metadata: StoredReliableDeliveryWorkMetadata {
            message_id: MessageId(Uuid::from_u128(701)),
            recipient: remote_member(),
            first_submitted_at: SystemTime::UNIX_EPOCH + Duration::from_secs(10),
        },
        encoded_envelope: Bytes::from_static(b"earlier encoded envelope"),
    };
    let later = StoredReliableDeliveryWork {
        metadata: StoredReliableDeliveryWorkMetadata {
            message_id: MessageId(Uuid::from_u128(702)),
            recipient: third_member(),
            first_submitted_at: SystemTime::UNIX_EPOCH + Duration::from_secs(20),
        },
        encoded_envelope: Bytes::from_static(b"later encoded envelope"),
    };

    wait_for_store_future(store.store_reliable_delivery_work(later.clone()))
        .expect("later work should store");
    wait_for_store_future(store.store_reliable_delivery_work(later.clone()))
        .expect("repeating the same reliable work should store idempotently");
    wait_for_store_future(store.store_reliable_delivery_work(earlier.clone()))
        .expect("earlier work should store");

    assert_delivery_metadata_pages(store.as_ref(), &earlier.metadata, &later.metadata);

    let mut metadata = wait_for_store_future(store.load_reliable_delivery_work_metadata())
        .expect("metadata should load");
    metadata.sort_by_key(|item| (item.first_submitted_at, item.message_id));
    assert_eq!(
        metadata,
        vec![earlier.metadata.clone(), later.metadata.clone()]
    );
    assert_eq!(
        wait_for_store_future(store.load_reliable_delivery_work(later.metadata.message_id))
            .expect("full work should load"),
        Some(later.clone())
    );
    assert!(
        wait_for_store_future(store.remove_reliable_delivery_work(earlier.metadata.message_id))
            .expect("stored work should remove")
    );
    assert!(
        !wait_for_store_future(store.remove_reliable_delivery_work(earlier.metadata.message_id))
            .expect("repeated removal should be a no-op")
    );
    assert_eq!(
        wait_for_store_future(store.load_reliable_delivery_work_metadata())
            .expect("remaining metadata should load"),
        vec![later.metadata.clone()]
    );
    wait_for_store_future(store.remove_reliable_delivery_work(later.metadata.message_id))
        .expect("final work should remove");
    assert_empty_delivery_metadata_page(store.as_ref());
}

/// Check bounded continuation, session binding, and exact-size exhaustion.
fn assert_delivery_metadata_pages(
    store: &SqliteReplicationStore,
    earlier: &StoredReliableDeliveryWorkMetadata,
    later: &StoredReliableDeliveryWorkMetadata,
) {
    let mut session = wait_for_store_future(store.begin_read_session())
        .expect("delivery metadata session should start");
    let mut cursor = PageCursor::new(());
    let mut batch = VecPageBatch::<StoredReliableDeliveryWorkMetadata, ()>::bounded(
        NonZeroUsize::new(1).expect("page size is non-zero"),
    );
    load_delivery_metadata_page(session.as_mut(), &mut cursor, &mut batch)
        .expect("first metadata page should load");
    assert_eq!(batch.values(), std::slice::from_ref(earlier));
    let mut other_session = wait_for_store_future(store.begin_read_session())
        .expect("second delivery metadata session should start");
    assert!(matches!(
        load_delivery_metadata_page(other_session.as_mut(), &mut cursor, &mut batch),
        Err(PageError::TransactionMismatch { .. })
    ));
    wait_for_store_future(other_session.release()).expect("second session should release");
    load_delivery_metadata_page(session.as_mut(), &mut cursor, &mut batch)
        .expect("second metadata page should load");
    assert_eq!(batch.values(), std::slice::from_ref(later));
    assert!(cursor.has_more(), "an exact-size page needs confirmation");
    load_delivery_metadata_page(session.as_mut(), &mut cursor, &mut batch)
        .expect("empty confirmation page should load");
    assert!(batch.values().is_empty());
    assert!(cursor.is_exhausted());
    wait_for_store_future(session.release()).expect("metadata session should release");
}

/// Check that an empty read session exhausts the cursor in one fill.
fn assert_empty_delivery_metadata_page(store: &SqliteReplicationStore) {
    let mut empty_session = wait_for_store_future(store.begin_read_session())
        .expect("empty delivery metadata session should start");
    let mut empty_cursor = PageCursor::new(());
    let mut batch = VecPageBatch::<StoredReliableDeliveryWorkMetadata, ()>::bounded(
        NonZeroUsize::new(1).expect("page size is non-zero"),
    );
    load_delivery_metadata_page(empty_session.as_mut(), &mut empty_cursor, &mut batch)
        .expect("empty metadata page should load");
    assert!(batch.values().is_empty());
    assert!(empty_cursor.is_exhausted());
    wait_for_store_future(empty_session.release()).expect("empty session should release");
}

#[test]
fn reliable_delivery_metadata_pages_keep_one_read_view() {
    let path = std::env::temp_dir().join(format!(
        "flotsync-delivery-paging-{}.sqlite",
        Uuid::new_v4()
    ));
    let provisioner = wait_for_store_future(SqliteReplicationStoreProvisioner::create_file(&path))
        .expect("file provisioner should build");
    wait_for_store_future(async {
        let mut connection = provisioner.pool.connections.acquire().await?;
        sqlx::query("PRAGMA journal_mode = WAL")
            .execute(&mut *connection)
            .await?;
        Ok::<_, sqlx::Error>(())
    })
    .expect("file store should allow concurrent readers and writers");
    wait_for_store_future(provision_local_identity(
        &provisioner,
        local_member(),
        &test_replication_security_secrets(),
    ))
    .expect("identity should provision");
    let store = wait_for_store_future(provisioner.into_replication_store())
        .expect("file store should activate");
    let store = SqliteStoreTestOwner::from_store(Arc::new(store));
    let first = reliable_delivery_work(801);
    let removed = reliable_delivery_work(803);
    let inserted = reliable_delivery_work(802);
    wait_for_store_future(store.store_reliable_delivery_work(first.clone()))
        .expect("first work should store");
    wait_for_store_future(store.store_reliable_delivery_work(removed.clone()))
        .expect("later work should store");

    let mut session = wait_for_store_future(store.begin_read_session())
        .expect("metadata read session should start");
    let mut cursor = PageCursor::new(());
    let mut batch = VecPageBatch::<StoredReliableDeliveryWorkMetadata, ()>::bounded(
        NonZeroUsize::new(1).expect("page size is non-zero"),
    );
    load_delivery_metadata_page(session.as_mut(), &mut cursor, &mut batch)
        .expect("first metadata page should load");
    assert_eq!(batch.values(), std::slice::from_ref(&first.metadata));

    wait_for_store_future(store.store_reliable_delivery_work(inserted.clone()))
        .expect("new work should store during the read session");
    wait_for_store_future(store.remove_reliable_delivery_work(removed.metadata.message_id))
        .expect("old work should remove during the read session");
    load_delivery_metadata_page(session.as_mut(), &mut cursor, &mut batch)
        .expect("second metadata page should load from the original read view");
    assert_eq!(batch.values(), &[removed.metadata]);
    load_delivery_metadata_page(session.as_mut(), &mut cursor, &mut batch)
        .expect("empty page should confirm the end of the original read view");
    assert!(cursor.is_exhausted());
    wait_for_store_future(session.release()).expect("metadata read session should release");

    let mut current = wait_for_store_future(store.load_reliable_delivery_work_metadata())
        .expect("new read view should load");
    current.sort_by_key(|metadata| metadata.message_id);
    assert_eq!(current, vec![first.metadata, inserted.metadata]);
    drop(store);
    std::fs::remove_file(path).expect("test database should be removed");
}

#[test]
fn failed_reliable_delivery_metadata_page_clears_output_and_releases_on_drop() {
    let store = in_memory_store(local_member());
    let valid = reliable_delivery_work(811);
    wait_for_store_future(store.store_reliable_delivery_work(valid.clone()))
        .expect("valid work should store");
    let invalid_id = Uuid::from_u128(812).to_string();
    wait_for_store_future(async {
        let mut connection = store.pool.connections.acquire().await?;
        sqlx::query(
            "INSERT INTO reliable_delivery_work (message_id, recipient, first_submitted_at, encoded_envelope) VALUES (?1, ?2, ?3, ?4)",
        )
        .bind(&invalid_id)
        .bind("bad!")
        .bind("1970-01-01T00:00:00Z")
        .bind(b"invalid".as_slice())
        .execute(&mut *connection)
        .await?;
        Ok::<_, sqlx::Error>(())
    })
    .expect("malformed metadata fixture should store");

    let mut session = wait_for_store_future(store.begin_read_session())
        .expect("metadata read session should start");
    let mut cursor = PageCursor::new(());
    let mut batch = VecPageBatch::<StoredReliableDeliveryWorkMetadata, ()>::bounded(
        NonZeroUsize::new(2).expect("page size is non-zero"),
    );
    assert!(matches!(
        load_delivery_metadata_page(session.as_mut(), &mut cursor, &mut batch),
        Err(PageError::Store { .. })
    ));
    assert!(batch.values().is_empty());
    assert!(cursor.is_failed());
    drop(session);

    wait_for_store_future(async {
        let mut connection = store.pool.connections.acquire().await?;
        sqlx::query("DELETE FROM reliable_delivery_work WHERE message_id = ?1")
            .bind(&invalid_id)
            .execute(&mut *connection)
            .await?;
        Ok::<_, sqlx::Error>(())
    })
    .expect("dropped failed session should release the database read view");
    assert_eq!(
        wait_for_store_future(store.load_reliable_delivery_work_metadata())
            .expect("valid metadata should load after failed session cleanup"),
        vec![valid.metadata]
    );
}

/// Build one stored sender-work record for metadata page tests.
fn reliable_delivery_work(message_id: u128) -> StoredReliableDeliveryWork {
    StoredReliableDeliveryWork {
        metadata: StoredReliableDeliveryWorkMetadata {
            message_id: MessageId(Uuid::from_u128(message_id)),
            recipient: remote_member(),
            first_submitted_at: SystemTime::UNIX_EPOCH + Duration::from_secs(10),
        },
        encoded_envelope: Bytes::from_static(b"stored envelope"),
    }
}

/// Drive one metadata page through the same public session boundary as callers.
fn load_delivery_metadata_page(
    session: &mut dyn ReliableDeliveryReadSession,
    cursor: &mut PageCursor<()>,
    batch: &mut VecPageBatch<StoredReliableDeliveryWorkMetadata, ()>,
) -> Result<(), PageError> {
    wait_for_store_future(session.load_reliable_delivery_work_metadata_into(cursor, batch))
}

fn insert_raw_local_member(
    provisioner: &SqliteReplicationStoreProvisioner,
    member_identity: &str,
    secret_byte: u8,
) {
    wait_for_store_future(async {
        let mut connection = provisioner
            .pool
            .connections
            .acquire()
            .await
            .context(SqlxSnafu)?;
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
        .bind(member_identity)
        .bind(1_i64)
        .bind(StoreSecretKeyId::from_u128_for_test(1).to_string())
        .bind(vec![0_u8; 24])
        .bind(vec![secret_byte])
        .execute(&mut *connection)
        .await
        .context(SqlxSnafu)?;
        Ok::<_, StoreError>(())
    })
    .expect("raw local member should insert");
}

#[test]
fn sqlite_store_activation_requires_one_provisioned_identity() {
    let provisioner = in_memory_provisioner();

    let result = wait_for_store_future(provisioner.into_replication_store());
    let Err(error) = result else {
        panic!("empty provisioner should not activate");
    };
    assert!(matches!(
        error,
        StoreError::StoreExternal { ref source, .. }
            if matches!(
                source.downcast_ref::<SqliteStoreError>(),
                Some(SqliteStoreError::MissingLocalMemberIdentity)
            )
    ));
}

#[test]
fn rolled_back_identity_material_remains_unprovisioned() {
    let provisioner = in_memory_provisioner();
    let mut transaction =
        wait_for_store_future(provisioner.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.ensure_local_member_private_keys(
        LocalMemberPrivateKeysRecord {
            member_id: local_member(),
            private_keys: EncryptedLocalMemberPrivateKeys {
                secret: sample_encrypted_secret(41),
            },
        },
    ))
    .expect("private keys should be staged");
    wait_for_store_future(transaction.rollback()).expect("transaction should roll back");

    let stored_member = wait_for_store_future(provisioner.local_member_identity())
        .expect("identity lookup should succeed");
    assert_eq!(stored_member, None);
}

#[test]
fn sqlite_store_activation_rejects_several_distinct_local_identities() {
    let provisioner = in_memory_provisioner();
    insert_raw_local_member(&provisioner, &local_member().to_string(), 41);
    insert_raw_local_member(&provisioner, &remote_member().to_string(), 42);

    let result = wait_for_store_future(provisioner.into_replication_store());
    let Err(error) = result else {
        panic!("ambiguous provisioner should not activate");
    };
    assert!(matches!(
        error,
        StoreError::StoreExternal { ref source, .. }
            if matches!(
                source.downcast_ref::<SqliteStoreError>(),
                Some(SqliteStoreError::AmbiguousLocalMemberIdentities { first, second })
                    if HashSet::from([first.clone(), second.clone()])
                        == HashSet::from([local_member(), remote_member()])
            )
    ));
}

#[test]
fn sqlite_store_activation_rejects_malformed_local_identity() {
    let provisioner = in_memory_provisioner();
    insert_raw_local_member(&provisioner, "bad!", 1);

    let result = wait_for_store_future(provisioner.into_replication_store());
    let Err(error) = result else {
        panic!("malformed identity should not activate");
    };
    assert!(matches!(
        error,
        StoreError::StoreExternal { ref source, .. }
            if matches!(
                source.downcast_ref::<SqliteStoreError>(),
                Some(SqliteStoreError::InvalidMemberIdentity { raw, .. }) if raw == "bad!"
            )
    ));
}

#[test]
fn sqlite_file_store_reopens_with_its_provisioned_identity() {
    let path = std::env::temp_dir().join(format!(
        "flotsync-replication-store-{}.sqlite",
        Uuid::new_v4()
    ));
    let member = local_member();
    let provisioner = wait_for_store_future(SqliteReplicationStoreProvisioner::create_file(&path))
        .expect("file provisioner should build");
    wait_for_store_future(provision_local_identity(
        &provisioner,
        member.clone(),
        &test_replication_security_secrets(),
    ))
    .expect("identity should provision");
    let store = wait_for_store_future(provisioner.into_replication_store())
        .expect("new store should activate");
    wait_for_store_future(store.close()).expect("new store should close");
    drop(store);

    let provisioner = wait_for_store_future(SqliteReplicationStoreProvisioner::open_file(&path))
        .expect("existing store should open");
    let store = wait_for_store_future(provisioner.into_replication_store())
        .expect("existing store should activate");
    let loaded_member =
        wait_for_store_future(store.local_member_identity()).expect("identity should load");
    assert_eq!(loaded_member, member);
    wait_for_store_future(store.close()).expect("reopened store should close");
    drop(store);
    std::fs::remove_file(path).expect("test database should be removed");
}

fn initial_versions(member_count: usize) -> VersionVector {
    VersionVector::initial(NonZeroUsize::new(member_count).expect("member count is non-zero"))
}

fn metadata_snapshot(primary_group_id: GroupId, equivalent_group_id: GroupId) -> InitialSnapshot {
    InitialSnapshot::Metadata(InitialSnapshotMetadata {
        primary_ref: SnapshotRef {
            group_id: primary_group_id,
            versions: initial_versions(2),
        },
        equivalent_refs: smallvec::smallvec![SnapshotRef {
            group_id: equivalent_group_id,
            versions: initial_versions(3),
        }],
        record_count: Some(7),
    })
}

fn inline_snapshot() -> InitialSnapshot {
    let row = RowValues::try_from_fields(
        title_schema().as_ref(),
        crate::row_values! {
            "title" => "stored",
        }
        .fields,
    )
    .expect("inline snapshot fixture row should match docs schema");
    InitialSnapshot::Inline(InitialGroupValueRows {
        datasets: vec![InitialDatasetValueRows {
            dataset_id: docs_dataset_id(),
            rows: vec![InitialValueRow {
                row_key: RowKey(Uuid::from_u128(30_001)),
                row,
            }],
        }],
    })
}

fn creation_invitation_decision(group_id: GroupId) -> PendingGroupDecisionRecord {
    PendingGroupDecisionRecord::GroupInvitation(GroupInvitation::new_creation(
        group_id,
        vec![local_member(), remote_member()],
        GroupSchema::default(),
        InitialSnapshot::Empty,
        Some("shared docs".to_owned()),
        Some("join".to_owned()),
    ))
}

fn migration_invitation_decision(migration_id: MigrationId) -> PendingGroupDecisionRecord {
    PendingGroupDecisionRecord::GroupInvitation(GroupInvitation::new_migration(
        migration_id,
        vec![local_member(), remote_member(), third_member()],
        docs_group_schema(),
        inline_snapshot(),
        None,
        Some("migration invite".to_owned()),
    ))
}

fn migration_proposal_decision(migration_id: MigrationId) -> PendingGroupDecisionRecord {
    PendingGroupDecisionRecord::MigrationProposal(MigrationProposal {
        migration_id,
        final_versions: initial_versions(2).with_version_at(0, 3),
        proposed_members: vec![local_member(), remote_member(), third_member()],
        group_schema: GroupSchema::default(),
        initial_snapshot: metadata_snapshot(migration_id.old_group_id, migration_id.new_group_id),
        group_name: Some("new docs".to_owned()),
        message: None,
    })
}

#[test]
fn pending_group_work_state_decodes_stable_sql_values() {
    assert_matches!(
        PendingGroupWorkState::try_from("decision".to_owned()),
        Ok(PendingGroupWorkState::AwaitingDecision)
    );
    assert_matches!(
        PendingGroupWorkState::try_from("activation".to_owned()),
        Ok(PendingGroupWorkState::AcceptedActivation)
    );
    assert_matches!(
        PendingGroupWorkState::try_from("unknown".to_owned()),
        Err(PendingGroupSqlKeyError::UnknownWorkState { raw }) if raw == "unknown"
    );
}

#[test]
fn pending_group_decisions_round_trip_through_sqlite_payloads() {
    let store = in_memory_store(local_member());
    let migration_id = MigrationId {
        old_group_id: GroupId(Uuid::from_u128(10_001)),
        new_group_id: GroupId(Uuid::from_u128(10_002)),
    };
    let proposal_migration_id = MigrationId {
        old_group_id: migration_id.old_group_id,
        new_group_id: GroupId(Uuid::from_u128(10_003)),
    };
    let records = vec![
        creation_invitation_decision(GroupId(Uuid::from_u128(10_000))),
        migration_invitation_decision(migration_id),
        migration_proposal_decision(proposal_migration_id),
    ];

    wait_for_store_future(async {
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("transaction should open");
        transaction
            .insert_replication_group(sample_group(migration_id.old_group_id))
            .await
            .expect("old group should store");
        for record in records.iter().cloned() {
            let (material, _) = sample_group(record.group_id()).into_parts();
            transaction
                .ensure_replication_group_material(material)
                .await
                .expect("target group material should store");
            transaction
                .upsert_pending_group_decision(record)
                .await
                .expect("pending decision should store");
        }
        transaction
            .commit()
            .await
            .expect("transaction should commit");
    });

    let loaded = wait_for_store_future(async {
        let mut transaction = store
            .begin_read_transaction()
            .await
            .expect("read transaction should open");
        let loaded = transaction
            .load_pending_group_decisions()
            .await
            .expect("pending decisions should load");
        transaction
            .release()
            .await
            .expect("read transaction should release");
        loaded
    });

    assert_eq!(loaded.len(), records.len());
    for record in records {
        assert!(
            loaded.contains(&record),
            "loaded decisions should contain {record:?}"
        );
    }
}

#[test]
fn pending_group_activation_remove_is_idempotent() {
    let store = in_memory_store(local_member());
    let migration_id = MigrationId {
        old_group_id: GroupId(Uuid::from_u128(11_001)),
        new_group_id: GroupId(Uuid::from_u128(11_002)),
    };
    let record = migration_proposal_decision(migration_id).into_activation();
    let key = record.key();

    wait_for_store_future(async {
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("transaction should open");
        transaction
            .insert_replication_group(sample_group(migration_id.old_group_id))
            .await
            .expect("old group should store");
        let (material, _) = sample_group(migration_id.new_group_id).into_parts();
        transaction
            .ensure_replication_group_material(material)
            .await
            .expect("target group material should store");
        transaction
            .upsert_pending_group_activation(record.clone())
            .await
            .expect("pending activation should store");
        transaction
            .commit()
            .await
            .expect("transaction should commit");
    });

    let loaded = wait_for_store_future(async {
        let mut transaction = store
            .begin_read_transaction()
            .await
            .expect("read transaction should open");
        let loaded = transaction
            .load_pending_group_activations()
            .await
            .expect("pending activations should load");
        transaction
            .release()
            .await
            .expect("read transaction should release");
        loaded
    });
    assert_eq!(loaded, vec![record]);

    let (first_removed, second_removed) = wait_for_store_future(async {
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("transaction should open");
        let first_removed = transaction
            .remove_pending_group_activation(key)
            .await
            .expect("first remove should succeed");
        let second_removed = transaction
            .remove_pending_group_activation(key)
            .await
            .expect("second remove should succeed");
        transaction
            .commit()
            .await
            .expect("transaction should commit");
        (first_removed, second_removed)
    });
    assert!(first_removed);
    assert!(!second_removed);
}

#[test]
fn pending_group_work_accepts_replay_and_rejects_conflicting_target_work() {
    let store = in_memory_store(local_member());
    let group_id = GroupId(Uuid::from_u128(11_100));
    let original = creation_invitation_decision(group_id);
    let mut metadata_update = original.clone();
    let PendingGroupDecisionRecord::GroupInvitation(invitation) = &mut metadata_update else {
        panic!("creation invitation fixture must contain an invitation");
    };
    invitation.message = Some("different invitation".to_owned());
    invitation.group_name = Some("renamed docs".to_owned());
    let mut conflicting = original.clone();
    let PendingGroupDecisionRecord::GroupInvitation(invitation) = &mut conflicting else {
        panic!("creation invitation fixture must contain an invitation");
    };
    invitation.proposed_members.push(third_member());

    let conflict = wait_for_store_future(async {
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("transaction should open");
        let (material, _) = sample_group(group_id).into_parts();
        transaction
            .ensure_replication_group_material(material)
            .await
            .expect("target group material should store");
        transaction
            .upsert_pending_group_decision(original.clone())
            .await
            .expect("pending decision should store");
        transaction
            .upsert_pending_group_decision(original.clone())
            .await
            .expect("exact pending decision replay should be idempotent");
        transaction
            .upsert_pending_group_decision(metadata_update.clone())
            .await
            .expect("metadata-only replay should update pending work");
        let conflict = transaction
            .upsert_pending_group_decision(conflicting)
            .await
            .expect_err("conflicting target work should be rejected");
        transaction
            .commit()
            .await
            .expect("transaction should commit");
        conflict
    });

    assert!(is_conflicting_pending_group_work(&conflict, group_id));
    let loaded = wait_for_store_future(async {
        let mut transaction = store
            .begin_read_transaction()
            .await
            .expect("read transaction should open");
        let loaded = transaction
            .load_pending_group_decision(&group_id)
            .await
            .expect("pending decision should load");
        transaction
            .release()
            .await
            .expect("read transaction should release");
        loaded
    });
    assert_eq!(loaded, Some(metadata_update));
}

#[test]
fn pending_group_decision_transitions_to_one_activation_for_target_group() {
    let store = in_memory_store(local_member());
    let group_id = GroupId(Uuid::from_u128(11_101));
    let decision = creation_invitation_decision(group_id);
    let activation = decision.clone().into_activation();
    let mut metadata_update = decision.clone();
    let PendingGroupDecisionRecord::GroupInvitation(invitation) = &mut metadata_update else {
        panic!("creation invitation fixture must contain an invitation");
    };
    invitation.group_name = Some("latest docs".to_owned());
    invitation.message = Some("latest message".to_owned());
    let expected_activation = metadata_update.clone().into_activation();

    wait_for_store_future(async {
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("transaction should open");
        let (material, _) = sample_group(group_id).into_parts();
        transaction
            .ensure_replication_group_material(material)
            .await
            .expect("target group material should store");
        transaction
            .upsert_pending_group_decision(decision)
            .await
            .expect("pending decision should store");
        transaction
            .upsert_pending_group_activation(activation.clone())
            .await
            .expect("pending decision should transition to activation");
        transaction
            .upsert_pending_group_decision(metadata_update)
            .await
            .expect("metadata replay should not regress accepted activation");
        transaction
            .commit()
            .await
            .expect("transaction should commit");
    });

    let (loaded_decision, loaded_activation) = wait_for_store_future(async {
        let mut transaction = store
            .begin_read_transaction()
            .await
            .expect("read transaction should open");
        let loaded_decision = transaction
            .load_pending_group_decision(&group_id)
            .await
            .expect("pending decision lookup should succeed");
        let loaded_activation = transaction
            .load_pending_group_activation(&group_id)
            .await
            .expect("pending activation lookup should succeed");
        transaction
            .release()
            .await
            .expect("read transaction should release");
        (loaded_decision, loaded_activation)
    });
    assert_eq!(loaded_decision, None);
    assert_eq!(loaded_activation, Some(expected_activation));
}

#[test]
fn inactive_group_material_is_not_active_and_cannot_own_data_state() {
    let dataset_id = docs_dataset_id();
    let schema = title_schema();
    let store = in_memory_store(local_member());
    let group_id = GroupId(Uuid::from_u128(11_102));
    let row_key = RowKey(Uuid::from_u128(11_103));
    let group = sample_group(group_id);
    let (material, _) = group.clone().into_parts();

    wait_for_store_future(async {
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("transaction should open");
        transaction
            .ensure_replication_group_material(material)
            .await
            .expect("inactive group material should store");
        transaction
            .commit()
            .await
            .expect("transaction should commit");
    });

    let (active_group, active_groups, stored_material) = wait_for_store_future(async {
        let mut transaction = store
            .begin_read_transaction()
            .await
            .expect("read transaction should open");
        let active_group = transaction
            .load_replication_group(&group_id)
            .await
            .expect("active group lookup should succeed");
        let active_groups = transaction
            .load_replication_groups()
            .await
            .expect("active groups should load");
        let stored_material = transaction
            .load_replication_group_material(&group_id)
            .await
            .expect("group material lookup should succeed");
        transaction
            .release()
            .await
            .expect("read transaction should release");
        (active_group, active_groups, stored_material)
    });
    assert_eq!(active_group, None);
    assert!(active_groups.is_empty());
    assert!(stored_material.is_some());

    let (row_error, update_error) = wait_for_store_future(async {
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("transaction should open");
        let row_error = apply_row_patch(
            transaction.as_mut(),
            &schema,
            DatasetRowStatePatch {
                group_id,
                dataset_id: dataset_id.clone(),
                actions: vec![DatasetRowStateWrite::UpsertActive {
                    row_key,
                    snapshot: title_snapshot(&schema, row_key, "inactive"),
                }],
                change_id: sample_change_id(),
                last_changed_versions: sample_last_changed_versions(),
            },
        )
        .await
        .expect_err("inactive material must not own row state");
        let update_error = transaction
            .append_replication_update(ReplicationUpdateRecord {
                group_id,
                update_id: UpdateId {
                    node_index: 0,
                    version: 1,
                },
                sender: local_member(),
                read_versions: VersionVector::initial(group.member_count()),
                dataset_updates: Vec::new(),
                applied_locally: false,
            })
            .await
            .expect_err("inactive material must not own update state");
        transaction
            .rollback()
            .await
            .expect("transaction should roll back");
        (row_error, update_error)
    });
    assert!(matches!(row_error, StoreError::StoreExternal { .. }));
    assert!(matches!(update_error, StoreError::StoreExternal { .. }));
}

#[test]
fn stored_member_identity_rejects_overlong_identifier() {
    let raw = std::iter::repeat_n("s", MAX_IDENTIFIER_SEGMENTS + 1).join(".");

    let error = decode_member_identity(&raw).unwrap_err();

    assert_matches!(error, StoreError::StoreExternal { .. });
    let StoreError::StoreExternal { source, .. } = error;
    let sqlite_error = source
        .downcast_ref::<SqliteStoreError>()
        .expect("store external error should preserve sqlite store error");

    assert_matches!(
        sqlite_error,
        SqliteStoreError::InvalidMemberIdentity {
            source: IdentifierParseError::ParseTooManySegmentsError {
                actual,
                ..
            },
            ..
        } if *actual == MAX_IDENTIFIER_SEGMENTS + 1
    );
}

fn title_schema() -> Arc<Schema> {
    Arc::new(Schema::from_fields([Field::linear_string("title")]))
}

fn heading_schema() -> Arc<Schema> {
    Arc::new(Schema::from_fields([Field::linear_string("heading")]))
}

fn docs_group_schema() -> GroupSchema {
    GroupSchema::new(HashMap::from([(
        docs_dataset_id(),
        SchemaSource::from(title_schema()),
    )]))
}

fn sample_encrypted_secret(seed: u8) -> EncryptedStoreSecret {
    EncryptedStoreSecret {
        crypto_version: StoreSecretCryptoVersion::new(1),
        key_id: StoreSecretKeyId::from_u128_for_test(u128::from(seed)),
        nonce: Vec::from([seed, seed.wrapping_add(1)]).into_boxed_slice(),
        ciphertext: Box::from([seed, seed.wrapping_add(1), seed.wrapping_add(2)]),
    }
}

fn sample_group(group_id: GroupId) -> ReplicationGroupRecord {
    let member_keys = [local_member(), remote_member()]
        .into_iter()
        .map(|member_id| MemberKeyId {
            fingerprint: test_public_member_keys(&member_id).fingerprint(),
            member_id,
        })
        .collect::<Vec<_>>();
    let member_keys = GroupMemberKeys::from_ordered_member_keys(member_keys)
        .expect("test group member keys should build");
    let mut version_vector = VersionVector::initial(NonZeroUsize::new(2).unwrap());
    version_vector.increment_at(0);
    ReplicationGroupRecord {
        group_id,
        member_keys,
        local_member_index: MemberIndex::new(0),
        group_schema: docs_group_schema(),
        version_vector,
        lifecycle: ReplicationGroupLifecycle::Open,
        security_material: current_slice_placeholder_group_security_material(group_id),
        ..Default::default()
    }
}

fn insert_group_expect_error(
    store: &SqliteReplicationStore,
    group: ReplicationGroupRecord,
) -> StoreError {
    wait_for_store_future(async {
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("transaction should start");
        let error = transaction
            .insert_replication_group(group)
            .await
            .expect_err("incomplete group should fail");
        transaction
            .rollback()
            .await
            .expect("failed insert should roll back");
        error
    })
}

fn assert_sqlite_store_error(
    error: &StoreError,
    predicate: impl FnOnce(&SqliteStoreError) -> bool,
) {
    let StoreError::StoreExternal { source, .. } = error;
    let sqlite_error = source
        .downcast_ref::<SqliteStoreError>()
        .expect("store error should retain SQLite context");
    assert!(predicate(sqlite_error), "unexpected store error: {error:?}");
}

fn load_all_groups(store: &SqliteReplicationStore) -> Vec<ReplicationGroupRecord> {
    wait_for_store_future(async {
        let mut transaction = store
            .begin_read_transaction()
            .await
            .expect("read transaction should start");
        let groups = transaction
            .load_replication_groups()
            .await
            .expect("groups should load");
        transaction
            .release()
            .await
            .expect("read transaction should release");
        groups
    })
}

fn is_conflicting_member_security_material(
    error: &StoreError,
    object: &'static str,
    member_id: &MemberIdentity,
) -> bool {
    match error {
        StoreError::StoreExternal { source, .. } => matches!(
            source.downcast_ref::<SqliteStoreError>(),
            Some(SqliteStoreError::ConflictingMemberSecurityMaterial {
                object: stored_object,
                member_id: stored_member_id,
            }) if *stored_object == object && stored_member_id == member_id
        ),
    }
}

fn is_conflicting_pending_group_work(error: &StoreError, group_id: GroupId) -> bool {
    match error {
        StoreError::StoreExternal { source, .. } => matches!(
            source.downcast_ref::<SqliteStoreError>(),
            Some(SqliteStoreError::ConflictingPendingGroupWork {
                group_id: stored_group_id,
            }) if *stored_group_id == group_id
        ),
    }
}

fn sample_last_changed_versions() -> VersionVector {
    let mut version_vector = VersionVector::initial(NonZeroUsize::new(2).unwrap());
    version_vector.increment_at(0);
    version_vector
}

fn sample_change_id() -> UpdateId {
    UpdateId {
        node_index: 0,
        version: 1,
    }
}

fn insert_row_patch(
    group_id: GroupId,
    dataset_id: &DatasetId,
    row_key: RowKey,
    operation: &flotsync_messages::SchemaOperation<'_>,
) -> DatasetRowStatePatch {
    let RowOperation::Insert { snapshot, .. } = &operation.operation else {
        panic!("expected insert operation");
    };
    DatasetRowStatePatch {
        group_id,
        dataset_id: dataset_id.clone(),
        actions: vec![DatasetRowStateWrite::UpsertActive {
            row_key,
            snapshot: snapshot.clone().into_owned(),
        }],
        change_id: sample_change_id(),
        last_changed_versions: sample_last_changed_versions(),
    }
}

fn title_snapshot(
    schema: &Arc<Schema>,
    row_key: RowKey,
    title: &str,
) -> ReplicationRowStateSnapshot {
    string_snapshot(schema, "title", row_key, title)
}

fn heading_snapshot(
    schema: &Arc<Schema>,
    row_key: RowKey,
    heading: &str,
) -> ReplicationRowStateSnapshot {
    string_snapshot(schema, "heading", row_key, heading)
}

fn string_snapshot(
    schema: &Arc<Schema>,
    field_name: &str,
    row_key: RowKey,
    value: &str,
) -> ReplicationRowStateSnapshot {
    let mut source_data = flotsync_messages::InMemoryStateData::new(schema.clone());
    let operation = source_data
        .insert_row(
            UpdateId {
                node_index: 0,
                version: 1,
            },
            row_key.0,
            vec![
                schema
                    .columns
                    .get(field_name)
                    .expect("string field should exist")
                    .initial(value)
                    .expect("field value should build"),
            ],
        )
        .expect("row insert should succeed");
    let RowOperation::Insert { snapshot, .. } = operation.operation else {
        panic!("expected insert operation");
    };
    snapshot.into_owned()
}

fn encoded_insert_snapshot(
    title: &str,
    schema: &Arc<Schema>,
) -> flotsync_messages::datamodel::SchemaOperation {
    let mut source_data = flotsync_messages::InMemoryStateData::new(schema.clone());
    let operation = source_data
        .insert_row(
            UpdateId {
                node_index: 0,
                version: 1,
            },
            Uuid::from_u128(30_001),
            vec![
                schema
                    .columns
                    .get("title")
                    .expect("title field should exist")
                    .initial(title)
                    .expect("field value should build"),
            ],
        )
        .expect("row insert should succeed");
    encode_schema_operation(&operation, schema.as_ref()).expect("operation should encode")
}

#[test]
fn dropping_open_sqlite_transaction_releases_store() {
    let store = Arc::new(in_memory_store(local_member()));
    let transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    drop(transaction);

    let (probe_result_tx, probe_result_rx) = std::sync::mpsc::channel();
    let store = store.clone();
    std::thread::spawn(move || {
        let probe_result = wait_for_store_future(store.begin_transaction()).map(|transaction| {
            wait_for_store_future(transaction.rollback())
                .expect("probe transaction should roll back");
        });
        let _ = probe_result_tx.send(probe_result);
    });

    let probe_result = probe_result_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("dropping an open transaction should release the SQLite store promptly");
    probe_result.expect("dropped transaction should leave the SQLite store usable");
}

#[test]
fn sqlite_store_roundtrips_replication_group_lifecycle() {
    let store = in_memory_store(local_member());
    let group_id = GroupId(Uuid::from_u128(99));
    let successor_group_id = GroupId(Uuid::from_u128(100));
    let group = sample_group(group_id);
    let final_versions = group.version_vector.clone();
    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.insert_replication_group(group))
        .expect("group should insert");
    wait_for_store_future(transaction.update_replication_group_lifecycle(
        &group_id,
        ReplicationGroupLifecycle::ReadOnly {
            successor_group_id,
            final_versions: final_versions.clone(),
        },
    ))
    .expect("read-only lifecycle should store");
    wait_for_store_future(transaction.commit()).expect("transaction should commit");

    let mut transaction =
        wait_for_store_future(store.begin_read_transaction()).expect("read should start");
    let stored = wait_for_store_future(transaction.load_replication_group(&group_id))
        .expect("group should load")
        .expect("group should exist");
    assert_eq!(
        stored.lifecycle,
        ReplicationGroupLifecycle::ReadOnly {
            successor_group_id,
            final_versions: final_versions.clone(),
        }
    );
    wait_for_store_future(transaction.release()).expect("read should release");

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.update_replication_group_lifecycle(
        &group_id,
        ReplicationGroupLifecycle::Closed {
            successor_group_id,
            final_versions: final_versions.clone(),
        },
    ))
    .expect("closed lifecycle should store");
    wait_for_store_future(transaction.commit()).expect("transaction should commit");

    let mut transaction =
        wait_for_store_future(store.begin_read_transaction()).expect("read should start");
    let stored = wait_for_store_future(transaction.load_replication_group(&group_id))
        .expect("group should load")
        .expect("group should exist");
    assert_eq!(
        stored.lifecycle,
        ReplicationGroupLifecycle::Closed {
            successor_group_id,
            final_versions,
        }
    );
    wait_for_store_future(transaction.release()).expect("read should release");
}

#[test]
fn sqlite_store_refreshes_compatible_group_metadata() {
    let store = in_memory_store(local_member());
    let group_id = GroupId(Uuid::from_u128(101));
    let mut group = sample_group(group_id);
    group.group_name = Some("first name".to_owned());

    wait_for_store_future(async {
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("transaction should start");
        transaction
            .insert_replication_group(group.clone())
            .await
            .expect("group should insert");
        transaction
            .commit()
            .await
            .expect("transaction should commit");
    });

    let mut refreshed = group.clone();
    refreshed.group_name = Some("latest name".to_owned());
    let (material, _) = refreshed.clone().into_parts();
    wait_for_store_future(async {
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("transaction should start");
        transaction
            .ensure_replication_group_material(material)
            .await
            .expect("compatible metadata should refresh");
        transaction
            .commit()
            .await
            .expect("transaction should commit");
    });

    let stored = wait_for_store_future(async {
        let mut transaction = store
            .begin_read_transaction()
            .await
            .expect("read should start");
        let stored = transaction
            .load_replication_group(&group_id)
            .await
            .expect("group should load")
            .expect("group should exist");
        transaction.release().await.expect("read should release");
        stored
    });
    assert_eq!(stored.group_name, refreshed.group_name);
}

#[test]
fn sqlite_store_rejects_incomplete_group_defaults_before_writing() {
    let store = in_memory_store(local_member());

    let material_error = wait_for_store_future(async {
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("transaction should start");
        let error = transaction
            .ensure_replication_group_material(ReplicationGroupMaterialRecord::default())
            .await
            .expect_err("default material should fail");
        transaction
            .rollback()
            .await
            .expect("failed material insert should roll back");
        error
    });
    assert_sqlite_store_error(&material_error, |error| {
        matches!(error, SqliteStoreError::NilGroupId)
    });

    let mut nil_group = sample_group(GroupId::NIL);
    nil_group.group_id = GroupId::NIL;
    let nil_error = insert_group_expect_error(&store, nil_group);
    assert_sqlite_store_error(&nil_error, |error| {
        matches!(error, SqliteStoreError::NilGroupId)
    });

    let mut empty_members = sample_group(GroupId(Uuid::from_u128(102)));
    empty_members.member_keys = ReplicationGroupRecord::default().member_keys;
    let members_error = insert_group_expect_error(&store, empty_members);
    assert_sqlite_store_error(&members_error, |error| {
        matches!(error, SqliteStoreError::EmptyGroupMembers)
    });

    let invalid_index_group_id = GroupId(Uuid::from_u128(107));
    let mut invalid_index = sample_group(invalid_index_group_id);
    invalid_index.local_member_index = MemberIndex::new(u32::MAX);
    let index_error = insert_group_expect_error(&store, invalid_index);
    assert_sqlite_store_error(&index_error, |error| {
        matches!(error, SqliteStoreError::InvalidLocalMemberIndex { .. })
    });

    let invalid_security_group_id = GroupId(Uuid::from_u128(103));
    let mut invalid_security = sample_group(invalid_security_group_id);
    invalid_security.security_material = ReplicationGroupRecord::default().security_material;
    let security_error = insert_group_expect_error(&store, invalid_security);
    assert_sqlite_store_error(&security_error, |error| {
        matches!(
            error,
            SqliteStoreError::InvalidDefaultGroupSecurityMaterial { group_id }
                if *group_id == invalid_security_group_id
        )
    });

    let mismatched_progress_group_id = GroupId(Uuid::from_u128(104));
    let mut mismatched_progress = sample_group(mismatched_progress_group_id);
    mismatched_progress.version_vector =
        VersionVector::initial(NonZeroUsize::new(1).expect("test vector has members"));
    let progress_error = insert_group_expect_error(&store, mismatched_progress);
    assert_sqlite_store_error(&progress_error, |error| {
        matches!(
            error,
            SqliteStoreError::ActiveVersionMemberCountMismatch { group_id, .. }
                if *group_id == mismatched_progress_group_id
        )
    });

    let mismatched_lifecycle_group_id = GroupId(Uuid::from_u128(105));
    let mut mismatched_lifecycle = sample_group(mismatched_lifecycle_group_id);
    mismatched_lifecycle.lifecycle = ReplicationGroupLifecycle::ReadOnly {
        successor_group_id: GroupId(Uuid::from_u128(106)),
        final_versions: VersionVector::initial(
            NonZeroUsize::new(1).expect("test vector has members"),
        ),
    };
    let lifecycle_error = insert_group_expect_error(&store, mismatched_lifecycle);
    assert_sqlite_store_error(&lifecycle_error, |error| {
        matches!(
            error,
            SqliteStoreError::LifecycleVersionMemberCountMismatch { group_id, .. }
                if *group_id == mismatched_lifecycle_group_id
        )
    });

    assert!(load_all_groups(&store).is_empty());
}

#[test]
fn sqlite_store_rejects_mismatched_row_patch_schema_context() {
    let dataset_id = docs_dataset_id();
    let other_dataset_id = DatasetId::try_from_static("other").expect("dataset id should build");
    let schema = title_schema();
    let store = in_memory_store(local_member());
    let group_id = GroupId(Uuid::from_u128(100));
    let other_group_id = GroupId(Uuid::from_u128(101));
    let patch = DatasetRowStatePatch {
        group_id,
        dataset_id: dataset_id.clone(),
        actions: Vec::new(),
        change_id: sample_change_id(),
        last_changed_versions: sample_last_changed_versions(),
    };
    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");

    let group_error = wait_for_store_future(transaction.apply_dataset_row_patch(
        GroupDatasetSchemaRef {
            group_id: &other_group_id,
            dataset_id: &dataset_id,
            schema: &schema,
        },
        &patch,
    ))
    .expect_err("mismatched context group should fail before empty-patch handling");
    let dataset_error = wait_for_store_future(transaction.apply_dataset_row_patch(
        GroupDatasetSchemaRef {
            group_id: &group_id,
            dataset_id: &other_dataset_id,
            schema: &schema,
        },
        &patch,
    ))
    .expect_err("mismatched context dataset should fail before empty-patch handling");
    wait_for_store_future(transaction.rollback()).expect("transaction should roll back");

    assert_eq!(
        group_error.classification().class,
        StoreErrorClass::Contract
    );
    assert_sqlite_store_error(&group_error, |error| {
        matches!(
            error,
            SqliteStoreError::InvalidDatasetRowPatchContext {
                context_group,
                context_dataset,
                patch_group,
                patch_dataset,
            } if *context_group == other_group_id
                && context_dataset == &dataset_id
                && *patch_group == group_id
                && patch_dataset == &dataset_id
        )
    });
    assert_eq!(
        dataset_error.classification().class,
        StoreErrorClass::Contract
    );
    assert_sqlite_store_error(&dataset_error, |error| {
        matches!(
            error,
            SqliteStoreError::InvalidDatasetRowPatchContext {
                context_group,
                context_dataset,
                patch_group,
                patch_dataset,
            } if *context_group == group_id
                && context_dataset == &other_dataset_id
                && *patch_group == group_id
                && patch_dataset == &dataset_id
        )
    });
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "This store roundtrip test keeps related group, dataset, and update assertions in one fixture."
)]
fn sqlite_store_roundtrips_group_dataset_and_update_records() {
    let dataset_id = docs_dataset_id();
    let schema = title_schema();
    let store = in_memory_store(local_member());
    let group_id = GroupId(Uuid::from_u128(101));
    let row_key = RowKey(Uuid::from_u128(202));
    let group = sample_group(group_id);
    let mut updated_version_vector = group.version_vector.clone();
    updated_version_vector.increment_at(1);
    let mut source_data = flotsync_messages::InMemoryStateData::new(schema.clone());
    let operation = source_data
        .insert_row(
            UpdateId {
                node_index: 0,
                version: 1,
            },
            row_key.0,
            vec![
                schema
                    .columns
                    .get("title")
                    .expect("title field should exist")
                    .initial("hello")
                    .expect("field value should build"),
            ],
        )
        .expect("row insert should succeed");
    let encoded_operation =
        encode_schema_operation(&operation, schema.as_ref()).expect("operation should encode");
    let row_patch = insert_row_patch(group_id, &dataset_id, row_key, &operation);
    let expected_row = match &row_patch.actions[0] {
        DatasetRowStateWrite::UpsertActive { row_key, snapshot } => ReplicationRowStateFixture {
            row_id: *row_key,
            snapshot: snapshot.clone(),
            tombstoned: false,
            created_by: Some(row_patch.change_id),
            last_changed_versions: row_patch.last_changed_versions.clone(),
        },
        DatasetRowStateWrite::UpsertTombstone { .. } => panic!("expected active row patch"),
    };
    let update = ReplicationUpdateRecord {
        group_id,
        update_id: UpdateId {
            node_index: 0,
            version: 1,
        },
        sender: local_member(),
        read_versions: VersionVector::initial(NonZeroUsize::new(2).unwrap()),
        dataset_updates: vec![DatasetUpdateRecord {
            dataset_id: dataset_id.clone(),
            operations: vec![encoded_operation.clone()],
        }],
        applied_locally: false,
    };

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.insert_replication_group(group.clone()))
        .expect("group should store");
    wait_for_store_future(
        transaction
            .update_replication_group_version_vector(&group_id, updated_version_vector.clone()),
    )
    .expect("group version vector should update");
    wait_for_store_future(apply_row_patch(transaction.as_mut(), &schema, row_patch))
        .expect("row patch should store");
    wait_for_store_future(transaction.append_replication_update(update.clone()))
        .expect("update should store");
    wait_for_store_future(transaction.commit()).expect("commit should succeed");

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    let loaded_group = wait_for_store_future(transaction.load_replication_group(&group_id))
        .expect("group should load")
        .expect("group should exist");
    assert_eq!(loaded_group.group_id, group.group_id);
    assert_eq!(loaded_group.member_keys, group.member_keys);
    assert_eq!(loaded_group.group_schema, group.group_schema);
    assert_eq!(loaded_group.security_material, group.security_material);
    assert_eq!(
        loaded_group.version_vector.iter().collect::<Vec<_>>(),
        updated_version_vector.iter().collect::<Vec<_>>()
    );

    let missing_row_key = RowKey(Uuid::from_u128(203));
    let requested_row_keys = [row_key, missing_row_key];
    let mut requested_row_keys = requested_row_keys.iter();
    let loaded_snapshot = wait_for_store_future(transaction.load_dataset_rows(
        GroupDatasetSchemaRef {
            group_id: &group_id,
            dataset_id: &dataset_id,
            schema: &schema,
        },
        &mut requested_row_keys,
    ))
    .expect("row slice should load");
    assert!(loaded_snapshot.dataset_exists);
    assert_eq!(loaded_snapshot.state_rows.len(), 1);
    assert_eq!(loaded_snapshot.missing_row_keys.len(), 1);
    assert_eq!(
        loaded_row_fixture(&loaded_snapshot, row_key),
        Some(expected_row)
    );
    assert!(loaded_snapshot.missing_row_keys.contains(&missing_row_key));

    let query = RequestedDatasetRowsQuery::new(
        DatasetRowsQuery::borrowed(GroupDatasetSchemaRef {
            group_id: &group_id,
            dataset_id: &dataset_id,
            schema: &schema,
        }),
        [missing_row_key, row_key, row_key],
    );
    let mut cursor = PageCursor::new(query);
    let mut requested_page = RequestedDatasetRowPageBatch::bounded(
        &schema,
        NonZeroUsize::new(1).expect("requested-row page limit should be non-zero"),
    );
    wait_for_store_future(transaction.load_dataset_rows_into(&mut cursor, &mut requested_page))
        .expect("first requested-row page should load");
    let first_outcomes = requested_page.outcomes().collect::<Vec<_>>();
    assert!(matches!(
        first_outcomes.as_slice(),
        [RequestedDatasetRowView::Present(row)] if row.metadata().row_key == row_key
    ));
    assert!(cursor.has_more());

    wait_for_store_future(transaction.load_dataset_rows_into(&mut cursor, &mut requested_page))
        .expect("second requested-row page should load");
    assert!(matches!(
        requested_page.outcomes().collect::<Vec<_>>().as_slice(),
        [RequestedDatasetRowView::Missing(key)] if *key == missing_row_key
    ));
    assert!(cursor.is_exhausted());

    let loaded_update =
        wait_for_store_future(transaction.load_replication_update(&group_id, update.update_id))
            .expect("update should load")
            .expect("update should exist");
    assert_eq!(loaded_update, update);
}

#[test]
fn sqlite_store_loads_replication_groups_by_requested_ids() {
    let store = in_memory_store(local_member());
    let first_group_id = GroupId(Uuid::from_u128(10_001));
    let second_group_id = GroupId(Uuid::from_u128(10_002));
    let missing_group_id = GroupId(Uuid::from_u128(10_003));
    let first_group = sample_group(first_group_id);
    let second_group = sample_group(second_group_id);

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.insert_replication_group(first_group))
        .expect("first group should store");
    wait_for_store_future(transaction.insert_replication_group(second_group))
        .expect("second group should store");
    wait_for_store_future(transaction.commit()).expect("commit should succeed");

    let requested_group_ids = HashSet::from([first_group_id, missing_group_id]);
    let mut write_transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    let loaded_groups = wait_for_store_future(
        write_transaction.load_replication_groups_for_ids(&requested_group_ids),
    )
    .expect("requested groups should load through write transaction");
    assert_eq!(
        loaded_groups
            .iter()
            .map(|group| group.group_id)
            .collect::<Vec<_>>(),
        vec![first_group_id]
    );
    wait_for_store_future(write_transaction.rollback()).expect("rollback should succeed");

    let mut read_transaction =
        wait_for_store_future(store.begin_read_transaction()).expect("transaction should start");
    let loaded_groups = wait_for_store_future(
        read_transaction.load_replication_groups_for_ids(&requested_group_ids),
    )
    .expect("requested groups should load through read transaction");
    assert_eq!(
        loaded_groups
            .iter()
            .map(|group| group.group_id)
            .collect::<Vec<_>>(),
        vec![first_group_id]
    );
    wait_for_store_future(read_transaction.release()).expect("release should succeed");
}

#[test]
fn sqlite_store_loads_only_writable_group_versions_without_ordering() {
    let store = in_memory_store(local_member());
    let read_only_group_id = GroupId(Uuid::from_u128(10_011));
    let writable_group_id = GroupId(Uuid::from_u128(10_012));
    let successor_group_id = GroupId(Uuid::from_u128(10_013));
    let read_only_group = sample_group(read_only_group_id);
    let final_versions = read_only_group.version_vector.clone();
    let mut writable_group = sample_group(writable_group_id);
    writable_group.version_vector.increment_at(1);
    let expected_writable_versions = writable_group.version_vector.clone();

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.insert_replication_group(read_only_group))
        .expect("read-only group should store while open");
    wait_for_store_future(transaction.insert_replication_group(writable_group))
        .expect("writable group should store");
    wait_for_store_future(transaction.update_replication_group_lifecycle(
        &read_only_group_id,
        ReplicationGroupLifecycle::ReadOnly {
            successor_group_id,
            final_versions,
        },
    ))
    .expect("group should become read-only");
    wait_for_store_future(transaction.commit()).expect("transaction should commit");

    let mut transaction =
        wait_for_store_future(store.begin_read_transaction()).expect("transaction should start");
    let group_versions =
        wait_for_store_future(transaction.load_writable_replication_group_versions())
            .expect("writable group versions should load")
            .into_iter()
            .map(|record| (record.group_id, record.version_vector))
            .collect::<HashMap<_, _>>();
    assert_eq!(
        group_versions,
        HashMap::from([(writable_group_id, expected_writable_versions)])
    );
    wait_for_store_future(transaction.release()).expect("transaction should release");
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "The shared metadata paging contract needs one coherently populated store fixture."
)]
fn sqlite_store_satisfies_metadata_paging_contract() {
    let store = in_memory_store(local_member());
    let provisioned_keys = wait_for_store_future(async {
        let mut transaction = store
            .begin_read_transaction()
            .await
            .expect("read transaction should start");
        let key_ids = transaction
            .load_member_public_key_ids()
            .await
            .expect("provisioned key id should load");
        let [key_id] = key_ids.as_slice() else {
            panic!("freshly provisioned store should contain one member key: {key_ids:?}");
        };
        let record = transaction
            .load_member_public_keys(key_id)
            .await
            .expect("provisioned public keys should load")
            .expect("provisioned public keys should exist");
        transaction
            .release()
            .await
            .expect("read transaction should release");
        record
    });
    let group_ids = [
        GroupId(Uuid::from_u128(20_001)),
        GroupId(Uuid::from_u128(20_002)),
        GroupId(Uuid::from_u128(20_003)),
    ];
    let successor_group_id = GroupId(Uuid::from_u128(20_004));
    let mut groups = group_ids.map(sample_group).to_vec();
    let final_versions = groups[1].version_vector.clone();
    groups[1].lifecycle = ReplicationGroupLifecycle::ReadOnly {
        successor_group_id,
        final_versions: final_versions.clone(),
    };
    let writable_group_versions = [groups[0].clone(), groups[2].clone()]
        .map(|group| WritableReplicationGroupVersionRecord {
            group_id: group.group_id,
            version_vector: group.version_vector,
        })
        .to_vec();

    let first_tied_member_keys =
        MemberPublicKeysRecord::from_public_keys(&test_public_member_keys(&remote_member()));
    let mut second_tied_member_keys =
        MemberPublicKeysRecord::from_public_keys(&test_public_member_keys(&third_member()));
    second_tied_member_keys.key_id.member_id = remote_member();
    let mut tied_fingerprint_keys = first_tied_member_keys.clone();
    tied_fingerprint_keys.key_id.member_id = third_member();
    let text_first_keys = MemberPublicKeysRecord::from_public_keys(&test_public_member_keys(
        &MemberIdentity::from_array(["app", "a-", "x"]),
    ));
    let mut text_second_keys = text_first_keys.clone();
    text_second_keys.key_id.member_id = MemberIdentity::from_array(["app", "a", "x"]);
    let member_public_keys = vec![
        provisioned_keys,
        text_first_keys.clone(),
        text_second_keys.clone(),
        first_tied_member_keys.clone(),
        second_tied_member_keys.clone(),
        tied_fingerprint_keys.clone(),
    ];
    let member_key_trust_evidence = vec![MemberKeyTrustEvidenceRecord {
        key_id: first_tied_member_keys.key_id.clone(),
        evidence_kind: MemberKeyTrustEvidenceKind::LocalExplicitTrust,
    }];

    let pending_group_decisions = vec![
        creation_invitation_decision(GroupId(Uuid::from_u128(21_001))),
        creation_invitation_decision(GroupId(Uuid::from_u128(21_002))),
    ];
    let pending_group_activations = vec![
        creation_invitation_decision(GroupId(Uuid::from_u128(21_003))).into_activation(),
        creation_invitation_decision(GroupId(Uuid::from_u128(21_004))).into_activation(),
    ];

    wait_for_store_future(async {
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("transaction should start");
        for group in groups.iter().cloned() {
            let mut stored_group = group;
            stored_group.lifecycle = ReplicationGroupLifecycle::Open;
            transaction
                .insert_replication_group(stored_group)
                .await
                .expect("paging group should store");
        }
        transaction
            .update_replication_group_lifecycle(
                &groups[1].group_id,
                ReplicationGroupLifecycle::ReadOnly {
                    successor_group_id,
                    final_versions,
                },
            )
            .await
            .expect("paging group lifecycle should update");
        for record in [
            first_tied_member_keys,
            second_tied_member_keys,
            tied_fingerprint_keys,
            text_first_keys,
            text_second_keys,
        ] {
            transaction
                .ensure_member_public_keys(record)
                .await
                .expect("paging public keys should store");
        }
        for record in member_key_trust_evidence.iter().cloned() {
            transaction
                .ensure_member_key_trust_evidence(record)
                .await
                .expect("paging trust evidence should store");
        }
        for record in pending_group_decisions.iter().cloned() {
            let (material, _) = sample_group(record.group_id()).into_parts();
            transaction
                .ensure_replication_group_material(material)
                .await
                .expect("pending-decision group material should store");
            transaction
                .upsert_pending_group_decision(record)
                .await
                .expect("pending decision should store");
        }
        for record in pending_group_activations.iter().cloned() {
            let (material, _) = sample_group(record.group_id()).into_parts();
            transaction
                .ensure_replication_group_material(material)
                .await
                .expect("pending-activation group material should store");
            transaction
                .upsert_pending_group_activation(record)
                .await
                .expect("pending activation should store");
        }
        transaction
            .commit()
            .await
            .expect("paging fixtures should commit");
    });

    let fixtures = MetadataPagingFixtures {
        groups,
        writable_group_versions,
        member_public_keys,
        member_key_trust_evidence,
        pending_group_decisions,
        pending_group_activations,
        missing_group_id: GroupId(Uuid::from_u128(29_999)),
    };
    wait_for_store_future(assert_metadata_paging_contract(store.as_ref(), &fixtures))
        .expect("SQLite metadata paging contract should pass");
}

#[test]
fn opaque_continuation_follows_sqlite_collation() {
    wait_for_store_future(async {
        let pool = SqlitePool::connect("sqlite::memory:")
            .await
            .expect("in-memory SQLite pool should open");
        sqlx::query("CREATE TABLE paging_collation (value TEXT COLLATE NOCASE PRIMARY KEY)")
            .execute(&pool)
            .await
            .expect("collation fixture table should be created");
        sqlx::query("INSERT INTO paging_collation (value) VALUES ('a'), ('B'), ('c')")
            .execute(&pool)
            .await
            .expect("collation fixture values should be inserted");

        let mut connection = pool
            .acquire()
            .await
            .expect("collation fixture connection should be acquired");
        let transaction_id = StoreTransactionId::new_random();
        let mut cursor = PageCursor::new(());
        let mut actual = Vec::new();
        let mut page_calls = 0;
        while cursor.has_more() {
            page_calls += 1;
            assert!(page_calls <= 4, "collation paging should terminate");
            let mut batch = VecPageBatch::bounded(NonZeroUsize::new(1).unwrap());
            let mut page = cursor
                .begin_page::<SqliteTextPageContinuation, _>(transaction_id, &mut batch)
                .expect("collation page should begin");
            let mut query_builder =
                QueryBuilder::<Sqlite>::new("SELECT value FROM paging_collation WHERE 1 = 1");
            push_text_page_window(&mut query_builder, &page, "value COLLATE NOCASE");
            let values = query_builder
                .build_query_scalar::<String>()
                .fetch_all(&mut *connection)
                .await
                .expect("collation page should load");
            let mut continuation = None;
            for value in values {
                page.push(value.clone())
                    .expect("collation value should enter the batch");
                continuation = Some(SqliteTextPageContinuation::new(value));
            }
            finish_page(page, continuation).expect("collation page should finish");
            actual.extend(batch.into_values());
        }

        assert_eq!(actual, ["a", "B", "c"]);
        let mut rust_order = actual.clone();
        rust_order.sort();
        assert_ne!(
            actual, rust_order,
            "fixture must disagree with Rust ordering"
        );
    });
}

#[test]
fn full_sqlite_page_requires_a_continuation() {
    let transaction_id = StoreTransactionId::new_random();
    let mut cursor = PageCursor::new(());
    let mut batch = VecPageBatch::bounded(NonZeroUsize::new(1).unwrap());
    let mut page = cursor
        .begin_page::<SqliteTextPageContinuation, _>(transaction_id, &mut batch)
        .expect("SQLite page should begin");
    page.push(1_u32).expect("record should enter the batch");

    assert!(matches!(
        finish_page(page, None),
        Err(PageError::MissingContinuation)
    ));
    assert!(batch.is_empty());
    assert!(cursor.is_failed());
}

#[test]
fn sqlite_store_roundtrips_local_member_private_keys() {
    let provisioner = in_memory_provisioner();
    let record = LocalMemberPrivateKeysRecord {
        member_id: local_member(),
        private_keys: EncryptedLocalMemberPrivateKeys {
            secret: sample_encrypted_secret(42),
        },
    };

    let mut transaction =
        wait_for_store_future(provisioner.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.ensure_local_member_private_keys(record.clone()))
        .expect("private keys should store");
    wait_for_store_future(transaction.ensure_local_member_private_keys(record.clone()))
        .expect("same private keys should be accepted");
    let loaded =
        wait_for_store_future(transaction.load_local_member_private_keys(&record.member_id))
            .expect("private keys should load")
            .expect("private keys should exist");
    assert_eq!(loaded, record);
    wait_for_store_future(transaction.commit()).expect("commit should succeed");

    let store =
        wait_for_store_future(provisioner.into_replication_store()).expect("store should activate");

    let mut read_transaction =
        wait_for_store_future(store.begin_read_transaction()).expect("transaction should start");
    let loaded =
        wait_for_store_future(read_transaction.load_local_member_private_keys(&record.member_id))
            .expect("private keys should load through read transaction")
            .expect("private keys should exist");
    assert_eq!(loaded, record);
    wait_for_store_future(read_transaction.release()).expect("release should succeed");
    wait_for_store_future(store.close()).expect("store should close");
}

#[test]
fn sqlite_store_rejects_conflicting_local_member_private_keys() {
    let provisioner = in_memory_provisioner();
    let record = LocalMemberPrivateKeysRecord {
        member_id: local_member(),
        private_keys: EncryptedLocalMemberPrivateKeys {
            secret: sample_encrypted_secret(42),
        },
    };
    let conflicting_record = LocalMemberPrivateKeysRecord {
        member_id: record.member_id.clone(),
        private_keys: EncryptedLocalMemberPrivateKeys {
            secret: sample_encrypted_secret(43),
        },
    };

    let mut transaction =
        wait_for_store_future(provisioner.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.ensure_local_member_private_keys(record.clone()))
        .expect("private keys should store");
    let error =
        wait_for_store_future(transaction.ensure_local_member_private_keys(conflicting_record))
            .expect_err("conflicting private keys should fail");
    assert!(is_conflicting_member_security_material(
        &error,
        "local member private keys",
        &record.member_id,
    ));
    wait_for_store_future(transaction.rollback()).expect("rollback should succeed");
}

#[test]
fn sqlite_store_rejects_private_keys_for_a_second_local_identity() {
    let provisioner = in_memory_provisioner();
    let first_member = local_member();
    let second_member = remote_member();
    let mut transaction =
        wait_for_store_future(provisioner.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.ensure_local_member_private_keys(
        LocalMemberPrivateKeysRecord {
            member_id: first_member.clone(),
            private_keys: EncryptedLocalMemberPrivateKeys {
                secret: sample_encrypted_secret(42),
            },
        },
    ))
    .expect("first local identity should store");

    let error = wait_for_store_future(transaction.ensure_local_member_private_keys(
        LocalMemberPrivateKeysRecord {
            member_id: second_member.clone(),
            private_keys: EncryptedLocalMemberPrivateKeys {
                secret: sample_encrypted_secret(43),
            },
        },
    ))
    .expect_err("second local identity should fail");
    assert!(matches!(
        error,
        StoreError::StoreExternal { ref source, .. }
            if matches!(
                source.downcast_ref::<SqliteStoreError>(),
                Some(SqliteStoreError::ConflictingLocalMemberIdentity {
                    existing,
                    requested,
                }) if existing == &first_member && requested == &second_member
            )
    ));
    wait_for_store_future(transaction.rollback()).expect("transaction should roll back");
}

#[test]
fn sqlite_store_roundtrips_member_public_keys_and_trust_evidence() {
    let store = in_memory_store(local_member());
    let record =
        MemberPublicKeysRecord::from_public_keys(&test_public_member_keys(&remote_member()));
    let evidence = MemberKeyTrustEvidenceRecord {
        key_id: record.key_id.clone(),
        evidence_kind: MemberKeyTrustEvidenceKind::LocalExplicitTrust,
    };

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.ensure_member_public_keys(record.clone()))
        .expect("member public keys should store");
    wait_for_store_future(transaction.ensure_member_public_keys(record.clone()))
        .expect("same member public keys should be accepted");
    wait_for_store_future(transaction.ensure_member_key_trust_evidence(evidence.clone()))
        .expect("trust evidence should store");
    wait_for_store_future(transaction.ensure_member_key_trust_evidence(evidence.clone()))
        .expect("same trust evidence should be accepted");
    let loaded = wait_for_store_future(transaction.load_member_public_keys(&record.key_id))
        .expect("member public keys should load")
        .expect("member public keys should exist");
    assert_eq!(loaded, record);
    let loaded_for_member = wait_for_store_future(
        transaction.load_member_public_keys_for_member(&record.key_id.member_id),
    )
    .expect("member public keys should load by member");
    assert_eq!(loaded_for_member, vec![record.clone()]);
    let loaded_evidence =
        wait_for_store_future(transaction.load_member_key_trust_evidence(&record.key_id))
            .expect("trust evidence should load");
    assert!(loaded_evidence.contains(MemberKeyTrustEvidenceKind::LocalExplicitTrust));
    wait_for_store_future(transaction.commit()).expect("commit should succeed");

    let mut read_transaction =
        wait_for_store_future(store.begin_read_transaction()).expect("transaction should start");
    let loaded = wait_for_store_future(read_transaction.load_member_public_keys(&record.key_id))
        .expect("member public keys should load through read transaction")
        .expect("member public keys should exist");
    assert_eq!(loaded, record);
    wait_for_store_future(read_transaction.release()).expect("release should succeed");
}

#[test]
fn sqlite_store_lists_all_member_public_key_ids() {
    let store = in_memory_store(local_member());
    let remote_first =
        MemberPublicKeysRecord::from_public_keys(&test_public_member_keys(&remote_member()));
    let mut local =
        MemberPublicKeysRecord::from_public_keys(&test_public_member_keys(&third_member()));
    local.key_id.member_id = local_member();
    let mut remote_second =
        MemberPublicKeysRecord::from_public_keys(&test_public_member_keys(&third_member()));
    remote_second.key_id.member_id = remote_member();
    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    let initial = wait_for_store_future(transaction.load_member_public_key_ids())
        .expect("provisioned member-key ids should load");
    let [initial] = initial.as_slice() else {
        panic!("exactly one provisioned member-key id should exist: {initial:?}");
    };
    assert_eq!(initial.member_id, local_member());
    let expected = HashSet::from([
        initial.clone(),
        remote_first.key_id.clone(),
        local.key_id.clone(),
        remote_second.key_id.clone(),
    ]);
    wait_for_store_future(transaction.ensure_member_public_keys(remote_first.clone()))
        .expect("first remote public keys should store");
    wait_for_store_future(transaction.ensure_member_public_keys(local))
        .expect("local public keys should store");
    wait_for_store_future(transaction.ensure_member_public_keys(remote_second))
        .expect("second remote public keys should store");

    let loaded = wait_for_store_future(transaction.load_member_public_key_ids())
        .expect("member-key ids should load")
        .into_iter()
        .collect::<HashSet<_>>();

    assert_eq!(loaded, expected);
    wait_for_store_future(transaction.rollback()).expect("rollback should succeed");
}

#[test]
fn sqlite_store_rejects_member_public_keys_with_mismatched_fingerprint() {
    let store = in_memory_store(local_member());
    let mut record =
        MemberPublicKeysRecord::from_public_keys(&test_public_member_keys(&remote_member()));
    record.key_id.fingerprint = test_public_member_keys(&local_member()).fingerprint();

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    let error = wait_for_store_future(transaction.ensure_member_public_keys(record.clone()))
        .expect_err("mismatched member public keys should fail");
    assert!(is_conflicting_member_security_material(
        &error,
        "member public keys fingerprint",
        &record.key_id.member_id,
    ));
    wait_for_store_future(transaction.rollback()).expect("rollback should succeed");
}

#[test]
fn sqlite_store_roundtrips_blocked_key_fingerprints() {
    let store = in_memory_store(local_member());
    let fingerprint = test_public_member_keys(&remote_member()).fingerprint();

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    assert!(
        !wait_for_store_future(transaction.is_key_fingerprint_blocked(&fingerprint))
            .expect("blocked fingerprint should load")
    );
    wait_for_store_future(transaction.ensure_blocked_key_fingerprint(fingerprint))
        .expect("blocked fingerprint should store");
    wait_for_store_future(transaction.ensure_blocked_key_fingerprint(fingerprint))
        .expect("same blocked fingerprint should be accepted");
    assert!(
        wait_for_store_future(transaction.is_key_fingerprint_blocked(&fingerprint))
            .expect("blocked fingerprint should load")
    );
    wait_for_store_future(transaction.rollback()).expect("rollback should succeed");
}

/// Store and records shared by one projected-update paging scenario.
struct UpdatePagingFixture {
    /// SQLite owner retained across committed and read transactions.
    store: TestSqliteStore,
    /// Group whose update log is under test.
    group_id: GroupId,
    /// Dataset carried by every fixture update.
    dataset_id: DatasetId,
    /// Expected update order: Alice 1, Bob 1, Alice 2, Alice 3, Bob maximum.
    updates: [ReplicationUpdateRecord; 5],
}

impl UpdatePagingFixture {
    /// Build updates with a same-version producer tie and the maximum supported version.
    fn new() -> Self {
        let dataset_id = docs_dataset_id();
        let schema = title_schema();
        let store = in_memory_store(local_member());
        let group_id = GroupId(Uuid::from_u128(10_011));
        let encoded_operation = encoded_insert_snapshot("range query", &schema);
        let update = |node_index, version, sender, applied_locally| ReplicationUpdateRecord {
            group_id,
            update_id: UpdateId {
                node_index,
                version,
            },
            sender,
            read_versions: VersionVector::initial(NonZeroUsize::new(2).unwrap()),
            dataset_updates: vec![DatasetUpdateRecord {
                dataset_id: dataset_id.clone(),
                operations: vec![encoded_operation.clone()],
            }],
            applied_locally,
        };
        let alice_v1 = update(0, 1, local_member(), false);
        let alice_v2 = update(0, 2, local_member(), true);
        let alice_v3 = update(0, 3, local_member(), true);
        let bob_v1 = update(1, 1, remote_member(), true);
        let bob_max = update(1, MAX_VERSION_VALUE, remote_member(), false);
        Self {
            store,
            group_id,
            dataset_id,
            updates: [alice_v1, bob_v1, alice_v2, alice_v3, bob_max],
        }
    }

    /// Return the identities in the expected composite SQL order.
    fn expected_ids(&self) -> Vec<UpdateId> {
        self.updates.iter().map(|update| update.update_id).collect()
    }
}

/// Insert all fixture updates into the transaction used for the paging checks.
fn insert_update_paging_fixture(
    transaction: &mut dyn ReplicationStoreTransaction,
    fixture: &UpdatePagingFixture,
) {
    let group = sample_group(fixture.group_id);
    wait_for_store_future(transaction.insert_replication_group(group)).expect("group should store");
    for update in fixture.updates.iter().cloned() {
        wait_for_store_future(transaction.append_replication_update(update))
            .expect("update should store");
    }
}

#[test]
fn sqlite_store_pages_projected_replication_updates_and_ids() {
    let fixture = UpdatePagingFixture::new();
    let mut transaction =
        wait_for_store_future(fixture.store.begin_transaction()).expect("transaction should start");
    insert_update_paging_fixture(transaction.as_mut(), &fixture);
    assert_projected_update_pages(transaction.as_mut(), &fixture);
    assert_bounded_update_id_pages(transaction.as_mut(), &fixture);
    assert_update_filters_and_legacy_limit(transaction.as_mut(), &fixture);
    wait_for_store_future(transaction.commit()).expect("commit should succeed");
    assert_inconsistent_update_payload(&fixture);
}

/// Check inline and Vec projections, changed limits, and exact-page exhaustion.
fn assert_projected_update_pages(
    transaction: &mut dyn ReplicationStoreTransaction,
    fixture: &UpdatePagingFixture,
) {
    let mut projected_cursor = PageCursor::new(ReplicationUpdatesQuery::new(
        fixture.group_id,
        ReplicationUpdateFilter::All,
    ));
    let mut projected = Vec::new();
    let mut first_batch =
        InlinePageBatch::<ProjectedUpdateSummary, (), 1, ReplicationUpdatePageInput, _>::new_with(
            project_update_summary,
        );
    wait_for_store_future(
        transaction.load_replication_updates_into(&mut projected_cursor, &mut first_batch),
    )
    .expect("first projected update page should load");
    assert_eq!(first_batch.values().len(), 1);
    projected.extend_from_slice(first_batch.values());

    let mut final_page_len = usize::MAX;
    while projected_cursor.has_more() {
        let mut batch =
            VecPageBatch::<ProjectedUpdateSummary, (), ReplicationUpdatePageInput, _>::bounded_with(
                NonZeroUsize::new(2).expect("two updates per page"),
                project_update_summary,
            );
        wait_for_store_future(
            transaction.load_replication_updates_into(&mut projected_cursor, &mut batch),
        )
        .expect("continued projected update page should load");
        final_page_len = batch.values().len();
        projected.extend(batch.into_values());
    }
    assert_eq!(
        final_page_len, 0,
        "an exact final page needs an empty confirmation"
    );
    assert_eq!(
        projected
            .iter()
            .map(|summary| summary.update_id)
            .collect::<Vec<_>>(),
        fixture.expected_ids()
    );
    assert!(projected.iter().all(|summary| {
        summary.dataset_id == fixture.dataset_id.as_str() && summary.operation_count == 1
    }));
    assert_eq!(
        projected
            .iter()
            .map(|summary| summary.applied_locally)
            .collect::<Vec<_>>(),
        vec![false, true, true, true, false]
    );

    let selected_ids = HashSet::from([fixture.updates[2].update_id, fixture.updates[4].update_id]);
    let query = ReplicationUpdatesQuery::new(fixture.group_id, ReplicationUpdateFilter::All)
        .with_update_ids(&selected_ids);
    let mut selected_cursor = PageCursor::new(query);
    let mut selected_batch =
        VecPageBatch::<ProjectedUpdateSummary, (), ReplicationUpdatePageInput, _>::bounded_with(
            NonZeroUsize::new(1).expect("one update per page"),
            project_update_summary,
        );
    let mut selected = Vec::new();
    while selected_cursor.has_more() {
        wait_for_store_future(
            transaction.load_replication_updates_into(&mut selected_cursor, &mut selected_batch),
        )
        .expect("selected update page should load");
        selected.extend(
            selected_batch
                .values()
                .iter()
                .map(|summary| summary.update_id),
        );
    }
    assert_eq!(
        selected,
        vec![fixture.updates[2].update_id, fixture.updates[4].update_id]
    );
}

/// Check that lightweight ID reads cross the same-version producer boundary.
fn assert_bounded_update_id_pages(
    transaction: &mut dyn ReplicationStoreTransaction,
    fixture: &UpdatePagingFixture,
) {
    let mut ids_cursor = PageCursor::new(ReplicationUpdatesQuery::new(
        fixture.group_id,
        ReplicationUpdateFilter::All,
    ));
    let mut ids_batch =
        VecPageBatch::<UpdateId, ()>::bounded(NonZeroUsize::new(1).expect("one update per page"));
    let mut paged_ids = Vec::new();
    while ids_cursor.has_more() {
        wait_for_store_future(
            transaction.load_replication_update_ids_into(&mut ids_cursor, &mut ids_batch),
        )
        .expect("update ids should load");
        paged_ids.extend_from_slice(ids_batch.values());
    }
    assert!(ids_cursor.is_exhausted());
    assert_eq!(paged_ids, fixture.expected_ids());

    let selected_ids = HashSet::from([fixture.updates[2].update_id]);
    let query = ReplicationUpdatesQuery::new(fixture.group_id, ReplicationUpdateFilter::All)
        .with_update_ids(&selected_ids);
    let mut selected_cursor = PageCursor::new(query);
    wait_for_store_future(
        transaction.load_replication_update_ids_into(&mut selected_cursor, &mut ids_batch),
    )
    .expect("selected update id should load");
    assert_eq!(ids_batch.values(), &[fixture.updates[2].update_id]);
}

/// Compare projected adapters and ID selection for every filter and one legacy limit.
fn assert_update_filters_and_legacy_limit(
    transaction: &mut dyn ReplicationStoreTransaction,
    fixture: &UpdatePagingFixture,
) {
    let [alice_v1, bob_v1, alice_v2, alice_v3, bob_max] = &fixture.updates;
    let filter_cases = [
        (
            ReplicationUpdateFilter::PendingApply,
            vec![alice_v1.update_id, bob_max.update_id],
        ),
        (
            ReplicationUpdateFilter::Applied,
            vec![bob_v1.update_id, alice_v2.update_id, alice_v3.update_id],
        ),
        (
            ReplicationUpdateFilter::ProducerRange {
                producer_index: MemberIndex::new(0),
                start_version: 2,
                end_version: 3,
            },
            vec![alice_v2.update_id, alice_v3.update_id],
        ),
        (
            ReplicationUpdateFilter::ProducerRange {
                producer_index: MemberIndex::new(0),
                start_version: 3,
                end_version: 2,
            },
            Vec::new(),
        ),
    ];
    for (filter, expected) in filter_cases {
        let updates = wait_for_store_future(transaction.load_replication_updates(
            &fixture.group_id,
            filter,
            None,
        ))
        .expect("filtered updates should load");
        let ids = wait_for_store_future(transaction.load_replication_update_ids(
            &fixture.group_id,
            filter,
            None,
        ))
        .expect("filtered update ids should load");
        let update_ids = updates
            .iter()
            .map(|update| update.update_id)
            .sorted()
            .collect::<Vec<_>>();
        assert_eq!(
            update_ids,
            expected.iter().copied().sorted().collect::<Vec<_>>()
        );
        assert_eq!(ids.into_iter().sorted().collect::<Vec<_>>(), update_ids);
    }

    let limited_alice = wait_for_store_future(transaction.load_replication_updates(
        &fixture.group_id,
        ReplicationUpdateFilter::ProducerRange {
            producer_index: MemberIndex::new(0),
            start_version: 2,
            end_version: 3,
        },
        NonZeroUsize::new(1),
    ))
    .expect("bounded compatibility query should load");
    assert_eq!(limited_alice, vec![alice_v2.clone()]);
}

/// Confirm ID-only reads skip a corrupt payload while full reads report its mismatch.
fn assert_inconsistent_update_payload(fixture: &UpdatePagingFixture) {
    let [_, _, alice_v2, alice_v3, _] = &fixture.updates;
    let mismatched_payload = UpdateMessageProtoSource::from(alice_v3).encode_proto_to_vec();
    replace_raw_update_message(
        &fixture.store,
        fixture.group_id,
        alice_v2.update_id,
        mismatched_payload,
    );
    let mut transaction =
        wait_for_store_future(fixture.store.begin_transaction()).expect("transaction should start");
    let ids = wait_for_store_future(transaction.load_replication_update_ids(
        &fixture.group_id,
        ReplicationUpdateFilter::All,
        None,
    ))
    .expect("id-only paging should not decode an inconsistent payload");
    assert_eq!(
        ids.into_iter().sorted().collect::<Vec<_>>(),
        fixture
            .expected_ids()
            .into_iter()
            .sorted()
            .collect::<Vec<_>>()
    );
    let selected_ids = HashSet::from([alice_v3.update_id]);
    let query = ReplicationUpdatesQuery::new(fixture.group_id, ReplicationUpdateFilter::All)
        .with_update_ids(&selected_ids);
    let mut selected_cursor = PageCursor::new(query);
    let mut selected_batch =
        VecPageBatch::<ProjectedUpdateSummary, (), ReplicationUpdatePageInput, _>::unlimited_with(
            project_update_summary,
        );
    wait_for_store_future(
        transaction.load_replication_updates_into(&mut selected_cursor, &mut selected_batch),
    )
    .expect("an unselected inconsistent payload should not be decoded");
    assert_eq!(
        selected_batch
            .values()
            .iter()
            .map(|summary| summary.update_id)
            .collect::<Vec<_>>(),
        vec![alice_v3.update_id]
    );
    let error = wait_for_store_future(transaction.load_replication_updates(
        &fixture.group_id,
        ReplicationUpdateFilter::All,
        None,
    ))
    .expect_err("projected update paging should validate indexed payload identity");
    assert_sqlite_store_error(&error, |error| {
        matches!(
            error,
            SqliteStoreError::StoredUpdateIdMismatch {
                expected_update_id,
                actual_update_id,
            } if *expected_update_id == alice_v2.update_id
                && *actual_update_id == alice_v3.update_id
        )
    });
    wait_for_store_future(transaction.rollback()).expect("rollback should succeed");
}

#[test]
fn sqlite_store_roundtrips_tombstoned_dataset_rows() {
    let dataset_id = docs_dataset_id();
    let schema = title_schema();
    let store = in_memory_store(local_member());
    let group_id = GroupId(Uuid::from_u128(104));
    let row_key = RowKey(Uuid::from_u128(204));
    let mut source_data = flotsync_messages::InMemoryStateData::new(schema.clone());
    let operation = source_data
        .insert_row(
            UpdateId {
                node_index: 0,
                version: 1,
            },
            row_key.0,
            vec![
                schema
                    .columns
                    .get("title")
                    .expect("title field should exist")
                    .initial("deleted")
                    .expect("field value should build"),
            ],
        )
        .expect("row insert should succeed");
    let RowOperation::Insert { snapshot, .. } = &operation.operation else {
        panic!("expected insert operation");
    };
    let stored_row = ReplicationRowStateFixture {
        row_id: row_key,
        snapshot: snapshot.clone().into_owned(),
        tombstoned: true,
        created_by: None,
        last_changed_versions: sample_last_changed_versions(),
    };

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.insert_replication_group(sample_group(group_id)))
        .expect("group should store");
    wait_for_store_future(apply_row_patch(
        transaction.as_mut(),
        &schema,
        DatasetRowStatePatch {
            group_id,
            dataset_id: dataset_id.clone(),
            actions: vec![DatasetRowStateWrite::UpsertTombstone {
                row_key,
                snapshot: stored_row.snapshot.clone(),
            }],
            change_id: sample_change_id(),
            last_changed_versions: stored_row.last_changed_versions.clone(),
        },
    ))
    .expect("row patch should store");
    wait_for_store_future(transaction.commit()).expect("commit should succeed");

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    let requested_row_keys = [row_key];
    let mut requested_row_keys = requested_row_keys.iter();
    let loaded_rows = wait_for_store_future(transaction.load_dataset_rows(
        GroupDatasetSchemaRef {
            group_id: &group_id,
            dataset_id: &dataset_id,
            schema: &schema,
        },
        &mut requested_row_keys,
    ))
    .expect("row slice should load");
    wait_for_store_future(transaction.commit()).expect("commit should succeed");

    assert_eq!(loaded_row_fixture(&loaded_rows, row_key), Some(stored_row));
}

#[test]
fn sqlite_store_scans_dataset_rows_in_key_order() {
    let dataset_id = docs_dataset_id();
    let schema = title_schema();
    let store = in_memory_store(local_member());
    let group_id = GroupId(Uuid::from_u128(106));
    let first_row_key = RowKey(Uuid::from_u128(206));
    let second_row_key = RowKey(Uuid::from_u128(207));

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.insert_replication_group(sample_group(group_id)))
        .expect("group should store");
    wait_for_store_future(apply_row_patch(
        transaction.as_mut(),
        &schema,
        DatasetRowStatePatch {
            group_id,
            dataset_id: dataset_id.clone(),
            actions: vec![
                DatasetRowStateWrite::UpsertTombstone {
                    row_key: second_row_key,
                    snapshot: title_snapshot(&schema, second_row_key, "second"),
                },
                DatasetRowStateWrite::UpsertActive {
                    row_key: first_row_key,
                    snapshot: title_snapshot(&schema, first_row_key, "first"),
                },
            ],
            change_id: sample_change_id(),
            last_changed_versions: sample_last_changed_versions(),
        },
    ))
    .expect("rows should store");
    wait_for_store_future(transaction.commit()).expect("transaction should commit");

    let mut transaction =
        wait_for_store_future(store.begin_read_transaction()).expect("read should start");
    let dataset = GroupDatasetSchemaRef {
        group_id: &group_id,
        dataset_id: &dataset_id,
        schema: &schema,
    };
    let mut cursor = PageCursor::new(DatasetRowsQuery::borrowed(dataset));
    let mut first_page = DatasetRowPageBatch::bounded(
        &schema,
        NonZeroUsize::new(1).expect("limit should be non-zero"),
    );
    wait_for_store_future(transaction.scan_dataset_rows_into(&mut cursor, &mut first_page))
        .expect("first batch should scan");
    let first_scanned_metadata = first_page
        .rows()
        .row(0)
        .expect("first scan should return one row")
        .metadata()
        .clone();
    assert!(cursor.has_more());
    let mut second_page = DatasetRowPageBatch::bounded(
        &schema,
        NonZeroUsize::new(2).expect("rotated limit should be non-zero"),
    );
    wait_for_store_future(transaction.scan_dataset_rows_into(&mut cursor, &mut second_page))
        .expect("second batch should scan");
    let second_scanned_metadata = second_page
        .rows()
        .row(0)
        .expect("second scan should return one row")
        .metadata()
        .clone();
    assert!(cursor.is_exhausted());
    wait_for_store_future(transaction.release()).expect("read should release");

    assert!(
        second_page
            .metadata()
            .expect("successful second page should retain metadata")
            .dataset_exists
    );
    assert_eq!(first_scanned_metadata.row_key, first_row_key);
    assert!(!first_scanned_metadata.tombstoned);
    assert_eq!(first_scanned_metadata.created_by, Some(sample_change_id()));
    assert_eq!(
        first_scanned_metadata.last_changed_versions,
        sample_last_changed_versions()
    );
    assert_eq!(second_scanned_metadata.row_key, second_row_key);
    assert!(second_scanned_metadata.tombstoned);
    assert_eq!(second_scanned_metadata.created_by, None);
    assert_eq!(
        second_scanned_metadata.last_changed_versions,
        sample_last_changed_versions()
    );
}

#[test]
fn sqlite_store_scan_clears_reused_rows_for_missing_dataset() {
    let dataset_id = docs_dataset_id();
    let missing_dataset_id =
        DatasetId::try_from_static("missing").expect("test dataset id should be valid");
    let schema = title_schema();
    let store = in_memory_store(local_member());
    let group_id = GroupId(Uuid::from_u128(1_106));
    let row_key = RowKey(Uuid::from_u128(1_206));

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.insert_replication_group(sample_group(group_id)))
        .expect("group should store");
    wait_for_store_future(apply_row_patch(
        transaction.as_mut(),
        &schema,
        DatasetRowStatePatch {
            group_id,
            dataset_id: dataset_id.clone(),
            actions: vec![DatasetRowStateWrite::UpsertActive {
                row_key,
                snapshot: title_snapshot(&schema, row_key, "present"),
            }],
            change_id: sample_change_id(),
            last_changed_versions: sample_last_changed_versions(),
        },
    ))
    .expect("row should store");
    wait_for_store_future(transaction.commit()).expect("transaction should commit");

    let mut transaction =
        wait_for_store_future(store.begin_read_transaction()).expect("read should start");
    let limit = NonZeroUsize::new(8).expect("limit should be non-zero");
    let mut state_rows = DatasetRowPageBatch::bounded(&schema, limit);
    let mut existing_cursor = PageCursor::new(DatasetRowsQuery::borrowed(GroupDatasetSchemaRef {
        group_id: &group_id,
        dataset_id: &dataset_id,
        schema: &schema,
    }));
    wait_for_store_future(
        transaction.scan_dataset_rows_into(&mut existing_cursor, &mut state_rows),
    )
    .expect("existing dataset should scan");
    assert_eq!(state_rows.rows().len(), 1);

    let mut missing_cursor = PageCursor::new(DatasetRowsQuery::borrowed(GroupDatasetSchemaRef {
        group_id: &group_id,
        dataset_id: &missing_dataset_id,
        schema: &schema,
    }));
    wait_for_store_future(transaction.scan_dataset_rows_into(&mut missing_cursor, &mut state_rows))
        .expect("missing dataset should return an empty page");
    assert!(
        !state_rows
            .metadata()
            .expect("successful missing-dataset page should retain metadata")
            .dataset_exists
    );
    assert!(missing_cursor.is_exhausted());
    assert!(state_rows.rows().is_empty());

    assert_requested_rows_for_missing_dataset(
        transaction.as_mut(),
        group_id,
        &missing_dataset_id,
        &schema,
        row_key,
    );

    wait_for_store_future(transaction.release()).expect("read should release");
}

#[test]
fn sqlite_store_scan_rejects_malformed_row_snapshot_without_publishing_metadata() {
    let dataset_id = docs_dataset_id();
    let schema = title_schema();
    let store = in_memory_store(local_member());
    let group_id = GroupId(Uuid::from_u128(2_106));
    let row_key = RowKey(Uuid::from_u128(2_206));

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.insert_replication_group(sample_group(group_id)))
        .expect("group should store");
    wait_for_store_future(apply_row_patch(
        transaction.as_mut(),
        &schema,
        DatasetRowStatePatch {
            group_id,
            dataset_id: dataset_id.clone(),
            actions: vec![DatasetRowStateWrite::UpsertActive {
                row_key,
                snapshot: title_snapshot(&schema, row_key, "corrupt me"),
            }],
            change_id: sample_change_id(),
            last_changed_versions: sample_last_changed_versions(),
        },
    ))
    .expect("row should store");
    wait_for_store_future(transaction.commit()).expect("transaction should commit");
    replace_raw_row_snapshot(&store, group_id, &dataset_id, row_key, vec![0xff]);

    let mut transaction =
        wait_for_store_future(store.begin_read_transaction()).expect("read should start");
    let mut cursor = PageCursor::new(DatasetRowsQuery::borrowed(GroupDatasetSchemaRef {
        group_id: &group_id,
        dataset_id: &dataset_id,
        schema: &schema,
    }));
    let mut state_rows = DatasetRowPageBatch::bounded(
        &schema,
        NonZeroUsize::new(1).expect("limit should be non-zero"),
    );
    let result =
        wait_for_store_future(transaction.scan_dataset_rows_into(&mut cursor, &mut state_rows));
    wait_for_store_future(transaction.release()).expect("read should release");

    assert!(result.is_err());
    assert!(state_rows.rows().is_empty());
    assert!(state_rows.metadata().is_none());
    assert!(cursor.is_failed());
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "This transition-scan contract scenario keeps both stored sides and their pagination assertions together."
)]
fn sqlite_store_scans_dataset_row_transitions_in_key_order() {
    let dataset_id = docs_dataset_id();
    let previous_schema = title_schema();
    let current_schema = heading_schema();
    let store = in_memory_store(local_member());
    let previous_group_id = GroupId(Uuid::from_u128(107));
    let current_group_id = GroupId(Uuid::from_u128(108));
    let previous_only = RowKey(Uuid::from_u128(301));
    let current_only = RowKey(Uuid::from_u128(302));
    let corresponding = RowKey(Uuid::from_u128(303));
    let previous_change_id = UpdateId {
        node_index: 0,
        version: 1,
    };
    let current_change_id = UpdateId {
        node_index: 1,
        version: 1,
    };

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.insert_replication_group(sample_group(previous_group_id)))
        .expect("previous group should store");
    wait_for_store_future(transaction.insert_replication_group(sample_group(current_group_id)))
        .expect("current group should store");
    wait_for_store_future(apply_row_patch(
        transaction.as_mut(),
        &previous_schema,
        DatasetRowStatePatch {
            group_id: previous_group_id,
            dataset_id: dataset_id.clone(),
            actions: vec![
                DatasetRowStateWrite::UpsertActive {
                    row_key: corresponding,
                    snapshot: title_snapshot(
                        &previous_schema,
                        corresponding,
                        "previous corresponding",
                    ),
                },
                DatasetRowStateWrite::UpsertActive {
                    row_key: previous_only,
                    snapshot: title_snapshot(&previous_schema, previous_only, "previous only"),
                },
            ],
            change_id: previous_change_id,
            last_changed_versions: sample_last_changed_versions(),
        },
    ))
    .expect("previous rows should store");
    wait_for_store_future(apply_row_patch(
        transaction.as_mut(),
        &current_schema,
        DatasetRowStatePatch {
            group_id: current_group_id,
            dataset_id: dataset_id.clone(),
            actions: vec![
                DatasetRowStateWrite::UpsertActive {
                    row_key: current_only,
                    snapshot: heading_snapshot(&current_schema, current_only, "current only"),
                },
                DatasetRowStateWrite::UpsertTombstone {
                    row_key: corresponding,
                    snapshot: heading_snapshot(&current_schema, corresponding, "current tombstone"),
                },
            ],
            change_id: current_change_id,
            last_changed_versions: sample_last_changed_versions(),
        },
    ))
    .expect("current rows should store");
    wait_for_store_future(transaction.commit()).expect("transaction should commit");

    let mut transaction =
        wait_for_store_future(store.begin_read_transaction()).expect("read should start");
    let previous_group = GroupDatasetSchemaRef {
        group_id: &previous_group_id,
        dataset_id: &dataset_id,
        schema: &previous_schema,
    };
    let current_group = GroupDatasetSchemaRef {
        group_id: &current_group_id,
        dataset_id: &dataset_id,
        schema: &current_schema,
    };
    let mut cursor = PageCursor::new(DatasetRowTransitionQuery::new(
        DatasetRowsQuery::borrowed(previous_group),
        DatasetRowsQuery::borrowed(current_group),
    ));
    let mut transition_rows = DatasetRowTransitionPageBatch::bounded(
        &previous_schema,
        &current_schema,
        NonZeroUsize::new(2).expect("limit should be non-zero"),
    );
    wait_for_store_future(
        transaction.scan_dataset_row_transitions_into(&mut cursor, &mut transition_rows),
    )
    .expect("first transition batch should scan");
    let first_batch = transition_rows
        .metadata()
        .expect("first transition batch should retain metadata")
        .clone();
    let first_row_presence = transition_rows
        .transitions()
        .rows()
        .map(|transition| {
            (
                transition.row_key(),
                transition.previous().is_some(),
                transition.current().is_some(),
            )
        })
        .collect::<Vec<_>>();
    assert!(cursor.has_more());
    wait_for_store_future(
        transaction.scan_dataset_row_transitions_into(&mut cursor, &mut transition_rows),
    )
    .expect("second transition batch should scan");
    wait_for_store_future(transaction.release()).expect("read should release");

    assert_eq!(first_batch.dataset_id, dataset_id);
    assert_eq!(first_batch.previous_group_id, previous_group_id);
    assert_eq!(first_batch.current_group_id, current_group_id);
    assert!(first_batch.previous_dataset_exists);
    assert!(first_batch.current_dataset_exists);
    assert_eq!(
        first_row_presence,
        vec![(previous_only, true, false), (current_only, false, true)]
    );
    assert_eq!(transition_rows.transitions().len(), 1);
    let transition = transition_rows
        .transitions()
        .row(0)
        .expect("transition must exist");
    assert_eq!(transition.row_key(), corresponding);
    assert_eq!(
        transition.previous().map(|row| row.metadata().created_by),
        Some(Some(previous_change_id))
    );
    assert_eq!(
        transition
            .current()
            .map(|row| (row.metadata().created_by, row.metadata().tombstoned)),
        Some((None, true))
    );
    assert!(cursor.is_exhausted());
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one store lifecycle covers dataset absence and exact-limit continuation together"
)]
fn sqlite_store_transition_scan_reports_missing_dataset_and_exact_limit_exhaustion() {
    let dataset_id = docs_dataset_id();
    let schema = title_schema();
    let store = in_memory_store(local_member());
    let previous_group_id = GroupId(Uuid::from_u128(110));
    let current_group_id = GroupId(Uuid::from_u128(111));
    let row_key = RowKey(Uuid::from_u128(305));

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.insert_replication_group(sample_group(previous_group_id)))
        .expect("previous group should store");
    wait_for_store_future(transaction.insert_replication_group(sample_group(current_group_id)))
        .expect("current group should store");
    wait_for_store_future(apply_row_patch(
        transaction.as_mut(),
        &schema,
        DatasetRowStatePatch {
            group_id: previous_group_id,
            dataset_id: dataset_id.clone(),
            actions: vec![DatasetRowStateWrite::UpsertActive {
                row_key,
                snapshot: title_snapshot(&schema, row_key, "previous"),
            }],
            change_id: sample_change_id(),
            last_changed_versions: sample_last_changed_versions(),
        },
    ))
    .expect("previous row should store");
    wait_for_store_future(transaction.commit()).expect("transaction should commit");

    let previous_group = GroupDatasetSchemaRef {
        group_id: &previous_group_id,
        dataset_id: &dataset_id,
        schema: &schema,
    };
    let current_group = GroupDatasetSchemaRef {
        group_id: &current_group_id,
        dataset_id: &dataset_id,
        schema: &schema,
    };
    let limit = NonZeroUsize::new(1).expect("limit should be non-zero");
    let mut transaction =
        wait_for_store_future(store.begin_read_transaction()).expect("read should start");
    let mut cursor = PageCursor::new(DatasetRowTransitionQuery::new(
        DatasetRowsQuery::borrowed(previous_group),
        DatasetRowsQuery::borrowed(current_group),
    ));
    let mut transition_rows = DatasetRowTransitionPageBatch::bounded(&schema, &schema, limit);
    wait_for_store_future(
        transaction.scan_dataset_row_transitions_into(&mut cursor, &mut transition_rows),
    )
    .expect("transition batch should scan");
    let batch = transition_rows
        .metadata()
        .expect("successful transition batch should retain metadata")
        .clone();
    let first_presence = transition_rows
        .transitions()
        .row(0)
        .map(|row| (row.previous().is_some(), row.current().is_some()));
    assert!(cursor.has_more());
    wait_for_store_future(
        transaction.scan_dataset_row_transitions_into(&mut cursor, &mut transition_rows),
    )
    .expect("exhaustion batch should scan");
    let exhausted_is_empty = transition_rows.transitions().is_empty();
    let empty_dataset_id =
        DatasetId::try_from_static("empty").expect("empty dataset id should build");
    let empty_previous_group = GroupDatasetSchemaRef {
        group_id: &previous_group_id,
        dataset_id: &empty_dataset_id,
        schema: &schema,
    };
    let empty_current_group = GroupDatasetSchemaRef {
        group_id: &current_group_id,
        dataset_id: &empty_dataset_id,
        schema: &schema,
    };
    let mut empty_cursor = PageCursor::new(DatasetRowTransitionQuery::new(
        DatasetRowsQuery::borrowed(empty_previous_group),
        DatasetRowsQuery::borrowed(empty_current_group),
    ));
    wait_for_store_future(
        transaction.scan_dataset_row_transitions_into(&mut empty_cursor, &mut transition_rows),
    )
    .expect("empty transition batch should scan");
    let empty = transition_rows
        .metadata()
        .expect("empty transition batch should retain metadata")
        .clone();
    let empty_is_empty = transition_rows.transitions().is_empty();
    let mut mismatch_cursor = PageCursor::new(DatasetRowTransitionQuery::new(
        DatasetRowsQuery::borrowed(previous_group),
        DatasetRowsQuery::borrowed(empty_current_group),
    ));
    let mismatch = wait_for_store_future(
        transaction.scan_dataset_row_transitions_into(&mut mismatch_cursor, &mut transition_rows),
    )
    .expect_err("different dataset references should be rejected");
    wait_for_store_future(transaction.release()).expect("read should release");

    assert!(batch.previous_dataset_exists);
    assert!(!batch.current_dataset_exists);
    assert_eq!(first_presence, Some((true, false)));
    assert!(exhausted_is_empty);
    assert!(cursor.is_exhausted());
    assert!(!empty.previous_dataset_exists);
    assert!(!empty.current_dataset_exists);
    assert!(empty_is_empty);
    assert!(empty_cursor.is_exhausted());
    assert_eq!(
        mismatch
            .store_error_classification()
            .expect("query mismatch should remain classified")
            .class,
        StoreErrorClass::Contract
    );
}

#[test]
fn sqlite_store_preserves_row_creator_through_updates_and_tombstoning() {
    let dataset_id = docs_dataset_id();
    let schema = title_schema();
    let store = in_memory_store(local_member());
    let group_id = GroupId(Uuid::from_u128(109));
    let row_key = RowKey(Uuid::from_u128(304));
    let first_upper_half_version = 1_u64 << 63;
    let created_by = UpdateId {
        node_index: 0,
        version: first_upper_half_version,
    };
    let updated_by = UpdateId {
        node_index: 0,
        version: first_upper_half_version + 1,
    };
    let deleted_by = UpdateId {
        node_index: 0,
        version: first_upper_half_version + 2,
    };

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.insert_replication_group(sample_group(group_id)))
        .expect("group should store");
    for (change_id, tombstoned, title) in [
        (created_by, false, "created"),
        (updated_by, false, "updated"),
        (deleted_by, true, "deleted"),
    ] {
        let action = if tombstoned {
            DatasetRowStateWrite::UpsertTombstone {
                row_key,
                snapshot: title_snapshot(&schema, row_key, title),
            }
        } else {
            DatasetRowStateWrite::UpsertActive {
                row_key,
                snapshot: title_snapshot(&schema, row_key, title),
            }
        };
        wait_for_store_future(apply_row_patch(
            transaction.as_mut(),
            &schema,
            DatasetRowStatePatch {
                group_id,
                dataset_id: dataset_id.clone(),
                actions: vec![action],
                change_id,
                last_changed_versions: sample_last_changed_versions(),
            },
        ))
        .expect("row change should store");
    }
    wait_for_store_future(transaction.commit()).expect("transaction should commit");

    let mut transaction =
        wait_for_store_future(store.begin_read_transaction()).expect("read should start");
    let requested_row_keys = [row_key];
    let mut requested_row_keys = requested_row_keys.iter();
    let loaded = wait_for_store_future(transaction.load_dataset_rows(
        GroupDatasetSchemaRef {
            group_id: &group_id,
            dataset_id: &dataset_id,
            schema: &schema,
        },
        &mut requested_row_keys,
    ))
    .expect("row should load");
    wait_for_store_future(transaction.release()).expect("read should release");

    let row = loaded
        .state_rows
        .rows()
        .find(|row| row.metadata().row_key == row_key)
        .expect("stored row should be present");
    let row = row.metadata();
    assert_eq!(row.created_by, Some(created_by));
    assert!(row.tombstoned);
}

#[test]
fn sqlite_store_rejects_incomplete_and_out_of_range_row_creators() {
    let dataset_id = docs_dataset_id();
    let schema = title_schema();
    let store = in_memory_store(local_member());
    let group_id = GroupId(Uuid::from_u128(112));
    let row_key = RowKey(Uuid::from_u128(306));
    let change_id = sample_change_id();

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.insert_replication_group(sample_group(group_id)))
        .expect("group should store");
    wait_for_store_future(apply_row_patch(
        transaction.as_mut(),
        &schema,
        DatasetRowStatePatch {
            group_id,
            dataset_id: dataset_id.clone(),
            actions: vec![DatasetRowStateWrite::UpsertActive {
                row_key,
                snapshot: title_snapshot(&schema, row_key, "created"),
            }],
            change_id,
            last_changed_versions: sample_last_changed_versions(),
        },
    ))
    .expect("row should store");
    wait_for_store_future(transaction.commit()).expect("transaction should commit");

    replace_raw_row_creator(
        store.as_ref(),
        group_id,
        &dataset_id,
        row_key,
        Some(0),
        None,
    );
    let mut transaction =
        wait_for_store_future(store.begin_read_transaction()).expect("read should start");
    let requested_row_keys = [row_key];
    let mut requested_row_keys = requested_row_keys.iter();
    let incomplete_error = wait_for_store_future(transaction.load_dataset_rows(
        GroupDatasetSchemaRef {
            group_id: &group_id,
            dataset_id: &dataset_id,
            schema: &schema,
        },
        &mut requested_row_keys,
    ))
    .expect_err("incomplete creator provenance should fail");
    wait_for_store_future(transaction.release()).expect("read should release");
    assert_sqlite_store_error(&incomplete_error, |source| {
        matches!(
            source,
            SqliteStoreError::IncompleteStoredRowCreationProvenance {
                row_key: stored_row_key,
            } if *stored_row_key == row_key
        )
    });

    replace_raw_row_creator(
        store.as_ref(),
        group_id,
        &dataset_id,
        row_key,
        Some(2),
        Some(i64::from(U64BitsInI64::from(change_id.version))),
    );
    let mut transaction =
        wait_for_store_future(store.begin_read_transaction()).expect("read should start");
    let requested_row_keys = [row_key];
    let mut requested_row_keys = requested_row_keys.iter();
    let out_of_range_error = wait_for_store_future(transaction.load_dataset_rows(
        GroupDatasetSchemaRef {
            group_id: &group_id,
            dataset_id: &dataset_id,
            schema: &schema,
        },
        &mut requested_row_keys,
    ))
    .expect_err("out-of-range creator should fail");
    wait_for_store_future(transaction.release()).expect("read should release");
    assert_sqlite_store_error(&out_of_range_error, |source| {
        matches!(
            source,
            SqliteStoreError::InvalidStoredRowCreatorIndex {
                row_key: stored_row_key,
                creator_index: 2,
                member_count: 2,
            } if *stored_row_key == row_key
        )
    });
}

#[test]
fn sqlite_store_rejects_tombstone_to_active_row_transition() {
    let dataset_id = docs_dataset_id();
    let schema = title_schema();
    let store = in_memory_store(local_member());
    let group_id = GroupId(Uuid::from_u128(105));
    let row_key = RowKey(Uuid::from_u128(205));
    let tombstone_snapshot = title_snapshot(&schema, row_key, "deleted");
    let active_snapshot = title_snapshot(&schema, row_key, "resurrected");

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.insert_replication_group(sample_group(group_id)))
        .expect("group should store");
    wait_for_store_future(apply_row_patch(
        transaction.as_mut(),
        &schema,
        DatasetRowStatePatch {
            group_id,
            dataset_id: dataset_id.clone(),
            actions: vec![DatasetRowStateWrite::UpsertTombstone {
                row_key,
                snapshot: tombstone_snapshot,
            }],
            change_id: sample_change_id(),
            last_changed_versions: sample_last_changed_versions(),
        },
    ))
    .expect("missing-to-tombstone upsert should store");

    let error = wait_for_store_future(apply_row_patch(
        transaction.as_mut(),
        &schema,
        DatasetRowStatePatch {
            group_id,
            dataset_id,
            actions: vec![DatasetRowStateWrite::UpsertActive {
                row_key,
                snapshot: active_snapshot,
            }],
            change_id: UpdateId {
                node_index: 0,
                version: 2,
            },
            last_changed_versions: sample_last_changed_versions(),
        },
    ))
    .expect_err("tombstone-to-active upsert should fail");
    wait_for_store_future(transaction.rollback()).expect("transaction should roll back");

    assert!(matches!(
        error,
        StoreError::StoreExternal { ref source, .. }
            if source.to_string().contains("cannot transition from tombstone to active")
    ));
}

#[test]
fn sqlite_store_rejects_duplicate_group_insert() {
    let store = in_memory_store(local_member());
    let group_id = GroupId(Uuid::from_u128(303));
    let group = sample_group(group_id);

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.insert_replication_group(group.clone()))
        .expect("group should store");
    wait_for_store_future(transaction.commit()).expect("commit should succeed");

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    let error = wait_for_store_future(transaction.insert_replication_group(group))
        .expect_err("duplicate group insert should fail");
    assert!(matches!(error, StoreError::StoreExternal { .. }));
}

#[test]
fn sqlite_store_rejects_duplicate_update_insert_but_allows_applied_toggle() {
    let dataset_id = docs_dataset_id();
    let schema = title_schema();
    let store = in_memory_store(local_member());
    let group_id = GroupId(Uuid::from_u128(404));
    let group = sample_group(group_id);
    let mut source_data = flotsync_messages::InMemoryStateData::new(schema.clone());
    let operation = source_data
        .insert_row(
            UpdateId {
                node_index: 0,
                version: 1,
            },
            Uuid::from_u128(505),
            vec![
                schema
                    .columns
                    .get("title")
                    .expect("title field should exist")
                    .initial("goodbye")
                    .expect("field value should build"),
            ],
        )
        .expect("row insert should succeed");
    let encoded_operation =
        encode_schema_operation(&operation, schema.as_ref()).expect("operation should encode");
    let update = ReplicationUpdateRecord {
        group_id,
        update_id: UpdateId {
            node_index: 0,
            version: u64::MAX - 1,
        },
        sender: local_member(),
        read_versions: VersionVector::initial(NonZeroUsize::new(2).unwrap()),
        dataset_updates: vec![DatasetUpdateRecord {
            dataset_id,
            operations: vec![encoded_operation],
        }],
        applied_locally: false,
    };

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.insert_replication_group(group)).expect("group should store");
    wait_for_store_future(transaction.append_replication_update(update.clone()))
        .expect("update should store");
    let duplicate_error =
        wait_for_store_future(transaction.append_replication_update(update.clone()))
            .expect_err("duplicate update insert should fail");
    assert!(matches!(duplicate_error, StoreError::StoreExternal { .. }));
    wait_for_store_future(transaction.mark_replication_update_applied(&group_id, update.update_id))
        .expect("applied toggle should succeed");
    wait_for_store_future(transaction.commit()).expect("commit should succeed");

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    let loaded_update =
        wait_for_store_future(transaction.load_replication_update(&group_id, update.update_id))
            .expect("update should load")
            .expect("update should exist");
    assert!(loaded_update.applied_locally);
    assert_eq!(loaded_update.update_id.version, u64::MAX - 1);
}
