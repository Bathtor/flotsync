//! Store-cut preparation and bounded application startup synchronisation.

use self::planning::{
    PendingGroupSynchronisation,
    ReadableGroupSynchronisation,
    plan_group_synchronisation,
};
use super::{
    errors::{InvalidGroupSnafu, RuntimeStartupError, StoreStartupSnafu},
    group_state::{RuntimeGroupStateSnapshot, SharedGroupState, resolve_group_schema},
};
use crate::api::{ApplicationReadToken, ApplicationSchemas, GroupReadToken, ReplicationStore};
use flotsync_core::MemberIdentity;
use snafu::ResultExt as _;
use std::{collections::HashSet, num::NonZeroUsize, sync::Arc};

mod planning;
mod provider;

pub(super) use self::provider::{
    ClaimedGroupSynchronisation,
    StoreGroupChangeProvider,
    StoreGroupSnapshotProvider,
    StoreSynchronisationProvider,
};

/// Prepare the runtime group view and application reconciliation from one store cut.
pub(super) async fn prepare_application_state(
    local_member: &MemberIdentity,
    application_schemas: &'static ApplicationSchemas,
    store: &Arc<dyn ReplicationStore>,
    application_read_token: Option<ApplicationReadToken>,
    max_rows_per_batch: NonZeroUsize,
) -> Result<PreparedApplicationState, RuntimeStartupError> {
    let mut transaction = store
        .begin_read_transaction()
        .await
        .context(StoreStartupSnafu)?;
    let mut persisted_groups = transaction
        .load_replication_groups()
        .await
        .context(StoreStartupSnafu)?;
    persisted_groups.sort_by_key(|group| group.group_id);

    let group_capacity = persisted_groups.len();
    let group_state = Arc::new(SharedGroupState::new(application_schemas));
    let mut runtime_snapshot = RuntimeGroupStateSnapshot::new();
    let mut readable_groups = Vec::with_capacity(group_capacity);
    let mut readable_group_ids = HashSet::with_capacity(group_capacity);
    let mut final_read_token = ApplicationReadToken::default();

    for persisted_group in persisted_groups {
        let group_id = persisted_group.group_id;
        let resolved_group_schema =
            resolve_group_schema(application_schemas, persisted_group.group_schema.clone());
        let readable_group = if persisted_group.lifecycle.is_readable() {
            let read_token = GroupReadToken::from_group_version(
                group_id,
                persisted_group.version_vector.clone(),
            );
            Some((read_token, resolved_group_schema.clone()))
        } else {
            None
        };
        runtime_snapshot
            .insert_record(local_member, resolved_group_schema, persisted_group)
            .context(InvalidGroupSnafu { group_id })?;
        if let Some((read_token, group_schema)) = readable_group {
            readable_group_ids.insert(group_id);
            final_read_token.merge_applied(&read_token);
            readable_groups.push(ReadableGroupSynchronisation {
                group_id,
                read_token,
                group_schema,
            });
        }
    }
    group_state.replace(runtime_snapshot);

    let mut groups = plan_group_synchronisation(
        readable_groups,
        &readable_group_ids,
        application_read_token.as_ref(),
    );
    groups
        .make_contiguous()
        .sort_by_key(PendingGroupSynchronisation::group_id);

    if groups.is_empty() {
        transaction.release().await.context(StoreStartupSnafu)?;
        Ok(PreparedApplicationState::Ready { group_state })
    } else {
        let synchronisation =
            StoreSynchronisationProvider::new(transaction, groups, max_rows_per_batch);
        Ok(PreparedApplicationState::Synchronising {
            group_state,
            final_read_token,
            synchronisation: Box::new(synchronisation),
        })
    }
}

/// Store-derived application state prepared before the Kompact system exists.
pub(super) enum PreparedApplicationState {
    /// No application reconciliation is needed for the prepared store cut.
    Ready {
        /// Complete runtime group view installed into every topology consumer.
        group_state: Arc<SharedGroupState>,
    },
    /// Application state must be reconciled before runtime logic starts.
    Synchronising {
        /// Complete runtime group view installed into every topology consumer.
        group_state: Arc<SharedGroupState>,
        /// Aggregate position which applications may optionally persist after full reconciliation.
        final_read_token: ApplicationReadToken,
        /// Per-group work and the read transaction which fixes its store cut.
        synchronisation: Box<StoreSynchronisationProvider>,
    },
}
