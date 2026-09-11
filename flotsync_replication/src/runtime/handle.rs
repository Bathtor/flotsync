//! Public replication runtime handle, lifecycle ownership, and test-only controls.

#[cfg(any(test, feature = "test-support"))]
use super::host::DeliveryRuntimeHostTestSupportExt;
#[cfg(test)]
use super::host::{DeliveryRuntimeHostTestExt, RuntimeHostTestSupport};
use super::{
    ReplicationRuntimeMessage,
    host::{DeliveryRuntimeHost, StartupEventPolicy},
    store_security_validation::{
        load_security_error_from_local_member,
        load_security_error_from_runtime,
        security_load_error,
        validate_loaded_group_security,
    },
    synchronisation::{
        ClaimedGroupSynchronisation,
        PreparedApplicationState,
        StoreGroupChangeProvider,
        StoreGroupSnapshotProvider,
        StoreSynchronisationProvider,
        prepare_application_state,
    },
};
use crate::{
    api::{
        ApiError,
        ApiResult,
        ApplicationReadToken,
        ApplicationSchemas,
        ChangeGroupMembershipRequest,
        CreateGroupRequest,
        FlotsyncDiagnostics,
        GroupReadToken,
        LoadError,
        MigrationId,
        PublishChangesRequest,
        PublishReceipt,
        ReplicationApi,
        ReplicationConfig,
        ReplicationEventListener,
        ReplicationGroupSnapshot,
        ReplicationSecuritySecrets,
        ReplicationStore,
        RouteEstablishmentDiagnostics,
        SnapshotRowProvider,
        Summary,
        SummaryRequest,
        api_error,
        load_error,
        security::{
            AssessPublicKeyBundleRequest,
            KnownMemberKeysReport,
            PublicKeyBundleReport,
            RecordPublicKeyBundleFeedbackRequest,
        },
    },
    delivery::security::DeliverySecurity,
    security_store::SecurityStore,
};
#[cfg(any(test, feature = "test-support"))]
use flotsync_core::MemberIdentity;
#[cfg(any(test, feature = "test-support"))]
use flotsync_core::membership::{GroupMembers, GroupMemberships};
use flotsync_core::{ApplicationId, GroupId};
use flotsync_routes::route_establishment::RouteEstablishmentMessage;
use flotsync_security::PublicKeyBundle;
use flotsync_utils::BoxFuture;
use futures_util::{FutureExt, future};
use kompact::{KompactLogger, prelude::*};
use snafu::prelude::*;
use std::sync::{Arc, RwLock, Weak};

#[cfg(any(test, feature = "test-support"))]
use super::errors::GroupInstallError;
#[cfg(test)]
use super::errors::InboundDeliveryError;
#[cfg(test)]
use crate::api::PendingGroupDecisionRecord;
#[cfg(test)]
use crate::codecs::messages::{GroupSetupMessage, UpdateBatchMessage, UpdateMessage};
#[cfg(any(test, feature = "test-support"))]
use std::time::Duration;

type ApiFuture<'a, T> = BoxFuture<'a, ApiResult<T>>;
/// Unit-valued result used when adapting independent cleanup failures.
type UnitResult<E> = Result<(), E>;

#[cfg(any(test, feature = "test-support"))]
const TEST_REPLY_TIMEOUT: Duration = Duration::from_secs(5);

/// Create one concrete replication runtime for the given application identity.
///
/// This asynchronous entry point returns a `Send` future which may move between
/// executor threads.
///
/// Clones of the returned handle share one internal runtime and lifecycle.
///
/// Listener callbacks run from the internally owned runtime rather than the
/// caller's executor. A listener which needs thread-affine application state
/// should hand the event to its application executor or event bus and resolve
/// its callback future when that hand-off has completed.
///
/// Before closing a caller-owned store or exiting the process, call
/// [`ReplicationApi::shutdown`] and await its completion. Applications should
/// explicitly shut down any outstanding [`ApplicationSynchronisation`] and
/// finish any event row providers before closing the store.
///
/// `application_id` scopes the loaded runtime instance for diagnostics and future
/// multi-application hosting.
/// `application_schemas` provides the process-static application schema for each
/// locally understood dataset. Stored group schemas remain authoritative; the
/// runtime reuses one of these references only when its definition matches.
/// `store` provides the local member identity and replication state.
/// `application_read_token` describes the application's materialised state. It
/// may be an aggregate stored atomically with that state or rebuilt from
/// independently persisted complete group positions. Passing `None` requests
/// a complete load.
/// Compatible behind positions return coalesced group-local changes; positions
/// which cannot be reconstructed safely fall back to complete group snapshots.
/// `listener` receives replication events produced by inbound delivery.
/// `config` carries public runtime policy and startup batch-size knobs.
///
/// A store cut which already matches the supplied application state returns
/// [`ReplicationRuntimeLoad::Ready`]. Otherwise the caller must exhaust and
/// apply every group returned by [`ApplicationSynchronisation::next_group`]
/// before calling [`ApplicationSynchronisation::complete`].
///
/// # Errors
///
/// See `LoadError` for failure conditions.
pub async fn load_replication_runtime(
    application_id: ApplicationId,
    application_schemas: &'static ApplicationSchemas,
    store: Arc<dyn ReplicationStore>,
    application_read_token: Option<ApplicationReadToken>,
    listener: Arc<dyn ReplicationEventListener>,
    config: ReplicationConfig,
    security_secrets: ReplicationSecuritySecrets,
) -> Result<ReplicationRuntimeLoad, LoadError> {
    let runtime = load_replication_runtime_typed_with_runtime_config_toml(
        application_id,
        application_schemas,
        store,
        application_read_token,
        listener,
        config,
        security_secrets,
        None,
    )
    .await?;
    Ok(runtime.into_public())
}

/// Create one concrete replication runtime with an additional in-memory TOML
/// config fragment merged into the internal Kompact runtime config.
///
/// This function has the same executor, ownership, listener, and shutdown
/// contract as [`load_replication_runtime`].
///
/// The TOML string only needs to live until this function returns; Kompact
/// copies it into its config builder before the runtime system is built.
///
/// # Errors
///
/// See `LoadError` for failure conditions.
#[allow(
    clippy::too_many_arguments,
    reason = "this explicit public startup boundary mirrors load_replication_runtime and adds only the borrowed runtime configuration fragment"
)]
pub async fn load_replication_runtime_with_runtime_config_toml(
    application_id: ApplicationId,
    application_schemas: &'static ApplicationSchemas,
    store: Arc<dyn ReplicationStore>,
    application_read_token: Option<ApplicationReadToken>,
    listener: Arc<dyn ReplicationEventListener>,
    config: ReplicationConfig,
    security_secrets: ReplicationSecuritySecrets,
    runtime_config_toml: &str,
) -> Result<ReplicationRuntimeLoad, LoadError> {
    let runtime = load_replication_runtime_typed_with_runtime_config_toml(
        application_id,
        application_schemas,
        store,
        application_read_token,
        listener,
        config,
        security_secrets,
        Some(runtime_config_toml),
    )
    .await?;
    Ok(runtime.into_public())
}

/// Result of preparing one replication runtime at the application startup boundary.
#[must_use = "a loaded runtime or staged synchronisation must be completed or shut down"]
#[non_exhaustive]
pub enum ReplicationRuntimeLoad<R = Arc<dyn ReplicationApi>> {
    /// No application reconciliation is required and listener delivery is active.
    Ready(R),
    /// Application state must reach the prepared store cut before listener delivery starts.
    Synchronising(ApplicationSynchronisation),
}

impl<R> std::fmt::Debug for ReplicationRuntimeLoad<R> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ready(_) => formatter.debug_tuple("Ready").field(&"<runtime>").finish(),
            Self::Synchronising(synchronisation) => formatter
                .debug_tuple("Synchronising")
                .field(synchronisation)
                .finish(),
        }
    }
}

/// Partly started runtime and coherent application reconciliation prepared from local storage.
///
/// Apply each group reconciliation and record its resulting position before
/// requesting the next group. Applications may persist positions independently
/// per group, or persist [`Self::final_read_token`] atomically after applying
/// the complete reconciliation.
/// Call [`Self::complete`] only after every group is exhausted; completion then
/// begins live replication operations.
#[must_use = "startup synchronisation must be completed or explicitly shut down"]
pub struct ApplicationSynchronisation {
    /// Private ownership state kept indirect so the public load enum remains compact.
    state: Box<ApplicationSynchronisationState>,
}

impl ApplicationSynchronisation {
    /// Build the intermediate application handle from prepared state and host ownership.
    fn new(
        final_read_token: ApplicationReadToken,
        groups: StoreSynchronisationProvider,
        pending: PendingReplicationRuntime,
    ) -> Self {
        Self {
            state: Box::new(ApplicationSynchronisationState {
                final_read_token,
                groups,
                pending,
            }),
        }
    }

    /// Prepare and return the next group reconciliation in deterministic group-id order.
    ///
    /// Snapshot and change providers must be exhausted before this method can
    /// yield a later group. `Ok(None)` means no further group can be claimed.
    /// It permits completion only when every claimed provider was exhausted
    /// naturally; dropping an incomplete provider also causes subsequent calls
    /// to return `Ok(None)`, but leaves this synchronisation incomplete.
    ///
    /// An operation-scoped retryable store failure leaves the current group
    /// unclaimed, so the caller may repeat this method inside the same retained
    /// store transaction.
    ///
    /// # Errors
    ///
    /// Returns [`crate::RowProviderError`] when preparing incremental changes
    /// or releasing the naturally exhausted store cut fails.
    pub async fn next_group(
        &mut self,
    ) -> Result<Option<SingleGroupSynchronisation<'_>>, crate::api::RowProviderError> {
        let group = self.state.groups.claim_next_group().await?;
        let group = group.map(|group| match group {
            ClaimedGroupSynchronisation::Snapshot {
                group_id,
                read_token,
                rows,
            } => {
                let snapshot = GroupSnapshotSynchronisation {
                    group_id,
                    read_token,
                    rows,
                };
                SingleGroupSynchronisation::Snapshot(snapshot)
            }
            ClaimedGroupSynchronisation::Changes { position, rows } => {
                let changes = GroupChangesSynchronisation { position, rows };
                SingleGroupSynchronisation::Changes(changes)
            }
            ClaimedGroupSynchronisation::Retired { group_id } => {
                let retired = RetiredGroupSynchronisation { group_id };
                SingleGroupSynchronisation::Retired(retired)
            }
        });
        Ok(group)
    }

    /// Return the aggregate position which may optionally be persisted after
    /// full reconciliation.
    ///
    /// Applications which persist state independently per group may instead
    /// store the position carried by each snapshot, change, or retirement
    /// entry and do not need to store this aggregate token.
    #[must_use]
    pub fn final_read_token(&self) -> &ApplicationReadToken {
        &self.state.final_read_token
    }

    /// Complete application synchronisation and begin live replication operations.
    ///
    /// # Errors
    ///
    /// Returns [`LoadError::SynchronisationIncomplete`] if not every group
    /// entry was consumed through natural provider exhaustion. Such a partial
    /// application state has no defined position from which incremental events
    /// can safely continue, so failed completion shuts down the staged runtime
    /// and requires a fresh load. Runtime startup or cleanup failures are
    /// returned through other [`LoadError`] variants.
    pub async fn complete(self) -> Result<Arc<dyn ReplicationApi>, LoadError> {
        let runtime = self.complete_typed().await?;
        Ok(runtime)
    }

    /// Shut down this partly started host without enabling listener delivery.
    ///
    /// # Errors
    ///
    /// Returns [`LoadError`] when releasing the retained store cut or shutting
    /// down the host fails. Every applicable cleanup step is still attempted.
    pub async fn shutdown(self) -> Result<(), LoadError> {
        let ApplicationSynchronisationState {
            groups, pending, ..
        } = *self.state;
        let PendingReplicationRuntime {
            application_id,
            mut host,
            ..
        } = pending;
        let provider_result = groups.abort().await;
        let host_result = host.shutdown().await;
        match (provider_result, host_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Ok(()), Err(source)) => UnitResult::Err(source)
                .boxed()
                .context(load_error::RuntimeSnafu { application_id }),
            (Err(source), Ok(())) => UnitResult::Err(source)
                .boxed()
                .context(load_error::RuntimeSnafu { application_id }),
            (Err(source), Err(host_error)) => {
                log::warn!(
                    "replication host shutdown also failed after synchronisation provider cleanup failed: {host_error}"
                );
                UnitResult::Err(source)
                    .boxed()
                    .context(load_error::RuntimeSnafu { application_id })
            }
        }
    }

    /// Drain every prepared reconciliation without applying it and complete startup.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) async fn complete_discarding_synchronisation(
        mut self,
    ) -> Result<Arc<ReplicationRuntime>, LoadError> {
        let drain_result = self.discard_synchronisation().await;
        if let Err(error) = drain_result {
            if let Err(cleanup_error) = self.shutdown().await {
                log::warn!(
                    "replication synchronisation shutdown also failed after reconciliation draining failed: {cleanup_error}"
                );
            }
            return Err(error);
        }
        self.complete_typed().await
    }

    /// Discard every prepared group reconciliation while preserving drain errors.
    #[cfg(any(test, feature = "test-support"))]
    async fn discard_synchronisation(&mut self) -> Result<(), LoadError> {
        let application_id = self.state.pending.application_id.clone();
        while let Some(mut group) =
            self.next_group()
                .await
                .boxed()
                .with_context(|_| load_error::RuntimeSnafu {
                    application_id: application_id.clone(),
                })?
        {
            match &mut group {
                SingleGroupSynchronisation::Snapshot(snapshot) => {
                    while snapshot
                        .rows()
                        .next_batch()
                        .await
                        .boxed()
                        .with_context(|_| load_error::RuntimeSnafu {
                            application_id: application_id.clone(),
                        })?
                        .is_some()
                    {
                        // Test-support callers deliberately discard each snapshot batch.
                    }
                }
                SingleGroupSynchronisation::Changes(changes) => {
                    while changes
                        .rows()
                        .next_batch()
                        .await
                        .boxed()
                        .with_context(|_| load_error::RuntimeSnafu {
                            application_id: application_id.clone(),
                        })?
                        .is_some()
                    {
                        // Test-support callers deliberately discard each change batch.
                    }
                }
                SingleGroupSynchronisation::Retired(_) => {
                    // Merely receiving a retirement entry consumes its row-free work.
                }
            }
        }
        Ok(())
    }

    /// Complete startup while retaining the concrete runtime for internal tests.
    async fn complete_typed(self) -> Result<Arc<ReplicationRuntime>, LoadError> {
        let ApplicationSynchronisationState {
            groups, pending, ..
        } = *self.state;
        let exhausted = groups.is_exhausted();
        if exhausted {
            pending.start().await
        } else {
            let PendingReplicationRuntime {
                application_id,
                mut host,
                ..
            } = pending;
            if let Err(error) = groups.abort().await {
                log::warn!(
                    "synchronisation provider cleanup also failed after premature completion: {error}"
                );
            }
            if let Err(error) = host.shutdown().await {
                log::warn!(
                    "replication host shutdown also failed after premature synchronisation completion: {error}"
                );
            }
            Err(LoadError::SynchronisationIncomplete { application_id })
        }
    }

    /// Return the staged host address which a test peer should use.
    #[cfg(test)]
    pub(super) fn advertised_loopback_udp_addr_for_test(&self) -> std::net::SocketAddr {
        self.state.pending.host.advertised_loopback_udp_addr()
    }

    /// Wait until group broadcast hands one inbound message to inactive runtime logic.
    #[cfg(test)]
    pub(super) fn wait_for_group_broadcast_inbound_for_test(&self) {
        self.state.pending.host.wait_for_group_broadcast_inbound();
    }

    /// Inject one captured group-broadcast message into inactive runtime logic.
    #[cfg(test)]
    pub(super) fn inject_group_broadcast_inbound_for_test(
        &self,
        indication: crate::delivery::contracts::GroupBroadcastPortIndication,
    ) {
        self.state
            .pending
            .host
            .inject_group_broadcast_inbound(indication);
    }
}

impl std::fmt::Debug for ApplicationSynchronisation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ApplicationSynchronisation")
            .field("final_read_token", &self.state.final_read_token)
            .field("groups_exhausted", &self.state.groups.is_exhausted())
            .finish_non_exhaustive()
    }
}

/// One explicit group-level application reconciliation prepared at startup.
#[must_use = "snapshot and change providers must be exhausted before synchronisation can complete"]
#[non_exhaustive]
pub enum SingleGroupSynchronisation<'a> {
    /// Complete current rows for one readable group.
    Snapshot(GroupSnapshotSynchronisation<'a>),
    /// Coalesced current changes since one compatible supplied position.
    Changes(GroupChangesSynchronisation<'a>),
    /// One supplied group which is absent from the current readable store cut.
    Retired(RetiredGroupSynchronisation),
}

impl SingleGroupSynchronisation<'_> {
    /// Return the replication group affected by this reconciliation entry.
    #[must_use]
    pub const fn group_id(&self) -> GroupId {
        match self {
            Self::Snapshot(snapshot) => snapshot.group_id(),
            Self::Changes(changes) => changes.group_id(),
            Self::Retired(retired) => retired.group_id(),
        }
    }
}

/// One readable replication group's complete startup snapshot.
pub struct GroupSnapshotSynchronisation<'a> {
    /// Replication group represented by this snapshot.
    group_id: GroupId,
    /// Group position reached after applying the complete snapshot.
    read_token: GroupReadToken,
    /// Bounded snapshot rows restricted to `group_id`.
    rows: StoreGroupSnapshotProvider<'a>,
}

impl GroupSnapshotSynchronisation<'_> {
    /// Return the replication group represented by this snapshot.
    #[must_use]
    pub const fn group_id(&self) -> GroupId {
        self.group_id
    }

    /// Return the group position reached after applying this complete snapshot.
    #[must_use]
    pub const fn read_token(&self) -> &GroupReadToken {
        &self.read_token
    }

    /// Return this group's bounded snapshot-row provider.
    pub fn rows(&mut self) -> &mut SnapshotRowProvider<'_> {
        &mut self.rows
    }
}

/// One readable replication group's coalesced incremental startup changes.
pub struct GroupChangesSynchronisation<'a> {
    /// Position reached after applying the complete change collection.
    position: crate::api::DataChangeReadPosition,
    /// In-memory changes exposed through the ordinary application row-provider contract.
    rows: StoreGroupChangeProvider<'a>,
}

impl GroupChangesSynchronisation<'_> {
    /// Return the replication group represented by these changes.
    #[must_use]
    pub const fn group_id(&self) -> GroupId {
        self.position.group_read_token().group_id()
    }

    /// Return the group-local position reached after applying every change.
    #[must_use]
    pub const fn position(&self) -> &crate::api::DataChangeReadPosition {
        &self.position
    }

    /// Return this group's coalesced row-change provider.
    ///
    /// The provider must be called through natural exhaustion even when it
    /// contains no rows, because an empty collection can still advance the
    /// application position.
    pub fn rows(
        &mut self,
    ) -> &mut (dyn crate::api::BatchProvider<Batch = crate::api::RowChangeBatch> + '_) {
        &mut self.rows
    }
}

/// Explicit retirement of one group from the supplied application state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetiredGroupSynchronisation {
    /// Group which is absent from the current readable store cut.
    group_id: GroupId,
}

impl RetiredGroupSynchronisation {
    /// Return the group whose application state and read token must be removed.
    #[must_use]
    pub const fn group_id(&self) -> GroupId {
        self.group_id
    }
}

/// Internal concrete-runtime counterpart of [`ReplicationRuntimeLoad`].
pub(crate) type TypedReplicationRuntimeLoad = ReplicationRuntimeLoad<Arc<ReplicationRuntime>>;

impl ReplicationRuntimeLoad<Arc<ReplicationRuntime>> {
    /// Erase the concrete ready-runtime type at the public boundary.
    fn into_public(self) -> ReplicationRuntimeLoad {
        match self {
            Self::Ready(runtime) => ReplicationRuntimeLoad::Ready(runtime),
            Self::Synchronising(synchronisation) => {
                ReplicationRuntimeLoad::Synchronising(synchronisation)
            }
        }
    }
}

/// Resources which exist together for the complete staged-startup lifetime.
struct ApplicationSynchronisationState {
    /// Aggregate position which applications may optionally persist after applying all groups.
    final_read_token: ApplicationReadToken,
    /// Per-group work retaining the store cut until exhausted or dropped.
    groups: StoreSynchronisationProvider,
    /// Partly started host and real listener retained until completion or shutdown.
    pending: PendingReplicationRuntime,
}

/// Host ownership retained between network preparation and runtime activation.
struct PendingReplicationRuntime {
    /// Application identity used in public load errors and the final runtime.
    application_id: ApplicationId,
    /// Real application listener installed immediately before runtime start.
    listener: Arc<dyn ReplicationEventListener>,
    /// Partly started host whose runtime logic remains inactive.
    host: DeliveryRuntimeHost,
    /// Runtime configuration retained for the final application handle.
    config: ReplicationConfig,
}

impl PendingReplicationRuntime {
    /// Activate runtime logic and build the concrete application handle.
    async fn start(self) -> Result<Arc<ReplicationRuntime>, LoadError> {
        // Split ownership up front so the host can be moved into the live handle on success or
        // mutably shut down on failure without cloning the independent identity/configuration.
        let Self {
            application_id,
            listener,
            mut host,
            config,
        } = self;
        let start_result = host.activate_runtime(listener).await;
        if start_result.is_err()
            && let Err(shutdown_error) = host.shutdown().await
        {
            log::warn!(
                "replication host shutdown also failed after runtime activation failure: {shutdown_error}"
            );
        }
        match start_result {
            Ok(()) => Ok(replication_runtime_from_host(application_id, config, host)),
            Err(source) => Result::<Arc<ReplicationRuntime>, _>::Err(source)
                .boxed()
                .context(load_error::RuntimeSnafu { application_id }),
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the internal typed loader mirrors the explicit public startup inputs"
)]
pub(super) async fn load_replication_runtime_typed_with_runtime_config_toml(
    application_id: ApplicationId,
    application_schemas: &'static ApplicationSchemas,
    store: Arc<dyn ReplicationStore>,
    application_read_token: Option<ApplicationReadToken>,
    listener: Arc<dyn ReplicationEventListener>,
    config: ReplicationConfig,
    security_secrets: ReplicationSecuritySecrets,
    runtime_config_toml: Option<&str>,
) -> Result<TypedReplicationRuntimeLoad, LoadError> {
    let local_member =
        store
            .local_member_identity()
            .await
            .context(load_error::StoreAccessSnafu {
                application_id: application_id.clone(),
            })?;
    let security_store = SecurityStore::new(store.clone(), config.trust_policy.clone());
    let security_f = DeliverySecurity::load(
        security_store,
        &local_member,
        security_secrets.store_secret_key().clone(),
        *security_secrets.store_secret_key_id(),
    );
    let security = security_f
        .await
        .map_err(|source| load_security_error_from_local_member(&local_member, source))
        .map_err(|source| security_load_error(application_id.clone(), source))?;
    let validation_f = validate_loaded_group_security(
        application_id.clone(),
        store.clone(),
        security_secrets.store_secret_key_id(),
    );
    validation_f
        .await
        .map_err(load_security_error_from_runtime)
        .map_err(|source| security_load_error(application_id.clone(), source))?;
    load_replication_runtime_typed_with_security(
        application_id,
        application_schemas,
        store,
        application_read_token,
        listener,
        config,
        security,
        runtime_config_toml,
        StartupEventPolicy::Log,
    )
    .await
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) async fn load_replication_runtime_typed_with_security_for_test(
    application_id: ApplicationId,
    application_schemas: &'static ApplicationSchemas,
    store: Arc<dyn ReplicationStore>,
    listener: Arc<dyn ReplicationEventListener>,
    config: ReplicationConfig,
    security: DeliverySecurity,
    runtime_config_toml: Option<&str>,
) -> Result<Arc<ReplicationRuntime>, LoadError> {
    let load = load_replication_runtime_typed_with_security(
        application_id,
        application_schemas,
        store,
        None,
        listener,
        config,
        security,
        runtime_config_toml,
        StartupEventPolicy::Log,
    )
    .await?;
    match load {
        ReplicationRuntimeLoad::Ready(runtime) => Ok(runtime),
        ReplicationRuntimeLoad::Synchronising(synchronisation) => {
            synchronisation.complete_discarding_synchronisation().await
        }
    }
}

/// Prepare one observed staged runtime with a strict inactive-listener policy.
///
/// The returned host alone inserts a proxy at the group-broadcast/runtime boundary so the focused
/// lifecycle test can observe inbound work while runtime logic remains inactive.
#[cfg(test)]
#[allow(
    clippy::too_many_arguments,
    reason = "the test loader mirrors the staged runtime inputs and opts into isolated host observation"
)]
pub(super) async fn load_replication_runtime_typed_with_observed_startup_for_test(
    application_id: ApplicationId,
    application_schemas: &'static ApplicationSchemas,
    store: Arc<dyn ReplicationStore>,
    listener: Arc<dyn ReplicationEventListener>,
    config: ReplicationConfig,
    security: DeliverySecurity,
    runtime_config_toml: Option<&str>,
) -> Result<TypedReplicationRuntimeLoad, LoadError> {
    load_replication_runtime_typed_with_security_inner(
        application_id,
        application_schemas,
        store,
        None,
        listener,
        config,
        security,
        runtime_config_toml,
        StartupEventPolicy::Panic,
        Some(RuntimeHostTestSupport::observing_group_broadcast_runtime()),
    )
    .await
}

#[allow(
    clippy::too_many_arguments,
    reason = "the internal security-ready loader mirrors the explicit public startup inputs"
)]
async fn load_replication_runtime_typed_with_security(
    application_id: ApplicationId,
    application_schemas: &'static ApplicationSchemas,
    store: Arc<dyn ReplicationStore>,
    application_read_token: Option<ApplicationReadToken>,
    listener: Arc<dyn ReplicationEventListener>,
    config: ReplicationConfig,
    security: DeliverySecurity,
    runtime_config_toml: Option<&str>,
    startup_event_policy: StartupEventPolicy,
) -> Result<TypedReplicationRuntimeLoad, LoadError> {
    load_replication_runtime_typed_with_security_inner(
        application_id,
        application_schemas,
        store,
        application_read_token,
        listener,
        config,
        security,
        runtime_config_toml,
        startup_event_policy,
        #[cfg(test)]
        None,
    )
    .await
}

#[allow(
    clippy::too_many_arguments,
    reason = "the internal loader adds one isolated unit-test support payload to the explicit startup inputs"
)]
/// Shared implementation for ordinary loading and the focused unit-test host seam.
async fn load_replication_runtime_typed_with_security_inner(
    application_id: ApplicationId,
    application_schemas: &'static ApplicationSchemas,
    store: Arc<dyn ReplicationStore>,
    application_read_token: Option<ApplicationReadToken>,
    listener: Arc<dyn ReplicationEventListener>,
    config: ReplicationConfig,
    security: DeliverySecurity,
    runtime_config_toml: Option<&str>,
    startup_event_policy: StartupEventPolicy,
    #[cfg(test)] runtime_host_test_support: Option<RuntimeHostTestSupport>,
) -> Result<TypedReplicationRuntimeLoad, LoadError> {
    let local_member =
        store
            .local_member_identity()
            .await
            .context(load_error::StoreAccessSnafu {
                application_id: application_id.clone(),
            })?;
    let prepared = prepare_application_state(
        &local_member,
        application_schemas,
        &store,
        application_read_token,
        config.application_synchronisation_batch_size,
    )
    .await
    .boxed()
    .context(load_error::RuntimeSnafu {
        application_id: application_id.clone(),
    })?;
    let group_state = match &prepared {
        PreparedApplicationState::Ready { group_state }
        | PreparedApplicationState::Synchronising { group_state, .. } => group_state.clone(),
    };
    let startup_listener = startup_event_policy.create_listener();
    let host_result = cfg_select! {
        test => match runtime_host_test_support {
            Some(test_support) => DeliveryRuntimeHost::prepare_with_test_support(
                &local_member,
                group_state,
                store,
                config.clone(),
                security,
                runtime_config_toml,
                startup_listener,
                test_support,
            ).await,
            None => DeliveryRuntimeHost::prepare_with_runtime_config_toml(
                &local_member,
                group_state,
                store,
                config.clone(),
                security,
                runtime_config_toml,
                startup_listener,
            ).await,
        },
        _ => DeliveryRuntimeHost::prepare_with_runtime_config_toml(
            &local_member,
            group_state,
            store,
            config.clone(),
            security,
            runtime_config_toml,
            startup_listener,
        ).await,
    };
    let host = host_result.boxed().context(load_error::RuntimeSnafu {
        application_id: application_id.clone(),
    })?;
    let pending = PendingReplicationRuntime {
        application_id,
        listener,
        host,
        config,
    };
    match prepared {
        PreparedApplicationState::Ready { .. } => {
            let runtime = pending.start().await?;
            Ok(TypedReplicationRuntimeLoad::Ready(runtime))
        }
        PreparedApplicationState::Synchronising {
            final_read_token,
            synchronisation,
            ..
        } => Ok(TypedReplicationRuntimeLoad::Synchronising(
            ApplicationSynchronisation::new(final_read_token, *synchronisation, pending),
        )),
    }
}

/// Construct the application-facing runtime around one fully started host.
fn replication_runtime_from_host(
    application_id: ApplicationId,
    config: ReplicationConfig,
    host: DeliveryRuntimeHost,
) -> Arc<ReplicationRuntime> {
    let runtime_component = host.runtime_component().clone();
    let runtime_ref = runtime_component
        .actor_ref()
        .hold()
        .expect("replication runtime component must expose a strong actor ref");
    let route_establishment_ref = host
        .route_establishment_component()
        .actor_ref()
        .hold()
        .expect("route establishment must expose a strong actor ref");
    let logger = host.logger().clone();
    // The weak self-view permits independently owned trait-object handles without either a
    // second runtime allocation or a strong reference cycle.
    Arc::new_cyclic(move |self_weak| ReplicationRuntime {
        _application_id: application_id,
        self_weak: self_weak.clone(),
        lifecycle: RwLock::new(Some(RuntimeLifecycle {
            runtime_ref,
            route_establishment_ref,
            host,
        })),
        logger,
        _config: config,
    })
}

/// Concrete application-facing runtime returned by `load_replication_runtime`.
pub(crate) struct ReplicationRuntime {
    _application_id: ApplicationId,
    /// Non-owning self-view used to expose another trait object for this exact allocation.
    self_weak: Weak<Self>,
    /// Live runtime ownership, taken exactly once during graceful shutdown.
    lifecycle: RwLock<Option<RuntimeLifecycle>>,
    /// Logger clone kept outside the lifecycle lock for hot-path diagnostics.
    logger: KompactLogger,
    _config: ReplicationConfig,
}

/// Live runtime resources that are created and shut down as one unit.
struct RuntimeLifecycle {
    /// Strong actor ref used by public API calls while the runtime is live.
    runtime_ref: ActorRefStrong<ReplicationRuntimeMessage>,
    /// Strong actor reference used by read-only peer-route diagnostic queries.
    route_establishment_ref: ActorRefStrong<RouteEstablishmentMessage>,
    /// Internal Kompact runtime host that owns component topology and system shutdown.
    host: DeliveryRuntimeHost,
}

impl ReplicationRuntime {
    fn runtime_ref(
        &self,
        operation: &'static str,
    ) -> ApiResult<Option<ActorRefStrong<ReplicationRuntimeMessage>>> {
        let Ok(lifecycle) = self.lifecycle.read() else {
            return Err(ApiError::RuntimeLifecyclePoisoned { operation });
        };
        Ok(lifecycle
            .as_ref()
            .map(|lifecycle| lifecycle.runtime_ref.clone()))
    }

    fn ask<T>(
        &self,
        build: impl FnOnce(KPromise<ApiResult<T>>) -> ReplicationRuntimeMessage + Send + 'static,
    ) -> ApiFuture<'static, T>
    where
        T: Send + 'static,
    {
        let runtime_ref = match self.runtime_ref("sending runtime request") {
            Ok(Some(runtime_ref)) => runtime_ref,
            Ok(None) => return future::ready(Err(ApiError::RuntimeUnavailable)).boxed(),
            Err(error) => return future::ready(Err(error)).boxed(),
        };
        let future = runtime_ref.ask_with(build);
        let logger = self.logger.clone();
        async move {
            match future.await {
                Ok(reply) => reply,
                Err(error) => {
                    warn!(logger, "replication runtime ask failed: {error}");
                    Err(ApiError::RuntimeUnavailable)
                }
            }
        }
        .boxed()
    }

    /// Return the live route-establishment actor used by diagnostic queries.
    fn route_establishment_ref(
        &self,
        operation: &'static str,
    ) -> ApiResult<Option<ActorRefStrong<RouteEstablishmentMessage>>> {
        let Ok(lifecycle) = self.lifecycle.read() else {
            return Err(ApiError::RuntimeLifecyclePoisoned { operation });
        };
        Ok(lifecycle
            .as_ref()
            .map(|lifecycle| lifecycle.route_establishment_ref.clone()))
    }
}

impl Drop for ReplicationRuntime {
    fn drop(&mut self) {
        let lifecycle = match self.lifecycle.get_mut() {
            Ok(lifecycle) => lifecycle.take(),
            Err(poisoned) => {
                warn!(
                    self.logger,
                    "replication runtime lifecycle lock was poisoned during drop; continuing best-effort cleanup"
                );
                poisoned.into_inner().take()
            }
        };
        drop(lifecycle);
    }
}

impl ReplicationApi for ReplicationRuntime {
    fn diagnostics(&self) -> Arc<dyn FlotsyncDiagnostics> {
        self.self_weak
            .upgrade()
            .expect("a live replication runtime reference must have a strong Arc owner")
    }

    fn group_state(&self) -> Result<Arc<dyn ReplicationGroupSnapshot>, ApiError> {
        let lifecycle = self
            .lifecycle
            .read()
            .map_err(|_| ApiError::RuntimeLifecyclePoisoned {
                operation: "loading group state",
            })?;
        let lifecycle = lifecycle.as_ref().ok_or(ApiError::RuntimeUnavailable)?;
        Ok(lifecycle.host.group_state_snapshot())
    }

    fn shutdown(&self) -> ApiFuture<'_, ()> {
        async move {
            let lifecycle = {
                let Ok(mut lifecycle) = self.lifecycle.write() else {
                    return Err(ApiError::RuntimeLifecyclePoisoned {
                        operation: "shutting runtime down",
                    });
                };
                lifecycle.take()
            };
            let Some(RuntimeLifecycle { mut host, .. }) = lifecycle else {
                return Ok(());
            };
            host.shutdown()
                .await
                .boxed()
                .context(api_error::ApiExternalSnafu)
        }
        .boxed()
    }

    fn local_public_key_bundle(&self) -> ApiFuture<'_, PublicKeyBundle> {
        self.ask(|promise| ReplicationRuntimeMessage::LocalPublicKeyBundle(Ask::new(promise, ())))
    }

    fn assess_public_key_bundle(
        &self,
        request: AssessPublicKeyBundleRequest,
    ) -> ApiFuture<'_, PublicKeyBundleReport> {
        self.ask(move |promise| {
            ReplicationRuntimeMessage::AssessPublicKeyBundle(Ask::new(promise, request))
        })
    }

    fn record_public_key_bundle_feedback(
        &self,
        request: RecordPublicKeyBundleFeedbackRequest,
    ) -> ApiFuture<'_, ()> {
        self.ask(move |promise| {
            ReplicationRuntimeMessage::RecordPublicKeyBundleFeedback(Ask::new(promise, request))
        })
    }

    fn known_member_keys(&self) -> ApiFuture<'_, KnownMemberKeysReport> {
        self.ask(|promise| ReplicationRuntimeMessage::KnownMemberKeys(Ask::new(promise, ())))
    }

    fn publish_changes(&self, request: PublishChangesRequest) -> ApiFuture<'_, PublishReceipt> {
        self.ask(move |promise| {
            ReplicationRuntimeMessage::PublishChanges(Ask::new(promise, request))
        })
    }

    fn request_summary(&self, request: SummaryRequest) -> ApiFuture<'_, Summary> {
        self.ask(move |promise| {
            ReplicationRuntimeMessage::RequestSummary(Ask::new(promise, request))
        })
    }

    fn create_group(&self, req: CreateGroupRequest) -> ApiFuture<'_, GroupId> {
        self.ask(move |promise| ReplicationRuntimeMessage::CreateGroup(Ask::new(promise, req)))
    }

    fn change_group_membership(
        &self,
        req: ChangeGroupMembershipRequest,
    ) -> ApiFuture<'_, MigrationId> {
        self.ask(move |promise| {
            ReplicationRuntimeMessage::ChangeGroupMembership(Ask::new(promise, req))
        })
    }
}

impl FlotsyncDiagnostics for ReplicationRuntime {
    fn peer_routes(&self) -> ApiFuture<'_, RouteEstablishmentDiagnostics> {
        let route_establishment_ref =
            match self.route_establishment_ref("requesting peer-route diagnostics") {
                Ok(Some(route_establishment_ref)) => route_establishment_ref,
                Ok(None) => return future::ready(Err(ApiError::RuntimeUnavailable)).boxed(),
                Err(error) => return future::ready(Err(error)).boxed(),
            };
        let future = route_establishment_ref
            .ask_with(|promise| RouteEstablishmentMessage::Diagnostics(Ask::new(promise, ())));
        let logger = self.logger.clone();
        async move {
            match future.await {
                Ok(snapshot) => Ok(snapshot),
                Err(error) => {
                    warn!(
                        logger,
                        "route-establishment diagnostics ask failed: {error}"
                    );
                    Err(ApiError::RuntimeUnavailable)
                }
            }
        }
        .boxed()
    }
}

#[cfg(any(test, feature = "test-support"))]
pub(super) fn wait_for_test_reply<F>(future: F) -> F::Output
where
    F: std::future::Future,
{
    flotsync_io::test_support::wait_for_future(
        TEST_REPLY_TIMEOUT,
        future,
        "timed out waiting for test reply",
    )
}

#[cfg(any(test, feature = "test-support"))]
impl ReplicationRuntime {
    fn with_host_for_test<T>(&self, read: impl FnOnce(&DeliveryRuntimeHost) -> T) -> T {
        let lifecycle = self
            .lifecycle
            .read()
            .expect("replication runtime lifecycle lock should not be poisoned");
        let lifecycle = lifecycle
            .as_ref()
            .expect("replication runtime host should be live during test access");
        read(&lifecycle.host)
    }

    pub(crate) fn membership_snapshot_for_test(&self) -> Arc<dyn GroupMemberships> {
        self.with_host_for_test(DeliveryRuntimeHost::membership_snapshot)
    }

    pub(crate) fn advertised_loopback_udp_addr_for_test(&self) -> std::net::SocketAddr {
        self.with_host_for_test(DeliveryRuntimeHostTestSupportExt::advertised_loopback_udp_addr)
    }

    pub(crate) fn publish_direct_peer_route_for_test(
        &self,
        peer: MemberIdentity,
        remote_addr: std::net::SocketAddr,
    ) {
        self.with_host_for_test(|host| host.publish_direct_peer_route(peer, remote_addr));
    }

    #[cfg(test)]
    pub(crate) fn withdraw_direct_peer_routes_for_test(&self, peer: MemberIdentity) {
        self.with_host_for_test(|host| host.withdraw_direct_peer_routes(peer));
    }

    #[cfg(test)]
    pub(crate) fn replace_route_establishment_watches_for_test(
        &self,
        watches: Vec<flotsync_routes::route_establishment::WatchedRoute>,
    ) {
        self.with_host_for_test(|host| host.replace_route_establishment_watches(watches));
    }

    // These route assertion helpers are only used by in-crate runtime tests.
    // Building them for the broader `test-support` feature leaves dead code.
    #[cfg(test)]
    pub(crate) fn knows_direct_peer_route_for_test(&self, peer: &MemberIdentity) -> bool {
        self.with_host_for_test(|host| host.knows_direct_peer_route(peer))
    }

    #[cfg(test)]
    pub(crate) fn wait_for_direct_peer_route_for_test(&self, peer: &MemberIdentity) {
        self.with_host_for_test(|host| host.wait_for_direct_peer_route(peer));
    }

    /// Capture one inbound message at an explicitly observed runtime boundary.
    #[cfg(test)]
    pub(super) fn capture_group_broadcast_inbound_for_test(
        &self,
    ) -> crate::delivery::contracts::GroupBroadcastPortIndication {
        self.with_host_for_test(DeliveryRuntimeHostTestExt::capture_group_broadcast_inbound)
    }

    pub(crate) fn install_group_for_test(
        &self,
        group_id: GroupId,
        members: GroupMembers,
    ) -> Result<(), GroupInstallError> {
        let runtime_ref = self
            .runtime_ref("installing test group")
            .expect("replication runtime lifecycle should be readable during test install")
            .expect("replication runtime should be live during test install");
        let future = runtime_ref.ask_with(|promise| {
            ReplicationRuntimeMessage::test_install_group(promise, group_id, members)
        });
        match wait_for_test_reply(future) {
            Ok(reply) => reply,
            Err(error) => {
                panic!(
                    "replication runtime component became unavailable during test install: {error:?}"
                )
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn group_read_token_for_test(&self, group_id: GroupId) -> GroupReadToken {
        let runtime_ref = self
            .runtime_ref("loading test group read token")
            .expect("replication runtime lifecycle should be readable during test token loading")
            .expect("replication runtime should be live during test token loading");
        let future = runtime_ref.ask_with(|promise| {
            ReplicationRuntimeMessage::test_read_group_token(promise, group_id)
        });
        wait_for_test_reply(future)
            .expect("replication runtime component should answer test token loading")
            .expect("test group token should load from the replication store")
            .expect("test group should exist in the replication store")
    }

    #[cfg(test)]
    pub(super) fn apply_update_for_test(
        &self,
        sender: MemberIdentity,
        message: UpdateMessage,
    ) -> Result<(), InboundDeliveryError> {
        let runtime_ref = self
            .runtime_ref("injecting test update")
            .expect("replication runtime lifecycle should be readable during test update injection")
            .expect("replication runtime should be live during test update injection");
        let future = runtime_ref.ask_with(|promise| {
            ReplicationRuntimeMessage::test_apply_update(promise, sender, message)
        });
        match wait_for_test_reply(future) {
            Ok(reply) => reply,
            Err(error) => {
                panic!(
                    "replication runtime component became unavailable during test apply_update: {error:?}"
                )
            }
        }
    }

    #[cfg(test)]
    pub(super) fn apply_update_batch_for_test(
        &self,
        sender: MemberIdentity,
        message: UpdateBatchMessage,
    ) -> Result<(), InboundDeliveryError> {
        let runtime_ref = self
            .runtime_ref("injecting test batch")
            .expect("replication runtime lifecycle should be readable during test batch injection")
            .expect("replication runtime should be live during test batch injection");
        let future = runtime_ref.ask_with(|promise| {
            ReplicationRuntimeMessage::test_apply_update_batch(promise, sender, message)
        });
        match wait_for_test_reply(future) {
            Ok(reply) => reply,
            Err(error) => {
                panic!(
                    "replication runtime component became unavailable during test apply_update_batch: {error:?}"
                )
            }
        }
    }

    /// Inject one pending-group delivery without transport for runtime logic tests.
    #[cfg(test)]
    pub(super) fn apply_pending_group_for_test(
        &self,
        sender: MemberIdentity,
        record: PendingGroupDecisionRecord,
        group_setup: Arc<GroupSetupMessage>,
    ) -> Result<(), InboundDeliveryError> {
        let runtime_ref = self
            .runtime_ref("injecting test pending-group delivery")
            .expect("replication runtime lifecycle should be readable during test injection")
            .expect("replication runtime should be live during test injection");
        let future = runtime_ref.ask_with(|promise| {
            ReplicationRuntimeMessage::test_apply_pending_group(
                promise,
                sender,
                record,
                group_setup,
            )
        });
        match wait_for_test_reply(future) {
            Ok(reply) => reply,
            Err(error) => {
                panic!(
                    "replication runtime component became unavailable during pending-group test injection: {error:?}"
                )
            }
        }
    }
}
