use super::{
    ReplicationRuntimeMessage,
    host::DeliveryRuntimeHost,
    store_security_validation::{
        load_security_error_from_local_member,
        load_security_error_from_runtime,
        security_load_error,
        validate_loaded_group_security,
    },
};
use crate::{
    api::{
        ApiError,
        ApiResult,
        ChangeGroupMembershipRequest,
        CreateGroupRequest,
        GroupMigration,
        LoadError,
        PublishChangesRequest,
        PublishReceipt,
        ReplicationApi,
        ReplicationConfig,
        ReplicationEventListener,
        ReplicationSecuritySecrets,
        ReplicationStore,
        RuntimeSnafu,
        SnapshotRows,
        SnapshotRowsRequest,
        Summary,
        SummaryRequest,
    },
    delivery::security::DeliverySecurity,
};
#[cfg(test)]
use flotsync_core::MemberIdentity;
#[cfg(any(test, feature = "test-support"))]
use flotsync_core::membership::GroupMembers;
use flotsync_core::{GroupId, member::Identifier};
use flotsync_utils::BoxFuture;
use futures_util::FutureExt;
use kompact::prelude::*;
use snafu::prelude::*;
use std::sync::Arc;

#[cfg(any(test, feature = "test-support"))]
use super::errors::GroupInstallError;
#[cfg(test)]
use super::{
    errors::InboundDeliveryError,
    messages::{UpdateBatchMessage, UpdateMessage},
};
#[cfg(any(test, feature = "test-support"))]
use std::time::Duration;

type ApiFuture<'a, T> = BoxFuture<'a, ApiResult<T>>;

#[cfg(any(test, feature = "test-support"))]
const TEST_REPLY_TIMEOUT: Duration = Duration::from_secs(5);

/// Create one concrete replication runtime for the given application identity.
///
/// `application_id` scopes the loaded runtime instance for diagnostics and future
/// multi-application hosting.
/// `store` provides the local member identity and dataset schemas needed by the
/// current replication runtime.
/// `listener` receives replication events produced by inbound delivery.
/// `config` carries public runtime policy knobs; the current runtime only honours
/// the migration-policy shape while the deeper protocol remains unimplemented.
///
/// # Errors
///
/// See `LoadError` for failure conditions.
pub async fn load_replication_runtime(
    application_id: Identifier,
    store: Arc<dyn ReplicationStore>,
    listener: Arc<dyn ReplicationEventListener>,
    config: ReplicationConfig,
    security_secrets: ReplicationSecuritySecrets,
) -> Result<Arc<dyn ReplicationApi>, LoadError> {
    let runtime = load_replication_runtime_typed_with_runtime_config_toml(
        application_id,
        store,
        listener,
        config,
        security_secrets,
        None,
    )
    .await?;
    Ok(runtime)
}

/// Create one concrete replication runtime with an additional in-memory TOML
/// config fragment merged into the internal Kompact runtime config.
///
/// The TOML string only needs to live until this function returns; Kompact
/// copies it into its config builder before the runtime system is built.
///
/// # Errors
///
/// See `LoadError` for failure conditions.
pub async fn load_replication_runtime_with_runtime_config_toml(
    application_id: Identifier,
    store: Arc<dyn ReplicationStore>,
    listener: Arc<dyn ReplicationEventListener>,
    config: ReplicationConfig,
    security_secrets: ReplicationSecuritySecrets,
    runtime_config_toml: &str,
) -> Result<Arc<dyn ReplicationApi>, LoadError> {
    let runtime = load_replication_runtime_typed_with_runtime_config_toml(
        application_id,
        store,
        listener,
        config,
        security_secrets,
        Some(runtime_config_toml),
    )
    .await?;
    Ok(runtime)
}

pub(super) async fn load_replication_runtime_typed_with_runtime_config_toml(
    application_id: Identifier,
    store: Arc<dyn ReplicationStore>,
    listener: Arc<dyn ReplicationEventListener>,
    config: ReplicationConfig,
    security_secrets: ReplicationSecuritySecrets,
    runtime_config_toml: Option<&str>,
) -> Result<Arc<ReplicationRuntime>, LoadError> {
    let local_member = store
        .local_member_identity()
        .await
        .boxed()
        .context(RuntimeSnafu {
            application_id: application_id.clone(),
        })?;
    let security_f = DeliverySecurity::load(
        store.clone(),
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
        &security,
    );
    validation_f
        .await
        .map_err(load_security_error_from_runtime)
        .map_err(|source| security_load_error(application_id.clone(), source))?;
    load_replication_runtime_typed_with_security(
        application_id,
        store,
        listener,
        config,
        security,
        runtime_config_toml,
    )
    .await
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) async fn load_replication_runtime_typed_with_security_for_test(
    application_id: Identifier,
    store: Arc<dyn ReplicationStore>,
    listener: Arc<dyn ReplicationEventListener>,
    config: ReplicationConfig,
    security: DeliverySecurity,
    runtime_config_toml: Option<&str>,
) -> Result<Arc<ReplicationRuntime>, LoadError> {
    load_replication_runtime_typed_with_security(
        application_id,
        store,
        listener,
        config,
        security,
        runtime_config_toml,
    )
    .await
}

async fn load_replication_runtime_typed_with_security(
    application_id: Identifier,
    store: Arc<dyn ReplicationStore>,
    listener: Arc<dyn ReplicationEventListener>,
    config: ReplicationConfig,
    security: DeliverySecurity,
    runtime_config_toml: Option<&str>,
) -> Result<Arc<ReplicationRuntime>, LoadError> {
    let local_member = store
        .local_member_identity()
        .await
        .boxed()
        .context(RuntimeSnafu {
            application_id: application_id.clone(),
        })?;
    let host = DeliveryRuntimeHost::start_with_runtime_config_toml(
        &local_member,
        store,
        listener,
        security,
        runtime_config_toml,
    )
    .await
    .boxed()
    .context(RuntimeSnafu {
        application_id: application_id.clone(),
    })?;
    let runtime_component = host.runtime_component().clone();
    let runtime_ref = runtime_component
        .actor_ref()
        .hold()
        .expect("replication runtime component must expose a strong actor ref");
    let host = Arc::new(host);
    Ok(Arc::new(ReplicationRuntime {
        _application_id: application_id,
        runtime_ref: Some(runtime_ref),
        host,
        _config: config,
    }))
}

/// Concrete application-facing runtime returned by `load_replication_runtime`.
pub(crate) struct ReplicationRuntime {
    _application_id: Identifier,
    runtime_ref: Option<ActorRefStrong<ReplicationRuntimeMessage>>,
    #[cfg_attr(not(test), allow(dead_code))]
    host: Arc<DeliveryRuntimeHost>,
    _config: ReplicationConfig,
}

impl ReplicationRuntime {
    fn runtime_ref(&self) -> &ActorRefStrong<ReplicationRuntimeMessage> {
        self.runtime_ref
            .as_ref()
            .expect("replication runtime shut down already")
    }

    fn ask<T>(
        &self,
        build: impl FnOnce(KPromise<ApiResult<T>>) -> ReplicationRuntimeMessage + Send + 'static,
    ) -> ApiFuture<'static, T>
    where
        T: Send + 'static,
    {
        let future = self.runtime_ref().ask_with(build);
        let logger = self.host.logger().clone();
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
}

impl Drop for ReplicationRuntime {
    fn drop(&mut self) {
        self.runtime_ref.take();
        if let Some(host) = Arc::get_mut(&mut self.host) {
            host.shutdown();
        }
    }
}

impl ReplicationApi for ReplicationRuntime {
    fn publish_changes(&self, request: PublishChangesRequest) -> ApiFuture<'_, PublishReceipt> {
        self.ask(move |promise| {
            ReplicationRuntimeMessage::PublishChanges(Ask::new(promise, request))
        })
    }

    fn snapshot_rows(&self, request: SnapshotRowsRequest) -> ApiFuture<'_, SnapshotRows> {
        self.ask(move |promise| ReplicationRuntimeMessage::SnapshotRows(Ask::new(promise, request)))
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
    ) -> ApiFuture<'_, GroupMigration> {
        self.ask(move |promise| {
            ReplicationRuntimeMessage::ChangeGroupMembership(Ask::new(promise, req))
        })
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
        "timed out waiting for test future to resolve",
    )
}

#[cfg(any(test, feature = "test-support"))]
impl ReplicationRuntime {
    pub(crate) fn host(&self) -> &DeliveryRuntimeHost {
        self.host.as_ref()
    }

    pub(crate) fn install_group_for_test(
        &self,
        group_id: GroupId,
        members: GroupMembers,
    ) -> Result<(), GroupInstallError> {
        let future = self.runtime_ref().ask_with(|promise| {
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
    pub(super) fn apply_update_for_test(
        &self,
        sender: MemberIdentity,
        message: UpdateMessage,
    ) -> Result<(), InboundDeliveryError> {
        let future = self.runtime_ref().ask_with(|promise| {
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
        let future = self.runtime_ref().ask_with(|promise| {
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
}
