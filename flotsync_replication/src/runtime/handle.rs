#[cfg(any(test, feature = "test-support"))]
use super::host::DeliveryRuntimeHostTestExt;
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
        ApiExternalSnafu,
        ApiResult,
        ChangeGroupMembershipRequest,
        CreateGroupRequest,
        LoadError,
        MigrationId,
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
        security::{
            AssessPublicKeyBundleRequest,
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
use flotsync_core::{GroupId, member::Identifier};
use flotsync_security::PublicKeyBundle;
use flotsync_utils::BoxFuture;
use futures_util::{FutureExt, future};
use kompact::{KompactLogger, prelude::*};
use snafu::prelude::*;
use std::sync::{Arc, RwLock};

#[cfg(any(test, feature = "test-support"))]
use super::errors::GroupInstallError;
#[cfg(test)]
use super::errors::InboundDeliveryError;
#[cfg(test)]
use crate::codecs::messages::{UpdateBatchMessage, UpdateMessage};
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
    let logger = host.logger().clone();
    Ok(Arc::new(ReplicationRuntime {
        _application_id: application_id,
        lifecycle: RwLock::new(Some(RuntimeLifecycle { runtime_ref, host })),
        logger,
        _config: config,
    }))
}

/// Concrete application-facing runtime returned by `load_replication_runtime`.
pub(crate) struct ReplicationRuntime {
    _application_id: Identifier,
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
            host.shutdown().await.boxed().context(ApiExternalSnafu)
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
    ) -> ApiFuture<'_, MigrationId> {
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

    pub(crate) fn membership_snapshot_for_test(&self) -> Arc<GroupMemberships> {
        self.with_host_for_test(DeliveryRuntimeHost::membership_snapshot)
    }

    pub(crate) fn advertised_loopback_udp_addr_for_test(&self) -> std::net::SocketAddr {
        self.with_host_for_test(DeliveryRuntimeHostTestExt::advertised_loopback_udp_addr)
    }

    pub(crate) fn publish_direct_peer_route_for_test(
        &self,
        peer: MemberIdentity,
        remote_addr: std::net::SocketAddr,
    ) {
        self.with_host_for_test(|host| host.publish_direct_peer_route(peer, remote_addr));
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
}
