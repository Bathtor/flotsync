use super::*;
type ApiFuture<'a, T> = BoxFuture<'a, ApiResult<T>>;

/// Create one concrete replication runtime for the given application identity.
///
/// `application_id` scopes the loaded runtime instance for diagnostics and future
/// multi-application hosting.
/// `store` provides the local member identity and dataset schemas needed by the
/// current replication runtime.
/// `listener` receives replication events produced by inbound delivery.
/// `config` carries public runtime policy knobs; the current runtime only honours
/// the migration-policy shape while the deeper protocol remains unimplemented.
pub async fn load_replication_runtime(
    application_id: Identifier,
    store: Arc<dyn ReplicationStore>,
    listener: Arc<dyn ReplicationEventListener>,
    config: ReplicationConfig,
) -> Result<Arc<dyn ReplicationApi>, LoadError> {
    let runtime = load_replication_runtime_typed(application_id, store, listener, config).await?;
    Ok(runtime)
}

pub(super) async fn load_replication_runtime_typed(
    application_id: Identifier,
    store: Arc<dyn ReplicationStore>,
    listener: Arc<dyn ReplicationEventListener>,
    config: ReplicationConfig,
) -> Result<Arc<ReplicationRuntime>, LoadError> {
    let local_member = store
        .local_member_identity()
        .await
        .boxed()
        .context(RuntimeSnafu {
            application_id: application_id.clone(),
        })?;
    let mut host = DeliveryRuntimeHost::start(local_member.clone())
        .boxed()
        .context(RuntimeSnafu {
            application_id: application_id.clone(),
        })?;
    let runtime_component = host
        .start_runtime_component(local_member, store, listener)
        .boxed()
        .context(RuntimeSnafu {
            application_id: application_id.clone(),
        })?;
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
pub(super) struct ReplicationRuntime {
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
        Box::pin(async move {
            match future.await {
                Ok(reply) => reply,
                Err(_) => Err(ApiError::RuntimeUnavailable),
            }
        })
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
    fn publish_changes(&self, changes: Vec<RowMutation>) -> ApiFuture<'_, PublishReceipt> {
        self.ask(move |promise| {
            ReplicationRuntimeMessage::PublishChanges(Ask::new(promise, changes))
        })
    }

    fn create_group(&self, req: CreateGroupRequest) -> ApiFuture<'_, crate::api::GroupId> {
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

#[cfg(test)]
pub(super) fn wait_for_test_reply<F>(future: F) -> F::Output
where
    F: Future,
{
    let mut future = pin!(future);
    flotsync_io::test_support::eventually_value(
        std::time::Duration::from_secs(5),
        || {
            let waker = Waker::noop();
            let mut context = Context::from_waker(waker);
            match future.as_mut().poll(&mut context) {
                Poll::Ready(value) => Some(value),
                Poll::Pending => None,
            }
        },
        "timed out waiting for test future to resolve",
    )
}

#[cfg(test)]
impl ReplicationRuntime {
    pub(super) fn host(&self) -> &DeliveryRuntimeHost {
        self.host.as_ref()
    }

    pub(super) fn install_group_for_test(
        &self,
        group_id: GroupId,
        members: GroupMembers,
    ) -> Result<(), GroupInstallError> {
        let future = self.runtime_ref().ask_with(|promise| {
            ReplicationRuntimeMessage::test_install_group(promise, group_id, members)
        });
        match wait_for_test_reply(future) {
            Ok(reply) => reply,
            Err(_) => {
                panic!("replication runtime component became unavailable during test install")
            }
        }
    }

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
            Err(_) => {
                panic!(
                    "replication runtime component became unavailable during test apply_update_batch"
                )
            }
        }
    }
}
