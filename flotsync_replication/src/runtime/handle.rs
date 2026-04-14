use super::*;

/// Create one concrete replication runtime for the given application identity.
///
/// `application_id` scopes the loaded runtime instance for diagnostics and future
/// multi-application hosting.
/// `store` provides the local member identity and dataset schemas needed by the
/// first replication slice.
/// `listener` receives replication events produced by inbound delivery.
/// `config` carries public runtime policy knobs; the first slice only honours
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
    let mut host = DeliveryRuntimeHost::new(local_member.clone())
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
///
/// The current slice supports one real end-to-end path:
/// creating a fixed-membership group and installing that membership remotely
/// through reliable delivery bootstrap messages.
pub(super) struct ReplicationRuntime {
    _application_id: Identifier,
    runtime_ref: Option<ActorRefStrong<ReplicationRuntimeMessage>>,
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) host: Arc<DeliveryRuntimeHost>,
    _config: ReplicationConfig,
}

impl ReplicationRuntime {
    fn runtime_ref(&self) -> &ActorRefStrong<ReplicationRuntimeMessage> {
        self.runtime_ref
            .as_ref()
            .expect("replication runtime actor ref must exist while the handle is live")
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
    fn publish_changes(
        &self,
        changes: Vec<RowMutation>,
    ) -> BoxFuture<'_, Result<PublishReceipt, ApiError>> {
        let (promise, future) = promise::<Result<PublishReceipt, ApiError>>();
        self.runtime_ref()
            .tell(ReplicationRuntimeMessage::PublishChanges(Ask::new(
                promise, changes,
            )));
        Box::pin(async move { resolve_runtime_future(future).await })
    }

    fn create_group(
        &self,
        req: CreateGroupRequest,
    ) -> BoxFuture<'_, Result<crate::api::GroupId, ApiError>> {
        let (promise, future) = promise::<Result<GroupId, ApiError>>();
        self.runtime_ref()
            .tell(ReplicationRuntimeMessage::CreateGroup(Ask::new(
                promise, req,
            )));
        Box::pin(async move { resolve_runtime_future(future).await })
    }

    fn change_group_membership(
        &self,
        req: ChangeGroupMembershipRequest,
    ) -> BoxFuture<'_, Result<GroupMigration, ApiError>> {
        let (promise, future) = promise::<Result<GroupMigration, ApiError>>();
        self.runtime_ref()
            .tell(ReplicationRuntimeMessage::ChangeGroupMembership(Ask::new(
                promise, req,
            )));
        Box::pin(async move { resolve_runtime_future(future).await })
    }
}

async fn resolve_runtime_future<T: Send>(
    future: KFuture<Result<T, ApiError>>,
) -> Result<T, ApiError> {
    match future.await {
        Ok(reply) => reply,
        Err(_) => Err(ApiError::ApiExternal {
            source: Box::new(io::Error::other(
                "replication runtime component became unavailable",
            )),
        }),
    }
}

#[cfg(test)]
pub(super) fn wait_for_test_future<F>(future: F) -> F::Output
where
    F: Future,
{
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "timed out waiting for test future to resolve"
                );
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
    }
}

#[cfg(test)]
impl ReplicationRuntime {
    pub(super) fn install_group_for_test(
        &self,
        group_id: GroupId,
        members: GroupMembers,
    ) -> Result<(), GroupInstallError> {
        let (promise, future) = promise::<Result<(), GroupInstallError>>();
        self.runtime_ref()
            .tell(ReplicationRuntimeMessage::InstallGroupForTest(Ask::new(
                promise,
                TestInstallGroup { group_id, members },
            )));
        match wait_for_test_future(future) {
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
        let (promise, future) = promise::<Result<(), InboundDeliveryError>>();
        self.runtime_ref()
            .tell(ReplicationRuntimeMessage::ApplyUpdateBatchForTest(
                Ask::new(promise, TestApplyUpdateBatch { sender, message }),
            ));
        match wait_for_test_future(future) {
            Ok(reply) => reply,
            Err(_) => {
                panic!(
                    "replication runtime component became unavailable during test apply_update_batch"
                )
            }
        }
    }
}
