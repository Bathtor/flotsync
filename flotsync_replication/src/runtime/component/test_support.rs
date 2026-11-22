//! Fixture-only runtime commands and their mailbox execution adapters.
//!
//! Feature builds expose installation only; unit tests also inject inbound work
//! and inspect progress through the same transactional/listener paths as production.

use super::{ReplicationRuntimeComponent, ReplicationRuntimeMessage};
#[cfg(test)]
use super::{inbound_flow::InboundRequest, inbound_support::InboundUpdateOrigin};
use crate::{
    api::{GroupMemberKeys, GroupSchema, MemberKeyId},
    runtime::errors::GroupInstallError,
    test_support::{test_group_key, test_public_member_keys},
};
#[cfg(test)]
use crate::{
    api::{GroupReadToken, PendingGroupDecisionRecord, StoreError},
    codecs::messages::{GroupSetupMessage, UpdateBatchMessage, UpdateMessage},
    runtime::errors::InboundDeliveryError,
};
#[cfg(test)]
use flotsync_core::MemberIdentity;
use flotsync_core::{GroupId, membership::GroupMembers};
use flotsync_data_types::schema::datamodel::SchemaSource;
#[cfg(test)]
use flotsync_utils::ResultExt as _;
use kompact::prelude::*;
#[cfg(test)]
use std::sync::Arc;

/// Fixture commands available only to unit tests or the test-support feature.
#[cfg(any(test, feature = "test-support"))]
#[derive(Debug)]
#[allow(
    private_interfaces,
    reason = "test-only ask plumbing reuses internal error types"
)]
pub enum ReplicationRuntimeTestMessage {
    /// Confirm that the runtime component is alive and able to process one
    /// mailbox turn after startup.
    #[cfg(test)]
    Ping(Ask<(), ()>),
    /// Install a group directly in runtime state for integration scenarios that
    /// need pre-existing group membership until production group setup exists.
    InstallGroup(Ask<(GroupId, GroupMembers), Result<(), GroupInstallError>>),
    /// Read one group's current store position for publication-oriented unit tests.
    #[cfg(test)]
    ReadGroupToken(Ask<GroupId, Result<Option<GroupReadToken>, StoreError>>),
    /// Apply one inbound update directly to runtime logic tests.
    #[cfg(test)]
    ApplyUpdate(Ask<(MemberIdentity, UpdateMessage), Result<(), InboundDeliveryError>>),
    /// Apply one inbound update batch directly to runtime logic tests.
    #[cfg(test)]
    ApplyUpdateBatch(Ask<(MemberIdentity, UpdateBatchMessage), Result<(), InboundDeliveryError>>),
    /// Apply one inbound pending-group message directly to runtime logic tests.
    #[cfg(test)]
    ApplyPendingGroup(PendingGroupTestAsk),
}

/// Test-only pending-group input kept boxed to avoid inflating the command enum.
#[cfg(test)]
type PendingGroupTestInput = (
    MemberIdentity,
    Box<PendingGroupDecisionRecord>,
    Arc<GroupSetupMessage>,
);

/// Test-only request used to inject pending-group work without transport.
#[cfg(test)]
type PendingGroupTestAsk = Ask<PendingGroupTestInput, Result<(), InboundDeliveryError>>;

// TODO(flotsync-zop): In flotsync-zop.24, move these fixture constructors beside
// ReplicationRuntimeMessage in messages.rs or replace this cross-file impl with
// fixture-local free builders, preserving cfg gates and runtime-level visibility.
#[cfg(any(test, feature = "test-support"))]
impl ReplicationRuntimeMessage {
    /// Build a unit-test liveness request for one mailbox turn.
    #[cfg(test)]
    pub(in crate::runtime) fn test_ping(promise: KPromise<()>) -> Self {
        Self::Test(ReplicationRuntimeTestMessage::Ping(Ask::new(promise, ())))
    }

    /// Build a fixture installation request using the supplied canonical members.
    pub(in crate::runtime) fn test_install_group(
        promise: KPromise<Result<(), GroupInstallError>>,
        group_id: GroupId,
        members: GroupMembers,
    ) -> Self {
        Self::Test(ReplicationRuntimeTestMessage::InstallGroup(Ask::new(
            promise,
            (group_id, members),
        )))
    }

    /// Build a unit-test request for the group's current store position.
    #[cfg(test)]
    pub(in crate::runtime) fn test_read_group_token(
        promise: KPromise<Result<Option<GroupReadToken>, StoreError>>,
        group_id: GroupId,
    ) -> Self {
        Self::Test(ReplicationRuntimeTestMessage::ReadGroupToken(Ask::new(
            promise, group_id,
        )))
    }

    /// Inject one authenticated producer update without a transport envelope.
    #[cfg(test)]
    pub(in crate::runtime) fn test_apply_update(
        promise: KPromise<Result<(), InboundDeliveryError>>,
        sender: MemberIdentity,
        message: UpdateMessage,
    ) -> Self {
        Self::Test(ReplicationRuntimeTestMessage::ApplyUpdate(Ask::new(
            promise,
            (sender, message),
        )))
    }

    /// Inject one forwarded update batch without a transport envelope.
    #[cfg(test)]
    pub(in crate::runtime) fn test_apply_update_batch(
        promise: KPromise<Result<(), InboundDeliveryError>>,
        sender: MemberIdentity,
        message: UpdateBatchMessage,
    ) -> Self {
        Self::Test(ReplicationRuntimeTestMessage::ApplyUpdateBatch(Ask::new(
            promise,
            (sender, message),
        )))
    }

    /// Inject pending group work with the setup that the sender advertised.
    #[cfg(test)]
    pub(in crate::runtime) fn test_apply_pending_group(
        promise: KPromise<Result<(), InboundDeliveryError>>,
        sender: MemberIdentity,
        record: PendingGroupDecisionRecord,
        group_setup: Arc<GroupSetupMessage>,
    ) -> Self {
        Self::Test(ReplicationRuntimeTestMessage::ApplyPendingGroup(Ask::new(
            promise,
            (sender, Box::new(record), group_setup),
        )))
    }
}

// TODO(flotsync-zop): Retire this temporary component extension in flotsync-zop.24
// using fixture-local helpers and thin entry points in component/mod.rs. Every
// component impl must be beside its declaration there before parent completion.
impl ReplicationRuntimeComponent {
    /// Dispatch a fixture command without adding test branches to production ingress.
    pub(super) fn handle_test_message(
        &mut self,
        message: ReplicationRuntimeTestMessage,
    ) -> HandlerResult {
        match message {
            ReplicationRuntimeTestMessage::InstallGroup(ask) => self.handle_test_install_group(ask),
            #[cfg(test)]
            ReplicationRuntimeTestMessage::Ping(ask) => self.handle_test_ping(ask),
            #[cfg(test)]
            ReplicationRuntimeTestMessage::ReadGroupToken(ask) => {
                self.handle_test_read_group_token(ask)
            }
            #[cfg(test)]
            ReplicationRuntimeTestMessage::ApplyUpdate(ask) => self.handle_test_apply_update(ask),
            #[cfg(test)]
            ReplicationRuntimeTestMessage::ApplyUpdateBatch(ask) => {
                self.handle_test_apply_update_batch(ask)
            }
            #[cfg(test)]
            ReplicationRuntimeTestMessage::ApplyPendingGroup(ask) => {
                self.handle_test_apply_pending_group(ask)
            }
        }
    }

    /// Install fixture group material through the ordinary persistence/publication path.
    #[cfg(any(test, feature = "test-support"))]
    fn handle_test_install_group(
        &mut self,
        ask: Ask<(GroupId, GroupMembers), Result<(), GroupInstallError>>,
    ) -> HandlerResult {
        let (promise, (group_id, members)) = ask.take();
        // TODO(flotsync-sec.9): Remove this test-only message once production
        // group creation installs real group secrets through the normal API.
        let group_key = test_group_key(group_id);
        let security_material = match self.security.seal_group_secret(group_id.0, &group_key) {
            Ok(security_material) => security_material,
            Err(source) => {
                let _ = promise.fulfil(Err(GroupInstallError::TestGroupSecurity {
                    group_id,
                    source,
                }));
                return Handled::OK;
            }
        };
        let member_keys = members
            .ordered_members()
            .into_iter()
            .map(|member_id| MemberKeyId {
                fingerprint: test_public_member_keys(&member_id).fingerprint(),
                member_id,
            })
            .collect::<Vec<_>>();
        let member_keys = GroupMemberKeys::from_ordered_member_keys(member_keys)
            .expect("test install group members were already validated");
        let group_schema = GroupSchema::new(
            self.group_memberships
                .application_schemas()
                .iter()
                .map(|(dataset_id, schema)| (dataset_id, SchemaSource::Static(schema)))
                .collect(),
        );
        let record = self.build_replication_group_record(
            group_id,
            None,
            member_keys,
            group_schema,
            security_material,
        );
        Handled::block_on(self, async move |mut async_self| {
            let reply = async_self
                .store_new_replication_group(record)
                .await
                .map(|_persisted_group| ());
            let _ = promise.fulfil(reply);
            Handled::OK
        })
    }

    /// Reply with the current stored position without blocking the fixture mailbox.
    #[cfg(test)]
    fn handle_test_read_group_token(
        &mut self,
        ask: Ask<GroupId, Result<Option<GroupReadToken>, StoreError>>,
    ) -> HandlerResult {
        let (promise, group_id) = ask.take();
        self.spawn_local(async move |mut async_self| {
            let reply = async_self.load_group_read_token_for_test(group_id).await;
            if promise.fulfil(reply).is_err() {
                warn!(async_self.log(), "dropping test group read-token reply");
            }
            Handled::OK
        });
        Handled::OK
    }

    /// Complete a liveness probe once the component receives its mailbox input.
    #[cfg(test)]
    #[allow(
        clippy::unused_self,
        reason = "Kompact test messages use the same component method shape as production handlers."
    )]
    fn handle_test_ping(&mut self, ask: Ask<(), ()>) -> HandlerResult {
        let (promise, ()) = ask.take();
        let _ = promise.fulfil(());
        Handled::OK
    }

    /// Apply one input through production admission, observations, callbacks and terminal release.
    #[cfg(test)]
    fn handle_test_apply_update(
        &mut self,
        ask: Ask<(MemberIdentity, UpdateMessage), Result<(), InboundDeliveryError>>,
    ) -> HandlerResult {
        let (promise, (sender, message)) = ask.take();
        self.handle_test_inbound_apply(
            promise,
            InboundRequest::Single {
                origin: InboundUpdateOrigin::Producer { sender },
                message,
            },
        )
    }

    /// Run the production per-input commit/callback sequence for a fixture batch.
    #[cfg(test)]
    fn handle_test_apply_update_batch(
        &mut self,
        ask: Ask<(MemberIdentity, UpdateBatchMessage), Result<(), InboundDeliveryError>>,
    ) -> HandlerResult {
        let (promise, (batch_sender, message)) = ask.take();
        self.handle_test_inbound_apply(
            promise,
            InboundRequest::Batch {
                sender: batch_sender,
                message,
            },
        )
    }

    /// Drive either fixture input family through the shared production flow and answer its ask.
    /// A dropped receiver does not interrupt the flow or prevent terminal ownership release.
    #[cfg(test)]
    fn handle_test_inbound_apply(
        &mut self,
        promise: KPromise<Result<(), InboundDeliveryError>>,
        request: InboundRequest,
    ) -> HandlerResult {
        Handled::block_on(self, async move |mut async_self| {
            let reply = async_self
                .apply_inbound_for_test(request)
                .await
                .whatever_unrecoverable("test inbound driver violated ownership/phase protocol")?;
            let _ = promise.fulfil(reply);
            Handled::OK
        })
    }

    /// Install fixture pending work, preserving fatal post-commit activation failures.
    #[cfg(test)]
    fn handle_test_apply_pending_group(&mut self, ask: PendingGroupTestAsk) -> HandlerResult {
        let (promise, (sender, record, group_setup)) = ask.take();
        Handled::block_on(self, async move |mut async_self| {
            let reply = async_self
                .install_pending_group_delivery(*record, group_setup, sender)
                .await;
            let reply = match reply {
                Err(error) if error.is_unrecoverable() => {
                    return Err(error).whatever_unrecoverable(
                        "test pending group activation failed after commit",
                    );
                }
                reply => reply,
            };
            let _ = promise.fulfil(reply);
            Handled::OK
        })
    }

    /// Load one current group position without restoring the removed snapshot API.
    ///
    /// `Some` is the stored group's token; `None` means the group is absent.
    #[cfg(test)]
    async fn load_group_read_token_for_test(
        &mut self,
        group_id: GroupId,
    ) -> Result<Option<GroupReadToken>, StoreError> {
        let mut transaction = self.store.begin_read_transaction().await?;
        let group = transaction.load_replication_group(&group_id).await?;
        transaction.release().await?;
        Ok(group.map(|group| GroupReadToken::from_group_version(group_id, group.version_vector)))
    }
}
