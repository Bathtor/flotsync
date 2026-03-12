use flotsync_core::versions::UpdateId;
use flotsync_data_types::{
    Decode,
    DecodeValueError,
    InMemoryFieldValue,
    schema::datamodel::NullableBasicValue,
};
use flotsync_utils::BoxFuture;
use smallvec::SmallVec;
use std::{borrow::Cow, collections::HashMap, num::NonZeroUsize, sync::Arc};

mod errors;
mod ids;
pub mod providers;

pub use errors::*;
pub use ids::*;

/// Immutable row view exposed on the event/read side.
///
/// This trait is object-safe. `get_field_value` and `get_nullable_field_value` helpers are
/// available on `dyn RowRead`.
pub trait RowRead: Send + Sync {
    fn get_field(&self, field_name: &str) -> Option<&InMemoryFieldValue<UpdateId>>;
}

impl dyn RowRead + '_ {
    pub fn get_field_value<T>(&self, field_name: &str) -> Result<Cow<'_, T>, DecodeValueError>
    where
        T: ?Sized + Decode<UpdateId>,
    {
        let field_value =
            self.get_field(field_name)
                .ok_or_else(|| DecodeValueError::FieldDoesNotExist {
                    field_name: field_name.to_owned(),
                })?;
        T::decode(field_value)
    }

    pub fn get_nullable_field_value<T>(
        &self,
        field_name: &str,
    ) -> Result<Option<Cow<'_, T>>, DecodeValueError>
    where
        T: ?Sized + Decode<UpdateId>,
    {
        let field_value =
            self.get_field(field_name)
                .ok_or_else(|| DecodeValueError::FieldDoesNotExist {
                    field_name: field_name.to_owned(),
                })?;
        match T::decode(field_value) {
            Ok(value) => Ok(Some(value)),
            Err(DecodeValueError::NullValue { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }
}

/// Write-only row payload submitted by applications.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MutableRow {
    /// Desired nullable field values keyed by field name.
    pub fields: HashMap<String, NullableBasicValue>,
}

impl MutableRow {
    pub fn new(fields: HashMap<String, NullableBasicValue>) -> Self {
        Self { fields }
    }
}

/// Convenience macro to build a [`MutableRow`] inline.
#[macro_export]
macro_rules! row_values {
    ($($field:expr => $value:expr),* $(,)?) => {{
        let mut fields: ::std::collections::HashMap<
            ::std::string::String,
            ::flotsync_data_types::schema::datamodel::NullableBasicValue,
        > = ::std::collections::HashMap::new();
        $(
            fields.insert(($field).to_string(), ($value).into());
        )*
        $crate::api::MutableRow::new(fields)
    }};
}

/// A row-level mutation submitted by an application.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RowMutation {
    Upsert { row_id: RowId, row: MutableRow },
    Delete { row_id: RowId },
}

/// Row-level change emitted by the framework to an application listener.
pub enum RowChange {
    Upsert {
        row_id: RowId,
        row: Arc<dyn RowRead>,
    },
    Delete {
        row_id: RowId,
    },
}

impl RowChange {
    pub fn row_id(&self) -> &RowId {
        match self {
            RowChange::Upsert { row_id, .. } | RowChange::Delete { row_id } => row_id,
        }
    }

    pub fn row(&self) -> Option<&dyn RowRead> {
        match self {
            RowChange::Upsert { row, .. } => Some(row.as_ref()),
            RowChange::Delete { .. } => None,
        }
    }
}

/// Batch of row changes emitted by a [`RowProvider`].
///
/// An empty batch marks end-of-stream.
pub type RowChangeBatch = SmallVec<[RowChange; 4]>;

/// Source for batched row-level changes carried in `ReplicationEvent::DataChanged`.
///
/// Returning an empty batch signals end-of-stream.
pub trait RowProvider: Send {
    fn next_batch<'a>(&'a mut self) -> BoxFuture<'a, Result<RowChangeBatch, RowProviderError>>;
}

/// One row entry in an initial dataset state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitialRowState {
    pub row_key: RowKey,
    pub row: MutableRow,
}

/// Batch type used by [`InitialRowProvider`].
///
/// An empty batch marks end-of-stream.
pub type InitialRowBatch = Vec<InitialRowState>;

/// Source for one dataset's initial rows in `InitialGroupState`.
///
/// `reuse` is owned and returned so callers can keep allocation capacity across calls.
/// Returning an empty batch signals end-of-stream.
pub trait InitialRowProvider: Send {
    fn next_batch<'a>(
        &'a mut self,
        reuse: InitialRowBatch,
        max_rows: NonZeroUsize,
    ) -> BoxFuture<'a, Result<InitialRowBatch, RowProviderError>>;
}

/// Initial state rows for one dataset.
pub struct InitialDatasetState {
    pub dataset_id: DatasetId,
    pub rows: Box<dyn InitialRowProvider>,
}

/// Initial group state grouped by dataset.
#[derive(Default)]
pub struct InitialGroupState {
    pub datasets: Vec<InitialDatasetState>,
}

/// How to handle a particular group migration change.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum GroupMigrationPolicyBehaviour {
    /// Accept incoming invitations automatically.
    AutoAccept,
    /// Forward invitations to the application listener for an explicit decision.
    ViaListener,
    /// Reject incoming invitations automatically.
    AutoReject,
}

/// Policy for handling membership migration invitations.
///
/// If more than one behaviour applies to a change, the most restrictive behaviour is used.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct GroupMigrationPolicy {
    /// Applied if the migration contains only an epoch change, so that the group does not run out
    /// of change versions.
    pub epoch_change: GroupMigrationPolicyBehaviour,
    /// Applied when a new top-level member was added to the group.
    pub member_added: GroupMigrationPolicyBehaviour,
    /// Applied when a member added a new device to the group.
    pub member_device_added: GroupMigrationPolicyBehaviour,
    /// Applied when a top-level member is removed from the group.
    pub member_removed: GroupMigrationPolicyBehaviour,
    /// Applied when a member removes one of their devices from the group.
    pub member_device_removed: GroupMigrationPolicyBehaviour,
}

impl Default for GroupMigrationPolicy {
    fn default() -> Self {
        Self {
            epoch_change: GroupMigrationPolicyBehaviour::AutoAccept,
            member_added: GroupMigrationPolicyBehaviour::ViaListener,
            member_device_added: GroupMigrationPolicyBehaviour::AutoAccept,
            member_removed: GroupMigrationPolicyBehaviour::ViaListener,
            member_device_removed: GroupMigrationPolicyBehaviour::AutoAccept,
        }
    }
}

/// Runtime configuration passed during `load`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicationConfig {
    pub group_migration_policy: GroupMigrationPolicy,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            group_migration_policy: Default::default(),
        }
    }
}

/// Request to create a new replication group.
pub struct CreateGroupRequest {
    pub members: Vec<MemberIdentity>,
    pub initial_state: Option<InitialGroupState>,
}

/// Request to change membership of an existing group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangeGroupMembershipRequest {
    pub group_id: GroupId,
    pub add_members: Vec<MemberIdentity>,
    pub remove_members: Vec<MemberIdentity>,
}

/// Receipt returned by `publish_changes`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PublishReceipt {
    pub update_id: UpdateId,
}

/// Invitation details surfaced when migration policy requires listener mediation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupInvitation {
    pub migration: GroupMigration,
    pub proposed_members: Vec<MemberIdentity>,
    pub group_name: Option<String>,
    pub message: Option<String>,
}

/// Listener-visible replication events.
pub enum ReplicationEvent {
    DataChanged {
        rows: Box<dyn RowProvider>,
    },
    GroupInvitation {
        invitation: GroupInvitation,
        respond: Box<dyn GroupInvitationResponder>,
    },
}

/// Callback for applications to accept or reject one migration invitation.
///
/// The callback is one-shot by construction: calling either method consumes the responder.
pub trait GroupInvitationResponder: Send {
    /// Join the new group and consider the old group (if any) closed.
    fn accept(self: Box<Self>) -> BoxFuture<'static, Result<(), ApiError>>;

    /// Refuse to join the new group, ignore all its messages.
    fn reject(self: Box<Self>, reason: RejectionReason)
    -> BoxFuture<'static, Result<(), ApiError>>;
}

/// Optional rejection reasons reported back to the framework.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RejectionReason {
    PolicyDenied,
    UserDenied,
    UnsupportedSchemaEpoch,
    IncompatibleMembership,
    Other(String),
}

/// Application listener callback interface.
pub trait ReplicationEventListener: Send + Sync {
    fn on_event(&self, event: ReplicationEvent) -> BoxFuture<'_, Result<(), ListenerError>>;
}

/// Application-facing replication control surface.
pub trait ReplicationApi: Send + Sync {
    fn publish_changes(
        &self,
        changes: Vec<RowMutation>,
    ) -> BoxFuture<'_, Result<PublishReceipt, ApiError>>;

    fn create_group(&self, req: CreateGroupRequest) -> BoxFuture<'_, Result<GroupId, ApiError>>;

    fn change_group_membership(
        &self,
        req: ChangeGroupMembershipRequest,
    ) -> BoxFuture<'_, Result<GroupMigration, ApiError>>;
}

/// Persistence extension point.
pub trait ReplicationStore: Send + Sync {
    // TODO(`flotsync-73q`): Complete the storage contract.
}
