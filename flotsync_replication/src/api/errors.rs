use super::{
    StoreSecretKeyId,
    ids::{GroupId, MemberIdentity},
};
use crate::GroupMembersError;
use flotsync_core::member::Identifier;
use flotsync_security::LocalStoreSecretError;
use snafu::{Location, prelude::*};
use std::error::Error;

pub type BoxError = Box<dyn Error + Send + Sync + 'static>;
pub type ApiResult<T> = Result<T, ApiError>;

#[derive(Debug, Snafu)]
pub enum DatasetIdError {
    #[snafu(display("Dataset identifier must not be empty."))]
    Empty,
    #[snafu(display(
        "Dataset identifier '{value}' has an invalid first character. Use [A-Za-z_]."
    ))]
    InvalidStartCharacter { value: String },
    #[snafu(display(
        "Dataset identifier '{value}' contains invalid character '{character}' at byte index {index}. Only [A-Za-z0-9_] are allowed."
    ))]
    InvalidCharacter {
        value: String,
        index: usize,
        character: char,
    },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum RowProviderError {
    #[snafu(display("Row provider failed: {source}"))]
    ProviderExternal { source: BoxError },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum ListenerError {
    #[snafu(display("Listener rejected event: {message}"))]
    Rejected { message: String },
    #[snafu(display("Listener failed: {source}"))]
    ListenerExternal { source: BoxError },
}

impl From<BoxError> for ListenerError {
    fn from(source: BoxError) -> Self {
        Self::ListenerExternal { source }
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum ApiError {
    #[snafu(display("Replication API operation failed: {source}"))]
    ApiExternal { source: BoxError },
    #[snafu(display("Replication runtime component became unavailable."))]
    RuntimeUnavailable,
    #[snafu(display("Timed out waiting for summary from member {target} in group {group_id}."))]
    SummaryTimedOut {
        group_id: GroupId,
        target: MemberIdentity,
    },
    #[snafu(display("Replication runtime operation '{operation}' is not implemented yet."))]
    UnsupportedOperation { operation: &'static str },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum StoreError {
    #[snafu(display("Replication store failed: {source}"))]
    StoreExternal { source: BoxError },
}

/// Security setup failures reported by public replication runtime loading.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum LoadSecurityError {
    /// Device-local store-secret profile loading failed before store records could be opened.
    #[snafu(display("Failed to load local store secret: {source}"))]
    LocalStoreSecret {
        #[snafu(source(from(LocalStoreSecretError, Box::new)))]
        source: Box<LocalStoreSecretError>,
    },
    /// The store does not contain private keys for the local member.
    #[snafu(display("Local private keys for member {member_id} are not provisioned."))]
    MissingLocalPrivateKeys { member_id: MemberIdentity },
    /// The local private-key record exists but cannot be used with the provided setup.
    #[snafu(display("Local private keys for member {member_id} are invalid: {source}"))]
    InvalidLocalPrivateKeys {
        member_id: MemberIdentity,
        source: BoxError,
    },
    /// A persisted group record no longer satisfies canonical member invariants.
    #[snafu(display("Stored replication group {group_id} has invalid members: {source}"))]
    StoredGroupInvalidMembers {
        group_id: GroupId,
        source: GroupMembersError,
    },
    /// A persisted group references a member without locally trusted public keys.
    #[snafu(display(
        "Stored replication group {group_id} is missing trusted public keys for member {member_id}."
    ))]
    StoredGroupMissingTrustedPublicKeys {
        group_id: GroupId,
        member_id: MemberIdentity,
    },
    /// A persisted group references trusted key bytes with the wrong fixed length.
    #[snafu(display(
        "Stored replication group {group_id} has trusted public key bytes for member {member_id} with invalid length {actual}; expected {expected}."
    ))]
    StoredGroupInvalidTrustedPublicKeyLength {
        group_id: GroupId,
        member_id: MemberIdentity,
        expected: usize,
        actual: usize,
    },
    /// A persisted group references trusted public keys that cannot be decoded.
    #[snafu(display(
        "Stored replication group {group_id} has invalid trusted public keys for member {member_id}: {source}"
    ))]
    StoredGroupInvalidTrustedPublicKeys {
        group_id: GroupId,
        member_id: MemberIdentity,
        source: BoxError,
    },
    /// A persisted group was sealed under a different store-secret key id.
    #[snafu(display(
        "Stored replication group {group_id} uses store-secret key id {actual}; expected {expected}."
    ))]
    StoredGroupKeyIdMismatch {
        group_id: GroupId,
        expected: StoreSecretKeyId,
        actual: StoreSecretKeyId,
    },
    /// A persisted group uses a store-secret crypto version this runtime cannot load.
    #[snafu(display(
        "Stored replication group {group_id} uses unsupported store-secret crypto version {version}; supported version is {supported}."
    ))]
    StoredGroupUnsupportedStoreSecretVersion {
        group_id: GroupId,
        version: u16,
        supported: u16,
    },
    /// A persisted group's encrypted group secret has a nonce with the wrong fixed length.
    #[snafu(display(
        "Stored replication group {group_id} has encrypted group-secret nonce length {actual}; expected {expected}."
    ))]
    StoredGroupInvalidGroupSecretNonceLength {
        group_id: GroupId,
        expected: usize,
        actual: usize,
    },
    /// Security loading failed for an internal reason that is not caller-actionable.
    #[snafu(display("Replication security loading failed at {location}: {source}"))]
    Other {
        source: BoxError,
        #[snafu(implicit)]
        location: Location,
    },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum LoadError {
    #[snafu(display("Failed to load replication for application '{application_id}': {source}"))]
    Runtime {
        application_id: Identifier,
        source: BoxError,
    },
    #[snafu(display(
        "Failed to load replication security for application '{application_id}': {source}"
    ))]
    Security {
        application_id: Identifier,
        source: Box<LoadSecurityError>,
    },
    #[snafu(display("Replication runtime is not available for application '{application_id}'."))]
    Unavailable { application_id: Identifier },
}
