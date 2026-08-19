use super::{
    StoreError,
    StoreErrorClassification,
    StoreErrorClassificationSource,
    StoreSecretKeyId,
};
use flotsync_core::{ApplicationId, GroupId, MemberIdentity, membership::GroupMembersError};
use flotsync_security::LocalStoreSecretError;
pub use flotsync_utils::BoxError;
use snafu::{Location, prelude::*};

pub type ApiResult<T> = Result<T, ApiError>;

/// Invalid replication dataset identifier.
#[derive(Debug, Snafu)]
pub enum DatasetIdError {
    /// The supplied identifier contains no characters.
    #[snafu(display("Dataset identifier must not be empty."))]
    Empty,
    /// The supplied identifier does not begin with an ASCII letter or underscore.
    #[snafu(display(
        "Dataset identifier '{value}' has an invalid first character. Use [A-Za-z_]."
    ))]
    InvalidStartCharacter {
        /// Complete invalid identifier supplied by the caller.
        value: String,
    },
    /// The supplied identifier contains an invalid character after its first character.
    #[snafu(display(
        "Dataset identifier '{value}' contains invalid character '{character}' at byte index {index}. Only [A-Za-z0-9_] are allowed."
    ))]
    InvalidCharacter {
        /// Complete invalid identifier supplied by the caller.
        value: String,
        /// Byte index of `character` within `value`.
        index: usize,
        /// Character rejected by dataset identifier validation.
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
#[snafu(visibility(pub(crate)), module(api_error))]
pub enum ApiError {
    #[snafu(display("Replication API operation failed: {source}"))]
    ApiExternal { source: BoxError },
    /// A replication API operation failed due to a store error.
    /// The error chain supplied a classification for this error.
    #[snafu(display("Replication API store operation failed [{classification}]: {source}"))]
    StoreExternal {
        /// Typed store classification retained before boxing the concrete source.
        classification: StoreErrorClassification,
        /// Concrete operation error retained for diagnostics and downcasting.
        source: BoxError,
    },
    #[snafu(display("Replication runtime component became unavailable."))]
    RuntimeUnavailable,
    #[snafu(display("Replication runtime lifecycle state was poisoned while {operation}."))]
    RuntimeLifecyclePoisoned { operation: &'static str },
    #[snafu(display("Timed out waiting for summary from member {target} in group {group_id}."))]
    SummaryTimedOut {
        group_id: GroupId,
        target: MemberIdentity,
    },
    #[snafu(display("Replication runtime operation '{operation}' is not implemented yet."))]
    UnsupportedOperation { operation: &'static str },
}

impl ApiError {
    /// Box one error while preserving an explicitly exposed store classification.
    ///
    /// Call sites opt into this conversion only for error types whose store-bearing variants
    /// should remain programmatically visible. An unclassified source remains an ordinary
    /// [`Self::ApiExternal`] error; it is not assigned an unknown store classification.
    pub(crate) fn from_store_classification_source<E>(source: E) -> Self
    where
        E: StoreErrorClassificationSource + Into<BoxError>,
    {
        if let Some(classification) = source.store_error_classification() {
            // Snapshot the small classification at this type-erasure boundary so this variant is
            // always classified while `BoxError` retains ordinary Snafu sourcing and downcasting.
            Self::StoreExternal {
                classification,
                source: source.into(),
            }
        } else {
            Self::ApiExternal {
                source: source.into(),
            }
        }
    }
}

impl StoreErrorClassificationSource for ApiError {
    fn store_error_classification(&self) -> Option<StoreErrorClassification> {
        match self {
            Self::StoreExternal { classification, .. } => Some(*classification),
            Self::ApiExternal { .. }
            | Self::RuntimeUnavailable
            | Self::RuntimeLifecyclePoisoned { .. }
            | Self::SummaryTimedOut { .. }
            | Self::UnsupportedOperation { .. } => None,
        }
    }
}

/// Security setup failures reported by public replication runtime loading.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), module(load_security_error))]
pub enum LoadSecurityError {
    /// Replication-store access failed while loading security state.
    #[snafu(display("Failed to access replication security records [{classification}]: {source}"))]
    StoreAccess {
        /// Typed classification retained before boxing the internal security wrapper.
        classification: StoreErrorClassification,
        /// Concrete security-loading error retained for diagnostics and downcasting.
        source: BoxError,
    },
    /// Device-local store-secret profile loading failed before store records could be opened.
    #[snafu(display("Failed to load local store secret: {source}"))]
    LocalStoreSecret {
        #[snafu(source(from(LocalStoreSecretError, Box::new)))]
        source: Box<LocalStoreSecretError>,
    },
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
    /// A persisted group references a member without a permitted public key bundle.
    #[snafu(display(
        "Stored replication group {group_id} does not have permitted public keys for member {member_id}."
    ))]
    StoredGroupMissingPermittedPublicKeys {
        group_id: GroupId,
        member_id: MemberIdentity,
    },
    /// A persisted group references a member with multiple permitted public key bundles.
    #[snafu(display(
        "Stored replication group {group_id} has {permitted_count} permitted public keys for member {member_id}."
    ))]
    StoredGroupAmbiguousPermittedPublicKeys {
        group_id: GroupId,
        member_id: MemberIdentity,
        permitted_count: usize,
    },
    /// A persisted group references member public-key bytes with the wrong fixed length.
    #[snafu(display(
        "Stored replication group {group_id} has public-key bytes for member {member_id} with invalid length {actual}; expected {expected}."
    ))]
    StoredGroupInvalidMemberPublicKeyLength {
        group_id: GroupId,
        member_id: MemberIdentity,
        expected: usize,
        actual: usize,
    },
    /// A persisted group references member public keys that cannot be decoded.
    #[snafu(display(
        "Stored replication group {group_id} has invalid public keys for member {member_id}: {source}"
    ))]
    StoredGroupInvalidMemberPublicKeys {
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

impl StoreErrorClassificationSource for LoadSecurityError {
    fn store_error_classification(&self) -> Option<StoreErrorClassification> {
        match self {
            Self::StoreAccess { classification, .. } => Some(*classification),
            Self::LocalStoreSecret { .. }
            | Self::InvalidLocalPrivateKeys { .. }
            | Self::StoredGroupInvalidMembers { .. }
            | Self::StoredGroupMissingPermittedPublicKeys { .. }
            | Self::StoredGroupAmbiguousPermittedPublicKeys { .. }
            | Self::StoredGroupInvalidMemberPublicKeyLength { .. }
            | Self::StoredGroupInvalidMemberPublicKeys { .. }
            | Self::StoredGroupKeyIdMismatch { .. }
            | Self::StoredGroupUnsupportedStoreSecretVersion { .. }
            | Self::StoredGroupInvalidGroupSecretNonceLength { .. }
            | Self::Other { .. } => None,
        }
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), module(load_error))]
pub enum LoadError {
    /// Replication-store access failed before the runtime could start.
    #[snafu(display(
        "Failed to access the replication store for application '{application_id}': {source}"
    ))]
    StoreAccess {
        application_id: ApplicationId,
        source: StoreError,
    },
    #[snafu(display("Failed to load replication for application '{application_id}': {source}"))]
    Runtime {
        application_id: ApplicationId,
        source: BoxError,
    },
    #[snafu(display(
        "Failed to load replication security for application '{application_id}': {source}"
    ))]
    Security {
        application_id: ApplicationId,
        source: Box<LoadSecurityError>,
    },
    #[snafu(display("Replication runtime is not available for application '{application_id}'."))]
    Unavailable { application_id: ApplicationId },
}

impl StoreErrorClassificationSource for LoadError {
    fn store_error_classification(&self) -> Option<StoreErrorClassification> {
        match self {
            Self::StoreAccess { source, .. } => source.store_error_classification(),
            Self::Security { source, .. } => source.store_error_classification(),
            Self::Runtime { .. } | Self::Unavailable { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{StoreErrorClass, StoreErrorResolution, StoreErrorScope};
    use std::{error::Error as StdError, fmt};

    /// Test error which explicitly reports that it has no store classification.
    #[derive(Debug)]
    struct UnclassifiedError;

    impl fmt::Display for UnclassifiedError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("unclassified test error")
        }
    }

    impl StdError for UnclassifiedError {}

    impl StoreErrorClassificationSource for UnclassifiedError {
        fn store_error_classification(&self) -> Option<StoreErrorClassification> {
            None
        }
    }

    /// Return one distinctive classification used to verify wrapper delegation.
    fn test_classification() -> StoreErrorClassification {
        StoreErrorClassification::UNKNOWN
            .with_scope(StoreErrorScope::Record)
            .with_class(StoreErrorClass::InvalidData)
            .with_resolution(StoreErrorResolution::Repair)
    }

    /// Build one classified store source without depending on a concrete backend.
    fn test_store_error() -> StoreError {
        StoreError::new(
            test_classification(),
            std::io::Error::other("injected classified store failure"),
        )
    }

    #[test]
    fn explicit_api_conversion_preserves_classification_and_concrete_source() {
        let error = ApiError::from_store_classification_source(test_store_error());

        assert_eq!(
            error.store_error_classification(),
            Some(test_classification())
        );
        assert_eq!(
            error.to_string(),
            "Replication API store operation failed [RX400PX]: Replication store failed [RX400PX]: injected classified store failure"
        );
        match error {
            ApiError::StoreExternal {
                classification,
                source,
            } => {
                assert_eq!(classification, test_classification());
                assert!(source.downcast_ref::<StoreError>().is_some());
            }
            other => panic!("unexpected API error: {other:?}"),
        }
    }

    #[test]
    fn explicit_api_conversion_does_not_invent_an_unknown_classification() {
        let error = ApiError::from_store_classification_source(UnclassifiedError);

        assert_eq!(error.store_error_classification(), None);
        match error {
            ApiError::ApiExternal { source } => {
                assert!(source.downcast_ref::<UnclassifiedError>().is_some());
            }
            other => panic!("unexpected API error: {other:?}"),
        }
    }

    #[test]
    fn public_load_errors_delegate_through_security_store_access() {
        let security_error = LoadSecurityError::StoreAccess {
            classification: test_classification(),
            source: test_store_error().into(),
        };
        let error = LoadError::Security {
            application_id: ApplicationId::from_array(["test-application"]),
            source: Box::new(security_error),
        };

        assert_eq!(
            error.store_error_classification(),
            Some(test_classification())
        );
    }
}
