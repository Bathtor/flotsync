//! Public security-state facade for member-key permission checks.

use crate::api::{
    AuthorityScope,
    MemberKeyId,
    MemberKeyTrustEvidenceKind,
    MemberKeyTrustEvidenceSet,
    MemberKeyTrustRequirement,
    MemberPublicKeysRecord,
    PermissionDecision,
    PermissionDenialReason,
    ReplicationStore,
    ReplicationStoreReadTransaction,
    StoreError,
    TrustPolicy,
};
use flotsync_core::MemberIdentity;
use flotsync_security::{ED25519_KEY_LENGTH, KeyFingerprint, PublicMemberKeys, X25519_KEY_LENGTH};
use flotsync_utils::{BoxError, BoxFuture};
use futures_util::FutureExt as _;
use snafu::{Location, prelude::*};
use std::sync::Arc;

/// Public security-state facade used by runtime security code.
#[derive(Clone)]
pub(crate) struct SecurityStore {
    store: Arc<dyn ReplicationStore>,
    trust_policy: TrustPolicy,
}

impl SecurityStore {
    /// Build a security facade over one replication store and trust policy.
    #[must_use]
    pub(crate) fn new(store: Arc<dyn ReplicationStore>, trust_policy: TrustPolicy) -> Self {
        Self {
            store,
            trust_policy,
        }
    }

    /// Return the underlying replication store.
    #[must_use]
    pub(crate) fn replication_store(&self) -> &Arc<dyn ReplicationStore> {
        &self.store
    }

    /// Request permission for one exact member-key binding.
    pub(crate) fn request_member_key_permission<'a>(
        &'a self,
        key_id: &'a MemberKeyId,
        authority_scope: AuthorityScope,
    ) -> BoxFuture<'a, Result<PermissionDecision, SecurityStoreError>> {
        async move {
            let mut transaction = self
                .store
                .begin_read_transaction()
                .await
                .context(StoreAccessSnafu)?;
            let decision = request_member_key_permission_from_transaction(
                transaction.as_mut(),
                &self.trust_policy,
                authority_scope,
                key_id,
            )
            .await?;
            transaction.release().await.context(StoreAccessSnafu)?;
            Ok(decision)
        }
        .boxed()
    }

    /// Request permission for one exact member/fingerprint binding.
    pub(crate) fn request_member_key_permission_for<'a>(
        &'a self,
        member_id: &'a MemberIdentity,
        fingerprint: KeyFingerprint,
        authority_scope: AuthorityScope,
    ) -> BoxFuture<'a, Result<PermissionDecision, SecurityStoreError>> {
        async move {
            let key_id = MemberKeyId {
                member_id: member_id.clone(),
                fingerprint,
            };
            self.request_member_key_permission(&key_id, authority_scope)
                .await
        }
        .boxed()
    }

    /// Load the single permitted public key bundle for one member and authority scope.
    pub(crate) fn load_permitted_member_public_keys<'a>(
        &'a self,
        member_id: &'a MemberIdentity,
        authority_scope: AuthorityScope,
    ) -> BoxFuture<'a, Result<PublicMemberKeys, SecurityStoreError>> {
        async move {
            let mut transaction = self
                .store
                .begin_read_transaction()
                .await
                .context(StoreAccessSnafu)?;
            let records = transaction
                .load_member_public_keys_for_member(member_id)
                .await
                .context(StoreAccessSnafu)?;
            let mut permitted = Vec::new();
            let mut denial_reasons = Vec::new();
            for record in records {
                let decision = request_member_key_permission_from_transaction(
                    transaction.as_mut(),
                    &self.trust_policy,
                    authority_scope,
                    &record.key_id,
                )
                .await?;
                match decision {
                    PermissionDecision::Permit => permitted.push(record),
                    PermissionDecision::Deny(reason) => denial_reasons.push(reason),
                }
            }
            transaction.release().await.context(StoreAccessSnafu)?;
            if permitted.is_empty() {
                if denial_reasons.is_empty() {
                    denial_reasons.push(PermissionDenialReason::MissingKeyMaterial);
                }
                return Err(SecurityStoreError::NoPermittedMemberPublicKeys {
                    member_id: member_id.clone(),
                    authority_scope,
                    denial_reasons,
                });
            }
            ensure!(
                permitted.len() == 1,
                AmbiguousPermittedMemberPublicKeysSnafu {
                    member_id: member_id.clone(),
                    authority_scope,
                    permitted_count: permitted.len(),
                }
            );
            let record = permitted
                .pop()
                .expect("permitted length is checked to contain exactly one key");
            public_keys_from_record(record)
        }
        .boxed()
    }
}

/// Security-store failures raised while loading or authorising public key material.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub(crate) enum SecurityStoreError {
    /// Replication-store access failed.
    #[snafu(display("Replication-store access failed at {location}: {source}"))]
    StoreAccess {
        source: StoreError,
        #[snafu(implicit)]
        location: Location,
    },
    /// No observed member key had permission for the requested authority scope.
    #[snafu(display(
        "No public keys for member {member_id} have {authority_scope:?} permission: {denial_reasons:?}."
    ))]
    NoPermittedMemberPublicKeys {
        member_id: MemberIdentity,
        authority_scope: AuthorityScope,
        denial_reasons: Vec<PermissionDenialReason>,
    },
    /// More than one observed member key had permission where one key was required.
    #[snafu(display(
        "{permitted_count} public keys for member {member_id} have {authority_scope:?} permission."
    ))]
    AmbiguousPermittedMemberPublicKeys {
        member_id: MemberIdentity,
        authority_scope: AuthorityScope,
        permitted_count: usize,
    },
    /// Stored public key bytes had the wrong fixed length.
    #[snafu(display(
        "Stored public key bytes for member {member_id} fingerprint {fingerprint} had invalid length {actual}; expected {expected}."
    ))]
    InvalidMemberPublicKeyLength {
        member_id: MemberIdentity,
        fingerprint: KeyFingerprint,
        expected: usize,
        actual: usize,
    },
    /// Stored public key bytes could not be decoded.
    #[snafu(display(
        "Stored public keys for member {member_id} fingerprint {fingerprint} were invalid: {source}"
    ))]
    InvalidMemberPublicKeys {
        member_id: MemberIdentity,
        fingerprint: KeyFingerprint,
        source: BoxError,
    },
    /// Stored public key bytes did not derive their stored fingerprint.
    #[snafu(display(
        "Stored public keys for member {member_id} derived fingerprint {actual}, expected {expected}."
    ))]
    MemberPublicKeyFingerprintMismatch {
        member_id: MemberIdentity,
        expected: KeyFingerprint,
        actual: KeyFingerprint,
    },
}

/// Request one member-key permission from already-loaded evidence.
pub(crate) fn request_member_key_permission(
    policy: &TrustPolicy,
    scope: AuthorityScope,
    _key_id: &MemberKeyId,
    evidence: MemberKeyTrustEvidenceSet,
    fingerprint_blocked: bool,
) -> PermissionDecision {
    if fingerprint_blocked {
        return PermissionDecision::Deny(PermissionDenialReason::FingerprintBlocked);
    }
    let requirement = match scope {
        AuthorityScope::ReplicationRuntime => policy.replication_runtime,
        AuthorityScope::BootstrapActivation => policy.bootstrap_activation,
    };
    match requirement {
        MemberKeyTrustRequirement::DenyAll => {
            PermissionDecision::Deny(PermissionDenialReason::PolicyDenied)
        }
        MemberKeyTrustRequirement::LocalExplicitTrust => {
            if evidence.contains(MemberKeyTrustEvidenceKind::LocalExplicitTrust) {
                PermissionDecision::Permit
            } else {
                PermissionDecision::Deny(PermissionDenialReason::MissingTrustEvidence)
            }
        }
    }
}

/// Request one member-key permission by loading evidence from a transaction.
async fn request_member_key_permission_from_transaction(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    policy: &TrustPolicy,
    scope: AuthorityScope,
    key_id: &MemberKeyId,
) -> Result<PermissionDecision, SecurityStoreError> {
    let material = transaction
        .load_member_public_keys(key_id)
        .await
        .context(StoreAccessSnafu)?;
    if material.is_none() {
        return Ok(PermissionDecision::Deny(
            PermissionDenialReason::MissingKeyMaterial,
        ));
    }
    let evidence = transaction
        .load_member_key_trust_evidence(key_id)
        .await
        .context(StoreAccessSnafu)?;
    let fingerprint_blocked = transaction
        .is_key_fingerprint_blocked(&key_id.fingerprint)
        .await
        .context(StoreAccessSnafu)?;
    Ok(request_member_key_permission(
        policy,
        scope,
        key_id,
        evidence,
        fingerprint_blocked,
    ))
}

/// Decode one opaque public-key store record into typed security keys.
pub(crate) fn public_keys_from_record(
    record: MemberPublicKeysRecord,
) -> Result<PublicMemberKeys, SecurityStoreError> {
    let signing_public_key =
        fixed_public_key::<ED25519_KEY_LENGTH>(&record.key_id, record.signing_public_key.as_ref())?;
    let encryption_public_key = fixed_public_key::<X25519_KEY_LENGTH>(
        &record.key_id,
        record.encryption_public_key.as_ref(),
    )?;
    let public_keys = PublicMemberKeys::from_key_bytes(
        record.key_id.member_id.clone(),
        signing_public_key,
        encryption_public_key,
    )
    .boxed()
    .context(InvalidMemberPublicKeysSnafu {
        member_id: record.key_id.member_id.clone(),
        fingerprint: record.key_id.fingerprint,
    })?;
    ensure!(
        public_keys.fingerprint() == record.key_id.fingerprint,
        MemberPublicKeyFingerprintMismatchSnafu {
            member_id: record.key_id.member_id,
            expected: record.key_id.fingerprint,
            actual: public_keys.fingerprint(),
        }
    );
    Ok(public_keys)
}

/// Validate and copy fixed-width public key bytes from store records.
fn fixed_public_key<const N: usize>(
    key_id: &MemberKeyId,
    bytes: &[u8],
) -> Result<[u8; N], SecurityStoreError> {
    bytes
        .try_into()
        .map_err(|_| SecurityStoreError::InvalidMemberPublicKeyLength {
            member_id: key_id.member_id.clone(),
            fingerprint: key_id.fingerprint,
            expected: N,
            actual: bytes.len(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        SqliteReplicationStore,
        api::{MemberKeyTrustEvidenceRecord, MemberPublicKeysRecord, ReplicationStore},
        test_support::test_public_member_keys,
    };
    use flotsync_core::member::Identifier;
    use std::time::Duration;

    const TEST_WAIT_TIMEOUT: Duration = Duration::from_secs(5);

    fn wait_for_security_store_future<F>(future: F) -> F::Output
    where
        F: std::future::Future,
    {
        flotsync_io::test_support::wait_for_future(
            TEST_WAIT_TIMEOUT,
            future,
            "timed out waiting for security-store future",
        )
    }

    fn local_member() -> MemberIdentity {
        Identifier::from_array(["security", "local"])
    }

    fn remote_member() -> MemberIdentity {
        Identifier::from_array(["security", "remote"])
    }

    fn alternate_member() -> MemberIdentity {
        Identifier::from_array(["security", "alternate"])
    }

    fn sqlite_store() -> Arc<SqliteReplicationStore> {
        Arc::new(SqliteReplicationStore::in_memory(local_member()).expect("store should build"))
    }

    fn security_store(store: Arc<SqliteReplicationStore>) -> SecurityStore {
        security_store_with_policy(store, TrustPolicy::default())
    }

    fn security_store_with_policy(
        store: Arc<SqliteReplicationStore>,
        trust_policy: TrustPolicy,
    ) -> SecurityStore {
        let store: Arc<dyn ReplicationStore> = store;
        SecurityStore::new(store, trust_policy)
    }

    fn provision_member_public_keys(
        store: &dyn ReplicationStore,
        member_id: MemberIdentity,
        source_member: &MemberIdentity,
        trusted: bool,
    ) -> MemberPublicKeysRecord {
        let public_keys = test_public_member_keys(source_member);
        let mut record = MemberPublicKeysRecord::from_public_keys(&public_keys);
        record.key_id.member_id = member_id;
        let mut transaction =
            wait_for_security_store_future(store.begin_transaction()).expect("transaction starts");
        wait_for_security_store_future(transaction.ensure_member_public_keys(record.clone()))
            .expect("member public keys store");
        if trusted {
            wait_for_security_store_future(transaction.ensure_member_key_trust_evidence(
                MemberKeyTrustEvidenceRecord {
                    key_id: record.key_id.clone(),
                    evidence_kind: MemberKeyTrustEvidenceKind::LocalExplicitTrust,
                },
            ))
            .expect("trust evidence stores");
        }
        wait_for_security_store_future(transaction.commit()).expect("transaction commits");
        record
    }

    #[test]
    fn evaluator_requires_local_explicit_trust_by_default() {
        let key_id = MemberKeyId {
            member_id: remote_member(),
            fingerprint: test_public_member_keys(&remote_member()).fingerprint(),
        };
        let evidence = MemberKeyTrustEvidenceSet::empty();

        let decision = request_member_key_permission(
            &TrustPolicy::default(),
            AuthorityScope::ReplicationRuntime,
            &key_id,
            evidence,
            false,
        );

        assert_eq!(
            decision,
            PermissionDecision::Deny(PermissionDenialReason::MissingTrustEvidence)
        );
    }

    #[test]
    fn evaluator_denies_blocked_fingerprint_before_positive_evidence() {
        let key_id = MemberKeyId {
            member_id: remote_member(),
            fingerprint: test_public_member_keys(&remote_member()).fingerprint(),
        };
        let mut evidence = MemberKeyTrustEvidenceSet::empty();
        evidence.insert(MemberKeyTrustEvidenceKind::LocalExplicitTrust);

        let decision = request_member_key_permission(
            &TrustPolicy::default(),
            AuthorityScope::ReplicationRuntime,
            &key_id,
            evidence,
            true,
        );

        assert_eq!(
            decision,
            PermissionDecision::Deny(PermissionDenialReason::FingerprintBlocked)
        );
    }

    #[test]
    fn evaluator_denies_local_explicit_trust_when_policy_denies_all() {
        let key_id = MemberKeyId {
            member_id: remote_member(),
            fingerprint: test_public_member_keys(&remote_member()).fingerprint(),
        };
        let mut evidence = MemberKeyTrustEvidenceSet::empty();
        evidence.insert(MemberKeyTrustEvidenceKind::LocalExplicitTrust);
        let policy = TrustPolicy {
            replication_runtime: MemberKeyTrustRequirement::DenyAll,
            bootstrap_activation: MemberKeyTrustRequirement::LocalExplicitTrust,
        };

        let decision = request_member_key_permission(
            &policy,
            AuthorityScope::ReplicationRuntime,
            &key_id,
            evidence,
            false,
        );

        assert_eq!(
            decision,
            PermissionDecision::Deny(PermissionDenialReason::PolicyDenied)
        );
    }

    #[test]
    fn evaluator_denies_bootstrap_activation_when_policy_denies_all() {
        let key_id = MemberKeyId {
            member_id: remote_member(),
            fingerprint: test_public_member_keys(&remote_member()).fingerprint(),
        };
        let mut evidence = MemberKeyTrustEvidenceSet::empty();
        evidence.insert(MemberKeyTrustEvidenceKind::LocalExplicitTrust);
        let policy = TrustPolicy {
            replication_runtime: MemberKeyTrustRequirement::LocalExplicitTrust,
            bootstrap_activation: MemberKeyTrustRequirement::DenyAll,
        };

        let decision = request_member_key_permission(
            &policy,
            AuthorityScope::BootstrapActivation,
            &key_id,
            evidence,
            false,
        );

        assert_eq!(
            decision,
            PermissionDecision::Deny(PermissionDenialReason::PolicyDenied)
        );
    }

    #[test]
    fn security_store_loads_locally_permitted_member_public_keys() {
        let store = sqlite_store();
        let record =
            provision_member_public_keys(store.as_ref(), remote_member(), &remote_member(), true);
        let security_store = security_store(store);

        let public_keys =
            wait_for_security_store_future(security_store.load_permitted_member_public_keys(
                &record.key_id.member_id,
                AuthorityScope::ReplicationRuntime,
            ))
            .expect("permitted member public keys should load");

        assert_eq!(public_keys.fingerprint(), record.key_id.fingerprint);
    }

    #[test]
    fn security_store_denies_observed_member_public_keys_without_trust_evidence() {
        let store = sqlite_store();
        let record =
            provision_member_public_keys(store.as_ref(), remote_member(), &remote_member(), false);
        let security_store = security_store(store);

        let error =
            wait_for_security_store_future(security_store.load_permitted_member_public_keys(
                &record.key_id.member_id,
                AuthorityScope::ReplicationRuntime,
            ))
            .expect_err("observed-only member public keys should not be permitted");

        assert!(matches!(
            error,
            SecurityStoreError::NoPermittedMemberPublicKeys {
                denial_reasons,
                ..
            } if denial_reasons.contains(&PermissionDenialReason::MissingTrustEvidence)
        ));
    }

    #[test]
    fn security_store_denies_globally_blocked_fingerprint() {
        let store = sqlite_store();
        let record =
            provision_member_public_keys(store.as_ref(), remote_member(), &remote_member(), true);
        let mut transaction =
            wait_for_security_store_future(store.begin_transaction()).expect("transaction starts");
        wait_for_security_store_future(
            transaction.ensure_blocked_key_fingerprint(record.key_id.fingerprint),
        )
        .expect("blocked fingerprint stores");
        wait_for_security_store_future(transaction.commit()).expect("transaction commits");
        let security_store = security_store(store);

        let error =
            wait_for_security_store_future(security_store.load_permitted_member_public_keys(
                &record.key_id.member_id,
                AuthorityScope::ReplicationRuntime,
            ))
            .expect_err("blocked fingerprint should not be permitted");

        assert!(matches!(
            error,
            SecurityStoreError::NoPermittedMemberPublicKeys {
                denial_reasons,
                ..
            } if denial_reasons.contains(&PermissionDenialReason::FingerprintBlocked)
        ));
    }

    #[test]
    fn security_store_denies_local_explicit_trust_when_policy_denies_all() {
        let store = sqlite_store();
        let record =
            provision_member_public_keys(store.as_ref(), remote_member(), &remote_member(), true);
        let security_store = security_store_with_policy(
            store,
            TrustPolicy {
                replication_runtime: MemberKeyTrustRequirement::DenyAll,
                bootstrap_activation: MemberKeyTrustRequirement::LocalExplicitTrust,
            },
        );

        let error =
            wait_for_security_store_future(security_store.load_permitted_member_public_keys(
                &record.key_id.member_id,
                AuthorityScope::ReplicationRuntime,
            ))
            .expect_err("policy should deny explicitly trusted member public keys");

        assert!(matches!(
            error,
            SecurityStoreError::NoPermittedMemberPublicKeys {
                denial_reasons,
                ..
            } if denial_reasons.contains(&PermissionDenialReason::PolicyDenied)
        ));
    }

    #[test]
    fn security_store_denies_bootstrap_activation_when_policy_denies_all() {
        let store = sqlite_store();
        let record =
            provision_member_public_keys(store.as_ref(), remote_member(), &remote_member(), true);
        let security_store = security_store_with_policy(
            store,
            TrustPolicy {
                replication_runtime: MemberKeyTrustRequirement::LocalExplicitTrust,
                bootstrap_activation: MemberKeyTrustRequirement::DenyAll,
            },
        );

        let error =
            wait_for_security_store_future(security_store.load_permitted_member_public_keys(
                &record.key_id.member_id,
                AuthorityScope::BootstrapActivation,
            ))
            .expect_err("policy should deny bootstrap activation for explicitly trusted keys");

        assert!(matches!(
            error,
            SecurityStoreError::NoPermittedMemberPublicKeys {
                denial_reasons,
                ..
            } if denial_reasons.contains(&PermissionDenialReason::PolicyDenied)
        ));
    }

    #[test]
    fn security_store_rejects_ambiguous_permitted_member_keys() {
        let store = sqlite_store();
        let member_id = remote_member();
        provision_member_public_keys(store.as_ref(), member_id.clone(), &remote_member(), true);
        provision_member_public_keys(store.as_ref(), member_id.clone(), &alternate_member(), true);
        let security_store = security_store(store);

        let error = wait_for_security_store_future(
            security_store
                .load_permitted_member_public_keys(&member_id, AuthorityScope::ReplicationRuntime),
        )
        .expect_err("multiple permitted member keys should be ambiguous");

        assert!(matches!(
            error,
            SecurityStoreError::AmbiguousPermittedMemberPublicKeys {
                member_id: error_member,
                permitted_count: 2,
                ..
            } if error_member == member_id
        ));
    }
}
