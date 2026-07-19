//! Shared fixtures and assertion helpers for security-store tests.

use super::*;

const TEST_WAIT_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) fn wait_for_security_store_future<F>(future: F) -> F::Output
where
    F: std::future::Future,
{
    flotsync_io::test_support::wait_for_future(
        TEST_WAIT_TIMEOUT,
        future,
        "timed out waiting for security-store future",
    )
}

pub(super) fn local_member() -> MemberIdentity {
    Identifier::from_array(["security", "local"])
}

pub(super) fn remote_member() -> MemberIdentity {
    Identifier::from_array(["security", "remote"])
}

pub(super) fn alternate_member() -> MemberIdentity {
    Identifier::from_array(["security", "alternate"])
}

pub(super) fn sqlite_store() -> Arc<SqliteReplicationStore> {
    Arc::new(
        wait_for_security_store_future(SqliteReplicationStore::in_memory(local_member()))
            .expect("store should build"),
    )
}

pub(super) fn security_store(store: Arc<SqliteReplicationStore>) -> SecurityStore {
    security_store_with_policy(store, TrustPolicy::default())
}

pub(super) fn security_store_with_policy(
    store: Arc<SqliteReplicationStore>,
    trust_policy: TrustPolicy,
) -> SecurityStore {
    let store: Arc<dyn ReplicationStore> = store;
    SecurityStore::new(store, trust_policy)
}

pub(super) fn provision_member_public_keys(
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

pub(super) fn binding_report<'a>(
    report: &'a PublicKeyBundleReport,
    key_id: &MemberKeyId,
) -> &'a MemberKeyBindingReport {
    report
        .known_bindings
        .iter()
        .find(|binding| &binding.key_id == key_id)
        .expect("binding report should exist")
}

pub(super) fn authority_decision(
    binding: &MemberKeyBindingReport,
    scope: AuthorityScope,
) -> PermissionDecision {
    binding
        .authority
        .iter()
        .find(|authority| authority.scope == scope)
        .expect("authority report should exist")
        .decision
}
