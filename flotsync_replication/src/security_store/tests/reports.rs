//! Tests for application-facing security-store reports.

use super::fixtures::*;

#[test]
fn known_member_keys_report_names_empty_store() {
    let security_store = security_store(sqlite_store());

    let report = wait_for_security_store_future(security_store.known_member_keys_report())
        .expect("empty known-member report should load");

    assert!(report.members.is_empty());
}

#[test]
fn known_member_keys_report_groups_bindings_and_local_trust() {
    let store = sqlite_store();
    let remote_trusted =
        provision_member_public_keys(store.as_ref(), remote_member(), &remote_member(), true);
    let alternate_record = provision_member_public_keys(
        store.as_ref(),
        alternate_member(),
        &alternate_member(),
        false,
    );
    let remote_observed =
        provision_member_public_keys(store.as_ref(), remote_member(), &alternate_member(), false);
    let security_store = security_store(store);

    let report = wait_for_security_store_future(security_store.known_member_keys_report())
        .expect("known-member report should load");

    assert_eq!(report.members.len(), 2);
    let alternate = report
        .members
        .iter()
        .find(|member| member.member_id == alternate_member())
        .expect("alternate member should be reported");
    assert_eq!(alternate.keys.len(), 1);
    assert_eq!(
        alternate.keys[0].fingerprint,
        alternate_record.key_id.fingerprint
    );
    assert!(!alternate.keys[0].trust.has_local_explicit_trust);

    let remote = report
        .members
        .iter()
        .find(|member| member.member_id == remote_member())
        .expect("remote member should be reported");
    assert_eq!(remote.keys.len(), 2);
    let trusted = remote
        .keys
        .iter()
        .find(|key| key.fingerprint == remote_trusted.key_id.fingerprint)
        .expect("trusted remote binding should be reported");
    let observed = remote
        .keys
        .iter()
        .find(|key| key.fingerprint == remote_observed.key_id.fingerprint)
        .expect("observed remote binding should be reported");
    assert!(trusted.trust.has_local_explicit_trust);
    assert!(!observed.trust.has_local_explicit_trust);
}
