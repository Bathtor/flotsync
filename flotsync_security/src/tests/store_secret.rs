//! Tests for encrypted store secrets and local keyring profiles.

use super::{fixtures::*, *};

#[test]
fn store_secret_round_trips_with_logical_context() {
    let key = StoreSecretKey::from_bytes([3_u8; 32]);
    let context = StoreSecretContext {
        table: "replication_group",
        column: "group_secret",
        row_id: b"group-1",
        key_id: STORE_SECRET_TEST_KEY_ID.as_bytes(),
        crypto_version: StoreSecretCryptoVersion::V1,
    };
    let sealed = seal_store_secret_for_test(&key, context, b"group key", [4_u8; 24]).unwrap();

    let opened = open_store_secret(&key, context, &sealed).unwrap();

    assert_eq!(opened.as_slice(), b"group key");
}

#[test]
fn store_secret_open_fails_when_context_changes() {
    let key = StoreSecretKey::from_bytes([3_u8; 32]);
    let context = StoreSecretContext {
        table: "replication_group",
        column: "group_secret",
        row_id: b"group-1",
        key_id: STORE_SECRET_TEST_KEY_ID.as_bytes(),
        crypto_version: StoreSecretCryptoVersion::V1,
    };
    let sealed = seal_store_secret_for_test(&key, context, b"group key", [4_u8; 24]).unwrap();
    let tampered_context = StoreSecretContext {
        row_id: b"group-2",
        ..context
    };

    let err = open_store_secret(&key, tampered_context, &sealed).unwrap_err();

    assert!(matches!(err, SecurityError::StoreSecretOpen));
}

#[test]
fn local_store_secret_create_then_load_round_trips_key() {
    install_local_store_secret_test_store().unwrap();
    let application_id = Identifier::from_array(["flotsync", "security", "tests"]);
    let profile = unique_local_store_secret_profile("round-trip");

    let created = load_or_create_local_store_secret(&application_id, &profile).unwrap();
    let (key_id, created_key) = created.into_parts();
    assert_ne!(key_id.as_bytes(), Uuid::nil().as_bytes());

    let context = StoreSecretContext {
        table: "replication_group",
        column: "group_secret",
        row_id: b"group-1",
        key_id: key_id.as_bytes(),
        crypto_version: StoreSecretCryptoVersion::V1,
    };
    let sealed =
        seal_store_secret_for_test(&created_key, context, b"group key", [5_u8; 24]).unwrap();

    let loaded = load_local_store_secret(&application_id, &profile).unwrap();
    let (loaded_key_id, loaded_key) = loaded.into_parts();
    let opened = open_store_secret(&loaded_key, context, &sealed).unwrap();

    assert_eq!(loaded_key_id, key_id);
    assert_eq!(opened.as_slice(), b"group key");
}

#[test]
fn local_store_secret_profiles_are_isolated() {
    install_local_store_secret_test_store().unwrap();
    let application_id = Identifier::from_array(["flotsync", "security", "tests"]);
    let first_profile = unique_local_store_secret_profile("first");
    let second_profile = unique_local_store_secret_profile("second");

    let first = load_or_create_local_store_secret(&application_id, &first_profile).unwrap();
    let second = load_or_create_local_store_secret(&application_id, &second_profile).unwrap();

    assert_ne!(first.key_id(), second.key_id());
}

#[test]
fn local_store_secret_application_ids_are_isolated() {
    install_local_store_secret_test_store().unwrap();
    let first_application = Identifier::from_array(["flotsync", "security", "first"]);
    let second_application = Identifier::from_array(["flotsync", "security", "second"]);
    let profile = unique_local_store_secret_profile("same-profile");

    let first = load_or_create_local_store_secret(&first_application, &profile).unwrap();
    let second = load_or_create_local_store_secret(&second_application, &profile).unwrap();

    assert_ne!(first.key_id(), second.key_id());
}

#[test]
fn local_store_secret_load_reports_missing_profile() {
    install_local_store_secret_test_store().unwrap();
    let application_id = Identifier::from_array(["flotsync", "security", "tests"]);
    let profile = unique_local_store_secret_profile("missing");

    let err = load_local_store_secret(&application_id, &profile).unwrap_err();

    assert!(matches!(err, LocalStoreSecretError::Missing { .. }));
}

#[test]
fn local_store_secret_profile_rejects_empty_selector() {
    let err = LocalStoreSecretProfile::new("  ").unwrap_err();

    assert!(matches!(err, LocalStoreSecretError::EmptyProfile));
}

#[test]
fn local_store_secret_profile_rejects_surrounding_whitespace() {
    let err = LocalStoreSecretProfile::new(" dev").unwrap_err();

    assert!(matches!(
        err,
        LocalStoreSecretError::ProfileWhitespace { .. }
    ));
}

#[test]
fn local_store_secret_profile_rejects_non_portable_selector() {
    let err = LocalStoreSecretProfile::new("Dev").unwrap_err();

    assert!(matches!(
        err,
        LocalStoreSecretError::ProfileNotPortable { .. }
    ));
}

#[test]
fn local_store_secret_profile_rejects_account_separator() {
    let err = LocalStoreSecretProfile::new("dev/local").unwrap_err();

    assert!(matches!(
        err,
        LocalStoreSecretError::ProfileNotPortable { .. }
    ));
}

#[test]
fn local_store_secret_rejects_non_portable_application_id() {
    install_local_store_secret_test_store().unwrap();
    let application_id = Identifier::from_array(["Flotsync", "security", "tests"]);
    let profile = unique_local_store_secret_profile("application");

    let err = load_or_create_local_store_secret(&application_id, &profile).unwrap_err();

    assert!(matches!(
        err,
        LocalStoreSecretError::ApplicationIdNotPortable { .. }
    ));
}
