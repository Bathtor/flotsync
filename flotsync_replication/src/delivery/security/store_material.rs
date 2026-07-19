//! Encryption and decryption of security records stored by the replication store.

use super::*;

impl DeliverySecurity {
    pub(super) fn open_group_secret(
        &self,
        group_id: GroupId,
        security_material: &EncryptedGroupSecurityMaterial,
    ) -> Result<GroupKey, DeliverySecurityError> {
        let secret = &security_material.encrypted_group_secret;
        ensure!(
            secret.key_id == self.store_secret_key_id,
            GroupSecretKeyIdMismatchSnafu {
                group_id,
                expected: self.store_secret_key_id,
                actual: secret.key_id,
            }
        );
        let sealed = secret.to_store_secret_ciphertext()?;
        let context = StoreSecretContext {
            table: LOGICAL_GROUP_TABLE,
            column: LOGICAL_GROUP_SECRET_COLUMN,
            row_id: group_id.0.as_bytes(),
            key_id: self.store_secret_key_id.as_bytes(),
            crypto_version: StoreSecretCryptoVersion::new(secret.crypto_version.as_u16()),
        };
        open_stored_group_key(&self.store_secret_key, context, &sealed)
            .boxed()
            .context(OpenGroupSecretSnafu { group_id })
    }

    pub(crate) fn seal_group_secret(
        &self,
        group_id: Uuid,
        group_key: &GroupKey,
    ) -> Result<EncryptedGroupSecurityMaterial, DeliverySecurityError> {
        let row_id = group_id.as_bytes();
        let context = StoreSecretContext {
            table: LOGICAL_GROUP_TABLE,
            column: LOGICAL_GROUP_SECRET_COLUMN,
            row_id,
            key_id: self.store_secret_key_id.as_bytes(),
            crypto_version: STORE_SECRET_CRYPTO_VERSION_V1,
        };
        let plaintext = group_key.stored_secret_plaintext();
        let sealed = seal_store_secret(&self.store_secret_key, context, plaintext.as_slice())
            .boxed()
            .context(SealGroupSecretSnafu)?;
        Ok(EncryptedGroupSecurityMaterial {
            encrypted_group_secret: EncryptedStoreSecret::from_store_secret_ciphertext(
                self.store_secret_key_id,
                sealed,
            ),
        })
    }
}

/// Open the local private-key bundle stored with the local member record.
pub(super) fn open_local_private_keys(
    record: &LocalMemberPrivateKeysRecord,
    store_secret_key: &StoreSecretKey,
) -> Result<LocalMemberKeys, DeliverySecurityError> {
    let secret = &record.private_keys.secret;
    let sealed = secret.to_store_secret_ciphertext()?;
    let row_id = record.member_id.to_string();
    let context = StoreSecretContext {
        table: LOGICAL_LOCAL_MEMBER_TABLE,
        column: LOGICAL_LOCAL_PRIVATE_KEYS_COLUMN,
        row_id: row_id.as_bytes(),
        key_id: secret.key_id.as_bytes(),
        crypto_version: StoreSecretCryptoVersion::new(secret.crypto_version.as_u16()),
    };
    let plaintext = open_store_secret(store_secret_key, context, &sealed)
        .boxed()
        .context(OpenLocalPrivateKeysSnafu)?;
    local_member_keys_from_private_bundle(plaintext.as_slice(), record.member_id.clone())
        .boxed()
        .context(InvalidLocalPrivateKeysSnafu)
}

impl EncryptedStoreSecret {
    /// Convert this opaque encrypted-cell record into the fixed-width security ciphertext type.
    pub(crate) fn to_store_secret_ciphertext(
        &self,
    ) -> Result<StoreSecretCiphertext, DeliverySecurityError> {
        let version = self.crypto_version.as_u16();
        ensure!(
            version == STORE_SECRET_CRYPTO_VERSION_V1.as_u16(),
            UnsupportedStoreSecretVersionSnafu { version }
        );
        let nonce = self.nonce.as_ref().try_into().map_err(|_| {
            DeliverySecurityError::InvalidStoreSecretNonce {
                actual: self.nonce.len(),
            }
        })?;
        Ok(StoreSecretCiphertext {
            nonce,
            ciphertext: self.ciphertext.as_ref().to_vec(),
        })
    }
}
