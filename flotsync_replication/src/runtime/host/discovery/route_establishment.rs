//! Runtime-host route-establishment integration helpers.

use crate::{api::AuthorityScope, delivery::security::DeliverySecurity};
use flotsync_core::MemberIdentity;
use flotsync_routes::route_establishment::{
    DiscoveryCredentialFuture,
    DiscoveryCredentials,
    DiscoveryKeyMaterialStatusFuture,
};
use flotsync_security::{KeyFingerprint, PublicKeyBundle, sign_discovery_payload};
use flotsync_utils::BoxError;
use futures_util::FutureExt as _;

impl DiscoveryCredentials for DeliverySecurity {
    fn local_discovery_key_fingerprint(&self) -> KeyFingerprint {
        DeliverySecurity::local_discovery_key_fingerprint(self)
    }

    fn local_discovery_public_key_bundle(&self) -> PublicKeyBundle {
        DeliverySecurity::local_public_key_bundle(self)
    }

    fn sign_discovery_payload(
        &self,
        payload: &[u8],
    ) -> Result<flotsync_security::FrameSignature, BoxError> {
        sign_discovery_payload(DeliverySecurity::local_keys(self), payload).map_err(Into::into)
    }

    fn discovery_key_material_status<'a>(
        &'a self,
        member: &'a MemberIdentity,
        key_fingerprint: KeyFingerprint,
    ) -> DiscoveryKeyMaterialStatusFuture<'a> {
        async move {
            DeliverySecurity::discovery_key_material_status(self, member, key_fingerprint)
                .await
                .map_err(Into::into)
        }
        .boxed()
    }

    fn verify_discovery_claim_payload<'a>(
        &'a self,
        member: &'a MemberIdentity,
        key_fingerprint: KeyFingerprint,
        payload: &'a [u8],
        signature: &'a flotsync_security::FrameSignature,
    ) -> DiscoveryCredentialFuture<'a> {
        async move {
            DeliverySecurity::verify_discovery_claim_payload(
                self,
                member,
                key_fingerprint,
                payload,
                signature,
            )
            .await
            .map_err(Into::into)
        }
        .boxed()
    }

    fn permit_member_route_publication<'a>(
        &'a self,
        member: &'a MemberIdentity,
        key_fingerprint: KeyFingerprint,
    ) -> DiscoveryCredentialFuture<'a> {
        async move {
            DeliverySecurity::require_member_key_permission(
                self,
                member,
                key_fingerprint,
                AuthorityScope::MemberRoutePublication,
            )
            .await
            .map_err(Into::into)
        }
        .boxed()
    }

    fn ensure_discovery_public_key_bundle<'a>(
        &'a self,
        member: &'a MemberIdentity,
        bundle: PublicKeyBundle,
    ) -> DiscoveryCredentialFuture<'a> {
        async move {
            DeliverySecurity::ensure_discovery_public_key_bundle(self, member, bundle)
                .await
                .map_err(Into::into)
        }
        .boxed()
    }
}
