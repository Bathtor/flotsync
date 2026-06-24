//! Runtime-host route-establishment integration helpers.

use crate::delivery::security::DeliverySecurity;
use flotsync_core::MemberIdentity;
use flotsync_routes::route_establishment::{DiscoveryCredentialFuture, DiscoveryCredentials};
use flotsync_utils::BoxError;
use futures_util::FutureExt as _;

impl DiscoveryCredentials for DeliverySecurity {
    fn sign_discovery_claim_payload(
        &self,
        payload: &[u8],
    ) -> Result<flotsync_security::FrameSignature, BoxError> {
        DeliverySecurity::sign_discovery_claim_payload(self, payload)
            .map_err(|error| Box::new(error) as BoxError)
    }

    fn verify_discovery_claim_payload<'a>(
        &'a self,
        member: &'a MemberIdentity,
        payload: &'a [u8],
        signature: &'a flotsync_security::FrameSignature,
    ) -> DiscoveryCredentialFuture<'a> {
        async move {
            DeliverySecurity::verify_discovery_claim_payload(self, member, payload, signature)
                .await
                .map_err(|error| Box::new(error) as BoxError)
        }
        .boxed()
    }
}
