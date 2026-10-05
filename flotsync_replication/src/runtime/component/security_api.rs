//! Kompact scheduling adapters for application-facing key and trust requests.
//!
//! The local bundle is already loaded. Store-backed queries and feedback retain
//! their existing independent spawned execution and API error classification.

use super::ReplicationRuntimeComponent;
use crate::api::{
    ApiError,
    security::{
        AssessPublicKeyBundleRequest,
        KnownMemberKeysReport,
        PublicKeyBundleReport,
        RecordPublicKeyBundleFeedbackRequest,
    },
};
use flotsync_security::PublicKeyBundle;
use kompact::prelude::*;

// TODO(flotsync-zop): Replace this temporary component extension in flotsync-zop.14
// with narrow security operation/service impls beside their own types. Keep thin
// component scheduling glue in component/mod.rs; all component impls must be there
// before parent completion, as checked by flotsync-zop.24.
impl ReplicationRuntimeComponent {
    /// Assess stored evidence or store candidate bindings according to the request mode.
    pub(super) fn handle_assess_public_key_bundle(
        &mut self,
        ask: Ask<AssessPublicKeyBundleRequest, Result<PublicKeyBundleReport, ApiError>>,
    ) -> HandlerResult {
        let (promise, request) = ask.take();
        self.spawn_local(move |async_self| async move {
            let reply = async_self
                .security
                .assess_public_key_bundle(request)
                .await
                .map_err(ApiError::from_store_classification_source);
            async_self.reply_api(promise, "assess_public_key_bundle", reply);
            Handled::OK
        });
        Handled::OK
    }

    /// Write key/trust/block feedback before replying through the existing API boundary.
    pub(super) fn handle_record_public_key_bundle_feedback(
        &mut self,
        ask: Ask<RecordPublicKeyBundleFeedbackRequest, Result<(), ApiError>>,
    ) -> HandlerResult {
        let (promise, request) = ask.take();
        self.spawn_local(move |async_self| async move {
            let reply = async_self
                .security
                .record_public_key_bundle_feedback(request)
                .await
                .map_err(ApiError::from_store_classification_source);
            async_self.reply_api(promise, "record_public_key_bundle_feedback", reply);
            Handled::OK
        });
        Handled::OK
    }

    /// Reply synchronously with the local member's already loaded shareable bundle.
    pub(super) fn handle_local_public_key_bundle(
        &mut self,
        ask: Ask<(), Result<PublicKeyBundle, ApiError>>,
    ) -> HandlerResult {
        let (promise, ()) = ask.take();
        let reply = Ok(self.security.local_public_key_bundle());
        self.reply_api(promise, "local_public_key_bundle", reply);
        Handled::OK
    }

    /// Read stored bindings and local trust without blocking other mailbox input.
    pub(super) fn handle_known_member_keys(
        &mut self,
        ask: Ask<(), Result<KnownMemberKeysReport, ApiError>>,
    ) -> HandlerResult {
        let (promise, ()) = ask.take();
        self.spawn_local(move |async_self| async move {
            let reply = async_self
                .security
                .known_member_keys_report()
                .await
                .map_err(ApiError::from_store_classification_source);
            async_self.reply_api(promise, "known_member_keys", reply);
            Handled::OK
        });
        Handled::OK
    }
}
