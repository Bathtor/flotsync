//! Direct discovery path for fetching missing public key bundles.

use crate::{
    RouteEndpointBinding,
    RouteEndpointLifecycle,
    RouteEndpointLifecyclePort,
    RouteEndpointUnavailableReason,
    config_keys,
    endpoint_discovery::{LocalUdpEndpointBinding, LocalUdpEndpointState},
    protocol::{
        DecodedKeyBundleLookupRequest,
        DiscoveryEndpointFrameSrc,
        DiscoveryRoute,
        KeyBundleLookupResponsePayload,
        KeyBundleLookupResponsePayloadView,
        classify_shared_endpoint_discovery_frame_from_buf,
    },
    route_establishment::{DiscoveryCredentials, DiscoveryKeyMaterialStatus},
};
use flotsync_core::MemberIdentity;
use flotsync_io::prelude::{
    EgressPool,
    IoPayload,
    SocketId,
    TransmissionId,
    UdpIndication,
    UdpPort,
    UdpRequest,
    UdpSendResult,
};
use flotsync_messages::{
    buffa::{Message as _, MessageField},
    discovery as discovery_proto,
    endpoint::EndpointFrame,
    proto::{DecodeProto as _, EncodeProto as _},
    serialisation::{FlotsyncSerializeError, encode_message_payload},
};
use flotsync_security::{FrameSignature, KeyFingerprint, verify_discovery_payload_signature};
use kompact::{Never, prelude::*};
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use uuid::Uuid;

/// Request to fetch missing key material from one discovery route.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchKeyMaterial {
    /// Endpoint that produced the introduction claim.
    pub route: DiscoveryRoute,
    /// Member identity named by the introduction claim.
    pub member: MemberIdentity,
    /// Exact key bundle fingerprint named by the introduction claim.
    pub key_fingerprint: KeyFingerprint,
}

/// Port carrying key-material fetch requests into key-material discovery.
#[derive(Clone, Copy, Debug, Default)]
pub struct KeyMaterialDiscoveryPort;

impl Port for KeyMaterialDiscoveryPort {
    type Request = FetchKeyMaterial;
    type Indication = Never;
}

/// Component that fetches missing public key bundles directly from discovery endpoints.
#[derive(ComponentDefinition)]
pub struct KeyMaterialDiscoveryComponent {
    /// Kompact component context.
    ctx: ComponentContext<Self>,
    /// Fetch requests from route establishment.
    key_material_discovery_port: ProvidedPort<KeyMaterialDiscoveryPort>,
    /// Shared local UDP endpoint traffic and send capability.
    udp_port: RequiredPort<UdpPort>,
    /// Accepted route endpoint lifecycle from the local endpoint manager.
    route_endpoint_lifecycle_port: RequiredPort<RouteEndpointLifecyclePort>,
    /// Local member identity whose keys sign outgoing lookup responses.
    local_member: MemberIdentity,
    /// Discovery key-material security and storage provider.
    credentials: Arc<dyn DiscoveryCredentials>,
    /// Shared egress pool used to encode outbound endpoint frames.
    egress_pool: EgressPool,
    /// Runtime endpoint socket used for raw discovery traffic.
    local_endpoint: LocalUdpEndpointState,
    /// Time for which an in-flight direct lookup suppresses identical fetch requests.
    suppression_lease: Duration,
    /// Next transmission id assigned to direct UDP sends.
    next_transmission_id: TransmissionId,
    /// Pending requests keyed by exact endpoint, member, and fingerprint.
    pending: HashMap<PendingLookupKey, PendingLookup>,
}

impl KeyMaterialDiscoveryComponent {
    /// Build one key-material discovery component.
    #[must_use]
    pub fn new(
        local_member: MemberIdentity,
        credentials: Arc<dyn DiscoveryCredentials>,
        egress_pool: EgressPool,
    ) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            key_material_discovery_port: ProvidedPort::uninitialised(),
            udp_port: RequiredPort::uninitialised(),
            route_endpoint_lifecycle_port: RequiredPort::uninitialised(),
            local_member,
            credentials,
            egress_pool,
            local_endpoint: LocalUdpEndpointState::Unbound,
            suppression_lease: Duration::default(),
            next_transmission_id: TransmissionId::ONE,
            pending: HashMap::new(),
        }
    }

    /// Return the port that receives key-material fetch requests.
    pub fn key_material_discovery_port(&mut self) -> ProvidedRef<KeyMaterialDiscoveryPort> {
        self.key_material_discovery_port.share()
    }

    /// Return the shared UDP port used for direct discovery frames.
    pub fn udp_port(&mut self) -> RequiredRef<UdpPort> {
        self.udp_port.share()
    }

    /// Return the route-endpoint lifecycle port.
    pub fn route_endpoint_lifecycle_port(&mut self) -> RequiredRef<RouteEndpointLifecyclePort> {
        self.route_endpoint_lifecycle_port.share()
    }

    /// Process one fetch request after checking local component state.
    async fn handle_fetch_key_material(&mut self, request: FetchKeyMaterial) {
        let key = PendingLookupKey::from_fetch(&request);
        if self.pending.contains_key(&key) {
            trace!(
                self.log(),
                "key-material discovery skipped duplicate lookup for {:?}", key
            );
            return;
        }
        let Some(endpoint) = self.local_endpoint.binding() else {
            debug!(
                self.log(),
                "key-material discovery cannot request {:?} while local endpoint is unbound", key
            );
            return;
        };
        let status = match self
            .credentials
            .discovery_key_material_status(&request.member, request.key_fingerprint)
            .await
        {
            Ok(status) => status,
            Err(error) => {
                debug!(
                    self.log(),
                    "key-material discovery could not check key status for {} {}: {}",
                    request.member,
                    request.key_fingerprint,
                    error
                );
                return;
            }
        };
        if status != DiscoveryKeyMaterialStatus::Missing {
            trace!(
                self.log(),
                "key-material discovery skipped lookup for {} {} with status {:?}",
                request.member,
                request.key_fingerprint,
                status
            );
            return;
        }
        let DiscoveryRoute::Udp(target) = request.route;
        let nonce = Uuid::new_v4();
        let frame = DiscoveryEndpointFrameSrc::KeyBundleLookupRequest {
            member: &request.member,
            key_fingerprint: request.key_fingerprint,
            request_nonce: nonce,
        }
        .encode_proto();
        if let Err(error) = self
            .send_endpoint_frame(endpoint, target, &frame, "key-bundle lookup request")
            .await
        {
            debug!(
                self.log(),
                "key-material discovery could not encode key-bundle lookup request for {}: {}",
                target,
                error
            );
            return;
        }
        let timeout_key = key.clone();
        let timer = self.schedule_once(self.suppression_lease, move |component, timeout| {
            component.handle_lookup_suppression_expiry(&timeout_key, nonce, &timeout)
        });
        self.pending.insert(key, PendingLookup { nonce, timer });
    }

    /// Submit one endpoint-discovery frame directly over the selected UDP socket.
    ///
    /// # Errors
    ///
    /// Returns [`FlotsyncSerializeError`] if the frame cannot be encoded before UDP submission.
    async fn send_endpoint_frame(
        &mut self,
        endpoint: LocalUdpEndpointBinding,
        target: SocketAddr,
        frame: &EndpointFrame,
        label: &'static str,
    ) -> Result<(), FlotsyncSerializeError> {
        let payload = encode_message_payload(&self.egress_pool, frame).await?;
        let transmission_id = self.next_transmission_id.take_next();
        self.udp_port.trigger(UdpRequest::Send {
            socket_id: endpoint.socket_id,
            transmission_id,
            payload,
            target: Some(target),
            reply_to: self.ctx.actor_ref().recipient(),
        });
        trace!(
            self.log(),
            "key-material discovery submitted {} directly over UDP socket {} to {} as {}",
            label,
            endpoint.socket_id,
            target,
            transmission_id
        );
        Ok(())
    }

    /// Expire one pending request if it still owns this suppression lease.
    fn handle_lookup_suppression_expiry(
        &mut self,
        key: &PendingLookupKey,
        nonce: Uuid,
        actual_timer: &ScheduledTimer,
    ) -> HandlerResult {
        if self
            .pending
            .get(key)
            .is_some_and(|pending| pending.nonce == nonce && pending.timer == *actual_timer)
        {
            self.pending.remove(key);
        }
        Handled::OK
    }

    /// Try to interpret one shared-endpoint payload as a key-material discovery frame.
    fn handle_possible_endpoint_discovery_frame(
        &mut self,
        source: SocketAddr,
        payload: &IoPayload,
    ) -> HandlerResult {
        let mut cursor = payload.cursor();
        let Some(discovery_frame) =
            classify_shared_endpoint_discovery_frame_from_buf(&mut cursor).benign_err()?
        else {
            return Handled::OK;
        };
        match discovery_frame.body {
            Some(discovery_proto::discovery_frame::Body::KeyBundleLookupRequest(request)) => {
                Handled::block_on(self, async move |mut async_self| {
                    async_self.answer_lookup_request(source, *request).await
                })
            }
            Some(discovery_proto::discovery_frame::Body::KeyBundleLookupResponse(response)) => {
                self.handle_lookup_response(source, *response)
            }
            Some(
                discovery_proto::discovery_frame::Body::IntroductionRequest(_)
                | discovery_proto::discovery_frame::Body::Introduction(_),
            ) => Handled::OK,
            None => {
                debug!(
                    self.log(),
                    "ignored key-material discovery endpoint frame from {} without a discovery body",
                    source
                );
                Handled::OK
            }
        }
    }

    /// Dispatch one UDP payload that may contain a key-material discovery frame.
    fn handle_udp_indication(&mut self, indication: UdpIndication) -> HandlerResult {
        let UdpIndication::Received {
            socket_id,
            source,
            payload,
        } = indication
        else {
            return Handled::OK;
        };
        if !self.received_on_local_endpoint(socket_id) {
            return Handled::OK;
        }
        self.handle_possible_endpoint_discovery_frame(source, &payload)
    }

    /// Return whether one UDP receive belongs to the current discovery endpoint.
    fn received_on_local_endpoint(&self, socket_id: SocketId) -> bool {
        self.local_endpoint
            .binding()
            .is_some_and(|binding| binding.socket_id == socket_id)
    }

    /// Build and send a signed lookup response when the request names the local bundle.
    async fn answer_lookup_request(
        &mut self,
        source: SocketAddr,
        request: discovery_proto::KeyBundleLookupRequest,
    ) -> HandlerResult {
        let request = DecodedKeyBundleLookupRequest::decode_proto(request).benign_err()?;
        if request.member != self.local_member
            || request.key_fingerprint != self.credentials.local_discovery_key_fingerprint()
        {
            trace!(
                self.log(),
                "ignored key-bundle lookup request from {} for non-local key {} {}",
                source,
                request.member,
                request.key_fingerprint
            );
            return Handled::OK;
        }
        let public_key_bundle = self.credentials.local_discovery_public_key_bundle();
        if public_key_bundle.fingerprint() != request.key_fingerprint {
            error!(
                self.log(),
                "local discovery key fingerprint did not match local public bundle fingerprint"
            );
            return Handled::OK;
        }
        let Some(endpoint) = self.local_endpoint.binding() else {
            debug!(
                self.log(),
                "ignored key-bundle lookup request from {} while local endpoint was unbound",
                source
            );
            return Handled::OK;
        };
        let payload = KeyBundleLookupResponsePayloadView {
            member: &request.member,
            key_fingerprint: request.key_fingerprint,
            request_nonce: request.request_nonce,
            public_key_bundle: &public_key_bundle,
        }
        .encode_proto();
        let payload = payload.encode_to_vec();
        let signature = match self.credentials.sign_discovery_payload(&payload) {
            Ok(signature) => signature,
            Err(error) => {
                error!(
                    self.log(),
                    "failed to sign key-bundle lookup response for {}: {}", source, error
                );
                return Handled::OK;
            }
        };
        let response = discovery_proto::SignedKeyBundleLookupResponse {
            response_payload: payload,
            signature: MessageField::some(signature.encode_proto()),
            ..discovery_proto::SignedKeyBundleLookupResponse::default()
        };
        let frame = DiscoveryEndpointFrameSrc::KeyBundleLookupResponse {
            response: &response,
        }
        .encode_proto();
        if let Err(error) = self
            .send_endpoint_frame(endpoint, source, &frame, "key-bundle lookup response")
            .await
        {
            debug!(
                self.log(),
                "key-material discovery could not encode key-bundle lookup response for {}: {}",
                source,
                error
            );
        }
        Handled::OK
    }

    /// Validate one signed lookup response enough to schedule background storage.
    fn handle_lookup_response(
        &mut self,
        source: SocketAddr,
        response: discovery_proto::SignedKeyBundleLookupResponse,
    ) -> HandlerResult {
        let decoded = match KeyBundleLookupResponsePayload::try_decode_proto_from_slice(
            &response.response_payload,
        ) {
            Ok(decoded) => decoded,
            Err(error) => {
                trace!(
                    self.log(),
                    "ignored invalid key-bundle lookup response from {}: {:?}", source, error
                );
                return Handled::OK;
            }
        };
        let key = PendingLookupKey {
            route: DiscoveryRoute::Udp(source),
            member: decoded.member.clone(),
            key_fingerprint: decoded.key_fingerprint,
        };
        let Some(pending) = self.pending.get(&key) else {
            trace!(
                self.log(),
                "ignored unsolicited key-bundle lookup response from {} for {:?}", source, key
            );
            return Handled::OK;
        };
        if decoded.request_nonce != pending.nonce {
            trace!(
                self.log(),
                "ignored key-bundle lookup response from {} with mismatched nonce", source
            );
            return Handled::OK;
        }
        let Some(signature) = self.decode_lookup_response_signature(source, &response) else {
            return Handled::OK;
        };
        let response_payload = response.response_payload;
        self.spawn_local(move |mut async_self| async move {
            async_self
                .verify_and_store_lookup_response(source, key, response_payload, signature, decoded)
                .await;
            Handled::OK
        });
        Handled::OK
    }

    /// Verify and store one matching lookup response without blocking the port handler.
    async fn verify_and_store_lookup_response(
        &mut self,
        source: SocketAddr,
        key: PendingLookupKey,
        response_payload: Vec<u8>,
        signature: FrameSignature,
        decoded: KeyBundleLookupResponsePayload,
    ) {
        let public_keys = decoded
            .public_key_bundle
            .clone()
            .bind_member(decoded.member.clone());
        if let Err(error) =
            verify_discovery_payload_signature(&public_keys, &response_payload, &signature)
        {
            trace!(
                self.log(),
                "ignored key-bundle lookup response from {} with invalid signature: {}",
                source,
                error
            );
            return;
        }
        match self
            .credentials
            .ensure_discovery_public_key_bundle(&decoded.member, decoded.public_key_bundle)
            .await
        {
            Ok(()) => {
                if let Some(pending) = self.pending.remove(&key) {
                    self.cancel_timer(pending.timer);
                }
            }
            Err(error) => {
                debug!(
                    self.log(),
                    "failed to ensure direct discovery key bundle from {}: {}", source, error
                );
            }
        }
    }

    /// Decode the detached signature attached to one key-bundle lookup response.
    fn decode_lookup_response_signature(
        &self,
        source: SocketAddr,
        response: &discovery_proto::SignedKeyBundleLookupResponse,
    ) -> Option<FrameSignature> {
        let Some(signature) = response.signature.as_option() else {
            trace!(
                self.log(),
                "ignored key-bundle lookup response from {} without a signature", source
            );
            return None;
        };
        match FrameSignature::decode_proto(signature.clone()) {
            Ok(signature) => Some(signature),
            Err(error) => {
                trace!(
                    self.log(),
                    "ignored key-bundle lookup response from {} with malformed signature: {}",
                    source,
                    error
                );
                None
            }
        }
    }

    /// Log one direct UDP send result reported by the shared UDP bridge.
    fn handle_udp_send_result(&mut self, result: &UdpSendResult) -> HandlerResult {
        match result {
            UdpSendResult::Ack {
                socket_id,
                transmission_id,
            } => {
                trace!(
                    self.log(),
                    "key-material discovery UDP send {} on socket {} was accepted",
                    transmission_id,
                    socket_id
                );
            }
            UdpSendResult::Nack {
                socket_id,
                transmission_id,
                reason,
            } => {
                debug!(
                    self.log(),
                    "key-material discovery UDP send {} on socket {} failed: {:?}",
                    transmission_id,
                    socket_id,
                    reason
                );
            }
        }
        Handled::OK
    }

    /// Record the endpoint authorised for direct key-material discovery traffic.
    fn handle_route_endpoint_available(&mut self, binding: RouteEndpointBinding) -> HandlerResult {
        let binding = LocalUdpEndpointBinding {
            socket_id: binding.socket_id,
            local_addr: binding.socket_bound_addr,
        };
        if self.local_endpoint.binding() == Some(binding) {
            return Handled::OK;
        }
        self.local_endpoint = LocalUdpEndpointState::Bound(binding);
        self.cancel_pending_lookups();
        Handled::OK
    }

    /// Clear endpoint state when the exact authorised endpoint is withdrawn.
    fn handle_route_endpoint_unavailable(
        &mut self,
        binding: RouteEndpointBinding,
        _reason: RouteEndpointUnavailableReason,
    ) -> HandlerResult {
        let binding = LocalUdpEndpointBinding {
            socket_id: binding.socket_id,
            local_addr: binding.socket_bound_addr,
        };
        if self.local_endpoint.binding() != Some(binding) {
            return Handled::OK;
        }
        self.local_endpoint = LocalUdpEndpointState::Unbound;
        self.cancel_pending_lookups();
        Handled::OK
    }

    /// Cancel all pending direct lookups after the local endpoint changes.
    ///
    /// Pending lookup keys are intentionally route-scoped. A malicious or broken endpoint may
    /// keep returning unusable material while another endpoint for the same member/fingerprint can
    /// still satisfy the lookup, so suppression must not be global to the key material alone.
    fn cancel_pending_lookups(&mut self) {
        let timers = self
            .pending
            .drain()
            .map(|(_key, pending)| pending.timer)
            .collect::<Vec<_>>();
        for timer in timers {
            self.cancel_timer(timer);
        }
    }
}

impl ComponentLifecycle for KeyMaterialDiscoveryComponent {
    fn on_start(&mut self) -> HandlerResult {
        self.suppression_lease = self
            .ctx
            .config()
            .read_or_default(&config_keys::KEY_MATERIAL_DISCOVERY_SUPPRESSION_LEASE)
            .unrecoverable_err()?;
        Handled::OK
    }
}

impl Actor for KeyMaterialDiscoveryComponent {
    type Message = UdpSendResult;

    fn receive_local(&mut self, msg: Self::Message) -> HandlerResult {
        self.handle_udp_send_result(&msg)
    }
}

impl Provide<KeyMaterialDiscoveryPort> for KeyMaterialDiscoveryComponent {
    fn handle(&mut self, request: FetchKeyMaterial) -> HandlerResult {
        Handled::block_on(self, async move |mut async_self| {
            async_self.handle_fetch_key_material(request).await;
            Handled::OK
        })
    }
}

impl Require<UdpPort> for KeyMaterialDiscoveryComponent {
    fn handle(&mut self, indication: UdpIndication) -> HandlerResult {
        self.handle_udp_indication(indication)
    }
}

impl Require<RouteEndpointLifecyclePort> for KeyMaterialDiscoveryComponent {
    fn handle(&mut self, indication: RouteEndpointLifecycle) -> HandlerResult {
        match indication {
            RouteEndpointLifecycle::Available(binding) => {
                self.handle_route_endpoint_available(binding)
            }
            RouteEndpointLifecycle::Unavailable { binding, reason } => {
                self.handle_route_endpoint_unavailable(binding, reason)
            }
        }
    }
}

/// Pending lookup identity.
///
/// The route is part of the key intentionally: one endpoint can be malicious or broken and keep
/// returning unusable material, while another endpoint for the same member and fingerprint can still
/// provide the valid bundle. Suppressing only by `(member, fingerprint)` would let one bad route
/// block acquisition from every other observed route until the suppression lease expires.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct PendingLookupKey {
    /// Discovery route whose endpoint owns this pending request attempt.
    route: DiscoveryRoute,
    /// Member identity whose key material is being requested.
    member: MemberIdentity,
    /// Exact public key bundle fingerprint being requested.
    key_fingerprint: KeyFingerprint,
}

impl PendingLookupKey {
    fn from_fetch(request: &FetchKeyMaterial) -> Self {
        Self {
            route: request.route,
            member: request.member.clone(),
            key_fingerprint: request.key_fingerprint,
        }
    }
}

/// One in-flight direct key-material lookup.
struct PendingLookup {
    /// Request nonce that the response must echo before its signature is trusted.
    nonce: Uuid,
    /// Lease timer used to stop suppressing duplicate lookup requests.
    timer: ScheduledTimer,
}

#[cfg(test)]
mod tests;
