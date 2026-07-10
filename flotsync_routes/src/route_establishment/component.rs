//! Kompact component implementing route establishment probes and introductions.

use super::{DiscoveryCredentials, DiscoveryKeyMaterialStatus};
use crate::{
    DatagramRouteScope,
    RouteDiscoveryPort,
    RouteEndpointBinding,
    RouteEndpointLifecycle,
    RouteEndpointLifecyclePort,
    RouteEndpointUnavailableReason,
    RoutePreferenceRank,
    RouteSharingKind,
    RouteTransportActorMessage,
    RouteTransportInboundDeliver,
    RouteTransportPort,
    SendRouteCandidate,
    TransportRouteKey,
    UdpRouteKey,
    config_keys,
    endpoint_discovery::{
        LocalUdpEndpointBinding,
        LocalUdpEndpointState,
        route_transport_inbound_source,
        submit_endpoint_discovery_frame,
    },
    key_material_discovery::{FetchKeyMaterial, KeyMaterialDiscoveryPort},
    protocol::{
        DecodedIntroductionClaimPayload,
        DiscoveryEndpointFrameSrc,
        decode_endpoint_discovery_frame_from_buf,
        decode_introduction_claim_payload_view,
        encode_member_key_selector_fields,
    },
};
use flotsync_core::{
    GroupId,
    MemberIdentity,
    member::{TrieMap, TrieSet},
    membership::{GroupMemberships, SharedGroupMemberships},
};
use flotsync_discovery::{
    endpoint_selection::{EndpointSelection, EndpointSelectionPort},
    protocol::DiscoveryRoute,
    services::{PeerAnnouncementObservationPort, PeerAnnouncementObserved},
};
use flotsync_io::prelude::IoPayload;
use flotsync_messages::{
    buffa::{Message as _, MessageField},
    discovery as discovery_proto,
    proto::{DecodeProtoView, EncodeProto},
    serialisation::FlotsyncSerializable,
    wire::{group_id_to_wire_bytes, uuid_from_wire_bytes, uuid_to_wire_bytes},
};
use flotsync_utils::{BoxError, option_when};
use kompact::prelude::*;
use snafu::{ResultExt, Snafu};
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};
use uuid::Uuid;

use super::{
    config::ConcreteRoutes,
    state::{
        ManualMemberFilter,
        ManualRouteWatchError,
        PendingClaimVerification,
        PendingProbe,
        WatchedRoute,
        WatchedRouteState,
    },
    wire::prepare_claim_for_verification,
};

/// Actor messages accepted by [`RouteEstablishmentComponent`].
#[derive(Debug)]
pub enum RouteEstablishmentMessage {
    /// Replace all manual route watches currently configured on this component.
    ReplaceManualRouteWatches(Ask<Vec<WatchedRoute>, Result<(), ManualRouteWatchError>>),
    /// Test hook that withdraws one route without waiting for a wall-clock timer.
    #[cfg(test)]
    ExpireRouteForTest {
        /// Exact route endpoint to withdraw.
        route: DiscoveryRoute,
    },
}

/// Startup failures for route establishment components.
#[derive(Debug, Snafu)]
enum RouteEstablishmentStartupError {
    /// A Kompact config value could not be read.
    #[snafu(display("could not load route-establishment config {key}: {reason}"))]
    ConfigurationFailed {
        /// Config key that failed to load.
        key: &'static str,
        /// Human-readable config lookup failure.
        reason: String,
    },
}

/// Failures after a claim signature has already verified.
#[derive(Debug, Snafu)]
enum VerifiedIntroductionClaimError {
    /// Full payload decoding disagreed with the selector decoded before signature verification.
    #[snafu(display(
        "verified introduction claim selector field '{field}' changed during full decode"
    ))]
    SelectorMismatch {
        /// Selector field that disagreed.
        field: &'static str,
    },
    /// Full payload decoding failed after signature verification.
    #[snafu(display("verified introduction claim payload could not be decoded: {source}"))]
    DecodePayload {
        /// Protocol-level payload decode failure.
        source: flotsync_discovery::protocol::DiscoveryProtocolError,
    },
}

/// Route establishment route source for signed peer introductions.
#[derive(ComponentDefinition)]
pub struct RouteEstablishmentComponent {
    /// Kompact component context.
    ctx: ComponentContext<Self>,
    /// Route source port where verified peer routes are published.
    discovery_port: ProvidedPort<RouteDiscoveryPort<TransportRouteKey>>,
    /// Key-material fetch request path for direct key-material discovery.
    key_material_discovery_port: RequiredPort<KeyMaterialDiscoveryPort>,
    /// Peer-announcement input from one or more announcement protocols.
    announcement_port: RequiredPort<PeerAnnouncementObservationPort>,
    /// Reassembled route-transport payloads sharing the runtime endpoint.
    route_transport_port: RequiredPort<RouteTransportPort<TransportRouteKey>>,
    /// Accepted route endpoint lifecycle from route transport.
    route_endpoint_lifecycle_port: RequiredPort<RouteEndpointLifecyclePort>,
    /// Actor interface used for directed introduction request/response submits.
    route_transport: ActorRefStrong<RouteTransportActorMessage<TransportRouteKey>>,
    /// Receives selected local endpoints used for signed introduction claims.
    endpoint_selection_port: RequiredPort<EndpointSelectionPort>,
    /// Static runtime settings for this discovery source.
    config: super::RouteEstablishmentConfig,
    /// Concrete local routes that this component may sign in introduction claims.
    claim_routes: ConcreteRoutes,
    /// Local member identity whose keys sign outgoing introduction claims.
    local_member: MemberIdentity,
    /// Discovery claim signing and verification provider.
    credentials: Arc<dyn DiscoveryCredentials>,
    /// Shared local group membership snapshot used for outgoing and accepted discovery claims.
    group_memberships: SharedGroupMemberships,
    /// Configured route establishment timing values.
    timing: RouteEstablishmentTiming,
    /// Runtime endpoint socket used for `IntroductionRequest`, `Introduction`, and replication.
    local_endpoint: LocalUdpEndpointState,
    /// Per route interest and verification state.
    route_state: HashMap<DiscoveryRoute, WatchedRouteState>,
    /// Routes with active manual watch interest, used to avoid scanning all route state on replace.
    manual_route_watch_routes: HashSet<DiscoveryRoute>,
    /// Last published route snapshot, rebuilt from route verification state.
    member_route_snapshots: TrieMap<BTreeSet<SocketAddr>>,
}

impl RouteEstablishmentComponent {
    /// Build one route establishment tracker.
    #[must_use]
    pub fn new(
        config: super::RouteEstablishmentConfig,
        route_transport: ActorRefStrong<RouteTransportActorMessage<TransportRouteKey>>,
        local_member: MemberIdentity,
        credentials: Arc<dyn DiscoveryCredentials>,
        group_memberships: SharedGroupMemberships,
    ) -> Self {
        let claim_routes = config.advertised_routes().clone();
        Self {
            ctx: ComponentContext::uninitialised(),
            discovery_port: ProvidedPort::uninitialised(),
            key_material_discovery_port: RequiredPort::uninitialised(),
            announcement_port: RequiredPort::uninitialised(),
            route_transport_port: RequiredPort::uninitialised(),
            route_endpoint_lifecycle_port: RequiredPort::uninitialised(),
            route_transport,
            endpoint_selection_port: RequiredPort::uninitialised(),
            config,
            claim_routes,
            local_member,
            credentials,
            group_memberships,
            timing: RouteEstablishmentTiming::default(),
            local_endpoint: LocalUdpEndpointState::Unbound,
            route_state: HashMap::new(),
            manual_route_watch_routes: HashSet::new(),
            member_route_snapshots: TrieMap::new(),
        }
    }

    /// Return the observed local UDP endpoint state without exposing writable internals.
    #[cfg(test)]
    pub(super) fn local_endpoint(&self) -> LocalUdpEndpointState {
        self.local_endpoint
    }

    /// Return the currently configured local routes that may be signed in introduction claims.
    #[cfg(test)]
    pub(super) fn advertised_routes(&self) -> &BTreeSet<SocketAddr> {
        self.claim_routes.routes()
    }

    /// Return the endpoint-selection port reference used by tests to inject selected endpoints.
    #[cfg(test)]
    pub(super) fn endpoint_selection_port(&mut self) -> RequiredRef<EndpointSelectionPort> {
        self.endpoint_selection_port.share()
    }

    /// Return the route-endpoint lifecycle port reference used by tests to inject endpoint state.
    #[cfg(test)]
    pub(super) fn route_endpoint_lifecycle_port(
        &mut self,
    ) -> RequiredRef<RouteEndpointLifecyclePort> {
        self.route_endpoint_lifecycle_port.share()
    }

    /// Load route-establishment timing from Kompact config.
    ///
    /// # Errors
    ///
    /// Returns [`RouteEstablishmentStartupError`] when one config value cannot be read.
    fn load_timing_from_config(
        &self,
    ) -> Result<RouteEstablishmentTiming, RouteEstablishmentStartupError> {
        let probe_timeout = self
            .ctx
            .config()
            .read_or_default(&config_keys::ROUTE_ESTABLISHMENT_PROBE_TIMEOUT)
            .map_err(
                |error| RouteEstablishmentStartupError::ConfigurationFailed {
                    key: config_keys::ROUTE_ESTABLISHMENT_PROBE_TIMEOUT.key,
                    reason: error.to_string(),
                },
            )?;
        let reachable_lease = self
            .ctx
            .config()
            .read_or_default(&config_keys::ROUTE_ESTABLISHMENT_REACHABLE_LEASE)
            .map_err(
                |error| RouteEstablishmentStartupError::ConfigurationFailed {
                    key: config_keys::ROUTE_ESTABLISHMENT_REACHABLE_LEASE.key,
                    reason: error.to_string(),
                },
            )?;
        Ok(RouteEstablishmentTiming {
            probe_timeout,
            reachable_lease,
        })
    }

    /// Record the route endpoint authorised for introduction traffic.
    fn handle_route_endpoint_available(&mut self, binding: RouteEndpointBinding) -> HandlerResult {
        let binding = LocalUdpEndpointBinding {
            socket_id: binding.socket_id,
            local_addr: binding.socket_bound_addr,
        };
        if self.local_endpoint.binding() == Some(binding) {
            return Handled::OK;
        }
        info!(
            self.log(),
            "route establishment accepted local endpoint bind local_addr={} socket_id={}",
            binding.local_addr,
            binding.socket_id
        );
        self.local_endpoint = LocalUdpEndpointState::Bound(binding);
        self.invalidate_route_verification_for_endpoint_change();
        Handled::block_on(self, async move |mut async_self| {
            async_self.probe_all_inactive_routes().await;
            Handled::OK
        })
    }

    /// Clear route endpoint state when the endpoint owner withdraws authorisation.
    fn handle_route_endpoint_unavailable(
        &mut self,
        binding: RouteEndpointBinding,
        reason: RouteEndpointUnavailableReason,
    ) -> HandlerResult {
        let binding = LocalUdpEndpointBinding {
            socket_id: binding.socket_id,
            local_addr: binding.socket_bound_addr,
        };
        if self.local_endpoint.binding() != Some(binding) {
            return Handled::OK;
        }
        debug!(
            self.log(),
            "route establishment endpoint became unavailable local_addr={} socket_id={}: {:?}",
            binding.local_addr,
            binding.socket_id,
            reason
        );
        self.local_endpoint = LocalUdpEndpointState::Unbound;
        self.invalidate_route_verification_for_endpoint_change();
        Handled::OK
    }

    /// Record advertised routes and probe any route without an active timeout.
    async fn handle_peer_announcement_async(&mut self, peer: PeerAnnouncementObserved) {
        let probes = self.record_peer_announcement(peer);
        self.send_introduction_requests(probes).await;
    }

    /// Replace manual route watches and probe newly active routes when an endpoint is available.
    async fn replace_manual_route_watches_async(
        &mut self,
        watches: Vec<WatchedRoute>,
    ) -> Result<(), ManualRouteWatchError> {
        let probes = self.replace_manual_route_watches(watches)?;
        if self.local_endpoint.binding().is_some() {
            self.send_introduction_requests(probes).await;
        }
        Ok(())
    }

    /// Record advertised routes and return routes that should be probed.
    pub(super) fn record_peer_announcement(
        &mut self,
        peer: PeerAnnouncementObserved,
    ) -> Vec<DiscoveryRoute> {
        if peer.instance_id == self.config.instance_id {
            return Vec::new();
        }

        let mut probes = Vec::with_capacity(peer.routes.len());
        for route in peer.routes {
            let route_state = self
                .route_state
                .entry(route)
                .or_insert(WatchedRouteState::NEW);
            route_state.interest.peer_announced = true;
            if !route_state.has_active_timeout() {
                probes.push(route);
            }
        }
        probes
    }

    /// Replace the manual route watches and return routes that should be probed.
    fn replace_manual_route_watches(
        &mut self,
        watches: Vec<WatchedRoute>,
    ) -> Result<Vec<DiscoveryRoute>, ManualRouteWatchError> {
        let mut manual_filters = manual_filters_from_watches(watches)?;

        let new_manual_route_watch_routes = manual_filters.keys().copied().collect::<HashSet<_>>();
        let old_and_new_routes: HashSet<DiscoveryRoute> = self
            .manual_route_watch_routes
            .union(&new_manual_route_watch_routes)
            .copied()
            .collect();

        self.manual_route_watch_routes = new_manual_route_watch_routes;

        let mut timers_to_cancel = Vec::new();
        let mut should_rebuild_published_routes = false;
        let mut probes = Vec::new();
        for route in old_and_new_routes {
            // The default is `None`, so for old routes not present in the new watches,
            // this effectively removes the manual filter.
            let new_manual_filter = manual_filters.remove(&route).unwrap_or_default();
            let route_state = self
                .route_state
                .entry(route)
                .or_insert(WatchedRouteState::NEW);
            let manual_filter_changed = route_state.interest.manual != new_manual_filter;
            route_state.interest.manual = new_manual_filter;

            if route_state.should_watch() {
                if manual_filter_changed {
                    let reconciliation = route_state.reconcile_reachable_members_with_interest();
                    if let Some(timer) = reconciliation.timer_to_cancel {
                        timers_to_cancel.push(timer);
                    }
                    if reconciliation.requires_snapshot_changes {
                        should_rebuild_published_routes = true;
                    }
                    debug_assert!(
                        route_state.should_watch(),
                        "reconciling reachable members must not remove route interest"
                    );
                }

                if !route_state.has_active_timeout() {
                    probes.push(route);
                }
            } else {
                // Nothing requires us to keep probing this route, so just remove the timers and
                // move on with the next route.
                if let Some(timer) = route_state.verification.mark_stale() {
                    timers_to_cancel.push(timer);
                    should_rebuild_published_routes = true;
                }
            }
        }
        for timer in timers_to_cancel {
            self.cancel_timer(timer);
        }
        if should_rebuild_published_routes {
            self.rebuild_published_member_routes();
        }
        Ok(probes)
    }

    /// Probe all known routes that are not already probing or published.
    async fn probe_all_inactive_routes(&mut self) {
        let routes = self
            .route_state
            .iter()
            .filter_map(|(route, state)| {
                option_when!(state.should_watch() && !state.has_active_timeout(), *route)
            })
            .collect::<Vec<_>>();
        trace!(
            self.log(),
            "route establishment probing {} inactive watched route(s)",
            routes.len()
        );
        self.send_introduction_requests(routes).await;
    }

    /// Send introduction probes for the provided routes in order.
    async fn send_introduction_requests(
        &mut self,
        routes: impl IntoIterator<Item = DiscoveryRoute>,
    ) {
        for route in routes {
            self.send_introduction_request(route).await;
        }
    }

    /// Send one introduction request and start the matching probe timeout.
    async fn send_introduction_request(&mut self, route: DiscoveryRoute) {
        if self
            .route_state
            .get(&route)
            .is_none_or(|state| !state.should_watch() || state.has_active_timeout())
        {
            trace!(
                self.log(),
                "route establishment skipped inactive probe for {:?}", route
            );
            return;
        }
        let Some(endpoint) = self.local_endpoint.binding() else {
            debug!(
                self.log(),
                "route establishment recorded route {:?} while local endpoint was not bound", route
            );
            return;
        };
        let DiscoveryRoute::Udp(target) = route;

        let nonce = Uuid::new_v4();
        let frame = DiscoveryEndpointFrameSrc::IntroductionRequest {
            request_nonce: nonce,
        }
        .encode_proto();
        if !self
            .submit_endpoint_frame(endpoint, target, Arc::new(frame), "introduction request")
            .await
        {
            return;
        }

        let timeout = self.timing.probe_timeout;
        let timer = self.schedule_once(timeout, move |component, timeout| {
            component.handle_probe_timeout(route, nonce, &timeout)
        });
        let timer_to_cancel = self
            .route_state
            .get_mut(&route)
            .expect("probe route was checked before scheduling")
            .verification
            .mark_probing(PendingProbe { nonce, timer });
        if let Some(timer) = timer_to_cancel {
            self.cancel_timer(timer);
        }
    }

    /// Submit one route-establishment endpoint frame through route transport.
    ///
    /// Returns `true` only after route transport reports the send as accepted. Returns `false`
    /// when no send was confirmed, so callers must not update probe state that assumes an
    /// outbound introduction request exists.
    async fn submit_endpoint_frame(
        &mut self,
        endpoint: LocalUdpEndpointBinding,
        target: SocketAddr,
        payload: Arc<dyn FlotsyncSerializable>,
        label: &'static str,
    ) -> bool {
        submit_endpoint_discovery_frame(
            &self.route_transport,
            self.log(),
            "route establishment",
            endpoint,
            target,
            payload,
            label,
        )
        .await
    }

    /// Expire one active introduction probe if the nonce and timer still match.
    fn handle_probe_timeout(
        &mut self,
        route: DiscoveryRoute,
        nonce: Uuid,
        actual_timer: &ScheduledTimer,
    ) -> HandlerResult {
        if let Some(state) = self.route_state.get_mut(&route)
            && state
                .verification
                .expire_probe_if_matches(nonce, actual_timer)
        {
            self.mark_route_stale(route);
        }
        Handled::OK
    }

    /// Try to interpret one shared-endpoint payload as a discovery frame.
    fn handle_possible_endpoint_discovery_frame(
        &mut self,
        source: SocketAddr,
        payload: &IoPayload,
    ) -> HandlerResult {
        let mut cursor = payload.cursor();
        let Some(discovery_frame) =
            decode_endpoint_discovery_frame_from_buf(&mut cursor).benign_err()?
        else {
            // Valid endpoint traffic for another semantic layer shares this socket.
            return Handled::OK;
        };
        match discovery_frame.body {
            Some(discovery_proto::discovery_frame::Body::IntroductionRequest(request)) => {
                Handled::block_on(self, async move |mut async_self| {
                    async_self
                        .answer_introduction_request_async(source, *request)
                        .await
                })
            }
            Some(discovery_proto::discovery_frame::Body::Introduction(introduction)) => {
                Handled::block_on(self, async move |mut async_self| {
                    async_self
                        .handle_introduction_async(source, *introduction)
                        .await;
                    Handled::OK
                })
            }
            Some(
                discovery_proto::discovery_frame::Body::KeyBundleLookupRequest(_)
                | discovery_proto::discovery_frame::Body::KeyBundleLookupResponse(_),
            ) => {
                trace!(
                    self.log(),
                    "route establishment ignored key-material discovery frame from {}", source
                );
                Handled::OK
            }
            None => {
                debug!(
                    self.log(),
                    "ignored discovery endpoint frame from {} without a discovery body", source
                );
                Handled::OK
            }
        }
    }

    /// Dispatch one route-transport payload that may contain a discovery endpoint frame.
    fn handle_route_transport_inbound(
        &mut self,
        inbound: &RouteTransportInboundDeliver<TransportRouteKey>,
    ) -> HandlerResult {
        let Some(source) = route_transport_inbound_source(inbound) else {
            debug!(
                self.log(),
                "ignored route establishment transport payload without UDP source metadata"
            );
            return Handled::OK;
        };
        self.handle_possible_endpoint_discovery_frame(source, &inbound.payload)
    }

    /// Build and send a signed introduction response for a valid introduction request.
    async fn answer_introduction_request_async(
        &mut self,
        source: SocketAddr,
        request: discovery_proto::IntroductionRequest,
    ) -> HandlerResult {
        let request_nonce =
            match uuid_from_wire_bytes(&request.request_nonce, "IntroductionRequest.request_nonce")
            {
                Ok(request_nonce) => request_nonce,
                Err(error) => {
                    trace!(
                        self.log(),
                        "ignored introduction request from {} with malformed nonce: {}",
                        source,
                        error
                    );
                    return Handled::OK;
                }
            };
        let Some(endpoint) = self.local_endpoint.binding() else {
            debug!(
                self.log(),
                "ignored introduction request from {} while local endpoint was not bound", source
            );
            return Handled::OK;
        };
        let group_ids = local_claim_group_ids(&self.group_memberships, &self.local_member);
        if group_ids.is_empty() {
            trace!(
                self.log(),
                "ignored introduction request from {} because there are no local groups", source
            );
            return Handled::OK;
        }
        if self.claim_routes.is_empty() {
            trace!(
                self.log(),
                "ignored introduction request from {} because there are no routes to advertise",
                source
            );
            return Handled::OK;
        }

        let instance_uuid = uuid_to_wire_bytes(self.config.instance_id);
        let request_nonce = uuid_to_wire_bytes(request_nonce);
        let group_ids = group_ids
            .iter()
            .copied()
            .map(group_id_to_wire_bytes)
            .collect::<Vec<_>>();
        let (member_id, key_fingerprint) = encode_member_key_selector_fields(
            &self.local_member,
            self.credentials.local_discovery_key_fingerprint(),
        );
        let mut claims = Vec::with_capacity(self.claim_routes.len());
        for route in self.claim_routes.iter() {
            let route = DiscoveryRoute::Udp(*route).encode_proto();
            let claim_payload = discovery_proto::IntroductionClaimPayload {
                instance_uuid: instance_uuid.clone(),
                request_nonce: request_nonce.clone(),
                route: MessageField::some(route),
                group_ids: group_ids.clone(),
                member_id: MessageField::some(member_id.clone()),
                key_fingerprint: key_fingerprint.clone(),
                ..discovery_proto::IntroductionClaimPayload::default()
            };
            let claim_payload = claim_payload.encode_to_vec();
            let signature = match self.credentials.sign_discovery_payload(&claim_payload) {
                Ok(signature) => signature,
                Err(error) => {
                    error!(
                        self.log(),
                        "failed to sign route establishment introduction for {}: {}", source, error
                    );
                    return Handled::OK;
                }
            };
            claims.push(discovery_proto::SignedIntroductionClaim {
                claim_payload,
                signature: MessageField::some(signature.encode_proto()),
                ..discovery_proto::SignedIntroductionClaim::default()
            });
        }
        let introduction = discovery_proto::Introduction {
            instance_uuid,
            request_nonce,
            claims,
            ..discovery_proto::Introduction::default()
        };
        let frame = DiscoveryEndpointFrameSrc::Introduction {
            introduction: &introduction,
        }
        .encode_proto();
        self.submit_endpoint_frame(endpoint, source, Arc::new(frame), "introduction response")
            .await;
        Handled::OK
    }

    /// Verify a signed introduction response and publish the route if it proves shared group
    /// membership.
    async fn handle_introduction_async(
        &mut self,
        source: SocketAddr,
        introduction: discovery_proto::Introduction,
    ) {
        let Some(prepared) = self.collect_verifiable_claims_for_active_probe(source, introduction)
        else {
            return;
        };
        if prepared.claims.is_empty() {
            trace!(
                self.log(),
                "ignored introduction from {} because it contained no verifiable claims", source
            );
            self.mark_route_stale(prepared.route);
            return;
        }
        let memberships = self.group_memberships.snapshot();
        let mut accepted_members = TrieSet::new();
        for claim in prepared.claims {
            if let Some(member) = self
                .accept_prepared_introduction_claim(
                    source,
                    prepared.route,
                    memberships.as_ref(),
                    claim,
                )
                .await
            {
                accepted_members.insert(member);
            }
        }
        if accepted_members.is_empty() {
            trace!(
                self.log(),
                "ignored introduction from {} because no verified claim matched local group memberships",
                source
            );
            self.mark_route_stale(prepared.route);
            return;
        }
        self.mark_route_reachable(prepared.route, accepted_members);
    }

    /// Return the claim member when one prepared claim verifies and is publishable.
    async fn accept_prepared_introduction_claim(
        &mut self,
        source: SocketAddr,
        route: DiscoveryRoute,
        memberships: &GroupMemberships,
        claim: PendingClaimVerification,
    ) -> Option<MemberIdentity> {
        if !self.route_interest_permits_member(route, &claim.member) {
            debug!(
                self.log(),
                "ignored introduction claim from {} for uninterested member {} on route {:?}",
                source,
                claim.member,
                route
            );
            return None;
        }
        match self
            .credentials
            .discovery_key_material_status(&claim.member, claim.key_fingerprint)
            .await
        {
            Ok(DiscoveryKeyMaterialStatus::Available) => {
                // Key material is present; continue to signature verification below.
            }
            Ok(DiscoveryKeyMaterialStatus::Missing) => {
                debug!(
                    self.log(),
                    "requested missing key material for introduction claim from {}: {} {}",
                    source,
                    claim.member,
                    claim.key_fingerprint
                );
                self.fetch_key_material(route, &claim.member, claim.key_fingerprint);
                return None;
            }
            Ok(DiscoveryKeyMaterialStatus::Unusable) => {
                debug!(
                    self.log(),
                    "ignored introduction claim from {} with unusable key material {} {}",
                    source,
                    claim.member,
                    claim.key_fingerprint
                );
                return None;
            }
            Err(error) => {
                debug!(
                    self.log(),
                    "ignored introduction claim from {} after key-material status check failed: {}",
                    source,
                    error
                );
                return None;
            }
        }
        match verify_prepared_claim(self.credentials.as_ref(), claim).await {
            Ok(verified_claim) => {
                self.member_accepted_for_route_publication(source, memberships, verified_claim)
                    .await
            }
            Err(error) => {
                debug!(
                    self.log(),
                    "ignored unverifiable introduction claim from {}: {}", source, error
                );
                None
            }
        }
    }

    /// Return the claim member if membership and route-publication policy both accept it.
    async fn member_accepted_for_route_publication(
        &mut self,
        source: SocketAddr,
        memberships: &GroupMemberships,
        verified_claim: VerifiedIntroductionClaim,
    ) -> Option<MemberIdentity> {
        if !claim_matches_group_memberships(
            memberships,
            &verified_claim.member,
            &verified_claim.group_ids,
        ) {
            return None;
        }
        let route_publication = self
            .credentials
            .permit_member_route_publication(&verified_claim.member, verified_claim.key_fingerprint)
            .await;
        match route_publication {
            Ok(()) => Some(verified_claim.member),
            Err(error) => {
                debug!(
                    self.log(),
                    "ignored introduction claim from {} without route-publication permission: {}",
                    source,
                    error
                );
                None
            }
        }
    }

    /// Collect introduction claims that match the currently active probe.
    fn collect_verifiable_claims_for_active_probe(
        &self,
        source: SocketAddr,
        introduction: discovery_proto::Introduction,
    ) -> Option<super::state::PartiallyVerifiedIntroduction> {
        let route = DiscoveryRoute::Udp(source);
        let Some(route_state) = self.route_state.get(&route) else {
            trace!(
                self.log(),
                "ignored unsolicited introduction from {} for unwatched route", source
            );
            return None;
        };
        let Some(probe) = route_state.pending_probe() else {
            trace!(
                self.log(),
                "ignored introduction from {} without an active probe", source
            );
            return None;
        };
        let Ok(instance_id) =
            uuid_from_wire_bytes(&introduction.instance_uuid, "Introduction.instance_uuid")
        else {
            trace!(
                self.log(),
                "ignored introduction from {} with malformed instance id", source
            );
            return None;
        };
        let Ok(introduction_nonce) =
            uuid_from_wire_bytes(&introduction.request_nonce, "Introduction.request_nonce")
        else {
            trace!(
                self.log(),
                "ignored introduction from {} with malformed probe nonce", source
            );
            return None;
        };
        if introduction_nonce != probe.nonce {
            trace!(
                self.log(),
                "ignored introduction from {} with mismatched probe nonce", source
            );
            return None;
        }

        let claims = introduction
            .claims
            .into_iter()
            .filter_map(|claim| {
                match prepare_claim_for_verification(
                    route,
                    instance_id,
                    probe.nonce,
                    &self.local_member,
                    claim,
                ) {
                    Ok(claim) => claim,
                    Err(error) => {
                        debug!(
                            self.log(),
                            "ignored invalid introduction claim from {}: {}", source, error
                        );
                        None
                    }
                }
            })
            .collect();
        Some(super::state::PartiallyVerifiedIntroduction { route, claims })
    }

    /// Request one direct key-material fetch.
    fn fetch_key_material(
        &mut self,
        route: DiscoveryRoute,
        member: &MemberIdentity,
        key_fingerprint: flotsync_security::KeyFingerprint,
    ) {
        self.key_material_discovery_port.trigger(FetchKeyMaterial {
            route,
            member: member.clone(),
            key_fingerprint,
        });
    }

    /// Publish one route for the verified members until the reachable lease expires.
    pub(super) fn mark_route_reachable(
        &mut self,
        route: DiscoveryRoute,
        accepted_members: TrieSet,
    ) {
        if !self.route_state.contains_key(&route) {
            return;
        }
        let timer = self.schedule_once(self.timing.reachable_lease, move |component, timeout| {
            component.handle_reachable_lease_expired(route, &timeout)
        });
        let timer_to_cancel = self
            .route_state
            .get_mut(&route)
            .expect("reachable route was checked before scheduling")
            .verification
            .mark_reachable(timer, accepted_members);
        if let Some(timer) = timer_to_cancel {
            self.cancel_timer(timer);
        }
        self.rebuild_published_member_routes();
    }

    /// Expire one reachable lease if it still owns the active timer, then schedule a refresh probe.
    fn handle_reachable_lease_expired(
        &mut self,
        route: DiscoveryRoute,
        actual_timer: &ScheduledTimer,
    ) -> HandlerResult {
        let Some(route_state) = self.route_state.get_mut(&route) else {
            return Handled::OK;
        };
        if !route_state
            .verification
            .expire_reachable_lease_if_matches(actual_timer)
        {
            return Handled::OK;
        }
        self.rebuild_published_member_routes();
        Handled::block_on(self, async move |mut async_self| {
            async_self.send_introduction_request(route).await;
            Handled::OK
        })
    }

    /// Withdraw one route and cancel any probe or reachable-lease timer attached to it.
    pub(super) fn mark_route_stale(&mut self, route: DiscoveryRoute) {
        let timer_to_cancel = self
            .route_state
            .get_mut(&route)
            .and_then(|state| state.verification.mark_stale());
        if let Some(timer) = timer_to_cancel {
            self.cancel_timer(timer);
        }
        self.rebuild_published_member_routes();
    }

    /// Withdraw all verified routes because the local endpoint changed.
    fn invalidate_route_verification_for_endpoint_change(&mut self) {
        let mut timers_to_cancel = Vec::with_capacity(self.route_state.len());
        for route_state in self.route_state.values_mut() {
            if let Some(timer) = route_state.verification.mark_stale() {
                timers_to_cancel.push(timer);
            }
        }
        for timer in timers_to_cancel {
            self.cancel_timer(timer);
        }
        self.rebuild_published_member_routes();
    }

    /// Recompute the member-to-routes snapshot and publish only members whose route set changed.
    fn rebuild_published_member_routes(&mut self) {
        let new_member_routes = self.published_member_routes_from_state();
        if new_member_routes == self.member_route_snapshots {
            return;
        }
        let mut changed_members = TrieSet::new();
        let mut existing_routes = self.member_route_snapshots.entries();
        while let Some((member, routes)) = existing_routes.next() {
            if new_member_routes.get(&member) != Some(routes) {
                changed_members.insert(member.to_owned());
            }
        }
        let mut new_routes = new_member_routes.entries();
        while let Some((member, routes)) = new_routes.next() {
            if self.member_route_snapshots.get(&member) != Some(routes) {
                changed_members.insert(member.to_owned());
            }
        }
        self.member_route_snapshots = new_member_routes;
        for member in changed_members.owned_keys() {
            self.publish_member_routes(member);
        }
    }

    /// Build the currently publishable member route map from reachable route state.
    fn published_member_routes_from_state(&self) -> TrieMap<BTreeSet<SocketAddr>> {
        let mut member_routes: TrieMap<BTreeSet<SocketAddr>> = TrieMap::new();
        for (route, route_state) in &self.route_state {
            let DiscoveryRoute::Udp(route) = route;
            if let Some(members) = route_state.reachable_members() {
                for member in members.owned_keys() {
                    let mut routes = member_routes.remove(&member).unwrap_or_default();
                    routes.insert(*route);
                    member_routes.insert(member, routes);
                }
            }
        }
        member_routes
    }

    /// Return whether current route interest allows publishing `member`.
    fn route_interest_permits_member(
        &self,
        route: DiscoveryRoute,
        member: &MemberIdentity,
    ) -> bool {
        self.route_state
            .get(&route)
            .is_some_and(|state| state.interest.permits_member(member))
    }

    /// Publish the latest route set for one member.
    fn publish_member_routes(&mut self, member: MemberIdentity) {
        let local_bind = self
            .local_endpoint
            .binding()
            .map(|endpoint| endpoint.local_addr);
        let routes = self
            .member_route_snapshots
            .get(&member)
            .into_iter()
            .flat_map(|routes| routes.iter().copied())
            .map(|remote_addr| SendRouteCandidate {
                coverage_key: TransportRouteKey::Udp(UdpRouteKey {
                    remote_addr,
                    scope: DatagramRouteScope::Unicast,
                    local_bind,
                }),
                sharing: RouteSharingKind::Exclusive,
                preference_rank: RoutePreferenceRank::new(1),
            })
            .collect();
        self.discovery_port
            .trigger(crate::DiscoveryRouteUpdate::PeerRoutes {
                peer: member,
                routes,
            });
    }
}

impl ComponentLifecycle for RouteEstablishmentComponent {
    fn on_start(&mut self) -> HandlerResult {
        self.timing = self.load_timing_from_config().unrecoverable_err()?;
        Handled::OK
    }
}

ignore_requests!(
    RouteDiscoveryPort<TransportRouteKey>,
    RouteEstablishmentComponent
);
ignore_indications!(KeyMaterialDiscoveryPort, RouteEstablishmentComponent);

impl Require<RouteTransportPort<TransportRouteKey>> for RouteEstablishmentComponent {
    fn handle(
        &mut self,
        indication: RouteTransportInboundDeliver<TransportRouteKey>,
    ) -> HandlerResult {
        self.handle_route_transport_inbound(&indication)
    }
}

impl Require<RouteEndpointLifecyclePort> for RouteEstablishmentComponent {
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

impl Require<PeerAnnouncementObservationPort> for RouteEstablishmentComponent {
    fn handle(&mut self, indication: PeerAnnouncementObserved) -> HandlerResult {
        Handled::block_on(self, async move |mut async_self| {
            async_self.handle_peer_announcement_async(indication).await;
            Handled::OK
        })
    }
}

impl Require<EndpointSelectionPort> for RouteEstablishmentComponent {
    fn handle(&mut self, indication: EndpointSelection) -> HandlerResult {
        match ConcreteRoutes::from_routes(indication.endpoints) {
            Ok(routes) => {
                self.claim_routes = routes;
                Handled::OK
            }
            // Endpoint selection is produced by local runtime code. A non-concrete endpoint here is
            // a producer bug, so fail noisily and recoverably instead of silently dropping it.
            Err(error) => Err(HandlerError::recoverable(error)),
        }
    }
}

impl Actor for RouteEstablishmentComponent {
    type Message = RouteEstablishmentMessage;

    fn receive_local(&mut self, msg: Self::Message) -> HandlerResult {
        match msg {
            RouteEstablishmentMessage::ReplaceManualRouteWatches(watches) => {
                Handled::block_on(self, async move |mut async_self| {
                    let (promise, watches) = watches.take();
                    let result = async_self.replace_manual_route_watches_async(watches).await;
                    if promise.fulfil(result).is_err() {
                        debug!(
                            async_self.log(),
                            "manual route watch replacement promise was dropped before reply"
                        );
                    }
                    Handled::OK
                })
            }
            #[cfg(test)]
            RouteEstablishmentMessage::ExpireRouteForTest { route } => {
                self.mark_route_stale(route);
                Handled::OK
            }
        }
    }
}

/// Collapse caller-provided watches into one manual filter per route.
///
/// Multiple constrained watches union their expected members. Mixing constrained and
/// unconstrained watches for one route is rejected.
fn manual_filters_from_watches(
    watches: Vec<WatchedRoute>,
) -> Result<HashMap<DiscoveryRoute, ManualMemberFilter>, ManualRouteWatchError> {
    let mut filters = HashMap::with_capacity(watches.len());
    for watch in watches {
        filters
            .entry(watch.route)
            .or_insert_with(ManualMemberFilter::default)
            .try_add_expected_member(watch.route, watch.expected_member)?;
    }
    Ok(filters)
}

/// Verify the signature on a prepared claim and keep its group ids with its member.
async fn verify_prepared_claim(
    credentials: &dyn DiscoveryCredentials,
    claim: PendingClaimVerification,
) -> Result<VerifiedIntroductionClaim, BoxError> {
    let PendingClaimVerification {
        member,
        key_fingerprint,
        claim,
        signature,
    } = claim;
    credentials
        .verify_discovery_claim_payload(&member, key_fingerprint, &claim.claim_payload, &signature)
        .await?;
    let payload =
        decode_introduction_claim_payload_view(&claim.claim_payload).context(DecodePayloadSnafu)?;
    let decoded =
        DecodedIntroductionClaimPayload::decode_proto_view(&payload).context(DecodePayloadSnafu)?;
    if decoded.member != member {
        return Err(Box::new(VerifiedIntroductionClaimError::SelectorMismatch {
            field: "member_id",
        }));
    }
    if decoded.key_fingerprint != key_fingerprint {
        return Err(Box::new(VerifiedIntroductionClaimError::SelectorMismatch {
            field: "key_fingerprint",
        }));
    }
    Ok(VerifiedIntroductionClaim {
        member,
        key_fingerprint,
        group_ids: decoded.group_ids,
    })
}

/// Return the local groups whose current membership snapshot includes the local member.
pub(super) fn local_claim_group_ids(
    group_memberships: &SharedGroupMemberships,
    local_member: &MemberIdentity,
) -> Vec<GroupId> {
    let memberships = group_memberships.snapshot();
    memberships
        .group_ids()
        .copied()
        .filter(|group_id| {
            memberships
                .members(group_id)
                .is_some_and(|members| members.contains(local_member))
        })
        .collect()
}

/// Return whether the verified claim names at least one group that currently contains `member`.
pub(super) fn claim_matches_group_memberships(
    memberships: &GroupMemberships,
    member: &MemberIdentity,
    group_ids: &HashSet<GroupId>,
) -> bool {
    group_ids.iter().any(|group_id| {
        memberships
            .members(group_id)
            .is_some_and(|members| members.contains(member))
    })
}

/// Introduction claim whose signature matched its claimed member.
struct VerifiedIntroductionClaim {
    /// Member whose selected key material verified the signed claim payload.
    member: MemberIdentity,
    /// Exact public key bundle fingerprint that verified the signed claim payload.
    key_fingerprint: flotsync_security::KeyFingerprint,
    /// Decoded claim group ids after the signature check succeeded.
    group_ids: HashSet<GroupId>,
}

#[derive(Clone, Copy, Debug)]
struct RouteEstablishmentTiming {
    /// Maximum wait for a signed introduction response.
    probe_timeout: Duration,
    /// Time for which a verified route remains published before refresh.
    reachable_lease: Duration,
}

impl Default for RouteEstablishmentTiming {
    fn default() -> Self {
        Self {
            probe_timeout: config_keys::ROUTE_ESTABLISHMENT_PROBE_TIMEOUT
                .default()
                .expect("route establishment probe timeout has a default"),
            reachable_lease: config_keys::ROUTE_ESTABLISHMENT_REACHABLE_LEASE
                .default()
                .expect("route establishment reachable lease has a default"),
        }
    }
}
