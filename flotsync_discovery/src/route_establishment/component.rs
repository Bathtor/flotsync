use super::{DiscoveryCredentials, PeerAnnouncementObserved};
use crate::{
    config_keys,
    endpoint_selection::{EndpointSelection, EndpointSelectionPort},
    protocol::{
        DiscoveryRoute,
        decode_endpoint_discovery_frame_from_buf,
        decode_introduction_claim_group_ids,
        introduction_endpoint_frame,
        introduction_request_endpoint_frame,
    },
    route_publication::{
        DiscoveryRouteCandidate,
        DiscoveryRouteCandidates,
        DiscoveryRoutePort,
        DiscoveryRouteUpdate,
    },
};
use flotsync_core::{
    GroupId,
    MemberIdentity,
    member::{TrieMap, TrieSet},
    membership::{GroupMemberships, SharedGroupMemberships},
};
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
    serialisation::encode_message_payload,
    wire::{
        group_id_to_wire_bytes,
        member_identity_to_wire_format,
        uuid_from_wire_bytes,
        uuid_to_wire_bytes,
    },
};
use flotsync_utils::{BoxError, ResultExt as _, option_when};
use kompact::prelude::*;
use snafu::Snafu;
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
    wire::{discovery_signature_to_wire, prepare_claim_for_verification},
};

/// Actor messages accepted by [`RouteEstablishmentComponent`].
#[derive(Debug)]
pub enum RouteEstablishmentMessage {
    /// Setup-layer report for a freshly bound runtime endpoint socket.
    LocalEndpointBound {
        /// Socket id owned by the shared runtime endpoint.
        socket_id: SocketId,
        /// Concrete local address for the runtime endpoint bind.
        local_addr: SocketAddr,
    },
    /// Replace all manual route watches currently configured on this component.
    ReplaceManualRouteWatches(Ask<Vec<WatchedRoute>, Result<(), ManualRouteWatchError>>),
    /// Private result for one UDP send request.
    SendResult(UdpSendResult),
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

/// Route establishment route source for signed peer introductions.
#[derive(ComponentDefinition)]
pub struct RouteEstablishmentComponent {
    /// Kompact component context.
    ctx: ComponentContext<Self>,
    /// Route source port where verified peer routes are published.
    discovery_port: ProvidedPort<DiscoveryRoutePort>,
    /// Peer-announcement input from one or more announcement protocols.
    announcement_port: RequiredPort<super::PeerAnnouncementObservationPort>,
    /// UDP transport port shared with the replication endpoint.
    udp_port: RequiredPort<UdpPort>,
    /// Receives selected local endpoints used for signed introduction claims.
    endpoint_selection_port: RequiredPort<EndpointSelectionPort>,
    /// Pool used to encode outgoing discovery frames into lease-backed payloads.
    egress_pool: EgressPool,
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
    /// Monotonic transmission id source for UDP sends.
    next_transmission_id: TransmissionId,
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
        local_member: MemberIdentity,
        credentials: Arc<dyn DiscoveryCredentials>,
        group_memberships: SharedGroupMemberships,
        egress_pool: EgressPool,
    ) -> Self {
        let claim_routes = config.advertised_routes().clone();
        Self {
            ctx: ComponentContext::uninitialised(),
            discovery_port: ProvidedPort::uninitialised(),
            announcement_port: RequiredPort::uninitialised(),
            udp_port: RequiredPort::uninitialised(),
            endpoint_selection_port: RequiredPort::uninitialised(),
            egress_pool,
            config,
            claim_routes,
            local_member,
            credentials,
            group_memberships,
            timing: RouteEstablishmentTiming::default(),
            local_endpoint: LocalUdpEndpointState::Unbound,
            next_transmission_id: TransmissionId::ONE,
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

    /// Process one UDP indication from the shared runtime endpoint.
    pub(super) fn handle_udp_indication(&mut self, indication: UdpIndication) -> HandlerResult {
        match indication {
            UdpIndication::Received {
                socket_id,
                source,
                payload,
            } => self.handle_udp_payload(socket_id, source, &payload),
            UdpIndication::Closed { socket_id, .. } => {
                if self.local_endpoint.is_bound_socket(socket_id) {
                    self.local_endpoint = LocalUdpEndpointState::Unbound;
                    self.invalidate_route_verification_for_endpoint_change();
                }
                Handled::OK
            }
            UdpIndication::Bound { .. }
            | UdpIndication::BindFailed { .. }
            | UdpIndication::Connected { .. }
            | UdpIndication::ConnectFailed { .. }
            | UdpIndication::Configured { .. }
            | UdpIndication::ConfigureFailed { .. }
            | UdpIndication::ReadSuspended { .. }
            | UdpIndication::ReadResumed { .. }
            | UdpIndication::WriteSuspended { .. }
            | UdpIndication::WriteResumed { .. } => Handled::OK,
        }
    }

    /// Record the currently bound shared runtime endpoint and invalidate routes tied to the old
    /// endpoint.
    fn on_local_endpoint_bound(&mut self, socket_id: SocketId, local_addr: SocketAddr) {
        let binding = LocalUdpEndpointBinding {
            socket_id,
            local_addr,
        };
        if self.local_endpoint.binding() == Some(binding) {
            return;
        }
        info!(
            self.log(),
            "route establishment accepted local endpoint bind local_addr={} socket_id={}",
            local_addr,
            socket_id
        );
        self.local_endpoint = LocalUdpEndpointState::Bound(binding);
        self.invalidate_route_verification_for_endpoint_change();
    }

    /// Dispatch UDP payloads received on the currently bound runtime endpoint.
    fn handle_udp_payload(
        &mut self,
        socket_id: SocketId,
        source: SocketAddr,
        payload: &IoPayload,
    ) -> HandlerResult {
        if self.local_endpoint.is_bound_socket(socket_id) {
            self.handle_possible_endpoint_discovery_frame(source, payload)
        } else {
            Handled::OK
        }
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
        let frame = introduction_request_endpoint_frame(uuid_to_wire_bytes(nonce));
        let payload = match encode_message_payload(&self.egress_pool, &frame).await {
            Ok(payload) => payload,
            Err(error) => {
                debug!(
                    self.log(),
                    "failed to encode route establishment introduction request for {:?}: {}",
                    route,
                    error
                );
                return;
            }
        };
        let transmission_id = self.next_transmission_id.take_next();
        let reply_to = self
            .actor_ref()
            .recipient_with(RouteEstablishmentMessage::SendResult);
        self.udp_port.trigger(UdpRequest::Send {
            socket_id: endpoint.socket_id,
            transmission_id,
            payload,
            target: Some(target),
            reply_to,
        });
        trace!(
            self.log(),
            "route establishment sent introduction request to {}", target
        );

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
            None => {
                debug!(
                    self.log(),
                    "ignored discovery endpoint frame from {} without a discovery body", source
                );
                Handled::OK
            }
        }
    }

    /// Build and send a signed introduction response for a valid introduction request.
    async fn answer_introduction_request_async(
        &mut self,
        source: SocketAddr,
        request: discovery_proto::IntroductionRequest,
    ) -> HandlerResult {
        if request.request_nonce.is_empty() {
            trace!(
                self.log(),
                "ignored introduction request from {} with empty nonce", source
            );
            return Handled::OK;
        }
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
        let request_nonce = request.request_nonce;
        let group_ids = group_ids
            .iter()
            .copied()
            .map(group_id_to_wire_bytes)
            .collect::<Vec<_>>();
        let member_id = member_identity_to_wire_format(&self.local_member);
        let mut claims = Vec::with_capacity(self.claim_routes.len());
        for route in self.claim_routes.iter() {
            let route = DiscoveryRoute::Udp(*route).to_wire_format();
            let claim_payload = discovery_proto::IntroductionClaimPayload {
                instance_uuid: instance_uuid.clone(),
                request_nonce: request_nonce.clone(),
                route: MessageField::some(route),
                group_ids: group_ids.clone(),
                ..discovery_proto::IntroductionClaimPayload::default()
            };
            let claim_payload = claim_payload.encode_to_vec();
            let signature = match self
                .credentials
                .sign_discovery_claim_payload(&claim_payload)
            {
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
                member_id: MessageField::some(member_id.clone()),
                claim_payload,
                signature: MessageField::some(discovery_signature_to_wire(&signature)),
                ..discovery_proto::SignedIntroductionClaim::default()
            });
        }
        let introduction = discovery_proto::Introduction {
            instance_uuid,
            request_nonce,
            claims,
            ..discovery_proto::Introduction::default()
        };
        let frame = introduction_endpoint_frame(introduction);
        let payload = encode_message_payload(&self.egress_pool, &frame)
            .await
            .whatever_benign(format!(
                "failed to encode route establishment introduction for {source}"
            ))?;
        let transmission_id = self.next_transmission_id.take_next();
        let reply_to = self
            .actor_ref()
            .recipient_with(RouteEstablishmentMessage::SendResult);
        self.udp_port.trigger(UdpRequest::Send {
            socket_id: endpoint.socket_id,
            transmission_id,
            payload,
            target: Some(source),
            reply_to,
        });
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
            match verify_prepared_claim(self.credentials.as_ref(), claim).await {
                Ok(verified_claim) => {
                    if claim_matches_group_memberships(
                        memberships.as_ref(),
                        &verified_claim.member,
                        &verified_claim.group_ids,
                    ) && self
                        .route_interest_permits_member(prepared.route, &verified_claim.member)
                    {
                        accepted_members.insert(verified_claim.member);
                    }
                }
                Err(error) => {
                    debug!(
                        self.log(),
                        "ignored unverifiable introduction claim from {}: {}", source, error
                    );
                }
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
        let probe_nonce = uuid_to_wire_bytes(probe.nonce);
        if introduction.request_nonce != probe_nonce {
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
                    &probe_nonce,
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
            .map(|route| DiscoveryRouteCandidate {
                route: DiscoveryRoute::Udp(route),
                local_bind,
            })
            .collect::<DiscoveryRouteCandidates>();
        self.discovery_port
            .trigger(DiscoveryRouteUpdate::PeerRoutes {
                peer: member,
                routes,
            });
    }

    /// Log the result of one UDP send request owned by route establishment.
    fn handle_send_result(&mut self, result: &UdpSendResult) -> HandlerResult {
        match result {
            UdpSendResult::Ack {
                socket_id,
                transmission_id,
            } => {
                trace!(
                    self.log(),
                    "route establishment send acknowledged socket_id={} transmission_id={}",
                    socket_id,
                    transmission_id
                );
            }
            UdpSendResult::Nack {
                socket_id,
                transmission_id,
                reason,
            } => {
                debug!(
                    self.log(),
                    "route establishment send failed socket_id={} transmission_id={}: {:?}",
                    socket_id,
                    transmission_id,
                    reason
                );
            }
        }
        Handled::OK
    }
}

impl ComponentLifecycle for RouteEstablishmentComponent {
    fn on_start(&mut self) -> HandlerResult {
        self.timing = self.load_timing_from_config().unrecoverable_err()?;
        Handled::OK
    }
}

ignore_requests!(DiscoveryRoutePort, RouteEstablishmentComponent);

impl Require<UdpPort> for RouteEstablishmentComponent {
    fn handle(&mut self, indication: UdpIndication) -> HandlerResult {
        self.handle_udp_indication(indication)
    }
}

impl Require<super::PeerAnnouncementObservationPort> for RouteEstablishmentComponent {
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
            RouteEstablishmentMessage::LocalEndpointBound {
                socket_id,
                local_addr,
            } => {
                self.on_local_endpoint_bound(socket_id, local_addr);
                Handled::block_on(self, async move |mut async_self| {
                    async_self.probe_all_inactive_routes().await;
                    Handled::OK
                })
            }
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
            RouteEstablishmentMessage::SendResult(result) => self.handle_send_result(&result),
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
    credentials
        .verify_discovery_claim_payload(&claim.member, &claim.claim.claim_payload, &claim.signature)
        .await?;
    let group_ids = decode_introduction_claim_group_ids(&claim.claim.claim_payload)?;
    Ok(VerifiedIntroductionClaim {
        member: claim.member,
        group_ids,
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
    /// Member whose trusted key verified the signed claim payload.
    member: MemberIdentity,
    /// Decoded claim group ids after the signature check succeeded.
    group_ids: HashSet<GroupId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct LocalUdpEndpointBinding {
    /// Socket id owned by the shared UDP runtime endpoint.
    pub(super) socket_id: SocketId,
    /// Concrete local address observed after bind.
    pub(super) local_addr: SocketAddr,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum LocalUdpEndpointState {
    /// The shared UDP runtime endpoint has not been observed yet.
    #[default]
    Unbound,
    /// The shared UDP runtime endpoint is available for discovery traffic.
    Bound(LocalUdpEndpointBinding),
}

impl LocalUdpEndpointState {
    pub(super) fn binding(self) -> Option<LocalUdpEndpointBinding> {
        match self {
            Self::Bound(binding) => Some(binding),
            Self::Unbound => None,
        }
    }

    fn is_bound_socket(self, socket_id: SocketId) -> bool {
        self.binding()
            .is_some_and(|binding| binding.socket_id == socket_id)
    }
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
