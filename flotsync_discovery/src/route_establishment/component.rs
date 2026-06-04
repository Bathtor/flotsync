use super::{DiscoveryCredentials, PeerAnnouncementObserved};
use crate::{
    config_keys,
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
    state::{PeerRouteState, PendingClaimVerification, PendingProbe, RouteProbeKey},
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
    /// Private result for one UDP send request.
    SendResult(UdpSendResult),
    /// Test hook that withdraws one route without waiting for a wall-clock timer.
    #[cfg(test)]
    ExpireRouteForTest {
        /// Peer process instance whose route should be withdrawn.
        instance_id: Uuid,
        /// Exact route endpoint to withdraw.
        route: SocketAddr,
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
    /// Pool used to encode outgoing discovery frames into lease-backed payloads.
    egress_pool: EgressPool,
    /// Static runtime settings and advertised routes for this discovery source.
    config: super::RouteEstablishmentConfig,
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
    /// Per advertised peer-instance route verification state.
    route_state: HashMap<RouteProbeKey, PeerRouteState>,
    /// Last published route snapshot, rebuilt from `routes` after verification state changes.
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
        Self {
            ctx: ComponentContext::uninitialised(),
            discovery_port: ProvidedPort::uninitialised(),
            announcement_port: RequiredPort::uninitialised(),
            udp_port: RequiredPort::uninitialised(),
            egress_pool,
            config,
            local_member,
            credentials,
            group_memberships,
            timing: RouteEstablishmentTiming::default(),
            local_endpoint: LocalUdpEndpointState::Unbound,
            next_transmission_id: TransmissionId::ONE,
            route_state: HashMap::new(),
            member_route_snapshots: TrieMap::new(),
        }
    }

    /// Return the observed local UDP endpoint state without exposing writable internals.
    #[cfg(test)]
    pub(super) fn local_endpoint(&self) -> LocalUdpEndpointState {
        self.local_endpoint
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

    /// Record advertised routes and return routes that should be probed.
    pub(super) fn record_peer_announcement(
        &mut self,
        peer: PeerAnnouncementObserved,
    ) -> Vec<RouteProbeKey> {
        let mut probes = Vec::new();
        for route in peer.routes {
            let DiscoveryRoute::Udp(route) = route;
            let key = RouteProbeKey {
                instance_id: peer.instance_id,
                route,
            };
            let route = self.route_state.entry(key).or_insert(PeerRouteState::KNOWN);
            if !route.has_active_timeout() {
                probes.push(key);
            }
        }
        probes
    }

    /// Probe all known routes that are not already probing or published.
    async fn probe_all_inactive_routes(&mut self) {
        let routes = self
            .route_state
            .iter()
            .filter_map(|(key, route)| option_when!(!route.has_active_timeout(), *key))
            .collect::<Vec<_>>();
        self.send_introduction_requests(routes).await;
    }

    /// Send introduction probes for the provided route keys in order.
    async fn send_introduction_requests(&mut self, keys: impl IntoIterator<Item = RouteProbeKey>) {
        for key in keys {
            self.send_introduction_request(key).await;
        }
    }

    /// Send one introduction request and start the matching probe timeout.
    async fn send_introduction_request(&mut self, key: RouteProbeKey) {
        if self
            .route_state
            .get(&key)
            .is_none_or(PeerRouteState::has_active_timeout)
        {
            return;
        }
        let Some(endpoint) = self.local_endpoint.binding() else {
            debug!(
                self.log(),
                "route establishment recorded route {} for instance {} while local endpoint was not bound",
                key.route,
                key.instance_id
            );
            return;
        };

        let nonce = Uuid::new_v4();
        let frame = introduction_request_endpoint_frame(uuid_to_wire_bytes(nonce));
        let payload = match encode_message_payload(&self.egress_pool, &frame).await {
            Ok(payload) => payload,
            Err(error) => {
                debug!(
                    self.log(),
                    "failed to encode route establishment introduction request for {}: {}",
                    key.route,
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
            target: Some(key.route),
            reply_to,
        });

        let timeout = self.timing.probe_timeout;
        let timer = self.schedule_once(timeout, move |component, timeout| {
            component.handle_probe_timeout(key, nonce, &timeout)
        });
        let timer_to_cancel = self
            .route_state
            .get_mut(&key)
            .expect("probe route was checked before scheduling")
            .mark_probing(PendingProbe { nonce, timer });
        if let Some(timer) = timer_to_cancel {
            self.cancel_timer(timer);
        }
    }

    /// Expire one active introduction probe if the nonce and timer still match.
    fn handle_probe_timeout(
        &mut self,
        key: RouteProbeKey,
        nonce: Uuid,
        actual_timer: &ScheduledTimer,
    ) -> HandlerResult {
        if let Some(route) = self.route_state.get_mut(&key)
            && route.expire_probe_if_matches(nonce, actual_timer)
        {
            self.mark_route_stale(key);
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
        if self.config.advertised_routes().is_empty() {
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
        let mut claims = Vec::with_capacity(self.config.advertised_routes().len());
        for route in self.config.advertised_routes() {
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
            self.mark_route_stale(prepared.key);
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
                    ) {
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
            self.mark_route_stale(prepared.key);
            return;
        }
        self.mark_route_reachable(prepared.key, accepted_members);
    }

    /// Collect introduction claims that match the currently active probe.
    fn collect_verifiable_claims_for_active_probe(
        &self,
        source: SocketAddr,
        introduction: discovery_proto::Introduction,
    ) -> Option<super::state::PartiallyVerifiedIntroduction> {
        let Ok(instance_id) =
            uuid_from_wire_bytes(&introduction.instance_uuid, "Introduction.instance_uuid")
        else {
            trace!(
                self.log(),
                "ignored introduction from {} with malformed instance id", source
            );
            return None;
        };
        let key = RouteProbeKey {
            instance_id,
            route: source,
        };
        let Some(route) = self.route_state.get(&key) else {
            trace!(
                self.log(),
                "ignored unsolicited introduction from {} for instance {}", source, instance_id
            );
            return None;
        };
        let Some(probe) = route.pending_probe() else {
            trace!(
                self.log(),
                "ignored introduction from {} without an active probe", source
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
                match prepare_claim_for_verification(&key, &probe_nonce, &self.local_member, claim)
                {
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
        Some(super::state::PartiallyVerifiedIntroduction { key, claims })
    }

    /// Publish one route for the verified members until the reachable lease expires.
    pub(super) fn mark_route_reachable(&mut self, key: RouteProbeKey, accepted_members: TrieSet) {
        if !self.route_state.contains_key(&key) {
            return;
        }
        let timer = self.schedule_once(self.timing.reachable_lease, move |component, timeout| {
            component.handle_reachable_lease_expired(key, &timeout)
        });
        let timer_to_cancel = self
            .route_state
            .get_mut(&key)
            .expect("reachable route was checked before scheduling")
            .mark_reachable(timer, accepted_members);
        if let Some(timer) = timer_to_cancel {
            self.cancel_timer(timer);
        }
        self.rebuild_published_member_routes();
    }

    /// Expire one reachable lease if it still owns the active timer, then schedule a refresh probe.
    fn handle_reachable_lease_expired(
        &mut self,
        key: RouteProbeKey,
        actual_timer: &ScheduledTimer,
    ) -> HandlerResult {
        let Some(route) = self.route_state.get_mut(&key) else {
            return Handled::OK;
        };
        if !route.expire_reachable_lease_if_matches(actual_timer) {
            return Handled::OK;
        }
        self.rebuild_published_member_routes();
        Handled::block_on(self, async move |mut async_self| {
            async_self.send_introduction_request(key).await;
            Handled::OK
        })
    }

    /// Withdraw one route and cancel any probe or reachable-lease timer attached to it.
    pub(super) fn mark_route_stale(&mut self, key: RouteProbeKey) {
        let timer_to_cancel = self
            .route_state
            .get_mut(&key)
            .and_then(PeerRouteState::mark_stale);
        if let Some(timer) = timer_to_cancel {
            self.cancel_timer(timer);
        }
        self.rebuild_published_member_routes();
    }

    /// Withdraw all verified routes because the local endpoint changed.
    fn invalidate_route_verification_for_endpoint_change(&mut self) {
        let mut timers_to_cancel = Vec::new();
        for route in self.route_state.values_mut() {
            if let Some(timer) = route.mark_stale() {
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
        for (member, routes) in &self.member_route_snapshots {
            if new_member_routes.get(&member) != Some(routes) {
                changed_members.insert(member);
            }
        }
        for (member, routes) in &new_member_routes {
            if self.member_route_snapshots.get(&member) != Some(routes) {
                changed_members.insert(member);
            }
        }
        self.member_route_snapshots = new_member_routes;
        for member in &changed_members {
            self.publish_member_routes(member);
        }
    }

    /// Build the currently publishable member route map from reachable route state.
    fn published_member_routes_from_state(&self) -> TrieMap<BTreeSet<SocketAddr>> {
        let mut member_routes: TrieMap<BTreeSet<SocketAddr>> = TrieMap::new();
        for (key, route) in &self.route_state {
            if let Some(members) = route.reachable_members() {
                for member in members {
                    let mut routes = member_routes.remove(&member).unwrap_or_default();
                    routes.insert(key.route);
                    member_routes.insert(member, routes);
                }
            }
        }
        member_routes
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
            RouteEstablishmentMessage::SendResult(result) => self.handle_send_result(&result),
            #[cfg(test)]
            RouteEstablishmentMessage::ExpireRouteForTest { instance_id, route } => {
                self.mark_route_stale(RouteProbeKey { instance_id, route });
                Handled::OK
            }
        }
    }
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
