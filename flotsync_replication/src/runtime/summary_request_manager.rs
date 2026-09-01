//! Application and route-triggered summary request protocol management.

use super::errors::{InboundDeliveryError, InboundFailureAction, SummaryError, inbound, summary};
use crate::{
    api::{ApiError, Summary, SummaryRequest},
    codecs::messages::{
        RuntimeMessage,
        RuntimeMessageDecodeContext,
        SummaryMessage,
        SummaryRequestMessage,
    },
    delivery::{
        contracts::{
            ReliableDeliveryPort,
            ReliableDeliveryPortIndication,
            ReliableDeliveryPortRequest,
        },
        reliable_delivery::{
            ReliableDeliveryDeliver,
            ReliableDeliverySubmit,
            ReliableMessageEnvelope,
            ReliableMessageHeader,
        },
        shared::{MessageId, PlaintextPayload, ReliableMessageScope},
    },
};
use flotsync_core::{GroupId, MemberIdentity, member::TrieSet, membership::SharedGroupMemberships};
use flotsync_messages::proto::{DecodeProtoViewWith, EncodeProto};
use flotsync_routes::{DiscoveryRouteUpdate, RouteDiscoveryPort, TransportRouteKey};
use flotsync_utils::{KClaimablePromise, OptionExt as _};
use iddqd::{BiHashItem, BiHashMap, bi_upcast};
use kompact::prelude::*;
use snafu::prelude::*;
use std::{
    collections::{HashMap, hash_map::RandomState},
    sync::Arc,
    time::Duration,
};
use uuid::Uuid;

/// Local-actor messages understood by [`SummaryRequestManagerComponent`].
#[derive(Debug)]
pub(super) enum SummaryRequestManagerMessage {
    /// Ask one group member for its current group version vector.
    RequestSummary(Ask<SummaryRequest, Result<Summary, ApiError>>),
}

/// Route-triggered summary operations indexed by semantic target and operation UUID.
type InternalSummaryRequests = BiHashMap<InternalSummaryRequest, RandomState>;

/// Outstanding application summary request waiting for a matching runtime reply.
struct PendingSummaryRequest {
    group_id: GroupId,
    target: MemberIdentity,
    promise: KPromise<Result<Summary, ApiError>>,
    timeout_timer: ScheduledTimer,
}

/// One group and peer whose route-triggered summary operation is tracked.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct InternalSummaryTarget {
    group_id: GroupId,
    peer: MemberIdentity,
}

/// Identity and timeout state for one route-triggered summary operation.
///
/// A missing `timeout_timer` means that the last attempt timed out and may be
/// resubmitted after a later route transition. `operation_id` is both its wire
/// correlation and reliable-delivery message identity, so a continuation cannot
/// create distinct retained work for the same semantic operation.
struct InternalSummaryRequest {
    /// Semantic group and peer key used to suppress duplicate operations.
    target: InternalSummaryTarget,
    /// Wire correlation and reliable-delivery message identity for this operation.
    operation_id: Uuid,
    /// Expected response timer, or `None` once the current attempt has timed out.
    timeout_timer: Option<ScheduledTimer>,
}

impl BiHashItem for InternalSummaryRequest {
    type K1<'a> = &'a InternalSummaryTarget;
    type K2<'a> = Uuid;

    fn key1(&self) -> Self::K1<'_> {
        &self.target
    }

    fn key2(&self) -> Self::K2<'_> {
        self.operation_id
    }

    bi_upcast!();
}

/// Envelope metadata included in inbound summary fault logs.
#[derive(Clone, Debug)]
enum SummaryInboundContext {
    /// Recipient-addressed envelope handed over by reliable delivery.
    Reliable {
        sender: MemberIdentity,
        recipient: MemberIdentity,
        message_id: MessageId,
    },
}

impl SummaryInboundContext {
    fn reliable(header: &ReliableMessageHeader) -> Self {
        Self::Reliable {
            sender: header.sender.clone(),
            recipient: header.recipient.clone(),
            message_id: header.message_id,
        }
    }
}

impl std::fmt::Display for SummaryInboundContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Reliable {
                sender,
                recipient,
                message_id,
            } => write!(
                f,
                "reliable delivery summary message {message_id} from {sender} to {recipient}"
            ),
        }
    }
}

/// Inbound summary failure paired with the envelope that caused it.
struct SummaryInboundFailure {
    context: SummaryInboundContext,
    error: Box<InboundDeliveryError>,
}

impl SummaryInboundFailure {
    fn new(context: SummaryInboundContext, error: InboundDeliveryError) -> Self {
        Self {
            context,
            error: Box::new(error),
        }
    }
}

/// Kompact component that owns summary request/response protocol state.
///
/// Besides application requests, the component observes direct-route transitions
/// and starts summary-based synchronisation for shared hosted groups. It tracks
/// those internal operations independently of current route availability so route
/// flapping cannot accumulate duplicate reliable-delivery work.
#[derive(ComponentDefinition)]
pub(super) struct SummaryRequestManagerComponent {
    ctx: ComponentContext<Self>,
    reliable_delivery_port: RequiredPort<ReliableDeliveryPort>,
    route_discovery_port: RequiredPort<RouteDiscoveryPort<TransportRouteKey>>,
    local_member: MemberIdentity,
    group_memberships: Arc<dyn SharedGroupMemberships>,
    request_timeout: Duration,
    pending_summaries: HashMap<Uuid, PendingSummaryRequest>,
    /// Route-triggered operations with atomic semantic-target and correlation indexes.
    internal_summaries: InternalSummaryRequests,
    /// Peers whose latest complete direct-route snapshot was non-empty.
    available_direct_peers: TrieSet,
}

impl SummaryRequestManagerComponent {
    pub(super) fn new(
        local_member: MemberIdentity,
        group_memberships: Arc<dyn SharedGroupMemberships>,
        request_timeout: Duration,
    ) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            reliable_delivery_port: RequiredPort::uninitialised(),
            route_discovery_port: RequiredPort::uninitialised(),
            local_member,
            group_memberships,
            request_timeout,
            pending_summaries: HashMap::new(),
            internal_summaries: BiHashMap::with_hasher(RandomState::new()),
            available_direct_peers: TrieSet::new(),
        }
    }

    /// Return whether the latest complete direct-route snapshot for `peer` was non-empty.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn knows_direct_route(&self, peer: &MemberIdentity) -> bool {
        self.available_direct_peers.contains(peer)
    }

    fn reply_api<T>(
        &self,
        promise: KPromise<Result<T, ApiError>>,
        operation: &'static str,
        reply: Result<T, ApiError>,
    ) where
        T: Send + 'static,
    {
        if promise.fulfil(reply).is_err() {
            warn!(self.log(), "dropping {operation} reply");
        }
    }

    fn validate_summary_request(&self, request: &SummaryRequest) -> Result<(), SummaryError> {
        let memberships = self.group_memberships.snapshot();
        let members =
            memberships
                .members(&request.group_id)
                .context(summary::UnknownGroupSnafu {
                    group_id: request.group_id,
                })?;
        ensure!(
            members.contains(&request.target),
            summary::TargetNotInGroupSnafu {
                group_id: request.group_id,
                target: request.target.clone(),
            }
        );
        Ok(())
    }

    fn submit_reliable_runtime_message(
        &mut self,
        recipient: MemberIdentity,
        message_id: MessageId,
        message: &RuntimeMessage,
    ) {
        let payload = message.encode_proto_to_bytes();
        self.reliable_delivery_port
            .trigger(ReliableDeliveryPortRequest::Submit(
                ReliableDeliverySubmit {
                    envelope: ReliableMessageEnvelope::<PlaintextPayload> {
                        header: ReliableMessageHeader {
                            sender: self.local_member.clone(),
                            recipient,
                            message_id,
                            scope: ReliableMessageScope::Group {
                                group_id: message.group_id(),
                            },
                        },
                        payload: PlaintextPayload { bytes: payload },
                    },
                },
            ));
    }

    /// Submit or retry one route-triggered summary operation.
    fn submit_internal_summary_request(
        &mut self,
        group_id: GroupId,
        target: MemberIdentity,
    ) -> Result<(), HandlerError> {
        let target = InternalSummaryTarget {
            group_id,
            peer: target,
        };
        let existing = self
            .internal_summaries
            .get1(&target)
            .map(|request| (request.operation_id, request.timeout_timer.is_none()));
        match existing {
            None => self.start_internal_summary_request(&target),
            Some((operation_id, true)) => {
                self.retry_internal_summary_request(&target, operation_id)
            }
            Some((_, false)) => {
                // The current attempt is still pending, so this route snapshot adds no work.
                Ok(())
            }
        }
    }

    /// Start tracking and submit a new route-triggered summary operation.
    fn start_internal_summary_request(
        &mut self,
        target: &InternalSummaryTarget,
    ) -> Result<(), HandlerError> {
        let operation_id = Uuid::new_v4();
        let timeout_timer = self.schedule_internal_summary_timeout(operation_id);
        let request = InternalSummaryRequest {
            target: target.clone(),
            operation_id,
            timeout_timer: Some(timeout_timer.clone()),
        };
        if self.internal_summaries.insert_unique(request).is_ok() {
            self.submit_internal_summary_message(target, operation_id);
            Ok(())
        } else {
            self.cancel_timer(timeout_timer);
            None.whatever_unrecoverable(format!(
                "new internal summary operation unexpectedly conflicts for group {} and peer {}",
                target.group_id, target.peer
            ))
        }
    }

    /// Retry a timed-out operation while preserving its reliable message identity.
    fn retry_internal_summary_request(
        &mut self,
        target: &InternalSummaryTarget,
        operation_id: Uuid,
    ) -> Result<(), HandlerError> {
        let timeout_timer = self.schedule_internal_summary_timeout(operation_id);
        let Some(mut request) = self.internal_summaries.get1_mut(target) else {
            self.cancel_timer(timeout_timer);
            return None.whatever_unrecoverable(format!(
                "checked internal summary operation disappeared for group {} and peer {}",
                target.group_id, target.peer
            ));
        };
        request.timeout_timer = Some(timeout_timer);
        drop(request);
        self.submit_internal_summary_message(target, operation_id);
        Ok(())
    }

    /// Set the response timeout for one internal operation attempt.
    fn schedule_internal_summary_timeout(&mut self, correlation_id: Uuid) -> ScheduledTimer {
        self.schedule_once(self.request_timeout, move |component, expected_timer| {
            component.handle_internal_summary_timeout(correlation_id, &expected_timer)
        })
    }

    /// Submit one internal request with its retained operation identity.
    fn submit_internal_summary_message(
        &mut self,
        target: &InternalSummaryTarget,
        operation_id: Uuid,
    ) {
        let message = RuntimeMessage::SummaryRequest(SummaryRequestMessage {
            group_id: target.group_id,
            correlation_id: operation_id,
        });
        self.submit_reliable_runtime_message(
            target.peer.clone(),
            MessageId(operation_id),
            &message,
        );
    }

    /// Start summary-based reconciliation for every hosted group shared with `peer`.
    fn synchronise_with_available_peer(
        &mut self,
        peer: &MemberIdentity,
    ) -> Result<(), HandlerError> {
        let memberships = self.group_memberships.snapshot();
        for group_id in memberships.groups_with_member(peer) {
            self.submit_internal_summary_request(group_id, peer.clone())?;
        }
        Ok(())
    }

    /// Track direct-route availability and synchronise on unavailable-to-available transitions.
    fn handle_route_update(
        &mut self,
        update: DiscoveryRouteUpdate<TransportRouteKey>,
    ) -> HandlerResult {
        match update {
            DiscoveryRouteUpdate::PeerRoutes { peer, routes } if peer != self.local_member => {
                if routes.is_empty() {
                    self.available_direct_peers.remove(&peer);
                } else if self.available_direct_peers.insert(peer.clone()) {
                    self.synchronise_with_available_peer(&peer)?;
                } // else: The peer stayed available, so there is no transition to synchronise.
            }
            DiscoveryRouteUpdate::PeerRoutes { .. } | DiscoveryRouteUpdate::RelayRoutes { .. } => {
                // Local-peer snapshots and relay routes do not establish direct remote work.
            }
        }
        Handled::OK
    }

    fn fulfil_pending_summary(
        &mut self,
        correlation_id: Uuid,
        summary: Summary,
    ) -> Result<(), HandlerError> {
        if let Some(pending) = self.pending_summaries.remove(&correlation_id) {
            if summary.group_id != pending.group_id || summary.responder != pending.target {
                warn!(
                    self.log(),
                    "ignoring mismatched summary response for group {} from {}",
                    summary.group_id,
                    summary.responder
                );
                self.pending_summaries.insert(correlation_id, pending);
            } else {
                self.cancel_timer(pending.timeout_timer);
                if pending.promise.fulfil(Ok(summary)).is_err() {
                    warn!(self.log(), "dropping request_summary reply");
                }
            }
        } else {
            self.fulfil_internal_summary(correlation_id, &summary)?;
        }
        Ok(())
    }

    /// Complete matching route-triggered work and retain mismatched responses.
    fn fulfil_internal_summary(
        &mut self,
        correlation_id: Uuid,
        summary: &Summary,
    ) -> Result<(), HandlerError> {
        if let Some(request) = self.internal_summaries.get2(&correlation_id) {
            if summary.group_id != request.target.group_id
                || summary.responder != request.target.peer
            {
                warn!(
                    self.log(),
                    "ignoring mismatched internal summary response for group {} from {}",
                    summary.group_id,
                    summary.responder
                );
            } else {
                let request = self
                    .internal_summaries
                    .remove2(&correlation_id)
                    .whatever_unrecoverable(
                        "validated internal summary operation must remain indexed",
                    )?;
                if let Some(timeout_timer) = request.timeout_timer {
                    self.cancel_timer(timeout_timer);
                }
            }
        }
        Ok(())
    }

    /// Mark one internal operation retryable after its current response timeout.
    fn handle_internal_summary_timeout(
        &mut self,
        correlation_id: Uuid,
        expected_timer: &ScheduledTimer,
    ) -> HandlerResult {
        if let Some(mut request) = self.internal_summaries.get2_mut(&correlation_id)
            && request.timeout_timer.as_ref() == Some(expected_timer)
        {
            request.timeout_timer = None;
        }
        Handled::OK
    }

    fn handle_summary_timeout(
        &mut self,
        correlation_id: Uuid,
        expected_timer: &ScheduledTimer,
    ) -> HandlerResult {
        let Some(pending) = self.pending_summaries.get(&correlation_id) else {
            return Handled::OK;
        };
        if &pending.timeout_timer != expected_timer {
            return Handled::OK;
        }
        let pending = self
            .pending_summaries
            .remove(&correlation_id)
            .whatever_unrecoverable("checked pending summary must still exist")?;
        if pending
            .promise
            .fulfil(Err(ApiError::SummaryTimedOut {
                group_id: pending.group_id,
                target: pending.target,
            }))
            .is_err()
        {
            warn!(self.log(), "dropping request_summary timeout reply");
        }
        Handled::OK
    }

    fn record_inbound_failure(&self, failure: &SummaryInboundFailure) -> InboundFailureAction {
        let action = failure.error.failure_action();
        match action {
            InboundFailureAction::Drop => {
                warn!(
                    self.log(),
                    "dropping inbound {} after recoverable error: {}",
                    failure.context,
                    failure.error
                );
            }
            InboundFailureAction::Fatal => {
                error!(
                    self.log(),
                    "fatal inbound {} failure: {}", failure.context, failure.error
                );
            }
        }
        action
    }

    fn handle_summary(
        &mut self,
        context: SummaryInboundContext,
        sender: MemberIdentity,
        processed: KClaimablePromise<()>,
        message: SummaryMessage,
    ) -> HandlerResult {
        let summary = Summary {
            group_id: message.group_id,
            responder: sender,
            has_versions: message.has_versions,
        };
        self.fulfil_pending_summary(message.correlation_id, summary)?;
        let reply = processed
            .complete()
            .context(inbound::CompleteProcessedPromiseSnafu {
                group_id: message.group_id,
            });
        if let Err(error) = reply {
            let failure = SummaryInboundFailure::new(context, error);
            let action = self.record_inbound_failure(&failure);
            return handled_after_inbound_failure(action, &failure);
        }
        Handled::OK
    }

    fn handle_reliable_delivery(&mut self, deliver: ReliableDeliveryDeliver) -> HandlerResult {
        let context = SummaryInboundContext::reliable(&deliver.envelope.header);
        let memberships = self.group_memberships.snapshot();
        let decode_context = RuntimeMessageDecodeContext::new(memberships.as_ref());
        let message_res = RuntimeMessage::decode_proto_view_from_slice_with(
            &deliver.envelope.payload.bytes,
            decode_context,
        )
        .context(inbound::DecodeMessageSnafu);
        let message = match message_res {
            Ok(message) => message,
            Err(error) => {
                let failure = SummaryInboundFailure::new(context, error);
                let action = self.record_inbound_failure(&failure);
                return handled_after_inbound_failure(action, &failure);
            }
        };
        match message {
            RuntimeMessage::Summary(message) => {
                let sender = deliver.envelope.header.sender.clone();
                self.handle_summary(context, sender, deliver.processed, message)
            }
            RuntimeMessage::Update(_)
            | RuntimeMessage::SummaryRequest(_)
            | RuntimeMessage::NeedRange(_)
            | RuntimeMessage::UpdateBatch(_)
            | RuntimeMessage::GroupInvitation(_)
            | RuntimeMessage::MigrationProposal(_) => Handled::OK,
        }
    }

    fn handle_request_summary(
        &mut self,
        ask: Ask<SummaryRequest, Result<Summary, ApiError>>,
    ) -> HandlerResult {
        let (promise, request) = ask.take();
        if let Err(error) = self.validate_summary_request(&request) {
            let reply = Err(ApiError::from_store_classification_source(error));
            self.reply_api(promise, "request_summary", reply);
            return Handled::OK;
        }

        let correlation_id = Uuid::new_v4();
        let timeout_group_id = request.group_id;
        let timeout_target = request.target.clone();
        let timeout_timer =
            self.schedule_once(self.request_timeout, move |component, expected_timer| {
                component.handle_summary_timeout(correlation_id, &expected_timer)
            });
        self.pending_summaries.insert(
            correlation_id,
            PendingSummaryRequest {
                group_id: timeout_group_id,
                target: timeout_target,
                promise,
                timeout_timer,
            },
        );
        let message = RuntimeMessage::SummaryRequest(SummaryRequestMessage {
            group_id: request.group_id,
            correlation_id,
        });
        self.submit_reliable_runtime_message(request.target, MessageId(correlation_id), &message);
        Handled::OK
    }
}

ignore_lifecycle!(SummaryRequestManagerComponent);

impl Require<ReliableDeliveryPort> for SummaryRequestManagerComponent {
    fn handle(&mut self, indication: ReliableDeliveryPortIndication) -> HandlerResult {
        let ReliableDeliveryPortIndication::Deliver(deliver) = indication;
        self.handle_reliable_delivery(deliver)
    }
}

impl Require<RouteDiscoveryPort<TransportRouteKey>> for SummaryRequestManagerComponent {
    fn handle(&mut self, indication: DiscoveryRouteUpdate<TransportRouteKey>) -> HandlerResult {
        self.handle_route_update(indication)
    }
}

impl Actor for SummaryRequestManagerComponent {
    type Message = SummaryRequestManagerMessage;

    fn receive_local(&mut self, msg: Self::Message) -> HandlerResult {
        match msg {
            SummaryRequestManagerMessage::RequestSummary(ask) => self.handle_request_summary(ask),
        }
    }
}

fn panic_if_fatal_inbound_failure(action: InboundFailureAction, failure: &SummaryInboundFailure) {
    if matches!(action, InboundFailureAction::Fatal) {
        panic!(
            "fatal inbound {} failure: {}",
            failure.context, failure.error
        );
    }
}

fn handled_after_inbound_failure(
    action: InboundFailureAction,
    failure: &SummaryInboundFailure,
) -> HandlerResult {
    panic_if_fatal_inbound_failure(action, failure);
    Handled::OK
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestGroupMemberships;
    use flotsync_core::{membership::GroupMembers, versions::VersionVector};
    use flotsync_io::{
        kompact::shutdown_system_bounded,
        test_support::{WAIT_TIMEOUT, start_component},
    };
    use flotsync_routes::{
        DatagramRouteScope,
        RoutePreferenceRank,
        RouteSharingKind,
        SendRouteCandidate,
        UdpRouteKey,
        test_support::build_delivery_test_system,
    };
    use flotsync_utils::kompact_testing::{
        PortTesterComponent,
        PortTestingExt as _,
        PortTestingRefExt as _,
        eventually_component_state,
    };
    use std::{cell::Cell, collections::HashSet, net::SocketAddr, num::NonZeroUsize};

    const NO_REQUEST_WINDOW: Duration = Duration::from_millis(100);

    fn member(name: &'static str) -> MemberIdentity {
        MemberIdentity::from_array([name])
    }

    fn group_members(members: impl IntoIterator<Item = MemberIdentity>) -> GroupMembers {
        GroupMembers::from_ordered_members(members).expect("test group members should build")
    }

    fn test_route(port: u16) -> SendRouteCandidate<TransportRouteKey> {
        SendRouteCandidate {
            coverage_key: TransportRouteKey::Udp(UdpRouteKey {
                remote_addr: SocketAddr::from(([127, 0, 0, 1], port)),
                scope: DatagramRouteScope::Unicast,
                local_bind: None,
            }),
            sharing: RouteSharingKind::Exclusive,
            preference_rank: RoutePreferenceRank::new(1),
        }
    }

    /// Decoded identity of one submitted summary request.
    #[derive(Clone, Debug, Eq, Hash, PartialEq)]
    struct SubmittedSummaryRequest {
        group_id: GroupId,
        recipient: MemberIdentity,
        correlation_id: Uuid,
        message_id: MessageId,
    }

    /// Component harness exposing route indications and reliable-delivery submissions.
    struct SummaryRequestManagerHarness {
        /// Kompact system owning every harness component.
        system: KompactSystem,
        /// Summary manager whose internal completion state is under test.
        manager: Arc<Component<SummaryRequestManagerComponent>>,
        /// Test provider for reliable-delivery requests.
        reliable_delivery: Arc<Component<PortTesterComponent<ReliableDeliveryPort>>>,
        /// Manager route-discovery port used for direct indication injection.
        route_discovery: RequiredRef<RouteDiscoveryPort<TransportRouteKey>>,
        /// Membership snapshot used to decode submitted runtime messages.
        group_memberships: Arc<dyn SharedGroupMemberships>,
        /// First reliable-delivery request not yet consumed by the test.
        reliable_delivery_cursor: Cell<usize>,
    }

    impl SummaryRequestManagerHarness {
        fn new(
            local_member: MemberIdentity,
            group_memberships: Arc<dyn SharedGroupMemberships>,
        ) -> Self {
            Self::with_request_timeout(local_member, group_memberships, Duration::from_secs(1))
        }

        fn with_request_timeout(
            local_member: MemberIdentity,
            group_memberships: Arc<dyn SharedGroupMemberships>,
            request_timeout: Duration,
        ) -> Self {
            let system = build_delivery_test_system();
            let manager_memberships = group_memberships.clone();
            let manager = system.create(move || {
                SummaryRequestManagerComponent::new(
                    local_member,
                    manager_memberships,
                    request_timeout,
                )
            });
            let reliable_delivery = system.create(ReliableDeliveryPort::tester_component_sidecar);
            let route_discovery =
                manager.on_definition(|component| component.route_discovery_port.share());

            biconnect_components::<ReliableDeliveryPort, _, _>(&reliable_delivery, &manager)
                .expect("summary manager must connect to reliable-delivery probe");

            start_component(&system, &manager);
            start_component(&system, &reliable_delivery);

            Self {
                system,
                manager,
                reliable_delivery,
                route_discovery,
                group_memberships,
                reliable_delivery_cursor: Cell::new(0),
            }
        }

        fn publish_route_update(&self, update: DiscoveryRouteUpdate<TransportRouteKey>) {
            self.system.trigger_i(update, &self.route_discovery);
        }

        fn receive_summary_request(&self) -> SubmittedSummaryRequest {
            let observed = self
                .reliable_delivery
                .actor_ref()
                .observe_request_from(self.reliable_delivery_cursor.get(), |_| true)
                .wait_timeout(WAIT_TIMEOUT)
                .expect("summary request should be submitted")
                .expect("reliable-delivery probe should stay live");
            self.reliable_delivery_cursor.set(observed.index() + 1);
            let ReliableDeliveryPortRequest::Submit(submit) = observed.request();
            let memberships = self.group_memberships.snapshot();
            let decode_context = RuntimeMessageDecodeContext::new(memberships.as_ref());
            let message = RuntimeMessage::decode_proto_view_from_slice_with(
                &submit.envelope.payload.bytes,
                decode_context,
            )
            .expect("submitted summary request should decode");
            let RuntimeMessage::SummaryRequest(message) = message else {
                panic!("expected SummaryRequest runtime message");
            };
            SubmittedSummaryRequest {
                group_id: message.group_id,
                recipient: submit.envelope.header.recipient.clone(),
                correlation_id: message.correlation_id,
                message_id: submit.envelope.header.message_id,
            }
        }

        fn complete_internal_summary(
            &self,
            group_id: GroupId,
            responder: MemberIdentity,
            correlation_id: Uuid,
        ) {
            self.manager.on_definition(|component| {
                component
                    .fulfil_pending_summary(
                        correlation_id,
                        Summary {
                            group_id,
                            responder,
                            has_versions: VersionVector::initial(
                                NonZeroUsize::new(2).expect("test group must have members"),
                            ),
                        },
                    )
                    .expect("internal summary completion should preserve manager invariants");
            });
        }

        fn wait_for_internal_summary_retryable(&self, group_id: GroupId, peer: MemberIdentity) {
            let target = InternalSummaryTarget { group_id, peer };
            eventually_component_state(
                WAIT_TIMEOUT,
                &self.manager,
                |component| {
                    matches!(
                        component.internal_summaries.get1(&target),
                        Some(request) if request.timeout_timer.is_none()
                    )
                },
                "internal summary request should become retryable",
            );
        }

        fn expect_internal_summary_indexed(
            &self,
            group_id: GroupId,
            peer: MemberIdentity,
            operation_id: Uuid,
        ) {
            let target = InternalSummaryTarget { group_id, peer };
            self.manager.on_definition(|component| {
                let by_target = component
                    .internal_summaries
                    .get1(&target)
                    .expect("internal summary should be indexed by target");
                let by_operation = component
                    .internal_summaries
                    .get2(&operation_id)
                    .expect("internal summary should be indexed by operation id");
                assert_eq!(by_target.operation_id, operation_id);
                assert_eq!(by_operation.target, target);
            });
        }

        fn expect_no_summary_request(&self) {
            self.reliable_delivery
                .actor_ref()
                .fail_if_request_observed_from(
                    self.reliable_delivery_cursor.get(),
                    NO_REQUEST_WINDOW,
                    |_| true,
                )
                .wait_timeout(WAIT_TIMEOUT)
                .expect("summary-request absence check should complete")
                .expect("reliable-delivery probe should stay live")
                .expect("summary manager should not submit another request");
        }

        fn shutdown(self) {
            let Self {
                system,
                manager: _,
                reliable_delivery: _,
                route_discovery: _,
                group_memberships: _,
                reliable_delivery_cursor: _,
            } = self;
            shutdown_system_bounded(system, WAIT_TIMEOUT, false);
        }
    }

    #[test]
    fn available_peer_requests_summaries_only_for_shared_hosted_groups() {
        let alice = member("alice");
        let bob = member("bob");
        let carol = member("carol");
        let first_shared_group = GroupId(Uuid::from_u128(1));
        let second_shared_group = GroupId(Uuid::from_u128(2));
        let unshared_group = GroupId(Uuid::from_u128(3));
        let memberships = TestGroupMemberships::from_groups([
            (
                first_shared_group,
                group_members([alice.clone(), bob.clone()]),
            ),
            (
                second_shared_group,
                group_members([bob.clone(), alice.clone()]),
            ),
            (
                unshared_group,
                group_members([alice.clone(), carol.clone()]),
            ),
        ])
        .shared();
        let harness = SummaryRequestManagerHarness::new(alice.clone(), memberships);

        harness.publish_route_update(DiscoveryRouteUpdate::PeerRoutes {
            peer: bob.clone(),
            routes: vec![test_route(10_001)],
        });
        let observed = HashSet::from([
            harness.receive_summary_request(),
            harness.receive_summary_request(),
        ]);
        assert!(
            observed
                .iter()
                .all(|request| request.message_id == MessageId(request.correlation_id))
        );
        let observed_groups = observed
            .iter()
            .map(|request| (request.group_id, request.recipient.clone()))
            .collect::<HashSet<_>>();
        assert_eq!(
            observed_groups,
            HashSet::from([
                (first_shared_group, bob.clone()),
                (second_shared_group, bob),
            ])
        );

        harness.publish_route_update(DiscoveryRouteUpdate::PeerRoutes {
            peer: member("dave"),
            routes: vec![test_route(10_002)],
        });
        harness.publish_route_update(DiscoveryRouteUpdate::PeerRoutes {
            peer: alice,
            routes: vec![test_route(10_003)],
        });
        harness.expect_no_summary_request();
        harness.shutdown();
    }

    #[test]
    fn peer_route_flap_does_not_duplicate_outstanding_summary() {
        let alice = member("alice");
        let bob = member("bob");
        let group_id = GroupId(Uuid::from_u128(4));
        let memberships = TestGroupMemberships::from_groups([(
            group_id,
            group_members([alice.clone(), bob.clone()]),
        )])
        .shared();
        let harness = SummaryRequestManagerHarness::new(alice, memberships);

        harness.publish_route_update(DiscoveryRouteUpdate::PeerRoutes {
            peer: bob.clone(),
            routes: vec![test_route(10_004)],
        });
        let _request = harness.receive_summary_request();

        harness.publish_route_update(DiscoveryRouteUpdate::PeerRoutes {
            peer: bob.clone(),
            routes: vec![test_route(10_005)],
        });
        harness.publish_route_update(DiscoveryRouteUpdate::RelayRoutes {
            relay: member("relay"),
            routes: vec![test_route(10_006)],
        });
        harness.expect_no_summary_request();

        harness.publish_route_update(DiscoveryRouteUpdate::PeerRoutes {
            peer: bob.clone(),
            routes: Vec::new(),
        });
        harness.expect_no_summary_request();
        harness.publish_route_update(DiscoveryRouteUpdate::PeerRoutes {
            peer: bob,
            routes: vec![test_route(10_007)],
        });
        harness.expect_no_summary_request();
        harness.shutdown();
    }

    #[test]
    fn completed_internal_summary_permits_later_reconnect_request() {
        let alice = member("alice");
        let bob = member("bob");
        let group_id = GroupId(Uuid::from_u128(5));
        let memberships = TestGroupMemberships::from_groups([(
            group_id,
            group_members([alice.clone(), bob.clone()]),
        )])
        .shared();
        let harness = SummaryRequestManagerHarness::new(alice, memberships);

        harness.publish_route_update(DiscoveryRouteUpdate::PeerRoutes {
            peer: bob.clone(),
            routes: vec![test_route(10_008)],
        });
        let first = harness.receive_summary_request();
        assert_eq!(first.message_id, MessageId(first.correlation_id));
        harness.expect_internal_summary_indexed(group_id, bob.clone(), first.correlation_id);
        harness.complete_internal_summary(group_id, member("carol"), first.correlation_id);
        harness.expect_internal_summary_indexed(group_id, bob.clone(), first.correlation_id);
        harness.complete_internal_summary(group_id, bob.clone(), first.correlation_id);

        harness.publish_route_update(DiscoveryRouteUpdate::PeerRoutes {
            peer: bob.clone(),
            routes: Vec::new(),
        });
        harness.publish_route_update(DiscoveryRouteUpdate::PeerRoutes {
            peer: bob,
            routes: vec![test_route(10_009)],
        });
        let second = harness.receive_summary_request();

        assert_eq!(second.group_id, group_id);
        assert_ne!(first.correlation_id, second.correlation_id);
        assert_ne!(first.message_id, second.message_id);
        harness.shutdown();
    }

    #[test]
    fn timed_out_internal_summary_retries_with_stable_identity_after_reconnect() {
        let alice = member("alice");
        let bob = member("bob");
        let group_id = GroupId(Uuid::from_u128(6));
        let memberships = TestGroupMemberships::from_groups([(
            group_id,
            group_members([alice.clone(), bob.clone()]),
        )])
        .shared();
        let harness = SummaryRequestManagerHarness::with_request_timeout(
            alice,
            memberships,
            Duration::from_millis(25),
        );

        harness.publish_route_update(DiscoveryRouteUpdate::PeerRoutes {
            peer: bob.clone(),
            routes: vec![test_route(10_010)],
        });
        let first = harness.receive_summary_request();
        assert_eq!(first.message_id, MessageId(first.correlation_id));
        harness.expect_internal_summary_indexed(group_id, bob.clone(), first.correlation_id);
        harness.wait_for_internal_summary_retryable(group_id, bob.clone());

        harness.publish_route_update(DiscoveryRouteUpdate::PeerRoutes {
            peer: bob.clone(),
            routes: Vec::new(),
        });
        harness.publish_route_update(DiscoveryRouteUpdate::PeerRoutes {
            peer: bob,
            routes: vec![test_route(10_011)],
        });
        let second = harness.receive_summary_request();

        assert_eq!(second.group_id, group_id);
        assert_eq!(first.correlation_id, second.correlation_id);
        assert_eq!(first.message_id, second.message_id);
        harness.shutdown();
    }

    #[test]
    fn missing_internal_summary_retry_is_unrecoverable() {
        let harness = SummaryRequestManagerHarness::new(
            member("alice"),
            TestGroupMemberships::default().shared(),
        );
        let missing_target = InternalSummaryTarget {
            group_id: GroupId(Uuid::from_u128(7)),
            peer: member("bob"),
        };

        let handled = harness.manager.on_definition(|component| {
            component.retry_internal_summary_request(&missing_target, Uuid::from_u128(8))
        });

        assert!(matches!(handled, Err(HandlerError::Unrecoverable(_))));
        harness.shutdown();
    }
}
