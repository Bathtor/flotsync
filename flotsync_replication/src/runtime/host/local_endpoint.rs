//! Local UDP endpoint binding and endpoint-selection publication.

use super::{
    BindLocalEndpointSnafu,
    ControlFutureSnafu,
    RuntimeControlError,
    RuntimeHostError,
    config_keys,
};
use flotsync_discovery::endpoint_selection::{
    EndpointSelection,
    EndpointSelectionPolicy,
    EndpointSelectionPort,
    InterfaceSnapshot,
    InterfaceSnapshotProvider,
    PnetInterfaceSnapshotProvider,
};
use flotsync_io::prelude::{
    SocketId,
    UdpBindOptions,
    UdpIndication,
    UdpLocalBind,
    UdpOpenRequestId,
    UdpPort,
    UdpRequest,
};
use flotsync_routes::{
    RouteEndpointBinding,
    RouteEndpointLifecycle,
    RouteEndpointLifecyclePort,
    RouteEndpointUnavailableReason,
};
use flotsync_utils::{
    FutureTimeoutExt as _,
    kompact_fsm::{State, StateUpdate},
    transform_state_match,
};
use futures_util::FutureExt as _;
use kompact::prelude::*;
use snafu::ResultExt;
use std::{error::Error as StdError, net::SocketAddr, sync::Arc, time::Duration};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct LocalEndpointBinding {
    pub(super) socket_id: SocketId,
    pub(super) local_addr: SocketAddr,
}

#[derive(Debug)]
pub(super) enum LocalEndpointManagerMessage {
    /// Ensure the configured UDP endpoint is bound, returning the current binding.
    ///
    /// On a successful new bind, the manager also publishes the selected discovery endpoints and
    /// starts wildcard endpoint-selection polling before completing the bind transition.
    EnsureBound(Ask<(), Result<LocalEndpointBinding, RuntimeControlError>>),
}

/// Single local delivery-endpoint owner for the current runtime slice.
///
/// This is a deliberate stub until `flotsync-665` lands. It owns one
/// configured UDP bind, records the concrete local address assigned by the
/// system, and is the place where later rebinding/network-change logic should
/// grow rather than scattering bind handling across the host.
#[derive(ComponentDefinition)]
pub(super) struct LocalEndpointManager {
    /// Kompact component context.
    ctx: ComponentContext<Self>,
    /// UDP transport port used to request the runtime endpoint bind.
    udp_port: RequiredPort<UdpPort>,
    /// Output port for route endpoint ownership lifecycle events.
    route_endpoint_lifecycle_port: ProvidedPort<RouteEndpointLifecyclePort>,
    /// Output port for concrete local endpoints selected for discovery advertisements.
    endpoint_selection_port: ProvidedPort<EndpointSelectionPort>,
    /// Configured bind address requested from the UDP transport.
    configured_bind_addr: SocketAddr,
    /// Policy that maps endpoint binds and interface snapshots to selected endpoints.
    endpoint_selection_policy: EndpointSelectionPolicy,
    /// Source of local interface snapshots for wildcard endpoint-selection refreshes.
    interface_snapshot_provider: Arc<dyn InterfaceSnapshotProvider + Send + Sync>,
    /// Poll interval for wildcard bind endpoint selection.
    endpoint_selection_refresh_interval: Duration,
    /// Currently scheduled wildcard endpoint-selection refresh, if any.
    endpoint_selection_timer: Option<ScheduledTimer>,
    /// Last endpoint selection, used to suppress unchanged publications.
    last_endpoint_selection: Option<EndpointSelection>,
    /// Current endpoint bind lifecycle state.
    state: State<LocalEndpointManagerState>,
}

impl LocalEndpointManager {
    pub(super) fn new(configured_bind_addr: SocketAddr) -> Self {
        Self::with_interface_snapshot_provider(
            configured_bind_addr,
            Arc::new(PnetInterfaceSnapshotProvider),
        )
    }

    /// Build a manager with an explicit interface provider for deterministic tests.
    fn with_interface_snapshot_provider(
        configured_bind_addr: SocketAddr,
        interface_snapshot_provider: Arc<dyn InterfaceSnapshotProvider + Send + Sync>,
    ) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            udp_port: RequiredPort::uninitialised(),
            route_endpoint_lifecycle_port: ProvidedPort::uninitialised(),
            endpoint_selection_port: ProvidedPort::uninitialised(),
            configured_bind_addr,
            endpoint_selection_policy: EndpointSelectionPolicy::default(),
            interface_snapshot_provider,
            endpoint_selection_refresh_interval: Duration::ZERO,
            endpoint_selection_timer: None,
            last_endpoint_selection: None,
            state: State::new(LocalEndpointManagerState::Unbound),
        }
    }

    /// Return the UDP port reference used by tests to inject driver indications.
    #[cfg(test)]
    fn udp_port(&mut self) -> RequiredRef<UdpPort> {
        self.udp_port.share()
    }

    /// Refresh selected endpoints for a bound endpoint and schedule future wildcard polls.
    fn begin_endpoint_selection_updates(&mut self, local_addr: SocketAddr) {
        self.clear_endpoint_selection_timer();
        self.refresh_endpoint_selection(local_addr);
        if local_addr.ip().is_unspecified() {
            self.schedule_endpoint_selection_refresh();
        }
    }

    /// Publish that this runtime endpoint socket is authorised for route traffic.
    fn publish_route_endpoint_available(&mut self, binding: LocalEndpointBinding) {
        self.route_endpoint_lifecycle_port
            .trigger(RouteEndpointLifecycle::Available(RouteEndpointBinding {
                socket_id: binding.socket_id,
                socket_bound_addr: binding.local_addr,
            }));
    }

    /// Publish that this runtime endpoint socket is no longer authorised for route traffic.
    fn publish_route_endpoint_unavailable(
        &mut self,
        binding: LocalEndpointBinding,
        reason: RouteEndpointUnavailableReason,
    ) {
        self.route_endpoint_lifecycle_port
            .trigger(RouteEndpointLifecycle::Unavailable {
                binding: RouteEndpointBinding {
                    socket_id: binding.socket_id,
                    socket_bound_addr: binding.local_addr,
                },
                reason,
            });
    }

    /// Return the current endpoint binding if the manager is bound.
    fn current_binding(&self) -> Option<LocalEndpointBinding> {
        match self.state.get() {
            LocalEndpointManagerState::Bound(binding) => Some(*binding),
            LocalEndpointManagerState::Unbound | LocalEndpointManagerState::Binding { .. } => None,
        }
    }

    /// Load endpoint-selection refresh timing from Kompact config.
    fn load_endpoint_selection_refresh_interval(&self) -> Result<Duration, RuntimeHostError> {
        self.ctx
            .config()
            .read_or_default(&config_keys::LOCAL_ENDPOINT_SELECTION_REFRESH_INTERVAL)
            .map_err(|error| RuntimeHostError::InvalidConfig {
                key: config_keys::LOCAL_ENDPOINT_SELECTION_REFRESH_INTERVAL.key,
                message: error.to_string(),
            })
    }

    /// Recompute and publish endpoint-selection changes for one bound local endpoint.
    fn refresh_endpoint_selection(&mut self, local_addr: SocketAddr) {
        let snapshot = if local_addr.ip().is_unspecified() {
            self.interface_snapshot_provider.snapshot()
        } else {
            InterfaceSnapshot::default()
        };
        let selection = self
            .endpoint_selection_policy
            .select_endpoints(local_addr, &snapshot);
        let Some(previous_selection) = &self.last_endpoint_selection else {
            self.log_selected_endpoints(local_addr, &selection);
            self.last_endpoint_selection = Some(selection.clone());
            self.endpoint_selection_port.trigger(selection);
            return;
        };
        if selection == *previous_selection {
            trace!(
                self.log(),
                "local endpoint selection unchanged for local endpoint {}: {:?}",
                local_addr,
                selection.endpoints
            );
            return;
        }
        debug!(
            self.log(),
            "local endpoint selection changed for local endpoint {} from {:?} to {:?}",
            local_addr,
            previous_selection.endpoints,
            selection.endpoints
        );
        self.last_endpoint_selection = Some(selection.clone());
        self.endpoint_selection_port.trigger(selection);
    }

    /// Log the first endpoint selection for a bound endpoint.
    fn log_selected_endpoints(&self, local_addr: SocketAddr, selection: &EndpointSelection) {
        if selection.is_empty() {
            debug!(
                self.log(),
                "local endpoint {} has no selected discovery endpoints", local_addr
            );
        } else {
            debug!(
                self.log(),
                "local endpoint {} selected discovery endpoints {:?}",
                local_addr,
                selection.endpoints
            );
        }
    }

    /// Set periodic endpoint-selection polling for wildcard endpoint binds.
    fn schedule_endpoint_selection_refresh(&mut self) {
        let timer = self.schedule_periodic(
            self.endpoint_selection_refresh_interval,
            self.endpoint_selection_refresh_interval,
            move |component, timeout| component.handle_endpoint_selection_refresh_timeout(&timeout),
        );
        self.endpoint_selection_timer = Some(timer);
    }

    /// Cancel any pending endpoint-selection polling timer.
    fn clear_endpoint_selection_timer(&mut self) {
        if let Some(timer) = self.endpoint_selection_timer.take() {
            self.cancel_timer(timer);
        }
    }

    /// Refresh wildcard-selected discovery endpoints if the expected timer fired.
    fn handle_endpoint_selection_refresh_timeout(
        &mut self,
        actual_timer: &ScheduledTimer,
    ) -> HandlerResult {
        let Some(expected_timer) = self.endpoint_selection_timer.as_ref() else {
            return Handled::OK;
        };
        if expected_timer != actual_timer {
            return Handled::OK;
        }
        let Some(binding) = self.current_binding() else {
            self.clear_endpoint_selection_timer();
            self.publish_empty_endpoint_selection();
            return Handled::OK;
        };
        self.refresh_endpoint_selection(binding.local_addr);
        Handled::OK
    }

    /// Publish an empty endpoint selection if the last published selection was non-empty.
    fn publish_empty_endpoint_selection(&mut self) {
        if self
            .last_endpoint_selection
            .as_ref()
            .is_some_and(EndpointSelection::is_empty)
        {
            return;
        }
        debug!(
            self.log(),
            "local endpoint is not bound; clearing selected discovery endpoints"
        );
        self.last_endpoint_selection = Some(EndpointSelection::default());
        self.endpoint_selection_port
            .trigger(EndpointSelection::default());
    }
}

impl ComponentLifecycle for LocalEndpointManager {
    fn on_start(&mut self) -> HandlerResult {
        self.endpoint_selection_refresh_interval = self
            .load_endpoint_selection_refresh_interval()
            .map_err(HandlerError::unrecoverable)?;
        Handled::OK
    }

    fn on_stop(&mut self) -> HandlerResult {
        self.clear_endpoint_selection_timer();
        Handled::OK
    }

    fn on_kill(&mut self) -> HandlerResult {
        self.clear_endpoint_selection_timer();
        Handled::OK
    }
}

ignore_requests!(EndpointSelectionPort, LocalEndpointManager);
ignore_requests!(RouteEndpointLifecyclePort, LocalEndpointManager);

impl Require<UdpPort> for LocalEndpointManager {
    fn handle(&mut self, indication: UdpIndication) -> HandlerResult {
        transform_state_match!(self, state, {
            LocalEndpointManagerState::Binding {
                request_id: current_request_id,
                promise,
            } => match indication {
                UdpIndication::Bound {
                    request_id,
                    socket_id,
                    local_addr,
                } if current_request_id == request_id => {
                    let binding = LocalEndpointBinding {
                        socket_id,
                        local_addr,
                    };
                    self.begin_endpoint_selection_updates(binding.local_addr);
                    self.publish_route_endpoint_available(binding);
                    let _ = promise.fulfil(Ok(binding));
                    StateUpdate::transition(LocalEndpointManagerState::Bound(binding))
                }
                UdpIndication::BindFailed {
                    request_id,
                    local_addr,
                    reason,
                } if current_request_id == request_id => {
                    let _ = promise.fulfil(Err(RuntimeControlError::failed(format!(
                        "bind at {local_addr} failed: {reason:?}"
                    ))));
                    StateUpdate::transition(LocalEndpointManagerState::Unbound)
                }
                _ => StateUpdate::ok(LocalEndpointManagerState::Binding {
                    request_id: current_request_id,
                    promise,
                }),
            },
            LocalEndpointManagerState::Bound(binding) => match indication {
                UdpIndication::Closed {
                    socket_id, reason, ..
                } if binding.socket_id == socket_id => {
                    self.publish_route_endpoint_unavailable(
                        binding,
                        RouteEndpointUnavailableReason::Closed { reason },
                    );
                    self.clear_endpoint_selection_timer();
                    self.publish_empty_endpoint_selection();
                    StateUpdate::transition(LocalEndpointManagerState::Unbound)
                }
                _ => StateUpdate::ok(LocalEndpointManagerState::Bound(binding)),
            },
            state => StateUpdate::ok(state),
        })
    }
}

impl Actor for LocalEndpointManager {
    type Message = LocalEndpointManagerMessage;

    fn receive_local(&mut self, msg: Self::Message) -> HandlerResult {
        match msg {
            LocalEndpointManagerMessage::EnsureBound(ask) => {
                let (promise, ()) = ask.take();
                transform_state_match!(self, state, {
                    LocalEndpointManagerState::Unbound => {
                        let request_id = UdpOpenRequestId::new();
                        self.udp_port.trigger(UdpRequest::Bind {
                            request_id,
                            bind: UdpLocalBind::Exact(self.configured_bind_addr),
                            options: UdpBindOptions::default(),
                        });
                        StateUpdate::transition(LocalEndpointManagerState::Binding {
                            request_id,
                            promise,
                        })
                    }
                    LocalEndpointManagerState::Binding {
                        request_id,
                        promise: pending_promise,
                    } => {
                        let _ = promise.fulfil(Err(RuntimeControlError::failed(
                            "local endpoint bind already in progress",
                        )));
                        StateUpdate::ok(LocalEndpointManagerState::Binding {
                            request_id,
                            promise: pending_promise,
                        })
                    }
                    LocalEndpointManagerState::Bound(binding) => {
                        let _ = promise.fulfil(Ok(binding));
                        StateUpdate::ok(LocalEndpointManagerState::Bound(binding))
                    }
                })
            }
        }
    }
}

/// Local endpoint bind lifecycle owned by [`LocalEndpointManager`].
enum LocalEndpointManagerState {
    /// No endpoint bind is active or in progress.
    Unbound,
    /// A UDP bind request is in flight and will fulfil `promise`.
    Binding {
        request_id: UdpOpenRequestId,
        promise: KPromise<Result<LocalEndpointBinding, RuntimeControlError>>,
    },
    /// The endpoint is bound and ready for runtime traffic.
    Bound(LocalEndpointBinding),
}

/// Ask the local endpoint manager to bind its configured UDP endpoint.
///
/// The caller must pass a live component whose actor ref can still be upgraded
/// to a strong ref. `control_timeout` bounds the whole ask/response exchange,
/// including the UDP bind indication that fulfils the manager promise.
pub(super) async fn ensure_local_endpoint_bound(
    local_endpoint_manager: &Arc<Component<LocalEndpointManager>>,
    control_timeout: Duration,
) -> Result<LocalEndpointBinding, RuntimeHostError> {
    let local_endpoint_ref = local_endpoint_manager
        .actor_ref()
        .hold()
        .expect("local endpoint manager must expose a strong actor ref");
    let future = local_endpoint_ref
        .ask_with(|promise| LocalEndpointManagerMessage::EnsureBound(Ask::new(promise, ())));
    future
        .map(flatten_local_endpoint_ask_result)
        .timeout_fold_err(control_timeout)
        .await
        .context(BindLocalEndpointSnafu)
}

fn flatten_local_endpoint_ask_result<E>(
    result: Result<Result<LocalEndpointBinding, RuntimeControlError>, E>,
) -> Result<LocalEndpointBinding, RuntimeControlError>
where
    E: StdError + Send + Sync + 'static,
{
    result.boxed().context(ControlFutureSnafu)?
}

#[cfg(test)]
mod tests {
    use super::*;
    use flotsync_discovery::endpoint_selection::{
        EndpointSelection,
        EndpointSelectionPort,
        InterfaceAddress,
        InterfaceFlags,
        InterfaceSnapshotEntry,
    };
    use flotsync_io::test_support::{
        build_test_kompact_system,
        build_test_kompact_system_with,
        eventually_component_state,
        kill_component,
        start_component,
    };
    use flotsync_utils::kompact_testing::{PortTestingExt, PortTestingRefExt};
    use std::{
        net::{IpAddr, Ipv4Addr},
        sync::{Arc, Mutex, mpsc},
    };

    /// Event observed by the cross-port endpoint ordering probe.
    #[derive(Debug, PartialEq, Eq)]
    enum EndpointOrderingEvent {
        /// Endpoint-selection update from the local endpoint manager.
        Selection(EndpointSelection),
        /// Route-endpoint lifecycle update from the local endpoint manager.
        Lifecycle(RouteEndpointLifecycle),
    }

    /// Test probe that records endpoint-selection and lifecycle events into one ordered stream.
    #[derive(ComponentDefinition)]
    struct EndpointOrderingProbeComponent {
        /// Kompact component context.
        ctx: ComponentContext<Self>,
        /// Required endpoint-selection port under test.
        endpoint_selection_port: RequiredPort<EndpointSelectionPort>,
        /// Required route-endpoint lifecycle port under test.
        route_endpoint_lifecycle_port: RequiredPort<RouteEndpointLifecyclePort>,
        /// Ordered event sink shared with the test thread.
        events: mpsc::Sender<EndpointOrderingEvent>,
    }

    impl EndpointOrderingProbeComponent {
        /// Build one ordering probe.
        fn new(events: mpsc::Sender<EndpointOrderingEvent>) -> Self {
            Self {
                ctx: ComponentContext::uninitialised(),
                endpoint_selection_port: RequiredPort::uninitialised(),
                route_endpoint_lifecycle_port: RequiredPort::uninitialised(),
                events,
            }
        }
    }

    ignore_lifecycle!(EndpointOrderingProbeComponent);

    impl Require<EndpointSelectionPort> for EndpointOrderingProbeComponent {
        fn handle(&mut self, indication: EndpointSelection) -> HandlerResult {
            self.events
                .send(EndpointOrderingEvent::Selection(indication))
                .expect("endpoint ordering receiver should stay live");
            Handled::OK
        }
    }

    impl Require<RouteEndpointLifecyclePort> for EndpointOrderingProbeComponent {
        fn handle(&mut self, indication: RouteEndpointLifecycle) -> HandlerResult {
            self.events
                .send(EndpointOrderingEvent::Lifecycle(indication))
                .expect("endpoint ordering receiver should stay live");
            Handled::OK
        }
    }

    impl Actor for EndpointOrderingProbeComponent {
        type Message = Never;

        fn receive_local(&mut self, _msg: Self::Message) -> HandlerResult {
            unreachable!("Never message type cannot be instantiated")
        }
    }

    /// Test interface provider whose snapshot can be replaced between refreshes.
    #[derive(Debug)]
    struct MutableInterfaceSnapshotProvider {
        snapshot: Mutex<InterfaceSnapshot>,
    }

    impl MutableInterfaceSnapshotProvider {
        fn new(snapshot: InterfaceSnapshot) -> Self {
            Self {
                snapshot: Mutex::new(snapshot),
            }
        }

        fn replace(&self, snapshot: InterfaceSnapshot) {
            *self
                .snapshot
                .lock()
                .expect("snapshot lock should not poison") = snapshot;
        }
    }

    impl InterfaceSnapshotProvider for MutableInterfaceSnapshotProvider {
        fn snapshot(&self) -> InterfaceSnapshot {
            self.snapshot
                .lock()
                .expect("snapshot lock should not poison")
                .clone()
        }
    }

    #[test]
    fn endpoint_manager_publishes_selected_routes_for_wildcard_bound_endpoint() {
        let provider = Arc::new(MutableInterfaceSnapshotProvider::new(
            snapshot_with_lan_addr(Ipv4Addr::new(192, 168, 1, 20)),
        ));
        let system = build_test_kompact_system();
        let manager_provider = provider.clone();
        let manager = system.create(move || {
            LocalEndpointManager::with_interface_snapshot_provider(
                SocketAddr::from(([0, 0, 0, 0], 0)),
                manager_provider,
            )
        });
        let probe = system.create(EndpointSelectionPort::tester_component_sidecar);
        let probe_ref = probe.actor_ref();
        biconnect_components::<EndpointSelectionPort, _, _>(&manager, &probe)
            .expect("connect endpoint selection probe");

        start_component(&system, &probe);
        start_component(&system, &manager);

        let update_future = probe_ref.observe_indication(|_| true);
        manager.on_definition(|manager| {
            let local_addr = SocketAddr::from(([0, 0, 0, 0], 45_100));
            manager
                .state
                .set(LocalEndpointManagerState::Bound(LocalEndpointBinding {
                    socket_id: SocketId(5),
                    local_addr,
                }));
            manager.begin_endpoint_selection_updates(local_addr);
        });

        let update = update_future
            .wait_timeout(Duration::from_secs(1))
            .expect("endpoint selection update should arrive")
            .expect("endpoint selection probe should stay live");
        assert_eq!(
            update.indication().endpoints,
            EndpointSelection::from_endpoints([SocketAddr::from(([192, 168, 1, 20], 45_100))])
                .endpoints
        );

        kill_component(&system, manager);
        kill_component(&system, probe);
        system.shutdown().wait().expect("Kompact shutdown");
    }

    #[test]
    fn endpoint_manager_publishes_empty_routes_when_wildcard_routes_disappear() {
        let provider = Arc::new(MutableInterfaceSnapshotProvider::new(
            snapshot_with_lan_addr(Ipv4Addr::new(192, 168, 1, 20)),
        ));
        let system = build_test_kompact_system();
        let manager_provider = provider.clone();
        let manager = system.create(move || {
            LocalEndpointManager::with_interface_snapshot_provider(
                SocketAddr::from(([0, 0, 0, 0], 0)),
                manager_provider,
            )
        });
        let probe = system.create(EndpointSelectionPort::tester_component_sidecar);
        let probe_ref = probe.actor_ref();
        biconnect_components::<EndpointSelectionPort, _, _>(&manager, &probe)
            .expect("connect endpoint selection probe");

        start_component(&system, &probe);
        start_component(&system, &manager);

        let initial_update_future = probe_ref.observe_indication(|_| true);
        manager.on_definition(|manager| {
            let local_addr = SocketAddr::from(([0, 0, 0, 0], 45_101));
            manager
                .state
                .set(LocalEndpointManagerState::Bound(LocalEndpointBinding {
                    socket_id: SocketId(6),
                    local_addr,
                }));
            manager.begin_endpoint_selection_updates(local_addr);
        });
        let initial_update = initial_update_future
            .wait_timeout(Duration::from_secs(1))
            .expect("initial endpoint selection update should arrive")
            .expect("endpoint selection probe should stay live");

        provider.replace(InterfaceSnapshot::default());
        let empty_update_future = probe_ref
            .observe_indication_from(initial_update.index() + 1, |update| {
                update.endpoints.is_empty()
            });
        manager.on_definition(|manager| {
            manager.begin_endpoint_selection_updates(SocketAddr::from(([0, 0, 0, 0], 45_101)));
        });
        empty_update_future
            .wait_timeout(Duration::from_secs(1))
            .expect("empty endpoint selection update should arrive")
            .expect("endpoint selection probe should stay live");

        kill_component(&system, manager);
        kill_component(&system, probe);
        system.shutdown().wait().expect("Kompact shutdown");
    }

    #[test]
    fn endpoint_manager_loads_endpoint_selection_refresh_interval_from_config() {
        let configured_interval = Duration::from_millis(25);
        let system = build_test_kompact_system_with(|config| {
            config.set_config_value(
                &config_keys::LOCAL_ENDPOINT_SELECTION_REFRESH_INTERVAL,
                configured_interval,
            );
        });
        let manager = system.create(|| {
            LocalEndpointManager::with_interface_snapshot_provider(
                SocketAddr::from(([127, 0, 0, 1], 0)),
                Arc::new(MutableInterfaceSnapshotProvider::new(
                    InterfaceSnapshot::default(),
                )),
            )
        });

        start_component(&system, &manager);

        manager.on_definition(|manager| {
            assert_eq!(
                manager.endpoint_selection_refresh_interval,
                configured_interval
            );
        });

        kill_component(&system, manager);
        system.shutdown().wait().expect("Kompact shutdown");
    }

    #[test]
    fn endpoint_manager_publishes_selection_before_endpoint_availability() {
        let system = build_test_kompact_system();
        let manager = system.create(|| {
            LocalEndpointManager::with_interface_snapshot_provider(
                SocketAddr::from(([127, 0, 0, 1], 0)),
                Arc::new(MutableInterfaceSnapshotProvider::new(
                    InterfaceSnapshot::default(),
                )),
            )
        });
        let (events_tx, events_rx) = mpsc::channel();
        let ordering_probe = system.create(move || EndpointOrderingProbeComponent::new(events_tx));
        biconnect_components::<EndpointSelectionPort, _, _>(&manager, &ordering_probe)
            .expect("connect endpoint selection ordering probe");
        biconnect_components::<RouteEndpointLifecyclePort, _, _>(&manager, &ordering_probe)
            .expect("connect route endpoint lifecycle ordering probe");

        start_component(&system, &ordering_probe);
        start_component(&system, &manager);

        let bind_future = manager
            .actor_ref()
            .ask_with(|promise| LocalEndpointManagerMessage::EnsureBound(Ask::new(promise, ())));
        eventually_component_state(
            Duration::from_secs(1),
            &manager,
            |manager| {
                matches!(
                    manager.state.get(),
                    LocalEndpointManagerState::Binding { .. }
                )
            },
            "endpoint manager should start binding",
        );
        let request_id = manager.on_definition(|manager| match manager.state.get() {
            LocalEndpointManagerState::Binding { request_id, .. } => *request_id,
            LocalEndpointManagerState::Unbound | LocalEndpointManagerState::Bound(_) => {
                panic!("endpoint manager should be binding")
            }
        });
        let udp_port = manager.on_definition(LocalEndpointManager::udp_port);
        let socket_id = SocketId(12);
        let local_addr = SocketAddr::from(([127, 0, 0, 1], 45_112));
        system.trigger_i(
            UdpIndication::Bound {
                request_id,
                socket_id,
                local_addr,
            },
            &udp_port,
        );
        bind_future
            .wait_timeout(Duration::from_secs(1))
            .expect("endpoint bind ask should complete")
            .expect("endpoint bind should succeed");

        let first_event = events_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("endpoint selection event should arrive first");
        let second_event = events_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("endpoint lifecycle event should arrive second");
        assert_eq!(
            first_event,
            EndpointOrderingEvent::Selection(EndpointSelection::from_endpoints([local_addr]))
        );
        assert_eq!(
            second_event,
            EndpointOrderingEvent::Lifecycle(RouteEndpointLifecycle::Available(
                RouteEndpointBinding {
                    socket_id,
                    socket_bound_addr: local_addr,
                },
            ))
        );

        kill_component(&system, manager);
        kill_component(&system, ordering_probe);
        system.shutdown().wait().expect("Kompact shutdown");
    }

    #[test]
    fn endpoint_manager_publishes_route_endpoint_lifecycle_for_active_binding() {
        let system = build_test_kompact_system();
        let manager = system.create(|| {
            LocalEndpointManager::with_interface_snapshot_provider(
                SocketAddr::from(([127, 0, 0, 1], 0)),
                Arc::new(MutableInterfaceSnapshotProvider::new(
                    InterfaceSnapshot::default(),
                )),
            )
        });
        let lifecycle_probe = system.create(RouteEndpointLifecyclePort::tester_component_sidecar);
        let lifecycle_probe_ref = lifecycle_probe.actor_ref();
        biconnect_components::<RouteEndpointLifecyclePort, _, _>(&manager, &lifecycle_probe)
            .expect("connect route endpoint lifecycle probe");

        start_component(&system, &lifecycle_probe);
        start_component(&system, &manager);

        let bind_future = manager
            .actor_ref()
            .ask_with(|promise| LocalEndpointManagerMessage::EnsureBound(Ask::new(promise, ())));
        eventually_component_state(
            Duration::from_secs(1),
            &manager,
            |manager| {
                matches!(
                    manager.state.get(),
                    LocalEndpointManagerState::Binding { .. }
                )
            },
            "endpoint manager should start binding",
        );
        let request_id = manager.on_definition(|manager| match manager.state.get() {
            LocalEndpointManagerState::Binding { request_id, .. } => *request_id,
            LocalEndpointManagerState::Unbound | LocalEndpointManagerState::Bound(_) => {
                panic!("endpoint manager should be binding")
            }
        });
        let udp_port = manager.on_definition(LocalEndpointManager::udp_port);
        let socket_id = SocketId(11);
        let local_addr = SocketAddr::from(([127, 0, 0, 1], 45_111));
        let available_future = lifecycle_probe_ref.observe_indication(move |indication| {
            *indication
                == RouteEndpointLifecycle::Available(RouteEndpointBinding {
                    socket_id,
                    socket_bound_addr: local_addr,
                })
        });
        system.trigger_i(
            UdpIndication::Bound {
                request_id,
                socket_id,
                local_addr,
            },
            &udp_port,
        );
        bind_future
            .wait_timeout(Duration::from_secs(1))
            .expect("endpoint bind ask should complete")
            .expect("endpoint bind should succeed");
        let available = available_future
            .wait_timeout(Duration::from_secs(1))
            .expect("available lifecycle should arrive")
            .expect("lifecycle probe should stay live");

        let unavailable_future =
            lifecycle_probe_ref.observe_indication_from(available.index() + 1, move |indication| {
                *indication
                    == RouteEndpointLifecycle::Unavailable {
                        binding: RouteEndpointBinding {
                            socket_id,
                            socket_bound_addr: local_addr,
                        },
                        reason: RouteEndpointUnavailableReason::Closed {
                            reason: flotsync_io::prelude::UdpCloseReason::Requested,
                        },
                    }
            });
        system.trigger_i(
            UdpIndication::Closed {
                socket_id,
                remote_addr: None,
                reason: flotsync_io::prelude::UdpCloseReason::Requested,
            },
            &udp_port,
        );
        unavailable_future
            .wait_timeout(Duration::from_secs(1))
            .expect("unavailable lifecycle should arrive")
            .expect("lifecycle probe should stay live");

        kill_component(&system, manager);
        kill_component(&system, lifecycle_probe);
        system.shutdown().wait().expect("Kompact shutdown");
    }

    fn snapshot_with_lan_addr(address: Ipv4Addr) -> InterfaceSnapshot {
        InterfaceSnapshot::new([InterfaceSnapshotEntry::new(
            "en0",
            InterfaceFlags::UP | InterfaceFlags::RUNNING,
            [InterfaceAddress::new(IpAddr::V4(address), 24)],
        )])
    }
}
