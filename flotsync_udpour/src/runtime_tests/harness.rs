//! Runtime-system harness used by `UDPour` scenarios.

use super::{
    frames::{bind_socket, build_runtime_test_kompact_system},
    support::*,
    *,
};

pub(in crate::runtime_tests) struct RuntimeHarness {
    system: KompactSystem,
    manual_timer: Option<KompactManualTimer>,
    socket_lease: ReservedSocketLease,
    driver: Arc<Component<IoDriverComponent>>,
    bridge: Arc<Component<IoBridge>>,
    observer: Arc<Component<UdpObserver>>,
    observer_ref: ActorRefStrong<UdpObserverMessage>,
    sender_proxy: Arc<Component<ScriptedUdpProxy>>,
    receiver_proxy: Arc<Component<ScriptedUdpProxy>>,
    sender_runtime: Arc<Component<UDPourComponent>>,
    receiver_runtime: Arc<Component<UDPourComponent>>,
    sender_runtime_ref: ActorRefStrong<UDPourComponentMessage>,
    sender_probe: Arc<Component<TransferProbe>>,
    receiver_probe: Arc<Component<TransferProbe>>,
    receiver_probe_ref: ActorRefStrong<TransferProbeMessage>,
    _bridge_to_observer: TwoWayChannel<UdpPort, IoBridge, UdpObserver>,
    _bridge_to_sender_proxy: TwoWayChannel<UdpPort, IoBridge, ScriptedUdpProxy>,
    _bridge_to_receiver_proxy: TwoWayChannel<UdpPort, IoBridge, ScriptedUdpProxy>,
    _sender_proxy_to_runtime: TwoWayChannel<UdpPort, ScriptedUdpProxy, UDPourComponent>,
    _receiver_proxy_to_runtime: TwoWayChannel<UdpPort, ScriptedUdpProxy, UDPourComponent>,
    _sender_runtime_to_probe: TwoWayChannel<UDPourPort, UDPourComponent, TransferProbe>,
    _receiver_runtime_to_probe: TwoWayChannel<UDPourPort, UDPourComponent, TransferProbe>,
    observer_rx: BufferedReceiver<UdpIndication>,
    receiver_rx: BufferedReceiver<UDPourDeliver>,
    pub(in crate::runtime_tests) sender_socket_id: SocketId,
    pub(in crate::runtime_tests) sender_addr: SocketAddr,
    pub(in crate::runtime_tests) receiver_socket_id: SocketId,
    pub(in crate::runtime_tests) receiver_addr: SocketAddr,
}

pub(in crate::runtime_tests) struct RuntimeHarnessBuildConfig {
    sender_request_behavior: ProxyRequestBehavior,
    sender_indication_behavior: ProxyIndicationBehavior,
    receiver_request_behavior: ProxyRequestBehavior,
    receiver_indication_behavior: ProxyIndicationBehavior,
    sender_config: SenderConfig,
    receiver_config: ReceiverConfig,
    send_rate_control: TestSendRateControl,
    manual_time: bool,
}

impl RuntimeHarness {
    pub(in crate::runtime_tests) fn new(
        sender_request_behavior: ProxyRequestBehavior,
        sender_indication_behavior: ProxyIndicationBehavior,
        receiver_request_behavior: ProxyRequestBehavior,
        receiver_indication_behavior: ProxyIndicationBehavior,
        sender_config: SenderConfig,
        receiver_config: ReceiverConfig,
    ) -> Self {
        Self::with_send_rate_control(
            sender_request_behavior,
            sender_indication_behavior,
            receiver_request_behavior,
            receiver_indication_behavior,
            sender_config,
            receiver_config,
            TestSendRateControl::default(),
        )
    }

    pub(in crate::runtime_tests) fn new_manual_time(
        sender_request_behavior: ProxyRequestBehavior,
        sender_indication_behavior: ProxyIndicationBehavior,
        receiver_request_behavior: ProxyRequestBehavior,
        receiver_indication_behavior: ProxyIndicationBehavior,
        sender_config: SenderConfig,
        receiver_config: ReceiverConfig,
    ) -> Self {
        Self::with_send_rate_control_manual_time(
            sender_request_behavior,
            sender_indication_behavior,
            receiver_request_behavior,
            receiver_indication_behavior,
            sender_config,
            receiver_config,
            TestSendRateControl::default(),
        )
    }

    pub(in crate::runtime_tests) fn with_send_rate_control(
        sender_request_behavior: ProxyRequestBehavior,
        sender_indication_behavior: ProxyIndicationBehavior,
        receiver_request_behavior: ProxyRequestBehavior,
        receiver_indication_behavior: ProxyIndicationBehavior,
        sender_config: SenderConfig,
        receiver_config: ReceiverConfig,
        send_rate_control: TestSendRateControl,
    ) -> Self {
        Self::build(RuntimeHarnessBuildConfig {
            sender_request_behavior,
            sender_indication_behavior,
            receiver_request_behavior,
            receiver_indication_behavior,
            sender_config,
            receiver_config,
            send_rate_control,
            manual_time: false,
        })
    }

    pub(in crate::runtime_tests) fn with_send_rate_control_manual_time(
        sender_request_behavior: ProxyRequestBehavior,
        sender_indication_behavior: ProxyIndicationBehavior,
        receiver_request_behavior: ProxyRequestBehavior,
        receiver_indication_behavior: ProxyIndicationBehavior,
        sender_config: SenderConfig,
        receiver_config: ReceiverConfig,
        send_rate_control: TestSendRateControl,
    ) -> Self {
        Self::build(RuntimeHarnessBuildConfig {
            sender_request_behavior,
            sender_indication_behavior,
            receiver_request_behavior,
            receiver_indication_behavior,
            sender_config,
            receiver_config,
            send_rate_control,
            manual_time: true,
        })
    }

    #[allow(
        clippy::too_many_lines,
        reason = "test harness construction wires several Kompact components that are clearer together"
    )]
    pub(in crate::runtime_tests) fn build(config: RuntimeHarnessBuildConfig) -> Self {
        let RuntimeHarnessBuildConfig {
            sender_request_behavior,
            sender_indication_behavior,
            receiver_request_behavior,
            receiver_indication_behavior,
            sender_config,
            receiver_config,
            send_rate_control,
            manual_time,
        } = config;
        let (system, manual_timer) =
            build_runtime_test_kompact_system(send_rate_control, manual_time);
        let mut socket_lease =
            reserve_sockets(&[ReservedSocketKind::UdpSocket, ReservedSocketKind::UdpSocket]);

        let driver = system.create(|| IoDriverComponent::new(DriverConfig::default()));
        let driver_for_bridge = driver.clone();
        let bridge = system.create(move || IoBridge::new(&driver_for_bridge));
        let bridge_handle = IoBridgeHandle::from_component(&bridge);
        let egress_pool = bridge_handle.egress_pool().clone();

        let (observer_tx, observer_rx) = mpsc::channel();
        let observer = system.create(move || UdpObserver::new(observer_tx));
        let observer_ref = observer
            .actor_ref()
            .hold()
            .expect("observer must expose a strong actor ref in tests");
        let bridge_to_observer = biconnect_components::<UdpPort, _, _>(&bridge, &observer)
            .expect("bridge/observer connection");

        start_component(&system, &driver);
        start_component(&system, &bridge);
        start_component(&system, &observer);

        let observer_rx = BufferedReceiver::new(observer_rx);
        let (sender_socket_id, sender_addr) =
            bind_socket(&observer, &observer_rx, socket_lease.addr(0));
        let (receiver_socket_id, receiver_addr) =
            bind_socket(&observer, &observer_rx, socket_lease.addr(1));
        socket_lease.release_binding(0);
        socket_lease.release_binding(1);

        let config = UDPourConfig::new(sender_config, receiver_config).unwrap();
        let sender_proxy = system.create(move || {
            ScriptedUdpProxy::new(
                sender_socket_id,
                sender_request_behavior,
                sender_indication_behavior,
            )
        });
        let receiver_proxy = system.create(move || {
            ScriptedUdpProxy::new(
                receiver_socket_id,
                receiver_request_behavior,
                receiver_indication_behavior,
            )
        });
        let sender_runtime = system.create({
            let config = config.clone();
            let egress_pool = egress_pool.clone();
            move || UDPourComponent::new(sender_socket_id, egress_pool, config)
        });
        let sender_runtime_ref = sender_runtime
            .actor_ref()
            .hold()
            .expect("sender runtime must still be live after creation");
        let receiver_runtime = system.create({
            let config = config.clone();
            let egress_pool = egress_pool.clone();
            move || UDPourComponent::new(receiver_socket_id, egress_pool, config)
        });
        let (sender_tx, _sender_rx) = mpsc::channel();
        let sender_probe = system.create(move || TransferProbe::new(sender_tx));
        let (receiver_tx, receiver_rx) = mpsc::channel();
        let receiver_probe = system.create(move || TransferProbe::new(receiver_tx));
        let receiver_probe_ref = receiver_probe
            .actor_ref()
            .hold()
            .expect("receiver probe must expose a strong actor ref in tests");

        let bridge_to_sender_proxy = biconnect_components::<UdpPort, _, _>(&bridge, &sender_proxy)
            .expect("bridge/sender_proxy connection");
        let bridge_to_receiver_proxy =
            biconnect_components::<UdpPort, _, _>(&bridge, &receiver_proxy)
                .expect("bridge/receiver_proxy connection");
        let sender_proxy_to_runtime =
            biconnect_components::<UdpPort, _, _>(&sender_proxy, &sender_runtime)
                .expect("sender_proxy/runtime connection");
        let receiver_proxy_to_runtime =
            biconnect_components::<UdpPort, _, _>(&receiver_proxy, &receiver_runtime)
                .expect("receiver_proxy/runtime connection");
        let sender_runtime_to_probe =
            biconnect_components::<UDPourPort, _, _>(&sender_runtime, &sender_probe)
                .expect("sender_runtime/probe connection");
        let receiver_runtime_to_probe =
            biconnect_components::<UDPourPort, _, _>(&receiver_runtime, &receiver_probe)
                .expect("receiver_runtime/probe connection");

        start_component(&system, &sender_proxy);
        start_component(&system, &receiver_proxy);
        start_component(&system, &sender_runtime);
        start_component(&system, &receiver_runtime);
        start_component(&system, &sender_probe);
        start_component(&system, &receiver_probe);

        Self {
            system,
            manual_timer,
            socket_lease,
            driver,
            bridge,
            observer,
            observer_ref,
            sender_proxy,
            receiver_proxy,
            sender_runtime,
            receiver_runtime,
            sender_runtime_ref,
            sender_probe,
            receiver_probe,
            receiver_probe_ref,
            _bridge_to_observer: bridge_to_observer,
            _bridge_to_sender_proxy: bridge_to_sender_proxy,
            _bridge_to_receiver_proxy: bridge_to_receiver_proxy,
            _sender_proxy_to_runtime: sender_proxy_to_runtime,
            _receiver_proxy_to_runtime: receiver_proxy_to_runtime,
            _sender_runtime_to_probe: sender_runtime_to_probe,
            _receiver_runtime_to_probe: receiver_runtime_to_probe,
            observer_rx,
            receiver_rx: BufferedReceiver::new(receiver_rx),
            sender_socket_id,
            sender_addr,
            receiver_socket_id,
            receiver_addr,
        }
    }

    pub(in crate::runtime_tests) fn advance_time(&self, duration: Duration) {
        // This only advances the logical manual clock. It does not wait for any
        // newly due timers to fire or for their resulting probe events to drain.
        self.manual_timer
            .as_ref()
            .expect("manual timer harness required for explicit time control")
            .advance_by(duration);
    }

    pub(in crate::runtime_tests) fn advance_time_and_process_due_timers(&self, duration: Duration) {
        let sender_before = self
            .sender_runtime
            .on_definition(|component| component.timer_snapshot());
        let receiver_before = self
            .receiver_runtime
            .on_definition(|component| component.timer_snapshot());
        self.advance_time(duration);
        Self::wait_for_due_timers_to_process(&self.sender_runtime, &sender_before, "sender");
        Self::wait_for_due_timers_to_process(&self.receiver_runtime, &receiver_before, "receiver");
        // This is intentionally scoped to timers that were already armed
        // before the logical-time advance. The helper assumes the harness was
        // otherwise settled before it was called.
        //
        // Once every previously due timer has changed, flush the observing
        // probes so any resulting indication already emitted is guaranteed to
        // be buffered before negative checks run.
        self.flush_observer_probe();
        self.flush_receiver_probe();
    }

    pub(in crate::runtime_tests) fn send(&self, payload: IoPayload) -> UDPourSubmitResult {
        self.submit_async(payload)
            .wait_timeout(WAIT_TIMEOUT)
            .expect("timed out waiting for sender submit result")
    }

    pub(in crate::runtime_tests) fn submit_async(
        &self,
        payload: IoPayload,
    ) -> KFuture<UDPourSubmitResult> {
        let target = self.receiver_addr;
        self.sender_runtime_ref.ask_with(|promise| {
            UDPourComponentMessage::Submit(Ask::new(promise, UDPourSend { target, payload }))
        })
    }

    pub(in crate::runtime_tests) fn close_sender_socket(&self) {
        let socket_id = self.sender_socket_id;
        self.observer.on_definition(|component| {
            component.udp.trigger(UdpRequest::Close { socket_id });
        });
    }

    pub(in crate::runtime_tests) fn inject_sender_indication(
        &self,
        source: SocketAddr,
        frame: &UDPourFrame,
    ) {
        self.system.trigger_i(
            UdpIndication::Received {
                socket_id: self.sender_socket_id,
                source,
                payload: encode_frame(frame).expect("injected sender frame must encode"),
            },
            &self.sender_runtime.required_ref(),
        );
    }

    pub(in crate::runtime_tests) fn inject_receiver_indication(
        &self,
        source: SocketAddr,
        frame: &UDPourFrame,
    ) {
        self.system.trigger_i(
            UdpIndication::Received {
                socket_id: self.receiver_socket_id,
                source,
                payload: encode_frame(frame).expect("injected receiver frame must encode"),
            },
            &self.receiver_runtime.required_ref(),
        );
    }

    pub(in crate::runtime_tests) fn wait_for_bridge_frame(
        &self,
        socket_id: SocketId,
        mut predicate: impl FnMut(&UDPourFrame) -> bool,
    ) -> UDPourFrame {
        let indication = self.observer_rx.recv_matching_or_fail(
            WAIT_TIMEOUT,
            |indication| {
                let UdpIndication::Received {
                    socket_id: indicated_socket_id,
                    payload,
                    ..
                } = indication
                else {
                    return false;
                };
                if *indicated_socket_id != socket_id {
                    return false;
                }
                let Ok(frame) = decode_frame(payload.clone()) else {
                    return false;
                };
                predicate(&frame)
            },
            |indication| match indication {
                UdpIndication::BindFailed {
                    request_id,
                    local_addr,
                    reason,
                } => Some(format!(
                    "UDPour test UDP bind request {request_id:?} failed at {local_addr} while waiting for a bridge frame on {socket_id:?}: {reason:?}"
                )),
                UdpIndication::Closed {
                    socket_id: indicated_socket_id,
                    remote_addr,
                    reason,
                } if *indicated_socket_id == socket_id => Some(format!(
                    "UDPour test socket {socket_id:?} closed while waiting for a bridge frame: remote={remote_addr:?}, reason={reason:?}"
                )),
                _ => None,
            },
        );
        let UdpIndication::Received { payload, .. } = indication else {
            unreachable!("recv_matching filtered to Received");
        };
        decode_frame(payload).expect("matched bridge frame must decode")
    }

    pub(in crate::runtime_tests) fn assert_no_bridge_frame(
        &self,
        socket_id: SocketId,
        duration: Duration,
        mut predicate: impl FnMut(&UDPourFrame) -> bool,
    ) {
        self.observer_rx.assert_no_match(duration, |indication| {
            let UdpIndication::Received {
                socket_id: indicated_socket_id,
                payload,
                ..
            } = indication
            else {
                return false;
            };
            if *indicated_socket_id != socket_id {
                return false;
            }
            let Ok(frame) = decode_frame(payload.clone()) else {
                return false;
            };
            predicate(&frame)
        });
    }

    pub(in crate::runtime_tests) fn wait_for_receiver_deliver(&self) -> UDPourDeliver {
        self.receiver_rx.recv_matching(WAIT_TIMEOUT, |_| true)
    }

    pub(in crate::runtime_tests) fn assert_no_receiver_deliver(&self, duration: Duration) {
        self.receiver_rx.assert_no_match(duration, |_| true);
    }

    pub(in crate::runtime_tests) fn wait_for_sender_dispatch_timer(&self) {
        eventually_component_state(
            WAIT_TIMEOUT,
            &self.sender_runtime,
            |component| component.is_waiting_for_dispatch_timer(),
            "timed out waiting for sender dispatch timer to be armed",
        );
    }

    pub(in crate::runtime_tests) fn wait_for_receiver_runtime_payload(
        &self,
        source: SocketAddr,
        header: UDPourHeader,
    ) {
        eventually_component_state(
            WAIT_TIMEOUT,
            &self.receiver_runtime,
            |component| component.receiver_has_reflected_payload(source, header),
            format_args!(
                "timed out waiting for receiver runtime to reflect payload part {:?} for message_id={:?} from {source}",
                header.part_number, header.message_id,
            ),
        );
    }

    pub(in crate::runtime_tests) fn wait_for_sender_runtime_need_parts(
        &self,
        source: SocketAddr,
        frame: &NeedPartsFrame,
    ) {
        eventually_component_state(
            WAIT_TIMEOUT,
            &self.sender_runtime,
            |component| component.sender_has_processed_need_parts(source, frame),
            format_args!(
                "timed out waiting for sender runtime to process NeedParts for message_id={:?} from {source} with missing parts {:?}",
                frame.header.message_id,
                frame.missing_parts.iter().collect::<Vec<_>>(),
            ),
        );
    }

    pub(in crate::runtime_tests) fn wait_for_due_timers_to_process(
        component: &Arc<Component<UDPourComponent>>,
        before: &RuntimeTimerSnapshot,
        component_name: &str,
    ) {
        eventually_component_state(
            WAIT_TIMEOUT,
            component,
            |component| {
                let after = component.timer_snapshot();
                Self::timer_processed(
                    before.dispatch_timer.as_ref(),
                    after.dispatch_timer.as_ref(),
                    after.now,
                ) && Self::timer_processed(
                    before.poll_timer.as_ref(),
                    after.poll_timer.as_ref(),
                    after.now,
                )
            },
            format_args!(
                "timed out waiting for {component_name} runtime to process timers due during manual-time advance"
            ),
        );
    }

    pub(in crate::runtime_tests) fn timer_processed(
        before: Option<&ActiveTimerSnapshot>,
        after: Option<&ActiveTimerSnapshot>,
        now: Instant,
    ) -> bool {
        let Some(before) = before else {
            return true;
        };
        if before.due_at > now {
            return true;
        }
        !matches!(after, Some(after) if after.handle == before.handle)
    }

    pub(in crate::runtime_tests) fn flush_observer_probe(&self) {
        let (promise, future) = promise::<()>();
        self.observer_ref.tell(UdpObserverMessage::Barrier(promise));
        future
            .wait_timeout(WAIT_TIMEOUT)
            .expect("timed out waiting for observer probe barrier");
    }

    pub(in crate::runtime_tests) fn flush_receiver_probe(&self) {
        let (promise, future) = promise::<()>();
        self.receiver_probe_ref
            .tell(TransferProbeMessage::Barrier(promise));
        future
            .wait_timeout(WAIT_TIMEOUT)
            .expect("timed out waiting for receiver probe barrier");
    }

    pub(in crate::runtime_tests) fn shutdown(mut self) {
        self.socket_lease
            .rebind_binding(0)
            .expect("rebind reserved sender UDP socket");
        self.socket_lease
            .rebind_binding(1)
            .expect("rebind reserved receiver UDP socket");
        kill_component(&self.system, self.receiver_probe);
        kill_component(&self.system, self.sender_probe);
        kill_component(&self.system, self.receiver_runtime);
        kill_component(&self.system, self.sender_runtime);
        kill_component(&self.system, self.receiver_proxy);
        kill_component(&self.system, self.sender_proxy);
        kill_component(&self.system, self.observer);
        kill_component(&self.system, self.bridge);
        kill_component(&self.system, self.driver);
        self.system.shutdown().wait().expect("Kompact shutdown");
    }
}
