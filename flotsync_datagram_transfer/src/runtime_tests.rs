use crate::{
    ReceiverConfig,
    SenderConfig,
    codec::{decode_frame, encode_frame},
    runtime::{
        DatagramTransferComponent,
        DatagramTransferConfig,
        DatagramTransferDeliver,
        DatagramTransferPort,
        DatagramTransferPortIndication,
        DatagramTransferPortRequest,
        DatagramTransferSend,
        DatagramTransferSendFailed,
        DatagramTransferSendFailureReason,
        DatagramTransferSendRef,
    },
    types::{
        AckFrame,
        Checksum,
        DatagramFrame,
        DatagramHeader,
        FrameType,
        MessageId,
        PROTOCOL_VERSION,
        PartCount,
        PartNumber,
        PayloadFrame,
    },
};
use bytes::Bytes;
use flotsync_io::{
    prelude::{
        DriverConfig,
        IoBridge,
        IoBridgeHandle,
        IoDriverComponent,
        IoPayload,
        SendFailureReason,
        SocketId,
        UdpIndication,
        UdpLocalBind,
        UdpOpenRequestId,
        UdpPort,
        UdpRequest,
        UdpSendResult,
    },
    test_support::{
        UdpObserver,
        WAIT_TIMEOUT,
        build_test_kompact_system,
        init_test_logger,
        kill_component,
        localhost,
        start_component,
    },
};
use kompact::prelude::*;
use std::{
    cell::RefCell,
    cmp::Reverse,
    collections::VecDeque,
    net::SocketAddr,
    sync::{Arc, mpsc},
    time::{Duration, Instant},
};

#[derive(ComponentDefinition)]
struct TransferProbe {
    ctx: ComponentContext<Self>,
    transfer: RequiredPort<DatagramTransferPort>,
    indications: mpsc::Sender<DatagramTransferPortIndication>,
}

impl TransferProbe {
    fn new(indications: mpsc::Sender<DatagramTransferPortIndication>) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            transfer: RequiredPort::uninitialised(),
            indications,
        }
    }
}

ignore_lifecycle!(TransferProbe);

impl Require<DatagramTransferPort> for TransferProbe {
    fn handle(&mut self, indication: DatagramTransferPortIndication) -> Handled {
        self.indications
            .send(indication)
            .expect("transfer indication receiver must stay live during tests");
        Handled::Ok
    }
}

impl Actor for TransferProbe {
    type Message = Never;

    fn receive_local(&mut self, _msg: Self::Message) -> Handled {
        unreachable!("Never type is empty")
    }

    fn receive_network(&mut self, _msg: NetMessage) -> Handled {
        unimplemented!("TransferProbe does not use network actor messages")
    }
}

#[derive(Debug)]
enum ProxyRequestBehavior {
    Pass,
    NackFirstSend {
        reason: SendFailureReason,
        fired: bool,
    },
}

#[derive(Debug)]
enum ProxyIndicationBehavior {
    Pass,
    DropFirstPayloadPart {
        part_number: PartNumber,
        dropped: bool,
    },
    ReorderFirstTransfer {
        buffered: Vec<UdpIndication>,
        expected_parts: Option<u32>,
        flushed: bool,
    },
    DuplicatePayloadPart {
        part_number: PartNumber,
        conflicting: bool,
        duplicated: bool,
        drop_later_payloads: bool,
        duplicated_message_id: Option<MessageId>,
    },
    InjectMalformedFramesOnce {
        injected: bool,
    },
}

#[derive(ComponentDefinition)]
struct ScriptedUdpProxy {
    ctx: ComponentContext<Self>,
    upstream: RequiredPort<UdpPort>,
    downstream: ProvidedPort<UdpPort>,
    socket_id: SocketId,
    request_behavior: ProxyRequestBehavior,
    indication_behavior: ProxyIndicationBehavior,
}

impl ScriptedUdpProxy {
    fn new(
        socket_id: SocketId,
        request_behavior: ProxyRequestBehavior,
        indication_behavior: ProxyIndicationBehavior,
    ) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            upstream: RequiredPort::uninitialised(),
            downstream: ProvidedPort::uninitialised(),
            socket_id,
            request_behavior,
            indication_behavior,
        }
    }

    fn transform_indication(&mut self, indication: UdpIndication) -> Vec<UdpIndication> {
        match &mut self.indication_behavior {
            ProxyIndicationBehavior::Pass => vec![indication],
            ProxyIndicationBehavior::DropFirstPayloadPart {
                part_number,
                dropped,
            } => {
                let Some(frame) = payload_frame_for_socket(self.socket_id, &indication) else {
                    return vec![indication];
                };
                if frame.header.part_number == *part_number && !*dropped {
                    *dropped = true;
                    return Vec::new();
                }
                vec![indication]
            }
            ProxyIndicationBehavior::ReorderFirstTransfer {
                buffered,
                expected_parts,
                flushed,
            } => {
                let Some(frame) = payload_frame_for_socket(self.socket_id, &indication) else {
                    return vec![indication];
                };
                if *flushed {
                    return vec![indication];
                }
                if expected_parts.is_none() {
                    *expected_parts = Some(frame.header.part_count.get());
                }
                buffered.push(indication);
                if buffered.len() as u32 != expected_parts.expect("expected_parts just set") {
                    return Vec::new();
                }
                *flushed = true;
                buffered.sort_by_key(|indication| {
                    let part_number = payload_frame_for_socket(self.socket_id, indication)
                        .expect("buffered indications are payloads for this socket")
                        .header
                        .part_number
                        .0;
                    Reverse(part_number)
                });
                std::mem::take(buffered)
            }
            ProxyIndicationBehavior::DuplicatePayloadPart {
                part_number,
                conflicting,
                duplicated,
                drop_later_payloads,
                duplicated_message_id,
            } => {
                let Some(frame) = payload_frame_for_socket(self.socket_id, &indication) else {
                    return vec![indication];
                };
                if *duplicated
                    && *drop_later_payloads
                    && Some(frame.header.message_id) == *duplicated_message_id
                {
                    return Vec::new();
                }
                if frame.header.part_number != *part_number || *duplicated {
                    return vec![indication];
                }
                *duplicated = true;
                *duplicated_message_id = Some(frame.header.message_id);
                let duplicate = if *conflicting {
                    conflicting_duplicate_indication(&indication, &frame)
                } else {
                    indication.clone()
                };
                vec![indication, duplicate]
            }
            ProxyIndicationBehavior::InjectMalformedFramesOnce { injected } => {
                let Some(_) = payload_frame_for_socket(self.socket_id, &indication) else {
                    return vec![indication];
                };
                if *injected {
                    return vec![indication];
                }
                *injected = true;
                let UdpIndication::Received {
                    socket_id, source, ..
                } = &indication
                else {
                    unreachable!("payload_frame_for_socket filtered to Received");
                };
                vec![
                    malformed_payload_indication(*socket_id, *source),
                    malformed_control_indication(*socket_id, *source),
                    indication,
                ]
            }
        }
    }
}

ignore_lifecycle!(ScriptedUdpProxy);

impl Provide<UdpPort> for ScriptedUdpProxy {
    fn handle(&mut self, request: UdpRequest) -> Handled {
        match request {
            UdpRequest::Send {
                socket_id,
                transmission_id,
                payload,
                target,
                reply_to,
            } => match &mut self.request_behavior {
                ProxyRequestBehavior::Pass => {
                    self.upstream.trigger(UdpRequest::Send {
                        socket_id,
                        transmission_id,
                        payload,
                        target,
                        reply_to,
                    });
                }
                ProxyRequestBehavior::NackFirstSend { reason, fired } if !*fired => {
                    *fired = true;
                    reply_to.tell(UdpSendResult::Nack {
                        socket_id,
                        transmission_id,
                        reason: *reason,
                    });
                }
                ProxyRequestBehavior::NackFirstSend { .. } => {
                    self.upstream.trigger(UdpRequest::Send {
                        socket_id,
                        transmission_id,
                        payload,
                        target,
                        reply_to,
                    });
                }
            },
            other => self.upstream.trigger(other),
        }
        Handled::Ok
    }
}

impl Require<UdpPort> for ScriptedUdpProxy {
    fn handle(&mut self, indication: UdpIndication) -> Handled {
        for forwarded in self.transform_indication(indication) {
            self.downstream.trigger(forwarded);
        }
        Handled::Ok
    }
}

impl Actor for ScriptedUdpProxy {
    type Message = Never;

    fn receive_local(&mut self, _msg: Self::Message) -> Handled {
        unreachable!("Never type is empty")
    }

    fn receive_network(&mut self, _msg: NetMessage) -> Handled {
        unimplemented!("ScriptedUdpProxy does not use network actor messages")
    }
}

#[derive(Debug)]
struct BufferedReceiver<T> {
    receiver: mpsc::Receiver<T>,
    deferred: RefCell<VecDeque<T>>,
}

impl<T> BufferedReceiver<T> {
    fn new(receiver: mpsc::Receiver<T>) -> Self {
        Self {
            receiver,
            deferred: RefCell::new(VecDeque::new()),
        }
    }

    fn take_deferred_match(&self, predicate: &mut impl FnMut(&T) -> bool) -> Option<T> {
        let mut deferred = self.deferred.borrow_mut();
        let deferred_len = deferred.len();
        for _ in 0..deferred_len {
            let event = deferred
                .pop_front()
                .expect("deferred length was just measured");
            if predicate(&event) {
                return Some(event);
            }
            deferred.push_back(event);
        }
        None
    }

    fn recv_matching(&self, timeout: Duration, mut predicate: impl FnMut(&T) -> bool) -> T {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(event) = self.take_deferred_match(&mut predicate) {
                return event;
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            let event = self
                .receiver
                .recv_timeout(remaining)
                .expect("timed out waiting for buffered test event");
            if predicate(&event) {
                return event;
            }
            self.deferred.borrow_mut().push_back(event);
        }
    }

    fn assert_no_match(&self, duration: Duration, mut predicate: impl FnMut(&T) -> bool) {
        if self.deferred.borrow().iter().any(&mut predicate) {
            panic!("unexpected buffered test event matched negative assertion");
        }

        let deadline = Instant::now() + duration;
        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let timeout = remaining.min(Duration::from_millis(10));
            let Ok(event) = self.receiver.recv_timeout(timeout) else {
                continue;
            };
            if predicate(&event) {
                panic!("unexpected test event matched negative assertion");
            }
            self.deferred.borrow_mut().push_back(event);
        }
    }
}

struct RuntimeHarness {
    system: KompactSystem,
    driver: Arc<Component<IoDriverComponent>>,
    bridge: Arc<Component<IoBridge>>,
    observer: Arc<Component<UdpObserver>>,
    sender_proxy: Arc<Component<ScriptedUdpProxy>>,
    receiver_proxy: Arc<Component<ScriptedUdpProxy>>,
    sender_runtime: Arc<Component<DatagramTransferComponent>>,
    receiver_runtime: Arc<Component<DatagramTransferComponent>>,
    sender_probe: Arc<Component<TransferProbe>>,
    receiver_probe: Arc<Component<TransferProbe>>,
    _bridge_to_observer: TwoWayChannel<UdpPort, IoBridge, UdpObserver>,
    _bridge_to_sender_proxy: TwoWayChannel<UdpPort, IoBridge, ScriptedUdpProxy>,
    _bridge_to_receiver_proxy: TwoWayChannel<UdpPort, IoBridge, ScriptedUdpProxy>,
    _sender_proxy_to_runtime: TwoWayChannel<UdpPort, ScriptedUdpProxy, DatagramTransferComponent>,
    _receiver_proxy_to_runtime: TwoWayChannel<UdpPort, ScriptedUdpProxy, DatagramTransferComponent>,
    _sender_runtime_to_probe:
        TwoWayChannel<DatagramTransferPort, DatagramTransferComponent, TransferProbe>,
    _receiver_runtime_to_probe:
        TwoWayChannel<DatagramTransferPort, DatagramTransferComponent, TransferProbe>,
    observer_rx: BufferedReceiver<UdpIndication>,
    sender_rx: BufferedReceiver<DatagramTransferPortIndication>,
    receiver_rx: BufferedReceiver<DatagramTransferPortIndication>,
    sender_socket_id: SocketId,
    receiver_socket_id: SocketId,
    receiver_addr: SocketAddr,
}

impl RuntimeHarness {
    fn new(
        sender_request_behavior: ProxyRequestBehavior,
        sender_indication_behavior: ProxyIndicationBehavior,
        receiver_request_behavior: ProxyRequestBehavior,
        receiver_indication_behavior: ProxyIndicationBehavior,
        sender_config: SenderConfig,
        receiver_config: ReceiverConfig,
    ) -> Self {
        init_test_logger();

        let system = build_test_kompact_system();
        let driver = system.create(|| IoDriverComponent::new(DriverConfig::default()));
        let driver_for_bridge = driver.clone();
        let bridge = system.create(move || IoBridge::new(&driver_for_bridge));
        let bridge_handle = IoBridgeHandle::from_component(&bridge);
        let egress_pool = bridge_handle.egress_pool().clone();

        let (observer_tx, observer_rx) = mpsc::channel();
        let observer = system.create(move || UdpObserver::new(observer_tx));
        let bridge_to_observer = biconnect_components::<UdpPort, _, _>(&bridge, &observer)
            .expect("bridge/observer connection");

        start_component(&system, &driver);
        start_component(&system, &bridge);
        start_component(&system, &observer);

        let observer_rx = BufferedReceiver::new(observer_rx);
        let (sender_socket_id, _) = bind_socket(&observer, &observer_rx);
        let (receiver_socket_id, receiver_addr) = bind_socket(&observer, &observer_rx);

        let config = DatagramTransferConfig::new(sender_config, receiver_config).unwrap();
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
            move || DatagramTransferComponent::new(sender_socket_id, egress_pool, config)
        });
        let receiver_runtime = system.create({
            let config = config.clone();
            let egress_pool = egress_pool.clone();
            move || DatagramTransferComponent::new(receiver_socket_id, egress_pool, config)
        });
        let (sender_tx, sender_rx) = mpsc::channel();
        let sender_probe = system.create(move || TransferProbe::new(sender_tx));
        let (receiver_tx, receiver_rx) = mpsc::channel();
        let receiver_probe = system.create(move || TransferProbe::new(receiver_tx));

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
            biconnect_components::<DatagramTransferPort, _, _>(&sender_runtime, &sender_probe)
                .expect("sender_runtime/probe connection");
        let receiver_runtime_to_probe =
            biconnect_components::<DatagramTransferPort, _, _>(&receiver_runtime, &receiver_probe)
                .expect("receiver_runtime/probe connection");

        start_component(&system, &sender_proxy);
        start_component(&system, &receiver_proxy);
        start_component(&system, &sender_runtime);
        start_component(&system, &receiver_runtime);
        start_component(&system, &sender_probe);
        start_component(&system, &receiver_probe);

        Self {
            system,
            driver,
            bridge,
            observer,
            sender_proxy,
            receiver_proxy,
            sender_runtime,
            receiver_runtime,
            sender_probe,
            receiver_probe,
            _bridge_to_observer: bridge_to_observer,
            _bridge_to_sender_proxy: bridge_to_sender_proxy,
            _bridge_to_receiver_proxy: bridge_to_receiver_proxy,
            _sender_proxy_to_runtime: sender_proxy_to_runtime,
            _receiver_proxy_to_runtime: receiver_proxy_to_runtime,
            _sender_runtime_to_probe: sender_runtime_to_probe,
            _receiver_runtime_to_probe: receiver_runtime_to_probe,
            observer_rx,
            sender_rx: BufferedReceiver::new(sender_rx),
            receiver_rx: BufferedReceiver::new(receiver_rx),
            sender_socket_id,
            receiver_socket_id,
            receiver_addr,
        }
    }

    fn send(&self, send_ref: DatagramTransferSendRef, payload: IoPayload) {
        let target = self.receiver_addr;
        self.sender_probe.on_definition(|component| {
            component
                .transfer
                .trigger(DatagramTransferPortRequest::Send(DatagramTransferSend {
                    send_ref,
                    target,
                    payload,
                }));
        });
    }

    fn wait_for_bridge_frame(
        &self,
        socket_id: SocketId,
        mut predicate: impl FnMut(&DatagramFrame) -> bool,
    ) -> DatagramFrame {
        let indication = self.observer_rx.recv_matching(WAIT_TIMEOUT, |indication| {
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
        let UdpIndication::Received { payload, .. } = indication else {
            unreachable!("recv_matching filtered to Received");
        };
        decode_frame(payload).expect("matched bridge frame must decode")
    }

    fn assert_no_bridge_frame(
        &self,
        socket_id: SocketId,
        duration: Duration,
        mut predicate: impl FnMut(&DatagramFrame) -> bool,
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

    fn wait_for_receiver_deliver(&self) -> DatagramTransferDeliver {
        let indication = self.receiver_rx.recv_matching(WAIT_TIMEOUT, |_| true);
        match indication {
            DatagramTransferPortIndication::Deliver(deliver) => deliver,
            DatagramTransferPortIndication::SendFailed(failed) => {
                panic!("unexpected receiver send failure: {failed:?}")
            }
        }
    }

    fn wait_for_sender_send_failed(&self) -> DatagramTransferSendFailed {
        let indication = self.sender_rx.recv_matching(WAIT_TIMEOUT, |_| true);
        match indication {
            DatagramTransferPortIndication::SendFailed(failed) => failed,
            DatagramTransferPortIndication::Deliver(deliver) => {
                panic!("unexpected sender deliver indication: {deliver:?}")
            }
        }
    }

    fn assert_no_sender_send_failed(&self, duration: Duration) {
        self.sender_rx.assert_no_match(duration, |indication| {
            matches!(indication, DatagramTransferPortIndication::SendFailed(_))
        });
    }

    fn assert_no_receiver_deliver(&self, duration: Duration) {
        self.receiver_rx.assert_no_match(duration, |indication| {
            matches!(indication, DatagramTransferPortIndication::Deliver(_))
        });
    }

    fn shutdown(self) {
        kill_component(&self.system, self.receiver_probe);
        kill_component(&self.system, self.sender_probe);
        kill_component(&self.system, self.receiver_runtime);
        kill_component(&self.system, self.sender_runtime);
        kill_component(&self.system, self.receiver_proxy);
        kill_component(&self.system, self.sender_proxy);
        kill_component(&self.system, self.observer);
        kill_component(&self.system, self.bridge);
        kill_component(&self.system, self.driver);
        self.system.shutdown().expect("Kompact shutdown");
    }
}

fn bind_socket(
    observer: &Arc<Component<UdpObserver>>,
    observer_rx: &BufferedReceiver<UdpIndication>,
) -> (SocketId, SocketAddr) {
    let request_id = UdpOpenRequestId::new();
    observer.on_definition(|component| {
        component.udp.trigger(UdpRequest::Bind {
            request_id,
            bind: UdpLocalBind::Exact(localhost(0)),
        });
    });
    match observer_rx.recv_matching(WAIT_TIMEOUT, |event| {
        matches!(
            event,
            UdpIndication::Bound {
                request_id: indicated_request_id,
                ..
            } if *indicated_request_id == request_id
        )
    }) {
        UdpIndication::Bound {
            request_id: indicated_request_id,
            socket_id,
            local_addr,
        } => {
            assert_eq!(indicated_request_id, request_id);
            (socket_id, local_addr)
        }
        other => unreachable!("filtered to Bound, got {other:?}"),
    }
}

fn default_sender_config(retention_timeout: Duration) -> SenderConfig {
    SenderConfig::new(4, retention_timeout, Duration::from_millis(100)).unwrap()
}

fn default_receiver_config(repair_interval: Duration) -> ReceiverConfig {
    ReceiverConfig {
        repair_interval,
        give_up_timeout: Duration::from_millis(200),
        max_need_parts_frame_len: 256,
    }
}

fn payload_frame_for_socket(
    socket_id: SocketId,
    indication: &UdpIndication,
) -> Option<PayloadFrame> {
    let UdpIndication::Received {
        socket_id: indicated_socket_id,
        payload,
        ..
    } = indication
    else {
        return None;
    };
    if *indicated_socket_id != socket_id {
        return None;
    }
    match decode_frame(payload.clone()).ok()? {
        DatagramFrame::Payload(frame) => Some(frame),
        _ => None,
    }
}

fn conflicting_duplicate_indication(
    indication: &UdpIndication,
    frame: &PayloadFrame,
) -> UdpIndication {
    let UdpIndication::Received {
        socket_id, source, ..
    } = indication
    else {
        unreachable!("payload indication must be Received");
    };
    let mut bytes = frame.payload.to_vec();
    if bytes.is_empty() {
        bytes.push(0xFF);
    } else {
        bytes[0] ^= 0xFF;
    }
    let duplicate = DatagramFrame::Payload(PayloadFrame {
        header: frame.header,
        payload: IoPayload::from(Bytes::from(bytes)),
    });
    UdpIndication::Received {
        socket_id: *socket_id,
        source: *source,
        payload: encode_frame(&duplicate).expect("duplicate payload frame must encode"),
    }
}

fn malformed_payload_indication(socket_id: SocketId, source: SocketAddr) -> UdpIndication {
    let valid = DatagramFrame::Payload(PayloadFrame {
        header: DatagramHeader::payload(
            MessageId(901),
            PartNumber(0),
            PartCount::new(1).unwrap(),
            Checksum(77),
        ),
        payload: IoPayload::from_static(b"x"),
    });
    let mut bytes = encode_frame(&valid)
        .expect("valid payload frame must encode")
        .to_vec();
    bytes[8..12].copy_from_slice(&1u32.to_be_bytes());
    UdpIndication::Received {
        socket_id,
        source,
        payload: IoPayload::from(Bytes::from(bytes)),
    }
}

fn malformed_control_indication(socket_id: SocketId, source: SocketAddr) -> UdpIndication {
    let valid = DatagramFrame::Ack(AckFrame {
        header: DatagramHeader::control(
            FrameType::Ack,
            MessageId(902),
            PartCount::new(1).unwrap(),
            Checksum(88),
        )
        .unwrap(),
    });
    let mut bytes = encode_frame(&valid)
        .expect("valid ack frame must encode")
        .to_vec();
    bytes[1] = PROTOCOL_VERSION + 1;
    UdpIndication::Received {
        socket_id,
        source,
        payload: IoPayload::from(Bytes::from(bytes)),
    }
}

#[test]
fn basic_component_smoke_send_deliver_ack_without_repair() {
    let harness = RuntimeHarness::new(
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::Pass,
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::Pass,
        default_sender_config(Duration::from_millis(200)),
        default_receiver_config(Duration::from_millis(40)),
    );

    harness.send(
        DatagramTransferSendRef(1),
        IoPayload::from_static(b"hello world"),
    );

    let deliver = harness.wait_for_receiver_deliver();
    assert_eq!(deliver.payload.to_vec().as_slice(), b"hello world");

    let ack = harness.wait_for_bridge_frame(harness.sender_socket_id, |frame| {
        matches!(frame, DatagramFrame::Ack(_))
    });
    assert!(matches!(ack, DatagramFrame::Ack(_)));
    harness.assert_no_bridge_frame(
        harness.sender_socket_id,
        Duration::from_millis(100),
        |frame| matches!(frame, DatagramFrame::NeedParts(_)),
    );
    harness.assert_no_sender_send_failed(Duration::from_millis(100));
    harness.shutdown();
}

#[test]
fn repair_path_emits_need_parts_and_retransmits_only_missing_part() {
    let harness = RuntimeHarness::new(
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::Pass,
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::DropFirstPayloadPart {
            part_number: PartNumber(1),
            dropped: false,
        },
        default_sender_config(Duration::from_millis(200)),
        default_receiver_config(Duration::from_millis(20)),
    );

    harness.send(
        DatagramTransferSendRef(2),
        IoPayload::from_static(b"abcdefghijkl"),
    );
    for _ in 0..3 {
        harness.wait_for_bridge_frame(harness.receiver_socket_id, |frame| {
            matches!(frame, DatagramFrame::Payload(_))
        });
    }

    let need_parts = harness.wait_for_bridge_frame(harness.sender_socket_id, |frame| {
        matches!(frame, DatagramFrame::NeedParts(_))
    });
    let DatagramFrame::NeedParts(need_parts) = need_parts else {
        unreachable!("filtered to NeedParts");
    };
    let missing: Vec<_> = need_parts.missing_parts.iter().collect();
    assert_eq!(missing, vec![1]);

    let retransmit = harness.wait_for_bridge_frame(harness.receiver_socket_id, |frame| {
        let DatagramFrame::Payload(frame) = frame else {
            return false;
        };
        if frame.header.part_number != PartNumber(1) {
            return false;
        }
        frame.payload.to_vec().as_slice() == b"efgh"
    });
    assert!(matches!(retransmit, DatagramFrame::Payload(_)));
    harness.assert_no_bridge_frame(
        harness.receiver_socket_id,
        Duration::from_millis(100),
        |frame| matches!(frame, DatagramFrame::Payload(_)),
    );

    let deliver = harness.wait_for_receiver_deliver();
    assert_eq!(deliver.payload.to_vec().as_slice(), b"abcdefghijkl");
    harness.shutdown();
}

#[test]
fn no_longer_available_after_sender_retention_expiry() {
    let harness = RuntimeHarness::new(
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::Pass,
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::DropFirstPayloadPart {
            part_number: PartNumber(1),
            dropped: false,
        },
        default_sender_config(Duration::from_millis(20)),
        default_receiver_config(Duration::from_millis(50)),
    );

    harness.send(
        DatagramTransferSendRef(3),
        IoPayload::from_static(b"abcdefghijkl"),
    );
    for _ in 0..3 {
        harness.wait_for_bridge_frame(harness.receiver_socket_id, |frame| {
            matches!(frame, DatagramFrame::Payload(_))
        });
    }
    harness.wait_for_bridge_frame(harness.sender_socket_id, |frame| {
        matches!(frame, DatagramFrame::NeedParts(_))
    });
    let nla = harness.wait_for_bridge_frame(harness.receiver_socket_id, |frame| {
        matches!(frame, DatagramFrame::NoLongerAvailable(_))
    });
    assert!(matches!(nla, DatagramFrame::NoLongerAvailable(_)));
    harness.assert_no_receiver_deliver(Duration::from_millis(150));
    harness.shutdown();
}

#[test]
fn runtime_nack_reports_one_send_failed_with_correct_identity() {
    let harness = RuntimeHarness::new(
        ProxyRequestBehavior::NackFirstSend {
            reason: SendFailureReason::Closed,
            fired: false,
        },
        ProxyIndicationBehavior::Pass,
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::Pass,
        default_sender_config(Duration::from_millis(200)),
        default_receiver_config(Duration::from_millis(40)),
    );

    harness.send(DatagramTransferSendRef(44), IoPayload::from_static(b"x"));

    let failed = harness.wait_for_sender_send_failed();
    assert_eq!(failed.send_ref, DatagramTransferSendRef(44));
    assert_eq!(failed.message_id, Some(MessageId(0)));
    assert_eq!(
        failed.reason,
        DatagramTransferSendFailureReason::Transport(SendFailureReason::Closed)
    );
    harness.assert_no_sender_send_failed(Duration::from_millis(100));
    harness.shutdown();
}

#[test]
fn runtime_out_of_order_payloads_still_reassemble_and_ack() {
    let harness = RuntimeHarness::new(
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::Pass,
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::ReorderFirstTransfer {
            buffered: Vec::new(),
            expected_parts: None,
            flushed: false,
        },
        default_sender_config(Duration::from_millis(200)),
        default_receiver_config(Duration::from_millis(40)),
    );

    harness.send(
        DatagramTransferSendRef(5),
        IoPayload::from_static(b"abcdefghijkl"),
    );

    let deliver = harness.wait_for_receiver_deliver();
    assert_eq!(deliver.payload.to_vec().as_slice(), b"abcdefghijkl");
    harness.wait_for_bridge_frame(harness.sender_socket_id, |frame| {
        matches!(frame, DatagramFrame::Ack(_))
    });
    harness.shutdown();
}

#[test]
fn runtime_duplicate_payload_datagrams_do_not_break_delivery() {
    let harness = RuntimeHarness::new(
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::Pass,
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::DuplicatePayloadPart {
            part_number: PartNumber(0),
            conflicting: false,
            duplicated: false,
            drop_later_payloads: false,
            duplicated_message_id: None,
        },
        default_sender_config(Duration::from_millis(200)),
        default_receiver_config(Duration::from_millis(40)),
    );

    harness.send(
        DatagramTransferSendRef(6),
        IoPayload::from_static(b"abcdefghijkl"),
    );

    let deliver = harness.wait_for_receiver_deliver();
    assert_eq!(deliver.payload.to_vec().as_slice(), b"abcdefghijkl");
    harness.assert_no_bridge_frame(
        harness.sender_socket_id,
        Duration::from_millis(100),
        |frame| matches!(frame, DatagramFrame::NeedParts(_)),
    );
    harness.shutdown();
}

#[test]
fn runtime_conflicting_duplicates_purge_without_requesting_repair() {
    let harness = RuntimeHarness::new(
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::Pass,
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::DuplicatePayloadPart {
            part_number: PartNumber(0),
            conflicting: true,
            duplicated: false,
            drop_later_payloads: true,
            duplicated_message_id: None,
        },
        default_sender_config(Duration::from_millis(200)),
        default_receiver_config(Duration::from_millis(40)),
    );

    harness.send(
        DatagramTransferSendRef(7),
        IoPayload::from_static(b"abcdefgh"),
    );
    harness.assert_no_receiver_deliver(Duration::from_millis(150));
    harness.assert_no_bridge_frame(
        harness.sender_socket_id,
        Duration::from_millis(150),
        |frame| matches!(frame, DatagramFrame::NeedParts(_)),
    );

    harness.send(DatagramTransferSendRef(8), IoPayload::from_static(b"qrst"));
    let deliver = harness.wait_for_receiver_deliver();
    assert_eq!(deliver.payload.to_vec().as_slice(), b"qrst");
    harness.shutdown();
}

#[test]
fn malformed_frames_are_dropped_without_poisoning_valid_traffic() {
    let harness = RuntimeHarness::new(
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::Pass,
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::InjectMalformedFramesOnce { injected: false },
        default_sender_config(Duration::from_millis(200)),
        default_receiver_config(Duration::from_millis(40)),
    );

    harness.send(
        DatagramTransferSendRef(9),
        IoPayload::from_static(b"abcdefghijkl"),
    );
    let first = harness.wait_for_receiver_deliver();
    assert_eq!(first.payload.to_vec().as_slice(), b"abcdefghijkl");

    harness.send(DatagramTransferSendRef(10), IoPayload::from_static(b"mnop"));
    let second = harness.wait_for_receiver_deliver();
    assert_eq!(second.payload.to_vec().as_slice(), b"mnop");
    harness.shutdown();
}

#[test]
fn zero_length_payload_round_trips_through_runtime() {
    let harness = RuntimeHarness::new(
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::Pass,
        ProxyRequestBehavior::Pass,
        ProxyIndicationBehavior::Pass,
        default_sender_config(Duration::from_millis(200)),
        default_receiver_config(Duration::from_millis(40)),
    );

    harness.send(DatagramTransferSendRef(11), IoPayload::from_static(b""));
    let deliver = harness.wait_for_receiver_deliver();
    assert_eq!(deliver.part_count, PartCount::new(1).unwrap());
    assert_eq!(deliver.payload.len(), 0);
    harness.wait_for_bridge_frame(harness.sender_socket_id, |frame| {
        matches!(frame, DatagramFrame::Ack(_))
    });
    harness.shutdown();
}
