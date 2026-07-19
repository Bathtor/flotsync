//! Test component doubles and scripted UDP proxy behaviour.

use super::{
    frames::{
        conflicting_duplicate_indication,
        malformed_control_indication,
        malformed_payload_indication,
        payload_frame_for_socket,
    },
    *,
};

#[derive(Clone, Copy, Debug)]
pub(in crate::runtime_tests) struct TestSendRateControl {
    pub(in crate::runtime_tests) send_delay: Duration,
    pub(in crate::runtime_tests) backpressure_retry_delay: Duration,
    pub(in crate::runtime_tests) max_in_flight_datagrams: usize,
}

impl Default for TestSendRateControl {
    fn default() -> Self {
        Self {
            send_delay: config_keys::SEND_DELAY
                .default()
                .expect("UDPour send-delay default must exist"),
            backpressure_retry_delay: config_keys::BACKPRESSURE_RETRY_DELAY
                .default()
                .expect("UDPour backpressure-retry-delay default must exist"),
            max_in_flight_datagrams: config_keys::MAX_IN_FLIGHT_DATAGRAMS
                .default()
                .expect("UDPour max-in-flight-datagrams default must exist"),
        }
    }
}

// TODO(flotsync-h1z0): Replace this observer/barrier fixture once generic
// actor-message testing support covers barrier-style test commands.
#[derive(ComponentDefinition)]
pub(in crate::runtime_tests) struct TransferProbe {
    ctx: ComponentContext<Self>,
    transfer: RequiredPort<UDPourPort>,
    indications: mpsc::Sender<UDPourDeliver>,
}

#[derive(Debug)]
pub(in crate::runtime_tests) enum TransferProbeMessage {
    /// Completes once the probe has processed every mailbox item that was
    /// queued before this barrier.
    Barrier(KPromise<()>),
}

impl TransferProbe {
    pub(in crate::runtime_tests) fn new(indications: mpsc::Sender<UDPourDeliver>) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            transfer: RequiredPort::uninitialised(),
            indications,
        }
    }
}

ignore_lifecycle!(TransferProbe);

impl Require<UDPourPort> for TransferProbe {
    fn handle(&mut self, indication: UDPourDeliver) -> HandlerResult {
        self.indications
            .send(indication)
            .expect("transfer indication receiver must stay live during tests");
        Handled::OK
    }
}

impl Actor for TransferProbe {
    type Message = TransferProbeMessage;

    fn receive_local(&mut self, msg: Self::Message) -> HandlerResult {
        match msg {
            TransferProbeMessage::Barrier(promise) => {
                let _ = promise.fulfil(());
                Handled::OK
            }
        }
    }
}

#[derive(Debug)]
pub(in crate::runtime_tests) enum ProxyRequestBehavior {
    Pass,
    NackFirstSend {
        reason: SendFailureReason,
        fired: bool,
    },
}

#[derive(Debug)]
pub(in crate::runtime_tests) enum ProxyIndicationBehavior {
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

// TODO(flotsync-wpvk): Replace with a generic scripted port proxy once the
// event-pattern/action API exists.
#[derive(ComponentDefinition)]
pub(in crate::runtime_tests) struct ScriptedUdpProxy {
    ctx: ComponentContext<Self>,
    upstream: RequiredPort<UdpPort>,
    downstream: ProvidedPort<UdpPort>,
    socket_id: SocketId,
    request_behavior: ProxyRequestBehavior,
    indication_behavior: ProxyIndicationBehavior,
}

impl ScriptedUdpProxy {
    pub(in crate::runtime_tests) fn new(
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
                let buffered_parts =
                    u32::try_from(buffered.len()).expect("test payload part count fits into u32");
                if buffered_parts != expected_parts.expect("expected_parts just set") {
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
    fn handle(&mut self, request: UdpRequest) -> HandlerResult {
        match request {
            UdpRequest::Send {
                socket_id,
                transmission_id,
                payload,
                target,
                reply_to,
            } => match &mut self.request_behavior {
                ProxyRequestBehavior::NackFirstSend { reason, fired } if !*fired => {
                    *fired = true;
                    reply_to.tell(UdpSendResult::Nack {
                        socket_id,
                        transmission_id,
                        reason: *reason,
                    });
                }
                ProxyRequestBehavior::Pass | ProxyRequestBehavior::NackFirstSend { .. } => {
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
        Handled::OK
    }
}

impl Require<UdpPort> for ScriptedUdpProxy {
    fn handle(&mut self, indication: UdpIndication) -> HandlerResult {
        for forwarded in self.transform_indication(indication) {
            self.downstream.trigger(forwarded);
        }
        Handled::OK
    }
}

impl Actor for ScriptedUdpProxy {
    type Message = Never;

    fn receive_local(&mut self, _msg: Self::Message) -> HandlerResult {
        unreachable!("Never type is empty")
    }
}
