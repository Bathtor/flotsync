use super::{
    NetcatInput,
    OutcomeSlot,
    Result,
    SCRIPTED_EXIT_GRACE,
    ShutdownTimerState,
    UdpMode,
    encode_line_payload,
    install_input_source,
    new_outcome_slot,
    print_payload,
    record_failure,
    record_success,
    start_component,
    wait_for_component_outcome,
};
use crate::app::ExampleRuntime;
use flotsync_io::prelude::*;
use kompact::prelude::*;
use snafu::prelude::*;
use std::{collections::VecDeque, net::SocketAddr};

/// Runs the UDP flavour of the netcat example.
pub(super) fn run_udp(
    runtime: &ExampleRuntime,
    scripted_lines: Vec<String>,
    exit_after_send: bool,
    mode: UdpMode,
) -> Result<()> {
    let mode = match mode {
        UdpMode::Connect { remote, bind } => UdpNetcatMode::Connected { remote, bind },
        UdpMode::Bind { bind, target } => UdpNetcatMode::Unconnected {
            bind: UdpLocalBind::Exact(bind),
            default_target: target,
        },
        UdpMode::SendTo { target, bind } => UdpNetcatMode::Unconnected {
            bind: bind.map_or(UdpLocalBind::ForPeer(target), UdpLocalBind::Exact),
            default_target: Some(target),
        },
    };
    let shutdown_after_input = exit_after_send;
    let outcome = new_outcome_slot();
    let component = runtime.system().create(|| {
        UdpNetcat::new(
            runtime.bridge_handle().clone(),
            mode,
            shutdown_after_input,
            outcome.clone(),
        )
    });
    match biconnect_components::<UdpPort, _, _>(runtime.bridge_component(), &component) {
        Ok(_) => {}
        Err(error) => {
            whatever!("failed to connect UDP netcat component to IoBridge: {error:?}")
        }
    }
    start_component(runtime, &component)?;
    install_input_source(component.actor_ref(), scripted_lines);
    wait_for_component_outcome(&component, &outcome)
}

/// Static UDP configuration chosen from the CLI.
#[derive(Clone, Copy, Debug)]
enum UdpNetcatMode {
    Connected {
        remote: SocketAddr,
        bind: Option<SocketAddr>,
    },
    Unconnected {
        bind: UdpLocalBind,
        default_target: Option<SocketAddr>,
    },
}

/// Lifecycle of the shared UDP socket used by the example component.
#[derive(Clone, Copy, Debug)]
enum UdpSocketState {
    Reserving,
    Opening(SocketId),
    Ready(SocketId),
    Closing(SocketId),
    Closed,
}

impl UdpSocketState {
    fn socket_id(self) -> Option<SocketId> {
        match self {
            Self::Opening(socket_id) | Self::Ready(socket_id) | Self::Closing(socket_id) => {
                Some(socket_id)
            }
            Self::Reserving | Self::Closed => None,
        }
    }
}

/// Local actor messages understood by the UDP netcat component.
#[derive(Debug)]
enum UdpNetcatMessage {
    Input(NetcatInput),
    SendResult(UdpSendResult),
}

impl From<NetcatInput> for UdpNetcatMessage {
    fn from(value: NetcatInput) -> Self {
        Self::Input(value)
    }
}

impl From<UdpSendResult> for UdpNetcatMessage {
    fn from(value: UdpSendResult) -> Self {
        Self::SendResult(value)
    }
}

/// UDP netcat component.
///
/// This component owns the live UDP socket capability, receives stdin/script input as actor
/// messages, and keeps the send/reply logic entirely inside Kompact-land.
#[derive(ComponentDefinition)]
struct UdpNetcat {
    ctx: ComponentContext<Self>,
    udp: RequiredPort<UdpPort>,
    bridge_handle: IoBridgeHandle,
    mode: UdpNetcatMode,
    socket_state: UdpSocketState,
    last_received_source: Option<SocketAddr>,
    queued_lines: VecDeque<String>,
    input_closed: bool,
    shutdown_after_input: bool,
    next_transmission_id: usize,
    send_pending: bool,
    shutdown_timer: Option<ShutdownTimerState>,
    next_shutdown_timer_generation: usize,
    outcome: OutcomeSlot,
}

impl UdpNetcat {
    fn new(
        bridge_handle: IoBridgeHandle,
        mode: UdpNetcatMode,
        shutdown_after_input: bool,
        outcome: OutcomeSlot,
    ) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            udp: RequiredPort::uninitialised(),
            bridge_handle,
            mode,
            socket_state: UdpSocketState::Reserving,
            last_received_source: None,
            queued_lines: VecDeque::new(),
            input_closed: false,
            shutdown_after_input,
            next_transmission_id: 1,
            send_pending: false,
            shutdown_timer: None,
            next_shutdown_timer_generation: 1,
            outcome,
        }
    }

    fn handle_input(&mut self, input: NetcatInput) -> Handled {
        match input {
            NetcatInput::Line(line) => {
                self.queued_lines.push_back(line);
                self.clear_shutdown_timer();
            }
            NetcatInput::Closed => {
                self.input_closed = true;
            }
        }
        self.start_next_send();
        self.set_shutdown_timer_if_idle();
        Handled::Ok
    }

    fn handle_indication(&mut self, indication: UdpIndication) -> Handled {
        let Some(socket_id) = self.socket_state.socket_id() else {
            return Handled::Ok;
        };

        match indication {
            UdpIndication::Bound {
                socket_id: event_socket_id,
                local_addr,
            } if event_socket_id == socket_id => {
                self.socket_state = UdpSocketState::Ready(socket_id);
                log::info!("UDP bound {local_addr}");
                self.start_next_send();
            }
            UdpIndication::BindFailed {
                socket_id: event_socket_id,
                local_addr,
                reason,
            } if event_socket_id == socket_id => {
                return self.fail_and_die(format!("udp bind to {local_addr} failed: {reason:?}"));
            }
            UdpIndication::Connected {
                socket_id: event_socket_id,
                local_addr,
                remote_addr,
            } if event_socket_id == socket_id => {
                self.socket_state = UdpSocketState::Ready(socket_id);
                log::info!("UDP connected {local_addr} -> {remote_addr}");
                self.start_next_send();
            }
            UdpIndication::ConnectFailed {
                socket_id: event_socket_id,
                local_addr,
                remote_addr,
                reason,
            } if event_socket_id == socket_id => {
                return self.fail_and_die(format!(
                    "UDP connect {:?} -> {remote_addr} failed: {reason:?}",
                    local_addr
                ));
            }
            UdpIndication::Received {
                socket_id: event_socket_id,
                source,
                payload,
            } if event_socket_id == socket_id => {
                self.last_received_source = Some(source);
                log::debug!("UDP recv from {source}");
                print_payload(payload);
            }
            UdpIndication::ReadSuspended {
                socket_id: event_socket_id,
            } if event_socket_id == socket_id => {
                log::debug!("UDP read suspended");
            }
            UdpIndication::ReadResumed {
                socket_id: event_socket_id,
            } if event_socket_id == socket_id => {
                log::debug!("UDP read resumed");
            }
            UdpIndication::WriteSuspended {
                socket_id: event_socket_id,
            } if event_socket_id == socket_id => {
                log::debug!("UDP write suspended");
            }
            UdpIndication::WriteResumed {
                socket_id: event_socket_id,
            } if event_socket_id == socket_id => {
                log::debug!("UDP write resumed");
            }
            UdpIndication::Configured {
                socket_id: event_socket_id,
                ..
            } if event_socket_id == socket_id => {}
            UdpIndication::ConfigureFailed {
                socket_id: event_socket_id,
                ..
            } if event_socket_id == socket_id => {}
            UdpIndication::Closed {
                socket_id: event_socket_id,
                remote_addr,
                reason,
            } if event_socket_id == socket_id => {
                self.socket_state = UdpSocketState::Closed;
                self.send_pending = false;
                match remote_addr {
                    Some(remote_addr) => {
                        log::info!("UDP closed ({reason:?}) for remote {remote_addr}");
                    }
                    None => {
                        log::info!("UDP closed ({reason:?})");
                    }
                }
                return self.finish_and_die();
            }
            _ => {}
        }

        self.set_shutdown_timer_if_idle();
        Handled::Ok
    }

    fn handle_send_result(&mut self, result: UdpSendResult) -> Handled {
        let Some(socket_id) = self.socket_state.socket_id() else {
            return Handled::Ok;
        };

        match result {
            UdpSendResult::Ack {
                socket_id: event_socket_id,
                transmission_id,
            } if event_socket_id == socket_id => {
                self.send_pending = false;
                log::debug!("UDP send ack tx#{:x}", transmission_id.0);
            }
            UdpSendResult::Nack {
                socket_id: event_socket_id,
                transmission_id,
                reason,
            } if event_socket_id == socket_id => {
                self.send_pending = false;
                log::debug!("UDP send nack tx#{:x}: {reason:?}", transmission_id.0);
            }
            _ => {}
        }

        self.start_next_send();
        self.set_shutdown_timer_if_idle();
        Handled::Ok
    }

    fn trigger_open_request(&mut self, socket_id: SocketId) {
        match self.mode {
            UdpNetcatMode::Connected { remote, bind } => {
                self.udp.trigger(UdpRequest::Connect {
                    socket_id,
                    remote_addr: remote,
                    local_addr: bind,
                });
            }
            UdpNetcatMode::Unconnected { bind, .. } => {
                self.udp.trigger(UdpRequest::Bind { socket_id, bind });
            }
        }
    }

    fn start_next_send(&mut self) {
        let UdpSocketState::Ready(socket_id) = self.socket_state else {
            return;
        };
        if self.send_pending {
            return;
        }
        let egress_pool = self.bridge_handle.egress_pool().clone();

        while let Some(line) = self.queued_lines.pop_front() {
            let target = match self.mode {
                UdpNetcatMode::Connected { .. } => None,
                UdpNetcatMode::Unconnected { default_target, .. } => {
                    let Some(target) = default_target.or(self.last_received_source) else {
                        log::debug!(
                            "udp send dropped: no explicit target configured and no peer has sent us a datagram yet"
                        );
                        continue;
                    };
                    Some(target)
                }
            };

            let transmission_id = self.next_transmission_id();
            let reply_to = self
                .actor_ref()
                .recipient_with(UdpNetcatMessage::SendResult);
            self.send_pending = true;
            self.spawn_local(move |mut async_self| async move {
                let payload = match encode_line_payload(egress_pool, line).await {
                    Ok(payload) => payload,
                    Err(error) => {
                        return async_self.fail_and_die(format!(
                            "failed to encode UDP payload from stdin/script input: {error}"
                        ));
                    }
                };
                async_self.udp.trigger(UdpRequest::Send {
                    socket_id,
                    transmission_id,
                    payload,
                    target,
                    reply_to,
                });
                Handled::Ok
            });
            return;
        }
    }

    fn set_shutdown_timer_if_idle(&mut self) {
        if self.should_set_shutdown_timer() {
            if self.shutdown_timer.is_none() {
                let generation = self.next_shutdown_timer_generation;
                self.next_shutdown_timer_generation =
                    self.next_shutdown_timer_generation.wrapping_add(1);
                let timer = self.schedule_once(SCRIPTED_EXIT_GRACE, move |component, _| {
                    component.handle_shutdown_timeout(generation)
                });
                self.shutdown_timer = Some(ShutdownTimerState { generation, timer });
            }
            return;
        }
        self.clear_shutdown_timer();
    }

    fn should_set_shutdown_timer(&self) -> bool {
        self.shutdown_after_input
            && self.input_closed
            && self.queued_lines.is_empty()
            && !self.send_pending
            && matches!(self.socket_state, UdpSocketState::Ready(_))
    }

    fn clear_shutdown_timer(&mut self) {
        if let Some(timer) = self.shutdown_timer.take() {
            self.cancel_timer(timer.timer);
        }
    }

    fn handle_shutdown_timeout(&mut self, generation: usize) -> Handled {
        let Some(timer) = self.shutdown_timer.take() else {
            return Handled::Ok;
        };
        if timer.generation != generation {
            self.shutdown_timer = Some(timer);
            return Handled::Ok;
        }
        if !self.should_set_shutdown_timer() {
            return Handled::Ok;
        }
        let UdpSocketState::Ready(socket_id) = self.socket_state else {
            return Handled::Ok;
        };
        log::debug!("UDP close requested");
        self.udp.trigger(UdpRequest::Close { socket_id });
        self.socket_state = UdpSocketState::Closing(socket_id);
        Handled::Ok
    }

    fn finish_and_die(&mut self) -> Handled {
        self.terminate_success();
        Handled::DieNow
    }

    fn fail_and_die(&mut self, message: String) -> Handled {
        self.terminate_failure(message);
        Handled::DieNow
    }

    fn terminate_success(&mut self) {
        self.clear_shutdown_timer();
        record_success(&self.outcome);
    }

    fn terminate_failure(&mut self, message: String) {
        self.clear_shutdown_timer();
        log::error!("UDP component terminal failure: {message}");
        record_failure(&self.outcome, message);
    }

    fn next_transmission_id(&mut self) -> TransmissionId {
        let id = self.next_transmission_id;
        self.next_transmission_id = self.next_transmission_id.wrapping_add(1);
        TransmissionId(id)
    }
}

impl ComponentLifecycle for UdpNetcat {
    fn on_start(&mut self) -> Handled {
        let reserve = self.bridge_handle.reserve_udp_socket();
        Handled::block_on(self, move |mut async_self| async move {
            let socket_reply = match reserve.await {
                Ok(reply) => reply,
                Err(error) => {
                    return async_self.fail_and_die(format!(
                        "UDP socket reservation dropped before completion: {error}"
                    ));
                }
            };
            let socket_id = match socket_reply {
                Ok(socket_id) => socket_id,
                Err(error) => {
                    return async_self
                        .fail_and_die(format!("failed to reserve UDP socket: {error}"));
                }
            };
            async_self.socket_state = UdpSocketState::Opening(socket_id);
            async_self.trigger_open_request(socket_id);
            Handled::Ok
        })
    }

    fn on_stop(&mut self) -> Handled {
        self.clear_shutdown_timer();
        Handled::Ok
    }

    fn on_kill(&mut self) -> Handled {
        self.clear_shutdown_timer();
        Handled::Ok
    }
}

impl Require<UdpPort> for UdpNetcat {
    fn handle(&mut self, indication: UdpIndication) -> Handled {
        self.handle_indication(indication)
    }
}

impl Actor for UdpNetcat {
    type Message = UdpNetcatMessage;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        match msg {
            UdpNetcatMessage::Input(input) => self.handle_input(input),
            UdpNetcatMessage::SendResult(result) => self.handle_send_result(result),
        }
    }

    fn receive_network(&mut self, _msg: NetMessage) -> Handled {
        unimplemented!("UDP netcat does not use network actor messages")
    }
}
