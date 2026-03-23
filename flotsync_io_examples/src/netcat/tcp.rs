use super::{
    NetcatInput,
    OutcomePromise,
    Result,
    SCRIPTED_EXIT_GRACE,
    ShutdownTimerState,
    TcpMode,
    complete_outcome,
    encode_line_payload,
    install_input_source,
    new_outcome_promise,
    print_payload,
    start_component,
    wait_for_component_outcome,
};
use crate::app::ExampleRuntime;
use flotsync_io::prelude::*;
use kompact::prelude::*;
use snafu::{FromString, Whatever};
use std::{collections::VecDeque, net::SocketAddr};

/// Runs the TCP flavour of the netcat example.
pub(super) fn run_tcp(
    runtime: &ExampleRuntime,
    scripted_lines: Vec<String>,
    exit_after_send: bool,
    mode: TcpMode,
) -> Result<()> {
    let shutdown_after_input = exit_after_send;
    match mode {
        TcpMode::Connect { remote, bind } => run_tcp_connect(
            runtime,
            scripted_lines,
            shutdown_after_input,
            TcpConnectConfig { remote, bind },
        ),
        TcpMode::Listen { bind } => run_tcp_listen(
            runtime,
            scripted_lines,
            shutdown_after_input,
            TcpListenConfig { bind },
        ),
    }
}

fn run_tcp_connect(
    runtime: &ExampleRuntime,
    scripted_lines: Vec<String>,
    shutdown_after_input: bool,
    config: TcpConnectConfig,
) -> Result<()> {
    let (outcome_promise, outcome_future) = new_outcome_promise();
    let bridge_handle = runtime.bridge_handle().clone();
    let component = runtime.system().create(move || {
        TcpConnectNetcat::new(bridge_handle, config, shutdown_after_input, outcome_promise)
    });
    start_component(runtime, &component)?;
    install_input_source(component.actor_ref(), scripted_lines);
    wait_for_component_outcome(&component, outcome_future, "netcat")
}

fn run_tcp_listen(
    runtime: &ExampleRuntime,
    scripted_lines: Vec<String>,
    shutdown_after_input: bool,
    config: TcpListenConfig,
) -> Result<()> {
    let (outcome_promise, outcome_future) = new_outcome_promise();
    let bridge_handle = runtime.bridge_handle().clone();
    let component = runtime.system().create(move || {
        TcpListenNetcat::new(bridge_handle, config, shutdown_after_input, outcome_promise)
    });
    start_component(runtime, &component)?;
    install_input_source(component.actor_ref(), scripted_lines);
    wait_for_component_outcome(&component, outcome_future, "netcat")
}

/// Static configuration for one outbound TCP session example.
#[derive(Clone, Copy, Debug)]
struct TcpConnectConfig {
    remote: SocketAddr,
    bind: Option<SocketAddr>,
}

/// Static configuration for one TCP listener example.
#[derive(Clone, Copy, Debug)]
struct TcpListenConfig {
    bind: SocketAddr,
}

/// Lifecycle of one outbound TCP session owned by the example.
#[derive(Clone, Debug)]
enum TcpConnectState {
    Ready(TcpSessionRef),
    Closing(TcpSessionRef),
    Closed,
}

impl TcpConnectState {
    fn session(&self) -> Option<&TcpSessionRef> {
        match self {
            Self::Ready(session) | Self::Closing(session) => Some(session),
            Self::Closed => None,
        }
    }
}

/// Lifecycle of the listener endpoint in listener mode.
#[derive(Clone, Debug)]
enum TcpListenerState {
    Ready(TcpListenerRef),
    Closing(TcpListenerRef),
    Closed,
}

impl TcpListenerState {
    fn listener(&self) -> Option<&TcpListenerRef> {
        match self {
            Self::Ready(listener) | Self::Closing(listener) => Some(listener),
            Self::Closed => None,
        }
    }
}

/// Lifecycle of the currently active accepted session in listener mode.
#[derive(Clone, Debug)]
enum TcpAcceptedSessionState {
    None,
    Accepting,
    Open(TcpSessionRef),
    Closing(TcpSessionRef),
}

impl TcpAcceptedSessionState {
    fn session(&self) -> Option<&TcpSessionRef> {
        match self {
            Self::Open(session) | Self::Closing(session) => Some(session),
            Self::None | Self::Accepting => None,
        }
    }
}

/// Local actor messages for the outbound TCP component.
#[derive(Debug)]
enum TcpConnectMessage {
    Input(NetcatInput),
    SessionEvent(TcpSessionEvent),
}

impl From<NetcatInput> for TcpConnectMessage {
    fn from(value: NetcatInput) -> Self {
        Self::Input(value)
    }
}

impl From<TcpSessionEvent> for TcpConnectMessage {
    fn from(value: TcpSessionEvent) -> Self {
        Self::SessionEvent(value)
    }
}

/// Local actor messages for the TCP listener component.
#[derive(Debug)]
enum TcpListenMessage {
    Input(NetcatInput),
    SessionEvent(TcpSessionEvent),
    ListenerEvent(TcpListenerEvent),
}

impl From<NetcatInput> for TcpListenMessage {
    fn from(value: NetcatInput) -> Self {
        Self::Input(value)
    }
}

impl From<TcpSessionEvent> for TcpListenMessage {
    fn from(value: TcpSessionEvent) -> Self {
        Self::SessionEvent(value)
    }
}

impl From<TcpListenerEvent> for TcpListenMessage {
    fn from(value: TcpListenerEvent) -> Self {
        Self::ListenerEvent(value)
    }
}

/// Component that manages one outbound TCP session.
#[derive(ComponentDefinition)]
struct TcpConnectNetcat {
    ctx: ComponentContext<Self>,
    bridge_handle: IoBridgeHandle,
    config: TcpConnectConfig,
    state: TcpConnectState,
    queued_lines: VecDeque<String>,
    input_closed: bool,
    shutdown_after_input: bool,
    pending_send: bool,
    next_transmission_id: TransmissionId,
    shutdown_timer: Option<ShutdownTimerState>,
    next_shutdown_timer_generation: usize,
    outcome: Option<OutcomePromise>,
}

impl TcpConnectNetcat {
    fn new(
        bridge_handle: IoBridgeHandle,
        config: TcpConnectConfig,
        shutdown_after_input: bool,
        outcome: OutcomePromise,
    ) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            bridge_handle,
            config,
            state: TcpConnectState::Closed,
            queued_lines: VecDeque::new(),
            input_closed: false,
            shutdown_after_input,
            pending_send: false,
            next_transmission_id: TransmissionId::ONE,
            shutdown_timer: None,
            next_shutdown_timer_generation: 1,
            outcome: Some(outcome),
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

    fn handle_session_event(&mut self, event: TcpSessionEvent) -> Handled {
        match event {
            TcpSessionEvent::Received { payload } => {
                log::debug!("TCP recv");
                print_payload(payload);
            }
            TcpSessionEvent::SendAck { transmission_id } => {
                self.pending_send = false;
                log::debug!("TCP send ack tx#{:x}", transmission_id.0);
                self.start_next_send();
            }
            TcpSessionEvent::SendNack {
                transmission_id,
                reason,
            } => {
                self.pending_send = false;
                log::debug!("TCP send nack tx#{:x}: {reason:?}", transmission_id.0);
                self.start_next_send();
            }
            TcpSessionEvent::ReadSuspended => {
                log::debug!("TCP read suspended");
            }
            TcpSessionEvent::ReadResumed => {
                log::debug!("TCP read resumed");
            }
            TcpSessionEvent::WriteSuspended => {
                log::debug!("TCP write suspended");
            }
            TcpSessionEvent::WriteResumed => {
                log::debug!("TCP write resumed");
            }
            TcpSessionEvent::Closed { reason } => {
                self.state = TcpConnectState::Closed;
                self.pending_send = false;
                log::info!("TCP session closed: {reason:?}");
                return self.finish_and_die();
            }
        }

        self.set_shutdown_timer_if_idle();
        Handled::Ok
    }

    fn start_next_send(&mut self) {
        let TcpConnectState::Ready(session) = &self.state else {
            return;
        };
        if self.pending_send {
            return;
        }
        let egress_pool = self.bridge_handle.egress_pool().clone();
        let Some(line) = self.queued_lines.pop_front() else {
            return;
        };

        // Need to mutate through `self`, so need to get rid of the ref.
        let session = session.clone();
        let transmission_id = self.next_transmission_id.take_next();
        self.pending_send = true;
        self.spawn_local(move |mut async_self| async move {
            let payload = match encode_line_payload(egress_pool, line).await {
                Ok(payload) => payload,
                Err(error) => {
                    return async_self.fail_and_die(format!(
                        "failed to encode TCP payload from stdin/script input: {error}"
                    ));
                }
            };
            session.tell(TcpSessionRequest::Send {
                transmission_id,
                payload,
            });
            Handled::Ok
        });
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
            && !self.pending_send
            && matches!(self.state, TcpConnectState::Ready(_))
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
        let Some(session) = self.state.session().cloned() else {
            return Handled::Ok;
        };
        session.tell(TcpSessionRequest::Close { abort: false });
        self.state = TcpConnectState::Closing(session);
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
        complete_outcome(&mut self.outcome, Ok(()));
    }

    fn terminate_failure(&mut self, message: String) {
        self.clear_shutdown_timer();
        complete_outcome(&mut self.outcome, Err(Whatever::without_source(message)));
    }
}

impl ComponentLifecycle for TcpConnectNetcat {
    fn on_start(&mut self) -> Handled {
        let open = self.bridge_handle.open_tcp_session(OpenTcpSession {
            remote_addr: self.config.remote,
            local_addr: self.config.bind,
            events_to: self
                .actor_ref()
                .recipient_with(TcpConnectMessage::SessionEvent),
        });
        Handled::block_on(self, move |mut async_self| async move {
            let session_reply = match open.await {
                Ok(reply) => reply,
                Err(error) => {
                    return async_self.fail_and_die(format!(
                        "TCP session open reply dropped before completion: {error}"
                    ));
                }
            };
            match session_reply {
                Ok(opened) => {
                    log::info!("TCP connected to {}", opened.peer_addr);
                    async_self.state = TcpConnectState::Ready(opened.session);
                    async_self.start_next_send();
                    Handled::Ok
                }
                Err(error) => {
                    return async_self
                        .fail_and_die(format!("failed to open TCP session: {error:?}"));
                }
            }
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

impl Actor for TcpConnectNetcat {
    type Message = TcpConnectMessage;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        match msg {
            TcpConnectMessage::Input(input) => self.handle_input(input),
            TcpConnectMessage::SessionEvent(event) => self.handle_session_event(event),
        }
    }

    fn receive_network(&mut self, _msg: NetMessage) -> Handled {
        unimplemented!("TCP connect netcat does not use network actor messages")
    }
}

/// Component that manages one TCP listener and one active accepted session at a time.
#[derive(ComponentDefinition)]
struct TcpListenNetcat {
    ctx: ComponentContext<Self>,
    bridge_handle: IoBridgeHandle,
    config: TcpListenConfig,
    listener_state: TcpListenerState,
    session_state: TcpAcceptedSessionState,
    queued_lines: VecDeque<String>,
    input_closed: bool,
    shutdown_after_input: bool,
    pending_send: bool,
    next_transmission_id: TransmissionId,
    shutdown_timer: Option<ShutdownTimerState>,
    next_shutdown_timer_generation: usize,
    outcome: Option<OutcomePromise>,
}

impl TcpListenNetcat {
    fn new(
        bridge_handle: IoBridgeHandle,
        config: TcpListenConfig,
        shutdown_after_input: bool,
        outcome: OutcomePromise,
    ) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            bridge_handle,
            config,
            listener_state: TcpListenerState::Closed,
            session_state: TcpAcceptedSessionState::None,
            queued_lines: VecDeque::new(),
            input_closed: false,
            shutdown_after_input,
            pending_send: false,
            next_transmission_id: TransmissionId::ONE,
            shutdown_timer: None,
            next_shutdown_timer_generation: 1,
            outcome: Some(outcome),
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

    fn handle_listener_event(&mut self, event: TcpListenerEvent) -> Handled {
        match event {
            TcpListenerEvent::Incoming { peer_addr, pending } => {
                self.handle_pending_session(peer_addr, pending)
            }
            TcpListenerEvent::Closed => {
                self.listener_state = TcpListenerState::Closed;
                log::info!("TCP listener closed");
                self.finish_if_terminal()
            }
        }
    }

    fn handle_session_event(&mut self, event: TcpSessionEvent) -> Handled {
        match event {
            TcpSessionEvent::Received { payload } => {
                log::debug!("TCP recv");
                print_payload(payload);
            }
            TcpSessionEvent::SendAck { transmission_id } => {
                self.pending_send = false;
                log::debug!("TCP send ack tx#{:x}", transmission_id.0);
                self.start_next_send();
            }
            TcpSessionEvent::SendNack {
                transmission_id,
                reason,
            } => {
                self.pending_send = false;
                log::debug!("TCP send nack tx#{:x}: {reason:?}", transmission_id.0);
                self.start_next_send();
            }
            TcpSessionEvent::ReadSuspended => {
                log::debug!("TCP read suspended");
            }
            TcpSessionEvent::ReadResumed => {
                log::debug!("TCP read resumed");
            }
            TcpSessionEvent::WriteSuspended => {
                log::debug!("TCP write suspended");
            }
            TcpSessionEvent::WriteResumed => {
                log::debug!("TCP write resumed");
            }
            TcpSessionEvent::Closed { reason } => {
                self.session_state = TcpAcceptedSessionState::None;
                self.pending_send = false;
                log::info!("TCP session closed: {reason:?}");
                return self.finish_if_terminal();
            }
        }

        self.set_shutdown_timer_if_idle();
        Handled::Ok
    }

    fn handle_pending_session(
        &mut self,
        peer_addr: SocketAddr,
        pending: PendingTcpSession,
    ) -> Handled {
        if !matches!(self.session_state, TcpAcceptedSessionState::None)
            || matches!(
                self.listener_state,
                TcpListenerState::Closing(_) | TcpListenerState::Closed
            )
        {
            log::info!("rejecting extra inbound TCP session from {peer_addr}");
            self.spawn_local(move |mut async_self| async move {
                let reject_reply = match pending.reject().await {
                    Ok(reply) => reply,
                    Err(error) => {
                        async_self.terminate_failure(format!(
                            "TCP pending-session reject reply dropped before completion: {error}"
                        ));
                        return Handled::Ok;
                    }
                };
                if let Err(error) = reject_reply {
                    return async_self
                        .fail_and_die(format!("failed to reject inbound TCP session: {error}"));
                }
                Handled::Ok
            });
            return Handled::Ok;
        }

        self.session_state = TcpAcceptedSessionState::Accepting;
        log::info!("accepting inbound TCP session from {peer_addr}");
        Handled::block_on(self, move |mut async_self| async move {
            let accept_reply = match pending
                .accept(
                    async_self
                        .actor_ref()
                        .recipient_with(TcpListenMessage::SessionEvent),
                )
                .await
            {
                Ok(reply) => reply,
                Err(error) => {
                    return async_self.fail_and_die(format!(
                        "TCP pending-session accept reply dropped before completion: {error}"
                    ));
                }
            };
            match accept_reply {
                Ok(session) => {
                    async_self.session_state = TcpAcceptedSessionState::Open(session);
                    async_self.start_next_send();
                    async_self.set_shutdown_timer_if_idle();
                    Handled::Ok
                }
                Err(error) => async_self
                    .fail_and_die(format!("failed to accept inbound TCP session: {error}")),
            }
        })
    }

    fn start_next_send(&mut self) {
        let TcpAcceptedSessionState::Open(session) = &self.session_state else {
            return;
        };
        if self.pending_send {
            return;
        }
        let egress_pool = self.bridge_handle.egress_pool().clone();
        let Some(line) = self.queued_lines.pop_front() else {
            return;
        };

        let session = session.clone();
        let transmission_id = self.next_transmission_id.take_next();
        self.pending_send = true;
        self.spawn_local(move |mut async_self| async move {
            let payload = match encode_line_payload(egress_pool, line).await {
                Ok(payload) => payload,
                Err(error) => {
                    return async_self.fail_and_die(format!(
                        "failed to encode TCP payload from stdin/script input: {error}"
                    ));
                }
            };
            session.tell(TcpSessionRequest::Send {
                transmission_id,
                payload,
            });
            Handled::Ok
        });
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
            && !self.pending_send
            && !matches!(self.session_state, TcpAcceptedSessionState::Accepting)
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

        if let Some(session) = self.session_state.session().cloned() {
            session.tell(TcpSessionRequest::Close { abort: false });
            self.session_state = TcpAcceptedSessionState::Closing(session);
            return Handled::Ok;
        }

        if let Some(listener) = self.listener_state.listener().cloned() {
            listener.tell(TcpListenerRequest::Close);
            self.listener_state = TcpListenerState::Closing(listener);
            return Handled::Ok;
        }

        self.finish_if_terminal()
    }

    fn finish_if_terminal(&mut self) -> Handled {
        self.set_shutdown_timer_if_idle();
        if matches!(self.listener_state, TcpListenerState::Closed)
            && matches!(self.session_state, TcpAcceptedSessionState::None)
        {
            return self.finish_and_die();
        }
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
        complete_outcome(&mut self.outcome, Ok(()));
    }

    fn terminate_failure(&mut self, message: String) {
        self.clear_shutdown_timer();
        complete_outcome(&mut self.outcome, Err(Whatever::without_source(message)));
    }
}

impl ComponentLifecycle for TcpListenNetcat {
    fn on_start(&mut self) -> Handled {
        let open = self.bridge_handle.open_tcp_listener(OpenTcpListener {
            local_addr: self.config.bind,
            incoming_to: self
                .actor_ref()
                .recipient_with(TcpListenMessage::ListenerEvent),
        });
        Handled::block_on(self, move |mut async_self| async move {
            let listener_reply = match open.await {
                Ok(reply) => reply,
                Err(error) => {
                    return async_self.fail_and_die(format!(
                        "TCP listener open reply dropped before completion: {error}"
                    ));
                }
            };
            match listener_reply {
                Ok(opened) => {
                    log::info!("TCP listening on {}", opened.local_addr);
                    async_self.listener_state = TcpListenerState::Ready(opened.listener);
                }
                Err(error) => {
                    return async_self
                        .fail_and_die(format!("failed to open TCP listener: {error:?}"));
                }
            }
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

impl Actor for TcpListenNetcat {
    type Message = TcpListenMessage;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        match msg {
            TcpListenMessage::Input(input) => self.handle_input(input),
            TcpListenMessage::SessionEvent(event) => self.handle_session_event(event),
            TcpListenMessage::ListenerEvent(event) => self.handle_listener_event(event),
        }
    }

    fn receive_network(&mut self, _msg: NetMessage) -> Handled {
        unimplemented!("TCP listener netcat does not use network actor messages")
    }
}
