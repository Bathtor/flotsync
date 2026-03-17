use super::{
    bridge::IoBridgeMessage,
    session::TcpSessionMessage,
    types::{OpenFailureReason, TcpSessionEvent, UdpIndication, UdpSendResult},
};
use crate::{
    api::{
        CloseReason,
        ConnectionId,
        SendFailureReason,
        SocketId,
        TcpCommand,
        TcpEvent,
        UdpCommand,
        UdpEvent,
    },
    driver::{DriverCommand, DriverConfig, DriverEvent, DriverEventSink, IoDriver},
    errors::{Error, Result},
};
use ::kompact::prelude::*;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};

/// Internal mailbox for the shared Kompact driver component.
///
/// This actor is the ownership boundary between the raw driver and all bridge/session components.
/// It allocates raw handles, remembers which Kompact endpoint owns them, and routes raw driver
/// events back to the corresponding bridge or session actor.
#[doc(hidden)]
#[derive(Debug)]
pub enum IoDriverComponentMessage {
    ReserveUdpSocket(Ask<ActorRefStrong<IoBridgeMessage>, Result<SocketId>>),
    ReleaseUdpSocket(Ask<SocketId, Result<()>>),
    OpenTcpSession(Ask<OpenTcpSessionRegistration, Result<ConnectionId>>),
    ReleaseTcpSession(Ask<ConnectionId, Result<()>>),
    DispatchUdp(UdpCommand),
    DispatchTcp(TcpCommand),
    DriverEvent(DriverEvent),
}

/// Registration payload used while opening one outbound TCP session.
///
/// The driver component needs both the future owner of the raw `ConnectionId` and the socket
/// addresses needed to dispatch the actual raw `TcpCommand::Connect`.
#[doc(hidden)]
#[derive(Debug)]
pub struct OpenTcpSessionRegistration {
    pub(crate) session: ActorRefStrong<TcpSessionMessage>,
    pub(crate) remote_addr: SocketAddr,
    pub(crate) local_addr: Option<SocketAddr>,
}

/// Strong local handle for talking to the shared driver component.
///
/// This helper keeps the rest of the Kompact adapter agnostic of the internal actor message enum
/// while still using the fast `ActorRefStrong` path for repeated local control messages.
#[derive(Clone, Debug)]
pub(crate) struct DriverComponentRef {
    actor: ActorRefStrong<IoDriverComponentMessage>,
}

impl DriverComponentRef {
    pub(crate) fn from_component(component: &Arc<Component<IoDriverComponent>>) -> Self {
        let actor = component
            .actor_ref()
            .hold()
            .expect("IoDriverComponent must still be live when wiring a bridge");
        Self { actor }
    }

    pub(crate) fn reserve_udp_socket(
        &self,
        owner: ActorRefStrong<IoBridgeMessage>,
    ) -> KFuture<Result<SocketId>> {
        let (promise, future) = promise::<Result<SocketId>>();
        self.actor
            .tell(IoDriverComponentMessage::ReserveUdpSocket(Ask::new(
                promise, owner,
            )));
        future
    }

    pub(crate) fn release_udp_socket(&self, socket_id: SocketId) -> KFuture<Result<()>> {
        let (promise, future) = promise::<Result<()>>();
        self.actor
            .tell(IoDriverComponentMessage::ReleaseUdpSocket(Ask::new(
                promise, socket_id,
            )));
        future
    }

    pub(crate) fn open_tcp_session(
        &self,
        session: ActorRefStrong<TcpSessionMessage>,
        remote_addr: SocketAddr,
        local_addr: Option<SocketAddr>,
    ) -> KFuture<Result<ConnectionId>> {
        let request = OpenTcpSessionRegistration {
            session,
            remote_addr,
            local_addr,
        };
        let (promise, future) = promise::<Result<ConnectionId>>();
        self.actor
            .tell(IoDriverComponentMessage::OpenTcpSession(Ask::new(
                promise, request,
            )));
        future
    }

    pub(crate) fn release_tcp_session(&self, connection_id: ConnectionId) -> KFuture<Result<()>> {
        let (promise, future) = promise::<Result<()>>();
        self.actor
            .tell(IoDriverComponentMessage::ReleaseTcpSession(Ask::new(
                promise,
                connection_id,
            )));
        future
    }

    pub(crate) fn dispatch_udp(&self, command: UdpCommand) {
        self.actor
            .tell(IoDriverComponentMessage::DispatchUdp(command));
    }

    pub(crate) fn dispatch_tcp(&self, command: TcpCommand) {
        self.actor
            .tell(IoDriverComponentMessage::DispatchTcp(command));
    }
}

/// Adapter that lets the raw driver publish events directly into the Kompact actor world.
///
/// Unit tests still use the channel-backed event sink exposed by the raw driver, while the
/// Kompact integration uses this actor-backed variant so no extra forwarding thread is needed.
#[derive(Debug)]
struct ActorBackedDriverEventSink {
    target: ActorRefStrong<IoDriverComponentMessage>,
}

impl DriverEventSink for ActorBackedDriverEventSink {
    fn publish(&self, event: DriverEvent) -> Result<()> {
        self.target
            .tell(IoDriverComponentMessage::DriverEvent(event));
        Ok(())
    }
}

/// Shared heavy-weight Kompact component that owns one raw `flotsync_io` driver instance.
#[derive(ComponentDefinition)]
pub struct IoDriverComponent {
    ctx: ComponentContext<Self>,
    config: DriverConfig,
    driver: Option<IoDriver>,
    udp_routes: HashMap<SocketId, ActorRefStrong<IoBridgeMessage>>,
    tcp_routes: HashMap<ConnectionId, ActorRefStrong<TcpSessionMessage>>,
}

impl IoDriverComponent {
    /// Creates one shared driver component with the provided raw driver configuration.
    ///
    /// A typical Kompact system creates exactly one instance and then passes it to one or more
    /// [`IoBridge`](super::IoBridge) components.
    pub fn new(config: DriverConfig) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            config,
            driver: None,
            udp_routes: HashMap::new(),
            tcp_routes: HashMap::new(),
        }
    }

    fn driver_ref(&self) -> Result<&IoDriver> {
        self.driver.as_ref().ok_or(Error::DriverUnavailable)
    }

    async fn reserve_udp_socket_for(
        &mut self,
        owner: ActorRefStrong<IoBridgeMessage>,
    ) -> Result<SocketId> {
        let request = {
            let driver = self.driver_ref()?;
            driver.reserve_socket()?
        };
        let socket_id = request.await?;
        self.udp_routes.insert(socket_id, owner);
        Ok(socket_id)
    }

    async fn release_udp_socket_inner(&mut self, socket_id: SocketId) -> Result<()> {
        self.udp_routes.remove(&socket_id);
        let request = {
            let driver = self.driver_ref()?;
            driver.release_socket(socket_id)?
        };
        request.await
    }

    async fn open_tcp_session_inner(
        &mut self,
        session: ActorRefStrong<TcpSessionMessage>,
        remote_addr: SocketAddr,
        local_addr: Option<SocketAddr>,
    ) -> Result<ConnectionId> {
        let reserve_request = {
            let driver = self.driver_ref()?;
            driver.reserve_connection()?
        };
        let connection_id = reserve_request.await?;
        self.tcp_routes.insert(connection_id, session);

        let dispatch_result = {
            let driver = self.driver_ref()?;
            driver.dispatch(DriverCommand::Tcp(TcpCommand::Connect {
                connection_id,
                local_addr,
                remote_addr,
            }))
        };
        if let Err(error) = dispatch_result {
            self.tcp_routes.remove(&connection_id);
            if let Ok(release_request) = self.driver_ref()?.release_connection(connection_id) {
                let _ = release_request.await;
            }
            return Err(error);
        }

        Ok(connection_id)
    }

    async fn release_tcp_session_inner(&mut self, connection_id: ConnectionId) -> Result<()> {
        self.tcp_routes.remove(&connection_id);
        let request = {
            let driver = self.driver_ref()?;
            driver.release_connection(connection_id)?
        };
        request.await
    }

    fn handle_driver_event(&mut self, event: DriverEvent) {
        match event {
            DriverEvent::Udp(event) => self.handle_udp_driver_event(event),
            DriverEvent::Tcp(event) => self.handle_tcp_driver_event(event),
        }
    }

    fn handle_udp_driver_event(&mut self, event: UdpEvent) {
        match event {
            UdpEvent::SendAck {
                socket_id,
                transmission_id,
            } => {
                if let Some(route) = self.udp_routes.get(&socket_id) {
                    route.tell(IoBridgeMessage::UdpSendResult(UdpSendResult::Ack {
                        socket_id,
                        transmission_id,
                    }));
                } else {
                    warn!(
                        self.log(),
                        "dropping UDP send ack for unknown bridge-owned socket {}", socket_id
                    );
                }
            }
            UdpEvent::SendNack {
                socket_id,
                transmission_id,
                reason,
            } => {
                if let Some(route) = self.udp_routes.get(&socket_id) {
                    route.tell(IoBridgeMessage::UdpSendResult(UdpSendResult::Nack {
                        socket_id,
                        transmission_id,
                        reason,
                    }));
                } else {
                    warn!(
                        self.log(),
                        "dropping UDP send nack for unknown bridge-owned socket {}", socket_id
                    );
                }
            }
            other => {
                let (socket_id, indication) = udp_indication_from_raw(other);
                if let Some(route) = self.udp_routes.get(&socket_id) {
                    route.tell(IoBridgeMessage::UdpIndication(indication.clone()));
                } else {
                    warn!(
                        self.log(),
                        "dropping UDP indication for unknown bridge-owned socket {}", socket_id
                    );
                }
                if matches!(indication, UdpIndication::Closed { .. }) {
                    self.udp_routes.remove(&socket_id);
                }
            }
        }
    }

    fn handle_tcp_driver_event(&mut self, event: TcpEvent) {
        let (connection_id, session_event, terminal) = tcp_session_event_from_raw(event);
        if let Some(route) = self.tcp_routes.get(&connection_id) {
            route.tell(TcpSessionMessage::DriverEvent(session_event));
        } else {
            warn!(
                self.log(),
                "dropping TCP session event for unknown routed connection {}", connection_id
            );
        }
        if terminal {
            self.tcp_routes.remove(&connection_id);
        }
    }

    fn handle_dispatch_udp(&mut self, command: UdpCommand) {
        let dispatch_result = self
            .driver
            .as_ref()
            .ok_or(Error::DriverUnavailable)
            .and_then(|driver| driver.dispatch(DriverCommand::Udp(command.clone())));
        if dispatch_result.is_ok() {
            return;
        }

        match command {
            UdpCommand::Bind {
                socket_id,
                local_addr,
            } => self.route_udp_indication(
                socket_id,
                UdpIndication::BindFailed {
                    socket_id,
                    local_addr,
                    reason: OpenFailureReason::DriverUnavailable,
                },
            ),
            UdpCommand::Connect {
                socket_id,
                remote_addr,
                local_addr,
            } => self.route_udp_indication(
                socket_id,
                UdpIndication::ConnectFailed {
                    socket_id,
                    local_addr,
                    remote_addr,
                    reason: OpenFailureReason::DriverUnavailable,
                },
            ),
            UdpCommand::Send {
                socket_id,
                transmission_id,
                ..
            } => self.route_udp_send_result(
                socket_id,
                UdpSendResult::Nack {
                    socket_id,
                    transmission_id,
                    reason: SendFailureReason::DriverUnavailable,
                },
            ),
            UdpCommand::Close { socket_id } => {
                self.route_udp_indication(socket_id, UdpIndication::Closed { socket_id });
                self.udp_routes.remove(&socket_id);
            }
        }
    }

    fn handle_dispatch_tcp(&mut self, command: TcpCommand) {
        let dispatch_result = self
            .driver
            .as_ref()
            .ok_or(Error::DriverUnavailable)
            .and_then(|driver| driver.dispatch(DriverCommand::Tcp(command.clone())));
        if dispatch_result.is_ok() {
            return;
        }

        match command {
            TcpCommand::Connect {
                connection_id,
                local_addr: _,
                remote_addr,
            } => {
                self.route_tcp_session_event(
                    connection_id,
                    TcpSessionEvent::ConnectFailed {
                        remote_addr,
                        reason: OpenFailureReason::DriverUnavailable,
                    },
                );
                self.tcp_routes.remove(&connection_id);
            }
            TcpCommand::Listen { .. } => {}
            TcpCommand::Send {
                connection_id,
                transmission_id,
                ..
            } => self.route_tcp_session_event(
                connection_id,
                TcpSessionEvent::SendNack {
                    transmission_id,
                    reason: SendFailureReason::DriverUnavailable,
                },
            ),
            TcpCommand::Close { connection_id, .. } => {
                self.route_tcp_session_event(
                    connection_id,
                    TcpSessionEvent::Closed {
                        reason: CloseReason::DriverShutdown,
                    },
                );
                self.tcp_routes.remove(&connection_id);
            }
        }
    }

    fn route_udp_indication(&self, socket_id: SocketId, indication: UdpIndication) {
        if let Some(route) = self.udp_routes.get(&socket_id) {
            route.tell(IoBridgeMessage::UdpIndication(indication));
        } else {
            warn!(
                self.log(),
                "dropping UDP indication for unknown bridge-owned socket {}", socket_id
            );
        }
    }

    fn route_udp_send_result(&self, socket_id: SocketId, result: UdpSendResult) {
        if let Some(route) = self.udp_routes.get(&socket_id) {
            route.tell(IoBridgeMessage::UdpSendResult(result));
        } else {
            warn!(
                self.log(),
                "dropping UDP send result for unknown bridge-owned socket {}", socket_id
            );
        }
    }

    fn route_tcp_session_event(&self, connection_id: ConnectionId, event: TcpSessionEvent) {
        if let Some(route) = self.tcp_routes.get(&connection_id) {
            route.tell(TcpSessionMessage::DriverEvent(event));
        } else {
            warn!(
                self.log(),
                "dropping TCP session event for unknown routed connection {}", connection_id
            );
        }
    }

    fn handle_local_message(&mut self, msg: IoDriverComponentMessage) -> Handled {
        match msg {
            IoDriverComponentMessage::ReserveUdpSocket(ask) => self.handle_reserve_udp_socket(ask),
            IoDriverComponentMessage::ReleaseUdpSocket(ask) => self.handle_release_udp_socket(ask),
            IoDriverComponentMessage::OpenTcpSession(ask) => self.handle_open_tcp_session(ask),
            IoDriverComponentMessage::ReleaseTcpSession(ask) => {
                self.handle_release_tcp_session(ask)
            }
            IoDriverComponentMessage::DispatchUdp(command) => {
                self.handle_dispatch_udp(command);
                Handled::Ok
            }
            IoDriverComponentMessage::DispatchTcp(command) => {
                self.handle_dispatch_tcp(command);
                Handled::Ok
            }
            IoDriverComponentMessage::DriverEvent(event) => {
                self.handle_driver_event(event);
                Handled::Ok
            }
        }
    }

    fn handle_reserve_udp_socket(
        &mut self,
        ask: Ask<ActorRefStrong<IoBridgeMessage>, Result<SocketId>>,
    ) -> Handled {
        let (promise, owner) = ask.take();
        Handled::block_on(self, move |mut async_self| async move {
            let reply = async_self.reserve_udp_socket_for(owner).await;
            if promise.fulfil(reply).is_err() {
                debug!(async_self.log(), "dropping UDP socket reservation reply");
            }
        })
    }

    fn handle_release_udp_socket(&mut self, ask: Ask<SocketId, Result<()>>) -> Handled {
        let (promise, socket_id) = ask.take();
        Handled::block_on(self, move |mut async_self| async move {
            let reply = async_self.release_udp_socket_inner(socket_id).await;
            if promise.fulfil(reply).is_err() {
                debug!(async_self.log(), "dropping UDP socket release reply");
            }
        })
    }

    fn handle_open_tcp_session(
        &mut self,
        ask: Ask<OpenTcpSessionRegistration, Result<ConnectionId>>,
    ) -> Handled {
        let (promise, request) = ask.take();
        Handled::block_on(self, move |mut async_self| async move {
            let reply = async_self
                .open_tcp_session_inner(request.session, request.remote_addr, request.local_addr)
                .await;
            if promise.fulfil(reply).is_err() {
                debug!(async_self.log(), "dropping TCP session open reply");
            }
        })
    }

    fn handle_release_tcp_session(&mut self, ask: Ask<ConnectionId, Result<()>>) -> Handled {
        let (promise, connection_id) = ask.take();
        Handled::block_on(self, move |mut async_self| async move {
            let reply = async_self.release_tcp_session_inner(connection_id).await;
            if promise.fulfil(reply).is_err() {
                debug!(async_self.log(), "dropping TCP session release reply");
            }
        })
    }
}

impl ComponentLifecycle for IoDriverComponent {
    fn on_start(&mut self) -> Handled {
        let actor = self
            .actor_ref()
            .hold()
            .expect("IoDriverComponent must be live during startup");
        let sink: Arc<dyn DriverEventSink> = Arc::new(ActorBackedDriverEventSink { target: actor });
        match IoDriver::start_with_event_sink(self.config.clone(), sink) {
            Ok(driver) => {
                self.driver = Some(driver);
                Handled::Ok
            }
            Err(error) => {
                error!(
                    self.log(),
                    "failed to start flotsync_io driver component: {}", error
                );
                Handled::DieNow
            }
        }
    }

    fn on_stop(&mut self) -> Handled {
        shutdown_driver_component(self)
    }

    fn on_kill(&mut self) -> Handled {
        shutdown_driver_component(self)
    }
}

impl Actor for IoDriverComponent {
    type Message = IoDriverComponentMessage;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        self.handle_local_message(msg)
    }

    fn receive_network(&mut self, _msg: NetMessage) -> Handled {
        unimplemented!("flotsync_io Kompact integration does not use network actor messages");
    }
}

fn shutdown_driver_component(component: &mut IoDriverComponent) -> Handled {
    let Some(driver) = component.driver.take() else {
        return Handled::Ok;
    };
    component.udp_routes.clear();
    component.tcp_routes.clear();
    let shutdown = component.spawn_off(async move { driver.shutdown() });
    Handled::block_on(component, move |async_self| async move {
        match shutdown.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                error!(
                    async_self.log(),
                    "failed to stop flotsync_io driver component cleanly: {}", error
                );
            }
            Err(error) => {
                error!(async_self.log(), "driver shutdown task failed: {}", error);
            }
        }
    })
}

fn udp_indication_from_raw(event: UdpEvent) -> (SocketId, UdpIndication) {
    match event {
        UdpEvent::Bound {
            socket_id,
            local_addr,
        } => (
            socket_id,
            UdpIndication::Bound {
                socket_id,
                local_addr,
            },
        ),
        UdpEvent::BindFailed {
            socket_id,
            local_addr,
            error_kind,
        } => (
            socket_id,
            UdpIndication::BindFailed {
                socket_id,
                local_addr,
                reason: OpenFailureReason::Io(error_kind),
            },
        ),
        UdpEvent::Connected {
            socket_id,
            local_addr,
            remote_addr,
        } => (
            socket_id,
            UdpIndication::Connected {
                socket_id,
                local_addr,
                remote_addr,
            },
        ),
        UdpEvent::ConnectFailed {
            socket_id,
            local_addr,
            remote_addr,
            error_kind,
        } => (
            socket_id,
            UdpIndication::ConnectFailed {
                socket_id,
                local_addr,
                remote_addr,
                reason: OpenFailureReason::Io(error_kind),
            },
        ),
        UdpEvent::Received {
            socket_id,
            source,
            payload,
        } => (
            socket_id,
            UdpIndication::Received {
                socket_id,
                source,
                payload,
            },
        ),
        UdpEvent::ReadSuspended { socket_id } => {
            (socket_id, UdpIndication::ReadSuspended { socket_id })
        }
        UdpEvent::ReadResumed { socket_id } => {
            (socket_id, UdpIndication::ReadResumed { socket_id })
        }
        UdpEvent::WriteSuspended { socket_id } => {
            (socket_id, UdpIndication::WriteSuspended { socket_id })
        }
        UdpEvent::WriteResumed { socket_id } => {
            (socket_id, UdpIndication::WriteResumed { socket_id })
        }
        UdpEvent::Closed { socket_id } => (socket_id, UdpIndication::Closed { socket_id }),
        UdpEvent::SendAck { .. } | UdpEvent::SendNack { .. } => {
            unreachable!("send outcomes are handled separately")
        }
    }
}

fn tcp_session_event_from_raw(event: TcpEvent) -> (ConnectionId, TcpSessionEvent, bool) {
    match event {
        TcpEvent::Connected {
            connection_id,
            peer_addr,
        } => (
            connection_id,
            TcpSessionEvent::Connected { peer_addr },
            false,
        ),
        TcpEvent::ConnectFailed {
            connection_id,
            remote_addr,
            error_kind,
        } => (
            connection_id,
            TcpSessionEvent::ConnectFailed {
                remote_addr,
                reason: OpenFailureReason::Io(error_kind),
            },
            true,
        ),
        TcpEvent::Received {
            connection_id,
            payload,
        } => (connection_id, TcpSessionEvent::Received { payload }, false),
        TcpEvent::SendAck {
            connection_id,
            transmission_id,
        } => (
            connection_id,
            TcpSessionEvent::SendAck { transmission_id },
            false,
        ),
        TcpEvent::SendNack {
            connection_id,
            transmission_id,
            reason,
        } => (
            connection_id,
            TcpSessionEvent::SendNack {
                transmission_id,
                reason,
            },
            false,
        ),
        TcpEvent::ReadSuspended { connection_id } => {
            (connection_id, TcpSessionEvent::ReadSuspended, false)
        }
        TcpEvent::ReadResumed { connection_id } => {
            (connection_id, TcpSessionEvent::ReadResumed, false)
        }
        TcpEvent::WriteSuspended { connection_id } => {
            (connection_id, TcpSessionEvent::WriteSuspended, false)
        }
        TcpEvent::WriteResumed { connection_id } => {
            (connection_id, TcpSessionEvent::WriteResumed, false)
        }
        TcpEvent::Closed {
            connection_id,
            reason,
        } => (connection_id, TcpSessionEvent::Closed { reason }, true),
        TcpEvent::Listening { listener_id, .. } | TcpEvent::Accepted { listener_id, .. } => {
            unreachable!(
                "listener-related TCP events must not be routed through the TCP session adapter for {}",
                listener_id
            )
        }
    }
}
