use super::{
    bridge::IoBridgeMessage,
    listener::{TcpListenerDriverEvent, TcpListenerMessage},
    session::TcpSessionMessage,
    types::{
        ConfigureFailureReason,
        OpenFailureReason,
        TcpSessionEvent,
        UdpIndication,
        UdpSendResult,
    },
};
use crate::{
    api::{
        CloseReason,
        ConnectionId,
        ListenerId,
        SendFailureReason,
        SocketId,
        TcpCommand,
        TcpEvent,
        UdpCloseReason,
        UdpCommand,
        UdpEvent,
    },
    driver::{DriverCommand, DriverConfig, DriverEvent, DriverEventSink, IoDriver},
    errors::{Error, Result},
    logging::erased_runtime_logger,
    pool::{EgressPool, IoBufferPools},
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
    OpenTcpListener(Ask<OpenTcpListenerRegistration, Result<ListenerId>>),
    ReleaseTcpListener(Ask<ListenerId, Result<()>>),
    OpenTcpSession(Ask<OpenTcpSessionRegistration, Result<ConnectionId>>),
    ReleaseTcpSession(Ask<ConnectionId, Result<()>>),
    AdoptAcceptedTcpSession(Ask<AdoptAcceptedTcpSessionRegistration, Result<()>>),
    RejectPendingTcpSession(Ask<ConnectionId, Result<()>>),
    DispatchUdp(UdpCommand),
    DispatchTcp(TcpCommand),
    DriverEvent(DriverEvent),
}

/// Registration payload used while opening one inbound TCP listener.
#[doc(hidden)]
#[derive(Debug)]
pub struct OpenTcpListenerRegistration {
    pub(crate) listener: ActorRefStrong<TcpListenerMessage>,
    pub(crate) local_addr: SocketAddr,
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

/// Registration payload used while adopting one previously accepted inbound TCP connection.
#[doc(hidden)]
#[derive(Debug)]
pub struct AdoptAcceptedTcpSessionRegistration {
    pub(crate) connection_id: ConnectionId,
    pub(crate) session: ActorRefStrong<TcpSessionMessage>,
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

    pub(crate) fn open_tcp_listener(
        &self,
        listener: ActorRefStrong<TcpListenerMessage>,
        local_addr: SocketAddr,
    ) -> KFuture<Result<ListenerId>> {
        let request = OpenTcpListenerRegistration {
            listener,
            local_addr,
        };
        let (promise, future) = promise::<Result<ListenerId>>();
        self.actor
            .tell(IoDriverComponentMessage::OpenTcpListener(Ask::new(
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

    pub(crate) fn release_tcp_listener(&self, listener_id: ListenerId) -> KFuture<Result<()>> {
        let (promise, future) = promise::<Result<()>>();
        self.actor
            .tell(IoDriverComponentMessage::ReleaseTcpListener(Ask::new(
                promise,
                listener_id,
            )));
        future
    }

    pub(crate) fn adopt_accepted_tcp_session(
        &self,
        session: ActorRefStrong<TcpSessionMessage>,
        connection_id: ConnectionId,
    ) -> KFuture<Result<()>> {
        let request = AdoptAcceptedTcpSessionRegistration {
            connection_id,
            session,
        };
        let (promise, future) = promise::<Result<()>>();
        self.actor
            .tell(IoDriverComponentMessage::AdoptAcceptedTcpSession(Ask::new(
                promise, request,
            )));
        future
    }

    pub(crate) fn reject_pending_tcp_session(
        &self,
        connection_id: ConnectionId,
    ) -> KFuture<Result<()>> {
        let (promise, future) = promise::<Result<()>>();
        self.actor
            .tell(IoDriverComponentMessage::RejectPendingTcpSession(Ask::new(
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
    buffers: IoBufferPools,
    driver: Option<IoDriver>,
    udp_routes: HashMap<SocketId, ActorRefStrong<IoBridgeMessage>>,
    tcp_listener_routes: HashMap<ListenerId, ActorRefStrong<TcpListenerMessage>>,
    tcp_routes: HashMap<ConnectionId, ActorRefStrong<TcpSessionMessage>>,
}

impl IoDriverComponent {
    /// Creates one shared driver component with the provided raw driver configuration.
    ///
    /// A typical Kompact system creates exactly one instance and then passes it to one or more
    /// [`IoBridge`](super::IoBridge) components.
    pub fn new(config: DriverConfig) -> Self {
        Self::try_new(config).expect("IoDriverComponent buffer configuration must be valid")
    }

    pub fn try_new(config: DriverConfig) -> Result<Self> {
        let buffers = IoBufferPools::new(config.buffer_config.clone())?;
        Ok(Self {
            ctx: ComponentContext::uninitialised(),
            config,
            buffers,
            driver: None,
            udp_routes: HashMap::new(),
            tcp_listener_routes: HashMap::new(),
            tcp_routes: HashMap::new(),
        })
    }

    pub(crate) fn egress_pool(&self) -> EgressPool {
        self.buffers.egress()
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

    async fn open_tcp_listener_inner(
        &mut self,
        listener: ActorRefStrong<TcpListenerMessage>,
        local_addr: SocketAddr,
    ) -> Result<ListenerId> {
        let reserve_request = {
            let driver = self.driver_ref()?;
            driver.reserve_listener()?
        };
        let listener_id = reserve_request.await?;
        self.tcp_listener_routes.insert(listener_id, listener);

        let dispatch_result = {
            let driver = self.driver_ref()?;
            driver.dispatch(DriverCommand::Tcp(TcpCommand::Listen {
                listener_id,
                local_addr,
            }))
        };
        if let Err(error) = dispatch_result {
            self.tcp_listener_routes.remove(&listener_id);
            if let Ok(release_request) = self.driver_ref()?.release_listener(listener_id) {
                let _ = release_request.await;
            }
            return Err(error);
        }

        Ok(listener_id)
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

    async fn adopt_accepted_tcp_session_inner(
        &mut self,
        session: ActorRefStrong<TcpSessionMessage>,
        connection_id: ConnectionId,
    ) -> Result<()> {
        self.tcp_routes.insert(connection_id, session);

        let dispatch_result = {
            let driver = self.driver_ref()?;
            driver.dispatch(DriverCommand::Tcp(TcpCommand::AdoptAccepted {
                connection_id,
            }))
        };
        if let Err(error) = dispatch_result {
            self.tcp_routes.remove(&connection_id);
            if let Ok(release_request) = self.driver_ref()?.release_connection(connection_id) {
                let _ = release_request.await;
            }
            return Err(error);
        }

        Ok(())
    }

    async fn reject_pending_tcp_session_inner(
        &mut self,
        connection_id: ConnectionId,
    ) -> Result<()> {
        let request = {
            let driver = self.driver_ref()?;
            driver.release_connection(connection_id)?
        };
        request.await
    }

    async fn release_tcp_listener_inner(&mut self, listener_id: ListenerId) -> Result<()> {
        self.tcp_listener_routes.remove(&listener_id);
        let request = {
            let driver = self.driver_ref()?;
            driver.release_listener(listener_id)?
        };
        request.await
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
        match event {
            TcpEvent::Listening {
                listener_id,
                local_addr,
            } => {
                self.route_tcp_listener_event(
                    listener_id,
                    TcpListenerDriverEvent::Listening { local_addr },
                );
            }
            TcpEvent::ListenFailed {
                listener_id,
                local_addr,
                error_kind,
            } => {
                self.route_tcp_listener_event(
                    listener_id,
                    TcpListenerDriverEvent::ListenFailed {
                        local_addr,
                        reason: OpenFailureReason::Io(error_kind),
                    },
                );
                self.tcp_listener_routes.remove(&listener_id);
            }
            TcpEvent::Accepted {
                listener_id,
                connection_id,
                peer_addr,
            } => {
                self.route_tcp_listener_incoming(listener_id, connection_id, peer_addr);
            }
            TcpEvent::ListenerClosed { listener_id } => {
                self.route_tcp_listener_event(listener_id, TcpListenerDriverEvent::Closed);
                self.tcp_listener_routes.remove(&listener_id);
            }
            other => {
                let (connection_id, session_event, terminal) = tcp_session_event_from_raw(other);
                if let Some(route) = self.tcp_routes.get(&connection_id) {
                    route.tell(TcpSessionMessage::DriverEvent(session_event));
                } else {
                    warn!(
                        self.log(),
                        "dropping TCP session event for unknown routed connection {}",
                        connection_id
                    );
                }
                if terminal {
                    self.tcp_routes.remove(&connection_id);
                }
            }
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
            UdpCommand::Bind { socket_id, bind } => self.route_udp_indication(
                socket_id,
                UdpIndication::BindFailed {
                    socket_id,
                    local_addr: bind.resolve_local_addr(),
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
            UdpCommand::Configure { socket_id, option } => self.route_udp_indication(
                socket_id,
                UdpIndication::ConfigureFailed {
                    socket_id,
                    option,
                    reason: ConfigureFailureReason::DriverUnavailable,
                },
            ),
            UdpCommand::Close { socket_id } => {
                self.route_udp_indication(
                    socket_id,
                    UdpIndication::Closed {
                        socket_id,
                        remote_addr: None,
                        reason: UdpCloseReason::Requested,
                    },
                );
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
            TcpCommand::Listen {
                listener_id,
                local_addr,
            } => {
                self.route_tcp_listener_event(
                    listener_id,
                    TcpListenerDriverEvent::ListenFailed {
                        local_addr,
                        reason: OpenFailureReason::DriverUnavailable,
                    },
                );
                self.tcp_listener_routes.remove(&listener_id);
            }
            TcpCommand::AdoptAccepted { connection_id } => {
                self.route_tcp_session_event(
                    connection_id,
                    TcpSessionEvent::Closed {
                        reason: CloseReason::DriverShutdown,
                    },
                );
                self.tcp_routes.remove(&connection_id);
            }
            TcpCommand::RejectAccepted { .. } => {}
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
            TcpCommand::CloseListener { listener_id } => {
                self.route_tcp_listener_event(listener_id, TcpListenerDriverEvent::Closed);
                self.tcp_listener_routes.remove(&listener_id);
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

    fn route_tcp_listener_event(&self, listener_id: ListenerId, event: TcpListenerDriverEvent) {
        if let Some(route) = self.tcp_listener_routes.get(&listener_id) {
            route.tell(TcpListenerMessage::DriverEvent(event));
        } else {
            warn!(
                self.log(),
                "dropping TCP listener event for unknown routed listener {}", listener_id
            );
        }
    }

    fn route_tcp_listener_incoming(
        &self,
        listener_id: ListenerId,
        connection_id: ConnectionId,
        peer_addr: SocketAddr,
    ) {
        if let Some(route) = self.tcp_listener_routes.get(&listener_id) {
            route.tell(TcpListenerMessage::DriverEvent(
                TcpListenerDriverEvent::Incoming {
                    peer_addr,
                    connection_id,
                },
            ));
        } else {
            warn!(
                self.log(),
                "dropping pending TCP connection {} for unknown listener {}",
                connection_id,
                listener_id
            );
        }
    }

    fn handle_local_message(&mut self, msg: IoDriverComponentMessage) -> Handled {
        match msg {
            IoDriverComponentMessage::ReserveUdpSocket(ask) => self.handle_reserve_udp_socket(ask),
            IoDriverComponentMessage::ReleaseUdpSocket(ask) => self.handle_release_udp_socket(ask),
            IoDriverComponentMessage::OpenTcpListener(ask) => self.handle_open_tcp_listener(ask),
            IoDriverComponentMessage::ReleaseTcpListener(ask) => {
                self.handle_release_tcp_listener(ask)
            }
            IoDriverComponentMessage::OpenTcpSession(ask) => self.handle_open_tcp_session(ask),
            IoDriverComponentMessage::ReleaseTcpSession(ask) => {
                self.handle_release_tcp_session(ask)
            }
            IoDriverComponentMessage::AdoptAcceptedTcpSession(ask) => {
                self.handle_adopt_accepted_tcp_session(ask)
            }
            IoDriverComponentMessage::RejectPendingTcpSession(ask) => {
                self.handle_reject_pending_tcp_session(ask)
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

    fn handle_open_tcp_listener(
        &mut self,
        ask: Ask<OpenTcpListenerRegistration, Result<ListenerId>>,
    ) -> Handled {
        let (promise, request) = ask.take();
        Handled::block_on(self, move |mut async_self| async move {
            let reply = async_self
                .open_tcp_listener_inner(request.listener, request.local_addr)
                .await;
            if promise.fulfil(reply).is_err() {
                debug!(async_self.log(), "dropping TCP listener open reply");
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

    fn handle_release_tcp_listener(&mut self, ask: Ask<ListenerId, Result<()>>) -> Handled {
        let (promise, listener_id) = ask.take();
        Handled::block_on(self, move |mut async_self| async move {
            let reply = async_self.release_tcp_listener_inner(listener_id).await;
            if promise.fulfil(reply).is_err() {
                debug!(async_self.log(), "dropping TCP listener release reply");
            }
        })
    }

    fn handle_adopt_accepted_tcp_session(
        &mut self,
        ask: Ask<AdoptAcceptedTcpSessionRegistration, Result<()>>,
    ) -> Handled {
        let (promise, request) = ask.take();
        Handled::block_on(self, move |mut async_self| async move {
            let reply = async_self
                .adopt_accepted_tcp_session_inner(request.session, request.connection_id)
                .await;
            if promise.fulfil(reply).is_err() {
                debug!(async_self.log(), "dropping TCP session adoption reply");
            }
        })
    }

    fn handle_reject_pending_tcp_session(&mut self, ask: Ask<ConnectionId, Result<()>>) -> Handled {
        let (promise, connection_id) = ask.take();
        Handled::block_on(self, move |mut async_self| async move {
            let reply = async_self
                .reject_pending_tcp_session_inner(connection_id)
                .await;
            if promise.fulfil(reply).is_err() {
                debug!(
                    async_self.log(),
                    "dropping TCP pending-session reject reply"
                );
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
        let logger = erased_runtime_logger(self.log());
        match IoDriver::start_with_logger_buffers_and_event_sink(
            self.config.clone(),
            logger,
            self.buffers.clone(),
            sink,
        ) {
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
    component.tcp_listener_routes.clear();
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
        UdpEvent::Configured { socket_id, option } => {
            (socket_id, UdpIndication::Configured { socket_id, option })
        }
        UdpEvent::ConfigureFailed {
            socket_id,
            option,
            error_kind,
        } => (
            socket_id,
            UdpIndication::ConfigureFailed {
                socket_id,
                option,
                reason: ConfigureFailureReason::Io(error_kind),
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
        UdpEvent::Closed {
            socket_id,
            remote_addr,
            reason,
        } => (
            socket_id,
            UdpIndication::Closed {
                socket_id,
                remote_addr,
                reason,
            },
        ),
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
        TcpEvent::ListenFailed { listener_id, .. }
        | TcpEvent::Listening { listener_id, .. }
        | TcpEvent::Accepted { listener_id, .. }
        | TcpEvent::ListenerClosed { listener_id } => {
            unreachable!(
                "listener-related TCP events must not be routed through the TCP session adapter for {}",
                listener_id
            )
        }
    }
}
