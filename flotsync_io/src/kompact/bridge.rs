use super::{
    driver_component::DriverComponentRef,
    listener::{TcpListener, TcpListenerMessage},
    resolve_kfuture,
    session::{TcpSession, TcpSessionMessage},
    types::{
        OpenFailureReason,
        OpenTcpListener,
        OpenTcpSession,
        TcpListenerRef,
        TcpSessionRef,
        UdpIndication,
        UdpPort,
        UdpRequest,
        UdpSendResult,
    },
};
use crate::{
    api::{SendFailureReason, SocketId, TransmissionId, UdpCommand},
    errors::Result,
};
use ::kompact::prelude::*;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
};

/// Internal actor mailbox for one bridge component.
///
/// The bridge receives both control-plane `ask` traffic and routed driver events on this mailbox.
/// Keeping them in one local message enum avoids exposing bridge-internal routing details on the
/// public UDP port surface.
#[doc(hidden)]
#[derive(Debug)]
pub enum IoBridgeMessage {
    ReserveUdpSocket(Ask<(), Result<SocketId>>),
    ReleaseUdpSocket(Ask<SocketId, Result<()>>),
    OpenTcpListener(Ask<OpenTcpListener, Result<TcpListenerRef>>),
    OpenTcpSession(Ask<OpenTcpSession, Result<TcpSessionRef>>),
    UdpIndication(UdpIndication),
    UdpSendResult(UdpSendResult),
}

/// Tracks private UDP send-completion recipients per owned socket.
///
/// The outer map keeps socket-level cleanup cheap when a socket closes or the bridge shuts down.
/// The inner map keeps exact `(socket_id, transmission_id)` completion routing cheap as well.
#[derive(Debug, Default)]
struct UdpSendReplyTable {
    by_socket: HashMap<SocketId, HashMap<TransmissionId, Recipient<UdpSendResult>>>,
}

impl UdpSendReplyTable {
    /// Inserts a reply recipient for one in-flight UDP send.
    ///
    /// Returns `Ok(())` when the slot was empty. Returns the original `reply_to` back to the
    /// caller when the exact `(socket_id, transmission_id)` slot was already occupied.
    fn insert(
        &mut self,
        socket_id: SocketId,
        transmission_id: TransmissionId,
        reply_to: Recipient<UdpSendResult>,
    ) -> std::result::Result<(), Recipient<UdpSendResult>> {
        let replies_for_socket = self.by_socket.entry(socket_id).or_default();
        if replies_for_socket.contains_key(&transmission_id) {
            return Err(reply_to);
        }
        replies_for_socket.insert(transmission_id, reply_to);
        Ok(())
    }

    fn take(
        &mut self,
        socket_id: SocketId,
        transmission_id: TransmissionId,
    ) -> Option<Recipient<UdpSendResult>> {
        let replies_for_socket = self.by_socket.get_mut(&socket_id)?;
        let reply_to = replies_for_socket.remove(&transmission_id);
        if replies_for_socket.is_empty() {
            self.by_socket.remove(&socket_id);
        }
        reply_to
    }

    fn fail_socket(&mut self, socket_id: SocketId, reason: SendFailureReason) {
        let Some(replies_for_socket) = self.by_socket.remove(&socket_id) else {
            return;
        };
        for (transmission_id, reply_to) in replies_for_socket {
            reply_to.tell(UdpSendResult::Nack {
                socket_id,
                transmission_id,
                reason,
            });
        }
    }

    fn clear(&mut self) {
        self.by_socket.clear();
    }
}

/// Helper handle for the bridge's actor-style control surface.
///
/// Use this handle for local resource-management operations such as reserving UDP socket ids and
/// opening TCP sessions. The actual UDP traffic surface remains a typed Kompact port on the
/// component itself.
#[derive(Clone, Debug)]
pub struct IoBridgeHandle {
    actor: ActorRefStrong<IoBridgeMessage>,
}

impl IoBridgeHandle {
    /// Creates a control handle for the given live bridge component.
    pub fn from_component(component: &Arc<Component<IoBridge>>) -> Self {
        let actor = component
            .actor_ref()
            .hold()
            .expect("IoBridge must still be live when creating a control handle");
        Self { actor }
    }

    /// Reserves one UDP socket handle owned by this bridge.
    pub fn reserve_udp_socket(&self) -> KFuture<Result<SocketId>> {
        let (promise, future) = promise::<Result<SocketId>>();
        self.actor
            .tell(IoBridgeMessage::ReserveUdpSocket(Ask::new(promise, ())));
        future
    }

    /// Releases one previously reserved UDP socket handle.
    pub fn release_udp_socket(&self, socket_id: SocketId) -> KFuture<Result<()>> {
        let (promise, future) = promise::<Result<()>>();
        self.actor.tell(IoBridgeMessage::ReleaseUdpSocket(Ask::new(
            promise, socket_id,
        )));
        future
    }

    /// Opens one outbound TCP session managed by this bridge.
    pub fn open_tcp_session(&self, request: OpenTcpSession) -> KFuture<Result<TcpSessionRef>> {
        let (promise, future) = promise::<Result<TcpSessionRef>>();
        self.actor
            .tell(IoBridgeMessage::OpenTcpSession(Ask::new(promise, request)));
        future
    }

    /// Opens one inbound TCP listener managed by this bridge.
    pub fn open_tcp_listener(&self, request: OpenTcpListener) -> KFuture<Result<TcpListenerRef>> {
        let (promise, future) = promise::<Result<TcpListenerRef>>();
        self.actor
            .tell(IoBridgeMessage::OpenTcpListener(Ask::new(promise, request)));
        future
    }
}

/// Per-client Kompact bridge that exposes shared UDP capability and TCP session management.
#[derive(ComponentDefinition)]
pub struct IoBridge {
    ctx: ComponentContext<Self>,
    udp: ProvidedPort<UdpPort>,
    driver: DriverComponentRef,
    owned_sockets: HashSet<SocketId>,
    udp_send_replies: UdpSendReplyTable,
}

impl IoBridge {
    /// Creates one bridge bound to the given shared driver component.
    ///
    /// The bridge owns any UDP socket handles reserved through its [`IoBridgeHandle`] and routes
    /// all raw driver events back into Kompact-native UDP port indications or TCP session events.
    pub fn new(driver_component: &Arc<Component<super::IoDriverComponent>>) -> Self {
        Self::with_driver(DriverComponentRef::from_component(driver_component))
    }

    pub(crate) fn with_driver(driver: DriverComponentRef) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            udp: ProvidedPort::uninitialised(),
            driver,
            owned_sockets: HashSet::new(),
            udp_send_replies: UdpSendReplyTable::default(),
        }
    }

    fn owns_socket(&self, socket_id: SocketId) -> bool {
        self.owned_sockets.contains(&socket_id)
    }

    fn handle_udp_request(&mut self, request: UdpRequest) {
        match request {
            UdpRequest::Bind {
                socket_id,
                local_addr,
            } => self.handle_udp_bind_request(socket_id, local_addr),
            UdpRequest::Connect {
                socket_id,
                remote_addr,
                local_addr,
            } => self.handle_udp_connect_request(socket_id, remote_addr, local_addr),
            UdpRequest::Send {
                socket_id,
                transmission_id,
                payload,
                target,
                reply_to,
            } => {
                self.handle_udp_send_request(socket_id, transmission_id, payload, target, reply_to)
            }
            UdpRequest::Close { socket_id } => self.handle_udp_close_request(socket_id),
        }
    }

    fn handle_udp_bind_request(&mut self, socket_id: SocketId, local_addr: SocketAddr) {
        if !self.owns_socket(socket_id) {
            self.udp.trigger(UdpIndication::BindFailed {
                socket_id,
                local_addr,
                reason: OpenFailureReason::InvalidHandle,
            });
            return;
        }
        self.driver.dispatch_udp(UdpCommand::Bind {
            socket_id,
            local_addr,
        });
    }

    fn handle_udp_connect_request(
        &mut self,
        socket_id: SocketId,
        remote_addr: SocketAddr,
        local_addr: Option<SocketAddr>,
    ) {
        if !self.owns_socket(socket_id) {
            self.udp.trigger(UdpIndication::ConnectFailed {
                socket_id,
                local_addr,
                remote_addr,
                reason: OpenFailureReason::InvalidHandle,
            });
            return;
        }
        self.driver.dispatch_udp(UdpCommand::Connect {
            socket_id,
            remote_addr,
            local_addr,
        });
    }

    fn handle_udp_send_request(
        &mut self,
        socket_id: SocketId,
        transmission_id: TransmissionId,
        payload: crate::api::IoPayload,
        target: Option<SocketAddr>,
        reply_to: Recipient<UdpSendResult>,
    ) {
        if !self.owns_socket(socket_id) {
            reply_to.tell(UdpSendResult::Nack {
                socket_id,
                transmission_id,
                reason: SendFailureReason::InvalidState,
            });
            return;
        }
        let insert_result = self
            .udp_send_replies
            .insert(socket_id, transmission_id, reply_to);
        if let Err(reply_to) = insert_result {
            reply_to.tell(UdpSendResult::Nack {
                socket_id,
                transmission_id,
                reason: SendFailureReason::InvalidState,
            });
            return;
        }
        self.driver.dispatch_udp(UdpCommand::Send {
            socket_id,
            transmission_id,
            payload,
            target,
        });
    }

    fn handle_udp_close_request(&mut self, socket_id: SocketId) {
        if !self.owns_socket(socket_id) {
            return;
        }
        self.driver.dispatch_udp(UdpCommand::Close { socket_id });
    }

    fn handle_udp_indication(&mut self, indication: UdpIndication) {
        if let UdpIndication::Closed { socket_id } = indication {
            self.owned_sockets.remove(&socket_id);
            self.udp_send_replies
                .fail_socket(socket_id, SendFailureReason::Closed);
            self.udp.trigger(UdpIndication::Closed { socket_id });
            return;
        }
        self.udp.trigger(indication);
    }

    fn handle_udp_send_result(&mut self, result: UdpSendResult) {
        let (socket_id, transmission_id) = match &result {
            UdpSendResult::Ack {
                socket_id,
                transmission_id,
            }
            | UdpSendResult::Nack {
                socket_id,
                transmission_id,
                ..
            } => (*socket_id, *transmission_id),
        };
        let Some(reply_to) = self.udp_send_replies.take(socket_id, transmission_id) else {
            warn!(
                self.log(),
                "dropping UDP send result for socket {} transmission {} without a waiting recipient",
                socket_id,
                transmission_id
            );
            return;
        };
        reply_to.tell(result);
    }

    fn handle_local_message(&mut self, msg: IoBridgeMessage) -> Handled {
        match msg {
            IoBridgeMessage::ReserveUdpSocket(ask) => self.handle_reserve_udp_socket(ask),
            IoBridgeMessage::ReleaseUdpSocket(ask) => self.handle_release_udp_socket(ask),
            IoBridgeMessage::OpenTcpListener(ask) => self.handle_open_tcp_listener(ask),
            IoBridgeMessage::OpenTcpSession(ask) => self.handle_open_tcp_session(ask),
            IoBridgeMessage::UdpIndication(indication) => {
                self.handle_udp_indication(indication);
                Handled::Ok
            }
            IoBridgeMessage::UdpSendResult(result) => {
                self.handle_udp_send_result(result);
                Handled::Ok
            }
        }
    }

    fn handle_reserve_udp_socket(&mut self, ask: Ask<(), Result<SocketId>>) -> Handled {
        let (promise, ()) = ask.take();
        let owner = self
            .actor_ref()
            .hold()
            .expect("IoBridge must be live while reserving a socket");
        Handled::block_on(self, move |mut async_self| async move {
            let reply = resolve_kfuture(async_self.driver.reserve_udp_socket(owner)).await;
            match reply {
                Ok(socket_id) => {
                    async_self.owned_sockets.insert(socket_id);
                    if promise.fulfil(Ok(socket_id)).is_err() {
                        debug!(async_self.log(), "dropping UDP socket reservation reply");
                    }
                }
                Err(error) => {
                    if promise.fulfil(Err(error)).is_err() {
                        debug!(async_self.log(), "dropping UDP socket reservation reply");
                    }
                }
            }
        })
    }

    fn handle_release_udp_socket(&mut self, ask: Ask<SocketId, Result<()>>) -> Handled {
        let (promise, socket_id) = ask.take();
        self.owned_sockets.remove(&socket_id);
        self.udp_send_replies
            .fail_socket(socket_id, SendFailureReason::Closed);
        Handled::block_on(self, move |async_self| async move {
            let reply = resolve_kfuture(async_self.driver.release_udp_socket(socket_id)).await;
            if promise.fulfil(reply).is_err() {
                debug!(async_self.log(), "dropping UDP socket release reply");
            }
        })
    }

    fn handle_open_tcp_session(
        &mut self,
        ask: Ask<OpenTcpSession, Result<TcpSessionRef>>,
    ) -> Handled {
        let (promise, request) = ask.take();
        let driver = self.driver.clone();
        Handled::block_on(self, move |async_self| async move {
            let session_component = async_self
                .ctx
                .system()
                .create(|| TcpSession::new(driver.clone(), request.events_to.clone()));
            let session_strong = session_component
                .actor_ref()
                .hold()
                .expect("newly created TCP session must be live");
            let session_ref = TcpSessionRef::new(session_strong.clone());
            async_self.ctx.system().start(&session_component);

            let reply = async_self.driver.open_tcp_session(
                session_strong.clone(),
                request.remote_addr,
                request.local_addr,
            );
            let reply = resolve_kfuture(reply).await;
            match reply {
                Ok(connection_id) => {
                    session_strong.tell(TcpSessionMessage::Attach(connection_id));
                    if promise.fulfil(Ok(session_ref)).is_err() {
                        debug!(async_self.log(), "dropping TCP session open reply");
                    }
                }
                Err(error) => {
                    async_self.ctx.system().kill(session_component);
                    if promise.fulfil(Err(error)).is_err() {
                        debug!(async_self.log(), "dropping TCP session open reply");
                    }
                }
            }
        })
    }

    fn handle_open_tcp_listener(
        &mut self,
        ask: Ask<OpenTcpListener, Result<TcpListenerRef>>,
    ) -> Handled {
        let (promise, request) = ask.take();
        let driver = self.driver.clone();
        Handled::block_on(self, move |async_self| async move {
            let listener_component = async_self
                .ctx
                .system()
                .create(|| TcpListener::new(driver.clone(), request.incoming_to.clone()));
            let listener_strong = listener_component
                .actor_ref()
                .hold()
                .expect("newly created TCP listener must be live");
            let listener_ref = TcpListenerRef::new(listener_strong.clone());
            async_self.ctx.system().start(&listener_component);

            let reply = async_self
                .driver
                .open_tcp_listener(listener_strong.clone(), request.local_addr);
            let reply = resolve_kfuture(reply).await;
            match reply {
                Ok(listener_id) => {
                    listener_strong.tell(TcpListenerMessage::Attach(listener_id));
                    if promise.fulfil(Ok(listener_ref)).is_err() {
                        debug!(async_self.log(), "dropping TCP listener open reply");
                    }
                }
                Err(error) => {
                    async_self.ctx.system().kill(listener_component);
                    if promise.fulfil(Err(error)).is_err() {
                        debug!(async_self.log(), "dropping TCP listener open reply");
                    }
                }
            }
        })
    }
}

impl ComponentLifecycle for IoBridge {
    fn on_stop(&mut self) -> Handled {
        shutdown_bridge(self)
    }

    fn on_kill(&mut self) -> Handled {
        shutdown_bridge(self)
    }
}

impl Provide<UdpPort> for IoBridge {
    fn handle(&mut self, request: <UdpPort as Port>::Request) -> Handled {
        self.handle_udp_request(request);
        Handled::Ok
    }
}

impl Actor for IoBridge {
    type Message = IoBridgeMessage;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        self.handle_local_message(msg)
    }

    fn receive_network(&mut self, _msg: NetMessage) -> Handled {
        unimplemented!("flotsync_io bridges do not use network actor messages");
    }
}

fn shutdown_bridge(bridge: &mut IoBridge) -> Handled {
    if bridge.owned_sockets.is_empty() {
        bridge.udp_send_replies.clear();
        return Handled::Ok;
    }

    let sockets: Vec<SocketId> = bridge.owned_sockets.drain().collect();
    for socket_id in &sockets {
        bridge
            .udp_send_replies
            .fail_socket(*socket_id, SendFailureReason::Closed);
    }
    let driver = bridge.driver.clone();
    Handled::block_on(bridge, move |async_self| async move {
        for socket_id in sockets {
            match resolve_kfuture(driver.release_udp_socket(socket_id)).await {
                Ok(()) => {}
                Err(error) => {
                    warn!(
                        async_self.log(),
                        "failed to release UDP socket {} during bridge shutdown: {}",
                        socket_id,
                        error
                    );
                }
            }
        }
    })
}
