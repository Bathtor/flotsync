use super::{
    driver_component::DriverComponentRef,
    listener::{TcpListener, TcpListenerMessage},
    resolve_kfuture,
    session::{TcpSession, TcpSessionMessage},
    types::{
        ConfigureFailureReason,
        OpenFailureReason,
        OpenTcpListener,
        OpenTcpSession,
        OpenedTcpListener,
        OpenedTcpSession,
        TcpSessionEventTarget,
        UdpIndication,
        UdpOpenRequestId,
        UdpPort,
        UdpRequest,
        UdpSendResult,
    },
};
use crate::{
    api::{SendFailureReason, SocketId, TransmissionId, UdpCommand, UdpLocalBind, UdpSocketOption},
    errors::Error,
    pool::EgressPool,
};
use ::kompact::prelude::*;
use flotsync_utils::{OptionExt as _, ResultExt as _};
use std::{
    collections::{HashMap, HashSet},
    fmt,
    net::SocketAddr,
    sync::Arc,
};

/// Bridge-local request to register one child required UDP port on the bridge's
/// shared provided UDP port.
///
/// `RequiredRef<UdpPort>` does not implement `Debug`, but Kompact actor
/// messages do. This wrapper therefore provides a manual `Debug`
/// implementation that documents the operation without trying to print the
/// opaque port reference itself.
#[doc(hidden)]
pub struct ConnectUdpPortRequest {
    promise: KPromise<crate::errors::Result<()>>,
    required: RequiredRef<UdpPort>,
}

impl fmt::Debug for ConnectUdpPortRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectUdpPortRequest")
            .field("required", &"<RequiredRef<UdpPort>>")
            .finish_non_exhaustive()
    }
}

/// Internal actor mailbox for one bridge component.
///
/// The bridge receives both control-plane `ask` traffic and routed driver events on this mailbox.
/// Keeping them in one local message enum avoids exposing bridge-internal routing details on the
/// public UDP port surface.
#[derive(Debug)]
#[doc(hidden)]
pub enum IoBridgeMessage {
    OpenTcpListener(
        Ask<OpenTcpListener, std::result::Result<OpenedTcpListener, OpenFailureReason>>,
    ),
    OpenTcpSession(Ask<OpenTcpSession, std::result::Result<OpenedTcpSession, OpenFailureReason>>),
    ConnectUdpPort(ConnectUdpPortRequest),
    UdpEvent(UdpBridgeEvent),
}

/// Internal UDP event routed from the shared driver component to one bridge.
///
/// The public UDP indication stream carries bridge-local `UdpOpenRequestId` values for open
/// correlation, while the raw driver knows only about `SocketId`. The bridge therefore receives
/// these raw-ish events, reattaches the pending open correlation id where needed, and only then
/// triggers the public port indication or private send result.
#[derive(Clone, Debug)]
pub enum UdpBridgeEvent {
    Bound {
        socket_id: SocketId,
        local_addr: SocketAddr,
    },
    BindFailed {
        socket_id: SocketId,
        local_addr: SocketAddr,
        reason: OpenFailureReason,
    },
    Connected {
        socket_id: SocketId,
        local_addr: SocketAddr,
        remote_addr: SocketAddr,
    },
    ConnectFailed {
        socket_id: SocketId,
        local_addr: SocketAddr,
        remote_addr: SocketAddr,
        reason: OpenFailureReason,
    },
    Received {
        socket_id: SocketId,
        source: SocketAddr,
        payload: crate::api::IoPayload,
    },
    SendAck {
        socket_id: SocketId,
        transmission_id: TransmissionId,
    },
    SendNack {
        socket_id: SocketId,
        transmission_id: TransmissionId,
        reason: SendFailureReason,
    },
    Configured {
        socket_id: SocketId,
        option: UdpSocketOption,
    },
    ConfigureFailed {
        socket_id: SocketId,
        option: UdpSocketOption,
        reason: ConfigureFailureReason,
    },
    ReadSuspended {
        socket_id: SocketId,
    },
    ReadResumed {
        socket_id: SocketId,
    },
    WriteSuspended {
        socket_id: SocketId,
    },
    WriteResumed {
        socket_id: SocketId,
    },
    Closed {
        socket_id: SocketId,
        remote_addr: Option<SocketAddr>,
        reason: crate::api::UdpCloseReason,
    },
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

/// Tracks UDP open requests until the first bound/connect outcome is delivered locally.
#[derive(Debug, Default)]
struct UdpOpenRequestTable {
    by_socket: HashMap<SocketId, UdpOpenRequestId>,
}

impl UdpOpenRequestTable {
    fn insert(&mut self, socket_id: SocketId, request_id: UdpOpenRequestId) {
        self.by_socket.insert(socket_id, request_id);
    }

    fn forget_socket(&mut self, socket_id: SocketId) {
        let _ = self.by_socket.remove(&socket_id);
    }

    fn take(&mut self, socket_id: SocketId) -> Option<UdpOpenRequestId> {
        self.by_socket.remove(&socket_id)
    }

    fn clear(&mut self) {
        self.by_socket.clear();
    }
}

/// Helper handle for the bridge's actor-style control surface.
///
/// Use this handle for local resource-management operations such as opening TCP sessions and
/// listeners. The actual UDP traffic surface remains a typed Kompact port on the component
/// itself.
#[derive(Clone)]
pub struct IoBridgeHandle {
    actor: ActorRefStrong<IoBridgeMessage>,
    egress_pool: EgressPool,
    udp: ProvidedRef<UdpPort>,
}

impl IoBridgeHandle {
    /// Creates a control handle for the given live bridge component.
    ///
    /// # Panics
    ///
    /// Panics if the bridge component no longer exposes a live actor reference.
    pub fn from_component(component: &Arc<Component<IoBridge>>) -> Self {
        let actor = component
            .actor_ref()
            .hold()
            .expect("IoBridge must still be live when creating a control handle");
        let (egress_pool, udp) =
            component.on_definition(|bridge| (bridge.egress_pool.clone(), bridge.provided_ref()));
        Self {
            actor,
            egress_pool,
            udp,
        }
    }

    /// Returns the shared egress pool used by the underlying raw driver instance.
    #[must_use]
    pub fn egress_pool(&self) -> &EgressPool {
        &self.egress_pool
    }

    /// Returns a clone of the bridge's shared UDP provided-port reference.
    #[must_use]
    pub fn udp_port_ref(&self) -> ProvidedRef<UdpPort> {
        self.udp.clone()
    }

    /// Connects one component's required UDP port to this bridge.
    ///
    /// The component-side connection is installed immediately on the (expected to be)
    /// not-yet-running component definition. The bridge-side connection is installed by the
    /// bridge actor itself so callers do not need direct mutable access to the
    /// bridge component.
    ///
    /// ## Warning
    /// If `component` is already running when this is called, this function might block.
    ///
    /// # Errors
    ///
    /// See `Error` for failure conditions.
    pub async fn connect_udp<C>(&self, component: &Arc<Component<C>>) -> crate::errors::Result<()>
    where
        C: ComponentDefinition + ComponentLifecycle + Require<UdpPort> + RequireRef<UdpPort>,
    {
        let required = component.on_definition(|definition| {
            definition.connect_to_provided(self.udp_port_ref());
            definition.required_ref()
        });
        let future = self.actor.ask_with(|promise| {
            IoBridgeMessage::ConnectUdpPort(ConnectUdpPortRequest { promise, required })
        });
        resolve_kfuture(future).await
    }

    /// Opens one outbound TCP session managed by this bridge.
    pub fn open_tcp_session(
        &self,
        request: OpenTcpSession,
    ) -> KFuture<std::result::Result<OpenedTcpSession, OpenFailureReason>> {
        let (promise, future) =
            promise::<std::result::Result<OpenedTcpSession, OpenFailureReason>>();
        self.actor
            .tell(IoBridgeMessage::OpenTcpSession(Ask::new(promise, request)));
        future
    }

    /// Opens one inbound TCP listener managed by this bridge.
    pub fn open_tcp_listener(
        &self,
        request: OpenTcpListener,
    ) -> KFuture<std::result::Result<OpenedTcpListener, OpenFailureReason>> {
        let (promise, future) =
            promise::<std::result::Result<OpenedTcpListener, OpenFailureReason>>();
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
    egress_pool: EgressPool,
    owned_sockets: HashSet<SocketId>,
    udp_open_requests: UdpOpenRequestTable,
    udp_send_replies: UdpSendReplyTable,
}

impl IoBridge {
    /// Creates one bridge bound to the given shared driver component.
    ///
    /// The bridge owns any UDP socket handles it opens on behalf of local UDP port requests and
    /// routes all raw driver events back into Kompact-native UDP port indications or TCP session
    /// events.
    pub fn new(driver_component: &Arc<Component<super::IoDriverComponent>>) -> Self {
        let driver = DriverComponentRef::from_component(driver_component);
        let egress_pool = driver_component.on_definition(|component| component.egress_pool());
        Self::with_driver(driver, egress_pool)
    }

    pub(crate) fn with_driver(driver: DriverComponentRef, egress_pool: EgressPool) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            udp: ProvidedPort::uninitialised(),
            driver,
            egress_pool,
            owned_sockets: HashSet::new(),
            udp_open_requests: UdpOpenRequestTable::default(),
            udp_send_replies: UdpSendReplyTable::default(),
        }
    }

    fn owns_socket(&self, socket_id: SocketId) -> bool {
        self.owned_sockets.contains(&socket_id)
    }

    fn handle_udp_request(&mut self, request: UdpRequest) -> HandlerResult {
        match request {
            UdpRequest::Bind { request_id, bind } => self.handle_udp_bind_request(request_id, bind),
            UdpRequest::Connect {
                request_id,
                remote_addr,
                bind,
            } => self.handle_udp_connect_request(request_id, remote_addr, bind),
            UdpRequest::Send {
                socket_id,
                transmission_id,
                payload,
                target,
                reply_to,
            } => {
                self.handle_udp_send_request(socket_id, transmission_id, payload, target, reply_to);
                Handled::OK
            }
            UdpRequest::Configure { socket_id, option } => {
                self.handle_udp_configure_request(socket_id, option);
                Handled::OK
            }
            UdpRequest::Close { socket_id } => {
                self.handle_udp_close_request(socket_id);
                Handled::OK
            }
        }
    }

    fn handle_udp_bind_request(
        &mut self,
        request_id: UdpOpenRequestId,
        bind: UdpLocalBind,
    ) -> HandlerResult {
        let owner = self
            .actor_ref()
            .hold()
            .expect("IoBridge must be live while opening a UDP socket");
        let local_addr = bind.resolve_local_addr();
        Handled::block_on(self, move |mut async_self| async move {
            let reply = resolve_kfuture(async_self.driver.reserve_udp_socket(owner)).await;
            let socket_id = match reply {
                Ok(socket_id) => socket_id,
                Err(error) => {
                    async_self.udp.trigger(UdpIndication::BindFailed {
                        request_id,
                        local_addr,
                        reason: open_failure_from_error(&error),
                    });
                    return Handled::OK;
                }
            };
            async_self.owned_sockets.insert(socket_id);
            async_self.udp_open_requests.insert(socket_id, request_id);
            async_self
                .driver
                .dispatch_udp(UdpCommand::Bind { socket_id, bind });
            Handled::OK
        })
    }

    fn handle_udp_connect_request(
        &mut self,
        request_id: UdpOpenRequestId,
        remote_addr: SocketAddr,
        bind: UdpLocalBind,
    ) -> HandlerResult {
        let owner = self
            .actor_ref()
            .hold()
            .expect("IoBridge must be live while opening a UDP socket");
        let local_addr = bind.resolve_local_addr();
        Handled::block_on(self, move |mut async_self| async move {
            let reply = resolve_kfuture(async_self.driver.reserve_udp_socket(owner)).await;
            let socket_id = match reply {
                Ok(socket_id) => socket_id,
                Err(error) => {
                    async_self.udp.trigger(UdpIndication::ConnectFailed {
                        request_id,
                        local_addr,
                        remote_addr,
                        reason: open_failure_from_error(&error),
                    });
                    return Handled::OK;
                }
            };
            async_self.owned_sockets.insert(socket_id);
            async_self.udp_open_requests.insert(socket_id, request_id);
            async_self.driver.dispatch_udp(UdpCommand::Connect {
                socket_id,
                remote_addr,
                bind,
            });
            Handled::OK
        })
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
        self.udp_open_requests.forget_socket(socket_id);
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

    fn handle_udp_configure_request(&mut self, socket_id: SocketId, option: UdpSocketOption) {
        if !self.owns_socket(socket_id) {
            self.udp.trigger(UdpIndication::ConfigureFailed {
                socket_id,
                option,
                reason: ConfigureFailureReason::InvalidHandle,
            });
            return;
        }
        self.udp_open_requests.forget_socket(socket_id);
        self.driver
            .dispatch_udp(UdpCommand::Configure { socket_id, option });
    }

    fn handle_udp_close_request(&mut self, socket_id: SocketId) {
        if !self.owns_socket(socket_id) {
            return;
        }
        self.udp_open_requests.forget_socket(socket_id);
        self.driver.dispatch_udp(UdpCommand::Close { socket_id });
    }

    #[allow(
        clippy::too_many_lines,
        reason = "Bridge UDP event handling fan-outs every runtime event to its Kompact port contract."
    )]
    fn handle_udp_event(&mut self, event: UdpBridgeEvent) {
        match event {
            UdpBridgeEvent::Bound {
                socket_id,
                local_addr,
            } => {
                let Some(request_id) = self.udp_open_requests.take(socket_id) else {
                    warn!(
                        self.log(),
                        "dropping UDP bound indication for socket {} without a pending open request",
                        socket_id
                    );
                    return;
                };
                self.udp.trigger(UdpIndication::Bound {
                    request_id,
                    socket_id,
                    local_addr,
                });
            }
            UdpBridgeEvent::BindFailed {
                socket_id,
                local_addr,
                reason,
            } => {
                self.owned_sockets.remove(&socket_id);
                let Some(request_id) = self.udp_open_requests.take(socket_id) else {
                    warn!(
                        self.log(),
                        "dropping UDP bind failure for socket {} without a pending open request",
                        socket_id
                    );
                    return;
                };
                self.udp.trigger(UdpIndication::BindFailed {
                    request_id,
                    local_addr,
                    reason,
                });
            }
            UdpBridgeEvent::Connected {
                socket_id,
                local_addr,
                remote_addr,
            } => {
                let Some(request_id) = self.udp_open_requests.take(socket_id) else {
                    warn!(
                        self.log(),
                        "dropping UDP connected indication for socket {} without a pending open request",
                        socket_id
                    );
                    return;
                };
                self.udp.trigger(UdpIndication::Connected {
                    request_id,
                    socket_id,
                    local_addr,
                    remote_addr,
                });
            }
            UdpBridgeEvent::ConnectFailed {
                socket_id,
                local_addr,
                remote_addr,
                reason,
            } => {
                self.owned_sockets.remove(&socket_id);
                let Some(request_id) = self.udp_open_requests.take(socket_id) else {
                    warn!(
                        self.log(),
                        "dropping UDP connect failure for socket {} without a pending open request",
                        socket_id
                    );
                    return;
                };
                self.udp.trigger(UdpIndication::ConnectFailed {
                    request_id,
                    local_addr,
                    remote_addr,
                    reason,
                });
            }
            UdpBridgeEvent::Received {
                socket_id,
                source,
                payload,
            } => {
                self.udp.trigger(UdpIndication::Received {
                    socket_id,
                    source,
                    payload,
                });
            }
            UdpBridgeEvent::SendAck {
                socket_id,
                transmission_id,
            } => {
                let Some(reply_to) = self.udp_send_replies.take(socket_id, transmission_id) else {
                    warn!(
                        self.log(),
                        "dropping UDP send ack for socket {} transmission {} without a waiting recipient",
                        socket_id,
                        transmission_id
                    );
                    return;
                };
                reply_to.tell(UdpSendResult::Ack {
                    socket_id,
                    transmission_id,
                });
            }
            UdpBridgeEvent::SendNack {
                socket_id,
                transmission_id,
                reason,
            } => {
                let Some(reply_to) = self.udp_send_replies.take(socket_id, transmission_id) else {
                    warn!(
                        self.log(),
                        "dropping UDP send nack for socket {} transmission {} without a waiting recipient",
                        socket_id,
                        transmission_id
                    );
                    return;
                };
                reply_to.tell(UdpSendResult::Nack {
                    socket_id,
                    transmission_id,
                    reason,
                });
            }
            UdpBridgeEvent::Configured { socket_id, option } => {
                self.udp
                    .trigger(UdpIndication::Configured { socket_id, option });
            }
            UdpBridgeEvent::ConfigureFailed {
                socket_id,
                option,
                reason,
            } => {
                self.udp.trigger(UdpIndication::ConfigureFailed {
                    socket_id,
                    option,
                    reason,
                });
            }
            UdpBridgeEvent::ReadSuspended { socket_id } => {
                self.udp.trigger(UdpIndication::ReadSuspended { socket_id });
            }
            UdpBridgeEvent::ReadResumed { socket_id } => {
                self.udp.trigger(UdpIndication::ReadResumed { socket_id });
            }
            UdpBridgeEvent::WriteSuspended { socket_id } => {
                self.udp
                    .trigger(UdpIndication::WriteSuspended { socket_id });
            }
            UdpBridgeEvent::WriteResumed { socket_id } => {
                self.udp.trigger(UdpIndication::WriteResumed { socket_id });
            }
            UdpBridgeEvent::Closed {
                socket_id,
                remote_addr,
                reason,
            } => {
                self.owned_sockets.remove(&socket_id);
                self.udp_open_requests.forget_socket(socket_id);
                self.udp_send_replies
                    .fail_socket(socket_id, SendFailureReason::Closed);
                self.udp.trigger(UdpIndication::Closed {
                    socket_id,
                    remote_addr,
                    reason,
                });
            }
        }
    }

    fn handle_local_message(&mut self, msg: IoBridgeMessage) -> HandlerResult {
        match msg {
            IoBridgeMessage::OpenTcpListener(ask) => self.handle_open_tcp_listener(ask),
            IoBridgeMessage::OpenTcpSession(ask) => self.handle_open_tcp_session(ask),
            IoBridgeMessage::ConnectUdpPort(ask) => self.handle_connect_udp_port(ask),
            IoBridgeMessage::UdpEvent(event) => {
                self.handle_udp_event(event);
                Handled::OK
            }
        }
    }

    fn handle_connect_udp_port(&mut self, request: ConnectUdpPortRequest) -> HandlerResult {
        self.udp.connect(request.required);
        request.promise.fulfil(Ok(())).whatever_benign(
            "dropping UDP port-connect completion because requester disappeared",
        )?;
        Handled::OK
    }

    fn handle_open_tcp_session(
        &mut self,
        ask: Ask<OpenTcpSession, std::result::Result<OpenedTcpSession, OpenFailureReason>>,
    ) -> HandlerResult {
        let (promise, request) = ask.take();
        let driver = self.driver.clone();
        Handled::block_on(self, move |async_self| async move {
            let session_component = async_self.ctx.system().create(|| {
                TcpSession::with_open_promise(
                    driver.clone(),
                    TcpSessionEventTarget::from_recipient(request.events_to.clone()),
                    async_self.egress_pool.clone(),
                    Some(promise),
                )
            });
            let session_strong = session_component
                .actor_ref()
                .hold()
                .whatever_unrecoverable("newly created TCP session must be live")?;
            async_self.ctx.system().start(&session_component);

            let reply = async_self.driver.open_tcp_session(
                session_strong.clone(),
                request.remote_addr,
                request.local_addr,
            );
            let submit_result = resolve_kfuture(reply).await;
            if let Err(error) = submit_result {
                session_strong.tell(TcpSessionMessage::DriverEvent(
                    super::session::TcpSessionDriverEvent::OpenFailed {
                        reason: open_failure_from_error(&error),
                    },
                ));
            }
            Handled::OK
        })
    }

    fn handle_open_tcp_listener(
        &mut self,
        ask: Ask<OpenTcpListener, std::result::Result<OpenedTcpListener, OpenFailureReason>>,
    ) -> HandlerResult {
        let (promise, request) = ask.take();
        let driver = self.driver.clone();
        Handled::block_on(self, move |async_self| async move {
            let listener_component = async_self.ctx.system().create(|| {
                TcpListener::new(
                    driver.clone(),
                    request.incoming_to.clone(),
                    async_self.egress_pool.clone(),
                    Some(promise),
                )
            });
            let listener_strong = listener_component
                .actor_ref()
                .hold()
                .whatever_unrecoverable("newly created TCP listener must be live")?;
            async_self.ctx.system().start(&listener_component);

            let reply = async_self
                .driver
                .open_tcp_listener(listener_strong.clone(), request.local_addr);
            let submit_result = resolve_kfuture(reply).await;
            if let Err(error) = submit_result {
                listener_strong.tell(TcpListenerMessage::DriverEvent(
                    super::listener::TcpListenerDriverEvent::ListenFailed {
                        reason: open_failure_from_error(&error),
                    },
                ));
            }
            Handled::OK
        })
    }
}

impl ComponentLifecycle for IoBridge {
    fn on_stop(&mut self) -> HandlerResult {
        shutdown_bridge(self)
    }

    fn on_kill(&mut self) -> HandlerResult {
        shutdown_bridge(self)
    }
}

impl Provide<UdpPort> for IoBridge {
    fn handle(&mut self, request: <UdpPort as Port>::Request) -> HandlerResult {
        self.handle_udp_request(request)
    }
}

impl Actor for IoBridge {
    type Message = IoBridgeMessage;

    fn receive_local(&mut self, msg: Self::Message) -> HandlerResult {
        self.handle_local_message(msg)
    }
}

fn shutdown_bridge(bridge: &mut IoBridge) -> HandlerResult {
    if bridge.owned_sockets.is_empty() {
        bridge.udp_open_requests.clear();
        bridge.udp_send_replies.clear();
        return Handled::OK;
    }

    let sockets: Vec<SocketId> = bridge.owned_sockets.drain().collect();
    bridge.udp_open_requests.clear();
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
        Handled::OK
    })
}

fn open_failure_from_error(error: &Error) -> OpenFailureReason {
    match error {
        Error::UnknownConnection { .. }
        | Error::UnknownListener { .. }
        | Error::UnknownSocket { .. } => OpenFailureReason::InvalidHandle,
        _ => OpenFailureReason::DriverUnavailable,
    }
}
