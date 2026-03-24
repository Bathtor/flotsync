use super::{
    driver_component::DriverComponentRef,
    resolve_kfuture,
    session::TcpSession,
    types::{
        OpenFailureReason,
        OpenedTcpListener,
        PendingTcpSession,
        TcpListenerEvent,
        TcpListenerRef,
        TcpListenerRequest,
        TcpSessionEventTarget,
        TcpSessionRef,
    },
};
use crate::{
    api::{ConnectionId, ListenerId, TcpCommand},
    errors::{Error, Result},
    pool::EgressPool,
};
use ::kompact::prelude::*;
use std::collections::HashSet;

/// Request payload used when accepting one pending inbound TCP connection.
#[derive(Debug)]
pub(crate) struct AcceptPendingTcpSession {
    pub(crate) connection_id: ConnectionId,
    pub(crate) events_to: TcpSessionEventTarget,
}

/// Internal listener-directed event routed from the shared driver component.
///
/// These events carry only the raw listener facts known to the driver component. The listener
/// actor turns them into the public [`TcpListenerEvent`] values, including construction of the
/// one-shot [`PendingTcpSession`] decision handle for newly accepted connections.
#[derive(Debug)]
pub(crate) enum TcpListenerDriverEvent {
    Listening {
        listener_id: ListenerId,
        local_addr: std::net::SocketAddr,
    },
    ListenFailed {
        reason: super::types::OpenFailureReason,
    },
    Incoming {
        peer_addr: std::net::SocketAddr,
        connection_id: ConnectionId,
    },
    Closed,
}

/// Internal mailbox for one TCP listener component.
#[derive(Debug)]
pub(crate) enum TcpListenerMessage {
    Request(TcpListenerRequest),
    DriverEvent(TcpListenerDriverEvent),
    AcceptPending(Ask<AcceptPendingTcpSession, Result<TcpSessionRef>>),
    RejectPending(Ask<ConnectionId, Result<()>>),
    DropPending(ConnectionId),
}

impl From<TcpListenerRequest> for TcpListenerMessage {
    fn from(request: TcpListenerRequest) -> Self {
        Self::Request(request)
    }
}

/// Kompact component that represents one live TCP listener endpoint.
///
/// The listener component owns the Kompact-facing listener identity while the raw driver owns the
/// actual listening socket and accepted raw `ConnectionId`s. Pending accepted connections remain
/// tracked here until application logic either accepts them into a session or rejects them.
#[derive(ComponentDefinition)]
pub(crate) struct TcpListener {
    ctx: ComponentContext<Self>,
    driver: DriverComponentRef,
    egress_pool: EgressPool,
    events_to: Recipient<TcpListenerEvent>,
    listener_id: Option<ListenerId>,
    open_promise: Option<KPromise<std::result::Result<OpenedTcpListener, OpenFailureReason>>>,
    opened: bool,
    pending_connections: HashSet<ConnectionId>,
    terminal: bool,
}

impl TcpListener {
    pub(crate) fn new(
        driver: DriverComponentRef,
        events_to: Recipient<TcpListenerEvent>,
        egress_pool: EgressPool,
        open_promise: Option<KPromise<std::result::Result<OpenedTcpListener, OpenFailureReason>>>,
    ) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            driver,
            egress_pool,
            events_to,
            listener_id: None,
            open_promise,
            opened: false,
            pending_connections: HashSet::new(),
            terminal: false,
        }
    }

    fn handle_request(&mut self, request: TcpListenerRequest) -> Handled {
        match request {
            TcpListenerRequest::Close => self.handle_close_request(),
        }
    }

    fn handle_close_request(&mut self) -> Handled {
        let Some(listener_id) = self.listener_id else {
            if !self.terminal {
                self.fulfil_open_failure(OpenFailureReason::DriverUnavailable);
                if self.opened {
                    self.events_to.tell(TcpListenerEvent::Closed);
                }
                self.terminal = true;
            }
            self.ctx.suicide();
            return Handled::Ok;
        };
        if self.terminal {
            return Handled::Ok;
        }

        self.driver
            .dispatch_tcp(TcpCommand::CloseListener { listener_id });
        Handled::Ok
    }

    fn handle_driver_event(&mut self, event: TcpListenerDriverEvent) -> Handled {
        match event {
            TcpListenerDriverEvent::Listening {
                listener_id,
                local_addr,
            } => {
                self.listener_id = Some(listener_id);
                self.opened = true;
                self.fulfil_open_success(local_addr);
            }
            TcpListenerDriverEvent::ListenFailed { reason } => {
                self.pending_connections.clear();
                self.fulfil_open_failure(reason);
                self.terminal = true;
                self.ctx.suicide();
            }
            TcpListenerDriverEvent::Incoming {
                peer_addr,
                connection_id,
            } => {
                self.pending_connections.insert(connection_id);
                let actor = self
                    .actor_ref()
                    .hold()
                    .expect("TCP listener must be live while forwarding pending session decisions");
                self.events_to.tell(TcpListenerEvent::Incoming {
                    peer_addr,
                    pending: PendingTcpSession::new(actor, connection_id),
                });
            }
            TcpListenerDriverEvent::Closed => {
                self.listener_id = None;
                self.pending_connections.clear();
                if self.opened {
                    self.events_to.tell(TcpListenerEvent::Closed);
                }
                self.terminal = true;
                self.ctx.suicide();
            }
        }
        Handled::Ok
    }

    fn handle_accept_pending(
        &mut self,
        ask: Ask<AcceptPendingTcpSession, Result<TcpSessionRef>>,
    ) -> Handled {
        let (promise, request) = ask.take();
        if !self.pending_connections.remove(&request.connection_id) {
            let fulfil_result = promise.fulfil(Err(Error::UnknownConnection {
                connection_id: request.connection_id,
            }));
            if fulfil_result.is_err() {
                debug!(self.log(), "dropping TCP pending-session accept reply");
            }
            return Handled::Ok;
        }

        let driver = self.driver.clone();
        Handled::block_on(self, move |async_self| async move {
            let session_component = async_self.ctx.system().create(|| {
                TcpSession::attached(
                    driver.clone(),
                    request.events_to.clone(),
                    request.connection_id,
                    async_self.egress_pool.clone(),
                )
            });
            let session_strong = session_component
                .actor_ref()
                .hold()
                .expect("newly created TCP session must be live");
            let session_ref =
                TcpSessionRef::new(session_strong.clone(), async_self.egress_pool.clone());
            async_self.ctx.system().start(&session_component);

            let adopt = async_self
                .driver
                .adopt_accepted_tcp_session(session_strong.clone(), request.connection_id);
            let reply = resolve_kfuture(adopt).await;
            match reply {
                Ok(()) => {
                    if promise.fulfil(Ok(session_ref)).is_err() {
                        debug!(
                            async_self.log(),
                            "dropping TCP pending-session accept reply"
                        );
                    }
                }
                Err(error) => {
                    async_self.ctx.system().kill(session_component);
                    if promise.fulfil(Err(error)).is_err() {
                        debug!(
                            async_self.log(),
                            "dropping TCP pending-session accept reply"
                        );
                    }
                }
            }
        })
    }

    fn handle_reject_pending(&mut self, ask: Ask<ConnectionId, Result<()>>) -> Handled {
        let (promise, connection_id) = ask.take();
        if !self.pending_connections.remove(&connection_id) {
            let fulfil_result = promise.fulfil(Err(Error::UnknownConnection { connection_id }));
            if fulfil_result.is_err() {
                debug!(self.log(), "dropping TCP pending-session reject reply");
            }
            return Handled::Ok;
        }

        Handled::block_on(self, move |async_self| async move {
            let reply =
                resolve_kfuture(async_self.driver.reject_pending_tcp_session(connection_id)).await;
            if promise.fulfil(reply).is_err() {
                debug!(
                    async_self.log(),
                    "dropping TCP pending-session reject reply"
                );
            }
        })
    }

    fn handle_drop_pending(&mut self, connection_id: ConnectionId) -> Handled {
        if !self.pending_connections.remove(&connection_id) {
            return Handled::Ok;
        }

        Handled::block_on(self, move |async_self| async move {
            let reject = async_self.driver.reject_pending_tcp_session(connection_id);
            match resolve_kfuture(reject).await {
                Ok(()) => {}
                Err(Error::UnknownConnection { .. }) => {}
                Err(error) => {
                    warn!(
                        async_self.log(),
                        "failed to auto-reject pending TCP connection {}: {}", connection_id, error
                    );
                }
            }
        })
    }

    fn fulfil_open_success(&mut self, local_addr: std::net::SocketAddr) {
        let Some(promise) = self.open_promise.take() else {
            return;
        };
        let actor = self
            .actor_ref()
            .hold()
            .expect("TCP listener must be live while fulfilling the open result");
        let opened = OpenedTcpListener {
            listener: TcpListenerRef::new(actor),
            local_addr,
        };
        if promise.fulfil(Ok(opened)).is_err() {
            debug!(self.log(), "dropping TCP listener open reply");
        }
    }

    fn fulfil_open_failure(&mut self, reason: OpenFailureReason) {
        let Some(promise) = self.open_promise.take() else {
            return;
        };
        if promise.fulfil(Err(reason)).is_err() {
            debug!(self.log(), "dropping TCP listener open reply");
        }
    }
}

impl ComponentLifecycle for TcpListener {
    fn on_stop(&mut self) -> Handled {
        shutdown_listener(self)
    }

    fn on_kill(&mut self) -> Handled {
        shutdown_listener(self)
    }
}

impl Actor for TcpListener {
    type Message = TcpListenerMessage;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        match msg {
            TcpListenerMessage::Request(request) => self.handle_request(request),
            TcpListenerMessage::DriverEvent(event) => self.handle_driver_event(event),
            TcpListenerMessage::AcceptPending(ask) => self.handle_accept_pending(ask),
            TcpListenerMessage::RejectPending(ask) => self.handle_reject_pending(ask),
            TcpListenerMessage::DropPending(connection_id) => {
                self.handle_drop_pending(connection_id)
            }
        }
    }

    fn receive_network(&mut self, _msg: NetMessage) -> Handled {
        unimplemented!("flotsync_io TCP listeners do not use network actor messages");
    }
}

fn shutdown_listener(listener: &mut TcpListener) -> Handled {
    if listener.terminal {
        listener.listener_id = None;
        listener.pending_connections.clear();
        return Handled::Ok;
    }

    let Some(listener_id) = listener.listener_id.take() else {
        listener.pending_connections.clear();
        return Handled::Ok;
    };

    let release = listener.driver.release_tcp_listener(listener_id);
    listener.pending_connections.clear();
    Handled::block_on(listener, move |async_self| async move {
        match resolve_kfuture(release).await {
            Ok(()) => {}
            Err(Error::UnknownListener { .. }) => {}
            Err(error) => {
                warn!(
                    async_self.log(),
                    "failed to release TCP listener {} during shutdown: {}", listener_id, error
                );
            }
        }
    })
}
