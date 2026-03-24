use super::{
    driver_component::DriverComponentRef,
    resolve_kfuture,
    types::{
        OpenFailureReason,
        OpenedTcpSession,
        TcpSessionEvent,
        TcpSessionEventTarget,
        TcpSessionRef,
        TcpSessionRequest,
    },
};
use crate::{
    api::{CloseReason, ConnectionId, SendFailureReason, TcpCommand},
    errors::Error,
    pool::EgressPool,
};
use ::kompact::prelude::*;
use std::net::SocketAddr;

/// Internal session-directed event routed from the shared driver component.
///
/// The Kompact public session event stream intentionally omits establishment events. The session
/// component therefore receives these richer internal events, resolves its open promise if it has
/// one, and forwards only the steady-state session events to `events_to`.
#[derive(Clone, Debug)]
pub(crate) enum TcpSessionDriverEvent {
    Opened {
        connection_id: ConnectionId,
        peer_addr: SocketAddr,
    },
    OpenFailed {
        reason: OpenFailureReason,
    },
    Received {
        payload: crate::api::IoPayload,
    },
    SendAck {
        transmission_id: crate::api::TransmissionId,
    },
    SendNack {
        transmission_id: crate::api::TransmissionId,
        reason: SendFailureReason,
    },
    ReadSuspended,
    ReadResumed,
    WriteSuspended,
    WriteResumed,
    Closed {
        reason: CloseReason,
    },
}

/// Internal mailbox for one TCP session component.
///
/// The session actor only accepts local control and driver-routing messages. It does not expose a
/// network actor surface because all real I/O happens inside the shared `flotsync_io` driver.
#[derive(Debug)]
pub(crate) enum TcpSessionMessage {
    Request(TcpSessionRequest),
    DriverEvent(TcpSessionDriverEvent),
}

impl From<TcpSessionRequest> for TcpSessionMessage {
    fn from(request: TcpSessionRequest) -> Self {
        Self::Request(request)
    }
}

/// Kompact component that represents one live outbound TCP session.
///
/// This component owns the Kompact-facing session identity while the raw driver owns the actual
/// socket and `ConnectionId`. The component terminates itself after terminal events so the bridge
/// does not need a separate garbage-collection mechanism for the happy path.
#[derive(ComponentDefinition)]
pub(crate) struct TcpSession {
    ctx: ComponentContext<Self>,
    driver: DriverComponentRef,
    egress_pool: EgressPool,
    events_to: TcpSessionEventTarget,
    connection_id: Option<ConnectionId>,
    open_promise: Option<KPromise<std::result::Result<OpenedTcpSession, OpenFailureReason>>>,
    opened: bool,
    terminal: bool,
}

impl TcpSession {
    pub(crate) fn attached(
        driver: DriverComponentRef,
        events_to: TcpSessionEventTarget,
        connection_id: ConnectionId,
        egress_pool: EgressPool,
    ) -> Self {
        Self::with_connection_and_open_promise(
            driver,
            events_to,
            Some(connection_id),
            egress_pool,
            None,
        )
    }

    pub(crate) fn with_open_promise(
        driver: DriverComponentRef,
        events_to: TcpSessionEventTarget,
        egress_pool: EgressPool,
        open_promise: Option<KPromise<std::result::Result<OpenedTcpSession, OpenFailureReason>>>,
    ) -> Self {
        Self::with_connection_and_open_promise(driver, events_to, None, egress_pool, open_promise)
    }

    fn with_connection_and_open_promise(
        driver: DriverComponentRef,
        events_to: TcpSessionEventTarget,
        connection_id: Option<ConnectionId>,
        egress_pool: EgressPool,
        open_promise: Option<KPromise<std::result::Result<OpenedTcpSession, OpenFailureReason>>>,
    ) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            driver,
            egress_pool,
            events_to,
            connection_id,
            open_promise,
            opened: connection_id.is_some(),
            terminal: false,
        }
    }

    fn handle_request(&mut self, request: TcpSessionRequest) -> Handled {
        match request {
            TcpSessionRequest::Send {
                transmission_id,
                payload,
            } => self.handle_send_request(transmission_id, payload),
            TcpSessionRequest::SendAndClose {
                transmission_id,
                payload,
            } => self.handle_send_and_close_request(transmission_id, payload),
            TcpSessionRequest::Close { abort } => self.handle_close_request(abort),
        }
    }

    fn handle_send_request(
        &mut self,
        transmission_id: crate::api::TransmissionId,
        payload: crate::api::IoPayload,
    ) -> Handled {
        let Some(connection_id) = self.connection_id else {
            self.events_to.tell(TcpSessionEvent::SendNack {
                transmission_id,
                reason: SendFailureReason::InvalidState,
            });
            return Handled::Ok;
        };
        if self.terminal {
            self.events_to.tell(TcpSessionEvent::SendNack {
                transmission_id,
                reason: SendFailureReason::Closed,
            });
            return Handled::Ok;
        }

        self.driver.dispatch_tcp(TcpCommand::Send {
            connection_id,
            transmission_id,
            payload,
        });
        Handled::Ok
    }

    fn handle_send_and_close_request(
        &mut self,
        transmission_id: crate::api::TransmissionId,
        payload: crate::api::IoPayload,
    ) -> Handled {
        let Some(connection_id) = self.connection_id else {
            self.events_to.tell(TcpSessionEvent::SendNack {
                transmission_id,
                reason: SendFailureReason::InvalidState,
            });
            return Handled::Ok;
        };
        if self.terminal {
            self.events_to.tell(TcpSessionEvent::SendNack {
                transmission_id,
                reason: SendFailureReason::Closed,
            });
            return Handled::Ok;
        }

        self.driver.dispatch_tcp(TcpCommand::SendAndClose {
            connection_id,
            transmission_id,
            payload,
        });
        Handled::Ok
    }

    fn handle_close_request(&mut self, abort: bool) -> Handled {
        let Some(connection_id) = self.connection_id else {
            self.fulfil_open_failure(OpenFailureReason::DriverUnavailable);
            if self.opened {
                self.events_to.tell(TcpSessionEvent::Closed {
                    reason: CloseReason::DriverShutdown,
                });
            }
            self.terminal = true;
            self.ctx.suicide();
            return Handled::Ok;
        };
        if self.terminal {
            return Handled::Ok;
        }

        self.driver.dispatch_tcp(TcpCommand::Close {
            connection_id,
            abort,
        });
        Handled::Ok
    }

    fn handle_driver_event(&mut self, event: TcpSessionDriverEvent) -> Handled {
        match event {
            TcpSessionDriverEvent::Opened {
                connection_id,
                peer_addr,
            } => {
                self.connection_id = Some(connection_id);
                self.opened = true;
                self.fulfil_open_success(peer_addr);
                Handled::Ok
            }
            TcpSessionDriverEvent::OpenFailed { reason } => {
                self.fulfil_open_failure(reason);
                self.terminal = true;
                self.ctx.suicide();
                Handled::Ok
            }
            TcpSessionDriverEvent::Received { payload } => {
                self.events_to.tell(TcpSessionEvent::Received { payload });
                Handled::Ok
            }
            TcpSessionDriverEvent::SendAck { transmission_id } => {
                self.events_to
                    .tell(TcpSessionEvent::SendAck { transmission_id });
                Handled::Ok
            }
            TcpSessionDriverEvent::SendNack {
                transmission_id,
                reason,
            } => {
                self.events_to.tell(TcpSessionEvent::SendNack {
                    transmission_id,
                    reason,
                });
                Handled::Ok
            }
            TcpSessionDriverEvent::ReadSuspended => {
                self.events_to.tell(TcpSessionEvent::ReadSuspended);
                Handled::Ok
            }
            TcpSessionDriverEvent::ReadResumed => {
                self.events_to.tell(TcpSessionEvent::ReadResumed);
                Handled::Ok
            }
            TcpSessionDriverEvent::WriteSuspended => {
                self.events_to.tell(TcpSessionEvent::WriteSuspended);
                Handled::Ok
            }
            TcpSessionDriverEvent::WriteResumed => {
                self.events_to.tell(TcpSessionEvent::WriteResumed);
                Handled::Ok
            }
            TcpSessionDriverEvent::Closed { reason } => {
                self.events_to.tell(TcpSessionEvent::Closed { reason });
                self.terminal = true;
                self.ctx.suicide();
                Handled::Ok
            }
        }
    }

    fn fulfil_open_success(&mut self, peer_addr: SocketAddr) {
        let Some(promise) = self.open_promise.take() else {
            return;
        };
        let actor = self
            .actor_ref()
            .hold()
            .expect("TCP session must be live while fulfilling the open result");
        let opened = OpenedTcpSession {
            session: TcpSessionRef::new(actor, self.egress_pool.clone()),
            peer_addr,
        };
        if promise.fulfil(Ok(opened)).is_err() {
            debug!(self.log(), "dropping TCP session open reply");
        }
    }

    fn fulfil_open_failure(&mut self, reason: OpenFailureReason) {
        let Some(promise) = self.open_promise.take() else {
            return;
        };
        if promise.fulfil(Err(reason)).is_err() {
            debug!(self.log(), "dropping TCP session open reply");
        }
    }
}

impl ComponentLifecycle for TcpSession {
    fn on_stop(&mut self) -> Handled {
        shutdown_session(self)
    }

    fn on_kill(&mut self) -> Handled {
        shutdown_session(self)
    }
}

impl Actor for TcpSession {
    type Message = TcpSessionMessage;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        match msg {
            TcpSessionMessage::Request(request) => self.handle_request(request),
            TcpSessionMessage::DriverEvent(event) => self.handle_driver_event(event),
        }
    }

    fn receive_network(&mut self, _msg: NetMessage) -> Handled {
        unimplemented!("flotsync_io TCP sessions do not use network actor messages");
    }
}

fn shutdown_session(session: &mut TcpSession) -> Handled {
    let Some(connection_id) = session.connection_id.take() else {
        return Handled::Ok;
    };
    if session.terminal {
        return Handled::Ok;
    }

    let release = session.driver.release_tcp_session(connection_id);
    Handled::block_on(session, move |async_self| async move {
        match resolve_kfuture(release).await {
            Ok(()) => {}
            Err(Error::UnknownConnection { .. }) => {}
            Err(error) => {
                warn!(
                    async_self.log(),
                    "failed to release TCP session {} during shutdown: {}", connection_id, error
                );
            }
        }
    })
}
