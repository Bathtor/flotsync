use super::{
    driver_component::DriverComponentRef,
    resolve_kfuture,
    types::{TcpSessionEvent, TcpSessionRequest},
};
use crate::{
    api::{CloseReason, ConnectionId, SendFailureReason, TcpCommand},
    errors::Error,
};
use ::kompact::prelude::*;

/// Internal mailbox for one TCP session component.
///
/// The session actor only accepts local control and driver-routing messages. It does not expose a
/// network actor surface because all real I/O happens inside the shared `flotsync_io` driver.
#[derive(Debug)]
pub(crate) enum TcpSessionMessage {
    Request(TcpSessionRequest),
    Attach(ConnectionId),
    DriverEvent(TcpSessionEvent),
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
    events_to: Recipient<TcpSessionEvent>,
    connection_id: Option<ConnectionId>,
    terminal: bool,
}

impl TcpSession {
    pub(crate) fn new(driver: DriverComponentRef, events_to: Recipient<TcpSessionEvent>) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            driver,
            events_to,
            connection_id: None,
            terminal: false,
        }
    }

    fn handle_request(&mut self, request: TcpSessionRequest) -> Handled {
        match request {
            TcpSessionRequest::Send {
                transmission_id,
                payload,
            } => self.handle_send_request(transmission_id, payload),
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

    fn handle_close_request(&mut self, abort: bool) -> Handled {
        let Some(connection_id) = self.connection_id else {
            self.events_to.tell(TcpSessionEvent::Closed {
                reason: CloseReason::DriverShutdown,
            });
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

    fn handle_driver_event(&mut self, event: TcpSessionEvent) -> Handled {
        let terminal = matches!(
            event,
            TcpSessionEvent::ConnectFailed { .. } | TcpSessionEvent::Closed { .. }
        );
        self.events_to.tell(event);
        if terminal {
            self.terminal = true;
            self.ctx.suicide();
        }
        Handled::Ok
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
            TcpSessionMessage::Attach(connection_id) => {
                self.connection_id = Some(connection_id);
                Handled::Ok
            }
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
