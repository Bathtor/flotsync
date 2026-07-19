//! Shared probes and reserved-socket helpers for Kompact I/O tests.

use super::*;

#[derive(Clone, Debug)]
pub(super) struct TaggedSessionEvent {
    pub(super) tag: usize,
    pub(super) event: TcpSessionEvent,
}

pub(super) fn wrap_tagged_session_event(tag: usize, event: TcpSessionEvent) -> TaggedSessionEvent {
    TaggedSessionEvent { tag, event }
}
pub(super) fn hold_reusable_tcp_reservation() -> (Socket, SocketAddr) {
    let socket = Socket::new(
        crate::socket_support::socket_domain(localhost(0)),
        Type::STREAM,
        Some(Protocol::TCP),
    )
    .expect("create reusable TCP reservation socket");
    configure_bind_reuse(&socket).expect("enable TCP re-use on reservation socket");
    socket
        .bind(&SockAddr::from(localhost(0)))
        .expect("bind reusable TCP reservation socket");
    let local_addr = socket
        .local_addr()
        .expect("TCP reservation local addr")
        .as_socket()
        .expect("TCP reservation must use an IP socket address");
    (socket, local_addr)
}

pub(super) fn hold_reusable_udp_reservation() -> (Socket, SocketAddr) {
    let socket = Socket::new(
        crate::socket_support::socket_domain(localhost(0)),
        Type::DGRAM,
        Some(Protocol::UDP),
    )
    .expect("create reusable UDP reservation socket");
    configure_bind_reuse(&socket).expect("enable UDP re-use on reservation socket");
    socket
        .bind(&SockAddr::from(localhost(0)))
        .expect("bind reusable UDP reservation socket");
    let local_addr = socket
        .local_addr()
        .expect("UDP reservation local addr")
        .as_socket()
        .expect("UDP reservation must use an IP socket address");
    (socket, local_addr)
}

// TODO(flotsync-h1z0): Replace with a generic tagged actor-message probe.
#[derive(ComponentDefinition)]
pub(super) struct TaggedSessionEventProbe {
    ctx: ComponentContext<Self>,
    events: mpsc::Sender<TaggedSessionEvent>,
}

impl TaggedSessionEventProbe {
    pub(super) fn new(events: mpsc::Sender<TaggedSessionEvent>) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            events,
        }
    }
}

ignore_lifecycle!(TaggedSessionEventProbe);

impl Actor for TaggedSessionEventProbe {
    type Message = TaggedSessionEvent;

    fn receive_local(&mut self, msg: Self::Message) -> HandlerResult {
        self.events
            .send(msg)
            .expect("tagged TCP session event receiver must stay live during integration tests");
        Handled::OK
    }
}
