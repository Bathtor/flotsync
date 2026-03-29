//! Transport-manager component sketch.
//!
//! This component sketch intentionally stops at state ownership, port
//! boundaries, and minimal handler skeletons. It does not yet implement route
//! opening, send execution, or connection-failure reporting.

use super::*;
use crate::delivery::shared::RouteSendId;
use flotsync_io::prelude::{IoBridgeHandle, SocketId, TcpSessionRef, UdpIndication, UdpPort};
use kompact::{Never, prelude::*};
use std::collections::HashMap;

type TransportConnectionInfoPort = ConnectionInfoPort<TransportRouteKey>;
type TransportRouteTransportSend = RouteTransportSend<TransportRouteKey>;
type TransportRouteTransportPort = RouteTransportPort<TransportRouteKey>;

/// Sketch of the concrete route-transport manager for the current UDP/TCP key type.
///
/// The intended ownership split is:
///
/// - semantic delivery provides logical sends on `TransportRouteTransportPort`
/// - discovery requires `TransportConnectionInfoPort` and receives
///   route-health indications from this manager
/// - the manager owns live UDP/TCP handle reuse keyed by `TransportRouteKey`
#[derive(ComponentDefinition)]
pub struct RouteTransportManager {
    ctx: ComponentContext<Self>,
    transport_port: ProvidedPort<TransportRouteTransportPort>,
    connection_info_port: ProvidedPort<TransportConnectionInfoPort>,
    udp_port: RequiredPort<UdpPort>,
    /// Shared bridge handle used to open transport resources on demand.
    bridge: IoBridgeHandle,
    /// Current live UDP route table keyed by the concrete UDP route key.
    udp_routes: HashMap<UdpRouteKey, LiveUdpRouteHandle>,
    /// Current live TCP route table keyed by the concrete TCP route key.
    tcp_routes: HashMap<TcpRouteKey, LiveTcpRouteHandle>,
    /// Logical sends that are still waiting for one concrete route outcome.
    pending_sends: HashMap<RouteSendId, PendingRouteSend>,
}

impl RouteTransportManager {
    /// Creates one manager sketch around the shared bridge handle.
    pub fn new(bridge: IoBridgeHandle) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            transport_port: ProvidedPort::uninitialised(),
            connection_info_port: ProvidedPort::uninitialised(),
            udp_port: RequiredPort::uninitialised(),
            bridge,
            udp_routes: HashMap::new(),
            tcp_routes: HashMap::new(),
            pending_sends: HashMap::new(),
        }
    }
}

impl ComponentLifecycle for RouteTransportManager {
    fn on_start(&mut self) -> Handled {
        Handled::Ok
    }

    fn on_stop(&mut self) -> Handled {
        Handled::Ok
    }

    fn on_kill(&mut self) -> Handled {
        Handled::Ok
    }
}

impl Provide<TransportRouteTransportPort> for RouteTransportManager {
    fn handle(&mut self, request: TransportRouteTransportSend) -> Handled {
        // TODO(flotsync-nzz): Resolve the concrete route kind, open or reuse a
        // live handle, serialize the payload, and drive one concrete transport
        // attempt. For now the sketch only records ownership of the logical send.
        self.pending_sends
            .insert(request.send_id, PendingRouteSend { send: request });
        Handled::Ok
    }
}

impl Provide<TransportConnectionInfoPort> for RouteTransportManager {
    fn handle(&mut self, request: Never) -> Handled {
        match request {}
    }
}

impl Require<UdpPort> for RouteTransportManager {
    fn handle(&mut self, indication: UdpIndication) -> Handled {
        let _ = indication;
        // TODO(flotsync-nzz): Translate concrete UDP outcomes into route-local
        // send results and, when necessary, emit `ReportRouteFailed` to
        // discovery through `connection_info`.
        Handled::Ok
    }
}

impl LocalActor for RouteTransportManager {
    type Message = Never;

    fn receive(&mut self, _msg: Self::Message) -> Handled {
        unreachable!("Cannot instantiate Never type");
    }
}

impl_local_actor!(RouteTransportManager);

/// Current UDP payload mode selected for one live UDP route.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)]
enum UdpPayloadMode {
    SingleDatagram,
    Multipart,
}

/// Live UDP route handle owned by the manager.
#[derive(Clone, Debug)]
#[allow(dead_code)]
struct LiveUdpRouteHandle {
    route: UdpRouteKey,
    socket_id: SocketId,
    payload_mode: UdpPayloadMode,
}

/// Live TCP route handle owned by the manager.
#[derive(Clone, Debug)]
#[allow(dead_code)]
struct LiveTcpRouteHandle {
    route: TcpRouteKey,
    session: Option<TcpSessionRef>,
    open_in_flight: bool,
}

/// Logical send remembered while one concrete route attempt is still in flight.
#[derive(Clone, Debug)]
#[allow(dead_code)]
struct PendingRouteSend {
    send: TransportRouteTransportSend,
}
