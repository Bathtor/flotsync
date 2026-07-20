//! Local UDP/TCP route state owned by the route-transport manager.

#[allow(
    clippy::wildcard_imports,
    reason = "The private helper module intentionally shares its parent's local implementation vocabulary."
)]
use super::*;

/// Live UDP route handle owned by the manager.
pub(super) struct LiveUdpSocketHandle {
    pub(super) socket_id: SocketId,
    pub(super) runtime: Arc<Component<UDPourComponent>>,
    pub(super) runtime_ref: ActorRefStrong<UDPourComponentMessage>,
    pub(super) _transfer_channel: ProviderChannel<UDPourPort, UDPourComponent>,
    /// Number of raw UDP datagrams that were queued onto this child's required
    /// `UdpPort` before the child was started.
    ///
    /// This survives the `Starting -> Live` transition so tests and diagnostics
    /// can still tell whether the lazy startup path buffered anything.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) startup_buffered_datagram_count: usize,
    /// Every concrete UDP route that currently relies on this socket.
    pub(super) known_routes: HashSet<UdpRouteKey>,
    /// Broadcast enablement state for this shared socket.
    pub(super) broadcast_state: UdpBroadcastState,
}

/// Live TCP route handle owned by the manager.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(super) struct LiveTcpRouteHandle {
    pub(super) route: TcpRouteKey,
    pub(super) session: Option<()>,
    pub(super) open_in_flight: bool,
}

/// Logical send remembered while one concrete route attempt is still in flight.
pub(super) struct PendingRouteSend {
    pub(super) send: TransportRouteTransportSend,
    pub(super) submit_promise: Option<KPromise<TransportRouteTransportSubmitResult>>,
}

/// UDP route that has been requested but is not yet ready for sends.
pub(super) struct PendingUdpSocketOpen {
    pub(super) queued_sends: Vec<QueuedUdpSend>,
}

/// One bound UDP socket that exists on the shared bridge but has not yet
/// needed a `UDPour` child.
pub(super) struct DormantUdpSocketHandle {
    pub(super) socket_id: SocketId,
}

/// One bound UDP socket whose child runtime is still being connected and started.
pub(super) struct StartingUdpSocketHandle {
    pub(super) socket_id: SocketId,
    pub(super) runtime: Arc<Component<UDPourComponent>>,
    pub(super) runtime_ref: ActorRefStrong<UDPourComponentMessage>,
    pub(super) udp_port: RequiredRef<UdpPort>,
    pub(super) transfer_channel: ProviderChannel<UDPourPort, UDPourComponent>,
    /// Whether this starting socket came from a manager-owned bind or from an externally bound
    /// dormant socket that may need to be restored on activation failure.
    pub(super) origin: UdpSocketStartOrigin,
    /// Logical sends queued while the child runtime is not yet ready.
    pub(super) queued_sends: Vec<QueuedUdpSend>,
    /// Number of raw UDP datagrams queued onto the child before it was started.
    pub(super) buffered_datagram_count: usize,
}

/// Provenance of one per-socket `UDPour` child while it is still starting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum UdpSocketStartOrigin {
    /// The manager opened this socket itself and owns the whole resource lifecycle.
    ManagerOwned,
    /// The bridge reported an external bind and the manager is only attaching a child on demand.
    ExternalDormant,
}

/// One logical UDP send queued while the shared local socket is not fully ready.
#[derive(Clone, Copy)]
pub(super) struct QueuedUdpSend {
    pub(super) send_id: RouteSendId,
    pub(super) route: UdpRouteKey,
}

/// Current broadcast enablement state for one live shared UDP socket.
pub(super) enum UdpBroadcastState {
    Disabled,
    Enabling { queued_sends: Vec<QueuedUdpSend> },
    Enabled,
    Failed,
}

/// Action selected while deciding how a live shared UDP socket should handle one send.
pub(super) enum LiveUdpSendAction {
    Dispatch,
    Queued,
    ConfigureBroadcast { socket_id: SocketId },
    FailBroadcast,
}

/// Key for one reusable local UDP socket.
///
/// This intentionally collapses multiple full UDP route keys onto one local
/// socket whenever they would bind the same concrete local address. In
/// particular, `ForPeer(...)` policies are normalized to the concrete local
/// bind shape they resolve to so one child `UDPour` runtime can multiplex many
/// targets on the same socket.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) struct UdpSocketKey {
    pub(super) local_addr: SocketAddr,
}

impl UdpSocketKey {
    pub(super) fn for_route(route: UdpRouteKey) -> Self {
        let bind = match route.local_bind {
            Some(local_addr) => UdpLocalBind::Exact(local_addr),
            None => UdpLocalBind::ForPeer(route.remote_addr),
        };
        Self {
            local_addr: bind.resolve_local_addr(),
        }
    }

    pub(super) fn bind_policy(self) -> UdpLocalBind {
        UdpLocalBind::Exact(self.local_addr)
    }
}
