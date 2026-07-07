//! Shared endpoint-discovery helpers for components using the runtime UDP endpoint.
//!
//! Route establishment sends small endpoint-discovery frames through route transport, while
//! direct key-material discovery shares the local endpoint binding for raw UDP traffic. This module
//! owns the neutral route and endpoint helpers so those components only keep their semantic state.

use crate::{
    DatagramRouteScope,
    RoutePreferenceRank,
    RouteSendId,
    RouteSharingKind,
    RouteTransportActorMessage,
    RouteTransportInboundDeliver,
    RouteTransportSend,
    RouteTransportSubmitResult,
    SendRouteCandidate,
    TransportRouteKey,
    UdpRouteKey,
};
use flotsync_io::prelude::SocketId;
use flotsync_messages::serialisation::FlotsyncSerializable;
use kompact::{
    KompactLogger,
    prelude::{ActorRefStrong, Ask, debug, error, trace},
};
use std::{net::SocketAddr, sync::Arc};
use uuid::Uuid;

/// Current shared UDP endpoint binding available for endpoint-discovery traffic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LocalUdpEndpointBinding {
    /// Socket id owned by the shared UDP runtime endpoint.
    pub(crate) socket_id: SocketId,
    /// Concrete local address observed after bind.
    pub(crate) local_addr: SocketAddr,
}

/// Local endpoint state observed from route transport lifecycle notifications.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum LocalUdpEndpointState {
    /// The shared UDP runtime endpoint has not been observed yet.
    #[default]
    Unbound,
    /// The shared UDP runtime endpoint is available for endpoint-discovery traffic.
    Bound(LocalUdpEndpointBinding),
}

impl LocalUdpEndpointState {
    /// Return the active endpoint binding, if one is available.
    pub(crate) fn binding(self) -> Option<LocalUdpEndpointBinding> {
        match self {
            Self::Bound(binding) => Some(binding),
            Self::Unbound => None,
        }
    }
}

/// Build the exclusive UDP route-transport candidate used for one endpoint-discovery send.
pub(crate) fn udp_route_transport_candidate(
    endpoint: LocalUdpEndpointBinding,
    remote_addr: SocketAddr,
) -> SendRouteCandidate<TransportRouteKey> {
    SendRouteCandidate {
        coverage_key: TransportRouteKey::Udp(UdpRouteKey {
            remote_addr,
            scope: DatagramRouteScope::Unicast,
            local_bind: Some(endpoint.local_addr),
        }),
        sharing: RouteSharingKind::Exclusive,
        preference_rank: RoutePreferenceRank::new(1),
    }
}

/// Extract the remote UDP source from one inbound route-transport delivery.
pub(crate) fn route_transport_inbound_source(
    inbound: &RouteTransportInboundDeliver<TransportRouteKey>,
) -> Option<SocketAddr> {
    if let Some(remote_addr) = inbound.transport.remote_addr {
        Some(remote_addr)
    } else {
        match inbound.transport.route {
            TransportRouteKey::Udp(route) => Some(route.remote_addr),
            TransportRouteKey::Tcp(_) => None,
        }
    }
}

/// Submit one endpoint-discovery frame through route transport.
///
/// Returns `true` only when route transport accepts the submit and reports it as sent. Returns
/// `false` when route transport reports a send failure or drops the submit promise.
pub(crate) async fn submit_endpoint_discovery_frame(
    route_transport: &ActorRefStrong<RouteTransportActorMessage<TransportRouteKey>>,
    log: &KompactLogger,
    owner: &'static str,
    endpoint: LocalUdpEndpointBinding,
    target: SocketAddr,
    payload: Arc<dyn FlotsyncSerializable>,
    label: &'static str,
) -> bool {
    let route = udp_route_transport_candidate(endpoint, target);
    let send = RouteTransportSend {
        send_id: RouteSendId(Uuid::new_v4()),
        route,
        payload,
    };
    let future = route_transport
        .ask_with(|promise| RouteTransportActorMessage::Submit(Ask::new(promise, send)));
    match future.await {
        Ok(RouteTransportSubmitResult::Sent { coverage_key }) => {
            trace!(
                log,
                "{} submitted {} through route transport via {:?}", owner, label, coverage_key
            );
            true
        }
        Ok(RouteTransportSubmitResult::SendFailed {
            coverage_key,
            reason,
        }) => {
            debug!(
                log,
                "{} {} transport submit failed via {:?}: {:?}", owner, label, coverage_key, reason
            );
            false
        }
        Err(_error) => {
            error!(
                log,
                "{} {} transport submit promise was dropped", owner, label
            );
            false
        }
    }
}
