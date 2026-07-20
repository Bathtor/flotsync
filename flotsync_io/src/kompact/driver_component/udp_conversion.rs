//! Conversion from raw UDP driver events to Kompact bridge events.

#[allow(
    clippy::wildcard_imports,
    reason = "The private helper module intentionally shares its parent's local implementation vocabulary."
)]
use super::*;

/// Return the socket that produced a raw UDP event.
pub(super) fn udp_event_socket_id(event: &UdpEvent) -> SocketId {
    match event {
        UdpEvent::Bound { socket_id, .. }
        | UdpEvent::BindFailed { socket_id, .. }
        | UdpEvent::Connected { socket_id, .. }
        | UdpEvent::ConnectFailed { socket_id, .. }
        | UdpEvent::Received { socket_id, .. }
        | UdpEvent::SendAck { socket_id, .. }
        | UdpEvent::SendNack { socket_id, .. }
        | UdpEvent::Configured { socket_id, .. }
        | UdpEvent::ConfigureFailed { socket_id, .. }
        | UdpEvent::ReadSuspended { socket_id }
        | UdpEvent::ReadResumed { socket_id }
        | UdpEvent::WriteSuspended { socket_id }
        | UdpEvent::WriteResumed { socket_id }
        | UdpEvent::Closed { socket_id, .. } => *socket_id,
    }
}

/// Convert a raw UDP event to the bridge-facing representation.
pub(super) fn udp_bridge_event_from_raw(event: UdpEvent) -> UdpBridgeEvent {
    match event {
        UdpEvent::Bound {
            socket_id,
            local_addr,
        } => UdpBridgeEvent::Bound {
            socket_id,
            local_addr,
        },
        UdpEvent::BindFailed {
            socket_id,
            local_addr,
            error_kind,
        } => UdpBridgeEvent::BindFailed {
            socket_id,
            local_addr,
            reason: OpenFailureReason::Io(error_kind),
        },
        UdpEvent::Connected {
            socket_id,
            local_addr,
            remote_addr,
        } => UdpBridgeEvent::Connected {
            socket_id,
            local_addr,
            remote_addr,
        },
        UdpEvent::ConnectFailed {
            socket_id,
            local_addr,
            remote_addr,
            error_kind,
        } => UdpBridgeEvent::ConnectFailed {
            socket_id,
            local_addr,
            remote_addr,
            reason: OpenFailureReason::Io(error_kind),
        },
        UdpEvent::Received {
            socket_id,
            source,
            payload,
        } => UdpBridgeEvent::Received {
            socket_id,
            source,
            payload,
        },
        UdpEvent::SendAck {
            socket_id,
            transmission_id,
        } => UdpBridgeEvent::SendAck {
            socket_id,
            transmission_id,
        },
        UdpEvent::SendNack {
            socket_id,
            transmission_id,
            reason,
        } => UdpBridgeEvent::SendNack {
            socket_id,
            transmission_id,
            reason,
        },
        UdpEvent::Configured { socket_id, option } => {
            UdpBridgeEvent::Configured { socket_id, option }
        }
        UdpEvent::ConfigureFailed {
            socket_id,
            option,
            error_kind,
        } => UdpBridgeEvent::ConfigureFailed {
            socket_id,
            option,
            reason: super::super::types::ConfigureFailureReason::Io(error_kind),
        },
        UdpEvent::ReadSuspended { socket_id } => UdpBridgeEvent::ReadSuspended { socket_id },
        UdpEvent::ReadResumed { socket_id } => UdpBridgeEvent::ReadResumed { socket_id },
        UdpEvent::WriteSuspended { socket_id } => UdpBridgeEvent::WriteSuspended { socket_id },
        UdpEvent::WriteResumed { socket_id } => UdpBridgeEvent::WriteResumed { socket_id },
        UdpEvent::Closed {
            socket_id,
            remote_addr,
            reason,
        } => UdpBridgeEvent::Closed {
            socket_id,
            remote_addr,
            reason,
        },
    }
}
