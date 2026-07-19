//! Conversion from raw TCP driver events to Kompact session events.

use super::*;

/// Convert a raw TCP session event and indicate whether it closes the session route.
pub(super) fn tcp_session_event_from_raw(
    event: TcpEvent,
) -> (ConnectionId, TcpSessionDriverEvent, bool) {
    match event {
        TcpEvent::Connected {
            connection_id,
            peer_addr,
        } => (
            connection_id,
            TcpSessionDriverEvent::Opened {
                connection_id,
                peer_addr,
            },
            false,
        ),
        TcpEvent::ConnectFailed {
            connection_id,
            remote_addr: _,
            error_kind,
        } => (
            connection_id,
            TcpSessionDriverEvent::OpenFailed {
                reason: OpenFailureReason::Io(error_kind),
            },
            true,
        ),
        TcpEvent::Received {
            connection_id,
            payload,
        } => (
            connection_id,
            TcpSessionDriverEvent::Received { payload },
            false,
        ),
        TcpEvent::SendAck {
            connection_id,
            transmission_id,
        } => (
            connection_id,
            TcpSessionDriverEvent::SendAck { transmission_id },
            false,
        ),
        TcpEvent::SendNack {
            connection_id,
            transmission_id,
            reason,
        } => (
            connection_id,
            TcpSessionDriverEvent::SendNack {
                transmission_id,
                reason,
            },
            false,
        ),
        TcpEvent::ReadSuspended { connection_id } => {
            (connection_id, TcpSessionDriverEvent::ReadSuspended, false)
        }
        TcpEvent::ReadResumed { connection_id } => {
            (connection_id, TcpSessionDriverEvent::ReadResumed, false)
        }
        TcpEvent::WriteSuspended { connection_id } => {
            (connection_id, TcpSessionDriverEvent::WriteSuspended, false)
        }
        TcpEvent::WriteResumed { connection_id } => {
            (connection_id, TcpSessionDriverEvent::WriteResumed, false)
        }
        TcpEvent::Closed {
            connection_id,
            reason,
        } => (
            connection_id,
            TcpSessionDriverEvent::Closed { reason },
            true,
        ),
        TcpEvent::ListenFailed { listener_id, .. }
        | TcpEvent::Listening { listener_id, .. }
        | TcpEvent::Accepted { listener_id, .. }
        | TcpEvent::ListenerClosed { listener_id } => unreachable!(
            "listener-related TCP events must not be routed through the TCP session adapter for {listener_id}"
        ),
    }
}
