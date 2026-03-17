use super::{
    DriverEventSink,
    DriverThreadConfig,
    WAKE_TOKEN,
    runtime::{DriverRuntimeState, ResourceKey},
};
use crate::{
    api::MAX_UDP_PAYLOAD_BYTES,
    errors::{DriverPollSnafu, Result},
    pool::IngressPool,
};
use mio::{Events, Poll};
use snafu::ResultExt;

pub(super) fn run_driver_thread(
    config: DriverThreadConfig,
    mut poll: Poll,
    command_rx: crossbeam_channel::Receiver<super::runtime::ControlCommand>,
    event_sink: std::sync::Arc<dyn DriverEventSink>,
    startup_tx: std::sync::mpsc::SyncSender<Result<()>>,
    ingress_pool: IngressPool,
) -> Result<()> {
    let mut state = DriverRuntimeState::default();
    let mut events = Events::with_capacity(config.events_capacity.max(1));
    // This scratch buffer is stack-allocated on purpose: 1472 bytes is small for a dedicated
    // driver thread stack, avoids a permanent heap allocation in the hot path, and matches the
    // current conservative UDP payload cap. Revisit this if we raise the payload ceiling
    // materially or add jump-frame support with tighter stack budgets.
    let mut udp_send_scratch = [0_u8; MAX_UDP_PAYLOAD_BYTES];

    log::debug!(
        "flotsync_io driver thread entering poll loop with event capacity {}",
        events.capacity()
    );

    if startup_tx.send(Ok(())).is_err() {
        log::warn!("flotsync_io driver startup receiver was dropped before readiness signal");
    }

    loop {
        #[cfg(test)]
        {
            state.test_state.poll_iterations += 1;
        }

        poll.poll(&mut events, config.poll_timeout)
            .context(DriverPollSnafu)?;

        'event_loop: for event in &events {
            if event.token() == WAKE_TOKEN {
                #[cfg(test)]
                {
                    state.test_state.wakeup_count += 1;
                }
                let should_stop = state.drain_commands(
                    &command_rx,
                    poll.registry(),
                    event_sink.as_ref(),
                    &mut udp_send_scratch,
                )?;
                if should_stop {
                    log::info!("flotsync_io driver thread leaving poll loop");
                    return Ok(());
                }
                state.resume_suspended_udp_reads(
                    poll.registry(),
                    &ingress_pool,
                    event_sink.as_ref(),
                )?;
                state.resume_suspended_tcp_reads(
                    poll.registry(),
                    &ingress_pool,
                    event_sink.as_ref(),
                )?;
                continue 'event_loop;
            }

            state.record_ready_hit(event.token());
            let Some(key) = state.readiness_key(event.token()) else {
                continue 'event_loop;
            };
            match key {
                ResourceKey::Connection(connection_id) => {
                    state.handle_tcp_ready(
                        connection_id,
                        poll.registry(),
                        &ingress_pool,
                        event_sink.as_ref(),
                        event.is_readable(),
                        event.is_writable(),
                    )?;
                }
                ResourceKey::Socket(socket_id) if event.is_readable() => {
                    state.handle_udp_readable(
                        socket_id,
                        poll.registry(),
                        &ingress_pool,
                        event_sink.as_ref(),
                    )?;
                }
                ResourceKey::Listener(_) | ResourceKey::Socket(_) => {}
            }
        }
    }
}

pub(super) fn send_reply<T>(
    reply_tx: futures_channel::oneshot::Sender<Result<T>>,
    reply: Result<T>,
    operation: &str,
) {
    if reply_tx.send(reply).is_err() {
        log::debug!(
            "dropping flotsync_io {} reply because the receiver was already gone",
            operation
        );
    }
}

pub(super) fn panic_payload_to_string(payload: Box<dyn std::any::Any + Send + 'static>) -> String {
    match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(message) => (*message).to_string(),
            Err(_) => "non-string panic payload".to_string(),
        },
    }
}
