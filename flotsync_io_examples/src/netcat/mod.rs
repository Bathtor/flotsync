use crate::app::ExampleRuntime;
use bytes::Bytes;
use clap::{Args, Parser, Subcommand};
use flotsync_io::prelude::{EgressPool, IoPayload};
use kompact::prelude::*;
use snafu::{Whatever, prelude::*};
use std::{
    io::{self, BufRead, BufReader, Write},
    net::SocketAddr,
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

mod tcp;
mod udp;

const CONTROL_TIMEOUT: Duration = Duration::from_secs(2);
const SCRIPTED_EXIT_GRACE: Duration = Duration::from_millis(200);

/// Result type used by the netcat-style example binary.
pub type Result<T, E = Error> = std::result::Result<T, E>;
pub type Error = Whatever;

/// Top-level command-line arguments for the netcat-style example.
#[derive(Debug, Parser)]
#[command(name = "netcat", version, about = "flotsync_io netcat-style example")]
pub struct NetcatArgs {
    /// Strings to send without reading stdin.
    #[arg(long = "send")]
    send: Vec<String>,
    /// Exit automatically once scripted sends have drained and a short quiet period elapsed.
    #[arg(long)]
    exit_after_send: bool,
    #[command(subcommand)]
    mode: NetcatMode,
}

/// Supported transport modes for the example.
#[derive(Debug, Subcommand)]
pub enum NetcatMode {
    Tcp(TcpArgs),
    Udp(UdpArgs),
}

/// TCP-specific CLI arguments.
#[derive(Debug, Args)]
pub struct TcpArgs {
    #[command(subcommand)]
    mode: TcpMode,
}

/// TCP session/listener subcommands.
#[derive(Debug, Subcommand)]
pub enum TcpMode {
    /// Open one outbound TCP session.
    Connect {
        #[arg(long)]
        remote: SocketAddr,
        #[arg(long)]
        bind: Option<SocketAddr>,
    },
    /// Open one TCP listener and accept at most one active session at a time.
    Listen {
        #[arg(long)]
        bind: SocketAddr,
    },
}

/// UDP-specific CLI arguments.
#[derive(Debug, Args)]
pub struct UdpArgs {
    #[command(subcommand)]
    mode: UdpMode,
}

/// UDP bind/connect subcommands.
#[derive(Debug, Subcommand)]
pub enum UdpMode {
    /// Use one connected UDP socket.
    Connect {
        #[arg(long)]
        remote: SocketAddr,
        #[arg(long)]
        bind: Option<SocketAddr>,
    },
    /// Bind one UDP socket and optionally send to one default target.
    Bind {
        #[arg(long)]
        bind: SocketAddr,
        #[arg(long)]
        target: Option<SocketAddr>,
    },
    /// Use one unconnected UDP socket that always sends to the same target.
    #[command(name = "sendto")]
    SendTo {
        #[arg(long)]
        target: SocketAddr,
        #[arg(long)]
        bind: Option<SocketAddr>,
    },
}

/// Input arriving from scripted sends or the optional stdin reader thread.
#[derive(Debug)]
enum NetcatInput {
    Line(String),
    Closed,
}

/// Shared terminal outcome signal written by the example component before it shuts down.
///
/// The outer process only needs to know whether the component finished cleanly or with a terminal
/// error. We wait for that signal directly instead of using `Component::wait_ended()`, because
/// Kompact only reports a component as "ended" once it is fully destroyed, and that can lag behind
/// a clean `DieNow` if other references are still alive briefly.
type OutcomeSlot = Arc<OutcomeSignal>;

/// One tiny synchronisation cell for terminal component outcomes.
struct OutcomeSignal {
    state: Mutex<Option<std::result::Result<(), String>>>,
    ready: Condvar,
}

/// One scheduled scripted-shutdown timer together with the generation that armed it.
///
/// The generation lets components ignore stale timeout callbacks that were already queued when the
/// timer got cleared or replaced.
struct ShutdownTimerState {
    generation: usize,
    timer: ScheduledTimer,
}

/// Runs the netcat-style example according to `args`.
pub fn run(args: NetcatArgs) -> Result<()> {
    let runtime = ExampleRuntime::setup()?;
    let run_result = match args.mode {
        NetcatMode::Udp(udp) => udp::run_udp(&runtime, args.send, args.exit_after_send, udp.mode),
        NetcatMode::Tcp(tcp) => tcp::run_tcp(&runtime, args.send, args.exit_after_send, tcp.mode),
    };
    let shutdown_result = runtime.shutdown();
    run_result?;
    shutdown_result?;
    Ok(())
}

fn new_outcome_slot() -> OutcomeSlot {
    Arc::new(OutcomeSignal {
        state: Mutex::new(None),
        ready: Condvar::new(),
    })
}

fn record_success(slot: &OutcomeSlot) {
    let mut guard = slot.state.lock().expect("netcat outcome lock");
    if guard.is_none() {
        *guard = Some(Ok(()));
        slot.ready.notify_all();
    }
}

fn record_failure(slot: &OutcomeSlot, message: impl Into<String>) {
    let mut guard = slot.state.lock().expect("netcat outcome lock");
    if guard.is_none() {
        *guard = Some(Err(message.into()));
        slot.ready.notify_all();
    }
}

fn start_component<C>(runtime: &ExampleRuntime, component: &Arc<Component<C>>) -> Result<()>
where
    C: ComponentDefinition + Actor + Sized + 'static,
{
    let start_driver = runtime.system().start_notify(runtime.driver_component());
    whatever!(
        start_driver.wait_timeout(CONTROL_TIMEOUT),
        "timed out waiting for the shared IoDriverComponent to start"
    );

    let start_bridge = runtime.system().start_notify(runtime.bridge_component());
    whatever!(
        start_bridge.wait_timeout(CONTROL_TIMEOUT),
        "timed out waiting for the shared IoBridge to start"
    );

    let start_component = runtime.system().start_notify(component);
    whatever!(
        start_component.wait_timeout(CONTROL_TIMEOUT),
        "timed out waiting for the netcat component to start"
    );

    Ok(())
}

fn wait_for_component_outcome<C>(component: &Arc<Component<C>>, outcome: &OutcomeSlot) -> Result<()>
where
    C: ComponentDefinition + Sized + 'static,
{
    let mut guard = outcome.state.lock().expect("netcat outcome lock");
    loop {
        if let Some(result) = guard.take() {
            return match result {
                Ok(()) => Ok(()),
                Err(message) => whatever!("{message}"),
            };
        }
        if component.is_faulty() {
            whatever!("netcat component faulted");
        }
        if component.is_destroyed() {
            whatever!("netcat component exited without recording an outcome");
        }
        let (next_guard, _) = outcome
            .ready
            .wait_timeout(guard, Duration::from_millis(100))
            .expect("netcat outcome wait");
        guard = next_guard;
    }
}

async fn encode_line_payload(
    egress_pool: EgressPool,
    line: String,
) -> flotsync_io::errors::Result<IoPayload> {
    if line.is_empty() {
        return Ok(IoPayload::Bytes(Bytes::new()));
    }

    let reservation_request = egress_pool.reserve(line.len())?;
    let reservation = reservation_request.await?;
    let (_, lease) = reservation.write_with(|writer| {
        writer.put_slice(line.as_bytes());
        Ok(())
    })?;
    Ok(IoPayload::Lease(lease))
}

fn install_input_source<M>(actor_ref: ActorRef<M>, scripted_lines: Vec<String>)
where
    M: MessageBounds + From<NetcatInput> + Send + 'static,
{
    if scripted_lines.is_empty() {
        spawn_stdin_reader(actor_ref);
        return;
    }

    for line in scripted_lines {
        let message: M = NetcatInput::Line(line).into();
        actor_ref.tell(message);
    }
    actor_ref.tell(NetcatInput::Closed);
}

fn spawn_stdin_reader<M>(actor_ref: ActorRef<M>)
where
    M: MessageBounds + From<NetcatInput> + Send + 'static,
{
    std::thread::spawn(move || {
        let mut reader = BufReader::new(io::stdin());
        loop {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => {
                    actor_ref.tell(NetcatInput::Closed);
                    return;
                }
                Ok(_) => {
                    actor_ref.tell(NetcatInput::Line(line));
                }
                Err(error) => {
                    log::warn!("stdin read failed: {error}");
                    actor_ref.tell(NetcatInput::Closed);
                    return;
                }
            }
        }
    });
}

fn payload_string(payload: IoPayload) -> String {
    let bytes = match payload {
        IoPayload::Lease(lease) => lease.create_byte_clone(),
        IoPayload::Bytes(bytes) => bytes,
        _ => unimplemented!("Unsupported IoPayload type"),
    };
    String::from_utf8_lossy(&bytes).into_owned()
}

fn print_payload(payload: IoPayload) {
    let text = payload_string(payload);
    if text.ends_with('\n') {
        print!("{text}");
    } else {
        println!("{text}");
    }
    let _ = io::stdout().flush();
}
