use clap::Parser;
#[cfg(feature = "zeroconf")]
use flotsync_discovery::services::{
    MDNS_ANNOUNCEMENT_SERVICE_DEFAULT_OPTIONS,
    MdnsAnnouncementComponent,
};
use flotsync_discovery::{
    endpoint_selection::EndpointSelection,
    kompact::prelude::*,
    services::{
        PEER_ANNOUNCEMENT_DEFAULT_OPTIONS,
        PeerAnnouncementComponent,
        peer_announcement_startup_signal,
    },
    uuid::Uuid,
};
use flotsync_io::prelude::{DriverConfig, IoRuntime};
use std::{
    io::{self, BufRead, BufReader},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Start the announcement service.
    #[arg(short, long)]
    active: bool,

    #[cfg(feature = "zeroconf")]
    /// Use zeroconf mDNS instead of a peer-announcement broadcast.
    #[arg(short, long)]
    mdns: bool,
    // Kompact's logger can't currently dynamically reconfigure the logging level.
    // /// Turn debugging information on
    // #[arg(short, long, action = clap::ArgAction::Count)]
    // debug: u8,
}
enum ActiveService {
    #[cfg(feature = "zeroconf")]
    Mdns {
        component: Arc<Component<MdnsAnnouncementComponent>>,
    },
    PeerAnnouncement {
        io_runtime: IoRuntime,
        component: Arc<Component<PeerAnnouncementComponent>>,
    },
}

impl ActiveService {
    fn stop(self, system: &KompactSystem) {
        match self {
            #[cfg(feature = "zeroconf")]
            Self::Mdns { component } => {
                kill_service_component(system, component, "mDNS announcement component");
            }
            Self::PeerAnnouncement {
                io_runtime,
                component,
            } => {
                let component_shutdown = system.kill_notify(component);
                let io_shutdown = io_runtime.kill_notify(system);
                if let Err(error) = component_shutdown.wait_timeout(SHUTDOWN_TIMEOUT) {
                    log::warn!("Timed out stopping peer announcement component: {error:?}");
                }
                match io_shutdown.wait_timeout(SHUTDOWN_TIMEOUT) {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        log::warn!("Could not stop UDP announcement runtime: {error}");
                    }
                    Err(error) => {
                        log::warn!("Timed out stopping UDP announcement runtime: {error:?}");
                    }
                }
            }
        }
    }
}

fn main() {
    let args = Args::parse();

    let kompact_system = match KompactConfig::default().build().wait() {
        Ok(system) => system,
        Err(error) => {
            eprintln!("Could not start Kompact system: {error}");
            std::process::exit(1);
        }
    };

    let active_service = if args.active {
        let instance_id = Uuid::new_v4();

        #[cfg(feature = "zeroconf")]
        if cfg!(feature = "zeroconf") && args.mdns {
            let mut options =
                MDNS_ANNOUNCEMENT_SERVICE_DEFAULT_OPTIONS.with_instance_id(instance_id);
            options.with_service_provider_name("flotsync_discovery_cli");
            let component =
                kompact_system.create(move || MdnsAnnouncementComponent::with_options(options));
            debug!(
                kompact_system.logger(),
                "Starting mDNS announcement component..."
            );
            kompact_system.start_notify(&component).wait();
            Some(ActiveService::Mdns { component })
        } else {
            Some(
                start_peer_announcement(&kompact_system, instance_id)
                    .unwrap_or_else(|error| shutdown_after_start_error(&kompact_system, &error)),
            )
        }
        #[cfg(not(feature = "zeroconf"))]
        Some(
            start_peer_announcement(&kompact_system, instance_id)
                .unwrap_or_else(|error| shutdown_after_start_error(&kompact_system, &error)),
        )
    } else {
        None
    };

    if let Err(error) = wait_for_enter() {
        log::warn!("Could not read shutdown prompt input: {error}");
    }

    log::info!("Shutting down service...");
    if let Some(active_service) = active_service {
        active_service.stop(&kompact_system);
    }
    if let Err(error) = kompact_system.shutdown().wait() {
        log::warn!("Could not shut down Kompact system: {error}");
    }
}

fn start_peer_announcement(
    system: &KompactSystem,
    instance_id: Uuid,
) -> std::result::Result<ActiveService, String> {
    let io_runtime = IoRuntime::build(system, DriverConfig::default());

    let (startup_promise, startup_future) = peer_announcement_startup_signal();
    let options = PEER_ANNOUNCEMENT_DEFAULT_OPTIONS.with_instance_id(instance_id);
    let placeholder_endpoint = SocketAddr::new(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        options.socket_bind_addr().port(),
    );
    let component = system.create(move || {
        PeerAnnouncementComponent::with_options_and_startup_promise(options, startup_promise)
    });
    debug!(system.logger(), "Starting UDP announcement runtime...");
    io_runtime
        .start_notify(system)
        .wait()
        .map_err(|error| error.to_string())?;
    block_on(io_runtime.bridge_handle().connect_udp(&component))
        .map_err(|error| error.to_string())?;
    system.start_notify(&component).wait();

    match startup_future.wait_timeout(Duration::from_secs(5)) {
        Ok(Ok(())) => {
            let endpoint_selection_port =
                component.on_definition(PeerAnnouncementComponent::endpoint_selection_port);
            system.trigger_i(
                EndpointSelection::from_endpoints([placeholder_endpoint]),
                &endpoint_selection_port,
            );
            Ok(ActiveService::PeerAnnouncement {
                io_runtime,
                component,
            })
        }
        Ok(Err(error)) => Err(error.to_string()),
        Err(error) => Err(format!(
            "Timed out waiting for the UDP peer-announcement runtime to start: {error:?}"
        )),
    }
}

#[cfg(feature = "zeroconf")]
fn kill_service_component<C>(system: &KompactSystem, component: Arc<Component<C>>, name: &str)
where
    C: ComponentDefinition + ComponentLifecycle + Sized + 'static,
{
    if let Err(error) = system.kill_notify(component).wait_timeout(SHUTDOWN_TIMEOUT) {
        log::warn!("Timed out stopping {name}: {error:?}");
    }
}

fn shutdown_after_start_error(system: &KompactSystem, error: &str) -> ! {
    eprintln!("Could not start peer announcement service: {error}");
    if let Err(shutdown_error) = system.clone().shutdown().wait() {
        eprintln!("Could not shut down Kompact system after startup failure: {shutdown_error}");
    }
    std::process::exit(1)
}

fn wait_for_enter() -> io::Result<()> {
    let mut reader = BufReader::new(std::io::stdin());
    let mut line = String::new();

    println!("Press Enter to exit...");

    reader.read_line(&mut line)?;
    Ok(())
}
