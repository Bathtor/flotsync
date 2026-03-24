use clap::Parser;
use flotsync_discovery::{kompact::prelude::*, services::*, uuid::Uuid};
use flotsync_io::prelude::{DriverConfig, IoBridge, IoDriverComponent, UdpPort};
use std::{
    io::{BufRead, BufReader},
    sync::Arc,
    time::Duration,
};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Start the announcement service.
    #[arg(short, long)]
    active: bool,

    #[cfg(feature = "zeroconf")]
    /// Use zeroconf mDNS instead of a custom UDP broadcast.
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
        _component: Arc<Component<MdnsAnnouncementComponent>>,
    },
    PeerAnnouncement {
        _driver: Arc<Component<IoDriverComponent>>,
        _bridge: Arc<Component<IoBridge>>,
        _component: Arc<Component<PeerAnnouncementComponent>>,
        _connection: Box<dyn Channel + Send + 'static>,
    },
}

fn main() {
    let args = Args::parse();

    let kompact_system = KompactConfig::default().build().expect("system");

    let _active_service = if args.active {
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
            Some(ActiveService::Mdns {
                _component: component,
            })
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

    wait_for_enter();

    log::info!("Shutting down service...");
    kompact_system.shutdown().expect("Kompact System shutdown");
}

fn start_peer_announcement(
    system: &KompactSystem,
    instance_id: Uuid,
) -> std::result::Result<ActiveService, String> {
    let driver = system.create(|| IoDriverComponent::new(DriverConfig::default()));
    let driver_for_bridge = driver.clone();
    let bridge = system.create(move || IoBridge::new(&driver_for_bridge));

    let (startup_promise, startup_future) = peer_announcement_startup_signal();
    let options = PEER_ANNOUNCEMENT_DEFAULT_OPTIONS.with_instance_id(instance_id);
    let component = system.create(move || {
        PeerAnnouncementComponent::with_options_and_startup_promise(options, startup_promise)
    });
    let connection = biconnect_components::<UdpPort, _, _>(&bridge, &component)
        .expect("connect peer announcement component to UDP bridge")
        .boxed();

    debug!(system.logger(), "Starting UDP announcement runtime...");
    system.start_notify(&driver).wait();
    system.start_notify(&bridge).wait();
    system.start_notify(&component).wait();

    match startup_future.wait_timeout(Duration::from_secs(5)) {
        Ok(Ok(())) => Ok(ActiveService::PeerAnnouncement {
            _driver: driver,
            _bridge: bridge,
            _component: component,
            _connection: connection,
        }),
        Ok(Err(error)) => Err(error.to_string()),
        Err(error) => Err(format!(
            "Timed out waiting for the UDP peer-announcement runtime to start: {error:?}"
        )),
    }
}

fn shutdown_after_start_error(system: &KompactSystem, error: &str) -> ! {
    eprintln!("Could not start peer announcement service: {error}");
    if let Err(shutdown_error) = system.clone().shutdown() {
        eprintln!("Could not shut down Kompact system after startup failure: {shutdown_error}");
    }
    std::process::exit(1)
}

fn wait_for_enter() {
    let mut reader = BufReader::new(std::io::stdin());
    let mut line = String::new();

    println!("Press Enter to exit...");

    // Wait for a line of input
    reader.read_line(&mut line).unwrap();
}
