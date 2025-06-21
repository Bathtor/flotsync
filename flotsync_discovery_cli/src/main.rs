use clap::Parser;
use flotsync_discovery::{errors::Result, services::*, uuid::Uuid};
use log::LevelFilter;
use tokio::io::{self, AsyncBufReadExt, BufReader};

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

    /// Turn debugging information on
    #[arg(short, long, action = clap::ArgAction::Count)]
    debug: u8,
}
impl Args {
    fn logging_level(&self) -> LevelFilter {
        match self.debug {
            0 => LevelFilter::Info,
            1 => LevelFilter::Debug,
            _ => LevelFilter::Trace,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    simple_logger::SimpleLogger::new()
        .with_level(args.logging_level())
        .init()
        .expect("Logger");

    let announcement_service_handle = if args.active {
        let instance_id = Uuid::new_v4();
        log::info!("Starting service for instance '{instance_id}'...");

        #[cfg(feature = "zeroconf")]
        let service = if cfg!(feature = "zeroconf") && args.mdns {
            start_mdns_service(instance_id).await?
        } else {
            start_custom_service(instance_id).await?
        };
        #[cfg(not(feature = "zeroconf"))]
        let service = start_custom_service(instance_id).await?;

        log::info!("done.");
        Some(service)
    } else {
        None
    };

    println!("Press Enter to exit...");

    let mut reader = BufReader::new(io::stdin());
    let mut line = String::new();

    // Wait for a line of input
    reader.read_line(&mut line).await.unwrap();

    log::info!("Shutting down service...");
    if let Some(service) = announcement_service_handle {
        service.shutdown().await.map_err(|e| {
            log::error!("Could not shut down peer announcement service: {e}");
            e
        })?;
        log::info!("done.");
    }
    Ok(())
}

async fn start_custom_service(instance_id: Uuid) -> Result<ServiceHandle> {
    let mut options = PeerAnnouncementService::DEFAULT_OPTIONS;
    options.instance_id = instance_id;
    let service = start_service(PeerAnnouncementService::setup, options)
        .await
        .map_err(|e| {
            log::error!("Could not start peer announcement service: {e}");
            e
        })?;
    Ok(service)
}

#[cfg(feature = "zeroconf")]
async fn start_mdns_service(instance_id: Uuid) -> Result<ServiceHandle> {
    use std::borrow::Cow;

    let mut options = MdnsAnnouncementService::DEFAULT_OPTIONS;
    options.instance_id = instance_id;
    options.service_provider_name = Cow::Borrowed("flotsync_discovery_cli");
    let service = start_service(MdnsAnnouncementService::setup, options)
        .await
        .map_err(|e| {
            log::error!("Could not start mDNS service: {e}");
            e
        })?;
    Ok(service)
}
