use clap::Parser;
use flotsync_discovery::{errors::Result, services::*};
use log::LevelFilter;
use tokio::io::{self, AsyncBufReadExt, BufReader};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Start the announcement service.
    #[arg(short, long)]
    active: bool,

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
        log::info!("Starting service...");
        let service = start_service(
            PeerAnnouncementService::setup,
            PeerAnnouncementService::DEFAULT_OPTIONS,
        )
        .await
        .map_err(|e| {
            log::error!("Could not start peer announcement service: {e}");
            e
        })?;
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
