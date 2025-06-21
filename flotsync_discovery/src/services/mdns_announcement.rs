use crate::utils::shutdown::{self, ShutdownHandle, ShutdownWatch};
use std::borrow::Cow;
use tokio::{runtime::Builder, task::LocalSet};
use uuid::Uuid;
use zeroconf_tokio::{MdnsService, MdnsServiceAsync, ServiceType, TxtRecord, prelude::*};

use super::*;

#[derive(Clone, Debug, PartialEq)]
pub struct Options {
    pub port: Port,
    pub instance_id: Uuid,
    pub service_provider_name: Cow<'static, str>,
}
impl Options {
    pub const DEFAULT: Self = Self {
        port: Port(52156),
        instance_id: Uuid::nil(),
        service_provider_name: Cow::Borrowed("flotsync_discovery"),
    };

    /// Replace the current instance id with a new random (V4) id.
    pub fn with_new_instance_id(&mut self) {
        self.instance_id = Uuid::new_v4();
    }
}
impl Default for Options {
    fn default() -> Self {
        Self::DEFAULT
    }
}

pub struct MdnsAnnouncementService {
    options: Options,
    service_type: ServiceType,
    txt_record: TxtRecord,
    state: ServiceState,
}
enum ServiceState {
    Setup,
    Running {
        // This is almost the same as the ServiceHandle again,
        // but MdnsServiceAsync doesn't let me move it between threads,
        // so I cannot hold it's state directly here -.-
        shutdown_handle: ShutdownHandle,
        join: std::thread::JoinHandle<Result<()>>,
    },
    Stopped,
}

impl MdnsAnnouncementService {
    /// The default options for this service:
    ///
    /// - port: 52156,
    /// - instance_id: Uuid::nil(),
    /// - service_provider_name: "flotsync_discovery",
    pub const DEFAULT_OPTIONS: Options = Options::DEFAULT;

    const SERVICE_NAME: &str = "flotsync";
    const PROTOCOL: &str = "udp";

    pub async fn setup(options: Options) -> Result<Self> {
        let service_type =
            ServiceType::new(Self::SERVICE_NAME, Self::PROTOCOL).context(ZeroconfSnafu)?;

        let mut txt_record = TxtRecord::new();

        txt_record
            .insert("id", &options.instance_id.as_hyphenated().to_string())
            .context(ZeroconfSnafu)?;

        Ok(Self {
            options,
            service_type,
            txt_record,
            state: ServiceState::Setup,
        })
    }
}

#[async_trait]
impl Service for MdnsAnnouncementService {
    type Options = Options;

    async fn run(&mut self) -> Result<()> {
        match self.state {
            ServiceState::Setup => {
                let service_type = self.service_type.clone();
                let port = *self.options.port;
                let service_provider_name = self.options.service_provider_name.clone();
                let txt_record = self.txt_record.clone();

                let (handle, watch) = shutdown::watcher();
                let rt = Builder::new_current_thread().enable_all().build().unwrap();
                let join = std::thread::spawn(move || {
                    let local = LocalSet::new();

                    let join = local.spawn_local(async move {
                        run_mdns_service(
                            service_type,
                            port,
                            service_provider_name.as_ref(),
                            txt_record,
                            watch,
                        )
                        .await
                    });

                    rt.block_on(
                        local.run_until(async move { join.await.context(JoinSnafu).flatten() }),
                    )
                });
                self.state = ServiceState::Running {
                    shutdown_handle: handle,
                    join,
                };
                Ok(())
            }
            ServiceState::Running { .. } => std::future::pending().await,
            ServiceState::Stopped => unreachable!("Should not invoke run anymore after stopping."),
        }
    }

    async fn shutdown(mut self) -> Result<()> {
        match self.state {
            ServiceState::Setup => {
                log::debug!("Shutting down service before starting.");
                self.state = ServiceState::Stopped;
                Ok(())
            }
            ServiceState::Running {
                shutdown_handle,
                join,
            } => {
                if let Err(e) = shutdown_handle.shutdown() {
                    log::warn!("Error sending mDNS service shutdown instruction: {e}");
                }
                tokio::task::spawn_blocking(|| {
                    join.join().map_err(|_| ThreadJoinSnafu.build()).flatten()
                })
                .await
                .context(JoinSnafu)
                .flatten()?;
                self.state = ServiceState::Stopped;
                Ok(())
            }
            ServiceState::Stopped => {
                log::warn!("Executed shutdown twice.");
                Ok(())
            }
        }
    }
}

async fn run_mdns_service(
    service_type: ServiceType,
    port: u16,
    service_provider_name: &str,
    txt_record: TxtRecord,
    mut shutdown: ShutdownWatch,
) -> Result<()> {
    const FALLBACK_HOST_NAME: &str = "unknown";

    let mut service = MdnsService::new(service_type, port);
    let host_name = hostname::get()
        .map(|s| {
            s.into_string()
                .map(|mut s| {
                    if s.ends_with(".local") {
                        let _ = s.split_off(s.len() - 6);
                    }
                    s
                })
                .unwrap_or_else(|s| {
                    log::warn!("Could not turn hostname '{s:?}' into Rust String");
                    FALLBACK_HOST_NAME.to_string()
                })
        })
        .unwrap_or_else(|e| {
            log::warn!("Could not get hostname: {e}");
            FALLBACK_HOST_NAME.to_string()
        });
    let service_name = format!("{service_provider_name}@{host_name}:{port:04X}");
    service.set_name(&service_name);
    service.set_txt_record(txt_record);
    let mut service = MdnsServiceAsync::new(service).context(ZeroconfSnafu)?;

    let registration = service.start().await.context(ZeroconfSnafu)?;
    log::info!("Registered mDNS service: {registration:?}");
    drop(registration);

    if let Err(e) = shutdown.wait().await {
        log::warn!("Error waiting for mDNS service shutdown instruction: {e}");
    }

    log::debug!("Shutting down mDNS service...");
    service.shutdown().await.context(ZeroconfSnafu)?;
    Ok(())
}
