use super::*;
use crate::{
    SocketPort,
    zeroconf::{ServiceType, TxtRecord, prelude::TTxtRecord},
};
use std::borrow::Cow;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
pub struct Options {
    pub port: SocketPort,
    pub instance_id: Uuid,
    pub service_provider_name: Cow<'static, str>,
}
impl Options {
    pub const DEFAULT: Self = Self {
        port: SocketPort(52156),
        instance_id: Uuid::nil(),
        service_provider_name: Cow::Borrowed("flotsync_discovery"),
    };

    /// Replace the current instance id with a new random (V4) id.
    pub fn with_new_instance_id(&mut self) {
        self.instance_id = Uuid::new_v4();
    }

    /// Replace the current service provider name with `name`.
    pub fn with_service_provider_name<I>(&mut self, name: I)
    where
        I: Into<Cow<'static, str>>,
    {
        self.service_provider_name = name.into();
    }
}
impl Default for Options {
    fn default() -> Self {
        Self::DEFAULT
    }
}

#[derive(Clone, Debug)]
struct ServiceConfig {
    options: Options,
    service_type: ServiceType,
    txt_record: TxtRecord,
}
impl ServiceConfig {
    const SERVICE_NAME: &str = "flotsync";
    const PROTOCOL: &str = "udp";

    fn try_from_options(options: Options) -> Result<Self> {
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
        })
    }
}

#[cfg(feature = "zeroconf-via-kompact")]
mod kompact_implementation {
    use super::*;
    use crate::{
        kompact::prelude::*,
        kompact_fsm::{State, StateHandled, StateUpdate},
        transform_state_match,
        utils::shutdown::{self, BlockingThreadShutdown},
        zeroconf::{ServiceRegistration, prelude::*},
    };
    use std::time::Duration;

    /// Default options for the mDNS service.
    ///
    /// - port: 52156,
    /// - instance_id: Uuid::nil(),
    /// - service_provider_name: "flotsync_discovery",
    pub const MDNS_ANNOUNCEMENT_SERVICE_DEFAULT_OPTIONS: Options = Options::DEFAULT;

    #[derive(ComponentDefinition)]
    pub struct MdnsAnnouncementComponent {
        ctx: ComponentContext<Self>,
        state: State<ComponentState>,
    }
    #[derive(Debug)]
    enum ComponentState {
        /// We haven't gotten any Options yet, so we can't run anything.
        Uninitialised,
        /// We have Options but we aren't currently running the announcement service.
        Initialised { options: Options },
        /// We have turned the Options into a valid config and are waiting for the service to confirm it's started.
        Starting {
            config: ServiceConfig,
            shutdown_handle: BlockingThreadShutdown<()>,
        },
        /// We are actively running the announcement service.
        Running {
            config: ServiceConfig,
            #[allow(unused)]
            registration: ServiceRegistration,
            shutdown_handle: BlockingThreadShutdown<()>,
        },
    }
    impl MdnsAnnouncementComponent {
        pub fn with_options(options: Options) -> Self {
            Self {
                ctx: ComponentContext::uninitialised(),
                state: State::new(ComponentState::Initialised { options }),
            }
        }

        pub fn new() -> Self {
            Self {
                ctx: ComponentContext::uninitialised(),
                state: State::new(ComponentState::Uninitialised),
            }
        }

        fn start_service(&mut self, options: Options) -> StateUpdate<ComponentState> {
            match ServiceConfig::try_from_options(options.clone()) {
                Ok(config) => {
                    let start_config = config.clone();
                    let (shutdown_handle, shutdown_watcher) = shutdown::watcher();
                    let actor_ref = self.actor_ref();

                    let join_handle = std::thread::spawn(move || {
                        let mut service = build_mdns_service(
                            start_config.service_type,
                            *start_config.options.port,
                            start_config.options.service_provider_name.as_ref(),
                            start_config.txt_record,
                        );

                        let actor_ref_for_callback = actor_ref.clone();
                        service.set_registered_callback(Box::new(move |result, _context| {
                            match result {
                                Ok(registration) => {
                                    actor_ref_for_callback
                                        .tell(MdnsAnnouncementMessages::registered(registration));
                                }
                                Err(e) => {
                                    actor_ref_for_callback
                                        .tell(MdnsAnnouncementMessages::registration_failed(e));
                                }
                            }
                        }));
                        match service.register() {
                            Ok(event_loop) => {
                                while !shutdown_watcher.should_shutdown() {
                                    // A compromise between super hot polling and shutdown speed.
                                    event_loop
                                        .poll(Duration::from_secs(1))
                                        .expect("should have been able to poll event loop");
                                    std::thread::yield_now();
                                }
                            }
                            Err(e) => {
                                actor_ref.tell(MdnsAnnouncementMessages::registration_failed(e));
                            }
                        }
                    });
                    let shutdown_handle = shutdown_handle.with_thread(join_handle);
                    StateUpdate::transition(ComponentState::Starting {
                        config,
                        shutdown_handle,
                    })
                }
                Err(e) => {
                    warn!(
                        self.log(),
                        "Could not derive service configuration from options={options:?}: {e}"
                    );
                    // Make this an uninitialised, since the options clearly aren't valid.
                    StateUpdate::transition(ComponentState::Uninitialised)
                }
            }
        }

        fn stop_service(
            &mut self,
            config: ServiceConfig,
            shutdown_handle: BlockingThreadShutdown<()>,
        ) -> StateUpdate<ComponentState> {
            Handled::block_on(self, async move |async_self| {
                if let Err(e) = shutdown_handle.shutdown().await {
                    error!(
                        async_self.log(),
                        "Could not shutdown mDNS announcement service: {e}"
                    );
                }
                Handled::Ok
            })
            .and_transition(ComponentState::Initialised {
                options: config.options,
            })
        }
    }
    impl Default for MdnsAnnouncementComponent {
        fn default() -> Self {
            Self::new()
        }
    }
    impl ComponentLifecycle for MdnsAnnouncementComponent {
        fn on_start(&mut self) -> Handled {
            transform_state_match!(self, state, {
                // There's nothing to do. We don't know the port, so we can't start the service.
                old_state@ComponentState::Uninitialised => StateUpdate::ok(old_state),
                ComponentState::Initialised { options } => self.start_service(options),
                ComponentState::Starting { .. } => {
                    StateUpdate::invalid("Illegal state, component was in Starting state when being started!")
                }
                ComponentState::Running { .. }  => {
                    StateUpdate::invalid("Illegal state, component was in Running/Starting state when being started!")
                }
            })
        }
        fn on_stop(&mut self) -> Handled {
            transform_state_match!(self, state, {
                old_state @ ComponentState::Uninitialised | old_state@ComponentState::Initialised { .. } => StateUpdate::ok(old_state),
                ComponentState::Running {
                    config,
                    shutdown_handle, ..
                } | ComponentState::Starting { config, shutdown_handle } => {
                    self.stop_service(config, shutdown_handle)
                }
            })
        }
        fn on_kill(&mut self) -> Handled {
            transform_state_match!(self, state, {
                 ComponentState::Running {
                    config,
                    shutdown_handle, ..
                } | ComponentState::Starting { config, shutdown_handle } => {
                    let res = self.stop_service(config, shutdown_handle);
                    // Transition to Uninitialised instead of Initialised,
                    // since we don't need to hang on to this memory if we are being killed anyway.
                    res.replace_new_state(ComponentState::Uninitialised)
                }
                _ => StateUpdate::transition(ComponentState::Uninitialised)
            })
        }
    }
    impl Actor for MdnsAnnouncementComponent {
        type Message = MdnsAnnouncementMessages;

        fn receive_local(&mut self, msg: Self::Message) -> Handled {
            match msg {
                MdnsAnnouncementMessages::Public(_) => unimplemented!("No public messages yet"),
                MdnsAnnouncementMessages::Internal(internal) => match internal {
                    InternalMdnsAnnouncementMessage::ServiceRegistered(registration) => {
                        transform_state_match!(self, state, {
                             ComponentState::Starting { config, shutdown_handle } => {
                                info!(self.log(), "mDNS service is running: {registration:?}");
                                StateUpdate::transition(ComponentState::Running { config, registration, shutdown_handle})
                            }
                            s => StateUpdate::invalid(format!("Got service registration in state other than Starting: {s:?}"))
                        })
                    }
                    InternalMdnsAnnouncementMessage::RegistrationFailed(error) => {
                        transform_state_match!(self, state, {
                             ComponentState::Starting { config, shutdown_handle } => {
                                error!(self.log(), "An error occurred during mDNS service registration: {error}");
                                shutdown_handle.shutdown_and_forget();
                                StateUpdate::transition(ComponentState::Initialised { options: config.options })
                            }
                            s => StateUpdate::invalid(format!("Got service registration error in state other than Starting: {s:?}"))
                        })
                    }
                },
            }
        }

        fn receive_network(&mut self, _msg: NetMessage) -> Handled {
            unimplemented!("Not using networking.");
        }
    }
    #[derive(Debug)]
    pub enum MdnsAnnouncementMessages {
        Public(MdnsAnnouncementMessage),
        #[allow(private_interfaces)]
        Internal(InternalMdnsAnnouncementMessage),
    }
    impl MdnsAnnouncementMessages {
        fn registered(registration: ServiceRegistration) -> Self {
            MdnsAnnouncementMessages::Internal(InternalMdnsAnnouncementMessage::ServiceRegistered(
                registration,
            ))
        }

        fn registration_failed(error: crate::zeroconf::error::Error) -> Self {
            MdnsAnnouncementMessages::Internal(InternalMdnsAnnouncementMessage::RegistrationFailed(
                error,
            ))
        }
    }
    #[derive(Debug)]
    pub enum MdnsAnnouncementMessage {
        // Nothing for now.
    }
    #[derive(Debug)]
    enum InternalMdnsAnnouncementMessage {
        ServiceRegistered(ServiceRegistration),
        RegistrationFailed(crate::zeroconf::error::Error),
    }
}
#[cfg(feature = "zeroconf-via-kompact")]
pub use kompact_implementation::{
    MDNS_ANNOUNCEMENT_SERVICE_DEFAULT_OPTIONS,
    MdnsAnnouncementComponent,
    MdnsAnnouncementMessage,
    MdnsAnnouncementMessages,
};

#[cfg(feature = "zeroconf-via-tokio")]
mod tokio_implementation {
    use super::*;
    use crate::{
        utils::shutdown::{self, ShutdownHandle, ShutdownWatch},
        zeroconf::service::MdnsServiceAsync,
    };
    use tokio::{runtime::Builder, task::LocalSet};

    pub struct MdnsAnnouncementService {
        config: ServiceConfig,
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

        pub async fn setup(options: Options) -> Result<Self> {
            let config = ServiceConfig::try_from_options(options)?;

            Ok(Self {
                config,
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
                    let service_type = self.config.service_type.clone();
                    let port = *self.config.options.port;
                    let service_provider_name = self.config.options.service_provider_name.clone();
                    let txt_record = self.config.txt_record.clone();

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
                ServiceState::Stopped => {
                    unreachable!("Should not invoke run anymore after stopping.")
                }
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
        let service = build_mdns_service(service_type, port, service_provider_name, txt_record);
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
}
#[cfg(feature = "zeroconf-via-tokio")]
pub use tokio_implementation::MdnsAnnouncementService;

const FALLBACK_HOST_NAME: &str = "unknown";

#[cfg(any(feature = "zeroconf", feature = "zeroconf-tokio"))]
fn build_mdns_service(
    service_type: ServiceType,
    port: u16,
    service_provider_name: &str,
    txt_record: TxtRecord,
) -> crate::zeroconf::MdnsService {
    use crate::zeroconf::prelude::*;

    let mut service = crate::zeroconf::MdnsService::new(service_type, port);
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
    service
}
