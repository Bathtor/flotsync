use flotsync_io::prelude::{DriverConfig, IoBridge, IoBridgeHandle, IoDriverComponent};
use kompact::prelude::{Component, KompactConfig, KompactSystem};
use slog::{Drain, Level, Logger, o};
use snafu::{FromString, Whatever, prelude::*};
use std::sync::{Arc, OnceLock};

/// Result type used by the example binaries.
pub type Result<T, E = Error> = std::result::Result<T, E>;
pub type Error = Whatever;

/// Shared Kompact runtime used by the example binaries.
///
/// The example applications intentionally keep one `IoDriverComponent` and one `IoBridge` alive
/// for the whole process. Transport-specific example components are created on top of that shared
/// runtime and own the actual UDP/TCP interaction logic.
pub struct ExampleRuntime {
    system: KompactSystem,
    _driver_component: Arc<Component<IoDriverComponent>>,
    bridge_component: Arc<Component<IoBridge>>,
    bridge_handle: IoBridgeHandle,
}

impl ExampleRuntime {
    /// Sets up a Kompact system together with the shared `flotsync_io` driver and bridge.
    ///
    /// This only constructs the shared runtime graph. Startup is deferred until the example has
    /// created and wired its transport-specific netcat component.
    pub fn setup() -> Result<Self> {
        init_logging();

        let system = KompactConfig::default().build().map_err(|error| {
            Whatever::without_source(format!("failed to build Kompact system: {error}"))
        })?;

        let driver_component = system.create(|| IoDriverComponent::new(DriverConfig::default()));
        let driver_for_bridge = driver_component.clone();
        let bridge_component = system.create(move || IoBridge::new(&driver_for_bridge));
        let bridge_handle = IoBridgeHandle::from_component(&bridge_component);

        Ok(Self {
            system,
            _driver_component: driver_component,
            bridge_component,
            bridge_handle,
        })
    }

    /// Returns the shared Kompact system.
    pub fn system(&self) -> &KompactSystem {
        &self.system
    }

    /// Returns the shared bridge component.
    pub fn bridge_component(&self) -> &Arc<Component<IoBridge>> {
        &self.bridge_component
    }

    /// Returns the shared driver component.
    pub fn driver_component(&self) -> &Arc<Component<IoDriverComponent>> {
        &self._driver_component
    }

    /// Returns the control handle for the shared bridge.
    pub fn bridge_handle(&self) -> &IoBridgeHandle {
        &self.bridge_handle
    }

    /// Shuts the example runtime down cleanly.
    pub fn shutdown(self) -> Result<()> {
        let Self {
            system,
            _driver_component,
            bridge_component,
            bridge_handle,
        } = self;
        drop(bridge_handle);
        drop(bridge_component);
        drop(_driver_component);

        match system.shutdown() {
            Ok(()) => Ok(()),
            Err(message) => whatever!("failed to shut down Kompact system: {message}"),
        }
    }
}

/// Installs the `log` facade bridge once using a small stderr-only `slog` logger for the app.
///
/// Kompact keeps its own default stdout logger. The example app logs go through the standard `log`
/// facade into a separate synchronous `slog-term` backend on stderr so the two streams do not
/// interfere with each other.
fn init_logging() {
    static LOGGER_GUARD: OnceLock<slog_scope::GlobalLoggerGuard> = OnceLock::new();
    LOGGER_GUARD.get_or_init(|| {
        let decorator = slog_term::PlainSyncDecorator::new(std::io::stderr());
        let drain = slog_term::FullFormat::new(decorator)
            .build()
            .filter_level(Level::Info)
            .fuse();
        let logger = Logger::root(drain, o!());
        let guard = slog_scope::set_global_logger(logger);
        slog_stdlog::init_with_level(log::Level::Info).expect("install app log bridge");
        guard
    });
}
