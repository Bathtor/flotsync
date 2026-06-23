//! Shared Kompact I/O runtime helpers.

use super::{IoBridge, IoBridgeHandle, IoDriverComponent};
use crate::driver::DriverConfig;
use ::kompact::prelude::*;
use snafu::{ResultExt, Snafu};
use std::{any::type_name, future::Future, sync::Arc};

/// Error returned by [`IoRuntime`] lifecycle futures.
#[derive(Debug, Snafu)]
#[snafu(module(io_runtime_error), visibility(pub(crate)))]
#[non_exhaustive]
pub enum IoRuntimeError {
    #[snafu(display("failed to start {component} component: {source}"))]
    StartComponent {
        component: &'static str,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display("failed to stop {component} component: {source}"))]
    StopComponent {
        component: &'static str,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display("failed to kill {component} component: {source}"))]
    KillComponent {
        component: &'static str,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

/// Shared Kompact I/O runtime for `flotsync_io` consumers.
///
/// The runtime owns one shared [`IoDriverComponent`] and one [`IoBridge`]. `kill_notify` consumes
/// the runtime so that component handles are released while Kompact is waiting for deallocation.
pub struct IoRuntime {
    /// Shared raw I/O driver component owned for the full runtime lifetime.
    driver: Arc<Component<IoDriverComponent>>,
    /// Shared Kompact bridge component exposing transport ports on top of the driver.
    bridge: Arc<Component<IoBridge>>,
    /// Control handle retained while the bridge is live and dropped before bridge deallocation.
    bridge_handle: IoBridgeHandle,
}

impl IoRuntime {
    /// Create the shared driver/bridge component pair.
    #[must_use]
    pub fn build(system: &KompactSystem, driver_config: DriverConfig) -> Self {
        let driver = system.create(move || IoDriverComponent::new(driver_config));
        let bridge_definition = IoBridge::new(&driver);
        let bridge = system.create(move || bridge_definition);
        let bridge_handle = IoBridgeHandle::from_component(&bridge);
        Self {
            driver,
            bridge,
            bridge_handle,
        }
    }

    /// Return the shared bridge component.
    #[must_use]
    pub fn bridge_component(&self) -> &Arc<Component<IoBridge>> {
        &self.bridge
    }

    /// Return the shared driver component.
    #[must_use]
    pub fn driver_component(&self) -> &Arc<Component<IoDriverComponent>> {
        &self.driver
    }

    /// Return the shared bridge control handle.
    #[must_use]
    pub fn bridge_handle(&self) -> &IoBridgeHandle {
        &self.bridge_handle
    }

    /// Start the shared driver/bridge pair.
    ///
    /// The driver is started before the bridge because bridge startup depends on the driver actor
    /// infrastructure being alive.
    pub fn start_notify<'runtime, 'system: 'runtime>(
        &'runtime self,
        system: &'system KompactSystem,
    ) -> impl Future<Output = Result<(), IoRuntimeError>> + Send + Unpin + 'runtime {
        let driver_start = system.start_notify(&self.driver);
        let bridge = &self.bridge;
        Box::pin(async move {
            driver_start
                .await
                .boxed()
                .context(io_runtime_error::StartComponentSnafu {
                    component: type_name::<IoDriverComponent>(),
                })?;

            system.start_notify(bridge).await.boxed().context(
                io_runtime_error::StartComponentSnafu {
                    component: type_name::<IoBridge>(),
                },
            )
        })
    }

    /// Stop the shared bridge/driver pair.
    ///
    /// The bridge is stopped before the driver so it can release driver-owned transport state.
    pub fn stop_notify<'runtime, 'system: 'runtime>(
        &'runtime self,
        system: &'system KompactSystem,
    ) -> impl Future<Output = Result<(), IoRuntimeError>> + Send + Unpin + 'runtime {
        let bridge_stop = system.stop_notify(&self.bridge);
        let driver = &self.driver;
        Box::pin(async move {
            bridge_stop
                .await
                .boxed()
                .context(io_runtime_error::StopComponentSnafu {
                    component: type_name::<IoBridge>(),
                })?;

            system
                .stop_notify(driver)
                .await
                .boxed()
                .context(io_runtime_error::StopComponentSnafu {
                    component: type_name::<IoDriverComponent>(),
                })
        })
    }

    /// Kill the shared bridge/driver pair and release runtime-owned component handles.
    ///
    /// The bridge is killed before the driver so it can release driver-owned transport state. The
    /// method consumes `self` to avoid retaining extra component references while Kompact is
    /// waiting for component deallocation.
    pub fn kill_notify(
        self,
        system: &KompactSystem,
    ) -> impl Future<Output = Result<(), IoRuntimeError>> + Send + Unpin + '_ {
        let Self {
            driver,
            bridge,
            bridge_handle,
        } = self;
        drop(bridge_handle);

        let bridge_kill = system.kill_notify(bridge);
        Box::pin(async move {
            bridge_kill
                .await
                .boxed()
                .context(io_runtime_error::KillComponentSnafu {
                    component: type_name::<IoBridge>(),
                })?;

            system
                .kill_notify(driver)
                .await
                .boxed()
                .context(io_runtime_error::KillComponentSnafu {
                    component: type_name::<IoDriverComponent>(),
                })
        })
    }
}
