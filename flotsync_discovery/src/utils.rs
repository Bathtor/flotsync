use base64::engine::{GeneralPurpose, general_purpose::URL_SAFE_NO_PAD};
use std::fmt;

/// Shorthand for [[base64::display::Base64Display]] with fixed engine.
pub struct Base64Display<'a> {
    inner: base64::display::Base64Display<'a, 'static, GeneralPurpose>,
}
impl<'a> Base64Display<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            inner: base64::display::Base64Display::new(bytes, &URL_SAFE_NO_PAD),
        }
    }
}
impl fmt::Display for Base64Display<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.inner)
    }
}

/// A simple wrapper for handling async shutdowns.
pub mod shutdown {
    use snafu::prelude::*;
    use tokio::sync::watch;

    #[derive(Debug, Snafu)]
    #[snafu(visibility(pub(crate)))]
    pub enum ShutdownError {
        #[snafu(display("The shutdown handle was already dropped."))]
        HandleDropped,
        #[snafu(display("The shutdown watcher was already dropped."))]
        WatcherDropped,
    }

    pub fn watcher() -> (ShutdownHandle, ShutdownWatch) {
        let (tx, rx) = watch::channel(false);
        (
            ShutdownHandle { sender: tx },
            ShutdownWatch { receiver: rx },
        )
    }

    #[derive(Debug)]
    pub struct ShutdownHandle {
        sender: watch::Sender<bool>,
    }

    impl ShutdownHandle {
        pub fn shutdown(self) -> Result<(), ShutdownError> {
            self.sender.send(true).map_err(|e| {
                log::debug!("Error during shutdown send: {e}");
                WatcherDroppedSnafu.build()
            })
        }
    }

    #[derive(Debug)]
    pub struct ShutdownWatch {
        receiver: watch::Receiver<bool>,
    }

    impl ShutdownWatch {
        pub async fn wait(&mut self) -> Result<(), ShutdownError> {
            self.receiver
                .wait_for(|b| *b)
                .await
                .map(|_| ())
                .map_err(|e| {
                    log::debug!("Error during shutdown wait: {e}");
                    HandleDroppedSnafu.build()
                })
        }
    }
}
