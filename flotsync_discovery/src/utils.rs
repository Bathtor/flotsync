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
#[cfg(feature = "zeroconf-via-kompact")]
pub mod shutdown {
    use snafu::prelude::*;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    #[derive(Debug, Snafu)]
    #[snafu(visibility(pub(crate)))]
    pub enum ShutdownError {
        #[snafu(display("The target of the shutdown panicked"))]
        Panic,
    }

    pub fn watcher() -> (ShutdownHandle, ShutdownWatch) {
        let flag = Arc::new(AtomicBool::new(false));
        (
            ShutdownHandle {
                flag: Arc::clone(&flag),
            },
            ShutdownWatch { flag },
        )
    }

    #[derive(Debug)]
    pub struct ShutdownHandle {
        flag: Arc<AtomicBool>,
    }

    impl ShutdownHandle {
        pub fn with_thread<T>(
            self,
            join_handle: std::thread::JoinHandle<T>,
        ) -> BlockingThreadShutdown<T> {
            BlockingThreadShutdown {
                shutdown_handle: self,
                join_handle,
            }
        }

        pub fn shutdown(self) {
            self.flag.store(true, Ordering::Release);
        }
    }

    #[derive(Debug)]
    pub struct ShutdownWatch {
        flag: Arc<AtomicBool>,
    }

    impl ShutdownWatch {
        pub fn should_shutdown(&self) -> bool {
            self.flag.load(Ordering::Acquire)
        }
    }

    #[derive(Debug)]
    pub struct BlockingThreadShutdown<T> {
        shutdown_handle: ShutdownHandle,
        join_handle: std::thread::JoinHandle<T>,
    }

    impl<T> BlockingThreadShutdown<T>
    where
        T: Send + 'static,
    {
        /// Tell the target to shutdown, but ignore any feedback.
        ///
        /// Useful when the target is likely errored out already and we are mostly making sure that the memory gets cleaned up.
        pub fn shutdown_and_forget(self) {
            self.shutdown_handle.shutdown();
        }

        /// Shutdown and wait for the thread to complete.
        pub async fn shutdown(self) -> Result<T, ShutdownError> {
            self.shutdown_handle.shutdown();
            blocking::unblock(|| self.join_handle.join().map_err(|_| PanicSnafu.build())).await
        }
    }
}
