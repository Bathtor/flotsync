//! Internal logging helpers for runtime code.
//!
//! The raw driver and pool implementation should not depend on the global `log` facade. They log
//! through an erased `slog::Logger` instead, so Kompact integration can pass the system logger
//! directly while standalone raw-driver users still get a simple fallback logger.

use ::kompact::KompactLogger;
use slog::{Drain, Level, Logger};

/// Erased slog logger used by the raw runtime implementation.
pub(crate) type RuntimeLogger = Logger;

/// Builds the simple synchronous fallback logger used outside Kompact integration.
pub(crate) fn default_runtime_logger() -> RuntimeLogger {
    let decorator = slog_term::PlainSyncDecorator::new(std::io::stderr());
    let drain = slog_term::FullFormat::new(decorator)
        .build()
        .filter_level(Level::Warning)
        .fuse();
    Logger::root(drain, slog::o!())
}

/// Converts Kompact's typed logger into the erased runtime logger used by the raw driver.
pub(crate) fn erased_runtime_logger(logger: &KompactLogger) -> RuntimeLogger {
    logger.to_erased()
}
