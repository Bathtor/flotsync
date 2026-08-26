//! Shared helpers for Flotsync build scripts.

/// Registers and emits the compiler-channel cfg used to select nightly APIs.
pub fn emit_rust_channel_cfg() {
    println!("cargo::rustc-check-cfg=cfg(flotsync_nightly)");
    if rustversion::cfg!(nightly) {
        println!("cargo::rustc-cfg=flotsync_nightly");
    }
}
