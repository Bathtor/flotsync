#!/bin/bash

set -euo pipefail

echo "##### Testing Default Features"
cargo build
cargo clippy --all-targets -- -D warnings

echo "##### Testing Feature tokio-sync-only"
cargo build --features tokio-sync-only
cargo clippy --all-targets --features tokio-sync-only -- -D warnings

echo "##### Testing Feature full-tokio"
cargo build --features full-tokio
cargo clippy --all-targets --features full-tokio -- -D warnings

echo "##### Testing Feature zeroconf-via-tokio"
cargo build --features zeroconf-via-tokio
cargo clippy --all-targets --features zeroconf-via-tokio -- -D warnings

echo "##### Testing Feature zeroconf-via-kompact"
cargo build --features zeroconf-via-kompact
cargo clippy --all-targets --features zeroconf-via-kompact -- -D warnings
