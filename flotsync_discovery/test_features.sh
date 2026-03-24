#!/bin/bash

set -euo pipefail

echo "##### Testing Default Features"
cargo build
cargo clippy --all-targets --no-deps -- -D warnings

echo "##### Testing Feature peer-announcement-via-kompact"
cargo build --no-default-features --features peer-announcement-via-kompact
cargo clippy --all-targets --no-deps --no-default-features --features peer-announcement-via-kompact -- -D warnings

echo "##### Testing Feature zeroconf-via-kompact"
cargo build --no-default-features --features zeroconf-via-kompact
cargo clippy --all-targets --no-deps --no-default-features --features zeroconf-via-kompact -- -D warnings
