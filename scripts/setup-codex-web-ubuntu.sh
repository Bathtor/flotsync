#!/usr/bin/env bash

set -euo pipefail

readonly BUF_VERSION="1.67.0"
readonly RUST_TOOLCHAIN="nightly-2026-04-11"

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"

log() {
    printf '==> %s\n' "$*"
}

warn() {
    printf 'warning: %s\n' "$*" >&2
}

die() {
    printf 'error: %s\n' "$*" >&2
    exit 1
}

require_command() {
    local cmd="$1"
    if ! command -v "${cmd}" >/dev/null 2>&1; then
        die "required command not found: ${cmd}"
    fi
}

ensure_repo_root() {
    if [[ ! -f "${repo_root}/Cargo.toml" ]]; then
        die "expected workspace Cargo.toml at ${repo_root}"
    fi
}

setup_apt() {
    local -a apt_prefix=()
    local -a packages=(
        build-essential
        ca-certificates
        clang
        curl
        git
        libavahi-compat-libdnssd-dev
        pkg-config
    )

    require_command apt-get

    if [[ "$(id -u)" -eq 0 ]]; then
        apt_prefix=()
    elif command -v sudo >/dev/null 2>&1; then
        apt_prefix=(sudo)
    else
        die "this script needs root or sudo in order to install Ubuntu packages"
    fi

    log "Installing Ubuntu packages required by Linux CI"
    "${apt_prefix[@]}" apt-get update
    "${apt_prefix[@]}" apt-get install -y "${packages[@]}"
}

ensure_rustup() {
    local installer
    local tmp_dir

    if command -v rustup >/dev/null 2>&1; then
        return
    fi

    require_command curl

    log "Installing rustup"
    tmp_dir="$(mktemp -d)"
    installer="${tmp_dir}/rustup-init.sh"
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o "${installer}"
    sh "${installer}" -y --profile minimal
    rm -rf "${tmp_dir}"
}

load_rustup_env() {
    local cargo_env="${HOME}/.cargo/env"

    if [[ -f "${cargo_env}" ]]; then
        # shellcheck disable=SC1090
        source "${cargo_env}"
    else
        export PATH="${HOME}/.cargo/bin:${PATH}"
    fi
}

setup_rust_toolchain() {
    require_command rustup

    log "Installing Rust toolchain ${RUST_TOOLCHAIN} with clippy and rustfmt"
    rustup toolchain install "${RUST_TOOLCHAIN}" \
        --profile minimal \
        --component clippy \
        --component rustfmt

    log "Setting a repo-local Rust override"
    (
        cd "${repo_root}"
        rustup override set "${RUST_TOOLCHAIN}"
    )
}

setup_buf() {
    local -a install_prefix=()
    local arch
    local archive
    local buf_binary
    local tmp_dir
    local url
    local version_output

    version_output=""
    if command -v buf >/dev/null 2>&1; then
        version_output="$(buf --version | head -n 1)"
    fi
    if [[ "${version_output}" == "${BUF_VERSION}" ]]; then
        return
    fi

    case "$(uname -m)" in
        x86_64)
            arch="x86_64"
            ;;
        aarch64 | arm64)
            arch="aarch64"
            ;;
        *)
            die "unsupported Linux architecture for buf installation: $(uname -m)"
            ;;
    esac

    if [[ "$(id -u)" -eq 0 ]]; then
        install_prefix=()
    elif command -v sudo >/dev/null 2>&1; then
        install_prefix=(sudo)
    else
        die "this script needs root or sudo in order to install buf into /usr/local/bin"
    fi

    log "Installing buf ${BUF_VERSION}"
    tmp_dir="$(mktemp -d)"
    archive="${tmp_dir}/buf.tar.gz"
    url="https://github.com/bufbuild/buf/releases/download/v${BUF_VERSION}/buf-Linux-${arch}.tar.gz"
    curl --proto '=https' --tlsv1.2 -sSfL "${url}" -o "${archive}"
    tar -xzf "${archive}" -C "${tmp_dir}"
    buf_binary="${tmp_dir}/bin/buf"
    if [[ ! -x "${buf_binary}" ]]; then
        buf_binary="$(find "${tmp_dir}" -type f -path '*/bin/buf' | head -n 1)"
    fi
    if [[ -z "${buf_binary}" || ! -x "${buf_binary}" ]]; then
        die "failed to find buf in downloaded archive"
    fi
    "${install_prefix[@]}" install -m 0755 "${buf_binary}" /usr/local/bin/buf
    rm -rf "${tmp_dir}"
}

print_summary() {
    log "Environment bootstrap complete"
    printf '\n'
    printf 'Rust toolchain: %s\n' "$(rustc --version)"
    printf 'Cargo: %s\n' "$(cargo --version)"
    printf 'Buf: %s\n' "$(buf --version)"
    printf '\n'
    printf 'Suggested next steps:\n'
    printf '  cd %s\n' "${repo_root}"
    printf '  export CARGO_NET_GIT_FETCH_WITH_CLI=true\n'
    printf '  buf format --diff --exit-code\n'
    printf '  buf lint\n'
    printf '  cargo fmt --all --check\n'
    printf '  cargo clippy --workspace --all-targets --no-deps --locked -- -D warnings\n'
    printf '  cargo test --workspace --locked\n'
    printf '  cd flotsync_discovery && ./test_features.sh\n'
}

main() {
    ensure_repo_root

    if [[ "$(uname -s)" != "Linux" ]]; then
        die "this setup script is intended for Linux hosts"
    fi

    if [[ -r /etc/os-release ]]; then
        # shellcheck disable=SC1091
        source /etc/os-release
        if [[ "${ID:-}" != "ubuntu" ]]; then
            warn "CI uses Ubuntu; continuing on ${PRETTY_NAME:-unknown Linux} because apt-get is available"
        fi
    fi

    setup_apt
    ensure_rustup
    load_rustup_env
    setup_rust_toolchain
    setup_buf
    print_summary
}

main "$@"
