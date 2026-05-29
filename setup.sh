#!/usr/bin/env bash
#
# pi-server installer
#
# One-liner:
#   curl -fsSL "https://raw.githubusercontent.com/mikesoylu/pi-server/main/setup.sh?$(date +%s)" | bash

set -euo pipefail

OWNER="${OWNER:-mikesoylu}"
REPO="${REPO:-pi-server}"
VERSION="${VERSION:-}"
DEST="${DEST:-$HOME/.local/bin}"
BIN_NAME="pi-server"
VERIFY=1
QUIET=0

usage() {
  cat <<'USAGE'
Usage: setup.sh [options]

Options:
  --version TAG       Install a specific GitHub release tag
  --dest DIR          Install directory (default: ~/.local/bin)
  --system            Install to /usr/local/bin
  --no-verify         Skip SHA256 verification
  --quiet, -q         Suppress non-error output
  -h, --help          Show this help

Environment:
  OWNER               GitHub owner (default: mikesoylu)
  REPO                GitHub repo (default: pi-server)
  VERSION             Release tag to install
  DEST                Install directory
USAGE
}

info() {
  [ "$QUIET" -eq 1 ] && return 0
  printf -- '-> %s\n' "$*" >&2
}

ok() {
  [ "$QUIET" -eq 1 ] && return 0
  printf 'OK %s\n' "$*" >&2
}

err() {
  printf 'error: %s\n' "$*" >&2
}

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    err "required command not found: $1"
    exit 1
  fi
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --version)
      if [ "$#" -lt 2 ] || [ -z "${2:-}" ]; then
        err "--version requires a tag"
        usage
        exit 1
      fi
      VERSION="$2"
      shift 2
      ;;
    --dest)
      if [ "$#" -lt 2 ] || [ -z "${2:-}" ]; then
        err "--dest requires a directory"
        usage
        exit 1
      fi
      DEST="$2"
      shift 2
      ;;
    --system)
      DEST="/usr/local/bin"
      shift
      ;;
    --no-verify)
      VERIFY=0
      shift
      ;;
    --quiet|-q)
      QUIET=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      err "unknown option: $1"
      usage
      exit 1
      ;;
  esac
done

need_cmd curl
need_cmd tar

tmp=""
cleanup() {
  if [ -n "$tmp" ] && [ -d "$tmp" ]; then
    rm -rf "$tmp"
  fi
}
trap cleanup EXIT

resolve_version() {
  if [ -n "$VERSION" ]; then
    return 0
  fi

  info "Resolving latest GitHub release"
  local api_url="https://api.github.com/repos/${OWNER}/${REPO}/releases?per_page=1"
  VERSION="$(
    curl -fsSL "$api_url" \
      | grep '"tag_name":' \
      | head -1 \
      | sed -E 's/.*"tag_name":[[:space:]]*"([^"]+)".*/\1/' \
      || true
  )"
  if [ -z "$VERSION" ]; then
    err "failed to resolve latest release tag"
    err "pass --version vX.Y.Z or check network connectivity"
    exit 1
  fi
  ok "Resolved $VERSION"
}

detect_arch() {
  case "$(uname -m)" in
    x86_64|amd64)
      printf 'amd64\n'
      ;;
    aarch64|arm64)
      printf 'arm64\n'
      ;;
    *)
      err "unsupported CPU architecture: $(uname -m)"
      exit 1
      ;;
  esac
}

detect_linux_runtime() {
  if [ "$(uname -s)" != "Linux" ]; then
    err "prebuilt pi-server releases currently support Linux only"
    exit 1
  fi

  if [ -f /etc/alpine-release ] \
    || ls /lib/ld-musl-*.so.1 >/dev/null 2>&1 \
    || { command -v ldd >/dev/null 2>&1 && ldd --version 2>&1 | grep -qi musl; }; then
    printf 'alpine\n'
  else
    printf 'debian\n'
  fi
}

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    err "sha256sum or shasum is required for verification"
    exit 1
  fi
}

download_and_install() {
  local runtime arch asset base_url archive checksum_file expected actual extract_dir found_bin
  runtime="$(detect_linux_runtime)"
  arch="$(detect_arch)"
  asset="pi-server-${VERSION}-linux-${runtime}-${arch}.tar.gz"
  base_url="https://github.com/${OWNER}/${REPO}/releases/download/${VERSION}"

  tmp="$(mktemp -d)"
  archive="$tmp/$asset"
  checksum_file="$tmp/$asset.sha256"

  info "Downloading $asset"
  curl -fsSL --retry 3 --retry-delay 1 -o "$archive" "$base_url/$asset"

  if [ "$VERIFY" -eq 1 ]; then
    info "Downloading checksum"
    curl -fsSL --retry 3 --retry-delay 1 -o "$checksum_file" "$base_url/$asset.sha256"
    expected="$(sed -nE 's/.*([0-9a-fA-F]{64}).*/\1/p' "$checksum_file" | head -1)"
    if [ -z "$expected" ]; then
      err "checksum file did not contain a SHA256 digest"
      exit 1
    fi
    actual="$(sha256_file "$archive")"
    if [ "$actual" != "$expected" ]; then
      err "checksum mismatch for $asset"
      err "expected: $expected"
      err "actual:   $actual"
      exit 1
    fi
    ok "Checksum verified"
  fi

  extract_dir="$tmp/extract"
  mkdir -p "$extract_dir"
  tar -xzf "$archive" -C "$extract_dir"
  found_bin="$(find "$extract_dir" -type f -name "$BIN_NAME" | head -1)"
  if [ -z "$found_bin" ]; then
    err "archive did not contain $BIN_NAME"
    exit 1
  fi

  mkdir -p "$DEST"
  if [ ! -w "$DEST" ]; then
    err "install directory is not writable: $DEST"
    err "use --dest DIR, --system with sudo, or adjust permissions"
    exit 1
  fi

  install -m 0755 "$found_bin" "$DEST/$BIN_NAME"
  ok "Installed $BIN_NAME to $DEST/$BIN_NAME"
}

resolve_version
download_and_install

if ! command -v "$BIN_NAME" >/dev/null 2>&1; then
  case ":$PATH:" in
    *":$DEST:"*) ;;
    *)
      info "$DEST is not on PATH"
      info "Add this to your shell profile: export PATH=\"$DEST:\$PATH\""
      ;;
  esac
fi
