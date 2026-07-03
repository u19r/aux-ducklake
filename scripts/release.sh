#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
    cat >&2 <<'USAGE'
usage: scripts/release.sh [linux-amd64|linux-arm64|macos-arm64 ...]

Build release artifacts for the requested platforms. With no platform arguments,
builds Linux amd64, Linux arm64, and the current host platform when it is
macOS arm64.
USAGE
}

default_platforms() {
    case "$(uname -s)-$(uname -m)" in
        Darwin-arm64) printf '%s\n' macos-arm64 ;;
    esac
    printf '%s\n' linux-amd64 linux-arm64
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

if [[ "$#" -eq 0 ]]; then
    mapfile -t platforms < <(default_platforms)
else
    platforms=("$@")
fi

macos_release_built=0
for platform in "${platforms[@]}"; do
    case "$platform" in
        linux-amd64 | linux-arm64)
            "$ROOT_DIR/scripts/build_ducklake_linux_release.sh" "$platform"
            ;;
        macos-arm64)
            if [[ "$(uname -s)-$(uname -m)" != "Darwin-arm64" ]]; then
                echo "macos-arm64 release artifacts must be built on macOS arm64" >&2
                exit 1
            fi
            if [[ "$macos_release_built" == "0" ]]; then
                "$ROOT_DIR/scripts/build_ducklake_release.sh"
                macos_release_built=1
            fi
            "$ROOT_DIR/scripts/package_ducklake_release.sh" macos-arm64
            ;;
        *)
            echo "unsupported release platform: $platform" >&2
            usage
            exit 1
            ;;
    esac
done

echo "ducklake_release=ok"
echo "artifacts_dir=$ROOT_DIR/artifacts"
