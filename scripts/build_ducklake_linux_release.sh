#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -gt 1 ]]; then
    echo "usage: $0 [linux-amd64|linux-arm64]" >&2
    exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DUCKLAKE_DIR="$ROOT_DIR/third_party/ducklake"
. "$ROOT_DIR/scripts/ducklake_build_common.sh"

platform="${1:-linux-amd64}"

case "$platform" in
    linux-amd64)
        docker_platform="linux/amd64"
        duckdb_platform="linux_amd64"
        ;;
    linux-arm64)
        docker_platform="linux/arm64"
        duckdb_platform="linux_arm64"
        ;;
    *)
        echo "unsupported Linux release platform: $platform" >&2
        exit 1
        ;;
esac

command -v docker >/dev/null 2>&1 || {
    echo "docker is required for Linux release builds" >&2
    exit 1
}

output_dir="$ROOT_DIR/build/docker-release/$platform"
release_version="$(ducklake_release_version "$DUCKLAKE_DIR")"
rm -rf "$output_dir"
mkdir -p "$output_dir" "$ROOT_DIR/artifacts"

docker buildx build \
    --platform "$docker_platform" \
    --build-arg "DUCKDB_PLATFORM=$duckdb_platform" \
    --build-arg "AUX_DUCKLAKE_PACKAGE_PLATFORM=$platform" \
    --build-arg "AUX_DUCKLAKE_RELEASE_VERSION=$release_version" \
    --output "type=local,dest=$output_dir" \
    -f "$ROOT_DIR/docker/ducklake-release.Dockerfile" \
    "$ROOT_DIR"

cp "$output_dir"/artifacts/* "$ROOT_DIR/artifacts/"

echo "ducklake_linux_release_build=ok"
echo "platform=$platform"
echo "version=$release_version"
echo "artifacts_dir=$ROOT_DIR/artifacts"
