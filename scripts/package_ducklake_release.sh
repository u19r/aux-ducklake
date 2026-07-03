#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -gt 1 ]]; then
    echo "usage: $0 [platform]" >&2
    exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DUCKLAKE_DIR="$ROOT_DIR/third_party/ducklake"
. "$ROOT_DIR/scripts/ducklake_build_common.sh"

release_dir="$DUCKLAKE_DIR/build/release"

default_platform() {
    case "$(uname -s)-$(uname -m)" in
        Darwin-arm64) printf 'macos-arm64' ;;
        Linux-x86_64) printf 'linux-amd64' ;;
        Linux-aarch64 | Linux-arm64) printf 'linux-arm64' ;;
        *) return 1 ;;
    esac
}

platform="${1:-$(default_platform)}"
version="$(ducklake_release_version "$DUCKLAKE_DIR")"
artifact_dir="$ROOT_DIR/artifacts"
bundle_root="$ROOT_DIR/build/release/$platform"
duckdb_dir="$bundle_root/opt/duckdb"
runtime_dir="$bundle_root/opt/aux-ducklake/lib"
runtime_library="$(ducklake_release_runtime_library "$ROOT_DIR")"
artifact_name="aux-ducklake-fdb-v$version-$platform.tar.gz"

[[ -x "$release_dir/duckdb" ]] || {
    echo "missing release DuckDB binary at $release_dir/duckdb; run scripts/build_ducklake_release.sh first" >&2
    exit 1
}
[[ -f "$DUCKLAKE_DIR/duckdb/src/include/duckdb.h" ]] || {
    echo "missing DuckDB C header; run scripts/build_ducklake_release.sh first" >&2
    exit 1
}
[[ -f "$runtime_library" ]] || {
    echo "missing release Rust FFI runtime at $runtime_library; run scripts/build_ducklake_release.sh first" >&2
    exit 1
}
if ! compgen -G "$release_dir/src/libduckdb.so*" >/dev/null && ! compgen -G "$release_dir/src/libduckdb*.dylib" >/dev/null; then
    echo "missing DuckDB shared library in $release_dir/src; run scripts/build_ducklake_release.sh first" >&2
    exit 1
fi

rm -rf "$bundle_root"
mkdir -p "$duckdb_dir" "$runtime_dir"
cp "$release_dir/duckdb" "$duckdb_dir/duckdb"
cp "$DUCKLAKE_DIR/duckdb/src/include/duckdb.h" "$duckdb_dir/duckdb.h"
cp "$runtime_library" "$runtime_dir/"

for library in "$release_dir"/src/libduckdb.so* "$release_dir"/src/libduckdb*.dylib; do
    if [[ -f "$library" ]]; then
        cp -a "$library" "$duckdb_dir/"
    fi
done

while IFS= read -r library; do
    [[ -n "$library" ]] || continue
    cp -a "$library" "$runtime_dir/"
done < <(ducklake_foundationdb_client_libraries)

mkdir -p "$artifact_dir"
tar -C "$bundle_root" -czf "$artifact_dir/$artifact_name" opt
ducklake_write_sha256 "$artifact_dir/$artifact_name" "$artifact_dir/$artifact_name.sha256"

echo "ducklake_release_package=ok"
echo "artifact=$artifact_dir/$artifact_name"
