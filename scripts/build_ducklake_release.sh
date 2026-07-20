#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DUCKLAKE_DIR="$ROOT_DIR/third_party/ducklake"
. "$ROOT_DIR/scripts/ducklake_build_common.sh"

ducklake_ensure_source_tree "$ROOT_DIR" "$DUCKLAKE_DIR"
ducklake_configure_build_environment release "$DUCKLAKE_DIR"

if command -v pkg-config >/dev/null 2>&1; then
    curl_include_dir="$(pkg-config --variable=includedir libcurl 2>/dev/null || true)"
    curl_header="$curl_include_dir/curl/curl.h"
    if [[ -n "$curl_include_dir" && -f "$curl_header" ]] &&
        ! grep -q "CURLSSLOPT_AUTO_CLIENT_CERT" "$curl_header"; then
        curl_compat_flag="-DCURLSSLOPT_AUTO_CLIENT_CERT=0"
        export CFLAGS="${CFLAGS:+$CFLAGS }$curl_compat_flag"
        export CXXFLAGS="${CXXFLAGS:+$CXXFLAGS }$curl_compat_flag"
    fi
fi

"$ROOT_DIR/scripts/cargo_with_sccache.sh" build -q -p ducklake-catalog --no-default-features --features foundationdb --release

release_dir="$DUCKLAKE_DIR/build/release"
rm -rf "$release_dir"
cmake_args=(
    -DFORCE_COLORED_OUTPUT=1
    -DEXTENSION_STATIC_BUILD=1
    -DDUCKDB_EXTENSION_CONFIGS="$DUCKLAKE_DIR/extension_config.cmake"
    -DDUCKDB_EXPLICIT_PLATFORM="$DUCKDB_PLATFORM"
    -DOVERRIDE_GIT_DESCRIBE="${AUX_DUCKLAKE_DUCKDB_GIT_DESCRIBE:-}"
    -DENABLE_UNITTEST_CPP_TESTS=FALSE
    -DENABLE_SANITIZER=FALSE
    -DENABLE_UBSAN=0
    -DCMAKE_BUILD_TYPE=Release
)
while IFS= read -r postgres_arg; do
    [[ -n "$postgres_arg" ]] && cmake_args+=("$postgres_arg")
done < <(ducklake_postgres_cmake_args required)
if [[ -n "${CMAKE_PREFIX_PATH:-}" ]]; then
    cmake_args+=("-DCMAKE_PREFIX_PATH=$CMAKE_PREFIX_PATH")
fi
case "${GEN:-}" in
    ninja) cmake_args=(-G Ninja "${cmake_args[@]}") ;;
    make | "") ;;
    *) echo "unsupported GEN for release build: $GEN" >&2; exit 1 ;;
esac
ENABLE_POSTGRES_SCANNER=1 cmake "${cmake_args[@]}" -S "$DUCKLAKE_DIR/duckdb" -B "$release_dir"
case "$(uname -s)" in
    Darwin) duckdb_shared_target="libduckdb.dylib" ;;
    Linux) duckdb_shared_target="libduckdb.so" ;;
    *) echo "unsupported release build host: $(uname -s)" >&2; exit 1 ;;
esac
cmake --build "$release_dir" --config Release --target \
    duckdb \
    shell \
    postgres_scanner.duckdb_extension \
    "$duckdb_shared_target"

runtime_library="$(ducklake_release_runtime_library "$ROOT_DIR")"
postgres_scanner_extension="$release_dir/extension/postgres_scanner/postgres_scanner.duckdb_extension"
[[ -x "$release_dir/duckdb" ]] || {
    echo "missing release DuckDB binary at $release_dir/duckdb" >&2
    exit 1
}
[[ -f "$DUCKLAKE_DIR/duckdb/src/include/duckdb.h" ]] || {
    echo "missing DuckDB C header at $DUCKLAKE_DIR/duckdb/src/include/duckdb.h" >&2
    exit 1
}
if ! compgen -G "$release_dir/src/libduckdb.so*" >/dev/null && ! compgen -G "$release_dir/src/libduckdb*.dylib" >/dev/null; then
    echo "missing release DuckDB shared library in $release_dir/src" >&2
    exit 1
fi
[[ -f "$runtime_library" ]] || {
    echo "missing release Rust FFI runtime at $runtime_library" >&2
    exit 1
}
[[ -f "$postgres_scanner_extension" ]] || {
    echo "missing release postgres_scanner extension at $postgres_scanner_extension" >&2
    exit 1
}

echo "ducklake_release_build=ok"
echo "duckdb_platform=$DUCKDB_PLATFORM"
echo "duckdb_bin=$release_dir/duckdb"
echo "duckdb_header=$DUCKLAKE_DIR/duckdb/src/include/duckdb.h"
echo "runtime_library=$runtime_library"
