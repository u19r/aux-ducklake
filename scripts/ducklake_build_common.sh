#!/usr/bin/env bash

ducklake_default_platform() {
    case "$(uname -s)-$(uname -m)" in
        Darwin-arm64) printf 'osx_arm64' ;;
        Darwin-x86_64) printf 'osx_amd64' ;;
        Linux-x86_64) printf 'linux_amd64' ;;
        Linux-aarch64 | Linux-arm64) printf 'linux_arm64' ;;
        *) return 1 ;;
    esac
}

ducklake_default_build_jobs() {
    if command -v sysctl >/dev/null 2>&1; then
        sysctl -n hw.logicalcpu 2>/dev/null && return
    fi
    if command -v nproc >/dev/null 2>&1; then
        nproc && return
    fi
    printf '4\n'
}

ducklake_default_build_generator() {
    local build_profile="$1"
    local ducklake_dir="$2"
    local cache_file="$ducklake_dir/build/$build_profile/CMakeCache.txt"
    local cached_generator

    if [[ -f "$cache_file" ]]; then
        cached_generator="$(awk -F= '/^CMAKE_GENERATOR:INTERNAL=/ { print $2; exit }' "$cache_file")"
        case "$cached_generator" in
            Ninja) printf 'ninja\n' && return ;;
            "Unix Makefiles") printf 'make\n' && return ;;
        esac
    fi

    if command -v ninja >/dev/null 2>&1; then
        printf 'ninja\n'
    fi
}

ducklake_cargo_target_dir() {
    "$1/scripts/cargo_with_sccache.sh" metadata --format-version 1 --no-deps |
        sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p'
}

ducklake_debug_target_dir() {
    local root_dir="$1"

    if [[ -n "${CARGO_TARGET_DIR:-}" ]]; then
        case "$CARGO_TARGET_DIR" in
            /*) printf '%s\n' "$CARGO_TARGET_DIR" ;;
            *) printf '%s\n' "$root_dir/$CARGO_TARGET_DIR" ;;
        esac
        return
    fi

    ducklake_cargo_target_dir "$root_dir"
}

ducklake_reuse_debug_build_enabled() {
    [[ "${AUX_DUCKLAKE_REUSE_DUCKLAKE_BUILD:-0}" == "1" ||
        "${AUX_DUCKLAKE_E2E_SKIP_DUCKLAKE_BUILD:-0}" == "1" ]]
}

ducklake_runtime_library_name() {
    case "$(uname -s)" in
        Darwin) printf 'libducklake_catalog.dylib' ;;
        Linux) printf 'libducklake_catalog.so' ;;
        *) return 1 ;;
    esac
}

ducklake_release_runtime_library() {
    local root_dir="$1"
    local target_dir
    target_dir="$(ducklake_cargo_target_dir "$root_dir")"
    printf '%s/release/%s\n' "$target_dir" "$(ducklake_runtime_library_name)"
}

ducklake_build_debug_catalog_runtime() {
    local root_dir="$1"
    local no_default_features="$2"
    local features="${3:-}"
    local runtime_library

    if [[ "$no_default_features" == "1" && -n "$features" ]]; then
        "$root_dir/scripts/cargo_with_sccache.sh" build -q -p ducklake-catalog --no-default-features --features "$features"
    elif [[ "$no_default_features" == "1" ]]; then
        "$root_dir/scripts/cargo_with_sccache.sh" build -q -p ducklake-catalog --no-default-features
    elif [[ -n "$features" ]]; then
        "$root_dir/scripts/cargo_with_sccache.sh" build -q -p ducklake-catalog --features "$features"
    else
        "$root_dir/scripts/cargo_with_sccache.sh" build -q -p ducklake-catalog
    fi

    runtime_library="$(ducklake_debug_target_dir "$root_dir")/debug/$(ducklake_runtime_library_name)"
    [[ -f "$runtime_library" ]] || {
        echo "runtime library was not built at $runtime_library" >&2
        return 1
    }
    printf '%s\n' "$runtime_library"
}

ducklake_release_version() {
    local ducklake_dir="$1"
    if [[ -n "${AUX_DUCKLAKE_RELEASE_VERSION:-}" ]]; then
        printf '%s\n' "$AUX_DUCKLAKE_RELEASE_VERSION"
    elif git -C "$ducklake_dir" rev-parse --short HEAD >/dev/null 2>&1; then
        git -C "$ducklake_dir" rev-parse --short HEAD
    elif [[ -f "$ducklake_dir/.aux-ducklake-pinned-commit" ]]; then
        cut -c1-8 "$ducklake_dir/.aux-ducklake-pinned-commit"
    else
        printf 'local\n'
    fi
}

ducklake_foundationdb_client_libraries() {
    case "$(uname -s)" in
        Darwin)
            find /opt/homebrew/lib /usr/local/lib -maxdepth 1 -name 'libfdb_c*.dylib' -print 2>/dev/null || true
            ;;
        Linux)
            find /usr/lib64 /usr/lib /usr/local/lib -maxdepth 1 \( -name 'libfdb_c.so' -o -name 'libfdb_c.so.*' \) -print 2>/dev/null || true
            ;;
    esac
}

ducklake_postgres_cmake_args() {
    local mode="${1:-required}"
    local pg_config_bin pg_include_dir pg_library_dir pg_library

    pg_config_bin="${PG_CONFIG:-$(command -v pg_config || true)}"
    if [[ -z "$pg_config_bin" || ! -x "$pg_config_bin" ]]; then
        [[ "$mode" == "optional" ]] && return 0
        echo "pg_config is required to build postgres_scanner" >&2
        return 1
    fi

    pg_include_dir="$("$pg_config_bin" --includedir)" || return 1
    pg_library_dir="$("$pg_config_bin" --libdir)" || return 1
    pg_library="$pg_library_dir/libpq.dylib"
    [[ -f "$pg_library" ]] || pg_library="$pg_library_dir/libpq.so"
    if [[ ! -f "$pg_library" ]]; then
        [[ "$mode" == "optional" ]] && return 0
        echo "could not find libpq in $pg_library_dir" >&2
        return 1
    fi

    printf '%s\n' "-DPostgreSQL_INCLUDE_DIR=$pg_include_dir"
    printf '%s\n' "-DPostgreSQL_LIBRARY=$pg_library"
}

ducklake_postgres_ext_debug_flags() {
    local mode="${1:-required}"
    local args_output include_arg library_arg

    args_output="$(ducklake_postgres_cmake_args "$mode")" || return 1
    while IFS= read -r arg; do
        case "$arg" in
            -DPostgreSQL_INCLUDE_DIR=*) include_arg="$arg" ;;
            -DPostgreSQL_LIBRARY=*) library_arg="$arg" ;;
        esac
    done <<<"$args_output"

    [[ -n "${include_arg:-}" && -n "${library_arg:-}" ]] || return 0
    printf '%s %s\n' "$include_arg" "$library_arg"
}

ducklake_clean_stale_postgres_scanner_fetch() {
    local ducklake_dir="$1"
    local postgres_scanner_extension="$2"

    if [[ -d "$ducklake_dir/build/debug/_deps/postgres_scanner_extension_fc-src" &&
        ! -f "$postgres_scanner_extension" ]]; then
        rm -rf "$ducklake_dir/build/debug/_deps/postgres_scanner_extension_fc-"*
    fi
}

ducklake_build_debug_duckdb_with_postgres_if_needed() {
    local root_dir="$1"
    local ducklake_dir="$2"
    local duckdb_bin="$3"
    local postgres_scanner_extension="$4"
    local postgres_flags

    if ducklake_reuse_debug_build_enabled && [[ -x "$duckdb_bin" && -f "$postgres_scanner_extension" ]]; then
        return
    fi

    ducklake_clean_stale_postgres_scanner_fetch "$ducklake_dir" "$postgres_scanner_extension"
    postgres_flags="$(ducklake_postgres_ext_debug_flags required)" || return 1
    EXT_DEBUG_FLAGS="$postgres_flags" \
        ENABLE_POSTGRES_SCANNER=1 \
        "$root_dir/scripts/build_ducklake_debug.sh"

    [[ -x "$duckdb_bin" ]] || {
        echo "modified duckdb executable was not built: $duckdb_bin" >&2
        return 1
    }
    [[ -f "$postgres_scanner_extension" ]] || {
        echo "postgres_scanner extension was not built: $postgres_scanner_extension" >&2
        return 1
    }
}

ducklake_build_debug_unittest_if_needed() {
    local root_dir="$1"
    local unittest_bin="$2"
    local backend="$3"
    local postgres_scanner_extension="$4"
    local postgres_flags

    if ducklake_reuse_debug_build_enabled && [[ -x "$unittest_bin" ]] &&
        [[ "$backend" != "postgres" || -f "$postgres_scanner_extension" ]]; then
        return
    fi

    if [[ "$backend" == "postgres" ]]; then
        postgres_flags="$(ducklake_postgres_ext_debug_flags required)" || return 1
        EXT_DEBUG_FLAGS="$postgres_flags" \
            ENABLE_POSTGRES_SCANNER=1 \
            "$root_dir/scripts/build_ducklake_debug.sh"
    else
        "$root_dir/scripts/build_ducklake_debug.sh"
    fi

    [[ -x "$unittest_bin" ]] || {
        echo "DuckLake unittest binary was not built at $unittest_bin" >&2
        return 1
    }
}

ducklake_build_debug_duckdb_if_needed() {
    local root_dir="$1"
    local duckdb_bin="$2"

    echo "e2e_step=build_modified_ducklake"
    if ! ducklake_reuse_debug_build_enabled; then
        "$root_dir/scripts/build_ducklake_debug.sh"
    fi
    [[ -x "$duckdb_bin" ]] || {
        echo "modified duckdb executable was not built: $duckdb_bin" >&2
        return 1
    }
}

ducklake_write_sha256() {
    local path="$1"
    local output="$2"
    if command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$path" >"$output"
    elif command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$path" >"$output"
    else
        echo "shasum or sha256sum is required to write $output" >&2
        exit 1
    fi
}

ducklake_ensure_source_tree() {
    local root_dir="$1"
    local ducklake_dir="$2"

    if [[ "${AUX_DUCKLAKE_SKIP_FETCH:-0}" != "1" ]]; then
        "$root_dir/scripts/fetch_ducklake.sh"
    else
        [[ -d "$ducklake_dir" ]] || {
            echo "third_party/ducklake is missing; unset AUX_DUCKLAKE_SKIP_FETCH for the first build" >&2
            exit 1
        }
    fi
}

ducklake_configure_build_environment() {
    local build_profile="$1"
    local ducklake_dir="$2"

    if [[ -z "${DUCKDB_PLATFORM:-}" ]]; then
        DUCKDB_PLATFORM="$(ducklake_default_platform)"
        export DUCKDB_PLATFORM
    fi

    export DISABLE_SANITIZER="${DISABLE_SANITIZER:-1}"
    export GEN="${GEN:-$(ducklake_default_build_generator "$build_profile" "$ducklake_dir")}"
    export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-$(ducklake_default_build_jobs)}"
}
