#!/usr/bin/env bash
# Versions: software-versions.snapshot.json; details in software-version-consumers.md.

# Keep script-owned SQL output independent of inherited coding-agent markers.
export DUCKDB_AGENT_MODE=0

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
    "$1/scripts/cargo_with_sccache.sh" metadata --manifest-path "$1/Cargo.toml" --format-version 1 --no-deps |
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
    elif [[ -f "$ducklake_dir/.aux-ducklake-pinned-commit" ]]; then
        cut -c1-8 "$ducklake_dir/.aux-ducklake-pinned-commit"
    elif git -C "$ducklake_dir" rev-parse HEAD >/dev/null 2>&1; then
        git -C "$ducklake_dir" rev-parse HEAD | cut -c1-8
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
    if [[ -z "$pg_config_bin" && "$(uname -s)" == "Darwin" ]]; then
        local candidate
        for candidate in /opt/homebrew/opt/libpq/bin/pg_config /usr/local/opt/libpq/bin/pg_config; do
            if [[ -x "$candidate" ]]; then
                pg_config_bin="$candidate"
                break
            fi
        done
    fi
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
        ducklake_validate_debug_artifacts "$root_dir" "$ducklake_dir" || return 1
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
    ducklake_validate_debug_artifacts "$root_dir" "$ducklake_dir"
}

ducklake_build_debug_unittest_if_needed() {
    local root_dir="$1"
    local unittest_bin="$2"
    local backend="$3"
    local postgres_scanner_extension="$4"
    local postgres_flags

    if ducklake_reuse_debug_build_enabled && [[ -x "$unittest_bin" ]] &&
        [[ "$backend" != "postgres" || -f "$postgres_scanner_extension" ]]; then
        ducklake_validate_debug_artifacts "$root_dir" "$root_dir/third_party/ducklake" || return 1
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
    ducklake_validate_debug_artifacts "$root_dir" "$root_dir/third_party/ducklake"
}

ducklake_build_debug_duckdb_if_needed() {
    local root_dir="$1"
    local duckdb_bin="$2"
    local ducklake_dir="$root_dir/third_party/ducklake"

    echo "e2e_step=build_modified_ducklake"
    if ! ducklake_reuse_debug_build_enabled; then
        "$root_dir/scripts/build_ducklake_debug.sh"
    else
        ducklake_validate_debug_artifacts "$root_dir" "$ducklake_dir"
    fi
    [[ -x "$duckdb_bin" ]] || {
        echo "modified duckdb executable was not built: $duckdb_bin" >&2
        return 1
    }
    ducklake_validate_debug_artifacts "$root_dir" "$ducklake_dir"
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

ducklake_hash_stream() {
    if command -v shasum >/dev/null 2>&1; then
        shasum -a 256
    elif command -v sha256sum >/dev/null 2>&1; then
        sha256sum
    else
        echo "shasum or sha256sum is required to hash build inputs" >&2
        return 1
    fi
}

ducklake_file_hash() {
    ducklake_hash_stream <"$1" | awk '{print $1}'
}

ducklake_validate_source_tree() {
    local root_dir="$1"
    local ducklake_dir="$2"
    . "$root_dir/scripts/ducklake-upstream-pins.sh"
    local ducklake_commit="$DUCKLAKE_COMMIT"
    local duckdb_commit="$DUCKDB_COMMIT"
    local patch_file="$root_dir/patches/ducklake/0001-add-aux-catalog-metadata-manager.patch"
    local patch_marker="$ducklake_dir/.aux-ducklake-bridge-patch"
    local ducklake_pin="$ducklake_dir/.aux-ducklake-pinned-commit"
    local duckdb_pin="$ducklake_dir/.aux-duckdb-pinned-commit"

    [[ -d "$ducklake_dir/.git" ]] || {
        echo "DuckLake source checkout is missing: $ducklake_dir" >&2
        return 1
    }
    [[ "$(git -C "$ducklake_dir" rev-parse HEAD 2>/dev/null)" == "$ducklake_commit" ]] || {
        echo "DuckLake source checkout is not pinned to $ducklake_commit" >&2
        return 1
    }
    [[ -f "$ducklake_pin" ]] &&
        [[ "$(tr -d '\r\n' <"$ducklake_pin")" == "$ducklake_commit" ]] || {
        echo "DuckLake source pin marker is missing or stale: $ducklake_pin" >&2
        return 1
    }
    [[ -e "$ducklake_dir/duckdb/.git" ]] || {
        echo "DuckDB submodule checkout is missing: $ducklake_dir/duckdb" >&2
        return 1
    }
    [[ "$(git -C "$ducklake_dir/duckdb" rev-parse HEAD 2>/dev/null)" == "$duckdb_commit" ]] || {
        echo "DuckDB submodule is not pinned to $duckdb_commit" >&2
        return 1
    }
    [[ -f "$duckdb_pin" ]] &&
        [[ "$(tr -d '\r\n' <"$duckdb_pin")" == "$duckdb_commit" ]] || {
        echo "DuckDB submodule pin marker is missing or stale: $duckdb_pin" >&2
        return 1
    }
    [[ -f "$patch_file" ]] || {
        echo "canonical DuckLake bridge patch is missing: $patch_file" >&2
        return 1
    }

    # Source this side-effect-free validator only after resolving the
    # requested repository. It checks every canonical hunk and the generated
    # manager's required EOF/function shape without modifying the checkout.
    . "$root_dir/scripts/ducklake_patch_state.sh"
    local patch_hash
    patch_hash="$(ducklake_patch_hash "$patch_file")" || return 1
    ducklake_require_canonical_source_tree "$ducklake_dir" "$patch_file" "$patch_marker" "$patch_hash"
}

ducklake_debug_source_fingerprint() {
    local ducklake_dir="$1"

    {
        printf 'ducklake_head=%s\n' "$(git -C "$ducklake_dir" rev-parse HEAD)"
        git -C "$ducklake_dir" diff --binary HEAD
        git -C "$ducklake_dir" ls-files --others --exclude-standard | sort | while IFS= read -r path; do
            case "$path" in
            .aux-* | .cache/* | build/*) continue ;;
            esac
            printf 'ducklake_untracked=%s\t' "$path"
            ducklake_file_hash "$ducklake_dir/$path"
        done
        if [[ -e "$ducklake_dir/duckdb/.git" ]]; then
            printf 'duckdb_head=%s\n' "$(git -C "$ducklake_dir/duckdb" rev-parse HEAD)"
            git -C "$ducklake_dir/duckdb" diff --binary HEAD
            git -C "$ducklake_dir/duckdb" ls-files --others --exclude-standard | sort |
                while IFS= read -r path; do
                    case "$path" in
                    .cache/* | build/*) continue ;;
                    esac
                    printf 'duckdb_untracked=%s\t' "$path"
                    ducklake_file_hash "$ducklake_dir/duckdb/$path"
                done
        fi
    } | ducklake_hash_stream | awk '{print $1}'
}

ducklake_debug_artifact_fingerprint_path() {
    local ducklake_dir="$1"
    printf '%s/build/debug/.aux-ducklake-debug-fingerprint\n' "$ducklake_dir"
}

ducklake_debug_artifact_relpaths() {
    local ducklake_dir="$1"

    printf '%s\n' \
        'duckdb' \
        'extension/ducklake/ducklake.duckdb_extension' \
        'test/unittest'
    if [[ -f "$ducklake_dir/build/debug/extension/postgres_scanner/postgres_scanner.duckdb_extension" ]]; then
        printf '%s\n' 'extension/postgres_scanner/postgres_scanner.duckdb_extension'
    fi
}

ducklake_file_size() {
    wc -c <"$1" | tr -d '[:space:]'
}

ducklake_file_mtime() {
    local path="$1"
    local mtime

    # Homebrew may put GNU coreutils ahead of the BSD utilities on macOS, so
    # select the format by validating the result instead of by uname alone.
    mtime="$(stat -f '%m' "$path" 2>/dev/null || true)"
    if [[ "$mtime" =~ ^[0-9]+$ ]]; then
        printf '%s\n' "$mtime"
        return 0
    fi
    mtime="$(stat -c '%Y' "$path" 2>/dev/null || true)"
    if [[ "$mtime" =~ ^[0-9]+$ ]]; then
        printf '%s\n' "$mtime"
        return 0
    fi
    echo "unsupported stat implementation for artifact freshness metadata: $path" >&2
    return 1
}

ducklake_write_debug_artifact_fingerprint() {
    local root_dir="$1"
    local ducklake_dir="$2"
    local build_dir="$ducklake_dir/build/debug"
    local fingerprint_file
    local fingerprint_tmp
    local source_fingerprint
    local build_config_fingerprint
    local rel path

    ducklake_validate_source_tree "$root_dir" "$ducklake_dir"
    [[ -f "$build_dir/CMakeCache.txt" ]] || {
        echo "debug build configuration is missing: $build_dir/CMakeCache.txt" >&2
        return 1
    }
    while IFS= read -r rel; do
        path="$build_dir/$rel"
        [[ -f "$path" ]] || {
            echo "debug build artifact is missing: $path" >&2
            return 1
        }
        case "$rel" in
        duckdb | test/unittest) [[ -x "$path" ]] || {
            echo "debug executable is not executable: $path" >&2
            return 1
        } ;;
        esac
    done < <(ducklake_debug_artifact_relpaths "$ducklake_dir")

    source_fingerprint="$(ducklake_debug_source_fingerprint "$ducklake_dir")" || return 1
    build_config_fingerprint="$(ducklake_file_hash "$build_dir/CMakeCache.txt")" || return 1
    fingerprint_file="$(ducklake_debug_artifact_fingerprint_path "$ducklake_dir")"
    fingerprint_tmp="$fingerprint_file.$$"
    {
        printf 'format=1\n'
        printf 'source_fingerprint=%s\n' "$source_fingerprint"
        printf 'build_config_fingerprint=%s\n' "$build_config_fingerprint"
        while IFS= read -r rel; do
            path="$build_dir/$rel"
            printf 'artifact\t%s\t%s\t%s\t%s\n' \
                "$rel" \
                "$(ducklake_file_hash "$path")" \
                "$(ducklake_file_size "$path")" \
                "$(ducklake_file_mtime "$path")"
        done < <(ducklake_debug_artifact_relpaths "$ducklake_dir")
    } >"$fingerprint_tmp"
    mv -f "$fingerprint_tmp" "$fingerprint_file"
    printf '%s\n' "$fingerprint_file"
}

ducklake_validate_debug_artifacts() {
    local root_dir="$1"
    local ducklake_dir="$2"
    local build_dir="$ducklake_dir/build/debug"
    local fingerprint_file
    local expected_source
    local expected_config
    local recorded_source
    local recorded_config
    local kind rel expected_hash expected_size expected_mtime path actual_hash actual_size actual_mtime

    ducklake_validate_source_tree "$root_dir" "$ducklake_dir"
    fingerprint_file="$(ducklake_debug_artifact_fingerprint_path "$ducklake_dir")"
    [[ -f "$fingerprint_file" ]] || {
        echo "debug artifact fingerprint is missing: $fingerprint_file" >&2
        return 1
    }
    [[ -f "$build_dir/CMakeCache.txt" ]] || {
        echo "debug build configuration is missing: $build_dir/CMakeCache.txt" >&2
        return 1
    }
    expected_source="$(ducklake_debug_source_fingerprint "$ducklake_dir")" || return 1
    expected_config="$(ducklake_file_hash "$build_dir/CMakeCache.txt")" || return 1
    recorded_source="$(sed -n 's/^source_fingerprint=//p' "$fingerprint_file")"
    recorded_config="$(sed -n 's/^build_config_fingerprint=//p' "$fingerprint_file")"
    [[ "$recorded_source" == "$expected_source" ]] || {
        echo "debug artifacts are stale for the current DuckLake source tree" >&2
        return 1
    }
    [[ "$recorded_config" == "$expected_config" ]] || {
        echo "debug artifacts are stale for the current build configuration" >&2
        return 1
    }

    while IFS=$'\t' read -r kind rel expected_hash expected_size expected_mtime; do
        [[ "$kind" == artifact ]] || continue
        case "$rel" in
        duckdb | extension/ducklake/ducklake.duckdb_extension | test/unittest | extension/postgres_scanner/postgres_scanner.duckdb_extension) ;;
        *)
            echo "debug artifact fingerprint contains an unknown path: $rel" >&2
            return 1
            ;;
        esac
        path="$build_dir/$rel"
        [[ -f "$path" ]] || {
            echo "fingerprinted debug artifact is missing: $path" >&2
            return 1
        }
        actual_hash="$(ducklake_file_hash "$path")"
        actual_size="$(ducklake_file_size "$path")"
        actual_mtime="$(ducklake_file_mtime "$path")"
        [[ "$actual_hash" == "$expected_hash" && "$actual_size" == "$expected_size" &&
            "$actual_mtime" == "$expected_mtime" ]] || {
            echo "debug artifact changed after its fingerprint was written: $path" >&2
            return 1
        }
    done <"$fingerprint_file"

    while IFS= read -r rel; do
        grep -Fq $'artifact\t'"$rel"$'\t' "$fingerprint_file" || {
            echo "debug artifact is not covered by its fingerprint: $rel" >&2
            return 1
        }
    done < <(ducklake_debug_artifact_relpaths "$ducklake_dir")
}

ducklake_ensure_source_tree() {
    local root_dir="$1"
    local ducklake_dir="$2"

    if [[ "${AUX_DUCKLAKE_SKIP_FETCH:-0}" != "1" ]]; then
        "$root_dir/scripts/fetch_ducklake.sh"
        ducklake_validate_source_tree "$root_dir" "$ducklake_dir"
    else
        [[ -d "$ducklake_dir" ]] || {
            echo "third_party/ducklake is missing; unset AUX_DUCKLAKE_SKIP_FETCH for the first build" >&2
            exit 1
        }
        ducklake_validate_source_tree "$root_dir" "$ducklake_dir"
    fi
}

ducklake_configure_build_environment() {
    local build_profile="$1"
    local ducklake_dir="$2"
    # shellcheck source=scripts/ducklake-upstream-pins.sh
    . "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/ducklake-upstream-pins.sh"
    export OVERRIDE_GIT_DESCRIBE="$DUCKDB_VERSION"
    if [[ "$(uname -s)" == Linux ]]; then
        /opt/aux-mold-3.0.0/bin/mold --version
        export LDFLAGS="${LDFLAGS:+$LDFLAGS }-B/opt/aux-mold-3.0.0/libexec/mold"
        export CMAKE_VARS="${CMAKE_VARS:+$CMAKE_VARS }-DCMAKE_C_LINKER_LAUNCHER=/opt/aux-mold-3.0.0/bin/mold-link -DCMAKE_CXX_LINKER_LAUNCHER=/opt/aux-mold-3.0.0/bin/mold-link"
    fi

    if [[ -z "${DUCKDB_PLATFORM:-}" ]]; then
        DUCKDB_PLATFORM="$(ducklake_default_platform)"
        export DUCKDB_PLATFORM
    fi

    export DISABLE_SANITIZER="${DISABLE_SANITIZER:-1}"
    export GEN="${GEN:-$(ducklake_default_build_generator "$build_profile" "$ducklake_dir")}"
    export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-$(ducklake_default_build_jobs)}"
}
