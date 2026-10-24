#!/usr/bin/env bash

# Keep patch-state validation separate from checkout/bootstrap. A generated
# checkout may already contain the canonical bridge patch when a debug build is
# resumed, but its marker is trusted only after every canonical hunk verifies.

ducklake_patch_hash() {
    local path="$1"

    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$path" | awk '{print $1}'
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$path" | awk '{print $1}'
    else
        echo "sha256sum or shasum is required to hash $path" >&2
        return 1
    fi
}

ducklake_patch_content_is_applied() {
    local checkout_dir="$1"
    local patch_file="$2"

    git -C "$checkout_dir" apply --reverse --check "$patch_file" >/dev/null 2>&1
}

ducklake_patch_is_applied() {
    local checkout_dir="$1"
    local patch_file="$2"
    local marker_file="$3"
    local patch_hash="$4"

    [[ -f "$marker_file" ]] || return 1
    [[ "$(tr -d '\n' <"$marker_file")" == "$patch_hash" ]] || return 1
    ducklake_patch_content_is_applied "$checkout_dir" "$patch_file"
}

ducklake_patch_is_absent() {
    local checkout_dir="$1"
    local patch_file="$2"

    git -C "$checkout_dir" apply --check "$patch_file" >/dev/null 2>&1
}

ducklake_assert_canonical_source_shape() {
    local checkout_dir="$1"
    local metadata_manager="$checkout_dir/src/metadata_manager/aux_catalog_metadata_manager.cpp"

    [[ -f "$metadata_manager" ]] || {
        echo "canonical aux_catalog metadata manager source is missing: $metadata_manager" >&2
        return 1
    }
    grep -Fq "AuxCatalogMetadataManager::RunListFilesForTable" "$metadata_manager" || {
        echo "canonical aux_catalog metadata manager source is incomplete: RunListFilesForTable is missing" >&2
        return 1
    }
    grep -Fq "} // namespace duckdb" "$metadata_manager" || {
        echo "canonical aux_catalog metadata manager source is incomplete: namespace closure is missing" >&2
        return 1
    }
    [[ "$(tail -n 1 "$metadata_manager")" == "} // namespace duckdb" ]] || {
        echo "canonical aux_catalog metadata manager source has unexpected EOF" >&2
        return 1
    }
    [[ "$(tail -c 1 "$metadata_manager" | od -An -t x1 | tr -d '[:space:]')" == "0a" ]] || {
        echo "canonical aux_catalog metadata manager source is missing its final newline" >&2
        return 1
    }
}

ducklake_require_canonical_source_tree() {
    local checkout_dir="$1"
    local patch_file="$2"
    local marker_file="$3"
    local patch_hash="$4"

    if ! ducklake_patch_is_applied "$checkout_dir" "$patch_file" "$marker_file" "$patch_hash" &&
        ! ducklake_patch_content_is_applied "$checkout_dir" "$patch_file"; then
        echo "DuckLake bridge patch is not exactly applied; refusing to reuse this source tree" >&2
        return 1
    fi
    ducklake_assert_canonical_source_shape "$checkout_dir"
}

ducklake_ensure_canonical_patch() {
    local checkout_dir="$1"
    local patch_file="$2"
    local marker_file="$3"
    local patch_hash="$4"

    if ducklake_patch_is_applied "$checkout_dir" "$patch_file" "$marker_file" "$patch_hash"; then
        return 0
    fi
    # A missing or stale marker is recoverable only when every canonical hunk
    # reverse-checks. This repairs old bootstrap markers without accepting a
    # partially applied or drifted checkout.
    if ducklake_patch_content_is_applied "$checkout_dir" "$patch_file"; then
        printf '%s' "$patch_hash" >"$marker_file"
        return 0
    fi
    if ducklake_patch_is_absent "$checkout_dir" "$patch_file"; then
        git -C "$checkout_dir" apply "$patch_file"
        printf '%s' "$patch_hash" >"$marker_file"
        return 0
    fi
    echo "DuckLake bridge patch is partially applied or drifted; refusing to continue" >&2
    return 1
}
