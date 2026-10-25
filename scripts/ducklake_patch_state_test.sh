#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
. "$ROOT_DIR/scripts/ducklake_patch_state.sh"

test_dir="$(mktemp -d "${TMPDIR:-/tmp}/ducklake-patch-state.XXXXXX")"
trap 'rm -rf "$test_dir"' EXIT

fail() {
    echo "ducklake patch state test failure: $*" >&2
    exit 1
}

new_repo() {
    local name="$1"
    local repo="$test_dir/$name"
    local patch_file="$test_dir/$name.patch"

    mkdir -p "$repo"
    git -C "$repo" init -q
    printf 'base\n' >"$repo/file.txt"
    git -C "$repo" add file.txt
    git -C "$repo" -c core.hooksPath=/dev/null -c commit.gpgsign=false \
        -c user.name=test -c user.email=test@example.invalid commit -qm base
    printf 'patched\n' >"$repo/file.txt"
    git -C "$repo" diff --binary >"$patch_file"
    git -C "$repo" show HEAD:file.txt >"$repo/file.txt"
    printf '%s\n' "$repo" "$patch_file"
}

new_multi_repo() {
    local name="$1"
    local repo="$test_dir/$name"
    local patch_file="$test_dir/$name.patch"

    mkdir -p "$repo"
    git -C "$repo" init -q
    printf 'base-a\n' >"$repo/file-a.txt"
    printf 'base-b\n' >"$repo/file-b.txt"
    git -C "$repo" add file-a.txt file-b.txt
    git -C "$repo" -c core.hooksPath=/dev/null -c commit.gpgsign=false \
        -c user.name=test -c user.email=test@example.invalid commit -qm base
    printf 'patched-a\n' >"$repo/file-a.txt"
    printf 'patched-b\n' >"$repo/file-b.txt"
    git -C "$repo" diff --binary >"$patch_file"
    git -C "$repo" show HEAD:file-a.txt >"$repo/file-a.txt"
    git -C "$repo" show HEAD:file-b.txt >"$repo/file-b.txt"
    printf '%s\n' "$repo" "$patch_file"
}

mapfile -t fixture < <(new_repo clean)
clean_repo="${fixture[0]}"
clean_patch="${fixture[1]}"
clean_marker="$clean_repo/.marker"
clean_hash="$(ducklake_patch_hash "$clean_patch")"
ducklake_ensure_canonical_patch "$clean_repo" "$clean_patch" "$clean_marker" "$clean_hash"
ducklake_patch_is_applied "$clean_repo" "$clean_patch" "$clean_marker" "$clean_hash" || fail "clean patch was not applied"

mapfile -t fixture < <(new_repo exact)
exact_repo="${fixture[0]}"
exact_patch="${fixture[1]}"
exact_marker="$exact_repo/.marker"
exact_hash="$(ducklake_patch_hash "$exact_patch")"
ducklake_ensure_canonical_patch "$exact_repo" "$exact_patch" "$exact_marker" "$exact_hash"
printf 'stale-marker' >"$exact_marker"
ducklake_ensure_canonical_patch "$exact_repo" "$exact_patch" "$exact_marker" "$exact_hash"
[[ "$(tr -d '\n' <"$exact_marker")" == "$exact_hash" ]] || fail "stale marker was not repaired"
[[ "$(git -C "$exact_repo" show HEAD:file.txt)" == 'base' ]] || fail "exact patch changed the committed tree"

mapfile -t fixture < <(new_multi_repo partial)
partial_repo="${fixture[0]}"
partial_patch="${fixture[1]}"
partial_marker="$partial_repo/.marker"
partial_hash="$(ducklake_patch_hash "$partial_patch")"
git -C "$partial_repo" apply --include file-a.txt "$partial_patch"
if ducklake_ensure_canonical_patch "$partial_repo" "$partial_patch" "$partial_marker" "$partial_hash"; then
    fail "partial patch was accepted"
fi

mapfile -t fixture < <(new_repo drift)
drift_repo="${fixture[0]}"
drift_patch="${fixture[1]}"
drift_marker="$drift_repo/.marker"
drift_hash="$(ducklake_patch_hash "$drift_patch")"
ducklake_ensure_canonical_patch "$drift_repo" "$drift_patch" "$drift_marker" "$drift_hash"
printf 'drifted\n' >"$drift_repo/file.txt"
if ducklake_ensure_canonical_patch "$drift_repo" "$drift_patch" "$drift_marker" "$drift_hash"; then
    fail "drifted patch was accepted"
fi

echo "ducklake patch state tests: ok"
