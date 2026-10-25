#!/usr/bin/env bash
set -euo pipefail

canonical_file() {
    local path="$1"
    local directory
    directory="$(cd "$(dirname "$path")" && pwd -P)" || return 1
    printf '%s/%s\n' "$directory" "$(basename "$path")"
}

sha256_file() {
    local path="$1"
    if command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$path" | awk '{ print $1 }'
    elif command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$path" | awk '{ print $1 }'
    else
        fail "shasum or sha256sum is required to verify artifact identity"
    fi
}

validate_sha256_argument() {
    local label="$1"
    local value="$2"
    [[ -z "$value" ]] && return 0
    [[ "$value" =~ ^[[:xdigit:]]{64}$ ]] ||
        fail "$label must be a 64-character hexadecimal SHA-256 digest"
    printf '%s' "$value" | tr '[:upper:]' '[:lower:]'
}

validate_artifact() {
    local label="$1"
    local path="$2"
    local executable="${3:-0}"

    [[ -f "$path" && ! -L "$path" ]] ||
        fail "$label must be an existing regular non-symlink file: $path"
    if [[ "$executable" == "1" && ! -x "$path" ]]; then
        fail "$label must be executable: $path"
    fi
}

check_expected_sha256() {
    local label="$1"
    local actual="$2"
    local expected="${3:-}"
    [[ -z "$expected" || "$actual" == "$expected" ]] ||
        fail "$label SHA-256 mismatch: expected $expected, got $actual"
}

check_artifact_freshness() {
    local label="$1"
    local artifact="$2"
    shift 2

    local source_path
    local newer_source
    for source_path in "$@"; do
        if [[ -d "$source_path" ]]; then
            newer_source="$(find "$source_path" -type f -newer "$artifact" -print -quit)"
        elif [[ -f "$source_path" ]]; then
            newer_source=""
            if [[ "$source_path" -nt "$artifact" ]]; then
                newer_source="$source_path"
            fi
        else
            continue
        fi
        [[ -z "$newer_source" ]] ||
            fail "stale $label: source is newer than $artifact: $newer_source"
    done
}
