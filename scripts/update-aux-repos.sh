#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" != 2 ]]; then
    echo "usage: scripts/update-aux-repos.sh <analytics-worktree> <release-tag>" >&2
    exit 1
fi

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
analytics_dir="$(cd "$1" && pwd)"
release_tag="$2"
. "$root_dir/scripts/ducklake-upstream-pins.sh"
release_build="v${DUCKLAKE_COMMIT:0:8}"

[[ -f "$analytics_dir/.git" ]] || {
    echo "analytics must be a linked Git worktree: $analytics_dir" >&2
    exit 1
}
[[ "$release_tag" =~ ^v[0-9]+[.][0-9]+[.][0-9]+$ ]] || {
    echo "release tag must have the form v0.1.7" >&2
    exit 1
}

consumer="$analytics_dir/scripts/download-duckdb-bundle.sh"
previous_tag="$(sed -n 's/.*AUX_DUCKLAKE_RELEASE_TAG:=\([^}]*\)}.*/\1/p' "$consumer")"
previous_build="$(sed -n 's/.*AUX_DUCKLAKE_RELEASE_BUILD:=\([^}]*\)}.*/\1/p' "$consumer")"
[[ -n "$previous_tag" && -n "$previous_build" ]] || {
    echo "missing analytics bundle release pins: $consumer" >&2
    exit 1
}

consumers=(
    docker-bake.hcl
    docker/aux-analytics.Dockerfile
    docker/analytics-canary.Dockerfile
    scripts/download-duckdb-bundle.sh
    docker/README.md
)
for consumer in "${consumers[@]}"; do
    if ! grep -Fq "$previous_tag" "$analytics_dir/$consumer" ||
        ! grep -Fq "$previous_build" "$analytics_dir/$consumer"; then
        echo "inconsistent analytics release tag: $consumer" >&2
        exit 1
    fi
done

for consumer in "${consumers[@]}"; do
    AUX_UPDATE_OLD_TAG="$previous_tag" AUX_UPDATE_OLD_BUILD="$previous_build" \
        AUX_UPDATE_TAG="$release_tag" AUX_UPDATE_BUILD="$release_build" \
        perl -0pi -e '
            s/\Q$ENV{AUX_UPDATE_OLD_TAG}\E/$ENV{AUX_UPDATE_TAG}/g;
            s/\Q$ENV{AUX_UPDATE_OLD_BUILD}\E/$ENV{AUX_UPDATE_BUILD}/g;
        ' "$analytics_dir/$consumer"
done

for consumer in "${consumers[@]}"; do
    grep -Fq "$release_tag" "$analytics_dir/$consumer"
    grep -Fq "$release_build" "$analytics_dir/$consumer"
done

echo "analytics_release_tag=$release_tag"
echo "analytics_release_build=$release_build"
echo "analytics_worktree=$analytics_dir"
