#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
workspace_dir="$(cd "$script_dir/../.." && pwd)"

previous_release_tag="v0.1.1"
release_tag="v0.1.2"
release_build="v2856687c"

require_literal() {
    local relative_path="$1"
    local value="$2"
    local path="$workspace_dir/$relative_path"

    if [[ ! -f "$path" ]]; then
        echo "Missing release consumer: $path" >&2
        return 1
    fi
    if ! grep -Fq "$value" "$path"; then
        echo "Expected release pin not found in $path: $value" >&2
        return 1
    fi
}

replace_literal() {
    local relative_path="$1"
    local old_value="$2"
    local new_value="$3"
    local path="$workspace_dir/$relative_path"

    if [[ -f "$path" ]] && grep -Fq "$old_value" "$path"; then
        AUX_UPDATE_OLD="$old_value" AUX_UPDATE_NEW="$new_value" perl -0pi -e '
            BEGIN {
                $old = $ENV{"AUX_UPDATE_OLD"};
                $new = $ENV{"AUX_UPDATE_NEW"};
            }
            s/\Q$old\E/$new/g;
        ' "$path"
        echo "Updated $relative_path"
    fi
    require_literal "$relative_path" "$new_value"
}

for relative_path in \
    aux-fn/docker-bake.hcl \
    aux-fn/docker/Dockerfile \
    aux-fn/docker/aux-fn-compose.Dockerfile \
    aux-fn/docker/test.Dockerfile \
    aux-fn/docker/scripts/download_duckdb_bundle.sh \
    aux-analytics/docker-bake.hcl \
    aux-analytics/docker/aux-analytics.Dockerfile \
    aux-analytics/docker/analytics-canary.Dockerfile \
    aux-analytics/docker/scripts/download_duckdb_bundle.sh; do
    replace_literal "$relative_path" "$previous_release_tag" "$release_tag"
    require_literal "$relative_path" "$release_build"
done

replace_literal \
    aux-analytics/docker/README.md \
    "release \`$previous_release_tag\`" \
    "release \`$release_tag\`"

replace_literal \
    aux-infra/crates/infra-ops/src/github_runners.rs \
    "AUX_DUCKLAKE_RELEASE_TAG: &str = \"$previous_release_tag\"" \
    "AUX_DUCKLAKE_RELEASE_TAG: &str = \"$release_tag\""
require_literal \
    aux-infra/crates/infra-ops/src/github_runners.rs \
    "AUX_DUCKLAKE_RELEASE_BUILD: &str = \"$release_build\""
replace_literal \
    aux-infra/crates/infra-ops/src/github_runners_tests.rs \
    "AUX_DUCKLAKE_RELEASE_TAG=$previous_release_tag" \
    "AUX_DUCKLAKE_RELEASE_TAG=$release_tag"
require_literal \
    aux-infra/crates/infra-ops/src/github_runners_tests.rs \
    "AUX_DUCKLAKE_RELEASE_BUILD=$release_build"
