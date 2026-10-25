#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[[ "$#" == 1 ]] || {
    echo "usage: $0 <bundle-root>" >&2
    exit 1
}
cluster_dir="$(mktemp -d)"
server_pid=""
mkdir -p "$cluster_dir/data" "$cluster_dir/logs"
cleanup() {
    if [[ -n "$server_pid" ]]; then
        kill "$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
    fi
    rm -rf "$cluster_dir"
}
trap cleanup EXIT
printf 'release:release@127.0.0.1:4500\n' >"$cluster_dir/fdb.cluster"
fdbserver -p 127.0.0.1:4500 -C "$cluster_dir/fdb.cluster" \
    -d "$cluster_dir/data" -L "$cluster_dir/logs" >"$cluster_dir/server.log" 2>&1 &
server_pid="$!"
is_ready=0
for ((attempt = 0; attempt < 6; attempt++)); do
    if ! kill -0 "$server_pid" 2>/dev/null; then
        break
    fi
    # Configuration can commit before the CLI receives its acknowledgement.
    if fdbcli -C "$cluster_dir/fdb.cluster" --timeout 10 --exec 'configure new single ssd' ||
        fdbcli -C "$cluster_dir/fdb.cluster" --timeout 10 --exec 'get release-readiness'; then
        is_ready=1
        break
    fi
done
if [[ "$is_ready" != 1 ]]; then
    cat "$cluster_dir/server.log" >&2
    echo 'temporary FoundationDB server did not become ready' >&2
    exit 1
fi
bundle_root="$(cd "$1" && pwd)"
LD_LIBRARY_PATH="$bundle_root/opt/duckdb:$bundle_root/opt/aux-ducklake/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
    AUX_DUCKLAKE_FDB_CLUSTER_FILE="$cluster_dir/fdb.cluster" \
    python3 "$root_dir/scripts/ducklake-release-smoke.py" "$1"
