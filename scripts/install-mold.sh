#!/usr/bin/env bash
# Canonical source: aux-infra/scripts/install-mold.sh; copied for standalone builds.
set -euo pipefail

[[ "$(uname -s)" == Linux ]] || exit 0
prefix=/opt/aux-mold-3.0.0
case "$(uname -m)" in
x86_64)
    archive_arch=x86_64
    checksum=6c90d4a474c7c0409dfb575be03a5345878ac14fdba18de8b40fa58c60121189
    ;;
aarch64 | arm64)
    archive_arch=aarch64
    checksum=52c759d3689babaea4c42af2f31062a74ab83e17ffb5a8090232534c67b70577
    ;;
*)
    printf 'Unsupported mold build architecture: %s\n' "$(uname -m)" >&2
    exit 1
    ;;
esac
if [[ -x "$prefix/bin/mold-cc" && -x "$prefix/bin/mold-link" && -x "$prefix/libexec/mold/ld.bfd" ]] &&
    [[ "$("$prefix/bin/mold" --version)" == 'mold 3.0.0 '* ]]; then
    exit 0
fi

work_dir=$(mktemp -d)
trap 'rm -rf "$work_dir"' EXIT
archive="mold-3.0.0-$archive_arch-linux.tar.gz"
curl --fail --location --retry 3 --output "$work_dir/$archive" \
    "https://github.com/rui314/mold/releases/download/v3.0.0/$archive"
printf '%s  %s\n' "$checksum" "$work_dir/$archive" | sha256sum --check
mkdir "$work_dir/mold"
tar -xzf "$work_dir/$archive" -C "$work_dir/mold" --strip-components=1
"$work_dir/mold/bin/mold" --version | grep -q '^mold 3\.0\.0 '
# GCC 11 in UBI9 accepts the bfd selector; these aliases resolve it to mold.
mkdir -p "$work_dir/mold/libexec/mold"
for linker in ld ld.bfd aarch64-linux-gnu-ld aarch64-linux-gnu-ld.bfd x86_64-linux-gnu-ld x86_64-linux-gnu-ld.bfd; do
    ln -sfn ../../bin/mold "$work_dir/mold/libexec/mold/$linker"
done
cat >"$work_dir/mold/bin/mold-cc" <<'LINKER'
#!/usr/bin/env bash
set -euo pipefail
test -x /opt/aux-mold-3.0.0/libexec/mold/ld
exec cc "$@" -B/opt/aux-mold-3.0.0/libexec/mold -fuse-ld=bfd
LINKER
cat >"$work_dir/mold/bin/mold-link" <<'LINKER'
#!/usr/bin/env bash
set -euo pipefail
test -x /opt/aux-mold-3.0.0/libexec/mold/ld
compiler=$1
shift
exec "$compiler" "$@" -B/opt/aux-mold-3.0.0/libexec/mold -fuse-ld=bfd
LINKER
chmod 755 "$work_dir/mold/bin/mold-cc" "$work_dir/mold/bin/mold-link"
elevate=()
if [[ "$EUID" != 0 ]]; then elevate=(sudo); fi
"${elevate[@]}" mkdir -p "$prefix"
"${elevate[@]}" cp -a "$work_dir/mold/." "$prefix/"
"${elevate[@]}" chown -R root:root "$prefix"
"$prefix/bin/mold" --version
