#!/usr/bin/env bash
# build.sh [--dirty] -> dist/monad-mcp-node-<sha>[-dirty] + dist/VERSION.
# The binary has no --version flag, so that name is the version.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

allow_dirty=no
while [ $# -gt 0 ]; do
    case $1 in
        --dirty) allow_dirty=yes ;;
        *) die "usage: build.sh [--dirty]" ;;
    esac
    shift
done

cd "$repo_dir"

dirt=$(git status --porcelain -- 'monad-mcp-*')
suffix=
if [ -n "$dirt" ]; then
    if [ "$allow_dirty" = no ]; then
        echo "$dirt" >&2
        die "monad-mcp-* is dirty; commit first or pass --dirty"
    fi
    suffix=-dirty
fi

sha=$(git rev-parse --short HEAD)
name=monad-mcp-node-$sha$suffix

cargo build --release -p monad-mcp-node

mkdir -p "$dist_dir"
install -m 755 "$repo_dir/target/release/monad-mcp-node" "$dist_dir/$name"
# keep the newest $keep_binaries; anything older is a rebuild away
ls -t "$dist_dir"/monad-mcp-node-* | tail -n +$((keep_binaries + 1)) | xargs -r rm -v

{
    echo "binary=$name"
    echo "sha=$(git rev-parse HEAD)$suffix"
    echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "rustc_host=$(rustc -vV | sed -n 's/^host: //p')"
    echo "glibc=$(ldd --version | head -1 | awk '{print $NF}')"
} > "$dist_dir/VERSION"

cat "$dist_dir/VERSION"
echo "built $dist_dir/$name ($(du -h "$dist_dir/$name" | cut -f1))"
