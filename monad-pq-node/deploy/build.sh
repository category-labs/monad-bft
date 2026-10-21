#!/usr/bin/env bash
# build.sh [--dirty] -> dist/{monad-pq-node,monad-mcp-rpc,monad-mcp-explorer,mcp-tx}-<sha>[-dirty]
# + dist/VERSION. The binaries have no --version flag, so that name is the version.
# rpc, explorer and mcp-tx are the unchanged monad-mcp-rpc/monad-mcp-explorer builds.
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

# every netctl.sh start rewrites genesis_deadline in the tracked host configs
dirt=$(git status --porcelain -- 'monad-pq-*' 'monad-mcp-*' \
    ':!monad-pq-node/deploy/config' ':!monad-mcp-node/deploy/config')
suffix=
if [ -n "$dirt" ]; then
    if [ "$allow_dirty" = no ]; then
        echo "$dirt" >&2
        die "monad-pq-* or monad-mcp-* is dirty; commit first or pass --dirty"
    fi
    suffix=-dirty
fi

sha=$(git rev-parse --short HEAD)
name=monad-pq-node-$sha$suffix
rpc_name=monad-mcp-rpc-$sha$suffix
explorer_name=monad-mcp-explorer-$sha$suffix
tx_name=mcp-tx-$sha$suffix

cargo build --release -p monad-pq-node -p monad-mcp-rpc -p monad-mcp-explorer \
    --bin monad-pq-node --bin monad-mcp-rpc --bin mcp-tx --bin monad-mcp-explorer

target=$(cargo metadata --format-version 1 --no-deps \
    | python3 -c 'import json, sys; print(json.load(sys.stdin)["target_directory"])')
mkdir -p "$dist_dir"
install -m 755 "$target/release/monad-pq-node" "$dist_dir/$name"
install -m 755 "$target/release/monad-mcp-rpc" "$dist_dir/$rpc_name"
install -m 755 "$target/release/monad-mcp-explorer" "$dist_dir/$explorer_name"
install -m 755 "$target/release/mcp-tx" "$dist_dir/$tx_name"
# keep the newest $keep_binaries of each; anything older is a rebuild away
for kind in monad-pq-node monad-mcp-rpc monad-mcp-explorer mcp-tx; do
    ls -t "$dist_dir/$kind"-* | tail -n +$((keep_binaries + 1)) | xargs -r rm -v
done

{
    echo "binary=$name"
    echo "rpc_binary=$rpc_name"
    echo "explorer_binary=$explorer_name"
    echo "tx_binary=$tx_name"
    echo "sha=$(git rev-parse HEAD)$suffix"
    echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "rustc_host=$(rustc -vV | sed -n 's/^host: //p')"
    echo "glibc=$(ldd --version | head -1 | awk '{print $NF}')"
} > "$dist_dir/VERSION"

cat "$dist_dir/VERSION"
for f in "$name" "$rpc_name" "$explorer_name" "$tx_name"; do
    echo "built $dist_dir/$f ($(du -h "$dist_dir/$f" | cut -f1))"
done
