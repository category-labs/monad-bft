#!/usr/bin/env bash
# push-config.sh: dist/config/<host>.toml -> ~/monad-mcp/config/node.toml,
# keeping the previous one as node.toml.<unix ts>~.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

ts=$(date +%s)

push_host() {
    local host=$1 src=$config_dir/$host.toml
    [ -f "$src" ] || die "no $src; run gen-config.sh first"
    rssh "$host" "set -e
        mkdir -p $remote_root/config
        if [ -f $remote_root/config/node.toml ]; then
            cp -p $remote_root/config/node.toml $remote_root/config/node.toml.$ts~
        fi"
    rsync_to "$host" "$src" "$remote_rel/config/node.toml"
    rssh "$host" "grep -E '^(node_id|genesis_deadline) ' $remote_root/config/node.toml | tr '\n' ' '; echo"
}

for host in $(hosts); do
    [ -f "$config_dir/$host.toml" ] || die "no $config_dir/$host.toml; run gen-config.sh first"
done

echo "pushing $config_dir/<host>.toml to $(host_count) hosts"
fanout push_host
