#!/usr/bin/env bash
# push-config.sh [host...]: config/<host>/ -> ~/monad-mcp/config/, printing the
# node.toml diff and keeping a changed node.toml as node.toml.<unix ts>~.
# Ships $MCP_LATENCY (default ~/tmp/mcp-latency/latency.toml) as config/latency.toml.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

ts=$(date +%s)

push_host() {
    local host=$1 src
    src=$(host_config "$host")
    local remote
    remote=$(rssh "$host" "cat $remote_root/config/node.toml 2> /dev/null || true")
    if [ -z "$remote" ]; then
        echo "node.toml: new"
    elif diff <(echo "$remote") "$src" > /dev/null; then
        echo "node.toml: unchanged"
    else
        echo "node.toml: remote -> local"
        diff <(echo "$remote") "$src" | sed 's/^/    /' || true
        rssh "$host" "cp -p $remote_root/config/node.toml $remote_root/config/node.toml.$ts~"
    fi
    rssh "$host" "mkdir -p $remote_root/config"
    rsync_to "$host" "$config_dir/$host/" "$remote_rel/config/"
    [ "$latency" = no ] || rsync_to "$host" "$latency_file" "$remote_rel/config/latency.toml"
    # top-level keys only: they end at the first [[validators]]
    rssh "$host" "awk '/^\[/ { exit } /^(node_id|genesis_deadline) /' $remote_root/config/node.toml | tr '\n' ' '; echo"
}

latency=no
if have_latency; then
    latency=yes
fi

[ $# = 0 ] || set_targets "$@"
for host in $(targets); do
    [ -f "$(host_config "$host")" ] || die "no $(host_config "$host"); run gen-config.sh first"
    # a node.toml copied between host dirs would run two nodes as one id
    [ "$(local_config_int "$host" node_id)" = "$(node_id_of "$host")" ] \
        || die "$(host_config "$host") has node_id $(local_config_int "$host" node_id), $host is $(node_id_of "$host")"
    [ "$latency" = yes ] || ! grep -q '^latency *=' "$(host_config "$host")" \
        || die "$(host_config "$host") names a latency file but there is no $latency_file; set MCP_LATENCY"
done

echo "pushing $config_dir/<host>/ to $(targets | xargs)"
if [ "$latency" = yes ]; then
    echo "with $latency_file as config/latency.toml"
else
    echo "no $latency_file: latency.toml not shipped"
fi
fanout push_host
