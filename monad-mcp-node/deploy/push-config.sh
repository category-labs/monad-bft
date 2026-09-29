#!/usr/bin/env bash
# push-config.sh [host...]: config/<host>/ -> ~/monad-mcp/config/, printing the
# node.toml diff and keeping a changed node.toml as node.toml.<unix ts>~.
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
    # top-level keys only: they end at the first [[validators]]
    rssh "$host" "awk '/^\[/ { exit } /^(node_id|genesis_deadline) /' $remote_root/config/node.toml | tr '\n' ' '; echo"
}

[ $# = 0 ] || set_targets "$@"
for host in $(targets); do
    [ -f "$(host_config "$host")" ] || die "no $(host_config "$host"); run gen-config.sh first"
    # a node.toml copied between host dirs would run two nodes as one id
    [ "$(local_config_int "$host" node_id)" = "$(node_id_of "$host")" ] \
        || die "$(host_config "$host") has node_id $(local_config_int "$host" node_id), $host is $(node_id_of "$host")"
done

echo "pushing $config_dir/<host>/ to $(targets | xargs)"
fanout push_host
