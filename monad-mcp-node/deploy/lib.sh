#!/usr/bin/env bash
# sourced by every script in this directory; never run directly.

set -euo pipefail

deploy_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_dir=$(cd "$deploy_dir/../.." && pwd)
dist_dir=$deploy_dir/dist
# config/<host>/ is tracked and mirrors ~/monad-mcp/config/ on that host
config_dir=$deploy_dir/config

port=${MCP_PORT:-8002}
supervisor=${MCP_SUPERVISOR:-systemd}
keep_binaries=${MCP_KEEP_BINARIES:-1}
unit=monad-mcp-node
rpc_unit=monad-mcp-rpc
explorer_unit=monad-mcp-explorer
cruft_unit=monad-mcp-cruft
remote_rel=monad-mcp
remote_root='$HOME/monad-mcp'
ssh_user=monad
ssh_port=9022
ssh_domain=devcore4.com
# the rtt matrix latency.sh writes; shipped next to node.toml when present
latency_file=$config_dir/latency.toml

die() {
    echo "error: $*" >&2
    exit 1
}

hosts() {
    grep -v -e '^[[:space:]]*#' -e '^[[:space:]]*$' "$deploy_dir/hosts.txt"
}

host_count() {
    hosts | wc -l | tr -d ' '
}

# the hosts fanout acts on: all of hosts.txt unless set_targets narrowed it.
# always in hosts.txt order, whatever order the subset was given in.
target_list=
targets() {
    if [ -z "$target_list" ]; then
        hosts
        return
    fi
    local h
    for h in $(hosts); do
        case " $target_list " in *" $h "*) echo "$h" ;; esac
    done
}

# set_targets host... ; also accepts comma-separated lists
set_targets() {
    local h
    target_list=
    for h in $(tr ',' ' ' <<< "$*"); do
        node_id_of "$h" > /dev/null
        target_list+=" $h"
    done
    [ -n "$target_list" ] || die "empty host list"
}

fqdn() {
    echo "$1.$ssh_domain"
}

node_id_of() {
    local i=0 h
    while read -r h; do
        if [ "$h" = "$1" ]; then
            echo "$i"
            return 0
        fi
        i=$((i + 1))
    done < <(hosts)
    die "unknown host: $1 (not in $deploy_dir/hosts.txt)"
}

# ewr-*/lax-* are absent from ~/.ssh/config's short-name pattern, so
# every connection goes to the fqdn with user and port spelled out.
ipv4_of() {
    local ip
    ip=$(getent ahostsv4 "$(fqdn "$1")" | awk '{print $1}' | sort -u | head -1)
    [ -n "$ip" ] || die "no IPv4 address for $(fqdn "$1")"
    echo "$ip"
}

rssh() {
    local host=$1
    shift
    ssh -o BatchMode=yes -o ConnectTimeout=10 -p "$ssh_port" -l "$ssh_user" \
        "$(fqdn "$host")" "$@"
}

# systemctl --user over a non-interactive ssh needs the session bus
remote_env() {
    printf 'export XDG_RUNTIME_DIR=/run/user/2000 DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/2000/bus; '
}

rssh_user_systemd() {
    local host=$1
    shift
    rssh "$host" "$(remote_env)$*"
}

rsync_ssh="ssh -o BatchMode=yes -o ConnectTimeout=2 -p $ssh_port -l $ssh_user"

rsync_to() {
    local host=$1 src=$2 dest=$3
    rsync -az -e "$rsync_ssh" "$src" "$(fqdn "$host"):$dest"
}

# one connection for a whole tree: paths under dir land at the same
# paths under the remote $HOME, directories created as needed.
rsync_tree_to() {
    local host=$1 dir=$2
    shift 2
    (cd "$dir" && rsync -azR -e "$rsync_ssh" "$@" "$(fqdn "$host"):")
}

# uutils date ignores %3N and prints nanoseconds, so ask python3
now_ms() {
    python3 -c 'import time; print(int(time.time() * 1000))'
}

# runs fn per host in parallel into dir/<host>.{out,rc}; always returns 0,
# since a `||` context would disable `set -e` in the per-host subshells too.
fanout_to() {
    local dir=$1 fn=$2
    shift 2
    mkdir -p "$dir"
    local host pids=() names=()
    for host in $(targets); do
        ("$fn" "$host" "$@") > "$dir/$host.out" 2>&1 &
        pids+=($!)
        names+=("$host")
    done
    local i hrc
    for i in "${!pids[@]}"; do
        hrc=0
        wait "${pids[$i]}" || hrc=$?
        echo "$hrc" > "$dir/${names[$i]}.rc"
    done
}

fanout_failures() {
    local dir=$1 host n=0
    for host in $(targets); do
        [ "$(cat "$dir/$host.rc" 2> /dev/null || echo 1)" = 0 ] || n=$((n + 1))
    done
    echo "$n"
}

# prints each host's output and fails if any host failed. Call it as a
# plain statement, never in a `||` list: see fanout_to.
fanout() {
    local dir failed host
    dir=$(mktemp -d)
    fanout_to "$dir" "$@"
    for host in $(targets); do
        printf '=== %-8s rc=%s\n' "$host" "$(cat "$dir/$host.rc")"
        sed 's/^/    /' "$dir/$host.out"
    done
    failed=$(fanout_failures "$dir")
    rm -rf "$dir"
    if [ "$failed" != 0 ]; then
        echo "$failed host(s) failed" >&2
        return 1
    fi
}

# remote command pointing bin/$2 (default current) at binary $1, already in
# bin/. atomic, and harmless to a running process, which keeps the old inode.
swap_cmd() {
    local link=${2:-current}
    echo "ln -sfn $1 $remote_root/bin/.$link.new
        mv -T $remote_root/bin/.$link.new $remote_root/bin/$link
        echo \"$link -> \$(readlink $remote_root/bin/$link)\""
}

# the binary names build.sh recorded, as `node rpc explorer`
dist_binaries() {
    [ -f "$dist_dir/VERSION" ] || die "no $dist_dir/VERSION; run build.sh first"
    local node rpc explorer
    node=$(sed -n 's/^binary=//p' "$dist_dir/VERSION")
    rpc=$(sed -n 's/^rpc_binary=//p' "$dist_dir/VERSION")
    explorer=$(sed -n 's/^explorer_binary=//p' "$dist_dir/VERSION")
    [ -n "$node" ] && [ -f "$dist_dir/$node" ] || die "no node binary in $dist_dir/VERSION; run build.sh"
    [ -n "$rpc" ] && [ -f "$dist_dir/$rpc" ] || die "no rpc binary in $dist_dir/VERSION; run build.sh"
    [ -n "$explorer" ] && [ -f "$dist_dir/$explorer" ] || die "no explorer binary in $dist_dir/VERSION; run build.sh"
    echo "$node $rpc $explorer"
}

# have_latency [ip...]: whether $latency_file exists; dies unless it matches hosts.txt and
# the validator ips given, or else those of every config/<host>/node.toml
have_latency() {
    [ -f "$latency_file" ] || return 1
    local h configs=()
    if [ $# = 0 ]; then
        for h in $(hosts); do configs+=("$(host_config "$h")"); done
    fi
    python3 "$deploy_dir/check-latency.py" "$latency_file" --hosts $(hosts) \
        ${1:+--ips "$@"} ${configs[0]:+--configs "${configs[@]}"} \
        || die "$latency_file is stale or malformed; re-run latency.sh"
}

# a host runs the explorer iff config/<host>/explorer.env exists
has_explorer() {
    [ -f "$config_dir/$1/explorer.env" ]
}

# `key = <integer>` from a host's live node.toml
remote_config_int() {
    local host=$1 key=$2 v
    v=$(rssh "$host" "grep -oE '^$key *= *[0-9]+' $remote_root/config/node.toml" | grep -oE '[0-9]+$') \
        || die "cannot read $key from $host:~/$remote_rel/config/node.toml"
    echo "$v"
}

host_config() {
    echo "$config_dir/$1/node.toml"
}

# the hosts in hosts.txt with no config/<host>/node.toml
missing_configs() {
    local h
    for h in $(hosts); do
        [ -f "$(host_config "$h")" ] || echo "$h"
    done
}

# top-level `key = <integer>` from a host's local config/<host>/node.toml;
# the first match, since [[validators]] repeat node_id
local_config_int() {
    local host=$1 key=$2 v
    v=$(grep -m1 -oE "^$key *= *[0-9]+" "$(host_config "$host")" | grep -oE '[0-9]+$') \
        || die "cannot read $key from $(host_config "$host")"
    echo "$v"
}

iso_of_ms() {
    date -u -d "@$(($1 / 1000))" +%Y-%m-%dT%H:%M:%SZ
}

# remote command printing the node log, since $1 (unix seconds) if given.
# the setsid fallback logs to a file and cannot filter by time.
node_log_cmd() {
    if [ "$supervisor" != systemd ]; then
        echo "cat $remote_root/logs/node.log"
    elif [ -n "${1:-}" ]; then
        echo "journalctl --user -u $unit -o cat --since @$1"
    else
        echo "journalctl --user -u $unit -o cat"
    fi
}
