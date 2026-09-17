#!/usr/bin/env bash
# sourced by every script in this directory; never run directly.

set -euo pipefail

deploy_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_dir=$(cd "$deploy_dir/../.." && pwd)
dist_dir=$deploy_dir/dist
config_dir=$dist_dir/config

port=${MCP_PORT:-8002}
supervisor=${MCP_SUPERVISOR:-systemd}
unit=monad-mcp-node
cruft_unit=monad-mcp-cruft
remote_rel=monad-mcp
remote_root='$HOME/monad-mcp'
ssh_user=monad
ssh_port=9022
ssh_domain=devcore4.com

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

rsync_to() {
    local host=$1 src=$2 dest=$3
    rsync -az -e "ssh -o BatchMode=yes -o ConnectTimeout=10 -p $ssh_port -l $ssh_user" \
        "$src" "$(fqdn "$host"):$dest"
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
    for host in $(hosts); do
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
    for host in $(hosts); do
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
    for host in $(hosts); do
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
