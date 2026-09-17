#!/usr/bin/env bash
# report.sh [--since <unix_ms>] [--logs <dir>]: swarm.sh's summary over
# each node's journal. --logs keeps, or reuses, the raw <host>.out files.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

since_ms=
log_dir=
# the window starts at the last start, not the genesis: nodes bind `lead` before it
for f in last-start last-genesis; do
    [ -f "$dist_dir/$f" ] || continue
    since_ms=$(cat "$dist_dir/$f")
    break
done

usage="usage: report.sh [--since <unix_ms>] [--logs <dir>]"
while [ $# -gt 0 ]; do
    case $1 in
        --since) since_ms=${2:?$usage}; shift ;;
        --logs) log_dir=${2:?$usage}; shift ;;
        *) die "$usage" ;;
    esac
    shift
done

logs=${log_dir:-$(mktemp -d)}
mkdir -p "$logs"
[ -n "$log_dir" ] || trap 'rm -rf "$logs"' EXIT

captured=yes
for host in $(hosts); do
    [ -f "$logs/$host.out" ] || captured=no
done

if [ "$captured" = no ]; then
    [ -n "$since_ms" ] || die "no $dist_dir/last-start, pass --since <unix_ms>"
    [[ $since_ms =~ ^[0-9]+$ ]] || die "--since takes unix milliseconds"
fi

fetch_host() {
    rssh_user_systemd "$1" "$(node_log_cmd $((since_ms / 1000)))"
}

count_lines() {
    grep -c -E "$1" || true
}

# the slots a node finalized, one per line, sorted
finalized_slots_of() {
    grep finalized "$logs/$1.log" | grep -o -E 'slot=[0-9]+' | sort || true
}

report_per_node() {
    printf '\n%-8s %-5s %-10s %-14s %-9s %-10s %-9s %s\n' \
        host node finalized all-committed proposed udp-bound warnings errors
    local host log
    for host in $(hosts); do
        log=$(cat "$logs/$host.log")
        printf '%-8s %-5s %-10s %-14s %-9s %-10s %-9s %s\n' \
            "$host" "$(node_id_of "$host")" \
            "$(count_lines 'finalized' <<< "$log")" \
            "$(count_lines 'block="?\++"?( |$)' <<< "$log")" \
            "$(count_lines 'proposing' <<< "$log")" \
            "$(count_lines 'udp bound' <<< "$log")" \
            "$(count_lines ' WARN' <<< "$log")" \
            "$(count_lines ' ERROR|panicked' <<< "$log")"
    done
}

report_agreement() {
    echo
    local first disagreements=0 host
    first=$(hosts | head -1)
    finalized_slots_of "$first" > "$logs/slots-$first.txt"
    for host in $(hosts); do
        [ "$host" != "$first" ] || continue
        finalized_slots_of "$host" > "$logs/slots-$host.txt"
        if ! cmp -s "$logs/slots-$first.txt" "$logs/slots-$host.txt"; then
            echo "$host finalized a different slot set than $first"
            disagreements=$((disagreements + 1))
        fi
    done
    if [ "$disagreements" = 0 ]; then
        echo "all hosts finalized the same $(wc -l < "$logs/slots-$first.txt" | tr -d ' ') slots"
    fi
}

report_warnings() {
    local warnings
    warnings=$(cat "$logs"/*.log | grep ' WARN' || true)
    if [ -n "$warnings" ]; then
        echo
        echo "most frequent warnings:"
        sed -E 's/^[^ ]+ +WARN +//' <<< "$warnings" | cut -c1-100 | sort | uniq -c | sort -rn | head -5
    fi
}

if [ "$captured" = yes ]; then
    echo "reading the logs already in $logs"
else
    echo "collecting logs since $since_ms ($(iso_of_ms "$since_ms")) from $(host_count) hosts"
    fanout_to "$logs" fetch_host
    failed=$(fanout_failures "$logs")
    [ "$failed" = 0 ] || echo "warning: $failed host(s) log could not be read" >&2
fi

for host in $(hosts); do
    printf '%-8s %s lines\n' "$host" "$(wc -l < "$logs/$host.out" | tr -d ' ')"
    sed -E 's/\x1b\[[0-9;]*m//g' "$logs/$host.out" > "$logs/$host.log"
done

report_per_node
report_agreement
report_warnings
[ -z "$log_dir" ] || echo "raw logs in $logs"
