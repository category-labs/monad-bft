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

# `host slot block` per finalized line, deduplicated; a host that logs one
# slot with two different blocks conflicts with itself
finalized_pairs() {
    local host
    for host in $(hosts); do
        sed -nE "s/.*finalized.*slot=([0-9]+).*block=([+-]+).*/$host \1 \2/p" "$logs/$host.log"
    done | sort -u
}

# agreement is per slot: every host that finalized a slot must have the same
# block. Late starts and lag show up as first/last/gaps, not as conflicts.
report_agreement() {
    echo
    finalized_pairs > "$logs/finalized.txt"
    awk -v hosts="$(hosts | tr '\n' ' ')" '
    {
        host = $1; slot = $2 + 0; block = $3
        if (!((host, slot) in seen)) {
            seen[host, slot] = 1
            count[host]++
            if (!(host in first) || slot < first[host]) first[host] = slot
            if (!(host in last) || slot > last[host]) last[host] = slot
        }
        if (!(slot in union)) {
            union[slot] = 1
            n++
            if (n == 1 || slot < lo) lo = slot
            if (n == 1 || slot > hi) hi = slot
        }
        if (!((slot, block) in shape_seen)) {
            shape_seen[slot, block] = 1
            nshapes[slot]++
            shapes[slot, nshapes[slot]] = block
        }
        members[slot, block] = members[slot, block] " " host
    }
    END {
        if (n == 0) {
            print "no finalized slots on any host"
            exit
        }
        conflicts = 0
        for (slot in nshapes) if (nshapes[slot] > 1) conflicts++
        printf "slots: union %d (%d..%d), conflicts %d\n\n", n, lo, hi, conflicts
        printf "%-8s %6s %6s %6s %5s\n", "host", "first", "last", "count", "gaps"
        nh = split(hosts, h, " ")
        for (i = 1; i <= nh; i++) {
            host = h[i]
            if (host in count)
                printf "%-8s %6d %6d %6d %5d\n", host, first[host], last[host], count[host], \
                    last[host] - first[host] + 1 - count[host]
            else
                printf "%-8s %6s %6s %6d %5d\n", host, "-", "-", 0, 0
        }
        if (conflicts == 0) exit
        print ""
        for (slot in nshapes) {
            if (nshapes[slot] < 2) continue
            line = "slot " slot ":"
            for (k = 1; k <= nshapes[slot]; k++)
                line = line "  " substr(members[slot, shapes[slot, k]], 2) "=" shapes[slot, k]
            print line | "sort -k2,2n"
        }
        close("sort -k2,2n")
    }' "$logs/finalized.txt"
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
