#!/usr/bin/env bash
# report.sh [--source host] [--since 'YYYY-MM-DD HH:MM:SS UTC'] [--slots n] [--logs <dir>]
# per-host unit state, chain tip, lag and clock offset, then finalization
# latency by path over the source host's latest --slots finalized slots.
# genesis and slot interval come from the source host's live node.toml, so
# another checkout's deploy cannot make them stale. --logs also pulls every
# host's journal over the window into <dir> (or reuses it) for the per-host
# counts and the cross-host per-slot agreement check.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

usage="usage: report.sh [--source host] [--since 'YYYY-MM-DD HH:MM:SS UTC'] [--slots n] [--logs <dir>]"
src=
since=
slots=10000
log_dir=
while [ $# -gt 0 ]; do
    case $1 in
        --source) src=${2:?$usage}; shift ;;
        --since) since=${2:?$usage}; shift ;;
        --slots) slots=${2:?$usage}; shift ;;
        --logs) log_dir=${2:?$usage}; shift ;;
        *) die "$usage" ;;
    esac
    shift
done
[ "$supervisor" = systemd ] || die "report.sh reads the journal; MCP_SUPERVISOR=$supervisor is not supported"
if [ -z "$src" ]; then
    src=$(hosts | grep -x ewr-002 || hosts | head -1)
fi
node_id_of "$src" > /dev/null

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# clk= node clock minus system clock: max(at - journal timestamp) over the
# last 50 finalizations, + = node ahead. now= is read after the tip.
state_prog='
    /finalized slot=/ {
        tip = $0
        match($0, /at=[0-9]+/); off[++k] = substr($0, RSTART + 3, RLENGTH - 3) / 1e6 - $1 * 1000
    }
    /fallback entry blocked/ {
        match($0, /slot=Slot\([0-9]+/); blocked[substr($0, RSTART, RLENGTH)] = 1
        match($0, /waits=[0-9]+/); w = substr($0, RSTART + 6, RLENGTH - 6) + 0; if (w > mw) mw = w
    }
    END {
        n = 0; for (s in blocked) n++
        print "tip=" tip
        print "blocked=" n "/" mw + 0
        m = "-"; for (i = (k > 50 ? k - 49 : 1); i <= k; i++) if (m == "-" || off[i] > m) m = off[i]
        print "clk=" (m == "-" ? m : sprintf("%+.1fms", m))
    }'

state_host() {
    rssh_user_systemd "$1" "
        echo state=\$(systemctl --user is-active $unit.service) \
            since=\$(systemctl --user show $unit.service -p ActiveEnterTimestamp --value | tr ' ' '_')
        journalctl --user -u $unit -o short-unix --since '1 min ago' | sed 's/\x1b\[[0-9;]*m//g' | awk '$state_prog'
        echo now=\$((\$(date +%s%N) / 1000000))"
}

field() {
    sed -n "s/^$2=//p" "$work/$1.out"
}

fanout_to "$work" state_host
[ "$(cat "$work/$src.rc")" = 0 ] || die "source $src unreachable: $(tail -1 "$work/$src.out")"

genesis=$(remote_config_int "$src" genesis_deadline)
interval=$(remote_config_int "$src" slot_interval)
src_tip=$(field "$src" tip | sed -n 's/.*slot=\([0-9]*\).*/\1/p')

# default window: deadline of slot (tip - slots), 30 s early for late
# finalizations, clamped to the unit start
if [ -z "$since" ]; then
    unit_start=$(date -u -d "$(field "$src" state | sed -n 's/.*since=//p' | tr '_' ' ')" +%s)
    if [ -n "$src_tip" ]; then
        w=$(((genesis + (src_tip - slots) * interval) / 1000 - 30))
    else
        w=$(($(date +%s) - slots * interval / 1000 - 30))
    fi
    since=$(date -u -d @$((unit_start > w ? unit_start : w)) +'%F %T UTC')
fi

# newest first; awk stops after `slots` finalized lines so journalctl never scans more than it must
rssh_user_systemd "$src" "journalctl --user -u $unit -o short-unix -r --since '$since' \
    | grep -F -e finalized -e 'mvba decided' -e 'entering fallback' -e 'entry blocked' \
    | sed 's/\x1b\[[0-9;]*m//g' | awk '/finalized slot=/ { if (++n > $slots) exit } { print }'" \
    > "$work/fin" || [ -s "$work/fin" ] || die "no finalized lines on $src since $since"

echo "genesis_deadline=$genesis ($(iso_of_ms "$genesis"))  slot_interval=${interval}ms  source=$src  since=$since  now=$(date -u +%T)"
printf '%-8s %-11s %-10s %-8s %-9s %-10s %s\n' host state tip_slot tip_age lag_slots node_clk 'blocked_1m(slots/max_waits)'
up=
for host in $(hosts); do
    if [ "$(cat "$work/$host.rc")" = 0 ]; then
        state=$(field "$host" state | cut -d' ' -f1)
    else
        state=unreachable
    fi
    [ "$state" != active ] || up+=" $host"
    tip=$(field "$host" tip | sed -n 's/.*slot=\([0-9]*\).* at=\([0-9]*\).*/\1 \2/p')
    now=$(field "$host" now)
    slot=- age=- lag=-
    if [ -n "$tip" ] && [ -n "$now" ]; then
        read -r slot at <<< "$tip"
        age="$((now - at / 1000000))ms"
        lag=$(((now - genesis) / interval - slot))
    fi
    clk=$(field "$host" clk)
    printf '%-8s %-11s %-10s %-8s %-9s %-10s %s\n' "$host" "$state" "$slot" "$age" "$lag" \
        "${clk:--}" "$(field "$host" blocked)"
done
echo "clock slot on $src: $((($(field "$src" now) - genesis) / interval))"
echo

python3 - "$work/fin" "$genesis" "$interval" "$(hosts | xargs)" "$up" << 'EOF'
import sys, re, collections
G = int(sys.argv[2]); IV = int(sys.argv[3]); HOSTS = sys.argv[4].split()
UP = {HOSTS.index(h) for h in sys.argv[5].split()}
fin = {}; dec = {}; ent = {}; late = collections.Counter(); blocked = 0
for l in open(sys.argv[1]):
    t = float(l.split()[0]) * 1000
    if m := re.search(r'finalized slot=(\d+) block=([-+]+) at=(\d+)', l): fin[int(m[1])] = (int(m[3]) / 1e6, m[2])
    elif m := re.search(r'mvba decided slot=Slot\((\d+)\) view=FallbackView\((\d+)\)', l): dec[int(m[1])] = int(m[2])
    elif m := re.search(r'entering fallback mvba slot=Slot\((\d+)\)', l): ent[int(m[1])] = t
    elif 'waits=1 ' in l and (m := re.search(r'enter_fallback_voters=\[([^]]*)\]', l)):
        blocked += 1; have = {int(x) for x in re.findall(r'\d+', m[1])}
        late.update(UP - have)
if not fin: sys.exit("no finalized lines")
n = len(fin); lat = lambda s: fin[s][0] - (G + s * IV)
def pct(x, p): x = sorted(x); return f"{x[min(len(x) - 1, int(p / 100 * len(x)))]:.0f}" if x else "-"
groups = collections.defaultdict(list)
for s in fin: groups["fast path" if s not in dec else f"MVBA view {dec[s]}" if dec[s] < 4 else "MVBA view 4+"].append(s)
v = [lat(s) for s in fin]
print(f"finalization latency by path, ms after slot deadline (n={n}, mean={sum(v) / n:.0f})")
print("| path | slots | share | MVBA entry p50 | p50 | p90 | p99 |\n|---|---|---|---|---|---|---|")
for k in ["fast path", "MVBA view 1", "MVBA view 2", "MVBA view 3", "MVBA view 4+"]:
    ss = groups.get(k, []); x = [lat(s) for s in ss]; e = [ent[s] - (G + s * IV) for s in ss if s in ent]
    print(f"| {k} | {len(ss)} | {len(ss) / n * 100:.1f}% | {pct(e, 50) if k != 'fast path' else '-'} | {pct(x, 50)} | {pct(x, 90)} | {pct(x, 99)} |")
print(f"| all | {n} | 100.0% | - | {pct(v, 50)} | {pct(v, 90)} | {pct(v, 99)} |")
neg = sum('+' not in b for _, b in fin.values())
print(f"all-negative blocks: {neg}/{n} ({neg / n * 100:.0f}%)")
# a missing voter at the first entry check (D+2Δ) pushes every MVBA entry out by one Δ
print(f"fallback entry blocked at D+2Δ: {blocked}/{len(dec)} MVBA slots; missing voter: "
      + (", ".join(f"{HOSTS[i]}={c}" for i, c in late.most_common(3)) or "-"))
EOF

[ -n "$log_dir" ] || exit 0

logs=$log_dir
mkdir -p "$logs"
captured=yes
for host in $(hosts); do
    [ -f "$logs/$host.out" ] || captured=no
done

fetch_host() {
    rssh_user_systemd "$1" "$(node_log_cmd "$(date -u -d "$since" +%s)")"
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

echo
if [ "$captured" = yes ]; then
    echo "reading the logs already in $logs"
else
    echo "collecting logs since $since from $(host_count) hosts into $logs"
    fanout_to "$logs" fetch_host
    failed=$(fanout_failures "$logs")
    [ "$failed" = 0 ] || echo "warning: $failed host(s) log could not be read" >&2
fi

for host in $(hosts); do
    sed -E 's/\x1b\[[0-9;]*m//g' "$logs/$host.out" > "$logs/$host.log"
done

report_per_node
report_agreement
report_warnings
