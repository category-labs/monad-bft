#!/usr/bin/env bash
# report.sh [--source host] [--since 'YYYY-MM-DD HH:MM:SS UTC'] [--slots n] [--logs <dir>]
#           [--tx n [--tx-interval ms] [--tx-host host]]
# per-host unit state, chain tip, lag, clock offset and last-minute fast/coda/
# halted counts, then finalization and halt latency by path (fast, coda view,
# gadget) over the source host's latest --slots finalized slots. genesis and
# slot interval come from the source host's live node.toml, so another
# checkout's deploy cannot make them stale. --logs also pulls every host's
# journal over the window into <dir> (or reuses it) for the per-host counts and
# the cross-host per-slot agreement check. --tx sends n txs through the pq rpc
# on --tx-host (default: the first explorer host) with mcp-tx and reports
# submit -> finalized latency; it is the only part that changes the chain.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

usage="usage: report.sh [--source host] [--since 'YYYY-MM-DD HH:MM:SS UTC'] [--slots n] [--logs <dir>] [--tx n [--tx-interval ms] [--tx-host host]]"
src=
since=
slots=10000
log_dir=
tx_count=0
tx_interval=50
tx_host=
while [ $# -gt 0 ]; do
    case $1 in
        --source) src=${2:?$usage}; shift ;;
        --since) since=${2:?$usage}; shift ;;
        --slots) slots=${2:?$usage}; shift ;;
        --logs) log_dir=${2:?$usage}; shift ;;
        --tx) tx_count=${2:?$usage}; shift ;;
        --tx-interval) tx_interval=${2:?$usage}; shift ;;
        --tx-host) tx_host=${2:?$usage}; shift ;;
        *) die "$usage" ;;
    esac
    shift
done
[ "$supervisor" = systemd ] || die "report.sh reads the journal; PQ_SUPERVISOR=$supervisor is not supported"
[[ $tx_count =~ ^[0-9]+$ ]] || die "--tx takes a count"
if [ -z "$src" ]; then
    src=$(hosts | grep -x ewr-002 || hosts | head -1)
fi
node_id_of "$src" > /dev/null
if [ -z "$tx_host" ]; then
    tx_host=$src
    for h in $(hosts); do
        if has_explorer "$h"; then
            tx_host=$h
            break
        fi
    done
fi
node_id_of "$tx_host" > /dev/null

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# clk= node clock minus system clock: max(at - journal timestamp) over the
# last 50 finalizations, + = node ahead. now= is read after the tip.
state_prog='
    /finalized slot=[0-9]/ {
        tip = $0; fin++
        if ($0 ~ /path=Fast/) fast++
        match($0, /at=[0-9]+/); off[++k] = substr($0, RSTART + 3, RLENGTH - 3) / 1e6 - $1 * 1000
    }
    /entering fallback coda/ { coda++ }
    /halted slot=[0-9]/ { halted++ }
    /misbehavior evidence/ { evidence++ }
    END {
        print "tip=" tip
        print "counts=" fin + 0 "/" fast + 0 "/" coda + 0 "/" halted + 0 "/" evidence + 0
        m = "-"; for (i = (k > 50 ? k - 49 : 1); i <= k; i++) if (m == "-" || off[i] > m) m = off[i]
        print "clk=" (m == "-" ? m : sprintf("%+.1fms", m))
    }'

state_host() {
    rssh_user_systemd "$1" "
        echo state=\$(systemctl --user is-active $unit.service) \
            since=\$(systemctl --user show $unit.service -p ActiveEnterTimestamp --value | tr ' ' '_')
        journalctl --user -u $unit -o short-unix --since '1 min ago' \
            | grep -F -e finalized -e 'entering fallback coda' -e halted -e 'misbehavior evidence' \
            | sed 's/\x1b\[[0-9;]*m//g' | awk '$state_prog'
        echo now=\$((\$(date +%s%N) / 1000000))"
}

field() {
    sed -n "s/^$2=//p" "$work/$1.out"
}

fanout_to "$work" state_host
[ "$(cat "$work/$src.rc")" = 0 ] || die "source $src unreachable: $(tail -1 "$work/$src.out")"

genesis=$(remote_config_int "$src" genesis_deadline)
interval=$(remote_config_int "$src" slot_interval)
src_tip=$(field "$src" tip | sed -n 's/.*finalized slot=\([0-9]*\).*/\1/p')

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
    | grep -F -e finalized -e 'coda decided' -e 'entering fallback coda' -e halted \
    | sed 's/\x1b\[[0-9;]*m//g' | awk '/finalized slot=[0-9]/ { if (++n > $slots) exit } { print }'" \
    > "$work/fin" || [ -s "$work/fin" ] || die "no finalized lines on $src since $since"

echo "genesis_deadline=$genesis ($(iso_of_ms "$genesis"))  slot_interval=${interval}ms  source=$src  since=$since  now=$(date -u +%T)"
printf '%-8s %-11s %-10s %-8s %-9s %-10s %s\n' host state tip_slot tip_age lag_slots node_clk \
    'last_1m(finalized/fast/coda_entries/halted/evidence)'
for host in $(hosts); do
    if [ "$(cat "$work/$host.rc")" = 0 ]; then
        state=$(field "$host" state | cut -d' ' -f1)
    else
        state=unreachable
    fi
    tip=$(field "$host" tip | sed -n 's/.*finalized slot=\([0-9]*\).* at=\([0-9]*\).*/\1 \2/p')
    now=$(field "$host" now)
    slot=- age=- lag=-
    if [ -n "$tip" ] && [ -n "$now" ]; then
        read -r slot at <<< "$tip"
        age="$((now - at / 1000000))ms"
        lag=$(((now - genesis) / interval - slot))
    fi
    clk=$(field "$host" clk)
    printf '%-8s %-11s %-10s %-8s %-9s %-10s %s\n' "$host" "$state" "$slot" "$age" "$lag" \
        "${clk:--}" "$(field "$host" counts)"
done
echo "clock slot on $src: $((($(field "$src" now) - genesis) / interval))"
echo

python3 - "$work/fin" "$genesis" "$interval" << 'EOF'
import sys, re, collections
G = int(sys.argv[2]); IV = int(sys.argv[3])
SLOT = r"slot=(?:Slot\()?(\d+)"
fin = {}; view = {}; ent = {}; halt = {}
for l in open(sys.argv[1]):
    t = float(l.split()[0]) * 1000
    if m := re.search(r"finalized slot=(\d+) block=([-+]+) at=(\d+) path=(\w+)", l):
        fin[int(m[1])] = (int(m[3]) / 1e6, m[2], m[4])
    elif m := re.search(r"halted slot=(\d+) at=(\d+)", l): halt[int(m[1])] = int(m[2]) / 1e6
    elif m := re.search(r"coda decided " + SLOT + r" view=(\d+)", l): view[int(m[1])] = int(m[2])
    elif m := re.search(r"entering fallback coda " + SLOT, l): ent[int(m[1])] = t
if not fin: sys.exit("no finalized lines")
n = len(fin); dl = lambda s: G + s * IV
def pct(x, p): x = sorted(x); return f"{x[min(len(x) - 1, int(p / 100 * len(x)))]:.0f}" if x else "-"
def band(s):
    path = fin[s][2]
    if path == "Coda": v = view.get(s); return f"Coda view {v}" if v and v < 3 else "Coda view 3+" if v else "Coda view ?"
    return path
groups = collections.defaultdict(list)
for s in fin: groups[band(s)].append(s)
v = [fin[s][0] - dl(s) for s in fin]
print(f"finalization latency by path, ms after slot deadline (n={n}, mean={sum(v) / n:.0f}); halted = local halt")
print("| path | slots | share | coda entry p50 | p50 | p90 | p99 | halted p50 | halted p99 |")
print("|---|---|---|---|---|---|---|---|---|")
order = ["Fast", "Coda view 1", "Coda view 2", "Coda view 3+", "Coda view ?", "Gadget"]
for k in order + sorted(set(groups) - set(order)):
    ss = groups.get(k, [])
    if not ss and k not in ("Fast", "Coda view 1", "Gadget"): continue
    x = [fin[s][0] - dl(s) for s in ss]; h = [halt[s] - dl(s) for s in ss if s in halt]
    e = [ent[s] - dl(s) for s in ss if s in ent]
    print(f"| {k} | {len(ss)} | {len(ss) / n * 100:.1f}% | {pct(e, 50) if k.startswith('Coda') else '-'} "
          f"| {pct(x, 50)} | {pct(x, 90)} | {pct(x, 99)} | {pct(h, 50)} | {pct(h, 99)} |")
h = [halt[s] - dl(s) for s in fin if s in halt]
print(f"| all | {n} | 100.0% | - | {pct(v, 50)} | {pct(v, 90)} | {pct(v, 99)} | {pct(h, 50)} | {pct(h, 99)} |")
neg = sum("+" not in b for _, b, _ in fin.values())
print(f"all-negative blocks: {neg}/{n} ({neg / n * 100:.0f}%); halted {len(set(fin) & set(halt))}/{n} finalized slots")
EOF

# mcp-tx prints one submit view per tx, then one final view per tx after --wait
tx_probe() {
    local wait=$((tx_count * tx_interval / 1000 + 30))
    echo
    echo "tx probe: $tx_count txs every ${tx_interval}ms via the pq rpc on $tx_host (127.0.0.1:$rpc_http_port), waiting up to ${wait}s"
    rssh "$tx_host" "$remote_root/bin/mcp-tx send --rpc http://127.0.0.1:$rpc_http_port \
        --count $tx_count --interval $tx_interval --wait $wait" > "$work/tx.jsonl" || true
    python3 - "$work/tx.jsonl" "$tx_count" << 'EOF'
import sys, json, collections
views = [json.loads(l) for l in open(sys.argv[1]) if l.startswith("{")]
final = [v for v in views if "known" not in v]
if not final: sys.exit(f"no final views from mcp-tx ({len(views)} submitted of {sys.argv[2]})")
states = collections.Counter(v["state"] for v in final)
lat = collections.defaultdict(list)
for v in final:
    if v["state"] == "committed" and v.get("finalized_at_ms") and v.get("sent_at_ns"):
        ms = v["finalized_at_ms"] - v["sent_at_ns"] / 1e6
        lat[v.get("path") or "?"].append(ms); lat["all"].append(ms)
def pct(x, p): x = sorted(x); return f"{x[min(len(x) - 1, int(p / 100 * len(x)))]:.0f}" if x else "-"
print("submitted " + sys.argv[2] + ", " + ", ".join(f"{k} {c}" for k, c in sorted(states.items())))
print("submit -> finalized latency, ms (mcp-tx sent_at to the colocated node's ledger finalized_at)")
print("| path | txs | p50 | p90 | p99 | max |\n|---|---|---|---|---|---|")
for k in sorted(set(lat) - {"all"}) + ["all"]:
    x = lat.get(k, [])
    print(f"| {k} | {len(x)} | {pct(x, 50)} | {pct(x, 90)} | {pct(x, 99)} | {pct(x, 100)} |")
EOF
}

[ "$tx_count" = 0 ] || tx_probe

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
    printf '\n%-8s %-5s %-10s %-14s %-6s %-6s %-7s %-7s %-9s %-10s %-9s %-9s %s\n' \
        host node finalized all-committed fast coda gadget halted proposed udp-bound evidence warnings errors
    local host log
    for host in $(hosts); do
        log=$(grep -E 'finalized slot=[0-9]|halted slot=[0-9]|proposing|udp bound|misbehavior evidence| WARN | ERROR |panicked' \
            "$logs/$host.log" || true)
        printf '%-8s %-5s %-10s %-14s %-6s %-6s %-7s %-7s %-9s %-10s %-9s %-9s %s\n' \
            "$host" "$(node_id_of "$host")" \
            "$(count_lines 'finalized slot=[0-9]' <<< "$log")" \
            "$(count_lines 'finalized slot=[0-9]+ block=\++ ' <<< "$log")" \
            "$(count_lines 'finalized slot=[0-9].* path=Fast' <<< "$log")" \
            "$(count_lines 'finalized slot=[0-9].* path=Coda' <<< "$log")" \
            "$(count_lines 'finalized slot=[0-9].* path=Gadget' <<< "$log")" \
            "$(count_lines 'halted slot=[0-9]' <<< "$log")" \
            "$(count_lines 'proposing' <<< "$log")" \
            "$(count_lines 'udp bound' <<< "$log")" \
            "$(count_lines 'misbehavior evidence' <<< "$log")" \
            "$(count_lines ' WARN' <<< "$log")" \
            "$(count_lines ' ERROR|panicked' <<< "$log")"
    done
}

# `host slot block` per finalized line, deduplicated; a host that logs one
# slot with two different blocks conflicts with itself
finalized_pairs() {
    local host
    for host in $(hosts); do
        sed -nE "s/.*finalized slot=([0-9]+) block=([+-]+) .*/$host \1 \2/p" "$logs/$host.log"
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
        sed -E 's/^[^ ]+ +WARN +(node[0-9]+: )?//' <<< "$warnings" | cut -c1-100 | sort | uniq -c | sort -rn | head -5
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
