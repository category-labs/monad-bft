#!/usr/bin/env bash
# latency.sh [--out file] [--count n]
# pings every validator from every host over ICMP and writes the RTT matrix
# (row = from node_id, col = to node_id, ms) as toml: rtt_ms is the p50,
# rtt_min_ms/rtt_avg_ms alongside. Validator IPs come from each host's
# tracked config/<host>/node.toml. A host or pair with no replies fails the run.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

usage="usage: latency.sh [--out file] [--count n]"
out=$HOME/tmp/mcp-latency/latency.toml
count=50
while [ $# -gt 0 ]; do
    case $1 in
        --out) out=${2:?$usage}; shift ;;
        --count) count=${2:?$usage}; shift ;;
        *) die "$usage" ;;
    esac
    shift
done

# "<node_id> <ip>" per validator, in node_id order
validator_ips() {
    awk -F'"' '/^node_id/ && v { id = $0; sub(/.*= */, "", id) }
        /^\[\[validators\]\]/ { v = 1 }
        /^address/ && v { split($2, a, ":"); print id, a[1] }' "$config_dir/$1/node.toml"
}

# prints "<to_id> <min> <avg> <p50>" for every other validator
probe() {
    local host=$1 self
    self=$(node_id_of "$host")
    local script=""
    while read -r id ip; do
        [ "$id" = "$self" ] && continue
        script+="(ping -n -c $count -i 0.2 -W 2 $ip | awk -v id=$id '
            /time=/ { sub(/.*time=/, \"\"); sub(/ ms.*/, \"\"); r[n++] = \$0 + 0 }
            END {
                if (!n) { print id, \"none\"; exit }
                for (i = 0; i < n; i++) for (j = i + 1; j < n; j++) if (r[j] < r[i]) { t = r[i]; r[i] = r[j]; r[j] = t }
                s = 0; for (i = 0; i < n; i++) s += r[i]
                printf \"%s %.3f %.3f %.3f\\n\", id, r[0], s / n, (n % 2) ? r[int(n / 2)] : (r[n / 2 - 1] + r[n / 2]) / 2
            }') &
"
    done < <(validator_ips "$host")
    rssh "$host" "$script wait"
}

n=$(host_count)
dir=$(mktemp -d)
trap 'rm -rf "$dir"' EXIT
echo "pinging: $n hosts x $((n - 1)) peers, $count probes each" >&2
fanout_to "$dir" probe
failed=0
for h in $(hosts); do
    if [ "$(cat "$dir/$h.rc")" != 0 ] || grep -q none "$dir/$h.out" \
        || [ "$(grep -c . "$dir/$h.out")" != $((n - 1)) ]; then
        echo "error: $h:" >&2
        sed 's/^/    /' "$dir/$h.out" >&2
        failed=1
    fi
done
[ "$failed" = 0 ] || die "missing replies; no matrix written"

mkdir -p "$(dirname "$out")"
python3 - "$dir" "$out" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" $(hosts) << 'EOF'
import sys
d, out, at, hosts = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4:]
n = len(hosts)
m = {k: [[0.0] * n for _ in range(n)] for k in ("min", "avg", "p50")}
for i, h in enumerate(hosts):
    for line in open(f"{d}/{h}.out"):
        j, lo, avg, p50 = line.split()
        j = int(j)
        m["min"][i][j], m["avg"][i][j], m["p50"][i][j] = float(lo), float(avg), float(p50)

def rows(a):
    return "[\n" + "".join("  [" + ", ".join(f"{v:.3f}" for v in r) + "],\n" for r in a) + "]"

with open(out, "w") as f:
    f.write(f'# written by monad-mcp-node/deploy/latency.sh; row = from node_id, col = to node_id\n')
    f.write(f'measured_at = "{at}"\n')
    f.write("hosts = [" + ", ".join(f'"{h}"' for h in hosts) + "]\n")
    f.write(f"rtt_ms = {rows(m['p50'])}\n")
    f.write(f"rtt_min_ms = {rows(m['min'])}\n")
    f.write(f"rtt_avg_ms = {rows(m['avg'])}\n")

p = m["p50"]
print("p50 RTT ms, row = from")
print(f"{'':>8} " + " ".join(f"{h:>8}" for h in hosts))
for i, h in enumerate(hosts):
    print(f"{h:>8} " + " ".join(f"{p[i][j]:>8.1f}" for j in range(n)))
asym = [(hosts[i], hosts[j], p[i][j], p[j][i]) for i in range(n) for j in range(i + 1, n)
        if abs(p[i][j] - p[j][i]) > 0.1 * min(p[i][j], p[j][i])]
for a, b, x, y in asym:
    print(f"asymmetric: {a}->{b} {x:.1f} ms vs {b}->{a} {y:.1f} ms")
print(f"wrote {out}")
EOF
