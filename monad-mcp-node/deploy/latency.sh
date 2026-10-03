#!/usr/bin/env bash
# latency.sh [--out file] [--count n]
# pings every validator from every host over ICMP and writes the p50 RTT matrix
# (rtt_ms, row = from node_id, col = to node_id) with the hosts and validator ips
# it was measured for; check-latency.py validates it before it replaces --out.
# Validator IPs come from the tracked config/<host>/node.toml, which must all agree.
# A host or pair with no replies fails the run.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

usage="usage: latency.sh [--out file] [--count n]"
out=$latency_file
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
        /^address/ && v { split($2, a, ":"); print id, a[1] }' "$(host_config "$1")"
}

configs=()
for h in $(hosts); do configs+=("$(host_config "$h")"); done
first=$(hosts | head -1)
for h in $(hosts); do
    [ "$(validator_ips "$h")" = "$(validator_ips "$first")" ] \
        || die "$(host_config "$h") and $(host_config "$first") list different validators; re-run gen-config.sh"
done

# prints "<to_id> <p50>" for every other validator
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
                printf \"%s %.3f\\n\", id, (n % 2) ? r[int(n / 2)] : (r[n / 2 - 1] + r[n / 2]) / 2
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

python3 - "$dir" "$dir/latency.toml" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    "$(validator_ips "$first" | awk '{ print $2 }' | xargs)" $(hosts) << 'EOF'
import sys
d, out, at, ips, hosts = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4].split(), sys.argv[5:]
n = len(hosts)
p = [[0.0] * n for _ in range(n)]
for i, h in enumerate(hosts):
    for line in open(f"{d}/{h}.out"):
        j, p50 = line.split()
        p[i][int(j)] = float(p50)

with open(out, "w") as f:
    f.write("# written by monad-mcp-node/deploy/latency.sh; p50 RTT ms, row = from node_id, col = to\n")
    f.write(f'measured_at = "{at}"\n')
    f.write("hosts = [" + ", ".join(f'"{h}"' for h in hosts) + "]\n")
    f.write("ips = [" + ", ".join(f'"{ip}"' for ip in ips) + "]\n")
    f.write("rtt_ms = [\n" + "".join("  [" + ", ".join(f"{v:.3f}" for v in r) + "],\n" for r in p) + "]\n")

print("p50 RTT ms, row = from")
print(f"{'':>8} " + " ".join(f"{h:>8}" for h in hosts))
for i, h in enumerate(hosts):
    print(f"{h:>8} " + " ".join(f"{p[i][j]:>8.1f}" for j in range(n)))
for i in range(n):
    for j in range(i + 1, n):
        if abs(p[i][j] - p[j][i]) > 0.1 * min(p[i][j], p[j][i]):
            print(f"asymmetric: {hosts[i]}->{hosts[j]} {p[i][j]:.1f} ms vs {hosts[j]}->{hosts[i]} {p[j][i]:.1f} ms")
EOF
python3 "$deploy_dir/check-latency.py" "$dir/latency.toml" --hosts $(hosts) --configs "${configs[@]}" \
    || die "measured matrix failed validation; $out left as is"
mkdir -p "$(dirname "$out")"
mv "$dir/latency.toml" "$out"
echo "wrote $out"
