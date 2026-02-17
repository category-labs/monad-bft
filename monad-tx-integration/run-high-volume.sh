#!/usr/bin/env bash
set -euo pipefail

# High Volume Stress Test
# Push maximum throughput to find system limits
# Goal: Determine max sustainable TPS and behavior at limits

BINARY="$(dirname "$0")/../target/release/monad-tx-integration"
OUT_DIR="${1:-/tmp/tx-experiment-high-volume}"
COMMIT_INTERVAL_MS=500
NUM_ACCOUNTS=5000

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

echo "=== Building ==="
cargo build -p monad-tx-integration --release 2>&1 | tail -3

echo "=== Starting node ==="
RUST_LOG=monad_tx_integration=info "$BINARY" node \
    --listen 0.0.0.0:0 \
    --rpc-listen 127.0.0.1:0 \
    --commit-interval-ms "$COMMIT_INTERVAL_MS" \
    --stats-file "$OUT_DIR/node-stats.jsonl" \
    --num-accounts "$NUM_ACCOUNTS" \
    --duration-secs 45 \
    > "$OUT_DIR/node.log" 2>&1 &
NODE_PID=$!

ADDR=""
for i in $(seq 1 50); do
    RC_TCP_ADDR=$(grep -oP 'RC_TCP_ADDR=\K.*' "$OUT_DIR/node.log" 2>/dev/null || true)
    RC_UDP_ADDR=$(grep -oP 'RC_UDP_ADDR=\K.*' "$OUT_DIR/node.log" 2>/dev/null || true)
    RC_AUTH_UDP_ADDR=$(grep -oP 'RC_AUTH_UDP_ADDR=\K.*' "$OUT_DIR/node.log" 2>/dev/null || true)
    LEANUDP_ADDR=$(grep -oP 'LEANUDP_ADDR=\K.*' "$OUT_DIR/node.log" 2>/dev/null || true)
    ADDR="$LEANUDP_ADDR"
    RPC_ADDR=$(grep -oP 'RPC_ADDR=\K.*' "$OUT_DIR/node.log" 2>/dev/null || true)
    [ -n "$ADDR" ] && [ -n "$RC_TCP_ADDR" ] && [ -n "$RC_UDP_ADDR" ] && [ -n "$RC_AUTH_UDP_ADDR" ] && [ -n "$LEANUDP_ADDR" ] && [ -n "$RPC_ADDR" ] && break
    sleep 0.1
done

if [ -z "$ADDR" ]; then
    echo "ERROR: Node failed to start"
    kill $NODE_PID 2>/dev/null
    exit 1
fi
echo "Node listening on $ADDR, RPC on $RPC_ADDR"

echo "=== Starting 50 high-volume peers ==="
# 50 peers at 400 tps = 20,000 tps total input
for i in $(seq 0 49); do
    RUST_LOG=monad_tx_integration=info "$BINARY" submit \
        --transport "${TRANSPORT:-leanudp}" \
        --rc-tcp-addr "$RC_TCP_ADDR" \
        --rc-udp-addr "$RC_UDP_ADDR" \
        --rc-auth-udp-addr "$RC_AUTH_UDP_ADDR" \
        --leanudp-addr "$LEANUDP_ADDR" \
        --rpc-addr "$RPC_ADDR" \
        --tps 400 \
        --sender-index "$i" \
        --duration-secs 40 \
        --stats-file "$OUT_DIR/peer-$i.json" \
        --max-fee-multiplier 2 \
        --priority-fee-multiplier 1 \
        > "$OUT_DIR/peer-$i.log" 2>&1 &
done
echo "  50 peers × 400 tps = 20,000 tps total input"

sleep 45
wait $NODE_PID 2>/dev/null || true

echo "=== Results ==="
python3 << 'PYEOF'
import json

out_dir = "/tmp/tx-experiment-high-volume"

blocks = []
with open(f"{out_dir}/node-stats.jsonl") as f:
    for line in f:
        if line.strip():
            blocks.append(json.loads(line))

t0 = blocks[0]["timestamp_ms"]

print("\n" + "=" * 70)
print("HIGH VOLUME STRESS TEST ANALYSIS")
print("=" * 70)

# Overall stats
total_recv = blocks[-1]["txs_received_total"]
total_incl = sum(b["txs_included"] for b in blocks)
duration = (blocks[-1]["timestamp_ms"] - t0) / 1000

print(f"\n### Overall Performance")
print(f"  Duration: {duration:.1f}s")
print(f"  Total received: {total_recv:,}")
print(f"  Total included: {total_incl:,}")
print(f"  Inclusion rate: {total_incl/total_recv*100:.1f}%")
print(f"  Effective TPS: {total_incl/duration:,.0f}")
print(f"  Input TPS (estimated): {total_recv/duration:,.0f}")

# Throughput over time
print(f"\n### Throughput Timeline")
print(f"{'Time(s)':>8} {'Cumul Recv':>12} {'Cumul Incl':>12} {'Window TPS':>12}")
print("-" * 50)

prev_incl = 0
prev_t = 0
for i in [10, 20, 30, 40, 50, 60, 70, 80]:
    if i >= len(blocks):
        continue
    b = blocks[i]
    t = (b["timestamp_ms"] - t0) / 1000
    cumul_incl = sum(blocks[j]["txs_included"] for j in range(i+1))
    window_tps = (cumul_incl - prev_incl) / (t - prev_t) if t > prev_t else 0
    print(f"{t:>8.1f} {b['txs_received_total']:>12,} {cumul_incl:>12,} {window_tps:>12,.0f}")
    prev_incl = cumul_incl
    prev_t = t

# Peak throughput
max_txs_block = max(b["txs_included"] for b in blocks)
print(f"\n### Peak Performance")
print(f"  Max txs in single block: {max_txs_block:,}")
print(f"  Peak block TPS: {max_txs_block * 2:,} (assuming 500ms blocks)")

# Check for issues
final_backlog = total_recv - total_incl
stalls = sum(1 for b in blocks if b["txs_included"] == 0)

print(f"\n### Health Indicators")
print(f"  Final backlog: {final_backlog:,}")
print(f"  Blocks with 0 txs: {stalls}/{len(blocks)}")

if stalls > len(blocks) * 0.3:
    print(f"  ❌ CRITICAL: High stall rate ({stalls/len(blocks)*100:.0f}%)")
elif final_backlog > total_recv * 0.5:
    print(f"  ⚠️ Large backlog - may indicate capacity limit")
else:
    print(f"  ✓ System handling high volume")

PYEOF

echo ""
echo "=== Results written to $OUT_DIR ==="
