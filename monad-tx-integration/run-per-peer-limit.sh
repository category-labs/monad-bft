#!/usr/bin/env bash
set -euo pipefail

# Per-Peer Limit Experiment
# Test the 10K per-peer ingress limit
# Goal: Verify single peer cannot monopolize ingress queue

BINARY="$(dirname "$0")/../target/release/monad-tx-integration"
OUT_DIR="${1:-/tmp/tx-experiment-peer-limit}"
COMMIT_INTERVAL_MS=500
NUM_ACCOUNTS=1000

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

echo "=== Building ==="
cargo build -p monad-tx-integration --release 2>&1 | tail -3

echo "=== Starting node ==="
RUST_LOG=monad_tx_integration=info "$BINARY" node \
    --listen 127.0.0.1:0 \
    --commit-interval-ms "$COMMIT_INTERVAL_MS" \
    --stats-file "$OUT_DIR/node-stats.jsonl" \
    --num-accounts "$NUM_ACCOUNTS" \
    --duration-secs 35 \
    > "$OUT_DIR/node.log" 2>&1 &
NODE_PID=$!

ADDR=""
for i in $(seq 1 50); do
    ADDR=$(grep -oP 'LISTEN_ADDR=\K.*' "$OUT_DIR/node.log" 2>/dev/null || true)
    [ -n "$ADDR" ] && break
    sleep 0.1
done

if [ -z "$ADDR" ]; then
    echo "ERROR: Node failed to start"
    kill $NODE_PID 2>/dev/null
    exit 1
fi
echo "Node listening on $ADDR"

echo "=== Starting aggressive peer (trying to flood) ==="
# One peer at very high TPS trying to monopolize
RUST_LOG=monad_tx_integration=info "$BINARY" submit \
    --node-addr "$ADDR" \
    --tps 2000 \
    --sender-index 0 \
    --duration-secs 30 \
    --stats-file "$OUT_DIR/aggressive.json" \
    --max-fee-multiplier 1 \
    --priority-fee-multiplier 0 \
    > "$OUT_DIR/aggressive.log" 2>&1 &
echo "  Aggressive peer: 2000 tps"

echo "=== Starting normal peer ==="
# Normal peer for comparison
RUST_LOG=monad_tx_integration=info "$BINARY" submit \
    --node-addr "$ADDR" \
    --tps 300 \
    --sender-index 1 \
    --duration-secs 30 \
    --stats-file "$OUT_DIR/normal.json" \
    --max-fee-multiplier 3 \
    --priority-fee-multiplier 2 \
    > "$OUT_DIR/normal.log" 2>&1 &
echo "  Normal peer: 300 tps, 3x fee"

sleep 35
wait $NODE_PID 2>/dev/null || true

echo "=== Results ==="
python3 << 'PYEOF'
import json

out_dir = "/tmp/tx-experiment-peer-limit"

blocks = []
with open(f"{out_dir}/node-stats.jsonl") as f:
    for line in f:
        if line.strip():
            blocks.append(json.loads(line))

print("\n" + "=" * 70)
print("PER-PEER LIMIT ANALYSIS")
print("=" * 70)

final = blocks[-1]["peers"]
aggressive = max(final, key=lambda p: p["txs_received_total"])
normal = min(final, key=lambda p: p["txs_received_total"])

print(f"\n### Peer Comparison")
print(f"{'Peer':>12} {'Sent':>10} {'Recv':>10} {'Incl':>10} {'Rate':>8} {'Score':>12}")
print("-" * 70)

agg_rate = aggressive["txs_included_total"] / aggressive["txs_received_total"] * 100 if aggressive["txs_received_total"] > 0 else 0
norm_rate = normal["txs_included_total"] / normal["txs_received_total"] * 100 if normal["txs_received_total"] > 0 else 0

# Load submitter stats
try:
    with open(f"{out_dir}/aggressive.json") as f:
        agg_stats = json.load(f)
    with open(f"{out_dir}/normal.json") as f:
        norm_stats = json.load(f)
except:
    agg_stats = {"total_sent": "?"}
    norm_stats = {"total_sent": "?"}

print(f"{'Aggressive':>12} {agg_stats.get('total_sent', '?'):>10} {aggressive['txs_received_total']:>10} {aggressive['txs_included_total']:>10} {agg_rate:>7.1f}% {aggressive['score']:>12.0f}")
print(f"{'Normal':>12} {norm_stats.get('total_sent', '?'):>10} {normal['txs_received_total']:>10} {normal['txs_included_total']:>10} {norm_rate:>7.1f}% {normal['score']:>12.0f}")

# Check if aggressive peer was limited
agg_sent = agg_stats.get("total_sent", 0)
if isinstance(agg_sent, int) and agg_sent > 0:
    drop_rate = (agg_sent - aggressive["txs_received_total"]) / agg_sent * 100
    print(f"\n### Rate Limiting Analysis")
    print(f"  Aggressive sent: {agg_sent:,}")
    print(f"  Aggressive received by node: {aggressive['txs_received_total']:,}")
    print(f"  Drop rate at ingress: {drop_rate:.1f}%")

    if drop_rate > 10:
        print(f"  ✓ Per-peer limiting active - dropped {drop_rate:.0f}% of aggressive peer's txs")
    else:
        print(f"  Per-peer limiting may not have been triggered")

# Bandwidth share
total_incl = aggressive["txs_included_total"] + normal["txs_included_total"]
if total_incl > 0:
    agg_share = aggressive["txs_included_total"] / total_incl * 100
    norm_share = normal["txs_included_total"] / total_incl * 100
    print(f"\n### Bandwidth Share")
    print(f"  Aggressive: {agg_share:.1f}%")
    print(f"  Normal: {norm_share:.1f}%")

    if norm_share > 20:
        print(f"  ✓ Normal peer maintained reasonable bandwidth share")
    else:
        print(f"  ⚠️ Normal peer was starved by aggressive peer")

PYEOF

echo ""
echo "=== Results written to $OUT_DIR ==="
