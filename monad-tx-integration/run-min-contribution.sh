#!/usr/bin/env bash
set -euo pipefail

# Minimum Viable Contribution Experiment
# Test different gas rates to find promotion threshold behavior
# Goal: Document minimum gas rate needed to get promoted and maintain bandwidth

BINARY="$(dirname "$0")/../target/release/monad-tx-integration"
OUT_DIR="${1:-/tmp/tx-experiment-min-contrib}"
COMMIT_INTERVAL_MS=500
NUM_ACCOUNTS=1000

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

echo "=== Building ==="
cargo build -p monad-tx-integration --release 2>&1 | tail -3

echo "=== Starting node ==="
RUST_LOG=monad_tx_integration=info "$BINARY" node \
    --listen 0.0.0.0:0 \
    --commit-interval-ms "$COMMIT_INTERVAL_MS" \
    --stats-file "$OUT_DIR/node-stats.jsonl" \
    --num-accounts "$NUM_ACCOUNTS" \
    --duration-secs 45 \
    > "$OUT_DIR/node.log" 2>&1 &
NODE_PID=$!

ADDR=""
for i in $(seq 1 50); do
    ADDR=$(grep -oP 'CONNECT_ADDR=\K.*' "$OUT_DIR/node.log" 2>/dev/null || true)
    [ -n "$ADDR" ] && break
    sleep 0.1
done

if [ -z "$ADDR" ]; then
    echo "ERROR: Node failed to start"
    kill $NODE_PID 2>/dev/null
    exit 1
fi
echo "Node listening on $ADDR"

echo "=== Starting peers with varying contribution rates ==="

# Very low rate (10 tps)
RUST_LOG=monad_tx_integration=info "$BINARY" submit \
    --node-addr "$ADDR" \
    --tps 10 \
    --sender-index 0 \
    --duration-secs 40 \
    --stats-file "$OUT_DIR/very-low.json" \
    --max-fee-multiplier 1 \
    --priority-fee-multiplier 0 \
    > "$OUT_DIR/very-low.log" 2>&1 &
echo "  Very-low: 10 tps, 1x fee"

# Low rate (50 tps)
RUST_LOG=monad_tx_integration=info "$BINARY" submit \
    --node-addr "$ADDR" \
    --tps 50 \
    --sender-index 1 \
    --duration-secs 40 \
    --stats-file "$OUT_DIR/low.json" \
    --max-fee-multiplier 1 \
    --priority-fee-multiplier 0 \
    > "$OUT_DIR/low.log" 2>&1 &
echo "  Low: 50 tps, 1x fee"

# Medium rate (100 tps)
RUST_LOG=monad_tx_integration=info "$BINARY" submit \
    --node-addr "$ADDR" \
    --tps 100 \
    --sender-index 2 \
    --duration-secs 40 \
    --stats-file "$OUT_DIR/medium.json" \
    --max-fee-multiplier 1 \
    --priority-fee-multiplier 0 \
    > "$OUT_DIR/medium.log" 2>&1 &
echo "  Medium: 100 tps, 1x fee"

# High rate (300 tps)
RUST_LOG=monad_tx_integration=info "$BINARY" submit \
    --node-addr "$ADDR" \
    --tps 300 \
    --sender-index 3 \
    --duration-secs 40 \
    --stats-file "$OUT_DIR/high.json" \
    --max-fee-multiplier 1 \
    --priority-fee-multiplier 0 \
    > "$OUT_DIR/high.log" 2>&1 &
echo "  High: 300 tps, 1x fee"

# Very high rate (500 tps)
RUST_LOG=monad_tx_integration=info "$BINARY" submit \
    --node-addr "$ADDR" \
    --tps 500 \
    --sender-index 4 \
    --duration-secs 40 \
    --stats-file "$OUT_DIR/very-high.json" \
    --max-fee-multiplier 1 \
    --priority-fee-multiplier 0 \
    > "$OUT_DIR/very-high.log" 2>&1 &
echo "  Very-high: 500 tps, 1x fee"

sleep 45
wait $NODE_PID 2>/dev/null || true

echo "=== Results ==="
python3 << 'PYEOF'
import json

out_dir = "/tmp/tx-experiment-min-contrib"

blocks = []
with open(f"{out_dir}/node-stats.jsonl") as f:
    for line in f:
        if line.strip():
            blocks.append(json.loads(line))

t0 = blocks[0]["timestamp_ms"]

print("\n" + "=" * 70)
print("MINIMUM VIABLE CONTRIBUTION ANALYSIS")
print("=" * 70)

# Track promotion status over time
tps_labels = ["Very-low (10)", "Low (50)", "Medium (100)", "High (300)", "Very-high (500)"]

print(f"\n### Promotion Timeline")
print(f"{'Time(s)':>8} " + " ".join(f"{l[:12]:>14}" for l in tps_labels))
print("-" * 85)

for t_target in [5, 10, 15, 20, 25, 30, 35, 40]:
    block_idx = int(t_target * 2)
    if block_idx >= len(blocks):
        continue
    b = blocks[block_idx]
    t = (b["timestamp_ms"] - t0) / 1000

    peers_by_recv = sorted(b["peers"], key=lambda p: p["txs_received_total"])
    statuses = []
    for p in peers_by_recv:
        status = "✓ PROMOTED" if p["promoted"] else "newcomer"
        statuses.append(f"{status:>14}")

    print(f"{t:>8.1f} " + " ".join(statuses))

# Final comparison
print(f"\n### Final Results")
print(f"{'Rate':>15} {'TPS':>6} {'Recv':>8} {'Incl':>8} {'Rate':>8} {'Score':>12} {'Promoted':>10}")
print("-" * 80)

final = blocks[-1]["peers"]
peers_sorted = sorted(final, key=lambda p: p["txs_received_total"])

rates = [10, 50, 100, 300, 500]
for i, p in enumerate(peers_sorted):
    rate = p["txs_included_total"] / p["txs_received_total"] * 100 if p["txs_received_total"] > 0 else 0
    promoted = "✓" if p["promoted"] else "✗"
    label = tps_labels[i] if i < len(tps_labels) else f"Peer {i}"
    print(f"{label:>15} {rates[i] if i < len(rates) else '?':>6} {p['txs_received_total']:>8} {p['txs_included_total']:>8} {rate:>7.1f}% {p['score']:>12.0f} {promoted:>10}")

# Score per TPS analysis
print(f"\n### Score Efficiency (score per tx sent)")
for i, p in enumerate(peers_sorted):
    if p["txs_received_total"] > 0:
        efficiency = p["score"] / p["txs_received_total"]
        label = tps_labels[i] if i < len(tps_labels) else f"Peer {i}"
        print(f"  {label}: {efficiency:.2f} score/tx")

PYEOF

echo ""
echo "=== Results written to $OUT_DIR ==="
