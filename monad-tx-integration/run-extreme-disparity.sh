#!/usr/bin/env bash
set -euo pipefail

# Extreme Score Disparity Experiment
# One peer builds score for 20s alone, then low-score peer joins
# Goal: Verify WFQ handles 100:1+ score ratios correctly

BINARY="$(dirname "$0")/../target/release/monad-tx-integration"
OUT_DIR="${1:-/tmp/tx-experiment-extreme-disparity}"
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
    --duration-secs 45 \
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

echo "=== High-score peer starts alone (builds score for 20s) ==="
RUST_LOG=monad_tx_integration=info "$BINARY" submit \
    --node-addr "$ADDR" \
    --tps 600 \
    --sender-index 0 \
    --duration-secs 40 \
    --stats-file "$OUT_DIR/high-score.json" \
    --max-fee-multiplier 10 \
    --priority-fee-multiplier 5 \
    > "$OUT_DIR/high-score.log" 2>&1 &
echo "  High-score peer: 600 tps, 10x fee"

sleep 20

echo "=== Low-score peer joins at t=20s ==="
RUST_LOG=monad_tx_integration=info "$BINARY" submit \
    --node-addr "$ADDR" \
    --tps 600 \
    --sender-index 1 \
    --duration-secs 20 \
    --stats-file "$OUT_DIR/low-score.json" \
    --max-fee-multiplier 1 \
    --priority-fee-multiplier 0 \
    > "$OUT_DIR/low-score.log" 2>&1 &
echo "  Low-score peer: 600 tps, 1x fee (same TPS, lower fees)"

sleep 20

wait $NODE_PID 2>/dev/null || true

echo "=== Results ==="
python3 << 'PYEOF'
import json

out_dir = "/tmp/tx-experiment-extreme-disparity"

blocks = []
with open(f"{out_dir}/node-stats.jsonl") as f:
    for line in f:
        if line.strip():
            blocks.append(json.loads(line))

t0 = blocks[0]["timestamp_ms"]

print("\n" + "=" * 70)
print("EXTREME SCORE DISPARITY ANALYSIS")
print("=" * 70)

# Track both peers
print(f"\n### Score Ratio Evolution")
print(f"{'Time(s)':>8} {'High Score':>14} {'Low Score':>14} {'Ratio':>10}")
print("-" * 50)

for t_target in [15, 20, 25, 30, 35, 40]:
    block_idx = int(t_target * 2)
    if block_idx >= len(blocks):
        continue
    b = blocks[block_idx]
    t = (b["timestamp_ms"] - t0) / 1000
    peers = sorted(b["peers"], key=lambda p: p["score"], reverse=True)

    if len(peers) >= 2:
        high = peers[0]
        low = peers[1]
        ratio = high["score"] / low["score"] if low["score"] > 0 else float('inf')
        print(f"{t:>8.1f} {high['score']:>14.0f} {low['score']:>14.0f} {ratio:>10.1f}x")
    elif len(peers) == 1:
        print(f"{t:>8.1f} {peers[0]['score']:>14.0f} {'N/A':>14} {'N/A':>10}")

# Final comparison
final = blocks[-1]["peers"]
if len(final) >= 2:
    sorted_final = sorted(final, key=lambda p: p["score"], reverse=True)
    high = sorted_final[0]
    low = sorted_final[1]

    high_rate = high["txs_included_total"] / high["txs_received_total"] * 100 if high["txs_received_total"] > 0 else 0
    low_rate = low["txs_included_total"] / low["txs_received_total"] * 100 if low["txs_received_total"] > 0 else 0

    print(f"\n### Final Results")
    print(f"  High-score: recv={high['txs_received_total']}, incl={high['txs_included_total']}, rate={high_rate:.1f}%, score={high['score']:.0f}")
    print(f"  Low-score:  recv={low['txs_received_total']}, incl={low['txs_included_total']}, rate={low_rate:.1f}%, score={low['score']:.0f}")

    score_ratio = high["score"] / low["score"] if low["score"] > 0 else 0
    incl_ratio = high["txs_included_total"] / low["txs_included_total"] if low["txs_included_total"] > 0 else 0

    print(f"\n### Proportionality Check")
    print(f"  Score ratio: {score_ratio:.1f}x")
    print(f"  Inclusion ratio: {incl_ratio:.1f}x")

    # During competition phase (t=20-40s), check if bandwidth is proportional to score
    # WFQ should give bandwidth proportional to score
    if abs(score_ratio - incl_ratio) / score_ratio < 0.3:
        print(f"  ✓ WFQ is allocating bandwidth proportionally (within 30%)")
    else:
        print(f"  ⚠️ Bandwidth allocation differs significantly from score ratio")

PYEOF

echo ""
echo "=== Results written to $OUT_DIR ==="
