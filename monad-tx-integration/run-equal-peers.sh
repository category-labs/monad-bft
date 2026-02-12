#!/usr/bin/env bash
set -euo pipefail

# Equal Peers Experiment
# 5 identical peers: same TPS, same fees, same start time
# Goal: Verify even distribution (Jain's index ~1.0)

BINARY="$(dirname "$0")/../target/release/monad-tx-integration"
OUT_DIR="${1:-/tmp/tx-experiment-equal-peers}"
COMMIT_INTERVAL_MS=500
NUM_ACCOUNTS=1000

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

echo "=== Building ==="
cargo build -p monad-tx-integration --release 2>&1 | tail -3

echo "=== Starting node ==="
EXTRA_RUST_LOG="${EXTRA_RUST_LOG:-}"
NODE_RUST_LOG="monad_tx_integration=info"
SUBMIT_RUST_LOG="monad_tx_integration=info"
if [ -n "$EXTRA_RUST_LOG" ]; then
    NODE_RUST_LOG="$NODE_RUST_LOG,$EXTRA_RUST_LOG"
    SUBMIT_RUST_LOG="$SUBMIT_RUST_LOG,$EXTRA_RUST_LOG"
fi

RUST_LOG="$NODE_RUST_LOG" "$BINARY" node \
    --listen 127.0.0.1:0 \
    --commit-interval-ms "$COMMIT_INTERVAL_MS" \
    --stats-file "$OUT_DIR/node-stats.jsonl" \
    --num-accounts "$NUM_ACCOUNTS" \
    --duration-secs 40 \
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

echo "=== Starting 5 identical peers ==="
for i in $(seq 0 4); do
    RUST_LOG="$SUBMIT_RUST_LOG" "$BINARY" submit \
        --node-addr "$ADDR" \
        --tps 400 \
        --sender-index "$i" \
        --duration-secs 35 \
        --stats-file "$OUT_DIR/peer-$i.json" \
        --max-fee-multiplier 3 \
        --priority-fee-multiplier 2 \
        > "$OUT_DIR/peer-$i.log" 2>&1 &
    echo "  Peer P$i: 400 tps, 3x fee"
done

sleep 40
wait $NODE_PID 2>/dev/null || true

echo "=== Results ==="
OUT_DIR="$OUT_DIR" python3 << 'PYEOF'
import json
import math
import os

out_dir = os.environ["OUT_DIR"]

blocks = []
with open(f"{out_dir}/node-stats.jsonl") as f:
    for line in f:
        if line.strip():
            blocks.append(json.loads(line))

print("\n" + "=" * 70)
print("EQUAL PEERS FAIRNESS ANALYSIS")
print("=" * 70)

final = blocks[-1]["peers"]

print(f"\n### Per-Peer Stats")
print(f"{'Peer':>10} {'Recv':>8} {'Incl':>8} {'Rate':>8} {'Score':>12}")
print("-" * 52)

inclusion_rates = []
for p in sorted(final, key=lambda x: x["peer_id"]):
    rate = p["txs_included_total"] / p["txs_received_total"] if p["txs_received_total"] > 0 else 0
    inclusion_rates.append(rate)
    print(f"{p['peer_id'][:10]:>10} {p['txs_received_total']:>8} {p['txs_included_total']:>8} {rate*100:>7.1f}% {p['score']:>12.0f}")

# Jain's Fairness Index
n = len(inclusion_rates)
if n > 0 and sum(r**2 for r in inclusion_rates) > 0:
    jain = (sum(inclusion_rates) ** 2) / (n * sum(r**2 for r in inclusion_rates))
else:
    jain = 0

# Standard deviation
mean_rate = sum(inclusion_rates) / n if n > 0 else 0
variance = sum((r - mean_rate) ** 2 for r in inclusion_rates) / n if n > 0 else 0
std_dev = math.sqrt(variance)

print(f"\n### Fairness Metrics")
print(f"  Number of peers: {n}")
print(f"  Mean inclusion rate: {mean_rate*100:.1f}%")
print(f"  Std deviation: {std_dev*100:.2f}%")
print(f"  Jain's Fairness Index: {jain:.4f}")

if jain >= 0.99:
    print(f"  ✓ Perfect fairness (Jain >= 0.99)")
elif jain >= 0.95:
    print(f"  ✓ Excellent fairness (Jain >= 0.95)")
elif jain >= 0.90:
    print(f"  ⚠️ Good fairness (Jain >= 0.90)")
else:
    print(f"  ❌ Poor fairness (Jain < 0.90)")

# Score variance
scores = [p["score"] for p in final]
mean_score = sum(scores) / len(scores) if scores else 0
score_variance = sum((s - mean_score) ** 2 for s in scores) / len(scores) if scores else 0
score_std = math.sqrt(score_variance)
score_cv = score_std / mean_score if mean_score > 0 else 0

print(f"\n### Score Distribution")
print(f"  Mean score: {mean_score:.0f}")
print(f"  Std deviation: {score_std:.0f}")
print(f"  Coefficient of variation: {score_cv*100:.1f}%")

PYEOF

echo ""
echo "=== Results written to $OUT_DIR ==="
