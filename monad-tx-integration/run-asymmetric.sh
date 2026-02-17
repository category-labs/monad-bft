#!/usr/bin/env bash
set -euo pipefail

# Asymmetric Duration Experiment
# Peers with different lifetimes: 10s, 20s, 30s
# Goal: Verify score decay and bandwidth reallocation when peers leave

BINARY="$(dirname "$0")/../target/release/monad-tx-integration"
OUT_DIR="${1:-/tmp/tx-experiment-asymmetric}"
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

echo "=== Starting peers with different durations ==="

# Short-lived peer (10s)
RUST_LOG=monad_tx_integration=info "$BINARY" submit \
    --node-addr "$ADDR" \
    --tps 500 \
    --sender-index 0 \
    --duration-secs 10 \
    --stats-file "$OUT_DIR/short.json" \
    --max-fee-multiplier 3 \
    --priority-fee-multiplier 2 \
    > "$OUT_DIR/short.log" 2>&1 &
echo "  Short peer: 500 tps, 10s duration"

# Medium-lived peer (20s)
RUST_LOG=monad_tx_integration=info "$BINARY" submit \
    --node-addr "$ADDR" \
    --tps 500 \
    --sender-index 1 \
    --duration-secs 20 \
    --stats-file "$OUT_DIR/medium.json" \
    --max-fee-multiplier 3 \
    --priority-fee-multiplier 2 \
    > "$OUT_DIR/medium.log" 2>&1 &
echo "  Medium peer: 500 tps, 20s duration"

# Long-lived peer (40s - full duration)
RUST_LOG=monad_tx_integration=info "$BINARY" submit \
    --node-addr "$ADDR" \
    --tps 500 \
    --sender-index 2 \
    --duration-secs 40 \
    --stats-file "$OUT_DIR/long.json" \
    --max-fee-multiplier 3 \
    --priority-fee-multiplier 2 \
    > "$OUT_DIR/long.log" 2>&1 &
echo "  Long peer: 500 tps, 40s duration"

sleep 45
wait $NODE_PID 2>/dev/null || true

echo "=== Results ==="
python3 << 'PYEOF'
import json

out_dir = "/tmp/tx-experiment-asymmetric"

blocks = []
with open(f"{out_dir}/node-stats.jsonl") as f:
    for line in f:
        if line.strip():
            blocks.append(json.loads(line))

t0 = blocks[0]["timestamp_ms"]

print("\n" + "=" * 70)
print("ASYMMETRIC DURATION ANALYSIS")
print("=" * 70)

# Track scores over time for each peer
print(f"\n### Score Evolution (3 peers with 10s, 20s, 40s durations)")
print(f"{'Time(s)':>8} {'Short':>12} {'Medium':>12} {'Long':>12} {'Peers':>6}")
print("-" * 56)

checkpoints = [5, 10, 15, 20, 25, 30, 35, 40]
for t_target in checkpoints:
    block_idx = int(t_target * 2)  # ~2 blocks/sec
    if block_idx >= len(blocks):
        continue
    b = blocks[block_idx]
    t = (b["timestamp_ms"] - t0) / 1000
    peers = b["peers"]

    # Sort by txs to identify: long > medium > short
    sorted_p = sorted(peers, key=lambda p: p["txs_received_total"], reverse=True)

    scores = [p["score"] for p in sorted_p[:3]] + [0, 0, 0]
    print(f"{t:>8.1f} {scores[2]:>12.0f} {scores[1]:>12.0f} {scores[0]:>12.0f} {len(peers):>6}")

# Final stats
print(f"\n### Final Peer Stats")
final = blocks[-1]["peers"]
for p in sorted(final, key=lambda x: x["txs_received_total"], reverse=True):
    rate = p["txs_included_total"] / p["txs_received_total"] * 100 if p["txs_received_total"] > 0 else 0
    print(f"  {p['peer_id'][:20]}: recv={p['txs_received_total']:>6}, incl={p['txs_included_total']:>6}, rate={rate:.1f}%, score={p['score']:.0f}")

# Bandwidth share evolution
print(f"\n### Bandwidth Share Over Time")
print(f"  t=0-10s: 3 peers competing")
print(f"  t=10-20s: 2 peers (short left)")
print(f"  t=20-40s: 1 peer (medium left)")

PYEOF

echo ""
echo "=== Results written to $OUT_DIR ==="
