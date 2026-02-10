#!/usr/bin/env bash
set -euo pipefail

# Time Weight Exploitation Experiment
# Test quadratic time weight advantage
# Early peer starts at t=0, late peer starts at t=20s
# Both have identical TPS and fees
# Goal: Verify established identities maintain advantage

BINARY="$(dirname "$0")/../target/release/monad-tx-integration"
OUT_DIR="${1:-/tmp/tx-experiment-time-weight}"
TOTAL_DURATION=40
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
    --duration-secs "$((TOTAL_DURATION + 5))" \
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

echo "=== Starting EARLY peer at t=0 ==="
RUST_LOG=monad_tx_integration=info "$BINARY" submit \
    --node-addr "$ADDR" \
    --tps 500 \
    --sender-index 0 \
    --duration-secs "$TOTAL_DURATION" \
    --stats-file "$OUT_DIR/early.json" \
    --max-fee-multiplier 3 \
    --priority-fee-multiplier 2 \
    > "$OUT_DIR/early.log" 2>&1 &
EARLY_PID=$!
echo "  Early peer: 500 tps, 3x fee, duration=${TOTAL_DURATION}s"

echo "=== Waiting 20s for time weight to accumulate ==="
sleep 20

echo "=== Starting LATE peer at t=20s ==="
RUST_LOG=monad_tx_integration=info "$BINARY" submit \
    --node-addr "$ADDR" \
    --tps 500 \
    --sender-index 1 \
    --duration-secs 20 \
    --stats-file "$OUT_DIR/late.json" \
    --max-fee-multiplier 3 \
    --priority-fee-multiplier 2 \
    > "$OUT_DIR/late.log" 2>&1 &
LATE_PID=$!
echo "  Late peer: 500 tps, 3x fee, duration=20s (identical params)"

sleep 20

echo "=== Waiting for processes ==="
wait $EARLY_PID 2>/dev/null || true
wait $LATE_PID 2>/dev/null || true
wait $NODE_PID 2>/dev/null || true

echo "=== Results ==="
python3 << 'PYEOF'
import json

out_dir = "/tmp/tx-experiment-time-weight"

blocks = []
with open(f"{out_dir}/node-stats.jsonl") as f:
    for line in f:
        if line.strip():
            blocks.append(json.loads(line))

t0 = blocks[0]["timestamp_ms"]

print("\n" + "=" * 70)
print("TIME WEIGHT EXPLOITATION ANALYSIS")
print("=" * 70)

# Track both peers over time
# Early peer starts at block 0, late peer appears around block 40

print(f"\n### Score Evolution")
print(f"{'Block':>6} {'Time(s)':>8} {'Early Score':>14} {'Late Score':>14} {'Ratio':>8}")
print("-" * 58)

checkpoints = [10, 20, 30, 40, 50, 60, 70, 80]
for i in checkpoints:
    if i >= len(blocks):
        continue
    b = blocks[i]
    t = (b["timestamp_ms"] - t0) / 1000
    peers = b["peers"]

    # Early peer has more txs at early checkpoints
    early = max(peers, key=lambda p: p["txs_received_total"]) if peers else None
    late = min(peers, key=lambda p: p["txs_received_total"]) if len(peers) > 1 else None

    if early and late and early != late:
        ratio = early["score"] / late["score"] if late["score"] > 0 else float('inf')
        print(f"{i:>6} {t:>8.1f} {early['score']:>14.0f} {late['score']:>14.0f} {ratio:>8.1f}x")
    elif early:
        print(f"{i:>6} {t:>8.1f} {early['score']:>14.0f} {'N/A':>14} {'N/A':>8}")

# Final comparison
final = blocks[-1]["peers"]
if len(final) >= 2:
    sorted_final = sorted(final, key=lambda p: p["txs_received_total"], reverse=True)
    early = sorted_final[0]
    late = sorted_final[1]

    print(f"\n### Final Results (t={len(blocks)*0.5:.0f}s)")
    print(f"  Early peer: recv={early['txs_received_total']}, incl={early['txs_included_total']}, score={early['score']:.0f}")
    print(f"  Late peer:  recv={late['txs_received_total']}, incl={late['txs_included_total']}, score={late['score']:.0f}")

    early_rate = early['txs_included_total'] / early['txs_received_total'] * 100 if early['txs_received_total'] > 0 else 0
    late_rate = late['txs_included_total'] / late['txs_received_total'] * 100 if late['txs_received_total'] > 0 else 0

    print(f"\n### Inclusion Rates")
    print(f"  Early peer: {early_rate:.1f}%")
    print(f"  Late peer: {late_rate:.1f}%")

    if early["score"] > late["score"]:
        advantage = early["score"] / late["score"]
        print(f"\n  ✓ Early peer has {advantage:.1f}x score advantage from time weight")
    else:
        print(f"\n  ✗ Late peer caught up - time weight may not be effective")

PYEOF

echo ""
echo "=== Results written to $OUT_DIR ==="
