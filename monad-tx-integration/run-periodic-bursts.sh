#!/usr/bin/env bash
set -euo pipefail

# Periodic Bursts Experiment
# Oscillating load: 5s high (5000 tps), 5s low (500 tps)
# 4 cycles over 40 seconds
# Goal: Verify consistent behavior under variable load

BINARY="$(dirname "$0")/../target/release/monad-tx-integration"
OUT_DIR="${1:-/tmp/tx-experiment-periodic}"
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
    --duration-secs 50 \
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

# Baseline peer runs continuously at moderate rate
echo "=== Starting baseline peer (continuous) ==="
RUST_LOG=monad_tx_integration=info "$BINARY" submit \
    --node-addr "$ADDR" \
    --tps 300 \
    --sender-index 0 \
    --duration-secs 45 \
    --stats-file "$OUT_DIR/baseline.json" \
    --max-fee-multiplier 3 \
    --priority-fee-multiplier 2 \
    > "$OUT_DIR/baseline.log" 2>&1 &
echo "  Baseline: 300 tps continuous"

# 4 burst cycles
for cycle in 1 2 3 4; do
    echo "=== Cycle $cycle: HIGH load (5s) ==="
    # Spawn 10 burst peers
    for i in $(seq 1 10); do
        idx=$((cycle * 100 + i))
        RUST_LOG=monad_tx_integration=info "$BINARY" submit \
            --node-addr "$ADDR" \
            --tps 400 \
            --sender-index "$idx" \
            --duration-secs 5 \
            --stats-file "$OUT_DIR/burst-c${cycle}-${i}.json" \
            --max-fee-multiplier 1 \
            --priority-fee-multiplier 0 \
            > "$OUT_DIR/burst-c${cycle}-${i}.log" 2>&1 &
    done
    sleep 5

    echo "=== Cycle $cycle: LOW load (5s) ==="
    sleep 5
done

echo "=== Waiting for processes ==="
wait $NODE_PID 2>/dev/null || true

echo "=== Results ==="
python3 << 'PYEOF'
import json

out_dir = "/tmp/tx-experiment-periodic"

blocks = []
with open(f"{out_dir}/node-stats.jsonl") as f:
    for line in f:
        if line.strip():
            blocks.append(json.loads(line))

t0 = blocks[0]["timestamp_ms"]

print("\n" + "=" * 70)
print("PERIODIC BURSTS ANALYSIS")
print("=" * 70)

# Analyze by 5-second windows
print(f"\n### Throughput by Phase")
print(f"{'Phase':>12} {'Time':>12} {'Recv':>10} {'Incl':>10} {'TPS':>8}")
print("-" * 56)

# Each cycle is 10s: 5s high + 5s low
for cycle in range(4):
    high_start = cycle * 20  # block index
    high_end = high_start + 10
    low_start = high_end
    low_end = low_start + 10

    if high_end <= len(blocks):
        high_blocks = blocks[high_start:high_end]
        high_incl = sum(b["txs_included"] for b in high_blocks)
        high_recv = high_blocks[-1]["txs_received_total"] - (blocks[high_start-1]["txs_received_total"] if high_start > 0 else 0)
        t = (high_blocks[0]["timestamp_ms"] - t0) / 1000
        print(f"{'Cycle '+str(cycle+1)+' HIGH':>12} {t:>10.1f}s {high_recv:>10} {high_incl:>10} {high_incl/5:>8.0f}")

    if low_end <= len(blocks):
        low_blocks = blocks[low_start:low_end]
        low_incl = sum(b["txs_included"] for b in low_blocks)
        low_recv = low_blocks[-1]["txs_received_total"] - blocks[low_start-1]["txs_received_total"]
        t = (low_blocks[0]["timestamp_ms"] - t0) / 1000
        print(f"{'Cycle '+str(cycle+1)+' LOW':>12} {t:>10.1f}s {low_recv:>10} {low_incl:>10} {low_incl/5:>8.0f}")

# Total stats
total_recv = blocks[-1]["txs_received_total"]
total_incl = sum(b["txs_included"] for b in blocks)
duration = (blocks[-1]["timestamp_ms"] - t0) / 1000

print(f"\n### Overall")
print(f"  Total received: {total_recv:,}")
print(f"  Total included: {total_incl:,}")
print(f"  Inclusion rate: {total_incl/total_recv*100:.1f}%" if total_recv > 0 else "  N/A")
print(f"  Effective TPS: {total_incl/duration:.0f}")

# Check for stalls
stall_blocks = [i for i, b in enumerate(blocks) if b["txs_included"] == 0 and i > 5]
if stall_blocks:
    print(f"\n  ⚠️ Stall detected at blocks: {stall_blocks[:10]}...")
else:
    print(f"\n  ✓ No stalls detected")

PYEOF

echo ""
echo "=== Results written to $OUT_DIR ==="
