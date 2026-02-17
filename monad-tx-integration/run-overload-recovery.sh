#!/usr/bin/env bash
set -euo pipefail

# Overload Recovery Experiment
# Phase 1 (0-10s): Normal load (1000 tps)
# Phase 2 (10-25s): Overload (8000 tps from many peers)
# Phase 3 (25-45s): Recovery (back to 1000 tps)
# Goal: Verify graceful degradation and full recovery

BINARY="$(dirname "$0")/../target/release/monad-tx-integration"
OUT_DIR="${1:-/tmp/tx-experiment-overload-recovery}"
COMMIT_INTERVAL_MS=500
NUM_ACCOUNTS=2000

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
    --duration-secs 55 \
    > "$OUT_DIR/node.log" 2>&1 &
NODE_PID=$!

ADDR=""
RPC_ADDR=""
for i in $(seq 1 50); do
    ADDR=$(grep -oP 'CONNECT_ADDR=\K.*' "$OUT_DIR/node.log" 2>/dev/null || true)
    RPC_ADDR=$(grep -oP 'RPC_ADDR=\K.*' "$OUT_DIR/node.log" 2>/dev/null || true)
    [ -n "$ADDR" ] && [ -n "$RPC_ADDR" ] && break
    sleep 0.1
done

if [ -z "$ADDR" ]; then
    echo "ERROR: Node failed to start"
    kill $NODE_PID 2>/dev/null
    exit 1
fi
echo "Node listening on $ADDR, RPC on $RPC_ADDR"

echo "=== Phase 1: Normal load (0-10s) ==="
# 2 baseline peers
for i in 0 1; do
    RUST_LOG=monad_tx_integration=info "$BINARY" submit \
        --node-addr "$ADDR" \
        --rpc-addr "$RPC_ADDR" \
        --tps 400 \
        --sender-index "$i" \
        --duration-secs 50 \
        --stats-file "$OUT_DIR/baseline-$i.json" \
        --max-fee-multiplier 5 \
        --priority-fee-multiplier 3 \
        > "$OUT_DIR/baseline-$i.log" 2>&1 &
    echo "  Baseline B$i: 400 tps, 5x fee (full duration)"
done

sleep 10

echo "=== Phase 2: OVERLOAD (10-25s) ==="
# Spawn 15 overload peers
for i in $(seq 10 24); do
    RUST_LOG=monad_tx_integration=info "$BINARY" submit \
        --node-addr "$ADDR" \
        --rpc-addr "$RPC_ADDR" \
        --tps 500 \
        --sender-index "$i" \
        --duration-secs 15 \
        --stats-file "$OUT_DIR/overload-$i.json" \
        --max-fee-multiplier 1 \
        --priority-fee-multiplier 0 \
        > "$OUT_DIR/overload-$i.log" 2>&1 &
done
echo "  Spawned 15 overload peers: 500 tps each (7500 tps total)"

sleep 15

echo "=== Phase 3: Recovery (25-50s) ==="
echo "  Overload peers finished, baseline peers continue"

sleep 25

echo "=== Waiting for processes ==="
wait $NODE_PID 2>/dev/null || true

echo "=== Results ==="
python3 << 'PYEOF'
import json

out_dir = "/tmp/tx-experiment-overload-recovery"

blocks = []
with open(f"{out_dir}/node-stats.jsonl") as f:
    for line in f:
        if line.strip():
            blocks.append(json.loads(line))

t0 = blocks[0]["timestamp_ms"]

print("\n" + "=" * 70)
print("OVERLOAD RECOVERY ANALYSIS")
print("=" * 70)

def analyze_window(start_s, end_s, name):
    start_block = int(start_s * 2)
    end_block = int(end_s * 2)
    window = blocks[start_block:min(end_block, len(blocks))]
    if not window:
        return

    incl = sum(b["txs_included"] for b in window)
    recv_start = blocks[start_block-1]["txs_received_total"] if start_block > 0 else 0
    recv_end = window[-1]["txs_received_total"]
    recv = recv_end - recv_start
    duration = end_s - start_s
    tps = incl / duration if duration > 0 else 0
    stalls = sum(1 for b in window if b["txs_included"] == 0)

    print(f"\n### {name} (t={start_s}-{end_s}s)")
    print(f"  Txs received: {recv:,}")
    print(f"  Txs included: {incl:,}")
    print(f"  Effective TPS: {tps:.0f}")
    print(f"  Blocks with 0 txs: {stalls}/{len(window)}")

analyze_window(0, 10, "Phase 1: Normal")
analyze_window(10, 25, "Phase 2: Overload")
analyze_window(25, 50, "Phase 3: Recovery")

# Check final backlog
total_recv = blocks[-1]["txs_received_total"]
total_incl = sum(b["txs_included"] for b in blocks)
backlog = total_recv - total_incl

print(f"\n### Overall")
print(f"  Total received: {total_recv:,}")
print(f"  Total included: {total_incl:,}")
print(f"  Final backlog: {backlog:,}")

if backlog > 1000:
    print(f"  ⚠️ Large backlog remaining - recovery incomplete")
else:
    print(f"  ✓ Backlog cleared - full recovery")

# Check for stalls in recovery phase
recovery_blocks = blocks[50:100]  # t=25-50s
recovery_stalls = sum(1 for b in recovery_blocks if b["txs_included"] == 0)
if recovery_stalls > len(recovery_blocks) * 0.5:
    print(f"  ❌ CRITICAL: System stalled during recovery ({recovery_stalls} stall blocks)")
else:
    print(f"  ✓ System remained responsive during recovery")

PYEOF

echo ""
echo "=== Results written to $OUT_DIR ==="
