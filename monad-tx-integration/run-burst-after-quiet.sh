#!/usr/bin/env bash
set -euo pipefail

# Burst-After-Quiet Experiment
# Phase 1 (0-10s): Light load from 2 established peers
# Phase 2 (10-20s): Sudden burst from 15 new peers
# Phase 3 (20-35s): Burst ends, original peers continue
# Goal: Verify graceful handling of sudden load spike and recovery

BINARY="$(dirname "$0")/../target/release/monad-tx-integration"
OUT_DIR="${1:-/tmp/tx-experiment-burst}"
TOTAL_DURATION=40
COMMIT_INTERVAL_MS=500
NUM_ACCOUNTS=1000

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
    --duration-secs "$((TOTAL_DURATION + 5))" \
    > "$OUT_DIR/node.log" 2>&1 &
NODE_PID=$!

# Wait for listen address
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
    cat "$OUT_DIR/node.log"
    kill $NODE_PID 2>/dev/null
    exit 1
fi
echo "Node listening on $ADDR, RPC on $RPC_ADDR (PID $NODE_PID)"

ESTABLISHED_PIDS=()
BURST_PIDS=()

echo "=== Phase 1: Starting 2 established peers (light load, full duration) ==="
for i in 0 1; do
    RUST_LOG=monad_tx_integration=info "$BINARY" submit \
        --node-addr "$ADDR" \
        --rpc-addr "$RPC_ADDR" \
        --tps 300 \
        --sender-index "$i" \
        --duration-secs "$TOTAL_DURATION" \
        --stats-file "$OUT_DIR/established-$i.json" \
        --max-fee-multiplier 3 \
        --priority-fee-multiplier 2 \
        > "$OUT_DIR/established-$i.log" 2>&1 &
    ESTABLISHED_PIDS+=($!)
    echo "  Established E$i: 300 tps, 3x max_fee (PID ${ESTABLISHED_PIDS[-1]})"
done

# Total Phase 1 load: 600 tps (light)

echo "=== Phase 1: Light load for 10s ==="
sleep 10

echo "=== Phase 2: BURST - Spawning 15 burst peers ==="
# Each burst peer: 500 tps, 1x fee (cheap flood)
for i in $(seq 10 24); do
    RUST_LOG=monad_tx_integration=info "$BINARY" submit \
        --node-addr "$ADDR" \
        --rpc-addr "$RPC_ADDR" \
        --tps 500 \
        --sender-index "$i" \
        --duration-secs 10 \
        --stats-file "$OUT_DIR/burst-$i.json" \
        --max-fee-multiplier 1 \
        --priority-fee-multiplier 0 \
        > "$OUT_DIR/burst-$i.log" 2>&1 &
    BURST_PIDS+=($!)
done
echo "  Spawned 15 burst peers (B10-B24): 500 tps each, 1x max_fee"
echo "  Total burst TPS: 7500 + 600 established = 8100 tps"

echo "=== Phase 2: Burst for 10s ==="
sleep 10

echo "=== Phase 3: Burst ends, recovery for 20s ==="
sleep 20

echo "=== Waiting for all processes to finish ==="
for pid in "${ESTABLISHED_PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
done
for pid in "${BURST_PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
done
wait "$NODE_PID" 2>/dev/null || true

echo ""
echo "=== Established Peer Stats ==="
for i in 0 1; do
    if [ -f "$OUT_DIR/established-$i.json" ]; then
        echo "--- E$i ---"
        cat "$OUT_DIR/established-$i.json"
        echo ""
    fi
done

echo "=== Burst Summary ==="
BURST_TOTAL=0
for i in $(seq 10 24); do
    if [ -f "$OUT_DIR/burst-$i.json" ]; then
        sent=$(jq -r '.total_sent' "$OUT_DIR/burst-$i.json")
        BURST_TOTAL=$((BURST_TOTAL + sent))
    fi
done
echo "Total burst txs sent: $BURST_TOTAL"

echo ""
echo "=== Results written to $OUT_DIR ==="

# Analysis
python3 << 'PYEOF'
import json
import os

out_dir = "/tmp/tx-experiment-burst"

# Load blocks
blocks = []
with open(f"{out_dir}/node-stats.jsonl") as f:
    for line in f:
        if line.strip():
            blocks.append(json.loads(line))

t0 = blocks[0]["timestamp_ms"]

print("\n" + "=" * 70)
print("BURST-AFTER-QUIET ANALYSIS")
print("=" * 70)

# Phase boundaries (approximate)
# Phase 1: 0-10s (blocks 0-20)
# Phase 2: 10-20s (blocks 20-40) - burst
# Phase 3: 20-40s (blocks 40-80) - recovery

def analyze_phase(blocks, start_block, end_block, phase_name):
    phase_blocks = blocks[start_block:min(end_block, len(blocks))]
    if not phase_blocks:
        return

    total_incl = sum(b["txs_included"] for b in phase_blocks)
    avg_incl = total_incl / len(phase_blocks) if phase_blocks else 0

    # Get start and end recv
    start_recv = phase_blocks[0]["txs_received_total"]
    end_recv = phase_blocks[-1]["txs_received_total"]
    recv_delta = end_recv - (blocks[start_block-1]["txs_received_total"] if start_block > 0 else 0)

    # Time span
    t_start = (phase_blocks[0]["timestamp_ms"] - t0) / 1000
    t_end = (phase_blocks[-1]["timestamp_ms"] - t0) / 1000

    # Peer count at end of phase
    peer_count = len(phase_blocks[-1]["peers"]) if phase_blocks else 0

    print(f"\n### {phase_name} (t={t_start:.1f}s to t={t_end:.1f}s)")
    print(f"  Blocks: {len(phase_blocks)}")
    print(f"  Txs received: {recv_delta:,}")
    print(f"  Txs included: {total_incl:,}")
    print(f"  Avg txs/block: {avg_incl:.0f}")
    print(f"  Active peers: {peer_count}")

    # Calculate effective TPS
    duration = (t_end - t_start) if t_end > t_start else 1
    eff_tps = total_incl / duration
    print(f"  Effective TPS: {eff_tps:.0f}")

analyze_phase(blocks, 0, 20, "Phase 1: Light Load")
analyze_phase(blocks, 20, 40, "Phase 2: Burst")
analyze_phase(blocks, 40, 80, "Phase 3: Recovery")

# Established peer analysis
print("\n### Established Peer Performance")
# Find peers present from the start (they have highest txs at block 20)
final_peers = blocks[-1]["peers"]
established_peers = sorted(final_peers, key=lambda p: p["txs_received_total"], reverse=True)[:2]

for p in established_peers:
    rate = p["txs_included_total"] / p["txs_received_total"] * 100 if p["txs_received_total"] > 0 else 0
    print(f"  {p['peer_id'][:25]}: recv={p['txs_received_total']:>6}, incl={p['txs_included_total']:>6}, rate={rate:.1f}%, score={p['score']:.0f}")

# Burst peer analysis
burst_peers = [p for p in final_peers if p not in established_peers]
if burst_peers:
    burst_recv = sum(p["txs_received_total"] for p in burst_peers)
    burst_incl = sum(p["txs_included_total"] for p in burst_peers)
    burst_rate = burst_incl / burst_recv * 100 if burst_recv > 0 else 0
    print(f"\n### Burst Peer Performance (15 peers)")
    print(f"  Total received: {burst_recv:,}")
    print(f"  Total included: {burst_incl:,}")
    print(f"  Avg inclusion rate: {burst_rate:.1f}%")

# Throughput over time
print(f"\n### Throughput Timeline")
print(f"{'Block':>6} {'Time(s)':>8} {'Txs/blk':>10} {'Recv':>10} {'Peers':>6}")
print("-" * 48)
checkpoints = [5, 10, 15, 20, 25, 30, 35, 40, 50, 60, 70, 80]
for i in checkpoints:
    if i < len(blocks):
        t = (blocks[i]["timestamp_ms"] - t0) / 1000
        print(f"{i:>6} {t:>8.1f} {blocks[i]['txs_included']:>10} {blocks[i]['txs_received_total']:>10} {len(blocks[i]['peers']):>6}")

# Backlog analysis
print(f"\n### Backlog Analysis")
max_backlog = 0
max_backlog_block = 0
cumulative_incl = 0
for i, b in enumerate(blocks):
    cumulative_incl += b["txs_included"]
    backlog = b["txs_received_total"] - cumulative_incl
    if backlog > max_backlog:
        max_backlog = backlog
        max_backlog_block = i

print(f"  Peak backlog: {max_backlog:,} txs at block {max_backlog_block}")
final_backlog = blocks[-1]["txs_received_total"] - cumulative_incl
print(f"  Final backlog: {final_backlog:,} txs")

PYEOF
