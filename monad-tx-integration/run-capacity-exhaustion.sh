#!/usr/bin/env bash
set -euo pipefail

# Capacity Exhaustion Experiment
# Push toward pool capacity limits (100K global, 10K per-peer)
# Goal: Verify graceful degradation at capacity boundaries

BINARY="$(dirname "$0")/../target/release/monad-tx-integration"
OUT_DIR="${1:-/tmp/tx-experiment-capacity}"
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
    --duration-secs 60 \
    --hard-tx-expiry-secs 10 \
    --soft-tx-expiry-secs 5 \
    > "$OUT_DIR/node.log" 2>&1 &
NODE_PID=$!

ADDR=""
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

echo "=== Phase 1: Ramp up to fill pool (0-20s) ==="
# Start 30 high-volume peers to quickly fill the pool
for i in $(seq 0 29); do
    RUST_LOG=monad_tx_integration=info "$BINARY" submit \
        --node-addr "$ADDR" \
        --rpc-addr "$RPC_ADDR" \
        --tps 200 \
        --sender-index "$i" \
        --duration-secs 20 \
        --stats-file "$OUT_DIR/ramp-$i.json" \
        --max-fee-multiplier 1 \
        --priority-fee-multiplier 0 \
        --nonce-sync-ms 200 \
        > "$OUT_DIR/ramp-$i.log" 2>&1 &
done
echo "  Spawned 30 peers at 200 tps each (6,000 tps total)"

sleep 20

echo "=== Phase 2: Sustained high load (20-40s) ==="
# Continue with 20 peers
for i in $(seq 100 119); do
    RUST_LOG=monad_tx_integration=info "$BINARY" submit \
        --node-addr "$ADDR" \
        --rpc-addr "$RPC_ADDR" \
        --tps 200 \
        --sender-index "$i" \
        --duration-secs 20 \
        --stats-file "$OUT_DIR/sustain-$i.json" \
        --max-fee-multiplier 2 \
        --priority-fee-multiplier 1 \
        --nonce-sync-ms 200 \
        > "$OUT_DIR/sustain-$i.log" 2>&1 &
done
echo "  Spawned 20 more peers at 200 tps (4,000 tps additional)"

sleep 20

echo "=== Phase 3: Wind down (40-55s) ==="
sleep 15

wait $NODE_PID 2>/dev/null || true

echo "=== Results ==="
python3 << 'PYEOF'
import json
import re

out_dir = "/tmp/tx-experiment-capacity"

blocks = []
with open(f"{out_dir}/node-stats.jsonl") as f:
    for line in f:
        if line.strip():
            blocks.append(json.loads(line))

t0 = blocks[0]["timestamp_ms"]

print("\n" + "=" * 70)
print("CAPACITY EXHAUSTION ANALYSIS")
print("=" * 70)

max_peers = max(len(b["peers"]) for b in blocks)
total_recv = blocks[-1]["txs_received_total"]
total_incl = sum(b["txs_included"] for b in blocks)

# Parse pool metrics from node log to distinguish real backlog from transport drops
with open(f"{out_dir}/node.log") as f:
    log_content = f.read()

pool_inserted = 0
pool_nonce_drops = 0
pool_full_drops = 0
pool_tracked = 0
m = re.findall(r'insert_forwarded_txs=(\d+)', log_content)
if m:
    pool_inserted = max(int(x) for x in m)
m = re.findall(r'drop_nonce_too_low=(\d+)', log_content)
if m:
    pool_nonce_drops = max(int(x) for x in m)
m = re.findall(r'drop_pool_full=(\d+)', log_content)
if m:
    pool_full_drops = max(int(x) for x in m)
# txs=N(+M) shows current pool tracked count; take last occurrence
m = re.findall(r'\btxs=(\d+)\(\+', log_content)
if m:
    pool_tracked = int(m[-1])
# Also check evict_expired_txs metric
m_evicted = re.findall(r'evict_expired_txs=(\d+)', log_content)
pool_evicted = max(int(x) for x in m_evicted) if m_evicted else 0

transport_loss = total_recv - pool_inserted - pool_nonce_drops
if transport_loss < 0:
    transport_loss = 0

print(f"\n### Capacity Metrics")
print(f"  Max concurrent peers: {max_peers}")
print(f"  Total txs received: {total_recv:,}")
print(f"  Total txs included: {total_incl:,}")
print(f"  Inclusion rate: {total_incl/total_recv*100:.1f}%")

print(f"\n### Pool Accounting")
print(f"  Inserted into pool: {pool_inserted:,}")
print(f"  Dropped (nonce too low): {pool_nonce_drops:,}")
print(f"  Dropped (pool full): {pool_full_drops:,}")
print(f"  Transport loss: {transport_loss:,} ({transport_loss/total_recv*100:.1f}%)")
print(f"  Evicted (expired): {pool_evicted:,}")
print(f"  Remaining in pool: {pool_tracked:,}")

if pool_full_drops > 0:
    print(f"  ⚠️ Pool reached capacity - txs were dropped")
else:
    print(f"  ✓ Pool never reached capacity")

# Throughput over time
print(f"\n### Throughput Timeline")
print(f"{'Time(s)':>8} {'Recv':>10} {'Incl':>10} {'Peers':>6}")
print("-" * 40)

cumulative = 0
for i in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]:
    if i >= len(blocks):
        continue
    b = blocks[i]
    t = (b["timestamp_ms"] - t0) / 1000
    cumulative = sum(blocks[j]["txs_included"] for j in range(i+1))
    print(f"{t:>8.1f} {b['txs_received_total']:>10,} {cumulative:>10,} {len(b['peers']):>6}")

# Final state: distinguish pool-drained from actual stall
print(f"\n### Final State")
stalls = sum(1 for b in blocks[-20:] if b["txs_included"] == 0)
recv_stopped = blocks[-1]["txs_received_total"] == blocks[-20]["txs_received_total"] if len(blocks) >= 20 else True

if pool_tracked == 0 and recv_stopped:
    print(f"  Pool fully drained (0 txs remaining)")
    print(f"  ✓ System handled capacity stress correctly")
elif stalls > 10 and not recv_stopped:
    print(f"  Pool has {pool_tracked:,} stuck txs while input continues")
    print(f"  ❌ CRITICAL: System stalled under active load")
elif pool_tracked > 0:
    print(f"  Pool has {pool_tracked:,} remaining txs (nonce gaps)")
    print(f"  ⚠️ Residual nonce gaps after submitters stopped")
else:
    print(f"  ✓ System handled capacity stress")

PYEOF

echo ""
echo "=== Results written to $OUT_DIR ==="
