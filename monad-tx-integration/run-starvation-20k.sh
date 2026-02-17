#!/usr/bin/env bash
set -euo pipefail

# Queue Starvation Test @ ~20k TPS
# 1 victim + many identical-fee aggressors
# Goal: measure queue-level starvation without fee bias

BINARY="$(dirname "$0")/../target/release/monad-tx-integration"
OUT_DIR="${1:-/tmp/tx-experiment-starvation-20k}"

COMMIT_INTERVAL_MS=500
NUM_ACCOUNTS="${NUM_ACCOUNTS:-5000}"
DURATION="${DURATION:-45}"
SUBMIT_DURATION="${SUBMIT_DURATION:-40}"
PROPOSAL_TX_LIMIT="${PROPOSAL_TX_LIMIT:-20000}"
PROPOSAL_GAS_LIMIT="${PROPOSAL_GAS_LIMIT:-10000000000}"
PROPOSAL_BYTE_LIMIT="${PROPOSAL_BYTE_LIMIT:-100000000}"
PROMOTION_THRESHOLD=1500000.0

VICTIM_TPS="${VICTIM_TPS:-200}"
AGGRESSOR_PEERS="${AGGRESSOR_PEERS:-49}"
AGGRESSOR_TPS="${AGGRESSOR_TPS:-404}"
TOTAL_INPUT_TPS=$((VICTIM_TPS + AGGRESSOR_PEERS * AGGRESSOR_TPS))

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

echo "=== Building ==="
cargo build -p monad-tx-integration --release 2>&1 | tail -3

echo "=== Starting node (proposal_tx_limit=$PROPOSAL_TX_LIMIT, proposal_gas_limit=$PROPOSAL_GAS_LIMIT, proposal_byte_limit=$PROPOSAL_BYTE_LIMIT) ==="
RUST_LOG=monad_tx_integration=info "$BINARY" node \
    --listen 0.0.0.0:0 \
    --rpc-listen 127.0.0.1:0 \
    --commit-interval-ms "$COMMIT_INTERVAL_MS" \
    --stats-file "$OUT_DIR/node-stats.jsonl" \
    --num-accounts "$NUM_ACCOUNTS" \
    --duration-secs "$DURATION" \
    --proposal-tx-limit "$PROPOSAL_TX_LIMIT" \
    --proposal-gas-limit "$PROPOSAL_GAS_LIMIT" \
    --proposal-byte-limit "$PROPOSAL_BYTE_LIMIT" \
    --promotion-threshold "$PROMOTION_THRESHOLD" \
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

if [ -z "$ADDR" ] || [ -z "$RPC_ADDR" ]; then
    echo "ERROR: Node failed to start"
    tail -50 "$OUT_DIR/node.log" || true
    kill "$NODE_PID" 2>/dev/null || true
    exit 1
fi
echo "Node listening on $ADDR, RPC on $RPC_ADDR"

echo "=== Starting victim peer ==="
RUST_LOG=monad_tx_integration=info "$BINARY" submit \
    --node-addr "$ADDR" \
    --rpc-addr "$RPC_ADDR" \
    --tps "$VICTIM_TPS" \
    --sender-index 0 \
    --duration-secs "$SUBMIT_DURATION" \
    --stats-file "$OUT_DIR/victim.json" \
    --max-fee-multiplier 2 \
    --priority-fee-multiplier 1 \
    > "$OUT_DIR/victim.log" 2>&1 &
echo "  Victim: ${VICTIM_TPS} tps, 2x fee, 1x priority"

echo "=== Starting $AGGRESSOR_PEERS aggressors ==="
for i in $(seq 1 "$AGGRESSOR_PEERS"); do
    RUST_LOG=monad_tx_integration=info "$BINARY" submit \
        --node-addr "$ADDR" \
        --rpc-addr "$RPC_ADDR" \
        --tps "$AGGRESSOR_TPS" \
        --sender-index "$i" \
        --duration-secs "$SUBMIT_DURATION" \
        --stats-file "$OUT_DIR/aggressor-$i.json" \
        --max-fee-multiplier 2 \
        --priority-fee-multiplier 1 \
        > "$OUT_DIR/aggressor-$i.log" 2>&1 &
done
echo "  Aggressors: ${AGGRESSOR_PEERS} x ${AGGRESSOR_TPS} tps, 2x fee, 1x priority"
echo "  Total input TPS: $TOTAL_INPUT_TPS"

sleep "$DURATION"
wait "$NODE_PID" 2>/dev/null || true

echo "=== Results ==="
OUT_DIR_ENV="$OUT_DIR" python3 << 'PYEOF'
import json
import os
import sys

out_dir = os.environ["OUT_DIR_ENV"]
stats_path = f"{out_dir}/node-stats.jsonl"

if not os.path.exists(stats_path):
    print(f"ERROR: missing {stats_path}")
    sys.exit(1)

blocks = []
with open(stats_path) as f:
    for line in f:
        line = line.strip()
        if line:
            blocks.append(json.loads(line))

if not blocks:
    print("ERROR: no block stats recorded")
    sys.exit(1)

t0 = blocks[0]["timestamp_ms"]
duration = (blocks[-1]["timestamp_ms"] - t0) / 1000 if len(blocks) > 1 else 0
total_recv = blocks[-1]["txs_received_total"]
total_incl = sum(b["txs_included"] for b in blocks)
effective_tps = total_incl / duration if duration > 0 else 0
input_tps = total_recv / duration if duration > 0 else 0

final_peers = blocks[-1]["peers"]
if not final_peers:
    print("ERROR: no peers in final snapshot")
    sys.exit(1)

# Victim sender_index=0 has the lowest target TPS in this harness.
victim = min(final_peers, key=lambda p: p["txs_received_total"])
victim_id = victim["peer_id"]
aggressors = [p for p in final_peers if p["peer_id"] != victim_id]

victim_recv = victim["txs_received_total"]
victim_incl = victim["txs_included_total"]
victim_rate = (victim_incl / victim_recv * 100) if victim_recv > 0 else 0
victim_share = (victim_incl / total_incl * 100) if total_incl > 0 else 0

aggr_recv = sum(p["txs_received_total"] for p in aggressors)
aggr_incl = sum(p["txs_included_total"] for p in aggressors)
aggr_rate = (aggr_incl / aggr_recv * 100) if aggr_recv > 0 else 0
aggr_share = (aggr_incl / total_incl * 100) if total_incl > 0 else 0

victim_sent = 0
aggr_sent = 0
try:
    with open(f"{out_dir}/victim.json") as f:
        victim_sent = int(json.load(f).get("total_sent", 0))
except Exception:
    victim_sent = 0

for i in range(1, 500):
    path = f"{out_dir}/aggressor-{i}.json"
    if not os.path.exists(path):
        continue
    try:
        with open(path) as f:
            aggr_sent += int(json.load(f).get("total_sent", 0))
    except Exception:
        pass

victim_accept = (victim_recv / victim_sent * 100) if victim_sent > 0 else 0
aggr_accept = (aggr_recv / aggr_sent * 100) if aggr_sent > 0 else 0

longest_zero_streak = 0
current_zero_streak = 0
for b in blocks:
    entry = next((p for p in b["peers"] if p["peer_id"] == victim_id), None)
    incl_this_block = entry["txs_included_this_block"] if entry else 0
    if incl_this_block == 0:
        current_zero_streak += 1
        longest_zero_streak = max(longest_zero_streak, current_zero_streak)
    else:
        current_zero_streak = 0

print("\n" + "=" * 72)
print("STARVATION TEST @ ~20K TPS")
print("=" * 72)

print("\n### Load & Throughput")
print(f"  Duration: {duration:.1f}s")
print(f"  Total received: {total_recv:,}")
print(f"  Total included: {total_incl:,}")
print(f"  Effective TPS: {effective_tps:,.0f}")
print(f"  Input TPS: {input_tps:,.0f}")

print("\n### Queue Admission")
if victim_sent > 0:
    print(f"  Victim sent/recv: {victim_sent:,} / {victim_recv:,} ({victim_accept:.2f}% admitted)")
else:
    print(f"  Victim sent/recv: ? / {victim_recv:,}")
if aggr_sent > 0:
    print(f"  Aggressors sent/recv: {aggr_sent:,} / {aggr_recv:,} ({aggr_accept:.2f}% admitted)")
else:
    print(f"  Aggressors sent/recv: ? / {aggr_recv:,}")

print("\n### Inclusion (sanity)")
print(f"  Victim promoted: {victim['promoted']}")
print(f"  Victim score: {victim['score']:.0f}")
print(f"  Victim recv/incl: {victim_recv:,} / {victim_incl:,}")
print(f"  Victim inclusion rate: {victim_rate:.2f}%")
print(f"  Victim inclusion share: {victim_share:.2f}%")
print(f"  Victim longest zero-include streak: {longest_zero_streak} blocks ({longest_zero_streak * 0.5:.1f}s)")
print(f"  Aggressors recv/incl: {aggr_recv:,} / {aggr_incl:,}")
print(f"  Aggressors inclusion rate: {aggr_rate:.2f}%")
print(f"  Aggressors inclusion share: {aggr_share:.2f}%")

print("\n### Verdict")
if victim_sent > 0 and aggr_sent > 0 and aggr_accept > 0:
    relative = victim_accept / aggr_accept
    print(f"  Relative admission (victim/aggressors): {relative:.2f}x")
    if relative < 0.20:
        print("  CRITICAL: queue-level starvation (victim admission far below aggressors)")
    elif relative < 0.50:
        print("  SEVERE: strong queue starvation pressure")
    elif relative < 0.80:
        print("  PARTIAL: moderate queue starvation pressure")
    else:
        print("  PASS: no severe queue-level starvation under this configuration")
else:
    if victim_incl == 0:
        print("  CRITICAL: victim got 0 included txs")
    elif victim_rate < 5.0 or victim_share < 1.0:
        print("  SEVERE: victim is effectively starved")
    elif victim_rate < 20.0 or victim_share < 5.0:
        print("  PARTIAL: strong starvation pressure observed")
    else:
        print("  PASS: no severe starvation under this configuration")
PYEOF

echo ""
echo "=== Results written to $OUT_DIR ==="
