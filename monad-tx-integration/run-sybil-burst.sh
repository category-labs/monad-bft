#!/usr/bin/env bash
set -euo pipefail

# Sybil Burst Attack Experiment
# Phase 1 (0-15s): 3 honest peers establish reputation
# Phase 2 (15-30s): 20 sybil identities spawn simultaneously + honest peers continue
# Goal: Verify honest peers maintain >50% bandwidth share despite 20:3 identity ratio

BINARY="$(dirname "$0")/../target/release/monad-tx-integration"
OUT_DIR="${1:-/tmp/tx-experiment-sybil-burst}"
TOTAL_DURATION=35
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

# Wait for listen address
ADDR=""
for i in $(seq 1 50); do
    ADDR=$(grep -oP 'LISTEN_ADDR=\K.*' "$OUT_DIR/node.log" 2>/dev/null || true)
    [ -n "$ADDR" ] && break
    sleep 0.1
done

if [ -z "$ADDR" ]; then
    echo "ERROR: Node failed to start"
    cat "$OUT_DIR/node.log"
    kill $NODE_PID 2>/dev/null
    exit 1
fi
echo "Node listening on $ADDR (PID $NODE_PID)"

HONEST_PIDS=()
SYBIL_PIDS=()

echo "=== Phase 1: Starting 3 honest peers (run for full duration) ==="
# Honest peers: high TPS, high fees, run for full duration
for i in 0 1 2; do
    RUST_LOG=monad_tx_integration=info "$BINARY" submit \
        --node-addr "$ADDR" \
        --tps 500 \
        --sender-index "$i" \
        --duration-secs "$TOTAL_DURATION" \
        --stats-file "$OUT_DIR/honest-$i.json" \
        --max-fee-multiplier 5 \
        --priority-fee-multiplier 3 \
        > "$OUT_DIR/honest-$i.log" 2>&1 &
    HONEST_PIDS+=($!)
    echo "  Honest H$i: 500 tps, 5x max_fee (PID ${HONEST_PIDS[-1]})"
done

echo "=== Phase 1: Honest peers building reputation for 15s ==="
sleep 15

echo "=== Phase 2: Spawning 20 sybil identities ==="
# Sybils: moderate TPS, low fees, start at t=15s, run for 20s
for i in $(seq 10 29); do
    RUST_LOG=monad_tx_integration=info "$BINARY" submit \
        --node-addr "$ADDR" \
        --tps 300 \
        --sender-index "$i" \
        --duration-secs 20 \
        --stats-file "$OUT_DIR/sybil-$i.json" \
        --max-fee-multiplier 1 \
        --priority-fee-multiplier 0 \
        > "$OUT_DIR/sybil-$i.log" 2>&1 &
    SYBIL_PIDS+=($!)
done
echo "  Spawned 20 sybils (S10-S29): 300 tps each, 1x max_fee"

# Total sybil TPS: 20 * 300 = 6000 tps
# Total honest TPS: 3 * 500 = 1500 tps
# Ratio: 6000:1500 = 4:1 sybil:honest by volume

echo "=== Phase 2: Running with sybils for 20s ==="
sleep 20

echo "=== Waiting for all processes to finish ==="
for pid in "${HONEST_PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
done
for pid in "${SYBIL_PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
done

wait "$NODE_PID" 2>/dev/null || true

echo ""
echo "=== Honest Peer Stats ==="
for i in 0 1 2; do
    if [ -f "$OUT_DIR/honest-$i.json" ]; then
        echo "--- H$i ---"
        cat "$OUT_DIR/honest-$i.json"
        echo ""
    fi
done

echo "=== Sybil Stats Summary ==="
SYBIL_TOTAL_SENT=0
SYBIL_COUNT=0
for i in $(seq 10 29); do
    if [ -f "$OUT_DIR/sybil-$i.json" ]; then
        sent=$(jq -r '.total_sent' "$OUT_DIR/sybil-$i.json")
        SYBIL_TOTAL_SENT=$((SYBIL_TOTAL_SENT + sent))
        SYBIL_COUNT=$((SYBIL_COUNT + 1))
    fi
done
echo "Total sybils: $SYBIL_COUNT"
echo "Total sybil txs sent: $SYBIL_TOTAL_SENT"

echo ""
echo "=== Node Stats (last 5 blocks) ==="
tail -5 "$OUT_DIR/node-stats.jsonl"

echo ""
echo "=== Results written to $OUT_DIR ==="
ls -la "$OUT_DIR/"

# Summary analysis
python3 << 'PYEOF'
import json
import os

out_dir = os.environ.get("OUT_DIR", "/tmp/tx-experiment-sybil-burst")

# Load blocks
blocks = []
with open(f"{out_dir}/node-stats.jsonl") as f:
    for line in f:
        if line.strip():
            blocks.append(json.loads(line))

# Identify honest vs sybil peers from final block
final_peers = blocks[-1]["peers"] if blocks else []

# Honest peers have higher scores (started earlier, more time weight)
# Sort by score to identify
sorted_peers = sorted(final_peers, key=lambda p: p["score"], reverse=True)

# Top 3 by score are likely honest (they had 15s head start)
honest_peers = sorted_peers[:3] if len(sorted_peers) >= 3 else sorted_peers
sybil_peers = sorted_peers[3:] if len(sorted_peers) > 3 else []

honest_included = sum(p["txs_included_total"] for p in honest_peers)
sybil_included = sum(p["txs_included_total"] for p in sybil_peers)
total_included = honest_included + sybil_included

honest_recv = sum(p["txs_received_total"] for p in honest_peers)
sybil_recv = sum(p["txs_received_total"] for p in sybil_peers)

print("\n" + "=" * 70)
print("SYBIL BURST ATTACK ANALYSIS")
print("=" * 70)
print(f"\n### Honest Peers (top 3 by score)")
for p in honest_peers:
    rate = p["txs_included_total"] / p["txs_received_total"] * 100 if p["txs_received_total"] > 0 else 0
    print(f"  {p['peer_id'][:25]}: recv={p['txs_received_total']:>6}, incl={p['txs_included_total']:>6}, rate={rate:.1f}%, score={p['score']:.0f}")

print(f"\n### Sybil Peers ({len(sybil_peers)} identities)")
if sybil_peers:
    sybil_rates = [p["txs_included_total"] / p["txs_received_total"] * 100 if p["txs_received_total"] > 0 else 0 for p in sybil_peers]
    avg_sybil_rate = sum(sybil_rates) / len(sybil_rates)
    min_score = min(p["score"] for p in sybil_peers)
    max_score = max(p["score"] for p in sybil_peers)
    print(f"  Total received: {sybil_recv:,}")
    print(f"  Total included: {sybil_included:,}")
    print(f"  Avg inclusion rate: {avg_sybil_rate:.1f}%")
    print(f"  Score range: {min_score:.0f} - {max_score:.0f}")

print(f"\n### Bandwidth Share")
if total_included > 0:
    honest_share = honest_included / total_included * 100
    sybil_share = sybil_included / total_included * 100
    print(f"  Honest share: {honest_share:.1f}% ({honest_included:,} txs)")
    print(f"  Sybil share: {sybil_share:.1f}% ({sybil_included:,} txs)")
    print(f"  Identity ratio: 3 honest vs {len(sybil_peers)} sybils")
    print(f"  Volume ratio: {honest_recv}:{sybil_recv} (honest:sybil)")

print(f"\n### Defense Effectiveness")
if honest_recv > 0 and sybil_recv > 0:
    honest_rate = honest_included / honest_recv * 100
    sybil_rate = sybil_included / sybil_recv * 100
    print(f"  Honest inclusion rate: {honest_rate:.1f}%")
    print(f"  Sybil inclusion rate: {sybil_rate:.1f}%")
    if honest_rate > sybil_rate:
        print(f"  ✓ Honest peers have {honest_rate/sybil_rate:.1f}x advantage")
    else:
        print(f"  ✗ Sybils have advantage - defense may be insufficient")

PYEOF
