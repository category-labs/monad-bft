#!/usr/bin/env bash
set -euo pipefail

# Score Pump-and-Dump Experiment
# Phase 1 (0-15s): Attacker builds high score with expensive txs at high rate
# Phase 2 (15-30s): Attacker floods with cheap txs while honest peer joins
# Goal: Verify EMA prevents score maintenance with low-cost txs

BINARY="$(dirname "$0")/../target/release/monad-tx-integration"
OUT_DIR="${1:-/tmp/tx-experiment-score-gaming}"
TOTAL_DURATION=35
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
    --duration-secs "$((TOTAL_DURATION + 5))" \
    --ema-half-life-secs 10 \
    > "$OUT_DIR/node.log" 2>&1 &
NODE_PID=$!

# Wait for listen address
ADDR=""
for i in $(seq 1 50); do
    ADDR=$(grep -oP 'CONNECT_ADDR=\K.*' "$OUT_DIR/node.log" 2>/dev/null || true)
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

echo "=== Phase 1: Attacker builds score with HIGH fees (15s) ==="
# Attacker: High TPS, high fees for 15 seconds only
RUST_LOG=monad_tx_integration=info "$BINARY" submit \
    --node-addr "$ADDR" \
    --tps 600 \
    --sender-index 0 \
    --duration-secs 15 \
    --stats-file "$OUT_DIR/attacker-phase1.json" \
    --max-fee-multiplier 10 \
    --priority-fee-multiplier 5 \
    > "$OUT_DIR/attacker-phase1.log" 2>&1 &
ATTACKER_P1_PID=$!
echo "  Attacker Phase 1: 600 tps, 10x max_fee, 5x priority (PID $ATTACKER_P1_PID)"

# Wait for phase 1 to complete
sleep 16

echo "=== Phase 2: Attacker switches to LOW fees, honest peer joins ==="
# Attacker: Continue with LOW fees (trying to coast on reputation)
RUST_LOG=monad_tx_integration=info "$BINARY" submit \
    --node-addr "$ADDR" \
    --tps 800 \
    --sender-index 0 \
    --duration-secs 20 \
    --stats-file "$OUT_DIR/attacker-phase2.json" \
    --max-fee-multiplier 1 \
    --priority-fee-multiplier 0 \
    > "$OUT_DIR/attacker-phase2.log" 2>&1 &
ATTACKER_P2_PID=$!
echo "  Attacker Phase 2: 800 tps, 1x max_fee (trying to coast)"

# Honest peer: Starts fresh but with moderate fees
RUST_LOG=monad_tx_integration=info "$BINARY" submit \
    --node-addr "$ADDR" \
    --tps 600 \
    --sender-index 1 \
    --duration-secs 20 \
    --stats-file "$OUT_DIR/honest.json" \
    --max-fee-multiplier 5 \
    --priority-fee-multiplier 3 \
    > "$OUT_DIR/honest.log" 2>&1 &
HONEST_PID=$!
echo "  Honest: 600 tps, 5x max_fee (starting fresh)"

echo "=== Phase 2: Running for 20s ==="
sleep 20

echo "=== Waiting for all processes to finish ==="
wait "$ATTACKER_P1_PID" 2>/dev/null || true
wait "$ATTACKER_P2_PID" 2>/dev/null || true
wait "$HONEST_PID" 2>/dev/null || true
wait "$NODE_PID" 2>/dev/null || true

echo ""
echo "=== Attacker Phase 1 Stats ==="
cat "$OUT_DIR/attacker-phase1.json" 2>/dev/null || echo "Not available"

echo ""
echo "=== Attacker Phase 2 Stats ==="
cat "$OUT_DIR/attacker-phase2.json" 2>/dev/null || echo "Not available"

echo ""
echo "=== Honest Peer Stats ==="
cat "$OUT_DIR/honest.json" 2>/dev/null || echo "Not available"

echo ""
echo "=== Node Stats (last 5 blocks) ==="
tail -5 "$OUT_DIR/node-stats.jsonl"

echo ""
echo "=== Results written to $OUT_DIR ==="

# Analysis
OUT_DIR_PY="$OUT_DIR" python3 << 'PYEOF'
import json
import os

out_dir = os.environ.get("OUT_DIR_PY", "/tmp/tx-experiment-score-gaming")

# Load blocks
blocks = []
with open(f"{out_dir}/node-stats.jsonl") as f:
    for line in f:
        if line.strip():
            blocks.append(json.loads(line))

# Track attacker and honest peer scores over time
# Attacker: sender_index=0, Honest: sender_index=1
# We'll identify them by looking at earliest activity

# Get block at t=15s (end of phase 1) and t=35s (end of phase 2)
# Assuming ~2 blocks/sec, block 30 is roughly t=15s, block 70 is roughly t=35s
phase1_end_block = min(30, len(blocks)-1)
phase2_end_block = len(blocks) - 1

print("\n" + "=" * 70)
print("SCORE PUMP-AND-DUMP ANALYSIS")
print("=" * 70)

# Find peer with highest txs at phase 1 end (attacker)
if phase1_end_block < len(blocks):
    p1_peers = blocks[phase1_end_block]["peers"]
    attacker_id = max(p1_peers, key=lambda p: p["txs_received_total"])["peer_id"]

    print(f"\n### Phase 1 End (block {phase1_end_block})")
    attacker_p1 = next((p for p in p1_peers if p["peer_id"] == attacker_id), None)
    if attacker_p1:
        print(f"  Attacker: recv={attacker_p1['txs_received_total']}, incl={attacker_p1['txs_included_total']}, score={attacker_p1['score']:.0f}")

# Find stats at end
final_peers = blocks[phase2_end_block]["peers"]
attacker_final = next((p for p in final_peers if p["peer_id"] == attacker_id), None)

# Honest peer is the one that's NOT the attacker and has significant activity
honest_final = None
for p in final_peers:
    if p["peer_id"] != attacker_id and p["txs_received_total"] > 5000:
        honest_final = p
        break

print(f"\n### Phase 2 End (block {phase2_end_block})")
if attacker_final:
    rate = attacker_final["txs_included_total"] / attacker_final["txs_received_total"] * 100 if attacker_final["txs_received_total"] > 0 else 0
    print(f"  Attacker: recv={attacker_final['txs_received_total']}, incl={attacker_final['txs_included_total']}, rate={rate:.1f}%, score={attacker_final['score']:.0f}")
if honest_final:
    rate = honest_final["txs_included_total"] / honest_final["txs_received_total"] * 100 if honest_final["txs_received_total"] > 0 else 0
    print(f"  Honest:   recv={honest_final['txs_received_total']}, incl={honest_final['txs_included_total']}, rate={rate:.1f}%, score={honest_final['score']:.0f}")

# Score evolution - track attacker score over time
print(f"\n### Attacker Score Evolution")
print(f"{'Block':>6} {'Time(s)':>8} {'Score':>12} {'Recv':>8} {'Incl':>8}")
print("-" * 50)
t0 = blocks[0]["timestamp_ms"]
checkpoints = [5, 10, 15, 20, 25, 30, 35, 40, 50, 60, 70]
for i in checkpoints:
    if i < len(blocks):
        t = (blocks[i]["timestamp_ms"] - t0) / 1000
        peer = next((p for p in blocks[i]["peers"] if p["peer_id"] == attacker_id), None)
        if peer:
            print(f"{i:>6} {t:>8.1f} {peer['score']:>12.0f} {peer['txs_received_total']:>8} {peer['txs_included_total']:>8}")

print(f"\n### Defense Assessment")
if attacker_final and honest_final:
    if honest_final["score"] > attacker_final["score"] * 0.5:
        print(f"  ✓ Honest peer caught up: score {honest_final['score']:.0f} vs attacker {attacker_final['score']:.0f}")
    else:
        print(f"  Attacker still has advantage: score {attacker_final['score']:.0f} vs honest {honest_final['score']:.0f}")

    if attacker_final["txs_included_total"] < attacker_final["txs_received_total"] * 0.5:
        print(f"  ✓ Attacker inclusion rate dropped to {attacker_final['txs_included_total']/attacker_final['txs_received_total']*100:.1f}%")
    else:
        print(f"  Attacker maintained {attacker_final['txs_included_total']/attacker_final['txs_received_total']*100:.1f}% inclusion rate")

PYEOF
