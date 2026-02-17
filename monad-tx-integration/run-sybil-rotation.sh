#!/usr/bin/env bash
set -euo pipefail

# Sybil Rotation Attack Experiment
# 2 honest peers run continuously
# Sybil identities rotate every 5 seconds (new batch replaces old)
# Goal: Verify newcomer pool LRU eviction protects honest peers

BINARY="$(dirname "$0")/../target/release/monad-tx-integration"
OUT_DIR="${1:-/tmp/tx-experiment-sybil-rotation}"
TOTAL_DURATION=40
COMMIT_INTERVAL_MS=500
NUM_ACCOUNTS=2000

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
    > "$OUT_DIR/node.log" 2>&1 &
NODE_PID=$!

ADDR=""
for i in $(seq 1 50); do
    RC_TCP_ADDR=$(grep -oP 'RC_TCP_ADDR=\K.*' "$OUT_DIR/node.log" 2>/dev/null || true)
    RC_UDP_ADDR=$(grep -oP 'RC_UDP_ADDR=\K.*' "$OUT_DIR/node.log" 2>/dev/null || true)
    RC_AUTH_UDP_ADDR=$(grep -oP 'RC_AUTH_UDP_ADDR=\K.*' "$OUT_DIR/node.log" 2>/dev/null || true)
    LEANUDP_ADDR=$(grep -oP 'LEANUDP_ADDR=\K.*' "$OUT_DIR/node.log" 2>/dev/null || true)
    ADDR="$LEANUDP_ADDR"
    [ -n "$ADDR" ] && [ -n "$RC_TCP_ADDR" ] && [ -n "$RC_UDP_ADDR" ] && [ -n "$RC_AUTH_UDP_ADDR" ] && [ -n "$LEANUDP_ADDR" ] && break
    sleep 0.1
done

if [ -z "$ADDR" ]; then
    echo "ERROR: Node failed to start"
    cat "$OUT_DIR/node.log"
    kill $NODE_PID 2>/dev/null
    exit 1
fi
echo "Node listening on $ADDR (PID $NODE_PID)"

echo "=== Starting 2 honest peers (full duration) ==="
for i in 0 1; do
    RUST_LOG=monad_tx_integration=info "$BINARY" submit \
        --transport "${TRANSPORT:-leanudp}" \
        --rc-tcp-addr "$RC_TCP_ADDR" \
        --rc-udp-addr "$RC_UDP_ADDR" \
        --rc-auth-udp-addr "$RC_AUTH_UDP_ADDR" \
        --leanudp-addr "$LEANUDP_ADDR" \
        --tps 400 \
        --sender-index "$i" \
        --duration-secs "$TOTAL_DURATION" \
        --stats-file "$OUT_DIR/honest-$i.json" \
        --max-fee-multiplier 5 \
        --priority-fee-multiplier 3 \
        > "$OUT_DIR/honest-$i.log" 2>&1 &
    echo "  Honest H$i: 400 tps, 5x fee (PID $!)"
done

# Rotate sybils every 5 seconds
# Wave 1: t=0-10s, identities 10-19
# Wave 2: t=10-20s, identities 20-29
# Wave 3: t=20-30s, identities 30-39
# Wave 4: t=30-40s, identities 40-49

spawn_sybil_wave() {
    local start_idx=$1
    local duration=$2
    local wave_name=$3
    echo "=== Spawning sybil wave $wave_name (identities $start_idx-$((start_idx+9))) ==="
    for i in $(seq $start_idx $((start_idx + 9))); do
        RUST_LOG=monad_tx_integration=info "$BINARY" submit \
            --transport "${TRANSPORT:-leanudp}" \
            --rc-tcp-addr "$RC_TCP_ADDR" \
            --rc-udp-addr "$RC_UDP_ADDR" \
            --rc-auth-udp-addr "$RC_AUTH_UDP_ADDR" \
            --leanudp-addr "$LEANUDP_ADDR" \
            --tps 300 \
            --sender-index "$i" \
            --duration-secs "$duration" \
            --stats-file "$OUT_DIR/sybil-$i.json" \
            --max-fee-multiplier 1 \
            --priority-fee-multiplier 0 \
            > "$OUT_DIR/sybil-$i.log" 2>&1 &
    done
}

spawn_sybil_wave 10 10 "1"
sleep 10
spawn_sybil_wave 20 10 "2"
sleep 10
spawn_sybil_wave 30 10 "3"
sleep 10
spawn_sybil_wave 40 10 "4"
sleep 10

echo "=== Waiting for all processes ==="
wait $NODE_PID 2>/dev/null || true

echo "=== Results ==="
python3 << 'PYEOF'
import json
import os

out_dir = "/tmp/tx-experiment-sybil-rotation"

blocks = []
with open(f"{out_dir}/node-stats.jsonl") as f:
    for line in f:
        if line.strip():
            blocks.append(json.loads(line))

final_peers = blocks[-1]["peers"] if blocks else []

# Honest peers have highest scores
sorted_peers = sorted(final_peers, key=lambda p: p["score"], reverse=True)
honest = sorted_peers[:2]
sybils = sorted_peers[2:]

print("\n" + "=" * 70)
print("SYBIL ROTATION ATTACK ANALYSIS")
print("=" * 70)

print(f"\n### Honest Peers")
for p in honest:
    rate = p["txs_included_total"] / p["txs_received_total"] * 100 if p["txs_received_total"] > 0 else 0
    print(f"  {p['peer_id'][:20]}: recv={p['txs_received_total']:>6}, incl={p['txs_included_total']:>6}, rate={rate:.1f}%, score={p['score']:.0f}")

print(f"\n### Sybil Peers ({len(sybils)} total across 4 waves)")
if sybils:
    total_recv = sum(p["txs_received_total"] for p in sybils)
    total_incl = sum(p["txs_included_total"] for p in sybils)
    avg_rate = total_incl / total_recv * 100 if total_recv > 0 else 0
    print(f"  Total received: {total_recv:,}")
    print(f"  Total included: {total_incl:,}")
    print(f"  Avg inclusion rate: {avg_rate:.1f}%")

honest_incl = sum(p["txs_included_total"] for p in honest)
sybil_incl = sum(p["txs_included_total"] for p in sybils)
total = honest_incl + sybil_incl

print(f"\n### Bandwidth Share")
if total > 0:
    print(f"  Honest: {honest_incl/total*100:.1f}% ({honest_incl:,} txs)")
    print(f"  Sybils: {sybil_incl/total*100:.1f}% ({sybil_incl:,} txs)")

PYEOF

echo ""
echo "=== Results written to $OUT_DIR ==="
