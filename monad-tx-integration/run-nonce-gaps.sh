#!/usr/bin/env bash
set -euo pipefail

# Nonce Gap Attack Experiment (4.1)
# 1 honest peer + 1 attacker with nonce gap every 10 txs
# Goal: Measure pool growth under nonce gap attack

BINARY="$(dirname "$0")/../target/release/monad-tx-integration"
OUT_DIR="${1:-/tmp/tx-experiment-nonce-gaps}"
COMMIT_INTERVAL_MS=500
NUM_ACCOUNTS=1000
DURATION=40

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
    --duration-secs "$DURATION" \
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

echo "=== Starting honest peer (P0) ==="
RUST_LOG=monad_tx_integration=info "$BINARY" submit \
    --node-addr "$ADDR" \
    --tps 400 \
    --sender-index 0 \
    --duration-secs $((DURATION - 5)) \
    --stats-file "$OUT_DIR/honest.json" \
    --max-fee-multiplier 3 \
    --priority-fee-multiplier 1 \
    > "$OUT_DIR/honest.log" 2>&1 &
echo "  Honest P0: 400 tps, 3x fee, no nonce gaps"

echo "=== Starting attacker peer (P1) with nonce gap every 10 txs ==="
RUST_LOG=monad_tx_integration=info "$BINARY" submit \
    --node-addr "$ADDR" \
    --tps 400 \
    --sender-index 1 \
    --duration-secs $((DURATION - 5)) \
    --stats-file "$OUT_DIR/attacker.json" \
    --max-fee-multiplier 3 \
    --priority-fee-multiplier 1 \
    --nonce-gap-interval 10 \
    > "$OUT_DIR/attacker.log" 2>&1 &
echo "  Attacker P1: 400 tps, 3x fee, gap every 10 txs"

sleep "$DURATION"
wait $NODE_PID 2>/dev/null || true

echo "=== Results ==="
python3 << 'PYEOF'
import json

out_dir = "/tmp/tx-experiment-nonce-gaps"

blocks = []
with open(f"{out_dir}/node-stats.jsonl") as f:
    for line in f:
        if line.strip():
            blocks.append(json.loads(line))

print("\n" + "=" * 70)
print("NONCE GAP ATTACK ANALYSIS (4.1)")
print("=" * 70)

final = blocks[-1]["peers"]

print(f"\n### Per-Peer Stats")
print(f"{'Peer':>10} {'Recv':>8} {'Incl':>8} {'Rate':>8} {'Score':>12} {'Promoted':>10}")
print("-" * 62)

for p in sorted(final, key=lambda x: x["peer_id"]):
    rate = p["txs_included_total"] / p["txs_received_total"] if p["txs_received_total"] > 0 else 0
    print(f"{p['peer_id'][:10]:>10} {p['txs_received_total']:>8} {p['txs_included_total']:>8} {rate*100:>7.1f}% {p['score']:>12.0f} {str(p['promoted']):>10}")

total_recv = sum(p["txs_received_total"] for p in final)
total_incl = sum(p["txs_included_total"] for p in final)

print(f"\n### Pool Impact")
print(f"  Total received: {total_recv}")
print(f"  Total included: {total_incl}")
print(f"  Inclusion rate: {total_incl/total_recv*100:.1f}%" if total_recv > 0 else "  No data")

# Track pool growth over time
print(f"\n### Pool Growth Over Time (last 10 blocks)")
for b in blocks[-10:]:
    print(f"  Block {b['block_number']:>4}: included={b['txs_included']:>4}, total_recv={b['txs_received_total']:>6}")

PYEOF

echo ""
echo "=== Results written to $OUT_DIR ==="
