#!/usr/bin/env bash
set -euo pipefail

# Minimum Contribution Stressed Experiment (10.5)
# 5 peers at varying rates with threshold ~200K
# Goal: Verify unpromoted peers get ~10% bandwidth (newcomer_capacity share)

BINARY="$(dirname "$0")/../target/release/monad-tx-integration"
OUT_DIR="${1:-/tmp/tx-experiment-min-contribution}"
COMMIT_INTERVAL_MS=500
NUM_ACCOUNTS=1000
DURATION=45
PROMOTION_THRESHOLD=200000.0

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

echo "=== Building ==="
cargo build -p monad-tx-integration --release 2>&1 | tail -3

echo "=== Starting node (promotion_threshold=$PROMOTION_THRESHOLD) ==="
RUST_LOG=monad_tx_integration=info "$BINARY" node \
    --listen 0.0.0.0:0 \
    --commit-interval-ms "$COMMIT_INTERVAL_MS" \
    --stats-file "$OUT_DIR/node-stats.jsonl" \
    --num-accounts "$NUM_ACCOUNTS" \
    --duration-secs "$DURATION" \
    --promotion-threshold "$PROMOTION_THRESHOLD" \
    > "$OUT_DIR/node.log" 2>&1 &
NODE_PID=$!

ADDR=""
for i in $(seq 1 50); do
    ADDR=$(grep -oP 'CONNECT_ADDR=\K.*' "$OUT_DIR/node.log" 2>/dev/null || true)
    [ -n "$ADDR" ] && break
    sleep 0.1
done

if [ -z "$ADDR" ]; then
    echo "ERROR: Node failed to start"
    kill $NODE_PID 2>/dev/null
    exit 1
fi
echo "Node listening on $ADDR"

echo "=== Starting 5 peers at varying rates ==="
# Peer 0-1: high rate (should eventually promote with enough score)
for i in 0 1; do
    RUST_LOG=monad_tx_integration=info "$BINARY" submit \
        --node-addr "$ADDR" \
        --tps 600 \
        --sender-index "$i" \
        --duration-secs $((DURATION - 5)) \
        --stats-file "$OUT_DIR/peer-$i.json" \
        --max-fee-multiplier 5 \
        --priority-fee-multiplier 3 \
        > "$OUT_DIR/peer-$i.log" 2>&1 &
    echo "  Peer P$i: 600 tps, 5x fee (high)"
done

# Peer 2: medium rate
RUST_LOG=monad_tx_integration=info "$BINARY" submit \
    --node-addr "$ADDR" \
    --tps 300 \
    --sender-index 2 \
    --duration-secs $((DURATION - 5)) \
    --stats-file "$OUT_DIR/peer-2.json" \
    --max-fee-multiplier 3 \
    --priority-fee-multiplier 1 \
    > "$OUT_DIR/peer-2.log" 2>&1 &
echo "  Peer P2: 300 tps, 3x fee (medium)"

# Peer 3-4: low rate (will stay unpromoted with high threshold)
for i in 3 4; do
    RUST_LOG=monad_tx_integration=info "$BINARY" submit \
        --node-addr "$ADDR" \
        --tps 100 \
        --sender-index "$i" \
        --duration-secs $((DURATION - 5)) \
        --stats-file "$OUT_DIR/peer-$i.json" \
        --max-fee-multiplier 2 \
        --priority-fee-multiplier 0 \
        > "$OUT_DIR/peer-$i.log" 2>&1 &
    echo "  Peer P$i: 100 tps, 2x fee (low)"
done

sleep "$DURATION"
wait $NODE_PID 2>/dev/null || true

echo "=== Results ==="
python3 << 'PYEOF'
import json

out_dir = "/tmp/tx-experiment-min-contribution"

blocks = []
with open(f"{out_dir}/node-stats.jsonl") as f:
    for line in f:
        if line.strip():
            blocks.append(json.loads(line))

print("\n" + "=" * 70)
print("MIN CONTRIBUTION STRESSED ANALYSIS (10.5)")
print("=" * 70)

final = blocks[-1]["peers"]

promoted_incl = 0
unpromoted_incl = 0

print(f"\n### Per-Peer Stats")
print(f"{'Peer':>10} {'Recv':>8} {'Incl':>8} {'Rate':>8} {'Score':>12} {'Promoted':>10}")
print("-" * 62)

for p in sorted(final, key=lambda x: x["peer_id"]):
    rate = p["txs_included_total"] / p["txs_received_total"] if p["txs_received_total"] > 0 else 0
    print(f"{p['peer_id'][:10]:>10} {p['txs_received_total']:>8} {p['txs_included_total']:>8} {rate*100:>7.1f}% {p['score']:>12.0f} {str(p['promoted']):>10}")
    if p["promoted"]:
        promoted_incl += p["txs_included_total"]
    else:
        unpromoted_incl += p["txs_included_total"]

total = promoted_incl + unpromoted_incl
if total > 0:
    unpromoted_pct = unpromoted_incl / total * 100
    print(f"\n### Bandwidth Allocation")
    print(f"  Promoted included:   {promoted_incl} ({promoted_incl/total*100:.1f}%)")
    print(f"  Unpromoted included: {unpromoted_incl} ({unpromoted_pct:.1f}%)")
    if 5 <= unpromoted_pct <= 15:
        print(f"  PASS: Unpromoted peers get ~10% bandwidth ({unpromoted_pct:.1f}%)")
    elif unpromoted_pct < 5:
        print(f"  WARN: Unpromoted bandwidth too low ({unpromoted_pct:.1f}% < 5%)")
    else:
        print(f"  WARN: Unpromoted bandwidth higher than expected ({unpromoted_pct:.1f}% > 15%)")
else:
    print("\n  No transactions included")

PYEOF

echo ""
echo "=== Results written to $OUT_DIR ==="
