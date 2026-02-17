#!/usr/bin/env bash
set -euo pipefail

# Replacement Transaction Spam Experiment (4.3)
# 1 honest peer + 1 attacker in replacement mode
# Goal: Verify replacement spam doesn't degrade honest peer inclusion

BINARY="$(dirname "$0")/../target/release/monad-tx-integration"
OUT_DIR="${1:-/tmp/tx-experiment-replacement-spam}"
COMMIT_INTERVAL_MS=500
NUM_ACCOUNTS=1000
DURATION=40

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
    --duration-secs "$DURATION" \
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
    kill $NODE_PID 2>/dev/null
    exit 1
fi
echo "Node listening on $ADDR"

echo "=== Starting honest peer (P0) ==="
RUST_LOG=monad_tx_integration=info "$BINARY" submit \
    --transport "${TRANSPORT:-leanudp}" \
    --rc-tcp-addr "$RC_TCP_ADDR" \
    --rc-udp-addr "$RC_UDP_ADDR" \
    --rc-auth-udp-addr "$RC_AUTH_UDP_ADDR" \
    --leanudp-addr "$LEANUDP_ADDR" \
    --tps 400 \
    --sender-index 0 \
    --duration-secs $((DURATION - 5)) \
    --stats-file "$OUT_DIR/honest.json" \
    --max-fee-multiplier 3 \
    --priority-fee-multiplier 1 \
    > "$OUT_DIR/honest.log" 2>&1 &
echo "  Honest P0: 400 tps, 3x fee, normal mode"

echo "=== Starting attacker peer (P1) in replacement mode ==="
RUST_LOG=monad_tx_integration=info "$BINARY" submit \
    --transport "${TRANSPORT:-leanudp}" \
    --rc-tcp-addr "$RC_TCP_ADDR" \
    --rc-udp-addr "$RC_UDP_ADDR" \
    --rc-auth-udp-addr "$RC_AUTH_UDP_ADDR" \
    --leanudp-addr "$LEANUDP_ADDR" \
    --tps 400 \
    --sender-index 1 \
    --duration-secs $((DURATION - 5)) \
    --stats-file "$OUT_DIR/attacker.json" \
    --max-fee-multiplier 3 \
    --priority-fee-multiplier 1 \
    --replacement-mode \
    > "$OUT_DIR/attacker.log" 2>&1 &
echo "  Attacker P1: 400 tps, 3x fee, replacement mode"

sleep "$DURATION"
wait $NODE_PID 2>/dev/null || true

echo "=== Results ==="
python3 << 'PYEOF'
import json

out_dir = "/tmp/tx-experiment-replacement-spam"

blocks = []
with open(f"{out_dir}/node-stats.jsonl") as f:
    for line in f:
        if line.strip():
            blocks.append(json.loads(line))

print("\n" + "=" * 70)
print("REPLACEMENT TX SPAM ANALYSIS (4.3)")
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

print(f"\n### Impact Analysis")
print(f"  Total received: {total_recv}")
print(f"  Total included: {total_incl}")
if total_recv > 0:
    print(f"  Overall inclusion: {total_incl/total_recv*100:.1f}%")

peers_sorted = sorted(final, key=lambda x: x["txs_included_total"], reverse=True)
if len(peers_sorted) >= 2:
    top = peers_sorted[0]
    bot = peers_sorted[1]
    print(f"\n  Highest includer: {top['txs_included_total']} (recv={top['txs_received_total']})")
    print(f"  Lowest includer:  {bot['txs_included_total']} (recv={bot['txs_received_total']})")
    if top["txs_included_total"] > 0 and bot["txs_included_total"] > 0:
        ratio = top["txs_included_total"] / bot["txs_included_total"]
        print(f"  Inclusion ratio: {ratio:.1f}:1")

PYEOF

echo ""
echo "=== Results written to $OUT_DIR ==="
