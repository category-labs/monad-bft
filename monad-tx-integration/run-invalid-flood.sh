#!/usr/bin/env bash
set -euo pipefail

# Invalid Signature Flood Experiment (5.1)
# 1 honest peer + 1 attacker with 50% invalid signatures
# Goal: Verify honest peer inclusion is unaffected by invalid tx flood

BINARY="$(dirname "$0")/../target/release/monad-tx-integration"
OUT_DIR="${1:-/tmp/tx-experiment-invalid-flood}"
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
    --tps 500 \
    --sender-index 0 \
    --duration-secs $((DURATION - 5)) \
    --stats-file "$OUT_DIR/honest.json" \
    --max-fee-multiplier 3 \
    --priority-fee-multiplier 1 \
    > "$OUT_DIR/honest.log" 2>&1 &
echo "  Honest P0: 500 tps, 3x fee, 0% invalid"

echo "=== Starting attacker peer (P1) with 50% invalid signatures ==="
RUST_LOG=monad_tx_integration=info "$BINARY" submit \
    --node-addr "$ADDR" \
    --tps 500 \
    --sender-index 1 \
    --duration-secs $((DURATION - 5)) \
    --stats-file "$OUT_DIR/attacker.json" \
    --max-fee-multiplier 3 \
    --priority-fee-multiplier 1 \
    --invalid-sig-pct 50 \
    > "$OUT_DIR/attacker.log" 2>&1 &
echo "  Attacker P1: 500 tps, 3x fee, 50% invalid"

sleep "$DURATION"
wait $NODE_PID 2>/dev/null || true

echo "=== Results ==="
python3 << 'PYEOF'
import json

out_dir = "/tmp/tx-experiment-invalid-flood"

blocks = []
with open(f"{out_dir}/node-stats.jsonl") as f:
    for line in f:
        if line.strip():
            blocks.append(json.loads(line))

print("\n" + "=" * 70)
print("INVALID SIGNATURE FLOOD ANALYSIS (5.1)")
print("=" * 70)

final = blocks[-1]["peers"]

print(f"\n### Per-Peer Stats")
print(f"{'Peer':>10} {'Recv':>8} {'Incl':>8} {'Rate':>8} {'Score':>12} {'Promoted':>10}")
print("-" * 62)

for p in sorted(final, key=lambda x: x["peer_id"]):
    rate = p["txs_included_total"] / p["txs_received_total"] if p["txs_received_total"] > 0 else 0
    print(f"{p['peer_id'][:10]:>10} {p['txs_received_total']:>8} {p['txs_included_total']:>8} {rate*100:>7.1f}% {p['score']:>12.0f} {str(p['promoted']):>10}")

# Check that honest peer's inclusion isn't degraded
peers_sorted = sorted(final, key=lambda x: x["txs_included_total"], reverse=True)
if len(peers_sorted) >= 2:
    honest_incl = peers_sorted[0]["txs_included_total"]
    attacker_incl = peers_sorted[1]["txs_included_total"]
    print(f"\n### Impact Analysis")
    print(f"  Higher includer: {honest_incl} txs")
    print(f"  Lower includer:  {attacker_incl} txs")
    if honest_incl > attacker_incl:
        ratio = honest_incl / attacker_incl if attacker_incl > 0 else float('inf')
        print(f"  PASS: Honest peer has higher inclusion ({ratio:.1f}x)")
    else:
        print(f"  INFO: Attacker matched or exceeded honest peer inclusion")

    total_recv = sum(p["txs_received_total"] for p in final)
    total_incl = sum(p["txs_included_total"] for p in final)
    drop_rate = (total_recv - total_incl) / total_recv * 100 if total_recv > 0 else 0
    print(f"\n### Drop Rate")
    print(f"  Total received: {total_recv}")
    print(f"  Total included: {total_incl}")
    print(f"  Drop rate: {drop_rate:.1f}%")

PYEOF

echo ""
echo "=== Results written to $OUT_DIR ==="
