#!/usr/bin/env bash
set -euo pipefail

# Session Exhaustion Experiment (3.3)
# Node with max_sessions_per_ip=10, low_watermark=0 (force cookie challenge for ALL sessions)
# multi-submit 15 identities — handshake takes ~10-11s due to cookie round-trip
# Goal: Verify max_sessions_per_ip is enforced after cookie flow completes
#
# Cookie flow timeline per identity:
#   t=0:   send handshake init (no cookie)
#   t=0:   responder sends cookie reply
#   t~10s: session timeout fires → retry with stored cookie
#   t~10s: responder checks max_sessions_per_ip → accept or drop

BINARY="$(dirname "$0")/../target/release/monad-tx-integration"
OUT_DIR="${1:-/tmp/tx-experiment-session-exhaustion}"
COMMIT_INTERVAL_MS=500
NUM_ACCOUNTS=1000
DURATION=50
MAX_SESSIONS=10
NUM_IDENTITIES=15

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

echo "=== Building ==="
cargo build -p monad-tx-integration --release 2>&1 | tail -3

echo "=== Starting node (max_sessions_per_ip=$MAX_SESSIONS, low_watermark=0, ip_rate_limit=0) ==="
RUST_LOG=monad_tx_integration=info "$BINARY" node \
    --listen 0.0.0.0:0 \
    --commit-interval-ms "$COMMIT_INTERVAL_MS" \
    --stats-file "$OUT_DIR/node-stats.jsonl" \
    --num-accounts "$NUM_ACCOUNTS" \
    --duration-secs "$DURATION" \
    --max-sessions-per-ip "$MAX_SESSIONS" \
    --low-watermark-sessions 0 \
    --ip-rate-limit-ms 0 \
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

RPC_ADDR=""
for i in $(seq 1 50); do
    RPC_ADDR=$(grep -oP 'RPC_ADDR=\K.*' "$OUT_DIR/node.log" 2>/dev/null || true)
    [ -n "$RPC_ADDR" ] && break
    sleep 0.1
done
echo "RPC at $RPC_ADDR"

echo "=== Starting multi-submit with $NUM_IDENTITIES identities (15s handshake timeout) ==="
RUST_LOG=monad_tx_integration=info "$BINARY" multi-submit \
    --node-addr "$ADDR" \
    --num-identities "$NUM_IDENTITIES" \
    --tps-per-identity 100 \
    --sender-index-base 0 \
    --duration-secs $((DURATION - 5)) \
    --max-fee-multiplier 3 \
    --priority-fee-multiplier 1 \
    > "$OUT_DIR/multi-submit.log" 2>&1 &
echo "  $NUM_IDENTITIES identities at 100 tps each"

sleep "$DURATION"
wait $NODE_PID 2>/dev/null || true

echo "=== Results ==="
python3 << 'PYEOF'
import json

out_dir = "/tmp/tx-experiment-session-exhaustion"

blocks = []
with open(f"{out_dir}/node-stats.jsonl") as f:
    for line in f:
        if line.strip():
            blocks.append(json.loads(line))

print("\n" + "=" * 70)
print("SESSION EXHAUSTION ANALYSIS (3.3)")
print("=" * 70)

if not blocks:
    print("  No blocks recorded")
    exit(0)

final = blocks[-1]["peers"]
num_peers = len(final)

print(f"\n### Peers Seen: {num_peers}")
print(f"{'Peer':>10} {'Recv':>8} {'Incl':>8} {'Rate':>8} {'Score':>12}")
print("-" * 52)

for p in sorted(final, key=lambda x: x["peer_id"]):
    rate = p["txs_included_total"] / p["txs_received_total"] if p["txs_received_total"] > 0 else 0
    print(f"{p['peer_id'][:10]:>10} {p['txs_received_total']:>8} {p['txs_included_total']:>8} {rate*100:>7.1f}% {p['score']:>12.0f}")

max_sessions = 10
num_identities = 15

print(f"\n### Session Limit Check")
print(f"  Attempted identities: {num_identities}")
print(f"  Max sessions per IP:  {max_sessions}")
print(f"  Peers observed:       {num_peers}")

if num_peers <= max_sessions:
    print(f"  PASS: Peer count ({num_peers}) <= max sessions ({max_sessions})")
else:
    print(f"  FAIL: Peer count ({num_peers}) > max sessions ({max_sessions})")

# Check multi-submit log for handshake details
import os
multi_log = os.path.join(out_dir, "multi-submit.log")
if os.path.exists(multi_log):
    with open(multi_log) as f:
        content = f.read()
    completed = content.count("handshake complete")
    failed = content.count("handshake failed")
    print(f"\n### Handshake Details")
    print(f"  Completed: {completed}")
    print(f"  Failed:    {failed}")

PYEOF

echo ""
echo "=== Results written to $OUT_DIR ==="
