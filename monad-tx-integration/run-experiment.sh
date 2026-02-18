#!/usr/bin/env bash
set -euo pipefail

BINARY="$(dirname "$0")/../target/release/monad-tx-integration"
OUT_DIR="${1:-/tmp/tx-integration-experiment}"
DURATION=30
COMMIT_INTERVAL_MS=500
NUM_ACCOUNTS=1000

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
    --duration-secs "$((DURATION + 5))" \
    > "$OUT_DIR/node.log" 2>&1 &
NODE_PID=$!

# Wait for listen address
ADDR=""
for i in $(seq 1 50); do
    RC_TCP_ADDR=$(grep -oP 'RC_TCP_ADDR=\K.*' "$OUT_DIR/node.log" 2>/dev/null || true)
    RC_UDP_ADDR=$(grep -oP 'RC_UDP_ADDR=\K.*' "$OUT_DIR/node.log" 2>/dev/null || true)
    RC_AUTH_UDP_ADDR=$(grep -oP 'RC_AUTH_UDP_ADDR=\K.*' "$OUT_DIR/node.log" 2>/dev/null || true)
    LEANUDP_ADDR=$(grep -oP 'LEANUDP_ADDR=\K.*' "$OUT_DIR/node.log" 2>/dev/null || true)
    RPC_ADDR=$(grep -oP 'RPC_ADDR=\K.*' "$OUT_DIR/node.log" 2>/dev/null || true)
    ADDR="$LEANUDP_ADDR"
    [ -n "$ADDR" ] && [ -n "$RC_TCP_ADDR" ] && [ -n "$RC_UDP_ADDR" ] && [ -n "$RC_AUTH_UDP_ADDR" ] && [ -n "$LEANUDP_ADDR" ] && [ -n "$RPC_ADDR" ] && break
    sleep 0.1
done

if [ -z "$ADDR" ]; then
    echo "ERROR: Node failed to start"
    cat "$OUT_DIR/node.log"
    kill $NODE_PID 2>/dev/null
    exit 1
fi
echo "Node listening on $ADDR, RPC on $RPC_ADDR (PID $NODE_PID)"

# 10 submitters: varying TPS and gas price
# Format: TPS  MAX_FEE_MULT  PRIORITY_FEE_MULT
#
# Low price peers (1x base fee, 0 priority):
#   S0: 100 tps, 1x fee, 0 priority
#   S1: 100 tps, 1x fee, 0 priority
#   S2: 200 tps, 1x fee, 0 priority
#
# Medium price peers (2x base fee, 1x priority):
#   S3: 100 tps, 2x fee, 1x priority
#   S4: 100 tps, 2x fee, 1x priority
#   S5: 200 tps, 2x fee, 1x priority
#
# High price peers (5x base fee, 3x priority):
#   S6: 100 tps, 5x fee, 3x priority
#   S7: 100 tps, 5x fee, 3x priority
#   S8: 200 tps, 5x fee, 3x priority
#
# Very high price peer (10x base fee, 5x priority):
#   S9: 200 tps, 10x fee, 5x priority

TPS_RATES=(     300  300  600  300  300  600  300  300  600  600)
MAX_FEE_MULT=(    1    1    1    2    2    2    5    5    5   10)
PRIO_FEE_MULT=(   0    0    0    1    1    1    3    3    3    5)

PIDS=()

echo "=== Starting 10 submit agents ==="
for i in $(seq 0 9); do
    TPS=${TPS_RATES[$i]}
    MFM=${MAX_FEE_MULT[$i]}
    PFM=${PRIO_FEE_MULT[$i]}
    RUST_LOG=monad_tx_integration=info "$BINARY" submit \
        --transport "${TRANSPORT:-leanudp}" \
        --rc-tcp-addr "$RC_TCP_ADDR" \
        --rc-udp-addr "$RC_UDP_ADDR" \
        --rc-auth-udp-addr "$RC_AUTH_UDP_ADDR" \
        --leanudp-addr "$LEANUDP_ADDR" \
        --rpc-addr "$RPC_ADDR" \
        --tps "$TPS" \
        --sender-index "$i" \
        --duration-secs "$DURATION" \
        --stats-file "$OUT_DIR/submit-$i.json" \
        --max-fee-multiplier "$MFM" \
        --priority-fee-multiplier "$PFM" \
        > "$OUT_DIR/submit-$i.log" 2>&1 &
    PIDS+=($!)
    echo "  S$i: ${TPS} tps, ${MFM}x max_fee, ${PFM}x priority (PID ${PIDS[-1]})"
done

echo "=== Running for ${DURATION}s ==="
sleep "$DURATION"

echo "=== Waiting for submitters to finish ==="
for pid in "${PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
done

echo "=== Waiting for node to finish ==="
wait "$NODE_PID" 2>/dev/null || true

echo ""
echo "=== Submitter Stats ==="
for i in $(seq 0 9); do
    if [ -f "$OUT_DIR/submit-$i.json" ]; then
        echo "--- S$i ---"
        cat "$OUT_DIR/submit-$i.json"
        echo ""
    fi
done

echo "=== Node Stats (last 3 blocks) ==="
tail -3 "$OUT_DIR/node-stats.jsonl"

echo ""
echo "=== Results written to $OUT_DIR ==="
ls -la "$OUT_DIR/"

python3 -c "
import json, os

out_dir = '$OUT_DIR'

submitters = []
for i in range(10):
    path = os.path.join(out_dir, f'submit-{i}.json')
    if os.path.exists(path):
        with open(path) as f:
            submitters.append(json.load(f))

blocks = []
node_path = os.path.join(out_dir, 'node-stats.jsonl')
if os.path.exists(node_path):
    with open(node_path) as f:
        for line in f:
            line = line.strip()
            if line:
                blocks.append(json.loads(line))

summary = {
    'submitters': submitters,
    'blocks': blocks,
    'total_blocks': len(blocks),
    'total_txs_sent': sum(s.get('total_sent', 0) for s in submitters),
    'total_txs_received': blocks[-1]['txs_received_total'] if blocks else 0,
    'total_txs_included': sum(b['txs_included'] for b in blocks),
}

with open(os.path.join(out_dir, 'experiment.json'), 'w') as f:
    json.dump(summary, f, indent=2)

print(f'Summary: {summary[\"total_blocks\"]} blocks, {summary[\"total_txs_sent\"]} sent, {summary[\"total_txs_received\"]} received, {summary[\"total_txs_included\"]} included')
" 2>&1 || echo "(python3 summary generation skipped)"
