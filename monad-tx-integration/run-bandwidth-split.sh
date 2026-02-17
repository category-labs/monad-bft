#!/usr/bin/env bash
set -euo pipefail

# Bandwidth Split Experiment (10.1)
# High-TPS staggered start to overwhelm FairQueue ingestion
# Goal: Verify fair queue bandwidth split (unpromoted get ~10% of inclusion)
#
# Capacity: proposal_tx_limit=200 @ 500ms = 400 TPS capacity
# Phase 1 (0-15s):   10 early identities × 5000 tps = 50K TPS (build score → promote)
# Phase 2 (15s-50s): 10 late identities × 5000 tps = 50K TPS more (total 100K TPS)
# All peers use identical 2x fee — bandwidth split comes from fair queue only
# Threshold: 400K — early peers cross it during head start, late peers can't catch up

BINARY="$(dirname "$0")/../target/release/monad-tx-integration"
OUT_DIR="${1:-/tmp/tx-experiment-bandwidth-split}"
COMMIT_INTERVAL_MS=500
NUM_ACCOUNTS=5000
DURATION=50
PROMOTION_THRESHOLD=400000.0
PROPOSAL_TX_LIMIT=200
HEAD_START_SECS=15
NUM_PROMOTED=10
NUM_UNPROMOTED=10
TPS_PER_IDENTITY=5000

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

echo "=== Building ==="
cargo build -p monad-tx-integration --release 2>&1 | tail -3

echo "=== Starting node (threshold=$PROMOTION_THRESHOLD, proposal_tx_limit=$PROPOSAL_TX_LIMIT) ==="
RUST_LOG=monad_tx_integration=info "$BINARY" node \
    --listen 0.0.0.0:0 \
    --commit-interval-ms "$COMMIT_INTERVAL_MS" \
    --stats-file "$OUT_DIR/node-stats.jsonl" \
    --num-accounts "$NUM_ACCOUNTS" \
    --duration-secs "$DURATION" \
    --promotion-threshold "$PROMOTION_THRESHOLD" \
    --proposal-tx-limit "$PROPOSAL_TX_LIMIT" \
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

TOTAL_PROMOTED_TPS=$((NUM_PROMOTED * TPS_PER_IDENTITY))
TOTAL_UNPROMOTED_TPS=$((NUM_UNPROMOTED * TPS_PER_IDENTITY))

echo "=== Phase 1: Starting $NUM_PROMOTED early identities (${TOTAL_PROMOTED_TPS} TPS total) ==="
RUST_LOG=monad_tx_integration=info "$BINARY" multi-submit \
    --transport "${TRANSPORT:-leanudp}" \
    --rc-tcp-addr "$RC_TCP_ADDR" \
    --rc-udp-addr "$RC_UDP_ADDR" \
    --rc-auth-udp-addr "$RC_AUTH_UDP_ADDR" \
    --leanudp-addr "$LEANUDP_ADDR" \
    --num-identities "$NUM_PROMOTED" \
    --tps-per-identity "$TPS_PER_IDENTITY" \
    --sender-index-base 0 \
    --duration-secs $((DURATION - 5)) \
    --max-fee-multiplier 2 \
    --priority-fee-multiplier 0 \
    --batch-size 64 \
    > "$OUT_DIR/promoted.log" 2>&1 &
echo "  ${NUM_PROMOTED} identities × ${TPS_PER_IDENTITY} tps, 2x fee (head start ${HEAD_START_SECS}s)"

echo "=== Waiting ${HEAD_START_SECS}s for early peers to build score... ==="
sleep "$HEAD_START_SECS"

echo "=== Phase 2: Starting $NUM_UNPROMOTED late identities (${TOTAL_UNPROMOTED_TPS} TPS total, ${TOTAL_PROMOTED_TPS} + ${TOTAL_UNPROMOTED_TPS} = $((TOTAL_PROMOTED_TPS + TOTAL_UNPROMOTED_TPS)) TPS combined) ==="
RUST_LOG=monad_tx_integration=info "$BINARY" multi-submit \
    --transport "${TRANSPORT:-leanudp}" \
    --rc-tcp-addr "$RC_TCP_ADDR" \
    --rc-udp-addr "$RC_UDP_ADDR" \
    --rc-auth-udp-addr "$RC_AUTH_UDP_ADDR" \
    --leanudp-addr "$LEANUDP_ADDR" \
    --num-identities "$NUM_UNPROMOTED" \
    --tps-per-identity "$TPS_PER_IDENTITY" \
    --sender-index-base "$NUM_PROMOTED" \
    --duration-secs $((DURATION - HEAD_START_SECS - 5)) \
    --max-fee-multiplier 2 \
    --priority-fee-multiplier 0 \
    --batch-size 64 \
    > "$OUT_DIR/unpromoted.log" 2>&1 &
echo "  ${NUM_UNPROMOTED} identities × ${TPS_PER_IDENTITY} tps, 2x fee (late join, expect unpromoted)"

sleep $((DURATION - HEAD_START_SECS))
wait $NODE_PID 2>/dev/null || true

echo "=== Results ==="
python3 << 'PYEOF'
import json, sys

out_dir = "/tmp/tx-experiment-bandwidth-split"
num_promoted = 10
total_peers = 20

blocks = []
with open(f"{out_dir}/node-stats.jsonl") as f:
    for line in f:
        if line.strip():
            blocks.append(json.loads(line))

print("\n" + "=" * 70)
print("BANDWIDTH SPLIT ANALYSIS (10.1)")
print("=" * 70)

# Find the first block where late peers appear (phase 2 start)
phase1_peer_count = len(blocks[min(5, len(blocks)-1)]["peers"]) if len(blocks) > 5 else 0
phase2_start = None
for i, b in enumerate(blocks):
    if len(b["peers"]) > phase1_peer_count:
        phase2_start = i
        break

if phase2_start is None:
    print(f"WARN: Late peers never appeared (saw max {phase1_peer_count} peers)")
    print("Showing full run stats instead")
    phase2_start = 0

phase2_blocks = blocks[phase2_start:]
print(f"\nPhase 1 (head start): {phase2_start} blocks with {phase1_peer_count} peers")
print(f"Phase 2 (mixed):      {len(phase2_blocks)} blocks with {len(blocks[-1]['peers'])} peers")

final = blocks[-1]["peers"]

promoted_peers = set()
unpromoted_peers = set()
for p in final:
    if p["promoted"]:
        promoted_peers.add(p["peer_id"])
    else:
        unpromoted_peers.add(p["peer_id"])

print(f"\nPromoted peers:   {len(promoted_peers)}")
print(f"Unpromoted peers: {len(unpromoted_peers)}")

# Show top 5 promoted and top 5 unpromoted by inclusion
print(f"\n### Per-Peer Stats (top 5 promoted + top 5 unpromoted)")
print(f"{'Peer':>10} {'Recv':>10} {'Incl':>8} {'Rate':>8} {'Score':>12} {'Promoted':>10}")
print("-" * 64)

promoted_list = sorted([p for p in final if p["promoted"]], key=lambda x: -x["txs_included_total"])
unpromoted_list = sorted([p for p in final if not p["promoted"]], key=lambda x: -x["txs_included_total"])

for p in promoted_list[:5]:
    rate = p["txs_included_total"] / p["txs_received_total"] if p["txs_received_total"] > 0 else 0
    print(f"{p['peer_id'][:10]:>10} {p['txs_received_total']:>10} {p['txs_included_total']:>8} {rate*100:>7.1f}% {p['score']:>12.0f} {str(p['promoted']):>10}")
if len(promoted_list) > 5:
    print(f"  ... and {len(promoted_list) - 5} more promoted peers")

for p in unpromoted_list[:5]:
    rate = p["txs_included_total"] / p["txs_received_total"] if p["txs_received_total"] > 0 else 0
    print(f"{p['peer_id'][:10]:>10} {p['txs_received_total']:>10} {p['txs_included_total']:>8} {rate*100:>7.1f}% {p['score']:>12.0f} {str(p['promoted']):>10}")
if len(unpromoted_list) > 5:
    print(f"  ... and {len(unpromoted_list) - 5} more unpromoted peers")

# Count per-peer inclusion only during phase 2
peer_incl_phase2 = {}
for b in phase2_blocks:
    for p in b["peers"]:
        pid = p["peer_id"]
        peer_incl_phase2[pid] = peer_incl_phase2.get(pid, 0) + p["txs_included_this_block"]

promoted_incl = sum(peer_incl_phase2.get(pid, 0) for pid in promoted_peers)
unpromoted_incl = sum(peer_incl_phase2.get(pid, 0) for pid in unpromoted_peers)
total = promoted_incl + unpromoted_incl

print(f"\n### Bandwidth Split (phase 2 only — {len(phase2_blocks)} blocks)")
if total > 0:
    print(f"  Promoted:   incl={promoted_incl} ({promoted_incl/total*100:.1f}%)")
    print(f"  Unpromoted: incl={unpromoted_incl} ({unpromoted_incl/total*100:.1f}%)")
    if unpromoted_incl > 0:
        ratio = promoted_incl / unpromoted_incl
        print(f"  Ratio: {ratio:.1f}:1")
        unpromoted_pct = unpromoted_incl / total * 100
        if 5 <= unpromoted_pct <= 20:
            print(f"  PASS: Unpromoted bandwidth ~{unpromoted_pct:.0f}% (expected ~10%)")
        elif unpromoted_pct < 5:
            print(f"  WARN: Unpromoted bandwidth too low ({unpromoted_pct:.1f}%)")
        else:
            print(f"  WARN: Unpromoted bandwidth higher than expected ({unpromoted_pct:.1f}%)")
    else:
        print(f"  WARN: Unpromoted got 0 txs included")
else:
    print("  ERROR: No txs included in phase 2")

# Per-block timeline (last 20 blocks) — show aggregate by promoted/unpromoted
print(f"\n### Per-Block Timeline (last 20 blocks)")
for b in blocks[-20:]:
    incl = b["txs_included"]
    p_incl = sum(p["txs_included_this_block"] for p in b["peers"] if p["promoted"])
    u_incl = sum(p["txs_included_this_block"] for p in b["peers"] if not p["promoted"])
    n_peers = len(b["peers"])
    n_prom = sum(1 for p in b["peers"] if p["promoted"])
    print(f"  Block {b['block_number']:>4}: {incl:>4} txs [P={p_incl}, U={u_incl}, peers={n_peers}({n_prom}P)]")

PYEOF

echo ""
echo "=== Results written to $OUT_DIR ==="
