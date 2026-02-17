#!/usr/bin/env bash
set -euo pipefail

# 5-Minute Soak Test
# Sustained moderate load for extended period
# Goal: Verify stability, no memory leaks, consistent performance

BINARY="$(dirname "$0")/../target/release/monad-tx-integration"
OUT_DIR="${1:-/tmp/tx-experiment-soak-5m}"
COMMIT_INTERVAL_MS=500
NUM_ACCOUNTS=2000
DURATION=300  # 5 minutes

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
    --duration-secs "$((DURATION + 10))" \
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

echo "=== Starting 5 sustained peers for ${DURATION}s ==="
for i in $(seq 0 4); do
    RUST_LOG=monad_tx_integration=info "$BINARY" submit \
        --transport "${TRANSPORT:-leanudp}" \
        --rc-tcp-addr "$RC_TCP_ADDR" \
        --rc-udp-addr "$RC_UDP_ADDR" \
        --rc-auth-udp-addr "$RC_AUTH_UDP_ADDR" \
        --leanudp-addr "$LEANUDP_ADDR" \
        --tps 400 \
        --sender-index "$i" \
        --duration-secs "$DURATION" \
        --stats-file "$OUT_DIR/peer-$i.json" \
        --max-fee-multiplier 3 \
        --priority-fee-multiplier 2 \
        > "$OUT_DIR/peer-$i.log" 2>&1 &
    echo "  Peer $i: 400 tps"
done
echo "  Total: 2000 tps sustained for 5 minutes"

echo "=== Running soak test (5 minutes) ==="
for min in $(seq 1 5); do
    sleep 60
    echo "  Minute $min/5 complete..."
done

echo "=== Waiting for processes ==="
wait $NODE_PID 2>/dev/null || true

echo "=== Results ==="
python3 << 'PYEOF'
import json

out_dir = "/tmp/tx-experiment-soak-5m"

blocks = []
with open(f"{out_dir}/node-stats.jsonl") as f:
    for line in f:
        if line.strip():
            blocks.append(json.loads(line))

t0 = blocks[0]["timestamp_ms"]

print("\n" + "=" * 70)
print("5-MINUTE SOAK TEST ANALYSIS")
print("=" * 70)

# Overall
total_recv = blocks[-1]["txs_received_total"]
total_incl = sum(b["txs_included"] for b in blocks)
duration = (blocks[-1]["timestamp_ms"] - t0) / 1000

print(f"\n### Overall Performance")
print(f"  Duration: {duration:.1f}s ({duration/60:.1f} minutes)")
print(f"  Total blocks: {len(blocks)}")
print(f"  Total received: {total_recv:,}")
print(f"  Total included: {total_incl:,}")
print(f"  Inclusion rate: {total_incl/total_recv*100:.1f}%")
print(f"  Average TPS: {total_incl/duration:,.0f}")

# Per-minute analysis
print(f"\n### Per-Minute Breakdown")
print(f"{'Minute':>8} {'Blocks':>8} {'Included':>10} {'TPS':>8} {'Stalls':>8}")
print("-" * 50)

blocks_per_min = 120  # ~2 blocks/sec * 60 sec
for minute in range(5):
    start_idx = minute * blocks_per_min
    end_idx = min((minute + 1) * blocks_per_min, len(blocks))
    if start_idx >= len(blocks):
        break

    minute_blocks = blocks[start_idx:end_idx]
    minute_incl = sum(b["txs_included"] for b in minute_blocks)
    minute_stalls = sum(1 for b in minute_blocks if b["txs_included"] == 0)
    minute_tps = minute_incl / 60 if len(minute_blocks) > 0 else 0

    print(f"{minute+1:>8} {len(minute_blocks):>8} {minute_incl:>10,} {minute_tps:>8,.0f} {minute_stalls:>8}")

# Consistency check
tps_per_minute = []
for minute in range(5):
    start_idx = minute * blocks_per_min
    end_idx = min((minute + 1) * blocks_per_min, len(blocks))
    if start_idx >= len(blocks):
        break
    minute_incl = sum(b["txs_included"] for b in blocks[start_idx:end_idx])
    tps_per_minute.append(minute_incl / 60)

if tps_per_minute:
    avg_tps = sum(tps_per_minute) / len(tps_per_minute)
    variance = sum((t - avg_tps)**2 for t in tps_per_minute) / len(tps_per_minute)
    std_dev = variance ** 0.5
    cv = std_dev / avg_tps * 100 if avg_tps > 0 else 0

    print(f"\n### Consistency Metrics")
    print(f"  Average TPS: {avg_tps:,.0f}")
    print(f"  Std deviation: {std_dev:,.0f}")
    print(f"  Coefficient of variation: {cv:.1f}%")

    if cv < 10:
        print(f"  ✓ Very consistent throughput")
    elif cv < 20:
        print(f"  ✓ Reasonably consistent throughput")
    else:
        print(f"  ⚠️ High throughput variance")

# Final health
final_backlog = total_recv - total_incl
total_stalls = sum(1 for b in blocks if b["txs_included"] == 0)

print(f"\n### Health Summary")
print(f"  Final backlog: {final_backlog:,}")
print(f"  Total stall blocks: {total_stalls}/{len(blocks)} ({total_stalls/len(blocks)*100:.1f}%)")

if total_stalls > len(blocks) * 0.1:
    print(f"  ⚠️ Significant stalling occurred")
elif final_backlog > 1000:
    print(f"  ⚠️ Backlog accumulated")
else:
    print(f"  ✓ System stable over 5 minutes")

PYEOF

echo ""
echo "=== Results written to $OUT_DIR ==="
