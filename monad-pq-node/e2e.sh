#!/usr/bin/env bash
#
# End-to-end tx latency of a local pq stack: one pq node (or a swarm of N on
# loopback) proposing from its mempool, the unchanged monad-mcp-rpc colocated
# with node 0 (reading node 0's config) and the unchanged monad-mcp-explorer
# over node 0's ledger and rpc. Txs go in through the explorer's POST /api/tx,
# the rpc's POST /tx and raw udp (mcp-tx send --node); their latency is read
# back from the ledger (sent -> finalized) and from the explorer (submit ->
# GET /api/tx/{hash} answers).
#
# usage: e2e.sh [--swarm N] [--seconds S] [--interval-ms I] [--warmup S] [--no-build]
#   --swarm N       N validators on loopback sharing one genesis (default: 1 node)
#   --seconds S     load duration, default 60
#   --interval-ms I one tx per I ms per channel, default 100
#   --warmup S      seconds of finalized chain before the load, default 5
#   --no-build      use the release binaries already built
# env: E2E_DIR (default /tmp/pq-e2e) holds configs, logs and ledgers;
#      EXPLORER_TAIL_MS (default 20) is the explorer's ledger tail interval;
#      the PQ_* variables of scripts/local-config.sh override the demo values.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(cd "$here/.." && pwd)
# shellcheck source=scripts/local-config.sh
source "$here/scripts/local-config.sh"

nodes=1
seconds=60
interval_ms=100
warmup=5
build=1
while (($#)); do
    case $1 in
        --swarm) nodes=$2; shift ;;
        --seconds) seconds=$2; shift ;;
        --interval-ms) interval_ms=$2; shift ;;
        --warmup) warmup=$2; shift ;;
        --no-build) build=0 ;;
        *) echo "e2e.sh: unknown argument $1" >&2; exit 2 ;;
    esac
    shift
done
((nodes >= 1)) || { echo "e2e.sh: --swarm needs at least 1 node" >&2; exit 2; }

log() { printf '[pq-e2e %s] %s\n' "$(date +%T)" "$*" >&2; }
die() { log "error: $*"; exit 1; }

if ((build)); then
    log "building pq node, rpc and explorer (release)"
    (cd "$repo" && cargo build --release -p monad-pq-node --bin monad-pq-node --example latency \
        && cargo build --release -p monad-mcp-rpc -p monad-mcp-explorer --bins)
fi
target=${CARGO_TARGET_DIR:-$(cd "$repo" && cargo metadata --format-version 1 --no-deps | jq -r .target_directory)}
bin=$target/release
for b in monad-pq-node monad-mcp-rpc monad-mcp-explorer mcp-tx examples/latency; do
    [[ -x $bin/$b ]] || die "missing $bin/$b; run without --no-build"
done

work=${E2E_DIR:-/tmp/pq-e2e}
rm -rf "$work"
mkdir -p "$work"
pids=()
# SIGINT every service, then SIGKILL whatever is still up after 5 s
stop_services() {
    local pid
    for pid in "${pids[@]}"; do kill -INT "$pid" 2> /dev/null || true; done
    for pid in "${pids[@]}"; do
        for _ in $(seq 50); do kill -0 "$pid" 2> /dev/null || break; sleep 0.1; done
        kill -KILL "$pid" 2> /dev/null || true
        wait "$pid" 2> /dev/null || true
    done
    pids=()
}
cleanup() {
    local status=$?
    trap - EXIT INT TERM
    stop_services
    if ((status != 0 && status != 130)); then
        for f in "$work"/*.log; do
            [[ -f $f ]] || continue
            log "last lines of $(basename "$f"):"
            tail -n 15 "$f" >&2
        done
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

in_use() { ss -Hltun "sport = :$1" | grep -q .; }
taken=()
free_port() {
    local p
    while :; do
        p=$((20000 + RANDOM % 40000))
        [[ " ${taken[*]} " == *" $p "* ]] && continue
        in_use "$p" && continue
        taken+=("$p")
        echo "$p"
        return
    done
}
udp_ports=()
for ((i = 0; i < nodes; i++)); do udp_ports+=("$(free_port)"); done
rpc_port=$(free_port)
explorer_port=$(free_port)
genesis_ms=$((3000 + 300 * (nodes - 1)))
genesis=$(($(date +%s%N) / 1000000 + genesis_ms))
for ((i = 0; i < nodes; i++)); do
    mkdir -p "$work/ledger-$i"
    write_pq_config "$work/node-$i.toml" "$i" "$genesis" "$work/ledger-$i" "${udp_ports[@]}"
done
ledger=$work/ledger-0
validators=$(for ((i = 0; i < nodes; i++)); do printf '%s=127.0.0.1:%s,' "$i" "${udp_ports[i]}"; done)
validators=${validators%,}

export RUST_LOG=${RUST_LOG:-info}
start() {
    local name=$1
    shift
    "$@" > "$work/$name.log" 2>&1 &
    pids+=($!)
}
alive() {
    local pid
    for pid in "${pids[@]}"; do
        kill -0 "$pid" 2> /dev/null || die "a service exited early (pid $pid)"
    done
}
# wait_for <secs> <what> <command...>
wait_for() {
    local secs=$1 what=$2
    shift 2
    local until=$((SECONDS + secs))
    until "$@" > /dev/null 2>&1; do
        alive
        ((SECONDS < until)) || die "timed out after ${secs}s waiting for $what"
        sleep 0.1
    done
}

for ((i = 0; i < nodes; i++)); do
    start "node-$i" "$bin/monad-pq-node" "$work/node-$i.toml"
done
for ((i = 0; i < nodes; i++)); do
    wait_for 10 "node $i to bind udp" grep -q "udp bound" "$work/node-$i.log"
done
log "$nodes pq node(s) up, genesis in ${genesis_ms} ms, work dir $work"

rpc_url=http://127.0.0.1:$rpc_port
explorer_url=http://127.0.0.1:$explorer_port
start rpc "$bin/monad-mcp-rpc" --http-addr "127.0.0.1:$rpc_port" --node-config "$work/node-0.toml"
start explorer "$bin/monad-mcp-explorer" --ledger-dir "$ledger" \
    --http-addr "127.0.0.1:$explorer_port" --rpc-url "$rpc_url" \
    --tail-interval-ms "${EXPLORER_TAIL_MS:-20}"
wait_for 15 "rpc health" sh -c "curl -sf '$rpc_url/health' | jq -e '.ok == true'"
wait_for 15 "explorer config" curl -sf "$explorer_url/api/config"
wait_for 30 "the explorer to index 10 blocks" \
    sh -c "curl -sf '$explorer_url/api/stats' | jq -e '.indexing.complete and .totals.blocks >= 10'"
sleep "$warmup"
log "stack up: explorer $explorer_url, rpc $rpc_url; load for ${seconds}s"

python3 "$here/scripts/e2e_load.py" --explorer "$explorer_url" --rpc "$rpc_url" \
    --mcp-tx "$bin/mcp-tx" --ledger-dir "$ledger" --validators "$validators" \
    --seconds "$seconds" --interval-ms "$interval_ms" | tee "$work/load.txt"

# the rpc reports a tx committed once it reads it back from the ledger
"$bin/mcp-tx" send --rpc "$rpc_url" --count 3 --interval 50 --wait 10 > "$work/rpc-check.jsonl" \
    || die "mcp-tx: a tx sent through the rpc did not commit"
log "mcp-tx status: $(tail -n 1 "$work/rpc-check.jsonl" | jq -r '.state + " in slot " + (.slot|tostring) + " (" + .path + ")"')"
alive
sleep 1

echo
"$bin/examples/latency" --ledger-dir "$ledger" --label explorer=e2e-explorer --label rpc=e2e-rpc \
    --label udp=e2e-udp | tee "$work/latency.txt"

# stop everything before reading the logs, so the summary covers whole runs
cleanup_status=0
stop_services
python3 "$here/scripts/summarize.py" --dir "$work" --nodes "$nodes" || cleanup_status=$?
if grep -l 'panicked' "$work"/*.log > /dev/null 2>&1; then
    die "a service panicked: $(grep -l 'panicked' "$work"/*.log | xargs -n1 basename)"
fi
exit "$cleanup_status"
