#!/usr/bin/env bash
#
# End-to-end run of the mcp tx demo on localhost: one node with 100 ms
# slots proposing from its mempool, the tx rpc colocated with it (reading its
# config, sending txs over udp to the leader it picks) and the explorer, each
# on temp dirs and free ports, then the Playwright suites (L6a-L6d).
#
# usage: run.sh [--swarm [N]] [--serve] [--no-build] [-- <playwright test args>]
#   --swarm N   N validators (default 5) on localhost sharing one genesis, rpc
#               and explorer on node 0; runs the swarm suite instead of L6a-L6d
#   --serve     start the stack, print its urls and wait for ctrl-c; no tests
#   --no-build  use the binaries already in the cargo target dir
# env: E2E_SCREENSHOT_DIR (default e2e/test-results), E2E_KEEP=1 keeps the temp dir
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(cd "$here/../.." && pwd)
serve=0
build=1
swarm=0
nodes=1
while (($#)); do
    case $1 in
        --swarm)
            swarm=1
            nodes=5
            if [[ ${2:-} =~ ^[0-9]+$ ]]; then
                nodes=$2
                shift
            fi
            ((nodes >= 2)) || { echo "run.sh: --swarm needs at least 2 nodes" >&2; exit 2; }
            ;;
        --serve) serve=1 ;;
        --no-build) build=0 ;;
        --) shift; break ;;
        *) echo "run.sh: unknown argument $1" >&2; exit 2 ;;
    esac
    shift
done

log() { printf '[e2e %s] %s\n' "$(date +%T)" "$*" >&2; }
die() { log "error: $*"; exit 1; }

if ((build)); then
    log "building node, rpc and explorer"
    (cd "$repo" && cargo build -p monad-mcp-node -p monad-mcp-rpc -p monad-mcp-explorer --bins)
fi
target=${CARGO_TARGET_DIR:-$(cd "$repo" && cargo metadata --format-version 1 --no-deps | jq -r .target_directory)}
bin=$target/debug
for b in monad-mcp-node monad-mcp-rpc monad-mcp-explorer mcp-tx; do
    [[ -x $bin/$b ]] || die "missing $bin/$b; run without --no-build"
done

work=$(mktemp -d "${TMPDIR:-/tmp}/mcp-e2e.XXXXXX")
pids=()
cleanup() {
    local status=$?
    trap - EXIT INT TERM
    for pid in "${pids[@]}"; do kill -TERM "$pid" 2> /dev/null || true; done
    for pid in "${pids[@]}"; do
        for _ in $(seq 50); do kill -0 "$pid" 2> /dev/null || break; sleep 0.1; done
        kill -KILL "$pid" 2> /dev/null || true
        wait "$pid" 2> /dev/null || true
    done
    if ((status != 0 && status != 130)); then
        for f in "$work"/*.log; do
            [[ -f $f ]] || continue
            log "last lines of $(basename "$f"):"
            tail -n 25 "$f" >&2
        done
    fi
    if [[ ${E2E_KEEP:-0} == 1 ]]; then
        log "kept $work"
    else
        rm -rf "$work"
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
node_ledger() { if ((swarm)); then echo "$work/ledger-$1"; else echo "$work/ledger"; fi; }
slot_ms=100
genesis_ms=$((3000 + 500 * (nodes - 1)))
genesis=$(($(date +%s%N) / 1000000 + genesis_ms))

for ((i = 0; i < nodes; i++)); do
    mkdir -p "$(node_ledger "$i")"
    {
        cat << EOF
node_id = $i
proposal_key_pair = $i
cadence_key_pair = $i
genesis_deadline = $genesis
EOF
        for ((v = 0; v < nodes; v++)); do
            cat << EOF

[[validators]]
node_id = $v
stake = 1
chorus_pubkey = $v
address = "127.0.0.1:${udp_ports[v]}"
EOF
        done
        cat << EOF

[network]
port = ${udp_ports[i]}

[cadence]
delta = 50
slot_interval = $slot_ms

[proposal]
num_proposals = 5
propose_before_deadline = 200
withhold_before_deadline = 0
source = "mempool"

[ledger]
dir = "$(node_ledger "$i")"
EOF
    } > "$work/node-$i.toml"
done
node_addr=127.0.0.1:${udp_ports[0]}
ledger=$(node_ledger 0)
ledgers=()
for ((i = 0; i < nodes; i++)); do ledgers+=("$(node_ledger "$i")"); done

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
    log "starting node $i (udp ${udp_ports[i]}, genesis in $genesis_ms ms) in $work"
    start "node-$i" "$bin/monad-mcp-node" "$work/node-$i.toml"
done
# udp has no handshake: a node is up once it logs its bound socket
for ((i = 0; i < nodes; i++)); do
    wait_for 10 "node $i to bind udp" grep -q "udp bound" "$work/node-$i.log"
done

rpc_url=http://127.0.0.1:$rpc_port
explorer_url=http://127.0.0.1:$explorer_port
# the rpc takes node 0's own config: its id, the validator set, schedule, clock and ledger
start rpc "$bin/monad-mcp-rpc" --http-addr "127.0.0.1:$rpc_port" --node-config "$work/node-0.toml"
start explorer "$bin/monad-mcp-explorer" --ledger-dir "$ledger" \
    --http-addr "127.0.0.1:$explorer_port" --rpc-url "$rpc_url"

wait_for 15 "rpc health" sh -c "curl -sf '$rpc_url/health' | jq -e '.ok == true'"
wait_for 15 "explorer config" curl -sf "$explorer_url/api/config"
# ten blocks past genesis: the chain is finalizing and the explorer tails it
wait_for 30 "the explorer to index 10 blocks" \
    sh -c "curl -sf '$explorer_url/api/stats' | jq -e '.indexing.complete and .totals.blocks >= 10'"
# block directories finalized so far by the ledger at $1
blocks() { ls "$1/blocks" 2> /dev/null | grep -E '^[0-9]{12}$'; }
# past the genesis ramp-up some recent block of node 0 has a proposer at every index
all_lanes_occupied() {
    local b
    for b in $(blocks "$ledger" | tail -n 20); do
        jq -e '[.lanes[].proposer] | all(. != null)' "$ledger/blocks/$b/meta.json" && return 0
    done
    return 1
}
finalized_at_least() { (($(blocks "$2" | wc -l) >= $1)); }
if ((swarm)); then
    for ((i = 1; i < nodes; i++)); do
        wait_for 30 "node $i to finalize 10 blocks" finalized_at_least 10 "${ledgers[i]}"
    done
    wait_for 30 "a block with all lanes occupied" all_lanes_occupied
fi
log "stack up: $nodes node(s), explorer $explorer_url, rpc $rpc_url, node 0 at $node_addr"

export E2E_BIN_DIR=$bin E2E_RPC_URL=$rpc_url E2E_EXPLORER_URL=$explorer_url
export E2E_NODE_ADDR=$node_addr E2E_LEDGER_DIR=$ledger
E2E_LEDGER_DIRS=$(IFS=:; echo "${ledgers[*]}")
export E2E_LEDGER_DIRS
if ((swarm)); then export E2E_SWARM=$nodes; else unset E2E_SWARM; fi
export E2E_SLOT_MS=$slot_ms
export E2E_SCREENSHOT_DIR=${E2E_SCREENSHOT_DIR:-$here/test-results}

if ((serve)); then
    log "serving; ctrl-c to stop"
    while :; do alive; sleep 1; done
fi

cd "$here"
[[ -d node_modules/@playwright/test ]] || npm ci --no-audit --no-fund
status=0
npx playwright test "$@" || status=$?
alive
if grep -l 'panicked' "$work"/*.log > /dev/null 2>&1; then
    die "a service panicked: $(grep -l 'panicked' "$work"/*.log | xargs -n1 basename)"
fi
exit "$status"
