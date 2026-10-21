#!/usr/bin/env bash
#
# Run a local swarm of pq stub validators on loopback, one process per
# node, then summarize what they finalized: per-node counts, agreement,
# path breakdown (fast / coda / gadget) and finalization latency.
#
#   swarm.sh [nodes] [seconds]
#
# Defaults: 4 nodes for 60 seconds. Configs, logs and ledgers are written
# to $SWARM_DIR (/tmp/pq-swarm). Ports count up from $BASE_PORT (9300).
# Genesis is $GENESIS_DELAY_MS (3000) after launch. NO_BUILD=1 reuses the
# release binaries; the PQ_* variables of scripts/local-config.sh override
# the local demo values.

set -euo pipefail

nodes=${1:-4}
seconds=${2:-60}
dir=${SWARM_DIR:-/tmp/pq-swarm}
base_port=${BASE_PORT:-9300}
genesis_delay_ms=${GENESIS_DELAY_MS:-3000}

crate_dir=$(cd "$(dirname "$0")" && pwd)
repo_dir=$(cd "$crate_dir/.." && pwd)
# shellcheck source=scripts/local-config.sh
source "$crate_dir/scripts/local-config.sh"

last_node=$((nodes - 1))

# ---------------------------------------------------------------- build

target_dir() {
    if [[ -n ${CARGO_TARGET_DIR:-} ]]; then
        echo "$CARGO_TARGET_DIR"
    else
        (cd "$repo_dir" && cargo metadata --format-version 1 --no-deps | jq -r .target_directory)
    fi
}

# prints cargo's output only when the build fails
build_binaries() {
    echo "building monad-pq-node (release)"
    local output="$dir/build.log"
    if ! (cd "$repo_dir" && cargo build --release -p monad-pq-node --bin monad-pq-node --example latency > "$output" 2>&1); then
        cat "$output"
        exit 1
    fi
}

# -------------------------------------------------------------- configs

write_configs() {
    local now_ms genesis i
    now_ms=$(($(date +%s%N) / 1000000))
    genesis=$((now_ms + genesis_delay_ms))
    echo "$genesis" > "$dir/genesis_ms"
    local ports=()
    for i in $(seq 0 $last_node); do
        ports+=($((base_port + i)))
    done
    for i in $(seq 0 $last_node); do
        mkdir -p "$dir/ledger-$i"
        write_pq_config "$dir/node-$i.toml" "$i" "$genesis" "$dir/ledger-$i" "${ports[@]}"
    done
}

# ------------------------------------------------------------------ run

pids=()

# SIGINT every node, then SIGKILL whatever is still up after 5 s
stop_nodes() {
    local pid
    kill -INT "${pids[@]}" 2> /dev/null || true
    for pid in "${pids[@]}"; do
        for _ in $(seq 50); do kill -0 "$pid" 2> /dev/null || break; sleep 0.1; done
        kill -KILL "$pid" 2> /dev/null || true
    done
    wait 2> /dev/null || true
}

run_nodes() {
    echo "launching $nodes nodes for ${seconds}s, logs in $dir"
    trap stop_nodes EXIT INT TERM
    local i
    for i in $(seq 0 $last_node); do
        RUST_LOG=${RUST_LOG:-info} "$bin/monad-pq-node" "$dir/node-$i.toml" > "$dir/node-$i.log" 2>&1 &
        pids+=($!)
    done
    sleep "$seconds"
    stop_nodes
    trap - EXIT INT TERM
}

# -------------------------------------------------------------- summary

summarize() {
    local status=0
    python3 "$crate_dir/scripts/summarize.py" --dir "$dir" --nodes "$nodes" || status=$?
    echo
    "$bin/examples/latency" --ledger-dir "$dir/ledger-0"
    return "$status"
}

# ----------------------------------------------------------------- main

rm -rf "$dir"
mkdir -p "$dir"
bin=$(target_dir)/release
if [[ ${NO_BUILD:-0} != 1 ]]; then
    build_binaries
fi
write_configs
run_nodes
summarize
