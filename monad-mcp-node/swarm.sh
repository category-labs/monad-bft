#!/usr/bin/env bash
#
# Run a local swarm of stub validators on loopback, one process per
# node, then summarize what they finalized.
#
#   swarm.sh [nodes] [seconds]
#
# Defaults: 10 nodes for 60 seconds. Configs and logs are written to
# $SWARM_DIR (/tmp/mcp-swarm). Ports count up from $BASE_PORT (9100).
# Genesis is $GENESIS_DELAY_MS (5000) after launch.

set -euo pipefail

nodes=${1:-10}
seconds=${2:-60}
dir=${SWARM_DIR:-/tmp/mcp-swarm}
base_port=${BASE_PORT:-9100}
genesis_delay_ms=${GENESIS_DELAY_MS:-5000}

crate_dir=$(cd "$(dirname "$0")" && pwd)
repo_dir=$(cd "$crate_dir/.." && pwd)
host=$(rustc -vV | sed -n 's/^host: //p')
binary="$repo_dir/target/$host/debug/monad-mcp-node"

last_node=$((nodes - 1))

# ---------------------------------------------------------------- build

# prints cargo's output only when the build fails
build_binary() {
    echo "building for $host"
    local output="$dir/build.log"
    mkdir -p "$dir"
    if ! (cd "$repo_dir" && cargo build -p monad-mcp-node --target "$host" > "$output" 2>&1); then
        cat "$output"
        exit 1
    fi
}

# -------------------------------------------------------------- configs

port_of() {
    echo $((base_port + $1))
}

# the validator set, identical in every config
validator_section() {
    local i
    for i in $(seq 0 $last_node); do
        echo
        echo "[[validators]]"
        echo "node_id = $i"
        echo "stake = 1"
        echo "chorus_pubkey = $i"
        echo "address = \"127.0.0.1:$(port_of $i)\""
    done
}

write_config() {
    local i=$1
    local genesis=$2
    {
        echo "node_id = $i"
        echo "proposal_key_pair = $i"
        echo "cadence_key_pair = $i"
        echo "genesis_deadline = $genesis"
        validator_section
        echo
        echo "[network]"
        echo "port = $(port_of $i)"
    } > "$dir/node-$i.toml"
}

write_configs() {
    local now_ms
    now_ms=$(python3 -c 'import time; print(int(time.time() * 1000))')
    local genesis=$((now_ms + genesis_delay_ms))

    local i
    for i in $(seq 0 $last_node); do
        write_config "$i" "$genesis"
    done
}

# ------------------------------------------------------------------ run

pids=()

stop_nodes() {
    kill "${pids[@]}" 2> /dev/null || true
    wait 2> /dev/null || true
}

run_nodes() {
    echo "launching $nodes nodes for ${seconds}s, logs in $dir"
    trap stop_nodes EXIT INT TERM
    local i
    for i in $(seq 0 $last_node); do
        RUST_LOG=${RUST_LOG:-info} "$binary" "$dir/node-$i.toml" > "$dir/node-$i.log" 2>&1 &
        pids+=($!)
    done
    sleep "$seconds"
    stop_nodes
}

# -------------------------------------------------------------- summary

# the node's log without terminal colors
log_of() {
    sed -E 's/\x1b\[[0-9;]*m//g' "$dir/node-$1.log"
}

count_lines() {
    grep -c -E "$1" || true
}

# the slots a node finalized, one per line, sorted
finalized_slots_of() {
    log_of "$1" | grep 'finalized' | grep -o -E 'slot=[0-9]+' | sort
}

# "finalized ... block=+-+++": one char per proposal, + committed
report_per_node() {
    echo
    echo "node  finalized  all-committed  proposed  warnings  errors"
    local i
    for i in $(seq 0 $last_node); do
        local log
        log=$(log_of "$i")
        local finalized full proposed warnings errors
        finalized=$(count_lines 'finalized' <<< "$log")
        full=$(count_lines 'block="?\++"?( |$)' <<< "$log")
        proposed=$(count_lines 'proposing' <<< "$log")
        warnings=$(count_lines ' WARN' <<< "$log")
        errors=$(count_lines ' ERROR|panicked' <<< "$log")
        printf '%-5s %-10s %-14s %-9s %-9s %s\n' "$i" "$finalized" "$full" "$proposed" "$warnings" "$errors"
    done
}

report_agreement() {
    echo
    finalized_slots_of 0 > "$dir/slots-0.txt"
    local disagreements=0
    local i
    for i in $(seq 1 $last_node); do
        finalized_slots_of "$i" > "$dir/slots-$i.txt"
        if ! cmp -s "$dir/slots-0.txt" "$dir/slots-$i.txt"; then
            echo "node $i finalized a different slot set than node 0"
            disagreements=$((disagreements + 1))
        fi
    done
    if [ "$disagreements" = 0 ]; then
        local count
        count=$(wc -l < "$dir/slots-0.txt" | tr -d ' ')
        echo "all nodes finalized the same $count slots"
    fi
}

report_warnings() {
    local warnings
    local i
    warnings=$(
        for i in $(seq 0 $last_node); do
            log_of "$i" | grep ' WARN'
        done | sed -E 's/^[^ ]+ +WARN +//' | cut -c1-100 | sort | uniq -c | sort -rn | head -5
    )
    if [ -n "$warnings" ]; then
        echo
        echo "most frequent warnings:"
        echo "$warnings"
    fi
}

summarize() {
    report_per_node
    report_agreement
    report_warnings
}

# ----------------------------------------------------------------- main

rm -rf "$dir"
build_binary
write_configs
run_nodes
summarize
