#!/usr/bin/env bash
# renders dist/config/<host>.toml for one shared genesis; see $usage below.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

genesis=
delta=150
slot_interval=100
slots_per_window=100
sync_boundary=80
num_proposals=5
propose_before=500
completed_slot_retention=50
repeater_interval=5000
repeater_retention=50
withhold_before=0

usage="usage: gen-config.sh --genesis <unix_ms> [--port n] [--delta ms] [--slot-interval ms] [--slots-per-window n] [--sync-boundary n] [--num-proposals n] [--propose-before ms] [--repeater-interval ms] [--repeater-retention n] [--withhold-before ms]"

while [ $# -gt 0 ]; do
    case $1 in
        --genesis) genesis=${2:?$usage}; shift ;;
        --port) port=${2:?$usage}; shift ;;
        --delta) delta=${2:?$usage}; shift ;;
        --slot-interval) slot_interval=${2:?$usage}; shift ;;
        --slots-per-window) slots_per_window=${2:?$usage}; shift ;;
        --sync-boundary) sync_boundary=${2:?$usage}; shift ;;
        --num-proposals) num_proposals=${2:?$usage}; shift ;;
        --propose-before) propose_before=${2:?$usage}; shift ;;
        --repeater-interval) repeater_interval=${2:?$usage}; shift ;;
        --repeater-retention) repeater_retention=${2:?$usage}; shift ;;
        --withhold-before) withhold_before=${2:?$usage}; shift ;;
        *) die "$usage" ;;
    esac
    shift
done

[ -n "$genesis" ] || die "$usage"
[[ $genesis =~ ^[0-9]+$ ]] || die "--genesis must be unix milliseconds"
now=$(now_ms)
[ "$genesis" -gt "$now" ] || die "genesis $genesis is not in the future (now $now)"

declare -A ip_of
for host in $(hosts); do
    ip_of[$host]=$(ipv4_of "$host")
done

# the validator set, identical in every config
validator_section() {
    local host
    for host in $(hosts); do
        echo
        echo "[[validators]]"
        echo "node_id = $(node_id_of "$host")"
        echo "stake = 1"
        echo "chorus_pubkey = $(node_id_of "$host")"
        echo "address = \"${ip_of[$host]}:$port\""
    done
}

validators=$(validator_section)

rm -rf "$config_dir"
mkdir -p "$config_dir"

for host in $(hosts); do
    id=$(node_id_of "$host")
    {
        echo "node_id = $id"
        echo "proposal_key_pair = $id"
        echo "cadence_key_pair = $id"
        echo "genesis_deadline = $genesis"
        echo "$validators"
        echo
        echo "[network]"
        echo "port = $port"
        echo
        echo "[cadence]"
        echo "delta = $delta"
        echo "slot_interval = $slot_interval"
        echo "slots_per_window = $slots_per_window"
        echo "sync_boundary_slots = $sync_boundary"
        echo
        echo "[repeater]"
        echo "interval = $repeater_interval"
        echo "certificate_retention = $repeater_retention"
        echo
        echo "[da]"
        echo "completed_slot_retention = $completed_slot_retention"
        echo
        echo "[proposal]"
        echo "num_proposals = $num_proposals"
        echo "propose_before_deadline = $propose_before"
        echo "withhold_before_deadline = $withhold_before"
    } > "$config_dir/$host.toml"
    echo "$config_dir/$host.toml  node_id=$id  address=${ip_of[$host]}:$port"
done

echo "genesis_deadline = $genesis ($(iso_of_ms "$genesis"), in $(((genesis - now) / 1000))s)"
