#!/usr/bin/env bash
# renders config/<host>/node.toml for one shared genesis; see $usage below.
# --only-genesis rewrites just the genesis_deadline line, keeping local edits.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

genesis=
delta=150
slot_interval=100
slots_per_window=100
sync_boundary=80
num_proposals=5
propose_before=200
completed_slot_retention=50
repeater_interval=500
repeater_retention=50
# withhold a seal that can no longer reach every validator by the deadline (= delta)
withhold_before=150
# lane vacant 5 of every 5 + 95 slots: all K lanes 95% of the time, tenure K * 100 slots
rotation_slack=95
# random: load without clients; mempool: txs from monad-mcp-rpc
proposal_source=random
keep_genesis=no
force=no
only_genesis=no
render_args=no

usage="usage: gen-config.sh --genesis <unix_ms> [--keep-genesis] { --only-genesis | [--force] [--source random|mempool] [--port n] [--delta ms] [--slot-interval ms] [--slots-per-window n] [--sync-boundary n] [--num-proposals n] [--propose-before ms] [--repeater-interval ms] [--repeater-retention n] [--withhold-before ms] [--rotation-slack n] }"

while [ $# -gt 0 ]; do
    case $1 in --genesis | --keep-genesis | --force | --only-genesis) ;; *) render_args=yes ;; esac
    case $1 in
        --genesis) genesis=${2:?$usage}; shift ;;
        --keep-genesis) keep_genesis=yes ;;
        --force) force=yes ;;
        --only-genesis) only_genesis=yes ;;
        --source) proposal_source=${2:?$usage}; shift ;;
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
        --rotation-slack) rotation_slack=${2:?$usage}; shift ;;
        *) die "$usage" ;;
    esac
    shift
done

[ -n "$genesis" ] || die "$usage"
case $proposal_source in random | mempool) ;; *) die "--source is random or mempool" ;; esac
[[ $genesis =~ ^[0-9]+$ ]] || die "--genesis must be unix milliseconds"
now=$(now_ms)
# --keep-genesis: the running network's genesis, for netctl.sh live-upgrade
[ "$keep_genesis" = yes ] || [ "$genesis" -gt "$now" ] \
    || die "genesis $genesis is not in the future (now $now)"

if [ "$only_genesis" = yes ]; then
    [ "$render_args" = no ] && [ "$force" = no ] || die "--only-genesis takes no render flags"
    missing=$(missing_configs | xargs)
    [ -z "$missing" ] || die "no config for $missing; render with gen-config.sh --genesis <ms> first"
    for host in $(hosts); do
        f=$(host_config "$host")
        [ "$(grep -c '^genesis_deadline *=' "$f")" = 1 ] || die "$f needs exactly one genesis_deadline line"
        awk -v g="$genesis" '/^genesis_deadline *=/ { $0 = "genesis_deadline = " g } { print }' "$f" > "$f.new"
        mv "$f.new" "$f"
        echo "$f  genesis_deadline=$genesis"
    done
    echo "genesis_deadline = $genesis ($(iso_of_ms "$genesis"), in $(((genesis - now) / 1000))s)"
    exit 0
fi

existing=$(for host in $(hosts); do [ ! -f "$(host_config "$host")" ] || echo "$host"; done | xargs)
[ -z "$existing" ] || [ "$force" = yes ] \
    || die "config exists for $existing; --force overwrites local edits (see git diff $config_dir), --only-genesis keeps them"

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

for host in $(hosts); do
    id=$(node_id_of "$host")
    f=$(host_config "$host")
    mkdir -p "$(dirname "$f")"
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
        echo "source = \"$proposal_source\""
        echo "num_proposals = $num_proposals"
        echo "propose_before_deadline = $propose_before"
        echo "withhold_before_deadline = $withhold_before"
        echo
        echo "[leader_election]"
        echo "rotation_slack = $rotation_slack"
        echo
        echo "[ledger]"
        echo "dir = \"/home/$ssh_user/$remote_rel/ledger\""
    } > "$f"
    echo "$f  node_id=$id  address=${ip_of[$host]}:$port"
done

echo "genesis_deadline = $genesis ($(iso_of_ms "$genesis"), in $(((genesis - now) / 1000))s)"
