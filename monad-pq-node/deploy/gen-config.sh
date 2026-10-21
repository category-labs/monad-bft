#!/usr/bin/env bash
# renders config/<host>/node.toml for one shared genesis; see $usage below.
# --only-genesis rewrites just the genesis_deadline line, keeping local edits.
# Defaults are the mcp_test row of docs/pq-cadence.md §8.3.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

genesis=
delta=150
slot_interval=100
slots_per_window=100
sync_boundary=80
lag_threshold=20
num_proposals=5
propose_before=150
completed_slot_retention=50
repeater_interval=500
repeater_retention=50
# empty: derived after parsing, so they follow --delta and --propose-before
withhold_before=
retransmit_interval=
decode_grace=
# lane vacant 5 of every 5 + 95 slots: all K lanes 95% of the time, tenure K * 100 slots
rotation_slack=95
release_chunks=vote
# mempool: txs from monad-mcp-rpc; random: load without clients, and no [rpc] table
proposal_source=mempool
keep_genesis=no
force=no
only_genesis=no
render_args=no

usage="usage: gen-config.sh --genesis <unix_ms> [--keep-genesis] { --only-genesis | [--force] [--source mempool|random] [--port n] [--delta ms] [--slot-interval ms] [--slots-per-window n] [--sync-boundary n] [--lag-threshold n] [--num-proposals n] [--propose-before ms] [--withhold-before ms] [--repeater-interval ms] [--repeater-retention n] [--rotation-slack n] [--retransmit-interval ms] [--decode-grace ms] [--release-chunks vote|receipt] }"

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
        --lag-threshold) lag_threshold=${2:?$usage}; shift ;;
        --num-proposals) num_proposals=${2:?$usage}; shift ;;
        --propose-before) propose_before=${2:?$usage}; shift ;;
        --withhold-before) withhold_before=${2:?$usage}; shift ;;
        --repeater-interval) repeater_interval=${2:?$usage}; shift ;;
        --repeater-retention) repeater_retention=${2:?$usage}; shift ;;
        --rotation-slack) rotation_slack=${2:?$usage}; shift ;;
        --retransmit-interval) retransmit_interval=${2:?$usage}; shift ;;
        --decode-grace) decode_grace=${2:?$usage}; shift ;;
        --release-chunks) release_chunks=${2:?$usage}; shift ;;
        *) die "$usage" ;;
    esac
    shift
done

[ -n "$genesis" ] || die "$usage"
case $proposal_source in random | mempool) ;; *) die "--source is mempool or random" ;; esac
case $release_chunks in vote | receipt) ;; *) die "--release-chunks is vote or receipt" ;; esac
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

# §8.3: withhold = L - 10, retransmit = delta, decode_grace = delta / 2
withhold_before=${withhold_before:-$((propose_before - 10))}
retransmit_interval=${retransmit_interval:-$delta}
decode_grace=${decode_grace:-$((delta / 2))}
# the node refuses these at startup; failing here keeps a bad render off the hosts
[ $((decode_grace * 2)) -le "$delta" ] || die "--decode-grace $decode_grace exceeds delta / 2 ($delta / 2)"
[ "$repeater_retention" -ge "$lag_threshold" ] \
    || die "--repeater-retention $repeater_retention is below --lag-threshold $lag_threshold"
[ "$retransmit_interval" -gt 0 ] && [ "$repeater_interval" -gt 0 ] \
    || die "--retransmit-interval and --repeater-interval must be positive"
[ "$withhold_before" -le "$propose_before" ] \
    || die "--withhold-before $withhold_before exceeds --propose-before $propose_before"

existing=$(for host in $(hosts); do [ ! -f "$(host_config "$host")" ] || echo "$host"; done | xargs)
[ -z "$existing" ] || [ "$force" = yes ] \
    || die "config exists for $existing; --force overwrites local edits (see git diff $config_dir), --only-genesis keeps them"

declare -A ip_of
for host in $(hosts); do
    ip_of[$host]=$(ipv4_of "$host")
done

# the validator set, identical in every config; equal stakes are a pq requirement
validator_section() {
    local host
    for host in $(hosts); do
        echo
        echo "# $host"
        echo "[[validators]]"
        echo "node_id = $(node_id_of "$host")"
        echo "stake = 1"
        echo "chorus_pubkey = $(node_id_of "$host")"
        echo "address = \"${ip_of[$host]}:$port\""
    done
}

validators=$(validator_section)

# only with a matrix to ship: an rpc whose latency file is missing does not start.
# the node refuses an [rpc] table unless the source is mempool.
latency=no
if [ "$proposal_source" = mempool ] \
    && have_latency $(for host in $(hosts); do echo "${ip_of[$host]}"; done); then
    latency=yes
fi

for host in $(hosts); do
    id=$(node_id_of "$host")
    f=$(host_config "$host")
    mkdir -p "$(dirname "$f")"
    {
        echo "node_id = $id"
        echo "proposal_key_pair = $id"
        echo "# unused by pq consensus; kept for NodeConfig and rpc compatibility"
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
        echo "lag_threshold = $lag_threshold"
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
        echo "[mempool]"
        echo
        echo "[ledger]"
        echo "dir = \"/home/$ssh_user/$remote_rel/ledger\""
        echo
        echo "[quantum]"
        echo "signature = \"stub\""
        echo "stub_signature_len = 32"
        echo "retransmit_interval = $retransmit_interval"
        echo "decode_grace = $decode_grace"
        echo "coda_timeout_deltas = 5"
        echo "release_chunks = \"$release_chunks\""
        echo "early_vote = true"
        echo "ledger_proof = \"path-votes\""
        echo "evidence = true"
        if [ "$latency" = yes ]; then
            echo
            echo "[rpc]"
            echo "latency = \"latency.toml\""
        fi
    } > "$f"
    echo "$f  node_id=$id  address=${ip_of[$host]}:$port"
done

if [ "$latency" = yes ]; then
    echo "rpc routes unpinned txs by $latency_file (push-config.sh ships it)"
elif [ "$proposal_source" = mempool ]; then
    echo "no $latency_file: rpc routes unpinned txs by tenure (latency.sh measures it)"
else
    echo "source=random: no [rpc] table, and txs sent to the rpc never commit"
fi

echo "genesis_deadline = $genesis ($(iso_of_ms "$genesis"), in $(((genesis - now) / 1000))s)"
