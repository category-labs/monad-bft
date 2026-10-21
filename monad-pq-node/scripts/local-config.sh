#!/usr/bin/env bash
#
# Sourced by swarm.sh and e2e.sh: writes a pq node.toml with the local demo
# values of the design (§8.3). The file is also a monad-mcp-node NodeConfig,
# so the unchanged monad-mcp-rpc reads it.
#
#   write_pq_config <out> <node_id> <genesis_ms> <ledger_dir> <port_0> ... <port_n-1>
#
# Overrides (env): PQ_DELTA_MS (50), PQ_SLOT_MS (100), PQ_PROPOSE_MS (100),
# PQ_WITHHOLD_MS (90), PQ_ROTATION_SLACK (95; 995 for a lone validator), PQ_REPEATER_MS (200),
# PQ_RETRANSMIT_MS (= delta), PQ_DECODE_GRACE_MS (= delta / 2),
# PQ_SOURCE (mempool), PQ_RELEASE_CHUNKS (vote), PQ_EARLY_VOTE (false),
# PQ_RPC_RESEND_MS (500).

write_pq_config() {
    local out=$1 id=$2 genesis=$3 ledger=$4
    shift 4
    local ports=("$@")
    local delta=${PQ_DELTA_MS:-50}
    # a lone validator cannot cover its lane's 5-slot rotation vacancy: rotate every 1000 slots, not 100
    local slack=${PQ_ROTATION_SLACK:-$((${#ports[@]} == 1 ? 995 : 95))}
    local v
    {
        cat << EOF
node_id = $id
proposal_key_pair = $id
cadence_key_pair = $id
genesis_deadline = $genesis
EOF
        for v in "${!ports[@]}"; do
            cat << EOF

[[validators]]
node_id = $v
stake = 1
chorus_pubkey = $v
address = "127.0.0.1:${ports[v]}"
EOF
        done
        cat << EOF

[network]
port = ${ports[id]}

[cadence]
delta = $delta
slot_interval = ${PQ_SLOT_MS:-100}
slots_per_window = 100
sync_boundary_slots = 80
lag_threshold = 20

[repeater]
interval = ${PQ_REPEATER_MS:-200}
certificate_retention = 50

[da]
completed_slot_retention = 50

[proposal]
source = "${PQ_SOURCE:-mempool}"
num_proposals = 5
propose_before_deadline = ${PQ_PROPOSE_MS:-100}
withhold_before_deadline = ${PQ_WITHHOLD_MS:-90}

[leader_election]
rotation_slack = $slack

[ledger]
dir = "$ledger"

[quantum]
retransmit_interval = ${PQ_RETRANSMIT_MS:-$delta}
decode_grace = ${PQ_DECODE_GRACE_MS:-$((delta / 2))}
release_chunks = "${PQ_RELEASE_CHUNKS:-vote}"
early_vote = ${PQ_EARLY_VOTE:-false}
EOF
        if [ "${PQ_SOURCE:-mempool}" = mempool ]; then
            printf '\n[rpc]\nresend_after_ms = %s\n' "${PQ_RPC_RESEND_MS:-500}"
        fi
    } > "$out"
}
