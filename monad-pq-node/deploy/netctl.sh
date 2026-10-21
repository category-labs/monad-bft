#!/usr/bin/env bash
# every whole-network start mints a new genesis and wipes the ledger;
# live-upgrade restarts one host at a time on the running genesis and ledger.
# takeover stops the monad-mcp-* user units (they hold the same udp port) and
# starts pq; handback stops pq and restarts the mcp units on their own config,
# genesis and ledger. Neither ever touches monad-bft or the mcp files.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

usage="usage: netctl.sh start [--lead 60] [--keep-ledger] [-- gen-config.sh flags] | stop | restart [start flags] | status | logs <host> [-f] | upgrade [start flags] | live-upgrade [--hosts a,b] [--timeout 120] [--max-lag 20] [--no-build] [--dirty] [-- gen-config.sh flags] | run-one <host> start|stop|restart | takeover [--no-start] [start flags] | handback"

# the mcp units each host was running when takeover stopped them, for handback
takeover_dir=$dist_dir/takeover

port_free_host() {
    rssh "$1" "if ss -Hlun | tr -s ' ' '\n' | grep -q ':$port\$'; then
            echo '$port/udp already bound'
            exit 1
        fi
        echo '$port/udp free'"
}

# everything start needs is on the host, so takeover never stops mcp for nothing
deployed_host() {
    local host=$1 explorer=
    has_explorer "$host" && explorer="test -L $remote_root/bin/explorer-current
        test -f .config/systemd/user/$explorer_unit.service"
    rssh "$host" "set -e
        test -L $remote_root/bin/current
        test -L $remote_root/bin/rpc-current
        test -L $remote_root/bin/mcp-tx
        test -f .config/systemd/user/$unit.service
        test -f .config/systemd/user/$rpc_unit.service
        $explorer
        echo \"deployed: current -> \$(readlink $remote_root/bin/current)\""
}

ledger_reset_host() {
    local host=$1 keep=$2 ts=$3
    if [ "$keep" = yes ]; then
        rssh "$host" "set -e
            mkdir -p $remote_root/ledger/blocks
            mv -T $remote_root/ledger/blocks $remote_root/ledger/blocks-$ts
            mkdir -p $remote_root/ledger/blocks
            echo 'archived to ledger/blocks-$ts'"
    else
        # rsync from an empty dir deletes millions of block files far faster than find -delete
        rssh "$host" "set -e
            mkdir -p $remote_root/ledger/blocks $remote_root/empty-dir
            rsync -r --delete $remote_root/empty-dir/ $remote_root/ledger/blocks/
            rmdir $remote_root/empty-dir
            echo 'ledger/blocks emptied'"
    fi
}

start_host() {
    local host=$1
    if [ "$supervisor" = systemd ]; then
        rssh_user_systemd "$host" "set -e
            systemctl --user start $unit.service
            systemctl --user is-active $unit.service"
    else
        rssh "$host" "$remote_root/run.sh"
    fi
}

# the rpc reads node.toml at startup, so it is (re)started after its node;
# 2 s catches a config error, since Restart=on-failure would hide it as activating
rpc_start_host() {
    local host=$1
    if [ "$supervisor" != systemd ]; then
        echo "supervisor=$supervisor: no rpc"
        return
    fi
    rssh_user_systemd "$host" "systemctl --user restart $rpc_unit.service
        sleep 2
        state=\$(systemctl --user is-active $rpc_unit.service || true)
        echo rpc=\$state
        [ \"\$state\" = active ]"
}

# restarted after its node too, since a new genesis wipes the ledger it has indexed
explorer_start_host() {
    local host=$1
    has_explorer "$host" || return 0
    if [ "$supervisor" != systemd ]; then
        echo "supervisor=$supervisor: no explorer"
        return
    fi
    rssh_user_systemd "$host" "systemctl --user restart $explorer_unit.service
        sleep 2
        state=\$(systemctl --user is-active $explorer_unit.service || true)
        echo explorer=\$state
        [ \"\$state\" = active ]"
}

# the bracket keeps pgrep from matching the ssh command line itself
stop_host() {
    local host=$1
    if [ "$supervisor" = systemd ]; then
        # separate calls: an rpc or explorer unit not deployed must not keep the node running
        rssh_user_systemd "$host" "systemctl --user stop $explorer_unit.service 2> /dev/null
            systemctl --user stop $rpc_unit.service 2> /dev/null
            systemctl --user stop $unit.service" || true
    else
        rssh "$host" "if [ -s $remote_root/run/pid ]; then
                kill -INT \$(cat $remote_root/run/pid) 2> /dev/null || true
            fi"
    fi
    rssh "$host" "for i in 1 2 3 4 5; do
            pgrep -f 'monad-pq/bin/curren[t]' > /dev/null || break
            sleep 1
        done
        if pgrep -f 'monad-pq/bin/curren[t]' > /dev/null; then
            echo 'still running'
            exit 1
        fi
        echo stopped"
}

wait_bound_host() {
    local host=$1 since=$2
    rssh_user_systemd "$host" "for i in \$(seq 1 60); do
            line=\$($(node_log_cmd "$since") 2> /dev/null | grep -m1 'udp bound' || true)
            if [ -n \"\$line\" ]; then
                echo \"\$line\"
                exit 0
            fi
            sleep 1
        done
        echo 'no \"udp bound\" line after 60s'
        exit 1"
}

status_host() {
    local host=$1 explorer='explorer=none'
    has_explorer "$host" && explorer="explorer=\$(systemctl --user is-active $explorer_unit.service 2> /dev/null || true)"
    rssh_user_systemd "$host" "
        state=\$(systemctl --user is-active $unit.service 2> /dev/null || true)
        rpc=\$(systemctl --user is-active $rpc_unit.service 2> /dev/null || true)
        $explorer
        mcp=\$(systemctl --user is-active monad-mcp-node.service 2> /dev/null || true)
        since=\$(systemctl --user show $unit.service -p ActiveEnterTimestamp --value 2> /dev/null || true)
        bound=\$(ss -Hlun | tr -s ' ' '\n' | grep -c ':$port\$' || true)
        last=\$($(node_log_cmd $(($(date +%s) - 120))) 2> /dev/null | sed 's/\x1b\[[0-9;]*m//g' \
            | grep 'finalized slot=[0-9]' | tail -1)
        echo \"state=\${state:-unknown} rpc=\${rpc:-unknown} explorer=\${explorer:-unknown} mcp_node=\${mcp:-unknown} port_bound=\$bound since=\${since:-none}\"
        echo \"last=\${last:-no finalized line}\""
}

# stops explorer, rpc, node; prints `mcp_active=<units>` (start order) for handback
mcp_stop_host() {
    local host=$1 stop_order
    stop_order=$(tr ' ' '\n' <<< "$mcp_units" | tac | xargs)
    rssh_user_systemd "$host" "active=
        for u in $mcp_units; do
            case \$(systemctl --user is-active \$u.service 2> /dev/null) in
                active | activating | reloading) active=\"\$active \$u\" ;;
            esac
        done
        echo \"mcp_active=\${active# }\"
        for u in $stop_order; do
            systemctl --user stop \$u.service 2> /dev/null || true
        done
        for i in \$(seq 1 10); do
            pgrep -f 'monad-mcp/bin/curren[t]' > /dev/null || break
            sleep 1
        done
        if pgrep -f 'monad-mcp/bin/curren[t]' > /dev/null; then
            echo 'mcp node still running'
            exit 1
        fi
        for i in \$(seq 1 10); do
            ss -Hlun | tr -s ' ' '\n' | grep -q ':$port\$' || break
            sleep 1
        done
        if ss -Hlun | tr -s ' ' '\n' | grep -q ':$port\$'; then
            if systemctl --user is-active -q $unit.service; then
                echo '$unit already holds $port/udp; netctl.sh restart starts a new pq genesis'
            else
                echo '$port/udp still bound with the mcp node stopped (monad-bft?)'
            fi
            exit 1
        fi
        echo 'mcp stopped, $port/udp free'"
}

# starts the given mcp units (start order) on their existing config and ledger,
# or every enabled one when takeover left no record; the node first, since the
# mcp rpc reads its node.toml at startup
mcp_start_host() {
    local host=$1 units=
    [ ! -f "$takeover_dir/$host.units" ] || units=$(cat "$takeover_dir/$host.units")
    rssh_user_systemd "$host" "units='$units'
        if [ -z \"\$units\" ]; then
            for u in $mcp_units; do
                if systemctl --user is-enabled -q \$u.service 2> /dev/null; then
                    units=\"\$units \$u\"
                fi
            done
            echo \"no takeover record: starting the enabled units:\$units\"
        fi
        since=\$(date +%s)
        rc=0
        for u in \$units; do
            if [ \$u = monad-mcp-node ] && systemctl --user is-active -q \$u.service; then
                echo \"\$u: already active\"
            elif [ \$u = monad-mcp-node ]; then
                if ss -Hlun | tr -s ' ' '\n' | grep -q ':$port\$'; then
                    echo '$port/udp still bound; monad-mcp-node not started'
                    exit 1
                fi
                systemctl --user start \$u.service
                for i in \$(seq 1 60); do
                    line=\$(journalctl --user -u \$u -o cat --since @\$since 2> /dev/null | grep -m1 'udp bound' || true)
                    [ -z \"\$line\" ] || break
                    sleep 1
                done
                echo \"\$u: \${line:-no udp bound line after 60s}\"
                [ -n \"\$line\" ] || rc=1
            else
                systemctl --user restart \$u.service
                sleep 2
            fi
            state=\$(systemctl --user is-active \$u.service || true)
            echo \"\$u=\$state\"
            [ \"\$state\" = active ] || rc=1
        done
        exit \$rc"
}

cmd_start() {
    local lead=60 keep_ledger=no gen_args=()
    while [ $# -gt 0 ]; do
        case $1 in
            --lead) lead=${2:?$usage}; shift ;;
            --keep-ledger) keep_ledger=yes ;;
            --) shift; gen_args=("$@"); break ;;
            *) die "$usage" ;;
        esac
        shift
    done

    echo "checking $port/udp is free on $(host_count) hosts (netctl.sh takeover frees it from mcp)"
    fanout port_free_host

    local genesis start_ms since ts
    ts=$(($(now_ms) / 1000))
    echo "resetting the ledger (keep-ledger=$keep_ledger)"
    fanout ledger_reset_host "$keep_ledger" "$ts"

    # minted after the reset, which can take minutes, so the lead is not spent before the start
    start_ms=$(now_ms)
    genesis=$((start_ms + lead * 1000))
    since=$((start_ms / 1000 - 5))

    local missing
    missing=$(missing_configs | xargs)
    if [ ${#gen_args[@]} -gt 0 ] || [ "$missing" = "$(hosts | xargs)" ]; then
        "$deploy_dir/gen-config.sh" --genesis "$genesis" --force "${gen_args[@]}"
    elif [ -n "$missing" ]; then
        die "no config for $missing; re-render all with: netctl.sh start -- --force (overwrites local edits)"
    else
        # keeps local edits in config/<host>/
        "$deploy_dir/gen-config.sh" --genesis "$genesis" --only-genesis
    fi
    # recorded before the start fanout so a partial failure cannot leave a stale window
    mkdir -p "$dist_dir"
    echo "$genesis" > "$dist_dir/last-genesis"
    echo "$start_ms" > "$dist_dir/last-start"
    "$deploy_dir/push-config.sh"

    echo "starting the network"
    fanout start_host

    echo "waiting for the udp bind on every host"
    fanout wait_bound_host "$since"

    echo "starting the rpc on every host"
    fanout rpc_start_host

    echo "starting the explorer where config/<host>/explorer.env exists"
    fanout explorer_start_host

    echo "genesis_deadline = $genesis ($(iso_of_ms "$genesis")), recorded in $dist_dir/last-genesis"
}

cmd_stop() {
    [ $# = 0 ] || die "$usage"
    echo "stopping the network"
    fanout stop_host
}

cmd_status() {
    [ $# = 0 ] || die "$usage"
    if [ -f "$dist_dir/last-genesis" ]; then
        local genesis
        genesis=$(cat "$dist_dir/last-genesis")
        echo "last genesis $genesis ($(iso_of_ms "$genesis"))"
    fi
    fanout status_host
}

cmd_logs() {
    local host=${1:?$usage} follow=
    shift
    if [ "${1:-}" = -f ]; then
        follow=-f
        shift
    fi
    [ $# = 0 ] || die "$usage"
    node_id_of "$host" > /dev/null
    if [ "$supervisor" = systemd ]; then
        rssh "$host" "$(remote_env)journalctl --user -u $unit -n 200 $follow"
    else
        rssh "$host" "tail -n 200 $follow $remote_root/logs/node.log"
    fi
}

cmd_upgrade() {
    "$deploy_dir/build.sh"
    cmd_stop
    "$deploy_dir/deploy.sh"
    cmd_start "$@"
}

# health_host <host> <since unix s> <genesis ms> <slot_interval ms> <min finalized> <max lag>
# counts finalized lines since the node's latest `udp bound`, so a restart
# resets it even in the setsid log. rc 0 healthy, 1 not yet, 2 not running.
health_host() {
    local host=$1 since=$2 genesis=$3 interval=$4 need=$5 max_lag=$6 state_cmd
    if [ "$supervisor" = systemd ]; then
        state_cmd="systemctl --user is-active $unit.service 2> /dev/null || true"
    else
        state_cmd="[ -s $remote_root/run/pid ] && kill -0 \$(cat $remote_root/run/pid) 2> /dev/null && echo active || echo inactive"
    fi
    local prog='
        /udp bound/ { n = 0; tip = -1 }
        /finalized slot=/ {
            match($0, /slot=[0-9]+/); s = substr($0, RSTART + 5, RLENGTH - 5) + 0
            n++; if (s > tip) tip = s
        }
        END {
            clock = int((now - g) / iv)
            if (tip < 0) printf "state=%s finalized=0 tip=- clock=%d lag=-\n", state, clock
            else printf "state=%s finalized=%d tip=%d clock=%d lag=%d\n", state, n, tip, clock, clock - tip
            if (state != "active") exit 2
            exit !(n >= need && tip >= 0 && clock - tip <= maxlag)
        }'
    rssh_user_systemd "$host" "state=\$($state_cmd)
        now=\$((\$(date +%s%N) / 1000000))
        $(node_log_cmd "$since") 2> /dev/null | sed 's/\x1b\[[0-9;]*m//g' \
            | awk -v state=\"\$state\" -v now=\"\$now\" -v g=$genesis -v iv=$interval \
                -v need=$need -v maxlag=$max_lag -v tip=-1 -v n=0 '$prog'"
}

cmd_live_upgrade() {
    local timeout=120 need=10 max_lag=20 build=yes build_args=() subset= render=no gen_args=()
    while [ $# -gt 0 ]; do
        case $1 in
            --hosts) subset=${2:?$usage}; shift ;;
            --timeout) timeout=${2:?$usage}; shift ;;
            --max-lag) max_lag=${2:?$usage}; shift ;;
            --no-build) build=no ;;
            --dirty) build_args=(--dirty) ;;
            --) shift; render=yes; gen_args=("$@"); break ;;
            *) die "$usage" ;;
        esac
        shift
    done

    local upgrade_hosts
    if [ -n "$subset" ]; then
        upgrade_hosts=$(set_targets "$subset"; targets | xargs)
    else
        upgrade_hosts=$(hosts | xargs)
    fi

    local src genesis interval
    src=$(hosts | head -1)
    genesis=$(remote_config_int "$src" genesis_deadline)
    interval=$(remote_config_int "$src" slot_interval)
    echo "live genesis $genesis ($(iso_of_ms "$genesis")) slot_interval=${interval}ms, from $src"

    echo "checking every host is finalizing"
    fanout health_host $(($(date +%s) - 10)) "$genesis" "$interval" "$need" "$max_lag"

    # rendered for every host so the validator set stays identical; pushed only per upgraded host
    [ "$render" = no ] || "$deploy_dir/gen-config.sh" --genesis "$genesis" --keep-genesis --force "${gen_args[@]}"
    local host
    for host in $upgrade_hosts; do
        [ -f "$(host_config "$host")" ] || die "no $(host_config "$host"); render on the live genesis with: live-upgrade -- --force"
        [ "$(local_config_int "$host" genesis_deadline)" = "$genesis" ] \
            || die "$(host_config "$host") genesis_deadline is not the live $genesis"
    done

    [ "$build" = no ] || "$deploy_dir/build.sh" "${build_args[@]}"
    local binary rpc_binary explorer_binary tx_binary
    read -r binary rpc_binary explorer_binary tx_binary <<< "$(dist_binaries)"
    # shellcheck disable=SC2086
    "$deploy_dir/deploy.sh" --stage $upgrade_hosts

    local done_hosts= since rc out deadline
    for host in $upgrade_hosts; do
        echo
        echo "=== $host: checking the network before taking it down"
        fanout health_host $(($(date +%s) - 10)) "$genesis" "$interval" "$need" "$max_lag"

        echo "=== $host: stop, swap to $binary + $rpc_binary, start"
        stop_host "$host"
        rssh "$host" "set -e
            $(swap_cmd "$binary")
            $(swap_cmd "$rpc_binary" rpc-current)
            $(swap_cmd "$tx_binary" mcp-tx)
            $(has_explorer "$host" && swap_cmd "$explorer_binary" explorer-current)"
        "$deploy_dir/push-config.sh" "$host"
        since=$(($(date +%s) - 1))
        start_host "$host"

        echo "=== $host: waiting up to ${timeout}s for >= $need finalized and lag <= $max_lag slots"
        deadline=$(($(date +%s) + timeout))
        while :; do
            rc=0
            out=$(health_host "$host" "$since" "$genesis" "$interval" "$need" "$max_lag") || rc=$?
            echo "    $out"
            [ "$rc" = 0 ] && break
            if [ "$rc" = 2 ] || [ "$(date +%s)" -ge "$deadline" ]; then
                echo "rollout stopped at $host; upgraded: ${done_hosts:-none}" >&2
                die "$host is not healthy on $binary; see netctl.sh logs $host"
            fi
            sleep 3
        done
        if ! rpc_start_host "$host"; then
            echo "rollout stopped at $host (node healthy, rpc not); upgraded: ${done_hosts:-none}" >&2
            die "rpc on $host is not active; see journalctl --user -u $rpc_unit on $host"
        fi
        if ! explorer_start_host "$host"; then
            echo "rollout stopped at $host (node and rpc healthy, explorer not); upgraded: ${done_hosts:-none}" >&2
            die "explorer on $host is not active; see journalctl --user -u $explorer_unit on $host"
        fi
        done_hosts+="${done_hosts:+ }$host"
    done
    echo
    echo "live upgrade done: $done_hosts -> $binary + $rpc_binary"
}

cmd_run_one() {
    local host=${1:?$usage} action=${2:?$usage}
    node_id_of "$host" > /dev/null
    case $action in
        start)
            port_free_host "$host"
            start_host "$host"
            rpc_start_host "$host"
            explorer_start_host "$host"
            ;;
        stop) stop_host "$host" ;;
        restart)
            stop_host "$host"
            port_free_host "$host"
            start_host "$host"
            rpc_start_host "$host"
            explorer_start_host "$host"
            ;;
        *) die "$usage" ;;
    esac
}

cmd_takeover() {
    local start=yes
    if [ "${1:-}" = --no-start ]; then
        start=no
        shift
    fi
    [ "$supervisor" = systemd ] || die "takeover drives the mcp user units; PQ_SUPERVISOR=$supervisor is not supported"

    echo "checking monad-pq is deployed on $(host_count) hosts"
    fanout deployed_host

    echo "stopping monad-mcp-{explorer,rpc,node} on $(host_count) hosts; their config, genesis and ledger stay as they are"
    local dir host active rc=0
    dir=$(mktemp -d)
    fanout_to "$dir" mcp_stop_host
    # a re-run finds mcp already stopped; it keeps the first run's record
    mkdir -p "$takeover_dir"
    for host in $(hosts); do
        active=$(sed -n 's/^mcp_active=//p' "$dir/$host.out")
        [ -z "$active" ] || echo "$active" > "$takeover_dir/$host.units"
    done
    fanout_print "$dir" || rc=$?
    rm -rf "$dir"
    [ "$rc" = 0 ] || die "mcp did not stop everywhere; netctl.sh handback restarts what was stopped"
    echo "mcp units to restore recorded in $takeover_dir/<host>.units"

    [ "$start" = no ] || cmd_start "$@"
}

cmd_handback() {
    [ $# = 0 ] || die "$usage"
    [ "$supervisor" = systemd ] || die "handback drives the mcp user units; PQ_SUPERVISOR=$supervisor is not supported"

    echo "stopping monad-pq-{explorer,rpc,node} on $(host_count) hosts"
    fanout stop_host

    echo "starting the mcp units on their existing config, genesis and ledger"
    local dir host rc=0
    dir=$(mktemp -d)
    fanout_to "$dir" mcp_start_host
    for host in $(hosts); do
        [ "$(cat "$dir/$host.rc")" != 0 ] || rm -f "$takeover_dir/$host.units"
    done
    fanout_print "$dir" || rc=$?
    rm -rf "$dir"
    [ "$rc" = 0 ] || die "mcp did not come back everywhere; records kept in $takeover_dir, re-run handback"
    echo "mcp handed back; check it with monad-mcp-node/deploy/report.sh"
}

cmd=${1:?$usage}
shift
case $cmd in
    start) cmd_start "$@" ;;
    stop) cmd_stop "$@" ;;
    restart)
        cmd_stop
        cmd_start "$@"
        ;;
    status) cmd_status "$@" ;;
    logs) cmd_logs "$@" ;;
    upgrade) cmd_upgrade "$@" ;;
    live-upgrade) cmd_live_upgrade "$@" ;;
    run-one) cmd_run_one "$@" ;;
    takeover) cmd_takeover "$@" ;;
    handback) cmd_handback "$@" ;;
    *) die "$usage" ;;
esac
