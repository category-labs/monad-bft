#!/usr/bin/env bash
# every whole-network start mints a new genesis and wipes the ledger;
# live-upgrade restarts one host at a time on the running genesis and ledger.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

usage="usage: netctl.sh start [--lead 60] [--keep-ledger] [-- gen-config.sh flags] | stop | restart [start flags] | status | logs <host> [-f] | upgrade [start flags] | live-upgrade [--hosts a,b] [--timeout 120] [--max-lag 20] [--no-build] [--dirty] [-- gen-config.sh flags] | run-one <host> start|stop|restart"

port_free_host() {
    rssh "$1" "if ss -Hlun | tr -s ' ' '\n' | grep -q ':$port\$'; then
            echo '$port/udp already bound'
            exit 1
        fi
        echo '$port/udp free'"
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
        rssh "$host" "set -e
            mkdir -p $remote_root/ledger/blocks
            find $remote_root/ledger/blocks -mindepth 1 -delete
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

# the bracket keeps pgrep from matching the ssh command line itself
stop_host() {
    local host=$1
    if [ "$supervisor" = systemd ]; then
        # separate calls: an rpc unit not yet deployed must not keep the node running
        rssh_user_systemd "$host" "systemctl --user stop $rpc_unit.service 2> /dev/null
            systemctl --user stop $unit.service" || true
    else
        rssh "$host" "if [ -s $remote_root/run/pid ]; then
                kill -INT \$(cat $remote_root/run/pid) 2> /dev/null || true
            fi"
    fi
    rssh "$host" "for i in 1 2 3 4 5; do
            pgrep -f 'monad-mcp/bin/curren[t]' > /dev/null || break
            sleep 1
        done
        if pgrep -f 'monad-mcp/bin/curren[t]' > /dev/null; then
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
    local host=$1
    rssh_user_systemd "$host" "
        state=\$(systemctl --user is-active $unit.service 2> /dev/null || true)
        rpc=\$(systemctl --user is-active $rpc_unit.service 2> /dev/null || true)
        since=\$(systemctl --user show $unit.service -p ActiveEnterTimestamp --value 2> /dev/null || true)
        bound=\$(ss -Hlun | tr -s ' ' '\n' | grep -c ':$port\$' || true)
        last=\$($(node_log_cmd) 2> /dev/null | grep finalized | tail -1)
        echo \"state=\${state:-unknown} rpc=\${rpc:-unknown} port_bound=\$bound since=\${since:-none}\"
        echo \"last=\${last:-no finalized line}\""
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

    echo "checking $port/udp is free on $(host_count) hosts"
    fanout port_free_host

    local genesis start_ms since ts
    start_ms=$(now_ms)
    genesis=$((start_ms + lead * 1000))
    since=$((start_ms / 1000 - 5))
    ts=$((start_ms / 1000))

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

    echo "resetting the ledger (keep-ledger=$keep_ledger)"
    fanout ledger_reset_host "$keep_ledger" "$ts"

    echo "starting the network"
    fanout start_host

    echo "waiting for the udp bind on every host"
    fanout wait_bound_host "$since"

    echo "starting the rpc on every host"
    fanout rpc_start_host

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
    local binary rpc_binary
    read -r binary rpc_binary <<< "$(dist_binaries)"
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
            $(swap_cmd "$rpc_binary" rpc-current)"
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
            start_host "$host"
            rpc_start_host "$host"
            ;;
        stop) stop_host "$host" ;;
        restart)
            stop_host "$host"
            start_host "$host"
            rpc_start_host "$host"
            ;;
        *) die "$usage" ;;
    esac
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
    *) die "$usage" ;;
esac
