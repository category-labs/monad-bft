#!/usr/bin/env bash
# every whole-network start mints a new genesis and wipes the ledger.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

usage="usage: netctl.sh start [--lead 60] [--keep-ledger] [-- gen-config.sh flags] | stop | restart [start flags] | status | logs <host> [-f] | upgrade [start flags] | run-one <host> start|stop"

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

# the bracket keeps pgrep from matching the ssh command line itself
stop_host() {
    local host=$1
    if [ "$supervisor" = systemd ]; then
        rssh_user_systemd "$host" "systemctl --user stop $unit.service" || true
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
        since=\$(systemctl --user show $unit.service -p ActiveEnterTimestamp --value 2> /dev/null || true)
        bound=\$(ss -Hlun | tr -s ' ' '\n' | grep -c ':$port\$' || true)
        last=\$($(node_log_cmd) 2> /dev/null | grep finalized | tail -1)
        echo \"state=\${state:-unknown} port_bound=\$bound since=\${since:-none}\"
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

    "$deploy_dir/gen-config.sh" --genesis "$genesis" "${gen_args[@]}"
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

cmd_run_one() {
    local host=${1:?$usage} action=${2:?$usage}
    node_id_of "$host" > /dev/null
    case $action in
        start) start_host "$host" ;;
        stop) stop_host "$host" ;;
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
    run-one) cmd_run_one "$@" ;;
    *) die "$usage" ;;
esac
