#!/usr/bin/env bash
# read-only fitness check plus a pairwise UDP probe; linger, layout, cruft
# timer and the legacy units only warn, since other scripts are what fix them.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

min_free_gb=${MIN_FREE_GB:-200}
max_clock_ms=50
probe_window=20   # seconds a listener waits for every sender's burst
probe_bind_wait=15   # seconds to wait for a listener's "bound" line
local_glibc=$(ldd --version | head -1 | awk '{print $NF}')

probe_host() {
    local host=$1
    rssh "$host" "MCP_PORT=$port bash -s" <<'PROBE'
port=${MCP_PORT:-8002}
root=$HOME/monad-mcp
export XDG_RUNTIME_DIR=/run/user/2000 DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/2000/bus
echo "glibc=$(ldd --version | head -1 | awk '{print $NF}')"
if grep -qw avx2 /proc/cpuinfo; then echo "avx2=yes"; else echo "avx2=no"; fi
if ss -Hlun | tr -s ' ' '\n' | grep -q ":${port}$"; then echo "port=bound"; else echo "port=free"; fi
echo "bft=$(systemctl show monad-bft -p ActiveState --value 2>/dev/null || echo unknown)"
echo "exec=$(systemctl show monad-execution -p ActiveState --value 2>/dev/null || echo unknown)"
echo "ntp=$(timedatectl show -p NTPSynchronized --value 2>/dev/null || echo unknown)"
echo "clock_ms=$(chronyc sources -v 2>/dev/null | awk '
    # only usable modes: an unreached ? or falseticker x source reports a 0 offset
    /^[\^=#][*+-]/ {
        if (match($0, /\[[ ]*[-+]?[0-9.]+(ns|us|ms|s)\]/)) {
            v = substr($0, RSTART + 1, RLENGTH - 2)
            gsub(/[ +]/, "", v)
            sub(/^-/, "", v)
            unit = v; gsub(/[0-9.]/, "", unit)
            num = v; gsub(/[^0-9.]/, "", num)
            if (unit == "ns") ms = num / 1000000
            else if (unit == "us") ms = num / 1000
            else if (unit == "ms") ms = num
            else ms = num * 1000
            if (best == "" || ms < best) best = ms
        }
    }
    END { printf "%s", (best == "" ? "none" : sprintf("%.1f", best)) }')"
echo "linger=$(loginctl show-user "$(id -un)" -p Linger --value 2>/dev/null || echo unknown)"
echo "usermgr=$(systemctl --user is-system-running 2>/dev/null || echo unknown)"
echo "free_gb=$(df -B1G --output=avail "$HOME" | tail -1 | tr -d ' ')"
layout=ok
for d in bin config run logs ledger/blocks; do
    [ -d "$root/$d" ] || layout=missing
done
echo "layout=$layout"
echo "cruft=$(systemctl --user is-active monad-mcp-cruft.timer 2>/dev/null || echo inactive)"
PROBE
}

field() {
    sed -n "s/^$2=//p" "$1" | tail -1
}

lt() {
    awk -v a="$1" -v b="$2" 'BEGIN { exit !(a + 0 < b + 0) }'
}

fails=0
warns=0
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

echo "probing $(host_count) hosts (read-only), local glibc $local_glibc"
fanout_to "$tmp" probe_host

printf '\n%-8s %-6s %-5s %-6s %-9s %-9s %-4s %-9s %-7s %-9s %-8s %-8s %-9s %s\n' \
    host glibc avx2 "$port" monad-bft monad-exec ntp clock_ms linger usermgr free_gb layout cruft verdict

for host in $(hosts); do
    out=$tmp/$host.out
    rc=$(cat "$tmp/$host.rc" 2> /dev/null || echo 1)
    if [ "$rc" != 0 ]; then
        printf '%-8s %s\n' "$host" "UNREACHABLE: $(tr '\n' ' ' < "$out" | cut -c1-90)"
        fails=$((fails + 1))
        continue
    fi

    glibc=$(field "$out" glibc)
    avx2=$(field "$out" avx2)
    portstate=$(field "$out" port)
    bft=$(field "$out" bft)
    exec_state=$(field "$out" exec)
    ntp=$(field "$out" ntp)
    clock=$(field "$out" clock_ms)
    linger=$(field "$out" linger)
    usermgr=$(field "$out" usermgr)
    free_gb=$(field "$out" free_gb)
    layout=$(field "$out" layout)
    cruft=$(field "$out" cruft)

    verdict=ok
    [ "$glibc" = "$local_glibc" ] || verdict=FAIL
    [ "$avx2" = yes ] || verdict=FAIL
    [ "$portstate" = free ] || verdict=FAIL
    [ "$ntp" = yes ] || verdict=FAIL
    { [ "$clock" != none ] && lt "$clock" "$max_clock_ms"; } || verdict=FAIL
    case $usermgr in
        running | degraded | starting) ;;
        *) verdict=FAIL ;;
    esac
    lt "$free_gb" "$min_free_gb" && verdict=FAIL
    if [ "$verdict" = ok ]; then
        [ "$linger" = yes ] || verdict=WARN
        [ "$layout" = ok ] || verdict=WARN
        [ "$cruft" = active ] || verdict=WARN
        case "$bft$exec_state" in
            inactiveinactive) ;;
            *) verdict=WARN ;;
        esac
    fi
    [ "$verdict" != FAIL ] || fails=$((fails + 1))
    [ "$verdict" != WARN ] || warns=$((warns + 1))

    printf '%-8s %-6s %-5s %-6s %-9s %-9s %-4s %-9s %-7s %-9s %-8s %-8s %-9s %s\n' \
        "$host" "$glibc" "$avx2" "$portstate" "$bft" "$exec_state" "$ntp" "$clock" \
        "$linger" "$usermgr" "$free_gb" "$layout" "$cruft" "$verdict"
done

# prints "bound" once listening, then "seen: <tags>" after every expected
# sender was heard or the window closed. stdout is line-buffered by -u.
listen_on() {
    rssh "$1" "python3 -u - $port $2 $probe_window" <<'PY'
import socket, sys, time

port, expected, secs = int(sys.argv[1]), int(sys.argv[2]), float(sys.argv[3])
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
try:
    sock.bind(("0.0.0.0", port))
except OSError as error:
    print("bind-failed:", error)
    sys.exit(1)
print("bound")
sock.settimeout(0.5)
seen = set()
end = time.time() + secs
while time.time() < end and len(seen) < expected:
    try:
        data, _ = sock.recvfrom(2048)
    except socket.timeout:
        continue
    tag = data.decode("ascii", "replace")[:16]
    if tag.replace("-", "").isalnum():
        seen.add(tag)
print("seen:", " ".join(sorted(seen)))
PY
}

# a spaced burst, so one lost or early datagram does not fail the pair
send_to() {
    rssh "$1" "python3 - $2 $port $1" <<'PY'
import socket, sys, time

ip, port, tag = sys.argv[1], int(sys.argv[2]), sys.argv[3]
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
for _ in range(5):
    sock.sendto(tag.encode(), (ip, port))
    time.sleep(0.2)
PY
}

# waits for the listener's "bound" line; its ssh handshake can take seconds
wait_bound() {
    local file=$1 i
    for i in $(seq 1 $((probe_bind_wait * 10))); do
        grep -qx bound "$file" 2> /dev/null && return 0
        sleep 0.1
    done
    return 1
}

udp_missing=0

udp_probe() {
    local listener ip host lpid pids expected
    expected=$(($(host_count) - 1))
    for listener in $(hosts); do
        ip=$(ipv4_of "$listener")
        (listen_on "$listener" "$expected") > "$tmp/seen-$listener" 2>&1 &
        lpid=$!
        if ! wait_bound "$tmp/seen-$listener"; then
            echo "    $listener: listener not bound after ${probe_bind_wait}s, skipping its senders" >&2
            wait "$lpid" 2> /dev/null || true
            continue
        fi
        pids=()
        for host in $(hosts); do
            [ "$host" != "$listener" ] || continue
            (send_to "$host" "$ip") > "$tmp/send-$listener-$host" 2>&1 &
            pids+=($!)
        done
        wait "${pids[@]}" 2> /dev/null || true
        wait "$lpid" 2> /dev/null || true
    done

    local row
    udp_missing=0
    printf '\n%-8s' 'listener'
    for host in $(hosts); do
        printf ' %2s' "$(node_id_of "$host")"
    done
    printf '   (columns are sender node ids, . delivered, X lost)\n'
    for listener in $(hosts); do
        row=$(sed -n 's/^seen: //p' "$tmp/seen-$listener" 2> /dev/null || true)
        printf '%-8s' "$listener"
        for host in $(hosts); do
            if [ "$host" = "$listener" ]; then
                printf ' %2s' -
            elif grep -qw -- "$host" <<< "$row"; then
                printf ' %2s' .
            else
                printf ' %2s' X
                udp_missing=$((udp_missing + 1))
            fi
        done
        printf '\n'
        grep -E '^bind-failed|^ssh:|Permission denied|timed out' "$tmp/seen-$listener" 2> /dev/null \
            | sed "s/^/    $listener: /" || true
    done
    if [ "$udp_missing" != 0 ]; then
        echo "error: $udp_missing of $(($(host_count) * ($(host_count) - 1))) ordered pairs lost datagrams" >&2
    fi
}

if grep -qx 'port=bound' "$tmp"/*.out; then
    echo
    echo "skipping the udp pair probe: $port/udp is still bound somewhere (monad-bft holds it; freeing it needs infra)"
else
    udp_probe
fi

echo
echo "$fails host(s) failed, $warns warned, $udp_missing udp pair(s) lost"
[ "$fails" = 0 ] && [ "$udp_missing" = 0 ] || exit 1
