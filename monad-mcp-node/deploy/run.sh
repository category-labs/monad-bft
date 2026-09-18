#!/usr/bin/env bash
# run.sh: the MCP_SUPERVISOR=setsid launcher, installed as
# ~/monad-mcp/run.sh. Runs on the validator; sources nothing from here.
set -euo pipefail

root=${MCP_ROOT:-$HOME/monad-mcp}
mkdir -p "$root/run" "$root/logs"

if [ -s "$root/run/pid" ] && kill -0 "$(cat "$root/run/pid")" 2> /dev/null; then
    echo "already running as pid $(cat "$root/run/pid")" >&2
    exit 1
fi

# the pid is written by the process that becomes the node, so exec
# keeps it valid
RUST_LOG=${RUST_LOG:-info,monad_mcp_chorus::slot=debug} setsid --fork bash -c \
    'echo $$ > "$1/run/pid"; exec "$1/bin/current" "$1/config/node.toml"' \
    run.sh "$root" >> "$root/logs/node.log" 2>&1

sleep 1
echo "started pid $(cat "$root/run/pid")"
