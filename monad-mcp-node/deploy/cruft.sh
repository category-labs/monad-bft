#!/usr/bin/env bash
# ledger pruner; runs on the validator from monad-mcp-cruft.timer, so it
# sources nothing here and takes its knobs from ~/monad-mcp/cruft.env.
set -euo pipefail

root=${MCP_ROOT:-$HOME/monad-mcp}
blocks=$root/ledger/blocks
retention=${RETENTION_BLOCKS:-1000000}
min_free_gb=${MIN_FREE_GB:-200}
batch=10000

free_gb() {
    df -B1G --output=avail "$root" | tail -1 | tr -d ' '
}

# zero-padded block numbers, so lexical order is numeric order
names() {
    ls -1 "$blocks" 2> /dev/null | grep -E '^[0-9]{12}' || true
}

delete() {
    sed "s|^|$blocks/|" | xargs -r rm -f --
}

if [ ! -d "$blocks" ]; then
    echo "no ledger at $blocks, nothing to prune"
    exit 0
fi

# a stopped node keeps its ledger for post-mortem
if [ -z "$(find "$blocks" -type f -mmin -20 -print -quit)" ]; then
    echo "no ledger writes in the last 20 min, skipping (free $(free_gb)GB)"
    exit 0
fi

tip_name=$(names | tail -1)
if [ -z "$tip_name" ]; then
    echo "no block files under $blocks, nothing to prune"
    exit 0
fi

tip=$((10#${tip_name%%.*}))
cutoff=$((tip - retention))
deleted=0

if [ "$cutoff" -gt 0 ]; then
    padded=$(printf '%012d' "$cutoff")
    old=$(names | awk -v c="$padded" 'substr($0, 1, length(c)) < c')
    if [ -n "$old" ]; then
        deleted=$(wc -l <<< "$old")
        printf '%s\n' "$old" | delete
    fi
fi

while [ "$(free_gb)" -lt "$min_free_gb" ]; do
    oldest=$(names | head -n "$batch")
    [ -n "$oldest" ] || break
    printf '%s\n' "$oldest" | delete
    deleted=$((deleted + $(wc -l <<< "$oldest")))
done

echo "tip=$tip cutoff=$cutoff retention=$retention deleted=$deleted free_gb=$(free_gb)"
