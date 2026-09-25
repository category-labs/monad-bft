#!/usr/bin/env bash
# cruft.sh against a temp root: block directories go, anything else stays.
set -euo pipefail

cruft=$(cd "$(dirname "$0")/../deploy" && pwd)/cruft.sh
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

# a ledger with blocks 1..10, a writer temp dir and foreign entries
make_root() {
    local root=$work/$1 blocks=$work/$1/ledger/blocks
    mkdir -p "$blocks"
    for slot in $(seq 1 10); do
        local dir
        dir=$blocks/$(printf '%012d' "$slot")
        mkdir "$dir"
        printf 'meta' > "$dir/meta.rlp"
        printf 'lane' > "$dir/lane-0.rlp"
    done
    mkdir "$blocks/.000000000011.tmp"
    printf 'partial' > "$blocks/.000000000011.tmp/lane-0.rlp"
    printf 'keep' > "$blocks/notes"
    mkdir "$blocks/000000000003-keep"
    printf 'keep' > "$blocks/000000000003-keep/meta.rlp"
    printf 'keep' > "$blocks/000000000004.json"
    mkdir "$blocks/archive"
    echo "$root"
}

blocks_left() {
    ls -1A "$1/ledger/blocks" | tr '\n' ' '
}

expect_left() {
    local root=$1 expected=$2 left
    left=$(blocks_left "$root")
    [ "$left" = "$expected" ] || fail "left '$left', expected '$expected'"
}

run() {
    local root=$1
    shift
    env MCP_ROOT="$root" "$@" bash "$cruft"
}

# retention keeps the tip and the blocks within RETENTION_BLOCKS of it
root=$(make_root retention)
out=$(run "$root" RETENTION_BLOCKS=3 MIN_FREE_GB=0)
echo "$out" | grep -q 'tip=10 cutoff=7 retention=3 deleted=6' || fail "retention: $out"
expect_left "$root" '.000000000011.tmp 000000000003-keep 000000000004.json 000000000007 000000000008 000000000009 000000000010 archive notes '
[ -f "$root/ledger/blocks/.000000000011.tmp/lane-0.rlp" ] || fail "temp dir contents touched"
[ -f "$root/ledger/blocks/000000000003-keep/meta.rlp" ] || fail "foreign dir contents touched"

# a second run has nothing left to prune
out=$(run "$root" RETENTION_BLOCKS=3 MIN_FREE_GB=0)
echo "$out" | grep -q 'deleted=0' || fail "rerun: $out"

# a disk below the free-space floor loses every block, oldest first
root=$(make_root pressure)
out=$(run "$root" RETENTION_BLOCKS=1000000 MIN_FREE_GB=999999999)
echo "$out" | grep -q 'deleted=10' || fail "pressure: $out"
expect_left "$root" '.000000000011.tmp 000000000003-keep 000000000004.json archive notes '

# a ledger nobody wrote to for 20 minutes is kept for post-mortem
root=$(make_root stopped)
find "$root/ledger/blocks" -exec touch -d '-1 hour' {} +
out=$(run "$root" RETENTION_BLOCKS=3 MIN_FREE_GB=0)
echo "$out" | grep -q 'skipping' || fail "stopped: $out"
[ "$(ls -1 "$root/ledger/blocks" | grep -c '^[0-9]\{12\}$')" = 10 ] || fail "a stopped ledger was pruned"

# no ledger at all
out=$(run "$work/missing" RETENTION_BLOCKS=3 MIN_FREE_GB=0)
echo "$out" | grep -q 'nothing to prune' || fail "missing: $out"

echo "cruft ok"
