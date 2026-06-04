#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
base="$repo_root/scripts/bench-chain-data-aws-direct.sh"

usage() {
  cat <<'USAGE'
Usage: bench-chain-data-aws-50m-50k-b10000-t4.sh --dynamo-prefix <fresh-prefix> [extra direct-bench args...]

Runs the 50M synthetic-start benchmark shape:
  start=50,000,000
  count=50,000
  batch-size=10,000
  dynamo global concurrency=256
  dynamo per-table concurrency=4
  fetch concurrency=5,000
  fetch buffer=100,000

Pass a fresh Dynamo prefix. Any extra args are forwarded after the fixed args,
so later repeated flags can override this wrapper when intentionally needed.
USAGE
}

if [[ $# -eq 0 ]]; then
  usage >&2
  exit 2
fi

case "${1:-}" in
  -h|--help)
    usage
    exit 0
    ;;
esac

exec "$base" \
  --start 50000000 \
  --count 50000 \
  --batch-size 10000 \
  --write-workers 1 \
  --dynamo-concurrency 256 \
  --dynamo-table-concurrency 4 \
  --fetch-buffer 100000 \
  --fetch-concurrency 5000 \
  --fetch-strategy spawned-semaphore-buffer \
  --log-every 1 \
  --s3-prefix "chain-data-synthstart-50000000-c50000-b10000-dyn256-t4-$(date +%Y%m%d-%H%M%S)" \
  --extra-arg --benchmark-synthetic-start \
  "$@"
