#!/usr/bin/env bash
# Demo + benchmark the remote signer over real AF_VSOCK loopback on a single
# Linux host (no confidential VM required). Run on swe-006 after copying the
# binaries built by build-linux.sh into the current directory.
#
# AF_VSOCK loopback uses VMADDR_CID_LOCAL = 1 and needs the vsock_loopback
# module. On a real SEV-SNP deployment the node runs on the host and the signer
# inside the CVM; only the CID changes (the guest's CID instead of 1).
set -euo pipefail

PORT="${PORT:-5005}"
THREADS="${THREADS:-8}"
DURATION="${DURATION:-5}"

echo "== loading vsock loopback module =="
sudo modprobe vsock_loopback || sudo modprobe vsock || true
lsmod | grep -E 'vsock' || { echo "vsock module not loaded; aborting"; exit 1; }

echo "== starting signer (--generate) on vsock port $PORT =="
./monad-signer --vsock-port "$PORT" --generate &
SIGNER_PID=$!
trap 'kill $SIGNER_PID 2>/dev/null || true' EXIT
sleep 1

echo "== benchmarking over vsock loopback (CID 1) =="
./monad-signer-bench --vsock-cid 1 --vsock-port "$PORT" \
  --threads "$THREADS" --duration-secs "$DURATION"
