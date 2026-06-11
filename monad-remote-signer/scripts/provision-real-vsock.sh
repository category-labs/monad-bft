#!/usr/bin/env bash
# Demo the full Option-2 provisioning flow over real AF_VSOCK using the actual
# node keystores: start an UNPROVISIONED signer, decrypt the node's keystores
# INSIDE it via the keyloader, then sign with the provisioned (real) key.
#
# The keystore password is loaded into the environment (KEYSTORE_PASSWORD), not
# passed as an argument, so it never appears in `ps`. Run on swe-006 with the
# binaries + ~/ks-work/{id-secp,id-bls} present.
set -euo pipefail

PORT="${PORT:-5005}"
sudo -n modprobe vsock_loopback 2>/dev/null || true

# Load only KEYSTORE_PASSWORD from the node's env file into our environment.
export KEYSTORE_PASSWORD="$(sudo -n grep -E '^KEYSTORE_PASSWORD=' /home/monad/.env \
  | head -1 | cut -d= -f2- | sed -E 's/^["'\'']?//; s/["'\'']?$//')"

echo "== starting UNPROVISIONED signer on vsock port $PORT =="
./monad-signer --vsock-port "$PORT" &
SPID=$!
trap 'kill $SPID 2>/dev/null || true' EXIT
sleep 1.5

echo "== provisioning REAL node keys (decrypt happens inside the signer) =="
./monad-keyloader --secp ~/ks-work/id-secp --bls ~/ks-work/id-bls \
  --vsock-cid 1 --vsock-port "$PORT"

echo "== signing with the provisioned (real) key over vsock =="
./monad-signer-bench --vsock-cid 1 --vsock-port "$PORT" --threads 1 --duration-secs 2

unset KEYSTORE_PASSWORD
