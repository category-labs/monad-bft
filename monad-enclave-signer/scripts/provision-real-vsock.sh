#!/usr/bin/env bash
# Demo the full Option-2 provisioning flow over real AF_VSOCK using the actual
# node keystores: start an UNPROVISIONED signer, decrypt the node's keystores
# INSIDE it via the keyloader, then sign with the provisioned (real) key.
#
# The devnet keystores use an empty passphrase, so no secret is needed. Run on
# swe-006 with the binaries + ~/ks-work/{id-secp,id-bls} present. If a non-empty
# password is ever needed, set KEYSTORE_PASSWORD in the environment first.
PORT="${PORT:-5005}"

sudo -n modprobe vsock_loopback 2>/dev/null || true
pkill -f monad-signer 2>/dev/null || true
sleep 0.5

echo "== starting UNPROVISIONED signer on vsock port $PORT =="
./monad-signer --vsock-port "$PORT" > ~/sig.out 2>&1 &
SPID=$!
sleep 1.5
echo "-- signer log --"; cat ~/sig.out

echo "== provisioning REAL node keys (decrypt happens inside the signer) =="
./monad-keyloader --secp ~/ks-work/id-secp --bls ~/ks-work/id-bls \
  --vsock-cid 1 --vsock-port "$PORT"
echo "keyloader exit=$?"

echo "== signing with the provisioned (real) key over vsock =="
./monad-signer-bench --vsock-cid 1 --vsock-port "$PORT" --threads 1 --duration-secs 2

kill "$SPID" 2>/dev/null || true
