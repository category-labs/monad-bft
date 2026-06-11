#!/usr/bin/env bash
# Build the signer binaries for linux/amd64 in Docker (no host toolchain needed).
# Run from the repo root. Outputs to ./target-linux/release/.
#
#   monad-signer        — the enclave-side service (vsock/unix listener)
#   monad-keyloader     — one-time provisioning client
#   monad-signer-bench  — hot-path throughput/latency benchmark
#
# bookworm's glibc (2.36) is older than the swe-006 host's, so the binaries run
# there. A separate CARGO_TARGET_DIR keeps these amd64 artifacts away from the
# host's native target/.
set -euo pipefail

docker run --rm --platform linux/amd64 \
  -v "$PWD":/work \
  -v monad-cargo-registry:/cargo/registry \
  -e CARGO_HOME=/cargo \
  -e CARGO_TARGET_DIR=/work/target-linux \
  -w /work \
  rust:1-bookworm \
  cargo build --release -p monad-remote-signer --bins

echo
echo "built:"
ls -la target-linux/release/monad-signer target-linux/release/monad-keyloader \
       target-linux/release/monad-signer-bench
