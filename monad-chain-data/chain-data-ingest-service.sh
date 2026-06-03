#!/usr/bin/env bash
# Service entrypoint for the full-chain backfill (systemd: chain-data-ingest.service).
# Runs the PREBUILT release binary as the sole writer:
#   read AWS S3 (mainnet-deu-009-0) -> blobs RustFS (chaindata) -> meta Scylla Alternator.
# Resumes from the published head on every (re)start, so systemd Restart=always
# just continues the backfill. Build the binary out-of-band (cargo build --release).
set -euo pipefail
cd /home/jhow/monad-bft

# triedb/execution C++ libs (binary has no rpath baked in)
export LD_LIBRARY_PATH="$(find target/release/build -path '*/out/*' -name '*.so' -printf '%h\n' | sort -u | paste -sd:)"
# RustFS blob credentials (ambient AWS_ACCESS_KEY_ID/SECRET for the blob client)
set -a; source /home/jhow/rustfs-credentials.env; set +a
export AWS_EC2_METADATA_DISABLED=true

exec target/release/chain-data-ingest \
  --single-writer \
  --meta-backend dynamo \
  --dynamo-table mainnet_deu_009 \
  --dynamo-endpoint-url http://100.81.221.102:8000 \
  --dynamo-region us-east-1 \
  --dynamo-access-key-id scylla --dynamo-secret-access-key scylla \
  --dynamo-max-concurrency 64 \
  --blob-backend s3 \
  --s3-bucket chaindata \
  --s3-prefix mainnet-deu-009 \
  --s3-endpoint-url http://100.124.21.75:9000 \
  --s3-region us-east-1 \
  --s3-force-path-style \
  --s3-max-concurrency 1024 \
  --block-data-source "aws mainnet-deu-009-0 --profile default --region us-east-2" \
  --count 200000000 \
  --concurrency 2000 --batch-size 2000 \
  --io-max-retries 50 --io-retry-backoff-ms 250 --io-retry-max-backoff-ms 10000 \
  --log-every 5000
