#!/usr/bin/env bash
# Ingest mainnet-deu-009: AWS S3 read -> RustFS S3 blobs -> remote Scylla Alternator meta.
# Usage: ./remote-ingest.sh [count]   (default 100000; resumes from the published head)
set -euo pipefail
cd "$(dirname "$0")/.."   # -> monad-bft repo root

# Always build the release binary first (incremental; fast when up to date).
cargo build --release -p monad-chain-data-ingest --bin chain-data-ingest \
  --features archive-ingest,s3,dynamo

# triedb/execution C++ libs (the binary has no rpath baked in)
export LD_LIBRARY_PATH="$(find target/release/build -path '*/out/*' -name '*.so' -printf '%h\n' | sort -u | paste -sd:)"
# RustFS blob credentials -> AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY (ambient,
# used by the blob S3 client). The archive read uses --profile default and the
# meta store uses explicit --dynamo creds, so both override these.
set -a; source /home/jhow/rustfs-credentials.env; set +a
export AWS_EC2_METADATA_DISABLED=true

# Tee all output (stdout+stderr) to a timestamped log. `pipefail` (set above)
# keeps the binary's exit status as the script's, not tee's.
LOG_FILE="${LOG_FILE:-remote-ingest-$(date +%Y%m%d-%H%M%S).log}"
echo "logging to $(pwd)/$LOG_FILE"

target/release/chain-data-ingest \
  --single-writer \
  --meta-backend dynamo \
  --dynamo-table mainnet_deu_009 \
  --dynamo-endpoint-url http://100.81.221.102:8000 \
  --dynamo-region us-east-1 \
  --dynamo-access-key-id scylla --dynamo-secret-access-key scylla \
  --dynamo-create-table \
  --blob-backend s3 \
  --s3-bucket chaindata \
  --s3-prefix mainnet-deu-009 \
  --s3-endpoint-url http://100.124.21.75:9000 \
  --s3-region us-east-1 \
  --s3-force-path-style \
  --s3-max-concurrency 1024 \
  --block-data-source "aws mainnet-deu-009-0 --profile default --region us-east-2" \
  --count "10000000" \
  --concurrency 2000 --batch-size 2000 \
  --io-max-retries 20 --io-retry-backoff-ms 250 --io-retry-max-backoff-ms 10000 \
  --log-every 500 2>&1 | tee -a "$LOG_FILE"
