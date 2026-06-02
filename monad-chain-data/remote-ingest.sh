#!/usr/bin/env bash
# Ingest mainnet-deu-009: AWS S3 read -> OVH S3 blobs -> remote Scylla Alternator meta.
# Usage: ./remote-ingest.sh [count]   (default 100000; resumes from the published head)
set -euo pipefail
cd "$(dirname "$0")/.."   # -> monad-bft repo root

# Always build the release binary first (incremental; fast when up to date).
cargo build --release -p monad-chain-data --bin chain-data-ingest \
  --features archive-ingest,s3,dynamo

# triedb/execution C++ libs (the binary has no rpath baked in)
export LD_LIBRARY_PATH="$(find target/release/build -path '*/out/*' -name '*.so' -printf '%h\n' | sort -u | paste -sd:)"
export AWS_PROFILE=telling-narlikar          # OVH blob credentials
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
  --s3-bucket telling-narlikar \
  --s3-prefix mainnet-deu-009 \
  --s3-endpoint-url https://s3.us-east-va.io.cloud.ovh.us \
  --s3-region us-east-1 \
  --s3-force-path-style \
  --s3-max-concurrency 10000 \
  --block-data-source "aws mainnet-deu-009-0 --profile default --region us-east-2" \
  --count "10000000" \
  --concurrency 1000 --batch-size 2000 --autotune --max-concurrency 20000 \
  --io-max-retries 20 --io-retry-backoff-ms 250 --io-retry-max-backoff-ms 10000 \
  --log-every 500 2>&1 | tee -a "$LOG_FILE"
