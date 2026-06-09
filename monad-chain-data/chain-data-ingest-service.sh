#!/usr/bin/env bash
# systemd entrypoint for continuous chain-data ingest:
# AWS archive -> RustFS blobs -> Scylla Alternator metadata.
set -euo pipefail

cd /home/jhow/monad-bft/monad-chain-data
export AWS_EC2_METADATA_DISABLED=true

exec /home/jhow/.cargo/bin/cargo +nightly-2025-12-09 run --release \
  -p monad-chain-data-ingest \
  --bin chain-data-ingest \
  --features archive-ingest,s3,dynamo \
  -- \
  --block-data-source "aws mainnet-deu-009-0 --profile default --region us-east-2" \
  --meta-backend dynamo \
  --dynamo-table chain_data_bench_coalesce_s3shard4_5m_start1_20260603_214634 \
  --dynamo-endpoint-url http://100.81.221.102:8000 \
  --dynamo-region us-east-1 \
  --dynamo-access-key-id scylla \
  --dynamo-secret-access-key scylla \
  --scylla-profile \
  --scylla-concurrency 256 \
  --blob-backend s3 \
  --s3-bucket chain-data-bench-coalesce-s3shard4-5m-start1-20260603-214634 \
  --s3-endpoint-url http://100.124.21.75:9000 \
  --s3-endpoint-url http://100.81.221.102:9000 \
  --s3-endpoint-url http://100.104.77.72:9000 \
  --s3-endpoint-url http://100.118.54.55:9000 \
  --s3-region us-east-1 \
  --s3-access-key-id rustfsadmin \
  --s3-secret-access-key txfJrrzCAVbzfhbZWU0Hs9bhaRsYOMlMsGjI3sfJ \
  --s3-force-path-style \
  --s3-max-concurrency 64 \
  --max-batch-size 1000 \
  --concurrency 5000 \
  --fetch-buffer 10000 \
  --write-workers 1 \
  --io-retry-forever \
  --io-retry-backoff-ms 250 \
  --io-retry-max-backoff-ms 10000
