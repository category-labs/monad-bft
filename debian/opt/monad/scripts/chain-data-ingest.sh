#!/bin/bash

set -euo pipefail

require_env() {
  local name="$1"
  if [ -z "${!name:-}" ]; then
    echo "Missing required environment variable: ${name}" >&2
    exit 1
  fi
}

add_value_arg() {
  local name="$1"
  local flag="$2"
  if [ -n "${!name:-}" ]; then
    args+=("${flag}" "${!name}")
  fi
}

add_bool_arg() {
  local name="$1"
  local flag="$2"
  case "${!name:-}" in
    1|true|TRUE|yes|YES|on|ON)
      args+=("${flag}")
      ;;
  esac
}

add_repeated_arg() {
  local name="$1"
  local flag="$2"
  local values="${!name:-}"
  local value
  if [ -n "${values}" ]; then
    for value in ${values}; do
      args+=("${flag}" "${value}")
    done
  fi
}

require_env CHAIN_DATA_BLOCK_DATA_SOURCE

args=(
  --block-data-source "${CHAIN_DATA_BLOCK_DATA_SOURCE}"
)

add_value_arg OTEL_ENDPOINT --otel-endpoint
add_value_arg CHAIN_DATA_START --start
add_value_arg CHAIN_DATA_END --end
add_value_arg CHAIN_DATA_COUNT --count
add_value_arg CHAIN_DATA_LOG_EVERY --log-every
add_value_arg CHAIN_DATA_CONCURRENCY --concurrency
add_value_arg CHAIN_DATA_MAX_RETRIES --max-retries
add_value_arg CHAIN_DATA_RETRY_BACKOFF_MS --retry-backoff-ms
add_value_arg CHAIN_DATA_MIN_CONCURRENCY --min-concurrency
add_value_arg CHAIN_DATA_MAX_CONCURRENCY --max-concurrency
add_value_arg CHAIN_DATA_FETCH_BUFFER --fetch-buffer
add_value_arg CHAIN_DATA_FETCH_STRATEGY --fetch-strategy
add_value_arg CHAIN_DATA_OPEN_INDEX_SNAPSHOT --open-index-snapshot
add_value_arg CHAIN_DATA_MAX_BATCH_SIZE --max-batch-size
add_value_arg CHAIN_DATA_MIN_BATCH_SIZE --min-batch-size
add_value_arg CHAIN_DATA_PLAN_BUFFER --plan-buffer
add_value_arg CHAIN_DATA_WRITE_BUFFER --write-buffer
add_value_arg CHAIN_DATA_WRITE_WORKERS --write-workers
add_value_arg CHAIN_DATA_PROGRESS_BUFFER --progress-buffer
add_value_arg CHAIN_DATA_IO_MAX_RETRIES --io-max-retries
add_value_arg CHAIN_DATA_IO_RETRY_BACKOFF_MS --io-retry-backoff-ms
add_value_arg CHAIN_DATA_IO_RETRY_MAX_BACKOFF_MS --io-retry-max-backoff-ms
add_value_arg CHAIN_DATA_OWNER_ID --owner-id
add_value_arg CHAIN_DATA_LEASE_BLOCKS --lease-blocks
add_value_arg CHAIN_DATA_RENEW_THRESHOLD_BLOCKS --renew-threshold-blocks
add_value_arg CHAIN_DATA_DATA_DIR --data-dir
add_value_arg CHAIN_DATA_META_DATA_DIR --meta-data-dir
add_value_arg CHAIN_DATA_BLOB_DATA_DIR --blob-data-dir
add_value_arg CHAIN_DATA_FJALL_JOURNAL_MIB --fjall-journal-mib
add_value_arg CHAIN_DATA_FJALL_MEMTABLE_MIB --fjall-memtable-mib
add_value_arg CHAIN_DATA_FJALL_WORKERS --fjall-workers
add_value_arg CHAIN_DATA_CACHE_MIB --cache-mib
add_value_arg CHAIN_DATA_META_BACKEND --meta-backend
add_value_arg CHAIN_DATA_BLOB_BACKEND --blob-backend

add_bool_arg CHAIN_DATA_NO_AUTOTUNE --no-autotune
add_bool_arg CHAIN_DATA_BENCHMARK_SYNTHETIC_START --benchmark-synthetic-start
add_bool_arg CHAIN_DATA_IO_RETRY_FOREVER --io-retry-forever
add_bool_arg CHAIN_DATA_READER_ONLY --reader-only
add_bool_arg CHAIN_DATA_SINGLE_WRITER --single-writer

add_value_arg CHAIN_DATA_S3_BUCKET --s3-bucket
add_value_arg CHAIN_DATA_S3_REGION --s3-region
add_repeated_arg CHAIN_DATA_S3_ENDPOINT_URLS --s3-endpoint-url
add_value_arg CHAIN_DATA_S3_PREFIX --s3-prefix
add_value_arg CHAIN_DATA_S3_MAX_CONCURRENCY --s3-max-concurrency
add_value_arg CHAIN_DATA_S3_ACCESS_KEY_ID --s3-access-key-id
add_value_arg CHAIN_DATA_S3_SECRET_ACCESS_KEY --s3-secret-access-key
add_bool_arg CHAIN_DATA_S3_FORCE_PATH_STYLE --s3-force-path-style
add_bool_arg CHAIN_DATA_S3_CREATE_BUCKET --s3-create-bucket

add_value_arg CHAIN_DATA_DYNAMO_TABLE --dynamo-table
add_value_arg CHAIN_DATA_DYNAMO_TABLE_LAYOUT --dynamo-table-layout
add_value_arg CHAIN_DATA_DYNAMO_TABLE_PREFIX --dynamo-table-prefix
add_value_arg CHAIN_DATA_SCYLLA_CONCURRENCY --scylla-concurrency
add_value_arg CHAIN_DATA_DYNAMO_ENDPOINT_URL --dynamo-endpoint-url
add_value_arg CHAIN_DATA_DYNAMO_REGION --dynamo-region
add_value_arg CHAIN_DATA_DYNAMO_MAX_CONCURRENCY --dynamo-max-concurrency
add_value_arg CHAIN_DATA_DYNAMO_TABLE_MAX_CONCURRENCY --dynamo-table-max-concurrency
add_value_arg CHAIN_DATA_DYNAMO_ACCESS_KEY_ID --dynamo-access-key-id
add_value_arg CHAIN_DATA_DYNAMO_SECRET_ACCESS_KEY --dynamo-secret-access-key
add_value_arg CHAIN_DATA_DYNAMO_SESSION_TOKEN --dynamo-session-token
add_value_arg CHAIN_DATA_DYNAMO_BLOB_TABLE --dynamo-blob-table
add_value_arg CHAIN_DATA_DYNAMO_BLOB_CHUNK_SIZE --dynamo-blob-chunk-size
add_bool_arg CHAIN_DATA_SCYLLA_PROFILE --scylla-profile
add_bool_arg CHAIN_DATA_DYNAMO_CREATE_TABLE --dynamo-create-table

exec "${CHAIN_DATA_INGEST_BIN:-/usr/local/bin/chain-data-ingest}" "${args[@]}"
