#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

region="us-east-2"
source_bucket="mainnet-deu-009-0"
aws_profile="default"
start=1
count=1000000
batch_size=5000
fetch_concurrency=4000
fetch_buffer=100000
fetch_strategy="spawned-tasks"
write_workers=1
dynamo_concurrency=256
dynamo_table_concurrency=16
s3_concurrency=512
io_max_retries=5
dynamo_prefix=""
s3_bucket="monad-chain-data-ingest-test-515966509249-us-east-2"
s3_prefix=""
log_dir="$repo_root"
log_every=5
open_index_snapshot=""
tokio_console=false
extra_args=()

usage() {
  cat <<'USAGE'
Usage: bench-chain-data-aws-direct.sh --dynamo-prefix <prefix> [options]

Runs the chain-data ingest benchmark directly with AWS archive input,
DynamoDB metadata, and S3 blob output. Defaults are the current 1M
ww1/dyn256 bench shape.

Required:
  --dynamo-prefix <prefix>       Fresh per-logical-table Dynamo prefix.

Options:
  --count <n>                    Blocks to ingest. Default: 1000000.
  --start <n>                    First block. Default: 1.
  --batch-size <n>               Blocks per ingest batch. Default: 5000.
  --fetch-concurrency <n>        Archive fetch concurrency. Default: 4000.
  --fetch-buffer <n>             Ordered fetch buffer. Default: 100000.
  --fetch-strategy <name>        semaphore-buffer|bounded-channel|spawned-tasks|
                                 spawned-semaphore-buffer.
                                 Default: spawned-tasks.
  --write-workers <n>            Durable write workers. Default: 1.
  --dynamo-concurrency <n>       BatchWriteItem concurrency. Default: 256.
  --dynamo-table-concurrency <n> BatchWriteItem concurrency per physical table.
                                 Default: 16.
  --s3-concurrency <n>           S3 PUT concurrency per batch. Default: 512.
  --io-max-retries <n>           Non-CAS IO write retries. Default: 5.
  --s3-bucket <bucket>           Output blob bucket.
  --s3-prefix <prefix>           Output blob prefix. Default: generated.
  --region <region>              AWS region for archive/S3/Dynamo. Default: us-east-2.
  --source-bucket <bucket>       Archive source bucket. Default: mainnet-deu-009-0.
  --profile <profile>            AWS profile for archive source. Default: default.
  --log-dir <dir>                Directory for run log/time files. Default: repo root.
  --log-every <n>                Progress log interval in batches. Default: 5.
  --open-index-snapshot <path>   Load/save local open-index snapshot for repeated benches.
  --tokio-console                Enable Tokio console instrumentation.
  --extra-arg <arg>              Append one extra chain-data-ingest arg. Repeatable.
  -h, --help                     Show this help.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dynamo-prefix)
      dynamo_prefix="$2"
      shift 2
      ;;
    --count)
      count="$2"
      shift 2
      ;;
    --start)
      start="$2"
      shift 2
      ;;
    --batch-size)
      batch_size="$2"
      shift 2
      ;;
    --fetch-concurrency)
      fetch_concurrency="$2"
      shift 2
      ;;
    --fetch-buffer)
      fetch_buffer="$2"
      shift 2
      ;;
    --fetch-strategy)
      fetch_strategy="$2"
      shift 2
      ;;
    --write-workers)
      write_workers="$2"
      shift 2
      ;;
    --dynamo-concurrency)
      dynamo_concurrency="$2"
      shift 2
      ;;
    --dynamo-table-concurrency|--dynamo-table-max-concurrency)
      dynamo_table_concurrency="$2"
      shift 2
      ;;
    --s3-concurrency)
      s3_concurrency="$2"
      shift 2
      ;;
    --io-max-retries)
      io_max_retries="$2"
      shift 2
      ;;
    --s3-bucket)
      s3_bucket="$2"
      shift 2
      ;;
    --s3-prefix)
      s3_prefix="$2"
      shift 2
      ;;
    --region)
      region="$2"
      shift 2
      ;;
    --source-bucket)
      source_bucket="$2"
      shift 2
      ;;
    --profile)
      aws_profile="$2"
      shift 2
      ;;
    --log-dir)
      log_dir="$2"
      shift 2
      ;;
    --log-every)
      log_every="$2"
      shift 2
      ;;
    --open-index-snapshot)
      open_index_snapshot="$2"
      shift 2
      ;;
    --tokio-console)
      tokio_console=true
      shift
      ;;
    --extra-arg)
      extra_args+=("$2")
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$dynamo_prefix" ]]; then
  echo "--dynamo-prefix is required and must be fresh" >&2
  usage >&2
  exit 2
fi
mkdir -p "$log_dir"
ts="$(date +%Y%m%d-%H%M%S)"
if [[ -z "$s3_prefix" ]]; then
  s3_prefix="chain-data-${count}-ww${write_workers}-dyn${dynamo_concurrency}-${ts}"
fi
log="$log_dir/chain-data-aws-${count}-ww${write_workers}-dyn${dynamo_concurrency}-${ts}.log"
features="archive-ingest,s3,dynamo"
if [[ "$tokio_console" == true ]]; then
  features="${features},tokio-console"
fi

cmd=(
  cargo +nightly-2025-12-09 run --release
  -p monad-chain-data-ingest
  --bin chain-data-ingest
  --features "$features"
  --
  --block-data-source "aws ${source_bucket} --profile ${aws_profile} --region ${region}"
  --start "$start"
  --count "$count"
  --batch-size "$batch_size"
  --concurrency "$fetch_concurrency"
  --fetch-buffer "$fetch_buffer"
  --fetch-strategy "$fetch_strategy"
  --write-workers "$write_workers"
  --write-buffer 2
  --progress-buffer 16
  --plan-buffer 2
  --single-writer
  --meta-backend dynamo
  --blob-backend s3
  --dynamo-region "$region"
  --dynamo-table-layout per-logical-table
  --dynamo-table-prefix "$dynamo_prefix"
  --dynamo-max-concurrency "$dynamo_concurrency"
  --dynamo-table-max-concurrency "$dynamo_table_concurrency"
  --s3-region "$region"
  --s3-bucket "$s3_bucket"
  --s3-prefix "$s3_prefix"
  --s3-max-concurrency "$s3_concurrency"
  --io-max-retries "$io_max_retries"
  --log-every "$log_every"
)

if [[ -n "$open_index_snapshot" ]]; then
  cmd+=(--open-index-snapshot "$open_index_snapshot")
fi

if [[ "$tokio_console" == true ]]; then
  cmd+=(--tokio-console)
fi

if (( ${#extra_args[@]} > 0 )); then
  cmd+=("${extra_args[@]}")
fi

echo "log=$log"
echo "s3_prefix=$s3_prefix"
echo "dynamo_prefix=$dynamo_prefix"
(
  cd "$repo_root"
  set -o pipefail
  if [[ "$tokio_console" == true ]]; then
    export RUSTFLAGS="${RUSTFLAGS:-} --cfg tokio_unstable"
  fi
  /usr/bin/time -v -o "$log.time" "${cmd[@]}" 2>&1 | tee "$log"
)
echo "LOG=$log"
