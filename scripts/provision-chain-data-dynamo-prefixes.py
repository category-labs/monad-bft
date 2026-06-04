#!/usr/bin/env python3
import argparse
import concurrent.futures
import json
import os
import random
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SUFFIXES = (
    "block-evm-header",
    "block-hash-to-number-index",
    "block-metadata",
    "log-bitmap-by-block",
    "log-bitmap-page-blob",
    "log-bitmap-page-counts",
    "log-dict-by-version",
    "log-dir-bucket",
    "log-dir-by-block",
    "log-open-bitmap-stream",
    "publication-state",
    "trace-bitmap-by-block",
    "trace-bitmap-page-blob",
    "trace-bitmap-page-counts",
    "trace-dict-by-version",
    "trace-dir-bucket",
    "trace-dir-by-block",
    "trace-open-bitmap-stream",
    "tx-bitmap-by-block",
    "tx-bitmap-page-blob",
    "tx-bitmap-page-counts",
    "tx-dict-by-version",
    "tx-dir-bucket",
    "tx-dir-by-block",
    "tx-hash-index",
    "tx-open-bitmap-stream",
)


class RateLimiter:
    def __init__(self, rate_per_second: float) -> None:
        self.min_interval = 0.0 if rate_per_second <= 0 else 1.0 / rate_per_second
        self.next_at = 0.0
        self.lock = threading.Lock()

    def wait(self) -> None:
        if self.min_interval == 0.0:
            return
        with self.lock:
            now = time.monotonic()
            sleep_s = max(0.0, self.next_at - now)
            self.next_at = max(now, self.next_at) + self.min_interval
        if sleep_s > 0.0:
            time.sleep(sleep_s)


@dataclass(frozen=True)
class AwsResult:
    ok: bool
    stdout: str
    stderr: str
    returncode: int


class Provisioner:
    def __init__(self, args: argparse.Namespace, ready_file: Path) -> None:
        self.args = args
        self.ready_file = ready_file
        self.ready_lock = threading.Lock()
        self.aws_limiter = RateLimiter(args.aws_rate_per_second)

    def append_ready(self, line: str) -> None:
        with self.ready_lock:
            with self.ready_file.open("a", encoding="utf-8") as f:
                f.write(line.rstrip() + "\n")
            print(line, flush=True)

    def log(self, log_path: Path, line: str) -> None:
        timestamp = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
        with log_path.open("a", encoding="utf-8") as f:
            f.write(f"{timestamp} {line.rstrip()}\n")

    def aws(self, cmd: list[str], timeout_s: int, log_path: Path) -> AwsResult:
        full_cmd = ["aws", *cmd]
        self.aws_limiter.wait()
        started = time.monotonic()
        try:
            proc = subprocess.run(
                full_cmd,
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=timeout_s,
            )
        except subprocess.TimeoutExpired as exc:
            stdout = exc.stdout or ""
            stderr = exc.stderr or ""
            elapsed_ms = int((time.monotonic() - started) * 1000)
            self.log(
                log_path,
                "aws timeout "
                f"elapsed_ms={elapsed_ms} timeout_s={timeout_s} "
                f"cmd={json.dumps(full_cmd)} stdout={json.dumps(stdout[-2000:])} "
                f"stderr={json.dumps(stderr[-2000:])}",
            )
            return AwsResult(False, stdout, stderr, 124)

        elapsed_ms = int((time.monotonic() - started) * 1000)
        self.log(
            log_path,
            "aws "
            f"returncode={proc.returncode} elapsed_ms={elapsed_ms} "
            f"cmd={json.dumps(full_cmd)}",
        )
        if proc.stdout.strip() and not proc.returncode == 0:
            self.log(log_path, f"stdout={proc.stdout.strip()[-4000:]}")
        if proc.stderr.strip():
            self.log(log_path, f"stderr={proc.stderr.strip()[-4000:]}")
        return AwsResult(proc.returncode == 0, proc.stdout, proc.stderr, proc.returncode)

    def sleep_before_retry(self, attempt: int, log_path: Path, table: str, reason: str) -> None:
        base = self.args.retry_sleep
        cap = self.args.max_retry_sleep
        sleep_s = min(cap, base * (2 ** max(0, attempt - 1)))
        sleep_s += random.uniform(0.0, min(1.0, sleep_s * 0.1))
        self.log(
            log_path,
            f"{table} retry_sleep seconds={sleep_s:.2f} attempt={attempt} reason={reason}",
        )
        time.sleep(sleep_s)

    def describe_table(self, table: str, log_path: Path) -> dict[str, Any] | None:
        result = self.aws(
            [
                "dynamodb",
                "describe-table",
                "--region",
                self.args.region,
                "--table-name",
                table,
                "--output",
                "json",
            ],
            self.args.aws_timeout,
            log_path,
        )
        if not result.ok:
            return None
        try:
            return json.loads(result.stdout).get("Table")
        except json.JSONDecodeError as exc:
            self.log(log_path, f"{table} describe json decode failed error={exc}")
            return None

    def table_exists(self, table: str, log_path: Path) -> bool:
        described = self.describe_table(table, log_path)
        if described is not None:
            self.log(log_path, f"{table} exists status={described.get('TableStatus')}")
            return True
        return False

    def create_table(self, prefix: str, suffix: str, log_path: Path) -> str:
        table = f"{prefix}-{suffix}"
        if self.table_exists(table, log_path):
            return table

        for attempt in range(1, self.args.retries + 1):
            self.log(log_path, f"{table} creating attempt={attempt}/{self.args.retries}")
            result = self.aws(
                [
                    "dynamodb",
                    "create-table",
                    "--region",
                    self.args.region,
                    "--table-name",
                    table,
                    "--attribute-definitions",
                    "AttributeName=pk,AttributeType=B",
                    "AttributeName=sk,AttributeType=B",
                    "--key-schema",
                    "AttributeName=pk,KeyType=HASH",
                    "AttributeName=sk,KeyType=RANGE",
                    "--billing-mode",
                    "PAY_PER_REQUEST",
                    "--warm-throughput",
                    (
                        f"ReadUnitsPerSecond={self.args.read_warm},"
                        f"WriteUnitsPerSecond={self.args.write_warm}"
                    ),
                    "--output",
                    "json",
                ],
                self.args.aws_timeout,
                log_path,
            )
            if result.ok:
                return table

            combined = f"{result.stdout}\n{result.stderr}"
            if "ResourceInUseException" in combined:
                self.log(log_path, f"{table} already exists or is being created")
                return table

            if attempt == self.args.retries:
                raise RuntimeError(
                    f"{table} create failed after {self.args.retries} attempts: "
                    f"{combined.strip()[-1000:]}"
                )
            self.sleep_before_retry(attempt, log_path, table, "create-table failed")

        raise AssertionError("unreachable")

    def wait_table_ready(self, table: str, log_path: Path) -> None:
        for attempt in range(1, self.args.ready_attempts + 1):
            described = self.describe_table(table, log_path)
            if described is not None:
                status = described.get("TableStatus")
                warm_status = (described.get("WarmThroughput") or {}).get("Status")
                self.log(
                    log_path,
                    f"{table} readiness attempt={attempt}/{self.args.ready_attempts} "
                    f"status={status} warm_status={warm_status}",
                )
                if status == "ACTIVE" and (warm_status in ("ACTIVE", None)):
                    return

            if attempt == self.args.ready_attempts:
                raise RuntimeError(f"{table} did not become ready")
            self.sleep_before_retry(attempt, log_path, table, "not ready")

    def create_prefix(self, prefix: str) -> bool:
        start = time.monotonic()
        log_path = self.args.out_dir / f"chain-data-provision-{prefix}.log"
        time_path = Path(f"{log_path}.time")
        log_path.write_text("", encoding="utf-8")

        self.append_ready(f"{prefix} START {datetime.now().astimezone().isoformat(timespec='seconds')}")
        self.log(
            log_path,
            "config "
            f"prefix={prefix} region={self.args.region} read_warm={self.args.read_warm} "
            f"write_warm={self.args.write_warm} parallel_tables={self.args.parallel_tables} "
            f"retries={self.args.retries} ready_attempts={self.args.ready_attempts} "
            f"aws_rate_per_second={self.args.aws_rate_per_second}",
        )

        try:
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.args.parallel_tables
            ) as executor:
                futures = {
                    executor.submit(self.create_table, prefix, suffix, log_path): suffix
                    for suffix in SUFFIXES
                }
                tables: list[str] = []
                for future in concurrent.futures.as_completed(futures):
                    suffix = futures[future]
                    try:
                        table = future.result()
                    except Exception as exc:
                        self.log(log_path, f"{prefix}-{suffix} create failed error={exc}")
                        raise
                    tables.append(table)
                    self.log(log_path, f"{table} create step complete")

            for table in sorted(tables):
                self.wait_table_ready(table, log_path)

            elapsed_s = int(time.monotonic() - start)
            time_path.write_text(f"real {elapsed_s}\n", encoding="utf-8")
            self.append_ready(
                f"{prefix} READY {datetime.now().astimezone().isoformat(timespec='seconds')} "
                f"log={log_path}"
            )
            return True
        except Exception as exc:
            elapsed_s = int(time.monotonic() - start)
            time_path.write_text(f"real {elapsed_s}\n", encoding="utf-8")
            self.log(log_path, f"{prefix} failed error={exc}")
            self.append_ready(
                f"{prefix} FAILED {datetime.now().astimezone().isoformat(timespec='seconds')} "
                f"log={log_path} error={exc}"
            )
            return False


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be >= 1")
    return parsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Provision per-logical-table DynamoDB prefixes for chain-data benchmarks."
    )
    parser.add_argument("--prefix", help="Create exactly this prefix. Implies --count 1.")
    parser.add_argument("--base", default="monad-chain-data-pt-spare")
    parser.add_argument("--count", type=positive_int, default=1)
    parser.add_argument("--region", default="us-east-2")
    parser.add_argument("--parallel-tables", type=positive_int, default=2)
    parser.add_argument("--read-warm", type=positive_int, default=12000)
    parser.add_argument("--write-warm", type=positive_int, default=40000)
    parser.add_argument("--out-dir", type=Path, default=Path("/home/jhow/monad-bft"))
    parser.add_argument("--retries", type=positive_int, default=8)
    parser.add_argument("--retry-sleep", type=positive_int, default=5)
    parser.add_argument("--max-retry-sleep", type=positive_int, default=60)
    parser.add_argument("--ready-attempts", type=positive_int, default=30)
    parser.add_argument("--aws-timeout", type=positive_int, default=60)
    parser.add_argument(
        "--aws-rate-per-second",
        type=float,
        default=8.0,
        help="Global AWS CLI call rate limit. Use <=0 to disable.",
    )
    args = parser.parse_args()
    if args.prefix:
        args.count = 1
    if args.aws_rate_per_second < 0:
        parser.error("--aws-rate-per-second must be >= 0")
    return args


def main() -> int:
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    ready_file = args.out_dir / f"chain-data-dynamo-prefixes-{datetime.now().strftime('%Y%m%d-%H%M%S')}.txt"
    provisioner = Provisioner(args, ready_file)
    failures = 0

    for i in range(1, args.count + 1):
        if args.prefix:
            prefix = args.prefix
        else:
            prefix = f"{args.base}-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{i}"
        if not provisioner.create_prefix(prefix):
            failures += 1

    print(f"ready_file={ready_file}", flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
