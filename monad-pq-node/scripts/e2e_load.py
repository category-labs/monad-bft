#!/usr/bin/env python3
"""Drive txs into a local pq stack and time them until the explorer shows
them (GET /api/tx/{hash} answers 200).

Three channels run side by side, each sending one tx per --interval-ms at a
random phase within the interval:
  explorer  POST <explorer>/api/tx            payload "e2e-explorer-<run>-<i>"
  rpc       POST <rpc>/tx (the colocated rpc) payload "e2e-rpc-<run>-<i>"
  udp       mcp-tx send --node (raw udp) to the proposer of the lowest
            occupied lane in node 0's newest ledger block
            payload "e2e-udp-<run>-<i>"

Every tx carries sent_at_ns, so the ledger itself gives the exact
submit -> finalized latency (examples/latency.rs with --label); this script
reports submit -> visible in the explorer, which adds the explorer's tail
interval and the poll period.

usage: e2e_load.py --explorer URL --rpc URL --mcp-tx BIN --ledger-dir D
                   --validators "0=127.0.0.1:p0,1=127.0.0.1:p1,..."
                   [--seconds S] [--interval-ms I] [--channels explorer,rpc,udp]
"""

import argparse
import json
import math
import random
import subprocess
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

POLL_S = 0.01
VISIBLE_TIMEOUT_S = 15


def now_ns():
    return time.time_ns()


def http(method, url, body=None, timeout=5):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"content-type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read() or b"null")
    except urllib.error.HTTPError as error:
        return error.code, None


class Tracker:
    """Pending hashes polled on the explorer until visible."""

    def __init__(self, explorer):
        self.explorer = explorer
        self.lock = threading.Lock()
        self.pending = {}  # hash -> (channel, submit_ns)
        self.visible = {}  # channel -> [ms]
        self.lost = {}  # channel -> count
        self.done = threading.Event()

    def add(self, channel, tx_hash, submit_ns):
        with self.lock:
            self.pending[tx_hash] = (channel, submit_ns)

    def run(self):
        while not (self.done.is_set() and not self.pending):
            with self.lock:
                items = list(self.pending.items())
            for tx_hash, (channel, submit_ns) in items:
                status, _ = http("GET", f"{self.explorer}/api/tx/{tx_hash}")
                seen_ns = now_ns()
                if status == 200:
                    with self.lock:
                        self.pending.pop(tx_hash, None)
                        self.visible.setdefault(channel, []).append((seen_ns - submit_ns) / 1e6)
                elif (seen_ns - submit_ns) / 1e9 > VISIBLE_TIMEOUT_S:
                    with self.lock:
                        self.pending.pop(tx_hash, None)
                        self.lost[channel] = self.lost.get(channel, 0) + 1
            time.sleep(POLL_S)


def send_times(count, interval_s):
    """One send per interval at a uniformly random phase, so sends never lock
    onto the slot clock."""
    start = time.monotonic()
    for i in range(count):
        at = start + (i + random.random()) * interval_s
        time.sleep(max(0.0, at - time.monotonic()))
        yield i


def submit_http(tracker, channel, url, run, count, interval_s):
    errors = 0
    for i in send_times(count, interval_s):
        sent = now_ns()
        body = {"payload_utf8": f"e2e-{channel}-{run}-{i}", "sent_at_ns": sent}
        status, reply = http("POST", url, body)
        if status != 200 or not reply:
            errors += 1
            continue
        tracker.add(channel, reply["tx_hash"], sent)
    return errors


def current_proposer(ledger_dir):
    """The proposer of the lowest occupied lane in node 0's newest block."""
    blocks = sorted((Path(ledger_dir) / "blocks").glob("[0-9]" * 12))
    for block in reversed(blocks[-5:]):
        try:
            meta = json.loads((block / "meta.json").read_text())
        except (OSError, json.JSONDecodeError):
            continue
        for lane in meta["lanes"]:
            if lane.get("proposer") is not None:
                return lane["proposer"]
    return 0


def submit_udp(tracker, mcp_tx, validators, ledger_dir, run, count, interval_s):
    errors = 0
    for i in send_times(count, interval_s):
        node = current_proposer(ledger_dir)
        cmd = [mcp_tx, "send", "--node", validators[node], "--sender-id", "0",
               "--payload", f"e2e-udp-{run}-{i}"]
        submit_ns = now_ns()
        done = subprocess.run(cmd, capture_output=True, text=True)
        try:
            tracker.add("udp", json.loads(done.stdout)["tx_hash"], submit_ns)
        except (json.JSONDecodeError, KeyError):
            errors += 1
    return errors


def percentile(sorted_samples, p):
    rank = math.ceil(p / 100 * len(sorted_samples))
    return sorted_samples[min(max(rank, 1), len(sorted_samples)) - 1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--explorer", required=True)
    parser.add_argument("--rpc", required=True)
    parser.add_argument("--mcp-tx", required=True)
    parser.add_argument("--ledger-dir", required=True)
    parser.add_argument("--validators", required=True)
    parser.add_argument("--seconds", type=float, default=30)
    parser.add_argument("--interval-ms", type=float, default=100)
    parser.add_argument("--channels", default="explorer,rpc,udp")
    args = parser.parse_args()

    validators = dict(item.split("=", 1) for item in args.validators.split(","))
    validators = {int(k): v for k, v in validators.items()}
    interval_s = args.interval_ms / 1000
    count = int(args.seconds / interval_s)
    run = time.strftime("%H%M%S")
    channels = args.channels.split(",")

    tracker = Tracker(args.explorer)
    poller = threading.Thread(target=tracker.run, daemon=True)
    poller.start()

    results = {}
    jobs = {
        "explorer": lambda: submit_http(tracker, "explorer", f"{args.explorer}/api/tx", run, count, interval_s),
        "rpc": lambda: submit_http(tracker, "rpc", f"{args.rpc}/tx", run, count, interval_s),
        "udp": lambda: submit_udp(tracker, args.mcp_tx, validators, args.ledger_dir, run, count, interval_s),
    }
    threads = []
    for channel in channels:
        def work(channel=channel):
            results[channel] = jobs[channel]()
        thread = threading.Thread(target=work)
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()
    tracker.done.set()
    poller.join(timeout=VISIBLE_TIMEOUT_S + 5)

    print(f"submit -> visible in the explorer ({count} txs per channel, one per {args.interval_ms:.0f} ms):")
    for channel in channels:
        samples = sorted(tracker.visible.get(channel, []))
        lost = tracker.lost.get(channel, 0)
        errors = results.get(channel, 0)
        if not samples:
            print(f"  {channel:<9} visible=0 lost={lost} errors={errors}")
            continue
        print(f"  {channel:<9} visible={len(samples):<5} p50={percentile(samples, 50):>7.1f} "
              f"p90={percentile(samples, 90):>7.1f} p99={percentile(samples, 99):>7.1f} "
              f"max={samples[-1]:>7.1f} ms  lost={lost} errors={errors}")
    print(f"run tag: {run}")


if __name__ == "__main__":
    main()
