#!/usr/bin/env python3
"""Concurrent load profiling for queryX.

Closed-loop threaded workers sweep concurrency levels against one of two
mixes:

  hit:    overlapping explorer/app/analytic queries on fixed tip-era ranges —
          the whole working set is cache-resident after warmup.
  thrash: the same shapes but with randomized ranges spread over full chain
          history, sized so the touched metadata/bitmap regions far exceed
          the cache budget.

Per level: achieved QPS, latency percentiles, error counts, plus server
CPU%/RSS sampled mid-run.

Usage:
  qx_load.py --mix hit --url http://127.0.0.1:9545 --levels 1,4,8,16,32,64,128
             --duration 20 --tag workers2 [--server-pid PID]
"""

import argparse
import json
import random
import statistics
import threading
import time

import requests

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
TIP_LO = 0x4CE2000  # 80,617,472
TIP_HI = 0x4CE4000  # 80,625,664
HIST_LO = 10_000_000
HIST_HI = 80_000_000
TOKENS = [
    "0x754704bc059f8c67012fed69bc8a327a5aafb603",
    "0x3bd359c1119da7da1d913d1c4d2b7c461115433a",
    "0xee8c0e9f1bffb4eb878d8f15f368a02a35481242",
    "0xd18b7ec58cdf4876f6afebd3ed1730e4ce10414b",
    "0x00000000efe302beaa2b3e6e1b18d08d69a9012a",
]
HOT_CONTRACT = "0x34b6552d57a35a1d042ccae1951bd1c370112a6f"
HOT_WALLET = "0x188d586ddcf52439676ca21a244753fa19f9ea8e"


def hit_mix(rng):
    """Fixed ranges: every worker touches the same cache-resident set."""
    return rng.choice([
        ("eth_queryBlocks",
         {"fromBlock": "latest", "order": "desc", "limit": 25}),
        ("eth_queryTransactions",
         {"fromBlock": "latest", "order": "desc", "limit": 25,
          "fields": {"transactions": ["hash", "blockNumber", "from", "to", "value"],
                     "blocks": ["number", "timestamp"]}}),
        ("eth_queryLogs",
         {"filter": {"address": HOT_CONTRACT},
          "fromBlock": hex(TIP_LO), "toBlock": hex(TIP_HI), "limit": 100,
          "fields": {"logs": ["blockNumber", "logIndex", "data", "topics"]}}),
        ("eth_queryLogs",
         {"filter": {"topics": [TRANSFER_TOPIC, None,
                                "0x" + "0" * 24 + HOT_WALLET[2:]]},
          "fromBlock": "latest", "toBlock": hex(TIP_LO), "limit": 25,
          "order": "desc",
          "fields": {"logs": ["blockNumber", "address", "data"],
                     "blocks": ["number", "timestamp"]}}),
        ("eth_queryLogs",
         {"filter": {"address": TOKENS[0], "topics": [TRANSFER_TOPIC]},
          "fromBlock": hex(TIP_LO), "toBlock": hex(TIP_HI), "limit": 1000,
          "fields": {"logs": ["blockNumber", "logIndex", "data", "topics"]}}),
        ("eth_queryTransactions",
         {"fromBlock": hex(TIP_LO), "toBlock": hex(TIP_LO + 200), "limit": 1000,
          "fields": {"transactions": ["hash", "blockNumber", "from", "to", "value"]}}),
    ])


def thrash_mix(rng):
    """Randomized ranges across full history: working set >> cache."""
    lo = rng.randrange(HIST_LO, HIST_HI - 200_000)
    shape = rng.randrange(4)
    if shape == 0:
        return ("eth_queryBlocks",
                {"fromBlock": hex(lo), "toBlock": hex(lo + 999), "limit": 1000,
                 "fields": {"blocks": ["number", "timestamp", "gasUsed"]}})
    if shape == 1:
        return ("eth_queryLogs",
                {"fromBlock": hex(lo), "toBlock": hex(lo + 50), "limit": 1000,
                 "fields": {"logs": ["blockNumber", "address", "topics"]}})
    if shape == 2:
        return ("eth_queryLogs",
                {"filter": {"address": rng.choice(TOKENS),
                            "topics": [TRANSFER_TOPIC]},
                 "fromBlock": hex(lo), "toBlock": hex(lo + 100_000), "limit": 1000,
                 "fields": {"logs": ["blockNumber", "logIndex", "data"]}})
    return ("eth_queryTransactions",
            {"fromBlock": hex(lo), "toBlock": hex(lo + 100), "limit": 1000,
             "fields": {"transactions": ["hash", "blockNumber", "from", "to"]}})


def worker(url, mix_fn, seed, stop_at, out, errors):
    rng = random.Random(seed)
    session = requests.Session()
    lat = []
    while time.monotonic() < stop_at:
        method, params = mix_fn(rng)
        body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": [params]}
        t0 = time.monotonic()
        try:
            resp = session.post(url, json=body, timeout=60)
            elapsed = (time.monotonic() - t0) * 1e3
            payload = resp.json()
            if resp.status_code != 200 or "error" in payload:
                errors.append(str(payload.get("error", resp.status_code))[:120])
            else:
                lat.append(elapsed)
        except Exception as e:  # noqa: BLE001
            errors.append(type(e).__name__)
    out.extend(lat)


def sample_server(pid, samples, stop_at):
    """Samples (cpu_jiffies, rss_kb) until stop."""
    import os
    hz = os.sysconf("SC_CLK_TCK")
    last = None
    while time.monotonic() < stop_at:
        try:
            with open(f"/proc/{pid}/stat") as f:
                parts = f.read().split()
            jiffies = int(parts[13]) + int(parts[14])
            with open(f"/proc/{pid}/status") as f:
                rss_kb = next(int(l.split()[1]) for l in f if l.startswith("VmRSS"))
            now = time.monotonic()
            if last is not None:
                cpu = (jiffies - last[0]) / hz / (now - last[1]) * 100
                samples.append((cpu, rss_kb))
            last = (jiffies, now)
        except Exception:  # noqa: BLE001
            pass
        time.sleep(1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://127.0.0.1:9545")
    ap.add_argument("--mix", choices=["hit", "thrash"], required=True)
    ap.add_argument("--levels", default="1,4,8,16,32,64,128")
    ap.add_argument("--duration", type=float, default=20.0)
    ap.add_argument("--tag", required=True)
    ap.add_argument("--server-pid", type=int, default=None)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    mix_fn = hit_mix if args.mix == "hit" else thrash_mix
    out_path = args.out or f"load-{args.tag}-{args.mix}.jsonl"
    out_f = open(out_path, "a")

    print(f"{'conc':>5} {'qps':>8} {'p50ms':>8} {'p90ms':>8} {'p99ms':>9} "
          f"{'max':>9} {'errs':>5} {'cpu%':>6} {'rssGB':>6}")
    for level in [int(x) for x in args.levels.split(",")]:
        lat, errors, sysstats = [], [], []
        stop_at = time.monotonic() + args.duration
        threads = [
            threading.Thread(target=worker,
                             args=(args.url, mix_fn, 1000 * level + i, stop_at, lat, errors))
            for i in range(level)
        ]
        if args.server_pid:
            threads.append(threading.Thread(
                target=sample_server, args=(args.server_pid, sysstats, stop_at)))
        t_start = time.monotonic()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        wall = time.monotonic() - t_start
        n = len(lat)
        if n == 0:
            print(f"{level:>5} all-errors: {errors[:3]}")
            continue
        lat.sort()
        q = lambda p: lat[min(n - 1, int(n * p))]
        cpu = max((c for c, _ in sysstats), default=0)
        rss = max((r for _, r in sysstats), default=0) / 1048576
        row = {
            "tag": args.tag, "mix": args.mix, "concurrency": level,
            "qps": round(n / wall, 1), "n": n,
            "p50": round(q(0.50), 1), "p90": round(q(0.90), 1),
            "p99": round(q(0.99), 1), "max": round(lat[-1], 1),
            "errors": len(errors), "error_sample": errors[:5],
            "cpu_peak_pct": round(cpu, 1), "rss_peak_gb": round(rss, 2),
            "ts": time.time(),
        }
        out_f.write(json.dumps(row) + "\n")
        out_f.flush()
        print(f"{level:>5} {row['qps']:>8} {row['p50']:>8} {row['p90']:>8} "
              f"{row['p99']:>9} {row['max']:>9} {len(errors):>5} {cpu:>6.0f} {rss:>6.2f}")
    out_f.close()
    print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
