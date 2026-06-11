#!/usr/bin/env python3
"""Explore mainnet-1 chain-data via queryX to pick benchmark corpus parameters.

Finds: published head, rows/block density for logs/txs/traces/transfers,
top ERC-20 contracts by Transfer count, busy wallets (senders/recipients),
hot function selectors.
"""

import argparse
import json
from collections import Counter

import requests

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def rpc(session, url, method, params):
    body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": [params]}
    r = session.post(url, json=body, timeout=600)
    r.raise_for_status()
    p = r.json()
    if "error" in p:
        raise RuntimeError(f"{method}: {p['error']}")
    return p["result"]


def h2i(h):
    return int(h, 16)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://127.0.0.1:9545")
    ap.add_argument("--window", type=int, default=200, help="blocks per density sample")
    args = ap.parse_args()
    s = requests.Session()

    # 1. Published head
    head_res = rpc(s, args.url, "eth_queryBlocks",
                   {"fromBlock": "latest", "order": "desc", "limit": 1})
    head_blk = head_res["data"]["blocks"][0]
    head = h2i(head_blk["number"])
    print(f"published head: {head} ({head/1e6:.2f}M)")
    print(f"head block fields: {sorted(head_blk.keys())}")

    # Sample ranges: recent tip, mid-history, early-history
    ranges = {
        "tip": (head - args.window, head),
        "mid": (head // 2, head // 2 + args.window),
        "early": (1_000_000, 1_000_000 + args.window),
    }

    for label, (lo, hi) in ranges.items():
        print(f"\n=== range {label}: blocks {lo}..{hi} ===")
        # block timestamps for rate
        blks = rpc(s, args.url, "eth_queryBlocks",
                   {"fromBlock": hex(lo), "toBlock": hex(hi), "limit": 10000,
                    "fields": {"blocks": ["number", "timestamp"]}})["data"]["blocks"]
        if len(blks) >= 2:
            dt = h2i(blks[-1]["timestamp"]) - h2i(blks[0]["timestamp"])
            print(f"blocks: {len(blks)}, span {dt}s, {dt/max(1,len(blks)-1):.2f}s/block")

        for method, key, fields in [
            ("eth_queryTransactions", "transactions", ["blockNumber"]),
            ("eth_queryLogs", "logs", ["blockNumber"]),
            ("eth_queryTraces", "traces", ["blockNumber"]),
            ("eth_queryTransfers", "transfers", ["blockNumber"]),
        ]:
            params = {"fromBlock": hex(lo), "toBlock": hex(hi), "limit": 10000,
                      "fields": {key: fields}}
            try:
                res = rpc(s, args.url, method, params)
                rows = res["data"].get(key, [])
                cursor = h2i(res["cursorBlock"]["number"])
                scanned = cursor - lo + 1
                print(f"{key:>13}: {len(rows):>6} rows in {scanned:>4} blocks "
                      f"({len(rows)/max(1,scanned):.1f}/block)"
                      f"{' [partial window]' if cursor < hi else ''}")
            except RuntimeError as e:
                print(f"{key:>13}: ERROR {e}")

    # 2. Hot contracts / topics in recent window
    lo, hi = head - 2000, head
    print(f"\n=== hot-set discovery: blocks {lo}..{hi} ===")
    addr_count = Counter()
    topic0_count = Counter()
    token_count = Counter()
    wallet_count = Counter()
    cur = lo
    pages = 0
    while cur <= hi and pages < 30:
        res = rpc(s, args.url, "eth_queryLogs",
                  {"fromBlock": hex(cur), "toBlock": hex(hi), "limit": 10000,
                   "fields": {"logs": ["address", "topics"]}})
        logs = res["data"].get("logs", [])
        for lg in logs:
            addr_count[lg["address"]] += 1
            t = lg.get("topics") or []
            if t:
                topic0_count[t[0]] += 1
                if t[0] == TRANSFER_TOPIC and len(t) >= 3:
                    token_count[lg["address"]] += 1
                    wallet_count["0x" + t[1][-40:]] += 1
                    wallet_count["0x" + t[2][-40:]] += 1
        cur = h2i(res["cursorBlock"]["number"]) + 1
        pages += 1
    print(f"scanned logs pages: {pages}, blocks {lo}..{cur-1}, total logs {sum(addr_count.values())}")
    print("\ntop 15 log-emitting contracts:")
    for a, c in addr_count.most_common(15):
        print(f"  {a} {c}")
    print("\ntop 10 topic0:")
    for t, c in topic0_count.most_common(10):
        print(f"  {t} {c}")
    print("\ntop 15 ERC20 tokens by Transfer count:")
    for a, c in token_count.most_common(15):
        print(f"  {a} {c}")
    print("\ntop 15 transfer wallets:")
    for a, c in wallet_count.most_common(15):
        print(f"  {a} {c}")

    # 3. Hot tx senders/recipients + selectors in recent window
    print(f"\n=== tx hot-set: blocks {head-500}..{head} ===")
    from_count = Counter()
    to_count = Counter()
    sel_count = Counter()
    cur = head - 500
    pages = 0
    while cur <= head and pages < 20:
        res = rpc(s, args.url, "eth_queryTransactions",
                  {"fromBlock": hex(cur), "toBlock": hex(head), "limit": 10000,
                   "fields": {"transactions": ["from", "to", "input"]}})
        txs = res["data"].get("transactions", [])
        for tx in txs:
            from_count[tx.get("from")] += 1
            if tx.get("to"):
                to_count[tx["to"]] += 1
            inp = tx.get("input") or "0x"
            if len(inp) >= 10:
                sel_count[inp[:10]] += 1
        cur = h2i(res["cursorBlock"]["number"]) + 1
        pages += 1
    print(f"txs sampled: {sum(from_count.values())}")
    print("\ntop 10 tx senders:")
    for a, c in from_count.most_common(10):
        print(f"  {a} {c}")
    print("\ntop 10 tx recipients:")
    for a, c in to_count.most_common(10):
        print(f"  {a} {c}")
    print("\ntop 10 selectors:")
    for sl, c in sel_count.most_common(10):
        print(f"  {sl} {c}")

    # dump machine-readable hot-set for corpus generation
    out = {
        "head": head,
        "top_contracts": addr_count.most_common(20),
        "top_topic0": topic0_count.most_common(10),
        "top_tokens": token_count.most_common(20),
        "top_wallets": wallet_count.most_common(20),
        "top_senders": from_count.most_common(15),
        "top_recipients": to_count.most_common(15),
        "top_selectors": sel_count.most_common(10),
    }
    with open("hotset.json", "w") as f:
        json.dump(out, f, indent=1)
    print("\nwrote hotset.json")


if __name__ == "__main__":
    main()
