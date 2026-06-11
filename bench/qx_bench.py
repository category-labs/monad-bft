#!/usr/bin/env python3
"""queryX benchmark harness.

Runs a corpus of eth_query* queries against a monad-rpc endpoint, paginating
each query to completion (or a page cap) and recording per-page and
full-iteration timings.

Usage:
  qx_bench.py --corpus corpus.json --tag baseline [--url http://127.0.0.1:8080]
              [--out results.jsonl] [--only name1,name2] [--repeat N]

Corpus entry shape:
  {
    "name": "usdc-transfers-recent",
    "method": "eth_queryLogs",
    "params": { ... queryX request object ... },
    "max_pages": 100,        # optional, default 100
    "repeat": 1              # optional, per-query repeats
  }
"""

import argparse
import json
import statistics
import sys
import time

import requests


def hex_to_int(h):
    return int(h, 16)


def run_query(session, url, method, params, max_pages, timeout):
    """Paginate one query to completion. Returns (summary, pages)."""
    pages = []
    cur_params = dict(params)
    order = cur_params.get("order", "asc")
    total_rows = 0
    rid = 0
    t_query0 = time.monotonic()
    while True:
        rid += 1
        body = {"jsonrpc": "2.0", "id": rid, "method": method, "params": [cur_params]}
        t0 = time.monotonic()
        resp = session.post(url, json=body, timeout=timeout)
        elapsed_ms = (time.monotonic() - t0) * 1e3
        resp.raise_for_status()
        payload = resp.json()
        if "error" in payload:
            return {
                "error": payload["error"],
                "pages": len(pages),
                "page_ms": [p["ms"] for p in pages],
            }, pages
        result = payload["result"]
        data = result.get("data", {})
        primary_key = {
            "eth_queryBlocks": "blocks",
            "eth_queryTransactions": "transactions",
            "eth_queryLogs": "logs",
            "eth_queryTraces": "traces",
            "eth_queryTransfers": "transfers",
        }[method]
        nrows = len(data.get(primary_key, []))
        total_rows += nrows
        cursor = hex_to_int(result["cursorBlock"]["number"])
        to_block = hex_to_int(result["toBlock"]["number"])
        from_block = hex_to_int(result["fromBlock"]["number"])
        pages.append(
            {
                "ms": elapsed_ms,
                "rows": nrows,
                "from": from_block,
                "cursor": cursor,
                "bytes": len(resp.content),
            }
        )
        done = cursor == to_block
        if done or len(pages) >= max_pages:
            break
        if order == "desc":
            cur_params["fromBlock"] = hex(cursor - 1)
            cur_params["toBlock"] = hex(to_block)
        else:
            cur_params["fromBlock"] = hex(cursor + 1)
            cur_params["toBlock"] = hex(to_block)
    total_ms = (time.monotonic() - t_query0) * 1e3
    page_ms = [p["ms"] for p in pages]
    blocks_scanned = sum(abs(p["cursor"] - p["from"]) + 1 for p in pages)
    summary = {
        "pages": len(pages),
        "complete": done,
        "total_rows": total_rows,
        "total_ms": round(total_ms, 1),
        "page_ms_p50": round(statistics.median(page_ms), 1),
        "page_ms_min": round(min(page_ms), 1),
        "page_ms_max": round(max(page_ms), 1),
        "page_ms_p95": round(
            statistics.quantiles(page_ms, n=20)[18] if len(page_ms) >= 20 else max(page_ms), 1
        ),
        "first_page_ms": round(page_ms[0], 1),
        "blocks_scanned": blocks_scanned,
        "rows_per_s": round(total_rows / (total_ms / 1e3), 1) if total_ms > 0 else 0,
        "blocks_per_s": round(blocks_scanned / (total_ms / 1e3), 1) if total_ms > 0 else 0,
        "resp_bytes": sum(p["bytes"] for p in pages),
    }
    return summary, pages


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--corpus", required=True)
    ap.add_argument("--url", default="http://127.0.0.1:8080")
    ap.add_argument("--tag", required=True)
    ap.add_argument("--out", default=None)
    ap.add_argument("--only", default=None, help="comma-separated query names")
    ap.add_argument("--repeat", type=int, default=1, help="global repeat multiplier")
    ap.add_argument("--timeout", type=float, default=300.0)
    args = ap.parse_args()

    with open(args.corpus) as f:
        corpus = json.load(f)
    if args.only:
        names = set(args.only.split(","))
        corpus = [c for c in corpus if c["name"] in names]

    out_path = args.out or f"results-{args.tag}.jsonl"
    out = open(out_path, "a")
    session = requests.Session()

    print(f"{'name':<42} {'rep':>3} {'pages':>5} {'rows':>8} {'total_ms':>9} "
          f"{'pg_p50':>7} {'pg_max':>8} {'first':>7}")
    for entry in corpus:
        repeats = entry.get("repeat", 1) * args.repeat
        for rep in range(repeats):
            summary, pages = run_query(
                session,
                args.url,
                entry["method"],
                entry["params"],
                entry.get("max_pages", 100),
                args.timeout,
            )
            record = {
                "tag": args.tag,
                "name": entry["name"],
                "rep": rep,
                "method": entry["method"],
                "params": entry["params"],
                "summary": summary,
                "pages": pages,
                "ts": time.time(),
            }
            out.write(json.dumps(record) + "\n")
            out.flush()
            if "error" in summary:
                print(f"{entry['name']:<42} {rep:>3} ERROR {summary['error']}")
            else:
                s = summary
                print(
                    f"{entry['name']:<42} {rep:>3} {s['pages']:>5} {s['total_rows']:>8} "
                    f"{s['total_ms']:>9} {s['page_ms_p50']:>7} {s['page_ms_max']:>8} "
                    f"{s['first_page_ms']:>7}{'' if s['complete'] else '  (capped)'}"
                )
    out.close()
    print(f"\nwrote {out_path}")


if __name__ == "__main__":
    main()
