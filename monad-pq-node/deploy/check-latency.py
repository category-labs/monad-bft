#!/usr/bin/env python3
"""check-latency.py <latency.toml> [--hosts h...] [--ips ip...] [--configs node.toml...]

Validates the RTT matrix latency.sh writes: rtt_ms is n x n with a zero diagonal and
finite positive entries elsewhere; `hosts` and `ips` (node_id order) match --hosts/--ips;
every --configs node.toml has exactly those validator ips in node_id order.
"""
import argparse
import math
import sys
import tomllib


def validator_ips(path):
    with open(path, "rb") as f:
        vs = tomllib.load(f).get("validators", [])
    vs = sorted(vs, key=lambda v: v["node_id"])
    if [v["node_id"] for v in vs] != list(range(len(vs))):
        raise ValueError(f"{path}: validator node_ids are not 0..{len(vs) - 1}")
    return [v["address"].rsplit(":", 1)[0] for v in vs]


def check(args):
    with open(args.matrix, "rb") as f:
        m = tomllib.load(f)
    ips = m.get("ips")
    rtt = m.get("rtt_ms")
    if not isinstance(ips, list) or not isinstance(rtt, list):
        return "needs `ips` and `rtt_ms`; re-run latency.sh"
    n = len(ips)
    if len(rtt) != n or any(not isinstance(r, list) or len(r) != n for r in rtt):
        return f"rtt_ms is not {n} x {n}"
    for i, row in enumerate(rtt):
        for j, v in enumerate(row):
            ok = isinstance(v, (int, float)) and math.isfinite(v) and (v == 0 if i == j else v > 0)
            if not ok:
                return f"rtt_ms[{i}][{j}] = {v!r}: want {'0' if i == j else 'a positive number'}"
    if args.hosts is not None and m.get("hosts") != args.hosts:
        return f"hosts {m.get('hosts')} != hosts.txt {args.hosts}"
    if args.ips is not None and ips != args.ips:
        return f"ips {ips} != resolved {args.ips}"
    for path in args.configs or []:
        got = validator_ips(path)
        if got != ips:
            return f"ips {ips} != {path} validators {got}"
    return None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("matrix")
    p.add_argument("--hosts", nargs="*")
    p.add_argument("--ips", nargs="*")
    p.add_argument("--configs", nargs="*")
    args = p.parse_args()
    try:
        err = check(args)
    except (OSError, ValueError, KeyError, tomllib.TOMLDecodeError) as e:
        err = str(e)
    if err:
        print(f"{args.matrix}: {err}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
