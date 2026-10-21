#!/usr/bin/env python3
"""Summarize a local pq run: per-node finalized counts, cross-node agreement
(lane roots of every slot finalized by more than one node), path breakdown
(fast / coda / gadget) and slot finalization latency after the deadline.

usage: summarize.py --dir D --nodes N [--skip-slots S]

Reads D/node-<i>.log ("finalized slot=N block=.. at=<ns> path=<P>",
"halted slot=N at=<ns>", "proposing") and D/ledger-<i>/blocks/*/meta.json.
"""

import argparse
import json
import math
import re
from collections import Counter, defaultdict
from pathlib import Path

ANSI = re.compile(r"\x1b\[[0-9;]*m")
FINALIZED = re.compile(r"\bfinalized slot=(\d+) block=(\S+) at=(\d+) path=(\w+)")
HALTED = re.compile(r"\bhalted slot=(\d+) at=(\d+)")


def percentile(sorted_samples, p):
    rank = math.ceil(p / 100 * len(sorted_samples))
    return sorted_samples[min(max(rank, 1), len(sorted_samples)) - 1]


def stats(samples):
    if not samples:
        return "n=0"
    s = sorted(samples)
    return "n={:<6} p50={:>7.1f} p90={:>7.1f} p99={:>7.1f} max={:>7.1f} ms".format(
        len(s), percentile(s, 50), percentile(s, 90), percentile(s, 99), s[-1]
    )


def read_log(path):
    finalized, halted = {}, {}
    proposed = warnings = errors = 0
    warning_lines = Counter()
    for line in ANSI.sub("", path.read_text(errors="replace")).splitlines():
        if m := FINALIZED.search(line):
            finalized[int(m[1])] = (m[2], int(m[3]), m[4])
        elif m := HALTED.search(line):
            halted[int(m[1])] = int(m[2])
        if " proposing " in line or line.endswith(" proposing"):
            proposed += 1
        if " WARN " in line:
            warnings += 1
            warning_lines[re.sub(r"^.*? WARN +(node\d+: )?", "", line)[:90]] += 1
        if " ERROR " in line or "panicked" in line:
            errors += 1
    return finalized, halted, proposed, warnings, errors, warning_lines


def read_ledger(path):
    blocks = {}
    for meta in (path / "blocks").glob("*/meta.json"):
        try:
            data = json.loads(meta.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        lanes = tuple(lane.get("root") for lane in data["lanes"])
        blocks[data["slot"]] = (lanes, data.get("deadline_ns"), data["finalized_at_ns"], data["tx_count"])
    return blocks


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", required=True, type=Path)
    parser.add_argument("--nodes", required=True, type=int)
    parser.add_argument("--skip-slots", type=int, default=0,
                        help="leave the first slots out of the latency stats")
    args = parser.parse_args()

    logs, ledgers = [], []
    for i in range(args.nodes):
        log = args.dir / f"node-{i}.log"
        logs.append(read_log(log) if log.exists() else ({}, {}, 0, 0, 0, Counter()))
        ledgers.append(read_ledger(args.dir / f"ledger-{i}"))

    print()
    print("node  finalized  ledger  all-committed  fast   coda   gadget  halted  proposed  warn  err")
    total_paths = Counter()
    for i, (finalized, halted, proposed, warnings, errors, _) in enumerate(logs):
        paths = Counter(path for _, _, path in finalized.values())
        total_paths.update(paths)
        full = sum(1 for shape, _, _ in finalized.values() if shape and set(shape) == {"+"})
        print(f"{i:<5} {len(finalized):<10} {len(ledgers[i]):<7} {full:<14} "
              f"{paths['Fast']:<6} {paths['Coda']:<6} {paths['Gadget']:<7} {len(halted):<7} "
              f"{proposed:<9} {warnings:<5} {errors}")

    # agreement: every slot two nodes both stored has the same lane roots
    print()
    by_slot = defaultdict(dict)
    for i, blocks in enumerate(ledgers):
        for slot, (lanes, *_rest) in blocks.items():
            by_slot[slot][i] = lanes
    disagreements = [slot for slot, per_node in by_slot.items() if len(set(per_node.values())) > 1]
    common = [slot for slot, per_node in by_slot.items() if len(per_node) == args.nodes]
    if disagreements:
        print(f"AGREEMENT VIOLATED in {len(disagreements)} slots, first {sorted(disagreements)[:5]}")
    else:
        print(f"agreement: OK ({len(by_slot)} slots stored, {len(common)} by every node, no conflicts)")
    finalized_sets = [set(f) for f, *_ in logs]
    if finalized_sets and all(s == finalized_sets[0] for s in finalized_sets):
        print(f"all nodes finalized the same {len(finalized_sets[0])} slots")
    else:
        sizes = [len(s) for s in finalized_sets]
        print(f"finalized slot sets differ in size: {sizes}")

    total = sum(total_paths.values()) or 1
    print()
    print("path breakdown (all nodes): " + ", ".join(
        f"{p.lower()} {total_paths[p]} ({100 * total_paths[p] / total:.1f}%)" for p in ("Fast", "Coda", "Gadget")))

    print()
    print("slot finalization latency after the deadline (all nodes):")
    by_path = defaultdict(list)
    halt_after = []
    for i, (finalized, halted, *_rest) in enumerate(logs):
        for slot, (_, at, path) in finalized.items():
            if slot < args.skip_slots or slot not in ledgers[i]:
                continue
            deadline = ledgers[i][slot][1]
            if deadline is None:
                continue
            after = (at - deadline) / 1e6
            by_path["all"].append(after)
            by_path[path].append(after)
            if slot in halted:
                halt_after.append((halted[slot] - deadline) / 1e6)
    for path in ("all", "Fast", "Coda", "Gadget"):
        print(f"  {path:<8} {stats(by_path[path])}")
    print(f"  {'halted':<8} {stats(halt_after)}")

    warning_lines = Counter()
    for *_rest, lines in logs:
        warning_lines.update(lines)
    if warning_lines:
        print()
        print("most frequent warnings:")
        for line, count in warning_lines.most_common(5):
            print(f"  {count:>6} {line}")
    return 1 if disagreements else 0


if __name__ == "__main__":
    raise SystemExit(main())
