from datetime import datetime
from datetime import timedelta
import json
import argparse
import matplotlib.pyplot as plt
from collections import OrderedDict

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Block time stat", description="Display block time stat"
    )
    parser.add_argument("filename")

    args = parser.parse_args()

    filename = args.filename
    with open(filename, "r") as f:
        stats = json.load(f)

    parse_datetime = lambda dt_str: datetime.strptime(
        dt_str[:-4], "%Y-%m-%dT%H:%M:%S.%f"
    )

    block_time_raw = OrderedDict(
        {parse_datetime(v): int(k) for k, v in stats["block_time"].items()}
    )

    block_time = []
    last_t, max_r = next(iter(block_time_raw.items()))
    reordered = 0
    for t, r in [(k, v) for k, v in block_time_raw.items()][1:]:
        # ignore the block if reordered: a later block is received first
        if r < max_r:
            reordered += 1
            continue
        block_time.append((t - last_t) / timedelta(milliseconds=1))
        last_t = t
        max_r = r

    print("reordered ", reordered)
    plt.hist(block_time)
    plt.title(f"block time histogram total {len(block_time) - reordered}")
    plt.ylabel("count")
    plt.xlabel("block time (ms)")
    plt.savefig("output.png")
