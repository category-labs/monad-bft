#!/usr/bin/env python3
"""Run the mixed TCP/UDP experiment and plot the measured counters.

Build first: cargo build --release -p monad-dataplane --example throughput
Requires numpy and matplotlib. All processes use IPv4 loopback.
"""

import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import time

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


def run(args):
    args.output.mkdir(parents=True, exist_ok=False)
    processes = []
    logs = []
    commands = []

    def start(name, arguments):
        log = (args.output / f"{name}.log").open("w")
        logs.append(log)
        command = [str(args.binary.resolve()), *map(str, arguments)]
        commands.append(command)
        process = subprocess.Popen(
            command, stdout=log, stderr=subprocess.STDOUT,
            env={**os.environ, "RUST_LOG": "info"},
        )
        processes.append(process)
        csv = args.output / f"{name}.csv"
        deadline = time.monotonic() + 10
        while not csv.exists() or csv.stat().st_size < 100:
            if process.poll() is not None or time.monotonic() > deadline:
                raise RuntimeError(f"{name} did not start; see {log.name}")
            time.sleep(0.01)
        return process

    def address(kind):
        with socket.socket(socket.AF_INET, kind) as reservation:
            reservation.bind(("127.0.0.1", 0))
            return f"127.0.0.1:{reservation.getsockname()[1]}"

    def sampling(name):
        return ["--samples-csv", args.output / f"{name}.csv",
                "--sample-interval-ms", args.sample_ms]

    try:
        targets = []
        for index in range(args.receivers):
            target = address(socket.SOCK_STREAM)
            start(f"tcp-{index + 1}", ["tcp-reader", "--bind-addr", target,
                                      *sampling(f"tcp-{index + 1}")])
            targets.append(target)
        udp_target = address(socket.SOCK_DGRAM)
        start("udp", ["reader", "--bind-addr", udp_target, *sampling("udp")])
        writer = start("writer", [
            "tcp-writer", ",".join(targets), "--db", args.bandwidth_mbps,
            "--message-size", args.message_size,
            "--udp-burst-target", udp_target,
            "--udp-burst-priority", args.udp_priority,
            "--udp-burst-bytes", args.burst_bytes,
            "--udp-burst-interval-ms", args.burst_interval_ms,
            *sampling("writer"),
        ])
        deadline = time.monotonic() + args.duration
        while time.monotonic() < deadline:
            if any(process.poll() is not None for process in processes):
                raise RuntimeError("an experiment process exited; inspect the logs")
            time.sleep(0.1)
        writer.terminate()
        writer.wait(timeout=5)
        time.sleep(0.25)  # let receiver samples include all bytes already sent
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for log in logs:
            log.close()
        (args.output / "commands.json").write_text(json.dumps(commands, indent=2) + "\n")
    config = {key: str(value) if isinstance(value, Path) else value
              for key, value in vars(args).items()}
    config["revision"] = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    diff = subprocess.check_output(["git", "diff", "HEAD"], text=True)
    config["working_tree_dirty"] = bool(diff)
    (args.output / "source.patch").write_text(diff)
    config["host"] = os.uname().nodename
    (args.output / "config.json").write_text(json.dumps(config, indent=2) + "\n")


def plot(output, window_ms):
    config = json.loads((output / "config.json").read_text())
    udp_priority = config.get("udp_priority", "high")

    def load(name):
        return np.atleast_1d(np.genfromtxt(output / f"{name}.csv", delimiter=",",
                                         names=True, dtype=np.uint64))

    writer = load("writer")
    udp_receiver = load("udp")
    receivers = [load(f"tcp-{index + 1}") for index in range(config["receivers"])]
    epoch = int(writer["unix_ns"][0])

    def timestamps(rows):
        return (rows["unix_ns"].astype(np.int64) - epoch) / 1e9

    end = min(timestamps(rows)[-1] for rows in [writer, *receivers])

    def rates(window):
        edges = np.arange(0, end, window)

        def rate(rows, key):
            counter = np.interp(edges, timestamps(rows), rows[key])
            return np.diff(counter) * 8 / np.diff(edges) / 1e6

        return (edges[1:], rate(writer, "udp_bytes"),
                np.array([rate(rows, "tcp_bytes") for rows in receivers]))

    t, udp, tcp_peers = rates(window_ms / 1000)
    tcp = tcp_peers.sum(axis=0)
    cap = config["bandwidth_mbps"]
    steady = t > 0.5
    # Select the interior of UDP bursts, excluding mixed boundary windows.
    active = steady & (udp > cap * 0.8)
    interior = active & np.roll(active, 1) & np.roll(active, -1)
    interior[[0, -1]] = False
    idle = steady & (udp < 1)
    deltas = np.diff(writer["udp_bytes"].astype(np.int64))
    sending = deltas > 0
    starts = np.flatnonzero(sending & ~np.r_[False, sending[:-1]])
    stops = np.flatnonzero(sending & ~np.r_[sending[1:], False]) + 1
    burst_windows = [{"start_s": float(timestamps(writer)[start]),
                      "duration_s": float(timestamps(writer)[stop] - timestamps(writer)[start]),
                      "bytes": int(deltas[start:stop].sum())}
                     for start, stop in zip(starts, stops)]

    def mean_or_none(values):
        return float(values.mean()) if values.size else None

    summary = {
        "duration_s": float(t[-1]),
        "sample_interval_ms": config["sample_ms"],
        "plot_window_ms": window_ms,
        "tcp_average_mbps": float(tcp[steady].mean()),
        "udp_average_mbps": float(udp[steady].mean()),
        "tcp_udp_idle_mbps": mean_or_none(tcp[idle]),
        "tcp_udp_active_mbps": mean_or_none(tcp[active]),
        "tcp_udp_burst_interior_mbps": mean_or_none(tcp[interior]),
        "udp_active_mbps": mean_or_none(udp[active]),
        "bursts": burst_windows,
        "tcp_per_receiver_average_mbps": [float(peer[steady].mean()) for peer in tcp_peers],
        "udp_sent_bytes": int(writer["udp_bytes"][-1]),
        "udp_received_bytes": int(udp_receiver["udp_bytes"][-1]),
        "sender_final_counters": {name: int(writer[name][-1]) for name in writer.dtype.names},
        "receiver_errors": sum(int(rows["tcp_errors"][-1]) for rows in receivers)
                           + int(udp_receiver["udp_errors"][-1]),
        "measurement": "TCP stream bytes read at receivers (includes 16-byte frame headers); "
                       "UDP payload bytes successfully sent; excludes IP/TCP/UDP headers",
    }
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")

    fig, axes = plt.subplots(2, 1, figsize=(16, 8), sharex=True, layout="constrained")
    axes[0].plot(t, tcp, label=f"TCP total ({summary['tcp_average_mbps']:.0f} Mbps avg)", linewidth=1)
    axes[0].plot(t, udp, label=f"UDP {udp_priority} ({summary['udp_average_mbps']:.0f} Mbps avg)", linewidth=1)
    axes[0].axhline(cap, color="black", linestyle="--", linewidth=0.8, label="Shared pacing limit")
    axes[0].set_title("TCP and UDP total throughput")
    axes[0].legend(loc="upper left", bbox_to_anchor=(1, 1))
    for index, peer in enumerate(tcp_peers):
        axes[1].plot(t, peer, linewidth=0.8,
                     label=f"tcp-{index + 1} ({peer[steady].mean():.1f} Mbps avg)")
    axes[1].set_title("TCP throughput per receiver")
    axes[1].legend(loc="upper left", bbox_to_anchor=(1, 1))
    axes[1].set_xlabel("Elapsed time (seconds)")
    for axis in axes:
        axis.set_ylabel("Throughput (Mbps)")
        axis.set_ylim(bottom=0)
        axis.grid(alpha=0.3)
    fig.suptitle(f"{cap} Mbps shared cap · {config['receivers']} TCP receivers · "
                 f"{config['burst_bytes'] / 2**20:g} MiB UDP every "
                 f"{config['burst_interval_ms'] / 1000:g} s\n"
                 f"{config['sample_ms']} ms samples, {window_ms:g} ms rate windows")
    for extension in ["png", "svg"]:
        fig.savefig(output / f"throughput.{extension}", dpi=180)
    plt.close(fig)

    fine_t, fine_udp, fine_tcp = rates(config["sample_ms"] / 1000)
    first_burst = config["burst_interval_ms"] / 1000
    fig, axis = plt.subplots(figsize=(12, 4), layout="constrained")
    axis.plot(fine_t, fine_tcp.sum(axis=0), label="TCP stream receive rate", linewidth=1)
    axis.plot(fine_t, fine_udp, label="UDP payload send rate", linewidth=1)
    axis.axhline(cap, color="black", linestyle="--", linewidth=0.8)
    axis.set(xlim=(first_burst - 0.1, first_burst + 0.7), ylim=(0, cap * 1.3),
             xlabel="Elapsed time (seconds)", ylabel="Throughput (Mbps)",
             title=f"First UDP burst — UDP {udp_priority}, TCP regular — {config['sample_ms']} ms windows")
    axis.grid(alpha=0.3)
    axis.legend()
    for extension in ["png", "svg"]:
        fig.savefig(output / f"burst-detail.{extension}", dpi=180)
    plt.close(fig)
    print(json.dumps(summary, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("target/release/examples/throughput"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--duration", type=float, default=61)
    parser.add_argument("--receivers", type=int, default=10)
    parser.add_argument("--bandwidth-mbps", type=int, default=1000)
    parser.add_argument("--message-size", type=int, default=2 * 1024 * 1024)
    parser.add_argument("--burst-bytes", type=int, default=50 * 1024 * 1024)
    parser.add_argument("--burst-interval-ms", type=int, default=2000)
    parser.add_argument("--udp-priority", choices=["high", "regular"], default="high")
    parser.add_argument("--sample-ms", type=int, default=10)
    parser.add_argument("--window-ms", type=float, default=50)
    parser.add_argument("--plot-only", action="store_true")
    args = parser.parse_args()
    if min(args.receivers, args.bandwidth_mbps, args.message_size, args.burst_bytes,
           args.burst_interval_ms, args.sample_ms, args.window_ms) <= 0:
        parser.error("counts, sizes, rates and intervals must be positive")
    if not args.plot_only:
        if args.duration <= args.burst_interval_ms / 1000 + 1:
            parser.error("duration must include at least one complete UDP burst")
        run(args)
    plot(args.output, args.window_ms)


if __name__ == "__main__":
    main()
