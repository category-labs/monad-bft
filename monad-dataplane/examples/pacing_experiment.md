Run from the repository root:

```bash
cargo build --release -p monad-dataplane --example throughput
python3 monad-dataplane/examples/pacing_experiment.py \
  --output reports/tcp-pacing --duration 61
```

The Python script requires numpy and matplotlib. It starts ten TCP stream readers,
one native dataplane UDP reader, and one dataplane sender on IPv4 loopback. The
sender keeps two 2 MiB messages outstanding per TCP receiver at background priority.
It submits a 50 MiB high-priority UDP burst every two seconds through the same
dataplane. Both protocols share the 1 Gbps global cap and per-IP pacing. All
loopback receivers use the same IP, so their ports share one peer budget.
The first burst starts after two seconds of TCP traffic. Running for 61 seconds
allows the thirtieth burst to finish before stopping the processes.

The output directory must be new. It contains raw CSV samples and process logs,
the exact commands and configuration, a JSON summary, and PNG/SVG plots:

- `throughput`: aggregate TCP/UDP rates and each TCP receiver's rate in 50 ms windows.
- `burst-detail`: the first UDP burst in 10 ms windows.

TCP is measured by bytes read from the stream, including the 16-byte frame header.
This avoids the whole-message steps in the sender's TCP completion counter. UDP
uses the sender's successful payload byte counter; the UDP receiver is sampled
separately to check delivery. IP, TCP and UDP network headers are excluded from the
plots. UDP pacing charges IP/UDP headers, so its payload rate is slightly below the
configured cap. These are application counter measurements on loopback, not packet
captures or a physical-link benchmark.

Samples use wall-clock timestamps on the same host. Rates interpolate cumulative
counters onto common window boundaries. The summary excludes the first 500 ms from
rate averages. It reports TCP throughput while UDP is idle and during burst windows
where UDP exceeds 80% of the cap. The burst-interior statistic also excludes the
first and last such window. Raw samples are retained; changing the plotting window
does not require rerunning traffic:

```bash
python3 monad-dataplane/examples/pacing_experiment.py \
  --output reports/tcp-pacing --plot-only --window-ms 100
```

To compare equal priority, repeat the experiment with
`--udp-priority regular --tcp-priority regular`.
The output config records the selected priorities;
`source.patch` records any uncommitted code used for the run.

Expect TCP to fill idle bandwidth, pause during each UDP burst, then recover.
Bytes already granted to a TCP writer or buffered in the kernel can still arrive
at the beginning of a UDP burst. A 50 MiB UDP burst should occupy roughly 430 ms
at this rate with 1200-byte UDP payloads. Short measurement windows may exceed the
cap because of bounded pacing bursts, scheduling, and counter sampling.

The throughput tool's `tcp-writer`, `tcp-reader`, and `reader` commands can also be
run independently; use `--help` for their options. `tcp-reader` is a stream sink,
so it counts bytes without decoding dataplane frames. It is not a protocol reader.

## Periodic large TCP obligations

To offer 100 MiB of TCP response payload per second alongside approximately
20 MiB of UDP every 330 ms under one 2 Gbps cap:

```bash
RUST_LOG=off python3 monad-dataplane/examples/pacing_experiment.py \
  --output reports/tcp-pacing-periodic --duration 34 \
  --bandwidth-mbps 2000 --receivers 1 --message-size 1048576 \
  --tcp-burst-bytes 104857600 --tcp-burst-interval-ms 1000 \
  --burst-bytes 20971520 --burst-interval-ms 330
```

`--tcp-burst-bytes` is an aggregate logical obligation, split across TCP targets.
It is streamed as legal-sized frames controlled by `--message-size` and
`--in-flight-per-receiver`; it is not a single oversized TCP frame. Each target
has an independent producer. A slow target accumulates schedule lateness while
retaining bounded outstanding payload. TCP completion log records include burst
number, bytes, duration and schedule lateness. `--tcp-priority high` permits an
equal-priority control; TCP defaults to background, below both UDP priorities.
Omitting `--tcp-burst-bytes`
retains continuous TCP traffic.

The UDP traffic in this example is synthetic. Testing receive, validation,
WireAuth and Raptorcast fanout together requires the separate real-rebroadcast
harness. Pacing accounts for UDP IP/UDP headers and TCP framing, but excludes TCP/IP
network headers. Short counter windows can also move in whole-frame steps.
