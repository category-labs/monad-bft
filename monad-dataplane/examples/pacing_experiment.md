Run from the repository root:

```bash
cargo build --release -p monad-dataplane --example throughput
python3 monad-dataplane/examples/pacing_experiment.py \
  --output reports/tcp-pacing --duration 61
```

The Python script requires numpy and matplotlib. It starts ten TCP stream readers,
one native dataplane UDP reader, and one dataplane sender on IPv4 loopback. The
sender keeps two 2 MiB messages outstanding per TCP receiver at regular priority.
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

To compare equal priority, repeat the experiment with `--udp-priority regular`.
TCP remains at regular priority. The output config records the selected priority;
`source.patch` records any uncommitted code used for the run.

Expect TCP to fill idle bandwidth, pause during each UDP burst, then recover.
Bytes already granted to a TCP writer or buffered in the kernel can still arrive
at the beginning of a UDP burst. A 50 MiB UDP burst should occupy roughly 430 ms
at this rate with 1200-byte UDP payloads. Short measurement windows may exceed the
cap because of bounded pacing bursts, scheduling, and counter sampling.

The throughput tool's `tcp-writer`, `tcp-reader`, and `reader` commands can also be
run independently; use `--help` for their options. `tcp-reader` is a stream sink,
so it counts bytes without decoding dataplane frames. It is not a protocol reader.
