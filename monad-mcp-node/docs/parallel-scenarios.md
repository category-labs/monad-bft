# Parallel scenario runs

The scenario harness (`tests/scenarios`) can run a swarm on the parallel
`monad-sim` engine (see `monad-sim/docs/parallel.md` and
`monad-sim-swarm/docs/parallel.md`). Results are the same for every thread
count and match the sequential build: `determinism.rs` checks the run digest
(every delivered message with its time, and every finalized slot) sequentially
and on 1, 2 and 4 threads, and `geo.rs` checks it with one domain per node and
with one domain per region.

## Using it

- `harness::start_swarm(seed, network, nodes, parallel)` builds the swarm and
  starts every node; `parallel` is `None` (sequential) or a
  `monad_sim_swarm::Parallel` (threads, grouping, placement).
- `MCP_SIM_THREADS=n` runs the scenarios built with
  `start_swarm(.., parallel_from_env())` (loss, mute) on `n` threads.
- `harness::geo(table, n, seed)` places `n` validators over a `RegionTable`
  (seeded `Placer`, 50–250 µs last mile, up to 1 ms jitter, 1 Gbit/s uplinks)
  and returns the network and each node's region; `harness::by_region` turns
  that into one domain per region. `harness::testnet_table()` is the 8-host
  testnet matrix (`deploy/config/latency.toml`, p50, with a 2 ms diagonal).
- `SourceKind::Fixed` makes every proposal exactly `max_payload_bytes` of
  random bytes, a fixed load; `Random` is log-uniform up to the limit (mean
  about 75 KB at 1 MiB).

The parallel engine needs a positive lower bound on every link between
domains. `Network::reliable` and `Network::with_matrix` have one; a sampled
latency needs `Network::min_latency`.

## Measurements

`tests/scenarios/bench.rs` (ignored; run with `--release -- --ignored
--nocapture`). Measured 2026-10-30 on a 14-core Apple M-series machine (10
performance cores), the three versions run back to back and alternating:

- *keccak*: the earlier `lars/csim/seeded-randomness` (keccak stub digests);
- *rapidhash*: `lars/csim/seeded-randomness` `b929023f7` (rapidhash stub
  digests);
- *containers*: `lars/csim/cadence-containers` `2030de1c8` (container types,
  `ValidatorIndex`/`NodeIndex` indexing, rapidhash lookup maps). Its revision
  `05685cd88`, the base of this branch, uses std's SipHash maps for lookups
  instead; it gives the same runs (identical digests and step counts) at the
  same speed, within 2% (measured 2026-11-02).

The containers version gives the same runs as rapidhash (identical digests and
step counts); keccak and rapidhash do the same work (identical step counts at
small payloads, within 0.03% at 400 KB).

These tables were measured with 32 groups. The default is now 128 (see
[Group count](#group-count)).

**Scaling** (`bench::scaling`): `reliable(10 ms).loss(0.05)`, payloads up to
2000 B, 20 slots, one domain per node. `W / D` is the sequential run's
available parallelism; with a stack per node it grows with `n`. Seconds,
sequential and on 10 threads:

| n | steps | W / D | keccak | rapidhash | containers | containers, speedup on 10 threads |
|---|---|---|---|---|---|---|
| 64 | 1.17 M | 64.6 | 10.7 / 1.47 | 4.37 / 0.69 | 3.27 / 0.49 | 6.4× |
| 128 | 4.64 M | 127.4 | 75.8 / 9.77 | 27.0 / 3.79 | 18.4 / 2.50 | 6.8× |

**Slot cost** (`bench::slot_cost`): every proposal exactly 400 KB, `geo`
placement, 5 slots, one domain per node. Seconds per simulated slot,
sequential and on 10 threads:

| n | table | steps per slot | keccak | rapidhash | containers |
|---|---|---|---|---|---|
| 64 | testnet | 370k | 1.50 / 0.34 | 0.93 / 0.25 | 0.84 / 0.24 |
| 64 | public internet | 366k | 1.50 / 0.35 | 0.93 / 0.28 | 0.83 / 0.26 |
| 190 | testnet | 1.46 M | 19.2 / 3.40 | 8.02 / 1.40 | 5.27 / 1.00 |
| 190 | public internet | 1.44 M | 19.1 / 2.98 | 8.03 / 1.26 | 5.19 / 0.92 |

Against keccak, the containers version is 3.2–4.1 times faster on large runs
(`n` of 128 and up), sequentially and in parallel. The parallel speedup is
5.3–6.8× at `n` of 128 and up; at `n = 64` with 400 KB proposals, steps are now
cheap enough that the fixed cost of each window caps it at 3.2–3.5×. At
`n = 190`, a 495-slot run takes about 8 minutes on 10 threads, against 43
minutes sequentially.

One domain per node (the default) beats one per region. Measured on the
previous version: region domains give wide windows (3.5 ms on the testnet
table, 6.6 ms on the public-internet one) but only 8–9 groups, and the regions
are unevenly weighted; at `n = 190` they took 6.0–6.7 s per slot against
3.0–3.4 s with a domain per node. With a domain per node, automatic grouping
forms balanced groups at about 1.1 ms windows.

## Group count

Groups are the parallel engine's tasks; with many more groups than threads,
work stealing evens out busy nodes. `BENCH_GROUPS` sets the target count
(capped at the number of domains). At `n = 190`, 400 KB proposals, 10 threads,
containers base, seconds per simulated slot:

| groups | 16 | 32 | 64 | 128 | 190 |
|---|---|---|---|---|---|
| testnet | 1.23 | 1.01–1.03 | 0.85 | 0.81–0.83 | 0.82 |
| public internet | 1.10 | 0.94–0.95 | 0.88 | 0.83–0.84 | 0.83 |

The window barely narrows (1.18 ms at 32 groups, 1.11 ms at 190). With the
default raised from 32 to 128, the small-payload scaling bench at `n = 128`
takes 2.20–2.30 s instead of 2.63 s on 10 threads, and runs at `n = 64` are
unchanged within noise (they have at most 64 domains). A profile in the
middle of a run with 32 groups showed the workers idle 35–40% of the time and
the serial exchange between windows at 1.6%.

At `n = 190`, a 495-slot run now takes about 7 minutes on 10 threads.
