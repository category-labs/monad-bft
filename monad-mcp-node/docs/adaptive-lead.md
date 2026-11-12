# The adaptive proposal lead in simulations

The adaptive proposal lead (`docs/0005_adaptive_proposal_lead.md`) is
experimental and off by default. A node turns it on with
`[proposal.adaptive] enabled = true` (see `config.example.toml`); the static
`propose_before_deadline` then stays the cold-start lead. This page says how
to run it in the scenario harness. The design, the parameters and the
results are in `docs/0005`.

## What the node does

- **Learns.** Every peer's batch vote reaches the planner with its arrival
  time (`ProposingInput::BatchVote`); on own slots, the vote on the own lane
  calibrates the model (`monad_mcp_cadence::adaptive_lead::LeadModel`).
- **Plans.** After 20 slots with arrivals, a seal is due at `D − L*`, with
  `L*` the shortest lead whose predicted negative stake on the own lane stays
  below `θ` with risk `ε`. Pending seals are re-planned on cap advances and
  on own-slot votes.
- **Sends** the first hop in the model's `target_far` order (DA's
  `send_order`), and **withholds** a late seal only when sending it risks a
  split.
- **Reports** each warm own slot as one log line, `adaptive lead: own slot`
  (info level): the lead planned and used, `M`, `√V` and the bound at the
  lead used, the margin `ŝ`, the core-set stake, the realized no, yes and
  missing stake, and the path.

The planner assumes the first hop does not queue behind other traffic on the
uplink. In the harness, `harness::first_hop_priority(network)` models that
with the network's priority classes (`monad-sim-swarm`, `Network::priority`):
a first-hop share waits behind at most the one datagram already on the wire.
The stub transport does not prioritize.

## Scenarios

- `adaptive::an_adaptive_run_is_the_same_in_parallel` (default suite):
  n = 16; warm seals, and the same run sequentially and per region.
- `adaptive::converges` (ignored): n = 64, 256 KiB, 120 slots on the
  public-internet table; no fallback in steady state.

  ```
  cargo test --release -p monad-mcp-node --test scenarios \
      adaptive::converges -- --ignored --nocapture
  ```
- `validation::records_match_the_planner`, `validation::delayed_networks_finalize`
  (default suite, sequential): the plumbing of the long runs.
- `validation::long_run` (ignored): the long runs that compare the node with
  the model, below.

## Long runs

`validation::long_run` runs the adaptive lead on the public-internet table
(1 Gbit/s, first-hop priority, clock offsets) and exports, for a model to run
on the identical network:

- `<name>.topology.json`: node-level one-way latencies (last miles included),
  the jitter bound, stakes, clock offsets, uplink, lanes and timing, plus the
  run's parameters;
- `<name>.slots.jsonl`: one line per own slot: the planner's report (warm
  seals) joined with what the harness saw (lead, verdicts, path);
- `<name>.validators.jsonl`: per own slot, each validator's first-hop
  arrivals and vote, in ms from the slot's nominal deadline;
- `<name>.summary.txt`: one line with the steady-state counts.

It is set up with `VAL_*` variables (each field of `Setup` in
`tests/scenarios/validation.rs`, in upper case); the defaults are the
configuration of `docs/0005` §7.12 ("all on"): n = 64, 256 KiB, θ = 1/4,
ε = 10⁻³, 5 ms blocks, a margin over 1000 own slots skipping the first 8,
clock offsets σ = 4 ms with `clock_sd_ms` = 4, and re-planning on votes.

```
VAL_OUT=/tmp/val VAL_NAME=D256.s1 VAL_SEED=1 VAL_SLOTS=2000 MCP_SIM_THREADS=10 \
cargo test --release -p monad-mcp-node --test scenarios \
    validation::long_run -- --ignored --nocapture
```

Other knobs: `VAL_ADAPTIVE=0` (the static lead), `VAL_PRIORITY=0` (one FIFO),
`VAL_PAYLOAD`, `VAL_THETA`, `VAL_CLOCK_SIGMA_MS`, `VAL_CLOCK_SD_MS`,
`VAL_CALIB_SKIP`, `VAL_REFRESH_ON_VOTE`, region-shared delays
(`VAL_SHARED_MS`) and a one-direction latency step (`VAL_STEP_MS`,
`VAL_STEP_REGION`, `VAL_STEP_SLOT`).

`VAL_SCHEME=d33_hollow` runs with hollow chunks
([simulation-data.md](simulation-data.md)): the same messages and sizes,
without the data. A run is then statistically like the d33 run, not
identical: on the D256 configuration (2 seeds × 2000 slots) the leads per
proposer and the exceedances of θ agree with d33. At n = 64 and 256 KiB, a
run of 2000 slots on 10 threads of a 14-core M4 Max took about 7 minutes
and 21 GB with d33, under 3 minutes and 2 GB with hollow chunks; use hollow
chunks for n = 190 and for long runs.

The parallel runs log from worker threads, so `long_run` captures the
planner's reports with a global subscriber: run it alone.
