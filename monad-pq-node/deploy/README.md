# monad-pq-node deployment (mcp_test, 8 validators)

Bash tooling to build `monad-pq-node` (Quantum Cadence) locally and operate it on the eight
`mcp_test` validators over ssh. It is a copy of `monad-mcp-node/deploy/` with the names
changed: user units `monad-pq-{node,rpc,explorer}` in `monad-pq.slice`, everything else in
`~/monad-pq/`. The rpc and explorer are the unchanged `monad-mcp-rpc` and `monad-mcp-explorer`
builds, and `mcp-tx` ships alongside for the tx probe. Design: `docs/pq-cadence.md` §8.3, §13.

The pq node binds the same `8002/udp` as `monad-mcp-node`, so the two networks never run at
the same time: `netctl.sh takeover` stops the mcp stack and starts pq, `netctl.sh handback`
stops pq and restarts mcp on its own config, genesis and ledger.

## Workflow

```
./build.sh [--dirty]               # dist/{monad-pq-node,monad-mcp-rpc,monad-mcp-explorer,mcp-tx}-<sha>, dist/VERSION
./preflight.sh --no-udp            # read-only; 8002 held by mcp only warns
./deploy.sh [host...]              # binaries, units, pruner into ~/monad-pq; mcp keeps running
./netctl.sh takeover --no-start    # stop monad-mcp-{explorer,rpc,node}, record them, wait for 8002
./preflight.sh                     # now with the udp pair probe on the freed port
./netctl.sh start                  # new genesis, push configs, start all 8 + rpc + explorers
./netctl.sh status
./report.sh                        # tips, lag, last-minute counts, latency by path, halted
./report.sh --logs <dir>           # + per-host counts and cross-host agreement
./report.sh --tx 200               # + 200 txs through the pq rpc on ewr-002, submit -> finalized
./netctl.sh handback               # stop pq, restart the mcp units that takeover stopped
```

`./netctl.sh takeover [start flags]` without `--no-start` does both steps at once
(`takeover -- --delta 200` passes flags through to `start` and on to `gen-config.sh`).

Upgrading a running pq network without a new genesis, and changing one host's config, work as
in mcp:

```
./netctl.sh live-upgrade [--hosts a,b] [--no-build] [--dirty] [-- gen-config.sh flags]
$EDITOR config/ams-001/node.toml && ./push-config.sh ams-001 && ./netctl.sh run-one ams-001 restart
```

## Takeover and handback

- **`takeover`** first checks every host has the pq binaries and units (`deploy.sh` ran), so
  it never stops mcp for nothing. Then, per host, it records which of
  `monad-mcp-node`/`monad-mcp-rpc`/`monad-mcp-explorer` were active into
  `dist/takeover/<host>.units`, stops them (explorer, rpc, node), waits for the mcp process to
  exit and `8002/udp` to be free, and fails the host otherwise. A re-run keeps the first record,
  so running it twice cannot make handback forget what to restore. Nothing under `~/monad-mcp`
  is touched, and the mcp units stay enabled.
- **`handback`** stops `monad-pq-{explorer,rpc,node}`, then per host starts the recorded mcp
  units in order: `systemctl --user start monad-mcp-node` (refused while 8002 is still bound),
  waits for its `udp bound` line, then restarts `monad-mcp-rpc` and `monad-mcp-explorer`. This
  is what mcp's own `netctl.sh run-one <host> start` does: no genesis is minted, no config is
  pushed and the ledger is kept, so the mcp nodes rejoin their old chain at the current clock
  slot. With no record (e.g. a fresh checkout) it starts every *enabled* mcp unit instead,
  which matches mcp's `deploy.sh`. A host's record is deleted once its units are back; check
  with `monad-mcp-node/deploy/report.sh`.
- Neither touches `monad-bft`/`monad-execution` (system units `monad` cannot manage anyway) or
  the mcp cruft timer, which skips a ledger with no writes in 20 minutes.
- `deploy.sh` installs but does **not enable** the pq node, rpc and explorer units (only the pq
  cruft timer). A reboot or user-manager restart therefore brings back the enabled mcp stack,
  never two nodes racing for 8002. Every pq start goes through `netctl.sh`.

## Files

| file | role |
|---|---|
| `hosts.txt` | the validator set, identical to mcp's; **line index is the `node_id`** |
| `lib.sh` | constants (units, `~/monad-pq`, ports) and the ssh/rsync/fan-out helpers |
| `build.sh` | release build of the four binaries; refuses a dirty `monad-pq-*`/`monad-mcp-*` tree without `--dirty` |
| `config/<host>/` | tracked per-host config: `node.toml`, plus `explorer.env` on `ewr-002` and `sgp-008` |
| `config/latency.toml` | RTT matrix (copied from mcp's, same hosts and ips); `latency.sh` re-measures it |
| `gen-config.sh` | renders `config/<host>/node.toml`; `--force` overwrites, `--only-genesis` rewrites just `genesis_deadline` |
| `preflight.sh [--no-udp]` | read-only fitness check + pairwise UDP probe |
| `deploy.sh [--stage] [host...]` | ships binaries, `cruft.sh`, `run.sh`, units; `--stage` leaves the `bin/` links alone |
| `push-config.sh [host...]` | `config/<host>/` → `~/monad-pq/config/`, printing the `node.toml` diff |
| `latency.sh`, `check-latency.py` | measure and validate the RTT matrix |
| `netctl.sh` | `start`/`stop`/`restart`/`status`/`logs`/`upgrade`/`live-upgrade`/`run-one`/`takeover`/`handback` |
| `report.sh` | state/tip/lag/clock, latency by path, halted, `--logs` agreement, `--tx` probe |
| `cruft.sh`, `cruft.env`, `run.sh`, `monad-pq-*.service`, `monad-pq-cruft.timer`, `monad-pq.slice` | pushed to the hosts |

`dist/` is generated and gitignored.

## Config (`gen-config.sh`)

Defaults are the mcp_test row of `docs/pq-cadence.md` §8.3:

| key | value |
|---|---|
| `[cadence] delta / slot_interval / slots_per_window / sync_boundary_slots / lag_threshold` | 150 / 100 / 100 / 80 / 20 |
| `[proposal] source / num_proposals` | `mempool` / 5 |
| `[proposal] propose_before_deadline / withhold_before_deadline` | 150 / 140 (L − 10) |
| `[repeater] interval / certificate_retention` | 500 / 50 (retention ≥ lag_threshold) |
| `[quantum] retransmit_interval / decode_grace` | 150 (= delta) / 75 (= delta / 2, the maximum) |
| `[quantum] coda_timeout_deltas / release_chunks / early_vote / ledger_proof / evidence` | 5 / `vote` / true / `path-votes` / true |
| `[quantum] signature` | `stub` (`--signature ml-dsa-44`: real ML-DSA-44 votes with keys derived from node ids, test networks only) |
| `[leader_election] rotation_slack` | 95 |
| `[ledger] dir` | `/home/monad/monad-pq/ledger` (evidence under `ledger/evidence`) |
| `[rpc] latency` | `latency.toml`, when `config/latency.toml` matches the validators |

`withhold`, `retransmit` and `decode_grace` follow `--propose-before` and `--delta` unless given.
`gen-config.sh` refuses `decode_grace > delta / 2`, `certificate_retention < lag_threshold` and
`withhold > propose`, which the node would refuse at startup. `--source random` drops the `[rpc]`
table, since the node requires `mempool` when one is present.

## Things worth knowing

- **Ports.** UDP `8002` (`PQ_PORT` overrides; 8000 is dropped by nftables for short datagrams).
  The pq rpc listens on `127.0.0.1:8090` (`lib.sh`'s `rpc_http_port` mirrors it for
  `report.sh --tx`). The pq explorer listens on `127.0.0.1:8081`, the same port as mcp's, because
  the sgp-008 nginx demo proxy (`/` → `127.0.0.1:8081`) targets it; the mcp and pq explorers
  therefore can't run on one host at the same time. Explorer tunnel:
  `ssh -N -L 8081:127.0.0.1:8081 -p 9022 monad@ewr-002.devcore4.com`, then
  `http://localhost:8081`. Rpc: `curl -s 127.0.0.1:8090/health` on a host.
- **`config/<host>/` is the source of truth**, as in mcp: edit locally and push. `netctl.sh
  start` keeps local edits and only rewrites `genesis_deadline`; flags after `--` re-render
  every host. Every whole-network `start`, `restart`, `upgrade` (and `takeover` without
  `--no-start`) mints a new genesis and wipes `~/monad-pq/ledger/blocks`.
- **`report.sh`** reads one source host (`--source`, default `ewr-002`). It splits the latest
  `--slots` finalizations by the node's `finalized slot=N block=… at=<ns> path=Fast|Coda|Gadget`
  line, Coda slots further by `coda decided slot=N view=V`; `coda entry p50` comes from
  `entering fallback coda` and `halted` from `halted slot=N at=<ns>`. The `coda`/`view` lines
  are debug level, which the unit's `RUST_LOG=info,monad_pq_chorus=debug` keeps. `--tx n`
  runs `~/monad-pq/bin/mcp-tx send --rpc http://127.0.0.1:8090 --count n --wait …` on
  `--tx-host` (default the first host with `explorer.env`) and reports `finalized_at − sent_at`
  per ledger path (the rpc reports Coda and Gadget as `Fallback`; §5.3); both clocks are that
  host's. Acceptance (§14.6): fast-path p99 < 600 ms, Coda view-1 p99 < 1 s.
- **`--logs <dir>`** adds per-host finalized/fast/coda/gadget/halted/evidence counts and the
  per-slot block agreement (every host that finalized a slot logged the same `+`/`-` shape).
- **Build host glibc must match the hosts**: `preflight.sh` fails a host whose glibc differs
  from the local one, as in mcp.
- The rest (`MCP_SUPERVISOR` → `PQ_SUPERVISOR=setsid` fallback, stub crypto on an
  Internet-exposed port, the ssh/`XDG_RUNTIME_DIR` notes, linger, ledger pruning) is as in
  `monad-mcp-node/deploy/README.md` with `~/monad-pq` and `monad-pq-*` in place of the mcp names.
