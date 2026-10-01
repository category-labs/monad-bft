# monad-mcp-node deployment (mcp_test, 8 validators)

Bash tooling to build `monad-mcp-node` locally and operate it on the eight `mcp_test`
validators over ssh. No ansible, no root: the node runs under a user-level systemd unit in
`~/.config/systemd/user/`, everything else lives in `~/monad-mcp/`.

## First run

```
./build.sh                  # dist/monad-mcp-node-<sha>, dist/VERSION
./preflight.sh              # read-only; never changes a host
./deploy.sh [host...]       # binary, units, pruner; does NOT start the node
./netctl.sh start           # mints a genesis, pushes configs, starts all 8
./netctl.sh status
./report.sh                 # safe at any time: tips, lag, latency by path
```

Upgrading a running network without a new genesis:

```
./netctl.sh live-upgrade                      # build, stage on all, then one host at a time
./netctl.sh live-upgrade --hosts ams-001,vin-002 --no-build
./netctl.sh live-upgrade -- --delta 200       # also re-render configs on the live genesis
```

Changing one host's config by hand:

```
$EDITOR config/ams-001/node.toml
./push-config.sh ams-001                      # prints the remote -> local diff
./netctl.sh run-one ams-001 restart
```

`preflight.sh` fails any host where 8002/udp is still bound, which today means the four
hosts still running `monad-bft`: `deu-011`, `ltu-010`, `sgp-008`, `swe-001`. Freeing them
needs infra — user `monad` cannot stop `monad-bft` itself, see "No permission to stop
monad-bft" below. Until then, run a reduced `hosts.txt` over the idle hosts (`ams-001`,
`ewr-002`, `lax-001`, `vin-002`); note that changes the network identity.

## Files

| file | role |
|---|---|
| `hosts.txt` | the validator set; **line index is the `node_id`** |
| `lib.sh` | ssh/rsync/fan-out helpers, sourced by every script |
| `build.sh` | release build → `dist/monad-mcp-{node,rpc,explorer}-<sha>[-dirty]` + `dist/VERSION` |
| `config/<host>/` | tracked per-host config, `node.toml` plus anything else to ship; mirrors `~/monad-mcp/config/` |
| `gen-config.sh` | renders `config/<host>/node.toml` for one shared genesis; `--force` to overwrite, `--only-genesis` rewrites just `genesis_deadline` |
| `preflight.sh` | read-only fitness check + pairwise UDP probe |
| `deploy.sh [--stage] [host...]` | ships node + rpc binaries, `cruft.sh`, `run.sh` and the user units; enables them; only the hosts given; `--stage` leaves `bin/{current,rpc-current}` alone |
| `push-config.sh [host...]` | `config/<host>/` → `~/monad-mcp/config/`; prints the `node.toml` diff, keeps a changed one as `node.toml.<ts>~` |
| `netctl.sh` | `start`/`stop`/`restart`/`status`/`logs`/`upgrade`/`live-upgrade`/`run-one` |
| `report.sh` | per-host state/tip/lag/clock offset, finalization latency by path; `--logs <dir>` adds per-host counts, per-slot block agreement, top warnings |
| `cruft.sh` | ledger pruner, runs on the host from `monad-mcp-cruft.timer` |
| `monad-mcp-node.service`, `monad-mcp-rpc.service`, `monad-mcp-cruft.{service,timer}`, `cruft.env`, `run.sh` | pushed to the hosts |
| `monad-mcp-explorer.service` | pushed only to hosts with `config/<host>/explorer.env` |

`dist/` is generated and gitignored. `cruft.sh` and `run.sh` run *on the validator*, so they
are the two scripts here that do not source `lib.sh`.

## Things worth knowing

- **`config/<host>/` is the source of truth for host config.** Edit it locally and push; never
  edit `~/monad-mcp/config/` on a host, the next push overwrites it. `push-config.sh` refuses a
  `node.toml` whose `node_id` is not the host's. `netctl.sh start` keeps local edits and only
  rewrites `genesis_deadline`; flags after `--` (`-- --force` for defaults) re-render every
  host from scratch, discarding edits (`git diff config/` shows what went). Re-render after
  changing `hosts.txt`: the validator set is baked into every file. `build.sh`'s dirty check
  ignores `config/`, since every start changes it.

- **`hosts.txt` order is the network identity.** The line index becomes `node_id`,
  `cadence_key_pair`, `proposal_key_pair` and `chorus_pubkey` (the stub env derives every key
  from that one u64). Reordering or inserting a host renumbers the validator set and needs a
  new genesis.
- **Every whole-network `start`, `restart` and `upgrade` mints a new genesis** (`now + lead`,
  default 60 s) and wipes `~/monad-mcp/ledger/`; `--keep-ledger` archives it to
  `ledger/blocks-<ts>` instead. The node keeps no state, so this is the normal way to change a
  cadence parameter: flags after `--` go to `gen-config.sh`, e.g. `./netctl.sh restart --
  --delta 200 --slot-interval 120`. `netctl.sh run-one <host> start|stop|restart` reuses the config
  already on the host, for crash/catch-up testing of a single node.
- **`live-upgrade` keeps the genesis and the ledger.** It reads `genesis_deadline` and
  `slot_interval` from the first host's `node.toml`, checks every host is finalizing, builds
  (skip with `--no-build`, `--dirty` passes through), stages the binary with `deploy.sh --stage`,
  then per host, in `hosts.txt` order: re-checks the whole network, stops the node, swaps
  `bin/current`, starts it, and waits up to `--timeout` (120 s) for at least 10 `finalized`
  lines since its `udp bound` with a tip within `--max-lag` (20) slots of the clock slot. A
  node that dies or misses the gate stops the rollout; the hosts after it keep the old
  `bin/current`. Each host gets its `config/<host>/` pushed just before its restart (a no-op if
  unchanged); every upgraded host's local `genesis_deadline` must equal the live one. Flags after
  `--` first re-render every config on the live genesis (`gen-config.sh --keep-genesis --force`,
  discarding local edits), so parameters differ across the network mid-rollout. `--hosts a,b`
  upgrades only those hosts. Each host's rpc is swapped with its node and started after the
  node passes the gate; an rpc that is not active 2 s later also stops the rollout.
- **`monad-mcp-rpc` runs beside every node** as the user unit `monad-mcp-rpc`
  (`bin/rpc-current --node-config ~/monad-mcp/config/node.toml`, `Restart=on-failure`). It
  reads the node's config and ledger, so `netctl.sh` stops it before its node and restarts it
  after (`start`, `run-one`, `live-upgrade`); `status` shows its state. HTTP is
  `127.0.0.1:8080` only (`/health`, `POST /tx`, `GET /tx/{hash}`): tunnel with
  `ssh -L 8080:127.0.0.1:8080`. Txs are proposed only with `gen-config.sh --source mempool`
  (`[proposal] source`); the default `random` keeps the load-test payloads and leaves rpc
  txs pending forever. No rpc under `MCP_SUPERVISOR=setsid`.
- **`monad-mcp-explorer` runs only where `config/<host>/explorer.env` exists** (today
  `ewr-002` and `sgp-008`). That file sets `EXPLORER_ARGS` (`--ledger-dir`, plus any flag overrides) and
  reaches the host with the rest of `config/<host>/`; `deploy.sh`, `netctl.sh` and `status`
  skip the explorer elsewhere. It indexes the local ledger in memory, so `netctl.sh` restarts
  it after its node and rpc (a new genesis wipes the ledger). HTTP is `127.0.0.1:8081` only;
  the page's send panel posts to the explorer's `/api/tx`, which forwards to the local rpc on
  8080, so one tunnel is enough: `ssh -N -L 8081:127.0.0.1:8081 -p 9022 monad@ewr-002.devcore4.com`,
  then open `http://localhost:8081`. Adding a host needs `explorer.env`, `push-config.sh
  <host>`, `deploy.sh <host>` and `netctl.sh run-one <host> restart` (or just
  `systemctl --user start monad-mcp-explorer` there); removing the file stops shipping it but
  does not uninstall it.
- **Always ssh to `<host>.devcore4.com`.** `~/.ssh/config` here has no `ewr-*`/`lax-*`
  short-name pattern; the FQDN matches `Host *.devcore4.com` (user `monad`, port 9022).
  `lib.sh` spells user and port out anyway.
- **Port 8002/udp.** 8000 is unusable: the nftables table `monad_mitigations` drops UDP to
  8000 with length 0-1400, which is every Cadence vote. `MCP_PORT` overrides it, 8001 being
  the only other open port.
- **`MCP_SUPERVISOR=setsid`** switches every script to the no-systemd fallback: `deploy.sh`
  skips `enable-linger`/`daemon-reload`/`enable`, `netctl.sh` launches
  `~/monad-mcp/run.sh` (`setsid` + `run/pid`), stops it with `kill -INT`, and `live-upgrade`
  reads `~/monad-mcp/logs/node.log`. `report.sh` needs the journal and refuses setsid.
  The default is `systemd`.
- **`preflight.sh` distinguishes FAIL from WARN.** Hard failures (non-zero exit): glibc
  mismatch, no AVX2, 8002 bound, `NTPSynchronized != yes`, no usable chrony source within 50 ms
  (`?`/`x` rows do not count), no user systemd manager, less than `MIN_FREE_GB` free,
  unreachable host. Warnings only:
  `Linger=no`, missing `~/monad-mcp` layout, inactive cruft timer, monad-bft/monad-execution
  still active — `deploy.sh` fixes the first three and it runs after the first preflight. The
  UDP pair probe is skipped while 8002 is still bound somewhere.
- **No permission to stop `monad-bft`.** `pkcheck` reports
  `org.freedesktop.systemd1.manage-units` as `auth_admin` for user `monad` on all eight hosts,
  and no monad polkit rule is installed, so `systemctl stop monad-bft monad-execution` fails
  with "Access denied ... requires interactive authentication" (verified 2026-09-17). Ask infra
  to stop the units, install the rule, or ship a system `monad-mcp-node.service`. This does not
  affect the node itself: `systemctl --user` needs no polkit and `set-self-linger` is
  authorized.
- **Ledger pruning** is a user timer: `monad-mcp-cruft.timer` runs `~/monad-mcp/cruft.sh`
  every 10 minutes, keeping the newest `RETENTION_BLOCKS` (default 1,000,000) files under
  `~/monad-mcp/ledger/blocks` and deleting the oldest in batches while free space is under
  `MIN_FREE_GB` (default 200). Knobs live in `~/monad-mcp/cruft.env`, which `deploy.sh`
  installs only if it is absent. The node writes finalized blocks there: `gen-config.sh`
  sets the required `[ledger] dir` to `~/monad-mcp/ledger`.
- **`build.sh` keeps the `MCP_KEEP_BINARIES` (default 3) newest binaries in `dist/`**; older shas are a rebuild away.
- **The node has no `--version` flag**, so the sha in the binary name plus `dist/VERSION` is
  the version, and `build.sh` refuses to build a dirty `monad-mcp-*` tree unless given
  `--dirty` (which stamps `-dirty` into the name). Health is the `udp bound` line plus
  `finalized` lines in the journal.
- **`report.sh` reads one source host** (`--source`, default `ewr-002`) for genesis, slot
  interval and the latency table, which splits slots by the `mvba decided ... FallbackView(n)`
  line into fast path and MVBA views over the latest `--slots` (10k) finalizations. Every host
  contributes its unit state, tip, lag and clock offset. `--logs <dir>` pulls every journal over
  the same window (`--since` widens it) for the cross-host agreement check. With delta 150 ms
  against RTTs of 154-240 ms, the far pairs are expected to finalize on the fallback path.
- **Stub crypto on an Internet-exposed port.** The UDP transport trusts the sender id in the
  frame and every key derives from a small integer; frames from outside the validator set are
  dropped, and that is the whole authentication story. This is a test network — do not put
  anything of value behind it.
- `systemctl --user` over a non-interactive ssh needs `XDG_RUNTIME_DIR` and
  `DBUS_SESSION_BUS_ADDRESS` (`lib.sh`'s `remote_env`); `systemctl disable` of `monad-bft` is
  not possible as `monad` (polkit allows start/stop only), so a reboot or the Jenkins
  `mcp_test` reset job will bring the legacy stack back and re-bind the ports.
