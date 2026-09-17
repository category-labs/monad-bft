# monad-mcp-node deployment (mcp_test, 8 validators)

Bash tooling to build `monad-mcp-node` locally and operate it on the eight `mcp_test`
validators over ssh. No ansible, no root: the node runs under a user-level systemd unit in
`~/.config/systemd/user/`, everything else lives in `~/monad-mcp/`.

## First run

```
./build.sh                  # dist/monad-mcp-node-<sha>, dist/VERSION
./preflight.sh              # read-only; never changes a host
./deploy.sh                 # binary, units, pruner; does NOT start the node
./netctl.sh start           # mints a genesis, pushes configs, starts all 8
./netctl.sh status
./report.sh                 # safe at any time; counts grow as slots finalize
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
| `build.sh` | release build → `dist/monad-mcp-node-<sha>[-dirty]` + `dist/VERSION` |
| `gen-config.sh` | renders `dist/config/<host>.toml` for one shared genesis |
| `preflight.sh` | read-only fitness check + pairwise UDP probe |
| `deploy.sh` | ships binary, `cruft.sh`, `run.sh` and the user units; enables them |
| `push-config.sh` | `dist/config/<host>.toml` → `~/monad-mcp/config/node.toml` |
| `netctl.sh` | `start`/`stop`/`restart`/`status`/`logs`/`upgrade`/`run-one` |
| `report.sh` | per-host finalized/committed counts, slot-set agreement, top warnings |
| `cruft.sh` | ledger pruner, runs on the host from `monad-mcp-cruft.timer` |
| `monad-mcp-node.service`, `monad-mcp-cruft.{service,timer}`, `cruft.env`, `run.sh` | pushed to the hosts |

`dist/` is generated and gitignored. `cruft.sh` and `run.sh` run *on the validator*, so they
are the two scripts here that do not source `lib.sh`.

## Things worth knowing

- **`hosts.txt` order is the network identity.** The line index becomes `node_id`,
  `cadence_key_pair`, `proposal_key_pair` and `chorus_pubkey` (the stub env derives every key
  from that one u64). Reordering or inserting a host renumbers the validator set and needs a
  new genesis.
- **Every whole-network `start`, `restart` and `upgrade` mints a new genesis** (`now + lead`,
  default 90 s) and wipes `~/monad-mcp/ledger/`; `--keep-ledger` archives it to
  `ledger/blocks-<ts>` instead. The node keeps no state, so this is the normal way to change a
  cadence parameter: flags after `--` go to `gen-config.sh`, e.g. `./netctl.sh restart --
  --delta 200 --slot-interval 120`. `netctl.sh run-one <host> start|stop` is the only path that
  reuses the config already on the host, for crash/catch-up testing of a single node.
- **Always ssh to `<host>.devcore4.com`.** `~/.ssh/config` here has no `ewr-*`/`lax-*`
  short-name pattern; the FQDN matches `Host *.devcore4.com` (user `monad`, port 9022).
  `lib.sh` spells user and port out anyway.
- **Port 8002/udp.** 8000 is unusable: the nftables table `monad_mitigations` drops UDP to
  8000 with length 0-1400, which is every Cadence vote. `MCP_PORT` overrides it, 8001 being
  the only other open port.
- **`MCP_SUPERVISOR=setsid`** switches every script to the no-systemd fallback: `deploy.sh`
  skips `enable-linger`/`daemon-reload`/`enable`, `netctl.sh` launches
  `~/monad-mcp/run.sh` (`setsid` + `run/pid`), stops it with `kill -INT`, and `report.sh`
  reads `~/monad-mcp/logs/node.log` (which it cannot filter by time, unlike journald).
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
  installs only if it is absent. The node has no ledger writer yet, so `ledger/blocks` stays
  empty and `cruft.sh` exits quietly; the pruner is in place for when the writer lands.
- **The node has no `--version` flag**, so the sha in the binary name plus `dist/VERSION` is
  the version, and `build.sh` refuses to build a dirty `monad-mcp-*` tree unless given
  `--dirty` (which stamps `-dirty` into the name). Health is the `udp bound` line plus
  `finalized` lines in the journal.
- **`report.sh` does not tell the fast path from the fallback path.** The distinction only
  exists in the ansi color of the `block` field, which the report strips rather than decodes, so
  it counts `finalized`, all-committed blocks (`block=+++++`), `proposing`, warnings and errors.
  With delta 150 ms against RTTs of 154-240 ms, the far pairs are expected to finalize on the
  fallback path.
- **Stub crypto on an Internet-exposed port.** The UDP transport trusts the sender id in the
  frame and every key derives from a small integer; frames from outside the validator set are
  dropped, and that is the whole authentication story. This is a test network — do not put
  anything of value behind it.
- `systemctl --user` over a non-interactive ssh needs `XDG_RUNTIME_DIR` and
  `DBUS_SESSION_BUS_ADDRESS` (`lib.sh`'s `remote_env`); `systemctl disable` of `monad-bft` is
  not possible as `monad` (polkit allows start/stop only), so a reboot or the Jenkins
  `mcp_test` reset job will bring the legacy stack back and re-bind the ports.
