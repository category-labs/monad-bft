> [!NOTE]
> The canonical version of this runbook lives in category-infra at
> `runbooks/internal/chain-data-archiver-runbook.md`. This copy was pasted
> verbatim as of 2026-06-11 — check the canonical version for updates.

Provision a chain-data archiver
========================================

> [!IMPORTANT]
> Owner: Joe Howarth  
> User(s):

Chain-data ingest runs embedded in the `monad-archiver` binary. The monad
debian package ships the unit `monad-chain-data-archiver.service`, which runs
`/usr/local/bin/monad-archiver --config /home/monad/chain-data-archiver.toml`
with `Restart=always` and `AWS_EC2_METADATA_DISABLED=true` already set.

This runbook starts from a host that **already has the monad deb installed**
and connects the service to existing Scylla and RustFS (or AWS S3) clusters.
Building and packaging are covered by the
[release runbook](./release-runbook.md).

**Rule: exactly one ingest writer per store pair (meta table + blob bucket).**
Never start a second writer against the same store. (Readers are unrestricted —
serve the store via
[the queryX rpc runbook](./chain-data-queryx-rpc-runbook.md).)

### Prerequisites

- The monad deb is installed: `/usr/local/bin/monad-archiver` exists and its
  shared libraries are in `/usr/local/lib` (the package `postinst` runs
  `ldconfig`; no `LD_LIBRARY_PATH` is needed).
- The `monad` user and `/home/monad` exist (host provisioning creates them —
  the deb does not). Fleet provisioning is an implicit prerequisite throughout:
  it also supplies the deb's `libhugetlbfs-bin` dependency (absent from stock
  noble repos) and `monadctl` (not shipped in the deb).
- Network reachability to: the Scylla Alternator endpoint(s) (port 8000), the
  RustFS endpoint(s) (port 9000) or AWS S3, and the source block-archive S3
  bucket.

### 1. Credentials — AWS CLI profiles

The config references three named profiles; create them under `~monad/.aws`
(mode 600, owner `monad`). Substitute your network for `mainnet`:

`/home/monad/.aws/credentials`

```ini
[s3-mainnet]
aws_access_key_id = <archive source bucket creds>
aws_secret_access_key = ...

[scylla-mainnet]
aws_access_key_id = scylla
aws_secret_access_key = scylla

[rustfs-mainnet]
aws_access_key_id = <rustfs creds>
aws_secret_access_key = ...
```

Profiles supply credentials only — regions and endpoints come from the TOML.

**Scylla auth:** CQL authentication and Alternator authorization are
independent gates on the same data. By default Alternator does **not** enforce
authorization — any signature is accepted, so anyone with network reach to
port 8000 has full read/write access to the Alternator tables even when CQL
requires a password. The dummy `scylla`/`scylla` creds above rely on that
default, which is acceptable only because the cluster is reachable solely over
the private overlay network — the trust boundary is the network, not the
credentials. For anything less isolated, set
`alternator_enforce_authorization: true` in `scylla.yaml`; enforcement reuses
the CQL role database: sign with `aws_access_key_id` = the Scylla role name
and `aws_secret_access_key` = that role's salted password hash, fetched over
CQL:

```sh
cqlsh <scylla-node> -u <admin> -p <pass> \
    -e "SELECT salted_hash FROM system.roles WHERE role = '<role>';"
```

(`system_auth.roles` on older Scylla versions.)

### 2. Config — `/home/monad/chain-data-archiver.toml`

Owner `monad`, mode `0600`:

```toml
unsafe_disable_normal_archiving = true
skip_connectivity_check = false

# Where ingest reads blocks/receipts/traces from (the network's block archive).
[block_data_source]
type = "aws"
bucket = "<source-archive-bucket>"     # e.g. mainnet-deu-009-0
region = "us-east-2"
profile = "s3-mainnet"
read_timeout_secs = 60
operation_attempt_timeout_secs = 60
operation_timeout_secs = 300

# Required by the config parser, but never used or created while
# unsafe_disable_normal_archiving = true.
[archive_sink]
type = "fs"
path = "/tmp/monad-chain-data-archive-sink-unused"

[chain_data_ingest]
enabled = true

# Meta store: Scylla via the Alternator (DynamoDB-compatible) API.
[chain_data_ingest.store.meta]
type = "dynamo"
table = "<meta-table>"                          # e.g. mainnet-1
endpoint_urls = ["http://<scylla-node>:8000"]   # multiple entries round-robin
region = "us-east-1"
profile = "scylla-mainnet"
create_table = true
scylla_profile = true
scylla_concurrency = 1024

# Blob store: RustFS (or AWS S3 — then drop endpoint_urls/force_path_style).
# OMITTABLE for `engine.payload = "external-archive"` stores: row locators
# then point into the source archive's own objects (add a read-only
# [chain_data_ingest.store.archive] block mirroring [block_data_source] —
# required, readers fetch payload bytes through it), no pack blobs are
# written, and checkpoints are auto-disabled (recovery rebuilds from the
# fragments at the published head; resume granularity is bounded by the
# checkpoint_every_blocks-clamped flush cadence). Live example:
# /home/jhow/chain-data-mainnet-2/mainnet-2.toml on vin-020.
[chain_data_ingest.store.blob]
type = "s3"
bucket = "<blob-bucket>"                        # e.g. mainnet-1
endpoint_urls = ["http://<rustfs-node>:9000"]
region = "us-east-1"
profile = "rustfs-mainnet"
prefix = ""
force_path_style = true                         # required for RustFS/MinIO
max_concurrency = 256
create_bucket = true

[chain_data_ingest.engine]
# Live (follow the tip): leave `end` unset.
# Bounded backfill: end = <inclusive block> (restart-safe).
#
# Fetch/pack defaults (fetch_concurrency = 2000, fetch_buffer = 5000,
# pack_max_blocks = 10000) are sized for high-latency per-object sources
# during catch-up and need no tuning on typical hosts. On memory-constrained
# hosts or against a rate-limited source, lower fetch_concurrency.
```

Notes:

- **`endpoint_urls` must not be empty** when targeting Scylla/RustFS — an
  empty list falls through to the real AWS endpoints for that service.
- Endpoints *can* instead live in the AWS profiles (`endpoint_url = ...` in
  `~/.aws/config`; the SDK honors it when the TOML `endpoint_urls` is empty),
  but keep them in the TOML: multiple `endpoint_urls` (Scylla round-robin,
  RustFS key-partitioning) only work there, and the blob store treats an empty
  `endpoint_urls` as "real AWS" when deciding how to create buckets.
- There is deliberately **no start-block knob**: ingest resumes from store
  state (or genesis on an empty store).

### 3. Test connectivity and auth

Run as the `monad` user so the exact profiles the service will use are
exercised:

```sh
# Scylla — Alternator API (what the service talks to)
sudo -u monad aws dynamodb list-tables \
    --endpoint-url http://<scylla-node>:8000 --profile scylla-mainnet --region us-east-1

# Scylla — CQL layer, if you need to inspect the cluster itself
cqlsh <scylla-node> 9042 -e "SELECT release_version FROM system.local;"

# RustFS
sudo -u monad aws s3 ls \
    --endpoint-url http://<rustfs-node>:9000 --profile rustfs-mainnet

# Source archive bucket (real AWS)
sudo -u monad aws s3 ls "s3://<source-archive-bucket>/" --profile s3-mainnet | head
```

All four should return without credential or connection errors before you
start the service.

### 4. Enable and start

The deb installs the unit but does not enable it:

```sh
sudo systemctl enable --now monad-chain-data-archiver
```

The unit's defaults (config path, `RUST_LOG` with the ingest throughput
probe enabled) are usually right; change them with
`sudo systemctl edit monad-chain-data-archiver` if needed.

### 5. Verify

```sh
journalctl -u monad-chain-data-archiver -f
```

- Startup probes the source bucket (`skip_connectivity_check = false`), then
  logs the ingest start and resume point.
- A throughput line logs every 5s under the `ingest::timing` target
  (`blocks_per_s`, `txs_per_s`, per-stage busy/blocked percentages).
- Health = process up + the published head advancing in the meta store:

  ```sh
  sudo -u monad monad-archiver chain-data-head
  ```

  (`--config` defaults to `/home/monad/chain-data-archiver.toml`, the unit's
  config path.) Run it twice; the printed block number should advance (during deep backfill
  publishes land in large strides — see Operations). There is no HTTP health
  endpoint or metrics exporter.
- `monadctl status` includes the service.

### Operations

- **Restarts are safe and automatic.** On startup the engine resumes from
  `max(checkpoint, published_head)`; re-ingest of overlapping blocks is
  idempotent. Checkpoints land every 10k blocks by default
  (`engine.checkpoint_every_blocks`).
- **Upgrade note (flush-boundary heads).** Builds since the flush-boundary
  publisher only publish heads the fragment rebuild can recover from. A store
  last written by an older build may hold a mid-pack head; before restarting
  it under the new build, confirm the latest checkpoint block is at/ahead of
  the published head (`chain-data-head` vs the resume-point log line) — the
  normal backfill state. If the head is ahead, let the old build reach its
  next checkpoint first, or plan a re-backfill.
- **The process is fail-fast by design.** A source fetch error, an exhausted
  Scylla batch-write retry budget, or a persistent blob write failure aborts
  the pipeline — recovery is `Restart=always` + automatic resume. A
  persistent crash-loop means the upstream/store problem is persistent; check
  endpoint reachability first.
- **Published head lags during deep backfill.** Flush cadence is
  `max(distance_to_tip / tip_lag_divisor, 1)` (divisor default 10), so from a
  cold start the queryable head advances in large strides. Raise
  `engine.tip_lag_divisor` to publish more incrementally.
- **Backfill vs live is not a mode switch** — the engine always follows the
  tip; `engine.end` just bounds the run (prefer `end` over `count`; `end` is
  restart-safe). Remove it and restart to go live.
- **Fleet-managed hosts:** the `monad_chain_data_archiver` ansible role
  (`playbook.yml -t chain_data_archiver`) and chain-ops
  (`push_chain_data_archiver_config`) both write this config file. On managed
  hosts ansible is authoritative — hand edits will be clobbered.
