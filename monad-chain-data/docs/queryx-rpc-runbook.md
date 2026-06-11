> [!NOTE]
> The canonical version of this runbook lives in category-infra at
> `runbooks/internal/chain-data-queryx-rpc-runbook.md`. This copy was pasted
> verbatim as of 2026-06-11 — check the canonical version for updates.

Enable queryX (eth_query*) on a monad-rpc node
========================================

> [!IMPORTANT]
> Owner: Joe Howarth  
> User(s):

queryX serving is part of the normal `monad-rpc` binary and service — there is
no separate queryX daemon. It switches on by pointing monad-rpc at a chain-data
store with `--chain-data-config <toml>`; without the flag the `eth_query*`
methods answer "Method not supported" (`-32601`) and nothing else about the
node changes. The deb's `monad-rpc.service` appends `$MONAD_RPC_EXTRA_ARGS`
(empty by default) to its arguments, so enabling queryX is config-only — no
unit edit.

This runbook starts from a host that **already runs `monad-rpc.service` from
the monad deb** and connects it read-only to an existing chain-data store
populated by an ingest writer (see the
[chain-data archiver runbook](./chain-data-archiver-runbook.md)).

Readers are strictly read-only and never create tables or buckets; any number
of readers can point at the same store pair while its single ingest writer
runs.

### Prerequisites

- The monad deb is installed and `monad-rpc.service` runs. The unit needs the
  `$MONAD_RPC_EXTRA_ARGS` hook (deb builds from 2026-06-11 onward).
- Network reachability to the Scylla Alternator endpoint(s) (port 8000) and
  the RustFS endpoint(s) (port 9000) or AWS S3.
- A chain-data store with a published head. From any host with store creds:
  `monad-archiver chain-data-head --config <store toml>`.

### 1. Credentials — AWS CLI profiles

Same scheme as the archiver runbook §1, but the reader needs only the two
store profiles under `~monad/.aws` (mode 600, owner `monad`) — no archive
source profile. Substitute your network for `mainnet`:

`/home/monad/.aws/credentials`

```ini
[scylla-mainnet]
aws_access_key_id = scylla
aws_secret_access_key = scylla

[rustfs-mainnet]
aws_access_key_id = <rustfs creds>
aws_secret_access_key = ...
```

See the archiver runbook for the Scylla Alternator authorization discussion —
the same trust-boundary caveats apply to readers.

### 2. Config — `/home/monad/chain-data-reader.toml`

Owner `monad`, mode `0600`. The store pair (table, bucket, prefix) must match
what the ingest writer writes:

```toml
# queryX per-request caps; breaches answer error -32005 (LimitExceeded).
max_limit = 1000
max_block_range = 10000

[store.meta]
type = "dynamo"
table = "<meta-table>"                          # e.g. mainnet-1
endpoint_urls = ["http://<scylla-node>:8000"]   # multiple entries round-robin
region = "us-east-1"
profile = "scylla-mainnet"
scylla_profile = true
scylla_concurrency = 1024

[store.blob]
type = "s3"
bucket = "<blob-bucket>"                        # e.g. mainnet-1
endpoint_urls = ["http://<rustfs-node>:9000"]
region = "us-east-1"
profile = "rustfs-mainnet"
prefix = ""
force_path_style = true                         # required for RustFS/MinIO
max_concurrency = 256
```

Notes:

- There are deliberately **no `create_table`/`create_bucket` keys**: readers
  must never create stores; the ingest writer owns store creation.
- **`endpoint_urls` must not be empty** when targeting Scylla/RustFS — an
  empty list falls through to the real AWS endpoints for that service.

### 3. Turn it on

```sh
sudo systemctl edit monad-rpc
```

add:

```ini
[Service]
Environment="MONAD_RPC_EXTRA_ARGS=--chain-data-config /home/monad/chain-data-reader.toml"
```

then `sudo systemctl restart monad-rpc`. (The unit also sources
`/home/monad/.env`, so a `MONAD_RPC_EXTRA_ARGS=...` line there works too.)

Startup logs `Chain-data reader initialized successfully`. A failed reader
init does **not** crash the node: it logs
`Unable to initialize chain-data reader` and serves everything else, with the
`eth_query*` methods answering "Method not supported" — so a silent
misconfiguration looks like the flag is missing. Check the journal line after
every change here.

### 4. Verify

```sh
curl -s -X POST 127.0.0.1:8080 -H 'Content-Type: application/json' -d '{
  "jsonrpc": "2.0", "method": "eth_queryBlocks",
  "params": [{"fromBlock": "0x100", "toBlock": "0x103"}], "id": 1}'
```

Expect `result.data.blocks` with 4 entries. The store's published head (the
upper bound of what queryX can answer):

```sh
sudo -u monad monad-archiver chain-data-head --config /home/monad/chain-data-reader.toml
```

(`chain-data-head` accepts both the archiver and reader TOML shapes.)

### Operations

- **Freshness** is the writer's published-head cadence: every block at the
  tip, large strides while the writer backfills (see the archiver runbook).
  Queries above the published head answer like the future: no data.
- **`max_limit` / `max_block_range` are per-host caps**; raising them raises
  the worst-case scan cost a single request can incur.
- **Disable** by removing the override (`sudo systemctl revert monad-rpc`
  removes *all* drop-ins; on fleet-managed hosts remove
  `monad-rpc.service.d/chain-data.conf`) and restarting.
- **Fleet-managed hosts:** the `monad_chain_data_rpc` ansible role
  (`playbook.yml -t chain_data_rpc`, gated on `monad_chain_data_rpc_enabled`)
  writes the reader TOML and the drop-in. Ansible is authoritative — hand
  edits will be clobbered.
