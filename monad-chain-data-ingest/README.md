# monad-chain-data-ingest

`monad-chain-data-ingest` owns archive-backed ingestion into `monad-chain-data`.
It exists outside `monad-chain-data` so the core query/ingest data model does
not depend on `monad-archive` backends, while binaries such as `monad-rpc` can
embed the same archive polling pipeline.

## Current Shape

- `src/lib.rs` contains the ingest implementation that used to live in
  `monad-chain-data/src/bin/chain-data-ingest.rs`.
- `src/bin/chain-data-ingest.rs` is a thin CLI wrapper around
  `monad_chain_data_ingest::run`.
- `monad-chain-data` keeps the fjall-backed store and query service, but no
  longer owns the archive ingest binary or archive-only dependencies.

## RPC Embed

The RPC process must open the fjall-backed chain-data service once, then share
that service between queryX handlers and the ingest task. Opening the same fjall
path independently from a standalone ingest process and from RPC is not viable
because fjall allows only one process to own the writer handle.

The shared API is:

- `open_fjall_chain_data(...) -> OpenChainDataIngest`
- `run_with_opened(opened: OpenChainDataIngest, config: Cli)`

`monad-rpc` uses this helper when
`--chain-data-ingest-block-data-source <SOURCE>` is set. The returned
`Arc<MonadChainDataService<...>>` is passed to RPC resources, and the ingest
future is spawned in the same tokio runtime.

When neither `--end` nor `--count` is present, ingest runs in live-follow mode:
it fetches `published_head + 1`, polls `LatestKind::Uploaded` while the archive
source has not published that block yet, and continues after each successful
publication. Finite backfill mode still uses the ordered concurrent prefetch
pipeline.

Batching is adaptive. `--max-batch` sets only the ceiling: the ingest worker
waits for one fetched block when none are ready, then drains already-ready
fetched blocks up to the configured maximum without waiting to fill the batch.
RPC exposes the same control as `--chain-data-ingest-max-batch`.

Operationally, RPC should fail startup if embedded ingest is requested without a
chain-data path and archive source. Once started, a fatal deterministic ingest
error should terminate RPC, while archive-tip misses should be treated as normal
polling backoff.
