# Docs Guide

This directory carries the `monad-chain-data` docs that have landed with the
review stack so far.

Start with these in order:

1. [design/bitmap-indexed-history-queries.md](design/bitmap-indexed-history-queries.md)
2. [design/immutable-artifact-model.md](design/immutable-artifact-model.md)
3. [overview.md](overview.md)
4. [plans/24-upstream-review-stack-refresh.md](plans/24-upstream-review-stack-refresh.md)

These early docs explain the target architecture we are upstreaming, even if
some later behavior and subsystem docs have not landed in `monad-bft` yet.

Docs such as storage-model, query-execution, write-authority, caching, and
family-specific deep dives should land with the later commits that make those
boundaries real.

## Source Walk

For the code that has landed so far, a good reading order is:

1. `src/lib.rs`
2. `src/error.rs`
3. `src/core/`
4. `src/api.rs`
5. `src/family.rs`
6. `src/logs/`
7. `src/txs/`
8. `src/traces/`
9. `src/store/`
10. `src/kernel/`
