# Chunk data in simulations

A simulated run carries every DA chunk as a message between nodes. With real
D33 chunks, every receiver of a proposal checks each chunk's Merkle proof,
Reed–Solomon decodes, and re-encodes to confirm the root. That is most of the
cost of a large-proposal run. The scenario harness can instead run with
hollow chunks: the same messages and sizes, without the data.

## Choosing the chunk data

A node proposes under `NodeConfig::proposal.encoding_scheme`
(`monad_mcp_da::ProposalScheme`):

| scheme | what travels | receivers |
|---|---|---|
| `D33` (default) | real chunks: symbol and Merkle proof, 1440 bytes per packet | check proofs, decode, re-encode |
| `D33Hollow` | the chunk id and a stand-in proof, 130 bytes per packet | check the stand-in proof, count chunks, take the message from a store the nodes of the process share |
| `D33HollowInconsistent` | as `D33Hollow`, for a Byzantine proposer: a root the message does not encode to | as for `D33Hollow`; every node resolves the proposal invalid |

The two hollow schemes exist only with the cargo feature `enable_hollow`
(`monad-mcp-cadence`, `monad-mcp-da`, `monad-mcp-node`). The node's tests and
examples turn it on (the node's dev-dependency on itself); a node built
without it, as a deployed node is, does not know the scheme: it cannot propose
hollow chunks or parse them. With the feature, a config file can also select
`encoding_scheme = "d33_hollow"`, but hollow chunks only decode within one
process. `D33HollowInconsistent` is set in code only.

In the scenarios:

- set `config.proposal.encoding_scheme` per node, as `loss.rs` does for
  `D33` and `D33Hollow`, and `byzantine.rs` does for a Byzantine proposer
  among hollow nodes;
- `BENCH_SCHEME=d33_hollow` runs `bench::slot_cost` and `bench::scaling` with
  hollow chunks.

## What hollow keeps and what it models

Kept exactly as with D33, so network timing is the same:

- the number of messages, their senders and receivers: the chunk assignment,
  first hop, relays, recovery requests and replies are D33's;
- message sizes: the harness charges a hollow chunk packet the size of the
  D33 segment it stands for (`monad_mcp_da::nominal_len`, used by
  `harness::wire_size`); the bench reports both, as "on the wire" and
  "charged";
- when a node can decode: at `k` distinct valid chunks, as for any MDS code;
- obligations and votes: which chunks a node holds from whom.

Modelled:

- **The root** is a digest of the message, not a Merkle root, so pids, and
  with them send orders, differ from a D33 run. Hollow runs are statistically
  like D33 runs, not identical: their digests differ.
- **Chunk validity.** The stand-in proof is the root with the chunk id folded
  into its last bytes. A chunk under another root or another id fails it,
  as a tampered, foreign or misplaced real chunk fails its Merkle proof.
- **Inconsistent proposals.** A proposer under `D33HollowInconsistent`
  publishes its message under a root it does not encode to
  (`SharedMessage::publish_inconsistent`). Its chunks are valid under that
  root, so nodes count them and vote, and every node resolves the proposal
  `InvalidEncoding` at decoding, as with real chunks that verify and form no
  codeword. `byzantine.rs` checks that such lanes commit and resolve invalid
  alike on every node, and that every slot still reaches the ledger.
- **Messages.** The author publishes its message, which any node holding a
  decoder for the root takes at decoding. The store (`THIN_AIR` in
  `encoding_scheme::hollow`) is shared by the nodes of the process. An entry
  lives while a node holds it: the author until it has disseminated, then
  every node that decoded it, until it drops the slot.

## Known gap: a message missing from the store

A node looks a hollow message up once, when it creates its decoder for the
root. If the store has no entry then, the decoder never resolves: the
proposal neither decodes nor fails, the node keeps pulling its chunks, and
a finalized slot that commits it is never handed to the ledger. That happens
in two cases:

- **No node of the process published the root**, for example a root a test
  forges without a message. Real chunks under such a root either fail their
  proofs or decode to an invalid encoding.
- **Every holder already dropped the message**, so a node that creates its
  decoder late misses it, where a D33 node would decode from the chunks it
  receives. Under the parallel engine, a holder's drop on one thread and a
  late node's lookup on another can fall in the same window in either order,
  so this can also depend on thread timing. It needs a node that lags behind
  the retention window of every other node; no scenario does this today.

This is waiting for the DA chunk store design in progress. Possible fixes are
keeping entries for a window of slots, so that an honest lookup never misses,
and resolving a miss at `k` chunks as `InvalidEncoding`.

## Cost

`bench::slot_cost` on `mcp-dev` `d724c2e9a`: every proposal exactly 400 KB,
`geo` placement, 1 Gbit/s uplinks, 10 slots, one domain per node, two
interleaved rounds on a 14-core Apple M4 Max. Seconds per simulated slot,
sequential / on 10 threads:

| n | table | D33 | D33 hollow |
|---|---|---|---|
| 64 | testnet | 0.59–0.60 / 0.15 | 0.19 / 0.04 |
| 64 | public internet | 0.59 / 0.17 | 0.19 / 0.04 |
| 190 | testnet | 2.38–2.39 / 0.36 | 1.11–1.12 / 0.15–0.16 |
| 190 | public internet | 2.40–2.42 / 0.37 | 1.11–1.15 / 0.16 |

Peak memory over the bench: 8.5 GB with D33, 2.5 GB with hollow. Every
node keeps a decoded proposal's encoding until it drops the slot, which with
real chunks is about 1.5 MB per 400 KB proposal per node. Chunk bytes
delivered per slot at `n = 190` (testnet): 541 MB with D33; 49 MB with
hollow, charged as 541 MB.

Hollow and D33 runs do the same protocol work (step counts within 0.003%,
the same lanes committed); hollow skips the receivers' proof checks, decodes and
re-encodes, and carries a tenth of the bytes. Earlier measurements, and the
parallel engine's speedup, are in [parallel-scenarios.md](parallel-scenarios.md).
