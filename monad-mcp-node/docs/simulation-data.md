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
| `D33HollowWithheld` | as `D33Hollow`, for a Byzantine proposer: a root whose message is in no store, as a forged root | as for `D33Hollow`; every node resolves the proposal invalid |

The hollow schemes exist only with the cargo feature `enable_hollow`
(`monad-mcp-cadence`, `monad-mcp-da`, `monad-mcp-node`). The node's tests and
examples turn it on (the node's dev-dependency on itself); a node built
without it, as a deployed node is, does not know the scheme: it cannot propose
hollow chunks or parse them. With the feature, a config file can also select
`encoding_scheme = "d33_hollow"`, but hollow chunks only decode within one
process. `D33HollowInconsistent` and `D33HollowWithheld` are set in code
only.

In the scenarios:

- set `config.proposal.encoding_scheme` per node, as `loss.rs` does for
  `D33` and `D33Hollow`, and `byzantine.rs` does for Byzantine proposers
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
- **Forged roots.** A proposer under `D33HollowWithheld` declares a root
  whose message no node of the process published. Its chunks are valid
  under the root; at `k` chunks every node, the proposer included, finds no
  message and resolves the proposal `InvalidEncoding`, as with real chunks
  under a root that no codeword has. `byzantine.rs` checks it as for
  inconsistent proposals.
- **Messages.** The author publishes its message in a store the nodes of the
  process share (`THIN_AIR` in `encoding_scheme::hollow`), and a node takes
  it from there when it creates its decoder for the root, by the rule below.

## Retention and misses

The store is the only place a node takes a hollow message from, so what a
node finds there would decide its outcome. To keep every outcome a function
of what the node received and of its own clock, a node does not ask whether
an entry happens to be there; when it creates its decoder for a proposal
authored at `τ` (the header's `unix_ts`, whole seconds), at its own time
`t`:

- `t < τ + HORIZON` (10 s): it takes the message published under the root,
  and at `k` chunks decodes it, or resolves the proposal invalid if the
  message was published inconsistent;
- `t ≥ τ + HORIZON`, or nothing was published under the root: it does not
  read the store, and resolves the proposal `InvalidEncoding` at `k` chunks.
  A `DecodingFailed` event follows, and the slot reaches the ledger.

The author's DA runtime keeps each message it publishes until its own clock
reads `τ + HORIZON + SLACK` (1 s more), and lets go of it at its next DA
tick. Nodes that hold the proposal keep it as well, until they drop the
slot; that only lengthens its life. So the entry is there whenever the rule
sends a node to it, on any number of threads:

- a node learns a root from a message, so its lookup comes after the
  author's publish;
- the parallel engine runs one window at a time, every step running at one
  moment lies in the current window, at most the smallest lookahead between
  domains wide (about 1 ms in the scenarios), and earlier windows are done.
  The author's release and a lookup the rule allows are more than `SLACK`
  minus the spread of node clock offsets apart in simulated time, so the
  release runs in a later window than the lookup.

This assumes `SLACK` is far larger than a window plus the spread of node
clock offsets (tens of milliseconds in the scenarios). Debug builds check
it: a node that misses within the horizon a root its author already let go
of panics. There is no store-wide sweep by simulation time: the store is
shared by every simulation in the process, such as the tests of one binary,
each with a clock of its own.

`HORIZON` is 100 slots at the scenarios' 100 ms slot interval, twice the 50
slots DA keeps a slot past the cap, so a node that is in sync, or pulls its
chunks from peers that still hold the slot, decodes. A node that takes a
proposal up later resolves it invalid, where a D33 node might still decode
it from chunks it already holds. `hollow_lag.rs` runs a node that handles
every message 7 s late (past every other node's slot drop, within the
horizon: it decodes every proposal) and 13 s late (beyond: it resolves every
other node's proposal invalid and still finalizes), and checks that both
runs are the same sequentially and on 2, 4 and 10 threads.

Memory: the messages authored in the last `HORIZON + SLACK` plus up to a
second, and those of slots some node still holds. At 5 lanes of 400 KB
every 100 ms that is at most about 240 MB, however long the run. Measured
in `bench::slot_cost` at `n = 190`: about 465 messages (186 MB) stay in the
store throughout, against about 243 (97 MB) when only the nodes holding a
slot keep its messages; the process's peak footprint is the same within
noise (9.7 GB over 300 slots, 10.0 GB over 2000).

A node removed from a swarm (`Swarm::remove_node`) takes its retention with
it. No scenario removes nodes; restarts would also need the nodes that take
a message to retain it.

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
