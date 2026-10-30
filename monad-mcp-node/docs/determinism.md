# Determinism under simulation

A simulated run of MCP nodes is a pure function of its configuration and
seeds: the same scenario gives the same finalized history and the same
delivered messages at the same times, in one process or across processes.
`tests/scenarios/determinism.rs` checks this.

## Where seeds come from

- The simulation seed goes to `Swarm::build`, which seeds the network
  (loss, jitter).
- Each node gets `NodeConfig::seed`. The scenario harness derives it from the
  simulation seed and the node id (`harness::node_seed`). `NodeConfig::rng`
  builds the node's generator from it; without a seed (production default) it
  draws from OS entropy.

No component in the node, chorus or DA crates needs genuine entropy: keys come
from config or the keystore.

## Rules for simulated code

- No `thread_rng`, `rand::random`, `OsRng` or `from_entropy`: take an RNG or
  seed from the config or the constructor.
- No output (messages, their order, certificates, requests) may depend on
  the iteration order of a hash table. Hashers are seeded per process (and
  std's per instance), and hashbrown lays tables out differently on arm and
  x86 (16- vs 8-byte probe groups), so even a fixed seed does not give a
  portable order. Pick the container for how it is used, from
  `monad_mcp_common::containers` (its module docs say which when):
  `ValidatorSet`/`ValidatorMap` for validators of an epoch, an `IndexVec` for
  dense state over another index (DA's `ByNode`), a `Vec` for lists that are
  only iterated, a `BTreeMap`/`BTreeSet` where the order means something, and
  `FastHashMap`/`FastHashSet` (or `SipHashMap`/`SipHashSet` for keys an
  adversary chooses) only for lookups; these offer iteration only as
  `*_unordered`. std's `HashMap` and `HashSet` are disallowed by clippy
  (`disallowed-types` in each crate's `clippy.toml`).
  `clippy::iter_over_hash_type` is denied too; it catches `for` loops over
  hash containers, including over `*_unordered`, but not iterator chains
  (`.iter_unordered().map(..).collect()`), which the determinism test has to
  catch. Every fast hash map is seeded at random per process, so that test
  sees any order that leaks. Mark a justified exception with `#[allow]` and a
  reason.
- No wall-clock reads (`Instant::now`, `SystemTime::now`): take `now` from
  the caller, i.e. the injected clock.

## Sources found and what was done

Audited on `mcp-dev` `e297f5204` (`monad-mcp-chorus`, `-da`, `-node`,
`-common`, `-chorus-sim`, the scenario harness).

Explicit randomness:

| source | resolution |
|---|---|
| random payloads, `thread_rng` (`component/proposing.rs`, `random_payload`) | `RandomProposing` takes a `ChaCha8Rng` from `NodeConfig::rng` |
| chunk-assignment shuffles in d33 and swiper (`ChaCha20Rng`) | unchanged: seeded from the header (slot, timestamp, author key), part of the protocol |
| conductor test (`StdRng::seed_from_u64`) | unchanged: test, seeded |

Containers whose order reached outputs, and the related DA state:

| source | resolution |
|---|---|
| `Dissemination.to` (DA `egress.rs`), iterated by the router's DA output handler | a `Vec<NodeId>` in send order (see below) |
| rebroadcast targets (DA `instance_rc`) | no set: `ChunkRouting::rebroadcast_targets` filters the instance's send order down to the chunk's targets |
| `unassigned_nodes`, the header-only first-hop recipients | a `Vec<NodeIndex>` in index order |
| `ProposalEnvelope::group` (DA `chunk.rs`) | a short list, envelopes in order of each header's first chunk |
| `MissingChunks`, `everything_missing` (pull recovery to owners) | a `Vec<(NodeId, ChunksSubset)>` in assignment (or validator) order, grouped in a `Vec` indexed by `NodeIndex`; requests in send order |
| `ServingTracker.served` | a `Vec` indexed by `NodeIndex` |
| `Recovery.decoded_holders` (pull recovery of our own chunks) | the author plus a `ValidatorSet` of hinted holders, listed in index order; requests in send order |
| `SignatureCollection::signers`/`verify` and the `aggregate`/`try_aggregate` inputs (chorus `spec.rs`, stub env) | `signers`/`verify` return a `ValidatorSet`; aggregation takes `(ValidatorIndex, &Signature)` pairs; their signers address header pulls and decoded-holder hints |
| `MissingHeaders` voters (header pull fan-out) | `ValidatorSet`; DA pulls in send order |
| `VotePool` buckets, voters and votes (chorus `types.rs`); a tally's bucket order decides weak QC order, timeout certificate groups, header-wait and fallback-wait lists | a tally sorts its buckets by the bytes each vote signs (canonical, independent of arrival); the pool keeps them in a short `Vec` in first-vote order, voters in a `ValidatorSet`, votes in a `ValidatorMap` |
| timeout certificate signer check (MVBA `certificates.rs`) | `ValidatorSet`, disjointness per group |

Validators are addressed by `ValidatorIndex` from the point a sender is
checked against the validator set (`Chorus::handle_message`, and
`MvbaRuntime::receive` for a standalone MVBA). The stub indexes validators in
`NodeId` order, so iterating a `ValidatorSet` in index order lists the same
nodes in the same order as the `BTreeSet<NodeId>` it replaced.

Send order: an ordered container alone would send to low node ids first,
every time. DA instead shuffles a sender's rebroadcasts, recovery requests
and header pulls for one proposal with `util::shuffle_sends`, an `OrderKey`
(ChaCha20 seeded from Blake3 of a domain tag and the RLP of pid and sender).
That is uniform over proposals and senders, the same in every run and
process, and pinned by a test so that it also stays the same across
releases. The first hop already follows the assignment's holder order, which
S11 and D33 shuffle per proposal. This stands in for a send strategy
(prioritized sends) still to be designed.

Drawing a pseudo-random order: use `monad_mcp_common::containers::OrderKey`,
an explicit keyed permutation, with a domain tag of its own; never the
iteration order of a hash container. Even with a fixed hasher seed,
hashbrown lays its table out differently on x86 (16-byte probe groups) and
arm (8-byte groups) and across versions, so its order is not portable. The
permutation costs one pass over the elements; where the list is materialized
anyway (the requests DA builds) the shuffle is in place and needs no extra
buffer, and where the same order is reused (a raptorcast instance's send
order) it is computed once and filtered per use.

Hash containers, lookup or membership only, all `FastHashMap`/`FastHashSet`
unless noted: DA `authenticated_headers`, `ProposerRaptorcast.tracked`; chorus
availability `headers`/`decoded`/`invalid`, MVBA `BlockStore` and its gc set,
the timeout certificate's distinct votes, the stub `ValidatorData`'s
`NodeId -> ValidatorIndex` map (stakes, pubkeys and node ids are in
index-ordered `IndexVec`s); node finalization `resolved`, the config and UDP
peer maps; chorus-sim's per-node logs. Mempool `pending`/`recent_set` are
`SipHashSet`s, since senders choose the tx hashes.

Wall clock:

| source | resolution |
|---|---|
| `LedgerFollower` rescan timer, `Instant::now` (chorus `ledger/store.rs`) | `poll` and `follow_from_now` take `now`; the rpc's `LedgerWatch` passes `Instant::now()` |

Outside the simulated path, unchanged: the production `Clock` and
`AsyncNode`, log timestamps (`logging.rs`), the UDP network's broadcast over
its peer map, the swarm example and the UDP tests; `monad-mcp-rpc` and
`monad-mcp-explorer` (client tooling, not under the Cadence lints; rpc takes
the node's peer map as a `FastHashMap`).


Stable hashing: the stub `ProposalId::pid` and stub signatures digest their
input with `monad_mcp_common::stub::noncrypto_digest` (rapidhash v3, over the
header's rlp encoding and over the signed bytes). Its output is fixed by the
reference algorithm, so it is the same on every platform and across crate
versions, unlike std's `DefaultHasher`, whose algorithm and derived-`Hash`
input Rust does not promise to keep across releases. It is not
cryptographic: the stub provides no security (keys derive from public
u64s), and it costs a few nanoseconds per call, which matters because every
vote re-verifies its pool's signatures. A production pid or signature needs a
cryptographic hash. With that, a seeded run should be the same across
releases too, except where a dependency changes its output (rand,
rand_chacha, blake3). Tests pin the send order, the digest and the random
payload lengths.

Platforms: simulated code avoids transcendental float functions (`log2`,
`powf`, `ln`, `exp`, ...). libm does not round them correctly, and results
differ in the last bit between platforms (macOS vs Linux, FMA vs no FMA on
arm and x86), so a seeded run could diverge between machines. The random
payload length is drawn with integers only. Basic float operations (`+`,
`*`, `ceil`, comparisons) are exact under IEEE 754 and safe; the network's
loss draw and DA's chunk counts use only those.

Not yet portable: `monad_sim::dist`'s normal and log-normal distributions
(rand_distr). rand_distr does its `exp`/`ln` through num-traits, which uses
the pure-Rust `libm` crate (bit-identical on arm and x86) unless some crate in
the build enables `num-traits/std`, and then the system libm. Cargo unifies
features, so which one applies depends on what is built together: MCP-only
builds get `libm`; `monad-mock-swarm` and `--workspace` builds get std (via
criterion, among others). MCP scenarios use only `uniform_duration` and
constant latencies; do not use the normal or log-normal ones until
`monad-sim` samples them through `libm` directly.
