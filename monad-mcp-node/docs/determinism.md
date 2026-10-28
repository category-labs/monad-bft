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
- No iteration over a std `HashMap`/`HashSet` where the order reaches an
  output (messages, their order, certificates, requests). std hashers are
  seeded per process and per instance. Use `BTreeMap`/`BTreeSet`, `IndexMap`
  (insertion order), or sort first. Lookup-only maps can stay hashed.
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

Hash-container order that reached outputs:

| source | resolution |
|---|---|
| `Dissemination.to` (DA `egress.rs`), iterated by the router's DA output handler | a `Vec<NodeId>` in send order (see below) |
| `full_rebroadcast_targets`, `partial_rebroadcast_targets` (DA `assignment.rs`) | `BTreeSet<NodeId>` membership, sent in send order |
| `unassigned_nodes`, the header-only first-hop recipients | `BTreeSet<NodeIndex>` |
| `ProposalEnvelope::group` (DA `chunk.rs`) | envelopes in order of each header's first chunk |
| `MissingChunks`, `missing_chunks`, `everything_missing` (pull recovery to owners) | `BTreeMap<NodeId, _>`, requests in send order |
| `Recovery.decoded_holders` (pull recovery of our own chunks) | `BTreeSet<NodeId>`, requests in send order |
| `SignatureCollection::signers`/`verify` and the `aggregate`/`try_aggregate` inputs (chorus `spec.rs`, stub env) | `BTreeSet`/`BTreeMap`; their signers address header pulls and decoded-holder hints |
| `MissingHeaders` voters (header pull fan-out) | `BTreeSet<NodeId>`; DA pulls in send order |
| `VotePool` buckets, voters and votes (chorus `types.rs`); a tally's bucket order decides weak QC order, timeout certificate groups, header-wait and fallback-wait lists | a tally sorts its buckets by the bytes each vote signs (canonical, independent of arrival); the pool keeps them in an `IndexMap` (first-vote order, only logs see it), voters `BTreeSet`, votes `BTreeMap` |

Send order: an ordered container alone would send to low node ids first,
every time. DA instead shuffles a sender's rebroadcasts, recovery requests
and header pulls for one proposal with `util::shuffle_sends`: ChaCha20 seeded
from Blake3 of (pid, sender). That is uniform over proposals and senders, the
same in every run and process, and pinned by a test so that it also stays the
same across releases. The first hop already follows the assignment's holder
order, which S11 and D33 shuffle per proposal. This stands in for a send
strategy (prioritized sends) still to be designed.

Hash containers kept, lookup or membership only: DA `authenticated_headers`,
`ServingTracker.served`, `ProposerRaptorcast.tracked`; chorus availability
`headers`/`decoded`/`invalid`, MVBA `BlockStore` and its gc set, timeout
certificate checks, stub `ValidatorData` maps (enumerated through its sorted
list); node finalization `resolved`, mempool `pending`/`recent_set`;
chorus-sim's per-node logs.

Wall clock:

| source | resolution |
|---|---|
| `LedgerFollower` rescan timer, `Instant::now` (chorus `ledger/store.rs`) | `poll` and `follow_from_now` take `now`; the rpc's `LedgerWatch` passes `Instant::now()` |

Outside the simulated path, unchanged: the production `Clock` and
`AsyncNode`, log timestamps (`logging.rs`), the UDP network's peer map, the
swarm example and the UDP tests; `monad-mcp-rpc` and `monad-mcp-explorer`
(client tooling).

Fixed-key hashing: the stub `ProposalId::pid` and stub signatures use
`DefaultHasher::new()`. Its keys are fixed, so it is the same across
processes, but Rust does not promise it stays the same across releases.
