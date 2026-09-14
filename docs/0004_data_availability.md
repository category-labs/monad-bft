# Data Availability (monad-mcp-da)

This document describes the architecture of the data availability (DA)
module in `monad-mcp-da`.

DA owns everything about chunks: receiving, checking, forwarding,
decoding, serving them on request, and telling consensus what it has
learned. It decides nothing about voting. Chorus (consensus) tells DA
what it needs through three commands.

## 1. Inputs and outputs

DA is a state machine with no clock, no sockets and no threads. The
surrounding runtime feeds it and drains it.

```
   wire                    conductor              chorus
    |   ^                      |                  |     ^
    |   |                      |                  |     |
    v   |                      v                  v     |
  +-----------------------------------------------------------+
  |                        DARuntime                          |
  +-----------------------------------------------------------+
                               |
                               v
                     decoded proposal (translation/execution)
```

**In:**

- from the wire: a proposal envelope, that is one header plus any
  subset of its chunks (a chunk is `header + chunk id + merkle proof +
  symbol`); or a chunk request from a peer.
- from the conductor: slot `Opened` and slot `Completed`.
- from chorus: `ReleaseChunks`, `PinRoot { j, root }`,
  `RecoverChunks { j, root, MyChunks | YourChunks, voters }`.

**Out:**

- to chorus, per `(slot, j)`: `HeaderSeen(header)`,
  `ProposerObligationFulfilled(root)`,
  `OwnerObligationFulfilled { owner, root }`, `Decoded(root)`,
  `DecodingFailed(root)`.
- to translation/execution: the decoded proposal bytes under
  `(slot, j, root)`.
- to the wire: envelopes to send (second hop, answers to requests) and
  chunk requests to send.

What the events mean to chorus:

- `HeaderSeen`: once per distinct signed root of a proposer. Two of
  them for one `(slot, j)` are an equivocation certificate.
- `ProposerObligationFulfilled`: all of our own chunks arrived from the
  author. This is what we vote positively on at the deadline.
- `OwnerObligationFulfilled { owner }`: that owner finished its second
  hop to us. Chorus then admits the owner's positive vote, so that a
  counted vote always means "we hold the chunks it vouches for".
- `Decoded` satisfies every gate at once. `DecodingFailed` marks the
  root invalid. Either one is "resolved".

What the commands mean:

- `PinRoot`: chunks of this root are to be admitted. Chorus pins a
  root when it expects to decode it (it voted for it, or holds a
  certificate for it).
- `ReleaseChunks`: let the slot's chunks leave DA: rebroadcast our
  owned chunks and answer requests. Sent at the deadline `D_s`.
- `RecoverChunks`: pull chunks from these peers, following ChunkSync.
  Chorus sends it on a certificate for an unresolved root (P1), with
  `MyChunks` towards positive fallback signers (P2), and at the fallback
  transition for blocked roots (P3, P4).

## 2. The raptorcast layering

DA is a tower of nested scopes. Each layer certifies one fact about
everything below it, so the layers below never re-check it.

```
DARuntime                                      node
  SlotRaptorcast                               slot
    ProposerRaptorcast                         (slot, j)
      RaptorcastInstance                       (slot, j, root)
```

| layer | certifies | holds |
|---|---|---|
| runtime | - | the live slots, following the conductor's slot lifecycle |
| slot | the slot is alive | the epoch context, and one proposer per proposal index of the slot |
| proposer | the proposer is legitimate for `(slot, j)` | the roots it has announced, one instance per admitted root |
| instance | the proposal is known: a header signed by the proposer | the chunk store, the decoding state, and the per-proposal trackers |

- **Runtime**: opens a slot when the conductor opens it, drops it some
  slots after completion.
- **Slot**: authenticates incoming headers. Signature plus election
  gives the proposer index `j`, and the envelope goes to that proposer.
- **Proposer**: validates the author's chunks and decides which of its
  roots get an instance. Every new root is reported as `HeaderSeen`
  once. Pinned roots are always admitted.
- **Instance**: stores and disseminates the chunks of one proposal.

## 3. The raptorcast instance

The instance's job is to hold chunks and decode. Ingesting a chunk is:
verify it against the root, store it, rebroadcast it if we own it, and
decode once enough chunks are held.

Two more duties fall to the instance because of what it holds:

- **Recovery.** It serves chunk requests because it holds the chunks,
  and it knows exactly which chunks to ask a peer for because it knows
  which ones it is missing.
- **Obligations.** It knows, for every chunk, who should have sent it,
  so it can tell chorus when the author or an owner has delivered
  everything it owed.

## 4. Assignment, upstream, obligation

The **assignment** is a table from chunk id to owner, determined by
the encoding scheme, the author and the validator set. Every validator
computes the same table. Owners get chunks in proportion to stake; the
author owns nothing.

The assignment implies, for each chunk and each receiver, an
**upstream**: who should deliver the chunk to that receiver. A chunk's
owner gets it from the author (first hop). Everyone else gets it from
the owner (second hop).

An **obligation** is the set of chunks one upstream owes a receiver.

- When the author has delivered all of our own chunks, its obligation
  is fulfilled, and chorus votes positively.
- When an owner has delivered all of its chunks to us, its obligation
  is fulfilled, and chorus admits that owner's positive vote.

## 5. The proposal header and the encoding scheme

The proposal header is the tuple (encoding scheme, merkle root,
signature), scoped to a slot. It is the minimal data that can be
validated on its own given the validator set, the slot and the proposer
election: the signature names the proposer, the election confirms it
proposes in this slot. That is why an equivocation certificate is
simply two headers of the same proposer with different roots.

To DA the scheme fixes three things:

1. the **packet layout**: segment size, tree depth, symbol size;
2. the **chunk assignment**: the set of valid chunk ids, and who owns
   which chunk;
3. the **codec**: how a message becomes symbols and vice versa.

```rust
trait DAEncodingScheme {
    type Encoder: SymbolEncoder;
    type Decoder: SymbolDecoder;

    fn packet_layout(&self, num_validators: usize) -> Option<PacketLayout>;
    fn chunk_assignment(&self, layout: &PacketLayout, author: &NodeId,
                        validator_data: &ValidatorData) -> ChunkAssignment;
    fn encoder(&self, layout: PacketLayout, num_chunks: usize) -> Self::Encoder;
    fn decoder(&self, layout: PacketLayout, num_chunks: usize) -> Self::Decoder;
}

// sizes only: how the 1440-byte segment splits into header, proof,
// chunk header and symbol, given the tree depth
struct PacketLayout { .. }
impl PacketLayout {
    fn symbol_len(&self) -> usize;
    fn merkle_leaf_hash(&self, chunk_id: WireChunkId, symbol: &[u8]) -> Hash;
}

// a frozen table, identical on every validator
struct ChunkAssignment {
  nodes: Vec<NodeId>,
  targets: Vec<NodeIndex> // targets.len() is num_chunks
}
impl ChunkAssignment {
    fn resolve_chunk_id(&self, chunk_id: u16) -> Option<(ChunkId, Owner)>;
    fn owned_chunks(&self, node: NodeIndex) -> impl Iterator<Item = ChunkId>;
}

trait SymbolEncoder {
    // outputs one symbol per chunk, in chunk id order
    fn encode(&self, message: &[u8]) -> Vec<Bytes>;
}

trait SymbolDecoder {
    fn ingest(&mut self, chunk_id: ChunkId, symbol: &Bytes);
    fn try_decode(&mut self) -> Option<Bytes>;
}
```
