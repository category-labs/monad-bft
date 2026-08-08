# Full Node Reputation Scoring

## Motivation

This spec introduces a reputation scoring system so that validators can account for full node behavior when selecting downstream peers for secondary raptorcast groups.

## Background

When receiving raptorcast chunks, the `DecoderCache` tracks messages in two phases:

1. `DecoderState` accumulates chunks until the original message can be recovered. Maintains a `recipient_chunks: BTreeMap<NodeIdHash, usize>` field tracking how many chunks were received from each node.
2. `RecentlyDecodedState` created once decoding succeeds and stored in an LRU cache.

Only `Secondary` mode chunks are relevant here, as they represent full node participation in rebroadcasting.

### Chunk flow in secondary raptorcast

1. Validator encodes a message into N chunks using `Partitioned::from_homogeneous_peers()`. Each chunk is assigned to exactly one full node, with the assignee's hash written into the chunk header as `recipient_hash`.
2. Validator sends each chunk directly to its assigned full node.
3. Assigned full node receives the chunk, sees `self_hash == recipient_hash`, and rebroadcasts it to all other full nodes in the group (via `iterate_rebroadcast_peers`).
4. Other full nodes receive the rebroadcast. The chunk retains its original `recipient_hash` (the assignee). These nodes feed it into `DecoderCache` but do not rebroadcast further (since `self_hash != recipient_hash`).

The validator has no direct visibility into whether full nodes actually rebroadcast. All observation of rebroadcast behavior happens on the receiving full nodes via `recipient_chunks`. On a receiving full node B, `recipient_chunks[A]` means the number of chunks originally assigned to full node A that reached me, which can be used to measure A's rebroadcast behavior.

## Design

### 1. Filter and preserve `recipient_chunks` for secondary mode only

The `BroadcastMode::Secondary` filter is applied at the `DecoderState` level, since `DecoderCache` is shared across both primary and secondary raptorcast. The `CacheKey` is `(app_message_hash, author)`, so chunks from both modes can land in the same decoder state. Filtering per-chunk inside `handle_message` ensures only secondary rebroadcast behavior is tracked:

```rust
impl DecoderState {
    pub fn handle_message<PT>(&mut self, message: &ValidatedMessage<PT>) -> Result<(), InvalidSymbol> {
        self.validate_symbol(message)?;
        let symbol_id = message.chunk_id.into();
        self.seen_esis.set(symbol_id, true);
        self.decoder.received_encoded_symbol(&message.chunk, symbol_id);
        // only track secondary raptorcast chunks for reputation scoring
        if message.broadcast_mode == BroadcastMode::Secondary {
            *self.recipient_chunks.entry(message.recipient_hash).or_insert(0) += 1;
        }
        Ok(())
    }
}
```

Carry over the filtered `recipient_chunks` from `DecoderState` into `RecentlyDecodedState` so that chunk contribution data survives message decoding:

```rust
struct RecentlyDecodedState {
    symbol_len: usize,
    app_message_len: usize,
    seen_esis: BitVec<usize, Lsb0>,
    excess_chunk_count: usize,
    // continue to track secondary raptorcast chunks
    recipient_chunks: BTreeMap<NodeIdHash, usize>,
}
```

Continue recording in `RecentlyDecodedState::handle_message` for chunks that arrive after decoding, so late-arriving chunks are still credited:

```rust
impl RecentlyDecodedState {
    pub fn handle_message<PT>(&mut self, message: &ValidatedMessage<PT>) -> Result<(), InvalidSymbol> {
        // ... existing validation ...
        if message.broadcast_mode == BroadcastMode::Secondary {
            *self.recipient_chunks.entry(message.recipient_hash).or_insert(0) += 1;
        }
        // ...
    }
}
```

### 2. Export chunk data periodically

Chunk contribution data is exported from `DecoderCache` to the secondary raptorcast `Client`:

```rust
struct ChunkContribution {
    peer_chunks: BTreeMap<NodeIdHash, usize>,
}
```

Each `RecentlyDecodedState` is marked as exported to prevent double-counting.


### 3. Compute participation rate

For each exported `ChunkContribution`, `total_received_chunks` is the sum of all values in `peer_chunks`. This is the total number of secondary chunks that reached this full node for a given message. The full node does not know the validator's redundancy factor, so `total_received_chunks` serves as the baseline rather than an absolute expected count.

Since chunk distribution is equal among secondary raptorcast, the expected contribution per peer is:

```
expected_chunks_per_peer = total_received_chunks / group_size
```

A peer's participation rate for a single message is:

```
participation_rate = received_chunks_from_peer / expected_chunks_per_peer
```

The overall reputation score for a peer is the average participation rate across all messages where that peer was a group member:

```
reputation(peer) = sum(participation_rate_i) / num_messages_with_peer
```

### 4. Report reputation data to validators

Full nodes report participation data to validators via a periodic report message every group round span.

```rust
struct PeerParticipationReport<PT: PubKey> {
    pub validator_id: NodeId<PT>,
    pub round_span: RoundSpan,
    pub peer_scores: LimitedVec<PeerParticipation, MAX_PEERS_IN_CONFIRM_GROUP>,
}

struct PeerParticipation {
    pub peer: NodeId,
    pub participation_rate: f64,
}
```

The report contains the averaged participation rate for all peers the full node has observed during the round span of the group. The validator uses this data when deciding which peers to select for the upcoming secondary raptorcast group. The report is sent periodically after every group round span.

### 5. Validator-side aggregation via median

The validator receives participation reports from multiple full nodes in the same group. To prevent malicious full nodes from manipulating scores, the validator computes the **median** reported participation rate for each peer:

```
final_score(peer) = median(reported_participation_rate from all reporters in same group)
```

Median is robust against outlier manipulation. A single malicious reporter (or small minority) cannot skew the result. However, smaller groups will be more fragile to manipulation.

### 6. Publisher-side filtering and chunk allocation

The `Publisher` module uses aggregated reputation scores to influence group formation and chunk distribution:
- New peers start at reputation score 0 (neutral)
- Peers that fail to rebroadcast (participation rate consistently below a threshold, e.g., 0.5) are dropped from future candidate list


## Considerations

### Score decay

Reputation scores should decay over time so that:
- Peers that stop participating eventually lose their reputation
- Historical good behavior does not permanently shield a peer from accountability
- The scoring window reflects recent network conditions

### Double-counting prevention

Each `RecentlyDecodedState` must only contribute to reputation scoring once. Track whether each entry has been exported with a boolean flag to prevent the same message from being counted multiple times across export cycles.

### Sybil cost

A sybil attacker spinning up fresh identities always starts at reputation 0. Each new identity:
- Gets admitted to a group
- Fails to rebroadcast, accumulating negative reputation after one group rotation
- Gets dropped from future groups

In the future, it would be beneficial to either allocate a smaller number of chunks to full nodes with zero reputation, or to limit the number of zero reputation full nodes in a group.
