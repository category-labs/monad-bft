# Sybil-Resistant Transaction Ingestion

## Table of Contents

- [Sybil-Resistant Transaction Ingestion](#sybil-resistant-transaction-ingestion)
  - [Table of Contents](#table-of-contents)
  - [Rationale](#rationale)
  - [Summary](#summary)
  - [Peer Score](#peer-score)
    - [Score Computation](#score-computation)
    - [Contribution Recording](#contribution-recording)
    - [Two-Tier Identity Storage](#two-tier-identity-storage)
  - [Weighted Fair Queue](#weighted-fair-queue)
    - [Dual-Pool Architecture](#dual-pool-architecture)
    - [Weighted Fair Queueing](#weighted-fair-queueing)
    - [Priority Pool Fallback](#priority-pool-fallback)
  - [LeanUDP Fragmentation](#leanudp-fragmentation)
    - [Packet Format](#packet-format)
    - [Fragment Reassembly](#fragment-reassembly)
    - [Reassembly Pool Eviction](#reassembly-pool-eviction)
  - [Impact on Other Components](#impact-on-other-components)
  - [Security Analysis](#security-analysis)
    - [Naive Flooding](#naive-flooding)
    - [Mempool Monopolization](#mempool-monopolization)
    - [Graceful Degradation](#graceful-degradation)

## Rationale

The current implementation processes transactions in arrival order without identity-based prioritization. This creates two vulnerabilities:

1. Bandwidth exhaustion: An attacker flooding the network can consume the entire ingestion pipeline, causing honest transactions to be delayed or dropped.

2. Unfair resource allocation: High-volume senders receive bandwidth proportional to their submission rate rather than their contribution to the network.

## Summary

This specification defines a sybil-resistant transaction ingestion protocol that allocates bandwidth fairly among authenticated peers based on their contributions to the network.

```mermaid
flowchart LR
    subgraph Ingestion Pipeline
        DP[Dataplane] --> LU[LeanUDP]
        LU --> FQ[FairQueue]
        FQ --> TP[TxPool]
    end

    subgraph Reputation
        PS[PeerScore]
    end

    TP -->|record contribution| PS
    PS -->|score lookup| LU
    PS -->|score lookup| FQ
```

The protocol consists of three components:

1. Peer Score: A reputation system that tracks peer contributions over time, computing scores that reflect both contribution history and recency of activity.

2. Weighted Fair Queue: A dual-pool queue that separates authenticated identities with positive reputation from unknown identities, allocating bandwidth proportionally to identity scores within the priority pool.

3. LeanUDP Fragmentation: A message fragmentation layer that integrates with peer scores.

These components ensure that peers who have consistently contributed valid transactions to the network receive proportionally higher bandwidth allocation, while unknown or low-reputation peers are rate-limited to a configurable fraction of total capacity.

## Peer Score

An identity is a public key that owns a wireauth session. All reputation tracking uses this public key as the identifier. The number of identities per IP address is limited to prevent a single IP from creating unbounded sessions.

### Score Computation

```
gas_contribution = min(total_gas_spent, max_gas_contribution)
time_weight = min((time_known / time_weight_unit)^2, max_time_weight)
decay = 0.5^(idle_time / decay_half_life)
score = gas_contribution × time_weight × decay
```

Gas contribution measures useful work by summing gas spent on transactions included in blocks. Time weight grows quadratically from 0 to 1 over 1 hour, giving established identities a 3,600x advantage over 1-minute-old ones. Quadratic growth offsets attacks using short-term IP rental. Decay halves score every 30 minutes of inactivity, requiring consistent uptime to maintain priority. The gas contribution cap ensures that gaining more bandwidth requires acquiring more identities and IPs.

Default parameters:

| Parameter | Value | Description |
|-----------|-------|-------------|
| time_weight_unit | 1 hour | Time for weight to reach 1.0 |
| max_time_weight | 1.0 | Cap on time multiplier |
| max_gas_contribution | 100M gas | Cap on gas that counts toward score |
| decay_half_life | 30 minutes | Continuous decay; score halves every 30 minutes of inactivity |
| promotion_threshold | 1M | Score required to enter promoted pool and priority queue (1% of max_gas_contribution) |
| max_identities | 100,000 | Eviction threshold for least recently active |

### Contribution Recording

Contributions are recorded as gas spent per identity per block. When a block includes transactions from an identity, the total gas used by those transactions is added to the identity's cumulative gas contribution, up to the cap.

Gas-based counting ties attack cost directly to on-chain fees. The cap forces attackers to acquire more identities (IPs) rather than outspending honest users from a single identity.

The scoring model intentionally avoids negative reputation (penalties for invalid transactions or spam). This keeps the model simple. Penalties may be introduced in future iterations if the positive-only approach proves insufficient against observed attack patterns.

### Two-Tier Identity Storage

To prevent identity table eviction attacks, the scorer maintains two separate pools:

| Pool | Capacity | Entry Condition | Eviction |
|------|----------|-----------------|----------|
| Promoted | 90,000 | score ≥ promotion_threshold | LRU within pool |
| Newcomers | 10,000 | score < promotion_threshold | LRU within pool |

New identities enter the newcomers pool. When score crosses the promotion threshold, the identity moves to the promoted pool. Flooding with new identities only evicts other newcomers; promoted peers remain protected.

Scores are not persisted across restarts. On node restart, all identities start fresh with zero score. Persistence may be considered in future iterations.

## Weighted Fair Queue

### Dual-Pool Architecture

The fair queue maintains two separate pools with distinct policies:

| Property | Priority Pool | Regular Pool |
|----------|---------------|--------------|
| Bandwidth share | 90% | 10% |
| Scheduling | Weighted by score | Equal weights |
| Max size | 100,000 | 100,000 |

Incoming transactions are routed based on authentication status and score. Authenticated identities with scores at or above `promotion_threshold` enter the priority pool; all others enter the regular pool.

The bandwidth split between pools defaults to 90/10. During dequeue, a counter cycles to probabilistically select which pool to serve, achieving the configured ratio over time.

### Weighted Fair Queueing

Within each pool, bandwidth is allocated using weighted fair queueing (WFQ). Each identity maintains a virtual finish time that determines scheduling priority:

```
finish_time(i) = virtual_time + (1 / score(i))
```

When a transaction is dequeued, the global virtual time advances to the finish time of the selected identity, and that identity's finish time is recalculated for its next queued transaction.

The algorithm guarantees that over time, each identity receives bandwidth proportional to its score. An identity with score 10 receives 10x the bandwidth of an identity with score 1.

### Priority Pool Fallback

When the priority pool cannot accept a transaction, promoted identities fall back to the regular pool rather than being rejected. This provides a safety mechanism if scoring is abused to block new identities.

## LeanUDP Fragmentation

LeanUDP is a lightweight protocol for fragmenting and reassembling large messages over plain UDP. It operates over authenticated UDP sessions and is designed for fast, short-lived message delivery where all fragments arrive within a narrow time window. Unlike TCP, it provides no retransmission or flow control; incomplete messages are discarded after a brief timeout.

### Packet Format

Large messages are fragmented for transmission over authenticated UDP sessions. Each fragment carries an 8-byte header:

| Field | Size | Description |
|-------|------|-------------|
| Version | 1 byte | Protocol version (currently 1) |
| Message ID | 3 bytes | Identifies fragments belonging to the same message within a single identity |
| Sequence Number | 2 bytes | Fragment index within message (0-65535) |
| Flags | 2 bytes | START (0x0001) and END (0x0002) flags indicating message boundaries |

Fragment types derived from flags:

| START | END | Type | Description |
|-------|-----|------|-------------|
| 1 | 1 | Complete | Single-fragment message |
| 1 | 0 | Start | First fragment of multi-fragment message |
| 0 | 0 | Middle | Interior fragment |
| 0 | 1 | End | Final fragment |

With default MTU of 1500 bytes, reserving 20 bytes for IP header, 8 bytes for UDP header, 32 bytes for wireauth header, and 8 bytes for leanudp header, maximum fragment payload is 1432 bytes.

Maximum message size is 128 KB (131,072 bytes), requiring at most 92 fragments (ceil(131072 / 1432)).

### Fragment Reassembly

The decoder maintains separate reassembly buffers for priority and regular traffic. Fragments from identities with scores at or above `promotion_threshold` are placed in priority buffers; others go to regular buffers with stricter limits.

Messages are keyed by `(identity, message_id)` tuple. The 3-byte message ID only needs to be unique per identity, not globally. This allows each identity to have up to 16M concurrent message IDs while keeping the header compact.

Default parameters:

| Parameter | Default | Description |
|-----------|---------|-------------|
| max_fragment_payload | 1432 | Bytes per fragment |
| max_message_size | 128 KB | Maximum assembled message size |
| max_fragments_per_message | 92 | Maximum fragments per message (128 KB / 1432) |
| max_priority_messages | 10,000 | Concurrent priority reassemblies |
| max_regular_messages | 1,000 | Concurrent regular reassemblies |
| max_messages_per_identity | 10 | Concurrent messages per identity |
| message_timeout | 100ms | After timeout, message is evicted when space is needed |

### Reassembly Pool Eviction

The priority pool uses timeout-based eviction, assuming most peers complete reassembly promptly—if malicious peers hold timers intentionally, the buffer fills and falls back to the regular pool. The regular pool uses random eviction, providing the same guarantees that exist without scoring.

## Impact on Other Components

Peer discovery: A new port tag `AuthTxIngestion = 3` with default port 8002. Nodes advertise this port in their name record to indicate support for authenticated transaction ingestion.

Transaction pool: The forwarding manager integrates the fair queue for ingress scheduling. Transactions are dequeued in timed chunks for processing as before.

Raptorcast: LeanUDP operates over authenticated UDP sessions and added as separate protocol in raptorcast router.

## Security Analysis

The protocol aims to prevent two attack classes:

### Naive Flooding

Attacker generates high transaction volumes to exhaust txpool resources.

Prevention: peer scoring and WFQ limit low-score peers to 10% of total bandwidth.

### Mempool Monopolization

Attacker floods txpool to crowd out competitors.

Prevention: quadratic time weight makes it infeasible to acquire sufficient score short-term. Instant attacks (1min prep) require 3,600x more IPs than prepared attacks (1h prep), forcing attackers to run continuous operations with ongoing IP and gas costs.

Assuming 1000 honest relays at gas cap (100M gas each), AWS IP cost $3.60/month, $0.23 gas cost per identity to reach 100M cap:

| Target BW | Attacker Prep | IPs Needed | IP Cost | Gas Cost | Total |
|-----------|---------------|------------|---------|----------|-------|
| 50% | 1h | 1,000 | $3,600/mo | $230 | $3,830/mo |
| 50% | 1min | 3,600,000 | ~0 | $828,000 | $828,000 |
| 80% | 1h | 4,000 | $14,400/mo | $920 | $15,320/mo |
| 80% | 1min | 14,400,000 | ~0 | $3,312,000 | $3,312,000 |

Due to quadratic time weight, instant attacks are 3,600x more expensive than prepared attacks.

### Graceful Degradation

When the scoring system itself is under attack (e.g., priority pools exhausted, reassembly buffers full), the protocol falls back to pre-scoring behavior rather than failing completely. Promoted identities overflow to regular pools, and regular pools use random eviction. This ensures that an attack on the prioritization layer cannot cause worse outcomes than having no prioritization at all.
