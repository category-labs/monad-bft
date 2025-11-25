# Forkpoint

A forkpoint is a checkpoint that captures the consensus state at a specific point in time, enabling a node to safely restart from a known valid state. It serves as a recovery point that includes all necessary information to resume consensus without replaying the entire chain history.

## Schema

```rust
pub struct Checkpoint<ST, SCT, EPT> {
    pub root: BlockId,
    pub high_certificate: RoundCertificate<ST, SCT, EPT>,
    pub validator_sets: Vec<LockedEpoch>,
}
```

A forkpoint wraps a `Checkpoint<ST, SCT, EPT>` structure containing:

- **`root: BlockId`** Block id of blocktree root, which points to the highest committed block
- **`high_certificate: RoundCertificate<ST, SCT, EPT>`** The highest quorum certificate (QC) or timeout certificate (TC) known to the node. High certificate is the proof to enter the next round
- **`validator_sets: Vec<LockedEpoch>`** Information about locked validator sets, where each `LockedEpoch` contains:
  - `epoch: Epoch` The epoch for which this validator set is active
  - `round: Round` The round at which this epoch is scheduled to start (determined by the committed boundary block from the previous epoch)

The validator sets vector typically contains only 1 entry, for the current epoch. When consensus is in the epoch lock stage, it contains an extra entry for the next epoch.

## Node Startup

On startup, a node loads the forkpoint from `forkpoint.rlp` (or `forkpoint.toml` as a human-readable fallback) and uses it to initialize consensus:

- **`root`**: Root of the blocktree
- **`high_certificate`**: Consensus starts on the next round of high certificate (`high_certificate.round() + 1`) or higher, preventing double voting for any round at or below it
- **`validator_sets`**: Provides the locked validator sets needed to verify certificates and participate in consensus, particularly important during epoch transitions

The forkpoint is validated on load to ensure the high certificate can be verified against the provided validator sets.

# Syncing

A node undergoes 3 stages of syncing on startup:

1. **BlockSync**: Fetches `2 * execution_delay` blocks working backward from the `root`, required for TFM reserve balance checking
2. **StateSync**: Synchronizes the database to version `root - execution_delay` using the delayed execution result from the `root` block as the target state root. While statesync is ongoing, the node listens for all proposals and stores them into the block buffer
3. **Block replay**: After both syncs complete, initializes the consensus blocktree with `root` and `high_certificate`, and processes all blocks from the block buffer


## Update Frequency

Forkpoints are generated and persisted to disk whenever:

- The node enters a new round (`EnterRound` command)
- Blocks are finalized (`CommitBlocks` command with a finalized block)

Each update writes both a binary `forkpoint.rlp` file (primary, using RLP encoding) and a `forkpoint.toml` file (human-readable, using TOML serialization). Backup copies are also saved with the format `forkpoint.<ext>.{root_seq_num}.{high_cert_round}`
