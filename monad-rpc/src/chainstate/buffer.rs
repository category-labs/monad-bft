// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use alloy_consensus::TxEnvelope;
use alloy_rpc_types::{Block, Transaction};
use dashmap::DashMap;
use itertools::Itertools;
use monad_exec_events::BlockCommitState;
use tokio::sync::Mutex;
use tracing::{error, warn};

use crate::{
    event::EventServerEvent,
    types::eth_json::{BlockTags, FixedData},
};

struct TxLoc {
    block_height: u64,
    tx_idx: u64,
}

/// Buffer maintains a capped buffer of blocks.
#[derive(Clone)]
pub struct ChainStateBuffer {
    // Ring buffer holding SeqNums
    block_heights: Arc<Mutex<VecDeque<u64>>>,
    // Capacity of the ring buffer
    block_heights_capacity: usize,

    // Maps a block by its SeqNum
    block_by_height: Arc<DashMap<u64, Block>>,
    // Maps a block by its blockhash
    block_height_by_hash: Arc<DashMap<FixedData<32>, u64>>,
    // Maps a transaction by its hash to a block's height and its index in that block
    tx_loc_by_hash: Arc<DashMap<FixedData<32>, TxLoc>>,

    // The latest voted block's SeqNum
    latest_voted: Arc<AtomicU64>,
    // The latest finalized block's SeqNum
    latest_finalized: Arc<AtomicU64>,
    // The latest proposed block's SeqNum
    latest_proposed: Arc<AtomicU64>,
}

impl ChainStateBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            block_heights: Arc::new(Mutex::new(VecDeque::with_capacity(capacity))),
            block_heights_capacity: capacity,

            block_by_height: Arc::new(DashMap::new()),
            block_height_by_hash: Arc::new(DashMap::new()),
            tx_loc_by_hash: Arc::new(DashMap::new()),

            latest_voted: Arc::new(AtomicU64::new(0)),
            latest_finalized: Arc::new(AtomicU64::new(0)),
            latest_proposed: Arc::new(AtomicU64::new(0)),
        }
    }

    pub async fn insert(&self, block_event: EventServerEvent) {
        let block_event = match block_event {
            EventServerEvent::Block { block, .. } => block,
            _ => return,
        };

        if block_event.commit_state != BlockCommitState::Proposed {
            match block_event.commit_state {
                BlockCommitState::Finalized => {
                    let height = block_event.data.header.number;
                    self.latest_finalized.fetch_max(height, Ordering::SeqCst);
                }
                BlockCommitState::Voted => {
                    let height = block_event.data.header.number;
                    let voted_block_height = self.latest_voted.fetch_max(height, Ordering::SeqCst);

                    if voted_block_height >= height {
                        warn!(?voted_block_height, event_block_height = height, "ChainStateBuffer received voted block event with lower height than existing voted block height");
                    }
                }
                _ => {}
            }
            return;
        }

        let block: Block<Transaction, alloy_rpc_types::Header> = Block {
            header: (**block_event.data.header).clone(),
            transactions: block_event.data.transactions.clone(),
            ..Default::default()
        };

        let block_height = block.header.number;
        let block_hash = block.header.hash;
        let block_tx_hashes = block.transactions.hashes().collect_vec();

        // Check if there's already a block at this height and clean it up
        if let Some(old_block) = self.block_by_height.insert(block_height, block) {
            warn!(
                ?block_height,
                old_hash = ?old_block.header.hash,
                new_hash = ?block_hash,
                "ChainStateBuffer received block event for existing block height, replacing old block"
            );

            // Remove old block's hash from by_hash
            self.block_height_by_hash.remove(&FixedData(old_block.header.hash.0));
        }

        if self
            .block_height_by_hash
            .insert(FixedData(block_hash.0), block_height)
            .is_some()
        {
            warn!(
                ?block_hash,
                "ChainStateBuffer received block event for existing block hash"
            );
        }

        for (tx_idx, tx_hash) in block_tx_hashes.into_iter().enumerate() {
            if self
                .tx_loc_by_hash
                .insert(
                    FixedData(tx_hash.0),
                    TxLoc {
                        block_height,
                        tx_idx: tx_idx as u64,
                    },
                )
                .is_some()
            {
                warn!(
                    ?tx_hash,
                    "ChainStateBuffer received block event with existing transaction hash"
                );
            }
        }

        let voted_block_height = self.latest_voted.fetch_max(block_height, Ordering::SeqCst);

        if voted_block_height >= block_height {
            warn!(
                ?voted_block_height,
                event_block_height = block_height,
                "ChainStateBuffer received voted block event with lower height than existing voted block height"
            );
            return;
        }

        self.latest_proposed.fetch_max(block_height, Ordering::SeqCst);

        let mut block_heights = self.block_heights.lock().await;
        block_heights.push_front(block_height);

        while block_heights.len() > self.block_heights_capacity {
            let Some(evicted_block_height) = block_heights.pop_back() else {
                continue;
            };

            if let Some((_, evicted_block)) = self.block_by_height.remove(&evicted_block_height) {
                match evicted_block.transactions {
                    alloy_rpc_types::BlockTransactions::Full(v) => {
                        v.into_iter().for_each(|tx| {
                            let id = tx.inner.tx_hash();
                            self.tx_loc_by_hash.remove(&FixedData(id.0));
                        });
                    }
                    alloy_rpc_types::BlockTransactions::Hashes(_) => {
                        error!("ChainStateBuffer evicted block transactions contained hashes");
                    }
                    alloy_rpc_types::BlockTransactions::Uncle => {
                        error!("ChainStateBuffer evicted block transactions were uncle");
                    }
                }

                self.block_height_by_hash
                    .remove(&FixedData(evicted_block.header.hash.0));
            }
        }
    }

    pub fn get_block_by_height(&self, height: u64) -> Option<Block> {
        Some(self.block_by_height.get(&height)?.clone())
    }

    pub fn get_block_by_hash(&self, hash: &FixedData<32>) -> Option<Block> {
        let block_height = *self.block_height_by_hash.get(hash)?;

        Some(self.block_by_height.get(&block_height)?.clone())
    }

    pub fn latest_block(&self) -> Option<Block> {
        let finalized_block_height = self.get_latest_finalized_block_num();

        Some(self.block_by_height.get(&finalized_block_height)?.clone())
    }

    pub fn get_transaction_by_hash(&self, hash: &FixedData<32>) -> Option<Transaction<TxEnvelope>> {
        let tx_loc = &*self.tx_loc_by_hash.get(hash)?;

        self.get_transaction_by_location(tx_loc.block_height, tx_loc.tx_idx)
    }

    pub fn get_transaction_by_location(
        &self,
        height: u64,
        idx: u64,
    ) -> Option<Transaction<TxEnvelope>> {
        let block = self.block_by_height.get(&height)?;

        if let alloy_rpc_types::BlockTransactions::Full(transactions) = &block.transactions {
            transactions.get(idx as usize).cloned()
        } else {
            None
        }
    }

    pub fn get_latest_proposed_block_num(&self) -> u64 {
        self.latest_proposed.load(Ordering::SeqCst)
    }

    pub fn get_latest_voted_block_num(&self) -> u64 {
        self.latest_voted.load(Ordering::SeqCst)
    }

    pub fn get_latest_finalized_block_num(&self) -> u64 {
        self.latest_finalized.load(Ordering::SeqCst)
    }
}

pub(super) fn block_height_from_tag(buffer: &ChainStateBuffer, tag: &BlockTags) -> u64 {
    match tag {
        BlockTags::Number(n) => n.0,
        BlockTags::Latest => buffer.get_latest_voted_block_num(),
        BlockTags::Safe => buffer.get_latest_voted_block_num(),
        BlockTags::Finalized => buffer.get_latest_finalized_block_num(),
        BlockTags::Proposed => buffer.get_latest_proposed_block_num(),
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{B256, TxKind, Address};
    use alloy_consensus::{TxEip1559, SignableTransaction};
    use alloy_signer::{SignerSync};
    use alloy_signer_local::PrivateKeySigner;
    use std::sync::Arc;
    use crate::serialize::JsonSerialized;
    use crate::eth_json_types::MonadNotification;
    use monad_types::BlockId;

    #[tokio::test]
    async fn test_many_proposed_blocks() {
        // Buffer receives many proposed blocks in a row and should handle it correctly.
        // Make sure that the ring buffer is correctly updated and the cached values are correctly updated and removed.
        let capacity = 3;
        let buffer = ChainStateBuffer::new(capacity);

        // Generate and propose (capacity + 2) blocks, so oldest blocks are evicted from the ring.
        let total_blocks = capacity + 2;
        for i in 0..total_blocks {
            let height = (i + 1) as u64;
            let block_hash = B256::from([i as u8; 32]);

            // Create a simple transaction
            let tx_inner = TxEip1559 {
                chain_id: 1,
                nonce: 0,
                gas_limit: 21000,
                max_fee_per_gas: 1000,
                max_priority_fee_per_gas: 100,
                to: TxKind::Call(Address::default()),
                value: Default::default(),
                access_list: Default::default(),
                input: vec![].into(),
            };
            let signer = PrivateKeySigner::random();
            let signature = signer.sign_hash_sync(&tx_inner.signature_hash()).unwrap();
            let tx_envelope = tx_inner.into_signed(signature).into();

            let tx = Transaction {
                inner: tx_envelope,
                block_hash: Some(block_hash),
                block_number: Some(height),
                transaction_index: Some(0),
                effective_gas_price: None,
                from: Address::default(),
            };

            // Create an alloy Header with inner field
            let inner_header = alloy_consensus::Header {
                number: height,
                ..Default::default()
            };
            let rpc_header = alloy_rpc_types::Header {
                inner: inner_header,
                hash: block_hash,
                total_difficulty: None,
                size: None,
            };

            // Wrap the header in the event type system
            let serialized_header = JsonSerialized::new_shared(rpc_header);
            let monad_header = MonadNotification {
                block_id: BlockId(monad_types::Hash(block_hash.0)),
                commit_state: BlockCommitState::Proposed,
                data: serialized_header.clone(),
            };
            let serialized_monad_header = JsonSerialized::new_shared(monad_header);

            // Create the block
            let rpc_block = alloy_rpc_types::Block {
                header: serialized_header,
                transactions: alloy_rpc_types::BlockTransactions::Full(vec![tx]),
                uncles: Vec::new(),
                withdrawals: None,
            };
            let serialized_block = JsonSerialized::new_shared(rpc_block);
            let monad_block = MonadNotification {
                block_id: BlockId(monad_types::Hash(block_hash.0)),
                commit_state: BlockCommitState::Proposed,
                data: serialized_block,
            };
            let serialized_monad_block = JsonSerialized::new_shared(monad_block);

            let event = EventServerEvent::Block {
                header: serialized_monad_header,
                block: serialized_monad_block,
                logs: Arc::new(Vec::new()),
            };

            // Propose the block.
            buffer.insert(event).await;

            // Check that the latest proposed height is correct.
            assert_eq!(buffer.get_latest_proposed_block_num(), height);

            // Verify the ring buffer length
            let ring = buffer.block_heights.lock().await;
            let expected_ring_len = if i < capacity { i + 1 } else { capacity };
            assert_eq!(
                ring.len(),
                expected_ring_len,
                "Ring buffer length should be {} at iteration {}", expected_ring_len, i
            );
            drop(ring); // Release the lock before continuing

            // Verify by_height and by_hash buffer lengths match ring buffer
            assert_eq!(
                buffer.block_by_height.len(),
                expected_ring_len,
                "by_height length should be {} at iteration {}", expected_ring_len, i
            );
            assert_eq!(
                buffer.block_height_by_hash.len(),
                expected_ring_len,
                "by_hash length should be {} at iteration {}", expected_ring_len, i
            );

            // Check the block is accessible by height if within ring.
            if i >= capacity {
                // The block at height (height - capacity) should be evicted.
                let evicted_height = height - capacity as u64;
                assert!(buffer.block_by_height.get(&evicted_height).is_none(),
                    "Block at height {} should be evicted", evicted_height);
            }
            assert!(buffer.block_by_height.get(&height).is_some(),
                "Block at height {} should be present", height);
        }

        // Verify final ring buffer length
        let ring = buffer.block_heights.lock().await;
        assert_eq!(
            ring.len(),
            capacity,
            "Final ring buffer length should be {}", capacity
        );
        drop(ring);

        // Verify final by_height and by_hash buffer lengths
        assert_eq!(
            buffer.block_by_height.len(),
            capacity,
            "Final by_height length should be {}", capacity
        );
        assert_eq!(
            buffer.block_height_by_hash.len(),
            capacity,
            "Final by_hash length should be {}", capacity
        );

        // Now verify that only the last 'capacity' blocks remain.
        for i in 0..total_blocks {
            let height = (i + 1) as u64;
            if i < total_blocks - capacity {
                // These should have been evicted from the ring.
                assert!(buffer.block_by_height.get(&height).is_none(),
                    "Block at height {} should be evicted", height);
            } else {
                // These should still be present.
                assert!(buffer.block_by_height.get(&height).is_some(),
                    "Block at height {} should still be present", height);
            }
        }
    }

    #[tokio::test]
    async fn test_duplicate_height_different_hash() {
        // Test inserting two proposed blocks with the same height but different hashes.

        let capacity = 5;
        let buffer = ChainStateBuffer::new(capacity);

        let height = 1u64;

        // Create first block at height 1 with hash A
        let block_hash_a = B256::from([1u8; 32]);
        let event_a = create_test_block_event(height, block_hash_a);
        buffer.insert(event_a).await;

        // Verify initial state
        assert_eq!(buffer.block_by_height.len(), 1, "Should have 1 entry in by_height after first insert");
        assert_eq!(buffer.block_height_by_hash.len(), 1, "Should have 1 entry in by_hash after first insert");
        assert_eq!(
            buffer.block_by_height.len(),
            buffer.block_height_by_hash.len(),
            "by_height and by_hash lengths should match after first insert"
        );

        // Create second block at the same height 1 but with different hash B
        let block_hash_b = B256::from([2u8; 32]);
        let event_b = create_test_block_event(height, block_hash_b);
        buffer.insert(event_b).await;

        assert_eq!(
            buffer.block_by_height.len(),
            buffer.block_height_by_hash.len(),
            "by_height and by_hash lengths should match after duplicate height insert"
        );

        // Verify the block stored at height 1 is the most recent one (block B)
        let stored_block = buffer.block_by_height.get(&height).expect("Block should exist at height 1");
        assert_eq!(
            stored_block.header.hash,
            block_hash_b,
            "Block at height 1 should be the most recently inserted block (hash B)"
        );

        let hash_a_exists = buffer.block_height_by_hash.get(&FixedData(block_hash_a.0)).is_some();
        let hash_b_exists = buffer.block_height_by_hash.get(&FixedData(block_hash_b.0)).is_some();

        println!("Hash A exists in by_hash: {}", hash_a_exists);
        println!("Hash B exists in by_hash: {}", hash_b_exists);

        assert!(
            hash_b_exists,
            "New hash B should exist in by_hash"
        );
    }

    // Helper function to create a test block event
    fn create_test_block_event(height: u64, block_hash: B256) -> EventServerEvent {
        // Create a simple transaction
        let tx_inner = TxEip1559 {
            chain_id: 1,
            nonce: 0,
            gas_limit: 21000,
            max_fee_per_gas: 1000,
            max_priority_fee_per_gas: 100,
            to: TxKind::Call(Address::default()),
            value: Default::default(),
            access_list: Default::default(),
            input: vec![].into(),
        };
        let signer = PrivateKeySigner::random();
        let signature = signer.sign_hash_sync(&tx_inner.signature_hash()).unwrap();
        let tx_envelope = tx_inner.into_signed(signature).into();

        let tx = Transaction {
            inner: tx_envelope,
            block_hash: Some(block_hash),
            block_number: Some(height),
            transaction_index: Some(0),
            effective_gas_price: None,
            from: Address::default(),
        };

        // Create an alloy Header with inner field
        let inner_header = alloy_consensus::Header {
            number: height,
            ..Default::default()
        };
        let rpc_header = alloy_rpc_types::Header {
            inner: inner_header,
            hash: block_hash,
            total_difficulty: None,
            size: None,
        };

        // Wrap the header in the event type system
        let serialized_header = JsonSerialized::new_shared(rpc_header);
        let monad_header = MonadNotification {
            block_id: BlockId(monad_types::Hash(block_hash.0)),
            commit_state: BlockCommitState::Proposed,
            data: serialized_header.clone(),
        };
        let serialized_monad_header = JsonSerialized::new_shared(monad_header);

        // Create the block
        let rpc_block = alloy_rpc_types::Block {
            header: serialized_header,
            transactions: alloy_rpc_types::BlockTransactions::Full(vec![tx]),
            uncles: Vec::new(),
            withdrawals: None,
        };
        let serialized_block = JsonSerialized::new_shared(rpc_block);
        let monad_block = MonadNotification {
            block_id: BlockId(monad_types::Hash(block_hash.0)),
            commit_state: BlockCommitState::Proposed,
            data: serialized_block,
        };
        let serialized_monad_block = JsonSerialized::new_shared(monad_block);

        EventServerEvent::Block {
            header: serialized_monad_header,
            block: serialized_monad_block,
            logs: Arc::new(Vec::new()),
        }
    }
}