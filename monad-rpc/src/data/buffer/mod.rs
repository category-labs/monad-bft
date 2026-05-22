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

use alloy_primitives::{BlockHash, TxHash};
use alloy_rpc_types::{Block, Transaction, TransactionReceipt};
use dashmap::DashMap;
use itertools::Itertools;
use monad_exec_events::BlockCommitState;
use tracing::{error, warn};

pub use self::view::BlockBufferView;
use crate::event::EventServerEvent;

mod view;

struct BlockBufferTxLoc {
    block_height: u64,
    tx_idx: u64,
}

struct BlockBufferBlockStates {
    voted: AtomicU64,
    finalized: AtomicU64,
    proposed: AtomicU64,
}

pub struct BlockBuffer {
    block_states: Arc<BlockBufferBlockStates>,

    // Ring buffer holding SeqNums
    block_heights: VecDeque<u64>,
    // Capacity of the ring buffer
    block_heights_capacity: usize,

    // Maps a block by its SeqNum
    block_by_height: Arc<DashMap<u64, Block>>,
    // Maps a block by its blockhash
    block_height_by_hash: Arc<DashMap<BlockHash, u64>>,
    // Maps a transaction hash to its block location and receipt
    tx_by_hash: Arc<DashMap<TxHash, (BlockBufferTxLoc, TransactionReceipt)>>,
}

impl BlockBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            block_states: Arc::new(BlockBufferBlockStates {
                voted: AtomicU64::default(),
                finalized: AtomicU64::default(),
                proposed: AtomicU64::default(),
            }),

            block_heights: VecDeque::with_capacity(capacity),
            block_heights_capacity: capacity,

            block_by_height: Arc::new(DashMap::new()),
            block_height_by_hash: Arc::new(DashMap::new()),
            tx_by_hash: Arc::new(DashMap::new()),
        }
    }

    pub fn create_view(&self) -> BlockBufferView {
        BlockBufferView {
            block_states: self.block_states.clone(),
            block_by_height: self.block_by_height.clone(),
            block_height_by_hash: self.block_height_by_hash.clone(),
            tx_by_hash: self.tx_by_hash.clone(),
        }
    }

    pub fn insert(&mut self, block_event: EventServerEvent) {
        let (commit_state, header, transactions) = match block_event {
            EventServerEvent::Block {
                commit_state,
                header,
                transactions,
            } => (commit_state, header, transactions),
            EventServerEvent::Gap => return,
        };

        let block_height = header.data.number;
        let block_hash = header.data.hash;

        match commit_state {
            BlockCommitState::Verified => {
                return;
            }
            BlockCommitState::Finalized => {
                self.block_states
                    .finalized
                    .fetch_max(block_height, Ordering::SeqCst);

                if self.block_height_by_hash.contains_key(&block_hash) {
                    return;
                }
            }
            BlockCommitState::Voted => {
                let voted_block_height = self
                    .block_states
                    .voted
                    .fetch_max(block_height, Ordering::SeqCst);

                if block_height < voted_block_height {
                    warn!(
                        ?voted_block_height,
                        event_block_height = block_height,
                        "BlockBuffer received voted block event with lower height than existing voted block height"
                    );
                }

                if self.block_height_by_hash.contains_key(&block_hash) {
                    return;
                }
            }
            BlockCommitState::Proposed => {
                self.block_states
                    .proposed
                    .fetch_max(block_height, Ordering::SeqCst);
            }
        }

        let block: Block<Transaction, alloy_rpc_types::Header> = Block {
            header: header.data.value().clone(),
            transactions: alloy_rpc_types::BlockTransactions::Full(
                transactions
                    .iter()
                    .map(|(tx, _, _)| tx.value().clone())
                    .collect_vec(),
            ),
            uncles: Vec::default(),
            withdrawals: None,
        };

        // Check if there's already a block at this height and clean it up
        if let Some(old_block) = self.block_by_height.insert(block_height, block) {
            warn!(
                ?block_height,
                old_hash = ?old_block.header.hash,
                new_hash = ?block_hash,
                "BlockBuffer received block event for existing block height, replacing old block"
            );

            // Remove old block's hash from by_hash
            self.block_height_by_hash.remove(&old_block.header.hash);

            // Remove old block's transactions from tx_loc_by_hash
            if let alloy_rpc_types::BlockTransactions::Full(txs) = &old_block.transactions {
                for tx in txs {
                    self.tx_by_hash.remove(tx.inner.tx_hash());
                }
            } else {
                error!(
                    ?block_height,
                    old_hash =?old_block.header.hash,
                    "BlockBuffer stored block without full transactions"
                );
            }
        }

        if self
            .block_height_by_hash
            .insert(block_hash, block_height)
            .is_some()
        {
            warn!(
                ?block_hash,
                "BlockBuffer received block event for existing block hash"
            );
        }

        for (tx_idx, tx_receipt) in transactions
            .iter()
            .map(|(_, tx_receipt, _)| tx_receipt)
            .enumerate()
        {
            let tx_hash = tx_receipt.transaction_hash;

            if self
                .tx_by_hash
                .insert(
                    tx_receipt.transaction_hash,
                    (
                        BlockBufferTxLoc {
                            block_height,
                            tx_idx: tx_idx as u64,
                        },
                        tx_receipt.value().clone(),
                    ),
                )
                .is_some()
            {
                warn!(
                    ?tx_hash,
                    "BlockBuffer received block event with existing transaction hash"
                );
            }
        }

        self.block_heights.push_front(block_height);

        while self.block_heights.len() > self.block_heights_capacity {
            let Some(evicted_block_height) = self.block_heights.pop_back() else {
                break;
            };

            if let Some((_, evicted_block)) = self.block_by_height.remove(&evicted_block_height) {
                match evicted_block.transactions {
                    alloy_rpc_types::BlockTransactions::Full(v) => {
                        v.into_iter().for_each(|tx| {
                            let id = tx.inner.tx_hash();
                            self.tx_by_hash.remove(id);
                        });
                    }
                    alloy_rpc_types::BlockTransactions::Hashes(_) => {
                        error!("BlockBuffer evicted block transactions contained hashes");
                    }
                    alloy_rpc_types::BlockTransactions::Uncle => {
                        error!("BlockBuffer evicted block transactions were uncle");
                    }
                }

                self.block_height_by_hash.remove(&evicted_block.header.hash);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use alloy_consensus::{
        transaction::Recovered, Eip658Value, Receipt, ReceiptEnvelope, ReceiptWithBloom,
        SignableTransaction, TxEip1559, TxEnvelope,
    };
    use alloy_primitives::{Address, Bloom, TxKind, B256};
    use alloy_rpc_types::TransactionReceipt;
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;
    use monad_types::BlockId;

    use super::*;
    use crate::types::{
        eth_json::{BlockTags, MonadNotification},
        serialize::JsonSerialized,
    };

    #[test]
    fn test_many_proposed_blocks() {
        // Buffer receives many proposed blocks in a row and should handle it correctly.
        // Make sure that the ring buffer is correctly updated and the cached values are correctly updated and removed.
        let capacity = 3;
        let mut buffer = BlockBuffer::new(capacity);

        let view = buffer.create_view();

        // Generate and propose (capacity + 2) blocks, so oldest blocks are evicted from the ring.
        let total_blocks = capacity + 2;
        for i in 0..total_blocks {
            let height = (i + 1) as u64;
            let block_hash = B256::from([i as u8; 32]);

            let event = create_test_block_event(height, block_hash, &[0]);
            buffer.insert(event);

            // Check that the latest proposed height is correct.
            assert_eq!(view.get_latest_proposed_block_num(), height);
            assert_eq!(
                view.resolve_block_height_from_tag(&BlockTags::Latest),
                height
            );

            // Verify the ring buffer length
            let expected_ring_len = if i < capacity { i + 1 } else { capacity };
            assert_eq!(
                buffer.block_heights.len(),
                expected_ring_len,
                "Ring buffer length should be {} at iteration {}",
                expected_ring_len,
                i
            );

            // Verify by_height and by_hash buffer lengths match ring buffer
            assert_eq!(
                buffer.block_by_height.len(),
                expected_ring_len,
                "by_height length should be {} at iteration {}",
                expected_ring_len,
                i
            );
            assert_eq!(
                buffer.block_height_by_hash.len(),
                expected_ring_len,
                "by_hash length should be {} at iteration {}",
                expected_ring_len,
                i
            );

            // Check the block is accessible by height if within ring.
            if i >= capacity {
                // The block at height (height - capacity) should be evicted.
                let evicted_height = height - capacity as u64;
                assert!(
                    buffer.block_by_height.get(&evicted_height).is_none(),
                    "Block at height {} should be evicted",
                    evicted_height
                );
            }
            assert!(
                buffer.block_by_height.get(&height).is_some(),
                "Block at height {} should be present",
                height
            );
        }

        // Verify final ring buffer length
        assert_eq!(
            buffer.block_heights.len(),
            capacity,
            "Final ring buffer length should be {}",
            capacity
        );

        // Verify final by_height and by_hash buffer lengths
        assert_eq!(
            buffer.block_by_height.len(),
            capacity,
            "Final by_height length should be {}",
            capacity
        );
        assert_eq!(
            buffer.block_height_by_hash.len(),
            capacity,
            "Final by_hash length should be {}",
            capacity
        );

        // Now verify that only the last 'capacity' blocks remain.
        for i in 0..total_blocks {
            let height = (i + 1) as u64;
            if i < total_blocks - capacity {
                // These should have been evicted from the ring.
                assert!(
                    buffer.block_by_height.get(&height).is_none(),
                    "Block at height {} should be evicted",
                    height
                );
            } else {
                // These should still be present.
                assert!(
                    buffer.block_by_height.get(&height).is_some(),
                    "Block at height {} should still be present",
                    height
                );
            }
        }
    }

    #[test]
    fn test_duplicate_height_different_hash() {
        // Test inserting two proposed blocks with the same height but different hashes.

        let capacity = 5;
        let mut buffer = BlockBuffer::new(capacity);

        let view = buffer.create_view();

        let height = 1u64;

        // Create first block at height 1 with hash A
        let block_hash_a = B256::from([1u8; 32]);
        let event_a = create_test_block_event(height, block_hash_a, &[0]);
        buffer.insert(event_a);

        // Capture block A's tx hash before it gets replaced
        let tx_hash_a = match &buffer.block_by_height.get(&height).unwrap().transactions {
            alloy_rpc_types::BlockTransactions::Full(txs) => *txs[0].inner.tx_hash(),
            _ => panic!("expected full transactions"),
        };

        // Verify initial state
        assert_eq!(
            view.block_by_height.len(),
            1,
            "Should have 1 entry in by_height after first insert"
        );
        assert_eq!(
            view.block_height_by_hash.len(),
            1,
            "Should have 1 entry in by_hash after first insert"
        );
        assert_eq!(
            view.tx_by_hash.len(),
            1,
            "Should have 1 entry in tx_loc_by_hash after first insert"
        );
        assert!(
            view.get_transaction_by_hash(&tx_hash_a).is_some(),
            "Block A's transaction should be findable by hash"
        );

        // Create second block at the same height 1 but with different hash B
        let block_hash_b = B256::from([2u8; 32]);
        let event_b = create_test_block_event(height, block_hash_b, &[0]);
        buffer.insert(event_b);

        // Capture block B's tx hash
        let tx_hash_b = match &buffer.block_by_height.get(&height).unwrap().transactions {
            alloy_rpc_types::BlockTransactions::Full(txs) => *txs[0].inner.tx_hash(),
            _ => panic!("expected full transactions"),
        };

        assert_eq!(
            buffer.block_by_height.len(),
            buffer.block_height_by_hash.len(),
            "by_height and by_hash lengths should match after duplicate height insert"
        );

        // Verify the block stored at height 1 is the most recent one (block B)
        let stored_block = buffer
            .block_by_height
            .get(&height)
            .expect("Block should exist at height 1");
        assert_eq!(
            stored_block.header.hash, block_hash_b,
            "Block at height 1 should be the most recently inserted block (hash B)"
        );

        let hash_a_exists = buffer.block_height_by_hash.get(&block_hash_a).is_some();
        let hash_b_exists = buffer.block_height_by_hash.get(&block_hash_b).is_some();

        assert!(!hash_a_exists, "Old hash A should be removed from by_hash");
        assert!(hash_b_exists, "New hash B should exist in by_hash");

        // Verify tx_loc_by_hash cleanup: old block's txs removed, new block's txs present
        assert_eq!(
            buffer.tx_by_hash.len(),
            1,
            "tx_loc_by_hash should have exactly 1 entry after replacement"
        );
        assert!(
            view.get_transaction_by_hash(&tx_hash_a).is_none(),
            "Block A's transaction should be removed from tx_loc_by_hash after replacement"
        );
        assert!(
            view.get_transaction_by_hash(&tx_hash_b).is_some(),
            "Block B's transaction should be findable by hash after replacement"
        );
    }

    #[test]
    fn test_bloom_mismatch_returns_empty_txs_and_receipts() {
        let mut buffer = BlockBuffer::new(5);
        let view = buffer.create_view();

        let height = 1u64;
        let block_hash = B256::from([1u8; 32]);

        let event = create_test_block_event(height, block_hash, &[0]);
        buffer.insert(event);

        let always_reject_filter = |_bloom: Bloom| false;
        let (header, transactions, receipts) = view
            .get_bloom_filtered_header_transactions_receipts(height, always_reject_filter)
            .expect("should return Some for a known block");

        assert_eq!(header.hash, block_hash);
        assert!(transactions.is_empty());
        assert!(receipts.is_empty());
    }

    #[test]
    fn test_bloom_match_returns_receipts_with_correct_log_indices() {
        let mut buffer = BlockBuffer::new(5);
        let view = buffer.create_view();

        let height = 1u64;
        let block_hash = B256::from([1u8; 32]);

        let logs_per_transaction = [2, 3, 0];
        let event = create_test_block_event(height, block_hash, &logs_per_transaction);
        buffer.insert(event);

        let always_accept_filter = |_bloom: Bloom| true;
        let (header, transactions, receipts) = view
            .get_bloom_filtered_header_transactions_receipts(height, always_accept_filter)
            .expect("should return Some for a known block");

        assert_eq!(header.hash, block_hash);
        assert_eq!(transactions.len(), 3);
        assert_eq!(receipts.len(), 3);

        assert_eq!(receipts[0].starting_log_index, 0);
        assert_eq!(receipts[1].starting_log_index, 2);
        assert_eq!(receipts[2].starting_log_index, 5);

        assert_eq!(receipts[0].receipt.logs().len(), 2);
        assert_eq!(receipts[1].receipt.logs().len(), 3);
        assert_eq!(receipts[2].receipt.logs().len(), 0);
    }

    #[test]
    fn test_bloom_filter_returns_none_for_unknown_height() {
        let mut buffer = BlockBuffer::new(5);

        let unknown_height = 999;
        let result = buffer
            .create_view()
            .get_bloom_filtered_header_transactions_receipts(unknown_height, |_| true);
        assert!(result.is_none());
    }

    fn create_test_block_event(
        height: u64,
        block_hash: B256,
        logs_per_transaction: &[usize],
    ) -> EventServerEvent {
        use alloy_primitives::LogData;

        let signer = PrivateKeySigner::random();
        let mut global_log_index = 0u64;
        let mut block_logs_bloom = Bloom::default();

        let txs: Vec<_> = logs_per_transaction
            .iter()
            .enumerate()
            .map(|(tx_idx, &num_logs)| {
                let tx_inner = TxEip1559 {
                    chain_id: 1,
                    nonce: tx_idx as u64,
                    gas_limit: 21000,
                    max_fee_per_gas: 1000,
                    max_priority_fee_per_gas: 100,
                    to: TxKind::Call(Address::default()),
                    value: Default::default(),
                    access_list: Default::default(),
                    input: vec![].into(),
                };
                let signature = signer.sign_hash_sync(&tx_inner.signature_hash()).unwrap();
                let tx_envelope: TxEnvelope = tx_inner.into_signed(signature).into();
                let transaction_hash = *tx_envelope.tx_hash();
                let tx_recovered = Recovered::new_unchecked(tx_envelope, signer.address());

                let tx = alloy_rpc_types::Transaction {
                    inner: tx_recovered,
                    block_hash: Some(block_hash),
                    block_number: Some(height),
                    block_timestamp: None,
                    transaction_index: Some(tx_idx as u64),
                    effective_gas_price: None,
                };
                let serialized_tx = JsonSerialized::new_shared(tx);

                let logs: Vec<alloy_rpc_types::Log> = (0..num_logs)
                    .map(|_| {
                        let log = alloy_rpc_types::Log {
                            inner: alloy_primitives::Log {
                                address: Address::default(),
                                data: LogData::default(),
                            },
                            block_hash: Some(block_hash),
                            block_number: Some(height),
                            block_timestamp: None,
                            transaction_hash: Some(transaction_hash),
                            transaction_index: Some(tx_idx as u64),
                            log_index: Some(global_log_index),
                            removed: false,
                        };
                        block_logs_bloom.accrue_log(&log.inner);
                        global_log_index += 1;
                        log
                    })
                    .collect();

                let tx_receipt = TransactionReceipt {
                    inner: ReceiptEnvelope::Eip1559(ReceiptWithBloom {
                        receipt: Receipt {
                            status: Eip658Value::Eip658(true),
                            cumulative_gas_used: 21000,
                            logs,
                        },
                        logs_bloom: Bloom::default(),
                    }),
                    transaction_hash,
                    transaction_index: Some(tx_idx as u64),
                    block_hash: Some(block_hash),
                    block_number: Some(height),
                    gas_used: 21000,
                    effective_gas_price: 0,
                    blob_gas_used: None,
                    blob_gas_price: None,
                    from: signer.address(),
                    to: Some(Address::default()),
                    contract_address: None,
                };
                let serialized_tx_receipt = JsonSerialized::new_shared(tx_receipt);

                (
                    serialized_tx,
                    serialized_tx_receipt,
                    vec![].into_boxed_slice(),
                )
            })
            .collect();

        let inner_header = alloy_consensus::Header {
            number: height,
            logs_bloom: block_logs_bloom,
            ..Default::default()
        };
        let rpc_header = alloy_rpc_types::Header {
            inner: inner_header,
            hash: block_hash,
            total_difficulty: None,
            size: None,
        };

        let serialized_header = JsonSerialized::new_shared(rpc_header);
        let monad_header = MonadNotification {
            block_id: BlockId(monad_types::Hash(block_hash.0)),
            commit_state: BlockCommitState::Proposed,
            data: serialized_header,
        };
        let serialized_monad_header = JsonSerialized::new_shared(monad_header);

        EventServerEvent::Block {
            commit_state: BlockCommitState::Proposed,
            header: serialized_monad_header,
            transactions: Arc::new(txs.into_boxed_slice()),
        }
    }
}
