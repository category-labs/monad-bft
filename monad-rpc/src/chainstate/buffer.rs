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
        }
    }

    pub async fn insert(&self, block_event: EventServerEvent) {
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

        if commit_state != BlockCommitState::Voted {
            if commit_state == BlockCommitState::Finalized {
                self.latest_finalized
                    .fetch_max(block_height, Ordering::SeqCst);
            }
            return;
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

        if self.block_by_height.insert(block_height, block).is_some() {
            warn!(
                ?block_height,
                "ChainStateBuffer received block event for existing block height"
            );
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

        for (tx_idx, tx_receipt) in transactions
            .iter()
            .map(|(_, tx_receipt, _)| tx_receipt)
            .enumerate()
        {
            let tx_hash = tx_receipt.transaction_hash;

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
    }
}
