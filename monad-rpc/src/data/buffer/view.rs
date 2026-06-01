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

use std::sync::{atomic::Ordering, Arc};

use alloy_consensus::{Header, TxEnvelope};
use alloy_primitives::{BlockHash, Bloom, TxHash};
use alloy_rpc_types::{Block, BlockTransactions, Transaction, TransactionReceipt};
use async_trait::async_trait;
use dashmap::{mapref::one::Ref as DashMapRef, DashMap};
use monad_eth_types::{BlockHeader, ReceiptWithLogIndex, TxEnvelopeWithSender};
use monad_types::{BlockId, Hash};
use tracing::{error, warn};

use super::{BlockBufferBlockStates, BlockBufferTxLoc};
use crate::{
    data::source::{BlockCommitState, BlockPointer, DataSourceResult, HistoricalDataSource},
    types::eth_json::BlockTags,
};

#[derive(Clone)]
pub struct BlockBufferView {
    pub(super) block_states: Arc<BlockBufferBlockStates>,

    // Maps a block by its SeqNum
    pub(super) block_by_height: Arc<DashMap<u64, Block>>,
    // Maps a block by its blockhash
    pub(super) block_height_by_hash: Arc<DashMap<BlockHash, u64>>,
    // Maps a transaction hash to its block location and receipt
    pub(super) tx_by_hash: Arc<DashMap<TxHash, (BlockBufferTxLoc, TransactionReceipt)>>,
}

impl BlockBufferView {
    pub fn get_latest_proposed_block_num(&self) -> u64 {
        self.block_states.proposed.load(Ordering::SeqCst)
    }

    pub fn get_latest_voted_block_num(&self) -> u64 {
        self.block_states.voted.load(Ordering::SeqCst)
    }

    pub fn get_latest_finalized_block_num(&self) -> u64 {
        self.block_states.finalized.load(Ordering::SeqCst)
    }

    pub fn resolve_block_height_from_tag(&self, tag: &BlockTags) -> u64 {
        match tag {
            BlockTags::Number(n) => n.0,
            BlockTags::Latest => self.get_latest_proposed_block_num(),
            BlockTags::Safe => self.get_latest_voted_block_num(),
            BlockTags::Finalized => self.get_latest_finalized_block_num(),
        }
    }

    pub fn get_block_by_hash(&self, block_hash: &BlockHash) -> Option<Block> {
        let block_height: u64 = *self.block_height_by_hash.get(block_hash)?.value();

        Some(self.block_by_height.get(&block_height)?.value().clone())
    }

    pub fn get_receipt_by_tx_hash(&self, tx_hash: &TxHash) -> Option<TransactionReceipt> {
        Some(self.tx_by_hash.get(tx_hash)?.value().1.clone())
    }

    pub fn get_receipts_by_block_height(&self, height: u64) -> Option<Vec<TransactionReceipt>> {
        let block = self.block_by_height.get(&height)?;

        let tx_hashes = match &block.transactions {
            BlockTransactions::Full(txs) => {
                txs.iter().map(|tx| tx.inner.tx_hash()).collect::<Vec<_>>()
            }
            _ => return None,
        };

        let mut receipts = Vec::with_capacity(tx_hashes.len());

        for tx_hash in tx_hashes {
            let receipt = self.tx_by_hash.get(tx_hash)?.1.clone();
            receipts.push(receipt);
        }

        Some(receipts)
    }

    pub fn get_bloom_filtered_header_transactions_receipts(
        &self,
        height: u64,
        filter_match: impl Fn(Bloom) -> bool,
    ) -> Option<(
        BlockHeader,
        Vec<TxEnvelopeWithSender>,
        Vec<ReceiptWithLogIndex>,
    )> {
        let block = self.block_by_height.get(&height)?;

        let header = BlockHeader {
            hash: block.header.hash,
            header: block.header.inner.clone(),
        };

        if !filter_match(header.header.logs_bloom) {
            return Some((header, vec![], vec![]));
        }

        let transactions = match &block.transactions {
            BlockTransactions::Full(txs) => txs
                .iter()
                .map(|tx| {
                    let (envelope, sender) = tx.inner.clone().into_parts();
                    TxEnvelopeWithSender {
                        tx: envelope,
                        sender,
                    }
                })
                .collect::<Vec<_>>(),
            _ => {
                error!(
                    ?height,
                    "BlockBuffer stored block without full transactions"
                );

                return None;
            }
        };

        let mut log_index = 0u64;

        let receipts = transactions
            .iter()
            .map(|transaction| {
                let tx_hash = transaction.tx.tx_hash();

                let Some(transaction_data) = self.tx_by_hash.get(tx_hash) else {
                    error!(
                        ?height,
                        ?tx_hash,
                        "BlockBuffer stored block but transaction was not stored"
                    );
                    return Err(());
                };

                let transaction_receipt = &transaction_data.1;

                let starting_log_index = log_index;

                if let Some(first_log) = transaction_receipt.logs().first() {
                    let Some(first_log_index) = first_log.log_index else {
                        error!(
                            ?height,
                            expected_log_index = ?starting_log_index,
                            "BlockBuffer stored receipt in block where log does not have log_index"
                        );
                        return Err(());
                    };

                    if first_log_index != starting_log_index {
                        error!(
                            ?height,
                            expected_log_index = ?starting_log_index,
                            ?first_log_index,
                            "BlockBuffer stored receipt block with invalid log indexing"
                        );
                        return Err(());
                    }
                }

                log_index += transaction_receipt.logs().len() as u64;

                Ok(ReceiptWithLogIndex {
                    receipt: transaction_receipt.inner.clone().into_primitives_receipt(),
                    starting_log_index,
                })
            })
            .collect::<Result<_, _>>();

        match receipts {
            Ok(receipts) => Some((header, transactions, receipts)),
            Err(()) => None,
        }
    }

    pub fn get_transaction_by_hash(&self, tx_hash: &TxHash) -> Option<Transaction<TxEnvelope>> {
        let tx_loc = &self.tx_by_hash.get(tx_hash)?.0;

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

    fn get_block_ref(&self, pointer: BlockPointer) -> Option<DashMapRef<'_, u64, Block>> {
        match pointer {
            BlockPointer::Finalized(block_number) => self.block_by_height.get(&block_number),
            BlockPointer::NonFinalized(block_number, block_id) => {
                let block = self.block_by_height.get(&block_number)?;

                if block.hash().0 != block_id.0 .0 {
                    return None;
                }

                Some(block)
            }
        }
    }
}

#[async_trait]
impl HistoricalDataSource for BlockBufferView {
    async fn try_resolve_block_commit_state(
        &self,
        commit_state: BlockCommitState,
    ) -> DataSourceResult<Option<BlockPointer>> {
        let block_number = match commit_state {
            BlockCommitState::Finalized => {
                return Ok(Some(BlockPointer::Finalized(
                    self.get_latest_finalized_block_num(),
                )))
            }
            BlockCommitState::Voted => self.get_latest_voted_block_num(),
            BlockCommitState::Proposed => self.get_latest_proposed_block_num(),
        };

        let Some(block) = self.block_by_height.get(&block_number) else {
            return Ok(None);
        };

        Ok(Some(BlockPointer::NonFinalized(
            block_number,
            monad_types::BlockId(monad_types::Hash(block.hash().0)),
        )))
    }

    async fn try_resolve_block_number(
        &self,
        block_number: u64,
    ) -> DataSourceResult<Option<BlockPointer>> {
        let finalized_block_number = self.get_latest_finalized_block_num();

        if block_number <= finalized_block_number {
            return Ok(Some(BlockPointer::Finalized(block_number)));
        }

        let Some(block) = self.block_by_height.get(&block_number) else {
            return Ok(None);
        };

        Ok(Some(BlockPointer::NonFinalized(
            block_number,
            monad_types::BlockId(monad_types::Hash(block.hash().0)),
        )))
    }

    async fn try_resolve_block_hash(
        &self,
        block_hash: BlockHash,
    ) -> DataSourceResult<Option<BlockPointer>> {
        let Some(block_number) = self
            .block_height_by_hash
            .get(&block_hash)
            .map(|block_number| *block_number.value())
        else {
            return Ok(None);
        };

        let Some(block_hash_at_block_number) = self
            .block_by_height
            .get(&block_number)
            .map(|block| block.hash().0)
        else {
            return Ok(None);
        };

        if block_hash_at_block_number != block_hash.0 {
            return Ok(None);
        }

        let finalized_block_number = self.get_latest_finalized_block_num();

        if block_number <= finalized_block_number {
            Ok(Some(BlockPointer::Finalized(block_number)))
        } else {
            Ok(Some(BlockPointer::NonFinalized(
                block_number,
                BlockId(Hash(block_hash.0)),
            )))
        }
    }

    async fn get_block(
        &self,
        pointer: BlockPointer,
    ) -> DataSourceResult<Option<(Header, Vec<TxEnvelopeWithSender>)>> {
        let Some(block) = self.get_block_ref(pointer) else {
            return Ok(None);
        };

        let BlockTransactions::Full(txs) = &block.value().transactions else {
            warn!(
                block_number = block.value().header.number,
                "BlockBufferView block does not contain full transaction list"
            );
            return Ok(None);
        };

        Ok(Some((
            block.value().header.inner.clone(),
            txs.iter()
                .map(|tx| {
                    let tx = tx.as_recovered();
                    let sender = tx.signer();

                    TxEnvelopeWithSender {
                        tx: tx.into_inner().clone(),
                        sender,
                    }
                })
                .collect(),
        )))
    }

    async fn get_block_header(&self, pointer: BlockPointer) -> DataSourceResult<Option<Header>> {
        let Some(block) = self.get_block_ref(pointer) else {
            return Ok(None);
        };

        Ok(Some(block.value().header.inner.clone()))
    }
}
