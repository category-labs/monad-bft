use alloy_consensus::ReceiptEnvelope;
use alloy_rlp::Encodable;
use eyre::{bail, Result};

use crate::prelude::*;

#[derive(Clone)]
pub struct TxIndexArchiver<Store = IndexStoreErased> {
    pub store: Store,
    pub block_data_archive: BlockDataArchive,
}

impl<Store: IndexStore> TxIndexArchiver<Store> {
    pub fn new(store: Store, block_data_archive: BlockDataArchive) -> TxIndexArchiver<Store> {
        Self {
            store,
            block_data_archive,
        }
    }

    pub async fn update_latest_indexed(&self, block_num: u64) -> Result<()> {
        self.block_data_archive
            .update_latest(block_num, LatestKind::Indexed)
            .await
    }

    pub async fn get_latest_indexed(&self) -> Result<u64> {
        self.block_data_archive
            .get_latest(LatestKind::Indexed)
            .await
    }

    pub async fn index_block(
        &self,
        block: Block,
        traces: Vec<Vec<u8>>,
        receipts: Vec<ReceiptEnvelope>,
    ) -> Result<()> {
        let block_number = block.header.number;
        let block_hash = block.header.hash_slow();
        let base_fee_per_gas = block.header.base_fee_per_gas;

        if block.body.transactions.len() != traces.len() || traces.len() != receipts.len() {
            bail!("Block must have same number of txs as traces and receipts. num_txs: {}, num_traces: {}, num_receipts: {}", 
            block.body.length(), traces.len(), receipts.len());
        }

        let mut prev_cumulative_gas_used = 0;

        let requests = block
            .body
            .transactions
            .into_iter()
            .zip(traces.into_iter())
            .zip(receipts.into_iter())
            .enumerate()
            .map(|(idx, ((tx, trace), receipt))| {
                // calculate gas used by this tx
                let gas_used = receipt.cumulative_gas_used() - prev_cumulative_gas_used;
                prev_cumulative_gas_used = receipt.cumulative_gas_used();

                TxIndexedData {
                    tx,
                    trace,
                    receipt,
                    header_subset: HeaderSubset {
                        block_hash,
                        block_number,
                        tx_index: idx as u64,
                        gas_used,
                        base_fee_per_gas,
                    },
                }
            });

        self.store.bulk_put(requests).await
    }
}
