use alloy_consensus::{Header as RlpHeader, Transaction as _};
use alloy_primitives::{FixedBytes, U256};
use alloy_rpc_types::{Block, BlockTransactions, Header, Transaction, TransactionReceipt};
use monad_archive::{
    model::BlockDataReader,
    prelude::{ArchiveReader, IndexReader, TxEnvelopeWithSender},
};
use monad_triedb_utils::triedb_env::{BlockKey, FinalizedBlockKey, TransactionLocation, Triedb};
use monad_types::SeqNum;
use tracing::{error, warn};

use crate::eth_json_types::{BlockTagOrHash, BlockTags};

#[derive(Clone)]
pub struct ChainState<T: Triedb> {
    triedb_env: T,
    archive_reader: Option<ArchiveReader>,
}

#[derive(Debug)]
pub enum Error {
    Triedb(String),
    ResourceNotFound,
}

pub fn get_block_key_from_tag<T: Triedb>(triedb_env: &T, tag: BlockTags) -> BlockKey {
    match tag {
        BlockTags::Number(n) => triedb_env.get_block_key(SeqNum(n.0)),
        BlockTags::Latest => triedb_env.get_latest_voted_block_key(),
        BlockTags::Safe => triedb_env.get_latest_voted_block_key(),
        BlockTags::Finalized => BlockKey::Finalized(triedb_env.get_latest_finalized_block_key()),
    }
}

impl<T: Triedb> ChainState<T> {
    pub fn new(triedb_env: T, archive_reader: Option<ArchiveReader>) -> Self {
        ChainState {
            triedb_env,
            archive_reader,
        }
    }

    pub fn get_latest_block_number(&self) -> u64 {
        self.triedb_env.get_latest_voted_block_key().seq_num().0
    }

    pub async fn get_transaction_receipt(
        &self,
        hash: [u8; 32],
    ) -> Result<TransactionReceipt, Error> {
        let latest_block_key = get_block_key_from_tag(&self.triedb_env, BlockTags::Latest);
        if let Some(TransactionLocation {
            tx_index,
            block_num,
        }) = self
            .triedb_env
            .get_transaction_location_by_hash(latest_block_key, hash)
            .await
            .map_err(Error::Triedb)?
        {
            let block_key = self.triedb_env.get_block_key(SeqNum(block_num));
            if let Some(receipt) =
                get_receipt_from_triedb(&self.triedb_env, block_key, tx_index).await?
            {
                return Ok(receipt);
            }
        }

        // try archive if transaction hash not found and archive reader specified
        if let Some(archive_reader) = &self.archive_reader {
            if let Ok(tx_data) = archive_reader.get_tx_indexed_data(&hash.into()).await {
                let receipt = crate::handlers::eth::txn::parse_tx_receipt(
                    tx_data.header_subset.base_fee_per_gas,
                    Some(tx_data.header_subset.block_timestamp),
                    tx_data.header_subset.block_hash,
                    tx_data.tx,
                    tx_data.header_subset.gas_used,
                    tx_data.receipt,
                    tx_data.header_subset.block_number,
                    tx_data.header_subset.tx_index,
                );

                return Ok(receipt);
            }
        }

        Err(Error::ResourceNotFound)
    }

    pub async fn get_transaction_with_block_and_index(
        &self,
        block: BlockTagOrHash,
        index: u64,
    ) -> Result<Transaction, Error> {
        match block {
            BlockTagOrHash::BlockTags(block) => {
                let block_key = get_block_key_from_tag(&self.triedb_env, block);
                if let Some(tx) =
                    get_transaction_from_triedb(&self.triedb_env, block_key, index).await?
                {
                    return Ok(tx);
                }

                // try archive if block header not found and archive reader specified
                if let (Some(archive_reader), BlockKey::Finalized(FinalizedBlockKey(block_num))) =
                    (&self.archive_reader, block_key)
                {
                    if let Ok(block) = archive_reader
                        .get_block_by_number(block_num.0)
                        .await
                        .inspect_err(|e| {
                            warn!("Error getting block by number from archive: {e:?}");
                        })
                    {
                        if let Some(tx) = block.body.transactions.get(index as usize) {
                            return Ok(parse_tx_content(
                                block.header.hash_slow(),
                                block.header.number,
                                block.header.base_fee_per_gas,
                                tx.clone(),
                                index,
                            ));
                        }
                    }
                }
            }
            BlockTagOrHash::Hash(hash) => {
                let latest_block_key = get_block_key_from_tag(&self.triedb_env, BlockTags::Latest);
                if let Some(block_num) = self
                    .triedb_env
                    .get_block_number_by_hash(latest_block_key, hash.0)
                    .await
                    .map_err(Error::Triedb)?
                {
                    let block_key = self.triedb_env.get_block_key(SeqNum(block_num));
                    if let Some(tx) =
                        get_transaction_from_triedb(&self.triedb_env, block_key, index).await?
                    {
                        return Ok(tx);
                    }
                }

                // try archive if block hash not found and archive reader specified
                if let Some(archive_reader) = &self.archive_reader {
                    if let Ok(block) = archive_reader.get_block_by_hash(&hash.0.into()).await {
                        if let Some(tx) = block.body.transactions.get(index as usize) {
                            return Ok(parse_tx_content(
                                hash.0.into(),
                                block.header.number,
                                block.header.base_fee_per_gas,
                                tx.clone(),
                                index,
                            ));
                        }
                    }
                }
            }
        }

        Err(Error::ResourceNotFound)
    }

    pub async fn get_transaction(&self, hash: [u8; 32]) -> Result<Transaction, Error> {
        let latest_block_key = get_block_key_from_tag(&self.triedb_env, BlockTags::Latest);
        if let Some(TransactionLocation {
            tx_index,
            block_num,
        }) = self
            .triedb_env
            .get_transaction_location_by_hash(latest_block_key, hash)
            .await
            .map_err(Error::Triedb)?
        {
            let block_key = self.triedb_env.get_block_key(SeqNum(block_num));
            if let Some(tx) =
                get_transaction_from_triedb(&self.triedb_env, block_key, tx_index).await?
            {
                return Ok(tx);
            };
        }

        // try archive if transaction hash not found and archive reader specified
        if let Some(archive_reader) = &self.archive_reader {
            if let Ok((tx, header_subset)) =
                archive_reader.get_tx(&hash.into()).await.inspect_err(|e| {
                    warn!("Error getting tx from archive: {e:?}");
                })
            {
                return Ok(parse_tx_content(
                    header_subset.block_hash,
                    header_subset.block_number,
                    header_subset.base_fee_per_gas,
                    tx,
                    header_subset.tx_index,
                ));
            }
        }

        Err(Error::ResourceNotFound)
    }

    pub async fn get_block_header(
        &self,
        block: BlockTagOrHash,
    ) -> Result<alloy_consensus::Header, Error> {
        let block_key = match block {
            BlockTagOrHash::BlockTags(tag) => get_block_key_from_tag(&self.triedb_env, tag),
            BlockTagOrHash::Hash(hash) => {
                let latest_block_key = get_block_key_from_tag(&self.triedb_env, BlockTags::Latest);
                if let Some(block_num) = self
                    .triedb_env
                    .get_block_number_by_hash(latest_block_key, hash.0)
                    .await
                    .map_err(Error::Triedb)?
                {
                    self.triedb_env.get_block_key(SeqNum(block_num))
                } else {
                    return Err(Error::ResourceNotFound);
                }
            }
        };

        if let Some(header) = self
            .triedb_env
            .get_block_header(block_key)
            .await
            .map_err(Error::Triedb)?
        {
            return Ok(header.header);
        }

        if let (Some(archive_reader), BlockKey::Finalized(FinalizedBlockKey(block_num))) =
            (&self.archive_reader, block_key)
        {
            if let Ok(block) = archive_reader
                .get_block_by_number(block_num.0)
                .await
                .inspect_err(|e| {
                    error!("Error getting block by number from archive: {e:?}");
                })
            {
                return Ok(block.header);
            }
        }

        Err(Error::ResourceNotFound)
    }

    pub async fn get_block(
        &self,
        block: BlockTagOrHash,
        return_full_txns: bool,
    ) -> Result<Block, Error> {
        let block_key = match block {
            BlockTagOrHash::BlockTags(tag) => get_block_key_from_tag(&self.triedb_env, tag),
            BlockTagOrHash::Hash(hash) => {
                let latest_block_key = get_block_key_from_tag(&self.triedb_env, BlockTags::Latest);
                if let Some(block_num) = self
                    .triedb_env
                    .get_block_number_by_hash(latest_block_key, hash.0)
                    .await
                    .map_err(Error::Triedb)?
                {
                    self.triedb_env.get_block_key(SeqNum(block_num))
                } else {
                    return Err(Error::ResourceNotFound);
                }
            }
        };

        if let Some(header) = self
            .triedb_env
            .get_block_header(block_key)
            .await
            .map_err(Error::Triedb)?
        {
            if let Ok(transactions) = self.triedb_env.get_transactions(block_key).await {
                return Ok(parse_block_content(
                    header.hash,
                    header.header,
                    transactions,
                    return_full_txns,
                ));
            }
        }

        if let (Some(archive_reader), BlockKey::Finalized(FinalizedBlockKey(block_num))) =
            (&self.archive_reader, block_key)
        {
            if let Ok(block) = archive_reader
                .get_block_by_number(block_num.0)
                .await
                .inspect_err(|e| {
                    error!("Error getting block by number from archive: {e:?}");
                })
            {
                return Ok(parse_block_content(
                    block.header.hash_slow(),
                    block.header,
                    block.body.transactions,
                    return_full_txns,
                ));
            }
        }

        Err(Error::ResourceNotFound)
    }

    /// Returns raw transaction receipts for a block.
    pub async fn get_raw_receipts(
        &self,
        block: BlockTags,
    ) -> Result<Vec<alloy_consensus::ReceiptEnvelope>, Error> {
        let block_key = get_block_key_from_tag(&self.triedb_env, block);
        if let Ok(receipts) = self.triedb_env.get_receipts(block_key).await {
            let receipts: Vec<alloy_consensus::ReceiptEnvelope> = receipts
                .into_iter()
                .map(|receipt_with_log_index| receipt_with_log_index.receipt)
                .collect();
            return Ok(receipts);
        };

        if let (Some(archive_reader), BlockKey::Finalized(FinalizedBlockKey(block_num))) =
            (&self.archive_reader, block_key)
        {
            if let Ok(receipts) = archive_reader
                .get_block_receipts(block_num.0)
                .await
                .inspect_err(|e| {
                    error!("Error getting block by number from archive: {e:?}");
                })
            {
                let receipts: Vec<alloy_consensus::ReceiptEnvelope> = receipts
                    .into_iter()
                    .map(|receipt_with_log_index| receipt_with_log_index.receipt)
                    .collect();
                return Ok(receipts);
            }
        }

        Err(Error::ResourceNotFound)
    }

    /// Returns transaction receipts mapped to their block and transaction info.
    pub async fn get_block_receipts(
        &self,
        block: BlockTagOrHash,
    ) -> Result<Vec<crate::eth_json_types::MonadTransactionReceipt>, Error> {
        if let Ok(block_key) = crate::handlers::eth::block::get_block_key_from_tag_or_hash(
            &self.triedb_env,
            block.clone(),
        )
        .await
        {
            if let Some(header) = self
                .triedb_env
                .get_block_header(block_key)
                .await
                .map_err(Error::Triedb)?
            {
                // if block header is present but transactions are not, the block is statesynced
                if let Ok(transactions) = self.triedb_env.get_transactions(block_key).await {
                    if let Ok(receipts) = self.triedb_env.get_receipts(block_key).await {
                        let block_receipts = crate::handlers::eth::block::map_block_receipts(
                            transactions,
                            receipts,
                            &header.header,
                            header.hash,
                            crate::eth_json_types::MonadTransactionReceipt,
                        )
                        .map_err(|_| Error::ResourceNotFound)?;
                        return Ok(block_receipts);
                    }
                }
            }
        }
        // try archive if header or transactions not found and archive reader specified
        if let Some(archive_reader) = &self.archive_reader {
            let block = match block {
                BlockTagOrHash::BlockTags(tag) => {
                    match get_block_key_from_tag(&self.triedb_env, tag) {
                        BlockKey::Finalized(FinalizedBlockKey(block_num)) => archive_reader
                            .get_block_by_number(block_num.0)
                            .await
                            .inspect_err(|e| {
                                error!("Error getting block by number from archive: {e:?}");
                            })
                            .ok(),
                        BlockKey::Proposed(_) => None,
                    }
                }
                BlockTagOrHash::Hash(hash) => archive_reader
                    .get_block_by_hash(&hash.0.into())
                    .await
                    .inspect_err(|e| {
                        error!("Error getting block by hash from archive: {e:?}");
                    })
                    .ok(),
            };
            if let Some(block) = block {
                if let Ok(receipts_with_log_index) = archive_reader
                    .get_block_receipts(block.header.number)
                    .await
                    .inspect_err(|e| {
                        error!("Error getting block receipts from archive: {e:?}");
                    })
                {
                    let block_receipts = crate::handlers::eth::block::map_block_receipts(
                        block.body.transactions,
                        receipts_with_log_index,
                        &block.header,
                        block.header.hash_slow(),
                        crate::eth_json_types::MonadTransactionReceipt,
                    )
                    .map_err(|_| Error::ResourceNotFound)?;
                    return Ok(block_receipts);
                }
            }
        }

        Err(Error::ResourceNotFound)
    }
}

fn parse_block_content(
    block_hash: FixedBytes<32>,
    header: RlpHeader,
    transactions: Vec<TxEnvelopeWithSender>,
    return_full_txns: bool,
) -> Block {
    // parse transactions
    let transactions = if return_full_txns {
        let txs = transactions
            .into_iter()
            .enumerate()
            .map(|(idx, tx)| {
                parse_tx_content(
                    block_hash,
                    header.number,
                    header.base_fee_per_gas,
                    tx,
                    idx as u64,
                )
            })
            .collect();

        BlockTransactions::Full(txs)
    } else {
        BlockTransactions::Hashes(transactions.iter().map(|tx| *tx.tx.tx_hash()).collect())
    };

    // NOTE: no withdrawals currently in monad-bft
    Block {
        header: Header {
            total_difficulty: Some(header.difficulty),
            hash: block_hash,
            size: Some(U256::from(header.size())),
            inner: header,
        },
        transactions,
        uncles: vec![],
        withdrawals: None,
    }
}

pub fn parse_tx_content(
    block_hash: FixedBytes<32>,
    block_number: u64,
    base_fee: Option<u64>,
    tx: TxEnvelopeWithSender,
    tx_index: u64,
) -> Transaction {
    // unpack transaction
    let sender = tx.sender;
    let tx = tx.tx;

    // effective gas price is calculated according to eth json rpc specification
    let effective_gas_price = tx.effective_gas_price(base_fee);

    Transaction {
        inner: tx,
        from: sender,
        block_hash: Some(block_hash),
        block_number: Some(block_number),
        effective_gas_price: Some(effective_gas_price),
        transaction_index: Some(tx_index),
    }
}

#[tracing::instrument(level = "debug")]
async fn get_transaction_from_triedb<T: Triedb>(
    triedb_env: &T,
    block_key: BlockKey,
    tx_index: u64,
) -> Result<Option<Transaction>, Error> {
    let header = match triedb_env
        .get_block_header(block_key)
        .await
        .map_err(Error::Triedb)?
    {
        Some(header) => header,
        None => return Ok(None),
    };

    match triedb_env
        .get_transaction(block_key, tx_index)
        .await
        .map_err(Error::Triedb)?
    {
        Some(tx) => Ok(Some(parse_tx_content(
            header.hash,
            header.header.number,
            header.header.base_fee_per_gas,
            tx,
            tx_index,
        ))),
        None => Ok(None),
    }
}

#[tracing::instrument(level = "debug")]
async fn get_receipt_from_triedb<T: Triedb>(
    triedb_env: &T,
    block_key: BlockKey,
    tx_index: u64,
) -> Result<Option<TransactionReceipt>, Error> {
    let header = match triedb_env
        .get_block_header(block_key)
        .await
        .map_err(Error::Triedb)?
    {
        Some(header) => header,
        None => return Ok(None),
    };

    let tx = match triedb_env
        .get_transaction(block_key, tx_index)
        .await
        .map_err(Error::Triedb)?
    {
        Some(tx) => tx,
        None => return Ok(None),
    };

    match triedb_env
        .get_receipt(block_key, tx_index)
        .await
        .map_err(Error::Triedb)?
    {
        Some(receipt) => {
            // Get the previous receipt's cumulative gas used to calculate gas used
            let gas_used = if tx_index > 0 {
                match triedb_env
                    .get_receipt(block_key, tx_index - 1)
                    .await
                    .map_err(Error::Triedb)?
                {
                    Some(prev_receipt) => {
                        receipt.receipt.cumulative_gas_used()
                            - prev_receipt.receipt.cumulative_gas_used()
                    }
                    None => return Err(Error::Triedb("error getting receipt".into())),
                }
            } else {
                receipt.receipt.cumulative_gas_used()
            };

            let receipt = crate::handlers::eth::txn::parse_tx_receipt(
                header.header.base_fee_per_gas,
                Some(header.header.timestamp),
                header.hash,
                tx,
                gas_used,
                receipt,
                block_key.seq_num().0,
                tx_index,
            );

            Ok(Some(receipt))
        }
        None => Ok(None),
    }
}
