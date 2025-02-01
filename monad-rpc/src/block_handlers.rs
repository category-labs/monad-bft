use alloy_consensus::Header as RlpHeader;
use alloy_primitives::{FixedBytes, U256};
use alloy_rpc_types::{Block, BlockTransactions, Header, TransactionReceipt};
use monad_archive::prelude::{ArchiveReader, BlockDataReader};
use monad_rpc_docs::rpc;
use monad_triedb_utils::triedb_env::{
    BlockKey, FinalizedBlockKey, ReceiptWithLogIndex, Triedb, TxEnvelopeWithSender,
};
use monad_types::SeqNum;
use serde::{Deserialize, Serialize};
use tracing::trace;

use crate::{
    eth_json_types::{
        BlockTagOrHash, BlockTags, EthHash, MonadBlock, MonadTransactionReceipt, Quantity,
    },
    eth_txn_handlers::{parse_tx_content, parse_tx_receipt},
    jsonrpc::{JsonRpcError, JsonRpcResult},
};

pub fn get_block_key_from_tag<T: Triedb>(triedb_env: &T, tag: BlockTags) -> BlockKey {
    match tag {
        BlockTags::Number(n) => triedb_env.get_block_key(SeqNum(n.0)),
        BlockTags::Latest => triedb_env.get_latest_voted_block_key(),
        BlockTags::Safe => triedb_env.get_latest_voted_block_key(),
        BlockTags::Finalized => BlockKey::Finalized(triedb_env.get_latest_finalized_block_key()),
    }
}

pub async fn get_block_key_from_tag_or_hash<T: Triedb>(
    triedb_env: &T,
    block_reference: BlockTagOrHash,
) -> JsonRpcResult<BlockKey> {
    match block_reference {
        BlockTagOrHash::BlockTags(tag) => Ok(get_block_key_from_tag(triedb_env, tag)),
        BlockTagOrHash::Hash(block_hash) => {
            let num = triedb_env
                .get_block_number_by_hash(triedb_env.get_latest_voted_block_key(), block_hash.0)
                .await
                .map_err(|_| JsonRpcError::resource_not_found())?
                .ok_or(JsonRpcError::resource_not_found())?;
            Ok(triedb_env.get_block_key(SeqNum(num)))
        }
    }
}

fn parse_block_content(
    block_hash: FixedBytes<32>,
    header: RlpHeader,
    transactions: Vec<TxEnvelopeWithSender>,
    return_full_txns: bool,
) -> JsonRpcResult<Option<MonadEthGetBlock>> {
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
            .collect::<Result<_, JsonRpcError>>()?;

        BlockTransactions::Full(txs)
    } else {
        BlockTransactions::Hashes(transactions.iter().map(|tx| *tx.tx.tx_hash()).collect())
    };

    // NOTE: no withdrawals currently in monad-bft
    let retval = Block {
        header: Header {
            total_difficulty: Some(header.difficulty),
            hash: block_hash,
            size: Some(U256::from(header.size())),
            inner: header,
        },
        transactions,
        uncles: vec![],
        withdrawals: None,
    };

    Ok(Some(MonadEthGetBlock {
        block: MonadBlock(retval),
    }))
}

#[rpc(method = "eth_blockNumber")]
#[allow(non_snake_case)]
#[tracing::instrument(level = "debug")]
/// Returns the number of most recent block.
pub async fn monad_eth_blockNumber<T: Triedb>(triedb_env: &T) -> JsonRpcResult<Quantity> {
    trace!("monad_eth_blockNumber");

    let block_key = triedb_env.get_latest_voted_block_key();
    Ok(Quantity(block_key.seq_num().0))
}

#[rpc(method = "eth_chainId", ignore = "chain_id")]
#[allow(non_snake_case)]
#[tracing::instrument(level = "debug")]
/// Returns the chain ID of the current network.
pub async fn monad_eth_chainId(chain_id: u64) -> JsonRpcResult<Quantity> {
    trace!("monad_eth_chainId");

    Ok(Quantity(chain_id))
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetBlockByHashParams {
    block_hash: EthHash,
    return_full_txns: bool,
}

#[derive(Serialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetBlock {
    #[serde(flatten)]
    block: MonadBlock,
}

#[rpc(method = "eth_getBlockByHash")]
#[allow(non_snake_case)]
#[tracing::instrument(level = "debug", skip(archive_reader))]
/// Returns information about a block by hash.
pub async fn monad_eth_getBlockByHash<T: Triedb>(
    triedb_env: &T,
    archive_reader: &Option<ArchiveReader>,
    params: MonadEthGetBlockByHashParams,
) -> JsonRpcResult<Option<MonadEthGetBlock>> {
    trace!("monad_eth_getBlockByHash: {params:?}");

    let latest_block_key = get_block_key_from_tag(triedb_env, BlockTags::Latest);
    if let Some(block_num) = triedb_env
        .get_block_number_by_hash(latest_block_key, params.block_hash.0)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        let block_key = triedb_env.get_block_key(SeqNum(block_num));
        if let Some(header) = triedb_env
            .get_block_header(block_key)
            .await
            .map_err(JsonRpcError::internal_error)?
        {
            let transactions = triedb_env
                .get_transactions(block_key)
                .await
                .map_err(JsonRpcError::internal_error)?;
            return parse_block_content(
                header.hash,
                header.header,
                transactions,
                params.return_full_txns,
            );
        }
    }

    // try archive if header not found and archive reader specified
    if let Some(archive_reader) = archive_reader {
        if let Ok(block) = archive_reader
            .get_block_by_hash(&params.block_hash.0.into())
            .await
        {
            return parse_block_content(
                block.header.hash_slow(),
                block.header,
                block.body.transactions,
                params.return_full_txns,
            );
        }
    }

    // return none if both triedb and archive fails
    Ok(None)
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetBlockByNumberParams {
    block_number: BlockTags,
    return_full_txns: bool,
}

#[rpc(method = "eth_getBlockByNumber")]
#[allow(non_snake_case)]
#[tracing::instrument(level = "debug", skip(archive_reader))]
/// Returns information about a block by number.
pub async fn monad_eth_getBlockByNumber<T: Triedb>(
    triedb_env: &T,
    archive_reader: &Option<ArchiveReader>,
    params: MonadEthGetBlockByNumberParams,
) -> JsonRpcResult<Option<MonadEthGetBlock>> {
    trace!("monad_eth_getBlockByNumber: {params:?}");

    let block_key = get_block_key_from_tag(triedb_env, params.block_number);

    if let Some(header) = triedb_env
        .get_block_header(block_key)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        // if block header is present but transactions are not, the block is statesynced
        if let Ok(transactions) = triedb_env.get_transactions(block_key).await {
            return parse_block_content(
                header.hash,
                header.header,
                transactions,
                params.return_full_txns,
            );
        }
    }

    // try archive if header or transactions not found and archive reader specified
    if let (Some(archive_reader), BlockKey::Finalized(FinalizedBlockKey(block_num))) =
        (archive_reader, block_key)
    {
        if let Ok(block) = archive_reader.get_block_by_number(block_num.0).await {
            return parse_block_content(
                block.header.hash_slow(),
                block.header,
                block.body.transactions,
                params.return_full_txns,
            );
        }
    }

    // return none if both triedb and archive fails
    Ok(None)
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetBlockTransactionCountByHashParams {
    block_hash: EthHash,
}

#[rpc(method = "eth_getBlockTransactionCountByHash")]
#[allow(non_snake_case)]
#[tracing::instrument(level = "debug", skip(archive_reader))]
/// Returns the number of transactions in a block from a block matching the given block hash.
pub async fn monad_eth_getBlockTransactionCountByHash<T: Triedb>(
    triedb_env: &T,
    archive_reader: &Option<ArchiveReader>,
    params: MonadEthGetBlockTransactionCountByHashParams,
) -> JsonRpcResult<Option<String>> {
    trace!("monad_eth_getBlockTransactionCountByHash: {params:?}");

    let latest_block_key = get_block_key_from_tag(triedb_env, BlockTags::Latest);
    if let Some(block_num) = triedb_env
        .get_block_number_by_hash(latest_block_key, params.block_hash.0)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        let block_key = triedb_env.get_block_key(SeqNum(block_num));
        let transactions = triedb_env
            .get_transactions(block_key)
            .await
            .map_err(JsonRpcError::internal_error)?;
        return Ok(Some(format!("0x{:x}", transactions.len())));
    }

    // try archive if block hash not found and archive reader specified
    if let Some(archive_reader) = archive_reader {
        if let Ok(block) = archive_reader
            .get_block_by_hash(&params.block_hash.0.into())
            .await
        {
            return Ok(Some(format!("0x{:x}", block.body.transactions.len())));
        }
    }

    Ok(None)
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetBlockTransactionCountByNumberParams {
    block_tag: BlockTags,
}

#[rpc(method = "eth_getBlockTransactionCountByNumber")]
#[allow(non_snake_case)]
#[tracing::instrument(level = "debug", skip(archive_reader))]
/// Returns the number of transactions in a block matching the given block number.
pub async fn monad_eth_getBlockTransactionCountByNumber<T: Triedb>(
    triedb_env: &T,
    archive_reader: &Option<ArchiveReader>,
    params: MonadEthGetBlockTransactionCountByNumberParams,
) -> JsonRpcResult<Option<String>> {
    trace!("monad_eth_getBlockTransactionCountByNumber: {params:?}");

    let block_key = get_block_key_from_tag(triedb_env, params.block_tag);
    if triedb_env
        .get_block_header(block_key)
        .await
        .map_err(JsonRpcError::internal_error)?
        .is_some()
    {
        // if block header is present but transactions are not, the block is statesynced
        if let Ok(transactions) = triedb_env.get_transactions(block_key).await {
            return Ok(Some(format!("0x{:x}", transactions.len())));
        }
    }

    // try archive if block number or transactions not found and archive reader specified
    if let (Some(archive_reader), BlockKey::Finalized(FinalizedBlockKey(block_num))) =
        (archive_reader, block_key)
    {
        if let Ok(block) = archive_reader.get_block_by_number(block_num.0).await {
            return Ok(Some(format!("0x{:x}", block.body.transactions.len())));
        }
    }

    Ok(None)
}

pub fn map_block_receipts<R>(
    transactions: Vec<TxEnvelopeWithSender>,
    receipts: Vec<ReceiptWithLogIndex>,
    block_header: &RlpHeader,
    block_hash: FixedBytes<32>,
    f: impl Fn(TransactionReceipt) -> R,
) -> Result<Vec<R>, JsonRpcError> {
    let block_num: u64 = block_header.number;

    if transactions.len() != receipts.len() {
        Err(JsonRpcError::internal_error(
            "number of receipts and txs mismatch".into(),
        ))?;
    }

    let mut prev_receipt = None;

    transactions
        .iter()
        .zip(receipts)
        .enumerate()
        .map(|(tx_index, (tx, receipt))| -> Result<R, JsonRpcError> {
            let prev_receipt = prev_receipt.replace(receipt.to_owned());
            let gas_used = if let Some(prev_receipt) = &prev_receipt {
                receipt.receipt.cumulative_gas_used() - prev_receipt.receipt.cumulative_gas_used()
            } else {
                receipt.receipt.cumulative_gas_used()
            };

            let parsed_receipt = parse_tx_receipt(
                block_header.base_fee_per_gas,
                Some(block_header.timestamp),
                block_hash,
                tx.to_owned(),
                gas_used,
                receipt,
                block_num,
                tx_index as u64,
            )?;

            Ok(f(parsed_receipt))
        })
        .collect()
}

pub fn block_receipts(
    transactions: Vec<TxEnvelopeWithSender>,
    receipts: Vec<ReceiptWithLogIndex>,
    block_header: &RlpHeader,
    block_hash: FixedBytes<32>,
) -> Result<Vec<TransactionReceipt>, JsonRpcError> {
    map_block_receipts(
        transactions,
        receipts,
        block_header,
        block_hash,
        |receipt| receipt,
    )
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetBlockReceiptsParams {
    block: BlockTagOrHash,
}

#[derive(Serialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetBlockReceiptsResult(Vec<MonadTransactionReceipt>);

#[rpc(method = "eth_getBlockReceipts")]
#[allow(non_snake_case)]
#[tracing::instrument(level = "debug", skip(archive_reader))]
/// Returns the receipts of a block by number or hash.
pub async fn monad_eth_getBlockReceipts<T: Triedb>(
    triedb_env: &T,
    archive_reader: &Option<ArchiveReader>,
    params: MonadEthGetBlockReceiptsParams,
) -> JsonRpcResult<Option<MonadEthGetBlockReceiptsResult>> {
    trace!("monad_eth_getBlockReceipts: {params:?}");

    let block_key = get_block_key_from_tag_or_hash(triedb_env, params.block).await?;

    if let Some(header) = triedb_env
        .get_block_header(block_key)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        // if block header is present but transactions are not, the block is statesynced
        if let Ok(transactions) = triedb_env.get_transactions(block_key).await {
            let receipts = triedb_env
                .get_receipts(block_key)
                .await
                .map_err(JsonRpcError::internal_error)?;
            let block_receipts = map_block_receipts(
                transactions,
                receipts,
                &header.header,
                header.hash,
                MonadTransactionReceipt,
            )?;
            return Ok(Some(MonadEthGetBlockReceiptsResult(block_receipts)));
        }
    }

    // try archive if header or transactions not found and archive reader specified
    if let (Some(archive_reader), BlockKey::Finalized(FinalizedBlockKey(block_num))) =
        (archive_reader, block_key)
    {
        if let Ok(receipts_with_log_index) = archive_reader.get_block_receipts(block_num.0).await {
            if let Ok(block) = archive_reader.get_block_by_number(block_num.0).await {
                let block_receipts = map_block_receipts(
                    block.body.transactions,
                    receipts_with_log_index,
                    &block.header,
                    block.header.hash_slow(),
                    MonadTransactionReceipt,
                )?;
                return Ok(Some(MonadEthGetBlockReceiptsResult(block_receipts)));
            }
        }
    }

    Ok(None)
}
