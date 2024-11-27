use alloy_primitives::{
    aliases::{U256, U64},
    FixedBytes,
};
use monad_archive::archive_reader::ArchiveReader;
use monad_rpc_docs::rpc;
use monad_triedb_utils::triedb_env::Triedb;
use reth_primitives::{Header as RlpHeader, ReceiptWithBloom, TransactionSigned};
use reth_rpc_types::{Block, BlockTransactions, Header, Transaction, TransactionReceipt};
use serde::{Deserialize, Serialize};
use tracing::trace;

use crate::{
    eth_json_types::{
        BlockReference, BlockTags, EthHash, MonadBlock, MonadTransactionReceipt, Quantity,
    },
    eth_txn_handlers::{parse_tx_content, parse_tx_receipt},
    jsonrpc::{JsonRpcError, JsonRpcResult},
};

pub async fn get_block_num_from_tag<T: Triedb>(
    triedb_env: &T,
    tag: BlockTags,
) -> JsonRpcResult<u64> {
    match tag {
        BlockTags::Number(n) => Ok(n.0),
        BlockTags::Latest => triedb_env
            .get_latest_block()
            .await
            .map_err(JsonRpcError::internal_error),
    }
}

fn parse_block_content(
    block_hash: FixedBytes<32>,
    rlp_header: RlpHeader,
    transactions: Vec<TransactionSigned>,
    return_full_txns: bool,
) -> JsonRpcResult<Option<MonadEthGetBlock>> {
    let size = rlp_header.size();

    // parse block header
    let header = Header {
        hash: Some(block_hash),
        parent_hash: rlp_header.parent_hash,
        uncles_hash: rlp_header.ommers_hash,
        miner: rlp_header.beneficiary,
        state_root: rlp_header.state_root,
        transactions_root: rlp_header.transactions_root,
        receipts_root: rlp_header.receipts_root,
        withdrawals_root: rlp_header.withdrawals_root,
        number: Some(U256::from(rlp_header.number)),
        gas_used: U256::from(rlp_header.gas_used),
        gas_limit: U256::from(rlp_header.gas_limit),
        extra_data: rlp_header.extra_data,
        logs_bloom: rlp_header.logs_bloom,
        timestamp: U256::from(rlp_header.timestamp),
        difficulty: rlp_header.difficulty,
        mix_hash: Some(rlp_header.mix_hash),
        nonce: Some(rlp_header.nonce.to_be_bytes().into()),
        base_fee_per_gas: rlp_header.base_fee_per_gas.map(U256::from),
        blob_gas_used: rlp_header.blob_gas_used.map(U64::from),
        excess_blob_gas: rlp_header.excess_blob_gas.map(U64::from),
        parent_beacon_block_root: rlp_header.parent_beacon_block_root,
    };

    // parse transactions
    let transactions = if return_full_txns {
        BlockTransactions::Full(
            transactions
                .into_iter()
                .enumerate()
                .map(|(index, tx)| {
                    parse_tx_content(
                        block_hash,
                        rlp_header.number,
                        rlp_header.base_fee_per_gas,
                        tx,
                        index as u64,
                    )
                })
                .collect::<Result<Vec<Transaction>, JsonRpcError>>()?,
        )
    } else {
        BlockTransactions::Hashes(transactions.iter().map(TransactionSigned::hash).collect())
    };

    // NOTE: no withdrawals currently in monad-bft
    let retval = Block {
        header,
        transactions,
        uncles: vec![],
        total_difficulty: Some(rlp_header.difficulty),
        withdrawals: None,
        size: Some(U256::from(size)),
        other: Default::default(),
    };

    Ok(Some(MonadEthGetBlock {
        block: MonadBlock(retval),
    }))
}

#[rpc(method = "eth_blockNumber")]
#[allow(non_snake_case)]
/// Returns the number of most recent block.
pub async fn monad_eth_blockNumber<T: Triedb>(triedb_env: &T) -> JsonRpcResult<Quantity> {
    trace!("monad_eth_blockNumber");

    let block_num = triedb_env
        .get_latest_block()
        .await
        .map_err(JsonRpcError::internal_error)?;
    Ok(Quantity(block_num))
}

#[rpc(method = "eth_chainId", ignore = "chain_id")]
#[allow(non_snake_case)]
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
/// Returns information about a block by hash.
pub async fn monad_eth_getBlockByHash<T: Triedb>(
    triedb_env: &T,
    archive_reader: &Option<ArchiveReader>,
    params: MonadEthGetBlockByHashParams,
) -> JsonRpcResult<Option<MonadEthGetBlock>> {
    trace!("monad_eth_getBlockByHash: {params:?}");

    let latest_block_num = get_block_num_from_tag(triedb_env, BlockTags::Latest).await?;
    if let Some(block_num) = triedb_env
        .get_block_number_by_hash(params.block_hash.0, latest_block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        if let Some(header) = triedb_env
            .get_block_header(block_num)
            .await
            .map_err(JsonRpcError::internal_error)?
        {
            let transactions = triedb_env
                .get_transactions(block_num)
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
        if let Ok(block) = archive_reader.get_block_by_hash(&params.block_hash.0).await {
            return parse_block_content(
                block.header.hash_slow(),
                block.header,
                block.body,
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
/// Returns information about a block by number.
pub async fn monad_eth_getBlockByNumber<T: Triedb>(
    triedb_env: &T,
    archive_reader: &Option<ArchiveReader>,
    params: MonadEthGetBlockByNumberParams,
) -> JsonRpcResult<Option<MonadEthGetBlock>> {
    trace!("monad_eth_getBlockByNumber: {params:?}");

    let block_num = get_block_num_from_tag(triedb_env, params.block_number).await?;

    if let Some(header) = triedb_env
        .get_block_header(block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        let transactions = triedb_env
            .get_transactions(block_num)
            .await
            .map_err(JsonRpcError::internal_error)?;
        return parse_block_content(
            header.hash,
            header.header,
            transactions,
            params.return_full_txns,
        );
    }

    // try archive if header not found and archive reader specified
    if let Some(archive_reader) = archive_reader {
        if let Ok(block) = archive_reader.get_block_by_number(block_num).await {
            return parse_block_content(
                block.header.hash_slow(),
                block.header,
                block.body,
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
/// Returns the number of transactions in a block from a block matching the given block hash.
pub async fn monad_eth_getBlockTransactionCountByHash<T: Triedb>(
    triedb_env: &T,
    archive_reader: &Option<ArchiveReader>,
    params: MonadEthGetBlockTransactionCountByHashParams,
) -> JsonRpcResult<Option<String>> {
    trace!("monad_eth_getBlockTransactionCountByHash: {params:?}");

    let latest_block_num = get_block_num_from_tag(triedb_env, BlockTags::Latest).await?;
    if let Some(block_num) = triedb_env
        .get_block_number_by_hash(params.block_hash.0, latest_block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        let transactions = triedb_env
            .get_transactions(block_num)
            .await
            .map_err(JsonRpcError::internal_error)?;
        return Ok(Some(format!("0x{:x}", transactions.len())));
    }

    // try archive if block hash not found and archive reader specified
    if let Some(archive_reader) = archive_reader {
        if let Ok(block) = archive_reader.get_block_by_hash(&params.block_hash.0).await {
            return Ok(Some(format!("0x{:x}", block.body.len())));
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
/// Returns the number of transactions in a block matching the given block number.
pub async fn monad_eth_getBlockTransactionCountByNumber<T: Triedb>(
    triedb_env: &T,
    archive_reader: &Option<ArchiveReader>,
    params: MonadEthGetBlockTransactionCountByNumberParams,
) -> JsonRpcResult<Option<String>> {
    trace!("monad_eth_getBlockTransactionCountByNumber: {params:?}");

    let block_num = get_block_num_from_tag(triedb_env, params.block_tag).await?;
    if triedb_env
        .get_block_header(block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
        .is_some()
    {
        let transactions = triedb_env
            .get_transactions(block_num)
            .await
            .map_err(JsonRpcError::internal_error)?;
        return Ok(Some(format!("0x{:x}", transactions.len())));
    }

    // try archive if block number not found and archive reader specified
    if let Some(archive_reader) = archive_reader {
        if let Ok(block) = archive_reader.get_block_by_number(block_num).await {
            return Ok(Some(format!("0x{:x}", block.body.len())));
        }
    }

    Ok(None)
}

pub async fn map_block_receipts<R>(
    transactions: &[TransactionSigned],
    receipts: Vec<ReceiptWithBloom>,
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
        .zip(receipts.into_iter())
        .enumerate()
        .map(|(tx_index, (tx, receipt))| -> Result<R, JsonRpcError> {
            let prev_receipt = prev_receipt.replace(receipt.receipt.to_owned());
            let gas_used = if let Some(prev_receipt) = prev_receipt {
                receipt.receipt.cumulative_gas_used - prev_receipt.cumulative_gas_used
            } else {
                receipt.receipt.cumulative_gas_used
            };

            let parsed_receipt = parse_tx_receipt(
                block_header.base_fee_per_gas,
                block_hash,
                tx,
                gas_used,
                receipt,
                block_num,
                tx_index as u64,
            )?;

            Ok(f(parsed_receipt))
        })
        .collect()
}

pub async fn block_receipts(
    transactions: &[TransactionSigned],
    receipts: Vec<ReceiptWithBloom>,
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
    .await
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetBlockReceiptsParams {
    block_reference: BlockReference,
}

#[derive(Serialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetBlockReceiptsResult(Vec<MonadTransactionReceipt>);

#[rpc(method = "eth_getBlockReceipts")]
#[allow(non_snake_case)]
/// Returns the receipts of a block by number or hash.
pub async fn monad_eth_getBlockReceipts<T: Triedb>(
    triedb_env: &T,
    archive_reader: &Option<ArchiveReader>,
    params: MonadEthGetBlockReceiptsParams,
) -> JsonRpcResult<Option<MonadEthGetBlockReceiptsResult>> {
    trace!("monad_eth_getBlockReceipts: {params:?}");

    let block_num = match params.block_reference {
        BlockReference::BlockTags(block_tag) => {
            get_block_num_from_tag(triedb_env, block_tag).await?
        }
        BlockReference::EthHash(block_hash) => {
            let latest_block_num = get_block_num_from_tag(triedb_env, BlockTags::Latest).await?;
            match triedb_env
                .get_block_number_by_hash(block_hash.0, latest_block_num)
                .await
                .map_err(JsonRpcError::internal_error)?
            {
                Some(block_num) => block_num,
                None => return Ok(None),
            }
        }
    };

    if let Some(header) = triedb_env
        .get_block_header(block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        let transactions = triedb_env
            .get_transactions(block_num)
            .await
            .map_err(JsonRpcError::internal_error)?;
        let receipts = triedb_env
            .get_receipts(block_num)
            .await
            .map_err(JsonRpcError::internal_error)?;
        let block_receipts = map_block_receipts(
            &transactions,
            receipts,
            &header.header,
            header.hash,
            MonadTransactionReceipt,
        )
        .await?;
        return Ok(Some(MonadEthGetBlockReceiptsResult(block_receipts)));
    }

    // try archive if header not found and archive reader specified
    if let Some(archive_reader) = archive_reader {
        if let Ok(bloom_receipts) = archive_reader.get_block_receipts(block_num).await {
            if let Ok(block) = archive_reader.get_block_by_number(block_num).await {
                let block_receipts = map_block_receipts(
                    &block.body,
                    bloom_receipts,
                    &block.header,
                    block.hash_slow(),
                    MonadTransactionReceipt,
                )
                .await?;
                return Ok(Some(MonadEthGetBlockReceiptsResult(block_receipts)));
            }
        }
    }

    Ok(None)
}
