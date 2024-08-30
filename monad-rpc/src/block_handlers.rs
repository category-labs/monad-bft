use alloy_primitives::aliases::{U256, U64};
use monad_blockdb::BlockValue;
use monad_blockdb_utils::BlockDbEnv;
use monad_rpc_docs::rpc;
use reth_rpc_types::{Block, BlockTransactions, Header, TransactionReceipt, Withdrawal};
use serde::{Deserialize, Serialize};
use tracing::trace;

use crate::{
    eth_json_types::{
        deserialize_block_tags, deserialize_fixed_data, BlockTags, EthHash, MonadBlock,
        MonadTransactionReceipt, Quantity,
    },
    eth_txn_handlers::{parse_tx_content, parse_tx_receipt},
    jsonrpc::{JsonRpcError, JsonRpcResult},
    receipt::{decode_receipt, ReceiptDetails},
    triedb::{TriedbEnv, TriedbResult},
};

fn parse_block_content(value: &BlockValue, return_full_txns: bool) -> Option<Block> {
    // parse block header
    let header = Header {
        hash: Some(value.block_id()),
        parent_hash: value.block.header.parent_hash,
        uncles_hash: value.block.header.ommers_hash,
        miner: value.block.header.beneficiary,
        state_root: value.block.header.state_root,
        transactions_root: value.block.header.transactions_root,
        receipts_root: value.block.header.receipts_root,
        withdrawals_root: value.block.header.withdrawals_root,
        number: Some(U256::from(value.block.header.number)),
        gas_used: U256::from(value.block.header.gas_used),
        gas_limit: U256::from(value.block.header.gas_limit),
        extra_data: value.block.header.clone().extra_data,
        logs_bloom: value.block.header.logs_bloom,
        // timestamp in block header is in Unix milliseconds but we parse it
        // to be in Unix seconds here for integration compatability
        timestamp: U256::ZERO,
        difficulty: value.block.header.difficulty,
        mix_hash: Some(value.block.header.mix_hash),
        nonce: Some(value.block.header.nonce.to_be_bytes().into()),
        base_fee_per_gas: value.block.header.base_fee_per_gas.map(U256::from),
        blob_gas_used: value.block.header.blob_gas_used.map(U64::from),
        excess_blob_gas: value.block.header.excess_blob_gas.map(U64::from),
        parent_beacon_block_root: value.block.header.parent_beacon_block_root,
    };

    // NOTE: depends on our staking logic
    // parse validators withdrawals
    let withdrawals = if value.block.header.withdrawals_root.is_some() {
        value.block.clone().withdrawals.map(|withdrawals| {
            withdrawals
                .into_iter()
                .map(|withdrawal| Withdrawal {
                    index: withdrawal.index,
                    address: withdrawal.address,
                    validator_index: withdrawal.validator_index,
                    amount: withdrawal.amount,
                })
                .collect()
        })
    } else {
        None
    };

    // parse transactions
    let transactions: BlockTransactions = match return_full_txns {
        true => {
            let transactions = value
                .block
                .body
                .iter()
                .enumerate()
                .map(|(index, tx)| parse_tx_content(value, tx, index as u64).unwrap_or_default())
                .collect();
            BlockTransactions::Full(transactions)
        }
        false => {
            let transactions = value.block.body.iter().map(|tx| tx.hash()).collect();
            BlockTransactions::Hashes(transactions)
        }
    };

    let retval = Block {
        header,
        transactions,
        uncles: vec![],
        total_difficulty: Some(value.block.header.difficulty),
        withdrawals,
        size: Some(U256::from(value.block.size())),
        other: Default::default(),
    };

    Some(retval)
}

#[rpc(method = "eth_blockNumber")]
#[allow(non_snake_case)]
/// Returns the number of most recent block.
pub async fn monad_eth_blockNumber(triedb_env: &TriedbEnv) -> JsonRpcResult<Quantity> {
    trace!("monad_eth_blockNumber");

    match triedb_env.get_latest_block().await {
        TriedbResult::BlockNum(num) => Ok(Quantity(num)),
        _ => Err(JsonRpcError::internal_error()),
    }
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
    #[serde(deserialize_with = "deserialize_fixed_data")]
    block_hash: EthHash,
    return_full_txns: bool,
}

#[derive(Serialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetBlock {
    #[serde(flatten)]
    block: Option<MonadBlock>,
}

#[rpc(method = "eth_getBlockByHash")]
#[allow(non_snake_case)]
/// Returns information about a block by hash.
pub async fn monad_eth_getBlockByHash(
    blockdb_env: &BlockDbEnv,
    params: MonadEthGetBlockByHashParams,
) -> JsonRpcResult<MonadEthGetBlock> {
    trace!("monad_eth_getBlockByHash: {params:?}");

    let key = params.block_hash.into();
    let Some(value) = blockdb_env.get_block_by_hash(key).await else {
        return Ok(MonadEthGetBlock { block: None });
    };

    let retval = parse_block_content(&value, params.return_full_txns);
    Ok(MonadEthGetBlock {
        block: retval.map(MonadBlock),
    })
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetBlockByNumberParams {
    #[serde(deserialize_with = "deserialize_block_tags")]
    block_number: BlockTags,
    return_full_txns: bool,
}

#[rpc(method = "eth_getBlockByNumber")]
#[allow(non_snake_case)]
/// Returns information about a block by number.
pub async fn monad_eth_getBlockByNumber(
    blockdb_env: &BlockDbEnv,
    params: MonadEthGetBlockByNumberParams,
) -> JsonRpcResult<MonadEthGetBlock> {
    trace!("monad_eth_getBlockByNumber: {params:?}");

    let Some(value) = blockdb_env
        .get_block_by_tag(params.block_number.into())
        .await
    else {
        return Ok(MonadEthGetBlock { block: None });
    };

    let retval = parse_block_content(&value, params.return_full_txns);
    Ok(MonadEthGetBlock {
        block: retval.map(MonadBlock),
    })
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetBlockTransactionCountByHashParams {
    #[serde(deserialize_with = "deserialize_fixed_data")]
    block_hash: EthHash,
}

#[rpc(method = "eth_getBlockTransactionCountByHash")]
#[allow(non_snake_case)]
/// Returns the number of transactions in a block from a block matching the given block hash.
pub async fn monad_eth_getBlockTransactionCountByHash(
    blockdb_env: &BlockDbEnv,
    params: MonadEthGetBlockTransactionCountByHashParams,
) -> JsonRpcResult<String> {
    trace!("monad_eth_getBlockTransactionCountByHash: {params:?}");

    let key = params.block_hash.into();
    let Some(value) = blockdb_env.get_block_by_hash(key).await else {
        return Ok(format!("0x{:x}", 0));
    };

    let count = value.block.body.len() as u64;
    Ok(format!("0x{:x}", count))
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetBlockTransactionCountByNumberParams {
    #[serde(deserialize_with = "deserialize_block_tags")]
    block_tag: BlockTags,
}

#[rpc(method = "eth_getBlockTransactionCountByNumber")]
#[allow(non_snake_case)]
/// Returns the number of transactions in a block matching the given block number.
pub async fn monad_eth_getBlockTransactionCountByNumber(
    blockdb_env: &BlockDbEnv,
    params: MonadEthGetBlockTransactionCountByNumberParams,
) -> JsonRpcResult<String> {
    trace!("monad_eth_getBlockTransactionCountByNumber: {params:?}");

    let Some(value) = blockdb_env.get_block_by_tag(params.block_tag.into()).await else {
        return Ok(format!("0x{:x}", 0));
    };

    let count = value.block.body.len() as u64;
    Ok(format!("0x{:x}", count))
}

pub async fn block_receipts(
    triedb_env: &TriedbEnv,
    block: BlockValue,
) -> Result<Vec<TransactionReceipt>, JsonRpcError> {
    let block_num: u64 = block.block.number;
    let mut block_receipts: Vec<(ReceiptDetails, u64)> = vec![];
    for txn_index in 0..block.block.body.len() {
        match triedb_env.get_receipt(txn_index as u64, block_num).await {
            TriedbResult::Null => continue,
            TriedbResult::Receipt(rlp_receipt) => {
                let mut rlp_buf = rlp_receipt.as_slice();
                let receipt =
                    decode_receipt(&mut rlp_buf).map_err(|_| JsonRpcError::internal_error())?;
                block_receipts.push((receipt, txn_index as u64));
            }
            _ => return Err(JsonRpcError::internal_error()),
        }
    }

    let block_receipts: Vec<TransactionReceipt> = block_receipts
        .into_iter()
        .scan(None, |prev, (receipt, txn_index)| {
            let parsed_receipt = parse_tx_receipt(
                &block,
                prev.to_owned(),
                receipt.clone(),
                block_num,
                txn_index,
            );
            *prev = Some(receipt);
            parsed_receipt
        })
        .collect();

    if block_receipts.len() != block.block.body.len() {
        return Err(JsonRpcError::internal_error());
    }

    Ok(block_receipts)
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetBlockReceiptsParams {
    #[serde(deserialize_with = "deserialize_block_tags")]
    block_tag: BlockTags,
}

#[derive(Serialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetBlockReceiptsResult {
    #[serde(flatten)]
    receipts: Vec<MonadTransactionReceipt>,
}

#[rpc(method = "eth_getBlockReceipts")]
#[allow(non_snake_case)]
/// Returns the receipts of a block by number or hash.
pub async fn monad_eth_getBlockReceipts(
    blockdb_env: &BlockDbEnv,
    triedb_env: &TriedbEnv,
    params: MonadEthGetBlockReceiptsParams,
) -> JsonRpcResult<MonadEthGetBlockReceiptsResult> {
    trace!("monad_eth_getBlockReceipts: {params:?}");

    let Some(block) = blockdb_env.get_block_by_tag(params.block_tag.into()).await else {
        return Ok(MonadEthGetBlockReceiptsResult { receipts: vec![] });
    };

    let block_receipts = block_receipts(triedb_env, block).await?;
    if block_receipts.is_empty() {
        Ok(MonadEthGetBlockReceiptsResult { receipts: vec![] })
    } else {
        Ok(MonadEthGetBlockReceiptsResult {
            receipts: block_receipts
                .into_iter()
                .map(MonadTransactionReceipt)
                .collect(),
        })
    }
}
