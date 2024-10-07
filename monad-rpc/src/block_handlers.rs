use std::ops::Div;

use alloy_primitives::aliases::{U256, U64};
use monad_rpc_docs::rpc;
use reth_primitives::Block as EthBlock;
use reth_rpc_types::{Block, BlockTransactions, Header, TransactionReceipt, Withdrawal};
use serde::{Deserialize, Serialize};
use tracing::{error, trace};

use crate::{
    block_util::{get_block_from_num, get_block_num_from_tag, BlockResult, FileBlockReader},
    eth_json_types::{BlockTags, EthHash, MonadBlock, MonadTransactionReceipt, Quantity},
    eth_txn_handlers::{parse_tx_content, parse_tx_receipt},
    jsonrpc::{JsonRpcError, JsonRpcResult},
    receipt::{decode_receipt, ReceiptDetails},
    triedb::{TriedbEnv, TriedbResult},
};

fn parse_block_content(value: &EthBlock, return_full_txns: bool) -> Option<Block> {
    // parse block header
    let header = Header {
        hash: None, //FIXME: figure out how to get this from consensus
        parent_hash: value.header.parent_hash,
        uncles_hash: value.header.ommers_hash,
        miner: value.header.beneficiary,
        state_root: value.header.state_root,
        transactions_root: value.header.transactions_root,
        receipts_root: value.header.receipts_root,
        withdrawals_root: value.header.withdrawals_root,
        number: Some(U256::from(value.header.number)),
        gas_used: U256::from(value.header.gas_used),
        gas_limit: U256::from(value.header.gas_limit),
        extra_data: value.header.clone().extra_data,
        logs_bloom: value.header.logs_bloom,
        // timestamp in block header is in Unix milliseconds but we parse it
        // to be in Unix seconds here for integration compatability
        timestamp: U256::from(value.header.timestamp.div(1000)),
        difficulty: value.header.difficulty,
        mix_hash: Some(value.header.mix_hash),
        nonce: Some(value.header.nonce.to_be_bytes().into()),
        base_fee_per_gas: value.header.base_fee_per_gas.map(U256::from),
        blob_gas_used: value.header.blob_gas_used.map(U64::from),
        excess_blob_gas: value.header.excess_blob_gas.map(U64::from),
        parent_beacon_block_root: value.header.parent_beacon_block_root,
    };

    // NOTE: depends on our staking logic
    // parse validators withdrawals
    let withdrawals = if value.header.withdrawals_root.is_some() {
        value.clone().withdrawals.map(|withdrawals| {
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
                .body
                .iter()
                .enumerate()
                .map(|(index, tx)| parse_tx_content(value, tx, index as u64).unwrap_or_default())
                .collect();
            BlockTransactions::Full(transactions)
        }
        false => {
            let transactions = value.body.iter().map(|tx| tx.hash()).collect();
            BlockTransactions::Hashes(transactions)
        }
    };

    let retval = Block {
        header,
        transactions,
        uncles: vec![],
        total_difficulty: Some(value.header.difficulty),
        withdrawals,
        size: Some(U256::from(value.size())),
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
        _ => {
            error!("triedb did not have latest block number");
            Err(JsonRpcError::internal_error(
                "missing latest block number".into(),
            ))
        }
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
    block_hash: EthHash,
    return_full_txns: bool,
}

#[derive(Serialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetBlock {
    #[serde(flatten)]
    block: MonadBlock,
}

#[rpc(method = "eth_getBlockByHash", ignore = "file_ledger_reader")]
#[allow(non_snake_case)]
/// Returns information about a block by hash.
pub async fn monad_eth_getBlockByHash(
    file_ledger_reader: &FileBlockReader,
    params: MonadEthGetBlockByHashParams,
) -> JsonRpcResult<Option<MonadEthGetBlock>> {
    trace!("monad_eth_getBlockByHash: {params:?}");

    let key = params.block_hash.into();
    let Some(block) = file_ledger_reader.get_block_by_hash(key) else {
        return Ok(None);
    };

    let retval = parse_block_content(&block, params.return_full_txns);
    Ok(retval.map(|block| MonadEthGetBlock {
        block: MonadBlock(block),
    }))
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetBlockByNumberParams {
    block_number: BlockTags,
    return_full_txns: bool,
}

#[rpc(method = "eth_getBlockByNumber", ignore = "file_ledger_reader")]
#[allow(non_snake_case)]
/// Returns information about a block by number.
pub async fn monad_eth_getBlockByNumber(
    file_ledger_reader: &FileBlockReader,
    triedb_env: &TriedbEnv,
    params: MonadEthGetBlockByNumberParams,
) -> JsonRpcResult<Option<MonadEthGetBlock>> {
    trace!("monad_eth_getBlockByNumber: {params:?}");

    let block_num = get_block_num_from_tag(triedb_env, params.block_number).await?;
    let block = match get_block_from_num(file_ledger_reader, block_num).await {
        BlockResult::Block(b) => b,
        BlockResult::NotFound => return Ok(None),
        BlockResult::DecodeFailed(e) => {
            return Err(JsonRpcError::internal_error(format!(
                "decode block failed: {}",
                e
            )))
        }
    };

    let parsed_block = parse_block_content(&block, params.return_full_txns);
    Ok(parsed_block.map(|block| MonadEthGetBlock {
        block: MonadBlock(block),
    }))
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetBlockTransactionCountByHashParams {
    block_hash: EthHash,
}

#[rpc(
    method = "eth_getBlockTransactionCountByHash",
    ignore = "file_ledger_reader"
)]
#[allow(non_snake_case)]
/// Returns the number of transactions in a block from a block matching the given block hash.
pub async fn monad_eth_getBlockTransactionCountByHash(
    file_ledger_reader: &FileBlockReader,
    params: MonadEthGetBlockTransactionCountByHashParams,
) -> JsonRpcResult<Option<String>> {
    trace!("monad_eth_getBlockTransactionCountByHash: {params:?}");

    let key = params.block_hash.into();
    let Some(block) = file_ledger_reader.get_block_by_hash(key) else {
        return Ok(None);
    };

    let count = block.body.len() as u64;
    Ok(Some(format!("0x{:x}", count)))
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetBlockTransactionCountByNumberParams {
    block_tag: BlockTags,
}

#[rpc(
    method = "eth_getBlockTransactionCountByNumber",
    ignore = "file_ledger_reader"
)]
#[allow(non_snake_case)]
/// Returns the number of transactions in a block matching the given block number.
pub async fn monad_eth_getBlockTransactionCountByNumber(
    file_ledger_reader: &FileBlockReader,
    triedb_env: &TriedbEnv,
    params: MonadEthGetBlockTransactionCountByNumberParams,
) -> JsonRpcResult<Option<String>> {
    trace!("monad_eth_getBlockTransactionCountByNumber: {params:?}");

    let block_num = get_block_num_from_tag(triedb_env, params.block_tag).await?;
    let block = match get_block_from_num(file_ledger_reader, block_num).await {
        BlockResult::Block(b) => b,
        BlockResult::NotFound => return Ok(None),
        BlockResult::DecodeFailed(e) => {
            return Err(JsonRpcError::internal_error(format!(
                "decode block failed: {}",
                e
            )))
        }
    };

    let count = block.body.len() as u64;
    Ok(Some(format!("0x{:x}", count)))
}

pub async fn block_receipts(
    triedb_env: &TriedbEnv,
    block: EthBlock,
) -> Result<Vec<TransactionReceipt>, JsonRpcError> {
    let block_num: u64 = block.number;
    let mut block_receipts: Vec<(ReceiptDetails, u64)> = vec![];
    for txn_index in 0..block.body.len() {
        match triedb_env.get_receipt(txn_index as u64, block_num).await {
            TriedbResult::Null => continue,
            TriedbResult::Receipt(rlp_receipt) => {
                let mut rlp_buf = rlp_receipt.as_slice();
                let receipt = decode_receipt(&mut rlp_buf).map_err(|e| {
                    JsonRpcError::internal_error(format!("decode receipt failed: {}", e))
                })?;
                block_receipts.push((receipt, txn_index as u64));
            }
            _ => return Err(JsonRpcError::internal_error("error reading from db".into())),
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

    if block_receipts.len() != block.body.len() {
        return Err(JsonRpcError::internal_error("receipts unavailable".into()));
    }

    Ok(block_receipts)
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetBlockReceiptsParams {
    block_tag: BlockTags,
}

#[derive(Serialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetBlockReceiptsResult(Vec<MonadTransactionReceipt>);

#[rpc(method = "eth_getBlockReceipts", ignore = "file_ledger_reader")]
#[allow(non_snake_case)]
/// Returns the receipts of a block by number or hash.
pub async fn monad_eth_getBlockReceipts(
    file_ledger_reader: &FileBlockReader,
    triedb_env: &TriedbEnv,
    params: MonadEthGetBlockReceiptsParams,
) -> JsonRpcResult<Option<MonadEthGetBlockReceiptsResult>> {
    trace!("monad_eth_getBlockReceipts: {params:?}");

    let block_num = get_block_num_from_tag(triedb_env, params.block_tag).await?;

    let Ok(encoded_block) = file_ledger_reader
        .async_read_encoded_eth_block(block_num)
        .await
    else {
        return Ok(None);
    };
    let decoded_block = match file_ledger_reader.decode_eth_block(encoded_block) {
        Ok(b) => b,
        Err(e) => {
            return Err(JsonRpcError::internal_error(format!(
                "decode block failed: {}",
                e
            )));
        }
    };

    let block_receipts = block_receipts(triedb_env, decoded_block).await?;
    Ok(Some(MonadEthGetBlockReceiptsResult(
        block_receipts
            .into_iter()
            .map(MonadTransactionReceipt)
            .collect(),
    )))
}
