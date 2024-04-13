use alloy_primitives::aliases::{U256, U64};
use log::{debug, trace};
use monad_blockdb::{BlockTableKey, BlockTagKey, BlockValue};
use reth_primitives::BlockHash;
use reth_rpc_types::{Block, BlockTransactions, Header, Withdrawal};
use serde::Deserialize;
use serde_json::Value;

use crate::{
    blockdb::BlockDbEnv,
    eth_json_types::{
        deserialize_block_tags, deserialize_fixed_data, serialize_result, BlockTags, EthHash,
    },
    eth_txn_handlers::parse_tx_content,
    jsonrpc::JsonRpcError,
};

#[allow(non_snake_case)]
pub async fn monad_eth_blockNumber(blockdb_env: &BlockDbEnv) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_blockNumber");

    let Some(block) = blockdb_env
        .get_block_by_tag(BlockTags::Default(BlockTagKey::Latest))
        .await
    else {
        return serialize_result(format!("0x{:x}", 0));
    };

    serialize_result(format!("0x{:x}", block.block.number))
}

// TODO: does chainId come from a config file?
#[allow(non_snake_case)]
pub async fn monad_eth_chainId(blockdb_env: &BlockDbEnv) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_chainId");

    serialize_result(format!("0x{:x}", 1337))
}

#[derive(Deserialize, Debug)]
struct MonadEthGetBlockByHashParams {
    #[serde(deserialize_with = "deserialize_fixed_data")]
    block_hash: EthHash,
    return_full_txns: bool,
}

fn parse_block_content(value: &BlockValue, return_full_txns: bool) -> Option<Block> {
    // parse block header
    let header = Header {
        hash: Some(value.block.hash_slow()),
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
        timestamp: U256::from(value.block.header.timestamp),
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
                .map(|(index, tx)| parse_tx_content(value, tx, index as u64).unwrap())
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

#[allow(non_snake_case)]
pub async fn monad_eth_getBlockByHash(
    blockdb_env: &BlockDbEnv,
    params: Value,
) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_getBlockByHash: {params:?}");

    let p: MonadEthGetBlockByHashParams = match serde_json::from_value(params) {
        Ok(s) => s,
        Err(e) => {
            debug!("invalid params {e}");
            return Err(JsonRpcError::invalid_params());
        }
    };

    let key = BlockTableKey(BlockHash::new(p.block_hash.0));
    let Some(value) = blockdb_env.get_block_by_hash(key).await else {
        return serialize_result(None::<Block>);
    };

    let retval = parse_block_content(&value, p.return_full_txns);
    serialize_result(retval)
}

#[derive(Deserialize, Debug)]
struct MonadEthGetBlockByNumberParams {
    #[serde(deserialize_with = "deserialize_block_tags")]
    block_number: BlockTags,
    return_full_txns: bool,
}

#[allow(non_snake_case)]
pub async fn monad_eth_getBlockByNumber(
    blockdb_env: &BlockDbEnv,
    params: Value,
) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_getBlockByNumber: {params:?}");

    let p: MonadEthGetBlockByNumberParams = match serde_json::from_value(params) {
        Ok(s) => s,
        Err(e) => {
            debug!("invalid params {e}");
            return Err(JsonRpcError::invalid_params());
        }
    };

    let Some(value) = blockdb_env.get_block_by_tag(p.block_number).await else {
        return serialize_result(None::<Block>);
    };

    let retval = parse_block_content(&value, p.return_full_txns);
    serialize_result(retval)
}
