use alloy_primitives::{
    aliases::{U256, U64},
    FixedBytes,
};
use monad_rpc_docs::rpc;
use reth_primitives::{Header as RlpHeader, ReceiptWithBloom, TransactionSigned};
use reth_rpc_types::{Block, BlockTransactions, Header, Transaction, TransactionReceipt};
use serde::{Deserialize, Serialize};
use tracing::trace;

use crate::{
    eth_json_types::{BlockTags, EthHash, MonadBlock, MonadTransactionReceipt, Quantity},
    eth_txn_handlers::{parse_tx_content, parse_tx_receipt},
    jsonrpc::{JsonRpcError, JsonRpcResult},
    triedb::{get_block_num_from_tag, Triedb},
};

fn parse_block_content(
    block_hash: FixedBytes<32>,
    rlp_header: &RlpHeader,
    transactions: &[TransactionSigned],
    return_full_txns: bool,
) -> Result<Block, JsonRpcError> {
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
        extra_data: rlp_header.clone().extra_data,
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
    let transactions: Result<BlockTransactions, JsonRpcError> = match return_full_txns {
        true => {
            let transactions: Result<Vec<Transaction>, JsonRpcError> = transactions
                .iter()
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
                .collect();
            transactions.map(BlockTransactions::Full)
        }
        false => {
            let transactions = transactions.iter().map(|tx| tx.hash()).collect();
            Ok(BlockTransactions::Hashes(transactions))
        }
    };
    let transactions = transactions?;

    // NOTE: no withdrawals currently in monad-bft
    let retval = Block {
        header,
        transactions,
        uncles: vec![],
        total_difficulty: Some(rlp_header.difficulty),
        withdrawals: None,
        size: Some(U256::from(rlp_header.size())),
        other: Default::default(),
    };

    Ok(retval)
}

#[rpc(method = "eth_blockNumber")]
#[allow(non_snake_case)]
/// Returns the number of most recent block.
pub async fn monad_eth_blockNumber<T: Triedb>(triedb_env: &T) -> JsonRpcResult<Quantity> {
    trace!("monad_eth_blockNumber");

    let block_num = triedb_env.get_latest_block().await?;
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
    params: MonadEthGetBlockByHashParams,
) -> JsonRpcResult<Option<MonadEthGetBlock>> {
    trace!("monad_eth_getBlockByHash: {params:?}");

    let Some(block_num) = triedb_env
        .get_block_number_by_hash(params.block_hash.0)
        .await?
    else {
        return Ok(None);
    };

    let header = match triedb_env.get_block_header(block_num).await? {
        Some(header) => header,
        None => return Ok(None),
    };
    let transactions = triedb_env.get_transactions(block_num).await?;

    parse_block_content(
        params.block_hash.0.into(),
        &header.header,
        &transactions,
        params.return_full_txns,
    )
    .map(|block| {
        Some(MonadEthGetBlock {
            block: MonadBlock(block),
        })
    })
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
    params: MonadEthGetBlockByNumberParams,
) -> JsonRpcResult<Option<MonadEthGetBlock>> {
    trace!("monad_eth_getBlockByNumber: {params:?}");

    let block_num = get_block_num_from_tag(triedb_env, params.block_number).await?;

    let header = match triedb_env.get_block_header(block_num).await? {
        Some(header) => header,
        None => return Ok(None),
    };
    let transactions = triedb_env.get_transactions(block_num).await?;

    parse_block_content(
        header.hash,
        &header.header,
        &transactions,
        params.return_full_txns,
    )
    .map(|block| {
        Some(MonadEthGetBlock {
            block: MonadBlock(block),
        })
    })
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
    params: MonadEthGetBlockTransactionCountByHashParams,
) -> JsonRpcResult<Option<String>> {
    trace!("monad_eth_getBlockTransactionCountByHash: {params:?}");

    let Some(block_num) = triedb_env
        .get_block_number_by_hash(params.block_hash.0)
        .await?
    else {
        return Ok(None);
    };

    let transactions = triedb_env.get_transactions(block_num).await?;
    Ok(Some(format!("0x{:x}", transactions.len())))
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
    params: MonadEthGetBlockTransactionCountByNumberParams,
) -> JsonRpcResult<Option<String>> {
    trace!("monad_eth_getBlockTransactionCountByNumber: {params:?}");

    let block_num = get_block_num_from_tag(triedb_env, params.block_tag).await?;

    let transactions = triedb_env.get_transactions(block_num).await?;
    Ok(Some(format!("0x{:x}", transactions.len())))
}

pub async fn block_receipts<T: Triedb>(
    triedb_env: &T,
    block_header: &RlpHeader,
    block_hash: FixedBytes<32>,
    transactions: &[TransactionSigned],
) -> Result<Vec<TransactionReceipt>, JsonRpcError> {
    let block_num: u64 = block_header.number;
    let transaction_count = transactions.len();
    let mut block_receipts: Vec<(ReceiptWithBloom, usize)> = vec![];
    for txn_index in 0..transaction_count {
        let receipt = match triedb_env.get_receipt(txn_index as u64, block_num).await? {
            Some(receipt) => receipt,
            None => continue,
        };
        block_receipts.push((receipt, txn_index));
    }

    let block_receipts: Result<Vec<TransactionReceipt>, JsonRpcError> = block_receipts
        .into_iter()
        .scan(None, |prev, (receipt, txn_index)| {
            let parsed_receipt = parse_tx_receipt(
                block_header,
                block_hash,
                &transactions[txn_index],
                prev.to_owned(),
                receipt.clone(),
                block_num,
                txn_index,
            );
            *prev = Some(receipt);
            Some(parsed_receipt)
        })
        .collect();
    let block_receipts = block_receipts?;

    if block_receipts.len() != transaction_count {
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

#[rpc(method = "eth_getBlockReceipts")]
#[allow(non_snake_case)]
/// Returns the receipts of a block by number or hash.
pub async fn monad_eth_getBlockReceipts<T: Triedb>(
    triedb_env: &T,
    params: MonadEthGetBlockReceiptsParams,
) -> JsonRpcResult<Option<MonadEthGetBlockReceiptsResult>> {
    trace!("monad_eth_getBlockReceipts: {params:?}");

    let block_num = get_block_num_from_tag(triedb_env, params.block_tag).await?;

    let header = match triedb_env.get_block_header(block_num).await? {
        Some(header) => header,
        None => return Ok(None),
    };
    let transactions = triedb_env.get_transactions(block_num).await?;

    let block_receipts =
        block_receipts(triedb_env, &header.header, header.hash, &transactions).await?;
    Ok(Some(MonadEthGetBlockReceiptsResult(
        block_receipts
            .into_iter()
            .map(MonadTransactionReceipt)
            .collect(),
    )))
}
