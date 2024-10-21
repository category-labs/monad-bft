use std::cmp::min;

use alloy_primitives::aliases::{B256, U128, U256, U64};
use monad_blockdb::{BlockValue, EthTxKey};
use monad_blockdb_utils::BlockDbEnv;
use monad_rpc_docs::rpc;
use reth_primitives::{transaction::TransactionKind, TransactionSigned};
use reth_rpc_types::{
    AccessListItem, BlockNumberOrTag, Filter, FilterBlockOption, FilteredParams, Log, Parity,
    Signature, Transaction, TransactionReceipt,
};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, trace, warn};

use crate::{
    block_handlers::block_receipts,
    eth_json_types::{
        deserialize_block_tags, deserialize_fixed_data, deserialize_quantity,
        deserialize_unformatted_data, BlockTagKey, BlockTags, EthHash, FixedData,
        MonadLog, MonadTransaction, MonadTransactionReceipt, Quantity, UnformattedData,
    },
    jsonrpc::{JsonRpcError, JsonRpcResult},
    receipt::{decode_receipt, ReceiptDetails},
    triedb::{TriedbEnv, TriedbResult},
};

pub fn parse_tx_content(
    value: &BlockValue,
    tx: &TransactionSigned,
    tx_index: u64,
) -> Option<Transaction> {
    // recover transaction signer
    let transaction = tx
        .clone()
        .into_ecrecovered_unchecked()
        .expect("transaction sender should exist");

    // parse fee parameters
    let base_fee = value.block.base_fee_per_gas;
    let gas_price = base_fee
        .and_then(|base_fee| {
            tx.effective_tip_per_gas(Some(base_fee))
                .map(|tip| tip + base_fee as u128)
        })
        .unwrap_or_else(|| tx.max_fee_per_gas());

    // parse signature
    let signature = Signature {
        r: tx.signature().r,
        s: tx.signature().s,
        v: U256::from(tx.signature().odd_y_parity as u8),
        y_parity: Some(Parity(tx.signature().odd_y_parity)),
    };

    // parse access list
    let access_list = tx.access_list().map(|list| {
        list.0
            .iter()
            .map(|item| AccessListItem {
                address: item.address.0.into(),
                storage_keys: item.storage_keys.iter().map(|key| key.0.into()).collect(),
            })
            .collect()
    });

    let retval = Transaction {
        hash: tx.hash(),
        nonce: U64::from(tx.nonce()),
        from: transaction.signer(),
        to: tx.to(),
        value: tx.value().into(),
        gas_price: Some(U128::from(gas_price)),
        max_fee_per_gas: Some(U128::from(tx.max_fee_per_gas())),
        max_priority_fee_per_gas: tx.max_priority_fee_per_gas().map(U128::from),
        signature: Some(signature),
        gas: U256::from(tx.gas_limit()),
        input: tx.input().clone(),
        chain_id: tx.chain_id().map(U64::from),
        access_list,
        transaction_type: Some(U64::from(tx.tx_type() as u8)),
        block_hash: Some(value.block_id()),
        block_number: Some(U256::from(value.block.number)),
        transaction_index: Some(U256::from(tx_index)),

        // only relevant for EIP-4844 transactions
        max_fee_per_blob_gas: None,
        blob_versioned_hashes: vec![],
        other: Default::default(),
    };

    Some(retval)
}

pub fn parse_tx_receipt(
    block: &BlockValue,
    prev_receipt: Option<ReceiptDetails>,
    receipt: ReceiptDetails,
    block_num: u64,
    tx_index: u64,
) -> Option<TransactionReceipt> {
    let tx = block.block.body.get(tx_index as usize)?;
    let transaction = tx
        .clone()
        .into_ecrecovered_unchecked()
        .expect("transaction sender should exist");
    let base_fee_per_gas = block.block.base_fee_per_gas.unwrap_or_default() as u128;
    // effective gas price is calculated according to eth json rpc specification
    let effective_gas_price = base_fee_per_gas
        + min(
            tx.max_fee_per_gas() - base_fee_per_gas,
            tx.max_priority_fee_per_gas().unwrap_or_default(),
        );
    let block_hash = Some(block.block_id());
    let block_number = Some(U256::from(block_num));
    let logs = receipt
        .logs
        .into_iter()
        .enumerate()
        .map(|(log_index, log_item)| Log {
            address: log_item.address,
            topics: log_item.topics,
            data: log_item.data,
            block_hash,
            block_number,
            transaction_hash: Some(tx.hash()),
            transaction_index: Some(U256::from(tx_index)),
            log_index: Some(U256::from(log_index)),
            removed: Default::default(),
        })
        .collect();

    let contract_address = match transaction.kind() {
        TransactionKind::Create => Some(transaction.signer().create(transaction.nonce())),
        _ => None,
    };

    let gas_used = if let Some(prev_receipt) = prev_receipt {
        U256::from(receipt.cumulative_gas_used - prev_receipt.cumulative_gas_used)
    } else {
        U256::from(receipt.cumulative_gas_used)
    };

    let tx_receipt = TransactionReceipt {
        transaction_type: receipt.tx_type,
        transaction_hash: Some(tx.hash()),
        transaction_index: U64::from(tx_index),
        block_hash,
        block_number,
        from: transaction.signer(),
        to: tx.to(),
        contract_address,
        gas_used: Some(gas_used),
        effective_gas_price: U128::from(effective_gas_price),
        cumulative_gas_used: U256::from(receipt.cumulative_gas_used),
        status_code: Some(receipt.status),
        logs,
        logs_bloom: receipt.logs_bloom,
        state_root: None,
        // TODO: EIP4844 fields
        blob_gas_used: None,
        blob_gas_price: None,
        ..Default::default()
    };
    Some(tx_receipt)
}

pub enum FilterError {
    InvalidBlockRange,
    RangeTooLarge,
}

impl From<FilterError> for JsonRpcError {
    fn from(e: FilterError) -> Self {
        match e {
            FilterError::InvalidBlockRange => {
                JsonRpcError::eth_filter_error("invalid block range".into())
            }
            FilterError::RangeTooLarge => {
                JsonRpcError::eth_filter_error("block range too large".into())
            }
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(transparent)]
pub struct MonadEthGetLogsParams {
    filters: Vec<Filter>,
}

#[derive(Serialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetLogsResult(pub Vec<MonadLog>);

#[allow(non_snake_case)]
/// Returns an array of all logs matching filter with given id.
pub async fn monad_eth_getLogs(
    blockdb_env: &BlockDbEnv,
    triedb_env: &TriedbEnv,
    p: MonadEthGetLogsParams,
) -> JsonRpcResult<MonadEthGetLogsResult> {
    trace!("monad_eth_getLogs: {p:?}");

    let mut logs = Vec::new();

    for req in p.filters {
        let filter: Filter = req.clone().into();
        let (from_block, to_block) = match req.block_option {
            FilterBlockOption::Range {
                from_block,
                to_block,
            } => {
                let into_block_tag = |block: Option<BlockNumberOrTag>| -> BlockTags {
                    match block {
                        None => BlockTags::default(),
                        Some(b) => match b {
                            BlockNumberOrTag::Number(q) => BlockTags::Number(Quantity(q)),
                            BlockNumberOrTag::Finalized => {
                                BlockTags::Default(BlockTagKey::Finalized)
                            }
                            _ => BlockTags::Default(BlockTagKey::Latest),
                        },
                    }
                };
                let from_block_Tag = into_block_tag(from_block);
                let to_block_tag = into_block_tag(to_block);

                let from_block = blockdb_env
                    .get_block_by_tag(from_block_Tag.into())
                    .await
                    .ok_or(JsonRpcError::custom(
                        "could not get starting block range".to_string(),
                    ))?;
                let to_block = blockdb_env
                    .get_block_by_tag(to_block_tag.into())
                    .await
                    .ok_or(JsonRpcError::custom(
                        "could not get ending block range".to_string(),
                    ))?;
                (from_block.block.number, to_block.block.number)
            }
            FilterBlockOption::AtBlockHash(block_hash) => {
                let key = FixedData::<32>::from(block_hash).into();
                let block = blockdb_env
                    .get_block_by_hash(key)
                    .await
                    .ok_or(JsonRpcError::internal_error())?;
                (block.block.number, block.block.number)
            }
        };

        if from_block > to_block {
            return Err(FilterError::InvalidBlockRange.into());
        }
        if to_block - from_block > 1000 {
            return Err(FilterError::RangeTooLarge.into());
        }

        let filtered_params = FilteredParams::new(Some(filter.clone()));

        for block_num in from_block..=to_block {
            let block = blockdb_env
                .get_block_by_tag(monad_blockdb_utils::BlockTags::Number(block_num))
                .await
                .ok_or(JsonRpcError::internal_error())?;

            // TODO: check block's log_bloom against the filter
            let block_receipts = block_receipts(triedb_env, block).await?;
            let mut receipt_logs: Vec<Log> = block_receipts
                .into_iter()
                .flat_map(|receipt| {
                    let logs: Vec<Log> = receipt
                        .logs
                        .into_iter()
                        .filter(|log: &Log| {
                            if filtered_params.filter.is_some()
                                && (!filtered_params.filter_address(log)
                                    || !filtered_params.filter_topics(log))
                            {
                                false
                            } else {
                                true
                            }
                        })
                        .collect();
                    logs
                })
                .collect();

            logs.append(&mut receipt_logs);
        }
    }

    Ok(MonadEthGetLogsResult(
        logs.into_iter().map(MonadLog).collect(),
    ))
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthSendRawTransactionParams {
    #[serde(deserialize_with = "deserialize_unformatted_data")]
    hex_tx: UnformattedData,
}

// TODO: need to support EIP-4844 transactions
#[rpc(method = "eth_sendRawTransaction", ignore = "ipc")]
#[allow(non_snake_case)]
/// Submits a raw transaction. For EIP-4844 transactions, the raw form must be the network form.
/// This means it includes the blobs, KZG commitments, and KZG proofs.
pub async fn monad_eth_sendRawTransaction(
    ipc: flume::Sender<TransactionSigned>,
    params: MonadEthSendRawTransactionParams,
    allow_unprotected_txs: bool,
) -> JsonRpcResult<String> {
    trace!("monad_eth_sendRawTransaction: {params:?}");

    match TransactionSigned::decode_enveloped(&mut &params.hex_tx.0[..]) {
        Ok(txn) => {
            // drop pre EIP-155 transactions if disallowed by the rpc (for user protection purposes)
            if !allow_unprotected_txs && txn.chain_id().is_none() {
                return Err(JsonRpcError::custom(
                    "Unprotected transactions are not allowed".to_string(),
                ));
            }

            let hash = txn.hash();
            debug!(name = "sendRawTransaction", txn_hash = ?hash);

            match ipc.try_send(txn) {
                Ok(_) => Ok(hash.to_string()),
                Err(err) => {
                    warn!(?err, "mempool ipc send error");
                    Err(JsonRpcError::internal_error())
                }
            }
        }
        Err(e) => {
            debug!("eth txn decode failed {:?}", e);
            Err(JsonRpcError::txn_decode_error())
        }
    }
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetTransactionReceiptParams {
    #[serde(deserialize_with = "deserialize_fixed_data")]
    tx_hash: EthHash,
}

#[rpc(method = "eth_getTransactionReceipt")]
#[allow(non_snake_case)]
/// Returns the receipt of a transaction by transaction hash.
pub async fn monad_eth_getTransactionReceipt(
    blockdb_env: &BlockDbEnv,
    triedb_env: &TriedbEnv,
    p: MonadEthGetTransactionReceiptParams,
) -> JsonRpcResult<Option<MonadTransactionReceipt>> {
    trace!("monad_eth_getTransactionReceipt: {p:?}");

    let key = EthTxKey(B256::new(p.tx_hash.0));
    let Some(txn_value) = blockdb_env.get_txn(key).await else {
        return Ok(None);
    };
    let txn_index = txn_value.transaction_index;

    let Some(block) = blockdb_env.get_block_by_hash(txn_value.block_hash).await else {
        return Ok(None);
    };
    let block_num = block.block.number;
    match triedb_env.get_receipt(txn_index, block_num).await {
        TriedbResult::Null => Ok(None),
        TriedbResult::Receipt(rlp_receipt) => {
            let mut rlp_buf = rlp_receipt.as_slice();
            let receipt =
                decode_receipt(&mut rlp_buf).map_err(|_| JsonRpcError::internal_error())?;

            let prev_receipt = if txn_index > 0 {
                let TriedbResult::Receipt(prev_receipt_rlp_buf) =
                    triedb_env.get_receipt(txn_index - 1, block_num).await
                else {
                    return Err(JsonRpcError::internal_error());
                };
                Some(
                    decode_receipt(&mut prev_receipt_rlp_buf.as_slice())
                        .map_err(|_| JsonRpcError::internal_error())?,
                )
            } else {
                None
            };

            let Some(receipt) =
                parse_tx_receipt(&block, prev_receipt, receipt, block_num, txn_index)
            else {
                return Err(JsonRpcError::internal_error());
            };
            Ok(Some(MonadTransactionReceipt(receipt)))
        }
        _ => Err(JsonRpcError::internal_error()),
    }
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetTransactionByHashParams {
    #[serde(deserialize_with = "deserialize_fixed_data")]
    tx_hash: EthHash,
}

#[rpc(method = "eth_getTransactionByHash")]
#[allow(non_snake_case)]
/// Returns the information about a transaction requested by transaction hash.
pub async fn monad_eth_getTransactionByHash(
    blockdb_env: &BlockDbEnv,
    params: MonadEthGetTransactionByHashParams,
) -> JsonRpcResult<Option<MonadTransaction>> {
    trace!("monad_eth_getTransactionByHash: {params:?}");

    let key = EthTxKey(B256::new(params.tx_hash.0));
    let Some(result) = blockdb_env.get_txn(key).await else {
        return Ok(None);
    };

    let block_key = result.block_hash;
    let Some(block) = blockdb_env.get_block_by_hash(block_key).await else {
        error!("txn was found so its block should exist");
        return Err(JsonRpcError::internal_error());
    };

    let Some(transaction) = block.block.body.get(result.transaction_index as usize) else {
        error!("txn and block found so its index should be correct");
        return Err(JsonRpcError::internal_error());
    };

    let retval =
        parse_tx_content(&block, transaction, result.transaction_index).map(MonadTransaction);

    Ok(retval)
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetTransactionByBlockHashAndIndexParams {
    #[serde(deserialize_with = "deserialize_fixed_data")]
    block_hash: EthHash,
    #[serde(deserialize_with = "deserialize_quantity")]
    index: Quantity,
}

#[rpc(method = "eth_getTransactionByBlockHashAndIndex")]
#[allow(non_snake_case)]
/// Returns information about a transaction by block hash and transaction index position.
pub async fn monad_eth_getTransactionByBlockHashAndIndex(
    blockdb_env: &BlockDbEnv,
    params: MonadEthGetTransactionByBlockHashAndIndexParams,
) -> JsonRpcResult<Option<MonadTransaction>> {
    trace!("monad_eth_getTransactionByBlockHashAndIndex: {params:?}");

    let key = params.block_hash.into();
    let Some(block) = blockdb_env.get_block_by_hash(key).await else {
        return Ok(None);
    };

    let Some(transaction) = block.block.body.get(params.index.0 as usize) else {
        return Ok(None);
    };

    let retval = parse_tx_content(&block, transaction, params.index.0).map(MonadTransaction);

    Ok(retval)
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetTransactionByBlockNumberAndIndexParams {
    #[serde(deserialize_with = "deserialize_block_tags")]
    block_tag: BlockTags,
    #[serde(deserialize_with = "deserialize_quantity")]
    index: Quantity,
}

#[rpc(method = "eth_getTransactionByBlockNumberAndIndex")]
#[allow(non_snake_case)]
/// Returns information about a transaction by block number and transaction index position.
pub async fn monad_eth_getTransactionByBlockNumberAndIndex(
    blockdb_env: &BlockDbEnv,
    params: MonadEthGetTransactionByBlockNumberAndIndexParams,
) -> JsonRpcResult<Option<MonadTransaction>> {
    trace!("monad_eth_getTransactionByBlockNumberAndIndex: {params:?}");

    let Some(block) = blockdb_env.get_block_by_tag(params.block_tag.into()).await else {
        return Ok(None);
    };

    let Some(transaction) = block.block.body.get(params.index.0 as usize) else {
        return Ok(None);
    };

    let retval = parse_tx_content(&block, transaction, params.index.0).map(MonadTransaction);

    Ok(retval)
}
