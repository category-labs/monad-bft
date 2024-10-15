use std::cmp::min;

use alloy_primitives::{
    aliases::{U128, U256, U64},
    Address, FixedBytes,
};
use bytes::Buf;
use monad_eth_block_policy::{static_validate_transaction, TransactionError};
use monad_rpc_docs::rpc;
use reth_primitives::{transaction::TransactionKind, Block as EthBlock, TransactionSigned};
use reth_rpc_types::{
    AccessListItem, Filter, FilteredParams, Log, Parity, Signature, Transaction, TransactionReceipt,
};
use serde::{Deserialize, Serialize};
use tracing::{debug, trace, warn};

use crate::{
    block_handlers::block_receipts,
    block_util::{get_block_from_num, get_block_num_from_tag, BlockResult, FileBlockReader},
    eth_json_types::{
        BlockTags, EthAddress, EthHash, MonadLog, MonadTransaction, MonadTransactionReceipt,
        Quantity, UnformattedData,
    },
    jsonrpc::{JsonRpcError, JsonRpcResult},
    receipt::{decode_receipt, ReceiptDetails},
    triedb::{TriedbEnv, TriedbResult},
};

pub fn parse_tx_content(
    block: &EthBlock,
    tx: &TransactionSigned,
    tx_index: u64,
) -> Option<Transaction> {
    // recover transaction signer
    let transaction = tx
        .clone()
        .into_ecrecovered_unchecked()
        .expect("transaction sender should exist");

    // parse fee parameters
    let base_fee = block.base_fee_per_gas;
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

    let block_hash = Some(get_block_hash(block));

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
        block_hash,
        block_number: Some(U256::from(block.number)),
        transaction_index: Some(U256::from(tx_index)),

        // only relevant for EIP-4844 transactions
        max_fee_per_blob_gas: None,
        blob_versioned_hashes: vec![],
        other: Default::default(),
    };

    Some(retval)
}

pub fn parse_tx_receipt(
    block: &EthBlock,
    prev_receipt: Option<ReceiptDetails>,
    receipt: ReceiptDetails,
    block_num: u64,
    tx_index: u64,
) -> Option<TransactionReceipt> {
    let tx = block.body.get(tx_index as usize)?;
    let transaction = tx
        .clone()
        .into_ecrecovered_unchecked()
        .expect("transaction sender should exist");
    let base_fee_per_gas = block.base_fee_per_gas.unwrap_or_default() as u128;
    // effective gas price is calculated according to eth json rpc specification
    let effective_gas_price = base_fee_per_gas
        + min(
            tx.max_fee_per_gas() - base_fee_per_gas,
            tx.max_priority_fee_per_gas().unwrap_or_default(),
        );

    let block_hash = Some(get_block_hash(block));
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
                JsonRpcError::filter_error("invalid block range".into())
            }
            FilterError::RangeTooLarge => {
                JsonRpcError::filter_error("block range too large".into())
            }
        }
    }
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(transparent)]
pub struct MonadEthGetLogsParams {
    filters: Vec<FilterParams>,
}

#[derive(Debug, schemars::JsonSchema)]
pub struct FilterParams {
    filter: LogFilter,
    address: AddressValueOrArray,
    topics: Option<Vec<EthHash>>,
}

// Must use this impl instead of derive because serde does not support default with flatten: https://github.com/serde-rs/serde/issues/1626
impl<'de> Deserialize<'de> for FilterParams {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        pub struct ParamsHelper {
            #[serde(flatten)]
            filter: Option<LogFilter>,
            address: AddressValueOrArray,
            topics: Option<Vec<EthHash>>,
        }

        let result = ParamsHelper::deserialize(deserializer)?;

        Ok(Self {
            filter: result.filter.unwrap_or_default(),
            address: result.address,
            topics: result.topics,
        })
    }
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(untagged, rename_all_fields = "camelCase")]
pub enum LogFilter {
    Range {
        from_block: BlockTags,
        to_block: BlockTags,
    },
    BlockHash {
        block_hash: EthHash,
    },
}

impl Default for LogFilter {
    fn default() -> Self {
        Self::Range {
            from_block: BlockTags::default(),
            to_block: BlockTags::default(),
        }
    }
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(untagged)]
pub enum AddressValueOrArray {
    Address(Option<EthAddress>),
    Addresses(Vec<EthAddress>),
}

#[derive(Serialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetLogsResult(pub Vec<MonadLog>);

#[rpc(method = "eth_getLogs", ignore = "file_ledger_reader")]
#[allow(non_snake_case)]
/// Returns an array of all logs matching filter with given id.
pub async fn monad_eth_getLogs(
    file_ledger_reader: &FileBlockReader,
    triedb_env: &TriedbEnv,
    p: MonadEthGetLogsParams,
) -> JsonRpcResult<MonadEthGetLogsResult> {
    trace!("monad_eth_getLogs: {p:?}");

    let mut logs = Vec::new();

    for req in p.filters {
        let mut filter = Filter::new();

        match req.address {
            AddressValueOrArray::Address(Some(address)) => {
                filter = filter.address(Address::from_slice(&address.0));
            }
            AddressValueOrArray::Addresses(addresses) => {
                filter = filter.address(
                    addresses
                        .iter()
                        .map(|a| Address::from_slice(&a.0))
                        .collect::<Vec<_>>(),
                );
            }
            _ => {}
        }

        if let Some(topics) = req.topics {
            for topic in topics {
                filter = filter.event_signature(FixedBytes::<32>::from(&topic.0));
            }
        }

        let (from_block, to_block) = match req.filter {
            LogFilter::Range {
                from_block,
                to_block,
            } => {
                let from_block = get_block_num_from_tag(triedb_env, from_block).await?;
                let to_block = get_block_num_from_tag(triedb_env, to_block).await?;
                filter = filter.from_block(from_block);
                filter = filter.to_block(to_block);
                (from_block, to_block)
            }
            LogFilter::BlockHash { block_hash } => {
                filter = filter.at_block_hash(Into::<FixedBytes<32>>::into(block_hash.0));
                let block = file_ledger_reader
                    .clone()
                    .get_block_by_hash(block_hash.into())
                    .ok_or(JsonRpcError::internal_error(
                        "error reading block data".into(),
                    ))?;
                (block.number, block.number)
            }
        };

        if from_block > to_block {
            return Err(FilterError::InvalidBlockRange.into());
        }
        if to_block - from_block > 1000 {
            return Err(FilterError::RangeTooLarge.into());
        }

        let filtered_params = FilteredParams::new(Some(filter.clone()));
        let address_filter = FilteredParams::address_filter(&filter.address);
        let topics_filter = FilteredParams::topics_filter(&filter.topics);

        for block_num in from_block..=to_block {
            let block = match get_block_from_num(file_ledger_reader, block_num).await {
                BlockResult::Block(b) => b,
                BlockResult::NotFound => {
                    return Err(JsonRpcError::internal_error("block not found".into()))
                }
                BlockResult::DecodeFailed(e) => {
                    return Err(JsonRpcError::internal_error(format!(
                        "decode block failed: {}",
                        e
                    )))
                }
            };

            let header_logs_bloom = block.header.logs_bloom;

            if FilteredParams::matches_address(header_logs_bloom, &address_filter)
                || FilteredParams::matches_topics(header_logs_bloom, &topics_filter)
            {
                let block_receipts = block_receipts(triedb_env, block).await?;
                let mut receipt_logs: Vec<Log> = block_receipts
                    .into_iter()
                    .flat_map(|receipt| {
                        let logs: Vec<Log> = receipt
                            .logs
                            .into_iter()
                            .filter(|log| {
                                filtered_params.filter_address(log)
                                    && filtered_params.filter_topics(log)
                            })
                            .collect();
                        logs
                    })
                    .collect();

                logs.append(&mut receipt_logs);
            }
        }
    }

    Ok(MonadEthGetLogsResult(
        logs.into_iter().map(MonadLog).collect(),
    ))
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthSendRawTransactionParams {
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
    chain_id: u64,
) -> JsonRpcResult<String> {
    trace!("monad_eth_sendRawTransaction: {params:?}");

    match TransactionSigned::decode_enveloped(&mut &params.hex_tx.0[..]) {
        Ok(txn) => {
            // drop transactions that will fail consensus check
            if let Err(err) = static_validate_transaction(&txn, chain_id) {
                let error_message = match err {
                    TransactionError::InvalidChainId => "Invalid chain ID",
                    TransactionError::MaxPriorityFeeTooHigh => "Max priority fee too high",
                    TransactionError::InitCodeLimitExceeded => "Init code size limit exceeded",
                    TransactionError::GasLimitTooLow => "Gas limit too low",
                };
                return Err(JsonRpcError::custom(error_message.to_string()));
            }

            let hash = txn.hash();
            debug!(name = "sendRawTransaction", txn_hash = ?hash);

            match ipc.try_send(txn) {
                Ok(_) => Ok(hash.to_string()),
                Err(err) => {
                    warn!(?err, "mempool ipc send error");
                    Err(JsonRpcError::internal_error(
                        "unable to send to mempool".into(),
                    ))
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
    tx_hash: EthHash,
}

#[rpc(method = "eth_getTransactionReceipt", ignore = "file_ledger_reader")]
#[allow(non_snake_case)]
/// Returns the receipt of a transaction by transaction hash.
pub async fn monad_eth_getTransactionReceipt(
    file_ledger_reader: &FileBlockReader,
    triedb_env: &TriedbEnv,
    p: MonadEthGetTransactionReceiptParams,
) -> JsonRpcResult<Option<MonadTransactionReceipt>> {
    trace!("monad_eth_getTransactionReceipt: {p:?}");

    let Some(txn_value) = file_ledger_reader.get_txn_by_hash(monad_types::Hash(p.tx_hash.0)) else {
        return Ok(None);
    };
    let txn_index = txn_value.transaction_index;

    let Some(block) = file_ledger_reader.get_block_by_hash(txn_value.block_hash) else {
        return Ok(None);
    };
    let block_num = block.number;
    match triedb_env.get_receipt(txn_index, block_num).await {
        TriedbResult::Null => Ok(None),
        TriedbResult::Receipt(rlp_receipt) => {
            let mut rlp_buf = rlp_receipt.as_slice();
            let receipt = decode_receipt(&mut rlp_buf).map_err(|e| {
                JsonRpcError::internal_error(format!("decode receipt failed: {}", e))
            })?;

            let prev_receipt = if txn_index > 0 {
                let TriedbResult::Receipt(prev_receipt_rlp_buf) =
                    triedb_env.get_receipt(txn_index - 1, block_num).await
                else {
                    return Err(JsonRpcError::internal_error("error reading from db".into()));
                };
                Some(
                    decode_receipt(&mut prev_receipt_rlp_buf.as_slice()).map_err(|e| {
                        JsonRpcError::internal_error(format!("decode receipt failed: {}", e))
                    })?,
                )
            } else {
                None
            };

            let Some(receipt) =
                parse_tx_receipt(&block, prev_receipt, receipt, block_num, txn_index)
            else {
                return Err(JsonRpcError::internal_error("parse receipt failed".into()));
            };
            Ok(Some(MonadTransactionReceipt(receipt)))
        }
        _ => Err(JsonRpcError::internal_error("error reading from db".into())),
    }
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetTransactionByHashParams {
    tx_hash: EthHash,
}

#[rpc(method = "eth_getTransactionByHash", ignore = "file_ledger_reader")]
#[allow(non_snake_case)]
/// Returns the information about a transaction requested by transaction hash.
pub async fn monad_eth_getTransactionByHash(
    file_ledger_reader: &FileBlockReader,
    params: MonadEthGetTransactionByHashParams,
) -> JsonRpcResult<Option<MonadTransaction>> {
    trace!("monad_eth_getTransactionByHash: {params:?}");

    let Some(txn_value) = file_ledger_reader.get_txn_by_hash(monad_types::Hash(params.tx_hash.0))
    else {
        return Ok(None);
    };

    let Some(block) = file_ledger_reader.get_block_by_hash(txn_value.block_hash) else {
        return Ok(None);
    };

    let transaction = block
        .body
        .get(txn_value.transaction_index as usize)
        .expect("txn and block found so its index should be correct");

    let retval =
        parse_tx_content(&block, transaction, txn_value.transaction_index).map(MonadTransaction);

    Ok(retval)
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetTransactionByBlockHashAndIndexParams {
    block_hash: EthHash,
    index: Quantity,
}

#[rpc(
    method = "eth_getTransactionByBlockHashAndIndex",
    ignore = "file_ledger_reader"
)]
#[allow(non_snake_case)]
/// Returns information about a transaction by block hash and transaction index position.
pub async fn monad_eth_getTransactionByBlockHashAndIndex(
    file_ledger_reader: &FileBlockReader,
    params: MonadEthGetTransactionByBlockHashAndIndexParams,
) -> JsonRpcResult<Option<MonadTransaction>> {
    trace!("monad_eth_getTransactionByBlockHashAndIndex: {params:?}");

    let key = params.block_hash.into();
    let Some(block) = file_ledger_reader.get_block_by_hash(key) else {
        return Ok(None);
    };

    let Some(transaction) = block.body.get(params.index.0 as usize) else {
        return Ok(None);
    };

    let retval = parse_tx_content(&block, transaction, params.index.0).map(MonadTransaction);

    Ok(retval)
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetTransactionByBlockNumberAndIndexParams {
    block_tag: BlockTags,
    index: Quantity,
}

#[rpc(
    method = "eth_getTransactionByBlockNumberAndIndex",
    ignore = "file_ledger_reader"
)]
#[allow(non_snake_case)]
/// Returns information about a transaction by block number and transaction index position.
pub async fn monad_eth_getTransactionByBlockNumberAndIndex(
    file_ledger_reader: &FileBlockReader,
    triedb_env: &TriedbEnv,
    params: MonadEthGetTransactionByBlockNumberAndIndexParams,
) -> JsonRpcResult<Option<MonadTransaction>> {
    trace!("monad_eth_getTransactionByBlockNumberAndIndex: {params:?}");

    let block_num = get_block_num_from_tag(triedb_env, params.block_tag).await?;
    let block = match get_block_from_num(file_ledger_reader, block_num).await {
        BlockResult::Block(b) => b,
        BlockResult::NotFound => return Ok(None),
        BlockResult::DecodeFailed(_) => return Ok(None),
    };

    let Some(transaction) = block.body.get(params.index.0 as usize) else {
        return Ok(None);
    };

    let retval = parse_tx_content(&block, transaction, params.index.0).map(MonadTransaction);

    Ok(retval)
}

fn get_block_hash(block: &EthBlock) -> FixedBytes<32> {
    let mut block_hash = [0; 32];
    block.extra_data.clone().copy_to_slice(&mut block_hash);
    FixedBytes::from(block_hash)
}
