use std::cmp::min;

use alloy_primitives::{
    aliases::{U128, U256, U64},
    FixedBytes,
};
use monad_eth_block_policy::{static_validate_transaction, TransactionError};
use monad_rpc_docs::rpc;
use monad_triedb_utils::triedb_env::{TransactionLocation, Triedb};
use reth_primitives::{
    transaction::TransactionKind, Header, Receipt, ReceiptWithBloom, TransactionSigned,
};
use reth_rpc_types::{
    AccessListItem, BlockNumberOrTag, Filter, FilterBlockOption, FilteredParams, Log, Parity,
    Signature, Transaction, TransactionReceipt,
};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, trace};

use crate::{
    block_handlers::{block_receipts, get_block_num_from_tag},
    eth_json_types::{
        BlockTags, EthHash, MonadLog, MonadTransaction, MonadTransactionReceipt, Quantity,
        UnformattedData,
    },
    jsonrpc::{JsonRpcError, JsonRpcResult},
    vpool,
};

pub fn parse_tx_content(
    block_hash: FixedBytes<32>,
    block_number: u64,
    base_fee: Option<u64>,
    tx: TransactionSigned,
    tx_index: u64,
) -> Result<Transaction, JsonRpcError> {
    // recover transaction signer
    let Some(tx) = tx.into_ecrecovered_unchecked() else {
        error!("transaction sender should exist");
        return Err(JsonRpcError::txn_decode_error());
    };

    // parse fee parameters
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
        from: tx.signer(),
        to: tx.to(),
        value: tx.value().into(),
        gas_price: Some(U128::from(gas_price)),
        max_fee_per_gas: Some(U128::from(tx.max_fee_per_gas())),
        max_priority_fee_per_gas: tx.max_priority_fee_per_gas().map(U128::from),
        signature: Some(signature),
        gas: U256::from(tx.gas_limit()),
        input: tx.input().to_owned(),
        chain_id: tx.chain_id().map(U64::from),
        access_list,
        transaction_type: Some(U64::from(tx.tx_type() as u8)),
        block_hash: Some(block_hash),
        block_number: Some(U256::from(block_number)),
        transaction_index: Some(U256::from(tx_index)),

        // only relevant for EIP-4844 transactions
        max_fee_per_blob_gas: None,
        blob_versioned_hashes: vec![],
        other: Default::default(),
    };

    Ok(retval)
}

pub fn parse_tx_receipt(
    block_header: &Header,
    block_hash: FixedBytes<32>,
    tx: TransactionSigned,
    prev_receipt: Option<Receipt>,
    receipt: ReceiptWithBloom,
    block_num: u64,
    tx_index: usize,
) -> Result<TransactionReceipt, JsonRpcError> {
    let Some(tx) = tx.into_ecrecovered_unchecked() else {
        error!("transaction sender should exist");
        return Err(JsonRpcError::txn_decode_error());
    };
    let base_fee_per_gas = block_header.base_fee_per_gas.unwrap_or_default() as u128;
    // effective gas price is calculated according to eth json rpc specification
    let effective_gas_price = base_fee_per_gas
        + min(
            tx.max_fee_per_gas() - base_fee_per_gas,
            tx.max_priority_fee_per_gas().unwrap_or_default(),
        );

    let block_hash = Some(block_hash);
    let block_number = Some(U256::from(block_num));

    let logs = receipt
        .receipt
        .logs
        .into_iter()
        .enumerate()
        .map(|(log_index, log)| Log {
            address: log.address,
            topics: log.topics,
            data: log.data,
            block_hash,
            block_number,
            transaction_hash: Some(tx.hash()),
            transaction_index: Some(U256::from(tx_index)),
            log_index: Some(U256::from(log_index)),
            removed: Default::default(),
        })
        .collect();

    let contract_address = match tx.kind() {
        TransactionKind::Create => Some(tx.signer().create(tx.nonce())),
        _ => None,
    };

    let gas_used = if let Some(prev_receipt) = prev_receipt {
        U256::from(receipt.receipt.cumulative_gas_used - prev_receipt.cumulative_gas_used)
    } else {
        U256::from(receipt.receipt.cumulative_gas_used)
    };

    let tx_receipt = TransactionReceipt {
        transaction_type: receipt.receipt.tx_type.into(),
        transaction_hash: Some(tx.hash()),
        transaction_index: U64::from(tx_index),
        block_hash,
        block_number,
        from: tx.signer(),
        to: tx.to(),
        contract_address,
        gas_used: Some(gas_used),
        effective_gas_price: U128::from(effective_gas_price),
        cumulative_gas_used: U256::from(receipt.receipt.cumulative_gas_used),
        status_code: Some(U64::from(receipt.receipt.success as u8)),
        logs,
        logs_bloom: receipt.bloom,
        state_root: None,
        // TODO: EIP4844 fields
        blob_gas_used: None,
        blob_gas_price: None,
        ..Default::default()
    };
    Ok(tx_receipt)
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

#[derive(Debug, Deserialize)]
#[serde(transparent)]
pub struct MonadEthGetLogsParams {
    filters: Vec<Filter>,
}

#[derive(Serialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetLogsResult(pub Vec<MonadLog>);

#[allow(non_snake_case)]
/// Returns an array of all logs matching filter with given id.
pub async fn monad_eth_getLogs<T: Triedb>(
    triedb_env: &T,
    p: MonadEthGetLogsParams,
) -> JsonRpcResult<MonadEthGetLogsResult> {
    trace!("monad_eth_getLogs: {p:?}");

    let mut logs = Vec::new();

    for req in p.filters {
        let filter: Filter = req.clone();
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
                            _ => BlockTags::Latest,
                        },
                    }
                };
                let from_block_tag = into_block_tag(from_block);
                let to_block_tag = into_block_tag(to_block);

                let from_block = get_block_num_from_tag(triedb_env, from_block_tag)
                    .await
                    .map_err(|_| {
                        JsonRpcError::internal_error(
                            "could not get starting block range".to_string(),
                        )
                    })?;
                let to_block = get_block_num_from_tag(triedb_env, to_block_tag)
                    .await
                    .map_err(|_| {
                        JsonRpcError::internal_error("could not get ending block range".to_string())
                    })?;
                (from_block, to_block)
            }
            FilterBlockOption::AtBlockHash(block_hash) => {
                let latest_block_num =
                    get_block_num_from_tag(triedb_env, BlockTags::Latest).await?;
                let block = triedb_env
                    .get_block_number_by_hash(block_hash.into(), latest_block_num)
                    .await
                    .map_err(|_| {
                        JsonRpcError::internal_error("could not get block hash".to_string())
                    })?;
                let block_num = match block {
                    Some(block_num) => block_num,
                    None => return Ok(MonadEthGetLogsResult(vec![])),
                };
                (block_num, block_num)
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
            let header = match triedb_env
                .get_block_header(block_num)
                .await
                .map_err(JsonRpcError::internal_error)?
            {
                Some(header) => header,
                None => {
                    return Err(JsonRpcError::internal_error(
                        "error getting block header".into(),
                    ))
                }
            };

            // TODO: check block's log_bloom against the filter
            let block_receipts = block_receipts(triedb_env, &header.header, header.hash).await?;

            let mut receipt_logs: Vec<Log> = block_receipts
                .into_iter()
                .flat_map(|receipt| {
                    let logs: Vec<Log> = receipt
                        .logs
                        .into_iter()
                        .filter(|log: &Log| {
                            !(filtered_params.filter.is_some()
                                && (!filtered_params.filter_address(log)
                                    || !filtered_params.filter_topics(log)))
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
    hex_tx: UnformattedData,
}

// TODO: need to support EIP-4844 transactions
#[rpc(method = "eth_sendRawTransaction", ignore = "tx_pool")]
#[allow(non_snake_case)]
/// Submits a raw transaction. For EIP-4844 transactions, the raw form must be the network form.
/// This means it includes the blobs, KZG commitments, and KZG proofs.
pub async fn monad_eth_sendRawTransaction(
    tx_pool: std::sync::Arc<tokio::sync::Mutex<vpool::VirtualPool>>,
    params: MonadEthSendRawTransactionParams,
    chain_id: u64,
    allow_unprotected_txs: bool,
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

            // drop pre EIP-155 transactions if disallowed by the rpc (for user protection purposes)
            if !allow_unprotected_txs && txn.chain_id().is_none() {
                return Err(JsonRpcError::custom(
                    "Unprotected transactions (pre-EIP155) are not allowed over RPC".to_string(),
                ));
            }

            let hash = txn.hash();
            debug!(name = "sendRawTransaction", txn_hash = ?hash);

            let txn = txn.try_into_ecrecovered().map_err(|_| {
                JsonRpcError::custom("cannot ec recover sender from transaction".to_string())
            })?;
            tx_pool.lock().await.add_transaction(txn).await;
            Ok(hash.to_string())
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

#[rpc(method = "eth_getTransactionReceipt")]
#[allow(non_snake_case)]
/// Returns the receipt of a transaction by transaction hash.
pub async fn monad_eth_getTransactionReceipt<T: Triedb>(
    triedb_env: &T,
    params: MonadEthGetTransactionReceiptParams,
) -> JsonRpcResult<Option<MonadTransactionReceipt>> {
    trace!("monad_eth_getTransactionReceipt: {params:?}");

    let latest_block_num = get_block_num_from_tag(triedb_env, BlockTags::Latest).await?;
    let Some(TransactionLocation {
        tx_index,
        block_num,
    }) = triedb_env
        .get_transaction_location_by_hash(params.tx_hash.0, latest_block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    else {
        return Ok(None);
    };

    let header = match triedb_env
        .get_block_header(block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        Some(header) => header,
        None => return Ok(None),
    };
    let Some(receipt) = triedb_env
        .get_receipt(tx_index, block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    else {
        return Ok(None);
    };
    let prev_receipt = if tx_index > 0 {
        match triedb_env
            .get_receipt(tx_index - 1, block_num)
            .await
            .map_err(JsonRpcError::internal_error)?
        {
            Some(receipt) => Some(receipt.receipt),
            None => return Err(JsonRpcError::internal_error("error getting receipt".into())),
        }
    } else {
        None
    };

    let Some(tx) = triedb_env
        .get_transaction(tx_index, block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    else {
        return Ok(None);
    };

    let receipt = parse_tx_receipt(
        &header.header,
        header.hash,
        tx,
        prev_receipt,
        receipt,
        block_num,
        tx_index as usize,
    )?;

    Ok(Some(MonadTransactionReceipt(receipt)))
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetTransactionByHashParams {
    tx_hash: EthHash,
}

#[rpc(method = "eth_getTransactionByHash")]
#[allow(non_snake_case)]
/// Returns the information about a transaction requested by transaction hash.
pub async fn monad_eth_getTransactionByHash<T: Triedb>(
    triedb_env: &T,
    params: MonadEthGetTransactionByHashParams,
) -> JsonRpcResult<Option<MonadTransaction>> {
    trace!("monad_eth_getTransactionByHash: {params:?}");

    let latest_block_num = get_block_num_from_tag(triedb_env, BlockTags::Latest).await?;
    let Some(TransactionLocation {
        tx_index,
        block_num,
    }) = triedb_env
        .get_transaction_location_by_hash(params.tx_hash.0, latest_block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    else {
        return Ok(None);
    };

    let header = match triedb_env
        .get_block_header(block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        Some(header) => header,
        None => return Ok(None),
    };
    let Some(tx) = triedb_env
        .get_transaction(tx_index, block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    else {
        return Ok(None);
    };

    parse_tx_content(
        header.hash,
        block_num,
        header.header.base_fee_per_gas,
        tx,
        tx_index,
    )
    .map(|txn| Some(MonadTransaction(txn)))
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetTransactionByBlockHashAndIndexParams {
    block_hash: EthHash,
    index: Quantity,
}

#[rpc(method = "eth_getTransactionByBlockHashAndIndex")]
#[allow(non_snake_case)]
/// Returns information about a transaction by block hash and transaction index position.
pub async fn monad_eth_getTransactionByBlockHashAndIndex<T: Triedb>(
    triedb_env: &T,
    params: MonadEthGetTransactionByBlockHashAndIndexParams,
) -> JsonRpcResult<Option<MonadTransaction>> {
    trace!("monad_eth_getTransactionByBlockHashAndIndex: {params:?}");

    let latest_block_num = get_block_num_from_tag(triedb_env, BlockTags::Latest).await?;
    let Some(block_num) = triedb_env
        .get_block_number_by_hash(params.block_hash.0, latest_block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    else {
        return Ok(None);
    };

    let header = match triedb_env
        .get_block_header(block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        Some(header) => header,
        None => return Ok(None),
    };
    let Some(tx) = triedb_env
        .get_transaction(params.index.0, block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    else {
        return Ok(None);
    };

    parse_tx_content(
        params.block_hash.0.into(),
        header.header.number,
        header.header.base_fee_per_gas,
        tx,
        params.index.0,
    )
    .map(|txn| Some(MonadTransaction(txn)))
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetTransactionByBlockNumberAndIndexParams {
    block_tag: BlockTags,
    index: Quantity,
}

#[rpc(method = "eth_getTransactionByBlockNumberAndIndex")]
#[allow(non_snake_case)]
/// Returns information about a transaction by block number and transaction index position.
pub async fn monad_eth_getTransactionByBlockNumberAndIndex<T: Triedb>(
    triedb_env: &T,
    params: MonadEthGetTransactionByBlockNumberAndIndexParams,
) -> JsonRpcResult<Option<MonadTransaction>> {
    trace!("monad_eth_getTransactionByBlockNumberAndIndex: {params:?}");

    let block_num = get_block_num_from_tag(triedb_env, params.block_tag).await?;

    let header = match triedb_env
        .get_block_header(block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        Some(header) => header,
        None => return Ok(None),
    };
    let Some(tx) = triedb_env
        .get_transaction(params.index.0, block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    else {
        return Ok(None);
    };

    parse_tx_content(
        header.hash,
        header.header.number,
        header.header.base_fee_per_gas,
        tx,
        params.index.0,
    )
    .map(|txn| Some(MonadTransaction(txn)))
}
