use std::cmp::min;

use alloy_consensus::{Header, ReceiptEnvelope, ReceiptWithBloom, Transaction as _, TxEnvelope};
use alloy_primitives::{FixedBytes, TxKind};
use alloy_rpc_types::{
    BlockNumberOrTag, Filter, FilterBlockOption, FilteredParams, Log, Receipt, Transaction,
    TransactionReceipt,
};
use monad_archive::archive_reader::ArchiveReader;
use monad_eth_block_policy::{static_validate_transaction, TransactionError};
use monad_rpc_docs::rpc;
use monad_triedb_utils::triedb_env::{TransactionLocation, Triedb};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, trace, warn};

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
    tx: TxEnvelope,
    tx_index: u64,
) -> Result<Transaction, JsonRpcError> {
    // recover transaction signer
    let Ok(from) = tx.recover_signer() else {
        error!("transaction sender should exist");
        return Err(JsonRpcError::txn_decode_error());
    };

    // effective gas price is calculated according to eth json rpc specification
    let base_fee: u128 = base_fee.unwrap_or_default().into();
    let effective_gas_price = base_fee
        + min(
            tx.max_fee_per_gas() - base_fee,
            tx.max_priority_fee_per_gas().unwrap_or_default(),
        );

    Ok(Transaction {
        inner: tx,
        from,
        block_hash: Some(block_hash),
        block_number: Some(block_number),
        effective_gas_price: Some(effective_gas_price),
        transaction_index: Some(tx_index),
    })
}

pub fn parse_tx_receipt(
    base_fee_per_gas: Option<u64>,
    block_timestamp: Option<u64>,
    block_hash: FixedBytes<32>,
    tx: &TxEnvelope,
    gas_used: u128,
    receipt: ReceiptEnvelope,
    block_num: u64,
    tx_index: u64,
) -> Result<TransactionReceipt, JsonRpcError> {
    let Ok(address) = tx.recover_signer() else {
        error!("transaction sender should exist");
        return Err(JsonRpcError::txn_decode_error());
    };
    let base_fee_per_gas = base_fee_per_gas.unwrap_or_default() as u128;
    // effective gas price is calculated according to eth json rpc specification
    let effective_gas_price = base_fee_per_gas
        + min(
            tx.max_fee_per_gas() - base_fee_per_gas,
            tx.max_priority_fee_per_gas().unwrap_or_default(),
        );

    let block_hash = Some(block_hash);
    let block_number = Some(block_num);

    let logs: Vec<Log> = receipt
        .logs()
        .into_iter()
        .enumerate()
        .map(|(log_index, log)| Log {
            inner: log.clone(),
            block_hash,
            block_number,
            block_timestamp,
            transaction_hash: Some((*tx.tx_hash()).into()),
            transaction_index: Some(tx_index as u64),
            log_index: Some(log_index as u64),
            removed: Default::default(),
        })
        .collect();

    let contract_address = match tx.kind() {
        TxKind::Create => Some(address.create(tx.nonce())),
        _ => None,
    };

    // TODO: support other transaction types
    let inner_receipt: ReceiptEnvelope<Log> = ReceiptEnvelope::Eip1559(ReceiptWithBloom {
        receipt: Receipt {
            status: receipt.status().into(),
            cumulative_gas_used: receipt.cumulative_gas_used(),
            logs,
        },
        logs_bloom: *receipt.logs_bloom(),
    });

    let tx_receipt = TransactionReceipt {
        inner: inner_receipt,
        transaction_hash: (*tx.tx_hash()).into(),
        transaction_index: Some(tx_index as u64),
        block_hash,
        block_number,
        from: address,
        to: tx.to(),
        contract_address,
        gas_used,
        effective_gas_price,
        // TODO: EIP4844 fields
        blob_gas_used: None,
        blob_gas_price: None,
        authorization_list: None,
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
    archive_reader: &Option<ArchiveReader>,
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
                    None => {
                        // retry from archive reader if block hash not available in triedb
                        if let Some(archive_reader) = archive_reader {
                            if let Ok(block) = archive_reader.get_block_by_hash(&block_hash).await {
                                block.header.number
                            } else {
                                return Ok(MonadEthGetLogsResult(vec![]));
                            }
                        } else {
                            return Ok(MonadEthGetLogsResult(vec![]));
                        }
                    }
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
            // TODO: check block's log_bloom against the filter
            let receipts: Vec<TransactionReceipt> = if let Some(header) = triedb_env
                .get_block_header(block_num)
                .await
                .map_err(JsonRpcError::internal_error)?
            {
                // try fetching from triedb
                let transactions = triedb_env
                    .get_transactions(block_num)
                    .await
                    .map_err(JsonRpcError::internal_error)?;
                let bloom_receipts = triedb_env
                    .get_receipts(block_num)
                    .await
                    .map_err(JsonRpcError::internal_error)?;
                block_receipts(&transactions, bloom_receipts, &header.header, header.hash).await?
            } else if let Some(archive_reader) = archive_reader {
                // fallback to archive reader if header not available in triedb
                let block = archive_reader
                    .get_block_by_number(block_num)
                    .await
                    .map_err(|_| {
                        JsonRpcError::internal_error("error getting block header".into())
                    })?;
                let bloom_receipts =
                    archive_reader
                        .get_block_receipts(block_num)
                        .await
                        .map_err(|_| {
                            JsonRpcError::internal_error("error getting block receipts".into())
                        })?;
                block_receipts(
                    &block.body.transactions,
                    bloom_receipts,
                    &block.header,
                    block.header.hash_slow(),
                )
                .await?
            } else {
                return Err(JsonRpcError::internal_error(
                    "error getting block header".into(),
                ));
            };

            let mut receipt_logs: Vec<Log> = receipts
                .into_iter()
                .flat_map(|receipt| {
                    let logs: Vec<Log> = receipt
                        .inner
                        .logs()
                        .into_iter()
                        .cloned()
                        .filter(|log: &Log| {
                            !(filtered_params.filter.is_some()
                                && (!filtered_params.filter_address(&log.address())
                                    || !filtered_params.filter_topics(log.topics())))
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
    tx_pool: &vpool::VirtualPool,
    params: MonadEthSendRawTransactionParams,
    chain_id: u64,
    allow_unprotected_txs: bool,
) -> JsonRpcResult<String> {
    trace!("monad_eth_sendRawTransaction: {params:?}");

    match alloy_rlp::decode_exact(&mut &params.hex_tx.0[..]) {
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

            if tx_pool.publisher.send(txn).is_err() {
                warn!("issue broadcasting transaction from pending pool");
            }
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
    archive_reader: &Option<ArchiveReader>,
    params: MonadEthGetTransactionReceiptParams,
) -> JsonRpcResult<Option<MonadTransactionReceipt>> {
    trace!("monad_eth_getTransactionReceipt: {params:?}");

    let latest_block_num = get_block_num_from_tag(triedb_env, BlockTags::Latest).await?;
    if let Some(TransactionLocation {
        tx_index,
        block_num,
    }) = triedb_env
        .get_transaction_location_by_hash(params.tx_hash.0, latest_block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        if let Some(receipt) = get_receipt_from_triedb(triedb_env, block_num, tx_index).await? {
            return Ok(Some(receipt));
        }
    }

    // try archive if transaction hash not found and archive reader specified
    if let Some(archive_reader) = archive_reader {
        if let Ok(Some(tx_data)) = archive_reader.get_txdata(params.tx_hash.to_string()).await {
            let receipt = parse_tx_receipt(
                tx_data.header_subset.base_fee_per_gas,
                None, // FIXME block timestamp
                tx_data.header_subset.block_hash,
                &tx_data.tx,
                tx_data.header_subset.gas_used,
                tx_data.receipt,
                tx_data.header_subset.block_number,
                tx_data.header_subset.tx_index,
            )?;

            return Ok(Some(MonadTransactionReceipt(receipt)));
        }
    }

    Ok(None)
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
    archive_reader: &Option<ArchiveReader>,
    params: MonadEthGetTransactionByHashParams,
) -> JsonRpcResult<Option<MonadTransaction>> {
    trace!("monad_eth_getTransactionByHash: {params:?}");

    let latest_block_num = get_block_num_from_tag(triedb_env, BlockTags::Latest).await?;
    if let Some(TransactionLocation {
        tx_index,
        block_num,
    }) = triedb_env
        .get_transaction_location_by_hash(params.tx_hash.0, latest_block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        if let Some(tx) = get_transaction_from_triedb(triedb_env, block_num, tx_index).await? {
            return Ok(Some(tx));
        };
    }

    // try archive if transaction hash not found and archive reader specified
    if let Some(archive_reader) = archive_reader {
        if let Ok(Some(tx_data)) = archive_reader.get_txdata(params.tx_hash.to_string()).await {
            return parse_tx_content(
                tx_data.header_subset.block_hash,
                tx_data.header_subset.block_number,
                tx_data.header_subset.base_fee_per_gas,
                tx_data.tx,
                tx_data.header_subset.tx_index,
            )
            .map(|txn| Some(MonadTransaction(txn)));
        }
    }

    Ok(None)
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
    archive_reader: &Option<ArchiveReader>,
    params: MonadEthGetTransactionByBlockHashAndIndexParams,
) -> JsonRpcResult<Option<MonadTransaction>> {
    trace!("monad_eth_getTransactionByBlockHashAndIndex: {params:?}");

    let latest_block_num = get_block_num_from_tag(triedb_env, BlockTags::Latest).await?;
    if let Some(block_num) = triedb_env
        .get_block_number_by_hash(params.block_hash.0, latest_block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        if let Some(tx) = get_transaction_from_triedb(triedb_env, block_num, params.index.0).await?
        {
            return Ok(Some(tx));
        }
    }

    // try archive if block hash not found and archive reader specified
    if let Some(archive_reader) = archive_reader {
        if let Ok(block) = archive_reader.get_block_by_hash(&params.block_hash.0).await {
            if let Some(tx) = block.body.transactions.get(params.index.0 as usize) {
                return parse_tx_content(
                    params.block_hash.0.into(),
                    block.header.number,
                    block.header.base_fee_per_gas,
                    tx.clone(),
                    params.index.0,
                )
                .map(|txn| Some(MonadTransaction(txn)));
            }
        }
    }

    Ok(None)
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
    archive_reader: &Option<ArchiveReader>,
    params: MonadEthGetTransactionByBlockNumberAndIndexParams,
) -> JsonRpcResult<Option<MonadTransaction>> {
    trace!("monad_eth_getTransactionByBlockNumberAndIndex: {params:?}");

    let block_num = get_block_num_from_tag(triedb_env, params.block_tag).await?;
    if let Some(tx) = get_transaction_from_triedb(triedb_env, block_num, params.index.0).await? {
        return Ok(Some(tx));
    }

    // try archive if block header not found and archive reader specified
    if let Some(archive_reader) = archive_reader {
        if let Ok(block) = archive_reader.get_block_by_number(block_num).await {
            if let Some(tx) = block.body.transactions.get(params.index.0 as usize) {
                return parse_tx_content(
                    block.header.hash_slow(),
                    block.header.number,
                    block.header.base_fee_per_gas,
                    tx.clone(),
                    params.index.0,
                )
                .map(|txn| Some(MonadTransaction(txn)));
            }
        }
    }

    Ok(None)
}

async fn get_receipt_from_triedb<T: Triedb>(
    triedb_env: &T,
    block_num: u64,
    tx_index: u64,
) -> JsonRpcResult<Option<MonadTransactionReceipt>> {
    let header = match triedb_env
        .get_block_header(block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        Some(header) => header,
        None => return Ok(None),
    };

    let tx = match triedb_env
        .get_transaction(tx_index, block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        Some(tx) => tx,
        None => return Ok(None),
    };

    match triedb_env
        .get_receipt(tx_index, block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        Some(receipt) => {
            let gas_used = if tx_index > 0 {
                match triedb_env
                    .get_receipt(tx_index - 1, block_num)
                    .await
                    .map_err(JsonRpcError::internal_error)?
                {
                    Some(prev_receipt) => {
                        receipt.cumulative_gas_used()
                            - prev_receipt.cumulative_gas_used()
                    }
                    None => {
                        return Err(JsonRpcError::internal_error("error getting receipt".into()))
                    }
                }
            } else {
                receipt.cumulative_gas_used()
            };

            let receipt = parse_tx_receipt(
                header.header.base_fee_per_gas,
                Some(header.header.timestamp),
                header.hash,
                &tx,
                gas_used,
                receipt,
                block_num,
                tx_index,
            )?;

            Ok(Some(MonadTransactionReceipt(receipt)))
        }
        None => Ok(None),
    }
}

async fn get_transaction_from_triedb<T: Triedb>(
    triedb_env: &T,
    block_num: u64,
    tx_index: u64,
) -> JsonRpcResult<Option<MonadTransaction>> {
    let header = match triedb_env
        .get_block_header(block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        Some(header) => header,
        None => return Ok(None),
    };

    match triedb_env
        .get_transaction(tx_index, block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        Some(tx) => parse_tx_content(
            header.hash,
            header.header.number,
            header.header.base_fee_per_gas,
            tx,
            tx_index,
        )
        .map(|txn| Some(MonadTransaction(txn))),
        None => Ok(None),
    }
}
