use std::cmp::min;

use alloy_primitives::{
    aliases::{U128, U256, U64},
    Address, FixedBytes,
};
use alloy_rlp::Decodable;
use monad_eth_block_policy::{static_validate_transaction, TransactionError};
use monad_rpc_docs::rpc;
use reth_primitives::{keccak256, transaction::TransactionKind, Header, TransactionSigned};
use reth_rpc_types::{
    AccessListItem, Filter, FilteredParams, Log, Parity, Signature, Transaction, TransactionReceipt,
};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, trace, warn};

use crate::{
    block_handlers::block_receipts,
    block_util::{get_block_num_from_tag, FileBlockReader},
    eth_json_types::{
        BlockTags, EthAddress, EthHash, MonadLog, MonadTransaction, MonadTransactionReceipt,
        Quantity, UnformattedData,
    },
    jsonrpc::{JsonRpcError, JsonRpcResult},
    receipt::{decode_receipt, ReceiptDetails},
    triedb::{Triedb, TriedbResult},
};

pub fn parse_tx_content(
    block_hash: FixedBytes<32>,
    block_number: u64,
    base_fee: Option<u64>,
    tx: &TransactionSigned,
    tx_index: u64,
) -> Result<Transaction, JsonRpcError> {
    // recover transaction signer
    let Some(transaction) = tx.clone().into_ecrecovered_unchecked() else {
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
    tx: &TransactionSigned,
    prev_receipt: Option<ReceiptDetails>,
    receipt: ReceiptDetails,
    block_num: u64,
    tx_index: usize,
) -> Result<TransactionReceipt, JsonRpcError> {
    let Some(transaction) = tx.clone().into_ecrecovered_unchecked() else {
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
pub async fn monad_eth_getLogs<T: Triedb>(
    file_ledger_reader: &FileBlockReader,
    triedb_env: &T,
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
            let block_hash: FixedBytes<32>;
            let header = match triedb_env.get_block_header(block_num).await {
                TriedbResult::BlockHeader(block_header_rlp) => {
                    block_hash = keccak256(&block_header_rlp);
                    let Ok(header) = Header::decode(&mut block_header_rlp.as_slice()) else {
                        return Err(JsonRpcError::internal_error(
                            "decode block header failed".into(),
                        ));
                    };
                    header
                }
                _ => {
                    return Err(JsonRpcError::internal_error(
                        "error reading block header from db".into(),
                    ))
                }
            };

            let transactions = match triedb_env.get_transactions(block_num).await {
                TriedbResult::BlockTransactions(transactions) => transactions,
                TriedbResult::Null => vec![],
                _ => {
                    return Err(JsonRpcError::internal_error(
                        "error reading transactions from db".into(),
                    ))
                }
            };

            let header_logs_bloom = header.logs_bloom;

            if FilteredParams::matches_address(header_logs_bloom, &address_filter)
                || FilteredParams::matches_topics(header_logs_bloom, &topics_filter)
            {
                let block_receipts =
                    block_receipts(triedb_env, &header, block_hash, &transactions).await?;
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

#[rpc(method = "eth_getTransactionReceipt")]
#[allow(non_snake_case)]
/// Returns the receipt of a transaction by transaction hash.
pub async fn monad_eth_getTransactionReceipt<T: Triedb>(
    triedb_env: &T,
    params: MonadEthGetTransactionReceiptParams,
) -> JsonRpcResult<Option<MonadTransactionReceipt>> {
    trace!("monad_eth_getTransactionReceipt: {params:?}");

    let (block_num, tx_index) = match triedb_env
        .get_transaction_location_by_hash(params.tx_hash.0)
        .await
    {
        TriedbResult::TransactionLocation(block_num, tx_index) => (block_num, tx_index),
        TriedbResult::Null => return Ok(None),
        _ => {
            return Err(JsonRpcError::internal_error(
                "error reading tx hash from db".into(),
            ))
        }
    };

    let block_hash: FixedBytes<32>;
    let header = match triedb_env.get_block_header(block_num).await {
        TriedbResult::BlockHeader(block_header_rlp) => {
            block_hash = keccak256(&block_header_rlp);
            let Ok(header) = Header::decode(&mut block_header_rlp.as_slice()) else {
                return Err(JsonRpcError::internal_error(
                    "decode block header failed".into(),
                ));
            };
            header
        }
        TriedbResult::Null => {
            debug!("No block header found");
            return Ok(None);
        }
        _ => {
            return Err(JsonRpcError::internal_error(
                "error reading block header from db".into(),
            ))
        }
    };

    match triedb_env.get_receipt(tx_index, block_num).await {
        TriedbResult::Null => Ok(None),
        TriedbResult::Receipt(rlp_receipt) => {
            let mut rlp_buf = rlp_receipt.as_slice();
            let receipt = decode_receipt(&mut rlp_buf).map_err(|e| {
                JsonRpcError::internal_error(format!("decode receipt failed: {}", e))
            })?;

            let prev_receipt = if tx_index > 0 {
                let TriedbResult::Receipt(prev_receipt_rlp_buf) =
                    triedb_env.get_receipt(tx_index - 1, block_num).await
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

            let transaction = match triedb_env.get_transaction(tx_index, block_num).await {
                TriedbResult::Transaction(transaction) => transaction,
                _ => return Ok(None),
            };

            let receipt = parse_tx_receipt(
                &header,
                block_hash,
                &transaction,
                prev_receipt,
                receipt,
                block_num,
                tx_index as usize,
            )?;
            Ok(Some(MonadTransactionReceipt(receipt)))
        }
        _ => Err(JsonRpcError::internal_error("error reading from db".into())),
    }
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

    let (block_num, tx_index) = match triedb_env
        .get_transaction_location_by_hash(params.tx_hash.0)
        .await
    {
        TriedbResult::TransactionLocation(block_num, tx_index) => (block_num, tx_index),
        TriedbResult::Null => return Ok(None),
        _ => {
            return Err(JsonRpcError::internal_error(
                "error reading tx hash from db".into(),
            ))
        }
    };

    let block_hash: FixedBytes<32>;
    let header = match triedb_env.get_block_header(block_num).await {
        TriedbResult::BlockHeader(block_header_rlp) => {
            block_hash = keccak256(&block_header_rlp);
            let Ok(header) = Header::decode(&mut block_header_rlp.as_slice()) else {
                return Err(JsonRpcError::internal_error(
                    "decode block header failed".into(),
                ));
            };
            header
        }
        TriedbResult::Null => {
            debug!("No block header found");
            return Ok(None);
        }
        _ => {
            return Err(JsonRpcError::internal_error(
                "error reading block header from db".into(),
            ))
        }
    };

    let transaction = match triedb_env.get_transaction(tx_index, block_num).await {
        TriedbResult::Transaction(transaction) => transaction,
        _ => return Ok(None),
    };

    parse_tx_content(
        block_hash,
        block_num,
        header.base_fee_per_gas,
        &transaction,
        tx_index,
    )
    .map(|txn| Some(MonadTransaction(txn)))
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

    parse_tx_content(
        block.hash_slow(),
        block.number,
        block.base_fee_per_gas,
        transaction,
        params.index.0,
    )
    .map(|txn| Some(MonadTransaction(txn)))
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
pub async fn monad_eth_getTransactionByBlockNumberAndIndex<T: Triedb>(
    triedb_env: &T,
    params: MonadEthGetTransactionByBlockNumberAndIndexParams,
) -> JsonRpcResult<Option<MonadTransaction>> {
    trace!("monad_eth_getTransactionByBlockNumberAndIndex: {params:?}");

    let block_num = get_block_num_from_tag(triedb_env, params.block_tag).await?;

    let block_hash: FixedBytes<32>;
    let header = match triedb_env.get_block_header(block_num).await {
        TriedbResult::BlockHeader(block_header_rlp) => {
            block_hash = keccak256(&block_header_rlp);
            let Ok(header) = Header::decode(&mut block_header_rlp.as_slice()) else {
                return Err(JsonRpcError::internal_error(
                    "decode block header failed".into(),
                ));
            };
            header
        }
        TriedbResult::Null => {
            debug!("No block header found");
            return Ok(None);
        }
        _ => {
            return Err(JsonRpcError::internal_error(
                "error reading block header from db".into(),
            ))
        }
    };

    let transaction = match triedb_env.get_transaction(params.index.0, block_num).await {
        TriedbResult::Transaction(transaction) => transaction,
        _ => return Ok(None),
    };

    parse_tx_content(
        block_hash,
        header.number,
        header.base_fee_per_gas,
        &transaction,
        params.index.0,
    )
    .map(|txn| Some(MonadTransaction(txn)))
}
