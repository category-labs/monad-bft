use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use alloy_consensus::{ReceiptEnvelope, ReceiptWithBloom, Transaction as _, TxEnvelope};
use alloy_primitives::{Address, Bloom, FixedBytes, TxKind, B256};
use alloy_rlp::Decodable;
use alloy_rpc_types::{
    BlockNumberOrTag, Filter, FilterBlockOption, FilterSet, FilteredParams, Log, Receipt,
    TransactionReceipt,
};
use futures::stream::{self, StreamExt, TryStreamExt};
use itertools::Either;
use monad_archive::prelude::{ArchiveReader, BlockDataReader, Context, ContextCompat, IndexReader};
use monad_rpc_docs::rpc;
use monad_triedb_utils::triedb_env::{
    BlockHeader, BlockKey, ReceiptWithLogIndex, Triedb, TxEnvelopeWithSender,
};
use monad_types::SeqNum;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, trace, warn};

use crate::{
    chainstate::{get_block_key_from_tag, ChainState, ChainStateError},
    eth_json_types::{
        BlockTagOrHash, BlockTags, EthHash, MonadLog, MonadTransaction, MonadTransactionReceipt,
        Quantity, UnformattedData,
    },
    fee::BaseFeePerGas,
    handlers::eth::block::block_receipts,
    jsonrpc::{JsonRpcError, JsonRpcResult},
    txpool::{EthTxPoolBridgeClient, TxStatus},
};

pub fn parse_tx_receipt(
    base_fee_per_gas: Option<u64>,
    block_timestamp: Option<u64>,
    block_hash: FixedBytes<32>,
    tx: TxEnvelopeWithSender,
    gas_used: u128,
    receipt: ReceiptWithLogIndex,
    block_num: u64,
    tx_index: u64,
) -> TransactionReceipt {
    // unpack data
    let sender = tx.sender;
    let tx = tx.tx;
    let starting_log_index = receipt.starting_log_index;
    let receipt = receipt.receipt;

    // effective gas price is calculated according to eth json rpc specification
    let effective_gas_price = tx.effective_gas_price(base_fee_per_gas);

    let block_hash = Some(block_hash);
    let block_number = Some(block_num);

    let logs: Vec<Log> = receipt
        .logs()
        .iter()
        .enumerate()
        .map(|(log_index, log)| Log {
            inner: log.clone(),
            block_hash,
            block_number,
            block_timestamp,
            transaction_hash: Some(*tx.tx_hash()),
            transaction_index: Some(tx_index),
            log_index: Some(starting_log_index + log_index as u64),
            removed: Default::default(),
        })
        .collect();

    let contract_address = match tx.kind() {
        TxKind::Create => Some(sender.create(tx.nonce())),
        _ => None,
    };

    let receipt_with_bloom = ReceiptWithBloom {
        receipt: Receipt {
            status: receipt.status().into(),
            cumulative_gas_used: receipt.cumulative_gas_used(),
            logs,
        },
        logs_bloom: *receipt.logs_bloom(),
    };
    let inner_receipt: ReceiptEnvelope<Log> = match receipt {
        ReceiptEnvelope::Legacy(_) => ReceiptEnvelope::Legacy(receipt_with_bloom),
        ReceiptEnvelope::Eip2930(_) => ReceiptEnvelope::Eip2930(receipt_with_bloom),
        ReceiptEnvelope::Eip1559(_) => ReceiptEnvelope::Eip1559(receipt_with_bloom),
        _ => ReceiptEnvelope::Eip1559(receipt_with_bloom),
    };

    let tx_receipt = TransactionReceipt {
        inner: inner_receipt,
        transaction_hash: *tx.tx_hash(),
        transaction_index: Some(tx_index),
        block_hash,
        block_number,
        from: sender,
        to: tx.to(),
        contract_address,
        gas_used,
        effective_gas_price,
        // TODO: EIP4844 and EIP7702 fields
        blob_gas_used: None,
        blob_gas_price: None,
        authorization_list: None,
    };
    tx_receipt
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

#[derive(Serialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetLogsResult(pub Vec<MonadLog>);

#[derive(Debug, Deserialize, JsonSchema)]
pub struct MonadEthGetLogsParams {
    #[schemars(schema_with = "schema_for_filter")]
    filters: Filter,
}

fn schema_for_filter(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    schemars::schema_for_value!(Filter::new().from_block(0).to_block(1).address(
        "0xAc4b3DacB91461209Ae9d41EC517c2B9Cb1B7DAF"
            .parse::<Address>()
            .unwrap()
    ))
    .schema
    .into()
}

#[rpc(method = "eth_getLogs", ignore = "max_block_range")]
#[allow(non_snake_case)]
/// Returns an array of all logs matching filter with given id.
#[tracing::instrument(level = "debug", skip(archive_reader))]
pub async fn monad_eth_getLogs<T: Triedb>(
    triedb_env: &T,
    archive_reader: &Option<ArchiveReader>,
    max_block_range: u64,
    p: MonadEthGetLogsParams,
    use_eth_get_logs_index: bool,
    dry_run_get_logs_index: bool,
    max_finalized_block_cache_len: u64,
) -> JsonRpcResult<MonadEthGetLogsResult> {
    trace!("monad_eth_getLogs: {p:?}");

    let filter = p.filters;
    let (from_block, to_block) = match filter.block_option {
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

            let from_block = get_block_key_from_tag(triedb_env, from_block_tag);
            let to_block = get_block_key_from_tag(triedb_env, to_block_tag);
            let latest_block = get_block_key_from_tag(triedb_env, BlockTags::Latest);
            (
                from_block.seq_num().0,
                std::cmp::min(to_block.seq_num().0, latest_block.seq_num().0),
            )
        }
        FilterBlockOption::AtBlockHash(block_hash) => {
            let latest_block_key = get_block_key_from_tag(triedb_env, BlockTags::Latest);

            let block = triedb_env
                .get_block_number_by_hash(latest_block_key, block_hash.into())
                .await
                .map_err(|e| {
                    warn!("Error getting block number by hash: {e:?}");
                    JsonRpcError::internal_error("could not get block hash".to_string())
                })?;
            let block_num = match block {
                Some(block_num) => block_num,
                None => {
                    // retry from archive reader if block hash not available in triedb
                    if let Some(archive_reader) = archive_reader {
                        if let Ok(block) = archive_reader
                            .get_block_by_hash(&block_hash)
                            .await
                            .inspect_err(|e| {
                                warn!("Error getting block by hash from archive: {e:?}");
                            })
                        {
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
    if to_block - from_block > max_block_range {
        return Err(FilterError::RangeTooLarge.into());
    }

    let latest_block = get_block_key_from_tag(triedb_env, BlockTags::Latest)
        .seq_num()
        .0;
    // Only use index if no blocks are cached, otherwise use triedb + cache
    let to_block_outside_cache = to_block + max_finalized_block_cache_len < latest_block;
    // Determine if the request actually filters any logs.
    // We only want to use the index if the query constrains the result set.
    // This is the case when either:
    //  * at least one address is provided, or
    //  * at least one topic filter set is non‑empty (i.e. it contains a value to match on).
    let has_filters = !filter.address.is_empty() || filter.topics.iter().any(|t| !t.is_empty());

    if use_eth_get_logs_index && archive_reader.is_some() && to_block_outside_cache && has_filters {
        let archive_reader = archive_reader.as_ref().unwrap();
        trace!("Using eth_getLogs index");
        match get_logs_with_index(archive_reader, from_block, to_block, &filter).await {
            Ok(logs) => {
                return Ok(MonadEthGetLogsResult(
                    logs.into_iter().map(MonadLog).collect(),
                ));
            }
            Err(e) => {
                debug!(
                    "Error getting logs with index. Falling back to unindexed method. Error: {e:?}"
                );
            }
        }
    }

    let filtered_params = FilteredParams::new(Some(filter.clone()));
    let block_range = from_block..=to_block;

    let filter_match =
        |bloom: Bloom, address: &FilterSet<Address>, topics: &[FilterSet<B256>]| -> bool {
            FilteredParams::matches_address(bloom, &FilteredParams::address_filter(address))
                && FilteredParams::matches_topics(bloom, &FilteredParams::topics_filter(topics))
        };

    let triedb_stream = stream::iter(block_range)
        .map(|block_num| {
            let block_key = triedb_env.get_block_key(SeqNum(block_num));
            let filter_clone = filter.clone();
            async move {
                if let Some(header) = triedb_env
                    .get_block_header(block_key)
                    .await
                    .map_err(JsonRpcError::internal_error)?
                {
                    if filter_match(
                        header.header.logs_bloom,
                        &filter_clone.address,
                        &filter_clone.topics,
                    ) {
                        // try fetching from triedb
                        if let Ok(transactions) = triedb_env.get_transactions(block_key).await {
                            let bloom_receipts = triedb_env
                                .get_receipts(block_key)
                                .await
                                .map_err(JsonRpcError::internal_error)?;
                            // successfully fetched from triedb
                            Ok(Either::Left((header, transactions, bloom_receipts)))
                        } else {
                            // header exists but not transactions, block is statesynced
                            // pass block number to try for archive
                            Ok(Either::Right(block_num))
                        }
                    } else {
                        Ok(Either::Left((header, vec![], vec![])))
                    }
                } else {
                    Ok(Either::Right(block_num)) // pass block number to try for archive
                }
            }
        })
        .buffered(10);

    let data = triedb_stream
        .map(|result| {
            let filter_clone = filter.clone();
            async move {
                match result {
                    Ok(Either::Left(data)) => Ok(data), // successfully fetched from triedb
                    Ok(Either::Right(block_num)) => {
                        // fallback and try fetching from archive
                        if let Some(archive_reader) = archive_reader {
                            let block = archive_reader
                                .get_block_by_number(block_num)
                                .await
                                .map_err(|e| {
                                    warn!("Error getting block header from archiver: {e:?}");
                                    JsonRpcError::internal_error(
                                        "error getting block header from archiver".into(),
                                    )
                                })?;
                            if filter_match(
                                block.header.logs_bloom,
                                &filter_clone.address,
                                &filter_clone.topics,
                            ) {
                                let bloom_receipts = archive_reader
                                    .get_block_receipts(block_num)
                                    .await
                                    .map_err(|e| {
                                        warn!("Error getting block receipts from archiver: {e:?}");
                                        JsonRpcError::internal_error(
                                            "error getting block receipts from archiver".into(),
                                        )
                                    })?;
                                Ok((
                                    BlockHeader {
                                        hash: block.header.hash_slow(),
                                        header: block.header,
                                    },
                                    block.body.transactions,
                                    bloom_receipts,
                                ))
                            } else {
                                Ok((
                                    BlockHeader {
                                        hash: block.header.hash_slow(),
                                        header: block.header,
                                    },
                                    vec![],
                                    vec![],
                                ))
                            }
                        } else {
                            Err(JsonRpcError::internal_error(
                                "error getting block header from triedb and archive".into(),
                            ))
                        }
                    }
                    Err(err) => Err(err),
                }
            }
        })
        .buffered(100)
        .collect::<Vec<_>>()
        .await;

    let data = data.into_iter().collect::<Result<Vec<_>, _>>()?;

    let receipts: Vec<TransactionReceipt> = data
        .iter()
        .map(|(header, transactions, bloom_receipts)| {
            block_receipts(
                transactions.to_vec(),
                bloom_receipts.to_vec(),
                &header.header,
                header.hash,
            )
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect();

    let receipt_logs: Vec<Log> = receipts
        .into_iter()
        .flat_map(|receipt| {
            let logs: Vec<Log> = receipt
                .inner
                .logs()
                .iter()
                .filter(|log: &&Log| {
                    !(filtered_params.filter.is_some()
                        && (!filtered_params.filter_address(&log.address())
                            || !filtered_params.filter_topics(log.topics())))
                })
                .cloned()
                .collect();
            logs
        })
        .collect();

    if dry_run_get_logs_index {
        let non_indexed = HashSet::from_iter(receipt_logs.iter().cloned());
        if let Some(archive_reader) = archive_reader.clone() {
            tokio::spawn(async move {
                if let Err(e) = check_dry_run_get_logs_index(
                    archive_reader,
                    from_block,
                    to_block,
                    filter,
                    non_indexed,
                )
                .await
                {
                    warn!("Error checking dry run get logs index: {e:?}");
                }
            });
        }
    }

    Ok(MonadEthGetLogsResult(
        receipt_logs.into_iter().map(MonadLog).collect(),
    ))
}

async fn check_dry_run_get_logs_index(
    archive_reader: ArchiveReader,
    from_block: u64,
    to_block: u64,
    filter: Filter,
    non_indexed: HashSet<Log>,
) -> monad_archive::prelude::Result<()> {
    let indexed = get_logs_with_index(&archive_reader, from_block, to_block, &filter)
        .await
        .map(HashSet::from_iter)
        .wrap_err("Error getting logs with index")?;

    let group_by = |mut map: HashMap<_, _>, log: &Log| {
        let Some(block_number) = log.block_number else {
            return map;
        };
        let Some(transaction_hash) = log.transaction_hash else {
            return map;
        };
        map.entry(block_number)
            .or_insert_with(|| Vec::with_capacity(2))
            .push(transaction_hash.to_string());
        map
    };

    let non_indexed_only = non_indexed
        .difference(&indexed)
        .fold(HashMap::new(), group_by);
    let indexed_only = indexed
        .difference(&non_indexed)
        .fold(HashMap::new(), group_by);

    if non_indexed_only.is_empty() && indexed_only.is_empty() {
        debug!("Indexed and non-indexed logs are identical");
    } else {
        let non_indexed_only_json = serde_json::to_string(&non_indexed_only)?;
        let indexed_only_json = serde_json::to_string(&indexed_only)?;
        warn!(
            non_indexed_only = non_indexed_only_json,
            indexed_only = indexed_only_json,
            "Index and non-index logs are not identical"
        );
    }

    Ok(())
}

async fn get_logs_with_index(
    reader: &ArchiveReader,
    from_block: u64,
    to_block: u64,
    filter: &Filter,
) -> monad_archive::prelude::Result<Vec<Log>> {
    let log_index = reader
        .log_index
        .as_ref()
        .wrap_err("Log index reader not present")?;

    let latest_indexed_tx = reader
        .get_latest_indexed()
        .await?
        .wrap_err("Latest indexed tx not found")?;

    if latest_indexed_tx < to_block {
        monad_archive::prelude::bail!(
            "Latest indexed tx is less than to_block. {}, {}",
            latest_indexed_tx,
            to_block
        );
    }

    let filtered_params = FilteredParams::new(Some(filter.clone()));

    // Note: we an limit returned (and queried!) data by using `query_logs_index_streamed`
    // and take_while we're under the response size limit
    let potential_matches = log_index
        .query_logs(from_block, to_block, filter.address.iter(), &filter.topics)
        .await?;
    let potential_matches = potential_matches.try_collect::<Vec<_>>().await?;

    Ok(potential_matches
        .into_iter()
        .flat_map(|tx_data| {
            let receipt = parse_tx_receipt(
                tx_data.header_subset.base_fee_per_gas,
                Some(tx_data.header_subset.block_timestamp),
                tx_data.header_subset.block_hash,
                tx_data.tx,
                tx_data.header_subset.gas_used,
                tx_data.receipt,
                tx_data.header_subset.block_number,
                tx_data.header_subset.tx_index,
            );
            receipt
                .inner
                .logs()
                .iter()
                .filter(|log: &&Log| {
                    !(filtered_params.filter.is_some()
                        && (!filtered_params.filter_address(&log.address())
                            || !filtered_params.filter_topics(log.topics())))
                })
                .cloned()
                .collect::<Vec<_>>()
        })
        .collect())
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthSendRawTransactionParams {
    hex_tx: UnformattedData,
}

fn base_fee_validation(max_fee_per_gas: u128, base_fee: impl BaseFeePerGas) -> bool {
    let current_block = 0; // TODO: this can get latest block from triedb in future
    let block_threshold = 1000; // TODO: configurable range of how many blocks to consider

    let Some(min_potential_base_fee) =
        base_fee.min_potential_base_fee_in_range(current_block, block_threshold)
    else {
        return false;
    };

    if max_fee_per_gas < min_potential_base_fee {
        return false;
    }

    true
}

const MAX_CONCURRENT_SEND_RAW_TX: usize = 1_000;
// TODO: need to support EIP-4844 transactions
#[rpc(method = "eth_sendRawTransaction", ignore = "tx_pool", ignore = "ipc")]
#[allow(non_snake_case)]
#[tracing::instrument(level = "debug", skip(txpool_bridge_client))]
/// Submits a raw transaction. For EIP-4844 transactions, the raw form must be the network form.
/// This means it includes the blobs, KZG commitments, and KZG proofs.
pub async fn monad_eth_sendRawTransaction<T: Triedb>(
    triedb_env: &T,
    txpool_bridge_client: &EthTxPoolBridgeClient,
    base_fee_per_gas: impl BaseFeePerGas,
    params: MonadEthSendRawTransactionParams,
    chain_id: u64,
    allow_unprotected_txs: bool,
) -> JsonRpcResult<String> {
    trace!("monad_eth_sendRawTransaction: {params:?}");
    let tx_inflight_guard = txpool_bridge_client.acquire_inflight_tx_guard();
    // (strong_count-1) is the total number of pending requests
    // This is because the Arc is held until the scope is exited.
    if Arc::strong_count(&tx_inflight_guard) > MAX_CONCURRENT_SEND_RAW_TX {
        warn!(MAX_CONCURRENT_SEND_RAW_TX, "txpool overloaded");
        return Err(JsonRpcError::custom(
            "overloaded, try again later".to_owned(),
        ));
    }

    match TxEnvelope::decode(&mut &params.hex_tx.0[..]) {
        Ok(tx) => {
            // drop pre EIP-155 transactions if disallowed by the rpc (for user protection purposes)
            if !allow_unprotected_txs && tx.chain_id().is_none() {
                return Err(JsonRpcError::custom(
                    "Unprotected transactions (pre-EIP155) are not allowed over RPC".to_string(),
                ));
            }

            if !base_fee_validation(tx.max_fee_per_gas(), base_fee_per_gas) {
                return Err(JsonRpcError::custom(
                    "maxFeePerGas too low to be include in upcoming blocks".to_string(),
                ));
            }

            let hash = *tx.tx_hash();
            debug!(name = "sendRawTransaction", txn_hash = ?hash);

            let (tx_status_send, tx_status_recv) = tokio::sync::oneshot::channel::<TxStatus>();

            if let Err(err) = txpool_bridge_client.try_send(tx, tx_status_send) {
                warn!(?err, "mempool ipc send error");
                return Err(JsonRpcError::internal_error(
                    "unable to send to validator".into(),
                ));
            }

            match tokio::time::timeout(Duration::from_secs(1), tx_status_recv).await {
                Ok(Ok(tx_status)) => match tx_status {
                    TxStatus::Evicted { reason: _ } | TxStatus::Replaced => {
                        return Err(JsonRpcError::custom("rejected".to_string()))
                    }
                    TxStatus::Dropped { reason } => {
                        return Err(JsonRpcError::custom(reason.as_user_string()))
                    }
                    TxStatus::Pending | TxStatus::Tracked | TxStatus::Committed => {
                        return Ok(hash.to_string())
                    }
                    TxStatus::Unknown => {
                        error!("txpool bridge sent unknown status");
                    }
                },
                Ok(Err(_)) | Err(_) => {
                    warn!("txpool not responding");
                }
            }

            return Err(JsonRpcError::custom("txpool not responding".to_string()));
        }
        Err(e) => {
            debug!(?e, "eth txn decode failed");
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
#[tracing::instrument(level = "debug", skip(chain_state))]
pub async fn monad_eth_getTransactionReceipt<T: Triedb>(
    chain_state: &ChainState<T>,
    params: MonadEthGetTransactionReceiptParams,
) -> JsonRpcResult<Option<MonadTransactionReceipt>> {
    trace!("monad_eth_getTransactionReceipt: {params:?}");

    match chain_state.get_transaction_receipt(params.tx_hash.0).await {
        Ok(receipt) => Ok(Some(MonadTransactionReceipt(receipt))),
        Err(ChainStateError::ResourceNotFound) => Ok(None),
        Err(ChainStateError::Triedb(err)) => Err(JsonRpcError::internal_error(err)),
    }
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetTransactionByHashParams {
    tx_hash: EthHash,
}

#[rpc(method = "eth_getTransactionByHash")]
#[allow(non_snake_case)]
/// Returns the information about a transaction requested by transaction hash.
#[tracing::instrument(level = "debug", skip(chain_state))]
pub async fn monad_eth_getTransactionByHash<T: Triedb>(
    chain_state: &ChainState<T>,
    params: MonadEthGetTransactionByHashParams,
) -> JsonRpcResult<Option<MonadTransaction>> {
    trace!("monad_eth_getTransactionByHash: {params:?}");

    match chain_state.get_transaction(params.tx_hash.0).await {
        Ok(tx) => Ok(Some(MonadTransaction(tx))),
        Err(ChainStateError::ResourceNotFound) => Ok(None),
        Err(ChainStateError::Triedb(err)) => Err(JsonRpcError::internal_error(err)),
    }
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetTransactionByBlockHashAndIndexParams {
    block_hash: EthHash,
    index: Quantity,
}

#[rpc(method = "eth_getTransactionByBlockHashAndIndex")]
#[allow(non_snake_case)]
#[tracing::instrument(level = "debug", skip(chain_state))]
/// Returns information about a transaction by block hash and transaction index position.
pub async fn monad_eth_getTransactionByBlockHashAndIndex<T: Triedb>(
    chain_state: &ChainState<T>,
    params: MonadEthGetTransactionByBlockHashAndIndexParams,
) -> JsonRpcResult<Option<MonadTransaction>> {
    trace!("monad_eth_getTransactionByBlockHashAndIndex: {params:?}");

    match chain_state
        .get_transaction_with_block_and_index(
            BlockTagOrHash::Hash(params.block_hash),
            params.index.0,
        )
        .await
    {
        Ok(tx) => Ok(Some(MonadTransaction(tx))),
        Err(ChainStateError::ResourceNotFound) => Ok(None),
        Err(ChainStateError::Triedb(err)) => Err(JsonRpcError::internal_error(err)),
    }
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetTransactionByBlockNumberAndIndexParams {
    block_tag: BlockTags,
    index: Quantity,
}

#[rpc(method = "eth_getTransactionByBlockNumberAndIndex")]
#[allow(non_snake_case)]
#[tracing::instrument(level = "debug", skip(chain_state))]
/// Returns information about a transaction by block number and transaction index position.
pub async fn monad_eth_getTransactionByBlockNumberAndIndex<T: Triedb>(
    chain_state: &ChainState<T>,
    params: MonadEthGetTransactionByBlockNumberAndIndexParams,
) -> JsonRpcResult<Option<MonadTransaction>> {
    trace!("monad_eth_getTransactionByBlockNumberAndIndex: {params:?}");

    match chain_state
        .get_transaction_with_block_and_index(
            crate::eth_json_types::BlockTagOrHash::BlockTags(params.block_tag),
            params.index.0,
        )
        .await
    {
        Ok(tx) => Ok(Some(MonadTransaction(tx))),
        Err(ChainStateError::ResourceNotFound) => Ok(None),
        Err(ChainStateError::Triedb(err)) => Err(JsonRpcError::internal_error(err)),
    }
}

#[tracing::instrument(level = "debug")]
async fn get_transaction_from_triedb<T: Triedb>(
    triedb_env: &T,
    block_key: BlockKey,
    tx_index: u64,
) -> JsonRpcResult<Option<MonadTransaction>> {
    let header = match triedb_env
        .get_block_header(block_key)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        Some(header) => header,
        None => return Ok(None),
    };

    match triedb_env
        .get_transaction(block_key, tx_index)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        Some(tx) => Ok(Some(MonadTransaction(crate::chainstate::parse_tx_content(
            header.hash,
            header.header.number,
            header.header.base_fee_per_gas,
            tx,
            tx_index,
        )))),
        None => Ok(None),
    }
}

async fn spawn_rayon_async<F, R>(func: F) -> Result<R, tokio::sync::oneshot::error::RecvError>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let (tx, rx) = tokio::sync::oneshot::channel();
    rayon::spawn(|| {
        let _ = tx.send(func());
    });
    rx.await
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope};
    use alloy_eips::eip2718::Encodable2718;
    use alloy_primitives::{Address, FixedBytes, TxKind};
    use alloy_rlp::Encodable;
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;
    use monad_triedb_utils::{mock_triedb::MockTriedb, triedb_env::Account};

    use super::{monad_eth_sendRawTransaction, MonadEthSendRawTransactionParams};
    use crate::{eth_json_types::UnformattedData, fee::FixedFee, txpool::EthTxPoolBridgeClient};

    fn serialize_tx(tx: (impl Encodable + Encodable2718)) -> UnformattedData {
        let mut rlp_encoded_tx = Vec::new();
        tx.encode_2718(&mut rlp_encoded_tx);
        UnformattedData(rlp_encoded_tx)
    }

    fn make_tx(
        sender: FixedBytes<32>,
        max_fee_per_gas: u128,
        max_priority_fee_per_gas: u128,
        gas_limit: u64,
        nonce: u64,
        chain_id: u64,
    ) -> TxEnvelope {
        let transaction = TxEip1559 {
            chain_id,
            nonce,
            gas_limit,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            to: TxKind::Call(Address::repeat_byte(0u8)),
            value: Default::default(),
            access_list: Default::default(),
            input: vec![].into(),
        };

        let signer = PrivateKeySigner::from_bytes(&sender).unwrap();
        let signature = signer
            .sign_hash_sync(&transaction.signature_hash())
            .unwrap();
        transaction.into_signed(signature).into()
    }

    #[tokio::test]
    async fn eth_send_raw_transaction() {
        let mut triedb = MockTriedb::default();
        let sender = FixedBytes::<32>::from([1u8; 32]);
        let signer = PrivateKeySigner::from_bytes(&sender).unwrap();

        triedb.set_account(
            signer.address().0.into(),
            Account {
                nonce: 10,
                ..Default::default()
            },
        );

        let expected_failures = vec![
            MonadEthSendRawTransactionParams {
                hex_tx: serialize_tx(make_tx(sender, 1000, 1000, 21_000, 11, 1337)), // invaid chain id
            },
            MonadEthSendRawTransactionParams {
                hex_tx: serialize_tx(make_tx(sender, 1000, 1000, 1_000, 11, 1)), // intrinsic gas too low
            },
            MonadEthSendRawTransactionParams {
                hex_tx: serialize_tx(make_tx(sender, 1000, 1000, 400_000_000_000, 11, 1)), // gas too high
            },
            MonadEthSendRawTransactionParams {
                hex_tx: serialize_tx(make_tx(sender, 1000, 1000, 21_000, 1, 1)), // nonce too low
            },
            MonadEthSendRawTransactionParams {
                hex_tx: serialize_tx(make_tx(sender, 1000, 12000, 21_000, 11, 1)), // max priority fee too high
            },
        ];

        for (idx, case) in expected_failures.into_iter().enumerate() {
            assert!(
                monad_eth_sendRawTransaction(
                    &triedb,
                    &EthTxPoolBridgeClient::for_testing(),
                    FixedFee::new(2000),
                    case,
                    1,
                    true
                )
                .await
                .is_err(),
                "Expected error for case: {:?}",
                idx + 1
            );
        }
    }
}
