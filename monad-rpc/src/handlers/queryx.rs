// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

//! queryX JSON-RPC methods (`eth_queryBlocks`, `eth_queryTransactions`,
//! `eth_queryLogs`, `eth_queryTraces`, `eth_queryTransfers`).
//!
//! These thin handlers translate the queryX request envelope into the
//! `monad-chain-data` query API, run the query against the embedded
//! `MonadChainDataService`, then project the typed result rows back into
//! the queryX JSON response shape (`{ data, fromBlock, toBlock,
//! cursorBlock }`) honoring the request's `fields` selection. See the
//! `monad-chain-data/queryX` spec document for the full method contract.

use std::collections::{HashMap, HashSet};

use alloy_consensus::Transaction;
use alloy_eips::{
    eip2718::{Encodable2718, Typed2718},
    BlockNumberOrTag,
};
use alloy_primitives::{Address, Bytes, Log as PrimitiveLog, LogData, B256, U256};
use alloy_rpc_types::{Filter, FilterBlockOption, Log as RpcLog};
use monad_chain_data::{
    scan_block_logs, scan_block_txs, Block, BlockRef, BlockSpan, CallKind,
    ConfiguredChainDataReader, EvmBlockHeader, Hash32, LimitExceededKind, LogEntry, LogFilter,
    LogsRelations, MemLogsBlock, MemTx, MonadChainDataError, QueryBlocksRequest,
    QueryBlocksResponse, QueryEnvelope, QueryLogsRequest, QueryLogsResponse, QueryOrder,
    QueryTracesRequest, QueryTracesResponse, QueryTransactionsRequest, QueryTransactionsResponse,
    QueryTransfersRequest, QueryTransfersResponse, TraceEntry, TraceFilter, TracesRelations,
    TransferEntry, TransferFilter, TransfersRelations, TxEntry, TxFilter, TxsRelations,
};
use monad_triedb_utils::triedb_env::Triedb;
use serde::Deserialize;
use serde_json::{value::RawValue, Map, Value};
use tracing::debug;

use crate::{
    data::DataProvider,
    handlers::{eth::txn::FilterError, resources::MonadRpcResources},
    middleware::TimingRequestId,
    types::{
        eth_json::{serialize_result, MonadLog},
        ethhex,
        jsonrpc::{JsonRpcError, JsonRpcResult, JsonRpcResultExt, RequestParams},
    },
};

/// Configured chain-data reader backing the queryX methods.
pub type ChainDataService = ConfiguredChainDataReader;

const BLOCK_FIELDS: &[&str] = &[
    "number",
    "hash",
    "parentHash",
    "timestamp",
    "miner",
    "gasLimit",
    "gasUsed",
    "baseFeePerGas",
    "stateRoot",
    "transactionsRoot",
    "receiptsRoot",
    "logsBloom",
    "extraData",
    "nonce",
    "mixHash",
    "difficulty",
];
const TX_FIELDS: &[&str] = &[
    "blockNumber",
    "blockHash",
    "transactionIndex",
    "hash",
    "from",
    "to",
    "nonce",
    "value",
    "gas",
    "gasPrice",
    "maxFeePerGas",
    "maxPriorityFeePerGas",
    "input",
    "type",
    "chainId",
];
const LOG_FIELDS: &[&str] = &[
    "blockNumber",
    "blockHash",
    "transactionIndex",
    "logIndex",
    "address",
    "topics",
    "data",
];
const TRACE_FIELDS: &[&str] = &[
    "blockNumber",
    "blockHash",
    "transactionIndex",
    "traceAddress",
    "type",
    "from",
    "to",
    "value",
    "gas",
    "gasUsed",
    "input",
    "output",
    "status",
    "depth",
];
const TRANSFER_FIELDS: &[&str] = &[
    "blockNumber",
    "blockHash",
    "transactionIndex",
    "traceAddress",
    "type",
    "from",
    "to",
    "value",
];

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawQueryRequest {
    #[serde(default)]
    filter: Option<Value>,
    #[serde(default)]
    fields: Option<HashMap<String, FieldSelector>>,
    #[serde(default)]
    order: Option<String>,
    #[serde(default)]
    from_block: Option<String>,
    #[serde(default)]
    to_block: Option<String>,
    #[serde(default)]
    limit: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum FieldSelector {
    All(bool),
    List(Vec<String>),
}

#[derive(Debug, Clone)]
struct QueryFieldPlan {
    primary: Vec<&'static str>,
    blocks: Option<Vec<&'static str>>,
    transactions: Option<Vec<&'static str>>,
}

#[allow(non_snake_case)]
pub async fn eth_queryBlocks(
    _: TimingRequestId,
    app_state: &MonadRpcResources,
    params: RequestParams<'_>,
) -> Result<Box<RawValue>, JsonRpcError> {
    let service = app_state.chain_data.as_ref().method_not_supported()?;
    let raw = decode_query_params(params)?;
    validate_empty_filter(raw.filter.as_ref())?;
    let fields = fields_plan(
        raw.fields.as_ref(),
        "blocks",
        BLOCK_FIELDS,
        &[("blocks", BLOCK_FIELDS)],
    )?;
    let request = QueryBlocksRequest {
        envelope: build_envelope(&raw, service).await?,
    };
    let response = service
        .query_blocks(request)
        .await
        .map_err(chain_data_error_to_jsonrpc)?;
    serialize_queryx_result(
        project_blocks_response(response, &fields.primary),
        app_state,
    )
}

#[allow(non_snake_case)]
pub async fn eth_queryTransactions(
    _: TimingRequestId,
    app_state: &MonadRpcResources,
    params: RequestParams<'_>,
) -> Result<Box<RawValue>, JsonRpcError> {
    let service = app_state.chain_data.as_ref().method_not_supported()?;
    let raw = decode_query_params(params)?;
    let fields = fields_plan(
        raw.fields.as_ref(),
        "transactions",
        TX_FIELDS,
        &[("transactions", TX_FIELDS), ("blocks", BLOCK_FIELDS)],
    )?;
    let response = execute_txs_query(
        service,
        app_state.data_provider.as_ref(),
        &raw,
        parse_tx_filter(raw.filter.as_ref())?,
        TxsRelations {
            blocks: fields.blocks.is_some(),
        },
    )
    .await?;
    serialize_queryx_result(project_txs_response(response, &fields)?, app_state)
}

#[allow(non_snake_case)]
pub async fn eth_queryLogs(
    _: TimingRequestId,
    app_state: &MonadRpcResources,
    params: RequestParams<'_>,
) -> Result<Box<RawValue>, JsonRpcError> {
    let service = app_state.chain_data.as_ref().method_not_supported()?;
    let raw = decode_query_params(params)?;
    let fields = fields_plan(
        raw.fields.as_ref(),
        "logs",
        LOG_FIELDS,
        &[
            ("logs", LOG_FIELDS),
            ("transactions", TX_FIELDS),
            ("blocks", BLOCK_FIELDS),
        ],
    )?;
    let response = execute_logs_query(
        service,
        app_state.data_provider.as_ref(),
        &raw,
        parse_log_filter(raw.filter.as_ref())?,
        LogsRelations {
            blocks: fields.blocks.is_some(),
            transactions: fields.transactions.is_some(),
        },
    )
    .await?;
    serialize_queryx_result(project_logs_response(response, &fields)?, app_state)
}

#[allow(non_snake_case)]
pub async fn eth_queryTraces(
    _: TimingRequestId,
    app_state: &MonadRpcResources,
    params: RequestParams<'_>,
) -> Result<Box<RawValue>, JsonRpcError> {
    let service = app_state.chain_data.as_ref().method_not_supported()?;
    let raw = decode_query_params(params)?;
    let fields = fields_plan(
        raw.fields.as_ref(),
        "traces",
        TRACE_FIELDS,
        &[
            ("traces", TRACE_FIELDS),
            ("transactions", TX_FIELDS),
            ("blocks", BLOCK_FIELDS),
        ],
    )?;
    let request = QueryTracesRequest {
        envelope: build_envelope(&raw, service).await?,
        filter: parse_trace_filter(raw.filter.as_ref())?,
        relations: TracesRelations {
            blocks: fields.blocks.is_some(),
            transactions: fields.transactions.is_some(),
        },
    };
    let response = service
        .query_traces(request)
        .await
        .map_err(chain_data_error_to_jsonrpc)?;
    serialize_queryx_result(project_traces_response(response, &fields)?, app_state)
}

#[allow(non_snake_case)]
pub async fn eth_queryTransfers(
    _: TimingRequestId,
    app_state: &MonadRpcResources,
    params: RequestParams<'_>,
) -> Result<Box<RawValue>, JsonRpcError> {
    let service = app_state.chain_data.as_ref().method_not_supported()?;
    let raw = decode_query_params(params)?;
    let fields = fields_plan(
        raw.fields.as_ref(),
        "transfers",
        TRANSFER_FIELDS,
        &[
            ("transfers", TRANSFER_FIELDS),
            ("transactions", TX_FIELDS),
            ("blocks", BLOCK_FIELDS),
        ],
    )?;
    let request = QueryTransfersRequest {
        envelope: build_envelope(&raw, service).await?,
        filter: parse_transfer_filter(raw.filter.as_ref())?,
        relations: TransfersRelations {
            blocks: fields.blocks.is_some(),
            transactions: fields.transactions.is_some(),
        },
    };
    let response = service
        .query_transfers(request)
        .await
        .map_err(chain_data_error_to_jsonrpc)?;
    serialize_queryx_result(project_transfers_response(response, &fields)?, app_state)
}

fn decode_query_params(params: RequestParams<'_>) -> Result<RawQueryRequest, JsonRpcError> {
    if params.get().trim().is_empty() {
        return Ok(RawQueryRequest::default());
    }
    let values: Vec<RawQueryRequest> = serde_json::from_str(params.get()).invalid_params()?;
    match values.len() {
        1 => Ok(values.into_iter().next().expect("one value")),
        _ => Err(JsonRpcError::invalid_params()),
    }
}

async fn build_envelope(
    raw: &RawQueryRequest,
    service: &ChainDataService,
) -> Result<QueryEnvelope, JsonRpcError> {
    let order = match raw.order.as_deref().unwrap_or("asc") {
        "asc" => QueryOrder::Ascending,
        "desc" => QueryOrder::Descending,
        _ => return Err(JsonRpcError::invalid_params()),
    };

    let needs_head = raw.from_block.as_deref() == Some("finalized")
        || raw.to_block.as_deref() == Some("finalized");
    let head = if needs_head {
        Some(
            service
                .load_published_head()
                .await
                .map_err(chain_data_error_to_jsonrpc)?
                .ok_or_else(|| JsonRpcError::internal_error("no published blocks".to_string()))?,
        )
    } else {
        None
    };

    Ok(QueryEnvelope {
        from_block: parse_block_bound(raw.from_block.as_deref(), head)?,
        to_block: parse_block_bound(raw.to_block.as_deref(), head)?,
        order,
        limit: parse_limit(raw.limit.as_deref())?,
    })
}

fn parse_block_bound(
    value: Option<&str>,
    finalized_head: Option<u64>,
) -> Result<Option<u64>, JsonRpcError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value == "finalized" {
        return Ok(finalized_head);
    }
    // chain-data only indexes finalized blocks, so non-finalized tags have
    // no meaning here; reject them rather than silently treating them as a
    // number.
    if matches!(value, "latest" | "safe" | "earliest" | "pending") {
        return Err(JsonRpcError::invalid_params());
    }
    ethhex::decode_quantity(value)
        .map(Some)
        .map_err(|_| JsonRpcError::invalid_params())
}

fn parse_limit(value: Option<&str>) -> Result<usize, JsonRpcError> {
    let Some(value) = value else {
        return Ok(monad_chain_data::DEFAULT_QUERY_LIMIT);
    };
    let limit = ethhex::decode_quantity(value).map_err(|_| JsonRpcError::invalid_params())?;
    usize::try_from(limit).map_err(|_| JsonRpcError::invalid_params())
}

fn fields_plan(
    fields: Option<&HashMap<String, FieldSelector>>,
    primary_key: &'static str,
    primary_allowed: &'static [&'static str],
    allowed: &[(&'static str, &'static [&'static str])],
) -> Result<QueryFieldPlan, JsonRpcError> {
    let Some(fields) = fields else {
        return Ok(QueryFieldPlan {
            primary: primary_allowed.to_vec(),
            blocks: None,
            transactions: None,
        });
    };

    for key in fields.keys() {
        if !allowed.iter().any(|(allowed_key, _)| key == allowed_key) {
            return Err(JsonRpcError::invalid_params());
        }
    }

    Ok(QueryFieldPlan {
        primary: selected_fields(fields.get(primary_key), primary_allowed)?,
        blocks: match fields.get("blocks") {
            Some(selector) => Some(selected_fields(Some(selector), BLOCK_FIELDS)?),
            None => None,
        },
        transactions: match fields.get("transactions") {
            Some(selector) => Some(selected_fields(Some(selector), TX_FIELDS)?),
            None => None,
        },
    })
}

fn selected_fields(
    selector: Option<&FieldSelector>,
    allowed: &'static [&'static str],
) -> Result<Vec<&'static str>, JsonRpcError> {
    match selector {
        None => Ok(allowed.to_vec()),
        Some(FieldSelector::All(true)) => Ok(allowed.to_vec()),
        Some(FieldSelector::All(false)) => Err(JsonRpcError::invalid_params()),
        Some(FieldSelector::List(fields)) => fields
            .iter()
            .map(|field| {
                allowed
                    .iter()
                    .copied()
                    .find(|allowed| field == allowed)
                    .ok_or_else(JsonRpcError::invalid_params)
            })
            .collect(),
    }
}

fn validate_empty_filter(filter: Option<&Value>) -> Result<(), JsonRpcError> {
    match filter {
        None => Ok(()),
        Some(Value::Object(map)) if map.is_empty() => Ok(()),
        _ => Err(JsonRpcError::invalid_params()),
    }
}

fn parse_tx_filter(filter: Option<&Value>) -> Result<TxFilter, JsonRpcError> {
    let Some(filter) = filter else {
        return Ok(TxFilter::default());
    };
    let object = as_object(filter)?;
    let mut out = TxFilter::default();
    for (key, value) in object {
        match key.as_str() {
            "from" => out.from = Some(parse_address_set(value)?),
            "to" => out.to = Some(parse_address_set(value)?),
            "selector" => out.selector = Some(parse_selector_set(value)?),
            _ => return Err(JsonRpcError::invalid_params()),
        }
    }
    Ok(out)
}

fn parse_log_filter(filter: Option<&Value>) -> Result<LogFilter, JsonRpcError> {
    let Some(filter) = filter else {
        return Ok(LogFilter::default());
    };
    let object = as_object(filter)?;
    let mut out = LogFilter::default();
    for (key, value) in object {
        match key.as_str() {
            "address" => out.address = Some(parse_address_set(value)?),
            "topics" => out.topics = parse_topic_filter(value)?,
            _ => return Err(JsonRpcError::invalid_params()),
        }
    }
    Ok(out)
}

fn parse_trace_filter(filter: Option<&Value>) -> Result<TraceFilter, JsonRpcError> {
    let Some(filter) = filter else {
        return Ok(TraceFilter::default());
    };
    let object = as_object(filter)?;
    let mut out = TraceFilter::default();
    for (key, value) in object {
        match key.as_str() {
            "from" => out.from = Some(parse_address_set(value)?),
            "to" => out.to = Some(parse_address_set(value)?),
            "selector" => out.selector = Some(parse_selector_set(value)?),
            "isTopLevel" => out.is_top_level = Some(parse_bool(value)?),
            _ => return Err(JsonRpcError::invalid_params()),
        }
    }
    Ok(out)
}

fn parse_transfer_filter(filter: Option<&Value>) -> Result<TransferFilter, JsonRpcError> {
    let Some(filter) = filter else {
        return Ok(TransferFilter::default());
    };
    let object = as_object(filter)?;
    let mut out = TransferFilter::default();
    for (key, value) in object {
        match key.as_str() {
            "from" => out.from = Some(parse_address_set(value)?),
            "to" => out.to = Some(parse_address_set(value)?),
            "isTopLevel" => out.is_top_level = Some(parse_bool(value)?),
            _ => return Err(JsonRpcError::invalid_params()),
        }
    }
    Ok(out)
}

fn as_object(value: &Value) -> Result<&Map<String, Value>, JsonRpcError> {
    value.as_object().ok_or_else(JsonRpcError::invalid_params)
}

fn parse_bool(value: &Value) -> Result<bool, JsonRpcError> {
    value.as_bool().ok_or_else(JsonRpcError::invalid_params)
}

fn parse_address_set(value: &Value) -> Result<HashSet<Address>, JsonRpcError> {
    parse_string_or_array(value, parse_address)
}

fn parse_selector_set(value: &Value) -> Result<HashSet<[u8; 4]>, JsonRpcError> {
    parse_string_or_array(value, parse_selector)
}

fn parse_topic_filter(value: &Value) -> Result<[Option<HashSet<B256>>; 4], JsonRpcError> {
    let values = value.as_array().ok_or_else(JsonRpcError::invalid_params)?;
    if values.len() > 4 {
        return Err(JsonRpcError::invalid_params());
    }
    let mut out = std::array::from_fn(|_| None);
    for (idx, value) in values.iter().enumerate() {
        if value.is_null() {
            continue;
        }
        out[idx] = Some(parse_string_or_array(value, parse_b256)?);
    }
    Ok(out)
}

fn parse_string_or_array<T, F>(value: &Value, parse: F) -> Result<HashSet<T>, JsonRpcError>
where
    T: Eq + std::hash::Hash,
    F: Fn(&str) -> Result<T, JsonRpcError> + Copy,
{
    match value {
        Value::String(s) => Ok([parse(s)?].into_iter().collect()),
        Value::Array(values) => values
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .ok_or_else(JsonRpcError::invalid_params)
                    .and_then(parse)
            })
            .collect(),
        _ => Err(JsonRpcError::invalid_params()),
    }
}

fn parse_address(value: &str) -> Result<Address, JsonRpcError> {
    let bytes = ethhex::decode_bytes(value).map_err(|_| JsonRpcError::invalid_params())?;
    let bytes: [u8; 20] = bytes
        .try_into()
        .map_err(|_| JsonRpcError::invalid_params())?;
    Ok(Address::from(bytes))
}

fn parse_b256(value: &str) -> Result<B256, JsonRpcError> {
    let bytes = ethhex::decode_bytes(value).map_err(|_| JsonRpcError::invalid_params())?;
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| JsonRpcError::invalid_params())?;
    Ok(B256::from(bytes))
}

fn parse_selector(value: &str) -> Result<[u8; 4], JsonRpcError> {
    ethhex::decode_bytes(value)
        .map_err(|_| JsonRpcError::invalid_params())?
        .try_into()
        .map_err(|_| JsonRpcError::invalid_params())
}

fn project_blocks_response(response: QueryBlocksResponse, block_fields: &[&str]) -> Value {
    let mut data = Map::new();
    data.insert(
        "blocks".to_string(),
        Value::Array(
            response
                .blocks
                .iter()
                .map(|block| project_block(block, block_fields))
                .collect(),
        ),
    );
    response_value(data, response.span)
}

fn project_txs_response(
    response: QueryTransactionsResponse,
    fields: &QueryFieldPlan,
) -> Result<Value, JsonRpcError> {
    let mut data = Map::new();
    data.insert(
        "transactions".to_string(),
        Value::Array(
            response
                .txs
                .iter()
                .map(|tx| project_tx(tx, &fields.primary))
                .collect::<Result<Vec<_>, _>>()?,
        ),
    );
    insert_blocks_relation(
        &mut data,
        response.blocks.as_deref(),
        fields.blocks.as_deref(),
    );
    Ok(response_value(data, response.span))
}

fn project_logs_response(
    response: QueryLogsResponse,
    fields: &QueryFieldPlan,
) -> Result<Value, JsonRpcError> {
    let mut data = Map::new();
    data.insert(
        "logs".to_string(),
        Value::Array(
            response
                .logs
                .iter()
                .map(|log| project_log(log, &fields.primary))
                .collect(),
        ),
    );
    insert_blocks_relation(
        &mut data,
        response.blocks.as_deref(),
        fields.blocks.as_deref(),
    );
    insert_txs_relation(
        &mut data,
        response.transactions.as_deref(),
        fields.transactions.as_deref(),
    )?;
    Ok(response_value(data, response.span))
}

fn project_traces_response(
    response: QueryTracesResponse,
    fields: &QueryFieldPlan,
) -> Result<Value, JsonRpcError> {
    let mut data = Map::new();
    data.insert(
        "traces".to_string(),
        Value::Array(
            response
                .traces
                .iter()
                .map(|trace| project_trace(trace, &fields.primary))
                .collect(),
        ),
    );
    insert_blocks_relation(
        &mut data,
        response.blocks.as_deref(),
        fields.blocks.as_deref(),
    );
    insert_txs_relation(
        &mut data,
        response.transactions.as_deref(),
        fields.transactions.as_deref(),
    )?;
    Ok(response_value(data, response.span))
}

fn project_transfers_response(
    response: QueryTransfersResponse,
    fields: &QueryFieldPlan,
) -> Result<Value, JsonRpcError> {
    let mut data = Map::new();
    data.insert(
        "transfers".to_string(),
        Value::Array(
            response
                .transfers
                .iter()
                .map(|transfer| project_transfer(transfer, &fields.primary))
                .collect(),
        ),
    );
    insert_blocks_relation(
        &mut data,
        response.blocks.as_deref(),
        fields.blocks.as_deref(),
    );
    insert_txs_relation(
        &mut data,
        response.transactions.as_deref(),
        fields.transactions.as_deref(),
    )?;
    Ok(response_value(data, response.span))
}

fn insert_blocks_relation(
    data: &mut Map<String, Value>,
    blocks: Option<&[Block]>,
    fields: Option<&[&str]>,
) {
    let Some(fields) = fields else {
        return;
    };
    data.insert(
        "blocks".to_string(),
        Value::Array(
            blocks
                .unwrap_or_default()
                .iter()
                .map(|block| project_block(block, fields))
                .collect(),
        ),
    );
}

fn insert_txs_relation(
    data: &mut Map<String, Value>,
    txs: Option<&[TxEntry]>,
    fields: Option<&[&str]>,
) -> Result<(), JsonRpcError> {
    let Some(fields) = fields else {
        return Ok(());
    };
    data.insert(
        "transactions".to_string(),
        Value::Array(
            txs.unwrap_or_default()
                .iter()
                .map(|tx| project_tx(tx, fields))
                .collect::<Result<Vec<_>, _>>()?,
        ),
    );
    Ok(())
}

fn response_value(data: Map<String, Value>, span: BlockSpan) -> Value {
    let mut out = Map::new();
    out.insert("data".to_string(), Value::Object(data));
    out.insert("fromBlock".to_string(), project_block_ref(span.from_block));
    out.insert("toBlock".to_string(), project_block_ref(span.to_block));
    out.insert(
        "cursorBlock".to_string(),
        project_block_ref(span.cursor_block),
    );
    Value::Object(out)
}

fn project_block_ref(block: BlockRef) -> Value {
    object([
        ("number", q_u64(block.number)),
        ("hash", fixed(block.hash)),
        ("parentHash", fixed(block.parent_hash)),
    ])
}

fn project_block(block: &Block, fields: &[&str]) -> Value {
    let header = &block.header;
    let mut out = Map::new();
    for field in fields {
        let value = match *field {
            "number" => q_u64(header.number),
            "hash" => fixed(block.hash),
            "parentHash" => fixed(header.parent_hash),
            "timestamp" => q_u64(header.timestamp),
            "miner" => fixed(header.beneficiary),
            "gasLimit" => q_u64(header.gas_limit),
            "gasUsed" => q_u64(header.gas_used),
            "baseFeePerGas" => optional_q_u64(header.base_fee_per_gas),
            "stateRoot" => fixed(header.state_root),
            "transactionsRoot" => fixed(header.transactions_root),
            "receiptsRoot" => fixed(header.receipts_root),
            "logsBloom" => fixed(header.logs_bloom),
            "extraData" => bytes(&header.extra_data),
            "nonce" => fixed(header.nonce),
            "mixHash" => fixed(header.mix_hash),
            "difficulty" => q_u256(header.difficulty),
            _ => continue,
        };
        out.insert((*field).to_string(), value);
    }
    Value::Object(out)
}

fn project_tx(tx: &TxEntry, fields: &[&str]) -> Result<Value, JsonRpcError> {
    let envelope = tx.envelope().map_err(chain_data_error_to_jsonrpc)?;
    let mut out = Map::new();
    for field in fields {
        let value = match *field {
            "blockNumber" => q_u64(tx.block_number),
            "blockHash" => fixed(tx.block_hash),
            "transactionIndex" => q_u64(u64::from(tx.tx_idx)),
            "hash" => fixed(tx.tx_hash),
            "from" => fixed(tx.sender),
            "to" => envelope.to().map(fixed).unwrap_or(Value::Null),
            "nonce" => q_u64(envelope.nonce()),
            "value" => q_u256(envelope.value()),
            "gas" => q_u64(envelope.gas_limit()),
            "gasPrice" => envelope.gas_price().map(q_u128).unwrap_or(Value::Null),
            "maxFeePerGas" => q_u128(envelope.max_fee_per_gas()),
            "maxPriorityFeePerGas" => envelope
                .max_priority_fee_per_gas()
                .map(q_u128)
                .unwrap_or(Value::Null),
            "input" => bytes(envelope.input()),
            "type" => q_u64(u64::from(envelope.ty())),
            "chainId" => envelope.chain_id().map(q_u64).unwrap_or(Value::Null),
            _ => continue,
        };
        out.insert((*field).to_string(), value);
    }
    Ok(Value::Object(out))
}

fn project_log(log: &LogEntry, fields: &[&str]) -> Value {
    let mut out = Map::new();
    for field in fields {
        let value = match *field {
            "blockNumber" => q_u64(log.block_number),
            "blockHash" => fixed(log.block_hash),
            "transactionIndex" => q_u64(u64::from(log.tx_index)),
            "logIndex" => q_u64(u64::from(log.log_index)),
            "address" => fixed(log.address),
            "topics" => Value::Array(log.topics.iter().copied().map(fixed).collect()),
            "data" => bytes(&log.data),
            _ => continue,
        };
        out.insert((*field).to_string(), value);
    }
    Value::Object(out)
}

fn project_trace(trace: &TraceEntry, fields: &[&str]) -> Value {
    let mut out = Map::new();
    for field in fields {
        let value = match *field {
            "blockNumber" => q_u64(trace.block_number),
            "blockHash" => fixed(trace.block_hash),
            "transactionIndex" => q_u64(u64::from(trace.tx_index)),
            "traceAddress" => project_trace_address(&trace.trace_address),
            "type" => Value::String(call_kind_str(trace.typ).to_string()),
            "from" => fixed(trace.from),
            "to" => trace.to.map(fixed).unwrap_or(Value::Null),
            "value" => q_u256(trace.value),
            "gas" => q_u64(trace.gas),
            "gasUsed" => q_u64(trace.gas_used),
            "input" => bytes(&trace.input),
            "output" => bytes(&trace.output),
            "status" => q_u64(u64::from(trace.status)),
            "depth" => q_u64(u64::from(trace.depth)),
            _ => continue,
        };
        out.insert((*field).to_string(), value);
    }
    Value::Object(out)
}

fn project_transfer(transfer: &TransferEntry, fields: &[&str]) -> Value {
    let mut out = Map::new();
    for field in fields {
        let value = match *field {
            "blockNumber" => q_u64(transfer.block_number),
            "blockHash" => fixed(transfer.block_hash),
            "transactionIndex" => q_u64(u64::from(transfer.tx_index)),
            "traceAddress" => project_trace_address(&transfer.trace_address),
            "type" => Value::String(call_kind_str(transfer.typ).to_string()),
            "from" => fixed(transfer.from),
            "to" => fixed(transfer.to),
            "value" => q_u256(transfer.value),
            _ => continue,
        };
        out.insert((*field).to_string(), value);
    }
    Value::Object(out)
}

fn project_trace_address(trace_address: &[u32]) -> Value {
    Value::Array(
        trace_address
            .iter()
            .map(|index| q_u64(u64::from(*index)))
            .collect(),
    )
}

fn call_kind_str(kind: CallKind) -> &'static str {
    match kind {
        CallKind::Call => "CALL",
        CallKind::DelegateCall => "DELEGATECALL",
        CallKind::CallCode => "CALLCODE",
        CallKind::Create => "CREATE",
        CallKind::Create2 => "CREATE2",
        CallKind::SelfDestruct => "SELFDESTRUCT",
        CallKind::StaticCall => "STATICCALL",
    }
}

fn object<const N: usize>(fields: [(&str, Value); N]) -> Value {
    Value::Object(
        fields
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect(),
    )
}

fn q_u64(value: u64) -> Value {
    Value::String(format!("0x{value:x}"))
}

fn q_u128(value: u128) -> Value {
    Value::String(format!("0x{value:x}"))
}

fn q_u256(value: U256) -> Value {
    Value::String(format!("0x{value:x}"))
}

fn optional_q_u64(value: Option<u64>) -> Value {
    value.map(q_u64).unwrap_or(Value::Null)
}

fn fixed<T: std::fmt::LowerHex>(value: T) -> Value {
    Value::String(format!("{value:#x}"))
}

fn bytes(value: &[u8]) -> Value {
    Value::String(ethhex::encode_bytes(value))
}

fn serialize_queryx_result(
    value: Value,
    app_state: &MonadRpcResources,
) -> Result<Box<RawValue>, JsonRpcError> {
    let raw = serialize_result(value)?;
    let response_size_bytes = raw.get().len();
    debug!(response_size_bytes, "queryX result projection size");
    if response_size_bytes > app_state.max_response_size as usize {
        return Err(JsonRpcError::max_size_exceeded());
    }
    Ok(raw)
}

fn chain_data_error_to_jsonrpc(error: MonadChainDataError) -> JsonRpcError {
    match error {
        MonadChainDataError::InvalidRequest(_) | MonadChainDataError::Decode(_) => {
            JsonRpcError::invalid_params()
        }
        MonadChainDataError::LimitExceeded {
            max_limit,
            max_block_range,
            ..
        } => JsonRpcError {
            code: -32005,
            message: "Limit exceeded".to_string(),
            data: Some(serde_json::json!({
                "maxLimit": q_u64(max_limit as u64),
                "maxBlockRange": q_u64(max_block_range),
            })),
        },
        MonadChainDataError::NotImplemented(message) => {
            JsonRpcError::custom(format!("not implemented: {message}"))
        }
        MonadChainDataError::Backend(message) => {
            JsonRpcError::internal_error(format!("chain-data backend error: {message}"))
        }
        MonadChainDataError::MissingData(message) => {
            JsonRpcError::internal_error(format!("chain-data missing data: {message}"))
        }
        MonadChainDataError::SealedDirectoryBucketMissingSummary { bucket_start } => {
            JsonRpcError::internal_error(format!(
                "chain-data sealed directory bucket missing summary: bucket_start={bucket_start}"
            ))
        }
        MonadChainDataError::SealedShardPageMissingArtifact { .. } => JsonRpcError::internal_error(
            "chain-data sealed shard page missing artifact".to_string(),
        ),
        MonadChainDataError::FencedOut { .. } => {
            JsonRpcError::internal_error("chain-data writer fenced out during query".to_string())
        }
        // Write-authority / lease errors belong to the ingest/write path and
        // should never surface on the read-only query path; treat as internal.
        MonadChainDataError::LeaseLost
        | MonadChainDataError::LeaseStillFresh
        | MonadChainDataError::LeaseObservationUnavailable
        | MonadChainDataError::ReadOnlyMode(_) => JsonRpcError::internal_error(
            "chain-data write-authority error during query".to_string(),
        ),
    }
}

/// Serves `eth_getLogs` from the chain-data index, extending it past the
/// finalized head with the unfinalized tip when a `data_provider` is supplied.
///
/// chain-data only indexes finalized blocks (up to the published head), so
/// the requested range is split at that head: `[from ..= head]` is answered
/// by paging `query_logs`, and `(head ..= to]` is answered by fetching each
/// tip block's receipts from `data_provider` (exec-events buffer → triedb →
/// archive) and running the *same* [`LogFilter`] over them via
/// [`filter_unfinalized_block_logs`]. When no `data_provider` is available the
/// tip is omitted and the behavior matches the finalized-only path.
///
/// Block tags resolve against the true latest block (the unfinalized tip
/// when `data_provider` is present, else the head): `latest`/`safe`/`finalized`
/// /`pending` and an omitted bound map to it, `earliest` maps to 0. The
/// finalized pager requests the `blocks` and `transactions` relations so each
/// log carries `blockTimestamp` and `transactionHash`; the tip path
/// reconstructs the same fields from the fetched header and transactions.
/// `max_response_size` is enforced across the combined result.
pub async fn get_logs_via_chain_data<T: Triedb>(
    service: &ChainDataService,
    data_provider: Option<&DataProvider<T>>,
    filter: Filter,
    max_response_size: u32,
    max_block_range: u64,
) -> JsonRpcResult<Vec<MonadLog>> {
    let head = service
        .load_published_head()
        .await
        .map_err(chain_data_error_to_jsonrpc)?;

    // The true upper bound of queryable blocks: the unfinalized tip when a
    // data_provider is available, otherwise the finalized head. With neither a
    // live data_provider nor any finalized block indexed yet, there is nothing
    // to serve.
    let latest = match data_provider {
        Some(data_provider) => data_provider.get_latest_block_number(),
        None => match head {
            Some(head) => head,
            None => return Ok(Vec::new()),
        },
    };

    let (from_block, to_block) = match filter.block_option {
        FilterBlockOption::Range {
            from_block,
            to_block,
        } => (
            resolve_eth_block_bound(from_block, head, latest),
            resolve_eth_block_bound(to_block, head, latest).min(latest),
        ),
        FilterBlockOption::AtBlockHash(block_hash) => {
            // chain-data covers finalized blocks; fall back to data_provider
            // for a hash that targets an unfinalized block.
            let number = match service
                .block_number_by_hash(&block_hash)
                .await
                .map_err(chain_data_error_to_jsonrpc)?
            {
                Some(number) => Some(number),
                None => match data_provider {
                    Some(data_provider) => {
                        data_provider
                            .resolve_block_number_by_hash(block_hash)
                            .await?
                    }
                    None => None,
                },
            };

            match number {
                Some(number) => (number, number),
                None => return Ok(Vec::new()),
            }
        }
    };

    if from_block > to_block {
        return Err(FilterError::InvalidBlockRange.into());
    }
    if to_block - from_block > max_block_range {
        return Err(FilterError::RangeTooLarge.into());
    }

    let log_filter = LogFilter {
        address: filter_set_to_opt(filter.address.iter().copied()),
        topics: std::array::from_fn(|i| filter_set_to_opt(filter.topics[i].iter().copied())),
    };

    let mut logs = Vec::new();
    let mut heuristic_response_size = 0u64;

    // Finalized portion: [from_block ..= min(to_block, head)].
    if let Some(head) = head {
        let finalized_to = to_block.min(head);
        if from_block <= finalized_to {
            query_finalized_logs(
                service,
                &log_filter,
                from_block,
                finalized_to,
                max_response_size,
                &mut heuristic_response_size,
                &mut logs,
            )
            .await?;
        }
    }

    // Unfinalized tip: (head ..= to_block], served from data_provider.
    if let Some(data_provider) = data_provider {
        let tip_from = match head {
            Some(head) => from_block.max(head + 1),
            None => from_block,
        };
        if tip_from <= to_block {
            append_unfinalized_logs(
                data_provider,
                &filter,
                tip_from,
                to_block,
                max_response_size,
                &mut heuristic_response_size,
                &mut logs,
            )
            .await?;
        }
    }

    Ok(logs)
}

/// Pages the finalized chain-data index over `[from_block ..= to_block]`,
/// appending projected logs to `logs` and accounting their size against
/// `max_response_size`. Because `query_logs` returns at most `limit` logs per
/// page (completing the current block), this advances a cursor until the
/// range is exhausted.
async fn query_finalized_logs(
    service: &ChainDataService,
    log_filter: &LogFilter,
    from_block: u64,
    to_block: u64,
    max_response_size: u32,
    heuristic_response_size: &mut u64,
    logs: &mut Vec<MonadLog>,
) -> JsonRpcResult<()> {
    let page_limit = service.limits().max_limit.max(1);
    let mut cursor = from_block;

    loop {
        let response = service
            .query_logs(QueryLogsRequest {
                envelope: QueryEnvelope {
                    from_block: Some(cursor),
                    to_block: Some(to_block),
                    order: QueryOrder::Ascending,
                    limit: page_limit,
                },
                filter: log_filter.clone(),
                relations: LogsRelations {
                    blocks: true,
                    transactions: true,
                },
            })
            .await
            .map_err(chain_data_error_to_jsonrpc)?;

        let timestamp_by_block: HashMap<u64, u64> = response
            .blocks
            .iter()
            .flatten()
            .map(|block| (block.header.number, block.header.timestamp))
            .collect();
        let tx_hash_by_location: HashMap<(u64, u32), Hash32> = response
            .transactions
            .iter()
            .flatten()
            .map(|tx| ((tx.block_number, tx.tx_idx), tx.tx_hash))
            .collect();

        for entry in &response.logs {
            let log = log_entry_to_rpc_log(entry, &timestamp_by_block, &tx_hash_by_location);
            *heuristic_response_size +=
                serde_json::to_vec(&log).map_or(0, |bytes| bytes.len()) as u64;
            if *heuristic_response_size > max_response_size as u64 {
                return Err(JsonRpcError::max_size_exceeded());
            }
            logs.push(MonadLog(log));
        }

        let scanned = response.span.cursor_block.number;
        // Empty page => the whole remaining window was scanned with no
        // matches (the limit bounds matched logs, not blocks scanned), so
        // we are done. `scanned >= to_block` means the range is exhausted.
        // `scanned < cursor` should never happen but guards against a
        // non-advancing cursor.
        if response.logs.is_empty() || scanned >= to_block || scanned < cursor {
            break;
        }
        cursor = scanned + 1;
    }

    Ok(())
}

/// Serves the unfinalized tip of an `eth_getLogs` range from `data_provider`,
/// appending matching logs to `logs`. Each block's receipts are fetched (and
/// bloom pre-filtered) via [`DataProvider::fetch_blocks_for_logs`], its logs are
/// matched against `log_filter` by the shared chain-data
/// [`filter_unfinalized_block_logs`], and the resulting entries are projected
/// to RPC logs using the block header's timestamp and the transactions' hashes
/// — the same shape as the finalized pager.
async fn append_unfinalized_logs<T: Triedb>(
    data_provider: &DataProvider<T>,
    filter: &Filter,
    from_block: u64,
    to_block: u64,
    max_response_size: u32,
    heuristic_response_size: &mut u64,
    logs: &mut Vec<MonadLog>,
) -> JsonRpcResult<()> {
    let log_filter = LogFilter {
        address: filter_set_to_opt(filter.address.iter().copied()),
        topics: std::array::from_fn(|i| filter_set_to_opt(filter.topics[i].iter().copied())),
    };

    let blocks = data_provider
        .fetch_blocks_for_logs(from_block, to_block, filter)
        .await?;

    for (header, transactions, receipts) in blocks {
        if receipts.is_empty() {
            continue;
        }

        let logs_by_tx: Vec<Vec<PrimitiveLog>> = receipts
            .iter()
            .map(|receipt| receipt.receipt.logs().to_vec())
            .collect();

        let entries = scan_block_logs(
            &MemLogsBlock {
                block_number: header.header.number,
                block_hash: header.hash,
                logs_by_tx: &logs_by_tx,
            },
            &log_filter,
        );
        if entries.is_empty() {
            continue;
        }

        let timestamp_by_block = HashMap::from([(header.header.number, header.header.timestamp)]);
        let tx_hash_by_location: HashMap<(u64, u32), Hash32> = transactions
            .iter()
            .enumerate()
            .map(|(tx_index, tx)| ((header.header.number, tx_index as u32), *tx.tx.tx_hash()))
            .collect();

        for entry in &entries {
            let log = log_entry_to_rpc_log(entry, &timestamp_by_block, &tx_hash_by_location);
            *heuristic_response_size +=
                serde_json::to_vec(&log).map_or(0, |bytes| bytes.len()) as u64;
            if *heuristic_response_size > max_response_size as u64 {
                return Err(JsonRpcError::max_size_exceeded());
            }
            logs.push(MonadLog(log));
        }
    }

    Ok(())
}

fn resolve_eth_block_bound(tag: Option<BlockNumberOrTag>, head: Option<u64>, latest: u64) -> u64 {
    match tag {
        // `latest`/`pending`/omitted track the unfinalized tip; the caller
        // splits the range at the finalized head, so the tip is served from
        // data_provider and the rest from the chain-data index.
        None | Some(BlockNumberOrTag::Latest) | Some(BlockNumberOrTag::Pending) => latest,
        Some(BlockNumberOrTag::Number(number)) => number,
        Some(BlockNumberOrTag::Earliest) => 0,
        // `finalized`/`safe` are bounded by the published (finalized) head,
        // never the unfinalized tip.
        Some(BlockNumberOrTag::Finalized) | Some(BlockNumberOrTag::Safe) => head.unwrap_or(0),
    }
}

// ---------------------------------------------------------------------------
// queryX (`eth_queryLogs` / `eth_queryTransactions`) tip merge
//
// The native queryX log/tx paths are extended past the finalized head the
// same way `get_logs_via_chain_data` extends the eth-compat path: the
// resolved range is split at the published head, the `[lo..=head]` part is
// served from the index, and the `(head..=hi]` part is scanned from the
// buffer→triedb→archive cascade ([`DataProvider::fetch_block_for_logs`]) and
// matched with the shared [`scan_block_logs`]/[`scan_block_txs`] so finalized
// and unfinalized rows are identical in shape. The merge honors
// `order`/`limit` (completing the current block at the limit) and records the
// last block scanned in `cursorBlock`. With no `data_provider` (e.g.
// `--queryx-only`) the tip is omitted and behavior is finalized-only.
// ---------------------------------------------------------------------------

/// One unfinalized block materialized from the cascade for in-memory
/// scanning. `header` is the alloy header ([`EvmBlockHeader`]); `logs_by_tx`
/// is the per-transaction log grouping and `txs` the per-transaction
/// envelopes, matching [`MemLogsBlock`]/[`MemTx`].
struct UnfinalizedBlock {
    hash: Hash32,
    header: EvmBlockHeader,
    logs_by_tx: Vec<Vec<PrimitiveLog>>,
    txs: Vec<MemTx>,
}

/// Materializes one unfinalized block from `data_provider` via the
/// buffer→triedb→archive cascade. Passes `|_| true` (no bloom pre-filter) so
/// the block is available for both log and transaction scans, which then
/// filter exactly in memory. Returns `None` when the block is absent from
/// every source.
async fn load_unfinalized_block<T: Triedb>(
    data_provider: &DataProvider<T>,
    height: u64,
) -> JsonRpcResult<Option<UnfinalizedBlock>> {
    let Some((header, txs, receipts)) =
        data_provider.fetch_block_for_logs(height, |_| true).await?
    else {
        return Ok(None);
    };

    let logs_by_tx = receipts
        .iter()
        .map(|receipt| receipt.receipt.logs().to_vec())
        .collect();
    let txs = txs
        .iter()
        .map(|tx| MemTx {
            tx_hash: *tx.tx.tx_hash(),
            sender: tx.sender,
            signed_tx_bytes: Bytes::from(tx.tx.encoded_2718()),
        })
        .collect();

    Ok(Some(UnfinalizedBlock {
        hash: header.hash,
        header: header.header,
        logs_by_tx,
        txs,
    }))
}

/// Error for a block inside the requested window that no source can
/// materialize. Loud by design (see the call sites): the alternative is a
/// silent hole in the result with the cursor advanced past unread data.
fn unavailable_block_error(height: u64) -> JsonRpcError {
    JsonRpcError::internal_error(format!(
        "block {height} is within the requested range but unavailable from any source \
         (index not yet caught up and block absent from buffer/triedb/archive)"
    ))
}

/// Resolved, order-normalized bounds for a merged finalized + unfinalized
/// query. `from`/`to` are the resolved request bounds (order-dependent roles,
/// used for the response `fromBlock`/`toBlock`); `lo`/`hi` are the inclusive
/// scan range with `lo <= hi`.
struct MergeBounds {
    order: QueryOrder,
    limit: usize,
    from: u64,
    to: u64,
    lo: u64,
    hi: u64,
    head_final: Option<u64>,
}

/// Resolves order/limit/bounds and the finalized vs latest heads. Returns
/// `None` when there is no data anywhere (no finalized head and no live
/// data_provider), so callers can surface "no published blocks".
async fn resolve_merge_bounds<T: Triedb>(
    service: &ChainDataService,
    data_provider: Option<&DataProvider<T>>,
    raw: &RawQueryRequest,
) -> JsonRpcResult<Option<MergeBounds>> {
    let order = match raw.order.as_deref().unwrap_or("asc") {
        "asc" => QueryOrder::Ascending,
        "desc" => QueryOrder::Descending,
        _ => return Err(JsonRpcError::invalid_params()),
    };
    let limit = parse_limit(raw.limit.as_deref())?;
    service
        .limits()
        .check_limit(limit)
        .map_err(chain_data_error_to_jsonrpc)?;

    let head_final = service
        .load_published_head()
        .await
        .map_err(chain_data_error_to_jsonrpc)?;
    // `latest` for a chain-data RPC means the unfinalized tip when a live
    // data_provider is present, otherwise the finalized head.
    let head_latest = match data_provider {
        Some(data_provider) => Some(data_provider.get_latest_block_number()),
        None => head_final,
    };
    let Some(head_latest) = head_latest else {
        return Ok(None);
    };

    let from = resolve_queryx_bound(
        raw.from_block.as_deref(),
        head_final,
        head_latest,
        order,
        true,
    )?;
    let to = resolve_queryx_bound(
        raw.to_block.as_deref(),
        head_final,
        head_latest,
        order,
        false,
    )?;
    let (lo, hi) = match order {
        QueryOrder::Ascending => (from, to),
        QueryOrder::Descending => (to, from),
    };
    if lo > hi {
        return Err(chain_data_error_to_jsonrpc(
            MonadChainDataError::InvalidRequest(
                "from_block and to_block do not form a valid range for the requested order",
            ),
        ));
    }
    // chain-data clamps the window span to `max_block_range` inside
    // `query_logs`, but that only covers the finalized sub-range we hand it;
    // the unfinalized tail is scanned here block-by-block via the cascade. Re-
    // apply the same cap to the whole merged window so a query reaching far
    // past a lagging index can't turn into an unbounded sequential crawl.
    let span = hi - lo + 1;
    if span > service.limits().max_block_range {
        return Err(chain_data_error_to_jsonrpc(
            MonadChainDataError::LimitExceeded {
                kind: LimitExceededKind::BlockRange,
                max_limit: service.limits().max_limit,
                max_block_range: service.limits().max_block_range,
            },
        ));
    }

    Ok(Some(MergeBounds {
        order,
        limit,
        from,
        to,
        lo,
        hi,
        head_final,
    }))
}

fn resolve_queryx_bound(
    value: Option<&str>,
    head_final: Option<u64>,
    head_latest: u64,
    order: QueryOrder,
    is_from: bool,
) -> JsonRpcResult<u64> {
    match value {
        None => Ok(match (order, is_from) {
            // fromBlock default: earliest (asc) / latest (desc).
            (QueryOrder::Ascending, true) => 0,
            (QueryOrder::Descending, true) => head_latest,
            // toBlock default: latest (asc) / earliest (desc).
            (QueryOrder::Ascending, false) => head_latest,
            (QueryOrder::Descending, false) => 0,
        }),
        Some("earliest") => Ok(0),
        Some("latest") | Some("pending") => Ok(head_latest),
        Some("finalized") | Some("safe") => Ok(head_final.unwrap_or(0)),
        Some(hex) => ethhex::decode_quantity(hex).map_err(|_| JsonRpcError::invalid_params()),
    }
}

/// Resolves a block number to a [`BlockRef`] (number + hash + parent hash)
/// for the response span: from the index for finalized blocks, otherwise from
/// the cascade. Falls back to a zeroed ref if the block can't be resolved.
async fn resolve_block_ref<T: Triedb>(
    service: &ChainDataService,
    data_provider: Option<&DataProvider<T>>,
    head_final: Option<u64>,
    number: u64,
) -> BlockRef {
    if head_final.is_some_and(|head| number <= head) {
        if let Ok(Some(record)) = service.load_block_record(number).await {
            return BlockRef {
                number,
                hash: record.block_hash,
                parent_hash: record.parent_hash,
            };
        }
    }
    if let Some(data_provider) = data_provider {
        if let Ok(Some(block)) = load_unfinalized_block(data_provider, number).await {
            return BlockRef {
                number,
                hash: block.hash,
                parent_hash: block.header.parent_hash,
            };
        }
    }
    BlockRef {
        number,
        hash: B256::ZERO,
        parent_hash: B256::ZERO,
    }
}

async fn build_merge_span<T: Triedb>(
    service: &ChainDataService,
    data_provider: Option<&DataProvider<T>>,
    bounds: &MergeBounds,
    cursor: Option<u64>,
) -> BlockSpan {
    let cursor_number = cursor.unwrap_or(bounds.from);
    BlockSpan {
        from_block: resolve_block_ref(service, data_provider, bounds.head_final, bounds.from).await,
        to_block: resolve_block_ref(service, data_provider, bounds.head_final, bounds.to).await,
        cursor_block: resolve_block_ref(service, data_provider, bounds.head_final, cursor_number)
            .await,
    }
}

fn dedup_sort_blocks(mut blocks: Vec<Block>) -> Vec<Block> {
    blocks.sort_by_key(|block| block.header.number);
    blocks.dedup_by_key(|block| block.header.number);
    blocks
}

fn dedup_sort_txs(mut txs: Vec<TxEntry>) -> Vec<TxEntry> {
    txs.sort_by_key(|tx| (tx.block_number, tx.tx_idx));
    txs.dedup_by_key(|tx| (tx.block_number, tx.tx_idx));
    txs
}

fn unfinalized_tx_entry(
    block: &UnfinalizedBlock,
    block_number: u64,
    tx_index: u32,
) -> Option<TxEntry> {
    block.txs.get(tx_index as usize).map(|tx| TxEntry {
        block_number,
        block_hash: block.hash,
        tx_idx: tx_index,
        tx_hash: tx.tx_hash,
        sender: tx.sender,
        signed_tx_bytes: tx.signed_tx_bytes.clone(),
    })
}

async fn query_finalized_logs_page(
    service: &ChainDataService,
    filter: &LogFilter,
    relations: LogsRelations,
    from: u64,
    to: u64,
    order: QueryOrder,
    limit: usize,
) -> JsonRpcResult<QueryLogsResponse> {
    service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(from),
                to_block: Some(to),
                order,
                limit,
            },
            filter: filter.clone(),
            relations,
        })
        .await
        .map_err(chain_data_error_to_jsonrpc)
}

/// Scans the unfinalized tip `[lo..=hi]` (in `order`) from `data_provider`,
/// appending matching logs (and, per `relations`, their blocks/txs) and
/// recording the last block scanned in `cursor`. Returns `true` if `limit`
/// was reached (the current block is always completed first).
#[allow(clippy::too_many_arguments)]
async fn scan_unfinalized_logs<T: Triedb>(
    data_provider: Option<&DataProvider<T>>,
    lo: u64,
    hi: u64,
    order: QueryOrder,
    limit: usize,
    filter: &LogFilter,
    relations: LogsRelations,
    logs: &mut Vec<LogEntry>,
    blocks: &mut Vec<Block>,
    txs: &mut Vec<TxEntry>,
    cursor: &mut Option<u64>,
) -> JsonRpcResult<bool> {
    let Some(data_provider) = data_provider else {
        return Ok(false);
    };
    if lo > hi {
        return Ok(false);
    }

    let heights: Box<dyn Iterator<Item = u64>> = match order {
        QueryOrder::Ascending => Box::new(lo..=hi),
        QueryOrder::Descending => Box::new((lo..=hi).rev()),
    };

    for height in heights {
        // Every height in `[lo, hi]` here sits at or below the proposed tip,
        // so it must be materializable from some source. A `None` means the
        // block is genuinely unavailable on this node (e.g. an unindexed
        // finalized block the index hasn't caught up to, pruned from triedb,
        // with no archive). Surfacing it loudly is correct: silently skipping
        // would drop that block's matches and advance the cursor past it,
        // reporting a hole as "no logs here".
        let block = load_unfinalized_block(data_provider, height)
            .await?
            .ok_or_else(|| unavailable_block_error(height))?;
        *cursor = Some(height);
        let mut block_logs = scan_block_logs(
            &MemLogsBlock {
                block_number: height,
                block_hash: block.hash,
                logs_by_tx: &block.logs_by_tx,
            },
            filter,
        );
        if order == QueryOrder::Descending {
            block_logs.reverse();
        }
        if !block_logs.is_empty() {
            if relations.blocks {
                blocks.push(Block {
                    hash: block.hash,
                    header: block.header.clone(),
                });
            }
            if relations.transactions {
                let mut seen = HashSet::new();
                for entry in &block_logs {
                    if seen.insert(entry.tx_index) {
                        if let Some(tx) = unfinalized_tx_entry(&block, height, entry.tx_index) {
                            txs.push(tx);
                        }
                    }
                }
            }
        }
        logs.append(&mut block_logs);
        if logs.len() >= limit {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Executes a logs query over `[from, to]`, serving the finalized portion
/// from the index and the unfinalized tip (when a `data_provider` is present)
/// via the cascade + in-memory scan, merged in `order` and bounded by `limit`
/// (completing the current block at the limit). `cursorBlock` records the
/// last block scanned.
async fn execute_logs_query<T: Triedb>(
    service: &ChainDataService,
    data_provider: Option<&DataProvider<T>>,
    raw: &RawQueryRequest,
    filter: LogFilter,
    relations: LogsRelations,
) -> JsonRpcResult<QueryLogsResponse> {
    let Some(bounds) = resolve_merge_bounds(service, data_provider, raw).await? else {
        return Err(JsonRpcError::internal_error(
            "no published blocks".to_string(),
        ));
    };

    let mut logs: Vec<LogEntry> = Vec::new();
    let mut blocks: Vec<Block> = Vec::new();
    let mut txs: Vec<TxEntry> = Vec::new();
    let mut cursor: Option<u64> = None;

    let finalized_hi = bounds.head_final.map(|head| bounds.hi.min(head));
    let unfinalized_from = match bounds.head_final {
        Some(head) => bounds.lo.max(head + 1),
        None => bounds.lo,
    };

    match bounds.order {
        QueryOrder::Ascending => {
            let mut finalized_done = true;
            if let Some(fin_hi) = finalized_hi {
                if bounds.lo <= fin_hi {
                    let response = query_finalized_logs_page(
                        service,
                        &filter,
                        relations,
                        bounds.lo,
                        fin_hi,
                        bounds.order,
                        bounds.limit,
                    )
                    .await?;
                    cursor = Some(response.span.cursor_block.number);
                    finalized_done = response.span.cursor_block.number >= fin_hi;
                    logs.extend(response.logs);
                    if let Some(b) = response.blocks {
                        blocks.extend(b);
                    }
                    if let Some(t) = response.transactions {
                        txs.extend(t);
                    }
                }
            }
            if finalized_done && logs.len() < bounds.limit {
                scan_unfinalized_logs(
                    data_provider,
                    unfinalized_from,
                    bounds.hi,
                    bounds.order,
                    bounds.limit,
                    &filter,
                    relations,
                    &mut logs,
                    &mut blocks,
                    &mut txs,
                    &mut cursor,
                )
                .await?;
            }
        }
        QueryOrder::Descending => {
            let reached_limit = bounds.hi >= unfinalized_from
                && scan_unfinalized_logs(
                    data_provider,
                    unfinalized_from,
                    bounds.hi,
                    bounds.order,
                    bounds.limit,
                    &filter,
                    relations,
                    &mut logs,
                    &mut blocks,
                    &mut txs,
                    &mut cursor,
                )
                .await?;
            if !reached_limit && logs.len() < bounds.limit {
                if let Some(fin_hi) = finalized_hi {
                    if bounds.lo <= fin_hi {
                        let remaining = bounds.limit.saturating_sub(logs.len()).max(1);
                        let response = query_finalized_logs_page(
                            service,
                            &filter,
                            relations,
                            fin_hi,
                            bounds.lo,
                            bounds.order,
                            remaining,
                        )
                        .await?;
                        cursor = Some(response.span.cursor_block.number);
                        logs.extend(response.logs);
                        if let Some(b) = response.blocks {
                            blocks.extend(b);
                        }
                        if let Some(t) = response.transactions {
                            txs.extend(t);
                        }
                    }
                }
            }
        }
    }

    let span = build_merge_span(service, data_provider, &bounds, cursor).await;
    Ok(QueryLogsResponse {
        logs,
        blocks: relations.blocks.then(|| dedup_sort_blocks(blocks)),
        transactions: relations.transactions.then(|| dedup_sort_txs(txs)),
        span,
    })
}

async fn query_finalized_txs_page(
    service: &ChainDataService,
    filter: &TxFilter,
    relations: TxsRelations,
    from: u64,
    to: u64,
    order: QueryOrder,
    limit: usize,
) -> JsonRpcResult<QueryTransactionsResponse> {
    service
        .query_transactions(QueryTransactionsRequest {
            envelope: QueryEnvelope {
                from_block: Some(from),
                to_block: Some(to),
                order,
                limit,
            },
            filter: filter.clone(),
            relations,
        })
        .await
        .map_err(chain_data_error_to_jsonrpc)
}

/// Transactions analogue of [`scan_unfinalized_logs`].
#[allow(clippy::too_many_arguments)]
async fn scan_unfinalized_txs<T: Triedb>(
    data_provider: Option<&DataProvider<T>>,
    lo: u64,
    hi: u64,
    order: QueryOrder,
    limit: usize,
    filter: &TxFilter,
    relations: TxsRelations,
    txs: &mut Vec<TxEntry>,
    blocks: &mut Vec<Block>,
    cursor: &mut Option<u64>,
) -> JsonRpcResult<bool> {
    let Some(data_provider) = data_provider else {
        return Ok(false);
    };
    if lo > hi {
        return Ok(false);
    }

    let heights: Box<dyn Iterator<Item = u64>> = match order {
        QueryOrder::Ascending => Box::new(lo..=hi),
        QueryOrder::Descending => Box::new((lo..=hi).rev()),
    };

    for height in heights {
        // See `scan_unfinalized_logs`: an unavailable in-range block is a
        // loud error, not a silent skip.
        let block = load_unfinalized_block(data_provider, height)
            .await?
            .ok_or_else(|| unavailable_block_error(height))?;
        *cursor = Some(height);
        let mut block_txs = scan_block_txs(height, block.hash, &block.txs, filter);
        if order == QueryOrder::Descending {
            block_txs.reverse();
        }
        if relations.blocks && !block_txs.is_empty() {
            blocks.push(Block {
                hash: block.hash,
                header: block.header.clone(),
            });
        }
        txs.append(&mut block_txs);
        if txs.len() >= limit {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Transactions analogue of [`execute_logs_query`].
async fn execute_txs_query<T: Triedb>(
    service: &ChainDataService,
    data_provider: Option<&DataProvider<T>>,
    raw: &RawQueryRequest,
    filter: TxFilter,
    relations: TxsRelations,
) -> JsonRpcResult<QueryTransactionsResponse> {
    let Some(bounds) = resolve_merge_bounds(service, data_provider, raw).await? else {
        return Err(JsonRpcError::internal_error(
            "no published blocks".to_string(),
        ));
    };

    let mut txs: Vec<TxEntry> = Vec::new();
    let mut blocks: Vec<Block> = Vec::new();
    let mut cursor: Option<u64> = None;

    let finalized_hi = bounds.head_final.map(|head| bounds.hi.min(head));
    let unfinalized_from = match bounds.head_final {
        Some(head) => bounds.lo.max(head + 1),
        None => bounds.lo,
    };

    match bounds.order {
        QueryOrder::Ascending => {
            let mut finalized_done = true;
            if let Some(fin_hi) = finalized_hi {
                if bounds.lo <= fin_hi {
                    let response = query_finalized_txs_page(
                        service,
                        &filter,
                        relations,
                        bounds.lo,
                        fin_hi,
                        bounds.order,
                        bounds.limit,
                    )
                    .await?;
                    cursor = Some(response.span.cursor_block.number);
                    finalized_done = response.span.cursor_block.number >= fin_hi;
                    txs.extend(response.txs);
                    if let Some(b) = response.blocks {
                        blocks.extend(b);
                    }
                }
            }
            if finalized_done && txs.len() < bounds.limit {
                scan_unfinalized_txs(
                    data_provider,
                    unfinalized_from,
                    bounds.hi,
                    bounds.order,
                    bounds.limit,
                    &filter,
                    relations,
                    &mut txs,
                    &mut blocks,
                    &mut cursor,
                )
                .await?;
            }
        }
        QueryOrder::Descending => {
            let reached_limit = bounds.hi >= unfinalized_from
                && scan_unfinalized_txs(
                    data_provider,
                    unfinalized_from,
                    bounds.hi,
                    bounds.order,
                    bounds.limit,
                    &filter,
                    relations,
                    &mut txs,
                    &mut blocks,
                    &mut cursor,
                )
                .await?;
            if !reached_limit && txs.len() < bounds.limit {
                if let Some(fin_hi) = finalized_hi {
                    if bounds.lo <= fin_hi {
                        let remaining = bounds.limit.saturating_sub(txs.len()).max(1);
                        let response = query_finalized_txs_page(
                            service,
                            &filter,
                            relations,
                            fin_hi,
                            bounds.lo,
                            bounds.order,
                            remaining,
                        )
                        .await?;
                        cursor = Some(response.span.cursor_block.number);
                        txs.extend(response.txs);
                        if let Some(b) = response.blocks {
                            blocks.extend(b);
                        }
                    }
                }
            }
        }
    }

    let span = build_merge_span(service, data_provider, &bounds, cursor).await;
    Ok(QueryTransactionsResponse {
        txs,
        blocks: relations.blocks.then(|| dedup_sort_blocks(blocks)),
        span,
    })
}

fn filter_set_to_opt<T: Eq + std::hash::Hash>(
    values: impl Iterator<Item = T>,
) -> Option<HashSet<T>> {
    let set: HashSet<T> = values.collect();
    if set.is_empty() {
        None
    } else {
        Some(set)
    }
}

fn log_entry_to_rpc_log(
    entry: &LogEntry,
    timestamp_by_block: &HashMap<u64, u64>,
    tx_hash_by_location: &HashMap<(u64, u32), Hash32>,
) -> RpcLog {
    RpcLog {
        inner: PrimitiveLog {
            address: entry.address,
            data: LogData::new_unchecked(entry.topics.clone(), entry.data.clone()),
        },
        block_hash: Some(entry.block_hash),
        block_number: Some(entry.block_number),
        block_timestamp: timestamp_by_block.get(&entry.block_number).copied(),
        transaction_hash: tx_hash_by_location
            .get(&(entry.block_number, entry.tx_index))
            .copied(),
        transaction_index: Some(u64::from(entry.tx_index)),
        log_index: Some(u64::from(entry.log_index)),
        removed: false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use monad_chain_data::{
        store::FjallStore, ConfiguredChainDataReader, EvmBlockHeader, FinalizedBlock,
        MonadChainDataService, QueryLimits,
    };
    use monad_triedb_utils::mock_triedb::MockTriedb;

    use super::*;

    #[test]
    fn fields_plan_enables_requested_relations() {
        let fields: HashMap<String, FieldSelector> = serde_json::from_value(serde_json::json!({
            "logs": ["blockNumber", "address"],
            "blocks": ["number"],
            "transactions": true
        }))
        .unwrap();

        let plan = fields_plan(
            Some(&fields),
            "logs",
            LOG_FIELDS,
            &[
                ("logs", LOG_FIELDS),
                ("transactions", TX_FIELDS),
                ("blocks", BLOCK_FIELDS),
            ],
        )
        .unwrap();

        assert_eq!(plan.primary, vec!["blockNumber", "address"]);
        assert_eq!(plan.blocks, Some(vec!["number"]));
        assert_eq!(plan.transactions, Some(TX_FIELDS.to_vec()));
    }

    #[test]
    fn fields_plan_rejects_unknown_keys_and_fields() {
        let fields: HashMap<String, FieldSelector> = serde_json::from_value(serde_json::json!({
            "logs": ["transactionHash"]
        }))
        .unwrap();
        assert!(fields_plan(Some(&fields), "logs", LOG_FIELDS, &[("logs", LOG_FIELDS)]).is_err());

        let fields: HashMap<String, FieldSelector> = serde_json::from_value(serde_json::json!({
            "receipts": true
        }))
        .unwrap();
        assert!(fields_plan(Some(&fields), "logs", LOG_FIELDS, &[("logs", LOG_FIELDS)]).is_err());
    }

    #[test]
    fn log_filter_matches_queryx_topic_shape() {
        let addr = "0x1111111111111111111111111111111111111111";
        let topic0 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let topic2 = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let filter = serde_json::json!({
            "address": [addr],
            "topics": [topic0, null, [topic2]]
        });

        let parsed = parse_log_filter(Some(&filter)).unwrap();
        assert!(parsed
            .address
            .unwrap()
            .contains(&parse_address(addr).unwrap()));
        assert!(parsed.topics[0]
            .as_ref()
            .unwrap()
            .contains(&parse_b256(topic0).unwrap()));
        assert!(parsed.topics[1].is_none());
        assert!(parsed.topics[2]
            .as_ref()
            .unwrap()
            .contains(&parse_b256(topic2).unwrap()));
    }

    #[test]
    fn trace_filter_parses_is_top_level_and_selector() {
        let filter = serde_json::json!({
            "from": "0x1111111111111111111111111111111111111111",
            "selector": ["0xa9059cbb"],
            "isTopLevel": true
        });
        let parsed = parse_trace_filter(Some(&filter)).unwrap();
        assert!(parsed.from.is_some());
        assert_eq!(parsed.is_top_level, Some(true));
        assert!(parsed.selector.unwrap().contains(&[0xa9, 0x05, 0x9c, 0xbb]));
    }

    #[test]
    fn transfer_filter_rejects_selector() {
        let filter = serde_json::json!({ "selector": ["0xa9059cbb"] });
        assert!(parse_transfer_filter(Some(&filter)).is_err());
    }

    #[test]
    fn block_bound_accepts_only_finalized_tag() {
        assert_eq!(parse_block_bound(Some("0x2a"), None).unwrap(), Some(42));
        assert_eq!(
            parse_block_bound(Some("finalized"), Some(7)).unwrap(),
            Some(7)
        );
        assert!(parse_block_bound(Some("latest"), Some(7)).is_err());
    }

    #[tokio::test]
    async fn queryx_only_rejects_non_queryx_methods() {
        let app_state = queryx_only_resources(None);

        let err = crate::handlers::rpc_select(
            &app_state,
            "eth_chainId",
            RequestParams::default(),
            TimingRequestId::random(),
        )
        .await
        .unwrap_err();

        assert_eq!(err, JsonRpcError::method_not_found());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn eth_query_blocks_handler_projects_selected_fields() {
        let dir = tempfile::tempdir().unwrap();
        let store = FjallStore::open(dir.path(), Default::default()).unwrap();
        let blob_store = store.clone();
        let writer =
            MonadChainDataService::new(store.clone(), blob_store, QueryLimits::new(10, 10));
        writer
            .ingest_block(FinalizedBlock {
                header: EvmBlockHeader {
                    number: 1,
                    timestamp: 123,
                    gas_limit: 45_000_000,
                    ..Default::default()
                },
                logs_by_tx: Vec::new(),
                txs: Vec::new(),
                traces: Vec::new(),
            })
            .await
            .unwrap();
        let service = Arc::new(ConfiguredChainDataReader::fjall(writer));

        let app_state = queryx_only_resources(Some(service));
        let params_raw = serde_json::value::to_raw_value(&serde_json::json!([{
            "fromBlock": "finalized",
            "toBlock": "finalized",
            "limit": "0x1",
            "fields": {
                "blocks": ["number", "timestamp", "gasLimit"]
            }
        }]))
        .unwrap();

        let result = eth_queryBlocks(
            TimingRequestId::random(),
            &app_state,
            RequestParams::new(&params_raw),
        )
        .await
        .unwrap();
        let result: Value = serde_json::from_str(result.get()).unwrap();

        assert_eq!(result["fromBlock"]["number"], "0x1");
        assert_eq!(result["toBlock"]["number"], "0x1");
        assert_eq!(result["cursorBlock"]["number"], "0x1");
        assert_eq!(result["data"]["blocks"][0]["number"], "0x1");
        assert_eq!(result["data"]["blocks"][0]["timestamp"], "0x7b");
        assert_eq!(result["data"]["blocks"][0]["gasLimit"], "0x2aea540");
        assert!(result["data"]["blocks"][0].get("hash").is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_logs_via_chain_data_builds_full_eth_log() {
        use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
        use alloy_eips::eip2718::Encodable2718;
        use alloy_primitives::{Bytes, Signature, TxKind};
        use monad_chain_data::IngestTx;

        let dir = tempfile::tempdir().unwrap();
        let store = FjallStore::open(dir.path(), Default::default()).unwrap();
        let blob_store = store.clone();
        let writer =
            MonadChainDataService::new(store.clone(), blob_store, QueryLimits::new(100, 1000));

        let address = Address::from([0x11u8; 20]);
        let topic = B256::from([0x22u8; 32]);
        let sender = Address::from([0x44u8; 20]);

        // A real (if dummily-signed) legacy tx so ingest and the tx
        // materializer can decode the stored envelope.
        let signature = Signature::new(U256::from(1), U256::from(1), false);
        let tx = TxLegacy {
            chain_id: Some(1),
            nonce: 0,
            gas_price: 1,
            gas_limit: 21_000,
            to: TxKind::Call(Address::ZERO),
            value: U256::ZERO,
            input: Bytes::new(),
        };
        let envelope: TxEnvelope = tx.into_signed(signature).into();
        let tx_hash = *envelope.tx_hash();
        let signed_tx_bytes = Bytes::from(envelope.encoded_2718());

        writer
            .ingest_block(FinalizedBlock {
                header: EvmBlockHeader {
                    number: 1,
                    timestamp: 0x7b,
                    gas_limit: 45_000_000,
                    ..Default::default()
                },
                logs_by_tx: vec![vec![PrimitiveLog {
                    address,
                    data: LogData::new_unchecked(vec![topic], Bytes::from_static(&[0xde, 0xad])),
                }]],
                txs: vec![IngestTx {
                    tx_hash,
                    sender,
                    signed_tx_bytes,
                }],
                traces: Vec::new(),
            })
            .await
            .unwrap();
        let service = ConfiguredChainDataReader::fjall(writer);

        // Address-filtered query over the finalized block. The relations
        // join must fill blockTimestamp (from the block) and
        // transactionHash (from the tx).
        let filter = Filter::new()
            .from_block(1u64)
            .to_block(1u64)
            .address(address);
        let logs = get_logs_via_chain_data(
            &service,
            None::<&DataProvider<MockTriedb>>,
            filter,
            u32::MAX,
            1000,
        )
        .await
        .unwrap();

        assert_eq!(logs.len(), 1);
        let log = &logs[0].0;
        assert_eq!(log.block_number, Some(1));
        assert_eq!(log.block_timestamp, Some(0x7b));
        assert_eq!(log.transaction_hash, Some(tx_hash));
        assert_eq!(log.transaction_index, Some(0));
        assert_eq!(log.log_index, Some(0));
        assert_eq!(log.address(), address);
        assert_eq!(log.topics()[0], topic);
        assert_eq!(log.inner.data.data, Bytes::from_static(&[0xde, 0xad]));

        // A filter that matches no address yields no logs.
        let other = Filter::new()
            .from_block(1u64)
            .to_block(1u64)
            .address(Address::from([0x99u8; 20]));
        let empty = get_logs_via_chain_data(
            &service,
            None::<&DataProvider<MockTriedb>>,
            other,
            u32::MAX,
            1000,
        )
        .await
        .unwrap();
        assert!(empty.is_empty());
    }

    /// A fixture with finalized block 1 (in the chain-data index) and
    /// unfinalized block 2 (served from a `MockTriedb` data_provider), each
    /// carrying a single tx that emits a single log. Shared by the eth and
    /// queryX tip tests.
    struct TipFixture {
        _dir: tempfile::TempDir,
        service: ChainDataService,
        data_provider: DataProvider<MockTriedb>,
        final_address: Address,
        tip_address: Address,
        final_tx_hash: B256,
        tip_tx_hash: B256,
    }

    async fn tip_fixture() -> TipFixture {
        use alloy_consensus::{
            Block as ConsensusBlock, BlockBody, Eip658Value, Header, Receipt, ReceiptEnvelope,
            ReceiptWithBloom, SignableTransaction, TxEip1559, TxEnvelope,
        };
        use alloy_eips::eip2718::Encodable2718;
        use alloy_primitives::{Bloom, Bytes, TxKind};
        use alloy_signer::SignerSync;
        use alloy_signer_local::PrivateKeySigner;
        use monad_chain_data::IngestTx;
        use monad_eth_types::ReceiptWithLogIndex;
        use monad_types::SeqNum;

        let dir = tempfile::tempdir().unwrap();
        let store = FjallStore::open(dir.path(), Default::default()).unwrap();
        let blob_store = store.clone();
        let writer =
            MonadChainDataService::new(store.clone(), blob_store, QueryLimits::new(100, 1000));

        // --- finalized block 1, ingested into the chain-data index ---
        let final_address = Address::from([0x11u8; 20]);
        let signer = PrivateKeySigner::random();
        let final_tx = TxEip1559 {
            chain_id: 1,
            nonce: 0,
            gas_limit: 21_000,
            max_fee_per_gas: 1,
            max_priority_fee_per_gas: 1,
            to: TxKind::Call(Address::ZERO),
            value: U256::ZERO,
            access_list: Default::default(),
            input: Bytes::new(),
        };
        let final_sig = signer.sign_hash_sync(&final_tx.signature_hash()).unwrap();
        let final_envelope: TxEnvelope = final_tx.into_signed(final_sig).into();
        let final_tx_hash = *final_envelope.tx_hash();

        writer
            .ingest_block(FinalizedBlock {
                header: EvmBlockHeader {
                    number: 1,
                    timestamp: 0x7b,
                    gas_limit: 45_000_000,
                    ..Default::default()
                },
                logs_by_tx: vec![vec![PrimitiveLog {
                    address: final_address,
                    data: LogData::new_unchecked(vec![], Bytes::from_static(&[0xde, 0xad])),
                }]],
                txs: vec![IngestTx {
                    tx_hash: final_tx_hash,
                    sender: signer.address(),
                    signed_tx_bytes: Bytes::from(final_envelope.encoded_2718()),
                }],
                traces: Vec::new(),
            })
            .await
            .unwrap();
        let service = ConfiguredChainDataReader::fjall(writer);

        // --- unfinalized block 2, served from data_provider (triedb) ---
        let tip_address = Address::from([0x22u8; 20]);
        let tip_tx = TxEip1559 {
            chain_id: 1,
            nonce: 1,
            gas_limit: 21_000,
            max_fee_per_gas: 1,
            max_priority_fee_per_gas: 1,
            to: TxKind::Call(Address::ZERO),
            value: U256::ZERO,
            access_list: Default::default(),
            input: Bytes::new(),
        };
        let tip_sig = signer.sign_hash_sync(&tip_tx.signature_hash()).unwrap();
        let tip_envelope: TxEnvelope = tip_tx.into_signed(tip_sig).into();
        let tip_tx_hash = *tip_envelope.tx_hash();

        let mut mock = MockTriedb::default();
        mock.set_latest_block(2);
        mock.set_finalized_block(
            SeqNum(2),
            ConsensusBlock {
                header: Header {
                    number: 2,
                    timestamp: 0x99,
                    gas_limit: 45_000_000,
                    ..Default::default()
                },
                body: BlockBody {
                    transactions: vec![tip_envelope],
                    ommers: vec![],
                    withdrawals: None,
                },
            },
        );
        mock.set_receipts(
            SeqNum(2),
            vec![ReceiptWithLogIndex {
                receipt: ReceiptEnvelope::Eip1559(ReceiptWithBloom {
                    receipt: Receipt {
                        status: Eip658Value::Eip658(true),
                        cumulative_gas_used: 21_000,
                        logs: vec![PrimitiveLog {
                            address: tip_address,
                            data: LogData::new_unchecked(vec![], Bytes::from_static(&[0xbe, 0xef])),
                        }],
                    },
                    logs_bloom: Bloom::default(),
                }),
                starting_log_index: 0,
            }],
        );

        TipFixture {
            _dir: dir,
            service,
            data_provider: DataProvider::new(None, mock.into(), None),
            final_address,
            tip_address,
            final_tx_hash,
            tip_tx_hash,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_logs_via_chain_data_includes_unfinalized_tip() {
        let f = tip_fixture().await;

        // Unfiltered query over `1..=latest`. `latest` resolves to the tip
        // (block 2) via data_provider, so the result spans the finalized block
        // 1 (from the index) and the unfinalized block 2 (from triedb).
        let filter = Filter::new()
            .from_block(1u64)
            .to_block(BlockNumberOrTag::Latest);
        let logs =
            get_logs_via_chain_data(&f.service, Some(&f.data_provider), filter, u32::MAX, 1000)
                .await
                .unwrap();

        assert_eq!(logs.len(), 2, "expected one finalized and one tip log");

        let finalized = &logs[0].0;
        assert_eq!(finalized.block_number, Some(1));
        assert_eq!(finalized.address(), f.final_address);
        assert_eq!(finalized.transaction_hash, Some(f.final_tx_hash));

        let tip = &logs[1].0;
        assert_eq!(tip.block_number, Some(2));
        assert_eq!(tip.block_timestamp, Some(0x99));
        assert_eq!(tip.address(), f.tip_address);
        assert_eq!(tip.transaction_hash, Some(f.tip_tx_hash));
        assert_eq!(tip.transaction_index, Some(0));
        assert_eq!(tip.log_index, Some(0));
        assert_eq!(
            tip.inner.data.data,
            alloy_primitives::Bytes::from_static(&[0xbe, 0xef])
        );

        // `toBlock: finalized` (and `safe`) must stop at the finalized head
        // and exclude the unfinalized tip, even though `latest` reaches it.
        for tag in [BlockNumberOrTag::Finalized, BlockNumberOrTag::Safe] {
            let finalized_filter = Filter::new().from_block(1u64).to_block(tag);
            let finalized_only = get_logs_via_chain_data(
                &f.service,
                Some(&f.data_provider),
                finalized_filter,
                u32::MAX,
                1000,
            )
            .await
            .unwrap();
            assert_eq!(finalized_only.len(), 1, "{tag:?} must exclude the tip");
            assert_eq!(finalized_only[0].0.block_number, Some(1));
        }
    }

    fn logs_request(
        order: Option<&str>,
        from: &str,
        to: &str,
        limit: Option<&str>,
    ) -> RawQueryRequest {
        RawQueryRequest {
            filter: None,
            fields: None,
            order: order.map(str::to_string),
            from_block: Some(from.to_string()),
            to_block: Some(to.to_string()),
            limit: limit.map(str::to_string),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn eth_query_logs_merges_unfinalized_tip() {
        let f = tip_fixture().await;
        let relations = LogsRelations {
            blocks: true,
            transactions: true,
        };

        // Ascending `1..=latest`: finalized block 1 from the index, tip block
        // 2 from the cascade, merged ascending; relations span both.
        let resp = execute_logs_query(
            &f.service,
            Some(&f.data_provider),
            &logs_request(None, "0x1", "latest", None),
            LogFilter::default(),
            relations,
        )
        .await
        .unwrap();
        assert_eq!(
            resp.logs.iter().map(|l| l.block_number).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(resp.logs[0].address, f.final_address);
        assert_eq!(resp.logs[1].address, f.tip_address);
        assert_eq!(resp.span.from_block.number, 1);
        assert_eq!(resp.span.to_block.number, 2);
        assert_eq!(resp.span.cursor_block.number, 2);
        assert_eq!(
            resp.blocks
                .unwrap()
                .iter()
                .map(|b| b.header.number)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(
            resp.transactions
                .unwrap()
                .iter()
                .map(|t| (t.block_number, t.tx_hash))
                .collect::<Vec<_>>(),
            vec![(1, f.final_tx_hash), (2, f.tip_tx_hash)]
        );

        // Descending `latest..=1`: tip first, then finalized.
        let resp_desc = execute_logs_query(
            &f.service,
            Some(&f.data_provider),
            &logs_request(Some("desc"), "latest", "0x1", None),
            LogFilter::default(),
            relations,
        )
        .await
        .unwrap();
        assert_eq!(
            resp_desc
                .logs
                .iter()
                .map(|l| l.block_number)
                .collect::<Vec<_>>(),
            vec![2, 1]
        );

        // `limit: 1` ascending completes block 1 and stops before the tip.
        let resp_limit = execute_logs_query(
            &f.service,
            Some(&f.data_provider),
            &logs_request(None, "0x1", "latest", Some("0x1")),
            LogFilter::default(),
            relations,
        )
        .await
        .unwrap();
        assert_eq!(
            resp_limit
                .logs
                .iter()
                .map(|l| l.block_number)
                .collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(resp_limit.span.cursor_block.number, 1);

        // `toBlock: finalized` excludes the unfinalized tip.
        let resp_final = execute_logs_query(
            &f.service,
            Some(&f.data_provider),
            &logs_request(None, "0x1", "finalized", None),
            LogFilter::default(),
            relations,
        )
        .await
        .unwrap();
        assert_eq!(
            resp_final
                .logs
                .iter()
                .map(|l| l.block_number)
                .collect::<Vec<_>>(),
            vec![1]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn eth_query_logs_rejects_window_past_max_block_range() {
        // A window reaching far past a lagging index must be rejected up front
        // rather than scanned block-by-block via the cascade. `max_block_range`
        // is the worst-case scan budget and bounds the whole merged window,
        // not just the finalized sub-range.
        let dir = tempfile::tempdir().unwrap();
        let store = FjallStore::open(dir.path(), Default::default()).unwrap();
        let blob_store = store.clone();
        // max_block_range = 4; nothing indexed yet (head_final = None).
        let writer =
            MonadChainDataService::new(store.clone(), blob_store, QueryLimits::new(100, 4));
        let service = ConfiguredChainDataReader::fjall(writer);

        // data_provider reports a tip far ahead of the (empty) index.
        let mut mock = MockTriedb::default();
        mock.set_latest_block(1000);
        let data_provider = DataProvider::new(None, mock.into(), None);

        // `1..=latest` spans 1000 blocks; the merge must refuse it (-32005)
        // before attempting any per-block fetch.
        let err = execute_logs_query(
            &service,
            Some(&data_provider),
            &logs_request(None, "0x1", "latest", None),
            LogFilter::default(),
            LogsRelations::default(),
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, -32005);

        // A window within the budget passes the span guard, but blocks 1..=4
        // aren't materializable from the empty mock → loud unavailable-block
        // error (-32603), not a silent empty result.
        let unavailable = execute_logs_query(
            &service,
            Some(&data_provider),
            &logs_request(None, "0x1", "0x4", None),
            LogFilter::default(),
            LogsRelations::default(),
        )
        .await
        .unwrap_err();
        assert_eq!(unavailable.code, -32603);
    }

    fn queryx_only_resources(chain_data: Option<Arc<ChainDataService>>) -> MonadRpcResources {
        MonadRpcResources {
            txpool_bridge_client: None,
            queryx_only: true,
            eth_call_handler: None,
            chain_id: 1337,
            data_provider: None,
            chain_data,
            event_server_client: None,
            batch_request_limit: 5,
            max_response_size: 25_000_000,
            allow_unprotected_txs: false,
            logs_max_block_range: 1000,
            eth_send_raw_transaction_sync_default_timeout_ms: 2_000,
            eth_send_raw_transaction_sync_max_timeout_ms: 10_000,
            dry_run_get_logs_index: false,
            use_eth_get_logs_index: false,
            max_finalized_block_cache_len: 200,
            metrics: None,
            rpc_comparator: None,
        }
    }
}
