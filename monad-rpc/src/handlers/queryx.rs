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

use std::collections::{HashMap, HashSet};

use alloy_consensus::Transaction;
use alloy_eips::eip2718::Typed2718;
use alloy_primitives::{Address, B256, U256};
use monad_chain_data::{
    Block, BlockRef, BlockSpan, CallKind, LogEntry, LogFilter, LogsRelations, MonadChainDataError,
    QueryBlocksRequest, QueryBlocksResponse, QueryEnvelope, QueryLogsRequest, QueryLogsResponse,
    QueryOrder, QueryTracesRequest, QueryTracesResponse, QueryTransactionsRequest,
    QueryTransactionsResponse, QueryTransfersRequest, QueryTransfersResponse, TraceEntry,
    TraceFilter, TracesRelations, TransferEntry, TransferFilter, TransfersRelations, TxEntry,
    TxFilter, TxsRelations,
};
use serde::Deserialize;
use serde_json::{value::RawValue, Map, Value};
use tracing::debug;

use crate::{
    handlers::resources::MonadRpcResources,
    middleware::TimingRequestId,
    types::{
        eth_json::serialize_result,
        ethhex,
        jsonrpc::{JsonRpcError, JsonRpcResultExt, RequestParams},
    },
};

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
    "txStatus",
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
    let request = QueryTransactionsRequest {
        envelope: build_envelope(&raw, service).await?,
        filter: parse_tx_filter(raw.filter.as_ref())?,
        relations: TxsRelations {
            blocks: fields.blocks.is_some(),
        },
    };
    let response = service
        .query_transactions(request)
        .await
        .map_err(chain_data_error_to_jsonrpc)?;
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
    let request = QueryLogsRequest {
        envelope: build_envelope(&raw, service).await?,
        filter: parse_log_filter(raw.filter.as_ref())?,
        relations: LogsRelations {
            blocks: fields.blocks.is_some(),
            transactions: fields.transactions.is_some(),
        },
    };
    let response = service
        .query_logs(request)
        .await
        .map_err(chain_data_error_to_jsonrpc)?;
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
    service: &monad_chain_data::MonadChainDataService<
        monad_chain_data::store::FjallStore,
        monad_chain_data::store::BlobCompressionStore<monad_chain_data::store::FjallStore>,
    >,
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
                .publication()
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
            "isTopLevel" => out.is_top_level = Some(as_bool(value)?),
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
            "isTopLevel" => out.is_top_level = Some(as_bool(value)?),
            _ => return Err(JsonRpcError::invalid_params()),
        }
    }
    Ok(out)
}

fn as_object(value: &Value) -> Result<&Map<String, Value>, JsonRpcError> {
    value.as_object().ok_or_else(JsonRpcError::invalid_params)
}

fn as_bool(value: &Value) -> Result<bool, JsonRpcError> {
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
            "traceAddress" => number_array(&trace.trace_address),
            "type" => Value::String(call_kind(trace.typ).to_string()),
            "from" => fixed(trace.from),
            "to" => trace.to.map(fixed).unwrap_or(Value::Null),
            "value" => q_u256(trace.value),
            "gas" => q_u64(trace.gas),
            "gasUsed" => q_u64(trace.gas_used),
            "input" => bytes(&trace.input),
            "output" => bytes(&trace.output),
            "status" => q_u64(u64::from(trace.status)),
            "depth" => q_u64(u64::from(trace.depth)),
            "txStatus" => Value::Bool(trace.tx_status),
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
            "traceAddress" => number_array(&transfer.trace_address),
            "type" => Value::String(call_kind(transfer.typ).to_string()),
            "from" => fixed(transfer.from),
            "to" => fixed(transfer.to),
            "value" => q_u256(transfer.value),
            _ => continue,
        };
        out.insert((*field).to_string(), value);
    }
    Value::Object(out)
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

fn number_array(values: &[u32]) -> Value {
    Value::Array(values.iter().copied().map(u64::from).map(q_u64).collect())
}

fn call_kind(kind: CallKind) -> &'static str {
    match kind {
        CallKind::Call => "call",
        CallKind::DelegateCall => "delegatecall",
        CallKind::CallCode => "callcode",
        CallKind::Create => "create",
        CallKind::Create2 => "create2",
        CallKind::SelfDestruct => "selfdestruct",
        CallKind::StaticCall => "staticcall",
    }
}

fn serialize_queryx_result(
    value: Value,
    app_state: &MonadRpcResources,
) -> Result<Box<RawValue>, JsonRpcError> {
    let raw = serialize_result(value)?;
    let response_size_bytes = raw.get().as_bytes().len();
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
        MonadChainDataError::FencedOut { .. } => {
            JsonRpcError::internal_error("chain-data writer fenced out during query".to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use monad_chain_data::{
        store::{BlobCompressionConfig, BlobCompressionStats, BlobCompressionStore, FjallStore},
        EvmBlockHeader, FinalizedBlock, MonadChainDataService, QueryLimits,
    };

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
    fn block_bound_accepts_only_finalized_tag_for_demo() {
        assert_eq!(parse_block_bound(Some("0x2a"), None).unwrap(), Some(42));
        assert_eq!(
            parse_block_bound(Some("finalized"), Some(7)).unwrap(),
            Some(7)
        );
        assert!(parse_block_bound(Some("latest"), Some(7)).is_err());
    }

    #[tokio::test]
    async fn queryx_only_rejects_non_queryx_methods() {
        let app_state = MonadRpcResources {
            txpool_bridge_client: None,
            queryx_only: true,
            eth_call_handler: None,
            chain_id: 1337,
            chain_state: None,
            chain_data: None,
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
        };

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
        let blob_store = BlobCompressionStore::new(
            store.clone(),
            BlobCompressionConfig::zstd(1, 1024),
            BlobCompressionStats::default(),
        );
        let service = Arc::new(MonadChainDataService::new(
            store.clone(),
            blob_store,
            QueryLimits::new(10, 10),
        ));
        service
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

        let app_state = MonadRpcResources {
            txpool_bridge_client: None,
            queryx_only: false,
            eth_call_handler: None,
            chain_id: 1337,
            chain_state: None,
            chain_data: Some(service),
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
        };
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
}
