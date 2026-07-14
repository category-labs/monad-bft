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

//! queryX transport: JSON-RPC adapters over the chain-data reader. The
//! `docs/queryX.md` spec shapes live in [`wire`]; this layer parses
//! params, converts to the engine's request types, calls the service, and
//! shapes the response (`data` keyed by object type, projected to the
//! requested `fields`, plus the resolved block references).

pub mod wire;

use std::sync::Arc;

use alloy_primitives::U64;
use monad_query_config::ConfiguredChainDataReader;
use monad_query_errors::QueryError;
use monad_query_read::{
    blocks::QueryBlocksRequest,
    logs::{LogsRelations, QueryLogsRequest},
    traces::{QueryTracesRequest, TracesRelations},
    transfers::{QueryTransfersRequest, TransfersRelations},
    txs::{QueryTransactionsRequest, TxsRelations},
};
use serde_json::value::RawValue;
use tracing::{debug, trace};
use wire::{
    BlockWire, EmptyFilterWire, FieldsPlan, LogFilterWire, LogWire, QueryRequestWire,
    QueryResponseWire, TraceFilterWire, TraceWire, TransferFilterWire, TransferWire, TxFilterWire,
};

use crate::types::jsonrpc::{JsonRpcError, JsonRpcResult, JsonRpcResultExt, RequestParams};

/// queryX execution context: the chain-data reader plus the dedicated
/// runtime its queries run on. RPC workers are few and single-threaded
/// (actix arbiters); queryX requests spawn onto this runtime so engine
/// fan-out and response serialization scale with `--chain-data-query-threads`
/// instead of `--worker-threads`.
#[derive(Clone)]
pub struct ChainDataQueryRuntime {
    pub reader: Arc<ConfiguredChainDataReader>,
    pub handle: tokio::runtime::Handle,
}

impl ChainDataQueryRuntime {
    /// Runs one queryX adapter future to completion on the dedicated runtime.
    pub async fn run<F, T>(&self, query: F) -> JsonRpcResult<T>
    where
        F: std::future::Future<Output = JsonRpcResult<T>> + Send + 'static,
        T: Send + 'static,
    {
        self.handle
            .spawn(query)
            .await
            .map_err(|e| JsonRpcError::internal_error(format!("chain-data query task: {e}")))?
    }
}

/// queryX limit breaches surface with their own error code (see
/// `monad_query_primitives::limits::QueryLimits`).
const QUERYX_LIMIT_EXCEEDED_CODE: i32 = -32005;

/// The spec says the `-32005` error's `data` SHOULD carry the server's limits.
fn limit_data(max_limit: usize, max_block_range: u64) -> Option<Box<RawValue>> {
    #[derive(serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Limits {
        max_limit: U64,
        max_block_range: U64,
    }
    serde_json::value::to_raw_value(&Limits {
        max_limit: U64::from(max_limit),
        max_block_range: U64::from(max_block_range),
    })
    .ok()
}

/// Maps engine errors to the JSON-RPC conventions: request-shape errors are
/// invalid params (with the engine's message), limit breaches are the queryX
/// `-32005` carrying the limits in `data`, and everything else (missing data,
/// backend) is an internal error.
pub(super) fn chain_data_error(e: QueryError) -> JsonRpcError {
    match e {
        QueryError::InvalidRequest(message) => JsonRpcError::filter_error(message.to_string()),
        e @ QueryError::LimitExceeded {
            max_limit,
            max_block_range,
            ..
        } => JsonRpcError {
            code: QUERYX_LIMIT_EXCEEDED_CODE,
            message: e.to_string(),
            data: limit_data(max_limit, max_block_range),
        },
        e => {
            debug!("chain-data error: {e}");
            JsonRpcError::internal_error(format!("chain-data error: {e}"))
        }
    }
}

/// Parses a queryX method's single optional request object (`[{...}]`).
/// Absent/empty params and `[]` mean an all-defaults request.
pub(super) fn parse_query_request<T>(params: RequestParams<'_>) -> JsonRpcResult<T>
where
    T: serde::de::DeserializeOwned + Default,
{
    let raw = params.get().trim();
    if raw.is_empty() || raw == "null" {
        return Ok(T::default());
    }
    let mut args: Vec<T> = serde_json::from_str(raw).invalid_params()?;
    if args.len() > 1 {
        return Err(JsonRpcError::invalid_params());
    }
    Ok(args.pop().unwrap_or_default())
}

#[allow(non_snake_case)]
#[tracing::instrument(level = "debug", skip_all)]
/// Executes a finalized queryX logs query over the published head.
pub async fn monad_eth_queryLogs(
    reader: &ConfiguredChainDataReader,
    mut request: QueryRequestWire<LogFilterWire>,
) -> JsonRpcResult<QueryResponseWire> {
    trace!("monad_eth_queryLogs: {request:?}");
    let fields = FieldsPlan::new(request.fields.take(), &["logs", "transactions", "blocks"])?;
    let request = QueryLogsRequest {
        envelope: request.envelope()?,
        filter: request.filter.unwrap_or_default().into_engine()?,
        relations: LogsRelations {
            blocks: fields.wants("blocks"),
            transactions: fields.wants("transactions"),
        },
    };
    let response = reader.query_logs(request).await.map_err(chain_data_error)?;
    let mut out = QueryResponseWire::new(response.span);
    let logs: Vec<LogWire> = response.logs.into_iter().map(LogWire::from).collect();
    fields.insert(&mut out.data, "logs", &logs)?;
    if let Some(txs) = response.transactions {
        fields.insert(&mut out.data, "transactions", &wire::rpc_transactions(txs))?;
    }
    if let Some(blocks) = response.blocks {
        let blocks: Vec<BlockWire> = blocks.into_iter().map(BlockWire::from).collect();
        fields.insert(&mut out.data, "blocks", &blocks)?;
    }
    Ok(out)
}

#[allow(non_snake_case)]
#[tracing::instrument(level = "debug", skip_all)]
/// Executes a finalized queryX transactions query over the published head.
pub async fn monad_eth_queryTransactions(
    reader: &ConfiguredChainDataReader,
    mut request: QueryRequestWire<TxFilterWire>,
) -> JsonRpcResult<QueryResponseWire> {
    trace!("monad_eth_queryTransactions: {request:?}");
    let fields = FieldsPlan::new(request.fields.take(), &["transactions", "blocks"])?;
    let request = QueryTransactionsRequest {
        envelope: request.envelope()?,
        filter: request.filter.unwrap_or_default().into_engine(),
        relations: TxsRelations {
            blocks: fields.wants("blocks"),
        },
    };
    let response = reader
        .query_transactions(request)
        .await
        .map_err(chain_data_error)?;
    let mut out = QueryResponseWire::new(response.span);
    fields.insert(
        &mut out.data,
        "transactions",
        &wire::rpc_transactions(response.txs),
    )?;
    if let Some(blocks) = response.blocks {
        let blocks: Vec<BlockWire> = blocks.into_iter().map(BlockWire::from).collect();
        fields.insert(&mut out.data, "blocks", &blocks)?;
    }
    Ok(out)
}

#[allow(non_snake_case)]
#[tracing::instrument(level = "debug", skip_all)]
/// Executes a finalized queryX traces query over the published head.
pub async fn monad_eth_queryTraces(
    reader: &ConfiguredChainDataReader,
    mut request: QueryRequestWire<TraceFilterWire>,
) -> JsonRpcResult<QueryResponseWire> {
    trace!("monad_eth_queryTraces: {request:?}");
    let fields = FieldsPlan::new(request.fields.take(), &["traces", "transactions", "blocks"])?;
    let request = QueryTracesRequest {
        envelope: request.envelope()?,
        filter: request.filter.unwrap_or_default().into_engine(),
        relations: TracesRelations {
            blocks: fields.wants("blocks"),
            transactions: fields.wants("transactions"),
        },
    };
    let response = reader
        .query_traces(request)
        .await
        .map_err(chain_data_error)?;
    let mut out = QueryResponseWire::new(response.span);
    let traces: Vec<TraceWire> = response.traces.into_iter().map(TraceWire::from).collect();
    fields.insert(&mut out.data, "traces", &traces)?;
    if let Some(txs) = response.transactions {
        fields.insert(&mut out.data, "transactions", &wire::rpc_transactions(txs))?;
    }
    if let Some(blocks) = response.blocks {
        let blocks: Vec<BlockWire> = blocks.into_iter().map(BlockWire::from).collect();
        fields.insert(&mut out.data, "blocks", &blocks)?;
    }
    Ok(out)
}

#[allow(non_snake_case)]
#[tracing::instrument(level = "debug", skip_all)]
/// Executes a finalized queryX transfers query over the published head.
pub async fn monad_eth_queryTransfers(
    reader: &ConfiguredChainDataReader,
    mut request: QueryRequestWire<TransferFilterWire>,
) -> JsonRpcResult<QueryResponseWire> {
    trace!("monad_eth_queryTransfers: {request:?}");
    let fields = FieldsPlan::new(
        request.fields.take(),
        &["transfers", "transactions", "blocks"],
    )?;
    let request = QueryTransfersRequest {
        envelope: request.envelope()?,
        filter: request.filter.unwrap_or_default().into_engine(),
        relations: TransfersRelations {
            blocks: fields.wants("blocks"),
            transactions: fields.wants("transactions"),
        },
    };
    let response = reader
        .query_transfers(request)
        .await
        .map_err(chain_data_error)?;
    let mut out = QueryResponseWire::new(response.span);
    let transfers: Vec<TransferWire> = response
        .transfers
        .into_iter()
        .map(TransferWire::from)
        .collect();
    fields.insert(&mut out.data, "transfers", &transfers)?;
    if let Some(txs) = response.transactions {
        fields.insert(&mut out.data, "transactions", &wire::rpc_transactions(txs))?;
    }
    if let Some(blocks) = response.blocks {
        let blocks: Vec<BlockWire> = blocks.into_iter().map(BlockWire::from).collect();
        fields.insert(&mut out.data, "blocks", &blocks)?;
    }
    Ok(out)
}

#[allow(non_snake_case)]
#[tracing::instrument(level = "debug", skip_all)]
/// Executes a finalized queryX blocks query over the published head.
pub async fn monad_eth_queryBlocks(
    reader: &ConfiguredChainDataReader,
    mut request: QueryRequestWire<EmptyFilterWire>,
) -> JsonRpcResult<QueryResponseWire> {
    trace!("monad_eth_queryBlocks: {request:?}");
    let fields = FieldsPlan::new(request.fields.take(), &["blocks"])?;
    let request = QueryBlocksRequest {
        envelope: request.envelope()?,
    };
    let response = reader
        .query_blocks(request)
        .await
        .map_err(chain_data_error)?;
    let mut out = QueryResponseWire::new(response.span);
    let blocks: Vec<BlockWire> = response.blocks.into_iter().map(BlockWire::from).collect();
    fields.insert(&mut out.data, "blocks", &blocks)?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, B256};
    use monad_query_errors::LimitExceededKind;
    use monad_query_primitives::order::QueryOrder;
    use serde_json::{json, Value};

    use super::{wire::OneOrMany, *};
    use crate::types::jsonrpc::RequestParams;

    fn params(raw: &str) -> Box<serde_json::value::RawValue> {
        serde_json::value::RawValue::from_string(raw.to_string()).unwrap()
    }

    fn parse_logs_request(raw: &str) -> JsonRpcResult<QueryRequestWire<LogFilterWire>> {
        let raw = params(raw);
        parse_query_request(RequestParams::new(&raw))
    }

    #[test]
    fn parse_query_request_full_logs_request() {
        let mut request = parse_logs_request(
            r#"[{
                "fromBlock": "0x1", "toBlock": "0x5", "order": "desc", "limit": "0x7",
                "filter": {"address": "0x00000000000000000000000000000000000000aa",
                           "topics": [["0x00000000000000000000000000000000000000000000000000000000000000bb"], null]},
                "fields": {"logs": ["address"], "blocks": true}
            }]"#,
        )
        .expect("parse");
        let envelope = request.envelope().expect("envelope");
        assert_eq!(envelope.from_block, Some(1));
        assert_eq!(envelope.to_block, Some(5));
        assert_eq!(envelope.order, QueryOrder::Descending);
        assert_eq!(envelope.limit, 7);

        let filter = request
            .filter
            .take()
            .unwrap()
            .into_engine()
            .expect("filter");
        let address = filter.address.expect("address filter");
        let expected: Address = "0x00000000000000000000000000000000000000aa"
            .parse()
            .unwrap();
        assert!(address.contains(&expected));
        let topic0 = filter.topics[0].as_ref().expect("topic0 filter");
        assert!(topic0.contains(&B256::with_last_byte(0xbb)));
        assert!(filter.topics[1].is_none());

        let fields = FieldsPlan::new(request.fields.take(), &["logs", "transactions", "blocks"])
            .expect("fields");
        assert!(fields.wants("blocks"));
        assert!(!fields.wants("transactions"));
    }

    #[test]
    fn parse_query_request_defaults_for_absent_empty_and_partial_params() {
        for raw in ["", "null", "[]", "[{}]"] {
            let raw = if raw.is_empty() {
                None
            } else {
                Some(params(raw))
            };
            let p = raw.as_deref().map(RequestParams::new).unwrap_or_default();
            let request: QueryRequestWire<LogFilterWire> =
                parse_query_request(p).expect("all-defaults parse");
            let envelope = request.envelope().expect("envelope");
            assert_eq!(
                envelope,
                monad_query_primitives::limits::QueryEnvelope::default()
            );
        }
        // Omitted fields fall back per-field (limit keeps the spec default).
        let request = parse_logs_request(r#"[{"fromBlock": "0x3"}]"#).expect("parse");
        let envelope = request.envelope().expect("envelope");
        assert_eq!(envelope.from_block, Some(3));
        assert_eq!(
            envelope.limit,
            monad_query_primitives::limits::QueryEnvelope::default().limit
        );
    }

    #[test]
    fn parse_query_request_rejects_malformed_and_extra_params() {
        for raw in [
            r#"[1]"#,
            r#"{"fromBlock": "0x1"}"#,
            r#"[{}, {}]"#,
            // Unknown request keys (the engine-internal names included).
            r#"[{"envelope": {"from_block": 1}}]"#,
            r#"[{"relations": {"blocks": true}}]"#,
            // The engine-internal order variants are not wire values.
            r#"[{"order": "Ascending"}]"#,
            // Unknown filter keys.
            r#"[{"filter": {"sender": "0x00000000000000000000000000000000000000aa"}}]"#,
        ] {
            let err = parse_logs_request(raw).expect_err("reject");
            assert_eq!(err.code, -32602, "{raw:?}");
        }
    }

    #[test]
    fn tags_resolve_to_defaults_or_reject() {
        // On the side whose omitted default they name, tags are the default.
        let request = parse_logs_request(r#"[{"toBlock": "latest"}]"#).expect("parse");
        assert_eq!(request.envelope().expect("envelope").to_block, None);
        let request = parse_logs_request(
            r#"[{"order": "desc", "fromBlock": "finalized", "toBlock": "earliest"}]"#,
        )
        .expect("parse");
        let envelope = request.envelope().expect("envelope");
        assert_eq!((envelope.from_block, envelope.to_block), (None, None));

        // Elsewhere they would re-anchor the scan at the head: rejected.
        for raw in [
            r#"[{"fromBlock": "latest"}]"#,
            r#"[{"toBlock": "pending"}]"#,
            r#"[{"order": "desc", "toBlock": "latest"}]"#,
        ] {
            let request = parse_logs_request(raw).expect("parse");
            assert_eq!(
                request.envelope().expect_err("reject").code,
                -32602,
                "{raw:?}"
            );
        }
    }

    #[test]
    fn filters_accept_single_values_and_arrays() {
        assert!(serde_json::from_value::<OneOrMany<u32>>(json!(1)).is_ok());
        assert!(serde_json::from_value::<OneOrMany<u32>>(json!([1, 2])).is_ok());
        assert!(serde_json::from_value::<OneOrMany<u32>>(json!("nope")).is_err());

        let filter: TxFilterWire = serde_json::from_value(json!({
            "from": "0x00000000000000000000000000000000000000aa",
            "to": ["0x00000000000000000000000000000000000000bb",
                   "0x00000000000000000000000000000000000000cc"],
            "selector": "0xa9059cbb"
        }))
        .expect("parse");
        let filter = filter.into_engine();
        assert_eq!(filter.from.as_ref().map(|s| s.len()), Some(1));
        assert_eq!(filter.to.as_ref().map(|s| s.len()), Some(2));
        let selectors = filter.selector.expect("selector filter");
        assert!(selectors.contains(&[0xa9, 0x05, 0x9c, 0xbb]));
    }

    #[test]
    fn log_filter_rejects_more_than_four_topic_positions() {
        let filter: LogFilterWire =
            serde_json::from_value(json!({"topics": [null, null, null, null, null]}))
                .expect("parse");
        assert_eq!(filter.into_engine().expect_err("reject").code, -32602);
    }

    #[test]
    fn fields_plan_validates_keys_and_projects() {
        let allowed = &["logs", "blocks"];
        let fields = |v: Value| serde_json::from_value(v).unwrap();

        let err = FieldsPlan::new(fields(json!({"traces": true})), allowed).expect_err("key");
        assert_eq!(err.code, -32602);
        let err = FieldsPlan::new(fields(json!({"logs": false})), allowed).expect_err("false");
        assert_eq!(err.code, -32602);

        let plan = FieldsPlan::new(
            fields(json!({"logs": ["address"], "blocks": true})),
            allowed,
        )
        .expect("plan");
        assert!(plan.wants("blocks") && !plan.wants("transactions"));
        let mut data = std::collections::BTreeMap::new();
        plan.insert(
            &mut data,
            "logs",
            &[json!({"address": "0xaa", "data": "0x"})],
        )
        .expect("insert logs");
        plan.insert(&mut data, "blocks", &[json!({"number": "0x1"})])
            .expect("insert blocks");
        let parse = |key: &str| -> Value { serde_json::from_str(data[key].get()).unwrap() };
        assert_eq!(parse("logs"), json!([{"address": "0xaa"}]));
        assert_eq!(parse("blocks"), json!([{"number": "0x1"}]));
    }

    #[test]
    fn chain_data_error_mapping() {
        let invalid = chain_data_error(QueryError::InvalidRequest("limit must be at least 1"));
        assert_eq!(invalid.code, -32602);
        assert_eq!(invalid.message, "limit must be at least 1");

        let limit = chain_data_error(QueryError::LimitExceeded {
            kind: LimitExceededKind::Limit,
            max_limit: 5,
            max_block_range: 100,
        });
        assert_eq!(limit.code, QUERYX_LIMIT_EXCEEDED_CODE);
        assert!(limit.message.contains("max_limit=5"));
        let data: Value = serde_json::from_str(limit.data.expect("limits data").get()).unwrap();
        assert_eq!(data, json!({"maxLimit": "0x5", "maxBlockRange": "0x64"}));

        for internal in [
            chain_data_error(QueryError::MissingData("no published blocks")),
            chain_data_error(QueryError::Backend("boom".into())),
        ] {
            assert_eq!(internal.code, -32603);
        }
    }
}
