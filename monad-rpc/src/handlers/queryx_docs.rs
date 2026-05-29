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

//! OpenRPC documentation for the queryX methods.
//!
//! The live handlers in [`super::queryx`] parse a loosely-typed
//! `serde_json::Value` request because the `filter`/`fields` objects are
//! method-specific, so they carry no `schemars` schema the OpenRPC
//! generator can read. This module mirrors the queryX request/response
//! shapes as typed `JsonSchema` structs and attaches them to one
//! `#[rpc]`-annotated stub per method. The stubs are never dispatched
//! (dispatch goes through [`super::queryx`]); the `#[rpc]` macro only
//! reads their signatures to register the method, its parameters, and its
//! result schema with `monad-rpc-docs`. Keep the field allowlists here in
//! sync with the projection allowlists in [`super::queryx`].

#![allow(dead_code)]

use monad_rpc_docs::rpc;
use schemars::JsonSchema;
use serde::Deserialize;

use crate::types::jsonrpc::JsonRpcError;

/// A `DATA` filter value: a single hex value or an array of values that
/// match if any element matches.
#[derive(Deserialize, JsonSchema)]
#[serde(untagged)]
enum ValueOrArray {
    Single(String),
    Many(Vec<String>),
}

/// A `fields` selection for one object type: `true` to include all
/// fields, or an explicit array of field names.
#[derive(Deserialize, JsonSchema)]
#[serde(untagged)]
enum FieldSelection {
    All(bool),
    Names(Vec<String>),
}

/// A resolved block reference returned in every queryX response for
/// pagination and reorg detection.
#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct BlockRefDoc {
    /// Block number (`QUANTITY`).
    number: String,
    /// Block hash (`DATA`).
    hash: String,
    /// Parent block hash (`DATA`).
    parent_hash: String,
}

// ---------------------------------------------------------------------------
// Projected primary/related object schemas. Every field is optional because
// `fields` selection controls which keys are present in the response.
// ---------------------------------------------------------------------------

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct BlockObject {
    number: Option<String>,
    hash: Option<String>,
    parent_hash: Option<String>,
    timestamp: Option<String>,
    miner: Option<String>,
    gas_limit: Option<String>,
    gas_used: Option<String>,
    base_fee_per_gas: Option<String>,
    state_root: Option<String>,
    transactions_root: Option<String>,
    receipts_root: Option<String>,
    logs_bloom: Option<String>,
    extra_data: Option<String>,
    nonce: Option<String>,
    mix_hash: Option<String>,
    difficulty: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct TransactionObject {
    block_number: Option<String>,
    block_hash: Option<String>,
    transaction_index: Option<String>,
    hash: Option<String>,
    from: Option<String>,
    to: Option<String>,
    nonce: Option<String>,
    value: Option<String>,
    gas: Option<String>,
    gas_price: Option<String>,
    max_fee_per_gas: Option<String>,
    max_priority_fee_per_gas: Option<String>,
    input: Option<String>,
    #[serde(rename = "type")]
    typ: Option<String>,
    chain_id: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct LogObject {
    block_number: Option<String>,
    block_hash: Option<String>,
    transaction_index: Option<String>,
    log_index: Option<String>,
    address: Option<String>,
    topics: Option<Vec<String>>,
    data: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct TraceObject {
    block_number: Option<String>,
    block_hash: Option<String>,
    transaction_index: Option<String>,
    trace_address: Option<Vec<String>>,
    #[serde(rename = "type")]
    typ: Option<String>,
    from: Option<String>,
    to: Option<String>,
    value: Option<String>,
    gas: Option<String>,
    gas_used: Option<String>,
    input: Option<String>,
    output: Option<String>,
    status: Option<String>,
    depth: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct TransferObject {
    block_number: Option<String>,
    block_hash: Option<String>,
    transaction_index: Option<String>,
    trace_address: Option<Vec<String>>,
    #[serde(rename = "type")]
    typ: Option<String>,
    from: Option<String>,
    to: Option<String>,
    value: Option<String>,
}

// ---------------------------------------------------------------------------
// eth_queryBlocks
// ---------------------------------------------------------------------------

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct BlocksFields {
    blocks: Option<FieldSelection>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct QueryBlocksParams {
    /// Not supported for `eth_queryBlocks`; must be omitted or `{}`.
    filter: Option<EmptyFilter>,
    fields: Option<BlocksFields>,
    /// Traversal direction: `"asc"` (default) or `"desc"`.
    order: Option<String>,
    /// Inclusive range start (`QUANTITY` or tag).
    from_block: Option<String>,
    /// Inclusive range end (`QUANTITY` or tag).
    to_block: Option<String>,
    /// Target number of primary objects (`QUANTITY`, default `0x64`).
    limit: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct EmptyFilter {}

#[derive(Deserialize, JsonSchema)]
struct BlocksData {
    blocks: Vec<BlockObject>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct QueryBlocksResult {
    data: BlocksData,
    from_block: BlockRefDoc,
    to_block: BlockRefDoc,
    cursor_block: BlockRefDoc,
}

#[rpc(method = "eth_queryBlocks")]
#[allow(non_snake_case)]
/// Query block headers over a block range. Supports `fields` selection on
/// the `blocks` object; filters and relations are not supported.
async fn doc_eth_queryBlocks(
    _params: QueryBlocksParams,
) -> Result<QueryBlocksResult, JsonRpcError> {
    unimplemented!("documentation stub")
}

// ---------------------------------------------------------------------------
// eth_queryTransactions
// ---------------------------------------------------------------------------

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct TransactionsFilter {
    from: Option<ValueOrArray>,
    to: Option<ValueOrArray>,
    /// 4-byte function selector (first 4 bytes of `input`).
    selector: Option<ValueOrArray>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct TransactionsFields {
    transactions: Option<FieldSelection>,
    blocks: Option<FieldSelection>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct QueryTransactionsParams {
    filter: Option<TransactionsFilter>,
    fields: Option<TransactionsFields>,
    order: Option<String>,
    from_block: Option<String>,
    to_block: Option<String>,
    limit: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct TransactionsData {
    transactions: Vec<TransactionObject>,
    /// Present only when `fields.blocks` is requested.
    blocks: Option<Vec<BlockObject>>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct QueryTransactionsResult {
    data: TransactionsData,
    from_block: BlockRefDoc,
    to_block: BlockRefDoc,
    cursor_block: BlockRefDoc,
}

#[rpc(method = "eth_queryTransactions")]
#[allow(non_snake_case)]
/// Query transactions over a block range. Filter by `from`, `to`, and/or
/// `selector`; optionally join the parent `blocks`.
async fn doc_eth_queryTransactions(
    _params: QueryTransactionsParams,
) -> Result<QueryTransactionsResult, JsonRpcError> {
    unimplemented!("documentation stub")
}

// ---------------------------------------------------------------------------
// eth_queryLogs
// ---------------------------------------------------------------------------

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct LogsFilter {
    address: Option<ValueOrArray>,
    /// Positional topic filter (up to 4 entries); each entry is a value,
    /// an array of values, or `null` (wildcard).
    topics: Option<Vec<Option<ValueOrArray>>>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct LogsFields {
    logs: Option<FieldSelection>,
    transactions: Option<FieldSelection>,
    blocks: Option<FieldSelection>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct QueryLogsParams {
    filter: Option<LogsFilter>,
    fields: Option<LogsFields>,
    order: Option<String>,
    from_block: Option<String>,
    to_block: Option<String>,
    limit: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct LogsData {
    logs: Vec<LogObject>,
    /// Present only when `fields.blocks` is requested.
    blocks: Option<Vec<BlockObject>>,
    /// Present only when `fields.transactions` is requested.
    transactions: Option<Vec<TransactionObject>>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct QueryLogsResult {
    data: LogsData,
    from_block: BlockRefDoc,
    to_block: BlockRefDoc,
    cursor_block: BlockRefDoc,
}

#[rpc(method = "eth_queryLogs")]
#[allow(non_snake_case)]
/// Query event logs over a block range. Filter by `address` and/or
/// positional `topics`; optionally join the related `transactions` and
/// parent `blocks`.
async fn doc_eth_queryLogs(_params: QueryLogsParams) -> Result<QueryLogsResult, JsonRpcError> {
    unimplemented!("documentation stub")
}

// ---------------------------------------------------------------------------
// eth_queryTraces
// ---------------------------------------------------------------------------

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct TracesFilter {
    from: Option<ValueOrArray>,
    to: Option<ValueOrArray>,
    selector: Option<ValueOrArray>,
    /// If `true`, only top-level traces (empty `traceAddress`).
    is_top_level: Option<bool>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct TracesFields {
    traces: Option<FieldSelection>,
    transactions: Option<FieldSelection>,
    blocks: Option<FieldSelection>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct QueryTracesParams {
    filter: Option<TracesFilter>,
    fields: Option<TracesFields>,
    order: Option<String>,
    from_block: Option<String>,
    to_block: Option<String>,
    limit: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct TracesData {
    traces: Vec<TraceObject>,
    /// Present only when `fields.blocks` is requested.
    blocks: Option<Vec<BlockObject>>,
    /// Present only when `fields.transactions` is requested.
    transactions: Option<Vec<TransactionObject>>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct QueryTracesResult {
    data: TracesData,
    from_block: BlockRefDoc,
    to_block: BlockRefDoc,
    cursor_block: BlockRefDoc,
}

#[rpc(method = "eth_queryTraces")]
#[allow(non_snake_case)]
/// Query internal call traces over a block range. Filter by `from`, `to`,
/// `selector`, and/or `isTopLevel`; optionally join the related
/// `transactions` and parent `blocks`.
async fn doc_eth_queryTraces(
    _params: QueryTracesParams,
) -> Result<QueryTracesResult, JsonRpcError> {
    unimplemented!("documentation stub")
}

// ---------------------------------------------------------------------------
// eth_queryTransfers
// ---------------------------------------------------------------------------

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct TransfersFilter {
    from: Option<ValueOrArray>,
    to: Option<ValueOrArray>,
    /// If `true`, only top-level transfers (initiated directly by a tx).
    is_top_level: Option<bool>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct TransfersFields {
    transfers: Option<FieldSelection>,
    transactions: Option<FieldSelection>,
    blocks: Option<FieldSelection>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct QueryTransfersParams {
    filter: Option<TransfersFilter>,
    fields: Option<TransfersFields>,
    order: Option<String>,
    from_block: Option<String>,
    to_block: Option<String>,
    limit: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct TransfersData {
    transfers: Vec<TransferObject>,
    /// Present only when `fields.blocks` is requested.
    blocks: Option<Vec<BlockObject>>,
    /// Present only when `fields.transactions` is requested.
    transactions: Option<Vec<TransactionObject>>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct QueryTransfersResult {
    data: TransfersData,
    from_block: BlockRefDoc,
    to_block: BlockRefDoc,
    cursor_block: BlockRefDoc,
}

#[rpc(method = "eth_queryTransfers")]
#[allow(non_snake_case)]
/// Query value transfers over a block range. Filter by `from`, `to`,
/// and/or `isTopLevel`; optionally join the related `transactions` and
/// parent `blocks`.
async fn doc_eth_queryTransfers(
    _params: QueryTransfersParams,
) -> Result<QueryTransfersResult, JsonRpcError> {
    unimplemented!("documentation stub")
}
