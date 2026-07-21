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

//! queryX wire shapes per the `docs/queryX.md` spec: flat camelCase
//! request objects with `QUANTITY | TAG` bounds, `"asc"`/`"desc"` order,
//! `DATA | DATA[]` filter values, and a `fields` object that both selects the
//! returned fields and opts into relations. Responses carry a `data` object
//! keyed by object type plus the resolved `fromBlock`/`toBlock`/`cursorBlock`
//! references. The conversions to and from the engine's request/response
//! types live here so the engine stays wire-agnostic.

use std::{
    collections::{BTreeMap, HashSet},
    hash::Hash,
};

use alloy_eips::BlockNumberOrTag;
use alloy_primitives::{Address, Bytes, FixedBytes, B256, U256, U64};
use monad_query_primitives::{
    limits::QueryEnvelope,
    order::QueryOrder,
    refs::{BlockRef, BlockSpan},
    EvmBlockHeader, Hash32,
};
use monad_query_read::{
    blocks::Block,
    logs::{LogEntry, LogFilter},
    traces::{TraceEntry, TraceFilter},
    transfers::{TransferEntry, TransferFilter},
    txs::{TxEntry, TxFilter},
};
use serde::{Deserialize, Serialize};
use serde_json::{
    value::{to_raw_value, RawValue},
    Value,
};

use crate::types::{
    jsonrpc::{JsonRpcError, JsonRpcResult},
    CallKind,
};

/// `DATA | DATA[]`: a spec filter field accepts a single value or an array
/// (an array matches if the value equals any element).
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum OneOrMany<T> {
    One(T),
    Many(Vec<T>),
}

impl<T: Eq + Hash> OneOrMany<T> {
    fn into_set(self) -> HashSet<T> {
        match self {
            Self::One(value) => HashSet::from_iter([value]),
            Self::Many(values) => values.into_iter().collect(),
        }
    }
}

fn value_set<T: Eq + Hash>(values: Option<OneOrMany<T>>) -> Option<HashSet<T>> {
    values.map(OneOrMany::into_set)
}

fn selector_set(values: Option<OneOrMany<FixedBytes<4>>>) -> Option<HashSet<[u8; 4]>> {
    values.map(|v| v.into_set().into_iter().map(|s| s.0).collect())
}

/// `"asc" | "desc"` traversal direction; `"asc"` is the spec default.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderWire {
    #[default]
    Asc,
    Desc,
}

impl From<OrderWire> for QueryOrder {
    fn from(value: OrderWire) -> Self {
        match value {
            OrderWire::Asc => Self::Ascending,
            OrderWire::Desc => Self::Descending,
        }
    }
}

/// A `fields` object value: `true` to include all of a schema's fields, or
/// the field names to keep (`string[]`).
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum FieldSelection {
    All(bool),
    Fields(Vec<String>),
}

/// The spec's common request shape, generic over the method's filter object.
/// Every field is optional; unknown keys are rejected (`-32602`).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, default)]
pub struct QueryRequestWire<F> {
    pub filter: Option<F>,
    pub fields: Option<BTreeMap<String, FieldSelection>>,
    pub order: Option<OrderWire>,
    pub from_block: Option<BlockNumberOrTag>,
    pub to_block: Option<BlockNumberOrTag>,
    pub limit: Option<U64>,
}

// Manual impl: a derive would demand `F: Default` for no reason.
impl<F> Default for QueryRequestWire<F> {
    fn default() -> Self {
        Self {
            filter: None,
            fields: None,
            order: None,
            from_block: None,
            to_block: None,
            limit: None,
        }
    }
}

impl<F> QueryRequestWire<F> {
    /// The engine envelope: spec defaults applied, tags resolved.
    pub fn envelope(&self) -> JsonRpcResult<QueryEnvelope> {
        let order = self.order.unwrap_or_default();
        // Which side's omitted default is the head depends on the order:
        // `asc` scans toward the head (`toBlock`), `desc` starts from it.
        let (from_defaults_to_latest, to_defaults_to_latest) = match order {
            OrderWire::Asc => (false, true),
            OrderWire::Desc => (true, false),
        };
        let mut envelope = QueryEnvelope {
            from_block: resolve_bound(self.from_block, "fromBlock", from_defaults_to_latest)?,
            to_block: resolve_bound(self.to_block, "toBlock", to_defaults_to_latest)?,
            order: order.into(),
            ..QueryEnvelope::default()
        };
        if let Some(limit) = self.limit {
            envelope.limit = usize::try_from(limit.to::<u64>())
                .map_err(|_| JsonRpcError::filter_error("limit out of range".into()))?;
        }
        Ok(envelope)
    }
}

/// Maps a `QUANTITY | TAG` bound onto the engine's `Option<u64>`, where
/// `None` means "this side's per-order default". Everything this store serves
/// is finalized, so `latest`/`finalized`/`safe` all name the published head:
/// they resolve to the default (`None`) on the side whose omitted default is
/// the head and are rejected on the other side (the engine addresses the head
/// implicitly, never by number); `earliest` mirrors that for the genesis side.
fn resolve_bound(
    bound: Option<BlockNumberOrTag>,
    name: &str,
    defaults_to_latest: bool,
) -> JsonRpcResult<Option<u64>> {
    let tag_is_default = |is_latest_tag: bool| {
        if is_latest_tag == defaults_to_latest {
            Ok(None)
        } else {
            Err(JsonRpcError::filter_error(format!(
                "{name}: this tag is only supported where it is the per-order default; \
                 pass an explicit block number instead"
            )))
        }
    };
    match bound {
        None => Ok(None),
        Some(BlockNumberOrTag::Number(number)) => Ok(Some(number)),
        Some(BlockNumberOrTag::Latest | BlockNumberOrTag::Finalized | BlockNumberOrTag::Safe) => {
            tag_is_default(true)
        }
        Some(BlockNumberOrTag::Earliest) => tag_is_default(false),
        Some(BlockNumberOrTag::Pending) => Err(JsonRpcError::filter_error(format!(
            "{name}: \"pending\" is not supported (finalized data only)"
        ))),
    }
}

/// `eth_queryLogs` filter: `address` is `DATA | DATA[]`; `topics` follows
/// `eth_getLogs` semantics (at most 4 positional entries, each
/// `DATA | DATA[] | null`, trailing `null`s may be omitted).
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, default)]
pub struct LogFilterWire {
    pub address: Option<OneOrMany<Address>>,
    pub topics: Option<Vec<Option<OneOrMany<B256>>>>,
}

impl LogFilterWire {
    pub fn into_engine(self) -> JsonRpcResult<LogFilter> {
        let mut filter = LogFilter {
            address: value_set(self.address),
            ..LogFilter::default()
        };
        if let Some(topics) = self.topics {
            if topics.len() > filter.topics.len() {
                return Err(JsonRpcError::filter_error(
                    "topics: at most 4 positional entries".into(),
                ));
            }
            for (position, entry) in topics.into_iter().enumerate() {
                filter.topics[position] = entry.map(OneOrMany::into_set);
            }
        }
        Ok(filter)
    }
}

/// `eth_queryTransactions` filter; `selector` entries are 4-byte `DATA`.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, default)]
pub struct TxFilterWire {
    pub from: Option<OneOrMany<Address>>,
    pub to: Option<OneOrMany<Address>>,
    pub selector: Option<OneOrMany<FixedBytes<4>>>,
}

impl TxFilterWire {
    pub fn into_engine(self) -> TxFilter {
        TxFilter {
            from: value_set(self.from),
            to: value_set(self.to),
            selector: selector_set(self.selector),
        }
    }
}

/// `eth_queryTraces` filter.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, default)]
pub struct TraceFilterWire {
    pub from: Option<OneOrMany<Address>>,
    pub to: Option<OneOrMany<Address>>,
    pub selector: Option<OneOrMany<FixedBytes<4>>>,
    pub is_top_level: Option<bool>,
}

impl TraceFilterWire {
    pub fn into_engine(self) -> TraceFilter {
        TraceFilter {
            from: value_set(self.from),
            to: value_set(self.to),
            selector: selector_set(self.selector),
            is_top_level: self.is_top_level,
        }
    }
}

/// `eth_queryTransfers` filter.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, default)]
pub struct TransferFilterWire {
    pub from: Option<OneOrMany<Address>>,
    pub to: Option<OneOrMany<Address>>,
    pub is_top_level: Option<bool>,
}

impl TransferFilterWire {
    pub fn into_engine(self) -> TransferFilter {
        TransferFilter {
            from: value_set(self.from),
            to: value_set(self.to),
            is_top_level: self.is_top_level,
        }
    }
}

/// `eth_queryBlocks` supports no filtering: the spec says the `filter` field
/// MUST be omitted or `{}`.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmptyFilterWire {}

/// The validated `fields` object of one request. A present key opts the
/// object into the response (relations join only when named); its selection
/// projects the returned fields. Field NAMES are not validated — an unknown
/// name simply selects nothing.
#[derive(Debug)]
pub struct FieldsPlan(BTreeMap<String, FieldSelection>);

impl FieldsPlan {
    /// Validates the `fields` keys against the method's object names
    /// (primary first, then its supported relations).
    pub fn new(
        fields: Option<BTreeMap<String, FieldSelection>>,
        allowed: &[&str],
    ) -> JsonRpcResult<Self> {
        let map = fields.unwrap_or_default();
        for (key, selection) in &map {
            if !allowed.contains(&key.as_str()) {
                return Err(JsonRpcError::filter_error(format!(
                    "unknown fields key {key:?} (expected one of: {})",
                    allowed.join(", ")
                )));
            }
            if matches!(selection, FieldSelection::All(false)) {
                return Err(JsonRpcError::filter_error(format!(
                    "fields[{key:?}] must be true or an array of field names"
                )));
            }
        }
        Ok(Self(map))
    }

    /// Whether the request opted this object into the response.
    pub fn wants(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }

    /// Inserts one `data` entry, projected to the requested field names.
    /// Without a projection the objects serialize straight to raw JSON in one
    /// pass — the dominant path for dense pages — instead of detouring
    /// through a `Value` tree that the response serializer walks again.
    pub fn insert<T: Serialize>(
        &self,
        data: &mut BTreeMap<String, Box<RawValue>>,
        key: &str,
        objects: &[T],
    ) -> JsonRpcResult<()> {
        let serialize_err =
            |e: serde_json::Error| JsonRpcError::internal_error(format!("serialize response: {e}"));
        let raw = match self.0.get(key) {
            Some(FieldSelection::Fields(fields)) => {
                let mut values = objects
                    .iter()
                    .map(serde_json::to_value)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(serialize_err)?;
                for value in &mut values {
                    if let Value::Object(map) = value {
                        map.retain(|name, _| fields.iter().any(|field| field == name));
                    }
                }
                to_raw_value(&values)
            }
            _ => to_raw_value(&objects),
        }
        .map_err(serialize_err)?;
        data.insert(key.to_owned(), raw);
        Ok(())
    }
}

/// Projects `transactions` objects into the standard RPC transaction shape
/// via [`TxEntry::to_rpc_transaction`] (`gasPrice` for dynamic-fee
/// transactions is omitted — it needs the block base fee, which this path
/// does not join).
pub fn rpc_transactions(txs: Vec<TxEntry>) -> Vec<alloy_rpc_types::Transaction> {
    txs.iter().map(TxEntry::to_rpc_transaction).collect()
}

/// One resolved block reference: `{ number, hash, parentHash }`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockRefWire {
    pub number: U64,
    pub hash: Hash32,
    pub parent_hash: Hash32,
}

impl From<BlockRef> for BlockRefWire {
    fn from(value: BlockRef) -> Self {
        Self {
            number: U64::from(value.number),
            hash: value.hash,
            parent_hash: value.parent_hash,
        }
    }
}

/// The spec's common response shape: `data` keyed by object type plus the
/// three resolved block references.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryResponseWire {
    pub data: BTreeMap<String, Box<RawValue>>,
    pub from_block: BlockRefWire,
    pub to_block: BlockRefWire,
    pub cursor_block: BlockRefWire,
}

impl QueryResponseWire {
    pub fn new(span: BlockSpan) -> Self {
        Self {
            data: BTreeMap::new(),
            from_block: span.from_block.into(),
            to_block: span.to_block.into(),
            cursor_block: span.cursor_block.into(),
        }
    }
}

/// A `logs` object (camelCase, QUANTITY-encoded scalars).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogWire {
    pub block_number: U64,
    pub block_hash: Hash32,
    pub transaction_index: U64,
    pub log_index: U64,
    pub address: Address,
    pub topics: Vec<B256>,
    pub data: Bytes,
}

impl From<LogEntry> for LogWire {
    fn from(value: LogEntry) -> Self {
        Self {
            block_number: U64::from(value.block_number),
            block_hash: value.block_hash,
            transaction_index: U64::from(value.tx_index),
            log_index: U64::from(value.log_index),
            address: value.address,
            topics: value.topics,
            data: value.data,
        }
    }
}

/// A `blocks` object: the full header (camelCase, QUANTITY-encoded by the
/// alloy serde impls) plus the block hash, which the header doesn't carry.
#[derive(Debug, Serialize)]
pub struct BlockWire {
    pub hash: Hash32,
    #[serde(flatten)]
    pub header: EvmBlockHeader,
}

impl From<Block> for BlockWire {
    fn from(value: Block) -> Self {
        Self {
            hash: value.hash,
            header: value.header,
        }
    }
}

/// A `traces` object. `traceAddress` entries are plain integers (the
/// `trace_*` API convention); `to` is `null` for `create`/`create2` frames.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceWire {
    pub block_number: U64,
    pub block_hash: Hash32,
    pub transaction_index: U64,
    pub trace_address: Vec<u32>,
    #[serde(rename = "type")]
    pub typ: CallKind,
    pub from: Address,
    pub to: Option<Address>,
    pub value: U256,
    pub gas: U64,
    pub gas_used: U64,
    pub input: Bytes,
    pub output: Bytes,
    pub status: U64,
    pub depth: U64,
    /// Whether the ENCLOSING transaction succeeded (`status` is this frame's).
    pub transaction_status: bool,
}

impl From<TraceEntry> for TraceWire {
    fn from(value: TraceEntry) -> Self {
        Self {
            block_number: U64::from(value.block_number),
            block_hash: value.block_hash,
            transaction_index: U64::from(value.tx_index),
            trace_address: value.trace_address,
            typ: value.typ.into(),
            from: value.from,
            to: value.to,
            value: value.value,
            gas: U64::from(value.gas),
            gas_used: U64::from(value.gas_used),
            input: value.input,
            output: value.output,
            status: U64::from(value.status),
            depth: U64::from(value.depth),
            transaction_status: value.tx_status,
        }
    }
}

/// A `transfers` object; `to` is always present (`create*` → new contract,
/// `selfdestruct` → beneficiary).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferWire {
    pub block_number: U64,
    pub block_hash: Hash32,
    pub transaction_index: U64,
    pub trace_address: Vec<u32>,
    #[serde(rename = "type")]
    pub typ: CallKind,
    pub from: Address,
    pub to: Address,
    pub value: U256,
}

impl From<TransferEntry> for TransferWire {
    fn from(value: TransferEntry) -> Self {
        Self {
            block_number: U64::from(value.block_number),
            block_hash: value.block_hash,
            transaction_index: U64::from(value.tx_index),
            trace_address: value.trace_address,
            typ: value.typ.into(),
            from: value.from,
            to: value.to,
            value: value.value,
        }
    }
}

#[cfg(test)]
mod tests {
    use monad_query_primitives::CallKind as EngineCallKind;
    use serde_json::json;

    use super::*;

    /// Pins the frozen trace/transfer wire JSON: camelCase keys, `type` as an
    /// UPPERCASE string, QUANTITY hex scalars, plain-integer `traceAddress`.
    #[test]
    fn trace_and_transfer_wire_shapes() {
        let trace = TraceWire::from(TraceEntry {
            block_number: 7,
            block_hash: Hash32::repeat_byte(0x01),
            tx_index: 2,
            trace_address: vec![0, 1],
            typ: EngineCallKind::DelegateCall,
            from: Address::repeat_byte(0xaa),
            to: None,
            value: U256::from(255),
            gas: 100_000,
            gas_used: 21_000,
            input: Bytes::from_static(&[0xa9, 0x05, 0x9c, 0xbb]),
            output: Bytes::new(),
            status: 1,
            depth: 1,
            tx_status: true,
        });
        assert_eq!(
            serde_json::to_value(trace).unwrap(),
            json!({
                "blockNumber": "0x7",
                "blockHash": format!("0x{}", "01".repeat(32)),
                "transactionIndex": "0x2",
                "traceAddress": [0, 1],
                "type": "DELEGATECALL",
                "from": format!("0x{}", "aa".repeat(20)),
                "to": null,
                "value": "0xff",
                "gas": "0x186a0",
                "gasUsed": "0x5208",
                "input": "0xa9059cbb",
                "output": "0x",
                "status": "0x1",
                "depth": "0x1",
                "transactionStatus": true,
            })
        );

        let transfer = TransferWire::from(TransferEntry {
            block_number: 7,
            block_hash: Hash32::repeat_byte(0x01),
            tx_index: 2,
            trace_address: Vec::new(),
            typ: EngineCallKind::Call,
            from: Address::repeat_byte(0xaa),
            to: Address::repeat_byte(0xbb),
            value: U256::from(16),
        });
        assert_eq!(
            serde_json::to_value(transfer).unwrap(),
            json!({
                "blockNumber": "0x7",
                "blockHash": format!("0x{}", "01".repeat(32)),
                "transactionIndex": "0x2",
                "traceAddress": [],
                "type": "CALL",
                "from": format!("0x{}", "aa".repeat(20)),
                "to": format!("0x{}", "bb".repeat(20)),
                "value": "0x10",
            })
        );
    }

    /// Pins the frozen log and block-reference wire JSON.
    #[test]
    fn log_and_block_ref_wire_shapes() {
        let log = LogWire::from(LogEntry {
            block_number: 1,
            block_hash: Hash32::repeat_byte(0x01),
            tx_index: 0,
            log_index: 3,
            address: Address::repeat_byte(0xaa),
            topics: vec![B256::repeat_byte(0x10)],
            data: Bytes::from_static(&[0xff]),
        });
        assert_eq!(
            serde_json::to_value(log).unwrap(),
            json!({
                "blockNumber": "0x1",
                "blockHash": format!("0x{}", "01".repeat(32)),
                "transactionIndex": "0x0",
                "logIndex": "0x3",
                "address": format!("0x{}", "aa".repeat(20)),
                "topics": [format!("0x{}", "10".repeat(32))],
                "data": "0xff",
            })
        );

        let span_ref = BlockRefWire::from(BlockRef {
            number: 2,
            hash: Hash32::repeat_byte(0x02),
            parent_hash: Hash32::repeat_byte(0x01),
        });
        assert_eq!(
            serde_json::to_value(span_ref).unwrap(),
            json!({
                "number": "0x2",
                "hash": format!("0x{}", "02".repeat(32)),
                "parentHash": format!("0x{}", "01".repeat(32)),
            })
        );
    }
}
