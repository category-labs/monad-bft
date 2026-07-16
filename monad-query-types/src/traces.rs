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

use alloy_primitives::{Address, Bytes, U256};
use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_query_errors::{QueryError, Result};
use monad_query_primitives::{CallKind, Hash32};

use crate::ingest_types::IngestTrace;

/// Public, owned per-trace view returned by queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceEntry {
    pub block_number: u64,
    pub block_hash: Hash32,
    pub tx_index: u32,
    pub trace_address: Vec<u32>,
    pub typ: CallKind,
    pub from: Address,
    pub to: Option<Address>,
    pub value: U256,
    pub gas: u64,
    pub gas_used: u64,
    pub input: Bytes,
    pub output: Bytes,
    pub status: u8,
    pub depth: u32,
    pub tx_status: bool,
}

impl TraceEntry {
    pub fn is_top_level(&self) -> bool {
        self.trace_address.is_empty()
    }

    /// Extracts the 4-byte function selector from call input.
    pub fn selector(&self) -> Option<[u8; 4]> {
        selector_from_input(&self.input)
    }
}

/// Extracts 4-byte function selector from call input.
pub fn selector_from_input(input: &[u8]) -> Option<[u8; 4]> {
    input
        .get(..4)
        .and_then(|first_four_bytes| first_four_bytes.try_into().ok())
}

/// Per-trace fields stored in block trace blob (RLP encoded).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredTrace {
    pub typ: CallKind,
    pub from: Address,
    pub to: Option<Address>,
    pub value: U256,
    pub gas: u64,
    pub gas_used: u64,
    pub input: Bytes,
    pub output: Bytes,
    pub status: u8,
    pub depth: u32,
    pub tx_index: u32,
    pub trace_address: Vec<u32>,
    pub tx_status: bool,
}

/// RLP-encodable shape for StoredTrace.
#[derive(Debug, RlpEncodable, RlpDecodable)]
struct StoredTraceRlp {
    typ_byte: u8,
    from: Address,
    to_bytes: Bytes,
    value: U256,
    gas: u64,
    gas_used: u64,
    input: Bytes,
    output: Bytes,
    status: u8,
    depth: u32,
    tx_index: u32,
    trace_address: Vec<u32>,
    tx_status_byte: u8,
}

impl StoredTrace {
    /// RLP-encodes this trace.
    pub fn encode(&self) -> Vec<u8> {
        let to_bytes = self
            .to
            .as_ref()
            .map(|addr| Bytes::copy_from_slice(addr.as_slice()))
            .unwrap_or_default();

        let rlp = StoredTraceRlp {
            typ_byte: self.typ.into(),
            from: self.from,
            to_bytes,
            value: self.value,
            gas: self.gas,
            gas_used: self.gas_used,
            input: self.input.clone(),
            output: self.output.clone(),
            status: self.status,
            depth: self.depth,
            tx_index: self.tx_index,
            trace_address: self.trace_address.clone(),
            tx_status_byte: u8::from(self.tx_status),
        };
        alloy_rlp::encode(&rlp)
    }

    /// RLP-decodes a trace from bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let decoded_rlp: StoredTraceRlp = alloy_rlp::decode_exact(bytes)
            .map_err(|_| QueryError::Decode("invalid trace entry rlp"))?;

        let typ = CallKind::try_from(decoded_rlp.typ_byte)
            .map_err(|_| QueryError::Decode("invalid trace call kind byte"))?;

        let to = match decoded_rlp.to_bytes.len() {
            0 => None,
            20 => Some(Address::from_slice(&decoded_rlp.to_bytes)),
            _ => return Err(QueryError::Decode("invalid trace `to` byte length")),
        };

        Ok(Self {
            typ,
            from: decoded_rlp.from,
            to,
            value: decoded_rlp.value,
            gas: decoded_rlp.gas,
            gas_used: decoded_rlp.gas_used,
            input: decoded_rlp.input,
            output: decoded_rlp.output,
            status: decoded_rlp.status,
            depth: decoded_rlp.depth,
            tx_index: decoded_rlp.tx_index,
            trace_address: decoded_rlp.trace_address,
            tx_status: decoded_rlp.tx_status_byte != 0,
        })
    }

    /// Expands this stored trace with block metadata into a full `TraceEntry`.
    pub fn into_trace_entry(self, block_number: u64, block_hash: Hash32) -> TraceEntry {
        TraceEntry {
            block_number,
            block_hash,
            tx_index: self.tx_index,
            trace_address: self.trace_address,
            typ: self.typ,
            from: self.from,
            to: self.to,
            value: self.value,
            gas: self.gas,
            gas_used: self.gas_used,
            input: self.input,
            output: self.output,
            status: self.status,
            depth: self.depth,
            tx_status: self.tx_status,
        }
    }
}

impl From<&IngestTrace> for StoredTrace {
    fn from(trace: &IngestTrace) -> Self {
        Self {
            typ: trace.typ,
            from: trace.from,
            to: trace.to,
            value: trace.value,
            gas: trace.gas,
            gas_used: trace.gas_used,
            input: trace.input.clone(),
            output: trace.output.clone(),
            status: trace.status,
            depth: trace.depth,
            tx_index: trace.tx_index,
            trace_address: trace.trace_address.clone(),
            tx_status: trace.tx_status,
        }
    }
}

/// Computes OpenEthereum-style `trace_address` for each frame in a DFS sequence.
/// The first frame must have depth 0; deeper frames must have `depth == parent.depth + 1`.
pub fn compute_trace_addresses(depths: &[u32]) -> Result<Vec<Vec<u32>>> {
    struct State {
        path: Vec<u32>,
        sibling_counters: Vec<u32>,
        addresses: Vec<Vec<u32>>,
        seen_root: bool,
    }

    depths
        .iter()
        .try_fold(
            State {
                path: Vec::new(),
                sibling_counters: Vec::new(),
                addresses: Vec::new(),
                seen_root: false,
            },
            |mut state, &depth| {
                let d = depth as usize;

                if d == 0 {
                    state.path.clear();
                    state.sibling_counters.clear();
                    state.addresses.push(Vec::new());
                    state.seen_root = true;
                } else {
                    if !state.seen_root {
                        return Err(QueryError::InvalidBlock(
                            "trace_address: first frame must have depth 0",
                        ));
                    }
                    if d > state.path.len() + 1 {
                        return Err(QueryError::InvalidBlock(
                            "trace_address: depth skipped a level",
                        ));
                    }

                    state.path.truncate(d - 1);
                    state.sibling_counters.truncate(d);
                    if state.sibling_counters.len() < d {
                        state.sibling_counters.resize(d, 0);
                    }

                    state.path.push(state.sibling_counters[d - 1]);
                    state.sibling_counters[d - 1] += 1;
                    state.addresses.push(state.path.clone());
                }

                Ok(state)
            },
        )
        .map(|state| state.addresses)
}

#[cfg(test)]
mod trace_address_tests {
    use monad_query_errors::QueryError;

    use super::compute_trace_addresses;

    #[test]
    fn trace_address_simple_tree() {
        let depths = [0u32, 1, 2, 2, 1, 2];
        let addresses = compute_trace_addresses(&depths).expect("compute");
        assert_eq!(
            addresses,
            vec![vec![], vec![0], vec![0, 0], vec![0, 1], vec![1], vec![1, 0],]
        );
    }

    #[test]
    fn trace_address_three_deep() {
        let depths = [0u32, 1, 2, 3, 3, 1];
        let addresses = compute_trace_addresses(&depths).expect("compute");
        assert_eq!(
            addresses,
            vec![
                vec![],
                vec![0],
                vec![0, 0],
                vec![0, 0, 0],
                vec![0, 0, 1],
                vec![1],
            ]
        );
    }

    #[test]
    fn trace_address_multiple_txs() {
        let depths = [0u32, 1, 1, 0, 1, 2];
        let addresses = compute_trace_addresses(&depths).expect("compute");
        assert_eq!(
            addresses,
            vec![vec![], vec![0], vec![1], vec![], vec![0], vec![0, 0],]
        );
    }

    #[test]
    fn trace_address_rejects_orphan_depth() {
        let depths = [1u32];
        let err = compute_trace_addresses(&depths).unwrap_err();
        assert!(matches!(err, QueryError::InvalidBlock(_)));
    }

    #[test]
    fn trace_address_rejects_depth_jump() {
        let depths = [0u32, 2];
        let err = compute_trace_addresses(&depths).unwrap_err();
        assert!(matches!(err, QueryError::InvalidBlock(_)));
    }
}
