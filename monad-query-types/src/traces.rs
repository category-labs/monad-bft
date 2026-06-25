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

use monad_query_errors::{MonadChainDataError, Result};
use monad_query_primitives::{CallKind, Hash32};

use crate::ingest_types::IngestTrace;

/// Public, owned per-trace view returned by queries. Block-level fields
/// are reconstructed from the `BlockRecord` at read time.
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

    /// 4-byte function selector; `None` when `input` is shorter than 4
    /// bytes (e.g. value-only transfers).
    pub fn selector(&self) -> Option<[u8; 4]> {
        selector_from_input(&self.input)
    }
}

/// 4-byte selector from raw call input; `None` when shorter than 4 bytes.
/// The single extraction rule shared by the `selector` index writes
/// (`ingest::stream_entries_for_trace`) and the read-side accessor.
pub fn selector_from_input(input: &[u8]) -> Option<[u8; 4]> {
    input.get(..4).and_then(|s| s.try_into().ok())
}

/// Per-trace fields stored in the block trace blob (RLP, `typ` as one
/// byte). Block-level fields are reconstructed at read time.
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

/// On-disk shape. `to_bytes` is empty when `to` is `None`, else a 20-byte
/// address; the RLP derives can't encode `Option<Address>` directly.
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
    pub fn encode(&self) -> Vec<u8> {
        let to_bytes = match self.to {
            Some(addr) => Bytes::copy_from_slice(addr.as_slice()),
            None => Bytes::new(),
        };
        let rlp = StoredTraceRlp {
            typ_byte: self.typ.as_u8(),
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

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let rlp: StoredTraceRlp = alloy_rlp::decode_exact(bytes)
            .map_err(|_| MonadChainDataError::Decode("invalid trace entry rlp"))?;
        let typ = CallKind::from_u8(rlp.typ_byte)
            .ok_or(MonadChainDataError::Decode("invalid trace call kind byte"))?;
        let to = match rlp.to_bytes.len() {
            0 => None,
            20 => Some(Address::from_slice(&rlp.to_bytes)),
            _ => {
                return Err(MonadChainDataError::Decode(
                    "invalid trace `to` byte length",
                ))
            }
        };
        Ok(Self {
            typ,
            from: rlp.from,
            to,
            value: rlp.value,
            gas: rlp.gas,
            gas_used: rlp.gas_used,
            input: rlp.input,
            output: rlp.output,
            status: rlp.status,
            depth: rlp.depth,
            tx_index: rlp.tx_index,
            trace_address: rlp.trace_address,
            tx_status: rlp.tx_status_byte != 0,
        })
    }

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

/// Computes the OpenEthereum-style `trace_address` for each frame in a
/// pre-flattened DFS sequence belonging to a single tx. The depth of the
/// first frame must be `0`; deeper frames must have `depth == parent.depth + 1`.
///
/// For a tx with the call tree `root -> A -> A1, A2; root -> B`, the
/// emitted addresses are `[]`, `[0]`, `[0, 0]`, `[0, 1]`, `[1]`. Shared by the
/// ingest write path and the external-payload decode read path, so they can
/// never compute a different addressing.
pub fn compute_trace_addresses<I>(frames: I) -> Result<Vec<Vec<u32>>>
where
    I: IntoIterator<Item = u32>,
{
    // Path components so far; `cursor.len()` == current depth.
    let mut cursor: Vec<u32> = Vec::new();
    // `next_child_slot[d]` is the next sibling index to assign at depth d + 1.
    let mut next_child_slot: Vec<u32> = Vec::new();
    let mut out: Vec<Vec<u32>> = Vec::new();
    let mut seen_root = false;

    for depth in frames {
        let d = depth as usize;
        if d == 0 {
            cursor.clear();
            next_child_slot.clear();
            out.push(Vec::new());
            seen_root = true;
        } else {
            if !seen_root {
                return Err(MonadChainDataError::InvalidBlock(
                    "trace_address: first frame must have depth 0",
                ));
            }
            if d > cursor.len() + 1 {
                return Err(MonadChainDataError::InvalidBlock(
                    "trace_address: depth skipped a level",
                ));
            }
            // Returning to a shallower depth closes deeper subtrees: drop
            // their path components and sibling counters.
            cursor.truncate(d - 1);
            next_child_slot.resize(d, 0);
            let slot_idx = d - 1;
            let slot = next_child_slot[slot_idx];
            next_child_slot[slot_idx] = slot + 1;
            cursor.push(slot);
            out.push(cursor.clone());
        }
    }

    Ok(out)
}

#[cfg(test)]
mod trace_address_tests {
    use monad_query_errors::MonadChainDataError;

    use super::compute_trace_addresses;

    #[test]
    fn trace_address_simple_tree() {
        // root -> {A -> {A1, A2}, B -> B1}
        let depths = [0u32, 1, 2, 2, 1, 2];
        let addresses = compute_trace_addresses(depths).expect("compute");
        assert_eq!(
            addresses,
            vec![vec![], vec![0], vec![0, 0], vec![0, 1], vec![1], vec![1, 0],]
        );
    }

    #[test]
    fn trace_address_three_deep() {
        // root -> A -> A1 -> A1a, A1b; root -> B
        let depths = [0u32, 1, 2, 3, 3, 1];
        let addresses = compute_trace_addresses(depths).expect("compute");
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
        // Two top-level frames, each its own tx.
        let depths = [0u32, 1, 1, 0, 1, 2];
        let addresses = compute_trace_addresses(depths).expect("compute");
        assert_eq!(
            addresses,
            vec![vec![], vec![0], vec![1], vec![], vec![0], vec![0, 0],]
        );
    }

    #[test]
    fn trace_address_rejects_orphan_depth() {
        let err = compute_trace_addresses([1u32]).unwrap_err();
        assert!(matches!(err, MonadChainDataError::InvalidBlock(_)));
    }

    #[test]
    fn trace_address_rejects_depth_jump() {
        let err = compute_trace_addresses([0u32, 2]).unwrap_err();
        assert!(matches!(err, MonadChainDataError::InvalidBlock(_)));
    }
}
